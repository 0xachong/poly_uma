// Package api 提供 UMA 事件查询的 HTTP 接口。
// 默认列表与 /proposed/latest 读最近 12h 的 MemReplica；传 source=sqlite 时读 SQLite 全量历史。
package api

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/polymas/go-polymarket-sdk/gamma"
	"github.com/polymas/poly_uma/internal/store"
)

//go:embed llms.txt
var llmsTxt []byte

// ListenAndServe 启动 HTTP 服务，ctx 取消时优雅关闭，阻塞直至退出。
// mem 为最近 12h 内存副本：默认列表走 mem；source=sqlite 走 db；mem 为 nil 时默认路径返回 503。
func ListenAndServe(ctx context.Context, addr string, db *store.SQLite, mem *store.MemReplica) error {
	// 单独的 Gin HTTP 日志文件（默认 gin-http.log，可通过 GIN_LOG_FILE 覆盖）
	ginLogFile := os.Getenv("GIN_LOG_FILE")
	if ginLogFile == "" {
		ginLogFile = "gin-http.log"
	}
	var ginLogWriter *os.File
	var err error
	ginLogWriter, err = os.OpenFile(ginLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("[WARN] 打开 Gin 日志文件失败（使用默认输出）: %v", err)
	} else {
		gin.DefaultWriter = ginLogWriter
		gin.DefaultErrorWriter = ginLogWriter
		defer ginLogWriter.Close()
	}

	r := gin.New()
	// Gin 默认格式 HTTP 日志 + 自定义 panic 恢复与业务日志
	r.Use(gin.Logger(), recoverAndLog())

	r.GET("/uma/v1/settled", makeTypeHandler(db, mem, "resolved"))
	r.GET("/uma/v1/disputed", makeTypeHandler(db, mem, "dispute"))
	r.GET("/uma/v1/proposed", makeTypeHandler(db, mem, "propose"))
	r.GET("/uma/v1/proposed/latest", makeLatestProposedHandler(db, mem))
	// WebSocket 实时推送 propose / dispute 事件（wss 接口）
	r.GET("/uma/v1/ws/proposed", makeWsTypeHandler(mem, "propose"))
	r.GET("/uma/v1/ws/disputed", makeWsTypeHandler(mem, "dispute"))
	// WebSocket 压测端点：回显 + 可控下行推流（仅在 WS_BENCH_ENABLE=1 时启用）
	if os.Getenv("WS_BENCH_ENABLE") == "1" {
		r.GET("/uma/v1/ws/bench", makeWsBenchHandler())
	}
	r.GET("/healthz", makeHealthzHandler(db))
	r.GET("/llms.txt", makeLLMsHandler())

	// 未注册路径 / 不允许的方法：统一返回 JSON，便于客户端解析
	r.NoRoute(func(c *gin.Context) {
		jsonError(c, http.StatusNotFound, "not found")
	})
	r.NoMethod(func(c *gin.Context) {
		jsonError(c, http.StatusMethodNotAllowed, "method not allowed")
	})

	// WriteTimeout 应大于 HTTP_HANDLER_TIMEOUT（默认 10s），否则可能在返回 503 JSON 前被底层关连接。
	writeTimeout := 30 * time.Second
	if s := strings.TrimSpace(os.Getenv("HTTP_WRITE_TIMEOUT")); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			writeTimeout = d
		}
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	log.Printf("[INFO] HTTP API 监听 %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func recoverAndLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[ERROR] HTTP handler panic: method=%s path=%s remote=%s err=%v\n%s",
					c.Request.Method, c.Request.URL.Path, c.ClientIP(), rec, string(debug.Stack()))
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
				return
			}
		}()
		c.Next()
	}
}

// handlerTimeout 返回单请求业务处理超时；超时后返回 503 JSON，避免长时间挂起直到 WriteTimeout 导致客户端 EOF。
// 环境变量 HTTP_HANDLER_TIMEOUT：Go duration，如 10s、500ms；空则默认 10s；0 / off / disable 表示关闭。
func handlerTimeout() time.Duration {
	s := strings.TrimSpace(os.Getenv("HTTP_HANDLER_TIMEOUT"))
	if s == "" {
		return 10 * time.Second
	}
	if s == "0" || strings.EqualFold(s, "off") || strings.EqualFold(s, "disable") {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 10 * time.Second
	}
	return d
}

// parseQuerySource 解析 source：缺省或 memory → 走内存；sqlite → 走 SQLite。
func parseQuerySource(c *gin.Context) (useMemory bool, errMsg string) {
	s := strings.ToLower(strings.TrimSpace(c.Query("source")))
	if s == "" || s == "memory" {
		return true, ""
	}
	if s == "sqlite" {
		return false, ""
	}
	return false, "source 须为 memory（默认）或 sqlite"
}

// makeTypeHandler 列表查询：默认 source=memory（仅最近 12h 内存）；source=sqlite 读库全量。
func makeTypeHandler(db *store.SQLite, mem *store.MemReplica, eventType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		useMemory, srcErr := parseQuerySource(c)
		if srcErr != "" {
			jsonError(c, http.StatusBadRequest, srcErr)
			return
		}
		fromTs, ok := requireInt64(c, "from_ts")
		if !ok {
			return
		}
		now := time.Now().Unix()
		toTs, ok := optInt64Query(c, "to_ts", now)
		if !ok {
			return
		}
		limit, ok := optIntQuery(c, "limit", 50)
		if !ok {
			return
		}
		limit = clamp(limit, 1, 500)
		if fromTs > toTs {
			jsonError(c, http.StatusBadRequest, "from_ts 不能大于 to_ts")
			return
		}
		cursor := c.Query("cursor")

		if useMemory {
			if mem == nil {
				jsonError(c, http.StatusServiceUnavailable, "memory replica not available")
				return
			}
			cutoff := store.RecentMemoryCutoffUnix()
			if fromTs < cutoff {
				jsonError(c, http.StatusBadRequest, "from_ts 早于内存窗口（12h），请传 source=sqlite 从 SQLite 查询")
				return
			}
		}
		if !useMemory && db == nil {
			jsonError(c, http.StatusServiceUnavailable, "sqlite not available")
			return
		}

		type queryResult struct {
			rows []store.EventRow
			err  error
		}
		ch := make(chan queryResult, 1)
		go func() {
			if useMemory {
				rows := mem.QueryByType(eventType, fromTs, toTs, limit, cursor)
				ch <- queryResult{rows: rows, err: nil}
				return
			}
			rows, err := db.QueryByType(eventType, fromTs, toTs, limit, cursor)
			ch <- queryResult{rows: rows, err: err}
		}()

		deadline := handlerTimeout()
		var rows []store.EventRow
		var err error
		if deadline <= 0 {
			res := <-ch
			rows, err = res.rows, res.err
		} else {
			timer := time.NewTimer(deadline)
			defer timer.Stop()
			select {
			case res := <-ch:
				rows, err = res.rows, res.err
			case <-timer.C:
				jsonError(c, http.StatusServiceUnavailable, "service temporarily unavailable: handler timeout")
				return
			}
		}
		if err != nil {
			jsonError(c, http.StatusInternalServerError, fmt.Sprintf("query db failed: %v", err))
			return
		}

		data := make([]map[string]interface{}, 0, len(rows))
		for _, row := range rows {
			data = append(data, eventDTO(row))
		}
		nextCursor := ""
		if len(rows) == limit {
			nextCursor = rows[len(rows)-1].TxHash
		}
		jsonOK(c, map[string]interface{}{
			"data":        data,
			"count":       len(data),
			"next_cursor": nextCursor,
		})
	}
}

func eventDTO(r store.EventRow) map[string]interface{} {
	cst := time.FixedZone("UTC+8", 8*3600)
	timeStr := time.Unix(r.Timestamp, 0).In(cst).Format("2006-01-02 15:04:05")
	return map[string]interface{}{
		"event_type":       r.EventType,
		"transaction_hash": r.TxHash,
		"log_index":        r.LogIndex,
		"block_number":     r.BlockNumber,
		"timestamp":        r.Timestamp,
		"time_utc8":        timeStr,
		"condition_id":     r.ConditionID,
		"market_id":        r.MarketID,
		"price":            r.Price,
	}
}

// ── WebSocket: 实时推送 propose / dispute ─────────────────────────────────────

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// makeWsTypeHandler 建立 WebSocket 连接，实时推送指定类型的最新事件。
// 仅支持 eventType 为 "propose" 或 "dispute"。
func makeWsTypeHandler(mem *store.MemReplica, eventType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if mem == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "memory replica not available"})
			return
		}
		conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("[ERROR] ws upgrade failed: path=%s remote=%s err=%v",
				c.Request.URL.Path, c.ClientIP(), err)
			return
		}
		defer conn.Close()

		ch, cancel := mem.Subscribe(eventType)
		defer cancel()

		log.Printf("[INFO] WS %s connected: remote=%s", eventType, c.ClientIP())
		defer log.Printf("[INFO] WS %s disconnected: remote=%s", eventType, c.ClientIP())

		// 不需要客户端发送消息，这里只做单向推送；但读取一下面避免对端关闭时资源泄露。
		go func() {
			for {
				if _, _, err := conn.NextReader(); err != nil {
					// 对端关闭或出错即退出
					return
				}
			}
		}()

		for {
			select {
			case row, ok := <-ch:
				if !ok {
					return
				}
				data := eventDTO(row)
				if err := conn.WriteJSON(data); err != nil {
					log.Printf("[WARN] WS %s write failed: remote=%s err=%v",
						eventType, c.ClientIP(), err)
					return
				}
			case <-c.Request.Context().Done():
				return
			}
		}
	}
}

// makeWsBenchHandler 提供压测用 WebSocket：
// - 可回显客户端消息（echo=1）
// - 可按速率下行推送固定 payload（send_bps, frame_bytes）
//
// Query 参数：
// - echo: 0/1（默认 1）
// - send_bps: 服务器每秒向客户端发送的字节数（默认 0，不主动推送）
// - frame_bytes: 单帧 payload 大小（默认 1024，范围 1..65536）
func makeWsBenchHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("[ERROR] ws bench upgrade failed: remote=%s err=%v", c.ClientIP(), err)
			return
		}
		defer conn.Close()

		echo := optInt(c, "echo", 1) != 0
		sendBps := optInt(c, "send_bps", 0)
		frameBytes := clamp(optInt(c, "frame_bytes", 1024), 1, 65536)
		payload := strings.Repeat("x", frameBytes)

		log.Printf("[INFO] WS bench connected: remote=%s echo=%v send_bps=%d frame_bytes=%d",
			c.ClientIP(), echo, sendBps, frameBytes)
		defer log.Printf("[INFO] WS bench disconnected: remote=%s", c.ClientIP())

		// 读循环：可选回显，保证对端关闭时及时退出。
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			for {
				mt, msg, err := conn.ReadMessage()
				if err != nil {
					return
				}
				if echo {
					_ = conn.WriteMessage(mt, msg)
				}
			}
		}()

		// 写循环：按 send_bps 推送 payload（精确节流）。
		// 使用“预算累积”方式：每 tick 增加 send_bps * dt 的预算，预算够一帧再发送。
		if sendBps > 0 {
			const tick = 10 * time.Millisecond
			ticker := time.NewTicker(tick)
			defer ticker.Stop()
			var budget float64
			budgetPerTick := float64(sendBps) * tick.Seconds()
			frameSize := float64(frameBytes)
			for {
				select {
				case <-ticker.C:
					budget += budgetPerTick
					for budget >= frameSize {
						// 避免写阻塞无限挂死：给每次写一个较短 deadline
						_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
						if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
							return
						}
						budget -= frameSize
					}
				case <-readDone:
					return
				case <-c.Request.Context().Done():
					return
				}
			}
		}

		// 不推送时就等待读结束或 context 结束
		select {
		case <-readDone:
		case <-c.Request.Context().Done():
		}
	}
}

// ── /llms.txt ────────────────────────────────────────────────────────────────

func makeLLMsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Data(http.StatusOK, "text/plain; charset=utf-8", llmsTxt)
	}
}

// ── /uma/v1/proposed/latest ───────────────────────────────────────────────────

// makeLatestProposedHandler 返回最新 propose；默认 source=memory（12h 内），source=sqlite 读库。
func makeLatestProposedHandler(db *store.SQLite, mem *store.MemReplica) gin.HandlerFunc {
	gammaClient := gamma.NewClient()
	return func(c *gin.Context) {
		type workResult struct {
			payload interface{}
			errText string
		}
		ch := make(chan workResult, 1)
		go func() {
			useMemory, srcErr := parseQuerySource(c)
			if srcErr != "" {
				ch <- workResult{errText: srcErr}
				return
			}
			var rows []store.EventRow
			var err error
			if useMemory {
				if mem == nil {
					ch <- workResult{errText: "memory replica not available"}
					return
				}
				rows = mem.QueryLatestProposed(1)
			} else {
				if db == nil {
					ch <- workResult{errText: "sqlite not available"}
					return
				}
				rows, err = db.QueryLatestProposed(1)
				if err != nil {
					ch <- workResult{errText: err.Error()}
					return
				}
			}

			conditionIDs := make([]string, 0, len(rows))
			seen := map[string]bool{}
			for _, row := range rows {
				if row.ConditionID != "" && !seen[row.ConditionID] {
					conditionIDs = append(conditionIDs, row.ConditionID)
					seen[row.ConditionID] = true
				}
			}

			marketByCondition := map[string]map[string]interface{}{}
			if len(conditionIDs) > 0 {
				markets, err := gammaClient.GetMarketsByConditionIDs(conditionIDs)
				if err != nil {
					log.Printf("[WARN] 获取 Polymarket 市场详情失败: %v", err)
				} else {
					for i := range markets {
						m := &markets[i]
						detail := map[string]interface{}{
							"market_id":             m.MarketID,
							"slug":                  m.Slug,
							"question":              m.Question,
							"description":           m.Description,
							"end_date":              m.EndDate,
							"liquidity":             m.Liquidity,
							"volume":                m.Volume,
							"uma_resolution_status": m.UmaResolutionStatus,
							"uma_end_date":          m.UmaEndDate,
						}
						marketByCondition[string(m.ConditionID)] = detail
					}
				}
			}

			data := make([]map[string]interface{}, 0, len(rows))
			for _, row := range rows {
				item := eventDTO(row)
				if d, ok := marketByCondition[row.ConditionID]; ok {
					item["market_detail"] = d
				}
				data = append(data, item)
			}

			var result interface{}
			if len(data) > 0 {
				result = data[0]
			}
			ch <- workResult{payload: result}
		}()

		deadline := handlerTimeout()
		var res workResult
		if deadline <= 0 {
			res = <-ch
		} else {
			timer := time.NewTimer(deadline)
			defer timer.Stop()
			select {
			case res = <-ch:
			case <-timer.C:
				jsonError(c, http.StatusServiceUnavailable, "service temporarily unavailable: handler timeout")
				return
			}
		}
		if res.errText != "" {
			code := http.StatusInternalServerError
			switch res.errText {
			case "memory replica not available", "sqlite not available":
				code = http.StatusServiceUnavailable
			}
			if strings.Contains(res.errText, "source 须为") {
				code = http.StatusBadRequest
			}
			jsonError(c, code, res.errText)
			return
		}
		jsonOK(c, res.payload)
	}
}

// ── /healthz ─────────────────────────────────────────────────────────────────

func makeHealthzHandler(db *store.SQLite) gin.HandlerFunc {
	return func(c *gin.Context) {
		checkpoint, _ := db.GetCheckpoint()
		latest := db.LatestSeenBlock()
		var lag int64
		if latest > 0 && latest >= checkpoint {
			lag = int64(latest - checkpoint)
		}
		status := "ok"
		if lag > 100 {
			status = "degraded"
		}
		log.Printf("[INFO] /healthz respond: status=%s lag=%d checkpoint=%d latest=%d",
			status, lag, checkpoint, latest)
		jsonOK(c, map[string]interface{}{
			"status":                status,
			"syncer_lag_blocks":     lag,
			"last_checkpoint_block": checkpoint,
			"latest_seen_block":     latest,
		})
	}
}

// ── 工具函数 ──────────────────────────────────────────────────────────────────

func requireInt64(c *gin.Context, name string) (int64, bool) {
	s := c.Query(name)
	if s == "" {
		jsonError(c, http.StatusBadRequest, name+" 为必填参数")
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		jsonError(c, http.StatusBadRequest, name+" 必须为整数")
		return 0, false
	}
	return v, true
}

func optInt64(c *gin.Context, name string, def int64) int64 {
	if s := c.Query(name); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
	}
	return def
}

// optInt64Query 可选查询参数：缺省用 def；若传了但非合法整数则 400。
func optInt64Query(c *gin.Context, name string, def int64) (int64, bool) {
	s := c.Query(name)
	if s == "" {
		return def, true
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		jsonError(c, http.StatusBadRequest, name+" 必须为整数")
		return 0, false
	}
	return v, true
}

func optInt(c *gin.Context, name string, def int) int {
	if s := c.Query(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

// optIntQuery 可选查询参数：缺省用 def；若传了但非合法整数则 400。
func optIntQuery(c *gin.Context, name string, def int) (int, bool) {
	s := c.Query(name)
	if s == "" {
		return def, true
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		jsonError(c, http.StatusBadRequest, name+" 必须为整数")
		return 0, false
	}
	return v, true
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func jsonOK(c *gin.Context, v interface{}) {
	c.JSON(http.StatusOK, v)
}

func jsonError(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(code, gin.H{"error": msg})
}
