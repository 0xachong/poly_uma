// Package api 提供 UMA 事件查询的 HTTP 接口。
// 直接读本地 SQLite uma_oo_events 表，无额外缓存层，毫秒级响应。
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
// recent 可选：非 nil 时，propose/dispute 在最近 12h 内优先从内存返回，否则查 SQLite。
func ListenAndServe(ctx context.Context, addr string, db *store.SQLite, recent *store.RecentCache) error {
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

	r.GET("/uma/v1/settled", makeTypeHandler(db, recent, "resolved"))
	r.GET("/uma/v1/disputed", makeTypeHandler(db, recent, "dispute"))
	r.GET("/uma/v1/proposed", makeTypeHandler(db, recent, "propose"))
	r.GET("/uma/v1/proposed/latest", makeLatestProposedHandler(db, recent))
	// WebSocket 实时推送 propose / dispute 事件（wss 接口）
	r.GET("/uma/v1/ws/proposed", makeWsTypeHandler(recent, "propose"))
	r.GET("/uma/v1/ws/disputed", makeWsTypeHandler(recent, "dispute"))
	// WebSocket 压测端点：回显 + 可控下行推流（仅在 WS_BENCH_ENABLE=1 时启用）
	if os.Getenv("WS_BENCH_ENABLE") == "1" {
		r.GET("/uma/v1/ws/bench", makeWsBenchHandler())
	}
	r.GET("/healthz", makeHealthzHandler(db))
	r.GET("/llms.txt", makeLLMsHandler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
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
		start := time.Now()
		// 记录请求基础信息与查询参数
		rawQuery := c.Request.URL.RawQuery
		if rawQuery != "" {
			log.Printf("[DEBUG] HTTP req begin: remote=%s method=%s path=%s query=%s",
				c.ClientIP(), c.Request.Method, c.Request.URL.Path, rawQuery)
		} else {
			log.Printf("[DEBUG] HTTP req begin: remote=%s method=%s path=%s",
				c.ClientIP(), c.Request.Method, c.Request.URL.Path)
		}
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[ERROR] HTTP handler panic: method=%s path=%s remote=%s err=%v\n%s",
					c.Request.Method, c.Request.URL.Path, c.ClientIP(), rec, string(debug.Stack()))
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
				return
			}
			log.Printf("[INFO] HTTP %s %s %s (%s)", c.ClientIP(), c.Request.Method, c.Request.URL.Path, time.Since(start))
		}()
		c.Next()
	}
}

// makeTypeHandler 返回按 event_type 查询的通用 handler。
// 当 recent 非 nil 且为 propose/dispute 且请求时间范围完全在最近 12 小时内时，优先从内存返回。
func makeTypeHandler(db *store.SQLite, recent *store.RecentCache, eventType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		fromTs, ok := requireInt64(c, "from_ts")
		if !ok {
			return
		}
		now := time.Now().Unix()
		toTs := optInt64(c, "to_ts", now)
		limit := clamp(optInt(c, "limit", 50), 1, 500)
		cursor := c.Query("cursor")

		log.Printf("[DEBUG] /uma/v1/%s query: from_ts=%d to_ts=%d limit=%d cursor=%q",
			eventType, fromTs, toTs, limit, cursor)

		const windowSec = 12 * 3600
		cutoff := now - windowSec
		var rows []store.EventRow
		var err error
		if recent != nil && (eventType == "propose" || eventType == "dispute") && fromTs >= cutoff && toTs >= cutoff {
			rows, ok = recent.Query(eventType, fromTs, toTs, limit, cursor)
			if !ok {
				rows, err = db.QueryByType(eventType, fromTs, toTs, limit, cursor)
			}
		} else {
			rows, err = db.QueryByType(eventType, fromTs, toTs, limit, cursor)
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
		log.Printf("[INFO] /uma/v1/%s respond: count=%d next_cursor=%q",
			eventType, len(data), nextCursor)
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
func makeWsTypeHandler(recent *store.RecentCache, eventType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if recent == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "recent cache not enabled"})
			return
		}
		conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("[ERROR] ws upgrade failed: path=%s remote=%s err=%v",
				c.Request.URL.Path, c.ClientIP(), err)
			return
		}
		defer conn.Close()

		ch, cancel := recent.Subscribe(eventType)
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

		// 写循环：按 send_bps 推送 payload。实现为每 10ms 发若干帧，近似目标带宽。
		if sendBps > 0 {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			bytesPerTick := sendBps / 100
			if bytesPerTick < frameBytes {
				bytesPerTick = frameBytes
			}
			for {
				select {
				case <-ticker.C:
					remain := bytesPerTick
					for remain > 0 {
						if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
							return
						}
						remain -= frameBytes
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

// makeLatestProposedHandler 返回最新 propose 事件，并附带 Polymarket 市场详情。
// 当 recent 非 nil 时优先从 12h 内存取最新一条，否则查 SQLite。
func makeLatestProposedHandler(db *store.SQLite, recent *store.RecentCache) gin.HandlerFunc {
	gammaClient := gamma.NewClient()
	return func(c *gin.Context) {
		log.Printf("[DEBUG] /uma/v1/proposed/latest query from client=%s", c.ClientIP())
		var rows []store.EventRow
		var err error
		if recent != nil {
			rows, _ = recent.QueryLatestProposed()
		}
		if len(rows) == 0 {
			rows, err = db.QueryLatestProposed(1)
			if err != nil {
				jsonError(c, http.StatusInternalServerError, err.Error())
				return
			}
		}

		// 收集所有非空 condition_id，批量拉取市场详情
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
		log.Printf("[INFO] /uma/v1/proposed/latest respond: has_result=%v", result != nil)
		jsonOK(c, result)
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

func optInt(c *gin.Context, name string, def int) int {
	if s := c.Query(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
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
