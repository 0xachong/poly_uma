// Package api 提供 UMA 事件查询的 HTTP 接口。
// 默认列表与 /proposed/latest 读最近 2h 的 MemReplica；传 source=sqlite 时读 SQLite 全量历史。
package api

import (
	"container/list"
	"context"
	_ "embed"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/polymas/go-polymarket-sdk/gamma"
	"github.com/polymas/poly_uma/internal/store"
	"golang.org/x/sync/singleflight"
)

//go:embed llms.txt
var llmsTxt []byte

// ListenAndServe 启动 HTTP 服务，ctx 取消时优雅关闭，阻塞直至退出。
// mem 为最近 2h 内存副本：默认列表走 mem；source=sqlite 走 db；mem 为 nil 时默认路径返回 503。
func ListenAndServe(ctx context.Context, addr string, db *store.SQLite, mem *store.MemReplica) error {
	// 单独的 Gin HTTP 日志文件（默认 gin-http.log，可通过 GIN_LOG_FILE 覆盖）
	// 实际按天切片，文件名如 gin-http-2026-03-23.log，并自动清理 3 天前及更早分片。
	ginLogFile := os.Getenv("GIN_LOG_FILE")
	if ginLogFile == "" {
		ginLogFile = "gin-http.log"
	}
	ginLogWriter, err := newDailyShardWriter(ginLogFile, 3)
	if err != nil {
		log.Printf("[WARN] 打开 Gin 日志文件失败（使用默认输出）: %v", err)
	} else {
		gin.DefaultWriter = ginLogWriter
		gin.DefaultErrorWriter = ginLogWriter
		defer ginLogWriter.Close()
	}

	r := gin.New()
	// Gin 默认格式 HTTP 日志 + 自定义 panic 恢复与业务日志
	r.Use(
		gin.Logger(),
		recoverAndLog(),
		inflightLimitMiddleware(httpMaxInflight()),
		ipRateLimitMiddleware(newIPLimiterStore(httpIPRate(), httpIPBurst())),
	)

	r.GET("/uma/v1/settled", makeTypeHandler(db, mem, "resolved"))
	r.GET("/uma/v1/disputed", makeTypeHandler(db, mem, "dispute"))
	r.GET("/uma/v1/proposed", makeTypeHandler(db, mem, "propose"))
	r.GET("/uma/v1/proposed/latest", makeLatestProposedHandler(db, mem))
	r.GET("/uma/v1/events", makeLookupHandler(db))
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

// inflightLimitMiddleware 控制 HTTP 同时在处理中的请求数。
// 超过阈值时快速返回 503，避免 goroutine 无上限堆积造成雪崩。
func inflightLimitMiddleware(maxInflight int) gin.HandlerFunc {
	if maxInflight <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	sem := make(chan struct{}, maxInflight)
	return func(c *gin.Context) {
		if isWSPath(c.Request.URL.Path) {
			// WebSocket 是长连接，不参与短请求并发闸门。
			c.Next()
			return
		}
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			c.Next()
		default:
			jsonError(c, http.StatusServiceUnavailable, "service temporarily unavailable: too many in-flight requests")
		}
	}
}

// ipRateLimitMiddleware 按客户端 IP 做令牌桶限速，超限返回 429。
func ipRateLimitMiddleware(store *ipLimiterStore) gin.HandlerFunc {
	if store == nil || store.rate <= 0 || store.burst <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		if isWSPath(c.Request.URL.Path) {
			c.Next()
			return
		}
		if !store.allow(c.ClientIP(), time.Now()) {
			jsonError(c, http.StatusTooManyRequests, "too many requests")
			return
		}
		c.Next()
	}
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

// makeTypeHandler 列表查询：默认 source=memory（仅最近 2h 内存）；source=sqlite 读库全量。
// 使用 singleflight + 短 TTL 缓存：相同参数的并发请求只查一次，减少锁争用。
func makeTypeHandler(db *store.SQLite, mem *store.MemReplica, eventType string) gin.HandlerFunc {
	var sfGroup singleflight.Group
	cache := newLatestProposedCache(typeCacheTTL())
	return func(c *gin.Context) {
		useMemory, srcErr := parseQuerySource(c)
		if srcErr != "" {
			jsonError(c, http.StatusBadRequest, srcErr)
			return
		}
		// from_ts 现在可选：有 cursor 时不需要 from_ts
		fromTs, ok := optInt64Query(c, "from_ts", 0)
		if !ok {
			return
		}
		toTs, ok := optInt64Query(c, "to_ts", 0)
		if !ok {
			return
		}
		limit, ok := optIntQuery(c, "limit", 50)
		if !ok {
			return
		}
		limit = clamp(limit, 1, 500)
		cursor, ok := optInt64Query(c, "cursor", 0)
		if !ok {
			return
		}

		if useMemory {
			if mem == nil {
				jsonError(c, http.StatusServiceUnavailable, "memory replica not available")
				return
			}
			// 无 cursor 且无 from_ts 时，默认从 2h 窗口起点开始
			if cursor == 0 && fromTs == 0 {
				fromTs = store.RecentMemoryCutoffUnix()
			}
		}
		if !useMemory && db == nil {
			jsonError(c, http.StatusServiceUnavailable, "sqlite not available")
			return
		}

		// 缓存 key
		src := "m"
		if !useMemory {
			src = "s"
		}
		cacheKey := fmt.Sprintf("%s:%s:%d:%d:%d:%d", eventType, src, fromTs, toTs, limit, cursor)

		if cached, hit := cache.get(cacheKey); hit {
			jsonOK(c, cached)
			return
		}

		// singleflight：相同参数的并发请求只执行一次查询
		type sfResult struct {
			payload interface{}
			errText string
			code    int
		}
		result, _, _ := sfGroup.Do(cacheKey, func() (interface{}, error) {
			var rows []store.EventRow
			var err error
			if useMemory {
				rows = mem.QueryByType(eventType, fromTs, toTs, limit, cursor)
			} else {
				rows, err = db.QueryByType(eventType, fromTs, toTs, limit, cursor)
			}
			if err != nil {
				return &sfResult{errText: fmt.Sprintf("query db failed: %v", err), code: http.StatusInternalServerError}, nil
			}
			data := make([]map[string]interface{}, 0, len(rows))
			for _, row := range rows {
				data = append(data, eventDTO(row))
			}
			nextCursor := ""
			if len(rows) == limit {
				nextCursor = strconv.FormatInt(rows[len(rows)-1].CursorID, 10)
			}
			payload := map[string]interface{}{
				"data":        data,
				"count":       len(data),
				"next_cursor": nextCursor,
			}
			cache.set(cacheKey, payload)
			return &sfResult{payload: payload}, nil
		})

		sr := result.(*sfResult)
		if sr.errText != "" {
			jsonError(c, sr.code, sr.errText)
			return
		}
		jsonOK(c, sr.payload)
	}
}

func eventDTO(r store.EventRow) map[string]interface{} {
	cst := time.FixedZone("UTC+8", 8*3600)
	timeStr := time.Unix(r.Timestamp, 0).In(cst).Format("2006-01-02 15:04:05")
	return map[string]interface{}{
		"id":               r.ID,
		"cursor_id":        r.CursorID,
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

// ── /uma/v1/events：按 condition_id 或 transaction_hash 查询 ─────────────────

// makeLookupHandler 按 condition_id 和/或 transaction_hash 从 SQLite 查询事件。
// 始终走 SQLite（全量历史），不受 2h 内存窗口限制。至少传一个检索参数。
// 结果缓存在容量受限的 LRU（默认 2048 键，TTL 30s）中，并用 singleflight 合并同 key 并发。
func makeLookupHandler(db *store.SQLite) gin.HandlerFunc {
	cache := newLookupLRU(lookupCacheCapacity(), lookupCacheTTL())
	var sfGroup singleflight.Group
	return func(c *gin.Context) {
		if db == nil {
			jsonError(c, http.StatusServiceUnavailable, "sqlite not available")
			return
		}
		conditionID := strings.TrimSpace(c.Query("condition_id"))
		txHash := strings.TrimSpace(c.Query("transaction_hash"))
		if conditionID == "" && txHash == "" {
			jsonError(c, http.StatusBadRequest, "condition_id 或 transaction_hash 至少传一个")
			return
		}
		eventType := strings.ToLower(strings.TrimSpace(c.Query("event_type")))
		if eventType != "" && !isValidEventType(eventType) {
			jsonError(c, http.StatusBadRequest, "event_type 须为 init/request/propose/dispute/resolved/settle 之一")
			return
		}
		limit, ok := optIntQuery(c, "limit", 100)
		if !ok {
			return
		}
		limit = clamp(limit, 1, 500)

		cacheKey := conditionID + "|" + txHash + "|" + eventType + "|" + strconv.Itoa(limit)
		if cached, hit := cache.get(cacheKey); hit {
			jsonOK(c, cached)
			return
		}

		type sfResult struct {
			payload interface{}
			errText string
			code    int
		}
		result, _, _ := sfGroup.Do(cacheKey, func() (interface{}, error) {
			rows, err := db.QueryByLookup(conditionID, txHash, eventType, limit)
			if err != nil {
				return &sfResult{errText: fmt.Sprintf("query db failed: %v", err), code: http.StatusInternalServerError}, nil
			}
			data := make([]map[string]interface{}, 0, len(rows))
			for _, row := range rows {
				data = append(data, eventDTO(row))
			}
			payload := map[string]interface{}{
				"data":  data,
				"count": len(data),
			}
			cache.set(cacheKey, payload)
			return &sfResult{payload: payload}, nil
		})
		sr := result.(*sfResult)
		if sr.errText != "" {
			jsonError(c, sr.code, sr.errText)
			return
		}
		jsonOK(c, sr.payload)
	}
}

// lookupLRU 是 /uma/v1/events 专用的容量受限 LRU + TTL 缓存。
// TTL 必要：事件是追加写入的，condition 的事件集合可能在 init→propose→resolved 之间演进。
type lookupLRU struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int
	ll       *list.List
	items    map[string]*list.Element
}

type lookupLRUItem struct {
	key     string
	payload interface{}
	expire  time.Time
}

func newLookupLRU(capacity int, ttl time.Duration) *lookupLRU {
	if capacity <= 0 {
		capacity = 2048
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &lookupLRU{
		ttl:      ttl,
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

func (c *lookupLRU) get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	it := el.Value.(*lookupLRUItem)
	if time.Now().After(it.expire) {
		c.ll.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return it.payload, true
}

func (c *lookupLRU) set(key string, payload interface{}) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		it := el.Value.(*lookupLRUItem)
		it.payload = payload
		it.expire = now.Add(c.ttl)
		return
	}
	el := c.ll.PushFront(&lookupLRUItem{
		key:     key,
		payload: payload,
		expire:  now.Add(c.ttl),
	})
	c.items[key] = el
	if c.ll.Len() > c.capacity {
		if tail := c.ll.Back(); tail != nil {
			c.ll.Remove(tail)
			delete(c.items, tail.Value.(*lookupLRUItem).key)
		}
	}
}

func lookupCacheCapacity() int {
	return envInt("HTTP_LOOKUP_CACHE_SIZE", 2048)
}

func lookupCacheTTL() time.Duration {
	return envDuration("HTTP_LOOKUP_CACHE_TTL", 30*time.Second)
}

func isValidEventType(t string) bool {
	switch t {
	case "init", "request", "propose", "dispute", "resolved", "settle":
		return true
	}
	return false
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

// makeLatestProposedHandler 返回最新 propose；默认 source=memory（2h 内），source=sqlite 读库。
// 使用 singleflight 合并并发请求，避免同时向 Gamma API 发起大量相同调用。
func makeLatestProposedHandler(db *store.SQLite, mem *store.MemReplica) gin.HandlerFunc {
	gammaClient := gamma.NewClient()
	cache := newLatestProposedCache(latestCacheTTL())
	var sfGroup singleflight.Group
	return func(c *gin.Context) {
		useMemory, srcErr := parseQuerySource(c)
		if srcErr != "" {
			jsonError(c, http.StatusBadRequest, srcErr)
			return
		}
		cacheKey := "memory"
		if !useMemory {
			cacheKey = "sqlite"
		}
		if cached, ok := cache.get(cacheKey); ok {
			jsonOK(c, cached)
			return
		}

		if useMemory && mem == nil {
			jsonError(c, http.StatusServiceUnavailable, "memory replica not available")
			return
		}
		if !useMemory && db == nil {
			jsonError(c, http.StatusServiceUnavailable, "sqlite not available")
			return
		}

		type sfResult struct {
			payload interface{}
			errText string
			code    int
		}

		result, _, _ := sfGroup.Do(cacheKey, func() (interface{}, error) {
			var rows []store.EventRow
			var err error
			if useMemory {
				rows = mem.QueryLatestProposed(1)
			} else {
				rows, err = db.QueryLatestProposed(1)
				if err != nil {
					return &sfResult{errText: err.Error(), code: http.StatusInternalServerError}, nil
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

			var payload interface{}
			if len(data) > 0 {
				payload = data[0]
			}
			cache.set(cacheKey, payload)
			return &sfResult{payload: payload}, nil
		})

		sr := result.(*sfResult)
		if sr.errText != "" {
			jsonError(c, sr.code, sr.errText)
			return
		}
		jsonOK(c, sr.payload)
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

func httpMaxInflight() int {
	return envInt("HTTP_MAX_INFLIGHT", 300)
}

func httpIPRate() float64 {
	return envFloat64("HTTP_IP_RATE", 60)
}

func httpIPBurst() float64 {
	return envFloat64("HTTP_IP_BURST", 120)
}

func latestCacheTTL() time.Duration {
	return envDuration("HTTP_LATEST_CACHE_TTL", 300*time.Millisecond)
}

// typeCacheTTL 列表查询缓存 TTL（默认 2s），用于 singleflight 之后短暂缓存结果。
func typeCacheTTL() time.Duration {
	return envDuration("HTTP_TYPE_CACHE_TTL", 2*time.Second)
}

func envInt(key string, def int) int {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func envFloat64(key string, def float64) float64 {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func envDuration(key string, def time.Duration) time.Duration {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	v, err := time.ParseDuration(s)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func isWSPath(path string) bool {
	return strings.HasPrefix(path, "/uma/v1/ws/")
}

type ipLimiter struct {
	tokens   float64
	last     time.Time
	lastSeen time.Time
}

type ipLimiterStore struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	items    map[string]*ipLimiter
	lastGCAt time.Time
}

func newIPLimiterStore(rate, burst float64) *ipLimiterStore {
	if rate <= 0 || burst <= 0 {
		return nil
	}
	s := &ipLimiterStore{
		rate:  rate,
		burst: burst,
		items: make(map[string]*ipLimiter),
	}
	// 后台主动 GC，不依赖请求触发，防止流量停止后残留条目无法释放
	go s.runGC()
	return s
}

func (s *ipLimiterStore) runGC() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for now := range ticker.C {
		s.mu.Lock()
		s.gcLocked(now)
		s.lastGCAt = now
		s.mu.Unlock()
	}
}

func (s *ipLimiterStore) allow(ip string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if now.Sub(s.lastGCAt) >= time.Minute {
		s.gcLocked(now)
		s.lastGCAt = now
	}

	l, ok := s.items[ip]
	if !ok {
		// 首次请求默认放行一次。
		s.items[ip] = &ipLimiter{
			tokens:   math.Max(s.burst-1, 0),
			last:     now,
			lastSeen: now,
		}
		return true
	}
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens = math.Min(s.burst, l.tokens+elapsed*s.rate)
	}
	l.last = now
	l.lastSeen = now
	if l.tokens < 1 {
		return false
	}
	l.tokens -= 1
	return true
}

func (s *ipLimiterStore) gcLocked(now time.Time) {
	expireBefore := now.Add(-10 * time.Minute)
	for ip, l := range s.items {
		if l.lastSeen.Before(expireBefore) {
			delete(s.items, ip)
		}
	}
}

type latestProposedCache struct {
	mu    sync.RWMutex
	ttl   time.Duration
	items map[string]cachedPayload
}

type cachedPayload struct {
	payload interface{}
	expire  time.Time
}

func newLatestProposedCache(ttl time.Duration) *latestProposedCache {
	if ttl <= 0 {
		ttl = 300 * time.Millisecond
	}
	return &latestProposedCache{
		ttl:   ttl,
		items: make(map[string]cachedPayload),
	}
}

func (c *latestProposedCache) get(key string) (interface{}, bool) {
	now := time.Now()
	c.mu.RLock()
	it, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if now.After(it.expire) {
		c.mu.Lock()
		// double-check：可能已被其他 goroutine 更新
		if it2, ok2 := c.items[key]; ok2 && now.After(it2.expire) {
			delete(c.items, key)
		}
		c.mu.Unlock()
		return nil, false
	}
	return it.payload, true
}

func (c *latestProposedCache) set(key string, payload interface{}) {
	c.mu.Lock()
	c.items[key] = cachedPayload{
		payload: payload,
		expire:  time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// dailyShardWriter 将日志写入按天分片文件，并在每日轮转时清理过期分片。
type dailyShardWriter struct {
	mu          sync.Mutex
	basePath    string
	dir         string
	baseName    string
	ext         string
	keepDays    int
	currentDate string
	file        *os.File
}

func newDailyShardWriter(basePath string, keepDays int) (io.WriteCloser, error) {
	w := &dailyShardWriter{
		basePath: basePath,
		keepDays: keepDays,
	}
	if err := w.init(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *dailyShardWriter) init() error {
	absPath, err := filepath.Abs(w.basePath)
	if err != nil {
		return err
	}
	w.dir = filepath.Dir(absPath)
	filename := filepath.Base(absPath)
	w.ext = filepath.Ext(filename)
	w.baseName = strings.TrimSuffix(filename, w.ext)
	if w.baseName == "" {
		w.baseName = "gin-http"
	}
	return w.rotateIfNeededLocked()
}

func (w *dailyShardWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateIfNeededLocked(); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}

func (w *dailyShardWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *dailyShardWriter) rotateIfNeededLocked() error {
	today := time.Now().Format("2006-01-02")
	if w.file != nil && w.currentDate == today {
		return nil
	}

	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}

	filePath := w.shardPath(today)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.currentDate = today

	if err := w.cleanupExpiredLocked(); err != nil {
		log.Printf("[WARN] 清理 Gin 历史日志分片失败: %v", err)
	}
	return nil
}

func (w *dailyShardWriter) shardPath(date string) string {
	filename := fmt.Sprintf("%s-%s%s", w.baseName, date, w.ext)
	return filepath.Join(w.dir, filename)
}

func (w *dailyShardWriter) cleanupExpiredLocked() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	// 删除 3 天前及更早：例如今天 23 号，阈值是 20 号，<=20 的分片会被清理。
	threshold := time.Now().Truncate(24 * time.Hour).AddDate(0, 0, -w.keepDays)
	prefix := w.baseName + "-"
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, w.ext) {
			continue
		}
		datePart := strings.TrimSuffix(strings.TrimPrefix(name, prefix), w.ext)
		d, err := time.Parse("2006-01-02", datePart)
		if err != nil || d.After(threshold) {
			continue
		}
		if removeErr := os.Remove(filepath.Join(w.dir, name)); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Printf("[WARN] 删除 Gin 历史日志分片失败: file=%s err=%v", name, removeErr)
		}
	}
	return nil
}
