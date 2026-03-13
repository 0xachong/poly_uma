// Package api 提供 UMA 事件查询的 HTTP 接口。
// 直接读本地 SQLite uma_oo_events 表，无额外缓存层，毫秒级响应。
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/polymas/go-polymarket-sdk/gamma"
	"github.com/polymas/poly_uma/internal/store"
)

//go:embed llms.txt
var llmsTxt []byte

// ListenAndServe 启动 HTTP 服务，ctx 取消时优雅关闭，阻塞直至退出。
// recent 可选：非 nil 时，propose/dispute 在最近 12h 内优先从内存返回，否则查 SQLite。
func ListenAndServe(ctx context.Context, addr string, db *store.SQLite, recent *store.RecentCache) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/uma/v1/settled", makeTypeHandler(db, recent, "resolved"))
	mux.HandleFunc("/uma/v1/disputed", makeTypeHandler(db, recent, "dispute"))
	mux.HandleFunc("/uma/v1/proposed", makeTypeHandler(db, recent, "propose"))
	mux.HandleFunc("/uma/v1/proposed/latest", makeLatestProposedHandler(db, recent))
	mux.HandleFunc("/healthz", makeHealthzHandler(db))
	mux.HandleFunc("/llms.txt", makeLLMsHandler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           recoverAndLog(mux),
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

func recoverAndLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[ERROR] HTTP handler panic: method=%s path=%s remote=%s err=%v\n%s",
					r.Method, r.URL.Path, r.RemoteAddr, rec, string(debug.Stack()))
				jsonError(w, http.StatusInternalServerError, "internal server error")
				return
			}
			log.Printf("[INFO] HTTP %s %s %s (%s)", r.RemoteAddr, r.Method, r.URL.Path, time.Since(start))
		}()
		next.ServeHTTP(w, r)
	})
}

// makeTypeHandler 返回按 event_type 查询的通用 handler。
// 当 recent 非 nil 且为 propose/dispute 且请求时间范围完全在最近 12 小时内时，优先从内存返回。
func makeTypeHandler(db *store.SQLite, recent *store.RecentCache, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fromTs, ok := requireInt64(w, r, "from_ts")
		if !ok {
			return
		}
		now := time.Now().Unix()
		toTs := optInt64(r, "to_ts", now)
		limit := clamp(optInt(r, "limit", 50), 1, 500)
		cursor := r.URL.Query().Get("cursor")

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
			jsonError(w, http.StatusInternalServerError, fmt.Sprintf("query db failed: %v", err))
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
		jsonOK(w, map[string]interface{}{
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

// ── /llms.txt ────────────────────────────────────────────────────────────────

func makeLLMsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write(llmsTxt)
	}
}

// ── /uma/v1/proposed/latest ───────────────────────────────────────────────────

// makeLatestProposedHandler 返回最新 propose 事件，并附带 Polymarket 市场详情。
// 当 recent 非 nil 时优先从 12h 内存取最新一条，否则查 SQLite。
func makeLatestProposedHandler(db *store.SQLite, recent *store.RecentCache) http.HandlerFunc {
	gammaClient := gamma.NewClient()
	return func(w http.ResponseWriter, r *http.Request) {
		var rows []store.EventRow
		var err error
		if recent != nil {
			rows, _ = recent.QueryLatestProposed()
		}
		if len(rows) == 0 {
			rows, err = db.QueryLatestProposed(1)
			if err != nil {
				jsonError(w, http.StatusInternalServerError, err.Error())
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
		jsonOK(w, result)
	}
}

// ── /healthz ─────────────────────────────────────────────────────────────────

func makeHealthzHandler(db *store.SQLite) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		jsonOK(w, map[string]interface{}{
			"status":                status,
			"syncer_lag_blocks":     lag,
			"last_checkpoint_block": checkpoint,
			"latest_seen_block":     latest,
		})
	}
}

// ── 工具函数 ──────────────────────────────────────────────────────────────────

func requireInt64(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	s := r.URL.Query().Get(name)
	if s == "" {
		jsonError(w, http.StatusBadRequest, name+" 为必填参数")
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		jsonError(w, http.StatusBadRequest, name+" 必须为整数")
		return 0, false
	}
	return v, true
}

func optInt64(r *http.Request, name string, def int64) int64 {
	if s := r.URL.Query().Get(name); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
	}
	return def
}

func optInt(r *http.Request, name string, def int) int {
	if s := r.URL.Query().Get(name); s != "" {
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

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
