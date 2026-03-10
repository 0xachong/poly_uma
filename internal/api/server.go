// Package api 提供 UMA 事件查询的 HTTP 接口。
// 直接读本地 SQLite uma_oo_events 表，无额外缓存层，毫秒级响应。
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/polymas/go-polymarket-sdk/gamma"
	"github.com/polymas/poly_uma/internal/store"
)

//go:embed llms.txt
var llmsTxt []byte

// ListenAndServe 启动 HTTP 服务，ctx 取消时优雅关闭，阻塞直至退出。
func ListenAndServe(ctx context.Context, addr string, db *store.SQLite) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/uma/v1/settled",  makeTypeHandler(db, "resolved"))
	mux.HandleFunc("/uma/v1/disputed", makeTypeHandler(db, "dispute"))
	mux.HandleFunc("/uma/v1/proposed",        makeTypeHandler(db, "propose"))
	mux.HandleFunc("/uma/v1/proposed/latest", makeLatestProposedHandler(db))
	mux.HandleFunc("/healthz",                makeHealthzHandler(db))
	mux.HandleFunc("/llms.txt",               makeLLMsHandler())

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
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

// makeTypeHandler 返回按 event_type 查询的通用 handler。
func makeTypeHandler(db *store.SQLite, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fromTs, ok := requireInt64(w, r, "from_ts")
		if !ok {
			return
		}
		toTs := optInt64(r, "to_ts", time.Now().Unix())
		limit := clamp(optInt(r, "limit", 50), 1, 500)
		cursor := r.URL.Query().Get("cursor")

		rows, err := db.QueryByType(eventType, fromTs, toTs, limit, cursor)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, err.Error())
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
	return map[string]interface{}{
		"event_type":       r.EventType,
		"transaction_hash": r.TxHash,
		"log_index":        r.LogIndex,
		"block_number":     r.BlockNumber,
		"timestamp":        r.Timestamp,
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
func makeLatestProposedHandler(db *store.SQLite) http.HandlerFunc {
	gammaClient := gamma.NewClient()
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.QueryLatestProposed(1)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, err.Error())
			return
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
						"market_id":              m.MarketID,
						"slug":                   m.Slug,
						"question":               m.Question,
						"description":            m.Description,
						"end_date":               m.EndDate,
						"liquidity":              m.Liquidity,
						"volume":                 m.Volume,
						"uma_resolution_status":  m.UmaResolutionStatus,
						"uma_end_date":           m.UmaEndDate,
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
