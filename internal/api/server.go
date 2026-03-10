// Package api 提供 UMA 事件查询的 HTTP 接口。
// 直接读本地 SQLite，无额外缓存层，毫秒级响应。
package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

// ListenAndServe 启动 HTTP 服务，阻塞直至错误。
func ListenAndServe(addr string, db *store.SQLite) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/uma/v1/settled",  makeSettledHandler(db))
	mux.HandleFunc("/uma/v1/disputed", makeDisputedHandler(db))
	mux.HandleFunc("/healthz",         makeHealthzHandler(db))

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	log.Printf("[INFO] HTTP API 监听 %s", addr)
	return srv.ListenAndServe()
}

// ── /uma/v1/settled ──────────────────────────────────────────────────────────

func makeSettledHandler(db *store.SQLite) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fromTs, ok := requireInt64(w, r, "from_ts")
		if !ok {
			return
		}
		toTs := optInt64(r, "to_ts", time.Now().Unix())
		limit := clamp(optInt(r, "limit", 50), 1, 500)
		cursor := r.URL.Query().Get("cursor")

		rows, err := db.QuerySettled(fromTs, toTs, limit, cursor)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, err.Error())
			return
		}

		data := make([]map[string]interface{}, 0, len(rows))
		for _, row := range rows {
			data = append(data, settledDTO(row))
		}
		nextCursor := ""
		if len(rows) == limit {
			nextCursor = rows[len(rows)-1].QuestionID
		}
		jsonOK(w, map[string]interface{}{
			"data":        data,
			"count":       len(data),
			"next_cursor": nextCursor,
		})
	}
}

func settledDTO(r store.FlowRow) map[string]interface{} {
	m := map[string]interface{}{
		"question_id":       r.QuestionID,
		"market_id":         r.MarketID,
		"condition_id":      r.ConditionID,
		"title":             r.Title,
		"phase":             r.Phase,
		"settled_price":     uma.ScalePrice(r.ResolvedSettledPrice),
		"resolved_block_ts": r.ResolvedBlockTs,
		"resolved_tx_hash":  r.ResolvedTxHash,
	}
	if r.ProposeProposer != "" {
		m["propose"] = map[string]interface{}{
			"proposer":         r.ProposeProposer,
			"proposed_price":   uma.ScalePrice(r.ProposeProposedPrice),
			"expiration_ts":    r.ProposeExpirationTs,
			"propose_block_ts": r.ProposeBlockTs,
		}
	}
	if r.DisputeTxHash != "" {
		m["dispute"] = map[string]interface{}{
			"disputer":        r.DisputeDisputer,
			"dispute_block_ts": r.DisputeBlockTs,
			"dispute_tx_hash": r.DisputeTxHash,
		}
	}
	return m
}

// ── /uma/v1/disputed ─────────────────────────────────────────────────────────

func makeDisputedHandler(db *store.SQLite) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fromTs, ok := requireInt64(w, r, "from_ts")
		if !ok {
			return
		}
		toTs := optInt64(r, "to_ts", time.Now().Unix())
		limit := clamp(optInt(r, "limit", 50), 1, 500)
		cursor := r.URL.Query().Get("cursor")

		rows, err := db.QueryDisputed(fromTs, toTs, limit, cursor)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, err.Error())
			return
		}

		data := make([]map[string]interface{}, 0, len(rows))
		for _, row := range rows {
			data = append(data, disputedDTO(row))
		}
		nextCursor := ""
		if len(rows) == limit {
			nextCursor = rows[len(rows)-1].QuestionID
		}
		jsonOK(w, map[string]interface{}{
			"data":        data,
			"count":       len(data),
			"next_cursor": nextCursor,
		})
	}
}

func disputedDTO(r store.FlowRow) map[string]interface{} {
	m := map[string]interface{}{
		"question_id":  r.QuestionID,
		"market_id":    r.MarketID,
		"condition_id": r.ConditionID,
		"title":        r.Title,
		"phase":        r.Phase,
	}
	if r.ProposeProposer != "" {
		m["propose"] = map[string]interface{}{
			"proposer":         r.ProposeProposer,
			"proposed_price":   uma.ScalePrice(r.ProposeProposedPrice),
			"expiration_ts":    r.ProposeExpirationTs,
			"propose_block_ts": r.ProposeBlockTs,
		}
	}
	m["dispute"] = map[string]interface{}{
		"disputer":             r.DisputeDisputer,
		"disputed_price":       uma.ScalePrice(r.DisputeProposedPrice),
		"dispute_block_ts":     r.DisputeBlockTs,
		"dispute_tx_hash":      r.DisputeTxHash,
	}
	return m
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
