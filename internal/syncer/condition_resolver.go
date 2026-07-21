package syncer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
	"golang.org/x/sync/singleflight"
)

const (
	marketIncrementalInterval = 5 * time.Minute
	marketIncrementalPages    = 10
)

// conditionResolver guarantees that callers never receive an empty condition_id.
type conditionResolver struct {
	db       *store.SQLite
	marketDB *store.MarketSQLite
	maintDB  *store.MaintenanceSQLite
	mem      *store.MemReplica
	proxyURL string

	mu        sync.RWMutex
	cache     map[string]string
	fetch     singleflight.Group
	pending   atomic.Int64
	oldestNS  atomic.Int64
	conflicts atomic.Int64
}

func newConditionResolver(db *store.SQLite, marketDB *store.MarketSQLite, maintDB *store.MaintenanceSQLite, mem *store.MemReplica, proxyURL string) *conditionResolver {
	var cache map[string]string
	var err error
	if marketDB != nil {
		cache, err = marketDB.LoadMarketConditionMap()
	}
	if marketDB == nil || err != nil || len(cache) == 0 {
		cache, err = db.LoadMarketConditionMap()
	}
	if err != nil {
		log.Printf("[WARN] condition resolver cache preload failed: %v", err)
		cache = make(map[string]string)
	}
	r := &conditionResolver{db: db, marketDB: marketDB, maintDB: maintDB, mem: mem, proxyURL: proxyURL, cache: cache}
	r.publishStats()
	log.Printf("[INFO] condition resolver persistent cache loaded: markets=%d", len(cache))
	return r
}

// ResolveRequired blocks only on a genuine L1/L2 miss. It never returns an empty value.
func (r *conditionResolver) ResolveRequired(ctx context.Context, marketID string) (string, error) {
	if marketID == "" {
		return "", fmt.Errorf("empty market id")
	}
	if value := r.cached(marketID); value != "" {
		return value, nil
	}
	if r.marketDB != nil {
		if value, err := r.marketDB.GetMarketConditionID(marketID); err != nil {
			log.Printf("[WARN] market primary read failed, falling back: market=%s err=%v", marketID, err)
		} else if value != "" {
			r.setCached(marketID, value)
			return value, nil
		}
	}
	if value, err := r.db.GetMarketConditionID(marketID); err != nil {
		return "", err
	} else if value != "" {
		r.setCached(marketID, value)
		return value, nil
	}

	value, err, _ := r.fetch.Do(marketID, func() (interface{}, error) {
		if cached := r.cached(marketID); cached != "" {
			return cached, nil
		}
		r.beginPending()
		defer r.endPending()
		backoff := time.Second
		for ctx.Err() == nil {
			requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			conditionID, fetchErr := uma.GammaConditionIDContext(requestCtx, marketID, r.proxyURL)
			cancel()
			if fetchErr == nil && conditionID != "" {
				if err := r.storeMapping(marketID, conditionID); err != nil {
					return nil, err
				}
				return conditionID, nil
			}
			log.Printf("[WARN] condition resolver exact lookup retry: market=%s err=%v", marketID, fetchErr)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}
		return nil, ctx.Err()
	})
	if err != nil {
		return "", err
	}
	conditionID, _ := value.(string)
	if conditionID == "" {
		return "", fmt.Errorf("condition id unresolved for market %s", marketID)
	}
	return conditionID, nil
}

func (r *conditionResolver) ResolveCached(marketID string) string {
	if value := r.cached(marketID); value != "" {
		return value
	}
	var value string
	var err error
	if r.marketDB != nil {
		value, err = r.marketDB.GetMarketConditionID(marketID)
		if err != nil {
			log.Printf("[WARN] market primary cached read failed, falling back: market=%s err=%v", marketID, err)
		}
	}
	if value == "" {
		value, err = r.db.GetMarketConditionID(marketID)
	}
	if err == nil && value != "" {
		r.setCached(marketID, value)
	}
	return value
}

func (r *conditionResolver) Prefetch(ctx context.Context, marketID string) {
	if marketID == "" || r.ResolveCached(marketID) != "" {
		return
	}
	go func() {
		if _, err := r.ResolveRequired(ctx, marketID); err != nil && ctx.Err() == nil {
			log.Printf("[WARN] condition resolver prefetch failed: market=%s err=%v", marketID, err)
		}
	}()
}

func (r *conditionResolver) Run(ctx context.Context, _ int) {
	go r.initialFullSync(ctx)
	go r.incrementalLoop(ctx)
	go r.dailyReconcileLoop(ctx)
	go r.statsLoop(ctx)
	<-ctx.Done()
}

func (r *conditionResolver) statsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.publishStats()
		}
	}
}

func (r *conditionResolver) initialFullSync(ctx context.Context) {
	for _, closed := range []bool{false, true} {
		task := fmt.Sprintf("initial_closed_%t", closed)
		for ctx.Err() == nil {
			state, err := r.db.GetMarketSyncState(task)
			if err == nil && state.Status == "complete" {
				break
			}
			if err == nil && r.fullSync(ctx, task, closed, state.NextCursor, state.ScannedCount) {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}
		}
	}
}

func (r *conditionResolver) fullSync(ctx context.Context, task string, closed bool, cursor string, scanned int64) bool {
	_ = r.db.SaveMarketSyncState(task, cursor, "running", scanned, "")
	for ctx.Err() == nil {
		pageCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		page, err := uma.FetchGammaMarketKeyset(pageCtx, r.proxyURL, cursor, closed)
		cancel()
		if err != nil {
			_ = r.db.SaveMarketSyncState(task, cursor, "error", scanned, err.Error())
			log.Printf("[WARN] market full sync failed: task=%s cursor=%s err=%v", task, cursor, err)
			return false
		}
		for _, market := range page.Markets {
			if market.ID != "" && market.ConditionID != "" {
				if err := r.storeCatalogMapping(market.ID, market.ConditionID); err != nil {
					log.Printf("[WARN] market mapping store failed: market=%s err=%v", market.ID, err)
				}
			}
		}
		scanned += int64(len(page.Markets))
		cursor = page.NextCursor
		status := "running"
		if cursor == "" {
			status = "complete"
		}
		_ = r.db.SaveMarketSyncState(task, cursor, status, scanned, "")
		log.Printf("[INFO] market full sync progress: task=%s scanned=%d cursor=%t", task, scanned, cursor != "")
		if cursor == "" {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false
}

func (r *conditionResolver) incrementalLoop(ctx context.Context) {
	r.runIncremental(ctx)
	ticker := time.NewTicker(marketIncrementalInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runIncremental(ctx)
		}
	}
}

func (r *conditionResolver) runIncremental(ctx context.Context) {
	for _, closed := range []bool{false, true} {
		knownPages := 0
		for page := 0; page < marketIncrementalPages && knownPages < 2; page++ {
			pageCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			markets, err := uma.FetchGammaRecentMarkets(pageCtx, r.proxyURL, closed, page*100)
			cancel()
			if err != nil {
				log.Printf("[WARN] market incremental sync failed: closed=%t page=%d err=%v", closed, page, err)
				break
			}
			newCount := 0
			for _, market := range markets {
				if market.ID == "" || market.ConditionID == "" {
					continue
				}
				inserted, err := r.storeCatalogMappingWithResult(market.ID, market.ConditionID)
				if err == nil && inserted {
					newCount++
				}
			}
			if newCount == 0 {
				knownPages++
			} else {
				knownPages = 0
			}
			if len(markets) < 100 {
				break
			}
		}
	}
}

func (r *conditionResolver) dailyReconcileLoop(ctx context.Context) {
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day(), 4, 0, 0, 0, now.Location())
		if !next.After(now) {
			next = next.Add(24 * time.Hour)
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			day := next.Format("20060102")
			for _, closed := range []bool{false, true} {
				_ = r.fullSync(ctx, fmt.Sprintf("reconcile_%s_closed_%t", day, closed), closed, "", 0)
			}
		}
	}
}

func (r *conditionResolver) storeMapping(marketID, conditionID string) error {
	_, conflict, err := r.db.UpsertMarketCondition(marketID, conditionID)
	if err != nil {
		return err
	}
	if conflict {
		r.conflicts.Add(1)
		r.publishStats()
		return fmt.Errorf("condition mapping conflict: market=%s", marketID)
	}
	r.setCached(marketID, conditionID)
	r.mirrorMarketMapping(marketID, conditionID)
	if err := r.db.UpdateConditionIDByMarketID(marketID, conditionID); err != nil {
		return err
	}
	if r.mem != nil {
		r.mem.UpdateConditionIDByMarketID(marketID, conditionID)
	}
	return nil
}

func (r *conditionResolver) storeCatalogMapping(marketID, conditionID string) error {
	_, err := r.storeCatalogMappingWithResult(marketID, conditionID)
	return err
}

func (r *conditionResolver) storeCatalogMappingWithResult(marketID, conditionID string) (bool, error) {
	inserted, conflict, err := r.db.UpsertMarketCondition(marketID, conditionID)
	if err != nil {
		return false, err
	}
	if conflict {
		r.conflicts.Add(1)
		r.publishStats()
		return false, fmt.Errorf("condition mapping conflict: market=%s", marketID)
	}
	r.setCached(marketID, conditionID)
	r.mirrorMarketMapping(marketID, conditionID)
	return inserted, nil
}

func (r *conditionResolver) mirrorMarketMapping(marketID, conditionID string) {
	if r.marketDB != nil {
		_, conflict, err := r.marketDB.UpsertMarketCondition(marketID, conditionID)
		if err != nil || conflict {
			log.Printf("[WARN] market mirror write failed: market=%s conflict=%t err=%v", marketID, conflict, err)
		}
	}
	if r.maintDB != nil {
		if err := r.maintDB.FillConditionByMarketID(marketID, conditionID); err != nil {
			log.Printf("[WARN] question mirror fill failed: market=%s err=%v", marketID, err)
		}
	}
}

func (r *conditionResolver) TrackQuestion(questionID, conditionID, marketID, txHash string) {
	if r.maintDB == nil || questionID == "" {
		return
	}
	// The legacy fallback uses questionID itself when the Gamma mapping is not ready.
	if conditionID == questionID {
		conditionID = ""
	}
	conflict, err := r.maintDB.UpsertQuestionMapping(questionID, conditionID, marketID, txHash)
	if err != nil || conflict {
		log.Printf("[WARN] question mirror write failed: question=%s market=%s conflict=%t err=%v", questionID, marketID, conflict, err)
	}
}

func (r *conditionResolver) cached(marketID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cache[marketID]
}
func (r *conditionResolver) setCached(marketID, conditionID string) {
	r.mu.Lock()
	r.cache[marketID] = conditionID
	r.mu.Unlock()
	r.publishStats()
}
func (r *conditionResolver) beginPending() {
	if r.pending.Add(1) == 1 {
		r.oldestNS.Store(time.Now().UnixNano())
	}
	r.publishStats()
}
func (r *conditionResolver) endPending() {
	if r.pending.Add(-1) == 0 {
		r.oldestNS.Store(0)
	}
	r.publishStats()
}
func (r *conditionResolver) publishStats() {
	r.mu.RLock()
	mappings := int64(len(r.cache))
	r.mu.RUnlock()
	oldestMS := int64(0)
	if ns := r.oldestNS.Load(); ns > 0 {
		oldestMS = time.Since(time.Unix(0, ns)).Milliseconds()
	}
	r.db.SetMarketSyncStats(mappings, r.pending.Load(), oldestMS, r.conflicts.Load())
}
