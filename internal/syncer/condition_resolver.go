package syncer

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"runtime"
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
	marketCacheLimit          = 100000
	closedMarketGrace         = 24 * time.Hour
	marketReconcileInterval   = 5 * time.Second
	activeReconcileCycle      = 24 * time.Hour
	closedReconcileCycle      = 7 * 24 * time.Hour
	reconcileHeapPauseBytes   = 550 << 20
)

type conditionCacheEntry struct {
	marketID    string
	conditionID string
}

// conditionResolver guarantees that callers never receive an empty condition_id.
type conditionResolver struct {
	db       *store.SQLite
	marketDB *store.MarketSQLite
	maintDB  *store.MaintenanceSQLite
	mem      *store.MemReplica
	proxyURL string

	mu               sync.RWMutex
	cache            map[string]*list.Element
	cacheLRU         *list.List
	fetch            singleflight.Group
	pending          atomic.Int64
	oldestNS         atomic.Int64
	conflicts        atomic.Int64
	wake             chan struct{}
	deliveryMu       sync.Mutex
	deliveryInflight map[string]struct{}
	deliverySlots    chan struct{}
}

func newConditionResolver(db *store.SQLite, marketDB *store.MarketSQLite, maintDB *store.MaintenanceSQLite, mem *store.MemReplica, proxyURL string) *conditionResolver {
	preload := make(map[string]string)
	var err error
	if marketDB != nil {
		preload, err = marketDB.LoadActiveMarketConditionMap(marketCacheLimit)
	}
	if err != nil {
		log.Printf("[WARN] condition resolver cache preload failed: %v", err)
		preload = make(map[string]string)
	}
	r := &conditionResolver{
		db: db, marketDB: marketDB, maintDB: maintDB, mem: mem, proxyURL: proxyURL,
		cache: make(map[string]*list.Element, len(preload)), cacheLRU: list.New(), wake: make(chan struct{}, 1),
		deliveryInflight: make(map[string]struct{}), deliverySlots: make(chan struct{}, 16),
	}
	for marketID, conditionID := range preload {
		r.setCached(marketID, conditionID)
	}
	r.publishStats()
	log.Printf("[INFO] condition resolver active cache loaded: markets=%d limit=%d", len(preload), marketCacheLimit)
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
			return "", fmt.Errorf("market mapping read: %w", err)
		} else if value != "" {
			r.setCached(marketID, value)
			return value, nil
		}
	} else {
		// Compatibility for tests and explicit single-DB rollback mode. Normal
		// production startup always supplies MarketDB.
		if value, err := r.db.GetMarketConditionID(marketID); err != nil {
			return "", err
		} else if value != "" {
			r.setCached(marketID, value)
			return value, nil
		}
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
			log.Printf("[WARN] market mapping read failed: market=%s err=%v", marketID, err)
		}
	} else {
		value, err = r.db.GetMarketConditionID(marketID)
	}
	if err == nil && value != "" {
		r.setCached(marketID, value)
	}
	return value
}

func (r *conditionResolver) ResolveQuestion(questionID string) (string, error) {
	if r.maintDB != nil {
		conditionID, err := r.maintDB.GetQuestionConditionID(questionID)
		if err != nil {
			log.Printf("[WARN] question primary read failed, falling back: question=%s err=%v", questionID, err)
		} else if conditionID != "" {
			return conditionID, nil
		}
	}
	return r.db.GetConditionIDByQuestionID(questionID)
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
	go r.incrementalLoop(ctx)
	go r.rollingReconcileLoop(ctx)
	go r.statsLoop(ctx)
	go r.pendingDeliveryLoop(ctx)
	<-ctx.Done()
}

func (r *conditionResolver) WakePending() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *conditionResolver) pendingDeliveryLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.wake:
		}
		r.drainPendingDeliveries(ctx)
	}
}

func (r *conditionResolver) drainPendingDeliveries(ctx context.Context) {
	pending, err := r.db.ListPendingMarketDeliveries(1000)
	if err != nil {
		log.Printf("[WARN] list pending market deliveries: %v", err)
		return
	}
	for _, item := range pending {
		item := item
		key := fmt.Sprintf("%s:%d", item.TxHash, item.LogIndex)
		r.deliveryMu.Lock()
		if _, exists := r.deliveryInflight[key]; exists {
			r.deliveryMu.Unlock()
			continue
		}
		r.deliveryInflight[key] = struct{}{}
		r.deliveryMu.Unlock()
		go func() {
			select {
			case r.deliverySlots <- struct{}{}:
				defer func() { <-r.deliverySlots }()
			case <-ctx.Done():
				r.finishPendingDelivery(key)
				return
			}
			defer r.finishPendingDelivery(key)
			r.completePendingDelivery(ctx, item)
		}()
	}
}

func (r *conditionResolver) finishPendingDelivery(key string) {
	r.deliveryMu.Lock()
	delete(r.deliveryInflight, key)
	r.deliveryMu.Unlock()
}

func (r *conditionResolver) completePendingDelivery(ctx context.Context, item store.PendingMarketDelivery) {
	conditionID := item.ConditionID
	if conditionID == "" {
		var err error
		resolveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conditionID, err = r.ResolveRequired(resolveCtx, item.MarketID)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("[WARN] resolve pending delivery: market=%s tx=%s err=%v", item.MarketID, item.TxHash, err)
			}
			return
		}
	}
	// ResolveRequired may have hit a mapping inserted by the catalog sync, whose
	// fast path does not rewrite event rows. Make the pending row durable and
	// visible to HTTP before broadcasting it.
	if err := r.db.UpdateConditionIDByMarketID(item.MarketID, conditionID); err != nil {
		log.Printf("[WARN] update pending delivery mapping: market=%s tx=%s err=%v", item.MarketID, item.TxHash, err)
		return
	}
	if r.mem != nil {
		r.mem.UpdateConditionIDByMarketID(item.MarketID, conditionID)
	}
	item.ConditionID = conditionID
	item.Source = "delayed_replay"
	item.EventRow.UpstreamReceivedAtMS = item.UpstreamReceivedAtMS
	if r.mem != nil {
		r.mem.BroadcastNew(item.EventType, item.EventRow)
	}
	broadcastAt := time.Now()
	r.db.MarkEventBroadcast(broadcastAt)
	if item.UpstreamReceivedAtMS > 0 {
		r.db.ObserveBroadcastDelay(time.Duration(broadcastAt.UnixMilli()-item.UpstreamReceivedAtMS) * time.Millisecond)
	}
	if err := r.db.DeletePendingMarketDelivery(item.TxHash, item.LogIndex); err != nil {
		log.Printf("[WARN] delete pending market delivery: tx=%s index=%d err=%v", item.TxHash, item.LogIndex, err)
	}
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
				if closed {
					market.Closed = true
				}
				inserted, err := r.storeCatalogMappingWithResult(market)
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

func (r *conditionResolver) rollingReconcileLoop(ctx context.Context) {
	if r.marketDB == nil {
		log.Printf("[WARN] market rolling reconcile disabled: MarketDB unavailable")
		return
	}
	// Recent markets are populated first by incrementalLoop. The catalog then
	// advances only one keyset page per tick and can yield to realtime work.
	ticker := time.NewTicker(marketReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileOnePage(ctx)
		}
	}
}

func (r *conditionResolver) reconcileOnePage(ctx context.Context) {
	stats := r.db.PipelineStats()
	if stats.QueueDepth > 0 || stats.Processing > 0 {
		r.db.SetMarketReconcileStats(stats.MarketReconcileClosed, stats.MarketReconcileScanned, 0, true)
		return
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	if memory.HeapAlloc >= reconcileHeapPauseBytes {
		r.db.SetMarketReconcileStats(stats.MarketReconcileClosed, stats.MarketReconcileScanned, 0, true)
		log.Printf("[WARN] market reconcile paused: heap_alloc_mb=%d threshold_mb=%d",
			memory.HeapAlloc>>20, reconcileHeapPauseBytes>>20)
		return
	}

	closed, state, ok := r.nextReconcileTask()
	if !ok {
		r.db.SetMarketReconcileStats(false, 0, 0, false)
		return
	}
	task := reconcileTaskName(closed)
	pageCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	page, err := uma.FetchGammaMarketKeyset(pageCtx, r.proxyURL, state.NextCursor, closed)
	cancel()
	if err != nil {
		_ = r.marketDB.SaveMarketSyncState(task, state.NextCursor, "error", state.ScannedCount, err.Error())
		log.Printf("[WARN] market rolling reconcile failed: task=%s cursor=%s err=%v", task, state.NextCursor, err)
		return
	}
	records := make([]store.MarketCatalogRecord, 0, len(page.Markets))
	for _, market := range page.Markets {
		if market.ID == "" || market.ConditionID == "" {
			continue
		}
		if closed {
			market.Closed = true
		}
		records = append(records, store.MarketCatalogRecord{
			MarketID: market.ID, ConditionID: market.ConditionID,
			Active: market.Active, Closed: market.Closed, ClosedAt: gammaClosedAt(market.ClosedTime),
		})
	}
	_, conflicts, err := r.marketDB.UpsertMarketCatalogBatch(records)
	if err != nil {
		_ = r.marketDB.SaveMarketSyncState(task, state.NextCursor, "error", state.ScannedCount, err.Error())
		log.Printf("[WARN] market rolling reconcile store failed: task=%s err=%v", task, err)
		return
	}
	if conflicts > 0 {
		r.conflicts.Add(conflicts)
	}
	scanned := state.ScannedCount + int64(len(page.Markets))
	status := "running"
	if page.NextCursor == "" {
		status = "complete"
	}
	if err := r.marketDB.SaveMarketSyncState(task, page.NextCursor, status, scanned, ""); err != nil {
		log.Printf("[WARN] market rolling reconcile cursor save failed: task=%s err=%v", task, err)
		return
	}
	r.db.SetMarketReconcileStats(closed, scanned, time.Now().UnixMilli(), false)
	log.Printf("[INFO] market rolling reconcile: task=%s page=%d scanned=%d status=%s conflicts=%d",
		task, len(page.Markets), scanned, status, conflicts)
}

func (r *conditionResolver) nextReconcileTask() (bool, store.MarketSyncState, bool) {
	now := time.Now()
	for _, candidate := range []struct {
		closed bool
		cycle  time.Duration
	}{
		{closed: false, cycle: activeReconcileCycle},
		{closed: true, cycle: closedReconcileCycle},
	} {
		task := reconcileTaskName(candidate.closed)
		state, err := r.marketDB.GetMarketSyncState(task)
		if err != nil {
			log.Printf("[WARN] market reconcile state read failed: task=%s err=%v", task, err)
			continue
		}
		if state.Status == "complete" {
			completedAt := time.Unix(state.CompletedAt, 0)
			if state.CompletedAt > 0 && now.Sub(completedAt) < candidate.cycle {
				continue
			}
			if err := r.marketDB.ResetMarketSyncState(task); err != nil {
				log.Printf("[WARN] market reconcile state reset failed: task=%s err=%v", task, err)
				continue
			}
			state = store.MarketSyncState{}
		}
		return candidate.closed, state, true
	}
	return false, store.MarketSyncState{}, false
}

func reconcileTaskName(closed bool) string {
	return fmt.Sprintf("rolling_closed_%t", closed)
}

func (r *conditionResolver) storeMapping(marketID, conditionID string) error {
	var conflict bool
	var err error
	if r.marketDB != nil {
		_, conflict, err = r.marketDB.UpsertMarketCondition(marketID, conditionID)
	} else {
		_, conflict, err = r.db.UpsertMarketCondition(marketID, conditionID)
	}
	if err != nil {
		return err
	}
	if conflict {
		r.conflicts.Add(1)
		r.publishStats()
		return fmt.Errorf("condition mapping conflict: market=%s", marketID)
	}
	r.setCached(marketID, conditionID)
	if r.maintDB != nil {
		if err := r.maintDB.FillConditionByMarketID(marketID, conditionID); err != nil {
			log.Printf("[WARN] question mapping fill failed: market=%s err=%v", marketID, err)
		}
	}
	if err := r.db.UpdateConditionIDByMarketID(marketID, conditionID); err != nil {
		return err
	}
	if r.mem != nil {
		r.mem.UpdateConditionIDByMarketID(marketID, conditionID)
	}
	return nil
}

func (r *conditionResolver) storeCatalogMapping(market uma.GammaMarketMapping) error {
	_, err := r.storeCatalogMappingWithResult(market)
	return err
}

func (r *conditionResolver) storeCatalogMappingWithResult(market uma.GammaMarketMapping) (bool, error) {
	if r.marketDB == nil {
		return false, fmt.Errorf("market catalog unavailable")
	}
	closedAt := gammaClosedAt(market.ClosedTime)
	inserted, conflict, err := r.marketDB.UpsertMarketConditionStatus(
		market.ID, market.ConditionID, market.Active, market.Closed, closedAt)
	if err != nil {
		return false, err
	}
	if conflict {
		r.conflicts.Add(1)
		r.publishStats()
		return false, fmt.Errorf("condition mapping conflict: market=%s", market.ID)
	}
	if market.Active && !market.Closed {
		r.setCached(market.ID, market.ConditionID)
	} else if market.Closed && closedAt > 0 && time.Since(time.Unix(closedAt, 0)) >= closedMarketGrace {
		r.removeCached(market.ID)
	}
	// Only a newly discovered mapping can complete an empty question mapping.
	// Rewriting maintenance rows for every already-known recent market creates
	// avoidable lock contention with realtime initialized events.
	if inserted && r.maintDB != nil {
		if err := r.maintDB.FillConditionByMarketID(market.ID, market.ConditionID); err != nil {
			log.Printf("[WARN] question mapping fill failed: market=%s err=%v", market.ID, err)
		}
	}
	return inserted, nil
}

func gammaClosedAt(value string) int64 {
	if value == "" {
		return 0
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05-07", "2006-01-02 15:04:05Z07:00"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.Unix()
		}
	}
	return 0
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
	r.mu.Lock()
	defer r.mu.Unlock()
	element := r.cache[marketID]
	if element == nil {
		return ""
	}
	r.cacheLRU.MoveToFront(element)
	return element.Value.(conditionCacheEntry).conditionID
}
func (r *conditionResolver) setCached(marketID, conditionID string) {
	if marketID == "" || conditionID == "" {
		return
	}
	r.mu.Lock()
	if element := r.cache[marketID]; element != nil {
		element.Value = conditionCacheEntry{marketID: marketID, conditionID: conditionID}
		r.cacheLRU.MoveToFront(element)
		r.mu.Unlock()
		return
	}
	element := r.cacheLRU.PushFront(conditionCacheEntry{marketID: marketID, conditionID: conditionID})
	r.cache[marketID] = element
	if len(r.cache) > marketCacheLimit {
		oldest := r.cacheLRU.Back()
		if oldest != nil {
			delete(r.cache, oldest.Value.(conditionCacheEntry).marketID)
			r.cacheLRU.Remove(oldest)
		}
	}
	r.mu.Unlock()
	r.publishStats()
}
func (r *conditionResolver) removeCached(marketID string) {
	r.mu.Lock()
	if element := r.cache[marketID]; element != nil {
		delete(r.cache, marketID)
		r.cacheLRU.Remove(element)
	}
	r.mu.Unlock()
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
	r.mu.Lock()
	mappings := int64(len(r.cache))
	r.mu.Unlock()
	oldestMS := int64(0)
	if ns := r.oldestNS.Load(); ns > 0 {
		oldestMS = time.Since(time.Unix(0, ns)).Milliseconds()
	}
	r.db.SetMarketSyncStats(mappings, marketCacheLimit, r.pending.Load(), oldestMS, r.conflicts.Load())
}
