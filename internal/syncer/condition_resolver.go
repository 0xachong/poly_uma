package syncer

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polymas/poly_uma/internal/notify"
	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
	"golang.org/x/sync/singleflight"
)

const (
	// Gamma can keep a market active for some time after its on-chain proposal.
	// Refresh the active tail frequently so the mapping normally exists before
	// the proposal reaches the realtime pipeline.
	marketIncrementalInterval  = 10 * time.Second
	marketIncrementalOverlap   = 2 * time.Minute
	marketIncrementalBootstrap = 15 * time.Minute
	marketIncrementalPageYield = 50 * time.Millisecond
	marketCacheLimit           = 100000
	closedMarketGrace          = 24 * time.Hour
	// Full baseline must converge quickly after the first rollout/cold restart.
	// reconcileAllowed still yields immediately whenever realtime work exists.
	marketReconcileInterval = 50 * time.Millisecond
	// A completed catalog snapshot only describes the point in time at which its
	// cursor passed each market. Start another active sweep shortly afterwards:
	// markets published by Gamma behind that cursor must not wait a day.
	activeReconcileCycle    = 30 * time.Minute
	reconcileHeapPauseBytes = 550 << 20
	clobSamplingInterval    = 30 * time.Second
	clobExitGrace           = 48 * time.Hour
	umaCandidatePin         = 48 * time.Hour
	umaCandidateLookback    = 7 * 24 * time.Hour
	umaCandidateLimit       = 20000
	umaCandidateWorkers     = 8
)

type conditionCacheEntry struct {
	marketID    string
	conditionID string
}

// conditionResolver guarantees that callers never receive an empty condition_id.
type conditionResolver struct {
	db            *store.SQLite
	marketDB      *store.MarketSQLite
	maintDB       *store.MaintenanceSQLite
	mem           *store.MemReplica
	proxyURL      string
	alerter       *notify.MappingAlerter
	activeCatalog atomic.Bool

	mu    sync.RWMutex
	cache map[string]*list.Element
	// snapshots is keyed by condition_id. UMA enrichment uses condition_id as
	// its sole hot-path lookup key; market_id remains a discovery/repair index.
	snapshots            map[string]*store.MarketSnapshot
	clobHot              map[string]struct{}
	clobReady            atomic.Bool
	cacheLRU             *list.List
	fetch                singleflight.Group
	repair               singleflight.Group
	pending              atomic.Int64
	oldestNS             atomic.Int64
	conflicts            atomic.Int64
	snapshotHits         atomic.Int64
	snapshotMisses       atomic.Int64
	snapshotRepairs      atomic.Int64
	wake                 chan struct{}
	deliveryMu           sync.Mutex
	deliveryInflight     map[string]struct{}
	deliverySlots        chan struct{}
	lastPauseLogNS       atomic.Int64
	incrementalMu        sync.Mutex
	incrementalHighWater time.Time
	incrementalLoaded    bool
}

func newConditionResolver(db *store.SQLite, marketDB *store.MarketSQLite, maintDB *store.MaintenanceSQLite, mem *store.MemReplica, proxyURL string, alerter ...*notify.MappingAlerter) *conditionResolver {
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
		snapshots: make(map[string]*store.MarketSnapshot), clobHot: make(map[string]struct{}),
		deliveryInflight: make(map[string]struct{}), deliverySlots: make(chan struct{}, 16),
	}
	if len(alerter) > 0 {
		r.alerter = alerter[0]
	}
	for marketID, conditionID := range preload {
		r.setCached(marketID, conditionID)
	}
	if marketDB != nil {
		if records, loadErr := marketDB.LoadActiveMarketSnapshots(); loadErr != nil {
			log.Printf("[WARN] active catalog snapshot preload failed: %v", loadErr)
		} else {
			for _, record := range records {
				var snapshot store.MarketSnapshot
				if json.Unmarshal([]byte(record.SnapshotJSON), &snapshot) == nil {
					r.setSnapshot(&snapshot)
				}
			}
		}
	}
	r.publishStats()
	log.Printf("[INFO] condition resolver active cache loaded: markets=%d limit=%d", len(preload), marketCacheLimit)
	return r
}

func (r *conditionResolver) ResolveSnapshotCached(conditionID string) *store.MarketSnapshot {
	if !r.activeCatalog.Load() {
		return nil
	}
	r.mu.RLock()
	snapshot := r.snapshots[strings.ToLower(conditionID)]
	r.mu.RUnlock()
	if conditionID == "" || snapshot == nil {
		r.snapshotMisses.Add(1)
	} else {
		r.snapshotHits.Add(1)
	}
	r.publishActiveCatalogStats()
	return snapshot
}

func (r *conditionResolver) ObserveSnapshotMiss(row store.EventRow) {
	if r == nil || !r.activeCatalog.Load() || row.MarketID == "" {
		return
	}
	kind, severity, detail := "active_catalog_snapshot_miss", "warning",
		"condition mapping exists but the full market snapshot was absent; one exceptional repair was queued"
	if row.ConditionID == "" {
		kind, severity = "condition_mapping_miss", "high"
		detail = "market_id to condition_id mapping was absent; one exceptional repair was queued"
	}
	r.alerter.Send(notify.MappingAlert{Kind: kind, Severity: severity, MarketID: row.MarketID,
		ConditionID: row.ConditionID, EventType: row.EventType, TxHash: row.TxHash, LogIndex: row.LogIndex,
		BlockNumber: row.BlockNumber, Detail: detail})
}

func (r *conditionResolver) ActiveCatalogEnabled() bool { return r != nil && r.activeCatalog.Load() }

// RepairSnapshot performs the exceptional I/O path and force-pins a complete
// routing snapshot. Proposed/disputed enrichment must work even when Gamma no
// longer reports the market as currently accepting orders.
func (r *conditionResolver) RepairSnapshot(ctx context.Context, marketID, conditionID string) (*store.MarketSnapshot, error) {
	value, err, _ := r.repair.Do(marketID, func() (interface{}, error) {
		market, err := uma.FetchGammaMarket(ctx, r.proxyURL, marketID)
		if err != nil {
			return nil, err
		}
		if conditionID != "" && !strings.EqualFold(market.ConditionID, conditionID) {
			return nil, fmt.Errorf("condition mapping conflict: market=%s expected=%s gamma=%s", marketID, conditionID, market.ConditionID)
		}
		_, err = r.storeCatalogMappingWithResultMode(market, true)
		if err != nil {
			return nil, err
		}
		return r.ResolveSnapshotCached(market.ConditionID), nil
	})
	if err != nil {
		return nil, err
	}
	snapshot, _ := value.(*store.MarketSnapshot)
	if snapshot == nil {
		return nil, fmt.Errorf("condition enrichment incomplete: market=%s condition=%s", marketID, conditionID)
	}
	return snapshot, nil
}

func (r *conditionResolver) SetActiveCatalogEnabled(enabled bool) {
	r.activeCatalog.Store(enabled)
	if enabled {
		r.publishActiveCatalogStats()
	}
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
	if marketID == "" {
		return
	}
	if !r.prefetchNeeded(marketID) {
		return
	}
	go func() {
		err := r.prefetchNow(ctx, marketID)
		if err != nil && ctx.Err() == nil {
			log.Printf("[WARN] condition resolver prefetch failed: market=%s err=%v", marketID, err)
		}
	}()
}

func (r *conditionResolver) prefetchNow(ctx context.Context, marketID string) error {
	prefetchCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	_, err, _ := r.repair.Do("prefetch:"+marketID, func() (interface{}, error) {
		market, err := uma.FetchGammaMarket(prefetchCtx, r.proxyURL, marketID)
		if err != nil {
			return nil, err
		}
		_, err = r.storeCatalogMappingWithResultMode(market, true)
		return nil, err
	})
	return err
}

func (r *conditionResolver) prefetchNeeded(marketID string) bool {
	conditionID := r.ResolveCached(marketID)
	return conditionID == "" || r.ResolveSnapshotCached(conditionID) == nil
}

func (r *conditionResolver) Run(ctx context.Context, _ int) {
	go r.prewarmHistoricalInitCandidates(ctx)
	go r.clobSamplingLoop(ctx)
	go r.incrementalLoop(ctx)
	go r.rollingReconcileLoop(ctx)
	go r.statsLoop(ctx)
	go r.pendingDeliveryLoop(ctx)
	<-ctx.Done()
}

func (r *conditionResolver) prewarmHistoricalInitCandidates(ctx context.Context) {
	if !r.activeCatalog.Load() || r.db == nil {
		return
	}
	marketIDs, err := r.db.ListUMAInitCandidates(time.Now().Add(-umaCandidateLookback).Unix(), umaCandidateLimit)
	if err != nil {
		log.Printf("[WARN] list historical UMA init candidates: %v", err)
		return
	}
	jobs := make(chan string)
	var wg sync.WaitGroup
	var warmed atomic.Int64
	for i := 0; i < umaCandidateWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for marketID := range jobs {
				if !r.prefetchNeeded(marketID) {
					continue
				}
				if err := r.prefetchNow(ctx, marketID); err == nil {
					warmed.Add(1)
				}
			}
		}()
	}
	for _, marketID := range marketIDs {
		select {
		case jobs <- marketID:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		}
	}
	close(jobs)
	wg.Wait()
	log.Printf("[INFO] historical UMA init prewarm complete: candidates=%d warmed=%d", len(marketIDs), warmed.Load())
}

func (r *conditionResolver) clobSamplingLoop(ctx context.Context) {
	if !r.activeCatalog.Load() {
		return
	}
	if !r.clobReady.Load() {
		r.refreshCLOBResidentSet(ctx)
	}
	ticker := time.NewTicker(clobSamplingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refreshCLOBResidentSet(ctx)
		}
	}
}

func (r *conditionResolver) refreshCLOBResidentSet(ctx context.Context) {
	if r.marketDB == nil {
		return
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	hot := make(map[string]struct{})
	cursor := ""
	pages := 0
	for {
		page, err := uma.FetchCLOBSamplingSimplifiedMarkets(refreshCtx, r.proxyURL, cursor)
		if err != nil {
			log.Printf("[WARN] CLOB sampling refresh failed: page=%d cursor=%s err=%v", pages+1, cursor, err)
			return // Never evict from a partial or failed authoritative snapshot.
		}
		pages++
		for _, market := range page.Data {
			if market.ConditionID != "" && market.Active && !market.Closed && !market.Archived && market.AcceptingOrders {
				hot[strings.ToLower(market.ConditionID)] = struct{}{}
			}
		}
		next := page.NextCursor
		if next == "" || next == "LTE=" || next == cursor {
			break
		}
		cursor = next
	}
	conditionIDs := make([]string, 0, len(hot))
	for conditionID := range hot {
		conditionIDs = append(conditionIDs, conditionID)
	}
	now := time.Now()
	if err := r.marketDB.ApplyCLOBResidentSet(conditionIDs, now.Unix(), now.Add(-clobExitGrace).Unix()); err != nil {
		log.Printf("[WARN] CLOB resident set persist failed: markets=%d err=%v", len(hot), err)
		return
	}
	records, err := r.marketDB.LoadActiveMarketSnapshots()
	if err != nil {
		log.Printf("[WARN] CLOB resident snapshots reload failed: %v", err)
		return
	}
	resident := make(map[string]*store.MarketSnapshot, len(records))
	for _, record := range records {
		var snapshot store.MarketSnapshot
		if json.Unmarshal([]byte(record.SnapshotJSON), &snapshot) == nil && snapshot.ConditionID != "" {
			resident[strings.ToLower(snapshot.ConditionID)] = &snapshot
		}
	}
	r.mu.Lock()
	r.clobHot = hot
	r.snapshots = resident
	r.mu.Unlock()
	r.clobReady.Store(true)
	r.publishActiveCatalogStats()
	log.Printf("[INFO] CLOB sampling refresh complete: pages=%d hot=%d resident_with_grace=%d", pages, len(hot), len(resident))
}

func (r *conditionResolver) isCLOBHot(conditionID string) bool {
	conditionID = strings.ToLower(conditionID)
	r.mu.RLock()
	_, hot := r.clobHot[conditionID]
	r.mu.RUnlock()
	return hot
}

func (r *conditionResolver) hasSnapshot(conditionID string) bool {
	r.mu.RLock()
	_, ok := r.snapshots[strings.ToLower(conditionID)]
	r.mu.RUnlock()
	return ok
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
	resolveStartedAt := time.Now()
	mappingWasMissing := item.ConditionID == ""
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
	mappingResolvedAt := time.Now()
	// ResolveRequired may have hit a mapping inserted by the catalog sync, whose
	// fast path does not rewrite event rows. Make the pending row durable and
	// visible to HTTP before broadcasting it.
	persistStartedAt := time.Now()
	if err := r.db.UpdateConditionIDByMarketID(item.MarketID, conditionID); err != nil {
		log.Printf("[WARN] update pending delivery mapping: market=%s tx=%s err=%v", item.MarketID, item.TxHash, err)
		return
	}
	if r.mem != nil {
		r.mem.UpdateConditionIDByMarketID(item.MarketID, conditionID)
	}
	persistFinishedAt := time.Now()
	item.ConditionID = conditionID
	if r.activeCatalog.Load() {
		snapshot := r.ResolveSnapshotCached(conditionID)
		if snapshot == nil {
			repairStarted := time.Now()
			repairCtx, repairCancel := context.WithTimeout(ctx, 10*time.Second)
			var repairErr error
			snapshot, repairErr = r.RepairSnapshot(repairCtx, item.MarketID, conditionID)
			repairCancel()
			if repairErr != nil {
				r.alerter.Send(notify.MappingAlert{Kind: "active_catalog_repair_failed", Severity: "critical",
					MarketID: item.MarketID, ConditionID: conditionID, EventType: item.EventType, TxHash: item.TxHash,
					LogIndex: item.LogIndex, BlockNumber: item.BlockNumber, Detail: repairErr.Error(), RepairElapsed: time.Since(repairStarted)})
				if ctx.Err() == nil {
					log.Printf("[WARN] enrich pending delivery: market=%s condition=%s tx=%s err=%v",
						item.MarketID, conditionID, item.TxHash, repairErr)
				}
				return
			}
			kind := "active_catalog_snapshot_miss"
			if mappingWasMissing {
				kind = "condition_mapping_miss"
			}
			r.alerter.Send(notify.MappingAlert{Kind: kind, Severity: "info", MarketID: item.MarketID,
				ConditionID: conditionID, EventType: item.EventType, TxHash: item.TxHash, LogIndex: item.LogIndex,
				BlockNumber: item.BlockNumber, Detail: "exceptional repair restored the condition-indexed snapshot",
				Recovered: true, RepairElapsed: time.Since(repairStarted)})
			r.snapshotRepairs.Add(1)
			r.publishActiveCatalogStats()
		}
		item.EventRow.Market = snapshot
	}
	item.Source = "delayed_replay"
	item.EventRow.UpstreamReceivedAtMS = item.UpstreamReceivedAtMS
	item.EventRow.MappingResolvedAtMS = mappingResolvedAt.UnixMilli()
	if item.UpstreamReceivedAtMS > 0 {
		item.EventRow.MappingWaitMS = mappingResolvedAt.UnixMilli() - item.UpstreamReceivedAtMS
	} else {
		item.EventRow.MappingWaitMS = mappingResolvedAt.Sub(resolveStartedAt).Milliseconds()
	}
	item.EventRow.MappingPersistMS = persistFinishedAt.Sub(persistStartedAt).Milliseconds()
	item.EventRow.ReplayReadyAtMS = persistFinishedAt.UnixMilli()
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
	// Use Gamma's documented keyset endpoint. A time overlap makes the sweep
	// resilient to cache skew between Gamma replicas; the opaque cursor makes
	// equal updatedAt cohorts lossless, unlike the former fixed 1000-row offset
	// window.
	r.incrementalMu.Lock()
	defer r.incrementalMu.Unlock()
	if !r.incrementalLoaded {
		r.incrementalLoaded = true
		if r.marketDB != nil {
			if state, err := r.marketDB.GetMarketSyncState("incremental_updated_at"); err == nil {
				if parsed, parseErr := time.Parse(time.RFC3339Nano, state.NextCursor); parseErr == nil {
					r.incrementalHighWater = parsed
				}
			}
		}
	}
	cutoff := r.incrementalHighWater.Add(-marketIncrementalOverlap)
	if r.incrementalHighWater.IsZero() {
		cutoff = time.Now().Add(-marketIncrementalBootstrap)
	}
	var cursor string
	var newest time.Time
	scanned := 0
	for {
		pageCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		page, err := uma.FetchGammaUpdatedMarketsKeyset(pageCtx, r.proxyURL, cursor, false)
		cancel()
		if err != nil {
			log.Printf("[WARN] active market incremental sync failed: cursor=%s scanned=%d err=%v", cursor, scanned, err)
			return
		}
		reachedCutoff := false
		for _, market := range page.Markets {
			updatedAt, parseErr := time.Parse(time.RFC3339Nano, market.UpdatedAt)
			if parseErr == nil {
				if newest.IsZero() || updatedAt.After(newest) {
					newest = updatedAt
				}
				if !updatedAt.After(cutoff) {
					reachedCutoff = true
					break
				}
			}
			if market.ID == "" || market.ConditionID == "" {
				continue
			}
			if _, err := r.storeCatalogMappingWithResult(market); err != nil {
				log.Printf("[WARN] active market incremental store failed: market=%s err=%v", market.ID, err)
			}
			scanned++
		}
		if reachedCutoff || page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		select {
		case <-ctx.Done():
			return
		case <-time.After(marketIncrementalPageYield):
		}
	}
	if !newest.IsZero() && newest.After(r.incrementalHighWater) {
		r.incrementalHighWater = newest
	}
	if r.marketDB != nil && !r.incrementalHighWater.IsZero() {
		if err := r.marketDB.SaveMarketSyncState("incremental_updated_at",
			r.incrementalHighWater.UTC().Format(time.RFC3339Nano), "complete", int64(scanned), ""); err != nil {
			log.Printf("[WARN] active market incremental watermark save failed: %v", err)
		}
	}
	log.Printf("[INFO] active market incremental keyset sync: scanned=%d cutoff=%s high_water=%s",
		scanned, cutoff.UTC().Format(time.RFC3339Nano), r.incrementalHighWater.UTC().Format(time.RFC3339Nano))
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
	if !r.reconcileAllowed() {
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
	// The closed=false keyset is the authoritative full baseline. Persist full
	// routing attributes only for genuinely tradable rows; the updatedAt keyset
	// handles additions and state changes between baseline sweeps.
	for _, market := range page.Markets {
		if !gammaMarketTradable(market) {
			continue
		}
		if _, storeErr := r.storeCatalogMappingWithResult(market); storeErr != nil {
			log.Printf("[WARN] full active snapshot store failed: market=%s err=%v", market.ID, storeErr)
		}
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

func (r *conditionResolver) incrementalAllowed() bool {
	stats := r.db.PipelineStats()
	if stats.QueueDepth > 0 || stats.Processing > 0 {
		return false
	}
	return true
}

func (r *conditionResolver) reconcileAllowed() bool {
	stats := r.db.PipelineStats()
	if !r.incrementalAllowed() {
		r.db.SetMarketReconcileStats(stats.MarketReconcileClosed, stats.MarketReconcileScanned, 0, true)
		return false
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	if memory.HeapAlloc < reconcileHeapPauseBytes {
		return true
	}
	r.db.SetMarketReconcileStats(stats.MarketReconcileClosed, stats.MarketReconcileScanned, 0, true)
	now := time.Now().UnixNano()
	last := r.lastPauseLogNS.Load()
	if now-last >= int64(time.Minute) && r.lastPauseLogNS.CompareAndSwap(last, now) {
		log.Printf("[WARN] market maintenance paused: heap_alloc_mb=%d threshold_mb=%d",
			memory.HeapAlloc>>20, reconcileHeapPauseBytes>>20)
	}
	return false
}

func reconcileTaskName(closed bool) string {
	return fmt.Sprintf("rolling_tradable_v2_closed_%t", closed)
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
	return r.storeCatalogMappingWithResultMode(market, false)
}

func (r *conditionResolver) storeCatalogMappingWithResultMode(market uma.GammaMarketMapping, forceSnapshot bool) (bool, error) {
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
		tradable := gammaMarketTradable(market)
		clobHot := r.isCLOBHot(market.ConditionID)
		if !forceSnapshot && !tradable && !clobHot {
			// Until the first complete CLOB snapshot arrives, preserve the durable
			// set and only update the immutable market/condition mapping.
			if !r.clobReady.Load() {
				return inserted, nil
			}
			// A previously tradable or UMA-pinned snapshot remains resident for its
			// persisted grace period. ApplyCLOBResidentSet performs the expiry.
			if r.hasSnapshot(market.ConditionID) {
				return inserted, nil
			}
			return inserted, nil
		}
		if !r.activeCatalog.Load() || market.Question == "" {
			return inserted, nil
		}
		snapshot := snapshotFromGamma(market)
		encoded, marshalErr := json.Marshal(snapshot)
		if marshalErr != nil {
			return false, marshalErr
		}
		if err := r.marketDB.UpsertActiveMarketSnapshot(store.ActiveMarketSnapshotRecord{
			MarketID: snapshot.MarketID, ConditionID: snapshot.ConditionID, SnapshotJSON: string(encoded),
			Active: snapshot.Active, Closed: snapshot.Closed, GammaUpdatedMS: snapshot.GammaUpdatedAtMS,
			SyncedAtUS: snapshot.CatalogSyncedAtUS,
			UMAPinnedUntil: func() int64 {
				if forceSnapshot {
					return time.Now().Add(umaCandidatePin).Unix()
				}
				return 0
			}(),
			CLOBLastSeenAt: func() int64 {
				if tradable || clobHot {
					return time.Now().Unix()
				}
				return 0
			}(),
		}); err != nil {
			return false, err
		}
		r.setSnapshot(snapshot)
	} else if market.Closed && closedAt > 0 && time.Since(time.Unix(closedAt, 0)) >= closedMarketGrace {
		r.removeCached(market.ID)
		r.removeSnapshot(market.ConditionID)
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

func gammaMarketTradable(market uma.GammaMarketMapping) bool {
	return market.ID != "" && market.ConditionID != "" && market.Active && !market.Closed && !market.Archived && market.AcceptingOrders
}

func snapshotFromGamma(market uma.GammaMarketMapping) *store.MarketSnapshot {
	snapshot := &store.MarketSnapshot{
		MarketID: market.ID, ConditionID: market.ConditionID, Question: market.Question,
		Slug: market.Slug, Description: market.Description, Category: market.Category,
		SportsMarketType: market.SportsMarketType, TokenIDs: append([]string(nil), market.TokenIDs...),
		Outcomes: append([]string(nil), market.Outcomes...), OutcomePrices: append([]float64(nil), market.OutcomePrices...),
		Active: market.Active, Closed: market.Closed, AcceptingOrders: market.AcceptingOrders,
		TakerBaseFee: market.TakerBaseFee, CatalogSyncedAtUS: time.Now().UnixMicro(),
	}
	if parsed, err := time.Parse(time.RFC3339Nano, market.UpdatedAt); err == nil {
		snapshot.GammaUpdatedAtMS = parsed.UnixMilli()
	}
	tags := market.Tags
	if len(market.Events) > 0 {
		event := market.Events[0]
		snapshot.PolymarketEventID, snapshot.PolymarketEventTitle, snapshot.PolymarketEventSlug = event.ID, event.Title, event.Slug
		if len(tags) == 0 {
			tags = event.Tags
		}
	}
	for _, tag := range tags {
		snapshot.Tags = append(snapshot.Tags, store.MarketTag{ID: tag.ID, Label: tag.Label, Slug: tag.Slug})
	}
	return snapshot
}

func (r *conditionResolver) setSnapshot(snapshot *store.MarketSnapshot) {
	if snapshot == nil || snapshot.MarketID == "" || snapshot.ConditionID == "" {
		return
	}
	r.mu.Lock()
	r.snapshots[strings.ToLower(snapshot.ConditionID)] = snapshot
	r.mu.Unlock()
	r.publishActiveCatalogStats()
}

func (r *conditionResolver) removeSnapshot(conditionID string) {
	r.mu.Lock()
	delete(r.snapshots, strings.ToLower(conditionID))
	r.mu.Unlock()
	r.publishActiveCatalogStats()
}

func (r *conditionResolver) publishActiveCatalogStats() {
	r.mu.RLock()
	count := int64(len(r.snapshots))
	r.mu.RUnlock()
	r.db.SetActiveCatalogStats(count, int64(r.snapshotHits.Load()), int64(r.snapshotMisses.Load()), int64(r.snapshotRepairs.Load()))
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
