package syncer

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
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
	marketCacheLimit           = 300000
	closedMarketGrace          = 24 * time.Hour
	// Full baseline must converge quickly after the first rollout/cold restart.
	// reconcileAllowed still yields immediately whenever realtime work exists.
	marketReconcileInterval = 50 * time.Millisecond
	// A completed catalog snapshot only describes the point in time at which its
	// cursor passed each market. Start another active sweep shortly afterwards:
	// markets published by Gamma behind that cursor must not wait a day.
	activeReconcileCycle = 30 * time.Minute
	// Production's compact active catalog currently settles above 550 MiB. A
	// lower guard permanently stalls the closed=false baseline after restart.
	// Keep the guard below the 2 GiB host limit while allowing the warm catalog
	// to converge; inactive snapshots omit their largest optional text field.
	reconcileHeapPauseBytes   = 900 << 20
	clobSamplingInterval      = 30 * time.Second
	clobExitGrace             = 48 * time.Hour
	umaCandidatePin           = 48 * time.Hour
	umaCandidateLookback      = 7 * 24 * time.Hour
	umaCandidateLimit         = 20000
	realtimePrefetchWorkers   = 4
	realtimePrefetchQueue     = 4096
	historicalPrefetchWorkers = 2
	incidentRetention         = 30 * 24 * time.Hour
	marketFetchFailureLimit   = 3
	marketFetchBackoffBase    = time.Minute
	marketFetchBackoffMax     = 24 * time.Hour
)

type marketRetentionPolicy struct {
	class          string
	inactiveWindow time.Duration
	exitGrace      time.Duration
}

var (
	fastRetention    = marketRetentionPolicy{class: "fast", inactiveWindow: 48 * time.Hour, exitGrace: 48 * time.Hour}
	normalRetention  = marketRetentionPolicy{class: "normal", inactiveWindow: 30 * 24 * time.Hour, exitGrace: 7 * 24 * time.Hour}
	longRetention    = marketRetentionPolicy{class: "long", inactiveWindow: 0, exitGrace: 30 * 24 * time.Hour}
	unknownRetention = marketRetentionPolicy{class: "unknown", inactiveWindow: 14 * 24 * time.Hour, exitGrace: 14 * 24 * time.Hour}
)

type conditionCacheEntry struct {
	marketID    string
	conditionID store.ConditionID
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
	snapshots            map[store.ConditionID]*store.MarketSnapshot
	clobHot              map[store.ConditionID]struct{}
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
	prefetchQueue        chan string
	prefetchMu           sync.Mutex
	prefetchQueued       map[string]struct{}
	prefetchDropped      atomic.Int64
	lastPauseLogNS       atomic.Int64
	incrementalMu        sync.Mutex
	incrementalHighWater time.Time
	incrementalLoaded    bool
	blacklistMu          sync.RWMutex
	fetchBlacklist       map[string]store.MarketFetchBlacklistEntry
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
		snapshots: make(map[store.ConditionID]*store.MarketSnapshot), clobHot: make(map[store.ConditionID]struct{}),
		deliveryInflight: make(map[string]struct{}), deliverySlots: make(chan struct{}, 16),
		prefetchQueue: make(chan string, realtimePrefetchQueue), prefetchQueued: make(map[string]struct{}),
		fetchBlacklist: make(map[string]store.MarketFetchBlacklistEntry),
	}
	if len(alerter) > 0 {
		r.alerter = alerter[0]
	}
	for marketID, conditionID := range preload {
		r.setCached(marketID, conditionID)
	}
	if marketDB != nil {
		blacklist, blacklistErr := marketDB.LoadMarketFetchBlacklist()
		if blacklistErr != nil {
			log.Printf("[WARN] condition resolver blacklist preload failed: %v", blacklistErr)
		} else {
			r.fetchBlacklist = blacklist
		}
		if records, loadErr := marketDB.LoadActiveMarketSnapshots(); loadErr != nil {
			log.Printf("[WARN] active catalog snapshot preload failed: %v", loadErr)
		} else {
			for _, record := range records {
				var snapshot store.MarketSnapshot
				if json.Unmarshal([]byte(record.SnapshotJSON), &snapshot) == nil {
					compactResidentSnapshot(&snapshot)
					r.setSnapshot(&snapshot)
				}
			}
		}
	}
	r.publishStats()
	log.Printf("[INFO] condition resolver active cache loaded: markets=%d blacklist=%d limit=%d", len(preload), len(r.fetchBlacklist), marketCacheLimit)
	return r
}

type marketFetchBackoffError struct {
	marketID string
	next     time.Time
}

func (e *marketFetchBackoffError) Error() string {
	return fmt.Sprintf("market %s fetch blacklisted until %s", e.marketID, e.next.Format(time.RFC3339))
}

func marketFetchBackoff(failures int) time.Duration {
	if failures < marketFetchFailureLimit {
		return time.Duration(1<<max(failures-1, 0)) * time.Second
	}
	shift := failures - marketFetchFailureLimit
	if shift > 20 {
		return marketFetchBackoffMax
	}
	delay := marketFetchBackoffBase * time.Duration(1<<shift)
	if delay > marketFetchBackoffMax {
		return marketFetchBackoffMax
	}
	return delay
}

func (r *conditionResolver) fetchRetryBlocked(marketID string, now time.Time) (store.MarketFetchBlacklistEntry, bool) {
	r.blacklistMu.RLock()
	entry, ok := r.fetchBlacklist[marketID]
	r.blacklistMu.RUnlock()
	return entry, ok && entry.FailureCount >= marketFetchFailureLimit && now.UnixMilli() < entry.NextRetryAt
}

func (r *conditionResolver) recordMarketFetchFailure(marketID string, fetchErr error) (store.MarketFetchBlacklistEntry, error) {
	now := time.Now()
	r.blacklistMu.Lock()
	entry := r.fetchBlacklist[marketID]
	entry.MarketID = marketID
	entry.FailureCount++
	if entry.FirstFailedAt == 0 {
		entry.FirstFailedAt = now.UnixMilli()
	}
	entry.LastFailedAt = now.UnixMilli()
	entry.NextRetryAt = now.Add(marketFetchBackoff(entry.FailureCount)).UnixMilli()
	entry.LastError = fetchErr.Error()
	r.fetchBlacklist[marketID] = entry
	r.blacklistMu.Unlock()
	if r.marketDB != nil {
		if err := r.marketDB.UpsertMarketFetchFailure(entry); err != nil {
			return entry, err
		}
	}
	return entry, nil
}

func (r *conditionResolver) clearMarketFetchFailure(marketID string) {
	r.blacklistMu.Lock()
	if _, exists := r.fetchBlacklist[marketID]; !exists {
		r.blacklistMu.Unlock()
		return
	}
	delete(r.fetchBlacklist, marketID)
	r.blacklistMu.Unlock()
	if r.marketDB != nil {
		if err := r.marketDB.DeleteMarketFetchBlacklist(marketID); err != nil {
			log.Printf("[WARN] delete market fetch blacklist: market=%s err=%v", marketID, err)
		}
	}
}

func (r *conditionResolver) ResolveSnapshotCached(conditionID string) *store.MarketSnapshot {
	if !r.activeCatalog.Load() {
		return nil
	}
	id, parseErr := store.ParseConditionID(conditionID)
	r.mu.RLock()
	var snapshot *store.MarketSnapshot
	if parseErr == nil {
		snapshot = r.snapshots[id]
	}
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
	r.recordIncident("miss_observed", row, 0, map[string]interface{}{
		"kind": kind, "condition_mapping_present": row.ConditionID != "", "detail": detail,
	})
}

func (r *conditionResolver) recordIncident(stage string, row store.EventRow, elapsed time.Duration, detail map[string]interface{}) {
	if r == nil || r.marketDB == nil {
		return
	}
	encoded, err := json.Marshal(detail)
	if err != nil {
		encoded = []byte(fmt.Sprintf(`{"marshal_error":%q}`, err.Error()))
	}
	evidence := store.MarketEnrichmentIncident{ObservedAtMS: time.Now().UnixMilli(), Stage: stage,
		MarketID: row.MarketID, ConditionID: row.ConditionID, EventType: row.EventType, TxHash: row.TxHash,
		LogIndex: row.LogIndex, BlockNumber: row.BlockNumber, ElapsedMS: elapsed.Milliseconds(), DetailJSON: string(encoded)}
	go func() {
		if err := r.marketDB.AppendMarketEnrichmentIncident(evidence); err != nil {
			log.Printf("[WARN] persist market enrichment incident: stage=%s market=%s err=%v", stage, row.MarketID, err)
		}
	}()
}

func (r *conditionResolver) ActiveCatalogEnabled() bool { return r != nil && r.activeCatalog.Load() }

// RepairSnapshot performs the exceptional I/O path and force-pins a complete
// routing snapshot. Proposed/disputed enrichment must work even when Gamma no
// longer reports the market as currently accepting orders.
func (r *conditionResolver) RepairSnapshot(ctx context.Context, marketID, conditionID string) (*store.MarketSnapshot, error) {
	value, err, _ := r.repair.Do(marketID, func() (interface{}, error) {
		market, err := r.fetchEnrichmentMarket(ctx, marketID, conditionID)
		if err != nil {
			return nil, err
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

func (r *conditionResolver) fetchEnrichmentMarket(ctx context.Context, marketID, conditionID string) (uma.GammaMarketMapping, error) {
	if entry, blocked := r.fetchRetryBlocked(marketID, time.Now()); blocked {
		return uma.GammaMarketMapping{}, &marketFetchBackoffError{marketID: marketID, next: time.UnixMilli(entry.NextRetryAt)}
	}
	market, err := r.fetchEnrichmentMarketUntracked(ctx, marketID, conditionID)
	if err == nil {
		r.clearMarketFetchFailure(marketID)
		return market, nil
	}
	entry, persistErr := r.recordMarketFetchFailure(marketID, err)
	if persistErr != nil {
		log.Printf("[WARN] persist market fetch blacklist: market=%s err=%v", marketID, persistErr)
	}
	if entry.FailureCount == marketFetchFailureLimit {
		log.Printf("[WARN] condition resolver market blacklisted: market=%s failures=%d retry_at=%s err=%v",
			marketID, entry.FailureCount, time.UnixMilli(entry.NextRetryAt).Format(time.RFC3339), err)
	}
	if entry.FailureCount >= marketFetchFailureLimit {
		return uma.GammaMarketMapping{}, &marketFetchBackoffError{marketID: marketID, next: time.UnixMilli(entry.NextRetryAt)}
	}
	return uma.GammaMarketMapping{}, err
}

func (r *conditionResolver) fetchEnrichmentMarketUntracked(ctx context.Context, marketID, conditionID string) (uma.GammaMarketMapping, error) {
	exact, err := uma.FetchGammaMarket(ctx, r.proxyURL, marketID)
	if err != nil {
		return uma.GammaMarketMapping{}, err
	}
	if conditionID == "" {
		conditionID = exact.ConditionID
	}
	if conditionID != "" && exact.ConditionID != "" && !strings.EqualFold(exact.ConditionID, conditionID) {
		return uma.GammaMarketMapping{}, fmt.Errorf("condition mapping conflict: market=%s expected=%s gamma=%s", marketID, conditionID, exact.ConditionID)
	}
	best := exact
	// The exact-market and condition-list endpoints are backed by different
	// Gamma views and can publish relations at different times. Cross-check only
	// when essential fields or the event relation are absent.
	if conditionID != "" && (strings.TrimSpace(best.Question) == "" || len(best.Events) == 0) {
		markets, reverseErr := uma.FetchGammaMarketsByConditionID(ctx, r.proxyURL, conditionID)
		if reverseErr == nil {
			for _, candidate := range markets {
				if candidate.ID == marketID && gammaEnrichmentScore(candidate) > gammaEnrichmentScore(best) {
					best = candidate
				}
			}
		} else if strings.TrimSpace(best.Question) == "" {
			return uma.GammaMarketMapping{}, fmt.Errorf("%s reverse_lookup=%v", gammaSnapshotDiagnostic(best), reverseErr)
		}
	}
	if strings.TrimSpace(best.Question) == "" {
		return uma.GammaMarketMapping{}, fmt.Errorf("%s", gammaSnapshotDiagnostic(best))
	}
	return best, nil
}

func gammaEnrichmentScore(market uma.GammaMarketMapping) int {
	score := 0
	if strings.TrimSpace(market.Question) != "" {
		score += 8
	}
	if len(market.Events) > 0 {
		score += 4
	}
	if len(market.Tags) > 0 {
		score += 2
	}
	if len(market.TokenIDs) > 0 {
		score++
	}
	return score
}

func gammaSnapshotDiagnostic(market uma.GammaMarketMapping) string {
	return fmt.Sprintf("gamma_snapshot_incomplete market=%s condition=%s active=%t closed=%t archived=%t accepting_orders=%t question_empty=%t events=%d tags=%d tokens=%d updated_at=%s",
		market.ID, market.ConditionID, market.Active, market.Closed, market.Archived, market.AcceptingOrders,
		strings.TrimSpace(market.Question) == "", len(market.Events), len(market.Tags), len(market.TokenIDs), market.UpdatedAt)
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
		r.clearMarketFetchFailure(marketID)
		return value, nil
	}
	if r.marketDB != nil {
		if value, err := r.marketDB.GetMarketConditionID(marketID); err != nil {
			return "", fmt.Errorf("market mapping read: %w", err)
		} else if value != "" {
			r.setCached(marketID, value)
			r.clearMarketFetchFailure(marketID)
			return value, nil
		}
	} else {
		// Compatibility for tests and explicit single-DB rollback mode. Normal
		// production startup always supplies MarketDB.
		if value, err := r.db.GetMarketConditionID(marketID); err != nil {
			return "", err
		} else if value != "" {
			r.setCached(marketID, value)
			r.clearMarketFetchFailure(marketID)
			return value, nil
		}
	}
	if entry, blocked := r.fetchRetryBlocked(marketID, time.Now()); blocked {
		return "", &marketFetchBackoffError{marketID: marketID, next: time.UnixMilli(entry.NextRetryAt)}
	}

	value, err, _ := r.fetch.Do(marketID, func() (interface{}, error) {
		if cached := r.cached(marketID); cached != "" {
			return cached, nil
		}
		r.beginPending()
		defer r.endPending()
		for ctx.Err() == nil {
			if entry, blocked := r.fetchRetryBlocked(marketID, time.Now()); blocked {
				return nil, &marketFetchBackoffError{marketID: marketID, next: time.UnixMilli(entry.NextRetryAt)}
			}
			requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			conditionID, fetchErr := uma.GammaConditionIDContext(requestCtx, marketID, r.proxyURL)
			cancel()
			if fetchErr == nil && conditionID != "" {
				if err := r.storeMapping(marketID, conditionID); err != nil {
					return nil, err
				}
				r.clearMarketFetchFailure(marketID)
				return conditionID, nil
			}
			if fetchErr == nil {
				fetchErr = fmt.Errorf("empty condition id")
			}
			entry, persistErr := r.recordMarketFetchFailure(marketID, fetchErr)
			if persistErr != nil {
				log.Printf("[WARN] persist market fetch blacklist: market=%s err=%v", marketID, persistErr)
			}
			if entry.FailureCount == marketFetchFailureLimit {
				log.Printf("[WARN] condition resolver market blacklisted: market=%s failures=%d retry_at=%s err=%v",
					marketID, entry.FailureCount, time.UnixMilli(entry.NextRetryAt).Format(time.RFC3339), fetchErr)
			} else if entry.FailureCount < marketFetchFailureLimit {
				log.Printf("[WARN] condition resolver exact lookup retry: market=%s failures=%d err=%v", marketID, entry.FailureCount, fetchErr)
			}
			if entry.FailureCount >= marketFetchFailureLimit {
				return nil, &marketFetchBackoffError{marketID: marketID, next: time.UnixMilli(entry.NextRetryAt)}
			}
			backoff := time.Until(time.UnixMilli(entry.NextRetryAt))
			if backoff < 0 {
				backoff = 0
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
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
	// The realtime path must remain memory-only. Gamma enrichment, rolling
	// catalog reconciliation and blacklist persistence all write market.sqlite.
	// Falling through to that same sql.DB on a cache miss used to serialize this
	// lookup behind those background writes (the pool intentionally has one
	// connection), indirectly turning asynchronous enrichment into multi-second
	// realtime latency. Startup preloads the active mapping set and every
	// successful background write updates this cache; a genuine miss is repaired
	// asynchronously by Prefetch/RepairSnapshot.
	return r.cached(marketID)
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

func (r *conditionResolver) Prefetch(_ context.Context, marketID string) {
	if marketID == "" {
		return
	}
	r.prefetchMu.Lock()
	if _, exists := r.prefetchQueued[marketID]; exists {
		r.prefetchMu.Unlock()
		return
	}
	r.prefetchQueued[marketID] = struct{}{}
	r.prefetchMu.Unlock()
	select {
	case r.prefetchQueue <- marketID:
	default:
		r.prefetchMu.Lock()
		delete(r.prefetchQueued, marketID)
		r.prefetchMu.Unlock()
		dropped := r.prefetchDropped.Add(1)
		if dropped == 1 || dropped%1000 == 0 {
			log.Printf("[WARN] realtime market prefetch queue full: dropped=%d capacity=%d", dropped, cap(r.prefetchQueue))
		}
	}
}

func (r *conditionResolver) realtimePrefetchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case marketID := <-r.prefetchQueue:
			if r.prefetchNeeded(marketID) {
				if err := r.prefetchNow(ctx, marketID); err != nil && ctx.Err() == nil {
					log.Printf("[WARN] condition resolver prefetch failed: market=%s err=%v", marketID, err)
				}
			}
			r.prefetchMu.Lock()
			delete(r.prefetchQueued, marketID)
			r.prefetchMu.Unlock()
		}
	}
}

func (r *conditionResolver) prefetchNow(ctx context.Context, marketID string) error {
	prefetchCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	_, err, _ := r.repair.Do("prefetch:"+marketID, func() (interface{}, error) {
		market, err := r.fetchEnrichmentMarket(prefetchCtx, marketID, r.ResolveCached(marketID))
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
	for i := 0; i < realtimePrefetchWorkers; i++ {
		go r.realtimePrefetchLoop(ctx)
	}
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
	for i := 0; i < historicalPrefetchWorkers; i++ {
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
	hot := make(map[store.ConditionID]struct{})
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
			if id, err := store.ParseConditionID(market.ConditionID); err == nil && market.Active && !market.Closed && !market.Archived && market.AcceptingOrders {
				hot[id] = struct{}{}
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
		conditionIDs = append(conditionIDs, conditionID.String())
	}
	now := time.Now()
	if err := r.marketDB.ApplyCLOBResidentSet(conditionIDs, now.Unix()); err != nil {
		log.Printf("[WARN] CLOB resident set persist failed: markets=%d err=%v", len(hot), err)
		return
	}
	records, err := r.marketDB.LoadActiveMarketSnapshots()
	if err != nil {
		log.Printf("[WARN] CLOB resident snapshots reload failed: %v", err)
		return
	}
	resident := make(map[store.ConditionID]*store.MarketSnapshot, len(records))
	for _, record := range records {
		var snapshot store.MarketSnapshot
		if json.Unmarshal([]byte(record.SnapshotJSON), &snapshot) == nil && !snapshot.ConditionID.IsZero() {
			compactResidentSnapshot(&snapshot)
			resident[snapshot.ConditionID] = &snapshot
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
	id, err := store.ParseConditionID(conditionID)
	if err != nil {
		return false
	}
	r.mu.RLock()
	_, hot := r.clobHot[id]
	r.mu.RUnlock()
	return hot
}

func (r *conditionResolver) hasSnapshot(conditionID string) bool {
	id, err := store.ParseConditionID(conditionID)
	if err != nil {
		return false
	}
	r.mu.RLock()
	_, ok := r.snapshots[id]
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
	if _, blocked := r.fetchRetryBlocked(item.MarketID, time.Now()); blocked {
		return
	}
	resolveStartedAt := time.Now()
	mappingWasMissing := item.ConditionID == ""
	conditionID := item.ConditionID
	if conditionID == "" {
		var err error
		resolveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conditionID, err = r.ResolveRequired(resolveCtx, item.MarketID)
		cancel()
		if err != nil {
			var backoffErr *marketFetchBackoffError
			if ctx.Err() == nil && !errors.As(err, &backoffErr) {
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
				r.recordIncident("repair_failed", item.EventRow, time.Since(repairStarted), map[string]interface{}{
					"error": repairErr.Error(), "condition_id": conditionID,
				})
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
			r.recordIncident("repair_recovered", item.EventRow, time.Since(repairStarted), map[string]interface{}{
				"condition_id": conditionID, "snapshot": map[string]interface{}{
					"question": snapshot.Question, "event_id": snapshot.PolymarketEventID,
					"event_title": snapshot.PolymarketEventTitle, "tags": len(snapshot.Tags),
					"tokens": len(snapshot.TokenIDs), "gamma_updated_at_ms": snapshot.GammaUpdatedAtMS,
				},
			})
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
	item.EventRow.CatalogLookupDoneUS = time.Now().UnixMicro()
	item.EventRow.MasterBroadcastAtUS = time.Now().UnixMicro()
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
	// The closed=false keyset is authoritative. Every row updates its durable
	// identity, while the retention policy decides whether its routing snapshot
	// remains resident; this avoids a second Gamma lookup on the UMA hot path.
	for _, market := range page.Markets {
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
	if status == "complete" {
		r.finalizeFullBaseline()
	}
	log.Printf("[INFO] market rolling reconcile: task=%s page=%d scanned=%d status=%s conflicts=%d",
		task, len(page.Markets), scanned, status, conflicts)
}

func (r *conditionResolver) finalizeFullBaseline() {
	now := time.Now()
	deleted, err := r.marketDB.PruneExpiredActiveMarketSnapshots(now.Add(-clobExitGrace).Unix(), now.Unix())
	if err != nil {
		log.Printf("[WARN] full baseline snapshot prune failed: %v", err)
		return
	}
	if _, pruneErr := r.marketDB.PruneMarketEnrichmentIncidents(now.Add(-incidentRetention).UnixMilli()); pruneErr != nil {
		log.Printf("[WARN] enrichment incident evidence prune failed: %v", pruneErr)
	}
	records, err := r.marketDB.LoadActiveMarketSnapshots()
	if err != nil {
		log.Printf("[WARN] full baseline snapshot reload failed: %v", err)
		return
	}
	resident := make(map[store.ConditionID]*store.MarketSnapshot, len(records))
	for _, record := range records {
		var snapshot store.MarketSnapshot
		if json.Unmarshal([]byte(record.SnapshotJSON), &snapshot) == nil && !snapshot.ConditionID.IsZero() {
			compactResidentSnapshot(&snapshot)
			resident[snapshot.ConditionID] = &snapshot
		}
	}
	r.mu.Lock()
	r.snapshots = resident
	r.mu.Unlock()
	r.publishActiveCatalogStats()
	log.Printf("[INFO] full active baseline finalized: resident=%d pruned=%d", len(resident), deleted)
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
	live := market.Active && !market.Closed
	snapshotEligible := gammaSnapshotEligible(market)
	if live || snapshotEligible {
		r.setCached(market.ID, market.ConditionID)
	}
	tradable := gammaMarketTradable(market)
	clobHot := r.isCLOBHot(market.ConditionID)
	shouldSnapshot := forceSnapshot || snapshotEligible || (live && (tradable || clobHot))
	if !shouldSnapshot && !market.Closed && !market.Archived && r.hasSnapshot(market.ConditionID) {
		shouldSnapshot = true // persisted exit grace; do not refresh last_seen.
	}
	if shouldSnapshot {
		if !r.activeCatalog.Load() {
			return inserted, nil
		}
		if strings.TrimSpace(market.Question) == "" {
			return false, fmt.Errorf("%s", gammaSnapshotDiagnostic(market))
		}
		snapshot := snapshotFromGamma(market)
		if snapshot.ConditionID.IsZero() {
			return false, fmt.Errorf("invalid condition_id for market=%s", market.ID)
		}
		encoded, marshalErr := json.Marshal(snapshot)
		if marshalErr != nil {
			return false, marshalErr
		}
		if err := r.marketDB.UpsertActiveMarketSnapshot(store.ActiveMarketSnapshotRecord{
			MarketID: snapshot.MarketID, ConditionID: snapshot.ConditionID.String(), SnapshotJSON: string(encoded),
			Active: true, Closed: false, GammaUpdatedMS: snapshot.GammaUpdatedAtMS,
			SyncedAtUS: snapshot.CatalogSyncedAtUS, RetentionSeconds: int64(retentionPolicy(snapshot).exitGrace / time.Second),
			UMAPinnedUntil: func() int64 {
				if forceSnapshot {
					return time.Now().Add(umaCandidatePin).Unix()
				}
				return 0
			}(),
			CLOBLastSeenAt: func() int64 {
				if snapshotEligible || tradable || clobHot {
					return time.Now().Unix()
				}
				return 0
			}(),
		}); err != nil {
			return false, err
		}
		r.setSnapshot(snapshot)
	} else if live {
		if !forceSnapshot && !tradable && !clobHot {
			// Until the first complete CLOB snapshot arrives, preserve the durable
			// set and only update the immutable market/condition mapping.
			if !r.clobReady.Load() {
				return inserted, nil
			}
			return inserted, nil
		}
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

// gammaSnapshotEligible follows both CLOB state and the market-type lifecycle.
// Long-tail civic markets remain warm while their order book is open; high
// volume recurring markets use a bounded inactive window.
func gammaSnapshotEligible(market uma.GammaMarketMapping) bool {
	return gammaSnapshotEligibleAt(market, time.Now())
}

func gammaSnapshotEligibleAt(market uma.GammaMarketMapping, now time.Time) bool {
	if market.ID == "" || market.ConditionID == "" || market.Closed || market.Archived {
		return false
	}
	if gammaMarketTradable(market) {
		return true
	}
	if !market.EnableOrderBook || gammaResolutionComplete(market.UMAResolutionStatus, market.UMAResolutionStatuses) {
		return false
	}
	policy := retentionPolicyFromGamma(market)
	if policy.inactiveWindow == 0 {
		return true
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, market.UpdatedAt)
	if err != nil || updatedAt.IsZero() || updatedAt.After(now) {
		return true // incomplete Gamma timestamps fail open; the exit TTL still bounds residency.
	}
	return now.Sub(updatedAt) <= policy.inactiveWindow
}

func snapshotFromGamma(market uma.GammaMarketMapping) *store.MarketSnapshot {
	conditionID, _ := store.ParseConditionID(market.ConditionID)
	snapshot := &store.MarketSnapshot{
		MarketID: market.ID, ConditionID: conditionID, Question: market.Question,
		Slug: market.Slug, Category: market.Category,
		SportsMarketType: market.SportsMarketType, TokenIDs: append([]string(nil), market.TokenIDs...),
		Outcomes: append([]string(nil), market.Outcomes...), OutcomePrices: append([]float64(nil), market.OutcomePrices...),
		Active: market.Active, Closed: market.Closed, AcceptingOrders: market.AcceptingOrders,
		EnableOrderBook: market.EnableOrderBook, UMAResolutionStatus: market.UMAResolutionStatus,
		UMAResolutionStatuses: append([]string(nil), market.UMAResolutionStatuses...),
		TakerBaseFee:          market.TakerBaseFee, CatalogSyncedAtUS: time.Now().UnixMicro(),
	}
	if len(snapshot.UMAResolutionStatuses) == 0 && strings.TrimSpace(snapshot.UMAResolutionStatus) != "" {
		snapshot.UMAResolutionStatuses = []string{snapshot.UMAResolutionStatus}
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
	} else {
		// Standalone or not-yet-related Gamma markets still provide a usable
		// downstream title; a later incremental update replaces it with event data.
		snapshot.PolymarketEventTitle = market.Question
		snapshot.PolymarketEventSlug = market.Slug
	}
	for _, tag := range tags {
		snapshot.Tags = append(snapshot.Tags, store.MarketTag{ID: tag.ID})
	}
	snapshot.RetentionClass = retentionPolicyFromGamma(market).class
	// Inactive markets only serve UMA routing. Trading arrays are restored by a
	// later active Gamma update before they can become worker candidates.
	compactResidentSnapshot(snapshot)
	return snapshot
}

func compactResidentSnapshot(snapshot *store.MarketSnapshot) {
	if snapshot == nil {
		return
	}
	// Resolution prose belongs in the durable Gamma source, not the O(1) UMA
	// routing set. It is never used by filtering or worker trade decisions.
	snapshot.Description = ""
	if !snapshot.Active {
		snapshot.TokenIDs = nil
		snapshot.Outcomes = nil
		snapshot.OutcomePrices = nil
		snapshot.TakerBaseFee = 0
	}
}

func gammaResolutionComplete(status string, statuses []string) bool {
	for _, value := range append([]string{status}, statuses...) {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "resolved", "settled", "finalized":
			return true
		}
	}
	return false
}

func retentionPolicy(snapshot *store.MarketSnapshot) marketRetentionPolicy {
	if snapshot == nil {
		return unknownRetention
	}
	switch snapshot.RetentionClass {
	case fastRetention.class:
		return fastRetention
	case normalRetention.class:
		return normalRetention
	case longRetention.class:
		return longRetention
	case unknownRetention.class:
		return unknownRetention
	}
	values := []string{snapshot.Category}
	for _, tag := range snapshot.Tags {
		values = append(values, tag.ID)
	}
	return retentionPolicyForValues(values)
}

func retentionPolicyForValues(values []string) marketRetentionPolicy {
	joined := " " + strings.ToLower(strings.Join(values, " ")) + " "
	if containsMarketClass(joined, []string{" 2 ", " 144 ", " 264 ", "politic", "election", "primary", "geopolitic", "legal", "regulation", "government"}) {
		return longRetention
	}
	if containsMarketClass(joined, []string{"entertainment", "movie", "netflix", "award", "technology", "finance", "company"}) {
		return normalRetention
	}
	if containsMarketClass(joined, []string{" 1 ", " 64 ", " 21 ", "sports", "esports", "soccer", "baseball", "basketball", "tennis", "crypto", "weather", "temperature", "recurring", "up-or-down"}) {
		return fastRetention
	}
	return unknownRetention
}

func retentionPolicyFromGamma(market uma.GammaMarketMapping) marketRetentionPolicy {
	values := []string{market.Category}
	tags := market.Tags
	if len(market.Events) > 0 && len(tags) == 0 {
		tags = market.Events[0].Tags
	}
	for _, tag := range tags {
		values = append(values, tag.ID, tag.Label, tag.Slug)
	}
	return retentionPolicyForValues(values)
}

func containsMarketClass(joined string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(joined, needle) {
			return true
		}
	}
	return false
}

func (r *conditionResolver) setSnapshot(snapshot *store.MarketSnapshot) {
	if snapshot == nil || snapshot.MarketID == "" || snapshot.ConditionID.IsZero() {
		return
	}
	r.mu.Lock()
	r.snapshots[snapshot.ConditionID] = snapshot
	r.mu.Unlock()
	r.publishActiveCatalogStats()
}

func (r *conditionResolver) removeSnapshot(conditionID string) {
	id, err := store.ParseConditionID(conditionID)
	if err != nil {
		return
	}
	r.mu.Lock()
	delete(r.snapshots, id)
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
	return element.Value.(conditionCacheEntry).conditionID.String()
}
func (r *conditionResolver) setCached(marketID, conditionID string) {
	if marketID == "" || conditionID == "" {
		return
	}
	id, parseErr := store.ParseConditionID(conditionID)
	if parseErr != nil {
		return
	}
	r.mu.Lock()
	if element := r.cache[marketID]; element != nil {
		element.Value = conditionCacheEntry{marketID: marketID, conditionID: id}
		r.cacheLRU.MoveToFront(element)
		r.mu.Unlock()
		return
	}
	element := r.cacheLRU.PushFront(conditionCacheEntry{marketID: marketID, conditionID: id})
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
	r.db.SetMarketPrefetchStats(int64(len(r.prefetchQueue)), int64(cap(r.prefetchQueue)), r.prefetchDropped.Load())
}
