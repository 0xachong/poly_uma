package syncer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

// conditionResolver keeps Gamma network traffic out of the real-time event path.
type conditionResolver struct {
	db       *store.SQLite
	mem      *store.MemReplica
	proxyURL string

	mu      sync.RWMutex
	cache   map[string]string
	pending map[string]struct{}
	jobs    chan string
}

func newConditionResolver(db *store.SQLite, mem *store.MemReplica, proxyURL string) *conditionResolver {
	cache, err := db.LoadMarketConditionMap()
	if err != nil {
		log.Printf("[WARN] condition resolver cache preload failed: %v", err)
		cache = make(map[string]string)
	}
	log.Printf("[INFO] condition resolver cache loaded: markets=%d", len(cache))
	return &conditionResolver{
		db:       db,
		mem:      mem,
		proxyURL: proxyURL,
		cache:    cache,
		pending:  make(map[string]struct{}),
		jobs:     make(chan string, 4096),
	}
}

// Resolve returns immediately. Cache misses are enriched in the background.
func (r *conditionResolver) Resolve(marketID string) string {
	if marketID == "" {
		return ""
	}
	r.mu.RLock()
	conditionID := r.cache[marketID]
	_, pending := r.pending[marketID]
	r.mu.RUnlock()
	if conditionID != "" || pending {
		return conditionID
	}

	r.mu.Lock()
	if conditionID = r.cache[marketID]; conditionID == "" {
		if _, exists := r.pending[marketID]; !exists {
			r.pending[marketID] = struct{}{}
			select {
			case r.jobs <- marketID:
			default:
				delete(r.pending, marketID)
				log.Printf("[WARN] condition resolver queue full: market=%s", marketID)
			}
		}
	}
	r.mu.Unlock()
	return conditionID
}

func (r *conditionResolver) Run(ctx context.Context, workers int) {
	if workers <= 0 {
		workers = 4
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case marketID := <-r.jobs:
					r.enrich(ctx, marketID)
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

func (r *conditionResolver) enrich(ctx context.Context, marketID string) {
	defer func() {
		r.mu.Lock()
		delete(r.pending, marketID)
		r.mu.Unlock()
	}()

	var conditionID string
	for attempt := 0; attempt < 3 && ctx.Err() == nil; attempt++ {
		conditionID = uma.GammaConditionID(marketID, r.proxyURL)
		if conditionID != "" {
			break
		}
		wait := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}[attempt]
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
	if conditionID == "" {
		log.Printf("[WARN] condition resolver enrichment failed: market=%s", marketID)
		return
	}

	r.mu.Lock()
	r.cache[marketID] = conditionID
	r.mu.Unlock()
	if err := r.db.UpdateConditionIDByMarketID(marketID, conditionID); err != nil {
		log.Printf("[WARN] condition resolver SQLite update failed: market=%s err=%v", marketID, err)
		return
	}
	if r.mem != nil {
		r.mem.UpdateConditionIDByMarketID(marketID, conditionID)
	}
}
