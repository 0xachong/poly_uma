// Package syncer 负责从 Polygon 链上实时同步 UMA OO 事件并写入本地存储。
//
// 主循环：
//  1. 读断点，从断点到链头补拉历史（分段 eth_getLogs）
//  2. 建立 WebSocket 订阅
//  3. 收到事件 → 富化（block_ts + condition_id）→ 先内存副本再 SQLite + MySQL audit
//  4. 断线后指数退避重连，重连前再次补拉断线窗口
package syncer

import (
	"context"
	"log"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/polymas/poly_uma/internal/audit"
	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

// Config 同步参数。
type Config struct {
	WssURL         string
	HttpRPCURL     string // 用于 eth_getLogs / block timestamp；空时从 WssURL 推导
	ReconnectDelay time.Duration
	ProxyURL       string // Gamma API 代理，可选
	WorkerCount    int    // 订阅事件并发处理 worker 数，<=0 时使用默认值
	EventQueueSize int    // 订阅事件有界队列大小，<=0 时使用默认值
	// CheckpointFlushInterval 为 checkpoint 刷新间隔。
	// <=0 时默认 1s，避免每条事件都写 checkpoint。
	CheckpointFlushInterval time.Duration
}

// Run 启动同步主循环，阻塞直至 ctx 取消。
// mem 可选：非 nil 时，每条新事件在写入 SQLite 之前先写入内存副本；SQLite 失败则回滚内存。
func Run(ctx context.Context, cfg Config, db *store.SQLite, au *audit.MySQL, mem *store.MemReplica) {
	httpURL := cfg.HttpRPCURL
	if httpURL == "" {
		httpURL = uma.WssToHttp(cfg.WssURL)
	}
	blockTsCache := newTsCache()
	reconnectDelay := cfg.ReconnectDelay
	if reconnectDelay <= 0 {
		reconnectDelay = 10 * time.Second
	}
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// ── 补拉历史 ────────────────────────────────────────────────────────
		if err := backfill(ctx, cfg, httpURL, db, au, blockTsCache, mem); err != nil {
			log.Printf("[WARN] backfill: %v", err)
		}

		// ── 建立 WebSocket 订阅 ──────────────────────────────────────────────
		wssClient, err := uma.NewClient(ctx, cfg.WssURL)
		if err != nil {
			wait := backoffDuration(attempt, reconnectDelay)
			log.Printf("[WARN] WSS 连接失败: %v，%v 后重试", err, wait)
			attempt++
			sleep(ctx, wait)
			continue
		}

		evCh, cleanup, err := wssClient.Subscribe(ctx)
		if err != nil {
			wssClient.Close()
			wait := backoffDuration(attempt, reconnectDelay)
			log.Printf("[WARN] Subscribe 失败: %v，%v 后重试", err, wait)
			attempt++
			sleep(ctx, wait)
			continue
		}

		log.Printf("[INFO] WebSocket 订阅已建立，等待 UMA 事件…")
		attempt = 0

		// ── 消费订阅事件：有界队列 + worker pool ───────────────────────────
		workerCount := cfg.WorkerCount
		if workerCount <= 0 {
			workerCount = defaultWorkerCount()
		}
		queueSize := cfg.EventQueueSize
		if queueSize <= 0 {
			queueSize = 4096
		}
		flushEvery := cfg.CheckpointFlushInterval
		if flushEvery <= 0 {
			flushEvery = time.Second
		}
		jobs := make(chan *uma.SubscribedEvent, queueSize)
		var maxHandledBlock atomic.Uint64
		var wg sync.WaitGroup
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for subEv := range jobs {
					if subEv == nil || subEv.Event == nil {
						continue
					}
					if err := handleEvent(ctx, subEv.Event, int(subEv.Raw.Index),
						db, au, httpURL, blockTsCache, cfg.ProxyURL, mem); err != nil {
						log.Printf("[WARN] handleEvent: %v", err)
						continue
					}
					if bn := subEv.Raw.BlockNumber; bn > 0 {
						setMaxBlock(&maxHandledBlock, bn)
					}
				}
			}()
		}
		stopFlush := make(chan struct{})
		go func() {
			ticker := time.NewTicker(flushEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					flushCheckpoint(db, &maxHandledBlock)
				case <-stopFlush:
					return
				case <-ctx.Done():
					return
				}
			}
		}()

		for subEv := range evCh {
			// 仅用于 healthz lag 计算，实时更新已观测链头。
			if bn := subEv.Raw.BlockNumber; bn > 0 {
				db.SetLatestSeenBlock(bn)
			}
			// 有界队列阻塞入队，避免无限积压导致内存膨胀。
			jobs <- subEv
		}
		close(jobs)
		wg.Wait()
		close(stopFlush)
		flushCheckpoint(db, &maxHandledBlock)

		cleanup()
		wssClient.Close()

		if ctx.Err() != nil {
			return
		}
		wait := backoffDuration(attempt, reconnectDelay)
		log.Printf("[INFO] 订阅断开，%v 后重连…", wait)
		attempt++
		sleep(ctx, wait)
	}
}

// backfill 从 checkpoint 补拉到当前链头，分段 2000 块。
// 若数据库无历史记录（checkpoint == 0），直接跳过。
func backfill(ctx context.Context, cfg Config, httpURL string,
	db *store.SQLite, au *audit.MySQL, cache *tsCache, mem *store.MemReplica) error {

	if httpURL == "" {
		return nil
	}

	checkpoint, _ := db.GetCheckpoint()
	if checkpoint == 0 {
		log.Printf("[INFO] 无历史记录，跳过补拉")
		return nil
	}

	httpClient, err := uma.NewClient(ctx, httpURL)
	if err != nil {
		return err
	}
	defer httpClient.Close()

	latest, err := httpClient.LatestBlock(ctx)
	if err != nil {
		return err
	}
	db.SetLatestSeenBlock(latest)

	if latest <= checkpoint {
		return nil
	}
	start := checkpoint + 1
	log.Printf("[INFO] 补拉历史: from=%d to=%d", start, latest)

	const step = uint64(2000)
	const maxRetries = 3
	for cur := start; cur <= latest; cur += step {
		end := min(cur+step-1, latest)
		var logs []ethtypes.Log
		var fetchErr error
		for retry := 0; retry < maxRetries; retry++ {
			logs, fetchErr = httpClient.FetchLogs(ctx, cur, end)
			if fetchErr == nil {
				break
			}
			log.Printf("[WARN] FetchLogs [%d,%d] attempt %d/%d: %v", cur, end, retry+1, maxRetries, fetchErr)
			sleep(ctx, time.Duration(retry+1)*2*time.Second)
		}
		if fetchErr != nil {
			log.Printf("[ERROR] FetchLogs [%d,%d] 全部重试失败，跳过: %v", cur, end, fetchErr)
			continue
		}
		for _, vLog := range logs {
			ev, err := uma.ParseLog(vLog)
			if err != nil || ev == nil {
				continue
			}
			if err := handleEvent(ctx, ev, int(vLog.Index),
				db, au, httpURL, cache, cfg.ProxyURL, mem); err != nil {
				log.Printf("[WARN] backfill handleEvent: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// handleEvent 富化单条事件（timestamp + condition_id）并写入 SQLite + MySQL audit。
// 若 mem 非 nil：先 InsertUnique 到内存副本，再写 SQLite；失败则 RevertInsert；成功则回填 id 并 WS 广播 propose/dispute。
// condition_id 优先级：Polymarket Gamma > question_id（init/resolved）> identifier（其他）
func handleEvent(ctx context.Context, ev *uma.Event, logIndex int,
	db *store.SQLite, au *audit.MySQL,
	httpURL string, cache *tsCache, proxyURL string, mem *store.MemReplica) error {

	// 获取区块时间戳（带缓存，复用 RPC 连接）
	blockTs := cache.getOrFetch(ctx, ev.BlockNumber(), httpURL)

	eventType := kindToType(ev.Kind)
	txHash := ev.TxHash()
	blockNumber := ev.BlockNumber()
	marketID := ev.MarketID()

	// condition_id：Gamma > question_id > identifier
	conditionID := ""
	if marketID != "" {
		conditionID = uma.GammaConditionID(marketID, proxyURL)
	}
	if conditionID == "" {
		conditionID = ev.QuestionID()
	}
	if conditionID == "" {
		conditionID = ev.Identifier()
	}

	row := store.EventRow{
		EventType:   eventType,
		TxHash:      txHash,
		LogIndex:    logIndex,
		BlockNumber: blockNumber,
		Timestamp:   blockTs,
		ConditionID: conditionID,
		MarketID:    marketID,
		Price:       uma.ScalePrice(ev.Price()),
	}
	inMem := mem != nil && blockTs >= store.RecentMemoryCutoffUnix()
	if inMem {
		if !mem.InsertUnique(row) {
			return nil // 与内存去重一致：已存在
		}
	}
	inserted, lastID, cursorID, err := db.InsertEvent(eventType, txHash, logIndex, blockNumber, blockTs,
		conditionID, marketID, row.Price)
	if err != nil {
		if inMem {
			mem.RevertInsert(eventType, txHash, logIndex)
		}
		return err
	}
	if !inserted {
		if inMem {
			mem.RevertInsert(eventType, txHash, logIndex)
		}
		return nil
	}
	if inMem && lastID > 0 {
		mem.SetCursorID(eventType, txHash, logIndex, lastID, cursorID)
		row.ID = lastID
		row.CursorID = cursorID
	}
	if inMem && (eventType == "propose" || eventType == "dispute") {
		mem.BroadcastNew(eventType, row)
	}

	// 写 MySQL audit（fire-and-forget，失败仅 WARN）
	au.Insert(eventType, txHash, logIndex, blockNumber, blockTs, conditionID, marketID, ev)

	log.Printf("[%s] block=%d market=%s tx=%s…",
		ev.Kind, blockNumber, marketID, txHash[:min(20, len(txHash))])
	return nil
}

// ── 工具 ─────────────────────────────────────────────────────────────────────

func kindToType(kind string) string {
	switch kind {
	case "QuestionInitialized":
		return "init"
	case "RequestPrice":
		return "request"
	case "ProposePrice":
		return "propose"
	case "DisputePrice":
		return "dispute"
	case "QuestionResolved":
		return "resolved"
	case "Settle":
		return "settle"
	default:
		return kind
	}
}

func backoffDuration(attempt int, base time.Duration) time.Duration {
	d := base * time.Duration(math.Pow(2, float64(min(attempt, 6))))
	const max = 60 * time.Second
	if d > max {
		d = max
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}


// tsCache 是区块时间戳的内存缓存（区块时间不可变）。
// 内置一个可复用的 HTTP RPC client，避免每次获取 timestamp 都新建连接。
type tsCache struct {
	mu      sync.RWMutex
	m       map[uint64]int64
	rpcOnce sync.Once
	rpcURL  string
	rpc     *uma.Client
}

func newTsCache() *tsCache { return &tsCache{m: make(map[uint64]int64)} }

func (c *tsCache) get(block uint64) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[block]
}

func (c *tsCache) set(block uint64, ts int64) {
	c.mu.Lock()
	c.m[block] = ts
	// 缓存超过 5000 条时清理较旧的 3/4，防止内存无限增长
	if len(c.m) > 5000 {
		c.evictLocked()
	}
	c.mu.Unlock()
}

// evictLocked 保留 block number 最大的 1/4 条目。
func (c *tsCache) evictLocked() {
	var maxBlock uint64
	for b := range c.m {
		if b > maxBlock {
			maxBlock = b
		}
	}
	keep := uint64(len(c.m) / 4)
	if keep < 100 {
		keep = 100
	}
	cutoff := maxBlock - keep
	for b := range c.m {
		if b < cutoff {
			delete(c.m, b)
		}
	}
}

// getOrFetch 先查缓存，未命中则通过复用的 RPC client 获取并缓存。
func (c *tsCache) getOrFetch(ctx context.Context, block uint64, httpURL string) int64 {
	if ts := c.get(block); ts != 0 {
		return ts
	}
	if httpURL == "" {
		return 0
	}
	c.rpcOnce.Do(func() {
		c.rpcURL = httpURL
		client, err := uma.NewClient(ctx, httpURL)
		if err != nil {
			log.Printf("[WARN] tsCache: 创建 RPC client 失败: %v", err)
			return
		}
		c.rpc = client
	})
	if c.rpc == nil {
		return 0
	}
	ts, err := c.rpc.BlockTimestamp(ctx, block)
	if err != nil {
		return 0
	}
	c.set(block, ts)
	return ts
}

func defaultWorkerCount() int {
	n := runtime.NumCPU()
	if n < 4 {
		return 4
	}
	if n > 16 {
		return 16
	}
	return n
}

func setMaxBlock(dst *atomic.Uint64, v uint64) {
	for {
		cur := dst.Load()
		if v <= cur {
			return
		}
		if dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

func flushCheckpoint(db *store.SQLite, maxHandled *atomic.Uint64) {
	if db == nil || maxHandled == nil {
		return
	}
	if b := maxHandled.Load(); b > 0 {
		_ = db.SetCheckpoint(b)
	}
}
