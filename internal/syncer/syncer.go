// Package syncer 负责从 Polygon 链上实时同步 UMA OO 事件并写入本地存储。
//
// 主循环：
//  1. 读断点，从断点到链头补拉历史（分段 eth_getLogs）
//  2. 建立 WebSocket 订阅
//  3. 收到事件 → 富化（block_ts + condition_id）→ 双写（SQLite + MySQL audit）
//  4. 断线后指数退避重连，重连前再次补拉断线窗口
package syncer

import (
	"context"
	"log"
	"math"
	"time"

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
}

// Run 启动同步主循环，阻塞直至 ctx 取消。
func Run(ctx context.Context, cfg Config, db *store.SQLite, au *audit.MySQL) {
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
		if err := backfill(ctx, cfg, httpURL, db, au, blockTsCache); err != nil {
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

		// ── 消费订阅事件 ─────────────────────────────────────────────────────
		for subEv := range evCh {
			if subEv.Event == nil {
				continue
			}
			if err := handleEvent(ctx, subEv.Event, int(subEv.Raw.Index),
				db, au, httpURL, blockTsCache, cfg.ProxyURL); err != nil {
				log.Printf("[WARN] handleEvent: %v", err)
			}
			// 更新断点
			if bn := subEv.Raw.BlockNumber; bn > 0 {
				_ = db.SetCheckpoint(bn)
				db.SetLatestSeenBlock(bn)
			}
		}

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
	db *store.SQLite, au *audit.MySQL, cache *tsCache) error {

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
	for cur := start; cur <= latest; cur += step {
		end := min64(cur+step-1, latest)
		logs, err := httpClient.FetchLogs(ctx, cur, end)
		if err != nil {
			log.Printf("[WARN] FetchLogs [%d,%d]: %v", cur, end, err)
			continue
		}
		for _, vLog := range logs {
			ev, err := uma.ParseLog(vLog)
			if err != nil || ev == nil {
				continue
			}
			if err := handleEvent(ctx, ev, int(vLog.Index),
				db, au, httpURL, cache, cfg.ProxyURL); err != nil {
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
// condition_id 优先级：Polymarket Gamma > question_id（init/resolved）> identifier（其他）
func handleEvent(ctx context.Context, ev *uma.Event, logIndex int,
	db *store.SQLite, au *audit.MySQL,
	httpURL string, cache *tsCache, proxyURL string) error {

	// 获取区块时间戳（带缓存）
	blockTs := cache.get(ev.BlockNumber())
	if blockTs == 0 && httpURL != "" {
		httpClient, err := uma.NewClient(ctx, httpURL)
		if err == nil {
			ts, err := httpClient.BlockTimestamp(ctx, ev.BlockNumber())
			httpClient.Close()
			if err == nil {
				blockTs = ts
				cache.set(ev.BlockNumber(), ts)
			}
		}
	}

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

	// 写 SQLite（去重）
	inserted, err := db.InsertEvent(eventType, txHash, logIndex, blockNumber, blockTs,
		conditionID, marketID, uma.ScalePrice(ev.Price()))
	if err != nil {
		return err
	}
	if !inserted {
		return nil // 已处理过
	}

	// 写 MySQL audit（fire-and-forget，失败仅 WARN）
	au.Insert(eventType, txHash, logIndex, blockNumber, blockTs, conditionID, marketID, ev)

	log.Printf("[%s] block=%d market=%s tx=%s…",
		ev.Kind, blockNumber, marketID, txHash[:min(20, len(txHash))])
	return nil
}

// ── 工具 ─────────────────────────────────────────────────────────────────────

func kindToType(kind string) string {
	m := map[string]string{
		"QuestionInitialized": "init",
		"RequestPrice":        "request",
		"ProposePrice":        "propose",
		"DisputePrice":        "dispute",
		"QuestionResolved":    "resolved",
		"Settle":              "settle",
	}
	if t, ok := m[kind]; ok {
		return t
	}
	return kind
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

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// tsCache 是区块时间戳的内存缓存（区块时间不可变，永不过期）。
type tsCache struct {
	m map[uint64]int64
}

func newTsCache() *tsCache { return &tsCache{m: make(map[uint64]int64)} }

func (c *tsCache) get(block uint64) int64  { return c.m[block] }
func (c *tsCache) set(block uint64, ts int64) { c.m[block] = ts }
