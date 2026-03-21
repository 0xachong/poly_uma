// Package store：最近 12 小时的 propose / dispute 事件内存缓存，用于 API 快速响应。
// 超过 12 小时的数据由调用方从 SQLite 查询；同步由 syncer 在写入 SQLite 后推入本缓存。
package store

import (
	"sync"
	"time"
)

const recentWindowSeconds = 12 * 3600 // 12 小时

// RecentCache 仅缓存 propose 与 dispute 两类事件，按 timestamp 保留最近 12 小时。
type RecentCache struct {
	mu      sync.RWMutex
	propose []EventRow
	dispute []EventRow

	proposeSubs map[chan EventRow]struct{}
	disputeSubs map[chan EventRow]struct{}
}

// NewRecentCache 创建空缓存。
func NewRecentCache() *RecentCache {
	return &RecentCache{
		propose:     nil,
		dispute:     nil,
		proposeSubs: make(map[chan EventRow]struct{}),
		disputeSubs: make(map[chan EventRow]struct{}),
	}
}

// cutoffTs 返回“当前时间 - 12 小时”的 Unix 秒，用于过滤与 evict。
func cutoffTs() int64 {
	return time.Now().Unix() - recentWindowSeconds
}

// evict 移除早于 12 小时的条目，保持按 timestamp 升序。
func (c *RecentCache) evictLocked(rows []EventRow) []EventRow {
	cutoff := cutoffTs()
	i := 0
	for i < len(rows) && rows[i].Timestamp < cutoff {
		i++
	}
	if i == 0 {
		return rows
	}
	return append([]EventRow(nil), rows[i:]...)
}

// Append 在写入 SQLite 成功后由 syncer 调用，仅接受 propose / dispute。
// 会先淘汰超过 12 小时的旧数据再追加。
func (c *RecentCache) Append(eventType string, row EventRow) {
	if eventType != "propose" && eventType != "dispute" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := cutoffTs()
	if row.Timestamp < cutoff {
		return
	}
	var subs map[chan EventRow]struct{}
	switch eventType {
	case "propose":
		c.propose = c.evictLocked(c.propose)
		c.propose = append(c.propose, row)
		subs = c.proposeSubs
	case "dispute":
		c.dispute = c.evictLocked(c.dispute)
		c.dispute = append(c.dispute, row)
		subs = c.disputeSubs
	}
	// 向订阅者广播新事件（非阻塞）
	for ch := range subs {
		select {
		case ch <- row:
		default:
		}
	}
}

// Subscribe 返回指定类型事件的订阅通道和取消函数。
// 仅支持 "propose" 与 "dispute"。
func (c *RecentCache) Subscribe(eventType string) (<-chan EventRow, func()) {
	ch := make(chan EventRow, 100)
	c.mu.Lock()
	switch eventType {
	case "propose":
		c.proposeSubs[ch] = struct{}{}
	case "dispute":
		c.disputeSubs[ch] = struct{}{}
	default:
		c.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	c.mu.Unlock()

	cancel := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		switch eventType {
		case "propose":
			delete(c.proposeSubs, ch)
		case "dispute":
			delete(c.disputeSubs, ch)
		}
		close(ch)
	}
	return ch, cancel
}

// Query 从缓存中查询指定类型、时间范围内的数据，与 DB 的 QueryByType 语义一致：
// timestamp in [fromTs, toTs]，按 timestamp ASC、transaction_hash ASC，游标分页。
// 仅当请求时间范围完全落在“最近 12 小时”内且类型为 propose/dispute 时才有意义；
// 返回 (rows, true) 表示命中缓存（rows 可能为空，表示该范围内确实无数据）；
// (nil, false) 表示应回退 SQLite：类型不对、时间超出 12h、或该类型缓存尚无任何条（冷启动未预热）。
func (c *RecentCache) Query(eventType string, fromTs, toTs int64, limit int, cursor string) ([]EventRow, bool) {
	if eventType != "propose" && eventType != "dispute" {
		return nil, false
	}
	cutoff := cutoffTs()
	if fromTs < cutoff || toTs < fromTs {
		return nil, false
	}

	c.mu.RLock()
	var src []EventRow
	switch eventType {
	case "propose":
		src = c.propose
	case "dispute":
		src = c.dispute
	default:
		c.mu.RUnlock()
		return nil, false
	}
	// 缓存里该类型尚无任何条：视为未预热，回退 SQLite（可能库里有历史数据但进程内还没 Append）。
	if len(src) == 0 {
		c.mu.RUnlock()
		return nil, false
	}
	// 拷贝一份，避免长时间持锁；与 DB 一致：ORDER BY timestamp ASC, transaction_hash ASC，cursor 为上一页最后一条 tx_hash
	out := make([]EventRow, 0, min(len(src), limit+1))
	for i := range src {
		r := &src[i]
		if r.Timestamp < fromTs || r.Timestamp > toTs {
			continue
		}
		if cursor != "" && r.TxHash <= cursor {
			continue
		}
		out = append(out, *r)
		if len(out) >= limit {
			break
		}
	}
	c.mu.RUnlock()

	// 时间范围落在 12h 内且缓存里已有该类型数据：筛完为 0 条也是合法命中（不应再扫 SQLite）。
	return out, true
}

// QueryLatestProposed 从缓存取最新一条 propose（timestamp 最大），若缓存为空或全部过期则返回 (nil, false)。
func (c *RecentCache) QueryLatestProposed() ([]EventRow, bool) {
	c.mu.RLock()
	src := c.propose
	cutoff := cutoffTs()
	c.mu.RUnlock()
	if len(src) == 0 {
		return nil, false
	}
	// propose 按时间升序追加，最新在末尾
	for i := len(src) - 1; i >= 0; i-- {
		if src[i].Timestamp >= cutoff {
			return []EventRow{src[i]}, true
		}
	}
	return nil, false
}

// RunEvict 后台定期淘汰超过 12 小时的条目，避免内存无限增长。可由 main 启动 goroutine 调用。
func (c *RecentCache) RunEvict(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		c.propose = c.evictLocked(c.propose)
		c.dispute = c.evictLocked(c.dispute)
		c.mu.Unlock()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
