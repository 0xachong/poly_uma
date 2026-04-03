// Package store：MemReplica 为 uma_oo_events 的「最近 12 小时」内存副本（对 API 默认读路径）。
// 启动时从 SQLite 加载 timestamp >= 当前时刻-12h 的行；后台定期淘汰过期行。
// 时间范围超出 12h 的列表查询须传 source=sqlite 强制读库；新事件仅当 timestamp 落在窗口内时才先写内存再写库。
package store

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// MemRecentWindowSeconds 内存副本保留的链上事件时间窗（秒），与 API 默认 source=memory 可答范围一致。
const MemRecentWindowSeconds = 12 * 3600

type eventKey struct {
	tx  string
	log int
}

// RecentMemoryCutoffUnix 返回「当前时刻 − 12h」的 Unix 秒，供 API 校验 from_ts 与 syncer 判断是否入内存。
func RecentMemoryCutoffUnix() int64 {
	return time.Now().Unix() - MemRecentWindowSeconds
}

// MemReplica 最近 12h 事件内存副本（按 event_type 分桶，桶内按 timestamp、tx、log_index、id 升序）。
type MemReplica struct {
	mu sync.RWMutex
	// 按 event_type 分桶，与 QueryByType 语义一致
	byType map[string][]EventRow
	seen   map[eventKey]struct{}

	proposeSubs map[chan EventRow]struct{}
	disputeSubs map[chan EventRow]struct{}
}

// NewMemReplica 创建空副本（须再调用 LoadFromSQLite）。
func NewMemReplica() *MemReplica {
	return &MemReplica{
		byType:      make(map[string][]EventRow),
		seen:        make(map[eventKey]struct{}),
		proposeSubs: make(map[chan EventRow]struct{}),
		disputeSubs: make(map[chan EventRow]struct{}),
	}
}

// LoadFromSQLite 启动时加载「最近 12 小时」内的行（单次范围扫描），构建内存索引。
func (m *MemReplica) LoadFromSQLite(s *SQLite) error {
	rows, err := s.ScanEventsSince(RecentMemoryCutoffUnix())
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byType = make(map[string][]EventRow)
	m.seen = make(map[eventKey]struct{})
	for _, r := range rows {
		k := eventKey{r.TxHash, r.LogIndex}
		if _, dup := m.seen[k]; dup {
			continue
		}
		m.seen[k] = struct{}{}
		m.byType[r.EventType] = append(m.byType[r.EventType], r)
	}
	return nil
}

// Stats 返回当前内存中的去重事件条数（用于启动日志）。
func (m *MemReplica) Stats() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fmt.Sprintf("unique_events=%d (last_%dh)", len(m.seen), MemRecentWindowSeconds/3600)
}

// evictBucketLocked 去掉该桶内 timestamp < cutoff 的头部，并同步 seen。
func (m *MemReplica) evictBucketLocked(eventType string, cutoff int64) {
	sl := m.byType[eventType]
	if len(sl) == 0 {
		return
	}
	i := 0
	for i < len(sl) && sl[i].Timestamp < cutoff {
		delete(m.seen, eventKey{sl[i].TxHash, sl[i].LogIndex})
		i++
	}
	if i == 0 {
		return
	}
	m.byType[eventType] = append([]EventRow(nil), sl[i:]...)
}

// evictAllLocked 淘汰所有桶内早于 cutoff 的事件。
func (m *MemReplica) evictAllLocked(cutoff int64) {
	for et := range m.byType {
		m.evictBucketLocked(et, cutoff)
	}
}

// RunEvict 后台按 interval 淘汰超过 12h 的数据，避免内存无限增长。
func (m *MemReplica) RunEvict(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		m.evictAllLocked(RecentMemoryCutoffUnix())
		m.mu.Unlock()
	}
}

func lessEvent(a, b EventRow) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	if a.TxHash != b.TxHash {
		return a.TxHash < b.TxHash
	}
	if a.LogIndex != b.LogIndex {
		return a.LogIndex < b.LogIndex
	}
	return a.ID < b.ID
}

// InsertUnique 在写入 SQLite 之前调用（调用方须保证 row.Timestamp 落在 12h 窗口内，见 syncer）：
// 若 (tx_hash,log_index) 已存在则返回 false；否则先淘汰该桶过期数据再按序插入并返回 true。
// 调用方在 SQLite 失败时应 RevertInsert。
func (m *MemReplica) InsertUnique(row EventRow) bool {
	k := eventKey{row.TxHash, row.LogIndex}
	m.mu.Lock()
	defer m.mu.Unlock()
	// 淘汰交给后台 RunEvict，不再在写路径中做，减少 WriteLock 持有时间。
	if _, ok := m.seen[k]; ok {
		return false
	}
	et := row.EventType
	sl := m.byType[et]
	pos := sort.Search(len(sl), func(i int) bool {
		return !lessEvent(sl[i], row)
	})
	sl = append(sl, EventRow{})
	copy(sl[pos+1:], sl[pos:])
	sl[pos] = row
	m.byType[et] = sl
	m.seen[k] = struct{}{}
	return true
}

// RevertInsert 在 SQLite 写入失败时撤销 InsertUnique。
// 刚插入的行在桶尾部附近，从后往前遍历可快速命中。
func (m *MemReplica) RevertInsert(eventType, txHash string, logIndex int) {
	k := eventKey{txHash, logIndex}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.seen, k)
	sl := m.byType[eventType]
	for i := len(sl) - 1; i >= 0; i-- {
		if sl[i].TxHash == txHash && sl[i].LogIndex == logIndex {
			m.byType[eventType] = append(sl[:i], sl[i+1:]...)
			return
		}
	}
}

// SetRowID 在 SQLite 成功插入后回填自增 id（供 latest 等按 id 排序）。
// 刚插入的行在桶尾部附近，从后往前遍历可快速命中，减少 WriteLock 持有时间。
func (m *MemReplica) SetRowID(eventType, txHash string, logIndex int, id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sl := m.byType[eventType]
	for i := len(sl) - 1; i >= 0; i-- {
		if sl[i].TxHash == txHash && sl[i].LogIndex == logIndex {
			sl[i].ID = id
			return
		}
	}
}

// QueryByType 与 SQLite.QueryByType 语义一致，仅扫内存中该类型桶（约最近 12h）。
// cursor 为上一页最后一条记录的 id，0 表示从头开始。
// 桶内按 timestamp 升序，利用二分搜索跳到 fromTs 位置，减少持锁时间。
func (m *MemReplica) QueryByType(eventType string, fromTs, toTs int64, limit int, cursor int64) []EventRow {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sl := m.byType[eventType]
	// 二分查找第一个 timestamp >= fromTs 的位置
	start := sort.Search(len(sl), func(i int) bool {
		return sl[i].Timestamp >= fromTs
	})
	out := make([]EventRow, 0, min(limit, len(sl)-start))
	for i := start; i < len(sl); i++ {
		r := sl[i]
		if r.Timestamp > toTs {
			break // 桶内有序，后续全部超出范围
		}
		if cursor > 0 && r.ID <= cursor {
			continue
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// QueryLatestProposed 等价于 SQLite 的 ORDER BY timestamp DESC, id DESC LIMIT n。
// 桶内已按 (timestamp, txHash, logIndex, id) 升序排列，直接从尾部倒取即可，无需全量拷贝+排序。
func (m *MemReplica) QueryLatestProposed(limit int) []EventRow {
	if limit <= 0 {
		limit = 1
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	sl := m.byType["propose"]
	if len(sl) == 0 {
		return nil
	}
	n := limit
	if n > len(sl) {
		n = len(sl)
	}
	out := make([]EventRow, n)
	for i := 0; i < n; i++ {
		out[i] = sl[len(sl)-1-i] // 倒序取出
	}
	return out
}

// Subscribe 返回 propose/dispute 的实时推送通道（与旧 RecentCache 行为一致）。
func (m *MemReplica) Subscribe(eventType string) (<-chan EventRow, func()) {
	ch := make(chan EventRow, 100)
	m.mu.Lock()
	switch eventType {
	case "propose":
		m.proposeSubs[ch] = struct{}{}
	case "dispute":
		m.disputeSubs[ch] = struct{}{}
	default:
		m.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	m.mu.Unlock()

	cancel := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		switch eventType {
		case "propose":
			delete(m.proposeSubs, ch)
		case "dispute":
			delete(m.disputeSubs, ch)
		}
		close(ch)
	}
	return ch, cancel
}

// BroadcastNew 在 SQLite 写入成功后调用，向 WebSocket 订阅者非阻塞推送。
// 如果订阅者缓冲满则丢弃并打印警告，避免阻塞写路径。
func (m *MemReplica) BroadcastNew(eventType string, row EventRow) {
	var subs map[chan EventRow]struct{}
	m.mu.RLock()
	switch eventType {
	case "propose":
		subs = m.proposeSubs
	case "dispute":
		subs = m.disputeSubs
	default:
		m.mu.RUnlock()
		return
	}
	for ch := range subs {
		select {
		case ch <- row:
		default:
			log.Printf("[WARN] WS %s 订阅者缓冲满，丢弃事件 tx=%s", eventType, row.TxHash)
		}
	}
	m.mu.RUnlock()
}
