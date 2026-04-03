// Package store 提供本地 SQLite 存储：事件去重流水（uma_oo_events）+ 断点（syncer_checkpoint）。
// 表结构与 Python 版 uma_subscribe_mysql.py 保持一致，可共用同一数据库文件。
package store

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS uma_oo_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type       TEXT    NOT NULL,
    transaction_hash TEXT    NOT NULL,
    log_index        INTEGER NOT NULL,
    block_number     INTEGER,
    timestamp        INTEGER,
    condition_id     TEXT,
    market_id        TEXT,
    price            TEXT,
    UNIQUE (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_ev_type      ON uma_oo_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ev_ts        ON uma_oo_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_ev_market    ON uma_oo_events(market_id);
CREATE INDEX IF NOT EXISTS idx_ev_condition ON uma_oo_events(condition_id);
CREATE INDEX IF NOT EXISTS idx_ev_type_ts   ON uma_oo_events(event_type, timestamp);

CREATE TABLE IF NOT EXISTS syncer_checkpoint (
    id         INTEGER PRIMARY KEY CHECK(id = 1),
    last_block INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO syncer_checkpoint(id, last_block, updated_at) VALUES (1, 0, 0);
`

// EventRow 对应 uma_oo_events 表的一行，用于 API 响应。
type EventRow struct {
	ID          int64
	EventType   string
	TxHash      string
	LogIndex    int
	BlockNumber uint64
	Timestamp   int64
	ConditionID string
	MarketID    string
	Price       string
}

// SQLite 是本地 SQLite 存储。
type SQLite struct {
	db              *sql.DB
	latestSeenBlock atomic.Uint64
}

// Open 打开（或创建）SQLite 数据库文件并初始化 schema。
func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_journal=WAL&_timeout=5000", path))
	if err != nil {
		return nil, fmt.Errorf("sql.Open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &SQLite{db: db}, nil
}

// Close 关闭数据库连接。
func (s *SQLite) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// SetLatestSeenBlock 由 syncer 调用，更新已观察到的最新链头块号（用于 healthz lag 计算）。
func (s *SQLite) SetLatestSeenBlock(b uint64) { s.latestSeenBlock.Store(b) }

// LatestSeenBlock 返回已观察的链头块号。
func (s *SQLite) LatestSeenBlock() uint64 { return s.latestSeenBlock.Load() }

// ── 断点 ─────────────────────────────────────────────────────────────────────

// GetCheckpoint 返回上次成功处理的最后区块号。
func (s *SQLite) GetCheckpoint() (uint64, error) {
	var b uint64
	err := s.db.QueryRow("SELECT last_block FROM syncer_checkpoint WHERE id=1").Scan(&b)
	return b, err
}

// SetCheckpoint 更新断点区块号。
func (s *SQLite) SetCheckpoint(block uint64) error {
	_, err := s.db.Exec(
		"UPDATE syncer_checkpoint SET last_block=?, updated_at=? WHERE id=1",
		block, time.Now().Unix(),
	)
	return err
}

// ── 事件流水 ──────────────────────────────────────────────────────────────────

// InsertEvent 幂等写入一条事件。返回 true 表示新行（首次写入），以及新行的自增 id（未插入时为 0）。
func (s *SQLite) InsertEvent(eventType, txHash string, logIndex int,
	blockNumber uint64, timestamp int64,
	conditionID, marketID, price string) (inserted bool, lastID int64, err error) {

	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO uma_oo_events
		 (event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		eventType, txHash, logIndex, blockNumber, timestamp,
		nullStr(conditionID), nullStr(marketID), nullStr(price),
	)
	if err != nil {
		return false, 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, 0, nil
	}
	id, _ := res.LastInsertId()
	return true, id, nil
}

// ── API 查询 ──────────────────────────────────────────────────────────────────

// QueryByType 按事件类型和时间范围查询，支持游标分页（cursor 为上一页最后一条记录的 id）。
func (s *SQLite) QueryByType(eventType string, fromTs, toTs int64, limit int, cursor int64) ([]EventRow, error) {
	args := []interface{}{eventType, fromTs, toTs}
	q := `SELECT id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price
	      FROM uma_oo_events
	      WHERE event_type = ? AND timestamp >= ? AND timestamp <= ?`
	if cursor > 0 {
		q += " AND id > ?"
		args = append(args, cursor)
	}
	q += " ORDER BY id ASC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		var conditionID, marketID, price sql.NullString
		if err := rows.Scan(&r.ID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueryLatestProposed 返回最新的 propose 事件，按 timestamp DESC 排序。
func (s *SQLite) QueryLatestProposed(limit int) ([]EventRow, error) {
	q := `SELECT id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price
	      FROM uma_oo_events
	      WHERE event_type = 'propose'
	      ORDER BY timestamp DESC, id DESC
	      LIMIT ?`
	rows, err := s.db.Query(q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		var conditionID, marketID, price sql.NullString
		if err := rows.Scan(&r.ID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// ScanEventsSince 加载 timestamp >= minTs 的事件，顺序与 MemReplica 桶内排序一致（启动加载最近 12h）。
func (s *SQLite) ScanEventsSince(minTs int64) ([]EventRow, error) {
	q := `SELECT id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price
	      FROM uma_oo_events
	      WHERE timestamp >= ?
	      ORDER BY event_type ASC, timestamp ASC, transaction_hash ASC, log_index ASC, id ASC`
	rows, err := s.db.Query(q, minTs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		var conditionID, marketID, price sql.NullString
		if err := rows.Scan(&r.ID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		out = append(out, r)
	}
	return out, rows.Err()
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
