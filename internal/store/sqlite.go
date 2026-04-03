// Package store 提供本地 SQLite 存储：事件去重流水（uma_oo_events）+ 断点（syncer_checkpoint）。
// 表结构与 Python 版 uma_subscribe_mysql.py 保持一致，可共用同一数据库文件。
package store

import (
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS uma_oo_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    cursor_id        INTEGER NOT NULL DEFAULT 0,
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
CREATE INDEX IF NOT EXISTS idx_ev_type_cursor ON uma_oo_events(event_type, cursor_id);

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
	CursorID    int64 // timestamp*1000 + 秒内序号(0-999)，用于游标分页
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
// 旧库自动迁移：检测 cursor_id 列是否存在，不存在则 ALTER + backfill + 建索引。
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
	// 旧库迁移：cursor_id 列
	if err := migrateCursorID(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate cursor_id: %w", err)
	}
	return &SQLite{db: db}, nil
}

// migrateCursorID 检测旧库并补充 cursor_id 列和回填数据。
func migrateCursorID(db *sql.DB) error {
	// 检查 cursor_id 列是否存在
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('uma_oo_events') WHERE name='cursor_id'`).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		// 列已存在，检查是否有未回填的行（cursor_id=0 且 timestamp>0）
		var unfilled int
		_ = db.QueryRow(`SELECT COUNT(*) FROM uma_oo_events WHERE cursor_id=0 AND timestamp>0`).Scan(&unfilled)
		if unfilled == 0 {
			return nil // 已完成迁移
		}
	} else {
		// 加列
		if _, err := db.Exec(`ALTER TABLE uma_oo_events ADD COLUMN cursor_id INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("alter table: %w", err)
		}
		log.Printf("[INFO] 迁移：已添加 cursor_id 列")
	}
	// 回填
	res, err := db.Exec(`UPDATE uma_oo_events SET cursor_id = timestamp * 1000 +
		(SELECT COUNT(*) FROM uma_oo_events AS b
		 WHERE b.timestamp = uma_oo_events.timestamp
		 AND (b.transaction_hash < uma_oo_events.transaction_hash
		      OR (b.transaction_hash = uma_oo_events.transaction_hash
		          AND b.log_index < uma_oo_events.log_index)))
		WHERE cursor_id = 0 AND timestamp > 0`)
	if err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	n, _ := res.RowsAffected()
	log.Printf("[INFO] 迁移：已回填 %d 行 cursor_id", n)
	// 确保索引存在
	_, _ = db.Exec(`CREATE INDEX IF NOT EXISTS idx_ev_type_cursor ON uma_oo_events(event_type, cursor_id)`)
	return nil
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

// InsertEvent 幂等写入一条事件。返回 true 表示新行（首次写入），自增 id 和 cursor_id。
func (s *SQLite) InsertEvent(eventType, txHash string, logIndex int,
	blockNumber uint64, timestamp int64,
	conditionID, marketID, price string) (inserted bool, lastID int64, cursorID int64, err error) {

	// 计算 cursor_id = timestamp*1000 + 秒内序号
	var seq int64
	_ = s.db.QueryRow(
		`SELECT COUNT(*) FROM uma_oo_events
		 WHERE timestamp = ? AND (transaction_hash < ? OR (transaction_hash = ? AND log_index < ?))`,
		timestamp, txHash, txHash, logIndex,
	).Scan(&seq)
	cid := timestamp*1000 + seq

	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO uma_oo_events
		 (cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cid, eventType, txHash, logIndex, blockNumber, timestamp,
		nullStr(conditionID), nullStr(marketID), nullStr(price),
	)
	if err != nil {
		return false, 0, 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, 0, 0, nil
	}
	id, _ := res.LastInsertId()
	return true, id, cid, nil
}

// ── API 查询 ──────────────────────────────────────────────────────────────────

// QueryByType 按事件类型查询，支持 cursor_id 游标分页。
// cursor 为上一页最后一条记录的 cursor_id；fromTs/toTs 可选（0 表示不限）。
func (s *SQLite) QueryByType(eventType string, fromTs, toTs int64, limit int, cursor int64) ([]EventRow, error) {
	args := []interface{}{eventType}
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price
	      FROM uma_oo_events
	      WHERE event_type = ?`
	if cursor > 0 {
		q += " AND cursor_id > ?"
		args = append(args, cursor)
	} else if fromTs > 0 {
		q += " AND cursor_id >= ?"
		args = append(args, fromTs*1000)
	}
	if toTs > 0 {
		q += " AND cursor_id <= ?"
		args = append(args, toTs*1000+999)
	}
	q += " ORDER BY cursor_id ASC LIMIT ?"
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
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
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

// QueryLatestProposed 返回最新的 propose 事件，按 cursor_id DESC 排序。
func (s *SQLite) QueryLatestProposed(limit int) ([]EventRow, error) {
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price
	      FROM uma_oo_events
	      WHERE event_type = 'propose'
	      ORDER BY cursor_id DESC
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
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
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
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price
	      FROM uma_oo_events
	      WHERE cursor_id >= ?
	      ORDER BY event_type ASC, cursor_id ASC`
	rows, err := s.db.Query(q, minTs*1000)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		var conditionID, marketID, price sql.NullString
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
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
