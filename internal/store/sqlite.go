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
    question_id      TEXT,
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

CREATE TABLE IF NOT EXISTS uma_oo_resolved_pending (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    question_id      TEXT    NOT NULL,
    transaction_hash TEXT    NOT NULL,
    log_index        INTEGER NOT NULL,
    block_number     INTEGER,
    timestamp        INTEGER,
    price            TEXT,
    created_at       INTEGER NOT NULL DEFAULT 0,
    UNIQUE (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_pending_qid ON uma_oo_resolved_pending(question_id);
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
	QuestionID  string // init/resolved 取自 log.topics[1]；其它事件暂时为空
}

// PendingResolved 对应 uma_oo_resolved_pending 表的一行。
// 用于暂存写入时没找到对应 init 的 resolved 事件，等待 reconciler 关联或丢弃。
type PendingResolved struct {
	ID          int64
	QuestionID  string
	TxHash      string
	LogIndex    int
	BlockNumber uint64
	Timestamp   int64
	Price       string
	CreatedAt   int64
}

// InitNeedingQuestionID 用于 reconciler 回填 question_id 的 init 行最小字段。
type InitNeedingQuestionID struct {
	ID     int64
	TxHash string
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
	// 旧库迁移：question_id 列
	if err := migrateQuestionID(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate question_id: %w", err)
	}
	return &SQLite{db: db}, nil
}

// migrateQuestionID 旧库如果缺 question_id 列则补列 + 建索引。
// 数据回填由运行时的 reconciler 异步完成（调 RPC 取 init tx 的 topic[1]）。
func migrateQuestionID(db *sql.DB) error {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('uma_oo_events') WHERE name='question_id'`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err := db.Exec(`ALTER TABLE uma_oo_events ADD COLUMN question_id TEXT`); err != nil {
			return fmt.Errorf("alter table add question_id: %w", err)
		}
		log.Printf("[INFO] 迁移：已添加 question_id 列")
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_ev_question ON uma_oo_events(question_id)`); err != nil {
		return fmt.Errorf("create idx_ev_question: %w", err)
	}
	return nil
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
	conditionID, marketID, price, questionID string) (inserted bool, lastID int64, cursorID int64, err error) {

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
		 (cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price, question_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cid, eventType, txHash, logIndex, blockNumber, timestamp,
		nullStr(conditionID), nullStr(marketID), nullStr(price), nullStr(questionID),
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
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price, question_id
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
		var conditionID, marketID, price, questionID sql.NullString
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price, &questionID); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		r.QuestionID = questionID.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueryByLookup 按 condition_id 和/或 transaction_hash 精确查询事件，可选按 event_type 过滤。
// condition_id 与 transaction_hash 至少传一个；都传时为 AND 合取。按 cursor_id ASC 排序，limit<=0 时不限条数。
func (s *SQLite) QueryByLookup(conditionID, txHash, eventType string, limit int) ([]EventRow, error) {
	if conditionID == "" && txHash == "" {
		return nil, fmt.Errorf("condition_id 或 transaction_hash 至少传一个")
	}
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price, question_id
	      FROM uma_oo_events
	      WHERE 1=1`
	var args []interface{}
	if conditionID != "" {
		q += " AND condition_id = ?"
		args = append(args, conditionID)
	}
	if txHash != "" {
		q += " AND transaction_hash = ?"
		args = append(args, txHash)
	}
	if eventType != "" {
		q += " AND event_type = ?"
		args = append(args, eventType)
	}
	q += " ORDER BY cursor_id ASC"
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		var conditionID, marketID, price, questionID sql.NullString
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price, &questionID); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		r.QuestionID = questionID.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueryEventsByConditionIDs 按 condition_id IN (...) 批量拉取事件，返回按 cid 分组的 map。
// 空输入返回空 map；不存在的 cid 不会出现在返回中。SQLite 默认参数上限 32K，批量上层应限制到 ≤ 数百。
func (s *SQLite) QueryEventsByConditionIDs(ids []string) (map[string][]EventRow, error) {
	out := make(map[string][]EventRow)
	if len(ids) == 0 {
		return out, nil
	}
	placeholders := make([]byte, 0, len(ids)*2)
	args := make([]interface{}, 0, len(ids))
	for i, id := range ids {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id)
	}
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price, question_id
	      FROM uma_oo_events
	      WHERE condition_id IN (` + string(placeholders) + `)
	      ORDER BY cursor_id ASC`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var r EventRow
		var conditionID, marketID, price, questionID sql.NullString
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price, &questionID); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		r.QuestionID = questionID.String
		out[r.ConditionID] = append(out[r.ConditionID], r)
	}
	return out, rows.Err()
}

// QueryLatestProposed 返回最新的 propose 事件，按 cursor_id DESC 排序。
func (s *SQLite) QueryLatestProposed(limit int) ([]EventRow, error) {
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price, question_id
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
		var conditionID, marketID, price, questionID sql.NullString
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price, &questionID); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		r.QuestionID = questionID.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueryLatestDisputed 返回最新的 dispute 事件，按 cursor_id DESC 排序。
func (s *SQLite) QueryLatestDisputed(limit int) ([]EventRow, error) {
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price, question_id
	      FROM uma_oo_events
	      WHERE event_type = 'dispute'
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
		var conditionID, marketID, price, questionID sql.NullString
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price, &questionID); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		r.QuestionID = questionID.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// ScanEventsSince 加载 timestamp >= minTs 的事件，顺序与 MemReplica 桶内排序一致（启动加载最近 2h）。
func (s *SQLite) ScanEventsSince(minTs int64) ([]EventRow, error) {
	q := `SELECT id, cursor_id, event_type, transaction_hash, log_index, block_number, timestamp, condition_id, market_id, price, question_id
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
		var conditionID, marketID, price, questionID sql.NullString
		if err := rows.Scan(&r.ID, &r.CursorID, &r.EventType, &r.TxHash, &r.LogIndex,
			&r.BlockNumber, &r.Timestamp, &conditionID, &marketID, &price, &questionID); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		r.ConditionID = conditionID.String
		r.MarketID = marketID.String
		r.Price = price.String
		r.QuestionID = questionID.String
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

// ── question_id 关联 / pending 区（reconciler 使用）────────────────────────────

// GetConditionIDByQuestionID 返回与 questionID 关联的 init 行的 condition_id；找不到返回 ""。
// 用于 resolved 写入时定位业务 condition_id。
func (s *SQLite) GetConditionIDByQuestionID(questionID string) (string, error) {
	if questionID == "" {
		return "", nil
	}
	var cid sql.NullString
	err := s.db.QueryRow(
		`SELECT condition_id FROM uma_oo_events
		 WHERE event_type='init' AND question_id=? AND condition_id IS NOT NULL AND condition_id != ''
		 LIMIT 1`, questionID).Scan(&cid)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return cid.String, nil
}

// ListInitsWithoutQuestionID 返回尚未回填 question_id 的 init 行（只含 id + tx_hash）。
// 按 id DESC 排序：先处理最近的 init，让新市场的 resolved 关联尽快生效；
// 老数据（ID 小）排在队尾，最终会被消化。reconciler 定时任务通过 tx receipt 的 topic[1] 回填。
func (s *SQLite) ListInitsWithoutQuestionID(limit int) ([]InitNeedingQuestionID, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT id, transaction_hash FROM uma_oo_events
		 WHERE event_type='init' AND (question_id IS NULL OR question_id='')
		 ORDER BY id DESC
		 LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InitNeedingQuestionID
	for rows.Next() {
		var r InitNeedingQuestionID
		if err := rows.Scan(&r.ID, &r.TxHash); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateQuestionID 回填 uma_oo_events.question_id。
func (s *SQLite) UpdateQuestionID(id int64, questionID string) error {
	_, err := s.db.Exec(
		`UPDATE uma_oo_events SET question_id=? WHERE id=?`,
		nullStr(questionID), id)
	return err
}

// InsertResolvedPending 将无法与 init 关联的 resolved 事件暂存到 pending 表。
// 幂等：(transaction_hash, log_index) 唯一。
func (s *SQLite) InsertResolvedPending(questionID, txHash string, logIndex int,
	blockNumber uint64, timestamp int64, price string) (inserted bool, err error) {
	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO uma_oo_resolved_pending
		 (question_id, transaction_hash, log_index, block_number, timestamp, price, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		questionID, txHash, logIndex, blockNumber, timestamp, nullStr(price), time.Now().Unix())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ListPendingResolveds 返回 pending 表的一批条目（按 id 升序，优先处理更早的）。
func (s *SQLite) ListPendingResolveds(limit int) ([]PendingResolved, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.Query(
		`SELECT id, question_id, transaction_hash, log_index, block_number, timestamp, price, created_at
		 FROM uma_oo_resolved_pending
		 ORDER BY id ASC
		 LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingResolved
	for rows.Next() {
		var p PendingResolved
		var price sql.NullString
		if err := rows.Scan(&p.ID, &p.QuestionID, &p.TxHash, &p.LogIndex,
			&p.BlockNumber, &p.Timestamp, &price, &p.CreatedAt); err != nil {
			return nil, err
		}
		p.Price = price.String
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeletePending 删除 pending 中的一行（promote 成功后或超期丢弃时调用）。
func (s *SQLite) DeletePending(id int64) error {
	_, err := s.db.Exec(`DELETE FROM uma_oo_resolved_pending WHERE id=?`, id)
	return err
}

// LegacyResolvedCandidate 是需要修正的一条 legacy resolved 行（分批处理时用）。
// QuestionID 是 v0.10.0 之前错放在 condition_id 字段里的 questionID。
type LegacyResolvedCandidate struct {
	ID         int64
	QuestionID string
}

// ListLegacyResolveds 返回能立刻修正的 legacy resolved 行（主表里 condition_id 存的是 questionID、
// 且能在 init 行里找到对应 question_id）。按 id DESC 排序：新 market 优先。
// 仅返回有匹配 init 的行（EXISTS 过滤），避免 reconciler 每轮对没 init 的行重复扫。
func (s *SQLite) ListLegacyResolveds(limit int) ([]LegacyResolvedCandidate, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(`
		SELECT r.id, r.condition_id
		FROM uma_oo_events r
		WHERE r.event_type='resolved'
		  AND (r.question_id IS NULL OR r.question_id='')
		  AND EXISTS (
		        SELECT 1 FROM uma_oo_events i
		        WHERE i.event_type='init'
		          AND i.question_id = r.condition_id
		          AND i.condition_id IS NOT NULL AND i.condition_id != ''
		    )
		ORDER BY r.id DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LegacyResolvedCandidate
	for rows.Next() {
		var c LegacyResolvedCandidate
		if err := rows.Scan(&c.ID, &c.QuestionID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetInitInfoByQuestionID 返回对应 init 的 (conditionID, marketID)；找不到返回 ("","",nil)。
func (s *SQLite) GetInitInfoByQuestionID(questionID string) (conditionID, marketID string, err error) {
	if questionID == "" {
		return "", "", nil
	}
	var cid, mid sql.NullString
	err = s.db.QueryRow(
		`SELECT condition_id, market_id FROM uma_oo_events
		 WHERE event_type='init' AND question_id=?
		   AND condition_id IS NOT NULL AND condition_id != ''
		 LIMIT 1`, questionID).Scan(&cid, &mid)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return cid.String, mid.String, nil
}

// UpdateLegacyResolved 单行修正 legacy resolved：
//   - condition_id ← 真业务 CID
//   - question_id  ← 原 condition_id 里存的 questionID
//   - market_id    ← init.market_id（仅在 init 有值时覆盖，否则保留原值）
func (s *SQLite) UpdateLegacyResolved(id int64, conditionID, questionID, marketID string) error {
	if marketID == "" {
		_, err := s.db.Exec(
			`UPDATE uma_oo_events SET condition_id=?, question_id=? WHERE id=?`,
			conditionID, questionID, id)
		return err
	}
	_, err := s.db.Exec(
		`UPDATE uma_oo_events SET condition_id=?, question_id=?, market_id=? WHERE id=?`,
		conditionID, questionID, nullStr(marketID), id)
	return err
}

// PromotePending 将 pending 表的一行关联到真 condition_id 并写入主事件表，成功后删除 pending。
// 返回 inserted=true 表示主表实际新增了一行；false 表示主表已存在同 tx+log_index（幂等）。
// 无事务：SQLite 单连接已天然串行；主表 INSERT OR IGNORE + pending DELETE 先后顺序不会丢数据。
func (s *SQLite) PromotePending(p PendingResolved, conditionID string) (inserted bool, err error) {
	inserted, _, _, err = s.InsertEvent("resolved", p.TxHash, p.LogIndex,
		p.BlockNumber, p.Timestamp, conditionID, "", p.Price, p.QuestionID)
	if err != nil {
		return false, err
	}
	if err := s.DeletePending(p.ID); err != nil {
		return inserted, err
	}
	return inserted, nil
}
