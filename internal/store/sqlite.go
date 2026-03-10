// Package store 提供本地 SQLite 存储：事件去重流水（uma_oo_events）+ 问题状态（uma_oo_flow）+ 断点（syncer_checkpoint）。
// uma_oo_flow 作为 HTTP API 的直接查询源，保证快速响应。
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS uma_oo_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type   TEXT    NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_ts     INTEGER,
    question_id  TEXT,
    market_id    TEXT,
    price        TEXT,
    UNIQUE (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_ev_type     ON uma_oo_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ev_block_ts ON uma_oo_events(block_ts);
CREATE INDEX IF NOT EXISTS idx_ev_market   ON uma_oo_events(market_id);

CREATE TABLE IF NOT EXISTS uma_oo_flow (
    question_id            TEXT PRIMARY KEY,
    market_id              TEXT,
    condition_id           TEXT,
    title                  TEXT,
    phase                  TEXT NOT NULL DEFAULT 'initialized',

    propose_tx_hash        TEXT,
    propose_block_number   INTEGER,
    propose_proposer       TEXT,
    propose_proposed_price TEXT,
    propose_timestamp      INTEGER,
    propose_expiration_ts  INTEGER,
    propose_block_ts       INTEGER,

    dispute_tx_hash        TEXT,
    dispute_block_number   INTEGER,
    dispute_disputer       TEXT,
    dispute_proposed_price TEXT,
    dispute_block_ts       INTEGER,

    resolved_tx_hash       TEXT,
    resolved_block_number  INTEGER,
    resolved_settled_price TEXT,
    resolved_block_ts      INTEGER,
    resolved_payouts       TEXT,

    settle_tx_hash         TEXT,
    settle_block_number    INTEGER,
    settle_price           TEXT,
    settle_block_ts        INTEGER,

    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_flow_phase       ON uma_oo_flow(phase);
CREATE INDEX IF NOT EXISTS idx_flow_market      ON uma_oo_flow(market_id);
CREATE INDEX IF NOT EXISTS idx_flow_resolved_ts ON uma_oo_flow(resolved_block_ts);
CREATE INDEX IF NOT EXISTS idx_flow_dispute_ts  ON uma_oo_flow(dispute_block_ts);

CREATE TABLE IF NOT EXISTS syncer_checkpoint (
    id         INTEGER PRIMARY KEY CHECK(id = 1),
    last_block INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO syncer_checkpoint(id, last_block, updated_at) VALUES (1, 0, 0);
`

// FlowRow 对应 uma_oo_flow 表的一行，用于 API 响应。
type FlowRow struct {
	QuestionID          string
	MarketID            string
	ConditionID         string
	Title               string
	Phase               string
	ProposeProposer     string
	ProposeProposedPrice string
	ProposeTimestamp    int64
	ProposeExpirationTs int64
	ProposeBlockTs      int64
	DisputeDisputer     string
	DisputeProposedPrice string
	DisputeBlockTs      int64
	DisputeTxHash       string
	ResolvedSettledPrice string
	ResolvedBlockTs     int64
	ResolvedTxHash      string
	ResolvedPayouts     []string
	SettlePrice         string
	SettleBlockTs       int64
}

// SQLite 是本地 SQLite 存储。
type SQLite struct {
	db             *sql.DB
	latestSeenBlock atomic.Uint64 // 供 healthz 展示，syncer 更新
}

// Open 打开（或创建）SQLite 数据库文件并初始化 schema。
func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_journal=WAL&_timeout=5000", path))
	if err != nil {
		return nil, fmt.Errorf("sql.Open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite 写并发限制为 1
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

// ── 事件流水（去重）──────────────────────────────────────────────────────────

// InsertEvent 幂等写入一条事件流水。返回 true 表示新行（首次写入）。
func (s *SQLite) InsertEvent(eventType, txHash string, logIndex int,
	blockNumber uint64, blockTs int64,
	questionID, marketID, price string) (bool, error) {

	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO uma_oo_events
		 (event_type,tx_hash,log_index,block_number,block_ts,question_id,market_id,price)
		 VALUES (?,?,?,?,?,?,?,?)`,
		eventType, txHash, logIndex, blockNumber, blockTs,
		nullStr(questionID), nullStr(marketID), nullStr(price),
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ── flow 状态表 ──────────────────────────────────────────────────────────────

// UpsertInit 按 question_id 插入或更新（QuestionInitialized）。
func (s *SQLite) UpsertInit(questionID, marketID, conditionID, title string, blockTs int64) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(`
		INSERT INTO uma_oo_flow (question_id,market_id,condition_id,title,phase,created_at,updated_at)
		VALUES (?,'initialized',?,?)
		ON CONFLICT(question_id) DO UPDATE SET
		    market_id    = COALESCE(NULLIF(excluded.market_id,''),    market_id),
		    condition_id = COALESCE(NULLIF(excluded.condition_id,''), condition_id),
		    title        = COALESCE(NULLIF(excluded.title,''),        title),
		    updated_at   = excluded.updated_at`,
		questionID, marketID, conditionID, title, now, now,
	)
	return err
}

// UpdatePropose 按 market_id 更新提议阶段（ProposePrice）。
func (s *SQLite) UpdatePropose(marketID, txHash string, blockNumber uint64,
	proposer, proposedPrice string, timestamp, expirationTs, blockTs int64,
	conditionID string) error {

	qid, err := s.ensureFlowByMarket(marketID)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	_, err = s.db.Exec(`
		UPDATE uma_oo_flow SET
		    propose_tx_hash=?, propose_block_number=?, propose_proposer=?,
		    propose_proposed_price=?, propose_timestamp=?, propose_expiration_ts=?,
		    propose_block_ts=?,
		    condition_id = COALESCE(NULLIF(?,''), condition_id),
		    phase = CASE WHEN phase IN ('initialized','requested') THEN 'proposed' ELSE phase END,
		    updated_at=?
		WHERE question_id=?`,
		txHash, blockNumber, proposer, proposedPrice, timestamp, expirationTs,
		blockTs, conditionID, now, qid,
	)
	return err
}

// UpdateDispute 按 market_id 更新争议阶段（DisputePrice）。
func (s *SQLite) UpdateDispute(marketID, txHash string, blockNumber uint64,
	disputer, proposedPrice string, blockTs int64) error {

	qid, err := s.ensureFlowByMarket(marketID)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	_, err = s.db.Exec(`
		UPDATE uma_oo_flow SET
		    dispute_tx_hash=?, dispute_block_number=?, dispute_disputer=?,
		    dispute_proposed_price=?, dispute_block_ts=?,
		    phase = CASE WHEN phase IN ('initialized','requested','proposed') THEN 'disputed' ELSE phase END,
		    updated_at=?
		WHERE question_id=?`,
		txHash, blockNumber, disputer, proposedPrice, blockTs, now, qid,
	)
	return err
}

// UpsertResolved 按 question_id 插入或更新结算阶段（QuestionResolved）。
func (s *SQLite) UpsertResolved(questionID, txHash string, blockNumber uint64,
	settledPrice string, payouts []string, blockTs int64) error {

	payoutsJSON, _ := json.Marshal(payouts)
	now := time.Now().Unix()
	_, err := s.db.Exec(`
		INSERT INTO uma_oo_flow
		    (question_id,phase,resolved_tx_hash,resolved_block_number,
		     resolved_settled_price,resolved_block_ts,resolved_payouts,created_at,updated_at)
		VALUES (?,'resolved',?,?,?,?,?,?,?)
		ON CONFLICT(question_id) DO UPDATE SET
		    resolved_tx_hash=excluded.resolved_tx_hash,
		    resolved_block_number=excluded.resolved_block_number,
		    resolved_settled_price=excluded.resolved_settled_price,
		    resolved_block_ts=excluded.resolved_block_ts,
		    resolved_payouts=excluded.resolved_payouts,
		    phase='resolved', updated_at=excluded.updated_at`,
		questionID, txHash, blockNumber, settledPrice, blockTs, string(payoutsJSON), now, now,
	)
	return err
}

// UpdateSettle 按 market_id 更新最终结算阶段（Settle）。
func (s *SQLite) UpdateSettle(marketID, txHash string, blockNumber uint64,
	price string, blockTs int64) error {

	if marketID == "" {
		return nil
	}
	now := time.Now().Unix()
	_, err := s.db.Exec(`
		UPDATE uma_oo_flow SET
		    settle_tx_hash=?, settle_block_number=?, settle_price=?, settle_block_ts=?,
		    phase='settled', updated_at=?
		WHERE market_id=? AND phase NOT IN ('settled')`,
		txHash, blockNumber, price, blockTs, now, marketID,
	)
	return err
}

// ensureFlowByMarket 找到或创建 market_id 对应的 flow 行，返回 question_id。
func (s *SQLite) ensureFlowByMarket(marketID string) (string, error) {
	var qid string
	err := s.db.QueryRow(
		"SELECT question_id FROM uma_oo_flow WHERE market_id=? ORDER BY updated_at DESC LIMIT 1",
		marketID,
	).Scan(&qid)
	if err == nil {
		return qid, nil
	}
	// Init 尚未到达，用占位 question_id
	placeholder := "__mkt__" + marketID
	now := time.Now().Unix()
	_, err = s.db.Exec(
		`INSERT OR IGNORE INTO uma_oo_flow (question_id,market_id,phase,created_at,updated_at)
		 VALUES (?,'initialized',?,?)`,
		placeholder, marketID, now, now,
	)
	return placeholder, err
}

// ── API 查询 ─────────────────────────────────────────────────────────────────

// QuerySettled 查询已结算事件（API /uma/v1/settled）。
func (s *SQLite) QuerySettled(fromTs, toTs int64, limit int, cursor string) ([]FlowRow, error) {
	args := []interface{}{fromTs, toTs}
	q := `SELECT * FROM uma_oo_flow
	      WHERE phase IN ('resolved','settled')
	        AND resolved_block_ts >= ? AND resolved_block_ts <= ?`
	if cursor != "" {
		q += " AND question_id > ?"
		args = append(args, cursor)
	}
	q += " ORDER BY resolved_block_ts ASC, question_id ASC LIMIT ?"
	args = append(args, limit)
	return s.queryFlow(q, args...)
}

// QueryDisputed 查询争议中事件（API /uma/v1/disputed）。
func (s *SQLite) QueryDisputed(fromTs, toTs int64, limit int, cursor string) ([]FlowRow, error) {
	args := []interface{}{fromTs, toTs}
	q := `SELECT * FROM uma_oo_flow
	      WHERE phase = 'disputed'
	        AND dispute_block_ts >= ? AND dispute_block_ts <= ?`
	if cursor != "" {
		q += " AND question_id > ?"
		args = append(args, cursor)
	}
	q += " ORDER BY dispute_block_ts ASC, question_id ASC LIMIT ?"
	args = append(args, limit)
	return s.queryFlow(q, args...)
}

func (s *SQLite) queryFlow(query string, args ...interface{}) ([]FlowRow, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FlowRow
	for rows.Next() {
		var r FlowRow
		var (
			marketID, conditionID, title                                sql.NullString
			proposeProposer, proposeProposedPrice                       sql.NullString
			proposeTimestamp, proposeExpirationTs, proposeBlockTs       sql.NullInt64
			disputeDisputer, disputeProposedPrice, disputeTxHash        sql.NullString
			disputeBlockTs                                              sql.NullInt64
			resolvedSettledPrice, resolvedTxHash, resolvedPayoutsJSON   sql.NullString
			resolvedBlockTs                                             sql.NullInt64
			settlePrice                                                 sql.NullString
			settleBlockTs                                               sql.NullInt64
			// unused columns (store only)
			_, _, _, _, _, _, _ sql.NullString
			_, _                sql.NullInt64
		)
		err := rows.Scan(
			&r.QuestionID, &marketID, &conditionID, &title, &r.Phase,
			// propose
			&_, &_, &proposeProposer, &proposeProposedPrice,
			&proposeTimestamp, &proposeExpirationTs, &proposeBlockTs,
			// dispute
			&disputeTxHash, &_, &disputeDisputer, &disputeProposedPrice, &disputeBlockTs,
			// resolved
			&resolvedTxHash, &_, &resolvedSettledPrice, &resolvedBlockTs, &resolvedPayoutsJSON,
			// settle
			&_, &_, &settlePrice, &settleBlockTs,
			// timestamps
			&_, &_,
		)
		if err != nil {
			return nil, fmt.Errorf("scan flow row: %w", err)
		}
		r.MarketID = marketID.String
		r.ConditionID = conditionID.String
		r.Title = title.String
		r.ProposeProposer = proposeProposer.String
		r.ProposeProposedPrice = proposeProposedPrice.String
		r.ProposeTimestamp = proposeTimestamp.Int64
		r.ProposeExpirationTs = proposeExpirationTs.Int64
		r.ProposeBlockTs = proposeBlockTs.Int64
		r.DisputeDisputer = disputeDisputer.String
		r.DisputeProposedPrice = disputeProposedPrice.String
		r.DisputeBlockTs = disputeBlockTs.Int64
		r.DisputeTxHash = disputeTxHash.String
		r.ResolvedSettledPrice = resolvedSettledPrice.String
		r.ResolvedBlockTs = resolvedBlockTs.Int64
		r.ResolvedTxHash = resolvedTxHash.String
		if resolvedPayoutsJSON.String != "" {
			_ = json.Unmarshal([]byte(resolvedPayoutsJSON.String), &r.ResolvedPayouts)
		}
		r.SettlePrice = settlePrice.String
		r.SettleBlockTs = settleBlockTs.Int64
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
