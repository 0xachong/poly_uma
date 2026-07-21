package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const marketSchema = `
CREATE TABLE IF NOT EXISTS market_condition_map (
    market_id    TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    updated_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS market_sync_state (
    task_name     TEXT PRIMARY KEY,
    next_cursor   TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'pending',
    scanned_count INTEGER NOT NULL DEFAULT 0,
    started_at    INTEGER NOT NULL DEFAULT 0,
    completed_at  INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT NOT NULL DEFAULT ''
);
`

const maintenanceSchema = `
CREATE TABLE IF NOT EXISTS question_condition_map (
    question_id  TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    market_id    TEXT,
    init_tx_hash TEXT,
    updated_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS resolved_pending (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    question_id      TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index        INTEGER NOT NULL,
    block_number     INTEGER,
    timestamp        INTEGER,
    price            TEXT,
    created_at       INTEGER NOT NULL DEFAULT 0,
    UNIQUE (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_maintenance_pending_qid ON resolved_pending(question_id);
CREATE TABLE IF NOT EXISTS migration_state (
    task_name  TEXT PRIMARY KEY,
    last_id    INTEGER NOT NULL DEFAULT 0,
    status     TEXT NOT NULL DEFAULT 'pending',
    updated_at INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS reconciliation_state (
    task_name    TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending',
    last_run_at  INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT NOT NULL DEFAULT ''
);
`

type MarketSQLite struct{ db *sql.DB }
type MaintenanceSQLite struct{ db *sql.DB }

type MarketMappingRecord struct {
	RowID       int64
	MarketID    string
	ConditionID string
}

type QuestionMappingRecord struct {
	ID          int64
	QuestionID  string
	ConditionID string
	MarketID    string
	TxHash      string
}

func openAuxiliary(path, schema, name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_journal=WAL&_timeout=5000", path))
	if err != nil {
		return nil, fmt.Errorf("open %s sqlite: %w", name, err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init %s sqlite: %w", name, err)
	}
	return db, nil
}

func OpenMarket(path string) (*MarketSQLite, error) {
	db, err := openAuxiliary(path, marketSchema, "market")
	if err != nil {
		return nil, err
	}
	return &MarketSQLite{db: db}, nil
}

func (s *MarketSQLite) Close() error { return s.db.Close() }

func (s *MarketSQLite) UpsertMarketCondition(marketID, conditionID string) (inserted, conflict bool, err error) {
	if marketID == "" || conditionID == "" {
		return false, false, fmt.Errorf("empty market mapping")
	}
	res, err := s.db.Exec(`INSERT OR IGNORE INTO market_condition_map(market_id,condition_id,updated_at) VALUES(?,?,?)`,
		marketID, conditionID, time.Now().Unix())
	if err != nil {
		return false, false, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return true, false, nil
	}
	var existing string
	if err := s.db.QueryRow(`SELECT condition_id FROM market_condition_map WHERE market_id=?`, marketID).Scan(&existing); err != nil {
		return false, false, err
	}
	return false, existing != conditionID, nil
}

func (s *MarketSQLite) MappingCount() (int64, error) {
	var count int64
	err := s.db.QueryRow(`SELECT count(*) FROM market_condition_map`).Scan(&count)
	return count, err
}

func (s *MarketSQLite) GetMarketConditionID(marketID string) (string, error) {
	var conditionID string
	err := s.db.QueryRow(`SELECT condition_id FROM market_condition_map WHERE market_id=?`, marketID).Scan(&conditionID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return conditionID, err
}

func (s *MarketSQLite) LoadMarketConditionMap() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT market_id,condition_id FROM market_condition_map`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var marketID, conditionID string
		if err := rows.Scan(&marketID, &conditionID); err != nil {
			return nil, err
		}
		out[marketID] = conditionID
	}
	return out, rows.Err()
}

func (s *MarketSQLite) UpsertMarketBatch(records []MarketMappingRecord) error {
	if len(records) == 0 {
		return nil
	}
	values := make([]string, 0, len(records))
	args := make([]interface{}, 0, len(records)*3)
	now := time.Now().Unix()
	for _, record := range records {
		if record.MarketID == "" || record.ConditionID == "" {
			continue
		}
		values = append(values, "(?,?,?)")
		args = append(args, record.MarketID, record.ConditionID, now)
	}
	if len(values) == 0 {
		return nil
	}
	_, err := s.db.Exec(`INSERT OR IGNORE INTO market_condition_map(market_id,condition_id,updated_at) VALUES `+
		strings.Join(values, ","), args...)
	return err
}

func OpenMaintenance(path string) (*MaintenanceSQLite, error) {
	db, err := openAuxiliary(path, maintenanceSchema, "maintenance")
	if err != nil {
		return nil, err
	}
	return &MaintenanceSQLite{db: db}, nil
}

func (s *MaintenanceSQLite) Close() error { return s.db.Close() }

// UpsertQuestionMapping mirrors newly observed init relationships. Empty conditionID is
// allowed temporarily and can later be completed when the market resolver succeeds.
func (s *MaintenanceSQLite) UpsertQuestionMapping(questionID, conditionID, marketID, txHash string) (conflict bool, err error) {
	if questionID == "" {
		return false, fmt.Errorf("empty question id")
	}
	_, err = s.db.Exec(`INSERT OR IGNORE INTO question_condition_map(question_id,condition_id,market_id,init_tx_hash,updated_at)
		VALUES(?,?,?,?,?)`, questionID, conditionID, nullStr(marketID), nullStr(txHash), time.Now().Unix())
	if err != nil {
		return false, err
	}
	var existingCondition string
	var existingMarket sql.NullString
	if err := s.db.QueryRow(`SELECT condition_id,market_id FROM question_condition_map WHERE question_id=?`, questionID).
		Scan(&existingCondition, &existingMarket); err != nil {
		return false, err
	}
	if existingMarket.Valid && existingMarket.String != "" && marketID != "" && existingMarket.String != marketID {
		return true, nil
	}
	if existingCondition != "" && conditionID != "" && existingCondition != conditionID {
		return true, nil
	}
	if existingCondition == "" && conditionID != "" {
		_, err = s.db.Exec(`UPDATE question_condition_map SET condition_id=?,market_id=COALESCE(market_id,?),updated_at=?
			WHERE question_id=? AND condition_id=''`, conditionID, nullStr(marketID), time.Now().Unix(), questionID)
	}
	return false, err
}

func (s *MaintenanceSQLite) FillConditionByMarketID(marketID, conditionID string) error {
	if marketID == "" || conditionID == "" {
		return nil
	}
	_, err := s.db.Exec(`UPDATE question_condition_map SET condition_id=?,updated_at=?
		WHERE market_id=? AND condition_id=''`, conditionID, time.Now().Unix(), marketID)
	return err
}

func (s *MaintenanceSQLite) QuestionMappingCount() (int64, error) {
	var count int64
	err := s.db.QueryRow(`SELECT count(*) FROM question_condition_map`).Scan(&count)
	return count, err
}

func (s *MaintenanceSQLite) GetQuestionConditionID(questionID string) (string, error) {
	var conditionID string
	err := s.db.QueryRow(`SELECT condition_id FROM question_condition_map WHERE question_id=? AND condition_id!=''`, questionID).
		Scan(&conditionID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return conditionID, err
}

type MigrationState struct {
	LastID int64
	Status string
}

func (s *MaintenanceSQLite) GetMigrationState(task string) (MigrationState, error) {
	var state MigrationState
	err := s.db.QueryRow(`SELECT last_id,status FROM migration_state WHERE task_name=?`, task).
		Scan(&state.LastID, &state.Status)
	if err == sql.ErrNoRows {
		return state, nil
	}
	return state, err
}

func (s *MaintenanceSQLite) SaveMigrationState(task string, lastID int64, status, lastError string) error {
	_, err := s.db.Exec(`INSERT INTO migration_state(task_name,last_id,status,updated_at,last_error) VALUES(?,?,?,?,?)
		ON CONFLICT(task_name) DO UPDATE SET last_id=excluded.last_id,status=excluded.status,
		updated_at=excluded.updated_at,last_error=excluded.last_error`, task, lastID, status, time.Now().Unix(), lastError)
	return err
}

func (s *MaintenanceSQLite) UpsertQuestionBatch(records []QuestionMappingRecord) error {
	if len(records) == 0 {
		return nil
	}
	values := make([]string, 0, len(records))
	args := make([]interface{}, 0, len(records)*5)
	now := time.Now().Unix()
	for _, record := range records {
		if record.QuestionID == "" {
			continue
		}
		values = append(values, "(?,?,?,?,?)")
		args = append(args, record.QuestionID, record.ConditionID, nullStr(record.MarketID), nullStr(record.TxHash), now)
	}
	if len(values) == 0 {
		return nil
	}
	_, err := s.db.Exec(`INSERT OR IGNORE INTO question_condition_map(question_id,condition_id,market_id,init_tx_hash,updated_at) VALUES `+
		strings.Join(values, ","), args...)
	return err
}
