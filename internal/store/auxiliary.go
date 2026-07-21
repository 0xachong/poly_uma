package store

import (
	"database/sql"
	"fmt"

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

func OpenMaintenance(path string) (*MaintenanceSQLite, error) {
	db, err := openAuxiliary(path, maintenanceSchema, "maintenance")
	if err != nil {
		return nil, err
	}
	return &MaintenanceSQLite{db: db}, nil
}

func (s *MaintenanceSQLite) Close() error { return s.db.Close() }
