package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const marketSchema = `
CREATE TABLE IF NOT EXISTS market_condition_map (
    market_id    TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    updated_at   INTEGER NOT NULL,
    active       INTEGER NOT NULL DEFAULT 0,
    closed       INTEGER NOT NULL DEFAULT 0,
    closed_at    INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0
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
CREATE TABLE IF NOT EXISTS active_market_snapshot (
    market_id       TEXT PRIMARY KEY,
    condition_id    TEXT NOT NULL,
    snapshot_json   TEXT NOT NULL,
    active          INTEGER NOT NULL DEFAULT 1,
    closed          INTEGER NOT NULL DEFAULT 0,
    gamma_updated_at_ms INTEGER NOT NULL DEFAULT 0,
    synced_at_us    INTEGER NOT NULL DEFAULT 0,
    clob_last_seen_at INTEGER NOT NULL DEFAULT 0,
    uma_pinned_until INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_active_snapshot_condition
    ON active_market_snapshot(condition_id);
CREATE INDEX IF NOT EXISTS idx_active_snapshot_condition_nocase
    ON active_market_snapshot(condition_id COLLATE NOCASE);
CREATE TABLE IF NOT EXISTS market_enrichment_incident (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at_ms INTEGER NOT NULL,
    stage         TEXT NOT NULL,
    market_id     TEXT NOT NULL,
    condition_id  TEXT NOT NULL DEFAULT '',
    event_type    TEXT NOT NULL DEFAULT '',
    tx_hash       TEXT NOT NULL DEFAULT '',
    log_index     INTEGER NOT NULL DEFAULT 0,
    block_number  INTEGER NOT NULL DEFAULT 0,
    elapsed_ms    INTEGER NOT NULL DEFAULT 0,
    detail_json   TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_market_incident_market_time
    ON market_enrichment_incident(market_id, observed_at_ms DESC);
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

type MarketCatalogRecord struct {
	MarketID    string
	ConditionID string
	Active      bool
	Closed      bool
	ClosedAt    int64
}

type ActiveMarketSnapshotRecord struct {
	MarketID       string
	ConditionID    string
	SnapshotJSON   string
	Active         bool
	Closed         bool
	GammaUpdatedMS int64
	SyncedAtUS     int64
	CLOBLastSeenAt int64
	UMAPinnedUntil int64
}

type MarketEnrichmentIncident struct {
	ObservedAtMS int64
	Stage        string
	MarketID     string
	ConditionID  string
	EventType    string
	TxHash       string
	LogIndex     int
	BlockNumber  uint64
	ElapsedMS    int64
	DetailJSON   string
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
	for _, migration := range []string{
		`ALTER TABLE market_condition_map ADD COLUMN active INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE market_condition_map ADD COLUMN closed INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE market_condition_map ADD COLUMN closed_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE market_condition_map ADD COLUMN last_seen_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE active_market_snapshot ADD COLUMN clob_last_seen_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE active_market_snapshot ADD COLUMN uma_pinned_until INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(migration); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			db.Close()
			return nil, fmt.Errorf("migrate market sqlite: %w", err)
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_market_hot
		ON market_condition_map(last_seen_at DESC) WHERE active=1 AND closed=0`); err != nil {
		db.Close()
		return nil, fmt.Errorf("index market sqlite: %w", err)
	}
	return &MarketSQLite{db: db}, nil
}

func (s *MarketSQLite) Close() error { return s.db.Close() }

func (s *MarketSQLite) UpsertActiveMarketSnapshot(record ActiveMarketSnapshotRecord) error {
	if record.MarketID == "" || record.ConditionID == "" || record.SnapshotJSON == "" {
		return fmt.Errorf("incomplete active market snapshot")
	}
	_, err := s.db.Exec(`INSERT INTO active_market_snapshot
		(market_id,condition_id,snapshot_json,active,closed,gamma_updated_at_ms,synced_at_us,clob_last_seen_at,uma_pinned_until)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(market_id) DO UPDATE SET
		condition_id=excluded.condition_id,snapshot_json=excluded.snapshot_json,
		active=excluded.active,closed=excluded.closed,
		gamma_updated_at_ms=excluded.gamma_updated_at_ms,synced_at_us=excluded.synced_at_us,
		clob_last_seen_at=CASE WHEN excluded.clob_last_seen_at>0 THEN excluded.clob_last_seen_at ELSE active_market_snapshot.clob_last_seen_at END,
		uma_pinned_until=MAX(active_market_snapshot.uma_pinned_until,excluded.uma_pinned_until)`,
		record.MarketID, record.ConditionID, record.SnapshotJSON, boolInt(record.Active), boolInt(record.Closed),
		record.GammaUpdatedMS, record.SyncedAtUS, record.CLOBLastSeenAt, record.UMAPinnedUntil)
	return err
}

// DeactivateActiveMarketSnapshot removes cold routing attributes. The durable
// market_id <-> condition_id identity lives in market_condition_map.
func (s *MarketSQLite) DeactivateActiveMarketSnapshot(conditionID string) error {
	if conditionID == "" {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM active_market_snapshot WHERE condition_id=? COLLATE NOCASE`, conditionID)
	return err
}

func (s *MarketSQLite) LoadActiveMarketSnapshots() ([]ActiveMarketSnapshotRecord, error) {
	rows, err := s.db.Query(`SELECT market_id,condition_id,snapshot_json,active,closed,gamma_updated_at_ms,synced_at_us,clob_last_seen_at,uma_pinned_until
		FROM active_market_snapshot WHERE active=1 AND closed=0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActiveMarketSnapshotRecord
	for rows.Next() {
		var r ActiveMarketSnapshotRecord
		var active, closed int
		if err := rows.Scan(&r.MarketID, &r.ConditionID, &r.SnapshotJSON, &active, &closed,
			&r.GammaUpdatedMS, &r.SyncedAtUS, &r.CLOBLastSeenAt, &r.UMAPinnedUntil); err != nil {
			return nil, err
		}
		r.Active, r.Closed = active != 0, closed != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// ApplyCLOBResidentSet refreshes current CLOB members. It deliberately does
// not evict: only a successfully completed full Gamma baseline may prune the
// last-known-good snapshot backup.
func (s *MarketSQLite) ApplyCLOBResidentSet(conditionIDs []string, seenAt int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`UPDATE active_market_snapshot SET active=1,clob_last_seen_at=? WHERE condition_id=? COLLATE NOCASE AND closed=0`)
	if err != nil {
		return err
	}
	for _, conditionID := range conditionIDs {
		if _, err := stmt.Exec(seenAt, conditionID); err != nil {
			stmt.Close()
			return err
		}
	}
	if err := stmt.Close(); err != nil {
		return err
	}
	return tx.Commit()
}

// PruneExpiredActiveMarketSnapshots runs only after a complete authoritative
// baseline. market_condition_map identities are intentionally never deleted.
func (s *MarketSQLite) PruneExpiredActiveMarketSnapshots(cutoff, now int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM active_market_snapshot WHERE clob_last_seen_at<? AND uma_pinned_until<?`, cutoff, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *MarketSQLite) AppendMarketEnrichmentIncident(v MarketEnrichmentIncident) error {
	if v.ObservedAtMS == 0 {
		v.ObservedAtMS = time.Now().UnixMilli()
	}
	if v.DetailJSON == "" {
		v.DetailJSON = "{}"
	}
	_, err := s.db.Exec(`INSERT INTO market_enrichment_incident
		(observed_at_ms,stage,market_id,condition_id,event_type,tx_hash,log_index,block_number,elapsed_ms,detail_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, v.ObservedAtMS, v.Stage, v.MarketID, v.ConditionID, v.EventType,
		v.TxHash, v.LogIndex, v.BlockNumber, v.ElapsedMS, v.DetailJSON)
	return err
}

func (s *MarketSQLite) ListMarketEnrichmentIncidents(marketID string, limit int) ([]MarketEnrichmentIncident, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT observed_at_ms,stage,market_id,condition_id,event_type,tx_hash,log_index,block_number,elapsed_ms,detail_json
		FROM market_enrichment_incident WHERE market_id=? ORDER BY observed_at_ms DESC,id DESC LIMIT ?`, marketID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MarketEnrichmentIncident
	for rows.Next() {
		var v MarketEnrichmentIncident
		if err := rows.Scan(&v.ObservedAtMS, &v.Stage, &v.MarketID, &v.ConditionID, &v.EventType,
			&v.TxHash, &v.LogIndex, &v.BlockNumber, &v.ElapsedMS, &v.DetailJSON); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ListMarketEnrichmentIncidentsSince returns incident evidence for aggregate
// monitoring. The caller deduplicates stages by the on-chain log identity.
func (s *MarketSQLite) ListMarketEnrichmentIncidentsSince(sinceMS int64) ([]MarketEnrichmentIncident, error) {
	rows, err := s.db.Query(`SELECT observed_at_ms,stage,market_id,condition_id,event_type,tx_hash,log_index,block_number,elapsed_ms,detail_json
		FROM market_enrichment_incident WHERE observed_at_ms>=? ORDER BY observed_at_ms ASC,id ASC`, sinceMS)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MarketEnrichmentIncident
	for rows.Next() {
		var v MarketEnrichmentIncident
		if err := rows.Scan(&v.ObservedAtMS, &v.Stage, &v.MarketID, &v.ConditionID, &v.EventType,
			&v.TxHash, &v.LogIndex, &v.BlockNumber, &v.ElapsedMS, &v.DetailJSON); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *MarketSQLite) LoadMarketSnapshotsByIDs(marketIDs []string) (map[string]MarketSnapshot, error) {
	out := make(map[string]MarketSnapshot, len(marketIDs))
	for start := 0; start < len(marketIDs); start += 500 {
		end := start + 500
		if end > len(marketIDs) {
			end = len(marketIDs)
		}
		args := make([]interface{}, 0, end-start)
		marks := make([]string, 0, end-start)
		for _, marketID := range marketIDs[start:end] {
			args = append(args, marketID)
			marks = append(marks, "?")
		}
		rows, err := s.db.Query(`SELECT market_id,snapshot_json FROM active_market_snapshot WHERE market_id IN (`+strings.Join(marks, ",")+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var marketID, encoded string
			if err := rows.Scan(&marketID, &encoded); err != nil {
				rows.Close()
				return nil, err
			}
			var snapshot MarketSnapshot
			if jsonErr := json.Unmarshal([]byte(encoded), &snapshot); jsonErr == nil {
				out[marketID] = snapshot
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

func (s *MarketSQLite) PruneMarketEnrichmentIncidents(beforeMS int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM market_enrichment_incident WHERE observed_at_ms<?`, beforeMS)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

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

// UpdateMarketStatus persists Gamma lifecycle metadata without changing the
// immutable market_id -> condition_id relationship.
func (s *MarketSQLite) UpdateMarketStatus(marketID string, active, closed bool, closedAt int64) error {
	_, err := s.db.Exec(`UPDATE market_condition_map SET active=?,closed=?,closed_at=?,last_seen_at=?,updated_at=?
		WHERE market_id=?`, boolInt(active), boolInt(closed), closedAt, time.Now().Unix(), time.Now().Unix(), marketID)
	return err
}

func (s *MarketSQLite) UpsertMarketConditionStatus(marketID, conditionID string, active, closed bool, closedAt int64) (inserted, conflict bool, err error) {
	inserted, conflict, err = s.UpsertMarketCondition(marketID, conditionID)
	if err != nil || conflict {
		return inserted, conflict, err
	}
	err = s.UpdateMarketStatus(marketID, active, closed, closedAt)
	return inserted, false, err
}

// UpsertMarketCatalogBatch persists one Gamma page in a single transaction.
// The immutable market_id -> condition_id relationship is never overwritten;
// lifecycle metadata is updated only when the existing condition_id agrees.
func (s *MarketSQLite) UpsertMarketCatalogBatch(records []MarketCatalogRecord) (inserted, conflicts int64, err error) {
	if len(records) == 0 {
		return 0, 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	insertStmt, err := tx.Prepare(`INSERT OR IGNORE INTO market_condition_map
		(market_id,condition_id,updated_at,active,closed,closed_at,last_seen_at)
		VALUES(?,?,?,?,?,?,?)`)
	if err != nil {
		return 0, 0, err
	}
	defer insertStmt.Close()
	updateStmt, err := tx.Prepare(`UPDATE market_condition_map
		SET active=?,closed=?,closed_at=?,last_seen_at=?,updated_at=?
		WHERE market_id=? AND condition_id=?`)
	if err != nil {
		return 0, 0, err
	}
	defer updateStmt.Close()
	var existingStmt *sql.Stmt
	existingStmt, err = tx.Prepare(`SELECT condition_id FROM market_condition_map WHERE market_id=?`)
	if err != nil {
		return 0, 0, err
	}
	defer existingStmt.Close()

	now := time.Now().Unix()
	for _, record := range records {
		if record.MarketID == "" || record.ConditionID == "" {
			continue
		}
		res, execErr := insertStmt.Exec(record.MarketID, record.ConditionID, now,
			boolInt(record.Active), boolInt(record.Closed), record.ClosedAt, now)
		if execErr != nil {
			err = execErr
			return 0, 0, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			inserted++
			continue
		}
		var existing string
		if scanErr := existingStmt.QueryRow(record.MarketID).Scan(&existing); scanErr != nil {
			err = scanErr
			return 0, 0, err
		}
		if existing != record.ConditionID {
			conflicts++
			continue
		}
		if _, execErr := updateStmt.Exec(boolInt(record.Active), boolInt(record.Closed),
			record.ClosedAt, now, now, record.MarketID, record.ConditionID); execErr != nil {
			err = execErr
			return 0, 0, err
		}
	}
	err = tx.Commit()
	return inserted, conflicts, err
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

// LoadActiveMarketConditionMap preloads both live mappings and every identity
// with a resident UMA snapshot. This keeps inactive-but-open order-book markets
// on the O(1) market_id hot path after restart.
func (s *MarketSQLite) LoadActiveMarketConditionMap(limit int) (map[string]string, error) {
	if limit <= 0 {
		limit = 100000
	}
	rows, err := s.db.Query(`SELECT m.market_id,m.condition_id FROM market_condition_map m
		WHERE (m.active=1 AND m.closed=0) OR EXISTS (
			SELECT 1 FROM active_market_snapshot s WHERE s.market_id=m.market_id AND s.active=1 AND s.closed=0
		)
		ORDER BY m.last_seen_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string, limit)
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

func (s *MarketSQLite) GetMarketSyncState(task string) (MarketSyncState, error) {
	var state MarketSyncState
	err := s.db.QueryRow(`SELECT next_cursor,status,scanned_count,completed_at FROM market_sync_state WHERE task_name=?`, task).
		Scan(&state.NextCursor, &state.Status, &state.ScannedCount, &state.CompletedAt)
	if err == sql.ErrNoRows {
		return state, nil
	}
	return state, err
}

func (s *MarketSQLite) SaveMarketSyncState(task, cursor, status string, scanned int64, lastError string) error {
	now := time.Now().Unix()
	completedAt := int64(0)
	if status == "complete" {
		completedAt = now
	}
	_, err := s.db.Exec(`INSERT INTO market_sync_state
		(task_name,next_cursor,status,scanned_count,started_at,completed_at,last_error)
		VALUES(?,?,?,?,?,?,?)
		ON CONFLICT(task_name) DO UPDATE SET
			next_cursor=excluded.next_cursor,status=excluded.status,
			scanned_count=excluded.scanned_count,
			completed_at=excluded.completed_at,last_error=excluded.last_error`,
		task, cursor, status, scanned, now, completedAt, lastError)
	return err
}

func (s *MarketSQLite) ResetMarketSyncState(task string) error {
	_, err := s.db.Exec(`DELETE FROM market_sync_state WHERE task_name=?`, task)
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

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

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
