package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const eventShardSchema = `
CREATE TABLE IF NOT EXISTS uma_oo_events (
    id INTEGER PRIMARY KEY,
    cursor_id INTEGER NOT NULL DEFAULT 0,
    event_type TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_number INTEGER,
    timestamp INTEGER,
    condition_id TEXT,
    market_id TEXT,
    price TEXT,
    question_id TEXT,
    UNIQUE(transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_shard_type_cursor ON uma_oo_events(event_type,cursor_id);
CREATE INDEX IF NOT EXISTS idx_shard_block_log ON uma_oo_events(block_number,log_index);
CREATE INDEX IF NOT EXISTS idx_shard_timestamp_order ON uma_oo_events(timestamp,transaction_hash,log_index);
CREATE TABLE IF NOT EXISTS legacy_replication_outbox (
    event_id INTEGER PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT 0,
    completed_at INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_legacy_replication_pending
    ON legacy_replication_outbox(completed_at,event_id);
`

// EventShardReplicator implements phase one of the lossless event-store split.
// The legacy database remains authoritative; its transactional outbox and a
// resumable historical watermark feed the two shadow databases.
type EventShardReplicator struct {
	source            *SQLite
	signal            *sql.DB
	lifecycle         *sql.DB
	legacyReplication atomic.Bool
}

func OpenEventShardReplicator(source *SQLite, signalPath, lifecyclePath string) (*EventShardReplicator, error) {
	open := func(path string) (*sql.DB, error) {
		db, err := sql.Open("sqlite", fmt.Sprintf("file:%s", path))
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		for _, pragma := range []string{
			`PRAGMA journal_mode=WAL`,
			`PRAGMA busy_timeout=5000`,
			`PRAGMA synchronous=NORMAL`,
		} {
			if _, err := db.Exec(pragma); err != nil {
				db.Close()
				return nil, fmt.Errorf("configure event shard %s: %w", pragma, err)
			}
		}
		if _, err := db.Exec(eventShardSchema); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}
	signal, err := open(signalPath)
	if err != nil {
		return nil, fmt.Errorf("open signal shadow: %w", err)
	}
	lifecycle, err := open(lifecyclePath)
	if err != nil {
		signal.Close()
		return nil, fmt.Errorf("open lifecycle shadow: %w", err)
	}
	r := &EventShardReplicator{source: source, signal: signal, lifecycle: lifecycle}
	if err := r.initializeWatermark(); err != nil {
		r.Close()
		return nil, err
	}
	source.shadowWriteEnabled.Store(true)
	return r, nil
}

func (r *EventShardReplicator) Close() error {
	var first error
	if r.signal != nil {
		first = r.signal.Close()
	}
	if r.lifecycle != nil {
		if err := r.lifecycle.Close(); first == nil {
			first = err
		}
	}
	return first
}

// EnableShardPrimary switches only new event writes. It first drains any
// reverse outbox left by a previous primary run so the legacy read model is
// complete before startup consumers load it.
func (r *EventShardReplicator) EnableShardPrimary() error {
	return r.EnableShardPrimaryWithLegacyReplication(true)
}

// EnableShardPrimaryWithLegacyReplication switches new writes to the shards.
// Existing reverse-outbox rows are drained only while legacy compatibility is
// requested. If legacyReplication is false, the legacy event table becomes a
// read-only cutover archive and neither startup nor new writes pay its I/O cost.
func (r *EventShardReplicator) EnableShardPrimaryWithLegacyReplication(legacyReplication bool) error {
	if legacyReplication {
		for {
			copied, err := r.copyLegacyOutboxBatch(1000)
			if err != nil {
				return err
			}
			if copied == 0 {
				break
			}
		}
	}
	r.source.shardSignal = r.signal
	r.source.shardLifecycle = r.lifecycle
	r.source.shardLegacyReplication.Store(legacyReplication)
	r.source.shadowWriteEnabled.Store(false)
	r.source.shardPrimary.Store(true)
	r.legacyReplication.Store(legacyReplication)
	return nil
}

func (r *EventShardReplicator) initializeWatermark() error {
	var watermark int64
	if err := r.source.db.QueryRow(`SELECT watermark_id FROM event_shard_migration_state WHERE id=1`).Scan(&watermark); err != nil {
		return err
	}
	if watermark != 0 {
		return nil
	}
	if err := r.source.db.QueryRow(`SELECT COALESCE(MAX(id),0) FROM uma_oo_events`).Scan(&watermark); err != nil {
		return err
	}
	_, err := r.source.db.Exec(`UPDATE event_shard_migration_state SET watermark_id=?,updated_at=? WHERE id=1`, watermark, time.Now().UnixMilli())
	return err
}

func (r *EventShardReplicator) Run(ctx context.Context) {
	backfillTicker := time.NewTicker(100 * time.Millisecond)
	outboxTicker := time.NewTicker(50 * time.Millisecond)
	statsTicker := time.NewTicker(5 * time.Second)
	defer backfillTicker.Stop()
	defer outboxTicker.Stop()
	defer statsTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-backfillTicker.C:
			if err := r.copyBackfillBatch(500); err != nil {
				r.source.shadowCopyFailures.Add(1)
				log.Printf("[WARN] event shard historical backfill: %v", err)
			}
		case <-outboxTicker.C:
			if err := r.copyOutboxBatch(200); err != nil {
				r.source.shadowCopyFailures.Add(1)
				log.Printf("[WARN] event shard outbox copy: %v", err)
			}
			if r.legacyReplication.Load() {
				if _, err := r.copyLegacyOutboxBatch(200); err != nil {
					r.source.shadowCopyFailures.Add(1)
					log.Printf("[WARN] legacy event reverse copy: %v", err)
				}
			}
		case <-statsTicker.C:
			r.refreshStats()
		}
	}
}

// ScanEventsSince reads the two authoritative shard databases for rebuilding
// the recent in-memory read model after legacy reverse replication is disabled.
func (r *EventShardReplicator) ScanEventsSince(since int64) ([]EventRow, error) {
	var out []EventRow
	for _, db := range []*sql.DB{r.signal, r.lifecycle} {
		rows, err := scanEventsSinceDB(db, since)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func scanEventsSinceDB(db *sql.DB, since int64) ([]EventRow, error) {
	rows, err := db.Query(`SELECT id,cursor_id,event_type,transaction_hash,log_index,block_number,timestamp,
		condition_id,market_id,price,question_id FROM uma_oo_events WHERE timestamp>=?`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var v EventRow
		var conditionID, marketID, price, questionID sql.NullString
		if err := rows.Scan(&v.ID, &v.CursorID, &v.EventType, &v.TxHash, &v.LogIndex, &v.BlockNumber,
			&v.Timestamp, &conditionID, &marketID, &price, &questionID); err != nil {
			return nil, err
		}
		v.ConditionID, v.MarketID, v.Price, v.QuestionID = conditionID.String, marketID.String, price.String, questionID.String
		out = append(out, v)
	}
	return out, rows.Err()
}

func (r *EventShardReplicator) copyLegacyOutboxBatch(limit int) (int, error) {
	total := 0
	for _, shard := range []*sql.DB{r.signal, r.lifecycle} {
		rows, err := shard.Query(`SELECT e.id,e.cursor_id,e.event_type,e.transaction_hash,e.log_index,e.block_number,e.timestamp,
			e.condition_id,e.market_id,e.price,e.question_id FROM legacy_replication_outbox o
			JOIN uma_oo_events e ON e.id=o.event_id WHERE o.completed_at=0 ORDER BY o.event_id LIMIT ?`, limit)
		if err != nil {
			return total, err
		}
		events, err := scanShardRows(rows)
		rows.Close()
		if err != nil {
			return total, err
		}
		for _, v := range events {
			_, _, _, err := r.source.insertLegacyEvent(v.eventType, v.txHash, v.logIndex, v.blockNumber, v.timestamp,
				v.conditionID.String, v.marketID.String, v.price.String, v.questionID.String)
			if err != nil {
				_, _ = shard.Exec(`UPDATE legacy_replication_outbox SET last_error=? WHERE event_id=?`, err.Error(), v.id)
				return total, err
			}
			if _, err := shard.Exec(`UPDATE legacy_replication_outbox SET completed_at=?,last_error='' WHERE event_id=?`, time.Now().UnixMilli(), v.id); err != nil {
				return total, err
			}
			total++
		}
	}
	return total, nil
}

type shardEventRow struct {
	id, cursorID                 int64
	eventType, txHash            string
	logIndex                     int
	blockNumber                  uint64
	timestamp                    int64
	conditionID, marketID, price sql.NullString
	questionID                   sql.NullString
}

func scanShardRows(rows *sql.Rows) ([]shardEventRow, error) {
	var out []shardEventRow
	for rows.Next() {
		var v shardEventRow
		if err := rows.Scan(&v.id, &v.cursorID, &v.eventType, &v.txHash, &v.logIndex, &v.blockNumber,
			&v.timestamp, &v.conditionID, &v.marketID, &v.price, &v.questionID); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (r *EventShardReplicator) target(eventType string) *sql.DB {
	if eventType == "propose" || eventType == "dispute" {
		return r.signal
	}
	return r.lifecycle
}

func insertShardRows(db *sql.DB, events []shardEventRow) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO uma_oo_events
		(id,cursor_id,event_type,transaction_hash,log_index,block_number,timestamp,condition_id,market_id,price,question_id)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, v := range events {
		if _, err := stmt.Exec(v.id, v.cursorID, v.eventType, v.txHash, v.logIndex, v.blockNumber,
			v.timestamp, v.conditionID, v.marketID, v.price, v.questionID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *EventShardReplicator) copyBackfillBatch(limit int) error {
	stats := r.source.PipelineStats()
	if stats.HighQueueDepth > 0 || stats.Processing > 0 {
		return nil
	}
	var cursor, watermark int64
	if err := r.source.db.QueryRow(`SELECT backfill_cursor,watermark_id FROM event_shard_migration_state WHERE id=1`).Scan(&cursor, &watermark); err != nil {
		return err
	}
	r.source.shadowBackfillCursor.Store(cursor)
	if cursor >= watermark {
		return nil
	}
	rows, err := r.source.db.Query(`SELECT id,cursor_id,event_type,transaction_hash,log_index,block_number,timestamp,
		condition_id,market_id,price,question_id FROM uma_oo_events WHERE id>? AND id<=? ORDER BY id LIMIT ?`, cursor, watermark, limit)
	if err != nil {
		return err
	}
	events, err := scanShardRows(rows)
	rows.Close()
	if err != nil || len(events) == 0 {
		return err
	}
	var signal, lifecycle []shardEventRow
	for _, v := range events {
		if v.eventType == "propose" || v.eventType == "dispute" {
			signal = append(signal, v)
		} else {
			lifecycle = append(lifecycle, v)
		}
	}
	if err := insertShardRows(r.signal, signal); err != nil {
		return err
	}
	if err := insertShardRows(r.lifecycle, lifecycle); err != nil {
		return err
	}
	last := events[len(events)-1].id
	_, err = r.source.db.Exec(`UPDATE event_shard_migration_state SET backfill_cursor=?,updated_at=? WHERE id=1`, last, time.Now().UnixMilli())
	r.source.shadowBackfillCursor.Store(last)
	return err
}

func (r *EventShardReplicator) copyOutboxBatch(limit int) error {
	rows, err := r.source.db.Query(`SELECT e.id,e.cursor_id,e.event_type,e.transaction_hash,e.log_index,e.block_number,e.timestamp,
		e.condition_id,e.market_id,e.price,e.question_id FROM event_shard_outbox o
		JOIN uma_oo_events e ON e.id=o.event_id WHERE o.completed_at=0 ORDER BY o.id LIMIT ?`, limit)
	if err != nil {
		return err
	}
	events, err := scanShardRows(rows)
	rows.Close()
	if err != nil {
		return err
	}
	for _, v := range events {
		if err := insertShardRows(r.target(v.eventType), []shardEventRow{v}); err != nil {
			_, _ = r.source.db.Exec(`UPDATE event_shard_outbox SET last_error=? WHERE transaction_hash=? AND log_index=?`, err.Error(), v.txHash, v.logIndex)
			return err
		}
		if _, err := r.source.db.Exec(`UPDATE event_shard_outbox SET completed_at=?,last_error='' WHERE transaction_hash=? AND log_index=?`, time.Now().UnixMilli(), v.txHash, v.logIndex); err != nil {
			return err
		}
	}
	return nil
}

func (r *EventShardReplicator) refreshStats() {
	var pending, oldest int64
	_ = r.source.db.QueryRow(`SELECT COUNT(*),COALESCE(MIN(created_at),0) FROM event_shard_outbox WHERE completed_at=0`).Scan(&pending, &oldest)
	r.source.shadowOutboxPending.Store(pending)
	if oldest > 0 {
		r.source.shadowOutboxOldestMS.Store(time.Now().UnixMilli() - oldest)
	} else {
		r.source.shadowOutboxOldestMS.Store(0)
	}
	var cursor, watermark int64
	_ = r.source.db.QueryRow(`SELECT backfill_cursor,watermark_id FROM event_shard_migration_state WHERE id=1`).Scan(&cursor, &watermark)
	r.source.shadowBackfillCursor.Store(cursor)
	if cursor < watermark || pending != 0 {
		return
	}
	// Full-table counts are a phase-one cutover gate. Once the shards become
	// primary, running COUNT(*) on the single-connection signal database can
	// monopolize that connection for tens of seconds and block ProposePrice.
	// Durability is monitored through the per-shard reverse outboxes instead.
	if r.source.shardPrimary.Load() {
		r.source.shadowConsistencyMismatch.Store(0)
		return
	}
	var sourceSignal, sourceLifecycle, shadowSignal, shadowLifecycle int64
	_ = r.source.db.QueryRow(`SELECT COUNT(*) FROM uma_oo_events WHERE event_type IN ('propose','dispute')`).Scan(&sourceSignal)
	_ = r.source.db.QueryRow(`SELECT COUNT(*) FROM uma_oo_events WHERE event_type NOT IN ('propose','dispute')`).Scan(&sourceLifecycle)
	_ = r.signal.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&shadowSignal)
	_ = r.lifecycle.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&shadowLifecycle)
	mismatch := abs64(sourceSignal-shadowSignal) + abs64(sourceLifecycle-shadowLifecycle)
	r.source.shadowConsistencyMismatch.Store(mismatch)
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
