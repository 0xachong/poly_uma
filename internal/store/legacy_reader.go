package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type LegacyReader struct{ db *sql.DB }

func OpenLegacyReader(path string) (*LegacyReader, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro&_journal=WAL&_timeout=5000", path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &LegacyReader{db: db}, nil
}

func (r *LegacyReader) Close() error { return r.db.Close() }

func (r *LegacyReader) MarketMappingsAfter(lastRowID int64, limit int) ([]MarketMappingRecord, error) {
	rows, err := r.db.Query(`SELECT rowid,market_id,condition_id FROM market_condition_map
		WHERE rowid>? ORDER BY rowid LIMIT ?`, lastRowID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MarketMappingRecord
	for rows.Next() {
		var record MarketMappingRecord
		if err := rows.Scan(&record.RowID, &record.MarketID, &record.ConditionID); err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (r *LegacyReader) QuestionMappingsAfter(lastID int64, limit int) ([]QuestionMappingRecord, error) {
	rows, err := r.db.Query(`SELECT id,question_id,COALESCE(condition_id,''),COALESCE(market_id,''),transaction_hash
		FROM uma_oo_events WHERE id>? AND event_type='init' AND question_id IS NOT NULL AND question_id!=''
		ORDER BY id LIMIT ?`, lastID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QuestionMappingRecord
	for rows.Next() {
		var record QuestionMappingRecord
		if err := rows.Scan(&record.ID, &record.QuestionID, &record.ConditionID, &record.MarketID, &record.TxHash); err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}
