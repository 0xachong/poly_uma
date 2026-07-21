package syncer

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

type AuxiliaryMigrator struct {
	Source      *store.LegacyReader
	Market      *store.MarketSQLite
	Maintenance *store.MaintenanceSQLite
	HotDB       *store.SQLite
	BatchSize   int
	Yield       time.Duration
}

func (m *AuxiliaryMigrator) Run(ctx context.Context) {
	if m == nil || m.Source == nil || m.Market == nil || m.Maintenance == nil {
		return
	}
	if m.BatchSize <= 0 {
		m.BatchSize = 500
	}
	if m.Yield <= 0 {
		m.Yield = 100 * time.Millisecond
	}
	log.Printf("[INFO] auxiliary migration started: batch=%d yield=%v", m.BatchSize, m.Yield)
	m.runMarket(ctx)
	m.runQuestions(ctx)
}

func (m *AuxiliaryMigrator) runMarket(ctx context.Context) {
	const task = "market_catalog_v1"
	state, err := m.Maintenance.GetMigrationState(task)
	if err != nil || state.Status == "complete" {
		return
	}
	for ctx.Err() == nil {
		if !m.waitForHotIdle(ctx) {
			return
		}
		records, err := m.Source.MarketMappingsAfter(state.LastID, m.BatchSize)
		if err == nil && len(records) > 0 {
			err = m.Market.UpsertMarketBatch(records)
		}
		if err != nil {
			_ = m.Maintenance.SaveMigrationState(task, state.LastID, "error", err.Error())
			log.Printf("[WARN] auxiliary market migration failed: cursor=%d err=%v", state.LastID, err)
			if isMigrationBusy(err) && migrationYield(ctx, 5*time.Second) {
				continue
			}
			return
		}
		if len(records) == 0 {
			_ = m.Maintenance.SaveMigrationState(task, state.LastID, "complete", "")
			log.Printf("[INFO] auxiliary market migration complete: cursor=%d", state.LastID)
			return
		}
		state.LastID = records[len(records)-1].RowID
		_ = m.Maintenance.SaveMigrationState(task, state.LastID, "running", "")
		if !migrationYield(ctx, m.Yield) {
			return
		}
	}
}

func (m *AuxiliaryMigrator) runQuestions(ctx context.Context) {
	const task = "question_map_v1"
	state, err := m.Maintenance.GetMigrationState(task)
	if err != nil || state.Status == "complete" {
		return
	}
	for ctx.Err() == nil {
		if !m.waitForHotIdle(ctx) {
			return
		}
		records, err := m.Source.QuestionMappingsAfter(state.LastID, m.BatchSize)
		if err == nil && len(records) > 0 {
			err = m.Maintenance.UpsertQuestionBatch(records)
		}
		if err != nil {
			_ = m.Maintenance.SaveMigrationState(task, state.LastID, "error", err.Error())
			log.Printf("[WARN] auxiliary question migration failed: cursor=%d err=%v", state.LastID, err)
			if isMigrationBusy(err) && migrationYield(ctx, 5*time.Second) {
				continue
			}
			return
		}
		if len(records) == 0 {
			_ = m.Maintenance.SaveMigrationState(task, state.LastID, "complete", "")
			log.Printf("[INFO] auxiliary question migration complete: cursor=%d", state.LastID)
			return
		}
		state.LastID = records[len(records)-1].ID
		_ = m.Maintenance.SaveMigrationState(task, state.LastID, "running", "")
		if !migrationYield(ctx, m.Yield) {
			return
		}
	}
}

func isMigrationBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "sqlite_busy") || strings.Contains(message, "database is locked")
}

func (m *AuxiliaryMigrator) waitForHotIdle(ctx context.Context) bool {
	for m.HotDB != nil {
		stats := m.HotDB.PipelineStats()
		if stats.QueueDepth == 0 && stats.Processing == 0 {
			break
		}
		if !migrationYield(ctx, time.Second) {
			return false
		}
	}
	return ctx.Err() == nil
}

func migrationYield(ctx context.Context, delay time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}
