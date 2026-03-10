// Package audit 提供远程 MySQL 审计写入。
// 每条事件以原始 JSON 写入 uma_audit_events 表，仅用于离线 debug，不参与查询。
// 写入失败仅打印 WARN，不影响主同步流程。
package audit

import (
	"database/sql"
	"encoding/json"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

const createTable = `
CREATE TABLE IF NOT EXISTS uma_audit_events (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_type   VARCHAR(32)  NOT NULL,
    tx_hash      VARCHAR(66)  NOT NULL,
    log_index    INT          NOT NULL,
    block_number BIGINT       NOT NULL,
    block_ts     BIGINT,
    question_id  VARCHAR(66),
    market_id    VARCHAR(64),
    raw_payload  JSON,
    created_at   DATETIME     NOT NULL DEFAULT NOW(),
    UNIQUE KEY uk_tx_log (tx_hash, log_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// MySQL 封装审计写入连接。
type MySQL struct {
	db *sql.DB
}

// Open 连接 MySQL 并确保审计表存在。
// dsn 格式：user:pass@tcp(host:port)/dbname?parseTime=true&charset=utf8mb4
func Open(dsn string) (*MySQL, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(createTable); err != nil {
		db.Close()
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	return &MySQL{db: db}, nil
}

// Close 关闭连接。
func (m *MySQL) Close() error {
	if m != nil && m.db != nil {
		return m.db.Close()
	}
	return nil
}

// Insert 异步追加一条事件记录，失败仅记录 WARN 日志。
// payload 为任意可 JSON 序列化的事件结构体。
func (m *MySQL) Insert(eventType, txHash string, logIndex int,
	blockNumber uint64, blockTs int64,
	questionID, marketID string,
	payload interface{}) {

	if m == nil {
		return
	}
	go func() {
		raw, err := json.Marshal(payload)
		if err != nil {
			log.Printf("[WARN] audit.Insert marshal: %v", err)
			return
		}
		_, err = m.db.Exec(
			`INSERT IGNORE INTO uma_audit_events
			 (event_type,tx_hash,log_index,block_number,block_ts,question_id,market_id,raw_payload)
			 VALUES (?,?,?,?,?,?,?,?)`,
			eventType, txHash, logIndex, blockNumber, blockTs,
			nullStr(questionID), nullStr(marketID), string(raw),
		)
		if err != nil {
			log.Printf("[WARN] audit insert %s tx=%s: %v", eventType, txHash, err)
		}
	}()
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
