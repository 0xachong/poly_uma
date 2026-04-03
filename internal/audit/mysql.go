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

// auditJob 表示一条待写入的审计记录。
type auditJob struct {
	eventType   string
	txHash      string
	logIndex    int
	blockNumber uint64
	blockTs     int64
	questionID  string
	marketID    string
	payload     interface{}
}

// MySQL 封装审计写入连接，使用有界异步队列避免 goroutine 爆炸。
type MySQL struct {
	db   *sql.DB
	jobs chan auditJob
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
	m := &MySQL{db: db, jobs: make(chan auditJob, 1024)}
	// 启动 2 个 worker 消费队列（匹配 MaxIdleConns）
	for i := 0; i < 2; i++ {
		go m.worker()
	}
	return m, nil
}

func (m *MySQL) worker() {
	for job := range m.jobs {
		raw, err := json.Marshal(job.payload)
		if err != nil {
			log.Printf("[WARN] audit.Insert marshal: %v", err)
			continue
		}
		_, err = m.db.Exec(
			`INSERT IGNORE INTO uma_audit_events
			 (event_type,tx_hash,log_index,block_number,block_ts,question_id,market_id,raw_payload)
			 VALUES (?,?,?,?,?,?,?,?)`,
			job.eventType, job.txHash, job.logIndex, job.blockNumber, job.blockTs,
			nullStr(job.questionID), nullStr(job.marketID), string(raw),
		)
		if err != nil {
			log.Printf("[WARN] audit insert %s tx=%s: %v", job.eventType, job.txHash, err)
		}
	}
}

// Close 关闭队列并等待 worker 完成，然后关闭数据库连接。
func (m *MySQL) Close() error {
	if m != nil && m.jobs != nil {
		close(m.jobs)
	}
	if m != nil && m.db != nil {
		return m.db.Close()
	}
	return nil
}

// Insert 将审计记录放入有界队列，队列满时非阻塞丢弃并记录 WARN。
// payload 为任意可 JSON 序列化的事件结构体。
func (m *MySQL) Insert(eventType, txHash string, logIndex int,
	blockNumber uint64, blockTs int64,
	questionID, marketID string,
	payload interface{}) {

	if m == nil {
		return
	}
	select {
	case m.jobs <- auditJob{eventType, txHash, logIndex, blockNumber, blockTs, questionID, marketID, payload}:
	default:
		log.Printf("[WARN] audit queue full, dropped %s tx=%s", eventType, txHash)
	}
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
