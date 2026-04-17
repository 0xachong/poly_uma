// reconciler：定时维护 question_id 关联一致性。
//
// 每 RECONCILER_INTERVAL（默认 10 分钟）执行两步：
//   1. 回填 init 行的 question_id：对每条 question_id 为空的 init 行，拉回执取 topic[1]
//   2. 扫 pending 表：找到对应 init 后提升到主表；超过 PENDING_RESOLVED_DISCARD_TTL（默认 24h）仍关联不上则丢弃并记日志
package syncer

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

// Reconciler 运行参数。
type Reconciler struct {
	DB           *store.SQLite
	HttpRPCURL   string
	Interval     time.Duration
	DiscardAfter time.Duration
	InitBatch    int // 每次回填 init question_id 的最大条数
	PendingBatch int // 每次扫 pending 的最大条数
}

// NewReconciler 用环境变量 + 合理默认值构造。HttpRPCURL 空字符串时 Run 会直接退出。
func NewReconciler(db *store.SQLite, httpRPCURL string) *Reconciler {
	return &Reconciler{
		DB:           db,
		HttpRPCURL:   httpRPCURL,
		Interval:     envReconcilerDuration("RECONCILER_INTERVAL", 10*time.Minute),
		DiscardAfter: envReconcilerDuration("PENDING_RESOLVED_DISCARD_TTL", 24*time.Hour),
		InitBatch:    100,
		PendingBatch: 1000,
	}
}

// Run 启动 reconciler 循环：启动时先跑一轮，然后按 Interval 周期执行。
func (r *Reconciler) Run(ctx context.Context) {
	if r.HttpRPCURL == "" {
		log.Printf("[WARN] reconciler: HttpRPCURL 为空，已禁用")
		return
	}
	log.Printf("[INFO] reconciler 启动 interval=%v discard_after=%v", r.Interval, r.DiscardAfter)

	r.runOnce(ctx)

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runOnce(ctx)
		}
	}
}

func (r *Reconciler) runOnce(ctx context.Context) {
	r.backfillInitQuestionIDs(ctx)
	r.sweepPending(ctx)
}

// backfillInitQuestionIDs 回填 question_id 为空的 init 行。
// 每次处理 InitBatch 条；更多的留给下一轮。
func (r *Reconciler) backfillInitQuestionIDs(ctx context.Context) {
	rows, err := r.DB.ListInitsWithoutQuestionID(r.InitBatch)
	if err != nil {
		log.Printf("[WARN] reconciler: ListInitsWithoutQuestionID: %v", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	client, err := uma.NewClient(ctx, r.HttpRPCURL)
	if err != nil {
		log.Printf("[WARN] reconciler: NewClient: %v", err)
		return
	}
	defer client.Close()

	topic := common.HexToHash(uma.TopicQuestionInitialized)
	filled := 0
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		receipt, err := client.TransactionReceipt(ctx, common.HexToHash(row.TxHash))
		if err != nil {
			log.Printf("[WARN] reconciler: receipt tx=%s: %v", row.TxHash, err)
			continue
		}
		qid := extractQuestionIDFromInitLogs(receipt.Logs, topic)
		if qid == "" {
			log.Printf("[WARN] reconciler: init tx 无 QuestionInitialized log tx=%s", row.TxHash)
			continue
		}
		if err := r.DB.UpdateQuestionID(row.ID, qid); err != nil {
			log.Printf("[WARN] reconciler: UpdateQuestionID id=%d: %v", row.ID, err)
			continue
		}
		filled++
	}
	if filled > 0 {
		log.Printf("[INFO] reconciler: 回填 init question_id %d 条（本轮候选 %d 条）", filled, len(rows))
	}
}

func extractQuestionIDFromInitLogs(logs []*ethtypes.Log, initTopic common.Hash) string {
	for _, lg := range logs {
		if len(lg.Topics) > 1 && lg.Topics[0] == initTopic {
			return lg.Topics[1].Hex()
		}
	}
	return ""
}

// sweepPending 扫 pending 表：能关联就提升到主表，老的直接丢。
func (r *Reconciler) sweepPending(ctx context.Context) {
	pendings, err := r.DB.ListPendingResolveds(r.PendingBatch)
	if err != nil {
		log.Printf("[WARN] reconciler: ListPendingResolveds: %v", err)
		return
	}
	if len(pendings) == 0 {
		return
	}
	discardBefore := time.Now().Unix() - int64(r.DiscardAfter.Seconds())

	promoted, discarded := 0, 0
	for _, p := range pendings {
		if ctx.Err() != nil {
			return
		}
		cid, err := r.DB.GetConditionIDByQuestionID(p.QuestionID)
		if err != nil {
			log.Printf("[WARN] reconciler: GetConditionIDByQuestionID qid=%s: %v", p.QuestionID, err)
			continue
		}
		if cid != "" {
			inserted, err := r.DB.PromotePending(p, cid)
			if err != nil {
				log.Printf("[WARN] reconciler: PromotePending id=%d: %v", p.ID, err)
				continue
			}
			if inserted {
				promoted++
				log.Printf("[INFO] reconciler: 提升 pending resolved qid=%s cid=%s tx=%s",
					p.QuestionID, cid, p.TxHash)
			}
			continue
		}
		if p.CreatedAt > 0 && p.CreatedAt < discardBefore {
			if err := r.DB.DeletePending(p.ID); err != nil {
				log.Printf("[WARN] reconciler: DeletePending id=%d: %v", p.ID, err)
				continue
			}
			discarded++
			log.Printf("[WARN] reconciler: 丢弃长时间未关联的 pending resolved qid=%s tx=%s age=%v",
				p.QuestionID, p.TxHash, time.Since(time.Unix(p.CreatedAt, 0)))
		}
	}
	if promoted > 0 || discarded > 0 {
		log.Printf("[INFO] reconciler sweep 完成: promoted=%d discarded=%d pending_total_scanned=%d",
			promoted, discarded, len(pendings))
	}
}

func envReconcilerDuration(key string, def time.Duration) time.Duration {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
