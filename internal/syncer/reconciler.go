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
	"sync/atomic"
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
	InitBatch    int // 每次 drain 拉取 init question_id 的最大条数（单 SQL LIMIT）
	PendingBatch int // 每次扫 pending 的最大条数

	// running 标记当前是否有一轮 runOnce 正在进行，避免 tick 撞车导致并发调 RPC。
	// 上一轮没跑完时新 tick 会被跳过，让上一轮继续。
	running atomic.Bool
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

// Run 启动 reconciler 循环：启动时先跑一轮，然后按 Interval 周期 tick；
// 每次 tick 如检测到上一轮 runOnce 仍在进行（running==true），本次 tick 跳过，
// 让上一轮继续跑完整（"上个 10 分钟没跑完的继续跑"）。
func (r *Reconciler) Run(ctx context.Context) {
	if r.HttpRPCURL == "" {
		log.Printf("[WARN] reconciler: HttpRPCURL 为空，已禁用")
		return
	}
	log.Printf("[INFO] reconciler 启动 interval=%v discard_after=%v init_batch=%d",
		r.Interval, r.DiscardAfter, r.InitBatch)

	r.tryRunOnce(ctx)

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tryRunOnce(ctx)
		}
	}
}

// tryRunOnce 仅当没有上一轮在进行时才启动 runOnce；否则记录 skip 日志并返回。
// runOnce 放到独立 goroutine 里跑，保证主循环的 ticker 不被长时间 drain 阻塞。
func (r *Reconciler) tryRunOnce(ctx context.Context) {
	if !r.running.CompareAndSwap(false, true) {
		log.Printf("[INFO] reconciler: 上一轮仍在运行，本轮 tick 跳过")
		return
	}
	go func() {
		defer r.running.Store(false)
		start := time.Now()
		r.runOnce(ctx)
		log.Printf("[INFO] reconciler: 本轮结束 elapsed=%v", time.Since(start).Truncate(time.Millisecond))
	}()
}

// runOnce 串行处理：先扫 pending（快），再 drain init 回填（慢）。
func (r *Reconciler) runOnce(ctx context.Context) {
	r.sweepPending(ctx)
	r.backfillInitQuestionIDs(ctx)
}

// backfillInitQuestionIDs 回填 question_id 为空的 init 行。
// 单线程 drain：持续按 id DESC 拉批次 → 串行调 RPC 取 receipt → UPDATE question_id，
// 直到 ListInitsWithoutQuestionID 返回空或 ctx 取消；本轮没跑完的任务在下一 tick 时，
// 如果上一轮仍在 drain，ticker 的 tryRunOnce 会跳过本次 tick，让上一轮继续跑。
func (r *Reconciler) backfillInitQuestionIDs(ctx context.Context) {
	client, err := uma.NewClient(ctx, r.HttpRPCURL)
	if err != nil {
		log.Printf("[WARN] reconciler: NewClient: %v", err)
		return
	}
	defer client.Close()
	topic := common.HexToHash(uma.TopicQuestionInitialized)

	totalFilled, totalScanned := 0, 0
	for {
		if ctx.Err() != nil {
			return
		}
		rows, err := r.DB.ListInitsWithoutQuestionID(r.InitBatch)
		if err != nil {
			log.Printf("[WARN] reconciler: ListInitsWithoutQuestionID: %v", err)
			return
		}
		if len(rows) == 0 {
			break
		}
		totalScanned += len(rows)

		batchFilled := 0
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
			batchFilled++
		}
		totalFilled += batchFilled
		log.Printf("[INFO] reconciler: 本批次回填 %d/%d 条（累计 %d）", batchFilled, len(rows), totalFilled)
		if batchFilled == 0 {
			// 整批次全失败（RPC 问题或 tx 不可得），避免死循环
			log.Printf("[WARN] reconciler: 本批次全部失败，退出本轮 drain")
			return
		}
	}
	if totalFilled > 0 {
		log.Printf("[INFO] reconciler: 本轮 init question_id drain 完成 filled=%d scanned=%d",
			totalFilled, totalScanned)
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
