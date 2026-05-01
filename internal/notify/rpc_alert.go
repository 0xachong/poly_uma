// rpc_alert.go：RPC 节点（QuickNode 等）不可用告警。
// 连续断线超过 threshold 即推送一次飞书告警，cooldown 时间内不再重复；
// 恢复时若已发出告警则补一条恢复通知。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// RPCAlerter 跟踪 RPC 节点可用性并按阈值/冷却推送飞书告警。
// 零值不可用，请使用 NewRPCAlerter 构造。
type RPCAlerter struct {
	webhookURL string
	threshold  time.Duration
	cooldown   time.Duration
	client     *http.Client

	mu          sync.Mutex
	downSince   time.Time // 零值表示当前可用
	lastErr     string
	alertSent   bool      // 当前这段断线是否已发出告警
	lastAlertAt time.Time // 上次告警时刻，用于跨断线周期的冷却
}

// NewRPCAlerter 创建 RPC 告警器。webhookURL 为空时返回 nil（调用端可安全持空指针）。
func NewRPCAlerter(webhookURL string, threshold, cooldown time.Duration) *RPCAlerter {
	if webhookURL == "" {
		return nil
	}
	if threshold <= 0 {
		threshold = 2 * time.Minute
	}
	if cooldown <= 0 {
		cooldown = 2 * time.Hour
	}
	return &RPCAlerter{
		webhookURL: webhookURL,
		threshold:  threshold,
		cooldown:   cooldown,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// MarkDown 标记一次失败。首次失败记录起点；累计断线时长达到阈值且不在冷却期内时发出告警。
// reason 用于卡片正文展示最近一次错误。
func (a *RPCAlerter) MarkDown(reason string) {
	if a == nil {
		return
	}
	now := time.Now()
	a.mu.Lock()
	if a.downSince.IsZero() {
		a.downSince = now
	}
	a.lastErr = reason
	downFor := now.Sub(a.downSince)
	shouldAlert := !a.alertSent &&
		downFor >= a.threshold &&
		(a.lastAlertAt.IsZero() || now.Sub(a.lastAlertAt) >= a.cooldown)
	if shouldAlert {
		a.alertSent = true
		a.lastAlertAt = now
	}
	a.mu.Unlock()

	if shouldAlert {
		go a.sendDown(downFor, reason)
	}
}

// MarkUp 标记 RPC 已恢复；若本次断线曾触发告警，则补一条恢复通知。
func (a *RPCAlerter) MarkUp() {
	if a == nil {
		return
	}
	now := time.Now()
	a.mu.Lock()
	wasDown := !a.downSince.IsZero()
	downFor := time.Duration(0)
	if wasDown {
		downFor = now.Sub(a.downSince)
	}
	sentAlert := a.alertSent
	a.downSince = time.Time{}
	a.alertSent = false
	a.lastErr = ""
	a.mu.Unlock()

	if wasDown && sentAlert {
		go a.sendUp(downFor)
	}
}

func (a *RPCAlerter) sendDown(downFor time.Duration, reason string) {
	bjt := time.Now().In(time.FixedZone("UTC+8", 8*3600))
	card := map[string]any{
		"msg_type": "interactive",
		"card": map[string]any{
			"header": map[string]any{
				"template": "red",
				"title": map[string]any{
					"tag":     "plain_text",
					"content": "RPC 节点不可用告警",
				},
			},
			"elements": []any{
				mdSection(fmt.Sprintf("**Polygon RPC 已连续不可用 %s**", humanDuration(downFor))),
				mdSection(fmt.Sprintf("最近错误: %s\n时间: %s\n冷却: %s 内不再重复",
					truncate(reason, 200),
					bjt.Format("2006-01-02 15:04:05"),
					humanDuration(a.cooldown))),
			},
		},
	}
	if err := a.post(card); err != nil {
		log.Printf("[WARN] rpc alert (down): %v", err)
		return
	}
	log.Printf("[INFO] rpc alert sent: down for %s", humanDuration(downFor))
}

func (a *RPCAlerter) sendUp(downFor time.Duration) {
	bjt := time.Now().In(time.FixedZone("UTC+8", 8*3600))
	card := map[string]any{
		"msg_type": "interactive",
		"card": map[string]any{
			"header": map[string]any{
				"template": "green",
				"title": map[string]any{
					"tag":     "plain_text",
					"content": "RPC 节点已恢复",
				},
			},
			"elements": []any{
				mdSection(fmt.Sprintf("**Polygon RPC 恢复，断线总时长 %s**", humanDuration(downFor))),
				mdSection(fmt.Sprintf("时间: %s", bjt.Format("2006-01-02 15:04:05"))),
			},
		},
	}
	if err := a.post(card); err != nil {
		log.Printf("[WARN] rpc alert (up): %v", err)
		return
	}
	log.Printf("[INFO] rpc alert sent: recovered after %s", humanDuration(downFor))
}

func (a *RPCAlerter) post(card map[string]any) error {
	body, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("marshal card: %w", err)
	}
	resp, err := a.client.Post(a.webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}

func humanDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
