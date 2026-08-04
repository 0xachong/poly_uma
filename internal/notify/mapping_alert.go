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

type MappingAlert struct {
	Kind          string
	Severity      string
	MarketID      string
	ConditionID   string
	EventType     string
	TxHash        string
	LogIndex      int
	BlockNumber   uint64
	Detail        string
	Recovered     bool
	RepairElapsed time.Duration
}

// MappingAlerter reports catalog integrity faults asynchronously. Fingerprints
// are cooled down to avoid flooding; recovery is always emitted once.
type MappingAlerter struct {
	webhook  string
	client   *http.Client
	jobs     chan MappingAlert
	mu       sync.Mutex
	last     map[string]time.Time
	cooldown time.Duration
}

func NewMappingAlerter(webhook string) *MappingAlerter {
	if webhook == "" {
		return nil
	}
	a := &MappingAlerter{
		webhook: webhook, client: &http.Client{Timeout: 10 * time.Second},
		jobs: make(chan MappingAlert, 256), last: make(map[string]time.Time), cooldown: 5 * time.Minute,
	}
	go a.run()
	return a
}

func (a *MappingAlerter) Send(alert MappingAlert) {
	if a == nil {
		return
	}
	fingerprint := alert.Kind + ":" + alert.MarketID + ":" + alert.ConditionID
	now := time.Now()
	a.mu.Lock()
	last := a.last[fingerprint]
	if !alert.Recovered && now.Sub(last) < a.cooldown {
		a.mu.Unlock()
		return
	}
	a.last[fingerprint] = now
	a.mu.Unlock()
	select {
	case a.jobs <- alert:
	default:
		log.Printf("[WARN] mapping alert queue full: kind=%s market=%s", alert.Kind, alert.MarketID)
	}
}

func (a *MappingAlerter) Close() {
	if a != nil {
		close(a.jobs)
	}
}

func (a *MappingAlerter) run() {
	for alert := range a.jobs {
		if err := a.post(alert); err != nil {
			log.Printf("[WARN] mapping alert post failed: %v", err)
		}
	}
}

func (a *MappingAlerter) post(v MappingAlert) error {
	title := "UMA 市场映射异常"
	template := "red"
	if v.Recovered {
		title, template = "UMA 市场映射已自动修复", "green"
	}
	content := fmt.Sprintf("**类型:** %s\n**级别:** %s\n**market_id:** %s\n**condition_id:** %s\n**事件:** %s\n**区块:** %d\n**交易:** %s\n**log_index:** %d\n**详情:** %s\n**修复耗时:** %s",
		v.Kind, v.Severity, v.MarketID, v.ConditionID, v.EventType, v.BlockNumber, v.TxHash, v.LogIndex, v.Detail, v.RepairElapsed)
	body, err := json.Marshal(map[string]any{
		"msg_type": "interactive",
		"card": map[string]any{
			"header":   map[string]any{"template": template, "title": map[string]any{"tag": "plain_text", "content": title}},
			"elements": []any{map[string]any{"tag": "div", "text": map[string]any{"tag": "lark_md", "content": content}}},
		},
	})
	if err != nil {
		return err
	}
	resp, err := a.client.Post(a.webhook, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
