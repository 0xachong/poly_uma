package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// TickSizeAlert describes a Gamma market whose minimum order-price increment
// transitioned to the watched value.
type TickSizeAlert struct {
	MarketID     string
	ConditionID  string
	Question     string
	Slug         string
	Previous     float64
	Current      float64
	GammaUpdated int64
}

// TickSizeAlerter sends notifications on a bounded background queue. Send is
// intentionally non-blocking so Gamma catalog refresh cannot be delayed by a
// webhook request.
type TickSizeAlerter struct {
	webhook string
	client  *http.Client
	jobs    chan TickSizeAlert
}

func NewTickSizeAlerter(webhook string) *TickSizeAlerter {
	if webhook == "" {
		return nil
	}
	a := &TickSizeAlerter{
		webhook: webhook,
		client:  &http.Client{Timeout: 10 * time.Second},
		jobs:    make(chan TickSizeAlert, 256),
	}
	go a.run()
	return a
}

func (a *TickSizeAlerter) Send(alert TickSizeAlert) {
	if a == nil {
		return
	}
	select {
	case a.jobs <- alert:
	default:
		log.Printf("[WARN] tick_size alert queue full: market=%s", alert.MarketID)
	}
}

func (a *TickSizeAlerter) Close() {
	if a != nil {
		close(a.jobs)
	}
}

func (a *TickSizeAlerter) run() {
	for alert := range a.jobs {
		if err := a.post(alert); err != nil {
			log.Printf("[WARN] tick_size alert post failed: market=%s err=%v", alert.MarketID, err)
		}
	}
}

func (a *TickSizeAlerter) post(v TickSizeAlert) error {
	updated := "未知"
	if v.GammaUpdated > 0 {
		updated = time.UnixMilli(v.GammaUpdated).Format(time.RFC3339)
	}
	content := fmt.Sprintf("**market_id:** %s\n**condition_id:** %s\n**问题:** %s\n**slug:** %s\n**原 tick_size:** %g\n**新 tick_size:** %g\n**Gamma updated_at:** %s\n**检测时间:** %s",
		v.MarketID, v.ConditionID, v.Question, v.Slug, v.Previous, v.Current, updated, time.Now().Format(time.RFC3339))
	body, err := json.Marshal(map[string]any{
		"msg_type": "interactive",
		"card": map[string]any{
			"header":   map[string]any{"template": "orange", "title": map[string]any{"tag": "plain_text", "content": "UMA 市场 tick_size 变为 0.01"}},
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
	log.Printf("[INFO] tick_size alert sent: market=%s previous=%g current=%g", v.MarketID, v.Previous, v.Current)
	return nil
}
