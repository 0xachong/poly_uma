// Package notify 提供飞书群机器人 Webhook 通知。
// 采用有界异步队列，仅通知 dispute 事件，失败仅 WARN 不阻塞主同步。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/polymas/go-polymarket-sdk/gamma"
	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

// DisputeDetail 包含 dispute 事件富化后的完整信息。
type DisputeDetail struct {
	Row store.EventRow
	Ev  *uma.DisputePriceEvent
	// StartupSnapshot 为 true 表示进程启动时根据 SQLite 补发的「当前库中最新一条」快照，非链上新事件。
	StartupSnapshot bool
}

// Feishu 飞书 Webhook 通知器。
type Feishu struct {
	webhookURL string
	client     *http.Client
	jobs       chan DisputeDetail
	proxyURL   string // Gamma API 代理，可选
}

// NewFeishu 创建飞书通知器并启动后台 worker。
// proxyURL 可选，用于 Gamma API 请求。
func NewFeishu(webhookURL, proxyURL string) *Feishu {
	f := &Feishu{
		webhookURL: webhookURL,
		client:     &http.Client{Timeout: 10 * time.Second},
		jobs:       make(chan DisputeDetail, 256),
		proxyURL:   proxyURL,
	}
	go f.worker()
	return f
}

// Send 将 dispute 详情放入异步队列，队列满时丢弃并记录 WARN。
func (f *Feishu) Send(detail DisputeDetail) {
	if f == nil {
		return
	}
	select {
	case f.jobs <- detail:
	default:
		log.Printf("[WARN] feishu notify queue full, dropped dispute tx=%s", detail.Row.TxHash)
	}
}

// Close 关闭队列。
func (f *Feishu) Close() {
	if f != nil && f.jobs != nil {
		close(f.jobs)
	}
}

func (f *Feishu) worker() {
	for detail := range f.jobs {
		if err := f.send(detail); err != nil {
			log.Printf("[WARN] feishu notify: %v", err)
		}
	}
}

func (f *Feishu) send(d DisputeDetail) error {
	bjt := time.Unix(d.Row.Timestamp, 0).In(time.FixedZone("UTC+8", 8*3600))
	timeStr := bjt.Format("2006-01-02 15:04:05")

	title := "-"
	resData := ""
	if d.Ev != nil && d.Ev.ParsedAncillary != nil {
		if d.Ev.ParsedAncillary.Title != "" {
			title = d.Ev.ParsedAncillary.Title
		}
		resData = d.Ev.ParsedAncillary.ResData
	}

	// 解读被争议的选项：ProposedPrice 1e18→Yes, 0→No
	disputedOption := interpretPrice(d.Row.Price, resData)

	marketID := d.Row.MarketID
	if marketID == "" {
		marketID = "-"
	}

	// 通过 Gamma API 按 condition_id 获取 tags、question、polymarket URL。
	// condition_id 是业务主键，比 marketID 更可靠（有些 dispute 事件 ancillary 解析失败但 condition_id 仍能被 syncer 富化填上）。
	tags := "-"
	polymarketURL := ""
	if d.Row.ConditionID != "" {
		mi := f.fetchMarketInfo(d.Row.ConditionID)
		if mi.Tags != "" {
			tags = mi.Tags
		}
		if mi.Question != "" && title == "-" {
			title = mi.Question
		}
		polymarketURL = mi.PolymarketURL
	}

	// 飞书富文本卡片消息（紧凑布局）
	elements := []any{
		mdSection(fmt.Sprintf("**%s**\n标签: %s | 市场ID: %s", title, tags, marketID)),
		mdSection(fmt.Sprintf("争议选项: %s (价格: %s)\n时间: %s | 区块: %d",
			disputedOption, displayPrice(d.Row.Price), timeStr, d.Row.BlockNumber)),
	}

	if polymarketURL != "" {
		elements = append(elements,
			map[string]any{
				"tag": "action",
				"actions": []any{
					button("查看市场", polymarketURL, "primary"),
					button("查看交易",
						"https://polygonscan.com/tx/"+d.Row.TxHash, "default"),
				},
			},
		)
	}

	headerTitle := "UMA 争议告警"
	if d.StartupSnapshot {
		headerTitle = "UMA 争议告警 · 启动快照"
	}
	card := map[string]any{
		"msg_type": "interactive",
		"card": map[string]any{
			"header": map[string]any{
				"template": "red",
				"title": map[string]any{
					"tag":     "plain_text",
					"content": headerTitle,
				},
			},
			"elements": elements,
		},
	}

	body, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("marshal card: %w", err)
	}

	resp, err := f.client.Post(f.webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	log.Printf("[INFO] feishu notify sent tx=%s", d.Row.TxHash)
	return nil
}

// interpretPrice 根据 scaled price 和 res_data 解读被争议的选项。
// UMA 约定：1e18 (scaled→"1") = Yes/p1, 0 = No/p2。
func interpretPrice(scaledPrice, resData string) string {
	switch scaledPrice {
	case "1", "1.00":
		return "Yes (p1: Yes was proposed)"
	case "0", "0.00":
		return "No (p2: No was proposed)"
	case "0.5", "0.50":
		return "Split / Unknown (p3)"
	default:
		if scaledPrice != "" {
			return fmt.Sprintf("Price=%s", scaledPrice)
		}
		return "-"
	}
}

func displayPrice(p string) string {
	if p == "" {
		return "-"
	}
	return p
}

func mdSection(content string) map[string]any {
	return map[string]any{
		"tag": "div",
		"text": map[string]any{
			"tag":     "lark_md",
			"content": content,
		},
	}
}


func button(text, url, typ string) map[string]any {
	return map[string]any{
		"tag":  "button",
		"text": map[string]any{"tag": "plain_text", "content": text},
		"type": typ,
		"url":  url,
	}
}

// marketInfo 聚合 Gamma API 返回的市场详情，用于飞书卡片展示。
type marketInfo struct {
	Tags          string // 逗号分隔的 tag 标签
	Question      string // 市场问题（标题）
	PolymarketURL string // 正确的 Polymarket 链接
}

// fetchMarketInfo 按 condition_id 通过 Gamma API 获取市场详情与 event URL。
// 失败时返回零值（降级，不影响通知）。URL 构建复用 SDK v1.6.1 的 GetEventURLByConditionID，
// 支持活跃 + 已关闭市场，自动处理单市场 / 多市场 event 的 slug 拼接规则。
func (f *Feishu) fetchMarketInfo(conditionID string) marketInfo {
	if conditionID == "" {
		return marketInfo{}
	}
	gammaClient := gamma.NewClient()

	var info marketInfo

	// 1. 取市场详情：先查活跃，miss 再查 closed（已结算市场）
	markets, err := gammaClient.GetMarketsByConditionIDs([]string{conditionID})
	if err != nil || len(markets) == 0 {
		markets, err = gammaClient.GetMarkets(1,
			gamma.WithConditionIDs([]string{conditionID}),
			gamma.WithClosed(true),
		)
	}
	if err == nil && len(markets) > 0 {
		m := &markets[0]
		if len(m.Tags) > 0 {
			labels := make([]string, 0, len(m.Tags))
			for _, t := range m.Tags {
				if t.Label != "" {
					labels = append(labels, t.Label)
				}
			}
			info.Tags = strings.Join(labels, ", ")
		}
		info.Question = m.Question
	}

	// 2. URL：SDK v1.6.1 的内置方法，活跃 / closed 都能覆盖
	if url, err := gammaClient.GetEventURLByConditionID(conditionID); err == nil {
		info.PolymarketURL = url
	}

	return info
}
