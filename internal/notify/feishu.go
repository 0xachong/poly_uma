// Package notify 提供飞书群机器人 Webhook 通知。
// 采用有界异步队列，仅通知 dispute 事件，失败仅 WARN 不阻塞主同步。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"


	"time"

	"github.com/polymas/go-polymarket-sdk/gamma"
	"github.com/polymas/go-polymarket-sdk/types"
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

	// 通过 Gamma API 获取 tags、question 和 polymarket 链接
	tags := "-"
	polymarketURL := ""
	if d.Row.MarketID != "" {
		mi := f.fetchMarketInfo(d.Row.MarketID)
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

// fetchMarketInfo 通过 Gamma API（/markets/{id}?include_tag=true）获取市场详情。
// 失败时返回零值（降级，不影响通知）。
func (f *Feishu) fetchMarketInfo(marketID string) marketInfo {
	gammaClient := gamma.NewClient()
	m, err := gammaClient.GetMarket(marketID)
	if err != nil || m == nil {
		return marketInfo{}
	}

	var info marketInfo

	// tags
	if len(m.Tags) > 0 {
		labels := make([]string, 0, len(m.Tags))
		for _, t := range m.Tags {
			if t.Label != "" {
				labels = append(labels, t.Label)
			}
		}
		info.Tags = strings.Join(labels, ", ")
	}

	// question
	info.Question = m.Question

	// 构建正确的 Polymarket 链接
	info.PolymarketURL = f.resolvePolymarketURL(gammaClient, m)

	return info
}

// dateInSlugRe 匹配 slug 中的日期部分（如 "xxx-2026-04-13-yyy" 中的 "2026-04-13"）。
var dateInSlugRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)

// resolvePolymarketURL 构建正确的 Polymarket event 链接。
// 优先通过 eventID 查 event slug；其次从 market slug 中提取日期前缀，
// 尝试 GetEventBySlug 查父 event；最终 fallback 用 market slug。
func (f *Feishu) resolvePolymarketURL(gammaClient gamma.Client, m *types.GammaMarket) string {
	// 1. 有 eventID 时直接查
	if m.EventID != "" {
		if eid, err := strconv.Atoi(m.EventID); err == nil {
			if event, err := gammaClient.GetEvent(eid, nil, nil); err == nil && event.Slug != "" {
				return "https://polymarket.com/event/" + event.Slug
			}
		}
	}

	// 2. 从 market slug 中截取到日期部分作为 event slug 尝试
	if m.Slug != "" {
		if loc := dateInSlugRe.FindStringIndex(m.Slug); loc != nil {
			eventSlug := m.Slug[:loc[1]] // 如 "dota2-mideng-yb1-2026-04-13"
			if event, err := gammaClient.GetEventBySlug(eventSlug, nil, nil); err == nil && event != nil && event.Slug != "" {
				return "https://polymarket.com/event/" + event.Slug
			}
		}
		// 3. fallback: market slug
		return "https://polymarket.com/event/" + m.Slug
	}

	return ""
}
