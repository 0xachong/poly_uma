package uma

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Client 封装 ethclient，提供 UMA 事件拉取与订阅。
type Client struct {
	ec *ethclient.Client
}

// NewClient 创建客户端，rpcURL 支持 http/https/wss。
func NewClient(ctx context.Context, rpcURL string) (*Client, error) {
	ec, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("ethclient dial %s: %w", rpcURL, err)
	}
	return &Client{ec: ec}, nil
}

// Close 关闭连接。
func (c *Client) Close() {
	if c.ec != nil {
		c.ec.Close()
	}
}

// LatestBlock 返回当前链头区块号。
func (c *Client) LatestBlock(ctx context.Context) (uint64, error) {
	h, err := c.ec.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("HeaderByNumber(latest): %w", err)
	}
	return h.Number.Uint64(), nil
}

// BlockTimestamp 返回指定区块的上链时间（Unix 秒）。
func (c *Client) BlockTimestamp(ctx context.Context, blockNum uint64) (int64, error) {
	h, err := c.ec.HeaderByNumber(ctx, new(big.Int).SetUint64(blockNum))
	if err != nil {
		return 0, fmt.Errorf("HeaderByNumber(%d): %w", blockNum, err)
	}
	return int64(h.Time), nil
}

// FetchLogs 拉取 [fromBlock, toBlock] 范围内的全部 UMA 六类事件（不限合约地址）。
func (c *Client) FetchLogs(ctx context.Context, fromBlock, toBlock uint64) ([]ethtypes.Log, error) {
	topics := make([]common.Hash, 0, len(AllTopics))
	for _, t := range AllTopics {
		topics = append(topics, common.HexToHash(t))
	}
	q := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Topics:    [][]common.Hash{topics},
	}
	logs, err := c.ec.FilterLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("FilterLogs [%d,%d]: %w", fromBlock, toBlock, err)
	}
	return logs, nil
}

// SubscribedEvent 订阅收到的一条：原始 log + 解析后事件（解析失败时 Event 为 nil）。
type SubscribedEvent struct {
	Raw   ethtypes.Log
	Event *Event
}

// Subscribe 通过 WebSocket 订阅 UMA 六类事件。
// 返回 channel 和 cleanup 函数；context 取消或订阅断开时 channel 关闭。
func (c *Client) Subscribe(ctx context.Context) (<-chan *SubscribedEvent, func(), error) {
	topics := make([]common.Hash, 0, len(AllTopics))
	for _, t := range AllTopics {
		topics = append(topics, common.HexToHash(t))
	}
	q := ethereum.FilterQuery{Topics: [][]common.Hash{topics}}
	logsCh := make(chan ethtypes.Log, 64)
	sub, err := c.ec.SubscribeFilterLogs(ctx, q, logsCh)
	if err != nil {
		return nil, nil, fmt.Errorf("SubscribeFilterLogs: %w", err)
	}

	outCh := make(chan *SubscribedEvent, 64)
	go func() {
		defer close(outCh)
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				if err != nil {
					log.Printf("[WARN] UMA 订阅断开: %v", err)
				}
				return
			case vLog, ok := <-logsCh:
				if !ok {
					return
				}
				ev, _ := ParseLog(vLog)
				select {
				case outCh <- &SubscribedEvent{Raw: vLog, Event: ev}:
				case <-ctx.Done():
					sub.Unsubscribe()
					return
				}
			}
		}
	}()
	return outCh, func() { sub.Unsubscribe() }, nil
}

// WssToHttp 将 wss:// 地址转为 https://，用于通过同节点 HTTP RPC 拉取历史数据。
func WssToHttp(wssURL string) string {
	s := strings.TrimSpace(wssURL)
	if strings.HasPrefix(s, "wss://") {
		return "https://" + s[6:]
	}
	if strings.HasPrefix(s, "ws://") {
		return "http://" + s[5:]
	}
	return s
}

// GammaConditionID 通过 Polymarket Gamma API 将 market_id 转为 CTF condition_id。
// 失败时返回空字符串（降级，不影响主流程）。
func GammaConditionID(marketID string, proxyURL string) string {
	if marketID == "" {
		return ""
	}
	apiURL := fmt.Sprintf("https://gamma-api.polymarket.com/markets/%s", url.PathEscape(marketID))
	client := &http.Client{Timeout: 8 * time.Second}
	if proxyURL != "" {
		proxyParsed, err := url.Parse(proxyURL)
		if err == nil {
			client.Transport = &http.Transport{Proxy: http.ProxyURL(proxyParsed)}
		}
	}
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "poly_uma/1.0")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if err != nil {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		return ""
	}
	for _, key := range []string{"conditionId", "condition_id"} {
		if v, ok := m[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}
