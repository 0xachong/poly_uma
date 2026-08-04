package main

import (
	"container/list"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const maxCachedResponseBytes = 8 << 20

const slaveLLMSSupplement = `

---

## Slave WebSocket 合流接口

Slave 与 Slave 集群入口支持两种实时订阅方式：

- 分流订阅：分别连接 ` + "`/uma/v1/ws/proposed`" + ` 与 ` + "`/uma/v1/ws/disputed`" + `。
- 合流订阅：只连接 ` + "`/uma/v1/ws/events`" + `，同时接收 propose 与 dispute；根据消息中的 ` + "`event_type`" + ` 区分。
- Compact v3：连接 ` + "`/uma/v2/ws/events?batch=true&format=compact`" + `，使用一条共享上游接收 propose 与 dispute batch。
- 体育胜负盘过滤：在 Compact v3 地址增加 ` + "`sports_types=moneyline,child_moneyline`" + `。Sports（tag ID 1）或 Esports（tag ID 64）只保留两类胜负盘；非体育市场正常发送。

JavaScript 示例：

    const ws = new WebSocket("ws://<slave-or-cluster-host>:<port>/uma/v1/ws/events");
    ws.onmessage = ({data}) => {
      const event = JSON.parse(data);
      if (event.event_type === "propose") {
        // proposed
      } else if (event.event_type === "dispute") {
        // disputed
      }
    };

旧合流接口不增加消息外层结构，不改变原字段。每台 Slave 到 Master 保持 proposed、disputed、compact events 各一条共享上游连接，不会按下游连接数放大 Master 连接数。合流事件不保证两种类型之间全局有序；客户端必须按 ` + "`condition_id:event_type`" + ` 做业务幂等，并保留 ` + "`transaction_hash/log_index`" + ` 用于链上审计。
`

type cachedHTTPResponse struct {
	key        string
	statusCode int
	header     http.Header
	body       []byte
	expiresAt  time.Time
	staleAt    time.Time
}

type httpQueryCache struct {
	masterURL *url.URL
	client    *http.Client
	capacity  int
	ttl       time.Duration
	staleTTL  time.Duration
	mu        sync.Mutex
	ll        *list.List
	items     map[string]*list.Element
	fetch     singleflight.Group
	hits      atomic.Uint64
	staleHits atomic.Uint64
	misses    atomic.Uint64
	refreshes atomic.Uint64
	upstream  atomic.Uint64
	evictions atomic.Uint64
}

func newHTTPQueryCache(masterURL *url.URL, capacity int, ttl, staleTTL time.Duration) *httpQueryCache {
	if capacity <= 0 {
		capacity = 512
	}
	if ttl <= 0 {
		ttl = 500 * time.Millisecond
	}
	if staleTTL <= 0 {
		staleTTL = 2 * time.Second
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}
	return &httpQueryCache{
		masterURL: masterURL,
		client:    &http.Client{Transport: transport},
		capacity:  capacity,
		ttl:       ttl,
		staleTTL:  staleTTL,
		ll:        list.New(),
		items:     make(map[string]*list.Element),
	}
}

func isCachedQueryPath(path string) bool {
	switch path {
	case "/uma/v1/proposed", "/uma/v1/disputed", "/uma/v1/settled", "/uma/v1/proposed/latest", "/healthz", "/llms.txt":
		return true
	default:
		return false
	}
}

func (c *httpQueryCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.RequestURI()
	if response, ok := c.get(key); ok {
		c.hits.Add(1)
		writeCachedResponse(w, response, "HIT")
		return
	}
	if response, ok := c.getStale(key); ok {
		c.staleHits.Add(1)
		c.refresh(key, r)
		writeCachedResponse(w, response, "STALE")
		return
	}
	c.misses.Add(1)
	value, err, _ := c.fetch.Do(key, func() (any, error) {
		if response, ok := c.get(key); ok {
			return response, nil
		}
		return c.fetchUpstream(r.Context(), r)
	})
	if err != nil {
		http.Error(w, `{"error":"upstream UMA service unavailable"}`, http.StatusBadGateway)
		return
	}
	response := value.(*cachedHTTPResponse)
	c.set(response)
	writeCachedResponse(w, response, "MISS")
}

func (c *httpQueryCache) refresh(key string, incoming *http.Request) {
	c.refreshes.Add(1)
	targetRequest := incoming.Clone(context.Background())
	_ = c.fetch.DoChan(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		response, err := c.fetchUpstream(ctx, targetRequest)
		if err == nil {
			c.set(response)
		}
		return response, err
	})
}

func (c *httpQueryCache) fetchUpstream(ctx context.Context, incoming *http.Request) (*cachedHTTPResponse, error) {
	c.upstream.Add(1)
	target := *c.masterURL
	target.Path = incoming.URL.Path
	target.RawQuery = incoming.URL.RawQuery
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", incoming.Header.Get("Accept"))
	request.Header.Set("User-Agent", "uma-slave/"+hostname())
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxCachedResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxCachedResponseBytes {
		return nil, &responseTooLargeError{}
	}
	header := response.Header.Clone()
	if incoming.URL.Path == "/llms.txt" && response.StatusCode == http.StatusOK {
		body = append(body, slaveLLMSSupplement...)
		header.Del("Content-Length")
	}
	now := time.Now()
	return &cachedHTTPResponse{
		key:        incoming.URL.RequestURI(),
		statusCode: response.StatusCode,
		header:     header,
		body:       body,
		expiresAt:  now.Add(c.ttl),
		staleAt:    now.Add(c.ttl + c.staleTTL),
	}, nil
}

func (c *httpQueryCache) get(key string) (*cachedHTTPResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return nil, false
	}
	response := element.Value.(*cachedHTTPResponse)
	if time.Now().After(response.expiresAt) {
		return nil, false
	}
	c.ll.MoveToFront(element)
	return response, true
}

func (c *httpQueryCache) getStale(key string) (*cachedHTTPResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return nil, false
	}
	response := element.Value.(*cachedHTTPResponse)
	now := time.Now()
	if now.After(response.staleAt) {
		c.removeElement(element)
		return nil, false
	}
	c.ll.MoveToFront(element)
	return response, true
}

func (c *httpQueryCache) set(response *cachedHTTPResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[response.key]; ok {
		element.Value = response
		c.ll.MoveToFront(element)
		return
	}
	element := c.ll.PushFront(response)
	c.items[response.key] = element
	for c.ll.Len() > c.capacity {
		c.removeElement(c.ll.Back())
		c.evictions.Add(1)
	}
}

func (c *httpQueryCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	c.ll.Remove(element)
	delete(c.items, element.Value.(*cachedHTTPResponse).key)
}

func (c *httpQueryCache) Stats() map[string]any {
	c.mu.Lock()
	entries := c.ll.Len()
	c.mu.Unlock()
	return map[string]any{
		"entries":           entries,
		"capacity":          c.capacity,
		"ttl_ms":            c.ttl.Milliseconds(),
		"stale_ttl_ms":      c.staleTTL.Milliseconds(),
		"hits":              c.hits.Load(),
		"stale_hits":        c.staleHits.Load(),
		"misses":            c.misses.Load(),
		"refreshes":         c.refreshes.Load(),
		"upstream_requests": c.upstream.Load(),
		"evictions":         c.evictions.Load(),
	}
}

func writeCachedResponse(w http.ResponseWriter, response *cachedHTTPResponse, cacheStatus string) {
	for key, values := range response.header {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.Header().Set("X-UMA-Slave", "true")
	w.Header().Set("X-UMA-Slave-Cache", cacheStatus)
	w.WriteHeader(response.statusCode)
	_, _ = w.Write(response.body)
}

func isHopByHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

type responseTooLargeError struct{}

func (*responseTooLargeError) Error() string { return "upstream response exceeds cache limit" }
