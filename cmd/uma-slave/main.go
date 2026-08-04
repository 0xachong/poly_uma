// Command uma-slave exposes the master's HTTP API through a reverse proxy and
// relays each supported WebSocket stream through one shared upstream connection.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	proposedPath      = "/uma/v1/ws/proposed"
	disputedPath      = "/uma/v1/ws/disputed"
	eventsPath        = "/uma/v1/ws/events"
	compactEventsPath = "/uma/v2/ws/events"
)

var version = "dev"

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

type subscriber struct {
	id          uint64
	send        chan []byte
	closeCode   int
	closeReason string
	clientIP    string
	clientPort  string
	stream      string
	connectedAt time.Time
	sportsTypes *sportsTypeFilter
}

type relayHub struct {
	path          string
	rawQuery      string
	nodeID        string
	masterURL     *url.URL
	queueSize     int
	mu            sync.RWMutex
	subscribers   map[*subscriber]struct{}
	upstreamUp    atomic.Bool
	reconnects    atomic.Uint64
	received      atomic.Uint64
	broadcasts    atomic.Uint64
	slowDropped   atomic.Uint64
	lastReceiveMS atomic.Int64
}

func newRelayHub(masterURL *url.URL, path, rawQuery string, queueSize int) *relayHub {
	return &relayHub{
		path:        path,
		rawQuery:    rawQuery,
		masterURL:   masterURL,
		queueSize:   queueSize,
		subscribers: make(map[*subscriber]struct{}),
	}
}

func (h *relayHub) subscribe(id uint64, stream, clientIP, clientPort string, connectedAt time.Time, sportsTypes *sportsTypeFilter) *subscriber {
	sub := &subscriber{
		id:          id,
		send:        make(chan []byte, h.queueSize),
		closeCode:   websocket.CloseTryAgainLater,
		closeReason: "slow client",
		clientIP:    clientIP,
		clientPort:  clientPort,
		stream:      stream,
		connectedAt: connectedAt,
		sportsTypes: sportsTypes,
	}
	h.mu.Lock()
	h.subscribers[sub] = struct{}{}
	h.mu.Unlock()
	return sub
}

type downstreamClient struct {
	ID               uint64 `json:"id"`
	IP               string `json:"ip"`
	Port             string `json:"port,omitempty"`
	Stream           string `json:"stream"`
	ConnectedAtMS    int64  `json:"connected_at_ms"`
	ConnectedSeconds int64  `json:"connected_seconds"`
}

func (h *relayHub) clients() []downstreamClient {
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := time.Now()
	clients := make([]downstreamClient, 0, len(h.subscribers))
	for sub := range h.subscribers {
		clients = append(clients, downstreamClient{
			ID: sub.id, IP: sub.clientIP, Port: sub.clientPort, Stream: sub.stream,
			ConnectedAtMS:    sub.connectedAt.UnixMilli(),
			ConnectedSeconds: int64(now.Sub(sub.connectedAt).Seconds()),
		})
	}
	return clients
}

func (h *relayHub) release(count int) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	released := 0
	for sub := range h.subscribers {
		sub.closeCode = websocket.CloseServiceRestart
		sub.closeReason = "cluster rebalance"
		delete(h.subscribers, sub)
		close(sub.send)
		released++
		if released >= count {
			break
		}
	}
	return released
}

func (h *relayHub) unsubscribe(sub *subscriber) {
	h.mu.Lock()
	if _, ok := h.subscribers[sub]; ok {
		delete(h.subscribers, sub)
		close(sub.send)
	}
	h.mu.Unlock()
}

func (h *relayHub) subscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

// broadcast never waits for a downstream client. A client whose private queue
// fills is disconnected so it cannot increase latency for healthy clients.
func (h *relayHub) broadcast(payload []byte) {
	h.mu.Lock()
	type filteredResult struct {
		payload []byte
		err     error
	}
	filtered := make(map[string]filteredResult)
	for sub := range h.subscribers {
		delivery := payload
		if sub.sportsTypes != nil {
			result, ok := filtered[sub.sportsTypes.key]
			if !ok {
				result.payload, result.err = filterCompactSportsBatch(payload, sub.sportsTypes)
				filtered[sub.sportsTypes.key] = result
			}
			if result.err != nil || len(result.payload) == 0 {
				continue
			}
			delivery = result.payload
		}
		select {
		case sub.send <- delivery:
			h.broadcasts.Add(1)
		default:
			delete(h.subscribers, sub)
			sub.closeCode = websocket.CloseTryAgainLater
			sub.closeReason = "slow client"
			close(sub.send)
			h.slowDropped.Add(1)
		}
	}
	h.mu.Unlock()
}

func (h *relayHub) run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := h.consume(ctx)
		h.upstreamUp.Store(false)
		if ctx.Err() != nil {
			return
		}
		h.reconnects.Add(1)
		log.Printf("[WARN] upstream WS disconnected: path=%s err=%v retry=%s", h.path, err, backoff)
		jitter := time.Duration(rand.Int63n(int64(backoff / 4)))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
}

func (h *relayHub) consume(ctx context.Context) error {
	target := *h.masterURL
	switch target.Scheme {
	case "https":
		target.Scheme = "wss"
	default:
		target.Scheme = "ws"
	}
	target.Path = h.path
	target.RawQuery = h.rawQuery

	dialer := websocket.Dialer{
		HandshakeTimeout:  5 * time.Second,
		EnableCompression: true,
		NetDialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	conn, response, err := dialer.DialContext(ctx, target.String(), nil)
	if err != nil {
		if response != nil {
			return errors.New(response.Status)
		}
		return err
	}
	defer conn.Close()
	stopCloser := make(chan struct{})
	defer close(stopCloser)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopCloser:
		}
	}()
	h.upstreamUp.Store(true)
	log.Printf("[INFO] upstream WS connected: path=%s target=%s", h.path, target.Redacted())

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		receivedAt := time.Now().UnixMilli()
		h.received.Add(1)
		h.lastReceiveMS.Store(receivedAt)
		h.broadcast(addRelayMetadata(payload, receivedAt, h.nodeID))
	}
}

func addRelayTimestamps(payload []byte, receivedAt int64) []byte {
	return addRelayMetadata(payload, receivedAt, "")
}

func addRelayMetadata(payload []byte, receivedAt int64, nodeID string) []byte {
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		return payload
	}
	message["slave_received_at_ms"] = receivedAt
	message["slave_broadcast_at_ms"] = time.Now().UnixMilli()
	if nodeID != "" {
		message["slave_node_id"] = nodeID
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		return payload
	}
	return encoded
}

type slaveServer struct {
	proxy      *httputil.ReverseProxy
	queryCache *httpQueryCache
	hubs       map[string]*relayHub
	nodeID     string
	startedAt  time.Time
	adminToken string
	nextClient atomic.Uint64
}

func newSlaveServer(masterURL *url.URL, queueSize int) *slaveServer {
	server := &slaveServer{
		proxy: newProxy(masterURL),
		queryCache: newHTTPQueryCache(
			masterURL,
			envInt("SLAVE_HTTP_CACHE_SIZE", 512),
			envDuration("SLAVE_HTTP_CACHE_TTL", 500*time.Millisecond),
			envDuration("SLAVE_HTTP_CACHE_STALE_TTL", 2*time.Second),
		),
		hubs: map[string]*relayHub{
			proposedPath:      newRelayHub(masterURL, proposedPath, "", queueSize),
			disputedPath:      newRelayHub(masterURL, disputedPath, "", queueSize),
			compactEventsPath: newRelayHub(masterURL, compactEventsPath, "batch=true&format=compact", queueSize),
		},
		nodeID:     envOr("SLAVE_NODE_ID", hostname()),
		startedAt:  time.Now(),
		adminToken: strings.TrimSpace(os.Getenv("SLAVE_ADMIN_TOKEN")),
	}
	for _, hub := range server.hubs {
		hub.nodeID = server.nodeID
	}
	return server
}

func (s *slaveServer) run(ctx context.Context) {
	for _, hub := range s.hubs {
		go hub.run(ctx)
	}
}

func (s *slaveServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hub := s.relayHub(r); hub != nil {
		s.serveWebSocket(hub, w, r)
		return
	}
	if r.URL.Path == eventsPath {
		s.serveCombinedWebSocket(w, r)
		return
	}
	if r.URL.Path == "/slave/healthz" {
		s.serveHealth(w)
		return
	}
	if r.URL.Path == "/slave/admin/rebalance" {
		s.serveRebalance(w, r)
		return
	}
	if r.URL.Path == "/slave/admin/clients" {
		s.serveClients(w, r)
		return
	}
	if r.Method == http.MethodGet && isCachedQueryPath(r.URL.Path) {
		s.queryCache.ServeHTTP(w, r)
		return
	}
	s.proxy.ServeHTTP(w, r)
}

// relayHub selects shared streams without changing any negotiated legacy
// request. Only an explicit v2 compact batch subscription uses the new hub.
// sports_types is an optional per-downstream filter handled locally by Slave.
func (s *slaveServer) relayHub(r *http.Request) *relayHub {
	if r.URL.Path == compactEventsPath {
		batch, err := strconv.ParseBool(strings.TrimSpace(r.URL.Query().Get("batch")))
		if err != nil || !batch || !strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("format")), "compact") {
			return nil
		}
		return s.hubs[r.URL.Path]
	}
	if r.URL.Query().Get("batch") != "" || r.URL.Query().Get("format") != "" || r.URL.Query().Get("sports_types") != "" {
		return nil
	}
	return s.hubs[r.URL.Path]
}

func (s *slaveServer) serveWebSocket(hub *relayHub, w http.ResponseWriter, r *http.Request) {
	sportsTypes, err := parseSportsTypeFilter(r.URL.Query().Get("sports_types"), r.URL.Query().Has("sports_types"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	conn, err := wsUpgrader.Upgrade(w, r, http.Header{
		"X-UMA-Slave":      []string{"true"},
		"X-UMA-Slave-Node": []string{s.nodeID},
	})
	if err != nil {
		return
	}
	defer conn.Close()

	clientIP, clientPort := requestClientAddress(r)
	sub := hub.subscribe(s.nextClient.Add(1), hub.path, clientIP, clientPort, time.Now(), sportsTypes)
	defer hub.unsubscribe(sub)

	clientGone := make(chan struct{})
	go func() {
		defer close(clientGone)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(25 * time.Second)
	defer pingTicker.Stop()
	for {
		select {
		case payload, ok := <-sub.send:
			if !ok {
				_ = conn.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(sub.closeCode, sub.closeReason),
					time.Now().Add(time.Second),
				)
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-pingTicker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-clientGone:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func (s *slaveServer) serveCombinedWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, http.Header{
		"X-UMA-Slave":      []string{"true"},
		"X-UMA-Slave-Node": []string{s.nodeID},
	})
	if err != nil {
		return
	}
	defer conn.Close()

	clientIP, clientPort := requestClientAddress(r)
	id := s.nextClient.Add(1)
	connectedAt := time.Now()
	proposedHub := s.hubs[proposedPath]
	disputedHub := s.hubs[disputedPath]
	proposed := proposedHub.subscribe(id, eventsPath, clientIP, clientPort, connectedAt, nil)
	disputed := disputedHub.subscribe(id, eventsPath, clientIP, clientPort, connectedAt, nil)
	defer proposedHub.unsubscribe(proposed)
	defer disputedHub.unsubscribe(disputed)

	clientGone := make(chan struct{})
	go func() {
		defer close(clientGone)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	writePayload := func(payload []byte, ok bool, sub *subscriber) bool {
		if !ok {
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(sub.closeCode, sub.closeReason),
				time.Now().Add(time.Second),
			)
			return false
		}
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return conn.WriteMessage(websocket.TextMessage, payload) == nil
	}

	pingTicker := time.NewTicker(25 * time.Second)
	defer pingTicker.Stop()
	for {
		// Prefer an already queued proposed event without starving disputed
		// traffic when the proposed queue is empty.
		select {
		case payload, ok := <-proposed.send:
			if !writePayload(payload, ok, proposed) {
				return
			}
			continue
		default:
		}
		select {
		case payload, ok := <-proposed.send:
			if !writePayload(payload, ok, proposed) {
				return
			}
		case payload, ok := <-disputed.send:
			if !writePayload(payload, ok, disputed) {
				return
			}
		case <-pingTicker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-clientGone:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func requestClientAddress(r *http.Request) (string, string) {
	ip, port, _ := net.SplitHostPort(r.RemoteAddr)
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if candidate := strings.TrimSpace(parts[len(parts)-1]); candidate != "" {
			ip = candidate
		}
	}
	if forwardedPort := strings.TrimSpace(r.Header.Get("X-Forwarded-Client-Port")); forwardedPort != "" {
		port = forwardedPort
	}
	return ip, port
}

func (s *slaveServer) serveHealth(w http.ResponseWriter) {
	type streamHealth struct {
		UpstreamConnected bool   `json:"upstream_connected"`
		Subscribers       int    `json:"subscribers"`
		Reconnects        uint64 `json:"reconnects"`
		MessagesReceived  uint64 `json:"messages_received"`
		Deliveries        uint64 `json:"deliveries"`
		SlowClients       uint64 `json:"slow_clients_disconnected"`
		LastReceiveAtMS   int64  `json:"last_receive_at_ms"`
	}
	streams := make(map[string]streamHealth, len(s.hubs))
	healthy := true
	for path, hub := range s.hubs {
		up := hub.upstreamUp.Load()
		healthy = healthy && up
		streams[path] = streamHealth{
			UpstreamConnected: up,
			Subscribers:       hub.subscriberCount(),
			Reconnects:        hub.reconnects.Load(),
			MessagesReceived:  hub.received.Load(),
			Deliveries:        hub.broadcasts.Load(),
			SlowClients:       hub.slowDropped.Load(),
			LastReceiveAtMS:   hub.lastReceiveMS.Load(),
		}
	}
	status := http.StatusOK
	if !healthy {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":         map[bool]string{true: "ok", false: "degraded"}[healthy],
		"version":        version,
		"node_id":        s.nodeID,
		"started_at_ms":  s.startedAt.UnixMilli(),
		"uptime_seconds": int64(time.Since(s.startedAt).Seconds()),
		"streams":        streams,
		"http_cache":     s.queryCache.Stats(),
	})
}

func main() {
	listenAddr := envOr("SLAVE_LISTEN_ADDR", "0.0.0.0:8011")
	masterRawURL := envOr("UMA_MASTER_URL", "http://43.154.60.204:8011")
	masterURL, err := url.Parse(masterRawURL)
	if err != nil || masterURL.Scheme == "" || masterURL.Host == "" {
		log.Fatalf("[ERROR] invalid UMA_MASTER_URL %q", masterRawURL)
	}
	queueSize := envInt("SLAVE_WS_CLIENT_QUEUE", 64)

	handler := newSlaveServer(masterURL, queueSize)
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	handler.run(ctx)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[WARN] shutdown: %v", err)
		}
	}()

	log.Printf("[INFO] UMA slave listening=%s master=%s client_queue=%d", listenAddr, masterURL.Redacted(), queueSize)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[ERROR] serve: %v", err)
	}
}

func newProxy(masterURL *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(masterURL)
	proxy.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[WARN] proxy failed: method=%s path=%s remote=%s err=%v", r.Method, r.URL.Path, r.RemoteAddr, err)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"upstream UMA service unavailable"}`))
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		response.Header.Set("X-UMA-Slave", "true")
		return nil
	}
	return proxy
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func hostname() string {
	value, err := os.Hostname()
	if err != nil || strings.TrimSpace(value) == "" {
		return "unknown"
	}
	return value
}
