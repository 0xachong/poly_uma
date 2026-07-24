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
	proposedPath = "/uma/v1/ws/proposed"
	disputedPath = "/uma/v1/ws/disputed"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

type subscriber struct {
	send chan []byte
}

type relayHub struct {
	path          string
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

func newRelayHub(masterURL *url.URL, path string, queueSize int) *relayHub {
	return &relayHub{
		path:        path,
		masterURL:   masterURL,
		queueSize:   queueSize,
		subscribers: make(map[*subscriber]struct{}),
	}
}

func (h *relayHub) subscribe() *subscriber {
	sub := &subscriber{send: make(chan []byte, h.queueSize)}
	h.mu.Lock()
	h.subscribers[sub] = struct{}{}
	h.mu.Unlock()
	return sub
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
	for sub := range h.subscribers {
		select {
		case sub.send <- payload:
			h.broadcasts.Add(1)
		default:
			delete(h.subscribers, sub)
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
	target.RawQuery = ""

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
		h.broadcast(addRelayTimestamps(payload, receivedAt))
	}
}

func addRelayTimestamps(payload []byte, receivedAt int64) []byte {
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		return payload
	}
	message["slave_received_at_ms"] = receivedAt
	message["slave_broadcast_at_ms"] = time.Now().UnixMilli()
	encoded, err := json.Marshal(message)
	if err != nil {
		return payload
	}
	return encoded
}

type slaveServer struct {
	proxy *httputil.ReverseProxy
	hubs  map[string]*relayHub
}

func newSlaveServer(masterURL *url.URL, queueSize int) *slaveServer {
	return &slaveServer{
		proxy: newProxy(masterURL),
		hubs: map[string]*relayHub{
			proposedPath: newRelayHub(masterURL, proposedPath, queueSize),
			disputedPath: newRelayHub(masterURL, disputedPath, queueSize),
		},
	}
}

func (s *slaveServer) run(ctx context.Context) {
	for _, hub := range s.hubs {
		go hub.run(ctx)
	}
}

func (s *slaveServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hub := s.hubs[r.URL.Path]; hub != nil {
		s.serveWebSocket(hub, w, r)
		return
	}
	if r.URL.Path == "/slave/healthz" {
		s.serveHealth(w)
		return
	}
	s.proxy.ServeHTTP(w, r)
}

func (s *slaveServer) serveWebSocket(hub *relayHub, w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, http.Header{"X-UMA-Slave": []string{"true"}})
	if err != nil {
		return
	}
	defer conn.Close()

	sub := hub.subscribe()
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
					websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "slow client"),
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
		"status":  map[bool]string{true: "ok", false: "degraded"}[healthy],
		"streams": streams,
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
