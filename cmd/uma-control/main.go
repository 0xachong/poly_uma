package main

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type nodeConfig struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	ServerKey string `json:"server_key"`
}

type nodeView struct {
	nodeConfig
	Healthy       bool           `json:"healthy"`
	Health        map[string]any `json:"health,omitempty"`
	HealthError   string         `json:"health_error,omitempty"`
	HAProxyStatus string         `json:"haproxy_status"`
	Weight        int            `json:"weight"`
	Connections   int            `json:"connections"`
	CheckedAtMS   int64          `json:"checked_at_ms"`
}

type auditEntry struct {
	AtMS      int64  `json:"at_ms"`
	Remote    string `json:"remote"`
	NodeID    string `json:"node_id"`
	Action    string `json:"action"`
	Value     int    `json:"value,omitempty"`
	Succeeded bool   `json:"succeeded"`
	Error     string `json:"error,omitempty"`
}

type controller struct {
	nodes       []nodeConfig
	socketPath  string
	backendName string
	user        string
	password    string
	httpClient  *http.Client
	auditPath   string
	auditMu     sync.Mutex
	startedAt   time.Time
}

func main() {
	nodes, err := parseNodes(os.Getenv("CONTROL_NODES"))
	if err != nil {
		log.Fatalf("[ERROR] CONTROL_NODES: %v", err)
	}
	control := &controller{
		nodes:       nodes,
		socketPath:  envOr("HAPROXY_RUNTIME_SOCKET", "/run/haproxy/admin.sock"),
		backendName: envOr("HAPROXY_BACKEND", "uma_slaves"),
		user:        envOr("CONTROL_USER", "admin"),
		password:    strings.TrimSpace(os.Getenv("CONTROL_PASSWORD")),
		httpClient:  &http.Client{Timeout: 3 * time.Second},
		auditPath:   envOr("CONTROL_AUDIT_FILE", "/opt/uma-control/audit.jsonl"),
		startedAt:   time.Now(),
	}
	if control.password == "" {
		log.Fatal("[ERROR] CONTROL_PASSWORD is required")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", control.serveDashboard)
	mux.HandleFunc("/api/status", control.serveStatus)
	mux.HandleFunc("/api/nodes/", control.serveNodeAction)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	handler := control.basicAuth(mux)
	server := &http.Server{
		Addr:              envOr("CONTROL_LISTEN_ADDR", "0.0.0.0:8080"),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("[INFO] UMA control listening=%s nodes=%d", server.Addr, len(nodes))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func parseNodes(raw string) ([]nodeConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("empty node list")
	}
	var nodes []nodeConfig
	seen := make(map[string]bool)
	for _, item := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(item), "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid node %q, want id=host:port", item)
		}
		id := strings.TrimSpace(parts[0])
		address := strings.TrimSpace(parts[1])
		if id == "" || address == "" || seen[id] {
			return nil, fmt.Errorf("invalid or duplicate node %q", item)
		}
		seen[id] = true
		nodes = append(nodes, nodeConfig{ID: id, Address: address, ServerKey: id})
	}
	return nodes, nil
}

func (c *controller) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		userOK := subtle.ConstantTimeCompare([]byte(user), []byte(c.user)) == 1
		passwordOK := subtle.ConstantTimeCompare([]byte(password), []byte(c.password)) == 1
		if !ok || !userOK || !passwordOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="UMA Cluster Control"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (c *controller) serveDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, dashboardHTML)
}

func (c *controller) serveStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	haproxyStats, haproxyError := c.readHAProxyStats()
	views := make([]nodeView, len(c.nodes))
	var wg sync.WaitGroup
	for i, node := range c.nodes {
		i, node := i, node
		wg.Add(1)
		go func() {
			defer wg.Done()
			view := nodeView{nodeConfig: node, CheckedAtMS: time.Now().UnixMilli()}
			if stat, ok := haproxyStats[node.ServerKey]; ok {
				view.HAProxyStatus = stat.Status
				view.Weight = stat.Weight
				view.Connections = stat.CurrentSessions
			} else {
				view.HAProxyStatus = "UNKNOWN"
			}
			response, err := c.httpClient.Get("http://" + node.Address + "/slave/healthz")
			if err != nil {
				view.HealthError = err.Error()
				views[i] = view
				return
			}
			defer response.Body.Close()
			var health map[string]any
			if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&health); err != nil {
				view.HealthError = err.Error()
			} else {
				view.Health = health
				view.Healthy = response.StatusCode == http.StatusOK
			}
			views[i] = view
		}()
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":          "ok",
		"nodes":           views,
		"haproxy_error":   errorString(haproxyError),
		"uptime_seconds":  int64(time.Since(c.startedAt).Seconds()),
		"generated_at_ms": time.Now().UnixMilli(),
	})
}

func (c *controller) serveNodeAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/nodes/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "action" {
		http.NotFound(w, r)
		return
	}
	node, ok := c.findNode(parts[0])
	if !ok {
		http.Error(w, "unknown node", http.StatusNotFound)
		return
	}
	var request struct {
		Action string `json:"action"`
		Value  int    `json:"value"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&request); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	command, err := c.actionCommand(node, request.Action, request.Value)
	entry := auditEntry{
		AtMS: time.Now().UnixMilli(), Remote: r.RemoteAddr, NodeID: node.ID,
		Action: request.Action, Value: request.Value,
	}
	if err == nil {
		_, err = c.haproxyCommand(command)
	}
	entry.Succeeded = err == nil
	entry.Error = errorString(err)
	c.appendAudit(entry)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node_id": node.ID, "action": request.Action})
}

func (c *controller) findNode(id string) (nodeConfig, bool) {
	for _, node := range c.nodes {
		if node.ID == id {
			return node, true
		}
	}
	return nodeConfig{}, false
}

func (c *controller) actionCommand(node nodeConfig, action string, value int) (string, error) {
	target := c.backendName + "/" + node.ServerKey
	switch action {
	case "drain":
		return "set server " + target + " state drain", nil
	case "ready":
		return "set server " + target + " state ready", nil
	case "maintenance":
		return "set server " + target + " state maint", nil
	case "weight":
		if value < 0 || value > 100 {
			return "", errors.New("weight must be between 0 and 100")
		}
		return fmt.Sprintf("set server %s weight %d%%", target, value), nil
	default:
		return "", errors.New("action must be drain, ready, maintenance, or weight")
	}
}

type proxyStat struct {
	Status          string
	Weight          int
	CurrentSessions int
}

func (c *controller) readHAProxyStats() (map[string]proxyStat, error) {
	output, err := c.haproxyCommand("show stat")
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(strings.NewReader(output))
	rows, err := reader.ReadAll()
	if err != nil || len(rows) < 2 {
		return nil, fmt.Errorf("parse HAProxy stats: %w", err)
	}
	header := make(map[string]int)
	for index, name := range rows[0] {
		header[strings.TrimPrefix(name, "# ")] = index
	}
	result := make(map[string]proxyStat)
	for _, row := range rows[1:] {
		if field(row, header["pxname"]) != c.backendName {
			continue
		}
		server := field(row, header["svname"])
		if server == "BACKEND" || server == "" {
			continue
		}
		result[server] = proxyStat{
			Status:          field(row, header["status"]),
			Weight:          atoi(field(row, header["weight"])),
			CurrentSessions: atoi(field(row, header["scur"])),
		}
	}
	return result, nil
}

func (c *controller) haproxyCommand(command string) (string, error) {
	connection, err := net.DialTimeout("unix", c.socketPath, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(connection, command+"\n"); err != nil {
		return "", err
	}
	var output strings.Builder
	scanner := bufio.NewScanner(connection)
	for scanner.Scan() {
		output.WriteString(scanner.Text())
		output.WriteByte('\n')
	}
	return output.String(), scanner.Err()
}

func (c *controller) appendAudit(entry auditEntry) {
	c.auditMu.Lock()
	defer c.auditMu.Unlock()
	file, err := os.OpenFile(c.auditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Printf("[WARN] audit write: %v", err)
		return
	}
	defer file.Close()
	_ = json.NewEncoder(file).Encode(entry)
}

func field(row []string, index int) string {
	if index < 0 || index >= len(row) {
		return ""
	}
	return row[index]
}

func atoi(value string) int {
	number, _ := strconv.Atoi(value)
	return number
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
