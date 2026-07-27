// Command uma_ws_conn_load opens many read-only WebSocket clients and
// automatically reconnects them when a connection is interrupted.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	target := flag.String("url", "", "WebSocket URL")
	connections := flag.Int("connections", 1000, "number of logical clients")
	ramp := flag.Int("ramp", 500, "clients started per second")
	duration := flag.Duration("duration", 20*time.Second, "hold duration after ramp")
	openTimeout := flag.Duration("open-timeout", 5*time.Second, "handshake timeout")
	reconnect := flag.Bool("reconnect", true, "automatically reconnect interrupted clients")
	retryMin := flag.Duration("retry-min", 100*time.Millisecond, "minimum reconnect delay")
	retryMax := flag.Duration("retry-max", 5*time.Second, "maximum reconnect delay")
	reportURL := flag.String("report-url", "", "optional latency report endpoint")
	reportInterval := flag.Duration("report-interval", 3*time.Second, "latency report interval")
	flag.Parse()
	if *target == "" || *connections <= 0 || *ramp <= 0 || *retryMin <= 0 || *retryMax < *retryMin {
		flag.Usage()
		os.Exit(2)
	}

	var dialAttempts, opened, alive, disconnects, failed, messages, bytesIn atomic.Int64
	var reconnectAttempts, reconnectSuccesses, maxRecoveryMS atomic.Int64
	stop := make(chan struct{})
	reporter := newLatencyReporter(*reportURL, strings.TrimSpace(os.Getenv("UMA_LATENCY_REPORT_TOKEN")), *reportInterval)
	go reporter.run(stop)
	var stopOnce sync.Once
	stopAll := func() { stopOnce.Do(func() { close(stop) }) }
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sig:
			stopAll()
		case <-stop:
		}
	}()

	dialer := websocket.Dialer{
		HandshakeTimeout: *openTimeout,
		Proxy:            http.ProxyFromEnvironment,
	}
	started := time.Now()
	interval := time.Second / time.Duration(*ramp)
	var clients sync.WaitGroup

	for id := 0; id < *connections; id++ {
		clients.Add(1)
		go func() {
			defer clients.Done()
			firstDial := true
			backoff := *retryMin
			var disconnectedAt time.Time
			for {
				select {
				case <-stop:
					return
				default:
				}

				if !firstDial {
					if !*reconnect {
						return
					}
					reconnectAttempts.Add(1)
					jitter := time.Duration(rand.Int63n(int64(backoff/2) + 1))
					delay := backoff/2 + jitter
					select {
					case <-time.After(delay):
					case <-stop:
						return
					}
				}

				dialAttempts.Add(1)
				conn, _, err := dialer.Dial(*target, nil)
				if err != nil {
					failed.Add(1)
					firstDial = false
					if disconnectedAt.IsZero() {
						disconnectedAt = time.Now()
					}
					backoff = nextBackoff(backoff, *retryMax)
					continue
				}

				opened.Add(1)
				alive.Add(1)
				if !firstDial {
					reconnectSuccesses.Add(1)
					recovery := time.Since(disconnectedAt).Milliseconds()
					updateMax(&maxRecoveryMS, recovery)
				}
				firstDial = false
				disconnectedAt = time.Time{}
				backoff = *retryMin

				readDone := make(chan struct{})
				go func() {
					select {
					case <-stop:
						_ = conn.Close()
					case <-readDone:
					}
				}()
				for {
					_, payload, err := conn.ReadMessage()
					if err != nil {
						close(readDone)
						alive.Add(-1)
						select {
						case <-stop:
							return
						default:
						}
						disconnects.Add(1)
						disconnectedAt = time.Now()
						break
					}
					messages.Add(1)
					bytesIn.Add(int64(len(payload)))
					reporter.observe(payload, time.Now().UnixMilli())
				}
			}
		}()
		time.Sleep(interval)
	}

	timer := time.NewTimer(*duration)
	ticker := time.NewTicker(time.Second)
	defer timer.Stop()
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ticker.C:
			fmt.Printf("t=%.0fs clients=%d alive=%d attempts=%d opened=%d failed=%d disconnects=%d reconnect_attempts=%d reconnect_success=%d max_recovery_ms=%d messages=%d bytes=%d\n",
				time.Since(started).Seconds(), *connections, alive.Load(), dialAttempts.Load(),
				opened.Load(), failed.Load(), disconnects.Load(), reconnectAttempts.Load(),
				reconnectSuccesses.Load(), maxRecoveryMS.Load(), messages.Load(), bytesIn.Load())
		case <-timer.C:
			stopAll()
			break loop
		case <-stop:
			break loop
		}
	}
	clients.Wait()
	fmt.Printf("SUMMARY clients=%d alive_at_end=%d attempts=%d opened=%d failed=%d disconnects=%d reconnect_attempts=%d reconnect_success=%d max_recovery_ms=%d messages=%d bytes=%d\n",
		*connections, alive.Load(), dialAttempts.Load(), opened.Load(), failed.Load(),
		disconnects.Load(), reconnectAttempts.Load(), reconnectSuccesses.Load(),
		maxRecoveryMS.Load(), messages.Load(), bytesIn.Load())
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func updateMax(target *atomic.Int64, value int64) {
	for {
		old := target.Load()
		if value <= old || target.CompareAndSwap(old, value) {
			return
		}
	}
}

var latencyStages = []string{
	"chain_to_master",
	"master_processing",
	"master_to_slave",
	"slave_processing",
	"slave_to_client",
	"end_to_end",
}

type latencyReporter struct {
	url      string
	token    string
	interval time.Duration
	client   *http.Client
	mu       sync.Mutex
	samples  map[string]map[string][]int64
}

type latencyQuantiles struct {
	Count         int   `json:"count"`
	ClockAdjusted int   `json:"clock_adjusted"`
	P50           int64 `json:"p50_ms"`
	P90           int64 `json:"p90_ms"`
	P95           int64 `json:"p95_ms"`
	P99           int64 `json:"p99_ms"`
	Max           int64 `json:"max_ms"`
}

type latencyWindow struct {
	GeneratedAtMS int64                                  `json:"generated_at_ms"`
	IntervalMS    int64                                  `json:"interval_ms"`
	Overall       map[string]latencyQuantiles            `json:"overall"`
	Nodes         map[string]map[string]latencyQuantiles `json:"nodes"`
}

func newLatencyReporter(url, token string, interval time.Duration) *latencyReporter {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	return &latencyReporter{
		url: url, token: token, interval: interval,
		client:  &http.Client{Timeout: 3 * time.Second},
		samples: make(map[string]map[string][]int64),
	}
}

func (r *latencyReporter) observe(payload []byte, clientReceivedMS int64) {
	if r.url == "" {
		return
	}
	var event struct {
		NodeID             string `json:"slave_node_id"`
		Source             string `json:"source"`
		Timestamp          int64  `json:"timestamp"`
		UpstreamReceivedMS int64  `json:"upstream_received_at_ms"`
		MasterBroadcastMS  int64  `json:"broadcast_at_ms"`
		SlaveReceivedMS    int64  `json:"slave_received_at_ms"`
		SlaveBroadcastMS   int64  `json:"slave_broadcast_at_ms"`
	}
	if json.Unmarshal(payload, &event) != nil || event.NodeID == "" ||
		event.Source != "realtime" || event.Timestamp <= 0 ||
		event.UpstreamReceivedMS <= 0 || event.MasterBroadcastMS <= 0 ||
		event.SlaveReceivedMS <= 0 || event.SlaveBroadcastMS <= 0 {
		return
	}
	chainToMaster := event.UpstreamReceivedMS - event.Timestamp*1000
	masterProcessing := event.MasterBroadcastMS - event.UpstreamReceivedMS
	masterToSlave := event.SlaveReceivedMS - event.MasterBroadcastMS
	slaveProcessing := event.SlaveBroadcastMS - event.SlaveReceivedMS
	slaveToClient := clientReceivedMS - event.SlaveBroadcastMS
	values := map[string]int64{
		"chain_to_master":   chainToMaster,
		"master_processing": masterProcessing,
		"master_to_slave":   masterToSlave,
		"slave_processing":  slaveProcessing,
		"slave_to_client":   slaveToClient,
		"end_to_end": nonNegative(chainToMaster) + nonNegative(masterProcessing) +
			nonNegative(masterToSlave) + nonNegative(slaveProcessing) + nonNegative(slaveToClient),
	}
	r.mu.Lock()
	if r.samples[event.NodeID] == nil {
		r.samples[event.NodeID] = make(map[string][]int64)
	}
	for stage, value := range values {
		r.samples[event.NodeID][stage] = append(r.samples[event.NodeID][stage], value)
	}
	r.mu.Unlock()
}

func (r *latencyReporter) run(stop <-chan struct{}) {
	if r.url == "" {
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.flush()
		case <-stop:
			r.flush()
			return
		}
	}
}

func (r *latencyReporter) flush() {
	r.mu.Lock()
	windowSamples := r.samples
	r.samples = make(map[string]map[string][]int64)
	r.mu.Unlock()
	if len(windowSamples) == 0 {
		return
	}
	report := latencyWindow{
		GeneratedAtMS: time.Now().UnixMilli(),
		IntervalMS:    r.interval.Milliseconds(),
		Overall:       make(map[string]latencyQuantiles),
		Nodes:         make(map[string]map[string]latencyQuantiles),
	}
	overall := make(map[string][]int64)
	for node, stages := range windowSamples {
		report.Nodes[node] = make(map[string]latencyQuantiles)
		for _, stage := range latencyStages {
			values := stages[stage]
			if len(values) == 0 {
				continue
			}
			report.Nodes[node][stage] = quantiles(values)
			overall[stage] = append(overall[stage], values...)
		}
	}
	for _, stage := range latencyStages {
		if len(overall[stage]) > 0 {
			report.Overall[stage] = quantiles(overall[stage])
		}
	}
	body, _ := json.Marshal(report)
	request, err := http.NewRequest(http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+r.token)
	response, err := r.client.Do(request)
	if err != nil {
		return
	}
	_ = response.Body.Close()
}

func quantiles(values []int64) latencyQuantiles {
	adjusted := 0
	for index, value := range values {
		if value < 0 {
			values[index] = 0
			adjusted++
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(percent int) int64 {
		index := (len(values)*percent + 99) / 100
		if index < 1 {
			index = 1
		}
		return values[index-1]
	}
	return latencyQuantiles{
		Count: len(values), ClockAdjusted: adjusted,
		P50: at(50), P90: at(90), P95: at(95),
		P99: at(99), Max: values[len(values)-1],
	}
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
