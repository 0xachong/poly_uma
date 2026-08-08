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
	wirev5 "github.com/polymas/poly_uma/internal/wire/v5"
	"google.golang.org/protobuf/proto"
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
	collectorID := flag.String("collector-id", "", "stable latency collector identity")
	flag.Parse()
	if *target == "" || *connections <= 0 || *ramp <= 0 || *retryMin <= 0 || *retryMax < *retryMin {
		flag.Usage()
		os.Exit(2)
	}

	var dialAttempts, opened, alive, disconnects, failed, messages, bytesIn atomic.Int64
	var reconnectAttempts, reconnectSuccesses, maxRecoveryMS atomic.Int64
	stop := make(chan struct{})
	reporter := newLatencyReporter(*reportURL, strings.TrimSpace(os.Getenv("UMA_LATENCY_REPORT_TOKEN")), *reportInterval, strings.TrimSpace(*collectorID))
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
					messageType, payload, err := conn.ReadMessage()
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
					reporter.observe(messageType, payload, time.Now().UnixMicro())
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
	url                string
	token              string
	interval           time.Duration
	collectorID        string
	client             *http.Client
	mu                 sync.Mutex
	samples            map[string]map[string][]int64
	framesReceived     atomic.Int64
	framesDecoded      atomic.Int64
	eventsReceived     atomic.Int64
	realtimeEvents     atomic.Int64
	decodeErrors       atomic.Int64
	unsupportedSchema  atomic.Int64
	missingTimestamp   atomic.Int64
	nonRealtimeSkipped atomic.Int64
	reportsSucceeded   atomic.Int64
	reportsFailed      atomic.Int64
	lastEventAtMS      atomic.Int64
	lastReportAtMS     atomic.Int64
	observedSchema     atomic.Int64
	observedEncoding   atomic.Int64
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
	GeneratedAtMS      int64                                  `json:"generated_at_ms"`
	IntervalMS         int64                                  `json:"interval_ms"`
	CollectorID        string                                 `json:"collector_id,omitempty"`
	CollectorStatus    string                                 `json:"collector_status,omitempty"`
	Encoding           string                                 `json:"encoding,omitempty"`
	SchemaVersion      int                                    `json:"schema_version,omitempty"`
	SampleCount        int                                    `json:"sample_count"`
	FramesReceived     int64                                  `json:"frames_received"`
	FramesDecoded      int64                                  `json:"frames_decoded"`
	EventsReceived     int64                                  `json:"events_received"`
	RealtimeEvents     int64                                  `json:"realtime_events"`
	DecodeErrors       int64                                  `json:"decode_errors"`
	UnsupportedSchema  int64                                  `json:"unsupported_schema"`
	MissingTimestamp   int64                                  `json:"missing_timestamp"`
	NonRealtimeSkipped int64                                  `json:"non_realtime_skipped"`
	ReportsSucceeded   int64                                  `json:"reports_succeeded"`
	ReportsFailed      int64                                  `json:"reports_failed"`
	LastEventAtMS      int64                                  `json:"last_event_at_ms,omitempty"`
	LastReportAtMS     int64                                  `json:"last_report_at_ms,omitempty"`
	Overall            map[string]latencyQuantiles            `json:"overall"`
	Nodes              map[string]map[string]latencyQuantiles `json:"nodes"`
}

func newLatencyReporter(url, token string, interval time.Duration, collectorID string) *latencyReporter {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	return &latencyReporter{
		url: url, token: token, interval: interval, collectorID: collectorID,
		client:  &http.Client{Timeout: 3 * time.Second},
		samples: make(map[string]map[string][]int64),
	}
}

func (r *latencyReporter) observe(messageType int, payload []byte, clientReceivedUS int64) {
	if r.url == "" {
		return
	}
	r.framesReceived.Add(1)
	if messageType == websocket.BinaryMessage {
		r.observedEncoding.Store(2)
		r.observeProtobuf(payload, clientReceivedUS)
		return
	}
	if messageType != websocket.TextMessage {
		r.unsupportedSchema.Add(1)
		return
	}
	r.observedEncoding.Store(1)
	var envelope struct {
		NodeID           string            `json:"slave_node_id"`
		SchemaVersion    int               `json:"schema_version"`
		PayloadFormat    string            `json:"payload_format"`
		SlaveReceivedMS  int64             `json:"slave_received_at_ms"`
		SlaveBroadcastMS int64             `json:"slave_broadcast_at_ms"`
		SlaveReceivedUS  int64             `json:"slave_received_at_us"`
		SlaveBroadcastUS int64             `json:"slave_broadcast_at_us"`
		Events           []json.RawMessage `json:"events"`
	}
	if json.Unmarshal(payload, &envelope) != nil {
		r.decodeErrors.Add(1)
		return
	}
	if (envelope.SchemaVersion == 4 || envelope.SchemaVersion == 5) && envelope.PayloadFormat == "compact_trade" {
		r.observedSchema.Store(int64(envelope.SchemaVersion))
		r.framesDecoded.Add(1)
		for _, raw := range envelope.Events {
			r.observeCompactTrade(raw, envelope, clientReceivedUS)
		}
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
		r.unsupportedSchema.Add(1)
		return
	}
	r.framesDecoded.Add(1)
	r.eventsReceived.Add(1)
	r.realtimeEvents.Add(1)
	r.lastEventAtMS.Store(clientReceivedUS / 1000)
	chainToMaster := event.UpstreamReceivedMS - event.Timestamp*1000
	masterProcessing := event.MasterBroadcastMS - event.UpstreamReceivedMS
	masterToSlave := event.SlaveReceivedMS - event.MasterBroadcastMS
	slaveProcessing := event.SlaveBroadcastMS - event.SlaveReceivedMS
	slaveToClient := clientReceivedUS/1000 - event.SlaveBroadcastMS
	r.record(event.NodeID, map[string]int64{
		"chain_to_master":   chainToMaster,
		"master_processing": masterProcessing,
		"master_to_slave":   masterToSlave,
		"slave_processing":  slaveProcessing,
		"slave_to_client":   slaveToClient,
		"end_to_end": nonNegative(chainToMaster) + nonNegative(masterProcessing) +
			nonNegative(masterToSlave) + nonNegative(slaveProcessing) + nonNegative(slaveToClient),
	})
}

func (r *latencyReporter) observeCompactTrade(raw json.RawMessage, envelope struct {
	NodeID           string            `json:"slave_node_id"`
	SchemaVersion    int               `json:"schema_version"`
	PayloadFormat    string            `json:"payload_format"`
	SlaveReceivedMS  int64             `json:"slave_received_at_ms"`
	SlaveBroadcastMS int64             `json:"slave_broadcast_at_ms"`
	SlaveReceivedUS  int64             `json:"slave_received_at_us"`
	SlaveBroadcastUS int64             `json:"slave_broadcast_at_us"`
	Events           []json.RawMessage `json:"events"`
}, clientReceivedUS int64) {
	var event struct {
		Source            string `json:"source"`
		ChainTimestampMS  int64  `json:"chain_timestamp_ms"`
		MasterReceivedUS  int64  `json:"master_received_at_us"`
		MasterBroadcastUS int64  `json:"master_broadcast_at_us"`
	}
	r.eventsReceived.Add(1)
	if json.Unmarshal(raw, &event) != nil {
		r.decodeErrors.Add(1)
		return
	}
	if event.Source != "realtime" {
		r.nonRealtimeSkipped.Add(1)
		return
	}
	if envelope.NodeID == "" || event.ChainTimestampMS <= 0 || event.MasterReceivedUS <= 0 || event.MasterBroadcastUS <= 0 {
		r.missingTimestamp.Add(1)
		return
	}
	slaveReceivedUS := envelope.SlaveReceivedUS
	if slaveReceivedUS <= 0 {
		slaveReceivedUS = envelope.SlaveReceivedMS * 1000
	}
	slaveBroadcastUS := envelope.SlaveBroadcastUS
	if slaveBroadcastUS <= 0 {
		slaveBroadcastUS = envelope.SlaveBroadcastMS * 1000
	}
	if slaveReceivedUS <= 0 || slaveBroadcastUS <= 0 {
		r.missingTimestamp.Add(1)
		return
	}
	r.realtimeEvents.Add(1)
	r.lastEventAtMS.Store(clientReceivedUS / 1000)
	chainToMaster := event.MasterReceivedUS/1000 - event.ChainTimestampMS
	masterProcessing := (event.MasterBroadcastUS - event.MasterReceivedUS) / 1000
	masterToSlave := (slaveReceivedUS - event.MasterBroadcastUS) / 1000
	slaveProcessing := (slaveBroadcastUS - slaveReceivedUS) / 1000
	slaveToClient := (clientReceivedUS - slaveBroadcastUS) / 1000
	r.record(envelope.NodeID, map[string]int64{
		"chain_to_master":   chainToMaster,
		"master_processing": masterProcessing,
		"master_to_slave":   masterToSlave,
		"slave_processing":  slaveProcessing,
		"slave_to_client":   slaveToClient,
		"end_to_end": nonNegative(chainToMaster) + nonNegative(masterProcessing) +
			nonNegative(masterToSlave) + nonNegative(slaveProcessing) + nonNegative(slaveToClient),
	})
}

func (r *latencyReporter) observeProtobuf(payload []byte, clientReceivedUS int64) {
	var batch wirev5.CompactTradeBatch
	if err := proto.Unmarshal(payload, &batch); err != nil {
		r.decodeErrors.Add(1)
		return
	}
	if batch.GetSchemaVersion() != 5 || batch.GetPayloadFormat() != "compact_trade" {
		r.unsupportedSchema.Add(1)
		return
	}
	r.observedSchema.Store(5)
	r.framesDecoded.Add(1)
	nodeID := r.collectorID
	if nodeID == "" {
		nodeID = "external-pb"
	}
	for _, event := range batch.GetEvents() {
		r.eventsReceived.Add(1)
		if event.GetSource() != "realtime" {
			r.nonRealtimeSkipped.Add(1)
			continue
		}
		if event.GetChainTimestampMs() <= 0 || event.GetMasterReceivedAtUs() <= 0 || event.GetMasterBroadcastAtUs() <= 0 || batch.GetSlaveReceivedAtUs() <= 0 || batch.GetSlaveBroadcastAtUs() <= 0 {
			r.missingTimestamp.Add(1)
			continue
		}
		r.realtimeEvents.Add(1)
		r.lastEventAtMS.Store(clientReceivedUS / 1000)
		chainToMaster := event.GetMasterReceivedAtUs()/1000 - event.GetChainTimestampMs()
		masterProcessing := (event.GetMasterBroadcastAtUs() - event.GetMasterReceivedAtUs()) / 1000
		masterToSlave := (batch.GetSlaveReceivedAtUs() - event.GetMasterBroadcastAtUs()) / 1000
		slaveProcessing := (batch.GetSlaveBroadcastAtUs() - batch.GetSlaveReceivedAtUs()) / 1000
		slaveToClient := (clientReceivedUS - batch.GetSlaveBroadcastAtUs()) / 1000
		r.record(nodeID, map[string]int64{
			"chain_to_master": chainToMaster, "master_processing": masterProcessing,
			"master_to_slave": masterToSlave, "slave_processing": slaveProcessing,
			"slave_to_client": slaveToClient,
			"end_to_end":      nonNegative(chainToMaster) + nonNegative(masterProcessing) + nonNegative(masterToSlave) + nonNegative(slaveProcessing) + nonNegative(slaveToClient),
		})
	}
}

func (r *latencyReporter) record(nodeID string, values map[string]int64) {
	r.mu.Lock()
	if r.samples[nodeID] == nil {
		r.samples[nodeID] = make(map[string][]int64)
	}
	for stage, value := range values {
		r.samples[nodeID][stage] = append(r.samples[nodeID][stage], value)
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
	report := latencyWindow{
		GeneratedAtMS: time.Now().UnixMilli(),
		IntervalMS:    r.interval.Milliseconds(),
		CollectorID:   r.collectorID, CollectorStatus: "connected",
		FramesReceived: r.framesReceived.Load(), FramesDecoded: r.framesDecoded.Load(),
		EventsReceived: r.eventsReceived.Load(), RealtimeEvents: r.realtimeEvents.Load(),
		DecodeErrors: r.decodeErrors.Load(), UnsupportedSchema: r.unsupportedSchema.Load(),
		MissingTimestamp: r.missingTimestamp.Load(), NonRealtimeSkipped: r.nonRealtimeSkipped.Load(),
		ReportsSucceeded: r.reportsSucceeded.Load(), ReportsFailed: r.reportsFailed.Load(),
		LastEventAtMS: r.lastEventAtMS.Load(), LastReportAtMS: r.lastReportAtMS.Load(),
		SchemaVersion: int(r.observedSchema.Load()),
		Overall:       make(map[string]latencyQuantiles),
		Nodes:         make(map[string]map[string]latencyQuantiles),
	}
	if r.observedEncoding.Load() == 2 {
		report.Encoding = "protobuf"
	} else if r.observedEncoding.Load() == 1 {
		report.Encoding = "json"
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
			if stage == "end_to_end" {
				report.SampleCount += len(values)
			}
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
		r.reportsFailed.Add(1)
		return
	}
	_ = response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		r.reportsFailed.Add(1)
		return
	}
	r.reportsSucceeded.Add(1)
	r.lastReportAtMS.Store(time.Now().UnixMilli())
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
