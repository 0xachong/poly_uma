package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/polymas/poly_uma/internal/mappingdiag"
)

type envelope struct {
	MessageType string              `json:"message_type"`
	Events      []mappingdiag.Event `json:"events"`
}

func main() {
	master := flag.String("master", env("MAPPING_DIAG_MASTER_WS", "ws://127.0.0.1:8011/uma/v2/ws/events?batch=true"), "Master unified WebSocket URL")
	output := flag.String("output", env("MAPPING_DIAG_OUTPUT", "data/mapping-diagnostics.jsonl"), "append-only JSONL evidence file")
	httpAddr := flag.String("http", env("MAPPING_DIAG_HTTP_ADDR", "127.0.0.1:8021"), "diagnostic stats listen address")
	proxy := flag.String("proxy", env("HTTP_PROXY", ""), "optional Gamma HTTP proxy")
	flag.Parse()

	recorder, err := mappingdiag.NewRecorder(*output)
	if err != nil {
		log.Fatalf("open evidence file: %v", err)
	}
	defer recorder.Close()
	monitor := mappingdiag.NewMonitor()
	server := &http.Server{Addr: *httpAddr, Handler: monitor, ReadHeaderTimeout: 3 * time.Second}
	go func() {
		log.Printf("[INFO] diagnostic stats listening on %s", *httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[ERROR] stats server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go run(ctx, *master, mappingdiag.Checker{ProxyURL: *proxy}, recorder, monitor)
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
}

func run(ctx context.Context, master string, checker mappingdiag.GammaChecker, recorder *mappingdiag.Recorder, monitor *mappingdiag.Monitor) {
	jobs := make(chan mappingdiag.Event, 512)
	var inflight sync.Map
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-jobs:
					investigate(ctx, checker, recorder, monitor, event)
					inflight.Delete(eventIdentity(event))
				}
			}
		}()
	}
	backoff := time.Second
	for ctx.Err() == nil {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, master, nil)
		if err != nil {
			log.Printf("[WARN] Master WSS connect failed: %v", err)
			wait(ctx, backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		monitor.Connected(true)
		log.Printf("[INFO] subscribed to Master %s", master)
		for ctx.Err() == nil {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				break
			}
			for _, event := range decodeEvents(payload) {
				miss := event.MappingWasMissing()
				monitor.Event(miss)
				if miss {
					identity := eventIdentity(event)
					if _, exists := inflight.LoadOrStore(identity, struct{}{}); exists {
						continue
					}
					select {
					case jobs <- event:
					default:
						inflight.Delete(identity)
						monitor.Dropped()
						log.Printf("[WARN] diagnostic queue full: event=%s", identity)
					}
				}
			}
		}
		monitor.Connected(false)
		_ = conn.Close()
	}
}

func eventIdentity(event mappingdiag.Event) string {
	return event.TransactionHash + ":" + fmt.Sprintf("%d", event.LogIndex)
}

func decodeEvents(payload []byte) []mappingdiag.Event {
	var batch envelope
	if json.Unmarshal(payload, &batch) == nil && batch.MessageType == "uma_event_batch" {
		return batch.Events
	}
	var event mappingdiag.Event
	if json.Unmarshal(payload, &event) == nil && event.EventType != "" {
		return []mappingdiag.Event{event}
	}
	return nil
}

func investigate(ctx context.Context, checker mappingdiag.GammaChecker, recorder *mappingdiag.Recorder, monitor *mappingdiag.Monitor, event mappingdiag.Event) {
	observed := time.Now()
	for attempt, delay := range []time.Duration{0, 2 * time.Second, 10 * time.Second, 30 * time.Second} {
		if !wait(ctx, delay) {
			return
		}
		checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		result := mappingdiag.Diagnose(checkCtx, checker, event, observed, attempt+1)
		cancel()
		if err := recorder.Append(result); err != nil {
			log.Printf("[ERROR] record diagnosis: %v", err)
		}
		monitor.Result(result.Classification)
		log.Printf("[DIAG] key=%s market=%s attempt=%d cause=%s exact=%s reverse=%s elapsed_ms=%d",
			result.ProcessingKey, result.MarketID, result.Attempt, result.Classification,
			compact(result.ExactHTTPState), compact(result.ReverseHTTPState), result.CheckElapsedMS)
		if result.Classification == "mapping_conflict" || result.Classification == "condition_points_to_other_market" {
			return
		}
	}
}

func wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func compact(value string) string {
	value = strings.ReplaceAll(value, " ", "_")
	if len(value) > 80 {
		return value[:80]
	}
	return value
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
