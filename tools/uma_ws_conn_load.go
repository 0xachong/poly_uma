// Command uma_ws_conn_load opens many read-only WebSocket clients and
// automatically reconnects them when a connection is interrupted.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
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
	flag.Parse()
	if *target == "" || *connections <= 0 || *ramp <= 0 || *retryMin <= 0 || *retryMax < *retryMin {
		flag.Usage()
		os.Exit(2)
	}

	var dialAttempts, opened, alive, disconnects, failed, messages, bytesIn atomic.Int64
	var reconnectAttempts, reconnectSuccesses, maxRecoveryMS atomic.Int64
	stop := make(chan struct{})
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
