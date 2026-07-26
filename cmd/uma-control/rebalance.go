package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	rebalanceBatch    = 20
	rebalanceInterval = 2 * time.Second
	rebalanceRounds   = 60
)

func (c *controller) serveRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !c.rebalancing.CompareAndSwap(false, true) {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "rebalance already running"})
		return
	}
	go c.runRebalance()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":   "started",
		"batch":    rebalanceBatch,
		"interval": rebalanceInterval.String(),
	})
}

func (c *controller) runRebalance() {
	defer c.rebalancing.Store(false)
	for round := 1; round <= rebalanceRounds; round++ {
		stats, err := c.readHAProxyStats()
		if err != nil {
			log.Printf("[WARN] rebalance stats: %v", err)
			return
		}
		active := make([]nodeConfig, 0, len(c.nodes))
		total := 0
		minimum, maximum := -1, 0
		for _, node := range c.nodes {
			stat, ok := stats[node.ServerKey]
			if !ok || !strings.HasPrefix(stat.Status, "UP") {
				continue
			}
			active = append(active, node)
			total += stat.CurrentSessions
			if minimum < 0 || stat.CurrentSessions < minimum {
				minimum = stat.CurrentSessions
			}
			if stat.CurrentSessions > maximum {
				maximum = stat.CurrentSessions
			}
		}
		if len(active) < 2 || maximum-minimum <= 1 {
			log.Printf("[INFO] rebalance complete: round=%d active=%d total=%d spread=%d", round, len(active), total, maximum-minimum)
			return
		}
		target := (total + len(active) - 1) / len(active)
		released := 0
		for _, node := range active {
			excess := stats[node.ServerKey].CurrentSessions - target
			if excess <= 0 {
				continue
			}
			count := excess
			if count > rebalanceBatch {
				count = rebalanceBatch
			}
			actual, err := c.releaseConnections(node, count)
			if err != nil {
				log.Printf("[WARN] rebalance release: node=%s count=%d err=%v", node.ID, count, err)
				continue
			}
			released += actual
			c.appendAudit(auditEntry{
				AtMS: time.Now().UnixMilli(), Remote: "controller", NodeID: node.ID,
				Action: "rebalance", Value: actual, Succeeded: true,
			})
		}
		log.Printf("[INFO] rebalance round=%d active=%d total=%d target=%d spread=%d released=%d",
			round, len(active), total, target, maximum-minimum, released)
		if released == 0 {
			return
		}
		time.Sleep(rebalanceInterval)
	}
	log.Printf("[WARN] rebalance stopped after maximum rounds=%d", rebalanceRounds)
}

func (c *controller) releaseConnections(node nodeConfig, count int) (int, error) {
	body, _ := json.Marshal(map[string]int{"count": count})
	request, err := http.NewRequest(http.MethodPost, "http://"+node.Address+"/slave/admin/rebalance", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+c.slaveToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return 0, fmt.Errorf("slave returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	var result struct {
		Released int `json:"released"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result); err != nil {
		return 0, err
	}
	return result.Released, nil
}
