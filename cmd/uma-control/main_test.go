package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardDisablesBrowserCache(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/", nil)
	(&controller{}).serveDashboard(recorder, request)
	if got := recorder.Header().Get("Cache-Control"); !strings.Contains(got, "no-store") {
		t.Fatalf("Cache-Control=%q", got)
	}
}

func TestParseNodes(t *testing.T) {
	nodes, err := parseNodes("slave-01=127.0.0.1:8011,slave-02=127.0.0.2:8011")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 || nodes[1].ID != "slave-02" || nodes[1].Address != "127.0.0.2:8011" {
		t.Fatalf("nodes=%+v", nodes)
	}
}

func TestActionCommands(t *testing.T) {
	control := &controller{backendName: "uma_slaves"}
	node := nodeConfig{ID: "slave-01", ServerKey: "slave-01"}
	command, err := control.actionCommand(node, "drain", 0)
	if err != nil || command != "set server uma_slaves/slave-01 state drain" {
		t.Fatalf("drain command=%q err=%v", command, err)
	}
	command, err = control.actionCommand(node, "weight", 25)
	if err != nil || command != "set server uma_slaves/slave-01 weight 25%" {
		t.Fatalf("weight command=%q err=%v", command, err)
	}
	if _, err := control.actionCommand(node, "weight", 101); err == nil {
		t.Fatal("accepted invalid weight")
	}
	command, err = control.actionCommand(node, "force", 0)
	if err != nil || command != "set server uma_slaves/slave-01 state maint" {
		t.Fatalf("force command=%q err=%v", command, err)
	}
}

func TestDashboardIncludesWorkerURIStatistics(t *testing.T) {
	for _, expected := range []string{"Worker 接入 URI 统计", "loadURIStats(nodes)", "独立 Worker IP"} {
		if !strings.Contains(dashboardHTML, expected) {
			t.Errorf("dashboard is missing %q", expected)
		}
	}
}

func TestDashboardShowsActualAndCurrentProxyConnections(t *testing.T) {
	for _, expected := range []string{"nodeSubscribers", "Worker实际连接", "HAProxy当前进程", "连接（实际/代理）", "代理当前"} {
		if !strings.Contains(dashboardHTML, expected) {
			t.Errorf("dashboard connection summary is missing %q", expected)
		}
	}
}

func TestDashboardLatencyChartKeepsFixedRangeAndTooltip(t *testing.T) {
	for _, expected := range []string{"max=1000", "id=\"charttooltip\"", "showLatencyTooltip", "纵轴固定 0–1000ms", "generated_at_ms", "id=\"latencycollector\"", "collector_id", "解码错误", "lastSample", "drawLatency(history,stage)", "最近样本", "batch_event_count", "batch_refs", "窗口 n="} {
		if !strings.Contains(dashboardHTML, expected) {
			t.Errorf("dashboard latency chart is missing %q", expected)
		}
	}
}

func TestDashboardUsesCompactSingleScreenMonitoringLayout(t *testing.T) {
	for _, expected := range []string{"height:100vh", "grid-template-areas", "grid-area:latency", "grid-area:nodes", "grid-area:events", "class=\"tablewrap nodepanel\"", "max-height:720px"} {
		if !strings.Contains(dashboardHTML, expected) {
			t.Errorf("dashboard compact layout is missing %q", expected)
		}
	}
}
