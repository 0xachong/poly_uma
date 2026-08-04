package main

import (
	"strings"
	"testing"
)

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

func TestDashboardShowsAndUsesCompactSubscriptionParameters(t *testing.T) {
	for _, expected := range []string{
		"/uma/v2/ws/events?batch=true&format=compact&sports_types=moneyline,child_moneyline",
		"/uma/v2/ws/events?batch=true&format=compact",
		"id=\"streamparams\"",
		"业务过滤流",
		"原始全量流",
		"Array.isArray(batch.events)",
		"connectEventWS()",
	} {
		if !strings.Contains(dashboardHTML, expected) {
			t.Errorf("dashboard is missing %q", expected)
		}
	}
	for _, obsolete := range []string{
		"connectEventWS('proposed')",
		"connectEventWS('disputed')",
	} {
		if strings.Contains(dashboardHTML, obsolete) {
			t.Errorf("dashboard still contains obsolete subscription %q", obsolete)
		}
	}
}
