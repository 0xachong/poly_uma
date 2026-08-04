package store

import "testing"

func TestBroadcastDisconnectsSlowSubscriber(t *testing.T) {
	t.Setenv("WS_DISCONNECT_SLOW_CLIENT", "1")
	mem := NewMemReplica()
	ch, cancel := mem.Subscribe("propose")
	for i := 0; i < cap(ch); i++ {
		mem.BroadcastNew("propose", EventRow{EventType: "propose", LogIndex: i})
	}
	mem.BroadcastNew("propose", EventRow{EventType: "propose", LogIndex: cap(ch)})

	// Must be idempotent after BroadcastNew removed and closed the subscriber.
	cancel()
	for range ch {
	}
}

func TestUnifiedSubscriptionReceivesProposeAndDisputeOnce(t *testing.T) {
	mem := NewMemReplica()
	ch, cancel := mem.SubscribeEvents()
	defer cancel()
	mem.BroadcastNew("propose", EventRow{EventType: "propose", ConditionID: "0xa"})
	mem.BroadcastNew("dispute", EventRow{EventType: "dispute", ConditionID: "0xb"})
	first, second := <-ch, <-ch
	if first.EventType != "propose" || second.EventType != "dispute" {
		t.Fatalf("unified events=%+v %+v", first, second)
	}
}
