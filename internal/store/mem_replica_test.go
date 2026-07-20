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
