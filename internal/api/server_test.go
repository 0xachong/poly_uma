package api

import (
	"testing"
	"time"
)

func TestLookupLRUEvictsAtCapacity(t *testing.T) {
	cache := newLookupLRU(2, time.Minute)
	cache.set("one", 1)
	cache.set("two", 2)
	cache.set("three", 3)

	if _, ok := cache.get("one"); ok {
		t.Fatal("oldest entry was not evicted")
	}
	if got, ok := cache.get("two"); !ok || got != 2 {
		t.Fatalf("second entry = %v, %t", got, ok)
	}
	if got, ok := cache.get("three"); !ok || got != 3 {
		t.Fatalf("third entry = %v, %t", got, ok)
	}
}

func TestLookupLRURemovesExpiredEntry(t *testing.T) {
	cache := newLookupLRU(2, time.Nanosecond)
	cache.set("expired", 1)
	time.Sleep(time.Millisecond)
	if _, ok := cache.get("expired"); ok {
		t.Fatal("expired entry was returned")
	}
	if len(cache.items) != 0 || cache.ll.Len() != 0 {
		t.Fatalf("expired entry retained: items=%d list=%d", len(cache.items), cache.ll.Len())
	}
}
