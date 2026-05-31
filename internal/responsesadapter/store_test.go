package responsesadapter

import (
	"errors"
	"testing"
	"time"
)

func TestMemoryStoreEvictsExpiredRecordsBeforeOldestLiveRecord(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewMemoryStore()
	store.now = func() time.Time { return now }
	store.ttl = time.Second
	store.max = 2
	scope := Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main"}

	store.Store(ResponseRecord{ID: "resp_old", Scope: scope, OutputText: "old", Status: ResponseStatusCompleted})
	now = now.Add(2 * time.Second)
	store.Store(ResponseRecord{ID: "resp_live_1", Scope: scope, OutputText: "live-1", Status: ResponseStatusCompleted})
	store.Store(ResponseRecord{ID: "resp_live_2", Scope: scope, OutputText: "live-2", Status: ResponseStatusCompleted})

	if _, err := store.Get("resp_old", scope); !errors.Is(err, ErrResponseNotFound) {
		t.Fatalf("old record err = %v, want ErrResponseNotFound", err)
	}
	if got, err := store.Get("resp_live_1", scope); err != nil || got.OutputText != "live-1" {
		t.Fatalf("live_1 = %#v, err = %v", got, err)
	}
	if got, err := store.Get("resp_live_2", scope); err != nil || got.OutputText != "live-2" {
		t.Fatalf("live_2 = %#v, err = %v", got, err)
	}
}

func TestMemoryStoreResolveChainBreaksCycles(t *testing.T) {
	store := NewMemoryStore()
	scope := Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main"}
	store.Store(ResponseRecord{ID: "resp_a", PreviousResponseID: "resp_b", Scope: scope, OutputText: "a", Status: ResponseStatusCompleted})
	store.Store(ResponseRecord{ID: "resp_b", PreviousResponseID: "resp_a", Scope: scope, OutputText: "b", Status: ResponseStatusCompleted})

	chain, err := store.ResolveChain("resp_a", scope)
	if err != nil {
		t.Fatalf("ResolveChain: %v", err)
	}
	if len(chain) != 2 || chain[0].ID != "resp_b" || chain[1].ID != "resp_a" {
		t.Fatalf("chain = %#v", chain)
	}
}
