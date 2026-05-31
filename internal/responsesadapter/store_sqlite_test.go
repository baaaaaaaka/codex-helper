package responsesadapter

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStorePersistsResponseChainAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "responses.sqlite")
	scope := Scope{
		Tenant:         "tenant",
		User:           "user",
		Provider:       "mimo",
		Model:          "mimo-v2.5",
		Thread:         "thread-a",
		Branch:         "main",
		KeyFingerprint: "key:a",
		BaseURLHash:    "url:a",
		ProfileVersion: "mimo:v1",
	}

	store, err := NewSQLiteStore(path, SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	if err := store.Store(ResponseRecord{
		ID:         "resp_1",
		Scope:      scope,
		InputText:  "one",
		OutputText: "answer one",
		Status:     ResponseStatusCompleted,
		Model:      "mimo-v2.5",
		Usage:      &Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15, CachedTokens: 7},
		ToolCalls:  []ToolCallRecord{{ID: "call_1", Name: "read", Arguments: `{}`}},
	}); err != nil {
		t.Fatalf("store resp_1: %v", err)
	}
	if err := store.Store(ResponseRecord{
		ID:                 "resp_2",
		PreviousResponseID: "resp_1",
		Scope:              scope,
		InputMessages:      []ProviderMessage{{Role: "user", Content: "two"}},
		OutputText:         "answer two",
		Status:             ResponseStatusCompleted,
		Model:              "mimo-v2.5",
	}); err != nil {
		t.Fatalf("store resp_2: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := NewSQLiteStore(path, SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer reopened.Close()

	chain, err := reopened.ResolveChain("resp_2", scope)
	if err != nil {
		t.Fatalf("ResolveChain after reopen: %v", err)
	}
	if len(chain) != 2 || chain[0].ID != "resp_1" || chain[1].ID != "resp_2" {
		t.Fatalf("chain after reopen = %#v", chain)
	}
	if chain[0].Usage == nil || chain[0].Usage.CachedTokens != 7 {
		t.Fatalf("usage after reopen = %#v", chain[0].Usage)
	}
	if len(chain[0].ToolCalls) != 1 || chain[0].ToolCalls[0].ID != "call_1" {
		t.Fatalf("tool calls after reopen = %#v", chain[0].ToolCalls)
	}
}

func TestSQLiteStoreScopeIncludesKeyFingerprint(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	scopeA := Scope{Tenant: "tenant", User: "user", Provider: "deepseek", Model: "deepseek-v4-flash", Thread: "thread", Branch: "main", KeyFingerprint: "key:a"}
	scopeB := scopeA
	scopeB.KeyFingerprint = "key:b"
	if err := store.Store(ResponseRecord{ID: "resp_key", Scope: scopeA, OutputText: "a", Status: ResponseStatusCompleted}); err != nil {
		t.Fatalf("store: %v", err)
	}
	if _, err := store.Get("resp_key", scopeB); !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("Get with different key fingerprint err = %v, want ErrScopeMismatch", err)
	}
	if _, err := store.ResolveChain("resp_key", scopeB); !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("ResolveChain with different key fingerprint err = %v, want ErrScopeMismatch", err)
	}
}

func TestSQLiteStoreActiveTurnIsolationDimensions(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	base := Scope{
		Tenant:         "tenant",
		User:           "user",
		Provider:       "provider",
		Model:          "model",
		Thread:         "thread",
		Branch:         "main",
		KeyFingerprint: "key:a",
		BaseURLHash:    "url:a",
		ProfileVersion: "profile:v1",
	}
	releaseBase, err := store.BeginTurn(base, "base")
	if err != nil {
		t.Fatalf("begin base: %v", err)
	}
	defer releaseBase()

	if release, err := store.BeginTurn(base, "same"); !errors.Is(err, ErrActiveTurn) {
		if release != nil {
			release()
		}
		t.Fatalf("same scope err = %v, want ErrActiveTurn", err)
	}

	cases := map[string]Scope{
		"tenant":          {Tenant: "tenant-b", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"},
		"user":            {Tenant: "tenant", User: "user-b", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"},
		"provider":        {Tenant: "tenant", User: "user", Provider: "provider-b", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"},
		"model":           {Tenant: "tenant", User: "user", Provider: "provider", Model: "model-b", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"},
		"thread":          {Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread-b", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"},
		"branch":          {Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "branch-b", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"},
		"key fingerprint": {Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:b", BaseURLHash: "url:a", ProfileVersion: "profile:v1"},
		"base url":        {Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:b", ProfileVersion: "profile:v1"},
		"profile version": {Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v2"},
	}
	for name, scope := range cases {
		t.Run(name, func(t *testing.T) {
			release, err := store.BeginTurn(scope, name)
			if err != nil {
				t.Fatalf("begin different %s: %v", name, err)
			}
			release()
		})
	}
}

func TestSQLiteStorePreservesFreshActiveTurnsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "responses.sqlite")
	scope := Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main"}

	store, err := NewSQLiteStore(path, SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	if _, err := store.BeginTurn(scope, "turn_1"); err != nil {
		t.Fatalf("begin turn: %v", err)
	}
	if _, err := store.BeginTurn(scope, "turn_2"); !errors.Is(err, ErrActiveTurn) {
		t.Fatalf("second begin err = %v, want ErrActiveTurn", err)
	}
	defer store.Close()

	reopened, err := NewSQLiteStore(path, SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer reopened.Close()
	if release, err := reopened.BeginTurn(scope, "turn_after_reopen"); !errors.Is(err, ErrActiveTurn) {
		if release != nil {
			release()
		}
		t.Fatalf("begin after fresh reopen err = %v, want ErrActiveTurn", err)
	}
}

func TestSQLiteStoreClearsStaleActiveTurnsOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "responses.sqlite")
	scope := Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main"}
	now := time.Unix(1000, 0)

	store, err := NewSQLiteStore(path, SQLiteStoreOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	if _, err := store.BeginTurn(scope, "turn_1"); err != nil {
		t.Fatalf("begin turn: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	now = now.Add(sqliteActiveTurnStaleAge + time.Minute)
	reopened, err := NewSQLiteStore(path, SQLiteStoreOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer reopened.Close()
	release, err := reopened.BeginTurn(scope, "turn_after_reopen")
	if err != nil {
		t.Fatalf("begin after stale reopen: %v", err)
	}
	release()
}

func TestSQLiteStoreEvictsByTTLAndMaxRecords(t *testing.T) {
	now := time.Unix(100, 0)
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{
		TTL:        time.Second,
		MaxRecords: 2,
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	scope := Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main"}

	if err := store.Store(ResponseRecord{ID: "expired", Scope: scope, OutputText: "old", Status: ResponseStatusCompleted}); err != nil {
		t.Fatalf("store expired: %v", err)
	}
	now = now.Add(2 * time.Second)
	for _, id := range []string{"live_1", "live_2", "live_3"} {
		if err := store.Store(ResponseRecord{ID: id, Scope: scope, OutputText: id, Status: ResponseStatusCompleted}); err != nil {
			t.Fatalf("store %s: %v", id, err)
		}
	}
	for _, id := range []string{"expired", "live_1"} {
		if _, err := store.Get(id, scope); !errors.Is(err, ErrResponseNotFound) {
			t.Fatalf("%s err = %v, want ErrResponseNotFound", id, err)
		}
	}
	for _, id := range []string{"live_2", "live_3"} {
		if got, err := store.Get(id, scope); err != nil || got.ID != id {
			t.Fatalf("%s got = %#v err = %v", id, got, err)
		}
	}
}
