package responsesadapter

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMemoryStoreActiveTurnIsolationDimensions(t *testing.T) {
	store := NewMemoryStore()
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

	for _, tc := range []struct {
		name  string
		scope Scope
	}{
		{name: "tenant", scope: Scope{Tenant: "tenant-b", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"}},
		{name: "user", scope: Scope{Tenant: "tenant", User: "user-b", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"}},
		{name: "provider", scope: Scope{Tenant: "tenant", User: "user", Provider: "provider-b", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"}},
		{name: "model", scope: Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model-b", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"}},
		{name: "thread", scope: Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread-b", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"}},
		{name: "branch", scope: Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "branch-b", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v1"}},
		{name: "key fingerprint", scope: Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:b", BaseURLHash: "url:a", ProfileVersion: "profile:v1"}},
		{name: "base url", scope: Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:b", ProfileVersion: "profile:v1"}},
		{name: "profile version", scope: Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "thread", Branch: "main", KeyFingerprint: "key:a", BaseURLHash: "url:a", ProfileVersion: "profile:v2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release, err := store.BeginTurn(tc.scope, tc.name)
			if err != nil {
				t.Fatalf("begin different %s: %v", tc.name, err)
			}
			release()
		})
	}
}

func TestFacadePromptCacheKeyDoesNotBypassScopeIsolation(t *testing.T) {
	store := NewMemoryStore()
	facade := newTestFacade(store, fakeAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "a"}, {Kind: ProviderEventDone}}})

	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","prompt_cache_key":"same-cache","input":"one"}`))
	first.Header.Set("x-codex-thread-id", "thread-a")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}

	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","prompt_cache_key":"same-cache","previous_response_id":"resp_001","input":"two"}`))
	second.Header.Set("x-codex-thread-id", "thread-b")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want conflict; body = %s", secondRec.Code, secondRec.Body.String())
	}
	if !strings.Contains(secondRec.Body.String(), ErrScopeMismatch.Error()) {
		t.Fatalf("body = %s", secondRec.Body.String())
	}
}

func TestFacadePromptCacheKeyFallbackSeparatesThreads(t *testing.T) {
	store := NewMemoryStore()
	facade := newTestFacade(store, fakeAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "a"}, {Kind: ProviderEventDone}}})

	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","prompt_cache_key":"cache-a","input":"one"}`))
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}

	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","prompt_cache_key":"cache-b","previous_response_id":"resp_001","input":"two"}`))
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want conflict; body = %s", secondRec.Code, secondRec.Body.String())
	}
	if !strings.Contains(secondRec.Body.String(), ErrScopeMismatch.Error()) {
		t.Fatalf("body = %s", secondRec.Body.String())
	}

	third := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","prompt_cache_key":"cache-a","previous_response_id":"resp_001","input":"three"}`))
	thirdRec := httptest.NewRecorder()
	facade.ServeHTTP(thirdRec, third)
	if thirdRec.Code != http.StatusOK {
		t.Fatalf("third status = %d, body = %s", thirdRec.Code, thirdRec.Body.String())
	}
}

func TestFacadeRejectsPreviousResponseWhenKeyFingerprintChanges(t *testing.T) {
	store := NewMemoryStore()
	facade := newTestFacade(store, fakeAdapter{events: []ProviderEvent{{Kind: ProviderEventTextDelta, Delta: "a"}, {Kind: ProviderEventDone}}})

	first := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","input":"one"}`))
	first.Header.Set("x-codex-thread-id", "thread-a")
	first.Header.Set("x-adapter-key-fingerprint", "key:a")
	firstRec := httptest.NewRecorder()
	facade.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}

	second := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"model-a","previous_response_id":"resp_001","input":"two"}`))
	second.Header.Set("x-codex-thread-id", "thread-a")
	second.Header.Set("x-adapter-key-fingerprint", "key:b")
	secondRec := httptest.NewRecorder()
	facade.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want conflict; body = %s", secondRec.Code, secondRec.Body.String())
	}
	if !strings.Contains(secondRec.Body.String(), ErrScopeMismatch.Error()) {
		t.Fatalf("body = %s", secondRec.Body.String())
	}
}
