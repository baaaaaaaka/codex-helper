package responsesadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestMemoryStoreStressConcurrentDistinctScopes(t *testing.T) {
	store := NewMemoryStore()
	const workers = 32
	const turnsPerWorker = 25
	store.max = workers*turnsPerWorker + 10

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			scope := Scope{
				Tenant:   "tenant",
				User:     "user",
				Provider: "provider",
				Model:    "model",
				Thread:   fmt.Sprintf("thread-%02d", worker),
				Branch:   "main",
			}
			previousID := ""
			for turn := 0; turn < turnsPerWorker; turn++ {
				id := fmt.Sprintf("resp_%02d_%02d", worker, turn)
				release, err := store.BeginTurn(scope, id)
				if err != nil {
					errCh <- fmt.Errorf("begin worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				store.Store(ResponseRecord{
					ID:                 id,
					PreviousResponseID: previousID,
					Scope:              scope,
					InputText:          fmt.Sprintf("input-%d-%d", worker, turn),
					OutputText:         fmt.Sprintf("output-%d-%d", worker, turn),
					Status:             ResponseStatusCompleted,
					Model:              "model",
				})
				got, err := store.Get(id, scope)
				if err != nil {
					release()
					errCh <- fmt.Errorf("get worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				if got.OutputText != fmt.Sprintf("output-%d-%d", worker, turn) {
					release()
					errCh <- fmt.Errorf("wrong output worker=%d turn=%d: %q", worker, turn, got.OutputText)
					return
				}
				chain, err := store.ResolveChain(id, scope)
				if err != nil {
					release()
					errCh <- fmt.Errorf("resolve worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				if len(chain) != turn+1 {
					release()
					errCh <- fmt.Errorf("chain len worker=%d turn=%d: got %d", worker, turn, len(chain))
					return
				}
				previousID = id
				release()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMemoryStoreStressSameScopeSingleWriter(t *testing.T) {
	store := NewMemoryStore()
	scope := Scope{Tenant: "tenant", User: "user", Provider: "provider", Model: "model", Thread: "same-thread", Branch: "main"}

	release, err := store.BeginTurn(scope, "owner")
	if err != nil {
		t.Fatalf("begin owner: %v", err)
	}
	const contenders = 64
	start := make(chan struct{})
	errCh := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			gotRelease, err := store.BeginTurn(scope, fmt.Sprintf("contender-%d", i))
			if !errors.Is(err, ErrActiveTurn) {
				if gotRelease != nil {
					gotRelease()
				}
				errCh <- fmt.Errorf("contender %d err = %v, want ErrActiveTurn", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	release()
	nextRelease, err := store.BeginTurn(scope, "next")
	if err != nil {
		t.Fatalf("begin after release: %v", err)
	}
	nextRelease()
}

func TestSQLiteStoreStressConcurrentDistinctScopes(t *testing.T) {
	const workers = 16
	const turnsPerWorker = 18
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{
		MaxRecords: workers*turnsPerWorker + 100,
	})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			scope := Scope{
				Tenant:         "tenant",
				User:           fmt.Sprintf("user-%02d", worker%4),
				Provider:       "deepseek",
				Model:          "deepseek-v4-flash",
				Thread:         fmt.Sprintf("thread-%02d", worker),
				Branch:         "main",
				KeyFingerprint: fmt.Sprintf("key-%02d", worker%3),
				BaseURLHash:    "url-deepseek",
				ProfileVersion: "deepseek:v1",
			}
			previousID := ""
			for turn := 0; turn < turnsPerWorker; turn++ {
				id := fmt.Sprintf("sqlite_resp_%02d_%02d", worker, turn)
				release, err := store.BeginTurn(scope, id)
				if err != nil {
					errCh <- fmt.Errorf("begin worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				callID := fmt.Sprintf("call_%02d_%02d", worker, turn)
				record := ResponseRecord{
					ID:                 id,
					PreviousResponseID: previousID,
					Scope:              scope,
					InputText:          fmt.Sprintf("input-%d-%d", worker, turn),
					OutputText:         fmt.Sprintf("output-%d-%d", worker, turn),
					ReasoningText:      fmt.Sprintf("reason-%d-%d", worker, turn),
					ToolCalls:          []ToolCallRecord{{ID: callID, Name: "exec_command", Arguments: `{"cmd":"pwd"}`, Status: "completed"}},
					Status:             ResponseStatusCompleted,
					Model:              scope.Model,
					Usage:              &Usage{InputTokens: turn + 1, OutputTokens: turn + 2, TotalTokens: 2*turn + 3, CachedTokens: turn},
				}
				if err := store.Store(record); err != nil {
					release()
					errCh <- fmt.Errorf("store worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				got, err := store.Get(id, scope)
				if err != nil {
					release()
					errCh <- fmt.Errorf("get worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				if got.OutputText != record.OutputText || got.ReasoningText != record.ReasoningText {
					release()
					errCh <- fmt.Errorf("wrong record worker=%d turn=%d: %#v", worker, turn, got)
					return
				}
				chain, err := store.ResolveChain(id, scope)
				if err != nil {
					release()
					errCh <- fmt.Errorf("resolve worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				if len(chain) != turn+1 {
					release()
					errCh <- fmt.Errorf("chain len worker=%d turn=%d: got %d", worker, turn, len(chain))
					return
				}
				reasoning, err := store.LookupReasoning(scope, ProviderMessage{Role: "assistant", ToolCalls: []ToolCallRecord{{ID: callID}}})
				if err != nil {
					release()
					errCh <- fmt.Errorf("lookup reasoning worker=%d turn=%d: %w", worker, turn, err)
					return
				}
				if reasoning != record.ReasoningText {
					release()
					errCh <- fmt.Errorf("reasoning worker=%d turn=%d: got %q, want %q", worker, turn, reasoning, record.ReasoningText)
					return
				}
				wrongScope := scope
				wrongScope.KeyFingerprint += "-other"
				if _, err := store.Get(id, wrongScope); !errors.Is(err, ErrScopeMismatch) {
					release()
					errCh <- fmt.Errorf("wrong scope worker=%d turn=%d err = %v, want ErrScopeMismatch", worker, turn, err)
					return
				}
				previousID = id
				release()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteStoreStressSameScopeSingleWriter(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	scope := Scope{
		Tenant:         "tenant",
		User:           "user",
		Provider:       "mimo",
		Model:          "mimo-v2.5",
		Thread:         "same-thread",
		Branch:         "main",
		KeyFingerprint: "key-mimo",
		BaseURLHash:    "url-mimo",
		ProfileVersion: "mimo:v1",
	}
	release, err := store.BeginTurn(scope, "owner")
	if err != nil {
		t.Fatalf("begin owner: %v", err)
	}
	const contenders = 64
	start := make(chan struct{})
	errCh := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			gotRelease, err := store.BeginTurn(scope, fmt.Sprintf("contender-%d", i))
			if !errors.Is(err, ErrActiveTurn) {
				if gotRelease != nil {
					gotRelease()
				}
				errCh <- fmt.Errorf("contender %d err = %v, want ErrActiveTurn", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	release()
	nextRelease, err := store.BeginTurn(scope, "next")
	if err != nil {
		t.Fatalf("begin after release: %v", err)
	}
	nextRelease()
}

func TestProviderRegistryStressConcurrentResolve(t *testing.T) {
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		DefaultProvider: "deepseek",
		KeySalt:         "stress-salt",
		ProxyKeys: map[string]string{
			"mi-key":  "mimo",
			"ds-key":  "deepseek",
			"all-key": "*",
		},
		Providers: []ProviderConfig{
			{
				ID:           "mimo",
				ProfileID:    "mimo",
				APIKey:       "sk-mimo",
				DefaultModel: "mimo-v2.5",
				Models:       []ModelInfo{{ID: "mimo-v2.5"}, {ID: "shared-coder"}},
				Adapter:      runtimeEchoAdapter{},
			},
			{
				ID:           "deepseek",
				ProfileID:    "deepseek",
				APIKey:       "sk-deepseek",
				DefaultModel: "deepseek-v4-flash",
				Models:       []ModelInfo{{ID: "deepseek-v4-flash"}, {ID: "shared-coder"}},
				Adapter:      runtimeEchoAdapter{},
			},
			{
				ID:           "qwen",
				ProfileID:    "openai-compatible",
				BaseURL:      "https://dashscope.aliyuncs.com/compatible-mode/v1",
				APIKey:       "sk-qwen",
				DefaultModel: "qwen-coder",
				Models:       []ModelInfo{{ID: "qwen-coder"}},
				Adapter:      runtimeEchoAdapter{},
			},
		},
	})

	type resolveCase struct {
		name           string
		auth           string
		model          string
		providerHeader string
		wantProvider   string
		wantModel      string
		wantStatus     int
	}
	cases := []resolveCase{
		{name: "mimo locked key", auth: "mi-key", model: "mimo-v2.5", wantProvider: "mimo", wantModel: "mimo-v2.5"},
		{name: "deepseek locked key", auth: "ds-key", model: "deepseek-v4-flash", wantProvider: "deepseek", wantModel: "deepseek-v4-flash"},
		{name: "wildcard qwen", auth: "all-key", model: "qwen-coder", wantProvider: "qwen", wantModel: "qwen-coder"},
		{name: "ambiguous model with explicit provider", auth: "all-key", model: "shared-coder", providerHeader: "mimo", wantProvider: "mimo", wantModel: "shared-coder"},
		{name: "missing proxy key", model: "mimo-v2.5", wantStatus: http.StatusUnauthorized},
		{name: "locked key wrong model", auth: "mi-key", model: "deepseek-v4-flash", wantStatus: http.StatusUnauthorized},
		{name: "ambiguous model without provider", auth: "all-key", model: "shared-coder", wantStatus: http.StatusConflict},
	}

	const workers = 32
	const iterations = 80
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < iterations; iter++ {
				tc := cases[(worker+iter)%len(cases)]
				req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				if tc.auth != "" {
					req.Header.Set("Authorization", "Bearer "+tc.auth)
				}
				if tc.providerHeader != "" {
					req.Header.Set("x-codex-provider", tc.providerHeader)
				}
				runtime, err := registry.Resolve(req, ResponsesRequest{Model: tc.model})
				if tc.wantStatus != 0 {
					if err == nil {
						errCh <- fmt.Errorf("%s worker=%d iter=%d succeeded, want status %d", tc.name, worker, iter, tc.wantStatus)
						return
					}
					if got := routeErrorStatus(err); got != tc.wantStatus {
						errCh <- fmt.Errorf("%s worker=%d iter=%d status = %d err = %v, want %d", tc.name, worker, iter, got, err, tc.wantStatus)
						return
					}
					continue
				}
				if err != nil {
					errCh <- fmt.Errorf("%s worker=%d iter=%d resolve: %w", tc.name, worker, iter, err)
					return
				}
				if runtime.ProviderID != tc.wantProvider || runtime.Model != tc.wantModel {
					errCh <- fmt.Errorf("%s worker=%d iter=%d runtime = %#v", tc.name, worker, iter, runtime)
					return
				}
				if runtime.KeyFingerprint == "" || runtime.KeyFingerprint == "no-key" || runtime.BaseURLHash == "" || runtime.ProfileVersion == "" {
					errCh <- fmt.Errorf("%s worker=%d iter=%d incomplete runtime identity: %#v", tc.name, worker, iter, runtime)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFacadeStressConcurrentStreamsStayScoped(t *testing.T) {
	store := NewMemoryStore()
	facade := &Facade{
		Adapter:      scopedEchoAdapter{},
		Store:        store,
		ProviderID:   "provider",
		DefaultModel: "model",
		Models:       []ModelInfo{{ID: "model", OwnedBy: "provider"}},
	}

	const requests = 48
	errCh := make(chan error, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			thread := fmt.Sprintf("thread-%02d", i)
			input := fmt.Sprintf("payload-%02d", i)
			want := "reply:" + thread + ":" + input
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"model","stream":true,"input":%q}`, input)))
			req.Header.Set("x-codex-thread-id", thread)
			rec := httptest.NewRecorder()
			facade.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				errCh <- fmt.Errorf("%s status = %d body = %s", thread, rec.Code, rec.Body.String())
				return
			}
			body := rec.Body.String()
			if !strings.Contains(body, "event: response.completed") {
				errCh <- fmt.Errorf("%s missing completed:\n%s", thread, body)
				return
			}
			if !strings.Contains(body, `"output_text":"`+want+`"`) {
				errCh <- fmt.Errorf("%s stream crossed or lost text, want %q:\n%s", thread, want, body)
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFacadeStressConcurrentToolCallStreamsStayScoped(t *testing.T) {
	store := NewMemoryStore()
	facade := &Facade{
		Adapter:      scopedToolCallAdapter{},
		Store:        store,
		ProviderID:   "provider",
		DefaultModel: "model",
		Models:       []ModelInfo{{ID: "model", OwnedBy: "provider"}},
	}

	const requests = 48
	errCh := make(chan error, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			thread := fmt.Sprintf("thread-%02d", i)
			input := fmt.Sprintf("payload-%02d", i)
			callID := "call_" + thread
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"model","stream":true,"input":%q}`, input)))
			req.Header.Set("x-codex-thread-id", thread)
			rec := httptest.NewRecorder()
			facade.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				errCh <- fmt.Errorf("%s status = %d body = %s", thread, rec.Code, rec.Body.String())
				return
			}
			body := rec.Body.String()
			for _, want := range []string{
				"event: response.function_call_arguments.delta",
				"event: response.completed",
				`"call_id":"` + callID + `"`,
				thread,
				input,
			} {
				if !strings.Contains(body, want) {
					errCh <- fmt.Errorf("%s stream missing %q:\n%s", thread, want, body)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFacadeStressConcurrentRouterSQLiteStoreIsolation(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{MaxRecords: 500})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		KeySalt: "facade-stress",
		ProxyKeys: map[string]string{
			"mi-key": "*",
			"ds-key": "*",
		},
		Providers: []ProviderConfig{
			{
				ID:           "mimo",
				ProfileID:    "mimo",
				APIKey:       "sk-mimo",
				DefaultModel: "mimo-v2.5",
				Models:       []ModelInfo{{ID: "mimo-v2.5"}},
				Adapter:      runtimeEchoAdapter{},
			},
			{
				ID:           "deepseek",
				ProfileID:    "deepseek",
				APIKey:       "sk-deepseek",
				DefaultModel: "deepseek-v4-flash",
				Models:       []ModelInfo{{ID: "deepseek-v4-flash"}},
				Adapter:      runtimeEchoAdapter{},
			},
		},
	})
	facade := &Facade{Router: registry, Store: store}

	const workers = 24
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			provider, model, auth := "mimo", "mimo-v2.5", "mi-key"
			otherProvider, otherModel, otherAuth := "deepseek", "deepseek-v4-flash", "ds-key"
			if worker%2 == 1 {
				provider, model, auth = "deepseek", "deepseek-v4-flash", "ds-key"
				otherProvider, otherModel, otherAuth = "mimo", "mimo-v2.5", "mi-key"
			}
			thread := fmt.Sprintf("router-thread-%02d", worker)
			firstBody := map[string]any{
				"model":                 model,
				"instructions":          "stay scoped",
				"input":                 fmt.Sprintf("first-%02d", worker),
				"tools":                 []map[string]any{{"type": "function", "name": "read_file", "parameters": map[string]any{"type": "object"}}},
				"tool_choice":           "auto",
				"parallel_tool_calls":   worker%3 == 0,
				"max_output_tokens":     512 + worker,
				"temperature":           0.1,
				"top_p":                 0.9,
				"reasoning":             map[string]any{"effort": "low"},
				"prompt_cache_key":      "shared-cache-key",
				"store":                 false,
				"include":               []string{"reasoning.encrypted_content"},
				"service_tier":          "auto",
				"client_metadata":       map[string]any{"worker": worker},
				"irrelevant_future_key": "ignored",
			}
			if worker%4 == 0 {
				firstBody["input"] = []map[string]any{{
					"type": "message",
					"role": "user",
					"content": []map[string]any{
						{"type": "input_text", "text": fmt.Sprintf("first-%02d", worker)},
						{"type": "input_image", "image_url": "data:image/png;base64,abc", "detail": "auto"},
					},
				}}
			}
			first, status, raw, err := doStressResponse(facade, firstBody, map[string]string{
				"Authorization":      "Bearer " + auth,
				"x-codex-provider":   provider,
				"x-codex-thread-id":  thread,
				"x-adapter-tenant":   "tenant-a",
				"x-adapter-user":     fmt.Sprintf("user-%02d", worker%5),
				"x-adapter-branch":   "main",
				"x-adapter-extra-id": "ignored",
			})
			if err != nil {
				errCh <- fmt.Errorf("%s first decode: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s first status = %d body = %s", thread, status, raw)
				return
			}
			for _, want := range []string{
				"reply:" + provider + ":" + model,
				"thread=" + thread,
				"history=0",
				"tools=1",
				"reasoning=low",
			} {
				if !strings.Contains(first.OutputText, want) {
					errCh <- fmt.Errorf("%s first output missing %q: %q", thread, want, first.OutputText)
					return
				}
			}
			if worker%4 == 0 && !strings.Contains(first.OutputText, "images=1") {
				errCh <- fmt.Errorf("%s first output missing image part: %q", thread, first.OutputText)
				return
			}

			second, status, raw, err := doStressResponse(facade, map[string]any{
				"model":                model,
				"previous_response_id": first.ID,
				"input":                fmt.Sprintf("second-%02d", worker),
				"prompt_cache_key":     "shared-cache-key",
			}, map[string]string{
				"Authorization":     "Bearer " + auth,
				"x-codex-provider":  provider,
				"x-codex-thread-id": thread,
				"x-adapter-tenant":  "tenant-a",
				"x-adapter-user":    fmt.Sprintf("user-%02d", worker%5),
				"x-adapter-branch":  "main",
			})
			if err != nil {
				errCh <- fmt.Errorf("%s second decode: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s second status = %d body = %s", thread, status, raw)
				return
			}
			for _, want := range []string{
				"reply:" + provider + ":" + model,
				"thread=" + thread,
				"history=1",
				fmt.Sprintf("second-%02d", worker),
			} {
				if !strings.Contains(second.OutputText, want) {
					errCh <- fmt.Errorf("%s second output missing %q: %q", thread, want, second.OutputText)
					return
				}
			}
			_, status, raw, err = doStressResponse(facade, map[string]any{
				"model":                otherModel,
				"previous_response_id": first.ID,
				"input":                "wrong-key-switch",
			}, map[string]string{
				"Authorization":     "Bearer " + otherAuth,
				"x-codex-provider":  otherProvider,
				"x-codex-thread-id": thread,
				"x-adapter-tenant":  "tenant-a",
				"x-adapter-user":    fmt.Sprintf("user-%02d", worker%5),
				"x-adapter-branch":  "main",
			})
			if err != nil {
				errCh <- fmt.Errorf("%s wrong-key request: %w", thread, err)
				return
			}
			if status != http.StatusConflict {
				errCh <- fmt.Errorf("%s wrong-key status = %d body = %s", thread, status, raw)
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFacadeStressReasoningCacheFullReplayWithSQLite(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{MaxRecords: 500})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	facade := &Facade{
		Adapter:        reasoningReplayStressAdapter{},
		Store:          store,
		ProviderID:     "mimo",
		DefaultModel:   "mimo-v2.5",
		Models:         []ModelInfo{{ID: "mimo-v2.5", OwnedBy: "mimo"}},
		KeyFingerprint: "mimo-key",
		BaseURLHash:    "mimo-url",
		ProfileVersion: "mimo:v1",
	}

	const workers = 24
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			thread := fmt.Sprintf("reason-thread-%02d", worker)
			callID := "call_" + thread
			first, status, raw, err := doStressResponse(facade, map[string]any{
				"model": "mimo-v2.5",
				"input": "first:" + thread,
			}, map[string]string{"x-codex-thread-id": thread})
			if err != nil {
				errCh <- fmt.Errorf("%s first decode: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s first status = %d body = %s", thread, status, raw)
				return
			}
			if first.ID == "" {
				errCh <- fmt.Errorf("%s first response id is empty", thread)
				return
			}

			second, status, raw, err := doStressResponse(facade, map[string]any{
				"model": "mimo-v2.5",
				"input": []map[string]any{
					{"type": "function_call", "call_id": callID, "name": "exec_command", "arguments": `{"cmd":"pwd"}`},
					{"type": "function_call_output", "call_id": callID, "output": "workspace"},
					{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "continue"}}},
				},
			}, map[string]string{"x-codex-thread-id": thread})
			if err != nil {
				errCh <- fmt.Errorf("%s second decode: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s second status = %d body = %s", thread, status, raw)
				return
			}
			want := "replay:" + thread + ":" + callID + ":reason:" + thread
			if !strings.Contains(second.OutputText, want) {
				errCh <- fmt.Errorf("%s replay output = %q, want %q", thread, second.OutputText, want)
				return
			}

			wrongThread := "other-" + thread
			wrong, status, raw, err := doStressResponse(facade, map[string]any{
				"model": "mimo-v2.5",
				"input": []map[string]any{
					{"type": "function_call", "call_id": callID, "name": "exec_command", "arguments": `{"cmd":"pwd"}`},
					{"type": "function_call_output", "call_id": callID, "output": "workspace"},
					{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "continue elsewhere"}}},
				},
			}, map[string]string{"x-codex-thread-id": wrongThread})
			if err != nil {
				errCh <- fmt.Errorf("%s wrong-thread decode: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s wrong-thread status = %d body = %s", thread, status, raw)
				return
			}
			if strings.Contains(wrong.OutputText, "reason:"+thread) {
				errCh <- fmt.Errorf("%s reasoning leaked into %s: %q", thread, wrongThread, wrong.OutputText)
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFacadeStressControlPlaneRouteErrorsDoNotPoisonValidRequests(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{MaxRecords: 500})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	registry := mustProviderRegistry(t, ProviderRegistryOptions{
		KeySalt: "route-error-stress",
		ProxyKeys: map[string]string{
			"mi-key":   "mimo",
			"ds-key":   "deepseek",
			"wild-key": "*",
		},
		Providers: []ProviderConfig{
			{
				ID:           "mimo",
				ProfileID:    "mimo",
				APIKey:       "sk-mimo",
				DefaultModel: "mimo-v2.5",
				Models:       []ModelInfo{{ID: "mimo-v2.5"}, {ID: "shared-coder"}},
				Adapter:      runtimeEchoAdapter{},
			},
			{
				ID:           "deepseek",
				ProfileID:    "deepseek",
				APIKey:       "sk-deepseek",
				DefaultModel: "deepseek-v4-flash",
				Models:       []ModelInfo{{ID: "deepseek-v4-flash"}, {ID: "shared-coder"}},
				Adapter:      runtimeEchoAdapter{},
			},
		},
	})
	facade := &Facade{Router: registry, Store: store}

	type routeCase struct {
		name       string
		body       map[string]any
		headers    map[string]string
		wantStatus int
		wantOutput []string
		wantError  string
	}
	cases := []routeCase{
		{
			name:       "valid mimo locked key",
			body:       map[string]any{"model": "mimo-v2.5", "input": "ok-mimo"},
			headers:    map[string]string{"Authorization": "Bearer mi-key", "x-codex-thread-id": "route-ok-mimo"},
			wantStatus: http.StatusOK,
			wantOutput: []string{"reply:mimo:mimo-v2.5", "thread=route-ok-mimo", "input=ok-mimo"},
		},
		{
			name:       "valid deepseek locked key",
			body:       map[string]any{"model": "deepseek-v4-flash", "input": "ok-deepseek"},
			headers:    map[string]string{"Authorization": "Bearer ds-key", "x-codex-thread-id": "route-ok-deepseek"},
			wantStatus: http.StatusOK,
			wantOutput: []string{"reply:deepseek:deepseek-v4-flash", "thread=route-ok-deepseek", "input=ok-deepseek"},
		},
		{
			name:       "missing key",
			body:       map[string]any{"model": "mimo-v2.5", "input": "missing-key"},
			headers:    map[string]string{"x-codex-thread-id": "route-missing"},
			wantStatus: http.StatusUnauthorized,
			wantError:  "missing proxy authorization key",
		},
		{
			name:       "invalid key",
			body:       map[string]any{"model": "mimo-v2.5", "input": "bad-key"},
			headers:    map[string]string{"Authorization": "Bearer bad-key", "x-codex-thread-id": "route-bad-key"},
			wantStatus: http.StatusUnauthorized,
			wantError:  "invalid proxy authorization key",
		},
		{
			name:       "locked key wrong provider header",
			body:       map[string]any{"model": "mimo-v2.5", "input": "wrong-provider"},
			headers:    map[string]string{"Authorization": "Bearer mi-key", "x-codex-provider": "deepseek", "x-codex-thread-id": "route-wrong-provider"},
			wantStatus: http.StatusUnauthorized,
			wantError:  "locked to provider",
		},
		{
			name:       "locked key wrong model",
			body:       map[string]any{"model": "deepseek-v4-flash", "input": "wrong-model"},
			headers:    map[string]string{"Authorization": "Bearer mi-key", "x-codex-thread-id": "route-wrong-model"},
			wantStatus: http.StatusUnauthorized,
			wantError:  "routes elsewhere",
		},
		{
			name:       "ambiguous model requires provider",
			body:       map[string]any{"model": "shared-coder", "input": "ambiguous"},
			headers:    map[string]string{"Authorization": "Bearer wild-key", "x-codex-thread-id": "route-ambiguous"},
			wantStatus: http.StatusConflict,
			wantError:  "configured by multiple providers",
		},
		{
			name:       "unknown explicit provider",
			body:       map[string]any{"model": "mimo-v2.5", "input": "unknown-provider"},
			headers:    map[string]string{"Authorization": "Bearer wild-key", "x-codex-provider": "missing", "x-codex-thread-id": "route-unknown-provider"},
			wantStatus: http.StatusBadRequest,
			wantError:  "provider",
		},
		{
			name:       "unknown model",
			body:       map[string]any{"model": "not-configured", "input": "unknown-model"},
			headers:    map[string]string{"Authorization": "Bearer wild-key", "x-codex-thread-id": "route-unknown-model"},
			wantStatus: http.StatusBadRequest,
			wantError:  "not configured for any provider",
		},
	}

	const rounds = 8
	errCh := make(chan error, rounds*len(cases))
	var wg sync.WaitGroup
	for round := 0; round < rounds; round++ {
		for _, tc := range cases {
			round, tc := round, tc
			wg.Add(1)
			go func() {
				defer wg.Done()
				body := cloneMap(tc.body)
				body["input"] = fmt.Sprintf("%v-%02d", body["input"], round)
				headers := cloneStringMap(tc.headers)
				headers["x-codex-thread-id"] = fmt.Sprintf("%s-%02d", headers["x-codex-thread-id"], round)
				response, status, raw, err := doStressResponse(facade, body, headers)
				if err != nil {
					errCh <- fmt.Errorf("%s round %d decode: %w", tc.name, round, err)
					return
				}
				if status != tc.wantStatus {
					errCh <- fmt.Errorf("%s round %d status = %d body = %s", tc.name, round, status, raw)
					return
				}
				if status == http.StatusOK {
					for _, want := range tc.wantOutput {
						if !strings.Contains(response.OutputText, want) {
							errCh <- fmt.Errorf("%s round %d output missing %q: %q", tc.name, round, want, response.OutputText)
							return
						}
					}
					return
				}
				if tc.wantError != "" && !strings.Contains(raw, tc.wantError) {
					errCh <- fmt.Errorf("%s round %d error body missing %q: %s", tc.name, round, tc.wantError, raw)
				}
			}()
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFacadeStressConcurrentCompactAndResponsesStayScoped(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{MaxRecords: 500})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	facade := &Facade{
		Adapter:        runtimeEchoAdapter{},
		Store:          store,
		ProviderID:     "mimo",
		DefaultModel:   "mimo-v2.5",
		Models:         []ModelInfo{{ID: "mimo-v2.5", OwnedBy: "mimo"}},
		KeyFingerprint: "mimo-key",
		BaseURLHash:    "mimo-url",
		ProfileVersion: "mimo:v1",
	}

	const workers = 24
	errCh := make(chan error, workers*2)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(2)
		go func() {
			defer wg.Done()
			thread := fmt.Sprintf("mixed-thread-%02d", worker)
			first, status, raw, err := doStressResponse(facade, map[string]any{
				"model":            "mimo-v2.5",
				"input":            "response-first-" + thread,
				"prompt_cache_key": "shared-control-cache",
			}, map[string]string{"x-codex-thread-id": thread})
			if err != nil {
				errCh <- fmt.Errorf("%s first decode: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s first status = %d body = %s", thread, status, raw)
				return
			}
			second, status, raw, err := doStressResponse(facade, map[string]any{
				"model":                "mimo-v2.5",
				"previous_response_id": first.ID,
				"input":                "response-second-" + thread,
				"prompt_cache_key":     "shared-control-cache",
			}, map[string]string{"x-codex-thread-id": thread})
			if err != nil {
				errCh <- fmt.Errorf("%s second decode: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s second status = %d body = %s", thread, status, raw)
				return
			}
			for _, want := range []string{"thread=" + thread, "history=1", "response-second-" + thread} {
				if !strings.Contains(second.OutputText, want) {
					errCh <- fmt.Errorf("%s second output missing %q: %q", thread, want, second.OutputText)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			thread := fmt.Sprintf("mixed-thread-%02d", worker)
			status, raw, err := doStressCompact(facade, map[string]any{
				"model": "mimo-v2.5",
				"input": []map[string]any{
					{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "compact-user-" + thread}}},
					{"type": "message", "role": "assistant", "content": []map[string]any{{"type": "output_text", "text": "compact-assistant-" + thread}}},
					{"type": "function_call", "call_id": "call_" + thread, "name": "exec_command", "arguments": `{"cmd":"pwd"}`},
					{"type": "function_call_output", "call_id": "call_" + thread, "output": "workspace-" + thread},
				},
			}, map[string]string{"x-codex-thread-id": thread})
			if err != nil {
				errCh <- fmt.Errorf("%s compact request: %w", thread, err)
				return
			}
			if status != http.StatusOK {
				errCh <- fmt.Errorf("%s compact status = %d body = %s", thread, status, raw)
				return
			}
			for _, want := range []string{"compact-user-" + thread, "compact-assistant-" + thread, "workspace-" + thread, "thread=" + thread} {
				if !strings.Contains(raw, want) {
					errCh <- fmt.Errorf("%s compact body missing %q: %s", thread, want, raw)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFacadeStressParallelLongConversationRoundsWithCache(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "responses.sqlite"), SQLiteStoreOptions{MaxRecords: 500})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	facade := &Facade{
		Adapter:        runtimeEchoAdapter{},
		Store:          store,
		ProviderID:     "deepseek",
		DefaultModel:   "deepseek-v4-flash",
		Models:         []ModelInfo{{ID: "deepseek-v4-flash", OwnedBy: "deepseek"}},
		KeyFingerprint: "deepseek-key",
		BaseURLHash:    "deepseek-url",
		ProfileVersion: "deepseek:v1",
	}

	const workers = 12
	const rounds = 3
	previous := make([]string, workers)
	for round := 1; round <= rounds; round++ {
		errCh := make(chan error, workers)
		next := make([]string, workers)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			worker, round := worker, round
			wg.Add(1)
			go func() {
				defer wg.Done()
				thread := fmt.Sprintf("parallel-long-%02d", worker)
				input := fmt.Sprintf("%s-round-%d", thread, round)
				body := map[string]any{
					"model":            "deepseek-v4-flash",
					"input":            input,
					"prompt_cache_key": "shared-live-cache-key",
				}
				if previous[worker] != "" {
					body["previous_response_id"] = previous[worker]
				}
				response, status, raw, err := doStressResponse(facade, body, map[string]string{"x-codex-thread-id": thread})
				if err != nil {
					errCh <- fmt.Errorf("%s round %d decode: %w", thread, round, err)
					return
				}
				if status != http.StatusOK {
					errCh <- fmt.Errorf("%s round %d status = %d body = %s", thread, round, status, raw)
					return
				}
				for _, want := range []string{
					"thread=" + thread,
					fmt.Sprintf("history=%d", round-1),
					"input=" + input,
				} {
					if !strings.Contains(response.OutputText, want) {
						errCh <- fmt.Errorf("%s round %d output missing %q: %q", thread, round, want, response.OutputText)
						return
					}
				}
				if response.Usage == nil || response.Usage.CachedTokens != round-1 {
					errCh <- fmt.Errorf("%s round %d usage = %#v, want cached %d", thread, round, response.Usage, round-1)
					return
				}
				next[worker] = response.ID
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		previous = next
	}

	_, status, raw, err := doStressResponse(facade, map[string]any{
		"model":                "deepseek-v4-flash",
		"previous_response_id": previous[0],
		"input":                "wrong-thread-resume",
		"prompt_cache_key":     "shared-live-cache-key",
	}, map[string]string{"x-codex-thread-id": "parallel-long-01"})
	if err != nil {
		t.Fatalf("wrong-thread decode: %v", err)
	}
	if status != http.StatusConflict || !strings.Contains(raw, ErrScopeMismatch.Error()) {
		t.Fatalf("wrong-thread status = %d body = %s, want scope mismatch", status, raw)
	}
}

type scopedEchoAdapter struct{}

func (scopedEchoAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent)
	go func() {
		defer close(ch)
		text := "reply:" + req.Scope.Thread + ":" + req.InputText
		mid := len(text) / 2
		events := []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: text[:mid]},
			{Kind: ProviderEventTextDelta, Delta: text[mid:]},
			{Kind: ProviderEventUsage, Usage: &Usage{InputTokens: len(req.InputText), OutputTokens: len(text), TotalTokens: len(req.InputText) + len(text)}},
			{Kind: ProviderEventDone},
		}
		for _, event := range events {
			select {
			case <-ctx.Done():
				return
			case ch <- event:
			}
		}
	}()
	return ch, nil
}

type scopedToolCallAdapter struct{}

func (scopedToolCallAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent)
	go func() {
		defer close(ch)
		arguments := fmt.Sprintf(`{"thread":%q,"input":%q}`, req.Scope.Thread, req.InputText)
		mid := len(arguments) / 2
		events := []ProviderEvent{
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: "call_" + req.Scope.Thread, Name: "scoped_tool", ArgumentsDelta: arguments[:mid]}},
			{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: arguments[mid:]}},
			{Kind: ProviderEventDone},
		}
		for _, event := range events {
			select {
			case <-ctx.Done():
				return
			case ch <- event:
			}
		}
	}()
	return ch, nil
}

type runtimeEchoAdapter struct{}

func (runtimeEchoAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent)
	go func() {
		defer close(ch)
		images := 0
		for _, message := range req.Messages {
			for _, part := range message.ContentParts {
				if part.Type == "image_url" && part.ImageURL != "" {
					images++
				}
			}
		}
		text := fmt.Sprintf(
			"reply:%s:%s:key=%s:thread=%s:history=%d:messages=%d:tools=%d:images=%d:reasoning=%s:input=%s",
			req.Scope.Provider,
			req.Model,
			req.Scope.KeyFingerprint,
			req.Scope.Thread,
			len(req.History),
			len(req.Messages),
			len(req.Tools),
			images,
			req.ReasoningEffort,
			req.InputText,
		)
		mid := len(text) / 2
		for _, event := range []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: text[:mid]},
			{Kind: ProviderEventTextDelta, Delta: text[mid:]},
			{Kind: ProviderEventUsage, Usage: &Usage{InputTokens: len(req.InputText), OutputTokens: len(text), TotalTokens: len(req.InputText) + len(text), CachedTokens: len(req.History)}},
			{Kind: ProviderEventDone},
		} {
			select {
			case <-ctx.Done():
				return
			case ch <- event:
			}
		}
	}()
	return ch, nil
}

type reasoningReplayStressAdapter struct{}

func (reasoningReplayStressAdapter) Stream(ctx context.Context, req ProviderRequest) (<-chan ProviderEvent, error) {
	ch := make(chan ProviderEvent)
	go func() {
		defer close(ch)
		if strings.HasPrefix(req.InputText, "first:") {
			callID := "call_" + req.Scope.Thread
			events := []ProviderEvent{
				{Kind: ProviderEventReasoningDelta, Delta: "reason:" + req.Scope.Thread},
				{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ID: callID, Name: "exec_command", ArgumentsDelta: `{"cmd":"`}},
				{Kind: ProviderEventToolCallDelta, ToolCall: &ProviderToolCallDelta{Index: 0, ArgumentsDelta: `pwd"}`}},
				{Kind: ProviderEventDone},
			}
			for _, event := range events {
				select {
				case <-ctx.Done():
					return
				case ch <- event:
				}
			}
			return
		}
		callID := ""
		reasoning := ""
		for _, message := range req.Messages {
			if message.Role != "assistant" || len(message.ToolCalls) == 0 {
				continue
			}
			callID = message.ToolCalls[0].ID
			reasoning = message.ReasoningContent
			break
		}
		text := "replay:" + req.Scope.Thread + ":" + callID + ":" + reasoning
		events := []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: text},
			{Kind: ProviderEventDone},
		}
		for _, event := range events {
			select {
			case <-ctx.Done():
				return
			case ch <- event:
			}
		}
	}()
	return ch, nil
}

func doStressResponse(facade *Facade, body any, headers map[string]string) (responseObject, int, string, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return responseObject{}, 0, "", err
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(raw)))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)
	bodyText := rec.Body.String()
	if rec.Code != http.StatusOK {
		return responseObject{}, rec.Code, bodyText, nil
	}
	var response responseObject
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		return responseObject{}, rec.Code, bodyText, err
	}
	return response, rec.Code, bodyText, nil
}

func doStressCompact(facade *Facade, body any, headers map[string]string) (int, string, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, "", err
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(string(raw)))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	facade.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String(), nil
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
