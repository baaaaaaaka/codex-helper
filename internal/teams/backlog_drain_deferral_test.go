package teams

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestPollDefersPlainContinuationWhenBacklogDrainIsDisabled(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	const chatID = "chat-backlog-deferred"
	const continuation = "/chats/chat-backlog-deferred/messages?$skiptoken=old-backlog"
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID:           chatID,
			Seeded:           true,
			PollState:        inboundPollStateCold,
			NextPollAt:       now.Add(-time.Minute),
			LastActivityAt:   now.Add(-5 * time.Hour),
			ContinuationPath: continuation,
			PollRevision:     17,
			UpdatedAt:        now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed deferred continuation: %v", err)
	}

	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	handled, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
		AllowBacklogDrain: false,
		MaxBacklogActions: 1,
	}, func(context.Context, ChatMessage, string) error {
		t.Fatal("deferred continuation was handled despite backlog drain being disabled")
		return nil
	})
	if err != nil {
		t.Fatalf("defer plain continuation: %v", err)
	}
	if handled {
		t.Fatal("deferred continuation reported handled work")
	}

	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("load deferred continuation: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != continuation {
		t.Fatalf("deferred continuation path = %q, want %q", poll.ContinuationPath, continuation)
	}
	if !poll.NextPollAt.After(now) {
		t.Fatalf("deferred continuation next poll = %s, want a future retry", poll.NextPollAt)
	}
	if poll.PollRevision <= 17 {
		t.Fatalf("deferred continuation poll revision = %d, want durable schedule change", poll.PollRevision)
	}
}

func TestPollBacklogDeferralSurvivesRestartAndWakesWhenDrainIsAllowed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	const chatID = "chat-backlog-restart"
	const continuation = "/chats/chat-backlog-restart/messages?$skiptoken=restart-backlog"
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID:           chatID,
			Seeded:           true,
			PollState:        inboundPollStateCold,
			NextPollAt:       now.Add(-time.Minute),
			LastActivityAt:   now.Add(-5 * time.Hour),
			ContinuationPath: continuation,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed restart continuation: %v", err)
	}
	first := newBridgeTestBridge(nil, store, &recordingExecutor{})
	if _, err := first.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
		AllowBacklogDrain: false,
	}, nil); err != nil {
		t.Fatalf("defer before restart: %v", err)
	}
	deferred, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok || deferred.ContinuationPath != continuation || !deferred.NextPollAt.After(now) {
		t.Fatalf("durable deferral before restart = %#v ok=%v err=%v", deferred, ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.RequestURI() != continuation {
			t.Errorf("restart continuation request = %q, want %q", r.URL.RequestURI(), continuation)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	reopened, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened store: %v", err)
		}
	})
	second := newBridgeTestBridge(graph, reopened, &recordingExecutor{})
	if _, err := second.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
		AllowBacklogDrain: true,
	}, nil); err != nil {
		t.Fatalf("resume after restart: %v", err)
	}
	resumed, ok, err := reopened.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("load resumed continuation: ok=%v err=%v", ok, err)
	}
	if requests != 1 || resumed.ContinuationPath != "" {
		t.Fatalf("restart resume requests=%d poll=%#v, want one continuation and cleared frontier", requests, resumed)
	}
}

func TestPollBacklogDrainDisabledSafetyFrontierMatrix(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	const continuation = "/chats/chat-frontier-matrix/messages?$skiptoken=old"

	tests := []struct {
		name             string
		poll             teamstore.ChatPollState
		running          bool
		wantRequests     int
		wantContinuation string
		wantPending      bool
		wantGap          bool
		wantAttempt      bool
		minDeferral      time.Duration
	}{
		{
			name: "plain-cold-continuation-is-deferred",
			poll: teamstore.ChatPollState{
				PollState:        inboundPollStateCold,
				ContinuationPath: continuation,
				PollRevision:     10,
				NextPollAt:       now.Add(-time.Minute),
			},
			wantContinuation: continuation,
			minDeferral:      inboundPollColdInterval,
		},
		{
			name: "plain-running-continuation-is-deferred",
			poll: teamstore.ChatPollState{
				PollState:        inboundPollStateRunning,
				ContinuationPath: continuation,
				PollRevision:     10,
				NextPollAt:       now.Add(-time.Minute),
			},
			running:          true,
			wantContinuation: continuation,
			minDeferral:      inboundPollRunningInterval,
		},
		{
			name: "pending-page-is-not-deferred",
			poll: func() teamstore.ChatPollState {
				page := &teamstore.ChatPollPendingPage{
					ChatID:      "chat-frontier-matrix",
					RequestPath: "/chats/chat-frontier-matrix/messages?$top=20",
					Frontier:    pollFrontierHead,
				}
				page.ReceiptID = pendingPageReceiptID(page)
				return teamstore.ChatPollState{
					PollState:    inboundPollStateWarm,
					PendingPage:  page,
					PollRevision: 10,
					NextPollAt:   now.Add(-time.Minute),
				}
			}(),
			// The pending page is intentionally processed, not deferred; the
			// successful empty receipt is cleared without a Graph request.
			wantPending: false,
		},
		{
			name: "directional-gap-is-not-deferred",
			poll: teamstore.ChatPollState{
				PollState:    inboundPollStateWarm,
				PollRevision: 10,
				NextPollAt:   now.Add(-time.Minute),
				Gap: &teamstore.ChatPollGap{
					Epoch:        1,
					Kind:         "unverified-continuation",
					RecoveryPath: "/chats/chat-frontier-matrix/messages?$skiptoken=gap",
					OpenedAt:     now.Add(-time.Minute),
				},
			},
			wantRequests: 1,
			wantGap:      true,
		},
		{
			name: "active-attempt-is-not-deferred",
			poll: teamstore.ChatPollState{
				PollState:        inboundPollStateWarm,
				ContinuationPath: continuation,
				PollRevision:     10,
				NextPollAt:       now.Add(-time.Minute),
			},
			wantContinuation: continuation,
			wantAttempt:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			chatID := "chat-frontier-matrix"
			poll := tc.poll
			poll.ChatID = chatID
			if poll.PendingPage != nil {
				poll.PendingPage.ChatID = chatID
				poll.PendingPage.ReceiptID = pendingPageReceiptID(poll.PendingPage)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ChatPolls[chatID] = poll
				return nil
			}); err != nil {
				t.Fatalf("seed frontier matrix state: %v", err)
			}

			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
			}))
			t.Cleanup(server.Close)
			graph := &GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 0,
				sleep:      func(context.Context, time.Duration) error { return nil },
				jitter:     func(d time.Duration) time.Duration { return d },
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			if tc.wantAttempt {
				attempt, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
					ChatID:             chatID,
					Owner:              "existing-poll-owner",
					ProcessIncarnation: "existing-poll-process",
					ExpectedFrontier:   pollFrontierIdentity(pollFrontierContinuation, continuation),
					Now:                now,
					TTL:                time.Minute,
				})
				if err != nil || !acquired || attempt.Attempt == nil {
					t.Fatalf("seed active attempt: acquired=%v attempt=%#v err=%v", acquired, attempt.Attempt, err)
				}
			}

			before, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("load matrix state: ok=%v err=%v", ok, err)
			}
			handled := 0
			if _, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, tc.running, before, true, pollChatWithRoleOptions{
				AllowBacklogDrain:        false,
				MaxBacklogActions:        1,
				RecoverStaleContinuation: true,
			}, func(context.Context, ChatMessage, string) error {
				handled++
				return nil
			}); err != nil {
				t.Fatalf("matrix poll: %v", err)
			}

			got, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("load matrix result: ok=%v err=%v", ok, err)
			}
			if requests != tc.wantRequests {
				t.Fatalf("Graph requests = %d, want %d", requests, tc.wantRequests)
			}
			if handled != 0 {
				t.Fatalf("handler calls = %d, want zero for this safety matrix", handled)
			}
			if got.ContinuationPath != tc.wantContinuation {
				t.Fatalf("continuation = %q, want %q", got.ContinuationPath, tc.wantContinuation)
			}
			if (got.PendingPage != nil) != tc.wantPending {
				t.Fatalf("pending page present = %v, want %v: %#v", got.PendingPage != nil, tc.wantPending, got)
			}
			if (got.Gap != nil) != tc.wantGap {
				t.Fatalf("gap present = %v, want %v: %#v", got.Gap != nil, tc.wantGap, got)
			}
			if (got.Attempt != nil) != tc.wantAttempt {
				t.Fatalf("attempt present = %v, want %v: %#v", got.Attempt != nil, tc.wantAttempt, got)
			}
			if tc.minDeferral > 0 {
				if got.PollRevision <= before.PollRevision {
					t.Fatalf("deferred poll revision = %d, want > %d", got.PollRevision, before.PollRevision)
				}
				if !got.NextPollAt.After(now.Add(tc.minDeferral / 2)) {
					t.Fatalf("deferred next poll = %s, want at least approximately %s in the future", got.NextPollAt, tc.minDeferral)
				}
			} else if tc.wantAttempt && got.PollRevision != before.PollRevision {
				t.Fatalf("active-attempt poll revision changed from %d to %d", before.PollRevision, got.PollRevision)
			}
		})
	}
}

func TestPollDeferredContinuationRecoversAfterTransientGraphFailure(t *testing.T) {
	for _, tc := range []struct {
		name            string
		statusCode      int
		failureRequests int
	}{
		{name: "429", statusCode: http.StatusTooManyRequests, failureRequests: 1},
		// A 429 keeps its provider Retry-After schedule, while still consuming
		// the finite continuation-failure budget so a persistent throttle cannot
		// hold one opaque frontier forever. Return the full retry budget for 5xx
		// failures before surfacing the transient failure to the durable frontier.
		{name: "503", statusCode: http.StatusServiceUnavailable, failureRequests: defaultGraphRetries + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			const chatID = "chat-deferred-transient"
			const continuation = "/chats/chat-deferred-transient/messages?$skiptoken=transient"
			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ChatPolls[chatID] = teamstore.ChatPollState{
					ChatID:           chatID,
					Seeded:           true,
					PollState:        inboundPollStateCold,
					NextPollAt:       now.Add(-time.Minute),
					ContinuationPath: continuation,
					PollRevision:     11,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed transient continuation: %v", err)
			}

			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if requests <= tc.failureRequests {
					if r.URL.RequestURI() != continuation {
						t.Errorf("transient request = %q, want %q", r.URL.RequestURI(), continuation)
					}
					if tc.statusCode == http.StatusTooManyRequests {
						w.Header().Set("Retry-After", "0")
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte(`{"error":{"code":"temporary","message":"temporary Graph failure"}}`))
					return
				}
				if r.URL.RequestURI() != continuation {
					t.Errorf("recovery request = %q, want %q", r.URL.RequestURI(), continuation)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
			}))
			t.Cleanup(server.Close)
			graph := &GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 0,
				sleep:      func(context.Context, time.Duration) error { return nil },
				jitter:     func(d time.Duration) time.Duration { return d },
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

			if _, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
				AllowBacklogDrain: false,
			}, nil); err != nil {
				t.Fatalf("initial durable deferral: %v", err)
			}
			if requests != 0 {
				t.Fatalf("Graph requests during durable deferral = %d, want zero", requests)
			}

			if _, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
				AllowBacklogDrain: true,
			}, nil); err == nil {
				t.Fatal("transient Graph failure unexpectedly succeeded")
			}
			failed, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("load failed continuation: ok=%v err=%v", ok, err)
			}
			if failed.ContinuationPath != continuation || failed.Attempt != nil || failed.LastError == "" || !failed.NextPollAt.After(now) {
				t.Fatalf("transient failure lost retryable frontier: %#v", failed)
			}
			if tc.statusCode == http.StatusTooManyRequests && failed.ContinuationFailureCount != 1 {
				t.Fatalf("429 continuation failure budget = %d, want one recorded failure: %#v", failed.ContinuationFailureCount, failed)
			}

			if _, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
				AllowBacklogDrain: true,
			}, nil); err != nil {
				t.Fatalf("continuation recovery after transient failure: %v", err)
			}
			recovered, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("load recovered continuation: ok=%v err=%v", ok, err)
			}
			if requests != tc.failureRequests+1 || recovered.ContinuationPath != "" || recovered.Gap != nil || recovered.Attempt != nil {
				t.Fatalf("transient continuation recovery requests=%d poll=%#v, want %d requests and cleared frontier", requests, recovered, tc.failureRequests+1)
			}
		})
	}
}

func TestPollBacklogDeferralSurvivesSQLiteRestartAndWakesWhenDrainIsAllowed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("open SQLite restart store: %v", err)
	}
	const chatID = "chat-backlog-sqlite-restart"
	const continuation = "/chats/chat-backlog-sqlite-restart/messages?$skiptoken=sqlite-restart"
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID:           chatID,
			Seeded:           true,
			PollState:        inboundPollStateCold,
			NextPollAt:       now.Add(-time.Minute),
			ContinuationPath: continuation,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed SQLite restart continuation: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate SQLite restart store: %v", err)
	}
	first := newBridgeTestBridge(nil, store, &recordingExecutor{})
	if _, err := first.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
		AllowBacklogDrain: false,
	}, nil); err != nil {
		t.Fatalf("defer SQLite continuation: %v", err)
	}
	deferred, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok || deferred.ContinuationPath != continuation || !deferred.NextPollAt.After(now) {
		t.Fatalf("SQLite durable deferral = %#v ok=%v err=%v", deferred, ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close SQLite restart store: %v", err)
	}

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.RequestURI() != continuation {
			t.Errorf("SQLite restart request = %q, want %q", r.URL.RequestURI(), continuation)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	reopened, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("reopen SQLite restart store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	second := newBridgeTestBridge(graph, reopened, &recordingExecutor{})
	if _, err := second.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
		AllowBacklogDrain: true,
	}, nil); err != nil {
		t.Fatalf("wake SQLite continuation after restart: %v", err)
	}
	recovered, ok, err := reopened.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("load SQLite recovered continuation: ok=%v err=%v", ok, err)
	}
	if requests != 1 || recovered.ContinuationPath != "" || recovered.Attempt != nil || recovered.Gap != nil {
		t.Fatalf("SQLite restart recovery requests=%d poll=%#v, want one request and cleared frontier", requests, recovered)
	}
}
