package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestPollFrontierFailureScopePreservesProcessBoundary(t *testing.T) {
	underlying := errors.New("durable poll write failed")
	cases := []struct {
		name        string
		err         error
		processWide bool
	}{
		{name: "chat", err: &pollScopedError{scope: pollFailureChat, err: underlying}},
		{name: "control", err: pollControlFailure(underlying), processWide: true},
		{name: "store", err: pollStoreFailure(underlying), processWide: true},
		{name: "lease", err: pollLeaseFailure(underlying), processWide: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isProcessWidePollFailure(tc.err); got != tc.processWide {
				t.Fatalf("process-wide=%v, want %v for %v", got, tc.processWide, tc.err)
			}
			if !errors.Is(tc.err, underlying) {
				t.Fatalf("scoped error %v no longer unwraps to underlying error", tc.err)
			}
		})
	}
	if isProcessWidePollFailure(nil) {
		t.Fatal("nil poll error was classified as process-wide")
	}
}

func TestPollFrontierPFirstNeverReadsHeadAndPromotesLegacyDeferred(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	oldPath := "/chats/chat-1/messages?$skiptoken=old"
	deferredPath := "/chats/chat-1/messages?$skiptoken=deferred"
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.String()
		mu.Lock()
		requests = append(requests, path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "old":
			writePollFrontierPage(t, w, []ChatMessage{bridgePollMessage("frontier-old", now.Format(time.RFC3339Nano), "old")}, "")
		case "deferred":
			writePollFrontierPage(t, w, []ChatMessage{bridgePollMessage("frontier-deferred", now.Add(time.Minute).Format(time.RFC3339Nano), "deferred")}, "")
		case "":
			t.Fatalf("head request issued while continuation frontier existed: %s", path)
		default:
			http.Error(w, "unexpected path", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-1"] = teamstore.ChatPollState{
			ChatID:                   "chat-1",
			Seeded:                   true,
			PollState:                inboundPollStateWarm,
			NextPollAt:               now,
			LastActivityAt:           now,
			LastModifiedCursor:       now.Add(-time.Hour),
			ContinuationPath:         oldPath,
			DeferredContinuationPath: deferredPath,
			FrontierEpoch:            1,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed frontiers: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}
	if _, err := bridge.pollChat(ctx, "chat-1", 20, handle); err != nil {
		t.Fatalf("old continuation poll: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read after old continuation: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != deferredPath || poll.DeferredContinuationPath != "" {
		t.Fatalf("legacy deferred frontier was not promoted after P drained: %#v", poll)
	}
	if got := strings.Join(handled, ","); got != "old" {
		t.Fatalf("first handled messages = %q, want old", got)
	}
	if _, err := bridge.pollChat(ctx, "chat-1", 20, handle); err != nil {
		t.Fatalf("deferred continuation poll: %v", err)
	}
	if got := strings.Join(handled, ","); got != "old,deferred" {
		t.Fatalf("handled messages = %q, want both frontiers", got)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 || !strings.Contains(gotRequests[0], "old") || !strings.Contains(gotRequests[1], "deferred") {
		t.Fatalf("request trace = %v, want old then deferred and no head", gotRequests)
	}
}

func TestPollFrontierNormalizationFencesActiveAttempt(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			path := "/chats/chat-normalize/messages?$skiptoken=opaque"
			if _, _, err := store.UpdateChatPoll(ctx, "chat-normalize", func(poll *teamstore.ChatPollState) error {
				poll.Seeded = true
				poll.PollState = inboundPollStateWarm
				poll.ContinuationPath = path
				poll.DeferredContinuationPath = path
				return nil
			}); err != nil {
				t.Fatalf("seed duplicate frontier: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate duplicate frontier: %v", err)
				}
			}
			before, ok, err := store.ChatPoll(ctx, "chat-normalize")
			if err != nil || !ok {
				t.Fatalf("read duplicate frontier: ok=%v err=%v", ok, err)
			}
			attempt, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
				ChatID:                  "chat-normalize",
				Owner:                   "owner-a",
				ProcessIncarnation:      "process-a",
				LeaseGeneration:         1,
				ExpectedPollRevision:    before.PollRevision,
				HasExpectedPollRevision: true,
				ExpectedFrontier:        pollFrontierIdentity(pollFrontierContinuation, path),
				Now:                     time.Now().UTC(),
			})
			if err != nil || !acquired || attempt.Attempt == nil {
				t.Fatalf("begin normalization attempt: acquired=%v attempt=%#v err=%v", acquired, attempt.Attempt, err)
			}
			oldAttemptID := attempt.Attempt.ID
			normalized, changed, err := normalizePollFrontier(ctx, store, "chat-normalize")
			if err != nil || !changed || normalized.DeferredContinuationPath != "" || normalized.Attempt != nil {
				t.Fatalf("normalize did not fence active attempt: changed=%v state=%#v err=%v", changed, normalized, err)
			}
			if _, committed, err := store.CommitChatPollAttempt(ctx, "chat-normalize", oldAttemptID, attempt.PollRevision, func(poll *teamstore.ChatPollState) error {
				poll.LastError = "stale normalization writer must not commit"
				return nil
			}); err != nil || committed {
				t.Fatalf("stale normalization attempt commit: committed=%v err=%v", committed, err)
			}
		})
	}
}

func TestPollFrontierPendingPageReplaysWithoutGraphAndDoesNotSkipTail(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		count := requests
		mu.Unlock()
		if r.URL.Query().Get("$skiptoken") != "" {
			t.Fatalf("continuation fetched before pending page was committed: %s", r.URL.String())
		}
		if count != 1 {
			t.Fatalf("unexpected second head request: %d", count)
		}
		first := bridgePollMessage("pending-first", now.Format(time.RFC3339Nano), "first")
		second := bridgePollMessage("pending-second", now.Add(time.Minute).Format(time.RFC3339Nano), "second")
		w.Header().Set("Content-Type", "application/json")
		writePollFrontierPage(t, w, []ChatMessage{first, second}, "/chats/chat-1/messages?$skiptoken=after-page")
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", now.Add(-time.Hour), true, false, 0); err != nil {
		t.Fatalf("seed poll: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}
	if _, err := bridge.pollChat(ctx, "chat-1", 20, handle); err != nil {
		t.Fatalf("first page quantum: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok || poll.PendingPage == nil {
		t.Fatalf("first quantum did not retain pending page: ok=%v err=%v poll=%#v", ok, err, poll)
	}
	if got := strings.Join(handled, ","); got != "pending-first" {
		t.Fatalf("first quantum handled = %q, want first only", got)
	}
	if _, err := bridge.pollChat(ctx, "chat-1", 20, handle); err != nil {
		t.Fatalf("pending page replay: %v", err)
	}
	mu.Lock()
	gotRequests := requests
	mu.Unlock()
	if gotRequests != 1 {
		t.Fatalf("pending replay issued %d Graph requests, want 1", gotRequests)
	}
	if got := strings.Join(handled, ","); got != "pending-first,pending-second" {
		t.Fatalf("replayed page handled = %q, want both records", got)
	}
	poll, ok, err = store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok || poll.PendingPage != nil || poll.ContinuationPath != "/chats/chat-1/messages?$skiptoken=after-page" {
		t.Fatalf("completed pending page state = %#v ok=%v err=%v", poll, ok, err)
	}
}

func TestPollFrontierBaselineReceiptReplayRemainsHistorical(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	first := bridgePollMessage("baseline-first", now.Format(time.RFC3339Nano), "old first")
	second := bridgePollMessage("baseline-second", now.Add(time.Minute).Format(time.RFC3339Nano), "old second")
	page, err := pendingPageFromWindow("chat-baseline", "/chats/chat-baseline/messages?$top=20", pollFrontierHead, 1, MessageWindow{
		Messages:  []ChatMessage{first, second},
		Truncated: true,
		NextPath:  "/chats/chat-baseline/messages?$skiptoken=older",
	}, true)
	if err != nil {
		t.Fatalf("build baseline receipt: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-baseline"] = teamstore.ChatPollState{
			ChatID:             "chat-baseline",
			PollState:          inboundPollStateWarm,
			LastModifiedCursor: now.Add(-time.Hour),
			PendingPage:        page,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed baseline receipt: %v", err)
	}
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	// Simulate a registry projection that was partially saved before the
	// process crashed. Baseline-ness must come from the receipt, not this
	// mutable projection.
	bridge.markRegistrySeen("chat-baseline", first.ID)
	handled := 0
	if got, err := bridge.pollChat(ctx, "chat-baseline", 20, func(context.Context, ChatMessage, string) error {
		handled++
		return nil
	}); err != nil {
		t.Fatalf("replay baseline receipt: %v", err)
	} else if got {
		t.Fatal("baseline receipt was reported as live work")
	}
	if handled != 0 {
		t.Fatalf("baseline handler calls = %d, want 0", handled)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-baseline")
	if err != nil || !ok {
		t.Fatalf("read baseline state: ok=%v err=%v", ok, err)
	}
	if !poll.Seeded || poll.PendingPage != nil || poll.Attempt != nil {
		t.Fatalf("baseline receipt was not committed: %#v", poll)
	}
	if poll.ContinuationPath != "" || poll.DeferredContinuationPath != "" {
		t.Fatalf("baseline receipt retained an older Graph page: %#v", poll)
	}
	if !poll.LastModifiedCursor.Equal(messageModifiedTime(second)) {
		t.Fatalf("baseline cursor = %s, want %s", poll.LastModifiedCursor, messageModifiedTime(second))
	}
}

func TestPollFrontierPendingPagePersistsOversizedDispositionAndReplaysLaterText(t *testing.T) {
	now := time.Now().UTC()
	oversized := bridgePollMessage("oversized-record", now.Format(time.RFC3339Nano), "")
	oversized.oversizedForPoll = true
	later := bridgePollMessage("ordinary-record", now.Add(time.Minute).Format(time.RFC3339Nano), "ordinary text")
	page, err := pendingPageFromWindow("chat-page", "/chats/chat-page/messages?$top=2", pollFrontierHead, 1, MessageWindow{
		Messages:  []ChatMessage{oversized, later},
		Truncated: true,
		NextPath:  "/chats/chat-page/messages?$skiptoken=next",
	}, false)
	if err != nil {
		t.Fatalf("stage oversized page: %v", err)
	}
	if len(page.Dispositions) != 2 || page.Dispositions[0] != "oversized_record" || page.Dispositions[1] != "received" {
		t.Fatalf("page dispositions = %#v, want oversized_record and received", page.Dispositions)
	}
	replayed, err := pendingPageToWindow(page)
	if err != nil {
		t.Fatalf("replay oversized page: %v", err)
	}
	if len(replayed.Messages) != 2 || !replayed.Messages[0].oversizedForPoll || !strings.Contains(replayed.Messages[1].Body.Content, "ordinary text") {
		t.Fatalf("replayed page = %#v, want explicit oversized marker and later text", replayed)
	}
	page.Dispositions[0] = "unknown"
	page.ReceiptID = pendingPageReceiptID(page)
	if _, err := pendingPageToWindow(page); err == nil || !strings.Contains(err.Error(), "unknown disposition") {
		t.Fatalf("unknown page disposition error = %v, want explicit validation failure", err)
	}
}

func TestPollFrontierValidatesPendingReceiptPathAndFingerprint(t *testing.T) {
	now := time.Now().UTC()
	page, err := pendingPageFromWindow("chat-path", "/chats/chat-path/messages?$skiptoken=opaque", pollFrontierContinuation, 2, MessageWindow{
		Messages: []ChatMessage{bridgePollMessage("path-record", now.Format(time.RFC3339Nano), "path")},
	}, false)
	if err != nil {
		t.Fatalf("build path receipt: %v", err)
	}
	page.RequestFingerprint = pollPathFingerprint("/chats/another-chat/messages?$skiptoken=opaque")
	if _, err := pendingPageToWindow(page); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("tampered request fingerprint error = %v, want identity failure", err)
	}
	if !pollRequestPathBelongsToChat("chat-path", "/chats/chat-path/messages?$skiptoken=opaque") {
		t.Fatal("same-chat continuation was rejected")
	}
	if pollRequestPathBelongsToChat("chat-path", "/chats/another-chat/messages?$skiptoken=opaque") {
		t.Fatal("cross-chat continuation was accepted")
	}
}

func TestPollFrontierMalformedPendingPageMovesToGapInsteadOfLivelocking(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ChatPolls["chat-malformed-page"] = teamstore.ChatPollState{
					ChatID:    "chat-malformed-page",
					Seeded:    true,
					PollState: inboundPollStateWarm,
					PendingPage: &teamstore.ChatPollPendingPage{
						ReceiptID: "malformed-receipt",
						ChatID:    "chat-malformed-page",
						Frontier:  pollFrontierHead,
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed malformed page: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate malformed page: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			if _, err := bridge.pollChat(ctx, "chat-malformed-page", 20, func(context.Context, ChatMessage, string) error {
				return nil
			}); err == nil || !strings.Contains(err.Error(), "empty Graph request path") {
				t.Fatalf("malformed page poll error = %v, want empty-path diagnostic", err)
			}
			poll, ok, err := store.ChatPoll(ctx, "chat-malformed-page")
			if err != nil || !ok {
				t.Fatalf("read repaired malformed page: ok=%v err=%v", ok, err)
			}
			if poll.PendingPage != nil || poll.Gap == nil || poll.Gap.QuarantinedPage == nil {
				t.Fatalf("malformed page was not retired into gap evidence: %#v", poll)
			}
			if poll.Gap.QuarantinedPage.ReceiptID != "malformed-receipt" || poll.Attempt != nil {
				t.Fatalf("malformed page evidence/attempt = receipt=%q attempt=%#v", poll.Gap.QuarantinedPage.ReceiptID, poll.Attempt)
			}
			_, requestPath, _ := pollPageRequestForState("chat-malformed-page", 20, inboundPollRoleWork, poll)
			if requestPath == "" {
				t.Fatalf("gap recovery did not produce a new request path: %#v", poll)
			}
		})
	}
}

func TestPollFrontierGapRetainsUnreplayablePageEvidence(t *testing.T) {
	now := time.Now().UTC()
	page, err := pendingPageFromWindow("chat-gap-evidence", "/chats/chat-gap-evidence/messages?$skiptoken=opaque", pollFrontierContinuation, 1, MessageWindow{
		Messages: []ChatMessage{bridgePollMessage("opaque-record", now.Format(time.RFC3339Nano), "opaque")},
	}, false)
	if err != nil {
		t.Fatalf("build gap evidence page: %v", err)
	}
	poll := teamstore.ChatPollState{
		ChatID: "chat-gap-evidence", ContinuationPath: page.RequestPath, PendingPage: page,
		LastModifiedCursor: now.Add(-time.Hour),
	}
	openPollGap(&poll, "invalid-page", "receipt changed", page.RequestPath, now)
	if poll.PendingPage != nil || poll.Gap == nil || poll.Gap.QuarantinedPage == nil {
		t.Fatalf("gap did not retain unreplayable page: %#v", poll)
	}
	if poll.Gap.QuarantinedPage.ReceiptID != page.ReceiptID || len(poll.Gap.QuarantinedPage.Records) != 1 {
		t.Fatalf("retained gap evidence = %#v, want original receipt", poll.Gap.QuarantinedPage)
	}
}

func TestPollFrontierGapFailureRetainsPendingReceiptEvidence(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			now := time.Now().UTC()
			page, err := pendingPageFromWindow("chat-gap-failure", "/chats/chat-gap-failure/messages?$skiptoken=recovery", pollFrontierGap, 3, MessageWindow{
				Messages: []ChatMessage{bridgePollMessage("gap-receipt", now.Format(time.RFC3339Nano), "held")},
			}, false)
			if err != nil {
				t.Fatalf("build gap receipt: %v", err)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ChatPolls["chat-gap-failure"] = teamstore.ChatPollState{
					ChatID:             "chat-gap-failure",
					Seeded:             true,
					PollState:          inboundPollStateWarm,
					LastModifiedCursor: now.Add(-time.Hour),
					ContinuationPath:   "",
					Gap: &teamstore.ChatPollGap{
						Epoch:          3,
						Kind:           "unverified-continuation",
						FrontierPath:   page.RequestPath,
						RecoveryPath:   page.RequestPath,
						SafeCursor:     now.Add(-time.Hour),
						RecoveryCursor: now.Add(-time.Hour),
						OpenedAt:       now,
					},
					PendingPage: page,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed gap failure state: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate gap failure state: %v", err)
				}
			}
			poll, ok, err := store.ChatPoll(ctx, "chat-gap-failure")
			if err != nil || !ok {
				t.Fatalf("read gap failure seed: ok=%v err=%v", ok, err)
			}
			attempt, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
				ChatID:                  "chat-gap-failure",
				Owner:                   "owner-a",
				ProcessIncarnation:      "process-a",
				LeaseGeneration:         4,
				ExpectedPollRevision:    poll.PollRevision,
				HasExpectedPollRevision: true,
				ExpectedFrontier:        pollFrontierIdentity(pollFrontierGap, page.RequestPath),
				ExpectedReceiptID:       page.ReceiptID,
				Now:                     now,
			})
			if err != nil || !acquired || attempt.Attempt == nil {
				t.Fatalf("begin gap failure attempt: acquired=%v attempt=%#v err=%v", acquired, attempt.Attempt, err)
			}
			committed, err := (&Bridge{store: store}).commitPollAttemptFailure(ctx, "chat-gap-failure", attempt.Attempt.ID, attempt.PollRevision, pollFrontierGap, page.RequestPath, errors.New("recovery page is no longer valid"), true, false)
			if err != nil || !committed {
				t.Fatalf("commit gap failure: committed=%v err=%v", committed, err)
			}
			poll, ok, err = store.ChatPoll(ctx, "chat-gap-failure")
			if err != nil || !ok || poll.PendingPage != nil || poll.Gap == nil || poll.Gap.QuarantinedPage == nil {
				t.Fatalf("gap failure lost receipt evidence: %#v ok=%v err=%v", poll, ok, err)
			}
			if poll.Gap.QuarantinedPage.ReceiptID != page.ReceiptID || poll.Gap.RecoveryPath != "" {
				t.Fatalf("gap failure evidence/frontier = %#v, want receipt retained and recovery path cleared", poll.Gap)
			}
			if poll.LastModifiedCursor.After(poll.Gap.SafeCursor) {
				t.Fatalf("gap failure advanced safe cursor: %#v", poll)
			}
		})
	}
}

func TestPollFrontierPersistedInboundWithoutTurnIsRetried(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			now := time.Now().UTC()
			msg := bridgePollMessage("persisted-without-turn", now.Format(time.RFC3339Nano), "retry this prompt")
			inbound := teamstore.InboundEvent{
				ID:             "inbound-persisted-without-turn",
				TeamsChatID:    "chat-persisted-without-turn",
				TeamsMessageID: msg.ID,
				Text:           "retry this prompt",
				TextHash:       normalizedTextHash("retry this prompt"),
				Source:         "teams",
				Status:         teamstore.InboundStatusPersisted,
				CreatedAt:      now.Add(-time.Minute),
				UpdatedAt:      now.Add(-time.Minute),
			}
			if _, created, err := store.PersistInbound(ctx, inbound); err != nil || !created {
				t.Fatalf("persist unqueued inbound: created=%v err=%v", created, err)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ChatPolls[inbound.TeamsChatID] = teamstore.ChatPollState{
					ChatID:             inbound.TeamsChatID,
					Seeded:             true,
					PollState:          inboundPollStateWarm,
					LastModifiedCursor: now.Add(-time.Hour),
					NextPollAt:         now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed persisted inbound poll: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate persisted inbound state: %v", err)
				}
			}
			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Content-Type", "application/json")
				writePollFrontierPage(t, w, []ChatMessage{msg}, "")
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
			handled := 0
			if got, err := bridge.pollChat(ctx, inbound.TeamsChatID, 20, func(_ context.Context, got ChatMessage, _ string) error {
				handled++
				if got.ID != msg.ID {
					t.Fatalf("handled message = %q, want %q", got.ID, msg.ID)
				}
				return nil
			}); err != nil {
				t.Fatalf("retry persisted inbound: %v", err)
			} else if !got {
				t.Fatal("retryable persisted inbound was not reported as handled")
			}
			if handled != 1 || requests != 1 {
				t.Fatalf("retryable inbound handled=%d Graph requests=%d, want 1/1", handled, requests)
			}
			poll, ok, err := store.ChatPoll(ctx, inbound.TeamsChatID)
			if err != nil || !ok || poll.PendingPage != nil || poll.Attempt != nil {
				t.Fatalf("retryable inbound final state = %#v ok=%v err=%v", poll, ok, err)
			}
		})
	}
}

func TestPollFrontierCrossChatContinuationOpensLocalGapBeforeGraph(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-owned"] = teamstore.ChatPollState{
			ChatID: "chat-owned", Seeded: true, PollState: inboundPollStateWarm,
			NextPollAt: now, LastModifiedCursor: now.Add(-time.Hour),
			ContinuationPath: "/chats/chat-other/messages?$skiptoken=foreign",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cross-chat continuation: %v", err)
	}
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	if _, err := bridge.pollChat(ctx, "chat-owned", 20, func(context.Context, ChatMessage, string) error {
		t.Fatal("cross-chat continuation reached a handler")
		return nil
	}); err == nil || !strings.Contains(err.Error(), "not owned by chat") {
		t.Fatalf("cross-chat poll error = %v, want local identity error", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-owned")
	if err != nil || !ok || poll.Gap == nil || poll.ContinuationPath != "" {
		t.Fatalf("cross-chat continuation state = %#v ok=%v err=%v", poll, ok, err)
	}
}

func TestPollFrontierOversizedUserRecordRefetchesBeforeHandling(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	large := bridgePollMessage("oversized-user", now.Format(time.RFC3339Nano), "large user request")
	large.Body.Content = "<p>" + strings.Repeat("x", maxGraphMessageRecordBytes) + "</p>"
	largeRaw, err := json.Marshal(large)
	if err != nil {
		t.Fatalf("marshal oversized user message: %v", err)
	}
	if len(largeRaw) <= maxGraphMessageRecordBytes {
		t.Fatalf("test message size = %d, want larger than list record bound", len(largeRaw))
	}
	listPayload, err := json.Marshal(map[string]any{"value": []json.RawMessage{largeRaw}})
	if err != nil {
		t.Fatalf("marshal oversized list page: %v", err)
	}
	var mu sync.Mutex
	listRequests := 0
	refetches := 0
	refetchAllowed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/messages/oversized-user") {
			mu.Lock()
			refetches++
			allowed := refetchAllowed
			mu.Unlock()
			if !allowed {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"code":"ServiceUnavailable","message":"temporary individual message outage"}}`))
				return
			}
			_, _ = w.Write(largeRaw)
			return
		}
		if r.URL.Path == "/chats/chat-oversized-user/messages" {
			mu.Lock()
			listRequests++
			mu.Unlock()
			_, _ = w.Write(listPayload)
			return
		}
		t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 1,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	if _, err := store.RecordChatPollSuccess(ctx, "chat-oversized-user", now.Add(-time.Hour), true, false, 0); err != nil {
		t.Fatalf("seed oversized-user poll: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, text string) error {
		if strings.TrimSpace(text) == "" {
			t.Fatal("oversized user message reached handler without recovered text")
		}
		handled = append(handled, msg.ID)
		return nil
	}
	if _, err := bridge.pollChat(ctx, "chat-oversized-user", 20, handle); err == nil {
		t.Fatal("oversized user poll unexpectedly hid individual fetch failure")
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-oversized-user")
	if err != nil || !ok || poll.PendingPage == nil {
		t.Fatalf("failed oversized refetch did not retain pending page: %#v ok=%v err=%v", poll, ok, err)
	}
	if bridge.registryHasSeenOrSentForPoll("chat-oversized-user", "oversized-user") {
		t.Fatal("failed oversized refetch marked the user message as seen")
	}
	mu.Lock()
	refetchAllowed = true
	mu.Unlock()
	if _, err := bridge.pollChat(ctx, "chat-oversized-user", 20, handle); err != nil {
		t.Fatalf("oversized user replay after refetch recovery: %v", err)
	}
	mu.Lock()
	gotRefetches := refetches
	gotListRequests := listRequests
	mu.Unlock()
	if gotRefetches != 3 {
		t.Fatalf("oversized user individual fetches = %d, want two failed attempts and one replay", gotRefetches)
	}
	if gotListRequests != 1 {
		t.Fatalf("oversized user list requests = %d, want one request and receipt replay", gotListRequests)
	}
	if got := strings.Join(handled, ","); got != "oversized-user" {
		t.Fatalf("handled oversized user messages = %q, want oversized-user", got)
	}
	poll, ok, err = store.ChatPoll(ctx, "chat-oversized-user")
	if err != nil || !ok || poll.PendingPage != nil || poll.Gap != nil {
		t.Fatalf("oversized user poll state = %#v ok=%v err=%v", poll, ok, err)
	}
}

func TestPollFrontierPermanentOversizedRecordIsolatedAndLaterRecordsContinue(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	large := bridgePollMessage("oversized-permanent", now.Format(time.RFC3339Nano), "large user request")
	large.Body.Content = "<p>" + strings.Repeat("x", maxGraphMessageRecordBytes) + "</p>"
	largeRaw, err := json.Marshal(large)
	if err != nil {
		t.Fatalf("marshal permanent oversized message: %v", err)
	}
	listPayload, err := json.Marshal(map[string]any{
		"value": []json.RawMessage{largeRaw, mustMarshalPollMessage(t, bridgePollMessage("after-permanent-oversized", now.Add(time.Minute).Format(time.RFC3339Nano), "later message"))},
	})
	if err != nil {
		t.Fatalf("marshal permanent oversized page: %v", err)
	}
	var mu sync.Mutex
	listRequests := 0
	refetches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/messages/oversized-permanent") {
			mu.Lock()
			refetches++
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"ItemNotFound","message":"attachment record unavailable"}}`))
			return
		}
		if r.URL.Path == "/chats/chat-permanent-oversized/messages" {
			mu.Lock()
			listRequests++
			mu.Unlock()
			_, _ = w.Write([]byte(listPayload))
			return
		}
		t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
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
	if _, err := store.RecordChatPollSuccess(ctx, "chat-permanent-oversized", now.Add(-time.Hour), true, false, 0); err != nil {
		t.Fatalf("seed permanent oversized poll: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}
	for attempt := 0; attempt < maxOversizedRecordRefetchAttempts; attempt++ {
		if _, err := bridge.pollChat(ctx, "chat-permanent-oversized", 20, handle); err == nil {
			t.Fatalf("permanent oversized refetch attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-permanent-oversized")
	if err != nil || !ok || poll.PendingPage == nil {
		t.Fatalf("permanent oversized page was not retained: %#v ok=%v err=%v", poll, ok, err)
	}
	if len(poll.PendingPage.Dispositions) != 2 || poll.PendingPage.Dispositions[0] != "oversized_record_quarantined" {
		t.Fatalf("permanent oversized disposition = %#v, want only first record quarantined", poll.PendingPage.Dispositions)
	}
	if poll.PendingPage.RefetchFailures[0] != maxOversizedRecordRefetchAttempts {
		t.Fatalf("permanent oversized refetch failures = %#v, want %d", poll.PendingPage.RefetchFailures, maxOversizedRecordRefetchAttempts)
	}
	if _, err := bridge.pollChat(ctx, "chat-permanent-oversized", 20, handle); err != nil {
		t.Fatalf("replay after permanent oversized quarantine: %v", err)
	}
	if got := strings.Join(handled, ","); got != "after-permanent-oversized" {
		t.Fatalf("handled permanent oversized page = %q, want later record only", got)
	}
	poll, ok, err = store.ChatPoll(ctx, "chat-permanent-oversized")
	if err != nil || !ok || poll.PendingPage != nil || len(poll.QuarantinedRecordIDs) != 1 || poll.QuarantinedRecordIDs[0] != "oversized-permanent" {
		t.Fatalf("permanent oversized recovery state = %#v ok=%v err=%v", poll, ok, err)
	}
	mu.Lock()
	gotListRequests := listRequests
	gotRefetches := refetches
	mu.Unlock()
	if gotListRequests != 1 || gotRefetches != maxOversizedRecordRefetchAttempts {
		t.Fatalf("permanent oversized request counts = list %d refetch %d, want list 1/refetch %d", gotListRequests, gotRefetches, maxOversizedRecordRefetchAttempts)
	}
}

func mustMarshalPollMessage(t *testing.T, msg ChatMessage) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal poll message %q: %v", msg.ID, err)
	}
	return raw
}

func TestPollFrontierExpiredContinuationUsesImmediateGapThenProcessesNewHead(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	oldPath := "/chats/chat-1/messages?$skiptoken=expired"
	newMessage := bridgePollMessage("after-gap", now.Add(time.Minute).Format(time.RFC3339Nano), "new head")
	var mu sync.Mutex
	var requests []string
	var recoveryTop string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.String())
		if r.URL.Query().Get("$skiptoken") != "expired" {
			recoveryTop = r.URL.Query().Get("$top")
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$skiptoken") == "expired" {
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"Gone","message":"continuation skiptoken expired"}}`)
			return
		}
		writePollFrontierPage(t, w, []ChatMessage{newMessage}, "")
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
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-1"] = teamstore.ChatPollState{
			ChatID:             "chat-1",
			Seeded:             true,
			PollState:          inboundPollStateWarm,
			NextPollAt:         now,
			LastActivityAt:     now,
			LastModifiedCursor: now.Add(-time.Hour),
			ContinuationPath:   oldPath,
			FrontierEpoch:      1,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed expired continuation: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	if _, err := bridge.pollChat(ctx, "chat-1", 20, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err == nil {
		t.Fatal("expired continuation unexpectedly succeeded")
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read gap state: ok=%v err=%v", ok, err)
	}
	if poll.Gap == nil || poll.ContinuationPath != "" || poll.PollState == inboundPollStateBlocked || !poll.BlockedUntil.IsZero() {
		t.Fatalf("expired continuation did not become a non-blocking gap: %#v", poll)
	}
	if _, err := bridge.pollChat(ctx, "chat-1", 20, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("gap recovery head: %v", err)
	}
	if got := strings.Join(handled, ","); got != "after-gap" {
		t.Fatalf("gap recovery handled = %q, want new head", got)
	}
	poll, ok, err = store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok || poll.Gap == nil || !poll.Gap.RecoveryCursor.After(poll.Gap.SafeCursor) {
		t.Fatalf("gap recovery cursor/proof = %#v ok=%v err=%v", poll, ok, err)
	}
	if poll.LastModifiedCursor.After(poll.Gap.SafeCursor) {
		t.Fatalf("safe cursor advanced across unresolved gap: %#v", poll)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 || !strings.Contains(gotRequests[len(gotRequests)-1], "%24filter") || recoveryTop != "1" {
		t.Fatalf("request trace = %v recovery top=%q, want one token failure then exact top=1 recovery head", gotRequests, recoveryTop)
	}
}

func TestPollFrontierGenericContinuationFailureUsesBoundedGap(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	continuation := "/chats/chat-generic/messages?$skiptoken=generic"
	var continuationRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$skiptoken") == "generic" {
			continuationRequests++
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, `{"error":{"code":"ServiceUnavailable","message":"temporary Graph outage"}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"value":[]}`)
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
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-generic"] = teamstore.ChatPollState{
			ChatID:             "chat-generic",
			Seeded:             true,
			PollState:          inboundPollStateWarm,
			NextPollAt:         now,
			LastActivityAt:     now,
			LastModifiedCursor: now.Add(-time.Hour),
			ContinuationPath:   continuation,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed generic continuation: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	for attempt := 0; attempt < continuationFailureBudget; attempt++ {
		if _, err := bridge.pollChat(ctx, "chat-generic", 20, func(context.Context, ChatMessage, string) error { return nil }); err == nil {
			t.Fatalf("generic continuation failure %d unexpectedly succeeded", attempt+1)
		}
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-generic")
	if err != nil || !ok {
		t.Fatalf("read generic continuation gap: ok=%v err=%v", ok, err)
	}
	wantRequests := continuationFailureBudget * (defaultGraphRetries + 1)
	if continuationRequests != wantRequests {
		t.Fatalf("generic continuation HTTP requests = %d, want %d (%d logical attempts with internal retries)", continuationRequests, wantRequests, continuationFailureBudget)
	}
	if poll.ContinuationPath != "" || poll.Gap == nil || poll.PollState == inboundPollStateBlocked || !poll.BlockedUntil.IsZero() {
		t.Fatalf("generic continuation did not become a non-blocking gap: %#v", poll)
	}
}

func TestPollFrontierChangingNextLinkWithSamePageOpensGapWithoutFollowingCycle(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	firstPath := "/chats/chat-cycle/messages?$skiptoken=first"
	secondPath := "/chats/chat-cycle/messages?$skiptoken=second"
	thirdPath := "/chats/chat-cycle/messages?$skiptoken=third"
	message := bridgePollMessage("cycle-message", now.Format(time.RFC3339Nano), "same page")
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.String()
		mu.Lock()
		requests = append(requests, path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "first":
			writePollFrontierPage(t, w, []ChatMessage{message}, secondPath)
		case "second":
			// Simulate Graph regenerating the opaque nextLink while returning
			// the same page. Following thirdPath would livelock this chat.
			writePollFrontierPage(t, w, []ChatMessage{message}, thirdPath)
		default:
			t.Fatalf("unexpected continuation request: %s", path)
		}
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-cycle"] = teamstore.ChatPollState{
			ChatID:             "chat-cycle",
			Seeded:             true,
			PollState:          inboundPollStateWarm,
			NextPollAt:         now,
			LastActivityAt:     now,
			LastModifiedCursor: now.Add(-time.Hour),
			ContinuationPath:   firstPath,
			FrontierEpoch:      1,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed changing-nextLink continuation: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	handle := func(_ context.Context, _ ChatMessage, _ string) error { return nil }
	if _, err := bridge.pollChat(ctx, "chat-cycle", 20, handle); err != nil {
		t.Fatalf("first continuation page: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-cycle")
	if err != nil || !ok {
		t.Fatalf("read after first continuation page: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != secondPath || len(poll.ContinuationPathHistory) != 1 || len(poll.ContinuationPageFingerprintHistory) != 1 {
		t.Fatalf("continuation evidence after first page = %#v", poll)
	}
	for attempt := 0; attempt < continuationFailureBudget; attempt++ {
		if _, err := bridge.pollChat(ctx, "chat-cycle", 20, handle); err == nil {
			t.Fatalf("same-page continuation attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	poll, ok, err = store.ChatPoll(ctx, "chat-cycle")
	if err != nil || !ok {
		t.Fatalf("read cycle gap: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != "" || poll.Gap == nil || poll.Gap.FrontierPath != secondPath || poll.PollState == inboundPollStateBlocked {
		t.Fatalf("same-page continuation did not become a non-blocking gap: %#v", poll)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 {
		t.Fatalf("continuation request trace = %v, want first page plus one staged retry", gotRequests)
	}
	for _, path := range gotRequests[1:] {
		if strings.Contains(path, "third") {
			t.Fatalf("livelock followed regenerated nextLink: %v", gotRequests)
		}
	}
}

func TestPollFrontierEmptyGapRecoveryBacksOffInsteadOfSpinning(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-empty-gap"] = teamstore.ChatPollState{
			ChatID:             "chat-empty-gap",
			Seeded:             true,
			PollState:          inboundPollStateWarm,
			NextPollAt:         now,
			LastActivityAt:     now,
			LastModifiedCursor: now.Add(-time.Hour),
			FrontierEpoch:      1,
			Gap: &teamstore.ChatPollGap{
				Epoch:          1,
				Kind:           "unverified-continuation",
				SafeCursor:     now.Add(-time.Hour),
				RecoveryCursor: now.Add(-time.Hour),
				OpenedAt:       now,
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed empty recovery gap: %v", err)
	}
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	attempt, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
		ChatID:             "chat-empty-gap",
		Owner:              "machine-a",
		ProcessIncarnation: "process-a",
		LeaseGeneration:    7,
		ExpectedFrontier:   pollFrontierIdentity(pollFrontierGap, "/chats/chat-empty-gap/messages?$top=1"),
		Now:                now,
	})
	if err != nil || !acquired || attempt.Attempt == nil {
		t.Fatalf("begin empty recovery attempt: acquired=%v attempt=%#v err=%v", acquired, attempt, err)
	}
	path := "/chats/chat-empty-gap/messages?$top=1"
	committed, err := bridge.commitPollAttemptSuccess(ctx, "chat-empty-gap", attempt.Attempt.ID, attempt.PollRevision, inboundPollRoleWork, false, pollFrontierGap, path, MessageWindow{}, pollMessageWindowResult{PageComplete: true}, false)
	if err != nil || !committed {
		t.Fatalf("commit empty recovery page: committed=%v err=%v", committed, err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-empty-gap")
	if err != nil || !ok {
		t.Fatalf("read empty recovery state: ok=%v err=%v", ok, err)
	}
	if poll.Gap == nil || poll.Gap.RecoveryPath != "" {
		t.Fatalf("empty recovery page changed gap unexpectedly: %#v", poll)
	}
	if !poll.NextPollAt.After(now) {
		t.Fatalf("empty recovery page scheduled a hot loop: next=%s start=%s", poll.NextPollAt, now)
	}
	if poll.NextPollAt.Sub(now) < inboundPollColdInterval/2 {
		t.Fatalf("empty recovery page backoff=%s, want approximately cold interval %s", poll.NextPollAt.Sub(now), inboundPollColdInterval)
	}
	decision := decideInboundPoll(inboundPollInput{
		ChatID: "chat-empty-gap", Role: inboundPollRoleWork, Poll: poll, HasPoll: true, Now: now.Add(time.Second),
	})
	if decision.Due {
		t.Fatalf("empty recovery gap was due again immediately: %#v", decision)
	}
}

func writePollFrontierPage(t *testing.T, w http.ResponseWriter, messages []ChatMessage, nextPath string) {
	t.Helper()
	payload := map[string]any{"value": messages}
	if strings.TrimSpace(nextPath) != "" {
		payload["@odata.nextLink"] = nextPath
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode Graph page: %v", err)
	}
}
