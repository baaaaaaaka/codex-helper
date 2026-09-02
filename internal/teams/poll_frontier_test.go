package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestPollFrontierHeadPageThenContinuationFailureRecoversFromPreHeadCursor(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newBridgeTestStore(t)
			base := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
			chatID := "chat-head-continuation-gap"
			oldPath := "/chats/" + chatID + "/messages?$skiptoken=expired-head-continuation"
			head := bridgePollMessage("head-newer", base.Add(2*time.Minute).Format(time.RFC3339Nano), "head message")
			old := bridgePollMessage("old-failed-page", base.Add(time.Minute).Format(time.RFC3339Nano), "old page message")
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.String()
				mu.Lock()
				requests = append(requests, path)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Query().Get("$skiptoken") == "" && r.URL.Query().Get("$orderby") == "lastModifiedDateTime desc" &&
					strings.Contains(r.URL.Query().Get("$filter"), "lastModifiedDateTime ge ") &&
					strings.Contains(r.URL.Query().Get("$filter"), "lastModifiedDateTime le ") {
					// Recovery may return the already-seen head record together
					// with the older record. The delivery ledger must suppress
					// that duplicate while still making the old record reachable.
					writePollFrontierPage(t, w, []ChatMessage{old, head}, "")
					return
				}
				switch r.URL.Query().Get("$skiptoken") {
				case "":
					writePollFrontierPage(t, w, []ChatMessage{head}, oldPath)
				case "expired-head-continuation":
					// This is a stale opaque Graph token. The durable gap must
					// retain the cursor from before the successful head page, not
					// the newer head cursor, so this old record remains reachable.
					w.WriteHeader(http.StatusNotFound)
					_, _ = fmt.Fprint(w, `{"error":{"code":"InvalidSkipToken","message":"expired skiptoken continuation"}}`)
				default:
					http.Error(w, "unexpected continuation", http.StatusBadRequest)
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
			if _, err := store.RecordChatPollSuccess(ctx, chatID, base, true, false, 0); err != nil {
				t.Fatalf("seed poll: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate poll state to SQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			var handled []string
			handle := func(_ context.Context, message ChatMessage, _ string) error {
				handled = append(handled, message.ID)
				return nil
			}

			if _, err := bridge.pollChat(ctx, chatID, 20, handle); err != nil {
				t.Fatalf("head page poll: %v", err)
			}
			poll, found, err := store.ChatPoll(ctx, chatID)
			if err != nil || !found || poll.ContinuationPath != oldPath || !poll.LastModifiedCursor.Equal(messageModifiedTime(head)) {
				t.Fatalf("after head page poll = %#v found=%v err=%v, want head cursor plus continuation", poll, found, err)
			}
			if !poll.ContinuationSafeCursor.Equal(base) {
				t.Fatalf("continuation safe cursor = %s, want pre-head cursor %s", poll.ContinuationSafeCursor, base)
			}

			if _, err := bridge.pollChat(ctx, chatID, 20, handle); err == nil {
				t.Fatal("expired continuation poll returned nil; want stale frontier error")
			}
			poll, found, err = store.ChatPoll(ctx, chatID)
			if err != nil || !found || poll.Gap == nil {
				t.Fatalf("after continuation failure = %#v found=%v err=%v, want recovery gap", poll, found, err)
			}
			if !poll.Gap.SafeCursor.Equal(base) || !poll.Gap.RecoveryCursor.Equal(messageModifiedTime(head)) {
				t.Fatalf("gap cursors = safe:%s recovery:%s, want safe cursor %s and head upper bound %s", poll.Gap.SafeCursor, poll.Gap.RecoveryCursor, base, messageModifiedTime(head))
			}

			if _, err := bridge.pollChat(ctx, chatID, 20, handle); err != nil {
				t.Fatalf("gap recovery poll: %v", err)
			}
			foundOld := false
			for _, id := range handled {
				if id == old.ID {
					foundOld = true
					break
				}
			}
			if !foundOld {
				t.Fatalf("recovery did not reach old failed-page record: handled=%v", handled)
			}
			mu.Lock()
			gotRequests := append([]string(nil), requests...)
			mu.Unlock()
			foundRecoveryRequest := false
			for _, request := range gotRequests {
				if strings.Contains(request, "$top=20") || strings.Contains(request, "%24top=20") {
					foundRecoveryRequest = true
					break
				}
			}
			if !foundRecoveryRequest {
				t.Fatalf("request trace = %v, want bounded directional top=20 recovery request", gotRequests)
			}
		})
	}
}

func TestPollFrontierNormalizationDoesNotRetireActiveAttempt(t *testing.T) {
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
			normalized, hasPoll, err := normalizePollFrontier(ctx, store, "chat-normalize")
			if err != nil || !hasPoll || normalized.DeferredContinuationPath != path || normalized.ContinuationPath != path || normalized.Attempt == nil || normalized.Attempt.ID != oldAttemptID {
				t.Fatalf("normalize retired or changed active attempt: hasPoll=%v state=%#v err=%v", hasPoll, normalized, err)
			}
			if _, committed, err := store.CommitChatPollAttempt(ctx, "chat-normalize", oldAttemptID, attempt.PollRevision, func(poll *teamstore.ChatPollState) error {
				poll.LastError = "active normalization owner can still commit"
				return nil
			}); err != nil || !committed {
				t.Fatalf("active normalization attempt was unexpectedly fenced: committed=%v err=%v", committed, err)
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

func TestPollFrontierPendingPageAcceptsSeveralBoundedLargeRecords(t *testing.T) {
	const recordBytes = 1 << 20
	messages := make([]ChatMessage, 0, 20)
	for i := 0; i < cap(messages); i++ {
		messages = append(messages, bridgePollMessage(
			fmt.Sprintf("large-page-%02d", i),
			time.Date(2026, 8, 31, 12, 0, i, 0, time.UTC).Format(time.RFC3339Nano),
			strings.Repeat("x", recordBytes),
		))
	}
	page, err := pendingPageFromWindow(
		"chat-large-page", "/chats/chat-large-page/messages?$top=20", pollFrontierHead, 1,
		MessageWindow{Messages: messages, Truncated: true, NextPath: "/chats/chat-large-page/messages?$skiptoken=after-large-page"},
		false,
	)
	if err != nil {
		t.Fatalf("bounded large page was rejected: %v", err)
	}
	if len(page.Records) != len(messages) || len(page.RecordIDs) != len(messages) || page.NextPath == "" {
		t.Fatalf("large page receipt = records=%d ids=%d next=%q, want all records and continuation", len(page.Records), len(page.RecordIDs), page.NextPath)
	}
	if _, err := pendingPageToWindow(page); err != nil {
		t.Fatalf("large page receipt did not replay: %v", err)
	}
}

func TestPollFrontierInvalidRecordQuarantineDoesNotHoldLaterMessages(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	invalid := bridgePollMessage("invalid-record", now.Format(time.RFC3339Nano), "")
	invalid.quarantinedForPoll = true
	later := bridgePollMessage("later-valid-record", now.Add(time.Minute).Format(time.RFC3339Nano), "later prompt")
	page, err := pendingPageFromWindow("chat-invalid-record", "/chats/chat-invalid-record/messages?$top=20", pollFrontierHead, 1, MessageWindow{
		Messages:  []ChatMessage{invalid, later},
		Truncated: true,
		NextPath:  "/chats/chat-invalid-record/messages?$skiptoken=older",
	}, false)
	if err != nil {
		t.Fatalf("build invalid-record receipt: %v", err)
	}
	if len(page.Dispositions) != 2 || page.Dispositions[0] != "invalid_record_quarantined" || page.Dispositions[1] != "received" {
		t.Fatalf("invalid-record receipt dispositions = %#v", page.Dispositions)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-invalid-record"] = teamstore.ChatPollState{
			ChatID:        "chat-invalid-record",
			Seeded:        true,
			PollState:     inboundPollStateWarm,
			NextPollAt:    now,
			FrontierEpoch: 1,
			PendingPage:   page,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed invalid-record receipt: %v", err)
	}

	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	var handled []string
	if _, err := bridge.pollChat(ctx, "chat-invalid-record", 20, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("replay invalid-record receipt: %v", err)
	}
	if got := strings.Join(handled, ","); got != later.ID {
		t.Fatalf("handled records = %q, want only %s", got, later.ID)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-invalid-record")
	if err != nil || !ok || poll.PendingPage != nil || len(poll.QuarantinedRecordIDs) != 1 || poll.QuarantinedRecordIDs[0] != invalid.ID {
		t.Fatalf("post-quarantine poll state = %#v ok=%v err=%v", poll, ok, err)
	}
}

func TestPollFrontierInvalidRecordRefetchesBeforeQuarantine(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	invalidRaw := json.RawMessage(`{"id":"invalid-recoverable","chatId":"chat-invalid-recoverable","createdDateTime":"` + now.Format(time.RFC3339Nano) + `","lastModifiedDateTime":"` + now.Format(time.RFC3339Nano) + `","body":"provider temporarily returned the wrong shape"}`)
	recovered := bridgePollMessage("invalid-recoverable", now.Format(time.RFC3339Nano), "recovered prompt")
	recoveredRaw, err := json.Marshal(recovered)
	if err != nil {
		t.Fatalf("marshal recovered message: %v", err)
	}
	later := bridgePollMessage("later-after-invalid-recoverable", now.Add(time.Minute).Format(time.RFC3339Nano), "later prompt")
	laterRaw, err := json.Marshal(later)
	if err != nil {
		t.Fatalf("marshal later message: %v", err)
	}
	pagePayload, err := json.Marshal(map[string]any{
		"value": []json.RawMessage{invalidRaw, laterRaw},
	})
	if err != nil {
		t.Fatalf("marshal invalid-record page: %v", err)
	}
	var listRequests, refetchRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/chats/chat-invalid-recoverable/messages":
			listRequests++
			_, _ = w.Write(pagePayload)
		case r.URL.Path == "/chats/chat-invalid-recoverable/messages/invalid-recoverable":
			refetchRequests++
			_, _ = w.Write(recoveredRaw)
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
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
	if _, err := store.RecordChatPollSuccess(ctx, "chat-invalid-recoverable", now.Add(-time.Hour), true, false, 0); err != nil {
		t.Fatalf("seed invalid-record poll: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	if _, err := bridge.pollChat(ctx, "chat-invalid-recoverable", 20, func(_ context.Context, msg ChatMessage, text string) error {
		handled = append(handled, msg.ID+":"+text)
		return nil
	}); err != nil {
		t.Fatalf("invalid-record recovery poll: %v", err)
	}
	if _, err := bridge.pollChat(ctx, "chat-invalid-recoverable", 20, func(_ context.Context, msg ChatMessage, text string) error {
		handled = append(handled, msg.ID+":"+text)
		return nil
	}); err != nil {
		t.Fatalf("invalid-record pending-page replay: %v", err)
	}
	if got := strings.Join(handled, ","); !strings.Contains(got, "invalid-recoverable:recovered prompt") || !strings.Contains(got, later.ID+":later prompt") {
		t.Fatalf("handled records = %q, want recovered and later messages", got)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-invalid-recoverable")
	if err != nil || !ok || poll.PendingPage != nil || len(poll.QuarantinedRecordIDs) != 0 {
		t.Fatalf("invalid-record recovery state = %#v ok=%v err=%v", poll, ok, err)
	}
	if listRequests != 1 || refetchRequests != 1 {
		t.Fatalf("invalid-record request counts = list %d/refetch %d, want one each", listRequests, refetchRequests)
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

func TestPollFrontierOversizedRefetchIdentityMismatchIsBounded(t *testing.T) {
	for _, tc := range []struct {
		name       string
		message    ChatMessage
		wantReason string
	}{
		{
			name:       "wrong-id",
			message:    bridgePollMessage("different-message", "2026-08-30T01:00:00Z", "not the requested message"),
			wantReason: "changed identity",
		},
		{
			name: "wrong-chat",
			message: func() ChatMessage {
				msg := bridgePollMessage("oversized-mismatch", "2026-08-30T01:00:00Z", "from another chat")
				msg.ChatID = "another-chat"
				return msg
			}(),
			wantReason: "changed chat",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			requested := bridgePollMessage("oversized-mismatch", "2026-08-30T00:00:00Z", "")
			requested.oversizedForPoll = true
			page, err := pendingPageFromWindow("chat-oversized-mismatch", "/chats/chat-oversized-mismatch/messages?$top=1", pollFrontierHead, 1, MessageWindow{
				Messages: []ChatMessage{requested},
			}, false)
			if err != nil {
				t.Fatalf("build oversized mismatch page: %v", err)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ChatPolls["chat-oversized-mismatch"] = teamstore.ChatPollState{
					ChatID:        "chat-oversized-mismatch",
					Seeded:        true,
					PollState:     inboundPollStateWarm,
					NextPollAt:    time.Now().Add(-time.Second),
					FrontierEpoch: 1,
					PendingPage:   page,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed oversized mismatch page: %v", err)
			}
			var refetches int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-oversized-mismatch/messages/oversized-mismatch" {
					t.Fatalf("unexpected oversized mismatch request: %s %s", r.Method, r.URL.String())
				}
				refetches++
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(tc.message); err != nil {
					t.Fatalf("encode mismatch message: %v", err)
				}
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
			for attempt := 0; attempt < maxOversizedRecordRefetchAttempts; attempt++ {
				if _, err := bridge.pollChat(ctx, "chat-oversized-mismatch", 20, func(context.Context, ChatMessage, string) error {
					t.Fatal("identity-mismatched oversized record reached the handler")
					return nil
				}); err == nil || !strings.Contains(err.Error(), tc.wantReason) {
					t.Fatalf("refetch attempt %d error = %v, want %q", attempt+1, err, tc.wantReason)
				}
			}
			poll, ok, err := store.ChatPoll(ctx, "chat-oversized-mismatch")
			if err != nil || !ok || poll.PendingPage == nil || len(poll.PendingPage.Dispositions) == 0 || len(poll.PendingPage.RefetchFailures) == 0 || poll.PendingPage.Dispositions[0] != "oversized_record_quarantined" || poll.PendingPage.RefetchFailures[0] != maxOversizedRecordRefetchAttempts {
				t.Fatalf("identity mismatch was not bounded: ok=%v err=%v poll=%#v", ok, err, poll)
			}
			if refetches != maxOversizedRecordRefetchAttempts {
				t.Fatalf("identity mismatch refetches = %d, want %d", refetches, maxOversizedRecordRefetchAttempts)
			}
			if _, err := bridge.pollChat(ctx, "chat-oversized-mismatch", 20, func(context.Context, ChatMessage, string) error {
				t.Fatal("quarantined identity mismatch reached the handler")
				return nil
			}); err != nil {
				t.Fatalf("consume quarantined identity mismatch: %v", err)
			}
			poll, ok, err = store.ChatPoll(ctx, "chat-oversized-mismatch")
			if err != nil || !ok || poll.PendingPage != nil || len(poll.QuarantinedRecordIDs) != 1 || poll.QuarantinedRecordIDs[0] != "oversized-mismatch" {
				t.Fatalf("post-quarantine state = %#v ok=%v err=%v", poll, ok, err)
			}
		})
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
	if err != nil || !ok || poll.Gap != nil {
		t.Fatalf("terminal gap recovery remained open = %#v ok=%v err=%v", poll, ok, err)
	}
	if !poll.LastModifiedCursor.Equal(now.Add(-time.Hour)) {
		t.Fatalf("normal cursor changed across gap recovery = %#v", poll)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 || !strings.Contains(gotRequests[len(gotRequests)-1], "%24filter") || recoveryTop != "20" {
		t.Fatalf("request trace = %v recovery top=%q, want one token failure then bounded top=20 recovery head", gotRequests, recoveryTop)
	}
	recoveryURL, err := url.Parse(gotRequests[len(gotRequests)-1])
	if err != nil {
		t.Fatalf("parse gap recovery request %q: %v", gotRequests[len(gotRequests)-1], err)
	}
	if got := strings.TrimSpace(recoveryURL.Query().Get("$orderby")); got != "lastModifiedDateTime desc" {
		t.Fatalf("gap recovery order = %q, want provider-supported descending order", got)
	}
}

func TestPollFrontierGapRecoveryWalksOldestBacklogWithoutSkipping(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	oldPath := "/chats/chat-gap-order/messages?$skiptoken=expired"
	oldest := bridgePollMessage("gap-oldest", now.Add(-50*time.Minute).Format(time.RFC3339Nano), "oldest backlog")
	middle := bridgePollMessage("gap-middle", now.Add(-40*time.Minute).Format(time.RFC3339Nano), "middle backlog")
	newest := bridgePollMessage("gap-newest", now.Add(-30*time.Minute).Format(time.RFC3339Nano), "newest backlog")
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.String())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "expired":
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"Gone","message":"continuation skiptoken expired"}}`)
			return
		case "gap-page-2":
			// Make the opaque continuation fail too. The following request must
			// use the durable lower/upper time bounds and recover this record.
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"Gone","message":"recovery continuation expired"}}`)
		default:
			if r.URL.Query().Get("$orderby") != "lastModifiedDateTime desc" {
				t.Errorf("gap recovery order = %q, want provider-supported descending", r.URL.Query().Get("$orderby"))
			}
			filter := r.URL.Query().Get("$filter")
			if strings.Contains(filter, "lastModifiedDateTime le ") {
				if !strings.Contains(filter, "lastModifiedDateTime ge "+now.Add(-time.Hour).Format(time.RFC3339Nano)) ||
					!strings.Contains(filter, "lastModifiedDateTime le "+messageModifiedTime(middle).Format(time.RFC3339Nano)) {
					t.Errorf("bounded gap filter = %q, want the remaining range", filter)
				}
				writePollFrontierPage(t, w, []ChatMessage{oldest}, "")
				return
			}
			if !strings.Contains(filter, "lastModifiedDateTime ge "+now.Add(-time.Hour).Format(time.RFC3339Nano)) {
				t.Errorf("initial gap filter = %q, want safe lower bound", filter)
			}
			// Graph returns descending pages. The bridge sorts each bounded page
			// before handling, so it can make progress without an unsupported
			// ascending provider request.
			writePollFrontierPage(t, w, []ChatMessage{newest, middle}, "/chats/chat-gap-order/messages?$skiptoken=gap-page-2")
		}
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
		state.ChatPolls["chat-gap-order"] = teamstore.ChatPollState{
			ChatID:                      "chat-gap-order",
			Seeded:                      true,
			PollState:                   inboundPollStateWarm,
			NextPollAt:                  now,
			LastActivityAt:              now,
			LastModifiedCursor:          now.Add(-time.Hour),
			ContinuationSafeCursor:      now.Add(-time.Hour),
			ContinuationSafeCursorKnown: true,
			ContinuationPath:            oldPath,
			FrontierEpoch:               1,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed gap ordering state: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}
	if _, err := bridge.pollChat(ctx, "chat-gap-order", 20, handle); err == nil {
		t.Fatal("expired continuation unexpectedly succeeded")
	}
	if _, err := bridge.pollChat(ctx, "chat-gap-order", 20, handle); err != nil {
		t.Fatalf("gap recovery page: %v", err)
	}
	if _, err := bridge.pollChat(ctx, "chat-gap-order", 20, handle); err != nil {
		t.Fatalf("pending gap page replay: %v", err)
	}
	if _, err := bridge.pollChat(ctx, "chat-gap-order", 20, handle); err == nil {
		t.Fatal("expired recovery continuation unexpectedly succeeded")
	}
	if _, err := bridge.pollChat(ctx, "chat-gap-order", 20, handle); err != nil {
		t.Fatalf("bounded gap recovery page: %v", err)
	}
	if got := strings.Join(handled, ","); got != "gap-middle,gap-newest,gap-oldest" {
		t.Fatalf("gap recovery handled = %q, want every record exactly once", got)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 4 {
		t.Fatalf("gap recovery request count = %d, want 4: %v", len(gotRequests), gotRequests)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-gap-order")
	if err != nil || !ok || poll.Gap != nil {
		t.Fatalf("terminal gap recovery remained open = %#v ok=%v err=%v", poll, ok, err)
	}
	if !poll.LastModifiedCursor.Equal(now.Add(-time.Hour)) {
		t.Fatalf("normal cursor changed across gap recovery = %#v", poll)
	}
}

// TestPollFrontierGapRecoveryKeepsEqualTimestampBucket verifies the boundary
// that strict time bounds cannot represent by themselves.
// Graph can legitimately return several messages with the same timestamp. A
// bounded recovery page must fetch the bucket together and let the durable
// pending-page receipt drain it; otherwise a top=1 recovery would advance the
// cursor after the first item and make the remaining equal-timestamp messages
// unreachable.
func TestPollFrontierGapRecoveryKeepsEqualTimestampBucket(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	const chatID = "chat-gap-equal-time"
	const expiredPath = "/chats/" + chatID + "/messages?$skiptoken=expired"
	equalTime := now.Add(-30 * time.Minute).Format(time.RFC3339Nano)
	messages := []ChatMessage{
		bridgePollMessage("gap-equal-1", equalTime, "equal timestamp one"),
		bridgePollMessage("gap-equal-2", equalTime, "equal timestamp two"),
		bridgePollMessage("gap-equal-3", equalTime, "equal timestamp three"),
	}
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.String())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$skiptoken") == "expired" {
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"Gone","message":"expired gap continuation"}}`)
			return
		}
		if got := r.URL.Query().Get("$orderby"); got != "lastModifiedDateTime desc" {
			t.Errorf("equal-time gap order = %q, want provider-supported descending", got)
		}
		filter := r.URL.Query().Get("$filter")
		if !strings.Contains(filter, "lastModifiedDateTime ge "+now.Add(-time.Hour).Format(time.RFC3339Nano)) ||
			!strings.Contains(filter, "lastModifiedDateTime le "+equalTime) {
			// A pre-fix strict-bounds request cannot include the equal-time
			// bucket. Model Graph's empty result so the regression fails by
			// leaving the bucket undelivered rather than accepting a fixture
			// that ignores the actual filter.
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": messages})
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
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID: chatID, Seeded: true, PollState: inboundPollStateWarm,
			NextPollAt: now, LastActivityAt: now, LastModifiedCursor: now.Add(-30 * time.Minute),
			ContinuationSafeCursor: now.Add(-time.Hour), ContinuationPath: expiredPath, FrontierEpoch: 1,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed equal-time gap: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}
	if _, err := bridge.pollChat(ctx, chatID, 20, handle); err == nil {
		t.Fatal("expired equal-time continuation unexpectedly succeeded")
	}
	for i := 0; i < len(messages); i++ {
		if _, err := bridge.pollChat(ctx, chatID, 20, handle); err != nil {
			t.Fatalf("equal-time recovery quantum %d: %v", i+1, err)
		}
	}
	if got := strings.Join(handled, ","); got != "gap-equal-1,gap-equal-2,gap-equal-3" {
		t.Fatalf("equal-time recovery handled = %q, want all bucket records", got)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 {
		t.Fatalf("equal-time recovery Graph requests = %d, want expired continuation plus one bounded bucket page: %v", len(gotRequests), gotRequests)
	}
	recovered, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok || recovered.PendingPage != nil || recovered.Gap != nil {
		t.Fatalf("equal-time recovery state = %#v ok=%v err=%v, want terminal gap release", recovered, ok, err)
	}
}

// TestPollFrontierPartialQuantumAdvancesDurableServiceAgeAcrossBackends
// protects the scheduler's starvation boundary.  A poll that handled one
// bounded action but retained a pending page is still a successful service
// quantum.  Its durable service time must move forward in both backends, or a
// continuously due chat can win every cycle cap and starve the tail forever.
func TestPollFrontierPartialQuantumAdvancesDurableServiceAgeAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate poll frontier store: %v", err)
				}
			}
			oldSuccess := time.Now().UTC().Add(-time.Hour)
			if _, err := store.RecordChatPollSuccess(ctx, "chat-partial-age", oldSuccess, true, false, 0); err != nil {
				t.Fatalf("seed poll state: %v", err)
			}
			if _, _, err := store.UpdateChatPoll(ctx, "chat-partial-age", func(poll *teamstore.ChatPollState) error {
				poll.LastSuccessfulPollAt = oldSuccess
				poll.NextPollAt = oldSuccess
				return nil
			}); err != nil {
				t.Fatalf("age poll state: %v", err)
			}
			before, ok, err := store.ChatPoll(ctx, "chat-partial-age")
			if err != nil || !ok {
				t.Fatalf("read partial-age poll: ok=%v err=%v", ok, err)
			}
			started, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
				ChatID: "chat-partial-age", Owner: "partial-owner", ProcessIncarnation: "partial-process",
				ExpectedPollRevision: before.PollRevision, HasExpectedPollRevision: true, Now: time.Now().UTC(),
			})
			if err != nil || !acquired || started.Attempt == nil {
				t.Fatalf("begin partial quantum: acquired=%v attempt=%#v err=%v", acquired, started.Attempt, err)
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			committed, err := bridge.commitPollAttemptPartial(ctx, "chat-partial-age", started.Attempt.ID, started.PollRevision, pollMessageWindowResult{
				ActivityAt: time.Now().UTC(), ActionLimitReached: true,
			}, false)
			if err != nil || !committed {
				t.Fatalf("commit partial quantum: committed=%v err=%v", committed, err)
			}
			after, ok, err := store.ChatPoll(ctx, "chat-partial-age")
			if err != nil || !ok {
				t.Fatalf("read committed partial-age poll: ok=%v err=%v", ok, err)
			}
			if !after.LastSuccessfulPollAt.After(before.LastSuccessfulPollAt) {
				t.Fatalf("partial quantum did not advance durable service age: before=%s after=%s", before.LastSuccessfulPollAt, after.LastSuccessfulPollAt)
			}
			if after.Attempt != nil {
				t.Fatalf("partial quantum left an active poll attempt: %#v", after.Attempt)
			}
		})
	}
}

// TestPollSchedulerContinuousPartialChatsRotateBeyondCycleCap proves the
// scheduler-level consequence of the partial-quantum service-age rule. Nine
// continuously due chats each retain a three-record pending page, while one
// chat may consume only one actionable record per quantum. After two bounded
// cycles the ninth chat must have received a quantum too; a scheduler that
// leaves LastSuccessfulPollAt unchanged for partial work repeatedly chooses
// the same first eight chats instead.
func TestPollSchedulerContinuousPartialChatsRotateBeyondCycleCap(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	bridge.maxWorkChatPollsPerCycle = 8
	now := time.Now().UTC()
	const chatCount = 9
	sessions := make([]Session, 0, chatCount)
	for index := 1; index <= chatCount; index++ {
		session := Session{
			ID:        fmt.Sprintf("partial-rotation-session-%02d", index),
			ChatID:    fmt.Sprintf("partial-rotation-chat-%02d", index),
			Status:    "active",
			UpdatedAt: now,
		}
		sessions = append(sessions, session)
		bridge.reg.Sessions = append(bridge.reg.Sessions, session)
		if err := bridge.ensureDurableSession(ctx, &bridge.reg.Sessions[len(bridge.reg.Sessions)-1]); err != nil {
			t.Fatalf("ensure partial-rotation session %s: %v", session.ID, err)
		}
		messages := make([]ChatMessage, 0, 3)
		for messageIndex := 1; messageIndex <= 3; messageIndex++ {
			msg := bridgeTestMessageWithText(
				fmt.Sprintf("partial-rotation-message-%02d-%02d", index, messageIndex),
				fmt.Sprintf("partial rotation %02d/%02d", index, messageIndex),
			)
			msg.ChatID = session.ChatID
			stamp := now.Add(time.Duration(index*10+messageIndex) * time.Second).Format(time.RFC3339Nano)
			msg.CreatedDateTime = stamp
			msg.LastModifiedDateTime = stamp
			messages = append(messages, msg)
		}
		page, err := pendingPageFromWindow(
			session.ChatID,
			"/chats/"+session.ChatID+"/messages",
			pollFrontierHead,
			1,
			MessageWindow{Messages: messages},
			false,
		)
		if err != nil {
			t.Fatalf("build partial page %s: %v", session.ChatID, err)
		}
		if err := store.Update(ctx, func(state *teamstore.State) error {
			state.ChatPolls[session.ChatID] = teamstore.ChatPollState{
				ChatID:               session.ChatID,
				Seeded:               true,
				PollState:            inboundPollStateHot,
				NextPollAt:           now.Add(-time.Minute),
				LastActivityAt:       now,
				LastSuccessfulPollAt: now.Add(-time.Duration(chatCount-index+1) * time.Minute),
				PendingPage:          page,
				FrontierEpoch:        1,
			}
			return nil
		}); err != nil {
			t.Fatalf("seed partial page %s: %v", session.ChatID, err)
		}
	}

	served := make(map[string]int, chatCount)
	for cycle := 0; cycle < 2; cycle++ {
		state, err := store.Load(ctx)
		if err != nil {
			t.Fatalf("load partial rotation cycle %d: %v", cycle+1, err)
		}
		decisions := make([]inboundPollDecision, 0, chatCount)
		for _, session := range sessions {
			poll, ok := state.ChatPolls[session.ChatID]
			if !ok {
				t.Fatalf("missing partial rotation poll %s", session.ChatID)
			}
			decision := decideInboundPoll(inboundPollInput{
				ChatID:           session.ChatID,
				Role:             inboundPollRoleWork,
				Poll:             poll,
				HasPoll:          true,
				SessionUpdatedAt: session.UpdatedAt,
				Now:              time.Now().UTC(),
			})
			if !decision.Due {
				t.Fatalf("partial rotation chat %s was not due in cycle %d: %#v", session.ChatID, cycle+1, decision)
			}
			decisions = append(decisions, decision)
		}
		sortInboundPollDecisions(decisions)
		selected := limitInboundPollDecisions(decisions, bridge.effectiveMaxWorkChatPollsPerCycle())
		if len(selected) != 8 {
			t.Fatalf("partial rotation selected %d chats in cycle %d, want 8", len(selected), cycle+1)
		}
		for _, decision := range selected {
			poll, ok := state.ChatPolls[decision.ChatID]
			if !ok {
				t.Fatalf("selected partial rotation chat disappeared: %s", decision.ChatID)
			}
			_, pollErr := bridge.pollChatWithRoleStateOptions(ctx, decision.ChatID, 20, inboundPollRoleWork, false, poll, true, pollChatWithRoleOptions{
				MaxBacklogActions: 1,
			}, func(context.Context, ChatMessage, string) error {
				served[decision.ChatID]++
				return nil
			})
			if pollErr != nil {
				t.Fatalf("partial rotation chat %s cycle %d: %v", decision.ChatID, cycle+1, pollErr)
			}
		}
	}
	for _, session := range sessions {
		if served[session.ChatID] == 0 {
			t.Fatalf("continuously partial chat starved beyond cycle cap: chat=%s served=%v", session.ChatID, served)
		}
	}
}

// TestPollFrontierFullWindowWithoutNextLinkIsTerminal documents the Graph
// pagination contract. A page that happens to contain the requested number of
// records is diagnostic information, but without @odata.nextLink it is still
// the terminal page for that filtered query. Treating every such response as
// a gap would turn a healthy chat into a permanent held state; the warning is
// retained for observability while the durable frontier advances normally.
func TestPollFrontierFullWindowWithoutNextLinkIsTerminal(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	base := time.Date(2026, 8, 31, 2, 0, 0, 0, time.UTC)
	const chatID = "chat-full-window-terminal"
	messages := make([]ChatMessage, 0, 20)
	for i := 0; i < 20; i++ {
		msg := bridgeTestMessageWithText(fmt.Sprintf("full-window-%02d", i), fmt.Sprintf("full-window message %02d", i))
		msg.ChatID = chatID
		stamp := base.Add(time.Duration(i+1) * time.Minute).Format(time.RFC3339Nano)
		msg.CreatedDateTime = stamp
		msg.LastModifiedDateTime = stamp
		messages = append(messages, msg)
	}
	var requests []string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/"+chatID+"/messages" {
			http.Error(w, "unexpected full-window request", http.StatusNotFound)
			return
		}
		if got := r.URL.Query().Get("$orderby"); got != "lastModifiedDateTime desc" {
			t.Errorf("full-window order = %q, want descending", got)
		}
		if got := r.URL.Query().Get("$filter"); !strings.HasPrefix(got, "lastModifiedDateTime gt ") {
			t.Errorf("full-window filter = %q, want strict modified-time filter", got)
		}
		mu.Lock()
		requests = append(requests, r.URL.RequestURI())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": messages})
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
	if _, err := store.RecordChatPollSuccess(ctx, chatID, base, true, false, 0); err != nil {
		t.Fatalf("seed full-window poll: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	if _, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, teamstore.ChatPollState{}, false, pollChatWithRoleOptions{
		AllowBacklogDrain: false,
		MaxBacklogActions: 0,
	}, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("full-window terminal poll: %v", err)
	}
	if len(handled) != len(messages) {
		t.Fatalf("full-window handled %d messages, want %d: %v", len(handled), len(messages), handled)
	}
	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("load full-window terminal poll: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != "" || poll.DeferredContinuationPath != "" || poll.Gap != nil || poll.PendingPage != nil {
		t.Fatalf("full-window no-nextLink opened a nonterminal frontier: %#v", poll)
	}
	if poll.LastModifiedCursor.IsZero() || !poll.LastModifiedCursor.Equal(messageModifiedTime(messages[len(messages)-1])) {
		t.Fatalf("full-window cursor = %s, want %s", poll.LastModifiedCursor, messageModifiedTime(messages[len(messages)-1]))
	}
	if poll.LastWindowFullAt.IsZero() || !strings.Contains(poll.LastWindowFullMessage, "full message window") {
		t.Fatalf("full-window diagnostic missing: %#v", poll)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 1 {
		t.Fatalf("full-window terminal Graph requests = %d, want one: %v", len(gotRequests), gotRequests)
	}
}

// A 2xx response that is not a message page must never be treated as a
// terminal empty continuation.  The invalid page is moved to the directional
// gap lane while the safe predecessor cursor remains unchanged.
func TestPollFrontierInvalidContinuationPageOpensGapWithoutAdvancingCursor(t *testing.T) {
	ctx := context.Background()
	const chatID = "chat-invalid-continuation-page"
	continuation := "/chats/" + chatID + "/messages?$skiptoken=invalid-page"
	base := time.Date(2026, 8, 31, 3, 0, 0, 0, time.UTC)
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != continuation {
			t.Errorf("invalid continuation request = %q, want %q", r.URL.RequestURI(), continuation)
		}
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)
	graph := newTestGraphClient(&fakeGraphAuth{token: "access"}, server, nil)
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(ctx, chatID, base, true, false, 0); err != nil {
		t.Fatalf("seed continuation poll: %v", err)
	}
	if _, _, err := store.UpdateChatPoll(ctx, chatID, func(poll *teamstore.ChatPollState) error {
		poll.ContinuationPath = continuation
		poll.ContinuationSafeCursor = base
		poll.NextPollAt = time.Time{}
		return nil
	}); err != nil {
		t.Fatalf("seed continuation path: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("load continuation poll: ok=%v err=%v", ok, err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	if _, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, poll, true, pollChatWithRoleOptions{}, nil); !errors.Is(err, errGraphMessagePageInvalid) {
		t.Fatalf("invalid continuation poll error = %v, want invalid-page classification", err)
	}
	got, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("load recovered continuation poll: ok=%v err=%v", ok, err)
	}
	if got.Gap == nil || got.Gap.FrontierPath != continuation {
		t.Fatalf("invalid continuation did not open bounded gap: %#v", got)
	}
	if got.ContinuationPath != "" {
		t.Fatalf("invalid continuation retained executable path: %#v", got)
	}
	if !got.LastModifiedCursor.Equal(base) || !got.Gap.SafeCursor.Equal(base) {
		t.Fatalf("invalid continuation advanced safe cursor: poll=%#v base=%s", got, base)
	}
	if requests != 1 {
		t.Fatalf("invalid continuation requests = %d, want one", requests)
	}
}

func TestPollFrontierInvalidNextLinkBoundarySurvivesPendingReceiptReplay(t *testing.T) {
	message := bridgePollMessage("boundary-message", "2026-08-31T04:01:00Z", "boundary message")
	page, err := pendingPageFromWindow(
		"chat-boundary-replay",
		"/chats/chat-boundary-replay/messages?$top=20",
		pollFrontierHead,
		1,
		MessageWindow{
			Messages:       []ChatMessage{message},
			boundaryReason: "Graph message page nextLink is invalid: host mismatch",
		},
		false,
	)
	if err != nil {
		t.Fatalf("stage invalid-nextLink page: %v", err)
	}
	if page.BoundaryReason == "" {
		t.Fatal("invalid-nextLink page did not retain a boundary reason")
	}
	replayed, err := pendingPageToWindow(page)
	if err != nil {
		t.Fatalf("replay invalid-nextLink page: %v", err)
	}
	if replayed.boundaryReason != page.BoundaryReason {
		t.Fatalf("replayed boundary reason = %q, want %q", replayed.boundaryReason, page.BoundaryReason)
	}
	if len(replayed.Messages) != 1 || replayed.Messages[0].ID != message.ID {
		t.Fatalf("replayed boundary messages = %#v, want %s", replayed.Messages, message.ID)
	}
}

func TestPollFrontierUnknownContinuationSafeCursorUsesConservativeBoundary(t *testing.T) {
	now := time.Date(2026, 8, 31, 5, 0, 0, 0, time.UTC)
	poll := teamstore.ChatPollState{
		ChatID:             "chat-legacy-continuation",
		LastModifiedCursor: now,
		ContinuationPath:   "/chats/chat-legacy-continuation/messages?$skiptoken=legacy",
	}
	openPollGap(&poll, "unverified-continuation", "legacy cursor proof missing", poll.ContinuationPath, now.Add(time.Minute))
	if poll.Gap == nil || !poll.Gap.SafeCursor.IsZero() {
		t.Fatalf("legacy continuation gap safe cursor = %#v, want conservative zero boundary", poll.Gap)
	}
	if poll.ContinuationSafeCursorKnown {
		t.Fatal("opened gap retained continuation safe-cursor proof")
	}
}

// Deterministic source-boundary failures on the normal head must also become
// recoverable gaps.  This prevents a bad provider nextLink or an identity-less
// oversized record from being retried as the same head forever while keeping
// the previous cursor as the recovery boundary.
func TestPollFrontierMalformedHeadResponseOpensBoundedGap(t *testing.T) {
	base := time.Date(2026, 8, 31, 4, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		raw  func() []byte
		want error
	}{
		{
			name: "invalid-next-link",
			raw: func() []byte {
				message := bridgePollMessage("head-valid-message", base.Add(time.Minute).Format(time.RFC3339Nano), "head message")
				payload, err := json.Marshal(map[string]any{
					"value":           []ChatMessage{message},
					"@odata.nextLink": "https://evil.example/chats/chat-malformed-head/messages?$skiptoken=evil",
				})
				if err != nil {
					panic(err)
				}
				return payload
			},
			want: errGraphNextLinkInvalid,
		},
		{
			name: "empty-body",
			raw: func() []byte {
				return nil
			},
			want: errGraphMessagePageInvalid,
		},
		{
			name: "malformed-json",
			raw: func() []byte {
				return []byte("{")
			},
			want: errGraphMessagePageInvalid,
		},
		{
			name: "oversized-without-id",
			raw: func() []byte {
				record, err := json.Marshal(map[string]any{
					"createdDateTime":      base.Add(time.Minute).Format(time.RFC3339Nano),
					"lastModifiedDateTime": base.Add(time.Minute).Format(time.RFC3339Nano),
					"body": map[string]string{
						"contentType": "html",
						"content":     strings.Repeat("x", maxGraphMessageRecordBytes),
					},
				})
				if err != nil {
					panic(err)
				}
				payload, err := json.Marshal(map[string]any{"value": []json.RawMessage{record}})
				if err != nil {
					panic(err)
				}
				return payload
			},
			want: errGraphMessagePageInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			chatID := "chat-malformed-head-" + tc.name
			payload := tc.raw()
			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/chats/"+chatID+"/messages" {
					t.Errorf("head path = %q, want chat path", r.URL.Path)
				}
				requests++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(payload)
			}))
			t.Cleanup(server.Close)
			graph := newTestGraphClient(&fakeGraphAuth{token: "access"}, server, nil)
			store := newBridgeTestStore(t)
			if _, err := store.RecordChatPollSuccess(ctx, chatID, base, true, false, 0); err != nil {
				t.Fatalf("seed head poll: %v", err)
			}
			poll, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("load head poll: ok=%v err=%v", ok, err)
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			var handled []string
			_, pollErr := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, poll, true, pollChatWithRoleOptions{}, func(_ context.Context, msg ChatMessage, _ string) error {
				handled = append(handled, msg.ID)
				return nil
			})
			if tc.name == "invalid-next-link" {
				if pollErr != nil {
					t.Fatalf("invalid nextLink returned error after preserving page: %v", pollErr)
				}
				if got := strings.Join(handled, ","); got != "head-valid-message" {
					t.Fatalf("invalid nextLink handled records = %q, want head-valid-message", got)
				}
			} else if !errors.Is(pollErr, tc.want) {
				t.Fatalf("malformed head error = %v, want %v", pollErr, tc.want)
			}
			got, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("load bounded head gap: ok=%v err=%v", ok, err)
			}
			if got.Gap == nil {
				t.Fatalf("malformed head did not open bounded gap: %#v", got)
			}
			if got.ContinuationPath != "" || !got.LastModifiedCursor.Equal(base) || !got.Gap.SafeCursor.Equal(base) {
				t.Fatalf("malformed head changed executable/cursor state: %#v", got)
			}
			if tc.name == "invalid-next-link" {
				wantRecoveryCursor := base.Add(time.Minute)
				if !got.Gap.RecoveryCursor.Equal(wantRecoveryCursor) {
					t.Fatalf("invalid nextLink recovery cursor = %s, want %s", got.Gap.RecoveryCursor, wantRecoveryCursor)
				}
			}
			if requests != 1 {
				t.Fatalf("malformed head requests = %d, want one", requests)
			}
		})
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

func TestPollFrontierLongRotatingContinuationStopsAtDurablePageBudget(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			now := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)
			chatID := "chat-long-rotating-cycle"
			paths := make([]string, continuationPageBudget+1)
			for i := range paths {
				paths[i] = fmt.Sprintf("/chats/%s/messages?$skiptoken=cycle-%02d", chatID, i)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ChatPolls[chatID] = teamstore.ChatPollState{
					ChatID:             chatID,
					Seeded:             true,
					PollState:          inboundPollStateWarm,
					NextPollAt:         now,
					LastActivityAt:     now,
					LastModifiedCursor: now.Add(-time.Hour),
					ContinuationPath:   paths[0],
					FrontierEpoch:      1,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed long rotating continuation: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate long rotating continuation: %v", err)
				}
			}
			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("$skiptoken") == "" {
					t.Fatalf("long rotating continuation unexpectedly read the head: %s", r.URL.String())
				}
				raw := strings.TrimPrefix(r.URL.Query().Get("$skiptoken"), "cycle-")
				index := 0
				if _, err := fmt.Sscanf(raw, "%d", &index); err != nil || index < 0 || index >= len(paths)-1 {
					t.Fatalf("unexpected rotating continuation token %q: %v", raw, err)
				}
				requests++
				writePollFrontierPage(t, w, []ChatMessage{
					bridgePollMessage(fmt.Sprintf("long-cycle-%02d", index), now.Add(time.Duration(index+1)*time.Minute).Format(time.RFC3339Nano), fmt.Sprintf("long cycle page %02d", index)),
				}, paths[index+1])
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
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			for i := 0; i < continuationPageBudget; i++ {
				if _, err := bridge.pollChat(ctx, chatID, 20, func(context.Context, ChatMessage, string) error { return nil }); err != nil {
					t.Fatalf("rotating continuation page %d: %v", i+1, err)
				}
			}
			poll, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("read long-cycle poll: ok=%v err=%v", ok, err)
			}
			if requests != continuationPageBudget {
				t.Fatalf("rotating continuation requests = %d, want durable budget %d", requests, continuationPageBudget)
			}
			if poll.ContinuationPath != "" || poll.Gap == nil || poll.Gap.Kind != "continuation-page-budget" || poll.ContinuationPageCount != 0 || poll.PollState == inboundPollStateBlocked {
				t.Fatalf("long rotating continuation was not isolated at durable budget: %#v", poll)
			}
		})
	}
}

func TestPollFrontierGapPageBudgetSchedulesHeadProbe(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			now := time.Now().UTC()
			chatID := "chat-gap-budget"
			recoveryPath := "/chats/" + chatID + "/messages?$skiptoken=gap-cycle-0"
			nextPath := "/chats/" + chatID + "/messages?$skiptoken=gap-cycle-1"
			message := bridgePollMessage("gap-budget-record", now.Add(-90*time.Minute).Format(time.RFC3339Nano), "gap page")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("$skiptoken") != "gap-cycle-0" {
					t.Fatalf("unexpected gap-budget request: %s", r.URL.String())
				}
				writePollFrontierPage(t, w, []ChatMessage{message}, nextPath)
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
				state.ChatPolls[chatID] = teamstore.ChatPollState{
					ChatID:                chatID,
					Seeded:                true,
					PollState:             inboundPollStateWarm,
					NextPollAt:            now,
					LastActivityAt:        now,
					LastModifiedCursor:    now.Add(-2 * time.Hour),
					ContinuationPageCount: continuationPageBudget - 1,
					FrontierEpoch:         1,
					Gap: &teamstore.ChatPollGap{
						Epoch:          1,
						Kind:           "unverified-continuation",
						SafeCursor:     now.Add(-2 * time.Hour),
						RecoveryCursor: now.Add(-time.Hour),
						RecoveryPath:   recoveryPath,
						OpenedAt:       now,
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed gap page-budget frontier: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate gap page-budget frontier: %v", err)
				}
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			if _, err := bridge.pollChat(ctx, chatID, 20, func(context.Context, ChatMessage, string) error { return nil }); err != nil {
				t.Fatalf("gap page-budget poll: %v", err)
			}
			poll, ok, err := store.ChatPoll(ctx, chatID)
			if err != nil || !ok {
				t.Fatalf("read gap page-budget state: ok=%v err=%v", ok, err)
			}
			if poll.Gap == nil || poll.Gap.RecoveryPath != "" || !poll.Gap.HeadProbePending || poll.ContinuationPageCount != 0 {
				t.Fatalf("gap page-budget state = %#v, want head probe with retained gap", poll)
			}
			frontier, requestPath, _ := pollPageRequestForState(chatID, 20, inboundPollRoleWork, poll)
			if frontier != pollFrontierHead || strings.Contains(requestPath, "$skiptoken=") {
				t.Fatalf("gap page-budget next action = %q %q, want bounded head probe", frontier, requestPath)
			}
		})
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
	_, expectedPath, _ := pollPageRequestForState("chat-empty-gap", 20, inboundPollRoleWork, teamstore.ChatPollState{
		ChatID: "chat-empty-gap", Gap: &teamstore.ChatPollGap{SafeCursor: now.Add(-time.Hour), RecoveryCursor: now.Add(-time.Hour)},
	})
	attempt, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
		ChatID:             "chat-empty-gap",
		Owner:              "machine-a",
		ProcessIncarnation: "process-a",
		LeaseGeneration:    0,
		ExpectedFrontier:   pollFrontierIdentity(pollFrontierGap, expectedPath),
		Now:                now,
	})
	if err != nil || !acquired || attempt.Attempt == nil {
		t.Fatalf("begin empty recovery attempt: acquired=%v attempt=%#v err=%v", acquired, attempt, err)
	}
	path := expectedPath
	committed, err := bridge.commitPollAttemptSuccess(ctx, "chat-empty-gap", attempt.Attempt.ID, attempt.PollRevision, inboundPollRoleWork, false, pollFrontierGap, path, MessageWindow{}, pollMessageWindowResult{PageComplete: true}, false)
	if err != nil || !committed {
		t.Fatalf("commit empty recovery page: committed=%v err=%v", committed, err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-empty-gap")
	if err != nil || !ok {
		t.Fatalf("read empty recovery state: ok=%v err=%v", ok, err)
	}
	if poll.Gap == nil || poll.Gap.RecoveryPath != "" || !poll.Gap.HeadProbePending {
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

func TestPollFrontierDeduplicatedGapPageDoesNotClearGap(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Date(2026, 8, 31, 6, 0, 0, 0, time.UTC)
	const chatID = "chat-deduplicated-gap"
	safe := now.Add(-2 * time.Hour)
	seen := now.Add(-time.Hour)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID:             chatID,
			Seeded:             true,
			PollState:          inboundPollStateWarm,
			LastModifiedCursor: safe,
			Gap: &teamstore.ChatPollGap{
				Epoch:          1,
				Kind:           "unverified-continuation",
				SafeCursor:     safe,
				RecoveryCursor: seen,
				RecoveryPath:   "/chats/" + chatID + "/messages?$skiptoken=deduplicated",
				OpenedAt:       now,
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed deduplicated gap: %v", err)
	}
	attempt, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
		ChatID:             chatID,
		Owner:              "machine-a",
		ProcessIncarnation: "process-a",
		Now:                time.Now(),
	})
	if err != nil || !acquired || attempt.Attempt == nil {
		t.Fatalf("begin deduplicated gap attempt: acquired=%v attempt=%#v err=%v", acquired, attempt, err)
	}
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	result := pollMessageWindowResult{PageComplete: true, MinModified: seen, MaxModified: seen, Fetched: 1}
	committed, err := bridge.commitPollAttemptSuccess(ctx, chatID, attempt.Attempt.ID, attempt.PollRevision, inboundPollRoleWork, false, pollFrontierGap, attempt.Attempt.ExpectedFrontier, MessageWindow{}, result, false)
	if err != nil || !committed {
		t.Fatalf("commit deduplicated gap page: committed=%v err=%v", committed, err)
	}
	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok {
		t.Fatalf("read deduplicated gap: ok=%v err=%v", ok, err)
	}
	if poll.Gap == nil || !poll.Gap.HeadProbePending {
		t.Fatalf("deduplicated gap page closed recovery gap: %#v", poll)
	}
	if poll.Attempt != nil {
		t.Fatalf("deduplicated gap attempt was not released: %#v", poll.Attempt)
	}
}

func TestPollFrontierEmptyGapRecoveryTakesHeadProbeWithoutDroppingGap(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	const chatID = "chat-empty-gap-head-probe"
	const expiredPath = "/chats/" + chatID + "/messages?$skiptoken=expired"
	newMessage := bridgePollMessage("head-after-empty-gap", now.Add(-30*time.Minute).Format(time.RFC3339Nano), "new message after empty recovery")
	var requests []string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.String())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Query().Get("$skiptoken") == "expired":
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"Gone","message":"expired continuation"}}`)
		case strings.Contains(r.URL.Query().Get("$filter"), "lastModifiedDateTime ge "):
			// The directional gap is still empty. The next quantum must not
			// remain trapped here forever.
			writePollFrontierPage(t, w, nil, "")
		default:
			writePollFrontierPage(t, w, []ChatMessage{newMessage}, "")
		}
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
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID: chatID, Seeded: true, PollState: inboundPollStateWarm,
			NextPollAt: now, LastActivityAt: now, LastModifiedCursor: now.Add(-time.Hour),
			ContinuationSafeCursor: now.Add(-time.Hour), ContinuationSafeCursorKnown: true,
			ContinuationPath: expiredPath, FrontierEpoch: 1,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed empty-gap head probe state: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}
	if _, err := bridge.pollChat(ctx, chatID, 20, handle); err == nil {
		t.Fatal("expired continuation unexpectedly succeeded")
	}
	if _, err := bridge.pollChat(ctx, chatID, 20, handle); err != nil {
		t.Fatalf("empty gap recovery page: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok || poll.Gap == nil || !poll.Gap.HeadProbePending {
		t.Fatalf("empty gap did not retain one-shot head probe: %#v ok=%v err=%v", poll, ok, err)
	}
	if _, err := bridge.pollChat(ctx, chatID, 20, handle); err != nil {
		t.Fatalf("head probe after empty gap: %v", err)
	}
	if got := strings.Join(handled, ","); got != newMessage.ID {
		t.Fatalf("head probe handled = %q, want %q", got, newMessage.ID)
	}
	poll, ok, err = store.ChatPoll(ctx, chatID)
	if err != nil || !ok || poll.Gap == nil || poll.Gap.HeadProbePending {
		t.Fatalf("head probe dropped gap or remained pending: %#v ok=%v err=%v", poll, ok, err)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 3 {
		t.Fatalf("head probe request count = %d, want expired continuation, empty gap, and head probe: %v", len(gotRequests), gotRequests)
	}
}

func TestPollFrontierHeadProbeRetainsTruncatedPageForGapRecovery(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	const chatID = "chat-empty-gap-truncated-head"
	const expiredPath = "/chats/" + chatID + "/messages?$skiptoken=expired"
	const olderPath = "/chats/" + chatID + "/messages?$skiptoken=older"
	known := bridgePollMessage("already-delivered", now.Add(-30*time.Minute).Format(time.RFC3339Nano), "already delivered")
	old := bridgePollMessage("older-actionable", now.Add(-45*time.Minute).Format(time.RFC3339Nano), "older actionable message")
	var mu sync.Mutex
	var requests []string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.String())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "expired":
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"Gone","message":"expired continuation"}}`)
		case "older":
			writePollFrontierPage(t, w, []ChatMessage{old}, "")
		default:
			if strings.Contains(r.URL.Query().Get("$filter"), "lastModifiedDateTime ge ") {
				// Empty recovery opens a one-shot head probe. The probe sees only
				// an already-known message, but Graph also gives it an opaque older
				// page containing the actionable record. That link belongs to the
				// directional gap and must survive the head commit.
				writePollFrontierPage(t, w, nil, "")
				return
			}
			if strings.Contains(r.URL.Query().Get("$filter"), "lastModifiedDateTime gt ") {
				writePollFrontierPage(t, w, []ChatMessage{known}, server.URL+olderPath)
				return
			}
			http.Error(w, "unexpected head request", http.StatusBadRequest)
		}
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
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID: chatID, Seeded: true, PollState: inboundPollStateWarm,
			NextPollAt: now, LastActivityAt: now, LastModifiedCursor: now.Add(-time.Hour),
			ContinuationSafeCursor: now.Add(-time.Hour), ContinuationSafeCursorKnown: true,
			ContinuationPath: expiredPath, FrontierEpoch: 1,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed truncated head-probe state: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.markRegistrySeen(chatID, known.ID)
	var handled []string
	if _, err := bridge.pollChat(ctx, chatID, 20, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err == nil {
		t.Fatal("expired continuation unexpectedly succeeded")
	}
	if _, err := bridge.pollChat(ctx, chatID, 20, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("empty gap recovery page: %v", err)
	}
	if _, err := bridge.pollChat(ctx, chatID, 20, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("truncated head probe: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil || !ok || poll.Gap == nil || poll.Gap.RecoveryPath == "" || poll.Gap.HeadProbePending {
		t.Fatalf("head probe did not preserve recovery path: %#v ok=%v err=%v", poll, ok, err)
	}
	if _, err := bridge.pollChat(ctx, chatID, 20, func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("recovery page after head probe: %v", err)
	}
	if strings.Join(handled, ",") != old.ID {
		t.Fatalf("handled records = %q, want only recovered actionable %q", strings.Join(handled, ","), old.ID)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 4 || !strings.Contains(gotRequests[3], "skiptoken=older") {
		t.Fatalf("request trace = %v, want head-probe link retained for recovery", gotRequests)
	}
}

func writePollFrontierPage(t *testing.T, w http.ResponseWriter, messages []ChatMessage, nextPath string) {
	t.Helper()
	// A nil Go slice marshals as JSON null. An empty Graph collection is still
	// an array, so make the valid empty-page fixture explicit.
	if messages == nil {
		messages = []ChatMessage{}
	}
	payload := map[string]any{"value": messages}
	if strings.TrimSpace(nextPath) != "" {
		payload["@odata.nextLink"] = nextPath
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode Graph page: %v", err)
	}
}
