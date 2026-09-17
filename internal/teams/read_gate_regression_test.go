package teams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// expireGraphReadGateForTest models the Retry-After interval elapsing without
// weakening the production gate. Tests that deliberately issue another
// logical attempt immediately must advance both the in-process fence and the
// durable poll deadline; clearing only the SQLite row is not sufficient after
// a worker has already observed a 429.
func expireGraphReadGateForTest(t *testing.T, bridge *Bridge, store *teamstore.Store, chatID string) {
	t.Helper()
	chatID = strings.TrimSpace(chatID)
	if bridge != nil {
		bridge.graphReadGateMu.Lock()
		if bridge.graphReadChatLocalUntil != nil {
			delete(bridge.graphReadChatLocalUntil, chatID)
		}
		if bridge.graphReadChatPending != nil {
			delete(bridge.graphReadChatPending, chatID)
		}
		bridge.graphReadGateMu.Unlock()
	}
	if store == nil || chatID == "" {
		return
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		poll, ok := state.ChatPolls[chatID]
		if !ok {
			return nil
		}
		poll.NextPollAt = time.Now().UTC().Add(-time.Second)
		poll.BlockedUntil = time.Time{}
		state.ChatPolls[chatID] = poll
		return nil
	}); err != nil {
		t.Fatalf("expire Graph read gate for %s: %v", chatID, err)
	}
}

func TestTeamsMessagePreparationRequiresGraphClassifiesOnlyGraphBackedShapes(t *testing.T) {
	tests := []struct {
		name string
		msg  ChatMessage
		want bool
	}{
		{
			name: "plain text",
			msg:  bridgeTestMessageWithText("plain", "local prompt"),
			want: false,
		},
		{
			name: "hosted inline content",
			msg: ChatMessage{
				ID: "hosted",
				Body: struct {
					ContentType string `json:"contentType"`
					Content     string `json:"content"`
				}{Content: `<p><img src="../hostedContents/content-1/$value"></p>`},
			},
			want: true,
		},
		{
			name: "sharepoint reference",
			msg: ChatMessage{Attachments: []MessageAttachment{{
				ContentType: "reference",
				ContentURL:  "https://contoso.sharepoint.com/sites/team/file.docx",
			}}},
			want: true,
		},
		{
			name: "message reference",
			msg: ChatMessage{Attachments: []MessageAttachment{{
				ID:          "quoted-1",
				ContentType: "messageReference",
				Content:     `{"messageId":"quoted-1","messagePreview":"local preview"}`,
			}}},
			want: true,
		},
		{
			name: "forwarded reference preview",
			msg: ChatMessage{Attachments: []MessageAttachment{{
				ID:          "forwarded-1",
				ContentType: "forwardedMessageReference",
				Content:     `{"messageId":"forwarded-1","messagePreview":"preview only"}`,
			}}},
			want: false,
		},
		{
			name: "unsupported attachment",
			msg: ChatMessage{Attachments: []MessageAttachment{{
				ID:          "opaque-1",
				ContentType: "application/octet-stream",
			}}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := teamsMessagePreparationRequiresGraph(tt.msg); got != tt.want {
				t.Fatalf("teamsMessagePreparationRequiresGraph() = %v, want %v for %#v", got, tt.want, tt.msg)
			}
		})
	}
}

func TestGraphReadFailureClassificationDoesNotUseUnrelatedDiagnosticText(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "local timeout wording", err: errors.New("invalid timeout configuration"), want: false},
		{name: "local attachment wording", err: errors.New("attachment connection reset while reading local file"), want: false},
		{name: "permanent graph status with timeout body", err: &GraphStatusError{Method: http.MethodGet, StatusCode: http.StatusBadRequest, Message: "timeout is not a valid filter"}, want: false},
		{name: "graph rate limit", err: &GraphStatusError{Method: http.MethodGet, StatusCode: http.StatusTooManyRequests}, want: true},
		{name: "context deadline", err: context.DeadlineExceeded, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isRetryableGraphReadFailure(test.err); got != test.want {
				t.Fatalf("isRetryableGraphReadFailure(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestGraphReadRetryMarkerPreventsDuplicateDurableGate(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	chatID := "chat-nested-read-retry-marker"
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls[chatID] = teamstore.ChatPollState{ChatID: chatID, Seeded: true, PollState: inboundPollStateWarm}
		return nil
	}); err != nil {
		t.Fatalf("seed retry-marker poll: %v", err)
	}
	readErr := &GraphStatusError{Method: http.MethodGet, Path: "/chats/" + chatID + "/messages/m1", StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute}
	marked := bridge.recordGraphReadRetryableFailureAndMark(ctx, chatID, readErr)
	if marked == nil || !graphReadRetryWasRecorded(marked) {
		t.Fatalf("marked retry error = %v, want recorded marker", marked)
	}
	first, found, err := store.ChatPoll(ctx, chatID)
	if err != nil || !found {
		t.Fatalf("read first retry gate: found=%v err=%v poll=%#v", found, err, first)
	}
	if first.FailureCount != 1 {
		t.Fatalf("first retry gate failure count = %d, want one", first.FailureCount)
	}
	if err := bridge.recordGraphReadRetryableFailure(ctx, chatID, marked); err != nil {
		t.Fatalf("record marked retry error a second time: %v", err)
	}
	second, found, err := store.ChatPoll(ctx, chatID)
	if err != nil || !found {
		t.Fatalf("read second retry gate: found=%v err=%v poll=%#v", found, err, second)
	}
	if second.FailureCount != first.FailureCount {
		t.Fatalf("nested retry marker was recorded twice: first=%#v second=%#v", first, second)
	}
}

func TestAccountReadGatePersistenceFailureIsReturnedAndFenced(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	if err := store.Update(ctx, func(*teamstore.State) error { return nil }); err != nil {
		t.Fatalf("materialize state before account-gate failure: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate store before account-gate failure: %v", err)
	}
	databasePath := filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName)
	if err := store.Close(); err != nil {
		t.Fatalf("close store before account-gate failure: %v", err)
	}
	if err := os.Rename(databasePath, databasePath+".unavailable"); err != nil {
		t.Fatalf("move disposable SQLite file out of the way: %v", err)
	}
	if err := os.Mkdir(databasePath, 0o700); err != nil {
		t.Fatalf("replace disposable SQLite path with unavailable directory: %v", err)
	}
	err := bridge.recordGraphReadAccountRateLimit(ctx, &GraphStatusError{
		Method: http.MethodGet, Path: "/chats/chat-account-gate-failure/messages", StatusCode: http.StatusTooManyRequests,
		RateLimitScope: "account", RetryAfter: time.Minute,
	})
	if err == nil {
		t.Fatal("account-gate persistence failure was swallowed")
	}
	bridge.graphReadGateMu.Lock()
	pendingUntil := bridge.graphReadAccountPendingUntil
	bridge.graphReadGateMu.Unlock()
	if !pendingUntil.After(time.Now()) {
		t.Fatalf("account-gate persistence failure did not retain a process-local retry fence: %s", pendingUntil)
	}
}

func TestBridgePreparationStopsGraphBackedAttachmentBeforeAnyReadDuringAccountGate(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	if _, err := store.SetChatRateLimit(ctx, graphReadAccountRateLimitKey, time.Now().Add(time.Hour), "account read gate"); err != nil {
		t.Fatalf("seed account read gate: %v", err)
	}
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	session := &Session{ID: "s001", ChatID: "chat-1"}
	msg := ChatMessage{ID: "hosted-gated"}
	msg.Body.Content = `<p><img src="../hostedContents/content-1/$value"></p>`
	_, cleanup, warning, err := bridge.prepareSessionPromptFromTeamsMessage(ctx, session, "turn-gated", session.ChatID, msg, "prompt")
	cleanup()
	if warning != "" {
		t.Fatalf("preparation warning = %q, want no warning before Graph access", warning)
	}
	var gateErr *graphReadGateActiveError
	if !errors.As(err, &gateErr) {
		t.Fatalf("preparation error = %v, want graph read gate error", err)
	}
}

func TestBridgeRetryTurnDoesNotFetchOriginalMessageDuringAccountReadGate(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridgePlaceholder := newBridgeTestBridge(nil, store, &recordingExecutor{})
	session := bridgePlaceholder.reg.SessionByChatID("chat-1")
	if session == nil {
		t.Fatal("test bridge did not contain chat-1 session")
	}
	if err := bridgePlaceholder.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}
	inbound, _, err := store.PersistInbound(ctx, teamstore.InboundEvent{
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "original-gated",
		Text:           "retry original",
		Status:         teamstore.InboundStatusPersisted,
	})
	if err != nil {
		t.Fatalf("PersistInbound: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, teamstore.Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn: %v", err)
	}
	if _, err := store.MarkTurnFailed(ctx, turn.ID, "previous execution failed"); err != nil {
		t.Fatalf("MarkTurnFailed: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, graphReadAccountRateLimitKey, time.Now().Add(time.Hour), "account read gate"); err != nil {
		t.Fatalf("seed account read gate: %v", err)
	}

	var gets atomic.Int32
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"value":[]}`)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		posts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"retry-gate-notice","messageType":"message"}`)
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
	if err := bridge.retryTurnCommand(ctx, session, turn.ID, "retry-command"); err != nil {
		t.Fatalf("retryTurnCommand: %v", err)
	}
	if got := gets.Load(); got != 0 {
		t.Fatalf("retry command fetched original message %d times while account gate was active, want 0", got)
	}
	if got := posts.Load(); got == 0 {
		t.Fatal("retry command did not send a durable pause notice")
	}
}

func TestBridgeStartupRecoveryScoped429GatesFollowingQueuedTurn(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	var firstGets atomic.Int32
	var secondGets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chats/chat-startup-1/messages/message-startup-1":
			firstGets.Add(1)
			w.Header().Set("X-CXP-RateLimit-Scope", "account")
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"startup read throttle"}}`, http.StatusTooManyRequests)
		case "/chats/chat-startup-2/messages/message-startup-2":
			secondGets.Add(1)
			http.Error(w, `{"error":{"code":"unexpected","message":"account gate was bypassed"}}`, http.StatusInternalServerError)
		default:
			t.Errorf("unexpected startup recovery request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
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
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	firstSession := Session{ID: "startup-session-1", ChatID: "chat-startup-1", Status: "active", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	secondSession := Session{ID: "startup-session-2", ChatID: "chat-startup-2", Status: "active", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := bridge.ensureDurableSession(ctx, &firstSession); err != nil {
		t.Fatalf("ensure first startup session: %v", err)
	}
	if err := bridge.ensureDurableSession(ctx, &secondSession); err != nil {
		t.Fatalf("ensure second startup session: %v", err)
	}
	firstCreated := time.Now().UTC().Add(-2 * time.Second)
	secondCreated := firstCreated.Add(time.Second)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.InboundEvents["startup-inbound-1"] = teamstore.InboundEvent{
			ID: "startup-inbound-1", SessionID: firstSession.ID, TeamsChatID: firstSession.ChatID,
			TeamsMessageID: "message-startup-1", TeamsBodyType: "html", Source: "teams",
			Status: teamstore.InboundStatusPersisted, CreatedAt: firstCreated, UpdatedAt: firstCreated,
		}
		state.InboundEvents["startup-inbound-2"] = teamstore.InboundEvent{
			ID: "startup-inbound-2", SessionID: secondSession.ID, TeamsChatID: secondSession.ChatID,
			TeamsMessageID: "message-startup-2", TeamsBodyType: "html", Source: "teams",
			Status: teamstore.InboundStatusPersisted, CreatedAt: secondCreated, UpdatedAt: secondCreated,
		}
		state.Turns["startup-turn-1"] = teamstore.Turn{
			ID: "startup-turn-1", SessionID: firstSession.ID, InboundEventID: "startup-inbound-1",
			Status: teamstore.TurnStatusQueued, CreatedAt: firstCreated, UpdatedAt: firstCreated,
		}
		state.Turns["startup-turn-2"] = teamstore.Turn{
			ID: "startup-turn-2", SessionID: secondSession.ID, InboundEventID: "startup-inbound-2",
			Status: teamstore.TurnStatusQueued, CreatedAt: secondCreated, UpdatedAt: secondCreated,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed startup queued turns: %v", err)
	}
	if err := bridge.recoverUnfinishedTurns(ctx); err != nil {
		t.Fatalf("recoverUnfinishedTurns: %v", err)
	}
	if got := firstGets.Load(); got != 1 {
		t.Fatalf("first startup message GETs = %d, want one throttled read", got)
	}
	if got := secondGets.Load(); got != 0 {
		t.Fatalf("second startup message GETs = %d, want zero while account gate is active", got)
	}
	if limit, ok, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil || !ok || !limit.BlockedUntil.After(time.Now()) {
		t.Fatalf("startup account read gate = found:%v err:%v limit:%#v, want durable future gate", ok, err, limit)
	}
}

func TestBridgeMessageReferenceScoped429StopsSiblingReads(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	var firstGets atomic.Int32
	var secondGets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chats/chat-reference-gated/messages/reference-1":
			firstGets.Add(1)
			w.Header().Set("X-CXP-RateLimit-Scope", "account")
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"reference throttle"}}`, http.StatusTooManyRequests)
		case "/chats/chat-reference-gated/messages/reference-2":
			secondGets.Add(1)
			http.Error(w, `{"error":{"code":"unexpected","message":"sibling reference was read"}}`, http.StatusInternalServerError)
		default:
			t.Errorf("unexpected reference request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
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
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := ChatMessage{Attachments: []MessageAttachment{
		{ID: "reference-1", ContentType: "messageReference", Content: `{"messageId":"reference-1","messagePreview":"first"}`},
		{ID: "reference-2", ContentType: "messageReference", Content: `{"messageId":"reference-2","messagePreview":"second"}`},
	}}
	if _, _, err := bridge.readMessageReferenceAttachments(ctx, "chat-reference-gated", msg); err == nil || !isRetryableGraphReadFailure(err) {
		t.Fatalf("readMessageReferenceAttachments error = %v, want retryable Graph error", err)
	}
	if got := firstGets.Load(); got != 1 {
		t.Fatalf("first reference GETs = %d, want one", got)
	}
	if got := secondGets.Load(); got != 0 {
		t.Fatalf("second reference GETs = %d, want zero after first 429", got)
	}
	if limit, ok, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil || !ok || !limit.BlockedUntil.After(time.Now()) {
		t.Fatalf("reference account read gate = found:%v err:%v limit:%#v, want durable future gate", ok, err, limit)
	}
}

func TestBridgeReferenceMetadataScoped429StopsContentDownload(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	t.Setenv("CODEX_HELPER_TEAMS_READ_SCOPES", "openid profile offline_access User.Read Chat.Read Files.Read")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS", "contoso.sharepoint.com")
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", filepath.Join(tmp, "read-token.json"))
	var metadataGets atomic.Int32
	var contentGets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/driveItem/content") {
			contentGets.Add(1)
			http.Error(w, "content must not be read after metadata 429", http.StatusInternalServerError)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/driveItem") {
			metadataGets.Add(1)
			w.Header().Set("X-CXP-RateLimit-Scope", "global")
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"metadata throttle"}}`, http.StatusTooManyRequests)
			return
		}
		t.Errorf("unexpected metadata request: %s %s", r.Method, r.URL.String())
		http.Error(w, "unexpected request", http.StatusBadRequest)
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
	msg := ChatMessage{ID: "reference-file-gated", Attachments: []MessageAttachment{{
		ContentType: "reference",
		ContentURL:  "https://contoso.sharepoint.com/sites/team/opaque-file",
	}}}
	files, cleanup, warning, err := bridge.downloadReferenceFileAttachments(ctx, &Session{ID: "s-file", ChatID: "chat-reference-file-gated"}, msg)
	cleanup()
	if err == nil || !isRetryableGraphReadFailure(err) {
		t.Fatalf("downloadReferenceFileAttachments error = %v files=%#v warning=%q metadata_gets=%d, want retryable Graph error", err, files, warning, metadataGets.Load())
	}
	if warning != "" || len(files) != 0 {
		t.Fatalf("metadata-throttled download result files=%#v warning=%q, want no result", files, warning)
	}
	if got := metadataGets.Load(); got != 1 {
		t.Fatalf("metadata GETs = %d, want one", got)
	}
	if got := contentGets.Load(); got != 0 {
		t.Fatalf("content GETs = %d, want zero after metadata 429", got)
	}
	if limit, ok, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil || !ok || !limit.BlockedUntil.After(time.Now()) {
		t.Fatalf("metadata account read gate = found:%v err:%v limit:%#v, want durable future gate", ok, err, limit)
	}
}

func TestBridgeDelayedTurnInterruptRejectsStaleOwnerCapability(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	scope := teamstore.ScopeIdentity{ID: "scope-delayed-interrupt", AccountID: "account-delayed-interrupt", Profile: "default"}
	machineA := teamstore.MachineRecord{ID: "machine-delayed-a", ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary}
	machineB := teamstore.MachineRecord{ID: "machine-delayed-b", ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary}
	now := time.Now().UTC()
	ownerA, err := teamstore.CurrentOwner("delayed-interrupt-a", "", "", now)
	if err != nil {
		t.Fatalf("CurrentOwner A: %v", err)
	}
	ownerA.ScopeID, ownerA.MachineID = scope.ID, machineA.ID
	first, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Hour, Now: now})
	if err != nil || first.Mode != teamstore.LeaseModeActive {
		t.Fatalf("claim owner A: decision=%#v err=%v", first, err)
	}
	if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, first.Lease.Generation); err != nil || !released {
		t.Fatalf("release owner A: released=%v err=%v", released, err)
	}
	ownerB, err := teamstore.CurrentOwner("delayed-interrupt-b", "", "", now.Add(time.Second))
	if err != nil {
		t.Fatalf("CurrentOwner B: %v", err)
	}
	ownerB.ScopeID, ownerB.MachineID = scope.ID, machineB.ID
	second, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Hour, Now: now.Add(time.Second)})
	if err != nil || second.Mode != teamstore.LeaseModeActive {
		t.Fatalf("claim owner B: decision=%#v err=%v", second, err)
	}
	turnID := "turn-delayed-interrupt"
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns[turnID] = teamstore.Turn{
			ID: turnID, SessionID: "s-owner", Status: teamstore.TurnStatusQueued,
			MachineID: machineB.ID, LeaseGeneration: second.Lease.Generation,
			CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed replacement-owner turn: %v", err)
	}

	bridge := &Bridge{store: store}
	staleCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{
		MachineID: machineA.ID, LeaseGeneration: first.Lease.Generation,
	})
	marked, err := bridge.markTurnInterruptedUnlessTerminal(staleCtx, turnID, "late preparation callback")
	if !errors.Is(err, teamstore.ErrControlLeaseNotHeld) || marked {
		t.Fatalf("stale delayed interrupt marked=%v err=%v, want owner-fenced rejection", marked, err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load after stale delayed interrupt: %v", err)
	}
	if got := state.Turns[turnID].Status; got != teamstore.TurnStatusQueued {
		t.Fatalf("replacement owner's turn status=%s, want queued", got)
	}
}

func TestBridgeFailedChatGateWriteRemainsReadBlockedUntilDurableRetry(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	scope := teamstore.ScopeIdentity{ID: "scope-chat-gate-retry", AccountID: "account-chat-gate-retry", Profile: "default"}
	machineA := teamstore.MachineRecord{ID: "machine-chat-gate-a", ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary}
	machineB := teamstore.MachineRecord{ID: "machine-chat-gate-b", ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary}
	now := time.Now().UTC()
	ownerA, err := teamstore.CurrentOwner("chat-gate-retry-a", "", "", now)
	if err != nil {
		t.Fatalf("CurrentOwner A: %v", err)
	}
	ownerA.ScopeID, ownerA.MachineID = scope.ID, machineA.ID
	first, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Hour, Now: now})
	if err != nil || first.Mode != teamstore.LeaseModeActive {
		t.Fatalf("claim owner A: decision=%#v err=%v", first, err)
	}
	if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, first.Lease.Generation); err != nil || !released {
		t.Fatalf("release owner A: released=%v err=%v", released, err)
	}
	ownerB, err := teamstore.CurrentOwner("chat-gate-retry-b", "", "", now.Add(time.Second))
	if err != nil {
		t.Fatalf("CurrentOwner B: %v", err)
	}
	ownerB.ScopeID, ownerB.MachineID = scope.ID, machineB.ID
	second, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Hour, Now: now.Add(time.Second)})
	if err != nil || second.Mode != teamstore.LeaseModeActive {
		t.Fatalf("claim owner B: decision=%#v err=%v", second, err)
	}

	bridge := &Bridge{store: store}
	readErr := &GraphStatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute, Message: "chat read throttle"}
	staleCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{
		MachineID: machineA.ID, LeaseGeneration: first.Lease.Generation,
	})
	if err := bridge.recordGraphReadRetryableFailure(staleCtx, "chat-gate-retry", readErr); !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
		t.Fatalf("stale chat gate write error=%v, want owner fence", err)
	}
	if _, blocked := bridge.chatReadBlockedUntil(staleCtx, "chat-gate-retry"); !blocked {
		t.Fatal("chat read became eligible after failed durable gate write")
	}

	// Advance only the in-memory retry edge. The next admission must retry the
	// durable mutation with the replacement owner, not reopen Graph reads.
	bridge.graphReadGateMu.Lock()
	intent := bridge.graphReadChatPending["chat-gate-retry"]
	intent.RetryAt = time.Now().Add(-time.Second)
	bridge.graphReadChatPending["chat-gate-retry"] = intent
	bridge.graphReadGateMu.Unlock()
	currentCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{
		MachineID: machineB.ID, LeaseGeneration: second.Lease.Generation,
	})
	if until, blocked := bridge.chatReadBlockedUntil(currentCtx, "chat-gate-retry"); !blocked || !until.After(time.Now()) {
		t.Fatalf("replacement owner retry admission=%s/%v, want durable future gate", until, blocked)
	}
	poll, found, err := store.ChatPoll(ctx, "chat-gate-retry")
	if err != nil || !found || !poll.BlockedUntil.After(time.Now()) || !strings.Contains(poll.LastError, "429") {
		t.Fatalf("durable chat gate after replacement retry: found=%v err=%v poll=%#v", found, err, poll)
	}
	bridge.graphReadGateMu.Lock()
	_, pending := bridge.graphReadChatPending["chat-gate-retry"]
	bridge.graphReadGateMu.Unlock()
	if pending {
		t.Fatal("successful replacement-owner retry left a stale chat gate write intent")
	}
}

func TestGraphReadAccountGateIgnoresExplicitWriteThrottle(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	post429 := &GraphStatusError{
		Method: http.MethodPost, StatusCode: http.StatusTooManyRequests,
		RetryAfter: time.Minute, RateLimitScope: "account", Message: "write throttle",
	}
	bridge.recordGraphReadAccountRateLimit(ctx, post429)
	if _, ok, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil {
		t.Fatalf("read gate lookup after POST 429: %v", err)
	} else if ok {
		t.Fatal("explicit POST 429 incorrectly installed the account-wide Graph read gate")
	}
	if err := bridge.recordGraphReadRetryableFailure(ctx, "chat-write-only", post429); err != nil {
		t.Fatalf("chat read gate recorder returned explicit write throttle error: %v", err)
	}
	if _, found, err := store.ChatPoll(ctx, "chat-write-only"); err != nil {
		t.Fatalf("chat poll lookup after explicit write throttle: %v", err)
	} else if found {
		t.Fatal("explicit POST 429 incorrectly installed a chat-local Graph read gate")
	}

	get429 := &GraphStatusError{
		Method: http.MethodGet, StatusCode: http.StatusTooManyRequests,
		RetryAfter: time.Minute, RateLimitScope: "global", Message: "read throttle",
	}
	bridge.recordGraphReadAccountRateLimit(ctx, get429)
	if limit, ok, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil || !ok || !limit.BlockedUntil.After(time.Now()) {
		t.Fatalf("read gate after GET 429: found=%v err=%v limit=%#v, want durable future gate", ok, err, limit)
	}
}

func TestGraphReadAccountGateMarkerAvoidsDuplicateDurableWrite(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			read429 := &GraphStatusError{
				Method: http.MethodGet, StatusCode: http.StatusTooManyRequests,
				RateLimitScope: "account", RetryAfter: time.Hour, Message: "account read throttle",
			}
			if err := bridge.recordGraphReadAccountRateLimit(ctx, read429); err != nil {
				t.Fatalf("record first account read gate: %v", err)
			}
			first, found, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey)
			if err != nil || !found {
				t.Fatalf("read first account gate: found=%v err=%v limit=%#v", found, err, first)
			}

			// Audience admission records the independent account gate and passes
			// this marker outward. The enclosing poll must not write the same
			// durable row a second time.
			marked := markGraphReadAccountRateLimitRecorded(read429)
			if err := bridge.recordGraphReadAccountRateLimit(ctx, marked); err != nil {
				t.Fatalf("record marked account read gate: %v", err)
			}
			second, found, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey)
			if err != nil || !found {
				t.Fatalf("read second account gate: found=%v err=%v limit=%#v", found, err, second)
			}
			if second.BlockedUntil != first.BlockedUntil || second.UpdatedAt != first.UpdatedAt || second.Reason != first.Reason {
				t.Fatalf("marked account gate was durably rewritten: first=%#v second=%#v", first, second)
			}
		})
	}
}

func TestGraphReadRateLimitKeepsLocalFenceBeyondShortRetryAfter(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	minimum := time.Now().Add(graphReadGateStoreFailureBackoff - 100*time.Millisecond)

	account429 := &GraphStatusError{
		Method: http.MethodGet, StatusCode: http.StatusTooManyRequests,
		RateLimitScope: "account", RetryAfter: time.Second, Message: "short account read throttle",
	}
	if err := bridge.recordGraphReadAccountRateLimit(ctx, account429); err != nil {
		t.Fatalf("record account read throttle: %v", err)
	}
	bridge.graphReadGateMu.Lock()
	accountUntil := bridge.graphReadAccountLocalUntil
	bridge.graphReadGateMu.Unlock()
	if accountUntil.Before(minimum) {
		t.Fatalf("account local fence = %s, want at least %s after durable write window", accountUntil, minimum)
	}

	chat429 := &GraphStatusError{
		Method: http.MethodGet, StatusCode: http.StatusTooManyRequests,
		RetryAfter: time.Second, Message: "short chat read throttle",
	}
	if err := bridge.recordGraphReadRetryableFailure(ctx, "short-chat-read-gate", chat429); err != nil {
		t.Fatalf("record chat read throttle: %v", err)
	}
	bridge.graphReadGateMu.Lock()
	chatUntil := bridge.graphReadChatLocalUntil["short-chat-read-gate"]
	bridge.graphReadGateMu.Unlock()
	if chatUntil.Before(minimum) {
		t.Fatalf("chat local fence = %s, want at least %s after durable write window", chatUntil, minimum)
	}
}

func TestGraphReadRateLimitDoesNotInstallOutboxWriteGate(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			read429 := &GraphStatusError{
				Method: http.MethodGet, StatusCode: http.StatusTooManyRequests,
				RateLimitScope: "account", RetryAfter: time.Hour, Message: "read throttle",
			}
			bridge.recordGraphRateLimit(ctx, "read-throttled-chat", "outbox:read-throttled", read429)

			if _, found, err := store.OutboxChatRateLimit(ctx, "read-throttled-chat"); err != nil {
				t.Fatalf("read outbox gate after GET 429: %v", err)
			} else if found {
				t.Fatal("GET 429 installed an outbox write gate")
			}
			if poll, found, err := store.ChatPoll(ctx, "read-throttled-chat"); err != nil || !found || !poll.BlockedUntil.After(time.Now()) {
				t.Fatalf("GET 429 did not install chat read gate: found=%v err=%v poll=%#v", found, err, poll)
			}
			if limit, found, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil || !found || !limit.BlockedUntil.After(time.Now()) {
				t.Fatalf("account-scoped GET 429 did not install account read gate: found=%v err=%v limit=%#v", found, err, limit)
			}
			if _, found, err := store.ChatRateLimit(ctx, graphWriteAccountRateLimitKey); err != nil {
				t.Fatalf("write account gate lookup after GET 429: %v", err)
			} else if found {
				t.Fatal("GET 429 installed an account-wide write gate")
			}
		})
	}
}

func TestGraphWriteAccountGateBlocksSiblingOutboxWithoutBlockingReads(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			for _, chatID := range []string{"write-gate-chat-a", "write-gate-chat-b"} {
				if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
					ID: "outbox:" + chatID, TeamsChatID: chatID, Kind: "helper", Body: "pending",
				}); err != nil {
					t.Fatalf("QueueOutbox(%s): %v", chatID, err)
				}
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			throttle := &GraphStatusError{
				Method: http.MethodPost, StatusCode: http.StatusTooManyRequests,
				RetryAfter: time.Hour, RateLimitScope: "account", Message: "account write throttle",
			}
			bridge.recordGraphRateLimit(ctx, "write-gate-chat-a", "outbox:write-gate-chat-a", throttle)

			for _, chatID := range []string{"write-gate-chat-a", "write-gate-chat-b"} {
				limit, found, err := store.OutboxChatRateLimit(ctx, chatID)
				if err != nil || !found || !limit.BlockedUntil.After(time.Now()) {
					t.Fatalf("outbox gate for %s: found=%v err=%v limit=%#v", chatID, found, err, limit)
				}
				until, blocked, err := bridge.chatBlockedUntil(ctx, chatID)
				if err != nil || !blocked || !until.After(time.Now()) {
					t.Fatalf("sender gate for %s: until=%s blocked=%v err=%v", chatID, until, blocked, err)
				}
			}
			pending, err := store.PendingOutboxChatIDsAt(ctx, teamstore.PendingOutboxQuery{Now: time.Now().UTC()}, 8)
			if err != nil {
				t.Fatalf("pending chats behind account write gate: %v", err)
			}
			if len(pending) != 0 {
				t.Fatalf("account write gate leaked pending sibling chats: %#v", pending)
			}
			if _, found, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil {
				t.Fatalf("read gate lookup after write throttle: %v", err)
			} else if found {
				t.Fatal("account write throttle incorrectly installed the account-wide read gate")
			}

			if err := store.ClearChatRateLimit(ctx, graphWriteAccountRateLimitKey); err != nil {
				t.Fatalf("clear account write gate: %v", err)
			}
			if err := store.ClearChatRateLimit(ctx, "write-gate-chat-a"); err != nil {
				t.Fatalf("clear local write gate: %v", err)
			}
			pending, err = store.PendingOutboxChatIDsAt(ctx, teamstore.PendingOutboxQuery{Now: time.Now().UTC()}, 8)
			if err != nil {
				t.Fatalf("pending chats after account write gate: %v", err)
			}
			if len(pending) != 2 {
				t.Fatalf("account write gate did not release healthy sibling chats: %#v", pending)
			}
		})
	}
}

func TestExpiredGlobalWriteGateClearsOnlyTheWinningRow(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			pastLocal := time.Now().UTC().Add(-2 * time.Minute)
			pastGlobal := time.Now().UTC().Add(-time.Minute)
			if _, err := store.SetChatRateLimit(ctx, "winning-row-chat", pastLocal, "expired local"); err != nil {
				t.Fatalf("SetChatRateLimit local: %v", err)
			}
			if _, err := store.SetChatRateLimit(ctx, graphWriteAccountRateLimitKey, pastGlobal, "expired global"); err != nil {
				t.Fatalf("SetChatRateLimit global: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			if _, blocked, err := bridge.chatBlockedUntil(ctx, "winning-row-chat"); err != nil || blocked {
				t.Fatalf("chatBlockedUntil expired global = blocked=%v err=%v, want clear", blocked, err)
			}
			if _, found, err := store.ChatRateLimit(ctx, graphWriteAccountRateLimitKey); err != nil || found {
				t.Fatalf("global gate after winning-row clear = found=%v err=%v, want absent", found, err)
			}
			if local, found, err := store.ChatRateLimit(ctx, "winning-row-chat"); err != nil || !found || !local.BlockedUntil.Equal(pastLocal) {
				t.Fatalf("local gate after global clear = %#v found=%v err=%v, want preserved", local, found, err)
			}
			if _, blocked, err := bridge.chatBlockedUntil(ctx, "winning-row-chat"); err != nil || blocked {
				t.Fatalf("chatBlockedUntil expired local = blocked=%v err=%v, want clear", blocked, err)
			}
			if _, found, err := store.ChatRateLimit(ctx, "winning-row-chat"); err != nil || found {
				t.Fatalf("local gate after second clear = found=%v err=%v, want absent", found, err)
			}
		})
	}
}

func TestBridgeOutbox429GateIsFencedBySenderOwnerAcrossBackends(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			scope := teamstore.ScopeIdentity{ID: "scope-outbox-429-owner-" + name, AccountID: "account-outbox-429"}
			machineA := teamstore.MachineRecord{ID: "machine-outbox-429-a-" + name, ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary}
			ownerA, err := teamstore.CurrentOwner("outbox-429-a", "", "", now)
			if err != nil {
				t.Fatalf("CurrentOwner A: %v", err)
			}
			ownerA.ScopeID, ownerA.MachineID = scope.ID, machineA.ID
			first, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Hour, Now: now})
			if err != nil || first.Mode != teamstore.LeaseModeActive {
				t.Fatalf("claim owner A: decision=%#v err=%v", first, err)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machineA.ID, first.Lease.Generation); err != nil || !released {
				t.Fatalf("release owner A: released=%v err=%v", released, err)
			}
			machineB := teamstore.MachineRecord{ID: "machine-outbox-429-b-" + name, ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary}
			ownerB, err := teamstore.CurrentOwner("outbox-429-b", "", "", now.Add(time.Second))
			if err != nil {
				t.Fatalf("CurrentOwner B: %v", err)
			}
			ownerB.ScopeID, ownerB.MachineID = scope.ID, machineB.ID
			second, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Hour, Now: now.Add(time.Second)})
			if err != nil || second.Mode != teamstore.LeaseModeActive {
				t.Fatalf("claim owner B: decision=%#v err=%v", second, err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate test store to SQLite: %v", err)
				}
			}

			bridge := &Bridge{store: store, machine: machineB}
			post429 := &GraphStatusError{Method: http.MethodPost, StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute, Message: "outbox write throttle"}
			staleCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{MachineID: machineA.ID, LeaseGeneration: first.Lease.Generation})
			bridge.recordGraphRateLimit(staleCtx, "chat-outbox-429-owner", "outbox-write-429", post429)
			if _, found, err := store.ChatRateLimit(ctx, "chat-outbox-429-owner"); err != nil {
				t.Fatalf("read gate after stale sender callback: %v", err)
			} else if found {
				t.Fatal("stale sender callback installed an outbound gate for the replacement owner")
			}

			currentCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{MachineID: machineB.ID, LeaseGeneration: second.Lease.Generation})
			bridge.recordGraphRateLimit(currentCtx, "chat-outbox-429-owner", "outbox-write-429-current", post429)
			limit, found, err := store.ChatRateLimit(ctx, "chat-outbox-429-owner")
			if err != nil || !found || !limit.BlockedUntil.After(now) || limit.PoisonOutboxID != "outbox-write-429-current" {
				t.Fatalf("current sender outbound gate = %#v found=%v err=%v, want durable current-owner gate", limit, found, err)
			}
		})
	}
}
