package teams

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestPollOnceHydratesOnlyFinalSelectedDurableWorkQuantum(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newBridgeTestStore(t)

	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{TeamsChatID: "control-chat", UpdatedAt: now}
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("scalar-quantum-session-%02d", i)
			chatID := fmt.Sprintf("scalar-quantum-chat-%02d", i)
			state.Sessions[id] = teamstore.SessionContext{
				ID: id, Status: teamstore.SessionStatusActive, TeamsChatID: chatID,
				CodexThreadID: "thread-" + id, UpdatedAt: now,
			}
			state.ChatPolls[chatID] = teamstore.ChatPollState{
				ChatID: chatID, Seeded: true, PollState: "warm",
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
			}
		}
		state.ChatPolls["control-chat"] = teamstore.ChatPollState{
			ChatID: "control-chat", Seeded: true, PollState: "warm",
			NextPollAt: now.Add(time.Hour), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed scalar quantum fixture: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate scalar quantum fixture: %v", err)
	}

	var graphGets atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			return nil, fmt.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		graphGets.Add(1)
		response := httptest.NewRecorder()
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.WriteString(`{"value":[]}`)
		return response.Result(), nil
	})}
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     client,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.maxWorkChatPollsPerCycle = 1
	for i := 1; i < 10; i++ {
		id := fmt.Sprintf("scalar-quantum-session-%02d", i)
		chatID := fmt.Sprintf("scalar-quantum-chat-%02d", i)
		bridge.reg.Sessions = append(bridge.reg.Sessions, Session{
			ID: id, ChatID: chatID, Status: "active", UpdatedAt: now,
		})
	}
	// The helper's default registry entry is a real tenth candidate; update its
	// chat/session identity to match the durable fixture instead of relying on a
	// missing-registry fallback.
	bridge.reg.Sessions[0] = Session{
		ID: "scalar-quantum-session-00", ChatID: "scalar-quantum-chat-00", Status: "active", UpdatedAt: now,
	}

	lengths := make(map[string]int)
	bridge.pollDecisionTraceHook = func(stage string, decisions []inboundPollDecision) {
		lengths[stage] = len(decisions)
	}
	if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil {
		t.Fatalf("pollOnce scalar quantum error: %v", err)
	}
	if got := lengths["candidate"]; got != 10 {
		t.Fatalf("candidate decision count=%d, want 10 durable candidates", got)
	}
	if got := lengths["selected"]; got != 1 {
		t.Fatalf("selected decision count=%d, want configured quantum 1", got)
	}
	if got := lengths["hydrated"]; got != 1 {
		t.Fatalf("hydrated decision count=%d, want only final selected quantum", got)
	}
	if got := graphGets.Load(); got != 1 {
		t.Fatalf("Graph GET count=%d, want one selected work chat", got)
	}
}

func TestPollOnceRefillsWhenSelectedHydrationFindsReboundSession(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newBridgeTestStore(t)
	const (
		firstSessionID  = "refill-session-00"
		firstChatID     = "refill-chat-00"
		secondSessionID = "refill-session-01"
		secondChatID    = "refill-chat-01"
	)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{TeamsChatID: "control-chat", UpdatedAt: now}
		state.ChatPolls["control-chat"] = teamstore.ChatPollState{
			ChatID: "control-chat", Seeded: true, PollState: "warm",
			NextPollAt: now.Add(time.Hour), UpdatedAt: now,
		}
		for _, item := range []struct {
			sessionID string
			chatID    string
		}{
			{sessionID: firstSessionID, chatID: firstChatID},
			{sessionID: secondSessionID, chatID: secondChatID},
		} {
			state.Sessions[item.sessionID] = teamstore.SessionContext{
				ID: item.sessionID, Status: teamstore.SessionStatusActive,
				TeamsChatID: item.chatID, CodexThreadID: "thread-" + item.sessionID,
				UpdatedAt: now,
			}
			state.ChatPolls[item.chatID] = teamstore.ChatPollState{
				ChatID: item.chatID, Seeded: true, PollState: "warm",
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed hydration-refill fixture: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate hydration-refill fixture: %v", err)
	}

	var graphGets atomic.Int32
	var graphChat atomic.Value
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			return nil, fmt.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		graphGets.Add(1)
		graphChat.Store(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages"))
		response := httptest.NewRecorder()
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.WriteString(`{"value":[]}`)
		return response.Result(), nil
	})}
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     client,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.maxWorkChatPollsPerCycle = 1
	bridge.reg.Sessions = []Session{
		{ID: firstSessionID, ChatID: firstChatID, Status: "active", UpdatedAt: now},
		{ID: secondSessionID, ChatID: secondChatID, Status: "active", UpdatedAt: now},
	}
	var mutated atomic.Bool
	bridge.pollDecisionTraceHook = func(stage string, decisions []inboundPollDecision) {
		if stage != "selected" || !mutated.CompareAndSwap(false, true) {
			return
		}
		if len(decisions) != 1 || decisions[0].ChatID != firstChatID {
			t.Fatalf("selected decisions=%#v, want first chat only before refill", decisions)
		}
		if err := store.Update(ctx, func(state *teamstore.State) error {
			session := state.Sessions[firstSessionID]
			session.Status = teamstore.SessionStatusClosed
			session.UpdatedAt = now.Add(time.Second)
			state.Sessions[firstSessionID] = session
			return nil
		}); err != nil {
			t.Fatalf("rebind selected session before hydration: %v", err)
		}
	}
	if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil {
		t.Fatalf("pollOnce hydration refill error: %v", err)
	}
	if !mutated.Load() {
		t.Fatal("selected hydration hook did not run")
	}
	if got := graphGets.Load(); got != 1 {
		t.Fatalf("Graph GET count=%d, want one refilled work chat", got)
	}
	if got, _ := graphChat.Load().(string); got != secondChatID {
		t.Fatalf("Graph chat=%q, want refilled chat %q", got, secondChatID)
	}
}

func TestPollOnceSkipsGatedCandidatePrefixBeforeApplyingWorkQuantum(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newBridgeTestStore(t)
	const (
		controlChatID = "control-chat"
		tailSessionID = "gated-tail-session"
		tailChatID    = "gated-tail-chat"
	)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{TeamsChatID: controlChatID, UpdatedAt: now}
		state.ChatPolls[controlChatID] = teamstore.ChatPollState{
			ChatID: controlChatID, Seeded: true, PollState: "warm",
			NextPollAt: now.Add(time.Hour), LastActivityAt: now, UpdatedAt: now,
		}
		for i := 0; i < 64; i++ {
			sessionID := fmt.Sprintf("gated-prefix-session-%02d", i)
			chatID := fmt.Sprintf("gated-prefix-chat-%02d", i)
			message := bridgePollMessage("gated-message-"+chatID, now.Format(time.RFC3339Nano), "gated")
			message.ChatID = chatID
			page, err := pendingPageFromWindow(chatID, "/chats/"+chatID+"/messages?$top=1", pollFrontierHead, 0, MessageWindow{Messages: []ChatMessage{message}}, false)
			if err != nil {
				return err
			}
			page.Dispositions = []string{"invalid_record"}
			page.RefetchFailures = []int{1}
			page.ReceiptID = pendingPageReceiptID(page)
			state.Sessions[sessionID] = teamstore.SessionContext{
				ID: sessionID, Status: teamstore.SessionStatusActive, TeamsChatID: chatID,
				CodexThreadID: "thread-" + sessionID, UpdatedAt: now,
			}
			state.ChatPolls[chatID] = teamstore.ChatPollState{
				ChatID: chatID, Seeded: true, PollState: "warm",
				NextPollAt: now.Add(-time.Hour), LastActivityAt: now, UpdatedAt: now,
				PendingPage: page,
			}
		}
		message := bridgePollMessage("gated-tail-message", now.Format(time.RFC3339Nano), "local tail")
		message.ChatID = tailChatID
		page, err := pendingPageFromWindow(tailChatID, "/chats/"+tailChatID+"/messages?$top=1", pollFrontierHead, 0, MessageWindow{}, false)
		if err != nil {
			return err
		}
		page.Records = nil
		page.RecordIDs = nil
		page.RecordHashes = nil
		page.Dispositions = nil
		page.RefetchFailures = nil
		page.ReceiptID = pendingPageReceiptID(page)
		state.Sessions[tailSessionID] = teamstore.SessionContext{
			ID: tailSessionID, Status: teamstore.SessionStatusActive, TeamsChatID: tailChatID,
			CodexThreadID: "thread-" + tailSessionID, UpdatedAt: now.Add(time.Second),
		}
		state.ChatPolls[tailChatID] = teamstore.ChatPollState{
			ChatID: tailChatID, Seeded: true, PollState: "warm",
			NextPollAt: now.Add(-time.Hour), LastActivityAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
			PendingPage: page,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed gated candidate prefix: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate gated candidate prefix: %v", err)
	}
	if _, err := store.SetChatRateLimit(ctx, graphReadAccountRateLimitKey, time.Now().Add(time.Hour), "account read gate"); err != nil {
		t.Fatalf("seed account read gate: %v", err)
	}
	if gate, ok, err := store.ChatRateLimit(ctx, graphReadAccountRateLimitKey); err != nil || !ok || !gate.BlockedUntil.After(time.Now()) {
		t.Fatalf("account read gate after seed = %#v/%v/%v", gate, ok, err)
	}

	var graphGets atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		graphGets.Add(1)
		return nil, fmt.Errorf("unexpected Graph read while account gate is active: %s", r.URL.String())
	})}
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     client,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.maxWorkChatPollsPerCycle = 1
	accountUntil, accountBlocked := bridge.graphReadAccountBlockedUntil(ctx)
	if !accountBlocked || !accountUntil.After(time.Now()) {
		t.Fatalf("bridge account gate snapshot = %s/%v", accountUntil, accountBlocked)
	}
	prefixPoll, prefixOK, prefixErr := store.ChatPoll(ctx, "gated-prefix-chat-00")
	if prefixErr != nil || !prefixOK {
		t.Fatalf("load gated prefix poll = %#v/%v/%v", prefixPoll, prefixOK, prefixErr)
	}
	if !pendingPageRequiresGraphReplay(prefixPoll.PendingPage) {
		t.Fatalf("gated prefix pending page was classified as local: %#v", prefixPoll.PendingPage)
	}
	if _, blocked := bridge.pollGraphReadBlockedUntilSnapshot(prefixPoll.ChatID, inboundPollRoleWork, prefixPoll, accountUntil, time.Now()); !blocked {
		t.Fatalf("gated prefix snapshot ignored account gate: poll=%#v account=%s", prefixPoll, accountUntil)
	}
	bridge.reg.Sessions = []Session{{ID: tailSessionID, ChatID: tailChatID, Status: "active", UpdatedAt: now.Add(time.Second)}}
	for i := 0; i < 64; i++ {
		sessionID := fmt.Sprintf("gated-prefix-session-%02d", i)
		chatID := fmt.Sprintf("gated-prefix-chat-%02d", i)
		bridge.reg.Sessions = append(bridge.reg.Sessions, Session{ID: sessionID, ChatID: chatID, Status: "active", UpdatedAt: now})
	}
	var candidateChats []string
	bridge.pollDecisionTraceHook = func(stage string, decisions []inboundPollDecision) {
		if stage != "candidate" {
			return
		}
		candidateChats = make([]string, 0, len(decisions))
		for _, decision := range decisions {
			candidateChats = append(candidateChats, decision.ChatID)
		}
	}
	if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil {
		t.Fatalf("pollOnce gated candidate prefix: %v", err)
	}
	if graphGets.Load() != 0 {
		t.Fatalf("Graph reads while account gate was active = %d, want zero", graphGets.Load())
	}
	if len(candidateChats) != 1 || candidateChats[0] != tailChatID {
		t.Fatalf("post-gate candidate decisions = %#v, want only healthy tail %q", candidateChats, tailChatID)
	}
}
