package teams

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	dockerBoundedAcceptanceChatID       = "chat-1"
	dockerBoundedAcceptanceDisabledMark = "__docker_bounded_acceptance_unknown_post_disabled__"
	dockerBoundedAcceptanceUnknownMark  = "bounded acceptance unknown post"
)

// dockerBoundedAcceptanceClock is intentionally a test-only clock. It drives
// fake Graph request witnesses and synthetic message timestamps; the bridge
// and store still use their production wall clock, which is recorded as a gap
// below instead of being disguised as virtual time.
type dockerBoundedAcceptanceClock struct {
	mu  sync.Mutex
	now time.Time
}

func newDockerBoundedAcceptanceClock() *dockerBoundedAcceptanceClock {
	return &dockerBoundedAcceptanceClock{now: time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)}
}

func (c *dockerBoundedAcceptanceClock) Now() time.Time {
	if c == nil {
		return time.Time{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *dockerBoundedAcceptanceClock) Advance(delta time.Duration) time.Time {
	if c == nil {
		return time.Time{}
	}
	c.mu.Lock()
	c.now = c.now.Add(delta)
	now := c.now
	c.mu.Unlock()
	return now
}

type dockerBoundedAcceptanceHarness struct {
	clock    *dockerBoundedAcceptanceClock
	graph    *dockerRealDataGraphServer
	server   *httptest.Server
	auth     *fakeGraphAuth
	store    *teamstore.Store
	bridge   *Bridge
	executor *dockerRealDataExecutor
	message  ChatMessage
}

func newDockerBoundedAcceptanceHarness(t *testing.T) *dockerBoundedAcceptanceHarness {
	t.Helper()
	clock := newDockerBoundedAcceptanceClock()
	user := User{ID: "user-1", DisplayName: "Bounded Acceptance User", UserPrincipalName: "bounded@example.test"}
	message := dockerRealDataMessage(dockerBoundedAcceptanceChatID, 0, 0, clock.Now().Add(time.Minute))
	message.ID = "docker-bounded-inbound-001"
	message.From.User = &struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
	}{ID: "bounded-external-user", DisplayName: "Bounded Acceptance External"}
	message.Body.Content = "<p>bounded acceptance Work request</p>"
	replay := map[string][]ChatMessage{dockerBoundedAcceptanceChatID: {message}}
	graph := newDockerRealDataGraphServerAt("", user, replay, clock.Now)
	graph.controlChatID = "control-chat"
	graph.setUnknownPostMarker(dockerBoundedAcceptanceDisabledMark)
	server := httptest.NewServer(graph)
	t.Cleanup(server.Close)
	auth := &fakeGraphAuth{token: ""}
	client := newTestGraphClient(auth, server, nil)
	store := newBridgeTestStore(t)
	executor := &dockerRealDataExecutor{now: clock.Now}
	bridge := newBridgeTestBridge(client, store, executor)
	return &dockerBoundedAcceptanceHarness{
		clock:    clock,
		graph:    graph,
		server:   server,
		auth:     auth,
		store:    store,
		bridge:   bridge,
		executor: executor,
		message:  message,
	}
}

func (h *dockerBoundedAcceptanceHarness) seedPoll(t *testing.T) {
	t.Helper()
	if h == nil {
		t.Fatal("bounded acceptance harness is nil")
	}
	// A seeded cursor makes the synthetic row actionable. The acceptance
	// scenario never relies on a wall-clock poll interval or sleeps for a gate
	// to expire.
	if _, err := h.store.RecordChatPollSuccess(context.Background(), dockerBoundedAcceptanceChatID, h.messageModifiedAt().Add(-time.Second), true, false, 0); err != nil {
		t.Fatalf("seed bounded acceptance poll: %v", err)
	}
}

func (h *dockerBoundedAcceptanceHarness) messageModifiedAt() time.Time {
	if h == nil {
		return time.Time{}
	}
	stamp, err := time.Parse(time.RFC3339Nano, h.message.LastModifiedDateTime)
	if err != nil {
		return time.Time{}
	}
	return stamp
}

func assertDockerBoundedAcceptanceNoToken(t *testing.T, h *dockerBoundedAcceptanceHarness) {
	t.Helper()
	if h == nil || h.auth == nil {
		t.Fatal("bounded acceptance auth harness is nil")
	}
	if h.auth.token != "" {
		t.Fatalf("bounded acceptance auth token = %q, want empty", h.auth.token)
	}
	if h.graph == nil || h.graph.token != "" {
		t.Fatalf("bounded acceptance fake Graph token is not empty: %#v", h.graph)
	}
}

func assertDockerBoundedAcceptanceGraphTimes(t *testing.T, h *dockerBoundedAcceptanceHarness, want time.Time) {
	t.Helper()
	requests := h.graph.graphRequestsSnapshot()
	if len(requests) == 0 {
		t.Fatal("bounded acceptance fake Graph recorded no request")
	}
	for i, request := range requests {
		if !request.StartedAt.Equal(want) || !request.CompletedAt.Equal(want) {
			t.Fatalf("fake Graph request %d time = started %s completed %s, want %s", i, request.StartedAt.Format(time.RFC3339Nano), request.CompletedAt.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
		}
	}
}

func assertDockerBoundedAcceptanceNoUnknownRoutes(t *testing.T, h *dockerBoundedAcceptanceHarness) {
	t.Helper()
	h.graph.mu.Lock()
	unknown := append([]string(nil), h.graph.unknownPath...)
	h.graph.mu.Unlock()
	if len(unknown) != 0 {
		t.Fatalf("bounded acceptance fake Graph saw unknown routes: %v", unknown)
	}
}

func TestDockerBoundedAcceptanceScenario(t *testing.T) {
	// This is deliberately a normal package test as well as a Docker target:
	// the only server is httptest, no fixture is copied, no token is read, and
	// every branch has a finite request budget. The wrapper runs this exact test
	// in a network-none scratch container for the stronger isolation proof.
	t.Run("account-get-429-installs-durable-read-gate", func(t *testing.T) {
		h := newDockerBoundedAcceptanceHarness(t)
		assertDockerBoundedAcceptanceNoToken(t, h)
		h.seedPoll(t)
		h.graph.setPersistentGlobalList429WithScope(1, dockerRealData429ScopeAccount)

		handled, err := h.bridge.pollChat(context.Background(), dockerBoundedAcceptanceChatID, dockerRealDataDefaultTop, func(context.Context, ChatMessage, string) error {
			t.Fatal("account-scoped 429 reached the message handler")
			return nil
		})
		if handled {
			t.Fatal("account-scoped 429 was reported as handled")
		}
		var graphErr *GraphStatusError
		if !errors.As(err, &graphErr) || graphErr.StatusCode != 429 || graphErr.RateLimitScope != dockerRealData429ScopeAccount || graphErr.Method != "GET" {
			t.Fatalf("account-scoped poll error = %T %v, want GET account 429", err, err)
		}
		limit, found, err := h.store.ChatRateLimit(context.Background(), graphReadAccountRateLimitKey)
		if err != nil {
			t.Fatalf("read durable account gate: %v", err)
		}
		if !found || limit.ChatID != graphReadAccountRateLimitKey || limit.BlockedUntil.IsZero() || !strings.Contains(limit.Reason, dockerRealData429ScopeAccount) {
			t.Fatalf("durable account gate = %#v found=%t, want non-zero account gate", limit, found)
		}
		poll, found, err := h.store.ChatPoll(context.Background(), dockerBoundedAcceptanceChatID)
		if err != nil || !found || poll.FailureCount == 0 {
			t.Fatalf("chat poll after account 429 = %#v found=%t err=%v, want recorded failure", poll, found, err)
		}
		if got := h.executor.runs.Load(); got != 0 {
			t.Fatalf("executor runs after account 429 = %d, want 0", got)
		}
		attempts, throttled, accepted := h.graph.graphOperationCounts(dockerRealDataGraphOpMessageList)
		if attempts != 1 || throttled != 1 || accepted != 0 {
			t.Fatalf("account list Graph counts = attempts %d throttled %d accepted %d, want 1/1/0", attempts, throttled, accepted)
		}
		assertDockerBoundedAcceptanceGraphTimes(t, h, h.clock.Now())
		assertDockerBoundedAcceptanceNoUnknownRoutes(t, h)
	})

	t.Run("global-get-429-installs-durable-read-gate", func(t *testing.T) {
		h := newDockerBoundedAcceptanceHarness(t)
		assertDockerBoundedAcceptanceNoToken(t, h)
		h.seedPoll(t)
		h.graph.setPersistentGlobalList429WithScope(1, dockerRealData429ScopeGlobal)

		handled, err := h.bridge.pollChat(context.Background(), dockerBoundedAcceptanceChatID, dockerRealDataDefaultTop, func(context.Context, ChatMessage, string) error {
			t.Fatal("global-scoped 429 reached the message handler")
			return nil
		})
		if handled {
			t.Fatal("global-scoped 429 was reported as handled")
		}
		var graphErr *GraphStatusError
		if !errors.As(err, &graphErr) || graphErr.StatusCode != 429 || graphErr.RateLimitScope != dockerRealData429ScopeGlobal || graphErr.Method != "GET" {
			t.Fatalf("global-scoped poll error = %T %v, want GET global 429", err, err)
		}
		limit, found, err := h.store.ChatRateLimit(context.Background(), graphReadAccountRateLimitKey)
		if err != nil {
			t.Fatalf("read durable global gate: %v", err)
		}
		if !found || limit.ChatID != graphReadAccountRateLimitKey || limit.BlockedUntil.IsZero() || strings.TrimSpace(limit.Reason) == "" {
			t.Fatalf("durable global gate = %#v found=%t, want non-zero global gate", limit, found)
		}
		if got := h.executor.runs.Load(); got != 0 {
			t.Fatalf("executor runs after global 429 = %d, want 0", got)
		}
		attempts, throttled, accepted := h.graph.graphOperationCounts(dockerRealDataGraphOpMessageList)
		if attempts != 1 || throttled != 1 || accepted != 0 {
			t.Fatalf("global list Graph counts = attempts %d throttled %d accepted %d, want 1/1/0", attempts, throttled, accepted)
		}
		assertDockerBoundedAcceptanceGraphTimes(t, h, h.clock.Now())
		assertDockerBoundedAcceptanceNoUnknownRoutes(t, h)
	})

	t.Run("intermittent-get-429-then-success-uses-virtual-time", func(t *testing.T) {
		h := newDockerBoundedAcceptanceHarness(t)
		assertDockerBoundedAcceptanceNoToken(t, h)
		h.graph.setPersistentGlobalList429WithScope(1, dockerRealData429ScopeAccount)
		ctx := context.Background()
		firstAt := h.clock.Now()
		_, firstErr := h.bridge.graph.ListMessagesWindowWithoutRateLimitRetry(ctx, dockerBoundedAcceptanceChatID, dockerRealDataDefaultTop, time.Time{})
		var graphErr *GraphStatusError
		if !errors.As(firstErr, &graphErr) || graphErr.StatusCode != 429 {
			t.Fatalf("first intermittent read error = %T %v, want 429", firstErr, firstErr)
		}
		secondAt := h.clock.Advance(250 * time.Millisecond)
		window, secondErr := h.bridge.graph.ListMessagesWindowWithoutRateLimitRetry(ctx, dockerBoundedAcceptanceChatID, dockerRealDataDefaultTop, time.Time{})
		if secondErr != nil {
			t.Fatalf("second intermittent read: %v", secondErr)
		}
		if len(window.Messages) != 1 || window.Messages[0].ID != h.message.ID {
			t.Fatalf("second intermittent read messages = %#v, want synthetic message", window.Messages)
		}
		requests := h.graph.graphRequestsSnapshot()
		if len(requests) != 2 || requests[0].StatusCode != 429 || requests[1].StatusCode != 200 {
			t.Fatalf("intermittent Graph requests = %#v, want GET 429 then GET 200", requests)
		}
		if !requests[0].StartedAt.Equal(firstAt) || !requests[0].CompletedAt.Equal(firstAt) || !requests[1].StartedAt.Equal(secondAt) || !requests[1].CompletedAt.Equal(secondAt) {
			t.Fatalf("intermittent Graph virtual times = %#v, want %s then %s", requests, firstAt.Format(time.RFC3339Nano), secondAt.Format(time.RFC3339Nano))
		}
		attempts, throttled, _ := h.graph.graphOperationCounts(dockerRealDataGraphOpMessageList)
		if attempts != 2 || throttled != 1 {
			t.Fatalf("intermittent list Graph counts = attempts %d throttled %d, want 2/1", attempts, throttled)
		}
		assertDockerBoundedAcceptanceNoUnknownRoutes(t, h)
	})

	t.Run("unknown-post-keeps-durable-ambiguous-outbox", func(t *testing.T) {
		h := newDockerBoundedAcceptanceHarness(t)
		assertDockerBoundedAcceptanceNoToken(t, h)
		h.graph.setUnknownPostMarker(dockerBoundedAcceptanceUnknownMark)

		err := h.bridge.sendToChat(context.Background(), dockerBoundedAcceptanceChatID, dockerBoundedAcceptanceUnknownMark)
		if err == nil {
			t.Fatal("unknown POST unexpectedly returned success")
		}
		state, loadErr := h.store.Load(context.Background())
		if loadErr != nil {
			t.Fatalf("load unknown-POST state: %v", loadErr)
		}
		var ambiguous []teamstore.OutboxMessage
		for _, outbox := range state.OutboxMessages {
			if outbox.TeamsChatID == dockerBoundedAcceptanceChatID && strings.HasPrefix(strings.ToLower(outbox.Kind), "helper") {
				ambiguous = append(ambiguous, outbox)
			}
		}
		if len(ambiguous) != 1 {
			t.Fatalf("unknown-POST helper outboxes = %#v, want one", ambiguous)
		}
		outbox := ambiguous[0]
		if outbox.Status != teamstore.OutboxStatusSending || outbox.TeamsMessageID != "" || !teamstore.OutboxSendIsAmbiguous(outbox) {
			t.Fatalf("unknown-POST durable row = %#v, want Sending/ambiguous/no message ID", outbox)
		}
		attempts, throttled, accepted := h.graph.graphOperationCounts(dockerRealDataGraphOpMessagePost)
		if attempts != 1 || throttled != 0 || accepted != 0 || h.graph.messagePostAccepts.Load() != 1 || h.graph.unknownPosts.Load() != 1 {
			t.Fatalf("unknown-POST Graph evidence = attempts %d throttled %d accepted %d remoteAccepts %d unknown %d, want 1/0/0/1/1", attempts, throttled, accepted, h.graph.messagePostAccepts.Load(), h.graph.unknownPosts.Load())
		}
		// The ordinary targeted flush excludes active ambiguous rows and must not
		// manufacture a second POST while the outcome is unresolved.
		_ = h.bridge.flushPendingOutboxForChat(context.Background(), dockerBoundedAcceptanceChatID)
		attemptsAfter, _, _ := h.graph.graphOperationCounts(dockerRealDataGraphOpMessagePost)
		if attemptsAfter != 1 || h.graph.unknownPostRepeats.Load() != 0 {
			t.Fatalf("unknown-POST retry evidence = attempts %d repeats %d, want 1/0", attemptsAfter, h.graph.unknownPostRepeats.Load())
		}
		assertDockerBoundedAcceptanceGraphTimes(t, h, h.clock.Now())
		assertDockerBoundedAcceptanceNoUnknownRoutes(t, h)
	})

	t.Run("positive-work-completion-is-durable", func(t *testing.T) {
		h := newDockerBoundedAcceptanceHarness(t)
		assertDockerBoundedAcceptanceNoToken(t, h)
		message := h.message
		message.ID = "docker-bounded-work-001"
		message.Body.Content = "<p>complete this bounded Work request</p>"
		if err := h.bridge.handleSessionMessage(context.Background(), dockerBoundedAcceptanceChatID, message, "complete this bounded Work request"); err != nil {
			t.Fatalf("positive Work handling: %v", err)
		}
		state, err := h.store.Load(context.Background())
		if err != nil {
			t.Fatalf("load positive completion state: %v", err)
		}
		var completed teamstore.Turn
		foundTurn := false
		for _, turn := range state.Turns {
			if turn.SessionID == "s001" && turn.Status == teamstore.TurnStatusCompleted {
				completed = turn
				foundTurn = true
				break
			}
		}
		if !foundTurn || completed.InboundEventID == "" || completed.CodexThreadID == "" || completed.CodexTurnID == "" || completed.CompletedAt.IsZero() {
			t.Fatalf("completed durable turn = %#v found=%t, want terminal Work proof", completed, foundTurn)
		}
		inbound, foundInbound := state.InboundEvents[completed.InboundEventID]
		if !foundInbound || inbound.TeamsMessageID != message.ID || inbound.TurnID != completed.ID || inbound.Status != teamstore.InboundStatusQueued {
			t.Fatalf("completed inbound = %#v found=%t, want queued receipt linked to turn", inbound, foundInbound)
		}
		var final teamstore.OutboxMessage
		foundFinal := false
		for _, outbox := range state.OutboxMessages {
			if outbox.TurnID == completed.ID && isFinalOutboxKind(outbox.Kind) {
				final = outbox
				foundFinal = true
				break
			}
		}
		if !foundFinal || final.Status != teamstore.OutboxStatusSent || final.TeamsMessageID == "" {
			t.Fatalf("completed final outbox = %#v found=%t, want Sent with Graph identity", final, foundFinal)
		}
		if h.executor.runs.Load() != 1 {
			t.Fatalf("positive completion executor runs = %d, want 1", h.executor.runs.Load())
		}
		attempts, throttled, accepted := h.graph.graphOperationCounts(dockerRealDataGraphOpMessagePost)
		if attempts != 2 || throttled != 0 || accepted != 2 || h.graph.messagePostAccepts.Load() != 2 || h.graph.unknownPosts.Load() != 0 {
			t.Fatalf("positive completion Graph evidence = attempts %d throttled %d accepted %d remoteAccepts %d unknown %d, want 2/0/2/2/0", attempts, throttled, accepted, h.graph.messagePostAccepts.Load(), h.graph.unknownPosts.Load())
		}
		assertDockerBoundedAcceptanceGraphTimes(t, h, h.clock.Now())
		assertDockerBoundedAcceptanceNoUnknownRoutes(t, h)
	})
}
