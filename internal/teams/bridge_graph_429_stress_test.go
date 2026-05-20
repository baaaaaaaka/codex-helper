package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type teamsGraph429StressScale struct {
	Chats    int
	Messages int
	Rounds   int
}

func loadTeamsGraph429StressScale() teamsGraph429StressScale {
	scale := teamsGraph429StressScale{Chats: 12, Messages: 3, Rounds: 3}
	if os.Getenv("CODEX_HELPER_TEAMS_GRAPH_429_STRESS") != "" {
		scale = teamsGraph429StressScale{Chats: 32, Messages: 6, Rounds: 8}
	}
	if value := positiveEnvInt("CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS"); value > 0 {
		scale.Chats = value
	}
	if value := positiveEnvInt("CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES"); value > 0 {
		scale.Messages = value
	}
	if value := positiveEnvInt("CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS"); value > 0 {
		scale.Rounds = value
	}
	return scale
}

func positiveEnvInt(name string) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func TestTeamsGraph429StressOutboxMaintainsAvailabilityAndSuppressesLoopsCI(t *testing.T) {
	scale := loadTeamsGraph429StressScale()
	ctx := context.Background()
	store := newBridgeTestStore(t)
	blockedChats := teamsGraph429BlockedChats(scale.Chats)
	var (
		mu          sync.Mutex
		requests    = map[string]int{}
		sentPlain   = map[string][]string{}
		blockedMode = true
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
		mu.Lock()
		requests[chatID]++
		shouldBlock := blockedMode && blockedChats[chatID]
		mu.Unlock()
		if shouldBlock {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"stress rate limit"}}`, http.StatusTooManyRequests)
			return
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode Graph request: %v", err)
		}
		mu.Lock()
		sentPlain[chatID] = append(sentPlain[chatID], PlainTextFromTeamsHTML(body.Body.Content))
		id := len(sentPlain[chatID])
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%s-%d","messageType":"message"}`, chatID, id)
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep: func(context.Context, time.Duration) error {
			t.Fatal("Teams Graph 429 stress path must record Retry-After before any hidden Graph sleep")
			return nil
		},
		jitter: func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	for chatNum := 1; chatNum <= scale.Chats; chatNum++ {
		chatID := teamsGraph429StressChatID(chatNum)
		for msgNum := 1; msgNum <= scale.Messages; msgNum++ {
			outbox := teamstore.OutboxMessage{
				ID:          fmt.Sprintf("outbox:stress:%s:%02d", chatID, msgNum),
				TeamsChatID: chatID,
				Kind:        "helper",
				Body:        fmt.Sprintf("initial %s message %02d", chatID, msgNum),
			}
			if _, _, err := store.QueueOutbox(ctx, outbox); err != nil {
				t.Fatalf("QueueOutbox %s: %v", outbox.ID, err)
			}
		}
	}

	err := bridge.flushPendingOutbox(ctx, "", "")
	if err == nil || !isGraphRateLimitError(err) {
		t.Fatalf("initial flush error = %v, want first Graph 429 while continuing other chats", err)
	}
	requireTeamsGraph429OutboxStressState(t, store, blockedChats, requests, sentPlain, scale, 0)
	blockedRequestsAfterInitial := snapshotIntMap(requests)

	for round := 1; round <= scale.Rounds; round++ {
		for chatNum := 1; chatNum <= scale.Chats; chatNum++ {
			chatID := teamsGraph429StressChatID(chatNum)
			if blockedChats[chatID] {
				continue
			}
			outbox := teamstore.OutboxMessage{
				ID:          fmt.Sprintf("outbox:stress:%s:extra:%02d", chatID, round),
				TeamsChatID: chatID,
				Kind:        "helper",
				Body:        fmt.Sprintf("round %02d still available for %s", round, chatID),
			}
			if _, _, err := store.QueueOutbox(ctx, outbox); err != nil {
				t.Fatalf("QueueOutbox %s: %v", outbox.ID, err)
			}
		}
		if err := bridge.flushPendingOutbox(ctx, "", ""); err != nil && !isOutboxDeliveryDeferred(err) {
			t.Fatalf("round %d flush error = %v, want only deferred rate-limit errors", round, err)
		}
		assertBlockedRequestCountsUnchanged(t, blockedChats, blockedRequestsAfterInitial, requests)
		requireTeamsGraph429OutboxStressState(t, store, blockedChats, requests, sentPlain, scale, round)
	}

	mu.Lock()
	blockedMode = false
	mu.Unlock()
	for chatID := range blockedChats {
		if err := store.ClearChatRateLimit(ctx, chatID); err != nil {
			t.Fatalf("ClearChatRateLimit %s: %v", chatID, err)
		}
	}
	if err := bridge.flushPendingOutbox(ctx, "", ""); err != nil {
		t.Fatalf("flush after clearing rate limits: %v", err)
	}

	requireTeamsGraph429OutboxFinalState(t, store, blockedChats, requests, sentPlain, scale)
}

func TestTeamsGraph429StressPollMaintainsAvailabilityAndSuppressesLoopsCI(t *testing.T) {
	scale := loadTeamsGraph429StressScale()
	ctx := context.Background()
	now := time.Now()
	store := newBridgeTestStore(t)
	blockedChats := teamsGraph429BlockedChats(scale.Chats)
	var (
		mu             sync.Mutex
		requests       = map[string]int{}
		messageVersion = map[string]int{}
		blockedMode    = true
	)
	readServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph read request: %s %s", r.Method, r.URL.String())
		}
		chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
		mu.Lock()
		requests[chatID]++
		shouldBlock := blockedMode && blockedChats[chatID]
		version := messageVersion[chatID]
		mu.Unlock()
		if shouldBlock {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"stress poll limit"}}`, http.StatusTooManyRequests)
			return
		}
		msg := bridgePollMessage(
			fmt.Sprintf("%s-poll-%02d", chatID, version),
			now.Add(time.Duration(version)*time.Second).UTC().Format(time.RFC3339),
			fmt.Sprintf("poll %02d prompt for %s", version, chatID),
		)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{msg}}); err != nil {
			t.Fatalf("encode poll response: %v", err)
		}
	}))
	t.Cleanup(readServer.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     readServer.Client(),
		baseURL:    readServer.URL,
		maxRetries: 0,
		sleep: func(context.Context, time.Duration) error {
			t.Fatal("Teams poll 429 stress path must record Retry-After before any hidden Graph sleep")
			return nil
		},
		jitter: func(d time.Duration) time.Duration { return d },
	}
	writeGraph, sent := newBridgeTestGraph(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "stress poll final",
		CodexThreadID: "stress-thread",
		CodexTurnID:   "stress-turn",
	}}
	bridge := newBridgeTestBridge(writeGraph, store, executor)
	bridge.readGraph = readGraph
	bridge.reg.Sessions = nil
	bridge.maxWorkChatPollsPerCycle = scale.Chats
	seedTeamsGraph429ControlPoll(t, store, now)
	for chatNum := 1; chatNum <= scale.Chats; chatNum++ {
		chatID := teamsGraph429StressChatID(chatNum)
		bridge.reg.Sessions = append(bridge.reg.Sessions, Session{
			ID:        fmt.Sprintf("stress-session-%02d", chatNum),
			ChatID:    chatID,
			ChatURL:   "https://teams.example/" + chatID,
			Topic:     "stress " + chatID,
			Status:    "active",
			CreatedAt: now,
			UpdatedAt: now,
		})
		if err := bridge.ensureDurableSession(ctx, &bridge.reg.Sessions[len(bridge.reg.Sessions)-1]); err != nil {
			t.Fatalf("ensure durable session %s: %v", chatID, err)
		}
		seedTeamsGraph429WorkPoll(t, store, chatID, now)
		messageVersion[chatID] = 1
	}

	err := bridge.pollOnce(ctx, 20)
	if err == nil || !isGraphRateLimitError(err) {
		t.Fatalf("initial pollOnce error = %v, want first Graph 429 while polling other chats", err)
	}
	openChats := scale.Chats - len(blockedChats)
	if got := len(executor.prompts); got != openChats {
		t.Fatalf("prompts after initial poll = %d, want %d open chats", got, openChats)
	}
	if got := len(*sent); got < openChats*2 {
		t.Fatalf("sent Teams messages after initial poll = %d, want at least ack+final for %d chats", got, openChats)
	}
	for chatID := range blockedChats {
		if requests[chatID] != 1 {
			t.Fatalf("blocked poll requests for %s = %d, want exactly one initial 429; all=%#v", chatID, requests[chatID], requests)
		}
	}
	blockedRequestsAfterInitial := snapshotIntMap(requests)

	for round := 1; round <= scale.Rounds; round++ {
		for chatNum := 1; chatNum <= scale.Chats; chatNum++ {
			chatID := teamsGraph429StressChatID(chatNum)
			if blockedChats[chatID] {
				continue
			}
			messageVersion[chatID] = round + 1
			scheduleTeamsGraph429PollDue(t, store, chatID, now.Add(time.Duration(round)*time.Second))
		}
		if err := bridge.pollOnce(ctx, 20); err != nil {
			t.Fatalf("round %d pollOnce while blocked chats are parked: %v", round, err)
		}
		assertBlockedRequestCountsUnchanged(t, blockedChats, blockedRequestsAfterInitial, requests)
		wantPrompts := openChats * (round + 1)
		if got := len(executor.prompts); got != wantPrompts {
			t.Fatalf("round %d prompts = %d, want %d open-chat prompts", round, got, wantPrompts)
		}
	}

	mu.Lock()
	blockedMode = false
	mu.Unlock()
	for chatNum := 1; chatNum <= scale.Chats; chatNum++ {
		chatID := teamsGraph429StressChatID(chatNum)
		if blockedChats[chatID] {
			continue
		}
		scheduleTeamsGraph429PollLater(t, store, chatID, now.Add(time.Duration(scale.Rounds+1)*time.Second))
	}
	for chatID := range blockedChats {
		messageVersion[chatID] = scale.Rounds + 2
		scheduleTeamsGraph429PollUnblocked(t, store, chatID, now.Add(time.Duration(scale.Rounds+2)*time.Second))
	}
	if err := bridge.pollOnce(ctx, 20); err != nil {
		t.Fatalf("pollOnce after clearing read rate limits: %v", err)
	}
	wantPrompts := openChats*(scale.Rounds+1) + len(blockedChats)
	if got := len(executor.prompts); got != wantPrompts {
		t.Fatalf("prompts after unblocking = %d, want %d", got, wantPrompts)
	}
	for chatID := range blockedChats {
		if requests[chatID] != 2 {
			t.Fatalf("blocked chat %s read requests after unblock = %d, want one 429 plus one successful retry", chatID, requests[chatID])
		}
		poll, ok, err := store.ChatPoll(ctx, chatID)
		if err != nil || !ok {
			t.Fatalf("ChatPoll %s ok=%v err=%v", chatID, ok, err)
		}
		if poll.PollState == inboundPollStateBlocked || !poll.BlockedUntil.IsZero() || poll.LastError != "" {
			t.Fatalf("blocked chat %s poll state after successful retry = %#v, want unblocked clean poll", chatID, poll)
		}
	}
}

func teamsGraph429StressChatID(n int) string {
	return fmt.Sprintf("stress-chat-%02d", n)
}

func teamsGraph429BlockedChats(chatCount int) map[string]bool {
	blocked := map[string]bool{}
	for chatNum := 1; chatNum <= chatCount; chatNum++ {
		if chatNum%4 == 0 {
			blocked[teamsGraph429StressChatID(chatNum)] = true
		}
	}
	if len(blocked) == 0 && chatCount > 0 {
		blocked[teamsGraph429StressChatID(chatCount)] = true
	}
	return blocked
}

func snapshotIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func assertBlockedRequestCountsUnchanged(t *testing.T, blockedChats map[string]bool, before map[string]int, after map[string]int) {
	t.Helper()
	for chatID := range blockedChats {
		if after[chatID] != before[chatID] {
			t.Fatalf("blocked chat %s Graph requests changed from %d to %d during Retry-After block; all=%#v", chatID, before[chatID], after[chatID], after)
		}
	}
}

func requireTeamsGraph429OutboxStressState(t *testing.T, store *teamstore.Store, blockedChats map[string]bool, requests map[string]int, sentPlain map[string][]string, scale teamsGraph429StressScale, extraRounds int) {
	t.Helper()
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load outbox stress state: %v", err)
	}
	for chatID, limit := range state.ChatRateLimits {
		if blockedChats[chatID] {
			if !limit.BlockedUntil.After(time.Now()) || strings.TrimSpace(limit.PoisonOutboxID) == "" {
				t.Fatalf("blocked chat %s rate-limit state = %#v, want future block with poison outbox", chatID, limit)
			}
			continue
		}
		t.Fatalf("unexpected rate-limit for unblocked chat %s: %#v", chatID, limit)
	}
	for chatNum := 1; chatNum <= scale.Chats; chatNum++ {
		chatID := teamsGraph429StressChatID(chatNum)
		if blockedChats[chatID] {
			if requests[chatID] != 1 {
				t.Fatalf("blocked chat %s requests = %d, want one Graph 429 before backoff", chatID, requests[chatID])
			}
			if len(sentPlain[chatID]) != 0 {
				t.Fatalf("blocked chat %s sent messages during Retry-After: %#v", chatID, sentPlain[chatID])
			}
			continue
		}
		wantSent := scale.Messages + extraRounds
		if len(sentPlain[chatID]) != wantSent {
			t.Fatalf("open chat %s sent messages = %d, want %d; sent=%#v", chatID, len(sentPlain[chatID]), wantSent, sentPlain[chatID])
		}
	}
}

func requireTeamsGraph429OutboxFinalState(t *testing.T, store *teamstore.Store, blockedChats map[string]bool, requests map[string]int, sentPlain map[string][]string, scale teamsGraph429StressScale) {
	t.Helper()
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load final outbox stress state: %v", err)
	}
	if len(state.ChatRateLimits) != 0 {
		t.Fatalf("chat rate limits after final replay = %#v, want none", state.ChatRateLimits)
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.Status != teamstore.OutboxStatusSent || strings.TrimSpace(outbox.TeamsMessageID) == "" {
			t.Fatalf("outbox %s final state = %#v, want sent with TeamsMessageID", outbox.ID, outbox)
		}
	}
	for chatNum := 1; chatNum <= scale.Chats; chatNum++ {
		chatID := teamsGraph429StressChatID(chatNum)
		wantSent := scale.Messages
		wantRequests := scale.Messages
		if blockedChats[chatID] {
			wantRequests++ // one initial 429, then one successful retry per initial message after unblock.
		} else {
			wantSent += scale.Rounds
			wantRequests += scale.Rounds
		}
		if len(sentPlain[chatID]) != wantSent {
			t.Fatalf("%s final sent count = %d, want %d; sent=%#v", chatID, len(sentPlain[chatID]), wantSent, sentPlain[chatID])
		}
		if requests[chatID] != wantRequests {
			t.Fatalf("%s final Graph request count = %d, want %d", chatID, requests[chatID], wantRequests)
		}
	}
}

func seedTeamsGraph429ControlPoll(t *testing.T, store *teamstore.Store, now time.Time) {
	t.Helper()
	if _, err := store.RecordChatPollSuccess(context.Background(), "control-chat", now.Add(-time.Minute), true, false, 1); err != nil {
		t.Fatalf("seed control poll: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         "control-chat",
		PollState:      inboundPollStateWarm,
		NextPollAt:     now.Add(time.Hour),
		LastActivityAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("schedule control poll: %v", err)
	}
}

func seedTeamsGraph429WorkPoll(t *testing.T, store *teamstore.Store, chatID string, now time.Time) {
	t.Helper()
	if _, err := store.RecordChatPollSuccess(context.Background(), chatID, now.Add(-time.Minute), true, false, 1); err != nil {
		t.Fatalf("seed work poll %s: %v", chatID, err)
	}
	scheduleTeamsGraph429PollDue(t, store, chatID, now.Add(-time.Minute))
}

func scheduleTeamsGraph429PollDue(t *testing.T, store *teamstore.Store, chatID string, activity time.Time) {
	t.Helper()
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         chatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     time.Now().Add(-time.Second),
		LastActivityAt: activity,
	}); err != nil {
		t.Fatalf("schedule poll %s due: %v", chatID, err)
	}
}

func scheduleTeamsGraph429PollLater(t *testing.T, store *teamstore.Store, chatID string, activity time.Time) {
	t.Helper()
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         chatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     time.Now().Add(time.Hour),
		LastActivityAt: activity,
	}); err != nil {
		t.Fatalf("schedule poll %s later: %v", chatID, err)
	}
}

func scheduleTeamsGraph429PollUnblocked(t *testing.T, store *teamstore.Store, chatID string, activity time.Time) {
	t.Helper()
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:            chatID,
		PollState:         inboundPollStateWarm,
		PreviousPollState: "",
		NextPollAt:        time.Now().Add(-time.Second),
		LastActivityAt:    activity,
		ClearBlockedUntil: true,
		ResetFailures:     true,
	}); err != nil {
		t.Fatalf("schedule poll %s unblocked: %v", chatID, err)
	}
}
