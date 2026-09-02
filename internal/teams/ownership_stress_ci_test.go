package teams

// These tests model the boundary between a Teams bridge and a local `cxp tui`
// (or another CXP process).  The local process is deliberately represented by
// a fake Codex writer rather than by starting a real helper process: the
// invariant under test is the durable request/ownership protocol, not process
// discovery or the Codex binary itself.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const teamsOwnershipStressStrictEnv = "CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_STRICT"

// The default scale is intentionally small enough for every CI run.  Setting
// CODEX_HELPER_TEAMS_OWNERSHIP_STRESS=1 or the individual knobs below makes
// the same deterministic scenarios useful as a local pressure test.
type teamsOwnershipStressScale struct {
	Burst      int
	Backlog    int
	PollRounds int
	Transcript int
}

func loadTeamsOwnershipStressScale() teamsOwnershipStressScale {
	scale := teamsOwnershipStressScale{Burst: 12, Backlog: 24, PollRounds: 12, Transcript: 24}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_OWNERSHIP_STRESS")) == "1" {
		scale = teamsOwnershipStressScale{Burst: 64, Backlog: 240, PollRounds: 80, Transcript: 160}
	}
	if value := positiveEnvInt("CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_BURST"); value > 0 {
		scale.Burst = value
	}
	if value := positiveEnvInt("CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_BACKLOG"); value > 0 {
		scale.Backlog = value
	}
	if value := positiveEnvInt("CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_POLLS"); value > 0 {
		scale.PollRounds = value
	}
	if value := positiveEnvInt("CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_TRANSCRIPT"); value > 0 {
		scale.Transcript = value
	}
	// These tests are intentionally useful as local pressure diagnostics, but
	// an accidental environment setting must not turn an ordinary package test
	// into an unbounded memory/time job.
	scale.Burst = capOwnershipStressKnob(scale.Burst, 256)
	scale.Backlog = capOwnershipStressKnob(scale.Backlog, 512)
	scale.PollRounds = capOwnershipStressKnob(scale.PollRounds, 1024)
	scale.Transcript = capOwnershipStressKnob(scale.Transcript, 512)
	return scale
}

func capOwnershipStressKnob(value int, max int) int {
	if value <= 0 {
		return value
	}
	if value > max {
		return max
	}
	return value
}

func ownershipStressTestTimeout(base time.Duration) time.Duration {
	// These scenarios deliberately hold a Graph request open while the race
	// detector and file-backed SQLite are active.  The short unit-test bound is
	// enough on an idle developer machine, but it can expire before the held
	// request is released in a busy full-suite CI worker.  That turns a
	// test-harness timeout into an ambiguous-send/deferred-delivery failure and
	// can strand the test goroutine. Keep the bound finite while giving the
	// deterministic scenario enough room to complete on every supported
	// platform.
	if base < 90*time.Second || runtime.GOOS == "windows" {
		return 90 * time.Second
	}
	return base
}

// TestTeamsOwnershipStressGraphStallThenTranscriptCatchupCI combines a long
// Graph outage with local TUI activity.  It checks two independent cursors:
// Graph failure must not advance the Teams inbound cursor, and the later local
// transcript sync must still publish every new visible TUI record exactly once.
func TestTeamsOwnershipStressGraphStallThenTranscriptCatchupCI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), ownershipStressTestTimeout(5*time.Second))
	defer cancel()
	scale := loadTeamsOwnershipStressScale()
	transcriptPath := filepathForOwnershipStress(t)
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-tui-catchup"}}`,
		`{"id":"tui-old","thread_id":"thread-tui-catchup","role":"assistant","text":"old TUI answer"}`,
		"",
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial TUI transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-tui-catchup", transcriptPath)
	defer restoreDiscover()

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{result: ExecutionResult{Text: "teams answer", CodexThreadID: "thread-tui-catchup", CodexTurnID: "teams-turn"}})
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-tui-catchup")
	seedOwnershipStressDuePoll(t, store, session.ChatID)
	initialPoll, ok, err := store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read seeded poll: ok=%v err=%v", ok, err)
	}

	blocked := make(chan struct{})
	release := make(chan struct{})
	readGraph := newOwnershipStressBlockingReadGraph(t, blocked, release)
	bridge.readGraph = readGraph
	seedOwnershipStressControlIdle(t, store)

	pollDone := make(chan error, 1)
	go func() {
		pollCtx, pollCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer pollCancel()
		_, err := bridge.pollChatWithRole(pollCtx, session.ChatID, 20, inboundPollRoleWork, false, func(context.Context, ChatMessage, string) error {
			return nil
		})
		pollDone <- err
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("Graph stall test did not reach the blocking read")
	}

	// While Graph is unavailable, the TUI keeps appending completed records.
	// These are deliberately visible records; hidden tool chatter is covered by
	// the dedicated transcript scanner tests.
	var appended []string
	for i := 0; i < scale.Transcript; i++ {
		marker := fmt.Sprintf("OWNERSHIP_STRESS_TUI_RECORD_%03d", i)
		appended = append(appended, marker)
		appendOwnershipStressTranscriptLine(t, transcriptPath, fmt.Sprintf(`{"id":"tui-%03d","thread_id":"thread-tui-catchup","role":"assistant","text":%q}`, i, marker))
	}
	if err := <-pollDone; err == nil {
		t.Fatalf("Graph stall poll returned nil; expected context cancellation")
	}
	close(release)

	poll, ok, err := store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read poll after Graph stall: ok=%v err=%v", ok, err)
	}
	if !poll.LastModifiedCursor.Equal(initialPoll.LastModifiedCursor) {
		// A failed request must not replace the durable cursor with a value from
		// an empty/partial response, nor advance it past unread Teams messages.
		t.Fatalf("Graph stall changed the durable inbound cursor: before=%#v after=%#v", initialPoll, poll)
	}

	if err := bridge.syncLinkedTranscripts(ctx); err != nil {
		t.Fatalf("sync TUI transcript after Graph recovery: %v", err)
	}
	// Transcript delivery is intentionally budgeted.  A pressure test must
	// distinguish a bounded batch from a lost checkpoint, so keep driving the
	// same recovery loop until the full local backlog is observed.
	for attempt := 0; attempt < 8; attempt++ {
		joined := sentPlainJoined(*sent)
		allPresent := true
		for _, marker := range appended {
			if !strings.Contains(joined, marker) {
				allPresent = false
				break
			}
		}
		if allPresent {
			break
		}
		if err := bridge.syncLinkedTranscripts(ctx); err != nil {
			t.Fatalf("resume TUI transcript sync attempt %d: %v", attempt+1, err)
		}
	}
	joined := sentPlainJoined(*sent)
	missing := make([]string, 0)
	duplicates := make([]string, 0)
	for _, marker := range appended {
		switch strings.Count(joined, marker) {
		case 0:
			missing = append(missing, marker)
		case 1:
		default:
			duplicates = append(duplicates, marker)
		}
	}
	if len(missing) != 0 || len(duplicates) != 0 {
		teamsOwnershipStressFinding(t, "Graph stall followed by TUI backlog delivery mismatch: missing=%v duplicates=%v", missing[:minOwnershipStressInt(len(missing), 8)], duplicates[:minOwnershipStressInt(len(duplicates), 8)])
	}
}

// TestTeamsOwnershipStressHeadReadFailureRecoversWithoutCursorAdvanceCI
// covers a bounded, retryable Graph outage rather than a permanently hanging
// request.  The failed head read must leave its cursor untouched, an unrelated
// chat must still be handled in the same poll cycle, and the next successful
// read must deliver the original message once.
func TestTeamsOwnershipStressHeadReadFailureRecoversWithoutCursorAdvanceCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "OWNERSHIP_STRESS_HEAD_RECOVERY_ANSWER",
		CodexThreadID: "thread-head-recovery",
		CodexTurnID:   "turn-head-recovery",
	}}
	first := &Session{ID: "s001", ChatID: "chat-1", CodexThreadID: "thread-head-recovery", Status: string(teamstore.SessionStatusActive)}
	second := &Session{ID: "s002", ChatID: "chat-2", CodexThreadID: "thread-other", Status: string(teamstore.SessionStatusActive)}
	bridge := newBridgeTestBridge(nil, store, executor)
	bridge.reg.Sessions = append(bridge.reg.Sessions, *second)
	bridge.reg.Sessions[0] = *first
	if err := bridge.ensureDurableSession(ctx, first); err != nil {
		t.Fatalf("ensure first session: %v", err)
	}
	if err := bridge.ensureDurableSession(ctx, second); err != nil {
		t.Fatalf("ensure second session: %v", err)
	}
	seedThreadRecoverySession(t, store, first, first.CodexThreadID, "")
	seedThreadRecoverySession(t, store, second, second.CodexThreadID, "")
	seedOwnershipStressControlIdle(t, store)
	cursor := time.Now().UTC().Add(-time.Minute)
	seedOwnershipStressDuePollAt(t, store, first.ChatID, cursor)
	seedOwnershipStressDuePollAt(t, store, second.ChatID, cursor)
	firstMessage := bridgeTestMessageWithText("head-recovery-message", "OWNERSHIP_STRESS_HEAD_RECOVERY_PROMPT")
	firstMessage.ChatID = first.ChatID
	firstMessage.CreatedDateTime = cursor.Add(10 * time.Second).Format(time.RFC3339Nano)
	firstMessage.LastModifiedDateTime = firstMessage.CreatedDateTime
	secondMessage := bridgeTestMessageWithText("other-chat-message", "OWNERSHIP_STRESS_OTHER_CHAT_PROMPT")
	secondMessage.ChatID = second.ChatID
	secondMessage.CreatedDateTime = cursor.Add(11 * time.Second).Format(time.RFC3339Nano)
	secondMessage.LastModifiedDateTime = secondMessage.CreatedDateTime

	var mu sync.Mutex
	requests := map[string]int{}
	recoveryAllowed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = fmt.Fprint(w, `{"id":"ownership-stress-head-recovery-sent","messageType":"message"}`)
			return
		}
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		chatID := parts[1]
		mu.Lock()
		requests[chatID]++
		requestNumber := requests[chatID]
		canRecover := recoveryAllowed
		mu.Unlock()
		if chatID == first.ChatID && !canRecover {
			http.Error(w, `{"error":{"code":"ServiceUnavailable"}}`, http.StatusServiceUnavailable)
			return
		}
		messages := []ChatMessage{}
		if chatID == first.ChatID && canRecover && requestNumber >= 1 {
			messages = []ChatMessage{firstMessage}
		} else if chatID == second.ChatID && requestNumber == 1 {
			messages = []ChatMessage{secondMessage}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": messages})
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		// GraphClient normalizes maxRetries <= 0 to its production default.
		// Keep this fixture deterministic without weakening that retry policy.
		sleep:  func(context.Context, time.Duration) error { return nil },
		jitter: func(d time.Duration) time.Duration { return d },
	}
	bridge.graph = graph
	bridge.readGraph = graph
	bridge.maxWorkChatPollsPerCycle = 2

	// pollOnce intentionally isolates a per-chat read error and may return a
	// nil aggregate error after allowing the other due chat to progress.
	_ = bridge.pollOnce(ctx, 20)
	firstPoll, ok, err := store.ChatPoll(ctx, first.ChatID)
	if err != nil || !ok {
		t.Fatalf("load first poll after 503: ok=%v err=%v", ok, err)
	}
	if !firstPoll.LastModifiedCursor.Equal(cursor) {
		mu.Lock()
		requestSnapshot := map[string]int{}
		for chatID, count := range requests {
			requestSnapshot[chatID] = count
		}
		mu.Unlock()
		t.Fatalf("head 503 advanced cursor: got=%s want=%s requests=%v", firstPoll.LastModifiedCursor, cursor, requestSnapshot)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after first poll: %v", err)
	}
	if got := ownershipStressInboundCount(state, second.ID, secondMessage.ID); got != 1 {
		t.Fatalf("unrelated chat inbound count after head 503 = %d, want one", got)
	}
	if got := ownershipStressInboundCount(state, first.ID, firstMessage.ID); got != 0 {
		t.Fatalf("failed head message persisted before recovery: count=%d", got)
	}
	mu.Lock()
	firstRequests := requests[first.ChatID]
	secondRequests := requests[second.ChatID]
	mu.Unlock()
	if firstRequests == 0 || secondRequests == 0 {
		t.Fatalf("head failure poll did not exercise both chats: first_requests=%d second_requests=%d", firstRequests, secondRequests)
	}
	if firstPoll.LastError == "" {
		t.Fatalf("head failure poll did not retain a per-chat error")
	}
	secondPoll, ok, err := store.ChatPoll(ctx, second.ChatID)
	if err != nil || !ok {
		t.Fatalf("load second poll after head 503: ok=%v err=%v", ok, err)
	}
	if secondPoll.LastError != "" {
		t.Fatalf("unrelated chat inherited head failure: %#v", secondPoll)
	}
	mu.Lock()
	recoveryAllowed = true
	mu.Unlock()

	if _, err := bridge.pollChatWithRole(ctx, first.ChatID, 20, inboundPollRoleWork, false, func(ctx context.Context, msg ChatMessage, text string) error {
		return bridge.handleSessionMessage(ctx, first.ChatID, msg, text)
	}); err != nil {
		t.Fatalf("recovery head poll: %v", err)
	}
	if _, err := bridge.pollChatWithRole(ctx, first.ChatID, 20, inboundPollRoleWork, false, func(ctx context.Context, msg ChatMessage, text string) error {
		return bridge.handleSessionMessage(ctx, first.ChatID, msg, text)
	}); err != nil {
		t.Fatalf("repeat recovered head poll: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}
	if got := ownershipStressInboundCount(state, first.ID, firstMessage.ID); got != 1 {
		t.Fatalf("recovered head message inbound count = %d, want exactly one", got)
	}
}

// TestTeamsOwnershipStressGraphStallDoesNotStopOtherChatPollCI models the
// service-level failure mode where one chat's Graph request hangs while the
// listener has other due chats to poll.  The current poll loop is intentionally
// observed as a diagnostic here: a second chat should be allowed to reach its
// own Graph request before the first request is released.
func TestTeamsOwnershipStressGraphStallDoesNotStopOtherChatPollCI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &teamsOwnershipStressExecutor{})
	seedOwnershipStressControlIdle(t, store)
	bridge.maxWorkChatPollsPerCycle = 2
	first := bridge.reg.SessionByID("s001")
	if first == nil {
		t.Fatal("missing first work session")
	}
	if err := bridge.ensureDurableSession(ctx, first); err != nil {
		t.Fatalf("ensure first durable session: %v", err)
	}
	second := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
	seedOwnershipStressDuePoll(t, store, first.ChatID)
	seedOwnershipStressDuePoll(t, store, second.ChatID)

	entered := make(chan struct{})
	otherProgress := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	firstChat := ""
	var progressOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		chatID := parts[1]
		mu.Lock()
		isFirst := firstChat == ""
		if isFirst {
			firstChat = chatID
		}
		mu.Unlock()
		if isFirst {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		} else {
			progressOnce.Do(func() { close(otherProgress) })
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	pollDone := make(chan error, 1)
	go func() { pollDone <- bridge.pollOnce(ctx, 20) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("poll loop did not reach the first blocked Graph chat")
	}
	select {
	case <-otherProgress:
	case <-time.After(2 * time.Second):
		// This is a diagnostic watchdog, not a product latency SLA.  The
		// generous timeout only prevents a broken poll loop from hanging CI.
		mu.Lock()
		observedFirst := firstChat
		mu.Unlock()
		teamsOwnershipStressFinding(t, "Graph stall blocked every other due chat before its own poll request: first_chat=%q", observedFirst)
	}
	close(release)
	select {
	case err := <-pollDone:
		if err != nil {
			t.Fatalf("poll loop after releasing Graph stall: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("poll loop did not finish after releasing Graph stall")
	}
}

// TestTeamsOwnershipStressControlGraphStallDoesNotStopWorkPollCI covers the
// control-chat variant. Control polling happens before work-chat scheduling,
// so a connection that never returns must be bounded and isolated rather than
// preventing the service from reading user messages in other chats.
func TestTeamsOwnershipStressControlGraphStallDoesNotStopWorkPollCI(t *testing.T) {
	previousTimeout := inboundPollGraphTimeout
	inboundPollGraphTimeout = 25 * time.Millisecond
	t.Cleanup(func() { inboundPollGraphTimeout = previousTimeout })

	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(newOwnershipStressWriteGraph(t), store, &teamsOwnershipStressExecutor{})
	seedOwnershipStressControlIdle(t, store)
	first := bridge.reg.SessionByID("s001")
	if first == nil {
		t.Fatal("missing first work session")
	}
	if err := bridge.ensureDurableSession(context.Background(), first); err != nil {
		t.Fatalf("ensure work session durable: %v", err)
	}
	seedOwnershipStressDuePoll(t, store, first.ChatID)
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         bridge.reg.ControlChatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     time.Now().Add(-time.Second),
		LastActivityAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("make control poll due: %v", err)
	}

	workProgress := make(chan struct{}, 1)
	var mu sync.Mutex
	workReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		chatID := parts[1]
		if chatID == bridge.reg.ControlChatID {
			<-r.Context().Done()
			return
		}
		mu.Lock()
		workReads++
		mu.Unlock()
		select {
		case workProgress <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	err := bridge.pollOnce(context.Background(), 20)
	if err == nil {
		t.Fatal("pollOnce returned nil after bounded control Graph timeout")
	}
	select {
	case <-workProgress:
	case <-time.After(time.Second):
		t.Fatal("work chat did not reach Graph after control Graph timeout")
	}
	mu.Lock()
	gotWorkReads := workReads
	mu.Unlock()
	if gotWorkReads != 1 {
		t.Fatalf("work Graph reads = %d, want one", gotWorkReads)
	}
}

// TestTeamsOwnershipStressControlReplyStallStillReachesWorkPollCI covers the
// less obvious control-chat stall: the head GET succeeds, but publishing the
// control response never returns. The real Graph client has a whole-request
// timeout; after that bounded failure, the work scheduler must still receive
// a quantum and the control message must remain retryable.
func TestTeamsOwnershipStressControlReplyStallStillReachesWorkPollCI(t *testing.T) {
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(newOwnershipStressWriteGraph(t), store, &teamsOwnershipStressExecutor{})
	seedOwnershipStressControlIdle(t, store)
	work := bridge.reg.SessionByID("s001")
	if work == nil {
		t.Fatal("missing work session")
	}
	if err := bridge.ensureDurableSession(context.Background(), work); err != nil {
		t.Fatalf("ensure work session durable: %v", err)
	}
	seedOwnershipStressDuePoll(t, store, bridge.reg.ControlChatID)
	seedOwnershipStressDuePoll(t, store, work.ChatID)

	var workReads int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 2 || parts[0] != "chats" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		chatID := parts[1]
		switch {
		case r.Method == http.MethodGet && chatID == bridge.reg.ControlChatID && strings.HasSuffix(r.URL.Path, "/messages"):
			msg := bridgeTestMessageWithText("control-help-stall", "help")
			msg.ChatID = chatID
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{msg}})
		case r.Method == http.MethodPost && chatID == bridge.reg.ControlChatID && strings.HasSuffix(r.URL.Path, "/messages"):
			select {
			case <-r.Context().Done():
			case <-time.After(75 * time.Millisecond):
				w.WriteHeader(http.StatusGatewayTimeout)
			}
		case r.Method == http.MethodGet && chatID == work.ChatID && strings.HasSuffix(r.URL.Path, "/messages"):
			mu.Lock()
			workReads++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"value":[]}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	httpClient := server.Client()
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     httpClient,
		baseURL:    server.URL,
		maxRetries: 1,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge.graph = bridge.readGraph

	err := bridge.pollOnce(context.Background(), 20)
	if err == nil {
		t.Fatal("pollOnce returned nil after bounded control reply timeout")
	}
	mu.Lock()
	gotWorkReads := workReads
	mu.Unlock()
	if gotWorkReads != 1 {
		t.Fatalf("work Graph reads after control reply stall = %d, want one", gotWorkReads)
	}
	poll, ok, pollErr := store.ChatPoll(context.Background(), bridge.reg.ControlChatID)
	if pollErr != nil || !ok {
		t.Fatalf("control poll after reply stall: ok=%v err=%v", ok, pollErr)
	}
	if poll.LastError == "" || poll.ContinuationPath != "" {
		t.Fatalf("control reply stall was not retained as retryable state: %#v", poll)
	}
}

// TestTeamsOwnershipStressContinuousControlBacklogDoesNotStarveWorkCI models
// a control chat that receives a fresh command on every cycle. Work polling
// must still receive its own bounded quantum instead of returning immediately
// after each control command.
func TestTeamsOwnershipStressContinuousControlBacklogDoesNotStarveWorkCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	controlChat := bridge.reg.ControlChatID
	workSession := bridge.reg.SessionByID("s001")
	if workSession == nil {
		t.Fatal("missing default work session")
	}
	seedOwnershipStressDuePoll(t, store, controlChat)
	seedOwnershipStressDuePoll(t, store, workSession.ChatID)

	var mu sync.Mutex
	controlReads := 0
	workReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 || parts[0] != "chats" || parts[2] != "messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if parts[1] == controlChat {
			mu.Lock()
			controlReads++
			id := fmt.Sprintf("control-help-%d", controlReads)
			mu.Unlock()
			msg := bridgeTestMessageWithText(id, "help")
			msg.ChatID = controlChat
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{msg}})
			return
		}
		if parts[1] == workSession.ChatID {
			mu.Lock()
			workReads++
			mu.Unlock()
			_, _ = fmt.Fprint(w, `{"value":[]}`)
			return
		}
		http.Error(w, "unexpected chat", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	for cycle := 0; cycle < 2; cycle++ {
		if cycle > 0 {
			for _, chatID := range []string{controlChat, workSession.ChatID} {
				if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
					ChatID:     chatID,
					NextPollAt: time.Now().Add(-time.Second),
				}); err != nil {
					t.Fatalf("re-due chat %s: %v", chatID, err)
				}
			}
		}
		if err := bridge.pollOnce(ctx, 20); err != nil {
			t.Fatalf("continuous control cycle %d: %v", cycle, err)
		}
	}
	mu.Lock()
	gotControlReads, gotWorkReads := controlReads, workReads
	mu.Unlock()
	if gotControlReads != 2 || gotWorkReads != 2 {
		t.Fatalf("continuous control backlog starved work: control_reads=%d work_reads=%d", gotControlReads, gotWorkReads)
	}
}

// TestTeamsOwnershipStressHeadTrafficDoesNotStarveContinuationCI checks that
// a continuous stream of fresh head messages does not permanently suppress an
// older durable continuation backlog.  The callback deliberately avoids
// starting Codex turns so this isolates poll fairness from execution state.
func TestTeamsOwnershipStressHeadTrafficDoesNotStarveContinuationCI(t *testing.T) {
	ctx := context.Background()
	scale := loadTeamsOwnershipStressScale()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &teamsOwnershipStressExecutor{})
	session := bridge.reg.SessionByID("s001")
	seedOwnershipStressDuePoll(t, store, session.ChatID)
	oldContinuation := "/chats/chat-1/messages?$skiptoken=old-backlog"
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, session.ChatID, time.Now().Add(-time.Minute), true, true, 20, oldContinuation); err != nil {
		t.Fatalf("seed old continuation: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     time.Now().Add(-time.Minute),
		LastActivityAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("seed continuation schedule: %v", err)
	}

	var mu sync.Mutex
	headRequests := 0
	continuationRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		if r.URL.Query().Get("$skiptoken") == "old-backlog" {
			continuationRequests++
			mu.Unlock()
			_, _ = fmt.Fprint(w, `{"value":[{"id":"old-backlog-1","messageType":"message","createdDateTime":"2026-08-22T00:00:00Z","lastModifiedDateTime":"2026-08-22T00:00:00Z","from":{"user":{"id":"user-1","displayName":"User"}},"body":{"contentType":"html","content":"old backlog"}}],"@odata.nextLink":"/chats/chat-1/messages?$skiptoken=old-backlog-2"}`)
			return
		}
		headRequests++
		head := headRequests
		mu.Unlock()
		msg := bridgeTestMessageWithText(fmt.Sprintf("head-traffic-%03d", head), fmt.Sprintf("OWNERSHIP_STRESS_HEAD_TRAFFIC_%03d", head))
		msg.CreatedDateTime = time.Now().UTC().Format(time.RFC3339Nano)
		msg.LastModifiedDateTime = msg.CreatedDateTime
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{msg}})
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	rounds := scale.Burst
	if rounds < 8 {
		rounds = 8
	}
	for round := 0; round < rounds; round++ {
		if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, func(context.Context, ChatMessage, string) error {
			return nil
		}); err != nil {
			t.Fatalf("head traffic poll round %d: %v", round, err)
		}
	}
	mu.Lock()
	continuations := continuationRequests
	heads := headRequests
	mu.Unlock()
	if continuations == 0 {
		teamsOwnershipStressFinding(t, "continuous actionable head traffic starved durable continuation: head_requests=%d continuation_requests=%d", heads, continuations)
	}
}

// TestTeamsOwnershipStressWarmChatNotStarvedByHotCycleCI goes through the
// actual pollOnce scheduler: eight continuously due hot chats compete with a
// due warm chat. The warm chat must receive one bounded poll in the same cycle
// rather than waiting forever behind the hot set.
func TestTeamsOwnershipStressWarmChatNotStarvedByHotCycleCI(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store := newBridgeTestStore(t)
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	seedOwnershipStressControlIdle(t, store)
	bridge.maxWorkChatPollsPerCycle = 8
	sessions := make([]Session, 0, 9)
	for i := 1; i <= 8; i++ {
		chatID := fmt.Sprintf("hot-chat-%02d", i)
		session := Session{ID: fmt.Sprintf("hot-session-%02d", i), ChatID: chatID, Status: "active", UpdatedAt: now}
		sessions = append(sessions, session)
		if _, err := store.RecordChatPollSuccess(ctx, chatID, now, true, false, 0); err != nil {
			t.Fatalf("seed hot poll %s: %v", chatID, err)
		}
		if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
			ChatID:         chatID,
			PollState:      inboundPollStateHot,
			NextPollAt:     now.Add(-time.Second),
			LastActivityAt: now.Add(-time.Minute),
		}); err != nil {
			t.Fatalf("schedule hot poll %s: %v", chatID, err)
		}
	}
	warmChat := "warm-backlog-chat"
	sessions = append(sessions, Session{ID: "warm-backlog-session", ChatID: warmChat, Status: "active", UpdatedAt: now.Add(-10 * time.Minute)})
	if _, err := store.RecordChatPollSuccess(ctx, warmChat, now.Add(-10*time.Minute), true, false, 0); err != nil {
		t.Fatalf("seed warm poll: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         warmChat,
		PollState:      inboundPollStateWarm,
		NextPollAt:     now.Add(-time.Second),
		LastActivityAt: now.Add(-10 * time.Minute),
	}); err != nil {
		t.Fatalf("schedule warm poll: %v", err)
	}
	bridge.reg.Sessions = sessions
	var mu sync.Mutex
	reads := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 || parts[0] != "chats" || parts[2] != "messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		mu.Lock()
		reads[parts[1]]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	if err := bridge.pollOnce(ctx, 20); err != nil {
		t.Fatalf("hot/warm scheduler cycle: %v", err)
	}
	mu.Lock()
	warmReads := reads[warmChat]
	totalReads := 0
	for _, count := range reads {
		totalReads += count
	}
	mu.Unlock()
	if warmReads != 1 || totalReads != 8 {
		t.Fatalf("warm chat was starved or poll cap was exceeded: warm_reads=%d total_reads=%d reads=%v", warmReads, totalReads, reads)
	}
}

// TestTeamsOwnershipStressDueHotChatsRotateBeyondCycleCapCI catches the
// opposite side of the warm-chat reservation: a continuously due operational
// frontier must also age fairly when there are more hot chats than the per
// cycle cap.  Each pass re-dueing all chats models a catch-up lane that keeps
// its NextPollAt at now; durable LastSuccessfulPollAt must still rotate the
// selected set.
func TestTeamsOwnershipStressDueHotChatsRotateBeyondCycleCapCI(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store := newBridgeTestStore(t)
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	seedOwnershipStressControlIdle(t, store)
	bridge.maxWorkChatPollsPerCycle = 8
	const hotChats = 9
	sessions := make([]Session, 0, hotChats)
	for i := 1; i <= hotChats; i++ {
		chatID := fmt.Sprintf("rotating-hot-chat-%02d", i)
		sessionID := fmt.Sprintf("rotating-hot-session-%02d", i)
		sessions = append(sessions, Session{ID: sessionID, ChatID: chatID, Status: "active", UpdatedAt: now})
		if _, err := store.RecordChatPollSuccess(ctx, chatID, now.Add(-time.Duration(i)*time.Second), true, false, 0); err != nil {
			t.Fatalf("seed hot poll %s: %v", chatID, err)
		}
		if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
			ChatID:         chatID,
			PollState:      inboundPollStateHot,
			NextPollAt:     now.Add(-time.Second),
			LastActivityAt: now,
		}); err != nil {
			t.Fatalf("schedule hot poll %s: %v", chatID, err)
		}
	}
	bridge.reg.Sessions = sessions
	var mu sync.Mutex
	reads := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 || parts[0] != "chats" || parts[2] != "messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		mu.Lock()
		reads[parts[1]]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	for cycle := 0; cycle < hotChats; cycle++ {
		if cycle > 0 {
			for _, session := range sessions {
				if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
					ChatID:     session.ChatID,
					NextPollAt: time.Now().Add(-time.Second),
				}); err != nil {
					t.Fatalf("re-due hot chat %s in cycle %d: %v", session.ChatID, cycle, err)
				}
			}
		}
		if err := bridge.pollOnce(ctx, 20); err != nil {
			t.Fatalf("rotating hot cycle %d: %v", cycle, err)
		}
	}

	mu.Lock()
	gotReads := make(map[string]int, len(reads))
	for chatID, count := range reads {
		gotReads[chatID] = count
	}
	mu.Unlock()
	for _, session := range sessions {
		if gotReads[session.ChatID] == 0 {
			t.Fatalf("due hot chat starved across cap cycles: chat=%s reads=%v", session.ChatID, gotReads)
		}
	}
}

// TestTeamsOwnershipStressFifthChatReachesNextWorkerWaveCI verifies the
// explicit worker-wave boundary. Four due chats may occupy the bounded Graph
// workers until their per-request timeout, but a fifth due chat must remain
// queued for the next wave rather than being dropped from the cycle.
func TestTeamsOwnershipStressFifthChatReachesNextWorkerWaveCI(t *testing.T) {
	previousTimeout := inboundPollGraphTimeout
	inboundPollGraphTimeout = 25 * time.Millisecond
	t.Cleanup(func() { inboundPollGraphTimeout = previousTimeout })
	ctx := context.Background()
	now := time.Now()
	store := newBridgeTestStore(t)
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &teamsOwnershipStressExecutor{})
	seedOwnershipStressControlIdle(t, store)
	bridge.maxWorkChatPollsPerCycle = 5
	sessions := make([]Session, 0, 5)
	for i := 1; i <= 5; i++ {
		chatID := fmt.Sprintf("wave-chat-%02d", i)
		session := Session{ID: fmt.Sprintf("wave-session-%02d", i), ChatID: chatID, Status: "active", UpdatedAt: now}
		sessions = append(sessions, session)
		if _, err := store.RecordChatPollSuccess(ctx, chatID, now, true, false, 0); err != nil {
			t.Fatalf("seed wave poll %s: %v", chatID, err)
		}
		if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
			ChatID:         chatID,
			PollState:      inboundPollStateWarm,
			NextPollAt:     now.Add(-time.Second),
			LastActivityAt: now,
		}); err != nil {
			t.Fatalf("schedule wave poll %s: %v", chatID, err)
		}
	}
	bridge.reg.Sessions = sessions
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/messages") {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		mu.Lock()
		requests++
		ordinal := requests
		mu.Unlock()
		if ordinal <= 4 {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 1,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	_ = bridge.pollOnce(ctx, 20)
	mu.Lock()
	gotRequests := requests
	mu.Unlock()
	if gotRequests != 5 {
		t.Fatalf("worker wave requests = %d, want all five due chats to reach Graph", gotRequests)
	}
}

// TestTeamsOwnershipStressSQLiteHeartbeatSurvivesSaturatedGraphWorkersCI
// composes two independent service liveness boundaries. Four due chats occupy
// every Graph worker until their per-request deadline, while a fifth chat is
// waiting for the next worker wave. The owner heartbeat must still advance on
// the SQLite runtime projection, the control lease must remain valid, and the
// fifth chat must reach Graph instead of being lost behind the outage.
func TestTeamsOwnershipStressSQLiteHeartbeatSurvivesSaturatedGraphWorkersCI(t *testing.T) {
	previousTimeout := inboundPollGraphTimeout
	inboundPollGraphTimeout = 500 * time.Millisecond
	t.Cleanup(func() { inboundPollGraphTimeout = previousTimeout })

	// Race instrumentation makes the first SQLite connection/schema path and
	// the worker-wave setup materially slower. Keep the same bounded scenario,
	// but leave enough time for the barrier to become observable on a busy CI
	// runner.
	ctx, cancel := context.WithTimeout(context.Background(), ownershipStressTestTimeout(10*time.Second))
	defer cancel()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	seedOwnershipStressControlIdle(t, store)

	sessions := make([]*Session, 0, 5)
	first := bridge.reg.SessionByID("s001")
	if first == nil {
		t.Fatal("missing first work session")
	}
	if err := bridge.ensureDurableSession(ctx, first); err != nil {
		t.Fatalf("ensure first durable session: %v", err)
	}
	sessions = append(sessions, first)
	for i := 2; i <= 5; i++ {
		sessions = append(sessions, appendBridgeTestSession(t, bridge, store, fmt.Sprintf("s%03d", i), fmt.Sprintf("heartbeat-wave-chat-%02d", i)))
	}
	for _, session := range sessions {
		seedOwnershipStressDuePoll(t, store, session.ChatID)
	}
	bridge.maxWorkChatPollsPerCycle = len(sessions)
	bridge.leaseDuration = time.Minute
	if active, err := bridge.claimControlLease(ctx); err != nil || !active {
		t.Fatalf("claim control lease: active=%t err=%v", active, err)
	}
	owner, err := teamstore.CurrentOwner("sqlite-worker-saturation", "", "", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner: %v", err)
	}
	owner.ScopeID = bridge.scope.ID
	owner.MachineID = bridge.machine.ID
	owner.LeaseGeneration = bridge.currentLeaseGeneration()
	bridge.setOwner(owner, time.Minute)
	bridge.ownerHeartbeatInterval = 5 * time.Millisecond
	if err := bridge.recordOwnerHeartbeat(ctx, "", ""); err != nil {
		t.Fatalf("initial owner heartbeat: %v", err)
	}
	initialOwner, ok, err := store.ReadOwner(ctx)
	if err != nil || !ok {
		t.Fatalf("read initial owner: ok=%v err=%v", ok, err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate saturated worker store to SQLite: %v", err)
	}

	var mu sync.Mutex
	requestCount := 0
	activeRequests := 0
	maxActiveRequests := 0
	var saturatedOnce sync.Once
	var fifthOnce sync.Once
	saturated := make(chan struct{})
	fifthRead := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/messages") {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		mu.Lock()
		requestCount++
		ordinal := requestCount
		if ordinal <= 4 {
			activeRequests++
			if activeRequests > maxActiveRequests {
				maxActiveRequests = activeRequests
			}
		}
		if activeRequests == maxConcurrentWorkChatPolls {
			saturatedOnce.Do(func() { close(saturated) })
		}
		mu.Unlock()
		if ordinal <= 4 {
			<-r.Context().Done()
			mu.Lock()
			activeRequests--
			mu.Unlock()
			return
		}
		if ordinal == 5 {
			fifthOnce.Do(func() { close(fifthRead) })
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := bridge.startOwnerHeartbeat(heartbeatCtx)
	pollDone := make(chan error, 1)
	go func() { pollDone <- bridge.pollOnce(ctx, 20) }()
	pollFinished := false
	t.Cleanup(func() {
		stopHeartbeat()
		cancel()
		if !pollFinished {
			select {
			case err := <-pollDone:
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("poll cycle cleanup error: %v", err)
				}
			case <-time.After(time.Second):
				t.Errorf("poll cycle did not stop during test cleanup")
			}
		}
		select {
		case err := <-heartbeatDone:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("owner heartbeat cleanup error: %v", err)
			}
		case <-time.After(time.Second):
			t.Errorf("owner heartbeat did not stop during test cleanup")
		}
	})
	select {
	case <-saturated:
	case pollErr := <-pollDone:
		pollFinished = true
		t.Fatalf("poll cycle ended before four Graph workers became saturated: %v", pollErr)
	case <-ctx.Done():
		t.Fatal("four Graph workers did not become saturated")
	}

	heartbeatDeadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	heartbeatAdvanced := false
	for !heartbeatAdvanced {
		latest, found, readErr := store.ReadOwner(ctx)
		if readErr != nil {
			t.Fatalf("read owner during saturated Graph workers: %v", readErr)
		}
		if found && latest.LastHeartbeat.After(initialOwner.LastHeartbeat) {
			heartbeatAdvanced = true
			break
		}
		select {
		case <-heartbeatDeadline.C:
			t.Fatal("owner heartbeat did not advance while all Graph workers were blocked")
		case <-ticker.C:
		}
	}
	if !heartbeatDeadline.Stop() {
		select {
		case <-heartbeatDeadline.C:
		default:
		}
	}
	ticker.Stop()

	select {
	case <-fifthRead:
	case <-ctx.Done():
		t.Fatal("fifth due chat did not reach Graph after the first worker wave")
	}
	select {
	case pollErr := <-pollDone:
		pollFinished = true
		if pollErr != nil && !errors.Is(pollErr, context.Canceled) && !errors.Is(pollErr, context.DeadlineExceeded) {
			t.Fatalf("poll cycle returned unexpected error after saturated Graph workers: %v", pollErr)
		}
	case <-ctx.Done():
		t.Fatal("poll cycle did not finish after saturated Graph workers timed out")
	}

	mu.Lock()
	gotRequests := requestCount
	mu.Unlock()
	if gotRequests != 5 {
		t.Fatalf("saturated Graph worker requests = %d, want exactly five due chats", gotRequests)
	}
	if maxActiveRequests != maxConcurrentWorkChatPolls {
		t.Fatalf("maximum simultaneous Graph worker requests = %d, want %d", maxActiveRequests, maxConcurrentWorkChatPolls)
	}
	latestOwner, found, err := store.ReadOwner(ctx)
	if err != nil || !found {
		t.Fatalf("read final owner: found=%v err=%v", found, err)
	}
	if !latestOwner.LastHeartbeat.After(initialOwner.LastHeartbeat) {
		t.Fatalf("final owner heartbeat did not advance: initial=%s final=%s", initialOwner.LastHeartbeat, latestOwner.LastHeartbeat)
	}
	lease := bridge.currentLease()
	if _, err := store.ValidateControlLease(ctx, bridge.machine.ID, lease.Generation, time.Now()); err != nil {
		t.Fatalf("control lease was lost during saturated Graph workers: %v", err)
	}
}

// TestTeamsOwnershipStressOverlappingOutOfOrderPagesAreExactlyOnceCI models a
// normal Graph paging race: the head window overlaps the continuation page,
// and the two pages are not ordered the same way.  A service restart can see
// this shape whenever new Teams messages arrive while an old backlog is being
// drained.  The durable inbound ledger and cursor must make the overlap
// harmless without losing the older message.
func TestTeamsOwnershipStressOverlappingOutOfOrderPagesAreExactlyOnceCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &teamsOwnershipStressExecutor{})
	session := bridge.reg.SessionByID("s001")

	message := func(id string, at time.Time) ChatMessage {
		msg := bridgeTestMessageWithText(id, "OWNERSHIP_STRESS_PAGE_"+id)
		msg.ChatID = session.ChatID
		msg.CreatedDateTime = at.UTC().Format(time.RFC3339Nano)
		msg.LastModifiedDateTime = msg.CreatedDateTime
		return msg
	}
	base := time.Now().UTC().Add(-5 * time.Minute)
	seedOwnershipStressDuePollAt(t, store, session.ChatID, base.Add(-time.Minute))
	old := message("page-old", base)
	overlap := message("page-overlap", base.Add(time.Minute))
	newest := message("page-newest", base.Add(2*time.Minute))
	var mu sync.Mutex
	var requests []string
	headRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/"+session.ChatID+"/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		token := r.URL.Query().Get("$skiptoken")
		mu.Lock()
		requests = append(requests, token)
		if token == "" {
			headRequests++
		}
		currentHeadRequest := headRequests
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if token == "older" {
			// Deliberately reverse the chronological order and repeat the
			// overlap item from the head page.
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{overlap, old}})
			return
		}
		payload := map[string]any{"value": []ChatMessage{newest, overlap}}
		if currentHeadRequest == 1 {
			payload["@odata.nextLink"] = "/chats/" + session.ChatID + "/messages?$skiptoken=older"
		}
		_ = json.NewEncoder(w).Encode(payload)
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}
	if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("head page poll: %v", err)
	}
	if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("overlapping continuation poll: %v", err)
	}
	// The normal backlog quantum is one actionable message. The old page itself
	// can therefore need a replay after its first message is handled; the
	// staged receipt must make that replay local and exactly-once.
	if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("final overlap continuation poll: %v", err)
	}
	if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("replay remaining overlap continuation poll: %v", err)
	}

	counts := map[string]int{}
	for _, id := range handled {
		counts[id]++
	}
	for _, id := range []string{old.ID, overlap.ID, newest.ID} {
		if counts[id] != 1 {
			t.Fatalf("message %s handled %d times, want exactly once; handled=%v", id, counts[id], handled)
		}
	}
	if len(handled) != 3 {
		t.Fatalf("handled IDs = %v, want exactly three unique messages", handled)
	}
	poll, ok, err := store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("load final poll: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != "" || !poll.LastModifiedCursor.Equal(newestTime(newest)) {
		t.Fatalf("final poll = %#v, want drained continuation and newest cursor; handled=%v", poll, handled)
	}
	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 || gotRequests[0] != "" || gotRequests[1] != "older" {
		t.Fatalf("Graph requests = %v, want one head and one continuation read; staged replays must be local", gotRequests)
	}
}

func newestTime(msg ChatMessage) time.Time {
	value, _ := time.Parse(time.RFC3339Nano, msg.LastModifiedDateTime)
	return value
}

// TestTeamsOwnershipStressHeadContinuationSurvivesOldContinuationFailureCI
// models two durable frontiers at once: the previous poll is draining an old
// continuation while the new head response exposes another page.  A retry of
// the old continuation must not discard the fresh head path when a later head
// response is no longer truncated.
func TestTeamsOwnershipStressHeadContinuationSurvivesOldContinuationFailureCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &teamsOwnershipStressExecutor{})
	session := bridge.reg.SessionByID("s001")
	base := time.Now().UTC().Add(-10 * time.Minute)
	seedOwnershipStressDuePollAt(t, store, session.ChatID, base)
	oldContinuation := "/chats/" + session.ChatID + "/messages?$skiptoken=old-frontier"
	newContinuation := "/chats/" + session.ChatID + "/messages?$skiptoken=new-frontier"
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, session.ChatID, base, true, true, 20, oldContinuation); err != nil {
		t.Fatalf("seed old continuation: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     base,
		LastActivityAt: base,
	}); err != nil {
		t.Fatalf("seed continuation schedule: %v", err)
	}
	message := func(id string, at time.Time) ChatMessage {
		msg := bridgeTestMessageWithText(id, "OWNERSHIP_STRESS_DUAL_FRONTIER_"+id)
		msg.ChatID = session.ChatID
		stamp := at.UTC().Format(time.RFC3339Nano)
		msg.CreatedDateTime = stamp
		msg.LastModifiedDateTime = stamp
		return msg
	}
	headMessage := message("dual-head", base.Add(time.Minute))
	oldMessage := message("dual-old", base.Add(30*time.Second))
	newMessage := message("dual-new", base.Add(2*time.Minute))
	var mu sync.Mutex
	headRequests := 0
	oldRequests := 0
	newRequests := 0
	oldRecoveryAllowed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/"+session.ChatID+"/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		token := r.URL.Query().Get("$skiptoken")
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		switch token {
		case "":
			headRequests++
		case "old-frontier":
			oldRequests++
		case "new-frontier":
			newRequests++
		}
		currentHead := headRequests
		mu.Unlock()
		switch token {
		case "":
			if currentHead == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"value":           []ChatMessage{headMessage},
					"@odata.nextLink": newContinuation,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
		case "old-frontier":
			if !oldRecoveryAllowed {
				http.Error(w, `{"error":{"code":"temporary"}}`, http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{oldMessage}})
		case "new-frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{newMessage}})
		}
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 1,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handled = append(handled, msg.ID)
		return nil
	}
	if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, handle); err == nil {
		t.Fatal("first dual-frontier poll returned nil despite old continuation failure")
	}
	oldRecoveryAllowed = true
	for i := 0; i < 3; i++ {
		if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, handle); err != nil {
			t.Fatalf("dual-frontier recovery poll %d: %v", i+1, err)
		}
	}
	counts := map[string]int{}
	for _, id := range handled {
		counts[id]++
	}
	for _, id := range []string{headMessage.ID, oldMessage.ID, newMessage.ID} {
		if counts[id] != 1 {
			t.Fatalf("dual-frontier message %s handled %d times, handled=%v", id, counts[id], handled)
		}
	}
	mu.Lock()
	gotNewRequests := newRequests
	mu.Unlock()
	if gotNewRequests == 0 {
		t.Fatalf("fresh head continuation was discarded: handled=%v", handled)
	}
}

// TestTeamsOwnershipStressContinuationFailureDoesNotBlockOtherChatCI covers a
// durable old continuation failing after the current head was read.  The
// failed chat may remain blocked, but the failure must not stop an unrelated
// Work chat from being polled successfully.
func TestTeamsOwnershipStressContinuationFailureDoesNotBlockOtherChatCI(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		status int
	}{
		{name: "rate-limit", status: http.StatusTooManyRequests},
		{name: "server-error", status: http.StatusServiceUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(nil, store, &teamsOwnershipStressExecutor{})
			first := bridge.reg.SessionByID("s001")
			second := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
			seedOwnershipStressDuePoll(t, store, first.ChatID)
			seedOwnershipStressDuePoll(t, store, second.ChatID)
			oldContinuation := "/chats/chat-1/messages?$skiptoken=old-failing-page"
			if _, err := store.RecordChatPollSuccessWithContinuation(ctx, first.ChatID, time.Now().Add(-time.Minute), true, true, 20, oldContinuation); err != nil {
				t.Fatalf("seed first continuation: %v", err)
			}
			if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
				ChatID:         first.ChatID,
				PollState:      inboundPollStateWarm,
				NextPollAt:     time.Now().Add(-time.Minute),
				LastActivityAt: time.Now().Add(-time.Minute),
			}); err != nil {
				t.Fatalf("seed first continuation schedule: %v", err)
			}

			var mu sync.Mutex
			requests := map[string]int{}
			continuationRequests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
					http.Error(w, "unexpected request", http.StatusNotFound)
					return
				}
				parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				chatID := parts[1]
				mu.Lock()
				requests[chatID]++
				if chatID == first.ChatID && r.URL.Query().Get("$skiptoken") == "old-failing-page" {
					continuationRequests++
				}
				mu.Unlock()
				if chatID == first.ChatID && r.URL.Query().Get("$skiptoken") == "old-failing-page" {
					http.Error(w, `{"error":{"code":"temporary"}}`, testCase.status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"value":[]}`)
			}))
			t.Cleanup(server.Close)
			bridge.readGraph = &GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 1,
				sleep:      func(context.Context, time.Duration) error { return nil },
				jitter:     func(d time.Duration) time.Duration { return d },
			}

			before, ok, err := store.ChatPoll(ctx, first.ChatID)
			if err != nil || !ok {
				t.Fatalf("read first poll before failure: ok=%v err=%v", ok, err)
			}
			if _, err := bridge.pollChatWithRole(ctx, first.ChatID, 20, inboundPollRoleWork, false, func(context.Context, ChatMessage, string) error {
				return nil
			}); err == nil {
				t.Fatalf("continuation failure status %d returned nil", testCase.status)
			}
			after, ok, err := store.ChatPoll(ctx, first.ChatID)
			if err != nil || !ok {
				t.Fatalf("read first poll after failure: ok=%v err=%v", ok, err)
			}
			if !after.LastModifiedCursor.Equal(before.LastModifiedCursor) || after.ContinuationPath != oldContinuation {
				teamsOwnershipStressFinding(t, "continuation failure changed durable progress: status=%d before=%#v after=%#v", testCase.status, before, after)
			}
			if _, err := bridge.pollChatWithRole(ctx, second.ChatID, 20, inboundPollRoleWork, false, func(context.Context, ChatMessage, string) error {
				return nil
			}); err != nil {
				t.Fatalf("unrelated chat poll after continuation failure: %v", err)
			}
			mu.Lock()
			otherRequests := requests[second.ChatID]
			continuations := continuationRequests
			mu.Unlock()
			if otherRequests == 0 || continuations == 0 {
				teamsOwnershipStressFinding(t, "continuation failure did not isolate chat: status=%d continuation_requests=%d other_chat_requests=%d", testCase.status, continuations, otherRequests)
			}
		})
	}
}

// TestTeamsOwnershipStressContinuationFailureIsIsolatedByPollOnceCI is the
// service-level version of the preceding check.  A direct second call can
// hide a scheduler bug; this drives the real due-chat loop and requires the
// second chat to reach Graph even though the first chat returns a continuation
// error.
func TestTeamsOwnershipStressContinuationFailureIsIsolatedByPollOnceCI(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		status int
	}{
		{name: "rate-limit", status: http.StatusTooManyRequests},
		{name: "server-error", status: http.StatusServiceUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			store := newBridgeTestStore(t)
			executor := &recordingExecutor{result: ExecutionResult{
				Text:          "continuation isolation answer",
				CodexThreadID: "thread-chat-2",
				CodexTurnID:   "turn-chat-2",
			}}
			bridge := newBridgeTestBridge(newOwnershipStressWriteGraph(t), store, executor)
			seedOwnershipStressControlIdle(t, store)
			first := bridge.reg.SessionByID("s001")
			if err := bridge.ensureDurableSession(ctx, first); err != nil {
				t.Fatalf("ensure first durable session: %v", err)
			}
			second := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
			seedThreadRecoverySession(t, store, first, "thread-chat-1", "")
			seedThreadRecoverySession(t, store, second, "thread-chat-2", "")
			seedOwnershipStressDuePoll(t, store, first.ChatID)
			seedOwnershipStressDuePoll(t, store, "chat-2")
			oldContinuation := "/chats/chat-1/messages?$skiptoken=poll-once-failing-page"
			if _, err := store.RecordChatPollSuccessWithContinuation(ctx, first.ChatID, time.Now().Add(-time.Minute), true, true, 20, oldContinuation); err != nil {
				t.Fatalf("seed first continuation: %v", err)
			}
			if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
				ChatID:         first.ChatID,
				PollState:      inboundPollStateWarm,
				NextPollAt:     time.Now().Add(-time.Minute),
				LastActivityAt: time.Now().Add(-time.Minute),
			}); err != nil {
				t.Fatalf("seed first continuation schedule: %v", err)
			}

			var mu sync.Mutex
			requests := map[string]int{}
			continuationRequests := 0
			otherMessage := bridgeTestMessageWithText("chat-2-after-continuation-error", "OWNERSHIP_STRESS_OTHER_CHAT_CONTINUES")
			otherMessage.ChatID = second.ChatID
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
					http.Error(w, "unexpected request", http.StatusNotFound)
					return
				}
				parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				chatID := parts[1]
				mu.Lock()
				requests[chatID]++
				isContinuation := chatID == first.ChatID && r.URL.Query().Get("$skiptoken") == "poll-once-failing-page"
				if isContinuation {
					continuationRequests++
				}
				mu.Unlock()
				if isContinuation {
					http.Error(w, `{"error":{"code":"temporary"}}`, testCase.status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if chatID == second.ChatID {
					_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{otherMessage}})
					return
				}
				_, _ = fmt.Fprint(w, `{"value":[]}`)
			}))
			t.Cleanup(server.Close)
			bridge.readGraph = &GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 1,
				sleep:      func(context.Context, time.Duration) error { return nil },
				jitter:     func(d time.Duration) time.Duration { return d },
			}

			err := bridge.pollOnce(ctx, 20)
			if err != nil {
				t.Fatalf("pollOnce leaked continuation status %d to the listener: %v", testCase.status, err)
			}
			mu.Lock()
			otherRequests := requests["chat-2"]
			continuations := continuationRequests
			mu.Unlock()
			if otherRequests == 0 || continuations == 0 {
				teamsOwnershipStressFinding(t, "pollOnce did not isolate continuation failure: status=%d continuation_requests=%d other_chat_requests=%d err=%v", testCase.status, continuations, otherRequests, err)
			}
			state, stateErr := store.Load(ctx)
			if stateErr != nil {
				t.Fatalf("load state after isolated continuation failure: %v", stateErr)
			}
			if got := ownershipStressInboundCount(state, second.ID, otherMessage.ID); got != 1 {
				teamsOwnershipStressFinding(t, "unrelated chat did not persist its inbound exactly once: status=%d inbound=%d prompts=%v", testCase.status, got, executor.prompts)
			}
			if got := ownershipStressCompletedTurnCount(state, second.ID); got != 1 {
				teamsOwnershipStressFinding(t, "unrelated chat did not complete its turn: status=%d completed=%d prompts=%v", testCase.status, got, executor.prompts)
			}
			poll, ok, pollErr := store.ChatPoll(ctx, first.ChatID)
			if pollErr != nil || !ok {
				t.Fatalf("read first poll after pollOnce: ok=%v err=%v", ok, pollErr)
			}
			if poll.ContinuationPath != oldContinuation || poll.LastModifiedCursor.IsZero() {
				teamsOwnershipStressFinding(t, "pollOnce continuation failure changed durable progress: status=%d poll=%#v", testCase.status, poll)
			}
		})
	}
}

// TestTeamsOwnershipStressTranscriptCatchupWhileTUIContinuesCI covers the
// outage-recovery shape where the helper has started importing local TUI
// output, but the same Codex transcript keeps receiving new completed turns.
// The first Graph send is held open so the second batch is appended while the
// import is genuinely in flight; a later bounded sync must deliver both
// batches exactly once.
func TestTeamsOwnershipStressTranscriptCatchupWhileTUIContinuesCI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), ownershipStressTestTimeout(5*time.Second))
	defer cancel()
	transcriptPath := filepathForOwnershipStress(t)
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-tui-live-catchup"}}`,
		`{"id":"tui-old","thread_id":"thread-tui-live-catchup","role":"assistant","text":"old TUI answer"}`,
		"",
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial TUI transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-tui-live-catchup", transcriptPath)
	defer restoreDiscover()

	initialGraph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(initialGraph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-tui-live-catchup")

	firstBatch := []string{"OWNERSHIP_STRESS_TUI_LIVE_000", "OWNERSHIP_STRESS_TUI_LIVE_001"}
	for i, marker := range firstBatch {
		appendOwnershipStressTranscriptLine(t, transcriptPath, fmt.Sprintf(`{"id":"live-first-%03d","thread_id":"thread-tui-live-catchup","role":"assistant","text":%q}`, i, marker))
	}
	secondBatch := make([]string, 0, 24)
	for i := 0; i < 24; i++ {
		marker := fmt.Sprintf("OWNERSHIP_STRESS_TUI_LIVE_%03d", i+2)
		secondBatch = append(secondBatch, marker)
		appendOwnershipStressTranscriptLine(t, transcriptPath, fmt.Sprintf(`{"id":"live-second-%03d","thread_id":"thread-tui-live-catchup","role":"assistant","text":%q}`, i, marker))
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var sendMu sync.Mutex
	var sent []bridgeSentMessage
	var enteredOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/"+session.ChatID+"/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		sendMu.Lock()
		sent = append(sent, bridgeSentMessage{ChatID: session.ChatID, Content: body.Body.Content})
		first := len(sent) == 1
		sendMu.Unlock()
		if first {
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"ownership-stress-live-sent","messageType":"message"}`)
	}))
	t.Cleanup(server.Close)
	bridge.graph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	syncDone := make(chan error, 1)
	go func() { syncDone <- bridge.syncLinkedTranscripts(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("transcript catchup did not reach the first Graph send")
	}
	// The second batch is appended while the first import is blocked in the
	// outbound path, which is the timing a service outage/recovery can create.
	for i := 0; i < 8; i++ {
		marker := fmt.Sprintf("OWNERSHIP_STRESS_TUI_LIVE_%03d", i+26)
		secondBatch = append(secondBatch, marker)
		appendOwnershipStressTranscriptLine(t, transcriptPath, fmt.Sprintf(`{"id":"live-third-%03d","thread_id":"thread-tui-live-catchup","role":"assistant","text":%q}`, i, marker))
	}
	close(release)
	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("live transcript catchup sync: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("live transcript catchup sync did not finish")
	}

	for attempt := 0; attempt < 8; attempt++ {
		sendMu.Lock()
		joined := sentPlainJoined(sent)
		sendMu.Unlock()
		allPresent := true
		for _, marker := range append(firstBatch, secondBatch...) {
			if !strings.Contains(joined, marker) {
				allPresent = false
				break
			}
		}
		if allPresent {
			break
		}
		if err := bridge.syncLinkedTranscripts(ctx); err != nil {
			t.Fatalf("resume live transcript catchup attempt %d: %v", attempt+1, err)
		}
	}
	sendMu.Lock()
	joined := sentPlainJoined(sent)
	sendMu.Unlock()
	var missing, duplicates []string
	for _, marker := range append(firstBatch, secondBatch...) {
		switch strings.Count(joined, marker) {
		case 0:
			missing = append(missing, marker)
		case 1:
		default:
			duplicates = append(duplicates, marker)
		}
	}
	if len(missing) != 0 || len(duplicates) != 0 {
		teamsOwnershipStressFinding(t, "live TUI transcript growth catchup mismatch: missing=%v duplicates=%v", missing, duplicates)
	}
}

// TestTeamsOwnershipStressTranscriptSyncDoesNotStartTeamsPromptBeforeCatchupCI
// covers the opposite direction of the same race: while local TUI history is
// being imported, a new Teams prompt arrives for that thread.  The prompt may
// be durably deferred, but it must not start a Codex writer until the import
// has finished.
func TestTeamsOwnershipStressTranscriptSyncDoesNotStartTeamsPromptBeforeCatchupCI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), ownershipStressTestTimeout(5*time.Second))
	defer cancel()
	transcriptPath := filepathForOwnershipStress(t)
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-tui-prompt-race"}}`,
		`{"id":"tui-old","thread_id":"thread-tui-prompt-race","role":"assistant","text":"old TUI answer"}`,
		"",
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial prompt-race transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-tui-prompt-race", transcriptPath)
	defer restoreDiscover()

	initialGraph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &teamsOwnershipStressExecutor{}
	bridge := newBridgeTestBridge(initialGraph, store, executor)
	bridge.asyncTurns = true
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-tui-prompt-race")
	appendOwnershipStressTranscriptLine(t, transcriptPath, `{"id":"tui-in-flight","thread_id":"thread-tui-prompt-race","role":"assistant","text":"TUI answer while Teams prompt arrives"}`)

	entered := make(chan struct{})
	release := make(chan struct{})
	var postMu sync.Mutex
	posts := 0
	var enteredOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/"+session.ChatID+"/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		postMu.Lock()
		posts++
		postNumber := posts
		first := postNumber == 1
		postMu.Unlock()
		if first {
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"prompt-race-sent-%d","messageType":"message"}`, postNumber)
	}))
	t.Cleanup(server.Close)
	bridge.graph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	syncDone := make(chan error, 1)
	go func() { syncDone <- bridge.syncLinkedTranscripts(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("prompt-race transcript sync did not reach Graph send")
	}
	prompt := bridgeTestMessageWithText("teams-during-tui-sync", "OWNERSHIP_STRESS_TEAMS_PROMPT_DURING_TUI_SYNC")
	if err := bridge.handleSessionMessage(ctx, session.ChatID, prompt, "OWNERSHIP_STRESS_TEAMS_PROMPT_DURING_TUI_SYNC"); err != nil {
		t.Fatalf("Teams prompt during transcript sync: %v", err)
	}
	if calls := executor.callCount(); calls != 0 {
		teamsOwnershipStressFinding(t, "Teams prompt started Codex while transcript import was still blocked: executor_calls=%d", calls)
	}
	close(release)
	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("prompt-race transcript sync: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("prompt-race transcript sync did not finish")
	}
	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("process deferred Teams prompt after transcript sync: %v", err)
	}
	waitForBridgeAsyncTurns(t, bridge)
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load prompt-race state: %v", err)
	}
	if ownershipStressCompletedTurnCount(state, session.ID) != 1 {
		teamsOwnershipStressFinding(t, "deferred Teams prompt did not complete after transcript import: turns=%#v", state.Turns)
	}
}

// TestTeamsOwnershipStressHistoryBaselinePartialTailBoundaryCI models a
// startup baseline while Codex is in the middle of appending one JSONL record.
// A startup baseline must not treat the incomplete suffix as already consumed:
// the next append can complete that record without adding a new line prefix.
func TestTeamsOwnershipStressHistoryBaselinePartialTailBoundaryCI(t *testing.T) {
	ctx := context.Background()
	path := filepathForOwnershipStress(t)
	completePrefix := `{"type":"session_meta","payload":{"id":"thread-partial-baseline"}}` + "\n"
	partial := `{"type":"event_msg","payload":{"id":"partial-final","type":"agent_message","phase":"final_answer","message":"restart boundary`
	if err := os.WriteFile(path, []byte(completePrefix+partial), 0o600); err != nil {
		t.Fatalf("write partial transcript: %v", err)
	}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &teamsOwnershipStressExecutor{})
	if err := bridge.baselineCodexHistoryWatch(ctx, []string{path}, time.Now()); err != nil {
		t.Fatalf("baseline history watch: %v", err)
	}
	state, err := store.HistoryWatchState(ctx)
	if err != nil {
		t.Fatalf("read history watch baseline: %v", err)
	}
	checkpoint := state.HistoryWatch[historyWatchCheckpointID(path)]
	if checkpoint.Offset != int64(len(completePrefix)) || checkpoint.PartialLineStartOffset != checkpoint.Offset || checkpoint.PartialReadOffset != int64(len(completePrefix)+len(partial)) {
		t.Fatalf("baseline partial boundary=%#v, want offset=%d partial_read=%d", checkpoint, len(completePrefix), len(completePrefix)+len(partial))
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open partial transcript for completion: %v", err)
	}
	if _, err := file.WriteString(`"}}` + "\n"); err != nil {
		_ = file.Close()
		t.Fatalf("complete partial transcript: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close completed partial transcript: %v", err)
	}

	result, err := historyTieredScanTail(path, historyTieredFileStateFromHistoryWatch(checkpoint), historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("scan transcript after restart append: %v", err)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "restart boundary" {
		teamsOwnershipStressFinding(t, "history baseline skipped a partial JSONL record after startup baseline: finals=%#v records=%#v state=%#v", result.Finals, result.Records, result.State)
	}
}

// TestTeamsOwnershipStressHistoryCompleteEOFWithoutNewlineCI covers a process
// ending immediately after writing a complete JSON object but before the
// record delimiter.  A complete record at EOF is still recoverable and must
// not be treated as an unbounded partial tail forever.
func TestTeamsOwnershipStressHistoryCompleteEOFWithoutNewlineCI(t *testing.T) {
	path := filepathForOwnershipStress(t)
	body := `{"type":"session_meta","payload":{"id":"thread-eof-no-newline"}}` + "\n" +
		`{"type":"event_msg","payload":{"id":"eof-final","type":"agent_message","phase":"final_answer","message":"complete at EOF"}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write complete EOF transcript: %v", err)
	}
	result, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("scan complete EOF transcript: %v", err)
	}
	if len(result.Finals) != 1 || result.Finals[0].Record.Text != "complete at EOF" {
		teamsOwnershipStressFinding(t, "complete JSON at EOF without newline was not recovered: incomplete=%v finals=%#v records=%#v", result.Incomplete, result.Finals, result.Records)
	}
}

// TestTeamsOwnershipStressHistoryRewriteAtSameSizeCI models a repaired or
// compacted transcript generation that replaces its contents without changing
// the file length.  Size-only checkpoint identity cannot safely continue from
// the old EOF in this case.
func TestTeamsOwnershipStressHistoryRewriteAtSameSizeCI(t *testing.T) {
	path := filepathForOwnershipStress(t)
	oldBody := `{"type":"session_meta","payload":{"id":"thread-rewrite"}}` + "\n" +
		`{"type":"event_msg","payload":{"id":"old-final","type":"agent_message","phase":"final_answer","message":"OLD_CONTENT"}}` + "\n"
	newBody := strings.Replace(oldBody, "OLD_CONTENT", "NEW_CONTENT", 1)
	if len(oldBody) != len(newBody) {
		t.Fatalf("rewrite fixture sizes differ: old=%d new=%d", len(oldBody), len(newBody))
	}
	if err := os.WriteFile(path, []byte(oldBody), 0o600); err != nil {
		t.Fatalf("write old rewrite generation: %v", err)
	}
	initial, err := historyTieredScanTail(path, historyTieredFileState{}, historyTieredMaxTailBytes)
	if err != nil || len(initial.Finals) != 1 {
		t.Fatalf("scan old rewrite generation: err=%v finals=%#v", err, initial.Finals)
	}
	if err := os.WriteFile(path, []byte(newBody), 0o600); err != nil {
		t.Fatalf("write same-size rewrite generation: %v", err)
	}
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatalf("set rewrite generation mtime: %v", err)
	}
	rewritten, err := historyTieredScanTail(path, initial.State, historyTieredMaxTailBytes)
	if err != nil {
		t.Fatalf("scan same-size rewrite generation: %v", err)
	}
	if !rewritten.Truncated || len(rewritten.Finals) != 0 || !rewritten.State.SourceRewriteBlocked {
		teamsOwnershipStressFinding(t, "same-size transcript rewrite was not fenced before cursor reuse: truncated=%v finals=%#v previous=%#v rewritten=%#v", rewritten.Truncated, rewritten.Finals, initial.State, rewritten.State)
	}
}

// TestTeamsOwnershipStressPersistedBlockedCheckpointReopensAndResumesRootCI
// models the upgrade/restart shape that the live blocked rows have: a
// history checkpoint is already persisted as blocked, the helper is replaced,
// and the transcript then contains an explicit new user prompt and root turn.
// The recovery must use the durable checkpoint, deliver the new final once,
// and remain silent about the old history gate.
func TestTeamsOwnershipStressPersistedBlockedCheckpointReopensAndResumesRootCI(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "session.jsonl")
	initial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-persisted-blocked"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","turn_id":"old-turn","phase":"final_answer","message":"old completed answer"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-persisted-blocked", path)
	defer restoreDiscover()

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedLinkedTranscriptForTest(t, bridge, path, "thread-persisted-blocked")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load seeded state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if err := bridge.markTranscriptImportBlocked(ctx, *session, path, checkpoint); err != nil {
		t.Fatalf("persist blocked checkpoint: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate blocked checkpoint to SQLite: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close pre-restart store: %v", err)
	}

	restartedStore, err := teamstore.Open(store.Path())
	if err != nil {
		t.Fatalf("reopen SQLite store: %v", err)
	}
	defer restartedStore.Close()
	restarted := newBridgeTestBridge(graph, restartedStore, &recordingExecutor{})
	if resumedSession := restarted.reg.SessionByID(session.ID); resumedSession != nil {
		resumedSession.CodexThreadID = "thread-persisted-blocked"
	} else {
		t.Fatalf("restarted registry missing session %s", session.ID)
	}
	appendOwnershipStressTranscriptLine(t, path, `{"type":"response_item","thread_id":"thread-persisted-blocked","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"new explicit Teams request"}]}}`)
	appendOwnershipStressTranscriptLine(t, path, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"new-root"}}`)
	appendOwnershipStressTranscriptLine(t, path, `{"type":"event_msg","payload":{"type":"agent_message","turn_id":"new-root","phase":"final_answer","message":"new root answer after restart"}}`)
	*sent = nil

	if err := restarted.syncLinkedTranscripts(ctx); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sync reopened blocked checkpoint: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	joined := sentPlainJoined(*sent)
	if strings.Count(joined, "new root answer after restart") != 1 {
		t.Fatalf("reopened blocked checkpoint delivered new root %d times: %#v", strings.Count(joined, "new root answer after restart"), *sent)
	}
	for _, forbidden := range []string{"previous Codex execution is still unconfirmed", "helper publish-history"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("reopened blocked checkpoint emitted stale history gate %q: %#v", forbidden, *sent)
		}
	}
	state, err = restartedStore.Load(ctx)
	if err != nil {
		t.Fatalf("load reopened final state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.Status != importCheckpointStatusComplete || checkpoint.UnresolvedExecution != nil || checkpoint.LastOffset != checkpoint.SourceSize {
		t.Fatalf("reopened checkpoint = %#v, want complete at EOF without execution fence", checkpoint)
	}

	*sent = nil
	if err := restarted.syncLinkedTranscripts(ctx); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("repeat reopened checkpoint sync: %v", err)
	}
	flushBridgeQueuedNotificationsForTest(t, restarted)
	if strings.Contains(sentPlainJoined(*sent), "new root answer after restart") {
		t.Fatalf("reopened root answer was replayed on the next scan: %#v", *sent)
	}
}

// TestTeamsOwnershipStressAcceptedOutboxFallsOutsideRecoveryHeadCI models a
// busy chat where the Teams POST succeeded, local persistence failed, and the
// helper restarted after more than the recovery page size of newer messages.
// The accepted echo is exposed only through the continuation page.  A safe
// recovery path must not blindly POST a duplicate while it has not exhausted
// that evidence source.
func TestTeamsOwnershipStressAcceptedOutboxFallsOutsideRecoveryHeadCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:ownership-stress-accepted-old",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "accepted response outside recovery head",
	})
	if err != nil {
		t.Fatalf("queue accepted outbox: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, queued.ID); err != nil {
		t.Fatalf("mark accepted outbox send attempt: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load accepted outbox state: %v", err)
	}
	retryOutbox := state.OutboxMessages[queued.ID]
	var lists, posts int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			lists++
			if r.URL.Query().Get("$skiptoken") == "accepted" {
				echo := ownershipStressAcceptedOutboxMessage("accepted-old-echo", "accepted response outside recovery head")
				echo.Body.Content = renderOutboxHTML(queued) + helperOutboxProvenanceMarker(queued.ID)
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{echo}})
				return
			}
			newer := make([]ChatMessage, 0, outboxRecoveryMessageTop)
			for i := 0; i < outboxRecoveryMessageTop; i++ {
				newer = append(newer, ownershipStressAcceptedOutboxMessage(fmt.Sprintf("newer-%02d", i), fmt.Sprintf("newer chat message %02d", i)))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value":           newer,
				"@odata.nextLink": server.URL + "/chats/chat-1/messages?$skiptoken=accepted",
			})
		case http.MethodPost:
			posts++
			_, _ = fmt.Fprint(w, `{"id":"duplicate-post","messageType":"message"}`)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	if err := bridge.sendQueuedOutbox(ctx, retryOutbox); err != nil {
		t.Fatalf("accepted outbox recovery: %v lists=%d posts=%d", err, lists, posts)
	}
	if lists == 0 {
		t.Fatal("accepted outbox recovery did not inspect Graph")
	}
	if posts != 0 {
		teamsOwnershipStressFinding(t, "accepted outbox outside recovery head caused duplicate POST: lists=%d posts=%d", lists, posts)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load accepted outbox result: %v", err)
	}
	recovered := state.OutboxMessages[queued.ID]
	if recovered.Status != teamstore.OutboxStatusSent || recovered.TeamsMessageID != "accepted-old-echo" {
		teamsOwnershipStressFinding(t, "accepted outbox outside recovery head did not reconcile continuation evidence: lists=%d posts=%d outbox=%#v", lists, posts, recovered)
	}
}

// TestTeamsOwnershipStressExpiredAmbiguousOutboxDoesNotPostBeforeContinuationCI
// closes the lease-expiry gap in the preceding test.  A fresh send lease is a
// valid duplicate-prevention fence, but once it expires the recovery reader
// must exhaust the continuation evidence before claiming a new POST attempt.
func TestTeamsOwnershipStressExpiredAmbiguousOutboxDoesNotPostBeforeContinuationCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:ownership-stress-expired-ambiguous",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "accepted after expired lease",
	})
	if err != nil {
		t.Fatalf("queue expired ambiguous outbox: %v", err)
	}
	attempt, err := store.MarkOutboxSendAttempt(ctx, queued.ID)
	if err != nil {
		t.Fatalf("mark expired ambiguous send attempt: %v", err)
	}
	if _, err := store.MarkOutboxAmbiguousSendErrorForAttempt(ctx, queued.ID, attempt.SendAttemptToken, "accepted by Graph before response was lost"); err != nil {
		t.Fatalf("mark expired ambiguous send: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		msg := state.OutboxMessages[queued.ID]
		msg.LastSendAttempt = time.Now().Add(-3 * time.Minute)
		state.OutboxMessages[queued.ID] = msg
		return nil
	}); err != nil {
		t.Fatalf("age expired ambiguous send lease: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load aged ambiguous outbox: %v", err)
	}
	retryOutbox := state.OutboxMessages[queued.ID]

	accepted := ownershipStressAcceptedOutboxMessage("accepted-after-expiry", "accepted after expired lease")
	accepted.Body.Content = renderOutboxHTML(queued) + helperOutboxProvenanceMarker(queued.ID)
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("$skiptoken") == "accepted-after-expiry" {
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{accepted}})
				return
			}
			newer := make([]ChatMessage, 0, outboxRecoveryMessageTop)
			for i := 0; i < outboxRecoveryMessageTop; i++ {
				newer = append(newer, ownershipStressAcceptedOutboxMessage(fmt.Sprintf("newer-expiry-%02d", i), fmt.Sprintf("newer message %02d", i)))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value":           newer,
				"@odata.nextLink": "/chats/chat-1/messages?$skiptoken=accepted-after-expiry",
			})
		case http.MethodPost:
			posts++
			_, _ = fmt.Fprint(w, `{"id":"duplicate-after-expiry","messageType":"message"}`)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	if err := bridge.sendQueuedOutbox(ctx, retryOutbox); err != nil {
		state, _ := store.Load(ctx)
		t.Fatalf("expired ambiguous outbox recovery: %v posts=%d outbox=%#v", err, posts, state.OutboxMessages[queued.ID])
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load expired ambiguous outbox result: %v", err)
	}
	recovered := state.OutboxMessages[queued.ID]
	if posts != 0 || recovered.Status != teamstore.OutboxStatusSent || recovered.TeamsMessageID != accepted.ID {
		teamsOwnershipStressFinding(t, "expired ambiguous outbox posted before exhausting continuation evidence: posts=%d outbox=%#v", posts, recovered)
	}
}

// TestTeamsOwnershipStressRecoveryPageBudgetPreservesContinuationCI proves
// that a bounded recovery pass counts its head page, then persists the next
// page when the pass ends.  The following pass must resume from that durable
// path and reconcile an accepted message without a duplicate POST.
func TestTeamsOwnershipStressRecoveryPageBudgetPreservesContinuationCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:ownership-stress-recovery-budget",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "accepted while recovery page budget was exhausted",
	})
	if err != nil {
		t.Fatalf("queue recovery-budget outbox: %v", err)
	}
	attempt, err := store.MarkOutboxSendAttempt(ctx, queued.ID)
	if err != nil {
		t.Fatalf("mark recovery-budget send attempt: %v", err)
	}
	if _, err := store.MarkOutboxAmbiguousSendErrorForAttempt(ctx, queued.ID, attempt.SendAttemptToken, "Graph accepted before the response was lost"); err != nil {
		t.Fatalf("mark recovery-budget ambiguous send: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load recovery-budget outbox: %v", err)
	}
	retryOutbox := state.OutboxMessages[queued.ID]
	accepted := ownershipStressAcceptedOutboxMessage("accepted-recovery-budget", "accepted while recovery page budget was exhausted")
	accepted.Body.Content = renderOutboxHTML(queued) + helperOutboxProvenanceMarker(queued.ID)
	var lists, posts int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			lists++
			if r.URL.Query().Get("$skiptoken") == "accepted-recovery-budget" {
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{accepted}})
				return
			}
			newer := make([]ChatMessage, 0, outboxRecoveryMessageTop)
			for i := 0; i < outboxRecoveryMessageTop; i++ {
				newer = append(newer, ownershipStressAcceptedOutboxMessage(fmt.Sprintf("recovery-budget-%02d", i), fmt.Sprintf("newer recovery message %02d", i)))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value":           newer,
				"@odata.nextLink": server.URL + "/chats/chat-1/messages?$skiptoken=accepted-recovery-budget",
			})
		case http.MethodPost:
			posts++
			_, _ = fmt.Fprint(w, `{"id":"duplicate-recovery-budget","messageType":"message"}`)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	pageBudget := 1
	handled, err := bridge.recoverAcceptedOutboxFromGraph(ctx, retryOutbox, outboxSendOptions{RecoveryPageBudget: &pageBudget})
	if !handled || err == nil {
		t.Fatalf("bounded recovery pass = handled %v err %v, want deferred result", handled, err)
	}
	if pageBudget != 0 {
		t.Fatalf("recovery page budget after head page = %d, want 0", pageBudget)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load recovery-budget continuation: %v", err)
	}
	checkpointed := state.OutboxMessages[queued.ID]
	if !strings.Contains(checkpointed.GraphRecoveryNextPath, "accepted-recovery-budget") {
		t.Fatalf("recovery continuation was not persisted after budget exhaustion: %#v", checkpointed)
	}

	pageBudget = 1
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("reload recovery-budget continuation: %v", err)
	}
	checkpointed = state.OutboxMessages[queued.ID]
	handled, err = bridge.recoverAcceptedOutboxFromGraph(ctx, checkpointed, outboxSendOptions{RecoveryPageBudget: &pageBudget})
	if !handled || err != nil {
		t.Fatalf("continuation recovery = handled %v err %v, want successful reconciliation", handled, err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load recovered recovery-budget outbox: %v", err)
	}
	recovered := state.OutboxMessages[queued.ID]
	if lists != 2 || posts != 0 || recovered.Status != teamstore.OutboxStatusSent || recovered.TeamsMessageID != accepted.ID {
		teamsOwnershipStressFinding(t, "bounded recovery did not resume and reconcile safely: lists=%d posts=%d outbox=%#v", lists, posts, recovered)
	}
}

// TestTeamsOwnershipStressExpiredOutboxContinuationFallsBackToHeadCI models a
// listener that was offline long enough for its persisted Graph skiptoken to
// expire. The recovery lane must discard only that transport cursor, probe the
// safe head for the exact provenance marker, and settle the row without a new
// POST.
func TestTeamsOwnershipStressExpiredOutboxContinuationFallsBackToHeadCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:ownership-stress-expired-continuation",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "accepted before the continuation expired",
	})
	if err != nil {
		t.Fatalf("queue expired-continuation outbox: %v", err)
	}
	claimed, err := store.MarkOutboxSendAttempt(ctx, queued.ID)
	if err != nil {
		t.Fatalf("claim expired-continuation outbox: %v", err)
	}
	if _, err := store.MarkOutboxAmbiguousSendErrorForAttempt(ctx, claimed.ID, claimed.SendAttemptToken, "Graph continuation was persisted before restart"); err != nil {
		t.Fatalf("mark expired-continuation outbox ambiguous: %v", err)
	}
	if _, err := store.MarkOutboxGraphRecoveryProgressForAttempt(ctx, claimed.ID, claimed.SendAttemptToken, "/chats/chat-1/messages?$skiptoken=expired", ""); err != nil {
		t.Fatalf("persist expired continuation: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load expired-continuation state: %v", err)
	}
	retryOutbox := state.OutboxMessages[queued.ID]
	accepted := ownershipStressAcceptedOutboxMessage("accepted-after-expired-continuation", "accepted before the continuation expired")
	accepted.Body.Content = renderOutboxHTML(queued) + helperOutboxProvenanceMarker(queued.ID)
	var heads, continuations, posts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			posts++
			_, _ = fmt.Fprint(w, `{"id":"duplicate-after-expired-continuation","messageType":"message"}`)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Query().Get("$skiptoken") {
		case "expired":
			continuations++
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"InvalidSkipToken","message":"continuation expired"}}`)
		case "":
			heads++
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{accepted}})
		default:
			http.Error(w, "unexpected continuation", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	pageBudget := 4
	handled, err := bridge.recoverAcceptedOutboxFromGraph(ctx, retryOutbox, outboxSendOptions{RecoveryPageBudget: &pageBudget})
	if !handled || err != nil {
		t.Fatalf("expired-continuation recovery = handled %v err %v, want head reconciliation", handled, err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load recovered expired-continuation state: %v", err)
	}
	recovered := state.OutboxMessages[queued.ID]
	if continuations != 1 || heads != 1 || posts != 0 || recovered.Status != teamstore.OutboxStatusSent || recovered.TeamsMessageID != accepted.ID || recovered.GraphRecoveryNextPath != "" || recovered.GraphRecoveryPageCount != 0 {
		teamsOwnershipStressFinding(t, "expired continuation did not fall back to exact head reconciliation: continuations=%d heads=%d posts=%d outbox=%#v", continuations, heads, posts, recovered)
	}
}

// TestTeamsOwnershipStressGraphAcceptedThen502RecoversWithoutDuplicateCI
// models a common HTTP ambiguity: Teams accepted the POST, but the client saw
// a 502 before it received the response body.  A fresh Bridge must search the
// head and its continuation before issuing another POST.
func TestTeamsOwnershipStressGraphAcceptedThen502RecoversWithoutDuplicateCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:ownership-stress-accepted-502",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "accepted before client saw 502",
	})
	if err != nil {
		t.Fatalf("queue ambiguous outbox: %v", err)
	}

	var server *httptest.Server
	var mu sync.Mutex
	postCount := 0
	headLists := 0
	continuationLists := 0
	accepted := ChatMessage{}
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			mu.Lock()
			postCount++
			postNumber := postCount
			mu.Unlock()
			if postNumber == 1 {
				accepted = bridgeTestMessageWithText("accepted-after-502", body.Body.Content)
				accepted.CreatedDateTime = time.Now().UTC().Format(time.RFC3339Nano)
				accepted.LastModifiedDateTime = accepted.CreatedDateTime
				http.Error(w, `{"error":{"code":"BadGateway"}}`, http.StatusBadGateway)
				return
			}
			_, _ = fmt.Fprintf(w, `{"id":"duplicate-post-%d","messageType":"message"}`, postNumber)
		case http.MethodGet:
			if r.URL.Query().Get("$skiptoken") == "accepted-after-502" {
				mu.Lock()
				continuationLists++
				mu.Unlock()
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{accepted}})
				return
			}
			mu.Lock()
			headLists++
			mu.Unlock()
			newer := make([]ChatMessage, 0, outboxRecoveryMessageTop)
			for i := 0; i < outboxRecoveryMessageTop; i++ {
				newer = append(newer, ownershipStressAcceptedOutboxMessage(fmt.Sprintf("newer-after-502-%02d", i), fmt.Sprintf("newer message %02d", i)))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value":           newer,
				"@odata.nextLink": server.URL + "/chats/chat-1/messages?$skiptoken=accepted-after-502",
			})
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
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
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	if err := bridge.sendQueuedOutbox(ctx, queued); err == nil {
		t.Fatal("ambiguous Graph 502 unexpectedly returned success")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load ambiguous outbox: %v", err)
	}
	retryOutbox := state.OutboxMessages[queued.ID]
	restarted := newBridgeTestBridge(graph, store, &recordingExecutor{})
	if err := restarted.sendQueuedOutbox(ctx, retryOutbox); err != nil {
		t.Fatalf("recover ambiguous outbox after restart: %v", err)
	}
	mu.Lock()
	posts, heads, continuations := postCount, headLists, continuationLists
	mu.Unlock()
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load recovered ambiguous outbox: %v", err)
	}
	recovered := state.OutboxMessages[queued.ID]
	if posts != 1 || heads == 0 || continuations == 0 || recovered.Status != teamstore.OutboxStatusSent || recovered.TeamsMessageID != "accepted-after-502" {
		teamsOwnershipStressFinding(t, "accepted Graph POST was not recovered exactly once: posts=%d head_lists=%d continuation_lists=%d outbox=%#v", posts, heads, continuations, recovered)
	}
}

func ownershipStressAcceptedOutboxMessage(id string, text string) ChatMessage {
	msg := bridgeTestMessageWithText(id, "<p>"+text+"</p>")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	msg.CreatedDateTime = now
	msg.LastModifiedDateTime = now
	return msg
}

// TestTeamsOwnershipStressLongRecoveryContinuationRemainsReachableCI proves
// that the durable per-pass page budget does not become a lifetime reachability
// limit. A busy chat can put the exact helper marker behind more pages than a
// fixed historical threshold; every pass must resume the opaque continuation
// and reconcile the marker without issuing a replacement POST.
func TestTeamsOwnershipStressLongRecoveryContinuationRemainsReachableCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:ownership-stress-long-recovery",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "accepted behind a long Graph continuation",
	})
	if err != nil {
		t.Fatalf("queue long-recovery outbox: %v", err)
	}
	claimed, err := store.MarkOutboxSendAttempt(ctx, queued.ID)
	if err != nil {
		t.Fatalf("claim long-recovery outbox: %v", err)
	}
	if _, err := store.MarkOutboxAmbiguousSendErrorForAttempt(ctx, queued.ID, claimed.SendAttemptToken, "Graph accepted before a long continuation scan"); err != nil {
		t.Fatalf("mark long-recovery outbox ambiguous: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load long-recovery outbox: %v", err)
	}
	accepted := ownershipStressAcceptedOutboxMessage("long-recovery-exact-marker", queued.Body)
	accepted.Body.Content = renderOutboxHTML(queued) + helperOutboxProvenanceMarker(queued.ID)
	const targetPage = 65
	var lists, posts int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			posts++
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "duplicate-long-recovery", "messageType": "message"})
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		lists++
		token := r.URL.Query().Get("$skiptoken")
		page := 0
		if token != "" {
			if _, scanErr := fmt.Sscanf(token, "long-recovery-%d", &page); scanErr != nil {
				http.Error(w, "unexpected continuation", http.StatusBadRequest)
				return
			}
		}
		if page == targetPage {
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{accepted}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value":           []ChatMessage{},
			"@odata.nextLink": fmt.Sprintf("%s/chats/chat-1/messages?$skiptoken=long-recovery-%d", server.URL, page+1),
		})
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:   &fakeGraphAuth{token: "access"},
		client: server.Client(), baseURL: server.URL, maxRetries: 0,
		sleep: sleepContext, jitter: func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	for pass := 0; pass <= targetPage+2; pass++ {
		state, err = store.Load(ctx)
		if err != nil {
			t.Fatalf("load long-recovery pass %d: %v", pass, err)
		}
		current := state.OutboxMessages[queued.ID]
		if current.Status == teamstore.OutboxStatusSent {
			break
		}
		pageBudget := 1
		handled, recoveryErr := bridge.recoverAcceptedOutboxFromGraph(ctx, current, outboxSendOptions{RecoveryPageBudget: &pageBudget})
		if !handled {
			t.Fatalf("long-recovery pass %d was not handled: err=%v row=%#v", pass, recoveryErr, current)
		}
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load final long-recovery state: %v", err)
	}
	recovered := state.OutboxMessages[queued.ID]
	if recovered.Status != teamstore.OutboxStatusSent || recovered.TeamsMessageID != accepted.ID {
		t.Fatalf("long continuation did not reach exact marker: lists=%d posts=%d row=%#v", lists, posts, recovered)
	}
	if posts != 0 {
		t.Fatalf("long continuation issued %d replacement POST(s)", posts)
	}
	if lists < targetPage+1 {
		t.Fatalf("long continuation stopped after %d Graph page(s), want at least %d", lists, targetPage+1)
	}
}

// TestTeamsOwnershipStressPagedBacklogAfterServiceOutageCI simulates a helper
// that was down while a large number of Teams messages accumulated.  Graph's
// current head is intentionally full and exposes an older continuation page;
// every page must eventually be claimed once, without letting the head cursor
// skip the continuation.
func TestTeamsOwnershipStressPagedBacklogAfterServiceOutageCI(t *testing.T) {
	ctx := context.Background()
	scale := loadTeamsOwnershipStressScale()
	if scale.Backlog < 12 {
		scale.Backlog = 12
	}
	graph := newOwnershipStressWriteGraph(t)
	store := newBridgeTestStore(t)
	executor := &teamsOwnershipStressExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-teams-backlog", "")

	messages := ownershipStressMessages(session.ChatID, scale.Backlog)
	firstMessageTime := parseGraphTime(messages[0].CreatedDateTime)
	seedOwnershipStressDuePollAt(t, store, session.ChatID, firstMessageTime.Add(-time.Second))
	readGraph, requests := newOwnershipStressPagedReadGraph(t, messages)
	bridge.readGraph = readGraph
	seedOwnershipStressControlIdle(t, store)
	var handledMessageIDs []string
	var roundDiagnostics []string
	maxRounds := scale.PollRounds
	if maxRounds < scale.Backlog+2 {
		// The production poll deliberately handles at most one continuation
		// action per pass. Give the pressure test enough passes to distinguish
		// bounded progress from a livelock.
		maxRounds = scale.Backlog + 2
	}

	for round := 0; round < maxRounds; round++ {
		if _, err := bridge.pollChatWithRole(ctx, session.ChatID, 20, inboundPollRoleWork, false, func(ctx context.Context, msg ChatMessage, text string) error {
			handledMessageIDs = append(handledMessageIDs, msg.ID)
			return bridge.handleSessionMessage(ctx, session.ChatID, msg, text)
		}); err != nil {
			t.Fatalf("backlog poll round %d: %v", round, err)
		}
		waitForBridgeAsyncTurns(t, bridge)
		drainOwnershipStressTurnQueue(t, bridge, scale.Backlog+4)
		state, err := store.Load(ctx)
		if err != nil {
			t.Fatalf("load backlog state round %d: %v", round, err)
		}
		if poll, ok, pollErr := store.ChatPoll(ctx, session.ChatID); pollErr == nil && ok {
			roundDiagnostics = append(roundDiagnostics, fmt.Sprintf("round=%d completed=%d cursor=%s continuation=%q handled=%v", round, ownershipStressCompletedInboundCount(state, session.ID), poll.LastModifiedCursor.Format(time.RFC3339), poll.ContinuationPath, handledMessageIDs))
		}
		if ownershipStressCompletedInboundCount(state, session.ID) >= scale.Backlog {
			break
		}
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load final backlog state: %v", err)
	}
	completed := ownershipStressCompletedInboundCount(state, session.ID)
	if completed != scale.Backlog {
		poll, _, _ := store.ChatPoll(ctx, session.ChatID)
		teamsOwnershipStressFinding(t, "paged Teams backlog was not drained: completed=%d want=%d poll=%#v requests=%v responses=%v handled=%v rounds=%v", completed, scale.Backlog, poll, requests.snapshot(), requests.responseSnapshot(), handledMessageIDs, roundDiagnostics)
	}
	for _, message := range messages {
		if got := ownershipStressInboundCount(state, session.ID, message.ID); got != 1 {
			teamsOwnershipStressFinding(t, "inbound message %s persisted %d times, want exactly once", message.ID, got)
		}
	}
}

// TestTeamsOwnershipStressMultiDayOutageCrossesExpiredBlockAndAutoParkCI
// models the lifecycle that is easy to miss in a short outage test:
//
//  1. the service is down for several days while an old continuation exists;
//  2. the first recovery read fails and blocks the chat;
//  3. the block expires while the chat is already beyond the auto-park idle
//     threshold;
//  4. a fresh service owner starts and a new user message is waiting.
//
// The expired error state must be recoverable by the normal scheduler and the
// auto-park sweeper.  An old safety state may defer work, but it must not turn
// into a permanent freeze that hides the new message.
func TestTeamsOwnershipStressMultiDayOutageCrossesExpiredBlockAndAutoParkCI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), ownershipStressTestTimeout(5*time.Second))
	defer cancel()
	now := time.Now()
	outageAt := now.Add(-72 * time.Hour)
	oldContinuation := "/chats/chat-1/messages?$skiptoken=multi-day-old-backlog"

	store := newBridgeTestStore(t)
	writeGraph := newOwnershipStressWriteGraph(t)
	phase := 0
	var mu sync.Mutex
	headReads := 0
	newMessage := bridgeTestMessageWithText("after-multi-day-outage", "OWNERSHIP_STRESS_AFTER_MULTI_DAY_OUTAGE")
	newMessage.From.User.ID = "user-2"
	newMessage.From.User.DisplayName = "Other User"
	newMessage.CreatedDateTime = now.Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	newMessage.LastModifiedDateTime = newMessage.CreatedDateTime
	oldMessage := bridgeTestMessageWithText("old-multi-day-backlog", "OWNERSHIP_STRESS_OLD_MULTI_DAY_BACKLOG")
	oldMessage.CreatedDateTime = outageAt.Add(time.Hour).UTC().Format(time.RFC3339Nano)
	oldMessage.LastModifiedDateTime = oldMessage.CreatedDateTime
	readServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		currentPhase := phase
		mu.Unlock()
		if r.URL.Query().Get("$skiptoken") == "multi-day-old-backlog" {
			if currentPhase == 0 {
				http.Error(w, `{"error":{"code":"ServiceUnavailable"}}`, http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{oldMessage}})
			return
		}
		mu.Lock()
		headReads++
		mu.Unlock()
		if currentPhase == 0 || currentPhase >= 2 {
			_, _ = fmt.Fprint(w, `{"value":[]}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value":           []ChatMessage{newMessage},
			"@odata.nextLink": oldContinuation,
		})
	}))
	t.Cleanup(readServer.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     readServer.Client(),
		baseURL:    readServer.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	firstExecutor := &recordingExecutor{}
	first := newBridgeTestBridge(writeGraph, store, firstExecutor)
	seedOwnershipStressControlIdle(t, store)
	session := first.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-multi-day-outage", "")
	if err := store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.Sessions[session.ID]
		current.UpdatedAt = outageAt
		state.Sessions[session.ID] = current
		return nil
	}); err != nil {
		t.Fatalf("age multi-day durable session: %v", err)
	}
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, session.ChatID, outageAt, true, true, 20, oldContinuation); err != nil {
		t.Fatalf("seed multi-day continuation: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateCold,
		NextPollAt:     outageAt,
		LastActivityAt: outageAt,
	}); err != nil {
		t.Fatalf("seed multi-day cold schedule: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate multi-day outage state: %v", err)
	}
	candidates, handled, err := store.IdleWorkChatParkCandidates(ctx, first.reg.ControlChatID, now.Add(-inboundPollParkAfter), 8)
	if err != nil {
		t.Fatalf("inspect multi-day auto-park candidates: %v", err)
	}
	if !handled || len(candidates) != 1 {
		t.Fatalf("multi-day cold chat was not an auto-park candidate: handled=%v candidates=%#v", handled, candidates)
	}

	// The first owner comes back while Graph is still unavailable. The idle
	// sweep deliberately contains this per-chat error so an unrelated idle chat
	// is not suppressed by one stale continuation. This is a retry schedule, not
	// a semantic chat block: the operational frontier remains visible and the
	// listener can continue serving other chats.
	if err := first.maybeRunIdleWorkChatAutoPark(ctx, now); err != nil {
		t.Fatalf("idle sweep leaked a local continuation failure: %v", err)
	}
	blocked, ok, err := store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read blocked multi-day poll: ok=%v err=%v", ok, err)
	}
	if blocked.PollState == inboundPollStateBlocked || blocked.ContinuationPath != oldContinuation || !blocked.BlockedUntil.IsZero() || blocked.LastError == "" || blocked.FailureCount == 0 {
		t.Fatalf("failed recovery did not preserve a recoverable non-blocking continuation: %#v", blocked)
	}

	// Exercise the actual process boundary before the fresh owner takes over.
	// Sharing one Store handle would miss pointer-cache, WAL, and reopen
	// behavior that occurs when the helper really restarts after the outage.
	storePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close first owner store before restart: %v", err)
	}
	reopenedStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store for fresh outage owner: %v", err)
	}
	t.Cleanup(func() {
		if err := reopenedStore.Close(); err != nil {
			t.Errorf("close reopened outage store: %v", err)
		}
	})
	store = reopenedStore

	// The message is already visible in Graph while the block is still active,
	// but a blocked chat must not bypass its durable retry deadline or auto-wake
	// itself.  Probe that state with a fresh owner before advancing the deadline.
	mu.Lock()
	phase = 1
	mu.Unlock()
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:            session.ChatID,
		PollState:         inboundPollStateBlocked,
		PreviousPollState: inboundPollStateCold,
		NextPollAt:        now.Add(time.Hour),
		LastActivityAt:    outageAt,
		BlockedUntil:      now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("keep multi-day blocked deadline active: %v", err)
	}

	secondExecutor := &recordingExecutor{}
	second := newBridgeTestBridge(writeGraph, store, secondExecutor)
	seedOwnershipStressControlIdle(t, store)
	second.readGraph = readGraph
	mu.Lock()
	headReadsBeforeBlockedProbe := headReads
	mu.Unlock()
	if err := second.pollOnce(ctx, 20); err != nil {
		t.Fatalf("blocked chat probe before retry deadline: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load blocked state before retry deadline: %v", err)
	}
	mu.Lock()
	headReadsAfterBlockedProbe := headReads
	mu.Unlock()
	if headReadsAfterBlockedProbe != headReadsBeforeBlockedProbe || len(secondExecutor.prompts) != 0 || ownershipStressInboundCount(state, session.ID, newMessage.ID) != 0 {
		teamsOwnershipStressFinding(t, "blocked chat bypassed its retry deadline: head_reads_before=%d after=%d prompts=%v inbound=%d state=%#v", headReadsBeforeBlockedProbe, headReadsAfterBlockedProbe, secondExecutor.prompts, ownershipStressInboundCount(state, session.ID, newMessage.ID), state)
	}

	// No real three-day sleep: advance only the durable deadline.  This is the
	// same state a fresh process observes after the outage and avoids a flaky
	// wall-clock test.
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:            session.ChatID,
		PollState:         inboundPollStateBlocked,
		PreviousPollState: inboundPollStateCold,
		NextPollAt:        outageAt.Add(time.Minute),
		LastActivityAt:    outageAt,
		BlockedUntil:      outageAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("age multi-day blocked deadline: %v", err)
	}
	if err := second.pollOnce(ctx, 20); err != nil {
		t.Fatalf("fresh owner scheduler handoff after multi-day outage: %v", err)
	}
	// One poll quantum recovers the old continuation. The fresh head is a
	// separate durable page and must be read by the next due quantum; keeping
	// these operations separate is what prevents a stale continuation from
	// monopolizing or overwriting the head frontier.
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after old continuation recovery: %v", err)
	}
	if got := ownershipStressInboundCount(state, session.ID, oldMessage.ID); got != 1 {
		teamsOwnershipStressFinding(t, "old continuation was not recovered after the expired block: inbound=%d state=%#v", got, state)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     time.Now().Add(-time.Minute),
		LastActivityAt: time.Now(),
	}); err != nil {
		t.Fatalf("make fresh head recovery due: %v", err)
	}
	if err := second.pollOnce(ctx, 20); err != nil {
		t.Fatalf("fresh owner head recovery after old continuation: %v", err)
	}
	if err := second.maybeRunIdleWorkChatAutoPark(ctx, now); err != nil {
		t.Fatalf("fresh owner auto-park recovery after multi-day outage: %v", err)
	}

	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load multi-day recovery state: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read multi-day recovery poll: ok=%v err=%v", ok, err)
	}
	if len(secondExecutor.prompts) == 0 || ownershipStressInboundCount(state, session.ID, newMessage.ID) != 1 {
		teamsOwnershipStressFinding(t, "multi-day outage crossed expired block into a permanent freeze: prompts=%v inbound=%d poll=%#v", secondExecutor.prompts, ownershipStressInboundCount(state, session.ID, newMessage.ID), poll)
	}
	if poll.PollState == inboundPollStateParked || !poll.ParkedAt.IsZero() || !poll.ParkNoticeSentAt.IsZero() {
		teamsOwnershipStressFinding(t, "fresh user activity remained auto-frozen after multi-day recovery: poll=%#v", poll)
	}
	if poll.ContinuationPath != oldContinuation {
		teamsOwnershipStressFinding(t, "fresh head activity unexpectedly discarded the old continuation: poll=%#v", poll)
	}

	// The first recovery cycle handled the fresh head message, so production
	// deliberately retained the old continuation.  A long outage must not make
	// that backlog unreachable merely because the head was active.  Drive a
	// second due cycle with an empty head and verify that the old continuation
	// is actually consumed exactly once before any later auto-park decision.
	mu.Lock()
	phase = 2
	mu.Unlock()
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     time.Now().Add(-time.Minute),
		LastActivityAt: time.Now(),
	}); err != nil {
		t.Fatalf("make retained multi-day continuation due: %v", err)
	}
	if err := second.pollOnce(ctx, 20); err != nil {
		t.Fatalf("drain retained continuation after multi-day recovery: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("reload multi-day state after continuation drain: %v", err)
	}
	poll, ok, err = store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read multi-day poll after continuation drain: ok=%v err=%v", ok, err)
	}
	if got := ownershipStressInboundCount(state, session.ID, oldMessage.ID); got != 1 || poll.ContinuationPath != "" {
		teamsOwnershipStressFinding(t, "retained multi-day continuation was not drained after fresh head recovery: old_inbound=%d prompts=%v poll=%#v", got, secondExecutor.prompts, poll)
	}
	if got := ownershipStressInboundCount(state, session.ID, newMessage.ID); got != 1 {
		teamsOwnershipStressFinding(t, "fresh multi-day message was not durable exactly once after continuation drain: inbound=%d", got)
	}
}

func drainOwnershipStressTurnQueue(t *testing.T, bridge *Bridge, maxRounds int) {
	t.Helper()
	if maxRounds < 1 {
		maxRounds = 1
	}
	ctx := context.Background()
	active := false
	for round := 0; round < maxRounds; round++ {
		if err := bridge.processQueuedTurns(ctx); err != nil {
			t.Fatalf("drain queued turn round %d: %v", round, err)
		}
		waitForBridgeAsyncTurns(t, bridge)
		state, err := bridge.store.Load(ctx)
		if err != nil {
			t.Fatalf("load queue state during drain: %v", err)
		}
		active = false
		for _, turn := range state.Turns {
			if turn.Status == teamstore.TurnStatusQueued || turn.Status == teamstore.TurnStatusRunning {
				active = true
				break
			}
		}
		if !active {
			return
		}
	}
	if active {
		teamsOwnershipStressFinding(t, "queued-turn drain budget was exhausted with queued or running work still present: max_rounds=%d", maxRounds)
	}
}

// TestTeamsOwnershipStressBlockedChatDoesNotStopOtherChatsCI checks the
// global-vs-local failure boundary.  A TUI-owned thread in one Work chat must
// not stop a second Teams Work chat from making progress.
func TestTeamsOwnershipStressBlockedChatDoesNotStopOtherChatsCI(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &teamsOwnershipStressExecutor{busyThreads: map[string]bool{"thread-blocked": true}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	blocked := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, blocked, "thread-blocked", "")
	open := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
	open.CodexThreadID = "thread-open"
	if err := store.UpdateSession(ctx, open.ID, func(state *teamstore.State) error {
		stored := state.Sessions[open.ID]
		stored.CodexThreadID = open.CodexThreadID
		state.Sessions[open.ID] = stored
		return nil
	}); err != nil {
		t.Fatalf("seed open thread: %v", err)
	}

	if err := bridge.handleSessionMessage(ctx, blocked.ChatID, bridgeTestMessageWithText("blocked-message", "OWNERSHIP_STRESS_BLOCKED"), "OWNERSHIP_STRESS_BLOCKED"); err != nil {
		t.Fatalf("blocked chat input: %v", err)
	}
	if err := bridge.handleSessionMessage(ctx, open.ChatID, bridgeTestMessageWithText("open-message", "OWNERSHIP_STRESS_OPEN"), "OWNERSHIP_STRESS_OPEN"); err != nil {
		t.Fatalf("open chat input: %v", err)
	}
	waitForBridgeAsyncTurns(t, bridge)

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load per-chat isolation state: %v", err)
	}
	if ownershipStressCompletedTurnCount(state, open.ID) != 1 {
		teamsOwnershipStressFinding(t, "blocked chat stopped unrelated chat: open completed turns=%d state=%#v", ownershipStressCompletedTurnCount(state, open.ID), state.Turns)
	}
}

// TestTeamsOwnershipStressControlLeaseSameMachineReacquireGenerationCI models
// a helper restart on the same machine after an explicit release.  A stale
// cleanup callback must not be able to match the newly acquired lease merely
// because the generation counter restarted at one.
func TestTeamsOwnershipStressControlLeaseSameMachineReacquireGenerationCI(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC)
	scope := teamstore.ScopeIdentity{ID: "ownership-stress-scope", AccountID: "user-1", OSUser: "tester", Profile: "default"}
	machine := teamstore.MachineRecord{ID: "ownership-stress-machine", ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary, Label: "stress"}
	first, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
		Scope: scope, Machine: machine, Duration: time.Minute, Now: now,
	})
	if err != nil {
		t.Fatalf("initial control lease claim: %v", err)
	}
	released, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, first.Lease.Generation)
	if err != nil || !released {
		t.Fatalf("release initial control lease: released=%v err=%v", released, err)
	}
	second, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
		Scope: scope, Machine: machine, Duration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("reacquire control lease after restart: %v", err)
	}
	if second.Mode != teamstore.LeaseModeActive {
		t.Fatalf("reacquired control lease mode=%q, want active", second.Mode)
	}
	if second.Lease.Generation <= first.Lease.Generation {
		teamsOwnershipStressFinding(t, "same-machine control lease generation was reused after release: first=%d second=%d", first.Lease.Generation, second.Lease.Generation)
	}

	// Simulate the old process finishing its deferred cleanup after the new
	// process has already become active.
	staleReleased, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, first.Lease.Generation)
	if err != nil {
		t.Fatalf("stale old control lease cleanup: %v", err)
	}
	if staleReleased {
		teamsOwnershipStressFinding(t, "stale same-machine lease cleanup released the replacement lease: first=%#v second=%#v", first.Lease, second.Lease)
	}
	current, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load control lease after stale cleanup: %v", err)
	}
	if current.ControlLease.HolderMachineID != machine.ID || current.ControlLease.Generation != second.Lease.Generation {
		teamsOwnershipStressFinding(t, "stale same-machine cleanup changed the replacement control lease: current=%#v replacement=%#v", current.ControlLease, second.Lease)
	}
}

// TestTeamsOwnershipStressSameMachineLiveLeaseTakeoverIncrementsGenerationCI
// covers the more dangerous restart shape than an explicit release: the old
// owner row can disappear while its lease is still live. A replacement process
// on the same host must not inherit the old process capability merely because
// the durable machine ID is unchanged.
func TestTeamsOwnershipStressSameMachineLiveLeaseTakeoverIncrementsGenerationCI(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate store to SQLite: %v", err)
				}
			}
			now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
			scope := teamstore.ScopeIdentity{ID: "same-machine-live-scope", AccountID: "user-1", OSUser: "tester", Profile: "default"}
			machine := teamstore.MachineRecord{ID: "same-machine-live-machine", ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary}
			ownerA := teamstore.OwnerMetadata{
				PID: 1111, Hostname: "same-machine-host", ExecutablePath: "/bin/cxp",
				StartedAt: now, LastHeartbeat: now, ScopeID: scope.ID, MachineID: machine.ID,
			}
			first, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
				Scope: scope, Machine: machine, Owner: ownerA, Duration: time.Minute, Now: now,
			})
			if err != nil || first.Mode != teamstore.LeaseModeActive {
				t.Fatalf("initial claim: decision=%#v err=%v", first, err)
			}
			ownerA.LeaseGeneration = first.Lease.Generation
			if _, err := store.RecordOwnerHeartbeat(ctx, ownerA, time.Minute, now); err != nil {
				t.Fatalf("record initial owner: %v", err)
			}
			// Simulate a missing owner row after the old process disappears while
			// the control lease has not yet expired.
			if err := store.ClearOwner(ctx); err != nil {
				t.Fatalf("clear stale owner row: %v", err)
			}
			ownerB := ownerA
			ownerB.PID = 2222
			ownerB.StartedAt = now.Add(time.Second)
			ownerB.LastHeartbeat = ownerB.StartedAt
			second, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
				Scope: scope, Machine: machine, Owner: ownerB, Duration: time.Minute, Now: now.Add(time.Second),
			})
			if err != nil || second.Mode != teamstore.LeaseModeActive {
				t.Fatalf("replacement claim: decision=%#v err=%v", second, err)
			}
			if second.Lease.Generation <= first.Lease.Generation {
				t.Fatalf("same-machine live-lease takeover reused generation: first=%d second=%d", first.Lease.Generation, second.Lease.Generation)
			}
			if released, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, first.Lease.Generation); err != nil {
				t.Fatalf("stale release: %v", err)
			} else if released {
				t.Fatal("stale same-machine release cleared replacement lease")
			}
			if _, err := store.ValidateControlLease(ctx, machine.ID, first.Lease.Generation, now.Add(time.Second)); !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
				t.Fatalf("stale generation validation error=%v, want ErrControlLeaseNotHeld", err)
			}
		})
	}
}

// TestTeamsOwnershipStressParkedChatProbeWakesMessageCI covers the user-visible
// freeze boundary: a chat is already parked, a message arrives while the
// service is absent, and the next owner must discover it through a bounded
// low-frequency probe. The waiting message must be consumed once rather than
// being silently discarded or requiring a second freeze notice.
func TestTeamsOwnershipStressParkedChatProbeWakesMessageCI(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	oldActivity := now.Add(-72 * time.Hour)
	store := newBridgeTestStore(t)
	writeGraph, sent := newBridgeTestGraph(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "parked chat resumed answer",
		CodexThreadID: "thread-parked-resume",
		CodexTurnID:   "turn-parked-resume",
	}}
	bridge := newBridgeTestBridge(writeGraph, store, executor)
	seedOwnershipStressControlIdle(t, store)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-parked-resume", "")
	if _, err := store.RecordChatPollSuccess(ctx, session.ChatID, oldActivity, true, false, 20); err != nil {
		t.Fatalf("seed parked chat cursor: %v", err)
	}
	parked, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateParked,
		NextPollAt:     oldActivity,
		LastActivityAt: oldActivity,
	})
	if err != nil {
		t.Fatalf("park chat before outage: %v", err)
	}
	if _, err := store.MarkChatPollParkNoticeSent(ctx, session.ChatID, parked.ParkedAt.Add(time.Minute)); err != nil {
		t.Fatalf("mark parked notice sent: %v", err)
	}

	waiting := bridgeTestMessageWithText("arrived-while-parked", "OWNERSHIP_STRESS_ARRIVED_WHILE_PARKED")
	waiting.ChatID = session.ChatID
	waiting.CreatedDateTime = now.Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	waiting.LastModifiedDateTime = waiting.CreatedDateTime
	var headReads int
	readServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/"+session.ChatID+"/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		headReads++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{waiting}})
	}))
	t.Cleanup(readServer.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     readServer.Client(),
		baseURL:    readServer.URL,
		maxRetries: 1,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	if err := bridge.pollOnce(ctx, 20); err != nil {
		t.Fatalf("probe parked chat during outage: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load parked state after probe: %v", err)
	}
	if len(executor.prompts) != 1 || ownershipStressInboundCount(state, session.ID, waiting.ID) != 1 || ownershipStressCompletedTurnCount(state, session.ID) != 1 {
		t.Fatalf("parked chat probe did not recover its waiting message exactly once: prompts=%v state=%#v", executor.prompts, state)
	}
	if countSentPlainContainingForChat(*sent, session.ChatID, "This chat is paused") != 0 {
		t.Fatalf("parked chat emitted a duplicate freeze notice: %#v", *sent)
	}
	poll, ok, err := store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read probed chat poll: ok=%v err=%v", ok, err)
	}
	if poll.PollState != inboundPollStateHot || !poll.ParkedAt.IsZero() || !poll.ParkNoticeSentAt.IsZero() {
		t.Fatalf("successful parked probe did not clear parked state: %#v", poll)
	}
	if headReads != 1 {
		t.Fatalf("parked probe read count = %d, want one", headReads)
	}
	// A second immediate scheduler pass must not poll again just because the
	// chat used to be parked; normal hot scheduling now owns the chat.
	if err := bridge.pollOnce(ctx, 20); err != nil {
		t.Fatalf("poll after parked probe: %v", err)
	}
	if headReads != 1 || len(executor.prompts) != 1 {
		teamsOwnershipStressFinding(t, "parked chat probe replayed a recovered message: head_reads=%d inbound=%d completed=%d prompts=%v", headReads, ownershipStressInboundCount(state, session.ID, waiting.ID), ownershipStressCompletedTurnCount(state, session.ID), executor.prompts)
	}
}

// TestTeamsOwnershipStressParkedStaleContinuationWithNewMessageCI combines an
// already published freeze notice with a multi-day stale Graph token. A new
// message must still be admitted exactly once, without another freeze notice;
// once the old frontier becomes readable again, the parked probe must clear
// the local error instead of leaving the chat permanently frozen.
func TestTeamsOwnershipStressParkedStaleContinuationWithNewMessageCI(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	oldActivity := now.Add(-72 * time.Hour)
	oldContinuation := "/chats/chat-1/messages?$skiptoken=parked-stale"
	store := newBridgeTestStore(t)
	writeGraph, sent := newBridgeTestGraph(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "parked stale continuation recovered answer",
		CodexThreadID: "thread-parked-stale",
		CodexTurnID:   "turn-parked-stale",
	}}
	bridge := newBridgeTestBridge(writeGraph, store, executor)
	seedOwnershipStressControlIdle(t, store)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-parked-stale", "")
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, session.ChatID, oldActivity, true, true, 20, oldContinuation); err != nil {
		t.Fatalf("seed parked stale continuation: %v", err)
	}
	parked, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateParked,
		NextPollAt:     oldActivity,
		LastActivityAt: oldActivity,
	})
	if err != nil {
		t.Fatalf("park stale chat: %v", err)
	}
	if _, err := store.MarkChatPollParkNoticeSent(ctx, session.ChatID, parked.ParkedAt.Add(time.Minute)); err != nil {
		t.Fatalf("mark parked stale notice sent: %v", err)
	}
	waiting := bridgeTestMessageWithText("arrived-after-stale-park", "OWNERSHIP_STRESS_ARRIVED_AFTER_STALE_PARK")
	waiting.ChatID = session.ChatID
	waiting.CreatedDateTime = now.Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	waiting.LastModifiedDateTime = waiting.CreatedDateTime
	phase := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/"+session.ChatID+"/messages" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$skiptoken") == "parked-stale" {
			mu.Lock()
			currentPhase := phase
			mu.Unlock()
			if currentPhase == 0 {
				w.WriteHeader(http.StatusGone)
				_, _ = fmt.Fprint(w, `{"error":{"code":"InvalidSkipToken","message":"continuation expired"}}`)
				return
			}
			_, _ = fmt.Fprint(w, `{"value":[]}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{waiting}})
	}))
	t.Cleanup(server.Close)
	bridge.readGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 1,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	if err := bridge.pollOnce(ctx, 20); err != nil {
		t.Fatalf("stale parked continuation leaked a chat-local error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load parked stale state: %v", err)
	}
	if len(executor.prompts) != 0 || ownershipStressInboundCount(state, session.ID, waiting.ID) != 0 || ownershipStressCompletedTurnCount(state, session.ID) != 0 {
		t.Fatalf("stale continuation should defer the head until its frontier is reconciled: prompts=%v state=%#v", executor.prompts, state)
	}
	if countSentPlainContainingForChat(*sent, session.ChatID, "This chat is paused") != 0 {
		t.Fatalf("stale parked recovery emitted a duplicate freeze notice: %#v", *sent)
	}
	poll, ok, err := store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read parked stale poll: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != "" || poll.Gap == nil || poll.LastError == "" {
		t.Fatalf("stale parked frontier was not isolated in a recovery gap: %#v", poll)
	}

	mu.Lock()
	phase = 1
	mu.Unlock()
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:            session.ChatID,
		PollState:         inboundPollStateWarm,
		NextPollAt:        now.Add(-time.Second),
		LastActivityAt:    now,
		ClearBlockedUntil: true,
	}); err != nil {
		t.Fatalf("make parked stale recovery due: %v", err)
	}
	if err := bridge.pollOnce(ctx, 20); err != nil {
		t.Fatalf("recover parked stale continuation: %v", err)
	}
	poll, ok, err = store.ChatPoll(ctx, session.ChatID)
	if err != nil || !ok {
		t.Fatalf("read recovered parked stale poll: ok=%v err=%v", ok, err)
	}
	if poll.ContinuationPath != "" || !poll.LastErrorAt.IsZero() || poll.PollState == inboundPollStateBlocked {
		t.Fatalf("parked stale recovery left chat frozen: %#v", poll)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("reload parked stale state after gap recovery: %v", err)
	}
	if len(executor.prompts) != 1 || ownershipStressInboundCount(state, session.ID, waiting.ID) != 1 || ownershipStressCompletedTurnCount(state, session.ID) != 1 {
		t.Fatalf("new message was not admitted exactly once by the gap recovery lane: prompts=%v state=%#v", executor.prompts, state)
	}
}

// TestTeamsOwnershipStressLateInboundClaimCannotOverwriteNewOwnerCI models a
// service takeover while an old poll goroutine is still unwinding.  The old
// claim's delayed completion/release must not mark the new owner's claim done
// or delete it before the new owner has persisted the inbound event.
func TestTeamsOwnershipStressLateInboundClaimCannotOverwriteNewOwnerCI(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
	claimedAt := time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC)
	oldClaim, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "late-claim", "owner-a", claimedAt)
	if err != nil || !claimed {
		t.Fatalf("old owner claim: claimed=%v err=%v", claimed, err)
	}
	newClaim, claimed, err := claimGlobalInbound(ctx, path, "chat-1", "late-claim", "owner-b", claimedAt.Add(globalInboundClaimTTL+time.Second))
	if err != nil || !claimed {
		t.Fatalf("new owner takeover: claimed=%v err=%v", claimed, err)
	}
	if err := completeGlobalInbound(ctx, oldClaim); err != nil {
		t.Fatalf("late old completion: %v", err)
	}
	releaseGlobalInbound(ctx, oldClaim)
	ledger, err := readGlobalInboundLedger(path)
	if err != nil {
		t.Fatalf("read ledger after stale old owner: %v", err)
	}
	item := ledger.Items[globalInboundKey("chat-1", "late-claim")]
	if item.Status != "claimed" || item.Owner != newClaim.Owner {
		teamsOwnershipStressFinding(t, "stale inbound owner overwrote replacement claim: item=%#v replacement=%#v", item, newClaim)
	}
	if err := completeGlobalInbound(ctx, newClaim); err != nil {
		t.Fatalf("new owner completion: %v", err)
	}
}

// TestTeamsOwnershipStressActiveInboundHandoffRemainsRetryableCI models a
// live owner that loses its handler after another service has already observed
// the message.  The observing service must not put the message in its local
// seen set: after the old claim is released, the next poll still has to run it.
func TestTeamsOwnershipStressActiveInboundHandoffRemainsRetryableCI(t *testing.T) {
	ctx := context.Background()
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	ledgerPath, ok := globalInboundLedgerPathForRegistry(registryPath)
	if !ok {
		t.Fatal("global inbound ledger path was not derived")
	}
	claimedAt := time.Now()
	oldClaim, claimed, err := claimGlobalInbound(ctx, ledgerPath, "chat-1", "handoff-message", "owner-a", claimedAt)
	if err != nil || !claimed {
		t.Fatalf("old owner claim: claimed=%v err=%v", claimed, err)
	}

	readGraph := newBridgePollGraph(t, []bridgePollPage{
		{messages: []ChatMessage{bridgeTestMessageWithText("handoff-message", "retry after owner failure")}},
	})
	writeGraph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	bridge.registryPath = registryPath
	bridge.machine.ID = "owner-b"
	seedOwnershipStressDuePoll(t, store, "chat-1")

	handled := 0
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, func(context.Context, ChatMessage, string) error {
		handled++
		return nil
	}); err != nil {
		t.Fatalf("poll while old owner is active: %v", err)
	}
	if handled != 0 {
		t.Fatalf("active owner handoff ran the message early: handled=%d", handled)
	}

	releaseGlobalInbound(ctx, oldClaim)
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, func(context.Context, ChatMessage, string) error {
		handled++
		return nil
	}); err != nil {
		t.Fatalf("poll after old owner release: %v", err)
	}
	if handled != 1 {
		teamsOwnershipStressFinding(t, "active inbound claim handoff skipped released message: handled=%d", handled)
	}
}

// TestTeamsOwnershipStressActiveClaimDoesNotAdvanceCursorCI models a page
// containing an older message claimed by another owner and a newer message
// that this owner can handle. The page's maximum timestamp must not move the
// durable cursor past the still-unresolved claim.
func TestTeamsOwnershipStressActiveClaimDoesNotAdvanceCursorCI(t *testing.T) {
	ctx := context.Background()
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	ledgerPath, ok := globalInboundLedgerPathForRegistry(registryPath)
	if !ok {
		t.Fatal("global inbound ledger path was not derived")
	}
	now := time.Now().UTC()
	oldTime := now.Add(-time.Minute)
	newTime := now.Add(time.Minute)
	oldMessage := bridgePollMessage("active-claim-old", oldTime.Format(time.RFC3339Nano), "old message after owner handoff")
	newMessage := bridgePollMessage("active-claim-new", newTime.Format(time.RFC3339Nano), "newer message remains processable")
	oldClaim, claimed, err := claimGlobalInbound(ctx, ledgerPath, "chat-1", oldMessage.ID, "owner-a", now)
	if err != nil || !claimed {
		t.Fatalf("seed active claim: claimed=%v err=%v", claimed, err)
	}
	defer releaseGlobalInbound(ctx, oldClaim)

	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", now, true, false, 0); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      inboundPollStateWarm,
		NextPollAt:     now.Add(-time.Second),
		LastActivityAt: now,
	}); err != nil {
		t.Fatalf("seed due poll: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{oldMessage, newMessage}})
	}))
	t.Cleanup(server.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	bridge.registryPath = registryPath
	bridge.machine.ID = "owner-b"
	handledIDs := make([]string, 0, 2)
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handledIDs = append(handledIDs, msg.ID)
		return nil
	}
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("poll while old owner is active: %v", err)
	}
	first, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read first poll: ok=%v err=%v", ok, err)
	}
	if !first.LastModifiedCursor.Equal(now) {
		t.Fatalf("active claim allowed cursor advance: got=%s want=%s", first.LastModifiedCursor, now)
	}
	if len(handledIDs) != 1 || handledIDs[0] != newMessage.ID {
		t.Fatalf("first poll handled=%v, want only newer message", handledIDs)
	}
	releaseGlobalInbound(ctx, oldClaim)
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("poll after old owner release: %v", err)
	}
	if len(handledIDs) != 2 || handledIDs[1] != oldMessage.ID {
		t.Fatalf("released active claim was not retried exactly once: handled=%v", handledIDs)
	}
	// The one-action quantum leaves the already-seen newer record in the staged
	// page after retrying the released older claim. A final local replay must
	// consume that record and commit the cursor without a third Graph read.
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("final active-claim page replay: %v", err)
	}
	second, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read second poll: ok=%v err=%v", ok, err)
	}
	if !second.LastModifiedCursor.After(now) {
		t.Fatalf("cursor did not advance after claim resolution: %#v", second)
	}
}

// TestTeamsOwnershipStressActiveClaimWithMultipleFrontiersDoesNotAdvanceCursorCI
// combines a service takeover with two durable Graph frontiers. A head message
// is still claimed by the old owner while the new owner drains the old current
// continuation. Successful work from that continuation must not advance the
// cursor past the unresolved head claim.
func TestTeamsOwnershipStressActiveClaimWithMultipleFrontiersDoesNotAdvanceCursorCI(t *testing.T) {
	ctx := context.Background()
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	ledgerPath, ok := globalInboundLedgerPathForRegistry(registryPath)
	if !ok {
		t.Fatal("global inbound ledger path was not derived")
	}
	now := time.Now().UTC()
	oldContinuation := "/chats/chat-1/messages?$skiptoken=old-frontier"
	deferredContinuation := "/chats/chat-1/messages?$skiptoken=deferred-frontier"
	claimedHead := bridgePollMessage("active-multi-frontier-head", now.Add(time.Minute).Format(time.RFC3339Nano), "head still owned by old helper")
	backlogMessage := bridgePollMessage("active-multi-frontier-backlog", now.Add(2*time.Minute).Format(time.RFC3339Nano), "older frontier message")
	oldClaim, claimed, err := claimGlobalInbound(ctx, ledgerPath, "chat-1", claimedHead.ID, "owner-a", now)
	if err != nil || !claimed {
		t.Fatalf("seed active multi-frontier claim: claimed=%v err=%v", claimed, err)
	}
	defer releaseGlobalInbound(ctx, oldClaim)

	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-1"] = teamstore.ChatPollState{
			ChatID:                   "chat-1",
			Seeded:                   true,
			PollState:                inboundPollStateWarm,
			NextPollAt:               now,
			LastActivityAt:           now,
			LastModifiedCursor:       now,
			ContinuationPath:         oldContinuation,
			DeferredContinuationPath: deferredContinuation,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed multiple frontier poll: %v", err)
	}

	headReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "old-frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{backlogMessage}})
		case "deferred-frontier":
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
		default:
			headReads++
			if headReads == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"value":           []ChatMessage{claimedHead},
					"@odata.nextLink": deferredContinuation,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{claimedHead}})
		}
	}))
	t.Cleanup(server.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	bridge.registryPath = registryPath
	bridge.machine.ID = "owner-b"
	handledIDs := make([]string, 0, 2)
	handle := func(_ context.Context, msg ChatMessage, _ string) error {
		handledIDs = append(handledIDs, msg.ID)
		return nil
	}

	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("poll with old claim and multiple frontiers: %v", err)
	}
	first, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read first multi-frontier poll: ok=%v err=%v", ok, err)
	}
	if !first.LastModifiedCursor.Equal(now) {
		t.Fatalf("backlog handling advanced cursor past active head claim: got=%s want=%s", first.LastModifiedCursor, now)
	}
	if len(handledIDs) != 1 || handledIDs[0] != backlogMessage.ID {
		t.Fatalf("first multi-frontier poll handled=%v, want only backlog message", handledIDs)
	}
	if first.ContinuationPath != deferredContinuation || first.DeferredContinuationPath != "" {
		t.Fatalf("first multi-frontier frontier handoff = %#v, want deferred path promoted", first)
	}

	releaseGlobalInbound(ctx, oldClaim)
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("poll after old multi-frontier claim release: %v", err)
	}
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("poll after deferred multi-frontier frontier promotion: %v", err)
	}
	if len(handledIDs) != 2 || handledIDs[1] != claimedHead.ID {
		t.Fatalf("released active head was not retried exactly once: handled=%v", handledIDs)
	}
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, handle); err != nil {
		t.Fatalf("drain deferred multi-frontier page: %v", err)
	}
	second, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read second multi-frontier poll: ok=%v err=%v", ok, err)
	}
	if !second.LastModifiedCursor.After(now) {
		t.Fatalf("cursor did not advance after multi-frontier claim resolution: %#v", second)
	}
}

// TestTeamsOwnershipStressActionLimitedContinuationDoesNotSkipActiveClaimCI
// covers a continuation page where the first message consumes the one-message
// backlog quantum, the next message is still claimed by another owner, and a
// newer message follows it. Unexamined records must not contribute to the
// cursor merely because they were present in the response page.
func TestTeamsOwnershipStressActionLimitedContinuationDoesNotSkipActiveClaimCI(t *testing.T) {
	ctx := context.Background()
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	ledgerPath, ok := globalInboundLedgerPathForRegistry(registryPath)
	if !ok {
		t.Fatal("global inbound ledger path was not derived")
	}
	now := time.Now().UTC()
	continuation := "/chats/chat-1/messages?$skiptoken=limited"
	firstMessage := bridgePollMessage("limited-first", now.Add(time.Minute).Format(time.RFC3339Nano), "first backlog message")
	activeMessage := bridgePollMessage("limited-active", now.Add(2*time.Minute).Format(time.RFC3339Nano), "active old owner message")
	newerMessage := bridgePollMessage("limited-newer", now.Add(3*time.Minute).Format(time.RFC3339Nano), "newer unexamined message")
	oldClaim, claimed, err := claimGlobalInbound(ctx, ledgerPath, "chat-1", activeMessage.ID, "owner-a", now)
	if err != nil || !claimed {
		t.Fatalf("seed action-limited active claim: claimed=%v err=%v", claimed, err)
	}
	defer releaseGlobalInbound(ctx, oldClaim)

	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, "chat-1", now, true, true, 20, continuation); err != nil {
		t.Fatalf("seed action-limited continuation: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      inboundPollStateWarm,
		NextPollAt:     now,
		LastActivityAt: now,
	}); err != nil {
		t.Fatalf("schedule action-limited continuation: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$skiptoken") == "limited" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []ChatMessage{firstMessage, activeMessage, newerMessage},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
	}))
	t.Cleanup(server.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 1,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	bridge.registryPath = registryPath
	bridge.machine.ID = "owner-b"
	handledIDs := make([]string, 0, 3)
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, func(_ context.Context, msg ChatMessage, _ string) error {
		handledIDs = append(handledIDs, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("action-limited continuation poll: %v", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read action-limited poll: ok=%v err=%v", ok, err)
	}
	if !poll.LastModifiedCursor.Equal(now) && !poll.LastModifiedCursor.Equal(messageModifiedTime(firstMessage)) {
		t.Fatalf("action-limited continuation skipped active claim: cursor=%s want <=%s", poll.LastModifiedCursor, messageModifiedTime(firstMessage))
	}
	if len(handledIDs) != 1 || handledIDs[0] != firstMessage.ID {
		t.Fatalf("action-limited continuation handled=%v, want only first message", handledIDs)
	}

	releaseGlobalInbound(ctx, oldClaim)
	if _, err := bridge.pollChatWithRole(ctx, "chat-1", 20, inboundPollRoleWork, false, func(_ context.Context, msg ChatMessage, _ string) error {
		handledIDs = append(handledIDs, msg.ID)
		return nil
	}); err != nil {
		t.Fatalf("retry action-limited continuation poll: %v", err)
	}
	if len(handledIDs) < 2 || handledIDs[1] != activeMessage.ID {
		t.Fatalf("released continuation claim was not retried: handled=%v", handledIDs)
	}
}

// TestTeamsOwnershipStressExpiredContinuationRetainsFrontierCI models a
// multi-day outage invalidating a saved Graph skip token. A terminal head page
// cannot prove that the unread records behind the old token were delivered, so
// the service immediately enters an explicit recovery gap for a token-specific
// failure, rather than a permanent scheduler block or a blind retry loop.
func TestTeamsOwnershipStressExpiredContinuationRetainsFrontierCI(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	continuation := "/chats/chat-1/messages?$skiptoken=old"
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-1"] = teamstore.ChatPollState{
			ChatID:             "chat-1",
			Seeded:             true,
			PollState:          inboundPollStateWarm,
			NextPollAt:         now,
			LastActivityAt:     now.Add(-time.Minute),
			LastModifiedCursor: now.Add(-time.Hour),
			ContinuationPath:   continuation,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed expired continuation: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$skiptoken") == "old" {
			w.WriteHeader(http.StatusGone)
			_, _ = fmt.Fprint(w, `{"error":{"code":"Gone","message":"old continuation expired"}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	if _, err := bridge.pollChatWithRoleStateOptions(ctx, "chat-1", 20, inboundPollRoleWork, false, teamstore.ChatPollState{
		ChatID:             "chat-1",
		Seeded:             true,
		PollState:          inboundPollStateWarm,
		NextPollAt:         now,
		LastActivityAt:     now.Add(-time.Minute),
		LastModifiedCursor: now.Add(-time.Hour),
		ContinuationPath:   continuation,
	}, true, pollChatWithRoleOptions{AllowBacklogDrain: true, RecoverStaleContinuation: true}, func(context.Context, ChatMessage, string) error {
		return nil
	}); err == nil {
		t.Fatal("expired continuation unexpectedly succeeded")
	}
	got, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read expired continuation poll: ok=%v err=%v", ok, err)
	}
	if got.ContinuationPath != "" || got.Gap == nil || got.PollState == inboundPollStateBlocked || !got.BlockedUntil.IsZero() || got.LastError == "" {
		t.Fatalf("expired continuation did not become an explicit non-blocking gap: %#v", got)
	}
}

// TestTeamsOwnershipStressMultipleContinuationFrontiersRemainDurableCI covers
// the legacy A+B representation. New runtime code drains A first and promotes
// B only after A completes; it never issues a third head request while A is
// operational.
func TestTeamsOwnershipStressMultipleContinuationFrontiersRemainDurableCI(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	oldContinuation := "/chats/chat-1/messages?$skiptoken=old"
	deferredContinuation := "/chats/chat-1/messages?$skiptoken=deferred"
	freshContinuation := "/chats/chat-1/messages?$skiptoken=fresh"
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-1"] = teamstore.ChatPollState{
			ChatID:                   "chat-1",
			Seeded:                   true,
			PollState:                inboundPollStateWarm,
			NextPollAt:               now,
			LastActivityAt:           now.Add(-time.Minute),
			LastModifiedCursor:       now.Add(-time.Hour),
			ContinuationPath:         oldContinuation,
			DeferredContinuationPath: deferredContinuation,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed multiple continuation frontiers: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token := r.URL.Query().Get("$skiptoken"); token != "old" && token != "deferred" {
			t.Fatalf("unexpected frontier request: %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$skiptoken") {
		case "old":
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
		case "deferred":
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
		}
	}))
	t.Cleanup(server.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	poll := teamstore.ChatPollState{
		ChatID:                   "chat-1",
		Seeded:                   true,
		PollState:                inboundPollStateWarm,
		NextPollAt:               now,
		LastActivityAt:           now.Add(-time.Minute),
		LastModifiedCursor:       now.Add(-time.Hour),
		ContinuationPath:         oldContinuation,
		DeferredContinuationPath: deferredContinuation,
	}
	if _, err := bridge.pollChatWithRoleStateOptions(ctx, "chat-1", 20, inboundPollRoleWork, false, poll, true, pollChatWithRoleOptions{
		AllowBacklogDrain:        true,
		RecoverStaleContinuation: true,
	}, func(context.Context, ChatMessage, string) error { return nil }); err != nil {
		t.Fatalf("legacy first frontier poll: %v", err)
	}
	got, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read multiple-frontier poll: ok=%v err=%v", ok, err)
	}
	if got.ContinuationPath != deferredContinuation || got.DeferredContinuationPath != "" || got.PollState == inboundPollStateBlocked {
		t.Fatalf("legacy deferred frontier was not promoted: %#v", got)
	}
	_ = freshContinuation
}

// TestTeamsOwnershipStressRepeatedContinuationStopsLivelockCI models Graph
// returning the same nextLink forever. The service must stop the per-chat
// loop with an explicit gap instead of recording success and issuing the same
// request forever.
func TestTeamsOwnershipStressRepeatedContinuationStopsLivelockCI(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	continuation := "/chats/chat-1/messages?$skiptoken=repeated"
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls["chat-1"] = teamstore.ChatPollState{
			ChatID:             "chat-1",
			Seeded:             true,
			PollState:          inboundPollStateWarm,
			NextPollAt:         now,
			LastActivityAt:     now.Add(-time.Minute),
			LastModifiedCursor: now.Add(-time.Hour),
			ContinuationPath:   continuation,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed repeated continuation: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$skiptoken") == "repeated" {
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}, "@odata.nextLink": continuation})
			return
		}
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	t.Cleanup(server.Close)
	readGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	writeGraph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	poll := teamstore.ChatPollState{
		ChatID:             "chat-1",
		Seeded:             true,
		PollState:          inboundPollStateWarm,
		NextPollAt:         now,
		LastActivityAt:     now.Add(-time.Minute),
		LastModifiedCursor: now.Add(-time.Hour),
		ContinuationPath:   continuation,
	}
	for attempt := 0; attempt < continuationFailureBudget; attempt++ {
		if _, err := bridge.pollChatWithRoleStateOptions(ctx, "chat-1", 20, inboundPollRoleWork, false, poll, true, pollChatWithRoleOptions{
			AllowBacklogDrain: true,
		}, func(context.Context, ChatMessage, string) error { return nil }); err == nil {
			t.Fatalf("repeated continuation attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	got, ok, err := store.ChatPoll(ctx, "chat-1")
	if err != nil || !ok {
		t.Fatalf("read repeated continuation state: ok=%v err=%v", ok, err)
	}
	if got.ContinuationPath != "" || got.Gap == nil || got.PollState == inboundPollStateBlocked || !strings.Contains(got.LastError, "no progress") {
		t.Fatalf("repeated continuation was not isolated with a durable gap: %#v", got)
	}
}

// TestTeamsOwnershipStressInboundClaimFencesSameOwnerABAAndDeletedRowsCI
// covers two restart variants that an owner string alone cannot distinguish:
// a new Bridge reuses the same stable machine ID after the claim TTL, and an
// old callback arrives after its claim row was already released.
func TestTeamsOwnershipStressInboundClaimFencesSameOwnerABAAndDeletedRowsCI(t *testing.T) {
	ctx := context.Background()
	claimedAt := time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC)

	t.Run("same-owner-takeover", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
		oldClaim, claimed, err := claimGlobalInbound(ctx, path, "chat-aba", "same-owner", "stable-machine", claimedAt)
		if err != nil || !claimed {
			t.Fatalf("old same-owner claim: claimed=%v err=%v", claimed, err)
		}
		newClaim, claimed, err := claimGlobalInbound(ctx, path, "chat-aba", "same-owner", "stable-machine", claimedAt.Add(globalInboundClaimTTL+time.Second))
		if err != nil || !claimed {
			t.Fatalf("replacement same-owner claim: claimed=%v err=%v", claimed, err)
		}
		releaseGlobalInbound(ctx, oldClaim)
		ledger, err := readGlobalInboundLedger(path)
		if err != nil {
			t.Fatalf("read same-owner ledger: %v", err)
		}
		item, ok := ledger.Items[newClaim.Key]
		if !ok || item.Status != "claimed" || item.Owner != newClaim.Owner {
			teamsOwnershipStressFinding(t, "same-owner stale release deleted replacement claim: item=%#v replacement=%#v", item, newClaim)
		}
	})

	t.Run("late-completion-after-release", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "teams", "global-inbound-ledger.json")
		oldClaim, claimed, err := claimGlobalInbound(ctx, path, "chat-deleted", "released-row", "owner-a", claimedAt)
		if err != nil || !claimed {
			t.Fatalf("claim released-row: claimed=%v err=%v", claimed, err)
		}
		releaseGlobalInbound(ctx, oldClaim)
		if err := completeGlobalInbound(ctx, oldClaim); err != nil {
			t.Fatalf("late completion after release: %v", err)
		}
		ledger, err := readGlobalInboundLedger(path)
		if err != nil {
			t.Fatalf("read released-row ledger: %v", err)
		}
		if _, ok := ledger.Items[oldClaim.Key]; ok {
			teamsOwnershipStressFinding(t, "late completion recreated a released inbound row as done: item=%#v", ledger.Items[oldClaim.Key])
		}
	})
}

type teamsOwnershipStressExecutor struct {
	mu          sync.Mutex
	busyThreads map[string]bool
	calls       []string
}

func (e *teamsOwnershipStressExecutor) Run(_ context.Context, session *Session, prompt string) (ExecutionResult, error) {
	threadID := ""
	if session != nil {
		threadID = strings.TrimSpace(session.CodexThreadID)
	}
	e.mu.Lock()
	e.calls = append(e.calls, prompt)
	busy := e.busyThreads[threadID]
	e.mu.Unlock()
	if busy {
		return ExecutionResult{}, fmt.Errorf("-32600: thread %s already has an active writer", threadID)
	}
	return ExecutionResult{
		Text:          "OWNERSHIP_STRESS_OK " + strings.TrimSpace(prompt),
		CodexThreadID: firstNonEmptyString(threadID, "thread-created-by-teams"),
		CodexTurnID:   fmt.Sprintf("teams-turn-%d", e.callCount()),
	}, nil
}

func (e *teamsOwnershipStressExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

type ownershipStressMessageFeed struct {
	mu          sync.Mutex
	messages    []ChatMessage
	requests    []string
	responseIDs []string
}

func (f *ownershipStressMessageFeed) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func (f *ownershipStressMessageFeed) responseSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.responseIDs...)
}

func newOwnershipStressPagedReadGraph(t *testing.T, messages []ChatMessage) (*GraphClient, *ownershipStressMessageFeed) {
	t.Helper()
	feed := &ownershipStressMessageFeed{messages: append([]ChatMessage(nil), messages...)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/messages/") && !strings.HasSuffix(r.URL.Path, "/messages") {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			messageID := parts[len(parts)-1]
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(bridgeTestMessageWithText(messageID, "recovered Teams message "+messageID))
			return
		}
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		feed.mu.Lock()
		feed.requests = append(feed.requests, r.URL.RequestURI())
		items := append([]ChatMessage(nil), feed.messages...)
		feed.mu.Unlock()
		const pageSize = 10
		page := 0
		if raw := r.URL.Query().Get("$skiptoken"); raw != "" {
			page, _ = strconv.Atoi(raw)
		}
		// Microsoft Graph returns the newest messages first. A continuation
		// link therefore walks toward older messages, which is the important
		// direction for recovering a backlog after an outage.
		end := len(items) - page*pageSize
		if end < 0 {
			end = 0
		}
		start := end - pageSize
		if start < 0 {
			start = 0
		}
		pageItems := make([]ChatMessage, 0, end-start)
		for index := end - 1; index >= start; index-- {
			pageItems = append(pageItems, items[index])
		}
		ids := make([]string, 0, len(pageItems))
		for _, item := range pageItems {
			ids = append(ids, item.ID)
		}
		feed.mu.Lock()
		feed.responseIDs = append(feed.responseIDs, fmt.Sprintf("skiptoken=%d ids=%v", page, ids))
		feed.mu.Unlock()
		payload := map[string]any{"value": pageItems}
		if start > 0 {
			payload["@odata.nextLink"] = "/chats/chat-1/messages?$skiptoken=" + strconv.Itoa(page+1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, feed
}

// newBridgeTestGraph intentionally only implements the outbound POST surface.
// Queued-turn recovery also reads the original Teams message, so ownership
// stress scenarios need the smallest read+write Graph surface as well.
func newOwnershipStressWriteGraph(t *testing.T) *GraphClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			_, _ = fmt.Fprint(w, `{"id":"ownership-stress-sent","messageType":"message"}`)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/chats/") && strings.Contains(r.URL.Path, "/messages/"):
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			messageID := parts[len(parts)-1]
			msg := bridgeTestMessageWithText(messageID, "recovered Teams message "+messageID)
			_ = json.NewEncoder(w).Encode(msg)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
}

func newOwnershipStressBlockingReadGraph(t *testing.T, entered chan<- struct{}, release <-chan struct{}) *GraphClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"value":[]}`)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
}

func ownershipStressMessages(chatID string, count int) []ChatMessage {
	base := time.Date(2026, 8, 23, 1, 0, 0, 0, time.UTC)
	messages := make([]ChatMessage, 0, count)
	for i := 0; i < count; i++ {
		msg := bridgeTestMessageWithText(fmt.Sprintf("backlog-%03d", i), fmt.Sprintf("OWNERSHIP_STRESS_BACKLOG_%03d", i))
		msg.ChatID = chatID
		created := base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
		msg.CreatedDateTime = created
		msg.LastModifiedDateTime = created
		messages = append(messages, msg)
	}
	return messages
}

func seedOwnershipStressDuePoll(t *testing.T, store *teamstore.Store, chatID string) {
	t.Helper()
	seedOwnershipStressDuePollAt(t, store, chatID, time.Now().Add(-time.Minute))
}

func seedOwnershipStressControlIdle(t *testing.T, store *teamstore.Store) {
	t.Helper()
	now := time.Now()
	if _, err := store.RecordChatPollSuccess(context.Background(), "control-chat", now, true, false, 0); err != nil {
		t.Fatalf("seed control poll cursor: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         "control-chat",
		PollState:      inboundPollStateWarm,
		NextPollAt:     now.Add(time.Hour),
		LastActivityAt: now,
	}); err != nil {
		t.Fatalf("seed idle control poll: %v", err)
	}
}

func seedOwnershipStressDuePollAt(t *testing.T, store *teamstore.Store, chatID string, now time.Time) {
	t.Helper()
	if _, err := store.RecordChatPollSuccess(context.Background(), chatID, now, true, false, 0); err != nil {
		t.Fatalf("seed due poll cursor: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         chatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     now.Add(-time.Second),
		LastActivityAt: now,
	}); err != nil {
		t.Fatalf("seed due poll schedule: %v", err)
	}
}

func ownershipStressCompletedInboundCount(state teamstore.State, sessionID string) int {
	count := 0
	for _, turn := range state.Turns {
		if turn.SessionID == sessionID && turn.Status == teamstore.TurnStatusCompleted {
			count++
		}
	}
	return count
}

func ownershipStressCompletedTurnCount(state teamstore.State, sessionID string) int {
	return ownershipStressCompletedInboundCount(state, sessionID)
}

func ownershipStressInboundCount(state teamstore.State, sessionID string, messageID string) int {
	count := 0
	for _, inbound := range state.InboundEvents {
		if inbound.SessionID == sessionID && inbound.TeamsMessageID == messageID {
			count++
		}
	}
	return count
}

func appendOwnershipStressTranscriptLine(t *testing.T, path string, line string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open transcript for append: %v", err)
	}
	defer file.Close()
	if _, err := file.WriteString(line + "\n"); err != nil {
		t.Fatalf("append transcript: %v", err)
	}
}

func filepathForOwnershipStress(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "rollout-ownership-stress.jsonl")
}

func minOwnershipStressInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func teamsOwnershipStressFinding(t *testing.T, format string, args ...any) {
	t.Helper()
	message := fmt.Sprintf(format, args...)
	t.Logf("OWNERSHIP_STRESS_FINDING: %s", message)
	if teamsOwnershipStressStrict() {
		t.Fatalf("%s=%s: %s", teamsOwnershipStressStrictEnv, "1", message)
	}
}

// teamsOwnershipStressStrict makes a finding fail closed.  The stress tests
// are part of the recovery gate, so an unset environment must not silently
// turn an assertion into a diagnostic-only log.  An explicit false value is
// retained for local exploratory pressure runs that intentionally continue
// after collecting multiple findings; CI and the recovery manifest override
// this with an explicit true value.
func teamsOwnershipStressStrict() bool {
	value, ok := os.LookupEnv(teamsOwnershipStressStrictEnv)
	if !ok {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "false", "no", "off":
		return false
	default:
		// Unknown values fail closed instead of weakening a required test gate.
		return true
	}
}

func TestTeamsOwnershipStressStrictModeFailsClosed(t *testing.T) {
	previous, wasSet := os.LookupEnv(teamsOwnershipStressStrictEnv)
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv(teamsOwnershipStressStrictEnv, previous)
			return
		}
		_ = os.Unsetenv(teamsOwnershipStressStrictEnv)
	})

	if err := os.Unsetenv(teamsOwnershipStressStrictEnv); err != nil {
		t.Fatalf("unset strict mode: %v", err)
	}
	if !teamsOwnershipStressStrict() {
		t.Fatal("unset strict mode must fail closed")
	}
	if err := os.Setenv(teamsOwnershipStressStrictEnv, "0"); err != nil {
		t.Fatalf("set exploratory strict mode: %v", err)
	}
	if teamsOwnershipStressStrict() {
		t.Fatal("explicit false strict mode should remain available for exploratory pressure runs")
	}
	if err := os.Setenv(teamsOwnershipStressStrictEnv, "unexpected"); err != nil {
		t.Fatalf("set invalid strict mode: %v", err)
	}
	if !teamsOwnershipStressStrict() {
		t.Fatal("invalid strict mode must fail closed")
	}
}
