package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type recordingExecutor struct {
	prompts  []string
	sessions []Session
	result   ExecutionResult
	err      error
}

func (e *recordingExecutor) Run(_ context.Context, session *Session, prompt string) (ExecutionResult, error) {
	e.prompts = append(e.prompts, prompt)
	if session != nil {
		e.sessions = append(e.sessions, *session)
	}
	return e.result, e.err
}

type streamingRecordingExecutor struct {
	events []codexrunner.StreamEvent
	result ExecutionResult
	err    error
}

func (e *streamingRecordingExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e *streamingRecordingExecutor) RunWithEventHandler(_ context.Context, _ *Session, _ string, handler codexrunner.EventHandler) (ExecutionResult, error) {
	for _, event := range e.events {
		if handler != nil {
			handler(event)
		}
	}
	return e.result, e.err
}

type blockingStreamingExecutor struct {
	started chan struct{}
	release chan struct{}
	result  ExecutionResult
}

func (e *blockingStreamingExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e *blockingStreamingExecutor) RunWithEventHandler(ctx context.Context, _ *Session, _ string, _ codexrunner.EventHandler) (ExecutionResult, error) {
	close(e.started)
	select {
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	case <-e.release:
		return e.result, nil
	}
}

type serialStreamingExecutor struct {
	started chan string
	release chan struct{}
	mu      sync.Mutex
	prompts []string
}

func (e *serialStreamingExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e *serialStreamingExecutor) RunWithEventHandler(ctx context.Context, _ *Session, prompt string, _ codexrunner.EventHandler) (ExecutionResult, error) {
	visible := strings.TrimSpace(StripHelperPromptEchoes(prompt))
	e.mu.Lock()
	e.prompts = append(e.prompts, visible)
	count := len(e.prompts)
	e.mu.Unlock()
	e.started <- visible
	select {
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	case <-e.release:
		return ExecutionResult{Text: fmt.Sprintf("done %d: %s", count, visible), CodexThreadID: "thread-1", CodexTurnID: fmt.Sprintf("turn-%d", count)}, nil
	}
}

func (e *serialStreamingExecutor) promptSnapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.prompts...)
}

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	result  ExecutionResult
}

func (e *blockingExecutor) Run(ctx context.Context, _ *Session, _ string) (ExecutionResult, error) {
	close(e.started)
	select {
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	case <-e.release:
		return e.result, nil
	}
}

type attachmentReadingExecutor struct {
	prompt string
	err    error
}

func (e *attachmentReadingExecutor) Run(_ context.Context, _ *Session, prompt string) (ExecutionResult, error) {
	e.prompt = prompt
	for _, field := range strings.Fields(prompt) {
		if strings.Contains(field, "attachment-001") || strings.Contains(field, "file-001") {
			_, e.err = os.ReadFile(strings.Trim(field, " \t\r\n-()"))
			break
		}
	}
	if e.err != nil {
		return ExecutionResult{}, e.err
	}
	return ExecutionResult{Text: "saw attachment", CodexThreadID: "thread-1", CodexTurnID: "turn-1"}, nil
}

func TestBridgeSessionMessagePersistsTurnRunsAndSendsOutbox(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "plain status reached codex",
		CodexThreadID: "thread-1",
		CodexTurnID:   "turn-1",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)

	err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-1"), "status")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.HasPrefix(got[0], "status\n\n") || !strings.Contains(got[0], ArtifactManifestFenceInfo) {
		t.Fatalf("executor prompts = %#v, want status plus artifact handoff instructions", got)
	}
	if got := len(*sent); got != 2 {
		t.Fatalf("sent message count = %d, want ack plus final", got)
	}
	if !strings.Contains((*sent)[0].Content, "Codex is working") || !strings.Contains((*sent)[1].Content, "plain status reached codex") {
		t.Fatalf("sent content did not include ack and executor output: %#v", *sent)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound count = %d, want 1", got)
	}
	if got := len(state.Turns); got != 1 {
		t.Fatalf("turn count = %d, want 1", got)
	}
	if got := len(state.OutboxMessages); got != 2 {
		t.Fatalf("outbox count = %d, want ack plus final", got)
	}
	var turn teamstore.Turn
	for _, item := range state.Turns {
		turn = item
	}
	if turn.Status != teamstore.TurnStatusCompleted {
		t.Fatalf("turn status = %q, want completed", turn.Status)
	}
	if turn.CodexThreadID != "thread-1" || turn.CodexTurnID != "turn-1" {
		t.Fatalf("turn codex ids = %q/%q, want thread-1/turn-1", turn.CodexThreadID, turn.CodexTurnID)
	}
	var outbox teamstore.OutboxMessage
	for _, item := range state.OutboxMessages {
		outbox = item
	}
	if outbox.Status != teamstore.OutboxStatusSent || outbox.TeamsMessageID == "" {
		t.Fatalf("outbox not marked sent: %#v", outbox)
	}
	if got := bridge.reg.SessionByChatID("chat-1").CodexThreadID; got != "thread-1" {
		t.Fatalf("registry CodexThreadID = %q, want thread-1", got)
	}
}

func TestBridgeStreamsCodexProgressButNotCommandsToTeams(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	exitCode := 0
	executor := &streamingRecordingExecutor{
		events: []codexrunner.StreamEvent{
			{Kind: codexrunner.StreamEventAgentMessage, Text: "I am checking the failing test first."},
			{Kind: codexrunner.StreamEventCommandStarted, Command: "/usr/bin/zsh -lc 'go test ./...'"},
			{Kind: codexrunner.StreamEventCommandCompleted, Command: "/usr/bin/zsh -lc 'go test ./...'", Status: "completed", ExitCode: &exitCode, AggregatedOutput: "--- FAIL: TestAdd\nFAIL\n"},
			{Kind: codexrunner.StreamEventAgentMessage, Text: "FINAL MARKER\nFixed the bug."},
			{Kind: codexrunner.StreamEventTurnCompleted},
		},
		result: ExecutionResult{Text: "FINAL MARKER\nFixed the bug.", CodexThreadID: "thread-1", CodexTurnID: "turn-1"},
	}
	bridge := newBridgeTestBridge(graph, store, executor)

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-stream"), "fix it"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	var plain []string
	for _, msg := range *sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(plain, "\n---\n")
	for _, want := range []string{
		"Codex is working",
		"🤖 ⏳ Codex status:\nI am checking the failing test first.",
		"🔧 Helper: ✅ Codex finished responding.",
		"🤖 ✅ Codex answer:\nFINAL MARKER",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("streamed Teams messages missing %q in:\n%s", want, joined)
		}
	}
	if strings.LastIndex(joined, "🔧 Helper: ✅ Codex finished responding.") < strings.LastIndex(joined, "🤖 ✅ Codex answer:\nFINAL MARKER") {
		t.Fatalf("final helper completion should appear after the Codex reply:\n%s", joined)
	}
	for _, leaked := range []string{
		"🤖 🛠️ Codex command",
		"Running command:",
		"Status: completed",
		"--- FAIL: TestAdd",
		"go test ./...",
	} {
		if strings.Contains(joined, leaked) {
			t.Fatalf("streamed Teams messages leaked command detail %q:\n%s", leaked, joined)
		}
	}
	if strings.Count(joined, "FINAL MARKER") != 1 {
		t.Fatalf("final agent message was duplicated in streamed transcript:\n%s", joined)
	}
}

func TestBridgeSendsCodexIdleStatusWhenStreamIsQuiet(t *testing.T) {
	oldInitial := codexIdleStatusInitialDelay
	oldRepeat := codexIdleStatusRepeatDelay
	oldMessage := codexIdleStatusMessage
	codexIdleStatusInitialDelay = 15 * time.Millisecond
	codexIdleStatusRepeatDelay = time.Hour
	codexIdleStatusMessage = "Still working. No new Codex update yet."
	defer func() {
		codexIdleStatusInitialDelay = oldInitial
		codexIdleStatusRepeatDelay = oldRepeat
		codexIdleStatusMessage = oldMessage
	}()

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &blockingStreamingExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
		result: ExecutionResult{
			Text:          "done after a long quiet step",
			CodexThreadID: "thread-1",
			CodexTurnID:   "turn-1",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	done := make(chan error, 1)
	go func() {
		done <- bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-idle-status"), "run a quiet long task")
	}()

	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("streaming executor did not start")
	}
	waitForOutboxBody(t, store, "Still working. No new Codex update yet.")
	close(executor.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleSessionMessage error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handleSessionMessage did not finish")
	}

	var plain []string
	for _, msg := range *sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(plain, "\n---\n")
	if !strings.Contains(joined, "🤖 ⏳ Codex status:\nStill working. No new Codex update yet.") {
		t.Fatalf("idle status missing or rendered with wrong label:\n%s", joined)
	}
	if strings.Count(joined, "Still working. No new Codex update yet.") != 1 {
		t.Fatalf("idle status should be sent once before repeat interval:\n%s", joined)
	}
	if !strings.Contains(joined, "🤖 ✅ Codex answer:\ndone after a long quiet step") {
		t.Fatalf("final answer missing after idle status:\n%s", joined)
	}
}

func TestBridgeAsyncTurnsQueuesTeamsInputWhileCodexIsRunning(t *testing.T) {
	graph, sent := newBridgeAsyncQueueGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 2),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	ctx := context.Background()

	first := bridgePollMessage("first", "2026-05-03T01:00:00Z", "first prompt")
	if err := bridge.handleSessionMessage(ctx, "chat-1", first, "first prompt"); err != nil {
		t.Fatalf("first handleSessionMessage error: %v", err)
	}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "first prompt") {
			t.Fatalf("first started prompt = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first Codex turn did not start")
	}

	second := bridgePollMessage("second", "2026-05-03T01:00:05Z", "second prompt")
	if err := bridge.handleSessionMessage(ctx, "chat-1", second, "second prompt"); err != nil {
		t.Fatalf("second handleSessionMessage error: %v", err)
	}
	select {
	case got := <-executor.started:
		t.Fatalf("second Codex turn started before first finished: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	waitForOutboxBody(t, store, "Queued. Codex will respond after the current request.")

	executor.release <- struct{}{}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "second prompt") {
			t.Fatalf("second started prompt = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queued second Codex turn did not start after first finished")
	}
	executor.release <- struct{}{}
	waitForCompletedTurnCount(t, store, "s001", 2)
	waitForNoActiveTurnsOrOutbox(t, store, "s001")

	prompts := executor.promptSnapshot()
	if len(prompts) != 2 || !strings.Contains(prompts[0], "first prompt") || !strings.Contains(prompts[1], "second prompt") {
		t.Fatalf("executor prompts = %#v, want first then second", prompts)
	}
	var plain []string
	for _, msg := range *sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(plain, "\n---\n")
	for _, want := range []string{
		"Codex is working. Request accepted.",
		"Queued. Codex will respond after the current request.",
		"🤖 ✅ Codex answer:\ndone 1",
		"🤖 ✅ Codex answer:\ndone 2",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("async queue transcript missing %q in:\n%s", want, joined)
		}
	}
}

func TestBridgeAsyncTurnsSendsSingleQueuedNoticeForBacklog(t *testing.T) {
	graph, sent := newBridgeAsyncQueueGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 3),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	ctx := context.Background()

	first := bridgePollMessage("first", "2026-05-03T01:00:00Z", "first prompt")
	if err := bridge.handleSessionMessage(ctx, "chat-1", first, "first prompt"); err != nil {
		t.Fatalf("first handleSessionMessage error: %v", err)
	}
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first Codex turn did not start")
	}

	for _, item := range []struct {
		id     string
		ts     string
		prompt string
	}{
		{id: "second", ts: "2026-05-03T01:00:05Z", prompt: "second prompt"},
		{id: "third", ts: "2026-05-03T01:00:06Z", prompt: "third prompt"},
	} {
		msg := bridgePollMessage(item.id, item.ts, item.prompt)
		if err := bridge.handleSessionMessage(ctx, "chat-1", msg, item.prompt); err != nil {
			t.Fatalf("%s handleSessionMessage error: %v", item.id, err)
		}
		select {
		case got := <-executor.started:
			t.Fatalf("%s Codex turn started before first finished: %q", item.id, got)
		case <-time.After(50 * time.Millisecond):
		}
	}

	var plain []string
	for _, msg := range *sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(plain, "\n---\n")
	if got := strings.Count(joined, "Queued. Codex will respond after the current request."); got != 1 {
		t.Fatalf("queued notice count = %d, want 1 in:\n%s", got, joined)
	}

	executor.release <- struct{}{}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "second prompt") {
			t.Fatalf("second started prompt = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued second Codex turn did not start after first finished")
	}
	executor.release <- struct{}{}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "third prompt") {
			t.Fatalf("third started prompt = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued third Codex turn did not start after second finished")
	}
	executor.release <- struct{}{}
	waitForCompletedTurnCount(t, store, "s001", 3)
	waitForNoActiveTurnsOrOutbox(t, store, "s001")

	prompts := executor.promptSnapshot()
	if len(prompts) != 3 ||
		!strings.Contains(prompts[0], "first prompt") ||
		!strings.Contains(prompts[1], "second prompt") ||
		!strings.Contains(prompts[2], "third prompt") {
		t.Fatalf("executor prompts = %#v, want first, second, third", prompts)
	}
}

func TestBridgeAsyncTurnsIgnoresPromptlessAdaptiveCardWhileRunning(t *testing.T) {
	graph, sent := newBridgeAsyncQueueGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 2),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	ctx := context.Background()

	first := bridgePollMessage("first", "2026-05-03T01:00:00Z", "first prompt")
	if err := bridge.handleSessionMessage(ctx, "chat-1", first, "first prompt"); err != nil {
		t.Fatalf("first handleSessionMessage error: %v", err)
	}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("first Codex turn did not start")
	}

	card := bridgeTestMessageWithText("adaptive-card-only", `<attachment id="card-1"></attachment>`)
	card.Attachments = []MessageAttachment{{
		ID:          "card-1",
		ContentType: "application/vnd.microsoft.card.adaptive",
		Name:        "Open Codex chat",
	}}
	if err := bridge.handleSessionMessage(ctx, "chat-1", card, ""); err != nil {
		t.Fatalf("adaptive card-only handleSessionMessage error: %v", err)
	}
	if strings.Contains(sentPlainJoined(*sent), "Queued. Codex will respond after the current request.") {
		t.Fatalf("adaptive card-only message should not send queued ack: %#v", *sent)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := queuedTurnCountForSession(state, "s001"); got != 0 {
		t.Fatalf("queued turn count = %d, want 0", got)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound event count = %d, want only the first real prompt", got)
	}

	executor.release <- struct{}{}
	waitForCompletedTurnCount(t, store, "s001", 1)
	waitForNoActiveTurnsOrOutbox(t, store, "s001")
	prompts := executor.promptSnapshot()
	if len(prompts) != 1 || !strings.Contains(prompts[0], "first prompt") {
		t.Fatalf("executor prompts = %#v, want only first prompt", prompts)
	}
}

func waitForNoActiveTurnsOrOutbox(t *testing.T, store *teamstore.Store, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	stable := 0
	for {
		state, err := store.Load(context.Background())
		if err != nil {
			t.Fatalf("load store while waiting for idle turn queue: %v", err)
		}
		active := false
		for _, turn := range state.Turns {
			if turn.SessionID != sessionID {
				continue
			}
			if turn.Status == teamstore.TurnStatusQueued || turn.Status == teamstore.TurnStatusRunning {
				active = true
				break
			}
		}
		if !active {
			for _, msg := range state.OutboxMessages {
				if msg.SessionID != sessionID {
					continue
				}
				if msg.Status == teamstore.OutboxStatusQueued || msg.Status == teamstore.OutboxStatusSending || msg.Status == teamstore.OutboxStatusAccepted {
					active = true
					break
				}
			}
		}
		if !active {
			stable++
			if stable >= 3 {
				return
			}
		} else {
			stable = 0
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for idle turn queue; turns=%#v outbox=%#v", state.Turns, state.OutboxMessages)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBridgeSuppressesQueuedCodexCommandOutbox(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	ctx := context.Background()

	queued, err := bridge.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox-codex-command",
		SessionID:   "session-1",
		TurnID:      "turn-1",
		TeamsChatID: "chat-1",
		Kind:        "codex-command-001",
		Body:        "Running command:\ngo test ./...",
	})
	if err != nil {
		t.Fatalf("queueOutbox error: %v", err)
	}
	if queued.Status != teamstore.OutboxStatusQueued {
		t.Fatalf("queued status = %q", queued.Status)
	}
	if err := bridge.flushPendingOutboxForChat(ctx, "chat-1"); err != nil {
		t.Fatalf("flushPendingOutboxForChat error: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("queued Codex command outbox should not be sent: %#v", *sent)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if got := state.OutboxMessages["outbox-codex-command"].Status; got != teamstore.OutboxStatusSent {
		t.Fatalf("suppressed command outbox status = %q, want sent", got)
	}
}

func waitForOutboxBody(t *testing.T, store *teamstore.Store, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		state, err := store.Load(context.Background())
		if err != nil {
			t.Fatalf("load store while waiting for outbox: %v", err)
		}
		for _, msg := range state.OutboxMessages {
			if strings.Contains(msg.Body, want) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for outbox body %q; outbox=%#v", want, state.OutboxMessages)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForCompletedTurnCount(t *testing.T, store *teamstore.Store, sessionID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		state, err := store.Load(context.Background())
		if err != nil {
			t.Fatalf("load store while waiting for completed turns: %v", err)
		}
		var got int
		for _, turn := range state.Turns {
			if turn.SessionID == sessionID && turn.Status == teamstore.TurnStatusCompleted {
				got++
			}
		}
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d completed turns; got %d; turns=%#v", want, got, state.Turns)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestShouldSuppressCodexCommandOutbox(t *testing.T) {
	for _, kind := range []string{"codex-command-001", "CODEX-COMMAND-123"} {
		if !shouldSuppressCodexCommandOutbox(kind) {
			t.Fatalf("should suppress %q", kind)
		}
	}
	for _, kind := range []string{"command-help", "error", "sync-status-1", "teams_session_command_deferred"} {
		if shouldSuppressCodexCommandOutbox(kind) {
			t.Fatalf("should not suppress helper/admin kind %q", kind)
		}
	}
}

func TestUserAnnotatedMessageHTMLPrefixesSenderOnSeparateLine(t *testing.T) {
	msg := bridgePollMessage("message-1", "2026-04-30T01:00:00Z", "fix this")
	msg.From.User.DisplayName = "Jason Wei"

	got, ok := userAnnotatedMessageHTML(msg, User{ID: "user-1", DisplayName: "Fallback"})
	if !ok {
		t.Fatal("userAnnotatedMessageHTML returned ok=false")
	}
	plain := PlainTextFromTeamsHTML(got)
	if !strings.HasPrefix(plain, "🧑‍💻 User:\nfix this") {
		t.Fatalf("annotated plain text = %q", plain)
	}
	if prompt := promptTextFromTeamsMessageHTML(got); prompt != "fix this" {
		t.Fatalf("prompt text after stripping annotation = %q", prompt)
	}
	if _, ok := userAnnotatedMessageHTML(ChatMessage{Body: struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	}{ContentType: "html", Content: got}}, User{}); ok {
		t.Fatal("already annotated message should not be annotated again")
	}
}

func TestUserAnnotatedMessageHTMLSkipsPromptlessAdaptiveCard(t *testing.T) {
	msg := bridgeTestMessageWithText("card-only", `<attachment id="card-1"></attachment>`)
	msg.Attachments = []MessageAttachment{{
		ID:          "card-1",
		ContentType: "application/vnd.microsoft.card.adaptive",
		Name:        "Open Codex chat",
	}}
	if !isPromptlessTeamsAttachmentPlaceholderMessage(msg) {
		t.Fatal("adaptive card-only message should be treated as promptless attachment placeholder")
	}
	if _, ok := userAnnotatedMessageHTML(msg, User{ID: "user-1"}); ok {
		t.Fatal("adaptive card-only message should not be annotated")
	}

	withoutAttachmentPayload := msg
	withoutAttachmentPayload.Attachments = nil
	if !isPromptlessTeamsAttachmentPlaceholderMessage(withoutAttachmentPayload) {
		t.Fatal("attachment placeholder-only message should be treated as promptless even when Graph omits attachments")
	}
	if _, ok := userAnnotatedMessageHTML(withoutAttachmentPayload, User{ID: "user-1"}); ok {
		t.Fatal("attachment placeholder-only message should not be annotated")
	}
}

func TestBridgeSessionUnknownSlashIsForwardedToCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "checked path",
		CodexThreadID: "thread-1",
		CodexTurnID:   "turn-1",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("slash-path"), "/tmp/a.log 这个文件是什么"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.Contains(got[0], "/tmp/a.log") {
		t.Fatalf("executor prompts = %#v, want slash path prompt", got)
	}
	if len(*sent) != 2 || !strings.Contains((*sent)[0].Content, "Codex is working") || !strings.Contains((*sent)[1].Content, "checked path") {
		t.Fatalf("sent = %#v, want ack and final", *sent)
	}
}

func TestBridgeSessionMessageAllowsEmptyCodexTurnID(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "completed without codex turn id",
		CodexThreadID: "thread-1",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)

	err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-1"), "status")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := len(*sent); got != 2 {
		t.Fatalf("sent message count = %d, want ack plus final", got)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	var turn teamstore.Turn
	for _, item := range state.Turns {
		turn = item
	}
	if turn.Status != teamstore.TurnStatusCompleted {
		t.Fatalf("turn status = %q, want completed", turn.Status)
	}
	if turn.CodexThreadID != "thread-1" || turn.CodexTurnID != "" {
		t.Fatalf("turn codex ids = %q/%q, want thread-1/empty", turn.CodexThreadID, turn.CodexTurnID)
	}
	session := state.Sessions["s001"]
	if session.CodexThreadID != "thread-1" || session.LatestCodexTurnID != "" {
		t.Fatalf("session codex ids = %q/%q, want thread-1/empty", session.CodexThreadID, session.LatestCodexTurnID)
	}
}

func TestBridgeQueuesAllLongOutputPartsBeforeFirstSend(t *testing.T) {
	store := newBridgeTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateSession(ctx, teamstore.SessionContext{
		ID:          "s001",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "chat-1",
	}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	text := strings.Repeat("chunk-data ", 9000)
	expectedParts := len(splitTextChunksForHTMLMessage("Codex", text, teamsChunkHTMLContentBytes))
	if expectedParts < 2 {
		t.Fatalf("test text produced %d chunks, want at least 2", expectedParts)
	}
	var firstSendChecked bool
	var sent []bridgeSentMessage
	var handlerErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			handlerErr = fmt.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
			http.Error(w, handlerErr.Error(), http.StatusInternalServerError)
			return
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
			Mentions []json.RawMessage `json:"mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			handlerErr = err
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !firstSendChecked {
			firstSendChecked = true
			state, err := store.Load(context.Background())
			if err != nil {
				handlerErr = err
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(state.OutboxMessages) != expectedParts {
				handlerErr = fmt.Errorf("outbox parts before first send = %d, want %d", len(state.OutboxMessages), expectedParts)
				http.Error(w, handlerErr.Error(), http.StatusInternalServerError)
				return
			}
		}
		chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
		sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.queueAndSendOutboxChunks(ctx, "s001", "turn-1", "chat-1", "final", text); err != nil {
		t.Fatalf("queueAndSendOutboxChunks error: %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("handler error: %v", handlerErr)
	}
	if len(sent) != expectedParts {
		t.Fatalf("sent parts = %d, want %d", len(sent), expectedParts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	sourceHash := normalizedTextHash(text)
	for _, msg := range state.OutboxMessages {
		if msg.Status != teamstore.OutboxStatusSent || msg.Sequence <= 0 || msg.PartCount != expectedParts || msg.PartIndex <= 0 || msg.SourceTextHash != sourceHash {
			t.Fatalf("outbox part metadata mismatch: %#v", msg)
		}
	}
	hashes := deliveredTranscriptHashes(state, "s001")
	if !shouldSkipDeliveredTranscriptRecord(TranscriptRecord{Kind: TranscriptKindAssistant}, text, hashes) {
		t.Fatal("chunked delivered final should dedupe the later full transcript record")
	}
}

func TestBridgeOutboxMentionOwnerUsesGraphMentionPayload(t *testing.T) {
	store := newBridgeTestStore(t)
	ctx := context.Background()
	var sawMention bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		var payload struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
			Mentions []json.RawMessage `json:"mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		sawMention = strings.Contains(payload.Body.Content, `<at id="0">`) && len(payload.Mentions) == 1
		_, _ = fmt.Fprint(w, `{"id":"sent-1","messageType":"message"}`)
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.user.DisplayName = "Owner"

	if err := bridge.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:           "outbox:notify",
		TeamsChatID:  "chat-1",
		Kind:         "notification",
		Body:         "long turn completed",
		MentionOwner: true,
	}); err != nil {
		t.Fatalf("queueAndSendOutbox error: %v", err)
	}
	if !sawMention {
		t.Fatal("expected Graph mention payload")
	}
}

func TestBridgeOutboxUsesRendererForNonMentionMessages(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, EchoExecutor{})
	if err := bridge.queueAndSendOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "outbox:rendered-final",
		SessionID:   "s001",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "done <b>visible</b>",
	}); err != nil {
		t.Fatalf("queueAndSendOutbox error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent count = %d, want 1", len(*sent))
	}
	if !strings.Contains((*sent)[0].Content, "<strong>🤖 ✅ Codex answer:</strong><br>") || strings.Contains((*sent)[0].Content, "<b>visible</b>") {
		t.Fatalf("rendered outbox content = %q", (*sent)[0].Content)
	}
	if strings.Index((*sent)[0].Content, "done") > strings.Index((*sent)[0].Content, "Codex finished responding") {
		t.Fatalf("rendered outbox content = %q", (*sent)[0].Content)
	}
}

func TestBridgeFinalOutboxStripsOAIMemoryCitationBeforeQueue(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, EchoExecutor{})
	text := strings.Join([]string{
		"visible answer",
		"",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[confirmed codex-helper repo context]",
		"</citation_entries>",
		"<rollout_ids>",
		"019d4393-5109-7b10-b5c2-05b2fe8635ba",
		"</rollout_ids>",
		"</oai-mem-citation>",
	}, "\n")
	if err := bridge.queueAndSendOutboxChunks(context.Background(), "s001", "turn-1", "chat-1", "final", text); err != nil {
		t.Fatalf("queueAndSendOutboxChunks error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent count = %d, want 1", len(*sent))
	}
	sentPlain := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(sentPlain, "visible answer") {
		t.Fatalf("sent message lost visible answer: %q", sentPlain)
	}
	for _, forbidden := range []string{"oai-mem-citation", "citation_entries", "MEMORY.md", "rollout_ids"} {
		if strings.Contains(sentPlain, forbidden) {
			t.Fatalf("sent message leaked %q: %q", forbidden, sentPlain)
		}
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if len(state.OutboxMessages) != 1 {
		t.Fatalf("outbox count = %d, want 1", len(state.OutboxMessages))
	}
	for _, msg := range state.OutboxMessages {
		if !strings.Contains(msg.Body, "visible answer") {
			t.Fatalf("queued outbox lost visible answer: %#v", msg)
		}
		for _, forbidden := range []string{"oai-mem-citation", "citation_entries", "MEMORY.md", "rollout_ids"} {
			if strings.Contains(msg.Body, forbidden) {
				t.Fatalf("queued outbox leaked %q: %#v", forbidden, msg)
			}
		}
	}
}

func TestBridgeLongRunningTurnKeepsOwnerHeartbeatActive(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &blockingExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
		result: ExecutionResult{
			Text:          "done",
			CodexThreadID: "thread-1",
			CodexTurnID:   "turn-1",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	owner, err := teamstore.CurrentOwner("v-test", "", "", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	bridge.setOwner(owner, time.Minute)
	bridge.ownerMu.Lock()
	bridge.ownerHeartbeatInterval = 5 * time.Millisecond
	bridge.ownerMu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-1"), "long task")
	}()
	select {
	case <-executor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}

	var activeOwner teamstore.OwnerMetadata
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		read, ok, err := store.ReadOwner(context.Background())
		if err != nil {
			t.Fatalf("ReadOwner error: %v", err)
		}
		if ok && read.ActiveSessionID == "s001" && read.ActiveTurnID != "" {
			activeOwner = read
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if activeOwner.ActiveSessionID != "s001" || activeOwner.ActiveTurnID == "" {
		t.Fatalf("owner was not marked active during turn: %#v", activeOwner)
	}
	contender := activeOwner
	contender.PID++
	if _, err := store.RecordOwnerHeartbeat(context.Background(), contender, time.Minute, time.Now()); !errors.Is(err, teamstore.ErrOwnerLive) {
		t.Fatalf("contender RecordOwnerHeartbeat error = %v, want ErrOwnerLive", err)
	}
	firstHeartbeat := activeOwner.LastHeartbeat
	time.Sleep(20 * time.Millisecond)
	read, ok, err := store.ReadOwner(context.Background())
	if err != nil {
		t.Fatalf("ReadOwner after heartbeat error: %v", err)
	}
	if !ok {
		t.Fatal("owner missing during turn")
	}
	if !read.LastHeartbeat.After(firstHeartbeat) {
		t.Fatalf("owner heartbeat did not advance during turn: before=%s after=%s", firstHeartbeat, read.LastHeartbeat)
	}

	close(executor.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleSessionMessage error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handleSessionMessage did not finish")
	}
	read, ok, err = store.ReadOwner(context.Background())
	if err != nil {
		t.Fatalf("ReadOwner after turn error: %v", err)
	}
	if !ok {
		t.Fatal("owner missing after direct turn")
	}
	if read.ActiveSessionID != "" || read.ActiveTurnID != "" {
		t.Fatalf("owner active fields not cleared after turn: %#v", read)
	}
}

func TestBridgeIdleOwnerHeartbeatPreservesActiveTurnFields(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, EchoExecutor{})
	owner, err := teamstore.CurrentOwner("v-test", "s001", "turn-1", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	bridge.setOwner(owner, time.Minute)
	bridge.ownerMu.Lock()
	bridge.ownerHeartbeatInterval = 5 * time.Millisecond
	bridge.ownerMu.Unlock()
	if err := bridge.recordOwnerHeartbeat(context.Background(), "s001", "turn-1"); err != nil {
		t.Fatalf("recordOwnerHeartbeat error: %v", err)
	}
	first, ok, err := store.ReadOwner(context.Background())
	if err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	}
	if !ok {
		t.Fatal("owner missing after initial heartbeat")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := bridge.startOwnerHeartbeat(ctx)
	defer func() {
		cancel()
		<-done
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		read, ok, err := store.ReadOwner(context.Background())
		if err != nil {
			t.Fatalf("ReadOwner after idle heartbeat error: %v", err)
		}
		if ok && read.LastHeartbeat.After(first.LastHeartbeat) {
			if read.ActiveSessionID != "s001" || read.ActiveTurnID != "turn-1" {
				t.Fatalf("idle heartbeat cleared active fields: %#v", read)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("idle owner heartbeat did not advance")
}

func TestBridgeSessionSlashCommandsDoNotRunCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)

	err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-1"), "/status")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "STATUS: Work chat") || !strings.Contains((*sent)[0].Content, "Chat: listening") || !strings.Contains((*sent)[0].Content, "Codex status: will start when you send a task") {
		t.Fatalf("status response = %q", (*sent)[0].Content)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 0 {
		t.Fatalf("slash command inbound count = %d, want 0", got)
	}
}

func TestBridgeControlWorkspacesAndSessionsDoNotRunCodex(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("control dashboard invoked Codex: %#v", executor.prompts)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-3"), "/details 1"); err != nil {
		t.Fatalf("/details session error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent count = %d, want 3", len(*sent))
	}
	workspaceDashboard := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(workspaceDashboard, "1 - /home/user/project/alpha") {
		t.Fatalf("dashboard output missing numbered absolute workspace path: %q", workspaceDashboard)
	}
	sessionDashboard := PlainTextFromTeamsHTML((*sent)[1].Content)
	if !strings.Contains(sessionDashboard, "Workspace: /home/user/project/alpha") {
		t.Fatalf("session dashboard should name selected workspace: %q", sessionDashboard)
	}
	if !strings.Contains(sessionDashboard, "fix alpha") || !strings.Contains(sessionDashboard, "c 1") {
		t.Fatalf("session dashboard missing title/action: %q", sessionDashboard)
	}
	if strings.Contains(sessionDashboard, "thread-alpha") {
		t.Fatalf("session dashboard leaked raw session id: %q", sessionDashboard)
	}
	details := PlainTextFromTeamsHTML((*sent)[2].Content)
	if !strings.Contains(details, "Codex session ID: thread-alpha") || !strings.Contains(details, "Working directory: /home/user/project/alpha") {
		t.Fatalf("/details should expose technical ids on demand, got %q", details)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if _, ok := state.DashboardViews["control-chat"]; !ok {
		t.Fatalf("dashboard view was not persisted: %#v", state.DashboardViews)
	}
}

func TestBridgeControlOpenRawLocalSessionSuggestsPublish(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-open-local"), "/open thread-alpha"); err != nil {
		t.Fatalf("/open local session error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent count = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(got, "not in Teams yet") || !strings.Contains(got, "choose its number") {
		t.Fatalf("/open raw local session response = %q", got)
	}
	if strings.Contains(got, "thread-alpha") {
		t.Fatalf("/open raw local session leaked raw session id: %q", got)
	}
}

func TestBridgeControlWorkspaceListDoesNotShowWorkspaceFingerprint(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key: "p1",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-workspace-fallback"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent count = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if strings.Contains(got, "workspace:") {
		t.Fatalf("workspace dashboard leaked workspace fingerprint: %q", got)
	}
	if !strings.Contains(got, "1 - Unknown workspace") || !strings.Contains(got, "Path: not recorded by Codex") {
		t.Fatalf("workspace dashboard missing unknown-workspace guidance: %q", got)
	}
}

func TestBridgeControlClosedLinkedSessionIsNotPresentedAsOpenable(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.reg.Sessions[0].Status = "closed"
	bridge.reg.Sessions[0].CodexThreadID = "thread-alpha"

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-workspaces"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-workspace"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-select"), "/open 1"); err != nil {
		t.Fatalf("open closed session error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-status"), "/status"); err != nil {
		t.Fatalf("/status error: %v", err)
	}
	if len(*sent) != 4 {
		t.Fatalf("sent count = %d, want 4", len(*sent))
	}
	sessions := PlainTextFromTeamsHTML((*sent)[1].Content)
	if !strings.Contains(sessions, "closed Teams chat") || strings.Contains(sessions, "Teams ready") {
		t.Fatalf("sessions output = %q, want closed guidance", sessions)
	}
	selection := PlainTextFromTeamsHTML((*sent)[2].Content)
	if !strings.Contains(selection, "closed Teams work chat") || !strings.Contains(selection, "continue 1") {
		t.Fatalf("closed session selection = %q", selection)
	}
	status := PlainTextFromTeamsHTML((*sent)[3].Content)
	if !strings.Contains(status, "no active linked work chats") || strings.Contains(status, "https://teams.example/chat-1") {
		t.Fatalf("control status = %q, want closed chats hidden", status)
	}
}

func TestParseNewSessionRequestUsesDirectoryWithOptionalCompatTitle(t *testing.T) {
	tmp := t.TempDir()
	got, err := parseNewSessionRequest(tmp)
	if err != nil {
		t.Fatalf("parse directory request: %v", err)
	}
	if got.WorkDir != tmp || got.Title != "" {
		t.Fatalf("directory request parsed as %#v", got)
	}

	got, err = parseNewSessionRequest(tmp + " -- inspect build")
	if err != nil {
		t.Fatalf("parse directory request with optional title: %v", err)
	}
	if got.WorkDir != tmp || got.Title != "inspect build" {
		t.Fatalf("directory request with optional title parsed as %#v", got)
	}

	quoted := strconv.Quote(filepath.Join(tmp, "dir with spaces"))
	got, err = parseNewSessionRequest(quoted + " -- inspect quoted build")
	if err != nil {
		t.Fatalf("parse quoted directory request: %v", err)
	}
	if got.WorkDir != filepath.Join(tmp, "dir with spaces") || got.Title != "inspect quoted build" {
		t.Fatalf("quoted directory request parsed as %#v", got)
	}

	got, err = parseNewSessionRequest("")
	if err != nil {
		t.Fatalf("empty request should be resolved by selected workspace later: %v", err)
	}
	if got.WorkDir != "" || got.Title != "" {
		t.Fatalf("empty request parsed as %#v, want empty for selected workspace fallback", got)
	}
}

func TestBridgeControlWorkspaceSessionsOnlyShowsSelectedWorkspace(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "alpha",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}, {
			Key:  "beta",
			Path: "/home/user/project/beta",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-beta",
				FirstPrompt: "fix beta",
				ProjectPath: "/home/user/project/beta",
				ModifiedAt:  time.Date(2026, 4, 30, 13, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if len(*sent) != 2 {
		t.Fatalf("sent = %#v, want workspace and selected-session dashboard", *sent)
	}
	sessions := PlainTextFromTeamsHTML((*sent)[1].Content)
	if !strings.Contains(sessions, "fix alpha") || strings.Contains(sessions, "fix beta") {
		t.Fatalf("selected workspace sessions output = %q", sessions)
	}
	if strings.Contains(sessions, "thread-alpha") || strings.Contains(sessions, "thread-beta") {
		t.Fatalf("selected workspace sessions leaked raw ids: %q", sessions)
	}
	if strings.Count(sessions, "1 -") != 1 {
		t.Fatalf("session numbering should be scoped to selected workspace, got %q", sessions)
	}
}

func TestBridgeDashboardNumbersSurviveViewRoundTrip(t *testing.T) {
	var call int
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		call++
		alpha := codexhistory.Project{Key: "alpha", Path: "/home/user/project/alpha"}
		beta := codexhistory.Project{Key: "beta", Path: "/home/user/project/beta", Sessions: []codexhistory.Session{{
			SessionID:   "thread-beta",
			FirstPrompt: "fix beta",
			ProjectPath: "/home/user/project/beta",
			ModifiedAt:  time.Date(2026, 4, 30, 13, 0, 0, 0, time.UTC),
		}}}
		if call >= 3 {
			return []codexhistory.Project{beta, alpha}, nil
		}
		return []codexhistory.Project{alpha, beta}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 2"); err != nil {
		t.Fatalf("/workspace 2 error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-3"), "/workspaces"); err != nil {
		t.Fatalf("second /workspaces error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent = %#v, want 3 dashboard messages", *sent)
	}
	refreshed := PlainTextFromTeamsHTML((*sent)[2].Content)
	if !strings.Contains(refreshed, "1 - /home/user/project/alpha") || !strings.Contains(refreshed, "2 - /home/user/project/beta") {
		t.Fatalf("workspace numbers changed after sessions view round trip: %q", refreshed)
	}
}

func TestBridgeDashboardEmptyWorkspaceKeepsSessionsContext(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "empty",
			Path: "/home/user/project/empty",
		}, {
			Key:  "beta",
			Path: "/home/user/project/beta",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-beta",
				FirstPrompt: "fix beta",
				ProjectPath: "/home/user/project/beta",
				ModifiedAt:  time.Date(2026, 4, 30, 13, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace 1 error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-3"), "/sessions"); err != nil {
		t.Fatalf("/sessions error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent = %#v, want 3 dashboard messages", *sent)
	}
	got := PlainTextFromTeamsHTML((*sent)[2].Content)
	if !strings.Contains(got, "No local Codex sessions") || strings.Contains(got, "thread-beta") {
		t.Fatalf("/sessions lost empty workspace context: %q", got)
	}
}

func TestBridgeControlUnknownTextFallsBackToCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "fallback answer",
		CodexThreadID: "control-thread-1",
		CodexTurnID:   "control-turn-1",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor

	msg := bridgePollMessage("control-unknown-1", "2026-04-30T01:00:00Z", "帮我看看现在该怎么操作")
	if err := bridge.handleControlMessage(context.Background(), msg, "帮我看看现在该怎么操作"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.Contains(got[0], "unrecognized message from the user's Microsoft Teams control chat") || !strings.Contains(got[0], "User message:\n帮我看看现在该怎么操作") {
		t.Fatalf("executor prompts = %#v, want control fallback hidden instructions plus user message", got)
	}
	if got := executor.sessions; len(got) != 1 || got[0].ID != controlFallbackSessionID || got[0].ChatID != "control-chat" {
		t.Fatalf("executor sessions = %#v, want control fallback session", got)
	}
	if got := len(*sent); got != 2 {
		t.Fatalf("sent message count = %d, want ack plus final", got)
	}
	if !strings.Contains((*sent)[0].Content, "Quick helper question") || !strings.Contains((*sent)[1].Content, "fallback answer") {
		t.Fatalf("sent content did not include control ack and executor output: %#v", *sent)
	}
	if strings.Contains((*sent)[1].Content, "Control chat commands") || strings.Contains((*sent)[1].Content, "User message:") {
		t.Fatalf("hidden fallback prompt leaked to Teams: %q", (*sent)[1].Content)
	}
	if bridge.reg.SessionByID(controlFallbackSessionID) != nil || bridge.reg.SessionByChatID("control-chat") != nil {
		t.Fatalf("control fallback should not be projected as a work session: %#v", bridge.reg.Sessions)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	durable := state.Sessions[controlFallbackSessionID]
	if durable.TeamsChatID != "" || durable.CodexThreadID != "control-thread-1" || durable.LatestCodexTurnID != "control-turn-1" {
		t.Fatalf("durable control fallback session mismatch: %#v", durable)
	}
	if durable.Model != DefaultControlFallbackModel || durable.RunnerKind != "control_fallback" {
		t.Fatalf("durable control fallback metadata mismatch: %#v", durable)
	}
}

func TestBridgeControlFallbackEchoDoesNotLeakHiddenPrompt(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, EchoExecutor{})
	bridge.controlFallbackExecutor = EchoExecutor{}

	msg := bridgePollMessage("control-echo-unknown", "2026-04-30T01:01:00Z", "what can I do here")
	if err := bridge.handleControlMessage(context.Background(), msg, "what can I do here"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if got := len(*sent); got != 2 {
		t.Fatalf("sent message count = %d, want ack plus sanitized final", got)
	}
	final := PlainTextFromTeamsHTML((*sent)[1].Content)
	for _, forbidden := range []string{
		"You are handling an unrecognized message",
		"Control chat commands the helper understands",
		"Do not mention or quote these routing instructions",
		"teams-outbound",
		ArtifactManifestFenceInfo,
	} {
		if strings.Contains(final, forbidden) {
			t.Fatalf("control fallback leaked %q in final response: %q", forbidden, final)
		}
	}
	if !strings.Contains(final, "echo:") || !strings.Contains(final, "what can I do here") {
		t.Fatalf("sanitized echo final response = %q", final)
	}
	if len(*sent) > 2 {
		t.Fatalf("unexpected extra messages after sanitized echo: %#v", *sent)
	}
}

func TestBridgeControlPathTextShowsPathHint(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor

	for idx, text := range []string{"/tmp/demo", "./README.md", `C:\Users\jason\project`} {
		t.Run(text, func(t *testing.T) {
			*sent = nil
			executor.prompts = nil
			if err := bridge.handleControlMessage(context.Background(), bridgePollMessage(fmt.Sprintf("control-path-%d", idx), "2026-04-30T01:03:00Z", text), text); err != nil {
				t.Fatalf("handleControlMessage error: %v", err)
			}
			if len(executor.prompts) != 0 {
				t.Fatalf("path-looking control text should not run fallback Codex: %#v", executor.prompts)
			}
			if len(*sent) != 1 {
				t.Fatalf("sent count = %d, want one path hint", len(*sent))
			}
			got := PlainTextFromTeamsHTML((*sent)[0].Content)
			if !strings.Contains(got, "Detected path") || !strings.Contains(got, "new "+quoteTeamsCommandPath(text)) || strings.Contains(got, " -- ") {
				t.Fatalf("path hint = %q", got)
			}
		})
	}
}

func TestBridgeControlAskUsesExplicitFallbackText(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "ask answer",
		CodexThreadID: "control-thread-ask",
		CodexTurnID:   "control-turn-ask",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor

	if err := bridge.handleControlMessage(context.Background(), bridgePollMessage("control-ask-1", "2026-04-30T01:00:00Z", "/ask what should I do"), "/ask what should I do"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.Contains(got[0], "User message:\nwhat should I do") || strings.Contains(got[0], "/ask what should I do") {
		t.Fatalf("executor prompts = %#v, want explicit /ask argument only", got)
	}
	if len(*sent) != 2 || !strings.Contains((*sent)[0].Content, "Quick helper question") || !strings.Contains((*sent)[1].Content, "ask answer") {
		t.Fatalf("sent content did not include explicit ask ACK and answer: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	var inbound teamstore.InboundEvent
	for _, item := range state.InboundEvents {
		inbound = item
	}
	if inbound.Text != "what should I do" {
		t.Fatalf("inbound text = %q, want /ask argument only", inbound.Text)
	}
}

func TestBridgeControlUnknownSlashShowsHelpWithoutCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-unknown-slash"), "/workspces"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "unknown control command") || !strings.Contains((*sent)[0].Content, "projects") {
		t.Fatalf("unknown slash response = %#v", *sent)
	}
}

func TestBridgeControlWorkOnlyHelperCommandExplainsCorrectChat(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("control-helper-file", "helper file report.md"), "helper file report.md"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent message count = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(got, "control chat") || !strings.Contains(got, "helper ...") || !strings.Contains(got, "Work chat") || !strings.Contains(got, "new <directory>") {
		t.Fatalf("wrong-chat helper response = %q", got)
	}
}

func TestBridgeControlHelpIsFirstClassCommand(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-help"), "help"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	helpText := PlainTextFromTeamsHTML((*sent)[0].Content)
	if len(*sent) != 1 || strings.Contains(helpText, "unknown control command") || !strings.Contains(helpText, "Start here") || !strings.Contains(helpText, "new <directory>") || !strings.Contains(helpText, "continue <number>") || !strings.Contains(helpText, "help advanced") || strings.Contains(helpText, "cx ") {
		t.Fatalf("control help response = %#v", *sent)
	}
}

func TestBridgeControlRestartRequiresConfirmationAndRunsRestarter(t *testing.T) {
	prevDelay := helperRestartDelay
	helperRestartDelay = 0
	t.Cleanup(func() { helperRestartDelay = prevDelay })

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor
	restarted := make(chan struct{}, 1)
	bridge.helperRestarter = func(context.Context) error {
		restarted <- struct{}{}
		return nil
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-restart-help"), "helper restart"); err != nil {
		t.Fatalf("handleControlMessage restart help error: %v", err)
	}
	select {
	case <-restarted:
		t.Fatal("helper restart without now should not restart")
	default:
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "helper restart now") {
		t.Fatalf("restart confirmation response = %#v", *sent)
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-restart-now"), "helper restart now"); err != nil {
		t.Fatalf("handleControlMessage restart now error: %v", err)
	}
	select {
	case <-restarted:
	case <-time.After(time.Second):
		t.Fatal("helper restart now did not call restarter")
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	if len(*sent) != 2 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[1].Content), "restart scheduled") {
		t.Fatalf("restart scheduled response = %#v", *sent)
	}
}

func TestBridgeControlRestartReportsUnavailableWhenNotConfigured(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-restart-unavailable"), "helper restart now"); err != nil {
		t.Fatalf("handleControlMessage restart unavailable error: %v", err)
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "not available") {
		t.Fatalf("restart unavailable response = %#v", *sent)
	}
}

func TestBridgeControlRestartNowDoesNotInterruptActiveWork(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	restarted := make(chan struct{}, 1)
	bridge.helperRestarter = func(context.Context) error {
		restarted <- struct{}{}
		return nil
	}
	if _, _, err := store.CreateSession(context.Background(), teamstore.SessionContext{ID: "s1", Status: teamstore.SessionStatusActive}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-active", SessionID: "s1", Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-restart-active"), "helper restart now"); err != nil {
		t.Fatalf("handleControlMessage restart active error: %v", err)
	}
	select {
	case <-restarted:
		t.Fatal("helper restart now should not restart while work is active")
	default:
	}
	if len(*sent) != 1 {
		t.Fatalf("sent message count = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(got, "Codex work is still active") || !strings.Contains(got, "helper restart force") {
		t.Fatalf("active-work restart response = %q", got)
	}
}

func TestBridgeControlRestartForceCanInterruptActiveWork(t *testing.T) {
	prevDelay := helperRestartDelay
	helperRestartDelay = 0
	t.Cleanup(func() { helperRestartDelay = prevDelay })

	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	restarted := make(chan struct{}, 1)
	bridge.helperRestarter = func(context.Context) error {
		restarted <- struct{}{}
		return nil
	}
	if _, _, err := store.CreateSession(context.Background(), teamstore.SessionContext{ID: "s1", Status: teamstore.SessionStatusActive}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-active", SessionID: "s1", Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-restart-force"), "helper restart force"); err != nil {
		t.Fatalf("handleControlMessage restart force error: %v", err)
	}
	select {
	case <-restarted:
	case <-time.After(time.Second):
		t.Fatal("helper restart force did not call restarter")
	}
}

func TestBridgeControlRestartDoesNotRunDuringHelperUpgrade(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	restarted := make(chan struct{}, 1)
	bridge.helperRestarter = func(context.Context) error {
		restarted <- struct{}{}
		return nil
	}
	if _, err := store.SetDraining(context.Background(), teamstore.HelperUpgradeReason); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-restart-upgrade"), "helper restart force"); err != nil {
		t.Fatalf("handleControlMessage restart upgrade error: %v", err)
	}
	select {
	case <-restarted:
		t.Fatal("helper restart must not run during helper upgrade drain")
	default:
	}
	if len(*sent) != 1 {
		t.Fatalf("sent message count = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(got, "upgrade is already in progress") || !strings.Contains(got, "will not start another restart") {
		t.Fatalf("upgrade restart response = %q", got)
	}
}

func TestBridgeControlReloadRequiresConfirmationAndRunsReloader(t *testing.T) {
	prevDelay := helperRestartDelay
	helperRestartDelay = 0
	t.Cleanup(func() { helperRestartDelay = prevDelay })

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor
	reloaded := make(chan HelperReloadOptions, 1)
	bridge.helperReloader = func(ctx context.Context, opts HelperReloadOptions) error {
		if opts.BeforeRestart != nil {
			if err := opts.BeforeRestart(ctx); err != nil {
				return err
			}
		}
		reloaded <- opts
		return nil
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-reload-help"), "helper reload"); err != nil {
		t.Fatalf("handleControlMessage reload help error: %v", err)
	}
	select {
	case <-reloaded:
		t.Fatal("helper reload without now should not reload")
	default:
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "helper reload now") {
		t.Fatalf("reload confirmation response = %#v", *sent)
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-reload-now"), "helper reload now"); err != nil {
		t.Fatalf("handleControlMessage reload now error: %v", err)
	}
	select {
	case opts := <-reloaded:
		if opts.Force {
			t.Fatal("helper reload now should not set force")
		}
	case <-time.After(time.Second):
		t.Fatal("helper reload now did not call reloader")
	}
	waitBridgeControlNotDraining(t, store)
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	if len(*sent) != 2 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[1].Content), "reload started") {
		t.Fatalf("reload started response = %#v", *sent)
	}
}

func TestBridgeControlReloadReportsUnavailableWhenNotConfigured(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-reload-unavailable"), "helper reload now"); err != nil {
		t.Fatalf("handleControlMessage reload unavailable error: %v", err)
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "not available") {
		t.Fatalf("reload unavailable response = %#v", *sent)
	}
}

func TestBridgeControlReloadNowDoesNotInterruptActiveWork(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	reloaded := make(chan struct{}, 1)
	bridge.helperReloader = func(context.Context, HelperReloadOptions) error {
		reloaded <- struct{}{}
		return nil
	}
	if _, _, err := store.CreateSession(context.Background(), teamstore.SessionContext{ID: "s1", Status: teamstore.SessionStatusActive}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-active", SessionID: "s1", Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-reload-active"), "helper reload now"); err != nil {
		t.Fatalf("handleControlMessage reload active error: %v", err)
	}
	select {
	case <-reloaded:
		t.Fatal("helper reload now should not reload while work is active")
	default:
	}
	if len(*sent) != 1 {
		t.Fatalf("sent message count = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(got, "Codex work is still active") || !strings.Contains(got, "helper reload force") {
		t.Fatalf("active-work reload response = %q", got)
	}
}

func TestBridgeControlReloadForceCanInterruptActiveWork(t *testing.T) {
	prevDelay := helperRestartDelay
	helperRestartDelay = 0
	t.Cleanup(func() { helperRestartDelay = prevDelay })

	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	reloaded := make(chan bool, 1)
	bridge.helperReloader = func(ctx context.Context, opts HelperReloadOptions) error {
		if opts.BeforeRestart != nil {
			if err := opts.BeforeRestart(ctx); err != nil {
				return err
			}
		}
		reloaded <- opts.Force
		return nil
	}
	if _, _, err := store.CreateSession(context.Background(), teamstore.SessionContext{ID: "s1", Status: teamstore.SessionStatusActive}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-active", SessionID: "s1", Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-reload-force"), "helper reload force"); err != nil {
		t.Fatalf("handleControlMessage reload force error: %v", err)
	}
	select {
	case force := <-reloaded:
		if !force {
			t.Fatal("helper reload force did not pass force option")
		}
	case <-time.After(time.Second):
		t.Fatal("helper reload force did not call reloader")
	}
	waitBridgeControlNotDraining(t, store)
}

func TestBridgeControlReloadDoesNotRunDuringHelperUpgradeOrReload(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		want   string
	}{
		{name: "upgrade", reason: teamstore.HelperUpgradeReason, want: "upgrade is already in progress"},
		{name: "reload", reason: teamstore.HelperReloadReason, want: "reload is already in progress"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graph, sent := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			reloaded := make(chan struct{}, 1)
			bridge.helperReloader = func(context.Context, HelperReloadOptions) error {
				reloaded <- struct{}{}
				return nil
			}
			if _, err := store.SetDraining(context.Background(), tc.reason); err != nil {
				t.Fatalf("SetDraining error: %v", err)
			}

			if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-reload-"+tc.name), "helper reload force"); err != nil {
				t.Fatalf("handleControlMessage reload %s error: %v", tc.name, err)
			}
			select {
			case <-reloaded:
				t.Fatalf("helper reload must not run during %s drain", tc.name)
			default:
			}
			if len(*sent) != 1 {
				t.Fatalf("sent message count = %d, want 1", len(*sent))
			}
			if got := PlainTextFromTeamsHTML((*sent)[0].Content); !strings.Contains(got, tc.want) {
				t.Fatalf("drain reload response = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBridgeControlReloadFailureClearsDrain(t *testing.T) {
	prevDelay := helperRestartDelay
	helperRestartDelay = 0
	t.Cleanup(func() { helperRestartDelay = prevDelay })

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	done := make(chan struct{}, 1)
	bridge.helperReloader = func(context.Context, HelperReloadOptions) error {
		done <- struct{}{}
		return errors.New("synthetic reload failure")
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-reload-failure"), "helper reload now"); err != nil {
		t.Fatalf("handleControlMessage reload failure error: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("helper reload did not call failing reloader")
	}
	waitBridgeControlNotDraining(t, store)
	deadline := time.Now().Add(time.Second)
	for {
		for _, msg := range *sent {
			if strings.Contains(PlainTextFromTeamsHTML(msg.Content), "Helper reload failed") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("reload failure response not sent: %#v", *sent)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitBridgeControlNotDraining(t *testing.T, store *teamstore.Store) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		state, err := store.Load(context.Background())
		if err != nil {
			t.Fatalf("Load error: %v", err)
		}
		if !state.ServiceControl.Draining {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("service control still draining: %#v", state.ServiceControl)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBridgeControlNaturalCommandFlowDoesNotRequireSlash(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("natural-projects"), "projects"); err != nil {
		t.Fatalf("projects error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("natural-project"), "project 1"); err != nil {
		t.Fatalf("project 1 error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("natural-details"), "details 1"); err != nil {
		t.Fatalf("details 1 error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent = %#v, want projects, sessions, details", *sent)
	}
	sessions := PlainTextFromTeamsHTML((*sent)[1].Content)
	if !strings.Contains(sessions, "c 1") || strings.Contains(sessions, "/publish") {
		t.Fatalf("natural sessions output = %q", sessions)
	}
	details := PlainTextFromTeamsHTML((*sent)[2].Content)
	if !strings.Contains(details, "Codex session ID: thread-alpha") {
		t.Fatalf("details output = %q", details)
	}
}

func TestBridgeWorkHelperCommandPrefixDoesNotRunCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("helper-status", "helper status"), "helper status"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "STATUS: Work chat") {
		t.Fatalf("helper status response = %#v", *sent)
	}
}

func TestTeamsPromptPreviewIsShortAndDoesNotNestBackticks(t *testing.T) {
	got := teamsPromptPreview("Run `go test ./...` in this project and explain the failure in a very long sentence that should be shortened for Teams.")
	if !strings.HasPrefix(got, `"`) || !strings.HasSuffix(got, `"`) {
		t.Fatalf("preview = %q, want quoted preview", got)
	}
	if strings.Contains(got, "`") {
		t.Fatalf("preview should not contain Teams/code backticks: %q", got)
	}
	if len([]rune(got)) > 84 {
		t.Fatalf("preview too long: %q", got)
	}
}

func TestBridgeWorkStatusForInterruptedTurnSuggestsRetryLast(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn:interrupted", SessionID: session.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnInterrupted(context.Background(), turn.ID, "network disconnected after Codex accepted the request"); err != nil {
		t.Fatalf("MarkTurnInterrupted error: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("helper-status-interrupted", "helper status"), "helper status"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent message count = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(got, "Last request: interrupted") || !strings.Contains(got, "helper retry last") || !strings.Contains(got, "changed files first") {
		t.Fatalf("interrupted status response = %q", got)
	}
}

func TestBridgeUpgradeDrainDefersAndReplaysControlFallbackOnce(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.SetDraining(context.Background(), teamstore.HelperUpgradeReason); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "replayed fallback answer",
		CodexThreadID: "control-thread-1",
		CodexTurnID:   "control-turn-1",
	}}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.controlFallbackExecutor = executor

	if err := bridge.handleControlMessage(context.Background(), bridgePollMessage("control-deferred-fallback", "2026-04-30T01:00:00Z", "自然语言请求"), "自然语言请求"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts during drain = %#v, want none", executor.prompts)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "upgrade in progress") {
		t.Fatalf("sent during helper upgrade drain = %#v, want upgrade notice", *sent)
	}
	deferred, err := store.DeferredInbound(context.Background())
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].Source != "teams_control_fallback" || deferred[0].Text != "自然语言请求" {
		t.Fatalf("deferred control fallback inbound = %#v", deferred)
	}
	if drained, err := bridge.drainComplete(context.Background()); err != nil || !drained {
		t.Fatalf("drainComplete = %v err=%v, want true", drained, err)
	}

	if _, err := store.ClearDrain(context.Background()); err != nil {
		t.Fatalf("ClearDrain error: %v", err)
	}
	if err := bridge.processDeferredInbound(context.Background()); err != nil {
		t.Fatalf("processDeferredInbound error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.Contains(got[0], "User message:\n自然语言请求") {
		t.Fatalf("executor prompts after drain = %#v, want replayed control fallback", got)
	}
	if len(*sent) != 3 || !strings.Contains((*sent)[1].Content, "Quick helper question") || !strings.Contains((*sent)[2].Content, "replayed fallback answer") {
		t.Fatalf("sent after replay = %#v, want control ACK then fallback answer", *sent)
	}
	if bridge.reg.SessionByID(controlFallbackSessionID) != nil || bridge.reg.SessionByChatID("control-chat") != nil {
		t.Fatalf("control fallback should not be projected as a work session: %#v", bridge.reg.Sessions)
	}
	if err := bridge.processDeferredInbound(context.Background()); err != nil {
		t.Fatalf("second processDeferredInbound error: %v", err)
	}
	if len(executor.prompts) != 1 || len(*sent) != 3 {
		t.Fatalf("deferred control fallback replayed more than once, prompts=%#v sent=%#v", executor.prompts, *sent)
	}
}

func TestBridgeControlFallbackResumesDurableThread(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, _, err := store.CreateSession(context.Background(), teamstore.SessionContext{
		ID:            controlFallbackSessionID,
		Status:        teamstore.SessionStatusActive,
		CodexThreadID: "control-thread-existing",
		RunnerKind:    "control_fallback",
		Model:         DefaultControlFallbackModel,
	}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "resumed",
		CodexThreadID: "control-thread-existing",
		CodexTurnID:   "control-turn-2",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.controlFallbackExecutor = executor

	msg := bridgePollMessage("control-unknown-2", "2026-04-30T01:05:00Z", "这个主对话能做什么")
	if err := bridge.handleControlMessage(context.Background(), msg, "这个主对话能做什么"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if got := executor.sessions; len(got) != 1 || got[0].CodexThreadID != "control-thread-existing" {
		t.Fatalf("fallback did not resume durable control thread: %#v", got)
	}
}

func TestBridgePublishImportsExistingTranscriptOnDemand(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := strings.Join([]string{
		`{"id":"u1","role":"user","text":"hello"}`,
		`{"id":"a1","role":"assistant","text":"hi there"}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	var sent []bridgeSentMessage
	var createdChat bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			createdChat = true
			writeTestOnlineMeeting(w, "work-chat", "Codex Work - local - thread-alpha - fix alpha")
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if createdChat {
		t.Fatal("workspace/session listing should not create a work chat")
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-3"), "1"); err != nil {
		t.Fatalf("bare session selection publish error: %v", err)
	}
	if !createdChat {
		t.Fatal("publish did not create work chat")
	}
	prepIndex := -1
	firstWorkIndex := -1
	var imported string
	for i, msg := range sent {
		plain := PlainTextFromTeamsHTML(msg.Content)
		if msg.ChatID == "control-chat" && strings.Contains(plain, "Preparing local Codex history now") {
			prepIndex = i
			if !strings.Contains(plain, "Open Work chat:") || !strings.Contains(plain, "teams.microsoft.com/l/chat/") {
				t.Fatalf("control prep message should include the work chat URL, got %q", plain)
			}
		}
		if msg.ChatID == "work-chat" {
			if firstWorkIndex < 0 {
				firstWorkIndex = i
			}
			imported += "\n" + plain
		}
	}
	if prepIndex < 0 || firstWorkIndex < 0 || prepIndex > firstWorkIndex {
		t.Fatalf("control prep message should be sent before work-chat import messages: prep=%d firstWork=%d sent=%#v", prepIndex, firstWorkIndex, sent)
	}
	if !strings.Contains(imported, "Work chat created:") || !strings.Contains(imported, "Imported Codex session") || !strings.Contains(imported, "User:") || !strings.Contains(imported, "hello") || !strings.Contains(imported, "Codex answer:") || !strings.Contains(imported, "hi there") {
		t.Fatalf("imported transcript missing expected records: %q", imported)
	}
	if got := bridge.reg.SessionByCodexThreadID("thread-alpha"); got == nil || got.ChatID != "work-chat" {
		t.Fatalf("published session not registered: %#v", bridge.reg.Sessions)
	}
}

func TestBridgePublishClosedLinkedSessionCreatesNewWorkChat(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(transcriptPath, []byte(`{"id":"u1","role":"user","text":"hello"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	var createCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			createCalls++
			writeTestOnlineMeeting(w, "new-work-chat", DefaultWorkChatMarker)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			_, _ = fmt.Fprint(w, `{"id":"sent-1","messageType":"message"}`)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.Sessions[0].Status = "closed"
	bridge.reg.Sessions[0].CodexThreadID = "thread-alpha"

	message, err := bridge.publishCodexSession(context.Background(), DashboardCommandTarget{Raw: "thread-alpha"})
	if err != nil {
		t.Fatalf("publish closed session error: %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("createCalls = %d, want new work chat for closed session", createCalls)
	}
	if !strings.Contains(message, "Published local Codex session as s002") {
		t.Fatalf("publish response = %q", message)
	}
	if got := bridge.reg.SessionByCodexThreadID("thread-alpha"); got == nil || got.ID != "s002" || got.Status != "active" {
		t.Fatalf("active codex thread lookup = %#v, registry=%#v", got, bridge.reg.Sessions)
	}
}

func TestBridgeImportCheckpointMismatchDoesNotMarkComplete(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(transcriptPath, []byte(`{"id":"u1","role":"user","text":"hello"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := *bridge.reg.SessionByID("s001")
	session.CodexThreadID = "thread-alpha"
	if err := bridge.ensureDurableSession(context.Background(), &session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	if err := store.UpdateSession(context.Background(), "s001", func(state *teamstore.State) error {
		state.ImportCheckpoints[transcriptCheckpointID("s001")] = teamstore.ImportCheckpoint{
			ID:           transcriptCheckpointID("s001"),
			SessionID:    "s001",
			SourcePath:   transcriptPath,
			LastRecordID: "missing-checkpoint",
			Status:       importCheckpointStatusComplete,
			UpdatedAt:    time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	err := bridge.importCodexTranscriptToTeams(context.Background(), session, codexhistory.Session{
		SessionID: "thread-alpha",
		FilePath:  transcriptPath,
	})
	if err == nil || !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("import error = %v, want checkpoint mismatch", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if got := state.ImportCheckpoints[transcriptCheckpointID("s001")].Status; got != importCheckpointStatusFailed {
		t.Fatalf("checkpoint status = %q, want failed", got)
	}
}

func TestBridgePrepareRecoversFailedTranscriptCheckpointBySourceLine(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(transcriptPath, []byte(`{"id":"u1","role":"user","text":"hello"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := *bridge.reg.SessionByID("s001")
	session.CodexThreadID = "thread-alpha"
	if err := bridge.ensureDurableSession(context.Background(), &session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	if err := store.UpdateSession(context.Background(), "s001", func(state *teamstore.State) error {
		state.ImportCheckpoints[transcriptCheckpointID("s001")] = teamstore.ImportCheckpoint{
			ID:             transcriptCheckpointID("s001"),
			SessionID:      "s001",
			SourcePath:     transcriptPath,
			LastRecordID:   "stale-checkpoint-key",
			LastSourceLine: 1,
			Status:         importCheckpointStatusFailed,
			UpdatedAt:      time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed checkpoint: %v", err)
	}

	gate, err := bridge.prepareLocalCodexBeforeTeamsTurn(context.Background(), &session)
	if err != nil {
		t.Fatalf("prepareLocalCodexBeforeTeamsTurn error: %v", err)
	}
	if gate.Block {
		t.Fatalf("gate blocked after recoverable checkpoint failure: %#v", gate)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID("s001")]
	if checkpoint.Status != importCheckpointStatusComplete || checkpoint.LastRecordID != "u1" || checkpoint.LastSourceLine != 1 {
		t.Fatalf("checkpoint was not recovered: %#v", checkpoint)
	}
}

func TestBridgePublishImportsExistingTranscriptInExactTeamsOrder(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	longAssistant := strings.Repeat("chunk-order-", 13000)
	transcript := strings.Join([]string{
		`{"id":"u1","role":"user","text":"first question"}`,
		`{"id":"a1","role":"assistant","text":"` + longAssistant + `"}`,
		`{"id":"tool1","type":"tool","text":"Tool read file.go"}`,
		`{"id":"status1","type":"status","text":"Thinking through plan"}`,
		`{"id":"u2","role":"user","text":"second question"}`,
		`{"id":"a2","role":"assistant","text":"second answer"}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/order",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-order",
				FirstPrompt: "first question",
				ProjectPath: "/home/user/project/order",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })

	var sent []bridgeSentMessage
	var createdTopic string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			createdTopic = decodeTestOnlineMeetingSubject(t, r)
			writeTestOnlineMeeting(w, "work-chat", createdTopic)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.machine.Label = "qa-host"

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-3"), "/publish 1"); err != nil {
		t.Fatalf("/publish error: %v", err)
	}
	if !strings.HasPrefix(createdTopic, "💬 Codex Work") || !strings.Contains(createdTopic, "qa-host") {
		t.Fatalf("created topic = %q, want work emoji and machine label", createdTopic)
	}

	var work []string
	for _, msg := range sent {
		if msg.ChatID == "work-chat" {
			work = append(work, PlainTextFromTeamsHTML(msg.Content))
		}
	}
	if len(work) < 7 {
		t.Fatalf("work import sent %d message(s), want create mention, title, user, chunked assistant, user, assistant: %#v", len(work), work)
	}
	if !strings.Contains(work[0], "Work chat created:") {
		t.Fatalf("first imported message = %q, want work chat creation mention", work[0])
	}
	if !strings.Contains(work[1], "Imported Codex session") {
		t.Fatalf("second imported message = %q, want import title", work[1])
	}
	if !strings.Contains(work[2], "User:\nfirst question") {
		t.Fatalf("third imported message = %q, want first user prompt", work[2])
	}
	chunkEnd := 3
	for chunkEnd < len(work) && strings.Contains(work[chunkEnd], "Codex answer [part ") {
		chunkEnd++
	}
	if chunkEnd <= 3 {
		t.Fatalf("long assistant was not chunked into consecutive assistant parts: %#v", work)
	}
	if chunkEnd >= len(work) {
		t.Fatalf("missing records after assistant chunks: %#v", work)
	}
	if strings.Contains(strings.Join(work, "\n"), "Tool read file.go") {
		t.Fatalf("historical tool records should be skipped from the Teams recall view: %#v", work)
	}
	tail := strings.Join(work[chunkEnd:], "\n")
	if !strings.Contains(tail, "Codex status:\nThinking through plan") {
		t.Fatalf("messages after assistant chunks = %q, want imported status record", tail)
	}
	if !strings.Contains(tail, "User:\nsecond question") {
		t.Fatalf("messages after status = %q, want second user prompt", tail)
	}
	if !strings.Contains(tail, "Codex answer:\nsecond answer") {
		t.Fatalf("messages after second user = %q, want second assistant answer", tail)
	}
	if strings.Index(tail, "Codex status:\nThinking through plan") > strings.Index(tail, "User:\nsecond question") ||
		strings.Index(tail, "User:\nsecond question") > strings.Index(tail, "Codex answer:\nsecond answer") {
		t.Fatalf("batched messages after assistant chunks are out of order: %q", tail)
	}
	if !strings.Contains(work[len(work)-1], "Imported 5 visible history item(s)") || !strings.Contains(work[len(work)-1], "skipped 1 background tool event") {
		t.Fatalf("completion message = %q, want skipped-tool summary", work[len(work)-1])
	}
}

func TestBridgePublishImportsCompleteLongTranscriptAndMarksAttachedSubagents(t *testing.T) {
	dir := t.TempDir()
	parentPath := filepath.Join(dir, "parent.jsonl")
	childPath := filepath.Join(dir, "child.jsonl")
	parentLines := []string{
		`{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"thread-parent","cwd":"/home/user/project/long","source":"cli"}}`,
	}
	for i := 1; i <= 55; i++ {
		parentLines = append(parentLines, fmt.Sprintf(`{"timestamp":"2026-05-01T00:%02d:00Z","type":"response_item","payload":{"id":"u%02d","type":"message","role":"user","content":[{"type":"input_text","text":"parent user %02d"}]}}`, i%60, i, i))
	}
	parentLines = append(parentLines,
		`{"timestamp":"2026-05-01T01:00:00Z","type":"response_item","payload":{"id":"a-final","type":"message","role":"assistant","content":[{"type":"output_text","text":"parent final answer after tui limit"}]}}`,
	)
	childLines := []string{
		`{"timestamp":"2026-05-01T00:10:00Z","type":"session_meta","payload":{"id":"thread-child","cwd":"/home/user/project/long","source":{"subagent":{"thread_spawn":{"parent_thread_id":"thread-parent","depth":1,"agent_nickname":"Reviewer","agent_role":"explorer"}}}}}`,
		`{"timestamp":"2026-05-01T00:11:00Z","type":"response_item","payload":{"id":"child-u1","type":"message","role":"user","content":[{"type":"input_text","text":"subagent review task"}]}}`,
		`{"timestamp":"2026-05-01T00:12:00Z","type":"response_item","payload":{"id":"child-a1","type":"message","role":"assistant","content":[{"type":"output_text","text":"subagent found edge case"}]}}`,
	}
	if err := os.WriteFile(parentPath, []byte(strings.Join(parentLines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write parent transcript: %v", err)
	}
	if err := os.WriteFile(childPath, []byte(strings.Join(childLines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write child transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/long",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-parent",
				FirstPrompt: "parent user 01",
				ProjectPath: "/home/user/project/long",
				FilePath:    parentPath,
				ModifiedAt:  time.Date(2026, 5, 1, 1, 0, 0, 0, time.UTC),
				Subagents: []codexhistory.SubagentSession{{
					AgentID:         "thread_spawn",
					ParentSessionID: "thread-parent",
					SessionID:       "thread-child",
					FirstPrompt:     "subagent review task",
					FilePath:        childPath,
					CreatedAt:       time.Date(2026, 5, 1, 0, 10, 0, 0, time.UTC),
					ModifiedAt:      time.Date(2026, 5, 1, 0, 12, 0, 0, time.UTC),
				}},
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })

	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			writeTestOnlineMeeting(w, "work-chat", "Codex Work - qa - thread-parent")
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if _, err := bridge.publishCodexSession(context.Background(), DashboardCommandTarget{Raw: "thread-parent"}); err != nil {
		t.Fatalf("publish error: %v", err)
	}
	var work []string
	for _, msg := range sent {
		if msg.ChatID == "work-chat" {
			work = append(work, PlainTextFromTeamsHTML(msg.Content))
		}
	}
	joined := strings.Join(work, "\n")
	for _, want := range []string{
		"User:\nparent user 01",
		"User:\nparent user 55",
		"Codex answer:\nparent final answer after tui limit",
		"Subagent spawned",
		"Subagent: subagent review task",
		"The child subagent transcript is not expanded here",
		"Import complete. This chat is ready",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("imported transcript missing %q in:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "User:\nsubagent review task") || strings.Contains(joined, "Codex answer:\nsubagent found edge case") {
		t.Fatalf("subagent child transcript should not be expanded into the parent Teams chat:\n%s", joined)
	}
	if strings.Index(joined, "User:\nparent user 55") > strings.Index(joined, "Codex answer:\nparent final answer after tui limit") {
		t.Fatalf("parent transcript order is wrong:\n%s", joined)
	}
	if strings.Index(joined, "Codex answer:\nparent final answer after tui limit") > strings.Index(joined, "Subagent spawned") {
		t.Fatalf("subagent marker should follow the parent transcript:\n%s", joined)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	published := bridge.reg.SessionByCodexThreadID("thread-parent")
	if published == nil {
		t.Fatalf("published session not registered: %#v", bridge.reg.Sessions)
	}
	if state.ImportCheckpoints[transcriptCheckpointID(published.ID)].LastRecordID != "a-final" {
		t.Fatalf("parent checkpoint = %#v, want final parent record", state.ImportCheckpoints[transcriptCheckpointID(published.ID)])
	}
	subCheckpointID := transcriptSubagentCheckpointID(published.ID, "thread-child", subagentImportKey(codexhistory.SubagentSession{SessionID: "thread-child"}, 1))
	if state.ImportCheckpoints[subCheckpointID].LastRecordID == "" || state.ImportCheckpoints[subCheckpointID].LastRecordID == "child-a1" {
		t.Fatalf("subagent checkpoint = %#v, want marker-only checkpoint", state.ImportCheckpoints[subCheckpointID])
	}
}

func TestTranscriptDedupeSkipsAdjacentUserEventAndResponseDuplicates(t *testing.T) {
	dedupe := newTranscriptDedupeState()
	first := TranscriptRecord{
		Kind:       TranscriptKindUser,
		Text:       "repeat this prompt",
		SourceLine: 10,
		DedupeKey:  "event-msg-user",
	}
	second := TranscriptRecord{
		Kind:       TranscriptKindUser,
		Text:       "repeat this prompt",
		SourceLine: 11,
		ItemID:     "response-item-user",
	}
	later := TranscriptRecord{
		Kind:       TranscriptKindUser,
		Text:       "repeat this prompt",
		SourceLine: 42,
		ItemID:     "later-user-repeat",
	}
	if dedupe.shouldSkip(first, formatTranscriptRecordForTeams(first)) {
		t.Fatalf("first adjacent user record should be kept")
	}
	if !dedupe.shouldSkip(second, formatTranscriptRecordForTeams(second)) {
		t.Fatalf("adjacent duplicate user record should be skipped")
	}
	if dedupe.shouldSkip(later, formatTranscriptRecordForTeams(later)) {
		t.Fatalf("later repeated user prompt should be kept")
	}
}

func TestBridgePublishLocalLongTranscriptSnapshotOptIn(t *testing.T) {
	transcriptPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LONG_TRANSCRIPT_PATH"))
	if transcriptPath == "" {
		t.Skip("set CODEX_HELPER_TEAMS_LONG_TRANSCRIPT_PATH to inspect a real long local Codex transcript import render snapshot")
	}
	info, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat long transcript %s: %v", transcriptPath, err)
	}
	transcript, err := ReadSessionTranscript(transcriptPath)
	if err != nil {
		t.Fatalf("ReadSessionTranscript(%s): %v", transcriptPath, err)
	}
	work := []string{
		PlainTextFromTeamsHTML(RenderTeamsHTML(TeamsRenderInput{
			Surface: TeamsRenderSurfaceOutbox,
			Kind:    TeamsRenderHelper,
			Text:    "Imported Codex session history\n\nThe messages below came from your local Codex session. Reply in this chat to continue from here.\n\nSession: long local transcript snapshot",
		})),
	}
	dedupe := newTranscriptDedupeState()
	stats := transcriptImportStats{Total: len(transcript.Records)}
	for i, record := range transcript.Records {
		if strings.TrimSpace(record.Text) == "" {
			continue
		}
		if shouldSkipImportedTranscriptRecord(record) {
			stats.SkippedBackground++
			continue
		}
		body := formatTranscriptRecordForTeams(record)
		if strings.TrimSpace(body) == "" || dedupe.shouldSkip(record, body) {
			continue
		}
		stats.Imported++
		kind := transcriptRecordOutboxKind("import", record, i+1)
		chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
			Surface: TeamsRenderSurfaceOutbox,
			Kind:    renderKindForOutbox(kind),
			Text:    body,
		}, TeamsRenderOptions{
			HardLimitBytes:   safeTeamsHTMLContentBytes,
			TargetLimitBytes: teamsChunkHTMLContentBytes,
		})
		for _, chunk := range chunks {
			work = append(work, PlainTextFromTeamsHTML(renderTeamsHTMLPart(TeamsRenderInput{
				Surface: TeamsRenderSurfaceOutbox,
				Kind:    renderKindForOutbox(kind),
				Text:    chunk.Text,
			}, chunk.PartIndex, chunk.PartCount)))
		}
	}
	work = append(work, PlainTextFromTeamsHTML(RenderTeamsHTML(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderHelper,
		Text:    formatTranscriptImportCompleteMessage(stats),
	})))
	if len(work) < 3 {
		t.Fatalf("long import produced too few Teams messages: %#v", work)
	}
	userCount, assistantCount, statusCount, partCount := 0, 0, 0, 0
	for _, text := range work {
		switch {
		case strings.Contains(text, "User:"):
			userCount++
		case strings.Contains(text, "Codex answer:"):
			assistantCount++
		case strings.Contains(text, "Codex status:"):
			statusCount++
		}
		if strings.Contains(text, "[part ") {
			partCount++
		}
	}
	firstSamples := work
	if len(firstSamples) > 5 {
		firstSamples = firstSamples[:5]
	}
	lastSamples := work
	if len(lastSamples) > 5 {
		lastSamples = lastSamples[len(lastSamples)-5:]
	}
	t.Logf("LONG_IMPORT_SUMMARY path=%s size=%d parsed_records=%d teams_messages=%d user=%d assistant=%d status=%d skipped_tool=%d chunked_parts=%d diagnostics=%d", transcriptPath, info.Size(), len(transcript.Records), len(work), userCount, assistantCount, statusCount, stats.SkippedBackground, partCount, len(transcript.Diagnostics))
	t.Logf("LONG_IMPORT_FIRST_MESSAGES %#v", firstSamples)
	t.Logf("LONG_IMPORT_LAST_MESSAGES %#v", lastSamples)
	if len(transcript.Records) > 50 && userCount+assistantCount == 0 {
		t.Fatalf("long transcript had %d records but import produced no visible user/assistant Teams messages", len(transcript.Records))
	}
	if !strings.Contains(strings.Join(work[len(work)-min(len(work), 5):], "\n"), "Import complete") {
		t.Fatalf("long import missing final completion message in tail: %#v", lastSamples)
	}
}

func TestBridgePublishImportsDuplicateSourceIDsWithoutDroppingRecords(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := strings.Join([]string{
		`{"id":"dup","role":"assistant","text":"first duplicate answer"}`,
		`{"id":"dup","role":"assistant","text":"second duplicate answer"}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/dup",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-dup",
				FirstPrompt: "duplicate ids",
				ProjectPath: "/home/user/project/dup",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			writeTestOnlineMeeting(w, "work-chat", "Codex Work - qa - thread-dup")
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if _, err := bridge.publishCodexSession(context.Background(), DashboardCommandTarget{Raw: "thread-dup"}); err != nil {
		t.Fatalf("publish error: %v", err)
	}
	var imported []string
	for _, msg := range sent {
		if msg.ChatID == "work-chat" {
			imported = append(imported, PlainTextFromTeamsHTML(msg.Content))
		}
	}
	joined := strings.Join(imported, "\n")
	if strings.Count(joined, "first duplicate answer") != 1 || strings.Count(joined, "second duplicate answer") != 1 {
		t.Fatalf("duplicate source id records should both be imported once, imported=%q", joined)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync after duplicate import error: %v", err)
	}
	joinedAfterSync := strings.Join(imported, "\n")
	if joinedAfterSync != joined {
		t.Fatalf("unexpected local mutation")
	}
	if len(sent) != len(imported) {
		t.Fatalf("sync after complete duplicate import should not resend records, sent=%#v", sent)
	}
}

func TestBridgeImportTranscriptDedupesNonAdjacentAssistantStatusEcho(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-echo"}}`,
		`{"type":"response_item","payload":{"id":"u1","type":"message","role":"user","content":[{"type":"input_text","text":"repeatable user prompt"}]}}`,
		`{"type":"response_item","payload":{"id":"a1","type":"message","role":"assistant","content":[{"type":"output_text","text":"same model text shown by two transcript surfaces"}]}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"s1","message":"intermediate visible status","phase":"commentary"}}`,
		`{"type":"response_item","payload":{"id":"u2","type":"message","role":"user","content":[{"type":"input_text","text":"repeatable user prompt"}]}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"s2","message":"same model text shown by two transcript surfaces","phase":"commentary"}}`,
		`{"type":"response_item","payload":{"id":"a2","type":"message","role":"assistant","content":[{"type":"output_text","text":"final distinct answer"}]}}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.Sessions[0]

	lastRecordID, _, stats, err := bridge.importTranscriptRecordsToTeams(context.Background(), session, transcriptPath, "import:"+session.ID, "import", transcriptCheckpointID(session.ID))
	if err != nil {
		t.Fatalf("import transcript records: %v", err)
	}
	if lastRecordID != "a2" {
		t.Fatalf("lastRecordID = %q, want a2", lastRecordID)
	}
	if stats.Total != 6 || stats.Imported != 5 {
		t.Fatalf("stats = %#v, want total 6 imported 5", stats)
	}
	var imported []string
	for _, msg := range *sent {
		imported = append(imported, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(imported, "\n")
	if strings.Count(joined, "repeatable user prompt") != 2 {
		t.Fatalf("repeated user prompts should be preserved, imported=%q", joined)
	}
	if strings.Count(joined, "same model text shown by two transcript surfaces") != 1 {
		t.Fatalf("assistant/status echo should be imported once, imported=%q", joined)
	}
	if !strings.Contains(joined, "🤖 ⏳ Codex status:\nintermediate visible status") {
		t.Fatalf("distinct status should remain visible, imported=%q", joined)
	}
	if !strings.Contains(joined, "🤖 ✅ Codex answer:\nfinal distinct answer") {
		t.Fatalf("final assistant answer missing, imported=%q", joined)
	}
}

func TestBridgeImportTranscriptBatchesVisibleHistoryRecords(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	var lines []string
	for i := 1; i <= 12; i++ {
		lines = append(lines,
			fmt.Sprintf(`{"id":"u-%02d","role":"user","text":"user prompt %02d"}`, i, i),
			fmt.Sprintf(`{"type":"response_item","payload":{"id":"tool-%02d","type":"function_call","name":"shell","arguments":"{\"cmd\":\"rg query %02d\"}"}}`, i, i),
			fmt.Sprintf(`{"type":"event_msg","payload":{"type":"agent_message","id":"s-%02d","message":"status update %02d","phase":"commentary"}}`, i, i),
			fmt.Sprintf(`{"id":"a-%02d","role":"assistant","text":"assistant answer %02d"}`, i, i),
		)
	}
	lines = append(lines, "")
	if err := os.WriteFile(transcriptPath, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.Sessions[0]

	lastRecordID, _, stats, err := bridge.importTranscriptRecordsToTeams(context.Background(), session, transcriptPath, "import:"+session.ID, "import", transcriptCheckpointID(session.ID))
	if err != nil {
		t.Fatalf("import transcript records: %v", err)
	}
	if lastRecordID != "a-12" {
		t.Fatalf("lastRecordID = %q, want a-12", lastRecordID)
	}
	if stats.Total != 48 || stats.Imported != 36 || stats.SkippedBackground != 12 {
		t.Fatalf("stats = %#v, want total 48 imported 36 skipped 12", stats)
	}
	if len(*sent) >= 36 {
		t.Fatalf("history import sent %d Teams messages, want visible records batched below 36", len(*sent))
	}
	if len(*sent) != 1 {
		t.Fatalf("small transcript should fit in one import batch, sent=%d", len(*sent))
	}
	plain := PlainTextFromTeamsHTML((*sent)[0].Content)
	for _, want := range []string{
		"🧑‍💻 User:\nuser prompt 01",
		"🤖 ⏳ Codex status:\nstatus update 01",
		"🤖 ✅ Codex answer:\nassistant answer 12",
	} {
		if !strings.Contains(plain, want) {
			t.Fatalf("batched import missing %q in:\n%s", want, plain)
		}
	}
	if strings.Contains(plain, "🔧 Helper:\n🧑‍💻 User:") {
		t.Fatalf("batched transcript should not be wrapped as one helper message:\n%s", plain)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	var batchOutbox teamstore.OutboxMessage
	for _, outbox := range state.OutboxMessages {
		if isTranscriptImportBatchOutboxKind(outbox.Kind) {
			batchOutbox = outbox
			break
		}
	}
	if batchOutbox.ID == "" {
		t.Fatalf("missing import batch outbox in state: %#v", state.OutboxMessages)
	}
	if renderOutboxHTML(batchOutbox) != batchOutbox.Body {
		t.Fatal("import batch outbox should render its pre-rendered safe HTML body directly")
	}
}

func TestBridgePollIgnoresImportBatchBeforeAnnotatingUserPrefix(t *testing.T) {
	importBatch := teamstore.OutboxMessage{
		ID:          "outbox:import-batch",
		TeamsChatID: "chat-1",
		Kind:        "import-batch-0001-first-last",
		Body: strings.Join([]string{
			renderTeamsHTMLPart(TeamsRenderInput{Surface: TeamsRenderSurfaceOutbox, Kind: TeamsRenderUser, Text: "historical user prompt"}, 1, 1),
			transcriptImportBatchSeparatorHTML,
			renderTeamsHTMLPart(TeamsRenderInput{Surface: TeamsRenderSurfaceOutbox, Kind: TeamsRenderAssistant, Text: "historical assistant answer"}, 1, 1),
		}, ""),
		Status: teamstore.OutboxStatusSent,
	}
	var patched bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages":
			msg := bridgePollMessage("teams-import-batch-1", "2026-04-30T01:05:00Z", "")
			msg.Body.Content = importBatch.Body
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{msg}}); err != nil {
				t.Fatalf("encode poll response: %v", err)
			}
		case r.Method == http.MethodPatch && r.URL.Path == "/chats/chat-1/messages/teams-import-batch-1":
			patched = true
			w.WriteHeader(http.StatusNoContent)
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
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	if _, _, err := store.QueueOutbox(context.Background(), importBatch); err != nil {
		t.Fatalf("QueueOutbox import batch error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.readGraph = graph
	bridge.annotateUserMessages = true

	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if patched {
		t.Fatal("import batch was annotated with a User prefix")
	}
	if len(handled) != 0 {
		t.Fatalf("import batch should not be handled as user input: %#v", handled)
	}
	if !bridge.reg.HasSent("chat-1", "teams-import-batch-1") {
		t.Fatal("import batch was not marked as sent after content-match ignore")
	}
}

func TestBridgePollSkipsWorkChatWhileTranscriptImporting(t *testing.T) {
	var workPolls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/control-chat/messages":
			if err := json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}}); err != nil {
				t.Fatalf("encode control response: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages":
			workPolls++
			if err := json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{
				bridgePollMessage("work-during-import", "2026-04-30T01:10:00Z", "do not interrupt import"),
			}}); err != nil {
				t.Fatalf("encode work response: %v", err)
			}
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
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.readGraph = graph
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := bridge.markTranscriptImportStarted(context.Background(), *session, "/tmp/session.jsonl"); err != nil {
		t.Fatalf("markTranscriptImportStarted error: %v", err)
	}

	if err := bridge.pollOnce(context.Background(), 20); err != nil {
		t.Fatalf("pollOnce error: %v", err)
	}
	if workPolls != 0 {
		t.Fatalf("work chat was polled %d time(s) while transcript import was active", workPolls)
	}
}

func TestBridgeSessionMessageDefersUntilTranscriptImportCompletes(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "deferred answer", CodexThreadID: "thread-1", CodexTurnID: "turn-deferred"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := bridge.markTranscriptImportStarted(context.Background(), *session, "/tmp/session.jsonl"); err != nil {
		t.Fatalf("markTranscriptImportStarted error: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("during-import", "run after import"), "run after import"); err != nil {
		t.Fatalf("handleSessionMessage during import error: %v", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	var deferred teamstore.InboundEvent
	for _, inbound := range state.InboundEvents {
		if inbound.TeamsMessageID == "during-import" {
			deferred = inbound
			break
		}
	}
	if deferred.ID == "" || deferred.Status != teamstore.InboundStatusDeferred || deferred.Text != "run after import" {
		t.Fatalf("message was not deferred cleanly during import: %#v", deferred)
	}
	if len(state.Turns) != 0 || len(*sent) != 0 || len(executor.prompts) != 0 {
		t.Fatalf("deferred import message should not queue, send ack, or run Codex yet; turns=%#v sent=%#v prompts=%#v", state.Turns, *sent, executor.prompts)
	}

	if err := bridge.markTranscriptImportComplete(context.Background(), *session, "/tmp/session.jsonl", "a-final", 42); err != nil {
		t.Fatalf("markTranscriptImportComplete error: %v", err)
	}
	if err := bridge.processDeferredInbound(context.Background()); err != nil {
		t.Fatalf("processDeferredInbound error: %v", err)
	}
	waitForCompletedTurnCount(t, store, "s001", 1)
	joined := sentPlainText(*sent)
	if !strings.Contains(joined, "Codex is working. Request accepted.") || !strings.Contains(joined, "deferred answer") {
		t.Fatalf("deferred message was not replayed after import completion, sent=\n%s", joined)
	}
}

func TestBridgeQueuedTurnsWaitForTranscriptImport(t *testing.T) {
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{result: ExecutionResult{Text: "should wait"}})
	bridge.asyncTurns = true
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(context.Background(), teamstore.InboundEvent{
		SessionID:      "s001",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "queued-during-import",
		Text:           "queued input",
		TextHash:       normalizedTextHash("queued input"),
		Status:         teamstore.InboundStatusPersisted,
	})
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{SessionID: "s001", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if err := bridge.markTranscriptImportStarted(context.Background(), *session, "/tmp/session.jsonl"); err != nil {
		t.Fatalf("markTranscriptImportStarted error: %v", err)
	}

	started, err := bridge.startQueuedTurn(context.Background(), session, "", nil)
	if err != nil {
		t.Fatalf("startQueuedTurn error: %v", err)
	}
	if started {
		t.Fatal("queued turn started while transcript import was active")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusQueued {
		t.Fatalf("turn status = %q, want queued while import is active", got)
	}
}

func TestBridgePublishRetriesInterruptedHistoryImport(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := strings.Join([]string{
		`{"id":"u1","role":"user","text":"hello"}`,
		`{"id":"a1","role":"assistant","text":"hi there"}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	var sent []bridgeSentMessage
	failAssistantOnce := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			writeTestOnlineMeeting(w, "work-chat", "Codex Work - local - thread-alpha - fix alpha")
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			if failAssistantOnce && strings.Contains(body.Body.Content, "hi there") {
				failAssistantOnce = false
				http.Error(w, "temporary", http.StatusInternalServerError)
				return
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-3"), "/publish 1"); err != nil {
		t.Fatalf("first /publish error: %v", err)
	}
	if got := bridge.reg.SessionByCodexThreadID("thread-alpha"); got == nil || got.ChatID != "work-chat" {
		t.Fatalf("published session not registered after interrupted import: %#v", bridge.reg.Sessions)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-4"), "/publish 1"); err != nil {
		t.Fatalf("retry /publish error: %v", err)
	}
	var imported string
	for _, msg := range sent {
		if msg.ChatID == "work-chat" {
			imported += "\n" + PlainTextFromTeamsHTML(msg.Content)
		}
	}
	if strings.Count(imported, "hello") != 1 || strings.Count(imported, "hi there") != 1 {
		t.Fatalf("retry import should resume without duplicates, imported=%q sent=%#v", imported, sent)
	}
	if !strings.Contains(imported, "User:") || !strings.Contains(imported, "Codex answer:") || strings.Contains(imported, "Helper: User:") {
		t.Fatalf("imported transcript role labels are confusing: %q", imported)
	}
}

func TestBridgePublishRetryAfterTitleFailureIsNotSkippedByBackgroundSync(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := strings.Join([]string{
		`{"id":"u1","role":"user","text":"first after title"}`,
		`{"id":"a1","role":"assistant","text":"answer after title"}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "first after title",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	var sent []bridgeSentMessage
	failFirstRecordAttempts := 1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			writeTestOnlineMeeting(w, "work-chat", "Codex Work - qa - thread-alpha")
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			if failFirstRecordAttempts > 0 && strings.Contains(body.Body.Content, "first after title") {
				failFirstRecordAttempts--
				http.Error(w, "temporary", http.StatusInternalServerError)
				return
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if _, err := bridge.publishCodexSession(context.Background(), DashboardCommandTarget{Raw: "thread-alpha"}); err == nil {
		t.Fatal("first publish unexpectedly succeeded")
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("background sync after interrupted publish failed: %v", err)
	}
	if _, err := bridge.publishCodexSession(context.Background(), DashboardCommandTarget{Raw: "thread-alpha"}); err != nil {
		t.Fatalf("retry publish error: %v", err)
	}

	var imported []string
	for _, msg := range sent {
		if msg.ChatID == "work-chat" {
			imported = append(imported, PlainTextFromTeamsHTML(msg.Content))
		}
	}
	joined := strings.Join(imported, "\n")
	if strings.Count(joined, "User:\nfirst after title") != 1 || strings.Count(joined, "Codex answer:\nanswer after title") != 1 {
		t.Fatalf("history import should resume after title-only failure without background-sync skip, imported=%q", joined)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID("s002")]
	if checkpoint.Status != importCheckpointStatusComplete || checkpoint.LastRecordID != "a1" {
		t.Fatalf("checkpoint after retry = %#v, want complete at last record", checkpoint)
	}
}

func TestBridgePublishDefersDuringHelperUpgradeDrain(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	var createCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			createCalls++
			writeTestOnlineMeeting(w, "work-chat", "Codex Work - local - thread-alpha - fix alpha")
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			_, _ = fmt.Fprint(w, `{"id":"sent","messageType":"message"}`)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	if _, err := store.SetDraining(context.Background(), teamstore.HelperUpgradeReason); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-1"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-2"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-3"), "/publish 1"); err != nil {
		t.Fatalf("/publish during drain error: %v", err)
	}
	if createCalls != 0 {
		t.Fatalf("/publish created work chat during helper upgrade drain")
	}
	if drained, err := bridge.drainComplete(context.Background()); err != nil || !drained {
		t.Fatalf("drainComplete = %v err=%v, want true with non-blocking deferred ACK", drained, err)
	}
	deferred, err := store.DeferredInbound(context.Background())
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].Source != "teams_control_publish" {
		t.Fatalf("deferred publish inbound = %#v", deferred)
	}
	if deferred[0].Text != "continue thread-alpha" {
		t.Fatalf("deferred publish text = %q, want resolved session id", deferred[0].Text)
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		view := state.DashboardViews["control-chat"]
		view.ExpiresAt = time.Now().Add(-time.Hour)
		state.DashboardViews["control-chat"] = view
		return nil
	}); err != nil {
		t.Fatalf("expire dashboard view: %v", err)
	}
	if _, err := store.ClearDrain(context.Background()); err != nil {
		t.Fatalf("ClearDrain error: %v", err)
	}
	if err := bridge.processDeferredInbound(context.Background()); err != nil {
		t.Fatalf("processDeferredInbound should use resolved session id instead of expired dashboard number, got: %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("createCalls after replay = %d, want 1", createCalls)
	}
	if got := bridge.reg.SessionByCodexThreadID("thread-alpha"); got == nil || got.ChatID != "work-chat" {
		t.Fatalf("published session after replay not registered: %#v", bridge.reg.Sessions)
	}
}

func TestBridgeSessionCancelQueuedTurnMarksInterrupted(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn:queued", SessionID: session.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	err = bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("cancel-command"), "/cancel "+turn.ID)
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "turn canceled") {
		t.Fatalf("cancel response = %q", (*sent)[0].Content)
	}
}

func TestBridgeSessionRetryFailedTurnFetchesOriginalMessage(t *testing.T) {
	graph, sent := newBridgeRetryGraph(t, bridgePollMessage("original-1", "2026-04-30T01:00:00Z", "retry prompt"))
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "retried answer",
		CodexThreadID: "thread-1",
		CodexTurnID:   "turn-retry",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(context.Background(), teamstore.InboundEvent{
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "original-1",
		Status:         teamstore.InboundStatusPersisted,
	})
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnFailed(context.Background(), turn.ID, "network"); err != nil {
		t.Fatalf("MarkTurnFailed error: %v", err)
	}

	err = bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("retry-command"), "/retry "+turn.ID)
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.HasPrefix(got[0], "retry prompt\n\n") || !strings.Contains(got[0], ArtifactManifestFenceInfo) {
		t.Fatalf("executor prompts = %#v, want retry prompt plus artifact handoff instructions", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "retried answer") {
		t.Fatalf("retry response = %q", (*sent)[0].Content)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	completedRetries := 0
	for _, item := range state.Turns {
		if strings.Contains(item.RecoveryReason, "retry of "+turn.ID) {
			completedRetries++
			if item.Status != teamstore.TurnStatusCompleted {
				t.Fatalf("retry turn status = %q, want completed", item.Status)
			}
		}
	}
	if completedRetries != 1 {
		t.Fatalf("completed retry count = %d, want 1; turns=%#v", completedRetries, state.Turns)
	}
}

func TestBridgeSessionAttachmentIsRejectedWithoutRunningCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	msg := bridgeTestMessage("message-attachment")
	msg.Attachments = []MessageAttachment{{ID: "att-1", ContentType: "image/png", Name: "screenshot.png"}}

	err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "please inspect this")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "I could not process") || !strings.Contains((*sent)[0].Content, "image/png") {
		t.Fatalf("attachment response = %q", (*sent)[0].Content)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.Turns); got != 0 {
		t.Fatalf("turn count = %d, want 0", got)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound count = %d, want 1", got)
	}
	for _, inbound := range state.InboundEvents {
		if inbound.Status != teamstore.InboundStatusIgnored {
			t.Fatalf("inbound status = %q, want ignored", inbound.Status)
		}
	}
}

func TestAttachmentDownloadOversizeErrorIsUserVisible(t *testing.T) {
	message, ok := attachmentDownloadUserMessage(errors.New("Graph response exceeds 20971520 bytes"))
	if !ok {
		t.Fatal("expected oversize attachment error to be user visible")
	}
	if !strings.Contains(message, "too large") || !strings.Contains(message, "20971520") {
		t.Fatalf("oversize message = %q", message)
	}
}

func TestBridgeSessionHostedContentIsDownloadedForCodexTurn(t *testing.T) {
	graph, sent := newBridgeHostedContentGraph(t)
	store := newBridgeTestStore(t)
	executor := &attachmentReadingExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	msg := bridgeTestMessage("message-hosted")
	msg.Body.ContentType = "html"
	msg.Body.Content = `<p>inspect this <img src="https://graph.microsoft.com/v1.0/chats/chat-1/messages/message-hosted/hostedContents/content-1/$value"></p>`

	err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "inspect this")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if !strings.Contains(executor.prompt, "Attached files saved locally") || !strings.Contains(executor.prompt, "attachment-001") {
		t.Fatalf("executor prompt missing local attachment:\n%s", executor.prompt)
	}
	if got := len(*sent); got != 2 {
		t.Fatalf("sent message count = %d, want ack plus final", got)
	}
	if !strings.Contains((*sent)[0].Content, "Codex is working") || !strings.Contains((*sent)[1].Content, "saw attachment") {
		t.Fatalf("hosted content response = %#v", *sent)
	}
}

func TestBridgeSessionReferenceFileAttachmentIsDownloadedForCodexTurn(t *testing.T) {
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_READ_SCOPES", "openid profile offline_access User.Read Chat.Read Files.Read")
	graph, sent := newBridgeReferenceFileGraph(t)
	store := newBridgeTestStore(t)
	executor := &attachmentReadingExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	msg := bridgeTestMessage("message-file")
	msg.Body.ContentType = "html"
	msg.Body.Content = "<p>inspect file</p>"
	msg.Attachments = []MessageAttachment{{
		ID:          "file-1",
		ContentType: "reference",
		ContentURL:  "https://contoso.sharepoint.com/sites/team/file.txt",
		Name:        "file.txt",
	}}

	err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "inspect file")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if !strings.Contains(executor.prompt, "Attached files saved locally") || !strings.Contains(executor.prompt, "file-001") {
		t.Fatalf("executor prompt missing local file:\n%s", executor.prompt)
	}
	if got := len(*sent); got != 2 {
		t.Fatalf("sent message count = %d, want ack plus final", got)
	}
	if !strings.Contains((*sent)[0].Content, "Codex is working") || !strings.Contains((*sent)[1].Content, "saw attachment") {
		t.Fatalf("file attachment response = %#v", *sent)
	}
}

func TestBridgeSessionSendFileCommandUploadsOutboundAttachment(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if err := writeTokenCache(cfg.CachePath, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot error: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte("report"), 0o600); err != nil {
		t.Fatalf("write outbound file: %v", err)
	}
	chatGraph, chatSent := newBridgeTestGraph(t)
	fileGraph, sent := newOutboundAttachmentGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(chatGraph, store, &recordingExecutor{})
	bridge.fileGraph = fileGraph

	err = bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("send-file-command"), "/send-file report.txt")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := len(*sent); got != 0 {
		t.Fatalf("file graph should only upload, sent attachment count = %d", got)
	}
	if len(*chatSent) != 1 || !strings.Contains((*chatSent)[0].Content, "attachment") {
		t.Fatalf("attachment message content = %#v", *chatSent)
	}
}

func TestBridgeSessionSendFileAttachmentUsesDurableOutboxOnRateLimit(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if err := writeTokenCache(cfg.CachePath, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot error: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte("report"), 0o600); err != nil {
		t.Fatalf("write outbound file: %v", err)
	}

	var sendAttempts int
	var sent []bridgeSentMessage
	rateLimited := true
	chatServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected chat request: %s %s", r.Method, r.URL.String())
		}
		sendAttempts++
		if rateLimited {
			http.Error(w, `{"error":{"code":"TooManyRequests"}}`, http.StatusTooManyRequests)
			return
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode chat request: %v", err)
		}
		sent = append(sent, bridgeSentMessage{ChatID: "chat-1", Content: body.Body.Content})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"message-attachment","messageType":"message"}`)
	}))
	defer chatServer.Close()

	fileGraph, fileSent := newOutboundAttachmentGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     chatServer.Client(),
		baseURL:    chatServer.URL,
		maxRetries: 1,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.fileGraph = fileGraph

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("send-file-rate-limit", "/send-file report.txt"), "/send-file report.txt"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(*fileSent) != 0 || len(sent) != 0 {
		t.Fatalf("attachment should upload but not send after rate limit, fileSent=%#v sent=%#v", *fileSent, sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	var queued teamstore.OutboxMessage
	for _, msg := range state.OutboxMessages {
		if msg.Kind == "attachment" {
			queued = msg
		}
	}
	if queued.Status != teamstore.OutboxStatusQueued || queued.DriveItemID != "item-1" || queued.LastSendError == "" {
		t.Fatalf("queued attachment outbox mismatch: %#v", queued)
	}

	if err := store.ClearChatRateLimit(context.Background(), "chat-1"); err != nil {
		t.Fatalf("ClearChatRateLimit error: %v", err)
	}
	rateLimited = false
	if err := bridge.flushPendingOutboxForChat(context.Background(), "chat-1"); err != nil {
		t.Fatalf("flushPendingOutboxForChat error: %v", err)
	}
	if len(sent) != 1 || !strings.Contains(sent[0].Content, "attachment") {
		t.Fatalf("sent attachment after retry = %#v", sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after retry error: %v", err)
	}
	if got := state.OutboxMessages[queued.ID].Status; got != teamstore.OutboxStatusSent {
		t.Fatalf("attachment outbox status = %s, want sent", got)
	}
}

func TestBridgeAttachmentSendFailureRestartReusesUploadedDriveItem(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if err := writeTokenCache(cfg.CachePath, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot error: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte("report"), 0o600); err != nil {
		t.Fatalf("write outbound file: %v", err)
	}

	var uploadPUTs int
	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.EscapedPath(), ":/content"):
			uploadPUTs++
			_, _ = fmt.Fprint(w, `{"id":"item-1","name":"upload-report.txt"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/me/drive/items/item-1":
			_, _ = fmt.Fprint(w, `{"id":"item-1","name":"upload-report.txt","eTag":"\"{1176C944-0CB9-4304-974C-5837185EFD6A},1\"","webDavUrl":"https://contoso.sharepoint.com/upload-report.txt"}`)
		default:
			t.Fatalf("unexpected file Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer fileServer.Close()
	var sent []bridgeSentMessage
	rateLimited := true
	chatServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Fatalf("unexpected chat request: %s %s", r.Method, r.URL.String())
		}
		if rateLimited {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests"}}`, http.StatusTooManyRequests)
			return
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode chat request: %v", err)
		}
		sent = append(sent, bridgeSentMessage{ChatID: "chat-1", Content: body.Body.Content})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"message-attachment","messageType":"message"}`)
	}))
	defer chatServer.Close()
	chatGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     chatServer.Client(),
		baseURL:    chatServer.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	fileGraph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     fileServer.Client(),
		baseURL:    fileServer.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	firstBridge := newBridgeTestBridge(chatGraph, store, &recordingExecutor{})
	firstBridge.fileGraph = fileGraph

	if err := firstBridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("send-file-rate-limit-restart", "/send-file report.txt"), "/send-file report.txt"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if uploadPUTs != 1 {
		t.Fatalf("upload PUT count = %d, want 1 before restart", uploadPUTs)
	}
	if len(sent) != 0 {
		t.Fatalf("attachment should not be sent while rate limited: %#v", sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	var queued teamstore.OutboxMessage
	for _, msg := range state.OutboxMessages {
		if msg.Kind == "attachment" {
			queued = msg
		}
	}
	if queued.Status != teamstore.OutboxStatusQueued || queued.DriveItemID != "item-1" {
		t.Fatalf("queued attachment after send failure mismatch: %#v", queued)
	}

	if err := store.ClearChatRateLimit(context.Background(), "chat-1"); err != nil {
		t.Fatalf("ClearChatRateLimit error: %v", err)
	}
	rateLimited = false
	restartedBridge := newBridgeTestBridge(chatGraph, store, &recordingExecutor{})
	if err := restartedBridge.flushPendingOutboxForChat(context.Background(), "chat-1"); err != nil {
		t.Fatalf("flushPendingOutboxForChat after restart error: %v", err)
	}
	if uploadPUTs != 1 {
		t.Fatalf("upload PUT count after restart = %d, want still 1", uploadPUTs)
	}
	if len(sent) != 1 || !strings.Contains(sent[0].Content, "attachment") {
		t.Fatalf("sent attachment after restart = %#v", sent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after restart error: %v", err)
	}
	if msg := state.OutboxMessages[queued.ID]; msg.Status != teamstore.OutboxStatusSent || msg.TeamsMessageID == "" || msg.DriveItemID != "item-1" {
		t.Fatalf("replayed attachment outbox mismatch: %#v", msg)
	}
}

func TestBridgeSessionSendFileQueuesDurableOutboxBeforeUpload(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if err := writeTokenCache(cfg.CachePath, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot error: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte("report"), 0o600); err != nil {
		t.Fatalf("write outbound file: %v", err)
	}

	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected failing upload request: %s %s", r.Method, r.URL.String())
		}
		http.Error(w, `{"error":{"code":"serviceUnavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer failServer.Close()
	chatGraph, chatSent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(chatGraph, store, &recordingExecutor{})
	bridge.fileGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     failServer.Client(),
		baseURL:    failServer.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("send-file-upload-fails", "/send-file report.txt"), "/send-file report.txt"); err == nil {
		t.Fatal("handleSessionMessage should report upload failure")
	}
	if len(*chatSent) != 0 {
		t.Fatalf("chat should not receive attachment before upload succeeds: %#v", *chatSent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	var queued teamstore.OutboxMessage
	for _, msg := range state.OutboxMessages {
		if msg.Kind == "attachment" {
			queued = msg
		}
	}
	if queued.ID == "" || queued.AttachmentPath == "" || queued.AttachmentUploadName == "" || queued.AttachmentHash == "" || queued.DriveItemID != "" || queued.LastSendError == "" {
		t.Fatalf("queued pre-upload attachment outbox mismatch: %#v", queued)
	}
	if queued.AttachmentPath == filepath.Join(root, "report.txt") || !strings.Contains(queued.AttachmentPath, string(filepath.Separator)+".outbox"+string(filepath.Separator)) {
		t.Fatalf("queued attachment should use a private staged copy, got path %q", queued.AttachmentPath)
	}
	if err := os.Remove(filepath.Join(root, "report.txt")); err != nil {
		t.Fatalf("remove original outbound file before recovery: %v", err)
	}

	goodFileGraph, _ := newOutboundAttachmentGraph(t)
	bridge.fileGraph = goodFileGraph
	if err := bridge.flushPendingOutboxForChat(context.Background(), "chat-1"); err != nil {
		t.Fatalf("flushPendingOutboxForChat after upload recovery error: %v", err)
	}
	if len(*chatSent) != 1 || !strings.Contains((*chatSent)[0].Content, "attachment") {
		t.Fatalf("chat should receive recovered attachment: %#v", *chatSent)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after recovery error: %v", err)
	}
	recovered := state.OutboxMessages[queued.ID]
	if recovered.Status != teamstore.OutboxStatusSent || recovered.DriveItemID != "item-1" || recovered.TeamsMessageID == "" {
		t.Fatalf("recovered attachment outbox mismatch: %#v", recovered)
	}
}

func TestBridgeAttachmentReplayRejectsTamperedStagedFileBeforeUpload(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if err := writeTokenCache(cfg.CachePath, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot error: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte("report"), 0o600); err != nil {
		t.Fatalf("write outbound file: %v", err)
	}

	failUpload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected failing upload request: %s %s", r.Method, r.URL.String())
		}
		http.Error(w, `{"error":{"code":"serviceUnavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer failUpload.Close()
	chatGraph, chatSent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(chatGraph, store, &recordingExecutor{})
	bridge.fileGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     failUpload.Client(),
		baseURL:    failUpload.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("send-file-tamper", "/send-file report.txt"), "/send-file report.txt"); err == nil {
		t.Fatal("handleSessionMessage should report upload failure")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	var queued teamstore.OutboxMessage
	for _, msg := range state.OutboxMessages {
		if msg.Kind == "attachment" {
			queued = msg
		}
	}
	if queued.AttachmentPath == "" || queued.AttachmentHash == "" || queued.DriveItemID != "" {
		t.Fatalf("queued pre-upload attachment mismatch: %#v", queued)
	}
	if err := os.WriteFile(queued.AttachmentPath, []byte("tampered"), 0o600); err != nil {
		t.Fatalf("tamper staged attachment: %v", err)
	}
	bridge.fileGraph = &GraphClient{
		auth: &fakeGraphAuth{token: "access"},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("tampered staged file must fail before Graph upload, got %s %s", req.Method, req.URL.String())
			return nil, nil
		})},
		baseURL: "https://graph.example.test",
	}
	err = bridge.flushPendingOutboxForChat(context.Background(), "chat-1")
	if err == nil || !(strings.Contains(err.Error(), "content changed") || strings.Contains(err.Error(), "size changed")) {
		t.Fatalf("flush error = %v, want content/size changed", err)
	}
	if len(*chatSent) != 0 {
		t.Fatalf("chat should not receive tampered attachment: %#v", *chatSent)
	}
}

func TestBridgeControlAttachmentGetsExplicitUnsupportedResponse(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessage("control-attachment")
	msg.Attachments = []MessageAttachment{{ID: "att-1", ContentType: "application/pdf", Name: "brief.pdf"}}

	err := bridge.handleControlMessage(context.Background(), msg, "")
	if err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "I could not process") || !strings.Contains((*sent)[0].Content, "application/pdf") || !strings.Contains((*sent)[0].Content, "Files and images belong") {
		t.Fatalf("attachment response = %q", (*sent)[0].Content)
	}
}

func TestBridgeControlOpenSessionReturnsChatURL(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-open"), "open s001")
	if err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "s001") || !strings.Contains((*sent)[0].Content, "https://teams.example/chat-1") || !strings.Contains((*sent)[0].Content, "does not import local history") {
		t.Fatalf("open response = %q", (*sent)[0].Content)
	}
}

func TestBridgeControlOpenNumberResolvesCurrentSessionView(t *testing.T) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "alpha",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.reg.Sessions[0].CodexThreadID = "thread-alpha"

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-open-workspaces"), "/workspaces"); err != nil {
		t.Fatalf("/workspaces error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-open-workspace"), "/workspace 1"); err != nil {
		t.Fatalf("/workspace error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-open-number"), "/open 1"); err != nil {
		t.Fatalf("/open number error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent = %#v, want 3 messages", *sent)
	}
	got := PlainTextFromTeamsHTML((*sent)[2].Content)
	if !strings.Contains(got, "s001") || !strings.Contains(got, "https://teams.example/chat-1") {
		t.Fatalf("/open number response = %q", got)
	}
	if strings.Contains(got, "thread-alpha") {
		t.Fatalf("/open number leaked raw Codex thread id: %q", got)
	}
}

func TestBridgePollSeedsThenUsesDurableModifiedCursor(t *testing.T) {
	oldTime := "2026-04-30T01:00:00Z"
	newTime := "2026-04-30T01:05:00Z"
	graph := newBridgePollGraph(t, []bridgePollPage{
		{
			messages: []ChatMessage{
				bridgePollMessage("old-1", oldTime, "old one"),
				bridgePollMessage("old-2", oldTime, "old two"),
			},
			assert: func(t *testing.T, r *http.Request) {
				t.Helper()
				if r.URL.Query().Get("$filter") != "" {
					t.Fatalf("first seed poll should not use filter: %s", r.URL.RawQuery)
				}
			},
		},
		{
			messages: []ChatMessage{
				bridgePollMessage("new-1", newTime, "new work"),
			},
			assert: func(t *testing.T, r *http.Request) {
				t.Helper()
				query := r.URL.Query()
				if query.Get("$orderby") != "lastModifiedDateTime desc" || !strings.Contains(query.Get("$filter"), "lastModifiedDateTime gt ") {
					t.Fatalf("second poll missing durable cursor query: %s", r.URL.RawQuery)
				}
			},
		},
	})
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("seed poll error: %v", err)
	}
	if len(handled) != 0 {
		t.Fatalf("seed poll handled messages: %#v", handled)
	}
	poll, ok, err := store.ChatPoll(context.Background(), "chat-1")
	if err != nil {
		t.Fatalf("ChatPoll error: %v", err)
	}
	if !ok || !poll.Seeded || poll.LastModifiedCursor.IsZero() {
		t.Fatalf("missing seeded poll state: %#v ok=%v", poll, ok)
	}

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("second poll error: %v", err)
	}
	if len(handled) != 1 || handled[0] != "new work" {
		t.Fatalf("handled messages = %#v, want new work", handled)
	}
}

func TestBridgePollUsesReadGraphAndSendsWithWriteGraph(t *testing.T) {
	readGraph := newBridgePollGraph(t, []bridgePollPage{{
		messages: []ChatMessage{bridgePollMessage("new-1", "2026-04-30T01:05:00Z", "run split-client check")},
	}})
	writeGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "split ok",
		CodexThreadID: "thread-1",
		CodexTurnID:   "turn-1",
	}}
	bridge := newBridgeTestBridge(writeGraph, store, executor)
	bridge.readGraph = readGraph

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(ctx context.Context, msg ChatMessage, text string) error {
		return bridge.handleSessionMessage(ctx, "chat-1", msg, text)
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(executor.prompts) != 1 || !strings.Contains(executor.prompts[0], "run split-client check") {
		t.Fatalf("executor prompts = %#v", executor.prompts)
	}
	if len(*sent) != 2 || !strings.Contains((*sent)[0].Content, "Codex is working") || !strings.Contains((*sent)[1].Content, "split ok") {
		t.Fatalf("sent via write graph = %#v", *sent)
	}
}

func TestBridgePollAnnotatesIncomingUserMessageBestEffort(t *testing.T) {
	patched := false
	msg := bridgePollMessage("new-1", "2026-04-30T01:05:00Z", "run split-client check")
	msg.From.User.DisplayName = "Jason Wei"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{msg}}); err != nil {
				t.Fatalf("encode poll response: %v", err)
			}
		case r.Method == http.MethodPatch && r.URL.Path == "/chats/chat-1/messages/new-1":
			patched = true
			var payload struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode patch payload: %v", err)
			}
			plain := PlainTextFromTeamsHTML(payload.Body.Content)
			if !strings.HasPrefix(plain, "🧑‍💻 User:\nrun split-client check") {
				t.Fatalf("patched message body = %q", plain)
			}
			w.WriteHeader(http.StatusNoContent)
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
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.readGraph = graph
	bridge.user.DisplayName = "Jason Wei"
	bridge.annotateUserMessages = true
	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if !patched {
		t.Fatal("incoming user message was not patched")
	}
	if len(handled) != 1 || handled[0] != "run split-client check" {
		t.Fatalf("handled prompts = %#v", handled)
	}
}

func TestBridgePollIgnoresPromptlessAdaptiveCardMessage(t *testing.T) {
	msg := bridgePollMessage("adaptive-card-only", "2026-04-30T01:05:00Z", "")
	msg.Body.Content = `<attachment id="card-1"></attachment>`
	msg.Attachments = []MessageAttachment{{
		ID:          "card-1",
		ContentType: "application/vnd.microsoft.card.adaptive",
		Name:        "Open Codex chat",
	}}
	graph := newBridgePollGraph(t, []bridgePollPage{{
		messages: []ChatMessage{msg},
	}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.readGraph = graph
	bridge.annotateUserMessages = true
	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(handled) != 0 {
		t.Fatalf("adaptive card-only message should not be handled: %#v", handled)
	}
	if !bridge.reg.HasSeen("chat-1", "adaptive-card-only") {
		t.Fatal("adaptive card-only message was not marked seen")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if len(state.InboundEvents) != 0 || len(state.Turns) != 0 {
		t.Fatalf("adaptive card-only message should not create durable work: inbound=%#v turns=%#v", state.InboundEvents, state.Turns)
	}
}

func TestBridgePollDurableCursorSurvivesEmptyRegistry(t *testing.T) {
	cursor := time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC)
	graph := newBridgePollGraph(t, []bridgePollPage{
		{
			messages: []ChatMessage{bridgePollMessage("new-1", "2026-04-30T01:05:00Z", "after restart")},
			assert: func(t *testing.T, r *http.Request) {
				t.Helper()
				if !strings.Contains(r.URL.Query().Get("$filter"), "lastModifiedDateTime gt ") {
					t.Fatalf("poll should use durable cursor despite empty registry: %s", r.URL.RawQuery)
				}
			},
		},
	})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", cursor, true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.reg.Chats = map[string]ChatState{}
	var handled []string

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("poll error: %v", err)
	}
	if len(handled) != 1 || handled[0] != "after restart" {
		t.Fatalf("handled messages = %#v, want after restart", handled)
	}
}

func TestBridgePollDoesNotDropUserPromptStartingWithCodexPrefix(t *testing.T) {
	graph := newBridgePollGraph(t, []bridgePollPage{{
		messages: []ChatMessage{bridgePollMessage("codex-prefix", "2026-04-30T01:05:00Z", "Codex: 这个日志是什么意思")},
	}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(handled) != 1 || handled[0] != "Codex: 这个日志是什么意思" {
		t.Fatalf("handled = %#v, want Codex-prefixed user prompt", handled)
	}
}

func TestBridgePollIgnoresDurableDeliveredOutboxAfterRegistryLoss(t *testing.T) {
	contentOnlyOutbox := teamstore.OutboxMessage{
		ID:          "outbox:sent-helper-without-message-id",
		TeamsChatID: "chat-1",
		Kind:        "codex-progress-003",
		Body:        "old process sent this but crashed before storing its Teams message id",
		Status:      teamstore.OutboxStatusSent,
	}
	contentOnlyMessage := bridgePollMessage("teams-helper-sent-by-content", "2026-04-30T01:05:01Z", "")
	contentOnlyMessage.Body.Content = renderOutboxHTML(contentOnlyOutbox)
	graph := newBridgePollGraph(t, []bridgePollPage{{
		messages: []ChatMessage{
			bridgePollMessage("teams-helper-sent-1", "2026-04-30T01:05:00Z", "🔧 Helper:\nImported Codex session"),
			contentOnlyMessage,
		},
	}})
	store := newBridgeTestStore(t)
	ctx := context.Background()
	if _, err := store.RecordChatPollSuccess(ctx, "chat-1", time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:             "outbox:sent-helper-message",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-helper-sent-1",
		Kind:           "helper",
		Body:           "Imported Codex session",
		Status:         teamstore.OutboxStatusSent,
	}); err != nil {
		t.Fatalf("QueueOutbox sent helper message error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, contentOnlyOutbox); err != nil {
		t.Fatalf("QueueOutbox content-only helper message error: %v", err)
	}
	executor := &recordingExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.reg.Chats = map[string]ChatState{}
	var handled []string

	if _, err := bridge.pollChat(ctx, "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(handled) != 0 {
		t.Fatalf("handled helper-authored durable outbox as inbound prompt: %#v", handled)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	if !bridge.reg.HasSent("chat-1", "teams-helper-sent-1") {
		t.Fatal("durable sent message was not restored into registry")
	}
	if !bridge.reg.HasSent("chat-1", "teams-helper-sent-by-content") {
		t.Fatal("content-matched sent message was not restored into registry")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.OutboxMessages[contentOnlyOutbox.ID].TeamsMessageID; got != "teams-helper-sent-by-content" {
		t.Fatalf("content-matched outbox TeamsMessageID = %q, want teams-helper-sent-by-content", got)
	}
	if got := len(state.InboundEvents); got != 0 {
		t.Fatalf("inbound events = %d, want none: %#v", got, state.InboundEvents)
	}
}

func TestBridgePollDoesNotDropRenderedOutboxPrefixWithoutDurableMatch(t *testing.T) {
	msg := bridgePollMessage("fresh-user-debug", "2026-04-30T01:05:00Z", "")
	msg.Body.Content = "<p><strong>🤖 ⏳ Codex status:</strong><br>why did this appear?</p>"
	graph := newBridgePollGraph(t, []bridgePollPage{{
		messages: []ChatMessage{msg},
	}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(handled) != 1 || !strings.Contains(handled[0], "why did this appear?") {
		t.Fatalf("handled = %#v, want fresh user debug prompt", handled)
	}
}

func TestBridgePollRecordsWindowWarningWhenContinuationReturned(t *testing.T) {
	requests := 0
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		requests++
		payload := map[string]any{
			"value": []ChatMessage{
				bridgePollMessage(
					fmt.Sprintf("m-%02d", requests),
					time.Date(2026, 4, 30, 1, requests, 0, 0, time.UTC).Format(time.RFC3339),
					fmt.Sprintf("work %02d", requests),
				),
			},
			"@odata.nextLink": server.URL + "/chats/chat-1/messages?$skiptoken=" + fmt.Sprint(requests),
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode poll response: %v", err)
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
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("poll error: %v", err)
	}
	if requests != 1 {
		t.Fatalf("Graph request count = %d, want one page per poll", requests)
	}
	if len(handled) != 1 {
		t.Fatalf("handled count = %d, want 1 (%#v)", len(handled), handled)
	}
	poll, ok, err := store.ChatPoll(context.Background(), "chat-1")
	if err != nil {
		t.Fatalf("ChatPoll error: %v", err)
	}
	if !ok || poll.LastWindowFullAt.IsZero() || !strings.Contains(poll.LastWindowFullMessage, "full message window") {
		t.Fatalf("expected full-window diagnostic, poll=%#v ok=%v", poll, ok)
	}
	if !strings.Contains(poll.ContinuationPath, "$skiptoken=1") {
		t.Fatalf("expected durable continuation, poll=%#v", poll)
	}
}

func TestBridgePollUsesDurableContinuationAfterOnePage(t *testing.T) {
	requests := 0
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		requests++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case requests == 1:
			payload := map[string]any{
				"value": []ChatMessage{
					bridgePollMessage(
						fmt.Sprintf("m-%02d", requests),
						time.Date(2026, 4, 30, 1, requests, 0, 0, time.UTC).Format(time.RFC3339),
						fmt.Sprintf("work %02d", requests),
					),
				},
				"@odata.nextLink": server.URL + "/chats/chat-1/messages?$skiptoken=" + fmt.Sprint(requests),
			}
			if err := json.NewEncoder(w).Encode(payload); err != nil {
				t.Fatalf("encode poll response: %v", err)
			}
		case requests == 2:
			if got := r.URL.Query().Get("$skiptoken"); got != "1" {
				t.Fatalf("continuation request skiptoken = %q, want 1 (%s)", got, r.URL.String())
			}
			payload := map[string]any{
				"value": []ChatMessage{
					bridgePollMessage("m-02", "2026-04-30T01:02:00Z", "work 02"),
				},
			}
			if err := json.NewEncoder(w).Encode(payload); err != nil {
				t.Fatalf("encode continuation response: %v", err)
			}
		default:
			t.Fatalf("unexpected extra poll request %d: %s", requests, r.URL.String())
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
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("first poll error: %v", err)
	}
	if len(handled) != 1 {
		t.Fatalf("first poll handled %d messages, want 1", len(handled))
	}
	poll, ok, err := store.ChatPoll(context.Background(), "chat-1")
	if err != nil {
		t.Fatalf("ChatPoll error: %v", err)
	}
	if !ok || !strings.Contains(poll.ContinuationPath, "$skiptoken=1") {
		t.Fatalf("missing continuation after page cap: %#v ok=%v", poll, ok)
	}

	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("continuation poll error: %v", err)
	}
	if len(handled) != 2 || handled[1] != "work 02" {
		t.Fatalf("handled after continuation = %#v", handled)
	}
	poll, ok, err = store.ChatPoll(context.Background(), "chat-1")
	if err != nil {
		t.Fatalf("ChatPoll after continuation error: %v", err)
	}
	if !ok || poll.ContinuationPath != "" || poll.LastWindowFullMessage != "" {
		t.Fatalf("continuation was not cleared after final page: %#v ok=%v", poll, ok)
	}
}

func TestBridgePollUsesConservativeCursorOverlapForDelayedMessages(t *testing.T) {
	cursor := time.Date(2026, 4, 30, 1, 5, 0, 0, time.UTC)
	graph := newBridgePollGraph(t, []bridgePollPage{{
		messages: []ChatMessage{bridgePollMessage("new-1", "2026-04-30T01:05:10Z", "after delay")},
		assert: func(t *testing.T, r *http.Request) {
			t.Helper()
			want := cursor.Add(-pollCursorOverlap).Format(time.RFC3339Nano)
			if filter := r.URL.Query().Get("$filter"); !strings.Contains(filter, want) {
				t.Fatalf("poll filter = %q, want cursor minus conservative overlap %s", filter, want)
			}
		},
	}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", cursor, true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(handled) != 1 || handled[0] != "after delay" {
		t.Fatalf("handled messages = %#v", handled)
	}
}

func TestBridgePausedSessionMessageIsIgnoredWithoutRunningCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.SetPaused(context.Background(), true, "upgrade"); err != nil {
		t.Fatalf("SetPaused error: %v", err)
	}
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)

	err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-1"), "please run")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "paused") || !strings.Contains((*sent)[0].Content, "upgrade") {
		t.Fatalf("paused response = %q", (*sent)[0].Content)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.Turns); got != 0 {
		t.Fatalf("turn count = %d, want 0", got)
	}
	if got := len(state.InboundEvents); got != 1 {
		t.Fatalf("inbound count = %d, want 1", got)
	}
	for _, inbound := range state.InboundEvents {
		if inbound.Status != teamstore.InboundStatusIgnored {
			t.Fatalf("inbound status = %q, want ignored", inbound.Status)
		}
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.Kind != "control" || outbox.Status != teamstore.OutboxStatusSent {
			t.Fatalf("control outbox mismatch: %#v", outbox)
		}
	}
}

func TestBridgePausedSessionDoesNotDownloadAttachments(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.SetPaused(context.Background(), true, "upgrade"); err != nil {
		t.Fatalf("SetPaused error: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessage("message-hosted-paused")
	msg.Body.Content = `<p>inspect <img src="https://graph.microsoft.com/v1.0/chats/chat-1/messages/message-hosted-paused/hostedContents/content-1/$value"></p>`

	err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "please inspect")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "paused") {
		t.Fatalf("paused response = %q", (*sent)[0].Content)
	}
}

func TestBridgePausedRetryDoesNotStartTurn(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.SetPaused(context.Background(), true, "upgrade"); err != nil {
		t.Fatalf("SetPaused error: %v", err)
	}
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(context.Background(), teamstore.InboundEvent{
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "original-paused",
		Status:         teamstore.InboundStatusPersisted,
	})
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnFailed(context.Background(), turn.ID, "network"); err != nil {
		t.Fatalf("MarkTurnFailed error: %v", err)
	}

	err = bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("retry-paused"), "/retry "+turn.ID)
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "paused") {
		t.Fatalf("paused response = %q", (*sent)[0].Content)
	}
}

func TestBridgeDrainingSessionMessageIsIgnoredWithoutRunningCodex(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.SetDraining(context.Background(), "restart"); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)

	err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-1"), "please run")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "draining") || !strings.Contains((*sent)[0].Content, "restart") {
		t.Fatalf("draining response = %q", (*sent)[0].Content)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.Turns); got != 0 {
		t.Fatalf("turn count = %d, want 0", got)
	}
}

func TestBridgeUpgradeDrainingSessionMessageIsDeferred(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.SetDraining(context.Background(), teamstore.HelperUpgradeReason); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)

	err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-upgrade"), "please run after upgrade")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1 upgrade notice for deferred upgrade input", got)
	}
	if !strings.Contains((*sent)[0].Content, "upgrade in progress") {
		t.Fatalf("upgrade notice = %q", (*sent)[0].Content)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.Turns); got != 0 {
		t.Fatalf("turn count = %d, want 0", got)
	}
	deferred, err := store.DeferredInbound(context.Background())
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].Status != teamstore.InboundStatusDeferred {
		t.Fatalf("deferred inbound = %#v", deferred)
	}
	if deferred[0].Text != "please run after upgrade" {
		t.Fatalf("deferred text = %q", deferred[0].Text)
	}
}

func TestBridgeProcessesDeferredUpgradeInputAfterDrainClears(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "resumed result", CodexThreadID: "thread-resumed", CodexTurnID: "codex-turn-resumed"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	if err := bridge.ensureDurableSession(context.Background(), bridge.reg.SessionByID("s001")); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if _, _, err := store.PersistInbound(context.Background(), teamstore.InboundEvent{
		ID:             "inbound:deferred",
		SessionID:      "s001",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-deferred",
		Text:           "resume this",
		Status:         teamstore.InboundStatusDeferred,
		Source:         "teams",
	}); err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}

	if err := bridge.processDeferredInbound(context.Background()); err != nil {
		t.Fatalf("processDeferredInbound error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.Contains(got[0], "resume this") {
		t.Fatalf("executor prompts = %#v, want resumed deferred input", got)
	}
	if len(*sent) != 2 || !strings.Contains((*sent)[0].Content, "Codex is working") || !strings.Contains((*sent)[1].Content, "resumed result") {
		t.Fatalf("sent messages = %#v, want ACK then result", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.InboundEvents["inbound:deferred"].Status; got != teamstore.InboundStatusQueued {
		t.Fatalf("deferred inbound status = %s, want queued", got)
	}
}

func TestBridgeDoesNotReplayDeferredSessionCommandAsPrompt(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	if _, err := store.SetDraining(context.Background(), teamstore.HelperUpgradeReason); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("deferred-send-file", "/send-file report.txt"), "/send-file report.txt"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if _, err := store.ClearDrain(context.Background()); err != nil {
		t.Fatalf("ClearDrain error: %v", err)
	}
	if err := bridge.processDeferredInbound(context.Background()); err != nil {
		t.Fatalf("processDeferredInbound error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none for deferred session command", executor.prompts)
	}
	if len(*sent) != 2 || !strings.Contains((*sent)[0].Content, "upgrade in progress") || !strings.Contains((*sent)[1].Content, "Please run the command again") {
		t.Fatalf("sent messages = %#v, want deferred command rejection", *sent)
	}
}

func TestBridgeUpgradeDrainingControlNewIsDeferredAndReplayed(t *testing.T) {
	parent := t.TempDir()
	workDir := filepath.Join(parent, "deferred-workspace")
	var createCalls int
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			createCalls++
			writeTestOnlineMeeting(w, "work-chat", "deferred")
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := newBridgeTestStore(t)
	if _, err := store.SetDraining(context.Background(), teamstore.HelperUpgradeReason); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.Sessions = nil

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-deferred-new"), "/new "+workDir+" -- deferred task"); err != nil {
		t.Fatalf("handleControlMessage error: %v", err)
	}
	if createCalls != 0 || len(sent) != 1 || !strings.Contains(sent[0].Content, "upgrade in progress") {
		t.Fatalf("control /new should be deferred during helper upgrade, createCalls=%d sent=%#v", createCalls, sent)
	}
	deferred, err := store.DeferredInbound(context.Background())
	if err != nil {
		t.Fatalf("DeferredInbound error: %v", err)
	}
	if len(deferred) != 1 || deferred[0].Source != "teams_control_new" || !strings.Contains(deferred[0].Text, "deferred task") {
		t.Fatalf("deferred control inbound = %#v", deferred)
	}

	if _, err := store.ClearDrain(context.Background()); err != nil {
		t.Fatalf("ClearDrain error: %v", err)
	}
	if err := bridge.processDeferredInbound(context.Background()); err != nil {
		t.Fatalf("processDeferredInbound error: %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("create chat calls = %d, want 1", createCalls)
	}
	if info, err := os.Stat(workDir); err != nil || !info.IsDir() {
		t.Fatalf("workdir was not created: info=%#v err=%v", info, err)
	}
	if len(sent) != 4 {
		t.Fatalf("sent messages = %d, want upgrade notice, creation mention, anchor, and control ack: %#v", len(sent), sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.InboundEvents[deferred[0].ID].Status; got != teamstore.InboundStatusIgnored {
		t.Fatalf("deferred control inbound status = %s, want ignored after replay", got)
	}
}

func TestBridgeListenStandbyDoesNotCreateControlChatOrPoll(t *testing.T) {
	store := newBridgeTestStore(t)
	scope := teamstore.ScopeIdentity{ID: "scope-standby", AccountID: "user-1", OSUser: "alice", Profile: "default"}
	now := time.Now().UTC()
	if _, err := store.ClaimControlLease(context.Background(), teamstore.ControlLeaseClaim{
		Scope:    scope,
		Machine:  teamstore.MachineRecord{ID: "primary", ScopeID: scope.ID, Kind: teamstore.MachineKindPrimary},
		Duration: time.Minute,
		Now:      now,
	}); err != nil {
		t.Fatalf("primary ClaimControlLease error: %v", err)
	}
	var graphCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		graphCalled = true
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	bridge := &Bridge{
		graph: &GraphClient{
			auth:       &fakeGraphAuth{token: "access"},
			client:     server.Client(),
			baseURL:    server.URL,
			maxRetries: 0,
			sleep:      sleepContext,
			jitter:     func(d time.Duration) time.Duration { return d },
		},
		user:    User{ID: "user-1", UserPrincipalName: "user@example.test"},
		reg:     Registry{Version: 1, UserID: "user-1", Chats: map[string]ChatState{}},
		scope:   scope,
		machine: teamstore.MachineRecord{ID: "temp", ScopeID: scope.ID, Kind: teamstore.MachineKindEphemeral},
		store:   store,
	}
	if err := bridge.Listen(context.Background(), BridgeOptions{Once: true, Interval: time.Millisecond, Executor: EchoExecutor{}, Store: store}); err != nil {
		t.Fatalf("Listen standby error: %v", err)
	}
	if graphCalled {
		t.Fatal("standby bridge should not call Graph")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.ControlChat.TeamsChatID != "" {
		t.Fatalf("standby bridge should not bind control chat: %#v", state.ControlChat)
	}
	if got := state.Machines["temp"].Status; got != teamstore.MachineStatusStandby {
		t.Fatalf("temp machine status = %q, want standby", got)
	}
}

func TestBridgeCloseUsesClosedStatusWithoutArchiveWording(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, EchoExecutor{})

	err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("message-1"), "/close")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	session := bridge.reg.SessionByChatID("chat-1")
	if session.Status != "closed" {
		t.Fatalf("session status = %q, want closed", session.Status)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	content := strings.ToLower((*sent)[0].Content)
	if !strings.Contains(content, "session closed") || strings.Contains(content, "archive") {
		t.Fatalf("close response has wrong wording: %q", (*sent)[0].Content)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions["s001"].Status; got != teamstore.SessionStatusClosed {
		t.Fatalf("durable session status = %q, want closed", got)
	}
}

func TestBridgeDuplicateInboundFlushesOutboxWithoutRerunning(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	ctx := context.Background()
	if err := bridge.ensureDurableSession(ctx, bridge.reg.SessionByChatID("chat-1")); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(ctx, teamstore.InboundEvent{
		SessionID:      "s001",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "message-1",
		Source:         "teams",
	})
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, teamstore.Turn{SessionID: "s001", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnCompleted(ctx, turn.ID, "thread-1", "turn-1"); err != nil {
		t.Fatalf("MarkTurnCompleted error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		SessionID:   "s001",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "queued before restart",
	}); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}

	err = bridge.handleSessionMessage(ctx, "chat-1", bridgeTestMessage("message-1"), "repeat")
	if err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if got := executor.prompts; len(got) != 0 {
		t.Fatalf("executor prompts = %#v, want none", got)
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sent message count = %d, want 1", got)
	}
	if !strings.Contains((*sent)[0].Content, "queued before restart") {
		t.Fatalf("sent content = %q", (*sent)[0].Content)
	}
}

func TestBridgeControlNewCreatesDirectoryBoundSession(t *testing.T) {
	parent := t.TempDir()
	workDir := filepath.Join(parent, "new-workspace")
	var createdTopic string
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			createdTopic = decodeTestOnlineMeetingSubject(t, r)
			writeTestOnlineMeeting(w, "work-chat", createdTopic)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.Sessions = nil

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-new"), "/new "+workDir); err != nil {
		t.Fatalf("/new error: %v", err)
	}
	if info, err := os.Stat(workDir); err != nil || !info.IsDir() {
		t.Fatalf("workdir was not created: info=%#v err=%v", info, err)
	}
	if !strings.Contains(createdTopic, DefaultWorkChatMarker) || !strings.Contains(createdTopic, "s001") || !strings.Contains(createdTopic, filepath.Base(workDir)) {
		t.Fatalf("created topic = %q, want work marker and session id", createdTopic)
	}
	session := bridge.reg.SessionByID("s001")
	if session == nil || session.Cwd != workDir || session.ChatID != "work-chat" {
		t.Fatalf("registered session mismatch: %#v", bridge.reg.Sessions)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions["s001"].Cwd; got != workDir {
		t.Fatalf("durable session cwd = %q, want %q", got, workDir)
	}
	if len(sent) != 3 {
		t.Fatalf("sent messages = %d, want creation mention, anchor, and control ack", len(sent))
	}
	controlAck := PlainTextFromTeamsHTML(sent[2].Content)
	if !strings.Contains(controlAck, "search for: s001") {
		t.Fatalf("control ack should help mobile users find the new chat, got %q", controlAck)
	}
	createdNotice := PlainTextFromTeamsHTML(sent[0].Content)
	if !strings.Contains(createdNotice, "Work chat created: s001") || sent[0].Mentions != 1 {
		t.Fatalf("work chat creation mention = %#v", sent[0])
	}
	anchor := PlainTextFromTeamsHTML(sent[1].Content)
	if !strings.Contains(anchor, "Codex will start automatically") ||
		!strings.Contains(anchor, "Status: waiting for your first task") ||
		!strings.Contains(anchor, "Project: "+filepath.Base(workDir)) ||
		strings.Contains(anchor, "Folder: "+workDir) {
		t.Fatalf("work chat anchor = %q", anchor)
	}
}

func TestBridgeControlNewDuplicateMessageDoesNotCreateSecondChat(t *testing.T) {
	parent := t.TempDir()
	workDir := filepath.Join(parent, "dedupe-workspace")
	var created int
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			created++
			subject := decodeTestOnlineMeetingSubject(t, r)
			writeTestOnlineMeeting(w, fmt.Sprintf("work-chat-%d", created), subject)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.Sessions = nil
	msg := bridgeTestMessage("control-new-duplicate")
	text := "new " + workDir

	if err := bridge.handleControlMessage(context.Background(), msg, text); err != nil {
		t.Fatalf("first new error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), msg, text); err != nil {
		t.Fatalf("duplicate new error: %v", err)
	}
	if created != 1 {
		t.Fatalf("created chats = %d, want 1", created)
	}
	if got := len(bridge.reg.Sessions); got != 1 {
		t.Fatalf("registered sessions = %d, want 1", got)
	}
	var plain []string
	for _, msg := range sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	if !strings.Contains(strings.Join(plain, "\n---\n"), "already handled this new request") {
		t.Fatalf("duplicate response did not explain idempotency:\n%s", strings.Join(plain, "\n---\n"))
	}
}

func TestBridgeControlNewUsesSelectedWorkspaceWhenNoDirectoryGiven(t *testing.T) {
	parent := t.TempDir()
	workDir := filepath.Join(parent, "selected-workspace")
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "selected",
			Path: workDir,
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-selected",
				FirstPrompt: "existing selected work",
				ProjectPath: workDir,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })

	var createdTopic string
	graph, sent := newBridgeCreateChatGraph(t, &createdTopic)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.reg.Sessions = nil

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-workspaces"), "projects"); err != nil {
		t.Fatalf("projects error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-select-workspace"), "1"); err != nil {
		t.Fatalf("select workspace error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-new-selected"), "new"); err != nil {
		t.Fatalf("new selected workspace error: %v", err)
	}
	session := bridge.reg.SessionByID("s001")
	if session == nil || session.Cwd != workDir {
		t.Fatalf("new without directory did not use selected workspace: %#v", bridge.reg.Sessions)
	}
	if !strings.Contains(createdTopic, filepath.Base(workDir)) {
		t.Fatalf("created topic = %q, want selected workspace title", createdTopic)
	}
	if len(*sent) < 4 {
		t.Fatalf("sent messages = %#v, want projects, sessions, anchor, control ack", *sent)
	}
	sessionsPage := PlainTextFromTeamsHTML((*sent)[1].Content)
	if !strings.HasPrefix(strings.TrimPrefix(sessionsPage, "🔧 Helper:\n"), "new") {
		t.Fatalf("sessions page should put new first, got %q", sessionsPage)
	}
}

func TestBridgeFastPollBoostAfterActivity(t *testing.T) {
	bridge := &Bridge{}
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	if got := bridge.nextPollInterval(5*time.Second, now); got != 5*time.Second {
		t.Fatalf("idle poll interval = %v, want 5s", got)
	}
	bridge.boostPolling(now)
	if got := bridge.nextPollInterval(5*time.Second, now.Add(time.Second)); got != fastPollInterval {
		t.Fatalf("boosted poll interval = %v, want %v", got, fastPollInterval)
	}
	if got := bridge.nextPollInterval(500*time.Millisecond, now.Add(time.Second)); got != 500*time.Millisecond {
		t.Fatalf("boost should not slow explicit fast interval, got %v", got)
	}
	if got := bridge.nextPollInterval(5*time.Second, now.Add(fastPollDuration+time.Second)); got != 5*time.Second {
		t.Fatalf("expired boost interval = %v, want 5s", got)
	}
}

func TestBridgePollOncePrioritizesControlAfterControlActivity(t *testing.T) {
	readGraph := newBridgePollGraph(t, []bridgePollPage{{
		messages: []ChatMessage{bridgePollMessage("control-help", "2026-05-02T01:05:00Z", "help")},
		assert: func(t *testing.T, r *http.Request) {
			t.Helper()
			if !strings.Contains(r.URL.Path, "/chats/control-chat/messages") {
				t.Fatalf("first poll should read control chat, got %s", r.URL.Path)
			}
		},
	}})
	writeGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "control-chat", time.Date(2026, 5, 2, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("seed control poll: %v", err)
	}
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 5, 2, 1, 0, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("seed work poll: %v", err)
	}
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph

	if err := bridge.pollOnce(context.Background(), 20); err != nil {
		t.Fatalf("pollOnce error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent = %#v, want one control help response", *sent)
	}
	if got := bridge.nextPollInterval(5*time.Second, time.Now()); got != fastPollInterval {
		t.Fatalf("control activity should boost next poll interval, got %v", got)
	}
}

func TestBridgePollOnceSkipsChatsUntilNextPollAt(t *testing.T) {
	now := time.Now()
	readGraph := newBridgePollGraph(t, nil)
	writeGraph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	for _, chatID := range []string{"control-chat", "chat-1"} {
		if _, err := store.RecordChatPollSuccess(context.Background(), chatID, now.Add(-time.Minute), true, false, 1); err != nil {
			t.Fatalf("seed poll %s: %v", chatID, err)
		}
		if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
			ChatID:         chatID,
			PollState:      inboundPollStateWarm,
			NextPollAt:     now.Add(time.Hour),
			LastActivityAt: now.Add(-time.Minute),
		}); err != nil {
			t.Fatalf("schedule poll %s: %v", chatID, err)
		}
	}
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph

	if err := bridge.pollOnce(context.Background(), 20); err != nil {
		t.Fatalf("pollOnce error: %v", err)
	}
}

func TestBridgePollOnceParksIdleWorkChatAndSendsFreezeNotice(t *testing.T) {
	now := time.Now()
	readGraph := newBridgePollGraph(t, nil)
	writeGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
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
	oldActivity := now.Add(-49 * time.Hour)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", oldActivity, true, false, 1); err != nil {
		t.Fatalf("seed work poll: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      inboundPollStateCold,
		NextPollAt:     now.Add(-time.Minute),
		LastActivityAt: oldActivity,
	}); err != nil {
		t.Fatalf("schedule work poll: %v", err)
	}
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	bridge.reg.ControlChatURL = "https://teams.microsoft.com/l/chat/control/conversations"
	bridge.reg.Sessions[0].UpdatedAt = oldActivity

	if err := bridge.pollOnce(context.Background(), 20); err != nil {
		t.Fatalf("pollOnce error: %v", err)
	}
	if len(*sent) != 1 || (*sent)[0].ChatID != "chat-1" || !strings.Contains((*sent)[0].Content, "This chat is paused") || !strings.Contains((*sent)[0].Content, "Step 2: Send: <code>r ") {
		t.Fatalf("freeze notice sent = %#v", *sent)
	}
	poll, ok, err := store.ChatPoll(context.Background(), "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll ok=%v err=%v", ok, err)
	}
	if poll.PollState != inboundPollStateParked || poll.ParkedAt.IsZero() || poll.ParkNoticeSentAt.IsZero() {
		t.Fatalf("work chat was not parked with notice: %#v", poll)
	}
}

func TestBridgePollOnceSkipsWorkChatDuringTranscriptImport(t *testing.T) {
	now := time.Now()
	readGraph := newBridgePollGraph(t, nil)
	writeGraph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
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
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", now.Add(-time.Minute), true, false, 1); err != nil {
		t.Fatalf("seed work poll: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         "chat-1",
		PollState:      inboundPollStateHot,
		NextPollAt:     now.Add(-time.Second),
		LastActivityAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("schedule work poll: %v", err)
	}
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph
	session := bridge.reg.Sessions[0]
	if err := bridge.markTranscriptImportStarted(context.Background(), session, "/tmp/session.jsonl"); err != nil {
		t.Fatalf("mark import started: %v", err)
	}

	if err := bridge.pollOnce(context.Background(), 20); err != nil {
		t.Fatalf("pollOnce error: %v", err)
	}
}

func TestBridgeControlResumeKeyReactivatesParkedWorkChat(t *testing.T) {
	writeGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	session := bridge.reg.Sessions[0]
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         session.ChatID,
		PollState:      inboundPollStateParked,
		LastActivityAt: time.Now().Add(-49 * time.Hour),
	}); err != nil {
		t.Fatalf("park chat: %v", err)
	}
	key := resumeKeyForSession(session)
	if err := bridge.handleControlMessage(context.Background(), bridgePollMessage("resume-1", "2026-05-02T01:05:00Z", "r "+key), "r "+key); err != nil {
		t.Fatalf("resume command error: %v", err)
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "Resumed s001") {
		t.Fatalf("resume response = %#v", *sent)
	}
	poll, ok, err := store.ChatPoll(context.Background(), session.ChatID)
	if err != nil || !ok {
		t.Fatalf("ChatPoll ok=%v err=%v", ok, err)
	}
	if poll.PollState != inboundPollStateHot || poll.NextPollAt.After(time.Now().Add(time.Second)) || poll.LastActivityAt.IsZero() {
		t.Fatalf("resume did not make chat hot and due: %#v", poll)
	}
}

func TestBridgeResumeKeyIgnoresMutableSessionMetadata(t *testing.T) {
	session := Session{ID: "s001", ChatID: "chat-1", Cwd: "/old", CodexThreadID: "thread-old"}
	key := resumeKeyForSession(session)
	session.Cwd = "/new"
	session.CodexThreadID = "thread-new"
	session.Topic = "new topic"
	if got := resumeKeyForSession(session); got != key {
		t.Fatalf("resume key changed after mutable metadata update: %q -> %q", key, got)
	}
}

func TestBridgePollChatReadRateLimitBlocksOnlyThatChat(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/chats/chat-1/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Retry-After", "60")
		http.Error(w, `{"error":{"code":"TooManyRequests"}}`, http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)
	readGraph := &GraphClient{
		auth:    &fakeGraphAuth{token: "access"},
		client:  server.Client(),
		baseURL: server.URL,
		sleep:   func(context.Context, time.Duration) error { return nil },
		jitter:  func(d time.Duration) time.Duration { return d },
	}
	writeGraph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.readGraph = readGraph

	if _, err := bridge.pollChatWithRole(context.Background(), "chat-1", 20, inboundPollRoleWork, false, func(context.Context, ChatMessage, string) error {
		t.Fatal("handler should not run after Graph 429")
		return nil
	}); err == nil || !isGraphRateLimitError(err) {
		t.Fatalf("pollChat error = %v, want Graph 429", err)
	}
	poll, ok, err := store.ChatPoll(context.Background(), "chat-1")
	if err != nil || !ok {
		t.Fatalf("ChatPoll ok=%v err=%v", ok, err)
	}
	if poll.PollState != inboundPollStateBlocked || poll.BlockedUntil.Before(time.Now().Add(50*time.Second)) {
		t.Fatalf("poll 429 did not block chat: %#v", poll)
	}
	if requests != defaultGraphRetries+1 {
		t.Fatalf("requests = %d, want %d", requests, defaultGraphRetries+1)
	}

	bridge.readGraph = newBridgePollGraph(t, nil)
	if _, err := store.RecordChatPollSuccess(context.Background(), "control-chat", time.Now().Add(-time.Minute), true, false, 1); err != nil {
		t.Fatalf("seed control poll: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         "control-chat",
		PollState:      inboundPollStateWarm,
		NextPollAt:     time.Now().Add(time.Hour),
		LastActivityAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("schedule control poll: %v", err)
	}
	if err := bridge.pollOnce(context.Background(), 20); err != nil {
		t.Fatalf("blocked pollOnce should skip chat-1 without reading it: %v", err)
	}
}

func TestBridgeDashboardProjectsCacheReusesDiscoveryForFollowupSelection(t *testing.T) {
	var calls int
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		calls++
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-alpha",
				FirstPrompt: "fix alpha",
				ProjectPath: "/home/user/project/alpha",
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-projects"), "p"); err != nil {
		t.Fatalf("projects error: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-select"), "1"); err != nil {
		t.Fatalf("select workspace error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("discovery calls = %d, want cached projects reused for follow-up selection", calls)
	}
	if len(*sent) != 2 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[1].Content), "Workspace: /home/user/project/alpha") {
		t.Fatalf("sent = %#v, want workspace sessions from cached discovery", *sent)
	}
}

func TestBridgeEnsureControlChatSkipsExistingSingleMemberGroupChat(t *testing.T) {
	store := newBridgeTestStore(t)
	t.Setenv(envTeamsMachineLabel, "qa-host")
	topic := ControlChatTitle(ChatTitleOptions{MachineLabel: "qa-host"})
	var created bool
	var readySent int
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.String())
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/me/chats":
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []map[string]string{{
				"id":       "existing-control",
				"topic":    topic,
				"chatType": "group",
				"webUrl":   "https://teams.example/existing-control",
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/chats/existing-control/members":
			_, _ = fmt.Fprint(w, `{"value":[{"id":"member-1","userId":"user-1","email":"user@example.test"}]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			created = true
			subject := decodeTestOnlineMeetingSubject(t, r)
			writeTestOnlineMeeting(w, "new-control", subject)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			readySent++
			_, _ = fmt.Fprintf(w, `{"id":"ready-%d","messageType":"message"}`, readySent)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.machine.Label = "qa-host"
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
	bridge.reg.ControlChatID = ""
	bridge.reg.ControlChatTopic = ""
	bridge.reg.ControlChatURL = ""

	chat, err := bridge.EnsureControlChat(context.Background())
	if err != nil {
		t.Fatalf("EnsureControlChat error: %v", err)
	}
	if !created || readySent != 2 {
		t.Fatalf("legacy single-member group should be skipped and replaced by meeting chat: created=%v ready=%v requests=%v", created, readySent, requests)
	}
	if chat.ID != "new-control" || bridge.reg.ControlChatID != "new-control" {
		t.Fatalf("control chat binding = chat=%#v reg=%#v", chat, bridge.reg)
	}
}

func TestBridgeEnsureControlChatRenamesOldControlTopic(t *testing.T) {
	var patchedTopic string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/chats/control-chat" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var payload struct {
			Topic string `json:"topic"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode patch: %v", err)
		}
		patchedTopic = payload.Topic
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
	bridge.reg.ControlChatTopic = "codex-helper control"

	chat, err := bridge.EnsureControlChat(context.Background())
	if err != nil {
		t.Fatalf("EnsureControlChat error: %v", err)
	}
	if !strings.Contains(patchedTopic, DefaultControlChatMarker) {
		t.Fatalf("patched topic = %q, want control marker", patchedTopic)
	}
	if chat.Topic != patchedTopic || bridge.reg.ControlChatTopic != patchedTopic {
		t.Fatalf("control topic not updated: chat=%q reg=%q patched=%q", chat.Topic, bridge.reg.ControlChatTopic, patchedTopic)
	}
}

func TestBridgeEnsureControlChatQueuesReadyMessageDurably(t *testing.T) {
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/me/chats":
			_, _ = fmt.Fprint(w, `{"value":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			writeTestOnlineMeeting(w, "new-control", "control")
		case r.Method == http.MethodPost && (r.URL.Path == "/chats/old-control/messages" || r.URL.Path == "/chats/new-control/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode ready message: %v", err)
			}
			sent = append(sent, bridgeSentMessage{ChatID: "new-control", Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"ready-message-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
	bridge.reg.ControlChatID = ""
	bridge.reg.ControlChatURL = ""
	bridge.reg.ControlChatTopic = ""

	chat, err := bridge.EnsureControlChat(context.Background())
	if err != nil {
		t.Fatalf("EnsureControlChat error: %v", err)
	}
	if chat.ID != "new-control" || bridge.reg.ControlChatID != "new-control" {
		t.Fatalf("control chat was not recorded: chat=%#v reg=%#v", chat, bridge.reg)
	}
	if len(sent) != 2 || !strings.Contains(sent[0].Content, "Control chat created.") || sent[0].Mentions != 1 || !strings.Contains(sent[1].Content, "control chat is ready") {
		t.Fatalf("ready message sent = %#v", sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state error: %v", err)
	}
	if state.ControlChat.TeamsChatID != "new-control" {
		t.Fatalf("control binding not durable before ready send: %#v", state.ControlChat)
	}
	var ready teamstore.OutboxMessage
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID == "new-control" && msg.Kind == "control" {
			ready = msg
			break
		}
	}
	if ready.Status != teamstore.OutboxStatusSent || ready.TeamsMessageID != "ready-message-2" || ready.Sequence <= 0 {
		t.Fatalf("ready outbox was not durably sent: %#v", ready)
	}
}

func TestBridgeRecreateControlChatCreatesFreshMeetingAndRetiresOldRouting(t *testing.T) {
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			writeTestOnlineMeeting(w, "new-control", decodeTestOnlineMeetingSubject(t, r))
		case r.Method == http.MethodPost && (r.URL.Path == "/chats/old-control/messages" || r.URL.Path == "/chats/new-control/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode recreated control message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"new-control-message-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := newBridgeTestStore(t)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		now := time.Now()
		state.ControlChat = teamstore.ControlChatBinding{
			TeamsChatID:    "old-control",
			TeamsChatURL:   "https://teams.example/old-control",
			TeamsChatTopic: "old topic",
			BoundAt:        now,
			UpdatedAt:      now,
		}
		state.ChatPolls["old-control"] = teamstore.ChatPollState{ChatID: "old-control", Seeded: true}
		state.ChatSequences["old-control"] = teamstore.ChatSequenceState{ChatID: "old-control", Next: 42}
		state.ChatRateLimits["old-control"] = teamstore.ChatRateLimitState{ChatID: "old-control", Reason: "blocked"}
		state.DashboardViews["old-view"] = teamstore.DashboardViewRecord{ID: "old-view", ChatID: "old-control"}
		state.DashboardNumbers["old-number"] = teamstore.DashboardNumberRecord{ID: "old-number", ChatID: "old-control"}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.ControlChatID = "old-control"
	bridge.reg.ControlChatURL = "https://teams.example/old-control"
	bridge.reg.ControlChatTopic = "old topic"
	bridge.reg.Chats = map[string]ChatState{"old-control": {SeenMessageIDs: []string{"seen-old"}}}
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")

	recreated, err := bridge.RecreateControlChat(context.Background())
	if err != nil {
		t.Fatalf("RecreateControlChat error: %v", err)
	}
	if recreated.OldChat.ID != "old-control" || recreated.NewChat.ID != "new-control" || bridge.reg.ControlChatID != "new-control" {
		t.Fatalf("recreated control mismatch: recreated=%#v reg=%#v", recreated, bridge.reg)
	}
	if _, ok := bridge.reg.Chats["old-control"]; ok {
		t.Fatalf("old control chat state should be retired: %#v", bridge.reg.Chats)
	}
	if len(sent) != 3 || sent[0].ChatID != "old-control" || sent[0].Mentions != 1 || !strings.Contains(sent[0].Content, "This chat moved") || !strings.Contains(sent[0].Content, "new-control") {
		t.Fatalf("old control migration notice = %#v", sent)
	}
	if !strings.Contains(sent[0].Content, `<a href="https://teams.microsoft.com/l/chat/new-control/0">Open the new Control chat</a>`) {
		t.Fatalf("old control migration notice did not render a clickable link: %s", sent[0].Content)
	}
	if strings.Contains(sent[0].Content, "🧑‍💻 User") {
		t.Fatalf("old control migration notice was rendered as a user message: %s", sent[0].Content)
	}
	if sent[1].ChatID != "new-control" || sent[1].Mentions != 1 || !strings.Contains(sent[1].Content, "Control chat recreated.") || sent[2].ChatID != "new-control" || !strings.Contains(sent[2].Content, "control chat is ready") {
		t.Fatalf("recreated control messages = %#v", sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	if state.ControlChat.TeamsChatID != "new-control" {
		t.Fatalf("control binding not updated: %#v", state.ControlChat)
	}
	if _, ok := state.ChatPolls["old-control"]; ok {
		t.Fatalf("old control poll was not retired: %#v", state.ChatPolls["old-control"])
	}
	if _, ok := state.ChatSequences["old-control"]; ok {
		t.Fatalf("old control sequence was not retired: %#v", state.ChatSequences["old-control"])
	}
	if _, ok := state.ChatRateLimits["old-control"]; ok {
		t.Fatalf("old control rate limit was not retired: %#v", state.ChatRateLimits["old-control"])
	}
	if _, ok := state.DashboardViews["old-view"]; ok {
		t.Fatalf("old control dashboard view was not retired")
	}
	if _, ok := state.DashboardNumbers["old-number"]; ok {
		t.Fatalf("old control dashboard number was not retired")
	}
}

func TestBridgeRecreateSessionChatRebindsSessionAndRetiresOldRouting(t *testing.T) {
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			writeTestOnlineMeeting(w, "new-work", decodeTestOnlineMeetingSubject(t, r))
		case r.Method == http.MethodPost && (r.URL.Path == "/chats/chat-1/messages" || r.URL.Path == "/chats/new-work/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode recreated session message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"new-work-message-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	store := newBridgeTestStore(t)
	now := time.Now()
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["s001"] = teamstore.SessionContext{
			ID:            "s001",
			Status:        teamstore.SessionStatusActive,
			TeamsChatID:   "chat-1",
			TeamsChatURL:  "https://teams.example/chat-1",
			TeamsTopic:    "topic",
			CodexThreadID: "thread-1",
			Cwd:           "/workspace/demo",
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		state.ChatPolls["chat-1"] = teamstore.ChatPollState{ChatID: "chat-1", Seeded: true}
		state.ChatSequences["chat-1"] = teamstore.ChatSequenceState{ChatID: "chat-1", Next: 7}
		state.ChatRateLimits["chat-1"] = teamstore.ChatRateLimitState{ChatID: "chat-1", Reason: "blocked"}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.reg.Sessions[0].CodexThreadID = "thread-1"
	bridge.reg.Sessions[0].Cwd = "/workspace/demo"
	bridge.reg.Chats = map[string]ChatState{"chat-1": {SeenMessageIDs: []string{"seen-old"}}}
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")

	recreated, err := bridge.RecreateSessionChat(context.Background(), "chat-1", RecreateSessionChatOptions{})
	if err != nil {
		t.Fatalf("RecreateSessionChat error: %v", err)
	}
	if recreated.SessionID != "s001" || recreated.OldChat.ID != "chat-1" || recreated.NewChat.ID != "new-work" {
		t.Fatalf("recreated session mismatch: %#v", recreated)
	}
	session := bridge.reg.SessionByID("s001")
	if session == nil || session.ChatID != "new-work" || session.CodexThreadID != "thread-1" || session.Cwd != "/workspace/demo" {
		t.Fatalf("registry session not rebound: %#v", bridge.reg.Sessions)
	}
	if _, ok := bridge.reg.Chats["chat-1"]; ok {
		t.Fatalf("old work chat state should be retired: %#v", bridge.reg.Chats)
	}
	if len(sent) != 3 || sent[0].ChatID != "chat-1" || sent[0].Mentions != 1 || !strings.Contains(sent[0].Content, "This chat moved") || !strings.Contains(sent[0].Content, "new-work") {
		t.Fatalf("old work migration notice = %#v", sent)
	}
	if !strings.Contains(sent[0].Content, `<a href="https://teams.microsoft.com/l/chat/new-work/0">Open the new Work chat for s001</a>`) {
		t.Fatalf("old work migration notice did not render a clickable link: %s", sent[0].Content)
	}
	if strings.Contains(sent[0].Content, "🧑‍💻 User") {
		t.Fatalf("old work migration notice was rendered as a user message: %s", sent[0].Content)
	}
	if sent[1].ChatID != "new-work" || sent[1].Mentions != 1 || !strings.Contains(sent[1].Content, "Work chat recreated: s001.") || sent[2].ChatID != "new-work" || !strings.Contains(PlainTextFromTeamsHTML(sent[2].Content), "ready") {
		t.Fatalf("recreated work messages = %#v", sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	durable := state.Sessions["s001"]
	if durable.TeamsChatID != "new-work" || durable.CodexThreadID != "thread-1" || durable.Cwd != "/workspace/demo" {
		t.Fatalf("durable session not rebound: %#v", durable)
	}
	if _, ok := state.ChatPolls["chat-1"]; ok {
		t.Fatalf("old work poll was not retired: %#v", state.ChatPolls["chat-1"])
	}
	if _, ok := state.ChatSequences["chat-1"]; ok {
		t.Fatalf("old work sequence was not retired: %#v", state.ChatSequences["chat-1"])
	}
	if _, ok := state.ChatRateLimits["chat-1"]; ok {
		t.Fatalf("old work rate limit was not retired: %#v", state.ChatRateLimits["chat-1"])
	}
}

func TestBridgeRestoresRoutingFromDurableStateWhenRegistryIsMissing(t *testing.T) {
	store := newBridgeTestStore(t)
	now := time.Now()
	controlTopic := ControlChatTitle(ChatTitleOptions{MachineLabel: machineLabel()})
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat = teamstore.ControlChatBinding{
			TeamsChatID:    "control-chat",
			TeamsChatURL:   "https://teams.example/control",
			TeamsChatTopic: controlTopic,
			BoundAt:        now,
			UpdatedAt:      now,
		}
		state.Sessions["s777"] = teamstore.SessionContext{
			ID:            "s777",
			Status:        teamstore.SessionStatusActive,
			TeamsChatID:   "work-chat",
			TeamsChatURL:  "https://teams.example/work",
			TeamsTopic:    "work topic",
			CodexThreadID: "thread-777",
			Cwd:           "/workspace/demo",
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		state.InboundEvents["inbound:work-chat:m1"] = teamstore.InboundEvent{
			ID:             "inbound:work-chat:m1",
			SessionID:      "s777",
			TeamsChatID:    "work-chat",
			TeamsMessageID: "m1",
			Status:         teamstore.InboundStatusQueued,
		}
		state.OutboxMessages["outbox:sent"] = teamstore.OutboxMessage{
			ID:             "outbox:sent",
			SessionID:      "s777",
			TeamsChatID:    "work-chat",
			TeamsMessageID: "sent-1",
			Status:         teamstore.OutboxStatusSent,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("durable restore should not call Graph: %s %s", r.Method, r.URL.String())
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
	bridge.reg = Registry{Version: 1, Chats: map[string]ChatState{}}

	chat, err := bridge.EnsureControlChat(context.Background())
	if err != nil {
		t.Fatalf("EnsureControlChat error: %v", err)
	}
	if chat.ID != "control-chat" || bridge.reg.ControlChatID != "control-chat" {
		t.Fatalf("control chat was not restored: chat=%#v reg=%#v", chat, bridge.reg)
	}
	session := bridge.reg.SessionByID("s777")
	if session == nil || session.ChatID != "work-chat" || session.CodexThreadID != "thread-777" || session.Cwd != "/workspace/demo" {
		t.Fatalf("session was not restored: %#v", bridge.reg.Sessions)
	}
	if !bridge.reg.HasSeen("work-chat", "m1") || !bridge.reg.HasSent("work-chat", "sent-1") {
		t.Fatalf("seen/sent ids were not restored: %#v", bridge.reg.Chats)
	}
}

func TestBridgeMigratesLegacyRegistryProjectionIntoDurableState(t *testing.T) {
	store := newBridgeTestStore(t)
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	controlTopic := ControlChatTitle(ChatTitleOptions{MachineLabel: machineLabel()})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("legacy registry migration should not call Graph: %s %s", r.Method, r.URL.String())
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
	bridge.reg = Registry{
		Version:          1,
		UserID:           "user-1",
		UserPrincipal:    "user@example.test",
		ControlChatID:    "legacy-control",
		ControlChatURL:   "https://teams.example/legacy-control",
		ControlChatTopic: controlTopic,
		Sessions: []Session{{
			ID:            "s042",
			ChatID:        "legacy-work",
			ChatURL:       "https://teams.example/legacy-work",
			Topic:         "legacy work",
			Status:        "active",
			CodexThreadID: "thread-42",
			Cwd:           "/workspace/legacy",
			CreatedAt:     now,
			UpdatedAt:     now,
		}},
		Chats: map[string]ChatState{
			"legacy-work": {
				SeenMessageIDs: []string{"seen-1"},
				SentMessageIDs: []string{"sent-1"},
			},
		},
	}

	chat, err := bridge.EnsureControlChat(context.Background())
	if err != nil {
		t.Fatalf("EnsureControlChat error: %v", err)
	}
	if chat.ID != "legacy-control" {
		t.Fatalf("control chat = %#v, want legacy-control", chat)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state error: %v", err)
	}
	if state.ControlChat.TeamsChatID != "legacy-control" || state.MachineIdentity.AccountID != "user-1" {
		t.Fatalf("control binding was not migrated: control=%#v machine=%#v", state.ControlChat, state.MachineIdentity)
	}
	session := state.Sessions["s042"]
	if session.TeamsChatID != "legacy-work" || session.CodexThreadID != "thread-42" || session.Cwd != "/workspace/legacy" {
		t.Fatalf("legacy session was not migrated: %#v", session)
	}
	if inbound := state.InboundEvents["inbound:legacy-work:seen-1"]; inbound.TeamsMessageID != "seen-1" || inbound.Source != "registry_migration" {
		t.Fatalf("legacy seen id was not migrated: %#v", inbound)
	}
	foundSent := false
	for _, outbox := range state.OutboxMessages {
		if outbox.TeamsChatID == "legacy-work" && outbox.TeamsMessageID == "sent-1" && outbox.Status == teamstore.OutboxStatusSent {
			foundSent = true
			break
		}
	}
	if !foundSent {
		t.Fatalf("legacy sent id was not migrated: %#v", state.OutboxMessages)
	}
}

func TestBridgeSessionRenameUpdatesTeamsTopicAndDurableState(t *testing.T) {
	var patchedTopic string
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/chats/chat-1":
			var payload struct {
				Topic string `json:"topic"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode patch: %v", err)
			}
			patchedTopic = payload.Topic
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/chats/chat-1/messages":
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			sent = append(sent, bridgeSentMessage{ChatID: "chat-1", Content: body.Body.Content})
			_, _ = fmt.Fprint(w, `{"id":"sent-1","messageType":"message"}`)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("rename"), "/rename release audit"); err != nil {
		t.Fatalf("/rename error: %v", err)
	}
	if !strings.Contains(patchedTopic, "release audit") || !strings.Contains(patchedTopic, DefaultWorkChatMarker) {
		t.Fatalf("patched topic = %q", patchedTopic)
	}
	if got := bridge.reg.SessionByChatID("chat-1").Topic; got != patchedTopic {
		t.Fatalf("registry topic = %q, want %q", got, patchedTopic)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions["s001"].TeamsTopic; got != patchedTopic {
		t.Fatalf("durable topic = %q, want %q", got, patchedTopic)
	}
	if len(sent) != 1 || !strings.Contains(sent[0].Content, "renamed") {
		t.Fatalf("rename ack = %#v", sent)
	}
}

func TestBridgeUploadsArtifactManifestFromCodexResult(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if err := writeTokenCache(cfg.CachePath, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot error: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "artifact.txt"), []byte("artifact-data"), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	chatGraph, chatSent := newBridgeTestGraph(t)
	fileGraph, fileSent := newOutboundAttachmentGraph(t)
	resultText := "done\n```" + ArtifactManifestFenceInfo + "\n" + `{"version":1,"files":[{"path":"artifact.txt","name":"artifact.txt"}]}` + "\n```"
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: resultText, CodexThreadID: "thread-1", CodexTurnID: "turn-1"}}
	bridge := newBridgeTestBridge(chatGraph, store, executor)
	bridge.fileGraph = fileGraph

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("artifact-turn"), "make artifact"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(*chatSent) != 3 || !strings.Contains((*chatSent)[0].Content, "Codex is working") || !strings.Contains((*chatSent)[1].Content, "done") || strings.Contains((*chatSent)[1].Content, ArtifactManifestFenceInfo) || !strings.Contains((*chatSent)[2].Content, "attachment") {
		t.Fatalf("final response should hide helper artifact manifest and keep normal answer visible: %#v", *chatSent)
	}
	if len(*fileSent) != 0 {
		t.Fatalf("file graph should not send Teams messages: %#v", *fileSent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if len(state.ArtifactRecords) != 1 {
		t.Fatalf("artifact records = %#v, want one", state.ArtifactRecords)
	}
	for _, artifact := range state.ArtifactRecords {
		if artifact.Status != "uploaded" || artifact.Path != "artifact.txt" || !strings.Contains(artifact.UploadName, "codex-artifact") {
			t.Fatalf("artifact record mismatch: %#v", artifact)
		}
	}
}

func TestBridgeSyncLinkedTranscriptSeedsThenImportsNewRecords(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := strings.Join([]string{
		`{"id":"u1","role":"user","text":"hello"}`,
		`{"id":"a1","role":"assistant","text":"hi"}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				FirstPrompt: "hello",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("initial sync should seed without sending, sent=%#v", *sent)
	}
	if err := os.WriteFile(transcriptPath, []byte(initial+`{"id":"a2","role":"assistant","text":"new local answer"}`+"\n"), 0o600); err != nil {
		t.Fatalf("append transcript: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("second sync error: %v", err)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "new local answer") {
		t.Fatalf("synced messages = %#v", *sent)
	}
}

func TestBridgeSyncLinkedTranscriptSkipsWhileTranscriptImporting(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}
	beforeState, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load before import error: %v", err)
	}
	beforeCheckpoint := beforeState.ImportCheckpoints[transcriptCheckpointID("s001")]
	if err := bridge.markTranscriptImportStarted(context.Background(), *session, transcriptPath); err != nil {
		t.Fatalf("mark import started: %v", err)
	}
	if err := os.WriteFile(transcriptPath, []byte(initial+`{"id":"a2","role":"assistant","text":"new answer during import"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write updated transcript: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("sync during import error: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("sync during import should not send live catch-up messages, sent=%#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID("s001")]
	if checkpoint.Status != importCheckpointStatusImporting || checkpoint.LastRecordID != beforeCheckpoint.LastRecordID {
		t.Fatalf("checkpoint during import = %#v, want importing at previous record %#v", checkpoint, beforeCheckpoint)
	}
}

func TestBridgeSyncLinkedTranscriptMirrorsLocalCodexConversation(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}

	next := strings.Join([]string{
		initial,
		`{"type":"response_item","payload":{"id":"u2","type":"message","role":"user","content":[{"type":"input_text","text":"local cli prompt"}]}}`,
		`{"type":"response_item","payload":{"id":"tool-1","type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"go test ./...\"}"}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"s2","message":"local cli visible status","phase":"commentary"}}`,
		`{"type":"response_item","payload":{"id":"a2","type":"message","role":"assistant","content":[{"type":"output_text","text":"local cli final answer"}]}}`,
		``,
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(next), 0o600); err != nil {
		t.Fatalf("write updated transcript: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("second sync error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent messages = %#v, want user, status, answer", *sent)
	}
	plain := make([]string, 0, len(*sent))
	for _, msg := range *sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(plain, "\n")
	for _, want := range []string{
		"🧑‍💻 User:\nlocal cli prompt",
		"🤖 ⏳ Codex status:\nlocal cli visible status",
		"🤖 ✅ Codex answer:\nlocal cli final answer",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("synced transcript missing %q in:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "go test ./...") || strings.Contains(joined, "exec_command") {
		t.Fatalf("synced transcript leaked command/tool output:\n%s", joined)
	}
	if strings.Index(joined, "local cli prompt") > strings.Index(joined, "local cli visible status") ||
		strings.Index(joined, "local cli visible status") > strings.Index(joined, "local cli final answer") {
		t.Fatalf("synced transcript order is wrong:\n%s", joined)
	}
	if (*sent)[0].Mentions != 0 || (*sent)[1].Mentions != 0 {
		t.Fatalf("user/status sync should not mention owner: %#v", *sent)
	}
	if (*sent)[2].Mentions != 1 || !strings.Contains((*sent)[2].Content, `<at id="0">`) || !strings.Contains((*sent)[2].Content, "Codex finished responding") {
		t.Fatalf("synced local final answer should mention owner at completion: %#v", (*sent)[2])
	}
}

func TestBridgeSyncLinkedTranscriptSkipsLargeAutomaticBacklog(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}

	var updated strings.Builder
	updated.WriteString(initial)
	for i := 0; i < transcriptSyncMaxAutoBacklogRecords+5; i++ {
		updated.WriteString(fmt.Sprintf(`{"id":"a%03d","role":"assistant","text":"backlog answer %03d"}`+"\n", i, i))
	}
	if err := os.WriteFile(transcriptPath, []byte(updated.String()), 0o600); err != nil {
		t.Fatalf("write backlog transcript: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("backlog sync error: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("large automatic backlog should not flood Teams, sent=%#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID("s001")]
	if checkpoint.LastRecordID != fmt.Sprintf("a%03d", transcriptSyncMaxAutoBacklogRecords+4) {
		t.Fatalf("checkpoint = %#v, want advanced to latest backlog record", checkpoint)
	}
}

func TestBridgeSyncLinkedTranscriptsIfDueThrottlesScans(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	discoverCalls := 0
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		discoverCalls++
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}

	now := time.Unix(1000, 0)
	if err := bridge.syncLinkedTranscriptsIfDue(context.Background(), now); err != nil {
		t.Fatalf("first sync error: %v", err)
	}
	if err := bridge.syncLinkedTranscriptsIfDue(context.Background(), now.Add(transcriptSyncMinInterval/2)); err != nil {
		t.Fatalf("throttled sync error: %v", err)
	}
	if discoverCalls != 1 {
		t.Fatalf("discoverCalls = %d, want one scan inside throttle interval", discoverCalls)
	}
	if err := bridge.syncLinkedTranscriptsIfDue(context.Background(), now.Add(transcriptSyncMinInterval)); err != nil {
		t.Fatalf("due sync error: %v", err)
	}
	if discoverCalls != 2 {
		t.Fatalf("discoverCalls = %d, want second scan when due", discoverCalls)
	}
}

func TestBridgeSyncLinkedTranscriptSkipsTeamsOriginUserPrompt(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}
	inbound, _, err := store.PersistInbound(context.Background(), teamstore.InboundEvent{
		SessionID:      "s001",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-origin",
		TextHash:       normalizedTextHash("team prompt"),
		Source:         "teams",
		Status:         teamstore.InboundStatusPersisted,
	})
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{SessionID: "s001", InboundEventID: inbound.ID}); err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	next := initial +
		`{"id":"u2","role":"user","text":"team prompt"}` + "\n" +
		`{"id":"a2","role":"assistant","text":"answer from codex"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(next), 0o600); err != nil {
		t.Fatalf("write updated transcript: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("second sync error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent messages = %#v, want one assistant catch-up", *sent)
	}
	plain := PlainTextFromTeamsHTML((*sent)[0].Content)
	if strings.Contains(plain, "team prompt") || !strings.Contains(plain, "answer from codex") {
		t.Fatalf("Teams-origin prompt was not skipped correctly: %q", plain)
	}
}

func TestBridgeSyncLinkedTranscriptSkipsTeamsOriginNoiseAndDeliveredFinal(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}
	inbound, _, err := store.PersistInbound(context.Background(), teamstore.InboundEvent{
		SessionID:      "s001",
		TeamsChatID:    "chat-1",
		TeamsMessageID: "teams-origin",
		TextHash:       normalizedTextHash("team prompt"),
		Source:         "teams",
		Status:         teamstore.InboundStatusPersisted,
	})
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{SessionID: "s001", InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:             "outbox:delivered-final",
		SessionID:      "s001",
		TurnID:         turn.ID,
		TeamsChatID:    "chat-1",
		Kind:           "final",
		Body:           "answer from codex",
		Status:         teamstore.OutboxStatusSent,
		TeamsMessageID: "teams-final",
	}); err != nil {
		t.Fatalf("QueueOutbox final error: %v", err)
	}
	if _, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:             "outbox:delivered-status",
		SessionID:      "s001",
		TurnID:         turn.ID,
		TeamsChatID:    "chat-1",
		Kind:           "codex-progress-001",
		Body:           "already streamed status",
		Status:         teamstore.OutboxStatusSent,
		TeamsMessageID: "teams-status",
	}); err != nil {
		t.Fatalf("QueueOutbox status error: %v", err)
	}
	augmentedPrompt := TeamsCodexPrompt("team prompt")
	next := initial +
		`{"id":"u2","role":"user","text":` + strconv.Quote(augmentedPrompt) + `}` + "\n" +
		`{"id":"u3","role":"user","text":` + strconv.Quote(augmentedPrompt) + `}` + "\n" +
		`{"id":"tool-1","type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"go test ./...\"}"}` + "\n" +
		`{"type":"event_msg","payload":{"type":"agent_message","id":"s2","message":"already streamed status","phase":"commentary"}}` + "\n" +
		`{"id":"a2","role":"assistant","text":"answer from codex"}` + "\n" +
		`{"id":"a3","role":"assistant","text":"answer from codex"}` + "\n" +
		`{"id":"a4","role":"assistant","text":"local follow-up"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(next), 0o600); err != nil {
		t.Fatalf("write updated transcript: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("second sync error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent messages = %#v, want only one local follow-up", *sent)
	}
	plain := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(plain, "local follow-up") {
		t.Fatalf("synced message = %q, want local follow-up", plain)
	}
	for _, leaked := range []string{"team prompt", "teams-outbound", "already streamed status", "answer from codex", "exec_command"} {
		if strings.Contains(plain, leaked) {
			t.Fatalf("synced message leaked %q: %q", leaked, plain)
		}
	}
}

func TestDeliveredTranscriptHashesIncludeLiveStatusAndFinal(t *testing.T) {
	state := teamstore.State{OutboxMessages: map[string]teamstore.OutboxMessage{
		"status": {
			SessionID:      "s001",
			TeamsChatID:    "chat-1",
			Kind:           "codex-progress-001",
			Body:           "already streamed status",
			Status:         teamstore.OutboxStatusSent,
			TeamsMessageID: "teams-status",
		},
		"final": {
			SessionID:      "s001",
			TeamsChatID:    "chat-1",
			Kind:           "final",
			Body:           "already delivered answer",
			Status:         teamstore.OutboxStatusSent,
			TeamsMessageID: "teams-final",
		},
		"queued": {
			SessionID:   "s001",
			TeamsChatID: "chat-1",
			Kind:        "codex-progress-002",
			Body:        "not delivered yet",
			Status:      teamstore.OutboxStatusQueued,
		},
	}}
	hashes := deliveredTranscriptHashes(state, "s001")
	if !shouldSkipDeliveredTranscriptRecord(TranscriptRecord{Kind: TranscriptKindStatus}, "already streamed status", hashes) {
		t.Fatal("delivered live status was not recognized as already sent")
	}
	if !shouldSkipDeliveredTranscriptRecord(TranscriptRecord{Kind: TranscriptKindAssistant}, "already delivered answer", hashes) {
		t.Fatal("delivered final answer was not recognized as already sent")
	}
	if shouldSkipDeliveredTranscriptRecord(TranscriptRecord{Kind: TranscriptKindStatus}, "not delivered yet", hashes) {
		t.Fatal("queued unsent status should not be treated as delivered")
	}
}

func TestBridgeQueuesTeamsPromptWhileExternalCodexTranscriptActive(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-1", transcriptPath)
	defer restoreDiscover()
	graph, sent := newBridgeAsyncQueueGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 1),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-1")
	beforeState, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load seeded state error: %v", err)
	}
	beforeCheckpoint := beforeState.ImportCheckpoints[transcriptCheckpointID(session.ID)]

	activeTranscript := initial +
		`{"id":"u-local","role":"user","text":"local CLI prompt"}` + "\n" +
		`{"type":"event_msg","payload":{"type":"agent_message","id":"s-local","message":"local CLI is editing files","phase":"commentary"}}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(activeTranscript), 0o600); err != nil {
		t.Fatalf("write active transcript: %v", err)
	}

	msg := bridgePollMessage("teams-during-local", "2026-05-03T01:01:00Z", "teams prompt during local")
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, msg, "teams prompt during local"); err != nil {
		t.Fatalf("handleSessionMessage while local active error: %v", err)
	}
	select {
	case got := <-executor.started:
		t.Fatalf("Teams turn started while local CLI transcript was active: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "Codex is active in the CLI") {
		t.Fatalf("active local CLI ack = %#v, want one clear queued notice", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state error: %v", err)
	}
	if checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]; checkpoint.LastRecordID != beforeCheckpoint.LastRecordID || checkpoint.LastSourceLine != beforeCheckpoint.LastSourceLine {
		t.Fatalf("checkpoint advanced during active local CLI turn: %#v, before %#v", checkpoint, beforeCheckpoint)
	}
	if got := queuedTurnCountForSession(state, session.ID); got != 1 {
		t.Fatalf("queued turn count = %d, want 1", got)
	}

	completedTranscript := activeTranscript +
		`{"id":"a-local","role":"assistant","text":"local CLI final answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(completedTranscript), 0o600); err != nil {
		t.Fatalf("write completed transcript: %v", err)
	}
	if err := bridge.processQueuedTurns(context.Background()); err != nil {
		t.Fatalf("processQueuedTurns after local completion error: %v", err)
	}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "teams prompt during local") {
			t.Fatalf("queued Teams prompt = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queued Teams prompt did not start after local CLI completed")
	}
	executor.release <- struct{}{}
	waitForCompletedTurnCount(t, store, session.ID, 1)

	var plain []string
	for _, msg := range *sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(plain, "\n---\n")
	for _, want := range []string{
		"Codex is active in the CLI",
		"🧑‍💻 User:\nlocal CLI prompt",
		"🤖 ⏳ Codex status:\nlocal CLI is editing files",
		"🤖 ✅ Codex answer:\nlocal CLI final answer",
		"🤖 ✅ Codex answer:\ndone 1: teams prompt during local",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("combined transcript missing %q in:\n%s", want, joined)
		}
	}
	if strings.Index(joined, "local CLI final answer") > strings.Index(joined, "done 1: teams prompt during local") {
		t.Fatalf("Teams turn answer was sent before local CLI catch-up:\n%s", joined)
	}
}

func TestBridgeSyncsCompletedLocalTranscriptBeforeStartingTeamsPrompt(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-1", transcriptPath)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 1),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-1")

	completedTranscript := initial +
		`{"id":"u-local","role":"user","text":"local completed prompt"}` + "\n" +
		`{"type":"event_msg","payload":{"type":"agent_message","id":"s-local","message":"local completed status","phase":"commentary"}}` + "\n" +
		`{"id":"a-local","role":"assistant","text":"local completed answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(completedTranscript), 0o600); err != nil {
		t.Fatalf("write completed transcript: %v", err)
	}

	msg := bridgePollMessage("teams-after-local", "2026-05-03T01:02:00Z", "teams prompt after local")
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, msg, "teams prompt after local"); err != nil {
		t.Fatalf("handleSessionMessage after local completion error: %v", err)
	}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "teams prompt after local") {
			t.Fatalf("started prompt = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Teams prompt did not start after completed local catch-up")
	}
	executor.release <- struct{}{}
	waitForCompletedTurnCount(t, store, session.ID, 1)

	var plain []string
	for _, msg := range *sent {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	joined := strings.Join(plain, "\n---\n")
	for _, want := range []string{
		"🧑‍💻 User:\nlocal completed prompt",
		"🤖 ⏳ Codex status:\nlocal completed status",
		"🤖 ✅ Codex answer:\nlocal completed answer",
		"Codex is working. Request accepted.",
		"🤖 ✅ Codex answer:\ndone 1: teams prompt after local",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("transcript missing %q in:\n%s", want, joined)
		}
	}
	if strings.Index(joined, "local completed answer") > strings.Index(joined, "Codex is working. Request accepted.") ||
		strings.Index(joined, "Codex is working. Request accepted.") > strings.Index(joined, "done 1: teams prompt after local") {
		t.Fatalf("completed local catch-up should precede Teams ack and answer:\n%s", joined)
	}
}

func TestBridgeQueuedTurnWaitsForLocalTranscriptCatchupLimit(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-1", transcriptPath)
	defer restoreDiscover()
	graph, sent := newBridgeAsyncQueueGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 1),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-1")

	var updated strings.Builder
	updated.WriteString(initial)
	for i := 0; i < transcriptSyncMaxRecordsPerSessionPerCycle+2; i++ {
		updated.WriteString(fmt.Sprintf(`{"id":"a-local-%02d","role":"assistant","text":"local answer %02d"}`+"\n", i, i))
	}
	if err := os.WriteFile(transcriptPath, []byte(updated.String()), 0o600); err != nil {
		t.Fatalf("write multi-record transcript: %v", err)
	}

	msg := bridgePollMessage("teams-after-catchup", "2026-05-03T01:03:00Z", "teams prompt after catchup")
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, msg, "teams prompt after catchup"); err != nil {
		t.Fatalf("handleSessionMessage during catch-up error: %v", err)
	}
	select {
	case got := <-executor.started:
		t.Fatalf("Teams turn started before local catch-up drained: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	if got := countSentPlainContaining(*sent, "local answer "); got != transcriptSyncMaxRecordsPerSessionPerCycle {
		t.Fatalf("synced local answer count = %d, want first catch-up batch of %d", got, transcriptSyncMaxRecordsPerSessionPerCycle)
	}
	if !sentPlainContains(*sent, "syncing recent Codex updates first") {
		t.Fatalf("catch-up queued ack missing in %#v", *sent)
	}

	if err := bridge.processQueuedTurns(context.Background()); err != nil {
		t.Fatalf("processQueuedTurns while catch-up remains error: %v", err)
	}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "teams prompt after catchup") {
			t.Fatalf("started prompt = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Teams prompt did not start after remaining catch-up drained")
	}
	executor.release <- struct{}{}
	waitForCompletedTurnCount(t, store, session.ID, 1)
	if !sentPlainContains(*sent, "local answer 09") {
		t.Fatalf("remaining local catch-up was not sent before Teams answer: %#v", *sent)
	}
}

func TestBridgeAllowsTeamsPromptAfterLocalTranscriptFailureTerminal(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-1", transcriptPath)
	defer restoreDiscover()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 1),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-1")

	failedTranscript := initial +
		`{"id":"u-local","role":"user","text":"local prompt before failure"}` + "\n" +
		`{"type":"turn.failed","error":{"message":"local CLI failed cleanly"}}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(failedTranscript), 0o600); err != nil {
		t.Fatalf("write failed transcript: %v", err)
	}

	msg := bridgePollMessage("teams-after-failed-local", "2026-05-03T01:04:00Z", "teams prompt after failed local")
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, msg, "teams prompt after failed local"); err != nil {
		t.Fatalf("handleSessionMessage after failed local transcript error: %v", err)
	}
	select {
	case got := <-executor.started:
		if !strings.Contains(got, "teams prompt after failed local") {
			t.Fatalf("started prompt = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Teams prompt did not start after terminal local failure")
	}
	executor.release <- struct{}{}
	waitForCompletedTurnCount(t, store, session.ID, 1)
	joined := sentPlainJoined(*sent)
	for _, want := range []string{
		"🧑‍💻 User:\nlocal prompt before failure",
		"🤖 ⏳ Codex status:\nlocal CLI failed cleanly",
		"🤖 ✅ Codex answer:\ndone 1: teams prompt after failed local",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("failed local transcript flow missing %q in:\n%s", want, joined)
		}
	}
}

func TestBridgeBlocksQueuedTurnOnLocalToolOnlyTranscriptActivity(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	restoreDiscover := stubDiscoverCodexSession(t, "thread-1", transcriptPath)
	defer restoreDiscover()
	graph, sent := newBridgeAsyncQueueGraph(t)
	store := newBridgeTestStore(t)
	executor := &serialStreamingExecutor{
		started: make(chan string, 1),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	session := seedLinkedTranscriptForTest(t, bridge, transcriptPath, "thread-1")

	activeToolOnly := initial + `{"type":"response_item","payload":{"id":"tool-local","type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"go test ./...\"}"}}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(activeToolOnly), 0o600); err != nil {
		t.Fatalf("write tool transcript: %v", err)
	}
	msg := bridgePollMessage("teams-during-tool", "2026-05-03T01:05:00Z", "teams prompt during tool")
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, msg, "teams prompt during tool"); err != nil {
		t.Fatalf("handleSessionMessage during tool-only local activity error: %v", err)
	}
	select {
	case got := <-executor.started:
		t.Fatalf("Teams turn started while local tool-only transcript was active: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	if len(*sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML((*sent)[0].Content), "Codex is active in the CLI") {
		t.Fatalf("tool-only active ack = %#v", *sent)
	}
}

func TestBridgeOutboxRateLimitBlocksOnlyFailingChat(t *testing.T) {
	store := newBridgeTestStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"code":"TooManyRequests"}}`, http.StatusTooManyRequests)
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.sendToChat(context.Background(), "chat-1", "rate limited"); err == nil {
		t.Fatal("sendToChat succeeded, want 429 error")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !state.ChatRateLimits["chat-1"].BlockedUntil.After(time.Now()) {
		t.Fatalf("chat-1 was not rate limited: %#v", state.ChatRateLimits)
	}
	if _, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "outbox:other",
		TeamsChatID: "chat-2",
		Kind:        "other",
		Body:        "other",
	}); err != nil {
		t.Fatalf("QueueOutbox other error: %v", err)
	}
	pending, err := store.PendingOutbox(context.Background())
	if err != nil {
		t.Fatalf("PendingOutbox error: %v", err)
	}
	if len(pending) != 1 || pending[0].TeamsChatID != "chat-2" {
		t.Fatalf("pending after rate limit = %#v, want only chat-2", pending)
	}
}

func TestBridgeFlushPendingOutboxContinuesAfterRateLimitedChat(t *testing.T) {
	store := newBridgeTestStore(t)
	var sent []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
		if chatID == "chat-1" {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests"}}`, http.StatusTooManyRequests)
			return
		}
		sent = append(sent, chatID)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%s","messageType":"message"}`, chatID)
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})
	if _, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{ID: "outbox:blocked", TeamsChatID: "chat-1", Kind: "helper", Body: "blocked"}); err != nil {
		t.Fatalf("QueueOutbox blocked error: %v", err)
	}
	if _, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{ID: "outbox:open", TeamsChatID: "chat-2", Kind: "helper", Body: "open"}); err != nil {
		t.Fatalf("QueueOutbox open error: %v", err)
	}

	err := bridge.flushPendingOutbox(context.Background(), "", "")
	if err == nil || !isGraphRateLimitError(err) {
		t.Fatalf("flushPendingOutbox error = %v, want rate-limit error after continuing", err)
	}
	if len(sent) != 1 || sent[0] != "chat-2" {
		t.Fatalf("sent chats = %#v, want chat-2 despite chat-1 rate limit", sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state error: %v", err)
	}
	if got := state.OutboxMessages["outbox:open"].Status; got != teamstore.OutboxStatusSent {
		t.Fatalf("chat-2 outbox status = %q, want sent", got)
	}
	if got := state.OutboxMessages["outbox:blocked"].Status; got != teamstore.OutboxStatusQueued {
		t.Fatalf("chat-1 outbox status = %q, want queued", got)
	}
	if limit := state.ChatRateLimits["chat-1"]; !limit.BlockedUntil.After(time.Now()) || limit.PoisonOutboxID != "outbox:blocked" {
		t.Fatalf("chat-1 rate-limit state not recorded: %#v", limit)
	}
}

func TestBridgeOutboxRateLimitRestartReplayPreservesPerChatFIFO(t *testing.T) {
	store := newBridgeTestStore(t)
	var sent []bridgeSentMessage
	rateLimited := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode Graph request: %v", err)
		}
		if rateLimited && chatID == "chat-A" {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":{"code":"TooManyRequests"}}`, http.StatusTooManyRequests)
			return
		}
		sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%s-%d","messageType":"message"}`, chatID, len(sent))
	}))
	defer server.Close()
	newBridge := func() *Bridge {
		return newBridgeTestBridge(&GraphClient{
			auth:       &fakeGraphAuth{token: "access"},
			client:     server.Client(),
			baseURL:    server.URL,
			maxRetries: 0,
			backoffMin: time.Millisecond,
			backoffMax: time.Millisecond,
			sleep:      func(context.Context, time.Duration) error { return nil },
			jitter:     func(d time.Duration) time.Duration { return d },
		}, store, &recordingExecutor{})
	}
	for _, msg := range []teamstore.OutboxMessage{
		{ID: "outbox:A1", TeamsChatID: "chat-A", Kind: "helper", Body: "A1"},
		{ID: "outbox:A2", TeamsChatID: "chat-A", Kind: "helper", Body: "A2"},
		{ID: "outbox:B1", TeamsChatID: "chat-B", Kind: "helper", Body: "B1"},
		{ID: "outbox:A3", TeamsChatID: "chat-A", Kind: "helper", Body: "A3"},
	} {
		if _, _, err := store.QueueOutbox(context.Background(), msg); err != nil {
			t.Fatalf("QueueOutbox %s error: %v", msg.ID, err)
		}
	}

	err := newBridge().flushPendingOutbox(context.Background(), "", "")
	if err == nil || !isGraphRateLimitError(err) {
		t.Fatalf("first flush error = %v, want chat-A rate limit", err)
	}
	if len(sent) != 1 || sent[0].ChatID != "chat-B" || !strings.Contains(sent[0].Content, "B1") {
		t.Fatalf("first flush sent = %#v, want only chat-B B1", sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after first flush error: %v", err)
	}
	for _, id := range []string{"outbox:A1", "outbox:A2", "outbox:A3"} {
		if got := state.OutboxMessages[id].Status; got != teamstore.OutboxStatusQueued {
			t.Fatalf("%s status after rate limit = %q, want queued", id, got)
		}
	}
	if got := state.OutboxMessages["outbox:B1"].Status; got != teamstore.OutboxStatusSent {
		t.Fatalf("B1 status = %q, want sent", got)
	}

	beforeRestartSends := len(sent)
	if err := newBridge().flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("restart flush while chat-A is blocked should be a no-op, got %v", err)
	}
	if len(sent) != beforeRestartSends {
		t.Fatalf("restart flush sent while rate limit active: before=%d after=%d %#v", beforeRestartSends, len(sent), sent)
	}

	if err := store.ClearChatRateLimit(context.Background(), "chat-A"); err != nil {
		t.Fatalf("ClearChatRateLimit error: %v", err)
	}
	rateLimited = false
	if err := newBridge().flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("flush after rate limit clear error: %v", err)
	}
	if len(sent) != 4 {
		t.Fatalf("sent count after replay = %d, want 4: %#v", len(sent), sent)
	}
	want := []struct {
		chat string
		body string
	}{
		{"chat-B", "B1"},
		{"chat-A", "A1"},
		{"chat-A", "A2"},
		{"chat-A", "A3"},
	}
	for i, want := range want {
		if sent[i].ChatID != want.chat || !strings.Contains(sent[i].Content, want.body) {
			t.Fatalf("sent[%d] = %#v, want chat=%s body containing %s; all=%#v", i, sent[i], want.chat, want.body, sent)
		}
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after replay error: %v", err)
	}
	for _, id := range []string{"outbox:A1", "outbox:A2", "outbox:A3", "outbox:B1"} {
		if got := state.OutboxMessages[id].Status; got != teamstore.OutboxStatusSent {
			t.Fatalf("%s status = %q, want sent", id, got)
		}
	}
}

func TestBridgeFlushPendingOutboxDoesNotOvertakeFreshSendingMessage(t *testing.T) {
	store := newBridgeTestStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("flush should not send a later same-chat message while an earlier send lease is fresh: %s %s", r.Method, r.URL.String())
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:    &fakeGraphAuth{token: "access"},
		client:  server.Client(),
		baseURL: server.URL,
	}, store, &recordingExecutor{})
	first, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{ID: "outbox:first", TeamsChatID: "chat-1", Kind: "helper", Body: "first"})
	if err != nil {
		t.Fatalf("QueueOutbox first error: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(context.Background(), first.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt first error: %v", err)
	}
	if _, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{ID: "outbox:second", TeamsChatID: "chat-1", Kind: "helper", Body: "second"}); err != nil {
		t.Fatalf("QueueOutbox second error: %v", err)
	}

	err = bridge.flushPendingOutboxForChat(context.Background(), "chat-1")
	if err == nil || !isOutboxDeliveryDeferred(err) {
		t.Fatalf("flushPendingOutboxForChat error = %v, want same-chat ordering deferral", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state error: %v", err)
	}
	if got := state.OutboxMessages["outbox:second"].Status; got != teamstore.OutboxStatusQueued {
		t.Fatalf("second outbox status = %q, want queued", got)
	}
}

func TestBridgeSendQueuedOutboxDefersWhenChatAlreadyRateLimited(t *testing.T) {
	store := newBridgeTestStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("rate-limited chat should not call Graph: %s %s", r.Method, r.URL.String())
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:    &fakeGraphAuth{token: "access"},
		client:  server.Client(),
		baseURL: server.URL,
	}, store, &recordingExecutor{})
	outbox, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{ID: "outbox:blocked", TeamsChatID: "chat-1", Kind: "helper", Body: "blocked"})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	until := time.Now().Add(time.Minute)
	if _, err := store.SetChatRateLimit(context.Background(), "chat-1", until, "429"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}

	err = bridge.sendQueuedOutbox(context.Background(), outbox)
	if err == nil || !isOutboxDeliveryDeferred(err) {
		t.Fatalf("sendQueuedOutbox error = %v, want delivery deferred", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.OutboxMessages[outbox.ID].Status; got != teamstore.OutboxStatusQueued {
		t.Fatalf("outbox status = %q, want queued", got)
	}
}

func TestBridgeAckSendFailureDoesNotBlockCodexTurn(t *testing.T) {
	store := newBridgeTestStore(t)
	requests := 0
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		requests++
		if requests <= 4 {
			http.Error(w, `{"error":{"code":"TooManyRequests"}}`, http.StatusTooManyRequests)
			return
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		sent = append(sent, bridgeSentMessage{ChatID: "chat-1", Content: body.Body.Content})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, requests)
	}))
	defer server.Close()
	executor := &recordingExecutor{result: ExecutionResult{Text: "final despite ack failure", CodexThreadID: "thread-1", CodexTurnID: "turn-1"}}
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, executor)

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("ack-failure"), "run anyway"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(executor.prompts) != 1 {
		t.Fatalf("executor prompts = %#v, want turn to run", executor.prompts)
	}
	if len(sent) != 2 || !strings.Contains(sent[0].Content, "Codex is working") || !strings.Contains(sent[1].Content, "final despite ack failure") {
		t.Fatalf("ack/final response order mismatch after ack failure: %#v", sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	var ack, final teamstore.OutboxMessage
	for _, msg := range state.OutboxMessages {
		switch msg.Kind {
		case "ack":
			ack = msg
		case "final":
			final = msg
		}
	}
	if ack.Status != teamstore.OutboxStatusSent || ack.LastSendError != "" {
		t.Fatalf("ack outbox should be sent before final after retry: %#v", ack)
	}
	if final.Status != teamstore.OutboxStatusSent {
		t.Fatalf("final outbox should be sent: %#v", final)
	}
}

func TestBridgeFlushPromotesAcceptedOutboxWithoutPostingAgain(t *testing.T) {
	store := newBridgeTestStore(t)
	msg, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "outbox:accepted",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "already sent",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if _, err := store.MarkOutboxAccepted(context.Background(), msg.ID, "teams-message-1"); err != nil {
		t.Fatalf("MarkOutboxAccepted error: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("accepted outbox should not post again: %s %s", r.Method, r.URL.String())
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("flushPendingOutbox error: %v", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.OutboxMessages[msg.ID].Status; got != teamstore.OutboxStatusSent {
		t.Fatalf("outbox status = %q, want sent", got)
	}
}

func TestBridgeFlushPromotesAcceptedOutboxBeforeLaterQueuedMessage(t *testing.T) {
	store := newBridgeTestStore(t)
	accepted, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "outbox:accepted",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "already accepted",
	})
	if err != nil {
		t.Fatalf("QueueOutbox accepted error: %v", err)
	}
	if _, err := store.MarkOutboxAccepted(context.Background(), accepted.ID, "teams-accepted-1"); err != nil {
		t.Fatalf("MarkOutboxAccepted error: %v", err)
	}
	queued, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "outbox:queued",
		TeamsChatID: "chat-1",
		Kind:        "final",
		Body:        "queued after accepted",
	})
	if err != nil {
		t.Fatalf("QueueOutbox queued error: %v", err)
	}

	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode Graph request: %v", err)
		}
		sent = append(sent, bridgeSentMessage{ChatID: "chat-1", Content: body.Body.Content})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"teams-queued-1","messageType":"message"}`)
	}))
	defer server.Close()
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, store, &recordingExecutor{})

	if err := bridge.flushPendingOutboxForChat(context.Background(), "chat-1"); err != nil {
		t.Fatalf("flushPendingOutboxForChat error: %v", err)
	}
	if len(sent) != 1 {
		t.Fatalf("sent count = %d, want only the later queued message", len(sent))
	}
	if strings.Contains(sent[0].Content, "already accepted") || !strings.Contains(sent[0].Content, "queued after accepted") {
		t.Fatalf("sent content = %q, want queued message only", sent[0].Content)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if msg := state.OutboxMessages[accepted.ID]; msg.Status != teamstore.OutboxStatusSent || msg.TeamsMessageID != "teams-accepted-1" {
		t.Fatalf("accepted outbox not promoted: %#v", msg)
	}
	if msg := state.OutboxMessages[queued.ID]; msg.Status != teamstore.OutboxStatusSent || msg.TeamsMessageID != "teams-queued-1" {
		t.Fatalf("queued outbox not sent: %#v", msg)
	}
	if state.OutboxMessages[accepted.ID].Sequence >= state.OutboxMessages[queued.ID].Sequence {
		t.Fatalf("outbox sequence order changed: accepted=%d queued=%d", state.OutboxMessages[accepted.ID].Sequence, state.OutboxMessages[queued.ID].Sequence)
	}
}

func TestBridgeRecoverUnfinishedQueuedTurnRunsOriginalPrompt(t *testing.T) {
	graph, sent := newBridgeRetryGraph(t, bridgePollMessage("original-1", "2026-04-30T01:00:00Z", "queued prompt"))
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "recovered answer",
		CodexThreadID: "thread-1",
		CodexTurnID:   "turn-recovered",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	inbound, _, err := store.PersistInbound(context.Background(), teamstore.InboundEvent{
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "original-1",
		Status:         teamstore.InboundStatusPersisted,
	})
	if err != nil {
		t.Fatalf("PersistInbound error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{SessionID: session.ID, InboundEventID: inbound.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	if err := bridge.recoverUnfinishedTurns(context.Background()); err != nil {
		t.Fatalf("recoverUnfinishedTurns error: %v", err)
	}
	if got := executor.prompts; len(got) != 1 || !strings.HasPrefix(got[0], "queued prompt\n\n") {
		t.Fatalf("executor prompts = %#v, want recovered queued prompt", got)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "recovered answer") {
		t.Fatalf("sent recovery response = %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusCompleted {
		t.Fatalf("turn status = %q, want completed", got)
	}
}

func TestBridgeRecoverUnfinishedRunningTurnMarksInterrupted(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn:running", SessionID: session.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	if _, err := store.MarkTurnRunning(context.Background(), turn.ID, "thread-1", "codex-turn-1"); err != nil {
		t.Fatalf("MarkTurnRunning error: %v", err)
	}

	if err := bridge.recoverUnfinishedTurns(context.Background()); err != nil {
		t.Fatalf("recoverUnfinishedTurns error: %v", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "helper retry "+turn.ID) {
		t.Fatalf("interruption notification = %#v", *sent)
	}
}

type bridgeSentMessage struct {
	ChatID   string
	Content  string
	Mentions int
}

func sentPlainText(sent []bridgeSentMessage) string {
	var parts []string
	for _, msg := range sent {
		parts = append(parts, PlainTextFromTeamsHTML(msg.Content))
	}
	return strings.Join(parts, "\n")
}

func decodeTestOnlineMeetingSubject(t *testing.T, r *http.Request) string {
	t.Helper()
	var body struct {
		Subject string `json:"subject"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode onlineMeeting: %v", err)
	}
	return body.Subject
}

func writeTestOnlineMeeting(w http.ResponseWriter, chatID string, subject string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":         "meeting-" + strings.TrimPrefix(chatID, "chat-"),
		"subject":    subject,
		"joinWebUrl": "https://teams.example/meeting/" + url.PathEscape(chatID),
		"chatInfo": map[string]any{
			"threadId": chatID,
		},
	})
}

type bridgePollPage struct {
	messages []ChatMessage
	nextLink string
	assert   func(*testing.T, *http.Request)
}

func newBridgePollGraph(t *testing.T, pages []bridgePollPage) *GraphClient {
	t.Helper()
	nextPage := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		if nextPage >= len(pages) {
			t.Fatalf("unexpected extra Graph poll: %s", r.URL.String())
		}
		page := pages[nextPage]
		nextPage++
		if page.assert != nil {
			page.assert(t, r)
		}
		payload := map[string]any{"value": page.messages}
		if page.nextLink != "" {
			payload["@odata.nextLink"] = page.nextLink
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode poll response: %v", err)
		}
	}))
	t.Cleanup(func() {
		server.Close()
		if nextPage != len(pages) {
			t.Fatalf("Graph poll count = %d, want %d", nextPage, len(pages))
		}
	})
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
}

func newBridgeRetryGraph(t *testing.T, original ChatMessage) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	gotOriginal := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages/original-1":
			gotOriginal = true
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(original); err != nil {
				t.Fatalf("encode original message: %v", err)
			}
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode Graph request: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(func() {
		server.Close()
		if !gotOriginal {
			t.Fatal("retry did not fetch original Teams message")
		}
	})
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}

func newBridgeAsyncQueueGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/chats/chat-1/messages/"):
			w.Header().Set("Content-Type", "application/json")
			id := strings.TrimPrefix(r.URL.Path, "/chats/chat-1/messages/")
			prompts := map[string]string{
				"second":              "second prompt",
				"third":               "third prompt",
				"teams-during-local":  "teams prompt during local",
				"teams-after-catchup": "teams prompt after catchup",
				"teams-during-tool":   "teams prompt during tool",
			}
			prompt, ok := prompts[id]
			if !ok {
				t.Fatalf("unexpected queued message fetch: %s", r.URL.String())
			}
			if err := json.NewEncoder(w).Encode(bridgePollMessage(id, "2026-05-03T01:00:05Z", prompt)); err != nil {
				t.Fatalf("encode queued message %s: %v", id, err)
			}
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode Graph request: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
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
	}, &sent
}

func newBridgeHostedContentGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	gotHostedContent := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages/message-hosted/hostedContents/content-1/$value":
			gotHostedContent = true
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write([]byte("image-bytes"))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode Graph request: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(func() {
		server.Close()
		if !gotHostedContent {
			t.Fatal("hosted content was not downloaded")
		}
	})
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}

func newBridgeReferenceFileGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	gotFile := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/shares/") && strings.HasSuffix(r.URL.Path, "/driveItem/content"):
			gotFile = true
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("file-bytes"))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode Graph request: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(func() {
		server.Close()
		if !gotFile {
			t.Fatal("reference file attachment was not downloaded")
		}
	})
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}

func bridgePollMessage(id string, timestamp string, text string) ChatMessage {
	msg := bridgeTestMessage(id)
	msg.CreatedDateTime = timestamp
	msg.LastModifiedDateTime = timestamp
	msg.Body.ContentType = "html"
	msg.Body.Content = "<p>" + text + "</p>"
	return msg
}

func stubDiscoverCodexSession(t *testing.T, threadID string, transcriptPath string) func() {
	t.Helper()
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   threadID,
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	return func() {
		discoverCodexProjectsForTeams = prevDiscover
	}
}

func seedLinkedTranscriptForTest(t *testing.T, bridge *Bridge, transcriptPath string, threadID string) *Session {
	t.Helper()
	session := bridge.reg.SessionByChatID("chat-1")
	if session == nil {
		t.Fatal("test registry missing chat-1 session")
	}
	session.CodexThreadID = threadID
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}
	state, err := bridge.store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state after seed error: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if strings.TrimSpace(checkpoint.SourcePath) != transcriptPath || strings.TrimSpace(checkpoint.LastRecordID) == "" {
		t.Fatalf("seed checkpoint = %#v, want source path %q and a last record", checkpoint, transcriptPath)
	}
	return session
}

func queuedTurnCountForSession(state teamstore.State, sessionID string) int {
	count := 0
	for _, turn := range state.Turns {
		if turn.SessionID == sessionID && turn.Status == teamstore.TurnStatusQueued {
			count++
		}
	}
	return count
}

func sentPlainJoined(messages []bridgeSentMessage) string {
	plain := make([]string, 0, len(messages))
	for _, msg := range messages {
		plain = append(plain, PlainTextFromTeamsHTML(msg.Content))
	}
	return strings.Join(plain, "\n---\n")
}

func sentPlainContains(messages []bridgeSentMessage, needle string) bool {
	return strings.Contains(sentPlainJoined(messages), needle)
}

func countSentPlainContaining(messages []bridgeSentMessage, needle string) int {
	count := 0
	for _, msg := range messages {
		if strings.Contains(PlainTextFromTeamsHTML(msg.Content), needle) {
			count++
		}
	}
	return count
}

func newBridgeTestGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
			Mentions []json.RawMessage `json:"mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode Graph request: %v", err)
		}
		chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
		sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}

func newBridgeCreateChatGraph(t *testing.T, createdTopic *string) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			subject := decodeTestOnlineMeetingSubject(t, r)
			if createdTopic != nil {
				*createdTopic = subject
			}
			writeTestOnlineMeeting(w, "work-chat", subject)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
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
	}, &sent
}

func newBridgeTestStore(t *testing.T) *teamstore.Store {
	t.Helper()
	store, err := teamstore.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("Open store error: %v", err)
	}
	return store
}

func newBridgeTestBridge(graph *GraphClient, store *teamstore.Store, executor Executor) *Bridge {
	now := time.Now()
	return &Bridge{
		graph: graph,
		user:  User{ID: "user-1", UserPrincipalName: "user@example.test"},
		reg: Registry{
			Version:       1,
			UserID:        "user-1",
			ControlChatID: "control-chat",
			Sessions: []Session{{
				ID:        "s001",
				ChatID:    "chat-1",
				ChatURL:   "https://teams.example/chat-1",
				Topic:     "topic",
				Status:    "active",
				CreatedAt: now,
				UpdatedAt: now,
			}},
			Chats: map[string]ChatState{},
		},
		executor: executor,
		store:    store,
	}
}

func bridgeTestMessage(id string) ChatMessage {
	var msg ChatMessage
	msg.ID = id
	msg.MessageType = "message"
	msg.CreatedDateTime = "2026-04-30T00:00:00Z"
	msg.From.User = &struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
	}{ID: "user-1", DisplayName: "User"}
	return msg
}

func bridgeTestMessageWithText(id string, text string) ChatMessage {
	msg := bridgeTestMessage(id)
	msg.Body.ContentType = "html"
	msg.Body.Content = text
	return msg
}
