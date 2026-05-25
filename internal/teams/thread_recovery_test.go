package teams

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestRunQueuedTurnBindsDurableThreadWhenRegistryIsStale(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "done", CodexThreadID: "thread-real", CodexTurnID: "codex-turn-2"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-real", "")
	session.CodexThreadID = ""
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-stale-registry")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.sessions) != 1 || executor.sessions[0].CodexThreadID != "thread-real" {
		t.Fatalf("executor session thread = %#v, want durable thread-real", executor.sessions)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusCompleted {
		t.Fatalf("turn status = %q, want completed", got)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "thread-real" {
		t.Fatalf("durable session thread = %q, want thread-real", got)
	}
}

func TestRunQueuedTurnIgnoresWrongRegistryThreadWhenDurableThreadExists(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "done", CodexThreadID: "thread-real", CodexTurnID: "codex-turn-2"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-real", "")
	session.CodexThreadID = "thread-registry-wrong"
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-wrong-registry")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.sessions) != 1 || executor.sessions[0].CodexThreadID != "thread-real" {
		t.Fatalf("executor session thread = %#v, want durable thread-real", executor.sessions)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "thread-real" {
		t.Fatalf("durable session thread = %q, want thread-real", got)
	}
}

func TestRunQueuedTurnBlocksRegistryOnlyThreadAsWeakHint(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not run", CodexThreadID: "thread-registry-only"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	session.CodexThreadID = "thread-registry-only"
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-registry-only")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "" {
		t.Fatalf("durable session thread = %q, want empty", got)
	}
	notice := joinedSentPlainText(*sent)
	if !strings.Contains(notice, "legacy registry projection") || !strings.Contains(notice, "helper restore-thread thread-registry-only") {
		t.Fatalf("registry-only notice = %q, want restore-thread guidance", notice)
	}
}

func TestRunQueuedTurnBlocksMissingThreadWhenPriorCodexTurnExists(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "should not run", CodexThreadID: "thread-new"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "codex-turn-old")
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-missing-thread")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if reason := state.Turns[turn.ID].RecoveryReason; !strings.Contains(reason, codexThreadMissingKind) {
		t.Fatalf("recovery reason = %q, want codex thread missing", reason)
	}
	if got := joinedSentPlainText(*sent); !strings.Contains(got, "Codex thread is missing") || strings.Contains(got, "helper retry last") {
		t.Fatalf("missing-thread notice = %q, want restore guidance without retry-last hint", got)
	}
}

func TestRunQueuedTurnBindsThreadFromJournalFallback(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "done", CodexThreadID: "thread-journal", CodexTurnID: "codex-turn-2"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	if err := bridge.appendThreadLinkJournal(ctx, threadLinkJournalRecord{
		Source:        "test",
		SessionID:     session.ID,
		ChatID:        session.ChatID,
		CodexThreadID: "thread-journal",
	}); err != nil {
		t.Fatalf("appendThreadLinkJournal error: %v", err)
	}
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-journal")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.sessions) != 1 || executor.sessions[0].CodexThreadID != "thread-journal" {
		t.Fatalf("executor session thread = %#v, want journal thread", executor.sessions)
	}
}

func TestRunQueuedTurnIgnoresJournalRecordsForOtherScopeOrChat(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "done", CodexThreadID: "thread-new", CodexTurnID: "codex-turn-1"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	for _, rec := range []threadLinkJournalRecord{
		{Source: "other-scope", ScopeID: "scope-other", SessionID: session.ID, ChatID: session.ChatID, CodexThreadID: "thread-other-scope"},
		{Source: "other-chat", ScopeID: bridge.scope.ID, SessionID: session.ID, ChatID: "chat-other", CodexThreadID: "thread-other-chat"},
	} {
		if err := bridge.appendThreadLinkJournal(ctx, rec); err != nil {
			t.Fatalf("appendThreadLinkJournal error: %v", err)
		}
	}
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-ignore-other-journal")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "start fresh"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.sessions) != 1 || executor.sessions[0].CodexThreadID != "" {
		t.Fatalf("executor session thread = %#v, want fresh start without mismatched journal binding", executor.sessions)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "thread-new" {
		t.Fatalf("durable session thread = %q, want thread-new", got)
	}
}

func TestRunQueuedTurnBindsThreadFromPriorTurnWhenSessionIsEmpty(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "done", CodexThreadID: "thread-prior", CodexTurnID: "codex-turn-2"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["turn-prior"] = teamstore.Turn{
			ID:            "turn-prior",
			SessionID:     session.ID,
			Status:        teamstore.TurnStatusCompleted,
			CodexThreadID: "thread-prior",
			CodexTurnID:   "codex-turn-1",
			UpdatedAt:     time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed prior turn: %v", err)
	}
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-prior-bind")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.sessions) != 1 || executor.sessions[0].CodexThreadID != "thread-prior" {
		t.Fatalf("executor session thread = %#v, want prior thread", executor.sessions)
	}
}

func TestRunQueuedTurnBlocksDurableJournalThreadConflict(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not run", CodexThreadID: "thread-real"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-real", "")
	if err := bridge.appendThreadLinkJournal(ctx, threadLinkJournalRecord{
		Source:        "test-conflict",
		SessionID:     session.ID,
		ChatID:        session.ChatID,
		CodexThreadID: "thread-other",
	}); err != nil {
		t.Fatalf("appendThreadLinkJournal error: %v", err)
	}
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-durable-journal-conflict")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if reason := state.Turns[turn.ID].RecoveryReason; !strings.Contains(reason, codexThreadConflictKind) {
		t.Fatalf("recovery reason = %q, want conflict", reason)
	}
	if got := joinedSentPlainText(*sent); !strings.Contains(got, "multiple candidate threads") || strings.Contains(got, "must not run") {
		t.Fatalf("conflict notice = %q, want conflict only", got)
	}
}

func TestRunQueuedTurnConflictingObservedThreadInterruptsWithoutFinal(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not send final", CodexThreadID: "thread-other", CodexTurnID: "codex-turn-2"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-real", "")
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-conflict")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if reason := state.Turns[turn.ID].RecoveryReason; !strings.Contains(reason, codexThreadConflictKind) {
		t.Fatalf("recovery reason = %q, want conflict", reason)
	}
	if got := joinedSentPlainText(*sent); !strings.Contains(got, "Codex thread conflict") || strings.Contains(got, "must not send final") {
		t.Fatalf("sent text = %q, want conflict only", got)
	}
}

func TestRunQueuedTurnStreamThreadMismatchInterrupts(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &streamingRecordingExecutor{
		events: []codexrunner.StreamEvent{{
			Kind:     codexrunner.StreamEventTurnStarted,
			ThreadID: "thread-other",
			TurnID:   "codex-turn-2",
		}},
		result: ExecutionResult{Text: "must not send final", CodexThreadID: "thread-real", CodexTurnID: "codex-turn-2"},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "thread-real", "")
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-stream-conflict")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if reason := state.Turns[turn.ID].RecoveryReason; !strings.Contains(reason, codexThreadConflictKind) {
		t.Fatalf("recovery reason = %q, want conflict", reason)
	}
	if got := joinedSentPlainText(*sent); strings.Contains(got, "must not send final") {
		t.Fatalf("stream mismatch sent final text: %q", got)
	}
}

func TestRunQueuedTurnFreshFirstTurnCanStartNewThread(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "done", CodexThreadID: "thread-new", CodexTurnID: "codex-turn-1"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-fresh")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "start"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.prompts) != 1 {
		t.Fatalf("executor prompts = %#v, want one fresh start", executor.prompts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "thread-new" {
		t.Fatalf("durable session thread = %q, want thread-new", got)
	}
}

func TestRunQueuedTurnBlocksJournalReadErrorBeforeExecutor(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not run", CodexThreadID: "thread-new"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "codex-turn-old")
	path := bridge.threadLinkJournalPath(session.ID)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatalf("MkdirAll journal path as directory: %v", err)
	}
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-journal-read-error")

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err != nil {
		t.Fatalf("runQueuedTurnInputWithExecutor error: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns[turn.ID].Status; got != teamstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
	if got := joinedSentPlainText(*sent); !strings.Contains(got, "could not read durable state") {
		t.Fatalf("journal read error notice = %q, want fail-closed read error", got)
	}
}

func TestRunQueuedTurnStoreReadErrorDoesNotCallExecutor(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not run", CodexThreadID: "thread-new"}}
	bridge := newBridgeTestBridge(graph, store, executor)
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "codex-turn-old")
	turn := seedQueuedThreadRecoveryTurn(t, store, session.ID, "turn-store-read-error")
	if err := os.WriteFile(store.Path(), []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("corrupt store: %v", err)
	}

	if err := bridge.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, session.ChatID, ExecutionInput{Prompt: "continue"}); err == nil {
		t.Fatal("runQueuedTurnInputWithExecutor error = nil, want store read error")
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("executor prompts = %#v, want none", executor.prompts)
	}
}

func TestRestoreThreadCommandFillsEmptyBindingAndRejectsUnsafeOverwrites(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "codex-turn-old")
	seedInterruptedThreadRecoveryTurn(t, store, session.ID, "turn-interrupted")

	if err := bridge.restoreThreadCommand(ctx, session, "thread-restored"); err != nil {
		t.Fatalf("restoreThreadCommand fill error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "thread-restored" {
		t.Fatalf("session thread = %q, want thread-restored", got)
	}
	if got := joinedSentPlainText(*sent); !strings.Contains(got, "helper retry turn-interrupted") {
		t.Fatalf("restore success notice = %q, want exact retry hint", got)
	}

	if err := bridge.restoreThreadCommand(ctx, session, "thread-restored"); err != nil {
		t.Fatalf("restoreThreadCommand same thread error: %v", err)
	}
	if err := bridge.restoreThreadCommand(ctx, session, "thread-other"); err != nil {
		t.Fatalf("restoreThreadCommand overwrite refusal error: %v", err)
	}
	if err := bridge.restoreThreadCommand(ctx, session, "thread-restored --force"); err != nil {
		t.Fatalf("restoreThreadCommand force refusal error: %v", err)
	}
	got := joinedSentPlainText(*sent)
	if !strings.Contains(got, "already bound to Codex thread") || !strings.Contains(got, "thread-restored") || !strings.Contains(got, "does not support") || !strings.Contains(got, "--force") {
		t.Fatalf("restore refusal notices = %q", got)
	}
}

func TestRestoreThreadCommandRejectsThreadBoundToOtherActiveSession(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	if _, _, err := store.CreateSession(ctx, teamstore.SessionContext{
		ID:            "s002",
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   "chat-2",
		CodexThreadID: "thread-shared",
	}); err != nil {
		t.Fatalf("CreateSession other error: %v", err)
	}

	if err := bridge.restoreThreadCommand(ctx, session, "thread-shared"); err != nil {
		t.Fatalf("restoreThreadCommand error: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "" {
		t.Fatalf("session thread = %q, want empty after rejection", got)
	}
	if got := joinedSentPlainText(*sent); !strings.Contains(got, "already bound to active Teams session") || !strings.Contains(got, "s002") {
		t.Fatalf("restore active-session rejection = %q", got)
	}
}

func TestPollDropsExactTeamsStatusSelfEchoWithoutDurableMatch(t *testing.T) {
	msg := bridgePollMessage("teams-status-self-echo", "2026-05-19T12:43:42Z", "")
	msg.Body.Content = `<p><strong>🤖 ⏳ Codex status:</strong></p><p>▶️ Codex is starting this queued request.</p><p>Now running:</p><p>🤖 ⏳ Codex status: v2 已进入第三轮终审。</p>`
	graph := newBridgePollGraph(t, []bridgePollPage{{messages: []ChatMessage{msg}}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 5, 19, 12, 40, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	executor := &recordingExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(handled) != 0 || len(executor.prompts) != 0 {
		t.Fatalf("status self-echo was handled: handled=%#v prompts=%#v", handled, executor.prompts)
	}
	if !bridge.reg.HasSent("chat-1", "teams-status-self-echo") {
		t.Fatal("ignored status self-echo was not marked sent")
	}
}

func TestPollDropsFreshRenderedCodexOutputLabelsWithoutDurableMatch(t *testing.T) {
	messages := []ChatMessage{
		bridgePollMessage("fresh-codex-answer-self-echo", "2026-05-19T12:43:42Z", ""),
		bridgePollMessage("fresh-codex-command-self-echo", "2026-05-19T12:43:43Z", ""),
		bridgePollMessage("fresh-codex-progress-self-echo", "2026-05-19T12:43:44Z", ""),
	}
	messages[0].Body.Content = `<p><strong>🤖 ✅ Codex answer:</strong></p><p>final answer that must not be queued again</p>`
	messages[1].Body.Content = `<p><strong>🤖 🛠️ Codex command:</strong></p><pre><code>go test ./internal/teams</code></pre>`
	messages[2].Body.Content = `<p><strong>🤖 Codex progress:</strong></p><p>still working</p>`
	graph := newBridgePollGraph(t, []bridgePollPage{{messages: messages}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 5, 19, 12, 40, 0, 0, time.UTC), true, false, 1); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	executor := &recordingExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	var handled []string
	if _, err := bridge.pollChat(context.Background(), "chat-1", 50, func(_ context.Context, _ ChatMessage, text string) error {
		handled = append(handled, text)
		return nil
	}); err != nil {
		t.Fatalf("pollChat error: %v", err)
	}
	if len(handled) != 0 || len(executor.prompts) != 0 {
		t.Fatalf("fresh Codex output labels were handled: handled=%#v prompts=%#v", handled, executor.prompts)
	}
	for _, msg := range messages {
		if !bridge.reg.HasSent("chat-1", msg.ID) {
			t.Fatalf("ignored Codex output self-echo %s was not marked sent", msg.ID)
		}
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := len(state.InboundEvents); got != 0 {
		t.Fatalf("inbound events = %d, want none: %#v", got, state.InboundEvents)
	}
}

func TestPollDoesNotDropPlainCurrentUserStatusText(t *testing.T) {
	msg := bridgePollMessage("plain-status-question", "2026-05-19T12:43:42Z", "🤖 ⏳ Codex status: 这条为什么出现？")
	graph := newBridgePollGraph(t, []bridgePollPage{{messages: []ChatMessage{msg}}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 5, 19, 12, 40, 0, 0, time.UTC), true, false, 1); err != nil {
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
	if len(handled) != 1 || !strings.Contains(handled[0], "这条为什么出现") {
		t.Fatalf("handled = %#v, want plain current-user status text", handled)
	}
	if bridge.reg.HasSent("chat-1", "plain-status-question") {
		t.Fatal("plain current-user status text should not be marked sent")
	}
}

func TestPollDoesNotDropCoworkerStrongStatusText(t *testing.T) {
	msg := bridgePollMessage("coworker-status-question", "2026-05-19T12:43:42Z", "")
	msg.From.User.ID = "coworker-1"
	msg.From.User.DisplayName = "Coworker"
	msg.Body.Content = `<p><strong>🤖 ⏳ Codex status:</strong></p><p>what does this mean?</p>`
	graph := newBridgePollGraph(t, []bridgePollPage{{messages: []ChatMessage{msg}}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 5, 19, 12, 40, 0, 0, time.UTC), true, false, 1); err != nil {
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
	if len(handled) != 1 || !strings.Contains(handled[0], "what does this mean") {
		t.Fatalf("handled = %#v, want coworker strong status text", handled)
	}
	if bridge.reg.HasSent("chat-1", "coworker-status-question") {
		t.Fatal("coworker status text should not be marked sent by current-user self-echo guard")
	}
}

func TestPollDropsFreshRenderedCodeAndUserTranscriptSelfEcho(t *testing.T) {
	messages := []ChatMessage{
		bridgePollMessage("fresh-code-self-echo", "2026-05-19T12:43:42Z", ""),
		bridgePollMessage("fresh-user-transcript-self-echo", "2026-05-19T12:43:43Z", ""),
	}
	messages[0].Body.Content = `<p><strong>💻 Code:</strong></p><pre><code>go test ./...</code></pre>`
	messages[1].Body.Content = `<p><strong>🧑‍💻 User:</strong></p><p>previous prompt</p>`
	graph := newBridgePollGraph(t, []bridgePollPage{{messages: messages}})
	store := newBridgeTestStore(t)
	if _, err := store.RecordChatPollSuccess(context.Background(), "chat-1", time.Date(2026, 5, 19, 12, 40, 0, 0, time.UTC), true, false, 1); err != nil {
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
	if len(handled) != 0 {
		t.Fatalf("handled fresh rendered helper transcript output: %#v", handled)
	}
	for _, msg := range messages {
		if !bridge.reg.HasSent("chat-1", msg.ID) {
			t.Fatalf("ignored rendered transcript output %s was not marked sent", msg.ID)
		}
	}
}

func TestReadThreadLinkJournalSkipsCorruptPartialOversizedAndMismatchedLines(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	path := bridge.threadLinkJournalPath(session.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll journal dir: %v", err)
	}
	lines := []string{
		`{"session_id":"other","codex_thread_id":"thread-other"}`,
		`not json`,
		`{"session_id":"s001","codex_thread_id":"thread-good"}`,
		strings.Repeat("x", threadLinkJournalMaxLineByte+1),
		`{"session_id":"s001","codex_thread_id":"partial"}`,
	}
	data := strings.Join(lines[:len(lines)-1], "\n") + "\n" + lines[len(lines)-1]
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("WriteFile journal: %v", err)
	}
	records, err := bridge.readThreadLinkJournal(ctx, session.ID)
	if err != nil {
		t.Fatalf("readThreadLinkJournal error: %v", err)
	}
	if len(records) != 1 || records[0].CodexThreadID != "thread-good" {
		t.Fatalf("journal records = %#v, want only thread-good", records)
	}
}

func TestReadThreadLinkJournalBudgetExceededFailsClosed(t *testing.T) {
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	path := bridge.threadLinkJournalPath(session.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll journal dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", threadLinkJournalMaxReplayByte+2)), 0o600); err != nil {
		t.Fatalf("WriteFile oversized journal: %v", err)
	}
	if _, err := bridge.resolveCodexThreadDecision(ctx, session, teamstore.Turn{ID: "turn-budget", SessionID: session.ID}); err == nil {
		t.Fatal("resolveCodexThreadDecision should fail closed when journal replay budget is exceeded")
	}
}

func TestThreadLinkJournalConcurrentAppendAndHashedPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	seedThreadRecoverySession(t, store, session, "", "")
	path := bridge.threadLinkJournalPath(session.ID)
	if strings.Contains(path, session.ID) || strings.Contains(path, session.ChatID) {
		t.Fatalf("journal path leaks raw ids: %s", path)
	}
	if filepath.Base(path) == ".jsonl" || !strings.HasSuffix(filepath.Base(path), ".jsonl") {
		t.Fatalf("journal path = %s, want hashed jsonl filename", path)
	}

	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- bridge.appendThreadLinkJournal(ctx, threadLinkJournalRecord{
				Source:        "concurrent-test",
				SessionID:     session.ID,
				ChatID:        session.ChatID,
				TeamsTurnID:   "turn-concurrent",
				CodexThreadID: "thread-concurrent-" + strconv.Itoa(i),
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("appendThreadLinkJournal concurrent error: %v", err)
		}
	}
	records, err := bridge.readThreadLinkJournal(ctx, session.ID)
	if err != nil {
		t.Fatalf("readThreadLinkJournal error: %v", err)
	}
	if len(records) != writers {
		t.Fatalf("journal records = %d, want %d: %#v", len(records), writers, records)
	}
	seen := map[string]bool{}
	for _, rec := range records {
		if rec.SessionID != session.ID || rec.CodexThreadID == "" {
			t.Fatalf("bad journal record: %#v", rec)
		}
		if seen[rec.CodexThreadID] {
			t.Fatalf("duplicate journal thread id: %s", rec.CodexThreadID)
		}
		seen[rec.CodexThreadID] = true
	}
}

func seedThreadRecoverySession(t *testing.T, store *teamstore.Store, session *Session, threadID string, latestCodexTurnID string) {
	t.Helper()
	if session == nil {
		t.Fatal("session is nil")
	}
	if _, _, err := store.CreateSession(context.Background(), teamstore.SessionContext{
		ID:                session.ID,
		Status:            teamstore.SessionStatusActive,
		TeamsChatID:       session.ChatID,
		TeamsChatURL:      session.ChatURL,
		TeamsTopic:        session.Topic,
		CodexThreadID:     threadID,
		LatestCodexTurnID: latestCodexTurnID,
		Cwd:               session.Cwd,
	}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	session.CodexThreadID = threadID
}

func seedQueuedThreadRecoveryTurn(t *testing.T, store *teamstore.Store, sessionID string, turnID string) teamstore.Turn {
	t.Helper()
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{
		ID:        turnID,
		SessionID: sessionID,
		Status:    teamstore.TurnStatusQueued,
		QueuedAt:  time.Now(),
	})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	return turn
}

func seedInterruptedThreadRecoveryTurn(t *testing.T, store *teamstore.Store, sessionID string, turnID string) {
	t.Helper()
	turn := seedQueuedThreadRecoveryTurn(t, store, sessionID, turnID)
	if _, err := store.MarkTurnInterrupted(context.Background(), turn.ID, "codex_thread_missing: restore needed"); err != nil {
		t.Fatalf("MarkTurnInterrupted error: %v", err)
	}
}

func joinedSentPlainText(sent []bridgeSentMessage) string {
	var b strings.Builder
	for _, msg := range sent {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(PlainTextFromTeamsHTML(msg.Content))
	}
	return b.String()
}
