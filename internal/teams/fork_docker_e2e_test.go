package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type dockerForkExecutor struct {
	mu        sync.Mutex
	forkCalls int
}

func (e *dockerForkExecutor) Run(context.Context, *Session, string) (ExecutionResult, error) {
	return ExecutionResult{}, fmt.Errorf("normal execution is not part of the fork smoke test")
}

func (e *dockerForkExecutor) ForkThread(context.Context, *Session, string) (ForkResult, error) {
	e.mu.Lock()
	e.forkCalls++
	e.mu.Unlock()
	return ForkResult{CodexThreadID: "child-codex-thread"}, nil
}

func TestForkWorkSessionDeterministicGraphEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	parent := &Session{
		ID:            "docker-fork-parent",
		ChatID:        "docker-parent-chat",
		ChatURL:       "https://teams.example/parent",
		Topic:         "Docker fork parent",
		Status:        string(teamstore.SessionStatusActive),
		CodexThreadID: "parent-codex-thread",
		Cwd:           "/workspace",
	}
	if _, created, err := store.CreateSession(ctx, teamstore.SessionContext{
		ID:            parent.ID,
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   parent.ChatID,
		TeamsChatURL:  parent.ChatURL,
		TeamsTopic:    parent.Topic,
		CodexThreadID: parent.CodexThreadID,
		Cwd:           parent.Cwd,
		CreatedAt:     time.Now().Add(-time.Hour),
		UpdatedAt:     time.Now(),
	}); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	transcriptPath := filepath.Join(t.TempDir(), "parent.jsonl")
	transcript := strings.Join([]string{
		`{"type":"thread.started","thread_id":"parent-codex-thread"}`,
		`{"type":"turn.started","turn_id":"cutoff-codex-turn"}`,
		`{"type":"event_msg","payload":{"type":"agent_message","id":"history-final","turn_id":"cutoff-codex-turn","phase":"final_answer","message":"history before fork"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	cutoffAt := time.Now().Add(-time.Minute)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["docker-fork-cutoff"] = teamstore.Turn{
			ID:          "docker-fork-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "cutoff-codex-turn",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: cutoffAt,
			CreatedAt:   cutoffAt,
			UpdatedAt:   cutoffAt,
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(parent.ID)]
		checkpoint.ID = transcriptCheckpointID(parent.ID)
		checkpoint.SessionID = parent.ID
		checkpoint.SourcePath = transcriptPath
		checkpoint.UpdatedAt = time.Now()
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed cutoff and transcript checkpoint: %v", err)
	}

	now := time.Now()
	bridge := &Bridge{
		store:   store,
		user:    User{ID: "docker-user", DisplayName: "Docker User", UserPrincipalName: "docker@example.test"},
		scope:   teamstore.ScopeIdentity{ID: "docker-fork-scope"},
		machine: teamstore.MachineRecord{ID: "docker-machine", ScopeID: "docker-fork-scope"},
		lease:   teamstore.ControlLease{HolderMachineID: "docker-machine", Generation: 7, LeaseUntil: now.Add(time.Hour)},
		out:     os.Stdout,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlLease = bridge.lease
		return nil
	}); err != nil {
		t.Fatalf("seed control lease: %v", err)
	}

	var mu sync.Mutex
	var sent []struct {
		ChatID string
		Body   string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/me":
			_, _ = w.Write([]byte(`{"id":"docker-user","displayName":"Docker User","userPrincipalName":"docker@example.test"}`))
		case req.Method == http.MethodPost && req.URL.Path == "/me/onlineMeetings/createOrGet":
			_, _ = w.Write([]byte(`{"id":"docker-meeting","subject":"Docker fork parent (fork)","joinWebUrl":"https://teams.example/child","chatInfo":{"threadId":"docker-child-chat"}}`))
		case req.Method == http.MethodPost && strings.HasPrefix(req.URL.Path, "/chats/") && strings.HasSuffix(req.URL.Path, "/messages"):
			var payload struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				t.Errorf("decode message: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(req.URL.Path, "/chats/"), "/messages")
			mu.Lock()
			sent = append(sent, struct {
				ChatID string
				Body   string
			}{ChatID: chatID, Body: payload.Body.Content})
			messageID := fmt.Sprintf("docker-message-%d", len(sent))
			mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"id":%q,"messageType":"message"}`, messageID)
		default:
			t.Errorf("unexpected Graph request: %s %s", req.Method, req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	bridge.graph = &GraphClient{
		auth:       &fakeGraphAuth{token: "docker-token"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	executor := &dockerForkExecutor{}
	bridge.executor = executor

	command := ChatMessage{ID: "docker-fork-command"}
	command.Body.Content = "fork"
	if err := bridge.forkWorkSession(ctx, parent, command); err != nil {
		t.Fatalf("forkWorkSession: %v", err)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	operationID := forkOperationID(parent.ID, command.ID, command.Body.Content)
	op, ok := state.ForkOperations[operationID]
	if !ok || op.Phase != teamstore.ForkPhaseLinkSent {
		t.Fatalf("fork operation = %#v ok=%v, want link_sent", op, ok)
	}
	if _, fenced, err := store.ParentFork(ctx, parent.ID); err != nil || fenced {
		t.Fatalf("parent fence after link = %v err=%v, want released", fenced, err)
	}
	child := state.Sessions[op.ChildSessionID]
	if child.Status != teamstore.SessionStatusActive || child.TeamsChatID != "docker-child-chat" || child.CodexThreadID != "child-codex-thread" {
		t.Fatalf("child session = %#v", child)
	}
	if executor.forkCalls != 1 {
		t.Fatalf("ForkThread calls = %d, want 1", executor.forkCalls)
	}

	mu.Lock()
	gotSent := append([]struct {
		ChatID string
		Body   string
	}(nil), sent...)
	mu.Unlock()
	if len(gotSent) < 4 {
		t.Fatalf("sent messages = %#v, want progress, history, marker, and link", gotSent)
	}
	linkIndex := -1
	markerIndex := -1
	for i, item := range gotSent {
		if item.ChatID == parent.ChatID && strings.Contains(item.Body, "Fork complete") {
			linkIndex = i
		}
		if item.ChatID == op.ChildChatID && strings.Contains(item.Body, "History import complete") {
			markerIndex = i
		}
	}
	if linkIndex < 0 || markerIndex < 0 || linkIndex <= markerIndex {
		t.Fatalf("history/link ordering = marker %d link %d messages=%#v", markerIndex, linkIndex, gotSent)
	}
}

func TestForkActivatedRecoverySendsParentLinkAfterRestart(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	parent := teamstore.SessionContext{
		ID:            "docker-activated-parent",
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   "docker-activated-parent-chat",
		TeamsChatURL:  "https://teams.example/activated-parent",
		TeamsTopic:    "Activated parent",
		CodexThreadID: "docker-activated-parent-thread",
		CreatedAt:     time.Now().Add(-time.Hour),
		UpdatedAt:     time.Now(),
	}
	if _, created, err := store.CreateSession(ctx, parent); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	cutoffAt := time.Now().Add(-time.Minute)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Turns["docker-activated-cutoff"] = teamstore.Turn{
			ID:          "docker-activated-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "docker-activated-turn",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: cutoffAt,
			CreatedAt:   cutoffAt,
			UpdatedAt:   cutoffAt,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cutoff: %v", err)
	}
	op, created, err := store.BeginFork(ctx, teamstore.ForkBeginRequest{
		OperationID:       "docker-activated-operation",
		ParentSessionID:   parent.ID,
		ParentChatID:      parent.TeamsChatID,
		ParentThreadID:    parent.CodexThreadID,
		ChildSession:      teamstore.SessionContext{ID: "docker-activated-child", Status: teamstore.SessionStatusStaging, TeamsChatID: "docker-activated-child-chat", TeamsChatURL: "https://teams.example/activated-child", CodexThreadID: "docker-activated-child-thread"},
		CutoffTurnID:      "docker-activated-cutoff",
		CutoffCodexTurnID: "docker-activated-turn",
	})
	if err != nil || !created {
		t.Fatalf("BeginFork op=%#v created=%v err=%v", op, created, err)
	}
	link, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:              "fork-link:docker-activated-operation",
		SessionID:       parent.ID,
		TeamsChatID:     parent.TeamsChatID,
		Kind:            "fork-link",
		Body:            "open activated child",
		ForkOperationID: op.ID,
		ForkRole:        "link",
	})
	if err != nil {
		t.Fatalf("QueueOutbox link: %v", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, link.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt link: %v", err)
	}
	if _, err := store.MarkOutboxSent(ctx, link.ID, "docker-activated-link-message"); err != nil {
		t.Fatalf("MarkOutboxSent link: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		child := state.Sessions[op.ChildSessionID]
		child.Status = teamstore.SessionStatusActive
		child.TeamsChatID = "docker-activated-child-chat"
		child.TeamsChatURL = "https://teams.example/activated-child"
		child.CodexThreadID = "docker-activated-child-thread"
		state.Sessions[child.ID] = child
		current := state.ForkOperations[op.ID]
		current.ChildChatID = child.TeamsChatID
		current.ChildChatURL = child.TeamsChatURL
		current.ChildThreadID = child.CodexThreadID
		current.LinkOutboxID = link.ID
		current.Phase = teamstore.ForkPhaseActivated
		state.ForkOperations[op.ID] = current
		return nil
	}); err != nil {
		t.Fatalf("seed activated checkpoint: %v", err)
	}
	if _, fenced, err := store.ParentFork(ctx, parent.ID); err != nil || !fenced {
		t.Fatalf("ParentFork before recovery fenced=%v err=%v, want fenced", fenced, err)
	}

	bridge := &Bridge{store: store}
	restartedOp, ok, err := store.ForkOperation(ctx, op.ID)
	if err != nil || !ok {
		t.Fatalf("load operation after restart: op=%#v ok=%v err=%v", restartedOp, ok, err)
	}
	if err := bridge.reconcileForkOperation(ctx, restartedOp); err != nil {
		t.Fatalf("reconcileForkOperation after restart: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load final state: %v", err)
	}
	if got := state.ForkOperations[op.ID].Phase; got != teamstore.ForkPhaseLinkSent {
		t.Fatalf("recovered phase = %q, want link_sent", got)
	}
	if _, fenced, err := store.ParentFork(ctx, parent.ID); err != nil || fenced {
		t.Fatalf("ParentFork after recovery fenced=%v err=%v, want released", fenced, err)
	}
}
