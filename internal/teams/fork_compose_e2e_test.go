package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// TestForkComposeSUT is the real Bridge process used by the multi-container
// compose harness. It is skipped in ordinary package tests and only enabled
// in the isolated SUT containers by CXP_FORK_COMPOSE_SUT=1.
func TestForkComposeSUT(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CXP_FORK_COMPOSE_SUT")) != "1" {
		t.Skip("compose SUT test requires CXP_FORK_COMPOSE_SUT=1")
	}
	ctx := context.Background()
	role := firstNonEmptyString(os.Getenv("CXP_FORK_COMPOSE_ROLE"), "sut-a")
	scenario := strings.TrimSpace(os.Getenv("CXP_FORK_COMPOSE_SCENARIO"))
	stateDir := firstNonEmptyString(os.Getenv("CXP_FORK_COMPOSE_STATE_DIR"), "/state")
	storeKind := firstNonEmptyString(os.Getenv("CXP_FORK_COMPOSE_STORE_KIND"), "json")
	if role == "sut-b" {
		if err := waitComposeMarker(ctx, filepath.Join(stateDir, "run-b")); err != nil {
			t.Fatalf("wait for controller takeover command: %v", err)
		}
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create compose state directory: %v", err)
	}
	if err := waitComposeHTTP(ctx, firstNonEmptyString(os.Getenv("CXP_FORK_COMPOSE_CODEX_URL"), "http://fake-codex:8080")+"/health"); err != nil {
		t.Fatalf("wait for fake Codex: %v", err)
	}
	if err := waitComposeHTTP(ctx, firstNonEmptyString(os.Getenv("CXP_FORK_COMPOSE_GRAPH_URL"), "http://fake-graph:8081")+"/health"); err != nil {
		t.Fatalf("wait for fake Graph: %v", err)
	}

	store, err := teamstore.Open(filepath.Join(stateDir, "teams.json"))
	if err != nil {
		t.Fatalf("open durable store: %v", err)
	}
	defer func() { _ = store.Close() }()

	var parent *Session
	if role == "sut-a" {
		parent, err = seedComposeForkParent(ctx, t, store, stateDir)
		if err != nil {
			t.Fatalf("seed compose parent: %v", err)
		}
		if strings.EqualFold(storeKind, "sqlite") {
			if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
				t.Fatalf("migrate compose store to SQLite: %v", err)
			}
		}
	} else {
		state, err := store.Load(ctx)
		if err != nil {
			t.Fatalf("load durable parent after takeover: %v", err)
		}
		durable, ok := state.Sessions["compose-parent"]
		if !ok {
			t.Fatalf("durable compose parent is missing")
		}
		parentValue := registrySessionFromDurable(durable)
		parentValue.Status = string(teamstore.SessionStatusActive)
		parent = &parentValue
		if err := store.Update(ctx, func(state *teamstore.State) error {
			state.ControlLease = teamstore.ControlLease{
				HolderMachineID: "compose-machine-b",
				Generation:      2,
				LeaseUntil:      time.Now().Add(10 * time.Minute),
			}
			return nil
		}); err != nil {
			t.Fatalf("take over compose control lease: %v", err)
		}
	}

	lease := teamstore.ControlLease{
		HolderMachineID: "compose-machine-a",
		Generation:      1,
		LeaseUntil:      time.Now().Add(10 * time.Minute),
	}
	if role == "sut-b" {
		lease = teamstore.ControlLease{
			HolderMachineID: "compose-machine-b",
			Generation:      2,
			LeaseUntil:      time.Now().Add(10 * time.Minute),
		}
	}
	bridge := &Bridge{
		store:   store,
		user:    User{ID: "compose-user", DisplayName: "Compose User", UserPrincipalName: "compose@example.test"},
		scope:   teamstore.ScopeIdentity{ID: "compose-scope"},
		machine: teamstore.MachineRecord{ID: lease.HolderMachineID, ScopeID: "compose-scope"},
		lease:   lease,
		out:     os.Stdout,
	}
	bridge.graph = &GraphClient{
		auth:       &fakeGraphAuth{token: "compose-token"},
		client:     &http.Client{Timeout: 10 * time.Second},
		baseURL:    firstNonEmptyString(os.Getenv("CXP_FORK_COMPOSE_GRAPH_URL"), "http://fake-graph:8081"),
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge.executor = &composeForkExecutor{
		baseURL:  firstNonEmptyString(os.Getenv("CXP_FORK_COMPOSE_CODEX_URL"), "http://fake-codex:8080"),
		role:     role,
		scenario: scenario,
		stateDir: stateDir,
		client:   &http.Client{Timeout: 10 * time.Second},
	}

	if role == "sut-a" {
		_ = os.WriteFile(filepath.Join(stateDir, "sut-a-started"), []byte(scenario+"\n"), 0o600)
		command := ChatMessage{ID: "compose-fork-command"}
		command.Body.Content = "fork"
		err := bridge.forkWorkSession(ctx, parent, command)
		if err != nil && !composeExpectedInitialFailure(scenario) {
			_ = os.WriteFile(filepath.Join(stateDir, "sut-a.failed"), []byte(err.Error()+"\n"), 0o600)
			t.Fatalf("forkWorkSession: %v", err)
		}
		_ = os.WriteFile(filepath.Join(stateDir, "sut-a.done"), []byte(composeErrorText(err)+"\n"), 0o600)
		return
	}

	if err := bridge.reconcileForkOperations(ctx); err != nil {
		_ = os.WriteFile(filepath.Join(stateDir, "sut-b.failed"), []byte(err.Error()+"\n"), 0o600)
		t.Fatalf("reconcile fork operations after takeover: %v", err)
	}
	_ = os.WriteFile(filepath.Join(stateDir, "sut-b.done"), []byte("recovered\n"), 0o600)
}

type composeForkExecutor struct {
	baseURL  string
	role     string
	scenario string
	stateDir string
	client   *http.Client
}

func (e *composeForkExecutor) Run(context.Context, *Session, string) (ExecutionResult, error) {
	return ExecutionResult{}, fmt.Errorf("normal execution is not part of the compose fork harness")
}

func (e *composeForkExecutor) ForkThread(ctx context.Context, session *Session, cutoffCodexTurnID string) (ForkResult, error) {
	if session == nil {
		return ForkResult{}, fmt.Errorf("compose fork parent is required")
	}
	var response composeCodexThreadWire
	err := composeHTTPJSON(ctx, e.client, http.MethodPost, e.baseURL+"/thread/fork", map[string]any{
		"parentThreadId": strings.TrimSpace(session.CodexThreadID),
		"lastTurnId":     strings.TrimSpace(cutoffCodexTurnID),
	}, &response)
	if e.scenario == "owner-takeover" && e.role == "sut-a" && err == nil {
		// The fake Codex has committed the child. Exit before the Bridge can
		// persist the child ID, emulating a process crash at the external
		// side-effect boundary. The controller will start sut-b with generation 2.
		_ = os.WriteFile(filepath.Join(e.stateDir, "sut-a.crashed"), []byte("after native child creation\n"), 0o600)
		os.Exit(0)
	}
	if err != nil {
		return ForkResult{}, &codexrunner.Error{Kind: codexrunner.ErrorAmbiguous, Message: "compose fake Codex fork response was lost", Err: err}
	}
	if strings.TrimSpace(response.ID) == "" {
		return ForkResult{}, &codexrunner.Error{Kind: codexrunner.ErrorParse, Message: "compose fake Codex returned an empty child"}
	}
	return ForkResult{CodexThreadID: response.ID, CodexThreadTitle: response.Name}, nil
}

func (e *composeForkExecutor) ReconcileForkThread(ctx context.Context, session *Session, cutoffCodexTurnID string, _ time.Time, _ time.Time) (ForkReconcileResult, error) {
	var list struct {
		Threads []composeCodexThreadWire `json:"threads"`
	}
	if err := composeHTTPJSON(ctx, e.client, http.MethodGet, e.baseURL+"/thread/list", nil, &list); err != nil {
		return ForkReconcileResult{}, err
	}
	verified := make([]composeCodexThreadWire, 0, len(list.Threads))
	for _, candidate := range list.Threads {
		if strings.TrimSpace(candidate.ID) == "" || candidate.ID == session.CodexThreadID || candidate.ForkedFromID != session.CodexThreadID {
			continue
		}
		var read composeCodexThreadWire
		if err := composeHTTPJSON(ctx, e.client, http.MethodPost, e.baseURL+"/thread/read", map[string]any{"threadId": candidate.ID}, &read); err != nil {
			continue
		}
		if read.ForkedFromID == session.CodexThreadID && read.LatestTurnID == cutoffCodexTurnID {
			verified = append(verified, read)
		}
	}
	result := ForkReconcileResult{MatchCount: len(verified)}
	if len(verified) == 1 {
		result.Result = ForkResult{CodexThreadID: verified[0].ID, CodexThreadTitle: verified[0].Name}
	}
	return result, nil
}

type composeCodexThreadWire struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	ForkedFromID string    `json:"forkedFromId"`
	LatestTurnID string    `json:"latestTurnId"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

func seedComposeForkParent(ctx context.Context, t *testing.T, store *teamstore.Store, stateDir string) (*Session, error) {
	t.Helper()
	now := time.Now()
	parent := teamstore.SessionContext{
		ID:            "compose-parent",
		Status:        teamstore.SessionStatusActive,
		TeamsChatID:   "compose-parent-chat",
		TeamsChatURL:  "https://teams.example/compose-parent",
		TeamsTopic:    "Compose fork parent",
		CodexThreadID: "compose-parent-thread",
		Cwd:           "/state/workspace",
		CreatedAt:     now.Add(-time.Hour),
		UpdatedAt:     now,
	}
	if _, created, err := store.CreateSession(ctx, parent); err != nil || !created {
		return nil, fmt.Errorf("CreateSession created=%v: %w", created, err)
	}
	transcriptPath := filepath.Join(stateDir, "parent.jsonl")
	transcript := "{" + `"type":"thread.started","thread_id":"compose-parent-thread"` + "}\n" +
		"{" + `"type":"turn.started","turn_id":"compose-cutoff-turn"` + "}\n" +
		"{" + `"type":"event_msg","payload":{"type":"agent_message","id":"compose-history-final","turn_id":"compose-cutoff-turn","phase":"final_answer","message":"history before compose fork"}}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		return nil, err
	}
	cutoffAt := now.Add(-time.Minute)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ControlLease = teamstore.ControlLease{
			HolderMachineID: "compose-machine-a",
			Generation:      1,
			LeaseUntil:      now.Add(10 * time.Minute),
		}
		state.Turns["compose-cutoff"] = teamstore.Turn{
			ID:          "compose-cutoff",
			SessionID:   parent.ID,
			CodexTurnID: "compose-cutoff-turn",
			Status:      teamstore.TurnStatusCompleted,
			CompletedAt: cutoffAt,
			CreatedAt:   cutoffAt,
			UpdatedAt:   cutoffAt,
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(parent.ID)]
		checkpoint.ID = transcriptCheckpointID(parent.ID)
		checkpoint.SessionID = parent.ID
		checkpoint.SourcePath = transcriptPath
		checkpoint.UpdatedAt = now
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		return nil, err
	}
	parentSession := registrySessionFromDurable(parent)
	parentSession.Status = string(teamstore.SessionStatusActive)
	return &parentSession, nil
}

func composeHTTPJSON(ctx context.Context, client *http.Client, method string, endpoint string, requestBody any, responseBody any) error {
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = strings.NewReader(string(encoded))
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("compose fake service %s %s returned %s: %s", method, endpoint, resp.Status, strings.TrimSpace(string(data)))
	}
	if responseBody == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(responseBody)
}

func waitComposeHTTP(ctx context.Context, endpoint string) error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err == nil {
			resp, requestErr := client.Do(req)
			if requestErr == nil {
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return nil
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", endpoint)
}

func waitComposeMarker(ctx context.Context, path string) error {
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out waiting for %s", path)
}

func composeExpectedInitialFailure(scenario string) bool {
	switch scenario {
	case "native-response-lost", "graph-response-lost", "activated-restart", "owner-takeover":
		return true
	default:
		return false
	}
}

func composeErrorText(err error) string {
	if err == nil {
		return "success"
	}
	return err.Error()
}
