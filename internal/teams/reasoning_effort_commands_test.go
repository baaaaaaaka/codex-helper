package teams

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type effortCatalogExecutor struct {
	catalog       ReasoningEffortCatalog
	defaultEffort string
}

func (e *effortCatalogExecutor) Run(context.Context, *Session, string) (ExecutionResult, error) {
	return ExecutionResult{Text: "done"}, nil
}

func (e *effortCatalogExecutor) ReasoningEffortCatalog(context.Context, *Session) (ReasoningEffortCatalog, error) {
	return e.catalog, nil
}

func (e *effortCatalogExecutor) DefaultReasoningEffort() string {
	return e.defaultEffort
}

func testEffortCatalogExecutor() *effortCatalogExecutor {
	return &effortCatalogExecutor{catalog: ReasoningEffortCatalog{
		Model:         "gpt-test",
		DisplayName:   "GPT Test",
		DefaultEffort: "medium",
		Options: []ReasoningEffortOption{
			{Effort: "low", Description: "fast"},
			{Effort: "medium", Description: "balanced"},
			{Effort: "xhigh", Description: "deep"},
		},
	}}
}

func TestReasoningEffortWorkCommandPersistsAndQueuedTurnSnapshots(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := testEffortCatalogExecutor()
	bridge := newBridgeTestBridge(nil, store, executor)
	session := bridge.reg.SessionByID("s001")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}

	message, err := bridge.handleReasoningEffortWorkCommand(ctx, session, "set xhigh")
	if err != nil {
		t.Fatalf("set work effort: %v", err)
	}
	if !strings.Contains(message, "`xhigh`") || session.ReasoningEffort != "xhigh" {
		t.Fatalf("message/session = %q / %#v", message, session)
	}
	turn, created, err := bridge.queueTurn(ctx, session, teamstore.InboundEvent{ID: "inbound-effort-1"})
	if err != nil || !created {
		t.Fatalf("queueTurn created=%v err=%v", created, err)
	}
	if turn.ReasoningEffort != "xhigh" {
		t.Fatalf("queued effort = %q, want xhigh", turn.ReasoningEffort)
	}

	if _, err := bridge.handleReasoningEffortWorkCommand(ctx, session, "low"); err != nil {
		t.Fatalf("switch work effort: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Sessions[session.ID].ReasoningEffort != "low" || state.Sessions[session.ID].ReasoningEffortSource != reasoningEffortSourceExplicit {
		t.Fatalf("durable session effort = %#v", state.Sessions[session.ID])
	}
	if state.Turns[turn.ID].ReasoningEffort != "xhigh" {
		t.Fatalf("queued turn changed after session switch: %#v", state.Turns[turn.ID])
	}
}

func TestReasoningEffortControlCommandUsesSameCommandsAndKeepsLowCompatibilityDefault(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := testEffortCatalogExecutor()
	bridge := newBridgeTestBridge(nil, store, executor)
	bridge.controlFallbackExecutor = executor
	bridge.modelProfileResolver = func(context.Context, string) (modelprofile.Snapshot, error) {
		return modelprofile.Snapshot{Name: "default", Provider: "default", Revision: 1}, nil
	}

	status, err := bridge.handleReasoningEffortControlCommand(ctx, "status")
	if err != nil {
		t.Fatalf("control status: %v", err)
	}
	if !strings.Contains(status, "`low`") {
		t.Fatalf("legacy control default status = %q", status)
	}
	message, err := bridge.handleReasoningEffortControlCommand(ctx, "set xhigh")
	if err != nil {
		t.Fatalf("control set: %v", err)
	}
	if !strings.Contains(message, "`xhigh`") {
		t.Fatalf("control set message = %q", message)
	}
	session, err := bridge.ensureControlFallbackSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if session.ReasoningEffort != "xhigh" || session.ReasoningEffortSource != reasoningEffortSourceExplicit {
		t.Fatalf("control session effort = %#v", session)
	}
	reset, err := bridge.handleReasoningEffortControlCommand(ctx, "reset")
	if err != nil {
		t.Fatalf("control reset: %v", err)
	}
	if !strings.Contains(reset, "`medium`") {
		t.Fatalf("control reset message = %q", reset)
	}
}

func TestReasoningEffortListPreservesCatalogOrder(t *testing.T) {
	session := &Session{ID: "s001", ReasoningEffort: "xhigh"}
	got := formatReasoningEffortCatalog(session, nil, testEffortCatalogExecutor().catalog)
	low := strings.Index(got, "`low`")
	medium := strings.Index(got, "`medium`")
	xhigh := strings.Index(got, "`xhigh`")
	if low < 0 || medium <= low || xhigh <= medium {
		t.Fatalf("catalog order was not preserved:\n%s", got)
	}
}

func TestReasoningEffortUsesExecutorDefaultUntilChatOverridesIt(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := testEffortCatalogExecutor()
	executor.defaultEffort = "medium"
	bridge := newBridgeTestBridge(nil, store, executor)
	session := bridge.reg.SessionByID("s001")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}

	turn, created, err := bridge.queueTurn(ctx, session, teamstore.InboundEvent{ID: "inbound-default"})
	if err != nil || !created {
		t.Fatalf("queueTurn created=%v err=%v", created, err)
	}
	if turn.ReasoningEffort != "medium" {
		t.Fatalf("queued effort = %q, want configured executor default medium", turn.ReasoningEffort)
	}
	status, err := bridge.handleReasoningEffortWorkCommand(ctx, session, "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(status, "Reasoning effort: `medium`") || !strings.Contains(status, "Source: `executor_default`") {
		t.Fatalf("status did not report executor default:\n%s", status)
	}
}

func TestReasoningEffortRejectsUnsupportedSetAndInvalidModelDefault(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := testEffortCatalogExecutor()
	executor.catalog.DefaultEffort = "future-default"
	bridge := newBridgeTestBridge(nil, store, executor)
	session := bridge.reg.SessionByID("s001")
	session.ReasoningEffort = "low"
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}

	if _, err := bridge.handleReasoningEffortWorkCommand(ctx, session, "set unsupported"); err == nil || !strings.Contains(err.Error(), "available values") {
		t.Fatalf("unsupported set error = %v", err)
	}
	if _, err := bridge.handleReasoningEffortWorkCommand(ctx, session, "reset"); err == nil || !strings.Contains(err.Error(), "future-default") {
		t.Fatalf("invalid default reset error = %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if session.ReasoningEffort != "low" || state.Sessions[session.ID].ReasoningEffort != "low" {
		t.Fatalf("invalid command mutated effort: session=%q durable=%q", session.ReasoningEffort, state.Sessions[session.ID].ReasoningEffort)
	}
}

func TestReasoningEffortAcceptsFutureModelAdvertisedValue(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := testEffortCatalogExecutor()
	executor.catalog.Options = append(executor.catalog.Options, ReasoningEffortOption{Effort: "future-ultra", Description: "future"})
	bridge := newBridgeTestBridge(nil, store, executor)
	session := bridge.reg.SessionByID("s001")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}
	if _, err := bridge.handleReasoningEffortWorkCommand(ctx, session, "set FUTURE-ULTRA"); err != nil {
		t.Fatalf("set future effort: %v", err)
	}
	if session.ReasoningEffort != "future-ultra" {
		t.Fatalf("future effort = %q, want canonical model value", session.ReasoningEffort)
	}
}

func TestRetryTurnReasoningEffortPreservesSnapshotAndUsesExecutorDefaultForLegacyTurn(t *testing.T) {
	executor := testEffortCatalogExecutor()
	executor.defaultEffort = "medium"
	session := &Session{ID: "s-retry", ReasoningEffort: "low"}
	if got := retryTurnReasoningEffort(teamstore.Turn{ReasoningEffort: "xhigh"}, session, executor); got != "xhigh" {
		t.Fatalf("snapshotted retry effort = %q, want xhigh", got)
	}
	session.ReasoningEffort = ""
	if got := retryTurnReasoningEffort(teamstore.Turn{}, session, executor); got != "medium" {
		t.Fatalf("legacy retry effort = %q, want executor default medium", got)
	}
}

func TestReasoningEffortConcurrentUpdatesKeepRegistryAndDurableStateConsistent(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := testEffortCatalogExecutor()
	bridge := newBridgeTestBridge(nil, store, executor)
	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
	session := bridge.reg.SessionByID("s001")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 33)
	for i := 0; i < 32; i++ {
		effort := "low"
		if i%2 == 1 {
			effort = "xhigh"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := bridge.setSessionReasoningEffort(ctx, session, effort, reasoningEffortSourceExplicit, executor.catalog)
			errs <- err
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 32; i++ {
			if err := bridge.Save(); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	durable := state.Sessions[session.ID]
	if session.ReasoningEffort != durable.ReasoningEffort || session.ReasoningEffortSource != durable.ReasoningEffortSource || !session.UpdatedAt.Equal(durable.UpdatedAt) {
		t.Fatalf("registry and durable effort diverged: registry=%#v durable=%#v", session, durable)
	}
}
