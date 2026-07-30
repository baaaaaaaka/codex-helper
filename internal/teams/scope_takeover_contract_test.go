package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/gofrs/flock"
)

// runtimeSafetyTakeoverContractOptions drives failure boundaries inside the
// production coordinator. Process identity itself is covered through the CLI
// validator/fencer and the ephemeral real-process Docker test.
type runtimeSafetyTakeoverContractOptions struct {
	WriterIdentity   string
	WriterPID        int
	PIDVisibility    string
	WindowsTaskState string
	FenceWriter      func(context.Context) error
	FailAfterStage   string
	OnStage          func(string)
}

type runtimeSafetyTakeoverContractResult struct {
	CanonicalPath     string
	RecoveryInventory AutomaticScopeTakeoverInventory
	PreflightCount    int
	Draining          bool
}

func exerciseRuntimeSafetyTakeoverContract(
	scope teamstore.ScopeIdentity,
	options runtimeSafetyTakeoverContractOptions,
	legacySources ...string,
) (runtimeSafetyTakeoverContractResult, error) {
	safetyCheck := func(context.Context) error {
		switch {
		case options.WriterIdentity == "mismatch":
			return fmt.Errorf("writer identity mismatch")
		case options.PIDVisibility == "unknown":
			return fmt.Errorf("writer PID visibility is unknown")
		case options.WindowsTaskState == "unknown":
			return fmt.Errorf("Windows task state is unknown")
		default:
			return nil
		}
	}
	result, err := ExecuteAutomaticScopeTakeover(
		context.Background(),
		scope,
		legacySources,
		AutomaticScopeTakeoverOptions{
			SafetyCheck: safetyCheck,
			ValidateLegacyState: func(context.Context, teamstore.State) (AutomaticScopeTakeoverFence, error) {
				if options.WriterPID <= 0 {
					return AutomaticScopeTakeoverFence{}, nil
				}
				return AutomaticScopeTakeoverFence{Writers: []AutomaticScopeTakeoverWriter{{
					PID:              options.WriterPID,
					ProcessStartTime: "contract-start-time",
					ExecutablePath:   options.WriterIdentity,
				}}}, nil
			},
			FenceWriter: func(ctx context.Context, _ AutomaticScopeTakeoverFence) error {
				if options.FenceWriter == nil {
					return nil
				}
				return options.FenceWriter(ctx)
			},
			OnStage:        options.OnStage,
			FailAfterStage: options.FailAfterStage,
		},
	)
	return runtimeSafetyTakeoverContractResult{
		CanonicalPath:     result.CanonicalPath,
		RecoveryInventory: result.RecoveryInventory,
		PreflightCount:    result.PreflightCount,
		Draining:          result.Draining,
	}, err
}

func loadRuntimeSafetyTakeoverState(t *testing.T, path string) teamstore.State {
	t.Helper()
	st, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("open store %s: %v", path, err)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		_ = st.Close()
		t.Fatalf("load store %s: %v", path, err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store %s: %v", path, err)
	}
	return state
}

func runtimeSafetyTakeoverBusinessState(state teamstore.State) any {
	return struct {
		ScopeID     string
		AccountID   string
		Profile     string
		ControlChat teamstore.ControlChatBinding
		Sessions    map[string]teamstore.SessionContext
		Turns       map[string]teamstore.Turn
	}{
		ScopeID:     state.Scope.ID,
		AccountID:   state.Scope.AccountID,
		Profile:     state.Scope.Profile,
		ControlChat: state.ControlChat,
		Sessions:    state.Sessions,
		Turns:       state.Turns,
	}
}

func requireRuntimeSafetyTakeoverDeferredWithoutMutation(
	t *testing.T,
	fixture runtimeSafetyTakeoverFixture,
	options runtimeSafetyTakeoverContractOptions,
	keywords ...string,
) {
	t.Helper()
	before := snapshotRuntimeSafetyFiles(t, fixture.Root)
	result, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, options, fixture.LegacyPath)
	if err == nil {
		t.Errorf("takeover selected %q instead of deferring", result.CanonicalPath)
	} else if !stringsContainAnyFold(err.Error(), keywords...) {
		t.Errorf("takeover error = %v, want one of %v", err, keywords)
	}
	after := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("deferred takeover modified files: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverDefersWriterIdentityMismatchWithoutMutationCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	requireRuntimeSafetyTakeoverDeferredWithoutMutation(
		t,
		fixture,
		runtimeSafetyTakeoverContractOptions{WriterIdentity: "mismatch"},
		"identity",
		"writer",
		"deferred",
	)
}

func TestTeamsRuntimeSafetyAutomaticTakeoverDefersInvisiblePIDNamespaceWithoutMutationCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	requireRuntimeSafetyTakeoverDeferredWithoutMutation(
		t,
		fixture,
		runtimeSafetyTakeoverContractOptions{PIDVisibility: "unknown"},
		"pid namespace",
		"process visibility",
		"deferred",
	)
}

func TestTeamsRuntimeSafetyAutomaticTakeoverDefersUnknownWSLTaskWithoutMutationCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	requireRuntimeSafetyTakeoverDeferredWithoutMutation(
		t,
		fixture,
		runtimeSafetyTakeoverContractOptions{WindowsTaskState: "unknown"},
		"scheduled task",
		"windows task",
		"deferred",
	)
}

func TestTeamsRuntimeSafetyAutomaticTakeoverAnyFileFamilyLockFailureIsZeroMutationCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	lock := flock.New(fixture.LegacyPath + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock legacy family: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	requireRuntimeSafetyTakeoverDeferredWithoutMutation(
		t,
		fixture,
		runtimeSafetyTakeoverContractOptions{},
		"lock",
		"busy",
		"deferred",
	)
}

func TestTeamsRuntimeSafetyAutomaticTakeoverRepreflightsWhenIdentityChangesAfterFencingCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	preflightCalls := 0
	changed := false
	result, err := exerciseRuntimeSafetyTakeoverContract(
		fixture.Scope,
		runtimeSafetyTakeoverContractOptions{
			OnStage: func(stage string) {
				if stage != "preflight-complete" || changed {
					return
				}
				preflightCalls++
				changed = true
				legacy, openErr := teamstore.Open(fixture.LegacyPath)
				if openErr != nil {
					t.Fatalf("open legacy during identity-change injection: %v", openErr)
				}
				updateErr := legacy.Update(context.Background(), func(state *teamstore.State) error {
					state.Scope.UpdatedAt = time.Now()
					return nil
				})
				closeErr := legacy.Close()
				if updateErr != nil {
					t.Fatalf("change legacy identity after preflight: %v", updateErr)
				}
				if closeErr != nil {
					t.Fatalf("close changed legacy: %v", closeErr)
				}
			},
		},
		fixture.LegacyPath,
	)
	if err != nil {
		t.Errorf("takeover did not recover from a preflight identity change: %v", err)
	}
	if result.PreflightCount < 2 || preflightCalls < 1 {
		t.Errorf("identity change was not followed by a complete re-preflight: result=%d callback=%d", result.PreflightCount, preflightCalls)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverResumesAfterActiveTurnCompletesCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacy, err := teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		state.Turns["turn-running"] = teamstore.Turn{
			ID:        "turn-running",
			SessionID: "legacy-only-session",
			Status:    teamstore.TurnStatusRunning,
			StartedAt: time.Now(),
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed active turn: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	if _, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath); err == nil {
		t.Error("takeover did not defer while the legacy turn was active")
	} else if !stringsContainAnyFold(err.Error(), "active turn", "drain", "deferred") {
		t.Errorf("active-turn result = %v, want deferred drain", err)
	}

	legacy, err = teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("reopen legacy: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		turn := state.Turns["turn-running"]
		turn.Status = teamstore.TurnStatusCompleted
		turn.CompletedAt = time.Now()
		turn.UpdatedAt = turn.CompletedAt
		state.Turns[turn.ID] = turn
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("complete active turn: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close completed legacy: %v", err)
	}

	result, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath)
	if err != nil {
		t.Fatalf("takeover did not resume after the active turn completed: %v", err)
	}
	if result.CanonicalPath != fixture.CanonicalPath {
		t.Fatalf("resumed path = %q, want canonical %q", result.CanonicalPath, fixture.CanonicalPath)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverLeavesCanonicalListenerStartToBridgeCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	result, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath)
	if err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	if !result.Draining {
		t.Error("takeover did not fence legacy work before returning to Bridge.Listen")
	}
	state := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)
	if state.ServiceOwner != nil || state.ControlLease.Status == teamstore.ControlLeaseStatusActive {
		t.Fatalf("takeover coordinator created a competing listener identity: owner=%#v lease=%#v", state.ServiceOwner, state.ControlLease)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverPreservesCanonicalBusinessSupersetCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	canonical, err := teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("open canonical: %v", err)
	}
	if err := canonical.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["canonical-only-session"] = teamstore.SessionContext{
			ID:          "canonical-only-session",
			Status:      teamstore.SessionStatusActive,
			TeamsChatID: "canonical-only-chat",
		}
		state.Turns["canonical-only-turn"] = teamstore.Turn{
			ID:        "canonical-only-turn",
			SessionID: "canonical-only-session",
			Status:    teamstore.TurnStatusCompleted,
		}
		return nil
	}); err != nil {
		_ = canonical.Close()
		t.Fatalf("seed canonical superset: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical: %v", err)
	}
	before := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)

	if _, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath); err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	after := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)
	if !reflect.DeepEqual(runtimeSafetyTakeoverBusinessState(before), runtimeSafetyTakeoverBusinessState(after)) {
		t.Fatalf("takeover changed canonical business state:\nbefore=%#v\nafter=%#v", runtimeSafetyTakeoverBusinessState(before), runtimeSafetyTakeoverBusinessState(after))
	}
	if got := after.Sessions["shared-session"].TeamsChatID; got != "canonical-chat" {
		t.Fatalf("same session ID conflict replaced canonical chat binding with %q", got)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverCorruptLegacyCannotRepairOrOverwriteCanonicalCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	canonicalBefore := snapshotRuntimeSafetyFiles(t, filepath.Dir(fixture.CanonicalPath))
	if err := os.WriteFile(fixture.LegacyPath, []byte(`{"schema_version":`), 0o600); err != nil {
		t.Fatalf("corrupt legacy pointer: %v", err)
	}

	result, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath)
	if err == nil {
		t.Errorf("corrupt legacy unexpectedly completed takeover to %q", result.CanonicalPath)
	} else if !stringsContainAnyFold(err.Error(), "legacy", "corrupt", "load", "deferred") {
		t.Errorf("corrupt legacy error = %v, want explicit legacy load/deferred classification", err)
	}
	canonicalAfter := snapshotRuntimeSafetyFiles(t, filepath.Dir(fixture.CanonicalPath))
	if !reflect.DeepEqual(canonicalBefore, canonicalAfter) {
		t.Fatalf("corrupt legacy repaired or overwrote canonical: %v", runtimeSafetySnapshotChanges(canonicalBefore, canonicalAfter))
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverReplayConflictKeepsCanonicalEvidenceCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	canonical, err := teamstore.Open(fixture.CanonicalPath)
	if err != nil {
		t.Fatalf("open canonical: %v", err)
	}
	if err := canonical.Update(context.Background(), func(state *teamstore.State) error {
		state.InboundEvents["inbound-terminal"] = teamstore.InboundEvent{
			ID:             "inbound-terminal",
			TeamsChatID:    "legacy-only-chat",
			TeamsMessageID: "teams-terminal",
			Text:           "canonical evidence wins",
			Status:         teamstore.InboundStatusIgnored,
		}
		return nil
	}); err != nil {
		_ = canonical.Close()
		t.Fatalf("seed canonical replay conflict: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical: %v", err)
	}

	if _, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath); err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	state := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)
	if got := state.InboundEvents["inbound-terminal"].Text; got != "canonical evidence wins" {
		t.Fatalf("legacy replay record overwrote canonical evidence: %q", got)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverSkippedOutboxIsInventoryOnlyCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacy, err := teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		state.OutboxMessages["legacy-skipped"] = teamstore.OutboxMessage{
			ID:          "legacy-skipped",
			SessionID:   "legacy-only-session",
			TeamsChatID: "legacy-only-chat",
			Body:        "recover only by explicit user action",
			Status:      teamstore.OutboxStatusSkipped,
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed skipped legacy outbox: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	result, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath)
	if err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	state := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)
	if _, ok := state.OutboxMessages["legacy-skipped"]; ok {
		t.Fatal("skipped legacy outbox was imported into the canonical send queue")
	}
	if result.RecoveryInventory.SkippedOutbox != 1 {
		t.Fatalf("skipped legacy outbox count = %d, want 1: %#v", result.RecoveryInventory.SkippedOutbox, result.RecoveryInventory)
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverGraphRereadDoesNotCreateDuplicateInboundCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	if _, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath); err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	registryPath, err := DefaultRegistryPathForScope(fixture.Scope.ID)
	if err != nil {
		t.Fatalf("default registry path: %v", err)
	}
	ledgerPath, ok := globalInboundLedgerPathForRegistry(registryPath)
	if !ok {
		t.Fatal("canonical inbound ledger path unavailable")
	}
	_, claimed, err := claimGlobalInbound(
		context.Background(),
		ledgerPath,
		"legacy-only-chat",
		"teams-terminal",
		"canonical-listener",
		time.Now(),
	)
	if err != nil {
		t.Fatalf("claim Graph reread: %v", err)
	}
	if claimed {
		t.Fatal("Graph reread was claimed and could start duplicate Codex work")
	}
}

func TestTeamsRuntimeSafetyAutomaticTakeoverNeverQueuesHistoricalLegacyOutboxCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	if _, err := exerciseRuntimeSafetyTakeoverContract(fixture.Scope, runtimeSafetyTakeoverContractOptions{}, fixture.LegacyPath); err != nil {
		t.Fatalf("automatic takeover: %v", err)
	}
	state := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)
	for _, id := range []string{"legacy-queued", "legacy-skipped"} {
		if msg, ok := state.OutboxMessages[id]; ok && msg.Status != teamstore.OutboxStatusSent {
			t.Fatalf("historical legacy outbox %q became sendable in canonical: %#v", id, msg)
		}
	}
	backup := loadRuntimeSafetyTakeoverState(t, fixture.BackupPath)
	if backup.OutboxMessages["legacy-queued"].Status != teamstore.OutboxStatusQueued {
		t.Fatalf("legacy recovery backup lost queued outbox: %#v", backup.OutboxMessages["legacy-queued"])
	}
}

func seedRuntimeSafetyTakeoverAuxiliarySources(t *testing.T, fixture runtimeSafetyTakeoverFixture) []string {
	t.Helper()
	scopePart := safeScopePathPart(fixture.Scope.ID)
	dataRoot := strings.TrimSpace(os.Getenv("XDG_DATA_HOME"))
	if dataRoot == "" {
		dataRoot = filepath.Join(fixture.Root, "data")
		t.Setenv("XDG_DATA_HOME", dataRoot)
	}
	sources := []string{fixture.LegacyPath}
	for _, item := range []struct {
		root string
		name string
		body string
	}{
		{root: os.Getenv("XDG_CACHE_HOME"), name: "registry.json", body: `{"control_chat_id":"legacy-control"}`},
		{root: dataRoot, name: "inbound-ledger.jsonl", body: "{\"id\":\"legacy-inbound\"}\n"},
	} {
		path := filepath.Join(item.root, "codex-helper", "teams", "scopes", scopePart, item.name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir auxiliary legacy family: %v", err)
		}
		if err := os.WriteFile(path, []byte(item.body), 0o600); err != nil {
			t.Fatalf("write auxiliary legacy family: %v", err)
		}
		sources = append(sources, path)
	}
	return sources
}

func TestTeamsRuntimeSafetyAutomaticTakeoverCrashRecoveryMatrixCI(t *testing.T) {
	stages := []string{
		"after-drain",
		"after-writer-exit",
		"before-replay-fence",
		"after-replay-fence",
		"before-rename-config",
		"after-rename-config",
		"before-rename-cache",
		"after-rename-cache",
		"before-rename-data",
		"after-rename-data",
		"before-canonical-listener-start",
	}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			fixture := seedRuntimeSafetyTakeoverFixture(t)
			sources := seedRuntimeSafetyTakeoverAuxiliarySources(t, fixture)
			canonicalBefore := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)

			_, err := exerciseRuntimeSafetyTakeoverContract(
				fixture.Scope,
				runtimeSafetyTakeoverContractOptions{FailAfterStage: stage},
				sources...,
			)
			if err == nil {
				t.Errorf("failpoint %q did not interrupt takeover", stage)
			} else if !stringsContainAnyFold(err.Error(), "injected", "crash", stage) {
				t.Errorf("failpoint %q error = %v, want injected crash classification", stage, err)
			}
			canonicalAfterCrash := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)
			if !reflect.DeepEqual(
				runtimeSafetyTakeoverBusinessState(canonicalBefore),
				runtimeSafetyTakeoverBusinessState(canonicalAfterCrash),
			) {
				t.Fatalf("failpoint %q rolled back or replaced canonical business data", stage)
			}
			for _, source := range sources {
				backup := runtimeSafetyMigrationBackupPath(source, fixture.Scope.ID)
				sourceExists, sourceErr := pathExists(source)
				backupExists, backupErr := pathExists(backup)
				if sourceErr != nil || backupErr != nil {
					t.Fatalf("inspect retry state for %s: source=%v backup=%v", source, sourceErr, backupErr)
				}
				if sourceExists && backupExists {
					t.Fatalf("failpoint %q left source and backup simultaneously present for %s", stage, source)
				}
			}

			result, retryErr := exerciseRuntimeSafetyTakeoverContract(
				fixture.Scope,
				runtimeSafetyTakeoverContractOptions{},
				sources...,
			)
			if retryErr != nil {
				t.Errorf("retry after failpoint %q was not idempotent: %v", stage, retryErr)
			} else if result.CanonicalPath != fixture.CanonicalPath {
				t.Errorf("retry after failpoint %q resolved %q, want %q", stage, result.CanonicalPath, fixture.CanonicalPath)
			}
		})
	}
}

func TestTeamsRuntimeSafetyCanonicalResolutionCostDoesNotGrowWithSessionOrOutboxCountCI(t *testing.T) {
	type operations struct {
		opens  int
		loads  int
		closes int
	}
	run := func(t *testing.T, rows int) operations {
		t.Helper()
		tmp := t.TempDir()
		isolateTeamsScopeUserDirsForTest(t, tmp)
		t.Setenv("USER", fmt.Sprintf("rows-%d", rows))
		t.Setenv(envTeamsProfile, "default")
		scope := ScopeIdentityForUser(User{ID: fmt.Sprintf("user-%d", rows), UserPrincipalName: fmt.Sprintf("u%d@example.test", rows)})
		path, err := DefaultStorePathForScope(scope.ID)
		if err != nil {
			t.Fatalf("DefaultStorePathForScope: %v", err)
		}
		openAndSeedRuntimeSafetyScopeStore(t, path, scope, "canonical-control")
		st, err := teamstore.Open(path)
		if err != nil {
			t.Fatalf("open canonical: %v", err)
		}
		if err := st.Update(context.Background(), func(state *teamstore.State) error {
			for i := 0; i < rows; i++ {
				id := fmt.Sprintf("session-%04d", i)
				state.Sessions[id] = teamstore.SessionContext{ID: id, Status: teamstore.SessionStatusActive, TeamsChatID: "chat-" + id}
				state.OutboxMessages["outbox-"+id] = teamstore.OutboxMessage{
					ID:          "outbox-" + id,
					SessionID:   id,
					TeamsChatID: "chat-" + id,
					Status:      teamstore.OutboxStatusQueued,
				}
			}
			return nil
		}); err != nil {
			_ = st.Close()
			t.Fatalf("seed %d rows: %v", rows, err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close canonical: %v", err)
		}

		prevOpen := resolveScopeStoreOpen
		prevLoad := resolveScopeStoreLoad
		prevClose := resolveScopeStoreClose
		var got operations
		resolveScopeStoreOpen = func(path string) (*teamstore.Store, error) {
			got.opens++
			return prevOpen(path)
		}
		resolveScopeStoreLoad = func(st *teamstore.Store, ctx context.Context) (teamstore.State, error) {
			got.loads++
			return prevLoad(st, ctx)
		}
		resolveScopeStoreClose = func(st *teamstore.Store) error {
			got.closes++
			return prevClose(st)
		}
		_, resolvedPath, resolveErr := ResolveStorePathForScope(scope)
		resolveScopeStoreOpen = prevOpen
		resolveScopeStoreLoad = prevLoad
		resolveScopeStoreClose = prevClose
		if resolveErr != nil {
			t.Fatalf("resolve %d-row canonical: %v", rows, resolveErr)
		}
		if resolvedPath != path {
			t.Fatalf("resolved %q, want %q", resolvedPath, path)
		}
		return got
	}

	empty := run(t, 0)
	large := run(t, 512)
	if empty != (operations{}) || large != (operations{}) {
		t.Fatalf("canonical hot path touched store rows: empty=%+v large=%+v, want zero operations", empty, large)
	}
}

func TestTeamsRuntimeSafetyCanonicalSecondStartupAddsNoWritesCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	path, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, path, scope, "canonical-control")
	if prepared, err := PrepareRuntimeStoreForListener(context.Background(), scope, AutomaticScopeTakeoverOptions{}); err != nil {
		t.Fatalf("first listener preparation: %v", err)
	} else if !prepared.Resolved || prepared.CanonicalPath != path {
		t.Fatalf("first listener preparation = %#v, want resolved canonical %q", prepared, path)
	}
	before := snapshotRuntimeSafetyFiles(t, tmp)
	for i := 0; i < 2; i++ {
		prepared, err := PrepareRuntimeStoreForListener(context.Background(), scope, AutomaticScopeTakeoverOptions{})
		if err != nil {
			t.Fatalf("listener preparation %d: %v", i+1, err)
		}
		if !prepared.Resolved || prepared.CanonicalPath != path {
			t.Fatalf("listener preparation %d = %#v, want resolved canonical %q", i+1, prepared, path)
		}
		if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
			t.Fatalf("resolution %d: %v", i+1, err)
		} else if resolvedPath != path {
			t.Fatalf("resolution %d path = %q, want %q", i+1, resolvedPath, path)
		}
	}
	after := snapshotRuntimeSafetyFiles(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("steady canonical startup wrote files: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyListenerPreparationDiscoversHistoricalLegacyScopeIDCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, canonicalPath, scope, "canonical-control")
	historicalPath, err := legacyDefaultStorePathForScope("scope-historical-id")
	if err != nil {
		t.Fatalf("historical legacy path: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, historicalPath, scope, "legacy-control")

	result, err := PrepareRuntimeStoreForListener(context.Background(), scope, AutomaticScopeTakeoverOptions{})
	if err != nil {
		t.Fatalf("prepare historical legacy takeover: %v", err)
	}
	if !result.Resolved || result.CanonicalPath != canonicalPath {
		t.Fatalf("prepare result = %#v, want canonical %q", result, canonicalPath)
	}
	if _, err := os.Stat(historicalPath); !os.IsNotExist(err) {
		t.Fatalf("historical legacy source remains visible: %v", err)
	}
	if _, err := os.Stat(migrationBackupPath(historicalPath, scope.ID)); err != nil {
		t.Fatalf("historical legacy backup unavailable: %v", err)
	}
	summary, ok, err := ReadRuntimeStoreTakeoverSummary(canonicalPath)
	if err != nil || !ok {
		t.Fatalf("read takeover summary: ok=%v err=%v", ok, err)
	}
	if summary.Status != "completed" || summary.LegacyStorePath != historicalPath {
		t.Fatalf("takeover summary = %#v", summary)
	}
}

func TestTeamsRuntimeSafetyListenerPreparationRejectsMultipleMatchingLegacyStoresReadOnlyCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, canonicalPath, scope, "canonical-control")
	for _, historicalID := range []string{"scope-historical-a", "scope-historical-b"} {
		path, err := legacyDefaultStorePathForScope(historicalID)
		if err != nil {
			t.Fatalf("historical legacy path %s: %v", historicalID, err)
		}
		openAndSeedRuntimeSafetyScopeStore(t, path, scope, "legacy-control")
	}
	before := snapshotRuntimeSafetyFiles(t, tmp)
	result, err := PrepareRuntimeStoreForListener(context.Background(), scope, AutomaticScopeTakeoverOptions{})
	if err == nil {
		t.Fatalf("multiple matching stores resolved to %#v", result)
	}
	if !stringsContainAnyFold(err.Error(), "multiple", "match", "deferred") {
		t.Fatalf("multiple-match error = %v", err)
	}
	after := snapshotRuntimeSafetyFiles(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("multiple-match preflight modified files: %v", runtimeSafetySnapshotChanges(before, after))
	}
}

func TestTeamsRuntimeSafetyTakeoverSummaryIsBoundedAndContainsNoMessageBodiesCI(t *testing.T) {
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	legacy, err := teamstore.Open(fixture.LegacyPath)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	const secret = "SECRET-LEGACY-MESSAGE-BODY-MUST-NOT-BE-COPIED"
	if err := legacy.Update(context.Background(), func(state *teamstore.State) error {
		for i := 0; i < 4096; i++ {
			id := fmt.Sprintf("queued-%04d", i)
			state.OutboxMessages[id] = teamstore.OutboxMessage{
				ID:          id,
				TeamsChatID: "legacy-only-chat",
				Status:      teamstore.OutboxStatusQueued,
				Body:        secret,
			}
		}
		return nil
	}); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed large recovery inventory: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	result, err := PrepareRuntimeStoreForListener(context.Background(), fixture.Scope, AutomaticScopeTakeoverOptions{})
	if err != nil {
		t.Fatalf("prepare takeover: %v", err)
	}
	if result.RecoveryInventory.QueuedOutbox != 4097 {
		t.Fatalf("queued recovery count = %d, want 4097", result.RecoveryInventory.QueuedOutbox)
	}
	summaryPath := runtimeStoreTakeoverSummaryPath(fixture.CanonicalPath)
	raw, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read takeover summary: %v", err)
	}
	if len(raw) > 4096 {
		t.Fatalf("takeover summary size = %d, want bounded metadata", len(raw))
	}
	if strings.Contains(string(raw), secret) || strings.Contains(string(raw), "queued-4095") {
		t.Fatal("takeover summary contains a message body or per-message recovery ID")
	}
	canonical := loadRuntimeSafetyTakeoverState(t, fixture.CanonicalPath)
	for _, message := range canonical.OutboxMessages {
		if strings.Contains(message.Body, secret) {
			t.Fatal("canonical store contains a copied legacy message body")
		}
	}
}

func TestTeamsRuntimeSafetyBridgeListenUsesCanonicalStoreAfterAutomaticTakeoverCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")

	user := User{ID: "teams-user-1", UserPrincipalName: "same@example.test"}
	scope := ScopeIdentityForUser(user)
	canonicalPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	legacyPath, err := legacyDefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("legacyDefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, canonicalPath, scope, "canonical-control")
	openAndSeedRuntimeSafetyScopeStore(t, legacyPath, scope, "legacy-control")
	machine := MachineRecordForUser(user, scope)
	controlTopic := ControlChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(machine.Label, machineLabel()),
		Profile:      scope.Profile,
	})
	canonical, err := teamstore.Open(canonicalPath)
	if err != nil {
		t.Fatalf("open canonical: %v", err)
	}
	if err := canonical.Update(context.Background(), func(state *teamstore.State) error {
		state.ControlChat.TeamsChatTopic = controlTopic
		return nil
	}); err != nil {
		_ = canonical.Close()
		t.Fatalf("seed canonical control topic: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical: %v", err)
	}
	registryPath, err := DefaultRegistryPathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultRegistryPathForScope: %v", err)
	}
	graph := newBridgePollGraph(t, []bridgePollPage{{messages: nil}})
	bridge := &Bridge{
		graph:        graph,
		readGraph:    graph,
		httpClient:   graph.httpClient(),
		registryPath: registryPath,
		reg: Registry{
			Version:          1,
			UserID:           user.ID,
			UserPrincipal:    user.UserPrincipalName,
			ControlChatID:    "canonical-control",
			ControlChatTopic: controlTopic,
			Chats:            map[string]ChatState{},
		},
		user:    user,
		scope:   scope,
		machine: machine,
	}
	if err := bridge.Listen(context.Background(), BridgeOptions{
		Once:            true,
		Interval:        time.Millisecond,
		OwnerStaleAfter: time.Minute,
		Executor:        EchoExecutor{},
		HelperVersion:   "v-takeover-bridge-test",
	}); err != nil {
		t.Fatalf("Bridge.Listen automatic takeover: %v", err)
	}
	if bridge.store == nil || !samePath(bridge.store.Path(), canonicalPath) {
		t.Fatalf("Bridge.Listen store = %#v, want canonical %q", bridge.store, canonicalPath)
	}
	if err := bridge.store.Close(); err != nil {
		t.Fatalf("close Bridge.Listen store: %v", err)
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("Bridge.Listen left legacy source in candidate directory: %v", err)
	}
	if summary, ok, err := ReadRuntimeStoreTakeoverSummary(canonicalPath); err != nil || !ok || summary.Status != "completed" {
		t.Fatalf("Bridge.Listen takeover summary: summary=%#v ok=%v err=%v", summary, ok, err)
	}
}

func TestTeamsRuntimeSafetyCanonicalResolverWorksOnReadOnlyStoreFamilyCI(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX read-only directory contract")
	}
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	path, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, path, scope, "canonical-control")
	if err := os.Remove(path + ".lock"); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove fixture lock: %v", err)
	}
	familyRoot := filepath.Dir(path)
	var paths []string
	if err := filepath.Walk(familyRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		paths = append(paths, path)
		if info.IsDir() {
			return os.Chmod(path, 0o555)
		}
		return os.Chmod(path, 0o444)
	}); err != nil {
		t.Fatalf("make canonical family read-only: %v", err)
	}
	t.Cleanup(func() {
		sort.Slice(paths, func(i, j int) bool { return len(paths[i]) > len(paths[j]) })
		for _, path := range paths {
			_ = os.Chmod(path, 0o700)
		}
	})

	if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
		t.Fatalf("read-only canonical path selection tried to lock or open the store: %v", err)
	} else if resolvedPath != path {
		t.Fatalf("read-only canonical resolved %q, want %q", resolvedPath, path)
	}
	if matches, err := filepath.Glob(filepath.Join(familyRoot, "*.lock")); err != nil {
		t.Fatalf("glob read-only lock files: %v", err)
	} else if len(matches) != 0 {
		t.Fatalf("read-only canonical resolver created lock files: %v", matches)
	}
}

func TestTeamsRuntimeSafetyCanonicalResolverSyscallProbeCI(t *testing.T) {
	if os.Getenv("CXP_TEAMS_RESOLVER_IO_PROBE") != "1" {
		t.Skip("set CXP_TEAMS_RESOLVER_IO_PROBE=1 and run under strace")
	}
	rootFile := strings.TrimSpace(os.Getenv("CXP_TEAMS_RESOLVER_IO_ROOT_FILE"))
	if rootFile == "" {
		t.Fatal("CXP_TEAMS_RESOLVER_IO_ROOT_FILE is required")
	}
	tmp := t.TempDir()
	isolateTeamsScopeUserDirsForTest(t, tmp)
	t.Setenv("USER", "alice")
	t.Setenv(envTeamsProfile, "default")
	scope := ScopeIdentityForUser(User{ID: "teams-user-1", UserPrincipalName: "same@example.test"})
	path, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	openAndSeedRuntimeSafetyScopeStore(t, path, scope, "canonical-control")
	if err := os.WriteFile(rootFile, []byte(tmp), 0o600); err != nil {
		t.Fatalf("write probe root: %v", err)
	}

	fmt.Fprintln(os.Stderr, "CXP_TEAMS_RESOLVER_IO_BEGIN")
	for i := 0; i < 2; i++ {
		if _, resolvedPath, err := ResolveStorePathForScope(scope); err != nil {
			t.Fatalf("probe resolution %d: %v", i+1, err)
		} else if resolvedPath != path {
			t.Fatalf("probe resolution %d path = %q, want %q", i+1, resolvedPath, path)
		}
	}
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_RESOLVER_IO_END")
	if prepared, err := PrepareRuntimeStoreForListener(context.Background(), scope, AutomaticScopeTakeoverOptions{}); err != nil {
		t.Fatalf("initial listener preparation: %v", err)
	} else if !prepared.Resolved || prepared.CanonicalPath != path {
		t.Fatalf("initial listener preparation = %#v, want resolved canonical %q", prepared, path)
	}
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_LISTENER_PREP_IO_BEGIN")
	for i := 0; i < 2; i++ {
		prepared, err := PrepareRuntimeStoreForListener(context.Background(), scope, AutomaticScopeTakeoverOptions{})
		if err != nil {
			t.Fatalf("probe listener preparation %d: %v", i+1, err)
		}
		if !prepared.Resolved || prepared.CanonicalPath != path {
			t.Fatalf("probe listener preparation %d = %#v, want resolved canonical %q", i+1, prepared, path)
		}
	}
	fmt.Fprintln(os.Stderr, "CXP_TEAMS_LISTENER_PREP_IO_END")
}
