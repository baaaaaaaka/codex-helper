package teams

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// These vertical tests intentionally combine the two durable workloads that
// used to be tested in isolation: a queued Teams turn and a non-empty Codex
// history tail.  A listener must keep the optional history maintenance out of
// the cycle while the Teams queue is not drained.  Otherwise a large history
// reconciliation can consume the phase/SQLite budget and make the owner
// heartbeat lose its lease before the next Teams message is admitted.
func TestTeamsListenFalseBacklogSkipsOptionalHistoryMaintenanceJSON(t *testing.T) {
	runTeamsListenFalseBacklogSkipsOptionalHistoryMaintenance(t, false)
}

func TestTeamsListenFalseBacklogSkipsOptionalHistoryMaintenanceSQLite(t *testing.T) {
	runTeamsListenFalseBacklogSkipsOptionalHistoryMaintenance(t, true)
}

func TestTeamsPollForegroundPressureSkipsColdMaintenanceForOneCycle(t *testing.T) {
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	ctx := context.Background()

	bridge.setPollForegroundPressure(true)
	plan, err := bridge.optionalMaintenancePlanForOwner(ctx, time.Now())
	if err != nil {
		t.Fatalf("optionalMaintenancePlanForOwner under poll pressure: %v", err)
	}
	if plan.runNormal || plan.runMandatory || !plan.backlogActive {
		t.Fatalf("poll-pressure maintenance plan = %#v, want all cold work deferred", plan)
	}

	bridge.setPollForegroundPressure(false)
	plan, err = bridge.optionalMaintenancePlanForOwner(ctx, time.Now())
	if err != nil {
		t.Fatalf("optionalMaintenancePlanForOwner after quiet poll: %v", err)
	}
	if !plan.runNormal || plan.runMandatory || plan.backlogActive {
		t.Fatalf("quiet-poll maintenance plan = %#v, want normal cold work eligible", plan)
	}
}

func TestTeamsPollForegroundPressureEventuallyYieldsColdMaintenance(t *testing.T) {
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	ctx := context.Background()

	// Simulate a chat whose scheduler row remains due on every poll. The first
	// cycle protects the handoff from the Graph read lane, but repeated due
	// polls must not starve history/linked maintenance forever.
	bridge.beginPollForegroundPressureCycle()
	bridge.setPollForegroundPressure(true)
	plan, err := bridge.optionalMaintenancePlanForOwner(ctx, time.Now())
	if err != nil {
		t.Fatalf("first pressured optional-maintenance plan: %v", err)
	}
	if plan.runNormal || !plan.backlogActive {
		t.Fatalf("first pressured plan = %#v, want cold work deferred", plan)
	}

	bridge.beginPollForegroundPressureCycle()
	bridge.setPollForegroundPressure(true)
	plan, err = bridge.optionalMaintenancePlanForOwner(ctx, time.Now())
	if err != nil {
		t.Fatalf("repeated pressured optional-maintenance plan: %v", err)
	}
	if !plan.runNormal || plan.runMandatory || plan.backlogActive {
		t.Fatalf("repeated pressured plan = %#v, want cold work admitted", plan)
	}
}

func TestTeamsPollForegroundPressureDoesNotHideMandatoryMaintenance(t *testing.T) {
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	ctx := context.Background()

	bridge.historyWatchEventsMu.Lock()
	bridge.historyWatchPendingDirty = map[string]struct{}{"mandatory-history.jsonl": {}}
	bridge.historyWatchEventsMu.Unlock()
	bridge.setPollForegroundPressure(true)

	plan, err := bridge.optionalMaintenancePlanForOwner(ctx, time.Now())
	if err != nil {
		t.Fatalf("optionalMaintenancePlanForOwner under foreground pressure: %v", err)
	}
	if plan.runNormal || !plan.runMandatory || !plan.backlogActive {
		t.Fatalf("foreground-pressure mandatory plan = %#v, want mandatory-only maintenance", plan)
	}
}

func TestTeamsFastPollHintDoesNotKeepColdMaintenanceBlockedAfterBacklogDrains(t *testing.T) {
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	ctx := context.Background()

	bridge.boostPolling(time.Now())
	plan, err := bridge.optionalMaintenancePlanForOwner(ctx, time.Now())
	if err != nil {
		t.Fatalf("optionalMaintenancePlanForOwner after fast-poll hint: %v", err)
	}
	if !plan.runNormal || plan.runMandatory || plan.backlogActive {
		t.Fatalf("post-backlog fast-poll maintenance plan = %#v, want normal cold work eligible", plan)
	}
}

func TestTeamsBacklogOptionalQuantumLimitsColdDiscoveryToOneJob(t *testing.T) {
	bridge := &Bridge{}
	optionalPaths := []string{"optional-a", "optional-b", "optional-c"}
	if got := bridge.selectBacklogHistoryDiscoveryPaths(optionalPaths, maxBacklogOptionalMaintenanceJobs); len(got) != 1 {
		t.Fatalf("optional history quantum selected %d paths, want one: %v", len(got), got)
	}
	optionalJobs := make([]linkedTranscriptSyncJob, 0, len(optionalPaths))
	for _, id := range optionalPaths {
		optionalJobs = append(optionalJobs, linkedTranscriptSyncJob{session: Session{ID: id}})
	}
	if got := bridge.selectBacklogLinkedRecoveryJobsWithLimit(optionalJobs, maxBacklogOptionalMaintenanceJobs); len(got) != 1 {
		t.Fatalf("optional linked quantum selected %d jobs, want one: %#v", len(got), got)
	}
	if maxBacklogOptionalMaintenanceJobs != 1 {
		t.Fatalf("optional maintenance quantum = %d, want one cold job", maxBacklogOptionalMaintenanceJobs)
	}
}

func TestTeamsBacklogOptionalLinkedJobSkipsChatTitleSideEffects(t *testing.T) {
	for _, test := range []struct {
		name       string
		queueOnly  bool
		suppressed bool
		wantPatch  int
	}{
		{name: "direct maintenance keeps title update", wantPatch: 1},
		{name: "explicit optional suppression", suppressed: true},
		{name: "queue-only listener defers title update", queueOnly: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			var patchCount int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPatch || r.URL.Path != "/chats/chat-1" {
					t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				patchCount++
				w.WriteHeader(http.StatusNoContent)
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
			session := bridge.reg.Sessions[0]
			session.Topic = "💬 Codex Work - old title"
			session.CodexThreadID = "thread-title-policy"
			checkpointID := transcriptCheckpointID(session.ID)
			if _, _, err := store.UpdateImportCheckpoint(context.Background(), checkpointID, func(_ teamstore.ImportCheckpoint, _ bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
				return teamstore.ImportCheckpoint{
					ID: checkpointID, SessionID: session.ID, Status: importCheckpointStatusImporting,
					UpdatedAt: now,
				}, true, nil
			}); err != nil {
				t.Fatalf("seed importing checkpoint: %v", err)
			}
			ctx := context.Background()
			if test.suppressed {
				ctx = context.WithValue(ctx, linkedTranscriptSkipNonEssentialSideEffects{}, true)
			}
			if err := bridge.syncSessionTranscriptFromSnapshotWithOptions(
				ctx, session, codexhistory.Session{SessionID: session.CodexThreadID, Summary: "new title"},
				teamstore.State{}, teamstore.ImportCheckpoint{ID: checkpointID, SessionID: session.ID, Status: importCheckpointStatusImporting}, true, test.queueOnly,
			); err != nil {
				t.Fatalf("sync transcript with title side-effect policy: %v", err)
			}
			if patchCount != test.wantPatch {
				t.Fatalf("chat title PATCH count = %d, want %d", patchCount, test.wantPatch)
			}
		})
	}
}

func TestTeamsQueueOnlyLinkedTranscriptQueuesTailWithoutChatTitlePatch(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	oldRecord := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(oldRecord), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	var patchCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/chats/chat-1" {
			t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		patchCount++
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	bridge := newBridgeTestBridge(&GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, newBridgeTestStore(t), &recordingExecutor{})
	bridge.machine.Label = "qa-host"
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-queue-only-title"
	session.Cwd = "/home/user/project/alpha"
	session.Topic = WorkChatTitle(ChatTitleOptions{
		MachineLabel: bridge.machine.Label,
		Topic:        "old title",
		Cwd:          session.Cwd,
	})
	session.TitleSource = sessionTitleSourceAuto
	local := codexhistory.Session{
		SessionID:   session.CodexThreadID,
		ProjectPath: session.Cwd,
		FilePath:    transcriptPath,
		Summary:     "old title",
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}
	// Establish the trusted old EOF through the direct maintenance path. The
	// later queue-only call must process only the appended tail.
	if err := bridge.syncSessionTranscriptFromSnapshotWithOptions(context.Background(), *session, local, teamstore.State{}, teamstore.ImportCheckpoint{}, false, false); err != nil {
		t.Fatalf("seed transcript checkpoint: %v", err)
	}
	state, err := bridge.store.Load(context.Background())
	if err != nil {
		t.Fatalf("load seeded state: %v", err)
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint, ok := state.ImportCheckpoints[checkpointID]
	if !ok || checkpoint.LastOffset <= 0 {
		t.Fatalf("seed checkpoint = %#v, want trusted old EOF", checkpoint)
	}
	newRecord := `{"id":"new","role":"assistant","text":"new answer"}` + "\n"
	file, err := os.OpenFile(transcriptPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open transcript for append: %v", err)
	}
	if _, err := file.WriteString(newRecord); err != nil {
		_ = file.Close()
		t.Fatalf("append transcript: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close transcript: %v", err)
	}
	local.Summary = "new title"
	state, err = bridge.store.TranscriptImportStateSnapshot(context.Background())
	if err != nil {
		t.Fatalf("load transcript state before queue-only sync: %v", err)
	}
	if err := bridge.syncSessionTranscriptFromSnapshotWithOptions(context.Background(), *session, local, state, checkpoint, true, true); err != nil {
		t.Fatalf("queue-only transcript sync: %v", err)
	}
	if patchCount != 0 {
		t.Fatalf("queue-only linked sync issued %d title PATCHes, want none", patchCount)
	}
	state, err = bridge.store.Load(context.Background())
	if err != nil {
		t.Fatalf("load queue-only state: %v", err)
	}
	foundTail := false
	for _, outbox := range state.OutboxMessages {
		if outbox.SessionID == session.ID && strings.Contains(outbox.Body, "new answer") {
			foundTail = true
			break
		}
	}
	if !foundTail {
		t.Fatalf("queue-only linked sync did not durably queue the appended tail: %#v", state.OutboxMessages)
	}
}

func TestTeamsBacklogOptionalLinkedJobPropagatesSideEffectPolicy(t *testing.T) {
	bridge := &Bridge{transcriptSyncWorkerCount: 1}
	var observed bool
	bridge.linkedTranscriptSessionHook = func(ctx context.Context, _ Session) error {
		observed = linkedTranscriptNonEssentialSideEffectsSuppressed(ctx)
		return context.Canceled
	}
	err := bridge.runLinkedTranscriptSyncJobs(context.Background(), []linkedTranscriptSyncJob{
		{session: Session{ID: "optional-policy"}, skipNonEssentialSideEffects: true},
	}, func(context.Context, Session, teamstore.ImportCheckpoint) (teamstore.State, error) {
		t.Fatal("optional linked job loaded durable state after its preflight hook failed")
		return teamstore.State{}, nil
	})
	if err == nil {
		t.Fatal("optional linked job swallowed its local cancellation")
	}
	if !observed {
		t.Fatal("optional linked job did not propagate the side-effect suppression policy")
	}
}

func TestTeamsLinkedTranscriptPhaseSeparatesBoundedOptionalDeferralFromFailure(t *testing.T) {
	deferred := &linkedTranscriptJobDeferredError{SessionID: "optional-deferred", Err: context.Canceled}
	bridge := &Bridge{}
	if err := bridge.runMainLoopPhase(context.Background(), "linked-transcript", func(context.Context) error {
		return deferred
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("linked phase returned %v, want the original child cancellation", err)
	}
	stats := bridge.mainLoopPhaseStatsSnapshot("linked-transcript")
	if stats.Errors != 0 || stats.Deferred != 1 {
		t.Fatalf("bounded linked deferral stats = %#v, want errors=0 deferred=1", stats)
	}

	bridge = &Bridge{}
	mixed := errors.Join(deferred, errors.New("real linked transcript failure"))
	if err := bridge.runMainLoopPhase(context.Background(), "linked-transcript", func(context.Context) error {
		return mixed
	}); err == nil {
		t.Fatal("mixed linked phase failure was swallowed")
	}
	stats = bridge.mainLoopPhaseStatsSnapshot("linked-transcript")
	if stats.Errors != 1 || stats.Deferred != 0 {
		t.Fatalf("mixed linked phase stats = %#v, want errors=1 deferred=0", stats)
	}
}

func TestTeamsLinkedTranscriptBudgetDeferralRequiresLiveParent(t *testing.T) {
	parent := context.Background()
	jobCtx, cancelJob := context.WithDeadline(parent, time.Now().Add(-time.Second))
	defer cancelJob()
	if !linkedTranscriptJobBudgetDeferral(context.Canceled, jobCtx, parent) {
		t.Fatal("child timeout surfaced as context.Canceled was not classified as a bounded deferral")
	}

	canceledParent, cancelParent := context.WithCancel(context.Background())
	jobCtx, cancelJob = context.WithDeadline(canceledParent, time.Now().Add(-time.Second))
	defer cancelJob()
	cancelParent()
	if linkedTranscriptJobBudgetDeferral(context.Canceled, jobCtx, canceledParent) {
		t.Fatal("owner/phase cancellation was incorrectly classified as an optional child deferral")
	}

	if !linkedTranscriptJobBudgetDeferral(teamstore.ErrSQLiteSessionTranscriptDedupeSnapshotChanged, context.Background(), parent) {
		t.Fatal("concurrent SQLite transcript snapshot change was not classified as a retryable deferral")
	}
	if linkedTranscriptJobBudgetDeferral(teamstore.ErrSQLiteSessionTranscriptDedupeSnapshotChanged, context.Background(), canceledParent) {
		t.Fatal("SQLite transcript snapshot change was classified as deferred after parent cancellation")
	}
}

func TestTeamsLinkedTranscriptSQLiteSnapshotChangeIsSessionDeferred(t *testing.T) {
	bridge := &Bridge{transcriptSyncWorkerCount: 1}
	err := bridge.runLinkedTranscriptSyncJobs(context.Background(), []linkedTranscriptSyncJob{
		{session: Session{ID: "sqlite-snapshot-race"}},
	}, func(context.Context, Session, teamstore.ImportCheckpoint) (teamstore.State, error) {
		return teamstore.State{}, teamstore.ErrSQLiteSessionTranscriptDedupeSnapshotChanged
	})
	if err == nil || !isLinkedTranscriptJobDeferred(err) {
		t.Fatalf("SQLite snapshot race error = %v, want linked-transcript deferral", err)
	}
}

func TestTeamsLinkedTranscriptBudgetTimeoutIsDeferredForMandatoryJob(t *testing.T) {
	bridge := &Bridge{transcriptSyncWorkerCount: 1}
	bridge.linkedTranscriptSessionHook = func(ctx context.Context, _ Session) error {
		<-ctx.Done()
		return ctx.Err()
	}
	parent, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := bridge.runLinkedTranscriptSyncJobs(parent, []linkedTranscriptSyncJob{
		{session: Session{ID: "mandatory-timeout"}, mandatory: true},
	}, func(context.Context, Session, teamstore.ImportCheckpoint) (teamstore.State, error) {
		t.Fatal("mandatory timed-out linked job loaded durable state after its hook timed out")
		return teamstore.State{}, nil
	})
	if err == nil || !isLinkedTranscriptJobDeferred(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mandatory child timeout = %v, want typed bounded deferral", err)
	}
}

func TestTeamsNormalMaintenanceRechecksBacklogAfterInitialQuietProbe(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate store to SQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})

			plan, err := bridge.optionalMaintenancePlanForOwner(ctx, time.Now())
			if err != nil {
				t.Fatalf("initial optional-maintenance plan: %v", err)
			}
			if !plan.runNormal || plan.backlogActive {
				t.Fatalf("initial quiet plan = %#v, want normal maintenance eligible", plan)
			}

			now := time.Now().UTC()
			if err := store.Update(ctx, func(state *teamstore.State) error {
				if state.InboundEvents == nil {
					state.InboundEvents = make(map[string]teamstore.InboundEvent)
				}
				if state.Turns == nil {
					state.Turns = make(map[string]teamstore.Turn)
				}
				state.InboundEvents["inbound-maintenance-race"] = teamstore.InboundEvent{
					ID:             "inbound-maintenance-race",
					SessionID:      "session-maintenance-race",
					TeamsChatID:    "chat-maintenance-race",
					TeamsMessageID: "message-maintenance-race",
					Status:         teamstore.InboundStatusPersisted,
					TurnID:         "turn-maintenance-race",
					CreatedAt:      now,
					UpdatedAt:      now,
				}
				state.Turns["turn-maintenance-race"] = teamstore.Turn{
					ID:             "turn-maintenance-race",
					SessionID:      "session-maintenance-race",
					InboundEventID: "inbound-maintenance-race",
					Status:         teamstore.TurnStatusQueued,
					CreatedAt:      now,
					UpdatedAt:      now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed backlog after initial quiet probe: %v", err)
			}

			allowed, err := bridge.normalOptionalMaintenanceStillAllowed(ctx)
			if err != nil {
				t.Fatalf("recheck normal maintenance admission: %v", err)
			}
			if allowed {
				t.Fatalf("normal maintenance remained allowed after a durable queued turn was added")
			}
		})
	}
}

func TestTeamsBacklogFairQuantumDelaysInitialRunAndReservesOptionalSlots(t *testing.T) {
	bridge := &Bridge{}
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	if bridge.backlogOptionalMaintenanceDue(now) {
		t.Fatal("first backlog probe consumed the optional fairness quantum immediately")
	}
	if bridge.backlogOptionalMaintenanceDue(now.Add(optionalMaintenanceBacklogFairInterval - time.Nanosecond)) {
		t.Fatal("optional fairness quantum became due before its interval")
	}
	if !bridge.backlogOptionalMaintenanceDue(now.Add(optionalMaintenanceBacklogFairInterval)) {
		t.Fatal("optional fairness quantum did not become due after its interval")
	}
	if !bridge.backlogOptionalMaintenanceDue(now.Add(2 * optionalMaintenanceBacklogFairInterval)) {
		t.Fatal("failed fairness quantum was silently consumed without completion")
	}
	bridge.backlogOptionalMaintenanceFailed(now.Add(2 * optionalMaintenanceBacklogFairInterval))
	if bridge.backlogOptionalMaintenanceDue(now.Add(2*optionalMaintenanceBacklogFairInterval + optionalMaintenanceBacklogFairRetryInterval - time.Nanosecond)) {
		t.Fatal("failed fairness quantum became due during its retry backoff")
	}
	if !bridge.backlogOptionalMaintenanceDue(now.Add(2*optionalMaintenanceBacklogFairInterval + optionalMaintenanceBacklogFairRetryInterval)) {
		t.Fatal("failed fairness quantum did not become due after its retry backoff")
	}
	bridge.backlogOptionalMaintenanceCompleted(now.Add(2 * optionalMaintenanceBacklogFairInterval))
	if bridge.backlogOptionalMaintenanceDue(now.Add(2*optionalMaintenanceBacklogFairInterval + time.Second)) {
		t.Fatal("completed fairness quantum was not delayed")
	}

	mandatoryPaths := []string{"mandatory-a", "mandatory-b", "mandatory-c", "mandatory-d"}
	optionalPaths := []string{"optional-a", "optional-b"}
	if got := bridge.selectBacklogHistoryRecoveryPathsWithLimit(mandatoryPaths, maxBacklogHistoryRecoveryJobs-1); len(got) != maxBacklogHistoryRecoveryJobs-1 {
		t.Fatalf("mandatory history fairness batch = %v, want %d rows", got, maxBacklogHistoryRecoveryJobs-1)
	}
	if got := bridge.selectBacklogHistoryDiscoveryPaths(optionalPaths, 1); len(got) != 1 {
		t.Fatalf("optional history fairness batch = %v, want one reserved row", got)
	}
	mandatoryJobs := make([]linkedTranscriptSyncJob, 0, maxBacklogLinkedRecoveryJobs)
	for _, id := range mandatoryPaths {
		mandatoryJobs = append(mandatoryJobs, linkedTranscriptSyncJob{session: Session{ID: id}, mandatory: true})
	}
	optionalJobs := []linkedTranscriptSyncJob{{session: Session{ID: "optional-linked"}}}
	if got := append(
		bridge.selectBacklogLinkedRecoveryJobsWithLimit(mandatoryJobs, maxBacklogLinkedRecoveryJobs-1),
		bridge.selectBacklogLinkedRecoveryJobsWithLimit(optionalJobs, 1)...,
	); len(got) != maxBacklogLinkedRecoveryJobs {
		t.Fatalf("linked history fairness batch = %d, want %d total jobs", len(got), maxBacklogLinkedRecoveryJobs)
	}
}

func TestBacklogFairCursorDoesNotPersistAfterPhaseCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if backlogFairCursorPersistAllowed(ctx, nil) {
		t.Fatal("canceled backlog phase was allowed to persist a fairness cursor")
	}
	if !backlogFairCursorPersistAllowed(context.Background(), nil) {
		t.Fatal("live backlog phase was rejected from fairness cursor persistence")
	}
}

func TestTeamsLinkedRecoveryCombinedQuantumReachesBothLanes(t *testing.T) {
	bridge := &Bridge{}
	jobs := make([]linkedTranscriptSyncJob, 0, 20)
	for i := 0; i < 10; i++ {
		jobs = append(jobs, linkedTranscriptSyncJob{
			session:   Session{ID: fmt.Sprintf("mandatory-linked-%02d", i)},
			mandatory: true,
		})
	}
	for i := 0; i < 10; i++ {
		jobs = append(jobs, linkedTranscriptSyncJob{
			session: Session{ID: fmt.Sprintf("optional-linked-%02d", i)},
		})
	}
	seenMandatory := make(map[string]bool)
	seenOptional := make(map[string]bool)
	for quantum := 0; quantum < 8; quantum++ {
		selected := bridge.selectBacklogLinkedRecoveryJobsWithOptionalReserve(jobs, 4)
		if len(selected) != 4 {
			t.Fatalf("combined linked quantum %d selected %d jobs, want 4", quantum, len(selected))
		}
		optional := 0
		for _, job := range selected {
			if job.mandatory {
				seenMandatory[linkedTranscriptFairJobKey(job)] = true
			} else {
				optional++
				seenOptional[linkedTranscriptFairJobKey(job)] = true
			}
		}
		if optional == 0 {
			t.Fatalf("combined linked quantum %d starved optional lane: %#v", quantum, selected)
		}
	}
	if len(seenMandatory) != 10 || len(seenOptional) != 10 {
		t.Fatalf("combined linked quanta did not reach both tails: mandatory=%v optional=%v", seenMandatory, seenOptional)
	}
}

func TestTeamsLinkedRecoveryCombinedQuantumSurvivesRestart(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ControlLease = teamstore.ControlLease{
					HolderMachineID: "combined-owner", Generation: 12, Status: teamstore.ControlLeaseStatusActive,
					LeaseUntil: time.Now().Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed combined owner: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate combined fairness store: %v", err)
				}
			}
			ownerCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{MachineID: "combined-owner", LeaseGeneration: 12})
			jobs := make([]linkedTranscriptSyncJob, 0, 20)
			keys := make([]string, 0, 20)
			for i := 0; i < 10; i++ {
				job := linkedTranscriptSyncJob{session: Session{ID: fmt.Sprintf("restart-mandatory-%02d", i)}, mandatory: true}
				jobs = append(jobs, job)
				keys = append(keys, linkedTranscriptFairJobKey(job))
			}
			for i := 0; i < 10; i++ {
				job := linkedTranscriptSyncJob{session: Session{ID: fmt.Sprintf("restart-optional-%02d", i)}}
				jobs = append(jobs, job)
				keys = append(keys, linkedTranscriptFairJobKey(job))
			}
			seen := make(map[string]bool)
			for quantum := 0; quantum < 12; quantum++ {
				// Recreate the bridge every quantum. The only state allowed to
				// survive is the owner-fenced durable scheduling hint.
				bridge := &Bridge{store: store}
				if err := bridge.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinked, keys); err != nil {
					t.Fatalf("restore combined fairness quantum %d: %v", quantum, err)
				}
				selected := bridge.selectBacklogLinkedRecoveryJobsWithOptionalReserve(jobs, 4)
				if len(selected) != 4 {
					t.Fatalf("restart combined fairness quantum %d selected %d jobs, want 4", quantum, len(selected))
				}
				optional := 0
				for _, job := range selected {
					seen[linkedTranscriptFairJobKey(job)] = true
					if !job.mandatory {
						optional++
					}
				}
				if optional == 0 {
					t.Fatalf("restart combined fairness quantum %d starved optional lane", quantum)
				}
				if err := bridge.persistBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinked); err != nil {
					t.Fatalf("persist combined fairness quantum %d: %v", quantum, err)
				}
			}
			if len(seen) != len(jobs) {
				t.Fatalf("restart combined fairness did not reach every job: seen=%v", seen)
			}
		})
	}
}

func TestTeamsBacklogHistoryAndLinkedDiscoveryCursorsAreIndependent(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ControlLease = teamstore.ControlLease{
					HolderMachineID: "discovery-owner", Generation: 11, Status: teamstore.ControlLeaseStatusActive,
					LeaseUntil: time.Now().Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed discovery cursor owner: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate discovery cursor store: %v", err)
				}
			}
			ownerCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{MachineID: "discovery-owner", LeaseGeneration: 11})
			bridge := &Bridge{store: store}
			root := t.TempDir()
			history := []string{filepath.Join(root, "history-a"), filepath.Join(root, "history-b"), filepath.Join(root, "history-c"), filepath.Join(root, "history-d"), filepath.Join(root, "history-e")}
			linked := []string{filepath.Join(root, "linked-a"), filepath.Join(root, "linked-b"), filepath.Join(root, "linked-c"), filepath.Join(root, "linked-d"), filepath.Join(root, "linked-e")}
			if err := bridge.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneHistoryDiscovery, history); err != nil {
				t.Fatalf("restore history discovery cursor: %v", err)
			}
			historySelected := bridge.selectBacklogHistoryDiscoveryPaths(history, 2)
			if err := bridge.persistBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneHistoryDiscovery); err != nil {
				t.Fatalf("persist history discovery cursor: %v", err)
			}
			if err := bridge.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinkedDiscovery, linked); err != nil {
				t.Fatalf("restore linked discovery cursor: %v", err)
			}
			linkedSelected := bridge.selectBacklogLinkedDiscoveryPaths(linked, 2)
			if err := bridge.persistBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinkedDiscovery); err != nil {
				t.Fatalf("persist linked discovery cursor: %v", err)
			}
			control, err := store.ReadControl(ctx)
			if err != nil {
				t.Fatalf("read discovery cursors: %v", err)
			}
			if control.BacklogHistoryDiscoveryFairCursor != historySelected[len(historySelected)-1] {
				t.Fatalf("history discovery cursor = %q, want %q", control.BacklogHistoryDiscoveryFairCursor, historySelected[len(historySelected)-1])
			}
			if control.BacklogLinkedDiscoveryFairCursor != linkedSelected[len(linkedSelected)-1] {
				t.Fatalf("linked discovery cursor = %q, want %q", control.BacklogLinkedDiscoveryFairCursor, linkedSelected[len(linkedSelected)-1])
			}
			if control.BacklogHistoryDiscoveryFairCursor == control.BacklogLinkedDiscoveryFairCursor {
				t.Fatal("history and linked discovery cursors unexpectedly share durable state")
			}
			restarted := &Bridge{store: store}
			if err := restarted.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneHistoryDiscovery, history); err != nil {
				t.Fatalf("restore history discovery cursor after restart: %v", err)
			}
			if got := restarted.selectBacklogHistoryDiscoveryPaths(history, 2); !reflect.DeepEqual(got, history[2:4]) {
				t.Fatalf("history discovery after restart = %v, want %v", got, history[2:4])
			}
			if err := restarted.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinkedDiscovery, linked); err != nil {
				t.Fatalf("restore linked discovery cursor after restart: %v", err)
			}
			if got := restarted.selectBacklogLinkedDiscoveryPaths(linked, 2); !reflect.DeepEqual(got, linked[2:4]) {
				t.Fatalf("linked discovery after restart = %v, want %v", got, linked[2:4])
			}
		})
	}
}

func TestTeamsLinkedDiscoveryQuantumAdvancesPastFailingPrefix(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ControlLease = teamstore.ControlLease{
					HolderMachineID: "linked-discovery-owner", Generation: 13,
					Status: teamstore.ControlLeaseStatusActive, LeaseUntil: time.Now().Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed linked discovery owner: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate linked discovery store: %v", err)
				}
			}
			ownerCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{MachineID: "linked-discovery-owner", LeaseGeneration: 13})
			root := t.TempDir()
			paths := make([]string, 16)
			for i := range paths {
				paths[i] = filepath.Join(root, fmt.Sprintf("session-%02d.jsonl", i))
			}
			seen := make(map[string]bool, len(paths))
			for quantum := 0; quantum < 4; quantum++ {
				// Recreate the bridge just as a listener restart would. The first
				// four files are a deliberately failing prefix; even when every
				// selected job fails, the discovery cursor must move to the next
				// bounded window so the tail is eventually examined.
				bridge := &Bridge{store: store}
				if err := bridge.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinkedDiscovery, paths); err != nil {
					t.Fatalf("restore linked discovery quantum %d: %v", quantum, err)
				}
				selected := bridge.selectBacklogLinkedDiscoveryPaths(paths, maxBacklogLinkedRecoveryJobs)
				want := paths[quantum*maxBacklogLinkedRecoveryJobs : (quantum+1)*maxBacklogLinkedRecoveryJobs]
				if !reflect.DeepEqual(selected, want) {
					t.Fatalf("linked discovery quantum %d = %v, want %v", quantum, selected, want)
				}
				for _, path := range selected {
					seen[path] = true
				}
				if err := bridge.persistBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinkedDiscovery); err != nil {
					t.Fatalf("persist linked discovery quantum %d: %v", quantum, err)
				}
			}
			if len(seen) != len(paths) {
				t.Fatalf("linked discovery cursor did not reach the tail after failing prefix: seen=%v", seen)
			}
		})
	}
}

func TestTeamsBacklogFairQuantumDiscoversRecentLinkedSessionWithoutCheckpoint(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			runTeamsBacklogFairQuantumDiscoversRecentLinkedSessionWithoutCheckpoint(t, useSQLite)
		})
	}
}

func runTeamsBacklogFairQuantumDiscoversRecentLinkedSessionWithoutCheckpoint(t *testing.T, useSQLite bool) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	root := newBridgeTestCodexRoot(t)
	now := time.Now().UTC()
	path := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "fair-linked.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create fair linked history directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"type":"session_meta","payload":{"id":"thread-fair-linked","history_mode":"paginated"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("write fair linked history file: %v", err)
	}
	session := appendBridgeTestSession(t, bridge, store, "s-fair-linked", "chat-fair-linked")
	session.CodexThreadID = "thread-fair-linked"
	if err := store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.Sessions[session.ID]
		current.CodexThreadID = session.CodexThreadID
		state.Sessions[session.ID] = current
		return nil
	}); err != nil {
		t.Fatalf("persist fair linked thread: %v", err)
	}
	bridge.regMu.Lock()
	for i := range bridge.reg.Sessions {
		if bridge.reg.Sessions[i].ID == session.ID {
			bridge.reg.Sessions[i].CodexThreadID = session.CodexThreadID
		}
	}
	bridge.regMu.Unlock()
	if useSQLite {
		if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
			t.Fatalf("migrate fair linked discovery store: %v", err)
		}
	}
	bridge.scope.CodexHome = root
	var discovered string
	bridge.linkedTranscriptSessionHook = func(_ context.Context, got Session) error {
		discovered = got.ID
		return context.Canceled
	}
	err := bridge.syncLinkedTranscriptsWithDiscoveryOptionsAndBacklog(ctx, false, now, true, true, true)
	if discovered != session.ID {
		t.Fatalf("fair linked discovery selected %q, want %q; err=%v", discovered, session.ID, err)
	}
	if err == nil {
		t.Fatal("fair linked discovery hook cancellation was swallowed")
	}
}

func TestTeamsLinkedBacklogFairCursorPersistsAfterSuccessfulQuanta(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ControlLease = teamstore.ControlLease{
					HolderMachineID: "fair-owner", Generation: 9, Status: teamstore.ControlLeaseStatusActive,
					LeaseUntil: time.Now().Add(time.Hour),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed fairness owner: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate fairness cursor store: %v", err)
				}
			}
			ownerCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{MachineID: "fair-owner", LeaseGeneration: 9})
			bridge := &Bridge{store: store}
			jobs := make([]linkedTranscriptSyncJob, 0, maxBacklogLinkedRecoveryJobs+1)
			keys := make([]string, 0, maxBacklogLinkedRecoveryJobs+1)
			for i := 0; i < maxBacklogLinkedRecoveryJobs+1; i++ {
				job := linkedTranscriptSyncJob{session: Session{ID: fmt.Sprintf("linked-fair-%d", i)}}
				jobs = append(jobs, job)
				keys = append(keys, linkedTranscriptFairJobKey(job))
			}
			seen := make(map[string]bool)
			for quantum := 0; quantum < 3; quantum++ {
				if err := bridge.restoreBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinked, keys); err != nil {
					t.Fatalf("restore linked fairness cursor quantum %d: %v", quantum, err)
				}
				selected := bridge.selectBacklogLinkedRecoveryJobsWithLimit(jobs, 2)
				if len(selected) != 2 {
					t.Fatalf("linked fairness quantum %d selected %d jobs, want 2", quantum, len(selected))
				}
				for _, job := range selected {
					seen[linkedTranscriptFairJobKey(job)] = true
				}
				if err := bridge.persistBacklogFairCursor(ownerCtx, teamstore.BacklogFairLaneLinked); err != nil {
					t.Fatalf("persist linked fairness cursor quantum %d: %v", quantum, err)
				}
				control, err := store.ReadControl(ctx)
				if err != nil {
					t.Fatalf("read linked fairness cursor quantum %d: %v", quantum, err)
				}
				want := linkedTranscriptFairJobKey(selected[len(selected)-1])
				if control.BacklogLinkedFairCursor != want {
					t.Fatalf("durable linked fairness cursor quantum %d = %q, want %q", quantum, control.BacklogLinkedFairCursor, want)
				}
			}
			if !seen["linked-fair-4"] {
				t.Fatalf("successful linked fairness quanta never reached tail job: seen=%v", seen)
			}
		})
	}
}

func runTeamsListenFalseBacklogSkipsOptionalHistoryMaintenance(t *testing.T, useSQLite bool) {
	t.Helper()
	ctx := context.Background()
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(*teamstore.State) error { return nil }); err != nil {
		t.Fatalf("materialize listener backlog fixture: %v", err)
	}
	if useSQLite {
		if migration, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
			t.Fatalf("migrate listener backlog fixture to SQLite: %v", err)
		} else if !migration.Migrated && !migration.AlreadyDB {
			t.Fatalf("unexpected listener backlog migration result: %#v", migration)
		}
	}

	graphMessages := map[string][]ChatMessage{
		"chat-backlog-history": {
			backlogHistoryTestMessage("teams-backlog-1", "first durable backlog prompt"),
			backlogHistoryTestMessage("teams-backlog-2", "second durable backlog prompt"),
		},
	}
	graph, _ := newListenerRecoveryGraph(t, nil, graphMessages, 0)
	executor := &listenerBacklogCompletingExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.maxQueuedTurnStartsPerCycle = 1
	listenerOutput := &listenerBacklogOutput{buffer: &bytes.Buffer{}}
	bridge.out = listenerOutput

	// Keep the fixture's real-looking work chat separate from the control chat,
	// then leave a second turn queued behind the deliberately blocked first one.
	session := appendBridgeTestSession(t, bridge, store, "s-backlog-history", "chat-backlog-history")
	now := time.Now().UTC()
	queueBridgeTurnForTest(t, bridge, session, "teams-backlog-1", "first durable backlog prompt", now.Add(-2*time.Second))
	queueBridgeTurnForTest(t, bridge, session, "teams-backlog-2", "second durable backlog prompt", now.Add(-time.Second))
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now.Add(-time.Minute))
	listenerRecoverySeedDuePoll(t, store, session.ChatID, now.Add(-time.Minute))

	// A real Codex root with a checkpointed file plus a fresh tail makes the
	// history phase enter its worker instead of returning from an empty scan.
	root := newBridgeTestCodexRoot(t)
	path := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "backlog-history.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create history fixture directory: %v", err)
	}
	initial := listenerRecoveryTranscriptLine("backlog-history-initial", "initial history record")
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial history fixture: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat initial history fixture: %v", err)
	}
	checkpoint := listenerRecoveryHistoryCheckpoint(path, session.ID, "backlog-history-thread", info)
	if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		history[checkpoint.ID] = checkpoint
		*ready = now.Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatalf("seed history checkpoint: %v", err)
	}
	if err := appendListenerRecoveryTranscript(path, listenerRecoveryTranscriptFinalLine("backlog-history-tail", "history tail")); err != nil {
		t.Fatalf("append history fixture tail: %v", err)
	}
	bridge.scope.CodexHome = root
	bridge.lastHistoryWatchReconcile = now
	bridge.lastHistoryWatchSync = time.Time{}

	// A second linked session carries an explicit source-rewrite fence. It is
	// mandatory recovery and must remain eligible even while the Teams queue is
	// active; this keeps the test from proving the unsafe variant that simply
	// skips both maintenance phases.
	mandatorySession := appendBridgeTestSession(t, bridge, store, "s-backlog-mandatory-linked", "chat-backlog-mandatory-linked")
	mandatorySession.CodexThreadID = "thread-backlog-mandatory-linked"
	if err := store.UpdateSession(ctx, mandatorySession.ID, func(state *teamstore.State) error {
		current := state.Sessions[mandatorySession.ID]
		current.CodexThreadID = mandatorySession.CodexThreadID
		state.Sessions[mandatorySession.ID] = current
		return nil
	}); err != nil {
		t.Fatalf("persist mandatory linked session: %v", err)
	}
	mandatoryLinkedPath := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "backlog-mandatory-linked.jsonl")
	mandatoryLinkedContent := `{"type":"session_meta","payload":{"id":"thread-backlog-mandatory-linked","history_mode":"paginated"}}` + "\n" + listenerRecoveryTranscriptLine("mandatory-linked-root", "mandatory linked root")
	if err := os.WriteFile(mandatoryLinkedPath, []byte(mandatoryLinkedContent), 0o600); err != nil {
		t.Fatalf("write mandatory linked fixture: %v", err)
	}
	mandatoryLinkedInfo, err := os.Stat(mandatoryLinkedPath)
	if err != nil {
		t.Fatalf("stat mandatory linked fixture: %v", err)
	}
	mandatoryLinkedSource, ok, err := codexPaginatedHistoryFile(mandatoryLinkedPath, mandatorySession.CodexThreadID)
	if err != nil || !ok {
		t.Fatalf("read mandatory linked fixture identity: ok=%v err=%v", ok, err)
	}
	mandatoryCheckpointID := transcriptCheckpointID(mandatorySession.ID)
	if _, _, err := store.UpdateImportCheckpoint(ctx, mandatoryCheckpointID, func(_ teamstore.ImportCheckpoint, _ bool, updatedAt time.Time) (teamstore.ImportCheckpoint, bool, error) {
		return teamstore.ImportCheckpoint{
			ID:                              mandatoryCheckpointID,
			SessionID:                       mandatorySession.ID,
			SourcePath:                      mandatoryLinkedPath,
			SourceGeneration:                "old-mandatory-linked-source",
			SourceFingerprint:               transcriptCheckpointSourceFingerprint(mandatoryLinkedPath, mandatoryLinkedInfo.Size()),
			LastOffset:                      mandatoryLinkedInfo.Size(),
			LastRecordID:                    "source:mandatory-linked-root",
			LastOffsetKnown:                 true,
			SourceSize:                      mandatoryLinkedInfo.Size(),
			SourceModTime:                   mandatoryLinkedInfo.ModTime(),
			SourceRewriteBlocked:            true,
			SourceRewriteRecoveryIdentity:   mandatoryLinkedSource.Identity,
			SourceRewriteRecoverySize:       mandatoryLinkedInfo.Size(),
			SourceRewriteRecoveryModTime:    mandatoryLinkedInfo.ModTime(),
			SourceRewriteRecoveryChangeTime: teamstore.SourceFileChangeTime(mandatoryLinkedPath, mandatoryLinkedInfo),
			Status:                          importCheckpointStatusBlocked,
			UnresolvedExecution:             &teamstore.ExecutionAnchor{SessionID: mandatorySession.ID, ThreadID: mandatorySession.CodexThreadID, OuterTurnID: "turn-mandatory-linked"},
			UpdatedAt:                       updatedAt,
		}, true, nil
	}); err != nil {
		t.Fatalf("seed mandatory linked checkpoint: %v", err)
	}

	// A separate HistoryWatch row carries the same kind of explicit source
	// rewrite fence. Backlog mode must retain this recovery lane too; testing
	// only linked checkpoints would allow the history phase to be suppressed
	// wholesale without proving the safety boundary.
	mandatoryHistoryPath := filepath.Join(root, "sessions", now.Format("2006"), now.Format("01"), now.Format("02"), "backlog-mandatory-history.jsonl")
	mandatoryHistoryContent := `{"type":"session_meta","payload":{"id":"backlog-history-thread","history_mode":"paginated"}}` + "\n" + listenerRecoveryTranscriptLine("mandatory-history-root", "mandatory history root")
	if err := os.WriteFile(mandatoryHistoryPath, []byte(mandatoryHistoryContent), 0o600); err != nil {
		t.Fatalf("write mandatory history fixture: %v", err)
	}
	mandatoryHistoryInfo, err := os.Stat(mandatoryHistoryPath)
	if err != nil {
		t.Fatalf("stat mandatory history fixture: %v", err)
	}
	mandatoryHistoryCheckpoint := listenerRecoveryHistoryCheckpoint(mandatoryHistoryPath, session.ID, "backlog-history-thread", mandatoryHistoryInfo)
	mandatoryHistoryCheckpoint.SourceGeneration = "old-mandatory-history-source"
	mandatoryHistoryCheckpoint.SourceRewriteBlocked = true
	mandatoryHistoryCheckpoint.SourceRewriteRecoveryIdentity = ""
	if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		history[mandatoryHistoryCheckpoint.ID] = mandatoryHistoryCheckpoint
		return nil
	}); err != nil {
		t.Fatalf("seed mandatory history checkpoint: %v", err)
	}

	historyEntered := make(chan struct{})
	var historyOnce sync.Once
	mandatoryHistoryEntered := make(chan struct{})
	var mandatoryHistoryOnce sync.Once
	bridge.historyWatchPathHook = func(ctx context.Context, path string) error {
		if cleanComparablePath(path) == cleanComparablePath(mandatoryHistoryPath) {
			mandatoryHistoryOnce.Do(func() { close(mandatoryHistoryEntered) })
			return nil
		}
		historyOnce.Do(func() { close(historyEntered) })
		<-ctx.Done()
		return ctx.Err()
	}
	linkedMandatoryEntered := make(chan struct{})
	var linkedMandatoryOnce sync.Once
	bridge.linkedTranscriptSessionHook = func(_ context.Context, session Session) error {
		if session.ID == mandatorySession.ID {
			linkedMandatoryOnce.Do(func() { close(linkedMandatoryEntered) })
		}
		return nil
	}

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	// This vertical test is about the durable backlog gate and the mandatory
	// versus optional lanes.  A 500ms synthetic phase budget makes the SQLite
	// assertion depend on race-instrumented JSON1/query startup rather than on
	// that gate; use the production budgets and keep lease expiry out of the
	// fairness assertion.  Short phase cancellation remains covered by the
	// dedicated poll/cleanup regression tests.
	options.PhaseBudget = mainLoopPhaseBudget
	options.PollWorkerBudget = mainLoopPollWorkerBudget
	options.OwnerStaleAfter = 2 * time.Minute
	listener := startListenerRecovery(t, bridge, options)
	var releaseOnce sync.Once
	releaseExecutor := func() {
		releaseOnce.Do(func() { close(executor.release) })
	}
	t.Cleanup(releaseExecutor)

	select {
	case <-executor.started:
	case err := <-listener.done:
		t.Fatalf("listener exited before durable backlog reached executor: %v; output=%s", err, listenerOutput.String())
	case <-time.After(listenerRecoveryDurableIOProgressTimeout):
		state, _ := store.Load(ctx)
		t.Fatalf("durable Teams backlog never reached the executor: turns=%#v phases={queued:%#v poll:%#v history:%#v} output=%s", state.Turns, bridge.mainLoopPhaseStatsSnapshot("queued-turns"), bridge.mainLoopPhaseStatsSnapshot("poll"), bridge.mainLoopPhaseStatsSnapshot("history-watch"), listenerOutput.String())
	}

	// The desired invariant is observable without waiting for a lease takeover:
	// while one turn is running and another remains queued, ordinary history
	// maintenance must not enter its worker. Mandatory linked recovery below is
	// still expected to run in the same backlog cycle.
	select {
	case <-historyEntered:
		t.Fatalf("history maintenance entered while Teams backlog was still durable; phase=%#v", bridge.mainLoopPhaseStatsSnapshot("history-watch"))
	case <-time.After(750 * time.Millisecond):
	}
	select {
	case <-mandatoryHistoryEntered:
		// Mandatory source-rewrite recovery remains eligible during backlog mode.
	case listenerErr := <-listener.done:
		t.Fatalf("listener exited before mandatory history recovery ran: %v; output=%s", listenerErr, listenerOutput.String())
	case <-time.After(listenerRecoveryDurableIOProgressTimeout):
		t.Fatalf("mandatory history recovery did not run during backlog; phase=%#v output=%s", bridge.mainLoopPhaseStatsSnapshot("history-watch"), listenerOutput.String())
	}
	select {
	case <-linkedMandatoryEntered:
		// Mandatory source-rewrite recovery remains eligible during backlog mode.
	case listenerErr := <-listener.done:
		t.Fatalf("listener exited before mandatory linked recovery ran: %v; output=%s", listenerErr, listenerOutput.String())
	case <-time.After(listenerRecoveryDurableIOProgressTimeout):
		t.Fatalf("mandatory linked recovery did not run during backlog; phase=%#v output=%s", bridge.mainLoopPhaseStatsSnapshot("linked-transcript"), listenerOutput.String())
	}

	backlog, err := store.TeamsOperationalBacklog(ctx)
	if err != nil {
		t.Fatalf("check durable Teams backlog after observation: %v", err)
	}
	if !backlog.Active() {
		t.Fatal("Teams backlog disappeared while the first executor was deliberately blocked")
	}
	control, err := store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("read durable optional-maintenance deferral: %v", err)
	}
	if control.OptionalMaintenanceDeferredUntil.IsZero() || control.OptionalMaintenanceDeferredReason == "" {
		t.Fatalf("optional-maintenance deferral = %#v, want durable wake deadline/reason", control)
	}

	releaseExecutor()
	// After the blocked turn is released, the second durable turn still has to
	// pass claim, execution, completion, and outbox delivery before the
	// optional lane may wake.  Under -race that multi-step tail can exceed the
	// ordinary 10-second assertion window even though it is making progress.
	deadline := time.Now().Add(listenerRecoveryMultiStepProgressTimeout)
	for time.Now().Before(deadline) {
		backlog, err = store.TeamsOperationalBacklog(ctx)
		if err != nil {
			t.Fatalf("check Teams backlog after releasing executor: %v", err)
		}
		if !backlog.Active() {
			break
		}
		time.Sleep(listenerRecoveryPollInterval)
	}
	if backlog.Active() {
		state, _ := store.Load(ctx)
		t.Fatalf("Teams operational backlog did not drain after executor release: backlog=%#v turns=%#v output=%s", backlog, state.Turns, listenerOutput.String())
	}
	select {
	case <-historyEntered:
		// The durable wake edge must restore ordinary maintenance after the
		// Teams queue is no longer active.
	case err := <-listener.done:
		t.Fatalf("listener exited before optional maintenance woke: %v; output=%s", err, listenerOutput.String())
	case <-time.After(listenerRecoveryMultiStepProgressTimeout):
		t.Fatalf("optional history maintenance did not wake after backlog drain; control=%#v phase=%#v output=%s", func() teamstore.ServiceControl {
			control, _ := store.ReadControl(ctx)
			return control
		}(), bridge.mainLoopPhaseStatsSnapshot("history-watch"), listenerOutput.String())
	}
	control, err = store.ReadControl(ctx)
	if err != nil {
		t.Fatalf("read optional-maintenance control after backlog drain: %v", err)
	}
	if !control.OptionalMaintenanceDeferredUntil.IsZero() || control.OptionalMaintenanceDeferredReason != "" {
		t.Fatalf("optional-maintenance deferral remained after backlog drain: %#v", control)
	}
	listener.stop(t)
}

type listenerBacklogOutput struct {
	mu     sync.Mutex
	buffer *bytes.Buffer
}

func (w *listenerBacklogOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}

func (w *listenerBacklogOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

func backlogHistoryTestMessage(id string, text string) ChatMessage {
	message := bridgeTestMessage(id)
	message.ChatID = "chat-backlog-history"
	message.Body.ContentType = "html"
	message.Body.Content = "<p>" + text + "</p>"
	return message
}

type listenerBacklogCompletingExecutor struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	once    sync.Once
	calls   int
}

func (e *listenerBacklogCompletingExecutor) Run(ctx context.Context, _ *Session, _ string) (ExecutionResult, error) {
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	}
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	return ExecutionResult{
		Text:          "durable backlog completion",
		CodexThreadID: "thread-backlog-history",
		CodexTurnID:   fmt.Sprintf("turn-backlog-history-%d", call),
	}, nil
}
