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
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestTeamsBeaconControlProfileLifecycleAndList(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	ctx := context.Background()
	adapter := writeBeaconProviderFixture(t, "provider_job_id=slurm-doctor raw_state=PD reason=doctor")
	create := "beacon profile create gpu --provider slurm --partition interactive --image image.sqsh --nodes 1 --gpu 1 --duration 4 --query-command " + adapter + " --submit-command " + adapter + " --cancel-command " + adapter + " --renew-command " + adapter
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-create", create), create); err != nil {
		t.Fatalf("create profile: %v", err)
	}
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-doctor", "beacon profile doctor gpu --smoke"), "beacon profile doctor gpu --smoke"); err != nil {
		t.Fatalf("doctor profile: %v", err)
	}
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-confirm", "beacon profile confirm gpu"), "beacon profile confirm gpu"); err != nil {
		t.Fatalf("confirm profile: %v", err)
	}
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-list", "beacon list"), "beacon list"); err != nil {
		t.Fatalf("list beacon: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Created beacon profile \"gpu\"", "Profile gpu doctor passed", "Smoke test:", "smoke_submit", "Confirmed beacon profile \"gpu\"", "Beacon list", "Profiles:", "gpu: ready", "Machines:"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon output missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconNewWorkChatAndWorkSwitch(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	seedTeamsBeaconProfile(t, "cpu")
	graph, sent := newBeaconMeetingBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	workDir := t.TempDir()

	ctx := context.Background()
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-new", "new "+workDir+" --beacon gpu"), "new "+workDir+" --beacon gpu"); err != nil {
		t.Fatalf("new beacon work chat: %v", err)
	}
	if len(bridge.reg.Sessions) < 2 {
		t.Fatalf("expected a new session, sessions=%#v", bridge.reg.Sessions)
	}
	session := bridge.reg.Sessions[len(bridge.reg.Sessions)-1]
	if err := bridge.handleSessionMessage(ctx, session.ChatID, bridgeTestMessageWithText("beacon-status", "beacon status"), "beacon status"); err != nil {
		t.Fatalf("work beacon status: %v", err)
	}
	if err := bridge.handleSessionMessage(ctx, session.ChatID, bridgeTestMessageWithText("beacon-switch", "beacon switch cpu"), "beacon switch cpu"); err != nil {
		t.Fatalf("work beacon switch: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Execution target: beacon:gpu", "Beacon status", "Current target: beacon:gpu", "profile: gpu", "Switched this Work chat to beacon:cpu"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon work output missing %q:\n%s", want, joined)
		}
	}
	st := loadTeamsBeaconState(t)
	conv := st.Conversations[session.ID]
	if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "cpu" {
		t.Fatalf("conversation current = %#v, want beacon cpu", conv.Current)
	}
}

func TestTeamsBeaconTurnDoesNotFallBackToLocalExecutor(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Conversations["s001"] = beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", Signature: "sig-gpu"}}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon conversation: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	teamStore := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "local should not run"}}
	bridge := newBridgeTestBridge(graph, teamStore, executor)

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("beacon-run", "run on gpu"), "run on gpu"); err != nil {
		t.Fatalf("handleSessionMessage: %v", err)
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("beacon turn must not fall back to local Codex executor, prompts=%#v", executor.prompts)
	}
	teamsState, err := teamStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	var failed teamstore.Turn
	for _, turn := range teamsState.Turns {
		failed = turn
	}
	if failed.Status != teamstore.TurnStatusFailed || !strings.Contains(failed.FailureMessage, "Beacon cannot start") {
		t.Fatalf("turn should fail before local Codex starts, turn=%#v", failed)
	}
	beaconState := loadTeamsBeaconState(t)
	if len(beaconState.Allocations) != 1 {
		t.Fatalf("beacon allocation request count = %d, want 1: %#v", len(beaconState.Allocations), beaconState.Allocations)
	}
	for _, req := range beaconState.Allocations {
		if req.State != beacon.AllocationNeedsAttention || !strings.Contains(req.ProviderReason, beacon.BeaconSlurmQueryCommandEnv) {
			t.Fatalf("failed beacon start should mark allocation needs_attention, req=%#v", req)
		}
	}
	if queued := beaconState.Conversations["s001"].Queued; len(queued) != 0 {
		t.Fatalf("failed pre-start beacon turn should not leave a queued turn snapshot, queued=%#v", queued)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Beacon cannot start", "Slurm provider adapter is not configured", "explicit beacon targets disable local fallback", "error_code: `BEACON_PROVIDER_ADAPTER_NOT_CONFIGURED`", "allocation: `req-"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon failure response missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconTurnReconcilesConfiguredProviderAdapter(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv(beacon.BeaconSlurmQueryCommandEnv, writeBeaconProviderFixture(t, `durable_negative=true`))
	t.Setenv(beacon.BeaconSlurmSubmitCommandEnv, writeBeaconProviderFixture(t, `{"provider_job_id":"slurm-999","raw_state":"PD","reason":"submitted"}`))
	prevAllocationPoll := beaconAllocationPollInterval
	prevJobPoll := beaconJobPollInterval
	beaconAllocationPollInterval = 5 * time.Millisecond
	beaconJobPollInterval = 5 * time.Millisecond
	t.Cleanup(func() {
		beaconAllocationPollInterval = prevAllocationPoll
		beaconJobPollInterval = prevJobPoll
	})
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Conversations["s001"] = beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", Signature: "sig-gpu"}}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon conversation: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	teamStore := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "local should not run"}}
	bridge := newBridgeTestBridge(graph, teamStore, executor)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- bridge.handleSessionMessage(ctx, "chat-1", bridgeTestMessageWithText("beacon-run-provider", "run on configured gpu"), "run on configured gpu")
	}()
	req := waitForBeaconAllocationProviderJob(t, store, "slurm-999")
	if err := store.Update(func(st *beacon.State) error {
		_, err := beacon.RegisterWorkerMachineForAllocation(st, req.ID, beacon.WorkerRegistrationInput{
			MachineID:     "machine-provider",
			LeaseID:       "lease-provider",
			ProviderJobID: "slurm-999",
			Host:          "worker-host",
			State:         beacon.LeaseAccepting,
			Doctor:        healthyTeamsBeaconDoctor(),
		}, time.Now())
		return err
	}); err != nil {
		t.Fatalf("register worker machine: %v", err)
	}
	job := waitForBeaconQueuedJobOrDone(t, store, "machine-provider", done)
	if err := store.Update(func(st *beacon.State) error {
		claimed, ok, err := beacon.ClaimNextJobForMachine(st, "machine-provider", "worker-provider", time.Now())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("queued provider job disappeared")
		}
		if claimed.ID != job.ID {
			return fmt.Errorf("claimed job %s, want %s", claimed.ID, job.ID)
		}
		started, err := beacon.MarkJobStarted(st, claimed.ID, time.Now())
		if err != nil {
			return err
		}
		_, err = beacon.AcceptWorkerTerminal(st, beacon.WorkerTerminalEnvelope{
			JobID:      started.ID,
			RequestID:  started.RequestID,
			TurnID:     started.TurnID,
			WorkerID:   started.WorkerID,
			LeaseID:    started.LeaseID,
			ClaimEpoch: started.ClaimEpoch,
			ProviderIdentity: beacon.ProviderIdentity{
				ProviderJobID: started.ProviderIdentity.ProviderJobID,
			},
			Payload: []byte(`{"text":"provider remote done","codex_thread_id":"thread-provider","codex_turn_id":"turn-provider"}`),
		}, time.Now())
		return err
	}); err != nil {
		t.Fatalf("complete provider beacon job: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleSessionMessage: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider beacon job completion")
	}
	if len(executor.prompts) != 0 {
		t.Fatalf("beacon turn must not run local Codex while allocation is pending or remote-ready, prompts=%#v", executor.prompts)
	}
	beaconState := loadTeamsBeaconState(t)
	if len(beaconState.Allocations) != 1 {
		t.Fatalf("beacon allocation request count = %d, want 1", len(beaconState.Allocations))
	}
	for _, req := range beaconState.Allocations {
		if req.ProviderIdentity.ProviderJobID != "slurm-999" || req.RawProviderState != "PD" || req.ProviderReason != "submitted" {
			t.Fatalf("provider adapter result not persisted: %#v", req)
		}
		if req.State != beacon.AllocationRunning {
			t.Fatalf("allocation should be bound to accepting worker after remote completion: %#v", req)
		}
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"provider remote done"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon provider response missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconReadyWorkerCompletesViaBeaconJob(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	prevPoll := beaconJobPollInterval
	beaconJobPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { beaconJobPollInterval = prevPoll })
	seedTeamsBeaconProfile(t, "gpu")
	beaconStore, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := beaconStore.Update(func(st *beacon.State) error {
		st.Machines["machine-1"] = beacon.Machine{ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(beacon.LeaseAccepting), ProviderState: beacon.ProviderJobRunning, Doctor: healthyTeamsBeaconDoctor(), LastHeartbeat: time.Now()}
		st.Conversations["s001"] = beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", MachineID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1"}}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	teamStore := newBridgeTestStore(t)
	localExecutor := &recordingExecutor{result: ExecutionResult{Text: "local should not run"}}
	bridge := newBridgeTestBridge(graph, teamStore, localExecutor)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- bridge.handleSessionMessage(ctx, "chat-1", bridgeTestMessageWithText("beacon-worker-run", "run remotely"), "run remotely")
	}()
	job := waitForBeaconQueuedJob(t, beaconStore, "machine-1")
	if err := beaconStore.Update(func(st *beacon.State) error {
		claimed, ok, err := beacon.ClaimNextJobForMachine(st, "machine-1", "worker-1", time.Now())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("queued job disappeared")
		}
		if claimed.ID != job.ID {
			return fmt.Errorf("claimed job %s, want %s", claimed.ID, job.ID)
		}
		started, err := beacon.MarkJobStarted(st, claimed.ID, time.Now())
		if err != nil {
			return err
		}
		_, err = beacon.AcceptWorkerTerminal(st, beacon.WorkerTerminalEnvelope{
			JobID:      started.ID,
			RequestID:  started.RequestID,
			TurnID:     started.TurnID,
			WorkerID:   started.WorkerID,
			LeaseID:    started.LeaseID,
			ClaimEpoch: started.ClaimEpoch,
			ProviderIdentity: beacon.ProviderIdentity{
				ProviderJobID: started.ProviderIdentity.ProviderJobID,
			},
			Payload: []byte(`{"text":"remote done","codex_thread_id":"thread-remote","codex_turn_id":"turn-remote"}`),
		}, time.Now())
		return err
	}); err != nil {
		t.Fatalf("complete beacon job: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleSessionMessage: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for beacon job completion")
	}
	if len(localExecutor.prompts) != 0 {
		t.Fatalf("ready beacon worker must not use local executor, prompts=%#v", localExecutor.prompts)
	}
	teamsState, err := teamStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	var completed teamstore.Turn
	for _, turn := range teamsState.Turns {
		completed = turn
	}
	if completed.Status != teamstore.TurnStatusCompleted || completed.CodexThreadID != "thread-remote" || completed.CodexTurnID != "turn-remote" {
		t.Fatalf("teams turn = %#v", completed)
	}
	beaconState := loadTeamsBeaconState(t)
	if queued := beaconState.Conversations["s001"].Queued; len(queued) != 0 {
		t.Fatalf("completed beacon turn should clean queued snapshot, queued=%#v", queued)
	}
	if jobs := beaconState.Machines["machine-1"].Jobs; len(jobs) != 0 {
		t.Fatalf("completed beacon job should leave machine reusable, jobs=%#v", jobs)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "remote done") {
		t.Fatalf("remote terminal response missing:\n%s", joined)
	}
}

func TestBeaconJobExecutorRechecksMachineReadinessBeforeEnqueue(t *testing.T) {
	store, err := beacon.NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	var req beacon.AllocationRequest
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles["gpu"] = beacon.Profile{
			Name:              "gpu",
			Provider:          beacon.ProviderSlurm,
			ProxyMode:         beacon.ProxyNone,
			IsolationDefault:  beacon.IsolationShared,
			Slurm:             beacon.SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4},
			Confirmed:         true,
			ProviderPreviewOK: true,
			DoctorOK:          true,
		}
		var err error
		req, _, err = beacon.EnsureAllocationRequest(st, "s001", "turn-stale-machine", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", Signature: "sig-gpu", MachineID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1"}, time.Now())
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.State = beacon.AllocationRunning
		req.Target.MachineID = "machine-1"
		req.Target.LeaseID = "lease-1"
		req.Target.ProviderJobID = "slurm-1"
		st.Allocations[req.ID] = req
		st.Machines["machine-1"] = beacon.Machine{
			ID:            "machine-1",
			LeaseID:       "lease-1",
			ProviderJobID: "slurm-1",
			Profile:       "gpu",
			State:         string(beacon.LeaseAccepting),
			ProviderState: beacon.ProviderJobRunning,
			Doctor:        healthyTeamsBeaconDoctor(),
			Execution:     beacon.ExecutionSignature{Hash: "sig-gpu"},
			LastHeartbeat: time.Now().Add(-10 * time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stale beacon machine: %v", err)
	}
	_, err = enqueueBeaconJobForTurn(context.Background(), store, beacon.TurnExecutionPlan{
		Action:              beacon.TurnRunBeacon,
		ConversationID:      "s001",
		TurnID:              "turn-stale-machine",
		AllocationRequestID: req.ID,
		MachineID:           "machine-1",
		LeaseID:             "lease-1",
		ProviderJobID:       "slurm-1",
		Snapshot:            beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", Signature: "sig-gpu", MachineID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1"},
	}, &Session{ID: "s001", Cwd: t.TempDir()}, ExecutionInput{Prompt: "must not enqueue on stale worker"})
	if err == nil || !strings.Contains(err.Error(), "no longer accepting") {
		t.Fatalf("enqueue should reject stale machine, err=%v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load beacon store: %v", err)
	}
	if len(st.JobAttempts) != 0 {
		t.Fatalf("stale machine should not receive a queued job: %#v", st.JobAttempts)
	}
}

func TestTeamsBeaconNewUsesSelectedWorkspaceAndIsolation(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
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
	bindBridgeTestControlChat(t, store, "control-chat")
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.reg.Sessions = nil

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-selected-projects", "projects"), "projects"); err != nil {
		t.Fatalf("projects: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-selected-workspace", "1"), "1"); err != nil {
		t.Fatalf("select workspace: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-selected-new", "new --beacon gpu --beacon-isolation exclusive"), "new --beacon gpu --beacon-isolation exclusive"); err != nil {
		t.Fatalf("new selected beacon workspace: %v", err)
	}

	session := bridge.reg.SessionByID("s001")
	if session == nil || session.Cwd != workDir {
		t.Fatalf("new --beacon without directory did not use selected workspace: %#v", bridge.reg.Sessions)
	}
	if !strings.Contains(createdTopic, "New message in "+filepath.Base(workDir)) {
		t.Fatalf("created topic = %q, want selected workspace placeholder title", createdTopic)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Execution target: beacon:gpu isolation=exclusive") {
		t.Fatalf("ready anchor missing beacon isolation target:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	conv := st.Conversations["s001"]
	if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" || conv.Current.Isolation != beacon.IsolationExclusive {
		t.Fatalf("conversation target = %#v, want beacon gpu exclusive", conv.Current)
	}
}

func TestTeamsBeaconSwitchDuringRunningTurnSchedulesPending(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test bridge missing s001")
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-running", SessionID: "s001", Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("seed running turn: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("beacon-switch-running", "beacon switch gpu"), "beacon switch gpu"); err != nil {
		t.Fatalf("switch during running: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Scheduled switch to beacon:gpu") || !strings.Contains(joined, "Future turns will use the pending target") {
		t.Fatalf("pending switch acknowledgement missing:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	conv := st.Conversations["s001"]
	if conv.Current.Target != "" && conv.Current.Target != beacon.TargetLocal {
		t.Fatalf("current target = %#v, want local/default", conv.Current)
	}
	if conv.Pending == nil || conv.Pending.Target != beacon.TargetBeacon || conv.Pending.Profile != "gpu" {
		t.Fatalf("pending target = %#v, want beacon gpu", conv.Pending)
	}
}

func TestTeamsBeaconWorkSwitchLocalImmediateAndPending(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test bridge missing s001")
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	beaconStore, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := beaconStore.Update(func(st *beacon.State) error {
		st.Conversations["s001"] = beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}}
		req, _, err := beacon.EnsureAllocationRequest(st, "s001", "turn-local-release", st.Conversations["s001"].Current, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.State = beacon.AllocationRunning
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed beacon conversation: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("beacon-local-idle", "beacon local"), "beacon local"); err != nil {
		t.Fatalf("switch local idle: %v", err)
	}
	st := loadTeamsBeaconState(t)
	if conv := st.Conversations["s001"]; conv.Current.Target != beacon.TargetLocal || conv.Pending != nil {
		t.Fatalf("idle local switch conversation = %#v, want current local with no pending", conv)
	}
	var released bool
	for _, req := range st.Allocations {
		if req.ConversationID == "s001" && req.State == beacon.AllocationCanceled {
			released = true
		}
	}
	if !released {
		t.Fatalf("switch local should release old allocation, allocations=%#v", st.Allocations)
	}

	pending := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
	var runningReqID string
	if err := beaconStore.Update(func(st *beacon.State) error {
		current := beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}
		st.Conversations[pending.ID] = beacon.Conversation{ID: pending.ID, Current: current}
		req, _, err := beacon.EnsureAllocationRequest(st, pending.ID, "turn-running-local", current, time.Unix(2, 0))
		if err != nil {
			return err
		}
		req.State = beacon.AllocationRunning
		st.Allocations[req.ID] = req
		runningReqID = req.ID
		return nil
	}); err != nil {
		t.Fatalf("seed pending beacon conversation: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-running-local", SessionID: pending.ID, Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("seed running turn: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), pending.ChatID, bridgeTestMessageWithText("beacon-local-running", "beacon switch local"), "beacon switch local"); err != nil {
		t.Fatalf("switch local running: %v", err)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Switched this Work chat to local execution", "Scheduled target switch", "Future turns will use local"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("local switch output missing %q:\n%s", want, joined)
		}
	}
	st = loadTeamsBeaconState(t)
	conv := st.Conversations[pending.ID]
	if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" {
		t.Fatalf("running local switch changed current target: %#v", conv.Current)
	}
	if conv.Pending == nil || conv.Pending.Target != beacon.TargetLocal {
		t.Fatalf("running local switch pending target = %#v, want local", conv.Pending)
	}
	if st.Allocations[runningReqID].State != beacon.AllocationRunning {
		t.Fatalf("pending local switch must not release the active allocation early: %#v", st.Allocations[runningReqID])
	}
}

func TestTeamsBeaconWorkReleaseSharedResourceDetachesCurrentChat(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	var requestID string
	if err := store.Update(func(st *beacon.State) error {
		conv := beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}}
		st.Conversations["s001"] = conv
		req, _, err := beacon.EnsureAllocationRequest(st, "s001", "turn-1", conv.Current, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.State = beacon.AllocationRunning
		req.ProviderIdentity.ProviderJobID = "slurm-shared"
		st.Allocations[req.ID] = req
		requestID = req.ID
		st.Machines["shared-a"] = beacon.Machine{ID: "shared-a", LeaseID: "lease-shared", ProviderJobID: "slurm-shared", Profile: "gpu", State: string(beacon.LeaseAccepting), Chats: []string{"s001", "s002"}}
		return nil
	}); err != nil {
		t.Fatalf("seed shared allocation: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("work-release-shared", "beacon release"), "beacon release"); err != nil {
		t.Fatalf("work release shared: %v", err)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Beacon release", "Detached this Work chat from shared beacon resources", "detached allocations: " + requestID} {
		if !strings.Contains(joined, want) {
			t.Fatalf("shared release detach output missing %q:\n%s", want, joined)
		}
	}
	st := loadTeamsBeaconState(t)
	if st.Allocations[requestID].State != beacon.AllocationCanceled {
		t.Fatalf("shared release should detach/cancel this chat allocation: %#v", st.Allocations[requestID])
	}
	if got := strings.Join(st.Machines["shared-a"].Chats, ","); got != "s002" {
		t.Fatalf("shared machine chats = %q, want s002", got)
	}
}

func TestTeamsBeaconReconcileReleasesAllocationWithNoConversationDemand(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	var requestID string
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles["gpu"] = beacon.Profile{Name: "gpu", Provider: beacon.ProviderLocal, ProxyMode: beacon.ProxyNone, ProviderPreviewOK: true, DoctorOK: true, Confirmed: true}
		req, _, err := beacon.EnsureAllocationRequest(st, "s001", "turn-1", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.Provider = beacon.ProviderLocal
		req.State = beacon.AllocationRunning
		st.Allocations[req.ID] = req
		requestID = req.ID
		st.Conversations["s001"] = beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetLocal}}
		return nil
	}); err != nil {
		t.Fatalf("seed stale demand: %v", err)
	}
	graph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})
	if err := bridge.reconcileBeaconState(context.Background(), store, beacon.NewCommandProviderAdapterFromEnv(nil), time.Unix(2, 0)); err != nil {
		t.Fatalf("reconcile beacon state: %v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load beacon state: %v", err)
	}
	if st.Allocations[requestID].State != beacon.AllocationCanceled {
		t.Fatalf("allocation state = %s, want canceled", st.Allocations[requestID].State)
	}
}

type beaconCancelIntentAdapter struct {
	cancels int
}

func (a *beaconCancelIntentAdapter) QueryAllocation(context.Context, beacon.AllocationRequest) (beacon.SchedulerQueryResult, error) {
	return beacon.SchedulerQueryResult{RawState: "R", Reason: "running"}, nil
}

func (a *beaconCancelIntentAdapter) SubmitAllocation(context.Context, beacon.AllocationRequest) (beacon.SchedulerQueryResult, error) {
	return beacon.SchedulerQueryResult{}, nil
}

func (a *beaconCancelIntentAdapter) CancelAllocation(_ context.Context, req beacon.AllocationRequest) (beacon.SchedulerQueryResult, error) {
	a.cancels++
	return beacon.SchedulerQueryResult{ProviderJobID: req.ProviderIdentity.ProviderJobID, RawState: "CA", Reason: "cancel_requested"}, nil
}

func TestTeamsBeaconReconcileRetriesPendingCancelIntent(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Allocations["req-1"] = beacon.AllocationRequest{
			ID:                "req-1",
			ConversationID:    "s001",
			TurnID:            "turn-1",
			Profile:           "gpu",
			Provider:          beacon.ProviderSlurm,
			State:             beacon.AllocationNeedsAttention,
			DeterministicName: "cxp-req-1",
			ProviderIdentity:  beacon.ProviderIdentity{ProviderJobID: "slurm-1"},
			CancelRequestedAt: time.Unix(1, 0),
			CancelReason:      "adapter was missing earlier",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cancel intent: %v", err)
	}
	adapter := &beaconCancelIntentAdapter{}
	bridge := newBridgeTestBridge(nil, newBridgeTestStore(t), &recordingExecutor{})
	if err := bridge.reconcileBeaconState(context.Background(), store, adapter, time.Unix(2, 0)); err != nil {
		t.Fatalf("reconcile beacon state: %v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load beacon state: %v", err)
	}
	if adapter.cancels != 1 || st.Allocations["req-1"].State != beacon.AllocationCanceled {
		t.Fatalf("cancel intent was not retried cleanly: cancels=%d allocation=%#v", adapter.cancels, st.Allocations["req-1"])
	}
}

func TestTeamsBeaconReconcileDoesNotCancelSharedDetachWhileJobStarted(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	now := time.Unix(2, 0)
	if err := store.Update(func(st *beacon.State) error {
		st.Conversations["s001"] = beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}}
		st.Conversations["s002"] = beacon.Conversation{ID: "s002", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}}
		st.Allocations["req-a"] = beacon.AllocationRequest{
			ID:                "req-a",
			ConversationID:    "s001",
			TurnID:            "turn-a",
			Profile:           "gpu",
			Provider:          beacon.ProviderSlurm,
			State:             beacon.AllocationRunning,
			DeterministicName: "cxp-req-a",
			ProviderIdentity:  beacon.ProviderIdentity{ProviderJobID: "slurm-1"},
			DetachRequestedAt: now,
			DetachReason:      "released from Teams Work chat",
		}
		st.Allocations["req-b"] = beacon.AllocationRequest{
			ID:                "req-b",
			ConversationID:    "s002",
			TurnID:            "turn-b",
			Profile:           "gpu",
			Provider:          beacon.ProviderSlurm,
			State:             beacon.AllocationRunning,
			DeterministicName: "cxp-req-b",
			ProviderIdentity:  beacon.ProviderIdentity{ProviderJobID: "slurm-1"},
		}
		st.Machines["machine-1"] = beacon.Machine{ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(beacon.LeaseAccepting), Chats: []string{"s001", "s002"}, Jobs: []string{"job-a"}}
		st.JobAttempts["job-a"] = beacon.JobAttempt{ID: "job-a", RequestID: "req-a", TurnID: "turn-a", Phase: beacon.JobStarted}
		return nil
	}); err != nil {
		t.Fatalf("seed detach intent: %v", err)
	}
	adapter := &beaconCancelIntentAdapter{}
	bridge := newBridgeTestBridge(nil, newBridgeTestStore(t), &recordingExecutor{})
	if err := bridge.reconcileBeaconState(context.Background(), store, adapter, now.Add(time.Minute)); err != nil {
		t.Fatalf("reconcile beacon state: %v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load beacon state: %v", err)
	}
	if adapter.cancels != 0 {
		t.Fatalf("shared detach with started work must not cancel provider job, cancels=%d", adapter.cancels)
	}
	if req := st.Allocations["req-a"]; req.State != beacon.AllocationRunning || !req.CancelRequestedAt.IsZero() || req.DetachRequestedAt.IsZero() {
		t.Fatalf("detach allocation mutated incorrectly: %#v", req)
	}
	if got := strings.Join(st.Machines["machine-1"].Chats, ","); got != "s001,s002" {
		t.Fatalf("active detach should not remove chat before job drains, got %q", got)
	}
}

func TestTeamsBeaconListIncludesAllocationsAndMachines(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	storePath := filepath.Clean(strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_STORE")))
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Allocations["req-1"] = beacon.AllocationRequest{
			ID:                "req-1",
			ConversationID:    "s001",
			TurnID:            "turn-1",
			Profile:           "gpu",
			Provider:          beacon.ProviderSlurm,
			State:             beacon.AllocationSubmitted,
			DeterministicName: "cxp-req-1",
			ProviderIdentity:  beacon.ProviderIdentity{ProviderJobID: "slurm-1"},
			RawProviderState:  "PD",
			ProviderReason:    "resources",
			CreatedAt:         time.Unix(1, 0),
		}
		st.Machines["gpu-a"] = beacon.Machine{ID: "gpu-a", LeaseID: "lease-gpu", Profile: "gpu", State: "accepting", Jobs: []string{"job-1"}, Chats: []string{"s001"}}
		return nil
	}); err != nil {
		t.Fatalf("seed machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-list-machines", "beacon list"), "beacon list"); err != nil {
		t.Fatalf("beacon list: %v", err)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Beacon list", "Profiles:", "gpu: ready", "Allocations:", "req-1: submitted on gpu (Slurm)", "provider_job=slurm-1", "Machines:", "gpu-a: accepting on gpu"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon list missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconProfileStatusShowsMissingProxyProfile(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-create", "beacon profile create jump --provider local --proxy ssh_profile --proxy-profile missing"), "beacon profile create jump --provider local --proxy ssh_profile --proxy-profile missing"); err != nil {
		t.Fatalf("create proxy profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-doctor", "beacon profile doctor jump"), "beacon profile doctor jump"); err != nil {
		t.Fatalf("doctor proxy profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-confirm", "beacon profile confirm jump"), "beacon profile confirm jump"); err != nil {
		t.Fatalf("confirm proxy profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-status", "beacon profile status jump"), "beacon profile status jump"); err != nil {
		t.Fatalf("status proxy profile: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"ssh_profile:missing", "proxy profile not found", "ready: false"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing proxy output missing %q:\n%s", want, joined)
		}
	}
	st := loadTeamsBeaconState(t)
	if st.Profiles["jump"].Ready(bridge.beaconProxyResolver()) {
		t.Fatalf("profile with missing proxy should remain draft: %#v", st.Profiles["jump"])
	}
}

func TestTeamsBeaconNewWithDraftProfileRejectsWithoutCreatingWorkChat(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles["draft"] = beacon.Profile{Name: "draft", Provider: beacon.ProviderSlurm, ProxyMode: beacon.ProxyNone}
		return nil
	}); err != nil {
		t.Fatalf("seed draft profile: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	teamStore := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, teamStore, &recordingExecutor{})
	workDir := filepath.Join(t.TempDir(), "should-not-create")

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-new-draft", "new "+workDir+" --beacon draft"), "new "+workDir+" --beacon draft"); err != nil {
		t.Fatalf("new draft beacon: %v", err)
	}

	if len(bridge.reg.Sessions) != 1 {
		t.Fatalf("draft beacon should not create a new work chat, sessions=%#v", bridge.reg.Sessions)
	}
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		t.Fatalf("draft beacon should not create the workspace, stat err=%v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "cannot create beacon work chat for profile \"draft\"") ||
		strings.Contains(joined, "Work chat created") {
		t.Fatalf("draft beacon response mismatch:\n%s", joined)
	}
}

func TestTeamsBeaconNewRejectsInvalidIsolationBeforeSideEffects(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	teamStore := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, teamStore, &recordingExecutor{})
	missingProfileDir := filepath.Join(t.TempDir(), "missing-profile")
	badIsolationDir := filepath.Join(t.TempDir(), "bad-isolation")

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-new-isolation-no-profile", "new "+missingProfileDir+" --beacon-isolation exclusive"), "new "+missingProfileDir+" --beacon-isolation exclusive"); err != nil {
		t.Fatalf("new beacon isolation without profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-new-isolation-invalid", "new "+badIsolationDir+" --beacon gpu --beacon-isolation solo"), "new "+badIsolationDir+" --beacon gpu --beacon-isolation solo"); err != nil {
		t.Fatalf("new beacon invalid isolation: %v", err)
	}

	if len(bridge.reg.Sessions) != 1 {
		t.Fatalf("invalid beacon new requests should not create Work chats, sessions=%#v", bridge.reg.Sessions)
	}
	for _, dir := range []string{missingProfileDir, badIsolationDir} {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("invalid beacon new request should not create workspace %s, stat err=%v", dir, err)
		}
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"--beacon-isolation requires --beacon <profile>", "isolation must be shared or exclusive"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("invalid isolation response missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconWrongChatCommandsDoNotMutateState(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("wrong-work-profile", "beacon profile create gpu --provider local"), "beacon profile create gpu --provider local"); err != nil {
		t.Fatalf("work wrong-chat profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("wrong-control-switch", "beacon switch gpu"), "beacon switch gpu"); err != nil {
		t.Fatalf("control wrong-chat switch: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if countSentPlainContaining(*sent, "Wrong chat") != 2 {
		t.Fatalf("wrong chat responses missing:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	if len(st.Profiles) != 0 || len(st.Conversations) != 0 {
		t.Fatalf("wrong-chat commands mutated beacon state: %#v", st)
	}
}

func TestTeamsBeaconWorkCommandFromCoworkerIsRejectedWithoutMutatingState(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessageWithText("beacon-coworker-switch", "beacon switch gpu")
	msg.From.User.ID = "user-2"
	msg.From.User.DisplayName = "Alex Kim"

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "beacon switch gpu"); err != nil {
		t.Fatalf("coworker beacon switch: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if len(*sent) != 1 || (*sent)[0].Mentions != 1 || !strings.Contains(joined, "Alex Kim") || !strings.Contains(joined, "Only the helper owner can run helper commands") {
		t.Fatalf("coworker beacon rejection mismatch, sent=%#v plain=%q", *sent, joined)
	}
	st := loadTeamsBeaconState(t)
	if len(st.Conversations) != 0 {
		t.Fatalf("coworker beacon command mutated conversations: %#v", st.Conversations)
	}
}

func TestTeamsBeaconMutationIsIdempotentAfterSendFailure(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newFlakyBeaconMessageGraph(t, 1)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessageWithText("beacon-create-flaky", "beacon profile create gpu --provider local")

	if err := bridge.handleControlMessage(context.Background(), msg, "beacon profile create gpu --provider local"); err == nil {
		t.Fatal("first send should fail after mutating beacon state")
	}
	st := loadTeamsBeaconState(t)
	if _, ok := st.Profiles["gpu"]; !ok {
		t.Fatalf("profile should exist after failed Teams send: %#v", st.Profiles)
	}
	if err := bridge.handleControlMessage(context.Background(), msg, "beacon profile create gpu --provider local"); err != nil {
		t.Fatalf("retry same Teams message should use beacon idempotency: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Created beacon profile \"gpu\"") {
		t.Fatalf("retry response missing create result:\n%s", joined)
	}
	st = loadTeamsBeaconState(t)
	if len(st.Profiles) != 1 {
		t.Fatalf("duplicate retry should not create extra profiles: %#v", st.Profiles)
	}
}

func TestTeamsBeaconWorkSwitchIsIdempotentAfterSendFailure(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newFlakyBeaconMessageGraph(t, 1)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessageWithText("beacon-switch-flaky", "beacon switch gpu")

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "beacon switch gpu"); err == nil {
		t.Fatal("first switch send should fail after mutating beacon state")
	}
	st := loadTeamsBeaconState(t)
	if conv := st.Conversations["s001"]; conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" {
		t.Fatalf("switch should persist before failed Teams send: %#v", conv)
	}
	if err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "beacon switch gpu"); err != nil {
		t.Fatalf("retry same Teams switch should be idempotent: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Switched this Work chat to beacon:gpu") {
		t.Fatalf("retry response missing switch result:\n%s", joined)
	}
	st = loadTeamsBeaconState(t)
	if len(st.Conversations) != 1 || len(st.Idempotency) != 1 {
		t.Fatalf("duplicate retry should not create extra state, conversations=%#v idempotency=%#v", st.Conversations, st.Idempotency)
	}
}

func TestTeamsBeaconIdempotencyIsScopedPerWorkChat(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test bridge missing s001")
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	second := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")

	firstMsg := bridgeTestMessageWithText("same-teams-message-id", "beacon switch gpu")
	secondMsg := bridgeTestMessageWithText("same-teams-message-id", "beacon switch gpu")
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, firstMsg, "beacon switch gpu"); err != nil {
		t.Fatalf("first chat switch: %v", err)
	}
	if err := bridge.handleSessionMessage(context.Background(), second.ChatID, secondMsg, "beacon switch gpu"); err != nil {
		t.Fatalf("second chat switch with same Teams message id: %v", err)
	}

	st := loadTeamsBeaconState(t)
	for _, sessionID := range []string{session.ID, second.ID} {
		conv := st.Conversations[sessionID]
		if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" {
			t.Fatalf("conversation %s target = %#v, want beacon gpu", sessionID, conv.Current)
		}
	}
	if len(st.Idempotency) != 2 {
		t.Fatalf("idempotency records = %#v, want separate records per Work chat", st.Idempotency)
	}
}

func TestTeamsBeaconProfileUpdateCreatesNewRevision(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("profile-create", "beacon profile create gpu --provider local"), "beacon profile create gpu --provider local"); err != nil {
		t.Fatalf("create profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("profile-update", "beacon profile update gpu --provider local --isolation exclusive"), "beacon profile update gpu --provider local --isolation exclusive"); err != nil {
		t.Fatalf("update profile: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Updated beacon profile \"gpu\"") || !strings.Contains(joined, "revision: 2") {
		t.Fatalf("profile update response missing revision:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	if got := st.Profiles["gpu"]; got.Revision != 2 || got.IsolationDefault != beacon.IsolationExclusive {
		t.Fatalf("updated profile = %#v, want revision 2 exclusive", got)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("profile-history", "beacon profile history gpu"), "beacon profile history gpu"); err != nil {
		t.Fatalf("profile history: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("profile-rollback", "beacon profile rollback gpu 1"), "beacon profile rollback gpu 1"); err != nil {
		t.Fatalf("profile rollback: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("profile-gc", "beacon profile gc gpu"), "beacon profile gc gpu"); err != nil {
		t.Fatalf("profile gc: %v", err)
	}
	joined = sentPlainJoined(*sent)
	for _, want := range []string{"Beacon profile history", "Rolled back beacon profile \"gpu\"", "revision: 3", "Pruned 2 unreferenced revisions"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("profile revision response missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconProfileCreateStoresAdapterCommands(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(
		context.Background(),
		bridgeTestMessageWithText("profile-create-adapter", "beacon profile create gpu --provider slurm --query-command /opt/cxp/query --submit-command /opt/cxp/submit --cancel-command /opt/cxp/cancel --renew-command /opt/cxp/renew"),
		"beacon profile create gpu --provider slurm --query-command /opt/cxp/query --submit-command /opt/cxp/submit --cancel-command /opt/cxp/cancel --renew-command /opt/cxp/renew",
	); err != nil {
		t.Fatalf("create profile: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "adapter: profile:query,submit,cancel,renew") {
		t.Fatalf("profile create response missing adapter summary:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	got := st.Profiles["gpu"].Adapter
	if got.SlurmQueryCommand != "/opt/cxp/query" || got.SlurmSubmitCommand != "/opt/cxp/submit" || got.SlurmCancelCommand != "/opt/cxp/cancel" || got.SlurmRenewCommand != "/opt/cxp/renew" {
		t.Fatalf("stored adapter = %#v", got)
	}
}

func TestTeamsControlCommandFormatsBeaconProviderAdapterErrors(t *testing.T) {
	msg := controlCommandErrorMessage(beacon.ProviderCommandNotConfiguredError{
		Provider:    beacon.ProviderSlurm,
		Operation:   "query",
		EnvName:     beacon.BeaconSlurmQueryCommandEnv,
		ProfileName: "gpu",
		ProfileFlag: "--query-command",
	})
	for _, want := range []string{
		"Beacon command failed: Slurm provider adapter is not configured.",
		"Summary:",
		"Profile `gpu` does not define `--query-command`",
		"Action needed:",
		"beacon profile update gpu --provider slurm ... --query-command <adapter-script>",
		"does not require a helper reload",
		"Details:",
		"error_code: `BEACON_PROVIDER_ADAPTER_NOT_CONFIGURED`",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatted provider adapter error missing %q:\n%s", want, msg)
		}
	}
}

func TestTeamsBeaconWorkReleaseCancelsAllocationButKeepsProfileBinding(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		conv := st.Conversations["s001"]
		conv.ID = "s001"
		conv.Current = beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}
		st.Conversations["s001"] = conv
		req, _, err := beacon.EnsureAllocationRequest(st, "s001", "turn-1", conv.Current, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.State = beacon.AllocationRunning
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("work-release", "beacon release"), "beacon release"); err != nil {
		t.Fatalf("work release: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Beacon release") || !strings.Contains(joined, "Profile binding is unchanged") {
		t.Fatalf("work release response mismatch:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	var canceled bool
	for _, req := range st.Allocations {
		if req.ConversationID == "s001" && req.State == beacon.AllocationCanceled {
			canceled = true
		}
	}
	if !canceled || st.Conversations["s001"].Current.Profile != "gpu" {
		t.Fatalf("release should cancel allocation and keep binding: allocations=%#v conv=%#v", st.Allocations, st.Conversations["s001"])
	}
}

func TestTeamsBeaconControlReleaseResolvesProfileAllocations(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	var req beacon.AllocationRequest
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles["gpu"] = beacon.Profile{
			Name:              "gpu",
			Provider:          beacon.ProviderLocal,
			ProxyMode:         beacon.ProxyNone,
			IsolationDefault:  beacon.IsolationShared,
			Confirmed:         true,
			ProviderPreviewOK: true,
			DoctorOK:          true,
		}
		var err error
		req, _, err = beacon.EnsureAllocationRequest(st, "s001", "turn-1", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.State = beacon.AllocationRunning
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("control-release-profile", "beacon release gpu"), "beacon release gpu"); err != nil {
		t.Fatalf("control release profile: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Beacon release") || !strings.Contains(joined, "action: mark_canceled") {
		t.Fatalf("control release response mismatch:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	if st.Allocations[req.ID].State != beacon.AllocationCanceled {
		t.Fatalf("allocation state = %s, want canceled", st.Allocations[req.ID].State)
	}
}

func TestTeamsBeaconMachineReleaseAndKillConfirmation(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["gpu-a"] = beacon.Machine{ID: "gpu-a", LeaseID: "lease-gpu", Profile: "gpu", State: "accepting", Jobs: []string{"job-1"}, Chats: []string{"s001"}}
		return nil
	}); err != nil {
		t.Fatalf("seed machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-release", "beacon machine release gpu-a"), "beacon machine release gpu-a"); err != nil {
		t.Fatalf("release machine: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-kill-missing", "beacon machine kill gpu-a"), "beacon machine kill gpu-a"); err != nil {
		t.Fatalf("kill missing confirm: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-kill-confirm", "beacon machine kill gpu-a --confirm KILL-lease-gpu"), "beacon machine kill gpu-a --confirm KILL-lease-gpu"); err != nil {
		t.Fatalf("kill confirmed: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"action: drain", "action: reject_confirmation", "kill confirmation: KILL-lease-gpu", "action: kill_quarantine"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("machine output missing %q:\n%s", want, joined)
		}
	}
	st := loadTeamsBeaconState(t)
	m := st.Machines["gpu-a"]
	if m.State != "kill_quarantine" {
		t.Fatalf("machine state = %q, want kill_quarantine", m.State)
	}
}

func TestTeamsBeaconMachineReleaseDeletesIdleMachine(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["idle-a"] = beacon.Machine{ID: "idle-a", LeaseID: "lease-idle", Profile: "gpu", State: "accepting"}
		return nil
	}); err != nil {
		t.Fatalf("seed idle machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-release-idle", "beacon machine release idle-a"), "beacon machine release idle-a"); err != nil {
		t.Fatalf("release idle machine: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "action: release") {
		t.Fatalf("idle machine release response mismatch:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	if _, ok := st.Machines["idle-a"]; ok {
		t.Fatalf("idle machine should be removed after release: %#v", st.Machines["idle-a"])
	}
}

func TestTeamsBeaconMachineKillRejectsExternalOwnedMachine(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["external-a"] = beacon.Machine{ID: "external-a", LeaseID: "lease-external", Profile: "gpu", State: "accepting", ExternalOwned: true}
		return nil
	}); err != nil {
		t.Fatalf("seed external machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-kill-external", "beacon machine kill external-a --confirm KILL-lease-external"), "beacon machine kill external-a --confirm KILL-lease-external"); err != nil {
		t.Fatalf("kill external machine: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "action: reject_external") || strings.Contains(joined, "action: kill_quarantine") {
		t.Fatalf("external machine kill response mismatch:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	m := st.Machines["external-a"]
	if m.State != "accepting" || !m.ExternalOwned {
		t.Fatalf("external machine should remain unchanged, got %#v", m)
	}
}

func seedTeamsBeaconProfile(t *testing.T, name string) {
	t.Helper()
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles[name] = beacon.Profile{
			Name:              name,
			Provider:          beacon.ProviderSlurm,
			ProxyMode:         beacon.ProxyNone,
			IsolationDefault:  beacon.IsolationShared,
			Slurm:             beacon.SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4},
			Confirmed:         true,
			ProviderPreviewOK: true,
			DoctorOK:          true,
			CreatedAt:         time.Now(),
			UpdatedAt:         time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon profile: %v", err)
	}
}

func writeBeaconProviderFixture(t *testing.T, output string) string {
	t.Helper()
	if os.PathSeparator != '/' {
		t.Skip("POSIX provider fixture script")
	}
	path := filepath.Join(t.TempDir(), "provider-fixture.sh")
	body := "#!/bin/sh\nprintf '%s\\n' " + shellSingleQuoteForBeaconTest(output) + "\n"
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write provider fixture: %v", err)
	}
	return path
}

func waitForBeaconQueuedJob(t *testing.T, store *beacon.Store, machineID string) beacon.JobAttempt {
	t.Helper()
	return waitForBeaconQueuedJobOrDone(t, store, machineID, nil)
}

func waitForBeaconQueuedJobOrDone(t *testing.T, store *beacon.Store, machineID string, done <-chan error) beacon.JobAttempt {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			st, loadErr := store.Load()
			stateText := ""
			if loadErr != nil {
				stateText = fmt.Sprintf("; failed to load beacon state: %v", loadErr)
			} else {
				stateText = fmt.Sprintf("; allocations=%#v machines=%#v jobs=%#v", st.Allocations, st.Machines, st.JobAttempts)
			}
			if err != nil {
				t.Fatalf("beacon turn ended before queuing job on machine %q: %v%s", machineID, err, stateText)
			}
			t.Fatalf("beacon turn ended before queuing job on machine %q%s", machineID, stateText)
		default:
		}
		st, err := store.Load()
		if err != nil {
			t.Fatalf("load beacon state while waiting for job: %v", err)
		}
		for _, job := range st.JobAttempts {
			if job.Phase == beacon.JobQueued && job.Target.MachineID == machineID {
				return job
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("timed out waiting for queued beacon job on machine %q; failed to load beacon state: %v", machineID, err)
	}
	t.Fatalf("timed out waiting for queued beacon job on machine %q; allocations=%#v machines=%#v jobs=%#v", machineID, st.Allocations, st.Machines, st.JobAttempts)
	return beacon.JobAttempt{}
}

func waitForBeaconAllocationProviderJob(t *testing.T, store *beacon.Store, providerJobID string) beacon.AllocationRequest {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		st, err := store.Load()
		if err != nil {
			t.Fatalf("load beacon state while waiting for allocation: %v", err)
		}
		for _, req := range st.Allocations {
			if req.ProviderIdentity.ProviderJobID == providerJobID {
				return req
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for beacon allocation provider job %s", providerJobID)
	return beacon.AllocationRequest{}
}

func shellSingleQuoteForBeaconTest(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func newFlakyBeaconMessageGraph(t *testing.T, failures int) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		if failures > 0 {
			failures--
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, `{"error":{"message":"temporary Teams send failure"}}`)
			return w.Result(), nil
		}
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
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		return w.Result(), nil
	})}
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     client,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}

func loadTeamsBeaconState(t *testing.T) beacon.State {
	t.Helper()
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load beacon state: %v", err)
	}
	return st
}

func healthyTeamsBeaconDoctor() beacon.WorkerDoctor {
	return beacon.WorkerDoctor{
		SharedRootMounted:  true,
		AtomicCreateOK:     true,
		FreeBytesOK:        true,
		FreeInodesOK:       true,
		CodexAvailable:     true,
		CXPAvailable:       true,
		HomeOK:             true,
		TmpWritable:        true,
		ProxyOK:            true,
		AuthPathOK:         true,
		ImageDigestMatch:   true,
		ProtocolOK:         true,
		MembershipProofOK:  true,
		ContainerRuntimeOK: true,
		ModulesOK:          true,
		BindMountsOK:       true,
		ProxyEnvInsideOK:   true,
	}
}

func newBeaconMeetingBridgeTestGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			_, _ = fmt.Fprint(w, `{"subject":"work topic","joinWebUrl":"https://teams.example/join","chatInfo":{"threadId":"work-chat"}}`)
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
