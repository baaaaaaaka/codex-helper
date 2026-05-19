package beacon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSnapshotTurnTargetIsIdempotentAndPreservesProfileSwitch(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{
		"gpu": readyProfile("gpu"),
		"cpu": readyProfile("cpu"),
	}
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu", Signature: "sig-gpu"}},
	}

	first, err := SnapshotTurnTarget(&st, "conv", "turn-1", time.Unix(1, 0))
	if err != nil {
		t.Fatalf("SnapshotTurnTarget first: %v", err)
	}
	duplicate, err := SnapshotTurnTarget(&st, "conv", "turn-1", time.Unix(2, 0))
	if err != nil {
		t.Fatalf("SnapshotTurnTarget duplicate: %v", err)
	}
	if first.Snapshot.Profile != "gpu" || duplicate.Snapshot.Profile != "gpu" || len(st.Conversations["conv"].Queued) != 1 {
		t.Fatalf("snapshot should be stable and idempotent, first=%#v duplicate=%#v queued=%#v", first, duplicate, st.Conversations["conv"].Queued)
	}

	res, err := SwitchProfile(&st, SwitchInput{ConversationID: "conv", ProfileName: "cpu", Signature: "sig-cpu", HasQueuedOrRunning: true, SignatureCompatible: true}, nil)
	if err != nil {
		t.Fatalf("SwitchProfile: %v", err)
	}
	if res.Action != "pending" {
		t.Fatalf("switch with queued turn should be pending, got %#v", res)
	}
	second, err := SnapshotTurnTarget(&st, "conv", "turn-2", time.Unix(3, 0))
	if err != nil {
		t.Fatalf("SnapshotTurnTarget second: %v", err)
	}
	if second.Snapshot.Profile != "cpu" {
		t.Fatalf("future turn should snapshot pending cpu profile, got %#v", second)
	}
	snap, ok := TargetSnapshotForTurn(st, "turn-1")
	if !ok || snap.Profile != "gpu" {
		t.Fatalf("turn map should preserve first profile, snap=%#v ok=%v", snap, ok)
	}
}

func TestPlanTurnExecutionCreatesAllocationAndNeverFallsBackToLocal(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	st.Profiles["gpu"] = copyProfile(st.Profiles["gpu"], func(p *Profile) {
		p.Provider = ProviderSlurm
		p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4}
	})
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu", Signature: "sig-a"}},
	}
	plan, err := PlanTurnExecution(&st, "conv", "turn-1", time.Unix(10, 0))
	if err != nil {
		t.Fatalf("PlanTurnExecution: %v", err)
	}
	if plan.Action != TurnWaitAllocation || plan.AllocationRequestID == "" || strings.Contains(string(plan.Action), "local") {
		t.Fatalf("explicit beacon should wait for managed allocation without local fallback, got %#v", plan)
	}
	req := st.Allocations[plan.AllocationRequestID]
	if req.Provider != ProviderSlurm || req.State != AllocationRequestPersisted || req.DeterministicName == "" {
		t.Fatalf("allocation request not persisted correctly: %#v", req)
	}

	plan, err = PlanTurnExecution(&st, "conv", "turn-1", time.Unix(11, 0))
	if err != nil {
		t.Fatalf("PlanTurnExecution duplicate: %v", err)
	}
	if plan.AllocationRequestID != req.ID || len(st.Allocations) != 1 || len(st.Conversations["conv"].Queued) != 1 {
		t.Fatalf("duplicate plan should reuse allocation and queued snapshot, plan=%#v allocations=%#v queued=%#v", plan, st.Allocations, st.Conversations["conv"].Queued)
	}
}

func TestPlanTurnExecutionRejectsDraftBeaconProfileWithoutLocalFallback(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"draft": {Name: "draft", Provider: ProviderSlurm, ProxyMode: ProxyNone}}
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "draft"}},
	}
	_, err := PlanTurnExecution(&st, "conv", "turn-1", time.Unix(1, 0))
	if err == nil || !strings.Contains(err.Error(), "is not ready") {
		t.Fatalf("draft beacon profile should reject without local fallback, err=%v", err)
	}
	if len(st.Allocations) != 0 {
		t.Fatalf("draft profile should not create allocation, allocations=%#v", st.Allocations)
	}
}

func TestPlanTurnExecutionLocalAndReadyBeaconLease(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	st.Conversations = map[string]Conversation{
		"local": {ID: "local", Current: TargetSnapshot{Target: TargetLocal}},
		"ready": {ID: "ready", Current: TargetSnapshot{
			Target:        TargetBeacon,
			Profile:       "gpu",
			MachineID:     "machine-1",
			LeaseID:       "lease-1",
			ProviderJobID: "slurm-1",
		}},
	}
	st.Machines = map[string]Machine{
		"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(LeaseAccepting), ProviderState: ProviderJobRunning, Doctor: healthyWorkerDoctor(), LastHeartbeat: time.Unix(1, 0)},
	}
	local, err := PlanTurnExecution(&st, "local", "turn-local", time.Unix(1, 0))
	if err != nil || local.Action != TurnRunLocal {
		t.Fatalf("local plan = %#v err=%v", local, err)
	}
	ready, err := PlanTurnExecution(&st, "ready", "turn-ready", time.Unix(2, 0))
	if err != nil || ready.Action != TurnRunBeacon || ready.MachineID != "machine-1" {
		t.Fatalf("ready beacon plan = %#v err=%v", ready, err)
	}
}

func TestPlanTurnExecutionLocalProviderReadyMachineDoesNotRequireProviderJob(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"local": readyProfile("local")}
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{
			Target:    TargetBeacon,
			Profile:   "local",
			MachineID: "machine-local",
			LeaseID:   "lease-local",
		}},
	}
	st.Machines = map[string]Machine{
		"machine-local": {
			ID:            "machine-local",
			LeaseID:       "lease-local",
			Profile:       "local",
			State:         string(LeaseAccepting),
			Doctor:        healthyWorkerDoctor(),
			LastHeartbeat: time.Unix(1, 0),
		},
	}

	plan, err := PlanTurnExecution(&st, "conv", "turn-local-provider", time.Unix(2, 0))
	if err != nil {
		t.Fatalf("PlanTurnExecution: %v", err)
	}
	if plan.Action != TurnRunBeacon || plan.ProviderJobID != "" || plan.MachineID != "machine-local" || plan.LeaseID != "lease-local" {
		t.Fatalf("local provider ready machine should not require provider job id, plan=%#v", plan)
	}
}

func TestPlanTurnExecutionRequiresAcceptingMachineNotJustSnapshotIDs(t *testing.T) {
	baseConversation := Conversation{ID: "conv", Current: TargetSnapshot{
		Target:        TargetBeacon,
		Profile:       "gpu",
		MachineID:     "machine-1",
		LeaseID:       "lease-1",
		ProviderJobID: "slurm-1",
	}}
	cases := map[string]map[string]Machine{
		"missing machine": {},
		"starting lease": {
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(LeaseStarting)},
		},
		"wrong provider job": {
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-other", Profile: "gpu", State: string(LeaseAccepting)},
		},
	}
	for name, machines := range cases {
		st := State{
			Profiles:      map[string]Profile{"gpu": readyProfile("gpu")},
			Conversations: map[string]Conversation{"conv": baseConversation},
			Machines:      machines,
		}
		plan, err := PlanTurnExecution(&st, "conv", "turn-"+name, time.Unix(1, 0))
		if err != nil {
			t.Fatalf("%s: PlanTurnExecution: %v", name, err)
		}
		if plan.Action == TurnRunBeacon {
			t.Fatalf("%s: stale snapshot should not run beacon without accepting matching machine: %#v", name, plan)
		}
		if plan.Action != TurnWaitAllocation || plan.AllocationRequestID == "" {
			t.Fatalf("%s: stale snapshot should create managed allocation, got %#v", name, plan)
		}
	}
}

func TestAllocationRequestAndJobNameAreDeterministic(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	snap := TargetSnapshot{Target: TargetBeacon, Profile: "gpu", Signature: "sig-a"}
	first, created, err := EnsureAllocationRequest(&st, "conv", "turn-1", snap, time.Unix(1, 0))
	if err != nil || !created {
		t.Fatalf("EnsureAllocationRequest first = %#v created=%v err=%v", first, created, err)
	}
	second, created, err := EnsureAllocationRequest(&st, "conv", "turn-1", snap, time.Unix(2, 0))
	if err != nil || created {
		t.Fatalf("EnsureAllocationRequest duplicate = %#v created=%v err=%v", second, created, err)
	}
	if first.ID != second.ID || first.DeterministicName != second.DeterministicName {
		t.Fatalf("request id/job name should be deterministic, first=%#v second=%#v", first, second)
	}
	if got := DeterministicJobName("req spaces/../../" + strings.Repeat("x", 120)); len(got) > 63 || strings.ContainsAny(got, " /") {
		t.Fatalf("job name should be scheduler-safe and bounded, got %q len=%d", got, len(got))
	}
}

func TestAllocationSubmitCrashWindowUsesSchedulerDiscoveryBeforeResubmit(t *testing.T) {
	req := AllocationRequest{ID: "req-1"}
	if got := DecideAllocationSubmit(req, SchedulerQueryResult{ProviderJobID: "slurm-123"}); got != AllocationSubmitAdopt {
		t.Fatalf("existing provider job should be adopted, got %s", got)
	}
	if got := DecideAllocationSubmit(AllocationRequest{ID: "req-2", SubmitAttempts: 1}, SchedulerQueryResult{}); got != AllocationSubmitWait {
		t.Fatalf("uncertain empty query after submit should wait, got %s", got)
	}
	if got := DecideAllocationSubmit(AllocationRequest{ID: "req-2", SubmitAttempts: 1}, SchedulerQueryResult{DurableNegative: true}); got != AllocationSubmitNow {
		t.Fatalf("durable negative query may resubmit, got %s", got)
	}
	for _, query := range []SchedulerQueryResult{{QueryError: true}, {MultipleMatches: true}} {
		got := DecideAllocationSubmit(AllocationRequest{ID: "req-3", SubmitAttempts: 1}, query)
		if got == AllocationSubmitNow {
			t.Fatalf("failed or ambiguous query must not blindly resubmit, query=%#v got=%s", query, got)
		}
	}
}

func TestReconcileAllocationSubmitUsesAdapterWithoutDuplicateSubmit(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	adapter := &fakeAllocationAdapter{query: SchedulerQueryResult{ProviderJobID: "slurm-123", RawState: "PD", Reason: "Resources"}}
	updated, action, err := ReconcileAllocationSubmit(context.Background(), &st, req.ID, adapter, time.Unix(2, 0))
	if err != nil {
		t.Fatalf("ReconcileAllocationSubmit adopt: %v", err)
	}
	if action != AllocationSubmitAdopt || updated.ProviderIdentity.ProviderJobID != "slurm-123" || adapter.submits != 0 {
		t.Fatalf("existing provider job should be adopted without submit, action=%s req=%#v submits=%d", action, updated, adapter.submits)
	}

	st.Allocations[req.ID] = copyAllocation(updated, func(in *AllocationRequest) {
		in.ProviderIdentity.ProviderJobID = ""
		in.SubmitAttempts = 1
	})
	adapter = &fakeAllocationAdapter{}
	updated, action, err = ReconcileAllocationSubmit(context.Background(), &st, req.ID, adapter, time.Unix(3, 0))
	if err != nil {
		t.Fatalf("ReconcileAllocationSubmit wait: %v", err)
	}
	if action != AllocationSubmitWait || adapter.submits != 0 || updated.ProviderIdentity.ProviderJobID != "" {
		t.Fatalf("uncertain empty scheduler query after submit must wait, action=%s req=%#v submits=%d", action, updated, adapter.submits)
	}

	adapter = &fakeAllocationAdapter{query: SchedulerQueryResult{DurableNegative: true}, submit: SchedulerQueryResult{ProviderJobID: "slurm-456", RawState: "PD"}}
	updated, action, err = ReconcileAllocationSubmit(context.Background(), &st, req.ID, adapter, time.Unix(4, 0))
	if err != nil {
		t.Fatalf("ReconcileAllocationSubmit resubmit: %v", err)
	}
	if action != AllocationSubmitNow || adapter.submits != 1 || updated.ProviderIdentity.ProviderJobID != "slurm-456" || updated.SubmitAttempts != 2 {
		t.Fatalf("durable negative should permit one submit, action=%s req=%#v submits=%d", action, updated, adapter.submits)
	}
}

func TestProviderRunDoesNotMeanWorkerAccepting(t *testing.T) {
	base := LeaseReadiness{
		ProviderState:       ProviderJobRunning,
		WorkerHeartbeat:     true,
		Doctor:              healthyWorkerDoctor(),
		MembershipProofOK:   true,
		SignatureMatch:      true,
		ProtocolCompatible:  true,
		ResourceAvailable:   true,
		RemainingTTLSeconds: 60,
		RequiredTTLSeconds:  30,
	}
	if !LeaseCanAccept(base) {
		t.Fatal("provider RUN plus worker heartbeat, doctor, membership, signature, protocol, resource, and TTL should accept")
	}
	cases := map[string]LeaseReadiness{
		"pending provider":   copyReadiness(base, func(in *LeaseReadiness) { in.ProviderState = ProviderJobPending }),
		"missing heartbeat":  copyReadiness(base, func(in *LeaseReadiness) { in.WorkerHeartbeat = false }),
		"doctor failed":      copyReadiness(base, func(in *LeaseReadiness) { in.Doctor.CodexAvailable = false }),
		"outside allocation": copyReadiness(base, func(in *LeaseReadiness) { in.MembershipProofOK = false }),
		"wrong signature":    copyReadiness(base, func(in *LeaseReadiness) { in.SignatureMatch = false }),
		"ttl too short":      copyReadiness(base, func(in *LeaseReadiness) { in.RemainingTTLSeconds = 29 }),
	}
	for name, in := range cases {
		if LeaseCanAccept(in) {
			t.Fatalf("%s must not become accepting: %#v", name, in)
		}
	}
}

func TestWorkerDoctorReportsEveryBootstrapBlocker(t *testing.T) {
	base := healthyWorkerDoctor()
	if blockers := WorkerDoctorBlockers(base); len(blockers) != 0 {
		t.Fatalf("healthy doctor should pass, blockers=%v", blockers)
	}
	cases := map[string]WorkerDoctor{
		"missing shared root": copyDoctor(base, func(in *WorkerDoctor) { in.SharedRootMounted = false }),
		"missing codex":       copyDoctor(base, func(in *WorkerDoctor) { in.CodexAvailable = false }),
		"missing cxp":         copyDoctor(base, func(in *WorkerDoctor) { in.CXPAvailable = false }),
		"bad proxy":           copyDoctor(base, func(in *WorkerDoctor) { in.ProxyOK = false }),
		"wrong image":         copyDoctor(base, func(in *WorkerDoctor) { in.ImageDigestMatch = false }),
		"login node worker":   copyDoctor(base, func(in *WorkerDoctor) { in.MembershipProofOK = false }),
	}
	for name, doctor := range cases {
		if WorkerDoctorPassed(doctor) {
			t.Fatalf("%s should block accepting: %#v", name, doctor)
		}
	}
}

func TestRawSlurmAndLSFStatesPreserveReasonAndConservativeAction(t *testing.T) {
	tests := []struct {
		provider Provider
		raw      string
		started  bool
		want     ReconcileAction
	}{
		{provider: ProviderSlurm, raw: "PD", want: ReconcilePending},
		{provider: ProviderSlurm, raw: "R", want: ReconcileRunning},
		{provider: ProviderSlurm, raw: "CG", started: true, want: ReconcileFinalizing},
		{provider: ProviderSlurm, raw: "CA", started: true, want: ReconcileQuarantine},
		{provider: ProviderSlurm, raw: "F", want: ReconcileLost},
		{provider: ProviderSlurm, raw: "OOM", started: true, want: ReconcileQuarantine},
		{provider: ProviderLSF, raw: "PEND", want: ReconcilePending},
		{provider: ProviderLSF, raw: "RUN", want: ReconcileRunning},
		{provider: ProviderLSF, raw: "DONE", started: true, want: ReconcileFinalizing},
		{provider: ProviderLSF, raw: "SSUSP", started: true, want: ReconcileSuspended},
		{provider: ProviderLSF, raw: "UNKWN", started: true, want: ReconcileQuarantine},
	}
	for _, tt := range tests {
		got := ProjectRawProviderState(tt.provider, tt.raw, "GPU unavailable", tt.started, false)
		if got.Action != tt.want || got.RawState != tt.raw || got.Reason != "GPU unavailable" {
			t.Fatalf("%s %s: got %#v want action=%s with raw reason preserved", tt.provider, tt.raw, got, tt.want)
		}
	}
	pendAfterRun := ProjectRawProviderState(ProviderLSF, "PEND", "preempted and requeued", true, true)
	if pendAfterRun.Action != ReconcileQuarantine {
		t.Fatalf("LSF PEND-after-RUN after start must not become claimable pending, got %#v", pendAfterRun)
	}
	pendBeforeStart := ProjectRawProviderState(ProviderLSF, "PEND", "preempted before start", false, true)
	if pendBeforeStart.Action != ReconcileSuspended {
		t.Fatalf("LSF PEND-after-RUN before start should pause, got %#v", pendBeforeStart)
	}
}

func TestRecoveryNeverReplaysAfterCodexMayHaveStarted(t *testing.T) {
	tests := []struct {
		name  string
		phase JobPhase
		alive bool
		term  TerminalIntegrity
		want  RecoveryAction
	}{
		{name: "queued", phase: JobQueued, want: RecoveryRequeue},
		{name: "claimed", phase: JobClaimed, want: RecoveryRequeue},
		{name: "start intent", phase: JobStartIntent, want: RecoveryAmbiguous},
		{name: "started", phase: JobStarted, want: RecoveryAmbiguous},
		{name: "live worker", phase: JobStarted, alive: true, want: RecoveryMonitor},
		{name: "valid terminal", phase: JobTerminal, term: TerminalValid, want: RecoveryComplete},
		{name: "bad terminal", phase: JobTerminal, term: TerminalHMACBad, want: RecoveryQuarantine},
	}
	for _, tt := range tests {
		if got := RecoverJob(tt.phase, tt.alive, tt.term); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
	windows := []struct {
		name string
		in   LaunchWindow
		want RecoveryAction
	}{
		{name: "unsynced claim", in: LaunchWindow{}, want: RecoveryQuarantine},
		{name: "claim before start intent", in: LaunchWindow{ClaimSynced: true}, want: RecoveryRequeue},
		{name: "start intent ambiguous", in: LaunchWindow{ClaimSynced: true, StartIntentSynced: true}, want: RecoveryAmbiguous},
		{name: "durable no exec proof", in: LaunchWindow{ClaimSynced: true, StartIntentSynced: true, WorkerProvesNoExec: true}, want: RecoveryRequeue},
		{name: "process start ack", in: LaunchWindow{ClaimSynced: true, StartIntentSynced: true, ProcessStartAckSynced: true}, want: RecoveryAmbiguous},
	}
	for _, tt := range windows {
		if got := RecoverLaunchWindow(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

func TestRuntimeFailureClassificationProtectsStartedWork(t *testing.T) {
	tests := []struct {
		name string
		in   RuntimeFailureInput
		want RuntimeFailureDecision
	}{
		{name: "allocation denied", in: RuntimeFailureInput{Kind: RuntimeFailureAllocationDenied}, want: RuntimeFailureDecision{Action: "fail_request", Retry: "after-edit"}},
		{name: "temporary scheduler", in: RuntimeFailureInput{Kind: RuntimeFailureTemporaryScheduler}, want: RuntimeFailureDecision{Action: "retry_allocation", Retry: "auto"}},
		{name: "oom before start", in: RuntimeFailureInput{Kind: RuntimeFailureOOM}, want: RuntimeFailureDecision{Action: "requeue", Retry: "auto"}},
		{name: "oom after start", in: RuntimeFailureInput{Kind: RuntimeFailureOOM, AfterProcessStart: true}, want: RuntimeFailureDecision{Action: "ambiguous", Retry: "fork-required"}},
		{name: "disk full before start", in: RuntimeFailureInput{Kind: RuntimeFailureDiskFull}, want: RuntimeFailureDecision{Action: "block_claim", Retry: "after-cleanup"}},
		{name: "disk full after start", in: RuntimeFailureInput{Kind: RuntimeFailureDiskFull, AfterProcessStart: true}, want: RuntimeFailureDecision{Action: "needs_attention", Retry: "unsafe"}},
		{name: "terminal wins", in: RuntimeFailureInput{Kind: RuntimeFailureWalltime, AfterProcessStart: true, ValidTerminal: true}, want: RuntimeFailureDecision{Action: "deliver_terminal", Retry: "no"}},
	}
	for _, tt := range tests {
		if got := ClassifyRuntimeFailure(tt.in); got != tt.want {
			t.Fatalf("%s: got %#v want %#v", tt.name, got, tt.want)
		}
	}
}

func TestRenderStatusIncludesManagedAllocationFields(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}},
	}
	plan, err := PlanTurnExecution(&st, "conv", "turn-1", time.Unix(1, 0))
	if err != nil {
		t.Fatalf("PlanTurnExecution: %v", err)
	}
	if _, err := RecordAllocationSubmit(&st, plan.AllocationRequestID, "slurm-123", "PD", "Resources", time.Unix(2, 0)); err != nil {
		t.Fatalf("RecordAllocationSubmit: %v", err)
	}
	status := RenderStatus(st, "conv")
	for _, want := range []string{
		"current_target=beacon",
		"profile=gpu",
		"turn_snapshot=beacon:gpu",
		"allocation=" + plan.AllocationRequestID,
		"allocation_state=submitted",
		"provider_job=slurm-123",
		"provider_state=PD",
		"provider_reason=Resources",
	} {
		if !strings.Contains(status, want) {
			t.Fatalf("status %q missing %q", status, want)
		}
	}
}

func TestRenderStatusKeepsLatestAllocationAfterTurnSnapshotCleanup(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}},
	}
	plan, err := PlanTurnExecution(&st, "conv", "turn-1", time.Unix(1, 0))
	if err != nil {
		t.Fatalf("PlanTurnExecution: %v", err)
	}
	req := st.Allocations[plan.AllocationRequestID]
	req.State = AllocationNeedsAttention
	req.ProviderReason = "worker execution not connected"
	req.UpdatedAt = time.Unix(2, 0)
	st.Allocations[req.ID] = req
	RemoveTurnSnapshot(&st, "conv", "turn-1", time.Unix(3, 0))

	status := RenderStatus(st, "conv")
	for _, want := range []string{
		"turn_snapshot=",
		"allocation=" + plan.AllocationRequestID,
		"allocation_state=needs_attention",
		"provider_reason=worker execution not connected",
	} {
		if !strings.Contains(status, want) {
			t.Fatalf("status %q missing %q", status, want)
		}
	}
}

func TestRemoveTurnSnapshotPromotesPendingAfterCurrentTargetDrains(t *testing.T) {
	var st State
	st.Conversations = map[string]Conversation{
		"conv": {
			ID:      "conv",
			Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProfileRevision: 1},
			Pending: &TargetSnapshot{Target: TargetBeacon, Profile: "cpu", ProfileRevision: 2},
			Queued: []QueuedTurn{
				{ID: "old-1", Snapshot: TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProfileRevision: 1}},
				{ID: "old-2", Snapshot: TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProfileRevision: 1}},
				{ID: "new-1", Snapshot: TargetSnapshot{Target: TargetBeacon, Profile: "cpu", ProfileRevision: 2}},
			},
		},
	}

	RemoveTurnSnapshot(&st, "conv", "old-1", time.Unix(1, 0))
	if conv := st.Conversations["conv"]; conv.Current.Profile != "gpu" || conv.Pending == nil {
		t.Fatalf("pending target should wait for all current-target turns to drain: %#v", conv)
	}
	RemoveTurnSnapshot(&st, "conv", "old-2", time.Unix(2, 0))
	conv := st.Conversations["conv"]
	if conv.Current.Profile != "cpu" || conv.Current.ProfileRevision != 2 || conv.Pending != nil {
		t.Fatalf("pending target should promote after current-target turns drain: %#v", conv)
	}
	if len(conv.Queued) != 1 || conv.Queued[0].ID != "new-1" {
		t.Fatalf("future queued turn should be preserved during promotion: %#v", conv.Queued)
	}
}

func TestBeaconErrorMessageContainsActionableFields(t *testing.T) {
	msg := RenderBeaconError(BeaconErrorContext{
		Phase:          "bootstrap",
		Target:         "beacon:gpu",
		ProviderJobID:  "slurm-123",
		ProviderState:  "R",
		ProviderReason: "missing shared root",
		ConversationID: "conv",
		JobID:          "job-1",
		Retry:          "unsafe",
		Next:           "beacon status --job job-1",
	})
	for _, want := range []string{"phase: `bootstrap`", "target: `beacon:gpu`", "provider_job: `slurm-123`", "provider_state: `R`", "provider_reason: `missing shared root`", "conversation: `conv`", "job: `job-1`", "retry: `unsafe`", "next: `beacon status --job job-1`"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q missing %q", msg, want)
		}
	}
}

func TestAcceptWorkerTerminalFencesByLeaseClaimAndProvider(t *testing.T) {
	st := State{JobAttempts: map[string]JobAttempt{
		"job-1": {
			ID:         "job-1",
			RequestID:  "req-1",
			TurnID:     "turn-1",
			WorkerID:   "worker-a",
			LeaseID:    "lease-a",
			LeaseEpoch: 9,
			ClaimEpoch: 3,
			Phase:      JobStarted,
			ProviderIdentity: ProviderIdentity{
				ProviderJobID: "slurm-1",
			},
		},
	}}
	envelope := WorkerTerminalEnvelope{
		JobID:      "job-1",
		RequestID:  "req-1",
		TurnID:     "turn-1",
		WorkerID:   "worker-a",
		LeaseID:    "lease-a",
		LeaseEpoch: 9,
		ClaimEpoch: 3,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: "slurm-1",
		},
		Payload: []byte(`{"ok":true}`),
	}

	first, err := AcceptWorkerTerminal(&st, envelope, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("AcceptWorkerTerminal first: %v", err)
	}
	if first.Integrity != TerminalValid || !first.OutboxQueued || st.JobAttempts["job-1"].Phase != JobTerminal {
		t.Fatalf("first terminal = %#v attempt=%#v", first, st.JobAttempts["job-1"])
	}
	duplicate, err := AcceptWorkerTerminal(&st, envelope, time.Unix(2, 0))
	if err != nil {
		t.Fatalf("AcceptWorkerTerminal duplicate: %v", err)
	}
	if duplicate.Integrity != TerminalDuplicateSame || duplicate.OutboxQueued {
		t.Fatalf("duplicate terminal = %#v", duplicate)
	}

	conflict := envelope
	conflict.Payload = []byte(`{"ok":false}`)
	rejected, err := AcceptWorkerTerminal(&st, conflict, time.Unix(3, 0))
	if err != nil {
		t.Fatalf("AcceptWorkerTerminal conflict: %v", err)
	}
	if rejected.Integrity != TerminalDuplicateConflict || rejected.Result.Action != "quarantine" || st.JobAttempts["job-1"].Phase != JobQuarantined {
		t.Fatalf("conflicting terminal should quarantine, decision=%#v attempt=%#v", rejected, st.JobAttempts["job-1"])
	}
}

func TestBeaconJobQueueClaimAndTerminalLifecycle(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	st.Machines = map[string]Machine{
		"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(LeaseAccepting)},
	}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", MachineID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	job, created, err := EnqueueJobAttempt(&st, req.ID, st.Machines["machine-1"], JobPayload{Prompt: "hello", WorkingDir: "/work", CodexThreadID: "thread-1"}, time.Unix(2, 0))
	if err != nil || !created {
		t.Fatalf("EnqueueJobAttempt: job=%#v created=%v err=%v", job, created, err)
	}
	duplicate, created, err := EnqueueJobAttempt(&st, req.ID, st.Machines["machine-1"], JobPayload{Prompt: "different"}, time.Unix(3, 0))
	if err != nil || created || duplicate.ID != job.ID || duplicate.Payload.Prompt != "hello" {
		t.Fatalf("duplicate enqueue should be idempotent, job=%#v created=%v err=%v", duplicate, created, err)
	}
	claimed, ok, err := ClaimNextJobForMachine(&st, "machine-1", "worker-1", time.Unix(4, 0))
	if err != nil || !ok {
		t.Fatalf("ClaimNextJobForMachine: job=%#v ok=%v err=%v", claimed, ok, err)
	}
	if claimed.Phase != JobClaimed || claimed.WorkerID != "worker-1" || claimed.ClaimEpoch != 1 {
		t.Fatalf("claimed job = %#v", claimed)
	}
	started, err := MarkJobStarted(&st, claimed.ID, time.Unix(5, 0))
	if err != nil || started.Phase != JobStarted {
		t.Fatalf("MarkJobStarted: job=%#v err=%v", started, err)
	}
	decision, err := AcceptWorkerTerminal(&st, WorkerTerminalEnvelope{
		JobID:      started.ID,
		RequestID:  started.RequestID,
		TurnID:     started.TurnID,
		WorkerID:   started.WorkerID,
		LeaseID:    started.LeaseID,
		ClaimEpoch: started.ClaimEpoch,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: started.ProviderIdentity.ProviderJobID,
		},
		Payload: []byte(`{"text":"done"}`),
	}, time.Unix(6, 0))
	if err != nil || decision.Integrity != TerminalValid {
		t.Fatalf("AcceptWorkerTerminal: decision=%#v err=%v", decision, err)
	}
	if st.Terminals[started.ID].Payload != `{"text":"done"}` {
		t.Fatalf("terminal payload not stored: %#v", st.Terminals[started.ID])
	}
	if jobs := st.Machines["machine-1"].Jobs; len(jobs) != 0 {
		t.Fatalf("completed terminal should remove active job from machine, jobs=%#v", jobs)
	}
}

func TestWorkerRegistrationHeartbeatAndRecoveryLifecycle(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	req.ProviderIdentity.ProviderJobID = "slurm-1"
	req.State = AllocationSubmitted
	st.Allocations[req.ID] = req
	machine, err := RegisterWorkerMachineForAllocation(&st, req.ID, WorkerRegistrationInput{
		MachineID:       "machine-1",
		LeaseID:         "lease-1",
		ProviderJobID:   "slurm-1",
		WorkerID:        "worker-1",
		Host:            "worker-host",
		State:           LeaseAccepting,
		Doctor:          healthyWorkerDoctor(),
		MembershipProof: "slurm:slurm-1",
	}, time.Unix(2, 0))
	if err != nil {
		t.Fatalf("RegisterWorkerMachineForAllocation: %v", err)
	}
	if machine.State != string(LeaseAccepting) || machine.LastHeartbeat.IsZero() || st.Allocations[req.ID].State != AllocationRunning {
		t.Fatalf("registered machine/allocation = %#v %#v", machine, st.Allocations[req.ID])
	}
	if _, err := RecordWorkerHeartbeat(&st, "machine-1", "worker-1", time.Unix(3, 0)); err != nil {
		t.Fatalf("RecordWorkerHeartbeat: %v", err)
	}
	if got := st.Machines["machine-1"].LastHeartbeat; !got.Equal(time.Unix(3, 0)) {
		t.Fatalf("heartbeat = %s", got)
	}
	st.Machines["machine-1"] = copyMachine(st.Machines["machine-1"], func(in *Machine) {
		in.LastHeartbeat = time.Unix(1, 0)
	})
	drained := DrainStaleWorkerMachines(&st, time.Minute, time.Unix(4*60, 0))
	if len(drained) != 1 || st.Machines["machine-1"].State != string(LeaseDraining) {
		t.Fatalf("DrainStaleWorkerMachines = %#v machine=%#v", drained, st.Machines["machine-1"])
	}
	st.JobAttempts = map[string]JobAttempt{
		"claimed": {ID: "claimed", RequestID: req.ID, Phase: JobClaimed, WorkerID: "worker-1", UpdatedAt: time.Unix(1, 0)},
		"started": {ID: "started", RequestID: req.ID, Phase: JobStarted, WorkerID: "worker-1", UpdatedAt: time.Unix(1, 0)},
	}
	recovered := RecoverStaleJobAttempts(&st, time.Minute, time.Unix(4*60, 0))
	if len(recovered) != 2 || st.JobAttempts["claimed"].Phase != JobQueued || st.JobAttempts["started"].Phase != JobAmbiguous {
		t.Fatalf("RecoverStaleJobAttempts = %#v jobs=%#v", recovered, st.JobAttempts)
	}
	bad, err := RegisterWorkerMachineForAllocation(&st, req.ID, WorkerRegistrationInput{
		MachineID:       "machine-bad",
		LeaseID:         "lease-bad",
		ProviderJobID:   "slurm-1",
		State:           LeaseAccepting,
		Doctor:          copyDoctor(healthyWorkerDoctor(), func(in *WorkerDoctor) { in.CodexAvailable = false }),
		MembershipProof: "slurm:slurm-1",
	}, time.Unix(5, 0))
	if err != nil {
		t.Fatalf("bad doctor registration should persist needs_attention, err=%v", err)
	}
	if bad.State != string(LeaseNeedsAttention) || len(bad.DoctorBlockers) == 0 {
		t.Fatalf("bad doctor should block accepting, machine=%#v", bad)
	}
}

func TestWorkerRegistrationRejectsCanceledAllocationAndWrongProviderJob(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	req.ProviderIdentity.ProviderJobID = "slurm-1"
	req.State = AllocationSubmitted
	st.Allocations[req.ID] = req

	if _, err := RegisterWorkerMachineForAllocation(&st, req.ID, WorkerRegistrationInput{
		MachineID:       "machine-wrong",
		LeaseID:         "lease-wrong",
		ProviderJobID:   "slurm-other",
		State:           LeaseAccepting,
		Doctor:          healthyWorkerDoctor(),
		MembershipProof: "slurm:slurm-other",
	}, time.Unix(2, 0)); err == nil || !strings.Contains(err.Error(), "bound to provider job") {
		t.Fatalf("wrong provider worker should be rejected, err=%v", err)
	}
	if st.Allocations[req.ID].ProviderIdentity.ProviderJobID != "slurm-1" || len(st.Machines) != 0 {
		t.Fatalf("wrong provider registration mutated state: allocations=%#v machines=%#v", st.Allocations, st.Machines)
	}

	CancelTurn(&st, "conv", "turn-1", "user cancel", time.Unix(3, 0))
	if _, err := RegisterWorkerMachineForAllocation(&st, req.ID, WorkerRegistrationInput{
		MachineID:       "machine-late",
		LeaseID:         "lease-late",
		ProviderJobID:   "slurm-1",
		State:           LeaseAccepting,
		Doctor:          healthyWorkerDoctor(),
		MembershipProof: "slurm:slurm-1",
	}, time.Unix(4, 0)); err == nil || !strings.Contains(err.Error(), "not accepting worker registration") {
		t.Fatalf("late worker registration after cancel should be rejected, err=%v", err)
	}
	if st.Allocations[req.ID].State != AllocationCanceled || len(st.Machines) != 0 {
		t.Fatalf("late worker registration resurrected canceled allocation: allocation=%#v machines=%#v", st.Allocations[req.ID], st.Machines)
	}
}

func TestWorkerRegistrationRejectsStaleReplacementProviderJob(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
		p.Provider = ProviderSlurm
		p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4}
	})}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	req.ProviderIdentity.ProviderJobID = "slurm-old"
	req.Target.ProviderJobID = "slurm-old"
	req.State = AllocationPending
	st.Allocations[req.ID] = req
	req, err = UpdateAllocationProjection(&st, req.ID, ProjectRawProviderState(ProviderSlurm, "F", "node failure", false, true), time.Unix(2, 0))
	if err != nil {
		t.Fatalf("UpdateAllocationProjection: %v", err)
	}
	if req.State != AllocationRequestPersisted || req.ReplacementID != "slurm-old" {
		t.Fatalf("pre-start provider loss should prepare replacement, req=%#v", req)
	}
	if _, err := RegisterWorkerMachineForAllocation(&st, req.ID, WorkerRegistrationInput{
		MachineID:       "machine-stale",
		LeaseID:         "lease-stale",
		ProviderJobID:   "slurm-old",
		State:           LeaseAccepting,
		Doctor:          healthyWorkerDoctor(),
		MembershipProof: "slurm:slurm-old",
	}, time.Unix(3, 0)); err == nil || !strings.Contains(err.Error(), "already replaced stale provider job") {
		t.Fatalf("stale replaced worker should be rejected, err=%v", err)
	}
	if st.Allocations[req.ID].State != AllocationRequestPersisted || st.Allocations[req.ID].ProviderIdentity.ProviderJobID != "" || len(st.Machines) != 0 {
		t.Fatalf("stale replaced worker mutated replacement allocation: allocation=%#v machines=%#v", st.Allocations[req.ID], st.Machines)
	}

	if _, err := RegisterWorkerMachineForAllocation(&st, req.ID, WorkerRegistrationInput{
		MachineID:       "machine-new",
		LeaseID:         "lease-new",
		ProviderJobID:   "slurm-new",
		State:           LeaseAccepting,
		Doctor:          healthyWorkerDoctor(),
		MembershipProof: "slurm:slurm-new",
	}, time.Unix(4, 0)); err != nil {
		t.Fatalf("new replacement worker should be accepted: %v", err)
	}
	if st.Allocations[req.ID].ProviderIdentity.ProviderJobID != "slurm-new" || st.Allocations[req.ID].State != AllocationRunning {
		t.Fatalf("new replacement worker did not bind allocation: %#v", st.Allocations[req.ID])
	}
}

func TestAcceptWorkerTerminalRejectsStaleWorkerMetadata(t *testing.T) {
	baseAttempt := JobAttempt{
		ID:         "job-1",
		RequestID:  "req-1",
		TurnID:     "turn-1",
		WorkerID:   "worker-a",
		LeaseID:    "lease-a",
		LeaseEpoch: 9,
		ClaimEpoch: 3,
		Phase:      JobStarted,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: "slurm-1",
		},
	}
	baseEnvelope := WorkerTerminalEnvelope{
		JobID:      "job-1",
		RequestID:  "req-1",
		TurnID:     "turn-1",
		WorkerID:   "worker-a",
		LeaseID:    "lease-a",
		LeaseEpoch: 9,
		ClaimEpoch: 3,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: "slurm-1",
		},
		Payload: []byte(`{"ok":true}`),
	}
	cases := map[string]WorkerTerminalEnvelope{
		"request":      copyWorkerTerminal(baseEnvelope, func(in *WorkerTerminalEnvelope) { in.RequestID = "req-other" }),
		"turn":         copyWorkerTerminal(baseEnvelope, func(in *WorkerTerminalEnvelope) { in.TurnID = "turn-other" }),
		"worker":       copyWorkerTerminal(baseEnvelope, func(in *WorkerTerminalEnvelope) { in.WorkerID = "worker-b" }),
		"lease":        copyWorkerTerminal(baseEnvelope, func(in *WorkerTerminalEnvelope) { in.LeaseID = "lease-b" }),
		"lease epoch":  copyWorkerTerminal(baseEnvelope, func(in *WorkerTerminalEnvelope) { in.LeaseEpoch = 8 }),
		"claim epoch":  copyWorkerTerminal(baseEnvelope, func(in *WorkerTerminalEnvelope) { in.ClaimEpoch = 2 }),
		"provider job": copyWorkerTerminal(baseEnvelope, func(in *WorkerTerminalEnvelope) { in.ProviderIdentity.ProviderJobID = "slurm-2" }),
	}
	for name, envelope := range cases {
		st := State{JobAttempts: map[string]JobAttempt{"job-1": baseAttempt}}
		decision, err := AcceptWorkerTerminal(&st, envelope, time.Unix(4, 0))
		if err != nil {
			t.Fatalf("%s: AcceptWorkerTerminal: %v", name, err)
		}
		if decision.Integrity != TerminalDuplicateConflict || decision.Result.Action != "quarantine" || len(st.Terminals) != 0 || st.JobAttempts["job-1"].Phase != JobQuarantined {
			t.Fatalf("%s: stale terminal not rejected, decision=%#v state=%#v", name, decision, st)
		}
	}
}

func TestCancelTurnTombstonesBeaconJobAndRejectsLateTerminal(t *testing.T) {
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-1": {ID: "req-1", ConversationID: "conv", TurnID: "turn-1", State: AllocationRunning, Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}},
		},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting), Jobs: []string{"job-1"}},
		},
		JobAttempts: map[string]JobAttempt{
			"job-1": {
				ID:               "job-1",
				RequestID:        "req-1",
				TurnID:           "turn-1",
				WorkerID:         "worker-1",
				LeaseID:          "lease-1",
				ClaimEpoch:       1,
				Phase:            JobStarted,
				ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
			},
		},
	}
	res := CancelTurn(&st, "conv", "turn-1", "user cancel", time.Unix(5, 0))
	if len(res.Allocations) != 1 || len(res.Jobs) != 1 || !res.Ambiguous {
		t.Fatalf("cancel result = %#v", res)
	}
	if st.Allocations["req-1"].State != AllocationCanceled || st.JobAttempts["job-1"].Phase != JobTombstoned || len(st.Machines["machine-1"].Jobs) != 0 {
		t.Fatalf("cancel did not tombstone state: allocation=%#v job=%#v machine=%#v", st.Allocations["req-1"], st.JobAttempts["job-1"], st.Machines["machine-1"])
	}
	decision, err := AcceptWorkerTerminal(&st, WorkerTerminalEnvelope{
		JobID:      "job-1",
		RequestID:  "req-1",
		TurnID:     "turn-1",
		WorkerID:   "worker-1",
		LeaseID:    "lease-1",
		ClaimEpoch: 1,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: "slurm-1",
		},
		Payload: []byte(`{"text":"late"}`),
	}, time.Unix(6, 0))
	if err != nil {
		t.Fatalf("AcceptWorkerTerminal: %v", err)
	}
	if decision.Integrity != TerminalLateWrite || decision.Result.Action != "ignored" || len(st.Terminals) != 0 || st.JobAttempts["job-1"].Phase != JobTombstoned {
		t.Fatalf("late terminal should be ignored without changing tombstone: decision=%#v state=%#v", decision, st)
	}
}

func TestProviderProjectionPropagatesToMachineAndJobs(t *testing.T) {
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-1": {ID: "req-1", State: AllocationRunning, Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}},
		},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting), Jobs: []string{"claimed", "started"}},
		},
		JobAttempts: map[string]JobAttempt{
			"claimed": {ID: "claimed", RequestID: "req-1", Phase: JobClaimed},
			"started": {ID: "started", RequestID: "req-1", Phase: JobStarted},
		},
	}
	projection := ProjectRawProviderState(ProviderSlurm, "F", "node failed", true, true)
	if _, err := UpdateAllocationProjection(&st, "req-1", projection, time.Unix(3, 0)); err != nil {
		t.Fatalf("UpdateAllocationProjection: %v", err)
	}
	if st.Machines["machine-1"].State != string(LeaseNeedsAttention) || st.JobAttempts["claimed"].Phase != JobQuarantined || st.JobAttempts["started"].Phase != JobQuarantined {
		t.Fatalf("projection did not quarantine machine/jobs: machine=%#v jobs=%#v", st.Machines["machine-1"], st.JobAttempts)
	}
}

func TestMachineReadinessRequiresFreshHeartbeatDoctorProviderAndSignature(t *testing.T) {
	now := time.Unix(100, 0)
	req := AllocationRequest{
		ID:       "req-1",
		Profile:  "gpu",
		Provider: ProviderSlurm,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: "slurm-1",
		},
		Execution: ExecutionSignature{Hash: "sig-a"},
	}
	ready := Machine{
		ID:            "machine-1",
		LeaseID:       "lease-1",
		ProviderJobID: "slurm-1",
		Profile:       "gpu",
		State:         string(LeaseAccepting),
		ProviderState: ProviderJobRunning,
		Doctor:        healthyWorkerDoctor(),
		Execution:     ExecutionSignature{Hash: "sig-a"},
		LastHeartbeat: now.Add(-time.Second),
	}
	if !MachineCanAcceptAllocation(ready, req, now) {
		t.Fatal("ready machine should accept allocation")
	}
	pendingProjection := copyMachine(ready, func(in *Machine) {
		in.ProviderState = ProviderJobPending
	})
	if !MachineCanAcceptAllocation(pendingProjection, req, now) {
		t.Fatal("accepting worker should remain usable when scheduler projection still says pending")
	}
	cases := map[string]Machine{
		"stale heartbeat": copyMachine(ready, func(in *Machine) { in.LastHeartbeat = now.Add(-10 * time.Minute) }),
		"missing doctor":  copyMachine(ready, func(in *Machine) { in.Doctor = WorkerDoctor{} }),
		"suspended provider": copyMachine(ready, func(in *Machine) {
			in.ProviderState = ProviderJobSuspended
		}),
		"signature mismatch": copyMachine(ready, func(in *Machine) {
			in.Execution.Hash = "sig-b"
		}),
	}
	for name, machine := range cases {
		if MachineCanAcceptAllocation(machine, req, now) {
			t.Fatalf("%s should not accept allocation: %#v", name, machine)
		}
	}
}

func copyMachine(in Machine, fn func(*Machine)) Machine {
	fn(&in)
	return in
}

func copyWorkerTerminal(in WorkerTerminalEnvelope, fn func(*WorkerTerminalEnvelope)) WorkerTerminalEnvelope {
	fn(&in)
	return in
}

func healthyWorkerDoctor() WorkerDoctor {
	return WorkerDoctor{
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

func copyDoctor(in WorkerDoctor, fn func(*WorkerDoctor)) WorkerDoctor {
	fn(&in)
	return in
}

func copyReadiness(in LeaseReadiness, fn func(*LeaseReadiness)) LeaseReadiness {
	fn(&in)
	return in
}

func copyProfile(in Profile, fn func(*Profile)) Profile {
	fn(&in)
	return in
}

type fakeAllocationAdapter struct {
	query     SchedulerQueryResult
	queryErr  error
	submit    SchedulerQueryResult
	submitErr error
	renew     SchedulerQueryResult
	renewErr  error
	cancel    SchedulerQueryResult
	cancelErr error
	submits   int
	renews    int
	cancels   int
}

func (a *fakeAllocationAdapter) QueryAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	if a.queryErr != nil {
		return SchedulerQueryResult{}, a.queryErr
	}
	return a.query, nil
}

func (a *fakeAllocationAdapter) SubmitAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	a.submits++
	if a.submitErr != nil {
		return SchedulerQueryResult{}, a.submitErr
	}
	return a.submit, nil
}

func (a *fakeAllocationAdapter) RenewAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	a.renews++
	if a.renewErr != nil {
		return SchedulerQueryResult{}, a.renewErr
	}
	return a.renew, nil
}

func (a *fakeAllocationAdapter) CancelAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	a.cancels++
	if a.cancelErr != nil {
		return SchedulerQueryResult{}, a.cancelErr
	}
	return a.cancel, nil
}

func copyAllocation(in AllocationRequest, fn func(*AllocationRequest)) AllocationRequest {
	fn(&in)
	return in
}

func TestReconcileAllocationRenewOutsideLockRenewsDueLeaseAndUpdatesMachine(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1000, 0)
	deadline := now.Add(time.Minute)
	if err := store.Save(State{
		Allocations: map[string]AllocationRequest{
			"req-1": {
				ID:               "req-1",
				State:            AllocationRunning,
				Provider:         ProviderSlurm,
				ProviderDeadline: deadline,
				ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
			},
		},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting), LeaseExpiresAt: deadline},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	renewedDeadline := now.Add(30 * time.Minute)
	adapter := &fakeAllocationAdapter{renew: SchedulerQueryResult{RawState: "R", Reason: "renewed", ProviderDeadline: renewedDeadline}}
	req, action, err := ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{}, now)
	if err != nil {
		t.Fatalf("ReconcileAllocationRenewOutsideLock: %v", err)
	}
	if action != AllocationRenewNow || adapter.renews != 1 || req.RenewEpoch != 1 || req.RenewError != "" {
		t.Fatalf("renew result mismatch: action=%s renews=%d req=%#v", action, adapter.renews, req)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := st.Allocations["req-1"].ProviderDeadline; !got.Equal(renewedDeadline) {
		t.Fatalf("allocation deadline = %s, want %s", got, renewedDeadline)
	}
	if got := st.Machines["machine-1"].LeaseExpiresAt; !got.Equal(renewedDeadline) {
		t.Fatalf("machine deadline = %s, want %s", got, renewedDeadline)
	}
	if err := ValidateAudit(st); err != nil {
		t.Fatalf("ValidateAudit after renew: %v", err)
	}
	for _, want := range []string{"allocation_renew_start", "allocation_renew_completed"} {
		if !auditActionsContain(st, want) {
			t.Fatalf("renew audit missing %s: %#v", want, st.Audit)
		}
	}
}

func TestReconcileAllocationRenewOutsideLockFailsUrgentLeaseNeedsAttention(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1000, 0)
	deadline := now.Add(10 * time.Second)
	if err := store.Save(State{Allocations: map[string]AllocationRequest{
		"req-1": {ID: "req-1", State: AllocationRunning, Provider: ProviderSlurm, ProviderDeadline: deadline, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}},
	}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	adapter := &fakeAllocationAdapter{renewErr: errors.New("renew denied")}
	req, action, err := ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{}, now)
	if err == nil || action != AllocationRenewNow || req.State != AllocationNeedsAttention || !strings.Contains(req.RenewError, "renew denied") {
		t.Fatalf("urgent renew failure should mark needs_attention: action=%s req=%#v err=%v", action, req, err)
	}
}

func TestReconcileAllocationRenewOutsideLockThrottlesRenewWithoutNewDeadline(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1000, 0)
	if err := store.Save(State{Allocations: map[string]AllocationRequest{
		"req-1": {ID: "req-1", State: AllocationRunning, Provider: ProviderSlurm, ProviderDeadline: now.Add(time.Minute), ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}},
	}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	adapter := &fakeAllocationAdapter{renew: SchedulerQueryResult{RawState: "R", Reason: "renew accepted"}}
	req, action, err := ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{}, now)
	if err != nil || action != AllocationRenewNow || adapter.renews != 1 || req.RenewError != "" {
		t.Fatalf("first renew should succeed: action=%s renews=%d req=%#v err=%v", action, adapter.renews, req, err)
	}
	req, action, err = ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{}, now.Add(time.Minute))
	if err != nil || action != AllocationRenewWait || adapter.renews != 1 {
		t.Fatalf("recent successful renew without deadline should be throttled: action=%s renews=%d req=%#v err=%v", action, adapter.renews, req, err)
	}
}

func TestReconcileAllocationRenewOutsideLockStartedOnlySkipsQueuedJobs(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1000, 0)
	if err := store.Save(State{
		Allocations: map[string]AllocationRequest{
			"req-1": {ID: "req-1", State: AllocationRunning, Provider: ProviderSlurm, ProviderDeadline: now.Add(time.Minute), ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}},
		},
		JobAttempts: map[string]JobAttempt{
			"job-1": {ID: "job-1", RequestID: "req-1", Phase: JobQueued},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	adapter := &fakeAllocationAdapter{renew: SchedulerQueryResult{ProviderDeadline: now.Add(time.Hour)}}
	req, action, err := ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{StartedOnly: true}, now)
	if err != nil || action != AllocationRenewSkip || adapter.renews != 0 || req.RenewEpoch != 0 {
		t.Fatalf("started-only renewal should skip queued jobs: action=%s renews=%d req=%#v err=%v", action, adapter.renews, req, err)
	}
	if err := store.Update(func(st *State) error {
		job := st.JobAttempts["job-1"]
		job.Phase = JobStartIntent
		st.JobAttempts[job.ID] = job
		return nil
	}); err != nil {
		t.Fatalf("mark start intent: %v", err)
	}
	req, action, err = ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{StartedOnly: true}, now.Add(time.Second))
	if err != nil || action != AllocationRenewNow || adapter.renews != 1 || req.RenewEpoch != 1 {
		t.Fatalf("started-only renewal should protect possible-start jobs: action=%s renews=%d req=%#v err=%v", action, adapter.renews, req, err)
	}
}

func TestCancelAllocationOutsideLockCancelsProviderWithoutMachine(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
			p.Provider = ProviderSlurm
			p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 2}
		})}
		req, _, err := EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, now)
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	adapter := &fakeAllocationAdapter{cancel: SchedulerQueryResult{ProviderJobID: "slurm-1", RawState: "CA", Reason: "canceled"}}
	res, err := CancelAllocationOutsideLock(context.Background(), store, "slurm-1", adapter, "manual release", false, now.Add(time.Second))
	if err != nil {
		t.Fatalf("CancelAllocationOutsideLock: %v", err)
	}
	if res.Action != AllocationCancelProviderCancel || adapter.cancels != 1 || res.Request.State != AllocationCanceled {
		t.Fatalf("cancel result action=%s cancels=%d req=%#v", res.Action, adapter.cancels, res.Request)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("Load after cancel: %v", err)
	}
	if err := ValidateAudit(st); err != nil {
		t.Fatalf("ValidateAudit after cancel: %v", err)
	}
	for _, want := range []string{"allocation_release_request", "allocation_cancel_completed"} {
		if !auditActionsContain(st, want) {
			t.Fatalf("cancel audit missing %s: %#v", want, st.Audit)
		}
	}
}

func TestCancelAllocationOutsideLockRemoteProviderWithoutAdapterNeedsAttention(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	var req AllocationRequest
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
			p.Provider = ProviderSlurm
			p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 2}
		})}
		var err error
		req, _, err = EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"}, now)
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		st.JobAttempts = map[string]JobAttempt{"job-queued": {ID: "job-queued", RequestID: req.ID, TurnID: "turn-1", Phase: JobQueued}}
		st.Machines = map[string]Machine{"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting), Jobs: []string{"job-queued"}}}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	res, err := CancelAllocationOutsideLock(context.Background(), store, "slurm-1", nil, "manual release", false, now.Add(time.Second))
	if err == nil || !strings.Contains(err.Error(), "cancel adapter is not configured") {
		t.Fatalf("CancelAllocationOutsideLock error = %v, want missing adapter", err)
	}
	loaded, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("Load: %v", loadErr)
	}
	if res.Action != AllocationCancelNeedsAttention || loaded.Allocations[req.ID].State != AllocationNeedsAttention {
		t.Fatalf("missing adapter state action=%s allocation=%#v", res.Action, loaded.Allocations[req.ID])
	}
	if !strings.Contains(loaded.Allocations[req.ID].ProviderReason, "cancel adapter is not configured") {
		t.Fatalf("provider reason = %q, want adapter diagnostic", loaded.Allocations[req.ID].ProviderReason)
	}
	if loaded.Machines["machine-1"].State != string(LeaseDraining) || loaded.JobAttempts["job-queued"].Phase != JobTombstoned {
		t.Fatalf("missing adapter should drain/tombstone local work: machine=%#v job=%#v", loaded.Machines["machine-1"], loaded.JobAttempts["job-queued"])
	}
}

func TestCancelAllocationOutsideLockMissingCancelAdapterClearsGoneProviderJob(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	var req AllocationRequest
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
			p.Provider = ProviderSlurm
			p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 2}
		})}
		var err error
		req, _, err = EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1"}, now)
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.RawProviderState = "CG"
		req.ProviderReason = "node-1"
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	adapter := &fakeAllocationAdapter{
		query: SchedulerQueryResult{
			DurableNegative: true,
			Reason:          "job is no longer visible in scheduler history",
		},
		cancelErr: ProviderCommandNotConfiguredError{
			Provider:  ProviderSlurm,
			Operation: "cancel",
			EnvName:   BeaconSlurmCancelCommandEnv,
		},
	}
	res, err := CancelAllocationOutsideLock(context.Background(), store, "slurm-1", adapter, "manual release", true, now.Add(time.Second))
	if err != nil {
		t.Fatalf("CancelAllocationOutsideLock: %v", err)
	}
	loaded, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("Load: %v", loadErr)
	}
	if res.Action != AllocationCancelMarkCanceled || adapter.cancels != 1 {
		t.Fatalf("cancel fallback action=%s cancels=%d", res.Action, adapter.cancels)
	}
	if loaded.Allocations[req.ID].State != AllocationCanceled {
		t.Fatalf("gone provider job should be locally canceled without cancel adapter: %#v", loaded.Allocations[req.ID])
	}
	if !strings.Contains(loaded.Allocations[req.ID].ProviderReason, "scheduler history") {
		t.Fatalf("provider reason = %q, want query fallback reason", loaded.Allocations[req.ID].ProviderReason)
	}
}

func TestCancelAllocationOutsideLockDrainsStartedJobWithoutProviderCancel(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
			p.Provider = ProviderSlurm
			p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 2}
		})}
		req, _, err := EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"}, now)
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		st.JobAttempts = map[string]JobAttempt{"job-1": {ID: "job-1", RequestID: req.ID, TurnID: "turn-1", Phase: JobStarted}}
		st.Machines = map[string]Machine{"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting)}}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	adapter := &fakeAllocationAdapter{cancel: SchedulerQueryResult{ProviderJobID: "slurm-1", RawState: "CA", Reason: "canceled"}}
	res, err := CancelAllocationOutsideLock(context.Background(), store, "slurm-1", adapter, "manual release", false, now.Add(time.Second))
	if err != nil {
		t.Fatalf("CancelAllocationOutsideLock: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Action != AllocationCancelDrainStarted || adapter.cancels != 0 || loaded.Allocations[res.Request.ID].State != AllocationRunning || loaded.Machines["machine-1"].State != string(LeaseDraining) {
		t.Fatalf("drain result action=%s cancels=%d allocation=%#v machine=%#v", res.Action, adapter.cancels, loaded.Allocations[res.Request.ID], loaded.Machines["machine-1"])
	}
}

func TestCancelAllocationOutsideLockCancelsAfterTerminalJob(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
			p.Provider = ProviderSlurm
			p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 2}
		})}
		req, _, err := EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"}, now)
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		st.JobAttempts = map[string]JobAttempt{"job-1": {ID: "job-1", RequestID: req.ID, TurnID: "turn-1", Phase: JobTerminal}}
		st.Machines = map[string]Machine{"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting)}}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	adapter := &fakeAllocationAdapter{cancel: SchedulerQueryResult{ProviderJobID: "slurm-1", RawState: "CA", Reason: "canceled"}}
	res, err := CancelAllocationOutsideLock(context.Background(), store, "slurm-1", adapter, "manual release", false, now.Add(time.Second))
	if err != nil {
		t.Fatalf("CancelAllocationOutsideLock: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Action != AllocationCancelProviderCancel || adapter.cancels != 1 || loaded.Allocations[res.Request.ID].State != AllocationCanceled || len(loaded.Machines) != 0 {
		t.Fatalf("terminal release action=%s cancels=%d allocation=%#v machines=%#v", res.Action, adapter.cancels, loaded.Allocations[res.Request.ID], loaded.Machines)
	}
}

func TestCancelAllocationOutsideLockTombstonesQueuedAndClaimedJobs(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
			p.Provider = ProviderSlurm
			p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 2}
		})}
		req, _, err := EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"}, now)
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		st.JobAttempts = map[string]JobAttempt{
			"job-queued":  {ID: "job-queued", RequestID: req.ID, TurnID: "turn-1", Phase: JobQueued},
			"job-claimed": {ID: "job-claimed", RequestID: req.ID, TurnID: "turn-1", Phase: JobClaimed},
		}
		st.Machines = map[string]Machine{"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting), Jobs: []string{"job-queued", "job-claimed"}}}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	adapter := &fakeAllocationAdapter{cancel: SchedulerQueryResult{ProviderJobID: "slurm-1", RawState: "CA", Reason: "canceled"}}
	res, err := CancelAllocationOutsideLock(context.Background(), store, "slurm-1", adapter, "manual release", false, now.Add(time.Second))
	if err != nil {
		t.Fatalf("CancelAllocationOutsideLock: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Action != AllocationCancelProviderCancel || adapter.cancels != 1 || loaded.Allocations[res.Request.ID].State != AllocationCanceled {
		t.Fatalf("cancel result action=%s cancels=%d allocation=%#v", res.Action, adapter.cancels, loaded.Allocations[res.Request.ID])
	}
	if loaded.JobAttempts["job-queued"].Phase != JobTombstoned || loaded.JobAttempts["job-claimed"].Phase != JobTombstoned || len(loaded.Machines) != 0 {
		t.Fatalf("jobs/machines not tombstoned: jobs=%#v machines=%#v", loaded.JobAttempts, loaded.Machines)
	}
}

func TestCancelAllocationOutsideLockDoesNotTouchMachinesWithoutStableMatchKey(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	var req AllocationRequest
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
		var err error
		req, _, err = EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, now)
		if err != nil {
			return err
		}
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		st.Machines = map[string]Machine{
			"unrelated-empty-lease": {ID: "unrelated-empty-lease", State: string(LeaseAccepting)},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	res, err := CancelAllocationOutsideLock(context.Background(), store, req.ID, nil, "manual release", false, now.Add(time.Second))
	if err != nil {
		t.Fatalf("CancelAllocationOutsideLock: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if res.Action != AllocationCancelMarkCanceled || loaded.Allocations[req.ID].State != AllocationCanceled {
		t.Fatalf("cancel result action=%s allocation=%#v", res.Action, loaded.Allocations[req.ID])
	}
	if _, ok := loaded.Machines["unrelated-empty-lease"]; !ok {
		t.Fatalf("release without provider job or lease should not delete unrelated machines: %#v", loaded.Machines)
	}
}

func TestCancelAllocationOutsideLockKeepsMachineWhenProviderCancelFails(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	if err := store.Update(func(st *State) error {
		st.Profiles = map[string]Profile{"gpu": copyProfile(readyProfile("gpu"), func(p *Profile) {
			p.Provider = ProviderSlurm
			p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 2}
		})}
		req, _, err := EnsureAllocationRequest(st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"}, now)
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-1"
		req.State = AllocationRunning
		st.Allocations[req.ID] = req
		st.JobAttempts = map[string]JobAttempt{"job-queued": {ID: "job-queued", RequestID: req.ID, TurnID: "turn-1", Phase: JobQueued}}
		st.Machines = map[string]Machine{"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseAccepting), Jobs: []string{"job-queued"}}}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	adapter := &fakeAllocationAdapter{cancelErr: fmt.Errorf("provider unavailable")}
	res, err := CancelAllocationOutsideLock(context.Background(), store, "slurm-1", adapter, "manual release", false, now.Add(time.Second))
	if err == nil {
		t.Fatal("CancelAllocationOutsideLock error = nil, want provider error")
	}
	loaded, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("Load: %v", loadErr)
	}
	if res.Action != AllocationCancelNeedsAttention || loaded.Allocations[res.Request.ID].State != AllocationNeedsAttention || loaded.Machines["machine-1"].State != string(LeaseDraining) {
		t.Fatalf("failed cancel state action=%s allocation=%#v machine=%#v", res.Action, loaded.Allocations[res.Request.ID], loaded.Machines["machine-1"])
	}
	if loaded.JobAttempts["job-queued"].Phase != JobTombstoned {
		t.Fatalf("queued job phase = %s, want tombstoned", loaded.JobAttempts["job-queued"].Phase)
	}
}

func TestReconcileAllocationSubmitOutsideLockProjectsKnownProviderState(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(1, 0)
	if err := store.Save(State{Allocations: map[string]AllocationRequest{
		"req-1": {
			ID:               "req-1",
			State:            AllocationRunning,
			Provider:         ProviderSlurm,
			ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
		},
	}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	adapter := &fakeAllocationAdapter{query: SchedulerQueryResult{ProviderJobID: "slurm-1", RawState: "F", Reason: "node failed"}}
	req, action, err := ReconcileAllocationSubmitOutsideLock(context.Background(), store, "req-1", adapter, now.Add(time.Second))
	if err != nil {
		t.Fatalf("ReconcileAllocationSubmitOutsideLock: %v", err)
	}
	if action != AllocationSubmitAlreadyKnown || req.State != AllocationRequestPersisted || req.RawProviderState != "F" || req.ReplacementID != "slurm-1" {
		t.Fatalf("known provider state was not projected: action=%s req=%#v", action, req)
	}
}

func TestRefreshKnownProviderAllocationsClearsGonePreStartProviderJob(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(3, 0)
	if err := store.Save(State{Allocations: map[string]AllocationRequest{
		"req-1": {
			ID:               "req-1",
			State:            AllocationRunning,
			Provider:         ProviderSlurm,
			Profile:          "gpu",
			ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
			Target:           TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1"},
			SubmitAttempts:   1,
		},
	}}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	errs := RefreshKnownProviderAllocationsOutsideLock(context.Background(), store, &fakeAllocationAdapter{
		query: SchedulerQueryResult{DurableNegative: true, Reason: "job gone from scheduler history"},
	}, now)
	if len(errs) != 0 {
		t.Fatalf("RefreshKnownProviderAllocationsOutsideLock errors: %v", errs)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	req := loaded.Allocations["req-1"]
	if req.State != AllocationRequestPersisted ||
		req.ProviderIdentity.ProviderJobID != "" ||
		req.Target.ProviderJobID != "" ||
		req.ReplacementID != "slurm-1" ||
		req.SubmitAttempts != 0 {
		t.Fatalf("gone pre-start provider job should be cleared for replacement without blocking upgrade: %#v", req)
	}
}

func TestRefreshKnownProviderAllocationsQuarantinesGoneStartedProviderJob(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(3, 0)
	if err := store.Save(State{
		Allocations: map[string]AllocationRequest{
			"req-1": {
				ID:               "req-1",
				State:            AllocationRunning,
				Provider:         ProviderSlurm,
				ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
			},
		},
		JobAttempts: map[string]JobAttempt{
			"job-1": {ID: "job-1", RequestID: "req-1", Phase: JobStarted},
		},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	errs := RefreshKnownProviderAllocationsOutsideLock(context.Background(), store, &fakeAllocationAdapter{
		query: SchedulerQueryResult{DurableNegative: true, Reason: "job gone after possible start"},
	}, now)
	if len(errs) != 0 {
		t.Fatalf("RefreshKnownProviderAllocationsOutsideLock errors: %v", errs)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Allocations["req-1"].State != AllocationNeedsAttention || loaded.JobAttempts["job-1"].Phase != JobQuarantined {
		t.Fatalf("gone started provider job must remain protected: allocation=%#v job=%#v", loaded.Allocations["req-1"], loaded.JobAttempts["job-1"])
	}
}

func TestProviderProjectionAutoReplacesOnlyBeforeStart(t *testing.T) {
	now := time.Unix(3, 0)
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-1": {ID: "req-1", State: AllocationPending, Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}, Target: TargetSnapshot{ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"}, SubmitAttempts: 1},
		},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", State: string(LeaseStarting), Jobs: []string{"job-1"}},
		},
		JobAttempts: map[string]JobAttempt{
			"job-1": {ID: "job-1", RequestID: "req-1", Phase: JobQueued, Target: TargetSnapshot{ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"}, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}},
		},
	}
	projection := ProjectRawProviderState(ProviderSlurm, "F", "node failed before start", false, false)
	if _, err := UpdateAllocationProjection(&st, "req-1", projection, now); err != nil {
		t.Fatalf("UpdateAllocationProjection: %v", err)
	}
	req := st.Allocations["req-1"]
	if req.State != AllocationRequestPersisted || req.ProviderIdentity.ProviderJobID != "" || req.SubmitAttempts != 0 || req.ReplacementID != "slurm-1" || req.ReplacementEpoch != 1 {
		t.Fatalf("lost pre-start allocation should be reset for replacement, req=%#v", req)
	}
	job := st.JobAttempts["job-1"]
	if job.Phase != JobQueued || job.ProviderIdentity.ProviderJobID != "" || job.Target.ProviderJobID != "" || job.Target.MachineID != "" || job.Target.LeaseID != "" {
		t.Fatalf("queued job should be rebound to replacement allocation, job=%#v", job)
	}

	st.JobAttempts["job-1"] = JobAttempt{ID: "job-1", RequestID: "req-1", Phase: JobStartIntent}
	st.Allocations["req-1"] = AllocationRequest{ID: "req-1", State: AllocationRunning, Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-2"}}
	projection = ProjectRawProviderState(ProviderSlurm, "F", "node failed after start intent", true, true)
	if _, err := UpdateAllocationProjection(&st, "req-1", projection, now.Add(time.Second)); err != nil {
		t.Fatalf("UpdateAllocationProjection after start: %v", err)
	}
	if st.Allocations["req-1"].State != AllocationNeedsAttention || st.JobAttempts["job-1"].Phase != JobQuarantined {
		t.Fatalf("after-start provider loss must not auto-replace: req=%#v job=%#v", st.Allocations["req-1"], st.JobAttempts["job-1"])
	}
}

func TestReconcileAllocationSubmitRecordsSubmitErrorsWithoutRetryingBlindly(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	adapter := &fakeAllocationAdapter{query: SchedulerQueryResult{DurableNegative: true}, submitErr: errors.New("scheduler unavailable")}
	updated, action, err := ReconcileAllocationSubmit(context.Background(), &st, req.ID, adapter, time.Unix(2, 0))
	if err == nil || action != AllocationSubmitNow || adapter.submits != 1 {
		t.Fatalf("submit error should be surfaced after exactly one attempt, action=%s req=%#v submits=%d err=%v", action, updated, adapter.submits, err)
	}
	if updated.State != AllocationNeedsAttention || !strings.Contains(updated.ProviderReason, "scheduler unavailable") {
		t.Fatalf("submit error should be recorded for status, req=%#v", updated)
	}
}

func TestReconcileAllocationSubmitOutsideLockRechecksCancelBeforeSubmit(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	if err := store.Save(st); err != nil {
		t.Fatalf("Save: %v", err)
	}
	adapter := &queryHookSubmitAdapter{
		query: SchedulerQueryResult{DurableNegative: true},
		hook: func() {
			_ = store.Update(func(st *State) error {
				CancelTurn(st, "conv", "turn-1", "cancel during provider query", time.Unix(2, 0))
				return nil
			})
		},
	}
	updated, action, err := ReconcileAllocationSubmitOutsideLock(context.Background(), store, req.ID, adapter, time.Unix(3, 0))
	if err != nil {
		t.Fatalf("ReconcileAllocationSubmitOutsideLock: %v", err)
	}
	if action != AllocationSubmitAlreadyKnown || updated.State != AllocationCanceled || adapter.submits != 0 {
		t.Fatalf("cancel before submit should skip external submit, action=%s submits=%d req=%#v", action, adapter.submits, updated)
	}
}

type queryHookSubmitAdapter struct {
	query   SchedulerQueryResult
	hook    func()
	submits int
}

func (a *queryHookSubmitAdapter) QueryAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	if a.hook != nil {
		a.hook()
	}
	return a.query, nil
}

func (a *queryHookSubmitAdapter) SubmitAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	a.submits++
	return SchedulerQueryResult{ProviderJobID: "slurm-unwanted"}, nil
}

func auditActionsContain(st State, action string) bool {
	for _, rec := range st.Audit {
		if rec.Action == action {
			return true
		}
	}
	return false
}
