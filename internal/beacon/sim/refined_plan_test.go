package sim

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type allocationSubmitStore struct {
	intents         map[string]string
	schedulerJobs   map[string]string
	queryError      map[string]bool
	queryMultiple   map[string]bool
	durableNegative map[string]bool
	submitCount     int
}

func newAllocationSubmitStore() *allocationSubmitStore {
	return &allocationSubmitStore{
		intents:         map[string]string{},
		schedulerJobs:   map[string]string{},
		queryError:      map[string]bool{},
		queryMultiple:   map[string]bool{},
		durableNegative: map[string]bool{},
	}
}

func (s *allocationSubmitStore) ensureSubmitted(requestID string) string {
	providerID, intentExists := s.intents[requestID]
	if providerID = strings.TrimSpace(providerID); providerID != "" {
		return providerID
	}
	if intentExists && (s.queryError[requestID] || s.queryMultiple[requestID]) {
		return ""
	}
	if providerID := strings.TrimSpace(s.schedulerJobs[requestID]); providerID != "" {
		s.intents[requestID] = providerID
		return providerID
	}
	if intentExists && !s.durableNegative[requestID] {
		return ""
	}
	s.submitCount++
	providerID = fmt.Sprintf("provider-job-%d", s.submitCount)
	s.schedulerJobs[requestID] = providerID
	s.intents[requestID] = providerID
	return providerID
}

func TestAllocationSubmitCrashWindowUsesDeterministicRequestID(t *testing.T) {
	store := newAllocationSubmitStore()
	store.intents["req-1"] = ""
	store.schedulerJobs["req-1"] = "slurm-123"

	if got := store.ensureSubmitted("req-1"); got != "slurm-123" {
		t.Fatalf("restart should discover existing provider job, got %q", got)
	}
	if store.submitCount != 0 {
		t.Fatalf("restart must not submit duplicate scheduler jobs, submitCount=%d", store.submitCount)
	}
	if got := store.ensureSubmitted("req-1"); got != "slurm-123" || store.submitCount != 0 {
		t.Fatalf("persisted provider id should remain idempotent, got=%q submitCount=%d", got, store.submitCount)
	}

	store = newAllocationSubmitStore()
	store.intents["req-2"] = ""
	if got := store.ensureSubmitted("req-2"); got != "" || store.submitCount != 0 {
		t.Fatalf("uncertain empty scheduler query after submit must wait, got=%q submitCount=%d", got, store.submitCount)
	}
	store.durableNegative["req-2"] = true
	if got := store.ensureSubmitted("req-2"); got == "" || store.submitCount != 1 {
		t.Fatalf("durable negative query may resubmit once, got=%q submitCount=%d", got, store.submitCount)
	}

	for _, setup := range []func(*allocationSubmitStore){
		func(s *allocationSubmitStore) { s.queryError["req-3"] = true },
		func(s *allocationSubmitStore) { s.queryMultiple["req-3"] = true },
	} {
		store = newAllocationSubmitStore()
		store.intents["req-3"] = ""
		setup(store)
		if got := store.ensureSubmitted("req-3"); got != "" || store.submitCount != 0 {
			t.Fatalf("failed/multiple scheduler query must not resubmit, got=%q submitCount=%d", got, store.submitCount)
		}
	}
}

type leaseReadinessInput struct {
	provider        providerJobState
	workerHeartbeat bool
	doctor          bootstrapCheck
	membershipProof bool
}

func leaseCanAcceptAfterProviderRun(in leaseReadinessInput) bool {
	return in.provider == providerRunning &&
		in.workerHeartbeat &&
		bootstrapAccepting(in.doctor) &&
		in.membershipProof
}

func TestProviderRunDoesNotMeanWorkerAccepting(t *testing.T) {
	doctor := bootstrapCheck{
		sharedRootMounted:  true,
		atomicCreateOK:     true,
		freeBytesOK:        true,
		freeInodesOK:       true,
		codexAvailable:     true,
		homeWritable:       true,
		proxyOK:            true,
		imageDigestMatch:   true,
		protocolOK:         true,
		containerRuntimeOK: true,
		modulesOK:          true,
		bindMountsOK:       true,
		tmpWritable:        true,
		authPathOK:         true,
		proxyEnvInsideOK:   true,
	}
	base := leaseReadinessInput{provider: providerRunning, workerHeartbeat: true, doctor: doctor, membershipProof: true}
	if !leaseCanAcceptAfterProviderRun(base) {
		t.Fatal("provider RUN plus worker heartbeat, doctor, and membership proof should accept")
	}
	cases := map[string]leaseReadinessInput{
		"pending provider":   copyLeaseReadiness(base, func(in *leaseReadinessInput) { in.provider = providerPending }),
		"missing heartbeat":  copyLeaseReadiness(base, func(in *leaseReadinessInput) { in.workerHeartbeat = false }),
		"doctor failed":      copyLeaseReadiness(base, func(in *leaseReadinessInput) { in.doctor.codexAvailable = false }),
		"outside allocation": copyLeaseReadiness(base, func(in *leaseReadinessInput) { in.membershipProof = false }),
	}
	for name, in := range cases {
		if leaseCanAcceptAfterProviderRun(in) {
			t.Fatalf("%s must not become accepting: %#v", name, in)
		}
	}
}

func copyLeaseReadiness(in leaseReadinessInput, fn func(*leaseReadinessInput)) leaseReadinessInput {
	fn(&in)
	return in
}

type launchCrashWindow struct {
	claimSynced           bool
	startIntentSynced     bool
	processStartAckSynced bool
	workerProvesNoExec    bool
}

func recoverLaunchWindow(in launchCrashWindow) recoveryAction {
	if !in.claimSynced {
		return recoverQuarantine
	}
	if !in.startIntentSynced {
		return recoverRequeue
	}
	if in.workerProvesNoExec {
		return recoverRequeue
	}
	if in.processStartAckSynced {
		return recoverAmbiguous
	}
	return recoverAmbiguous
}

func TestReplayAfterStartRequiresDurableNoExecProof(t *testing.T) {
	tests := []struct {
		name string
		in   launchCrashWindow
		want recoveryAction
	}{
		{name: "unsynced claim quarantines", in: launchCrashWindow{}, want: recoverQuarantine},
		{name: "claim before start intent requeues", in: launchCrashWindow{claimSynced: true}, want: recoverRequeue},
		{name: "start intent without proof ambiguous", in: launchCrashWindow{claimSynced: true, startIntentSynced: true}, want: recoverAmbiguous},
		{name: "worker durable no exec proof requeues", in: launchCrashWindow{claimSynced: true, startIntentSynced: true, workerProvesNoExec: true}, want: recoverRequeue},
		{name: "process start ack ambiguous", in: launchCrashWindow{claimSynced: true, startIntentSynced: true, processStartAckSynced: true}, want: recoverAmbiguous},
	}
	for _, tt := range tests {
		if got := recoverLaunchWindow(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

func providerDoneReconcileWithGrace(job jobState, terminal terminalIntegrity, graceActive bool) reconcileAction {
	if terminal == terminalValid || terminal == terminalDuplicateSame {
		return reconcileCompleted
	}
	if graceActive {
		return reconcileFinalizing
	}
	if job == jobStarted || job == jobStartIntent {
		return reconcileQuarantine
	}
	return reconcileLost
}

func TestProviderDoneAllowsFinalizingGraceForSharedFSTerminal(t *testing.T) {
	if got := providerDoneReconcileWithGrace(jobStarted, terminalNone, true); got != reconcileFinalizing {
		t.Fatalf("DONE without visible terminal should finalizing during grace, got %s", got)
	}
	if got := providerDoneReconcileWithGrace(jobStarted, terminalValid, true); got != reconcileCompleted {
		t.Fatalf("valid terminal during grace should complete, got %s", got)
	}
	if got := providerDoneReconcileWithGrace(jobStarted, terminalNone, false); got != reconcileQuarantine {
		t.Fatalf("DONE after grace with started job should quarantine, got %s", got)
	}
}

type rawProviderProjection struct {
	provider string
	rawState string
	reason   string
	action   reconcileAction
}

func projectRawProviderState(provider, rawState, reason string, started bool) rawProviderProjection {
	return projectRawProviderStateWithHistory(provider, rawState, reason, started, false)
}

func projectRawProviderStateWithHistory(provider, rawState, reason string, started bool, previouslyRan bool) rawProviderProjection {
	normalized := strings.ToUpper(strings.TrimSpace(rawState))
	projection := rawProviderProjection{provider: provider, rawState: rawState, reason: reason}
	if provider == "lsf" && normalized == "PEND" && previouslyRan {
		if started {
			projection.action = reconcileQuarantine
		} else {
			projection.action = reconcileSuspended
		}
		return projection
	}
	switch provider + ":" + normalized {
	case "slurm:PD", "lsf:PEND":
		projection.action = reconcilePending
	case "slurm:R", "lsf:RUN":
		projection.action = reconcileRunning
	case "slurm:CG", "slurm:CD", "lsf:DONE", "lsf:EXIT":
		projection.action = reconcileFinalizing
	case "slurm:CA", "slurm:F", "slurm:TO":
		if started {
			projection.action = reconcileQuarantine
		} else {
			projection.action = reconcileLost
		}
	case "slurm:S", "slurm:STOPPED", "lsf:SSUSP", "lsf:USUSP", "lsf:PSUSP":
		projection.action = reconcileSuspended
	case "slurm:PR", "slurm:REQUEUE", "slurm:NF", "slurm:OOM", "lsf:UNKWN", "lsf:ZOMBI":
		if started {
			projection.action = reconcileQuarantine
		} else {
			projection.action = reconcileLost
		}
	default:
		projection.action = reconcileDrain
	}
	return projection
}

func TestRawSlurmAndLSFStatesPreserveReasonAndConservativeAction(t *testing.T) {
	tests := []struct {
		provider string
		raw      string
		started  bool
		want     reconcileAction
	}{
		{provider: "slurm", raw: "PD", want: reconcilePending},
		{provider: "slurm", raw: "R", want: reconcileRunning},
		{provider: "slurm", raw: "CG", started: true, want: reconcileFinalizing},
		{provider: "slurm", raw: "CD", started: true, want: reconcileFinalizing},
		{provider: "slurm", raw: "CA", started: true, want: reconcileQuarantine},
		{provider: "slurm", raw: "F", want: reconcileLost},
		{provider: "slurm", raw: "TO", started: true, want: reconcileQuarantine},
		{provider: "slurm", raw: "PR", started: true, want: reconcileQuarantine},
		{provider: "slurm", raw: "PR", want: reconcileLost},
		{provider: "slurm", raw: "OOM", started: true, want: reconcileQuarantine},
		{provider: "lsf", raw: "PEND", want: reconcilePending},
		{provider: "lsf", raw: "RUN", want: reconcileRunning},
		{provider: "lsf", raw: "DONE", started: true, want: reconcileFinalizing},
		{provider: "lsf", raw: "EXIT", started: true, want: reconcileFinalizing},
		{provider: "lsf", raw: "SSUSP", started: true, want: reconcileSuspended},
		{provider: "lsf", raw: "USUSP", started: true, want: reconcileSuspended},
		{provider: "lsf", raw: "PSUSP", started: true, want: reconcileSuspended},
		{provider: "lsf", raw: "UNKWN", started: true, want: reconcileQuarantine},
		{provider: "lsf", raw: "ZOMBI", started: true, want: reconcileQuarantine},
	}
	for _, tt := range tests {
		got := projectRawProviderState(tt.provider, tt.raw, "GPU unavailable", tt.started)
		if got.action != tt.want || got.reason != "GPU unavailable" || got.rawState != tt.raw {
			t.Fatalf("%s %s: got %#v want action=%s with raw reason preserved", tt.provider, tt.raw, got, tt.want)
		}
	}

	pendAfterRun := projectRawProviderStateWithHistory("lsf", "PEND", "preempted and requeued", true, true)
	if pendAfterRun.action != reconcileQuarantine {
		t.Fatalf("LSF PEND-after-RUN after process start must not become normal pending, got %#v", pendAfterRun)
	}
	pendAfterRunPreStart := projectRawProviderStateWithHistory("lsf", "PEND", "preempted before start", false, true)
	if pendAfterRunPreStart.action != reconcileSuspended {
		t.Fatalf("LSF PEND-after-RUN before process start should pause instead of claim, got %#v", pendAfterRunPreStart)
	}
}

type resourceVector struct {
	cpus          int
	memGB         int
	gpuSlices     map[string]int
	licenses      map[string]int
	exclusiveNode bool
}

type resourceRequest struct {
	cpus          int
	memGB         int
	gpuSlice      string
	license       string
	exclusiveNode bool
}

func resourceCanReserve(cap resourceVector, used resourceVector, req resourceRequest) bool {
	if cap.exclusiveNode || used.exclusiveNode || req.exclusiveNode {
		return req.exclusiveNode &&
			!used.exclusiveNode &&
			used.cpus == 0 &&
			used.memGB == 0 &&
			allZero(used.gpuSlices) &&
			allZero(used.licenses)
	}
	if used.cpus+req.cpus > cap.cpus || used.memGB+req.memGB > cap.memGB {
		return false
	}
	if req.gpuSlice != "" && used.gpuSlices[req.gpuSlice] >= cap.gpuSlices[req.gpuSlice] {
		return false
	}
	if req.license != "" && used.licenses[req.license] >= cap.licenses[req.license] {
		return false
	}
	return true
}

func allZero(values map[string]int) bool {
	for _, value := range values {
		if value != 0 {
			return false
		}
	}
	return true
}

func TestSharedExclusiveUsesResourceVectorsNotOnlySlots(t *testing.T) {
	cap := resourceVector{
		cpus:      16,
		memGB:     128,
		gpuSlices: map[string]int{"gpu0": 1, "gpu1": 1, "mig0": 1},
		licenses:  map[string]int{"eda": 1},
	}
	usedGPU0 := resourceVector{cpus: 4, memGB: 16, gpuSlices: map[string]int{"gpu0": 1}, licenses: map[string]int{}}
	if resourceCanReserve(cap, usedGPU0, resourceRequest{cpus: 4, memGB: 16, gpuSlice: "gpu0"}) {
		t.Fatal("shared jobs must not reuse the same GPU/MIG slice")
	}
	if !resourceCanReserve(cap, usedGPU0, resourceRequest{cpus: 4, memGB: 16, gpuSlice: "gpu1"}) {
		t.Fatal("different free GPU slice should be usable")
	}
	if resourceCanReserve(cap, usedGPU0, resourceRequest{exclusiveNode: true}) {
		t.Fatal("exclusive request must not reuse a shared active lease")
	}
	if !resourceCanReserve(cap, resourceVector{gpuSlices: map[string]int{}, licenses: map[string]int{}}, resourceRequest{exclusiveNode: true}) {
		t.Fatal("exclusive request should reserve an empty lease")
	}
	if resourceCanReserve(cap, resourceVector{exclusiveNode: true, gpuSlices: map[string]int{}, licenses: map[string]int{}}, resourceRequest{exclusiveNode: true}) {
		t.Fatal("second exclusive request must not reuse an occupied exclusive lease")
	}
	if resourceCanReserve(cap, resourceVector{memGB: 1, gpuSlices: map[string]int{}, licenses: map[string]int{}}, resourceRequest{exclusiveNode: true}) {
		t.Fatal("exclusive request must not ignore memory-only occupancy")
	}
	if resourceCanReserve(cap, resourceVector{gpuSlices: map[string]int{}, licenses: map[string]int{"eda": 1}}, resourceRequest{exclusiveNode: true}) {
		t.Fatal("exclusive request must not ignore license-only occupancy")
	}
	if resourceCanReserve(cap, resourceVector{cpus: 15, memGB: 120, gpuSlices: map[string]int{}, licenses: map[string]int{}}, resourceRequest{cpus: 2, memGB: 1}) {
		t.Fatal("CPU exhaustion should block shared reservation")
	}
}

type byoAttachInput struct {
	schedulerEnv                bool
	sameSchedulerUser           bool
	coordinatorOwnsAllocation   bool
	explicitProviderKillAllowed bool
	providerRunning             bool
	membershipProof             bool
	sameScope                   bool
	signatureMatch              bool
	doctorPassed                bool
	attested                    bool
}

type byoAttachDecision string

const (
	byoReject  byoAttachDecision = "reject"
	byoTainted byoAttachDecision = "tainted"
	byoAccept  byoAttachDecision = "accept"
)

func decideBYOAttach(in byoAttachInput) byoAttachDecision {
	if !in.schedulerEnv || !in.sameSchedulerUser || !in.providerRunning || !in.membershipProof || !in.sameScope || !in.signatureMatch || !in.doctorPassed {
		return byoReject
	}
	if !in.attested {
		return byoTainted
	}
	return byoAccept
}

func TestBYOAttachRequiresSchedulerOwnershipAndAttestation(t *testing.T) {
	base := byoAttachInput{
		schedulerEnv:      true,
		sameSchedulerUser: true,
		providerRunning:   true,
		membershipProof:   true,
		sameScope:         true,
		signatureMatch:    true,
		doctorPassed:      true,
		attested:          true,
	}
	if got := decideBYOAttach(base); got != byoAccept {
		t.Fatalf("verified BYO attach should accept, got %s", got)
	}
	if got := decideBYOAttach(copyBYO(base, func(in *byoAttachInput) { in.attested = false })); got != byoTainted {
		t.Fatalf("unattested but otherwise valid worker should be tainted, got %s", got)
	}
	for name, in := range map[string]byoAttachInput{
		"outside scheduler": copyBYO(base, func(in *byoAttachInput) { in.schedulerEnv = false }),
		"wrong owner":       copyBYO(base, func(in *byoAttachInput) { in.sameSchedulerUser = false }),
		"not running":       copyBYO(base, func(in *byoAttachInput) { in.providerRunning = false }),
		"wrong cgroup":      copyBYO(base, func(in *byoAttachInput) { in.membershipProof = false }),
		"wrong scope":       copyBYO(base, func(in *byoAttachInput) { in.sameScope = false }),
		"wrong image":       copyBYO(base, func(in *byoAttachInput) { in.signatureMatch = false }),
		"doctor failed":     copyBYO(base, func(in *byoAttachInput) { in.doctorPassed = false }),
	} {
		if got := decideBYOAttach(in); got != byoReject {
			t.Fatalf("%s should reject, got %s", name, got)
		}
	}
}

func byoReleaseMayKillProvider(in byoAttachInput) bool {
	return in.coordinatorOwnsAllocation || in.explicitProviderKillAllowed
}

func TestBYOAttachSeparatesUserOwnershipFromProviderKillPermission(t *testing.T) {
	external := byoAttachInput{
		schedulerEnv:      true,
		sameSchedulerUser: true,
		providerRunning:   true,
		membershipProof:   true,
		sameScope:         true,
		signatureMatch:    true,
		doctorPassed:      true,
		attested:          true,
	}
	if got := decideBYOAttach(external); got != byoAccept {
		t.Fatalf("external BYO job owned by same scheduler user should attach, got %s", got)
	}
	if byoReleaseMayKillProvider(external) {
		t.Fatal("BYO attach must not grant provider kill permission by default")
	}
	if !byoReleaseMayKillProvider(copyBYO(external, func(in *byoAttachInput) { in.explicitProviderKillAllowed = true })) {
		t.Fatal("explicit opt-in should allow provider kill for BYO")
	}
	if !byoReleaseMayKillProvider(copyBYO(external, func(in *byoAttachInput) { in.coordinatorOwnsAllocation = true })) {
		t.Fatal("managed allocation ownership should allow provider kill")
	}
}

func copyBYO(in byoAttachInput, fn func(*byoAttachInput)) byoAttachInput {
	fn(&in)
	return in
}

type jobCapability struct {
	keyID      string
	jobID      string
	attempt    int
	workerID   string
	leaseEpoch int
	claimEpoch int
	secret     []byte
}

type workerVisibleCapability struct {
	keyID      string
	jobID      string
	attempt    int
	workerID   string
	leaseEpoch int
	claimEpoch int
}

func issueJobCapability(jobID string, attempt int, workerID string, leaseEpoch int, claimEpoch int, secret string) jobCapability {
	return jobCapability{
		keyID:      "cap-" + jobID,
		jobID:      jobID,
		attempt:    attempt,
		workerID:   workerID,
		leaseEpoch: leaseEpoch,
		claimEpoch: claimEpoch,
		secret:     []byte(secret),
	}
}

func (c jobCapability) visible() workerVisibleCapability {
	return workerVisibleCapability{
		keyID:      c.keyID,
		jobID:      c.jobID,
		attempt:    c.attempt,
		workerID:   c.workerID,
		leaseEpoch: c.leaseEpoch,
		claimEpoch: c.claimEpoch,
	}
}

func signCapability(c jobCapability, stamp writeStamp) string {
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write([]byte(fmt.Sprintf("%s/%d/%s/%d/%d/%s/%s/%s/%s/%s/%d/%s",
		stamp.jobID,
		stamp.jobAttempt,
		stamp.workerID,
		stamp.leaseEpoch,
		stamp.claimEpoch,
		stamp.providerJobID,
		stamp.allocationID,
		stamp.stepID,
		stamp.runIncarnation,
		stamp.signatureHash,
		stamp.protocolWrite,
		stamp.eventHash,
	)))
	return fmt.Sprintf("%x", mac.Sum(nil))
}

func verifyCapability(c jobCapability, stamp writeStamp) bool {
	if c.jobID != stamp.jobID ||
		c.attempt != stamp.jobAttempt ||
		c.workerID != stamp.workerID ||
		c.leaseEpoch != stamp.leaseEpoch ||
		c.claimEpoch != stamp.claimEpoch ||
		c.keyID != stamp.macKeyID {
		return false
	}
	return hmac.Equal([]byte(stamp.mac), []byte(signCapability(c, stamp)))
}

func TestPerJobCapabilityCannotSignSiblingWorkerTerminal(t *testing.T) {
	capA := issueJobCapability("job-a", 1, "worker-a", 1, 1, "secret-a")
	capB := issueJobCapability("job-b", 1, "worker-b", 1, 1, "secret-b")
	stampA := writeStamp{
		jobID:          "job-a",
		jobAttempt:     1,
		workerID:       "worker-a",
		providerJobID:  "slurm-1",
		allocationID:   "alloc-1",
		stepID:         "step-1",
		runIncarnation: "run-1",
		host:           "host-a",
		leaseEpoch:     1,
		claimEpoch:     1,
		protocolWrite:  1,
		signatureHash:  "sig-a",
		macKeyID:       capA.keyID,
		eventHash:      "event-a",
	}
	stampA.mac = signCapability(capA, stampA)
	if !verifyCapability(capA, stampA) {
		t.Fatal("matching per-job capability should verify its own terminal")
	}
	forged := stampA
	forged.jobID = "job-b"
	forged.workerID = "worker-b"
	forged.macKeyID = capB.keyID
	forged.mac = signCapability(capA, forged)
	if verifyCapability(capB, forged) {
		t.Fatal("sibling worker capability must not verify a terminal signed with another job secret")
	}
	tampered := stampA
	tampered.signatureHash = "sig-b"
	if verifyCapability(capA, tampered) {
		t.Fatal("mutating a bound field should invalidate the capability MAC")
	}
	visible := capA.visible()
	if fmt.Sprintf("%#v", visible) == fmt.Sprintf("%#v", capA) || strings.Contains(fmt.Sprintf("%#v", visible), "secret-a") {
		t.Fatalf("worker-visible capability must not expose verifier secret: %#v", visible)
	}
}

type coordinatorMutation struct {
	actor        beaconActor
	op           beaconOperation
	actorEpoch   int
	currentEpoch int
}

func coordinatorMutationAllowed(in coordinatorMutation) bool {
	if !operationAllowed(in.actor, in.op) {
		return false
	}
	if in.op == opReadState {
		return true
	}
	return in.actor == actorActiveCoordinator && in.actorEpoch == in.currentEpoch
}

func TestCoordinatorSplitBrainRequiresCurrentControlEpoch(t *testing.T) {
	current := coordinatorMutation{actor: actorActiveCoordinator, op: opDispatch, actorEpoch: 5, currentEpoch: 5}
	if !coordinatorMutationAllowed(current) {
		t.Fatal("current active coordinator should dispatch")
	}
	stale := current
	stale.actorEpoch = 4
	for _, op := range []beaconOperation{opDispatch, opCleanup, opQueueTeams, opFlushTeams} {
		stale.op = op
		if coordinatorMutationAllowed(stale) {
			t.Fatalf("stale coordinator epoch must not %s", op)
		}
	}
	stale.op = opReadState
	if !coordinatorMutationAllowed(stale) {
		t.Fatal("stale coordinator may still read status")
	}
}

type destructiveCoordinatorOp string

const (
	opProfileConfirm   destructiveCoordinatorOp = "profile_confirm"
	opProfileSwitch    destructiveCoordinatorOp = "profile_switch"
	opAllocationCancel destructiveCoordinatorOp = "allocation_cancel"
	opAllocationKill   destructiveCoordinatorOp = "allocation_kill"
	opTerminalAccept   destructiveCoordinatorOp = "terminal_accept"
	opArtifactAccept   destructiveCoordinatorOp = "artifact_accept"
	opOutboxSend       destructiveCoordinatorOp = "outbox_send"
	opCodexPromote     destructiveCoordinatorOp = "codex_promote"
)

func destructiveCoordinatorOpAllowed(actorEpoch, currentEpoch int) bool {
	return actorEpoch == currentEpoch
}

func TestStaleCoordinatorEpochCannotPerformDestructiveSideEffects(t *testing.T) {
	for _, op := range []destructiveCoordinatorOp{
		opProfileConfirm,
		opProfileSwitch,
		opAllocationCancel,
		opAllocationKill,
		opTerminalAccept,
		opArtifactAccept,
		opOutboxSend,
		opCodexPromote,
	} {
		if destructiveCoordinatorOpAllowed(4, 5) {
			t.Fatalf("stale coordinator epoch must not perform %s", op)
		}
		if !destructiveCoordinatorOpAllowed(5, 5) {
			t.Fatalf("current coordinator epoch should perform %s", op)
		}
	}
}

type commandScope string
type beaconCommand string
type commandChannel string

const (
	scopeControlChat commandScope = "control_chat"
	scopeWorkChat    commandScope = "work_chat"

	cmdChannelTeams commandChannel = "teams"
	cmdChannelTUI   commandChannel = "tui"
	cmdChannelCLI   commandChannel = "cli"

	commandNew      beaconCommand = "new"
	commandStatus   beaconCommand = "status"
	commandProfile  beaconCommand = "profile"
	commandMachine  beaconCommand = "machine"
	commandSwitch   beaconCommand = "switch_profile"
	commandHardKill beaconCommand = "hard_kill"
)

func commandAllowedInScope(cmd beaconCommand, scope commandScope) bool {
	switch cmd {
	case commandProfile, commandMachine:
		return scope == scopeControlChat
	case commandNew, commandStatus, commandSwitch:
		return scope == scopeWorkChat
	case commandHardKill:
		return scope == scopeControlChat
	default:
		return false
	}
}

func TestBeaconTeamsCommandScopePreventsWrongChatMutations(t *testing.T) {
	for _, tc := range []struct {
		cmd   beaconCommand
		scope commandScope
		want  bool
	}{
		{commandProfile, scopeControlChat, true},
		{commandProfile, scopeWorkChat, false},
		{commandNew, scopeWorkChat, true},
		{commandNew, scopeControlChat, false},
		{commandSwitch, scopeWorkChat, true},
		{commandMachine, scopeControlChat, true},
		{commandHardKill, scopeWorkChat, false},
	} {
		if got := commandAllowedInScope(tc.cmd, tc.scope); got != tc.want {
			t.Fatalf("%s in %s: got %v want %v", tc.cmd, tc.scope, got, tc.want)
		}
	}
}

func commandContextValid(channel commandChannel, scope commandScope, selectedConversation bool, explicitConversation bool) bool {
	switch channel {
	case cmdChannelTeams:
		return scope == scopeControlChat || scope == scopeWorkChat
	case cmdChannelTUI:
		return scope == scopeControlChat || selectedConversation
	case cmdChannelCLI:
		return scope == scopeControlChat || explicitConversation
	default:
		return false
	}
}

func TestCommandContextAcrossTeamsTUIAndCLI(t *testing.T) {
	tests := []struct {
		name                 string
		channel              commandChannel
		scope                commandScope
		selectedConversation bool
		explicitConversation bool
		want                 bool
	}{
		{name: "Teams work chat has conversation", channel: cmdChannelTeams, scope: scopeWorkChat, want: true},
		{name: "TUI selected conversation", channel: cmdChannelTUI, scope: scopeWorkChat, selectedConversation: true, want: true},
		{name: "TUI no selected conversation", channel: cmdChannelTUI, scope: scopeWorkChat},
		{name: "CLI explicit conversation", channel: cmdChannelCLI, scope: scopeWorkChat, explicitConversation: true, want: true},
		{name: "CLI missing conversation", channel: cmdChannelCLI, scope: scopeWorkChat},
		{name: "CLI global profile", channel: cmdChannelCLI, scope: scopeControlChat, want: true},
	}
	for _, tt := range tests {
		if got := commandContextValid(tt.channel, tt.scope, tt.selectedConversation, tt.explicitConversation); got != tt.want {
			t.Fatalf("%s: got %v want %v", tt.name, got, tt.want)
		}
	}
}

type machinePreview struct {
	machineID     string
	leaseID       string
	providerJobID string
	chats         []string
	jobs          []string
	confirmation  string
	externalOwned bool
}

func renderMachinePreview(p machinePreview) string {
	return strings.Join([]string{
		"machine=" + p.machineID,
		"lease=" + p.leaseID,
		"provider_job=" + p.providerJobID,
		"chats=" + strings.Join(p.chats, ","),
		"jobs=" + strings.Join(p.jobs, ","),
		"confirm=" + p.confirmation,
	}, " ")
}

func hardKillPreviewValid(p machinePreview, exactLeaseID string, token string) bool {
	return !p.externalOwned &&
		strings.TrimSpace(exactLeaseID) == p.leaseID &&
		strings.TrimSpace(token) == p.confirmation &&
		len(p.jobs) > 0
}

func TestMachineReleaseKillPreviewShowsImpactBeforeDestructiveMutation(t *testing.T) {
	preview := machinePreview{
		machineID:     "gpu-a",
		leaseID:       "lease-1",
		providerJobID: "slurm-123",
		chats:         []string{"chat-a", "chat-b"},
		jobs:          []string{"job-1"},
		confirmation:  "KILL-lease-1",
	}
	text := renderMachinePreview(preview)
	for _, want := range []string{"machine=gpu-a", "lease=lease-1", "provider_job=slurm-123", "chats=chat-a,chat-b", "jobs=job-1", "confirm=KILL-lease-1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("preview %q missing %q", text, want)
		}
	}
	if !hardKillPreviewValid(preview, "lease-1", "KILL-lease-1") {
		t.Fatal("exact lease and confirmation should permit hard-kill preview")
	}
	if hardKillPreviewValid(preview, "lease-2", "KILL-lease-1") || hardKillPreviewValid(preview, "lease-1", "wrong") {
		t.Fatal("hard kill should require exact lease id and confirmation token")
	}
	preview.externalOwned = true
	if hardKillPreviewValid(preview, "lease-1", "KILL-lease-1") {
		t.Fatal("external/BYO allocation should not hard-kill provider job by default")
	}
}

type commandDeduper struct {
	results map[string]string
	kills   int
}

func (d *commandDeduper) run(messageID, normalizedCommand, confirmationToken string) string {
	if d.results == nil {
		d.results = map[string]string{}
	}
	key := messageID + "\x00" + normalizedCommand + "\x00" + confirmationToken
	if result, ok := d.results[key]; ok {
		return result
	}
	result := "operation-" + fmt.Sprint(len(d.results)+1)
	if strings.Contains(normalizedCommand, "kill") {
		d.kills++
	}
	d.results[key] = result
	return result
}

func TestTeamsMobileDuplicateCommandsAreIdempotent(t *testing.T) {
	var dedupe commandDeduper
	for _, cmd := range []string{
		"new --beacon-profile gpu",
		"beacon profile confirm gpu",
		"beacon switch-profile cpu",
		"beacon machine release lease-1",
	} {
		first := dedupe.run("teams-msg-"+cmd, cmd, "")
		second := dedupe.run("teams-msg-"+cmd, cmd, "")
		if first != second {
			t.Fatalf("duplicate %q should reuse existing operation, first=%q second=%q", cmd, first, second)
		}
	}
	kill := dedupe.run("teams-msg-2", "beacon machine kill lease-1", "KILL-LEASE-1")
	killAgain := dedupe.run("teams-msg-2", "beacon machine kill lease-1", "KILL-LEASE-1")
	if kill != killAgain || dedupe.kills != 1 {
		t.Fatalf("duplicate hard kill must not run twice, kill=%q again=%q count=%d", kill, killAgain, dedupe.kills)
	}
	if got := dedupe.run("teams-msg-2", "beacon machine kill lease-1", "DIFFERENT"); got == kill {
		t.Fatalf("different confirmation token should not reuse destructive result, got %q", got)
	}
}

type terminalStore struct {
	envelopeHash string
	outboxCount  int
	auditCount   int
	quarantined  bool
}

func (s *terminalStore) acceptTerminal(envelope []byte, valid bool) recoveryAction {
	hash := fmt.Sprintf("%x", sha256.Sum256(envelope))
	if s.envelopeHash == "" {
		if !valid {
			s.quarantined = true
			return recoverQuarantine
		}
		s.envelopeHash = hash
		s.outboxCount++
		s.auditCount++
		return recoverComplete
	}
	if s.envelopeHash == hash {
		if s.outboxCount == 0 {
			s.outboxCount++
		}
		return recoverComplete
	}
	s.quarantined = true
	return recoverQuarantine
}

func TestDuplicateTerminalIsIdempotentOnlyForIdenticalEnvelope(t *testing.T) {
	var store terminalStore
	envelope := []byte(`{"job":"job-1","seq":3,"body":"done"}`)
	if got := store.acceptTerminal(envelope, true); got != recoverComplete {
		t.Fatalf("first valid terminal should complete, got %s", got)
	}
	if got := store.acceptTerminal(envelope, true); got != recoverComplete {
		t.Fatalf("identical duplicate terminal should be idempotent, got %s", got)
	}
	if store.outboxCount != 1 || store.auditCount != 1 {
		t.Fatalf("identical duplicate must not enqueue/audit twice, outbox=%d audit=%d", store.outboxCount, store.auditCount)
	}
	nearDuplicate := []byte(`{"job":"job-1","seq":3,"body":"done","time":"later"}`)
	if got := store.acceptTerminal(nearDuplicate, true); got != recoverQuarantine || !store.quarantined {
		t.Fatalf("near duplicate with different bytes should quarantine, got=%s quarantined=%v", got, store.quarantined)
	}
	if store.outboxCount != 1 {
		t.Fatalf("conflicting duplicate must not overwrite/enqueue another final, outbox=%d", store.outboxCount)
	}
}

func TestTerminalRecoveryAcrossCoordinatorRestartQueuesOneProtectedOutbox(t *testing.T) {
	envelope := []byte(`{"job":"job-1","seq":3,"body":"done"}`)
	for _, phase := range []string{"before_result_ingest", "after_result_ingest", "after_outbox_enqueue"} {
		store := terminalStore{}
		if phase == "after_result_ingest" {
			_ = store.acceptTerminal(envelope, true)
			store.outboxCount = 0
		}
		if phase == "after_outbox_enqueue" {
			_ = store.acceptTerminal(envelope, true)
		}
		_ = store.acceptTerminal(envelope, true)
		if store.outboxCount != 1 {
			t.Fatalf("%s should leave exactly one protected outbox, got %d", phase, store.outboxCount)
		}
	}
}

type profileDraftStatus struct {
	confirmed         bool
	proxyResolved     bool
	providerPreviewed bool
	doctorPassed      bool
}

func profileDraftReasons(in profileDraftStatus) []string {
	var reasons []string
	if !in.confirmed {
		reasons = append(reasons, "needs confirm")
	}
	if !in.proxyResolved {
		reasons = append(reasons, "proxy unresolved")
	}
	if !in.providerPreviewed {
		reasons = append(reasons, "provider preview missing")
	}
	if !in.doctorPassed {
		reasons = append(reasons, "doctor failed")
	}
	return reasons
}

func TestDraftProfilesAreListableAndNotSelectable(t *testing.T) {
	reasons := profileDraftReasons(profileDraftStatus{confirmed: true, proxyResolved: false, providerPreviewed: true, doctorPassed: false})
	joined := strings.Join(reasons, ",")
	if !strings.Contains(joined, "proxy unresolved") || !strings.Contains(joined, "doctor failed") {
		t.Fatalf("draft reasons should be actionable, got %q", joined)
	}
	if len(profileDraftReasons(profileDraftStatus{confirmed: true, proxyResolved: true, providerPreviewed: true, doctorPassed: true})) != 0 {
		t.Fatal("ready profile should have no draft reasons")
	}
}

type draftProfileAction string

const (
	draftCreate  draftProfileAction = "create"
	draftResume  draftProfileAction = "resume"
	draftEdit    draftProfileAction = "edit"
	draftDoctor  draftProfileAction = "doctor"
	draftConfirm draftProfileAction = "confirm"
	draftDelete  draftProfileAction = "delete"
	draftSelect  draftProfileAction = "select"
)

func draftProfileNext(status profileDraftStatus, action draftProfileAction) profileDraftStatus {
	switch action {
	case draftCreate, draftResume:
		return status
	case draftEdit:
		status.providerPreviewed = false
		status.doctorPassed = false
	case draftDoctor:
		status.doctorPassed = true
	case draftConfirm:
		status.confirmed = true
	case draftDelete:
		return profileDraftStatus{}
	}
	return status
}

func draftProfileSelectable(status profileDraftStatus) bool {
	return len(profileDraftReasons(status)) == 0
}

func TestDraftProfileLifecycleRejectsSelectionUntilReady(t *testing.T) {
	status := draftProfileNext(profileDraftStatus{proxyResolved: true}, draftCreate)
	if draftProfileSelectable(status) {
		t.Fatal("partial created profile must remain draft")
	}
	status.providerPreviewed = true
	status = draftProfileNext(status, draftDoctor)
	status = draftProfileNext(status, draftConfirm)
	if !draftProfileSelectable(status) {
		t.Fatalf("confirmed previewed doctored profile should be selectable, reasons=%v", profileDraftReasons(status))
	}
	status = draftProfileNext(status, draftEdit)
	if draftProfileSelectable(status) {
		t.Fatal("editing a ready profile should return it to draft until preview/doctor rerun")
	}
}

func lsfProfilePreviewReady(queueName string, sitePolicyDerivesResources bool, explicitAdvancedApproved bool) bool {
	if strings.TrimSpace(queueName) == "" {
		return false
	}
	return sitePolicyDerivesResources || explicitAdvancedApproved
}

func TestLSFQueueOnlyProfileRequiresSitePolicyDerivation(t *testing.T) {
	if !lsfProfilePreviewReady("o_pri_interactive", true, false) {
		t.Fatal("queue-only LSF wizard should pass when site policy derives resource shape")
	}
	if lsfProfilePreviewReady("o_pri_interactive", false, false) {
		t.Fatal("queue-only LSF profile should remain draft when provider derivation is incomplete")
	}
	if !lsfProfilePreviewReady("o_pri_interactive", false, true) {
		t.Fatal("locally approved advanced LSF fields should satisfy provider preview")
	}
}

func renderSwitchAck(current, future, forkCommand string) string {
	return "current turn stays " + current + "; future turns use " + future + "; fork with " + forkCommand
}

func TestProfileSwitchAckShowsCurrentFutureAndForkCommand(t *testing.T) {
	msg := renderSwitchAck("beacon:gpu", "beacon:cpu", "beacon switch-profile cpu --fork")
	for _, want := range []string{"current turn stays beacon:gpu", "future turns use beacon:cpu", "fork with beacon switch-profile cpu --fork"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("switch ack %q missing %q", msg, want)
		}
	}
}

type executionStatusView struct {
	currentTarget  string
	pendingTarget  string
	turnSnapshot   string
	jobID          string
	leaseID        string
	machineID      string
	providerJobID  string
	profile        string
	proxy          string
	isolation      isolation
	providerState  string
	providerReason string
}

func renderExecutionStatus(view executionStatusView) string {
	proxy := strings.TrimSpace(view.proxy)
	if proxy == "" {
		proxy = "none"
	}
	return strings.Join([]string{
		"current_target=" + view.currentTarget,
		"pending_target=" + view.pendingTarget,
		"turn_snapshot=" + view.turnSnapshot,
		"job=" + view.jobID,
		"lease=" + view.leaseID,
		"machine=" + view.machineID,
		"provider_job=" + view.providerJobID,
		"profile=" + view.profile,
		"proxy=" + proxy,
		"isolation=" + string(view.isolation),
		"provider_state=" + view.providerState,
		"provider_reason=" + view.providerReason,
	}, " ")
}

func TestExecutionStatusSeparatesTargetAndProxyRoute(t *testing.T) {
	for _, tc := range []executionStatusView{
		{currentTarget: "local", isolation: isolationShared},
		{currentTarget: "local", proxy: "jump-a", isolation: isolationShared},
		{currentTarget: "beacon:gpu", proxy: "jump-a", isolation: isolationShared},
	} {
		msg := renderExecutionStatus(tc)
		if !strings.Contains(msg, "current_target="+tc.currentTarget) || !strings.Contains(msg, "proxy=") {
			t.Fatalf("status should show separate target/proxy fields, got %q", msg)
		}
	}
}

func TestExecutionStatusIncludesPendingSnapshotAndProviderDetails(t *testing.T) {
	msg := renderExecutionStatus(executionStatusView{
		currentTarget:  "beacon:gpu",
		pendingTarget:  "beacon:cpu",
		turnSnapshot:   "beacon:gpu@sig-a",
		jobID:          "job-1",
		leaseID:        "lease-1",
		machineID:      "gpu-a",
		providerJobID:  "slurm-123",
		profile:        "gpu",
		proxy:          "jump-a",
		isolation:      isolationExclusive,
		providerState:  "PD",
		providerReason: "Resources",
	})
	for _, want := range []string{
		"current_target=beacon:gpu",
		"pending_target=beacon:cpu",
		"turn_snapshot=beacon:gpu@sig-a",
		"job=job-1",
		"lease=lease-1",
		"machine=gpu-a",
		"provider_job=slurm-123",
		"profile=gpu",
		"proxy=jump-a",
		"isolation=exclusive",
		"provider_state=PD",
		"provider_reason=Resources",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("status %q missing %q", msg, want)
		}
	}
}

type newTargetInput struct {
	channel               commandChannel
	legacyProxyPreference string
	explicitBeaconProfile string
	explicitBeaconFailed  bool
}

type newTargetResolution struct {
	target        executionTarget
	proxyRoute    string
	requiresError bool
}

func resolveNewTargetAcrossChannels(in newTargetInput) newTargetResolution {
	if strings.TrimSpace(in.explicitBeaconProfile) != "" {
		return newTargetResolution{target: targetBeacon, proxyRoute: in.legacyProxyPreference, requiresError: in.explicitBeaconFailed}
	}
	return newTargetResolution{target: targetLocal, proxyRoute: in.legacyProxyPreference}
}

func TestNewDefaultsLocalAcrossTeamsTUIAndCLIDespiteProxyPreference(t *testing.T) {
	for _, channel := range []commandChannel{cmdChannelTeams, cmdChannelTUI, cmdChannelCLI} {
		got := resolveNewTargetAcrossChannels(newTargetInput{channel: channel, legacyProxyPreference: "jump-a"})
		if got.target != targetLocal || got.proxyRoute != "jump-a" || got.requiresError {
			t.Fatalf("%s new should stay local while preserving proxy route, got %#v", channel, got)
		}
		explicit := resolveNewTargetAcrossChannels(newTargetInput{channel: channel, legacyProxyPreference: "jump-a", explicitBeaconProfile: "gpu", explicitBeaconFailed: true})
		if explicit.target != targetBeacon || !explicit.requiresError {
			t.Fatalf("%s explicit beacon failure must surface error without local fallback, got %#v", channel, explicit)
		}
	}
}

type artifactRef struct {
	sharedRoot           string
	path                 string
	declaredHash         string
	actualHash           string
	size                 int64
	limit                int64
	symlink              bool
	hardlinkOutOfRoot    bool
	changedAfterAccept   bool
	workerDeliveryField  bool
	openedNoFollow       bool
	fstatStable          bool
	hashFromOpenedFile   bool
	stagedFromOpenedFile bool
	hardlinkCount        int
}

func artifactIngestAllowed(ref artifactRef) bool {
	if ref.workerDeliveryField || ref.symlink || ref.hardlinkOutOfRoot || ref.changedAfterAccept {
		return false
	}
	if !ref.openedNoFollow || !ref.fstatStable || !ref.hashFromOpenedFile || !ref.stagedFromOpenedFile || ref.hardlinkCount > 1 {
		return false
	}
	if ref.size <= 0 || ref.size > ref.limit {
		return false
	}
	if strings.TrimSpace(ref.declaredHash) == "" || ref.declaredHash != ref.actualHash {
		return false
	}
	cleanRoot := filepath.Clean(ref.sharedRoot)
	cleanPath := filepath.Clean(ref.path)
	return cleanPath != cleanRoot && strings.HasPrefix(cleanPath, cleanRoot+string(filepath.Separator))
}

func TestArtifactIngestionRejectsWorkerDeliveryFieldsAndTampering(t *testing.T) {
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte("artifact")))
	base := artifactRef{
		sharedRoot:           "/shared/beacon/jobs/job-1/artifacts",
		path:                 "/shared/beacon/jobs/job-1/artifacts/report.txt",
		declaredHash:         hash,
		actualHash:           hash,
		size:                 1024,
		limit:                4096,
		openedNoFollow:       true,
		fstatStable:          true,
		hashFromOpenedFile:   true,
		stagedFromOpenedFile: true,
		hardlinkCount:        1,
	}
	if !artifactIngestAllowed(base) {
		t.Fatal("valid shared artifact should ingest")
	}
	for name, ref := range map[string]artifactRef{
		"worker attachment path":   copyArtifact(base, func(r *artifactRef) { r.workerDeliveryField = true }),
		"symlink":                  copyArtifact(base, func(r *artifactRef) { r.symlink = true; r.openedNoFollow = false }),
		"hardlink":                 copyArtifact(base, func(r *artifactRef) { r.hardlinkOutOfRoot = true }),
		"hardlink count":           copyArtifact(base, func(r *artifactRef) { r.hardlinkCount = 2 }),
		"changed":                  copyArtifact(base, func(r *artifactRef) { r.changedAfterAccept = true; r.fstatStable = false }),
		"hash not from opened fd":  copyArtifact(base, func(r *artifactRef) { r.hashFromOpenedFile = false }),
		"stage not from opened fd": copyArtifact(base, func(r *artifactRef) { r.stagedFromOpenedFile = false }),
		"too large":                copyArtifact(base, func(r *artifactRef) { r.size = 4097 }),
		"hash mismatch":            copyArtifact(base, func(r *artifactRef) { r.actualHash = "bad" }),
		"out of root":              copyArtifact(base, func(r *artifactRef) { r.path = "/shared/beacon/jobs/job-2/artifacts/report.txt" }),
	} {
		if artifactIngestAllowed(ref) {
			t.Fatalf("%s should be rejected: %#v", name, ref)
		}
	}
}

func copyArtifact(in artifactRef, fn func(*artifactRef)) artifactRef {
	fn(&in)
	return in
}

type sharedFSRecord struct {
	completeManifest bool
	partialJSON      bool
	oversizedJSON    bool
	futureVersion    bool
	tempFile         bool
	mtimeFresh       bool
	signedSeqFresh   bool
}

func ingestSharedFSRecord(in sharedFSRecord) reconcileAction {
	if in.tempFile {
		return reconcileDrain
	}
	if !in.completeManifest || in.partialJSON || in.oversizedJSON || in.futureVersion {
		return reconcileQuarantine
	}
	if in.signedSeqFresh {
		return reconcileAccept
	}
	if in.mtimeFresh {
		return reconcileDrain
	}
	return reconcileDrain
}

func TestSharedFSFaultsDoNotUseMTimeAsAuthority(t *testing.T) {
	if got := ingestSharedFSRecord(sharedFSRecord{completeManifest: true, mtimeFresh: false, signedSeqFresh: true}); got != reconcileAccept {
		t.Fatalf("fresh signed sequence should win over stale mtime, got %s", got)
	}
	for name, rec := range map[string]sharedFSRecord{
		"partial":     {completeManifest: true, partialJSON: true, signedSeqFresh: true},
		"oversized":   {completeManifest: true, oversizedJSON: true, signedSeqFresh: true},
		"future":      {completeManifest: true, futureVersion: true, signedSeqFresh: true},
		"no manifest": {signedSeqFresh: true},
	} {
		if got := ingestSharedFSRecord(rec); got != reconcileQuarantine {
			t.Fatalf("%s should quarantine, got %s", name, got)
		}
	}
	if got := ingestSharedFSRecord(sharedFSRecord{tempFile: true, mtimeFresh: true}); got != reconcileDrain {
		t.Fatalf("orphan temp files should be ignored/reaped later, got %s", got)
	}
}

type storeWritePhase string

const (
	storeEventAppend    storeWritePhase = "event_append"
	storeTerminalTemp   storeWritePhase = "terminal_temp"
	storeTerminalFsync  storeWritePhase = "terminal_fsync"
	storeTerminalRename storeWritePhase = "terminal_rename"
	storeQuarantineMark storeWritePhase = "quarantine_marker"
	storeArtifactStage  storeWritePhase = "artifact_stage"
	storeOutboxEnqueue  storeWritePhase = "outbox_enqueue"
)

func classifyENOSPCAfterStart(phase storeWritePhase, reserveAvailable bool) failureOutcome {
	if reserveAvailable {
		return outcomeAmbiguous
	}
	switch phase {
	case storeEventAppend, storeTerminalTemp, storeTerminalFsync, storeTerminalRename, storeQuarantineMark, storeArtifactStage, storeOutboxEnqueue:
		return outcomeStoreAttention
	default:
		return outcomeAmbiguous
	}
}

func TestENOSPCAfterStartNeedsStoreAttentionAcrossWritePhases(t *testing.T) {
	for _, phase := range []storeWritePhase{
		storeEventAppend,
		storeTerminalTemp,
		storeTerminalFsync,
		storeTerminalRename,
		storeQuarantineMark,
		storeArtifactStage,
		storeOutboxEnqueue,
	} {
		if got := classifyENOSPCAfterStart(phase, false); got != outcomeStoreAttention {
			t.Fatalf("%s should require store attention when reserve is gone, got %s", phase, got)
		}
	}
}

type upgradeOperation string

const (
	upgradeHelperReload       upgradeOperation = "helper_reload"
	upgradeHelperRestart      upgradeOperation = "helper_restart"
	upgradePendingReplacement upgradeOperation = "pending_replacement"
	upgradePrelistenCodex     upgradeOperation = "prelisten_codex"
	upgradeBeaconCodexTarget  upgradeOperation = "beacon_codex_target"
)

type operationUpgradeInput struct {
	op                      upgradeOperation
	queuedTeamsTurns        int
	runningTeamsTurns       int
	activeBeaconSameTarget  int
	activeBeaconOtherTarget int
	queuedSameCodexTarget   int
	protectedOutbox         int
}

func operationUpgradeBlocked(in operationUpgradeInput) bool {
	if in.runningTeamsTurns > 0 || in.protectedOutbox > 0 {
		return true
	}
	switch in.op {
	case upgradeHelperReload, upgradeHelperRestart, upgradePendingReplacement:
		return in.activeBeaconSameTarget+in.activeBeaconOtherTarget > 0
	case upgradePrelistenCodex:
		return in.queuedTeamsTurns+in.activeBeaconSameTarget+in.activeBeaconOtherTarget+in.queuedSameCodexTarget > 0
	case upgradeBeaconCodexTarget:
		return in.activeBeaconSameTarget+in.queuedSameCodexTarget > 0
	default:
		return true
	}
}

func TestUpgradeBlockersAreOperationSpecific(t *testing.T) {
	if operationUpgradeBlocked(operationUpgradeInput{op: upgradeHelperRestart, queuedTeamsTurns: 1}) {
		t.Fatal("helper restart may preserve queued Teams turns")
	}
	if !operationUpgradeBlocked(operationUpgradeInput{op: upgradeHelperRestart, runningTeamsTurns: 1}) {
		t.Fatal("helper restart must block running Teams turns")
	}
	if !operationUpgradeBlocked(operationUpgradeInput{op: upgradePrelistenCodex, queuedTeamsTurns: 1}) {
		t.Fatal("pre-listen Codex upgrade must block queued Teams turns")
	}
	if !operationUpgradeBlocked(operationUpgradeInput{op: upgradeBeaconCodexTarget, queuedSameCodexTarget: 1}) {
		t.Fatal("per-target Codex upgrade must block queued work on same target")
	}
	if operationUpgradeBlocked(operationUpgradeInput{op: upgradeBeaconCodexTarget, activeBeaconOtherTarget: 1}) {
		t.Fatal("per-target Codex upgrade should not block on other install target")
	}
}

type auditEntry struct {
	seq      int
	prevHash string
	action   string
	target   string
	secret   string
	hash     string
}

func appendAudit(seq int, prevHash, action, target, secret string) auditEntry {
	payload := fmt.Sprintf("%d\x00%s\x00%s\x00%s", seq, action, target, prevHash)
	return auditEntry{
		seq:      seq,
		prevHash: prevHash,
		action:   action,
		target:   target,
		secret:   "[redacted]",
		hash:     fmt.Sprintf("%x", sha256.Sum256([]byte(payload))),
	}
}

type auditHead struct {
	seq  int
	hash string
}

func auditChainValid(entries []auditEntry, head auditHead) bool {
	prev := ""
	seq := 0
	for _, entry := range entries {
		seq++
		if entry.seq != seq || entry.prevHash != prev || entry.secret != "[redacted]" {
			return false
		}
		want := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", entry.seq, entry.action, entry.target, entry.prevHash))))
		if entry.hash != want {
			return false
		}
		prev = entry.hash
	}
	return head.seq == seq && head.hash == prev
}

func TestAuditLogRedactsSecretsAndDetectsReorder(t *testing.T) {
	first := appendAudit(1, "", "profile_confirm", "gpu", "token-a")
	second := appendAudit(2, first.hash, "worker_claim", "job-1", "token-b")
	head := auditHead{seq: 2, hash: second.hash}
	if !auditChainValid([]auditEntry{first, second}, head) {
		t.Fatal("valid audit chain should verify")
	}
	if auditChainValid([]auditEntry{second, first}, head) {
		t.Fatal("reordered audit chain should fail")
	}
	if auditChainValid([]auditEntry{first}, head) {
		t.Fatal("tail truncation should fail when checked against anchored head")
	}
	duplicateSeq := second
	duplicateSeq.seq = 1
	duplicateSeq.hash = fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", duplicateSeq.seq, duplicateSeq.action, duplicateSeq.target, duplicateSeq.prevHash))))
	if auditChainValid([]auditEntry{first, duplicateSeq}, auditHead{seq: 1, hash: duplicateSeq.hash}) {
		t.Fatal("duplicate sequence should fail audit verification")
	}
	if first.secret != "[redacted]" || second.secret != "[redacted]" {
		t.Fatalf("audit log must redact secrets: %#v %#v", first, second)
	}
}

func renderBeaconError(phase, target, providerJob, providerState, providerReason, conversationID, jobID, retry, next string) string {
	return "phase=" + phase +
		" target=" + target +
		" provider_job=" + providerJob +
		" provider_state=" + providerState +
		" provider_reason=" + providerReason +
		" conversation=" + conversationID +
		" job=" + jobID +
		" retry=" + retry +
		" next=" + next
}

func TestBeaconFailureMessagesContainActionableFields(t *testing.T) {
	cases := []struct {
		phase          string
		target         string
		providerJob    string
		providerState  string
		providerReason string
		retry          string
		next           string
	}{
		{phase: "profile", target: "beacon:gpu", providerState: "invalid", providerReason: "draft", retry: "after-edit", next: "beacon profile status gpu"},
		{phase: "allocation", target: "beacon:gpu", providerJob: "slurm-123", providerState: "PD", providerReason: "Resources", retry: "auto", next: "beacon status"},
		{phase: "bootstrap", target: "beacon:gpu", providerJob: "slurm-123", providerState: "R", providerReason: "missing shared root", retry: "unsafe", next: "beacon status --job job-1"},
		{phase: "result", target: "beacon:gpu", providerJob: "slurm-123", providerState: "OOM", providerReason: "disk full", retry: "fork-required", next: "beacon retry --fork job-1"},
	}
	for _, tc := range cases {
		msg := renderBeaconError(tc.phase, tc.target, tc.providerJob, tc.providerState, tc.providerReason, "conv-1", "job-1", tc.retry, tc.next)
		for _, want := range []string{
			"phase=" + tc.phase,
			"target=" + tc.target,
			"provider_state=" + tc.providerState,
			"provider_reason=" + tc.providerReason,
			"conversation=conv-1",
			"job=job-1",
			"retry=" + tc.retry,
			"next=" + tc.next,
		} {
			if !strings.Contains(msg, want) {
				t.Fatalf("message %q missing %q", msg, want)
			}
		}
	}
}

func TestBeaconOutboxUsesRealTeamsStoreUpgradeContract(t *testing.T) {
	now := time.Now()
	state := teamstore.State{ChatRateLimits: map[string]teamstore.ChatRateLimitState{}}
	progress := teamstore.OutboxMessage{ID: "progress", TeamsChatID: "chat-1", Kind: "progress-beacon-job", Status: teamstore.OutboxStatusQueued, UpgradeNonBlocking: true}
	if !teamstore.OutboxDeliveryTransient(progress) || teamstore.OutboxBlocksUpgrade(state, progress, now) {
		t.Fatalf("beacon progress should be transient and non-blocking: %#v", progress)
	}
	for _, msg := range []teamstore.OutboxMessage{
		{ID: "final", TeamsChatID: "chat-1", Kind: "final-beacon-answer", Status: teamstore.OutboxStatusQueued, UpgradeNonBlocking: true},
		{ID: "artifact", TeamsChatID: "chat-1", Kind: "helper", AttachmentPath: "/tmp/report.txt", Status: teamstore.OutboxStatusQueued, UpgradeNonBlocking: true},
		{ID: "attention", TeamsChatID: "chat-1", Kind: "final-beacon-needs-attention", Status: teamstore.OutboxStatusQueued, UpgradeNonBlocking: true},
		{ID: "completed", TeamsChatID: "chat-1", Kind: "helper", NotificationKind: "turn_completed", Status: teamstore.OutboxStatusQueued, UpgradeNonBlocking: true},
	} {
		if !teamstore.OutboxBlocksUpgrade(state, msg, now) {
			t.Fatalf("%s should block upgrade under real Teams outbox rules: %#v", msg.ID, msg)
		}
	}
	sent := teamstore.OutboxMessage{ID: "sent-final", TeamsChatID: "chat-1", Kind: "final-beacon-answer", Status: teamstore.OutboxStatusSent, TeamsMessageID: "teams-1"}
	if teamstore.OutboxBlocksUpgrade(state, sent, now) {
		t.Fatalf("sent protected message should not block upgrade: %#v", sent)
	}
}
