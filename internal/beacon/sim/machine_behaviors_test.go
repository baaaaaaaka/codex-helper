package sim

import "testing"

type simulatedMachine struct {
	id             string
	leaseState     leaseState
	protocol       protocolMode
	remainingTTL   int
	requiredTTL    int
	heartbeatFresh bool
	scheduler      schedulerStatus
	capacity       int
	used           int
	leaseEpoch     int
}

func (m simulatedMachine) canClaimQueuedJob() bool {
	if reconcileLease(reconcileInput{
		heartbeatFresh: m.heartbeatFresh,
		scheduler:      m.scheduler,
		remainingTTL:   m.remainingTTL,
		requiredTTL:    m.requiredTTL,
	}) != reconcileAccept {
		return false
	}
	return canClaim(claimInput{
		leaseState:     m.leaseState,
		protocol:       m.protocol,
		remainingTTL:   m.remainingTTL,
		requiredTTL:    m.requiredTTL,
		signatureMatch: true,
		slotAvailable:  m.used < m.capacity,
		jobState:       jobQueued,
	})
}

func TestMachineLeaseExpiryAndLongRunningJobsDrainBeforeClaim(t *testing.T) {
	expiring := simulatedMachine{
		id:             "gpu-1",
		leaseState:     leaseAccepting,
		protocol:       protocolFull,
		remainingTTL:   59,
		requiredTTL:    60,
		heartbeatFresh: true,
		scheduler:      schedulerActive,
		capacity:       1,
	}
	if expiring.canClaimQueuedJob() {
		t.Fatal("machine with insufficient TTL must not claim new long-running job")
	}
	if got := reconcileLease(reconcileInput{
		heartbeatFresh: expiring.heartbeatFresh,
		scheduler:      expiring.scheduler,
		remainingTTL:   expiring.remainingTTL,
		requiredTTL:    expiring.requiredTTL,
	}); got != reconcileDrain {
		t.Fatalf("insufficient TTL should drain, got %s", got)
	}

	expiredWhileRunning := expiring
	expiredWhileRunning.scheduler = schedulerDead
	if got := reconcileLease(reconcileInput{
		heartbeatFresh: true,
		scheduler:      expiredWhileRunning.scheduler,
		jobState:       jobStarted,
		remainingTTL:   0,
		requiredTTL:    60,
	}); got != reconcileQuarantine {
		t.Fatalf("started job whose scheduler lease died should quarantine, got %s", got)
	}
}

func TestHungMachineDoesNotDeleteActiveWorkOrBlockHealthyMachine(t *testing.T) {
	hung := simulatedMachine{
		id:             "gpu-hung",
		leaseState:     leaseAccepting,
		protocol:       protocolFull,
		remainingTTL:   300,
		requiredTTL:    60,
		heartbeatFresh: false,
		scheduler:      schedulerActive,
		capacity:       1,
		used:           1,
	}
	if got := reconcileLease(reconcileInput{
		heartbeatFresh: hung.heartbeatFresh,
		scheduler:      hung.scheduler,
		jobState:       jobStarted,
		remainingTTL:   hung.remainingTTL,
		requiredTTL:    hung.requiredTTL,
	}); got != reconcileDrain {
		t.Fatalf("hung machine with active scheduler should drain/unhealthy, got %s", got)
	}
	if got := cleanupDecision(cleanupInput{schedulerAlive: true, jobState: jobStarted}); got != cleanupNone {
		t.Fatalf("hung but scheduler-alive active job must not be deleted, got %s", got)
	}

	healthy := simulatedMachine{
		id:             "gpu-healthy",
		leaseState:     leaseAccepting,
		protocol:       protocolFull,
		remainingTTL:   300,
		requiredTTL:    60,
		heartbeatFresh: true,
		scheduler:      schedulerActive,
		capacity:       1,
	}
	if !healthy.canClaimQueuedJob() {
		t.Fatal("healthy machine should still accept work while another machine is hung")
	}
}

type atomicClaimStore struct {
	claims   map[string]string
	attempts map[string]int
}

func newAtomicClaimStore() *atomicClaimStore {
	return &atomicClaimStore{
		claims:   map[string]string{},
		attempts: map[string]int{},
	}
}

func (s *atomicClaimStore) tryClaim(jobID string, workerID string) (int, bool) {
	if _, exists := s.claims[jobID]; exists {
		return s.attempts[jobID], false
	}
	s.attempts[jobID]++
	s.claims[jobID] = workerID
	return s.attempts[jobID], true
}

func TestMultipleMachinesCannotClaimSameJobButCanClaimDifferentJobs(t *testing.T) {
	store := newAtomicClaimStore()
	attempt, ok := store.tryClaim("job-1", "worker-a")
	if !ok || attempt != 1 {
		t.Fatalf("first claim should succeed with attempt 1, got attempt=%d ok=%v", attempt, ok)
	}
	attempt, ok = store.tryClaim("job-1", "worker-b")
	if ok || attempt != 1 {
		t.Fatalf("second claim for same job should fail without new attempt, got attempt=%d ok=%v", attempt, ok)
	}
	attempt, ok = store.tryClaim("job-2", "worker-b")
	if !ok || attempt != 1 {
		t.Fatalf("different job should be claimable by another worker, got attempt=%d ok=%v", attempt, ok)
	}
}

func dispatchQueuedJobs(machines []simulatedMachine, jobs []string) map[string]string {
	assignments := map[string]string{}
	for _, job := range jobs {
		for i := range machines {
			if machines[i].canClaimQueuedJob() {
				machines[i].used++
				assignments[job] = machines[i].id
				break
			}
		}
	}
	return assignments
}

func TestMultipleMachinesRespectCapacityAndSkipDrainingOrHungWorkers(t *testing.T) {
	machines := []simulatedMachine{
		{
			id:             "hung",
			leaseState:     leaseAccepting,
			protocol:       protocolFull,
			remainingTTL:   300,
			requiredTTL:    60,
			heartbeatFresh: false,
			scheduler:      schedulerActive,
			capacity:       1,
		},
		{
			id:             "draining",
			leaseState:     leaseDraining,
			protocol:       protocolFull,
			remainingTTL:   300,
			requiredTTL:    60,
			heartbeatFresh: true,
			scheduler:      schedulerActive,
			capacity:       1,
		},
		{
			id:             "healthy-a",
			leaseState:     leaseAccepting,
			protocol:       protocolFull,
			remainingTTL:   300,
			requiredTTL:    60,
			heartbeatFresh: true,
			scheduler:      schedulerActive,
			capacity:       1,
		},
		{
			id:             "healthy-b",
			leaseState:     leaseAccepting,
			protocol:       protocolFull,
			remainingTTL:   300,
			requiredTTL:    60,
			heartbeatFresh: true,
			scheduler:      schedulerActive,
			capacity:       2,
		},
	}
	assignments := dispatchQueuedJobs(machines, []string{"job-1", "job-2", "job-3", "job-4"})
	if len(assignments) != 3 {
		t.Fatalf("expected only three capacity slots, got assignments %#v", assignments)
	}
	for job, worker := range assignments {
		if worker == "hung" || worker == "draining" {
			t.Fatalf("%s should not be assigned to unavailable worker %s", job, worker)
		}
	}
}

func TestLeaseReacquireEpochRejectsOldMachineWrites(t *testing.T) {
	expected := writeStamp{
		jobID:         "job-1",
		jobAttempt:    1,
		workerID:      "worker-new",
		leaseEpoch:    2,
		claimEpoch:    1,
		protocolWrite: 1,
	}
	oldLeaseWrite := writeStamp{
		jobID:         "job-1",
		jobAttempt:    1,
		workerID:      "worker-old",
		leaseEpoch:    1,
		claimEpoch:    1,
		protocolWrite: 1,
	}
	if acceptWorkerWrite(expected, oldLeaseWrite, 1, 1) {
		t.Fatal("old worker from previous lease epoch must not write into reacquired lease")
	}
}

type providerJobState string

const (
	providerPending   providerJobState = "pending"
	providerRunning   providerJobState = "running"
	providerSuspended providerJobState = "suspended"
	providerPreempted providerJobState = "preempted"
	providerRequeued  providerJobState = "requeued"
	providerDone      providerJobState = "done"
	providerFailed    providerJobState = "failed"
	providerNodeFail  providerJobState = "node_fail"
)

func providerStateReconcile(state providerJobState, job jobState) reconcileAction {
	switch state {
	case providerRunning:
		return reconcileRunning
	case providerPending:
		return reconcilePending
	case providerSuspended:
		return reconcileSuspended
	case providerDone:
		if job == jobTerminal {
			return reconcileCompleted
		}
		return reconcileFinalizing
	case providerPreempted, providerRequeued, providerFailed, providerNodeFail:
		if job == jobStarted || job == jobStartIntent {
			return reconcileQuarantine
		}
		return reconcileLost
	default:
		return reconcileDrain
	}
}

func TestProviderJobStatesMapToConservativeLeaseActions(t *testing.T) {
	tests := []struct {
		name  string
		state providerJobState
		job   jobState
		want  reconcileAction
	}{
		{name: "running awaits worker readiness", state: providerRunning, job: jobQueued, want: reconcileRunning},
		{name: "pending remains pending", state: providerPending, job: jobQueued, want: reconcilePending},
		{name: "suspended pauses without cleanup", state: providerSuspended, job: jobStarted, want: reconcileSuspended},
		{name: "preempted started quarantines", state: providerPreempted, job: jobStarted, want: reconcileQuarantine},
		{name: "requeued before start lost", state: providerRequeued, job: jobClaimed, want: reconcileLost},
		{name: "node fail started quarantines", state: providerNodeFail, job: jobStarted, want: reconcileQuarantine},
		{name: "done terminal completed", state: providerDone, job: jobTerminal, want: reconcileCompleted},
		{name: "done without terminal enters finalizing grace", state: providerDone, job: jobStarted, want: reconcileFinalizing},
	}
	for _, tt := range tests {
		if got := providerStateReconcile(tt.state, tt.job); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}
