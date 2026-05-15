package sim

import (
	"strings"
	"testing"
)

type teamsTurnProjection string

const (
	teamsTurnQueued       teamsTurnProjection = "queued"
	teamsTurnRunning      teamsTurnProjection = "running"
	teamsTurnDelegated    teamsTurnProjection = "delegated"
	teamsTurnInterrupted  teamsTurnProjection = "interrupted"
	teamsTurnResultQueued teamsTurnProjection = "result_queued"
)

type beaconRecoverInput struct {
	turn       teamsTurnProjection
	job        jobState
	workerLive bool
	terminal   terminalIntegrity
}

type beaconRecoverDecision struct {
	turnAfter           teamsTurnProjection
	action              recoveryAction
	acceptLaterTerminal bool
	ignoreInbound       bool
}

func recoverTeamsTurnWithBeacon(in beaconRecoverInput) beaconRecoverDecision {
	action := recoverJob(in.job, in.workerLive, in.terminal)
	switch action {
	case recoverMonitor:
		return beaconRecoverDecision{
			turnAfter:           teamsTurnDelegated,
			action:              action,
			acceptLaterTerminal: true,
		}
	case recoverComplete:
		return beaconRecoverDecision{
			turnAfter: teamsTurnResultQueued,
			action:    action,
		}
	case recoverRequeue:
		return beaconRecoverDecision{
			turnAfter: teamsTurnQueued,
			action:    action,
		}
	default:
		return beaconRecoverDecision{
			turnAfter:     teamsTurnInterrupted,
			action:        action,
			ignoreInbound: true,
		}
	}
}

func TestBeaconAwareRecoverDoesNotInterruptLiveRemoteExecution(t *testing.T) {
	got := recoverTeamsTurnWithBeacon(beaconRecoverInput{
		turn:       teamsTurnRunning,
		job:        jobStarted,
		workerLive: true,
	})
	if got.turnAfter != teamsTurnDelegated || got.action != recoverMonitor || !got.acceptLaterTerminal || got.ignoreInbound {
		t.Fatalf("live remote job should stay delegated and able to complete later, got %#v", got)
	}

	got = recoverTeamsTurnWithBeacon(beaconRecoverInput{
		turn: teamsTurnRunning,
		job:  jobStarted,
	})
	if got.turnAfter != teamsTurnInterrupted || got.action != recoverAmbiguous || !got.ignoreInbound {
		t.Fatalf("lost started job should become interrupted/ambiguous, got %#v", got)
	}

	got = recoverTeamsTurnWithBeacon(beaconRecoverInput{
		turn:     teamsTurnRunning,
		job:      jobTerminal,
		terminal: terminalValid,
	})
	if got.turnAfter != teamsTurnResultQueued || got.action != recoverComplete {
		t.Fatalf("valid terminal should queue result instead of staying interrupted, got %#v", got)
	}
}

func TestWorkerTerminalSurvivesCoordinatorRestartWhenClaimEpochMatches(t *testing.T) {
	expected := writeStamp{
		jobID:         "job-1",
		jobAttempt:    1,
		workerID:      "worker-a",
		leaseEpoch:    9,
		claimEpoch:    4,
		protocolWrite: 1,
	}
	terminalAfterRestart := expected
	currentCoordinatorEpoch := 7
	if currentCoordinatorEpoch == terminalAfterRestart.claimEpoch {
		t.Fatal("test setup should simulate a restarted coordinator with a new control epoch")
	}
	if !acceptWorkerWrite(expected, terminalAfterRestart, 1, 1) {
		t.Fatal("worker terminal should be fenced by job claim epoch, not current coordinator epoch")
	}
}

type beaconOutboxIntent string

const (
	beaconIntentLeaseStatus beaconOutboxIntent = "lease_status"
	beaconIntentJobProgress beaconOutboxIntent = "job_progress"
	beaconIntentFinal       beaconOutboxIntent = "final"
	beaconIntentArtifact    beaconOutboxIntent = "artifact"
	beaconIntentAttention   beaconOutboxIntent = "attention"
)

type beaconOutboxShape struct {
	kind          string
	hasAttachment bool
}

func beaconOutboxForIntent(intent beaconOutboxIntent) beaconOutboxShape {
	switch intent {
	case beaconIntentLeaseStatus:
		return beaconOutboxShape{kind: "status-beacon-lease"}
	case beaconIntentJobProgress:
		return beaconOutboxShape{kind: "progress-beacon-job"}
	case beaconIntentFinal:
		return beaconOutboxShape{kind: "final-beacon-answer"}
	case beaconIntentArtifact:
		return beaconOutboxShape{kind: "artifact-beacon", hasAttachment: true}
	case beaconIntentAttention:
		return beaconOutboxShape{kind: "final-beacon-needs-attention"}
	default:
		return beaconOutboxShape{kind: "helper"}
	}
}

func beaconOutboxProtected(msg beaconOutboxShape) bool {
	kind := strings.ToLower(strings.TrimSpace(msg.kind))
	return msg.hasAttachment ||
		strings.Contains(kind, "final") ||
		strings.Contains(kind, "answer") ||
		strings.Contains(kind, "artifact") ||
		strings.Contains(kind, "attachment")
}

func beaconOutboxTransient(msg beaconOutboxShape) bool {
	kind := strings.ToLower(strings.TrimSpace(msg.kind))
	return strings.HasPrefix(kind, "status-") || strings.HasPrefix(kind, "progress-")
}

func TestBeaconOutboxKindsMatchTeamsRecoverAndUpgradeSemantics(t *testing.T) {
	transient := []beaconOutboxIntent{beaconIntentLeaseStatus, beaconIntentJobProgress}
	for _, intent := range transient {
		msg := beaconOutboxForIntent(intent)
		if !beaconOutboxTransient(msg) || beaconOutboxProtected(msg) {
			t.Fatalf("%s should be transient and non-protected, got %#v", intent, msg)
		}
	}

	protected := []beaconOutboxIntent{beaconIntentFinal, beaconIntentArtifact, beaconIntentAttention}
	for _, intent := range protected {
		msg := beaconOutboxForIntent(intent)
		if !beaconOutboxProtected(msg) || beaconOutboxTransient(msg) {
			t.Fatalf("%s should be protected and not transient, got %#v", intent, msg)
		}
	}
}

type beaconUpgradeInput struct {
	queuedTeamsTurns      int
	runningTeamsTurns     int
	activeBeaconJobs      int
	protectedBeaconOutbox int
	transientBeaconOutbox int
	idleBeaconWorkers     int
	protocolMismatchJobs  int
}

func beaconUpgradeBlockers(in beaconUpgradeInput) []string {
	var blockers []string
	if in.queuedTeamsTurns+in.runningTeamsTurns > 0 {
		blockers = append(blockers, "teams_turn")
	}
	if in.activeBeaconJobs > 0 {
		blockers = append(blockers, "beacon_job")
	}
	if in.protectedBeaconOutbox > 0 {
		blockers = append(blockers, "beacon_outbox")
	}
	if in.protocolMismatchJobs > 0 {
		blockers = append(blockers, "beacon_protocol")
	}
	return blockers
}

func TestBeaconUpgradeBlockersIncludeRemoteWorkButIgnoreIdleAndTransientState(t *testing.T) {
	if blockers := beaconUpgradeBlockers(beaconUpgradeInput{
		transientBeaconOutbox: 3,
		idleBeaconWorkers:     2,
	}); len(blockers) != 0 {
		t.Fatalf("idle workers and transient progress should not block upgrade: %#v", blockers)
	}

	blockers := beaconUpgradeBlockers(beaconUpgradeInput{
		activeBeaconJobs:      1,
		protectedBeaconOutbox: 1,
		protocolMismatchJobs:  1,
	})
	want := []string{"beacon_job", "beacon_outbox", "beacon_protocol"}
	if strings.Join(blockers, ",") != strings.Join(want, ",") {
		t.Fatalf("blockers = %#v, want %#v", blockers, want)
	}
}

type beaconActor string
type beaconOperation string

const (
	actorActiveCoordinator  beaconActor = "active_coordinator"
	actorStandbyCoordinator beaconActor = "standby_coordinator"
	actorWorker             beaconActor = "worker"

	opDispatch       beaconOperation = "dispatch"
	opCleanup        beaconOperation = "cleanup"
	opQueueTeams     beaconOperation = "queue_teams_outbox"
	opFlushTeams     beaconOperation = "flush_teams_outbox"
	opWriteJobResult beaconOperation = "write_job_result"
	opReadState      beaconOperation = "read_state"
)

func operationAllowed(actor beaconActor, op beaconOperation) bool {
	switch actor {
	case actorActiveCoordinator:
		return op == opDispatch || op == opCleanup || op == opQueueTeams || op == opFlushTeams || op == opReadState
	case actorStandbyCoordinator:
		return op == opReadState
	case actorWorker:
		return op == opWriteJobResult || op == opReadState
	default:
		return false
	}
}

func TestOnlyActiveCoordinatorDispatchesCleansAndTouchesTeamsOutbox(t *testing.T) {
	for _, op := range []beaconOperation{opDispatch, opCleanup, opQueueTeams, opFlushTeams} {
		if !operationAllowed(actorActiveCoordinator, op) {
			t.Fatalf("active coordinator should be allowed to %s", op)
		}
		if operationAllowed(actorStandbyCoordinator, op) {
			t.Fatalf("standby coordinator must not be allowed to %s", op)
		}
		if operationAllowed(actorWorker, op) {
			t.Fatalf("worker must not be allowed to %s", op)
		}
	}
	if !operationAllowed(actorWorker, opWriteJobResult) {
		t.Fatal("worker should be allowed to write fenced job results")
	}
}

type executionProfileNamespace string

const (
	namespaceSSHProxy   executionProfileNamespace = "ssh_proxy"
	namespaceTeamsScope executionProfileNamespace = "teams_scope"
	namespaceBeaconExec executionProfileNamespace = "beacon_execution"
)

type namedProfile struct {
	namespace executionProfileNamespace
	name      string
}

func profileStorageKey(p namedProfile) string {
	return string(p.namespace) + ":" + strings.TrimSpace(p.name)
}

func TestExecutionProfileNamespaceDoesNotCollideWithSSHProxyOrTeamsScopeProfile(t *testing.T) {
	sameName := "gpu"
	keys := map[string]bool{}
	for _, profile := range []namedProfile{
		{namespace: namespaceSSHProxy, name: sameName},
		{namespace: namespaceTeamsScope, name: sameName},
		{namespace: namespaceBeaconExec, name: sameName},
	} {
		key := profileStorageKey(profile)
		if keys[key] {
			t.Fatalf("profile key collided: %s", key)
		}
		keys[key] = true
	}
	if len(keys) != 3 {
		t.Fatalf("profiles with same display name must remain distinct, got %#v", keys)
	}
}

type executionSnapshot struct {
	target    executionTarget
	profile   string
	signature string
}

type plannedTurn struct {
	id       string
	snapshot executionSnapshot
}

type conversationPlacement struct {
	current executionSnapshot
	pending *executionSnapshot
	queue   []plannedTurn
}

func (c *conversationPlacement) queueTurn(id string) {
	snapshot := c.current
	c.queue = append(c.queue, plannedTurn{id: id, snapshot: snapshot})
}

func (c *conversationPlacement) switchProfile(next executionSnapshot) {
	if len(c.queue) > 0 {
		c.pending = &next
		return
	}
	c.current = next
}

func (c *conversationPlacement) finishTurn() {
	if len(c.queue) > 0 {
		c.queue = c.queue[1:]
	}
	if len(c.queue) == 0 && c.pending != nil {
		c.current = *c.pending
		c.pending = nil
	}
}

func TestQueuedTurnKeepsExecutionTargetSnapshotAcrossProfileSwitch(t *testing.T) {
	c := conversationPlacement{
		current: executionSnapshot{target: targetBeacon, profile: "gpu-h100", signature: "image-a"},
	}
	c.queueTurn("turn-1")
	c.switchProfile(executionSnapshot{target: targetBeacon, profile: "cpu", signature: "image-a"})

	if c.queue[0].snapshot.profile != "gpu-h100" {
		t.Fatalf("queued turn snapshot changed after profile switch: %#v", c.queue[0].snapshot)
	}
	if c.current.profile != "gpu-h100" || c.pending == nil || c.pending.profile != "cpu" {
		t.Fatalf("profile switch while queued should be pending, current=%#v pending=%#v", c.current, c.pending)
	}

	c.finishTurn()
	if c.current.profile != "cpu" || c.pending != nil {
		t.Fatalf("pending profile should apply after queued turn finishes, current=%#v pending=%#v", c.current, c.pending)
	}
	c.queueTurn("turn-2")
	if c.queue[0].snapshot.profile != "cpu" {
		t.Fatalf("next turn should use switched profile, got %#v", c.queue[0].snapshot)
	}
}
