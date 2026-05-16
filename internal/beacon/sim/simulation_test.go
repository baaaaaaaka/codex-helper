package sim

import "testing"

type leaseState string

const (
	leaseStarting     leaseState = "starting"
	leaseAccepting    leaseState = "accepting"
	leaseDraining     leaseState = "draining"
	leaseDrained      leaseState = "drained"
	leaseExpired      leaseState = "expired"
	leaseLost         leaseState = "lost"
	leaseIncompatible leaseState = "incompatible"
)

type jobState string

const (
	jobQueued      jobState = "queued"
	jobClaimed     jobState = "claimed"
	jobStartIntent jobState = "start_intent"
	jobStarted     jobState = "started"
	jobTerminal    jobState = "terminal"
	jobAmbiguous   jobState = "ambiguous"
	jobQuarantined jobState = "quarantined"
	jobTombstoned  jobState = "tombstoned"
)

type protocolMode string

const (
	protocolFull              protocolMode = "full"
	protocolReadOnlyFuture    protocolMode = "read_only_future"
	protocolMissingCapability protocolMode = "missing_capability"
	protocolMajorMismatch     protocolMode = "major_mismatch"
)

type claimInput struct {
	serviceDraining bool
	leaseState      leaseState
	protocol        protocolMode
	remainingTTL    int
	requiredTTL     int
	signatureMatch  bool
	slotAvailable   bool
	jobState        jobState
	tombstoned      bool
}

func canClaim(in claimInput) bool {
	return !in.serviceDraining &&
		in.leaseState == leaseAccepting &&
		in.protocol == protocolFull &&
		in.remainingTTL >= in.requiredTTL &&
		in.signatureMatch &&
		in.slotAvailable &&
		in.jobState == jobQueued &&
		!in.tombstoned
}

func TestClaimMatrixRequiresEveryPlacementGate(t *testing.T) {
	base := claimInput{
		leaseState:     leaseAccepting,
		protocol:       protocolFull,
		remainingTTL:   60,
		requiredTTL:    30,
		signatureMatch: true,
		slotAvailable:  true,
		jobState:       jobQueued,
	}
	if !canClaim(base) {
		t.Fatal("base case should be claimable")
	}

	cases := map[string]claimInput{
		"service drain":      with(base, func(in *claimInput) { in.serviceDraining = true }),
		"lease starting":     with(base, func(in *claimInput) { in.leaseState = leaseStarting }),
		"lease draining":     with(base, func(in *claimInput) { in.leaseState = leaseDraining }),
		"read only protocol": with(base, func(in *claimInput) { in.protocol = protocolReadOnlyFuture }),
		"major mismatch":     with(base, func(in *claimInput) { in.protocol = protocolMajorMismatch }),
		"ttl too short":      with(base, func(in *claimInput) { in.remainingTTL = 29 }),
		"signature mismatch": with(base, func(in *claimInput) { in.signatureMatch = false }),
		"no slot":            with(base, func(in *claimInput) { in.slotAvailable = false }),
		"already claimed":    with(base, func(in *claimInput) { in.jobState = jobClaimed }),
		"tombstoned":         with(base, func(in *claimInput) { in.tombstoned = true }),
	}
	for name, in := range cases {
		if canClaim(in) {
			t.Fatalf("%s should block claim: %#v", name, in)
		}
	}
}

func with(in claimInput, fn func(*claimInput)) claimInput {
	fn(&in)
	return in
}

type protocolDecision struct {
	claimNew              bool
	monitorActive         bool
	acceptWorkerTerminal  bool
	coordinatorMayMutate  bool
	drainWorker           bool
	quarantineActiveState bool
}

func decideProtocol(mode protocolMode, activeJob bool) protocolDecision {
	switch mode {
	case protocolFull:
		return protocolDecision{
			claimNew:             true,
			monitorActive:        activeJob,
			acceptWorkerTerminal: true,
			coordinatorMayMutate: true,
		}
	case protocolReadOnlyFuture:
		return protocolDecision{
			monitorActive:        activeJob,
			acceptWorkerTerminal: activeJob,
			drainWorker:          true,
		}
	case protocolMissingCapability:
		return protocolDecision{
			monitorActive:        activeJob,
			acceptWorkerTerminal: activeJob,
			drainWorker:          true,
		}
	default:
		return protocolDecision{
			drainWorker:           true,
			quarantineActiveState: activeJob,
		}
	}
}

func TestProtocolCompatibilityMatrixSeparatesReadAndWrite(t *testing.T) {
	tests := []struct {
		name   string
		mode   protocolMode
		active bool
		want   protocolDecision
	}{
		{
			name:   "fully compatible worker claims and mutates",
			mode:   protocolFull,
			active: true,
			want: protocolDecision{
				claimNew:             true,
				monitorActive:        true,
				acceptWorkerTerminal: true,
				coordinatorMayMutate: true,
			},
		},
		{
			name:   "future readable worker is monitored but drained",
			mode:   protocolReadOnlyFuture,
			active: true,
			want: protocolDecision{
				monitorActive:        true,
				acceptWorkerTerminal: true,
				drainWorker:          true,
			},
		},
		{
			name:   "older worker missing claim capability drains after active job",
			mode:   protocolMissingCapability,
			active: true,
			want: protocolDecision{
				monitorActive:        true,
				acceptWorkerTerminal: true,
				drainWorker:          true,
			},
		},
		{
			name:   "major mismatch does not accept active state automatically",
			mode:   protocolMajorMismatch,
			active: true,
			want: protocolDecision{
				drainWorker:           true,
				quarantineActiveState: true,
			},
		},
	}
	for _, tt := range tests {
		if got := decideProtocol(tt.mode, tt.active); got != tt.want {
			t.Fatalf("%s: got %#v want %#v", tt.name, got, tt.want)
		}
	}
}

type recoveryAction string

const (
	recoverRequeue    recoveryAction = "requeue"
	recoverMonitor    recoveryAction = "monitor"
	recoverComplete   recoveryAction = "complete"
	recoverAmbiguous  recoveryAction = "ambiguous"
	recoverQuarantine recoveryAction = "quarantine"
)

type terminalIntegrity string

const (
	terminalNone              terminalIntegrity = "none"
	terminalValid             terminalIntegrity = "valid"
	terminalEventGap          terminalIntegrity = "event_gap"
	terminalHMACBad           terminalIntegrity = "hmac_bad"
	terminalSeqBad            terminalIntegrity = "seq_bad"
	terminalDuplicate         terminalIntegrity = "duplicate"
	terminalDuplicateSame     terminalIntegrity = "duplicate_same"
	terminalDuplicateConflict terminalIntegrity = "duplicate_conflict"
	terminalLateWrite         terminalIntegrity = "late_write"
)

func recoverJob(state jobState, workerAlive bool, terminal terminalIntegrity) recoveryAction {
	switch terminal {
	case terminalValid, terminalDuplicateSame:
		return recoverComplete
	case terminalEventGap, terminalHMACBad, terminalSeqBad, terminalDuplicate, terminalDuplicateConflict, terminalLateWrite:
		return recoverQuarantine
	}
	if workerAlive {
		return recoverMonitor
	}
	switch state {
	case jobQueued, jobClaimed:
		return recoverRequeue
	case jobStartIntent, jobStarted:
		return recoverAmbiguous
	default:
		return recoverQuarantine
	}
}

func TestRecoveryNeverReplaysStartedCodex(t *testing.T) {
	tests := []struct {
		state       jobState
		workerAlive bool
		terminal    terminalIntegrity
		want        recoveryAction
	}{
		{state: jobQueued, want: recoverRequeue},
		{state: jobClaimed, want: recoverRequeue},
		{state: jobStartIntent, want: recoverAmbiguous},
		{state: jobStarted, want: recoverAmbiguous},
		{state: jobStarted, workerAlive: true, want: recoverMonitor},
		{state: jobStarted, terminal: terminalValid, want: recoverComplete},
		{state: jobTerminal, terminal: terminalDuplicateSame, want: recoverComplete},
		{state: jobTerminal, terminal: terminalEventGap, want: recoverQuarantine},
		{state: jobTerminal, terminal: terminalDuplicate, want: recoverQuarantine},
		{state: jobTerminal, terminal: terminalDuplicateConflict, want: recoverQuarantine},
	}
	for _, tt := range tests {
		if got := recoverJob(tt.state, tt.workerAlive, tt.terminal); got != tt.want {
			t.Fatalf("recover %s alive=%v terminal=%s: got %s want %s", tt.state, tt.workerAlive, tt.terminal, got, tt.want)
		}
	}
}

type writeStamp struct {
	jobID          string
	jobAttempt     int
	workerID       string
	providerJobID  string
	allocationID   string
	stepID         string
	runIncarnation string
	host           string
	leaseEpoch     int
	claimEpoch     int
	protocolWrite  int
	signatureHash  string
	macKeyID       string
	mac            string
	eventHash      string
}

func acceptWorkerWrite(expected, got writeStamp, minWriteVersion, maxWriteVersion int) bool {
	return got.jobID == expected.jobID &&
		got.jobAttempt == expected.jobAttempt &&
		got.workerID == expected.workerID &&
		got.providerJobID == expected.providerJobID &&
		got.allocationID == expected.allocationID &&
		got.stepID == expected.stepID &&
		got.runIncarnation == expected.runIncarnation &&
		got.host == expected.host &&
		got.leaseEpoch == expected.leaseEpoch &&
		got.claimEpoch == expected.claimEpoch &&
		got.signatureHash == expected.signatureHash &&
		got.macKeyID == expected.macKeyID &&
		got.mac == expected.mac &&
		got.eventHash == expected.eventHash &&
		got.protocolWrite >= minWriteVersion &&
		got.protocolWrite <= maxWriteVersion
}

func TestFencingRejectsLateWritesAcrossEpochsAttemptsAndProtocols(t *testing.T) {
	expected := writeStamp{
		jobID:          "job-1",
		jobAttempt:     2,
		workerID:       "worker-a",
		providerJobID:  "slurm-100",
		allocationID:   "alloc-100",
		stepID:         "step-1",
		runIncarnation: "boot-a",
		host:           "gpu-a",
		leaseEpoch:     7,
		claimEpoch:     3,
		protocolWrite:  1,
		signatureHash:  "sig-a",
		macKeyID:       "cap-a",
		mac:            "mac-a",
		eventHash:      "event-a",
	}
	if !acceptWorkerWrite(expected, expected, 1, 1) {
		t.Fatal("matching write stamp should be accepted")
	}

	cases := map[string]writeStamp{
		"old attempt":      copyStamp(expected, func(s *writeStamp) { s.jobAttempt = 1 }),
		"wrong worker":     copyStamp(expected, func(s *writeStamp) { s.workerID = "worker-b" }),
		"wrong provider":   copyStamp(expected, func(s *writeStamp) { s.providerJobID = "slurm-101" }),
		"wrong allocation": copyStamp(expected, func(s *writeStamp) { s.allocationID = "alloc-101" }),
		"wrong step":       copyStamp(expected, func(s *writeStamp) { s.stepID = "step-2" }),
		"wrong run":        copyStamp(expected, func(s *writeStamp) { s.runIncarnation = "boot-b" }),
		"wrong host":       copyStamp(expected, func(s *writeStamp) { s.host = "gpu-b" }),
		"old lease epoch":  copyStamp(expected, func(s *writeStamp) { s.leaseEpoch = 6 }),
		"old claim epoch":  copyStamp(expected, func(s *writeStamp) { s.claimEpoch = 2 }),
		"future protocol":  copyStamp(expected, func(s *writeStamp) { s.protocolWrite = 2 }),
		"wrong signature":  copyStamp(expected, func(s *writeStamp) { s.signatureHash = "sig-b" }),
		"wrong mac key":    copyStamp(expected, func(s *writeStamp) { s.macKeyID = "cap-b" }),
		"bad mac":          copyStamp(expected, func(s *writeStamp) { s.mac = "mac-b" }),
		"wrong event hash": copyStamp(expected, func(s *writeStamp) { s.eventHash = "event-b" }),
		"wrong job":        copyStamp(expected, func(s *writeStamp) { s.jobID = "job-2" }),
	}
	for name, got := range cases {
		if acceptWorkerWrite(expected, got, 1, 1) {
			t.Fatalf("%s should be rejected: %#v", name, got)
		}
	}
}

func copyStamp(in writeStamp, fn func(*writeStamp)) writeStamp {
	fn(&in)
	return in
}

type installOrigin string

const (
	originManagedPersistent installOrigin = "managed_persistent"
	originFixedPath         installOrigin = "fixed_path"
	originReadOnlyImage     installOrigin = "readonly_image"
	originSystemGlobal      installOrigin = "system_global"
	originUnknown           installOrigin = "unknown"
	originEphemeralOverlay  installOrigin = "ephemeral_overlay"
)

type codexUpgradeInput struct {
	scoped             bool
	origin             installOrigin
	activeSameTarget   bool
	activeOtherTarget  bool
	targetLockAcquired bool
	maintenanceWorker  bool
}

func canUpgradeCodex(in codexUpgradeInput) bool {
	return in.scoped &&
		in.origin == originManagedPersistent &&
		!in.activeSameTarget &&
		in.targetLockAcquired &&
		in.maintenanceWorker
}

func TestCodexUpgradeRequiresScopedPersistentIdleTarget(t *testing.T) {
	base := codexUpgradeInput{
		scoped:             true,
		origin:             originManagedPersistent,
		targetLockAcquired: true,
		maintenanceWorker:  true,
	}
	if !canUpgradeCodex(base) {
		t.Fatal("base managed persistent target should be upgradeable")
	}
	if !canUpgradeCodex(codexUpgradeInput{
		scoped:             true,
		origin:             originManagedPersistent,
		activeOtherTarget:  true,
		targetLockAcquired: true,
		maintenanceWorker:  true,
	}) {
		t.Fatal("different active install target must not block this target")
	}

	cases := map[string]codexUpgradeInput{
		"ambiguous scope":    {origin: originManagedPersistent, targetLockAcquired: true, maintenanceWorker: true},
		"active same target": {scoped: true, origin: originManagedPersistent, activeSameTarget: true, targetLockAcquired: true, maintenanceWorker: true},
		"fixed path":         {scoped: true, origin: originFixedPath, targetLockAcquired: true, maintenanceWorker: true},
		"readonly image":     {scoped: true, origin: originReadOnlyImage, targetLockAcquired: true, maintenanceWorker: true},
		"system global":      {scoped: true, origin: originSystemGlobal, targetLockAcquired: true, maintenanceWorker: true},
		"unknown origin":     {scoped: true, origin: originUnknown, targetLockAcquired: true, maintenanceWorker: true},
		"ephemeral overlay":  {scoped: true, origin: originEphemeralOverlay, targetLockAcquired: true, maintenanceWorker: true},
		"lock not acquired":  {scoped: true, origin: originManagedPersistent, maintenanceWorker: true},
		"production worker":  {scoped: true, origin: originManagedPersistent, targetLockAcquired: true},
	}
	for name, in := range cases {
		if canUpgradeCodex(in) {
			t.Fatalf("%s should not be upgradeable: %#v", name, in)
		}
	}
}

type isolation string

const (
	isolationShared    isolation = "shared"
	isolationExclusive isolation = "exclusive"
)

type reservationInput struct {
	leaseIsolation isolation
	request        isolation
	capacity       int
	used           int
}

func canReserve(in reservationInput) bool {
	switch {
	case in.leaseIsolation == isolationShared && in.request == isolationShared:
		return in.used < in.capacity
	case in.leaseIsolation == isolationExclusive && in.request == isolationExclusive:
		return in.used == 0
	default:
		return false
	}
}

func TestSharedExclusiveReservationMatrix(t *testing.T) {
	tests := []struct {
		name string
		in   reservationInput
		want bool
	}{
		{name: "shared slot available", in: reservationInput{leaseIsolation: isolationShared, request: isolationShared, capacity: 2, used: 1}, want: true},
		{name: "shared full", in: reservationInput{leaseIsolation: isolationShared, request: isolationShared, capacity: 1, used: 1}},
		{name: "exclusive empty", in: reservationInput{leaseIsolation: isolationExclusive, request: isolationExclusive, capacity: 1}, want: true},
		{name: "exclusive already used", in: reservationInput{leaseIsolation: isolationExclusive, request: isolationExclusive, capacity: 1, used: 1}},
		{name: "exclusive request does not reuse shared lease", in: reservationInput{leaseIsolation: isolationShared, request: isolationExclusive, capacity: 2}},
		{name: "shared request does not reuse exclusive lease", in: reservationInput{leaseIsolation: isolationExclusive, request: isolationShared, capacity: 1}},
	}
	for _, tt := range tests {
		if got := canReserve(tt.in); got != tt.want {
			t.Fatalf("%s: got %v want %v", tt.name, got, tt.want)
		}
	}
}

func ttlAllowsTurn(remaining, estimated, checkpoint, grace int) bool {
	return remaining >= estimated+checkpoint+grace
}

func TestTTLGateIncludesCheckpointAndGraceBudget(t *testing.T) {
	if !ttlAllowsTurn(120, 90, 20, 10) {
		t.Fatal("exact TTL budget should allow a turn")
	}
	if ttlAllowsTurn(119, 90, 20, 10) {
		t.Fatal("one second below total budget should drain instead of claim")
	}
}

type helperUpgradeInput struct {
	activeBeaconJobs     int
	staleOwnerMarkers    int
	unreconciledMarkers  int
	startedBeaconJobs    int
	protocolMismatchJobs int
	idleWorkers          int
	blockingOutbox       int
	queuedTeamsTurns     int
	runningTeamsTurns    int
	force                bool
}

func helperUpgradeBlocked(in helperUpgradeInput) bool {
	if in.force &&
		in.activeBeaconJobs == 0 &&
		in.unreconciledMarkers == 0 &&
		in.startedBeaconJobs == 0 &&
		in.blockingOutbox == 0 &&
		in.protocolMismatchJobs == 0 &&
		in.runningTeamsTurns == 0 {
		return false
	}
	return in.activeBeaconJobs > 0 ||
		in.unreconciledMarkers > 0 ||
		in.startedBeaconJobs > 0 ||
		in.blockingOutbox > 0 ||
		in.runningTeamsTurns > 0 ||
		in.protocolMismatchJobs > 0
}

func TestHelperUpgradeBlockersIncludeActiveBeaconJobsButNotIdleWorkers(t *testing.T) {
	tests := []struct {
		name string
		in   helperUpgradeInput
		want bool
	}{
		{name: "idle workers do not block", in: helperUpgradeInput{idleWorkers: 3}},
		{name: "active beacon job blocks", in: helperUpgradeInput{activeBeaconJobs: 1}, want: true},
		{name: "queued Teams turn preserved for helper restart", in: helperUpgradeInput{queuedTeamsTurns: 1}},
		{name: "running Teams turn blocks", in: helperUpgradeInput{runningTeamsTurns: 1}, want: true},
		{name: "outbox blocks", in: helperUpgradeInput{blockingOutbox: 1}, want: true},
		{name: "force may bypass stale owner marker only", in: helperUpgradeInput{staleOwnerMarkers: 1, force: true}},
		{name: "force does not bypass unreconciled active job", in: helperUpgradeInput{activeBeaconJobs: 1, force: true}, want: true},
		{name: "force does not bypass unreconciled marker", in: helperUpgradeInput{unreconciledMarkers: 1, force: true}, want: true},
		{name: "force does not bypass started beacon job", in: helperUpgradeInput{startedBeaconJobs: 1, force: true}, want: true},
		{name: "force does not bypass running Teams turn", in: helperUpgradeInput{runningTeamsTurns: 1, force: true}, want: true},
		{name: "force does not bypass protected outbox", in: helperUpgradeInput{blockingOutbox: 1, force: true}, want: true},
		{name: "force does not bypass protocol mismatch", in: helperUpgradeInput{protocolMismatchJobs: 1, force: true}, want: true},
	}
	for _, tt := range tests {
		if got := helperUpgradeBlocked(tt.in); got != tt.want {
			t.Fatalf("%s: got %v want %v", tt.name, got, tt.want)
		}
	}
}

func TestCrossFeatureScenariosExposeUnsafeCombinations(t *testing.T) {
	type scenario struct {
		name              string
		claim             claimInput
		helperUpgrade     helperUpgradeInput
		codexUpgrade      codexUpgradeInput
		recoverState      jobState
		workerAlive       bool
		terminalIntegrity terminalIntegrity
		wantClaim         bool
		wantHelperBlocked bool
		wantCodexUpgrade  bool
		wantRecover       recoveryAction
	}

	baseClaim := claimInput{
		leaseState:     leaseAccepting,
		protocol:       protocolFull,
		remainingTTL:   120,
		requiredTTL:    60,
		signatureMatch: true,
		slotAvailable:  true,
		jobState:       jobQueued,
	}
	baseUpgrade := codexUpgradeInput{
		scoped:             true,
		origin:             originManagedPersistent,
		targetLockAcquired: true,
		maintenanceWorker:  true,
	}
	tests := []scenario{
		{
			name:              "healthy idle worker can claim and does not block helper upgrade",
			claim:             baseClaim,
			helperUpgrade:     helperUpgradeInput{idleWorkers: 1},
			codexUpgrade:      baseUpgrade,
			recoverState:      jobQueued,
			terminalIntegrity: terminalNone,
			wantClaim:         true,
			wantCodexUpgrade:  true,
			wantRecover:       recoverRequeue,
		},
		{
			name:              "service drain stops new claims while active work blocks helper upgrade",
			claim:             with(baseClaim, func(in *claimInput) { in.serviceDraining = true }),
			helperUpgrade:     helperUpgradeInput{activeBeaconJobs: 1},
			codexUpgrade:      copyCodexUpgrade(baseUpgrade, func(in *codexUpgradeInput) { in.activeSameTarget = true }),
			recoverState:      jobStarted,
			terminalIntegrity: terminalNone,
			wantHelperBlocked: true,
			wantRecover:       recoverAmbiguous,
		},
		{
			name:              "protocol mismatch drains worker and quarantines active state",
			claim:             with(baseClaim, func(in *claimInput) { in.protocol = protocolMajorMismatch }),
			helperUpgrade:     helperUpgradeInput{activeBeaconJobs: 1, force: true},
			codexUpgrade:      copyCodexUpgrade(baseUpgrade, func(in *codexUpgradeInput) { in.activeSameTarget = true }),
			recoverState:      jobTerminal,
			terminalIntegrity: terminalLateWrite,
			wantHelperBlocked: true,
			wantRecover:       recoverQuarantine,
		},
		{
			name:              "readonly image prevents Codex upgrade even when helper upgrade is idle",
			claim:             with(baseClaim, func(in *claimInput) { in.signatureMatch = false }),
			helperUpgrade:     helperUpgradeInput{idleWorkers: 1},
			codexUpgrade:      copyCodexUpgrade(baseUpgrade, func(in *codexUpgradeInput) { in.origin = originReadOnlyImage }),
			recoverState:      jobQueued,
			terminalIntegrity: terminalNone,
			wantRecover:       recoverRequeue,
		},
	}

	for _, tt := range tests {
		if got := canClaim(tt.claim); got != tt.wantClaim {
			t.Fatalf("%s claim: got %v want %v", tt.name, got, tt.wantClaim)
		}
		if got := helperUpgradeBlocked(tt.helperUpgrade); got != tt.wantHelperBlocked {
			t.Fatalf("%s helper upgrade: got %v want %v", tt.name, got, tt.wantHelperBlocked)
		}
		if got := canUpgradeCodex(tt.codexUpgrade); got != tt.wantCodexUpgrade {
			t.Fatalf("%s codex upgrade: got %v want %v", tt.name, got, tt.wantCodexUpgrade)
		}
		if got := recoverJob(tt.recoverState, tt.workerAlive, tt.terminalIntegrity); got != tt.wantRecover {
			t.Fatalf("%s recover: got %s want %s", tt.name, got, tt.wantRecover)
		}
	}
}

func copyCodexUpgrade(in codexUpgradeInput, fn func(*codexUpgradeInput)) codexUpgradeInput {
	fn(&in)
	return in
}

type cleanupAction string

const (
	cleanupNone       cleanupAction = "none"
	cleanupTombstone  cleanupAction = "tombstone"
	cleanupDelete     cleanupAction = "delete"
	cleanupQuarantine cleanupAction = "quarantine"
)

type cleanupInput struct {
	leaseAlive       bool
	schedulerAlive   bool
	jobState         jobState
	retentionExpired bool
}

func cleanupDecision(in cleanupInput) cleanupAction {
	if in.leaseAlive || in.schedulerAlive {
		return cleanupNone
	}
	switch in.jobState {
	case jobStarted, jobStartIntent:
		return cleanupQuarantine
	case jobTerminal:
		return cleanupTombstone
	case jobTombstoned:
		if in.retentionExpired {
			return cleanupDelete
		}
		return cleanupTombstone
	default:
		return cleanupTombstone
	}
}

func TestCleanupNeverDeletesLiveOrStartedAmbiguousWork(t *testing.T) {
	tests := []struct {
		name string
		in   cleanupInput
		want cleanupAction
	}{
		{name: "live lease untouched", in: cleanupInput{leaseAlive: true, jobState: jobStarted}, want: cleanupNone},
		{name: "scheduler alive untouched", in: cleanupInput{schedulerAlive: true, jobState: jobStarted}, want: cleanupNone},
		{name: "started lost quarantined", in: cleanupInput{jobState: jobStarted}, want: cleanupQuarantine},
		{name: "terminal retained before retention", in: cleanupInput{jobState: jobTerminal}, want: cleanupTombstone},
		{name: "terminal first tombstoned even after retention", in: cleanupInput{jobState: jobTerminal, retentionExpired: true}, want: cleanupTombstone},
		{name: "tombstone deleted after retention", in: cleanupInput{jobState: jobTombstoned, retentionExpired: true}, want: cleanupDelete},
	}
	for _, tt := range tests {
		if got := cleanupDecision(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}
