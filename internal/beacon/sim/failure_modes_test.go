package sim

import "testing"

type allocationState string

const (
	allocationSubmitted allocationState = "submitted"
	allocationPending   allocationState = "pending"
	allocationRunning   allocationState = "running"
	allocationFailed    allocationState = "failed"
	allocationCanceled  allocationState = "canceled"
	allocationExpired   allocationState = "expired"
)

type allocationDecision string

const (
	allocationWait        allocationDecision = "wait"
	allocationStartWorker allocationDecision = "start_worker"
	allocationRetry       allocationDecision = "retry"
	allocationUserAction  allocationDecision = "user_action"
	allocationTerminal    allocationDecision = "terminal"
)

type allocationInput struct {
	state          allocationState
	transientError bool
	userCanceled   bool
	duplicateSeen  bool
}

func reconcileAllocation(in allocationInput) allocationDecision {
	if in.userCanceled {
		return allocationTerminal
	}
	if in.duplicateSeen {
		return allocationUserAction
	}
	switch in.state {
	case allocationSubmitted, allocationPending:
		return allocationWait
	case allocationRunning:
		return allocationStartWorker
	case allocationFailed:
		if in.transientError {
			return allocationRetry
		}
		return allocationUserAction
	case allocationCanceled, allocationExpired:
		return allocationTerminal
	default:
		return allocationUserAction
	}
}

func TestAllocationLifecycleSeparatesPendingFromHungWorker(t *testing.T) {
	tests := []struct {
		name string
		in   allocationInput
		want allocationDecision
	}{
		{name: "submitted waits", in: allocationInput{state: allocationSubmitted}, want: allocationWait},
		{name: "pending waits", in: allocationInput{state: allocationPending}, want: allocationWait},
		{name: "running starts worker", in: allocationInput{state: allocationRunning}, want: allocationStartWorker},
		{name: "transient failure retries", in: allocationInput{state: allocationFailed, transientError: true}, want: allocationRetry},
		{name: "permanent failure needs user action", in: allocationInput{state: allocationFailed}, want: allocationUserAction},
		{name: "user canceled terminal", in: allocationInput{state: allocationPending, userCanceled: true}, want: allocationTerminal},
		{name: "duplicate provider job needs user action", in: allocationInput{state: allocationPending, duplicateSeen: true}, want: allocationUserAction},
	}
	for _, tt := range tests {
		if got := reconcileAllocation(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

type bootstrapCheck struct {
	sharedRootMounted  bool
	atomicCreateOK     bool
	freeBytesOK        bool
	freeInodesOK       bool
	codexAvailable     bool
	homeWritable       bool
	proxyOK            bool
	imageDigestMatch   bool
	protocolOK         bool
	containerRuntimeOK bool
	modulesOK          bool
	bindMountsOK       bool
	tmpWritable        bool
	authPathOK         bool
	proxyEnvInsideOK   bool
}

func bootstrapAccepting(in bootstrapCheck) bool {
	return in.sharedRootMounted &&
		in.atomicCreateOK &&
		in.freeBytesOK &&
		in.freeInodesOK &&
		in.codexAvailable &&
		in.homeWritable &&
		in.proxyOK &&
		in.imageDigestMatch &&
		in.protocolOK &&
		in.containerRuntimeOK &&
		in.modulesOK &&
		in.bindMountsOK &&
		in.tmpWritable &&
		in.authPathOK &&
		in.proxyEnvInsideOK
}

func TestWorkerBootstrapDoctorBlocksBadMachinesBeforeAccepting(t *testing.T) {
	base := bootstrapCheck{
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
	if !bootstrapAccepting(base) {
		t.Fatal("healthy worker should become accepting")
	}
	cases := map[string]bootstrapCheck{
		"missing shared root":                copyBootstrap(base, func(in *bootstrapCheck) { in.sharedRootMounted = false }),
		"atomic create fails":                copyBootstrap(base, func(in *bootstrapCheck) { in.atomicCreateOK = false }),
		"disk full":                          copyBootstrap(base, func(in *bootstrapCheck) { in.freeBytesOK = false }),
		"inodes full":                        copyBootstrap(base, func(in *bootstrapCheck) { in.freeInodesOK = false }),
		"missing codex":                      copyBootstrap(base, func(in *bootstrapCheck) { in.codexAvailable = false }),
		"home readonly":                      copyBootstrap(base, func(in *bootstrapCheck) { in.homeWritable = false }),
		"bad proxy":                          copyBootstrap(base, func(in *bootstrapCheck) { in.proxyOK = false }),
		"tag drift":                          copyBootstrap(base, func(in *bootstrapCheck) { in.imageDigestMatch = false }),
		"bad protocol":                       copyBootstrap(base, func(in *bootstrapCheck) { in.protocolOK = false }),
		"missing container runtime":          copyBootstrap(base, func(in *bootstrapCheck) { in.containerRuntimeOK = false }),
		"missing modules":                    copyBootstrap(base, func(in *bootstrapCheck) { in.modulesOK = false }),
		"missing bind mount":                 copyBootstrap(base, func(in *bootstrapCheck) { in.bindMountsOK = false }),
		"tmp readonly":                       copyBootstrap(base, func(in *bootstrapCheck) { in.tmpWritable = false }),
		"auth path unavailable":              copyBootstrap(base, func(in *bootstrapCheck) { in.authPathOK = false }),
		"proxy env missing inside container": copyBootstrap(base, func(in *bootstrapCheck) { in.proxyEnvInsideOK = false }),
	}
	for name, in := range cases {
		if bootstrapAccepting(in) {
			t.Fatalf("%s should prevent accepting: %#v", name, in)
		}
	}
}

func copyBootstrap(in bootstrapCheck, fn func(*bootstrapCheck)) bootstrapCheck {
	fn(&in)
	return in
}

type failurePhase string
type failureKind string
type failureOutcome string

const (
	phaseBeforeProcessStart failurePhase = "before_process_start"
	phaseAfterProcessStart  failurePhase = "after_process_start"
	phaseTerminalWritten    failurePhase = "terminal_written"

	failureAllocationDenied failureKind = "allocation_denied"
	failureSchedulerTemp    failureKind = "scheduler_temporary"
	failureOOMKill          failureKind = "oom_kill"
	failureGPUOOMTerminal   failureKind = "gpu_oom_terminal"
	failureWalltimeKill     failureKind = "walltime_kill"
	failureDiskFull         failureKind = "disk_full"
	failureAdminCancel      failureKind = "admin_cancel"
	failureNodeReboot       failureKind = "node_reboot"

	outcomeRequeueSafe    failureOutcome = "requeue_safe"
	outcomeRetryRequest   failureOutcome = "retry_request"
	outcomeUserAction     failureOutcome = "user_action"
	outcomeAmbiguous      failureOutcome = "ambiguous"
	outcomeFailedResult   failureOutcome = "failed_result"
	outcomeComplete       failureOutcome = "complete"
	outcomeNoClaim        failureOutcome = "no_claim"
	outcomeQuarantine     failureOutcome = "quarantine"
	outcomeStoreAttention failureOutcome = "store_needs_attention"
)

func classifyFailure(kind failureKind, phase failurePhase, explicitBeacon bool) failureOutcome {
	if phase == phaseTerminalWritten {
		if kind == failureDiskFull {
			return outcomeStoreAttention
		}
		return outcomeComplete
	}
	switch kind {
	case failureAllocationDenied:
		return outcomeUserAction
	case failureSchedulerTemp:
		return outcomeRetryRequest
	case failureGPUOOMTerminal:
		return outcomeFailedResult
	case failureDiskFull:
		if phase == phaseBeforeProcessStart {
			return outcomeNoClaim
		}
		return outcomeAmbiguous
	case failureOOMKill, failureWalltimeKill, failureAdminCancel, failureNodeReboot:
		if phase == phaseBeforeProcessStart {
			return outcomeRequeueSafe
		}
		return outcomeAmbiguous
	default:
		if explicitBeacon {
			return outcomeUserAction
		}
		return outcomeQuarantine
	}
}

func TestFailureModesNeverReplayAfterCodexMayHaveStarted(t *testing.T) {
	tests := []struct {
		name  string
		kind  failureKind
		phase failurePhase
		want  failureOutcome
	}{
		{name: "oom before start requeues", kind: failureOOMKill, phase: phaseBeforeProcessStart, want: outcomeRequeueSafe},
		{name: "oom after start ambiguous", kind: failureOOMKill, phase: phaseAfterProcessStart, want: outcomeAmbiguous},
		{name: "walltime after start ambiguous", kind: failureWalltimeKill, phase: phaseAfterProcessStart, want: outcomeAmbiguous},
		{name: "admin cancel after start ambiguous", kind: failureAdminCancel, phase: phaseAfterProcessStart, want: outcomeAmbiguous},
		{name: "node reboot after start ambiguous", kind: failureNodeReboot, phase: phaseAfterProcessStart, want: outcomeAmbiguous},
		{name: "gpu oom terminal is failed result", kind: failureGPUOOMTerminal, phase: phaseAfterProcessStart, want: outcomeFailedResult},
		{name: "disk full before start does not claim", kind: failureDiskFull, phase: phaseBeforeProcessStart, want: outcomeNoClaim},
		{name: "disk full after start ambiguous", kind: failureDiskFull, phase: phaseAfterProcessStart, want: outcomeAmbiguous},
		{name: "disk full during terminal write needs store attention", kind: failureDiskFull, phase: phaseTerminalWritten, want: outcomeStoreAttention},
		{name: "valid terminal wins", kind: failureWalltimeKill, phase: phaseTerminalWritten, want: outcomeComplete},
		{name: "allocation denied needs user action", kind: failureAllocationDenied, phase: phaseBeforeProcessStart, want: outcomeUserAction},
		{name: "scheduler temporary retries request", kind: failureSchedulerTemp, phase: phaseBeforeProcessStart, want: outcomeRetryRequest},
	}
	for _, tt := range tests {
		if got := classifyFailure(tt.kind, tt.phase, true); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

type releaseInput struct {
	allocation    allocationState
	job           jobState
	externalOwned bool
	hardKill      bool
	exactID       bool
	confirmed     bool
}

type releaseOutcome string

const (
	releaseDrain          releaseOutcome = "drain"
	releaseCancelPending  releaseOutcome = "cancel_pending"
	releaseReject         releaseOutcome = "reject"
	releaseKillQuarantine releaseOutcome = "kill_quarantine"
	releaseFreeIdle       releaseOutcome = "free_idle"
)

func decideRelease(in releaseInput) releaseOutcome {
	if in.hardKill {
		if !in.exactID || !in.confirmed {
			return releaseReject
		}
		if in.externalOwned && in.job == jobStarted {
			return releaseReject
		}
		return releaseKillQuarantine
	}
	if in.allocation == allocationPending || in.allocation == allocationSubmitted {
		return releaseCancelPending
	}
	if in.job == jobStarted || in.job == jobStartIntent {
		return releaseDrain
	}
	return releaseFreeIdle
}

func TestReleaseAndKillAreConservativeAndUserVisible(t *testing.T) {
	tests := []struct {
		name string
		in   releaseInput
		want releaseOutcome
	}{
		{name: "release pending cancels request", in: releaseInput{allocation: allocationPending, job: jobQueued}, want: releaseCancelPending},
		{name: "release running job drains", in: releaseInput{allocation: allocationRunning, job: jobStarted}, want: releaseDrain},
		{name: "release idle frees", in: releaseInput{allocation: allocationRunning, job: jobQueued}, want: releaseFreeIdle},
		{name: "hard kill requires exact id", in: releaseInput{allocation: allocationRunning, job: jobStarted, hardKill: true, confirmed: true}, want: releaseReject},
		{name: "hard kill requires confirmation", in: releaseInput{allocation: allocationRunning, job: jobStarted, hardKill: true, exactID: true}, want: releaseReject},
		{name: "hard kill quarantines started work", in: releaseInput{allocation: allocationRunning, job: jobStarted, hardKill: true, exactID: true, confirmed: true}, want: releaseKillQuarantine},
		{name: "external owned running job not killed by default", in: releaseInput{allocation: allocationRunning, job: jobStarted, externalOwned: true, hardKill: true, exactID: true, confirmed: true}, want: releaseReject},
	}
	for _, tt := range tests {
		if got := decideRelease(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

type canonicalSignature struct {
	provider    string
	queue       string
	resource    string
	gpuType     string
	gpuCount    int
	imageDigest string
	mountDigest string
	envDigest   string
	installID   string
	protocol    int
	isolation   isolation
}

func signaturesCompatible(a, b canonicalSignature) bool {
	return a == b
}

func TestExecutionSignatureUsesCanonicalFieldsNotDisplayName(t *testing.T) {
	base := canonicalSignature{
		provider:    "slurm",
		queue:       "interactive",
		resource:    "gpu",
		gpuType:     "h100",
		gpuCount:    1,
		imageDigest: "sha256:image-a",
		mountDigest: "sha256:mounts-a",
		envDigest:   "sha256:env-a",
		installID:   "codex-managed-a",
		protocol:    1,
		isolation:   isolationShared,
	}
	if !signaturesCompatible(base, base) {
		t.Fatal("identical signatures should be compatible")
	}
	cases := map[string]canonicalSignature{
		"provider":     copySignature(base, func(s *canonicalSignature) { s.provider = "lsf" }),
		"queue":        copySignature(base, func(s *canonicalSignature) { s.queue = "cpu" }),
		"resource":     copySignature(base, func(s *canonicalSignature) { s.resource = "cpu" }),
		"gpu type":     copySignature(base, func(s *canonicalSignature) { s.gpuType = "a100" }),
		"gpu count":    copySignature(base, func(s *canonicalSignature) { s.gpuCount = 2 }),
		"image digest": copySignature(base, func(s *canonicalSignature) { s.imageDigest = "sha256:image-b" }),
		"mounts":       copySignature(base, func(s *canonicalSignature) { s.mountDigest = "sha256:mounts-b" }),
		"env":          copySignature(base, func(s *canonicalSignature) { s.envDigest = "sha256:env-b" }),
		"install":      copySignature(base, func(s *canonicalSignature) { s.installID = "codex-managed-b" }),
		"protocol":     copySignature(base, func(s *canonicalSignature) { s.protocol = 2 }),
		"isolation":    copySignature(base, func(s *canonicalSignature) { s.isolation = isolationExclusive }),
	}
	for name, sig := range cases {
		if signaturesCompatible(base, sig) {
			t.Fatalf("%s mismatch should be incompatible", name)
		}
	}
}

func copySignature(in canonicalSignature, fn func(*canonicalSignature)) canonicalSignature {
	fn(&in)
	return in
}
