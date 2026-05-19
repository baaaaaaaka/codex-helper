package sim

import (
	"fmt"
	"strings"
	"testing"
)

type remainingSmokeOutcome string

const (
	remainingSmokePass           remainingSmokeOutcome = "pass"
	remainingSmokeMissingService remainingSmokeOutcome = "missing_service_env"
	remainingSmokeNotExecutable  remainingSmokeOutcome = "not_executable"
	remainingSmokeBadOutput      remainingSmokeOutcome = "bad_output"
)

type remainingAdapterOp string

const (
	remainingOpQuery  remainingAdapterOp = "query"
	remainingOpSubmit remainingAdapterOp = "submit"
	remainingOpCancel remainingAdapterOp = "cancel"
	remainingOpRenew  remainingAdapterOp = "renew"
)

type remainingServiceSmoke struct {
	serviceEnv map[remainingAdapterOp]string
	cliEnv     map[remainingAdapterOp]string
	executable map[string]bool
	output     map[remainingAdapterOp]string
	require    []remainingAdapterOp
}

func runRemainingServiceSmoke(in remainingServiceSmoke) remainingSmokeOutcome {
	for _, op := range in.require {
		cmd := strings.TrimSpace(in.serviceEnv[op])
		if cmd == "" {
			return remainingSmokeMissingService
		}
		if !in.executable[cmd] {
			return remainingSmokeNotExecutable
		}
		if !remainingProviderOutputParseable(in.output[op]) {
			return remainingSmokeBadOutput
		}
	}
	return remainingSmokePass
}

func remainingProviderOutputParseable(out string) bool {
	out = strings.TrimSpace(out)
	if strings.HasPrefix(out, "{") && strings.HasSuffix(out, "}") && strings.Contains(out, "provider_job_id") {
		return true
	}
	return strings.Contains(out, "=") && (strings.Contains(out, "provider_job_id=") || strings.Contains(out, "job_id="))
}

func TestRemainingPlanServiceEnvSmokeUsesHelperServiceEnvironment(t *testing.T) {
	base := remainingServiceSmoke{
		serviceEnv: map[remainingAdapterOp]string{
			remainingOpQuery:  "/svc/query",
			remainingOpSubmit: "/svc/submit",
			remainingOpCancel: "/svc/cancel",
			remainingOpRenew:  "/svc/renew",
		},
		cliEnv: map[remainingAdapterOp]string{
			remainingOpQuery:  "/cli/query",
			remainingOpSubmit: "/cli/submit",
			remainingOpCancel: "/cli/cancel",
			remainingOpRenew:  "/cli/renew",
		},
		executable: map[string]bool{
			"/svc/query":  true,
			"/svc/submit": true,
			"/svc/cancel": true,
			"/svc/renew":  true,
			"/cli/query":  true,
			"/cli/submit": true,
			"/cli/cancel": true,
			"/cli/renew":  true,
		},
		output: map[remainingAdapterOp]string{
			remainingOpQuery:  `{"provider_job_id":"job-1","raw_state":"PD"}`,
			remainingOpSubmit: `provider_job_id=job-1 raw_state=PD`,
			remainingOpCancel: `provider_job_id=job-1 raw_state=CA`,
			remainingOpRenew:  `provider_job_id=job-1 raw_state=R`,
		},
		require: []remainingAdapterOp{remainingOpQuery, remainingOpSubmit, remainingOpCancel, remainingOpRenew},
	}
	if got := runRemainingServiceSmoke(base); got != remainingSmokePass {
		t.Fatalf("complete service-visible adapter smoke should pass, got %s", got)
	}

	cases := []struct {
		name string
		in   remainingServiceSmoke
		want remainingSmokeOutcome
	}{
		{
			name: "cli env does not satisfy helper service env",
			in: copyRemainingSmoke(base, func(in *remainingServiceSmoke) {
				in.serviceEnv = map[remainingAdapterOp]string{}
			}),
			want: remainingSmokeMissingService,
		},
		{
			name: "all required operations must exist",
			in: copyRemainingSmoke(base, func(in *remainingServiceSmoke) {
				delete(in.serviceEnv, remainingOpCancel)
			}),
			want: remainingSmokeMissingService,
		},
		{
			name: "adapter must be executable",
			in: copyRemainingSmoke(base, func(in *remainingServiceSmoke) {
				in.executable["/svc/renew"] = false
			}),
			want: remainingSmokeNotExecutable,
		},
		{
			name: "adapter output must be structured",
			in: copyRemainingSmoke(base, func(in *remainingServiceSmoke) {
				in.output[remainingOpQuery] = "queued on some node"
			}),
			want: remainingSmokeBadOutput,
		},
	}
	for _, tc := range cases {
		if got := runRemainingServiceSmoke(tc.in); got != tc.want {
			t.Fatalf("%s: got %s want %s", tc.name, got, tc.want)
		}
	}
}

func copyRemainingSmoke(in remainingServiceSmoke, fn func(*remainingServiceSmoke)) remainingServiceSmoke {
	in.serviceEnv = copyRemainingOpStringMap(in.serviceEnv)
	in.cliEnv = copyRemainingOpStringMap(in.cliEnv)
	in.executable = copyRemainingStringBoolMap(in.executable)
	in.output = copyRemainingOpStringMap(in.output)
	in.require = append([]remainingAdapterOp(nil), in.require...)
	fn(&in)
	return in
}

func copyRemainingOpStringMap(in map[remainingAdapterOp]string) map[remainingAdapterOp]string {
	out := make(map[remainingAdapterOp]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func copyRemainingStringBoolMap(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type remainingSchedulerOp string

const (
	remainingEnsureDemand   remainingSchedulerOp = "ensure_demand"
	remainingProviderRun    remainingSchedulerOp = "provider_running"
	remainingRegisterWorker remainingSchedulerOp = "register_worker"
	remainingStartTurn      remainingSchedulerOp = "start_turn"
	remainingRenew          remainingSchedulerOp = "renew"
	remainingRelease        remainingSchedulerOp = "release"
	remainingProviderGone   remainingSchedulerOp = "provider_gone"
	remainingReconcile      remainingSchedulerOp = "reconcile"
)

type remainingSchedulerModel struct {
	demand           bool
	providerJob      string
	providerRunning  bool
	workerRegistered bool
	turnStarted      bool
	releaseRequested bool
	drainingStarted  bool
	needsAttention   bool
	cleaned          bool
	submitCount      int
	cancelCount      int
	renewCount       int
	renewAfterStop   int
	resubmitStarted  bool
	trace            []remainingSchedulerOp
}

func newRemainingSchedulerModel() *remainingSchedulerModel {
	return &remainingSchedulerModel{demand: true}
}

func (m *remainingSchedulerModel) apply(op remainingSchedulerOp) {
	m.trace = append(m.trace, op)
	switch op {
	case remainingEnsureDemand:
		if m.demand && !m.releaseRequested && m.providerJob == "" && !m.needsAttention {
			m.submitCount++
			if m.turnStarted {
				m.resubmitStarted = true
			}
			m.providerJob = fmt.Sprintf("job-%d", m.submitCount)
			m.providerRunning = false
			m.workerRegistered = false
			m.cleaned = false
		}
	case remainingProviderRun:
		if m.providerJob != "" && !m.releaseRequested {
			m.providerRunning = true
		}
	case remainingRegisterWorker:
		if m.providerJob != "" && m.providerRunning && !m.releaseRequested {
			m.workerRegistered = true
		}
	case remainingStartTurn:
		if m.workerRegistered && !m.releaseRequested {
			m.turnStarted = true
		}
	case remainingRenew:
		if m.providerJob != "" && !m.releaseRequested && !m.needsAttention {
			m.renewCount++
		} else if m.releaseRequested || m.providerJob == "" || m.needsAttention {
			m.renewAfterStop++
		}
	case remainingRelease:
		m.demand = false
		m.releaseRequested = true
		if m.turnStarted {
			m.drainingStarted = true
			return
		}
		if m.providerJob != "" {
			m.cancelCount++
			m.providerJob = ""
			m.providerRunning = false
			m.workerRegistered = false
		}
		m.cleaned = true
	case remainingProviderGone:
		if m.providerJob == "" {
			return
		}
		m.providerJob = ""
		m.providerRunning = false
		m.workerRegistered = false
		if m.turnStarted {
			m.needsAttention = true
			return
		}
		m.cleaned = true
	case remainingReconcile:
		if m.releaseRequested && m.providerJob == "" && !m.turnStarted {
			m.cleaned = true
		}
	}
}

func TestRemainingPlanSchedulerReleaseRenewAndProviderLossGoldens(t *testing.T) {
	cases := []struct {
		name string
		ops  []remainingSchedulerOp
		want func(*testing.T, *remainingSchedulerModel)
	}{
		{
			name: "release cancels provider job even without registered machine",
			ops:  []remainingSchedulerOp{remainingEnsureDemand, remainingProviderRun, remainingRelease, remainingReconcile},
			want: func(t *testing.T, m *remainingSchedulerModel) {
				if m.cancelCount != 1 || m.providerJob != "" || !m.cleaned {
					t.Fatalf("release should cancel no-machine provider job and clean state: %#v", m)
				}
			},
		},
		{
			name: "release after possible start drains instead of killing provider",
			ops:  []remainingSchedulerOp{remainingEnsureDemand, remainingProviderRun, remainingRegisterWorker, remainingStartTurn, remainingRelease, remainingRenew},
			want: func(t *testing.T, m *remainingSchedulerModel) {
				if m.cancelCount != 0 || !m.drainingStarted || m.renewAfterStop != 1 {
					t.Fatalf("started work should drain and reject later renew: %#v", m)
				}
			},
		},
		{
			name: "provider gone before start may be replaced once demand still exists",
			ops:  []remainingSchedulerOp{remainingEnsureDemand, remainingProviderRun, remainingProviderGone, remainingReconcile, remainingEnsureDemand},
			want: func(t *testing.T, m *remainingSchedulerModel) {
				if m.submitCount != 2 || m.needsAttention || m.resubmitStarted {
					t.Fatalf("pre-start provider loss should allow replacement without needs attention: %#v", m)
				}
			},
		},
		{
			name: "provider gone after start needs attention and must not resubmit",
			ops:  []remainingSchedulerOp{remainingEnsureDemand, remainingProviderRun, remainingRegisterWorker, remainingStartTurn, remainingProviderGone, remainingReconcile, remainingEnsureDemand},
			want: func(t *testing.T, m *remainingSchedulerModel) {
				if !m.needsAttention || m.submitCount != 1 || m.resubmitStarted {
					t.Fatalf("post-start provider loss must not replay user work: %#v", m)
				}
			},
		},
		{
			name: "release before provider-gone does not resubmit later",
			ops:  []remainingSchedulerOp{remainingEnsureDemand, remainingRelease, remainingProviderGone, remainingReconcile, remainingEnsureDemand},
			want: func(t *testing.T, m *remainingSchedulerModel) {
				if m.submitCount != 1 || m.cancelCount != 1 || m.providerJob != "" || !m.cleaned {
					t.Fatalf("released demand should stay released after provider disappears: %#v", m)
				}
			},
		},
	}
	for _, tc := range cases {
		model := newRemainingSchedulerModel()
		for _, op := range tc.ops {
			model.apply(op)
		}
		tc.want(t, model)
	}
}

func TestRemainingPlanGeneratedSchedulerOrderCasesKeepSafetyInvariants(t *testing.T) {
	ops := []remainingSchedulerOp{
		remainingEnsureDemand,
		remainingProviderRun,
		remainingRegisterWorker,
		remainingStartTurn,
		remainingRenew,
		remainingRelease,
		remainingProviderGone,
		remainingReconcile,
	}
	walkRemainingGeneratedSequences(ops, 5, func(seq []remainingSchedulerOp) {
		model := newRemainingSchedulerModel()
		for _, op := range seq {
			model.apply(op)
		}
		trace := remainingTrace(seq)
		if model.cancelCount > model.submitCount {
			t.Fatalf("%s: cancel count exceeded submitted provider jobs: %#v", trace, model)
		}
		if model.resubmitStarted {
			t.Fatalf("%s: provider loss after possible start must not resubmit: %#v", trace, model)
		}
		if model.needsAttention && model.submitCount > 1 {
			t.Fatalf("%s: needs-attention started work must not get replacement submit: %#v", trace, model)
		}
	})
}

type remainingLoadScheduler struct {
	limitPerTick int
	tick         int
	perTick      map[int]int
	submitted    map[string]bool
	submitCount  map[string]int
	nextTry      map[string]int
}

func newRemainingLoadScheduler(limitPerTick int) *remainingLoadScheduler {
	return &remainingLoadScheduler{
		limitPerTick: limitPerTick,
		perTick:      map[int]int{},
		submitted:    map[string]bool{},
		submitCount:  map[string]int{},
		nextTry:      map[string]int{},
	}
}

func (s *remainingLoadScheduler) reconcile(requestID string) {
	if s.submitted[requestID] || s.tick < s.nextTry[requestID] {
		return
	}
	if s.perTick[s.tick] >= s.limitPerTick {
		s.nextTry[requestID] = s.tick + 1 + len(requestID)%3
		return
	}
	s.perTick[s.tick]++
	s.submitted[requestID] = true
	s.submitCount[requestID]++
}

func (s *remainingLoadScheduler) advance() {
	s.tick++
}

func TestRemainingPlanFakeSchedulerLoadBackoffStaysIdempotent(t *testing.T) {
	scheduler := newRemainingLoadScheduler(3)
	var requests []string
	for i := 0; i < 17; i++ {
		requests = append(requests, fmt.Sprintf("req-%02d", i))
	}
	for tick := 0; tick < 30; tick++ {
		for _, requestID := range requests {
			scheduler.reconcile(requestID)
			scheduler.reconcile(requestID)
		}
		scheduler.advance()
	}
	for _, requestID := range requests {
		if !scheduler.submitted[requestID] || scheduler.submitCount[requestID] != 1 {
			t.Fatalf("%s should be submitted exactly once under fake load, submitted=%v count=%d", requestID, scheduler.submitted[requestID], scheduler.submitCount[requestID])
		}
	}
	for tick, count := range scheduler.perTick {
		if count > scheduler.limitPerTick {
			t.Fatalf("tick %d submitted %d jobs above limit %d", tick, count, scheduler.limitPerTick)
		}
	}
}

func walkRemainingGeneratedSequences(ops []remainingSchedulerOp, length int, visit func([]remainingSchedulerOp)) {
	var walk func([]remainingSchedulerOp, int)
	walk = func(prefix []remainingSchedulerOp, remaining int) {
		if remaining == 0 {
			visit(append([]remainingSchedulerOp(nil), prefix...))
			return
		}
		for _, op := range ops {
			walk(append(prefix, op), remaining-1)
		}
	}
	walk(nil, length)
}

func remainingTrace(seq []remainingSchedulerOp) string {
	parts := make([]string, 0, len(seq))
	for _, op := range seq {
		parts = append(parts, string(op))
	}
	return strings.Join(parts, " -> ")
}

type remainingPrewarmMachine struct {
	id              string
	profileRevision string
	signature       string
	idleSeconds     int
	maxIdleSeconds  int
	used            resourceVector
}

type remainingResourceDemand struct {
	chat            string
	profileRevision string
	signature       string
	request         resourceRequest
}

func remainingCanUsePrewarm(machine remainingPrewarmMachine, demand remainingResourceDemand, capacity resourceVector) bool {
	return machine.profileRevision == demand.profileRevision &&
		machine.signature == demand.signature &&
		machine.idleSeconds <= machine.maxIdleSeconds &&
		resourceCanReserve(capacity, machine.used, demand.request)
}

func TestRemainingPlanResourceAccountingCoversPrewarmSharedAndExclusive(t *testing.T) {
	capacity := resourceVector{
		cpus:      16,
		memGB:     128,
		gpuSlices: map[string]int{"gpu0": 1, "gpu1": 1},
		licenses:  map[string]int{"eda": 1},
	}
	prewarm := remainingPrewarmMachine{
		id:              "warm-a",
		profileRevision: "gpu@2",
		signature:       "sig-a",
		maxIdleSeconds:  600,
		used:            resourceVector{gpuSlices: map[string]int{}, licenses: map[string]int{}},
	}
	if !remainingCanUsePrewarm(prewarm, remainingResourceDemand{profileRevision: "gpu@2", signature: "sig-a", request: resourceRequest{cpus: 4, memGB: 16, gpuSlice: "gpu0"}}, capacity) {
		t.Fatal("matching ready prewarm should satisfy compatible demand")
	}
	for name, machine := range map[string]remainingPrewarmMachine{
		"wrong revision": copyRemainingPrewarm(prewarm, func(m *remainingPrewarmMachine) { m.profileRevision = "gpu@1" }),
		"wrong signature": copyRemainingPrewarm(prewarm, func(m *remainingPrewarmMachine) {
			m.signature = "sig-b"
		}),
		"idle expired": copyRemainingPrewarm(prewarm, func(m *remainingPrewarmMachine) {
			m.idleSeconds = 601
		}),
		"same gpu occupied": copyRemainingPrewarm(prewarm, func(m *remainingPrewarmMachine) {
			m.used.gpuSlices = map[string]int{"gpu0": 1}
		}),
		"license occupied": copyRemainingPrewarm(prewarm, func(m *remainingPrewarmMachine) {
			m.used.licenses = map[string]int{"eda": 1}
		}),
	} {
		request := resourceRequest{cpus: 4, memGB: 16, gpuSlice: "gpu0"}
		if name == "license occupied" {
			request = resourceRequest{cpus: 1, memGB: 1, license: "eda"}
		}
		if remainingCanUsePrewarm(machine, remainingResourceDemand{profileRevision: "gpu@2", signature: "sig-a", request: request}, capacity) {
			t.Fatalf("%s prewarm should not be usable: %#v", name, machine)
		}
	}
	if resourceCanReserve(capacity, resourceVector{cpus: 4, memGB: 16, gpuSlices: map[string]int{"gpu0": 1}, licenses: map[string]int{}}, resourceRequest{exclusiveNode: true}) {
		t.Fatal("exclusive demand must not share a node with existing shared GPU use")
	}
}

func copyRemainingPrewarm(in remainingPrewarmMachine, fn func(*remainingPrewarmMachine)) remainingPrewarmMachine {
	in.used.gpuSlices = copyRemainingStringIntMap(in.used.gpuSlices)
	in.used.licenses = copyRemainingStringIntMap(in.used.licenses)
	fn(&in)
	return in
}

func copyRemainingStringIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type remainingBYOOrderOp string

const (
	remainingBYOAttach       remainingBYOOrderOp = "attach"
	remainingBYORelease      remainingBYOOrderOp = "release"
	remainingBYOForceRelease remainingBYOOrderOp = "force_release"
)

type remainingBYOOrderModel struct {
	attached                    bool
	attested                    bool
	coordinatorOwnsAllocation   bool
	explicitProviderKillAllowed bool
	detached                    bool
	providerKilled              bool
}

func (m *remainingBYOOrderModel) apply(op remainingBYOOrderOp) {
	switch op {
	case remainingBYOAttach:
		if m.attested {
			m.attached = true
		}
	case remainingBYORelease:
		if m.attached {
			m.detached = true
		}
	case remainingBYOForceRelease:
		if m.attached && (m.coordinatorOwnsAllocation || m.explicitProviderKillAllowed) {
			m.providerKilled = true
		}
	}
}

func TestRemainingPlanBYOReleaseOrdersNeverKillExternalProviderByDefault(t *testing.T) {
	permutations := [][]remainingBYOOrderOp{
		{remainingBYOAttach, remainingBYORelease, remainingBYOForceRelease},
		{remainingBYOAttach, remainingBYOForceRelease, remainingBYORelease},
		{remainingBYORelease, remainingBYOAttach, remainingBYOForceRelease},
	}
	for _, seq := range permutations {
		model := remainingBYOOrderModel{attested: true}
		for _, op := range seq {
			model.apply(op)
		}
		if model.providerKilled {
			t.Fatalf("%v: BYO release must not kill provider unless ownership or explicit kill is set: %#v", seq, model)
		}
	}
	managed := remainingBYOOrderModel{attested: true, coordinatorOwnsAllocation: true}
	for _, op := range []remainingBYOOrderOp{remainingBYOAttach, remainingBYOForceRelease} {
		managed.apply(op)
	}
	if !managed.providerKilled {
		t.Fatal("managed allocation force release should be allowed to kill provider")
	}
	externalOptIn := remainingBYOOrderModel{attested: true, explicitProviderKillAllowed: true}
	for _, op := range []remainingBYOOrderOp{remainingBYOAttach, remainingBYOForceRelease} {
		externalOptIn.apply(op)
	}
	if !externalOptIn.providerKilled {
		t.Fatal("explicit BYO provider-kill opt-in should allow force release")
	}
}

type remainingArtifactOp string

const (
	remainingArtifactTerminal remainingArtifactOp = "terminal"
	remainingArtifactOK       remainingArtifactOp = "artifact_ok"
	remainingArtifactBad      remainingArtifactOp = "artifact_bad"
	remainingArtifactGraceEnd remainingArtifactOp = "artifact_grace_end"
	remainingArtifactRecover  remainingArtifactOp = "recover"
)

type remainingArtifactPipeline struct {
	declared         int
	terminal         bool
	accepted         int
	attention        int
	protectedOutbox  int
	finalOutbox      int
	attentionOutbox  int
	completionQueued bool
}

func (p *remainingArtifactPipeline) apply(op remainingArtifactOp) {
	switch op {
	case remainingArtifactTerminal:
		p.terminal = true
	case remainingArtifactOK:
		if p.accepted+p.attention < p.declared {
			p.accepted++
		}
	case remainingArtifactBad:
		if p.accepted+p.attention < p.declared {
			p.attention++
		}
	case remainingArtifactGraceEnd:
		if p.terminal && p.accepted+p.attention < p.declared {
			p.attention = p.declared - p.accepted
		}
	case remainingArtifactRecover:
	}
	p.maybeQueue()
}

func (p *remainingArtifactPipeline) maybeQueue() {
	if p.completionQueued || !p.terminal || p.accepted+p.attention < p.declared {
		return
	}
	p.completionQueued = true
	p.protectedOutbox++
	if p.attention > 0 {
		p.attentionOutbox++
		return
	}
	p.finalOutbox++
}

func TestRemainingPlanArtifactPipelineOrdersQueueOneProtectedOutcome(t *testing.T) {
	for _, seq := range [][]remainingArtifactOp{
		{remainingArtifactTerminal, remainingArtifactOK, remainingArtifactRecover},
		{remainingArtifactOK, remainingArtifactTerminal, remainingArtifactRecover},
		{remainingArtifactRecover, remainingArtifactTerminal, remainingArtifactOK},
	} {
		pipeline := remainingArtifactPipeline{declared: 1}
		for _, op := range seq {
			pipeline.apply(op)
		}
		if pipeline.protectedOutbox != 1 || pipeline.finalOutbox != 1 || pipeline.attentionOutbox != 0 {
			t.Fatalf("%v: valid artifact pipeline should queue one protected final: %#v", seq, pipeline)
		}
	}
	for _, seq := range [][]remainingArtifactOp{
		{remainingArtifactTerminal, remainingArtifactBad, remainingArtifactRecover},
		{remainingArtifactBad, remainingArtifactTerminal, remainingArtifactRecover},
		{remainingArtifactRecover, remainingArtifactBad, remainingArtifactTerminal},
		{remainingArtifactTerminal, remainingArtifactRecover, remainingArtifactGraceEnd},
	} {
		pipeline := remainingArtifactPipeline{declared: 1}
		for _, op := range seq {
			pipeline.apply(op)
		}
		if pipeline.protectedOutbox != 1 || pipeline.finalOutbox != 0 || pipeline.attentionOutbox != 1 {
			t.Fatalf("%v: bad/missing artifact should queue one protected needs-attention: %#v", seq, pipeline)
		}
	}
}

type remainingServiceEnvField struct {
	name     string
	family   string
	required bool
	secret   bool
}

func remainingServiceConfigValid(fields []remainingServiceEnvField, rendered map[string]string) bool {
	seenFamilies := map[string]bool{}
	for _, field := range fields {
		value := strings.TrimSpace(rendered[field.name])
		if field.required && value == "" {
			return false
		}
		if value != "" {
			seenFamilies[field.family] = true
		}
		if field.secret && strings.Contains(strings.ToLower(value), "token=") {
			return false
		}
	}
	for _, family := range []string{"root", "teams", "graph", "proxy", "beacon"} {
		if !seenFamilies[family] {
			return false
		}
	}
	return true
}

func TestRemainingPlanServiceConfigDriftCoversNonBeaconEnvFamilies(t *testing.T) {
	fields := []remainingServiceEnvField{
		{name: "CODEX_HOME", family: "root", required: true},
		{name: "CODEX_DIR", family: "root", required: true},
		{name: "CODEX_HELPER_TEAMS_STORE", family: "teams", required: true},
		{name: "CODEX_HELPER_GRAPH_AUTH_CACHE", family: "graph", required: true, secret: true},
		{name: "CODEX_PROXY_HTTP_PROXY", family: "proxy"},
		{name: "CODEX_HELPER_BEACON_STORE", family: "beacon", required: true},
		{name: "CODEX_HELPER_BEACON_SLURM_QUERY", family: "beacon"},
	}
	good := map[string]string{
		"CODEX_HOME":                      "/home/user/.codex",
		"CODEX_DIR":                       "/home/user/.codex",
		"CODEX_HELPER_TEAMS_STORE":        "/home/user/.cache/cxp/teams.json",
		"CODEX_HELPER_GRAPH_AUTH_CACHE":   "/home/user/.cache/cxp/graph-auth.json",
		"CODEX_PROXY_HTTP_PROXY":          "http://127.0.0.1:18080",
		"CODEX_HELPER_BEACON_STORE":       "/shared/cxp/beacon.json",
		"CODEX_HELPER_BEACON_SLURM_QUERY": "/opt/cxp/slurm-query",
	}
	if !remainingServiceConfigValid(fields, good) {
		t.Fatal("complete service config should pass simulated drift check")
	}
	for name, rendered := range map[string]map[string]string{
		"missing graph family": copyRemainingStringMap(good, func(m map[string]string) {
			delete(m, "CODEX_HELPER_GRAPH_AUTH_CACHE")
		}),
		"missing proxy family": copyRemainingStringMap(good, func(m map[string]string) {
			delete(m, "CODEX_PROXY_HTTP_PROXY")
		}),
		"secret value embedded": copyRemainingStringMap(good, func(m map[string]string) {
			m["CODEX_HELPER_GRAPH_AUTH_CACHE"] = "token=secret"
		}),
		"missing beacon store": copyRemainingStringMap(good, func(m map[string]string) {
			delete(m, "CODEX_HELPER_BEACON_STORE")
		}),
	} {
		if remainingServiceConfigValid(fields, rendered) {
			t.Fatalf("%s should fail service config drift simulation: %#v", name, rendered)
		}
	}
}

func copyRemainingStringMap(in map[string]string, fn func(map[string]string)) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	fn(out)
	return out
}

type remainingRetryDecision string

const (
	remainingRetryReplay     remainingRetryDecision = "replay"
	remainingRetryAskUser    remainingRetryDecision = "ask_user"
	remainingRetryReject     remainingRetryDecision = "reject"
	remainingRetryInterrupt  remainingRetryDecision = "interrupt"
	remainingRetryNeedsAuth  remainingRetryDecision = "needs_auth"
	remainingRetryNeedsFiles remainingRetryDecision = "needs_files"
)

type remainingRetryInput struct {
	authValid                 bool
	readScopeValid            bool
	hostedAttachmentAvailable bool
	referenceAttachmentOK     bool
	helperGeneratedOriginal   bool
	possibleStart             bool
	userConfirmedFork         bool
}

func remainingRetryAfterFailure(in remainingRetryInput) remainingRetryDecision {
	if in.helperGeneratedOriginal {
		return remainingRetryReject
	}
	if !in.authValid || !in.readScopeValid {
		return remainingRetryNeedsAuth
	}
	if !in.hostedAttachmentAvailable || !in.referenceAttachmentOK {
		return remainingRetryNeedsFiles
	}
	if in.possibleStart && !in.userConfirmedFork {
		return remainingRetryAskUser
	}
	if in.possibleStart && in.userConfirmedFork {
		return remainingRetryInterrupt
	}
	return remainingRetryReplay
}

func TestRemainingPlanRetrySimulationBlocksAuthAttachmentAndAmbiguousReplay(t *testing.T) {
	base := remainingRetryInput{
		authValid:                 true,
		readScopeValid:            true,
		hostedAttachmentAvailable: true,
		referenceAttachmentOK:     true,
	}
	if got := remainingRetryAfterFailure(base); got != remainingRetryReplay {
		t.Fatalf("safe retry should replay, got %s", got)
	}
	cases := map[string]struct {
		in   remainingRetryInput
		want remainingRetryDecision
	}{
		"auth lost": {
			in:   copyRemainingRetry(base, func(in *remainingRetryInput) { in.authValid = false }),
			want: remainingRetryNeedsAuth,
		},
		"read scope lost": {
			in:   copyRemainingRetry(base, func(in *remainingRetryInput) { in.readScopeValid = false }),
			want: remainingRetryNeedsAuth,
		},
		"hosted attachment missing": {
			in:   copyRemainingRetry(base, func(in *remainingRetryInput) { in.hostedAttachmentAvailable = false }),
			want: remainingRetryNeedsFiles,
		},
		"reference attachment missing": {
			in:   copyRemainingRetry(base, func(in *remainingRetryInput) { in.referenceAttachmentOK = false }),
			want: remainingRetryNeedsFiles,
		},
		"helper generated original": {
			in:   copyRemainingRetry(base, func(in *remainingRetryInput) { in.helperGeneratedOriginal = true }),
			want: remainingRetryReject,
		},
		"possible start requires user fork": {
			in:   copyRemainingRetry(base, func(in *remainingRetryInput) { in.possibleStart = true }),
			want: remainingRetryAskUser,
		},
		"confirmed fork is explicit interruption not silent replay": {
			in: copyRemainingRetry(base, func(in *remainingRetryInput) {
				in.possibleStart = true
				in.userConfirmedFork = true
			}),
			want: remainingRetryInterrupt,
		},
	}
	for name, tc := range cases {
		if got := remainingRetryAfterFailure(tc.in); got != tc.want {
			t.Fatalf("%s: got %s want %s", name, got, tc.want)
		}
	}
}

func copyRemainingRetry(in remainingRetryInput, fn func(*remainingRetryInput)) remainingRetryInput {
	fn(&in)
	return in
}

type remainingNotificationRoute string

const (
	remainingNotifyWebhook        remainingNotificationRoute = "webhook"
	remainingNotifyControlMention remainingNotificationRoute = "control_mention"
	remainingNotifyProtectedWait  remainingNotificationRoute = "protected_wait"
	remainingNotifyDropTransient  remainingNotificationRoute = "drop_transient"
)

type remainingNotificationInput struct {
	workflowConfigured bool
	workflowSendOK     bool
	protectedOutbox    bool
	transient          bool
	ownerMentionOff    bool
}

func remainingNotificationDecision(in remainingNotificationInput) remainingNotificationRoute {
	if in.protectedOutbox && !in.workflowSendOK {
		return remainingNotifyProtectedWait
	}
	if in.workflowConfigured && in.workflowSendOK {
		return remainingNotifyWebhook
	}
	if in.transient {
		return remainingNotifyDropTransient
	}
	if !in.ownerMentionOff {
		return remainingNotifyControlMention
	}
	return remainingNotifyProtectedWait
}

func TestRemainingPlanWorkflowFallbackDoesNotDropProtectedBeaconOutput(t *testing.T) {
	if got := remainingNotificationDecision(remainingNotificationInput{workflowConfigured: true, workflowSendOK: true, protectedOutbox: true, ownerMentionOff: true}); got != remainingNotifyWebhook {
		t.Fatalf("healthy workflow should send webhook, got %s", got)
	}
	if got := remainingNotificationDecision(remainingNotificationInput{workflowConfigured: true, workflowSendOK: false, protectedOutbox: true, ownerMentionOff: true}); got != remainingNotifyProtectedWait {
		t.Fatalf("protected beacon output should wait instead of disappearing, got %s", got)
	}
	if got := remainingNotificationDecision(remainingNotificationInput{workflowConfigured: true, workflowSendOK: false, transient: true}); got != remainingNotifyDropTransient {
		t.Fatalf("transient status may be dropped/superseded, got %s", got)
	}
	if got := remainingNotificationDecision(remainingNotificationInput{workflowConfigured: false, workflowSendOK: false}); got != remainingNotifyControlMention {
		t.Fatalf("non-protected final without webhook should fall back to control mention, got %s", got)
	}
}

type remainingInstallTarget struct {
	os              string
	arch            string
	path            string
	profileRevision string
	executionHash   string
}

type remainingUpgradeCheck struct {
	target        remainingInstallTarget
	active        []remainingInstallTarget
	queued        []remainingInstallTarget
	lockHeld      bool
	versionOK     bool
	selfCheckOK   bool
	persistentOK  bool
	workerStarted bool
}

func remainingSameInstallTarget(a, b remainingInstallTarget) bool {
	return a.os == b.os &&
		a.arch == b.arch &&
		a.path == b.path &&
		a.profileRevision == b.profileRevision &&
		a.executionHash == b.executionHash
}

func remainingCanPromoteTargetedUpgrade(in remainingUpgradeCheck) bool {
	if !in.lockHeld || !in.versionOK || !in.selfCheckOK || !in.persistentOK || in.workerStarted {
		return false
	}
	for _, active := range in.active {
		if remainingSameInstallTarget(in.target, active) {
			return false
		}
	}
	for _, queued := range in.queued {
		if remainingSameInstallTarget(in.target, queued) {
			return false
		}
	}
	return true
}

func TestRemainingPlanPerBeaconTargetUpgradeMatchingIsExact(t *testing.T) {
	target := remainingInstallTarget{os: "linux", arch: "amd64", path: "/shared/codex/bin/codex", profileRevision: "gpu@2", executionHash: "sig-a"}
	base := remainingUpgradeCheck{target: target, lockHeld: true, versionOK: true, selfCheckOK: true, persistentOK: true}
	if !remainingCanPromoteTargetedUpgrade(base) {
		t.Fatal("idle exact target with lock and checks should promote")
	}
	for name, check := range map[string]remainingUpgradeCheck{
		"same active target": copyRemainingUpgrade(base, func(in *remainingUpgradeCheck) {
			in.active = []remainingInstallTarget{target}
		}),
		"same queued target": copyRemainingUpgrade(base, func(in *remainingUpgradeCheck) {
			in.queued = []remainingInstallTarget{target}
		}),
		"worker already started": copyRemainingUpgrade(base, func(in *remainingUpgradeCheck) {
			in.workerStarted = true
		}),
		"no lock": copyRemainingUpgrade(base, func(in *remainingUpgradeCheck) {
			in.lockHeld = false
		}),
		"self check failed": copyRemainingUpgrade(base, func(in *remainingUpgradeCheck) {
			in.selfCheckOK = false
		}),
	} {
		if remainingCanPromoteTargetedUpgrade(check) {
			t.Fatalf("%s should block targeted upgrade: %#v", name, check)
		}
	}
	otherRevision := copyRemainingInstallTarget(target, func(t *remainingInstallTarget) { t.profileRevision = "gpu@3" })
	otherPath := copyRemainingInstallTarget(target, func(t *remainingInstallTarget) { t.path = "/other/codex" })
	if !remainingCanPromoteTargetedUpgrade(copyRemainingUpgrade(base, func(in *remainingUpgradeCheck) {
		in.active = []remainingInstallTarget{otherRevision, otherPath}
	})) {
		t.Fatal("different profile revision/path should not block exact targeted upgrade")
	}
}

func copyRemainingUpgrade(in remainingUpgradeCheck, fn func(*remainingUpgradeCheck)) remainingUpgradeCheck {
	in.active = append([]remainingInstallTarget(nil), in.active...)
	in.queued = append([]remainingInstallTarget(nil), in.queued...)
	fn(&in)
	return in
}

func copyRemainingInstallTarget(in remainingInstallTarget, fn func(*remainingInstallTarget)) remainingInstallTarget {
	fn(&in)
	return in
}
