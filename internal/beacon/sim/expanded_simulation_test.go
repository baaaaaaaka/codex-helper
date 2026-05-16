package sim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"
)

type legacyTeamsState struct {
	SchemaVersion int               `json:"schema_version"`
	Turns         map[string]string `json:"turns,omitempty"`
}

func TestBeaconStateMustStayOutOfLegacyTeamsState(t *testing.T) {
	raw := []byte(`{"schema_version":3,"turns":{"t1":"running"},"beacon":{"leases":{"l1":{"state":"accepting"}}}}`)
	var state legacyTeamsState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("unmarshal legacy state: %v", err)
	}
	rewritten, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal legacy state: %v", err)
	}
	if bytes.Contains(rewritten, []byte(`"beacon"`)) {
		t.Fatalf("legacy rewrite unexpectedly preserved beacon state: %s", rewritten)
	}
}

type eventRecord struct {
	Seq     int
	Prev    string
	Hash    string
	Payload string
}

func newEvent(seq int, prev string, payload string) eventRecord {
	hash := fmt.Sprintf("h:%d:%s:%s", seq, prev, payload)
	return eventRecord{Seq: seq, Prev: prev, Hash: hash, Payload: payload}
}

func validateEventChain(events []eventRecord, terminalSeq int, terminalHash string) bool {
	prev := ""
	for i, ev := range events {
		wantSeq := i + 1
		if ev.Seq != wantSeq || ev.Prev != prev {
			return false
		}
		wantHash := fmt.Sprintf("h:%d:%s:%s", ev.Seq, ev.Prev, ev.Payload)
		if ev.Hash != wantHash {
			return false
		}
		prev = ev.Hash
	}
	if len(events) == 0 {
		return terminalSeq == 0 && terminalHash == ""
	}
	return terminalSeq == len(events) && terminalHash == prev
}

func TestEventChainRequiresContiguousSeqPrevHashAndTerminalReference(t *testing.T) {
	ev1 := newEvent(1, "", "start")
	ev2 := newEvent(2, ev1.Hash, "finish")
	if !validateEventChain([]eventRecord{ev1, ev2}, 2, ev2.Hash) {
		t.Fatal("valid event chain should pass")
	}

	cases := map[string][]eventRecord{
		"gap":      {ev1, newEvent(3, ev1.Hash, "finish")},
		"bad prev": {ev1, newEvent(2, "wrong", "finish")},
		"bad hash": {ev1, {Seq: 2, Prev: ev1.Hash, Hash: "tampered", Payload: "finish"}},
	}
	for name, events := range cases {
		last := events[len(events)-1]
		if validateEventChain(events, last.Seq, last.Hash) {
			t.Fatalf("%s event chain should be rejected", name)
		}
	}
	if validateEventChain([]eventRecord{ev1, ev2}, 1, ev1.Hash) {
		t.Fatal("terminal referencing non-final event should be rejected")
	}
}

type schedulerStatus string

const (
	schedulerActive  schedulerStatus = "active"
	schedulerDead    schedulerStatus = "dead"
	schedulerUnknown schedulerStatus = "unknown"
)

type reconcileAction string

const (
	reconcileAccept     reconcileAction = "accept"
	reconcileDrain      reconcileAction = "drain"
	reconcileLost       reconcileAction = "lost"
	reconcileQuarantine reconcileAction = "quarantine"
	reconcilePending    reconcileAction = "pending"
	reconcileRunning    reconcileAction = "running"
	reconcileSuspended  reconcileAction = "suspended"
	reconcileCompleted  reconcileAction = "completed"
	reconcileFinalizing reconcileAction = "finalizing"
)

type reconcileInput struct {
	heartbeatFresh bool
	scheduler      schedulerStatus
	jobState       jobState
	remainingTTL   int
	requiredTTL    int
}

func reconcileLease(in reconcileInput) reconcileAction {
	if in.scheduler == schedulerDead {
		if in.jobState == jobStarted || in.jobState == jobStartIntent {
			return reconcileQuarantine
		}
		return reconcileLost
	}
	if in.scheduler == schedulerUnknown || !in.heartbeatFresh {
		return reconcileDrain
	}
	if in.remainingTTL < in.requiredTTL {
		return reconcileDrain
	}
	return reconcileAccept
}

func TestSchedulerReconcileTreatsSchedulerAsAuthoritative(t *testing.T) {
	tests := []struct {
		name string
		in   reconcileInput
		want reconcileAction
	}{
		{name: "fresh heartbeat and active scheduler accepts", in: reconcileInput{heartbeatFresh: true, scheduler: schedulerActive, remainingTTL: 60, requiredTTL: 30}, want: reconcileAccept},
		{name: "fresh heartbeat cannot override dead scheduler", in: reconcileInput{heartbeatFresh: true, scheduler: schedulerDead, jobState: jobQueued, remainingTTL: 60, requiredTTL: 30}, want: reconcileLost},
		{name: "dead scheduler with started job quarantines", in: reconcileInput{heartbeatFresh: true, scheduler: schedulerDead, jobState: jobStarted, remainingTTL: 60, requiredTTL: 30}, want: reconcileQuarantine},
		{name: "stale heartbeat drains even while scheduler active", in: reconcileInput{scheduler: schedulerActive, remainingTTL: 60, requiredTTL: 30}, want: reconcileDrain},
		{name: "unknown scheduler drains conservatively", in: reconcileInput{heartbeatFresh: true, scheduler: schedulerUnknown, remainingTTL: 60, requiredTTL: 30}, want: reconcileDrain},
		{name: "insufficient TTL drains", in: reconcileInput{heartbeatFresh: true, scheduler: schedulerActive, remainingTTL: 29, requiredTTL: 30}, want: reconcileDrain},
	}
	for _, tt := range tests {
		if got := reconcileLease(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

type workerKind string
type workerAction string

const (
	workerProduction   workerKind   = "production"
	workerMaintenance  workerKind   = "maintenance"
	actionBusinessTurn workerAction = "business_turn"
	actionCodexUpgrade workerAction = "codex_upgrade"
	actionSelfCheck    workerAction = "self_check"
)

func workerCanRun(kind workerKind, action workerAction, used bool, acl bool) bool {
	if used {
		return false
	}
	switch action {
	case actionBusinessTurn:
		return kind == workerProduction
	case actionCodexUpgrade, actionSelfCheck:
		return kind == workerMaintenance && acl
	default:
		return false
	}
}

func TestMaintenanceWorkerIsOneShotAndCannotRunBusinessTurns(t *testing.T) {
	if !workerCanRun(workerProduction, actionBusinessTurn, false, false) {
		t.Fatal("production worker should run business turn")
	}
	if workerCanRun(workerMaintenance, actionBusinessTurn, false, true) {
		t.Fatal("maintenance worker must not run business turn")
	}
	if workerCanRun(workerProduction, actionCodexUpgrade, false, true) {
		t.Fatal("production worker must not run Codex upgrade")
	}
	if !workerCanRun(workerMaintenance, actionCodexUpgrade, false, true) {
		t.Fatal("maintenance worker with ACL should run Codex upgrade")
	}
	if workerCanRun(workerMaintenance, actionCodexUpgrade, true, true) {
		t.Fatal("maintenance worker is one-shot and must not run twice")
	}
	if workerCanRun(workerMaintenance, actionCodexUpgrade, false, false) {
		t.Fatal("maintenance action requires explicit ACL")
	}
}

type providerCommand struct {
	argv              []string
	fromProfile       bool
	userPromptInArgv  bool
	containsShellMeta bool
}

func providerCommandAllowed(cmd providerCommand) bool {
	return cmd.fromProfile &&
		len(cmd.argv) > 0 &&
		!cmd.userPromptInArgv &&
		!cmd.containsShellMeta &&
		!strings.Contains(strings.Join(cmd.argv, "\x00"), "..")
}

func TestProviderCommandMustComeFromAllowlistedArgv(t *testing.T) {
	allowed := providerCommand{fromProfile: true, argv: []string{"submit_job", "--gpu", "1", "--image", "image.sqsh"}}
	if !providerCommandAllowed(allowed) {
		t.Fatal("profile-provided argv command should be allowed")
	}
	cases := map[string]providerCommand{
		"empty":              {fromProfile: true},
		"user prompt":        {fromProfile: true, argv: []string{"submit_job", "from prompt"}, userPromptInArgv: true},
		"not profile":        {argv: []string{"submit_job"}},
		"shell meta":         {fromProfile: true, argv: []string{"sh", "-c", "submit_job && rm -rf x"}, containsShellMeta: true},
		"path traversal arg": {fromProfile: true, argv: []string{"submit_job", "../evil"}},
	}
	for name, cmd := range cases {
		if providerCommandAllowed(cmd) {
			t.Fatalf("%s command should be rejected: %#v", name, cmd)
		}
	}
}

type executionTarget string

const (
	targetLocal  executionTarget = "local"
	targetBeacon executionTarget = "beacon"
)

func resolveNewExecutionTarget(explicitBeaconProfile string, legacyProxyPreferencePresent bool) executionTarget {
	if strings.TrimSpace(explicitBeaconProfile) != "" {
		return targetBeacon
	}
	return targetLocal
}

func TestNewDefaultsLocalEvenWhenLegacyProxyPreferenceExists(t *testing.T) {
	if got := resolveNewExecutionTarget("", true); got != targetLocal {
		t.Fatalf("legacy proxy preference should not make new default beacon/proxy target: %s", got)
	}
	if got := resolveNewExecutionTarget("gpu-a", true); got != targetBeacon {
		t.Fatalf("explicit beacon profile should select beacon target: %s", got)
	}
}

type cliExecutionInput struct {
	legacyProxyProfile string
	beaconProfile      string
}

type cliExecutionResolution struct {
	target     executionTarget
	proxyName  string
	beaconName string
}

func resolveCLIExecution(in cliExecutionInput) cliExecutionResolution {
	if strings.TrimSpace(in.beaconProfile) != "" {
		return cliExecutionResolution{target: targetBeacon, beaconName: in.beaconProfile}
	}
	return cliExecutionResolution{target: targetLocal, proxyName: in.legacyProxyProfile}
}

func TestLegacyProxyProfileDoesNotSelectBeaconExecution(t *testing.T) {
	got := resolveCLIExecution(cliExecutionInput{legacyProxyProfile: "jump-a"})
	if got.target != targetLocal || got.proxyName != "jump-a" || got.beaconName != "" {
		t.Fatalf("legacy proxy profile should remain local/proxy execution, got %#v", got)
	}
	got = resolveCLIExecution(cliExecutionInput{legacyProxyProfile: "jump-a", beaconProfile: "gpu-a"})
	if got.target != targetBeacon || got.beaconName != "gpu-a" {
		t.Fatalf("explicit beacon profile should select beacon execution, got %#v", got)
	}
}

type profileCreateChannel string
type proxySelection string

const (
	channelCLI   profileCreateChannel = "cli"
	channelTeams profileCreateChannel = "teams"
	channelTUI   profileCreateChannel = "tui"

	proxyUnspecified proxySelection = ""
	proxyNone        proxySelection = "none"
	proxyExisting    proxySelection = "existing"
)

type profileCreateInput struct {
	channel             profileCreateChannel
	provider            string
	proxy               proxySelection
	proxyName           string
	confirmed           bool
	rawProviderCommand  bool
	secretFields        bool
	advancedFields      bool
	localPolicyApproved bool
}

func canCreateBeaconProfile(in profileCreateInput) bool {
	if in.provider != "slurm" && in.provider != "lsf" {
		return false
	}
	if !in.confirmed {
		return false
	}
	switch in.proxy {
	case proxyNone:
	case proxyExisting:
		if strings.TrimSpace(in.proxyName) == "" {
			return false
		}
	default:
		return false
	}
	if in.rawProviderCommand || in.secretFields {
		return false
	}
	if in.advancedFields && in.channel != channelCLI && !in.localPolicyApproved {
		return false
	}
	switch in.channel {
	case channelCLI, channelTeams, channelTUI:
		return true
	default:
		return false
	}
}

func TestProfileCreationRequiresExplicitProxySelection(t *testing.T) {
	base := profileCreateInput{channel: channelCLI, provider: "slurm", confirmed: true}
	if canCreateBeaconProfile(base) {
		t.Fatal("profile creation without explicit proxy selection should be rejected")
	}
	if !canCreateBeaconProfile(copyProfileCreate(base, func(in *profileCreateInput) { in.proxy = proxyNone })) {
		t.Fatal("explicit no-proxy profile should be accepted")
	}
	if !canCreateBeaconProfile(copyProfileCreate(base, func(in *profileCreateInput) {
		in.proxy = proxyExisting
		in.proxyName = "jump-a"
	})) {
		t.Fatal("explicit existing proxy profile should be accepted")
	}
	if canCreateBeaconProfile(copyProfileCreate(base, func(in *profileCreateInput) { in.proxy = proxyExisting })) {
		t.Fatal("existing proxy selection without proxy name should be rejected")
	}
}

func TestTeamsAndTUICanCreateStructuredBeaconProfiles(t *testing.T) {
	for _, channel := range []profileCreateChannel{channelTeams, channelTUI} {
		if !canCreateBeaconProfile(profileCreateInput{
			channel:   channel,
			provider:  "lsf",
			proxy:     proxyNone,
			confirmed: true,
		}) {
			t.Fatalf("%s basic structured profile creation should be accepted", channel)
		}
		if canCreateBeaconProfile(profileCreateInput{
			channel:            channel,
			provider:           "slurm",
			proxy:              proxyNone,
			confirmed:          true,
			rawProviderCommand: true,
		}) {
			t.Fatalf("%s must reject raw provider commands", channel)
		}
		if canCreateBeaconProfile(profileCreateInput{
			channel:      channel,
			provider:     "slurm",
			proxy:        proxyNone,
			confirmed:    true,
			secretFields: true,
		}) {
			t.Fatalf("%s must reject secret-bearing profile fields", channel)
		}
		if canCreateBeaconProfile(profileCreateInput{
			channel:        channel,
			provider:       "slurm",
			proxy:          proxyNone,
			confirmed:      true,
			advancedFields: true,
		}) {
			t.Fatalf("%s advanced profile creation requires local policy approval", channel)
		}
		if !canCreateBeaconProfile(profileCreateInput{
			channel:             channel,
			provider:            "slurm",
			proxy:               proxyExisting,
			proxyName:           "jump-a",
			confirmed:           true,
			advancedFields:      true,
			localPolicyApproved: true,
		}) {
			t.Fatalf("%s advanced profile creation should pass with local policy approval", channel)
		}
	}
}

type profileLifecycleState string

const (
	profileDraft profileLifecycleState = "draft"
	profileReady profileLifecycleState = "ready"
)

type profileActivationInput struct {
	created           bool
	confirmed         bool
	doctorPassed      bool
	proxyResolved     bool
	providerPreviewed bool
}

func profileStateAfterCreate(in profileActivationInput) profileLifecycleState {
	if in.created && in.confirmed && in.doctorPassed && in.proxyResolved && in.providerPreviewed {
		return profileReady
	}
	return profileDraft
}

func TestCreatedProfilesRemainDraftUntilDoctorProxyAndProviderPreviewPass(t *testing.T) {
	base := profileActivationInput{
		created:           true,
		confirmed:         true,
		doctorPassed:      true,
		proxyResolved:     true,
		providerPreviewed: true,
	}
	if got := profileStateAfterCreate(base); got != profileReady {
		t.Fatalf("fully validated profile should become ready, got %s", got)
	}
	cases := map[string]profileActivationInput{
		"not confirmed":      copyProfileActivation(base, func(in *profileActivationInput) { in.confirmed = false }),
		"doctor failed":      copyProfileActivation(base, func(in *profileActivationInput) { in.doctorPassed = false }),
		"proxy unresolved":   copyProfileActivation(base, func(in *profileActivationInput) { in.proxyResolved = false }),
		"provider not shown": copyProfileActivation(base, func(in *profileActivationInput) { in.providerPreviewed = false }),
		"not created":        copyProfileActivation(base, func(in *profileActivationInput) { in.created = false }),
	}
	for name, in := range cases {
		if got := profileStateAfterCreate(in); got != profileDraft {
			t.Fatalf("%s should remain draft, got %s", name, got)
		}
	}
}

type conversationTurnState string
type profileSwitchMode string
type profileSwitchAction string

const (
	turnIdle    conversationTurnState = "idle"
	turnQueued  conversationTurnState = "queued"
	turnRunning conversationTurnState = "running"

	switchResume  profileSwitchMode = "resume"
	switchFork    profileSwitchMode = "fork"
	switchPending profileSwitchMode = "pending"

	switchApplyNow     profileSwitchAction = "apply_now"
	switchApplyPending profileSwitchAction = "apply_pending"
	switchRequireFork  profileSwitchAction = "require_fork"
	switchReject       profileSwitchAction = "reject"
)

type profileSwitchInput struct {
	turnState           conversationTurnState
	targetReady         bool
	signatureCompatible bool
	mode                profileSwitchMode
}

func decideProfileSwitch(in profileSwitchInput) profileSwitchAction {
	if !in.targetReady {
		return switchReject
	}
	if !in.signatureCompatible {
		if in.mode == switchFork {
			return switchApplyNow
		}
		return switchRequireFork
	}
	if in.turnState == turnQueued || in.turnState == turnRunning {
		return switchApplyPending
	}
	return switchApplyNow
}

func TestExistingConversationProfileSwitchRules(t *testing.T) {
	base := profileSwitchInput{turnState: turnIdle, targetReady: true, signatureCompatible: true, mode: switchResume}
	if got := decideProfileSwitch(base); got != switchApplyNow {
		t.Fatalf("compatible idle conversation should switch immediately, got %s", got)
	}
	tests := []struct {
		name string
		in   profileSwitchInput
		want profileSwitchAction
	}{
		{name: "draft target rejected", in: copyProfileSwitch(base, func(in *profileSwitchInput) { in.targetReady = false }), want: switchReject},
		{name: "queued turn schedules pending by default", in: copyProfileSwitch(base, func(in *profileSwitchInput) { in.turnState = turnQueued }), want: switchApplyPending},
		{name: "running turn schedules pending by default", in: copyProfileSwitch(base, func(in *profileSwitchInput) { in.turnState = turnRunning }), want: switchApplyPending},
		{name: "queued turn can schedule pending switch", in: copyProfileSwitch(base, func(in *profileSwitchInput) { in.turnState = turnQueued; in.mode = switchPending }), want: switchApplyPending},
		{name: "running turn can schedule pending switch", in: copyProfileSwitch(base, func(in *profileSwitchInput) { in.turnState = turnRunning; in.mode = switchPending }), want: switchApplyPending},
		{name: "incompatible resume requires fork", in: copyProfileSwitch(base, func(in *profileSwitchInput) { in.signatureCompatible = false }), want: switchRequireFork},
		{name: "incompatible explicit fork can switch", in: copyProfileSwitch(base, func(in *profileSwitchInput) { in.signatureCompatible = false; in.mode = switchFork }), want: switchApplyNow},
	}
	for _, tt := range tests {
		if got := decideProfileSwitch(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

func copyProfileSwitch(in profileSwitchInput, fn func(*profileSwitchInput)) profileSwitchInput {
	fn(&in)
	return in
}

func copyProfileActivation(in profileActivationInput, fn func(*profileActivationInput)) profileActivationInput {
	fn(&in)
	return in
}

func copyProfileCreate(in profileCreateInput, fn func(*profileCreateInput)) profileCreateInput {
	fn(&in)
	return in
}

func artifactPathAllowed(sharedRoot, candidate string) bool {
	root := path.Clean(sharedRoot)
	candidatePath := path.Clean(candidate)
	if !path.IsAbs(root) || !path.IsAbs(candidatePath) {
		return false
	}
	if candidatePath == root {
		return false
	}
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}
	return strings.HasPrefix(candidatePath, root)
}

func TestArtifactPathsMustBeCoordinatorReadableSharedPaths(t *testing.T) {
	root := "/shared/beacon/jobs/job-1"
	if !artifactPathAllowed(root, "/shared/beacon/jobs/job-1/out/report.txt") {
		t.Fatal("shared artifact path should be allowed")
	}
	cases := map[string]string{
		"relative":       "out/report.txt",
		"container only": "/workspace/out/report.txt",
		"parent":         "/shared/beacon/jobs/job-1/../secret.txt",
		"root itself":    "/shared/beacon/jobs/job-1",
	}
	for name, path := range cases {
		if artifactPathAllowed(root, path) {
			t.Fatalf("%s path should be rejected: %s", name, path)
		}
	}
}

type upgradePromoteInput struct {
	lockHeld         bool
	staged           bool
	selfCheckOK      bool
	versionVerified  bool
	persistent       bool
	activeSameTarget bool
}

func canPromoteUpgrade(in upgradePromoteInput) bool {
	return in.lockHeld &&
		in.staged &&
		in.selfCheckOK &&
		in.versionVerified &&
		in.persistent &&
		!in.activeSameTarget
}

func TestUpgradePromoteRequiresStageSelfCheckVersionAndPersistence(t *testing.T) {
	base := upgradePromoteInput{lockHeld: true, staged: true, selfCheckOK: true, versionVerified: true, persistent: true}
	if !canPromoteUpgrade(base) {
		t.Fatal("fully validated staged upgrade should promote")
	}
	cases := map[string]upgradePromoteInput{
		"no lock":            {staged: true, selfCheckOK: true, versionVerified: true, persistent: true},
		"not staged":         {lockHeld: true, selfCheckOK: true, versionVerified: true, persistent: true},
		"self check failed":  {lockHeld: true, staged: true, versionVerified: true, persistent: true},
		"version mismatch":   {lockHeld: true, staged: true, selfCheckOK: true, persistent: true},
		"not persistent":     {lockHeld: true, staged: true, selfCheckOK: true, versionVerified: true},
		"active same target": {lockHeld: true, staged: true, selfCheckOK: true, versionVerified: true, persistent: true, activeSameTarget: true},
	}
	for name, in := range cases {
		if canPromoteUpgrade(in) {
			t.Fatalf("%s should not promote: %#v", name, in)
		}
	}
}

type auditInput struct {
	Actor      string
	Action     string
	TargetPath string
	Token      string
	Env        map[string]string
	Prompt     string
}

func auditRecord(in auditInput) map[string]string {
	return map[string]string{
		"actor":       in.Actor,
		"action":      in.Action,
		"target_path": in.TargetPath,
	}
}

func TestAuditRecordsKeepMetadataButRedactSecretsEnvAndPrompt(t *testing.T) {
	record := auditRecord(auditInput{
		Actor:      "user-1",
		Action:     "codex_upgrade",
		TargetPath: "/shared/codex/bin/codex",
		Token:      "secret-token",
		Env:        map[string]string{"OPENAI_API_KEY": "secret-key"},
		Prompt:     "please print my token",
	})
	joined := fmt.Sprint(record)
	for _, secret := range []string{"secret-token", "secret-key", "please print"} {
		if strings.Contains(joined, secret) {
			t.Fatalf("audit record leaked secret/prompt content %q: %s", secret, joined)
		}
	}
	for _, key := range []string{"actor", "action", "target_path"} {
		if record[key] == "" {
			t.Fatalf("audit record missing metadata key %s: %#v", key, record)
		}
	}
}
