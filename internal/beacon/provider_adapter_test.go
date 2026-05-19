package beacon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCommandProviderAdapterSubmitsSlurmWithProfileSnapshotAndNoShell(t *testing.T) {
	req := slurmAllocationRequestForAdapter(t)
	runner := &recordingProviderRunner{
		output: `{"provider_job_id":"12345","raw_state":"PD","reason":"queued"}`,
	}
	adapter := CommandProviderAdapter{
		Config: ProviderCommandConfig{SlurmQueryCommand: "/opt/cxp/query-slurm", SlurmSubmitCommand: "/opt/cxp/submit-slurm"},
		Runner: runner,
	}

	query, err := adapter.QueryAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("query allocation: %v", err)
	}
	submitted, err := adapter.SubmitAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("submit allocation: %v", err)
	}
	if query.ProviderJobID != "12345" || submitted.ProviderJobID != "12345" {
		t.Fatalf("provider result not parsed, query=%#v submit=%#v", query, submitted)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %#v", runner.calls)
	}
	if runner.calls[0].name != "/opt/cxp/query-slurm" || runner.calls[1].name != "/opt/cxp/submit-slurm" {
		t.Fatalf("provider commands = %#v", runner.calls)
	}
	for _, call := range runner.calls {
		if call.name == "sh" || call.name == "bash" || containsProviderArg(call.args, "-c") {
			t.Fatalf("provider adapter must not invoke a shell, call=%#v", call)
		}
		for _, want := range [][]string{
			{"--request-id", req.ID},
			{"--name", req.DeterministicName},
			{"--partition", "interactive"},
			{"--image", "image.sqsh"},
			{"--nodes", "2"},
			{"--gpu", "8"},
			{"--duration", "6"},
			{"--execution-hash", "sig-a"},
		} {
			if !containsProviderArgPair(call.args, want[0], want[1]) {
				t.Fatalf("provider args missing %v in call %#v", want, call)
			}
		}
	}
	if !containsProviderArgPair(runner.calls[0].args, "--operation", "query") {
		t.Fatalf("query call missing operation: %#v", runner.calls[0])
	}
	if !containsProviderArgPair(runner.calls[1].args, "--operation", "submit") {
		t.Fatalf("submit call missing operation: %#v", runner.calls[1])
	}
}

func TestCommandProviderAdapterMissingCommandIsActionable(t *testing.T) {
	req := slurmAllocationRequestForAdapter(t)
	adapter := CommandProviderAdapter{Config: ProviderCommandConfig{}, Runner: &recordingProviderRunner{}}

	_, err := adapter.QueryAllocation(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), BeaconSlurmQueryCommandEnv) || !strings.Contains(err.Error(), "--query-command") || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("missing query command error = %v", err)
	}
	_, err = adapter.SubmitAllocation(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), BeaconSlurmSubmitCommandEnv) || !strings.Contains(err.Error(), "--submit-command") || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("missing submit command error = %v", err)
	}
	_, err = adapter.CancelAllocation(context.Background(), req)
	if err == nil || !IsProviderCommandNotConfigured(err) || !strings.Contains(err.Error(), BeaconSlurmCancelCommandEnv) || !strings.Contains(err.Error(), "--cancel-command") {
		t.Fatalf("missing cancel command error = %v", err)
	}
}

func TestCommandProviderAdapterUsesProfileCommandsBeforeEnvironment(t *testing.T) {
	req := slurmAllocationRequestForAdapter(t)
	req.ProfileSnapshot.Adapter = ProviderCommandConfigForProvider(req.Provider, "/profile/query", "/profile/submit", "", "")
	runner := &recordingProviderRunner{
		outputByCommand: map[string]string{
			"/profile/query":  `{"provider_job_id":"111","raw_state":"PD","reason":"profile query"}`,
			"/profile/submit": `{"provider_job_id":"222","raw_state":"PD","reason":"profile submit"}`,
		},
	}
	adapter := CommandProviderAdapter{
		Config: ProviderCommandConfig{SlurmQueryCommand: "/env/query", SlurmSubmitCommand: "/env/submit"},
		Runner: runner,
	}

	query, err := adapter.QueryAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("query allocation: %v", err)
	}
	submitted, err := adapter.SubmitAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("submit allocation: %v", err)
	}
	if query.ProviderJobID != "111" || submitted.ProviderJobID != "222" {
		t.Fatalf("unexpected provider results: query=%#v submit=%#v", query, submitted)
	}
	got := []string{runner.calls[0].name, runner.calls[1].name}
	want := []string{"/profile/query", "/profile/submit"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("provider commands = %v, want %v", got, want)
	}
}

func TestCommandProviderAdapterFallsBackToEnvironmentCommands(t *testing.T) {
	req := slurmAllocationRequestForAdapter(t)
	runner := &recordingProviderRunner{output: "provider_job_id=333 raw_state=PD"}
	adapter := CommandProviderAdapter{
		Config: ProviderCommandConfig{SlurmQueryCommand: "/env/query"},
		Runner: runner,
	}
	result, err := adapter.QueryAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("query allocation: %v", err)
	}
	if result.ProviderJobID != "333" || len(runner.calls) != 1 || runner.calls[0].name != "/env/query" {
		t.Fatalf("result=%#v calls=%#v", result, runner.calls)
	}
}

func TestParseProviderCommandResultSupportsJSONAndKeyValue(t *testing.T) {
	jsonResult, err := ParseProviderCommandResult(`{"provider_job_id":"job-1","raw_state":"R","reason":"running","durable_negative":true,"multiple_matches":true,"provider_deadline":"2026-05-18T10:00:00Z"}`)
	if err != nil {
		t.Fatalf("parse json: %v", err)
	}
	keyValueResult, err := ParseProviderCommandResult("job_id=job-2 state=PD reason=resources query_error=true")
	if err != nil {
		t.Fatalf("parse key value: %v", err)
	}
	if jsonResult.ProviderJobID != "job-1" || jsonResult.RawState != "R" || !jsonResult.DurableNegative || !jsonResult.MultipleMatches {
		t.Fatalf("json result = %#v", jsonResult)
	}
	if jsonResult.ProviderDeadline.IsZero() {
		t.Fatalf("json deadline was not parsed: %#v", jsonResult)
	}
	if keyValueResult.ProviderJobID != "job-2" || keyValueResult.RawState != "PD" || keyValueResult.Reason != "resources" || !keyValueResult.QueryError {
		t.Fatalf("key value result = %#v", keyValueResult)
	}
	if _, err := ParseProviderCommandResult("this is not structured"); err == nil {
		t.Fatal("unstructured provider output should fail")
	}
}

func TestCommandProviderAdapterCanRunThroughUserShell(t *testing.T) {
	t.Setenv("SHELL", "/bin/zsh")
	t.Setenv("CODEX_HELPER_CLI_PATH", "/opt/cxp")
	req := slurmAllocationRequestForAdapter(t)
	req.ProfileSnapshot.Adapter = ProviderCommandConfig{
		SlurmSubmitCommand: "/opt/cxp/submit-slurm",
		ShellMode:          ProviderCommandShellUser,
	}
	shellEnv := "startup banner\n" + providerShellEnvBegin + "\x00PATH=/opt/site/bin:/usr/bin\x00SUBMIT_ACCOUNT=acct\x00CODEX_HELPER_CLI_PATH=/tmp/.nfs802014de01c482a800000492\x00CODEX_PROXY_INSTALL_DIR=/tmp/codex-proxy\x00" + providerShellEnvEnd + "\x00trailing output\n"
	runner := &recordingProviderRunner{outputByCommand: map[string]string{
		"/bin/zsh":              shellEnv,
		"/opt/cxp/submit-slurm": `{"provider_job_id":"shell-1","raw_state":"PD"}`,
	}}
	adapter := CommandProviderAdapter{Runner: runner}

	submitted, err := adapter.SubmitAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("submit allocation: %v", err)
	}
	if submitted.ProviderJobID != "shell-1" {
		t.Fatalf("submitted = %#v", submitted)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("calls = %#v", runner.calls)
	}
	envCall := runner.calls[0]
	if envCall.name != "/bin/zsh" {
		t.Fatalf("shell command = %q, want /bin/zsh", envCall.name)
	}
	for _, want := range []string{"-lic", providerShellEnvBegin, providerShellEnvEnd} {
		if !containsProviderArgSubstring(envCall.args, want) {
			t.Fatalf("shell env args missing %q: %#v", want, envCall.args)
		}
	}
	adapterCall := runner.calls[1]
	if adapterCall.name != "/opt/cxp/submit-slurm" {
		t.Fatalf("adapter command = %q", adapterCall.name)
	}
	if !containsProviderArgPair(adapterCall.args, "--operation", "submit") {
		t.Fatalf("adapter args were not forwarded: %#v", adapterCall.args)
	}
	for _, want := range []string{"PATH=/opt/site/bin:/usr/bin", "SUBMIT_ACCOUNT=acct", "CODEX_HELPER_CLI_PATH=/opt/cxp"} {
		if !containsProviderArg(adapterCall.env, want) {
			t.Fatalf("adapter env missing %q: %#v", want, adapterCall.env)
		}
	}
	for _, blocked := range []string{"CODEX_HELPER_CLI_PATH=/tmp/.nfs802014de01c482a800000492", "CODEX_PROXY_INSTALL_DIR=/tmp/codex-proxy"} {
		if containsProviderArg(adapterCall.env, blocked) {
			t.Fatalf("adapter env should not include shell volatile helper env %q: %#v", blocked, adapterCall.env)
		}
	}
}

func TestCommandProviderAdapterShellCommandFallbackRunsAdapterThroughShell(t *testing.T) {
	t.Setenv("SHELL", "/bin/zsh")
	req := slurmAllocationRequestForAdapter(t)
	req.ProfileSnapshot.Adapter = ProviderCommandConfig{
		SlurmSubmitCommand: "/opt/cxp/submit-slurm",
		ShellMode:          ProviderCommandShellCommand,
	}
	runner := &recordingProviderRunner{output: `{"provider_job_id":"shell-command-1","raw_state":"PD"}`}
	adapter := CommandProviderAdapter{Runner: runner}

	submitted, err := adapter.SubmitAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("submit allocation: %v", err)
	}
	if submitted.ProviderJobID != "shell-command-1" || len(runner.calls) != 1 {
		t.Fatalf("submitted=%#v calls=%#v", submitted, runner.calls)
	}
	call := runner.calls[0]
	for _, want := range []string{"/bin/zsh", "-lic", `exec "$0" "$@"`, "/opt/cxp/submit-slurm"} {
		if call.name != want && !containsProviderArg(call.args, want) {
			t.Fatalf("shell-command call missing %q: %#v", want, call)
		}
	}
}

func TestExecProviderCommandRunnerIgnoresSuccessfulStderr(t *testing.T) {
	out, err := ExecProviderCommandRunner{}.RunProviderCommandWithEnv(
		context.Background(),
		os.Args[0],
		[]string{"-test.run=^TestExecProviderCommandRunnerHelperProcess$"},
		append(os.Environ(), "CODEX_HELPER_TEST_PROVIDER_ADAPTER=stderr-ok"),
	)
	if err != nil {
		t.Fatalf("run provider command: %v", err)
	}
	result, err := ParseProviderCommandResult(out)
	if err != nil {
		t.Fatalf("parse provider command output %q: %v", out, err)
	}
	if result.ProviderJobID != "stderr-ok" || result.RawState != "PD" {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecProviderCommandRunnerHelperProcess(t *testing.T) {
	if os.Getenv("CODEX_HELPER_TEST_PROVIDER_ADAPTER") != "stderr-ok" {
		return
	}
	_, _ = os.Stderr.WriteString("shell startup warning\n")
	_, _ = os.Stdout.WriteString("{\"provider_job_id\":\"stderr-ok\",\"raw_state\":\"PD\"}\n")
	os.Exit(0)
}

func TestCommandProviderAdapterCancelAndRenewUseDedicatedCommands(t *testing.T) {
	req := slurmAllocationRequestForAdapter(t)
	req.ProviderIdentity.ProviderJobID = "12345"
	runner := &recordingProviderRunner{output: `{"provider_job_id":"12345","raw_state":"CA","reason":"canceled"}`}
	adapter := CommandProviderAdapter{
		Config: ProviderCommandConfig{SlurmCancelCommand: "/opt/cxp/cancel-slurm", SlurmRenewCommand: "/opt/cxp/renew-slurm"},
		Runner: runner,
	}
	if _, err := adapter.CancelAllocation(context.Background(), req); err != nil {
		t.Fatalf("cancel allocation: %v", err)
	}
	if _, err := adapter.RenewAllocation(context.Background(), req); err != nil {
		t.Fatalf("renew allocation: %v", err)
	}
	if len(runner.calls) != 2 || runner.calls[0].name != "/opt/cxp/cancel-slurm" || runner.calls[1].name != "/opt/cxp/renew-slurm" {
		t.Fatalf("calls = %#v", runner.calls)
	}
	if !containsProviderArgPair(runner.calls[0].args, "--operation", "cancel") || !containsProviderArgPair(runner.calls[0].args, "--provider-job-id", "12345") {
		t.Fatalf("cancel args = %#v", runner.calls[0].args)
	}
	if !containsProviderArgPair(runner.calls[1].args, "--operation", "renew") {
		t.Fatalf("renew args = %#v", runner.calls[1].args)
	}
}

func TestBeaconProviderEnvironmentVariablesStayDocumented(t *testing.T) {
	envVars := []string{
		BeaconSlurmQueryCommandEnv,
		BeaconSlurmSubmitCommandEnv,
		BeaconSlurmCancelCommandEnv,
		BeaconSlurmRenewCommandEnv,
		BeaconLSFQueryCommandEnv,
		BeaconLSFSubmitCommandEnv,
		BeaconLSFCancelCommandEnv,
		BeaconLSFRenewCommandEnv,
		BeaconProviderShellModeEnv,
	}
	files := map[string]string{
		"README":        filepath.Join("..", "..", "README.md"),
		"matrix":        filepath.Join("..", "..", "docs", "cxp_feature_interference_matrix.md"),
		"builtin skill": filepath.Join("..", "skills", "builtin", "cxp", "references", "commands.md"),
		"fallback help": filepath.Join("..", "cli", "teams_control_fallback_help.go"),
	}
	for label, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s %s: %v", label, path, err)
		}
		text := string(data)
		for _, envVar := range envVars {
			if !strings.Contains(text, envVar) {
				t.Fatalf("%s is missing %s", label, envVar)
			}
		}
	}
}

func TestProviderCommandArgsUseAllocationProfileSnapshotAfterProfileChanges(t *testing.T) {
	now := time.Unix(1779090000, 0)
	original := copyProfile(readyProfile("gpu"), func(p *Profile) {
		p.Provider = ProviderSlurm
		p.Slurm = SlurmProfile{Nodes: 2, GPUCount: 8, Partition: "interactive", Image: "image.sqsh", Duration: 6}
	})
	st := State{
		Profiles: map[string]Profile{"gpu": original},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-old", Profile: "gpu", State: string(LeaseStarting)},
		},
	}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", Signature: "sig-a", ProxyRoute: "ssh://proxy-a"}, now)
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	req.ProviderIdentity.ProviderJobID = "slurm-old"
	req.Target.ProviderJobID = "slurm-old"
	req.Target.MachineID = "machine-1"
	req.Target.LeaseID = "lease-1"
	req.State = AllocationRunning
	req.SubmitAttempts = 1
	st.Allocations[req.ID] = req

	st.Profiles["gpu"] = copyProfile(original, func(p *Profile) {
		p.Slurm = SlurmProfile{Nodes: 99, GPUCount: 99, Partition: "wrong", Image: "wrong.sqsh", Duration: 99}
	})
	delete(st.Profiles, "gpu")

	lost := ProjectRawProviderState(ProviderSlurm, "F", "lost before worker claim", false, true)
	replacement, err := UpdateAllocationProjection(&st, req.ID, lost, now.Add(time.Second))
	if err != nil {
		t.Fatalf("UpdateAllocationProjection: %v", err)
	}
	if replacement.State != AllocationRequestPersisted || replacement.ReplacementID != "slurm-old" || replacement.ReplacementEpoch != 1 {
		t.Fatalf("expected pre-start replacement allocation, got %#v", replacement)
	}
	args := providerCommandArgs(replacement, "submit")
	for _, want := range [][]string{
		{"--partition", "interactive"},
		{"--image", "image.sqsh"},
		{"--nodes", "2"},
		{"--gpu", "8"},
		{"--duration", "6"},
		{"--execution-hash", "sig-a"},
		{"--proxy-route", "ssh://proxy-a"},
		{"--replacement-of", "slurm-old"},
		{"--replacement-epoch", "1"},
	} {
		if !containsProviderArgPair(args, want[0], want[1]) {
			t.Fatalf("provider args should use allocation snapshot and replacement fence, missing %v in %v", want, args)
		}
	}
	for _, stale := range []string{"wrong", "wrong.sqsh", "99"} {
		if containsProviderArg(args, stale) {
			t.Fatalf("provider args used mutated live profile value %q: %v", stale, args)
		}
	}
}

func TestCommandProviderAdapterLSFUsesQueueSnapshot(t *testing.T) {
	req := AllocationRequest{
		ID:                "req-lsf",
		ConversationID:    "conv",
		TurnID:            "turn",
		Profile:           "batch",
		ProfileSnapshot:   Profile{Name: "batch", Provider: ProviderLSF, LSF: LSFProfile{QueueName: "normal", SitePolicyDerivesResources: true, AdvancedApproved: true}},
		Provider:          ProviderLSF,
		Isolation:         IsolationExclusive,
		DeterministicName: "cxp-req-lsf",
	}
	runner := &recordingProviderRunner{output: "provider_job_id=lsf-1 raw_state=PEND"}
	adapter := CommandProviderAdapter{
		Config: ProviderCommandConfig{LSFSubmitCommand: "/opt/cxp/submit-lsf"},
		Runner: runner,
	}
	result, err := adapter.SubmitAllocation(context.Background(), req)
	if err != nil {
		t.Fatalf("submit lsf: %v", err)
	}
	if result.ProviderJobID != "lsf-1" || len(runner.calls) != 1 {
		t.Fatalf("result=%#v calls=%#v", result, runner.calls)
	}
	args := runner.calls[0].args
	for _, want := range [][]string{{"--queue", "normal"}, {"--isolation", string(IsolationExclusive)}, {"--operation", "submit"}} {
		if !containsProviderArgPair(args, want[0], want[1]) {
			t.Fatalf("lsf args missing %v in %v", want, args)
		}
	}
	for _, flag := range []string{"--lsf-site-policy", "--lsf-advanced-approved"} {
		if !containsProviderArg(args, flag) {
			t.Fatalf("lsf args missing %s in %v", flag, args)
		}
	}
}

func TestReconcileAllocationWithCommandAdapterPersistsSubmitError(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	st.Profiles["gpu"] = copyProfile(st.Profiles["gpu"], func(p *Profile) {
		p.Provider = ProviderSlurm
		p.Slurm = SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4}
	})
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	runner := &recordingProviderRunner{
		outputByCommand: map[string]string{"/query": "durable_negative=true"},
		errByCommand:    map[string]error{"/submit": errors.New("scheduler down")},
	}
	adapter := CommandProviderAdapter{
		Config: ProviderCommandConfig{SlurmQueryCommand: "/query", SlurmSubmitCommand: "/submit"},
		Runner: runner,
	}

	updated, action, err := ReconcileAllocationSubmit(context.Background(), &st, req.ID, adapter, time.Unix(2, 0))
	if err == nil || !strings.Contains(err.Error(), "scheduler down") {
		t.Fatalf("reconcile error = %v", err)
	}
	if action != AllocationSubmitNow || updated.State != AllocationNeedsAttention || updated.SubmitAttempts != 1 {
		t.Fatalf("unexpected reconcile result action=%s req=%#v", action, updated)
	}
	if got := []string{runner.calls[0].name, runner.calls[1].name}; !reflect.DeepEqual(got, []string{"/query", "/submit"}) {
		t.Fatalf("calls = %#v", runner.calls)
	}
}

func TestReconcileAllocationSubmitOutsideLockAllowsAdapterStoreReads(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn", TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	if err := store.Save(st); err != nil {
		t.Fatalf("save store: %v", err)
	}
	adapter := &storeReadingAllocationAdapter{
		store:  store,
		query:  SchedulerQueryResult{DurableNegative: true},
		submit: SchedulerQueryResult{ProviderJobID: "slurm-1", RawState: "PD", Reason: "submitted"},
	}
	updated, action, err := ReconcileAllocationSubmitOutsideLock(context.Background(), store, req.ID, adapter, time.Unix(2, 0))
	if err != nil || action != AllocationSubmitNow || updated.ProviderIdentity.ProviderJobID != "slurm-1" || adapter.submits != 1 {
		t.Fatalf("outside-lock reconcile action=%s req=%#v submits=%d err=%v", action, updated, adapter.submits, err)
	}
}

type storeReadingAllocationAdapter struct {
	store   *Store
	query   SchedulerQueryResult
	submit  SchedulerQueryResult
	submits int
}

func (a *storeReadingAllocationAdapter) QueryAllocation(ctx context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	if _, err := a.store.Load(); err != nil {
		return SchedulerQueryResult{}, err
	}
	return a.query, nil
}

func (a *storeReadingAllocationAdapter) SubmitAllocation(ctx context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	if _, err := a.store.Load(); err != nil {
		return SchedulerQueryResult{}, err
	}
	a.submits++
	return a.submit, nil
}

func slurmAllocationRequestForAdapter(t *testing.T) AllocationRequest {
	t.Helper()
	var st State
	st.Profiles = map[string]Profile{"gpu": readyProfile("gpu")}
	st.Profiles["gpu"] = copyProfile(st.Profiles["gpu"], func(p *Profile) {
		p.Provider = ProviderSlurm
		p.Slurm = SlurmProfile{Nodes: 2, GPUCount: 8, Partition: "interactive", Image: "image.sqsh", Duration: 6}
	})
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn", TargetSnapshot{Target: TargetBeacon, Profile: "gpu", Signature: "sig-a"}, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest: %v", err)
	}
	return req
}

type providerRunnerCall struct {
	name string
	args []string
	env  []string
}

type recordingProviderRunner struct {
	calls           []providerRunnerCall
	output          string
	outputByCommand map[string]string
	errByCommand    map[string]error
}

func (r *recordingProviderRunner) RunProviderCommand(_ context.Context, name string, args []string) (string, error) {
	r.calls = append(r.calls, providerRunnerCall{name: name, args: append([]string(nil), args...)})
	if err := r.errByCommand[name]; err != nil {
		return "", err
	}
	if out, ok := r.outputByCommand[name]; ok {
		return out, nil
	}
	return r.output, nil
}

func (r *recordingProviderRunner) RunProviderCommandWithEnv(_ context.Context, name string, args []string, env []string) (string, error) {
	r.calls = append(r.calls, providerRunnerCall{name: name, args: append([]string(nil), args...), env: append([]string(nil), env...)})
	if err := r.errByCommand[name]; err != nil {
		return "", err
	}
	if out, ok := r.outputByCommand[name]; ok {
		return out, nil
	}
	return r.output, nil
}

func containsProviderArg(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsProviderArgSubstring(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func containsProviderArgPair(values []string, key string, value string) bool {
	for i := 0; i+1 < len(values); i++ {
		if values[i] == key && values[i+1] == value {
			return true
		}
	}
	return false
}
