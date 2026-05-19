package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestBeaconProfileCLIWorkflow(t *testing.T) {
	t.Setenv(beacon.BeaconProviderShellModeEnv, "")
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	adapter := writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-doctor raw_state=PD reason=doctor`)

	out, err := runBeaconRootCommand(t,
		"--config", configPath,
		"beacon", "--store", storePath,
		"profile", "create", "gpu",
		"--provider", "slurm",
		"--nodes", "1",
		"--gpu", "1",
		"--partition", "interactive",
		"--image", "image.sqsh",
		"--duration", "4",
		"--query-command", adapter,
		"--submit-command", adapter,
		"--cancel-command", adapter,
		"--renew-command", adapter,
	)
	if err != nil {
		t.Fatalf("profile create: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Created draft beacon profile") || !strings.Contains(out, "doctor failed") || !strings.Contains(out, "needs confirm") {
		t.Fatalf("create output should show draft reasons:\n%s", out)
	}

	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "doctor", "gpu", "--smoke")
	if err != nil {
		t.Fatalf("profile doctor: %v\n%s", err, out)
	}
	if !strings.Contains(out, "smoke: operation=smoke_submit") || !strings.Contains(out, "provider_job=slurm-doctor") {
		t.Fatalf("profile doctor smoke output missing expected facts:\n%s", out)
	}
	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "confirm", "gpu")
	if err != nil {
		t.Fatalf("profile confirm: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Confirmed ready beacon profile") {
		t.Fatalf("confirm output = %s", out)
	}

	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "status", "gpu")
	if err != nil {
		t.Fatalf("profile status: %v\n%s", err, out)
	}
	if !strings.Contains(out, "profile=gpu") || !strings.Contains(out, "status=ready") || !strings.Contains(out, "adapter=profile:query,submit,cancel,renew,shell=user(default)") {
		t.Fatalf("status output = %s", out)
	}

	out, err = runBeaconRootCommand(t,
		"--config", configPath,
		"beacon", "--store", storePath,
		"profile", "update", "gpu",
		"--provider", "slurm",
		"--nodes", "2",
		"--gpu", "4",
		"--partition", "interactive",
		"--image", "new.sqsh",
		"--duration", "8",
	)
	if err != nil {
		t.Fatalf("profile update: %v\n%s", err, out)
	}
	if !strings.Contains(out, "revision 2") || !strings.Contains(out, "doctor failed") {
		t.Fatalf("update output = %s", out)
	}

	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "history", "gpu")
	if err != nil {
		t.Fatalf("profile history: %v\n%s", err, out)
	}
	if !strings.Contains(out, "rev=1") || !strings.Contains(out, "rev=2") || !strings.Contains(out, "current") {
		t.Fatalf("history output = %s", out)
	}
	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "rollback", "gpu", "1")
	if err != nil {
		t.Fatalf("profile rollback: %v\n%s", err, out)
	}
	if !strings.Contains(out, "revision 3") || !strings.Contains(out, "ready") {
		t.Fatalf("rollback output = %s", out)
	}
	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "gc", "gpu")
	if err != nil {
		t.Fatalf("profile gc: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Pruned 2") {
		t.Fatalf("profile gc output = %s", out)
	}
}

func TestCLIBeaconAdapterLabelHonorsGlobalShellMode(t *testing.T) {
	t.Setenv(beacon.BeaconProviderShellModeEnv, beacon.ProviderCommandShellDirect)
	profile := beacon.Profile{
		Provider: beacon.ProviderSlurm,
		Adapter:  beacon.ProviderCommandConfigForProvider(beacon.ProviderSlurm, "/query", "/submit", "/cancel", "/renew"),
	}
	if got := cliBeaconAdapterLabel(profile); got != "profile:query,submit,cancel,renew,shell=direct" {
		t.Fatalf("adapter label = %q", got)
	}
}

func TestBeaconBootstrapDiagnosticsUsesStableCXPPath(t *testing.T) {
	lockCLITestHooks(t)
	prevExecutable := beaconExecutable
	prevArgv0 := restartArgv0
	t.Cleanup(func() {
		beaconExecutable = prevExecutable
		restartArgv0 = prevArgv0
	})

	dir := t.TempDir()
	name := "codex-proxy"
	if runtime.GOOS == "windows" {
		name = "codex-proxy.exe"
	}
	stable := filepath.Join(dir, name)
	if err := os.WriteFile(stable, []byte("helper"), 0o755); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	beaconExecutable = func() (string, error) {
		return filepath.Join(t.TempDir(), "go-build123", "b001", "exe", name), nil
	}
	restartArgv0 = func() string { return stable }

	got := beaconWorkerBootstrapDiagnostics("", "")
	if got.CXPPath != stable {
		t.Fatalf("CXPPath = %q, want stable %q", got.CXPPath, stable)
	}
}

func TestBeaconCLIRejectsDraftSwitchAndShowsStatus(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	if _, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "create", "lsf", "--provider", "lsf", "--queue", "q"); err != nil {
		t.Fatalf("create lsf draft: %v", err)
	}
	out, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "switch-profile", "lsf", "--session", "conv-1")
	if err == nil {
		t.Fatalf("switch to draft profile succeeded, want error\n%s", out)
	}
	if !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("switch error = %v", err)
	}
}

func TestBeaconCLILSFQueueOnlyProfileCanBecomeReady(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	adapter := writeBeaconCLIProviderFixture(t, `provider_job_id=lsf-doctor raw_state=PEND reason=doctor`)
	if _, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "create", "lsf", "--provider", "lsf", "--queue", "o_pri_interactive", "--query-command", adapter, "--submit-command", adapter, "--cancel-command", adapter, "--renew-command", adapter); err != nil {
		t.Fatalf("create lsf profile: %v", err)
	}
	if _, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "doctor", "lsf"); err != nil {
		t.Fatalf("doctor lsf profile: %v", err)
	}
	out, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "confirm", "lsf")
	if err != nil {
		t.Fatalf("confirm lsf profile: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Confirmed ready beacon profile") {
		t.Fatalf("confirm output = %s", out)
	}
}

func TestBeaconCLISwitchRejectsMissingProxyProfile(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles["gpu"] = beacon.Profile{
			Name:              "gpu",
			Provider:          beacon.ProviderSlurm,
			ProxyMode:         beacon.ProxySSHProfile,
			ProxyProfile:      "missing-proxy",
			IsolationDefault:  beacon.IsolationShared,
			Slurm:             beacon.SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4},
			Confirmed:         true,
			ProviderPreviewOK: true,
			DoctorOK:          true,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
	out, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "switch-profile", "gpu", "--session", "conv-1")
	if err == nil || !strings.Contains(err.Error(), "proxy profile not found") {
		t.Fatalf("switch output=%q error=%v, want missing proxy rejection", out, err)
	}
}

func TestBeaconCLISwitchAfterCurrentTurnSetsPendingTarget(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
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
		st.Conversations["conv-1"] = beacon.Conversation{
			ID:      "conv-1",
			Current: beacon.TargetSnapshot{Target: beacon.TargetLocal},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon state: %v", err)
	}

	out, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "switch-profile", "gpu", "--session", "conv-1", "--after-current-turn")
	if err != nil {
		t.Fatalf("switch after current turn: %v\n%s", err, out)
	}
	if !strings.Contains(out, "future turns use gpu") {
		t.Fatalf("switch output = %s", out)
	}
	if strings.Contains(out, "stays ;") || !strings.Contains(out, "current turn stays local") {
		t.Fatalf("switch output should name the preserved local target, got %s", out)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load beacon state: %v", err)
	}
	conv := st.Conversations["conv-1"]
	if conv.Current.Target != beacon.TargetLocal {
		t.Fatalf("current target = %#v, want local target preserved", conv.Current)
	}
	if conv.Pending == nil || conv.Pending.Target != beacon.TargetBeacon || conv.Pending.Profile != "gpu" {
		t.Fatalf("pending target = %#v, want beacon gpu", conv.Pending)
	}
	queued, err := beacon.QueueTurn(&st, "conv-1", "turn-next", time.Now())
	if err != nil {
		t.Fatalf("QueueTurn after deferred switch: %v", err)
	}
	if queued.Snapshot.Target != beacon.TargetBeacon || queued.Snapshot.Profile != "gpu" {
		t.Fatalf("queued snapshot = %#v, want future turn on beacon gpu", queued.Snapshot)
	}
}

func TestBeaconCLISwitchRequiresForkForIncompatibleSignature(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
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
		st.Conversations["conv-1"] = beacon.Conversation{
			ID:      "conv-1",
			Current: beacon.TargetSnapshot{Target: beacon.TargetLocal},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon state: %v", err)
	}

	out, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "switch-profile", "gpu", "--session", "conv-1", "--signature-compatible=false")
	if err != nil {
		t.Fatalf("incompatible switch should return guidance, not a hard error: %v\n%s", err, out)
	}
	if !strings.Contains(out, "incompatible execution signature") || !strings.Contains(out, "--fork") {
		t.Fatalf("incompatible switch output = %s", out)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after rejected switch: %v", err)
	}
	if st.Conversations["conv-1"].Current.Target != beacon.TargetLocal || st.Conversations["conv-1"].Pending != nil {
		t.Fatalf("incompatible switch without fork changed state: %#v", st.Conversations["conv-1"])
	}

	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "switch-profile", "gpu", "--session", "conv-1", "--signature-compatible=false", "--fork")
	if err != nil {
		t.Fatalf("switch with fork: %v\n%s", err, out)
	}
	st, err = store.Load()
	if err != nil {
		t.Fatalf("load after fork switch: %v", err)
	}
	if st.Conversations["conv-1"].Current.Target != beacon.TargetBeacon || st.Conversations["conv-1"].Current.Profile != "gpu" {
		t.Fatalf("fork switch did not apply beacon target: %#v", st.Conversations["conv-1"].Current)
	}
}

func TestBeaconCLIProfileListResolvesExistingProxyProfile(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	cfgStore, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("config.NewStore: %v", err)
	}
	if err := cfgStore.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{{ID: "jump-a", Name: "jump-a"}}}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	adapter := writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-doctor raw_state=PD reason=doctor`)
	if _, err := runBeaconRootCommand(t,
		"--config", configPath,
		"beacon", "--store", storePath,
		"profile", "create", "gpu",
		"--provider", "slurm",
		"--proxy", "ssh_profile",
		"--proxy-profile", "jump-a",
		"--nodes", "1",
		"--gpu", "1",
		"--partition", "interactive",
		"--image", "image.sqsh",
		"--duration", "4",
		"--query-command", adapter,
		"--submit-command", adapter,
		"--cancel-command", adapter,
		"--renew-command", adapter,
	); err != nil {
		t.Fatalf("create profile: %v", err)
	}
	if _, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "doctor", "gpu"); err != nil {
		t.Fatalf("doctor profile: %v", err)
	}
	if _, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "confirm", "gpu"); err != nil {
		t.Fatalf("confirm profile: %v", err)
	}
	out, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "list")
	if err != nil {
		t.Fatalf("list profile: %v\n%s", err, out)
	}
	if !strings.Contains(out, "gpu\tslurm\trev=1\tready") {
		t.Fatalf("profile list output = %s", out)
	}
}

func TestBeaconMachineCLIReleaseAndKillPreview(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["gpu-a"] = beacon.Machine{
			ID:            "gpu-a",
			LeaseID:       "lease-1",
			ProviderJobID: "slurm-1",
			State:         "accepting",
			Chats:         []string{"chat-a"},
			Jobs:          []string{"job-1"},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed machine: %v", err)
	}
	out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "machine", "status", "gpu-a")
	if err != nil {
		t.Fatalf("machine status: %v\n%s", err, out)
	}
	if !strings.Contains(out, "kill_confirmation: `KILL-lease-1`") || !strings.Contains(out, "jobs: `job-1`") {
		t.Fatalf("machine status output = %s", out)
	}
	out, err = runBeaconRootCommand(t, "beacon", "--store", storePath, "machine", "release", "gpu-a")
	if err != nil {
		t.Fatalf("machine release: %v\n%s", err, out)
	}
	if !strings.Contains(out, "action=drain") {
		t.Fatalf("release output = %s", out)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after release: %v", err)
	}
	if st.Machines["gpu-a"].State != "draining" {
		t.Fatalf("machine state after release = %q, want draining", st.Machines["gpu-a"].State)
	}
	out, err = runBeaconRootCommand(t, "beacon", "--store", storePath, "machine", "kill", "lease-1")
	if err != nil {
		t.Fatalf("machine kill preview without confirm should not error: %v\n%s", err, out)
	}
	if !strings.Contains(out, "action=reject_confirmation") {
		t.Fatalf("kill without confirm output = %s", out)
	}
	out, err = runBeaconRootCommand(t, "beacon", "--store", storePath, "machine", "kill", "lease-1", "--confirm", "KILL-lease-1")
	if err != nil {
		t.Fatalf("machine kill: %v\n%s", err, out)
	}
	if !strings.Contains(out, "action=kill_quarantine") {
		t.Fatalf("kill output = %s", out)
	}
	st, err = store.Load()
	if err != nil {
		t.Fatalf("load after kill: %v", err)
	}
	if st.Machines["gpu-a"].State != "kill_quarantine" {
		t.Fatalf("machine state after kill = %q, want kill_quarantine", st.Machines["gpu-a"].State)
	}
}

func TestBeaconAllocationCLIListStatusAndReconcile(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
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
		_, _, err := beacon.EnsureAllocationRequest(st, "conv-1", "turn-1", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", Signature: "sig-a"}, time.Unix(1, 0))
		return err
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}

	out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "allocation", "list")
	if err != nil {
		t.Fatalf("allocation list: %v\n%s", err, out)
	}
	if !strings.Contains(out, "request_persisted on gpu") || !strings.Contains(out, "(Slurm)") {
		t.Fatalf("allocation list output = %s", out)
	}
	out, err = runBeaconRootCommand(t, "beacon", "--store", storePath, "allocation", "status", "conv-1")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("allocation status by unknown ref should fail, out=%q err=%v", out, err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load store: %v", err)
	}
	var req beacon.AllocationRequest
	for _, value := range st.Allocations {
		req = value
	}
	out, err = runBeaconRootCommand(t, "beacon", "--store", storePath, "allocation", "status", req.ID)
	if err != nil {
		t.Fatalf("allocation status: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Beacon allocation: "+req.ID) || !strings.Contains(out, "deterministic_name: `"+req.DeterministicName+"`") {
		t.Fatalf("allocation status output = %s", out)
	}

	t.Setenv(beacon.BeaconSlurmQueryCommandEnv, writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-777 raw_state=PD reason=resources`))
	out, err = runBeaconRootCommand(t, "beacon", "--store", storePath, "allocation", "reconcile", req.ID)
	if err != nil {
		t.Fatalf("allocation reconcile: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Beacon allocation reconcile: adopt_existing") || !strings.Contains(out, "provider_job: `slurm-777`") {
		t.Fatalf("allocation reconcile output = %s", out)
	}
	st, err = store.Load()
	if err != nil {
		t.Fatalf("load after reconcile: %v", err)
	}
	if got := st.Allocations[req.ID].ProviderIdentity.ProviderJobID; got != "slurm-777" {
		t.Fatalf("provider job after reconcile = %q", got)
	}
}

func TestBeaconProfileAdapterCommandsDriveAllocationReconcileWithoutHelperEnv(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	query := writeBeaconCLIProviderFixture(t, `durable_negative=true`)
	submit := writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-profile raw_state=PD reason=submitted-from-profile`)
	cancel := writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-profile raw_state=CA reason=canceled`)
	renew := writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-profile raw_state=R reason=renewed`)

	out, err := runBeaconRootCommand(t,
		"--config", configPath,
		"beacon", "--store", storePath,
		"profile", "create", "gpu",
		"--provider", "slurm",
		"--nodes", "1",
		"--gpu", "1",
		"--partition", "interactive",
		"--image", "image.sqsh",
		"--duration", "4",
		"--query-command", query,
		"--submit-command", submit,
		"--cancel-command", cancel,
		"--renew-command", renew,
	)
	if err != nil {
		t.Fatalf("profile create: %v\n%s", err, out)
	}
	for _, args := range [][]string{
		{"beacon", "--store", storePath, "profile", "doctor", "gpu"},
		{"beacon", "--store", storePath, "profile", "confirm", "gpu"},
	} {
		if out, err := runBeaconRootCommand(t, append([]string{"--config", configPath}, args...)...); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
	}
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	var req beacon.AllocationRequest
	if err := store.Update(func(st *beacon.State) error {
		var err error
		req, _, err = beacon.EnsureAllocationRequest(st, "conv-profile-adapter", "turn-1", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		return err
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}

	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "allocation", "reconcile", req.ID)
	if err != nil {
		t.Fatalf("allocation reconcile with profile adapter: %v\n%s", err, out)
	}
	for _, want := range []string{"Beacon allocation reconcile: submit", "provider_job: `slurm-profile`"} {
		if !strings.Contains(out, want) {
			t.Fatalf("reconcile output missing %q:\n%s", want, out)
		}
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after reconcile: %v", err)
	}
	if got := st.Allocations[req.ID].ProviderIdentity.ProviderJobID; got != "slurm-profile" {
		t.Fatalf("provider job after reconcile = %q", got)
	}
}

func TestBeaconProfileUpdateAdapterShellPreservesExistingFields(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	adapter := writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-profile raw_state=PD`)

	out, err := runBeaconRootCommand(t,
		"--config", configPath,
		"beacon", "--store", storePath,
		"profile", "create", "gpu",
		"--provider", "slurm",
		"--nodes", "1",
		"--gpu", "1",
		"--partition", "interactive",
		"--image", "image.sqsh",
		"--duration", "4",
		"--query-command", adapter,
		"--submit-command", adapter,
	)
	if err != nil {
		t.Fatalf("profile create: %v\n%s", err, out)
	}
	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "update", "gpu", "--adapter-shell", "user")
	if err != nil {
		t.Fatalf("profile update: %v\n%s", err, out)
	}
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load store: %v", err)
	}
	p := st.Profiles["gpu"]
	if p.Provider != beacon.ProviderSlurm || p.Slurm.Image != "image.sqsh" || p.Slurm.GPUCount != 1 {
		t.Fatalf("partial update changed profile fields: %#v", p)
	}
	if p.Adapter.SlurmSubmitCommand != adapter || p.Adapter.ShellMode != beacon.ProviderCommandShellUser {
		t.Fatalf("partial update adapter = %#v", p.Adapter)
	}
}

func TestBeaconReleaseCLIResolvesProfileAllocations(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
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
		req, _, err = beacon.EnsureAllocationRequest(st, "conv-1", "turn-1", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.State = beacon.AllocationRunning
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}

	out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "release", "gpu")
	if err != nil {
		t.Fatalf("release profile: %v\n%s", err, out)
	}
	if !strings.Contains(out, "action=mark_canceled") || !strings.Contains(out, "profile=gpu") {
		t.Fatalf("release output = %s", out)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after release: %v", err)
	}
	if st.Allocations[req.ID].State != beacon.AllocationCanceled {
		t.Fatalf("allocation state = %s, want canceled", st.Allocations[req.ID].State)
	}
}

func TestBeaconProviderTemplateCLI(t *testing.T) {
	out, err := runBeaconRootCommand(t, "beacon", "provider", "template", "slurm")
	if err != nil {
		t.Fatalf("provider template slurm: %v\n%s", err, out)
	}
	for _, want := range []string{"squeue", "sbatch", "scancel", "beacon worker serve --allocation", "--provider-job-id", "cancel)", "renew)", "renew_requires_site_policy", "exit 1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("slurm template missing %q:\n%s", want, out)
		}
	}
	out, err = runBeaconRootCommand(t, "beacon", "provider", "template", "lsf")
	if err != nil {
		t.Fatalf("provider template lsf: %v\n%s", err, out)
	}
	for _, want := range []string{"bjobs", "bsub", "bkill", "beacon worker serve --allocation", "--provider-job-id", "cancel)", "renew)", "renew_requires_site_policy", "exit 1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("lsf template missing %q:\n%s", want, out)
		}
	}
}

func TestBeaconAllocationReconcileAllRecoversStaleState(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
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
		req, _, err = beacon.EnsureAllocationRequest(st, "conv-reconcile", "turn-reconcile", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-rec"
		req.State = beacon.AllocationSubmitted
		st.Allocations[req.ID] = req
		st.Machines["machine-rec"] = beacon.Machine{ID: "machine-rec", LeaseID: "lease-rec", ProviderJobID: "slurm-rec", WorkerID: "worker-rec", Profile: "gpu", State: string(beacon.LeaseAccepting), LastHeartbeat: time.Unix(1, 0)}
		st.JobAttempts["job-claimed"] = beacon.JobAttempt{ID: "job-claimed", RequestID: req.ID, Phase: beacon.JobClaimed, WorkerID: "worker-rec", UpdatedAt: time.Unix(1, 0)}
		return nil
	}); err != nil {
		t.Fatalf("seed reconcile-all state: %v", err)
	}
	t.Setenv(beacon.BeaconSlurmQueryCommandEnv, writeBeaconCLIProviderFixture(t, `provider_job_id=slurm-rec raw_state=R reason=running`))
	out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "allocation", "reconcile-all", "--stale-after", "1s", "--stale-job-after", "1s")
	if err != nil {
		t.Fatalf("allocation reconcile-all: %v\n%s", err, out)
	}
	for _, want := range []string{"allocation " + req.ID + ":", "state=running", "machine=machine-rec action=drain_stale", "job=job-claimed action=recover_stale phase=queued"} {
		if !strings.Contains(out, want) {
			t.Fatalf("reconcile-all output missing %q:\n%s", want, out)
		}
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after reconcile-all: %v", err)
	}
	if st.Machines["machine-rec"].State != string(beacon.LeaseDraining) || st.JobAttempts["job-claimed"].Phase != beacon.JobQueued {
		t.Fatalf("reconcile-all state = machine=%#v job=%#v", st.Machines["machine-rec"], st.JobAttempts["job-claimed"])
	}
}

func TestBeaconWorkerRunOnceClaimsJobAndWritesTerminal(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
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
		st.Machines["machine-1"] = beacon.Machine{ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(beacon.LeaseAccepting)}
		req, _, err := beacon.EnsureAllocationRequest(st, "conv-1", "turn-1", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu", MachineID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		_, _, err = beacon.EnqueueJobAttempt(st, req.ID, st.Machines["machine-1"], beacon.JobPayload{Prompt: "remote prompt", WorkingDir: t.TempDir()}, time.Unix(2, 0))
		return err
	}); err != nil {
		t.Fatalf("seed beacon job: %v", err)
	}
	codexPath := writeBeaconCLICodexFixture(t, "worker done")
	out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "worker", "run-once", "--machine", "machine-1", "--worker", "worker-1", "--codex-path", codexPath)
	if err != nil {
		t.Fatalf("worker run-once: %v\n%s", err, out)
	}
	if !strings.Contains(out, "terminal=valid") || !strings.Contains(out, "outbox_queued=true") {
		t.Fatalf("worker output = %s", out)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after worker: %v", err)
	}
	if len(st.Terminals) != 1 {
		t.Fatalf("terminal count = %d, state=%#v", len(st.Terminals), st)
	}
	for _, terminal := range st.Terminals {
		if !strings.Contains(terminal.Payload, "worker done") || !strings.Contains(terminal.Payload, "thread-worker") {
			t.Fatalf("terminal payload = %#v", terminal)
		}
	}
	if jobs := st.Machines["machine-1"].Jobs; len(jobs) != 0 {
		t.Fatalf("worker terminal should clear active machine job, jobs=%#v", jobs)
	}
}

func TestBeaconWorkerRunOnceRegistersAllocationAndWaitsForJob(t *testing.T) {
	lockCLITestHooks(t)
	stableHelper := filepath.Join(t.TempDir(), "codex-proxy")
	if err := os.WriteFile(stableHelper, []byte("helper"), 0o755); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	prevExecutable := beaconExecutable
	beaconExecutable = func() (string, error) { return stableHelper, nil }
	t.Cleanup(func() { beaconExecutable = prevExecutable })

	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
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
		req, _, err = beacon.EnsureAllocationRequest(st, "conv-1", "turn-1", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-42"
		req.State = beacon.AllocationSubmitted
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	codexPath := writeBeaconCLICodexFixture(t, "waited worker done")
	type commandResult struct {
		out string
		err error
	}
	done := make(chan commandResult, 1)
	go func() {
		out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "worker", "run-once", "--allocation", req.ID, "--worker", "worker-alloc", "--provider-job", "slurm-42", "--wait", "2s", "--codex-path", codexPath)
		done <- commandResult{out: out, err: err}
	}()
	machine := waitForBeaconMachineByProviderJob(t, store, "slurm-42")
	if err := store.Update(func(st *beacon.State) error {
		_, _, err := beacon.EnqueueJobAttempt(st, req.ID, machine, beacon.JobPayload{Prompt: "remote prompt", WorkingDir: t.TempDir()}, time.Unix(2, 0))
		return err
	}); err != nil {
		t.Fatalf("enqueue job after worker registration: %v", err)
	}
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("worker run-once --allocation: %v\n%s", res.err, res.out)
		}
		if !strings.Contains(res.out, "registered machine="+machine.ID) || !strings.Contains(res.out, "terminal=valid") {
			t.Fatalf("worker output = %s", res.out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for allocation worker command")
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after allocation worker: %v", err)
	}
	if len(st.Terminals) != 1 {
		t.Fatalf("terminal count = %d, state=%#v", len(st.Terminals), st)
	}
	for _, terminal := range st.Terminals {
		if !strings.Contains(terminal.Payload, "waited worker done") {
			t.Fatalf("terminal payload = %#v", terminal)
		}
	}
}

func TestBeaconWorkerRegistrationFailurePreservesNeedsAttention(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
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
		req, _, err = beacon.EnsureAllocationRequest(st, "conv-bad", "turn-bad", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-bad"
		req.State = beacon.AllocationSubmitted
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	missingCodex := filepath.Join(t.TempDir(), "missing-codex")
	out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "worker", "run-once", "--allocation", req.ID, "--provider-job", "slurm-bad", "--codex-path", missingCodex)
	if err == nil || !strings.Contains(err.Error(), "not accepting after doctor") {
		t.Fatalf("bad worker registration should fail before claim, err=%v out=%s", err, out)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after bad worker: %v", err)
	}
	var machine beacon.Machine
	for _, current := range st.Machines {
		if current.ProviderJobID == "slurm-bad" {
			machine = current
			break
		}
	}
	if machine.ID == "" || machine.State != string(beacon.LeaseNeedsAttention) || len(machine.DoctorBlockers) == 0 {
		t.Fatalf("doctor failure should remain visible as needs_attention, machine=%#v state=%#v", machine, st)
	}
}

func TestBeaconWorkerServeRegistersAndRunsOneJob(t *testing.T) {
	lockCLITestHooks(t)
	stableHelper := filepath.Join(t.TempDir(), "codex-proxy")
	if err := os.WriteFile(stableHelper, []byte("helper"), 0o755); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	prevExecutable := beaconExecutable
	beaconExecutable = func() (string, error) { return stableHelper, nil }
	t.Cleanup(func() { beaconExecutable = prevExecutable })

	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
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
		req, _, err = beacon.EnsureAllocationRequest(st, "conv-serve", "turn-serve", beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}, time.Unix(1, 0))
		if err != nil {
			return err
		}
		req.ProviderIdentity.ProviderJobID = "slurm-serve"
		req.State = beacon.AllocationSubmitted
		st.Allocations[req.ID] = req
		return nil
	}); err != nil {
		t.Fatalf("seed serve allocation: %v", err)
	}
	codexPath := writeBeaconCLICodexFixture(t, "served worker done")
	type commandResult struct {
		out string
		err error
	}
	done := make(chan commandResult, 1)
	go func() {
		out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "worker", "serve", "--allocation", req.ID, "--worker", "worker-serve", "--provider-job", "slurm-serve", "--idle-timeout", "2s", "--max-jobs", "1", "--codex-path", codexPath)
		done <- commandResult{out: out, err: err}
	}()
	machine := waitForBeaconMachineByProviderJob(t, store, "slurm-serve")
	if machine.State != string(beacon.LeaseAccepting) {
		t.Fatalf("serve registered machine state = %#v", machine)
	}
	if err := store.Update(func(st *beacon.State) error {
		_, _, err := beacon.EnqueueJobAttempt(st, req.ID, machine, beacon.JobPayload{Prompt: "serve prompt", WorkingDir: t.TempDir()}, time.Unix(2, 0))
		return err
	}); err != nil {
		t.Fatalf("enqueue serve job: %v", err)
	}
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("worker serve: %v\n%s", res.err, res.out)
		}
		for _, want := range []string{"registered machine=" + machine.ID, "terminal=valid", "Served max jobs: 1"} {
			if !strings.Contains(res.out, want) {
				t.Fatalf("worker serve output missing %q:\n%s", want, res.out)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker serve")
	}
}

func TestBeaconMachineCLIReleaseIdleMachineRemovesLease(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["cpu-a"] = beacon.Machine{ID: "cpu-a", LeaseID: "lease-cpu", State: "accepting"}
		return nil
	}); err != nil {
		t.Fatalf("seed machine: %v", err)
	}
	out, err := runBeaconRootCommand(t, "beacon", "--store", storePath, "machine", "release", "cpu-a")
	if err != nil {
		t.Fatalf("machine release: %v\n%s", err, out)
	}
	if !strings.Contains(out, "action=release") {
		t.Fatalf("release output = %s", out)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load after release: %v", err)
	}
	if _, ok := st.Machines["cpu-a"]; ok {
		t.Fatalf("idle machine should be removed after release: %#v", st.Machines["cpu-a"])
	}
}

func writeBeaconCLIProviderFixture(t *testing.T, output string) string {
	t.Helper()
	if os.PathSeparator != '/' {
		t.Skip("POSIX provider fixture script")
	}
	path := filepath.Join(t.TempDir(), "provider-fixture.sh")
	body := "#!/bin/sh\nprintf '%s\\n' " + shellSingleQuoteForBeaconCLITest(output) + "\n"
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write provider fixture: %v", err)
	}
	return path
}

func writeBeaconCLICodexFixture(t *testing.T, final string) string {
	t.Helper()
	if os.PathSeparator != '/' {
		t.Skip("POSIX codex fixture script")
	}
	path := filepath.Join(t.TempDir(), "codex-fixture.sh")
	body := strings.Join([]string{
		"#!/bin/sh",
		"cat >/dev/null",
		"printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"thread-worker\"}'",
		"printf '%s\\n' '{\"type\":\"item.completed\",\"item\":{\"id\":\"item-1\",\"type\":\"agent_message\",\"text\":" + shellSingleQuoteForBeaconCLITestJSON(final) + "}}'",
		"printf '%s\\n' '{\"type\":\"turn.completed\"}'",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write codex fixture: %v", err)
	}
	return path
}

func waitForBeaconMachineByProviderJob(t *testing.T, store *beacon.Store, providerJobID string) beacon.Machine {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err := store.Load()
		if err != nil {
			t.Fatalf("load beacon state while waiting for machine: %v", err)
		}
		for _, machine := range st.Machines {
			if machine.ProviderJobID == providerJobID {
				return machine
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for beacon machine provider job %s", providerJobID)
	return beacon.Machine{}
}

func shellSingleQuoteForBeaconCLITestJSON(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`
}

func shellSingleQuoteForBeaconCLITest(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func runBeaconRootCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}
