package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestBeaconProfileCLIWorkflow(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "beacon.json")
	configPath := filepath.Join(t.TempDir(), "config.json")

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
	)
	if err != nil {
		t.Fatalf("profile create: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Created draft beacon profile") || !strings.Contains(out, "doctor failed") || !strings.Contains(out, "needs confirm") {
		t.Fatalf("create output should show draft reasons:\n%s", out)
	}

	out, err = runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "doctor", "gpu")
	if err != nil {
		t.Fatalf("profile doctor: %v\n%s", err, out)
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
	if !strings.Contains(out, "profile=gpu") || !strings.Contains(out, "status=ready") {
		t.Fatalf("status output = %s", out)
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
	if _, err := runBeaconRootCommand(t, "--config", configPath, "beacon", "--store", storePath, "profile", "create", "lsf", "--provider", "lsf", "--queue", "o_pri_interactive"); err != nil {
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
	if !strings.Contains(out, "gpu\tslurm\tready") {
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
	if !strings.Contains(out, "confirm=KILL-lease-1") || !strings.Contains(out, "jobs=job-1") {
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
