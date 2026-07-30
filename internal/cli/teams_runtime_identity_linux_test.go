//go:build linux

package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

const teamsRuntimeIdentityHelperEnv = "CXP_TEST_TEAMS_RUNTIME_IDENTITY_HELPER"

func TestTeamsRuntimeSafetyManagedRuntimeHelperProcess(t *testing.T) {
	if os.Getenv(teamsRuntimeIdentityHelperEnv) != "1" {
		return
	}
	time.Sleep(30 * time.Second)
}

func TestTeamsRuntimeSafetyStableEntryCanManageActivatedImmutableRuntimeCI(t *testing.T) {
	cmd, stable, _ := startTeamsRuntimeSafetyManagedRuntimeFixture(t)

	identity := &teamsServiceLocalSupervisorProcessIdentity{Executable: stable}
	if err := teamsServiceLocalSupervisorVerifyRecordedIdentity(cmd.Process.Pid, identity, "local supervisor child"); err != nil {
		t.Fatalf("stable managed entry must recognize its activated immutable runtime: %v", err)
	}
}

func TestTeamsRuntimeSafetyManagedRuntimeLineageStillRejectsReusedPIDStartTimeCI(t *testing.T) {
	cmd, _, runtimePath := startTeamsRuntimeSafetyManagedRuntimeFixture(t)
	identity := &teamsServiceLocalSupervisorProcessIdentity{
		Executable:    runtimePath,
		ProcStartTime: "definitely-not-the-live-process-start-time",
	}
	if err := teamsServiceLocalSupervisorVerifyRecordedIdentity(cmd.Process.Pid, identity, "local supervisor child"); err == nil ||
		!strings.Contains(err.Error(), "start time changed") {
		t.Fatalf("managed-runtime lineage must not bypass PID-reuse protection; error = %v", err)
	}
}

func startTeamsRuntimeSafetyManagedRuntimeFixture(t *testing.T) (*exec.Cmd, string, string) {
	t.Helper()
	tmp := t.TempDir()
	testBinary, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatalf("helperpath.RawExecutable: %v", err)
	}
	raw, err := os.ReadFile(testBinary)
	if err != nil {
		t.Fatalf("read test binary: %v", err)
	}
	stable := filepath.Join(tmp, ".local", "bin", "cxp")
	runtimePath := filepath.Join(tmp, ".local", "bin", ".cxp-runtime", "versions", "v1.2.4", "cxp")
	for _, path := range []string{stable, runtimePath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, raw, 0o755); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	cmd := exec.Command(runtimePath, "-test.run=^TestTeamsRuntimeSafetyManagedRuntimeHelperProcess$")
	cmd.Env = append(os.Environ(), teamsRuntimeIdentityHelperEnv+"=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start immutable runtime fixture: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	time.Sleep(50 * time.Millisecond)
	return cmd, stable, runtimePath
}
