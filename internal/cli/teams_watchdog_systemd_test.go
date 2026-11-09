//go:build linux

package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

// Called only as the MainPID of the private units in the hosted-runner gate.
// Ordinary package tests cannot operate the development host's services.
func TestLegacyTeamsWatchdogSystemdChild(t *testing.T) {
	root := os.Getenv("CXP_SYSTEMD_WATCHDOG_TEST_ROOT")
	if root == "" {
		return
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("RUNNER_ENVIRONMENT") != "github-hosted" {
		t.Fatal("systemd fixture requires an ephemeral GitHub-hosted runner")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := handoffLegacyTeamsWatchdog(ctx, root, "v9.9.1", os.Getpid(), teamsServiceRunSystemctl, os.Readlink, helperruntime.ReadActive); err != nil {
		t.Fatal(err)
	}
	// The predicate may safely skip, so independently require an actual
	// replacement and verify that the main test process survived it.
	out, err := teamsServiceRunSystemctl(ctx, "show", "--property=MainPID", "--value", teamsServiceWatchdogUnitName)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil || exe != filepath.Join(root, "versions", "v9.9.1", "cxp") {
		t.Fatalf("watchdog executable: %q %v", exe, err)
	}
	if err := os.WriteFile(filepath.Join(root, "main-survived"), []byte("ok\n"), 0600); err != nil {
		t.Fatal(err)
	}
}
