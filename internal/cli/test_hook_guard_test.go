package cli

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/update"
)

var cliTestHookGuard sync.Mutex

// lockCLITestHooks serializes tests that mutate package- or process-global
// state such as seam function variables, stack starters, or os.Stdin.
func lockCLITestHooks(t *testing.T) {
	t.Helper()
	cliTestHookGuard.Lock()
	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy")
	if err := os.WriteFile(installPath, []byte("test helper"), 0o755); err != nil {
		t.Fatalf("write test helper install path: %v", err)
	}
	prevResolveInstallPathForCLI := resolveInstallPathForCLI
	prevRestartArgv0 := restartArgv0
	prevTeamsServiceArgv0 := teamsServiceArgv0
	prevTeamsUpdatePendingHelperActivationOwned := teamsUpdatePendingHelperActivationOwned
	resolveInstallPathForCLI = func(path string) (string, error) {
		if path != "" {
			return update.ResolveInstallPath(path)
		}
		return installPath, nil
	}
	restartArgv0 = func() string { return installPath }
	teamsServiceArgv0 = func() string { return installPath }
	teamsUpdatePendingHelperActivationOwned = func(string, string) bool { return true }
	t.Cleanup(func() {
		resolveInstallPathForCLI = prevResolveInstallPathForCLI
		restartArgv0 = prevRestartArgv0
		teamsServiceArgv0 = prevTeamsServiceArgv0
		teamsUpdatePendingHelperActivationOwned = prevTeamsUpdatePendingHelperActivationOwned
		cliTestHookGuard.Unlock()
	})
}
