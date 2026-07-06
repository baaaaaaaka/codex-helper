package cli

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/update"
	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

type cliTestUserPathResolverFunc func(context.Context, userpath.Request) (userpath.Result, error)

func (f cliTestUserPathResolverFunc) Resolve(ctx context.Context, request userpath.Request) (userpath.Result, error) {
	return f(ctx, request)
}

var cliTestHookGuard sync.Mutex

// lockCLITestHooks serializes tests that mutate package- or process-global
// state such as seam function variables, stack starters, or os.Stdin.
func lockCLITestHooks(t testing.TB) {
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
	prevTeamsUserPathResolver := teamsUserPathResolver
	type envValue struct {
		value string
		set   bool
	}
	prevEnv := map[string]envValue{}
	for _, key := range []string{
		update.EnvInstallPath,
		update.EnvInstallDir,
		update.EnvRepo,
		update.EnvVersion,
	} {
		value, set := os.LookupEnv(key)
		prevEnv[key] = envValue{value: value, set: set}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
	resolveInstallPathForCLI = func(path string) (string, error) {
		if path != "" {
			return update.ResolveInstallPath(path)
		}
		return installPath, nil
	}
	restartArgv0 = func() string { return installPath }
	teamsServiceArgv0 = func() string { return installPath }
	teamsUpdatePendingHelperActivationOwned = func(string, string) bool { return true }
	teamsUserPathResolver = cliTestUserPathResolverFunc(func(_ context.Context, request userpath.Request) (userpath.Result, error) {
		pathValue, _ := userpath.EnvironmentValue(request.ServiceEnvironment, "PATH", false)
		return userpath.Result{Path: pathValue, Mode: userpath.ModeService, Source: "test-service-environment"}, nil
	})
	t.Cleanup(func() {
		resolveInstallPathForCLI = prevResolveInstallPathForCLI
		restartArgv0 = prevRestartArgv0
		teamsServiceArgv0 = prevTeamsServiceArgv0
		teamsUpdatePendingHelperActivationOwned = prevTeamsUpdatePendingHelperActivationOwned
		teamsUserPathResolver = prevTeamsUserPathResolver
		for _, key := range []string{
			update.EnvInstallPath,
			update.EnvInstallDir,
			update.EnvRepo,
			update.EnvVersion,
		} {
			prev := prevEnv[key]
			if prev.set {
				_ = os.Setenv(key, prev.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
		cliTestHookGuard.Unlock()
	})
}
