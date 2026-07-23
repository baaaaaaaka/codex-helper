package cli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

const detachedContractChildEnv = "CODEX_HELPER_DETACHED_CONTRACT_CHILD"

func TestProxyStartBackgroundConfiguresDetachedCommand(t *testing.T) {
	if os.Getenv(detachedContractChildEnv) == "proxy" {
		os.Exit(0)
	}
	lockCLITestHooks(t)
	configureDetachedContractStablePathForWindows(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID: "p1", Name: "dev", Host: "host", Port: 22, User: "alice",
		}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	prevExecutable, prevCommand := proxyExecutable, proxyCommand
	t.Cleanup(func() {
		proxyExecutable = prevExecutable
		proxyCommand = prevCommand
	})
	proxyExecutable = func() (string, error) { return exe, nil }
	var launched *exec.Cmd
	proxyCommand = func(_ string, _ ...string) *exec.Cmd {
		launched = exec.Command(exe, "-test.run=^TestProxyStartBackgroundConfiguresDetachedCommand$")
		launched.Env = append(os.Environ(), detachedContractChildEnv+"=proxy")
		return launched
	}

	cmd := newProxyStartCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute proxy start: %v", err)
	}
	assertDetachedCommandConfigured(t, launched)
}

func TestAppProxyLaunchConfiguresDetachedCommand(t *testing.T) {
	if os.Getenv(detachedContractChildEnv) == "app" {
		os.Exit(0)
	}
	lockCLITestHooks(t)
	configureDetachedContractStablePathForWindows(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	prevExecutable, prevCommand := proxyExecutable, proxyCommand
	t.Cleanup(func() {
		proxyExecutable = prevExecutable
		proxyCommand = prevCommand
	})
	proxyExecutable = func() (string, error) { return exe, nil }
	var launched *exec.Cmd
	proxyCommand = func(_ string, _ ...string) *exec.Cmd {
		launched = exec.Command(exe, "-test.run=^TestAppProxyLaunchConfiguresDetachedCommand$")
		launched.Env = append(os.Environ(), detachedContractChildEnv+"=app")
		return launched
	}
	if _, err := startCodexAppProxyDaemon(context.Background(), store, config.Profile{ID: "p1", Name: "dev"}); err != nil {
		t.Fatalf("start app proxy daemon: %v", err)
	}
	assertDetachedCommandConfigured(t, launched)
}

func TestModelAdapterLaunchConfiguresDetachedCommand(t *testing.T) {
	if os.Getenv(detachedContractChildEnv) == "adapter" {
		os.Exit(0)
	}
	lockCLITestHooks(t)
	configureDetachedContractStablePathForWindows(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	prevExecutable, prevCommand := proxyExecutable, proxyCommand
	t.Cleanup(func() {
		proxyExecutable = prevExecutable
		proxyCommand = prevCommand
	})
	proxyExecutable = func() (string, error) { return exe, nil }
	var launched *exec.Cmd
	proxyCommand = func(_ string, _ ...string) *exec.Cmd {
		launched = exec.Command(exe, "-test.run=^TestModelAdapterLaunchConfiguresDetachedCommand$")
		launched.Env = append(os.Environ(), detachedContractChildEnv+"=adapter")
		return launched
	}
	resolved := modelprofile.Resolved{
		Name:     "adapter",
		Profile:  config.ModelProfile{Provider: "test", Model: "test-model", Revision: 1},
		Provider: modelprofile.ProviderSpec{ID: "test", DisplayName: "Test", DefaultModel: "test-model", UsesAdapter: true},
		Model:    modelprofile.ModelSpec{ID: "test-model"},
	}
	if _, err := startModelProfileAdapterDaemon(context.Background(), store, resolved, "adapter-profile", "", nil, false); err != nil {
		t.Fatalf("start model adapter daemon: %v", err)
	}
	assertDetachedCommandConfigured(t, launched)
}

func configureDetachedContractStablePathForWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "windows" {
		return
	}
	path := filepath.Join(t.TempDir(), helperpath.BinaryName(runtime.GOOS))
	if err := os.WriteFile(path, []byte("test helper"), 0o755); err != nil {
		t.Fatalf("write stable helper path: %v", err)
	}
	restartArgv0 = func() string { return path }
}

func TestProxyStartForegroundDoesNotLaunchDetachedCommand(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID: "p1", Name: "dev", Host: "host", Port: 22, User: "alice",
		}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	prevRunDaemon, prevCommand := runProxyDaemonOwnedFunc, proxyCommand
	t.Cleanup(func() {
		runProxyDaemonOwnedFunc = prevRunDaemon
		proxyCommand = prevCommand
	})
	runProxyDaemonOwnedFunc = func(context.Context, *config.Store, string, string) error { return nil }
	called := false
	proxyCommand = func(string, ...string) *exec.Cmd {
		called = true
		return exec.Command("false")
	}

	cmd := newProxyStartCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--foreground"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute foreground proxy start: %v", err)
	}
	if called {
		t.Fatal("foreground proxy start unexpectedly launched a detached child")
	}
}
