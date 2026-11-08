package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func TestTeamsServiceWatchdogRefreshesRuntimeBeforeHealthCheck(t *testing.T) {
	lockCLITestHooks(t)
	previousVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = previousVersion })
	for _, tc := range []struct {
		name, active           string
		dry, unmanaged, legacy bool
		wantExec, wantCheck    bool
	}{
		{name: "same version", active: "v1.2.3", wantCheck: true},
		{name: "upgrade", active: "v1.2.4", wantExec: true},
		{name: "rollback", active: "v1.2.2", wantExec: true},
		{name: "legacy launcher same version", active: "v1.2.3", legacy: true, wantCheck: true},
		{name: "legacy launcher upgrade", active: "v1.2.4", legacy: true, wantExec: true},
		{name: "legacy launcher rollback", active: "v1.2.2", legacy: true, wantExec: true},
		{name: "dry run", active: "v1.2.4", dry: true, wantCheck: true},
		{name: "unmanaged", unmanaged: true, wantCheck: true},
		{name: "missing pointer"},
		{name: "invalid pointer", active: "not a version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			entry := filepath.Join(dir, "cxp")
			t.Setenv(helperruntime.EnvRuntime, "1")
			t.Setenv(helperruntime.EnvRuntimeRoot, dir)
			t.Setenv(helperruntime.EnvRuntimeVersion, "v1.2.3")
			if tc.legacy {
				t.Setenv(helperruntime.EnvRuntimeVersion, "")
			}
			t.Setenv(helperruntime.EnvEntryPath, entry)
			if tc.unmanaged {
				t.Setenv(helperruntime.EnvRuntime, "")
			}
			if tc.active != "" {
				writeFile(t, filepath.Join(dir, "active"), tc.active)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			prevExec, prevStart := execSelf, startSelf
			prevCollect, prevPath := teamsServiceWatchdogCollectSnapshot, teamsServiceWatchdogStatePath
			t.Cleanup(func() {
				execSelf, startSelf = prevExec, prevStart
				teamsServiceWatchdogCollectSnapshot, teamsServiceWatchdogStatePath = prevCollect, prevPath
			})
			calls, checks := 0, 0
			stop := errors.New("test exec boundary")
			replace := func(exe string, args, env []string) error {
				calls++
				if exe != entry {
					t.Fatalf("restart executable %q, want stable entry %q", exe, entry)
				}
				for _, v := range env {
					if strings.HasPrefix(v, "CXP_RUNTIME") || strings.HasPrefix(v, "CXP_ENTRY_PATH=") {
						t.Fatalf("runtime marker leaked: %s", v)
					}
				}
				return stop
			}
			execSelf, startSelf = replace, replace
			teamsServiceWatchdogStatePath = func() (string, error) { return filepath.Join(dir, "watchdog.json"), nil }
			teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
				checks++
				cancel()
				return teamsServiceWatchdogSnapshot{}, nil
			}
			_, err := runTeamsServiceWatchdogIteration(ctx, teamsServiceWatchdogOptions{DryRun: tc.dry}, true, io.Discard)
			if (calls == 1) != tc.wantExec || (checks == 1) != tc.wantCheck {
				t.Fatalf("execs=%d checks=%d err=%v", calls, checks, err)
			}
			if tc.wantExec && !errors.Is(err, stop) {
				t.Fatalf("exec failure swallowed: %v", err)
			}
			if !tc.wantExec && !tc.wantCheck && err == nil {
				t.Fatal("runtime error ignored")
			}
		})
	}
}

// A failed refresh must skip health collection and retry on the next loop tick.
// Repair the pointer only when the loop reports its first refresh error.
func TestTeamsServiceWatchdogRuntimeRefreshErrorsDoNotStopLoop(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			for _, invalid := range []string{"", "not a version"} {
				t.Run("pointer="+invalid, func(t *testing.T) {
					testTeamsServiceWatchdogRuntimeRefreshRecovery(t, func(root string) func() {
						if legacy {
							t.Setenv(helperruntime.EnvRuntimeVersion, "")
						}
						if invalid == "" {
							if err := os.Remove(filepath.Join(root, "active")); err != nil {
								t.Fatal(err)
							}
						} else {
							writeFile(t, filepath.Join(root, "active"), invalid)
						}
						return func() { writeFile(t, filepath.Join(root, "active"), "v1.2.3") }
					})
				})
			}
		})
	}
}

type watchdogErrorWriter func([]byte) (int, error)

func (w watchdogErrorWriter) Write(p []byte) (int, error) { return w(p) }

func testTeamsServiceWatchdogRuntimeRefreshRecovery(t *testing.T, unavailable func(string) func()) {
	t.Helper()
	lockCLITestHooks(t)
	root := t.TempDir()
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v1.2.3")
	t.Setenv(helperruntime.EnvEntryPath, filepath.Join(root, "cxp"))
	writeFile(t, filepath.Join(root, "active"), "v1.2.3")
	restore := unavailable(root)
	var restored sync.Once
	recoverPointer := func() { restored.Do(restore) }
	t.Cleanup(recoverPointer)
	prevVersion := version
	prevCollect, prevPath := teamsServiceWatchdogCollectSnapshot, teamsServiceWatchdogStatePath
	t.Cleanup(func() {
		version = prevVersion
		teamsServiceWatchdogCollectSnapshot, teamsServiceWatchdogStatePath = prevCollect, prevPath
	})
	version = "v1.2.3"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	checks, logs := 0, 0
	teamsServiceWatchdogStatePath = func() (string, error) { return filepath.Join(root, "watchdog.json"), nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		checks++
		if logs != 1 {
			t.Fatalf("health collected before runtime read recovered: logs=%d", logs)
		}
		cancel()
		return teamsServiceWatchdogSnapshot{}, nil
	}
	errOut := watchdogErrorWriter(func(p []byte) (int, error) {
		logs++
		if !strings.Contains(string(p), "read watchdog runtime:") {
			t.Fatalf("unexpected refresh error: %s", p)
		}
		if checks != 0 {
			t.Fatal("health collected while runtime pointer was unreadable")
		}
		recoverPointer()
		return len(p), nil
	})
	err := runTeamsServiceWatchdogLoop(ctx, teamsServiceWatchdogOptions{}, time.Millisecond, true, io.Discard, errOut)
	if !errors.Is(err, context.Canceled) || checks != 1 || logs != 1 {
		t.Fatalf("refresh recovery: checks=%d logs=%d err=%v", checks, logs, err)
	}
}

func TestTeamsServiceWatchdogRuntimeRestartErrorsDoNotStopLoop(t *testing.T) {
	lockCLITestHooks(t)
	root := t.TempDir()
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v1.2.3")
	t.Setenv(helperruntime.EnvEntryPath, filepath.Join(root, "cxp"))
	writeFile(t, filepath.Join(root, "active"), "v1.2.4")
	prevVersion, prevExec, prevStart := version, execSelf, startSelf
	prevCollect := teamsServiceWatchdogCollectSnapshot
	t.Cleanup(func() {
		version, execSelf, startSelf = prevVersion, prevExec, prevStart
		teamsServiceWatchdogCollectSnapshot = prevCollect
	})
	version = "v1.2.3"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	calls := 0
	restartErr := errors.New("injected restart failure")
	restart := func(string, []string, []string) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return restartErr
	}
	execSelf, startSelf = restart, restart
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		t.Fatal("old runtime checked health after failed restart")
		return teamsServiceWatchdogSnapshot{}, nil
	}
	var errOut strings.Builder
	err := runTeamsServiceWatchdogLoop(ctx, teamsServiceWatchdogOptions{}, time.Millisecond, true, io.Discard, &errOut)
	if !errors.Is(err, context.Canceled) || calls != 2 || strings.Count(errOut.String(), restartErr.Error()) != 2 {
		t.Fatalf("restart retry: calls=%d err=%v logs=%q", calls, err, errOut.String())
	}
}
