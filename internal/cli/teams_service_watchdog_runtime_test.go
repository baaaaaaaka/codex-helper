package cli

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
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
			err := runTeamsServiceWatchdogLoop(ctx, teamsServiceWatchdogOptions{DryRun: tc.dry}, time.Millisecond, true, io.Discard, io.Discard)
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
