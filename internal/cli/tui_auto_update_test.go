package cli

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/tui"
	"github.com/spf13/cobra"
)

func TestTUIBootClassificationMatchesReservedCommands(t *testing.T) {
	cmd := newRootCmd()
	for _, sub := range cmd.Commands() {
		if sub.Name() == "tui" {
			continue
		}
		for _, name := range append([]string{sub.Name()}, sub.Aliases...) {
			if helperruntime.TUIInvocation([]string{name}) {
				t.Errorf("reserved command %q consumes a TUI trial", name)
			}
		}
	}
}

func TestTUIAutoUpdateLoopStopsAndBoundsEachCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	calls := make(chan struct{}, 2)
	go func() {
		defer close(done)
		runTUIAutoUpdate(ctx, helperruntime.Context{}, 0, time.Millisecond, func(checkCtx context.Context, _ helperruntime.Context) error {
			if deadline, ok := checkCtx.Deadline(); !ok || time.Until(deadline) > 3*time.Minute {
				t.Error("unbounded update request")
			}
			calls <- struct{}{}
			<-checkCtx.Done()
			return checkCtx.Err()
		})
	}()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("background checker never ran")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("checker survived its owning invocation")
	}
}

func TestTUIAutoUpdateContextOutlivesHistorySelector(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	t.Setenv("CODEX_DIR", filepath.Join(home, "codex"))
	t.Setenv("CXP_AUTO_UPDATE", "0")
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	effectivePathsRunningAsRoot = func() bool { return false }
	oldProxy, oldSelect, oldRun, oldStart := ensureProxyPreferenceFunc, selectSession, runCodexNewSessionFn, startTUIAutoUpdateFn
	t.Cleanup(func() {
		ensureProxyPreferenceFunc, selectSession, runCodexNewSessionFn, startTUIAutoUpdateFn = oldProxy, oldSelect, oldRun, oldStart
	})
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion}, nil
	}
	var owner context.Context
	starts := 0
	startTUIAutoUpdateFn = func(ctx context.Context) { owner = ctx; starts++ }
	selectSession = func(_ context.Context, opts tui.Options) (*tui.Selection, error) {
		if opts.CheckUpdate != nil {
			t.Fatal("opt-out still installed the legacy network checker")
		}
		if starts != 0 {
			t.Fatal("update started before first frame")
		}
		if err := opts.OnReady(); err != nil {
			return nil, err
		}
		return &tui.Selection{Cwd: home}, nil
	}
	runCodexNewSessionFn = func(ctx context.Context, _ *rootOptions, _ *config.Store, _ *config.Profile, _ []config.Instance, _ string, _ string, _ string, _ bool, _ io.Writer) error {
		if owner == nil || owner != ctx || owner.Err() != nil {
			t.Fatal("updater ended with the history selector")
		}
		return nil
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: filepath.Join(home, "config.json")}, "", filepath.Join(home, "codex"), "", 0); err != nil {
		t.Fatal(err)
	}
	if starts != 1 || owner.Err() == nil {
		t.Fatal("updater did not follow the outer invocation lifetime")
	}
}

func TestTUIBootRollbackConfigReaderContract(t *testing.T) {
	if helperruntime.BootProtocol == 1 && config.MinReaderVersion != 1 {
		t.Fatal("a breaking config reader floor requires a new boot protocol and an explicit bootstrap install")
	}
}
