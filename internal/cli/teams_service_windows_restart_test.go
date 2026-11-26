package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
)

type teamsWindowsRestartTestBackend struct {
	teamsServiceWindowsTaskBackend
	restart func() error
	active  bool
}

func (b teamsWindowsRestartTestBackend) Run(_ context.Context, _ string) ([]byte, error) {
	return nil, b.restart()
}

func (b teamsWindowsRestartTestBackend) Active(context.Context) (bool, error) {
	return b.active, nil
}

func TestTeamsServiceWindowsRestartHandoffContract(t *testing.T) {
	ctx := context.Background()
	host, err := teamsHostname()
	if err != nil {
		t.Fatal(err)
	}
	boot := teamsBootID()
	acquire := func(t *testing.T, anchor, host string) *lease.Holder {
		t.Helper()
		holder, err := lease.Acquire(ctx, lease.Config{Anchor: anchor, Self: lease.Local(host, boot)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = holder.Release() })
		if err := holder.WaitQuiet(ctx); err != nil {
			t.Fatal(err)
		}
		if err := holder.Activate(); err != nil {
			t.Fatal(err)
		}
		return holder
	}
	t.Run("new generation may own the bridge before its first poll", func(t *testing.T) {
		dir := t.TempDir()
		anchor := filepath.Join(dir, "core.sqlite")
		first := acquire(t, anchor, host)
		if err := writeTeamsPollStamp(dir, teamsPollStamp{Host: host, PID: os.Getpid(), Generation: first.Gen(), At: time.Now().UnixMilli()}); err != nil {
			t.Fatal(err)
		}
		// Release before the command so this process does not need to exit.
		if err := first.Release(); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		backend := teamsWindowsRestartTestBackend{active: true, restart: func() error { acquire(t, anchor, host); return nil }}
		if err := restartTeamsWindowsTask(ctx, &out, anchor, host, boot, time.Second, 0, backend); err != nil || !strings.Contains(out.String(), "successful Teams poll is pending") {
			t.Fatalf("restart: output=%q error=%v", out.String(), err)
		}
	})
	t.Run("remote owner leaves the local task in standby", func(t *testing.T) {
		anchor := filepath.Join(t.TempDir(), "core.sqlite")
		acquire(t, anchor, "remote-teams-restart")
		var out bytes.Buffer
		backend := teamsWindowsRestartTestBackend{active: true, restart: func() error { return nil }}
		if err := restartTeamsWindowsTask(ctx, &out, anchor, host, boot, time.Second, 0, backend); err != nil || !strings.Contains(out.String(), "task started in standby") {
			t.Fatalf("restart: output=%q error=%v", out.String(), err)
		}
	})
	t.Run("scheduler start without a bridge is not success", func(t *testing.T) {
		anchor := filepath.Join(t.TempDir(), "core.sqlite")
		for _, active := range []bool{false, true} {
			backend := teamsWindowsRestartTestBackend{active: active, restart: func() error { return nil }}
			err := restartTeamsWindowsTask(ctx, io.Discard, anchor, host, boot, 0, 0, backend)
			want := "exited before"
			if active {
				want = "no new bridge acquired"
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("active=%t, restart error=%v, want %q", active, err, want)
			}
		}
	})
}

func TestTeamsServiceWindowsRestartUsesInstalledState(t *testing.T) {
	dir := t.TempDir()
	installed := filepath.Join(dir, "it's the service state", "core.sqlite")
	t.Setenv(envTeamsCoreStore, filepath.Join(dir, "user-selected.sqlite"))
	script := buildTeamsServiceWindowsPowerShell(teamsServiceSpec{Environment: map[string]string{envTeamsCoreStore: installed}}, []string{"teams", "run"})
	launcher := writeProbeScript(t, dir, "service.ps1", script)
	got, err := teamsWindowsRestartAnchor(launcher)
	if err != nil || got != installed {
		t.Fatalf("installed launcher resolved to %q, %v; want %q", got, err, installed)
	}
	got, err = teamsWindowsRestartAnchor(filepath.Join(dir, "legacy-direct-exec.ps1"))
	if err != nil || got != filepath.Join(dir, "user-selected.sqlite") {
		t.Fatalf("legacy direct task resolved to %q, %v", got, err)
	}
}
