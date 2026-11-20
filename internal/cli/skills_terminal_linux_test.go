package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/skills"
	"github.com/baaaaaaaka/codex-helper/internal/tui"
)

func TestDailySkillSyncLeavesTUIKeyboardAvailable(t *testing.T) {
	if root := os.Getenv("CXP_TEST_SKILL_SYNC_TERMINAL"); root != "" {
		testDailySkillSyncFromTerminal(t, root)
		return
	}
	requireCLIGit(t)
	root := t.TempDir()
	ssh := filepath.Join(root, "ssh")
	ready := filepath.Join(root, "ssh-started")
	writeCLIFile(t, ssh, `#!/bin/sh
if ( : <> /dev/tty ) 2>/dev/null; then
    echo 'SSH can open the TUI terminal' >&2
else
    printf '%s\n' 'Host key verification failed.' >&2
fi
touch "$CXP_TEST_SKILL_SSH_READY"
exit 255
`, 0o700)

	master, slave := openLinuxPTY(t)
	defer master.Close()
	defer slave.Close()
	if err := unix.IoctlSetWinsize(int(slave.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: 24, Col: 100}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDailySkillSyncLeavesTUIKeyboardAvailable$")
	cmd.Env = append(os.Environ(),
		"CXP_TEST_SKILL_SYNC_TERMINAL="+root, "CXP_TEST_SKILL_SSH_READY="+ready,
		"HOME="+root, "XDG_CACHE_HOME="+root, "TERM=xterm",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_SSH_COMMAND="+shellQuote(ssh), "GIT_SSH_VARIANT=ssh",
	)
	cmd.Stdin = slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = slave.Close()
	terminalDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, master)
		close(terminalDone)
	}()
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(ready); err == nil {
				_, _ = master.WriteString("q")
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	err := cmd.Wait()
	_ = master.Close()
	<-terminalDone
	if err != nil {
		_, readyErr := os.Stat(ready)
		t.Fatalf("TUI did not exit cleanly after SSH failure and q: %v (SSH marker: %v)\n%s",
			err, readyErr, output.String())
	}
}

func testDailySkillSyncFromTerminal(t *testing.T, root string) {
	paths := effectivePaths{Home: root, CodexDir: filepath.Join(root, "codex"), ConfigPath: filepath.Join(root, "config.json")}
	store, err := skills.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source := skills.Source{
		ID: "remote", Name: "remote", RemoteURL: "ssh://git@example.test:12051/repo.git",
		Ref: "main", AutoSync: true, TargetKind: skills.TargetAgents, TargetRoot: filepath.Join(root, "skills"),
	}
	if err := store.PutSubscription(source, skills.SourceState{ID: source.ID, Status: skills.StatusReady}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var syncDone <-chan struct{}
	selection, err := tui.SelectSession(ctx, tui.Options{
		LoadProjects: func(context.Context) ([]codexhistory.Project, error) { return nil, nil },
		OnReady: func() error {
			syncDone = startSkillsDailyAutoSync(ctx, paths)
			return nil
		},
	})
	if err != nil || selection != nil {
		t.Fatalf("q should exit the TUI: selection=%+v, error=%v", selection, err)
	}
	select {
	case <-syncDone:
	case <-ctx.Done():
		t.Fatal("skill sync did not finish")
	}
	state, ok, err := store.SourceState(source.ID)
	if err != nil || !ok || state.Status != skills.StatusSyncFailed || !strings.Contains(state.LastError, "Host key verification failed.") {
		t.Fatalf("expected saved SSH verification error: state=%+v, found=%t, error=%v", state, ok, err)
	}
}
