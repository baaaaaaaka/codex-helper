//go:build linux

package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

func TestProxyStartBackgroundReapsExitedDetachedChild(t *testing.T) {
	if os.Getenv("CODEX_PROXY_DETACHED_CHILD") == "1" {
		os.Exit(0)
	}
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:   "p1",
			Name: "dev",
			Host: "host",
			Port: 22,
			User: "alice",
		}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevExecutable := proxyExecutable
	prevCommand := proxyCommand
	t.Cleanup(func() {
		proxyExecutable = prevExecutable
		proxyCommand = prevCommand
	})
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	proxyExecutable = func() (string, error) { return exe, nil }
	proxyCommand = func(string, ...string) *exec.Cmd {
		cmd := exec.Command(exe, "-test.run=^TestProxyStartBackgroundReapsExitedDetachedChild$")
		cmd.Env = append(os.Environ(), "CODEX_PROXY_DETACHED_CHILD=1")
		return cmd
	}

	cmd := newProxyStartCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 1 || cfg.Instances[0].DaemonPID <= 0 {
		t.Fatalf("unexpected detached instance: %+v", cfg.Instances)
	}

	pid := cfg.Instances[0].DaemonPID
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, exists, err := linuxProcessState(pid)
		if err != nil {
			t.Fatalf("inspect detached child pid %d: %v", pid, err)
		}
		if !exists {
			return
		}
		if state == 'Z' {
			t.Fatalf("detached proxy child pid %d became a zombie", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("detached proxy child pid %d did not exit", pid)
}

func TestStartCodexAppProxyDaemonReapsExitedDetachedChild(t *testing.T) {
	if os.Getenv("CODEX_PROXY_DETACHED_CHILD") == "1" {
		os.Exit(0)
	}
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevExecutable := proxyExecutable
	prevCommand := proxyCommand
	t.Cleanup(func() {
		proxyExecutable = prevExecutable
		proxyCommand = prevCommand
	})
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	proxyExecutable = func() (string, error) { return exe, nil }
	proxyCommand = func(string, ...string) *exec.Cmd {
		cmd := exec.Command(exe, "-test.run=^TestStartCodexAppProxyDaemonReapsExitedDetachedChild$")
		cmd.Env = append(os.Environ(), "CODEX_PROXY_DETACHED_CHILD=1")
		return cmd
	}

	if _, err := startCodexAppProxyDaemon(context.Background(), store, config.Profile{ID: "p1", Name: "dev"}); err != nil {
		t.Fatalf("start app proxy daemon: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 1 || cfg.Instances[0].DaemonPID <= 0 {
		t.Fatalf("unexpected app detached instance: %+v", cfg.Instances)
	}

	pid := cfg.Instances[0].DaemonPID
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, exists, err := linuxProcessState(pid)
		if err != nil {
			t.Fatalf("inspect app detached child pid %d: %v", pid, err)
		}
		if !exists {
			return
		}
		if state == 'Z' {
			t.Fatalf("app proxy child pid %d became a zombie", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("app proxy child pid %d did not exit", pid)
}

func linuxProcessState(pid int) (byte, bool, error) {
	if pid <= 0 {
		return 0, false, errors.New("invalid pid")
	}
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	closeParen := strings.LastIndexByte(string(data), ')')
	if closeParen < 0 || closeParen+2 >= len(data) {
		return 0, true, errors.New("malformed /proc stat")
	}
	fields := strings.Fields(string(data[closeParen+1:]))
	if len(fields) == 0 || len(fields[0]) != 1 {
		return 0, true, errors.New("missing process state in /proc stat")
	}
	return fields[0][0], true, nil
}
