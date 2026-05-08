//go:build !windows

package proc

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestIsAlive(t *testing.T) {
	if IsAlive(0) {
		t.Fatalf("expected pid 0 to be dead")
	}
	if IsAlive(-1) {
		t.Fatalf("expected negative pid to be dead")
	}
	if !IsAlive(os.Getpid()) {
		t.Fatalf("expected current pid to be alive")
	}
}

func TestIsAliveAfterProcessExit(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if IsAlive(pid) {
		t.Fatalf("expected exited process to be dead")
	}
}

func TestIsAliveTreatsLinuxZombieAsDead(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("requires Linux procfs")
	}
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	childPID := cmd.Process.Pid
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if isLinuxZombie(childPID) {
			if IsAlive(childPID) {
				t.Fatalf("expected zombie pid %d to be treated as dead", childPID)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Skip("child did not become a visible zombie before timeout")
}

func TestLinuxProcStateFromStatParsesLastParen(t *testing.T) {
	tests := []struct {
		name   string
		stat   string
		want   byte
		wantOK bool
	}{
		{
			name:   "simple running process",
			stat:   "123 (sh) S 1 2 3",
			want:   'S',
			wantOK: true,
		},
		{
			name:   "zombie process",
			stat:   "124 (sleep) Z 1 2 3",
			want:   'Z',
			wantOK: true,
		},
		{
			name:   "process name contains right paren",
			stat:   "125 (name with ) inside) R 1 2 3",
			want:   'R',
			wantOK: true,
		},
		{
			name:   "missing comm terminator",
			stat:   "126 name S 1 2 3",
			wantOK: false,
		},
		{
			name:   "truncated after comm",
			stat:   "127 (sh) ",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := linuxProcStateFromStat(tt.stat)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("linuxProcStateFromStat(%q) = (%q, %v), want (%q, %v)", tt.stat, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestCommandLine(t *testing.T) {
	t.Run("invalid pid", func(t *testing.T) {
		if _, err := CommandLine(0); err == nil {
			t.Fatal("expected invalid pid error")
		}
	})

	t.Run("current process", func(t *testing.T) {
		cmdline, err := CommandLine(os.Getpid())
		if err != nil {
			t.Fatalf("CommandLine(current pid): %v", err)
		}
		if strings.TrimSpace(cmdline) == "" {
			t.Fatal("expected non-empty command line")
		}
	})
}
