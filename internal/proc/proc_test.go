//go:build !windows

package proc

import (
	"bufio"
	"os"
	"os/exec"
	"strconv"
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
	cmd := exec.Command("sh", "-c", "sh -c 'exit 0' & echo $!; sleep 30")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start parent: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read child pid: %v", err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("parse child pid %q: %v", line, err)
	}
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
