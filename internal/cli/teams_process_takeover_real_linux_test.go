//go:build linux

package cli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	teamsTakeoverRealProcessDockerEnv = "CXP_TEAMS_TAKEOVER_REAL_PROCESS_DOCKER"
	teamsTakeoverWriterHelperEnv      = "CXP_TEAMS_CLI_TAKEOVER_WRITER_HELPER"
	teamsTakeoverWriterReadyEnv       = "CXP_TEAMS_CLI_TAKEOVER_WRITER_READY"
)

func TestTeamsServiceTakeoverWriterProcessHelper(t *testing.T) {
	if os.Getenv(teamsTakeoverWriterHelperEnv) != "1" {
		t.Skip("real-process takeover helper")
	}
	readyPath := os.Getenv(teamsTakeoverWriterReadyEnv)
	if readyPath == "" {
		t.Fatal("writer ready path is required")
	}
	if err := os.WriteFile(readyPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatalf("write writer readiness: %v", err)
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestTeamsRuntimeSafetyExactLegacyWriterFenceRealProcessDockerCI(t *testing.T) {
	if os.Getenv(teamsTakeoverRealProcessDockerEnv) != "1" {
		t.Skip("runs only in the ephemeral Docker process-fencing shard")
	}
	tmp := t.TempDir()
	cxpPath := filepath.Join(tmp, "cxp")
	if err := os.Symlink(os.Args[0], cxpPath); err != nil {
		t.Fatalf("create stable-entry symlink: %v", err)
	}
	startWriter := func(name string) (*exec.Cmd, <-chan struct{}) {
		t.Helper()
		readyPath := filepath.Join(tmp, name+".ready")
		cmd := &exec.Cmd{
			Path: os.Args[0],
			Args: []string{
				cxpPath,
				"-test.run=^TestTeamsServiceTakeoverWriterProcessHelper$",
				"cxp",
				"teams",
				"run",
			},
			Env: append(
				os.Environ(),
				teamsTakeoverWriterHelperEnv+"=1",
				teamsTakeoverWriterReadyEnv+"="+readyPath,
			),
		}
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		t.Cleanup(func() {
			select {
			case <-done:
			default:
				_ = cmd.Process.Kill()
				<-done
			}
		})
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(readyPath); err == nil {
				break
			} else if !os.IsNotExist(err) {
				t.Fatalf("stat %s readiness: %v", name, err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s did not become ready", name)
			}
			time.Sleep(10 * time.Millisecond)
		}
		return cmd, done
	}

	target, targetDone := startWriter("target")
	unrelated, unrelatedDone := startWriter("unrelated")
	env := teamsServiceCurrentProcessScopeEnvForTest()
	spec := teamsServiceSpec{Executable: cxpPath, Environment: env}
	now := time.Now()
	state := store.State{ServiceOwner: &store.OwnerMetadata{
		PID:            target.Process.Pid,
		ExecutablePath: cxpPath,
		StartedAt:      now,
		LastHeartbeat:  now,
	}}
	fence, err := teamsServiceValidateLegacyStoreWriters(context.Background(), state, spec)
	if err != nil {
		t.Fatalf("validate real writer: %v", err)
	}
	if len(fence.Writers) != 1 || fence.Writers[0].PID != target.Process.Pid || fence.Writers[0].ProcessStartTime == "" {
		t.Fatalf("real writer fence = %#v", fence)
	}
	if err := teamsServiceFenceLegacyStoreWriters(context.Background(), fence, spec); err != nil {
		t.Fatalf("fence real writer: %v", err)
	}
	select {
	case <-targetDone:
	case <-time.After(5 * time.Second):
		t.Fatal("validated writer remained alive after exact fencing")
	}
	if err := unrelated.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unrelated matching writer was stopped: %v", err)
	}
	if err := unrelated.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("stop unrelated helper: %v", err)
	}
	select {
	case <-unrelatedDone:
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated helper did not stop during cleanup")
	}
}
