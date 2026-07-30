//go:build linux

package teams

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const (
	runtimeSafetyTakeoverWriterHelperEnv  = "CXP_TEAMS_TAKEOVER_WRITER_HELPER"
	runtimeSafetyTakeoverWriterReadyEnv   = "CXP_TEAMS_TAKEOVER_WRITER_READY"
	runtimeSafetyTakeoverRealProcessCIEnv = "CXP_TEAMS_TAKEOVER_REAL_PROCESS_DOCKER"
)

func TestTeamsRuntimeSafetyTakeoverWriterProcessHelper(t *testing.T) {
	if os.Getenv(runtimeSafetyTakeoverWriterHelperEnv) != "1" {
		t.Skip("real-process helper")
	}
	readyPath := os.Getenv(runtimeSafetyTakeoverWriterReadyEnv)
	if readyPath == "" {
		t.Fatal("writer ready path is required")
	}
	if err := os.WriteFile(readyPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatalf("write writer readiness: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
}

func TestTeamsRuntimeSafetyAutomaticTakeoverRealWriterProcessDockerCI(t *testing.T) {
	if os.Getenv(runtimeSafetyTakeoverRealProcessCIEnv) != "1" {
		t.Skip("runs only in the ephemeral Docker process-fencing shard")
	}
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	readyPath := filepath.Join(t.TempDir(), "writer-ready")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestTeamsRuntimeSafetyTakeoverWriterProcessHelper$", "-test.count=1")
	cmd.Env = append(
		os.Environ(),
		runtimeSafetyTakeoverWriterHelperEnv+"=1",
		runtimeSafetyTakeoverWriterReadyEnv+"="+readyPath,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start real legacy writer process: %v", err)
	}
	waited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(waited)
	}()
	t.Cleanup(func() {
		select {
		case <-waited:
		default:
			_ = cmd.Process.Kill()
			<-waited
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat writer readiness: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("real legacy writer process did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	result, err := exerciseRuntimeSafetyTakeoverContract(
		fixture.Scope,
		runtimeSafetyTakeoverContractOptions{
			WriterIdentity: "verified-local-managed-writer",
			WriterPID:      cmd.Process.Pid,
			PIDVisibility:  "visible",
		},
		fixture.LegacyPath,
	)
	if err != nil {
		t.Fatalf("automatic takeover with a real fenced writer: %v", err)
	}
	if result.CanonicalPath != fixture.CanonicalPath {
		t.Fatalf("takeover path = %q, want canonical %q", result.CanonicalPath, fixture.CanonicalPath)
	}
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("takeover returned while the real legacy writer process was still alive")
	}
	if _, err := os.Stat(fixture.LegacyPath); !os.IsNotExist(err) {
		t.Fatalf("takeover left legacy source after fencing the real writer: %v", err)
	}
}
