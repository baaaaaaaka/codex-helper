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

	"github.com/gofrs/flock"
)

const (
	runtimeSafetyTakeoverWriterHelperEnv  = "CXP_TEAMS_TAKEOVER_WRITER_HELPER"
	runtimeSafetyTakeoverWriterReadyEnv   = "CXP_TEAMS_TAKEOVER_WRITER_READY"
	runtimeSafetyTakeoverWriterLockEnv    = "CXP_TEAMS_TAKEOVER_WRITER_LOCK"
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
	lockPath := os.Getenv(runtimeSafetyTakeoverWriterLockEnv)
	if lockPath == "" {
		t.Fatal("writer lock path is required")
	}
	lock := flock.New(lockPath)
	if err := lock.Lock(); err != nil {
		t.Fatalf("hold writer lock: %v", err)
	}
	defer func() { _ = lock.Unlock() }()
	if err := os.WriteFile(readyPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatalf("write writer readiness: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
}

func TestTeamsRuntimeSafetyOfflineTakeoverWaitsForRealWriterExitDockerCI(t *testing.T) {
	if os.Getenv(runtimeSafetyTakeoverRealProcessCIEnv) != "1" {
		t.Skip("runs only in the ephemeral Docker offline-takeover shard")
	}
	fixture := seedRuntimeSafetyTakeoverFixture(t)
	readyPath := filepath.Join(t.TempDir(), "writer-ready")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestTeamsRuntimeSafetyTakeoverWriterProcessHelper$", "-test.count=1")
	cmd.Env = append(
		os.Environ(),
		runtimeSafetyTakeoverWriterHelperEnv+"=1",
		runtimeSafetyTakeoverWriterReadyEnv+"="+readyPath,
		runtimeSafetyTakeoverWriterLockEnv+"="+fixture.LegacyPath+".lock",
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

	before := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if err := CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath); err == nil {
		t.Fatal("offline takeover succeeded while the real writer still held the store lock")
	}
	after := snapshotRuntimeSafetyFiles(t, fixture.Root)
	if changes := runtimeSafetySnapshotChanges(before, after); len(changes) != 0 {
		t.Fatalf("blocked offline takeover modified files: %v", changes)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("stop real legacy writer process: %v", err)
	}
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("real legacy writer process did not exit")
	}
	if err := CompleteOfflineRuntimeStoreTakeover(context.Background(), fixture.Scope, fixture.LegacyPath); err != nil {
		t.Fatalf("offline takeover after service writer exit: %v", err)
	}
	if _, err := os.Stat(fixture.LegacyPath); !os.IsNotExist(err) {
		t.Fatalf("offline takeover left legacy source after writer exit: %v", err)
	}
}
