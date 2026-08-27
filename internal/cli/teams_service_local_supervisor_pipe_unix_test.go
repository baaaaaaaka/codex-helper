//go:build linux

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const supervisorPipeHolderEnv = "CXP_TEST_SUPERVISOR_PIPE_HOLDER"
const supervisorPipeHolderChildPIDFileEnv = supervisorPipeHolderEnv + "_CHILD_PID_FILE"

func TestTeamsServiceLocalSupervisorPipeHolder(t *testing.T) {
	if os.Getenv(supervisorPipeHolderEnv) != "1" {
		t.Skip("helper subprocess only")
	}
	pidPath := strings.TrimSpace(os.Getenv(supervisorPipeHolderEnv + "_PID_FILE"))
	if pidPath == "" {
		t.Fatal("pipe-holder PID file is not configured")
	}
	childPIDPath := strings.TrimSpace(os.Getenv(supervisorPipeHolderChildPIDFileEnv))
	if childPIDPath == "" {
		t.Fatal("supervisor child PID file is not configured")
	}
	if err := os.WriteFile(childPIDPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatalf("write supervisor child PID: %v", err)
	}
	leaf := exec.Command("/bin/sh", "-c", `exec /bin/sleep 30`)
	leaf.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	leaf.Stdout = os.Stdout
	leaf.Stderr = os.Stderr
	if err := leaf.Start(); err != nil {
		t.Fatalf("start separate pipe-holder group: %v", err)
	}
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(leaf.Process.Pid)), 0o600); err != nil {
		_ = syscall.Kill(-leaf.Process.Pid, syscall.SIGKILL)
		_ = leaf.Process.Kill()
		t.Fatalf("write pipe-holder PID: %v", err)
	}
	select {}
}

func TestTeamsServiceLocalSupervisorChildHealthRestartWithSeparatePipeHolder(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	pidPath := filepath.Join(tmp, "pipe-holder.pid")
	wrapperPath := filepath.Join(tmp, "fake-cxp-wrapper")
	wrapper := fmt.Sprintf("#!/bin/sh\nexec %s -test.run='^TestTeamsServiceLocalSupervisorPipeHolder$'\n", shellQuoteForSupervisorTest(os.Args[0]))
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0o700); err != nil {
		t.Fatalf("write supervisor child wrapper: %v", err)
	}
	logPath := filepath.Join(tmp, "supervisor.log")
	childPIDPath := filepath.Join(tmp, "supervisor-child.pid")
	logWriter, err := openTeamsServiceLocalSupervisorLogWriter(logPath)
	if err != nil {
		t.Fatalf("open supervisor log writer: %v", err)
	}
	childDone := make(chan error, 1)
	childStarted := make(chan struct{})
	childFinished := make(chan struct{})
	status := teamsServiceLocalSupervisorStatus{Version: teamsServiceLocalSupervisorStatusVersion, ConfigPath: filepath.Join(tmp, "local-supervisor.json"), SupervisorPID: os.Getpid(), SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID(), UpdatedAt: time.Now()}
	prevHeartbeat := teamsServiceLocalSupervisorHeartbeatEvery
	prevTerminationWait := teamsServiceLocalSupervisorTerminationWait
	prevHealth := teamsServiceLocalSupervisorCheckChildHealth
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	healthReady := make(chan struct{})
	healthCalled := make(chan struct{}, 1)
	teamsServiceLocalSupervisorHeartbeatEvery = 10 * time.Millisecond
	teamsServiceLocalSupervisorTerminationWait = 100 * time.Millisecond
	teamsServiceLocalSupervisorCheckChildHealth = func(context.Context, *teamsServiceWatchdogState) (teamsServiceWatchdogDecision, error) {
		select {
		case <-healthReady:
			select {
			case healthCalled <- struct{}{}:
			default:
			}
			return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionRestart, Reason: "test stale child with separate pipe holder"}, nil
		default:
			return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "waiting for separate pipe holder"}, nil
		}
	}
	t.Cleanup(func() {
		select {
		case <-childStarted:
			cancel()
			select {
			case <-childFinished:
				killSupervisorTestProcessGroup(pidPath)
			case <-time.After(100 * time.Millisecond):
				killSupervisorTestProcessGroup(pidPath)
				killSupervisorTestPID(supervisorTestPIDFromFile(childPIDPath))
				select {
				case <-childFinished:
				case <-time.After(time.Second):
					t.Errorf("supervisor child did not exit after cleanup killed its pipe holder")
				}
			}
		default:
		}
		teamsServiceLocalSupervisorHeartbeatEvery = prevHeartbeat
		teamsServiceLocalSupervisorTerminationWait = prevTerminationWait
		teamsServiceLocalSupervisorCheckChildHealth = prevHealth
		if err := logWriter.Close(); err != nil {
			t.Errorf("close supervisor log writer: %v", err)
		}
	})
	childConfig := teamsServiceLocalSupervisorConfig{Version: teamsServiceLocalSupervisorConfigVersion, Spec: teamsServiceSpec{Executable: wrapperPath, WorkingDir: tmp, Environment: map[string]string{supervisorPipeHolderEnv: "1", supervisorPipeHolderEnv + "_PID_FILE": pidPath, supervisorPipeHolderChildPIDFileEnv: childPIDPath}}}
	close(childStarted)
	go func() {
		defer close(childFinished)
		childDone <- runTeamsServiceLocalSupervisorChild(ctx, childConfig, &status, logWriter)
	}()
	deadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	var holderPID int
	func() {
		defer deadline.Stop()
		defer ticker.Stop()
		for holderPID == 0 {
			if data, readErr := os.ReadFile(pidPath); readErr == nil {
				if parsed, parseErr := strconv.Atoi(strings.TrimSpace(string(data))); parseErr == nil && parsed > 0 {
					holderPID = parsed
					break
				}
			}
			select {
			case <-ticker.C:
			case <-deadline.C:
				return
			}
		}
	}()
	if holderPID == 0 {
		t.Fatal("supervisor child did not create the separate pipe holder")
	}
	holderPGID, err := syscall.Getpgid(holderPID)
	if err != nil {
		t.Fatalf("get separate pipe-holder process group: %v", err)
	}
	if holderPGID != holderPID || holderPGID == syscall.Getpgrp() {
		t.Fatalf("pipe-holder process group = %d for pid=%d, want its own group distinct from test group %d", holderPGID, holderPID, syscall.Getpgrp())
	}
	close(healthReady)
	select {
	case <-healthCalled:
	case <-ctx.Done():
		t.Fatal("supervisor child did not run the health check")
	}
	select {
	case err = <-childDone:
	case <-time.After(2 * time.Second):
		cancel()
		killSupervisorTestProcessGroup(pidPath)
		killSupervisorTestPID(supervisorTestPIDFromFile(childPIDPath))
		t.Fatal("supervisor child did not finish bounded health restart")
	}
	if err == nil || !strings.Contains(err.Error(), "health check restarted child") {
		t.Fatalf("run child err = %v, want bounded health restart despite inherited pipe", err)
	}
	if status.ChildPID != 0 || status.ChildPGID != 0 || status.State != "waiting" {
		t.Fatalf("status after health restart = %#v, want cleared waiting child", status)
	}
	if status.LastHealthReason != "test stale child with separate pipe holder" || status.LastHealthAction != teamsServiceWatchdogActionRestart {
		t.Fatalf("health status = reason %q action %q", status.LastHealthReason, status.LastHealthAction)
	}
}

// Exercise the actual local-supervisor child loop rather than only testing a
// watchdog decision.  During startup the replacement child is already alive,
// but the previous owner's stale row can remain until listener initialization
// records the new owner.  A stale row must not make the supervisor terminate
// that replacement child before it gets a chance to initialize.
func TestTeamsServiceLocalSupervisorDoesNotRestartChildBeforeOwnerRegistration(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	statePath := filepath.Join(tmp, "canonical", "state.json")
	if err := os.MkdirAll(filepath.Dir(statePath), 0o700); err != nil {
		t.Fatalf("mkdir watchdog state dir: %v", err)
	}
	if err := os.WriteFile(statePath, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write watchdog state placeholder: %v", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname: %v", err)
	}
	now := time.Now()
	previousOwner := teamsstore.OwnerMetadata{
		PID:             os.Getpid(),
		Hostname:        hostname,
		ScopeID:         "scope-supervisor-startup",
		LeaseGeneration: 19,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeat:   now.Add(-10 * time.Minute),
	}
	watchdogState := teamsstore.State{
		Scope:        teamsstore.ScopeIdentity{ID: previousOwner.ScopeID},
		ServiceOwner: &previousOwner,
		LockOwner:    &previousOwner,
	}

	fake := filepath.Join(tmp, "fake-cxp")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatalf("write fake child: %v", err)
	}

	status := teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     filepath.Join(tmp, "local-supervisor.json"),
		SupervisorPID:  os.Getpid(),
		SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID(),
		UpdatedAt:      now,
	}
	prevHeartbeat := teamsServiceLocalSupervisorHeartbeatEvery
	prevTerminationWait := teamsServiceLocalSupervisorTerminationWait
	prevHealth := teamsServiceLocalSupervisorCheckChildHealth
	prevStorePaths := teamsServiceWatchdogStorePaths
	prevLoadState := teamsServiceWatchdogLoadState
	prevInstalled := teamsServiceWatchdogInstalled
	prevActive := teamsServiceWatchdogActive
	prevManagedChild := teamsServiceWatchdogManagedChild
	managedChildCalls := 0
	t.Cleanup(func() {
		teamsServiceLocalSupervisorHeartbeatEvery = prevHeartbeat
		teamsServiceLocalSupervisorTerminationWait = prevTerminationWait
		teamsServiceLocalSupervisorCheckChildHealth = prevHealth
		teamsServiceWatchdogStorePaths = prevStorePaths
		teamsServiceWatchdogLoadState = prevLoadState
		teamsServiceWatchdogInstalled = prevInstalled
		teamsServiceWatchdogActive = prevActive
		teamsServiceWatchdogManagedChild = prevManagedChild
	})
	teamsServiceLocalSupervisorHeartbeatEvery = 10 * time.Millisecond
	teamsServiceLocalSupervisorTerminationWait = 100 * time.Millisecond
	teamsServiceWatchdogStorePaths = func() ([]string, error) {
		return []string{statePath}, nil
	}
	teamsServiceWatchdogLoadState = func(context.Context, string) (teamsstore.State, error) {
		return watchdogState, nil
	}
	teamsServiceWatchdogInstalled = func() (bool, error) { return true, nil }
	teamsServiceWatchdogActive = func(context.Context) (bool, error) { return true, nil }
	teamsServiceWatchdogManagedChild = func() (teamsServiceWatchdogManagedChildIdentity, bool) {
		managedChildCalls++
		if status.ChildPID <= 0 || status.LastChildStartAt.IsZero() {
			return teamsServiceWatchdogManagedChildIdentity{}, false
		}
		return teamsServiceWatchdogManagedChildIdentity{
			PID:       status.ChildPID,
			StartedAt: status.LastChildStartAt,
		}, true
	}
	teamsServiceLocalSupervisorCheckChildHealth = defaultTeamsServiceLocalSupervisorCheckChildHealth

	logPath := filepath.Join(tmp, "supervisor.log")
	logWriter, err := openTeamsServiceLocalSupervisorLogWriter(logPath)
	if err != nil {
		t.Fatalf("open supervisor log writer: %v", err)
	}
	defer logWriter.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err = runTeamsServiceLocalSupervisorChild(ctx, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: fake,
			WorkingDir: tmp,
		},
	}, &status, logWriter)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("supervisor child ended with %v, want context deadline without watchdog restart (managed-child calls=%d)", err, managedChildCalls)
	}
	if status.ChildPID != 0 || status.ChildPGID != 0 || status.State != "waiting" {
		t.Fatalf("status after context cleanup = %#v, want cleared waiting child", status)
	}
}

func TestDefaultTeamsLocalSupervisorTerminateProcessGroupToleratesGoneLeader(t *testing.T) {
	prevProcessGroupID := teamsLocalSupervisorProcessGroupID
	prevProcessGroupAlive := teamsLocalSupervisorProcessGroupAlive
	t.Cleanup(func() {
		teamsLocalSupervisorProcessGroupID = prevProcessGroupID
		teamsLocalSupervisorProcessGroupAlive = prevProcessGroupAlive
	})

	teamsLocalSupervisorProcessGroupID = func(int) (int, error) {
		return 0, syscall.ESRCH
	}
	teamsLocalSupervisorProcessGroupAlive = func(int) bool {
		return false
	}

	if err := defaultTeamsLocalSupervisorTerminateProcessGroup(12345, 67890, 0); err != nil {
		t.Fatalf("terminate process group with gone leader: %v", err)
	}
}

func TestDefaultTeamsLocalSupervisorTerminateProcessGroupRejectsGoneLeaderWithLiveGroup(t *testing.T) {
	prevProcessGroupID := teamsLocalSupervisorProcessGroupID
	prevProcessGroupAlive := teamsLocalSupervisorProcessGroupAlive
	t.Cleanup(func() {
		teamsLocalSupervisorProcessGroupID = prevProcessGroupID
		teamsLocalSupervisorProcessGroupAlive = prevProcessGroupAlive
	})

	teamsLocalSupervisorProcessGroupID = func(int) (int, error) {
		return 0, syscall.ESRCH
	}
	teamsLocalSupervisorProcessGroupAlive = func(int) bool {
		return true
	}

	err := defaultTeamsLocalSupervisorTerminateProcessGroup(12345, 67890, 0)
	if err == nil || !strings.Contains(err.Error(), "leader pid 67890 disappeared") {
		t.Fatalf("terminate process group with live group: %v, want fail-closed leader race error", err)
	}
}

func shellQuoteForSupervisorTest(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
func killSupervisorTestProcessGroup(pidPath string) {
	killSupervisorTestPID(supervisorTestPIDFromFile(pidPath))
}
func supervisorTestPIDFromFile(pidPath string) int {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}
func killSupervisorTestPID(pid int) {
	if pid <= 0 {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

var _ io.Writer = (*teamsServiceLocalSupervisorLogWriter)(nil)
