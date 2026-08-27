//go:build linux

package cli

import (
	"context"
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
