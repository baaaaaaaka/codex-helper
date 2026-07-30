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
	teamsTakeoverSupervisorHelperEnv  = "CXP_TEAMS_TAKEOVER_SUPERVISOR_HELPER"
	teamsTakeoverSupervisorConfigEnv  = "CXP_TEAMS_TAKEOVER_SUPERVISOR_CONFIG"
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

func TestTeamsServiceTakeoverSupervisorProcessHelper(t *testing.T) {
	if os.Getenv(teamsTakeoverSupervisorHelperEnv) != "1" {
		t.Skip("real-process supervisor helper")
	}
	configPath := os.Getenv(teamsTakeoverSupervisorConfigEnv)
	if configPath == "" {
		t.Fatal("supervisor config path is required")
	}
	if err := runTeamsServiceLocalSupervisor(context.Background(), configPath); err != nil {
		t.Fatalf("run takeover supervisor helper: %v", err)
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

func TestTeamsRuntimeSafetySupervisorFencePreventsChildRestartRealProcessDockerCI(t *testing.T) {
	if os.Getenv(teamsTakeoverRealProcessDockerEnv) != "1" {
		t.Skip("runs only in the ephemeral Docker process-fencing shard")
	}
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	cxpPath := filepath.Join(tmp, "cxp")
	if err := os.Symlink(os.Args[0], cxpPath); err != nil {
		t.Fatalf("create supervisor stable-entry symlink: %v", err)
	}
	childReady := filepath.Join(tmp, "supervisor-child.ready")
	specEnv := teamsServiceCurrentProcessScopeEnvForTest()
	specEnv["CXP_TEAMS_TAKEOVER_SUPERVISOR_CHILD_HELPER"] = "1"
	specEnv["CXP_TEAMS_TAKEOVER_SUPERVISOR_CHILD_READY"] = childReady
	spec := teamsServiceSpec{
		Executable:   cxpPath,
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "registry.json"),
		Environment:  specEnv,
	}
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("supervisor config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    spec,
	}); err != nil {
		t.Fatalf("write supervisor config: %v", err)
	}

	supervisor := &exec.Cmd{
		Path: os.Args[0],
		Args: []string{
			cxpPath,
			"-test.run=^TestTeamsServiceTakeoverSupervisorProcessHelper$",
			"-test.count=1",
			"teams", "service", "local-supervisor", "--config", configPath,
		},
		Env: append(
			os.Environ(),
			"CXP_TEAMS_TEST_PRESERVE_USER_DIRS=1",
			teamsTakeoverSupervisorHelperEnv+"=1",
			teamsTakeoverSupervisorConfigEnv+"="+configPath,
		),
	}
	configureTargetProcessGroup(supervisor)
	if err := supervisor.Start(); err != nil {
		t.Fatalf("start real supervisor: %v", err)
	}
	supervisorDone := make(chan struct{})
	go func() {
		_ = supervisor.Wait()
		close(supervisorDone)
	}()
	t.Cleanup(func() {
		select {
		case <-supervisorDone:
		default:
			_ = supervisor.Process.Kill()
			<-supervisorDone
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	var status teamsServiceLocalSupervisorStatus
	for {
		if _, err := os.Stat(childReady); err == nil {
			if current, ok, readErr := readTeamsServiceLocalSupervisorStatus(); readErr == nil && ok && current.ChildPID > 0 {
				status = current
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("real supervisor child did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	initialChildPID := status.ChildPID
	now := time.Now()
	state := store.State{ServiceOwner: &store.OwnerMetadata{
		PID:            initialChildPID,
		ExecutablePath: cxpPath,
		StartedAt:      now,
		LastHeartbeat:  now,
	}}
	fence, err := teamsServiceValidateLegacyStoreWriters(context.Background(), state, spec)
	if err != nil {
		t.Fatalf("validate supervisor-owned writer: %v", err)
	}
	if err := teamsServiceFenceLegacyStoreWriters(context.Background(), fence, spec); err != nil {
		t.Fatalf("fence owning supervisor: %v", err)
	}
	select {
	case <-supervisorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("owning supervisor remained alive after fencing")
	}
	time.Sleep(2 * time.Second)
	if current, ok, err := readTeamsServiceLocalSupervisorStatus(); err != nil {
		t.Fatalf("read stopped supervisor status: %v", err)
	} else if !ok || current.SupervisorPID != 0 || current.ChildPID != 0 || current.State != "stopped" {
		t.Fatalf("stopped supervisor status = %#v", current)
	}
	if err := syscall.Kill(initialChildPID, 0); err == nil {
		t.Fatalf("legacy child pid %d remained alive or was restarted", initialChildPID)
	}
}
