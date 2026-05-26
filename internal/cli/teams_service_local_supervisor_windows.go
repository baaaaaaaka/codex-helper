//go:build windows

package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"time"
)

func configureTeamsLocalSupervisorDetachedCommand(cmd *exec.Cmd) {
	configureTeamsServiceDetachedCommand(cmd)
}

func teamsLocalSupervisorNotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt)
}

var teamsLocalSupervisorCurrentProcessGroupID = defaultTeamsLocalSupervisorCurrentProcessGroupID

func defaultTeamsLocalSupervisorCurrentProcessGroupID() int {
	return os.Getpid()
}

var teamsLocalSupervisorProcessGroupID = defaultTeamsLocalSupervisorProcessGroupID

func defaultTeamsLocalSupervisorProcessGroupID(pid int) (int, error) {
	return pid, nil
}

var teamsLocalSupervisorProcessAlive = defaultTeamsLocalSupervisorProcessAlive

func defaultTeamsLocalSupervisorProcessAlive(pid int) bool {
	return pid > 0
}

var teamsLocalSupervisorTerminateProcessGroup = defaultTeamsLocalSupervisorTerminateProcessGroup

func defaultTeamsLocalSupervisorTerminateProcessGroup(_ int, _ int, _ time.Duration) error {
	return fmt.Errorf("local supervisor process groups are not supported on Windows")
}

func teamsLocalSupervisorVerifiedProcessGroupID(pid int, statusPGID int) (int, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("local supervisor pid is required to verify process group")
	}
	if statusPGID > 0 && statusPGID != pid {
		return 0, fmt.Errorf("refusing to use stale local supervisor process group %d for pid %d", statusPGID, pid)
	}
	return pid, nil
}

func defaultTeamsLocalSupervisorVerifyProcessIdentity(_ int, _ string) error {
	return nil
}

func defaultTeamsLocalSupervisorVerifyChildIdentity(_ int, _ teamsServiceSpec) error {
	return nil
}

func defaultTeamsLocalSupervisorProcessArgs(_ int) ([]string, error) {
	return nil, nil
}

func defaultTeamsLocalSupervisorProcessEnvironment(_ int) (map[string]string, error) {
	return nil, nil
}

func defaultTeamsLocalSupervisorProcessStartTime(_ int) (string, error) {
	return "", nil
}

func teamsLocalSupervisorExecutableMatches(_ int, _ string, _ []string) error {
	return nil
}

func validateTeamsServiceLocalSupervisorDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("local supervisor directory must not be a symlink: %s", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("local supervisor path is not a directory: %s", path)
	}
	return nil
}

func validateTeamsServiceLocalSupervisorExistingDir(path string) error {
	return validateTeamsServiceLocalSupervisorDir(path)
}
