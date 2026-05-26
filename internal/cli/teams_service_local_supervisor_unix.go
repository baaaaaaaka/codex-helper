//go:build !windows

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func configureTeamsLocalSupervisorDetachedCommand(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	attr := cmd.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	attr.Setsid = true
	cmd.SysProcAttr = attr
}

func teamsLocalSupervisorNotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

var teamsLocalSupervisorCurrentProcessGroupID = defaultTeamsLocalSupervisorCurrentProcessGroupID

func defaultTeamsLocalSupervisorCurrentProcessGroupID() int {
	return syscall.Getpgrp()
}

var teamsLocalSupervisorProcessGroupID = defaultTeamsLocalSupervisorProcessGroupID

func defaultTeamsLocalSupervisorProcessGroupID(pid int) (int, error) {
	return syscall.Getpgid(pid)
}

var teamsLocalSupervisorProcessAlive = defaultTeamsLocalSupervisorProcessAlive

func defaultTeamsLocalSupervisorProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

var teamsLocalSupervisorTerminateProcessGroup = defaultTeamsLocalSupervisorTerminateProcessGroup

func defaultTeamsLocalSupervisorTerminateProcessGroup(pgid int, leaderPID int, grace time.Duration) error {
	if pgid <= 0 {
		return nil
	}
	if leaderPID > 0 {
		livePGID, err := teamsLocalSupervisorProcessGroupID(leaderPID)
		if err != nil {
			return err
		}
		if livePGID != pgid {
			return fmt.Errorf("refusing to terminate process group %d for pid %d because live process group is %d", pgid, leaderPID, livePGID)
		}
	}
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	if waitTeamsLocalSupervisorProcessGroupGone(pgid, grace) {
		return nil
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	if waitTeamsLocalSupervisorProcessGroupGone(pgid, time.Second) {
		return nil
	}
	if leaderPID > 0 && !teamsLocalSupervisorProcessAlive(leaderPID) && !teamsLocalSupervisorProcessGroupAlive(pgid) {
		return nil
	}
	return errors.New("local supervisor process group is still alive after termination")
}

func waitTeamsLocalSupervisorProcessGroupGone(pgid int, grace time.Duration) bool {
	if pgid <= 0 {
		return true
	}
	if !teamsLocalSupervisorProcessGroupAlive(pgid) {
		return true
	}
	deadline := time.Now().Add(grace)
	for grace > 0 && time.Now().Before(deadline) {
		if !teamsLocalSupervisorProcessGroupAlive(pgid) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return !teamsLocalSupervisorProcessGroupAlive(pgid)
}

func teamsLocalSupervisorProcessGroupAlive(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func defaultTeamsLocalSupervisorVerifyProcessIdentity(pid int, configPath string) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	args, err := readProcNULFileFields(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("local supervisor pid %d is no longer running", pid)
		}
		return fmt.Errorf("could not verify local supervisor pid %d identity: %w", pid, err)
	}
	if teamsLocalSupervisorArgsMatch(args, configPath) {
		return nil
	}
	return fmt.Errorf("refusing to manage pid %d because it does not look like the local supervisor for %s: %s", pid, configPath, strings.Join(args, " "))
}

func defaultTeamsLocalSupervisorVerifyChildIdentity(pid int, spec teamsServiceSpec) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	args, err := readProcNULFileFields(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("local supervisor child pid %d is no longer running", pid)
		}
		return fmt.Errorf("could not verify local supervisor child pid %d identity: %w", pid, err)
	}
	envFields, _ := readProcNULFileFields(filepath.Join("/proc", strconv.Itoa(pid), "environ"))
	env := envMapFromFields(envFields)
	if env["CODEX_HELPER_TEAMS_SERVICE"] != "1" {
		return fmt.Errorf("refusing to manage pid %d because it is missing CODEX_HELPER_TEAMS_SERVICE=1", pid)
	}
	if err := teamsLocalSupervisorEnvironmentMatches(pid, env, teamsServiceLocalSupervisorIdentityEnvironment(spec.Environment)); err != nil {
		return err
	}
	if !teamsLocalSupervisorChildArgsMatch(args, spec) {
		return fmt.Errorf("refusing to manage pid %d because it does not look like the local supervisor child for registry %s: %s", pid, spec.RegistryPath, strings.Join(args, " "))
	}
	if err := teamsLocalSupervisorExecutableMatches(pid, spec.Executable, args); err != nil {
		return err
	}
	return nil
}

func teamsLocalSupervisorEnvironmentMatches(pid int, got map[string]string, want map[string]string) error {
	for _, key := range sortedEnvironmentKeys(want) {
		if got[key] != want[key] {
			return fmt.Errorf("refusing to manage pid %d because environment %s does not match expected service identity", pid, key)
		}
	}
	return nil
}

func defaultTeamsLocalSupervisorProcessArgs(pid int) ([]string, error) {
	if runtime.GOOS != "linux" {
		return nil, nil
	}
	return readProcNULFileFields(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
}

func defaultTeamsLocalSupervisorProcessEnvironment(pid int) (map[string]string, error) {
	if runtime.GOOS != "linux" {
		return nil, nil
	}
	fields, err := readProcNULFileFields(filepath.Join("/proc", strconv.Itoa(pid), "environ"))
	if err != nil {
		return nil, err
	}
	return envMapFromFields(fields), nil
}

func defaultTeamsLocalSupervisorProcessStartTime(pid int) (string, error) {
	if runtime.GOOS != "linux" {
		return "", nil
	}
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", err
	}
	text := string(data)
	end := strings.LastIndex(text, ")")
	if end < 0 || end+2 >= len(text) {
		return "", fmt.Errorf("malformed proc stat for pid %d", pid)
	}
	fields := strings.Fields(text[end+2:])
	if len(fields) < 20 {
		return "", fmt.Errorf("proc stat for pid %d has %d fields after comm, want at least 20", pid, len(fields))
	}
	return fields[19], nil
}

func teamsLocalSupervisorArgsMatch(args []string, configPath string) bool {
	for i := 0; i+2 < len(args); i++ {
		if args[i] != "teams" || args[i+1] != "service" || args[i+2] != "local-supervisor" {
			continue
		}
		return teamsLocalSupervisorArgsConfigMatches(args[i+3:], configPath)
	}
	return false
}

func teamsLocalSupervisorVerifiedProcessGroupID(pid int, statusPGID int) (int, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("local supervisor pid is required to verify process group")
	}
	livePGID, err := teamsLocalSupervisorProcessGroupID(pid)
	if err != nil {
		return 0, fmt.Errorf("could not resolve process group for pid %d: %w", pid, err)
	}
	if livePGID <= 0 {
		return 0, fmt.Errorf("local supervisor process group is unknown for pid %d", pid)
	}
	if statusPGID > 0 && statusPGID != livePGID {
		return 0, fmt.Errorf("refusing to use stale local supervisor process group %d for pid %d because live process group is %d", statusPGID, pid, livePGID)
	}
	return livePGID, nil
}

func teamsLocalSupervisorChildArgsMatch(args []string, spec teamsServiceSpec) bool {
	want := buildTeamsServiceRunArgs(spec)
	for i := 0; i+len(want) <= len(args); i++ {
		matched := true
		for j := range want {
			if args[i+j] != want[j] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func teamsLocalSupervisorExecutableMatches(pid int, executable string, args []string) error {
	want := strings.TrimSpace(executable)
	if want == "" {
		return fmt.Errorf("local supervisor child executable is empty")
	}
	for _, arg := range args {
		if teamsLocalSupervisorSameExecutable(arg, want) {
			return nil
		}
	}
	got, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "exe"))
	if err != nil {
		return fmt.Errorf("could not verify local supervisor child pid %d executable: %w", pid, err)
	}
	if teamsLocalSupervisorSameExecutable(got, want) {
		return nil
	}
	return fmt.Errorf("refusing to manage pid %d because executable %s does not match expected %s", pid, got, want)
}

func teamsLocalSupervisorSameExecutable(a string, b string) bool {
	a = filepath.Clean(strings.TrimSpace(a))
	b = filepath.Clean(strings.TrimSpace(b))
	a = strings.TrimSuffix(a, " (deleted)")
	b = strings.TrimSuffix(b, " (deleted)")
	if a == b {
		return true
	}
	if resolvedA, err := filepath.EvalSymlinks(a); err == nil {
		a = filepath.Clean(resolvedA)
	}
	if resolvedB, err := filepath.EvalSymlinks(b); err == nil {
		b = filepath.Clean(resolvedB)
	}
	return a == b
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
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("local supervisor directory %s is owned by uid %d, want %d", path, stat.Uid, os.Geteuid())
	}
	if info.Mode().Perm()&0o077 != 0 {
		if err := os.Chmod(path, info.Mode().Perm()&0o700); err != nil {
			return fmt.Errorf("tighten local supervisor directory permissions for %s: %w", path, err)
		}
	}
	return nil
}

func validateTeamsServiceLocalSupervisorExistingDir(path string) error {
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
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("local supervisor directory %s is owned by uid %d, want %d", path, stat.Uid, os.Geteuid())
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("local supervisor directory %s permissions %o are too broad", path, info.Mode().Perm())
	}
	return nil
}

func teamsLocalSupervisorArgsConfigMatches(args []string, configPath string) bool {
	want := teamsServiceCleanRegistryPath(configPath)
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		if arg == "--config" && i+1 < len(args) {
			return teamsServiceCleanRegistryPath(args[i+1]) == want
		}
		if strings.HasPrefix(arg, "--config=") {
			return teamsServiceCleanRegistryPath(strings.TrimPrefix(arg, "--config=")) == want
		}
	}
	return false
}
