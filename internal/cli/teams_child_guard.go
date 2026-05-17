package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	envTeamsCodexChild     = "CODEX_HELPER_TEAMS_CHILD"
	envTeamsCodexParentPID = "CODEX_HELPER_TEAMS_PARENT_PID"
	envTeamsHelperCLIPath  = "CODEX_HELPER_CLI_PATH"
	envTeamsHelperCLIDir   = "CODEX_HELPER_CLI_DIR"
)

var teamsChildExecutablePath = os.Executable

func teamsCodexChildEnv() []string {
	env := []string{
		envTeamsCodexChild + "=1",
		envTeamsCodexParentPID + "=" + strconv.Itoa(os.Getpid()),
	}
	exe, err := teamsChildExecutablePath()
	if err != nil {
		return env
	}
	if strings.TrimSpace(exe) == "" {
		return env
	}
	env = append(env, envTeamsHelperCLIPath+"="+exe)
	dir := filepath.Dir(exe)
	if strings.TrimSpace(dir) == "" || dir == "." {
		return env
	}
	env = append(env, envTeamsHelperCLIDir+"="+dir)
	if path := prependPathDir(dir, os.Getenv("PATH")); path != "" {
		env = append(env, "PATH="+path)
	}
	return env
}

func prependPathDir(dir string, current string) string {
	if strings.TrimSpace(dir) == "" {
		return current
	}
	for _, existing := range filepath.SplitList(current) {
		if existing == dir {
			return current
		}
	}
	if current == "" {
		return dir
	}
	return dir + string(os.PathListSeparator) + current
}

func runningInsideTeamsCodexChild() bool {
	value := strings.TrimSpace(os.Getenv(envTeamsCodexChild))
	return value == "1" || strings.EqualFold(value, "true") || strings.EqualFold(value, "yes")
}

func rejectTeamsHelperSelfManagementFromChild(action string, controlCommand string) error {
	if !runningInsideTeamsCodexChild() {
		return nil
	}
	action = strings.TrimSpace(action)
	if action == "" {
		action = "manage the Teams helper"
	}
	controlCommand = strings.TrimSpace(controlCommand)
	if controlCommand == "" {
		controlCommand = "helper status"
	}
	return fmt.Errorf("refusing to %s from a Codex turn launched by Teams helper; finish this turn first, then send `%s` in the Teams control chat", action, controlCommand)
}
