package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

const (
	envTeamsCodexChild             = "CODEX_HELPER_TEAMS_CHILD"
	envTeamsCodexParentPID         = "CODEX_HELPER_TEAMS_PARENT_PID"
	envTeamsHelperCLIPath          = "CODEX_HELPER_CLI_PATH"
	envTeamsHelperCLIDir           = "CODEX_HELPER_CLI_DIR"
	envTeamsLocalSupervisorVersion = "CODEX_HELPER_TEAMS_LOCAL_SUPERVISOR_VERSION"
	envTeamsLinuxServiceBackend    = "CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND"
	envTeamsWSLServiceBackend      = "CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND"
)

var teamsChildExecutablePath = helperpath.RawExecutable

func teamsCodexChildEnv() []string {
	env := []string{
		envTeamsCodexChild + "=1",
		envTeamsCodexParentPID + "=" + strconv.Itoa(os.Getpid()),
	}
	var exe string
	if current, managed := helperruntime.Current(); managed {
		// An immutable runtime is not an installation entry: invoking it with
		// clean child environment would bootstrap a nested runtime directory.
		// Keep the owning entry even if it disappears; never select another
		// installation from PATH or fall back to the immutable executable.
		exe = current.EntryPath
	} else {
		raw, err := teamsChildExecutablePath()
		if err != nil || strings.TrimSpace(raw) == "" {
			return env
		}
		resolved, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
		if err != nil {
			return env
		}
		exe = resolved.Path
	}
	env = append(env, envTeamsHelperCLIPath+"="+exe)
	dir := filepath.Dir(exe)
	if strings.TrimSpace(dir) == "" || dir == "." {
		return env
	}
	env = append(env, envTeamsHelperCLIDir+"="+dir)
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
	controlCommand = strings.TrimSpace(controlCommand)
	if controlCommand == "" {
		controlCommand = "helper status"
	}
	return rejectTeamsHelperSelfManagementFromChildWithAdvice(action, "send `"+controlCommand+"` in the Teams control chat")
}

func rejectTeamsHelperSelfManagementFromChildWithAdvice(action string, advice string) error {
	if !runningInsideTeamsCodexChild() {
		return nil
	}
	action = strings.TrimSpace(action)
	if action == "" {
		action = "manage the Teams helper"
	}
	advice = strings.TrimSpace(advice)
	if advice == "" {
		advice = "send `helper status` in the Teams control chat"
	}
	return fmt.Errorf("refusing to %s from a Codex turn launched by Teams helper; finish this turn first, then %s", action, advice)
}
