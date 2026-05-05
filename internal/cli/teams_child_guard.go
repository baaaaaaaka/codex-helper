package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	envTeamsCodexChild     = "CODEX_HELPER_TEAMS_CHILD"
	envTeamsCodexParentPID = "CODEX_HELPER_TEAMS_PARENT_PID"
)

func teamsCodexChildEnv() []string {
	return []string{
		envTeamsCodexChild + "=1",
		envTeamsCodexParentPID + "=" + strconv.Itoa(os.Getpid()),
	}
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
