package cli

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type execIdentity struct {
	UID         uint32
	GID         uint32
	Groups      []uint32
	GroupsKnown bool
	Username    string
	Home        string
}

func applyExecIdentityEnv(envVars []string, identity *execIdentity) []string {
	if identity == nil {
		return envVars
	}
	if home := strings.TrimSpace(identity.Home); home != "" {
		envVars = setEnvValue(envVars, "HOME", home)
	}
	if username := strings.TrimSpace(identity.Username); username != "" {
		envVars = setEnvValue(envVars, "USER", username)
		envVars = setEnvValue(envVars, "LOGNAME", username)
	}
	return envVars
}

func applyExecIdentity(cmd *exec.Cmd, envVars []string, identity *execIdentity) ([]string, error) {
	envVars = applyExecIdentityEnv(envVars, identity)
	if identity == nil || identity.UID == 0 {
		return envVars, nil
	}
	if cmd == nil {
		return envVars, fmt.Errorf("missing command")
	}
	return applyPlatformExecIdentity(cmd, envVars, identity)
}

func parseUint32(raw string) (uint32, bool) {
	if strings.TrimSpace(raw) == "" {
		return 0, false
	}
	v, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(v), true
}
