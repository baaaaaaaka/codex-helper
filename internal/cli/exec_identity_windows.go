//go:build windows

package cli

import (
	"os/exec"
	"os/user"
)

func execIdentityForHome(string) (*execIdentity, error) {
	return nil, nil
}

func execIdentityForUser(*user.User, string) (*execIdentity, error) {
	return nil, nil
}

func ensurePathOwnedByIdentity(string, *execIdentity) error {
	return nil
}

func applyPlatformExecIdentity(cmd *exec.Cmd, envVars []string, identity *execIdentity) ([]string, error) {
	return envVars, nil
}

func execIdentityRequiredError(string) error {
	return nil
}
