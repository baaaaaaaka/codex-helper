//go:build !windows

package cli

import (
	"os/exec"
	"testing"
)

func TestConfigureTeamsServiceDetachedCommandStartsNewSession(t *testing.T) {
	cmd := exec.Command("true")
	configureTeamsServiceDetachedCommand(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Fatalf("detached service command SysProcAttr = %#v, want Setsid", cmd.SysProcAttr)
	}
}
