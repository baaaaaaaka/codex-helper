//go:build !windows

package cli

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureTargetProcessGroupPreservesExistingSysProcAttr(t *testing.T) {
	credential := &syscall.Credential{Uid: 1000, Gid: 1001, Groups: []uint32{1002}}
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: credential}

	configureTargetProcessGroup(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr was cleared")
	}
	if cmd.SysProcAttr.Credential != credential {
		t.Fatal("existing command credential was not preserved")
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Fatal("Setpgid was not enabled")
	}
}
