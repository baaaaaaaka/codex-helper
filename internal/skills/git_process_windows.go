package skills

import (
	"context"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

func detachGitCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW}
	cmd.Cancel = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		kill := exec.CommandContext(ctx, "taskkill.exe", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F")
		kill.SysProcAttr = cmd.SysProcAttr
		if err := kill.Run(); err == nil {
			return nil
		}
		return cmd.Process.Kill()
	}
}
