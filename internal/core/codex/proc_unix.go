//go:build !windows

package codex

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// configure puts the app-server (and everything it spawns) in its own
// process group so it can be killed as a whole, also by the next helper
// process after a crash.
func configure(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// signalGroup sends sig to the process group pgid.
func signalGroup(pgid int, sig syscall.Signal) error {
	if pgid <= 1 {
		return errors.New("codex: refusing to signal process group <= 1")
	}
	return syscall.Kill(-pgid, sig)
}

func terminateGroup(pgid int) error { return signalGroup(pgid, syscall.SIGTERM) }
func killGroup(pgid int) error      { return signalGroup(pgid, syscall.SIGKILL) }

// groupAlive reports whether any process of the group still exists.
func groupAlive(pgid int) bool {
	if pgid <= 1 {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// startTime returns an identifier of when pid started (Linux: the start
// time in clock ticks from /proc), or "" when unknown. It guards against
// killing a recycled pid.
func startTime(pid int) string {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(data)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return ""
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return ""
	}
	return f[19] // field 22 overall: starttime
}
