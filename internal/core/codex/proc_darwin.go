//go:build darwin

package codex

import (
	"strconv"

	"golang.org/x/sys/unix"
)

// startTime returns an identifier of when pid started, or "" when unknown.
// It guards against killing a recycled pid.
func startTime(pid int) string {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || kp == nil || kp.Proc.P_starttime.Sec == 0 {
		return ""
	}
	t := kp.Proc.P_starttime
	return strconv.FormatInt(int64(t.Sec), 10) + "." + strconv.FormatInt(int64(t.Usec), 10)
}

// bootID identifies this boot session: it changes on every boot and
// differs between machines.
func bootID() string {
	s, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return ""
	}
	return s
}

func pidNamespace() string { return "" }
