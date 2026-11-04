package asr

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// processID identifies a running process across PID reuse: its start time
// and command name. Zombies count as gone.
func processID(pid int) (string, bool) {
	const zombie = 5 // SZOMB
	k, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || k.Proc.P_pid != int32(pid) || k.Proc.P_stat == zombie {
		return "", false
	}
	t := k.Proc.P_starttime
	return fmt.Sprintf("%d.%06d %s", t.Sec, t.Usec, unix.ByteSliceToString(k.Proc.P_comm[:])), true
}
