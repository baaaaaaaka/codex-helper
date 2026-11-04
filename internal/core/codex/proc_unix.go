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

// groupAlive reports whether any live (non-zombie) process of the group
// still exists. A killed process stays a zombie until its parent reaps it,
// and an orphaned group member may never be reaped promptly (for example
// under a container init that does not reap), so kill(-pgid, 0) alone would
// report a dead group as alive. Where /proc is available the group's
// members are checked for their state; elsewhere kill(-pgid, 0) decides.
func groupAlive(pgid int) bool {
	if pgid <= 1 {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	if !(err == nil || errors.Is(err, syscall.EPERM)) {
		return false
	}
	live, ok := liveGroupMembers(pgid)
	if !ok {
		return true
	}
	return live
}

// liveGroupMembers scans /proc for a process of group pgid that is not a
// zombie. ok is false when /proc cannot be read.
func liveGroupMembers(pgid int) (live, ok bool) {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return false, false
	}
	want := strconv.Itoa(pgid)
	seen := false
	for _, e := range ents {
		name := e.Name()
		if name == "" || name[0] < '0' || name[0] > '9' {
			continue
		}
		data, err := os.ReadFile("/proc/" + name + "/stat")
		if err != nil {
			continue
		}
		s := string(data)
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(s[i+1:])
		if len(f) < 3 {
			continue
		}
		seen = true
		// f[0] is the state, f[2] the process group.
		if f[2] == want && f[0] != "Z" && f[0] != "X" {
			return true, true
		}
	}
	return false, seen
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
