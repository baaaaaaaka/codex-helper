//go:build !windows

package asr

import (
	"errors"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// proc is a started child process that can be killed together with its
// children.
type proc struct{ cmd *exec.Cmd }

// startProc starts cmd in its own process group. Callers must Wait on the
// returned proc's cmd: ETXTBSY is retried on a fresh copy of cmd, because a
// binary this process just wrote can still be open in a child forked
// concurrently by another goroutine (golang/go#22315).
func startProc(cmd *exec.Cmd) (*proc, error) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	setDeathSignal(cmd.SysProcAttr)
	for attempt := 1; ; attempt++ {
		c := exec.Command(cmd.Path, cmd.Args[1:]...)
		c.Env, c.Dir, c.Stdin, c.Stdout, c.Stderr, c.SysProcAttr = cmd.Env, cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr, cmd.SysProcAttr
		err := c.Start()
		if err == nil {
			return &proc{cmd: c}, nil
		}
		if !errors.Is(err, syscall.ETXTBSY) || attempt == 10 {
			return nil, err
		}
		time.Sleep(time.Duration(attempt) * 20 * time.Millisecond)
	}
}

// kill stops the whole process group.
func (p *proc) kill() {
	if p.cmd.Process != nil {
		_ = unix.Kill(-p.cmd.Process.Pid, unix.SIGKILL)
	}
}

func (p *proc) release() {}

func freeBytes(path string) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}
