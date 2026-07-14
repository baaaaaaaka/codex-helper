package cli

import (
	"errors"
	"os/exec"
	"time"
)

// detachedProcess owns the Wait call for a process which is intentionally
// allowed to outlive the command that started it.  Calling Process.Release
// alone is not enough on Unix: if the child exits while the parent is still
// alive, it can remain a zombie until the parent itself exits.
//
// Exactly one goroutine calls Cmd.Wait.  Callers that abort startup can wait
// briefly for that goroutine after terminating the process, while successful
// detached launches leave it running until the daemon exits.
type detachedProcess struct {
	cmd  *exec.Cmd
	done <-chan error
}

func startDetachedProcess(cmd *exec.Cmd) (*detachedProcess, error) {
	if cmd == nil {
		return nil, errors.New("detached process command is nil")
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	return &detachedProcess{cmd: cmd, done: done}, nil
}

func (p *detachedProcess) wait(timeout time.Duration) error {
	if p == nil || p.done == nil {
		return nil
	}
	if timeout <= 0 {
		return <-p.done
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-p.done:
		return err
	case <-timer.C:
		return errors.New("detached process did not exit before cleanup timeout")
	}
}
