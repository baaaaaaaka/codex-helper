// Package manifestprocess runs CI helper commands with bounded process-tree
// cleanup.  The recovery manifest deliberately starts one test process per
// entry; a timed-out command must not leave the test binary (or a compiler
// child) running after the runner has returned.
package manifestprocess

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

const (
	processWaitDelay       = 2 * time.Second
	processTerminationWait = 10 * time.Second
)

// Run starts cmd, waits for it, and terminates its process tree when ctx is
// canceled. The command must be created with exec.Command, not
// exec.CommandContext; this package owns cancellation so it can terminate
// descendants as well as the direct process.
func Run(ctx context.Context, cmd *exec.Cmd) error {
	if cmd == nil {
		return errors.New("manifest process command is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	configure(cmd)
	// A descendant can inherit an os/exec pipe even after the direct process
	// exits. The platform termination code normally removes that descendant;
	// WaitDelay is the final bounded guard for a detached child or a broken
	// cleanup path.
	cmd.WaitDelay = processWaitDelay
	if err := cmd.Start(); err != nil {
		return err
	}
	handle := attach(cmd)
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		closeHandle(handle)
		return err
	case <-ctx.Done():
		// Prefer a process that completed at the cancellation boundary. This
		// avoids turning a successful command into a timeout solely because
		// Wait's notification and the context deadline became ready together.
		select {
		case err := <-done:
			closeHandle(handle)
			return err
		default:
		}
		terminate(cmd, handle)
		timer := time.NewTimer(processTerminationWait)
		defer timer.Stop()
		select {
		case err := <-done:
			closeHandle(handle)
			if err != nil {
				// Callers use the context error to classify the watchdog expiry;
				// retain it even though Wait reports the expected killed-process
				// error.
				return ctx.Err()
			}
			return ctx.Err()
		case <-timer.C:
			// Do not let a failed platform tree-kill leave the manifest runner
			// waiting forever.  Windows closeHandle also closes a
			// KILL_ON_JOB_CLOSE job, so perform it before reporting the bounded
			// cleanup failure.
			closeHandle(handle)
			return fmt.Errorf("manifest process did not terminate after cancellation: %w", ctx.Err())
		}
	}
}
