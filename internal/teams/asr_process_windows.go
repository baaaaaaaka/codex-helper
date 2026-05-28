//go:build windows

package teams

import (
	"context"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

func runASRCommand(ctx context.Context, cmd *exec.Cmd) error {
	if ctx == nil {
		ctx = context.Background()
	}
	job, _ := newASRWindowsJob()
	if job != 0 {
		defer windows.CloseHandle(job)
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	assignedJob := false
	if job != 0 && cmd.Process != nil {
		process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
		if err == nil {
			if windows.AssignProcessToJobObject(job, process) == nil {
				assignedJob = true
			}
			_ = windows.CloseHandle(process)
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if assignedJob {
			_ = windows.TerminateJobObject(job, 1)
		} else if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
		return ctx.Err()
	}
}

func newASRWindowsJob() (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return 0, err
	}
	return job, nil
}
