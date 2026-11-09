//go:build windows

package helperruntime

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/term"
)

func killBootTree(pid int) {
	cmd := exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T", "/F")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NO_WINDOW}
	_ = cmd.Run()
}

func configureBootProbe(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NO_WINDOW}
	cmd.Cancel = func() error {
		killBootTree(cmd.Process.Pid)
		return cmd.Process.Kill()
	}
}

func abortBootStartup() {
	// Job ownership normally handles a killed parent. Also cover hosts that
	// rejected job assignment and required taskkill's process-tree fallback.
	killBootTree(os.Getpid())
	os.Exit(1)
}

func prepareBootTerminal(cmd *exec.Cmd) func() {
	fd := int(os.Stdin.Fd())
	s, err := term.GetState(fd)
	return func() {
		if err == nil {
			_ = term.Restore(fd, s)
		}
	}
}

// Match the broker's Job Object ownership, including its native fallback when
// a Windows host rejects nested jobs. No detached console/service is created.
func attachBootProcess(cmd *exec.Cmd) (func(), func(), func() error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err == nil {
		info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
		info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
		_, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
		if err == nil {
			var process windows.Handle
			process, err = windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
			if err == nil {
				err = windows.AssignProcessToJobObject(job, process)
				_ = windows.CloseHandle(process)
			}
		}
		if err != nil {
			_ = windows.CloseHandle(job)
			job = 0
		}
	}
	return func() {
			if job != 0 {
				_ = windows.CloseHandle(job)
			}
		}, func() {
			if job != 0 {
				_ = windows.TerminateJobObject(job, 1)
				return
			}
			killBootTree(cmd.Process.Pid)
			_ = cmd.Process.Kill()
		}, func() error {
			if job != 0 {
				// After handoff the CLI owns ordinary session/child lifetimes.
				// Closing the launcher must not kill a user-detached terminal.
				info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
				_, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
				return err
			}
			return nil
		}
}

func signalBootProcess(cmd *exec.Cmd, sig os.Signal) {
	// Console Ctrl+C already reaches both parent and child. Do not duplicate it.
	if sig != os.Interrupt {
		_ = cmd.Process.Kill()
	}
}

func bootCancellation(exit *exec.ExitError) (bool, int) {
	return uint32(exit.ExitCode()) == 0xc000013a, 130 // STATUS_CONTROL_C_EXIT
}
