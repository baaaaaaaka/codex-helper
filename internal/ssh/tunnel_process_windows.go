//go:build windows

package ssh

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type tunnelProcessHandle struct {
	job windows.Handle
}

const windowsCreateNoWindow = 0x08000000

func configureTunnelCommand(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	attr := cmd.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	attr.HideWindow = true
	attr.CreationFlags |= windowsCreateNoWindow
	cmd.SysProcAttr = attr
}

func attachTunnelProcess(cmd *exec.Cmd) tunnelProcessHandle {
	if cmd == nil || cmd.Process == nil {
		return tunnelProcessHandle{}
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return tunnelProcessHandle{}
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return tunnelProcessHandle{}
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(cmd.Process.Pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return tunnelProcessHandle{}
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		_ = windows.CloseHandle(job)
		return tunnelProcessHandle{}
	}
	return tunnelProcessHandle{job: job}
}

func closeTunnelProcess(handle tunnelProcessHandle) {
	if handle.job != 0 {
		_ = windows.CloseHandle(handle.job)
	}
}

func terminateTunnelProcess(cmd *exec.Cmd, handle tunnelProcessHandle, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(os.Interrupt)
	if grace > 0 {
		time.Sleep(grace)
	}
	if handle.job != 0 {
		_ = windows.TerminateJobObject(handle.job, 1)
		_ = windows.CloseHandle(handle.job)
		return nil
	}
	// A nested Job Object may be unavailable under a managed runner. Keep the
	// process-tree guarantee through the native Windows fallback.
	_ = exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run()
	_ = cmd.Process.Kill()
	return nil
}
