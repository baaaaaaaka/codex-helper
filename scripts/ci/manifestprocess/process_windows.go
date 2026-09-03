//go:build windows

package manifestprocess

import (
	"context"
	"os/exec"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type processHandle struct {
	job windows.Handle
}

const (
	windowsCreateNoWindow = 0x08000000
	processKillTimeout    = 5 * time.Second
)

func configure(cmd *exec.Cmd) {
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

func attach(cmd *exec.Cmd) processHandle {
	if cmd == nil || cmd.Process == nil {
		return processHandle{}
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return processHandle{}
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return processHandle{}
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(cmd.Process.Pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return processHandle{}
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		_ = windows.CloseHandle(job)
		return processHandle{}
	}
	return processHandle{job: job}
}

func terminate(cmd *exec.Cmd, handle processHandle) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if handle.job != 0 {
		// The Job Object is attached immediately after CreateProcess, but a
		// very fast child can create a descendant in that small interval. Run
		// the native tree-kill fallback as well so those pre-attachment
		// descendants cannot survive the watchdog.
		ctx, cancel := context.WithTimeout(context.Background(), processKillTimeout)
		_ = exec.CommandContext(ctx, "taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run()
		cancel()
		_ = windows.TerminateJobObject(handle.job, 1)
		return
	}
	// Managed runners can reject a nested Job Object. taskkill /T is the
	// native fallback that still covers the go wrapper and its descendants.
	ctx, cancel := context.WithTimeout(context.Background(), processKillTimeout)
	defer cancel()
	_ = exec.CommandContext(ctx, "taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run()
	_ = cmd.Process.Kill()
}

func closeHandle(handle processHandle) {
	if handle.job != 0 {
		_ = windows.CloseHandle(handle.job)
	}
}
