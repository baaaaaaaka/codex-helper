//go:build windows

package codexrunner

import (
	"os/exec"
	"strconv"
	"unsafe"

	"golang.org/x/sys/windows"
)

type appServerProcessHandle struct {
	job windows.Handle
}

func configureAppServerProcess(cmd *exec.Cmd) {
	configureBackgroundProcess(cmd)
}

func attachAppServerProcess(cmd *exec.Cmd) appServerProcessHandle {
	if cmd == nil || cmd.Process == nil {
		return appServerProcessHandle{}
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return appServerProcessHandle{}
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return appServerProcessHandle{}
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(cmd.Process.Pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return appServerProcessHandle{}
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		_ = windows.CloseHandle(job)
		return appServerProcessHandle{}
	}
	return appServerProcessHandle{job: job}
}

func closeAppServerProcess(handle appServerProcessHandle) {
	if handle.job != 0 {
		_ = windows.CloseHandle(handle.job)
	}
}

func terminateAppServerProcess(cmd *exec.Cmd, handle appServerProcessHandle) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if handle.job != 0 {
		_ = windows.TerminateJobObject(handle.job, 1)
		return
	}
	// Managed Windows environments can reject nested Job Objects. taskkill is
	// the native fallback that still terminates the wrapper's descendants.
	_ = exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run()
	_ = cmd.Process.Kill()
}
