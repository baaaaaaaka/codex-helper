//go:build windows

package asr

import (
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// proc is a started child process inside a kill-on-close job object: killing
// the job (or the helper exiting) stops the child and all its children.
type proc struct {
	cmd *exec.Cmd
	job windows.Handle
}

func startProc(cmd *exec.Cmd) (*proc, error) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NO_WINDOW
	job, _ := newKillOnCloseJob()
	if err := cmd.Start(); err != nil {
		if job != 0 {
			_ = windows.CloseHandle(job)
		}
		return nil, err
	}
	p := &proc{cmd: cmd}
	if job != 0 {
		h, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
		if err == nil && windows.AssignProcessToJobObject(job, h) == nil {
			p.job = job
		} else {
			_ = windows.CloseHandle(job)
		}
		if err == nil {
			_ = windows.CloseHandle(h)
		}
	}
	return p, nil
}

func (p *proc) kill() {
	if p.job != 0 {
		_ = windows.TerminateJobObject(p.job, 1)
	} else if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
}

// release closes the job handle after the process has exited.
func (p *proc) release() {
	if p.job != 0 {
		_ = windows.CloseHandle(p.job)
		p.job = 0
	}
}

// processID identifies a running process across PID reuse: its creation
// time and image path.
func processID(pid int) (string, bool) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", false
	}
	defer windows.CloseHandle(h)
	return handleID(h)
}

func handleID(h windows.Handle) (string, bool) {
	const stillActive = 259
	var code uint32
	if windows.GetExitCodeProcess(h, &code) != nil || code != stillActive {
		return "", false
	}
	var created, exited, kernel, user windows.Filetime
	if windows.GetProcessTimes(h, &created, &exited, &kernel, &user) != nil {
		return "", false
	}
	buf := make([]uint16, 4096)
	n := uint32(len(buf))
	if windows.QueryFullProcessImageName(h, 0, &buf[0], &n) != nil {
		return "", false
	}
	return fmt.Sprintf("%d %s", created.Nanoseconds(), windows.UTF16ToString(buf[:n])), true
}

// killIfMatches terminates pid if it is still the process identified by id;
// the check and the kill use one handle, so a recycled PID is never hit.
func killIfMatches(pid int, id string) bool {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return false
	}
	defer windows.CloseHandle(h)
	if got, ok := handleID(h); !ok || got != id {
		return false
	}
	return windows.TerminateProcess(h, 1) == nil
}

func newKillOnCloseJob() (windows.Handle, error) {
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

func freeBytes(path string) (uint64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var free uint64
	if err := windows.GetDiskFreeSpaceEx(p, &free, nil, nil); err != nil {
		return 0, err
	}
	return free, nil
}
