package codex

import (
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

func configureSharedLifetime(*exec.Cmd) {}

// Keep the job handle in the helper. A helper crash closes it and terminates
// the server tree, without depending on the next helper starting on this host.
func attachSharedLifetime(cmd *exec.Cmd) (func(), error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (func(), error) { _ = windows.CloseHandle(job); return nil, err }
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		return fail(err)
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(cmd.Process.Pid))
	if err != nil {
		return fail(err)
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		return fail(err)
	}
	return func() { _ = windows.CloseHandle(job) }, nil
}
