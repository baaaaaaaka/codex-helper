//go:build windows

package cli

import (
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"golang.org/x/sys/windows"
)

func init() {
	teamsWindowsBridgeExitProbe = openTeamsWindowsBridgeExitProbe
}

func openTeamsWindowsBridgeExitProbe(owner lease.Record) (func() bool, func()) {
	if owner.PID <= 0 || owner.Wall <= 0 {
		return nil, nil
	}
	process, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(owner.PID))
	if err != nil {
		return nil, nil
	}
	var created, exited, kernel, user windows.Filetime
	if windows.GetProcessTimes(process, &created, &exited, &kernel, &user) != nil ||
		time.Unix(0, created.Nanoseconds()).After(time.UnixMilli(owner.Wall).Add(time.Millisecond)) {
		_ = windows.CloseHandle(process)
		return nil, nil
	}
	return func() bool {
		state, err := windows.WaitForSingleObject(process, 0)
		return err == nil && state == windows.WAIT_OBJECT_0
	}, func() { _ = windows.CloseHandle(process) }
}
