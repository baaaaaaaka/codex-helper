package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Restart-loop protection (R-13: the owner forbids restart loops; the old
// service restarted about 3,000 times).
//
// A service-managed helper paces itself at start, so one rule covers every
// supervisor (local supervisor, systemd of any version, launchd, Windows
// tasks): each start that follows the previous start by less than
// teamsCrashLoopHealthyRun counts as a fast restart, and the n-th
// consecutive fast restart waits teamsCrashLoopDelay(n) in total (10 s
// doubling to 5 min). A run of 10 min or more resets the count. The
// supervisor's own 10 s restart delay is part of that total, so a first fast
// restart adds nothing, and a restart after a healthy run (an update or
// `helper restart`, exit 75) is not delayed at all. The wait is recorded so
// the watchdog leaves a backing-off helper alone.
//
// systemd's RestartSteps= is not used: systemd only resets that step counter
// on a manual start, not after a healthy run, so after five automatic
// restarts every exit-75 restart would wait the full maximum.
//
// Expected refusals do not restart at all: a service-managed helper that is
// refused the store because the kill-switch file exists or another host's
// helper is live waits in-process and checks again every teamsRefusalRecheck.
const (
	teamsCrashLoopBase       = 10 * time.Second
	teamsCrashLoopCap        = 5 * time.Minute
	teamsCrashLoopHealthyRun = 10 * time.Minute
)

var (
	teamsRefusalRecheck      = 45 * time.Second
	teamsServiceBackoffNow   = time.Now
	teamsServiceBackoffSleep = sleepContext
)

// teamsCrashLoopDelay is the total pause before the n-th consecutive fast
// restart (0 for none).
func teamsCrashLoopDelay(n int) time.Duration {
	if n <= 0 {
		return 0
	}
	d := teamsCrashLoopBase
	for i := 1; i < n && d < teamsCrashLoopCap; i++ {
		d *= 2
	}
	if d > teamsCrashLoopCap {
		d = teamsCrashLoopCap
	}
	return d
}

type teamsServiceRestartRecord struct {
	Streak    int       `json:"streak"`
	LastStart time.Time `json:"last_start"`
	WaitUntil time.Time `json:"wait_until,omitempty"`
}

// teamsServiceRestartRecordPath is per host: hosts sharing a home share the
// store directory.
func teamsServiceRestartRecordPath(storeDir, host string) string {
	name := "service-restarts.json"
	if host = safeWindowsTaskNamePart(host, 64); host != "" {
		name = "service-restarts." + host + ".json"
	}
	return filepath.Join(storeDir, name)
}

func readTeamsServiceRestartRecord(path string) teamsServiceRestartRecord {
	var rec teamsServiceRestartRecord
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &rec)
	}
	return rec
}

func writeTeamsServiceRestartRecord(path string, rec teamsServiceRestartRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// teamsServiceStartBackoff paces a service-managed helper at start (see the
// comment above). It returns ctx's error if the helper is stopped while
// waiting.
func teamsServiceStartBackoff(ctx context.Context, path string, errOut io.Writer) error {
	rec := readTeamsServiceRestartRecord(path)
	now := teamsServiceBackoffNow()
	n := 0
	if !rec.LastStart.IsZero() && now.Sub(rec.LastStart) < teamsCrashLoopHealthyRun {
		n = rec.Streak + 1
	}
	if extra := teamsCrashLoopDelay(n) - teamsCrashLoopBase; extra > 0 {
		_ = writeTeamsServiceRestartRecord(path, teamsServiceRestartRecord{Streak: n, LastStart: rec.LastStart, WaitUntil: now.Add(extra)})
		_, _ = fmt.Fprintf(errOut, "Teams helper restarted %d times in a row within %s of starting; waiting %s before it starts again\n",
			n, teamsCrashLoopHealthyRun, extra.Round(time.Second))
		if err := teamsServiceBackoffSleep(ctx, extra); err != nil {
			return err
		}
	}
	return writeTeamsServiceRestartRecord(path, teamsServiceRestartRecord{Streak: n, LastStart: teamsServiceBackoffNow()})
}

// teamsRefusalReason names an expected refusal to take the store ("" for any
// other error).
func teamsRefusalReason(err error) string {
	switch {
	case errors.Is(err, corestore.ErrDisabled):
		return "the kill-switch file " + corestore.DisabledFile + " exists"
	case errors.Is(err, corestore.ErrOwnerLive):
		return "another live helper owns the Teams state"
	}
	return ""
}

// teamsAcquireOrWait acquires the store. A service-managed helper (wait)
// that is refused for an expected reason waits quietly, checking again
// every teamsRefusalRecheck; it returns ctx's error when stopped.
func teamsAcquireOrWait(ctx context.Context, acquire func() error, wait bool, errOut io.Writer) error {
	last := ""
	for {
		err := acquire()
		if err == nil {
			return nil
		}
		reason := teamsRefusalReason(err)
		if !wait || reason == "" {
			return err
		}
		if reason != last {
			_, _ = fmt.Fprintf(errOut, "Teams helper standing by: %s (%v); checking again every %s\n", reason, err, teamsRefusalRecheck)
			last = reason
		}
		if err := teamsServiceBackoffSleep(ctx, teamsRefusalRecheck); err != nil {
			return err
		}
	}
}
