package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestCrashLoopDelayDoublesFromTenSecondsToFiveMinutes(t *testing.T) {
	want := map[int]time.Duration{0: 0, 1: 10 * time.Second, 2: 20 * time.Second, 3: 40 * time.Second,
		4: 80 * time.Second, 5: 160 * time.Second, 6: 5 * time.Minute, 7: 5 * time.Minute, 50: 5 * time.Minute}
	for n, d := range want {
		if got := teamsCrashLoopDelay(n); got != d {
			t.Errorf("teamsCrashLoopDelay(%d) = %s, want %s", n, got, d)
		}
	}
}

// fakeBackoffClock drives teamsServiceStartBackoff: sleeping advances it.
type fakeBackoffClock struct {
	now    time.Time
	slept  []time.Duration
	cancel bool
}

func withFakeBackoffClock(t *testing.T) *fakeBackoffClock {
	t.Helper()
	c := &fakeBackoffClock{now: time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)}
	prevNow, prevSleep := teamsServiceBackoffNow, teamsServiceBackoffSleep
	teamsServiceBackoffNow = func() time.Time { return c.now }
	teamsServiceBackoffSleep = func(ctx context.Context, d time.Duration) error {
		c.slept = append(c.slept, d)
		if c.cancel {
			return context.Canceled
		}
		c.now = c.now.Add(d)
		return nil
	}
	t.Cleanup(func() { teamsServiceBackoffNow, teamsServiceBackoffSleep = prevNow, prevSleep })
	return c
}

// A helper that keeps exiting soon after it starts waits longer each time
// (10 s doubling to a 5 min total, the supervisor's 10 s included); a run of
// 10 min resets it, so a restart after a healthy run is not delayed.
func TestServiceStartBackoffPacesFastRestartsAndResetsAfterHealthyRun(t *testing.T) {
	c := withFakeBackoffClock(t)
	path := teamsServiceRestartRecordPath(t.TempDir(), "host-a")
	var out bytes.Buffer
	start := func() time.Duration {
		t.Helper()
		before := len(c.slept)
		if err := teamsServiceStartBackoff(context.Background(), path, &out); err != nil {
			t.Fatal(err)
		}
		var waited time.Duration
		for _, d := range c.slept[before:] {
			waited += d
		}
		return waited
	}
	if got := start(); got != 0 {
		t.Fatalf("first start waited %s", got)
	}
	// Each run exits after 2 s; the supervisor restarts after 10 s.
	var waits []time.Duration
	for i := 0; i < 8; i++ {
		c.now = c.now.Add(12 * time.Second)
		waits = append(waits, start())
	}
	want := []time.Duration{0, 10 * time.Second, 30 * time.Second, 70 * time.Second, 150 * time.Second, 290 * time.Second, 290 * time.Second, 290 * time.Second}
	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Fatalf("waits on top of the supervisor's 10 s = %v, want %v", waits, want)
	}
	if !strings.Contains(out.String(), "waiting 4m50s before it starts again") {
		t.Fatalf("the wait is not reported:\n%s", out.String())
	}
	// A healthy run (10 min) resets: the next restart, e.g. exit 75 for an
	// update, is not delayed, nor is the first fast one after it.
	c.now = c.now.Add(teamsCrashLoopHealthyRun)
	if got := start(); got != 0 {
		t.Fatalf("restart after a healthy run waited %s", got)
	}
	c.now = c.now.Add(2 * time.Second)
	if got := start(); got != 0 {
		t.Fatalf("first fast restart after a healthy run waited %s", got)
	}
	c.now = c.now.Add(2 * time.Second)
	if got := start(); got != 10*time.Second {
		t.Fatalf("second fast restart waited %s, want 10s", got)
	}
}

// While a helper waits, its record says until when, so the watchdog leaves
// it alone; a helper stopped while waiting returns at once.
func TestServiceStartBackoffRecordsTheWaitForTheWatchdog(t *testing.T) {
	c := withFakeBackoffClock(t)
	path := teamsServiceRestartRecordPath(t.TempDir(), "host-a")
	if err := writeTeamsServiceRestartRecord(path, teamsServiceRestartRecord{Streak: 4, LastStart: c.now.Add(-30 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	c.cancel = true
	if err := teamsServiceStartBackoff(context.Background(), path, &bytes.Buffer{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped while waiting: err = %v", err)
	}
	rec := readTeamsServiceRestartRecord(path)
	if want := c.now.Add(teamsCrashLoopDelay(5) - teamsCrashLoopBase); !rec.WaitUntil.Equal(want) || rec.Streak != 5 {
		t.Fatalf("record = %+v, want streak 5 waiting until %s", rec, want)
	}
	if filepath.Base(path) != "service-restarts.host-a.json" {
		t.Fatalf("record is not per host: %s", path)
	}
}

// The kill-switch file and another host's live owner are expected refusals:
// a service-managed helper stands by in-process (no exit, no restart) and
// takes the store once the refusal is gone; a foreground run still fails.
func TestServiceChildStandsByOnExpectedRefusal(t *testing.T) {
	c := withFakeBackoffClock(t)
	for _, refusal := range []error{corestore.ErrDisabled, fmt.Errorf("%w: host-b pid 7", corestore.ErrOwnerLive)} {
		c.slept = nil
		tries := 0
		acquire := func() error {
			tries++
			if tries <= 3 {
				return refusal
			}
			return nil
		}
		var out bytes.Buffer
		if err := teamsAcquireOrWait(context.Background(), acquire, true, &out); err != nil {
			t.Fatalf("%v: service child did not stand by: %v", refusal, err)
		}
		if tries != 4 || len(c.slept) != 3 || c.slept[0] != teamsRefusalRecheck {
			t.Fatalf("%v: tries=%d sleeps=%v, want 3 rechecks of %s", refusal, tries, c.slept, teamsRefusalRecheck)
		}
		if n := strings.Count(out.String(), "standing by"); n != 1 {
			t.Fatalf("%v: standby reported %d times:\n%s", refusal, n, out.String())
		}
		tries = 0
		if err := teamsAcquireOrWait(context.Background(), acquire, false, &out); !errors.Is(err, refusal) && !errors.Is(err, corestore.ErrOwnerLive) {
			t.Fatalf("%v: a foreground run must fail at once, got %v", refusal, err)
		}
	}
	other := errors.New("disk I/O error")
	if err := teamsAcquireOrWait(context.Background(), func() error { return other }, true, &bytes.Buffer{}); !errors.Is(err, other) {
		t.Fatalf("an unexpected error must not be waited out: %v", err)
	}
	c.cancel = true
	if err := teamsAcquireOrWait(context.Background(), func() error { return corestore.ErrDisabled }, true, &bytes.Buffer{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopping a standing-by child: %v", err)
	}
}

// The watchdog starts nothing while the kill-switch file exists or the
// helper is pacing its restarts.
func TestWatchdogLeavesDisabledOrPacingHelperAlone(t *testing.T) {
	now := time.Now()
	opts := teamsServiceWatchdogOptions{Now: now}
	for name, snap := range map[string]teamsServiceWatchdogSnapshot{
		"disabled, stopped":     {Installed: true, Active: false, Disabled: true},
		"disabled, stale owner": {Installed: true, Active: true, StateFiles: 1, OwnerFound: true, Disabled: true},
		"pacing, stopped":       {Installed: true, Active: false, BackoffUntil: now.Add(time.Minute)},
		"pacing, no owner":      {Installed: true, Active: true, StateFiles: 1, BackoffUntil: now.Add(time.Minute)},
	} {
		d := evaluateTeamsServiceWatchdog(snap, teamsServiceWatchdogState{ConsecutiveStale: 5}, opts)
		if d.Action != teamsServiceWatchdogActionNoop {
			t.Errorf("%s: watchdog decided %+v, want noop", name, d)
		}
	}
	d := evaluateTeamsServiceWatchdog(teamsServiceWatchdogSnapshot{Installed: true, BackoffUntil: now.Add(-time.Second)}, teamsServiceWatchdogState{}, opts)
	if d.Action != teamsServiceWatchdogActionStart {
		t.Fatalf("after the pause the watchdog starts the stopped service: %+v", d)
	}
}
