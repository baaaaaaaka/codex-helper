package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
)

// The running helper records when it last read Teams successfully in a
// small file next to its store, for `cxp teams status` (F-CLI-1 "last
// poll"). It is not kept in the store: the store takes no periodic
// write, and status reads the file even when the store is in use on
// another host.
const teamsPollStampName = "core-poll.sqlite"

// teamsPollStampEvery bounds how often the helper rewrites the file while
// it reads Teams: "last poll" is at most this much older than the truth.
const teamsPollStampEvery = 10 * time.Second

type teamsPollStamp struct {
	Host, Version string
	PID           int
	At            int64 // unix ms of the last successful read
	Generation    int64
	FailureSince  int64 // unix ms, only when this owner detects a sustained failure
}

func teamsPollStampPath(dir string) string { return filepath.Join(dir, teamsPollStampName) }

func writeTeamsPollStamp(dir string, s teamsPollStamp) error {
	return writeTeamsPollSnapshot(teamsPollStampPath(dir), s)
}

func readTeamsPollStamp(dir string) (teamsPollStamp, bool) {
	s, err := readTeamsPollSnapshot(teamsPollStampPath(dir))
	return s, err == nil && (s.At != 0 || s.FailureSince != 0)
}

// teamsPollStamper writes when a poll or degradation state changes, at
// most every teamsPollStampEvery. It uses the existing 5 s watcher tick.
type teamsPollStamper struct {
	dir, host, version string
	gen                int64
	lastRead           func() time.Time
	failedSince        func(time.Time) time.Time
	written            teamsPollStamp
	wroteAt            time.Time
}

func (p *teamsPollStamper) tick(now time.Time) {
	at := p.lastRead()
	stamp := teamsPollStamp{Host: p.host, PID: os.Getpid(), Version: p.version, Generation: p.gen}
	if !at.IsZero() {
		stamp.At = at.UnixMilli()
	}
	if p.failedSince != nil {
		if since := p.failedSince(at); !since.IsZero() {
			stamp.FailureSince = since.UnixMilli()
		}
	}
	if (stamp.At == 0 && stamp.FailureSince == 0 && p.written.FailureSince == 0) ||
		stamp == p.written || (!p.wroteAt.IsZero() && now.Sub(p.wroteAt) < teamsPollStampEvery) {
		return
	}
	if err := writeTeamsPollStamp(p.dir, stamp); err == nil {
		p.written, p.wroteAt = stamp, now
	}
}

func teamsPollFromOwner(view teamsStoreView, s teamsPollStamp, now time.Time) bool {
	in := view.Info
	return in.Live(now, lease.ProductionStale) && teamsHostEqual(in.Head.Host, s.Host) && in.Head.PID == s.PID &&
		(s.Generation == 0 || s.Generation == in.HeadGen)
}

// teamsLastPollLine is the status line for the last successful read of
// Teams ("" when no helper ever recorded one).
func teamsLastPollLine(view teamsStoreView, now time.Time) string {
	s, ok := readTeamsPollStamp(filepath.Dir(view.Anchor))
	line := "Last poll: none recorded"
	if !ok {
		return line
	}
	current := teamsPollFromOwner(view, s, now)
	if s.At != 0 {
		at := time.UnixMilli(s.At)
		age := max(0, now.Sub(at).Round(time.Second))
		line = fmt.Sprintf("Last poll: %s (%s ago)", at.Local().Format(time.RFC3339), age)
		if !current {
			line += fmt.Sprintf(" by the helper on %s (pid %d), which is not running now", s.Host, s.PID)
		}
	}
	if current && s.FailureSince != 0 {
		age := max(0, now.Sub(time.UnixMilli(s.FailureSince)).Round(time.Second))
		line += fmt.Sprintf("\nTeams Graph: degraded (proxy or gateway failures for %s)", age)
	}
	return line
}

// teamsPendingVersionLine says that the version installed here (this
// command's) is not the one the live helper runs (R-17): it becomes the
// running version when the helper restarts. "" when they match or no
// helper is running.
func teamsPendingVersionLine(view teamsStoreView, running string, now time.Time) string {
	in := view.Info
	installed := buildVersion()
	if running == "" || running == installed || !in.HeadOK || in.Head.State == lease.Released || !in.Live(now, lease.ProductionStale) {
		return ""
	}
	return fmt.Sprintf("Pending version: %s installed, not running yet (the running helper switches to it when it restarts)", installed)
}
