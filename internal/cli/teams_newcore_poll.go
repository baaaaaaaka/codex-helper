package cli

import (
	"encoding/json"
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
const teamsPollStampName = "core-poll.json"

// teamsPollStampEvery bounds how often the helper rewrites the file while
// it reads Teams: "last poll" is at most this much older than the truth.
const teamsPollStampEvery = 10 * time.Second

type teamsPollStamp struct {
	Host    string `json:"host"`
	PID     int    `json:"pid"`
	Version string `json:"version,omitempty"`
	At      int64  `json:"at"` // unix ms of the last successful read
}

func teamsPollStampPath(dir string) string { return filepath.Join(dir, teamsPollStampName) }

func writeTeamsPollStamp(dir string, s teamsPollStamp) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	path := teamsPollStampPath(dir)
	tmp := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func readTeamsPollStamp(dir string) (teamsPollStamp, bool) {
	var s teamsPollStamp
	raw, err := os.ReadFile(teamsPollStampPath(dir))
	if err != nil || json.Unmarshal(raw, &s) != nil || s.At == 0 {
		return teamsPollStamp{}, false
	}
	return s, true
}

// teamsPollStamper rewrites the file when the engine has read Teams since
// the last write, at most every teamsPollStampEvery. It runs on the
// helper's existing 5 s watcher tick: no timer of its own.
type teamsPollStamper struct {
	dir, host, version string
	lastRead           func() time.Time
	written, wroteAt   time.Time
}

func (p *teamsPollStamper) tick(now time.Time) {
	at := p.lastRead()
	if at.IsZero() || !at.After(p.written) || (!p.wroteAt.IsZero() && now.Sub(p.wroteAt) < teamsPollStampEvery) {
		return
	}
	if err := writeTeamsPollStamp(p.dir, teamsPollStamp{Host: p.host, PID: os.Getpid(), Version: p.version, At: at.UnixMilli()}); err == nil {
		p.written, p.wroteAt = at, now
	}
}

// teamsLastPollLine is the status line for the last successful read of
// Teams ("" when no helper ever recorded one).
func teamsLastPollLine(view teamsStoreView, now time.Time) string {
	s, ok := readTeamsPollStamp(filepath.Dir(view.Anchor))
	if !ok {
		return "Last poll: none recorded"
	}
	at := time.UnixMilli(s.At)
	age := now.Sub(at).Round(time.Second)
	if age < 0 {
		age = 0
	}
	line := fmt.Sprintf("Last poll: %s (%s ago)", at.Local().Format(time.RFC3339), age)
	in := view.Info
	if !in.HeadOK || in.Head.State == lease.Released || !in.Live(now, lease.ProductionStale) || in.Head.Host != s.Host || in.Head.PID != s.PID {
		line += fmt.Sprintf(" by the helper on %s (pid %d), which is not running now", s.Host, s.PID)
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
