package migrate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Identify returns the legacy store version of a scope directory (holding
// state.json). It wraps ErrUnsupported for any other version.
func Identify(dir string) (Version, error) {
	v, _, err := identify(dir)
	return v, err
}

// Pending lists the legacy scope directories under a state root that have
// not been converted yet (no core.sqlite next to them and no marker).
func Pending(stateDir string) []string {
	var out []string
	for _, dir := range Candidates(stateDir) {
		if _, err := os.Stat(filepath.Join(dir, TargetName)); err == nil {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, MarkerName)); err == nil {
			continue
		}
		out = append(out, dir)
	}
	return out
}

// LastActivity is the newest modification time of a scope's store files
// (state.json, the SQLite file and its WAL), in unix ms.
func LastActivity(dir string) int64 {
	var newest int64
	for _, name := range []string{"state.json", "store.sqlite", "store.sqlite-wal"} {
		if st, err := os.Stat(filepath.Join(dir, name)); err == nil && st.ModTime().UnixMilli() > newest {
			newest = st.ModTime().UnixMilli()
		}
	}
	return newest
}

// ChooseSource picks the scope to convert at first start among pending
// ones: the single supported store. It refuses when there is none (an
// unsupported intermediate release store: run rc.59 once first), when
// several supported stores exist, or when an unsupported store was used
// more recently than the supported one (that one may hold newer state).
func ChooseSource(pending []string) (string, error) {
	var supported, unsupported []string
	for _, dir := range pending {
		if _, err := Identify(dir); err == nil {
			supported = append(supported, dir)
		} else if errors.Is(err, ErrUnsupported) {
			unsupported = append(unsupported, dir)
		} else {
			return "", fmt.Errorf("Teams store at %s: %w", dir, err)
		}
	}
	switch {
	case len(supported) == 0 && len(unsupported) > 0:
		return "", fmt.Errorf("the Teams store at %s is from an intermediate release this version cannot convert: run v0.1.22-rc.59 once against it, then upgrade", unsupported[0])
	case len(supported) == 0:
		return "", nil
	case len(supported) > 1:
		return "", fmt.Errorf("several Teams stores from earlier versions exist (%s); convert the one to keep with `cxp teams migrate --source <dir>`", strings.Join(supported, ", "))
	}
	for _, u := range unsupported {
		if LastActivity(u) > LastActivity(supported[0]) {
			return "", fmt.Errorf("the Teams store at %s (an intermediate release) was used more recently than the one at %s: run v0.1.22-rc.59 once, then upgrade, or convert one explicitly with `cxp teams migrate --source <dir>`", u, supported[0])
		}
	}
	return supported[0], nil
}

// interruptedNoticeKey prefixes the delivery key of the interruption notice
// the conversion adds for a turn that was running (convert.go).
const interruptedNoticeKey = "legacy-interrupted:"

// summary is the short user-facing text the engine posts to the control
// chat once after the conversion (no internal ids or paths).
func (c *converter) summary() string {
	q := func(query string, args ...any) int64 {
		n, err := c.im.QueryInt(query, args...)
		if err != nil {
			return 0
		}
		return n
	}
	live := q(`SELECT count(*) FROM conversations WHERE kind='work' AND state IN ('active','creating','hold')`)
	parked := q(`SELECT count(*) FROM conversations WHERE kind='work' AND state='parked'`)
	closed := q(`SELECT count(*) FROM conversations WHERE kind='work' AND state='closed'`)
	turns := q(`SELECT count(*) FROM turns`)
	// The previous helper's messages that are sent now; the interruption
	// notices the conversion itself adds are not among them.
	unsent := q(`SELECT count(*) FROM outbox WHERE state IN ('queued','unknown') AND substr(delivery_key, 1, ?) <> ?`,
		len(interruptedNoticeKey), interruptedNoticeKey)
	interrupted := c.rep.E("turns").Outcomes["running -> interrupted (+notice)"]

	var b strings.Builder
	fmt.Fprintf(&b, "Teams helper data converted from %s\n\n", c.l.version)
	fmt.Fprintf(&b, "Work chats: %d (%d active, %d parked, %d closed). Past requests: %d.", live+parked+closed, live, parked, closed, turns)
	if interrupted > 0 {
		if interrupted == 1 {
			b.WriteString("\n\n1 request was running when the previous helper stopped: its chat gets one notice; it is not re-run.")
		} else {
			fmt.Fprintf(&b, "\n\n%d requests were running when the previous helper stopped: each chat gets one notice; they are not re-run.", interrupted)
		}
	}
	c.quarantineSummary(&b)
	if unsent > 0 {
		if unsent == 1 {
			b.WriteString("\n\n1 message that was not sent yet is sent now.")
		} else {
			fmt.Fprintf(&b, "\n\n%d messages that were not sent yet are sent now.", unsent)
		}
	}
	if n := len(c.checkAnswer); n > 0 {
		var first, last string
		for _, at := range c.checkAnswer {
			d := time.UnixMilli(at).In(c.now.Location()).Format("2006-01-02")
			if first == "" || d < first {
				first = d
			}
			if d > last {
				last = d
			}
		}
		when := "from " + first
		if last != first {
			when = "from " + first + " to " + last
		}
		if n == 1 {
			fmt.Fprintf(&b, "\n\n1 answer %s that the previous helper could not confirm as sent is checked against the chat history now, and sent only if it is missing.", when)
		} else {
			fmt.Fprintf(&b, "\n\n%d answers %s that the previous helper could not confirm as sent are checked against the chat history now, and sent only if they are missing.", n, when)
		}
	}
	if c.staleUnsent > 0 {
		fmt.Fprintf(&b, "\n\n%d older messages (from more than a day ago) that the previous helper had not sent or could not confirm are not sent now.", c.staleUnsent)
	}
	if bl := c.rep.Backlog; bl.Counted {
		sessions := "sessions"
		if bl.Sources == 1 {
			sessions = "session"
		}
		fmt.Fprintf(&b, "\n\nCatch-up of local Codex sessions: %d prompts and answers from %d %s (%.1f MB), posted to their chats from now on.",
			bl.Prompts+bl.Finals, bl.Sources, sessions, float64(bl.Bytes)/1e6)
	} else {
		b.WriteString("\n\nLocal Codex sessions are caught up from now on.")
	}
	if n := len(c.histChecks); n == 1 {
		b.WriteString("\n\n1 session's chat history is read first: what the chat already has is not posted again.")
	} else if n > 1 {
		fmt.Fprintf(&b, "\n\n%d sessions' chat histories are read first: what a chat already has is not posted again.", n)
	}
	b.WriteString("\n\nThe previous data is kept unchanged as a backup. The full report is " + ReportName + " next to the helper's store (`cxp teams status` shows where).")
	return b.String()
}

// quarantineSummary says what became of each quarantined chat
// (quarantineRuling), by its title and final state.
func (c *converter) quarantineSummary(b *strings.Builder) {
	name := func(label string) string {
		if si := c.sessions[label]; si != nil {
			return chatName(firstNonEmpty(si.s.UserTitle, si.s.TeamsTopic), label)
		}
		return chatName("", label)
	}
	for _, label := range c.quarantine["quarantined (self-echo breaker) -> active"] {
		fmt.Fprintf(b, "\n\n%s: the previous helper paused it automatically (self-echo protection); it is no longer paused", name(label))
		if n, err := c.im.QueryInt(`SELECT count(*) FROM conversations WHERE label = ? AND state = 'parked'`, label); err == nil && n > 0 {
			fmt.Fprintf(b, ", and parked because another chat continues its Codex session (wake it with `r %s`)", label)
		}
		b.WriteString(". Messages posted there before now are not answered.")
	}
	for _, label := range c.quarantine["quarantined (owner) -> closed"] {
		fmt.Fprintf(b, "\n\n%s: you quarantined it with the previous helper; it is closed.", name(label))
	}
	for _, label := range c.quarantine["quarantined -> active+paused"] {
		fmt.Fprintf(b, "\n\n%s: quarantined by the previous helper; it stays paused.", name(label))
	}
}

// chatName names a chat in the summary: its label and title.
func chatName(title, label string) string {
	if title == "" {
		return "Chat " + label
	}
	return "Chat " + label + " (\u201c" + title + "\u201d)"
}
