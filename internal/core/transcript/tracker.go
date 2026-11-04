package transcript

import (
	"fmt"
	"strings"
	"time"
)

// Output is one message to mirror. Key is stable for the same record no
// matter from which offset the file is re-read, so it can be used as the
// outbox delivery key and re-enqueueing is a no-op.
type Output struct {
	Key    string // "<thread>:<turn>:u<n>" (user prompt n of the turn) or "<thread>:<turn>:f" (final)
	User   bool   // a user prompt (else the turn's final answer)
	Text   string
	Time   time.Time
	TurnID string
}

// Tracker turns the records of one rollout into mirror outputs: the user
// prompts and the final answer of every turn, skipping turns whose user
// message carries a client id the helper itself sent (Teams turns).
//
// The persisted position (Resume) is the start of the turn still open, or
// the read position when no turn is open, so a restart re-reads at most the
// open turn and regenerates the same keys.
type Tracker struct {
	threadID string
	skip     func(clientID string) bool

	off, line int64 // next read position
	turn      *openTurn
	quiet     bool
	quietTo   time.Time
	last      time.Time
}

type openTurn struct {
	id         string
	start      int64
	startLine  int64
	skip       bool
	users      []string
	final      string
	lastAgent  string
	finalPhase bool
}

// NewTracker starts reading at off (line number line). threadID may be
// empty; it is then taken from the session_meta record. skip reports the
// client ids of turns that must not be mirrored.
func NewTracker(threadID string, off, line int64, skip func(clientID string) bool) *Tracker {
	if skip == nil {
		skip = func(string) bool { return false }
	}
	return &Tracker{threadID: threadID, skip: skip, off: off, line: line}
}

// ThreadID returns the session id (known after session_meta or from NewTracker).
func (t *Tracker) ThreadID() string { return t.threadID }

// Offset is the next read position.
func (t *Tracker) Offset() int64 { return t.off }

// Resume returns the position to persist: the start of the open turn, or
// the read position.
func (t *Tracker) Resume() (offset, line int64) {
	if t.turn != nil {
		return t.turn.start, t.turn.startLine
	}
	return t.off, t.line
}

// LastTime is the timestamp of the newest record fed.
func (t *Tracker) LastTime() time.Time { return t.last }

// Realign restarts from the beginning of a rewritten file. Records written
// at or before upTo were already processed; they are re-read only to
// rebuild the open turn's state and produce no output (realignment by the
// last delivered record's time, never blocking on a missing record).
func (t *Tracker) Realign(upTo time.Time) {
	t.off, t.line, t.turn = 0, 0, nil
	t.quiet, t.quietTo = true, upTo
}

// Advance consumes bytes that were skipped without lines (alignment).
func (t *Tracker) Advance(next int64) {
	if next > t.off {
		t.off = next
	}
}

// Feed processes lines read from the current offset and returns the
// outputs they complete, in file order.
func (t *Tracker) Feed(lines []Line) []Output {
	var out []Output
	for _, l := range lines {
		if l.End > t.off {
			t.off = l.End
		}
		t.line++
		if l.Oversize {
			continue
		}
		ev := Parse(l.Data)
		if !ev.Time.IsZero() && ev.Time.After(t.last) {
			t.last = ev.Time
		}
		emit := !t.quiet || (!ev.Time.IsZero() && ev.Time.After(t.quietTo))
		if t.quiet && emit {
			t.quiet = false
		}
		out = append(out, t.apply(ev, l, emit)...)
	}
	return out
}

func (t *Tracker) apply(ev Event, l Line, emit bool) []Output {
	if ev.ThreadID != "" && t.threadID != "" && ev.Kind != KindSessionMeta && ev.ThreadID != t.threadID {
		return nil // a subagent's item inside this rollout
	}
	switch ev.Kind {
	case KindSessionMeta:
		if t.threadID == "" {
			t.threadID = ev.Meta.ID
		}
	case KindTurnStarted:
		t.begin(ev.TurnID, l)
	case KindUserMessage:
		if t.turn == nil || (ev.TurnID != "" && ev.TurnID != t.turn.id) {
			t.begin(ev.TurnID, l)
		}
		tr := t.turn
		if ev.ClientID != "" && t.skip(ev.ClientID) {
			tr.skip = true
		}
		text := strings.TrimSpace(ev.Text)
		if tr.skip || text == "" || IsBoilerplate(text) {
			return nil
		}
		for _, u := range tr.users {
			if u == text {
				return nil // the same prompt in the legacy and the current record
			}
		}
		tr.users = append(tr.users, text)
		if !emit {
			return nil
		}
		return []Output{{Key: fmt.Sprintf("%s:%s:u%d", t.threadID, tr.id, len(tr.users)), User: true, Text: text, Time: ev.Time, TurnID: tr.id}}
	case KindAgentMessage:
		tr := t.turn
		if tr == nil || (ev.TurnID != "" && ev.TurnID != tr.id) {
			return nil
		}
		if strings.TrimSpace(ev.Text) == "" {
			return nil
		}
		tr.lastAgent = ev.Text
		if ev.Phase == "final_answer" {
			tr.final, tr.finalPhase = ev.Text, true
		}
	case KindTurnComplete:
		tr := t.turn
		if tr == nil || (ev.TurnID != "" && ev.TurnID != tr.id) {
			t.turn = nil
			return nil
		}
		t.turn = nil
		text := tr.final
		if !tr.finalPhase {
			text = ev.Text
			if strings.TrimSpace(text) == "" {
				text = tr.lastAgent
			}
		}
		if tr.skip || strings.TrimSpace(text) == "" || !emit {
			return nil
		}
		return []Output{{Key: fmt.Sprintf("%s:%s:f", t.threadID, tr.id), Text: strings.TrimSpace(text), Time: ev.Time, TurnID: tr.id}}
	case KindTurnAborted:
		if t.turn != nil && (ev.TurnID == "" || ev.TurnID == t.turn.id) {
			t.turn = nil
		}
	}
	return nil
}

func (t *Tracker) begin(turnID string, l Line) {
	if turnID == "" {
		turnID = fmt.Sprintf("o%d", l.Off)
	}
	t.turn = &openTurn{id: turnID, start: l.Off, startLine: t.line - 1}
}
