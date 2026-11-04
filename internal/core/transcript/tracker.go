package transcript

import (
	"strings"
	"time"
)

// Output is one record to mirror.
type Output struct {
	RecordID string // response-item id, or "t:<turn>:final" / "t:<turn>:prompt:<line>"
	User     bool   // a user prompt (else the turn's final answer)
	Text     string
	Time     time.Time
	TurnID   string
	End      int64 // end offset of the record
	line     int64 // 1-based line of the record (ids of id-less prompts)
	start    int64
}

// Key returns the delivery key of part (1-based) of o in thread.
func (o Output) Key(threadID string, part int) string { return MirrorKey(threadID, o.RecordID, part) }

// Skip reports whether a turn was run by the helper (and is therefore
// already in the chat): by the client id of its prompt, or by its Codex turn
// id (legacy turns without client id recorded in the store).
type Skip func(clientID, turnID string) bool

// Tracker turns the records of one rollout into mirror outputs: the user
// prompts and the final answer of every turn not run by the helper. A
// turn's records are held until it is known whether the helper ran it (the
// prompt record precedes the item that carries the client id).
//
// The persisted position (Resume) is the start of the open turn, or the
// read position when no turn is open, so a restart re-reads at most the
// open turn and regenerates the same record ids.
type Tracker struct {
	threadID string
	skip     Skip

	off, line int64 // next read position
	turn      *openTurn
	last      time.Time
}

type openTurn struct {
	id        string
	started   bool // its start record was read (not a scan that began mid-turn)
	start     int64
	startLine int64
	teams     bool
	decided   bool // the helper/local decision is known
	finalSeen bool
	pending   []Output
}

// NewTracker starts reading at off (line number line, the count of records
// before off). threadID may be empty; it is then taken from session_meta.
func NewTracker(threadID string, off, line int64, skip Skip) *Tracker {
	if skip == nil {
		skip = func(string, string) bool { return false }
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
	if t.turn != nil && t.turn.started {
		return t.turn.start, t.turn.startLine
	}
	if t.turn != nil && len(t.turn.pending) > 0 {
		// A turn learned mid-scan with held prompts: re-read from the
		// first held record (its line count is approximate, lines are
		// informational only).
		return t.turn.pending[0].start, t.turn.pending[0].line - 1
	}
	return t.off, t.line
}

// Restart rereads a rewritten file from its beginning. The caller drops
// the outputs that were already delivered (by record id) and those before
// the last delivered record (realignment by record id, §3.9).
func (t *Tracker) Restart() {
	t.off, t.line, t.turn = 0, 0, nil
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
		out = t.apply(out, ev, l)
	}
	return out
}

// flush releases the held records of the open turn (dropped when the
// helper ran it).
func (t *Tracker) flush(out []Output) []Output {
	tr := t.turn
	if tr == nil {
		return out
	}
	if !tr.teams {
		for _, o := range tr.pending {
			out = append(out, t.resolve(o))
		}
	}
	tr.pending = nil
	return out
}

func (t *Tracker) apply(out []Output, ev Event, l Line) []Output {
	if ev.ThreadID != "" && t.threadID != "" && ev.Kind != KindSessionMeta && ev.ThreadID != t.threadID {
		return out // a subagent's item inside this rollout
	}
	// Learn the turn of a scan that began mid-turn.
	if ev.TurnID != "" && t.turn == nil && ev.Kind != KindTurnStarted {
		t.turn = &openTurn{id: ev.TurnID, teams: t.skip("", ev.TurnID)}
	}
	if ev.TurnID != "" && t.turn != nil && t.turn.id == "" {
		t.turn.id = ev.TurnID
		t.turn.teams = t.turn.teams || t.skip("", ev.TurnID)
	}
	switch ev.Kind {
	case KindSessionMeta:
		if t.threadID == "" {
			t.threadID = ev.Meta.ID
		}
	case KindTurnStarted:
		if t.turn != nil && ev.TurnID != "" && t.turn.id == ev.TurnID {
			return out // turn_context after task_started
		}
		out = t.end(out)
		t.turn = &openTurn{id: ev.TurnID, started: true, start: l.Off, startLine: t.line - 1,
			teams: ev.TurnID != "" && t.skip("", ev.TurnID)}
	case KindUserMessage:
		text := strings.TrimSpace(ev.Text)
		if text == "" || IsBoilerplate(text) {
			return out
		}
		if t.turn == nil {
			// A prompt without a turn record (very old rollouts): its own turn.
			t.turn = &openTurn{started: true, start: l.Off, startLine: t.line - 1}
		}
		tr := t.turn
		if LegacyTeamsPrompt(text) {
			tr.teams = true
		}
		o := Output{RecordID: ev.RecordID, User: true, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off}
		if tr.decided {
			if !tr.teams {
				out = append(out, t.resolve(o))
			}
		} else {
			tr.pending = append(tr.pending, o)
		}
	case KindUserItem:
		tr := t.turn
		if tr == nil {
			return out
		}
		if ev.ClientID != "" && t.skip(ev.ClientID, tr.id) {
			tr.teams = true
		}
		tr.decided = true
		out = t.flush(out)
	case KindAgentMessage:
		tr := t.turn
		if tr == nil || ev.Phase != "final_answer" || strings.TrimSpace(ev.Text) == "" {
			return out
		}
		tr.finalSeen, tr.decided = true, true
		out = t.flush(out)
		if !tr.teams {
			id := ev.RecordID
			if id == "" {
				id = turnRecordID(tr.id, true, 0)
			}
			out = append(out, Output{RecordID: id, Text: strings.TrimSpace(ev.Text), Time: ev.Time, TurnID: tr.id, End: l.End})
		}
	case KindTurnComplete:
		tr := t.turn
		if tr == nil {
			return out
		}
		tr.decided = true
		out = t.flush(out)
		// last_agent_message is the final only for a turn whose start was
		// read and that had no final_answer item: a scan that began
		// mid-turn usually begins right after the delivered final.
		if tr.started && !tr.finalSeen && !tr.teams && strings.TrimSpace(ev.Text) != "" {
			out = append(out, Output{RecordID: turnRecordID(tr.id, true, 0), Text: strings.TrimSpace(ev.Text), Time: ev.Time, TurnID: tr.id, End: l.End})
		}
		t.turn = nil
	case KindTurnAborted:
		out = t.end(out)
	}
	return out
}

// end closes the open turn, releasing its held prompts.
func (t *Tracker) end(out []Output) []Output {
	if t.turn != nil {
		t.turn.decided = true
		out = t.flush(out)
		t.turn = nil
	}
	return out
}

// resolve fills the turn id and the record id of an id-less record.
func (t *Tracker) resolve(o Output) Output {
	if t.turn != nil {
		o.TurnID = t.turn.id
	}
	if o.RecordID == "" {
		o.RecordID = turnRecordID(o.TurnID, !o.User, o.line)
	}
	return o
}
