package transcript

import (
	"strings"
	"time"
)

// OutputKind is what a mirrored record is.
type OutputKind string

const (
	OutputPrompt  OutputKind = "prompt"  // a user prompt
	OutputFinal   OutputKind = "final"   // the final answer of a turn
	OutputStatus  OutputKind = "status"  // commentary (assistant message, phase commentary)
	OutputCompact OutputKind = "compact" // a context compaction notice (Text is CompactText)
)

// CompactText is the text of a compaction notice (v0.1.20
// transcriptContextCompactMessage).
const CompactText = "Context compacted. Earlier turns were summarized so Codex can continue the thread."

// Output is one record to mirror.
//
// RecordID is the response-item id when the record has one, else
// "t:<turn>:<kind>:<line>" (final, prompt, status, compact; line is the
// 1-based line of the record).
type Output struct {
	RecordID string
	Kind     OutputKind
	User     bool // Kind == OutputPrompt
	Text     string
	Time     time.Time
	TurnID   string
	End      int64 // end offset of the record
	line     int64 // 1-based line of the record (ids of id-less records)
	start    int64
}

// Key returns the delivery key of part (1-based) of o in thread.
func (o Output) Key(threadID string, part int) string { return MirrorKey(threadID, o.RecordID, part) }

// Skip reports whether a turn was run by the helper (and is therefore
// already in the chat): by the client id of its prompt, or by its Codex turn
// id (legacy turns without client id recorded in the store).
type Skip func(clientID, turnID string) bool

// Tracker turns the records of one rollout into mirror outputs, v0.1.20's
// transcript-sync selection: the user prompts, the commentary (status), the
// compaction notices and the final answer of every turn not run by the
// helper, in file order. A turn's records are held until it is known
// whether the helper ran it (the prompt record precedes the item that
// carries the client id).
//
// Prompts, commentary and answers are read from the response_item message
// records only: their event_msg twins (user_message, agent_message) and
// the item_completed items repeat them, and v0.1.20 posted each such pair
// once. A compacted record directly followed by a context_compacted record
// is one compaction: v0.1.20 posted the second (compactTranscriptRecords),
// so a compacted record is held until the next record shows which.
//
// The persisted position (Resume) is the start of the open turn (or of a
// held record), or the read position when nothing is open, so a restart
// re-reads at most the open turn and regenerates the same record ids.
type Tracker struct {
	threadID string
	skip     Skip

	off, line int64 // next read position
	turn      *openTurn
	prev      *openTurn    // the last closed turn: a compaction between turns belongs to it
	held      *heldCompact // a compacted record that a context_compacted may still replace
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

type heldCompact struct {
	o      Output
	turn   *openTurn // the turn it belongs to (nil: none known)
	inTurn bool      // read inside turn (not after it closed)
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

// Resume returns the position to persist: the start of the open turn (or
// of the earliest held record), or the read position.
func (t *Tracker) Resume() (offset, line int64) {
	offset, line = t.off, t.line
	at := func(off, l int64) {
		if off < offset {
			offset, line = off, l
		}
	}
	if tr := t.turn; tr != nil {
		if tr.started {
			at(tr.start, tr.startLine)
		} else if len(tr.pending) > 0 {
			// A turn learned mid-scan with held records: re-read from
			// the first one.
			at(tr.pending[0].start, tr.pending[0].line-1)
		}
	}
	if h := t.held; h != nil {
		if h.inTurn && h.turn.started {
			// From the start of its turn, which may have ended since,
			// so that the turn's decision is read again.
			at(h.turn.start, h.turn.startLine)
		} else {
			// Read between turns (none in the fidelity data): the
			// position does not move back to the closed turn, so after a
			// restart there the compaction belongs to no known turn and
			// is mirrored.
			at(h.o.start, h.o.line-1)
		}
	}
	return offset, line
}

// Restart rereads a rewritten file from its beginning. The caller drops
// the outputs that were already delivered (by record id) and those before
// the last delivered record (realignment by record id, §3.9).
func (t *Tracker) Restart() {
	t.off, t.line, t.turn, t.prev, t.held = 0, 0, nil, nil, nil
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
		ev := ParseLine(l)
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
			out = append(out, resolve(o, tr))
		}
	}
	tr.pending = nil
	return out
}

// add emits o, a record of turn tr, when the helper/local decision of tr is
// known (dropping it when the helper ran the turn), and holds it in tr
// otherwise. tr nil: a record outside any known turn, mirrored.
func (t *Tracker) add(out []Output, o Output, tr *openTurn) []Output {
	switch {
	case tr == nil:
		return append(out, resolve(o, nil))
	case !tr.decided:
		tr.pending = append(tr.pending, o)
	case !tr.teams:
		out = append(out, resolve(o, tr))
	}
	return out
}

// release mirrors the held compacted record: no context_compacted record
// replaced it.
func (t *Tracker) release(out []Output) []Output {
	if h := t.held; h != nil {
		t.held = nil
		out = t.add(out, h.o, h.turn)
	}
	return out
}

func (t *Tracker) apply(out []Output, ev Event, l Line) []Output {
	if ev.ThreadID != "" && t.threadID != "" && ev.Kind != KindSessionMeta && ev.ThreadID != t.threadID {
		return out // a subagent's item inside this rollout
	}
	if t.held != nil && ev.Kind != KindCompact && breaksCompaction(l, ev) {
		out = t.release(out)
	}
	// Learn the turn of a scan that began mid-turn.
	if ev.TurnID != "" && t.turn == nil && ev.Kind != KindTurnStarted {
		t.turn = &openTurn{id: ev.TurnID, teams: t.skip("", ev.TurnID)}
	}
	if ev.TurnID != "" && t.turn != nil && t.turn.id == "" && (t.turn.started || ev.Kind != KindTurnStarted) {
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
		if LegacyTeamsPrompt(text) {
			t.turn.teams = true
		}
		o := Output{RecordID: ev.RecordID, Kind: OutputPrompt, User: true, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off}
		out = t.add(out, o, t.turn)
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
		text := strings.TrimSpace(ev.Text)
		if text == "" {
			return out
		}
		switch ev.Phase {
		case "commentary":
			if t.turn == nil {
				// A scan that began mid-turn before any record naming
				// the turn: held until the turn is known.
				t.turn = &openTurn{}
			}
			o := Output{RecordID: ev.RecordID, Kind: OutputStatus, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off}
			return t.add(out, o, t.turn)
		case "final_answer":
		default:
			return out
		}
		tr := t.turn
		if tr == nil {
			// A scan that began mid-turn before any record naming the turn
			// (older rollouts carry the turn id only on task_complete):
			// hold the final until the turn, and so whether the helper ran
			// it, is known (fidelity run, s523/s526).
			tr = &openTurn{}
			t.turn = tr
		}
		if tr.id == "" && !tr.started {
			// Still held: a later final of the same unknown turn waits too,
			// so each gets the turn id once it is known (s698).
			tr.finalSeen = true
			tr.pending = append(tr.pending, Output{RecordID: ev.RecordID, Kind: OutputFinal, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off})
			return out
		}
		tr.finalSeen, tr.decided = true, true
		out = t.flush(out)
		if !tr.teams {
			id := ev.RecordID
			if id == "" {
				id = turnRecordID(tr.id, OutputFinal, t.line)
			}
			out = append(out, Output{RecordID: id, Kind: OutputFinal, Text: text, Time: ev.Time, TurnID: tr.id, End: l.End, line: t.line, start: l.Off})
		}
	case KindCompact:
		o := Output{Kind: OutputCompact, Text: CompactText, Time: ev.Time, End: l.End, line: t.line, start: l.Off}
		tr := t.turn
		if tr == nil {
			tr = t.prev
		}
		switch {
		case ev.Phase == compactedType:
			out = t.release(out)
			t.held = &heldCompact{o: o, turn: tr, inTurn: tr != nil && tr == t.turn}
			return out
		case ev.Phase == contextCompactedType && t.held != nil:
			t.held = nil // one compaction: v0.1.20 posted this record of the pair
		default:
			out = t.release(out)
		}
		out = t.add(out, o, tr)
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
			out = append(out, Output{RecordID: turnRecordID(tr.id, OutputFinal, t.line), Kind: OutputFinal, Text: strings.TrimSpace(ev.Text), Time: ev.Time, TurnID: tr.id, End: l.End, line: t.line, start: l.Off})
		}
		t.turn, t.prev = nil, tr
	case KindTurnAborted:
		out = t.end(out)
	}
	return out
}

// end closes the open turn, releasing its held records.
func (t *Tracker) end(out []Output) []Output {
	if t.turn != nil {
		t.turn.decided = true
		out = t.flush(out)
		t.turn, t.prev = nil, t.turn
	}
	return out
}

// resolve fills the turn id (of tr) and the record id of an id-less record.
func resolve(o Output, tr *openTurn) Output {
	if tr != nil {
		o.TurnID = tr.id
	}
	if o.RecordID == "" {
		o.RecordID = turnRecordID(o.TurnID, o.Kind, o.line)
	}
	return o
}
