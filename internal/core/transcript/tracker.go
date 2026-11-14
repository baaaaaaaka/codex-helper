package transcript

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// OutputKind is what a mirrored record is.
type OutputKind string

const (
	OutputEnd     OutputKind = "end"     // internal durable ordering barrier; never rendered
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
	helperInput    bool
	localPrompt    bool   // an identified native TUI input inside a shared turn
	LegacyRecordID string // line-based key used by older helpers
	RecordID       string
	Kind           OutputKind
	User           bool // Kind == OutputPrompt
	Text           string
	Time           time.Time
	TurnID         string
	End            int64     // end offset of the record
	Info           Info      // the thread's goal and settings when the record was released
	Schedule       *Schedule // evidence for this output's turn only
	line           int64     // 1-based line of the record (ids of id-less records)
	start          int64
}

// Info is what the rollout said last of its thread's goal and model
// settings: the goal (thread_goal_updated), the model and effort
// (turn_context, thread_settings_applied) and the service tier
// (thread_settings_applied; "" = standard).
type Info struct {
	Goal   *Goal  `json:"goal,omitempty"`
	Model  string `json:"model,omitempty"`
	Effort string `json:"effort,omitempty"`
	Tier   string `json:"tier,omitempty"`
}

func (i *Info) update(u *InfoUpdate) {
	if u == nil {
		return
	}
	if u.Goal != nil {
		g := *u.Goal
		i.Goal = &g
	}
	if u.Model != "" {
		i.Model = u.Model
	}
	if u.Effort != nil {
		i.Effort = *u.Effort
	}
	if u.Tier != nil {
		i.Tier = *u.Tier
	}
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
	stableIDs      bool
	threadID       string
	skip           Skip
	helperProgress func(string, string) bool

	off, line int64 // next read position
	turn      *openTurn
	prev      *openTurn    // the last closed turn: a compaction between turns belongs to it
	held      *heldCompact // a compacted record that a context_compacted may still replace
	last      time.Time
	keep      int64 // KeepTeams: helper-run records ending at or before it are mirrored too
	info      Info
}

// Info returns what the records read so far said of the thread's goal and
// settings.
func (t *Tracker) Info() Info { return t.info }

// SetInfo seeds Info for a tracker that starts after the records that
// said it (the mirror persists it with the read position).
func (t *Tracker) SetInfo(i Info) { t.info = i }

// stamp gives the outputs out[from:] the current Info.
func (t *Tracker) stamp(out []Output, from int) {
	for i := from; i < len(out); i++ {
		out[i].Info = t.info
		if !t.stableIDs && out[i].LegacyRecordID != "" {
			out[i].RecordID = out[i].LegacyRecordID
			out[i].LegacyRecordID = ""
		}
	}
}

type openTurn struct {
	schedule        *Schedule
	humanInput      bool
	id              string
	clientID        string
	started         bool // its start record was read (not a scan that began mid-turn)
	start           int64
	startLine       int64
	teams           bool
	inputIdentified bool
	legacyInput     bool
	decided         bool // the helper/local decision is known
	finalSeen       bool
	unphased        *Output // one-message look-behind; bounded independently of turn length
	lastAsync       string  // bounded by the rollout reader's record limit
	// prompted: a prompt record was read; early: decided by a commentary
	// record of a turn without a prompt (a goal continuation, which Codex
	// starts by itself), so a prompt steered into it later waits for its
	// user item again.
	prompted, early bool
	pending         []Output
	fallback        map[OutputKind]int64
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

// KeepTeams mirrors the records of helper-run turns too, up to and
// including the record that ends at until (a history import into a new
// chat, F-MIR-5: those turns were answered in another chat, or by a helper
// that kept no record of them). Their prompts lose the helper's envelope.
func (t *Tracker) KeepTeams(until int64) { t.keep = until }

// StableFallbackIDs uses turn-local occurrences for id-less records, with
// legacy line keys as aliases. Enable for durable mirroring, not old converters.
func (t *Tracker) StableFallbackIDs() { t.stableIDs = true }

// HelperProgress enables only commentary and compactions of executions whose
// runner delegates progress to this reader. Prompts and final artifacts still
// belong to the runner; legacy helper executions remain excluded.
func (t *Tracker) HelperProgress(enabled func(string) bool) {
	t.helperProgress = func(_ string, turn string) bool { return enabled(turn) }
}

// HelperProgressByClient recognizes ownership even before the native started callback.
func (t *Tracker) HelperProgressByClient(enabled func(string, string) bool) {
	t.helperProgress = enabled
}

// kept returns o, a record of a helper-run turn, when KeepTeams keeps it.
func (t *Tracker) kept(o Output, tr *openTurn) (Output, bool) {
	client := ""
	if tr != nil {
		client = tr.clientID
	}
	if (o.Kind == OutputStatus || o.Kind == OutputCompact) && t.helperProgress != nil && t.helperProgress(client, o.TurnID) {
		return o, true
	}
	if o.End > t.keep {
		return o, false
	}
	if o.Kind == OutputPrompt {
		if o.Text = StripHelperPrompt(o.Text); o.Text == "" {
			return o, false
		}
	}
	return o, true
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

// Restart rereads a rewritten file from its beginning. The caller checks
// every output part's durable identity; a later represented record does
// not hide an earlier missing one.
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
		n := len(out)
		out = t.apply(out, ev, l)
		t.stamp(out, n)
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
	for _, o := range tr.pending {
		o = resolve(o, tr)
		if (tr.teams || o.helperInput) && !o.localPrompt {
			var ok bool
			if o, ok = t.kept(o, tr); !ok {
				continue
			}
		}
		out = append(out, resolve(o, tr))
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
	default:
		if k, ok := t.kept(resolve(o, tr), tr); ok {
			out = append(out, resolve(k, tr))
		}
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
	t.info.update(ev.Info)
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
			if ev.Schedule != nil && t.turn.schedule == nil {
				t.turn.schedule = ev.Schedule
			}
			return out // turn_context after task_started
		}
		out = t.end(out)
		t.turn = &openTurn{id: ev.TurnID, started: true, start: l.Off, startLine: t.line - 1,
			teams: ev.TurnID != "" && t.skip("", ev.TurnID), schedule: ev.Schedule}
	case KindUserMessage:
		text := strings.TrimSpace(ev.Text)
		if text == "" || ev.Schedule == nil && IsBoilerplate(text) {
			return out
		}
		if t.turn == nil {
			// A prompt without a turn record (very old rollouts): its own turn.
			t.turn = &openTurn{started: true, start: l.Off, startLine: t.line - 1}
		}
		if ev.Schedule != nil {
			s := *ev.Schedule
			s.Mixed = t.turn.humanInput || t.turn.schedule != nil && (t.turn.schedule.Mixed || t.turn.schedule.ID != "" && t.turn.schedule.ID != s.ID)
			t.turn.schedule = &s
		} else {
			if t.turn.schedule == nil {
				t.turn.humanInput = true
			}
			if t.turn.schedule != nil && t.turn.prompted {
				t.turn.schedule.Mixed = true
			}
		}
		if LegacyTeamsPrompt(text) {
			t.turn.legacyInput = true
		}
		scheduleInput := ev.Schedule != nil || t.turn.schedule != nil && !t.turn.prompted
		t.turn.prompted = true
		t.turn.decided, t.turn.early = false, false
		o := Output{helperInput: LegacyTeamsPrompt(text), RecordID: ev.RecordID, Kind: OutputPrompt, User: true, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off}
		if t.turn.schedule != nil {
			s := *t.turn.schedule
			s.Input = scheduleInput
			o.Schedule = &s
		}
		out = t.add(out, o, t.turn)
	case KindUserItem:
		tr := t.turn
		if tr == nil {
			return out
		}
		if ev.ClientID != "" {
			own := t.skip(ev.ClientID, "")
			steer := strings.HasPrefix(ev.ClientID, "cxp-steer-")
			if !steer && (tr.clientID == "" || own) {
				tr.clientID = ev.ClientID
			}
			if !steer && ((own && !tr.inputIdentified) || tr.legacyInput) {
				tr.teams = true
			}
			tr.legacyInput = false
			tr.inputIdentified = true
			for i := range tr.pending {
				o := &tr.pending[i]
				if o.User {
					o.helperInput = o.helperInput || own
					o.localPrompt = !o.helperInput && !t.skip(ev.ClientID, tr.id)
				}
			}
		}
		tr.decided = true
		out = t.flush(out)
	case KindAsyncMessage:
		if t.turn == nil {
			t.turn = &openTurn{}
		}
		t.turn.lastAsync = strings.TrimSpace(StripMemCitations(ev.Text))
	case KindAgentMessage:
		if t.turn != nil {
			t.turn.lastAsync = ""
		}
		if t.turn != nil && t.turn.legacyInput {
			t.turn.teams = true
		}
		text := strings.TrimSpace(StripMemCitations(ev.Text)) // F-MIR-8
		if text == "" {
			return out
		}
		out = t.flushUnphased(out, false)
		switch ev.Phase {
		case "commentary":
			if t.turn == nil {
				// A scan that began mid-turn before any record naming
				// the turn: held until the turn is known.
				t.turn = &openTurn{}
			}
			if tr := t.turn; tr.started && tr.id != "" && !tr.decided && !tr.prompted {
				// Codex works without a prompt (goal continuation): its
				// status goes out as it comes, not at the turn's end.
				tr.decided, tr.early = true, true
				out = t.flush(out)
			}
			o := Output{RecordID: ev.RecordID, Kind: OutputStatus, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off}
			return t.add(out, o, t.turn)
		case "final_answer":
		case "":
			if !t.stableIDs {
				return out
			} // legacy converter selection stays compatible
			if t.turn == nil {
				t.turn = &openTurn{}
			}
			t.turn.unphased = &Output{RecordID: ev.RecordID, Kind: OutputStatus, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off}
			return out
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
		if !tr.teams || l.End <= t.keep {
			out = append(out, resolve(Output{RecordID: ev.RecordID, Kind: OutputFinal, Text: text, Time: ev.Time, End: l.End, line: t.line, start: l.Off}, tr))
		}
	case KindCompact:
		out = t.flushUnphased(out, false)
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
		out = t.flush(out) // held prompts must precede a last unphased answer
		if tr.unphased != nil {
			final := strings.TrimSpace(StripMemCitations(ev.Text))
			isFinal := final == "" || final == tr.unphased.Text
			out = t.flushUnphased(out, isFinal)
		}
		out = t.flush(out)
		// last_agent_message is the final only for a turn whose start was
		// read and that had no final_answer item: a scan that began
		// mid-turn usually begins right after the delivered final.
		if final := strings.TrimSpace(StripMemCitations(ev.Text)); tr.started && !tr.finalSeen && (!tr.teams || l.End <= t.keep) && final != "" && final != tr.lastAsync {
			out = append(out, resolve(Output{Kind: OutputFinal, Text: final, Time: ev.Time, End: l.End, line: t.line, start: l.Off}, tr))
		}
		out = t.progressEnd(out, tr, l.End)
		t.turn, t.prev = nil, tr
	case KindTurnAborted:
		tr := t.turn
		out = t.end(out)
		out = t.progressEnd(out, tr, l.End)
	}
	return out
}

// Settle decides the open turn when no record will: the rollout stopped
// growing before the engine started, so a turn still waiting for its
// helper/local decision was abandoned mid-way by a Codex that writes no
// user item (fidelity run, s671: its prompt was never mirrored). Its held
// records are mirrored unless the helper ran it (by its Codex turn id or a
// Teams wrapper). A turn learned mid-scan without its start stays held: its
// prompt, and so its wrapper, was not read.
func (t *Tracker) Settle() []Output {
	tr := t.turn
	if tr == nil || tr.decided || !tr.started {
		return nil
	}
	if tr.id != "" && t.skip("", tr.id) {
		tr.teams = true
	}
	tr.decided = true
	out := t.flush(nil)
	t.stamp(out, 0)
	return out
}

// end closes the open turn, releasing its held records.
func (t *Tracker) end(out []Output) []Output {
	out = t.flushUnphased(out, false)
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
		if tr.schedule != nil && o.Kind != OutputPrompt && o.Schedule == nil {
			s := *tr.schedule
			o.Schedule = &s
		}
	}
	if o.RecordID == "" {
		o.RecordID = turnRecordID(o.TurnID, o.Kind, o.line)
		if tr != nil && tr.id != "" && tr.started {
			// Turn-local occurrence, independent of unrelated file line shifts.
			// Repeated identical text remains distinct. Old line keys remain
			// valid aliases, so upgrades never resend already represented parts.
			if tr.fallback == nil {
				tr.fallback = map[OutputKind]int64{}
			}
			tr.fallback[o.Kind]++
			o.LegacyRecordID = o.RecordID
			sum := sha256.Sum256([]byte(o.Text))
			o.RecordID = "t:" + tr.id + ":" + string(o.Kind) + ":o:" + strconv.FormatInt(tr.fallback[o.Kind], 10) + ":" + hex.EncodeToString(sum[:8])
		}
	}
	return o
}

func (t *Tracker) flushUnphased(out []Output, final bool) []Output {
	tr := t.turn
	if tr == nil || tr.unphased == nil {
		return out
	}
	o := *tr.unphased
	tr.unphased = nil
	if final {
		o.Kind = OutputFinal
		tr.finalSeen = true
	}
	return t.add(out, o, tr)
}

func (t *Tracker) progressEnd(out []Output, tr *openTurn, end int64) []Output {
	if tr != nil && t.helperProgress != nil && t.helperProgress(tr.clientID, tr.id) {
		return append(out, Output{Kind: OutputEnd, TurnID: tr.id, End: end})
	}
	return out
}
