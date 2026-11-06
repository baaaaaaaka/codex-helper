package migrate

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Resume positions (STORE-DESIGN §7.2, owner decision 2026-11-04). The old
// core had two position producers, import checkpoints and the history
// watcher, and neither is proof of delivery: the watcher could scan without
// ever delivering (finding F1). A tracked rollout therefore resumes right
// after the last record a delivery ledger proves handled
// (transcript_deliveries, transcript_ledger). A checkpoint or watcher
// position is used instead only when no prompt or final that the mirror
// would post lies between that proof and the position. Records without
// proof are mirrored again: duplicates are possible, loss is not.

// MirrorKey is the outbox delivery key of one part of a mirrored rollout
// record. Old and new code can both compute it from Codex identities: the
// thread id and the record's response-item id (or, for a record without an
// id, "t:<codex turn id>:<prompt|final>"). A record counts as delivered when
// any key with the prefix MirrorKeyPrefix(thread, record) exists.
func MirrorKey(threadID, recordID string, part int) string {
	return MirrorKeyPrefix(threadID, recordID) + strconv.Itoa(part)
}

// MirrorKeyPrefix is the per-record prefix of MirrorKey.
func MirrorKeyPrefix(threadID, recordID string) string {
	return "mirror:" + threadID + ":" + recordID + ":"
}

// codexRecordID reports whether a legacy source_record_id is a Codex record
// identity (not a legacy line fallback and not a batch range "a..b").
func codexRecordID(id string) bool {
	return id != "" && !strings.Contains(id, "..") && !strings.HasPrefix(id, "fallback:") && !strings.ContainsAny(id, " \t\n")
}

// proof is the furthest delivery-ledger evidence for one rollout path.
type proof struct {
	endOff      int64 // end offset of the furthest delivered/handled record
	endLine     int64
	fromSkipped bool // that furthest entry is a "skipped" delivery (handled, not posted)
	entries     int
	ledgerLines map[int64]bool  // transcript_ledger lines (1-based)
	ledgerIDs   map[string]bool // Codex record ids proven handled (both ledgers)
}

func (c *converter) proofOf(path string) *proof {
	p := c.proofs[path]
	if p == nil {
		p = &proof{ledgerLines: map[int64]bool{}, ledgerIDs: map[string]bool{}}
		c.proofs[path] = p
	}
	return p
}

// scanProofs reads the delivery ledgers once: per-path proofs for the resume
// rule, and the mirror key of every delivered outbox row so per-record
// dedupe works across the migration boundary.
func (c *converter) scanProofs() error {
	err := c.l.each("transcript_deliveries", func(r row) error {
		var d legacyDelivery
		if _, err := decode(r.JSON, &d); err != nil || d.SourcePath == "" {
			return nil // counted by convertDeliveries
		}
		status := firstNonEmpty(d.Status, r.Status)
		if status != "sent" && status != "skipped" {
			return nil
		}
		p := c.proofOf(d.SourcePath)
		p.entries++
		if d.SourceOffset > p.endOff {
			p.endOff, p.endLine, p.fromSkipped = d.SourceOffset, d.SourceLine, status == "skipped"
		}
		// The record id also proves the record when the file was rewritten
		// and the offset no longer points at a record end.
		if codexRecordID(d.SourceRecordID) {
			p.ledgerIDs[d.SourceRecordID] = true
		}
		if status == "sent" && d.OutboxID != "" && d.CodexThreadID != "" && codexRecordID(d.SourceRecordID) {
			c.outboxMirror[d.OutboxID] = MirrorKey(d.CodexThreadID, d.SourceRecordID, max(d.PartIndex, 1))
		}
		return nil
	})
	if err != nil {
		return err
	}
	e := c.rep.E("transcript_ledger")
	return c.l.each("transcript_ledger", func(r row) error {
		e.Source++
		var lg legacyLedger
		if _, err := decode(r.JSON, &lg); err != nil {
			e.Failed = append(e.Failed, r.ID)
			return nil
		}
		if lg.SourcePath == "" {
			e.Outcomes["no source path -> dropped"]++
			return nil
		}
		p := c.proofOf(lg.SourcePath)
		p.entries++
		if lg.SourceLine > 0 {
			p.ledgerLines[lg.SourceLine] = true
		}
		if codexRecordID(lg.SourceRecordID) {
			p.ledgerIDs[lg.SourceRecordID] = true
		}
		c.ledger = append(c.ledger, lg)
		return nil
	})
}

// mirrorRec is a rollout record the new mirror would post: a user prompt or
// a final answer of a turn that was not run from Teams.
type mirrorRec struct {
	Start, End, Line int64
	Kind             string // "prompt" or "final"
	ID               string
	Key              string // echoKey of its text (echo.go)
	At               int64  // its rollout timestamp (unix ms; 0: unknown)
}

type scanReq struct {
	path      string
	from      int64 // a record boundary
	fromLine  int64
	to        int64 // scan at least up to this offset (capped at EOF)
	toLine    int64 // and at least up to this line
	offsets   []int64
	lines     map[int64]bool
	ids       map[string]bool
	teamsTurn map[string]bool // Codex turn ids run from Teams (read-only)
}

type scanResult struct {
	Bytes    int64
	EndOff   int64
	EndLine  int64
	Recs     []mirrorRec
	Boundary map[int64]int64     // requested offsets that are record ends -> line
	LineEnd  map[int64]int64     // requested ledger lines -> end offset
	IDEnd    map[string][2]int64 // requested ledger record ids -> {end offset, line}
	Err      error
}

// maxScanRecord bounds the bytes kept for one record; a longer record is
// counted but not classified (its length still advances the offset).
const maxScanRecord = 64 << 20

var (
	jTurnContext = []byte(`"type":"turn_context"`)
	jTaskStarted = []byte(`"type":"task_started"`)
	jTaskDone    = []byte(`"type":"task_complete"`)
	jRespItem    = []byte(`"type":"response_item"`)
	jMessage     = []byte(`"type":"message"`)
)

// scanRollout streams [from, to) of a rollout read-only and classifies the
// records the mirror would post.
func scanRollout(q scanReq) scanResult {
	res := scanResult{Boundary: map[int64]int64{}, LineEnd: map[int64]int64{}, IDEnd: map[string][2]int64{}}
	f, err := os.Open(q.path)
	if err != nil {
		res.Err = err
		return res
	}
	defer f.Close()
	if _, err := f.Seek(q.from, io.SeekStart); err != nil {
		res.Err = err
		return res
	}
	want := map[int64]bool{}
	for _, o := range q.offsets {
		want[o] = true
	}
	off, line := q.from, q.fromLine
	if want[off] {
		res.Boundary[off] = line
	}
	cl := classifier{teams: q.teamsTurn}
	if q.from > 0 {
		cl.twin = deliveredEventFinal(f, q.from)
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var buf []byte
	var recLen int64
	big := false
	for off < q.to || line < q.toLine {
		chunk, err := br.ReadSlice('\n')
		res.Bytes += int64(len(chunk))
		recLen += int64(len(chunk))
		if errors.Is(err, bufio.ErrBufferFull) {
			if !big && len(buf)+len(chunk) <= maxScanRecord {
				buf = append(buf, chunk...)
			} else {
				big, buf = true, nil
			}
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			res.Err = err
			break
		}
		if len(chunk) == 0 || chunk[len(chunk)-1] != '\n' {
			break // EOF; a partial last line is not a record yet
		}
		rec := chunk
		if len(buf) > 0 {
			buf = append(buf, chunk...)
			rec = buf
		}
		start := off
		off += recLen
		line++
		if !big {
			cl.classify(rec, start, off, line)
			if len(q.ids) > 0 {
				// The old core names a repeated id's later copies
				// "<id>#line:N", so a bare ledger id proves only its first
				// occurrence.
				if id := recordID(rec); id != "" && q.ids[id] {
					if _, seen := res.IDEnd[id]; !seen {
						res.IDEnd[id] = [2]int64{off, line}
					}
				}
			}
		}
		if want[off] {
			res.Boundary[off] = line
		}
		if q.lines[line] {
			res.LineEnd[line] = off
		}
		buf, recLen, big = buf[:0], 0, false
		if err != nil {
			break
		}
	}
	cl.flush()
	res.Recs = cl.out
	res.EndOff, res.EndLine = off, line
	return res
}

// classifier follows turns through a rollout. Records are held per turn and
// dropped when the turn turns out to be run from Teams; a scan that starts
// inside a turn learns the turn id from its task_complete.
type classifier struct {
	teams map[string]bool
	// twin is the text of the event_msg agent_message final that ends at
	// the scan start (the delivered record of the proof): Codex writes the
	// same final again as the response_item that follows it, which is then
	// not a record to mirror (fidelity run, s278).
	twin      string
	turn      string
	teamsTurn bool
	finalSeen bool
	pending   []mirrorRec
	out       []mirrorRec
}

// setTurn moves to turn id; ends marks a task_complete (the end of a turn).
func (c *classifier) setTurn(id string, ends bool) {
	if id == "" || id == c.turn {
		return
	}
	if c.turn == "" && ends {
		// a scan that started mid-turn: the records so far belong to this turn
		c.turn, c.teamsTurn = id, c.teamsTurn || c.teams[id]
		return
	}
	c.flush()
	c.turn, c.teamsTurn, c.finalSeen, c.twin = id, c.teams[id], false, ""
}

// flush keeps the held records unless their turn was run from Teams.
func (c *classifier) flush() {
	if !c.teamsTurn {
		for _, r := range c.pending {
			if r.ID == "" {
				r.ID = turnRecordID(c.turn, r.Kind, r.Line)
			}
			c.out = append(c.out, r)
		}
	}
	c.pending = c.pending[:0]
}

// turnRecordID is the record id of a prompt or final without a response-item
// id: "t:<codex turn id>:<kind>:<line>", as in the mirror. A final carries
// its line: a turn can write several final_answer items (fidelity run, s698).
func turnRecordID(turn, kind string, line int64) string {
	if turn == "" {
		turn = "?"
	}
	return "t:" + turn + ":" + kind + ":" + strconv.FormatInt(line, 10)
}

type rolloutLine struct {
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Payload   struct {
		Type             string  `json:"type"`
		ID               string  `json:"id"`
		Role             string  `json:"role"`
		Phase            string  `json:"phase"`
		TurnID           string  `json:"turn_id"`
		LastAgentMessage *string `json:"last_agent_message"`
		Content          []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"payload"`
}

func (c *classifier) classify(rec []byte, start, end, line int64) {
	head := rec[:min(len(rec), 512)]
	isCtx, isStart, isDone := bytes.Contains(head, jTurnContext), bytes.Contains(head, jTaskStarted), bytes.Contains(head, jTaskDone)
	isMsg := bytes.Contains(head, jRespItem) && bytes.Contains(head, jMessage)
	if !isCtx && !isStart && !isDone && !isMsg {
		return
	}
	var l rolloutLine
	if json.Unmarshal(rec, &l) != nil {
		return
	}
	pl := l.Payload
	r := mirrorRec{Start: start, End: end, Line: line}
	if t, err := time.Parse(time.RFC3339Nano, l.Timestamp); err == nil {
		r.At = t.UnixMilli()
	}
	switch {
	case l.Type == "turn_context":
		c.setTurn(pl.TurnID, false)
	case l.Type == "event_msg" && pl.Type == "task_started":
		c.setTurn(pl.TurnID, false)
	case l.Type == "event_msg" && pl.Type == "task_complete":
		// last_agent_message is the final only for a turn whose start was
		// scanned and that had no final_answer item: a scan that starts
		// mid-turn usually starts right after the delivered final_answer.
		started := c.turn != "" && c.turn == pl.TurnID
		c.setTurn(pl.TurnID, true)
		seen := c.finalSeen
		c.finalSeen = true
		if started && !seen && pl.LastAgentMessage != nil && strings.TrimSpace(*pl.LastAgentMessage) != "" {
			r.Kind, r.Key = "final", echoKey(*pl.LastAgentMessage)
			c.pending = append(c.pending, r)
		}
	case l.Type == "response_item" && pl.Type == "message" && pl.Role == "user":
		var sb strings.Builder
		for _, ct := range pl.Content {
			sb.WriteString(ct.Text)
		}
		text := strings.TrimSpace(sb.String())
		switch {
		case text == "" || injectedContext(text):
		case LegacyTeamsPrompt(text):
			c.teamsTurn = true
		default:
			c.twin = ""
			r.Kind, r.ID, r.Key = "prompt", pl.ID, echoKey(text)
			c.pending = append(c.pending, r)
		}
	case l.Type == "response_item" && pl.Type == "message" && pl.Role == "assistant" && pl.Phase == "final_answer":
		c.finalSeen = true
		var sb strings.Builder
		for _, ct := range pl.Content {
			sb.WriteString(ct.Text)
		}
		text := strings.TrimSpace(sb.String())
		if c.twin != "" {
			twin := text == c.twin
			c.twin = ""
			if twin {
				return
			}
		}
		r.Kind, r.ID, r.Key = "final", pl.ID, echoKey(text)
		c.pending = append(c.pending, r)
	}
}

// injectedContext reports user-role items Codex adds itself (environment,
// AGENTS.md, aborted-turn markers); they are not prompts.
func injectedContext(text string) bool {
	return strings.HasPrefix(text, "<") || strings.HasPrefix(text, "# AGENTS.md instructions")
}

// LegacyTeamsPrompt recognises a prompt written by the legacy Teams helper
// by the wrapper text it appended or prepended (rc.59 artifact_handoff.go
// and history_watch.go; earlier versions used the same leads). 4,092 legacy
// turns have no Codex turn id, so the mirror must also recognise them by
// their wrapper. The markers are matched with their surrounding line breaks
// so a local prompt that merely quotes one is not taken for a Teams turn.
func LegacyTeamsPrompt(text string) bool {
	for _, m := range legacyTeamsMarkers {
		if strings.Contains(text, m) {
			return true
		}
	}
	return strings.HasPrefix(text, "[codex-helper-control]") ||
		strings.HasPrefix(text, "You are handling an unrecognized message from the user's Microsoft Teams control chat for codex-helper.")
}

var legacyTeamsMarkers = []string{
	"\n\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.",
	"\n\nIf you need to return generated files or images to the Teams user, write them under this local directory:\n",
	"<codex-helper-control-context",
}

// recordID extracts the record's own payload.id cheaply (ledger id
// matching). Only a direct member of the top-level "payload" object counts:
// a nested id (for example the replacement_history of a "compacted" record,
// which repeats earlier message ids) never names this record.
func recordID(rec []byte) string {
	head := rec[:min(len(rec), 4096)]
	depth, payloadDepth := 0, 0
	var lastKey string
	for i := 0; i < len(head); i++ {
		switch c := head[i]; c {
		case '"':
			j := i + 1
			for j < len(head) && head[j] != '"' {
				if head[j] == '\\' {
					j++
				}
				j++
			}
			if j >= len(head) {
				return ""
			}
			str := head[i+1 : j]
			k := j + 1
			for k < len(head) && (head[k] == ' ' || head[k] == '\t') {
				k++
			}
			if k < len(head) && head[k] == ':' {
				lastKey = string(str)
			} else {
				if payloadDepth > 0 && depth == payloadDepth && lastKey == "id" {
					return string(str)
				}
				lastKey = ""
			}
			i = j
		case '{', '[':
			depth++
			if c == '{' && depth == 2 && lastKey == "payload" && payloadDepth == 0 {
				payloadDepth = 2
			}
			lastKey = ""
		case '}', ']':
			if payloadDepth > 0 && depth == payloadDepth {
				return "" // payload closed without an id
			}
			depth--
			lastKey = ""
		case ',':
			lastKey = ""
		}
	}
	return ""
}

// maxTwinRecord bounds the record read back from a proof offset.
const maxTwinRecord = 1 << 20

// deliveredEventFinal returns the text of the event_msg agent_message
// final_answer record that ends at off ("" when the record there is
// anything else or longer than maxTwinRecord). Read-only pread.
func deliveredEventFinal(f *os.File, off int64) string {
	from := max(0, off-maxTwinRecord)
	b := make([]byte, off-from)
	if n, err := f.ReadAt(b, from); n != len(b) || (err != nil && !errors.Is(err, io.EOF)) {
		return ""
	}
	if len(b) == 0 || b[len(b)-1] != '\n' {
		return ""
	}
	i := bytes.LastIndexByte(b[:len(b)-1], '\n')
	if i < 0 && from > 0 {
		return ""
	}
	rec := b[i+1:]
	var l struct {
		Type    string `json:"type"`
		Payload struct {
			Type    string `json:"type"`
			Phase   string `json:"phase"`
			Message string `json:"message"`
		} `json:"payload"`
	}
	if json.Unmarshal(rec, &l) != nil || l.Type != "event_msg" || l.Payload.Type != "agent_message" || l.Payload.Phase != "final_answer" {
		return ""
	}
	return strings.TrimSpace(l.Payload.Message)
}

// recordBoundary reports whether off is 0 or follows a newline (read-only pread).
func recordBoundary(path string, off int64) bool {
	if off == 0 {
		return true
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	b := make([]byte, 1)
	n, err := f.ReadAt(b, off-1)
	return err == nil && n == 1 && b[0] == '\n'
}

// runScans runs the scans with a small worker pool and records the I/O cost.
func (c *converter) runScans(reqs map[string]scanReq) map[string]scanResult {
	start := time.Now()
	out := make(map[string]scanResult, len(reqs))
	var mu sync.Mutex
	work := make(chan scanReq)
	var wg sync.WaitGroup
	for i := 0; i < min(8, runtime.NumCPU()); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for q := range work {
				r := scanRollout(q)
				mu.Lock()
				out[q.path] = r
				mu.Unlock()
			}
		}()
	}
	keys := make([]string, 0, len(reqs))
	for k := range reqs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		work <- reqs[k]
	}
	close(work)
	wg.Wait()
	for _, r := range out {
		c.rep.Scan.Bytes += r.Bytes
		if r.Err != nil {
			c.rep.Scan.Errors++
		}
	}
	c.rep.Scan.Files = len(reqs)
	c.rep.Scan.Wall = time.Since(start)
	return out
}
