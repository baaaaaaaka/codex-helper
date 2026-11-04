package transcript

// Real-data selection harness (the new-core side of
// scripts/fidelity/select_diff.py). Skipped unless FIDELITY_MODE=select; it
// reads the owner's rollouts read-only and writes one JSONL file:
//
//	FIDELITY_MODE=select FIDELITY_IN=sources.json FIDELITY_OUT=new-select.jsonl [FIDELITY_RESTART=1]
//
// Every tracking source is read from its converted position with a Tracker
// whose skip is the engine's mirrorSkip (helper client ids, or the Codex
// turn ids of the converted turns), and each output is written as one line:
// conv, path, end offset, line, record id, kind, turn and a hash of its text
// with whitespace removed; the end-of-source line lists the turns the
// tracker took for helper turns. No time window applies (the v0.1.20 golden
// ran without one). With FIDELITY_RESTART each source is read a second time
// with a restart (a new tracker from Resume) after every chunk, and the
// record ids delivered must be those of the uninterrupted read.

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
	"unicode"
)

// selectTextHash is the hash select_diff.py computes for a text: sha256 of
// the text without whitespace, first 16 hex digits.
func selectTextHash(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}

func TestFidelitySelect(t *testing.T) {
	if os.Getenv("FIDELITY_MODE") != "select" {
		t.Skip("real-data selection harness: set FIDELITY_MODE=select, FIDELITY_IN and FIDELITY_OUT")
	}
	var in struct {
		Sources []struct {
			Conv, Path, Thread string
			Offset, Line       int64
		}
		TeamsTurnIDs []string `json:"teams_turn_ids"`
	}
	raw, err := os.ReadFile(os.Getenv("FIDELITY_IN"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		t.Fatal(err)
	}
	teamsTurn := map[string]bool{}
	for _, id := range in.TeamsTurnIDs {
		teamsTurn[strings.TrimSpace(id)] = true
	}
	skip := func(clientID, turnID string) bool {
		return strings.HasPrefix(clientID, "cxp-") || (turnID != "" && teamsTurn[turnID])
	}
	f, err := os.Create(os.Getenv("FIDELITY_OUT"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, s := range in.Sources {
		tr := NewTracker(s.Thread, s.Offset, s.Line, skip)
		fh, err := os.Open(s.Path)
		if err != nil {
			_ = enc.Encode(map[string]any{"conv": s.Conv, "path": s.Path, "error": err.Error()})
			continue
		}
		n, between := 0, 0
		var ids []string
		helperTurns := map[string]bool{}
		for {
			lines, next, err := Read(fh, tr.Offset(), 4<<20)
			if err != nil {
				t.Fatalf("%s: %v", s.Path, err)
			}
			if len(lines) == 0 {
				tr.Advance(next)
				break
			}
			lineAt := make(map[int64]int64, len(lines))
			for k, l := range lines {
				lineAt[l.End] = tr.line + int64(k) + 1
			}
			// Line by line, to note the turns the tracker took for helper
			// turns (client id, turn id or the legacy wrapper) and the
			// compactions read outside any turn.
			for _, l := range lines {
				if tr.turn == nil && !l.Oversize && ParseLine(l).Kind == KindCompact {
					between++
				}
				outs := tr.Feed([]Line{l})
				for _, tt := range []*openTurn{tr.turn, tr.prev} {
					if tt != nil && tt.teams && tt.id != "" {
						helperTurns[tt.id] = true
					}
				}
				for _, o := range outs {
					n++
					ids = append(ids, o.RecordID)
					ln, ok := lineAt[o.End]
					if !ok {
						ln = o.line // held from an earlier chunk
					}
					rec := map[string]any{"conv": s.Conv, "path": s.Path, "end": o.End, "line": ln,
						"record_id": o.RecordID, "kind": string(o.Kind), "turn": o.TurnID, "hash": selectTextHash(o.Text)}
					if err := enc.Encode(rec); err != nil {
						t.Fatal(err)
					}
				}
			}
			tr.Advance(next)
		}
		fh.Close()
		off, line := tr.Resume()
		var ht []string
		for id := range helperTurns {
			ht = append(ht, id)
		}
		sort.Strings(ht)
		eof := map[string]any{"conv": s.Conv, "path": s.Path, "eof": true, "outputs": n, "resume": off, "resume_line": line,
			"held_compact": tr.held != nil, "compact_between_turns": between, "helper_turns": ht}
		if os.Getenv("FIDELITY_RESTART") != "" {
			got, restarts := fidelityRestartIDs(t, s.Path, s.Thread, s.Offset, s.Line, skip)
			eof["restarts"], eof["restart_ids_equal"] = restarts, strings.Join(got, "\n") == strings.Join(ids, "\n")
			if !eof["restart_ids_equal"].(bool) {
				t.Errorf("%s: record ids differ after restarts (%d vs %d outputs)", s.Conv, len(got), len(ids))
			}
		}
		if err := enc.Encode(eof); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

// fidelityRestartIDs reads a source the way the engine does across
// restarts: after every chunk the tracker is replaced by a new one from the
// persisted position (Resume), and outputs already seen (delivered) are
// dropped by record id. It returns the record ids in delivery order.
func fidelityRestartIDs(t *testing.T, path, thread string, off, line int64, skip Skip) ([]string, int) {
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	seen := map[string]bool{}
	var ids []string
	restarts := 0
	for {
		tr := NewTracker(thread, off, line, skip)
		lines, next, err := Read(fh, tr.Offset(), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) == 0 {
			return ids, restarts
		}
		for _, o := range tr.Feed(lines) {
			if !seen[o.RecordID] {
				seen[o.RecordID] = true
				ids = append(ids, o.RecordID)
			}
		}
		tr.Advance(next)
		noff, nline := tr.Resume()
		if noff <= off && tr.Offset() > off {
			// The open turn is longer than a chunk: carry on with this
			// tracker until its turn ends, then restart there.
			for noff <= off {
				lines, next, err = Read(fh, tr.Offset(), 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				if len(lines) == 0 {
					return ids, restarts
				}
				for _, o := range tr.Feed(lines) {
					if !seen[o.RecordID] {
						seen[o.RecordID] = true
						ids = append(ids, o.RecordID)
					}
				}
				tr.Advance(next)
				noff, nline = tr.Resume()
			}
		}
		off, line = noff, nline
		restarts++
	}
}
