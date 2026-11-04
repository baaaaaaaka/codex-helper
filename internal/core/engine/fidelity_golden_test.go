package engine

// Real-data fidelity harness (the new-core side of the golden diff in
// scripts/fidelity). Skipped unless FIDELITY_MODE is set; it reads the
// owner's rollouts read-only and writes one JSONL file.
//
//	FIDELITY_MODE=transcript FIDELITY_IN=sources.json FIDELITY_OUT=new-transcript.jsonl FIDELITY_NOW=<RFC3339>
//	FIDELITY_MODE=outbox     FIDELITY_IN=outbox.jsonl FIDELITY_OUT=new-outbox.jsonl
//
// transcript: every tracking source is read from its converted position
// with the engine's Tracker and each output is rendered with renderMirror
// and the real Teams adapter (no math runtime: formulas fall back to code).
// outbox: the body of every message the old helper sent is rendered the way
// the new core renders the same kind of message.

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

type fidelityTokens struct{}

func (fidelityTokens) Token(context.Context) (string, error)           { return "t", nil }
func (fidelityTokens) Refresh(context.Context, string) (string, error) { return "t", nil }

type fidelityPart struct {
	Part  int    `json:"part"`
	Parts int    `json:"parts"`
	HTML  string `json:"html"`
}

func fidelityParts(msgs []store.OutMsg) []fidelityPart {
	var out []fidelityPart
	for _, m := range msgs {
		out = append(out, fidelityPart{Part: m.Part, Parts: m.Parts, HTML: m.Body})
	}
	return out
}

func TestFidelityGolden(t *testing.T) {
	mode := os.Getenv("FIDELITY_MODE")
	if mode == "" {
		t.Skip("real-data fidelity harness: set FIDELITY_MODE, FIDELITY_IN and FIDELITY_OUT")
	}
	ad, err := teams.New(teams.Config{Tokens: fidelityTokens{}})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{ch: ad}
	out, err := os.Create(os.Getenv("FIDELITY_OUT"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, 1<<20)
	defer w.Flush()
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	switch mode {
	case "transcript":
		fidelityTranscript(t, e, enc)
	case "outbox":
		fidelityOutbox(t, e, enc)
	default:
		t.Fatalf("FIDELITY_MODE %q", mode)
	}
}

func fidelityTranscript(t *testing.T, e *Engine, enc *json.Encoder) {
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
	now, err := time.Parse(time.RFC3339, os.Getenv("FIDELITY_NOW"))
	if err != nil {
		t.Fatal(err)
	}
	teamsTurn := map[string]bool{}
	for _, id := range in.TeamsTurnIDs {
		teamsTurn[id] = true
	}
	// The engine's mirrorSkip: helper client ids, or the Codex turn ids of
	// the converted turns.
	skip := func(clientID, turnID string) bool {
		return strings.HasPrefix(clientID, "cxp-") || (turnID != "" && teamsTurn[turnID])
	}
	_ = now // no catch-up window any more: every record is mirrored
	ctx := context.Background()
	for _, s := range in.Sources {
		tr := transcript.NewTracker(s.Thread, s.Offset, s.Line, skip)
		fh, err := os.Open(s.Path)
		if err != nil {
			_ = enc.Encode(map[string]any{"conv": s.Conv, "path": s.Path, "error": err.Error()})
			continue
		}
		for {
			lines, next, err := transcript.Read(fh, tr.Offset(), mirrorChunk)
			if err != nil {
				t.Fatalf("%s: %v", s.Path, err)
			}
			if len(lines) == 0 {
				tr.Advance(next)
				break
			}
			for _, o := range tr.Feed(lines) {
				rec := map[string]any{"conv": s.Conv, "path": s.Path, "end": o.End, "record_id": o.RecordID,
					"user": o.User, "turn": o.TurnID, "ts": o.Time.UTC().Format(time.RFC3339Nano), "text": o.Text}
				thread := tr.ThreadID()
				if thread == "" {
					thread = s.Thread
				}
				msgs, err := e.renderMirror(ctx, thread, o)
				if err != nil {
					rec["error"] = err.Error()
				} else {
					rec["parts"] = fidelityParts(msgs)
				}
				if err := enc.Encode(rec); err != nil {
					t.Fatal(err)
				}
			}
			tr.Advance(next)
		}
		fh.Close()
		off, line := tr.Resume()
		_ = enc.Encode(map[string]any{"conv": s.Conv, "path": s.Path, "eof": true, "resume": off, "resume_line": line})
	}
}

// fidelityDoc is the document the new core posts for an old message kind
// with the same text (ok false: the new core has no such message).
func fidelityDoc(kind, body string) (channel.RichDoc, string, bool) {
	k := kind
	switch {
	case strings.HasPrefix(k, "sync-user-"), strings.HasPrefix(k, "import-user-"), strings.HasPrefix(k, "publish-full-user-"), strings.HasPrefix(k, "fork-history-user"):
		return userDoc(strings.TrimSpace(body)), store.OutMirror, true
	case strings.HasPrefix(k, "sync-assistant-"), strings.HasPrefix(k, "import-assistant-"), strings.HasPrefix(k, "publish-full-assistant-"), strings.HasPrefix(k, "fork-history-assistant"):
		return answerDoc(body), store.OutMirror, true
	case k == "final" || strings.HasPrefix(k, "final-"):
		return answerDoc(body), store.OutFinal, true
	case strings.HasPrefix(k, "codex-progress-"), strings.HasPrefix(k, "codex-compact-"):
		return statusDoc(body), store.OutProgress, true
	case strings.HasPrefix(k, "sync-status-source-rewritten"):
		return channel.RichDoc{}, "", false // no such notice in the new core
	case strings.HasPrefix(k, "sync-status-"), strings.HasPrefix(k, "sync-compact-"):
		return statusDoc(body), store.OutMirror, true
	case strings.Contains(k, "-batch-"),
		strings.HasPrefix(k, "import-status"), strings.HasPrefix(k, "import-compact"), strings.HasPrefix(k, "publish-full-status"),
		strings.HasPrefix(k, "publish-full-compact"), strings.HasPrefix(k, "fork-history-status"):
		return channel.RichDoc{}, "", false // not mirrored / not batched by the new core
	case strings.HasPrefix(k, "ack"):
		return channel.RichDoc{Label: labelHelper, Markdown: body}, store.OutAck, true
	case k == "error", strings.Contains(k, "interrupted"), strings.Contains(k, "failed"), strings.Contains(k, "tool"), strings.Contains(k, "artifact"):
		// v0.1.20 renderKindForOutbox: Codex status.
		return channel.RichDoc{Label: labelStatus, Markdown: body}, store.OutNotice, true
	}
	return channel.RichDoc{Label: labelHelper, Markdown: body}, store.OutNotice, true
}

func fidelityOutbox(t *testing.T, e *Engine, enc *json.Encoder) {
	f, err := os.Open(os.Getenv("FIDELITY_IN"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type row struct {
		ID        string `json:"id"`
		TurnID    string `json:"turn_id"`
		Chat      string `json:"teams_chat_id"`
		Kind      string `json:"kind"`
		Body      string `json:"body"`
		PartIndex int    `json:"part_index"`
		PartCount int    `json:"part_count"`
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<20), 64<<20)
	ctx := context.Background()
	// Multi-part messages are re-joined (parts in order) and rendered once.
	groups := map[string][]row{}
	var order []string
	for sc.Scan() {
		var r row
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.PartCount <= 1 {
			fidelityRender(ctx, t, e, enc, []string{r.ID}, r.Kind, r.Body)
			continue
		}
		base := r.Kind
		if i := strings.LastIndexByte(base, '-'); i > 0 && len(base)-i == 4 {
			base = base[:i]
		}
		g := r.Chat + "\x00" + r.TurnID + "\x00" + base
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	for _, g := range order {
		rs := groups[g]
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].PartIndex < rs[j].PartIndex })
		var ids, bodies []string
		for _, r := range rs {
			ids = append(ids, r.ID)
			bodies = append(bodies, r.Body)
		}
		base := strings.Split(g, "\x00")[2]
		fidelityRender(ctx, t, e, enc, ids, base, strings.Join(bodies, "\n"))
	}
}

func fidelityRender(ctx context.Context, t *testing.T, e *Engine, enc *json.Encoder, ids []string, kind, body string) {
	rec := map[string]any{"ids": ids, "kind": kind}
	doc, newKind, ok := fidelityDoc(kind, body)
	if !ok {
		rec["new_kind"] = "none"
	} else {
		rec["new_kind"] = newKind
		msgs, err := e.render(ctx, doc, newKind, "x")
		if err != nil {
			rec["error"] = err.Error()
		} else {
			rec["parts"] = fidelityParts(msgs)
		}
	}
	if err := enc.Encode(rec); err != nil {
		t.Fatal(err)
	}
}
