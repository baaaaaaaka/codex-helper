package migrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const s001Thread = "11111111-1111-4111-8111-000000000001"

// rollout builds a Codex rollout and remembers each line's end offset.
type rollout struct {
	b    strings.Builder
	ends []int64 // ends[n] = end offset of line n (1-based; ends[0] = 0)
	ts   string  // timestamp of the next lines
}

func newRollout() *rollout { return &rollout{ends: []int64{0}, ts: "2029-12-31T00:00:00Z"} }

func (r *rollout) add(v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	r.b.Write(raw)
	r.b.WriteByte('\n')
	r.ends = append(r.ends, int64(r.b.Len()))
}

func (r *rollout) line(typ string, payload map[string]any) {
	r.add(map[string]any{"timestamp": r.ts, "type": typ, "payload": payload})
}

func (r *rollout) turn(id string) {
	r.line("turn_context", map[string]any{"turn_id": id})
}

func (r *rollout) user(id, text string) {
	r.line("response_item", map[string]any{"type": "message", "id": id, "role": "user", "content": []any{map[string]any{"type": "input_text", "text": text}}})
}

func (r *rollout) final(id, text string) {
	r.line("response_item", map[string]any{"type": "message", "id": id, "role": "assistant", "phase": "final_answer", "content": []any{map[string]any{"type": "output_text", "text": text}}})
}

func (r *rollout) done(turn, last string) {
	r.line("event_msg", map[string]any{"type": "task_complete", "turn_id": turn, "last_agent_message": last})
}

const teamsWrapper = "\n\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.\n- Do not restart the helper."

// sampleRollout: a local turn (lines 2-5), a Teams turn recognised by its
// wrapper (6-9), a local turn (10-13), an AGENTS.md injection (14).
func sampleRollout() *rollout {
	r := newRollout()
	r.line("session_meta", map[string]any{"id": s001Thread, "originator": "codex_cli_rs", "source": "cli"})
	r.turn("ta")
	r.user("u1", "hello local")
	r.final("f1", "local answer")
	r.done("ta", "local answer")
	r.turn("tb")
	r.user("u2", "from teams"+teamsWrapper)
	r.final("f2", "teams answer")
	r.done("tb", "teams answer")
	r.turn("tc")
	r.user("u3", "second local")
	r.final("f3", "second answer")
	r.done("tc", "second answer")
	r.user("u4", "# AGENTS.md instructions for /work")
	return r
}

func writeRollout(t *testing.T, r *rollout) (sessions, path string) {
	t.Helper()
	sessions = filepath.Join(t.TempDir(), "sessions")
	day := filepath.Join(sessions, "2029", "12", "31")
	if err := os.MkdirAll(day, 0o700); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(day, "rollout-2029-12-31T00-00-00-"+s001Thread+".jsonl")
	if err := os.WriteFile(path, []byte(r.b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return sessions, path
}

// positions points s001's checkpoint and history_watch entry at path and
// sets the transcript_deliveries ledger.
func positions(path string, ckpt, ckptLine, watch, watchLine int64, deliveries map[string]any) func(map[string]any) {
	return func(m map[string]any) {
		cp := m["import_checkpoints"].(map[string]any)["transcript:s001"].(map[string]any)
		cp["source_path"], cp["last_offset"], cp["last_source_line"], cp["source_size"] = path, ckpt, ckptLine, ckpt
		m["history_watch"] = map[string]any{"w1": map[string]any{"id": "w1", "path": path, "thread_id": s001Thread, "offset": watch, "line": watchLine}}
		m["transcript_deliveries"] = deliveries
	}
}

// Owner decision 2026-11-04: resume right after the last record a delivery
// ledger proves, unless an old position lies further with nothing the mirror
// would post in between; records in the gap are mirrored again.
func TestProofBasedResume(t *testing.T) {
	r := sampleRollout()
	sessions, path := writeRollout(t, r)
	delivered := map[string]any{"d1": map[string]any{
		"id": "d1", "session_id": "s001", "outbox_id": "o1", "teams_message_id": "1788256809000", "status": "sent",
		"codex_thread_id": s001Thread, "source_path": path, "source_line": 4, "source_offset": r.ends[4], "source_record_id": "f1", "part_index": 1,
	}}

	t.Run("watcher confirmed, checkpoint past an unproven turn", func(t *testing.T) {
		dir := fixture(t, "rc59", "json")
		// checkpoint past the second local turn (no proof), watcher after the Teams turn
		editJSON(t, dir, positions(path, r.ends[13], 13, r.ends[9], 9, delivered))
		rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions, o.CountBacklog = sessions, true })
		if !rep.OK() {
			t.Fatalf("checks: %+v", rep.Checks)
		}
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@"+strconv.FormatInt(r.ends[9], 10)+":9")
		eq(t, "bucket", rep.Buckets["linked tracking: resume at history_watch (nothing to mirror between proof and it)"], 1)
		eq(t, "catch-up prompts", rep.Backlog.Prompts, 1)
		eq(t, "catch-up finals", rep.Backlog.Finals, 1)
		eq(t, "gap", rep.Backlog.Gap, 2)
		// The control-chat summary carries the catch-up size.
		if sum := qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaMigrationSummary); !strings.Contains(sum, "Catch-up of local Codex sessions: 2 prompts and answers from 1 session (") {
			t.Errorf("summary:\n%s", sum)
		}
		// The delivered outbox row carries the mirror key of its record.
		eq(t, "o1 key", qs(t, db, `SELECT state FROM outbox WHERE delivery_key = ?`, MirrorKey(s001Thread, "f1", 1)), "sent")
		eq(t, "o1 legacy key gone", qi(t, db, `SELECT count(*) FROM outbox WHERE delivery_key = 'legacy-outbox:o1'`), int64(0))
	})

	t.Run("no proof", func(t *testing.T) {
		dir := fixture(t, "rc59", "json")
		editJSON(t, dir, positions(path, r.ends[13], 13, r.ends[13], 13, nil))
		rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions, o.CountBacklog = sessions, true })
		if !rep.OK() {
			t.Fatalf("checks: %+v", rep.Checks)
		}
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@0:0")
		eq(t, "no-proof sources", rep.Backlog.NoProofSources, 1)
		eq(t, "no-proof prompts (Teams turn and injection excluded)", rep.Backlog.NoProofPrompts, 2)
		var row *ResumeRow
		for i := range rep.Resume {
			if rep.Resume[i].Case == "no-proof" {
				row = &rep.Resume[i]
			}
		}
		if row == nil {
			t.Fatalf("no no-proof row: %+v", rep.Resume)
		}
		eq(t, "row gap", row.GapPrompts+row.GapFinals, 4)
		eq(t, "row checkpoint", row.Checkpoint, r.ends[13])
	})

	t.Run("rewritten file: proof by record id", func(t *testing.T) {
		dir := fixture(t, "rc59", "json")
		moved := map[string]any{"d1": map[string]any{}}
		for k, v := range delivered["d1"].(map[string]any) {
			moved["d1"].(map[string]any)[k] = v
		}
		moved["d1"].(map[string]any)["source_offset"] = r.ends[4] + 7 // offsets of an older file
		editJSON(t, dir, positions(path, r.ends[5], 5, 0, 0, moved))
		rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions = sessions })
		if !rep.OK() {
			t.Fatalf("checks: %+v", rep.Checks)
		}
		// f1 found by id at line 4; the checkpoint at line 5 has nothing in between.
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@"+strconv.FormatInt(r.ends[5], 10)+":5")
	})
	// Fidelity run, s509: the file was rewritten, so the delivery offset is
	// not a record end; the ledger id names a record past the old
	// checkpoint. The id proof must be found without CountBacklog too.
	t.Run("id proof past the delivery offset without backlog count", func(t *testing.T) {
		d := map[string]any{"d1": map[string]any{
			"id": "d1", "session_id": "s001", "outbox_id": "o1", "teams_message_id": "1788256809000", "status": "sent",
			"codex_thread_id": s001Thread, "source_path": path, "source_line": 4, "source_offset": r.ends[3] + 7, "source_record_id": "f1", "part_index": 1,
		}}
		for _, count := range []bool{false, true} {
			dir := fixture(t, "rc59", "json")
			editJSON(t, dir, positions(path, r.ends[2], 2, 0, 0, d))
			rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions, o.CountBacklog = sessions, count })
			if !rep.OK() {
				t.Fatalf("checks: %+v", rep.Checks)
			}
			eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@"+strconv.FormatInt(r.ends[4], 10)+":4")
		}
	})

	// Fidelity run, s278: the old core delivered the event_msg
	// agent_message final; the response_item that repeats it right after
	// the proof offset is the same final, not a record to mirror again.
	t.Run("response_item twin of the delivered event final", func(t *testing.T) {
		rt := newRollout()
		rt.line("session_meta", map[string]any{"id": s001Thread, "originator": "codex_cli_rs", "source": "cli"})
		rt.turn("ta")
		rt.user("u1", "ping")
		rt.line("event_msg", map[string]any{"type": "agent_message", "message": "pong answer", "phase": "final_answer"})
		rt.final("f1", "pong answer")
		rt.done("ta", "pong answer")
		sessions, path := writeRollout(t, rt)
		d := map[string]any{"d1": map[string]any{
			"id": "d1", "session_id": "s001", "outbox_id": "o1", "teams_message_id": "1788256809000", "status": "sent",
			"codex_thread_id": s001Thread, "source_path": path, "source_line": 4, "source_offset": rt.ends[4], "source_record_id": "fallback:x", "part_index": 1,
		}}
		dir := fixture(t, "rc59", "json")
		editJSON(t, dir, positions(path, rt.ends[6], 6, 0, 0, d))
		rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions, o.CountBacklog = sessions, true })
		if !rep.OK() {
			t.Fatalf("checks: %+v", rep.Checks)
		}
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@"+strconv.FormatInt(rt.ends[6], 10)+":6")
		eq(t, "catch-up finals", rep.Backlog.Finals, 0)
	})

	// Bug B1 (fidelity run, s547/s519): a later "compacted" record repeats
	// delivered message ids inside replacement_history. Those nested ids are
	// not the record's own id and must not move the resume position past
	// the undelivered records in between.
	t.Run("compacted history repeats a delivered id", func(t *testing.T) {
		rc := sampleRollout()
		rc.add(map[string]any{"timestamp": rc.ts, "ordinal": 15, "type": "compacted", "payload": map[string]any{
			"message": "", "replacement_history": []any{
				map[string]any{"type": "message", "id": "f1", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "local answer"}}},
				map[string]any{"type": "message", "id": "u1", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hello local"}}},
			}}})
		sessions, path := writeRollout(t, rc)
		d := map[string]any{"d1": map[string]any{
			"id": "d1", "session_id": "s001", "outbox_id": "o1", "teams_message_id": "1788256809000", "status": "sent",
			"codex_thread_id": s001Thread, "source_path": path, "source_line": 4, "source_offset": rc.ends[4], "source_record_id": "f1", "part_index": 1,
		}}
		dir := fixture(t, "rc59", "json")
		editJSON(t, dir, positions(path, rc.ends[4], 4, 0, 0, d))
		rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions, o.CountBacklog = sessions, true })
		if !rep.OK() {
			t.Fatalf("checks: %+v", rep.Checks)
		}
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@"+strconv.FormatInt(rc.ends[4], 10)+":4")
		eq(t, "catch-up prompts", rep.Backlog.Prompts, 1)
		eq(t, "catch-up finals", rep.Backlog.Finals, 1)
	})
}

func TestRecordID(t *testing.T) {
	for rec, want := range map[string]string{
		`{"timestamp":"t","type":"response_item","payload":{"type":"message","id":"msg_1","role":"user"}}`:                 "msg_1",
		`{"timestamp":"t","ordinal":3,"type":"response_item","payload":{"type":"message","role":"user","id":"msg_2"}}`:     "msg_2",
		`{"timestamp":"t","ordinal":9,"type":"compacted","payload":{"message":"","replacement_history":[{"id":"msg_1"}]}}`: "",
		`{"timestamp":"t","type":"response_item","payload":{"type":"message","role":"user","content":[{"id":"x"}]}}`:       "",
		`{"timestamp":"t","type":"event_msg","payload":{"type":"agent_message","message":"say \"id\":\"no\"","id":"e1"}}`:  "e1",
		`{"id":"top","payload":{"type":"message"}}`: "",
	} {
		eq(t, rec, recordID([]byte(rec)), want)
	}
}

func TestScanClassifier(t *testing.T) {
	r := sampleRollout()
	_, path := writeRollout(t, r)
	kinds := func(res scanResult) string {
		var out []string
		for _, x := range res.Recs {
			out = append(out, x.Kind+":"+x.ID)
		}
		return strings.Join(out, ",")
	}
	full := scanRollout(scanReq{path: path, to: r.ends[len(r.ends)-1]})
	if full.Err != nil {
		t.Fatal(full.Err)
	}
	eq(t, "full scan", kinds(full), "prompt:u1,final:f1,prompt:u3,final:f3")
	eq(t, "end", full.EndOff, r.ends[len(r.ends)-1])

	// A Teams turn known by its Codex turn id; the wrapper is not needed.
	r2 := newRollout()
	r2.turn("tx")
	r2.user("p", "plain prompt from Teams")
	r2.final("q", "answer")
	r2.done("tx", "answer")
	r2.turn("ty")
	r2.user("p2", "local")
	r2.done("ty", "local answer without final item")
	_, p2 := writeRollout(t, r2)
	res := scanRollout(scanReq{path: p2, to: r2.ends[len(r2.ends)-1], teamsTurn: map[string]bool{"tx": true}})
	eq(t, "teams turn by id", kinds(res), "prompt:p2,final:t:ty:final")

	// Mid-turn start after the delivered final: task_complete is not a second final.
	mid := scanRollout(scanReq{path: path, from: r.ends[4], fromLine: 4, to: r.ends[len(r.ends)-1]})
	eq(t, "mid-turn", kinds(mid), "prompt:u3,final:f3")
	// Mid-turn start inside a Teams turn whose wrapper was before the start.
	midTeams := scanRollout(scanReq{path: path, from: r.ends[7], fromLine: 7, to: r.ends[len(r.ends)-1], teamsTurn: map[string]bool{"tb": true}})
	eq(t, "mid Teams turn", kinds(midTeams), "prompt:u3,final:f3")
	// A partial last line is not a record.
	if err := os.WriteFile(p2, []byte(r2.b.String()+`{"type":"turn_context"`), 0o600); err != nil {
		t.Fatal(err)
	}
	part := scanRollout(scanReq{path: p2, to: 1 << 40, teamsTurn: map[string]bool{"tx": true}})
	eq(t, "partial line", part.EndOff, r2.ends[len(r2.ends)-1])
}

func TestLegacyTeamsPrompt(t *testing.T) {
	for text, want := range map[string]bool{
		"do it" + teamsWrapper: true,
		"x\n\nIf you need to return generated files or images to the Teams user, write them under this local directory:\n/tmp": true,
		"[codex-helper-control] status":                     true,
		"please grep for Teams helper safety: in the code":  false,
		"why does Teams helper safety:\n- appear in diffs?": false,
		"plain": false,
	} {
		eq(t, text, LegacyTeamsPrompt(text), want)
	}
}

func TestMirrorKey(t *testing.T) {
	eq(t, "key", MirrorKey("th", "msg_1", 2), "mirror:th:msg_1:2")
	eq(t, "prefix", strings.HasPrefix(MirrorKey("th", "msg_1", 2), MirrorKeyPrefix("th", "msg_1")), true)
	for id, want := range map[string]bool{"msg_1": true, "a..b": false, "fallback:session:x:line:3:kind:user": false, "": false} {
		eq(t, "codexRecordID "+id, codexRecordID(id), want)
	}
}

// Owner decision 2026-11-05: the catch-up has no window. Unproven records
// are caught up however old; a closed conversation gets no catch-up and
// stays at its old position.
func TestCatchUpHasNoWindowAndClosed(t *testing.T) {
	r := newRollout()
	r.ts = "2029-06-01T00:00:00Z" // 214 days before testNow
	r.line("session_meta", map[string]any{"id": s001Thread, "originator": "codex_cli_rs", "source": "cli"})
	r.turn("ta")
	r.user("u1", "old local")
	r.final("f1", "old answer")
	r.done("ta", "old answer")
	r.ts = "2029-12-31T00:00:00Z"
	r.turn("tb")
	r.user("u2", "recent local")
	r.final("f2", "recent answer")
	r.done("tb", "recent answer")
	sessions, path := writeRollout(t, r)

	t.Run("old records caught up", func(t *testing.T) {
		dir := fixture(t, "rc59", "json")
		editJSON(t, dir, positions(path, r.ends[9], 9, r.ends[9], 9, nil)) // no proof
		rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions, o.CountBacklog = sessions, true })
		if !rep.OK() {
			t.Fatalf("checks: %+v", rep.Checks)
		}
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@0:0")
		eq(t, "catch-up", rep.Backlog.Prompts+rep.Backlog.Finals, 4)
		eq(t, "no-proof row", rep.Resume[0].Case, "no-proof")
	})

	t.Run("without --count-backlog the resume stays at the old position before the old turn", func(t *testing.T) {
		dir := fixture(t, "rc59", "json")
		editJSON(t, dir, positions(path, r.ends[2], 2, 0, 0, nil)) // old position before the old turn
		_, db := dryRun(t, dir, func(o *Options) { o.CodexSessions = sessions })
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "tracking@"+strconv.FormatInt(r.ends[2], 10)+":2")
	})

	t.Run("closed conversation", func(t *testing.T) {
		dir := fixture(t, "rc59", "json")
		editJSON(t, dir, positions(path, r.ends[2], 2, 0, 0, nil))
		editJSON(t, dir, func(m map[string]any) {
			m["sessions"].(map[string]any)["s001"].(map[string]any)["status"] = "closed"
		})
		rep, db := dryRun(t, dir, func(o *Options) { o.CodexSessions, o.CountBacklog = sessions, true })
		if !rep.OK() {
			t.Fatalf("checks: %+v", rep.Checks)
		}
		eq(t, "s001 source", qs(t, db, sourceQ, "%"+s001Thread+".jsonl"), "baseline@"+strconv.FormatInt(r.ends[2], 10)+":2")
		eq(t, "closed sources", rep.Backlog.ClosedSources, 1)
		eq(t, "skipped closed", rep.Backlog.SkippedClosed, 4)
		eq(t, "no catch-up", rep.Backlog.Prompts+rep.Backlog.Finals, 0)
	})
}

// legacy-<16 hex> turn keys cannot collide with keys the core creates
// ('<turn_key_prefix>-<n>', prefix 'cxp-<scope8>', or 'cxp-<8 hex>' on a fresh store).
func TestLegacyTurnKeysDisjoint(t *testing.T) {
	_, db := dryRun(t, fixture(t, "rc59", "json"), nil)
	legacy := regexp.MustCompile(`^legacy-[0-9a-f]{16}$`)
	rows, err := db.Query(`SELECT turn_key FROM turns`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		n++
		if !legacy.MatchString(k) {
			t.Errorf("turn key %q is not legacy-<16 hex>", k)
		}
	}
	if n == 0 {
		t.Fatal("no turns")
	}
	prefix := qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaTurnKeyPrefix)
	if !strings.HasPrefix(prefix, "cxp-") {
		t.Fatalf("turn key prefix %q", prefix)
	}
	if legacy.MatchString(prefix+"-1") || strings.HasPrefix(prefix, "legacy-") {
		t.Fatal("new turn keys could collide with legacy keys")
	}
}
