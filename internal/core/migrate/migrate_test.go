package migrate

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// The fixtures under testdata/{rc59,v0120} were written by the real old store
// code (testdata/gen/gen.sh runs it in Docker from a throwaway worktree of the
// tagged release). json/ is the full-JSON form; sqlite-wal/ is the SQLite
// pointer form copied while the old store was still open after one more
// write, so rows i-wal and o-wal exist only in store.sqlite-wal.

var testNow = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

// fixture copies testdata/<version>/<form> into a writable temp scope dir.
func fixture(t *testing.T, version, form string) string {
	t.Helper()
	src := filepath.Join("testdata", version, form)
	dst := filepath.Join(t.TempDir(), "scope")
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		copyFile(t, filepath.Join(src, e.Name()), filepath.Join(dst, e.Name()))
	}
	return dst
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func dryRun(t *testing.T, dir string, mod func(*Options)) (*Report, *sql.DB) {
	t.Helper()
	// The fixtures are from 2026 and testNow is 2030: keep their unsent rows
	// (TestStaleUnsentRowsDropped covers the default).
	opt := Options{Source: dir, Out: filepath.Join(t.TempDir(), "out"), DryRun: true, StaleUnsent: -1, Now: func() time.Time { return testNow }}
	if mod != nil {
		mod(&opt)
	}
	rep, err := Run(opt)
	if err != nil {
		if rep != nil {
			var b strings.Builder
			rep.WriteText(&b)
			t.Log(b.String())
		}
		t.Fatalf("Run: %v", err)
	}
	return rep, openResult(t, filepath.Join(opt.Out, TargetName))
}

func openResult(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func qs(t *testing.T, db *sql.DB, q string, args ...any) string {
	t.Helper()
	var v sql.NullString
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("%s %v: %v", q, args, err)
	}
	return v.String
}

func qi(t *testing.T, db *sql.DB, q string, args ...any) int64 {
	t.Helper()
	var v sql.NullInt64
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("%s %v: %v", q, args, err)
	}
	return v.Int64
}

func eq[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

const (
	convQ    = `SELECT state || '/' || COALESCE(paused_reason,'') FROM conversations WHERE label = ?`
	inQ      = `SELECT state || '/' || COALESCE(body,'<null>') FROM inbound WHERE ext_msg_id = ?`
	outQ     = `SELECT state FROM outbox WHERE delivery_key = ?`
	turnQ    = `SELECT state FROM turns WHERE turn_key = ?`
	sourceQ  = `SELECT state || '@' || offset || ':' || line FROM transcript_sources WHERE path LIKE ?`
	streamQ  = `SELECT cursor FROM streams WHERE key = ?`
	s007Path = "%000000000007.jsonl"
)

// TestConvertFixtures checks the §7.2 mapping on every fixture form.
func TestConvertFixtures(t *testing.T) {
	for _, tc := range []struct{ version, form string }{
		{"rc59", "json"}, {"rc59", "sqlite-wal"}, {"v0120", "json"}, {"v0120", "sqlite-wal"},
	} {
		t.Run(tc.version+"/"+tc.form, func(t *testing.T) {
			rc59 := tc.version == "rc59"
			rep, db := dryRun(t, fixture(t, tc.version, tc.form), nil)
			eq(t, "version", rep.Version, map[bool]Version{true: RC59, false: V0120}[rc59])
			if !rep.OK() {
				t.Fatalf("checks failed: %+v", rep.Checks)
			}

			// Conversations.
			eq(t, "control", qs(t, db, `SELECT ext_chat_id FROM conversations WHERE kind = 'control'`), "19:control@thread.v2")
			eq(t, "s001", qs(t, db, convQ, "s001"), "active/")
			eq(t, "s002", qs(t, db, convQ, "s002"), "parked/")
			eq(t, "s003", qs(t, db, convQ, "s003"), "active/migrated-quarantine: fixture self-echo")
			eq(t, "s005", qs(t, db, convQ, "s005"), "closed/")
			eq(t, "s001 thread", qs(t, db, `SELECT thread_id FROM threads JOIN conversations c ON c.id = conv_id WHERE c.label = 's001'`), "11111111-1111-4111-8111-000000000001")
			if rc59 {
				eq(t, "s006 staging", qs(t, db, convQ, "s006"), "hold/")
				eq(t, "fork", qs(t, db, `SELECT phase FROM forks`), "hold")
				eq(t, "control fallback thread", qs(t, db, `SELECT thread_id FROM threads JOIN conversations c ON c.id = conv_id WHERE c.kind = 'control'`), "11111111-1111-4111-8111-0000000000ff")
				eq(t, "s007 (held messages)", qs(t, db, convQ, "s007"), "active/migrated-held-messages")
			}

			// Turns: all imported (no retention), running -> interrupted.
			eq(t, "t0 (older terminal kept)", qs(t, db, turnQ, LegacyTurnKey("t0")), "completed")
			eq(t, "t1", qs(t, db, turnQ, LegacyTurnKey("t1")), "completed")
			eq(t, "t-run", qs(t, db, turnQ, LegacyTurnKey("t-run")), "interrupted")
			eq(t, "t-q", qs(t, db, turnQ, LegacyTurnKey("t-q")), "queued")
			eq(t, "t-q linked", qs(t, db, `SELECT i.ext_msg_id FROM turns t JOIN inbound i ON i.id = t.inbound_id WHERE t.turn_key = ?`, LegacyTurnKey("t-q")), "1788256812000")
			eq(t, "never-run deferred prompt gets a queued turn", qs(t, db, turnQ, LegacyTurnKey("inbound:i11")), "queued")

			// Inbound: bodies kept on every row.
			eq(t, "i1", qs(t, db, inQ, "1788256801000"), "done/first prompt")
			eq(t, "i4 queued+terminal turn", qs(t, db, inQ, "1788256802000"), "done/queued but turn already done")
			eq(t, "i5", qs(t, db, inQ, "1788256803000"), "ignored/helper echo")
			eq(t, "i3", qs(t, db, inQ, "1788256812000"), "queued/queued prompt")
			eq(t, "i11", qs(t, db, inQ, "1788256814000"), "queued/deferred, no turn")
			eq(t, "i12 control persisted", qs(t, db, inQ, "1788256815000"), "done/status")
			// No data loss: an unknown chat's rows land on a closed placeholder conversation.
			eq(t, "i13 unknown chat -> orphan", qs(t, db, `SELECT c.kind || '/' || c.state || '/' || c.label || '/' || c.ext_chat_id FROM inbound i JOIN conversations c ON c.id = i.conv_id WHERE i.ext_msg_id = '1788256816000'`),
				"work/closed/orphan-1/"+qs(t, db, `SELECT ext_chat_id FROM conversations WHERE label = 'orphan-1'`))
			if rc59 {
				eq(t, "i6 control manual_hold", qs(t, db, inQ, "1788256806000"), "ignored/held control")
				eq(t, "i7 work manual_hold", qs(t, db, inQ, "1788256807000"), "held/held work")
				eq(t, "i8 uncertain", qs(t, db, inQ, "1788256808000"), "held/uncertain")
			}

			// Outbox.
			eq(t, "o1", qs(t, db, `SELECT state || '/' || body || '/' || ext_msg_id FROM outbox WHERE delivery_key = 'legacy-outbox:o1'`), "sent/answer one/1788256809000")
			h := sha256.Sum256([]byte("answer one"))
			eq(t, "o1 hash", qs(t, db, `SELECT hex(body_hash) FROM outbox WHERE delivery_key = 'legacy-outbox:o1'`), strings.ToUpper(hexs(h[:])))
			eq(t, "o1 hash marked legacy", qs(t, db, `SELECT hash_algo FROM outbox WHERE delivery_key = 'legacy-outbox:o1'`), "legacy")
			// An answer with an unknown outcome is checked against the
			// chat history at first start (owner ruling 2026-11-05, Q1).
			eq(t, "o2 sending (answer)", qs(t, db, outQ, "legacy-outbox:o2"), "dropped")
			eq(t, "o6 accepted", qs(t, db, outQ, "legacy-outbox:o6"), "unknown")
			eq(t, "o3", qs(t, db, outQ, "legacy-outbox:o3"), "queued")
			eq(t, "o7 control", qs(t, db, `SELECT o.state || '/' || c.kind FROM outbox o JOIN conversations c ON c.id = o.conv_id WHERE delivery_key = 'legacy-outbox:o7'`), "queued/control")
			eq(t, "o4 skipped", qs(t, db, `SELECT state || '/' || note || '/' || body FROM outbox WHERE delivery_key = 'legacy-outbox:o4'`), "dropped/legacy skipped: superseded by interrupted turn/skip me")
			eq(t, "interruption notice", qs(t, db, outQ, "legacy-interrupted:t-run"), "queued")
			eq(t, "unknown rows", qi(t, db, `SELECT count(*) FROM outbox WHERE state = 'unknown'`), int64(1))
			// Unsent rows keep the legacy text: the core renders them.
			eq(t, "unsent rows marked legacy", qi(t, db, `SELECT count(*) FROM outbox WHERE state IN ('queued','unknown') AND hash_algo IS NULL`), int64(0))

			// Streams.
			eq(t, "s001 cursor", qs(t, db, streamQ, StreamKey("19:chat-s001@thread.v2")), "2026-09-01T10:00:05Z")
			if rc59 {
				eq(t, "s007 gap cursor", qs(t, db, streamQ, StreamKey("19:chat-s007@thread.v2")), "2026-09-01T10:00:15Z")
			}

			// Gates: unexpired only.
			eq(t, "gate", qi(t, db, `SELECT until FROM gates WHERE key = ?`, store.ChatGate("teams", "19:chat-s001@thread.v2")), testNow.Add(time.Hour).UnixMilli()) // 2099 capped
			eq(t, "expired gate", qi(t, db, `SELECT count(*) FROM gates`), int64(1))

			// Transcript sources (no rollout access): stored offsets.
			eq(t, "s001 source", qs(t, db, sourceQ, "%000000000001.jsonl"), "tracking@40000:100")
			eq(t, "s004 rewrite-blocked flag ignored", qs(t, db, sourceQ, "%000000000004.jsonl"), "tracking@7000:7")
			// s007 has an execution anchor at 3000 and a later publish
			// checkpoint at 3500: resume at the furthest delivered offset.
			eq(t, "s007 anchored source", qs(t, db, sourceQ, s007Path), "tracking@3500:35")
			eq(t, "synthetic turns", qi(t, db, `SELECT count(*) FROM turns WHERE turn_key NOT LIKE 'legacy-%'`), int64(0))

			eq(t, "notifications", qi(t, db, `SELECT count(*) FROM notifications`), int64(2))
			eq(t, "notification kinds", qi(t, db, `SELECT count(*) FROM notifications WHERE kind <> ''`), int64(2))
			eq(t, "meta migrated_from", qs(t, db, `SELECT value FROM meta WHERE key = 'migrated_from'`), string(rep.Version))
		})
	}
}

func hexs(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&15])
	}
	return string(out)
}

// Hazard: rows that exist only in the -wal file are included (online backup).
func TestWALOnlyRows(t *testing.T) {
	for _, v := range []string{"rc59", "v0120"} {
		t.Run(v, func(t *testing.T) {
			dir := fixture(t, v, "sqlite-wal")
			// The main file alone does not have them.
			main := filepath.Join(t.TempDir(), "main.sqlite")
			copyFile(t, filepath.Join(dir, "store.sqlite"), main)
			mdb, err := sql.Open("sqlite", "file:"+main+"?mode=ro&immutable=1")
			if err != nil {
				t.Fatal(err)
			}
			eq(t, "i-wal in main file", qi(t, mdb, `SELECT count(*) FROM inbound_events WHERE id = 'i-wal'`), int64(0))
			mdb.Close()

			_, db := dryRun(t, dir, nil)
			eq(t, "i-wal", qs(t, db, inQ, "1788256830000"), "queued/written last")
			eq(t, "o-wal", qs(t, db, outQ, "legacy-outbox:o-wal"), "queued")
		})
	}
}

// --immutable refuses a non-empty -wal (it would miss rows) and reads a
// checkpointed snapshot directly.
func TestImmutableSnapshot(t *testing.T) {
	dir := fixture(t, "rc59", "sqlite-wal")
	_, err := Run(Options{Source: dir, Out: t.TempDir(), DryRun: true, Immutable: true, Now: func() time.Time { return testNow }})
	if err == nil || !strings.Contains(err.Error(), "-wal is not empty") {
		t.Fatalf("immutable with WAL: %v", err)
	}
	snap := filepath.Join(t.TempDir(), "snap")
	if err := os.MkdirAll(snap, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := backup(filepath.Join(dir, "store.sqlite"), filepath.Join(snap, "store.sqlite")); err != nil {
		t.Fatal(err)
	}
	copyFile(t, filepath.Join(dir, "state.json"), filepath.Join(snap, "state.json"))
	if err := os.Chmod(snap, 0o500); err != nil { // read-only directory
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(snap, 0o700) })
	_, db := dryRun(t, snap, func(o *Options) { o.Immutable = true })
	eq(t, "i-wal via snapshot", qs(t, db, inQ, "1788256830000"), "queued/written last")
	if _, err := os.Stat(filepath.Join(snap, "store.sqlite-shm")); err == nil {
		t.Error("immutable read created a -shm file in the source")
	}
}

// Hazard: unparsable rows are reported by id, never abort the run, and are
// imported fail-closed (non-terminal -> held/unknown; terminal keys kept).
func TestOpaqueRows(t *testing.T) {
	dir := fixture(t, "rc59", "sqlite-wal")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`UPDATE inbound_events SET json = '{broken' WHERE id IN ('i1','i3')`,
		`UPDATE outbox_messages SET json = 'not json' WHERE id IN ('o1','o3')`,
		`UPDATE turns SET json = '[' WHERE id = 't0'`,
		`UPDATE notifications SET json = '{' WHERE id = 'n2'`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	db.Close()
	rep, out := dryRun(t, dir, nil)
	failed := map[string][]string{}
	for _, e := range rep.Entities() {
		failed[e.Name] = e.Failed
	}
	eq(t, "inbound failed ids", strings.Join(failed["inbound_events"], ","), "i1,i3")
	eq(t, "outbox failed ids", strings.Join(failed["outbox_messages"], ","), "o1,o3")
	eq(t, "turn failed ids", strings.Join(failed["turns"], ","), "t0")
	eq(t, "i1 terminal: key kept", qs(t, out, inQ, "1788256801000"), "done/<null>")
	eq(t, "i3 queued: held", qs(t, out, inQ, "1788256812000"), "held/<null>")
	eq(t, "o1 sent: key kept", qs(t, out, `SELECT state || '/' || ext_msg_id FROM outbox WHERE delivery_key = 'legacy-outbox:o1'`), "sent/1788256809000")
	eq(t, "o3 queued: unknown", qs(t, out, outQ, "legacy-outbox:o3"), "unknown")
	eq(t, "n2", qs(t, out, `SELECT state FROM notifications WHERE key = 'legacy:n2'`), "dropped")
	// o3 (unparsable), o6 (a title); o2, an answer, is checked against the
	// chat history instead (owner ruling 2026-11-05, Q1).
	eq(t, "unknown outbox rows", qi(t, out, `SELECT count(*) FROM outbox WHERE state = 'unknown'`), int64(2))
}

// Hazard: an initialised session without a poll row must not rebaseline at
// the head: its stream starts just before its oldest known inbound row.
func TestInitialisedSessionWithoutPollRow(t *testing.T) {
	for _, v := range []string{"rc59", "v0120"} {
		_, db := dryRun(t, fixture(t, v, "json"), nil)
		eq(t, v+" s004 cursor", qs(t, db, streamQ, StreamKey("19:chat-s004@thread.v2")), cursor(time.UnixMilli(1788253200000-1000)))
		eq(t, v+" s004 seeded", qi(t, db, `SELECT seeded FROM streams WHERE key = ?`, StreamKey("19:chat-s004@thread.v2")), int64(1))
	}
}

// editJSON rewrites the full-JSON state.json of a fixture copy.
func editJSON(t *testing.T, dir string, fn func(map[string]any)) {
	t.Helper()
	p := filepath.Join(dir, "state.json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	fn(m)
	raw, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// Hazard: wrong-typed last_offset_known never makes a checkpoint unparsable.
func TestWrongTypedLastOffsetKnown(t *testing.T) {
	for _, val := range []any{"yes", 1, "1", map[string]any{}, []any{true}} {
		dir := fixture(t, "rc59", "json")
		editJSON(t, dir, func(m map[string]any) {
			cps := m["import_checkpoints"].(map[string]any)
			for _, id := range []string{"transcript:s001", "transcript:s004"} {
				cps[id].(map[string]any)["last_offset_known"] = val
			}
		})
		rep, db := dryRun(t, dir, nil)
		for _, e := range rep.Entities() {
			if e.Name == "import_checkpoints" && len(e.Failed) > 0 {
				t.Errorf("%v: checkpoints failed to parse: %v", val, e.Failed)
			}
		}
		eq(t, "s001 source", qs(t, db, sourceQ, "%000000000001.jsonl"), "tracking@40000:100")
		eq(t, "s004 source", qs(t, db, sourceQ, "%000000000004.jsonl"), "tracking@7000:7")
	}
}

// Hazard: only v0.1.20 and rc.59 are accepted.
func TestUnsupportedVersionRefused(t *testing.T) {
	for _, tc := range []struct {
		version, form string
		schema        float64
	}{{"rc59", "json", 8}, {"rc59", "json", 12}, {"v0120", "sqlite-wal", 9}, {"rc59", "sqlite-wal", 13}} {
		dir := fixture(t, tc.version, tc.form)
		editJSON(t, dir, func(m map[string]any) { m["schema_version"] = tc.schema })
		_, err := Run(Options{Source: dir, Out: t.TempDir(), DryRun: true, Now: func() time.Time { return testNow }})
		if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "run v0.1.22-rc.59 once") {
			t.Errorf("%s/%s schema %v: %v", tc.version, tc.form, tc.schema, err)
		}
	}
}

// Hazard: a live writer (held lock, fresh heartbeat, valid lease) is refused.
func TestLiveWriterRefused(t *testing.T) {
	run := func(dir string) error {
		_, err := Run(Options{Source: dir, Out: t.TempDir(), DryRun: true, Now: func() time.Time { return testNow }})
		return err
	}
	t.Run("lock held", func(t *testing.T) {
		dir := fixture(t, "rc59", "sqlite-wal")
		fl := flock.New(filepath.Join(dir, "state.json.lock"))
		if ok, err := fl.TryLock(); !ok || err != nil {
			t.Fatal(ok, err)
		}
		defer fl.Unlock()
		if err := run(dir); !errors.Is(err, ErrLiveWriter) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("fresh heartbeat", func(t *testing.T) {
		dir := fixture(t, "v0120", "json")
		editJSON(t, dir, func(m map[string]any) {
			m["service_owner"].(map[string]any)["last_heartbeat"] = testNow.Add(-time.Minute).Format(time.RFC3339)
		})
		if err := run(dir); !errors.Is(err, ErrLiveWriter) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("valid lease (runtime_state)", func(t *testing.T) {
		dir := fixture(t, "rc59", "sqlite-wal")
		db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "store.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		lease := `{"scope_id":"scope:fixture0001","holder_machine_id":"machine:x","status":"active","generation":4,"lease_until":"` +
			testNow.Add(time.Hour).Format(time.RFC3339) + `"}`
		if _, err := db.Exec(`UPDATE runtime_state SET json = ? WHERE key = 'control_lease'`, lease); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if err := run(dir); !errors.Is(err, ErrLiveWriter) {
			t.Fatalf("got %v", err)
		}
	})
}

// A real (non-dry) run writes core.sqlite and the marker into the scope dir,
// leaves the source files byte-identical, and refuses a second run.
func TestRealRunLeavesSourceUntouched(t *testing.T) {
	dir := fixture(t, "v0120", "sqlite-wal")
	sum := func() map[string][32]byte {
		out := map[string][32]byte{}
		for _, n := range []string{"state.json", "store.sqlite", "store.sqlite-wal"} {
			b, err := os.ReadFile(filepath.Join(dir, n))
			if err != nil {
				t.Fatal(err)
			}
			out[n] = sha256.Sum256(b)
		}
		return out
	}
	before := sum()
	rep, err := Run(Options{Source: dir, Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatal(err)
	}
	if after := sum(); after != nil {
		for n, h := range before {
			if after[n] != h {
				t.Errorf("%s changed", n)
			}
		}
	}
	for _, n := range []string{TargetName, MarkerName, ReportName} {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s: %v", n, err)
		}
	}
	for _, n := range []string{TargetName + ".tmp", "migrate.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			t.Errorf("%s left behind", n)
		}
	}
	eq(t, "out path", rep.OutPath, filepath.Join(dir, TargetName))
	if _, err := Run(Options{Source: dir, Now: func() time.Time { return testNow }}); err == nil || !strings.Contains(err.Error(), "already migrated") {
		t.Fatalf("second run: %v", err)
	}
}

// Rollout classification by stat and first line only (amendments 4 and 5).
func TestRolloutClassification(t *testing.T) {
	sessions := filepath.Join(t.TempDir(), "sessions")
	day := filepath.Join(sessions, "2029", "06", "01")
	if err := os.MkdirAll(day, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(thread, first string, age time.Duration) string {
		p := filepath.Join(day, "rollout-2029-06-01T00-00-00-"+thread+".jsonl")
		if err := os.WriteFile(p, []byte(first+"\n{\"type\":\"event\"}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		mt := testNow.Add(-age)
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
		return p
	}
	meta := func(id, originator, source string) string {
		return `{"type":"session_meta","payload":{"id":"` + id + `","originator":"` + originator + `","source":` + source + `}}`
	}
	const d = 24 * time.Hour
	sub := write("aaaaaaaa-0000-4000-8000-000000000001", meta("a1", "codex_cli_rs", `{"subagent":{"other":"x"}}`), 200*d)
	write("aaaaaaaa-0000-4000-8000-000000000002", meta("a2", "codex_exec", `"exec"`), 10*d)
	write("aaaaaaaa-0000-4000-8000-000000000003", meta("a3", "codex-helper", `"vscode"`), 10*d)
	write("aaaaaaaa-0000-4000-8000-000000000004", meta("a4", "codex_cli_rs", `"cli"`), 10*d)
	write("aaaaaaaa-0000-4000-8000-000000000005", meta("a5", "codex_cli_rs", `"cli"`), 200*d)
	bad := write("aaaaaaaa-0000-4000-8000-000000000006", `not json`, 10*d)
	linked := write("11111111-1111-4111-8111-000000000002", meta("s002", "codex_cli_rs", `"cli"`), 10*d)

	rep, db := dryRun(t, fixture(t, "rc59", "json"), func(o *Options) { o.CodexSessions = sessions })
	state := func(n string) string {
		var v sql.NullString
		_ = db.QueryRow(`SELECT state || '@' || offset FROM transcript_sources WHERE path LIKE ?`, "%aaaaaaaa-0000-4000-8000-00000000000"+n+".jsonl").Scan(&v)
		return v.String
	}
	size := func(p string) string {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return strconv.FormatInt(st.Size(), 10)
	}
	eq(t, "subagent", state("1"), "ignored@"+size(sub))
	eq(t, "exec", strings.HasPrefix(state("2"), "ignored@"), true)
	eq(t, "codex-helper originator", strings.HasPrefix(state("3"), "ignored@"), true)
	eq(t, "recent interactive: no row (discovery)", state("4"), "")
	eq(t, "old interactive: no row (discovery, no window)", state("5"), "")
	eq(t, "unreadable first line: baseline at EOF", state("6"), "baseline@"+size(bad))
	eq(t, "linked by file name: tracking at EOF", qs(t, db, `SELECT s.state || '@' || s.offset || '/' || c.label FROM transcript_sources s JOIN conversations c ON c.id = s.conv_id WHERE path = ?`, linked), "tracking@"+size(linked)+"/s002")
	// Checkpoint paths that do not exist on disk become 'missing' with their offsets kept.
	eq(t, "missing checkpoint file", qs(t, db, sourceQ, "%000000000001.jsonl"), "missing@40000:100")
	eq(t, "bucket interactive", rep.Buckets["unlinked interactive -> no row (discovery)"], 2)
	eq(t, "bucket subagent/exec", rep.Buckets["unlinked subagent/exec -> ignored"], 2)
	eq(t, "bucket originator", rep.Buckets["unlinked originator=codex-helper -> ignored"], 1)
}

// Workflow card config and its webhook secret (SPEC §5.2): the secret is
// copied 0600 next to the new store and never appears in the report.
func TestWorkflowImport(t *testing.T) {
	dir := fixture(t, "rc59", "json")
	const secret = "https://example.invalid/workflows/SECRET-TOKEN"
	if err := os.WriteFile(filepath.Join(dir, "workflow-webhook-url"), []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := `{"version":1,"workflow":{"enabled":true,"control_webhook_url_file":"/nonexistent/workflow-webhook-url","control_chat_id":"19:control@thread.v2"}}`
	if err := os.WriteFile(filepath.Join(dir, "workflow-notifications.json"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out")
	rep, db := dryRun(t, dir, func(o *Options) { o.Out = out })
	eq(t, "enabled", qs(t, db, `SELECT value FROM meta WHERE key = ?`, MetaWorkflowEnabled), "true")
	eq(t, "control chat", qs(t, db, `SELECT value FROM meta WHERE key = ?`, MetaWorkflowControlID), "19:control@thread.v2")
	dst := filepath.Join(out, "workflow-webhook-url")
	eq(t, "secret file", qs(t, db, `SELECT value FROM meta WHERE key = ?`, MetaWorkflowSecretFile), dst)
	st, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "mode", st.Mode().Perm(), os.FileMode(0o600))
	raw, _ := os.ReadFile(dst)
	eq(t, "content", string(raw), secret)
	var b strings.Builder
	rep.WriteText(&b)
	if strings.Contains(b.String(), "SECRET-TOKEN") {
		t.Error("secret in the report")
	}
}

// Shared Codex thread: the conversation with the newest activity keeps the
// rollout; the other is parked, keeps its thread id and owns no source.
func TestSharedThreadWinner(t *testing.T) {
	dir := fixture(t, "rc59", "json")
	editJSON(t, dir, func(m map[string]any) {
		m["sessions"].(map[string]any)["s002"].(map[string]any)["codex_thread_id"] = "11111111-1111-4111-8111-000000000001"
	})
	rep, db := dryRun(t, dir, nil)
	if !rep.OK() {
		t.Fatalf("checks: %+v", rep.Checks)
	}
	if len(rep.Shared) != 1 {
		t.Fatalf("shared: %+v", rep.Shared)
	}
	sh := rep.Shared[0]
	eq(t, "winner newer", sh.WinnerLast >= sh.LoserLast, true)
	eq(t, "winner", sh.Winner, "s001") // s001 has the newer turns
	eq(t, "loser parked", qs(t, db, `SELECT state FROM conversations WHERE label = ?`, sh.Loser), "parked")
	eq(t, "loser keeps thread", qs(t, db, `SELECT t.thread_id FROM threads t JOIN conversations c ON c.id = t.conv_id WHERE c.label = ?`, sh.Loser), "11111111-1111-4111-8111-000000000001")
	eq(t, "loser has no source", qi(t, db, `SELECT count(*) FROM transcript_sources s JOIN conversations c ON c.id = s.conv_id WHERE c.label = ?`, sh.Loser), int64(0))
}

// The old store's registry machine id is kept (internal/core/delegate
// publishes the card and opens the inbox under it).
func TestMachineIdentityKept(t *testing.T) {
	dir := fixture(t, "v0120", "json")
	editJSON(t, dir, func(m map[string]any) {
		m["machine_identity"].(map[string]any)["id"] = "machine:0123456789abcdef"
	})
	_, db := dryRun(t, dir, nil)
	eq(t, "machine_id", qs(t, db, `SELECT value FROM meta WHERE key = 'machine_id'`), "machine:0123456789abcdef")
	_, db = dryRun(t, fixture(t, "v0120", "json"), nil)
	eq(t, "no machine_id without an identity", qi(t, db, `SELECT COUNT(*) FROM meta WHERE key = 'machine_id'`), int64(0))
}

// Unsent legacy rows from more than a day before the conversion are dropped
// (not sent or resolved weeks later) and counted in the summary; the
// interruption notice made at the conversion is kept.
func TestStaleUnsentRowsDropped(t *testing.T) {
	for _, v := range []string{"v0120", "rc59"} {
		t.Run(v, func(t *testing.T) {
			rep, db := dryRun(t, fixture(t, v, "sqlite-wal"), func(o *Options) { o.StaleUnsent = 0 })
			if !rep.OK() {
				t.Fatalf("checks: %+v", rep.Checks)
			}
			for _, k := range []string{"legacy-outbox:o3", "legacy-outbox:o6", "legacy-outbox:o7"} {
				eq(t, k, qs(t, db, `SELECT state || '/' || substr(note, 1, 34) FROM outbox WHERE delivery_key = ?`, k), "dropped/migrated: unsent for more than 24h")
			}
			eq(t, "unknown rows", qi(t, db, `SELECT count(*) FROM outbox WHERE state = 'unknown'`), int64(0))
			eq(t, "interruption notice", qs(t, db, outQ, "legacy-interrupted:t-run"), "queued")
			sum := qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaMigrationSummary)
			n := qi(t, db, `SELECT count(*) FROM outbox WHERE note LIKE 'migrated: unsent for more than%'`)
			if want := fmt.Sprintf("%d older messages (from more than a day ago)", n); n == 0 || !strings.Contains(sum, want) {
				t.Errorf("summary lacks %q:\n%s", want, sum)
			}
		})
	}
}

// The summary counts the previous helper's messages that are sent now, not
// the interruption notices the conversion adds itself (E2E
// ENTRY-AUTOMIGRATE: "1 message that was not sent yet is sent now" named
// only the conversion's own notice).
func TestSummaryCountsOnlyThePreviousHelpersUnsentMessages(t *testing.T) {
	for _, v := range []string{"v0120", "rc59"} {
		t.Run(v, func(t *testing.T) {
			// Kept unsent rows (StaleUnsent -1) plus the interruption notice.
			_, db := dryRun(t, fixture(t, v, "sqlite-wal"), nil)
			eq(t, "interruption notice", qs(t, db, outQ, "legacy-interrupted:t-run"), "queued")
			own := qi(t, db, `SELECT count(*) FROM outbox WHERE state IN ('queued','unknown') AND delivery_key NOT LIKE 'legacy-interrupted:%'`)
			sum := qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaMigrationSummary)
			if own < 2 || !strings.Contains(sum, fmt.Sprintf("\n\n%d messages that were not sent yet are sent now.", own)) {
				t.Errorf("summary should count the %d unsent messages of the previous helper:\n%s", own, sum)
			}
			// Only the interruption notice is left to send: no such line.
			_, db = dryRun(t, fixture(t, v, "sqlite-wal"), func(o *Options) { o.StaleUnsent = 0 })
			eq(t, "interruption notice (stale rows dropped)", qs(t, db, outQ, "legacy-interrupted:t-run"), "queued")
			if sum := qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaMigrationSummary); strings.Contains(sum, "not sent yet") {
				t.Errorf("summary counts the conversion's own notice as an unsent message:\n%s", sum)
			}
		})
	}
}

// Owner ruling 2026-11-05 (Q7): a quarantined session is converted by the
// cause of its quarantine. The previous helper's automatic self-echo
// breaker is lifted (not paused, and what was posted in the chat before the
// conversion is not answered); a quarantine the owner applied with
// `cxp teams chat quarantine` closes the chat; any other cause keeps the
// chat paused.
func TestQuarantineByCause(t *testing.T) {
	const chat = "19:chat-s003@thread.v2"
	const restored = "Chat s003: the previous helper paused it automatically (self-echo protection); it is no longer paused"
	for _, v := range []string{"v0120", "rc59"} {
		for _, tc := range []struct {
			name, source, conv, summary string
			sharedThread, restored      bool
		}{
			{"breaker", "teams_self_echo_breaker", "active/", restored + ". Messages posted there before now are not answered.", false, true},
			// s388 on the real store: s413 continues its Codex thread, so the
			// shared-thread rule parks it (not paused; `r s388` wakes it).
			{"breaker-shared-thread", "teams_self_echo_breaker", "parked/", restored + ", and parked because another chat continues its Codex session (wake it with `r s003`). Messages posted there before now are not answered.", true, true},
			{"manual", "teams_chat_quarantine_cli", "closed/", "Chat s003: you quarantined it with the previous helper; it is closed.", false, false},
			{"decoder", "store_decoder", "active/migrated-quarantine: fixture self-echo", "Chat s003: quarantined by the previous helper; it stays paused.", false, false},
			{"no-source", "", "active/migrated-quarantine: fixture self-echo", "Chat s003: quarantined by the previous helper; it stays paused.", false, false},
		} {
			t.Run(v+"/"+tc.name, func(t *testing.T) {
				dir := fixture(t, v, "json")
				editJSON(t, dir, func(m map[string]any) {
					s003 := m["sessions"].(map[string]any)["s003"].(map[string]any)
					if tc.source != "" {
						s003["quarantine_source"] = tc.source
					}
					if tc.sharedThread {
						s003["codex_thread_id"] = "11111111-1111-4111-8111-000000000001" // s001's, newer
					}
				})
				rep, db := dryRun(t, dir, nil)
				if !rep.OK() {
					t.Fatalf("checks: %+v", rep.Checks)
				}
				eq(t, "s003", qs(t, db, convQ, "s003"), tc.conv)
				eq(t, "s003 paused", qi(t, db, `SELECT paused_at IS NOT NULL FROM conversations WHERE label = 's003'`) == 1, strings.HasPrefix(tc.conv, "active/migrated-quarantine"))
				wantCursor := "2026-08-19T00:00:00Z"
				if tc.restored {
					wantCursor = cursor(testNow)
				}
				eq(t, "s003 cursor", qs(t, db, streamQ, StreamKey(chat)), wantCursor)
				sum := qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaMigrationSummary)
				if !strings.Contains(sum, "\n\n"+tc.summary) {
					t.Errorf("summary lacks %q:\n%s", tc.summary, sum)
				}
				eq(t, "s001", qs(t, db, convQ, "s001"), "active/")
				eq(t, "s005", qs(t, db, convQ, "s005"), "closed/")
			})
		}
	}
}

// Owner ruling 2026-11-05 (Q1): an answer of the previous helper whose send
// outcome is unknown, of any age, is imported dropped (it does not hold its
// conversation's sends back) and listed for the engine's history check,
// with what the check matches on: the old outbox id (its hidden marker),
// the recorded Teams message id, the creation time and the text. Other
// unknown kinds keep their rules (o6, a title: unknown or stale-dropped).
func TestUnknownAnswerListedForHistoryCheck(t *testing.T) {
	for _, v := range []string{"v0120", "rc59"} {
		for _, stale := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stale=%v", v, stale), func(t *testing.T) {
				_, db := dryRun(t, fixture(t, v, "sqlite-wal"), func(o *Options) {
					if stale {
						o.StaleUnsent = 0
					}
				})
				eq(t, "o2", qs(t, db, `SELECT state || '/' || note FROM outbox WHERE delivery_key = 'legacy-outbox:o2'`),
					"dropped/migrated: send outcome unknown (sending); checked against the chat history at first start, resent only if absent")
				var list []store.LegacyFinalCheck
				if err := json.Unmarshal([]byte(qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaLegacyFinalChecks)), &list); err != nil {
					t.Fatal(err)
				}
				s001 := qi(t, db, `SELECT id FROM conversations WHERE label = 's001'`)
				want := store.LegacyFinalCheck{Key: "legacy-outbox:o2", ConvID: s001, LegacyID: "o2",
					Created: time.Date(2026, 9, 1, 10, 0, 20, 0, time.UTC).UnixMilli(), Body: "maybe posted"}
				if len(list) != 1 || list[0] != want {
					t.Fatalf("checks = %+v, want [%+v]", list, want)
				}
				o6 := map[bool]string{false: "unknown", true: "dropped"}[stale]
				eq(t, "o6 (a title, not an answer)", qs(t, db, outQ, "legacy-outbox:o6"), o6)
				sum := qs(t, db, `SELECT value FROM meta WHERE key = ?`, store.MetaMigrationSummary)
				line := "\n\n1 answer from 2026-09-01 that the previous helper could not confirm as sent is checked against the chat history now, and sent only if it is missing."
				if !strings.Contains(sum, line) {
					t.Errorf("summary lacks %q:\n%s", line, sum)
				}
			})
		}
	}
}

// scopedFixture copies testdata/<version>/<form> into <tmp>/teams/scopes/<id>,
// where rc.59 keeps a scope's store.
func scopedFixture(t *testing.T, version, form string) string {
	t.Helper()
	src := fixture(t, version, form)
	dst := filepath.Join(t.TempDir(), "teams", "scopes", "scope_fixture0001")
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(src, dst); err != nil {
		t.Fatal(err)
	}
	return dst
}

// rc59RefusesGuard applies rc.59's readRuntimeStoreMigrationCleanup
// (scope_runtime.go) to the guard: a regular file under 4 KiB with valid
// JSON whose source_path is not a scopes/*/state.json makes rc.59 refuse to
// start on the store.
func rc59RefusesGuard(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, OldHelperGuardName)
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("guard: %v", err)
	}
	if !fi.Mode().IsRegular() || fi.Size() > 4096 {
		t.Fatalf("guard is not a small regular file: %v %d", fi.Mode(), fi.Size())
	}
	raw, _ := os.ReadFile(path)
	var m struct {
		SourcePath string `json:"source_path"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("guard is not JSON rc.59 can parse: %v", err)
	}
	sp := filepath.Clean(strings.TrimSpace(m.SourcePath))
	if sp == "." || (filepath.Base(sp) == "state.json" && filepath.Base(filepath.Dir(filepath.Dir(sp))) == "scopes") {
		t.Fatalf("rc.59 would accept source_path %q", m.SourcePath)
	}
	return m.SourcePath
}

// Converting an rc.59 scope leaves a file beside the old store that makes
// rc.59 refuse to start there (another host sharing the home would serve
// the old store next to the new helper), and leaves the old store's data
// byte-identical. A dry run writes nothing, a live old writer still stops
// the conversion (the guard stays: that writer is refused at its next
// start), and a file rc.59 wrote itself is kept.
func TestRealRunGuardsTheOldHelper(t *testing.T) {
	t.Run("converted", func(t *testing.T) {
		dir := scopedFixture(t, "rc59", "sqlite-wal")
		names := []string{"state.json", "store.sqlite", "store.sqlite-wal"}
		sum := func() map[string][32]byte {
			out := map[string][32]byte{}
			for _, n := range names {
				b, err := os.ReadFile(filepath.Join(dir, n))
				if err != nil {
					t.Fatal(err)
				}
				out[n] = sha256.Sum256(b)
			}
			return out
		}
		before := sum()
		if _, err := Run(Options{Source: dir, Now: func() time.Time { return testNow }}); err != nil {
			t.Fatal(err)
		}
		if sp := rc59RefusesGuard(t, dir); !strings.Contains(sp, "delete this file") {
			t.Errorf("the refusal does not say how to roll back: %q", sp)
		}
		after := sum()
		for _, n := range names {
			if after[n] != before[n] {
				t.Errorf("%s changed", n)
			}
		}
	})
	t.Run("dry run", func(t *testing.T) {
		dir := scopedFixture(t, "rc59", "sqlite-wal")
		if _, err := Run(Options{Source: dir, Out: t.TempDir(), DryRun: true, StaleUnsent: -1, Now: func() time.Time { return testNow }}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dir, OldHelperGuardName)); !os.IsNotExist(err) {
			t.Fatalf("dry run wrote the guard: %v", err)
		}
	})
	t.Run("live old writer", func(t *testing.T) {
		dir := scopedFixture(t, "rc59", "sqlite-wal")
		db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "store.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		lease := `{"scope_id":"scope:fixture0001","holder_machine_id":"machine:x","status":"active","generation":4,"lease_until":"` +
			testNow.Add(time.Hour).Format(time.RFC3339) + `"}`
		if _, err := db.Exec(`UPDATE runtime_state SET json = ? WHERE key = 'control_lease'`, lease); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if _, err := Run(Options{Source: dir, Now: func() time.Time { return testNow }}); !errors.Is(err, ErrLiveWriter) {
			t.Fatalf("got %v", err)
		}
		rc59RefusesGuard(t, dir)
	})
	t.Run("rc.59's own cleanup marker is kept", func(t *testing.T) {
		dir := scopedFixture(t, "rc59", "sqlite-wal")
		own := []byte(`{"source_path":"/x/scopes/a/state.json"}` + "\n")
		if err := os.WriteFile(filepath.Join(dir, OldHelperGuardName), own, 0o600); err != nil {
			t.Fatal(err)
		}
		rep, err := Run(Options{Source: dir, Now: func() time.Time { return testNow }})
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(filepath.Join(dir, OldHelperGuardName)); string(got) != string(own) {
			t.Fatalf("rc.59's marker was replaced: %s", got)
		}
		if !strings.Contains(strings.Join(rep.Notes, "\n"), OldHelperGuardName) {
			t.Fatalf("no report note: %q", rep.Notes)
		}
	})
	t.Run("not a scope directory", func(t *testing.T) {
		dir := fixture(t, "v0120", "sqlite-wal")
		if _, err := Run(Options{Source: dir, Now: func() time.Time { return testNow }}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dir, OldHelperGuardName)); !os.IsNotExist(err) {
			t.Fatalf("guard outside a scopes dir: %v", err)
		}
	})
}
