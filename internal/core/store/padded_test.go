package store

import (
	"crypto/sha256"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

// Real history on the owner's machine after 4.5 months (STORE-DESIGN §6),
// times 10. There is no retention, so hot paths must not grow with these.
const (
	padConvs    = 5_800
	padTurns    = 91_000
	padInbound  = 1_180_000
	padOutbox   = 450_000
	padSources  = 80_000
	padBodySize = 1_200 // average outbox body on the real store
)

// TestPaddedHistoryKeepsHotPathsFlat pads a store to 10x the real history and
// checks that every hot statement still uses an index (no SCAN of a hot
// table) and that a hot-path cycle is not materially slower than on an
// empty store.
func TestPaddedHistoryKeepsHotPathsFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("padded history test skipped in -short mode")
	}
	if raceEnabled {
		// A latency check on one goroutine: the race detector has nothing to
		// find here, and the race build of the pure-Go SQLite pads 10x the
		// real history about 30x slower (10 minutes on 4 CPUs) and skews the
		// latencies the test compares. Every non-race job runs it in full.
		t.Skip("padded history latency test skipped under -race")
	}
	s, c := openTest(t)
	a := newWork(t, s, "a")
	cycle := hotCycle(t, s, c, a)

	small := timeCycles(cycle, 300)

	start := time.Now()
	pad(t, s)
	padTime := time.Since(start)
	st, err := os.Stat(s.Path())
	mustT(t, err)
	t.Logf("padded in %s: %d conversations, %d turns, %d inbound, %d outbox, %d sources; db %.0f MB",
		padTime.Round(time.Millisecond), padConvs, padTurns, padInbound, padOutbox, padSources, float64(st.Size())/1e6)

	checkHotPlans(t, s)

	padded := timeCycles(cycle, 300)
	t.Logf("hot-path cycle median: empty %s, padded %s (p90 %s / %s)", small.p50, padded.p50, small.p90, padded.p90)
	if padded.p50 > 3*small.p50+time.Millisecond {
		t.Errorf("hot path grew with history: median %s -> %s", small.p50, padded.p50)
	}
}

type latency struct{ p50, p90 time.Duration }

func timeCycles(cycle func(i int), n int) latency {
	for i := 0; i < 20; i++ { // warm up
		cycle(i)
	}
	d := make([]time.Duration, n)
	for i := range d {
		t0 := time.Now()
		cycle(1000 + i)
		d[i] = time.Since(t0)
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return latency{d[n/2], d[n*9/10]}
}

// hotCycle is one round of the runtime's hot paths on conversation a: an
// unchanged poll, a new prompt run to completion and sent, an echo check,
// an unchanged transcript scan, and the runnable/sendable lookups.
func hotCycle(t *testing.T, s *Store, c *clock, a int64) func(i int) {
	window := []InboundMsg{prompt("w1", 1), prompt("w2", 2)}
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.AdmitWindow("teams:chat:a", a, window, "c0")
		return err
	}))
	src := TranscriptSource{Path: "/r/hot.jsonl", ThreadID: "th-a", ConvID: a, State: SrcTracking, Size: 500, MTime: 77, Inode: 9}
	hash := sha256.Sum256([]byte("echo"))
	return func(i int) {
		c.add(time.Millisecond)
		ext := "hot-" + strconv.Itoa(i)
		mustT(t, s.Update(func(tx *Tx) error {
			if _, err := tx.AdmitWindow("teams:chat:a", a, window, "c0"); err != nil { // unchanged
				return err
			}
			adm, err := tx.AdmitWindow("teams:chat:a", a, []InboundMsg{prompt(ext, int64(10+i))}, "c0")
			if err != nil || len(adm) == 0 {
				return err
			}
			if _, _, err := tx.NextSteer(a); err != nil {
				return err
			}
			next, ok, err := tx.NextRunnable(a)
			if err != nil || !ok {
				return err
			}
			if err := tx.MarkStarting(next.ID, "th-a"); err != nil {
				return err
			}
			if err := tx.MarkRunning(next.ID, "ct-"+ext); err != nil {
				return err
			}
			return tx.FinishTurn(next.ID, TurnCompleted, "", []OutMsg{out("final-"+ext, OutFinal)})
		}))
		mustT(t, s.Update(func(tx *Tx) error {
			rows, err := tx.NextSendable(c.now().UnixMilli())
			if err != nil {
				return err
			}
			for _, o := range rows {
				if _, err := tx.BeginSend(o.ID); err != nil {
					return err
				}
				if err := tx.MarkSent(o.ID, "x-"+o.DeliveryKey); err != nil {
					return err
				}
			}
			if _, err := tx.IsOwn(a, "x-final-"+ext, hash[:], c.now().UnixMilli()-600_000); err != nil {
				return err
			}
			id, err := tx.UpsertSource(src)
			if err != nil {
				return err
			}
			return tx.AdvanceSource(id, 500, 4, 500, 77, 9, nil)
		}))
	}
}

// pad bulk-inserts terminal history into other conversations.
func pad(t *testing.T, s *Store) {
	t.Helper()
	steps := []string{
		`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < ?1)
INSERT INTO conversations(label,kind,channel,ext_chat_id,state,parked_at,created_at,updated_at)
SELECT 'pad-' || value, 'work', 'teams', 'pad-chat-' || value,
  CASE WHEN value % 10 = 0 THEN 'closed' ELSE 'parked' END, value, value, value
FROM n`,
		`INSERT INTO threads(conv_id,thread_id,cwd,updated_at)
SELECT id, 'pad-thread-' || id, '/pad', 1 FROM conversations WHERE label LIKE 'pad-%'`,
		`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < ?2)
INSERT INTO turns(conv_id,turn_key,state,thread_id,model,queued_at,started_at,finished_at)
SELECT (SELECT MIN(id) FROM conversations WHERE label LIKE 'pad-%') + value % ?1, 'pad-t-' || value,
  CASE WHEN value % 13 = 0 THEN 'failed' WHEN value % 17 = 0 THEN 'interrupted' ELSE 'completed' END,
  'pad-thread', 'gpt-5', value, value, value + 1
FROM n`,
		`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < ?2)
INSERT INTO inbound(conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body)
SELECT (SELECT MIN(id) FROM conversations WHERE label LIKE 'pad-%') + value % ?1, 'pad-m-' || value, value, value,
  CASE WHEN value % 4 = 0 THEN 'other' ELSE 'owner' END, 'prompt',
  CASE WHEN value % 4 = 0 THEN 'ignored' ELSE 'done' END, 'padding prompt body ' || value
FROM n`,
		`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < ?2)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,body_hash,state,ext_msg_id,created_at,sent_at)
SELECT (SELECT MIN(id) FROM conversations WHERE label LIKE 'pad-%') + value % ?1, 'pad-o-' || value, value,
  CASE value % 5 WHEN 0 THEN 'final' WHEN 1 THEN 'ack' ELSE 'mirror' END,
  substr(printf('%0' || ?3 || 'd', value), 1, ?3), randomblob(32),
  CASE WHEN value % 40 = 0 THEN 'dropped' ELSE 'sent' END, 'pad-x-' || value, value, value
FROM n`,
		`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < ?2)
INSERT INTO transcript_sources(path,thread_id,conv_id,state,offset,line,size,mtime,inode,updated_at)
SELECT '/pad/rollout-' || value || '.jsonl', 'pad-thread-' || value,
  CASE WHEN value % 10 = 0 THEN (SELECT MIN(id) FROM conversations WHERE label LIKE 'pad-%') + value % ?1 END,
  CASE WHEN value % 10 = 0 THEN 'tracking' WHEN value % 3 = 0 THEN 'baseline' ELSE 'ignored' END,
  value, value, value, value, value, value
FROM n`,
	}
	args := [][]any{
		{padConvs},
		nil,
		{padConvs, padTurns},
		{padConvs, padInbound},
		{padConvs, padOutbox, padBodySize},
		{padConvs, padSources},
	}
	for i, q := range steps {
		mustT(t, s.Update(func(tx *Tx) error {
			_, err := tx.exec(q, args[i]...)
			return err
		}))
	}
}
