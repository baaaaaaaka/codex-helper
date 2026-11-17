package codexhistory

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSessionPreviewHiddenLongSequenceFlushesBothBudgets(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	path, store := newSessionPreviewCachedFile(t, previewRecord(0)+"\n")
	oldNow, oldHook := sessionPreviewNow, sessionPreviewSQLiteCommitHook
	t.Cleanup(func() { sessionPreviewNow, sessionPreviewSQLiteCommitHook = oldNow, oldHook })
	now := time.Unix(1000000, 0)
	sessionPreviewNow = func() time.Time { return now }
	commits := 0
	sessionPreviewSQLiteCommitHook = func(string) { commits++ }
	hidden := `{"type":"response_item","payload":{"type":"function_call_output","output":"hidden"}}` + "\n"
	// Cross the existing byte threshold without advancing the clock.
	for i := 0; i < 1000; i++ {
		appendFile(t, path, hidden)
		if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if commits != 1 {
		t.Fatalf("byte budget commits=%d, want 1", commits)
	}
	now = now.Add(sessionPreviewAppendFlushDelay + time.Second)
	appendFile(t, path, hidden)
	if _, err := ReadSessionPreviewText(path, 0, 0); err != nil {
		t.Fatal(err)
	}
	if commits != 2 {
		t.Fatalf("time budget commits=%d, want 2", commits)
	}
	entry, _, err := store.loadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if entry.offset != info.Size() || entry.nextOrdinal != 1 {
		t.Fatalf("hidden progress: %+v", entry)
	}
	// The candidate lookup must seek the source index, never scan all messages.
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN SELECT source_id FROM preview_message WHERE session_id=? AND generation=? AND source_id<>'' AND source_id IN (?)`, entry.sessionID, entry.generation, "response_item:missing")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	plan := ""
	for rows.Next() {
		var a, b, c int
		var detail string
		if err := rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "SEARCH preview_message USING COVERING INDEX preview_message_source") {
		t.Fatal(plan)
	}
}

// Opt in: two real minutes, production thresholds, an old visible string kept
// alive as the UI does. This is a stability probe, not a unit timing assertion.
func TestSessionPreviewLongRealTimeProbe(t *testing.T) {
	if os.Getenv("CXP_PREVIEW_LONG_PROBE") != "1" {
		t.Skip("opt-in two-minute preview stability probe")
	}
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	path := filepath.Join(os.Getenv(EnvCodexDir), "large.jsonl")
	var source strings.Builder
	for i := 0; i < 60000; i++ {
		fmt.Fprintf(&source, "{\"type\":\"response_item\",\"payload\":{\"id\":\"m-%d\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"%s\"}]}}\n", i, strings.Repeat("answer ", 32))
	}
	if err := os.WriteFile(path, []byte(source.String()), 0600); err != nil {
		t.Fatal(err)
	}
	source.Reset()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	oldText, err := ReadSessionPreviewText(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var warm runtime.MemStats
	runtime.ReadMemStats(&warm)
	oldHook := sessionPreviewSQLiteCommitHook
	t.Cleanup(func() { sessionPreviewSQLiteCommitHook = oldHook })
	commits := 0
	sessionPreviewSQLiteCommitHook = func(string) { commits++ }
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	start := time.Now()
	reads := 0
	var peak uint64
	for time.Since(start) < 2*time.Minute {
		line := `{"type":"response_item","payload":{"type":"function_call_output","output":"hidden"}}` + "\n"
		if reads%100 == 99 {
			line = fmt.Sprintf("{\"type\":\"response_item\",\"payload\":{\"id\":\"new-%d\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"visible append\"}]}}\n", reads)
		}
		if _, err := f.WriteString(line); err != nil {
			t.Fatal(err)
		}
		text, err := ReadSessionPreviewText(path, 0, 0)
		if err != nil || len(text) < len(oldText) {
			t.Fatalf("read %d: %v", reads, err)
		}
		var sample runtime.MemStats
		runtime.ReadMemStats(&sample)
		if sample.HeapAlloc > peak {
			peak = sample.HeapAlloc
		}
		reads++
		time.Sleep(50 * time.Millisecond)
	}
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)
	runtime.KeepAlive(oldText)
	if commits < 3 {
		t.Fatalf("did not span production time flushes: %d", commits)
	}
	if final.HeapAlloc > warm.HeapAlloc+40<<20 {
		t.Fatalf("unbounded retained heap: warm=%d final=%d", warm.HeapAlloc, final.HeapAlloc)
	}
	t.Logf("duration=%s reads=%d commits=%d body=%d heap-before=%d warm=%d final=%d sampled-peak=%d allocations=%d", time.Since(start), reads, commits, len(oldText), before.HeapAlloc, warm.HeapAlloc, final.HeapAlloc, peak, final.TotalAlloc-warm.TotalAlloc)
}
