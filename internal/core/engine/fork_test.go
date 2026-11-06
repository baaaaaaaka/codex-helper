package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// forkCutoff (gap #20): the end of the parent's history a fork copies.
func TestForkCutoff(t *testing.T) {
	const ts = `{"timestamp":"2026-09-01T00:00:00Z",`
	meta := ts + `"type":"session_meta","payload":{"id":"th"}}` + "\n"
	start := func(turn string) string {
		return ts + `"type":"event_msg","payload":{"type":"task_started","turn_id":"` + turn + `"}}` + "\n"
	}
	ctx := func(turn string) string {
		return ts + `"type":"turn_context","payload":{"turn_id":"` + turn + `","cwd":"/w"}}` + "\n"
	}
	user := func(text string) string {
		return ts + `"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"` + text + `"}]}}` + "\n"
	}
	done := func(turn string) string {
		return ts + `"type":"event_msg","payload":{"type":"task_complete","turn_id":"` + turn + `","last_agent_message":"a"}}` + "\n"
	}
	aborted := func(turn string) string {
		return ts + `"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"` + turn + `"}}` + "\n"
	}
	t1 := start("t1") + ctx("t1") + user("one") + done("t1")
	t2 := start("t2") + ctx("t2") + user("two") + done("t2")
	open2 := start("t2") + ctx("t2") + user("two")
	cases := []struct {
		name, body, cutoff string
		want               int // bytes of body copied
	}{
		{"every completed turn", meta + t1 + t2, "", len(meta + t1 + t2)},
		{"a running turn is left out", meta + t1 + open2, "", len(meta + t1)},
		{"cut at the last completed turn while one runs", meta + t1 + open2, "t1", len(meta + t1)},
		{"a turn that completed after the cut is left out", meta + t1 + t2, "t1", len(meta + t1)},
		{"the cut turn's own context does not end it", meta + t1 + t2 + start("t3") + user("three"), "t2", len(meta + t1 + t2)},
		{"an aborted turn ended", meta + t1 + start("t2") + user("two") + aborted("t2"), "", len(meta + t1 + start("t2") + user("two") + aborted("t2"))},
		{"a partial last line is not copied", meta + t1 + `{"timestamp":"2026`, "", len(meta + t1)},
		{"an unknown cutoff copies every completed turn", meta + t1 + open2, "gone", len(meta + t1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "r.jsonl")
			if err := os.WriteFile(path, []byte(c.body), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := forkCutoff(path, c.cutoff)
			if err != nil || got != int64(c.want) {
				t.Fatalf("forkCutoff = %d, %v; want %d (copied %q)", got, err, c.want, strings.TrimSpace(c.body[:min(int(got), len(c.body))]))
			}
		})
	}
}

// copyPrefix and rolloutPrefix: the snapshot is the exact prefix; the
// child's start is the end of its last complete line.
func TestForkSnapshotFiles(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.jsonl")
	if err := os.WriteFile(src, []byte("a\nbb\nccc"), 0o600); err != nil {
		t.Fatal(err)
	}
	off, n, err := rolloutPrefix(src)
	if err != nil || off != 5 || n != 2 {
		t.Fatalf("rolloutPrefix = %d, %d, %v; want 5, 2", off, n, err)
	}
	dst := filepath.Join(dir, "fork-history", "child.jsonl")
	if err := copyPrefix(src, dst, off); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(dst); err != nil || string(b) != "a\nbb\n" {
		t.Fatalf("snapshot = %q, %v", b, err)
	}
	if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left: %v", err)
	}
}
