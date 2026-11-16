package engine

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// Recovery retains uncertain content instead of assuming it was delivered. The incomplete
// ledger is injected only into a stopped, temporary test store.
func TestMirrorCopiedRolloutRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		replace, missing, appendTurn bool
	}{
		{"unchanged_missing_ledger", false, true, false},
		{"copy_complete_ledger", true, false, false},
		{"copy_missing_ledger", true, true, false},
		{"copy_append_missing_ledger", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, sessions := mirrorRig(t)
			r.mirrorInitialised()
			stage, cwd := t.TempDir(), t.TempDir()
			w, err := rollout.New(stage, cwd, rollout.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = w.AppendTurn(rollout.Turn{Prompt: "review old prompt", Answer: "review old answer"}); err != nil {
				t.Fatal(err)
			}
			appendCompaction(t, w.Path)
			if _, err = w.AppendTurn(rollout.Turn{Prompt: "review second prompt", Answer: "review second answer"}); err != nil {
				t.Fatal(err)
			}
			rel, err := filepath.Rel(stage, w.Path)
			if err != nil {
				t.Fatal(err)
			}
			dst := filepath.Join(sessions, rel)
			if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.Rename(w.Path, dst); err != nil {
				t.Fatal(err)
			}
			chat := r.localChat()
			r.wait("original output delivered", func() bool {
				return len(r.own(chat, transcript.CompactText)) == 1 && len(r.own(chat, "review second answer")) == 1
			})
			r.stop()
			original, err := transcript.Stat(dst)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if tc.missing {
				db, err := sql.Open("sqlite", r.path)
				if err != nil {
					t.Fatal(err)
				}
				res, err := db.Exec("DELETE FROM outbox WHERE kind='mirror' AND body LIKE ?", "%"+transcript.CompactText+"%")
				if err != nil {
					db.Close()
					t.Fatal(err)
				}
				n, err := res.RowsAffected()
				db.Close()
				if err != nil || n != 1 {
					t.Fatalf("removed %d rows, err=%v; want exactly 1", n, err)
				}
			}
			if tc.replace {
				tmp := filepath.Join(stage, "copy.jsonl")
				if err = os.WriteFile(tmp, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Chtimes(tmp, original.MTime, original.MTime); err != nil {
					t.Fatal(err)
				}
				if err = os.Rename(tmp, dst); err != nil {
					t.Fatal(err)
				}
				copied, err := transcript.Stat(dst)
				if err != nil {
					t.Fatal(err)
				}
				if copied.Inode == original.Inode || copied.Size != original.Size || !copied.MTime.Equal(original.MTime) {
					t.Fatalf("copy did not preserve size/mtime and change identity: %+v -> %+v", original, copied)
				}
			}
			if tc.appendTurn {
				added := rollout.Open(dst, w.SessionID, cwd)
				if _, err = added.AppendTurn(rollout.Turn{Prompt: "review new prompt", Answer: "review new answer"}); err != nil {
					t.Fatal(err)
				}
			}
			current, err := transcript.Stat(dst)
			if err != nil {
				t.Fatal(err)
			}
			r.start()
			// Wait for the replacement checkpoint and all its queued sends, rather
			// than guessing a delay after startup.
			r.wait("recovered source and drained outbox", func() bool {
				srcs, err := r.st.Sources()
				if err != nil {
					return false
				}
				for _, src := range srcs {
					if src.Path == dst && src.Inode == current.Inode && src.Size == current.Size {
						drained := false
						err = r.st.Read(func(tx *store.Tx) error { rows, e := tx.PendingOutbox(src.ConvID); drained = len(rows) == 0; return e })
						return err == nil && drained
					}
				}
				return false
			})
			if tc.appendTurn {
				r.wait("new answer delivered", func() bool { return len(r.own(chat, "review new answer")) == 1 })
			}
			r.stop()
			want := 1
			if tc.replace && tc.missing {
				want = 2
			}
			if n := len(r.own(chat, transcript.CompactText)); n != want {
				t.Errorf("compaction posted %d times; want %d (missing ledger must recover content)", n, want)
			}
			if tc.replace && tc.missing && len(r.own(chat, "Recovered history")) != 1 {
				t.Error("missing recovery label")
			}
			for _, s := range []string{"review old prompt", "review old answer", "review second prompt", "review second answer"} {
				if n := len(r.own(chat, s)); n != 1 {
					t.Errorf("%q posted %d times; want 1", s, n)
				}
			}
		})
	}
}
