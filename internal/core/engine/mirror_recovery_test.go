package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

func TestMirrorUnprovedReplacementPreservesCursor(t *testing.T) {
	for _, tc := range []string{"no checkpoint", "lost anchor", "interrupted legacy recovery"} {
		t.Run(tc, func(t *testing.T) {
			ctx := context.Background()
			sessions := t.TempDir()
			w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.AppendTurn(rollout.Turn{Prompt: "old prompt", Answer: "old answer"}); err != nil {
				t.Fatal(err)
			}
			before, err := transcript.Stat(w.Path)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			clock := func() time.Time { return now }
			st, err := store.OpenWith(filepath.Join(t.TempDir(), "core.db"), store.Options{Now: clock})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if _, err := st.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
				t.Fatal(err)
			}
			var source store.TranscriptSource
			if err := st.Update(func(tx *store.Tx) error {
				conv, err := tx.CreateConversation(store.NewConversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: "work"})
				if err != nil {
					return err
				}
				if err := tx.Bind(store.Thread{ConvID: conv, ThreadID: w.SessionID, Cwd: w.Cwd}); err != nil {
					return err
				}
				source = store.TranscriptSource{Path: w.Path, ThreadID: w.SessionID, ConvID: conv, State: store.SrcTracking}
				if tc != "lost anchor" {
					source.Offset, source.Size, source.MTime, source.Inode = before.Size, before.Size, ms(before.MTime), before.Inode
				}
				source.ID, err = tx.UpsertSource(source)
				if err == nil && tc == "interrupted legacy recovery" {
					err = tx.SetMeta(realignKey(source.ID), "end:999999")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			load := func() *Engine {
				t.Helper()
				e, err := New(Config{Store: st, Channel: channeltest.New(), Agent: newAgent(), SessionsDir: sessions, Now: clock})
				if err != nil {
					t.Fatal(err)
				}
				if err := e.load(); err != nil {
					t.Fatal(err)
				}
				if err := e.mirrorInit(); err != nil {
					t.Fatal(err)
				}
				return e
			}
			e := load()
			if tc == "lost anchor" {
				e.mirrorRead(ctx, e.mirror.files[w.Path], before)
			}
			checkCursor := func() store.TranscriptSource {
				t.Helper()
				var src store.TranscriptSource
				if err := st.Read(func(tx *store.Tx) error {
					var err error
					src, _, err = tx.SourceByID(source.ID)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				return src
			}
			want := checkCursor()
			original, err := os.ReadFile(w.Path)
			if err != nil {
				t.Fatal(err)
			}
			native := "accepted-native"
			var turn store.Turn
			if tc == "lost anchor" {
				if err := st.Update(func(tx *store.Tx) error {
					admitted, err := tx.AdmitWindow("chat", source.ConvID, []store.InboundMsg{{ExtMsgID: "prompt", Sender: store.SenderOwner, Kind: store.InPrompt, Body: "team prompt"}}, "")
					if err != nil {
						return err
					}
					id := admitted[0].TurnID
					if err := tx.MarkStarting(id, w.SessionID); err != nil {
						return err
					}
					if err := tx.SetTranscriptProgress(id); err != nil {
						return err
					}
					if err := tx.MarkRunning(id, native); err != nil {
						return err
					}
					turn, _, err = tx.Turn(id)
					if err != nil {
						return err
					}
					return tx.FinishTurn(id, store.TurnCompleted, "", []store.OutMsg{{Kind: store.OutFinal, DeliveryKey: "same-final-key", Body: "durable final"}})
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Replace(func(lines [][]byte) [][]byte {
				return lines[:1] // keep session id, discard all completed turns
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := w.AppendTurn(rollout.Turn{Prompt: "ambiguous prompt", Answer: "ambiguous answer"}); err != nil {
				t.Fatal(err)
			}
			current, err := transcript.Stat(w.Path)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				// The final was already waiting before restart. Each newly
				// observed obstruction still gets the verification grace.
				now = now.Add(20 * time.Second)
				e = load()
				f := e.mirror.files[w.Path]
				e.mirrorRead(ctx, f, current)
				if tc != "interrupted legacy recovery" {
					select {
					case job := <-e.mirror.rebindJobs:
						res := verifyMirrorBinding(ctx, job)
						if res.err == nil {
							t.Fatal("unproved replacement passed verification")
						}
						e.applyMirrorRebind(res)
					default:
						t.Fatal("replacement not queued for verification")
					}
				}
				if !e.historyBlocked(source.ConvID, source.ThreadID) {
					t.Fatal("unproved source did not block transcript mirroring")
				}
				if got := checkCursor(); got != want {
					t.Fatalf("unverified replacement changed cursor: got %+v, want %+v", got, want)
				}
				if err := st.Read(func(tx *store.Tx) error {
					rows, err := tx.PendingOutbox(source.ConvID)
					for _, row := range rows {
						if strings.Contains(row.Body, "ambiguous") || row.Recovery() {
							t.Errorf("queued unverified history: %q", row.Body)
						}
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			if tc != "lost anchor" {
				return
			}
			e.convs[source.ConvID].transcriptOpen = true // restored from held outbox on Run
			held := func(wantHold int64) {
				t.Helper()
				if err := st.Read(func(tx *store.Tx) error {
					rows, err := tx.PendingOutbox(source.ConvID)
					if err != nil {
						return err
					}
					found := 0
					for _, row := range rows {
						if strings.Contains(row.Body, "late transcript progress") {
							t.Error("late transcript progress was queued after its final")
						}
						if row.DeliveryKey == "same-final-key" {
							found++
							if row.HoldTurn != wantHold || row.Body != "durable final" {
								t.Errorf("held outcome = %+v, want hold %d", row, wantHold)
							}
						}
					}
					if found != 1 {
						t.Errorf("durable outcome has %d rows", found)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			e.releaseBlockedProgress()
			held(turn.ID)
			now = now.Add(mirrorGapWait - time.Millisecond)
			e.releaseBlockedProgress()
			held(turn.ID)
			now = now.Add(time.Millisecond)
			e.releaseBlockedProgress()
			held(0)

			if err := os.WriteFile(w.Path, original, 0600); err != nil {
				t.Fatal(err)
			}
			w = rollout.Open(w.Path, w.SessionID, w.Cwd)
			if _, err := w.AppendTurn(rollout.Turn{ID: native, ClientID: turn.TurnKey, Prompt: "team prompt", Commentary: "late transcript progress", Answer: "durable final"}); err != nil {
				t.Fatal(err)
			}
			current, err = transcript.Stat(w.Path)
			if err != nil {
				t.Fatal(err)
			}
			f := e.mirror.files[w.Path]
			f.rebindNext = time.Time{}
			e.mirrorRead(ctx, f, current)
			var job rebindRequest
			select {
			case job = <-e.mirror.rebindActive:
			case job = <-e.mirror.rebindJobs:
			default:
				t.Fatal("restored source was not checked")
			}
			res := verifyMirrorBinding(ctx, job)
			if res.err != nil {
				t.Fatal(res.err)
			}
			e.applyMirrorRebind(res)
			e.mirrorRead(ctx, f, current)
			if e.historyBlocked(source.ConvID, source.ThreadID) || checkCursor().Offset <= want.Offset {
				t.Fatal("restored source did not resume")
			}
			held(0)

		})
	}
}

func TestMirrorRebindRequiresUniqueRelocatedAnchor(t *testing.T) {
	w, err := rollout.New(t.TempDir(), t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRecord("fixture-padding", map[string]any{"data": strings.Repeat("x", 16<<10)}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "anchor prompt", Answer: "anchor response"}); err != nil {
		t.Fatal(err)
	}
	fh, err := os.Open(w.Path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := transcript.StatOpen(fh)
	if err != nil {
		t.Fatal(err)
	}
	lines, _, err := transcript.Read(fh, 0, info.Size)
	if err != nil {
		t.Fatal(err)
	}
	tr := transcript.NewTracker(w.SessionID, 0, 0, nil)
	tr.Feed(lines)
	off, line := tr.Resume()
	source := store.TranscriptSource{Path: w.Path, ThreadID: w.SessionID, Inode: info.Inode, Offset: off, Line: line}
	f := mirrorFile{src: source, tracker: tr}
	proof, _, err := f.checkpoint(fh, info.Device, off, line, tr.Offset())
	if err != nil || proof.AnchorEnd != off || len(proof.Anchor) == 0 {
		fh.Close()
		t.Fatalf("fixture failed to capture anchor at %d: %+v, %v", off, proof, err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
	check := func() rebindResult {
		t.Helper()
		now, err := transcript.Stat(w.Path)
		if err != nil {
			t.Fatal(err)
		}
		return verifyMirrorBinding(context.Background(), rebindRequest{src: source, info: now, proof: proof})
	}
	if err := w.Replace(func(lines [][]byte) [][]byte { return append(lines[:1:1], lines[2:]...) }); err != nil {
		t.Fatal(err)
	}
	got := check()
	if want := off - (lines[1].End - lines[1].Off); got.err != nil || got.off != want || got.line != line-1 {
		t.Fatalf("relocated resume = %d:%d, want %d:%d, err %v", got.off, got.line, want, line-1, got.err)
	}
	if err := w.Replace(func(lines [][]byte) [][]byte { return append(lines, lines[1:]...) }); err != nil {
		t.Fatal(err)
	}
	if got := check(); got.err != errAnchorDuplicate {
		t.Fatalf("duplicate anchor must be held, got %v", got.err)
	}
}
