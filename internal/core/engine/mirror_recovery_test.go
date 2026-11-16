package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// Drive each read and restart explicitly: no sender/reader goroutine or timing
// guess can hide a checkpoint or delivery ordering error.
func TestMirrorRecoveryAcrossRestartKeepsLiveTail(t *testing.T) {
	ctx := context.Background()
	sessions := t.TempDir()
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.AppendTurn(rollout.Turn{Prompt: "original question", Answer: "original full answer"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 701; i++ {
		appendCompaction(t, w.Path)
	}
	if _, err = w.AppendTurn(rollout.Turn{Prompt: "historical question", Commentary: "historical full progress", Answer: "historical full answer"}); err != nil {
		t.Fatal(err)
	}
	before, err := transcript.Stat(w.Path)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "core.db")
	ch := channeltest.New()
	var st *store.Store
	reopen := func() {
		t.Helper()
		if st != nil {
			st.Release()
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		st, err = store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = st.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	reopen()
	t.Cleanup(func() { st.Release(); st.Close() })
	var conv, srcID int64
	if err = st.Update(func(tx *store.Tx) error {
		var err error
		conv, err = tx.CreateConversation(store.NewConversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: "work"})
		if err != nil {
			return err
		}
		if err = tx.Bind(store.Thread{ConvID: conv, ThreadID: w.SessionID, Cwd: w.Cwd}); err != nil {
			return err
		}
		if err = tx.Park(conv); err != nil {
			return err
		}
		srcID, err = tx.UpsertSource(store.TranscriptSource{Path: w.Path, ThreadID: w.SessionID, ConvID: conv, State: store.SrcTracking, Offset: before.Size, Size: before.Size, MTime: ms(before.MTime), Inode: before.Inode + 1})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	load := func() *Engine {
		t.Helper()
		e, err := New(Config{Store: st, Channel: ch, Agent: newAgent(), SessionsDir: sessions})
		if err != nil {
			t.Fatal(err)
		}
		if err = e.load(); err != nil {
			t.Fatal(err)
		}
		if err = e.mirrorInit(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := load()
	f := e.mirror.files[w.Path]
	activity := e.convs[conv].lastActivity
	e.mirrorRead(ctx, f, before)
	if f.tracker.Offset() >= before.Size {
		t.Fatal("fixture did not span multiple passes")
	}
	if f.recoveryEnd != before.Size {
		t.Fatalf("boundary=%d want %d", f.recoveryEnd, before.Size)
	}
	if !e.convs[conv].lastActivity.Equal(activity) {
		t.Fatal("recovery heated idle chat")
	}
	if err = st.Read(func(tx *store.Tx) error {
		c, _, err := tx.Conversation(conv)
		if c.State != store.ConvParked {
			t.Fatal("recovery woke parked chat")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// The live turn appears after the recovery snapshot and must stay live even
	// though most of the old snapshot is unread when this process is stopped.
	if _, err = w.AppendTurn(rollout.Turn{Prompt: "new live question", Answer: "new live full answer"}); err != nil {
		t.Fatal(err)
	}
	reopen()
	e = load()
	f = e.mirror.files[w.Path]
	for pass := 0; pass < 30; pass++ {
		info, err := transcript.Stat(w.Path)
		if err != nil {
			t.Fatal(err)
		}
		e.mirrorRead(ctx, f, info)
		if f.tracker.Offset() >= info.Size {
			break
		}
		if pass == 29 {
			t.Fatal("recovery did not finish")
		}
	}
	if f.recoveryEnd != before.Size {
		t.Fatal("restart enlarged recovery range")
	}
	var rows []store.Outbox
	if err = st.Read(func(tx *store.Tx) error { var err error; rows, err = tx.PendingOutbox(conv); return err }); err != nil {
		t.Fatal(err)
	}
	compacts := 0
	for _, r := range rows {
		if strings.Contains(r.Body, transcript.CompactText) {
			compacts++
			if !r.Recovery() {
				t.Fatal("compaction lost recovery classification")
			}
		}
		if strings.Contains(r.Body, "new live") && r.Recovery() {
			t.Fatal("new append classified as recovery")
		}
	}
	if compacts != 701 {
		t.Fatalf("compaction records=%d want 701", compacts)
	}
	// Current messages go first; old rows retain their original identities and
	// full payloads. The same transaction freezes and begins every actual send.
	var sent []string
	for i := 0; i < 80; i++ {
		var r store.Outbox
		if err = st.Update(func(tx *store.Tx) error {
			heads, err := tx.NextSendable(tx.Now())
			if err != nil || len(heads) == 0 {
				return err
			}
			r = heads[0]
			r, err = e.progressBatch(tx, r, tx.Now())
			if err != nil {
				return err
			}
			if _, err = tx.BeginSend(r.ID); err != nil {
				return err
			}
			return tx.MarkSent(r.ID, fmt.Sprint("external-", i))
		}); err != nil {
			t.Fatal(err)
		}
		if r.ID == 0 {
			break
		}
		sent = append(sent, r.Body)
	}
	if len(sent) == 0 || !strings.Contains(sent[0], "new live question") {
		t.Fatalf("live tail blocked: %v", sent)
	}
	all := strings.Join(sent, "\n")
	for _, text := range []string{"original question", "original full answer", "historical question", "historical full progress", "historical full answer", "new live question", "new live full answer", "Recovered history"} {
		if !strings.Contains(all, text) {
			t.Errorf("lost %q", text)
		}
	}
	if len(sent) > 15 {
		t.Fatalf("recovery storm: %d transport posts for 701 compactions", len(sent))
	}
	if err = st.Read(func(tx *store.Tx) error {
		pending, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		if len(pending) != 0 {
			t.Fatalf("unsent content remains: %d", len(pending))
		}
		represented, err := tx.RecordedParts([]store.OutMsg{{DeliveryKey: rows[0].DeliveryKey}})
		if !represented {
			t.Fatal("batch lost individual identity")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Re-reading the same replacement after another process restart must not
	// create more content. Persisted recoveryEnd is retained for held outputs.
	reopen()
	e = load()
	f = e.mirror.files[w.Path]
	info, _ := transcript.Stat(w.Path)
	e.mirrorRead(ctx, f, info)
	if err = st.Read(func(tx *store.Tx) error {
		pending, err := tx.PendingOutbox(conv)
		if len(pending) != 0 {
			t.Fatalf("restart duplicated %d records", len(pending))
		}
		v, _, err2 := tx.Meta(realignKey(srcID))
		if !strings.HasPrefix(v, "end:") {
			t.Fatal("boundary not durable", v)
		}
		if err != nil {
			return err
		}
		return err2
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMirrorRecoveryTruncationAndLegacyMarker(t *testing.T) {
	// The integration fixture above models replacement; this exercises the
	// legacy marker upgrade and genuine shrink through the public detection.
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			r, sessions := mirrorRig(t)
			r.mirrorInitialised()
			w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = w.AppendTurn(rollout.Turn{Prompt: "before truncate", Answer: "old truncate answer"}); err != nil {
				t.Fatal(err)
			}
			chat := r.localChat()
			r.wait("old final", func() bool { return len(r.own(chat, "old truncate answer")) == 1 })
			r.stop()
			// Keep the session preamble, replace the remainder with a different turn,
			// and make the original longer first so this path is a real shrink.
			data, err := os.ReadFile(w.Path)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.SplitAfter(string(data), "\n")
			cut := 0
			for _, line := range lines {
				if strings.Contains(line, `"type":"task_started"`) {
					break
				}
				cut += len(line)
			}
			if cut >= len(data) {
				t.Fatal("missing turn start")
			}
			if err = os.WriteFile(w.Path, data[:cut], 0600); err != nil {
				t.Fatal(err)
			}
			n := rollout.Open(w.Path, w.SessionID, w.Cwd)
			if _, err = n.AppendTurn(rollout.Turn{ID: "after-truncate", Prompt: "short", Answer: "new short"}); err != nil {
				t.Fatal(err)
			}
			// Simulate the last committed old offset beyond this shortened file.
			db, err := store.Open(r.path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
				t.Fatal(err)
			}
			if err = db.Update(func(tx *store.Tx) error {
				src, _, err := tx.Source(w.Path)
				if err != nil {
					return err
				}
				if legacy {
					if err = tx.SetMeta(realignKey(src.ID), "1790000000000"); err != nil {
						return err
					}
				}
				return tx.AdvanceSource(src.ID, int64(len(data))+10000, src.Line, int64(len(data))+10000, src.MTime, src.Inode, nil)
			}); err != nil {
				t.Fatal(err)
			}
			db.Release()
			db.Close()
			r.start()
			r.wait("new truncated content delivered", func() bool { return len(r.own(chat, "new short")) == 1 })
			if len(r.own(chat, "Recovered history")) == 0 {
				t.Fatal("truncation content not labeled")
			}
		})
	}
}

func TestMirrorRecoveryKeepsMultipleOldChatsParked(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err = st.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	sessions := t.TempDir()
	const chats = 12
	for i := 0; i < chats; i++ {
		w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.AppendTurn(rollout.Turn{Prompt: fmt.Sprintf("old question %d", i), Answer: fmt.Sprintf("old answer %d", i)}); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < 70; n++ {
			appendCompaction(t, w.Path)
		}
		info, err := transcript.Stat(w.Path)
		if err != nil {
			t.Fatal(err)
		}
		if err = st.Update(func(tx *store.Tx) error {
			conv, err := tx.CreateConversation(store.NewConversation{Label: fmt.Sprintf("s%03d", i), Kind: store.KindWork, Channel: "teams", ExtChatID: fmt.Sprint(i)})
			if err != nil {
				return err
			}
			if err = tx.Bind(store.Thread{ConvID: conv, ThreadID: w.SessionID, Cwd: w.Cwd}); err != nil {
				return err
			}
			if err = tx.Park(conv); err != nil {
				return err
			}
			_, err = tx.UpsertSource(store.TranscriptSource{Path: w.Path, ThreadID: w.SessionID, ConvID: conv, State: store.SrcTracking, Offset: info.Size, Size: info.Size, MTime: ms(info.MTime), Inode: info.Inode + 1})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	e, err := New(Config{Store: st, Channel: channeltest.New(), Agent: newAgent(), SessionsDir: sessions})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.load(); err != nil {
		t.Fatal(err)
	}
	if err = e.mirrorInit(); err != nil {
		t.Fatal(err)
	}
	for _, f := range e.mirror.files {
		activity := e.convs[f.src.ConvID].lastActivity
		info, err := transcript.Stat(f.src.Path)
		if err != nil {
			t.Fatal(err)
		}
		e.mirrorRead(context.Background(), f, info)
		if !e.convs[f.src.ConvID].lastActivity.Equal(activity) {
			t.Fatal("recovery heated an idle chat")
		}
	}
	posts, records := 0, 0
	if err = st.Update(func(tx *store.Tx) error {
		for _, f := range e.mirror.files {
			c, _, err := tx.Conversation(f.src.ConvID)
			if err != nil {
				return err
			}
			if c.State != store.ConvParked {
				t.Fatal("old chat woke during recovery")
			}
			rows, err := tx.PendingOutbox(c.ID)
			if err != nil {
				return err
			}
			records += len(rows)
		}
		for {
			heads, err := tx.NextSendable(tx.Now())
			if err != nil || len(heads) == 0 {
				return err
			}
			for _, head := range heads {
				r, err := e.progressBatch(tx, head, tx.Now())
				if err != nil {
					return err
				}
				if _, err = tx.BeginSend(r.ID); err != nil {
					return err
				}
				posts++
				if err = tx.MarkSent(r.ID, fmt.Sprint("post-", posts)); err != nil {
					return err
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if records != chats*72 || posts != chats {
		t.Fatalf("records=%d posts=%d; want %d complete records in %d batches", records, posts, chats*72, chats)
	}
}
