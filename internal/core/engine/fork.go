package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// Fork (SPEC F-WRK-10 / F-CTL-13 / R-41; v0.1.21-rc.6 1a210e6). Work
// `helper fork`, or control `fork <session>`:
//
//  1. The requesting chat gets "⏳ Fork requested…".
//  2. Codex forks the thread at the last completed turn (thread/fork with
//     lastTurnId while a turn runs; every completed turn otherwise).
//  3. The child chat gets its notice, then the parent's visible history up
//     to that cutoff, then rc.6's marker "History import complete. The new
//     Codex chat is ready.".
//  4. Once the marker is delivered (every earlier message of the child chat
//     is then settled: a chat's outbox is sent strictly in order), the
//     parent gets "✅ Fork complete. Open the new Codex chat: <url>" and the
//     control chat its "Work chat created" notice, in the transaction that
//     marks the marker sent (forkSettled): the link never comes before the
//     history, and it comes once.
//
// The history comes from the parent's rollout, not the child's: Codex
// 0.166 writes no copy of the parent's records into the child's rollout
// (its history is read through thread/read), 0.145 copies the turns up to
// the cutoff and 0.133 copies the running turn too (realcodex probe
// TestRealCodexForkRollout). Like rc.6's immutable manifest, the parent's
// rollout up to the cutoff is copied once into a snapshot file
// (FilesDir/fork-history/<child thread>.jsonl) before the child exists,
// and a history import (import.go, kind importFork) mirrors that file
// into the child chat as catch-up, helper-run turns included, under a
// delivery key namespace of the child chat (the parent's keys are already
// delivered in the parent chat). So, as for `continue`, every record is
// posted once whatever crashes in between: the cursor and the job are in
// the store and the delivery keys deduplicate.
//
// The child's own rollout is mirrored from the end of what it held when
// the fork returned (a copied prefix, or only its session_meta), recorded
// as its transcript source in the fork's transaction; a rollout Codex
// names no path for is mirrored from its start. So the child's own records
// are mirrored once, with no gap and nothing of the parent's twice.

// Fork texts (0.1.21-rc.6 1a210e6, SPEC F-WRK-10 steps 1, 3 and 4).
const (
	textForkRequested   = "⏳ Fork requested. I fixed the parent cutoff and am preparing the new chat; I will send its link only after the visible history is confirmed."
	textForkHistoryDone = "History import complete. The new Codex chat is ready."
)

func textForkComplete(url string) string {
	return "✅ Fork complete. Open the new Codex chat:\n" + url
}

func textForkIntro(label string) string {
	return "Forked from " + label + ": this chat continues a copy of that session; " + label + " itself is unchanged. Send `helper help` for commands."
}

const (
	// metaForkChildPrefix + conv id: "1" for a chat made by a fork (its
	// notices are queued with the conversation; its rollout's start is
	// recorded, never guessed).
	metaForkChildPrefix = "engine_fork_child:"
	// forkMarkPrefix starts the delivery key of the child's marker.
	forkMarkPrefix = "forkhist:"
	forkDirName    = "fork-history"
)

func forkChildKey(conv int64) string          { return fmt.Sprintf("%s%d", metaForkChildPrefix, conv) }
func forkMarkBase(conv int64) string          { return fmt.Sprintf("%s%d", forkMarkPrefix, conv) }
func forkNS(thread string, conv int64) string { return fmt.Sprintf("%s@c%d", thread, conv) }

// isForkChild reports whether conv was made by a fork of this core.
func isForkChild(get func(string) (string, bool, error), conv int64) bool {
	v, _, err := get(forkChildKey(conv))
	return err == nil && v != ""
}

// cmdFork is work `helper fork` and control `fork <session>` (cs: the
// session forked).
func (e *Engine) cmdFork(ctx context.Context, j cmdJob, cs *convState) {
	e.mu.Lock()
	label, threadID, cwd, running := cs.label, cs.threadID, cs.cwd, cs.running != nil
	e.mu.Unlock()
	if threadID == "" {
		e.reply(j, "Nothing to fork yet: "+label+" has no Codex conversation. Send a request first.", nil)
		return
	}
	// A fork while a request runs (batch E; v0.1.20's `model fork` did not
	// wait either) is cut at the last completed turn, so the child never
	// holds a half-finished one.
	cutoff := ""
	if running {
		cutoff = e.lastCompletedCodexTurn(cs.id)
	}
	// Until the child is bound, discovery leaves new rollouts alone: the
	// child's must not become a local session of its own (newRollout).
	e.imp.forking.Add(1)
	defer e.imp.forking.Add(-1)
	child, err := e.ag.Fork(ctx, cs.id, threadID, cutoff, cwd)
	if err != nil {
		e.reply(j, "Fork failed: "+userError(err.Error())+". Nothing was changed.", nil)
		return
	}
	prep := e.forkPrepare(cs.id, threadID, cutoff, child)
	var id int64
	queued := false
	e.reply(j, textForkRequested, func(tx *store.Tx) error {
		id, queued = 0, false
		newLabel, err := e.nextLabel(tx)
		if err != nil {
			return err
		}
		cid, err := tx.CreateConversation(store.NewConversation{Label: newLabel, Kind: store.KindWork, Channel: e.cfg.ChannelName,
			Title: chatTitle(newLabel, "fork of "+label), TitleSource: "auto"})
		if err != nil {
			return err
		}
		parent, _, err := tx.Thread(cs.id)
		if err != nil {
			return err
		}
		if err := tx.Bind(store.Thread{ConvID: cid, ThreadID: child.ThreadID, Cwd: cwd, Model: parent.Model, ModelSource: parent.ModelSource,
			Effort: parent.Effort, EffortSource: parent.EffortSource, ForkParent: cs.id}); err != nil {
			return err
		}
		if err := tx.SetMeta(forkChildKey(cid), "1"); err != nil {
			return err
		}
		// The child chat: its notice, the history, the marker, in order.
		if err := e.enqueueNotice(tx, cid, "ready:"+newLabel, textForkIntro(label)); err != nil {
			return err
		}
		if prep.child.Path != "" {
			src := prep.child
			src.ConvID = cid
			if _, err := tx.UpsertSource(src); err != nil {
				return err
			}
		}
		if prep.snapshot != "" {
			if _, err := e.queueImport(tx, importJob{Conv: cid, Kind: importFork, Thread: threadID, Path: prep.snapshot,
				Rewind: true, KeepTeams: true, NS: forkNS(threadID, cid)}); err != nil {
				return err
			}
			queued = true
		} else if err := e.enqueueNotice(tx, cid, forkMarkBase(cid), textForkHistoryDone); err != nil {
			return err // no history to publish: the marker alone
		}
		if _, err := e.refresh(tx, cid); err != nil {
			return err
		}
		id = cid
		return nil
	})
	if id == 0 || e.conv(id) == nil {
		if prep.snapshot != "" {
			_ = os.Remove(prep.snapshot)
		}
		return
	}
	e.mu.Lock()
	if c := e.convs[id]; c != nil {
		c.cwd, c.threadID, c.forkParent = cwd, child.ThreadID, cs.id
	}
	e.byThread[child.ThreadID] = id
	e.mu.Unlock()
	if queued {
		e.importQueued(id)
	}
	wake(e.mirrorWake) // the child's rollout is bound now
	e.createChatNow(ctx, id)
}

// lastCompletedCodexTurn is the Codex turn id of the newest completed turn
// of a conversation ("" if none is known).
func (e *Engine) lastCompletedCodexTurn(conv int64) string {
	var id string
	_ = e.update(func(tx *store.Tx) error {
		id = ""
		turns, err := tx.RecentTurns(conv, retryHorizon)
		for _, tr := range turns {
			if tr.State == store.TurnCompleted && tr.CodexTurnID != "" {
				id = tr.CodexTurnID
				break
			}
		}
		return err
	})
	return id
}

// forkPrep is what a fork records before its transaction: the snapshot of
// the parent's history ("" = none to publish) and the child's rollout
// source (Path "" = Codex named none).
type forkPrep struct {
	snapshot string
	child    store.TranscriptSource
}

func (e *Engine) forkDir() string {
	if e.cfg.FilesDir == "" || e.cfg.SessionsDir == "" {
		return "" // no mirror (or nowhere to keep a snapshot): no history
	}
	return filepath.Join(e.cfg.FilesDir, forkDirName)
}

// forkPrepare snapshots the parent's visible history up to the fork's cutoff
// and records where the child's own rollout starts.
func (e *Engine) forkPrepare(conv int64, thread, cutoff string, child Forked) forkPrep {
	var p forkPrep
	if child.Path != "" {
		if off, line, err := rolloutPrefix(child.Path); err == nil {
			if info, err := e.statFile(child.Path); err == nil {
				p.child = store.TranscriptSource{Path: child.Path, ThreadID: child.ThreadID, State: store.SrcTracking,
					Offset: off, Line: line, Size: info.Size, MTime: ms(info.MTime), Inode: info.Inode}
			}
		}
	}
	dir := e.forkDir()
	if dir == "" {
		return p
	}
	src := e.parentRollout(conv, thread)
	if src == "" {
		e.logf("fork of %s: its rollout was not found; the fork gets no history", thread)
		return p
	}
	end, err := forkCutoff(src, cutoff)
	if err != nil || end <= 0 {
		e.logf("fork of %s: cutoff in %s: %v", thread, filepath.Base(src), err)
		return p
	}
	dst := filepath.Join(dir, child.ThreadID+".jsonl")
	if err := copyPrefix(src, dst, end); err != nil {
		e.logf("fork of %s: snapshot: %v", thread, err)
		return p
	}
	p.snapshot = dst
	return p
}

// parentRollout is the rollout of thread: its transcript source (conv's
// first), else the file under the sessions directory named by the id.
func (e *Engine) parentRollout(conv int64, thread string) string {
	dir := e.forkDir()
	if srcs, err := e.st.Sources(); err == nil {
		path := ""
		for _, s := range srcs {
			if s.ThreadID != thread || (dir != "" && filepath.Dir(s.Path) == dir) {
				continue // another thread, or a fork's snapshot
			}
			if s.ConvID == conv {
				return s.Path
			}
			path = s.Path
		}
		if path != "" {
			return path
		}
	}
	found, _ := transcript.Walk(e.cfg.SessionsDir)
	for _, f := range found {
		if strings.Contains(filepath.Base(f.Path), thread) {
			return f.Path
		}
	}
	return ""
}

// forkCutoff is the end of the history a fork copies from rollout path:
// the start of the first turn that begins after cutoffTurn ended. Without
// cutoffTurn (or when the rollout does not hold it) every turn that ended
// is copied: the end is the start of a turn still open at the end of the
// file, else the end of its last complete line. A turn superseded by the
// next one without an end record ended there (Codex forks it as
// interrupted).
func forkCutoff(path, cutoffTurn string) (int64, error) {
	fh, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer fh.Close()
	var (
		off, end, openStart int64
		cur                 string
		open, passed        bool
	)
	for {
		lines, next, err := transcript.Read(fh, off, 1<<20)
		if err != nil {
			return 0, err
		}
		for _, l := range lines {
			ev := transcript.ParseLine(l)
			switch ev.Kind {
			case transcript.KindTurnStarted:
				if open && (ev.TurnID == "" || ev.TurnID == cur) {
					break // turn_context of the open turn
				}
				if cutoffTurn != "" && (passed || (open && cur == cutoffTurn)) {
					return l.Off, nil
				}
				open, cur, openStart = true, ev.TurnID, l.Off
			case transcript.KindTurnComplete, transcript.KindTurnAborted:
				id := ev.TurnID
				if id == "" {
					id = cur
				}
				if cutoffTurn != "" && id == cutoffTurn {
					passed = true
				}
				if id == cur {
					open = false
				}
			}
			end = l.End
		}
		if next == off {
			break
		}
		off = next
	}
	if open && !passed {
		return openStart, nil
	}
	return end, nil
}

// rolloutPrefix is the end of the last complete line of a rollout and the
// number of lines before it.
func rolloutPrefix(path string) (int64, int64, error) {
	fh, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer fh.Close()
	var off, n int64
	for {
		lines, next, err := transcript.Read(fh, off, 1<<20)
		if err != nil {
			return 0, 0, err
		}
		n += int64(len(lines))
		if next == off {
			return off, n, nil
		}
		off = next
	}
}

// copyPrefix writes the first n bytes of src to dst, durably, replacing it.
func copyPrefix(src, dst string, n int64) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = io.CopyN(out, in, n)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, dst)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if d, err := os.Open(filepath.Dir(dst)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// forkHistoryDone is importFinish for a fork's history: the marker closes
// the child's history, and the snapshot is retired (forkSweep removes the
// file).
func (e *Engine) forkHistoryDone(tx *store.Tx, j *importJob) error {
	if err := e.enqueueNotice(tx, j.Conv, forkMarkBase(j.Conv), textForkHistoryDone); err != nil {
		return err
	}
	if src, ok, err := tx.SourceByID(j.Src); err != nil {
		return err
	} else if ok {
		src.State = store.SrcIgnored
		if _, err := tx.UpsertSource(src); err != nil {
			return err
		}
	}
	e.imp.sweep.Store(true)
	return nil
}

// forkSettled is called in the transaction that settles an outbox row
// (sent, or dropped for good): the child's marker releases the link in the
// parent and the control chat's notice, once each (their delivery keys).
func (e *Engine) forkSettled(tx *store.Tx, r store.Outbox) error {
	if !strings.HasPrefix(r.DeliveryKey, forkMarkPrefix) || max(r.Part, 1) < max(r.Parts, 1) {
		return nil
	}
	c, ok, err := tx.Conversation(r.ConvID)
	if err != nil || !ok {
		return err
	}
	th, ok, err := tx.Thread(r.ConvID)
	if err != nil {
		return err
	}
	if ok && th.ForkParent != 0 {
		p, ok, err := tx.Conversation(th.ForkParent)
		if err != nil {
			return err
		}
		if ok && p.State != store.ConvClosed {
			if err := e.enqueueNotice(tx, th.ForkParent, fmt.Sprintf("fork-done:%d", r.ConvID), textForkComplete(c.ExtURL)); err != nil {
				return err
			}
		}
	}
	if err := e.enqueueControlCreated(tx, r.ConvID, c.Label, c.ExtURL); err != nil {
		return err
	}
	e.wakeSender()
	return nil
}

// forkSweep removes the snapshots no import job reads any more (mirror
// goroutine: after a fork's history was published, and at start).
func (e *Engine) forkSweep() {
	dir := e.forkDir()
	if dir == "" {
		return
	}
	if e.imp.forking.Load() > 0 {
		e.imp.sweep.Store(true) // a snapshot may be waiting for its job: later
		return
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	jobs, err := loadImports(e.st.Meta)
	if err != nil {
		return
	}
	used := map[string]bool{}
	for _, j := range jobs {
		used[j.Path] = true
	}
	for _, ent := range ents {
		path := filepath.Join(dir, ent.Name())
		if used[path] {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			e.logf("fork snapshot %s: %v", ent.Name(), err)
			continue
		}
		e.mirror.mu.Lock()
		delete(e.mirror.files, path)
		e.mirror.known[path] = store.SrcIgnored
		e.mirror.mu.Unlock()
	}
}
