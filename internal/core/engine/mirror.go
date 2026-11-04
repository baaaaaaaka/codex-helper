package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// The mirror posts the turns a person runs in Codex itself (TUI, IDE) to
// the session's work chat: the prompt ("User: …") and the final answer of
// every turn the helper did not run. Unchanged files cost one stat per
// tick and no store access.

const (
	mirrorChunk    = 1 << 20 // bytes read per Read call
	mirrorPassMax  = 8 << 20 // bytes read per file per pass
	firstPromptMax = 256 << 10
)

type mirrorState struct {
	mu        sync.Mutex
	skipTurns map[string]bool // Codex turn ids the helper ran
	files     map[string]*mirrorFile
	known     map[string]string // path -> state of untracked sources (baseline/ignored/missing)
	// checked: the file state of each baseline rollout the last discovery
	// pass looked at and left as baseline; an unchanged file is skipped
	// without a store transaction (review L11).
	checked   map[string]transcript.FileInfo
	first     bool // first start of this store: pre-existing rollouts are baselined
	startedAt time.Time
}

type mirrorFile struct {
	src     store.TranscriptSource
	tracker *transcript.Tracker
	info    transcript.FileInfo
	// realign is set while a rewritten rollout is re-read from the start,
	// over as many passes as it takes (review M8): records already
	// delivered, and records at or before realignAt (the old file's last
	// processed modification time), are not posted again. Persisted in meta.
	realign   bool
	realignAt time.Time
	// fails and retryAt back off a rollout whose records failed to render
	// (review M7); the records are re-read, never dropped.
	fails   int
	retryAt time.Time
}

func realignKey(srcID int64) string { return fmt.Sprintf("%s%d", metaRealignPrefix, srcID) }

func (e *Engine) mirrorSkipTurn(codexTurnID string) {
	if e.mirror == nil || codexTurnID == "" {
		return
	}
	e.mirror.mu.Lock()
	e.mirror.skipTurns[codexTurnID] = true
	e.mirror.mu.Unlock()
}

// mirrorBound is called when a conversation gets a (new) thread.
func (e *Engine) mirrorBound(threadID string, conv int64) {
	if e.mirror != nil {
		wake(e.mirrorWake)
	}
}

// skip reports helper-run turns: by client id (every helper turn key starts
// with "cxp-"), or by the Codex turn ids recorded on turns (legacy turns).
func (e *Engine) mirrorSkip(clientID, turnID string) bool {
	if strings.HasPrefix(clientID, "cxp-") {
		return true
	}
	if turnID == "" {
		return false
	}
	e.mirror.mu.Lock()
	defer e.mirror.mu.Unlock()
	return e.mirror.skipTurns[turnID]
}

func (e *Engine) mirrorInit() error {
	m := &mirrorState{skipTurns: map[string]bool{}, files: map[string]*mirrorFile{}, known: map[string]string{}, checked: map[string]transcript.FileInfo{}, startedAt: e.now()}
	ids, err := e.st.CodexTurnIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		m.skipTurns[id] = true
	}
	v, _, err := e.st.Meta(metaMirrorInit)
	if err != nil {
		return err
	}
	from, _, err := e.st.Meta(store.MetaMigratedFrom)
	if err != nil {
		return err
	}
	// A fresh install baselines every existing rollout (no flood for a new
	// user). A migrated store is not fresh: the converter left the recent
	// interactive rollouts without a source row for discovery to catch up
	// (DECISIONS: 120-day window).
	m.first = v == "" && from == ""
	srcs, err := e.st.Sources()
	if err != nil {
		return err
	}
	e.mirror = m
	for _, s := range srcs {
		if s.State != store.SrcTracking || s.ConvID == 0 || !e.convOpen(s.ConvID) {
			m.known[s.Path] = s.State
			if s.State == store.SrcTracking {
				m.known[s.Path] = store.SrcIgnored // closed conversation: not tracked
			}
			continue
		}
		f := &mirrorFile{src: s, tracker: transcript.NewTracker(s.ThreadID, s.Offset, s.Line, e.mirrorSkip),
			info: transcript.FileInfo{Size: s.Size, MTime: fromMS(s.MTime), Inode: s.Inode}}
		if v, _, err := e.st.Meta(realignKey(s.ID)); err == nil && v != "" {
			var at int64
			if _, err := fmt.Sscan(v, &at); err == nil {
				f.realign, f.realignAt = true, fromMS(at)
			}
		}
		m.files[s.Path] = f
	}
	return nil
}

func (e *Engine) convOpen(id int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[id]
	return cs != nil && cs.state != store.ConvClosed
}

// mirrorLoop watches $CODEX_HOME/sessions.
func (e *Engine) mirrorLoop(ctx context.Context) {
	if err := e.mirrorInit(); err != nil {
		e.logf("mirror: %v", err)
		return
	}
	e.discover(ctx, true)
	if e.mirror.first {
		if err := e.update(func(tx *store.Tx) error { return tx.SetMeta(metaMirrorInit, "1") }); err != nil {
			e.logf("mirror: %v", err)
		}
	}
	stat := time.NewTicker(e.t.MirrorStat)
	defer stat.Stop()
	disc := time.NewTicker(e.t.Discovery)
	defer disc.Stop()
	full := time.NewTicker(e.t.FullWalk)
	defer full.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stat.C:
		case <-e.mirrorWake:
			e.discover(ctx, false)
		case <-disc.C:
			e.discover(ctx, false)
		case <-full.C:
			e.discover(ctx, true)
		}
		e.mirrorPoll(ctx)
	}
}

// discover walks the sessions directory for rollouts not seen yet (all
// directories when full, else only recent day directories).
func (e *Engine) discover(ctx context.Context, full bool) {
	now := e.now()
	dirsFrom := now.Add(-48 * time.Hour)
	if full {
		dirsFrom = time.Time{}
	}
	found, err := transcript.Walk(e.cfg.SessionsDir, now.Add(-transcript.Window), dirsFrom)
	if err != nil {
		e.logf("mirror walk: %v", err)
	}
	var baseline []store.TranscriptSource
	for _, f := range found {
		if ctx.Err() != nil {
			return
		}
		e.mirror.mu.Lock()
		_, tracked := e.mirror.files[f.Path]
		state, known := e.mirror.known[f.Path]
		e.mirror.mu.Unlock()
		if tracked {
			continue
		}
		if known && state != store.SrcBaseline {
			continue
		}
		if known && state == store.SrcBaseline {
			e.promoteBaseline(ctx, f)
			continue
		}
		if b, ok := e.newRollout(ctx, f); ok {
			baseline = append(baseline, b)
		}
	}
	if len(baseline) > 0 {
		err := e.update(func(tx *store.Tx) error {
			for _, b := range baseline {
				if _, err := tx.UpsertSource(b); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			e.logf("mirror baseline: %v", err)
			return
		}
		e.mirror.mu.Lock()
		for _, b := range baseline {
			e.mirror.known[b.Path] = b.State
		}
		e.mirror.mu.Unlock()
	}
}

// newRollout decides what to do with a rollout seen for the first time. It
// returns a source row to record in bulk (baseline/ignored), or tracks the
// file itself.
func (e *Engine) newRollout(ctx context.Context, f transcript.Found) (store.TranscriptSource, bool) {
	meta, err := transcript.ReadMeta(f.Path)
	if err != nil {
		return store.TranscriptSource{}, false // being written; next pass
	}
	src := store.TranscriptSource{Path: f.Path, ThreadID: meta.ID, Size: f.Info.Size, MTime: ms(f.Info.MTime), Inode: f.Info.Inode}
	if meta.Subagent() {
		src.State = store.SrcIgnored
		return src, true
	}
	e.mu.Lock()
	conv, bound := e.byThread[meta.ID]
	var closed, forked bool
	if cs := e.convs[conv]; bound && cs != nil {
		closed = cs.state == store.ConvClosed
		forked = cs.forkParent != 0
	}
	forked = forked || e.forkThreads[meta.ID]
	e.mu.Unlock()
	switch {
	case bound && closed:
		src.State = store.SrcIgnored
		return src, true
	case bound:
		src.State, src.ConvID = store.SrcTracking, conv
		if forked {
			// A fork's rollout starts with a copy of the parent's history,
			// which is already in the parent chat.
			src.Offset = f.Info.Size
		}
		e.track(src)
		return store.TranscriptSource{}, false
	case e.mirror.first && f.Info.MTime.Before(e.mirror.startedAt),
		f.Info.MTime.Before(e.now().Add(-transcript.Window)):
		src.State, src.Offset = store.SrcBaseline, f.Info.Size
		return src, true
	case !meta.Interactive():
		src.State = store.SrcIgnored
		return src, true
	}
	e.mirrorSession(ctx, src, meta, 0)
	return store.TranscriptSource{}, false
}

// promoteBaseline starts mirroring a baselined rollout that grew: a person
// continued an old session.
func (e *Engine) promoteBaseline(ctx context.Context, f transcript.Found) {
	e.mirror.mu.Lock()
	last, seen := e.mirror.checked[f.Path]
	e.mirror.mu.Unlock()
	if seen && last == f.Info {
		return
	}
	stay := func() {
		e.mirror.mu.Lock()
		e.mirror.checked[f.Path] = f.Info
		e.mirror.mu.Unlock()
	}
	var src store.TranscriptSource
	var ok bool
	if err := e.update(func(tx *store.Tx) error {
		var err error
		src, ok, err = tx.Source(f.Path)
		return err
	}); err != nil || !ok {
		return
	}
	if f.Info.Size <= src.Offset && f.Info.Inode == src.Inode {
		stay()
		return
	}
	meta, err := transcript.ReadMeta(f.Path)
	if err != nil {
		return
	}
	if meta.Subagent() || !meta.Interactive() {
		stay()
		return
	}
	e.mu.Lock()
	conv, bound := e.byThread[meta.ID]
	e.mu.Unlock()
	if bound {
		if !e.convOpen(conv) {
			return
		}
		src.State, src.ConvID = store.SrcTracking, conv
		e.track(src)
		return
	}
	e.mirrorSession(ctx, src, meta, src.Offset)
}

// track records a tracking source and starts following it.
func (e *Engine) track(src store.TranscriptSource) {
	err := e.update(func(tx *store.Tx) error {
		id, err := tx.UpsertSource(src)
		src.ID = id
		if err != nil {
			return err
		}
		cur, _, err := tx.SourceByID(id)
		src.Offset, src.Line = cur.Offset, cur.Line
		return err
	})
	if err != nil {
		e.logf("mirror track %s: %v", filepath.Base(src.Path), err)
		return
	}
	e.mirror.mu.Lock()
	delete(e.mirror.known, src.Path)
	delete(e.mirror.checked, src.Path)
	e.mirror.files[src.Path] = &mirrorFile{src: src, tracker: transcript.NewTracker(src.ThreadID, src.Offset, src.Line, e.mirrorSkip)}
	e.mirror.mu.Unlock()
}

// mirrorSession creates a work conversation for a local session and tracks
// its rollout from offset.
func (e *Engine) mirrorSession(ctx context.Context, src store.TranscriptSource, meta transcript.SessionMeta, offset int64) {
	first := transcript.FirstPrompt(src.Path, firstPromptMax)
	var id int64
	var label string
	err := e.update(func(tx *store.Tx) error {
		var err error
		label, err = e.nextLabel(tx)
		if err != nil {
			return err
		}
		name := oneLine(first, 48)
		if name == "" {
			name = filepath.Base(meta.Cwd)
		}
		id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName,
			Title: chatTitle(label, name), TitleSource: "auto"})
		if err != nil {
			return err
		}
		if err := tx.Bind(store.Thread{ConvID: id, ThreadID: meta.ID, Cwd: meta.Cwd}); err != nil {
			return err
		}
		src.ConvID, src.State = id, store.SrcTracking
		if src.ID, err = tx.UpsertSource(src); err != nil {
			return err
		}
		_, err = e.refresh(tx, id)
		return err
	})
	if err != nil {
		e.logf("mirror session %s: %v", meta.ID, err)
		return
	}
	e.mu.Lock()
	if cs := e.convs[id]; cs != nil {
		cs.cwd, cs.threadID = meta.Cwd, meta.ID
	}
	e.byThread[meta.ID] = id
	e.intro[id] = "Work chat created: " + label + ". This chat mirrors a local Codex session in `" + meta.Cwd +
		"`: turns run in Codex itself appear here. Work chat is ready: messages you send here continue the same session."
	e.mu.Unlock()
	e.mirror.mu.Lock()
	delete(e.mirror.known, src.Path)
	delete(e.mirror.checked, src.Path)
	e.mirror.files[src.Path] = &mirrorFile{src: src, tracker: transcript.NewTracker(meta.ID, offset, src.Line, e.mirrorSkip)}
	e.mirror.mu.Unlock()
	e.logf("mirroring local session %s as %s", meta.ID, label)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.createChat(ctx, id)
	}()
}

func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	return clipText(s, n)
}

// mirrorPoll stats every tracked rollout and reads the changed ones.
func (e *Engine) mirrorPoll(ctx context.Context) {
	e.mirror.mu.Lock()
	files := make([]*mirrorFile, 0, len(e.mirror.files))
	for _, f := range e.mirror.files {
		files = append(files, f)
	}
	e.mirror.mu.Unlock()
	for _, f := range files {
		if ctx.Err() != nil {
			return
		}
		info, err := transcript.Stat(f.src.Path)
		if err != nil {
			continue // removed or unreadable; keep the row
		}
		if info == f.info && f.tracker.Offset() >= info.Size {
			continue
		}
		if e.now().Before(f.retryAt) {
			continue
		}
		e.mirrorRead(ctx, f, info)
	}
}

// mirrorRead reads new records of one rollout and posts them.
func (e *Engine) mirrorRead(ctx context.Context, f *mirrorFile, info transcript.FileInfo) {
	if !e.convOpen(f.src.ConvID) {
		e.mirror.mu.Lock()
		delete(e.mirror.files, f.src.Path)
		e.mirror.known[f.src.Path] = store.SrcIgnored
		e.mirror.mu.Unlock()
		return
	}
	// A shrink below the read offset needs no inode (Windows: the file
	// index stands in for it, see transcript.Stat).
	rewritten := transcript.Rewritten(info, f.tracker.Offset(), f.src.Inode)
	realignAt := f.realignAt
	if rewritten {
		e.logf("mirror: %s was rewritten; realigning", filepath.Base(f.src.Path))
		f.tracker.Restart()
		realignAt = fromMS(f.src.MTime)
	}
	realign := rewritten || f.realign
	fh, err := os.Open(f.src.Path)
	if err != nil {
		return
	}
	var outs []transcript.Output
	read := int64(0)
	for read < mirrorPassMax {
		lines, next, err := transcript.Read(fh, f.tracker.Offset(), mirrorChunk)
		if err != nil {
			e.logf("mirror read %s: %v", filepath.Base(f.src.Path), err)
			break
		}
		if len(lines) == 0 {
			f.tracker.Advance(next)
			break
		}
		outs = append(outs, f.tracker.Feed(lines)...)
		f.tracker.Advance(next)
		read += next - lines[0].Off
	}
	fh.Close()
	more := f.tracker.Offset() < info.Size && read >= mirrorPassMax

	// Catch-up window per record (§9.4): older records are not posted.
	horizon := e.now().Add(-transcript.Window)
	kept := outs[:0]
	for _, o := range outs {
		if !o.Time.IsZero() && o.Time.Before(horizon) {
			continue
		}
		kept = append(kept, o)
	}
	outs = kept

	// Render outside the transaction.
	thread := f.tracker.ThreadID()
	if thread == "" {
		thread = f.src.ThreadID
	}
	type rendered struct {
		o    transcript.Output
		msgs []store.OutMsg
	}
	var rs []rendered
	for _, o := range outs {
		msgs, err := e.renderMirror(ctx, thread, o)
		if err != nil {
			// Re-read these records from the persisted position after a
			// backoff (review M7): never lose them.
			f.fails++
			back := e.t.MirrorStat << min(f.fails, 8)
			if back > 5*time.Minute {
				back = 5 * time.Minute
			}
			f.retryAt = e.now().Add(back)
			if f.fails <= 3 || f.fails%20 == 0 {
				e.logf("mirror render %s (%d in a row, retry in %s): %v", filepath.Base(f.src.Path), f.fails, back, err)
			}
			f.tracker = transcript.NewTracker(f.src.ThreadID, f.src.Offset, f.src.Line, e.mirrorSkip)
			return
		}
		rs = append(rs, rendered{o: o, msgs: msgs})
	}
	f.fails, f.retryAt = 0, time.Time{}
	off, line := f.tracker.Resume()
	if off == 0 && f.src.Offset > 0 {
		off = 1 // the store never moves a source back to 0; 1 re-aligns to line 2
	}
	woke := false
	var posted int
	err = e.update(func(tx *store.Tx) error {
		posted, woke = 0, false
		var msgs []store.OutMsg
		start := 0
		if realign {
			// Realign: in this pass, drop everything up to the last record
			// already delivered.
			last := -1
			for i, r := range rs {
				ok, err := tx.Delivered(transcript.MirrorKeyPrefix(thread, r.o.RecordID))
				if err != nil {
					return err
				}
				if ok {
					last = i
				}
			}
			start = last + 1
		}
		for _, r := range rs[start:] {
			ok, err := tx.Delivered(transcript.MirrorKeyPrefix(thread, r.o.RecordID))
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			if realign && (r.o.Time.IsZero() || !r.o.Time.After(realignAt)) {
				continue // in the file before the rewrite: already handled
			}
			msgs = append(msgs, r.msgs...)
			posted++
		}
		// Realignment lasts until a pass reaches the end of the file, across
		// passes and restarts.
		switch {
		case realign && more:
			if err := tx.SetMeta(realignKey(f.src.ID), fmt.Sprint(ms(realignAt))); err != nil {
				return err
			}
		case realign:
			if err := tx.SetMeta(realignKey(f.src.ID), ""); err != nil {
				return err
			}
		}
		if posted > 0 || info.Size > f.src.Size {
			c, ok, err := tx.Conversation(f.src.ConvID)
			if err != nil {
				return err
			}
			if ok && c.State == store.ConvParked {
				if err := tx.Wake(c.ID); err != nil {
					return err
				}
				woke = true
			}
		}
		return tx.AdvanceSource(f.src.ID, off, line, info.Size, ms(info.MTime), info.Inode, msgs)
	})
	if err != nil {
		e.logf("mirror advance %s: %v", filepath.Base(f.src.Path), err)
		// Re-read from the persisted position next time.
		f.tracker = transcript.NewTracker(f.src.ThreadID, f.src.Offset, f.src.Line, e.mirrorSkip)
		return
	}
	f.src.Offset, f.src.Line, f.src.Size, f.src.MTime, f.src.Inode = off, line, info.Size, ms(info.MTime), info.Inode
	f.realign, f.realignAt = realign && more, realignAt
	if !more {
		f.info = info
	}
	if woke {
		_ = e.update(func(tx *store.Tx) error { _, err := e.refresh(tx, f.src.ConvID); return err })
		e.logf("mirror: woke %d (local activity)", f.src.ConvID)
	}
	if posted > 0 || woke {
		e.heat(f.src.ConvID)
		e.wakeSender()
	}
}

// renderMirror renders one mirrored record. Prompts read "User: <text>",
// answers carry the Codex label; neither mentions the owner.
func (e *Engine) renderMirror(ctx context.Context, thread string, o transcript.Output) ([]store.OutMsg, error) {
	var doc channel.RichDoc
	if o.User {
		text := strings.TrimSpace(o.Text)
		if blockStart(text) {
			text = "**" + labelUser + ":**\n\n" + text
		} else {
			text = "**" + labelUser + ":** " + text
		}
		doc = channel.RichDoc{Markdown: text}
	} else {
		doc = channel.RichDoc{Label: labelCodex, Markdown: o.Text, Math: true}
	}
	parts, err := e.ch.Render(ctx, doc)
	if err != nil {
		return nil, err
	}
	out := make([]store.OutMsg, 0, len(parts))
	for _, p := range parts {
		out = append(out, store.OutMsg{DeliveryKey: o.Key(thread, p.Index), Kind: store.OutMirror, Part: p.Index, Parts: p.Count,
			Body: p.Body, BodyHash: hashBytes(p.Hash)})
	}
	return out, nil
}

// blockStart reports Markdown that must start its own block.
func blockStart(s string) bool {
	for _, p := range []string{"#", "```", "~~~", "- ", "* ", "+ ", "> ", "|", "    ", "\t"} {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	if i := strings.IndexAny(s, ".)"); i > 0 && i < 10 {
		if _, err := fmt.Sscanf(s[:i], "%d", new(int)); err == nil {
			return true
		}
	}
	return false
}
