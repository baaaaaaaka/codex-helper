package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
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
	checked map[string]transcript.FileInfo
	// unreadable: rollouts whose first record could not be read (being
	// written, or not a session); re-read only after they change.
	unreadable map[string]transcript.FileInfo
	// oldDirs: day directories older than the recent window still to be
	// walked in this cycle, a few per discovery tick (bounded work).
	oldDirs   []string
	lastCycle time.Time
	// recent: what the last walk of each recent day directory saw (B3).
	recent map[string]*recentDir
	first  bool // first start of this store: pre-existing rollouts are baselined
	// firstFound is what is left of the start-up walk (B4): every rollout
	// not seen yet, newest first, with its state at start. It is
	// classified a batch per stat tick; firstPending is set until done.
	firstFound []transcript.Found
	// firstAt is the state at start of each rollout still in firstFound:
	// another walk that meets one first classifies it as of the start.
	firstAt      map[string]transcript.FileInfo
	firstPending bool
	startedAt    time.Time
}

// recentDir is what the last walk of a recent day directory saw (B3).
type recentDir struct {
	mtime time.Time
	// settled: the walk ran dirSettle after mtime, so a rollout created
	// within the same mtime tick as an earlier one was listed too.
	settled bool
	// pending are the rollouts re-stat'ed while the directory is
	// unchanged: baselined ones (a person may continue the session),
	// unreadable ones (being written) and any not classified yet.
	// Tracked rollouts are stat'ed by mirrorPoll; ignored ones never
	// change state.
	pending []string
}

// dirSettle is how long after a directory's mtime its listing is trusted
// to be complete (coarse directory timestamps on some file systems).
const dirSettle = 5 * time.Second

// oldWalkFiles bounds the rollouts one discovery tick stats in old day
// directories (a full cycle over every directory spreads over many ticks).
const oldWalkFiles = 256

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
	// nextStat is when mirrorPoll stats the rollout next (B1).
	nextStat time.Time
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
	m := &mirrorState{skipTurns: map[string]bool{}, files: map[string]*mirrorFile{}, known: map[string]string{}, checked: map[string]transcript.FileInfo{}, unreadable: map[string]transcript.FileInfo{}, startedAt: e.now()}
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
	e.startFirstWalk()
	e.discoverFirst(ctx)
	stat := time.NewTicker(e.t.MirrorStat)
	defer stat.Stop()
	disc := time.NewTicker(e.t.Discovery)
	defer disc.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stat.C:
		case <-e.mirrorWake:
			e.discover(ctx)
		case <-disc.C:
			e.discover(ctx)
			e.discoverOld(ctx)
		}
		e.discoverFirst(ctx)
		e.mirrorPoll(ctx)
	}
}

// firstWalkFiles bounds the rollouts the start-up walk classifies per stat
// tick (B4): classifying reads each one's first record.
const firstWalkFiles = 128

// startFirstWalk lists every rollout once at start (a stat each, cheap) and
// queues them newest first for classification. Their sizes and times are
// the ones at start: a fresh install baselines what existed then, and a
// session continued while the queue drains is promoted from that size.
func (e *Engine) startFirstWalk() {
	m := e.mirror
	found, err := transcript.Walk(e.cfg.SessionsDir, e.now().Add(-transcript.Window), time.Time{})
	if err != nil {
		e.logf("mirror walk: %v", err)
	}
	slices.Reverse(found)
	m.firstFound, m.firstPending = found, true
	m.firstAt = make(map[string]transcript.FileInfo, len(found))
	for _, f := range found {
		m.firstAt[f.Path] = f.Info
	}
}

// discoverFirst classifies the next batch of the start-up walk (reading a
// rollout's first record is what costs). When it is done, the
// older-directory cycle starts and a fresh store is marked as initialised.
func (e *Engine) discoverFirst(ctx context.Context) {
	m := e.mirror
	if !m.firstPending {
		return
	}
	n := min(firstWalkFiles, len(m.firstFound))
	batch := m.firstFound[:n:n]
	m.firstFound = m.firstFound[n:]
	e.consider(ctx, batch)
	if ctx.Err() != nil || len(m.firstFound) > 0 {
		return
	}
	m.firstPending, m.firstFound, m.firstAt = false, nil, nil
	m.lastCycle = e.now()
	if m.first {
		if err := e.update(func(tx *store.Tx) error { return tx.SetMeta(metaMirrorInit, "1") }); err != nil {
			e.logf("mirror: %v", err)
		}
	}
}

// discoverOld walks the older day directories a few at a time: a session
// started long ago may have been resumed. One cycle over every directory
// starts at most every FullWalk; each tick stats about oldWalkFiles
// rollouts, so a large sessions tree never costs one long tick.
func (e *Engine) discoverOld(ctx context.Context) {
	m := e.mirror
	if m.firstPending {
		return // the start-up walk covers every directory
	}
	now := e.now()
	if len(m.oldDirs) == 0 {
		if now.Sub(m.lastCycle) < e.t.FullWalk {
			return
		}
		m.lastCycle = now
		dirs, err := transcript.DayDirs(e.cfg.SessionsDir)
		if err != nil {
			e.logf("mirror walk: %v", err)
		}
		recent := now.Add(-48 * time.Hour)
		for _, d := range dirs {
			if d.Day.Before(recent.Truncate(24*time.Hour).AddDate(0, 0, -1)) {
				m.oldDirs = append(m.oldDirs, d.Path)
			}
		}
	}
	since := now.Add(-transcript.Window)
	var found []transcript.Found
	for len(m.oldDirs) > 0 && len(found) < oldWalkFiles {
		dir := m.oldDirs[0]
		m.oldDirs = m.oldDirs[1:]
		f, err := transcript.Walk(dir, since, time.Time{})
		if err != nil {
			e.logf("mirror walk: %v", err)
		}
		found = append(found, f...)
	}
	e.consider(ctx, found)
}

// discover walks the recent day directories for rollouts not seen yet.
func (e *Engine) discover(ctx context.Context) { e.discoverRecent(ctx, e.now()) }

// discoverRecent walks the recent day directories (the last 48 h, with a
// day of margin either side for time zones). A directory whose mtime did
// not change since its last walk is not listed again (B3): a new rollout
// changes it. Only its pending rollouts are stat'ed then; appends to
// tracked rollouts are mirrorPoll's.
func (e *Engine) discoverRecent(ctx context.Context, now time.Time) {
	m := e.mirror
	if m.recent == nil {
		m.recent = map[string]*recentDir{}
	}
	since := now.Add(-transcript.Window)
	dirs := recentDayDirs(e.cfg.SessionsDir, now)
	var found []transcript.Found
	walked := map[string][]string{}
	for _, dir := range dirs {
		fi, err := os.Stat(dir)
		if err != nil {
			delete(m.recent, dir)
			continue
		}
		if rd := m.recent[dir]; rd != nil && rd.settled && fi.ModTime().Equal(rd.mtime) {
			for _, p := range rd.pending {
				if info, err := e.statFile(p); err == nil && !info.MTime.Before(since) {
					found = append(found, transcript.Found{Path: p, Info: info})
				}
			}
			continue
		}
		f, err := transcript.Walk(dir, since, time.Time{})
		if err != nil {
			e.logf("mirror walk: %v", err)
			delete(m.recent, dir)
		} else {
			m.recent[dir] = &recentDir{mtime: fi.ModTime(), settled: now.Sub(fi.ModTime()) >= dirSettle}
		}
		paths := make([]string, len(f))
		for i, x := range f {
			paths[i] = x.Path
		}
		walked[dir] = paths
		found = append(found, f...)
	}
	sort.SliceStable(found, func(i, j int) bool { return found[i].Info.MTime.Before(found[j].Info.MTime) })
	e.consider(ctx, found)
	// What stays pending after this pass.
	keep := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		keep[dir] = true
		rd := m.recent[dir]
		if rd == nil {
			continue
		}
		paths, ok := walked[dir]
		if !ok {
			paths = rd.pending
		}
		rd.pending = e.mirrorPending(paths)
	}
	for dir := range m.recent {
		if !keep[dir] {
			delete(m.recent, dir)
		}
	}
}

// mirrorPending keeps the rollouts discovery must stat again (see
// recentDir.pending).
func (e *Engine) mirrorPending(paths []string) []string {
	var out []string
	e.mirror.mu.Lock()
	defer e.mirror.mu.Unlock()
	for _, p := range paths {
		if _, tracked := e.mirror.files[p]; tracked {
			continue
		}
		if state, known := e.mirror.known[p]; known && state != store.SrcBaseline {
			continue
		}
		out = append(out, p)
	}
	return out
}

// recentDayDirs are the sessions/YYYY/MM/DD directories the recent walk
// visits: from the day before now-48h to two days after now (UTC dates; a
// local date is within a day of them).
func recentDayDirs(sessionsDir string, now time.Time) []string {
	from := now.UTC().Add(-48*time.Hour).Truncate(24*time.Hour).AddDate(0, 0, -1)
	to := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, 2)
	var out []string
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		out = append(out, filepath.Join(sessionsDir, d.Format("2006"), d.Format("01"), d.Format("02")))
	}
	return out
}

// consider handles the rollouts a walk found: new ones are classified
// (reading their first record once), grown baselined ones may be promoted;
// known and unchanged ones cost a map lookup.
func (e *Engine) consider(ctx context.Context, found []transcript.Found) {
	var baseline []store.TranscriptSource
	for _, f := range found {
		if ctx.Err() != nil {
			return
		}
		if at, ok := e.mirror.firstAt[f.Path]; ok {
			// Not classified yet by the start-up walk: as of the start
			// (a fresh install baselines what existed then).
			f.Info = at
			delete(e.mirror.firstAt, f.Path)
		}
		e.mirror.mu.Lock()
		_, tracked := e.mirror.files[f.Path]
		state, known := e.mirror.known[f.Path]
		last, unreadable := e.mirror.unreadable[f.Path]
		e.mirror.mu.Unlock()
		if tracked || (unreadable && last == f.Info) {
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
		// Being written, or not a session: read again once it changes.
		e.mirror.mu.Lock()
		e.mirror.unreadable[f.Path] = f.Info
		e.mirror.mu.Unlock()
		return store.TranscriptSource{}, false
	}
	e.mirror.mu.Lock()
	delete(e.mirror.unreadable, f.Path)
	e.mirror.mu.Unlock()
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
	delegated := e.delegThreads[meta.ID]
	e.mu.Unlock()
	if delegated {
		// A delegated task's thread: its result goes to the requester,
		// it is not a local session (no chat of its own).
		src.State = store.SrcIgnored
		return src, true
	}
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
		stay() // read again once it changes
		return
	}
	if meta.Subagent() || !meta.Interactive() {
		stay()
		return
	}
	e.mu.Lock()
	conv, bound := e.byThread[meta.ID]
	delegated := e.delegThreads[meta.ID]
	e.mu.Unlock()
	if delegated {
		stay()
		return
	}
	if bound {
		if !e.convOpen(conv) {
			stay()
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

// mirrorPoll stats the tracked rollouts that are due and reads the changed
// ones. The stat is tiered (B1): see mirrorStatEvery.
func (e *Engine) mirrorPoll(ctx context.Context) {
	now := e.now()
	e.mirror.mu.Lock()
	var due []*mirrorFile
	for _, f := range e.mirror.files {
		if !now.Before(f.nextStat) {
			due = append(due, f)
		}
	}
	e.mirror.mu.Unlock()
	for _, f := range due {
		if ctx.Err() != nil {
			return
		}
		first := f.nextStat.IsZero()
		info, err := e.statFile(f.src.Path)
		mtime := f.info.MTime
		if err == nil {
			mtime = info.MTime
		}
		every := e.mirrorStatEvery(f.src.ConvID, mtime, now)
		f.nextStat = now.Add(every)
		if first && every > 0 {
			// Spread the rollouts over the period, so the slow tiers
			// do not stat every file in the same tick.
			f.nextStat = now.Add(time.Duration(pathHash(f.src.Path) % uint64(every)))
		}
		if err != nil {
			continue // removed or unreadable; keep the row
		}
		if info == f.info && f.tracker.Offset() >= info.Size {
			continue
		}
		if now.Before(f.retryAt) {
			continue
		}
		e.mirrorRead(ctx, f, info)
		if f.tracker.Offset() < info.Size {
			f.nextStat = now.Add(e.t.MirrorStat) // more to read: next tick
		}
	}
}

// mirrorStatEvery is the stat period of a tracked rollout last modified at
// mtime (B1): MirrorStat while it changed within MirrorHotFor or its
// conversation runs a turn, MirrorParkedStat when its conversation is
// parked (a local session wakes it within that bound), MirrorIdleStat
// otherwise.
func (e *Engine) mirrorStatEvery(conv int64, mtime, now time.Time) time.Duration {
	if now.Sub(mtime) < e.t.MirrorHotFor {
		return e.t.MirrorStat
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[conv]
	switch {
	case cs == nil:
		return e.t.MirrorIdleStat
	case cs.running != nil:
		return e.t.MirrorStat
	case cs.state == store.ConvParked:
		return e.t.MirrorParkedStat
	}
	return e.t.MirrorIdleStat
}

// pathHash is a stable hash of a path (FNV-1a).
func pathHash(p string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(p); i++ {
		h ^= uint64(p[i])
		h *= 1099511628211
	}
	return h
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
