package engine

import (
	"context"
	"crypto/sha256"
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
// the session's work chat: the prompt ("🧑‍💻 User: …") and the final answer of
// every turn the helper did not run. Unchanged files cost one stat per
// tick and no store access.

const (
	mirrorChunk      = 1 << 20 // bytes read per Read call
	mirrorPassMax    = 8 << 20 // bytes read per file per pass
	firstPromptMax   = 256 << 10
	mirrorRecordsMax = 256
)

type mirrorState struct {
	bound           []string // bindings waiting for the reader to reconsider a pre-discovered source
	mu              sync.Mutex
	progressClients map[string]int64
	progressTurns   map[string]int64
	skipTurns       map[string]bool // helper-owned Codex turn id -> shared transport
	files           map[string]*mirrorFile
	known           map[string]string // path -> state of untracked sources (baseline/ignored/missing)
	// checked: the file state of each baseline rollout the last discovery
	// pass looked at and left as baseline; an unchanged file is skipped
	// without a store transaction (review L11).
	checked map[string]transcript.FileInfo
	// unreadable: rollouts whose first record could not be read (being
	// written, or not a session); re-read only after they change.
	unreadable map[string]transcript.FileInfo
	// oldDirs: day directories older than the recent ones still to be
	// walked in this cycle, a few per discovery tick (bounded work).
	oldDirs []string
	// oldFound: rollouts the cycle listed but has not considered yet; a
	// tick considers them for at most firstWalkClassify.
	oldFound  []transcript.Found
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
// directories (a full cycle over every directory, the whole tree with no
// age cut, spreads over many ticks).
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
	// catchUpEnd is the file end when this process began catching the
	// rollout up (catchUpSet once known): see catchingUp.
	catchUpEnd int64
	catchUpSet bool
	// held: the rollout's resume point is being checked against its
	// chat's history (history_check.go); nothing is read until decided.
	// proven: the end of the newest record the check found in the chat;
	// records up to it are not posted.
	held   bool
	proven int64
	// ns and keepTeams are set by a history import (import.go): the
	// delivery key namespace ("" = the thread id) and the file end up to
	// which helper-run turns are mirrored too.
	ns        string
	keepTeams int64
	// infoSaved is the tracker settings last persisted.
	infoSaved store.SourceInfo
}

// catchingUp is the one boundary between catch-up and live mirroring
// (coordinator decision, parity round 2026-11-05). A record that already
// existed when its source began catching up is catch-up: the source was
// behind its end when the engine started (a converted store included), or
// it is an old session found by discovery. Catch-up preserves all selected
// content and notifies nobody. A record past that end is live and can
// produce an owner notification for its final.
func (f *mirrorFile) catchingUp(o transcript.Output) bool { return o.End <= f.catchUpEnd }

// catchUpFrom sets the catch-up end of a rollout first seen in state at:
// what it held then if it existed when the engine started (atStart), else
// nothing (a session started since is live from its first record).
func catchUpFrom(at transcript.FileInfo, atStart bool) int64 {
	if atStart {
		return at.Size
	}
	return 0
}

func realignKey(srcID int64) string { return fmt.Sprintf("%s%d", metaRealignPrefix, srcID) }

// restoreInfo seeds a resumed tracker from typed, last-observed settings.
func (e *Engine) restoreInfo(f *mirrorFile, t *transcript.Tracker) {
	if f.src.ID == 0 || f.src.Offset == 0 {
		return
	}
	var saved store.SourceInfo
	var ok bool
	if err := e.st.Read(func(tx *store.Tx) (err error) { saved, ok, err = tx.SourceInfo(f.src.ID); return }); err != nil || !ok {
		return
	}
	i := transcript.Info{Model: saved.Model, Effort: saved.Effort, Tier: saved.Tier}
	if saved.HasGoal {
		i.Goal = &transcript.Goal{Objective: saved.GoalObjective, Status: saved.GoalStatus}
	}
	t.SetInfo(i)
	f.infoSaved = saved
}

func sourceInfo(i transcript.Info) store.SourceInfo {
	n := store.SourceInfo{Model: i.Model, Effort: i.Effort, Tier: i.Tier, HasGoal: i.Goal != nil}
	if i.Goal != nil {
		n.GoalObjective = i.Goal.Objective
		n.GoalStatus = i.Goal.Status
	}
	return n
}

func (e *Engine) mirrorSkipTurn(codexTurnID string) {
	e.mirrorSkipTurnMode(codexTurnID, false)
}
func (e *Engine) mirrorSkipTurnMode(codexTurnID string, shared bool) {
	if e.mirror == nil || codexTurnID == "" {
		return
	}
	e.mirror.mu.Lock()
	e.mirror.skipTurns[codexTurnID] = shared
	e.mirror.mu.Unlock()
}

// mirrorBound is called when a conversation gets a (new) thread.
func (e *Engine) mirrorBound(threadID string, conv int64) {
	if e.mirror != nil {
		e.mirror.mu.Lock()
		e.mirror.bound = append(e.mirror.bound, threadID)
		e.mirror.mu.Unlock()
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
	shared, owned := e.mirror.skipTurns[turnID]
	return owned && (clientID == "" || !shared)
}

func (e *Engine) mirrorInit() error {
	m := &mirrorState{skipTurns: map[string]bool{}, files: map[string]*mirrorFile{}, known: map[string]string{}, checked: map[string]transcript.FileInfo{}, unreadable: map[string]transcript.FileInfo{}, startedAt: e.now()}
	ids, err := e.st.CodexTurnModes()
	if err != nil {
		return err
	}
	m.skipTurns = ids
	m.progressTurns, m.progressClients, err = e.st.TranscriptProgressTurns()
	if err != nil {
		return err
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
	// user). A migrated store is not fresh: the converter left the
	// unlinked interactive rollouts, however old, without a source row for
	// discovery to catch up (no window, owner decision 2026-11-05).
	m.first = v == "" && from == ""
	srcs, err := e.st.Sources()
	if err != nil {
		return err
	}
	e.mirror = m
	e.importLoad() // import.go
	for _, s := range srcs {
		if s.State != store.SrcTracking || s.ConvID == 0 || !e.convOpen(s.ConvID) {
			m.known[s.Path] = s.State
			if s.State == store.SrcTracking {
				m.known[s.Path] = store.SrcIgnored // closed conversation: not tracked
			}
			continue
		}
		f := &mirrorFile{src: s, tracker: e.newMirrorTracker(s.ThreadID, s.Offset, s.Line, e.mirrorSkip),
			info: transcript.FileInfo{Size: s.Size, MTime: fromMS(s.MTime), Inode: s.Inode}}
		if v, _, err := e.st.Meta(realignKey(s.ID)); err == nil && v != "" {
			var at int64
			if _, err := fmt.Sscan(v, &at); err == nil {
				f.realign, f.realignAt = true, fromMS(at)
			}
		}
		e.restoreInfo(f, f.tracker)
		e.importRestore(f) // import.go
		m.files[s.Path] = f
	}
	e.mirrorHistory()
	return nil
}

func (e *Engine) convOpen(id int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[id]
	// The control chat's Codex questions are not mirrored (batch B).
	return cs != nil && (cs.state != store.ConvClosed || cs.transcriptOpen) && cs.kind != store.KindControl
}

// mirrorLoop watches $CODEX_HOME/sessions.
func (e *Engine) mirrorLoop(ctx context.Context) {
	if e.mirror == nil {
		if err := e.mirrorInit(); err != nil {
			e.logf("mirror: %v", err)
			e.catchUpReading.Store(false)
			return
		}
	}
	e.startFirstWalk()
	var discoverAfter time.Time
	stat := time.NewTicker(e.t.MirrorStat)
	defer stat.Stop()
	disc := time.NewTicker(e.t.Discovery)
	defer disc.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stat.C:
			// One start-up batch per stat tick, and none at start: the
			// listing above is the start's cost, and a wake or the
			// discovery tick in the same second adds no second batch
			// (W1b).
			e.discoverFirst(ctx)
		case <-e.mirrorWake:
			if !e.now().Before(discoverAfter) {
				e.discover(ctx)
				discoverAfter = e.now().Add(e.t.MirrorStat)
			}
		case <-e.historyWake:
			e.mirrorHistory() // a resume history check was decided
		case <-disc.C:
			e.discover(ctx)
			e.discoverOld(ctx)
		}
		e.reconsiderBoundSources(ctx)
		e.mirrorPoll(ctx)
		e.catchUpReading.Store(e.catchUpPending())
	}
}

// catchUpPending reports whether the catch-up still has records to read:
// the start-up walk is not done, or a rollout was not read up to its
// catch-up end yet. Its chat may not exist yet, so the new-chat summary
// waits for it (announcePass).
func (e *Engine) catchUpPending() bool {
	m := e.mirror
	if m.firstPending {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range m.files {
		if f.catchUpSet && f.tracker.Offset() < f.catchUpEnd {
			return true
		}
	}
	return false
}

// firstWalkFiles and firstWalkBudget bound a batch of the start-up walk
// per stat tick (B4, W1): classifying reads each rollout's first record,
// so a batch stops at whichever comes first, 128 rollouts or about 10 ms
// (a slow disk never makes one long tick). The budget covers the whole
// batch: classification stops at firstWalkClassify, which leaves the rest
// for the batch's store write (W1b).
const (
	firstWalkFiles    = 128
	firstWalkBudget   = 10 * time.Millisecond
	firstWalkClassify = firstWalkBudget / 2
)

// startFirstWalk lists every rollout once at start (a stat each, cheap) and
// queues them newest first for classification. Their sizes and times are
// the ones at start: a fresh install baselines what existed then, and a
// session continued while the queue drains is promoted from that size.
func (e *Engine) startFirstWalk() {
	m := e.mirror
	found, err := transcript.Walk(e.cfg.SessionsDir)
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
	n = e.considerFor(ctx, m.firstFound[:n:n], firstWalkClassify)
	m.firstFound = m.firstFound[n:]
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
// starts at most every FullWalk. A tick stats about oldWalkFiles rollouts
// and considers what it listed for at most firstWalkClassify; the rest
// waits for the next tick, which lists nothing new until it is done. So a
// large sessions tree, walked whole with no age cut, never costs one long
// tick.
func (e *Engine) discoverOld(ctx context.Context) {
	m := e.mirror
	if m.firstPending {
		return // the start-up walk covers every directory
	}
	if len(m.oldFound) == 0 {
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
		for len(m.oldDirs) > 0 && len(m.oldFound) < oldWalkFiles {
			dir := m.oldDirs[0]
			m.oldDirs = m.oldDirs[1:]
			f, err := transcript.Walk(dir)
			if err != nil {
				e.logf("mirror walk: %v", err)
			}
			m.oldFound = append(m.oldFound, f...)
		}
	}
	n := e.considerFor(ctx, m.oldFound, firstWalkClassify)
	m.oldFound = m.oldFound[n:]
	if len(m.oldFound) == 0 {
		m.oldFound = nil
	}
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
				if info, err := e.statFile(p); err == nil {
					found = append(found, transcript.Found{Path: p, Info: info})
				}
			}
			continue
		}
		f, err := transcript.Walk(dir)
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
	e.considerFor(ctx, found, 0)
}

// considerFor is consider whose classification stops once budget (0:
// none) has passed, after at least one rollout; the store write of what it
// classified follows. It returns how many it handled.
func (e *Engine) considerFor(ctx context.Context, found []transcript.Found, budget time.Duration) int {
	start := time.Now()
	var baseline []store.TranscriptSource
	done := 0
	for _, f := range found {
		if ctx.Err() != nil {
			return done
		}
		if budget > 0 && done > 0 && time.Since(start) >= budget {
			break
		}
		done++
		at, atStart := e.mirror.firstAt[f.Path]
		if atStart {
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
			e.promoteBaseline(ctx, f, atStart)
			continue
		}
		if b, ok := e.newRollout(ctx, f, atStart); ok {
			baseline = append(baseline, b)
		}
	}
	e.recordBaseline(baseline)
	return done
}

// recordBaseline records the baselined and ignored rollouts consider
// classified, in one transaction.
func (e *Engine) recordBaseline(baseline []store.TranscriptSource) {
	if len(baseline) == 0 {
		return
	}
	err := e.update(func(tx *store.Tx) error {
		_, err := tx.UpsertSources(baseline) // one prepared statement (W1b)
		return err
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

// newRollout decides what to do with a rollout seen for the first time. It
// returns a source row to record in bulk (baseline/ignored), or tracks the
// file itself.
func (e *Engine) newRollout(ctx context.Context, f transcript.Found, atStart bool) (store.TranscriptSource, bool) {
	meta, err := e.readMeta(f.Path)
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
	e.mu.Lock()
	conv, bound := e.byThread[meta.ID]
	var closed, forked bool
	if cs := e.convs[conv]; bound && cs != nil {
		closed = (cs.state == store.ConvClosed && !cs.transcriptOpen) || cs.kind == store.KindControl // batch B: control Q&A
		forked = cs.forkParent != 0
	}
	delegated := e.delegThreads[meta.ID]
	e.mu.Unlock()
	if delegated {
		// A delegated task's thread: its result goes to the requester,
		// it is not a local session (no chat of its own).
		src.State = store.SrcIgnored
		return src, true
	}
	if !bound && meta.Subagent() {
		src.State = store.SrcIgnored
		return src, true
	}
	if !bound && e.imp.forking.Load() > 0 {
		// Perhaps the child of a fork not bound yet: look again on the
		// next pass (fork.go).
		return store.TranscriptSource{}, false
	}
	switch {
	case bound && closed:
		src.State = store.SrcIgnored
		return src, true
	case bound:
		src.State, src.ConvID = store.SrcTracking, conv
		if forked && !isForkChild(e.st.Meta, conv) {
			// A fork made before this core (converted): its rollout may
			// start with a copy of the parent's history, which is already
			// in the parent chat. A fork of this core recorded where its
			// rollout starts (fork.go), or it starts with its own records.
			src.Offset = f.Info.Size
		}
		e.track(src, catchUpFrom(f.Info, atStart || meta.Time.Before(e.mirror.startedAt)))
		return store.TranscriptSource{}, false
	case e.mirror.first && f.Info.MTime.Before(e.mirror.startedAt):
		src.State, src.Offset = store.SrcBaseline, f.Info.Size
		return src, true
	case !meta.Interactive():
		src.State = store.SrcIgnored
		return src, true
	}
	e.mirrorSession(ctx, src, meta, 0, catchUpFrom(f.Info, atStart || meta.Time.Before(e.mirror.startedAt)))
	return store.TranscriptSource{}, false
}

// promoteBaseline starts mirroring a baselined rollout that grew: a person
// continued an old session.
func (e *Engine) promoteBaseline(ctx context.Context, f transcript.Found, atStart bool) {
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
	meta, err := e.readMeta(f.Path)
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
		e.track(src, catchUpFrom(f.Info, atStart))
		return
	}
	e.mirrorSession(ctx, src, meta, src.Offset, catchUpFrom(f.Info, atStart))
}

// track records a tracking source and starts following it; catchUp is its
// catch-up end (catchingUp).
func (e *Engine) track(src store.TranscriptSource, catchUp int64) {
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
	f := &mirrorFile{src: src, tracker: e.newMirrorTracker(src.ThreadID, src.Offset, src.Line, e.mirrorSkip),
		catchUpEnd: catchUp, catchUpSet: true}
	e.restoreInfo(f, f.tracker)
	e.importRestore(f) // import.go
	e.mirror.mu.Lock()
	delete(e.mirror.known, src.Path)
	delete(e.mirror.checked, src.Path)
	e.mirror.files[src.Path] = f
	e.mirror.mu.Unlock()
}

// mirrorSession creates a work conversation for a local session and tracks
// its rollout from offset.
func (e *Engine) mirrorSession(ctx context.Context, src store.TranscriptSource, meta transcript.SessionMeta, offset, catchUp int64) {
	first := transcript.FirstPrompt(src.Path, firstPromptMax)
	machine := e.machineLabel() // reads the store: not inside the transaction
	var id, bound int64
	var label string
	err := e.update(func(tx *store.Tx) error {
		// A history import (`continue`, import.go) may have bound the
		// thread meanwhile: track its rollout for that chat instead.
		var err error
		if bound, err = e.importBoundConv(tx, meta.ID); err != nil || bound != 0 {
			return err
		}
		label, err = e.nextLabel(tx)
		if err != nil {
			return err
		}
		// v0.1.20's title of a published local session: "💬 <machine> -
		// <first prompt>" (the directory's name without one); the
		// automatic title follows the thread name after an answer.
		id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName,
			Title: workTitle(machine, "", promptTitle(first), meta.Cwd), TitleSource: "auto"})
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
		if err := tx.SetMeta(discoveredKey(id), discoveredPending); err != nil {
			return err
		}
		_, err = e.refresh(tx, id)
		return err
	})
	if err != nil {
		e.logf("mirror session %s: %v", meta.ID, err)
		return
	}
	if bound != 0 {
		src.ConvID, src.State = bound, store.SrcTracking
		e.track(src, catchUp)
		return
	}
	e.mu.Lock()
	if cs := e.convs[id]; cs != nil {
		cs.cwd, cs.threadID = meta.Cwd, meta.ID
	}
	e.byThread[meta.ID] = id
	e.mu.Unlock()
	e.mirror.mu.Lock()
	delete(e.mirror.known, src.Path)
	delete(e.mirror.checked, src.Path)
	e.mirror.files[src.Path] = &mirrorFile{src: src, tracker: e.newMirrorTracker(meta.ID, offset, src.Line, e.mirrorSkip),
		catchUpEnd: catchUp, catchUpSet: true}
	e.mirror.mu.Unlock()
	// The chat is created with the first record to mirror (mirrorRead).
	e.logf("mirroring local session %s as %s", meta.ID, label)
}

func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	return clipText(s, n)
}

// mirrorPoll stats the tracked rollouts that are due and reads the changed
// ones. The stat is tiered (B1): see mirrorStatEvery.
func (e *Engine) mirrorPoll(ctx context.Context) {
	e.importApply(ctx) // import.go: queued history imports move their cursors first
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
		if !f.catchUpSet {
			// A source restored at start: what it holds now and has not
			// been read yet is catch-up (catchingUp).
			f.catchUpSet = true
			if f.tracker.Offset() < info.Size {
				f.catchUpEnd = info.Size
			}
		}
		if f.held {
			continue
		}
		if sameFile(info, f.info) && f.tracker.Offset() >= info.Size {
			e.settleProgressAtEOF(f, info, now)
			continue
		}
		if !e.convOpen(f.src.ConvID) {
			e.mirrorRead(ctx, f, info)
			continue
		}
		if now.Before(f.retryAt) {
			continue
		}
		blocked, err := e.st.PendingAtLeast(f.src.ConvID, mirrorRecordsMax)
		if err != nil || blocked {
			continue
		}
		e.mirrorRead(ctx, f, info)
		if f.tracker.Offset() < info.Size {
			f.nextStat = now.Add(e.t.MirrorStat) // more to read: next tick
		}
	}
}

// sameFile reports whether two stats of a rollout show the same file. The
// store keeps the mtime in milliseconds (W1c): a file restored at start-up
// compares equal to its nanosecond stat, so an unchanged rollout is not
// read again, with its own transaction, on every helper start.
func sameFile(a, b transcript.FileInfo) bool {
	return a.Size == b.Size && a.Inode == b.Inode && ms(a.MTime) == ms(b.MTime)
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
		f.proven = 0 // offsets of the old file
	}
	realign := rewritten || f.realign
	fh, err := os.Open(f.src.Path)
	if err != nil {
		return
	}
	var outs []transcript.Output
	read := int64(0)
	records := 0
	for read < mirrorPassMax && records < mirrorRecordsMax {
		lines, next, err := transcript.Read(fh, f.tracker.Offset(), mirrorChunk)
		if err != nil {
			e.logf("mirror read %s: %v", filepath.Base(f.src.Path), err)
			break
		}
		if len(lines) == 0 {
			f.tracker.Advance(next)
			break
		}
		if len(lines) > mirrorRecordsMax-records {
			lines = lines[:mirrorRecordsMax-records]
			next = lines[len(lines)-1].End
		}
		records += len(lines)
		outs = append(outs, f.tracker.Feed(lines)...)
		f.tracker.Advance(next)
		read += next - lines[0].Off
	}
	fh.Close()
	if f.tracker.Offset() >= info.Size && info.MTime.Before(e.mirror.startedAt) {
		// At the end of a rollout unchanged since before the start: no
		// record will decide a turn still held (abandoned, s671).
		outs = append(outs, f.tracker.Settle()...)
	}
	more := f.tracker.Offset() < info.Size && (read >= mirrorPassMax || records >= mirrorRecordsMax)

	// Render outside the transaction.
	thread := f.tracker.ThreadID()
	if thread == "" {
		thread = f.src.ThreadID
	}
	if f.ns != "" {
		thread = f.ns // a chat with delivery keys of its own (import.go)
	}
	type rendered struct {
		o    transcript.Output
		msgs []store.OutMsg
	}
	var rs []rendered
	for _, o := range outs {
		if o.Kind == transcript.OutputEnd {
			rs = append(rs, rendered{o: o})
			continue
		}
		if o.End <= f.proven && (o.Kind == transcript.OutputPrompt || o.Kind == transcript.OutputFinal) {
			continue // found in the chat's history (history_check.go)
		}
		if o.Kind != transcript.OutputFinal || !strings.Contains(o.Text, "```"+artifactFence) {
			complete, err := e.st.RepresentedRecord(transcript.MirrorKeyPrefix(thread, o.RecordID))
			if err != nil {
				f.tracker = e.mirrorTracker(f)
				return
			}
			if complete {
				continue
			}
		}
		msgs, err := e.renderMirror(ctx, thread, o, !f.catchingUp(o))
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
			f.tracker = e.mirrorTracker(f)
			return
		}
		rs = append(rs, rendered{o: o, msgs: msgs})
	}
	f.fails, f.retryAt = 0, time.Time{}
	off, line := f.tracker.Resume()
	if off == 0 && f.src.Offset > 0 {
		off = 1 // the store never moves a source back to 0; 1 re-aligns to line 2
	}
	woke, created := false, false
	e.mu.Lock()
	var label string
	creating := false
	if cs := e.convs[f.src.ConvID]; cs != nil {
		label, creating = cs.label, cs.state == store.ConvCreating
	}
	e.mu.Unlock()
	var posted int
	released := false
	var liveThrough int64 // the seq of the last row queued for a live record
	saved := f.infoSaved
	err = e.update(func(tx *store.Tx) error {
		posted, woke, created, liveThrough = 0, false, false, 0
		released = false
		saved = f.infoSaved
		anyLive := false
		var msgs []store.OutMsg
		firstLive := false
		for _, r := range rs {
			if r.o.Kind == transcript.OutputEnd {
				if _, err := tx.Enqueue(f.src.ConvID, msgs); err != nil {
					return err
				}
				msgs = nil
				e.mirror.mu.Lock()
				local := e.mirror.progressTurns[r.o.TurnID]
				e.mirror.mu.Unlock()
				if err := tx.CompleteTranscriptProgress(local, r.o.TurnID); err != nil {
					return err
				}
				released = true
				continue
			}
			ok, err := tx.RecordedParts(r.msgs)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			live := !f.catchingUp(r.o)
			if posted == 0 {
				firstLive = live
			}
			anyLive = anyLive || (live && len(r.msgs) > 0)
			msgs = append(msgs, r.msgs...)
			posted++
			if live && r.o.Kind == transcript.OutputFinal {
				// A live final: the owner is notified once it is posted
				// (Workflow card, else a control-chat @mention), as for
				// a Teams-run final; catch-up finals notify nobody.
				if err := e.addNotify(tx, pendingNotify{Conv: f.src.ConvID, Key: transcript.MirrorKeyPrefix(thread, r.o.RecordID),
					Text: clipText(r.o.Text, 400)}); err != nil {
					return err
				}
			}
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
		if posted > 0 && creating {
			// A discovered session's first records: its notice goes first,
			// then its chat is created (v0.1.20: no chat for an empty
			// session).
			v, _, err := tx.Meta(discoveredKey(f.src.ConvID))
			if err != nil {
				return err
			}
			if v == discoveredPending {
				if err := e.enqueueNotice(tx, f.src.ConvID, "ready:"+label, textLocalDetected(label)); err != nil {
					return err
				}
				if err := tx.SetMeta(discoveredKey(f.src.ConvID), discoveredReady); err != nil {
					return err
				}
				created = true
				if firstLive {
					// v0.1.20 local_session_started, for a live session
					// only; a session caught up at start is announced in
					// the control chat's summary instead (announce.go).
					if err := e.addNotify(tx, pendingNotify{Conv: f.src.ConvID, Key: "ready:" + label + ":", Kind: notifyLocal}); err != nil {
						return err
					}
					if err := markAnnounced(tx, f.src.ConvID); err != nil {
						return err
					}
				}
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
		if err := tx.AdvanceSource(f.src.ID, off, line, info.Size, ms(info.MTime), info.Inode, msgs); err != nil {
			return err
		}
		if v := sourceInfo(f.tracker.Info()); v != f.infoSaved && f.src.ID != 0 {
			if err := tx.SetSourceInfo(f.src.ID, v); err != nil {
				return err
			}
			saved = v
		}
		if anyLive {
			var err error
			if liveThrough, err = tx.LastSeq(f.src.ConvID); err != nil {
				return err
			}
		}
		return e.importProgress(tx, f, posted) // import.go
	})
	if err != nil {
		e.logf("mirror advance %s: %v", filepath.Base(f.src.Path), err)
		// Re-read from the persisted position next time.
		f.tracker = e.mirrorTracker(f)
		return
	}
	for _, r := range rs {
		if r.o.Kind != transcript.OutputEnd {
			continue
		}
		e.mirror.mu.Lock()
		local := e.mirror.progressTurns[r.o.TurnID]
		e.mirror.mu.Unlock()
		if done, err := e.st.TranscriptProgressDone(local); err == nil && done {
			e.mu.Lock()
			if cs := e.convs[f.src.ConvID]; cs != nil && (cs.running == nil || cs.running.turnID == local) {
				cs.transcriptOpen = false
			}
			e.mu.Unlock()
		}
	}
	f.src.Offset, f.src.Line, f.src.Size, f.src.MTime, f.src.Inode = off, line, info.Size, ms(info.MTime), info.Inode
	f.infoSaved = saved
	f.realign, f.realignAt = realign && more, realignAt
	if !more {
		f.info = info
	}
	if liveThrough > 0 {
		e.markLive(f.src.ConvID, liveThrough)
	}
	if created {
		// A caught-up session's chat goes with the backlog; a session in
		// use now gets its chat with the live messages.
		class := sendMirror
		if liveThrough > 0 {
			class = sendLive
		}
		e.requestChat(f.src.ConvID, class)
	}
	if woke {
		_ = e.update(func(tx *store.Tx) error { _, err := e.refresh(tx, f.src.ConvID); return err })
		e.logf("mirror: woke %d (local activity)", f.src.ConvID)
	}
	if posted > 0 || woke || released {
		e.heat(f.src.ConvID)
		e.wakeSender()
	}
}

// renderMirror renders one mirrored record as v0.1.20 did: a prompt under
// "🧑‍💻 User:", an answer under "🤖 ✅ Codex answer:" with the completion
// footer, a commentary or compaction notice under "🤖 ⏳ Codex status:"; a
// long one carries "[part i/n]" in every part's header. None mentions the
// owner in the work chat.
//
// A live record (not catch-up) of Codex closes with the goal and model
// lines of its thread as the rollout said them then (infoNotes).
func (e *Engine) renderMirror(ctx context.Context, thread string, o transcript.Output, live bool) ([]store.OutMsg, error) {
	var files []store.OutMsg
	if o.Kind == transcript.OutputFinal && o.TurnID != "" && e.cfg.FilesDir != "" && strings.Contains(o.Text, "```"+artifactFence) {
		key := sharedOutputKey(o.TurnID)
		if _, err := os.Stat(e.outboundDir(key)); err == nil {
			var problems []string
			prefix := o.Key(thread, 0)
			o.Text, files, problems = e.stageArtifactsAt(key, fmt.Sprintf("mirror-%x", sha256.Sum256([]byte(prefix))), prefix, o.Text)
			if len(problems) > 0 {
				o.Text = strings.TrimSpace(o.Text + "\n\n_Some files could not be attached:_\n- " + strings.Join(problems, "\n- "))
			}
			for i := range files {
				files[i].Kind = store.OutMirror
			}
		}
	}
	var doc channel.RichDoc
	switch {
	case o.User || o.Kind == transcript.OutputPrompt:
		doc = userDoc(strings.TrimSpace(o.Text))
	case o.Kind == transcript.OutputStatus || o.Kind == transcript.OutputCompact:
		doc = statusDoc(o.Text) // commentary, compaction: Codex status
	default:
		doc = answerDoc(o.Text)
	}
	if live && !o.User && o.Kind != transcript.OutputPrompt {
		doc.Notes = infoNotes(mirrorInfo(o.Info), o.Kind == transcript.OutputFinal)
		// Helper-owned progress keeps the runner's live goal annotation.
		// Goal changes can arrive through the native API before rollout metadata.
		owned := false
		if e.mirror != nil {
			e.mirror.mu.Lock()
			owned = e.mirror.progressTurns[o.TurnID] != 0
			e.mirror.mu.Unlock()
		}
		if owned && (o.Kind == transcript.OutputStatus || o.Kind == transcript.OutputCompact) {
			e.mu.Lock()
			conv := e.byThread[thread]
			e.mu.Unlock()
			doc.Notes = infoNotes(e.turnInfo(conv), false)
		}
	}
	parts, err := e.ch.Render(ctx, doc)
	if err != nil {
		return nil, err
	}
	group := ""
	if o.Kind == transcript.OutputStatus && o.TurnID != "" {
		group = "mirror:" + thread + ":" + o.TurnID
	}
	out := make([]store.OutMsg, 0, len(parts))
	for _, p := range parts {
		alternate := ""
		if o.LegacyRecordID != "" {
			alternate = transcript.MirrorKey(thread, o.LegacyRecordID, p.Index)
		}
		out = append(out, store.OutMsg{AlternateKey: alternate, DeliveryKey: o.Key(thread, p.Index), Kind: store.OutMirror, Part: p.Index, Parts: p.Count,
			Body: p.Body, BodyHash: hashBytes(p.Hash), ProgressGroup: group})
	}
	return append(out, files...), nil
}

// mirrorInfo is the TurnInfo of a mirrored record.
func mirrorInfo(i transcript.Info) TurnInfo {
	out := TurnInfo{Model: i.Model, Effort: i.Effort, Tier: i.Tier}
	if i.Goal != nil && i.Goal.Status == GoalActive {
		out.Goal = i.Goal.Objective
	}
	return out
}
