package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// History import (batch A: F-CTL-5, F-MIR-5, F-MIR-6, F-WRK-3), v0.1.20's
// publishCodexSessionLocalWithOptions (B:15741) and publish-history
// (B:16039, B:16083).
//
// An import IS mirroring: it moves the rollout's mirror cursor (its
// transcript_sources row) and lets mirrorRead post the records, with the
// same delivery keys and the same dedupe as catch-up. So:
//
//   - nothing is posted twice: a record is posted once per delivery key,
//     whoever reads it (import, catch-up, live mirror, a second import);
//   - nothing is lost: the job and the cursor are in the store; a crash, a
//     restart or a takeover re-reads from the persisted cursor and the job
//     finishes when the cursor reaches the file end it recorded;
//   - it is catch-up: prompts and finals only, nobody notified (catchingUp
//     covers the whole imported range);
//   - it is paced like any mirrored message (send class sendMirror, behind
//     the control chat and the replies), a pass of mirrorPassMax per stat
//     tick, so a long import never starves the control chat.
//
// A work chat whose thread already has deliveries under its plain mirror
// keys (a closed chat of the same session) gets a key namespace of its
// own (metaMirrorNS): the new chat receives the history again, once.
//
// The jobs are a JSON list in meta (few, short-lived). Nothing runs while
// none is queued: the mirror loop checks one atomic flag per pass.

const (
	metaImports        = "engine_imports"
	metaImportSeq      = "engine_import_seq"
	metaMirrorNSPrefix = "engine_mirror_ns:"    // + source id: the delivery key namespace ("" = the thread id)
	metaFullHistPrefix = "engine_full_history:" // + conv id: "1" once a full import completed
	metaImportedPrefix = "engine_imported:"     // + conv id: "1" for a chat made by `continue` (titled from its local session)
)

// Import kinds.
const (
	importNew    = "new"    // `continue`: a new work chat with the session's history
	importReopen = "reopen" // `continue` on a linked chat with unread history
	importSync   = "sync"   // `helper publish-history`
	importFull   = "full"   // `helper publish-history full`
	importFork   = "fork"   // a fork's history: the marker, then the link (fork.go)
)

// importJob is one queued or running import.
type importJob struct {
	ID     int64  `json:"id"`
	Conv   int64  `json:"conv"`
	Kind   string `json:"kind"`
	Thread string `json:"thread,omitempty"`
	Path   string `json:"path,omitempty"`
	// Rewind reads the rollout again from its start; else from the cursor.
	Rewind bool `json:"rewind,omitempty"`
	// KeepTeams also posts the helper-run turns (up to Until): a new chat
	// has none of them.
	KeepTeams bool `json:"keep_teams,omitempty"`
	// NS is the delivery key namespace of the source ("" = the thread id).
	NS string `json:"ns,omitempty"`
	// Applied once the cursor was moved; Src is the source, Until the
	// file end then: the job is done when the cursor reaches it.
	Applied bool  `json:"applied,omitempty"`
	Src     int64 `json:"src,omitempty"`
	Until   int64 `json:"until,omitempty"`
	Posted  int   `json:"posted,omitempty"`
	// Resumed: the parked chat was woken by the command (reopen text).
	Resumed bool `json:"resumed,omitempty"`
	// Reached: the cursor reached Until before the chat existed; the job
	// finishes when the chat is created.
	Reached bool `json:"reached,omitempty"`
}

type importState struct {
	mu      sync.Mutex
	loaded  bool
	convs   map[int64]bool // conversations with a job (a cache: the store decides)
	pending atomic.Bool    // a job waits to be applied
	retryAt time.Time      // mirror goroutine only
	// forking counts forks between thread/fork and their transaction;
	// sweep asks the mirror goroutine to remove retired fork snapshots
	// (fork.go).
	forking atomic.Int32
	sweep   atomic.Bool
}

var errImportRunning = errors.New("history import is still running")

func mirrorNSKey(src int64) string  { return fmt.Sprintf("%s%d", metaMirrorNSPrefix, src) }
func importedKey(conv int64) string { return fmt.Sprintf("%s%d", metaImportedPrefix, conv) }
func fullHistKey(conv int64) string { return fmt.Sprintf("%s%d", metaFullHistPrefix, conv) }

func loadImports(get func(string) (string, bool, error)) ([]importJob, error) {
	v, _, err := get(metaImports)
	if err != nil || v == "" {
		return nil, err
	}
	var jobs []importJob
	if err := json.Unmarshal([]byte(v), &jobs); err != nil {
		return nil, fmt.Errorf("import jobs: %w", err)
	}
	return jobs, nil
}

func saveImports(tx *store.Tx, jobs []importJob) error {
	if len(jobs) == 0 {
		return tx.SetMeta(metaImports, "")
	}
	b, err := json.Marshal(jobs)
	if err != nil {
		return err
	}
	return tx.SetMeta(metaImports, string(b))
}

// importLoad fills the cache from the store once (mirrorInit).
func (e *Engine) importLoad() {
	e.imp.mu.Lock()
	loaded := e.imp.loaded
	e.imp.mu.Unlock()
	if loaded {
		return
	}
	// Read outside imp.mu: transactions take it (importChatCreated).
	jobs, err := loadImports(e.st.Meta)
	if err != nil {
		e.logf("import: %v", err)
		return
	}
	e.imp.mu.Lock()
	defer e.imp.mu.Unlock()
	if e.imp.loaded {
		return
	}
	e.imp.loaded = true
	e.imp.sweep.Store(true) // fork snapshots left by a crash (fork.go)
	if e.imp.convs == nil {
		// Jobs queued since the read are already in it (importQueued).
		e.imp.convs = map[int64]bool{}
	}
	for _, j := range jobs {
		e.imp.convs[j.Conv] = true
		if !j.Applied {
			e.imp.pending.Store(true)
		}
	}
}

func (e *Engine) importIndexed(conv int64) bool {
	e.imp.mu.Lock()
	defer e.imp.mu.Unlock()
	return e.imp.convs[conv]
}

// queueImport queues a history import into job.Conv in tx (one job per
// conversation: errImportRunning when it has one) and returns its id.
// After tx commits, call importQueued(job.Conv).
//
// This is the entry point for other features (the fork's history, #20,
// fork.go):
//
//	id, err := e.queueImport(tx, importJob{Conv: conv, Kind: importFork,
//	        Thread: thread, Path: rollout, Rewind: true, KeepTeams: true, NS: ns})
//	... after commit: e.importQueued(conv)
//
// The records of the rollout are then mirrored into conv as catch-up,
// once per delivery key (NS: a key namespace for a chat whose thread was
// already mirrored elsewhere). KeepTeams posts helper-run turns too, up to
// the file end when the job is applied. Path may be empty when the thread
// already has a transcript source.
func (e *Engine) queueImport(tx *store.Tx, job importJob) (int64, error) {
	jobs, err := loadImports(tx.Meta)
	if err != nil {
		return 0, err
	}
	kept := jobs[:0]
	for _, j := range jobs {
		c, ok, err := tx.Conversation(j.Conv)
		if err != nil {
			return 0, err
		}
		if !ok || c.State == store.ConvClosed {
			continue // its chat was closed: the job ends with it
		}
		if j.Conv == job.Conv {
			return 0, errImportRunning
		}
		kept = append(kept, j)
	}
	v, _, err := tx.Meta(metaImportSeq)
	if err != nil {
		return 0, err
	}
	var seq int64
	fmt.Sscan(v, &seq)
	seq++
	if err := tx.SetMeta(metaImportSeq, fmt.Sprint(seq)); err != nil {
		return 0, err
	}
	job.ID = seq
	job.Applied, job.Src, job.Until, job.Posted, job.Reached = false, 0, 0, 0, false
	return seq, saveImports(tx, append(kept, job))
}

// importQueued wakes the mirror loop for a job committed for conv.
func (e *Engine) importQueued(conv int64) {
	e.imp.mu.Lock()
	if e.imp.convs == nil {
		e.imp.convs = map[int64]bool{}
	}
	e.imp.convs[conv] = true
	e.imp.retryAt = time.Time{}
	e.imp.mu.Unlock()
	e.imp.pending.Store(true)
	wake(e.mirrorWake)
}

// importJobOf returns the job of conv, if any (store read).
func (e *Engine) importJobOf(conv int64) (importJob, bool) {
	if !e.importIndexed(conv) {
		return importJob{}, false
	}
	jobs, err := loadImports(e.st.Meta)
	if err != nil {
		return importJob{}, false
	}
	for _, j := range jobs {
		if j.Conv == conv {
			return j, true
		}
	}
	return importJob{}, false
}

// importApply moves the cursors of queued jobs (mirror goroutine, at the
// start of mirrorPoll). A job whose rollout is not found yet is retried
// with discovery.
func (e *Engine) importApply(ctx context.Context) {
	if e.imp.sweep.Swap(false) {
		e.forkSweep()
	}
	if !e.imp.pending.Load() {
		return
	}
	now := e.now()
	e.imp.mu.Lock()
	wait := now.Before(e.imp.retryAt)
	e.imp.mu.Unlock()
	if wait {
		return
	}
	e.imp.pending.Store(false)
	jobs, err := loadImports(e.st.Meta)
	if err != nil {
		e.logf("import: %v", err)
		return
	}
	retry := false
	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		if !j.Applied && !e.importApplyOne(j) {
			retry = true
		}
	}
	if retry {
		e.imp.mu.Lock()
		e.imp.retryAt = now.Add(e.t.Discovery)
		e.imp.mu.Unlock()
		e.imp.pending.Store(true)
	}
}

// importPath finds the rollout of a job.
func (e *Engine) importPath(j importJob) string {
	if j.Path != "" {
		return j.Path
	}
	srcs, err := e.st.Sources()
	if err != nil {
		return ""
	}
	path := ""
	for _, s := range srcs {
		if s.ThreadID != j.Thread {
			continue
		}
		if s.ConvID == j.Conv {
			return s.Path
		}
		path = s.Path
	}
	return path
}

// importApplyOne applies one job: false to retry it later.
func (e *Engine) importApplyOne(j importJob) bool {
	path := e.importPath(j)
	if path == "" {
		return false
	}
	info, err := e.statFile(path)
	if err != nil {
		return false
	}
	var src store.TranscriptSource
	var job importJob
	install := false
	err = e.update(func(tx *store.Tx) error {
		install = false
		jobs, err := loadImports(tx.Meta)
		if err != nil {
			return err
		}
		i := -1
		for k := range jobs {
			if jobs[k].ID == j.ID {
				i = k
			}
		}
		if i < 0 || jobs[i].Applied {
			return nil
		}
		job = jobs[i]
		c, ok, err := tx.Conversation(job.Conv)
		if err != nil {
			return err
		}
		if !ok || c.State == store.ConvClosed {
			return saveImports(tx, append(jobs[:i:i], jobs[i+1:]...))
		}
		cur, ok, err := tx.Source(path)
		if err != nil {
			return err
		}
		thread := job.Thread
		if thread == "" {
			thread = cur.ThreadID
		}
		row := store.TranscriptSource{Path: path, ThreadID: thread, ConvID: job.Conv, State: store.SrcTracking,
			Size: info.Size, MTime: ms(info.MTime), Inode: info.Inode}
		if ok && !job.Rewind {
			// Read on from the cursor, as the file stood when it was
			// last read (mirrorRead compares it with the file now).
			row.Size, row.MTime, row.Inode = cur.Size, cur.MTime, cur.Inode
		}
		id, err := tx.UpsertSource(row) // never moves the cursor
		if err != nil {
			return err
		}
		if job.Rewind {
			if err := tx.RewindSource(id, job.Conv, store.SrcTracking); err != nil {
				return err
			}
			if err := tx.SetMeta(realignKey(id), ""); err != nil {
				return err
			}
		}
		if src, _, err = tx.SourceByID(id); err != nil {
			return err
		}
		if err := tx.SetMeta(mirrorNSKey(id), job.NS); err != nil {
			return err
		}
		job.Applied, job.Src, job.Until = true, id, info.Size
		jobs[i] = job
		install = true
		if src.Offset >= job.Until {
			done, err := e.importFinish(tx, &job)
			if err != nil {
				return err
			}
			if done {
				return saveImports(tx, append(jobs[:i:i], jobs[i+1:]...))
			}
			jobs[i] = job
		}
		return saveImports(tx, jobs)
	})
	if err != nil {
		e.logf("import %d: %v", j.ID, err)
		return false
	}
	if !install {
		return true
	}
	e.mirror.mu.Lock()
	old := e.mirror.files[path]
	if old != nil && !job.Rewind && old.src.ConvID == job.Conv && old.src.ID == src.ID {
		old.ns = job.NS // a sync: the cursor did not move
	} else {
		f := &mirrorFile{src: src, info: transcript.FileInfo{Size: src.Size, MTime: fromMS(src.MTime), Inode: src.Inode},
			ns: job.NS, catchUpEnd: job.Until, catchUpSet: true}
		if job.KeepTeams {
			f.keepTeams = job.Until
		}
		if old != nil && old.src.ConvID == job.Conv {
			// The same chat: what its history check proved stays proven.
			f.held, f.proven = old.held, old.proven
			f.catchUpEnd = max(f.catchUpEnd, old.catchUpEnd)
		}
		f.tracker = e.mirrorTracker(f)
		e.mirror.files[path] = f
	}
	delete(e.mirror.known, path)
	delete(e.mirror.checked, path)
	e.mirror.mu.Unlock()
	e.wakeSender()
	return true
}

// mirrorTracker is a tracker for f at its persisted cursor.
func (e *Engine) mirrorTracker(f *mirrorFile) *transcript.Tracker {
	t := transcript.NewTracker(f.src.ThreadID, f.src.Offset, f.src.Line, e.mirrorSkip)
	e.restoreInfo(f, t)
	if f.keepTeams > 0 {
		t.KeepTeams(f.keepTeams)
	}
	return t
}

// importRestore restores what a job set on a source's mirror file (its
// key namespace, the kept helper turns) when the file is tracked again
// (mirrorInit, track).
func (e *Engine) importRestore(f *mirrorFile) {
	if f.src.ID == 0 {
		return
	}
	if ns, _, err := e.st.Meta(mirrorNSKey(f.src.ID)); err == nil {
		f.ns = ns
	}
	if !e.importIndexed(f.src.ConvID) {
		return
	}
	jobs, err := loadImports(e.st.Meta)
	if err != nil {
		return
	}
	for _, j := range jobs {
		if j.Applied && j.Src == f.src.ID && j.Conv == f.src.ConvID {
			if j.KeepTeams {
				f.keepTeams = j.Until
				f.tracker.KeepTeams(j.Until)
			}
			// The imported range is catch-up (a source restored at start
			// adds what the file holds then, mirrorPoll).
			f.catchUpEnd = max(f.catchUpEnd, j.Until)
		}
	}
}

// importProgress records a mirror pass of f in its job (mirrorRead's
// transaction, after the cursor moved): the job finishes once the cursor
// reached the file end it recorded.
func (e *Engine) importProgress(tx *store.Tx, f *mirrorFile, posted int) error {
	if !e.importIndexed(f.src.ConvID) {
		return nil
	}
	jobs, err := loadImports(tx.Meta)
	if err != nil {
		return err
	}
	for i, j := range jobs {
		if !j.Applied || j.Src != f.src.ID || j.Conv != f.src.ConvID {
			continue
		}
		j.Posted += posted
		if f.tracker.Offset() >= j.Until {
			done, err := e.importFinish(tx, &j)
			if err != nil {
				return err
			}
			if done {
				return saveImports(tx, append(jobs[:i:i], jobs[i+1:]...))
			}
		}
		jobs[i] = j
		return saveImports(tx, jobs)
	}
	return nil
}

// Import texts (v0.1.20).
const (
	textImportComplete   = "Import complete. This chat is ready; reply here to continue the Codex session."
	textImportNothingNew = "No new visible local Codex history needed to be imported. This chat is ready."
	textFullHistoryDone  = "Full local Codex history is now up to date in this chat."
)

func textImportTitle(title string) string {
	return "Imported Codex session history\n\nThe messages below came from your local Codex session. Reply in this chat to continue from here.\n\nSession: " + title
}

func textImportPreparing(label, url string, reopened bool) string {
	status := "Work chat created"
	if reopened {
		status = "Work chat reopened"
	}
	return strings.TrimSpace(strings.Join([]string{
		fmt.Sprintf("✅ %s: %s", status, label), "", "Open Work chat:", strings.TrimSpace(url), "",
		"Preparing local Codex history now. Long sessions can take a few minutes.",
		`Wait for "Import complete" in the Work chat before sending a new task there.`,
	}, "\n"))
}

func resumedText(resumed bool) string {
	if resumed {
		return " The parked Work chat was resumed."
	}
	return ""
}

// importFinish queues a finished job's notices: false (and Reached) while
// its chat does not exist yet.
func (e *Engine) importFinish(tx *store.Tx, j *importJob) (bool, error) {
	c, ok, err := tx.Conversation(j.Conv)
	if err != nil {
		return false, err
	}
	if !ok || c.State == store.ConvClosed {
		return true, nil
	}
	if c.ExtChatID == "" {
		j.Reached = true
		return false, nil
	}
	work := func(text string) error {
		return e.enqueueNotice(tx, j.Conv, fmt.Sprintf("import:%d:done", j.ID), text)
	}
	control := func(text string) error {
		if e.controlID == 0 {
			return nil
		}
		return e.enqueueNotice(tx, e.controlID, fmt.Sprintf("import:%d:result", j.ID), text)
	}
	switch j.Kind {
	case importNew:
		if err := work(textImportComplete); err != nil {
			return false, err
		}
		err = control(fmt.Sprintf("Published local Codex session as %s: %s\n\nOpen this Teams work chat and send a message there to continue.", c.Label, c.ExtURL))
	case importReopen:
		if err := work(textImportComplete); err != nil {
			return false, err
		}
		err = control(fmt.Sprintf("Already published as %s: %s\n\nNew local history was imported.%s Open this Teams work chat and send a message there to continue.", c.Label, c.ExtURL, resumedText(j.Resumed)))
	case importSync:
		text := textImportComplete
		if j.Posted == 0 {
			text = textImportNothingNew
		}
		err = work(text)
	case importFull:
		if err := work(textImportComplete); err != nil {
			return false, err
		}
		if err := tx.SetMeta(fullHistKey(j.Conv), "1"); err != nil {
			return false, err
		}
		err = e.enqueueNotice(tx, j.Conv, fmt.Sprintf("import:%d:full", j.ID), textFullHistoryDone)
	case importFork:
		err = e.forkHistoryDone(tx, j)
	}
	return err == nil, err
}

// importChatCreated is createChat's step for a conversation made by an
// import (true: handled): the control chat hears that the chat exists and
// its history is coming; a job that already reached its end finishes.
func (e *Engine) importChatCreated(tx *store.Tx, id int64, label, url string) (bool, error) {
	if !e.importIndexed(id) {
		return false, nil
	}
	jobs, err := loadImports(tx.Meta)
	if err != nil {
		return false, err
	}
	for i, j := range jobs {
		if j.Conv != id || (j.Kind != importNew && j.Kind != importFork) {
			continue
		}
		if e.controlID != 0 && j.Kind == importNew {
			if err := e.enqueueNotice(tx, e.controlID, fmt.Sprintf("import:%d:progress", j.ID), textImportPreparing(label, url, false)); err != nil {
				return true, err
			}
		}
		if j.Reached {
			done, err := e.importFinish(tx, &j)
			if err != nil {
				return true, err
			}
			if done {
				return true, saveImports(tx, append(jobs[:i:i], jobs[i+1:]...))
			}
		}
		return true, nil
	}
	return false, nil
}

// ---- `continue` ----

// cmdContinue is control `c <n|session id>` (v0.1.20 B:15710).
func (e *Engine) cmdContinue(ctx context.Context, j cmdJob, arg string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		e.reply(j, "usage: `continue <number-or-session-id>`", nil)
		return
	}
	list, err := e.dashDiscover()
	if err != nil {
		e.reply(j, "workspace discovery failed: "+err.Error(), nil)
		return
	}
	var s *dashSession
	if n, isNum := dashNumber(arg); isNum {
		sel, err := e.dashResolve(list, n)
		if err == nil && sel.kind != dashItemSession {
			err = fmt.Errorf("number %d is a directory in the current list; send `project %d` first to list its sessions, then `continue <session-number>`", n, n)
		}
		if err != nil {
			e.reply(j, err.Error(), nil)
			return
		}
		s = sel.session
	} else if s = findDashSession(list, arg); s == nil {
		e.reply(j, "Codex session not found: "+arg, nil)
		return
	}
	e.dashTitles([]*dashSession{s})
	e.publishSession(ctx, j, s, nil)
}

// publishSession continues local session s in Teams: its linked work chat
// (resumed if parked, unread history imported), else a new work chat with
// its history. extra commits with the command.
func (e *Engine) publishSession(ctx context.Context, j cmdJob, s *dashSession, extra func(tx *store.Tx) error) {
	if l := e.linkOf(s.thread); l.found && l.open {
		e.publishLinked(j, s, l, extra)
		return
	}
	id, linked, err := e.publishNew(j, s, extra)
	if errors.Is(err, errLinkedMeanwhile) {
		// Discovery gave the session a chat meanwhile: it is linked.
		if l := e.linkConv(linked); l.found && l.open {
			e.publishLinked(j, s, l, extra)
			return
		}
	}
	if err != nil {
		e.reply(j, "publish failed: "+userError(err.Error()), extra)
		return
	}
	e.createChatNow(ctx, id)
}

var errLinkedMeanwhile = errors.New("linked meanwhile")

// publishLinked is `continue` on a session with an open work chat.
func (e *Engine) publishLinked(j cmdJob, s *dashSession, l dashLink, extra func(tx *store.Tx) error) {
	if _, running := e.importJobOf(l.conv); running {
		e.reply(j, fmt.Sprintf("Already published as %s: %s\n\nHistory import is still running. Wait for \"Import complete\" in the Work chat before sending a new task there.", l.label, l.url), extra)
		return
	}
	backlog := false
	var srcPath string
	if srcs, err := e.st.Sources(); err == nil {
		for _, src := range srcs {
			if src.ConvID == l.conv && src.ThreadID == s.thread && src.State == store.SrcTracking {
				srcPath = src.Path
				if info, err := e.statFile(src.Path); err == nil && src.Offset < info.Size {
					backlog = true
				}
			}
		}
	}
	resumed := l.parked
	wakeBase := fmt.Sprintf("resumed:%d", j.inbound)
	if !backlog {
		e.reply(j, fmt.Sprintf("Already published as %s: %s\n\nNo new local history was imported.%s Open this Teams work chat and send a message there to continue.", l.label, l.url, resumedText(resumed)),
			joinTx(extra, e.wakeStep(l.conv, wakeBase)))
		if resumed {
			e.heatUser(l.conv)
		}
		return
	}
	queued := false
	err := e.update(func(tx *store.Tx) error {
		queued = false
		if extra != nil {
			if err := extra(tx); err != nil {
				return err
			}
		}
		ns := ""
		if src, ok, err := tx.Source(srcPath); err != nil {
			return err
		} else if ok {
			if ns, _, err = tx.Meta(mirrorNSKey(src.ID)); err != nil {
				return err
			}
		}
		id, err := e.queueImport(tx, importJob{Conv: l.conv, Kind: importReopen, Thread: s.thread, Path: srcPath, NS: ns, Resumed: resumed})
		if errors.Is(err, errImportRunning) {
			msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("cmd:%d", j.inbound),
				fmt.Sprintf("Already published as %s: %s\n\nHistory import is still running. Wait for \"Import complete\" in the Work chat before sending a new task there.", l.label, l.url), j.quote)
			if err != nil {
				return err
			}
			if _, err := tx.Enqueue(j.conv, msgs); err != nil {
				return err
			}
			return tx.SetInbound(j.inbound, store.InDone, 0)
		}
		if err != nil {
			return err
		}
		queued = true
		if err := e.enqueueNotice(tx, l.conv, fmt.Sprintf("import:%d:title", id), textImportTitle(s.title)); err != nil {
			return err
		}
		if err := e.wakeStep(l.conv, wakeBase)(tx); err != nil {
			return err
		}
		msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("cmd:%d", j.inbound), textImportPreparing(l.label, l.url, true), j.quote)
		if err != nil {
			return err
		}
		if _, err := tx.Enqueue(j.conv, msgs); err != nil {
			return err
		}
		return tx.SetInbound(j.inbound, store.InDone, 0)
	})
	if err != nil {
		e.logf("continue %s: %v", l.label, err)
	}
	if queued {
		e.importQueued(l.conv)
		if resumed {
			e.heatUser(l.conv)
		}
	}
	e.wakeSender()
}

// publishNew makes a new work chat for session s and queues its import
// (v0.1.20: a new chat with the lifecycle notice, the import title, the
// history, "Import complete"). A session found by discovery with nothing
// mirrored yet keeps its conversation. It returns the conversation.
func (e *Engine) publishNew(j cmdJob, s *dashSession, extra func(tx *store.Tx) error) (int64, int64, error) {
	var id, linked int64
	var label string
	machine := e.machineLabel()
	err := e.update(func(tx *store.Tx) error {
		id, linked, label = 0, 0, ""
		bound, err := tx.ThreadConvs(s.thread)
		if err != nil {
			return err
		}
		reuse := int64(0)
		for _, b := range bound {
			c, ok, err := tx.Conversation(b)
			if err != nil || !ok {
				return err
			}
			switch c.State {
			case store.ConvClosed, store.ConvHold:
				continue
			case store.ConvCreating:
				if v, _, err := tx.Meta(discoveredKey(b)); err != nil {
					return err
				} else if v == discoveredPending {
					reuse = b
					continue
				}
			}
			linked = b
			return errLinkedMeanwhile
		}
		if extra != nil {
			if err := extra(tx); err != nil {
				return err
			}
		}
		if reuse != 0 {
			id = reuse
			c, _, err := tx.Conversation(reuse)
			if err != nil {
				return err
			}
			label = c.Label
			if err := tx.SetMeta(discoveredKey(id), ""); err != nil {
				return err
			}
			// `continue` announces this chat itself (announce.go).
			if err := markAnnounced(tx, id); err != nil {
				return err
			}
		} else {
			if label, err = e.nextLabel(tx); err != nil {
				return err
			}
			// v0.1.20 titles an imported chat "💬 <machine> - <session
			// title>" (WorkChatTitle in publishCodexSessionWithProgress).
			id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName,
				Title: workTitle(machine, "", s.title, s.cwd), TitleSource: "auto"})
			if err != nil {
				return err
			}
			if err := tx.Bind(store.Thread{ConvID: id, ThreadID: s.thread, Cwd: s.cwd}); err != nil {
				return err
			}
		}
		if err := tx.SetMeta(importedKey(id), "1"); err != nil {
			return err
		}
		ns := ""
		if prior, err := tx.Delivered("mirror:" + s.thread + ":"); err != nil {
			return err
		} else if prior {
			// The session was mirrored into another chat: this chat
			// gets its own delivery keys (posted once here too).
			ns = fmt.Sprintf("%s@c%d", s.thread, id)
		}
		jid, err := e.queueImport(tx, importJob{Conv: id, Kind: importNew, Thread: s.thread, Path: s.path, Rewind: true, KeepTeams: true, NS: ns})
		if err != nil {
			return err
		}
		if err := e.enqueueNotice(tx, id, "ready:"+label, textWorkChatCreated(label, s.cwd)); err != nil {
			return err
		}
		if err := e.enqueueNotice(tx, id, fmt.Sprintf("import:%d:title", jid), textImportTitle(s.title)); err != nil {
			return err
		}
		if _, err := e.refresh(tx, id); err != nil {
			return err
		}
		return tx.SetInbound(j.inbound, store.InDone, 0)
	})
	if err != nil {
		return 0, linked, err
	}
	e.mu.Lock()
	if cs := e.convs[id]; cs != nil {
		cs.cwd, cs.threadID = s.cwd, s.thread
	}
	e.byThread[s.thread] = id
	e.mu.Unlock()
	e.importQueued(id)
	e.logf("publishing local session %s as %s", s.thread, label)
	return id, 0, nil
}

// ---- `helper publish-history [full]` ----

// cmdPublishHistory is work `helper publish-history [full]` / `!ph`
// (v0.1.20 B:8051, B:16039, B:16083).
func (e *Engine) cmdPublishHistory(j cmdJob, cs *convState, arg string) {
	arg = strings.ToLower(strings.TrimSpace(arg))
	full := arg == "full"
	if arg != "" && !full {
		e.reply(j, "Usage: `helper publish-history` imports the pending backlog; `helper publish-history full` imports the complete local Codex history into this chat.", nil)
		return
	}
	e.mu.Lock()
	conv, thread := cs.id, cs.threadID
	e.mu.Unlock()
	var src store.TranscriptSource
	found := false
	if srcs, err := e.st.Sources(); err == nil {
		for _, s := range srcs {
			if s.ConvID == conv && s.State == store.SrcTracking && (thread == "" || s.ThreadID == thread) {
				src, found = s, true
			}
		}
	}
	if !found {
		e.reply(j, "No local Codex history file is linked to this Work chat.", nil)
		return
	}
	if job, running := e.importJobOf(conv); running {
		text := "History import is already running. Wait for `Import complete` before sending another task."
		if full {
			text = "History import is already running. Wait for `Import complete` before publishing full history."
			if job.Kind == importFull {
				text = "Full history import is already running for this chat."
			}
		}
		e.reply(j, text, nil)
		return
	}
	info, err := e.statFile(src.Path)
	if err != nil {
		e.reply(j, "No local Codex history file is linked to this Work chat.", nil)
		return
	}
	if !full && src.Offset >= info.Size {
		e.reply(j, textImportNothingNew, nil)
		return
	}
	if full {
		if v, _, err := e.st.Meta(fullHistKey(conv)); err == nil && v == "1" && src.Offset >= info.Size {
			e.reply(j, "Full local Codex history is already published to this chat.", nil)
			return
		}
	}
	queued := false
	err = e.update(func(tx *store.Tx) error {
		queued = false
		ns, _, err := tx.Meta(mirrorNSKey(src.ID))
		if err != nil {
			return err
		}
		job := importJob{Conv: conv, Kind: importSync, Thread: src.ThreadID, Path: src.Path, NS: ns}
		if full {
			// The whole rollout again, under this chat's keys: what is
			// already here is not posted twice, what is missing is.
			job.Kind, job.Rewind = importFull, true
		}
		id, err := e.queueImport(tx, job)
		if errors.Is(err, errImportRunning) {
			msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("cmd:%d", j.inbound), "History import is already running. Wait for `Import complete` before sending another task.", j.quote)
			if err != nil {
				return err
			}
			if _, err := tx.Enqueue(j.conv, msgs); err != nil {
				return err
			}
			return tx.SetInbound(j.inbound, store.InDone, 0)
		}
		if err != nil {
			return err
		}
		queued = true
		if full {
			if err := e.enqueueNotice(tx, conv, fmt.Sprintf("import:%d:title", id), textImportTitle(e.sessionTitleOf(src.Path, src.ThreadID))); err != nil {
				return err
			}
		}
		// No reply now: "Import complete" (or the no-new text) answers
		// once the history is in the chat.
		return tx.SetInbound(j.inbound, store.InDone, 0)
	})
	if err != nil {
		e.logf("publish-history in %d: %v", conv, err)
	}
	if queued {
		e.importQueued(conv)
	}
	e.wakeSender()
}

// sessionTitleOf is a session's display title (v0.1.20 DisplayTitle).
func (e *Engine) sessionTitleOf(path, thread string) string {
	s := &dashSession{thread: thread, path: path}
	e.dashTitles([]*dashSession{s})
	if s.title == "" {
		return filepath.Base(path)
	}
	return s.title
}

// importBoundConv returns the open work conversation bound to thread in tx
// (0: none): mirrorSession's check against a `continue` that bound the
// thread after discovery looked.
func (e *Engine) importBoundConv(tx *store.Tx, thread string) (int64, error) {
	ids, err := tx.ThreadConvs(thread)
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		c, ok, err := tx.Conversation(id)
		if err != nil {
			return 0, err
		}
		if ok && c.Kind == store.KindWork && c.State != store.ConvClosed && c.State != store.ConvHold {
			return id, nil
		}
	}
	return 0, nil
}
