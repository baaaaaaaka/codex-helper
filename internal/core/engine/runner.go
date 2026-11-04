package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// runnerLoop starts runnable turns. It is woken by the transactions that
// make a conversation runnable (new turn, last pending send delivered, turn
// finished, unpause) and by capacity-retry timers; a slow sweep is only a
// safety net.
func (e *Engine) runnerLoop(ctx context.Context) {
	sem := make(chan struct{}, e.cfg.Turns)
	sweep := time.NewTicker(e.t.Maintenance)
	defer sweep.Stop()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		all := false
		select {
		case <-ctx.Done():
			return
		case <-e.runnerWake:
		case <-sweep.C:
			all = true
		case <-timer.C:
			all = true
		}
		for _, id := range e.takeDirty(all) {
			if ctx.Err() != nil {
				return
			}
			select {
			case sem <- struct{}{}:
			default:
				e.wakeRunnerLater(id) // every slot busy: a finishing turn wakes us
				continue
			}
			if !e.tryStart(ctx, id, sem) {
				<-sem
			}
		}
		if next := e.nextCapWake(); !next.IsZero() {
			d := time.Until(next)
			if d < e.t.Tick {
				d = e.t.Tick
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
		}
	}
}

// takeDirty returns the conversations to check (all when all or when the
// wake carried no ids).
func (e *Engine) takeDirty(all bool) []int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runnerAll {
		all, e.runnerAll = true, false
	}
	var ids []int64
	if all {
		for id := range e.convs {
			ids = append(ids, id)
		}
	} else {
		for id := range e.runnerDirty {
			ids = append(ids, id)
		}
	}
	e.runnerDirty = map[int64]bool{}
	return ids
}

// wakeRunnerLater keeps id marked without waking the loop.
func (e *Engine) wakeRunnerLater(id int64) {
	e.mu.Lock()
	e.runnerDirty[id] = true
	e.mu.Unlock()
}

func (e *Engine) nextCapWake() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	var next time.Time
	now := e.now()
	for _, cs := range e.convs {
		if cs.capWake.After(now) && (next.IsZero() || cs.capWake.Before(next)) {
			next = cs.capWake
		}
	}
	return next
}

// tryStart claims the next runnable turn of a conversation and starts it.
// It reports whether a turn was started (and owns the semaphore slot).
func (e *Engine) tryStart(ctx context.Context, id int64, sem chan struct{}) bool {
	e.mu.Lock()
	cs := e.convs[id]
	ok := cs != nil && cs.kind == store.KindWork && cs.state == store.ConvActive && !cs.paused &&
		cs.running == nil && !e.now().Before(cs.capWake)
	var lastEnd time.Time
	if ok {
		lastEnd = cs.lastTurnEnd
	}
	e.mu.Unlock()
	if !ok {
		return false
	}
	var (
		tr      store.Turn
		req     TurnRequest
		got     bool
		request string // delegation request text (inbox turns)
	)
	err := e.update(func(tx *store.Tx) error {
		var err error
		tr, got, err = tx.NextRunnable(id)
		if err != nil || !got {
			return err
		}
		if id == e.inboxID {
			req, request, got, err = e.startDelegation(tx, id, tr)
			return err
		}
		th, _, err := tx.Thread(id)
		if err != nil {
			return err
		}
		in, _, err := tx.Inbound(tr.InboundID)
		if err != nil {
			return err
		}
		files, err := tx.InboundFiles(tr.InboundID)
		if err != nil {
			return err
		}
		req = TurnRequest{ConvID: id, ThreadID: th.ThreadID, Cwd: th.Cwd, Model: th.Model, Effort: th.Effort,
			TurnKey: tr.TurnKey, Text: in.Body}
		if th.PendingModel != "" {
			req.Model = th.PendingModel
		}
		if th.PendingEffort != "" {
			req.Effort = th.PendingEffort
		}
		req.Text, req.Images = turnInput(in.Body, files)
		req.Text = strings.TrimSpace(req.Text + "\n\n" + outboundInstructions(e.outboundDir(tr.TurnKey)))
		if err := tx.MarkStarting(tr.ID, th.ThreadID); err != nil {
			return err
		}
		if !lastEnd.IsZero() && ms(lastEnd) > tr.QueuedAt {
			// It was queued behind the turn that just ended (R-35).
			return e.enqueueNotice(tx, id, fmt.Sprintf("turn:%d:starting", tr.ID), textStarting(in.Body))
		}
		return nil
	})
	if err != nil {
		e.logf("start turn in %d: %v", id, err)
		return false
	}
	if !got {
		return false
	}
	rs := &runState{turnID: tr.ID, turnKey: tr.TurnKey, done: make(chan struct{}), request: request}
	e.mu.Lock()
	cs.running = rs
	e.mu.Unlock()
	e.count(func(s *engineStats) { s.turns++ })
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() { <-sem }()
		e.execTurn(ctx, cs, tr, req, rs)
	}()
	return true
}

// turnInput builds the Codex input of a prompt and its attachments: images
// become image inputs, files are listed with their local path and original
// name, voice messages contribute their transcript.
// voiceLead introduces voice transcripts in a prompt.
const voiceLead = "Voice message (automatic transcript; it may contain recognition errors, especially names, identifiers and commands; prefer typed text when they conflict):"

func turnInput(body string, files []store.InboundFile) (string, []string) {
	text := strings.TrimSpace(body)
	var images, listed, voice, failed []string
	for _, f := range files {
		var ref fileRef
		_ = json.Unmarshal([]byte(f.ExtRef), &ref)
		ct := firstNonEmpty(ref.ContentType, f.Kind)
		switch {
		case f.State != "ready":
			failed = append(failed, fmt.Sprintf("- %s: %s", f.Name, firstNonEmpty(f.Error, "not available")))
		case f.Kind == "image":
			images = append(images, f.LocalPath)
		case f.Kind == "voice":
			data, err := os.ReadFile(f.LocalPath)
			if err != nil {
				failed = append(failed, fmt.Sprintf("- %s: the transcript is missing", f.Name))
				continue
			}
			voice = append(voice, strings.TrimSpace(string(data)))
		default:
			listed = append(listed, fmt.Sprintf("- %s (original name: %s; %s)", f.LocalPath, f.Name, ct))
		}
	}
	var sections []string
	if len(voice) > 0 {
		v := voiceLead + "\n" + strings.Join(voice, "\n\n")
		if text == "" {
			text = v
		} else {
			sections = append(sections, v)
		}
	}
	if len(listed) > 0 {
		sections = append(sections, "Attached files (saved locally):\n"+strings.Join(listed, "\n"))
	}
	if len(failed) > 0 {
		sections = append(sections, "Attachments that could not be used:\n"+strings.Join(failed, "\n"))
	}
	if len(sections) > 0 {
		text = strings.TrimSpace(text + "\n\n" + strings.Join(sections, "\n\n"))
	}
	return text, images
}

// execTurn runs one turn and records its outcome.
func (e *Engine) execTurn(ctx context.Context, cs *convState, tr store.Turn, req TurnRequest, rs *runState) {
	defer close(rs.done)
	id := cs.id
	accepted := false
	prog := &progressState{}
	touch := func() { rs.lastEvent.Store(ms(e.now())) }
	touch()
	hooks := TurnHooks{
		Progress: func(text string) {
			touch()
			if rs.request == "" { // delegated turns post no progress
				e.progress(ctx, cs, tr, prog, text)
			}
		},
		Activity: touch,
		Cancelled: func() bool {
			e.mu.Lock()
			defer e.mu.Unlock()
			return rs.cancelled
		},
		ThreadStarted: func(threadID string) error {
			if rs.request != "" {
				return e.bindDelegThread(tr, rs.request, threadID)
			}
			return e.bindThread(id, threadID)
		},
		Started: func(codexTurnID string) {
			touch()
			accepted = true
			e.mirrorSkipTurn(codexTurnID)
			if err := e.update(func(tx *store.Tx) error { return tx.MarkRunning(tr.ID, codexTurnID) }); err != nil {
				e.logf("mark running %s: %v", cs.label, err)
			}
		},
	}
	if dir := e.outboundDir(tr.TurnKey); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			e.logf("output directory of %s: %v", cs.label, err)
		}
	}
	stopWatch := e.watchSilence(ctx, cs, tr, rs)
	out, err := e.ag.Run(ctx, req, hooks)
	stopWatch()
	if ctx.Err() != nil {
		// Shutting down: nothing is recorded; the next start reconciles.
		e.clearRun(id, rs)
		return
	}
	if errors.Is(err, ErrUnknownOutcome) {
		e.logf("turn %d in %s: %v; looking it up", tr.ID, cs.label, err)
		threadID := req.ThreadID
		if threadID == "" {
			threadID = e.turnThread(cs, tr)
		}
		found, ok, lerr := e.ag.Lookup(ctx, id, threadID, tr.TurnKey)
		switch {
		case ctx.Err() != nil:
			e.clearRun(id, rs)
			return
		case lerr == nil && ok && found.Status != StatusInProgress:
			out = found
		case lerr == nil && !ok && !accepted:
			out = TurnOutcome{NotStarted: true, Status: StatusFailed, Error: "not recorded by Codex"}
		default:
			out = TurnOutcome{Status: StatusInterrupted, Error: "Codex stopped during the turn"}
		}
	} else if err != nil {
		out = TurnOutcome{Status: StatusFailed, Error: err.Error()}
	}
	e.mu.Lock()
	cancelled, stalled := rs.cancelled, rs.stalled
	e.mu.Unlock()
	if stalled && !cancelled && out.Status != StatusCompleted {
		out = TurnOutcome{CodexTurnID: out.CodexTurnID, Status: StatusInterrupted, Error: errStalled}
	}
	e.finish(ctx, cs, tr, out, cancelled)
	e.clearRun(id, rs)
}

func (e *Engine) clearRun(id int64, rs *runState) {
	e.mu.Lock()
	if cs := e.convs[id]; cs != nil && cs.running == rs {
		cs.running = nil
	}
	e.mu.Unlock()
	e.heatUser(id)
	e.wakeSender()
	e.wakeAll()
}

// wakeAll marks every conversation for the next runner pass (a slot freed).
func (e *Engine) wakeAll() {
	e.mu.Lock()
	e.runnerAll = true
	e.mu.Unlock()
	wake(e.runnerWake)
}

func (e *Engine) threadOf(id int64) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cs := e.convs[id]; cs != nil {
		return cs.threadID
	}
	return ""
}

// bindThread makes a new thread binding durable before turn/start.
func (e *Engine) bindThread(id int64, threadID string) error {
	err := e.update(func(tx *store.Tx) error {
		th, _, err := tx.Thread(id)
		if err != nil {
			return err
		}
		th.ConvID, th.ThreadID = id, threadID
		return tx.Bind(th)
	})
	if err != nil {
		return err
	}
	e.mu.Lock()
	if cs := e.convs[id]; cs != nil {
		cs.threadID = threadID
	}
	e.byThread[threadID] = id
	e.mu.Unlock()
	e.mirrorBound(threadID, id)
	return nil
}

// finish records the outcome of a turn: the final or one notice, in the
// same transaction as the turn's end.
func (e *Engine) finish(ctx context.Context, cs *convState, tr store.Turn, out TurnOutcome, cancelled bool) {
	if cs.id == e.inboxID {
		if out.Capacity && (out.NotStarted || out.Status == StatusFailed) && !cancelled && e.capacityRetry(cs, tr, out) {
			return
		}
		e.finishDelegation(ctx, cs, tr, out, cancelled)
		return
	}
	var (
		state, errText string
		msgs           []store.OutMsg
		err            error
		notify         bool
	)
	base := fmt.Sprintf("turn:%d", tr.ID)
	switch {
	case cancelled:
		state, errText = store.TurnCancelled, "cancelled by the user"
		text := "Request cancelled."
		if out.Status == StatusCompleted {
			text = "Request cancelled. Codex had already finished; its answer was discarded."
		}
		msgs, err = e.notice(base+":cancelled", text)
	case out.Capacity && (out.NotStarted || out.Status == StatusFailed):
		if e.capacityRetry(cs, tr, out) {
			return
		}
		state, errText = store.TurnFailed, out.Error
		msgs, err = e.notice(base+":failed", "Codex is over capacity and the request did not run within 5 minutes of retrying. Resend it later.")
	case out.ActiveWriter:
		state, errText = store.TurnFailed, out.Error
		msgs, err = e.notice(base+":active", textActive)
	case out.NotStarted:
		state, errText = store.TurnNotStarted, out.Error
		msgs, err = e.notice(base+":notstarted", "Codex did not start this request ("+userError(out.Error)+"). It was not run; resend it if you still need it.")
	case out.Status == StatusCompleted:
		state = store.TurnCompleted
		visible, files, problems := e.stageArtifacts(tr, out.Final)
		final := strings.TrimSpace(visible)
		if len(problems) > 0 {
			final = strings.TrimSpace(final + "\n\n_Some files could not be attached:_\n- " + strings.Join(problems, "\n- "))
		}
		if final == "" && len(files) == 0 {
			final = "_Codex finished without a text answer._"
		}
		out.Final = final
		if final != "" {
			msgs, err = e.render(ctx, channel.RichDoc{Label: labelCodex, Markdown: final, Math: true}, store.OutFinal, base+":final")
		}
		msgs = append(msgs, files...)
		// Exactly one owner notification per final (CORE-DESIGN §4): the
		// Workflow card when configured, else one control-chat @mention.
		// It is recorded with the turn's end and sent once the final is
		// delivered (review L8; notify.go).
		notify = err == nil
	case out.Status == StatusInterrupted && out.Error == errStalled:
		state, errText = store.TurnInterrupted, out.Error
		msgs, err = e.notice(base+":interrupted", textStalled(e.t.StallLimit))
	case out.Status == StatusInterrupted:
		state, errText = store.TurnInterrupted, out.Error
		msgs, err = e.notice(base+":interrupted", textInterrupted)
	default:
		state, errText = store.TurnFailed, out.Error
		msgs, err = e.notice(base+":failed", "Codex reported an error and the request did not finish: "+userError(out.Error)+"\n\nIt was not re-run; resend it if you still need it.")
	}
	if err != nil {
		e.logf("render outcome of turn %d: %v", tr.ID, err)
		msgs, _ = e.notice(base+":failed", "The answer could not be rendered for this chat. Resend the request if you still need it.")
	}
	if !e.commitFinish(ctx, tr, func(tx *store.Tx) error {
		if err := tx.FinishTurn(tr.ID, state, errText, msgs); err != nil {
			return err
		}
		if err := recordUsage(tx, cs.id, state, out.Usage); err != nil {
			return err
		}
		if notify {
			return addNotify(tx, pendingNotify{Turn: tr.ID, Conv: cs.id, Text: clipText(out.Final, 400)})
		}
		return nil
	}) {
		return
	}
	if state == store.TurnCompleted {
		e.count(func(s *engineStats) { s.completed++ })
	}
	e.mu.Lock()
	cs.capWake = time.Time{}
	cs.lastTurnEnd = e.now()
	e.mu.Unlock()
	if notify {
		e.notifyN.Add(1)
		wake(e.notifyWake)
	}
}

// commitFinish runs a turn's finish transaction. A failure (busy
// database, full disk) is retried with backoff: the turn stays open and
// the chat waits until it commits (review L1). It reports success.
func (e *Engine) commitFinish(ctx context.Context, tr store.Turn, fn func(tx *store.Tx) error) bool {
	for attempt := 1; ; attempt++ {
		err := e.update(func(tx *store.Tx) error {
			if e.cfg.finishFault != nil {
				if err := e.cfg.finishFault(); err != nil {
					return err
				}
			}
			return fn(tx)
		})
		if err == nil {
			return true
		}
		if finalStoreError(err) || ctx.Err() != nil {
			e.logf("finish turn %d: %v", tr.ID, err)
			return false
		}
		back := e.t.Hot << min(attempt, 8)
		if back > e.t.MaxBackoff {
			back = e.t.MaxBackoff
		}
		if attempt <= 3 || attempt%10 == 0 {
			e.logf("finish turn %d failed (%d): %v; retry in %s", tr.ID, attempt, err, back)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(back):
		}
	}
}

// finalStoreError: store errors a retry cannot fix.
func finalStoreError(err error) bool {
	for _, f := range []error{store.ErrFenced, store.ErrNotAcquired, store.ErrDisabled, store.ErrState, store.ErrNotFound} {
		if errors.Is(err, f) {
			return true
		}
	}
	return false
}

// capacityRetry requeues a capacity failure inside the retry window
// measured from the first capacity error. It reports whether it requeued.
func (e *Engine) capacityRetry(cs *convState, tr store.Turn, out TurnOutcome) bool {
	now := e.now()
	deadline := fromMS(tr.RetryDeadline)
	if deadline.IsZero() {
		deadline = now.Add(e.t.CapacityRetry)
	}
	if !now.Before(deadline) {
		return false
	}
	back := e.t.CapacityBackoff << min(tr.Attempt, 5)
	if back > e.t.CapacityBackoff*16 {
		back = e.t.CapacityBackoff * 16
	}
	if rest := deadline.Sub(now); back > rest {
		back = rest
	}
	first := tr.RetryDeadline == 0
	err := e.update(func(tx *store.Tx) error {
		if err := tx.RequeueTurn(tr.ID, ms(deadline)); err != nil {
			return err
		}
		if first && cs.id != e.inboxID { // the inbox carries protocol records only
			return e.enqueueNotice(tx, cs.id, fmt.Sprintf("turn:%d:capacity", tr.ID),
				"Codex is at capacity right now; the request will be retried automatically for up to 5 minutes.")
		}
		return nil
	})
	if err != nil {
		e.logf("requeue turn %d: %v", tr.ID, err)
		return false
	}
	e.mu.Lock()
	cs.capWake = now.Add(back)
	e.mu.Unlock()
	e.logf("turn %d in %s hit capacity (attempt %d); retrying in %s", tr.ID, cs.label, tr.Attempt, back)
	return true
}

// mentionNotice is the control-chat @mention for a final.
func (e *Engine) mentionNotice(turnID int64, cs *convState) ([]store.OutMsg, error) {
	e.mu.Lock()
	label, title, url := cs.label, cs.title, cs.url
	ctl := e.controlID
	e.mu.Unlock()
	if ctl == 0 {
		return nil, nil
	}
	text := "Codex finished in " + title
	if title == "" {
		text = "Codex finished in " + label
	}
	if url != "" {
		text += ". Open: " + url
	}
	return e.render(context.Background(), channel.RichDoc{Label: labelHelper, Markdown: text + ".", Mention: true},
		store.OutNotice, fmt.Sprintf("notify:%d", turnID))
}

// internalRE matches ids and paths that must not reach a chat: UUIDs,
// Teams chat and message ids, Codex item ids and absolute paths.
var internalRE = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|19:[^\s"']+|\b(?:msg|turn|thread|rs|call)_[A-Za-z0-9]+|(?:[A-Za-z]:)?(?:/[^\s/"':]+){2,}/?|(?:\b[A-Za-z]:|\\\\[^\s\\"']+)\\[^\s"']*`)

// userError makes an error text fit for a chat: one line, no internal ids
// or paths.
func userError(s string) string {
	s = internalRE.ReplaceAllString(s, "…")
	s = strings.Join(strings.Fields(s), " ")
	if s == "" {
		s = "unknown error"
	}
	return clipText(s, 300)
}

func clipText(s string, n int) string {
	s = strings.TrimSpace(s)
	if len([]rune(s)) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

// cancelTurn cancels the running turn of a conversation: turn/interrupt,
// then kill after CancelWait. It reports whether a turn was running.
func (e *Engine) cancelRunning(ctx context.Context, id int64) bool {
	e.mu.Lock()
	cs := e.convs[id]
	var rs *runState
	if cs != nil {
		rs = cs.running
	}
	if rs != nil {
		rs.cancelled = true
	}
	e.mu.Unlock()
	if rs == nil {
		return false
	}
	ictx, cancel := context.WithTimeout(ctx, e.t.CancelWait)
	err := e.ag.Interrupt(ictx, id)
	cancel()
	if err != nil {
		e.logf("interrupt %d: %v; stopping its Codex process", id, err)
		e.ag.Kill(id)
		return true
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-rs.done:
		case <-ctx.Done():
		case <-time.After(e.t.CancelWait):
			e.logf("turn in %d did not stop after interrupt; stopping its Codex process", id)
			e.ag.Kill(id)
		}
	}()
	return true
}

// reconcile settles turns that were starting or running when the previous
// helper process stopped. Prepare already killed the old app-servers, so
// no such turn can still change; each is looked up by its turn key and ends
// with its final or exactly one notice. The conversation's next turn cannot
// start before (NextRunnable blocks on the open turn and the notice).
func (e *Engine) reconcile(ctx context.Context) {
	turns, err := e.st.OpenTurns()
	if err != nil {
		e.logf("reconcile: %v", err)
		return
	}
	sem := make(chan struct{}, e.cfg.Turns)
	for _, tr := range turns {
		if tr.State == store.TurnQueued {
			continue
		}
		cs := e.conv(tr.ConvID)
		if cs == nil {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		e.wg.Add(1)
		go func(tr store.Turn) {
			defer e.wg.Done()
			defer func() { <-sem }()
			e.reconcileTurn(ctx, cs, tr)
		}(tr)
	}
}

func (e *Engine) reconcileTurn(ctx context.Context, cs *convState, tr store.Turn) {
	threadID := tr.ThreadID
	if threadID == "" {
		threadID = e.turnThread(cs, tr)
	}
	rs := &runState{turnID: tr.ID, turnKey: tr.TurnKey, done: make(chan struct{})}
	e.mu.Lock()
	cs.running = rs
	e.mu.Unlock()
	defer func() {
		close(rs.done)
		e.clearRun(cs.id, rs)
	}()
	var out TurnOutcome
	if threadID == "" {
		out = TurnOutcome{NotStarted: true, Status: StatusFailed, Error: "no Codex thread yet"}
		e.finishReconciled(ctx, cs, tr, out)
		return
	}
	back := time.Second
	for attempt := 1; ; attempt++ {
		found, ok, err := e.ag.Lookup(ctx, cs.id, threadID, tr.TurnKey)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err == nil && !ok:
			if tr.State == store.TurnRunning {
				out = TurnOutcome{Status: StatusInterrupted, Error: "not found after restart"}
			} else {
				out = TurnOutcome{NotStarted: true, Status: StatusFailed, Error: "not recorded by Codex"}
			}
		case err == nil && found.Status != StatusInProgress:
			out = found
			if out.CodexTurnID != "" {
				e.mirrorSkipTurn(out.CodexTurnID)
			}
		case attempt >= 4:
			if err != nil {
				e.logf("reconcile turn %d in %s: %v", tr.ID, cs.label, err)
			}
			out = TurnOutcome{Status: StatusInterrupted, Error: "unresolved after restart"}
		default:
			if err != nil {
				e.logf("reconcile turn %d in %s (attempt %d): %v", tr.ID, cs.label, attempt, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(back):
			}
			back *= 2
			continue
		}
		break
	}
	e.finishReconciled(ctx, cs, tr, out)
}

func (e *Engine) finishReconciled(ctx context.Context, cs *convState, tr store.Turn, out TurnOutcome) {
	if cs.id == e.inboxID {
		out.Capacity = false
		e.finishDelegation(ctx, cs, tr, out, false)
		return
	}
	if out.NotStarted && !out.Capacity && !out.ActiveWriter {
		msgs, err := e.notice(fmt.Sprintf("turn:%d:notstarted", tr.ID), textNotStarted)
		if err == nil {
			err = e.update(func(tx *store.Tx) error { return tx.FinishTurn(tr.ID, store.TurnNotStarted, out.Error, msgs) })
		}
		if err != nil {
			e.logf("finish turn %d: %v", tr.ID, err)
		}
		return
	}
	if out.Capacity {
		// A capacity failure found after restart is reported, not retried.
		out.Capacity = false
		out.Status = StatusFailed
	}
	e.finish(ctx, cs, tr, out, false)
}

// Progress (commentary while a turn runs). Each commentary message becomes
// one chat message before the final, except:
//   - group chats (other people present): none;
//   - coalescing (CORE-DESIGN §3): when other turns wait behind this one in
//     the conversation, the account's writes are paced below the demand
//     (pace.go), or the account write gate closed within the last 5 min,
//     at most one progress message per turn;
//   - at most maxProgress per turn.
// The final is always complete; progress only shows the turn is alive.

const (
	maxProgress         = 20
	progressCoalesceFor = 5 * time.Minute
)

// progressState counts one turn's progress messages. Progress callbacks can
// come from more than one goroutine (the agent's reader and its replay of
// early items), so mu serializes them: each message gets its own delivery
// key, in callback order.
type progressState struct {
	mu     sync.Mutex
	posted int
}

func (e *Engine) progress(ctx context.Context, cs *convState, tr store.Turn, ps *progressState, text string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	text = strings.TrimSpace(text)
	e.mu.Lock()
	group := cs.group
	e.mu.Unlock()
	if text == "" || group || ps.posted >= maxProgress {
		return
	}
	now := e.now()
	// Paced account writes (the write rate is below the demand) or a
	// recent write throttle: one progress message per turn.
	coalesce := e.writeTokens.limited()
	if until := e.gate(e.gWrite); !until.IsZero() && now.Sub(until) < progressCoalesceFor {
		coalesce = true
	}
	if coalesce && ps.posted >= 1 {
		return
	}
	msgs, err := e.render(ctx, channel.RichDoc{Label: labelCodex, Markdown: text, Math: true}, store.OutProgress,
		fmt.Sprintf("turn:%d:progress:%d", tr.ID, ps.posted+1))
	if err != nil {
		e.logf("progress of %s: %v", cs.label, err)
		return
	}
	queued := false
	err = e.update(func(tx *store.Tx) error {
		queued = false
		if ps.posted >= 1 {
			open, err := tx.OpenTurnsOf(cs.id)
			if err != nil {
				return err
			}
			if len(open) > 1 {
				return nil // turns wait behind this one: one progress message is enough
			}
		}
		if _, err := tx.Enqueue(cs.id, msgs); err != nil {
			return err
		}
		queued = true
		return nil
	})
	if err != nil {
		e.logf("progress of %s: %v", cs.label, err)
		return
	}
	if queued {
		ps.posted++
		e.wakeSender()
	}
}
