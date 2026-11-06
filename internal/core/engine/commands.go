package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// cmdJob is one admitted owner command.
type cmdJob struct {
	conv    int64
	inbound int64
	ext     string
	text    string
	quote   string
}

// commandLoop dispatches commands without running them, so admission never
// waits on a slow command (review M2). Commands of one conversation run in
// admission order on that conversation's worker; conversations run in
// parallel. Urgent commands (cancel, status, help) run at once, so a slow
// `model` or `fork` never delays `helper cancel`.
func (e *Engine) commandLoop(ctx context.Context) {
	requeued := e.requeued
	e.requeued = nil
	for _, j := range requeued {
		e.dispatchCommand(ctx, j)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-e.cmdQueue:
			e.dispatchCommand(ctx, j)
		}
	}
}

func (e *Engine) dispatchCommand(ctx context.Context, j cmdJob) {
	kind := ""
	if cs := e.conv(j.conv); cs != nil {
		e.mu.Lock()
		kind = cs.kind
		e.mu.Unlock()
	}
	if urgentCommand(kind, j.text) {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.runCommand(ctx, j)
		}()
		return
	}
	e.cmdMu.Lock()
	defer e.cmdMu.Unlock()
	q, busy := e.cmdPending[j.conv]
	e.cmdPending[j.conv] = append(q, j)
	if busy {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			e.cmdMu.Lock()
			q := e.cmdPending[j.conv]
			if len(q) == 0 || ctx.Err() != nil {
				delete(e.cmdPending, j.conv)
				e.cmdMu.Unlock()
				return
			}
			next := q[0]
			e.cmdPending[j.conv] = q[1:]
			e.cmdMu.Unlock()
			e.runCommand(ctx, next)
		}
	}()
}

// urgentCommand: commands that must not wait behind another command of
// the same chat. They only read state or stop work. The text is parsed as
// it is run (parse.go), so `!status`, `/cancel` and `cx st` are urgent too.
func urgentCommand(kind, text string) bool {
	var p parsed
	if kind == store.KindControl {
		p = parseControl(text)
	} else {
		t, _ := stripCodexMention(text)
		p = parseWork(t)
	}
	if p.form != formCommand {
		return false
	}
	switch p.name {
	case cmdCancelName, cmdStatus, cmdHelp, cmdShutdown:
		return true
	}
	return false
}

// requeueCommands re-runs commands admitted but not finished before the
// last stop (every command is safe to repeat: the reply key dedupes). The
// command loop dispatches them all when it starts, so none is dropped
// for a full queue (review L9).
func (e *Engine) requeueCommands() {
	var jobs []cmdJob
	_ = e.update(func(tx *store.Tx) error {
		jobs = jobs[:0]
		e.mu.Lock()
		ids := make([]int64, 0, len(e.convs))
		group := map[int64]bool{}
		for id, cs := range e.convs {
			ids = append(ids, id)
			group[id] = cs.group
		}
		e.mu.Unlock()
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			open, err := tx.OpenInbound(id)
			if err != nil {
				return err
			}
			for _, in := range open {
				if in.Kind == store.InCommand && in.State == store.InQueued && in.Sender == store.SenderOwner {
					j := cmdJob{conv: id, inbound: in.ID, ext: in.ExtMsgID, text: in.Body}
					if group[id] {
						j.quote = in.ExtMsgID // as when it was admitted
					}
					jobs = append(jobs, j)
				}
			}
		}
		return nil
	})
	e.requeued = jobs
}

// reply finishes a command with a visible reply in the same transaction.
func (e *Engine) reply(j cmdJob, text string, extra func(tx *store.Tx) error) {
	msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("cmd:%d", j.inbound), text, j.quote)
	if err != nil {
		e.logf("render reply: %v", err)
		return
	}
	err = e.update(func(tx *store.Tx) error {
		if extra != nil {
			if err := extra(tx); err != nil {
				return err
			}
		}
		if _, err := tx.Enqueue(j.conv, msgs); err != nil {
			return err
		}
		return tx.SetInbound(j.inbound, store.InDone, 0)
	})
	if err != nil {
		e.logf("command reply in %d: %v", j.conv, err)
	}
	e.wakeSender()
}

// complete finishes a command that answers later (or elsewhere): extra and
// the command's end commit together, without a reply.
func (e *Engine) complete(j cmdJob, extra func(tx *store.Tx) error) {
	err := e.update(func(tx *store.Tx) error {
		if err := extra(tx); err != nil {
			return err
		}
		return tx.SetInbound(j.inbound, store.InDone, 0)
	})
	if err != nil {
		e.logf("command in %d: %v", j.conv, err)
	}
	e.wakeSender()
}

func (e *Engine) runCommand(ctx context.Context, j cmdJob) {
	cs := e.conv(j.conv)
	if cs == nil {
		return
	}
	e.mu.Lock()
	kind := cs.kind
	e.mu.Unlock()
	// The grammar is in parse.go, the handlers in cmdtable.go.
	if kind == store.KindControl {
		e.runControl(ctx, j, cs)
		return
	}
	e.runWork(ctx, j, cs)
}

// cmdNew creates a work conversation. The command and the conversation row
// commit together; the chat itself is created by ensureChats, so a restart
// in between neither loses nor duplicates it.
func (e *Engine) cmdNew(ctx context.Context, j cmdJob, arg string) {
	dir, err := checkDir(arg)
	if err != nil {
		e.reply(j, err.Error(), nil)
		return
	}
	var id int64
	// No reply of its own: as in v0.1.20, the control chat hears about the
	// new chat once it exists ("✅ Work chat created: …", createChat).
	e.complete(j, func(tx *store.Tx) error {
		label, err := e.nextLabel(tx)
		if err != nil {
			return err
		}
		id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName,
			Title: chatTitle(label, filepath.Base(dir)), TitleSource: "auto"})
		if err != nil {
			return err
		}
		if err := tx.Bind(store.Thread{ConvID: id, Cwd: dir}); err != nil {
			return err
		}
		_, err = e.refresh(tx, id)
		return err
	})
	if id != 0 {
		e.mu.Lock()
		if cs := e.convs[id]; cs != nil {
			cs.cwd = dir
		}
		e.mu.Unlock()
		e.createChatNow(ctx, id)
	}
}

func chatTitle(label, name string) string {
	if name == "" || name == "." || name == "/" {
		return label
	}
	return label + " · " + name
}

func checkDir(arg string) (string, error) {
	arg = strings.Trim(strings.TrimSpace(arg), "`\"'")
	if arg == "" {
		return "", fmt.Errorf("Usage: `new <directory>`")
	}
	if arg == "~" || strings.HasPrefix(arg, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			arg = filepath.Join(home, strings.TrimPrefix(arg, "~"))
		}
	}
	if !filepath.IsAbs(arg) {
		return "", fmt.Errorf("Use an absolute directory path: `new /path/to/project`")
	}
	st, err := os.Stat(arg)
	if err != nil || !st.IsDir() {
		return "", fmt.Errorf("Directory not found: `%s`", arg)
	}
	return filepath.Clean(arg), nil
}

// nextLabel allocates the next free sNNN label.
func (e *Engine) nextLabel(tx *store.Tx) (string, error) {
	v, _, err := tx.Meta(metaNextLabel)
	if err != nil {
		return "", err
	}
	n := 1
	fmt.Sscan(v, &n)
	e.mu.Lock()
	used := map[string]bool{}
	for _, cs := range e.convs {
		used[cs.label] = true
	}
	e.mu.Unlock()
	for used[fmt.Sprintf("s%03d", n)] {
		n++
	}
	if err := tx.SetMeta(metaNextLabel, fmt.Sprint(n+1)); err != nil {
		return "", err
	}
	return fmt.Sprintf("s%03d", n), nil
}

// cmdCancel: `cancel` / `cancel last` / `cancel queued` cancels the newest
// queued request, else the running one; `cancel running` the running one
// only; `cancel all` cancels everything.
func (e *Engine) cmdCancel(ctx context.Context, j cmdJob, cs *convState, arg string) {
	e.mu.Lock()
	var runningID int64
	if cs.running != nil {
		runningID = cs.running.turnID
	}
	e.mu.Unlock()
	var cancelled int
	all := arg == "all"
	// `helper cancel running` (named by the queued ACK) interrupts the
	// running request only; the queued ones stay.
	onlyRunning := arg == "running"
	err := e.update(func(tx *store.Tx) error {
		cancelled = 0
		if onlyRunning {
			return nil
		}
		open, err := tx.OpenTurnsOf(cs.id)
		if err != nil {
			return err
		}
		for i := len(open) - 1; i >= 0; i-- {
			tr := open[i]
			if tr.State != store.TurnQueued || tr.ID == runningID {
				continue
			}
			msgs, err := e.notice(fmt.Sprintf("turn:%d:cancelled", tr.ID), "Queued request cancelled.")
			if err != nil {
				return err
			}
			if err := tx.FinishTurn(tr.ID, store.TurnCancelled, "cancelled by the user", msgs); err != nil {
				return err
			}
			cancelled++
			if !all {
				break
			}
		}
		return nil
	})
	if err != nil {
		e.logf("cancel in %s: %v", cs.label, err)
	}
	// The reply is enqueued before the running turn is interrupted, so
	// it always precedes the turn's "Request cancelled." (R-20-33).
	e.mu.Lock()
	running := cs.running != nil && (all || cancelled == 0)
	e.mu.Unlock()
	switch {
	case running && cancelled > 0:
		e.reply(j, fmt.Sprintf("Cancelling the running request and %s.", plural(cancelled, "queued request", "queued requests")), nil)
	case running:
		e.reply(j, "Cancelling the running request…", nil)
	case cancelled > 0:
		e.reply(j, fmt.Sprintf("Cancelled %s.", plural(cancelled, "queued request", "queued requests")), nil)
	default:
		e.reply(j, "Nothing to cancel: no request is running or queued.", nil)
	}
	if running {
		e.cancelRunning(ctx, cs.id)
	}
	e.wakeRunner(cs.id)
}

// cmdRetry runs a request again: `helper retry` the last duplicate-ignored
// prompt, else (and `helper retry last` first) the last request that
// failed because Codex needed an update.
func (e *Engine) cmdRetry(j cmdJob, cs *convState, arg string) {
	key := fmt.Sprintf("%s%d", metaRetryPrefix, cs.id)
	var failed int64
	if v, ok, err := e.st.Meta(key); err == nil && ok {
		failed, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	}
	e.mu.Lock()
	dup := cs.lastDup
	useDup := dup != 0 && (arg != "last" || failed == 0)
	if useDup {
		cs.lastDup = 0
	}
	e.mu.Unlock()
	switch {
	case useDup:
		e.reply(j, "Running the ignored request again.", func(tx *store.Tx) error {
			_, err := tx.RetryTurn(cs.id, dup)
			return err
		})
	case failed != 0:
		e.reply(j, "Running the failed request again.", func(tx *store.Tx) error {
			if err := tx.SetMeta(key, ""); err != nil {
				return err
			}
			_, err := tx.RetryTurn(cs.id, failed)
			return err
		})
	default:
		e.reply(j, "Nothing to retry: no request was ignored as a duplicate or failed for a Codex update recently.", nil)
		return
	}
	e.wakeRunner(cs.id)
}

// cmdUnpause lifts the breaker. Held prompts are not run: they are marked
// ignored first so Unpause releases nothing.
func (e *Engine) cmdUnpause(j cmdJob, cs *convState) {
	held := 0
	e.reply(j, "Resumed: new requests in this chat run again. Requests sent while paused were not run; resend them if needed.", func(tx *store.Tx) error {
		held = 0
		open, err := tx.OpenInbound(cs.id)
		if err != nil {
			return err
		}
		for _, in := range open {
			if in.State == store.InHeld {
				if err := tx.SetInbound(in.ID, store.InIgnored, 0); err != nil {
					return err
				}
				held++
			}
		}
		if _, err := tx.Unpause(cs.id); err != nil {
			return err
		}
		_, err = e.refresh(tx, cs.id)
		return err
	})
	e.mu.Lock()
	cs.breaker = nil
	e.mu.Unlock()
	if held > 0 {
		e.logf("unpaused %s; %d held prompts not run", cs.label, held)
	}
	e.wakeRunner(cs.id)
}

func (e *Engine) cmdPark(j cmdJob, cs *convState) {
	e.mu.Lock()
	busy := cs.running != nil
	e.mu.Unlock()
	if busy {
		e.reply(j, "A request is still running; park the chat after it ends.", nil)
		return
	}
	e.reply(j, "Parked "+cs.label+": this chat is no longer read. Messages sent here while it is parked are read and run once it wakes. Wake it from the control chat with `r "+cs.label+"`, or by using the session in Codex.", func(tx *store.Tx) error {
		if err := tx.Park(cs.id); err != nil {
			return err
		}
		_, err := e.refresh(tx, cs.id)
		return err
	})
}

// cmdRestart restarts the helper process (the service supervisor, or the
// caller of Run, starts it again on the same state when Run returns
// ErrRestart). A running turn is interrupted by it, so it needs `now`.
func (e *Engine) cmdRestart(j cmdJob, arg string) {
	e.mu.Lock()
	busy := 0
	for _, cs := range e.convs {
		if cs.running != nil {
			busy++
		}
	}
	e.mu.Unlock()
	if busy > 0 && arg != "now" {
		e.reply(j, fmt.Sprintf("%s running; restarting would interrupt it. Send `helper restart now` to restart anyway.",
			plural(busy, "request is", "requests are")), nil)
		return
	}
	// The new process confirms, once it has read the chat the command came
	// from; the old one only records that a confirmation is owed.
	err := e.update(func(tx *store.Tx) error {
		if err := tx.SetMeta(metaRestart, fmt.Sprintf("%d:%d:%s", j.conv, j.inbound, j.quote)); err != nil {
			return err
		}
		return tx.SetInbound(j.inbound, store.InDone, 0)
	})
	if err != nil {
		e.logf("restart: %v", err)
		e.reply(j, "Restart failed: the request could not be recorded.", nil)
		return
	}
	e.logf("restart requested")
	e.fail(ErrRestart)
}

// findLabel finds a work conversation by label ("s003", "3").
func (e *Engine) findLabel(arg string) *convState {
	label := strings.ToLower(strings.TrimSpace(arg))
	if label != "" && label[0] >= '0' && label[0] <= '9' {
		var n int
		fmt.Sscan(label, &n)
		label = fmt.Sprintf("s%03d", n)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, cs := range e.convs {
		if strings.EqualFold(cs.label, label) && cs.kind == store.KindWork {
			return cs
		}
	}
	return nil
}

// cmdParkLabel parks a session from the control chat.
func (e *Engine) cmdParkLabel(j cmdJob, arg string) {
	cs := e.findLabel(arg)
	if cs == nil {
		e.reply(j, "No session `"+strings.TrimSpace(arg)+"`. Send `list` to see sessions.", nil)
		return
	}
	e.mu.Lock()
	busy, state, label := cs.running != nil, cs.state, cs.label
	e.mu.Unlock()
	switch {
	case state == store.ConvParked:
		e.reply(j, label+" is already parked.", nil)
	case state != store.ConvActive:
		e.reply(j, label+" is "+state+"; it cannot be parked.", nil)
	case busy:
		e.reply(j, label+" is running a request; park it after it ends.", nil)
	default:
		e.reply(j, "Parked "+label+": its chat is no longer read. Messages sent to it while it is parked are read and run once it wakes. Wake it with `r "+label+"`, or by using the session in Codex.", func(tx *store.Tx) error {
			if err := tx.Park(cs.id); err != nil {
				return err
			}
			_, err := e.refresh(tx, cs.id)
			return err
		})
	}
}

// cmdWake wakes a parked conversation by label.
func (e *Engine) cmdWake(j cmdJob, arg string) {
	target := e.findLabel(arg)
	if target == nil {
		e.reply(j, "No session `"+strings.TrimSpace(arg)+"`. Send `list` to see sessions.", nil)
		return
	}
	e.mu.Lock()
	state, label := target.state, target.label
	e.mu.Unlock()
	if state != store.ConvParked {
		e.reply(j, label+" is "+state+"; nothing to wake.", nil)
		return
	}
	e.reply(j, "Resumed "+label+": its chat is read again.", func(tx *store.Tx) error {
		if err := tx.Wake(target.id); err != nil {
			return err
		}
		if err := e.enqueueNotice(tx, target.id, fmt.Sprintf("resumed:%d", j.inbound),
			"This chat was resumed: messages here are read again. Requests sent while it was parked run now."); err != nil {
			return err
		}
		_, err := e.refresh(tx, target.id)
		return err
	})
	e.heatUser(target.id)
}

func (e *Engine) listText() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var rows []*convState
	for _, cs := range e.convs {
		if cs.kind == store.KindWork && cs.state != store.ConvClosed && cs.id != e.inboxID {
			rows = append(rows, cs)
		}
	}
	if len(rows) == 0 {
		return "No sessions yet. Start one with `new <directory>`."
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].label < rows[j].label })
	var b strings.Builder
	b.WriteString("Sessions:\n\n")
	for _, cs := range rows {
		st := cs.state
		if cs.running != nil {
			st = "running"
		}
		if cs.paused {
			st += ", paused"
		}
		fmt.Fprintf(&b, "- %s — %s (%s)\n", cs.label, cs.title, st)
	}
	return b.String()
}

func (e *Engine) statusText() string {
	s := e.Snapshot()
	text := fmt.Sprintf("Helper %s on %s: up %s; %d sessions (%d active, %d parked, %d running); %d requests run, %d answered.",
		e.cfg.Version, e.machineLabel(), s.Uptime.Round(time.Second), s.Conversations, s.Active, s.Parked, s.Running, s.Turns, s.Completed)
	if e.draining() {
		text += " New requests wait for an update (it waits for running requests to finish)."
	} else if v, ok, err := e.st.Meta(metaUpdateRequest); err == nil && ok && v != "" {
		text += " A helper update is scheduled (`update status`)."
	}
	return text
}

func (e *Engine) workStatus(cs *convState) string {
	e.mu.Lock()
	label, state, cwd, running, paused := cs.label, cs.state, cs.cwd, cs.running != nil, cs.paused
	e.mu.Unlock()
	var queued int
	_ = e.update(func(tx *store.Tx) error {
		open, err := tx.OpenTurnsOf(cs.id)
		for _, t := range open {
			if t.State == store.TurnQueued {
				queued++
			}
		}
		return err
	})
	run := "idle"
	if running {
		run = "running a request"
	}
	p := ""
	if paused {
		p = " Auto-start is paused (`helper unpause`)."
	}
	return fmt.Sprintf("%s (%s): %s, %d queued. Directory: `%s`.%s", label, state, run, queued, cwd, p)
}

// restartConfirm is a `helper restart` confirmation the new process owes.
type restartConfirm struct {
	conv, inbound int64
	quote         string
}

// loadRestartConfirm reads a pending restart confirmation left by the
// previous process.
func (e *Engine) loadRestartConfirm() {
	v, ok, err := e.st.Meta(metaRestart)
	if err != nil || !ok || v == "" {
		return
	}
	parts := strings.SplitN(v, ":", 3)
	if len(parts) != 3 {
		return
	}
	conv, _ := strconv.ParseInt(parts[0], 10, 64)
	inbound, _ := strconv.ParseInt(parts[1], 10, 64)
	e.mu.Lock()
	e.restart = restartConfirm{conv: conv, inbound: inbound, quote: parts[2]}
	e.mu.Unlock()
}

// confirmRestart posts the restart confirmation after the first successful
// read of the chat the command came from, so that it follows this process
// taking over.
func (e *Engine) confirmRestart(conv int64) {
	e.mu.Lock()
	rc := e.restart
	if rc.conv == 0 || rc.conv != conv {
		e.mu.Unlock()
		return
	}
	e.restart = restartConfirm{}
	e.mu.Unlock()
	msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("restart:%d", rc.inbound), textRestarted, rc.quote)
	if err != nil {
		e.logf("render restart confirmation: %v", err)
		return
	}
	err = e.update(func(tx *store.Tx) error {
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		return tx.SetMeta(metaRestart, "")
	})
	if err != nil {
		e.logf("restart confirmation: %v", err)
	}
	e.wakeSender()
}
