package engine

import (
	"context"
	"fmt"
	"sort"
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
	queueKey := j.conv
	if answerCommand(kind, j.text) {
		queueKey = -j.conv
	}
	q, busy := e.cmdPending[queueKey]
	e.cmdPending[queueKey] = append(q, j)
	if busy {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			e.cmdMu.Lock()
			q := e.cmdPending[queueKey]
			if len(q) == 0 || ctx.Err() != nil {
				delete(e.cmdPending, queueKey)
				e.cmdMu.Unlock()
				return
			}
			next := q[0]
			e.cmdPending[queueKey] = q[1:]
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
	defer e.wakeRunner(j.conv)
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
	req, errText := e.parseNew(ctx, arg) // batch A: v0.1.20 `new` (dashboard.go)
	if errText != "" {
		e.reply(j, errText, nil)
		return
	}
	dir, titleSource := req.dir, "user" // `new <dir> -- <title>`
	if sanitizeTitle(req.title) == "" {
		titleSource = "auto"
	}
	var id int64
	// v0.1.20's title of a new chat: "💬 <machine> - New message in
	// <workspace>" until its first answer names it; `-- <title>` is the
	// owner's title, kept like `helper rename`.
	machine := e.machineLabel() // reads the store: not inside the transaction
	title := workTitle(machine, "", placeholderTitle(dir), dir)
	if titleSource == "user" {
		title = workTitle(machine, req.title, "", dir)
	}
	// No reply of its own: as in v0.1.20, the control chat hears about the
	// new chat once it exists ("✅ Work chat created: …", createChat).
	e.complete(j, func(tx *store.Tx) error {
		label, err := e.nextLabel(tx)
		if err != nil {
			return err
		}
		id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName,
			Title: title, TitleSource: titleSource})
		if err != nil {
			return err
		}
		if titleSource == "user" {
			if err := tx.SetMeta(fmt.Sprint(metaUserTitle, id), sanitizeTitle(req.title)); err != nil {
				return err
			}
		}
		th, err := e.newChatThread(tx, id, dir) // the global defaults (defaults.go, batch B)
		if err != nil {
			return err
		}
		if req.model != "" { // `new --model` (batch A)
			th.Model, th.ModelSource = selector(req.model), modelSrcChatOverride
		}
		if err := tx.Bind(th); err != nil {
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

// placeholderTitle is v0.1.20's NewWorkChatPlaceholderTitle: the title of
// a work chat before its first answer names it.
func placeholderTitle(cwd string) string {
	ws := ""
	if base := lastPathElement(cwd); base != "" {
		ws = sanitizeTitle(base)
	}
	if ws == "" {
		ws = "workspace"
	}
	return "New message in " + ws
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
	state, label, url := target.state, target.label, target.url
	e.mu.Unlock()
	if state != store.ConvParked {
		e.reply(j, label+" is "+state+"; nothing to wake.", nil)
		return
	}
	message := "Resumed " + label + ": its chat is read again."
	if url != "" {
		message = fmt.Sprintf("Resumed %s.\n\nOpen Work chat: %s\n\nMessages in that chat will be read again.", label, url)
	}
	e.reply(j, message, func(tx *store.Tx) error {
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
	at := e.activityTimes()
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
	sort.Slice(rows, func(i, j int) bool { return byActivity(at, rows[i].id, rows[j].id) }) // activity.go
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
		fmt.Fprintf(&b, "- %s — %s (%s; auto park: %s)\n", cs.label, cs.title, st, autoParkValue(cs.autoParkOff))
	}
	return b.String()
}

func (e *Engine) statusText() string {
	s := e.Snapshot()
	text := fmt.Sprintf("Helper %s on %s: up %s; %d sessions (%d active, %d parked, %d running); %d requests run, %d answered.",
		e.cfg.Version, e.machineLabel(), s.Uptime.Round(time.Second), s.Conversations, s.Active, s.Parked, s.Running, s.Turns, s.Completed)
	if e.draining() {
		text += " New requests wait for an update (it waits for running requests to finish)."
	} else if s, err := ReadStoreStatus(e.st); err == nil && s.UpdateScheduled != "" {
		text += " A helper update is scheduled (`update status`)."
	}
	return text
}

func (e *Engine) workStatus(cs *convState) string {
	e.mu.Lock()
	label, state, cwd, running, paused := cs.label, cs.state, cs.cwd, cs.running != nil, cs.paused
	autoParkOff := cs.autoParkOff
	e.mu.Unlock()
	var queued, unsent, unconfirmed int
	_ = e.update(func(tx *store.Tx) error {
		open, err := tx.OpenTurnsOf(cs.id)
		for _, t := range open {
			if t.State == store.TurnQueued {
				queued++
			}
		}
		if err != nil {
			return err
		}
		unsent, unconfirmed, err = tx.DeliveryCounts(cs.id)
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
	text := fmt.Sprintf("%s (%s): %s, %d queued. Directory: `%s`.%s Auto park: %s.", label, state, run, queued, cwd, p, autoParkValue(autoParkOff))
	if unconfirmed > 0 {
		text += fmt.Sprintf(" Delivery: %d awaiting send, %d unconfirmed (retained without automatic resend; later messages can continue).", unsent, unconfirmed)
	}
	return text
}

// restartConfirm is a `helper restart` confirmation the new process owes.
type restartConfirm struct {
	conv, inbound int64
	quote         string
}

// loadRestartConfirm reads a pending restart confirmation left by the
// previous process.
func (e *Engine) loadRestartConfirm() {
	v, ok, err := e.st.RestartConfirmation()
	if err != nil || !ok {
		return
	}
	e.mu.Lock()
	e.restart = restartConfirm{conv: v.Conv, inbound: v.Inbound, quote: v.Quote}
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
		if err := e.addServiceNotify(tx, conv, fmt.Sprintf("restart:%d", rc.inbound), textRestarted); err != nil {
			return err
		}
		return tx.DeleteRestartConfirmation()
	})
	if err != nil {
		e.logf("restart confirmation: %v", err)
	}
	e.wakeSender()
}

// Answers keep admission order on their own command lane. A slow fork/model
// command must not block them, and concurrent answer goroutines must not
// reorder an edit followed by the last answer of a group.
func answerCommand(kind, text string) bool {
	if kind == store.KindControl {
		return parseControl(text).name == cmdAnswer
	}
	text, _ = stripCodexMention(text)
	return parseWork(text).name == cmdAnswer
}
