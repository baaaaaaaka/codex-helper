package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// `helper restart` (F-CTL-11 / R-18, v0.1.20 restartHelperFromControl
// B:5286) and the drain notice (F-SVC-10):
//
//   - `helper restart` shows v0.1.20's safe-restart confirmation.
//   - `helper restart now` restarts when no Codex work runs. While work
//     runs it waits for it (SPEC F-SVC-10; v0.1.20 refused instead), at
//     most Timing.UpdateDrain, holding new turn starts; prompts sent
//     meanwhile get "…I saved this message and will resume it…" and run
//     once after the restart.
//   - `helper restart force` restarts at once; running turns are
//     interrupted and reported once by the next process (F-TRN-5).
//
// The request is durable (metaRestartRequest, committed with the reply):
// a process that stops while waiting is replaced by one that waits again.
// The restart itself records the confirmation the next process owes
// (metaRestart) in the transaction that clears the request, so "✅ Helper
// restart completed" is posted exactly once.

const metaRestartRequest = "engine_restart_request" // JSON restartRequest: a `helper restart now|force` not yet done

type restartRequest struct {
	Conv    int64  `json:"conv"`
	Inbound int64  `json:"inbound"`
	Quote   string `json:"quote,omitempty"`
	Force   bool   `json:"force,omitempty"`
}

// Texts (v0.1.20 unless noted).
const (
	textRestartConfirm = "## 🔄 Helper restart\n\n" +
		"This restarts the local Teams helper on this machine.\n" +
		"It will only restart when there is no active Codex work.\n\n" +
		"To restart, send:\n`helper restart now`\n\n" +
		"If you are debugging a stuck helper and accept interrupting work, send:\n`helper restart force`"
	textRestartScheduled = "🔄 Helper restart scheduled\n\n" +
		"I may be silent for a few seconds.\n" +
		"After it comes back, send `st` if you want to check status."
	textRestartBlockedUpdate = "⏳ Helper upgrade is already in progress.\n\n" +
		"I will not start another restart during upgrade.\n" +
		"Wait for the upgrade to finish, then send `st`."
	textRestartBlockedCodex = "⏳ Codex upgrade is already in progress.\n\n" +
		"I will not start another restart during the Codex upgrade.\n" +
		"Wait for the upgrade to finish, then send `st`."
	// textRestartRecordFailed: v0.1.20's text without the raw error.
	textRestartRecordFailed = "⚠️ Helper restart was not started because I could not record the post-restart confirmation."
	// The F-SVC-10 notices that replace the ACK of a prompt admitted
	// while a restart or an upgrade waits for running work (v0.1.20
	// sendDeferredServiceControlNotice; the restart one is its reload
	// text, as `helper reload` was removed by the owner).
	textDeferredUpgrade = "upgrade in progress. I saved this message and will resume it after the upgrade finishes."
	textDeferredRestart = "helper restart in progress. I saved this message and will resume it after the helper is back online."
)

// textRestartWaiting answers `helper restart now` while work runs (v0.1.20
// refused with "⏳ Codex work is still active."; SPEC F-SVC-10 waits).
func textRestartWaiting(limit time.Duration) string {
	return "⏳ Codex work is still active.\n\n" +
		"I will restart the helper when the running work finishes (waiting at most " + fmtDuration(limit) + "). " +
		"Messages sent meanwhile are saved and run once after the restart.\n\n" +
		"Debug only: `helper restart force` may interrupt active work."
}

func textRestartTimedOut(limit time.Duration) string {
	return "⚠️ Helper restart failed\n\n" +
		"Codex work was still running after " + fmtDuration(limit) + ", so the helper did not restart. " +
		"Send `helper restart now` again when it is idle, or `helper restart force` to interrupt it."
}

// restartForce is v0.1.20's helperRestartForce.
func restartForce(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "force", "--force", "restart force", "restart --force":
		return true
	}
	return false
}

// cmdRestart is `helper restart [now|force]`.
func (e *Engine) cmdRestart(ctx context.Context, j cmdJob, arg string) {
	if !restartConfirmed(arg) {
		e.reply(j, textRestartConfirm, nil)
		return
	}
	force := restartForce(arg)
	if e.draining() && !e.restartDrain.Load() {
		// An update waits for running work (or installs): it restarts
		// the helper itself.
		text := textRestartBlockedUpdate
		if v, ok, err := e.st.Meta(metaCodexUpgrade); err == nil && ok && v != "" {
			text = textRestartBlockedCodex
		}
		e.reply(j, text, nil)
		return
	}
	text := textRestartScheduled
	if !force && e.busy() {
		text = textRestartWaiting(e.t.UpdateDrain)
	}
	err := e.replyIn(j, func(tx *store.Tx) (string, error) {
		var prev restartRequest
		if ok, err := metaJSON(tx, metaRestartRequest, &prev); err != nil {
			return "", err
		} else if ok && !force {
			return "⏳ A helper restart is already waiting for the running work to finish.\n\nDebug only: `helper restart force` may interrupt active work.", nil
		}
		return text, setMetaJSON(tx, metaRestartRequest, restartRequest{Conv: j.conv, Inbound: j.inbound, Quote: j.quote, Force: force})
	})
	if err != nil {
		return
	}
	e.startRestart(ctx) // a waiting one picks up `force` at its next tick
}

// resumeRestart continues a restart request left by the previous process.
func (e *Engine) resumeRestart(ctx context.Context) {
	if v, ok, err := e.st.Meta(metaRestartRequest); err == nil && ok && v != "" {
		e.startRestart(ctx)
	}
}

func (e *Engine) startRestart(ctx context.Context) {
	if !e.restartRun.CompareAndSwap(false, true) {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.runRestart(ctx)
	}()
}

// runRestart waits for running work (unless forced: a `helper restart
// force` sent while it waits takes over at once), lets the reply go out,
// records the confirmation the next process owes and restarts. It polls
// only while a restart waits.
func (e *Engine) runRestart(ctx context.Context) {
	defer e.restartRun.Store(false)
	e.restartDrain.Store(true)
	e.drainFlag.Store(true)
	wake(e.registryWake)
	deadline := e.now().Add(e.t.UpdateDrain)
	tick := time.NewTicker(e.t.Tick)
	defer tick.Stop()
	logged := false
	var req restartRequest
	for {
		var have bool
		if err := e.update(func(tx *store.Tx) error {
			var err error
			have, err = metaJSON(tx, metaRestartRequest, &req)
			return err
		}); err != nil || !have {
			e.restartDrain.Store(false)
			e.undrain()
			return
		}
		if req.Force || !e.busy() {
			break
		}
		if !logged {
			e.logf("restart: waiting for running turns")
			logged = true
		}
		if !e.now().Before(deadline) {
			e.restartTimedOut(req)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
	// Everything already queued (the reply "scheduled" / "waiting", the
	// answers and notices of the work that just finished) goes out first,
	// so the confirmation after the restart follows it (bounded wait).
	e.waitOutboxIdle(ctx, 15*time.Second)
	err := e.update(func(tx *store.Tx) error {
		if err := tx.SetMeta(metaRestart, fmt.Sprintf("%d:%d:%s", req.Conv, req.Inbound, req.Quote)); err != nil {
			return err
		}
		return tx.SetMeta(metaRestartRequest, "")
	})
	if err != nil {
		e.logf("restart: %v", err)
		e.restartDrain.Store(false)
		e.undrain()
		e.restartNotice(req, textRestartRecordFailed, false)
		return
	}
	e.logf("restart requested")
	// The hold stays on: nothing starts before the restart.
	e.fail(ErrRestart)
}

// restartTimedOut gives up a `helper restart now` whose wait ran out.
func (e *Engine) restartTimedOut(req restartRequest) {
	e.logf("restart: turns still running after %s; not restarted", e.t.UpdateDrain)
	e.restartDrain.Store(false)
	e.undrain()
	e.restartNotice(req, textRestartTimedOut(e.t.UpdateDrain), true)
}

// restartNotice clears the request and tells the chat it came from.
func (e *Engine) restartNotice(req restartRequest, text string, clear bool) {
	err := e.update(func(tx *store.Tx) error {
		if clear {
			if err := tx.SetMeta(metaRestartRequest, ""); err != nil {
				return err
			}
		}
		if e.conv(req.Conv) == nil {
			return nil
		}
		msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("restart-failed:%d", req.Inbound), text, req.Quote)
		if err != nil {
			return err
		}
		_, err = tx.Enqueue(req.Conv, msgs)
		return err
	})
	if err != nil {
		e.logf("restart: %v", err)
	}
	e.wakeSender()
}

// waitOutboxIdle waits (at most limit) until no outbox row is queued or
// being sent. A row whose outcome is unknown does not hold it.
func (e *Engine) waitOutboxIdle(ctx context.Context, limit time.Duration) {
	e.wakeSender()
	deadline := time.Now().Add(limit)
	t := time.NewTicker(e.t.Tick)
	defer t.Stop()
	for time.Now().Before(deadline) {
		n, err := e.st.OutboxCounts()
		if err != nil || n["queued"]+n["sending"] == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// drainNotice is the text that replaces the ACK of a prompt admitted while
// running work is drained for a restart or an upgrade ("" otherwise).
func (e *Engine) drainNotice() string {
	switch {
	case !e.draining():
		return ""
	case e.restartDrain.Load():
		return textDeferredRestart
	}
	return textDeferredUpgrade
}
