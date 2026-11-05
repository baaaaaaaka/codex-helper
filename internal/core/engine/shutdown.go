package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// `shutdown` in the control chat (owner ruling 2026-11-05): the same stop
// as `cxp teams stop`. It creates the kill-switch file DISABLED next to the
// store (recorded as a control-chat shutdown on this host), posts one
// reply, and stops this process once the reply is out. Every other helper
// sharing the store stays idle until `cxp teams start` removes the file;
// Teams is no longer read, so the reply says how to start it again.
//
// From the moment the file exists the helper starts no turn, reads no chat
// and sends only the control chat's messages up to the reply (in order), so
// the reply is the last thing it posts. Everything else already queued
// stays in the store and is sent after the start.
//
// The file is written before the command is marked done: if the process
// dies in between, the command runs again at the next start (after `cxp
// teams start`) and stops the helper again, which errs on the side of the
// owner's last word; the reply is keyed by the command, so it is posted
// once.

// textShutdown is the reply to `shutdown`.
const textShutdown = "Stopped on all machines. Teams is no longer read, so it cannot be restarted from here: run `cxp teams start` on any machine."

// shutdownReplyWait bounds how long the stopping helper waits for its
// reply to be sent before it stops anyway (the reply then stays queued and
// is sent after the next start).
var shutdownReplyWait = 30 * time.Second

func (e *Engine) cmdShutdown(ctx context.Context, j cmdJob) {
	rec := store.StopRecord{Source: store.StopByTeams, Host: e.cfg.Host, At: e.now()}
	if rec.Host == "" {
		rec.Host = e.cfg.Machine
	}
	if _, err := store.WriteStop(filepath.Dir(e.st.Path()), rec); err != nil {
		e.logf("shutdown: %v", err)
		e.reply(j, "Shutdown failed: the stop could not be recorded ("+userError(err.Error())+"). The helper keeps running; run `cxp teams stop` on a machine that shares its state.", nil)
		return
	}
	// The reply's parts are keyed cmd:<inbound>:<part> (reply, render).
	prefix := fmt.Sprintf("cmd:%d:", j.inbound)
	e.mu.Lock()
	again := e.stopReply != ""
	if !again {
		e.stopReply = prefix
	}
	e.mu.Unlock()
	if again {
		// A second `shutdown` while the first one stops the helper: the
		// first reply answers both.
		e.complete(j, func(*store.Tx) error { return nil })
		return
	}
	e.logf("shutdown requested: stopped on all machines")
	e.stopping.Store(true)
	e.reply(j, textShutdown, nil)
	deadline := e.now().Add(shutdownReplyWait)
	for e.now().Before(deadline) && e.pendingKeys(j.conv, prefix) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.t.Tick):
		}
	}
	e.fail(store.ErrDisabled)
}

// shutdownCommand reports whether a command text is `shutdown` (as
// urgentCommand reads it).
func shutdownCommand(text string) bool {
	f := strings.Fields(strings.ToLower(text))
	if len(f) > 0 && f[0] == "helper" {
		f = f[1:]
	}
	return len(f) > 0 && f[0] == "shutdown"
}

// shutdownThrough is the seq of the shutdown reply's last part in the
// control chat ctl (0 when none is pending): while the helper stops, the
// control chat is sent up to it, in order, and nothing after it.
func shutdownThrough(tx *store.Tx, ctl int64, prefix string) (int64, error) {
	rows, err := tx.PendingOutbox(ctl)
	if err != nil {
		return 0, err
	}
	var through int64
	for _, r := range rows {
		if strings.HasPrefix(r.DeliveryKey, prefix) && r.Seq > through {
			through = r.Seq
		}
	}
	return through, nil
}

// pendingKeys reports whether an outbox row of conv whose delivery key
// starts with prefix is not sent yet (true on a store error: keep waiting).
func (e *Engine) pendingKeys(conv int64, prefix string) bool {
	pending := true
	_ = e.update(func(tx *store.Tx) error {
		rows, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		pending = false
		for _, r := range rows {
			if strings.HasPrefix(r.DeliveryKey, prefix) {
				pending = true
			}
		}
		return nil
	})
	return pending
}
