package engine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// ensureControl makes sure the control conversation exists. Its chat is
// created by createChat (idempotent per key), here or later by
// maintenance when the channel is unavailable at start.
func (e *Engine) ensureControl(ctx context.Context) error {
	e.mu.Lock()
	id := e.controlID
	e.mu.Unlock()
	if id == 0 {
		err := e.update(func(tx *store.Tx) error {
			var err error
			id, err = tx.CreateConversation(store.NewConversation{Label: "control", Kind: store.KindControl,
				Channel: e.cfg.ChannelName, Title: e.controlTitle(), TitleSource: "auto"})
			if err != nil {
				return err
			}
			_, err = e.refresh(tx, id)
			return err
		})
		if err != nil {
			return fmt.Errorf("engine: create control conversation: %w", err)
		}
		e.mu.Lock()
		e.controlID = id
		e.mu.Unlock()
	}
	return nil
}

// ensureInbox makes sure the machine's delegation inbox conversation exists
// when the registry is on (F-XM-2). The inbox is a work-kind conversation
// without a thread: it is never polled on its own (the activity scan covers
// it), never parked, never listed, and nothing is announced in it.
func (e *Engine) ensureInbox() error {
	if !e.cfg.Registry {
		return nil
	}
	var id int64
	err := e.update(func(tx *store.Tx) error {
		v, ok, err := tx.Meta(metaInbox)
		if err != nil {
			return err
		}
		if ok && v != "" {
			if n, perr := strconv.ParseInt(v, 10, 64); perr == nil {
				if c, found, err := tx.Conversation(n); err != nil {
					return err
				} else if found && c.State != store.ConvClosed {
					id = n
					return nil
				}
			}
		}
		id, err = tx.CreateConversation(store.NewConversation{Label: "inbox", Kind: store.KindWork,
			Channel: e.cfg.ChannelName, Title: e.inboxTitle(), TitleSource: "auto"})
		if err != nil {
			return err
		}
		if err := tx.SetMeta(metaInbox, strconv.FormatInt(id, 10)); err != nil {
			return err
		}
		_, err = e.refresh(tx, id)
		return err
	})
	if err != nil {
		return fmt.Errorf("engine: create delegation inbox: %w", err)
	}
	e.mu.Lock()
	e.inboxID = id
	if cs := e.convs[id]; cs != nil {
		cs.nextRead = e.now().Add(e.t.SafetyRead) // covered by the activity scan
	}
	e.mu.Unlock()
	return nil
}

func (e *Engine) inboxTitle() string {
	if e.cfg.Machine == "" {
		return "CXP Machine Inbox"
	}
	return "CXP Machine Inbox " + e.cfg.Machine
}

func (e *Engine) controlTitle() string {
	if e.cfg.Machine == "" {
		return "Codex Control"
	}
	return "Codex Control - " + e.cfg.Machine
}

// resumeCreating creates the chats of conversations still in creating
// state (control first). Failures are retried by maintenance.
func (e *Engine) resumeCreating(ctx context.Context) {
	e.mu.Lock()
	var ids []int64
	if cs := e.convs[e.controlID]; cs != nil && cs.state == store.ConvCreating {
		ids = append(ids, cs.id)
	}
	for id, cs := range e.convs {
		if cs.state == store.ConvCreating && id != e.controlID {
			ids = append(ids, id)
		}
	}
	e.mu.Unlock()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		e.createChat(ctx, id)
	}
}

// createChat creates the chat of a creating conversation and announces it.
func (e *Engine) createChat(ctx context.Context, id int64) bool {
	cs := e.conv(id)
	if cs == nil {
		return false
	}
	e.mu.Lock()
	title, kind, label, state := cs.title, cs.kind, cs.label, cs.state
	retry := e.now().Before(cs.nextRead) && !cs.nextRead.IsZero() && cs.failures > 0
	intro := e.intro[id]
	e.mu.Unlock()
	if state != store.ConvCreating || retry {
		return false
	}
	key := fmt.Sprintf("conv-%s-%d", e.prefix, id)
	if kind == store.KindControl {
		key = "control-" + e.prefix
	}
	inbox := id == e.inboxID
	if inbox && e.cfg.InboxKey != "" {
		key = e.cfg.InboxKey
	}
	info, err := e.ch.CreateChat(ctx, title, key)
	if err != nil {
		e.mu.Lock()
		cs.failures++
		cs.nextRead = e.now().Add(min(e.t.Hot<<min(cs.failures, 8), e.t.MaxBackoff))
		e.mu.Unlock()
		e.logf("create chat for %s: %v", label, err)
		return false
	}
	err = e.update(func(tx *store.Tx) error {
		if err := tx.SetChat(id, info.ID, info.URL); err != nil {
			return err
		}
		if inbox {
			// Nothing is announced in or about the inbox.
		} else if kind == store.KindControl {
			if err := e.enqueueNotice(tx, id, "ready:control", "Codex control chat is ready on "+e.machine()+". Send `new <directory>` to start a session, or `help`."); err != nil {
				return err
			}
		} else {
			text := "Work chat created: " + label + ". Work chat is ready: messages here go to Codex. Send `helper help` for commands."
			if intro != "" {
				text = intro
			}
			if err := e.enqueueNotice(tx, id, "ready:"+label, text); err != nil {
				return err
			}
			if e.controlID != 0 {
				if err := e.enqueueNotice(tx, e.controlID, fmt.Sprintf("created:%d", id),
					fmt.Sprintf("Work chat created: %s — %s. Open it: %s", label, title, info.URL)); err != nil {
					return err
				}
			}
		}
		_, err := e.refresh(tx, id)
		return err
	})
	if err != nil {
		e.logf("record chat of %s: %v", label, err)
		return false
	}
	e.mu.Lock()
	cs.failures = 0
	cs.nextRead = e.now()
	if inbox {
		cs.nextRead = e.now().Add(e.t.SafetyRead)
	}
	cs.lastActivity = e.now()
	delete(e.intro, id)
	e.mu.Unlock()
	e.wakeReader()
	e.wakeSender()
	return true
}

func (e *Engine) machine() string {
	if e.cfg.Machine == "" {
		return "this machine"
	}
	return e.cfg.Machine
}

// retryCreating is called by maintenance.
func (e *Engine) retryCreating(ctx context.Context) {
	e.mu.Lock()
	n := 0
	for _, cs := range e.convs {
		if cs.state == store.ConvCreating {
			n++
		}
	}
	e.mu.Unlock()
	if n > 0 {
		e.resumeCreating(ctx)
	}
}

var _ = time.Second
