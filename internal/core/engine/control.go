package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// ensureControl makes sure the control conversation exists. Its chat is
// created by createChat (idempotent per key) on the sender's first pass,
// ahead of every other write, or later when the channel is unavailable at
// start.
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

// resumeCreating asks the sender to create the chats of conversations
// still in creating state (control first); maintenance calls it again for
// failures. Every chat creation is a Graph write: the sender makes it under
// the account's write pacing, ranked like a send (dueChats).
func (e *Engine) resumeCreating() {
	e.mu.Lock()
	var ids []int64
	for id, cs := range e.convs {
		if cs.state == store.ConvCreating {
			ids = append(ids, id)
		}
	}
	e.mu.Unlock()
	for _, id := range ids {
		if v, _, err := e.st.Meta(discoveredKey(id)); err != nil || v == discoveredPending {
			continue // a discovered session with nothing to mirror yet: no chat
		}
		// Nobody waits for these chats (a retry, or chats left from an
		// earlier process): they go with the background class, after
		// the replies.
		e.requestChat(id, sendMirror)
	}
}

// requestChat asks the sender to create the chat of a creating
// conversation, ranked in send class class (sendMirror: nobody waits for
// it; the control chat always goes first, and the chats the owner asks for
// are created by createChatNow). A request stays until the chat exists or
// the conversation leaves the creating state.
func (e *Engine) requestChat(id int64, class int) {
	e.mu.Lock()
	if e.chatWant == nil {
		e.chatWant = map[int64]int{}
	}
	if c, ok := e.chatWant[id]; !ok || class < c {
		e.chatWant[id] = class
	}
	e.mu.Unlock()
	e.wakeSender()
}

// createChatNow creates the chat the owner asked for (`new`, fork) in the
// command, as the old helper did, so its notices keep their order with the
// command's other replies; it waits for a write token first, ahead of the
// sender. A failure is retried in the background (retryCreating).
func (e *Engine) createChatNow(ctx context.Context, id int64) {
	e.mu.Lock()
	cs := e.convs[id]
	if cs == nil || cs.sending {
		e.mu.Unlock()
		return // the sender is creating it
	}
	cs.sending = true // the sender leaves it alone (dueChats)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		cs.sending = false
		e.mu.Unlock()
		e.wakeSender()
	}()
	if err := e.waitWrite(ctx); err != nil {
		return
	}
	e.createChat(ctx, id)
}

// createChat creates the chat of a creating conversation and announces it.
func (e *Engine) createChat(ctx context.Context, id int64) bool {
	cs := e.conv(id)
	if cs == nil {
		return false
	}
	e.mu.Lock()
	title, kind, label, state, cwd := cs.title, cs.kind, cs.label, cs.state, cs.cwd
	retry := e.now().Before(cs.nextRead) && !cs.nextRead.IsZero() && cs.failures > 0
	intro := e.intro[id]
	e.mu.Unlock()
	if state != store.ConvCreating {
		e.mu.Lock()
		delete(e.chatWant, id)
		e.mu.Unlock()
		return false
	}
	if retry {
		return false
	}
	if v, _, err := e.st.Meta(discoveredKey(id)); err != nil || v == discoveredPending {
		return false // a discovered session with nothing to mirror yet: no chat
	}
	key := fmt.Sprintf("conv-%s-%d", e.prefix, id)
	if kind == store.KindControl {
		key = "control-" + e.prefix
	}
	inbox := id == e.inboxID
	if inbox && e.cfg.InboxKey != "" {
		key = e.cfg.InboxKey
	}
	started := e.now()
	info, err := e.ch.CreateChat(ctx, title, key)
	if err != nil {
		now := e.now()
		next := now.Add(min(e.t.Hot<<min(cs.failures+1, 8), e.t.MaxBackoff))
		if th, ok := channel.AsThrottled(err); ok {
			// A chat creation spends the account's write budget like a
			// send: an account throttle slows every write.
			next = now.Add(e.retryAfter(th))
			if th.Scope == channel.ScopeAccount || th.Scope == channel.ScopeUnknown {
				e.setGate(e.gWrite, next, "create chat throttled")
				e.paceThrottled(started, next)
			}
		}
		e.mu.Lock()
		cs.failures++
		cs.nextRead = next
		e.mu.Unlock()
		e.logf("create chat for %s: %v", label, err)
		return false
	}
	err = e.update(func(tx *store.Tx) error {
		if err := tx.SetChat(id, info.ID, info.URL); err != nil {
			return err
		}
		discovered, _, err := tx.Meta(discoveredKey(id))
		if err != nil {
			return err
		}
		switch {
		case inbox:
			// Nothing is announced in or about the inbox.
		case kind == store.KindControl:
			if err := e.enqueueNotice(tx, id, "ready:control", "Codex control chat is ready on "+e.machine()+". Send `new <directory>` to start a session, or `help`."); err != nil {
				return err
			}
		case discovered != "":
			// A local session found on disk: its one notice in its work
			// chat is queued with its first records (mirrorRead); nothing
			// goes to the control chat.
			if err := tx.SetMeta(discoveredKey(id), ""); err != nil {
				return err
			}
		case intro != "":
			if err := e.enqueueNotice(tx, id, "ready:"+label, intro); err != nil {
				return err
			}
			if err := e.enqueueControlCreated(tx, id, label, info.URL); err != nil {
				return err
			}
		default:
			// `new`: v0.1.20's chat-created notice, then its ready anchor.
			if err := e.enqueueNotice(tx, id, "ready:"+label, textWorkChatCreated(label, cwd)); err != nil {
				return err
			}
			if err := e.enqueueNotice(tx, id, "anchor:"+label, textWorkChatReady(label, cwd)); err != nil {
				return err
			}
			if err := e.enqueueControlCreated(tx, id, label, info.URL); err != nil {
				return err
			}
		}
		_, err = e.refresh(tx, id)
		return err
	})
	if err != nil {
		e.logf("record chat of %s: %v", label, err)
		return false
	}
	e.mu.Lock()
	delete(e.chatWant, id)
	cs.failures = 0
	cs.nextRead = e.now()
	if inbox {
		cs.nextRead = e.now().Add(e.t.SafetyRead)
	}
	cs.lastActivity = e.now()
	delete(e.intro, id)
	e.mu.Unlock()
	if inbox {
		wake(e.registryWake) // the card can be published now
	}
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
func (e *Engine) retryCreating() {
	e.mu.Lock()
	n := 0
	for _, cs := range e.convs {
		if cs.state == store.ConvCreating {
			n++
		}
	}
	e.mu.Unlock()
	if n > 0 {
		e.resumeCreating()
	}
}

func discoveredKey(conv int64) string { return fmt.Sprintf("%s%d", metaDiscoveredPrefix, conv) }

// textLocalDetected is v0.1.20's notice in the work chat of a local
// session found on disk.
func textLocalDetected(label string) string {
	return "Local Codex chat detected: " + label + "."
}

// textWorkChatCreated is v0.1.20's first notice in a work chat made by
// `new`.
func textWorkChatCreated(label, cwd string) string {
	text := "Work chat created: " + label + "."
	if cwd = strings.TrimSpace(cwd); cwd != "" {
		text += "\nWorking directory: " + cwd
	}
	return text
}

// textWorkChatReady is v0.1.20's ready anchor that follows it.
func textWorkChatReady(label, cwd string) string {
	lines := []string{
		"💬 Work chat is ready.",
		"Send a task in this chat. Codex will start automatically and continue this session.",
		"Status: waiting for your first task.",
		"Session: " + label,
		"Project: " + projectLabel(cwd),
	}
	if cwd = strings.TrimSpace(cwd); cwd != "" {
		lines = append(lines, "Working directory: "+cwd)
	}
	lines = append(lines,
		"Commands: `helper status` or `!status`, `helper stats`, `helper help`, `helper close` to close this Codex session in Teams.",
		"Need more details? Send `helper status`.")
	return strings.Join(lines, "\n")
}

// projectLabel is v0.1.20's project name of a working directory: its last
// element made safe for a title, at most 64 characters.
func projectLabel(cwd string) string {
	p := strings.TrimRight(strings.ReplaceAll(strings.TrimSpace(cwd), "\\", "/"), "/")
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		p = p[i+1:]
	}
	if p == "" && strings.TrimSpace(cwd) != "" {
		p = "root"
	}
	p = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsControl(r):
			return -1
		case strings.ContainsRune(`:<>"/\|?*`, r):
			return '-'
		}
		return r
	}, p)
	p = strings.Trim(strings.Join(strings.Fields(p), " "), " -.")
	if r := []rune(p); len(r) > 64 {
		p = strings.TrimSpace(string(r[:61])) + "..."
	}
	if p == "" || p == "untitled" {
		return "helper default directory"
	}
	return p
}

// enqueueControlCreated is v0.1.20's control-chat answer to `new` (and a
// fork): where the new work chat is.
func (e *Engine) enqueueControlCreated(tx *store.Tx, id int64, label, url string) error {
	if e.controlID == 0 {
		return nil
	}
	return e.enqueueNotice(tx, e.controlID, fmt.Sprintf("created:%d", id),
		"✅ Work chat created: "+label+"\n\nOpen this Teams link and send your task there:\n"+url+
			"\n\nIf Teams does not show it right away, search for: "+label)
}
