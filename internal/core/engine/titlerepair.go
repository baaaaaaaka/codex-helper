package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Startup title repair (owner request 2026-11-07). Until rc.66 the core
// created work chats titled "<label> · <name>" (the bare label without a
// name), where v0.1.20 titled them "💬 <machine> - <title>"; only a chat
// that got a live answer was renamed afterwards, so the chats of sessions
// caught up at start kept the label title. New chats are created with
// v0.1.20's titles now; at every start the chats that still carry a label
// title are renamed once to the title they should have: the owner's title
// when there is one (`helper rename`, `new -- <title>`), else the
// automatic title (thread name, first prompt), else v0.1.20's placeholder
// "New message in <workspace>". The renames are chat edits made by the
// sender with the mirrored messages, under the account's write pacing.
// Chats with any other title (v0.1.20's, the owner's, migrated) are left
// alone.

// labelTitle reports whether title is a title the core gave a chat at
// creation until rc.66 ("<label> · <name>" or the bare label).
func labelTitle(title, label string) bool {
	title = strings.TrimSpace(title)
	return label != "" && (title == label || strings.HasPrefix(title, label+" · "))
}

// titleBody is a chat title without its label prefix ("<label> · ") or
// its machine prefix ("💬 <machine> - ").
func titleBody(title, label string, machines ...string) string {
	t := strings.TrimSpace(title)
	if rest, ok := strings.CutPrefix(t, label+" · "); ok && label != "" {
		t = rest
	} else if t == label {
		t = ""
	}
	return stripMachinePrefix(t, machines)
}

// repairTitles runs at start: chats not created yet get their v0.1.20
// title in the store (it is created with it); chats that exist get a
// rename queued.
func (e *Engine) repairTitles() {
	machine := e.machineLabel() // reads the store: not inside a transaction
	type target struct {
		id    int64
		label string
		chat  bool
	}
	var targets []target
	e.mu.Lock()
	for id, cs := range e.convs {
		if cs.kind != store.KindWork || id == e.inboxID || cs.state == store.ConvClosed || !labelTitle(cs.title, cs.label) {
			continue
		}
		targets = append(targets, target{id: id, label: cs.label, chat: cs.chat != ""})
	}
	e.mu.Unlock()
	queued, set := 0, 0
	for _, t := range targets {
		if t.chat {
			id := t.id
			e.queueEdit(id, chatEdit{key: "title-repair", what: "title repair", class: sendMirror,
				run: func(ctx context.Context, chat string) error { return e.repairTitle(ctx, id, chat) }})
			queued++
			continue
		}
		// Not created yet: created with the repaired title.
		f, err := e.loadTitleFacts(t.id)
		if err != nil || !f.found {
			continue
		}
		want, source, user := e.repairedTitle(f, machine)
		if err := e.update(func(tx *store.Tx) error {
			c, ok, err := tx.Conversation(t.id)
			if err != nil || !ok || c.State != store.ConvCreating || c.ExtChatID != "" || c.Title != f.c.Title {
				return err
			}
			if err := tx.SetTitle(t.id, want, source); err != nil {
				return err
			}
			if user != "" {
				if err := tx.SetMeta(fmt.Sprint(metaUserTitle, t.id), user); err != nil {
					return err
				}
			}
			set++
			_, err = e.refresh(tx, t.id)
			return err
		}); err != nil {
			e.logf("title repair of %s: %v", t.label, err)
		}
	}
	if queued+set > 0 {
		e.logf("title repair: %d chats renamed to v0.1.20 titles, %d chats not created yet retitled", queued, set)
		e.wakeSender()
	}
}

// repairedTitle is the title a chat with a label title should have, its
// title source, and the owner's title to record ("" when none is new).
func (e *Engine) repairedTitle(f titleFacts, machine string) (title, source, newUser string) {
	c := f.c
	user := strings.TrimSpace(f.user)
	if user == "" && strings.EqualFold(strings.TrimSpace(c.TitleSource), "user") {
		// `new <dir> -- <title>` before rc.67: the owner's title is the
		// name after the label.
		user = sanitizeTitle(titleBody(c.Title, c.Label))
		newUser = user
	}
	cwd := f.th.Cwd
	if user != "" {
		return workTitle(machine, user, "", cwd), "user", newUser
	}
	name := e.autoName(f)
	if name == "" {
		body := titleBody(c.Title, c.Label)
		switch {
		case strings.HasPrefix(body, "fork of "):
			name = body
		case f.path != "" || f.first != "":
			name = body // a session whose prompt has no title text
		default:
			name = placeholderTitle(cwd)
		}
	}
	return workTitle(machine, "", name, cwd), "auto", ""
}

// repairTitle renames one chat with a label title (a chat edit: the sender
// took its write token). A chat renamed meanwhile is left alone.
func (e *Engine) repairTitle(ctx context.Context, conv int64, chat string) error {
	e.titles.run.Lock()
	defer e.titles.run.Unlock()
	f, err := e.loadTitleFacts(conv)
	if err != nil || !f.found {
		return err
	}
	if f.c.State == store.ConvClosed || !labelTitle(f.c.Title, f.c.Label) {
		return nil
	}
	want, source, user := e.repairedTitle(f, e.machineLabel())
	if err := e.renameOnce(ctx, chat, want); err != nil {
		return err
	}
	return e.update(func(tx *store.Tx) error {
		if err := tx.SetTitle(conv, want, source); err != nil {
			return err
		}
		if user != "" {
			if err := tx.SetMeta(fmt.Sprint(metaUserTitle, conv), user); err != nil {
				return err
			}
		}
		_, err := e.refresh(tx, conv)
		return err
	})
}
