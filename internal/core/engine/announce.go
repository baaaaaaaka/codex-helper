package engine

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// New chats of local sessions caught up at start (owner request
// 2026-11-07). Teams lists a new meeting chat only after the owner opens
// it, so a chat nobody announces stays out of sight. v0.1.20 announced
// every local session it found with one Workflow card (or control-chat
// @mention); rc.66 announced only sessions found live, and the 170
// sessions it caught up at its first start got chats nobody was told
// about.
//
// A session found live keeps its own notification (mirror.go, notifyLocal).
// The chats of sessions caught up at start are announced together in the
// control chat: a few summary posts with one link per chat, not one card
// per chat, and an @mention on the first post only. The same pass, at
// every start, announces the chats an earlier process created without
// announcing them. The mark engine_announced:<conv> is set in the
// transaction that queues a chat's announcement, so no chat is announced
// twice; chats from before the core (migrated) and chats the owner made
// (`new`, fork, `continue`) are never discovered sessions and are not
// announced here.

const (
	// metaAnnouncedPrefix + conversation id: the chat of a discovered
	// session was announced (its notification or a summary was queued).
	metaAnnouncedPrefix = "engine_announced:"
	// announceBatch is the number of chats in one summary post.
	announceBatch = 20
	// announceHold is how long a chat waits for the rest of the catch-up
	// to be created before its summary goes anyway.
	announceHold = 10 * time.Minute
)

func announcedKey(conv int64) string { return fmt.Sprintf("%s%d", metaAnnouncedPrefix, conv) }

// markAnnounced records in tx that conv's chat was announced.
func markAnnounced(tx *store.Tx, conv int64) error {
	return tx.SetMeta(announcedKey(conv), "1")
}

type announceItem struct {
	id               int64
	label, name, url string
}

// announcePass queues the control-chat summaries of the discovered
// sessions' chats nobody was told about. While catch-up chats are still
// being created it waits for a full post, or for announceHold.
func (e *Engine) announcePass() {
	if e.stopping.Load() {
		return
	}
	e.announceRun.Lock()
	defer e.announceRun.Unlock()
	var discovered, announced map[string]string
	if err := e.update(func(tx *store.Tx) error {
		var err error
		if discovered, err = tx.MetaPrefix(metaDiscoveredPrefix); err != nil {
			return err
		}
		announced, err = tx.MetaPrefix(metaAnnouncedPrefix)
		return err
	}); err != nil {
		e.logf("announce new chats: %v", err)
		return
	}
	label := e.machineLabel() // reads the store: not inside a transaction
	now := e.now()
	var items []announceItem
	waiting := 0
	e.mu.Lock()
	ctl := e.controlID
	for k, v := range discovered {
		id, err := strconv.ParseInt(strings.TrimPrefix(k, metaDiscoveredPrefix), 10, 64)
		if err != nil || v == discoveredPending || announced[announcedKey(id)] != "" {
			continue
		}
		cs := e.convs[id]
		if cs == nil || cs.kind != store.KindWork || id == e.inboxID || cs.state == store.ConvClosed {
			continue
		}
		if cs.chat == "" {
			if cs.state == store.ConvCreating {
				waiting++ // its chat is still to be created
			}
			continue
		}
		if e.announceSeen == nil {
			e.announceSeen = map[int64]time.Time{}
		}
		if _, ok := e.announceSeen[id]; !ok {
			e.announceSeen[id] = now
		}
		items = append(items, announceItem{id: id, label: cs.label, name: announceName(cs, label, e.cfg.Machine), url: cs.url})
	}
	held := false
	if waiting > 0 && len(items) < announceBatch {
		held = true
		for _, it := range items {
			if now.Sub(e.announceSeen[it.id]) >= announceHold {
				held = false
				break
			}
		}
	}
	e.mu.Unlock()
	if ctl == 0 || len(items) == 0 || held {
		return
	}
	sort.Slice(items, func(a, b int) bool { return items[a].id < items[b].id })
	batches := (len(items) + announceBatch - 1) / announceBatch
	if waiting > 0 && batches > 1 && len(items)%announceBatch != 0 {
		// More chats are on their way: keep the partial post for them.
		batches--
		items = items[:batches*announceBatch]
	}
	for b := 0; b < batches; b++ {
		part := items[b*announceBatch : min((b+1)*announceBatch, len(items))]
		msgs, err := e.render(context.Background(), channel.RichDoc{Label: labelHelper,
			Markdown: announceText(part, len(items), b, batches), Mention: b == 0},
			store.OutNotice, fmt.Sprintf("announce:%d", part[0].id))
		if err != nil {
			e.logf("announce new chats: %v", err)
			return
		}
		if err := e.update(func(tx *store.Tx) error {
			for _, it := range part {
				if v, _, err := tx.Meta(announcedKey(it.id)); err != nil || v != "" {
					return err // announced meanwhile: the next pass recounts
				}
			}
			if _, err := tx.Enqueue(ctl, msgs); err != nil {
				return err
			}
			for _, it := range part {
				if err := markAnnounced(tx, it.id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			e.logf("announce new chats: %v", err)
			return
		}
		e.mu.Lock()
		for _, it := range part {
			delete(e.announceSeen, it.id)
		}
		e.mu.Unlock()
	}
	e.logf("announced %d new chats of local sessions in the control chat", len(items))
	e.wakeSender()
}

// announceName is the chat's title without the machine prefix or the
// label prefix of the chats rc.66 created ("sNNN · name").
func announceName(cs *convState, labels ...string) string {
	t := titleBody(cs.title, cs.label, labels...)
	t = strings.NewReplacer("[", "(", "]", ")", "\n", " ").Replace(t)
	return shortLine(t, 80)
}

func announceText(items []announceItem, total, batch, batches int) string {
	var s strings.Builder
	head := fmt.Sprintf("**🆕 New local Codex chats (%d)**", total)
	if batches > 1 {
		head = fmt.Sprintf("**🆕 New local Codex chats (%d, part %d of %d)**", total, batch+1, batches)
	}
	s.WriteString(head)
	if batch == 0 {
		s.WriteString("\n\nThese local Codex sessions got Teams work chats while the helper was catching up. " +
			"Teams lists a new chat only after it is opened: open one here to watch it and continue from its full local context.")
	}
	s.WriteString("\n")
	for _, it := range items {
		name := it.name
		if name == "" {
			name = "Open chat"
		}
		if it.url != "" {
			fmt.Fprintf(&s, "\n- **%s** · [%s](%s)", it.label, name, it.url)
		} else {
			fmt.Fprintf(&s, "\n- **%s** · %s", it.label, name)
		}
	}
	return s.String()
}
