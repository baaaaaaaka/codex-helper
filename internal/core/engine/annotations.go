package engine

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// In-place annotations of v0.1.20 (edits.go makes them):
//
//   - the user label "🧑‍💻 User:" on each message of the owner the helper
//     acts on, in the control chat and in work chats (F-RND-4 / R-8;
//     annotateIncomingUserMessage). Not in chats with other people, not on
//     voice messages (they get their transcript), never on mirrored or
//     caught-up records (those are the helper's own posts), never
//     re-posted when the edit is impossible.
//   - the transcript of a voice message, under it (F-RND-11;
//     annotateIncomingUserMessageWithASRTranscripts).
//   - the freeze notice of a chat parked after ParkAfter idle, appended to
//     its newest message, or posted when that message cannot be edited;
//     once per park (F-WRK-5 / R-22; parkIdleWorkChat).

// asrLabel heads a voice message's transcripts (v0.1.20
// teamsASRTranscriptAnnotationLabel).
const asrLabel = "🎙️ Speech-to-text transcript (automatic; may contain recognition errors)"

// labelAdmitted queues the user label of an admitted message the helper
// acts on. Called in the admission transaction.
func (e *Engine) labelAdmitted(conv int64, group bool, a store.Admitted, c *classified) {
	if c == nil || group || c.chatter || c.delegation != "" || c.in.Sender != store.SenderOwner ||
		conv == e.inboxID || hasVoice(c.msg.Files) {
		return
	}
	// A message refused for a raw API key is not labelled: the label edit
	// would send the pasted key back to Graph and add one more copy of it
	// to the message's edit history. (v0.1.20 labelled it; R-26 wins.)
	// Other refusals (attachment limits) keep v0.1.20's label.
	if c.reject == textRawAPIKey {
		return
	}
	// `shutdown` stops the process right after its reply; its label would
	// be left to the bounded flush at the stop (edits.go flushEdits), so
	// whether it went out would depend on timing.
	// It is never labelled (shutdown.go), in any of its forms (`shutdown`,
	// `!shutdown`, `/shutdown`, `helper shutdown`; parse.go).
	if a.Kind == store.InCommand && c.control && c.cmd.form == formCommand && c.cmd.name == cmdShutdown {
		return
	}
	acted := a.Kind == store.InCommand && a.State == store.InQueued ||
		a.Kind == store.InPrompt && (a.State == store.InHeld || a.TurnID != 0)
	if !acted {
		return
	}
	ann, ok := e.annotator()
	if !ok {
		return
	}
	id := a.ExtMsgID
	e.queueEdit(conv, chatEdit{key: "label:" + id, what: "user label", run: func(ctx context.Context, chat string) error {
		if err := e.waitRead(ctx, editReadWait); err != nil {
			return err
		}
		return ann.Annotate(ctx, chat, id, channel.Annotation{Label: labelUser})
	}})
}

// voiceClip is a transcribed voice attachment of an inbound message.
type voiceClip struct {
	name, path string
}

// labelVoice queues the transcript annotation of a voice message once
// every attachment of it is done. Called in the transaction that records
// the last one.
func (e *Engine) labelVoice(conv int64, in store.Inbound, clips []voiceClip) {
	if len(clips) == 0 || in.Sender != store.SenderOwner || in.ExtMsgID == "" || conv == e.inboxID {
		return
	}
	ann, ok := e.annotator()
	if !ok {
		return
	}
	var out []channel.Clip
	for _, c := range clips {
		text, err := os.ReadFile(c.path)
		if err != nil {
			continue
		}
		out = append(out, channel.Clip{Name: c.name, Text: strings.TrimSpace(string(text))})
	}
	if len(out) == 0 {
		return
	}
	id := in.ExtMsgID
	e.queueEdit(conv, chatEdit{key: "voice:" + id, what: "voice transcript", run: func(ctx context.Context, chat string) error {
		// A pending voice can finish before startup's first member check.
		// Defer through the existing edit queue, then use current state.
		e.mu.Lock()
		cs := e.convs[conv]
		ready := cs != nil && cs.readOnce
		group := cs == nil || cs.group
		e.mu.Unlock()
		if !ready {
			return errEditLater
		}
		if group {
			return nil
		}
		if err := e.waitRead(ctx, editReadWait); err != nil {
			return err
		}
		return ann.Annotate(ctx, chat, id, channel.Annotation{Label: labelUser, ClipsLabel: asrLabel, Clips: out})
	}})
}

// metaFreezePrefix + conversation id: "<parked_at>" while the freeze
// notice of that park is owed, "done:<parked_at>" once made.
const metaFreezePrefix = "engine_freeze:"

func freezeKey(conv int64) string { return metaFreezePrefix + strconv.FormatInt(conv, 10) }

// owesFreeze records in tx that the park of conv at parkedAt owes its
// freeze notice.
func owesFreeze(tx *store.Tx, conv, parkedAt int64) error {
	return tx.SetMeta(freezeKey(conv), strconv.FormatInt(parkedAt, 10))
}

// resumeFreezes queues the freeze notices owed by parked chats (a restart
// between the park and its notice).
func (e *Engine) resumeFreezes() {
	convs, err := e.st.Conversations()
	if err != nil {
		return
	}
	for _, c := range convs {
		if c.State != store.ConvParked {
			continue
		}
		if v, ok, _ := e.st.Meta(freezeKey(c.ID)); ok && v == strconv.FormatInt(c.ParkedAt, 10) {
			e.queueFreeze(c.ID, c.ParkedAt)
		}
	}
}

// freezeText is the freeze notice (v0.1.20 renderTeamsFreezeNoticeHTML),
// its warning adapted to the owner's rule that messages sent while parked
// are read after the wake (2026-11-05 Q6).
func freezeText(label, controlURL string, idle time.Duration) string {
	open := "Step 1: Open Control chat"
	if controlURL != "" {
		open = "Step 1: Open [Control chat](" + controlURL + ")"
	}
	return "🧊 This chat is paused\n" +
		"⚠ **Messages sent here while it is paused are read and run once it wakes.**\n" +
		"Your Codex work is safe. Paused after " + idleText(idle) + " idle.\n\n" +
		"▶️ **Continue chat:**\n" + open + "\nStep 2: Send: `r " + label + "`"
}

func idleText(d time.Duration) string {
	if d >= time.Hour && d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	}
	return d.String()
}

// queueFreeze queues the freeze notice of the park of conv at parkedAt.
func (e *Engine) queueFreeze(conv, parkedAt int64) {
	e.queueEdit(conv, chatEdit{key: fmt.Sprintf("freeze:%d", parkedAt), what: "freeze notice", run: func(ctx context.Context, chat string) error {
		return e.freeze(ctx, conv, parkedAt, chat)
	}})
	e.wakeSender()
}

// freeze makes the freeze notice: appended to the chat's newest message
// when that is the helper's post or an owner message it admitted and the
// edit is lossless, else posted as a notice of its own.
func (e *Engine) freeze(ctx context.Context, conv, parkedAt int64, chat string) error {
	e.mu.Lock()
	cs, ctl := e.convs[conv], e.convs[e.controlID]
	var label, url string
	parked := cs != nil && cs.state == store.ConvParked
	if cs != nil {
		label = cs.label
	}
	if ctl != nil {
		url = ctl.url
	}
	e.mu.Unlock()
	if !parked {
		return nil // woken before the notice: nothing to say
	}
	if v, _, err := e.st.Meta(freezeKey(conv)); err != nil {
		return err
	} else if v != strconv.FormatInt(parkedAt, 10) {
		return nil // made, or a later park owes its own
	}
	text := freezeText(label, url, e.t.ParkAfter)
	ann, annOK := e.annotator()
	pager, pagerOK := e.cfg.Channel.(channel.HistoryPager)
	appended := false
	if annOK && pagerOK {
		if err := e.waitRead(ctx, time.Minute); err != nil {
			return err
		}
		msgs, _, err := pager.HistoryPage(ctx, chat, time.Time{}, "")
		if err != nil {
			return err
		}
		var newest *channel.InboundMsg
		for i := range msgs {
			m := &msgs[i]
			if m.Kind != channel.KindMessage || strings.TrimSpace(m.Text) == "" && len(m.Files) == 0 {
				continue
			}
			if newest == nil || m.Created.After(newest.Created) || m.Created.Equal(newest.Created) && m.ID > newest.ID {
				newest = m
			}
		}
		if newest != nil && newest.ByOwner {
			known := false
			err := e.update(func(tx *store.Tx) error {
				own, err := tx.IsOwn(conv, newest.ID, nil, 0)
				if err != nil || own {
					known = own
					return err
				}
				known, err = tx.HasInbound(conv, newest.ID)
				return err
			})
			if err != nil {
				return err
			}
			if known {
				// The edit reads the message again: its read token.
				if err := e.waitRead(ctx, time.Minute); err != nil {
					return err
				}
				err := ann.Annotate(ctx, chat, newest.ID, channel.Annotation{
					Notice:  &channel.RichDoc{Label: labelHelper, Markdown: text},
					Present: []string{"This chat is paused", "r " + label},
				})
				switch {
				case err == nil:
					appended = true
				case channel.IsAmbiguous(err):
					return err // PATCH is idempotent: tried again
				default:
					if _, ok := channel.AsThrottled(err); ok {
						return err
					}
					e.logf("freeze notice of %s posted instead of appended: %v", label, err)
				}
			}
		}
	}
	err := e.update(func(tx *store.Tx) error {
		if !appended {
			if err := e.enqueueNotice(tx, conv, fmt.Sprintf("freeze:%d", parkedAt), text); err != nil {
				return err
			}
		}
		return tx.SetMeta(freezeKey(conv), "done:"+strconv.FormatInt(parkedAt, 10))
	})
	if err == nil && !appended {
		e.wakeSender()
	}
	return err
}
