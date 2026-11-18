package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

const waitNoticeThreshold = time.Minute
const quietWaitReplayLimit = 4096

func (e *Engine) nativeWait(conv, turn int64, rs *runState, id string, duration time.Duration, started bool) {
	if id == "" || duration <= 0 || duration > 12*time.Hour {
		return
	}
	rs.silenceMu.Lock()
	defer rs.silenceMu.Unlock()
	e.mu.Lock()
	if cs := e.convs[conv]; cs == nil || cs.running != rs || rs.turnID != turn || rs.cancelled || rs.switching {
		e.mu.Unlock()
		return
	}
	if !started {
		ended := rs.waitID == id && !rs.waitUntil.IsZero()
		if rs.waitID == "" {
			// A completion can arrive before its start response. Remember it
			// without inventing a start or an end message.
			rs.waitID = id
		}
		announced := rs.waitAnnounced
		if ended {
			rs.waitUntil = time.Time{}
			rs.lastEvent.Store(ms(e.now()))
		}
		e.mu.Unlock()
		if ended && announced {
			e.waitNotice(conv, fmt.Sprintf("turn:%d:wait:%s:end", turn, id), "🕒 Wait ended", "Continuing the task.")
		}
		return
	}
	if _, seen := rs.quietWaitIDs[id]; rs.waitID == id || seen {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	showNotice := duration > waitNoticeThreshold
	announced := false
	if showNotice {
		// The outbox deduplicates visible waits even after a newer wait starts.
		added, err := e.waitNotice(conv, fmt.Sprintf("turn:%d:wait:%s:start", turn, id), "🕒 Wait started", fmt.Sprintf("Waiting for up to %s. New input can end the wait early.", duration))
		if err == nil && !added {
			return
		}
		announced = err == nil && added
	}
	e.mu.Lock()
	if cs := e.convs[conv]; cs != nil && cs.running == rs && !rs.cancelled && !rs.switching {
		if !showNotice {
			// Quiet waits have no outbox key. Remember them for this run so a
			// replay cannot replace a newer wait's deadline or end notice.
			if rs.quietWaitIDs == nil {
				rs.quietWaitIDs = make(map[string]struct{})
			}
			if len(rs.quietWaitOrder) == quietWaitReplayLimit {
				delete(rs.quietWaitIDs, rs.quietWaitOrder[rs.quietWaitNext])
				rs.quietWaitOrder[rs.quietWaitNext] = id
				rs.quietWaitNext = (rs.quietWaitNext + 1) % quietWaitReplayLimit
			} else {
				rs.quietWaitOrder = append(rs.quietWaitOrder, id)
			}
			rs.quietWaitIDs[id] = struct{}{}
		}
		now := e.now()
		rs.waitID, rs.waitUntil = id, now.Add(duration)
		rs.waitAnnounced = announced
		rs.lastEvent.Store(ms(now))
	}
	e.mu.Unlock()
}

// Called under the per-run silence lock so wait events retain their order.
// Enqueue's indexed delivery key deduplicates all phases, including replay.
func (e *Engine) waitNotice(conv int64, key, label, text string) (bool, error) {
	if conv == e.inboxID {
		return true, nil // the inbox carries protocol records only
	}
	msgs, err := e.render(context.Background(), channel.RichDoc{Label: label, Markdown: text}, store.OutNotice, key)
	if err != nil {
		e.logf("wait event in %d: %v", conv, err)
		return false, err
	}
	added := 0
	err = e.update(func(tx *store.Tx) error {
		var err error
		added, err = tx.Enqueue(conv, msgs)
		return err
	})
	if err != nil {
		e.logf("wait event in %d: %v", conv, err)
		return false, err
	}
	if added > 0 {
		e.wakeSender()
	}
	return added > 0, nil
}

// scheduledDoc presents the native scheduled input as a single execution
// event. Later messages retain their normal labels and completion footer.
// Reuse the input's delivery identity and checkpoint for replay deduplication.
func scheduledDoc(doc channel.RichDoc, o transcript.Output) channel.RichDoc {
	if o.Schedule == nil || !(o.User || o.Kind == transcript.OutputPrompt) {
		return doc
	}
	doc.Label = "🕒 Scheduled task started"
	if o.Schedule.ID != "" {
		doc.Label += " (" + o.Schedule.ID + ")"
		doc.Markdown = o.Schedule.Instructions
		if strings.TrimSpace(doc.Markdown) == "" {
			doc.Markdown = "_No instructions were included in this scheduled input._"
		}
	}
	return doc
}
