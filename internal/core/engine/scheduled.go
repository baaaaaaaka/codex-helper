package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

const scheduledResultHint = "Open the work chat for this run's result. This does not confirm that the schedule has ended."

func resultNotice(kind string) (string, string) {
	switch kind {
	case notifyScheduled:
		return "🕒 Scheduled task result", scheduledResultHint
	case notifyWait:
		return "🕒 In-session wait result", "Open the work chat for this run's result. An in-session wait does not confirm a saved schedule."
	default:
		return "✅ Codex finished", "The answer is ready in the work chat."
	}
}

func (e *Engine) nativeWait(conv, turn int64, rs *runState, id string, duration time.Duration, started bool) {
	if id == "" || duration <= 0 || duration > 12*time.Hour {
		return
	}
	e.mu.Lock()
	if cs := e.convs[conv]; cs == nil || cs.running != rs || rs.turnID != turn || rs.cancelled || rs.switching {
		e.mu.Unlock()
		return
	}
	if !started {
		if rs.waitID == id && !rs.waitUntil.IsZero() {
			rs.waitUntil = time.Time{}
			rs.lastEvent.Store(ms(e.now()))
		}
		e.mu.Unlock()
		return
	}
	if rs.waitID == id {
		e.mu.Unlock()
		return
	}
	first := rs.waitID == ""
	// Keep the last ID after completion so replay cannot restart that wait.
	rs.waitID, rs.waitUntil = id, e.now().Add(duration)
	rs.lastEvent.Store(ms(e.now()))
	e.mu.Unlock()
	if first {
		e.silenceNotice(conv, fmt.Sprintf("turn:%d:wait", turn), fmt.Sprintf("Codex entered an in-session wait (up to %s for this wait). New input can end a wait early. Further waits in this run will be quiet. This does not confirm a saved schedule.", duration))
	}
}

// scheduledDoc only changes messages carrying native scheduling evidence.
// Delivery identities, ordinary labels and notification defaults stay intact.
func scheduledDoc(doc channel.RichDoc, o transcript.Output) channel.RichDoc {
	if o.Schedule == nil {
		return doc
	}
	doc.Label = scheduledLabel(o)
	if o.Schedule.Input && o.Schedule.ID != "" {
		doc.Markdown = o.Schedule.Instructions
		if strings.TrimSpace(doc.Markdown) == "" {
			doc.Markdown = "_No instructions were included in this scheduled input._"
		}
	}
	doc.FooterLabel, doc.Footer = "", ""
	return doc
}

func scheduledLabel(o transcript.Output) string {
	label := "🕒 Scheduled task result"
	if o.Kind == transcript.OutputStatus || o.Kind == transcript.OutputCompact {
		label = "🕒 Scheduled task status"
	}
	if o.Schedule.Mixed {
		label += " with additional input"
	}
	if o.User || o.Kind == transcript.OutputPrompt {
		label = "🕒 Additional input during scheduled task"
		if o.Schedule.Input {
			label = "🕒 Scheduled task instructions"
		}
	}
	if o.Schedule.ID != "" {
		label += " (" + o.Schedule.ID + ")"
	}
	return label
}
