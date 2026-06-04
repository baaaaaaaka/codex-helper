package codexrunner

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"sync"
)

type EventHandler func(StreamEvent)

type StreamEventKind string

const (
	StreamEventThreadStarted    StreamEventKind = "thread_started"
	StreamEventTurnStarted      StreamEventKind = "turn_started"
	StreamEventTurnCompleted    StreamEventKind = "turn_completed"
	StreamEventTurnFailed       StreamEventKind = "turn_failed"
	StreamEventAgentMessage     StreamEventKind = "agent_message"
	StreamEventCommandStarted   StreamEventKind = "command_started"
	StreamEventCommandCompleted StreamEventKind = "command_completed"
	StreamEventContextCompacted StreamEventKind = "context_compacted"
	StreamEventStreamRetry      StreamEventKind = "stream_retry"
	StreamEventUsage            StreamEventKind = "usage"
)

type StreamEvent struct {
	Kind             StreamEventKind
	ThreadID         string
	TurnID           string
	ItemID           string
	ItemType         string
	Phase            string
	Text             string
	Command          string
	AggregatedOutput string
	Status           string
	ExitCode         *int
	Failure          *TurnFailure
	WillRetry        bool
	Usage            Usage
	Raw              []byte
}

func ParseStreamEventJSONL(line []byte) (StreamEvent, bool, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] != '{' {
		return StreamEvent{}, false, nil
	}
	var event codexEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return StreamEvent{}, false, err
	}
	return streamEventFromCodexEvent(event, line)
}

func streamEventFromCodexEvent(event codexEvent, raw []byte) (StreamEvent, bool, error) {
	return streamEventFromCodexEventWithOptions(event, raw, true)
}

func streamEventFromCodexEventWithOptions(event codexEvent, raw []byte, includeRaw bool) (StreamEvent, bool, error) {
	out := StreamEvent{
		ThreadID: firstNonEmpty(event.ThreadIDCamel, event.ThreadID),
		TurnID:   firstNonEmpty(event.TurnID, event.Turn.ID),
	}
	if includeRaw {
		out.Raw = append([]byte(nil), raw...)
	}
	switch event.Type {
	case "thread.started", "thread/started":
		out.Kind = StreamEventThreadStarted
	case "turn.started", "turn/started":
		out.Kind = StreamEventTurnStarted
	case "turn.completed", "turn/completed":
		out.Kind = StreamEventTurnCompleted
		out.Usage = usageFromEvent(event)
	case "turn.failed", "turn/failed":
		out.Kind = StreamEventTurnFailed
		out.Usage = usageFromEvent(event)
		out.Failure = failureFromEvent(event)
	case "context_compaction", "context_compacted", "thread.compacted", "thread/compacted", "compacted":
		out.Kind = StreamEventContextCompacted
	case "event_msg", "response_item":
		if !applyContextCompactPayload(event.Payload, &out) {
			if !applyTranscriptPayloadStreamEvent(event, &out) {
				return StreamEvent{}, false, nil
			}
		}
	case "error":
		out.Failure = failureFromEvent(event)
		out.WillRetry = event.WillRetry
		if event.WillRetry {
			out.Kind = StreamEventStreamRetry
		} else {
			out.Kind = StreamEventTurnFailed
		}
	case "item.started", "item/started", "item.completed", "item/completed":
		out.ItemID = event.Item.ID
		out.ItemType = event.Item.Type
		out.Phase = event.Item.Phase
		out.Text = agentMessageText(event.Item)
		out.Command = event.Item.Command
		out.AggregatedOutput = commandOutputText(event.Item)
		out.Status = event.Item.Status
		out.ExitCode = commandExitCode(event.Item)
		switch {
		case isAgentMessageItem(event.Item):
			if strings.TrimSpace(out.Text) == "" {
				return StreamEvent{}, false, nil
			}
			out.Kind = StreamEventAgentMessage
		case isCommandExecutionItem(event.Item):
			if event.Type == "item.started" || event.Type == "item/started" {
				out.Kind = StreamEventCommandStarted
			} else {
				out.Kind = StreamEventCommandCompleted
			}
		default:
			return StreamEvent{}, false, nil
		}
	default:
		usage := usageFromEvent(event)
		if usage != (Usage{}) {
			out.Kind = StreamEventUsage
			out.Usage = usage
		} else {
			return StreamEvent{}, false, nil
		}
	}
	return out, true, nil
}

func applyTranscriptPayloadStreamEvent(event codexEvent, out *StreamEvent) bool {
	payload, ok := parseTranscriptPayload(event.Payload)
	if !ok {
		return false
	}
	out.ThreadID = firstNonEmpty(out.ThreadID, payload.ThreadIDCamel, payload.ThreadID)
	out.TurnID = firstNonEmpty(out.TurnID, transcriptPayloadTurnID(payload))
	switch event.Type {
	case "event_msg":
		switch strings.ToLower(strings.TrimSpace(payload.Type)) {
		case "agent_message":
			// Stream both in-turn commentary and the final answer as live agent
			// messages: the forwarder holds the latest as pending and flushes the
			// previous as "progress", so intermediate commentary reaches Teams
			// while the turn runs. This restores the behavior the old
			// item.completed agent_message format had (no phase gate). Note this
			// intentionally diverges from parser.go, which stays final-only
			// because it computes the terminal TurnResult.
			if !isStreamableAgentPhase(payload.Phase) {
				return false
			}
			text := strings.TrimSpace(payload.Message)
			if text == "" {
				return false
			}
			out.Kind = StreamEventAgentMessage
			out.Phase = payload.Phase
			out.Text = text
			return true
		case "task_complete":
			out.Kind = StreamEventTurnCompleted
			out.Text = firstNonEmpty(payload.LastAgentMessage, payload.LastAgentMessageCamel)
			return true
		default:
			return false
		}
	case "response_item":
		if strings.ToLower(strings.TrimSpace(payload.Type)) != "message" ||
			strings.ToLower(strings.TrimSpace(payload.Role)) != "assistant" ||
			!isStreamableAgentPhase(payload.Phase) {
			return false
		}
		text := strings.TrimSpace(agentMessageText(codexItem{Content: payload.Content}))
		if text == "" {
			return false
		}
		out.Kind = StreamEventAgentMessage
		out.Phase = payload.Phase
		out.Text = text
		return true
	default:
		return false
	}
}

// isStreamableAgentPhase reports whether an agent message phase should be
// forwarded as a live agent message. Both commentary (in-turn progress) and
// final_answer qualify; an empty/unknown phase does not, which preserves the
// guard against treating stray assistant content as a turn message.
func isStreamableAgentPhase(phase string) bool {
	return isFinalAnswerPhase(phase) || isCommentaryPhase(phase)
}

func isCommentaryPhase(phase string) bool {
	return strings.EqualFold(strings.TrimSpace(phase), "commentary")
}

func applyContextCompactPayload(raw json.RawMessage, out *StreamEvent) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false
	}
	var payload struct {
		Type          string    `json:"type"`
		ThreadID      string    `json:"thread_id"`
		ThreadIDCamel string    `json:"threadId"`
		TurnID        string    `json:"turn_id"`
		TurnIDCamel   string    `json:"turnId"`
		Turn          codexTurn `json:"turn"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	if !isContextCompactStreamType(payload.Type) {
		return false
	}
	out.Kind = StreamEventContextCompacted
	out.ThreadID = firstNonEmpty(out.ThreadID, payload.ThreadIDCamel, payload.ThreadID)
	out.TurnID = firstNonEmpty(out.TurnID, payload.TurnIDCamel, payload.TurnID, payload.Turn.ID)
	return true
}

func isContextCompactStreamType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "context_compaction", "context_compacted", "thread.compacted", "thread/compacted", "compacted":
		return true
	default:
		return false
	}
}

func usageFromEvent(event codexEvent) Usage {
	var usage Usage
	mergeUsage(&usage, event.Usage)
	return usage
}

type EventStreamWriter struct {
	dst                  io.Writer
	handler              EventHandler
	includeCommandOutput bool
	includeRawEvent      bool
	mu                   sync.Mutex
	pending              []byte
	lineNo               int
	result               TurnResult
	err                  error
	done                 bool
}

func NewEventStreamWriter(dst io.Writer, handler EventHandler) io.Writer {
	if handler == nil {
		return dst
	}
	return NewEventStreamCollector(dst, handler)
}

type EventStreamOptions struct {
	IncludeCommandOutput bool
	IncludeRawEvent      bool
}

func NewEventStreamCollector(dst io.Writer, handler EventHandler) *EventStreamWriter {
	return NewEventStreamCollectorWithOptions(dst, handler, EventStreamOptions{IncludeCommandOutput: true, IncludeRawEvent: true})
}

func NewEventStreamCollectorWithOptions(dst io.Writer, handler EventHandler, options EventStreamOptions) *EventStreamWriter {
	return &EventStreamWriter{
		dst:                  dst,
		handler:              handler,
		includeCommandOutput: options.IncludeCommandOutput,
		includeRawEvent:      options.IncludeRawEvent,
	}
}

func (w *EventStreamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return 0, io.ErrClosedPipe
	}
	if w.dst != nil {
		if _, err := w.dst.Write(p); err != nil {
			return 0, err
		}
	}
	w.pending = append(w.pending, p...)
	w.processCompleteLinesLocked()
	return len(p), nil
}

func (w *EventStreamWriter) Finish() (TurnResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return w.result, w.err
	}
	w.done = true
	if len(bytes.TrimSpace(w.pending)) > 0 {
		w.processLineLocked(w.pending)
	}
	w.pending = nil
	return w.result, w.err
}

func (w *EventStreamWriter) Result() (TurnResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.result, w.err
}

func (w *EventStreamWriter) processCompleteLinesLocked() {
	start := 0
	for {
		idx := bytes.IndexByte(w.pending[start:], '\n')
		if idx < 0 {
			break
		}
		lineEnd := start + idx
		w.processLineLocked(w.pending[start:lineEnd])
		start = lineEnd + 1
	}
	if start > 0 {
		copy(w.pending, w.pending[start:])
		w.pending = w.pending[:len(w.pending)-start]
	}
}

func (w *EventStreamWriter) processLineLocked(line []byte) {
	if w.err != nil {
		return
	}
	w.lineNo++
	event, trimmed, ok, err := parseCodexEventJSONLLineWithOptions(line, w.lineNo, w.includeCommandOutput)
	if err != nil {
		w.err = err
		return
	}
	if !ok {
		return
	}
	applyEvent(&w.result, event, trimmed)
	if w.handler == nil {
		return
	}
	if streamEvent, ok, err := streamEventFromCodexEventWithOptions(event, trimmed, w.includeRawEvent); err == nil && ok {
		w.handler(streamEvent)
	}
}
