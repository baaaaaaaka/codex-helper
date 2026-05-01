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
	StreamEventUsage            StreamEventKind = "usage"
)

type StreamEvent struct {
	Kind             StreamEventKind
	ThreadID         string
	TurnID           string
	ItemID           string
	ItemType         string
	Text             string
	Command          string
	AggregatedOutput string
	Status           string
	ExitCode         *int
	Failure          *TurnFailure
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
	out := StreamEvent{
		ThreadID: event.ThreadID,
		TurnID:   firstNonEmpty(event.TurnID, event.Turn.ID),
		Raw:      append([]byte(nil), line...),
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
	case "item.started", "item/started", "item.completed", "item/completed":
		out.ItemID = event.Item.ID
		out.ItemType = event.Item.Type
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

func usageFromEvent(event codexEvent) Usage {
	var usage Usage
	mergeUsage(&usage, event.Usage)
	return usage
}

type EventStreamWriter struct {
	dst     io.Writer
	handler EventHandler
	mu      sync.Mutex
	pending []byte
}

func NewEventStreamWriter(dst io.Writer, handler EventHandler) io.Writer {
	if handler == nil {
		return dst
	}
	return &EventStreamWriter{dst: dst, handler: handler}
}

func (w *EventStreamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dst != nil {
		if _, err := w.dst.Write(p); err != nil {
			return 0, err
		}
	}
	w.pending = append(w.pending, p...)
	for {
		idx := bytes.IndexByte(w.pending, '\n')
		if idx < 0 {
			break
		}
		line := append([]byte(nil), w.pending[:idx]...)
		w.pending = append(w.pending[:0], w.pending[idx+1:]...)
		if event, ok, err := ParseStreamEventJSONL(line); err == nil && ok {
			w.handler(event)
		}
	}
	return len(p), nil
}
