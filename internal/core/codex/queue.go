package codex

import (
	"context"
	"errors"
)

// QueuedSubmission is a native durable input, identified independently of its turn.
type QueuedSubmission struct {
	ID       string `json:"id"`
	ClientID string `json:"clientUserMessageId"`
}

func inputs(input []Input) []any {
	out := make([]any, 0, len(input))
	for _, in := range input {
		if in.Image != "" {
			out = append(out, map[string]any{"type": "localImage", "path": in.Image})
		} else {
			out = append(out, map[string]any{"type": "text", "text": in.Text, "text_elements": []any{}})
		}
	}
	return out
}
func (s *Server) TurnSteer(ctx context.Context, p TurnStartParams, expected string) error {
	return s.Call(ctx, "turn/steer", map[string]any{"threadId": p.ThreadID, "expectedTurnId": expected, "input": inputs(p.Input), "clientUserMessageId": p.ClientID}, nil)
}
func (s *Server) QueueAdd(ctx context.Context, p TurnStartParams) (string, error) {
	var out struct {
		Submission QueuedSubmission `json:"queuedSubmission"`
	}
	err := s.Call(ctx, "thread/queue/add", map[string]any{"threadId": p.ThreadID, "input": inputs(p.Input), "clientUserMessageId": p.ClientID}, &out)
	if err == nil && out.Submission.ID == "" {
		err = errors.New("codex: missing queued submission id")
	}
	return out.Submission.ID, err
}
func (s *Server) QueueStart(ctx context.Context, thread, id string) error {
	return s.Call(ctx, "thread/queue/start", map[string]any{"threadId": thread, "queuedSubmissionId": id}, nil)
}
func (s *Server) QueueDelete(ctx context.Context, thread, id string) (bool, error) {
	var out struct {
		Deleted bool `json:"deleted"`
	}
	err := s.Call(ctx, "thread/queue/delete", map[string]any{"threadId": thread, "queuedSubmissionId": id}, &out)
	return out.Deleted, err
}

// RemoveQueuedInput is used before resuming a recovered thread: resuming
// would otherwise dispatch a request whose outcome the helper is reconciling.
// Pagination is bounded by the native page size, and runs only on recovery.
func (s *Server) RemoveQueuedInput(ctx context.Context, thread, client string) (bool, error) {
	id, err := s.queuedInput(ctx, thread, client)
	if err != nil || id == "" {
		return false, err
	}
	return s.QueueDelete(ctx, thread, id)
}

func (s *Server) queuedInput(ctx context.Context, thread, client string) (string, error) {
	cursor := ""
	for {
		var page struct {
			Data []QueuedSubmission `json:"data"`
			Next *string            `json:"nextCursor"`
		}
		p := map[string]any{"threadId": thread, "limit": 100}
		if cursor != "" {
			p["cursor"] = cursor
		}
		if err := s.Call(ctx, "thread/queue/list", p, &page); err != nil {
			return "", err
		}
		for _, q := range page.Data {
			if q.ClientID == client {
				return q.ID, nil
			}
		}
		if page.Next == nil || *page.Next == "" {
			return "", nil
		}
		cursor = *page.Next
	}
}
