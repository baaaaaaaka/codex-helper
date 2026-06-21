package teams

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
)

type MachineRegistryGraphAdapter struct {
	graph *GraphClient
}

func NewMachineRegistryGraphAdapter(graph *GraphClient) MachineRegistryGraphAdapter {
	return MachineRegistryGraphAdapter{graph: graph}
}

func NewMachineRegistryGraphAdapterForAuth(auth *AuthManager, out io.Writer) MachineRegistryGraphAdapter {
	return NewMachineRegistryGraphAdapter(NewGraphClient(auth, out))
}

func (a MachineRegistryGraphAdapter) SendHTML(ctx context.Context, chatID string, html string) (machineregistry.ChatMessage, error) {
	msg, err := a.graph.SendHTML(ctx, chatID, html)
	if err != nil {
		return machineregistry.ChatMessage{}, wrapMachineRegistryGraphError(err)
	}
	return machineRegistryMessage(msg), nil
}

func (a MachineRegistryGraphAdapter) UpdateChatMessageHTML(ctx context.Context, chatID string, messageID string, html string) error {
	if err := a.graph.UpdateChatMessageHTML(ctx, chatID, messageID, html); err != nil {
		return wrapMachineRegistryGraphError(err)
	}
	return nil
}

func (a MachineRegistryGraphAdapter) ListMessages(ctx context.Context, chatID string, top int) ([]machineregistry.ChatMessage, error) {
	messages, err := a.graph.ListMessages(ctx, chatID, top)
	if err != nil {
		return nil, wrapMachineRegistryGraphError(err)
	}
	out := make([]machineregistry.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, machineRegistryMessage(msg))
	}
	return out, nil
}

func (a MachineRegistryGraphAdapter) ListMessagesExactTopWithoutRateLimitRetry(ctx context.Context, chatID string, top int) ([]machineregistry.ChatMessage, error) {
	messages, err := a.graph.ListMessagesExactTopWithoutRateLimitRetry(ctx, chatID, top)
	if err != nil {
		return nil, wrapMachineRegistryGraphError(err)
	}
	out := make([]machineregistry.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, machineRegistryMessage(msg))
	}
	return out, nil
}

func (a MachineRegistryGraphAdapter) ListMessagesWindow(ctx context.Context, chatID string, top int) (machineregistry.MessageWindow, error) {
	window, err := a.graph.ListMessagesWindow(ctx, chatID, top, time.Time{})
	if err != nil {
		return machineregistry.MessageWindow{}, wrapMachineRegistryGraphError(err)
	}
	return machineRegistryMessageWindow(window), nil
}

func (a MachineRegistryGraphAdapter) ListMessagesWindowFromPath(ctx context.Context, path string) (machineregistry.MessageWindow, error) {
	window, err := a.graph.ListMessagesWindowFromPath(ctx, path)
	if err != nil {
		return machineregistry.MessageWindow{}, wrapMachineRegistryGraphError(err)
	}
	return machineRegistryMessageWindow(window), nil
}

func (a MachineRegistryGraphAdapter) CreateOrGetMeetingChatWindow(ctx context.Context, topic string, externalID string, start time.Time, end time.Time) (machineregistry.Chat, machineregistry.OnlineMeeting, error) {
	chat, meeting, err := a.graph.CreateOrGetMeetingChatWindow(ctx, topic, externalID, start, end)
	if err != nil {
		return machineregistry.Chat{}, machineregistry.OnlineMeeting{}, wrapMachineRegistryGraphError(err)
	}
	return machineRegistryChat(chat), machineRegistryMeeting(meeting), nil
}

func (a MachineRegistryGraphAdapter) GetOnlineMeeting(ctx context.Context, meetingID string) (machineregistry.OnlineMeeting, error) {
	meeting, err := a.graph.GetOnlineMeeting(ctx, meetingID)
	if err != nil {
		return machineregistry.OnlineMeeting{}, wrapMachineRegistryGraphError(err)
	}
	return machineRegistryMeeting(meeting), nil
}

func (a MachineRegistryGraphAdapter) UpdateOnlineMeetingWindow(ctx context.Context, meetingID string, start time.Time, end time.Time) (machineregistry.OnlineMeeting, error) {
	meeting, err := a.graph.UpdateOnlineMeetingWindow(ctx, meetingID, start, end)
	if err != nil {
		return machineregistry.OnlineMeeting{}, wrapMachineRegistryGraphError(err)
	}
	return machineRegistryMeeting(meeting), nil
}

func machineRegistryMessage(msg ChatMessage) machineregistry.ChatMessage {
	return machineregistry.ChatMessage{
		ID:                   msg.ID,
		CreatedDateTime:      msg.CreatedDateTime,
		LastModifiedDateTime: msg.LastModifiedDateTime,
		Body: machineregistry.ChatMessageBody{
			Content: msg.Body.Content,
		},
	}
}

func machineRegistryMessageWindow(window MessageWindow) machineregistry.MessageWindow {
	out := machineregistry.MessageWindow{
		Truncated: window.Truncated,
		NextPath:  window.NextPath,
		Messages:  make([]machineregistry.ChatMessage, 0, len(window.Messages)),
	}
	for _, msg := range window.Messages {
		out.Messages = append(out.Messages, machineRegistryMessage(msg))
	}
	return out
}

func machineRegistryChat(chat Chat) machineregistry.Chat {
	return machineregistry.Chat{
		ID:       chat.ID,
		Topic:    chat.Topic,
		ChatType: chat.ChatType,
		WebURL:   chat.WebURL,
	}
}

func machineRegistryMeeting(meeting OnlineMeeting) machineregistry.OnlineMeeting {
	return machineregistry.OnlineMeeting{
		ID:             meeting.ID,
		Subject:        meeting.Subject,
		JoinWebURL:     meeting.JoinWebURL,
		StartDateTime:  meeting.StartDateTime,
		EndDateTime:    meeting.EndDateTime,
		ExpiryDateTime: meeting.ExpiryDateTime,
		ChatThreadID:   meeting.ChatInfo.ThreadID,
	}
}

func wrapMachineRegistryGraphError(err error) error {
	if err == nil {
		return nil
	}
	var graphErr *GraphStatusError
	if errors.As(err, &graphErr) {
		return &machineregistry.StatusError{StatusCode: graphErr.StatusCode, RetryAfter: graphErr.RetryAfter, Err: err}
	}
	return err
}
