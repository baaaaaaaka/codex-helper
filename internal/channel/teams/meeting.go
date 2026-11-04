package teams

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Meeting is the online meeting behind a meeting chat.
type Meeting struct {
	ID      string
	ChatID  string
	Subject string
	JoinURL string
	Start   time.Time
	End     time.Time
}

type graphMeeting struct {
	ID       string `json:"id"`
	Subject  string `json:"subject"`
	JoinURL  string `json:"joinWebUrl"`
	Start    string `json:"startDateTime"`
	End      string `json:"endDateTime"`
	ChatInfo struct {
		ThreadID string `json:"threadId"`
	} `json:"chatInfo"`
}

func (g graphMeeting) meeting() Meeting {
	m := Meeting{ID: g.ID, ChatID: strings.TrimSpace(g.ChatInfo.ThreadID), Subject: g.Subject, JoinURL: g.JoinURL}
	m.Start, _ = time.Parse(time.RFC3339Nano, g.Start)
	m.End, _ = time.Parse(time.RFC3339Nano, g.End)
	return m
}

// CreateMeetingChat is CreateChat with an explicit meeting window, returning
// the meeting too (the machine registry renews long-lived meetings).
func (a *Adapter) CreateMeetingChat(ctx context.Context, title, key string, start, end time.Time) (channel.ChatInfo, Meeting, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return channel.ChatInfo{}, Meeting{}, &channel.Rejected{Permanent: true, Err: errors.New("CreateChat needs a key")}
	}
	start, end = start.UTC().Truncate(time.Second), end.UTC().Truncate(time.Second)
	if start.IsZero() || !end.After(start) {
		return channel.ChatInfo{}, Meeting{}, &channel.Rejected{Permanent: true, Err: errors.New("meeting window must end after it starts")}
	}
	me, err := a.me(ctx)
	if err != nil {
		return channel.ChatInfo{}, Meeting{}, err
	}
	subject := SanitizeTopic(title)
	in := map[string]any{
		"subject":       subject,
		"externalId":    key,
		"startDateTime": start.Format(time.RFC3339),
		"endDateTime":   end.Format(time.RFC3339),
		"participants": map[string]any{"attendees": []map[string]any{{
			"upn": me.UPN, "role": "attendee",
			"identity": map[string]any{"user": map[string]any{"id": me.ID}},
		}}},
		"allowMeetingChat":               "enabled",
		"shareMeetingChatHistoryDefault": "all",
	}
	var out graphMeeting
	if err := a.g.writeJSON(ctx, http.MethodPost, "/me/onlineMeetings/createOrGet", in, &out); err != nil {
		return channel.ChatInfo{}, Meeting{}, err
	}
	mt := out.meeting()
	if mt.ChatID == "" {
		return channel.ChatInfo{}, mt, &channel.Ambiguous{Err: errors.New("onlineMeeting response has no chatInfo.threadId")}
	}
	return channel.ChatInfo{ID: mt.ChatID, Title: first(SanitizeTopic(out.Subject), subject), URL: ChatURL(mt.ChatID, a.cfg.TenantID)}, mt, nil
}

// GetMeeting reads a meeting by id.
func (a *Adapter) GetMeeting(ctx context.Context, id string) (Meeting, error) {
	var out graphMeeting
	path := "/me/onlineMeetings/" + url.PathEscape(id) + "?$select=id,subject,joinWebUrl,startDateTime,endDateTime,chatInfo"
	if err := a.g.getJSON(ctx, path, smallBodyLimit, &out); err != nil {
		return Meeting{}, err
	}
	return out.meeting(), nil
}

// RenewMeeting moves a meeting's window (PATCH startDateTime/endDateTime) so
// a long-lived meeting chat stays usable. Idempotent.
func (a *Adapter) RenewMeeting(ctx context.Context, id string, start, end time.Time) (Meeting, error) {
	start, end = start.UTC().Truncate(time.Second), end.UTC().Truncate(time.Second)
	if start.IsZero() || !end.After(start) {
		return Meeting{}, &channel.Rejected{Permanent: true, Err: errors.New("meeting window must end after it starts")}
	}
	var out graphMeeting
	in := map[string]string{"startDateTime": start.Format(time.RFC3339), "endDateTime": end.Format(time.RFC3339)}
	if err := a.g.writeJSON(ctx, http.MethodPatch, "/me/onlineMeetings/"+url.PathEscape(id), in, &out); err != nil {
		return Meeting{}, err
	}
	return out.meeting(), nil
}
