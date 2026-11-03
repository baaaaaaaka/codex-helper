// Package fakegraph is an in-memory, deliberately strict model of the subset
// of Microsoft Graph (Teams chats and messages) and the Microsoft identity
// token endpoint that the Teams helper talks to.
//
// It is test-only infrastructure for the black-box E2E harness in
// internal/e2e. It never talks to the real network. Every request is
// recorded, and any request that real Graph would reject (or that this fake
// does not model) is answered with HTTP 400 and recorded as a contract
// violation so a scenario report can list it.
package fakegraph

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Origin records who created a message from the fake's point of view. Graph
// itself cannot tell these apart (the helper posts as the user), so Origin is
// never served to the SUT; it exists only for assertions.
type Origin string

const (
	// OriginSUT marks a message created through HTTP by the system under test.
	OriginSUT Origin = "sut"
	// OriginUser marks a message the scenario posted as the human owner.
	OriginUser Origin = "user"
	// OriginOtherHelper marks a message the scenario posted while acting as
	// a different helper instance that shares the same account.
	OriginOtherHelper Origin = "other-helper"
	// OriginCoworker marks a message posted by a second human account.
	OriginCoworker Origin = "coworker"
	// OriginSystem marks Teams-generated system event messages.
	OriginSystem Origin = "system"
)

// User is a Graph user known to the fake tenant.
type User struct {
	ID                string
	DisplayName       string
	UserPrincipalName string
}

// Message is one stored chat message.
type Message struct {
	ID           string
	ChatID       string
	Created      time.Time
	Modified     time.Time
	MessageType  string
	FromUserID   string
	FromName     string
	ContentType  string
	Content      string
	Attachments  []json.RawMessage
	Mentions     []json.RawMessage
	Hosted       int
	QuotedIDs    []string
	Origin       Origin
	Instance     string // SUT instance tag (from the harness transport), if any
	RequestSeq   int64  // request sequence number that created it, if any
	PatchCount   int
	PatchHistory []string
}

// Chat is one stored chat.
type Chat struct {
	ID         string
	Topic      string
	ChatType   string // "meeting" or "group"
	Created    time.Time
	Members    []string // user ids
	MeetingID  string
	ExternalID string
	Messages   []*Message
	Hidden     bool
	UnreadMark int
	TopicLog   []string
}

// Meeting is one stored onlineMeeting.
type Meeting struct {
	ID         string
	Subject    string
	ExternalID string
	Start      time.Time
	End        time.Time
	ChatID     string
	OwnerID    string
}

const graphTimeLayout = "2006-01-02T15:04:05.000Z"

func formatTime(t time.Time) string {
	return t.UTC().Format(graphTimeLayout)
}

// Snapshot is a copy of a message safe to use outside the server lock.
func (m *Message) Snapshot() Message {
	c := *m
	c.Attachments = append([]json.RawMessage(nil), m.Attachments...)
	c.Mentions = append([]json.RawMessage(nil), m.Mentions...)
	c.QuotedIDs = append([]string(nil), m.QuotedIDs...)
	c.PatchHistory = append([]string(nil), m.PatchHistory...)
	return c
}

// PlainText returns a rough plain-text rendering of the message body, good
// enough for substring assertions.
func (m Message) PlainText() string {
	if m.ContentType != "html" {
		return m.Content
	}
	return HTMLToText(m.Content)
}

// HTMLToText strips tags and decodes the handful of entities the helper
// emits. It is intentionally simple: assertions use unique tokens.
func HTMLToText(s string) string {
	var b strings.Builder
	inTag := false
	inComment := false
	for i := 0; i < len(s); i++ {
		if inComment {
			if strings.HasPrefix(s[i:], "-->") {
				inComment = false
				i += 2
			}
			continue
		}
		c := s[i]
		if !inTag && strings.HasPrefix(s[i:], "<!--") {
			inComment = true
			i += 3
			continue
		}
		switch {
		case c == '<':
			inTag = true
			tag := strings.ToLower(s[i:min(len(s), i+5)])
			if strings.HasPrefix(tag, "<br") || strings.HasPrefix(tag, "<p") || strings.HasPrefix(tag, "</p") || strings.HasPrefix(tag, "<li") || strings.HasPrefix(tag, "<div") || strings.HasPrefix(tag, "</div") || strings.HasPrefix(tag, "<tr") || strings.HasPrefix(tag, "<h") {
				b.WriteByte('\n')
			}
		case c == '>' && inTag:
			inTag = false
		case !inTag:
			b.WriteByte(c)
		}
	}
	r := strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`, "&#39;", "'", "&nbsp;", " ", "&#x27;", "'")
	return r.Replace(b.String())
}

func (m *Message) toGraph() map[string]any {
	out := map[string]any{
		"@odata.type":          "#microsoft.graph.chatMessage",
		"id":                   m.ID,
		"replyToId":            nil,
		"etag":                 strconv.FormatInt(m.Modified.UnixMilli(), 10),
		"messageType":          m.MessageType,
		"createdDateTime":      formatTime(m.Created),
		"lastModifiedDateTime": formatTime(m.Modified),
		"lastEditedDateTime":   nil,
		"deletedDateTime":      nil,
		"subject":              nil,
		"summary":              nil,
		"chatId":               m.ChatID,
		"importance":           "normal",
		"locale":               "en-us",
		"webUrl":               nil,
		"channelIdentity":      nil,
		"policyViolation":      nil,
		"eventDetail":          nil,
		"body": map[string]any{
			"contentType": m.ContentType,
			"content":     m.Content,
		},
		"attachments": rawList(m.Attachments),
		"mentions":    rawList(m.Mentions),
		"reactions":   []any{},
	}
	if m.PatchCount > 0 {
		out["lastEditedDateTime"] = formatTime(m.Modified)
	}
	if m.MessageType == "systemEventMessage" {
		out["from"] = nil
		out["eventDetail"] = map[string]any{"@odata.type": "#microsoft.graph.membersAddedEventMessageDetail"}
	} else {
		out["from"] = map[string]any{
			"application": nil,
			"device":      nil,
			"user": map[string]any{
				"@odata.type":      "#microsoft.graph.teamworkUserIdentity",
				"id":               m.FromUserID,
				"displayName":      m.FromName,
				"userIdentityType": "aadUser",
				"tenantId":         TenantID,
			},
		}
	}
	return out
}

func rawList(items []json.RawMessage) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

func (c *Chat) toGraph(tenant string) map[string]any {
	return map[string]any{
		"id":                  c.ID,
		"topic":               c.Topic,
		"createdDateTime":     formatTime(c.Created),
		"lastUpdatedDateTime": formatTime(c.Created),
		"chatType":            c.ChatType,
		"webUrl":              "https://teams.microsoft.com/l/chat/" + c.ID + "/0?tenantId=" + tenant,
		"tenantId":            tenant,
		"onlineMeetingInfo":   nil,
		"viewpoint":           map[string]any{"isHidden": c.Hidden},
	}
}

func (mt *Meeting) toGraph() map[string]any {
	return map[string]any{
		"id":             mt.ID,
		"subject":        mt.Subject,
		"externalId":     mt.ExternalID,
		"joinWebUrl":     "https://teams.microsoft.com/l/meetup-join/" + mt.ChatID + "/0",
		"startDateTime":  formatTime(mt.Start),
		"endDateTime":    formatTime(mt.End),
		"expiryDateTime": formatTime(mt.End.Add(60 * 24 * time.Hour)),
		"chatInfo": map[string]any{
			"threadId":  mt.ChatID,
			"messageId": "0",
		},
	}
}
