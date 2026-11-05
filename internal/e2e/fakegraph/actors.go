package fakegraph

import (
	"fmt"
	"html"
	"sort"
	"strings"
	"time"
)

// ChatInfo is a snapshot of a chat without its messages.
type ChatInfo struct {
	ID         string
	Topic      string
	ChatType   string
	Created    time.Time
	MeetingID  string
	ExternalID string
	Messages   int
	Members    []string
	TopicLog   []string
}

// Chats returns all chats in creation order.
func (s *Server) Chats() []ChatInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ChatInfo, 0, len(s.chatOrder))
	for _, id := range s.chatOrder {
		c := s.chats[id]
		out = append(out, ChatInfo{
			ID: c.ID, Topic: c.Topic, ChatType: c.ChatType, Created: c.Created,
			MeetingID: c.MeetingID, ExternalID: c.ExternalID, Messages: len(c.Messages),
			Members: append([]string(nil), c.Members...), TopicLog: append([]string(nil), c.TopicLog...),
		})
	}
	return out
}

// Chat returns one chat snapshot.
func (s *Server) Chat(chatID string) (ChatInfo, bool) {
	for _, c := range s.Chats() {
		if c.ID == chatID {
			return c, true
		}
	}
	return ChatInfo{}, false
}

// Messages returns the messages of a chat in creation order.
func (s *Server) Messages(chatID string) []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return nil
	}
	out := make([]Message, 0, len(c.Messages))
	for _, m := range c.Messages {
		out = append(out, m.Snapshot())
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Created.Before(out[j].Created) })
	return out
}

// AllMessages returns every message in every chat in creation order.
func (s *Server) AllMessages() []Message {
	var out []Message
	for _, c := range s.Chats() {
		out = append(out, s.Messages(c.ID)...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Created.Before(out[j].Created) })
	return out
}

// Requests returns recorded requests matching the filter, in arrival order.
func (s *Server) Requests(f RequestFilter) []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Request
	for _, r := range s.requests {
		if f.match(r) {
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// LastSeq returns the sequence number of the most recent request.
func (s *Server) LastSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

// Violations returns all recorded contract violations.
func (s *Server) Violations() []Violation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Violation(nil), s.violations...)
}

// RecordBlocked records a request to a host the fake does not serve (it is
// dropped unanswered, as the offline sandbox would).
func (s *Server) RecordBlocked(b BlockedRequest) {
	s.mu.Lock()
	s.blocked = append(s.blocked, b)
	s.mu.Unlock()
}

// Blocked returns refused foreign-host requests.
func (s *Server) Blocked() []BlockedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]BlockedRequest(nil), s.blocked...)
}

// InFlight reports requests currently being served.
func (s *Server) InFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight
}

// ---------------------------------------------------------------------------
// Acting as people

// TextToHTML renders plain text the way the Teams desktop client does for a
// typed message: one <p> with escaped text and <br> for line breaks.
func TextToHTML(text string) string {
	lines := strings.Split(text, "\n")
	for i, l := range lines {
		lines[i] = html.EscapeString(l)
	}
	return "<p>" + strings.Join(lines, "<br>") + "</p>"
}

// PostAsUser posts text into a chat as the signed-in human owner, the way
// the Teams client would (HTML body).
func (s *Server) PostAsUser(chatID, text string) (Message, error) {
	return s.post(chatID, OriginUser, s.opts.Owner, "html", TextToHTML(text))
}

// PostHTMLAsUser posts raw HTML as the owner.
func (s *Server) PostHTMLAsUser(chatID, htmlBody string) (Message, error) {
	return s.post(chatID, OriginUser, s.opts.Owner, "html", htmlBody)
}

// PostAsOtherHelper posts raw HTML as if another helper instance that uses
// the same account wrote it.
func (s *Server) PostAsOtherHelper(chatID, htmlBody string) (Message, error) {
	return s.post(chatID, OriginOtherHelper, s.opts.Owner, "html", htmlBody)
}

// PostAsCoworker posts text as the second account. The coworker is added to
// the chat as a side effect.
func (s *Server) PostAsCoworker(chatID, text string) (Message, error) {
	s.mu.Lock()
	if c := s.chats[chatID]; c != nil {
		found := false
		for _, m := range c.Members {
			if m == s.opts.Coworker.ID {
				found = true
			}
		}
		if !found {
			c.Members = append(c.Members, s.opts.Coworker.ID)
		}
	}
	s.mu.Unlock()
	return s.post(chatID, OriginCoworker, s.opts.Coworker, "html", TextToHTML(text))
}

func (s *Server) post(chatID string, origin Origin, from User, ct, content string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return Message{}, fmt.Errorf("fakegraph: no chat %s", chatID)
	}
	m := s.appendMessageLocked(c, origin, from, ct, content)
	return m.Snapshot(), nil
}

// PostSystemEvent appends a Teams system event message (messageType
// systemEventMessage, no sender), as Teams does for membership and meeting
// events.
func (s *Server) PostSystemEvent(chatID string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return Message{}, fmt.Errorf("fakegraph: no chat %s", chatID)
	}
	m := s.appendMessageLocked(c, OriginSystem, User{}, "html", "<systemEventMessage/>")
	m.MessageType = "systemEventMessage"
	return m.Snapshot(), nil
}

// EditAsUser replaces a message body the way a user edit would, bumping
// lastModifiedDateTime.
func (s *Server) EditAsUser(chatID, messageID, text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return fmt.Errorf("fakegraph: no chat %s", chatID)
	}
	for _, m := range c.Messages {
		if m.ID == messageID {
			m.PatchHistory = append(m.PatchHistory, m.Content)
			m.Content = TextToHTML(text)
			m.ContentType = "html"
			m.Modified = s.nextStampLocked()
			return nil
		}
	}
	return fmt.Errorf("fakegraph: no message %s", messageID)
}

// CreateMeetingChatAsUser creates a meeting chat outside the SUT (as if the
// user created it in the Teams client).
func (s *Server) CreateMeetingChatAsUser(topic string) ChatInfo {
	s.mu.Lock()
	c := s.addChatLocked("meeting", topic, []string{s.opts.Owner.ID})
	s.mu.Unlock()
	info, _ := s.Chat(c.ID)
	return info
}

// SeedChat adds a group chat with a known id that existed before the
// scenario (an older helper created it), with the owner as its member.
func (s *Server) SeedChat(id, topic string) ChatInfo {
	s.mu.Lock()
	if s.chats[id] == nil {
		c := &Chat{ID: id, Topic: topic, ChatType: "group", Created: s.nextStampLocked(), Members: []string{s.opts.Owner.ID}}
		s.chats[id] = c
		s.chatOrder = append(s.chatOrder, id)
	}
	s.mu.Unlock()
	info, _ := s.Chat(id)
	return info
}

// SeedMessage adds a message that existed before the scenario, posted by the
// owner's account (the owner, or an older helper posting as the owner), with
// its original id and time. Seed each chat in chronological order before the
// SUT starts; a message at or after the fake's clock is refused.
func (s *Server) SeedMessage(chatID, id string, at time.Time, htmlBody string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return fmt.Errorf("seed message: unknown chat %s", chatID)
	}
	at = at.UTC().Truncate(time.Millisecond)
	if !at.Before(time.Now()) || !at.After(time.Time{}) {
		return fmt.Errorf("seed message %s: time %s is not in the past", id, at)
	}
	if n := len(c.Messages); n > 0 && at.Before(c.Messages[n-1].Created) {
		return fmt.Errorf("seed message %s: out of order", id)
	}
	c.Messages = append(c.Messages, &Message{ID: id, ChatID: c.ID, Created: at, Modified: at, MessageType: "message",
		FromUserID: s.opts.Owner.ID, FromName: s.opts.Owner.DisplayName, ContentType: "html", Content: htmlBody, Origin: OriginSeeded})
	return nil
}

// SetPreviewLag changes Options.PreviewLag at run time.
func (s *Server) SetPreviewLag(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opts.PreviewLag = d
}
