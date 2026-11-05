package fakegraph

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

// Quoted messages and multi-attachment posts as the Teams client records
// them:
//
//   - a reply quotes a message of the same chat: an attachment
//     {contentType:"messageReference", id:<msg id>, content:"{messageId,
//     messagePreview, messageSender}"} referenced from the body by
//     <attachment id=…>; the preview is the client's short excerpt, the
//     full text is read with GET /chats/{chat}/messages/{id};
//   - a forward carries the original inline: {contentType:
//     "forwardedMessageReference", content:"{originalMessageId,
//     originalMessageContent, originalSentDateTime, originalMessageSender,
//     originalChatId}"}.

// QuoteSpec is one quoted (replied-to) or forwarded message of a post.
type QuoteSpec struct {
	MessageID  string    // the quoted message (a reply quotes one of the same chat)
	Preview    string    // the client's messagePreview (reply) or the original text (forward)
	SenderName string    // display name of the quoted message's author
	SenderID   string    // its user id
	Forwarded  bool      // a forward instead of a reply
	Sent       time.Time // originalSentDateTime of a forward
	FromChat   string    // originalChatId of a forward
}

// FileSpec is one OneDrive file of a post.
type FileSpec struct {
	Name string
	Data []byte
}

// PostWithQuotesAsUser posts text that quotes (replies to) or forwards the
// given messages, as the owner.
func (s *Server) PostWithQuotesAsUser(chatID, text string, quotes ...QuoteSpec) (Message, error) {
	return s.PostAttachmentsAsUser(chatID, text, nil, nil, quotes...)
}

// PostAttachmentsAsUser posts text with pasted images (hosted contents),
// OneDrive files and quoted messages, in one message, as the owner.
func (s *Server) PostAttachmentsAsUser(chatID, text string, images [][]byte, files []FileSpec, quotes ...QuoteSpec) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return Message{}, fmt.Errorf("fakegraph: no chat %s", chatID)
	}
	m := s.appendMessageLocked(c, OriginUser, s.opts.Owner, "html", "")
	content := ""
	for _, q := range quotes {
		s.idCounter++
		who := map[string]any{"application": nil, "device": nil, "user": map[string]any{
			"userIdentityType": "aadUser", "id": q.SenderID, "displayName": q.SenderName}}
		var att []byte
		if q.Forwarded {
			body, _ := json.Marshal(map[string]any{
				"originalMessageId": q.MessageID, "originalMessageContent": TextToHTML(q.Preview),
				"originalSentDateTime":  q.Sent.UTC().Format("2006-01-02T15:04:05.000Z"),
				"originalMessageSender": who, "originalChatId": q.FromChat,
			})
			att, _ = json.Marshal(map[string]any{"id": fmt.Sprintf("e2e0f0d0-0000-4000-8000-%012d", s.idCounter),
				"contentType": "forwardedMessageReference", "contentUrl": nil, "content": string(body), "name": nil})
		} else {
			body, _ := json.Marshal(map[string]any{"messageId": q.MessageID, "messagePreview": q.Preview, "messageSender": who})
			att, _ = json.Marshal(map[string]any{"id": q.MessageID, "contentType": "messageReference",
				"contentUrl": nil, "content": string(body), "name": nil})
		}
		var id struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(att, &id)
		content += fmt.Sprintf(`<attachment id="%s"></attachment>`, id.ID)
		m.Attachments = append(m.Attachments, att)
	}
	content += TextToHTML(text)
	for i, png := range images {
		hcID := fmt.Sprintf("aWQ9eF8%d-LXd1cy1kMTEtZTJl", i)
		src := fmt.Sprintf("https://graph.microsoft.com/v1.0/chats/%s/messages/%s/hostedContents/%s/$value", chatID, m.ID, hcID)
		content += fmt.Sprintf(`<p><img src="%s" width="64" height="64" alt="image" itemid="%d"></p>`, src, i)
		m.Hosted++
		m.HostedHashes = append(m.HostedHashes, HashBytes(png))
		if s.hosted == nil {
			s.hosted = map[hostedKey][]byte{}
		}
		s.hosted[hostedKey{chatID, m.ID, hcID}] = append([]byte(nil), png...)
	}
	for _, f := range files {
		s.idCounter++
		attID := fmt.Sprintf("e2e0f11e-0000-4000-8000-%012d", s.idCounter)
		fileURL := "https://" + SharePointHost + "/personal/owner_e2e_example/Documents/Microsoft%20Teams%20Chat%20Files/" + url.PathEscape(f.Name)
		content += fmt.Sprintf(`<attachment id="%s"></attachment>`, attID)
		att, _ := json.Marshal(map[string]any{
			"id": attID, "contentType": "reference", "contentUrl": fileURL,
			"content": nil, "name": f.Name, "thumbnailUrl": nil, "teamsAppId": nil,
		})
		m.Attachments = append(m.Attachments, att)
		if s.drive == nil {
			s.drive = map[string]driveFile{}
		}
		s.drive[fileURL] = driveFile{name: f.Name, data: append([]byte(nil), f.Data...)}
	}
	m.Content = content
	return m.Snapshot(), nil
}
