package fakegraph

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// Inbound attachments as the Teams client produces them:
//
//   - a pasted image is a hosted content of the message, referenced from the
//     HTML body as .../messages/{id}/hostedContents/{hc}/$value and fetched
//     with GET .../hostedContents/{hc}/$value;
//   - a file is uploaded by the client to the sender's OneDrive and attached
//     as {contentType:"reference", contentUrl:<SharePoint URL>, name}; the
//     helper resolves it through GET /shares/u!{base64url(url)}/driveItem
//     (metadata) and .../driveItem/content.

// SharePointHost is the host of fake OneDrive file URLs (matches the old
// core's default *.sharepoint.com allowlist).
const SharePointHost = "e2etenant-my.sharepoint.com"

type hostedKey struct{ chat, message, id string }

type driveFile struct {
	name string
	data []byte
}

// PostImageAsUser posts text plus one pasted image (hosted content) as the
// owner.
func (s *Server) PostImageAsUser(chatID, text string, png []byte) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return Message{}, fmt.Errorf("fakegraph: no chat %s", chatID)
	}
	m := s.appendMessageLocked(c, OriginUser, s.opts.Owner, "html", "")
	hcID := "aWQ9eF8wLXd1cy1kMTEtZTJl" // opaque, base64-looking like Teams ids
	src := fmt.Sprintf("https://graph.microsoft.com/v1.0/chats/%s/messages/%s/hostedContents/%s/$value", chatID, m.ID, hcID)
	m.Content = TextToHTML(text) + fmt.Sprintf(`<p><img src="%s" width="64" height="64" alt="image" itemid="0"></p>`, src)
	m.Hosted = 1
	if s.hosted == nil {
		s.hosted = map[hostedKey][]byte{}
	}
	s.hosted[hostedKey{chatID, m.ID, hcID}] = append([]byte(nil), png...)
	return m.Snapshot(), nil
}

// PostFileAsUser posts text plus one file attachment (a OneDrive reference)
// as the owner.
func (s *Server) PostFileAsUser(chatID, text, name string, data []byte) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return Message{}, fmt.Errorf("fakegraph: no chat %s", chatID)
	}
	s.idCounter++
	attID := fmt.Sprintf("e2e0f11e-0000-4000-8000-%012d", s.idCounter)
	fileURL := "https://" + SharePointHost + "/personal/owner_e2e_example/Documents/Microsoft%20Teams%20Chat%20Files/" + url.PathEscape(name)
	m := s.appendMessageLocked(c, OriginUser, s.opts.Owner, "html", TextToHTML(text)+fmt.Sprintf(`<attachment id="%s"></attachment>`, attID))
	att, _ := json.Marshal(map[string]any{
		"id": attID, "contentType": "reference", "contentUrl": fileURL,
		"content": nil, "name": name, "thumbnailUrl": nil, "teamsAppId": nil,
	})
	m.Attachments = append(m.Attachments, att)
	if s.drive == nil {
		s.drive = map[string]driveFile{}
	}
	s.drive[fileURL] = driveFile{name: name, data: append([]byte(nil), data...)}
	return m.Snapshot(), nil
}

// AddCoworker adds the coworker account to a chat (as when the owner adds
// a colleague in the Teams client), making it a multi-member chat.
func (s *Server) AddCoworker(chatID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chats[chatID]
	if c == nil {
		return fmt.Errorf("fakegraph: no chat %s", chatID)
	}
	for _, m := range c.Members {
		if m == s.opts.Coworker.ID {
			return nil
		}
	}
	c.Members = append(c.Members, s.opts.Coworker.ID)
	return nil
}

func (s *Server) hostedContent(route routeMatch) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, resp := s.memberChatLocked(route.chatID)
	if c == nil {
		return resp
	}
	data, ok := s.hosted[hostedKey{route.chatID, route.messageID, route.extra}]
	if !ok {
		return response{status: 404, body: graphError("NotFound", "hosted content not found")}
	}
	return response{status: 200, raw: data, rawType: "image/png"}
}

// shareURL decodes a "u!" sharing token.
func shareURL(token string) (string, bool) {
	rest, ok := strings.CutPrefix(token, "u!")
	if !ok {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(rest)
	if err != nil {
		return "", false
	}
	return string(raw), true
}

func (s *Server) shareItem(route routeMatch, content bool, rec *Request) response {
	u, ok := shareURL(route.extra)
	if !ok {
		return violationResponse(rec, "graph-contract", "share id must be u! + unpadded base64url of the URL")
	}
	s.mu.Lock()
	f, found := s.drive[u]
	s.mu.Unlock()
	if !found {
		return response{status: 404, body: graphError("itemNotFound", "The resource could not be found.")}
	}
	if content {
		return response{status: 200, raw: f.data, rawType: "application/octet-stream"}
	}
	return response{status: 200, body: map[string]any{
		"id":     "01E2E" + strings.ToUpper(fmt.Sprintf("%x", len(u)))[:2] + "FILE",
		"name":   f.name,
		"size":   len(f.data),
		"eTag":   `"{E2E0F11E-0000-4000-8000-000000000001},1"`,
		"webUrl": u,
		"file":   map[string]any{"mimeType": "application/octet-stream"},
	}}
}
