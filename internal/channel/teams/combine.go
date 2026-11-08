package teams

import (
	"encoding/json"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// CombineProgress keeps complete HTML documents in order. Payloads with hosted
// images, mentions or attachments are left alone: their identifiers/references
// cannot safely be concatenated. Math and rich payloads still send normally.
func (a *Adapter) CombineProgress(parts []channel.Part) (channel.Part, bool) {
	if len(parts) < 2 {
		return channel.Part{}, false
	}
	var html strings.Builder
	for i, p := range parts {
		if p.Index != 1 || p.Count != 1 || p.Mention || p.ReplyTo != "" {
			return channel.Part{}, false
		}
		var msg map[string]json.RawMessage
		if json.Unmarshal([]byte(p.Body), &msg) != nil || len(msg) != 1 {
			return channel.Part{}, false
		}
		var body struct {
			ContentType string `json:"contentType"`
			Content     string `json:"content"`
		}
		if json.Unmarshal(msg["body"], &body) != nil || body.ContentType != "html" {
			return channel.Part{}, false
		}
		if i > 0 {
			html.WriteString("<hr>")
		}
		html.WriteString(body.Content)
		if html.Len()+markerReserve > targetPartBytes {
			return channel.Part{}, false
		}
	}
	body, _ := json.Marshal(map[string]any{"body": map[string]string{"contentType": "html", "content": html.String()}})
	return channel.Part{Index: 1, Count: 1, Body: string(body), Size: html.Len(), Hash: channel.TextHash(plainText(html.String()))}, true
}
