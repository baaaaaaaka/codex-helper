package teams

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// CombineProgress keeps complete HTML documents in order. Payloads with hosted
// images, mentions or attachments are left alone: their identifiers/references
// cannot safely be concatenated. Math and rich payloads still send normally.
func (a *Adapter) CombineProgress(parts []channel.Part) (channel.Part, bool) {
	return a.combineParts(parts, false)
}

func (a *Adapter) CombineRecovery(parts []channel.Part) (channel.Part, bool) {
	return a.combineParts(parts, true)
}

func (a *Adapter) combineParts(parts []channel.Part, recovery bool) (channel.Part, bool) {
	if len(parts) == 0 || (!recovery && len(parts) < 2) {
		return channel.Part{}, false
	}
	var html strings.Builder
	if recovery {
		html.WriteString("<p><strong>Recovered history</strong></p>")
	}
	for i := 0; i < len(parts); i++ {
		p := parts[i]
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
		if recovery {
			n := 1
			for i+n < len(parts) && parts[i+n] == p {
				n++
			}
			if n > 1 {
				fmt.Fprintf(&html, "<p>Repeated %d times:</p>", n)
				i += n - 1
			}
		}
		html.WriteString(body.Content)
		if html.Len()+markerReserve > targetPartBytes {
			return channel.Part{}, false
		}
	}
	body, _ := json.Marshal(map[string]any{"body": map[string]string{"contentType": "html", "content": html.String()}})
	return channel.Part{Index: 1, Count: 1, Body: string(body), Size: html.Len(), Hash: channel.TextHash(plainText(html.String()))}, true
}
