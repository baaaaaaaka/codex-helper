package teams

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"golang.org/x/net/html"
)

// confirmationEdit appends one initial receipt, identified by its target message
// link. This is not an own-post marker: the message still belongs to the user.
func confirmationEdit(chat, id, content string, isHTML bool, atts []graphAttachment, an channel.Annotation) (string, error) {
	refuse := func() (string, error) {
		return "", fmt.Errorf("confirmation cannot be appended losslessly: %w", channel.ErrNotEditable)
	}
	p := an.Confirmation
	if an.ConfirmationKey == "" || p.Count != 1 {
		return refuse()
	}
	var part struct {
		Body struct {
			ContentType string `json:"contentType"`
			Content     string `json:"content"`
		} `json:"body"`
		Hosted      []json.RawMessage `json:"hostedContents"`
		Attachments []json.RawMessage `json:"attachments"`
		Mentions    []json.RawMessage `json:"mentions"`
	}
	if json.Unmarshal([]byte(p.Body), &part) != nil || part.Body.ContentType != "html" || part.Body.Content == "" || len(part.Hosted)+len(part.Attachments)+len(part.Mentions) != 0 {
		return refuse()
	}
	// A normal Teams message link survives comment stripping and markup
	// normalization. Its target identifies this request; copied old receipts
	// point at a different message. Never infer identity from receipt text.
	link := "https://teams.microsoft.com/l/message/" + url.PathEscape(chat) + "/" + url.PathEscape(id) + "?context=%7B%22contextType%22%3A%22chat%22%7D"
	if hasConfirmationLink(content, link) {
		return "", nil
	}
	part.Body.Content = strings.Replace(part.Body.Content, "<p>", `<p><a href="`+esc(link)+`">`, 1)
	part.Body.Content = strings.Replace(part.Body.Content, "</p>", "</a></p>", 1)
	if !isHTML && len(atts) > 0 && strings.TrimSpace(content) != "" {
		return refuse()
	}
	content = asHTML(content, isHTML)
	// Voice cards sometimes have no placeholder. Preserve them exactly as the
	// existing transcript edit does; all other attachments must already be shown.
	check := append([]graphAttachment(nil), atts...)
	for i, at := range check {
		if !isMediaCard(at) {
			continue
		}
		if strings.TrimSpace(at.ID) == "" {
			return refuse()
		}
		shown := false
		for _, tag := range attachmentTagRE.FindAllStringSubmatch(content, -1) {
			if tag[1] == at.ID {
				shown = true
			}
		}
		if !shown {
			content += `<attachment id="` + esc(at.ID) + `"></attachment>`
		}
		check[i].ContentType = "reference" // validation only; raw cards are sent unchanged
		check[i].Content = ""              // the preserved media card owns its hosted URL
	}
	if attachmentContext(content, check) && !losslessAttachments(content, check) {
		return refuse()
	}
	next := content + "<hr/>" + part.Body.Content
	if len(next)+markerReserve > maxPartBytes {
		return refuse()
	}
	return next, nil
}

// Tokenize attributes, rather than comparing serialized HTML: Teams can change
// quoting, entity escaping and attribute order without changing the link.
func hasConfirmationLink(content, link string) bool {
	z := html.NewTokenizer(strings.NewReader(content))
	for {
		switch z.Next() {
		case html.ErrorToken:
			return false
		case html.StartTagToken:
			t := z.Token()
			if t.Data != "a" {
				continue
			}
			for _, a := range t.Attr {
				if a.Key == "href" && a.Val == link {
					return true
				}
			}
		}
	}
}
