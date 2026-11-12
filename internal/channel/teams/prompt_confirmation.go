package teams

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"golang.org/x/net/html"
)

// splitPromptConfirmation keeps later user annotations above the receipt's
// rule. Match the Helper header and this message's stamp in the same section;
// ordinary rules, quoted examples and another message's receipt stay user HTML.
// Slice the original bytes so attachments and Teams HTML are not reserialized.
func splitPromptConfirmation(content, link string) (string, string) {
	z := html.NewTokenizer(strings.NewReader(content))
	offset, blocked, rule, afterRule := 0, 0, -1, 0
	linked := false
	confirmed := func(end int) bool {
		if rule < 0 || !linked {
			return false
		}
		section := content[afterRule:end]
		return hasLabel(section, "🔧 Helper") &&
			(hasAnnotationStamp(section, link, "Received at", "") ||
				// Older receipts linked the Helper header instead of the time.
				hasAnnotationStamp(section, link, "🔧", ""))
	}
	for {
		token := z.Next()
		start := offset
		offset += len(z.Raw())
		switch token {
		case html.ErrorToken:
			if confirmed(len(content)) {
				return content[:rule], content[rule:]
			}
			return content, ""
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			switch t.Data {
			case "blockquote", "pre", "code":
				blocked++
			case "a":
				linked = linked || blocked == 0 && attr(t, "href") == link
			case "hr":
				if blocked == 0 {
					if confirmed(start) {
						return content[:rule], content[rule:]
					}
					rule, afterRule = start, offset
					linked = false
				}
			}
		case html.EndTagToken:
			switch z.Token().Data {
			case "blockquote", "pre", "code":
				if blocked > 0 {
					blocked--
				}
			}
		}
	}
}

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
	link := annotationLink(chat, id, "")
	// Old receipts have a linked Helper header followed by this receipt's
	// first paragraph. An unrelated self-link or helper example is not proof.
	first := leadingParagraphRE.FindString(part.Body.Content)
	rest := strings.TrimSpace(strings.TrimPrefix(part.Body.Content, first))
	legacyText := strings.Join(strings.Fields(plainText(leadingParagraphRE.FindString(rest))), " ")
	hasReceipt := hasAnnotationStamp(content, link, "Received at", legacyText)
	for _, at := range atts {
		if isMediaCard(at) {
			an.Label = ""
			break
		} // transcripts own media labels
	}
	if hasReceipt && (an.Label == "" || hasLabel(content, an.Label)) {
		return "", nil // unchanged retries need no attachment scan or PATCH
	}
	if !isHTML && len(atts) > 0 && strings.TrimSpace(content) != "" {
		return refuse()
	}
	original := content
	content = asHTML(content, isHTML)
	// Voice cards sometimes have no placeholder. Preserve them exactly as the
	// existing transcript edit does; all other attachments must already be shown.
	check := append([]graphAttachment(nil), atts...)
	shown := attachmentIDs(content)
	for i, at := range check {
		if !isMediaCard(at) {
			continue
		}
		if strings.TrimSpace(at.ID) == "" {
			return refuse()
		}
		if shown[at.ID] == 0 {
			content += `<attachment id="` + esc(at.ID) + `"></attachment>`
		}
		check[i].ContentType = "reference" // validation only; raw cards are sent unchanged
		check[i].Content = ""              // the preserved media card owns its hosted URL
	}
	if !losslessAttachments(content, check) {
		return refuse()
	}
	body := part.Body.Content
	// New receipts persist their displayed time with the rendered outbox body.
	// Old pending outbox rows use their original creation time on delivery.
	if start := strings.LastIndex(body, "<p>Received at "); start >= 0 {
		stamp := strings.TrimSuffix(body[start+len("<p>Received at "):], "</p>")
		body = body[:start] + `<p>Received at <a href="` + esc(link) + `">` + stamp + `</a></p>`
	} else {
		body += timeStamp("Received at", link, an.At)
	}
	next := content
	if !hasReceipt {
		next += "<hr/>" + body
	}
	if an.Label != "" && !hasLabel(content, an.Label) {
		next = labelHTML(an.Label) + next
	}
	if next == original {
		return "", nil
	}
	if len(next)+markerReserve > maxPartBytes {
		return refuse()
	}
	return next, nil
}
