package teams

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

var confirmationSeparator = regexp.MustCompile(`(?i)<hr\b[^>]*>`)

// confirmationEdit appends one initial receipt. Its marker deliberately differs
// from the outbox own-post marker: the edited message still belongs to the user.
func confirmationEdit(content string, isHTML bool, atts []graphAttachment, an channel.Annotation) (string, error) {
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
	sum := sha256.Sum256([]byte(an.ConfirmationKey))
	marker := "<!-- cxp-prompt-confirmation:" + hex.EncodeToString(sum[:16]) + " -->"
	if strings.Contains(content, marker) {
		return "", nil
	}
	// Teams can strip comments or normalize markup. Recognize the complete
	// confirmation after a separator too, including when ASR was appended later.
	normalize := func(s string) string { return strings.Join(strings.Fields(plainText(s)), " ") }
	expected := normalize(part.Body.Content)
	sections := confirmationSeparator.Split(content, -1)
	for _, section := range sections[1:] {
		visible := normalize(section)
		if visible == expected || strings.HasPrefix(visible, expected+" ") {
			return "", nil
		}
	}
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
	next := content + "<hr/>" + marker + part.Body.Content
	if len(next)+markerReserve > maxPartBytes {
		return refuse()
	}
	return next, nil
}
