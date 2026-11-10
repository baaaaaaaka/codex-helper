package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// In-place annotations (channel.Annotator), as v0.1.20 made them
// (annotateIncomingUserMessage, annotateIncomingUserMessageWithASRTranscripts,
// appendFreezeNoticeToLatestMessage): one GET of the message, then one
// PATCH that sends the message's attachments and mentions back unchanged.
//
// The user label is added only when the edit is lossless: no hosted
// images, no cards, and every attachment a reference whose <attachment>
// placeholder is in the body exactly once (messageAttachmentContextLosslessPatchable).
// The freeze notice additionally accepts the formula images this helper
// renders (alt "Rendered TeX formula", hostedContentOnlyMessageContext).
// The transcript edit keeps the voice card and adds its placeholder when
// the body lacks it.

var (
	attachmentTagRE    = regexp.MustCompile(`(?i)<attachment\b[^>]*\bid\s*=\s*"([^"]*)"`)
	imgTagRE           = regexp.MustCompile(`(?i)<img\b[^>]*>`)
	imgAltRE           = regexp.MustCompile(`(?i)\balt\s*=\s*"([^"]*)"`)
	leadingParagraphRE = regexp.MustCompile(`(?is)^\s*<p\b[^>]*>.*?</p\s*>`)
)

const formulaAlt = "Rendered TeX formula"

type annotatedMessage struct {
	MessageType string  `json:"messageType"`
	Deleted     *string `json:"deletedDateTime"`
	From        *struct {
		User *graphIdentity `json:"user"`
	} `json:"from"`
	Body struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	} `json:"body"`
	Attachments []json.RawMessage `json:"attachments"`
	Mentions    []json.RawMessage `json:"mentions"`
}

// Annotate implements channel.Annotator.
func (a *Adapter) Annotate(ctx context.Context, chat, id string, an channel.Annotation) error {
	if strings.TrimSpace(id) == "" {
		return &channel.Rejected{Permanent: true, Err: fmt.Errorf("annotate needs a message id: %w", channel.ErrNotEditable)}
	}
	me, err := a.me(ctx)
	if err != nil {
		return err
	}
	var m annotatedMessage
	if err := a.g.getJSON(ctx, messagePath(chat, id), smallBodyLimit, &m); err != nil {
		var rejected *channel.Rejected
		if an.Confirmation != nil && errors.As(err, &rejected) && rejected.Permanent {
			return fmt.Errorf("confirmation read refused (%v): %w", err, channel.ErrNotEditable)
		}
		return err
	}
	if m.Deleted != nil && *m.Deleted != "" || m.MessageType != "" && m.MessageType != "message" {
		return fmt.Errorf("message %s is not a visible message: %w", id, channel.ErrNotEditable)
	}
	if m.From == nil || m.From.User == nil || !strings.EqualFold(m.From.User.ID, me.ID) {
		return fmt.Errorf("message %s is not the signed-in account's: %w", id, channel.ErrNotEditable)
	}
	atts := make([]graphAttachment, 0, len(m.Attachments))
	for _, raw := range m.Attachments {
		var at graphAttachment
		_ = json.Unmarshal(raw, &at)
		atts = append(atts, at)
	}
	content := strings.TrimSpace(m.Body.Content)
	isHTML := m.Body.ContentType == "" || strings.EqualFold(m.Body.ContentType, "html")
	var next string
	switch {
	case an.Confirmation != nil:
		next, err = confirmationEdit(content, isHTML, atts, an)
	case len(an.Clips) > 0:
		next, err = transcriptEdit(content, isHTML, atts, an)
	case an.Notice != nil:
		next, err = a.noticeEdit(ctx, content, isHTML, atts, an)
	default:
		next, err = labelEdit(content, isHTML, atts, an.Label)
	}
	if err != nil || next == "" {
		return err
	}
	in := map[string]any{"body": map[string]string{"contentType": "html", "content": next}}
	if len(m.Attachments) > 0 {
		in["attachments"] = m.Attachments
	}
	if len(m.Mentions) > 0 {
		in["mentions"] = m.Mentions
	}
	err = a.g.writeJSON(ctx, http.MethodPatch, messagePath(chat, id), in, nil)
	// A definite refusal can safely fall back to POST; uncertain PATCH outcomes
	// must instead retry the idempotent confirmation.
	var rejected *channel.Rejected
	if an.Confirmation != nil && errors.As(err, &rejected) && rejected.Permanent {
		return fmt.Errorf("confirmation edit refused (%v): %w", err, channel.ErrNotEditable)
	}
	return err
}

// labelHTML is the label line v0.1.20 put above an annotated message.
func labelHTML(label string) string {
	return "<p><strong>" + esc(label) + ":</strong></p>"
}

// hasLabel reports whether the message text already starts with label
// (splitLeadingUserAnnotationPrefix).
func hasLabel(content, label string) bool {
	if label == "" {
		return false
	}
	text := strings.TrimSpace(plainText(content))
	return strings.HasPrefix(text, label+":") || strings.HasPrefix(text, label+" [part ")
}

// asHTML is a body as HTML: a text body is escaped, line breaks kept.
func asHTML(content string, isHTML bool) string {
	if isHTML {
		return content
	}
	content = strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(content)
	return "<p>" + strings.ReplaceAll(esc(content), "\n", "<br>") + "</p>"
}

func contentType(at graphAttachment) string {
	return strings.ToLower(strings.TrimSpace(strings.Split(at.ContentType, ";")[0]))
}

func isMediaCard(at graphAttachment) bool {
	switch contentType(at) {
	case "application/vnd.microsoft.card.audio", "application/vnd.microsoft.card.video":
		return true
	}
	return false
}

// attachmentContext: the message carries attachments, hosted images or
// attachment placeholders (messageHasTeamsAttachmentContext).
func attachmentContext(content string, atts []graphAttachment) bool {
	return len(atts) > 0 || hostedRE.MatchString(content) || strings.Contains(strings.ToLower(content), "<attachment")
}

// losslessAttachments: every attachment is a reference shown by exactly
// one placeholder, and there are no hosted images or cards
// (messageAttachmentContextLosslessPatchable).
func losslessAttachments(content string, atts []graphAttachment) bool {
	if len(atts) == 0 || hostedRE.MatchString(content) {
		return false
	}
	ids := attachmentTagRE.FindAllStringSubmatch(content, -1)
	if len(ids) == 0 || len(ids) != len(atts) {
		return false
	}
	seen := map[string]bool{}
	for _, m := range ids {
		id := strings.TrimSpace(m[1])
		if id == "" || seen[id] {
			return false
		}
		seen[id] = true
	}
	for _, at := range atts {
		if hostedRE.MatchString(at.Content) {
			return false
		}
		switch contentType(at) {
		case "reference", "messagereference", "forwardedmessagereference":
		default:
			return false
		}
		if !seen[strings.TrimSpace(at.ID)] {
			return false
		}
	}
	return true
}

// formulaImagesOnly: the only attachment context is hosted images, each an
// image this helper rendered for a formula (hostedContentOnlyMessageContext).
func formulaImagesOnly(content string, atts []graphAttachment) bool {
	if len(atts) > 0 || strings.Contains(strings.ToLower(content), "<attachment") {
		return false
	}
	ids := hostedRE.FindAllStringSubmatch(content, -1)
	if len(ids) == 0 {
		return false
	}
	covered := map[string]bool{}
	for _, tag := range imgTagRE.FindAllString(content, -1) {
		srcs := hostedRE.FindAllStringSubmatch(tag, -1)
		if len(srcs) == 0 {
			continue
		}
		alt := imgAltRE.FindStringSubmatch(tag)
		if alt == nil || strings.TrimSpace(alt[1]) != formulaAlt {
			return false
		}
		for _, s := range srcs {
			covered[s[1]] = true
		}
	}
	for _, id := range ids {
		if !covered[id[1]] {
			return false
		}
	}
	return true
}

// labelEdit is the user marker (userMarkerPrependMessageHTML).
func labelEdit(content string, isHTML bool, atts []graphAttachment, label string) (string, error) {
	if strings.TrimSpace(label) == "" || content == "" || hasLabel(content, label) {
		return "", nil
	}
	for _, at := range atts {
		if isMediaCard(at) {
			return "", fmt.Errorf("a voice or video message gets its transcript instead: %w", channel.ErrNotEditable)
		}
	}
	if attachmentContext(content, atts) {
		if !losslessAttachments(content, atts) || !isHTML {
			return "", fmt.Errorf("the edit could lose an attachment: %w", channel.ErrNotEditable)
		}
	} else if strings.TrimSpace(plainText(content)) == "" {
		return "", nil
	}
	return labelHTML(label) + asHTML(content, isHTML), nil
}

// transcriptEdit adds the transcripts of a voice message
// (userAnnotatedASRTranscriptMessageHTML).
func transcriptEdit(content string, isHTML bool, atts []graphAttachment, an channel.Annotation) (string, error) {
	if content != "" {
		content = asHTML(content, isHTML)
	}
	if an.ClipsLabel != "" && strings.Contains(plainText(content), an.ClipsLabel) {
		return "", nil
	}
	var b strings.Builder
	if an.Label != "" && !hasLabel(content, an.Label) {
		b.WriteString(labelHTML(an.Label))
	}
	b.WriteString(content)
	shown := map[string]bool{}
	for _, m := range attachmentTagRE.FindAllStringSubmatch(content, -1) {
		shown[strings.TrimSpace(m[1])] = true
	}
	for _, at := range atts {
		id := strings.TrimSpace(at.ID)
		if !isMediaCard(at) || id == "" || shown[id] {
			continue
		}
		shown[id] = true
		b.WriteString(`<attachment id="` + esc(id) + `"></attachment>`)
	}
	b.WriteString(clipsHTML(an.ClipsLabel, an.Clips))
	return b.String(), nil
}

// clipsHTML renders transcripts as v0.1.20 did (asrTranscriptAnnotationHTML).
func clipsHTML(label string, clips []channel.Clip) string {
	var b strings.Builder
	b.WriteString("<p><strong>" + esc(label) + ":</strong></p>")
	for i, c := range clips {
		name := "Clip"
		if len(clips) > 1 {
			name = fmt.Sprintf("Clip %d", i+1)
		}
		clip := strings.TrimSpace(c.Name)
		if clip == "" {
			clip = fmt.Sprintf("media-%03d", i+1)
		}
		b.WriteString("<p><em>" + esc(name+": "+clip) + "</em></p>")
		text := strings.TrimSpace(c.Text)
		if text == "" {
			text = "(empty transcript)"
		}
		for _, line := range strings.Split(text, "\n") {
			if line = strings.TrimRight(line, " \t"); strings.TrimSpace(line) != "" {
				b.WriteString("<p>" + esc(line) + "</p>")
			}
		}
	}
	return b.String()
}

// noticeEdit appends a rendered notice (appendFreezeNoticeToMessageHTML).
func (a *Adapter) noticeEdit(ctx context.Context, content string, isHTML bool, atts []graphAttachment, an channel.Annotation) (string, error) {
	if content == "" {
		return "", fmt.Errorf("the message is empty: %w", channel.ErrNotEditable)
	}
	if len(an.Present) > 0 {
		text := strings.Join(strings.Fields(plainText(content)), " ")
		all := true
		for _, p := range an.Present {
			if !strings.Contains(text, strings.Join(strings.Fields(p), " ")) {
				all = false
				break
			}
		}
		if all {
			return "", nil
		}
	}
	if attachmentContext(content, atts) && !losslessAttachments(content, atts) && !formulaImagesOnly(content, atts) {
		return "", fmt.Errorf("the edit could lose an attachment: %w", channel.ErrNotEditable)
	}
	doc := *an.Notice
	doc.Mention, doc.ReplyTo, doc.Files = false, "", nil
	parts, err := a.Render(ctx, doc)
	if err != nil {
		return "", err
	}
	if len(parts) != 1 {
		return "", fmt.Errorf("the notice does not fit one message: %w", channel.ErrNotEditable)
	}
	var p struct {
		Body struct {
			Content string `json:"content"`
		} `json:"body"`
		Hosted []json.RawMessage `json:"hostedContents"`
	}
	if err := json.Unmarshal([]byte(parts[0].Body), &p); err != nil || len(p.Hosted) > 0 {
		return "", fmt.Errorf("the notice cannot be added by an edit: %w", channel.ErrNotEditable)
	}
	return asHTML(content, isHTML) + "<p>&nbsp;</p>" + p.Body.Content, nil
}
