package teams

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/net/html"
	"net/http"
	"regexp"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// In-place annotations (channel.Annotator), as v0.1.20 made them
// (annotateIncomingUserMessage, annotateIncomingUserMessageWithASRTranscripts,
// appendFreezeNoticeToLatestMessage): one GET of the message, then one
// PATCH that sends the message's attachments and mentions back unchanged.
// Existing hosted images are preserved by retaining their original HTML.
// Cards remain restricted to the supported reference and voice/video forms.
var leadingParagraphRE = regexp.MustCompile(`(?is)^\s*<p\b[^>]*>.*?</p\s*>`)
var splitLabelRE = regexp.MustCompile(`^ \[part [1-9][0-9]*/[1-9][0-9]*\]:$`)

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
		next, err = confirmationEdit(chat, id, content, isHTML, atts, an)
	case len(an.Clips) > 0:
		next, err = transcriptEdit(chat, id, content, isHTML, atts, an)
	case an.Notice != nil:
		if an.Key == "" {
			return fmt.Errorf("notice needs an event key: %w", channel.ErrNotEditable)
		}
		an.Key = annotationLink(chat, id, an.Key)
		next, err = a.noticeEdit(ctx, content, isHTML, atts, an)
	default:
		next, err = labelEdit(content, isHTML, atts, an.Label)
	}
	if err != nil || next == "" {
		return err
	}
	if len(next)+markerReserve > maxPartBytes {
		return fmt.Errorf("annotation exceeds message size: %w", channel.ErrNotEditable)
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
	matches := func(text string) bool {
		text = strings.TrimSpace(text)
		return text == label+":" || strings.HasPrefix(text, label) && splitLabelRE.MatchString(strings.TrimPrefix(text, label))
	}
	if !strings.HasPrefix(strings.TrimSpace(content), "<") {
		first, _, _ := strings.Cut(content, "\n")
		return matches(first)
	}
	// Read the first visible block through harmless wrappers. Never accept a
	// code example, quote, or partial first paragraph as the helper's label.
	z := html.NewTokenizer(strings.NewReader(content))
	var text strings.Builder
	for {
		switch z.Next() {
		case html.ErrorToken:
			return false
		case html.TextToken:
			text.Write(z.Text())
		case html.StartTagToken, html.SelfClosingTagToken:
			switch z.Token().Data {
			case "blockquote", "pre", "code", "img", "attachment", "hr":
				return false
			case "br":
				if strings.TrimSpace(text.String()) != "" {
					return matches(text.String())
				}
			}
		case html.EndTagToken:
			switch z.Token().Data {
			case "p", "div":
				if strings.TrimSpace(text.String()) != "" {
					return matches(text.String())
				}
			}
		}
	}
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

// attachmentIDs reads only actual placeholders, decoding HTML attributes.
// The original HTML is never reserialized. Images need no upload or rewrite.
func attachmentIDs(content string) map[string]int {
	ids := map[string]int{}
	z := html.NewTokenizer(strings.NewReader(content))
	for {
		switch z.Next() {
		case html.ErrorToken:
			return ids
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			if t.Data == "attachment" {
				ids[strings.TrimSpace(attr(t, "id"))]++
			}
		}
	}
}

func attr(t html.Token, key string) string {
	for _, a := range t.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

// Every supported attachment must retain its placeholder. Inline
// images are already in the unchanged body and do not make an edit unsafe.
func losslessAttachments(content string, atts []graphAttachment) bool {
	ids := attachmentIDs(content)
	for _, at := range atts {
		id := strings.TrimSpace(at.ID)
		if id == "" || ids[id] < 1 {
			return false
		}
		ids[id]--
		switch contentType(at) {
		case "reference", "messagereference", "forwardedmessagereference":
		default:
			return false
		}
	}
	for _, count := range ids {
		if count != 0 {
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
	if len(atts) > 0 || isHTML {
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
func transcriptEdit(chat, id, content string, isHTML bool, atts []graphAttachment, an channel.Annotation) (string, error) {
	if content != "" {
		content = asHTML(content, isHTML)
	}
	clips := clipsHTML(an.ClipsLabel, an.Clips)
	link := annotationLink(chat, id, fmt.Sprintf("transcript:%x", sha256.Sum256([]byte(clips))))
	if hasAnnotationStamp(content, link, "Clip", "") || hasAnnotationStamp(content, link, "Clip:", "") {
		if an.Label != "" && !hasLabel(content, an.Label) {
			return labelHTML(an.Label) + content, nil
		}
		return "", nil
	}
	clips = strings.Replace(clips, "<em>", `<em><a href="`+esc(link)+`">`, 1)
	clips = strings.Replace(clips, "</em>", "</a></em>", 1)
	var b strings.Builder
	if an.Label != "" && !hasLabel(content, an.Label) {
		b.WriteString(labelHTML(an.Label))
	}
	user, confirmation := splitPromptConfirmation(content, annotationLink(chat, id, ""))
	b.WriteString(user)
	shown := attachmentIDs(content)
	for _, at := range atts {
		id := strings.TrimSpace(at.ID)
		if !isMediaCard(at) || id == "" || shown[id] > 0 {
			continue
		}
		shown[id] = 1
		b.WriteString(`<attachment id="` + esc(id) + `"></attachment>`)
	}
	b.WriteString(clips)
	b.WriteString(confirmation)
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
	if an.Key == "" {
		return "", fmt.Errorf("notice needs an event identity: %w", channel.ErrNotEditable)
	}
	if hasAnnotationStamp(content, an.Key, "Paused at", "") {
		return "", nil
	}
	if !losslessAttachments(content, atts) {
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
	return asHTML(content, isHTML) + "<p>&nbsp;</p>" + p.Body.Content + timeStamp("Paused at", an.Key, an.At), nil
}
