package teams

import (
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"
	"unicode"
)

var (
	tagPattern        = regexp.MustCompile(`(?s)<[^>]*>`)
	lineBreakPattern  = regexp.MustCompile(`(?i)<br\s*/?>`)
	tableCellPattern  = regexp.MustCompile(`(?i)</(?:th|td)>`)
	tableRowPattern   = regexp.MustCompile(`(?i)</tr>`)
	blockClosePattern = regexp.MustCompile(`(?i)</(?:p|div|li|pre|table)>`)
)

func SanitizeTopic(topic string) string {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		topic = "codex session"
	}
	var b strings.Builder
	for _, r := range topic {
		if unicode.IsControl(r) {
			continue
		}
		switch r {
		case ':', '<', '>', '"', '/', '\\', '|', '?', '*':
			b.WriteByte('-')
		default:
			b.WriteRune(r)
		}
	}
	clean := strings.Join(strings.Fields(b.String()), " ")
	clean = strings.Trim(clean, " -.")
	if clean == "" {
		clean = "codex session"
	}
	const maxRunes = 120
	rs := []rune(clean)
	if len(rs) > maxRunes {
		clean = strings.TrimSpace(string(rs[:maxRunes]))
	}
	return clean
}

func SessionTopic(now time.Time, request string) string {
	request = strings.TrimSpace(request)
	if request == "" {
		request = "untitled"
	}
	return SanitizeTopic("codex " + now.Format("2006-01-02 150405") + " " + request)
}

func HTMLMessage(prefix string, text string) string {
	prefix = strings.TrimSpace(prefix)
	text = strings.TrimSpace(text)
	if prefix != "" {
		return "<p><strong>" + html.EscapeString(prefix) + ":</strong> " + html.EscapeString(text) + "</p>"
	}
	return "<p>" + html.EscapeString(text) + "</p>"
}

func PlainTextFromTeamsHTML(content string) string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = lineBreakPattern.ReplaceAllString(content, "\n")
	content = tableCellPattern.ReplaceAllString(content, "\t")
	content = tableRowPattern.ReplaceAllString(content, "\n")
	content = blockClosePattern.ReplaceAllString(content, "\n")
	content = tagPattern.ReplaceAllString(content, "")
	content = html.UnescapeString(content)
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func IsHelperText(text string) bool {
	text = strings.TrimSpace(strings.ToLower(text))
	return strings.HasPrefix(text, "codex:") ||
		strings.HasPrefix(text, "codex echo:") ||
		strings.HasPrefix(text, "codex-helper:")
}

func UnsupportedAttachmentMessage(attachments []MessageAttachment) string {
	count := len(attachments)
	if count == 0 {
		return ""
	}
	types := make([]string, 0, count)
	for _, attachment := range attachments {
		contentType := strings.TrimSpace(attachment.ContentType)
		if contentType == "" {
			contentType = "unknown"
		}
		types = append(types, contentType)
		if len(types) >= 3 {
			break
		}
	}
	detail := strings.Join(types, ", ")
	if count > len(types) {
		detail += fmt.Sprintf(", +%d more", count-len(types))
	}
	return fmt.Sprintf("I could not process %d Teams attachment(s): %s. Supported inputs are plain text plus Teams-hosted inline/reference file attachments that the current Graph scopes can download. For generated files from Codex, use `helper file <relative-path>` from the Teams outbound root.", count, detail)
}

func UnsupportedControlAttachmentMessage(attachments []MessageAttachment) string {
	count := len(attachments)
	if count == 0 {
		return ""
	}
	return UnsupportedAttachmentMessage(attachments) + " Files and images belong in a 💬 Work chat; the 🏠 control chat only handles text commands like `projects`, `new <directory>`, `continue <number>`, and `status`."
}
