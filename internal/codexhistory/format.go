package codexhistory

import (
	"strings"
	"time"
)

func FormatSession(s Session) string {
	var b strings.Builder
	b.WriteString("Session: ")
	b.WriteString(s.SessionID)
	b.WriteString("\n")
	if s.ProjectPath != "" {
		b.WriteString("Project: ")
		b.WriteString(s.ProjectPath)
		b.WriteString("\n")
	}
	if s.Summary != "" {
		b.WriteString("Summary: ")
		b.WriteString(s.Summary)
		b.WriteString("\n")
	}
	if !s.CreatedAt.IsZero() {
		b.WriteString("Created: ")
		b.WriteString(s.CreatedAt.Format(time.RFC3339))
		b.WriteString("\n")
	}
	if !s.ModifiedAt.IsZero() {
		b.WriteString("Modified: ")
		b.WriteString(s.ModifiedAt.Format(time.RFC3339))
		b.WriteString("\n")
	}
	b.WriteString("\n")

	if s.FilePath != "" {
		msgs, err := ReadSessionMessages(s.FilePath, 50)
		if err == nil && len(msgs) > 0 {
			b.WriteString(FormatMessages(msgs, 0))
			b.WriteString("\n")
		}
	}
	return b.String()
}

func FormatMessages(msgs []Message, maxLen int) string {
	var b strings.Builder
	for i, msg := range msgs {
		if i > 0 {
			b.WriteString("\n")
		}
		role := roleLabel(msg.Role)
		b.WriteString(role)
		b.WriteString(":\n")
		text := strings.TrimSpace(msg.Content)
		if maxLen > 0 {
			text = truncateRunes(text, maxLen)
		}
		b.WriteString(text)
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func roleLabel(role string) string {
	switch role {
	case "user":
		return "User"
	case "assistant":
		return "Assistant"
	case "assistant_commentary":
		return "Assistant (update)"
	case "tool":
		return "Tool"
	case "tool_result":
		return "Tool Result"
	case "thinking":
		return "Thinking"
	default:
		return "Message"
	}
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "…"
}
