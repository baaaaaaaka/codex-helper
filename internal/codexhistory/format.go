package codexhistory

import "strings"

// TODO(phase2): Implement session formatting for TUI preview

func FormatSession(s Session) string {
	// TODO(phase2): format session details for display
	var b strings.Builder
	b.WriteString("Session: ")
	b.WriteString(s.SessionID)
	b.WriteString("\n")
	if s.Summary != "" {
		b.WriteString(s.Summary)
		b.WriteString("\n")
	}
	return b.String()
}

func FormatMessages(msgs []Message, maxLen int) string {
	// TODO(phase2): format messages for preview pane
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString("[")
		b.WriteString(m.Role)
		b.WriteString("] ")
		b.WriteString(m.Content)
		b.WriteString("\n\n")
	}
	return b.String()
}
