package channel

import "strings"

// WithLinks adds named destinations to model input without changing the visible
// text used for command parsing or echo detection. Channels validate URLs when
// extracting them. Persist the result with the prompt, so recovery needs no fetch.
func WithLinks(text string, links []Link) string {
	if len(links) == 0 {
		return text
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(text))
	b.WriteString("\n\nLinks in this message:")
	for _, link := range links {
		b.WriteString("\n- ")
		label := strings.Join(strings.Fields(link.Text), " ")
		if label != "" {
			b.WriteString(label)
			b.WriteString(": ")
		}
		b.WriteString(link.URL)
	}
	return strings.TrimSpace(b.String())
}
