package teamshtml

import (
	"html"
	"regexp"
	"strings"
)

var (
	tagPattern            = regexp.MustCompile(`(?s)<[^>]*>`)
	horizontalRulePattern = regexp.MustCompile(`(?i)<hr\s*/?>`)
	lineBreakPattern      = regexp.MustCompile(`(?i)<br\s*/?>`)
	tableCellPattern      = regexp.MustCompile(`(?i)</(?:th|td)>`)
	tableRowPattern       = regexp.MustCompile(`(?i)</tr>`)
	blockClosePattern     = regexp.MustCompile(`(?i)</(?:p|div|li|pre|table|blockquote)>`)
)

func PlainTextFromTeamsHTML(content string) string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = horizontalRulePattern.ReplaceAllString(content, "\n———\n")
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
