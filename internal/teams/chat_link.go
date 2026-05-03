package teams

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var teamsChatIDPattern = regexp.MustCompile(`19:[A-Za-z0-9._~+=%:-]+@thread\.(?:v2|tacv2)`)

// ExtractChatID returns a Teams chat id from a raw chat id, Teams chat URL, or
// copied meeting/chat link. It intentionally treats Teams URLs as opaque and
// only extracts the stable Graph chat id embedded in the link.
func ExtractChatID(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("Teams chat id or link is required")
	}
	if decoded, err := url.QueryUnescape(value); err == nil {
		value = decoded
	}
	if teamsChatIDPattern.MatchString(value) {
		return teamsChatIDPattern.FindString(value), nil
	}
	return "", fmt.Errorf("could not find a Teams chat id in %q", redactLongInput(raw))
}

func TeamsChatURL(chatID string, tenantID string) string {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ""
	}
	link := "https://teams.microsoft.com/l/chat/" + url.QueryEscape(chatID) + "/0"
	if tenantID = strings.TrimSpace(tenantID); tenantID != "" {
		link += "?tenantId=" + url.QueryEscape(tenantID)
	}
	return link
}

func redactLongInput(raw string) string {
	value := strings.TrimSpace(raw)
	if len(value) <= 96 {
		return value
	}
	return value[:48] + "..." + value[len(value)-24:]
}
