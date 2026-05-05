package codexhistory

import "strings"

var teamsHelperPromptSuffixMarkers = []string{
	"Teams helper safety:",
	"If you need to return generated files or images to the Teams user,",
	"Then include this fenced manifest in your final answer:",
}

func shouldSkipFirstPrompt(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return true
	}
	// XML-wrapped system content: <environment_context>...</environment_context> etc.
	if strings.HasPrefix(trimmed, "<") && strings.HasSuffix(trimmed, ">") {
		return true
	}
	// Codex injects AGENTS.md skill instructions as a user message
	if strings.HasPrefix(trimmed, "# AGENTS.md") {
		return true
	}
	// Instruction blocks sometimes appear unwrapped
	if strings.Contains(trimmed, "<INSTRUCTIONS>") {
		return true
	}
	return false
}

func firstPromptTitleText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	for _, marker := range teamsHelperPromptSuffixMarkers {
		if strings.HasPrefix(text, marker) {
			return ""
		}
		for _, prefix := range []string{"\n\n", "\n"} {
			if idx := strings.Index(text, prefix+marker); idx >= 0 {
				text = strings.TrimSpace(text[:idx])
				break
			}
		}
	}
	return text
}
