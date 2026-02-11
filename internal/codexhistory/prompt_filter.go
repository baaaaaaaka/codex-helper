package codexhistory

import "strings"

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
