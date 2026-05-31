package responsesadapter

import (
	"fmt"
	"strings"
)

func upstreamHTTPError(statusCode int, status string, snippet string, model string) error {
	if message := detectContextOverflow(statusCode, snippet, model); message != "" {
		return fmt.Errorf("%s", message)
	}
	if message := detectMalformedJSONField(statusCode, snippet); message != "" {
		return fmt.Errorf("%s", message)
	}
	return fmt.Errorf("upstream chat completions returned %s: %s", status, snippet)
}

func detectContextOverflow(statusCode int, snippet string, model string) string {
	if statusCode != 400 || strings.TrimSpace(snippet) == "" {
		return ""
	}
	lower := strings.ToLower(snippet)
	patterns := []string{
		"context_length_exceeded",
		"maximum context length",
		"context length exceeded",
		"prompt is too long",
		"input length and `max_tokens` exceed",
		"tokens exceeds the maximum",
		"上下文过长",
		"上下文长度超出",
	}
	for _, pattern := range patterns {
		if strings.Contains(lower, pattern) {
			prefix := "upstream context length exceeded; start a new Codex session or compact the conversation with /compact"
			if strings.TrimSpace(model) != "" {
				prefix = fmt.Sprintf("upstream context length exceeded for model %s; start a new Codex session or compact the conversation with /compact", strings.TrimSpace(model))
			}
			return prefix + ": " + snippet
		}
	}
	return ""
}

func detectMalformedJSONField(statusCode int, snippet string) string {
	if statusCode != 400 || strings.TrimSpace(snippet) == "" {
		return ""
	}
	lower := strings.ToLower(snippet)
	if !strings.Contains(lower, "unexpected end of data") && !strings.Contains(lower, "unterminated string") && !strings.Contains(lower, "malformed json") {
		return ""
	}
	return "upstream rejected a malformed JSON field, usually tool_call.arguments from a truncated model stream; retry after /compact or start a new Codex session: " + snippet
}
