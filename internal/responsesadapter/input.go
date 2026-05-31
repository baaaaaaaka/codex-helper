package responsesadapter

import (
	"encoding/json"
	"fmt"
	"strings"
)

type parsedInput struct {
	Text     string
	Messages []ProviderMessage
}

func parseInput(raw json.RawMessage) (parsedInput, error) {
	if len(raw) == 0 {
		return parsedInput{}, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if strings.TrimSpace(text) == "" {
			return parsedInput{}, nil
		}
		return parsedInput{
			Text:     text,
			Messages: []ProviderMessage{{Role: "user", Content: text}},
		}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return parsedInput{}, fmt.Errorf("input must be a string or an array")
	}
	var parsed parsedInput
	for _, itemRaw := range items {
		message, textPart, ok, err := parseInputItem(itemRaw)
		if err != nil {
			return parsedInput{}, err
		}
		if !ok {
			continue
		}
		parsed.Messages = append(parsed.Messages, message)
		if textPart != "" {
			if parsed.Text != "" {
				parsed.Text += "\n"
			}
			parsed.Text += textPart
		}
	}
	parsed.Messages = normalizeProviderMessages(parsed.Messages)
	return parsed, nil
}

func parseInputItem(raw json.RawMessage) (ProviderMessage, string, bool, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ProviderMessage{}, "", false, nil
	}
	itemType := rawString(obj["type"])
	switch itemType {
	case "", "message":
		role := firstNonEmpty(rawString(obj["role"]), "user")
		text, parts := extractMessageContent(obj["content"])
		if strings.TrimSpace(text) == "" {
			return ProviderMessage{}, "", false, nil
		}
		textPart := ""
		if role == "user" || role == "developer" || role == "system" {
			textPart = text
		}
		return ProviderMessage{Role: role, Content: text, ContentParts: parts}, textPart, true, nil
	case "function_call":
		callID := rawString(obj["call_id"])
		name := rawString(obj["name"])
		if callID == "" || name == "" {
			return ProviderMessage{}, "", false, fmt.Errorf("function_call input requires call_id and name")
		}
		return ProviderMessage{
			Role: "assistant",
			ToolCalls: []ToolCallRecord{{
				ID:        callID,
				Name:      name,
				Arguments: sanitizeToolArguments(rawString(obj["arguments"])),
				Status:    "completed",
			}},
		}, "", true, nil
	case "reasoning":
		reasoning := extractReasoningInputText(obj)
		if strings.TrimSpace(reasoning) == "" {
			return ProviderMessage{}, "", false, nil
		}
		return ProviderMessage{Role: "assistant", ReasoningContent: reasoning}, "", true, nil
	case "function_call_output", "custom_tool_call_output", "mcp_tool_call_output":
		callID := rawString(obj["call_id"])
		if callID == "" {
			return ProviderMessage{}, "", false, fmt.Errorf("%s input requires call_id", itemType)
		}
		return ProviderMessage{
			Role:       "tool",
			ToolCallID: callID,
			Content:    extractToolOutputText(obj["output"]),
		}, "", true, nil
	default:
		return ProviderMessage{}, "", false, nil
	}
}

func normalizeProviderMessages(messages []ProviderMessage) []ProviderMessage {
	if len(messages) == 0 {
		return nil
	}
	merged := make([]ProviderMessage, 0, len(messages))
	for _, message := range messages {
		if message.Role == "assistant" && len(merged) > 0 && merged[len(merged)-1].Role == "assistant" {
			last := &merged[len(merged)-1]
			if strings.TrimSpace(message.Content) != "" {
				if strings.TrimSpace(last.Content) != "" {
					last.Content += "\n"
				}
				last.Content += message.Content
			}
			if strings.TrimSpace(message.ReasoningContent) != "" {
				if strings.TrimSpace(last.ReasoningContent) != "" {
					last.ReasoningContent += "\n\n"
				}
				last.ReasoningContent += message.ReasoningContent
			}
			last.ToolCalls = append(last.ToolCalls, message.ToolCalls...)
			if len(message.ContentParts) > 0 {
				last.ContentParts = append(last.ContentParts, message.ContentParts...)
			}
			continue
		}
		merged = append(merged, message)
	}

	normalized := make([]ProviderMessage, 0, len(merged))
	for i := 0; i < len(merged); i++ {
		message := merged[i]
		if message.Role == "assistant" && strings.TrimSpace(message.Content) == "" && strings.TrimSpace(message.ReasoningContent) != "" && len(message.ToolCalls) == 0 {
			continue
		}
		normalized = append(normalized, message)
		if message.Role != "assistant" || len(message.ToolCalls) == 0 {
			continue
		}
		seen := map[string]bool{}
		j := i + 1
		for j < len(merged) && merged[j].Role == "tool" {
			if merged[j].ToolCallID != "" {
				seen[merged[j].ToolCallID] = true
			}
			normalized = append(normalized, merged[j])
			j++
		}
		for _, call := range message.ToolCalls {
			if call.ID != "" && !seen[call.ID] {
				normalized = append(normalized, ProviderMessage{
					Role:       "tool",
					ToolCallID: call.ID,
					Content:    "Tool output missing.",
				})
			}
		}
		i = j - 1
	}
	return normalized
}

func buildProviderMessages(history []ResponseRecord, inputMessages []ProviderMessage) []ProviderMessage {
	messages := make([]ProviderMessage, 0, len(history)*2+len(inputMessages))
	for _, record := range history {
		if len(record.InputMessages) > 0 {
			messages = append(messages, record.InputMessages...)
		} else if strings.TrimSpace(record.InputText) != "" {
			messages = append(messages, ProviderMessage{Role: "user", Content: record.InputText})
		}
		if strings.TrimSpace(record.OutputText) != "" || strings.TrimSpace(record.ReasoningText) != "" || len(record.ToolCalls) > 0 {
			messages = append(messages, ProviderMessage{
				Role:             "assistant",
				Content:          record.OutputText,
				ReasoningContent: record.ReasoningText,
				ToolCalls:        record.ToolCalls,
			})
		}
	}
	messages = append(messages, inputMessages...)
	return messages
}

func validateToolMessageLinks(inputMessages []ProviderMessage, history []ResponseRecord) error {
	knownCalls := map[string]bool{}
	for _, record := range history {
		for _, call := range record.ToolCalls {
			if call.ID != "" {
				knownCalls[call.ID] = true
			}
		}
	}
	for _, message := range inputMessages {
		for _, call := range message.ToolCalls {
			if call.ID != "" {
				knownCalls[call.ID] = true
			}
		}
		if message.Role != "tool" {
			continue
		}
		if message.ToolCallID == "" {
			return fmt.Errorf("tool message requires tool_call_id")
		}
		if !knownCalls[message.ToolCallID] {
			return fmt.Errorf("function_call_output without matching function_call: %s", message.ToolCallID)
		}
	}
	return nil
}

func extractReasoningInputText(obj map[string]json.RawMessage) string {
	for _, key := range []string{"encrypted_content", "content"} {
		if text := extractContentText(obj[key]); text != "" {
			return text
		}
		if text := rawString(obj[key]); text != "" {
			return text
		}
	}
	if text := extractContentText(obj["summary"]); text != "" {
		return text
	}
	return ""
}

func extractToolOutputText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		if content := rawString(obj["content"]); content != "" {
			return content
		}
		if content := extractContentText(obj["content"]); content != "" {
			return content
		}
	}
	if text := extractContentText(raw); text != "" {
		return text
	}
	encoded, err := json.Marshal(json.RawMessage(raw))
	if err != nil {
		return ""
	}
	return string(encoded)
}

func rawString(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return ""
}
