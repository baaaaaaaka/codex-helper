package codexhistory

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"
)

type Message struct {
	Role      string
	Content   string
	Timestamp time.Time
}

func ReadSessionMessages(filePath string, maxMessages int) ([]Message, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ring []Message
	reader := bufio.NewReaderSize(f, 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			for _, msg := range parseLineMessages(line) {
				appendMessage(&ring, msg, maxMessages)
			}
		}
		if err == io.EOF {
			break
		}
	}
	return ring, nil
}

func appendMessage(ring *[]Message, msg Message, maxMessages int) {
	if maxMessages > 0 && len(*ring) >= maxMessages {
		*ring = append((*ring)[1:], msg)
		return
	}
	*ring = append(*ring, msg)
}

func parseLineMessages(line []byte) []Message {
	var env codexEnvelope
	if json.Unmarshal(line, &env) != nil {
		return nil
	}
	ts := parseTimestamp(env.Timestamp)

	switch env.Type {
	case "response_item":
		return parseResponseItem(env.Payload, ts)
	case "event_msg":
		return parseEventMsg(env.Payload, ts)
	}
	return nil
}

// parseResponseItem handles the main message types in Codex sessions.
func parseResponseItem(raw json.RawMessage, ts time.Time) []Message {
	var header struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &header) != nil {
		return nil
	}

	switch header.Type {
	case "message":
		var payload codexResponsePayload
		if json.Unmarshal(raw, &payload) != nil {
			return nil
		}
		return parseMessagePayload(payload, ts)
	case "function_call":
		return parseFunctionCall(raw, ts)
	case "function_call_output":
		return parseFunctionCallOutput(raw, ts)
	case "custom_tool_call":
		var payload codexResponsePayload
		if json.Unmarshal(raw, &payload) != nil {
			return nil
		}
		return parseCustomToolCall(payload, ts)
	case "custom_tool_call_output":
		var payload codexResponsePayload
		if json.Unmarshal(raw, &payload) != nil {
			return nil
		}
		return parseCustomToolCallOutput(payload, ts)
	case "reasoning":
		return parseReasoning(raw, ts)
	}
	return nil
}

func parseMessagePayload(payload codexResponsePayload, ts time.Time) []Message {
	role := strings.ToLower(payload.Role)

	// Skip developer/system messages
	if role != "user" && role != "assistant" {
		return nil
	}

	text := extractContentText(payload.Content)
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	// Skip system-injected user messages (AGENTS.md, environment_context, etc.)
	if role == "user" && shouldSkipFirstPrompt(text) {
		return nil
	}

	displayRole := role
	if role == "assistant" {
		phase := strings.ToLower(payload.Phase)
		if phase == "commentary" {
			displayRole = "assistant_commentary"
		}
	}

	return []Message{{Role: displayRole, Content: text, Timestamp: ts}}
}

func parseFunctionCall(raw json.RawMessage, ts time.Time) []Message {
	var fc struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	if json.Unmarshal(raw, &fc) != nil {
		return nil
	}
	name := fc.Name
	if name == "" {
		name = "function_call"
	}

	label := "Tool: " + name
	if fc.Arguments != "" {
		var parsed any
		if json.Unmarshal([]byte(fc.Arguments), &parsed) == nil {
			if formatted, err := json.MarshalIndent(parsed, "", "  "); err == nil {
				label += "\n" + string(formatted)
			} else {
				label += "\n" + fc.Arguments
			}
		} else {
			label += "\n" + fc.Arguments
		}
	}

	return []Message{{Role: "tool", Content: label, Timestamp: ts}}
}

func parseFunctionCallOutput(raw json.RawMessage, ts time.Time) []Message {
	var fco struct {
		Output string `json:"output"`
	}
	if json.Unmarshal(raw, &fco) != nil {
		return nil
	}
	text := strings.TrimSpace(fco.Output)
	if text == "" {
		return nil
	}
	return []Message{{Role: "tool_result", Content: text, Timestamp: ts}}
}

func parseCustomToolCall(payload codexResponsePayload, ts time.Time) []Message {
	name := payload.Name
	if name == "" {
		name = "custom_tool"
	}
	label := "Tool: " + name
	text := extractContentText(payload.Content)
	if text != "" {
		label += "\n" + strings.TrimSpace(text)
	}
	return []Message{{Role: "tool", Content: label, Timestamp: ts}}
}

func parseCustomToolCallOutput(payload codexResponsePayload, ts time.Time) []Message {
	text := extractContentText(payload.Content)
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	return []Message{{Role: "tool_result", Content: text, Timestamp: ts}}
}

func parseReasoning(raw json.RawMessage, ts time.Time) []Message {
	var reasoning struct {
		Type    string `json:"type"`
		Summary []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"summary"`
	}
	if json.Unmarshal(raw, &reasoning) != nil {
		return nil
	}
	var parts []string
	for _, item := range reasoning.Summary {
		if text := strings.TrimSpace(item.Text); text != "" {
			parts = append(parts, text)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return []Message{{Role: "thinking", Content: strings.Join(parts, "\n"), Timestamp: ts}}
}

func parseEventMsg(raw json.RawMessage, ts time.Time) []Message {
	var event struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return nil
	}

	// Only extract user_message as a fallback for sessions without response_item/user
	if event.Type == "user_message" {
		var userMsg struct {
			Content string `json:"content"`
		}
		if json.Unmarshal(raw, &userMsg) == nil {
			text := strings.TrimSpace(userMsg.Content)
			if text != "" {
				return []Message{{Role: "user", Content: text, Timestamp: ts}}
			}
		}
	}
	return nil
}
