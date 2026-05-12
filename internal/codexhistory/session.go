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
	sourceID  string
}

func ReadSessionMessages(filePath string, maxMessages int) ([]Message, error) {
	return readSessionMessages(filePath, maxMessages, nil)
}

func ReadSessionPreviewMessages(filePath string, maxMessages int) ([]Message, error) {
	return readSessionMessages(filePath, maxMessages, isPreviewMessage)
}

func readSessionMessages(filePath string, maxMessages int, keep func(Message) bool) ([]Message, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ring []Message
	reader := bufio.NewReaderSize(f, 64*1024)
	seenSourceIDs := map[string]bool{}
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			for _, msg := range parseLineMessages(line) {
				if keep != nil && !keep(msg) {
					continue
				}
				if msg.sourceID != "" {
					if seenSourceIDs[msg.sourceID] {
						continue
					}
					seenSourceIDs[msg.sourceID] = true
				}
				appendMessage(&ring, msg, maxMessages)
			}
		}
		if err == io.EOF {
			break
		}
	}
	return ring, nil
}

func isPreviewMessage(msg Message) bool {
	switch msg.Role {
	case "assistant", "assistant_commentary":
		return strings.TrimSpace(msg.Content) != ""
	default:
		return false
	}
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
	case "item.completed":
		return parseItemCompletedLine(line, ts)
	case "turn.completed":
		return parseTurnCompletedLine(line, ts)
	case "turn.failed":
		return parseTurnFailedLine(line, ts)
	}
	switch env.Method {
	case "item/completed":
		return parseItemCompletedRaw(env.Params, ts)
	case "turn/completed":
		return parseTurnCompletedRaw(env.Params, ts)
	case "turn/failed":
		return parseTurnFailedRaw(env.Params, ts)
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
	case "agent_message", "assistant_message":
		var payload codexResponsePayload
		if json.Unmarshal(raw, &payload) != nil {
			return nil
		}
		return parseAgentMessagePayload(payload, ts)
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

func parseAgentMessagePayload(payload codexResponsePayload, ts time.Time) []Message {
	text := firstNonEmptyString(
		extractContentText(payload.Message),
		extractContentText(payload.Text),
		extractContentText(payload.Content),
	)
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	role := "assistant"
	if strings.ToLower(payload.Phase) == "commentary" {
		role = "assistant_commentary"
	}
	return []Message{{Role: role, Content: text, Timestamp: ts, sourceID: messageSourceID(payload.Type, payload.ID)}}
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

	return []Message{{Role: displayRole, Content: text, Timestamp: ts, sourceID: messageSourceID(payload.Type, payload.ID)}}
}

func parseFunctionCall(raw json.RawMessage, ts time.Time) []Message {
	var fc struct {
		ID        string          `json:"id"`
		CallID    string          `json:"call_id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(raw, &fc) != nil {
		return nil
	}
	name := fc.Name
	if name == "" {
		name = "function_call"
	}

	label := "Tool: " + name
	if args := formatJSONFieldText(fc.Arguments); args != "" {
		label += "\n" + args
	}

	return []Message{{Role: "tool", Content: label, Timestamp: ts, sourceID: messageSourceID("function_call", firstNonEmptyString(fc.ID, fc.CallID))}}
}

func parseFunctionCallOutput(raw json.RawMessage, ts time.Time) []Message {
	var fco struct {
		ID     string          `json:"id"`
		CallID string          `json:"call_id"`
		Output json.RawMessage `json:"output"`
	}
	if json.Unmarshal(raw, &fco) != nil {
		return nil
	}
	text := strings.TrimSpace(formatJSONFieldText(fco.Output))
	if text == "" {
		return nil
	}
	return []Message{{Role: "tool_result", Content: text, Timestamp: ts, sourceID: messageSourceID("function_call_output", firstNonEmptyString(fco.ID, fco.CallID))}}
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
	return []Message{{Role: "tool", Content: label, Timestamp: ts, sourceID: messageSourceID(payload.Type, payload.ID)}}
}

func parseCustomToolCallOutput(payload codexResponsePayload, ts time.Time) []Message {
	text := extractContentText(payload.Content)
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	return []Message{{Role: "tool_result", Content: text, Timestamp: ts, sourceID: messageSourceID(payload.Type, payload.ID)}}
}

func parseReasoning(raw json.RawMessage, ts time.Time) []Message {
	var reasoning struct {
		ID      string `json:"id"`
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
	return []Message{{Role: "thinking", Content: strings.Join(parts, "\n"), Timestamp: ts, sourceID: messageSourceID(reasoning.Type, reasoning.ID)}}
}

func parseEventMsg(raw json.RawMessage, ts time.Time) []Message {
	var event struct {
		ID      string          `json:"id"`
		Type    string          `json:"type"`
		Phase   string          `json:"phase"`
		Content json.RawMessage `json:"content"`
		Message json.RawMessage `json:"message"`
		Text    json.RawMessage `json:"text"`
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return nil
	}

	// Only extract user_message as a fallback for sessions without response_item/user
	if event.Type == "user_message" {
		text := strings.TrimSpace(firstNonEmptyString(
			extractContentText(event.Content),
			extractContentText(event.Message),
			extractContentText(event.Text),
		))
		if text != "" {
			return []Message{{Role: "user", Content: text, Timestamp: ts, sourceID: messageSourceID(event.Type, event.ID)}}
		}
	}
	if event.Type == "agent_message" || event.Type == "assistant_message" {
		text := firstNonEmptyString(
			extractContentText(event.Message),
			extractContentText(event.Text),
			extractContentText(event.Content),
			extractContentText(event.Payload),
		)
		text = strings.TrimSpace(text)
		if text == "" {
			return nil
		}
		role := "assistant"
		if strings.ToLower(event.Phase) == "commentary" {
			role = "assistant_commentary"
		}
		return []Message{{Role: role, Content: text, Timestamp: ts, sourceID: messageSourceID(event.Type, event.ID)}}
	}
	return nil
}

func parseItemCompletedLine(line []byte, ts time.Time) []Message {
	var env struct {
		Item    json.RawMessage `json:"item"`
		Payload json.RawMessage `json:"payload"`
		Params  json.RawMessage `json:"params"`
	}
	if json.Unmarshal(line, &env) != nil {
		return nil
	}
	if len(bytes.TrimSpace(env.Item)) > 0 {
		return parseCompletedItem(env.Item, ts)
	}
	if len(bytes.TrimSpace(env.Payload)) > 0 {
		return parseItemCompletedRaw(env.Payload, ts)
	}
	if len(bytes.TrimSpace(env.Params)) > 0 {
		return parseItemCompletedRaw(env.Params, ts)
	}
	return nil
}

func parseItemCompletedRaw(raw json.RawMessage, ts time.Time) []Message {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	var payload struct {
		Item json.RawMessage `json:"item"`
	}
	if json.Unmarshal(raw, &payload) == nil && len(bytes.TrimSpace(payload.Item)) > 0 {
		return parseCompletedItem(payload.Item, ts)
	}
	return parseCompletedItem(raw, ts)
}

func parseCompletedItem(raw json.RawMessage, ts time.Time) []Message {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	return parseResponseItem(raw, ts)
}

func parseTurnCompletedLine(line []byte, ts time.Time) []Message {
	var env struct {
		Payload json.RawMessage `json:"payload"`
		Params  json.RawMessage `json:"params"`
	}
	if json.Unmarshal(line, &env) != nil {
		return nil
	}
	if len(bytes.TrimSpace(env.Payload)) > 0 {
		if msgs := parseTurnCompletedRaw(env.Payload, ts); len(msgs) > 0 {
			return msgs
		}
	}
	if len(bytes.TrimSpace(env.Params)) > 0 {
		if msgs := parseTurnCompletedRaw(env.Params, ts); len(msgs) > 0 {
			return msgs
		}
	}
	return parseTurnCompletedRaw(json.RawMessage(line), ts)
}

func parseTurnCompletedRaw(raw json.RawMessage, ts time.Time) []Message {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	var payload struct {
		Turn struct {
			Items []json.RawMessage `json:"items"`
		} `json:"turn"`
		Items []json.RawMessage `json:"items"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil
	}
	items := payload.Turn.Items
	if len(items) == 0 {
		items = payload.Items
	}
	if len(items) == 0 {
		return nil
	}
	var out []Message
	for _, item := range items {
		out = append(out, parseCompletedItem(item, ts)...)
	}
	return out
}

func parseTurnFailedLine(line []byte, ts time.Time) []Message {
	var env struct {
		Payload json.RawMessage `json:"payload"`
		Params  json.RawMessage `json:"params"`
	}
	if json.Unmarshal(line, &env) != nil {
		return nil
	}
	if len(bytes.TrimSpace(env.Payload)) > 0 {
		if msgs := parseTurnFailedRaw(env.Payload, ts); len(msgs) > 0 {
			return msgs
		}
	}
	if len(bytes.TrimSpace(env.Params)) > 0 {
		if msgs := parseTurnFailedRaw(env.Params, ts); len(msgs) > 0 {
			return msgs
		}
	}
	return parseTurnFailedRaw(json.RawMessage(line), ts)
}

func parseTurnFailedRaw(raw json.RawMessage, ts time.Time) []Message {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	var payload struct {
		ID          string `json:"id"`
		TurnID      string `json:"turn_id"`
		TurnIDCamel string `json:"turnId"`
		Turn        struct {
			ID string `json:"id"`
		} `json:"turn"`
		Error struct {
			Code              string `json:"code"`
			Message           string `json:"message"`
			AdditionalDetails string `json:"additionalDetails"`
		} `json:"error"`
		Message   string `json:"message"`
		Code      string `json:"code"`
		WillRetry bool   `json:"willRetry"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil
	}
	text := firstNonEmptyString(
		payload.Error.Message,
		payload.Message,
		payload.Error.AdditionalDetails,
		payload.Error.Code,
		payload.Code,
	)
	if strings.TrimSpace(text) == "" && payload.WillRetry {
		text = "Codex stream disconnected; reconnecting"
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	sourceID := firstNonEmptyString(payload.ID, payload.TurnID, payload.TurnIDCamel, payload.Turn.ID)
	return []Message{{Role: "assistant_commentary", Content: text, Timestamp: ts, sourceID: messageSourceID("turn.failed", sourceID)}}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func messageSourceID(kind string, id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "item"
	}
	return kind + ":" + id
}

func formatJSONFieldText(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return ""
		}
		return formatMaybeJSONText(s)
	}
	var parsed any
	if json.Unmarshal(raw, &parsed) == nil {
		if formatted, err := json.MarshalIndent(parsed, "", "  "); err == nil {
			return string(formatted)
		}
	}
	return string(raw)
}

func formatMaybeJSONText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	var parsed any
	if json.Unmarshal([]byte(text), &parsed) == nil {
		if formatted, err := json.MarshalIndent(parsed, "", "  "); err == nil {
			return string(formatted)
		}
	}
	return text
}
