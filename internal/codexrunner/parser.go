package codexrunner

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func ParseJSONL(r io.Reader) (TurnResult, error) {
	var result TurnResult
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024), 16<<20)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		event, line, ok, err := parseCodexEventJSONLLine(scanner.Bytes(), lineNo)
		if err != nil {
			return result, err
		}
		if !ok {
			continue
		}
		applyEvent(&result, event, line)
	}
	if err := scanner.Err(); err != nil {
		return result, &Error{Kind: ErrorParse, Message: "failed to read JSONL events", Err: err}
	}
	return result, nil
}

func parseCodexEventJSONLLine(line []byte, lineNo int) (codexEvent, []byte, bool, error) {
	return parseCodexEventJSONLLineWithOptions(line, lineNo, true)
}

func parseCodexEventJSONLLineWithOptions(line []byte, lineNo int, includeCommandOutput bool) (codexEvent, []byte, bool, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] != '{' {
		return codexEvent{}, nil, false, nil
	}
	if !includeCommandOutput {
		event, err := unmarshalCodexEventWithoutCommandOutput(line)
		if err != nil {
			return codexEvent{}, nil, false, newJSONLParseError(lineNo, err)
		}
		return event, line, true, nil
	}
	var event codexEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return codexEvent{}, nil, false, newJSONLParseError(lineNo, err)
	}
	return event, line, true, nil
}

func newJSONLParseError(lineNo int, err error) error {
	return &Error{Kind: ErrorParse, Message: fmt.Sprintf("invalid JSON event on line %d", lineNo), Err: err}
}

type codexEvent struct {
	Type          string          `json:"type"`
	ThreadID      string          `json:"thread_id"`
	ThreadIDCamel string          `json:"threadId"`
	ThreadName    string          `json:"thread_name"`
	ThreadName2   string          `json:"threadName"`
	Name          string          `json:"name"`
	Title         string          `json:"title"`
	TurnID        string          `json:"turn_id"`
	Turn          codexTurn       `json:"turn"`
	Thread        codexThread     `json:"thread"`
	Item          codexItem       `json:"item"`
	Payload       json.RawMessage `json:"payload"`
	Usage         codexUsage      `json:"usage"`
	Error         codexEventError `json:"error"`
	Message       string          `json:"message"`
	Code          string          `json:"code"`
	WillRetry     bool            `json:"willRetry"`
	Raw           json.RawMessage `json:"-"`
}

type codexTurn struct {
	ID string `json:"id"`
}

type codexThread struct {
	ID            string `json:"id"`
	ThreadID      string `json:"thread_id"`
	ThreadIDCamel string `json:"threadId"`
	Name          string `json:"name"`
	ThreadName    string `json:"thread_name"`
	ThreadName2   string `json:"threadName"`
	Title         string `json:"title"`
}

type codexItem struct {
	ID                    string         `json:"id"`
	Type                  string         `json:"type"`
	Text                  string         `json:"text"`
	Phase                 string         `json:"phase"`
	Content               []codexContent `json:"content"`
	Command               string         `json:"command"`
	AggregatedOutput      string         `json:"aggregated_output"`
	AggregatedOutputCamel string         `json:"aggregatedOutput"`
	ExitCode              *int           `json:"exit_code"`
	ExitCodeCamel         *int           `json:"exitCode"`
	Status                string         `json:"status"`
}

type codexItemWithoutCommandOutput struct {
	ID            string         `json:"id"`
	Type          string         `json:"type"`
	Text          string         `json:"text"`
	Phase         string         `json:"phase"`
	Content       []codexContent `json:"content"`
	Command       string         `json:"command"`
	ExitCode      *int           `json:"exit_code"`
	ExitCodeCamel *int           `json:"exitCode"`
	Status        string         `json:"status"`
}

func (item codexItemWithoutCommandOutput) toCodexItem() codexItem {
	return codexItem{
		ID:            item.ID,
		Type:          item.Type,
		Text:          item.Text,
		Phase:         item.Phase,
		Content:       item.Content,
		Command:       item.Command,
		ExitCode:      item.ExitCode,
		ExitCodeCamel: item.ExitCodeCamel,
		Status:        item.Status,
	}
}

func unmarshalCodexEventWithoutCommandOutput(line []byte) (codexEvent, error) {
	if event, ok := fastCommandExecutionEvent(line); ok {
		return event, nil
	}
	var lite struct {
		Type          string                        `json:"type"`
		ThreadID      string                        `json:"thread_id"`
		ThreadIDCamel string                        `json:"threadId"`
		ThreadName    string                        `json:"thread_name"`
		ThreadName2   string                        `json:"threadName"`
		Name          string                        `json:"name"`
		Title         string                        `json:"title"`
		TurnID        string                        `json:"turn_id"`
		Turn          codexTurn                     `json:"turn"`
		Thread        codexThread                   `json:"thread"`
		Item          codexItemWithoutCommandOutput `json:"item"`
		Payload       json.RawMessage               `json:"payload"`
		Usage         codexUsage                    `json:"usage"`
		Error         codexEventError               `json:"error"`
		Message       string                        `json:"message"`
		Code          string                        `json:"code"`
		WillRetry     bool                          `json:"willRetry"`
	}
	if err := json.Unmarshal(line, &lite); err != nil {
		return codexEvent{}, err
	}
	return codexEvent{
		Type:          lite.Type,
		ThreadID:      lite.ThreadID,
		ThreadIDCamel: lite.ThreadIDCamel,
		ThreadName:    lite.ThreadName,
		ThreadName2:   lite.ThreadName2,
		Name:          lite.Name,
		Title:         lite.Title,
		TurnID:        lite.TurnID,
		Turn:          lite.Turn,
		Thread:        lite.Thread,
		Item:          lite.Item.toCodexItem(),
		Payload:       lite.Payload,
		Usage:         lite.Usage,
		Error:         lite.Error,
		Message:       lite.Message,
		Code:          lite.Code,
		WillRetry:     lite.WillRetry,
	}, nil
}

func fastCommandExecutionEvent(line []byte) (codexEvent, bool) {
	eventType, ok := jsonStringFieldValue(line, "type")
	if !ok {
		return codexEvent{}, false
	}
	switch eventType {
	case "item.started", "item/started", "item.completed", "item/completed":
	default:
		return codexEvent{}, false
	}
	if !jsonStringFieldValueExists(line, "type", "command_execution") {
		return codexEvent{}, false
	}
	if !json.Valid(line) {
		return codexEvent{}, false
	}
	prefix := line
	if idx := bytes.Index(line, []byte(`"item"`)); idx > 0 {
		prefix = line[:idx]
	}
	return codexEvent{
		Type:          eventType,
		ThreadID:      firstJSONStringField(prefix, "thread_id"),
		ThreadIDCamel: firstJSONStringField(prefix, "threadId"),
		TurnID:        firstJSONStringField(prefix, "turn_id"),
		Item:          codexItem{Type: "command_execution"},
	}, true
}

func firstJSONStringField(line []byte, key string) string {
	value, _ := jsonStringFieldValue(line, key)
	return value
}

func jsonStringFieldValueExists(line []byte, key string, want string) bool {
	pos := 0
	for pos < len(line) {
		value, next, ok := nextJSONStringFieldValue(line[pos:], key)
		if !ok {
			return false
		}
		if value == want {
			return true
		}
		pos += next
	}
	return false
}

func jsonStringFieldValue(line []byte, key string) (string, bool) {
	value, _, ok := nextJSONStringFieldValue(line, key)
	return value, ok
}

func nextJSONStringFieldValue(line []byte, key string) (string, int, bool) {
	pattern := []byte(strconv.Quote(key))
	idx := bytes.Index(line, pattern)
	if idx < 0 {
		return "", len(line), false
	}
	i := idx + len(pattern)
	for i < len(line) && (line[i] == ' ' || line[i] == '\t' || line[i] == '\r' || line[i] == '\n') {
		i++
	}
	if i >= len(line) || line[i] != ':' {
		return "", idx + len(pattern), false
	}
	i++
	for i < len(line) && (line[i] == ' ' || line[i] == '\t' || line[i] == '\r' || line[i] == '\n') {
		i++
	}
	if i >= len(line) || line[i] != '"' {
		return "", idx + len(pattern), false
	}
	i++
	start := i
	escaped := false
	for i < len(line) {
		c := line[i]
		if escaped {
			escaped = false
			i++
			continue
		}
		if c == '\\' {
			escaped = true
			i++
			continue
		}
		if c == '"' {
			if bytes.IndexByte(line[start:i], '\\') >= 0 {
				var decoded string
				if err := json.Unmarshal(line[start-1:i+1], &decoded); err != nil {
					return "", i + 1, false
				}
				return decoded, i + 1, true
			}
			return string(line[start:i]), i + 1, true
		}
		i++
	}
	return "", len(line), false
}

type codexContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type codexUsage struct {
	InputTokens           int64                   `json:"input_tokens"`
	OutputTokens          int64                   `json:"output_tokens"`
	ReasoningOutputTokens int64                   `json:"reasoning_output_tokens"`
	TotalTokens           int64                   `json:"total_tokens"`
	CachedInputTokens     int64                   `json:"cached_input_tokens"`
	InputTokensDetails    codexInputTokenDetails  `json:"input_tokens_details"`
	PromptTokenDetails    codexPromptTokenDetails `json:"prompt_tokens_details"`
}

type codexInputTokenDetails struct {
	CachedTokens      int64 `json:"cached_tokens"`
	CachedInputTokens int64 `json:"cached_input_tokens"`
}

type codexPromptTokenDetails struct {
	CachedTokens int64 `json:"cached_tokens"`
}

type codexEventError struct {
	Code              string          `json:"code"`
	Message           string          `json:"message"`
	AdditionalDetails string          `json:"additionalDetails"`
	CodexErrorInfo    json.RawMessage `json:"codexErrorInfo"`
}

type codexTranscriptPayload struct {
	ID                    string         `json:"id"`
	Type                  string         `json:"type"`
	Phase                 string         `json:"phase"`
	Role                  string         `json:"role"`
	Message               string         `json:"message"`
	LastAgentMessage      string         `json:"last_agent_message"`
	LastAgentMessageCamel string         `json:"lastAgentMessage"`
	ThreadID              string         `json:"thread_id"`
	ThreadIDCamel         string         `json:"threadId"`
	TurnID                string         `json:"turn_id"`
	TurnIDCamel           string         `json:"turnId"`
	Turn                  codexTurn      `json:"turn"`
	Content               []codexContent `json:"content"`
}

func applyEvent(result *TurnResult, event codexEvent, raw []byte) {
	switch event.Type {
	case "session_meta":
		applySessionMetaEvent(result, event)
		mergeUsage(&result.Usage, event.Usage)
	case "thread.started", "thread/started":
		if id := firstNonEmpty(event.ThreadIDCamel, event.ThreadID, event.Thread.ThreadIDCamel, event.Thread.ThreadID, event.Thread.ID); id != "" {
			result.ThreadID = id
		}
		setThreadName(result, event)
	case "thread.name.updated", "thread/name/updated":
		if id := firstNonEmpty(event.ThreadIDCamel, event.ThreadID, event.Thread.ThreadIDCamel, event.Thread.ThreadID, event.Thread.ID); id != "" {
			result.ThreadID = id
		}
		setThreadName(result, event)
	case "turn.started", "turn/started":
		result.Status = TurnStatusStarted
		setTurnID(result, event)
	case "item.completed", "item/completed":
		if isAgentMessageItem(event.Item) {
			if text := agentMessageText(event.Item); strings.TrimSpace(text) != "" {
				result.FinalAgentMessage = text
				result.FinalAgentMessageComplete = true
				result.RawCompletedMessage = append(result.RawCompletedMessage[:0], raw...)
			}
		}
	case "turn.completed", "turn/completed":
		result.Status = TurnStatusCompleted
		setTurnID(result, event)
		mergeUsage(&result.Usage, event.Usage)
	case "turn.failed", "turn/failed":
		result.Status = TurnStatusFailed
		setTurnID(result, event)
		mergeUsage(&result.Usage, event.Usage)
		result.Failure = failureFromEvent(event)
	case "event_msg", "response_item":
		applyTranscriptPayloadEvent(result, event, raw)
		mergeUsage(&result.Usage, event.Usage)
	default:
		mergeUsage(&result.Usage, event.Usage)
	}
}

func applySessionMetaEvent(result *TurnResult, event codexEvent) {
	payload, ok := parseTranscriptPayload(event.Payload)
	if !ok {
		return
	}
	if strings.TrimSpace(result.ThreadID) == "" {
		if id := firstNonEmpty(payload.ThreadIDCamel, payload.ThreadID, payload.ID); id != "" {
			result.ThreadID = id
		}
	}
}

func applyTranscriptPayloadEvent(result *TurnResult, event codexEvent, raw []byte) bool {
	payload, ok := parseTranscriptPayload(event.Payload)
	if !ok {
		return false
	}
	switch event.Type {
	case "event_msg":
		switch strings.ToLower(strings.TrimSpace(payload.Type)) {
		case "agent_message":
			if !isFinalAnswerPhase(payload.Phase) {
				return false
			}
			text := strings.TrimSpace(payload.Message)
			if text == "" {
				return false
			}
			completeTurnFromTranscriptPayload(result, text, transcriptPayloadTurnID(payload), raw)
			return true
		case "task_complete":
			text := firstNonEmpty(payload.LastAgentMessage, payload.LastAgentMessageCamel)
			completeTurnFromTranscriptPayload(result, text, transcriptPayloadTurnID(payload), raw)
			return true
		default:
			return false
		}
	case "response_item":
		if strings.ToLower(strings.TrimSpace(payload.Type)) != "message" ||
			strings.ToLower(strings.TrimSpace(payload.Role)) != "assistant" ||
			!isFinalAnswerPhase(payload.Phase) {
			return false
		}
		text := strings.TrimSpace(agentMessageText(codexItem{Content: payload.Content}))
		if text == "" {
			return false
		}
		completeTurnFromTranscriptPayload(result, text, transcriptPayloadTurnID(payload), raw)
		return true
	default:
		return false
	}
}

func parseTranscriptPayload(raw json.RawMessage) (codexTranscriptPayload, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return codexTranscriptPayload{}, false
	}
	var payload codexTranscriptPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return codexTranscriptPayload{}, false
	}
	return payload, true
}

func completeTurnFromTranscriptPayload(result *TurnResult, text string, turnID string, raw []byte) {
	if text = strings.TrimSpace(text); text != "" {
		result.FinalAgentMessage = text
		result.FinalAgentMessageComplete = true
		result.RawCompletedMessage = append(result.RawCompletedMessage[:0], raw...)
	}
	if turnID = strings.TrimSpace(turnID); turnID != "" {
		result.TurnID = turnID
	}
	if result.Failure == nil && result.Status != TurnStatusFailed {
		result.Status = TurnStatusCompleted
	}
}

func transcriptPayloadTurnID(payload codexTranscriptPayload) string {
	return firstNonEmpty(payload.TurnIDCamel, payload.TurnID, payload.Turn.ID)
}

func isFinalAnswerPhase(phase string) bool {
	return strings.EqualFold(strings.TrimSpace(phase), "final_answer")
}

func setThreadName(result *TurnResult, event codexEvent) {
	if name := firstNonEmpty(event.Thread.Name, event.Thread.ThreadName2, event.Thread.ThreadName, event.Thread.Title, event.ThreadName2, event.ThreadName, event.Name, event.Title); name != "" {
		result.ThreadName = strings.TrimSpace(name)
	}
}

func setTurnID(result *TurnResult, event codexEvent) {
	if event.TurnID != "" {
		result.TurnID = event.TurnID
		return
	}
	if event.Turn.ID != "" {
		result.TurnID = event.Turn.ID
	}
}

func agentMessageText(item codexItem) string {
	if item.Text != "" {
		return item.Text
	}
	var parts []string
	for _, content := range item.Content {
		if content.Text != "" {
			parts = append(parts, content.Text)
		}
	}
	return strings.Join(parts, "")
}

func commandOutputText(item codexItem) string {
	return firstNonEmpty(item.AggregatedOutput, item.AggregatedOutputCamel)
}

func commandExitCode(item codexItem) *int {
	if item.ExitCode != nil {
		return item.ExitCode
	}
	return item.ExitCodeCamel
}

func mergeUsage(dst *Usage, src codexUsage) {
	if src.InputTokens != 0 {
		dst.InputTokens = src.InputTokens
	}
	if src.OutputTokens != 0 {
		dst.OutputTokens = src.OutputTokens
	}
	if src.ReasoningOutputTokens != 0 {
		dst.ReasoningOutputTokens = src.ReasoningOutputTokens
	}
	if src.TotalTokens != 0 {
		dst.TotalTokens = src.TotalTokens
	}
	if src.CachedInputTokens != 0 {
		dst.CachedInputTokens = src.CachedInputTokens
	}
	if src.InputTokensDetails.CachedInputTokens != 0 {
		dst.CachedInputTokens = src.InputTokensDetails.CachedInputTokens
	}
	if src.InputTokensDetails.CachedTokens != 0 {
		dst.CachedInputTokens = src.InputTokensDetails.CachedTokens
	}
	if src.PromptTokenDetails.CachedTokens != 0 {
		dst.CachedInputTokens = src.PromptTokenDetails.CachedTokens
	}
}

func failureFromEvent(event codexEvent) *TurnFailure {
	failure := &TurnFailure{
		Code:    firstNonEmpty(event.Error.Code, event.Code, codexErrorInfoCode(event.Error.CodexErrorInfo)),
		Message: composeCodexFailureMessage(event.Error.Message, event.Message, event.Error.AdditionalDetails),
	}
	if failure.Message == "" {
		if event.WillRetry {
			failure.Message = "Codex stream disconnected; reconnecting"
		} else {
			failure.Message = "Codex turn failed"
		}
	}
	return failure
}

func composeCodexFailureMessage(primary, fallback, additional string) string {
	message := sanitizeCodexErrorText(firstNonEmpty(primary, fallback), 2048)
	detail := sanitizeCodexErrorText(additional, 512)
	if message == "" {
		return detail
	}
	if detail == "" || strings.Contains(message, detail) {
		return message
	}
	return message + ": " + detail
}

func sanitizeTurnFailure(failure *TurnFailure) *TurnFailure {
	if failure == nil {
		return nil
	}
	result := *failure
	result.Code = sanitizeCodexErrorText(result.Code, 128)
	result.Message = sanitizeCodexErrorText(result.Message, 2048)
	return &result
}

func codexErrorInfoCode(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var name string
	if err := json.Unmarshal(raw, &name); err == nil {
		return name
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return ""
	}
	for key := range object {
		return key
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
