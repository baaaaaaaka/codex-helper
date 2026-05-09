package codexrunner

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func ParseJSONL(r io.Reader) (TurnResult, error) {
	var result TurnResult
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024), 16<<20)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var event codexEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return result, &Error{Kind: ErrorParse, Message: fmt.Sprintf("invalid JSON event on line %d", lineNo), Err: err}
		}
		applyEvent(&result, event, line)
	}
	if err := scanner.Err(); err != nil {
		return result, &Error{Kind: ErrorParse, Message: "failed to read JSONL events", Err: err}
	}
	return result, nil
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
	Content               []codexContent `json:"content"`
	Command               string         `json:"command"`
	AggregatedOutput      string         `json:"aggregated_output"`
	AggregatedOutputCamel string         `json:"aggregatedOutput"`
	ExitCode              *int           `json:"exit_code"`
	ExitCodeCamel         *int           `json:"exitCode"`
	Status                string         `json:"status"`
}

type codexContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type codexUsage struct {
	InputTokens        int64                   `json:"input_tokens"`
	OutputTokens       int64                   `json:"output_tokens"`
	TotalTokens        int64                   `json:"total_tokens"`
	CachedInputTokens  int64                   `json:"cached_input_tokens"`
	InputTokensDetails codexInputTokenDetails  `json:"input_tokens_details"`
	PromptTokenDetails codexPromptTokenDetails `json:"prompt_tokens_details"`
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

func applyEvent(result *TurnResult, event codexEvent, raw []byte) {
	switch event.Type {
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
	default:
		mergeUsage(&result.Usage, event.Usage)
	}
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
		Message: firstNonEmpty(event.Error.Message, event.Message, event.Error.AdditionalDetails),
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
