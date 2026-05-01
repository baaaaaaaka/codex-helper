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
	Type     string          `json:"type"`
	ThreadID string          `json:"thread_id"`
	TurnID   string          `json:"turn_id"`
	Turn     codexTurn       `json:"turn"`
	Item     codexItem       `json:"item"`
	Usage    codexUsage      `json:"usage"`
	Error    codexEventError `json:"error"`
	Message  string          `json:"message"`
	Code     string          `json:"code"`
	Raw      json.RawMessage `json:"-"`
}

type codexTurn struct {
	ID string `json:"id"`
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
	Code    string `json:"code"`
	Message string `json:"message"`
}

func applyEvent(result *TurnResult, event codexEvent, raw []byte) {
	switch event.Type {
	case "thread.started", "thread/started":
		if strings.TrimSpace(event.ThreadID) != "" {
			result.ThreadID = event.ThreadID
		}
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
		Code:    firstNonEmpty(event.Error.Code, event.Code),
		Message: firstNonEmpty(event.Error.Message, event.Message),
	}
	if failure.Message == "" {
		failure.Message = "Codex turn failed"
	}
	return failure
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
