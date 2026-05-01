package teams

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type TranscriptKind string

const (
	TranscriptKindUser      TranscriptKind = "user"
	TranscriptKindAssistant TranscriptKind = "assistant"
	TranscriptKindTool      TranscriptKind = "tool"
	TranscriptKindStatus    TranscriptKind = "status"
	TranscriptKindArtifact  TranscriptKind = "artifact"
	TranscriptKindUnknown   TranscriptKind = "unknown"
)

type TranscriptParseOptions struct {
	SourceName string
}

type Transcript struct {
	SourceName      string
	FileFingerprint string
	ThreadID        string
	Records         []TranscriptRecord
	Diagnostics     []TranscriptDiagnostic
}

type TranscriptRecord struct {
	ItemID       string
	SourceItemID string
	DedupeKey    string
	ThreadID     string
	TurnID       string
	Kind         TranscriptKind
	Text         string
	CreatedAt    time.Time
	SourceLine   int
	SourceType   string
}

type TranscriptDiagnostic struct {
	SourceLine int
	Kind       string
	Message    string
}

func ReadSessionTranscript(filePath string) (Transcript, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return Transcript{}, err
	}
	defer f.Close()

	sourceName := filePath
	if abs, err := filepath.Abs(filePath); err == nil {
		sourceName = abs
	}
	return ParseCodexTranscript(f, TranscriptParseOptions{SourceName: sourceName})
}

func ReadSessionTranscriptSince(filePath string, afterKey string) (Transcript, error) {
	transcript, err := ReadSessionTranscript(filePath)
	if err != nil {
		return transcript, err
	}
	afterKey = strings.TrimSpace(afterKey)
	if afterKey == "" {
		return transcript, nil
	}
	for i, record := range transcript.Records {
		if record.DedupeKey == afterKey || record.ItemID == afterKey {
			transcript.Records = append([]TranscriptRecord(nil), transcript.Records[i+1:]...)
			return transcript, nil
		}
	}
	transcript.Records = nil
	transcript.Diagnostics = append(transcript.Diagnostics, TranscriptDiagnostic{
		Kind:    "checkpoint_not_found",
		Message: "transcript checkpoint was not found; refusing to guess an import position",
	})
	return transcript, nil
}

func ParseCodexTranscript(r io.Reader, opts TranscriptParseOptions) (Transcript, error) {
	var state transcriptParseState
	transcript := Transcript{SourceName: strings.TrimSpace(opts.SourceName)}
	reader := bufio.NewReaderSize(r, 64*1024)
	digest := sha256.New()
	lineNo := 0

	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			_, _ = digest.Write(line)
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				records, diagnostics := parseTranscriptLine(trimmed, lineNo, &state)
				transcript.Records = append(transcript.Records, records...)
				transcript.Diagnostics = append(transcript.Diagnostics, diagnostics...)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return transcript, err
		}
	}

	transcript.ThreadID = state.threadID
	transcript.FileFingerprint = transcriptFileFingerprint(transcript.SourceName, state.sessionID, digest.Sum(nil))
	finalizeTranscriptRecordIDs(&transcript)
	return transcript, nil
}

type transcriptParseState struct {
	sessionID string
	threadID  string
	turnID    string
}

type pendingTranscriptRecord struct {
	sourceItemID string
	threadID     string
	turnID       string
	kind         TranscriptKind
	text         string
	createdAt    time.Time
	sourceLine   int
	sourceType   string
}

func parseTranscriptLine(line []byte, lineNo int, state *transcriptParseState) ([]TranscriptRecord, []TranscriptDiagnostic) {
	if len(line) == 0 || line[0] != '{' {
		return nil, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(line, &obj); err != nil {
		return nil, []TranscriptDiagnostic{{
			SourceLine: lineNo,
			Kind:       "invalid_json",
			Message:    "invalid JSON transcript line; skipping this line",
		}}
	}

	createdAt := transcriptTimestamp(obj)
	threadID := firstNonEmptyString(
		jsonStringField(obj, "thread_id", "threadId", "conversation_id", "conversationId"),
		state.threadID,
	)
	turnID := firstNonEmptyString(
		jsonStringField(obj, "turn_id", "turnId"),
		nestedJSONID(obj, "turn"),
		state.turnID,
	)

	if method := jsonStringField(obj, "method"); method != "" {
		return parseTranscriptMethodLine(obj, method, lineNo, createdAt, threadID, turnID, state)
	}

	lineType := jsonStringField(obj, "type")
	switch lineType {
	case "session_meta":
		if payload, ok := jsonObjectField(obj, "payload"); ok {
			sessionID := jsonStringField(payload, "id", "session_id", "sessionId")
			if sessionID != "" {
				state.sessionID = sessionID
				if state.threadID == "" {
					state.threadID = sessionID
				}
			}
		}
		return nil, nil
	case "response_item":
		payload, ok := jsonObjectField(obj, "payload")
		if !ok {
			return nil, nil
		}
		record, ok := responseItemTranscriptRecord(payload, lineNo, createdAt, threadID, turnID)
		if !ok {
			return nil, nil
		}
		return []TranscriptRecord{record.toRecord()}, nil
	case "event_msg":
		payload, ok := jsonObjectField(obj, "payload")
		if !ok {
			return nil, nil
		}
		record, ok := eventMsgTranscriptRecord(payload, lineNo, createdAt, threadID, turnID)
		if !ok {
			return nil, nil
		}
		return []TranscriptRecord{record.toRecord()}, nil
	case "thread.started":
		if id := jsonStringField(obj, "thread_id", "threadId"); id != "" {
			state.threadID = id
		}
		return nil, nil
	case "turn.started":
		if id := firstNonEmptyString(jsonStringField(obj, "turn_id", "turnId"), nestedJSONID(obj, "turn")); id != "" {
			state.turnID = id
		}
		return statusEventTranscriptRecord(obj, lineNo, createdAt, state.threadID, state.turnID)
	case "turn.completed":
		if id := firstNonEmptyString(jsonStringField(obj, "turn_id", "turnId"), nestedJSONID(obj, "turn")); id != "" {
			state.turnID = id
		}
		return statusEventTranscriptRecord(obj, lineNo, createdAt, state.threadID, state.turnID)
	case "turn.failed":
		if id := firstNonEmptyString(jsonStringField(obj, "turn_id", "turnId"), nestedJSONID(obj, "turn")); id != "" {
			state.turnID = id
		}
		record, ok := failedTurnTranscriptRecord(obj, lineNo, createdAt, state.threadID, state.turnID)
		if !ok {
			return nil, nil
		}
		return []TranscriptRecord{record.toRecord()}, nil
	case "item.completed":
		record, ok := completedItemTranscriptRecord(obj, lineNo, createdAt, threadID, turnID)
		if !ok {
			return nil, nil
		}
		return []TranscriptRecord{record.toRecord()}, nil
	}

	record, ok := genericTranscriptRecord(obj, lineNo, createdAt, threadID, turnID)
	if !ok {
		return nil, nil
	}
	return []TranscriptRecord{record.toRecord()}, nil
}

func parseTranscriptMethodLine(obj map[string]json.RawMessage, method string, lineNo int, createdAt time.Time, threadID string, turnID string, state *transcriptParseState) ([]TranscriptRecord, []TranscriptDiagnostic) {
	params, _ := jsonObjectField(obj, "params")
	threadID = firstNonEmptyString(jsonStringField(params, "threadId", "thread_id"), threadID)
	turnID = firstNonEmptyString(jsonStringField(params, "turnId", "turn_id"), nestedJSONID(params, "turn"), turnID)
	if threadID != "" {
		state.threadID = threadID
	}
	if turnID != "" {
		state.turnID = turnID
	}

	switch method {
	case "item/completed":
		record, ok := completedItemTranscriptRecord(params, lineNo, createdAt, threadID, turnID)
		if !ok {
			return nil, nil
		}
		return []TranscriptRecord{record.toRecord()}, nil
	case "turn/completed":
		return turnCompletedMethodTranscriptRecords(params, lineNo, createdAt, threadID, turnID), nil
	case "error", "configWarning":
		record, ok := statusObjectTranscriptRecord(params, method, lineNo, createdAt, threadID, turnID)
		if !ok {
			return nil, nil
		}
		return []TranscriptRecord{record.toRecord()}, nil
	}
	return nil, nil
}

func responseItemTranscriptRecord(payload map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) (pendingTranscriptRecord, bool) {
	itemType := jsonStringField(payload, "type")
	sourceID := jsonStringField(payload, "id", "item_id", "itemId", "call_id", "callId")
	threadID = firstNonEmptyString(jsonStringField(payload, "thread_id", "threadId"), threadID)
	turnID = firstNonEmptyString(jsonStringField(payload, "turn_id", "turnId"), nestedJSONID(payload, "turn"), turnID)
	kind, text, ok := responseItemKindText(payload)
	if !ok {
		return pendingTranscriptRecord{}, false
	}
	return pendingTranscriptRecord{
		sourceItemID: sourceID,
		threadID:     threadID,
		turnID:       turnID,
		kind:         kind,
		text:         text,
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   itemType,
	}, true
}

func eventMsgTranscriptRecord(payload map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) (pendingTranscriptRecord, bool) {
	eventType := jsonStringField(payload, "type")
	sourceID := jsonStringField(payload, "id", "item_id", "itemId", "message_id", "messageId")
	threadID = firstNonEmptyString(jsonStringField(payload, "thread_id", "threadId"), threadID)
	turnID = firstNonEmptyString(jsonStringField(payload, "turn_id", "turnId"), turnID)

	kind := kindFromType(eventType)
	if kind == TranscriptKindUnknown && eventType == "user_message" {
		kind = TranscriptKindUser
	}
	if kind == TranscriptKindAssistant && strings.EqualFold(jsonStringField(payload, "phase"), "commentary") {
		kind = TranscriptKindStatus
	}
	text := firstNonEmptyString(
		jsonStringField(payload, "content", "text", "message"),
		textFromJSONRaw(payload["content"]),
	)
	if strings.TrimSpace(text) == "" {
		return pendingTranscriptRecord{}, false
	}
	if kind == TranscriptKindUser && shouldSkipTranscriptUserText(text) {
		return pendingTranscriptRecord{}, false
	}
	return pendingTranscriptRecord{
		sourceItemID: sourceID,
		threadID:     threadID,
		turnID:       turnID,
		kind:         kind,
		text:         strings.TrimSpace(text),
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   eventType,
	}, true
}

func completedItemTranscriptRecord(obj map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) (pendingTranscriptRecord, bool) {
	item, ok := jsonObjectField(obj, "item")
	if !ok {
		return pendingTranscriptRecord{}, false
	}
	threadID = firstNonEmptyString(jsonStringField(obj, "thread_id", "threadId"), threadID)
	turnID = firstNonEmptyString(jsonStringField(obj, "turn_id", "turnId"), turnID)

	sourceID := jsonStringField(item, "id", "item_id", "itemId", "call_id", "callId")
	itemType := jsonStringField(item, "type")
	kind := kindFromType(itemType)
	text := firstNonEmptyString(
		jsonStringField(item, "text", "content", "message", "output"),
		textFromJSONRaw(item["content"]),
	)
	if strings.TrimSpace(text) == "" {
		return pendingTranscriptRecord{}, false
	}
	if kind == TranscriptKindUser && shouldSkipTranscriptUserText(text) {
		return pendingTranscriptRecord{}, false
	}
	return pendingTranscriptRecord{
		sourceItemID: sourceID,
		threadID:     threadID,
		turnID:       turnID,
		kind:         kind,
		text:         strings.TrimSpace(text),
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   itemType,
	}, true
}

func statusEventTranscriptRecord(obj map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) ([]TranscriptRecord, []TranscriptDiagnostic) {
	record, ok := statusObjectTranscriptRecord(obj, jsonStringField(obj, "type"), lineNo, createdAt, threadID, turnID)
	if !ok {
		return nil, nil
	}
	return []TranscriptRecord{record.toRecord()}, nil
}

func statusObjectTranscriptRecord(obj map[string]json.RawMessage, sourceType string, lineNo int, createdAt time.Time, threadID string, turnID string) (pendingTranscriptRecord, bool) {
	text := firstNonEmptyString(
		jsonStringField(obj, "message", "status", "text"),
		errorMessageFromObject(obj),
	)
	if strings.TrimSpace(text) == "" {
		return pendingTranscriptRecord{}, false
	}
	return pendingTranscriptRecord{
		sourceItemID: jsonStringField(obj, "id", "item_id", "itemId"),
		threadID:     threadID,
		turnID:       turnID,
		kind:         TranscriptKindStatus,
		text:         strings.TrimSpace(text),
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   sourceType,
	}, true
}

func failedTurnTranscriptRecord(obj map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) (pendingTranscriptRecord, bool) {
	text := firstNonEmptyString(errorMessageFromObject(obj), jsonStringField(obj, "message", "code"))
	if strings.TrimSpace(text) == "" {
		return pendingTranscriptRecord{}, false
	}
	return pendingTranscriptRecord{
		sourceItemID: jsonStringField(obj, "id", "item_id", "itemId"),
		threadID:     threadID,
		turnID:       turnID,
		kind:         TranscriptKindStatus,
		text:         strings.TrimSpace(text),
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   "turn.failed",
	}, true
}

func turnCompletedMethodTranscriptRecords(params map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) []TranscriptRecord {
	turn, ok := jsonObjectField(params, "turn")
	if !ok {
		return nil
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(turn["items"], &rawItems); err != nil {
		return nil
	}
	var records []TranscriptRecord
	for _, rawItem := range rawItems {
		item := rawToObject(rawItem)
		if item == nil {
			continue
		}
		wrapper := map[string]json.RawMessage{"item": rawItem}
		record, ok := completedItemTranscriptRecord(wrapper, lineNo, createdAt, threadID, turnID)
		if ok {
			records = append(records, record.toRecord())
		}
	}
	return records
}

func genericTranscriptRecord(obj map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) (pendingTranscriptRecord, bool) {
	sourceID := jsonStringField(obj, "id", "item_id", "itemId", "record_id", "recordId", "message_id", "messageId")
	sourceType := jsonStringField(obj, "type", "kind")
	kind := kindFromRole(jsonStringField(obj, "role"))
	if kind == TranscriptKindUnknown {
		kind = kindFromType(sourceType)
	}
	text := firstNonEmptyString(
		jsonStringField(obj, "text", "message", "output", "delta"),
		textFromJSONRaw(obj["content"]),
	)
	if strings.TrimSpace(text) == "" {
		return pendingTranscriptRecord{}, false
	}
	if kind == TranscriptKindUser && shouldSkipTranscriptUserText(text) {
		return pendingTranscriptRecord{}, false
	}
	return pendingTranscriptRecord{
		sourceItemID: sourceID,
		threadID:     threadID,
		turnID:       turnID,
		kind:         kind,
		text:         strings.TrimSpace(text),
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   sourceType,
	}, true
}

func responseItemKindText(payload map[string]json.RawMessage) (TranscriptKind, string, bool) {
	itemType := jsonStringField(payload, "type")
	switch itemType {
	case "message":
		role := strings.ToLower(jsonStringField(payload, "role"))
		if role == "system" || role == "developer" {
			return "", "", false
		}
		kind := kindFromRole(role)
		if kind == TranscriptKindAssistant && strings.EqualFold(jsonStringField(payload, "phase"), "commentary") {
			kind = TranscriptKindStatus
		}
		text := firstNonEmptyString(
			textFromJSONRaw(payload["content"]),
			jsonStringField(payload, "text", "message"),
		)
		if strings.TrimSpace(text) == "" {
			return "", "", false
		}
		if kind == TranscriptKindUser && shouldSkipTranscriptUserText(text) {
			return "", "", false
		}
		return kind, strings.TrimSpace(text), true
	case "function_call":
		return TranscriptKindTool, transcriptFunctionCallText(payload), true
	case "function_call_output":
		text := firstNonEmptyString(jsonStringField(payload, "output"), textFromJSONRaw(payload["content"]))
		if strings.TrimSpace(text) == "" {
			return "", "", false
		}
		return TranscriptKindTool, strings.TrimSpace(text), true
	case "custom_tool_call":
		return TranscriptKindTool, transcriptCustomToolCallText(payload), true
	case "custom_tool_call_output":
		text := firstNonEmptyString(textFromJSONRaw(payload["content"]), jsonStringField(payload, "output"))
		if strings.TrimSpace(text) == "" {
			return "", "", false
		}
		return TranscriptKindTool, strings.TrimSpace(text), true
	case "reasoning":
		text := reasoningSummaryText(payload)
		if strings.TrimSpace(text) == "" {
			return "", "", false
		}
		return TranscriptKindStatus, strings.TrimSpace(text), true
	case "artifact", "file", "image":
		text := firstNonEmptyString(jsonStringField(payload, "text", "message", "path", "name"), textFromJSONRaw(payload["content"]))
		if strings.TrimSpace(text) == "" {
			return "", "", false
		}
		return TranscriptKindArtifact, strings.TrimSpace(text), true
	default:
		kind := kindFromType(itemType)
		text := firstNonEmptyString(
			textFromJSONRaw(payload["content"]),
			jsonStringField(payload, "text", "message", "output"),
		)
		if strings.TrimSpace(text) == "" {
			return "", "", false
		}
		return kind, strings.TrimSpace(text), true
	}
}

func transcriptFunctionCallText(payload map[string]json.RawMessage) string {
	name := jsonStringField(payload, "name")
	if name == "" {
		name = "function_call"
	}
	text := "Tool: " + name
	if args := jsonStringField(payload, "arguments"); args != "" {
		var parsed any
		if json.Unmarshal([]byte(args), &parsed) == nil {
			if formatted, err := json.MarshalIndent(parsed, "", "  "); err == nil {
				text += "\n" + string(formatted)
				return text
			}
		}
		text += "\n" + args
	}
	return text
}

func transcriptCustomToolCallText(payload map[string]json.RawMessage) string {
	name := jsonStringField(payload, "name")
	if name == "" {
		name = "custom_tool"
	}
	text := "Tool: " + name
	if content := strings.TrimSpace(textFromJSONRaw(payload["content"])); content != "" {
		text += "\n" + content
	}
	return text
}

func reasoningSummaryText(payload map[string]json.RawMessage) string {
	var reasoning struct {
		Summary []struct {
			Text string `json:"text"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(mustMarshalObject(payload), &reasoning); err != nil {
		return ""
	}
	var parts []string
	for _, item := range reasoning.Summary {
		if text := strings.TrimSpace(item.Text); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func (p pendingTranscriptRecord) toRecord() TranscriptRecord {
	return TranscriptRecord{
		SourceItemID: p.sourceItemID,
		ThreadID:     p.threadID,
		TurnID:       p.turnID,
		Kind:         p.kind,
		Text:         p.text,
		CreatedAt:    p.createdAt,
		SourceLine:   p.sourceLine,
		SourceType:   p.sourceType,
	}
}

func finalizeTranscriptRecordIDs(transcript *Transcript) {
	seenItemIDs := map[string]int{}
	seenSourceIDs := map[string]int{}
	for i := range transcript.Records {
		record := &transcript.Records[i]
		if record.Kind == "" {
			record.Kind = TranscriptKindUnknown
		}
		sourceID := strings.TrimSpace(record.SourceItemID)
		if sourceID != "" {
			seenSourceIDs[sourceID]++
			record.DedupeKey = "source:" + sourceID
			if seenSourceIDs[sourceID] == 1 {
				record.ItemID = sourceID
			} else {
				record.ItemID = sourceID + "#line:" + strconv.Itoa(record.SourceLine)
				transcript.Diagnostics = append(transcript.Diagnostics, TranscriptDiagnostic{
					SourceLine: record.SourceLine,
					Kind:       "duplicate_item_id",
					Message:    fmt.Sprintf("source transcript item id %q was repeated; preserving order with a line-scoped item id", sourceID),
				})
			}
		} else {
			record.ItemID = fallbackTranscriptItemID(transcript.FileFingerprint, record.SourceLine, record.Kind)
			record.DedupeKey = record.ItemID
		}
		if seenItemIDs[record.ItemID] > 0 {
			record.ItemID = record.ItemID + "#ordinal:" + strconv.Itoa(seenItemIDs[record.ItemID]+1)
		}
		seenItemIDs[record.ItemID]++
	}
}

func fallbackTranscriptItemID(fileFingerprint string, lineNo int, kind TranscriptKind) string {
	if fileFingerprint == "" {
		fileFingerprint = "unknown"
	}
	if kind == "" {
		kind = TranscriptKindUnknown
	}
	return fmt.Sprintf("fallback:%s:line:%d:kind:%s", fileFingerprint, lineNo, kind)
}

func transcriptFileFingerprint(sourceName string, sessionID string, contentDigest []byte) string {
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		return "session:" + sessionID
	}
	if sourceName = strings.TrimSpace(sourceName); sourceName != "" {
		sum := sha256.Sum256([]byte(filepath.Clean(sourceName)))
		return "file:" + hex.EncodeToString(sum[:8])
	}
	if len(contentDigest) > 0 {
		return "stream:" + hex.EncodeToString(contentDigest[:min(len(contentDigest), 8)])
	}
	return "stream:empty"
}

func kindFromRole(role string) TranscriptKind {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "user":
		return TranscriptKindUser
	case "assistant", "agent":
		return TranscriptKindAssistant
	case "tool", "tool_result", "function", "function_call", "custom_tool":
		return TranscriptKindTool
	case "status", "assistant_commentary", "thinking", "reasoning":
		return TranscriptKindStatus
	case "artifact", "file", "image":
		return TranscriptKindArtifact
	default:
		return TranscriptKindUnknown
	}
}

func kindFromType(value string) TranscriptKind {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "user_message", "user", "input_message":
		return TranscriptKindUser
	case "agent_message", "agentmessage", "assistant_message", "assistant", "message":
		return TranscriptKindAssistant
	case "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output", "tool", "tool_result", "command_execution":
		return TranscriptKindTool
	case "status", "agent_status", "turn.started", "turn.completed", "turn.failed", "reasoning", "configwarning", "error":
		return TranscriptKindStatus
	case "artifact", "file", "image":
		return TranscriptKindArtifact
	default:
		return TranscriptKindUnknown
	}
}

func shouldSkipTranscriptUserText(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return true
	}
	if strings.HasPrefix(trimmed, "<") && strings.HasSuffix(trimmed, ">") {
		return true
	}
	if strings.HasPrefix(trimmed, "# AGENTS.md") {
		return true
	}
	if strings.Contains(trimmed, "<INSTRUCTIONS>") {
		return true
	}
	return false
}

func transcriptTimestamp(obj map[string]json.RawMessage) time.Time {
	for _, key := range []string{"timestamp", "created_at", "createdAt", "time"} {
		if t := parseTranscriptTimestampValue(obj[key]); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func parseTranscriptTimestampValue(raw json.RawMessage) time.Time {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return time.Time{}
	}
	if raw[0] == '"' {
		value := rawScalarString(raw)
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, strings.TrimSpace(value)); err == nil {
				return t
			}
		}
		return time.Time{}
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&number) != nil {
		return time.Time{}
	}
	if seconds, err := strconv.ParseInt(number.String(), 10, 64); err == nil {
		return time.Unix(seconds, 0).UTC()
	}
	if f, err := strconv.ParseFloat(number.String(), 64); err == nil {
		seconds := int64(f)
		nanos := int64((f - float64(seconds)) * 1e9)
		return time.Unix(seconds, nanos).UTC()
	}
	return time.Time{}
}

func errorMessageFromObject(obj map[string]json.RawMessage) string {
	if errObj, ok := jsonObjectField(obj, "error"); ok {
		return firstNonEmptyString(jsonStringField(errObj, "message"), jsonStringField(errObj, "code"))
	}
	return ""
}

func jsonObjectField(obj map[string]json.RawMessage, key string) (map[string]json.RawMessage, bool) {
	if obj == nil {
		return nil, false
	}
	raw, ok := obj[key]
	if !ok {
		return nil, false
	}
	parsed := rawToObject(raw)
	return parsed, parsed != nil
}

func rawToObject(raw json.RawMessage) map[string]json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	var parsed map[string]json.RawMessage
	if json.Unmarshal(raw, &parsed) != nil {
		return nil
	}
	return parsed
}

func nestedJSONID(obj map[string]json.RawMessage, key string) string {
	nested, ok := jsonObjectField(obj, key)
	if !ok {
		return ""
	}
	return jsonStringField(nested, "id")
}

func jsonStringField(obj map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if obj == nil {
			return ""
		}
		raw, ok := obj[key]
		if !ok {
			continue
		}
		if value := strings.TrimSpace(rawScalarString(raw)); value != "" {
			return value
		}
	}
	return ""
}

func rawScalarString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return value
		}
		return ""
	}
	switch string(raw) {
	case "null", "true", "false":
		return ""
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&number) == nil {
		return number.String()
	}
	return ""
}

func textFromJSONRaw(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	switch raw[0] {
	case '"':
		return rawScalarString(raw)
	case '[':
		var items []struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(raw, &items) == nil {
			var parts []string
			for _, item := range items {
				if text := strings.TrimSpace(item.Text); text != "" {
					parts = append(parts, text)
				}
			}
			return strings.Join(parts, "\n")
		}
	case '{':
		obj := rawToObject(raw)
		return firstNonEmptyString(jsonStringField(obj, "text", "content", "message", "output"))
	}
	return ""
}

func mustMarshalObject(obj map[string]json.RawMessage) []byte {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil
	}
	return data
}
