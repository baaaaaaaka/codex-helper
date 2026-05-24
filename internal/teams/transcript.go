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
	"unicode/utf8"
)

type TranscriptKind string

const (
	TranscriptKindUser      TranscriptKind = "user"
	TranscriptKindAssistant TranscriptKind = "assistant"
	TranscriptKindTool      TranscriptKind = "tool"
	TranscriptKindStatus    TranscriptKind = "status"
	TranscriptKindCompact   TranscriptKind = "compact"
	TranscriptKindArtifact  TranscriptKind = "artifact"
	TranscriptKindUnknown   TranscriptKind = "unknown"
)

const transcriptContextCompactMessage = "Context compacted. Earlier turns were summarized so Codex can continue the thread."

type TranscriptParseOptions struct {
	SourceName       string
	InitialSessionID string
	InitialThreadID  string
	InitialTurnID    string
	InitialLineNo    int
	InitialOffset    int64
}

type Transcript struct {
	SourceName      string
	FileFingerprint string
	ThreadID        string
	Records         []TranscriptRecord
	Diagnostics     []TranscriptDiagnostic
}

type TranscriptRecord struct {
	ItemID            string
	SourceItemID      string
	DedupeKey         string
	ThreadID          string
	TurnID            string
	Kind              TranscriptKind
	Text              string
	CreatedAt         time.Time
	SourceLine        int
	SourceStartOffset int64
	SourceOffset      int64
	SourceType        string
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
	afterKey = strings.TrimSpace(afterKey)
	if afterKey == "" {
		return ReadSessionTranscript(filePath)
	}
	if transcript, ok, err := readSessionTranscriptSinceFast(filePath, afterKey); err != nil {
		return transcript, err
	} else if ok {
		return transcript, nil
	}
	transcript, err := ReadSessionTranscript(filePath)
	if err != nil {
		return transcript, err
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

func readSessionTranscriptSinceFast(filePath string, afterKey string) (Transcript, bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return Transcript{}, false, err
	}
	defer f.Close()

	sourceName := filePath
	if abs, err := filepath.Abs(filePath); err == nil {
		sourceName = abs
	}

	reader := bufio.NewReaderSize(f, 64*1024)
	var state transcriptParseState
	lineNo := 0
	var offset int64
	var checkpointOffset int64 = -1
	checkpointLine := 0
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			nextOffset := offset + int64(len(line))
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				if checkpointLineMatches(trimmed, lineNo, afterKey, state, sourceName) {
					advanceTranscriptScanState(trimmed, lineNo, &state)
					checkpointOffset = nextOffset
					checkpointLine = lineNo
					break
				}
				advanceTranscriptScanState(trimmed, lineNo, &state)
			}
			offset = nextOffset
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return Transcript{}, false, err
		}
	}
	if checkpointOffset < 0 {
		return Transcript{}, false, nil
	}
	if _, err := f.Seek(checkpointOffset, io.SeekStart); err != nil {
		return Transcript{}, false, err
	}
	transcript, err := ParseCodexTranscript(f, TranscriptParseOptions{
		SourceName:       sourceName,
		InitialSessionID: state.sessionID,
		InitialThreadID:  state.threadID,
		InitialTurnID:    state.turnID,
		InitialLineNo:    checkpointLine,
		InitialOffset:    checkpointOffset,
	})
	if err != nil {
		return transcript, false, err
	}
	if transcriptSuffixMayNeedPrefixSourceCounts(filePath, checkpointOffset, transcript.Records) {
		return Transcript{}, false, nil
	}
	return transcript, true, nil
}

func checkpointLineMatches(line []byte, lineNo int, afterKey string, state transcriptParseState, sourceName string) bool {
	afterKey = strings.TrimSpace(afterKey)
	if afterKey == "" {
		return false
	}
	lineKey, hasLineKey := transcriptCheckpointLineNumber(afterKey)
	if hasLineKey && lineNo != lineKey {
		return false
	}
	probeKey := strings.TrimPrefix(afterKey, "source:")
	if probeKey == "" {
		return false
	}
	if !hasLineKey && !bytes.Contains(line, []byte(afterKey)) && !bytes.Contains(line, []byte(probeKey)) {
		return false
	}
	probeState := state
	records, _ := parseTranscriptLine(line, lineNo, &probeState)
	for _, record := range records {
		sourceID := strings.TrimSpace(record.SourceItemID)
		if sourceID != "" {
			if afterKey == sourceID || afterKey == "source:"+sourceID || afterKey == sourceID+"#line:"+strconv.Itoa(record.SourceLine) {
				return true
			}
		}
		if fallbackTranscriptItemID(transcriptFileFingerprint(sourceName, state.sessionID, nil), record.SourceLine, record.Kind) == afterKey {
			return true
		}
	}
	return false
}

func transcriptCheckpointLineNumber(key string) (int, bool) {
	key = strings.TrimSpace(key)
	for _, marker := range []string{"#line:", ":line:"} {
		idx := strings.Index(key, marker)
		if idx < 0 {
			continue
		}
		start := idx + len(marker)
		end := start
		for end < len(key) && key[end] >= '0' && key[end] <= '9' {
			end++
		}
		if end == start {
			continue
		}
		lineNo, err := strconv.Atoi(key[start:end])
		if err == nil && lineNo > 0 {
			return lineNo, true
		}
	}
	return 0, false
}

func advanceTranscriptScanState(line []byte, lineNo int, state *transcriptParseState) {
	if len(line) == 0 || state == nil {
		return
	}
	if !bytes.Contains(line, []byte(`"session_meta"`)) &&
		!bytes.Contains(line, []byte(`"thread.started"`)) &&
		!bytes.Contains(line, []byte(`"turn.started"`)) &&
		!bytes.Contains(line, []byte(`"turn.completed"`)) &&
		!bytes.Contains(line, []byte(`"method"`)) {
		return
	}
	_, _ = parseTranscriptLine(line, lineNo, state)
}

func transcriptSuffixMayNeedPrefixSourceCounts(filePath string, checkpointOffset int64, records []TranscriptRecord) bool {
	sourceIDs := make(map[string]struct{})
	for _, record := range records {
		if sourceID := strings.TrimSpace(record.SourceItemID); sourceID != "" {
			sourceIDs[sourceID] = struct{}{}
		}
	}
	if len(sourceIDs) == 0 {
		return false
	}
	f, err := os.Open(filePath)
	if err != nil {
		return true
	}
	defer f.Close()
	reader := bufio.NewReaderSize(io.LimitReader(f, checkpointOffset), 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			for sourceID := range sourceIDs {
				if bytes.Contains(line, []byte(sourceID)) {
					return true
				}
			}
		}
		if err != nil {
			return err != io.EOF
		}
	}
}

func ParseCodexTranscript(r io.Reader, opts TranscriptParseOptions) (Transcript, error) {
	state := transcriptParseState{
		sessionID: strings.TrimSpace(opts.InitialSessionID),
		threadID:  strings.TrimSpace(opts.InitialThreadID),
		turnID:    strings.TrimSpace(opts.InitialTurnID),
	}
	transcript := Transcript{SourceName: strings.TrimSpace(opts.SourceName)}
	reader := bufio.NewReaderSize(r, 64*1024)
	digest := sha256.New()
	lineNo := opts.InitialLineNo
	offset := opts.InitialOffset

	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineStartOffset := offset
			lineNo++
			offset += int64(len(line))
			_, _ = digest.Write(line)
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				records, diagnostics := parseTranscriptLine(trimmed, lineNo, &state)
				for i := range records {
					records[i].SourceStartOffset = lineStartOffset
					records[i].SourceOffset = offset
				}
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
	transcript.Records = compactTranscriptRecords(transcript.Records)
	transcript.FileFingerprint = transcriptFileFingerprint(transcript.SourceName, state.sessionID, digest.Sum(nil))
	finalizeTranscriptRecordIDs(&transcript)
	return transcript, nil
}

func compactTranscriptRecords(records []TranscriptRecord) []TranscriptRecord {
	if len(records) == 0 {
		return records
	}
	out := records[:0]
	for i := 0; i < len(records); i++ {
		if compactedRecordIsShadowedByEvent(records, i) {
			continue
		}
		if assistantRecordIsPrefixShadowedByLaterAssistant(records, i) {
			continue
		}
		out = append(out, records[i])
	}
	return out
}

func compactedRecordIsShadowedByEvent(records []TranscriptRecord, index int) bool {
	if index < 0 || index+1 >= len(records) {
		return false
	}
	current := records[index]
	next := records[index+1]
	return current.Kind == TranscriptKindCompact &&
		strings.EqualFold(strings.TrimSpace(current.SourceType), "compacted") &&
		next.Kind == TranscriptKindCompact &&
		strings.EqualFold(strings.TrimSpace(next.SourceType), "context_compacted")
}

func assistantRecordIsPrefixShadowedByLaterAssistant(records []TranscriptRecord, index int) bool {
	if index < 0 || index+1 >= len(records) {
		return false
	}
	current := records[index]
	currentText := strings.TrimSpace(current.Text)
	if current.Kind != TranscriptKindAssistant || !transcriptRecordCanBeStreamingAssistantPrefix(current) || utf8.RuneCountInString(currentText) < 40 {
		return false
	}
	for i := index + 1; i < len(records); i++ {
		next := records[i]
		if next.Kind == TranscriptKindUser {
			return false
		}
		if next.Kind != TranscriptKindAssistant {
			continue
		}
		if !transcriptRecordCanShadowStreamingAssistantPrefix(next) {
			continue
		}
		if !transcriptRecordsCanShadowSameAssistant(current, next) {
			continue
		}
		nextText := strings.TrimSpace(next.Text)
		if len(nextText) > len(currentText) && strings.HasPrefix(nextText, currentText) {
			return true
		}
	}
	return false
}

func transcriptRecordsCanShadowSameAssistant(left TranscriptRecord, right TranscriptRecord) bool {
	leftThread := strings.TrimSpace(left.ThreadID)
	rightThread := strings.TrimSpace(right.ThreadID)
	if leftThread != "" && rightThread != "" && leftThread != rightThread {
		return false
	}
	leftTurn := strings.TrimSpace(left.TurnID)
	rightTurn := strings.TrimSpace(right.TurnID)
	if leftTurn != "" && rightTurn != "" && leftTurn != rightTurn {
		return false
	}
	return true
}

func transcriptRecordCanBeStreamingAssistantPrefix(record TranscriptRecord) bool {
	sourceType := strings.ToLower(strings.TrimSpace(record.SourceType))
	return sourceType == "agent_message" || sourceType == "agentmessage" || sourceType == "assistant_message"
}

func transcriptRecordCanShadowStreamingAssistantPrefix(record TranscriptRecord) bool {
	sourceType := strings.ToLower(strings.TrimSpace(record.SourceType))
	return sourceType == "message" || sourceType == "agent_message" || sourceType == "agentmessage" || sourceType == "assistant_message"
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
	case "thread/compacted":
		record := contextCompactTranscriptRecord(params, method, lineNo, createdAt, threadID, turnID)
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
	if kind == TranscriptKindCompact {
		return contextCompactTranscriptRecord(payload, eventType, lineNo, createdAt, threadID, turnID), true
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
	role := strings.ToLower(strings.TrimSpace(jsonStringField(item, "role")))
	if role == "system" || role == "developer" {
		return pendingTranscriptRecord{}, false
	}
	kind := kindFromRole(role)
	if kind == TranscriptKindUnknown {
		kind = kindFromType(itemType)
	}
	if kind == TranscriptKindAssistant && strings.EqualFold(jsonStringField(item, "phase"), "commentary") {
		kind = TranscriptKindStatus
	}
	text := firstNonEmptyString(
		jsonStringField(item, "text", "content", "message", "output"),
		textFromJSONRaw(item["content"]),
	)
	if kind == TranscriptKindCompact && strings.TrimSpace(text) == "" {
		text = transcriptContextCompactMessage
	}
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
	if kind == TranscriptKindCompact && strings.TrimSpace(text) == "" {
		text = transcriptContextCompactMessage
	}
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
	case "context_compaction", "context_compacted":
		return TranscriptKindCompact, transcriptContextCompactMessage, true
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
		if kind == TranscriptKindCompact && strings.TrimSpace(text) == "" {
			text = transcriptContextCompactMessage
		}
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

func contextCompactTranscriptRecord(obj map[string]json.RawMessage, sourceType string, lineNo int, createdAt time.Time, threadID string, turnID string) pendingTranscriptRecord {
	return pendingTranscriptRecord{
		sourceItemID: jsonStringField(obj, "id", "item_id", "itemId", "compaction_id", "compactionId"),
		threadID:     threadID,
		turnID:       turnID,
		kind:         TranscriptKindCompact,
		text:         transcriptContextCompactMessage,
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   sourceType,
	}
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
	case "context_compacted", "context_compaction", "thread/compacted", "compacted":
		return TranscriptKindCompact
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
