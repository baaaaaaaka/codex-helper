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
	"sort"
	"strings"
	"time"
)

const historyTieredTailReaderSize = 4 * 1024

type historyTieredFileState struct {
	Path        string
	Size        int64
	ModTime     time.Time
	Offset      int64
	Line        int
	SessionID   string
	ThreadID    string
	TurnID      string
	LastFinalID string

	pendingAssistant historyTieredAssistantCandidate
}

type historyTieredAssistantCandidate struct {
	Record TranscriptRecord
}

type historyTieredStatChange struct {
	Path      string
	Size      int64
	ModTime   time.Time
	Truncated bool
}

type historyTieredTailResult struct {
	State        historyTieredFileState
	Records      []TranscriptRecord
	Finals       []historyTieredFinal
	Truncated    bool
	TooLarge     bool
	Incomplete   bool
	BytesRead    int64
	LinesRead    int
	MaxTailBytes int64
}

type historyTieredFinal struct {
	Key          string
	Record       TranscriptRecord
	TerminalLine int
	TerminalKind string
}

func historyTieredDetectStatChanges(paths []string, states map[string]historyTieredFileState) ([]historyTieredStatChange, error) {
	changes := make([]historyTieredStatChange, 0)
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return changes, err
		}
		if info.IsDir() {
			continue
		}
		state := states[path]
		truncated := state.Size > 0 && info.Size() < state.Size
		if state.Size == info.Size() && state.ModTime.Equal(info.ModTime()) {
			continue
		}
		changes = append(changes, historyTieredStatChange{
			Path:      path,
			Size:      info.Size(),
			ModTime:   info.ModTime(),
			Truncated: truncated,
		})
	}
	return changes, nil
}

func historyTieredListSessionFilesInDirs(dirs []string) ([]string, error) {
	files := make([]string, 0)
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return files, err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
				continue
			}
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

func historyTieredScanTail(path string, previous historyTieredFileState, maxTailBytes int64) (historyTieredTailResult, error) {
	info, err := os.Stat(path)
	if err != nil {
		return historyTieredTailResult{}, err
	}
	next := previous
	next.Path = path
	next.Size = info.Size()
	next.ModTime = info.ModTime()
	if previous.Offset > info.Size() {
		next.Size = 0
		next.ModTime = time.Time{}
		next.Offset = 0
		next.Line = 0
		next.pendingAssistant = historyTieredAssistantCandidate{}
		return historyTieredTailResult{State: next, Truncated: true}, nil
	}

	tailBytes := info.Size() - previous.Offset
	if maxTailBytes > 0 && tailBytes > maxTailBytes {
		return historyTieredTailResult{
			State:        previous,
			TooLarge:     true,
			MaxTailBytes: maxTailBytes,
		}, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return historyTieredTailResult{}, err
	}
	defer f.Close()
	if previous.Offset > 0 {
		if _, err := f.Seek(previous.Offset, io.SeekStart); err != nil {
			return historyTieredTailResult{}, err
		}
	}

	parseState := transcriptParseState{
		sessionID: strings.TrimSpace(previous.SessionID),
		threadID:  strings.TrimSpace(previous.ThreadID),
		turnID:    strings.TrimSpace(previous.TurnID),
	}
	reader := bufio.NewReaderSize(f, historyTieredTailReaderSize)
	lineNo := previous.Line
	offset := previous.Offset
	pending := previous.pendingAssistant
	result := historyTieredTailResult{
		State:        next,
		MaxTailBytes: maxTailBytes,
	}
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if err == io.EOF && !bytes.HasSuffix(line, []byte("\n")) {
				result.Incomplete = true
				break
			}
			lineStartOffset := offset
			lineNo++
			offset += int64(len(line))
			result.BytesRead += int64(len(line))
			result.LinesRead++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				signals := historyTieredLineSignals(trimmed)
				records, _ := parseTranscriptLine(trimmed, lineNo, &parseState)
				for i := range records {
					records[i].SourceStartOffset = lineStartOffset
					records[i].SourceOffset = offset
				}
				result.Records = append(result.Records, records...)
				for _, record := range records {
					if record.Kind == TranscriptKindAssistant && strings.TrimSpace(record.Text) != "" {
						pending = historyTieredAssistantCandidate{Record: record}
						if signals.FinalAnswer {
							final := historyTieredFinalFromCandidate(pending, record.SourceLine, "final_answer")
							if final.Key != "" && final.Key != next.LastFinalID {
								result.Finals = append(result.Finals, final)
								next.LastFinalID = final.Key
							}
							pending = historyTieredAssistantCandidate{}
						}
					}
				}
				if signals.TurnCompleted {
					terminalTurnID := firstNonEmptyString(signals.TurnID, parseState.turnID)
					if pending.Record.Text != "" && historyTieredTurnMatches(pending.Record.TurnID, terminalTurnID) {
						final := historyTieredFinalFromCandidate(pending, lineNo, signals.TerminalKind)
						if final.Key != "" && final.Key != next.LastFinalID {
							result.Finals = append(result.Finals, final)
							next.LastFinalID = final.Key
						}
						pending = historyTieredAssistantCandidate{}
					}
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return result, err
		}
	}

	next.Offset = offset
	next.Line = lineNo
	next.SessionID = parseState.sessionID
	next.ThreadID = parseState.threadID
	next.TurnID = parseState.turnID
	next.pendingAssistant = pending
	if len(result.Records) > 0 {
		transcript := Transcript{
			SourceName:      path,
			FileFingerprint: transcriptFileFingerprint(path, parseState.sessionID, nil),
			Records:         compactTranscriptRecords(result.Records),
		}
		finalizeTranscriptRecordIDs(&transcript)
		result.Records = transcript.Records
	}
	result.State = next
	return result, nil
}

type historyTieredLineSignal struct {
	FinalAnswer   bool
	TurnCompleted bool
	TerminalKind  string
	TurnID        string
}

func historyTieredLineSignals(line []byte) historyTieredLineSignal {
	var signal historyTieredLineSignal
	var obj map[string]json.RawMessage
	if bytes.Contains(line, []byte(`"final_answer"`)) {
		if err := json.Unmarshal(line, &obj); err == nil {
			signal.FinalAnswer = historyTieredHasFinalAnswerPhase(obj)
		}
	}
	if !bytes.Contains(line, []byte(`"turn.completed"`)) && !bytes.Contains(line, []byte(`"turn/completed"`)) {
		return signal
	}
	if obj == nil {
		if err := json.Unmarshal(line, &obj); err != nil {
			return signal
		}
	}
	if obj == nil {
		return signal
	}
	lineType := jsonStringField(obj, "type")
	method := jsonStringField(obj, "method")
	switch {
	case lineType == "turn.completed":
		signal.TurnCompleted = true
		signal.TerminalKind = lineType
		signal.TurnID = firstNonEmptyString(jsonStringField(obj, "turn_id", "turnId"), nestedJSONID(obj, "turn"))
	case method == "turn/completed":
		signal.TurnCompleted = true
		signal.TerminalKind = method
		params, _ := jsonObjectField(obj, "params")
		signal.TurnID = firstNonEmptyString(jsonStringField(params, "turn_id", "turnId"), nestedJSONID(params, "turn"))
	}
	return signal
}

func historyTieredHasFinalAnswerPhase(obj map[string]json.RawMessage) bool {
	if strings.EqualFold(jsonStringField(obj, "phase"), "final_answer") {
		return true
	}
	for _, field := range []string{"payload", "item", "params"} {
		child, ok := jsonObjectField(obj, field)
		if !ok {
			continue
		}
		if strings.EqualFold(jsonStringField(child, "phase"), "final_answer") {
			return true
		}
	}
	return false
}

func historyTieredTurnMatches(recordTurnID string, terminalTurnID string) bool {
	recordTurnID = strings.TrimSpace(recordTurnID)
	terminalTurnID = strings.TrimSpace(terminalTurnID)
	return recordTurnID == "" || terminalTurnID == "" || recordTurnID == terminalTurnID
}

func historyTieredFinalFromCandidate(candidate historyTieredAssistantCandidate, terminalLine int, terminalKind string) historyTieredFinal {
	record := candidate.Record
	key := historyTieredCompletionKey(record, terminalLine, terminalKind)
	if key == "" {
		return historyTieredFinal{}
	}
	return historyTieredFinal{
		Key:          key,
		Record:       record,
		TerminalLine: terminalLine,
		TerminalKind: terminalKind,
	}
}

func historyTieredCompletionKey(record TranscriptRecord, terminalLine int, terminalKind string) string {
	threadID := strings.TrimSpace(record.ThreadID)
	if threadID == "" {
		threadID = "unknown-thread"
	}
	turnID := strings.TrimSpace(record.TurnID)
	if turnID == "" {
		turnID = "unknown-turn"
	}
	sourceID := strings.TrimSpace(record.SourceItemID)
	if sourceID != "" {
		return "codex-final:v1:" + threadID + ":" + turnID + ":" + sourceID
	}
	textHash := sha256.Sum256([]byte(strings.TrimSpace(record.Text)))
	return fmt.Sprintf("codex-final:v1:%s:%s:terminal-line:%d:final-line:%d:%s:%s",
		threadID,
		turnID,
		terminalLine,
		record.SourceLine,
		historyTieredKeyPart(terminalKind),
		hex.EncodeToString(textHash[:8]),
	)
}

func historyTieredKeyPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "unknown"
	}
	return out
}
