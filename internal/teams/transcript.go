package teams

import (
	"bufio"
	"bytes"
	"context"
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

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
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

const transcriptCheckpointFingerprintBytes int64 = 8 * 1024

type TranscriptParseOptions struct {
	SourceName       string
	InitialSessionID string
	InitialThreadID  string
	InitialTurnID    string
	InitialLineNo    int
	InitialOffset    int64
	// RequireFinalNewline makes file-backed/full-history parsing fail closed on
	// a writer tail that has not reached a JSONL boundary. The default remains
	// permissive for in-memory protocol fixtures and callers that already own a
	// stable complete snapshot.
	RequireFinalNewline bool
}

type Transcript struct {
	SourceName       string
	FileFingerprint  string
	SourceChangeTime int64
	ThreadID         string
	// SourceReadProof authenticates the bounded byte range consumed by an
	// incremental scan.  The cursor proof above covers only the trusted prefix;
	// keeping the read-range proof with the scan result lets the bridge reject a
	// same-size in-place rewrite of the newly read tail before it queues or
	// advances the checkpoint.  Full imports leave these fields empty because
	// they already carry their own cold-path proof.
	SourceReadProofFingerprint string
	SourceReadProofStartOffset int64
	SourceReadProofEndOffset   int64
	SourceReadProofRangeKnown  bool
	// UnresolvedContinuation is set by the bounded incremental scanner when
	// execution-bearing work appears after a prior terminal boundary. Callers
	// performing automatic delivery must fail closed rather than treating the
	// returned records as an ordinary backlog.
	UnresolvedContinuation bool
	// PendingContinuation means a root-shaped task_started followed a terminal
	// without enough evidence yet to classify it as either a new outer request
	// or an internal continuation. Automatic linked sync must leave its
	// checkpoint unchanged and rescan when the next record supplies ownership.
	PendingContinuation bool
	// QuarantinedFinals carries final candidates that the scanner could not
	// safely attribute to an execution. They are kept separate from Records so
	// automatic callers cannot accidentally publish them. The quarantine is a
	// history-only observation; it is never an execution-owner proof.
	QuarantinedFinals    []TranscriptRecord
	TranscriptQuarantine *teamstore.TranscriptQuarantine
	ContextGap           *teamstore.ContextGapState
	// TerminalBoundary is persisted independently of visible final delivery.
	// A terminal/status record can be the only reliable scope boundary in a
	// transcript, so linked import must retain its exact source proof across a
	// restart as well as history-watch.
	TerminalBoundary *teamstore.TerminalBoundary
	// PendingHistoryRange records a physically consumed but semantically
	// quarantined suffix. It prevents a safe scanner from rereading the same
	// unresolved marker forever while keeping its records out of automatic
	// visible delivery.
	PendingHistoryRange *teamstore.HistoryPendingRange
	// RootReleaseWitness is a transient, source-bound proof that a previously
	// quarantined root marker is followed by a complete external prompt.  It is
	// never persisted and never delivered; the bridge must validate it against
	// the durable Teams turn before clearing the semantic history frontier.
	RootReleaseWitness  *TranscriptRootReleaseWitness
	HistoryRootReleased bool
	// FinalBoundary is the latest final provenance observed by the bounded
	// scanner. It is persisted with the generic import cursor so a mirror that
	// arrives on the next poll is still recognized as an ambiguous anonymous
	// continuation rather than a new publishable answer.
	FinalBoundary *TranscriptFinalBoundary
	// Partial records an unterminated JSONL read that has not reached a safe
	// newline boundary. It is persisted as a bounded read hint only; callers
	// must not publish or advance LastOffset from it.
	Partial *TranscriptPartialProgress
	// PendingOpaqueRecord identifies a complete, previously partial record whose
	// newline is proven but whose payload is intentionally not materialized. It
	// is carried across bounded linked-transcript scans until the scanner can
	// consume that exact range and install an opaque context gap.
	PendingOpaqueRecord *TranscriptOpaqueRecordProgress
	// Consumed records a complete, newline-bounded record that the scanner
	// intentionally did not turn into a user-visible TranscriptRecord (for
	// example task_started/session metadata or an opaque tool envelope). It is
	// an ignored disposition, not a delivery claim. The bridge may persist this
	// cursor only when the scan has no unresolved/quarantined frontier.
	Consumed *TranscriptConsumedProgress
	// TailBudgetExhausted means the bounded scanner consumed a complete safe
	// prefix but stopped before source EOF. Import callers must keep the
	// checkpoint resumable at that prefix rather than treating the snapshot as
	// a complete EOF import.
	TailBudgetExhausted bool
	Records             []TranscriptRecord
	Diagnostics         []TranscriptDiagnostic
}

type TranscriptRootReleaseWitness struct {
	SourcePath       string
	SourceGeneration string
	RangeFingerprint string
	MarkerRecordID   string
	MarkerLine       int
	MarkerStart      int64
	MarkerEnd        int64
	MarkerThreadID   string
	MarkerTurnID     string
	PromptRecordID   string
	PromptLine       int
	PromptStart      int64
	PromptEnd        int64
	PromptThreadID   string
	PromptTurnID     string
	PromptTextHash   string
}

type TranscriptPartialProgress struct {
	LineStartOffset  int64
	ReadOffset       int64
	ObservedSize     int64
	Line             int
	StartedAt        time.Time
	SourceIdentity   string
	SourceChangeTime int64
	ReplayOffset     int64
	ReplayLine       int
	LastProgressAt   time.Time
	PrefixReleased   bool
}

type TranscriptOpaqueRecordProgress struct {
	StartOffset int64
	EndOffset   int64
	Line        int
	RecordID    string
}

type TranscriptConsumedProgress struct {
	RecordID string
	Line     int
	Offset   int64
}

type TranscriptFinalBoundary struct {
	ID               string
	Line             int
	StartOffset      int64
	StartOffsetKnown bool
	ThreadID         string
	TurnID           string
	TextHash         string
	TerminalSeen     bool
	TerminalLine     int
}

type TranscriptRecord struct {
	ItemID       string
	SourceItemID string
	DedupeKey    string
	ThreadID     string
	TurnID       string
	// TurnIDExplicit distinguishes a protocol turn_id on this record from an
	// ID inherited from parser context. Recovery must not treat an inherited
	// ID as proof that a final belongs to the requested Codex turn.
	TurnIDExplicit    bool
	Kind              TranscriptKind
	Text              string
	CreatedAt         time.Time
	SourceLine        int
	SourceStartOffset int64
	SourceOffset      int64
	SourceType        string
	Phase             string
	Internal          bool
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
	// A valid JSON value at a stable EOF is a complete logical JSONL record even
	// when the producer stopped before writing the optional final delimiter. The
	// incremental scanner applies the same rule; keeping the full-file path
	// strict stranded small transcripts forever after a normal process exit.
	return ParseCodexTranscript(f, TranscriptParseOptions{SourceName: sourceName, RequireFinalNewline: false})
}

// ReadSessionTranscriptFromOffset parses a suffix beginning at an already
// selected JSONL line boundary. It is intentionally a cold-path primitive for
// explicit history recovery; automatic linked polling must use the bounded
// stateful scanner so it can preserve parser context and ownership fences.
// InitialLineNo/InitialOffset keep source positions stable, while the initial
// protocol IDs let a recovery marker establish the context for ID-less suffix
// records without rereading the entire transcript.
func ReadSessionTranscriptFromOffset(filePath string, opts TranscriptParseOptions) (Transcript, error) {
	if strings.TrimSpace(filePath) == "" {
		return Transcript{}, fmt.Errorf("transcript path is required")
	}
	if opts.InitialOffset < 0 || opts.InitialLineNo < 0 {
		return Transcript{}, fmt.Errorf("transcript suffix boundary is invalid")
	}
	f, err := os.Open(filePath)
	if err != nil {
		return Transcript{}, err
	}
	defer f.Close()
	if _, err := f.Seek(opts.InitialOffset, io.SeekStart); err != nil {
		return Transcript{}, err
	}
	if strings.TrimSpace(opts.SourceName) == "" {
		opts.SourceName = filePath
	}
	return ParseCodexTranscript(f, opts)
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
		read, err := historyTieredReadJSONLRecord(reader, historyTieredMaxRecordBytes, historyTieredMaxRecordReadBytes)
		complete := read.Complete
		if read.BytesRead > 0 {
			line := read.Line
			lineNo++
			nextOffset := offset + read.BytesRead
			if complete && !read.Oversized {
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
		SourceName:          sourceName,
		InitialSessionID:    state.sessionID,
		InitialThreadID:     state.threadID,
		InitialTurnID:       state.turnID,
		InitialLineNo:       checkpointLine,
		InitialOffset:       checkpointOffset,
		RequireFinalNewline: false,
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
	_, _, ok := checkpointLineMatchRecords(line, lineNo, afterKey, state, sourceName)
	return ok
}

func checkpointLineMatchRecords(line []byte, lineNo int, afterKey string, state transcriptParseState, sourceName string) ([]TranscriptRecord, int, bool) {
	afterKey = strings.TrimSpace(afterKey)
	if afterKey == "" {
		return nil, -1, false
	}
	lineKey, hasLineKey := transcriptCheckpointLineNumber(afterKey)
	if hasLineKey && lineNo != lineKey {
		return nil, -1, false
	}
	probeKey := strings.TrimPrefix(afterKey, "source:")
	if probeKey == "" {
		return nil, -1, false
	}
	if !hasLineKey && !bytes.Contains(line, []byte(afterKey)) && !bytes.Contains(line, []byte(probeKey)) {
		return nil, -1, false
	}
	probeState := state
	records, _ := parseTranscriptLine(line, lineNo, &probeState)
	for i, record := range records {
		sourceID := strings.TrimSpace(record.SourceItemID)
		if sourceID != "" {
			if afterKey == sourceID || afterKey == "source:"+sourceID || afterKey == sourceID+"#line:"+strconv.Itoa(record.SourceLine) {
				return records, i, true
			}
		}
		if fallbackTranscriptItemID(transcriptFileFingerprint(sourceName, state.sessionID, nil), record.SourceLine, record.Kind) == afterKey {
			return records, i, true
		}
	}
	return records, -1, false
}

type transcriptCheckpointPosition struct {
	Line          int
	Offset        int64
	SourceSize    int64
	SourceModTime time.Time
}

func findTranscriptCheckpointPosition(filePath string, afterKey string) (transcriptCheckpointPosition, bool, error) {
	return findTranscriptCheckpointPositionWithContext(context.Background(), filePath, afterKey)
}

// findTranscriptCheckpointPositionWithContext is the cancellation-aware form
// used by automatic recovery.  Position reconstruction is a cold path, but it
// must still yield at JSONL record boundaries when a listener phase expires;
// otherwise a large inherited transcript can monopolize the phase.  The
// context cannot interrupt a single kernel/file read, so callers that need a
// hard process boundary must still use an external watchdog.
func findTranscriptCheckpointPositionWithContext(ctx context.Context, filePath string, afterKey string) (transcriptCheckpointPosition, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return transcriptCheckpointPosition{}, false, err
	}
	afterKey = strings.TrimSpace(afterKey)
	if strings.TrimSpace(filePath) == "" || afterKey == "" {
		return transcriptCheckpointPosition{}, false, nil
	}
	f, err := os.Open(filePath)
	if err != nil {
		return transcriptCheckpointPosition{}, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return transcriptCheckpointPosition{}, false, err
	}
	if info.IsDir() {
		return transcriptCheckpointPosition{}, false, fmt.Errorf("transcript path %q is a directory", filePath)
	}

	sourceName := filePath
	if abs, err := filepath.Abs(filePath); err == nil {
		sourceName = abs
	}

	reader := bufio.NewReaderSize(f, 64*1024)
	var state transcriptParseState
	lineNo := 0
	var offset int64
	for {
		if err := ctx.Err(); err != nil {
			return transcriptCheckpointPosition{}, false, err
		}
		read, err := historyTieredReadJSONLRecord(reader, historyTieredMaxRecordBytes, historyTieredMaxRecordReadBytes)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return transcriptCheckpointPosition{}, false, ctxErr
		}
		complete := read.Complete
		if read.BytesRead > 0 {
			line := read.Line
			lineNo++
			lineStartOffset := offset
			nextOffset := offset + read.BytesRead
			if complete && !read.Oversized {
				trimmed := bytes.TrimSpace(line)
				if len(trimmed) > 0 {
					records, index, ok := checkpointLineMatchRecords(trimmed, lineNo, afterKey, state, sourceName)
					if ok {
						pos := transcriptCheckpointPosition{
							Line:          lineNo,
							Offset:        nextOffset,
							SourceSize:    info.Size(),
							SourceModTime: info.ModTime(),
						}
						for i := index + 1; i < len(records); i++ {
							if records[i].SourceLine != records[index].SourceLine {
								break
							}
							if strings.TrimSpace(transcriptRecordCheckpointKey(records[i])) != "" {
								pos.Line = lineNo
								if pos.Line > 0 {
									pos.Line--
								}
								pos.Offset = lineStartOffset
								break
							}
						}
						return pos, true, nil
					}
					advanceTranscriptScanState(trimmed, lineNo, &state)
				}
			}
			offset = nextOffset
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return transcriptCheckpointPosition{}, false, err
		}
	}
	return transcriptCheckpointPosition{}, false, nil
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
		read, err := historyTieredReadJSONLRecord(reader, historyTieredMaxRecordBytes, historyTieredMaxRecordReadBytes)
		complete := read.Complete
		if read.BytesRead > 0 && complete && !read.Oversized {
			for sourceID := range sourceIDs {
				if bytes.Contains(read.Line, []byte(sourceID)) {
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
		read, err := historyTieredReadJSONLRecord(reader, historyTieredMaxRecordBytes, historyTieredMaxRecordReadBytes)
		complete := read.Complete || (!opts.RequireFinalNewline && err == io.EOF && read.BytesRead > 0)
		if read.BytesRead > 0 {
			lineStartOffset := offset
			lineNo++
			offset += read.BytesRead
			if read.Oversized && complete {
				transcript.Records = append(transcript.Records, historyTieredOversizedRecord(transcript.SourceName, lineNo, lineStartOffset, offset, read.Line))
				transcript.Diagnostics = append(transcript.Diagnostics, TranscriptDiagnostic{
					SourceLine: lineNo,
					Kind:       "oversized_record",
					Message:    "oversized JSONL record was retained as an opaque disposition",
				})
			} else if complete {
				_, _ = digest.Write(read.Line)
				trimmed := bytes.TrimSpace(read.Line)
				if len(trimmed) > 0 {
					records, diagnostics := parseTranscriptLine(trimmed, lineNo, &state)
					for i := range records {
						records[i].SourceStartOffset = lineStartOffset
						records[i].SourceOffset = offset
					}
					transcript.Records = append(transcript.Records, records...)
					transcript.Diagnostics = append(transcript.Diagnostics, diagnostics...)
				}
			} else {
				transcript.Diagnostics = append(transcript.Diagnostics, TranscriptDiagnostic{
					SourceLine: lineNo,
					Kind:       "incomplete_tail",
					Message:    "transcript source ended with an incomplete JSON record",
				})
				break
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
		// Internal collaboration records are retained for checkpoint progress,
		// but must not participate in visible assistant prefix compaction.
		if records[i].Internal {
			out = append(out, records[i])
			continue
		}
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
	if record.Internal {
		return false
	}
	sourceType := strings.ToLower(strings.TrimSpace(record.SourceType))
	return sourceType == "agent_message" || sourceType == "agentmessage" || sourceType == "assistant_message"
}

func transcriptRecordCanShadowStreamingAssistantPrefix(record TranscriptRecord) bool {
	if record.Internal {
		return false
	}
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
	phase        string
	internal     bool
}

func newInternalTranscriptRecord(sourceItemID string, threadID string, turnID string, kind TranscriptKind, createdAt time.Time, lineNo int, sourceType string, phase string) pendingTranscriptRecord {
	if kind == "" {
		kind = TranscriptKindUnknown
	}
	return pendingTranscriptRecord{
		sourceItemID: sourceItemID,
		threadID:     threadID,
		turnID:       turnID,
		kind:         kind,
		createdAt:    createdAt,
		sourceLine:   lineNo,
		sourceType:   sourceType,
		phase:        phase,
		internal:     true,
	}
}

func isInternalTranscriptType(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	return normalized == "reasoning" ||
		strings.HasPrefix(normalized, "reasoning_") ||
		strings.HasPrefix(normalized, "agent_reasoning") ||
		strings.HasPrefix(normalized, "token_count") ||
		strings.HasPrefix(normalized, "patch_apply")
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
	case "turn_context":
		// token_stats.go consumes this independently, but the JSONL line still
		// needs a durable transcript position so linked sync does not rescan it
		// on every poll. The finalizer supplies a stable line-based ID when the
		// event has no explicit source ID.
		payload, _ := jsonObjectField(obj, "payload")
		turnID = firstNonEmptyString(
			jsonStringField(obj, "turn_id", "turnId"),
			jsonStringField(payload, "turn_id", "turnId"),
			nestedJSONID(payload, "turn"),
			turnID,
		)
		if turnID != "" {
			state.turnID = turnID
		}
		return []TranscriptRecord{newInternalTranscriptRecord(
			firstNonEmptyString(jsonStringField(obj, "id", "context_id", "contextId"), nestedJSONID(obj, "payload")),
			threadID,
			turnID,
			TranscriptKindUnknown,
			createdAt,
			lineNo,
			lineType,
			jsonStringField(obj, "phase"),
		).toRecord()}, nil
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
	metadata, _ := jsonObjectField(payload, "internal_chat_message_metadata_passthrough")
	turnID = firstNonEmptyString(
		jsonStringField(payload, "turn_id", "turnId"),
		nestedJSONID(payload, "turn"),
		jsonStringField(metadata, "turn_id", "turnId"),
		turnID,
	)
	phase := jsonStringField(payload, "phase")
	if transcriptEnvelopeIsInternalAgentMessage(payload) {
		// Keep only the source identity and position.  The collaboration
		// envelope can contain encrypted content or a plaintext FINAL_ANSWER;
		// neither belongs in the in-memory visible transcript or history-watch
		// pending-assistant state.
		return pendingTranscriptRecord{
			sourceItemID: sourceID,
			threadID:     threadID,
			turnID:       turnID,
			kind:         TranscriptKindAssistant,
			createdAt:    createdAt,
			sourceLine:   lineNo,
			sourceType:   itemType,
			phase:        phase,
			internal:     true,
		}, true
	}
	if isInternalTranscriptType(itemType) {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, itemType, phase), true
	}
	if strings.TrimSpace(itemType) != "" && kindFromType(itemType) == TranscriptKindUnknown {
		// Do not decode content from an explicitly unknown response item. It is
		// not a public transcript surface, and fail-closed classification also
		// avoids allocating its potentially large payload.
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, itemType, phase), true
	}
	kind, text, ok := responseItemKindText(payload)
	if !ok {
		return pendingTranscriptRecord{}, false
	}
	if kind == TranscriptKindUnknown {
		kind = transcriptKindFromTypeOrRole(itemType, jsonStringField(payload, "role"))
		if kind == TranscriptKindUnknown {
			return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, itemType, phase), true
		}
	}
	if kind == TranscriptKindAssistant && strings.EqualFold(phase, "commentary") {
		kind = TranscriptKindStatus
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
		phase:        phase,
	}, true
}

func transcriptEnvelopeIsInternalAgentMessage(payload map[string]json.RawMessage) bool {
	itemType := strings.ToLower(strings.TrimSpace(jsonStringField(payload, "type")))
	if itemType != "agent_message" && itemType != "agentmessage" {
		return false
	}
	if _, ok := payload["internal_chat_message_metadata_passthrough"]; ok {
		return true
	}
	return strings.TrimSpace(jsonStringField(payload, "author")) != "" &&
		strings.TrimSpace(jsonStringField(payload, "recipient")) != ""
}

func eventMsgTranscriptRecord(payload map[string]json.RawMessage, lineNo int, createdAt time.Time, threadID string, turnID string) (pendingTranscriptRecord, bool) {
	eventType := jsonStringField(payload, "type")
	sourceID := jsonStringField(payload, "id", "item_id", "itemId", "message_id", "messageId")
	threadID = firstNonEmptyString(jsonStringField(payload, "thread_id", "threadId"), threadID)
	turnID = firstNonEmptyString(jsonStringField(payload, "turn_id", "turnId"), turnID)
	phase := jsonStringField(payload, "phase")
	if transcriptEnvelopeIsInternalAgentMessage(payload) {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindAssistant, createdAt, lineNo, eventType, phase), true
	}
	if isInternalTranscriptType(eventType) {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, eventType, phase), true
	}

	kind := transcriptKindFromTypeOrRole(eventType, jsonStringField(payload, "role"))
	if kind == TranscriptKindUnknown {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, eventType, phase), true
	}
	if kind == TranscriptKindAssistant && strings.EqualFold(phase, "commentary") {
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
		phase:        phase,
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
	phase := firstNonEmptyString(jsonStringField(item, "phase"), jsonStringField(obj, "phase"))
	if transcriptEnvelopeIsInternalAgentMessage(item) {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindAssistant, createdAt, lineNo, itemType, phase), true
	}
	if isInternalTranscriptType(itemType) {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, itemType, phase), true
	}
	role := strings.ToLower(strings.TrimSpace(jsonStringField(item, "role")))
	if role == "system" || role == "developer" {
		return pendingTranscriptRecord{}, false
	}
	kind := transcriptKindFromTypeOrRole(itemType, role)
	if kind == TranscriptKindUnknown {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, itemType, phase), true
	}
	if kind == TranscriptKindAssistant && strings.EqualFold(phase, "commentary") {
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
		phase:        phase,
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
	if transcriptEnvelopeIsInternalAgentMessage(obj) {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindAssistant, createdAt, lineNo, sourceType, jsonStringField(obj, "phase")), true
	}
	if isInternalTranscriptType(sourceType) {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, sourceType, jsonStringField(obj, "phase")), true
	}
	kind := transcriptKindFromTypeOrRole(sourceType, jsonStringField(obj, "role"))
	if kind == TranscriptKindUnknown {
		return newInternalTranscriptRecord(sourceID, threadID, turnID, TranscriptKindUnknown, createdAt, lineNo, sourceType, jsonStringField(obj, "phase")), true
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

func transcriptKindFromTypeOrRole(typeValue string, role string) TranscriptKind {
	typeValue = strings.TrimSpace(typeValue)
	if typeValue == "" {
		return kindFromRole(role)
	}
	// Legacy message envelopes use role to distinguish user and assistant,
	// while an explicit future/unknown type must fail closed rather than
	// inheriting a visible assistant role.
	if strings.EqualFold(typeValue, "message") {
		if roleKind := kindFromRole(role); roleKind != TranscriptKindUnknown {
			return roleKind
		}
	}
	return kindFromType(typeValue)
}

func responseItemKindText(payload map[string]json.RawMessage) (TranscriptKind, string, bool) {
	itemType := strings.ToLower(strings.TrimSpace(jsonStringField(payload, "type")))
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
		phase:        jsonStringField(obj, "phase"),
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
		Phase:        p.phase,
		Internal:     p.internal,
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

// transcriptCheckpointSourceFingerprint hashes a bounded window ending at a
// trusted JSONL cursor and binds it to the stable source-file identity.  The
// identity is intentionally part of the existing fingerprint field so old
// JSON/SQLite rows remain readable without a schema migration while an atomic
// replacement cannot reuse a prefix proof from the previous inode. Size/mtime
// are only change hints; an in-place rewrite can preserve both. Keeping the
// content window small avoids turning every unchanged-file poll into a full
// transcript read.
func transcriptCheckpointSourceFingerprint(sourcePath string, offset int64) string {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return ""
	}
	f, err := os.Open(sourcePath)
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() || info.Size() < 0 {
		return ""
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(sourcePath, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		return ""
	}
	return transcriptCheckpointSourceFingerprintFromReaderWithIdentity(f, sourcePath, identity, info.Size(), offset)
}

func transcriptCheckpointSourceFingerprintFromReader(reader io.ReaderAt, sourcePath string, sourceSize int64, offset int64) string {
	var identity string
	var err error
	if file, ok := reader.(*os.File); ok {
		if info, statErr := file.Stat(); statErr == nil {
			identity, err = teamstore.SourceFileIdentityFromFileInfo(sourcePath, info)
		} else {
			err = statErr
		}
	} else {
		identity, err = teamstore.SourceFileIdentity(sourcePath)
	}
	if err != nil || strings.TrimSpace(identity) == "" {
		return ""
	}
	return transcriptCheckpointSourceFingerprintFromReaderWithIdentity(reader, sourcePath, identity, sourceSize, offset)
}

func transcriptCheckpointSourceFingerprintFromReaderWithIdentity(reader io.ReaderAt, sourcePath string, identity string, sourceSize int64, offset int64) string {
	sourcePath = strings.TrimSpace(sourcePath)
	identity = strings.TrimSpace(identity)
	if reader == nil || sourcePath == "" || identity == "" || sourceSize < 0 {
		return ""
	}
	// A negative offset is the explicit sentinel for the end of the source.
	// Zero is a valid, known empty-prefix cursor and must not silently become
	// EOF: doing so lets legacy/unknown checkpoints appear trusted.
	if offset < 0 {
		offset = sourceSize
	}
	if offset > sourceSize {
		return ""
	}
	start := offset - transcriptCheckpointFingerprintBytes
	if start < 0 {
		start = 0
	}
	length := offset - start
	if length < 0 {
		return ""
	}
	window := make([]byte, length)
	if length > 0 {
		if n, err := reader.ReadAt(window, start); err != nil || n != len(window) {
			return ""
		}
	}
	h := sha256.New()
	_, _ = h.Write([]byte(identity))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(filepath.Clean(sourcePath)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(start, 10)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(window)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// transcriptSourceRangeFingerprint authenticates the bytes that were present
// in the incremental read window, not just the small cursor suffix used by the
// checkpoint proof.  It is used only by the cold transcript-import path: a
// same-size in-place rewrite in the middle of a large read window must not be
// able to pass the old-cursor proof and move the checkpoint past stale records.
func transcriptSourceRangeFingerprint(sourcePath string, start, end int64) string {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" || start < 0 || end < start {
		return ""
	}
	f, err := os.Open(sourcePath)
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() || end > info.Size() {
		return ""
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(sourcePath, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		return ""
	}
	fingerprint := transcriptSourceRangeFingerprintFromReader(f, sourcePath, identity, start, end)
	if fingerprint == "" {
		return ""
	}
	fdInfo, err := f.Stat()
	if err != nil || fdInfo.IsDir() || !os.SameFile(info, fdInfo) || fdInfo.Size() < end {
		return ""
	}
	// Keep the path check as well as the descriptor check: an atomic replace
	// must not bind a proof read from the old descriptor to the new pathname.
	postInfo, err := os.Stat(sourcePath)
	if err != nil || postInfo.IsDir() || !os.SameFile(info, postInfo) || postInfo.Size() < end {
		return ""
	}
	return fingerprint
}

// transcriptSourceRangeFingerprintFromReader hashes an exact byte range from
// the already-open source descriptor. Callers that use the result as a proof
// must fstat the descriptor after this function returns and compare it with
// the pathname; this helper deliberately does not reopen the path.
func transcriptSourceRangeFingerprintFromReader(reader io.ReaderAt, sourcePath string, identity string, start, end int64) string {
	sourcePath = strings.TrimSpace(sourcePath)
	identity = strings.TrimSpace(identity)
	if reader == nil || sourcePath == "" || identity == "" || start < 0 || end < start {
		return ""
	}
	h := sha256.New()
	_, _ = h.Write([]byte(identity))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(filepath.Clean(sourcePath)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(start, 10)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(end, 10)))
	_, _ = h.Write([]byte{0})
	if _, err := io.Copy(h, io.NewSectionReader(reader, start, end-start)); err != nil {
		return ""
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
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
