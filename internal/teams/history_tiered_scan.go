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
	"strconv"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const historyTieredTailReaderSize = 4 * 1024

// A tail budget may end in the middle of one JSONL record. Allow that single
// record to finish without widening the normal scan to the rest of the file;
// genuinely pathological records remain resumable/quarantined at the same
// line boundary.
const historyTieredMaxRecordBytes = 8 * 1024 * 1024

// historyTieredMaxRecordReadBytes is an independent framing safety valve. A
// complete JSONL record may be larger than the per-pass history budget (for
// example an image accidentally serialized as base64), but a newline-less
// record must not make one poll read an unbounded file. The scanner persists a
// partial read cursor when this cap is reached and resumes only after growth.
const historyTieredMaxRecordReadBytes int64 = 64 * 1024 * 1024

// A complete oversized record is still a safe newline-bounded disposition.
// Its read-range proof may therefore cover the per-pass budget plus the full
// bounded record envelope, rather than the smaller decoded-record cap.  This
// keeps a real image/base64 record (for example ~10 MiB) recoverable without
// allowing an unbounded persisted proof to make the sender reread a file.
const historyTieredMaxReadProofBytes int64 = historyTieredMaxTailBytes + historyTieredMaxRecordReadBytes

var (
	historyTieredContinuationMarkers = [][]byte{
		[]byte(`"task_started"`), []byte(`"task.started"`), []byte(`"task/started"`), []byte(`"task-started"`),
		[]byte(`"goal_continuation"`), []byte(`"goal.continuation"`), []byte(`"goal/continuation"`), []byte(`"goal-continuation"`),
		[]byte(`"turn_context"`), []byte(`"turn.context"`), []byte(`"turn/context"`), []byte(`"turn-context"`),
	}
	historyTieredTerminalMarkers = [][]byte{
		[]byte(`"final_answer"`),
		[]byte(`"turn_completed"`), []byte(`"turn.completed"`), []byte(`"turn/completed"`), []byte(`"turn-completed"`),
		[]byte(`"turn_failed"`), []byte(`"turn.failed"`), []byte(`"turn/failed"`), []byte(`"turn-failed"`),
		[]byte(`"task_complete"`), []byte(`"task_completed"`), []byte(`"task.completed"`), []byte(`"task/complete"`), []byte(`"task-complete"`),
		[]byte(`"task_failed"`), []byte(`"task.failed"`), []byte(`"task/failed"`), []byte(`"task-failed"`),
	}
)

type historyTieredFileState struct {
	Path                          string
	Size                          int64
	ModTime                       time.Time
	SourceFingerprint             string
	SourceRewriteBlocked          bool
	LegacySourceUnverified        bool
	OversizedRecordBlocked        bool
	SourceRewriteRecoveryIdentity string
	Offset                        int64
	Line                          int
	// Partial* represent a record that has not reached a newline. Offset and
	// Line remain the last complete JSONL boundary; these fields are only a
	// resumable read hint and never a publishable cursor.
	PartialLineStartOffset int64
	PartialReadOffset      int64
	PartialObservedSize    int64
	PartialLine            int
	PartialStartedAt       time.Time
	PartialSourceIdentity  string
	SessionID              string
	ThreadID               string
	TeamsOriginThreadID    string
	TurnID                 string
	TeamsOriginTurnID      string
	// ExternalUserPromptSeen is a positive ownership hint for a new root turn.
	// It is set only by a visible, non-internal user record after the most recent
	// terminal boundary and is consumed by the following task_started marker.
	// Goal-continuation context is filtered and never sets this bit.  Keeping the
	// hint scoped to the post-terminal portion is important: the outer Teams
	// prompt that preceded task_complete must not prove a later S462 child.
	ExternalUserPromptSeen    bool
	LastFinalID               string
	LastFinalLine             int
	LastFinalStartOffset      int64
	LastFinalStartOffsetKnown bool
	LastFinalThreadID         string
	LastFinalTurnID           string
	LastFinalTextHash         string
	TerminalBoundarySeen      bool
	TerminalBoundaryLine      int
	// UnresolvedContinuation is set when a task/goal continuation appears after
	// a previously observed final boundary. It is intentionally carried with
	// the bounded scan state so recovery cannot infer ownership from a reused
	// Codex turn ID alone.
	UnresolvedContinuation       bool
	UnresolvedContinuationLine   int
	UnresolvedContinuationOffset int64
	// PendingRootTaskStarted is a root-shaped task_started seen after a
	// terminal boundary without a preceding visible user prompt.  Keep the
	// tail quarantined while waiting for that prompt; if none arrives before
	// the scan ends, it is promoted to UnresolvedContinuation.
	PendingRootTaskStarted       bool
	PendingRootTaskStartedLine   int
	PendingRootTaskStartedOffset int64
	// Transient end boundary for the pending marker. The durable checkpoint
	// stores the resulting ignored disposition, not the marker payload.
	PendingRootTaskStartedEndOffset int64
	// TranscriptQuarantine is a history-only frontier. It is intentionally
	// separate from UnresolvedContinuation: ambiguous transcript mirrors do
	// not prove that an app-server execution is still alive.
	TranscriptQuarantine *teamstore.TranscriptQuarantine

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
	State   historyTieredFileState
	Records []TranscriptRecord
	Finals  []historyTieredFinal
	// QuarantinedFinals is retained separately when a final group is
	// ambiguous. The ordinary Records/Finals views intentionally omit that
	// group so callers cannot publish it without an explicit boundary choice.
	QuarantinedFinals []historyTieredFinal
	Truncated         bool
	TooLarge          bool
	// BudgetExhausted means the scanner consumed a safe complete prefix of a
	// tail larger than MaxTailBytes. TooLarge is retained for compatibility with
	// older callers, but BudgetExhausted makes this a resumable result rather
	// than a hard history gate.
	BudgetExhausted bool
	// OversizedRecord means the scanner reached one complete JSONL record that
	// exceeds the explicit per-record cap. It is a durable quarantine boundary,
	// not resumable byte-budget progress.
	OversizedRecord      bool
	Incomplete           bool
	BytesRead            int64
	LinesRead            int
	MaxTailBytes         int64
	ReadProofFingerprint string
	ReadProofStartOffset int64
	ReadProofEndOffset   int64
	ReadProofRangeKnown  bool
	LastConsumedRecordID string
	LastConsumedLine     int
	LastConsumedOffset   int64
}

type historyTieredRecordRead struct {
	Line      []byte
	Complete  bool
	Oversized bool
	// BytesRead includes prefix bytes already consumed by the bounded reader.
	// It is used to advance the safe cursor past a complete oversized record.
	BytesRead int64
}

func historyTieredReadJSONLRecord(reader *bufio.Reader, maxBytes int64, maxReadBytes int64) (historyTieredRecordRead, error) {
	if reader == nil || maxBytes <= 0 || maxReadBytes <= 0 {
		return historyTieredRecordRead{}, nil
	}
	var line []byte
	var total int64
	oversized := false
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			total += int64(len(chunk))
			if !oversized {
				if total <= maxBytes {
					line = append(line, chunk...)
				} else {
					oversized = true
					if int64(len(line)) > maxBytes {
						line = line[:maxBytes]
					}
				}
			}
			if bytes.HasSuffix(chunk, []byte{'\n'}) {
				return historyTieredRecordRead{Line: line, Complete: true, Oversized: oversized, BytesRead: total}, nil
			}
		}
		if total >= maxReadBytes {
			return historyTieredRecordRead{Line: line, Oversized: oversized, BytesRead: total}, nil
		}
		if err != nil {
			if err == io.EOF {
				if total == 0 {
					return historyTieredRecordRead{}, io.EOF
				}
				// The framer only calls a record complete after seeing its JSONL
				// newline. Parser callers may explicitly treat a non-empty EOF as
				// the final record boundary, while the incremental history scanner
				// must retain it as a partial tail until a newline is durable.
				return historyTieredRecordRead{Line: line, Oversized: oversized, BytesRead: total}, io.EOF
			}
			if err == bufio.ErrBufferFull {
				continue
			}
			return historyTieredRecordRead{Line: line, Oversized: oversized, BytesRead: total}, err
		}
	}
}

// historyTieredLineHint is a deliberately conservative, allocation-free
// prefilter.  It only decides whether a line is worth sending through one of
// the existing structured parsers; it never decides that a line is a final or
// an execution boundary.  All ownership/quarantine decisions remain based on
// the parsed records below.
//
// The protocol marker spellings all share these quoted prefixes:
// task_*, task.*, task/*, task-*; goal_*; turn_* / turn.* / turn/* / turn-*;
// and final_answer.  A false positive merely performs the old JSON parse; a
// false negative would be unsafe, so keep this list broader than the exact
// marker set above.
type historyTieredLineHint struct {
	MayContinuation  bool
	MayTerminal      bool
	MayTerminalEvent bool
	MayFinal         bool
	MayTurnField     bool
}

func historyTieredLineHints(line []byte) historyTieredLineHint {
	var hint historyTieredLineHint
	for i, value := range line {
		if value != '"' || i+1 >= len(line) {
			continue
		}
		rest := line[i+1:]
		switch {
		case len(rest) >= 4 && rest[0] == 't' && rest[1] == 'a' && rest[2] == 's' && rest[3] == 'k':
			hint.MayContinuation = true
			hint.MayTerminal = true
			hint.MayTerminalEvent = true
		case len(rest) >= 4 && rest[0] == 'g' && rest[1] == 'o' && rest[2] == 'a' && rest[3] == 'l':
			hint.MayContinuation = true
		case len(rest) >= 4 && rest[0] == 't' && rest[1] == 'u' && rest[2] == 'r' && rest[3] == 'n':
			hint.MayTurnField = true
			// `turn_id`, `turnId`, and a nested `turn` object are provenance
			// fields, not continuation/terminal markers.  Keep those on the
			// cheap turn-ID path without forcing the continuation parser.
			turnField := (len(rest) >= 7 && rest[4] == '_' && rest[5] == 'i' && rest[6] == 'd') ||
				(len(rest) >= 6 && rest[4] == 'I' && rest[5] == 'd') ||
				(len(rest) >= 5 && rest[4] == '"')
			turnEvent := false
			if !turnField && len(rest) >= 6 {
				suffix := rest[4]
				if suffix == '_' && !(len(rest) >= 7 && rest[5] == 'i' && rest[6] == 'd') {
					turnEvent = true
				} else if (suffix == '.' || suffix == '/' || suffix == '-') && historyTieredASCIIAlpha(rest[5]) {
					turnEvent = true
				}
			}
			if turnEvent {
				hint.MayContinuation = true
				hint.MayTerminal = true
				hint.MayTerminalEvent = true
			}
		case len(rest) >= 5 && rest[0] == 'f' && rest[1] == 'i' && rest[2] == 'n' && rest[3] == 'a' && rest[4] == 'l':
			hint.MayTerminal = true
			hint.MayFinal = true
		}
		if hint.MayContinuation && hint.MayTerminal && hint.MayFinal && hint.MayTurnField {
			break
		}
	}
	return hint
}

func historyTieredASCIIAlpha(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z')
}

type historyTieredFinal struct {
	Key                      string
	Record                   TranscriptRecord
	TerminalLine             int
	TerminalKind             string
	NoTurnIDNeedsQuarantine  bool
	TranscriptOnlyQuarantine bool
}

func historyTieredDetectStatChanges(paths []string, states map[string]historyTieredFileState, verifyUnchangedOpt ...bool) ([]historyTieredStatChange, error) {
	verifyUnchanged := len(verifyUnchangedOpt) > 0 && verifyUnchangedOpt[0]
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
		// A bounded scan can deliberately leave a durable cursor before the
		// observed EOF. The file may be unchanged since that scan, but the
		// unread suffix is still work and must be revisited on the next poll.
		// Stat equality alone cannot represent this resumable state.
		if state.Offset > 0 && state.Offset < info.Size() {
			changes = append(changes, historyTieredStatChange{
				Path:      path,
				Size:      info.Size(),
				ModTime:   info.ModTime(),
				Truncated: truncated,
			})
			continue
		}
		if state.Size == info.Size() && state.ModTime.Equal(info.ModTime()) {
			if !verifyUnchanged || info.Size() == 0 {
				continue
			}
			fingerprint := strings.TrimSpace(state.SourceFingerprint)
			if fingerprint == "" {
				// A legacy checkpoint without content provenance is not an
				// unchanged proof. Reconcile it once so the next poll can use
				// the bounded fingerprint.
			} else if current := strings.TrimSpace(transcriptCheckpointSourceFingerprint(path, state.Offset)); current != "" && current == fingerprint {
				continue
			}
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
	if previous.Offset > info.Size() || (previous.Size > 0 && info.Size() < previous.Size) {
		// The source was replaced or truncated. Do not carry an old final
		// boundary, turn identity, or unresolved-continuation marker into the
		// new file; the next pass reparses its fresh session metadata.
		next = historyTieredFileState{Path: path}
		return historyTieredTailResult{State: next, Truncated: true}, nil
	}
	if previous.PartialLineStartOffset >= previous.Offset && previous.PartialReadOffset > previous.PartialLineStartOffset {
		identity := historyTieredSourceIdentity(path, info)
		if strings.TrimSpace(previous.PartialSourceIdentity) != "" && identity != "" && identity != strings.TrimSpace(previous.PartialSourceIdentity) {
			next := previous
			next.Path = path
			next.Size = info.Size()
			next.ModTime = info.ModTime()
			return historyTieredTailResult{State: next, Truncated: true}, nil
		}
		if previous.PartialReadOffset > info.Size() {
			next := previous
			next.Path = path
			next.Size = info.Size()
			next.ModTime = info.ModTime()
			return historyTieredTailResult{State: next, Truncated: true}, nil
		}
		if previous.PartialReadOffset == info.Size() && previous.PartialObservedSize == info.Size() && previous.ModTime.Equal(info.ModTime()) {
			next := previous
			next.Path = path
			next.Size = info.Size()
			next.ModTime = info.ModTime()
			return historyTieredTailResult{State: next, Incomplete: true, MaxTailBytes: maxTailBytes}, nil
		}
		if hasNewline, err := historyTieredPartialHasNewline(path, previous.PartialReadOffset, info.Size()); err != nil {
			return historyTieredTailResult{}, err
		} else if !hasNewline {
			next := previous
			next.Path = path
			next.Size = info.Size()
			next.ModTime = info.ModTime()
			next.PartialReadOffset = info.Size()
			next.PartialObservedSize = info.Size()
			next.PartialSourceIdentity = firstNonEmptyString(next.PartialSourceIdentity, identity)
			return historyTieredTailResult{State: next, Incomplete: true, MaxTailBytes: maxTailBytes}, nil
		}
		// The writer completed the record. Re-read only that record from its
		// start with the normal bounded parser; this keeps the persisted state
		// small and avoids a cross-poll JSON lexer.
		previous.Offset = previous.PartialLineStartOffset
		previous.Line = previous.PartialLine - 1
		if previous.Line < 0 {
			previous.Line = 0
		}
		previous.PartialLineStartOffset = 0
		previous.PartialReadOffset = 0
		previous.PartialObservedSize = 0
		previous.PartialLine = 0
		previous.PartialStartedAt = time.Time{}
		previous.PartialSourceIdentity = ""
	}
	// The partial-completion branch rewinds the local scan cursor to the start
	// of the now-complete record. Refresh the result state after that rewind so
	// stale Partial* fields are not persisted alongside the new newline cursor.
	next = previous
	next.Path = path
	next.Size = info.Size()
	next.ModTime = info.ModTime()

	tailBytes := info.Size() - previous.Offset
	scanEnd := info.Size()
	budgeted := maxTailBytes > 0 && tailBytes > maxTailBytes
	if budgeted {
		scanEnd = previous.Offset + maxTailBytes
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
	// Hash the exact bounded suffix consumed by this scan while it is read.
	// Limiting the reader to the initial file size makes an append racing the
	// scan part of the next poll rather than silently widening this proof.
	readEnd := info.Size()
	readProof := sha256.New()
	readProofReady := false
	if identity, identityErr := teamstore.SourceFileIdentityFromFileInfo(path, info); identityErr == nil && strings.TrimSpace(identity) != "" {
		_, _ = readProof.Write([]byte(identity))
		_, _ = readProof.Write([]byte{0})
		_, _ = readProof.Write([]byte(filepath.Clean(path)))
		_, _ = readProof.Write([]byte{0})
		_, _ = readProof.Write([]byte(strconv.FormatInt(previous.Offset, 10)))
		_, _ = readProof.Write([]byte{0})
		_, _ = readProof.Write([]byte(strconv.FormatInt(readEnd, 10)))
		_, _ = readProof.Write([]byte{0})
		readProofReady = true
	}

	parseState := transcriptParseState{
		sessionID: strings.TrimSpace(previous.SessionID),
		threadID:  strings.TrimSpace(previous.ThreadID),
		turnID:    strings.TrimSpace(previous.TurnID),
	}
	reader := bufio.NewReaderSize(io.LimitReader(f, scanEnd-previous.Offset), historyTieredTailReaderSize)
	lineNo := previous.Line
	offset := previous.Offset
	pending := previous.pendingAssistant
	// LastFinalID is also persisted with the last parsed turn identity. A
	// subsequent task_started is ambiguous only when it lacks the root-turn
	// markers used by a normal new Codex turn; child/goal continuations do not
	// carry those markers. This preserves incremental detection without
	// treating every ordinary next turn as an orphan.
	// Older HistoryWatch checkpoints persisted only a byte cursor.  They may
	// resume immediately after a terminal record without any final-boundary
	// provenance.  Treat every non-zero legacy cursor as a conservative
	// terminal scope: a child task_started is then quarantined, while a proven
	// ordinary root task_started below can still reset the scope.  This is a
	// one-time safety bias for pre-anchor state and avoids attributing a child
	// final to the previous Teams turn.
	legacyCursorBoundary := previous.Offset > 0 && strings.TrimSpace(previous.LastFinalID) == ""
	// A linked checkpoint with a generic record key and no remembered turn ID
	// still needs a conservative task-started boundary so an unprompted root
	// marker becomes pending instead of being treated as a fresh owner.
	legacyCheckpointBoundary := previous.Offset > 0 && strings.TrimSpace(previous.LastFinalID) != "" && strings.TrimSpace(previous.TurnID) == "" && !previous.TerminalBoundarySeen && !previous.UnresolvedContinuation
	// A legacy no-offset checkpoint may contain only a final de-duplication key,
	// without a source cursor, turn identity, or explicit terminal boundary.
	// Treat that exact shape as a conservative visible-final scope. Offset-based
	// linked checkpoints use LastRecordID as a generic record key and retain
	// normal mirror compatibility unless the bounded prefix probe established a
	// real terminal boundary.
	legacyNoOffsetCheckpointBoundary := previous.Offset == 0 && strings.TrimSpace(previous.LastFinalID) != "" && strings.TrimSpace(previous.TurnID) == "" && !previous.TerminalBoundarySeen && !previous.UnresolvedContinuation
	// A few pre-anchor callers persisted the outer turn identity alongside only
	// the final de-duplication key.  That is not enough evidence to classify an
	// ordinary event_msg mirror as a child (doing so regresses valid backfill),
	// but it is sufficient to apply the stricter response_item provenance check
	// below.  response_item is the app-server's canonical visible-final shape
	// and must not inherit the old outer turn when it appears with a new/missing
	// identity after such a checkpoint.
	legacyResponseItemBoundary := previous.Offset > 0 && strings.TrimSpace(previous.LastFinalID) != "" && strings.TrimSpace(previous.TurnID) != "" && !previous.TerminalBoundarySeen && !previous.UnresolvedContinuation
	// Only a boundary persisted by a prior scan proves that this tail follows a
	// completed scope. A final observed earlier in the same scan is still part
	// of the current batch (older transcripts can contain multiple no-ID final
	// records), so it must not make the next line look like a child by itself.
	persistedFinalBoundary := previous.TerminalBoundarySeen || previous.UnresolvedContinuation
	// A legacy cursor or checkpoint key is useful for deciding how to treat a
	// subsequent task_started marker, but it is not proof that the saved record
	// was a terminal execution boundary. Older healthy transcripts commonly
	// resume at a generic assistant record and then emit a new no-ID
	// response_item final; treating the cursor itself as terminal would turn
	// that compatible mirror shape into a false orphan. Only an explicit
	// terminal observed by this state machine (or a boundary already persisted
	// by a previous scan) authorizes no-ID final quarantine.
	terminalBoundarySeen := previous.TerminalBoundarySeen || previous.UnresolvedContinuation
	terminalEventBoundarySeen := previous.TerminalBoundarySeen || previous.UnresolvedContinuation
	previousAnonymousFinalCandidate := previous.TerminalBoundarySeen && !previous.UnresolvedContinuation &&
		previous.LastFinalStartOffsetKnown && (previous.LastFinalStartOffset > 0 || previous.LastFinalLine > 0) &&
		strings.TrimSpace(previous.LastFinalID) != "" && strings.TrimSpace(previous.LastFinalTurnID) == ""
	next.TerminalBoundarySeen = previous.TerminalBoundarySeen || previous.UnresolvedContinuation
	if next.TerminalBoundarySeen && next.TerminalBoundaryLine == 0 {
		next.TerminalBoundaryLine = previous.LastFinalLine
	}
	// Final-text de-duplication is a checkpoint-boundary check, not a
	// same-scan check.  `next` is updated after every final in this scan; using
	// it here would suppress the second record in a newly observed anonymous
	// mirror pair before compaction can quarantine the pair.  Keep the durable
	// boundary separate and clear it only when this scan proves a fresh root
	// turn, never when it merely observes another final.
	finalDedupBoundary := previous
	suppressFinalsAfterContinuation := previous.UnresolvedContinuation || previous.PendingRootTaskStarted
	externalUserPromptSeen := previous.ExternalUserPromptSeen
	pendingRootTaskStarted := previous.PendingRootTaskStarted && !previous.UnresolvedContinuation
	pendingRootTaskStartedLine := previous.PendingRootTaskStartedLine
	pendingRootTaskStartedOffset := previous.PendingRootTaskStartedOffset
	pendingRootTaskStartedEndOffset := previous.PendingRootTaskStartedEndOffset
	result := historyTieredTailResult{
		State:        next,
		MaxTailBytes: maxTailBytes,
		TooLarge:     budgeted,
	}
	for {
		read, err := historyTieredReadJSONLRecord(reader, historyTieredMaxRecordBytes, historyTieredMaxRecordReadBytes)
		line := read.Line
		if read.BytesRead > 0 {
			lineStartOffset := offset
			if !read.Complete {
				if budgeted && lineStartOffset+read.BytesRead >= scanEnd {
					completed, readErr := historyTieredCompleteBudgetedLine(f, line, read.BytesRead, readEnd-scanEnd, historyTieredMaxRecordBytes)
					if readErr != nil {
						return result, readErr
					}
					if completed.Complete {
						if completed.Oversized {
							// A complete oversized record has a safe JSONL boundary even
							// though its payload is intentionally not decoded. Keep the
							// cursor moving and emit a deterministic opaque disposition so
							// later status/final records remain reachable.
							lineNo++
							offset = lineStartOffset + completed.BytesRead
							result.BytesRead += completed.BytesRead
							result.LinesRead++
							result.OversizedRecord = true
							if record := historyTieredOversizedRecord(path, lineNo, lineStartOffset, offset, line); record.ItemID != "" {
								result.Records = append(result.Records, record)
							}
							// Preserve the per-pass budget. The next poll starts at the
							// complete boundary and can reach the following records.
							break
						}
						line = completed.Line
						read = completed
						err = nil
					} else {
						result.Incomplete = true
						next.Size = info.Size()
						next.PartialLineStartOffset = lineStartOffset
						next.PartialReadOffset = lineStartOffset + completed.BytesRead
						next.PartialObservedSize = info.Size()
						next.PartialLine = lineNo + 1
						next.PartialStartedAt = firstNonZeroTime(previous.PartialStartedAt, info.ModTime())
						next.PartialSourceIdentity = firstNonEmptyString(previous.PartialSourceIdentity, historyTieredSourceIdentity(path, info))
						break
					}
				} else {
					result.Incomplete = true
					// Keep the source-size cursor at the last complete byte. The
					// file may grow by completing this same line; recording the full
					// current size would make the next pass incorrectly take the
					// unchanged fast path and lose the tail.
					next.Size = info.Size()
					next.PartialLineStartOffset = lineStartOffset
					next.PartialReadOffset = lineStartOffset + read.BytesRead
					next.PartialObservedSize = info.Size()
					next.PartialLine = lineNo + 1
					next.PartialStartedAt = firstNonZeroTime(previous.PartialStartedAt, info.ModTime())
					next.PartialSourceIdentity = firstNonEmptyString(previous.PartialSourceIdentity, historyTieredSourceIdentity(path, info))
					break
				}
			}
			if readProofReady {
				_, _ = readProof.Write(line)
			}
			lineNo++
			offset += read.BytesRead
			result.BytesRead += read.BytesRead
			result.LinesRead++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				hints := historyTieredLineHints(trimmed)
				signals := historyTieredLineSignalsWithHints(trimmed, hints)
				if signals.TurnCompleted || signals.TerminalFailed {
					terminalEventBoundarySeen = true
				}
				priorTurnID := strings.TrimSpace(parseState.turnID)
				if priorTurnID == "" {
					priorTurnID = strings.TrimSpace(previous.LastFinalTurnID)
				}
				// Continuation markers are sparse. Avoid unmarshalling every ordinary
				// assistant/tool/status line merely to discover that it is not a
				// task/goal boundary; explicit task_started/goal_continuation tokens
				// are sufficient candidates for the structured check.
				if hints.MayContinuation {
					if continuationType, continuationTurnID, continuation, ordinaryRootStart := historyTieredContinuationSignal(trimmed); continuation {
						// app-server reuses the ordinary root task_started shape for
						// internal goal continuations (including model_context_window and
						// collaboration_mode_kind).  Once this cursor is known to follow a
						// terminal, those fields are not ownership proof: keep the
						// fail-closed boundary and require an explicit history import or a
						// separately proven outer execution.  The legacy byte-only watcher
						// path (no LastFinalID) remains the only path that can use the root
						// markers as a fresh-turn proof.
						// A byte-only legacy cursor is not ownership proof.  Older
						// checkpoints have no terminal provenance and are exactly where a
						// persistent child can resemble a fresh root task.  Require the
						// visible prompt (or a later explicit operator import) before
						// releasing this boundary.
						rootProof := externalUserPromptSeen
						// Older app-server versions may omit the root metadata from
						// task_started, and some versions use turn_context as the
						// execution-boundary marker. Treat both as root-like only while
						// waiting for a visible prompt; a final before that proof remains
						// unresolved rather than being attributed to the prior owner.
						rootLikeStart := ordinaryRootStart || continuationType == "task_started" || continuationType == "turn_context"
						// A checkpoint that already carried a final key and turn identity
						// but predates the explicit boundary fields is also ambiguous: it
						// can be the old linked-state shape for an S462 continuation.
						rootBoundary := terminalBoundarySeen || legacyCursorBoundary || legacyCheckpointBoundary || legacyNoOffsetCheckpointBoundary || legacyResponseItemBoundary
						pendingRootStart := rootLikeStart && rootBoundary && !rootProof
						if pendingRootStart {
							pendingRootTaskStarted = true
							pendingRootTaskStartedLine = lineNo - 1
							pendingRootTaskStartedOffset = lineStartOffset
							pendingRootTaskStartedEndOffset = offset
							suppressFinalsAfterContinuation = true
						}
						externalUserPromptSeen = false
						if rootLikeStart && !pendingRootStart {
							pendingRootTaskStarted = false
							pendingRootTaskStartedLine = 0
							pendingRootTaskStartedOffset = 0
							pendingRootTaskStartedEndOffset = 0
							// A proven new root turn starts a fresh terminal scope. Clear
							// any old child marker so one ambiguous continuation cannot
							// permanently pause an otherwise healthy local session.
							terminalBoundarySeen = false
							next.LastFinalID = ""
							next.LastFinalLine = 0
							next.LastFinalStartOffset = 0
							next.LastFinalStartOffsetKnown = false
							next.LastFinalThreadID = ""
							next.LastFinalTurnID = ""
							next.LastFinalTextHash = ""
							finalDedupBoundary.LastFinalThreadID = ""
							finalDedupBoundary.LastFinalTurnID = ""
							finalDedupBoundary.LastFinalTextHash = ""
							next.TerminalBoundarySeen = false
							next.TerminalBoundaryLine = 0
							next.UnresolvedContinuation = false
							suppressFinalsAfterContinuation = false
						}
						if rootBoundary && !rootLikeStart && !pendingRootStart {
							if !next.UnresolvedContinuation {
								next.UnresolvedContinuation = true
								next.UnresolvedContinuationLine = lineNo - 1
								next.UnresolvedContinuationOffset = lineStartOffset
							}
							suppressFinalsAfterContinuation = true
						}
						// A continuation may omit turn_id on its later records. Clear
						// the inherited outer ID, or use the child identity when one is
						// present, so a child final cannot be attributed to the outer turn.
						parseState.turnID = strings.TrimSpace(continuationTurnID)
					}
				}
				records, diagnostics := parseTranscriptLine(trimmed, lineNo, &parseState)
				if len(diagnostics) > 0 {
					// Keep the cursor before the malformed complete line. Treating
					// it as consumed could let a later final be recovered under an
					// inherited turn identity.
					result.Incomplete = true
					offset = lineStartOffset
					lineNo--
					unresolved := next.UnresolvedContinuation
					unresolvedLine := next.UnresolvedContinuationLine
					unresolvedOffset := next.UnresolvedContinuationOffset
					next = previous
					next.Path = path
					next.Size = offset
					next.ModTime = info.ModTime()
					next.UnresolvedContinuation = unresolved
					next.UnresolvedContinuationLine = unresolvedLine
					next.UnresolvedContinuationOffset = unresolvedOffset
					break
				}
				// A complete newline-bounded line always has a durable ignored
				// disposition, even when the parser emits no TranscriptRecord.
				// This is what lets a marker-only or opaque tool line move the
				// linked cursor without making the scanner publish it.
				result.LastConsumedRecordID = historyTieredConsumedRecordKey(path, lineNo, lineStartOffset, offset)
				result.LastConsumedLine = lineNo
				result.LastConsumedOffset = offset
				for i := range records {
					records[i].TurnIDExplicit = historyTieredLineHasExplicitTurnIDWithHint(trimmed, hints.MayTurnField)
					records[i].SourceStartOffset = lineStartOffset
					records[i].SourceOffset = offset
				}
				if pendingRootTaskStarted && !next.UnresolvedContinuation && (signals.FinalAnswer || signals.TurnCompleted || signals.TerminalFailed) {
					next.UnresolvedContinuation = true
					next.UnresolvedContinuationLine = pendingRootTaskStartedLine
					next.UnresolvedContinuationOffset = pendingRootTaskStartedOffset
					pendingRootTaskStarted = false
					suppressFinalsAfterContinuation = true
				}
				knownFinalBoundary := persistedFinalBoundary || legacyNoOffsetCheckpointBoundary || (legacyCursorBoundary && priorTurnID != "")
				// terminalBoundarySeen is the state before this line is emitted. Include
				// it as a boundary proof so a no-ID event_msg/agent_message final on
				// the next line cannot inherit the previous outer turn. Do not use the
				// current line's own final as proof; the first final in a fresh scope
				// remains compatible with older transcripts.
				// A final observed earlier in this same scan is not by itself enough
				// to classify the next anonymous final as a child: older transcripts
				// can contain more than one no-ID final in one complete tail.  Keep
				// the fail-closed rule for a persisted boundary, an explicit terminal
				// event, or a same-scan final whose prior record established an outer
				// turn identity.  The last case is the important no-ID child shape:
				// an anonymous final after an explicitly-owned outer final cannot be
				// attributed to that owner.
				scopeBoundaryBeforeFinal := persistedFinalBoundary || terminalEventBoundarySeen || (terminalBoundarySeen && priorTurnID != "")
				anonymousFinalNeedsQuarantine := false
				transcriptOnlyFinalNeedsQuarantine := false
				if ambiguous, childTurnID, anonymousOnly := historyTieredVisibleFinalNeedsQuarantine(records, priorTurnID, scopeBoundaryBeforeFinal, knownFinalBoundary || terminalEventBoundarySeen, legacyNoOffsetCheckpointBoundary, terminalEventBoundarySeen); ambiguous {
					// A no-ID final is not enough to establish a live child owner. Keep
					// it as a candidate until the bounded scan can see whether a second
					// final is the normal event_msg/response_item mirror. A single
					// candidate is still promoted to the real execution fence below.
					deferAsTranscriptQuarantine := anonymousOnly || previousAnonymousFinalCandidate || historyTieredFinalsCouldBeMixedIDMirror(result.Finals, records)
					if deferAsTranscriptQuarantine {
						anonymousFinalNeedsQuarantine = anonymousOnly
						transcriptOnlyFinalNeedsQuarantine = previousAnonymousFinalCandidate
						if anonymousOnly {
							// parseTranscriptLine may have carried the previous outer
							// turn ID into a record whose source line did not contain
							// one. Clear that inherited value before the final is
							// materialized; otherwise the end-of-scan promotion would
							// mistake the anonymous candidate for the old owner.
							for i := range records {
								if records[i].Kind == TranscriptKindAssistant && !records[i].TurnIDExplicit {
									records[i].TurnID = ""
								}
							}
						}
						childTurnID = ""
					} else {
						if !next.UnresolvedContinuation {
							next.UnresolvedContinuation = true
							next.UnresolvedContinuationLine = lineNo - 1
							next.UnresolvedContinuationOffset = lineStartOffset
						}
						suppressFinalsAfterContinuation = true
					}
					parseState.turnID = strings.TrimSpace(childTurnID)
				}
				// Some app-server revisions emit a child response_item without a
				// preceding task_started event.  An explicit visible turn ID that
				// differs from the prior terminal scope is still an ownership
				// boundary; internal collaboration response_items remain ordinary
				// skippable records and do not poison the watcher.
				// A legacy byte-only cursor is not itself proof that the preceding
				// record was a successful terminal.  Allow the first explicit
				// response_item after that baseline; once a real final boundary (or
				// an already unresolved continuation) is known, missing/different
				// provenance remains fail-closed.
				// A legacy checkpoint that still carries the prior turn identity is
				// sufficient for the response_item provenance guard: an explicit
				// different child ID (or an anonymous response-only final) must not
				// inherit that old turn. The ordinary linked bridge path does not
				// populate TurnID for its generic checkpoint, so valid no-ID mirrors
				// remain compatible there.
				responseItemAnonymousFinalNeedsQuarantine := false
				responseItemTranscriptOnlyFinalNeedsQuarantine := false
				responseItemBoundarySeen := terminalBoundarySeen || legacyNoOffsetCheckpointBoundary || legacyResponseItemBoundary || legacyCursorBoundary
				if responseItemBoundarySeen && (knownFinalBoundary || priorTurnID != "") && !suppressFinalsAfterContinuation && historyTieredLineIsResponseItem(trimmed) {
					for i := range records {
						record := &records[i]
						if record.Internal || record.Kind != TranscriptKindAssistant || !strings.EqualFold(strings.TrimSpace(record.Phase), "final_answer") {
							continue
						}
						if !record.TurnIDExplicit || strings.TrimSpace(record.TurnID) == "" {
							// Missing execution identity remains a deferred candidate. If
							// it is the only candidate, the end-of-scan promotion below
							// creates the real unresolved execution fence; if a typed
							// mirror is present, it becomes transcript-only quarantine.
							record.TurnID = ""
							responseItemAnonymousFinalNeedsQuarantine = true
							continue
						}
						if previousAnonymousFinalCandidate && historyTieredFinalMatchesPreviousAnonymous(*record, previous) {
							// A typed response_item arriving after an anonymous final
							// may be the canonical mirror from the next poll. Defer it
							// to transcript-only compaction instead of promoting it to
							// a new execution owner here.
							responseItemTranscriptOnlyFinalNeedsQuarantine = true
							continue
						}
						if priorTurnID != "" && strings.TrimSpace(record.TurnID) == priorTurnID {
							continue
						}
						next.UnresolvedContinuation = true
						next.UnresolvedContinuationLine = lineNo - 1
						next.UnresolvedContinuationOffset = lineStartOffset
						suppressFinalsAfterContinuation = true
						parseState.turnID = strings.TrimSpace(record.TurnID)
						break
					}
				}
				for _, record := range records {
					if !record.Internal && record.Kind == TranscriptKindUser && strings.TrimSpace(record.Text) != "" && !shouldSkipTranscriptUserText(record.Text) {
						externalUserPromptSeen = true
						if (pendingRootTaskStarted || terminalBoundarySeen) && !next.UnresolvedContinuation {
							// A visible user prompt following a root-shaped task_started
							// proves that it was a normal new outer request rather than
							// an internal goal continuation. This also handles a marker
							// whose ignored cursor was committed on the previous poll;
							// an already durable unresolved anchor remains fail-closed.
							pendingRootTaskStarted = false
							pendingRootTaskStartedLine = 0
							pendingRootTaskStartedOffset = 0
							pendingRootTaskStartedEndOffset = 0
							terminalBoundarySeen = false
							suppressFinalsAfterContinuation = false
							next.LastFinalID = ""
							next.LastFinalLine = 0
							next.LastFinalStartOffset = 0
							next.LastFinalStartOffsetKnown = false
							next.LastFinalThreadID = ""
							next.LastFinalTurnID = ""
							next.LastFinalTextHash = ""
							finalDedupBoundary.LastFinalThreadID = ""
							finalDedupBoundary.LastFinalTurnID = ""
							finalDedupBoundary.LastFinalTextHash = ""
							next.TerminalBoundarySeen = false
							next.TerminalBoundaryLine = 0
						}
					}
					if !record.Internal && record.TurnIDExplicit && strings.TrimSpace(record.TurnID) != "" {
						parseState.turnID = strings.TrimSpace(record.TurnID)
						break
					}
				}
				result.Records = append(result.Records, records...)
				for _, record := range records {
					if record.Internal {
						continue
					}
					if record.Kind == TranscriptKindAssistant && strings.TrimSpace(record.Text) != "" {
						pending = historyTieredAssistantCandidate{Record: record}
						if signals.FinalAnswer && !suppressFinalsAfterContinuation {
							terminalBoundarySeen = true
							final := historyTieredFinalFromCandidate(pending, record.SourceLine, "final_answer", scopeBoundaryBeforeFinal)
							if (anonymousFinalNeedsQuarantine || responseItemAnonymousFinalNeedsQuarantine) && strings.TrimSpace(final.Record.TurnID) == "" {
								final.NoTurnIDNeedsQuarantine = true
							}
							if responseItemTranscriptOnlyFinalNeedsQuarantine {
								final.TranscriptOnlyQuarantine = true
							}
							if transcriptOnlyFinalNeedsQuarantine {
								final.TranscriptOnlyQuarantine = true
							}
							if previousAnonymousFinalCandidate && historyTieredFinalMatchesPreviousAnonymous(final.Record, previous) {
								// The previous poll already committed an anonymous final. A
								// same-text typed surface arriving later is still only a
								// possible mirror; do not let the cross-poll split bypass
								// the transcript-only quarantine.
								final.TranscriptOnlyQuarantine = true
							}
							if final.Key != "" && !historyTieredFinalMatchesPersistedBoundary(final.Record, finalDedupBoundary) && final.Key != next.LastFinalID {
								result.Finals = append(result.Finals, final)
								next.LastFinalID = final.Key
								next.LastFinalLine = final.Record.SourceLine
								next.LastFinalStartOffset = final.Record.SourceStartOffset
								next.LastFinalStartOffsetKnown = true
								setHistoryTieredFinalBoundaryMetadata(&next, final.Record)
							}
							next.TerminalBoundarySeen = true
							next.TerminalBoundaryLine = lineNo
							pending = historyTieredAssistantCandidate{}
						}
					}
				}
				if (signals.TurnCompleted || signals.TerminalFailed) && !suppressFinalsAfterContinuation {
					terminalBoundarySeen = true
					next.TerminalBoundarySeen = true
					next.TerminalBoundaryLine = lineNo
					if signals.TerminalFailed {
						// A failed terminal is still an ownership boundary, but it
						// must never promote a pending assistant fragment to a
						// successful final.
						pending = historyTieredAssistantCandidate{}
						continue
					}
					terminalTurnID := firstNonEmptyString(signals.TurnID, parseState.turnID)
					if pending.Record.Text != "" && historyTieredTurnMatches(pending.Record.TurnID, terminalTurnID) {
						final := historyTieredFinalFromCandidate(pending, lineNo, signals.TerminalKind, scopeBoundaryBeforeFinal)
						if final.Key != "" && !historyTieredFinalMatchesPersistedBoundary(final.Record, finalDedupBoundary) && final.Key != next.LastFinalID {
							result.Finals = append(result.Finals, final)
							next.LastFinalID = final.Key
							next.LastFinalLine = final.Record.SourceLine
							next.LastFinalStartOffset = final.Record.SourceStartOffset
							next.LastFinalStartOffsetKnown = true
							setHistoryTieredFinalBoundaryMetadata(&next, final.Record)
						}
						pending = historyTieredAssistantCandidate{}
					}
				}
				// A prompt before this terminal belongs to the just-completed
				// execution.  Do not let it prove a later root-shaped continuation;
				// only a prompt observed after the terminal may release that marker.
				if signals.FinalAnswer || signals.TurnCompleted || signals.TerminalFailed {
					externalUserPromptSeen = false
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
	if budgeted && !result.Incomplete && !result.Truncated {
		result.BudgetExhausted = true
	}

	next.Offset = offset
	next.Line = lineNo
	next.SessionID = parseState.sessionID
	next.ThreadID = parseState.threadID
	next.TurnID = parseState.turnID
	next.ExternalUserPromptSeen = externalUserPromptSeen
	next.PendingRootTaskStarted = pendingRootTaskStarted
	next.PendingRootTaskStartedLine = pendingRootTaskStartedLine
	next.PendingRootTaskStartedOffset = pendingRootTaskStartedOffset
	next.PendingRootTaskStartedEndOffset = pendingRootTaskStartedEndOffset
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
	if readProofReady && !result.Truncated && !result.TooLarge && !result.Incomplete && next.Offset == readEnd {
		// Revalidate the pathname identity after the scan. The sender and
		// checkpoint CAS perform their own final proof checks, so this is only a
		// cheap way to pass the exact bytes read to the bridge.
		if postInfo, postErr := os.Stat(path); postErr == nil && !postInfo.IsDir() && os.SameFile(info, postInfo) && postInfo.Size() >= readEnd {
			result.ReadProofFingerprint = "sha256:" + hex.EncodeToString(readProof.Sum(nil))
			result.ReadProofStartOffset = previous.Offset
			result.ReadProofEndOffset = readEnd
			result.ReadProofRangeKnown = true
		}
	}
	if budgeted && !result.Truncated && !result.Incomplete && next.Offset > previous.Offset {
		if fingerprint := transcriptSourceRangeFingerprint(path, previous.Offset, next.Offset); fingerprint != "" {
			result.ReadProofFingerprint = fingerprint
			result.ReadProofStartOffset = previous.Offset
			result.ReadProofEndOffset = next.Offset
			result.ReadProofRangeKnown = true
		}
	}
	// Anonymous finals are resolved only after the bounded scan has seen the
	// complete candidate group. A single no-ID final after a trusted terminal is
	// a real execution-ownership ambiguity; two or more candidates are a
	// transcript-only quarantine because they may be the event_msg/response_item
	// mirror emitted for one answer. Neither case may publish the candidates.
	anonymous := make([]int, 0, 2)
	for i := range result.Finals {
		if strings.TrimSpace(result.Finals[i].Record.TurnID) == "" {
			anonymous = append(anonymous, i)
		}
	}
	if len(anonymous) == 1 && result.Finals[anonymous[0]].NoTurnIDNeedsQuarantine {
		anonymousIndex := anonymous[0]
		mixedIDMirror := false
		for i := range result.Finals {
			if i == anonymousIndex || !historyTieredFinalCouldBeMixedIDMirror(result.Finals[anonymousIndex], result.Finals[i]) {
				continue
			}
			mixedIDMirror = true
			result.Finals[i].TranscriptOnlyQuarantine = true
		}
		if mixedIDMirror {
			result.Finals[anonymousIndex].TranscriptOnlyQuarantine = true
		} else if previousAnonymousFinalCandidate {
			result.QuarantinedFinals = append(result.QuarantinedFinals, result.Finals[anonymousIndex])
		} else {
			if !result.State.UnresolvedContinuation {
				result.State.UnresolvedContinuation = true
				first := result.Finals[anonymousIndex].Record
				result.State.UnresolvedContinuationLine = first.SourceLine
				result.State.UnresolvedContinuationOffset = first.SourceStartOffset
			}
		}
		if !mixedIDMirror {
			result.Finals = append(append([]historyTieredFinal(nil), result.Finals[:anonymousIndex]...), result.Finals[anonymousIndex+1:]...)
		}
	}
	if len(anonymous) >= 2 {
		for _, i := range anonymous {
			result.Finals[i].NoTurnIDNeedsQuarantine = true
		}
	}
	if len(result.Finals) > 0 {
		retained := make([]historyTieredFinal, 0, len(result.Finals))
		for _, final := range result.Finals {
			if final.TranscriptOnlyQuarantine {
				result.QuarantinedFinals = append(result.QuarantinedFinals, final)
				continue
			}
			retained = append(retained, final)
		}
		result.Finals = retained
	}
	var ambiguousMirror bool
	var compactedQuarantine []historyTieredFinal
	result.Finals, compactedQuarantine, ambiguousMirror = compactHistoryTieredFinals(result.Finals)
	if ambiguousMirror {
		result.QuarantinedFinals = append(result.QuarantinedFinals, compactedQuarantine...)
	}
	if len(result.QuarantinedFinals) > 0 {
		kind := "ambiguous_final_group"
		if len(anonymous) > 0 {
			kind = "anonymous_final_group"
		} else if ambiguousMirror {
			kind = "mixed_id_final_mirror"
		}
		result.State.TranscriptQuarantine = historyTieredTranscriptQuarantine(result.State, kind, result.QuarantinedFinals)
		if previousAnonymousFinalCandidate && result.State.TranscriptQuarantine != nil {
			quarantine := result.State.TranscriptQuarantine
			if previous.LastFinalStartOffsetKnown && previous.LastFinalStartOffset < quarantine.FrontierOffset {
				quarantine.FrontierRecordID = strings.TrimSpace(previous.LastFinalID)
				quarantine.FrontierLine = previous.LastFinalLine
				quarantine.FrontierOffset = previous.LastFinalStartOffset
			}
			previousHash := strings.TrimSpace(previous.LastFinalTextHash)
			if previousHash != "" && len(quarantine.CandidateTextHashes) < 4 {
				seen := false
				for _, hash := range quarantine.CandidateTextHashes {
					if hash == previousHash {
						seen = true
						break
					}
				}
				if !seen {
					quarantine.CandidateTextHashes = append(quarantine.CandidateTextHashes, previousHash)
				}
			}
		}
	}
	return result, nil
}

// historyTieredCompleteBudgetedLine drains the remainder of one record after a
// per-pass tail budget ended in its middle. It captures only ordinary-sized
// records; once the independent record cap is crossed it keeps draining in
// small chunks until the newline and returns an opaque, complete disposition.
// A newline-less record is reported incomplete and its consumed byte count is
// persisted as a partial read hint by the caller.
func historyTieredCompleteBudgetedLine(file *os.File, prefix []byte, prefixBytes int64, remaining int64, maxBytes int64) (historyTieredRecordRead, error) {
	if file == nil || remaining <= 0 || maxBytes <= 0 {
		return historyTieredRecordRead{Line: prefix, BytesRead: prefixBytes}, nil
	}
	readLimit := remaining
	if cap := historyTieredMaxRecordReadBytes - prefixBytes; cap < readLimit {
		readLimit = cap
	}
	if readLimit < 0 {
		readLimit = 0
	}
	reader := bufio.NewReaderSize(io.LimitReader(file, readLimit), historyTieredTailReaderSize)
	line := append([]byte(nil), prefix...)
	total := prefixBytes
	oversized := total > maxBytes
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			total += int64(len(chunk))
			if !oversized {
				if total <= maxBytes {
					line = append(line, chunk...)
				} else {
					oversized = true
					// The prefix is useful for the bounded visibility hint. Keep
					// it, but never append the opaque payload after the cap.
					if int64(len(line)) > maxBytes {
						line = line[:maxBytes]
					}
				}
			}
			if bytes.HasSuffix(chunk, []byte("\n")) {
				if oversized {
					line = line[:minInt64(int64(len(line)), maxBytes)]
				}
				return historyTieredRecordRead{Line: line, Complete: true, Oversized: oversized, BytesRead: total}, nil
			}
		}
		if err != nil {
			if err == io.EOF {
				return historyTieredRecordRead{Line: line, Oversized: oversized, BytesRead: total}, nil
			}
			if err == bufio.ErrBufferFull {
				// ReadSlice returns the available fragment and keeps the reader
				// positioned at the next byte. Continue draining this record.
				continue
			}
			return historyTieredRecordRead{Line: line, Oversized: oversized, BytesRead: total}, err
		}
	}
}

func historyTieredPartialHasNewline(path string, startOffset int64, endOffset int64) (bool, error) {
	if strings.TrimSpace(path) == "" || startOffset < 0 || endOffset <= startOffset {
		return false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return false, err
	}
	reader := bufio.NewReaderSize(io.LimitReader(f, endOffset-startOffset), historyTieredTailReaderSize)
	for {
		chunk, readErr := reader.ReadSlice('\n')
		if bytes.Contains(chunk, []byte{'\n'}) {
			return true, nil
		}
		if readErr != nil {
			if readErr == io.EOF {
				return false, nil
			}
			if readErr == bufio.ErrBufferFull {
				continue
			}
			return false, readErr
		}
	}
}

func minInt64(left, right int64) int {
	if left < right {
		return int(left)
	}
	return int(right)
}

func historyTieredSourceIdentity(path string, info os.FileInfo) string {
	if info == nil {
		return ""
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, info)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(identity)
}

func historyTieredOversizedRecord(path string, lineNo int, startOffset int64, endOffset int64, prefix []byte) TranscriptRecord {
	if lineNo <= 0 || startOffset < 0 || endOffset <= startOffset {
		return TranscriptRecord{}
	}
	sum := sha256.New()
	_, _ = sum.Write([]byte(filepath.Clean(path)))
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write([]byte(strconv.FormatInt(startOffset, 10)))
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write([]byte(strconv.FormatInt(endOffset, 10)))
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write(prefix)
	key := "oversized:" + hex.EncodeToString(sum.Sum(nil))[:24]
	kind := TranscriptKindUnknown
	text := ""
	// A giant opaque tool/image record is safely ignored. If the bounded
	// envelope proves that this is a visible final/status record, leave one
	// deterministic placeholder so the user sees a delivery rather than a
	// silent cursor jump. The payload itself is never copied into the record.
	if bytes.Contains(prefix, []byte(`"phase":"final_answer"`)) || bytes.Contains(prefix, []byte(`"phase": "final_answer"`)) {
		kind = TranscriptKindAssistant
		text = "Imported transcript record exceeded the local display limit; the original payload remains in the local Codex transcript."
	} else if bytes.Contains(prefix, []byte(`"turn_completed"`)) || bytes.Contains(prefix, []byte(`"turn.completed"`)) || bytes.Contains(prefix, []byte(`"status"`)) {
		kind = TranscriptKindStatus
		text = "Imported oversized transcript status record; the original payload remains in the local Codex transcript."
	}
	return TranscriptRecord{
		ItemID:            key,
		SourceItemID:      key,
		DedupeKey:         key,
		Kind:              kind,
		Text:              text,
		SourceLine:        lineNo,
		SourceStartOffset: startOffset,
		SourceOffset:      endOffset,
		SourceType:        "oversized_record",
	}
}

func historyTieredConsumedRecordKey(path string, lineNo int, startOffset int64, endOffset int64) string {
	if lineNo <= 0 || startOffset < 0 || endOffset <= startOffset {
		return ""
	}
	payload := filepath.Clean(path) + "\x00" + strconv.Itoa(lineNo) + "\x00" + strconv.FormatInt(startOffset, 10) + "\x00" + strconv.FormatInt(endOffset, 10)
	sum := sha256.Sum256([]byte(payload))
	return "ignored:" + hex.EncodeToString(sum[:])[:24]
}

func historyTieredLineHasExplicitTurnID(line []byte) bool {
	// Most transcript lines have no turn provenance. Keep the common path at a
	// cheap byte scan; only decode JSON when a turn-shaped field is present.
	return historyTieredLineHasExplicitTurnIDWithHint(line, historyTieredLineHints(line).MayTurnField)
}

func historyTieredLineHasExplicitTurnIDWithHint(line []byte, mayTurnField bool) bool {
	if !mayTurnField {
		return false
	}
	var value any
	if err := json.Unmarshal(line, &value); err != nil {
		return false
	}
	var visit func(any) bool
	visit = func(current any) bool {
		switch item := current.(type) {
		case map[string]any:
			for key, child := range item {
				if key == "turn_id" || key == "turnId" {
					if text, ok := child.(string); ok && strings.TrimSpace(text) != "" {
						return true
					}
				}
				if key == "turn" {
					if nested, ok := child.(map[string]any); ok {
						if text, ok := nested["id"].(string); ok && strings.TrimSpace(text) != "" {
							return true
						}
					}
				}
				if visit(child) {
					return true
				}
			}
		case []any:
			for _, child := range item {
				if visit(child) {
					return true
				}
			}
		}
		return false
	}
	return visit(value)
}

func historyTieredMayContainContinuation(line []byte) bool {
	return historyTieredLineHints(line).MayContinuation
}

// historyTieredContinuationSignal recognizes only execution-bearing child
// signals. Ordinary turn.started records are common in healthy sessions and
// must not poison final recovery; task_started, turn_context, and explicit
// goal-continuation records are the ambiguity boundary.
func historyTieredContinuationSignal(line []byte) (string, string, bool, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(line, &obj); err != nil || obj == nil {
		return "", "", false, false
	}
	read := func(value map[string]json.RawMessage) (string, string, bool) {
		if value == nil {
			return "", "", false
		}
		typeName := strings.ToLower(strings.TrimSpace(jsonStringField(value, "type", "event", "method")))
		typeName = strings.NewReplacer(".", "_", "/", "_", "-", "_").Replace(typeName)
		id := firstNonEmptyString(jsonStringField(value, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(value, "turn"), jsonStringField(value, "id"))
		if typeName == "response_item" {
			if payload, ok := jsonObjectField(value, "payload"); ok {
				id = firstNonEmptyString(jsonStringField(payload, "turn_id", "turnId", "task_id", "taskId", "id"), nestedJSONID(payload, "turn"), id)
			}
		}
		ordinaryRootStart := typeName == "task_started" && historyTieredOrdinaryRootTaskStarted(value)
		return typeName, id, ordinaryRootStart
	}
	typeName, id, ordinaryRootStart := read(obj)
	if typeName == "event_msg" {
		if payload, ok := jsonObjectField(obj, "payload"); ok {
			typeName, id, ordinaryRootStart = read(payload)
		}
	}
	switch typeName {
	case "task_started", "goal_continuation":
		return typeName, id, true, ordinaryRootStart
	case "turn_context":
		// A context record without an execution ID is also emitted by the
		// ChatGPT app for internal bookkeeping.  The transcript parser keeps
		// that line as a hidden checkpoint record, but it is not enough evidence
		// to quarantine a new execution.  An explicitly identified context
		// record remains an execution boundary for continuation safety.
		if strings.TrimSpace(id) == "" {
			return "", "", false, false
		}
		return typeName, id, true, ordinaryRootStart
	default:
		return "", "", false, false
	}
}

func historyTieredLineIsResponseItem(line []byte) bool {
	if !bytes.Contains(line, []byte(`"response_item"`)) && !bytes.Contains(line, []byte(`"response.item"`)) {
		return false
	}
	var value any
	if err := json.Unmarshal(line, &value); err != nil {
		return false
	}
	var visit func(any) bool
	visit = func(current any) bool {
		switch item := current.(type) {
		case map[string]any:
			typeName := strings.ToLower(strings.TrimSpace(stringValue(item["type"])))
			typeName = strings.NewReplacer(".", "_", "/", "_", "-", "_").Replace(typeName)
			if typeName == "response_item" {
				return true
			}
			for _, child := range item {
				if visit(child) {
					return true
				}
			}
		case []any:
			for _, child := range item {
				if visit(child) {
					return true
				}
			}
		}
		return false
	}
	return visit(value)
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func historyTieredVisibleFinalNeedsQuarantine(records []TranscriptRecord, priorTurnID string, terminalBoundarySeen bool, knownFinalBoundary bool, legacyAnonymousBoundary bool, explicitTerminalBoundary bool) (bool, string, bool) {
	if !terminalBoundarySeen && !knownFinalBoundary && !legacyAnonymousBoundary {
		return false, "", false
	}
	priorTurnID = strings.TrimSpace(priorTurnID)
	for _, record := range records {
		if record.Internal || record.Kind != TranscriptKindAssistant || strings.TrimSpace(record.Text) == "" || !strings.EqualFold(strings.TrimSpace(record.Phase), "final_answer") {
			continue
		}
		turnID := strings.TrimSpace(record.TurnID)
		if !record.TurnIDExplicit || turnID == "" {
			// Once any terminal or unresolved boundary is known, an assistant
			// final without explicit execution provenance cannot be attributed to
			// the outer execution. This includes event_msg/agent_message records
			// with a source item ID: that ID identifies a transcript record, not
			// the owning Codex execution.
			return true, "", true
		}
		if priorTurnID == "" && !legacyAnonymousBoundary && !explicitTerminalBoundary {
			// A linked checkpoint may carry only a generic record key. Until an
			// explicit terminal marker is observed in the scanned suffix, a
			// record with its own turn ID is allowed to establish the next
			// execution scope; this is the normal post-restart/live-turn path.
			continue
		}
		if priorTurnID == "" && !legacyAnonymousBoundary && strings.EqualFold(strings.TrimSpace(record.SourceType), "message") {
			// A legacy byte-only cursor can legitimately resume at the first
			// canonical response_item for a new root turn. The response_item-specific
			// guard below still rejects anonymous or mismatched child records once a
			// prior execution scope is known.
			continue
		}
		if priorTurnID != "" && turnID == priorTurnID {
			continue
		}
		// Once a terminal boundary is known, a visible final is safe only when
		// its own explicit execution ID proves it belongs to that same turn.  A
		// different child ID (or an explicit ID with no prior scope) is
		// quarantined.
		return true, turnID, false
	}
	return false, "", false
}

func historyTieredOrdinaryRootTaskStarted(value map[string]json.RawMessage) bool {
	if value == nil {
		return false
	}
	_, hasWindow := value["model_context_window"]
	_, hasCollaboration := value["collaboration_mode_kind"]
	return hasWindow && hasCollaboration
}

type historyTieredLineSignal struct {
	FinalAnswer    bool
	TurnCompleted  bool
	TerminalFailed bool
	TerminalKind   string
	ThreadID       string
	TurnID         string
}

func historyTieredLineSignals(line []byte) historyTieredLineSignal {
	return historyTieredLineSignalsWithHints(line, historyTieredLineHints(line))
}

func historyTieredLineSignalsWithHints(line []byte, hints historyTieredLineHint) historyTieredLineSignal {
	var signal historyTieredLineSignal
	if !hints.MayTerminal {
		return signal
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(line, &obj); err != nil {
		return signal
	}
	if obj == nil {
		return signal
	}
	if hints.MayFinal {
		signal.FinalAnswer = historyTieredHasFinalAnswerPhase(obj)
	}
	// A final_answer phase is not itself a turn/task terminal event.  Avoid the
	// recursive terminal walk for the common event_msg final shape; the caller
	// still runs all record provenance, quarantine, and finalization checks.
	if signal.FinalAnswer && !hints.MayTerminalEvent {
		return signal
	}
	var readTerminal func(map[string]json.RawMessage) (string, string, bool, bool)
	readTerminal = func(value map[string]json.RawMessage) (string, string, bool, bool) {
		if value == nil {
			return "", "", false, false
		}
		// JSON-RPC turn/completed records carry their identity in params; inspect
		// nested protocol fields before interpreting the wrapper itself.
		for _, field := range []string{"payload", "params"} {
			if child, ok := jsonObjectField(value, field); ok {
				if kind, id, terminal, failed := readTerminal(child); terminal {
					return kind, firstNonEmptyString(id, jsonStringField(value, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(value, "turn")), true, failed
				}
			}
		}
		typeName := strings.ToLower(strings.TrimSpace(jsonStringField(value, "type", "method", "event")))
		normalized := strings.NewReplacer(".", "_", "/", "_", "-", "_").Replace(typeName)
		nestedID := ""
		for _, field := range []string{"payload", "params"} {
			if child, ok := jsonObjectField(value, field); ok {
				nestedID = firstNonEmptyString(nestedID, jsonStringField(child, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(child, "turn"))
			}
		}
		switch normalized {
		case "turn_completed", "task_complete", "task_completed":
			return typeName, firstNonEmptyString(jsonStringField(value, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(value, "turn"), nestedID), true, false
		case "turn_failed", "task_failed":
			return typeName, firstNonEmptyString(jsonStringField(value, "turn_id", "turnId", "task_id", "taskId"), nestedJSONID(value, "turn"), nestedID), true, true
		default:
			return "", "", false, false
		}
	}
	if terminalKind, turnID, ok, failed := readTerminal(obj); ok {
		signal.TurnCompleted = !failed
		signal.TerminalFailed = failed
		signal.TerminalKind = terminalKind
		signal.ThreadID = historyTieredTerminalThreadID(obj)
		signal.TurnID = turnID
	}
	return signal
}

func historyTieredTerminalThreadID(value map[string]json.RawMessage) string {
	if value == nil {
		return ""
	}
	if threadID := jsonStringField(value, "thread_id", "threadId"); threadID != "" {
		return threadID
	}
	for _, field := range []string{"payload", "params", "item"} {
		child, ok := jsonObjectField(value, field)
		if !ok {
			continue
		}
		if threadID := historyTieredTerminalThreadID(child); threadID != "" {
			return threadID
		}
	}
	return ""
}

func historyTieredMayContainTerminalSignal(line []byte) bool {
	return historyTieredLineHints(line).MayTerminal
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

func historyTieredFinalFromCandidate(candidate historyTieredAssistantCandidate, terminalLine int, terminalKind string, quarantine ...bool) historyTieredFinal {
	record := candidate.Record
	key := historyTieredCompletionKey(record, terminalLine, terminalKind)
	if key == "" {
		return historyTieredFinal{}
	}
	needsQuarantine := len(quarantine) > 0 && quarantine[0]
	return historyTieredFinal{
		Key:                     key,
		Record:                  record,
		TerminalLine:            terminalLine,
		TerminalKind:            terminalKind,
		NoTurnIDNeedsQuarantine: needsQuarantine && strings.TrimSpace(record.TurnID) == "",
	}
}

func compactHistoryTieredFinals(finals []historyTieredFinal) ([]historyTieredFinal, []historyTieredFinal, bool) {
	if len(finals) < 2 {
		return finals, nil, false
	}
	// app-server can expose one assistant final both as an event_msg and as a
	// response_item. They have different source IDs, so ID-only dedupe would
	// publish two Teams answers. Collapse exact same-turn/text mirrors first,
	// preferring the canonical response_item/message representation. Use a map
	// for the common exact-mirror lookup; the ordered slice preserves output
	// order and the prefix-shadow pass remains linear.
	deduped := make([]historyTieredFinal, 0, len(finals))
	mirrorIndex := make(map[string][]int, len(finals))
	ambiguousIndexes := make(map[int]bool)
	anonymousIndexes := make([]int, 0, len(finals))
	for _, current := range finals {
		anonymousCurrent := false
		if current.NoTurnIDNeedsQuarantine && strings.TrimSpace(current.Record.TurnID) == "" {
			for _, candidateIndex := range anonymousIndexes {
				leftThread := strings.TrimSpace(deduped[candidateIndex].Record.ThreadID)
				rightThread := strings.TrimSpace(current.Record.ThreadID)
				if leftThread != "" && rightThread != "" && !strings.EqualFold(leftThread, rightThread) {
					continue
				}
				ambiguousIndexes[candidateIndex] = true
				anonymousCurrent = true
			}
		}
		key := historyTieredFinalMirrorKey(current)
		if key == "" {
			if current.NoTurnIDNeedsQuarantine && strings.TrimSpace(current.Record.TurnID) == "" {
				if anonymousCurrent {
					ambiguousIndexes[len(deduped)] = true
				}
				anonymousIndexes = append(anonymousIndexes, len(deduped))
			}
			deduped = append(deduped, current)
			continue
		}
		mirror := -1
		ambiguousCurrent := false
		for _, candidate := range mirrorIndex[key] {
			if historyTieredFinalIsExactMirror(deduped[candidate], current) {
				mirror = candidate
				break
			}
			if historyTieredFinalCouldBeMixedIDMirror(deduped[candidate], current) {
				ambiguousIndexes[candidate] = true
				ambiguousCurrent = true
			}
		}
		if mirror >= 0 {
			if historyTieredFinalPreferred(current, deduped[mirror]) {
				deduped[mirror] = current
			}
			continue
		}
		mirrorIndex[key] = append(mirrorIndex[key], len(deduped))
		if ambiguousCurrent {
			ambiguousIndexes[len(deduped)] = true
		}
		if current.NoTurnIDNeedsQuarantine && strings.TrimSpace(current.Record.TurnID) == "" {
			if anonymousCurrent {
				ambiguousIndexes[len(deduped)] = true
			}
			anonymousIndexes = append(anonymousIndexes, len(deduped))
		}
		deduped = append(deduped, current)
	}
	out := make([]historyTieredFinal, 0, len(deduped))
	quarantined := make([]historyTieredFinal, 0, len(ambiguousIndexes))
	for i := range deduped {
		if ambiguousIndexes[i] {
			quarantined = append(quarantined, deduped[i])
			continue
		}
		if historyTieredFinalIsPrefixShadowed(deduped, i) {
			continue
		}
		out = append(out, deduped[i])
	}
	return out, quarantined, len(ambiguousIndexes) > 0
}

func historyTieredFinalMirrorKey(final historyTieredFinal) string {
	record := final.Record
	textHash := normalizedTextHash(strings.TrimSpace(record.Text))
	if textHash == "" {
		return ""
	}
	// Keep explicit and missing turn/thread IDs in one text bucket. Exact mirror
	// matching below still requires compatible thread/turn provenance; the
	// shared bucket lets us detect a missing-thread or mixed-ID pair and
	// quarantine it instead of publishing both copies. Different explicit
	// threads remain separate because the exact/provenance checks reject them.
	return "text|" + textHash
}

func historyTieredFinalIsExactMirror(left historyTieredFinal, right historyTieredFinal) bool {
	leftThread, rightThread := strings.TrimSpace(left.Record.ThreadID), strings.TrimSpace(right.Record.ThreadID)
	leftTurn, rightTurn := strings.TrimSpace(left.Record.TurnID), strings.TrimSpace(right.Record.TurnID)
	textMirror := normalizedTextHash(strings.TrimSpace(left.Record.Text)) == normalizedTextHash(strings.TrimSpace(right.Record.Text))
	if leftTurn == "" || rightTurn == "" {
		// A missing turn ID is not provenance. Even an adjacent pair must stay
		// ambiguous unless a typed completion group proves that the records are
		// mirrors; compacting by text here can publish a child final twice.
		return false
	}
	if leftTurn != rightTurn {
		return false
	}
	if leftThread != "" && rightThread != "" && leftThread != rightThread {
		return false
	}
	return textMirror
}

// historyTieredFinalCouldBeMixedIDMirror is deliberately conservative. A
// missing turn ID is not execution provenance. Any same-thread/text pair with
// exactly one missing turn ID is unsafe to publish as two answers. We do not
// infer a completion group from source type or adjacency; callers quarantine
// the pair unless a future typed protocol proof is available.
func historyTieredFinalCouldBeMixedIDMirror(left historyTieredFinal, right historyTieredFinal) bool {
	leftTurn := strings.TrimSpace(left.Record.TurnID)
	rightTurn := strings.TrimSpace(right.Record.TurnID)
	if (leftTurn == "") == (rightTurn == "") {
		return false
	}
	leftThread := strings.TrimSpace(left.Record.ThreadID)
	rightThread := strings.TrimSpace(right.Record.ThreadID)
	if leftThread != "" && rightThread != "" && !strings.EqualFold(leftThread, rightThread) {
		return false
	}
	if normalizedTextHash(strings.TrimSpace(left.Record.Text)) != normalizedTextHash(strings.TrimSpace(right.Record.Text)) {
		return false
	}
	return true
}

func historyTieredFinalsCouldBeMixedIDMirror(existing []historyTieredFinal, records []TranscriptRecord) bool {
	for _, current := range records {
		if current.Internal || current.Kind != TranscriptKindAssistant ||
			!strings.EqualFold(strings.TrimSpace(current.Phase), "final_answer") ||
			strings.TrimSpace(current.TurnID) == "" {
			continue
		}
		candidate := historyTieredFinal{Record: current}
		for _, prior := range existing {
			if strings.TrimSpace(prior.Record.TurnID) == "" && prior.NoTurnIDNeedsQuarantine &&
				historyTieredFinalCouldBeMixedIDMirror(prior, candidate) {
				return true
			}
		}
	}
	return false
}

func historyTieredFinalMatchesPreviousAnonymous(record TranscriptRecord, previous historyTieredFileState) bool {
	if strings.TrimSpace(record.TurnID) == "" || strings.TrimSpace(previous.LastFinalTextHash) == "" {
		return false
	}
	if normalizedTextHash(strings.TrimSpace(record.Text)) != strings.TrimSpace(previous.LastFinalTextHash) {
		return false
	}
	previousThread := strings.TrimSpace(previous.LastFinalThreadID)
	recordThread := strings.TrimSpace(record.ThreadID)
	return previousThread == "" || recordThread == "" || strings.EqualFold(previousThread, recordThread)
}

func historyTieredTranscriptQuarantine(state historyTieredFileState, kind string, finals []historyTieredFinal) *teamstore.TranscriptQuarantine {
	if len(finals) == 0 {
		return nil
	}
	quarantine := &teamstore.TranscriptQuarantine{
		Kind:              strings.TrimSpace(kind),
		SourcePath:        strings.TrimSpace(state.Path),
		SourceFingerprint: strings.TrimSpace(state.SourceFingerprint),
	}
	first := finals[0]
	quarantine.FrontierRecordID = strings.TrimSpace(first.Key)
	quarantine.FrontierLine = first.Record.SourceLine
	quarantine.FrontierOffset = first.Record.SourceStartOffset
	frontierSet := first.Record.SourceStartOffset >= 0
	seenHashes := make(map[string]bool, 4)
	for _, final := range finals {
		if len(quarantine.CandidateTextHashes) >= 4 {
			break
		}
		hash := normalizedTextHash(strings.TrimSpace(final.Record.Text))
		if hash == "" || seenHashes[hash] {
			continue
		}
		seenHashes[hash] = true
		quarantine.CandidateTextHashes = append(quarantine.CandidateTextHashes, hash)
		if final.Record.SourceStartOffset >= 0 && (!frontierSet || final.Record.SourceStartOffset < quarantine.FrontierOffset) {
			quarantine.FrontierRecordID = strings.TrimSpace(final.Key)
			quarantine.FrontierLine = final.Record.SourceLine
			quarantine.FrontierOffset = final.Record.SourceStartOffset
			frontierSet = true
		}
	}
	return quarantine
}

func historyTieredFinalPreferred(candidate historyTieredFinal, current historyTieredFinal) bool {
	canonical := func(record TranscriptRecord) bool {
		return strings.EqualFold(strings.TrimSpace(record.SourceType), "message")
	}
	return canonical(candidate.Record) && !canonical(current.Record)
}

func historyTieredFinalMatchesPersistedBoundary(record TranscriptRecord, state historyTieredFileState) bool {
	if strings.TrimSpace(state.LastFinalTextHash) == "" {
		return false
	}
	threadID, turnID := strings.TrimSpace(record.ThreadID), strings.TrimSpace(record.TurnID)
	if turnID == "" {
		return strings.TrimSpace(state.LastFinalTurnID) == "" && normalizedTextHash(strings.TrimSpace(record.Text)) == strings.TrimSpace(state.LastFinalTextHash)
	}
	if turnID != strings.TrimSpace(state.LastFinalTurnID) {
		return false
	}
	if strings.TrimSpace(state.LastFinalThreadID) != "" && threadID != "" && threadID != strings.TrimSpace(state.LastFinalThreadID) {
		return false
	}
	return normalizedTextHash(strings.TrimSpace(record.Text)) == strings.TrimSpace(state.LastFinalTextHash)
}

func setHistoryTieredFinalBoundaryMetadata(state *historyTieredFileState, record TranscriptRecord) {
	if state == nil {
		return
	}
	state.LastFinalThreadID = strings.TrimSpace(record.ThreadID)
	state.LastFinalTurnID = strings.TrimSpace(record.TurnID)
	state.LastFinalTextHash = normalizedTextHash(strings.TrimSpace(record.Text))
}

func historyTieredFinalIsPrefixShadowed(finals []historyTieredFinal, index int) bool {
	if index < 0 || index+1 >= len(finals) {
		return false
	}
	current := finals[index]
	currentText := strings.TrimSpace(current.Record.Text)
	if current.Record.Kind != TranscriptKindAssistant || !transcriptRecordCanBeStreamingAssistantPrefix(current.Record) || len([]rune(currentText)) < 40 {
		return false
	}
	for i := index + 1; i < len(finals); i++ {
		next := finals[i]
		if next.Record.Kind != TranscriptKindAssistant {
			continue
		}
		if !transcriptRecordCanShadowStreamingAssistantPrefix(next.Record) {
			continue
		}
		if !transcriptRecordsCanShadowSameAssistant(current.Record, next.Record) {
			continue
		}
		nextText := strings.TrimSpace(next.Record.Text)
		if len(nextText) > len(currentText) && strings.HasPrefix(nextText, currentText) {
			return true
		}
	}
	return false
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
