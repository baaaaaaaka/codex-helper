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
	Path                 string
	Size                 int64
	ModTime              time.Time
	SourceFingerprint    string
	SourceRewriteBlocked bool
	Offset               int64
	Line                 int
	SessionID            string
	ThreadID             string
	TeamsOriginThreadID  string
	TurnID               string
	TeamsOriginTurnID    string
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
	// AnonymousFinals is retained separately when a pair is quarantined. The
	// ordinary Records/Finals views intentionally omit that pair so callers
	// cannot publish it without an explicit compatibility proof.
	AnonymousFinals []historyTieredFinal
	Truncated       bool
	TooLarge        bool
	Incomplete      bool
	BytesRead       int64
	LinesRead       int
	MaxTailBytes    int64
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
	Key                     string
	Record                  TranscriptRecord
	TerminalLine            int
	TerminalKind            string
	NoTurnIDNeedsQuarantine bool
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
	next.TerminalBoundarySeen = previous.TerminalBoundarySeen || previous.UnresolvedContinuation
	if next.TerminalBoundarySeen && next.TerminalBoundaryLine == 0 {
		next.TerminalBoundaryLine = previous.LastFinalLine
	}
	suppressFinalsAfterContinuation := previous.UnresolvedContinuation || previous.PendingRootTaskStarted
	externalUserPromptSeen := previous.ExternalUserPromptSeen
	pendingRootTaskStarted := previous.PendingRootTaskStarted && !previous.UnresolvedContinuation
	pendingRootTaskStartedLine := previous.PendingRootTaskStartedLine
	pendingRootTaskStartedOffset := previous.PendingRootTaskStartedOffset
	result := historyTieredTailResult{
		State:        next,
		MaxTailBytes: maxTailBytes,
	}
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineStartOffset := offset
			if err == io.EOF && !bytes.HasSuffix(line, []byte("\n")) {
				result.Incomplete = true
				// Keep the source-size cursor at the last complete byte.  The
				// file may grow by completing this same line; recording the full
				// current size would make the next pass incorrectly take the
				// unchanged fast path and lose the tail.
				next.Size = lineStartOffset
				break
			}
			lineNo++
			offset += int64(len(line))
			result.BytesRead += int64(len(line))
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
							suppressFinalsAfterContinuation = true
						}
						externalUserPromptSeen = false
						if rootLikeStart && !pendingRootStart {
							pendingRootTaskStarted = false
							pendingRootTaskStartedLine = 0
							pendingRootTaskStartedOffset = 0
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
				if ambiguous, childTurnID := historyTieredVisibleFinalNeedsQuarantine(records, priorTurnID, scopeBoundaryBeforeFinal, knownFinalBoundary || terminalEventBoundarySeen, legacyNoOffsetCheckpointBoundary, terminalEventBoundarySeen); ambiguous {
					if !next.UnresolvedContinuation {
						next.UnresolvedContinuation = true
						next.UnresolvedContinuationLine = lineNo - 1
						next.UnresolvedContinuationOffset = lineStartOffset
					}
					suppressFinalsAfterContinuation = true
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
				responseItemBoundarySeen := terminalBoundarySeen || legacyNoOffsetCheckpointBoundary || legacyResponseItemBoundary || legacyCursorBoundary
				if responseItemBoundarySeen && (knownFinalBoundary || priorTurnID != "") && !suppressFinalsAfterContinuation && historyTieredLineIsResponseItem(trimmed) {
					for _, record := range records {
						if record.Internal || record.Kind != TranscriptKindAssistant || !strings.EqualFold(strings.TrimSpace(record.Phase), "final_answer") {
							continue
						}
						if !record.TurnIDExplicit || strings.TrimSpace(record.TurnID) == "" {
							// A visible final response_item after a known terminal scope
							// must carry its own execution identity.  Inheriting the
							// previous outer turn would publish an orphan answer under
							// the wrong Teams request.  A legacy LastFinalID-only
							// checkpoint is already a known terminal scope even though
							// it lacks a prior turn ID; quarantine that combination too.
							if priorTurnID == "" && !legacyNoOffsetCheckpointBoundary && !legacyResponseItemBoundary {
								continue
							}
							next.UnresolvedContinuation = true
							next.UnresolvedContinuationLine = lineNo - 1
							next.UnresolvedContinuationOffset = lineStartOffset
							suppressFinalsAfterContinuation = true
							parseState.turnID = ""
							break
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
						if pendingRootTaskStarted && !next.UnresolvedContinuation {
							// A visible user prompt following a root-shaped task_started
							// proves that it was a normal new outer request rather than
							// an internal goal continuation. Release only this pending
							// candidate; an already durable unresolved anchor remains
							// fail-closed.
							pendingRootTaskStarted = false
							pendingRootTaskStartedLine = 0
							pendingRootTaskStartedOffset = 0
							terminalBoundarySeen = false
							suppressFinalsAfterContinuation = false
							next.LastFinalID = ""
							next.LastFinalLine = 0
							next.LastFinalStartOffset = 0
							next.LastFinalStartOffsetKnown = false
							next.LastFinalThreadID = ""
							next.LastFinalTurnID = ""
							next.LastFinalTextHash = ""
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
							if final.Key != "" && !historyTieredFinalMatchesPersistedBoundary(final.Record, next) && final.Key != next.LastFinalID {
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
						if final.Key != "" && !historyTieredFinalMatchesPersistedBoundary(final.Record, next) && final.Key != next.LastFinalID {
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

	next.Offset = offset
	next.Line = lineNo
	next.SessionID = parseState.sessionID
	next.ThreadID = parseState.threadID
	next.TurnID = parseState.turnID
	next.ExternalUserPromptSeen = externalUserPromptSeen
	next.PendingRootTaskStarted = pendingRootTaskStarted
	next.PendingRootTaskStartedLine = pendingRootTaskStartedLine
	next.PendingRootTaskStartedOffset = pendingRootTaskStartedOffset
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
	// Two anonymous finals are ambiguous even when the explicit terminal event
	// has not arrived yet (for example one event_msg and one response_item). A
	// single anonymous fragment remains compatible with the existing
	// wait-for-terminal behavior, but a pair must never be published as two
	// answers or attributed to one outer owner. Persist the unresolved boundary
	// so a linked-transcript caller also fails closed; HistoryWatch checks the
	// per-final marker before publishing.
	anonymous := make([]int, 0, 2)
	for i := range result.Finals {
		if strings.TrimSpace(result.Finals[i].Record.TurnID) == "" {
			anonymous = append(anonymous, i)
		}
	}
	if len(anonymous) >= 2 {
		result.AnonymousFinals = make([]historyTieredFinal, 0, len(anonymous))
		for _, i := range anonymous {
			result.Finals[i].NoTurnIDNeedsQuarantine = true
			result.AnonymousFinals = append(result.AnonymousFinals, result.Finals[i])
		}
		if !result.State.UnresolvedContinuation {
			result.State.UnresolvedContinuation = true
			first := result.Finals[anonymous[0]].Record
			result.State.UnresolvedContinuationLine = first.SourceLine
			result.State.UnresolvedContinuationOffset = first.SourceStartOffset
		}
	}
	var ambiguousMirror bool
	result.Finals, ambiguousMirror = compactHistoryTieredFinals(result.Finals)
	if ambiguousMirror && !result.State.UnresolvedContinuation {
		// A mixed-ID pair with identical text but no trustworthy source
		// relationship cannot be safely classified as either a mirror or two
		// distinct turns. Do not publish either candidate; the bridge will
		// persist the blocked boundary and emit one idempotent notice.
		result.State.UnresolvedContinuation = true
		result.State.UnresolvedContinuationLine = result.State.Line
		result.State.UnresolvedContinuationOffset = result.State.Offset
	}
	return result, nil
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
	case "task_started", "turn_context", "goal_continuation":
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

func historyTieredVisibleFinalNeedsQuarantine(records []TranscriptRecord, priorTurnID string, terminalBoundarySeen bool, knownFinalBoundary bool, legacyAnonymousBoundary bool, explicitTerminalBoundary bool) (bool, string) {
	if !terminalBoundarySeen && !knownFinalBoundary && !legacyAnonymousBoundary {
		return false, ""
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
			return true, ""
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
		return true, turnID
	}
	return false, ""
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

func compactHistoryTieredFinals(finals []historyTieredFinal) ([]historyTieredFinal, bool) {
	if len(finals) < 2 {
		return finals, false
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
	for i := range deduped {
		if ambiguousIndexes[i] {
			continue
		}
		if historyTieredFinalIsPrefixShadowed(deduped, i) {
			continue
		}
		out = append(out, deduped[i])
	}
	return out, len(ambiguousIndexes) > 0
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
