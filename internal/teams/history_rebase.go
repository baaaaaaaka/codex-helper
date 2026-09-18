package teams

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// Codex's canonical rollout writer bounds individual JSONL records, but a
// history can be arbitrarily large. Rebase therefore reads at most one line at
// a time and never materializes the complete rollout. Keep this guard above the
// canonical per-line limit so a malformed/future source cannot make recovery
// allocate an unbounded buffer.
const historyRebaseMaxLineBytes = 64 * 1024 * 1024

// A source-rewrite recovery scan is cold work. Keep each listener pass
// resumable so a large, otherwise valid rollout cannot monopolize the entire
// history-watch phase. The durable checkpoint may retain the bounded scan
// cursor, but it remains blocked until a complete, source-bound anchor is
// found.
const historyRebaseMaxScanBytesPerPass int64 = 4 * 1024 * 1024

type codexHistoryHeader struct {
	ThreadID    string
	HistoryMode string
}

type codexHistoryFile struct {
	Header   codexHistoryHeader
	Info     os.FileInfo
	Identity string
}

type historyWatchRebaseAnchor struct {
	Record       TranscriptRecord
	TextHash     string
	CursorLine   int
	CursorOffset int64
	Info         os.FileInfo
	Identity     string
}

type historyWatchRebaseScanProgress struct {
	SourceIdentity string
	Offset         int64
	Line           int
	State          transcriptParseState
	MatchFound     bool
	MatchLine      int
	MatchOffset    int64
	MatchRecord    TranscriptRecord
	MatchTextHash  string
}

type historyWatchRebaseAnchorScanResult struct {
	Anchor    historyWatchRebaseAnchor
	Found     bool
	Complete  bool
	Ambiguous bool
	Progress  historyWatchRebaseScanProgress
}

// historyRewriteRecoverySnapshotMatches is the second half of the automatic
// rebase retry key. SourceFileIdentity deliberately stays stable across
// append-only writes (and can stay stable after an in-place repair), so a
// recovery marker containing only the inode would permanently hide a newly
// repairable source. Conversely, an unchanged marker must keep blocked legacy
// rows out of the cold rebase scanner on every poll.
func historyRewriteRecoverySnapshotMatches(state historyTieredFileState, path string, info os.FileInfo) bool {
	if info == nil || info.IsDir() || strings.TrimSpace(state.SourceRewriteRecoveryIdentity) == "" {
		return false
	}
	if state.SourceRewriteRecoverySize <= 0 || state.SourceRewriteRecoverySize != info.Size() ||
		state.SourceRewriteRecoveryModTime.IsZero() || !state.SourceRewriteRecoveryModTime.Equal(info.ModTime()) {
		return false
	}
	if state.SourceRewriteRecoveryChangeTime != 0 &&
		teamstore.SourceFileChangeTime(path, info) != state.SourceRewriteRecoveryChangeTime {
		return false
	}
	return true
}

// historyRebaseChangeTimeAvailable is a fail-closed capability check.  A
// bounded anchor scan proves the bytes it saw, but without a native change-time
// revision a same-inode, same-size, same-mtime rewrite can race the final proof
// check and make an old cursor look portable.  Such platforms remain usable for
// normal incremental reads; only automatic source-rewrite rebase is held for
// explicit recovery.
func historyRebaseChangeTimeAvailable(path string, info os.FileInfo) bool {
	return info != nil && teamstore.SourceFileChangeTime(path, info) != 0
}

// readCodexHistoryHeader only reads the first JSONL record. Invalid or
// unrecognized headers are treated as ineligible for automatic migration; the
// existing explicit recovery path remains available for those files.
func readCodexHistoryHeader(path string) (codexHistoryHeader, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return codexHistoryHeader{}, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return codexHistoryHeader{}, false, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 64*1024)
	read, err := historyTieredReadJSONLRecord(reader, historyRebaseMaxLineBytes, historyRebaseMaxLineBytes)
	if read.BytesRead == 0 {
		if err == io.EOF {
			return codexHistoryHeader{}, false, nil
		}
		return codexHistoryHeader{}, false, err
	}
	if !read.Complete || read.Oversized {
		return codexHistoryHeader{}, false, nil
	}
	line := read.Line
	var envelope struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(bytes.TrimSpace(line), &envelope) != nil || strings.TrimSpace(envelope.Type) != "session_meta" {
		return codexHistoryHeader{}, false, nil
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(envelope.Payload, &payload) != nil {
		return codexHistoryHeader{}, false, nil
	}
	header := codexHistoryHeader{
		ThreadID:    strings.TrimSpace(jsonStringField(payload, "id", "thread_id", "session_id", "sessionId")),
		HistoryMode: strings.ToLower(strings.TrimSpace(jsonStringField(payload, "history_mode"))),
	}
	return header, true, nil
}

func codexPaginatedHistoryFile(path string, expectedThreadID string) (codexHistoryFile, bool, error) {
	header, ok, err := readCodexHistoryHeader(path)
	if err != nil || !ok || header.HistoryMode != "paginated" {
		return codexHistoryFile{}, false, err
	}
	expectedThreadID = strings.TrimSpace(expectedThreadID)
	if expectedThreadID == "" || header.ThreadID == "" || header.ThreadID != expectedThreadID {
		return codexHistoryFile{}, false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return codexHistoryFile{}, false, err
	}
	if info.IsDir() {
		return codexHistoryFile{}, false, nil
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		return codexHistoryFile{}, false, err
	}
	return codexHistoryFile{Header: header, Info: info, Identity: strings.TrimSpace(identity)}, true, nil
}

func codexPaginatedHistoryIdentity(path string, expectedThreadID string) (string, bool) {
	file, ok, err := codexPaginatedHistoryFile(path, expectedThreadID)
	if err != nil || !ok {
		return "", false
	}
	return file.Identity, true
}

// historyWatchRebaseAnchorScan locates the old HistoryWatch final without
// parsing the complete file into memory. A stable file identity is required on
// both sides of the scan so an atomic replacement racing this lookup fails
// closed instead of producing a cursor for a different source.  The scan is
// resumable across listener cycles, and checks its context between complete
// JSONL records; a slow source therefore yields the phase to other paths.
func historyWatchRebaseAnchorScan(ctx context.Context, path string, previous historyTieredFileState, source codexHistoryFile, progress historyWatchRebaseScanProgress) (historyWatchRebaseAnchorScanResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(progress.SourceIdentity) != strings.TrimSpace(source.Identity) || progress.Offset < 0 {
		progress = historyWatchRebaseScanProgress{
			SourceIdentity: strings.TrimSpace(source.Identity),
			State: transcriptParseState{
				sessionID: strings.TrimSpace(previous.SessionID),
				threadID:  strings.TrimSpace(previous.ThreadID),
				turnID:    strings.TrimSpace(previous.TurnID),
			},
		}
	}
	result := historyWatchRebaseAnchorScanResult{Progress: progress}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	f, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return result, err
	}
	if info.IsDir() || !os.SameFile(source.Info, info) {
		return result, fmt.Errorf("history source %q changed before rebase scan", path)
	}
	if progress.Offset > info.Size() {
		return result, fmt.Errorf("history source %q is shorter than its rebase scan cursor", path)
	}
	if progress.Offset > 0 {
		if _, err := f.Seek(progress.Offset, io.SeekStart); err != nil {
			return result, err
		}
	}
	reader := bufio.NewReaderSize(f, 64*1024)
	state := progress.State
	offset := progress.Offset
	lineNo := progress.Line
	scanStart := offset
	matchFound := progress.MatchFound
	matchLine := progress.MatchLine
	matchOffset := progress.MatchOffset
	matchRecord := progress.MatchRecord
	matchTextHash := strings.TrimSpace(progress.MatchTextHash)
	if matchTextHash == "" && matchFound {
		matchTextHash = normalizedTextHash(strings.TrimSpace(matchRecord.Text))
	}
	setProgress := func() historyWatchRebaseScanProgress {
		return historyWatchRebaseScanProgress{
			SourceIdentity: strings.TrimSpace(source.Identity),
			Offset:         offset,
			Line:           lineNo,
			State:          state,
			MatchFound:     matchFound,
			MatchLine:      matchLine,
			MatchOffset:    matchOffset,
			MatchRecord:    matchRecord,
			MatchTextHash:  matchTextHash,
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			result.Progress = setProgress()
			return result, err
		}
		if offset > scanStart && offset-scanStart >= historyRebaseMaxScanBytesPerPass {
			if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
				return result, stableErr
			}
			result.Progress = setProgress()
			return result, nil
		}
		read, readErr := historyTieredReadJSONLRecord(reader, historyRebaseMaxLineBytes, historyRebaseMaxLineBytes)
		if err := ctx.Err(); err != nil {
			result.Progress = setProgress()
			return result, err
		}
		complete := read.Complete || (readErr == io.EOF && read.BytesRead > 0)
		if read.BytesRead > 0 {
			line := read.Line
			if read.Oversized || !complete {
				if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
					return result, stableErr
				}
				result.Complete = true
				result.Progress = setProgress()
				return result, nil
			}
			lineStart := offset
			nextOffset := offset + read.BytesRead
			lineNo++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				records, diagnostics := parseTranscriptLine(trimmed, lineNo, &state)
				if len(diagnostics) > 0 {
					if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
						return result, stableErr
					}
					result.Complete = true
					result.Progress = setProgress()
					return result, nil
				}
				for i := range records {
					records[i].SourceStartOffset = lineStart
					records[i].SourceOffset = nextOffset
					if !historyWatchRebaseAnchorMatches(records[i], previous) {
						continue
					}
					// A future parser may emit more than one logical record from a
					// physical JSONL line. Re-read the whole line rather than skip
					// any record after the anchor.
					cursorLine, cursorOffset := transcriptCheckpointPositionAfterMatch(records, i, lineNo, lineStart, nextOffset)
					if err := historyRebaseStableSource(path, info, source.Identity); err != nil {
						return result, err
					}
					if matchFound {
						result.Progress = setProgress()
						result.Ambiguous = true
						return result, &TranscriptCheckpointAmbiguousError{
							AfterKey:   strings.TrimSpace(previous.LastFinalID),
							MatchCount: 2,
						}
					}
					matchFound = true
					matchLine = cursorLine
					matchOffset = cursorOffset
					matchRecord = records[i]
					matchTextHash = normalizedTextHash(strings.TrimSpace(records[i].Text))
				}
			}
			offset = nextOffset
			result.Progress = setProgress()
		}
		if readErr != nil {
			if readErr != io.EOF {
				return result, readErr
			}
			if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
				return result, stableErr
			}
			result.Complete = true
			result.Found = matchFound && historyWatchRebaseMatchProofValid(matchRecord, matchTextHash, previous)
			result.Progress = setProgress()
			break
		}
	}
	if matchFound {
		result.Anchor = historyWatchRebaseAnchor{
			Record:       matchRecord,
			TextHash:     matchTextHash,
			CursorLine:   matchLine,
			CursorOffset: matchOffset,
			Info:         info,
			Identity:     source.Identity,
		}
	}
	return result, nil
}

func historyWatchRebaseMatchProofValid(record TranscriptRecord, textHash string, previous historyTieredFileState) bool {
	if record.Kind != TranscriptKindAssistant || strings.TrimSpace(textHash) == "" {
		return false
	}
	if sourceID := historyTieredFinalSourceID(previous.LastFinalID); sourceID != "" {
		if strings.TrimSpace(record.SourceItemID) != sourceID {
			return false
		}
		if expectedTurn := strings.TrimSpace(previous.LastFinalTurnID); expectedTurn != "" &&
			strings.TrimSpace(record.TurnID) != expectedTurn {
			return false
		}
		if expectedThread := strings.TrimSpace(previous.LastFinalThreadID); expectedThread != "" &&
			strings.TrimSpace(record.ThreadID) != expectedThread {
			return false
		}
		if expectedHash := strings.TrimSpace(previous.LastFinalTextHash); expectedHash != "" &&
			textHash != expectedHash {
			return false
		}
		return true
	}
	if strings.TrimSpace(previous.LastFinalTextHash) != "" && textHash != strings.TrimSpace(previous.LastFinalTextHash) {
		return false
	}
	if strings.TrimSpace(previous.LastFinalTurnID) != "" && strings.TrimSpace(record.TurnID) != "" &&
		strings.TrimSpace(record.TurnID) != strings.TrimSpace(previous.LastFinalTurnID) {
		return false
	}
	if strings.TrimSpace(previous.LastFinalThreadID) != "" && strings.TrimSpace(record.ThreadID) != "" &&
		strings.TrimSpace(record.ThreadID) != strings.TrimSpace(previous.LastFinalThreadID) {
		return false
	}
	return strings.TrimSpace(previous.LastFinalID) != "" || strings.TrimSpace(previous.LastFinalTextHash) != ""
}

func historyRebaseStableSource(path string, expected os.FileInfo, expectedIdentity string) error {
	current, err := os.Stat(path)
	if err != nil {
		return err
	}
	if expected == nil || expected.IsDir() || current.IsDir() || !os.SameFile(expected, current) {
		return fmt.Errorf("history source %q changed during rebase", path)
	}
	// SourceFileIdentity protects against replacement, but it intentionally
	// remains stable for append-only growth and same-inode repairs.  The rebase
	// scanner is proving an anchor against one bounded snapshot, so accepting a
	// changed stat here could combine bytes from two different revisions.
	if current.Size() != expected.Size() || !current.ModTime().Equal(expected.ModTime()) {
		return fmt.Errorf("history source %q changed during rebase", path)
	}
	expectedChange := teamstore.SourceFileChangeTime(path, expected)
	currentChange := teamstore.SourceFileChangeTime(path, current)
	if expectedChange == 0 || currentChange == 0 || currentChange != expectedChange {
		return fmt.Errorf("history source %q changed during rebase", path)
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, current)
	if err != nil {
		return err
	}
	if strings.TrimSpace(identity) == "" || strings.TrimSpace(identity) != strings.TrimSpace(expectedIdentity) {
		return fmt.Errorf("history source %q identity changed during rebase", path)
	}
	return nil
}

func historyWatchRebaseAnchorMatches(record TranscriptRecord, previous historyTieredFileState) bool {
	if record.Internal || record.Kind != TranscriptKindAssistant || strings.TrimSpace(record.Text) == "" {
		return false
	}
	if sourceID := historyTieredFinalSourceID(previous.LastFinalID); sourceID != "" {
		if strings.TrimSpace(record.SourceItemID) != sourceID {
			return false
		}
		// Source item IDs are normally globally unique, but old rollout writers
		// have emitted the same ID in more than one turn.  A rebase cursor is a
		// semantic boundary, so an ID-only match must not select a record from a
		// different execution context.
		if expectedTurn := strings.TrimSpace(previous.LastFinalTurnID); expectedTurn != "" &&
			strings.TrimSpace(record.TurnID) != expectedTurn {
			return false
		}
		if expectedThread := strings.TrimSpace(previous.LastFinalThreadID); expectedThread != "" &&
			strings.TrimSpace(record.ThreadID) != expectedThread {
			return false
		}
		if expectedHash := strings.TrimSpace(previous.LastFinalTextHash); expectedHash != "" &&
			normalizedTextHash(strings.TrimSpace(record.Text)) != expectedHash {
			return false
		}
		return true
	}
	if strings.TrimSpace(previous.LastFinalID) != "" && historyTieredCompletionKey(record, 0, "") == strings.TrimSpace(previous.LastFinalID) {
		return true
	}
	if strings.TrimSpace(previous.LastFinalTextHash) == "" || strings.TrimSpace(previous.LastFinalTurnID) == "" {
		return false
	}
	if strings.TrimSpace(record.TurnID) != strings.TrimSpace(previous.LastFinalTurnID) {
		return false
	}
	if strings.TrimSpace(previous.LastFinalThreadID) != "" && strings.TrimSpace(record.ThreadID) != "" &&
		strings.TrimSpace(record.ThreadID) != strings.TrimSpace(previous.LastFinalThreadID) {
		return false
	}
	return normalizedTextHash(strings.TrimSpace(record.Text)) == strings.TrimSpace(previous.LastFinalTextHash)
}

func historyTieredFinalSourceID(key string) string {
	key = strings.TrimSpace(key)
	const prefix = "codex-final:v1:"
	if !strings.HasPrefix(key, prefix) {
		return ""
	}
	parts := strings.SplitN(strings.TrimPrefix(key, prefix), ":", 3)
	if len(parts) != 3 || strings.HasPrefix(parts[2], "terminal-line:") {
		return ""
	}
	return strings.TrimSpace(parts[2])
}

func historyRebaseSourceProof(path string, expected os.FileInfo, offset int64) (string, string, os.FileInfo, bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", nil, false
	}
	defer f.Close()
	info, err := f.Stat()
	expectedChange := teamstore.SourceFileChangeTime(path, expected)
	currentChange := teamstore.SourceFileChangeTime(path, info)
	if err != nil || expected == nil || info.IsDir() || !os.SameFile(expected, info) ||
		info.Size() != expected.Size() || !info.ModTime().Equal(expected.ModTime()) ||
		expectedChange == 0 || currentChange == 0 || currentChange != expectedChange ||
		info.Size() < offset {
		return "", "", nil, false
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		return "", "", nil, false
	}
	fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprintFromReaderWithIdentity(f, path, identity, info.Size(), offset))
	// Revalidate against the snapshot captured by the complete bounded scan,
	// not against the descriptor's own freshly-read metadata. Otherwise an
	// append or same-size rewrite racing the proof read could be accepted as a
	// valid cursor merely because the post-read stat matched itself.
	if fingerprint == "" || historyRebaseStableSource(path, expected, identity) != nil {
		return "", "", nil, false
	}
	return strings.TrimSpace(identity), fingerprint, info, true
}

func historyWatchRebaseNeedsProofHold(previous historyTieredFileState, sourceGeneration string) bool {
	sourceGeneration = strings.TrimSpace(sourceGeneration)
	if sourceGeneration == "" {
		return true
	}
	previousSourceGeneration := strings.TrimSpace(previous.SourceGeneration)
	if previousSourceGeneration == sourceGeneration {
		return true
	}
	if previousSourceGeneration == "" {
		// A legacy blocked row has no generation to compare. It may bootstrap a
		// generation from a stable anchor only when no semantic fence needs to be
		// carried across the rewrite. Same-generation modern rows remain held
		// above; context gaps, pending ranges, unresolved continuations, and
		// partial/opaque records remain explicit-recovery work below.
		return historyRebaseHasSemanticFence(previous)
	}
	if previous.ContextGap != nil && strings.TrimSpace(previous.ContextGap.SourceGeneration) != sourceGeneration {
		return true
	}
	if previous.PendingHistoryRange != nil && strings.TrimSpace(previous.PendingHistoryRange.SourceGeneration) != sourceGeneration {
		return true
	}
	return historyRebaseHasUnportableFence(previous)
}

func historyRebaseHasSemanticFence(previous historyTieredFileState) bool {
	return previous.ContextGap != nil || previous.PendingHistoryRange != nil ||
		previous.TranscriptQuarantine != nil || previous.UnresolvedContinuation ||
		previous.PendingRootTaskStarted || previous.ExternalUserPromptSeen ||
		previous.PartialLineStartOffset != 0 || previous.PartialReadOffset != 0 ||
		previous.PartialObservedSize != 0 || previous.PartialSourceIdentity != "" ||
		!previous.PartialStartedAt.IsZero() || previous.PartialPrefixReleased ||
		previous.PendingOpaqueRecordStartOffset != 0 || previous.PendingOpaqueRecordEndOffset != 0 ||
		previous.PendingOpaqueRecordID != "" || previous.HistoryRootReleased ||
		previous.pendingAssistant.Record.SourceItemID != "" || previous.pendingAssistant.Record.Text != "" ||
		previous.TerminalBoundary != nil
}

// historyRebaseHasUnportableFence lists semantic state that was derived from
// the old source and cannot be safely carried to a new source generation.
// TerminalBoundary is intentionally excluded: the rebase first proves a
// unique prior final anchor, clears the old byte-range terminal proof, and the
// next ordinary scan re-observes the new generation's terminal boundary. The
// other fields either represent an unresolved meaning or a byte range that a
// new generation may have reused for different content.
func historyRebaseHasUnportableFence(previous historyTieredFileState) bool {
	return previous.LegacySourceUnverified || previous.RecoveryProofUnusable ||
		previous.OversizedRecordBlocked || previous.HistoryRootReleased ||
		previous.UnresolvedContinuation || previous.PendingRootTaskStarted ||
		previous.ExternalUserPromptSeen || previous.PartialLineStartOffset != 0 ||
		previous.PartialReadOffset != 0 || previous.PartialObservedSize != 0 ||
		previous.PartialSourceIdentity != "" || !previous.PartialStartedAt.IsZero() ||
		previous.PartialPrefixReleased || previous.PendingOpaqueRecordStartOffset != 0 ||
		previous.PendingOpaqueRecordEndOffset != 0 || previous.PendingOpaqueRecordID != "" ||
		previous.TranscriptQuarantine != nil || previous.ContextGap != nil ||
		previous.PendingHistoryRange != nil || previous.pendingAssistant.Record.SourceItemID != "" ||
		previous.pendingAssistant.Record.Text != ""
}

func (b *Bridge) recordHistoryWatchRebaseAttempt(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, previous historyTieredFileState, path string, source codexHistoryFile, reason string, now time.Time) error {
	if expected == nil || strings.TrimSpace(source.Identity) == "" {
		return nil
	}
	previous.Path = path
	previous.SourceRewriteRecoveryIdentity = strings.TrimSpace(source.Identity)
	previous.SourceRewriteRecoverySize = source.Info.Size()
	previous.SourceRewriteRecoveryModTime = source.Info.ModTime()
	previous.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTime(path, source.Info)
	previous.SourceRewriteRecoveryScanPending = false
	previous.SourceRewriteRecoveryScanOffset = 0
	previous.SourceRewriteRecoveryScanLine = 0
	previous.SourceRewriteRecoveryScanSessionID = ""
	previous.SourceRewriteRecoveryScanThreadID = ""
	previous.SourceRewriteRecoveryScanTurnID = ""
	clearHistoryWatchRebaseScan(&previous)
	previous.SourceRewriteRecoveryReason = strings.TrimSpace(reason)
	return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expected, previous, now)
}

func clearHistoryWatchRebaseScan(state *historyTieredFileState) {
	if state == nil {
		return
	}
	state.SourceRewriteRecoveryScanPending = false
	state.SourceRewriteRecoveryScanOffset = 0
	state.SourceRewriteRecoveryScanLine = 0
	state.SourceRewriteRecoveryScanSessionID = ""
	state.SourceRewriteRecoveryScanThreadID = ""
	state.SourceRewriteRecoveryScanTurnID = ""
	state.SourceRewriteRecoveryScanMatchFound = false
	state.SourceRewriteRecoveryScanMatchLine = 0
	state.SourceRewriteRecoveryScanMatchOffset = 0
	state.SourceRewriteRecoveryScanMatchSourceItemID = ""
	state.SourceRewriteRecoveryScanMatchThreadID = ""
	state.SourceRewriteRecoveryScanMatchTurnID = ""
	state.SourceRewriteRecoveryScanMatchTextHash = ""
	state.SourceRewriteRecoveryScanMatchSourceLine = 0
	state.SourceRewriteRecoveryScanMatchStartOffset = 0
	state.SourceRewriteRecoveryScanMatchEndOffset = 0
}

func historyWatchRebaseRecordFromState(state historyTieredFileState) TranscriptRecord {
	return TranscriptRecord{
		SourceItemID:      strings.TrimSpace(state.SourceRewriteRecoveryScanMatchSourceItemID),
		ThreadID:          strings.TrimSpace(state.SourceRewriteRecoveryScanMatchThreadID),
		TurnID:            strings.TrimSpace(state.SourceRewriteRecoveryScanMatchTurnID),
		Kind:              TranscriptKindAssistant,
		SourceLine:        state.SourceRewriteRecoveryScanMatchSourceLine,
		SourceStartOffset: state.SourceRewriteRecoveryScanMatchStartOffset,
		SourceOffset:      state.SourceRewriteRecoveryScanMatchEndOffset,
	}
}

func (b *Bridge) historyWatchRebaseScanProgress(id string, identity string, previous historyTieredFileState) historyWatchRebaseScanProgress {
	initial := historyWatchRebaseScanProgress{
		SourceIdentity: strings.TrimSpace(identity),
		State: transcriptParseState{
			sessionID: strings.TrimSpace(previous.SessionID),
			threadID:  strings.TrimSpace(previous.ThreadID),
			turnID:    strings.TrimSpace(previous.TurnID),
		},
	}
	if previous.SourceRewriteRecoveryScanPending &&
		strings.TrimSpace(previous.SourceRewriteRecoveryIdentity) == strings.TrimSpace(identity) &&
		previous.SourceRewriteRecoveryScanOffset >= 0 {
		initial.Offset = previous.SourceRewriteRecoveryScanOffset
		initial.Line = previous.SourceRewriteRecoveryScanLine
		initial.State = transcriptParseState{
			sessionID: firstNonEmptyString(previous.SourceRewriteRecoveryScanSessionID, previous.SessionID),
			threadID:  firstNonEmptyString(previous.SourceRewriteRecoveryScanThreadID, previous.ThreadID),
			turnID:    firstNonEmptyString(previous.SourceRewriteRecoveryScanTurnID, previous.TurnID),
		}
		initial.MatchFound = previous.SourceRewriteRecoveryScanMatchFound
		initial.MatchLine = previous.SourceRewriteRecoveryScanMatchLine
		initial.MatchOffset = previous.SourceRewriteRecoveryScanMatchOffset
		initial.MatchTextHash = strings.TrimSpace(previous.SourceRewriteRecoveryScanMatchTextHash)
		if initial.MatchFound {
			initial.MatchRecord = historyWatchRebaseRecordFromState(previous)
		}
	}
	if b == nil {
		return initial
	}
	b.historyRebaseMu.Lock()
	defer b.historyRebaseMu.Unlock()
	progress, ok := b.historyRebaseScans[strings.TrimSpace(id)]
	if !ok || strings.TrimSpace(progress.SourceIdentity) != strings.TrimSpace(identity) || progress.Offset < 0 {
		return initial
	}
	// Durable progress is authoritative across callbacks and owners. An
	// in-memory hint can be older when another callback won the checkpoint CAS;
	// never let that hint move the next scan backwards or erase a durable match.
	if progress.Offset < initial.Offset || (initial.MatchFound && !progress.MatchFound) {
		return initial
	}
	return progress
}

func (b *Bridge) recordHistoryWatchRebaseScanProgress(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, previous historyTieredFileState, path string, source codexHistoryFile, progress historyWatchRebaseScanProgress, now time.Time) error {
	if expected == nil || strings.TrimSpace(source.Identity) == "" {
		return nil
	}
	next := previous
	next.Path = path
	next.SourceRewriteRecoveryIdentity = strings.TrimSpace(source.Identity)
	next.SourceRewriteRecoverySize = source.Info.Size()
	next.SourceRewriteRecoveryModTime = source.Info.ModTime()
	next.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTime(path, source.Info)
	next.SourceRewriteRecoveryScanPending = true
	next.SourceRewriteRecoveryScanOffset = progress.Offset
	next.SourceRewriteRecoveryScanLine = progress.Line
	next.SourceRewriteRecoveryScanSessionID = strings.TrimSpace(progress.State.sessionID)
	next.SourceRewriteRecoveryScanThreadID = strings.TrimSpace(progress.State.threadID)
	next.SourceRewriteRecoveryScanTurnID = strings.TrimSpace(progress.State.turnID)
	next.SourceRewriteRecoveryScanMatchFound = progress.MatchFound
	next.SourceRewriteRecoveryScanMatchLine = progress.MatchLine
	next.SourceRewriteRecoveryScanMatchOffset = progress.MatchOffset
	next.SourceRewriteRecoveryScanMatchSourceItemID = strings.TrimSpace(progress.MatchRecord.SourceItemID)
	next.SourceRewriteRecoveryScanMatchThreadID = strings.TrimSpace(progress.MatchRecord.ThreadID)
	next.SourceRewriteRecoveryScanMatchTurnID = strings.TrimSpace(progress.MatchRecord.TurnID)
	next.SourceRewriteRecoveryScanMatchTextHash = strings.TrimSpace(progress.MatchTextHash)
	if next.SourceRewriteRecoveryScanMatchTextHash == "" && progress.MatchFound {
		next.SourceRewriteRecoveryScanMatchTextHash = normalizedTextHash(strings.TrimSpace(progress.MatchRecord.Text))
	}
	next.SourceRewriteRecoveryScanMatchSourceLine = progress.MatchRecord.SourceLine
	next.SourceRewriteRecoveryScanMatchStartOffset = progress.MatchRecord.SourceStartOffset
	next.SourceRewriteRecoveryScanMatchEndOffset = progress.MatchRecord.SourceOffset
	next.SourceRewriteRecoveryReason = "scanning for a unique source checkpoint anchor"
	// The phase context may have expired because this cold scan used its whole
	// budget. Use a short context that preserves owner values but not the
	// cancellation, so the durable cursor can still be recorded for the next
	// cycle without allowing a slow store write to hang the listener.
	persistBase := ctx
	if persistBase == nil {
		persistBase = context.Background()
	}
	persistCtx := context.WithoutCancel(persistBase)
	persistCtx, cancel := context.WithTimeout(persistCtx, 2*time.Second)
	defer cancel()
	return b.recordHistoryWatchCheckpointIfCurrent(persistCtx, id, expected, next, now)
}

func (b *Bridge) rememberHistoryWatchRebaseScanProgress(id string, progress historyWatchRebaseScanProgress) {
	if b == nil || strings.TrimSpace(id) == "" || strings.TrimSpace(progress.SourceIdentity) == "" {
		return
	}
	b.historyRebaseMu.Lock()
	defer b.historyRebaseMu.Unlock()
	if b.historyRebaseScans == nil {
		b.historyRebaseScans = make(map[string]historyWatchRebaseScanProgress)
	}
	b.historyRebaseScans[strings.TrimSpace(id)] = progress
}

func (b *Bridge) forgetHistoryWatchRebaseScanProgress(id string) {
	if b == nil {
		return
	}
	b.historyRebaseMu.Lock()
	defer b.historyRebaseMu.Unlock()
	delete(b.historyRebaseScans, strings.TrimSpace(id))
}

func (b *Bridge) rebaseHistoryWatchSourceRewrite(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, previous historyTieredFileState, path string, now time.Time) (bool, error) {
	if !previous.SourceRewriteBlocked {
		return false, nil
	}
	source, ok, err := codexPaginatedHistoryFile(path, firstNonEmptyString(previous.ThreadID, previous.SessionID))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil || !ok {
		return false, err
	}
	if !historyRebaseChangeTimeAvailable(path, source.Info) {
		return false, b.recordHistoryWatchRebaseAttempt(ctx, id, expected, previous, path, source, "automatic source rebase requires a native file change-time revision", now)
	}
	sameIdentity := strings.TrimSpace(source.Identity) == strings.TrimSpace(previous.SourceRewriteRecoveryIdentity)
	if sameIdentity && historyRewriteRecoverySnapshotMatches(previous, path, source.Info) && !previous.SourceRewriteRecoveryScanPending {
		return false, nil
	}
	// A same-inode source may have been repaired in place. Do not resume a
	// completed scan from the old EOF in that case; the repaired anchor can be
	// anywhere in the source. Appends also reset the cold cursor harmlessly.
	if sameIdentity && !historyRewriteRecoverySnapshotMatches(previous, path, source.Info) {
		b.forgetHistoryWatchRebaseScanProgress(id)
		clearHistoryWatchRebaseScan(&previous)
	}
	progress := b.historyWatchRebaseScanProgress(id, source.Identity, previous)
	scan, err := historyWatchRebaseAnchorScan(ctx, path, previous, source, progress)
	if err != nil {
		if scan.Progress.Offset >= progress.Offset && strings.TrimSpace(scan.Progress.SourceIdentity) == strings.TrimSpace(source.Identity) {
			b.rememberHistoryWatchRebaseScanProgress(id, scan.Progress)
		}
		// Context cancellation is expected when the history phase budget expires;
		// preserve the in-memory scan cursor and let a later cycle continue. Other
		// source/read errors remain fail-closed and are retried only after the
		// candidate identity changes or an explicit recovery is requested.
		if ctx != nil && ctx.Err() != nil {
			if persistErr := b.recordHistoryWatchRebaseScanProgress(ctx, id, expected, previous, path, source, scan.Progress, now); persistErr != nil {
				return false, persistErr
			}
			return false, nil
		}
		var ambiguous *TranscriptCheckpointAmbiguousError
		if errors.As(err, &ambiguous) {
			return false, b.recordHistoryWatchRebaseAttempt(ctx, id, expected, previous, path, source, err.Error(), now)
		}
		b.forgetHistoryWatchRebaseScanProgress(id)
		return false, nil
	}
	if !scan.Complete {
		b.rememberHistoryWatchRebaseScanProgress(id, scan.Progress)
		if persistErr := b.recordHistoryWatchRebaseScanProgress(ctx, id, expected, previous, path, source, scan.Progress, now); persistErr != nil {
			return false, persistErr
		}
		return false, nil
	}
	b.forgetHistoryWatchRebaseScanProgress(id)
	if !scan.Found {
		return false, b.recordHistoryWatchRebaseAttempt(ctx, id, expected, previous, path, source, "checkpoint anchor was not found in the complete source", now)
	}
	anchor := scan.Anchor
	identity, fingerprint, info, ok := historyRebaseSourceProof(path, anchor.Info, anchor.CursorOffset)
	if !ok || identity != source.Identity {
		return false, nil
	}
	// Context gaps and pending history ranges describe bytes whose semantic
	// disposition was not proven.  They are bound to the old source identity;
	// an anchor for the last visible final does not prove that those ranges mean
	// the same thing after replacement.  Keep the rewrite fence and record the
	// candidate identity, but do not advance the physical cursor or surface a
	// phase error on every poll.  Explicit history recovery remains the only
	// operation allowed to choose a new boundary.
	if historyWatchRebaseNeedsProofHold(previous, identity) {
		return false, b.recordHistoryWatchRebaseAttempt(ctx, id, expected, previous, path, source, "checkpoint anchor found but a source-bound semantic fence requires explicit recovery", now)
	}
	next := previous
	next.Path = path
	next.Size = anchor.CursorOffset
	next.ModTime = info.ModTime()
	next.SourceChangeTime = teamstore.SourceFileChangeTime(path, info)
	next.SourceFingerprint = fingerprint
	next.SourceGeneration = source.Identity
	next.SourceRewriteBlocked = false
	next.SourceRewriteRecoveryIdentity = ""
	next.SourceRewriteRecoverySize = 0
	next.SourceRewriteRecoveryModTime = time.Time{}
	next.SourceRewriteRecoveryChangeTime = 0
	next.SourceRewriteRecoveryScanPending = false
	next.SourceRewriteRecoveryScanOffset = 0
	next.SourceRewriteRecoveryScanLine = 0
	next.SourceRewriteRecoveryScanSessionID = ""
	next.SourceRewriteRecoveryScanThreadID = ""
	next.SourceRewriteRecoveryScanTurnID = ""
	clearHistoryWatchRebaseScan(&next)
	next.SourceRewriteRecoveryReason = ""
	next.Offset = anchor.CursorOffset
	next.Line = anchor.CursorLine
	next.SessionID = firstNonEmptyString(source.Header.ThreadID, previous.SessionID)
	next.ThreadID = firstNonEmptyString(source.Header.ThreadID, previous.ThreadID)
	next.TurnID = strings.TrimSpace(anchor.Record.TurnID)
	next.LastFinalLine = anchor.Record.SourceLine
	next.LastFinalStartOffset = anchor.Record.SourceStartOffset
	next.LastFinalStartOffsetKnown = true
	next.LastFinalThreadID = strings.TrimSpace(anchor.Record.ThreadID)
	next.LastFinalTurnID = strings.TrimSpace(anchor.Record.TurnID)
	next.LastFinalTextHash = firstNonEmptyString(strings.TrimSpace(anchor.TextHash), normalizedTextHash(strings.TrimSpace(anchor.Record.Text)), previous.LastFinalTextHash)
	// The old terminal boundary is a byte-range proof for the replaced source.
	// The anchor above establishes the prior final again, while the next normal
	// scan will parse and persist a fresh terminal boundary from the new source.
	// Retain the seen bit so the scanner remains conservative, but never carry
	// the stale range object into the new generation.
	if next.TerminalBoundary != nil && strings.TrimSpace(next.TerminalBoundary.SourceGeneration) != identity {
		next.TerminalBoundary = nil
	}
	if strings.TrimSpace(anchor.Record.SourceItemID) != "" {
		next.LastFinalID = historyTieredCompletionKey(anchor.Record, 0, "")
	}
	if next.TerminalBoundarySeen {
		next.TerminalBoundaryLine = anchor.Record.SourceLine
	}
	next.ExternalUserPromptSeen = false
	next.PendingRootTaskStarted = false
	next.PendingRootTaskStartedLine = 0
	next.PendingRootTaskStartedOffset = 0
	next.pendingAssistant = historyTieredAssistantCandidate{}
	if next.UnresolvedContinuation {
		next.UnresolvedContinuationLine = anchor.Record.SourceLine
		next.UnresolvedContinuationOffset = anchor.Record.SourceStartOffset
	}
	if err := b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expected, next, now); err != nil {
		return false, err
	}
	return true, nil
}

func linkedTranscriptRebaseScanProgress(checkpoint teamstore.ImportCheckpoint, identity string, sessionID string, threadID string, turnID string) transcriptCheckpointScanProgress {
	progress := transcriptCheckpointScanProgress{
		SourceIdentity: strings.TrimSpace(identity),
		State: transcriptParseState{
			sessionID: strings.TrimSpace(sessionID),
			threadID:  strings.TrimSpace(threadID),
			turnID:    strings.TrimSpace(turnID),
		},
	}
	if checkpoint.SourceRewriteRecoveryScanPending && strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) == strings.TrimSpace(identity) &&
		checkpoint.SourceRewriteRecoveryScanOffset >= 0 {
		progress.Offset = checkpoint.SourceRewriteRecoveryScanOffset
		progress.Line = checkpoint.SourceRewriteRecoveryScanLine
		progress.State = transcriptParseState{
			sessionID: firstNonEmptyString(checkpoint.SourceRewriteRecoveryScanSessionID, sessionID),
			threadID:  firstNonEmptyString(checkpoint.SourceRewriteRecoveryScanThreadID, threadID),
			turnID:    firstNonEmptyString(checkpoint.SourceRewriteRecoveryScanTurnID, turnID),
		}
		progress.MatchFound = checkpoint.SourceRewriteRecoveryScanMatchFound
		progress.MatchLine = checkpoint.SourceRewriteRecoveryScanMatchLine
		progress.MatchOffset = checkpoint.SourceRewriteRecoveryScanMatchOffset
	}
	return progress
}

func clearLinkedTranscriptRebaseScan(checkpoint *teamstore.ImportCheckpoint) {
	if checkpoint == nil {
		return
	}
	checkpoint.SourceRewriteRecoveryScanPending = false
	checkpoint.SourceRewriteRecoveryScanOffset = 0
	checkpoint.SourceRewriteRecoveryScanLine = 0
	checkpoint.SourceRewriteRecoveryScanSessionID = ""
	checkpoint.SourceRewriteRecoveryScanThreadID = ""
	checkpoint.SourceRewriteRecoveryScanTurnID = ""
	checkpoint.SourceRewriteRecoveryScanMatchFound = false
	checkpoint.SourceRewriteRecoveryScanMatchLine = 0
	checkpoint.SourceRewriteRecoveryScanMatchOffset = 0
}

func (b *Bridge) recordLinkedTranscriptRebaseScanProgress(ctx context.Context, checkpoint teamstore.ImportCheckpoint, path string, source codexHistoryFile, progress transcriptCheckpointScanProgress) error {
	if b == nil || b.store == nil || strings.TrimSpace(checkpoint.ID) == "" || strings.TrimSpace(source.Identity) == "" {
		return nil
	}
	persistCtx := ctx
	if persistCtx == nil {
		persistCtx = context.Background()
	}
	if persistCtx.Err() != nil {
		persistCtx = context.WithoutCancel(persistCtx)
		var cancel context.CancelFunc
		persistCtx, cancel = context.WithTimeout(persistCtx, 2*time.Second)
		defer cancel()
	}
	_, _, err := b.updateImportCheckpoint(persistCtx, checkpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		if !found || !linkedTranscriptRebaseCheckpointMatches(current, checkpoint) {
			return current, false, nil
		}
		next := current
		next.SourceRewriteRecoveryIdentity = strings.TrimSpace(source.Identity)
		next.SourceRewriteRecoverySize = source.Info.Size()
		next.SourceRewriteRecoveryModTime = source.Info.ModTime()
		next.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTime(path, source.Info)
		next.SourceRewriteRecoveryScanPending = true
		next.SourceRewriteRecoveryScanOffset = progress.Offset
		next.SourceRewriteRecoveryScanLine = progress.Line
		next.SourceRewriteRecoveryScanSessionID = strings.TrimSpace(progress.State.sessionID)
		next.SourceRewriteRecoveryScanThreadID = strings.TrimSpace(progress.State.threadID)
		next.SourceRewriteRecoveryScanTurnID = strings.TrimSpace(progress.State.turnID)
		next.SourceRewriteRecoveryScanMatchFound = progress.MatchFound
		next.SourceRewriteRecoveryScanMatchLine = progress.MatchLine
		next.SourceRewriteRecoveryScanMatchOffset = progress.MatchOffset
		next.SourceRewriteRecoveryReason = "scanning for a unique source checkpoint anchor"
		next.UpdatedAt = now
		return next, true, nil
	})
	return err
}

func (b *Bridge) recordLinkedTranscriptRebaseAttempt(ctx context.Context, checkpoint teamstore.ImportCheckpoint, path string, source codexHistoryFile, reason string) error {
	if b == nil || b.store == nil || strings.TrimSpace(checkpoint.ID) == "" || strings.TrimSpace(source.Identity) == "" {
		return nil
	}
	_, _, err := b.updateImportCheckpoint(ctx, checkpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		if !found || !linkedTranscriptRebaseCheckpointMatches(current, checkpoint) {
			return current, false, nil
		}
		next := current
		next.SourceRewriteRecoveryIdentity = strings.TrimSpace(source.Identity)
		next.SourceRewriteRecoverySize = source.Info.Size()
		next.SourceRewriteRecoveryModTime = source.Info.ModTime()
		next.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTime(path, source.Info)
		clearLinkedTranscriptRebaseScan(&next)
		next.SourceRewriteRecoveryReason = strings.TrimSpace(reason)
		next.UpdatedAt = now
		return next, true, nil
	})
	return err
}

func (b *Bridge) rebaseLinkedTranscriptSourceRewrite(ctx context.Context, session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint) (bool, error) {
	if b == nil || b.store == nil || !checkpoint.SourceRewriteBlocked || strings.TrimSpace(checkpoint.LastRecordID) == "" {
		return false, nil
	}
	path := strings.TrimSpace(firstNonEmptyString(local.FilePath, checkpoint.SourcePath))
	if path == "" || (strings.TrimSpace(checkpoint.SourcePath) != "" && cleanComparablePath(checkpoint.SourcePath) != cleanComparablePath(path)) {
		return false, nil
	}
	expectedThreadID := firstNonEmptyString(local.SessionID, session.CodexThreadID)
	source, ok, err := codexPaginatedHistoryFile(path, expectedThreadID)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil || !ok {
		return false, err
	}
	if !historyRebaseChangeTimeAvailable(path, source.Info) {
		return false, b.recordLinkedTranscriptRebaseAttempt(ctx, checkpoint, path, source, "automatic source rebase requires a native file change-time revision")
	}
	recoverySnapshot := historyTieredFileState{
		SourceRewriteRecoveryIdentity:   strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity),
		SourceRewriteRecoverySize:       checkpoint.SourceRewriteRecoverySize,
		SourceRewriteRecoveryModTime:    checkpoint.SourceRewriteRecoveryModTime,
		SourceRewriteRecoveryChangeTime: checkpoint.SourceRewriteRecoveryChangeTime,
	}
	if strings.TrimSpace(source.Identity) == strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) &&
		historyRewriteRecoverySnapshotMatches(recoverySnapshot, path, source.Info) {
		if !checkpoint.SourceRewriteRecoveryScanPending {
			return false, nil
		}
	} else {
		// A changed same-inode source starts a new proof attempt. A previously
		// saved match belongs to the old snapshot and must never be combined with
		// bytes observed from the new one.
		// Keep the durable checkpoint unchanged as the CAS expectation; only the
		// in-memory scan hint is reset below.
	}
	expectedCheckpoint := checkpoint
	scanCheckpoint := checkpoint
	if strings.TrimSpace(source.Identity) != strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) ||
		!historyRewriteRecoverySnapshotMatches(recoverySnapshot, path, source.Info) {
		clearLinkedTranscriptRebaseScan(&scanCheckpoint)
	}
	progress := linkedTranscriptRebaseScanProgress(scanCheckpoint, source.Identity, expectedThreadID, expectedThreadID, "")
	scan, scanErr := scanTranscriptCheckpointWithContext(ctx, path, checkpoint.LastRecordID, source.Info, source.Identity, progress, historyRebaseMaxScanBytesPerPass)
	if os.IsNotExist(scanErr) {
		return false, nil
	}
	if scanErr != nil {
		var ambiguous *TranscriptCheckpointAmbiguousError
		if errors.As(scanErr, &ambiguous) {
			return false, b.recordLinkedTranscriptRebaseAttempt(ctx, expectedCheckpoint, path, source, scanErr.Error())
		}
		if ctx != nil && ctx.Err() != nil {
			return false, b.recordLinkedTranscriptRebaseScanProgress(ctx, expectedCheckpoint, path, source, scan.Progress)
		}
		return false, nil
	}
	if !scan.Complete {
		return false, b.recordLinkedTranscriptRebaseScanProgress(ctx, expectedCheckpoint, path, source, scan.Progress)
	}
	if !scan.Found {
		return false, b.recordLinkedTranscriptRebaseAttempt(ctx, expectedCheckpoint, path, source, "checkpoint anchor was not found in the complete source")
	}
	position := scan.Position
	currentInfo, err := os.Stat(path)
	if err != nil || currentInfo.IsDir() || !os.SameFile(source.Info, currentInfo) || currentInfo.Size() < position.Offset {
		return false, nil
	}
	identity, fingerprint, info, ok := historyRebaseSourceProof(path, source.Info, position.Offset)
	if !ok || identity != source.Identity {
		return false, nil
	}
	if linkedTranscriptRebaseNeedsProofHold(expectedCheckpoint, identity) {
		_, _, updateErr := b.updateImportCheckpoint(ctx, expectedCheckpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
			if !found || !linkedTranscriptRebaseCheckpointMatches(current, expectedCheckpoint) {
				return current, false, nil
			}
			current.SourceRewriteRecoveryIdentity = source.Identity
			current.SourceRewriteRecoverySize = source.Info.Size()
			current.SourceRewriteRecoveryModTime = source.Info.ModTime()
			current.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTime(path, source.Info)
			current.UpdatedAt = now
			return current, true, nil
		})
		return false, updateErr
	}
	_, changed, err := b.updateImportCheckpoint(ctx, expectedCheckpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		if !found || !linkedTranscriptRebaseCheckpointMatches(current, expectedCheckpoint) {
			return current, false, nil
		}
		next := current
		next.SourcePath = path
		next.SourceFingerprint = fingerprint
		next.SourceGeneration = identity
		next.LastSourceLine = position.Line
		next.LastOffset = position.Offset
		next.LastOffsetKnown = true
		next.SourceSize = info.Size()
		next.SourceModTime = info.ModTime()
		next.SourceChangeTime = teamstore.SourceFileChangeTime(path, info)
		next.SourceRewriteBlocked = false
		next.SourceRewriteRecoveryIdentity = ""
		next.SourceRewriteRecoverySize = 0
		next.SourceRewriteRecoveryModTime = time.Time{}
		next.SourceRewriteRecoveryChangeTime = 0
		next.SourceRewriteRecoveryReason = ""
		clearLinkedTranscriptRebaseScan(&next)
		if next.TerminalBoundary != nil && strings.TrimSpace(next.TerminalBoundary.SourceGeneration) != identity {
			// The rebase anchor restores the old durable record position, while a
			// later incremental scan will re-observe the terminal event in this
			// source. Do not carry a byte proof from the replaced generation.
			next.TerminalBoundary = nil
		}
		if next.CompletionPending {
			next.Status = importCheckpointStatusImporting
		} else if next.Status == importCheckpointStatusBlocked {
			next.Status = importCheckpointStatusComplete
		}
		next.UpdatedAt = now
		return next, true, nil
	})
	return changed, err
}

// linkedTranscriptRebaseCheckpointMatches supplies the expected-row CAS that
// the generic import-checkpoint updater does not provide itself. Rebase reads
// the source outside the store transaction, so checking only the source fence
// and record ID could overwrite a concurrent execution/quarantine update.
// UpdatedAt is an audit timestamp and is intentionally excluded, matching the
// history-watch checkpoint CAS semantics.
func linkedTranscriptRebaseCheckpointMatches(current, expected teamstore.ImportCheckpoint) bool {
	current.UpdatedAt = time.Time{}
	expected.UpdatedAt = time.Time{}
	return reflect.DeepEqual(current, expected)
}

func linkedTranscriptRebaseNeedsProofHold(checkpoint teamstore.ImportCheckpoint, sourceGeneration string) bool {
	sourceGeneration = strings.TrimSpace(sourceGeneration)
	if sourceGeneration == "" {
		return true
	}
	// SourceFileIdentity deliberately survives same-inode edits. A linked
	// rebase may therefore cross the automatic boundary when a modern checkpoint
	// records a different source generation, or when a legacy checkpoint with no
	// generation has no semantic fence and supplies a stable anchor. A modern
	// same-generation repair, and a legacy row carrying unresolved execution or
	// history-only state, remain in the explicit recovery lane.
	if previousSourceGeneration := strings.TrimSpace(checkpoint.SourceGeneration); previousSourceGeneration == sourceGeneration {
		return true
	}
	if strings.TrimSpace(checkpoint.SourceGeneration) == "" {
		return checkpoint.ContextGap != nil || checkpoint.PendingHistoryRange != nil ||
			checkpoint.TranscriptQuarantine != nil || checkpoint.UnresolvedExecution != nil ||
			checkpoint.CompletionPending || checkpoint.TerminalBoundarySeen || checkpoint.TerminalBoundary != nil ||
			linkedTranscriptRebaseHasUnportableFence(checkpoint)
	}
	// These durable fences have no source-bound proof that can be carried to a
	// replacement generation.  Keep them in explicit recovery; the existing
	// execution/completion path below deliberately preserves its unresolved
	// owner state while rebuilding only the history cursor.
	if linkedTranscriptRebaseHasUnportableFence(checkpoint) {
		return true
	}
	if checkpoint.ContextGap != nil && strings.TrimSpace(checkpoint.ContextGap.SourceGeneration) != sourceGeneration {
		return true
	}
	if checkpoint.PendingHistoryRange != nil && strings.TrimSpace(checkpoint.PendingHistoryRange.SourceGeneration) != sourceGeneration {
		return true
	}
	if checkpoint.TranscriptQuarantine != nil && strings.TrimSpace(checkpoint.TranscriptQuarantine.SourceGeneration) != "" &&
		strings.TrimSpace(checkpoint.TranscriptQuarantine.SourceGeneration) != sourceGeneration {
		return true
	}
	return false
}

func linkedTranscriptRebaseHasUnportableFence(checkpoint teamstore.ImportCheckpoint) bool {
	return checkpoint.LegacySourceUnverified || checkpoint.RecoveryProofUnusable ||
		checkpoint.OversizedRecordBlocked || checkpoint.DeliveryNeedsAttention ||
		checkpoint.HistoryRootReleased ||
		checkpoint.PartialLineStartOffset > 0 || checkpoint.PartialReadOffset > 0 ||
		checkpoint.PartialObservedSize > 0 || checkpoint.PartialSourceIdentity != "" ||
		!checkpoint.PartialStartedAt.IsZero() || checkpoint.PartialPrefixReleased ||
		checkpoint.PendingOpaqueRecordStartOffset > 0 || checkpoint.PendingOpaqueRecordEndOffset > 0 ||
		checkpoint.PendingOpaqueRecordID != ""
}
