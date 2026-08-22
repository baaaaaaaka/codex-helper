package teams

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
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
	CursorLine   int
	CursorOffset int64
	Info         os.FileInfo
	Identity     string
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
// closed instead of producing a cursor for a different source.
func historyWatchRebaseAnchorScan(path string, previous historyTieredFileState, source codexHistoryFile) (historyWatchRebaseAnchor, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return historyWatchRebaseAnchor{}, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return historyWatchRebaseAnchor{}, false, err
	}
	if info.IsDir() || !os.SameFile(source.Info, info) {
		return historyWatchRebaseAnchor{}, false, fmt.Errorf("history source %q changed before rebase scan", path)
	}
	reader := bufio.NewReaderSize(f, 64*1024)
	state := transcriptParseState{
		sessionID: strings.TrimSpace(previous.SessionID),
		threadID:  strings.TrimSpace(previous.ThreadID),
		turnID:    strings.TrimSpace(previous.TurnID),
	}
	var offset int64
	var lineNo int
	for {
		read, readErr := historyTieredReadJSONLRecord(reader, historyRebaseMaxLineBytes, historyRebaseMaxLineBytes)
		complete := read.Complete || (readErr == io.EOF && read.BytesRead > 0)
		if read.BytesRead > 0 {
			line := read.Line
			if read.Oversized || !complete {
				return historyWatchRebaseAnchor{}, false, historyRebaseStableSource(path, info, source.Identity)
			}
			lineStart := offset
			nextOffset := offset + read.BytesRead
			lineNo++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				records, diagnostics := parseTranscriptLine(trimmed, lineNo, &state)
				if len(diagnostics) > 0 {
					return historyWatchRebaseAnchor{}, false, historyRebaseStableSource(path, info, source.Identity)
				}
				for i := range records {
					records[i].SourceStartOffset = lineStart
					records[i].SourceOffset = nextOffset
					if !historyWatchRebaseAnchorMatches(records[i], previous) {
						continue
					}
					cursorLine, cursorOffset := lineNo, nextOffset
					// A future parser may emit more than one logical record from a
					// physical JSONL line. Re-read the whole line rather than skip
					// any record after the anchor.
					if i+1 < len(records) {
						cursorLine, cursorOffset = lineNo-1, lineStart
					}
					if historyRebaseStableSource(path, info, source.Identity) != nil {
						return historyWatchRebaseAnchor{}, false, nil
					}
					return historyWatchRebaseAnchor{
						Record:       records[i],
						CursorLine:   cursorLine,
						CursorOffset: cursorOffset,
						Info:         info,
						Identity:     source.Identity,
					}, true, nil
				}
			}
			offset = nextOffset
		}
		if readErr != nil {
			if readErr != io.EOF {
				return historyWatchRebaseAnchor{}, false, readErr
			}
			break
		}
	}
	if historyRebaseStableSource(path, info, source.Identity) != nil {
		return historyWatchRebaseAnchor{}, false, nil
	}
	return historyWatchRebaseAnchor{Info: info, Identity: source.Identity}, false, nil
}

func historyRebaseStableSource(path string, expected os.FileInfo, expectedIdentity string) error {
	current, err := os.Stat(path)
	if err != nil {
		return err
	}
	if current.IsDir() || !os.SameFile(expected, current) {
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
		return strings.TrimSpace(record.SourceItemID) == sourceID
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
	if err != nil || info.IsDir() || !os.SameFile(expected, info) || info.Size() < offset {
		return "", "", nil, false
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		return "", "", nil, false
	}
	fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprintFromReaderWithIdentity(f, path, identity, info.Size(), offset))
	if fingerprint == "" || historyRebaseStableSource(path, info, identity) != nil {
		return "", "", nil, false
	}
	return strings.TrimSpace(identity), fingerprint, info, true
}

func (b *Bridge) recordHistoryWatchRebaseAttempt(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, previous historyTieredFileState, path string, identity string, now time.Time) error {
	if expected == nil || strings.TrimSpace(identity) == "" || strings.TrimSpace(identity) == strings.TrimSpace(previous.SourceRewriteRecoveryIdentity) {
		return nil
	}
	previous.Path = path
	previous.SourceRewriteRecoveryIdentity = strings.TrimSpace(identity)
	return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expected, previous, now)
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
	if strings.TrimSpace(source.Identity) == strings.TrimSpace(previous.SourceRewriteRecoveryIdentity) {
		return false, nil
	}
	anchor, found, err := historyWatchRebaseAnchorScan(path, previous, source)
	if err != nil {
		return false, nil
	}
	if !found {
		return false, b.recordHistoryWatchRebaseAttempt(ctx, id, expected, previous, path, source.Identity, now)
	}
	identity, fingerprint, info, ok := historyRebaseSourceProof(path, anchor.Info, anchor.CursorOffset)
	if !ok || identity != source.Identity {
		return false, nil
	}
	next := previous
	next.Path = path
	next.Size = anchor.CursorOffset
	next.ModTime = info.ModTime()
	next.SourceFingerprint = fingerprint
	next.SourceRewriteBlocked = false
	next.SourceRewriteRecoveryIdentity = ""
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
	next.LastFinalTextHash = normalizedTextHash(strings.TrimSpace(anchor.Record.Text))
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
	if err != nil || !ok || strings.TrimSpace(source.Identity) == strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) {
		return false, err
	}
	position, found, err := findTranscriptCheckpointPosition(path, checkpoint.LastRecordID)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !found {
		_, _, updateErr := b.store.UpdateImportCheckpoint(ctx, checkpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
			if !found || !current.SourceRewriteBlocked || strings.TrimSpace(current.LastRecordID) != strings.TrimSpace(checkpoint.LastRecordID) {
				return current, false, nil
			}
			current.SourceRewriteRecoveryIdentity = source.Identity
			current.UpdatedAt = now
			return current, true, nil
		})
		return false, updateErr
	}
	currentInfo, err := os.Stat(path)
	if err != nil || currentInfo.IsDir() || !os.SameFile(source.Info, currentInfo) || currentInfo.Size() < position.Offset {
		return false, nil
	}
	identity, fingerprint, info, ok := historyRebaseSourceProof(path, source.Info, position.Offset)
	if !ok || identity != source.Identity {
		return false, nil
	}
	_, changed, err := b.store.UpdateImportCheckpoint(ctx, checkpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		if !found || !current.SourceRewriteBlocked || strings.TrimSpace(current.LastRecordID) != strings.TrimSpace(checkpoint.LastRecordID) ||
			strings.TrimSpace(current.SourcePath) != "" && cleanComparablePath(current.SourcePath) != cleanComparablePath(path) {
			return current, false, nil
		}
		next := current
		next.SourcePath = path
		next.SourceFingerprint = fingerprint
		next.LastSourceLine = position.Line
		next.LastOffset = position.Offset
		next.LastOffsetKnown = true
		next.SourceSize = info.Size()
		next.SourceModTime = info.ModTime()
		next.SourceRewriteBlocked = false
		next.SourceRewriteRecoveryIdentity = ""
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
