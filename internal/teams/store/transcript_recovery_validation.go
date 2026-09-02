package store

import (
	"fmt"
	"strings"
)

// Execution-anchor provenance is deliberately independent of RecoveryReason.
// Empty provenance is a legacy row and must never be treated as runtime proof.
const (
	ExecutionAnchorProvenanceRuntime     = "runtime"
	ExecutionAnchorProvenanceHistoryOnly = "history_only"
	ExecutionAnchorProvenanceLegacy      = "legacy_unknown"
)

func ExecutionAnchorIsRuntime(anchor *ExecutionAnchor) bool {
	return anchor != nil && strings.TrimSpace(anchor.Provenance) == ExecutionAnchorProvenanceRuntime
}

func ExecutionAnchorIsHistoryOnly(anchor *ExecutionAnchor) bool {
	return anchor != nil && strings.TrimSpace(anchor.Provenance) == ExecutionAnchorProvenanceHistoryOnly
}

func ExecutionAnchorIsLegacyUnknown(anchor *ExecutionAnchor) bool {
	if anchor == nil {
		return false
	}
	provenance := strings.TrimSpace(anchor.Provenance)
	return provenance == "" || provenance == ExecutionAnchorProvenanceLegacy
}

// These exported predicates are the store boundary used by JSON/SQLite
// validators. Nil means that the optional proof is absent, which is valid for
// legacy checkpoints; a present proof must be internally complete.
func ContextGapStateValid(gap *ContextGapState) bool {
	return gap == nil || gap.valid()
}

func ContextGapStateCommitted(gap *ContextGapState) bool {
	if gap == nil {
		return false
	}
	switch gap.Phase {
	case ContextGapPhasePostGapCommitted, ContextGapPhaseRetired:
		return true
	default:
		return false
	}
}

func HistoryPendingRangeValid(rng *HistoryPendingRange) bool {
	return rng == nil || rng.valid()
}

// TranscriptQuarantineValid validates only the shape of a persisted
// quarantine.  A metadata-only quarantine is intentionally valid: it is an
// explanation/fence, but it contains no frontier that can advance a cursor.
// Only fields that describe a concrete byte frontier require the complete
// source observation; source names, generations, fingerprints, and candidate
// hashes may be retained as diagnostic metadata without becoming a proof.
func TranscriptQuarantineValid(quarantine *TranscriptQuarantine) bool {
	if quarantine == nil {
		return true
	}
	if strings.TrimSpace(quarantine.Kind) == "" {
		return false
	}
	hasFrontier := strings.TrimSpace(quarantine.FrontierRecordID) != "" ||
		quarantine.FrontierLine != 0 || quarantine.FrontierOffset != 0 ||
		quarantine.ExclusiveEndOffset != 0 || strings.TrimSpace(quarantine.RangeFingerprint) != ""
	if !hasFrontier {
		return true
	}
	return strings.TrimSpace(quarantine.SourcePath) != "" &&
		strings.TrimSpace(quarantine.SourceGeneration) != "" &&
		strings.TrimSpace(quarantine.FrontierRecordID) != "" &&
		quarantine.FrontierLine > 0 &&
		quarantine.FrontierOffset >= 0 &&
		quarantine.ExclusiveEndOffset > quarantine.FrontierOffset &&
		strings.TrimSpace(quarantine.RangeFingerprint) != ""
}

// TranscriptQuarantineHasFrontier distinguishes a diagnostic quarantine from
// one that claims a concrete cursor boundary. It is intentionally weaker than
// TranscriptQuarantineValid so callers can fail closed on a malformed claimed
// frontier while still ignoring metadata-only explanations for cursor
// advancement.
func TranscriptQuarantineHasFrontier(quarantine *TranscriptQuarantine) bool {
	if quarantine == nil {
		return false
	}
	return strings.TrimSpace(quarantine.FrontierRecordID) != "" ||
		quarantine.FrontierLine != 0 || quarantine.FrontierOffset != 0 ||
		quarantine.ExclusiveEndOffset != 0 || strings.TrimSpace(quarantine.RangeFingerprint) != ""
}

// TranscriptQuarantineFrontierUsable is the only predicate that permits an
// automatic scanner to use a quarantine as a physical frontier.
func TranscriptQuarantineFrontierUsable(quarantine *TranscriptQuarantine) bool {
	return TranscriptQuarantineHasFrontier(quarantine) && TranscriptQuarantineValid(quarantine)
}

// importCheckpointOptionalProofUsable is deliberately separate from the
// identity/provenance validators.  A malformed optional range proof should
// make automatic history conservative, but must not make the entire store
// unreadable or turn a history-only issue into a live execution fence.
func importCheckpointOptionalProofUsable(checkpoint ImportCheckpoint) bool {
	if !ContextGapStateValid(checkpoint.ContextGap) ||
		!HistoryPendingRangeValid(checkpoint.PendingHistoryRange) ||
		!TranscriptQuarantineValid(checkpoint.TranscriptQuarantine) ||
		!TerminalBoundaryValid(checkpoint.TerminalBoundary) {
		return false
	}
	sourceGeneration := strings.TrimSpace(checkpoint.SourceGeneration)
	if sourceGeneration == "" && (checkpoint.ContextGap != nil || checkpoint.PendingHistoryRange != nil || checkpoint.TerminalBoundary != nil) {
		return false
	}
	if checkpoint.ContextGap != nil && strings.TrimSpace(checkpoint.ContextGap.SourceGeneration) != sourceGeneration {
		return false
	}
	if checkpoint.PendingHistoryRange != nil && strings.TrimSpace(checkpoint.PendingHistoryRange.SourceGeneration) != sourceGeneration {
		return false
	}
	if quarantine := checkpoint.TranscriptQuarantine; quarantine != nil && strings.TrimSpace(quarantine.SourceGeneration) != "" && strings.TrimSpace(quarantine.SourceGeneration) != sourceGeneration {
		return false
	}
	if checkpoint.TerminalBoundary != nil && strings.TrimSpace(checkpoint.TerminalBoundary.SourceGeneration) != sourceGeneration {
		return false
	}
	return true
}

func historyWatchOptionalProofUsable(checkpoint HistoryWatchCheckpoint) bool {
	if !ContextGapStateValid(checkpoint.ContextGap) ||
		!HistoryPendingRangeValid(checkpoint.PendingHistoryRange) ||
		!TerminalBoundaryValid(checkpoint.TerminalBoundary) {
		return false
	}
	sourceGeneration := strings.TrimSpace(checkpoint.SourceGeneration)
	if sourceGeneration == "" && (checkpoint.ContextGap != nil || checkpoint.PendingHistoryRange != nil || checkpoint.TerminalBoundary != nil) {
		return false
	}
	if checkpoint.ContextGap != nil && strings.TrimSpace(checkpoint.ContextGap.SourceGeneration) != sourceGeneration {
		return false
	}
	if checkpoint.PendingHistoryRange != nil && strings.TrimSpace(checkpoint.PendingHistoryRange.SourceGeneration) != sourceGeneration {
		return false
	}
	if checkpoint.TerminalBoundary != nil && strings.TrimSpace(checkpoint.TerminalBoundary.SourceGeneration) != sourceGeneration {
		return false
	}
	return true
}

func TerminalBoundaryValid(boundary *TerminalBoundary) bool {
	if boundary == nil {
		return true
	}
	return strings.TrimSpace(boundary.SourceGeneration) != "" &&
		strings.TrimSpace(boundary.RecordID) != "" &&
		boundary.Line > 0 &&
		boundary.StartOffset >= 0 &&
		boundary.ExclusiveEndOffset > boundary.StartOffset &&
		strings.TrimSpace(boundary.RangeFingerprint) != "" &&
		strings.TrimSpace(boundary.Kind) != ""
}

// validateHistoryWatchCheckpointState is intentionally stricter than the
// legacy JSON decoder but still accepts old rows that have no source identity
// or range proof. Once a proof is present, all offsets and generations must
// agree so a corrupt row cannot be used as a new scanner cursor.
func validateHistoryWatchCheckpointState(checkpoint HistoryWatchCheckpoint, id string) error {
	id = strings.TrimSpace(id)
	if id == "" || strings.TrimSpace(checkpoint.ID) != id {
		return fmt.Errorf("%w: history-watch row id %q does not match requested %q", ErrSessionStateProvenanceMismatch, checkpoint.ID, id)
	}
	if checkpoint.Size < 0 || checkpoint.Offset < 0 ||
		checkpoint.Line < 0 || checkpoint.LastFinalLine < 0 ||
		checkpoint.LastFinalStartOffset < 0 || checkpoint.UnresolvedContinuationLine < 0 ||
		checkpoint.UnresolvedContinuationOffset < 0 ||
		checkpoint.PartialLineStartOffset < 0 || checkpoint.PartialReadOffset < 0 ||
		checkpoint.PartialObservedSize < 0 || checkpoint.PartialLine < 0 ||
		checkpoint.PendingAssistantSourceLine < 0 || checkpoint.PendingAssistantStartOffset < 0 ||
		checkpoint.PendingAssistantOffset < 0 || checkpoint.SourceRewriteRecoveryScanOffset < 0 ||
		checkpoint.SourceRewriteRecoveryScanLine < 0 {
		return fmt.Errorf("%w: history-watch row %q has a negative cursor", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.SourceRewriteRecoveryScanPending {
		if strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) == "" ||
			checkpoint.SourceRewriteRecoverySize <= 0 || checkpoint.SourceRewriteRecoveryModTime.IsZero() {
			return fmt.Errorf("%w: history-watch row %q has an unbound rebase scan cursor", ErrSessionStateProvenanceMismatch, id)
		}
	}
	if checkpoint.PartialReadOffset > 0 && checkpoint.PartialReadOffset < checkpoint.PartialLineStartOffset {
		return fmt.Errorf("%w: history-watch row %q partial cursor precedes line start", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.PartialObservedSize > 0 && checkpoint.PartialReadOffset > checkpoint.PartialObservedSize {
		return fmt.Errorf("%w: history-watch row %q partial cursor exceeds observed size", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.PartialLineStartOffset > 0 && checkpoint.PartialLineStartOffset < checkpoint.Offset {
		return fmt.Errorf("%w: history-watch row %q partial line precedes durable cursor", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.PartialReadOffset > 0 && checkpoint.Size > 0 && checkpoint.PartialReadOffset > checkpoint.Size {
		return fmt.Errorf("%w: history-watch row %q partial cursor exceeds file size", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.RecoveryProofUnusable {
		return nil
	}
	if !historyWatchOptionalProofUsable(checkpoint) {
		return fmt.Errorf("%w: history-watch row %q has invalid recovery proof", ErrSessionStateProvenanceMismatch, id)
	}
	sourceGeneration := strings.TrimSpace(checkpoint.SourceGeneration)
	if sourceGeneration == "" && (checkpoint.ContextGap != nil || checkpoint.PendingHistoryRange != nil || checkpoint.TerminalBoundary != nil) {
		return fmt.Errorf("%w: history-watch row %q has proof without source generation", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.ContextGap != nil && strings.TrimSpace(checkpoint.ContextGap.SourceGeneration) != sourceGeneration {
		return fmt.Errorf("%w: history-watch row %q context-gap generation mismatch", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.PendingHistoryRange != nil && strings.TrimSpace(checkpoint.PendingHistoryRange.SourceGeneration) != sourceGeneration {
		return fmt.Errorf("%w: history-watch row %q pending-range generation mismatch", ErrSessionStateProvenanceMismatch, id)
	}
	if checkpoint.TerminalBoundary != nil && strings.TrimSpace(checkpoint.TerminalBoundary.SourceGeneration) != sourceGeneration {
		return fmt.Errorf("%w: history-watch row %q terminal-boundary generation mismatch", ErrSessionStateProvenanceMismatch, id)
	}
	return nil
}
