package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ForkOperationPhase is deliberately persisted instead of inferred from the
// registry. The registry is a projection and may be stale or absent after a
// crash.
type ForkOperationPhase string

const (
	ForkPhaseRequested            ForkOperationPhase = "requested"
	ForkPhaseParentFenced         ForkOperationPhase = "parent_fenced"
	ForkPhaseSnapshotMaterialized ForkOperationPhase = "snapshot_materialized"
	ForkPhaseCodexForked          ForkOperationPhase = "codex_forked"
	ForkPhaseChildChatStaged      ForkOperationPhase = "child_chat_staged"
	ForkPhaseHistoryPublishing    ForkOperationPhase = "history_publishing"
	ForkPhaseHistoryVerified      ForkOperationPhase = "history_verified"
	ForkPhaseActivated            ForkOperationPhase = "activated"
	ForkPhaseLinkSent             ForkOperationPhase = "link_sent"
	ForkPhaseFailed               ForkOperationPhase = "failed"
	ForkPhaseBlockedAmbiguous     ForkOperationPhase = "blocked_ambiguous"
	ForkPhaseAbandoned            ForkOperationPhase = "abandoned"
)

type ForkHistoryDeliveryStatus string

const (
	ForkHistoryDeliveryQueued           ForkHistoryDeliveryStatus = "queued"
	ForkHistoryDeliverySending          ForkHistoryDeliveryStatus = "sending"
	ForkHistoryDeliveryAccepted         ForkHistoryDeliveryStatus = "accepted"
	ForkHistoryDeliverySent             ForkHistoryDeliveryStatus = "sent"
	ForkHistoryDeliveryAmbiguous        ForkHistoryDeliveryStatus = "ambiguous"
	ForkHistoryDeliveryDuplicateSettled ForkHistoryDeliveryStatus = "duplicate-settled"
)

type ForkOperation struct {
	ID                      string             `json:"id"`
	CommandInboundID        string             `json:"command_inbound_id,omitempty"`
	ParentSessionID         string             `json:"parent_session_id"`
	ParentChatID            string             `json:"parent_chat_id,omitempty"`
	ParentThreadID          string             `json:"parent_thread_id,omitempty"`
	ChildSessionID          string             `json:"child_session_id,omitempty"`
	ChildChatID             string             `json:"child_chat_id,omitempty"`
	ChildChatURL            string             `json:"child_chat_url,omitempty"`
	ChildThreadID           string             `json:"child_thread_id,omitempty"`
	Phase                   ForkOperationPhase `json:"phase"`
	OwnerMachineID          string             `json:"owner_machine_id,omitempty"`
	OwnerLeaseGeneration    int64              `json:"owner_lease_generation,omitempty"`
	ParentFence             string             `json:"parent_fence,omitempty"`
	CutoffTurnID            string             `json:"cutoff_turn_id,omitempty"`
	CutoffCodexTurnID       string             `json:"cutoff_codex_turn_id,omitempty"`
	CutoffSourceRecordID    string             `json:"cutoff_source_record_id,omitempty"`
	CutoffSourceLine        int                `json:"cutoff_source_line,omitempty"`
	CutoffSourceStartOffset int64              `json:"cutoff_source_start_offset,omitempty"`
	CutoffSourceOffset      int64              `json:"cutoff_source_offset,omitempty"`
	SourcePrefixHash        string             `json:"source_prefix_hash,omitempty"`
	SourceTranscriptPath    string             `json:"source_transcript_path,omitempty"`
	SourceFingerprint       string             `json:"source_fingerprint,omitempty"`
	NativeForkWindowStart   time.Time          `json:"native_fork_window_start,omitempty"`
	NativeForkWindowEnd     time.Time          `json:"native_fork_window_end,omitempty"`
	NativeForkIntentAt      time.Time          `json:"native_fork_intent_at,omitempty"`
	ManifestID              string             `json:"manifest_id,omitempty"`
	ManifestCount           int                `json:"manifest_count,omitempty"`
	ManifestHash            string             `json:"manifest_hash,omitempty"`
	GraphExternalID         string             `json:"graph_external_id,omitempty"`
	ChatStart               time.Time          `json:"chat_start,omitempty"`
	ChatEnd                 time.Time          `json:"chat_end,omitempty"`
	HistoryNamespace        string             `json:"history_namespace,omitempty"`
	HistoryPlanVersion      int                `json:"history_plan_version,omitempty"`
	HistoryCompleteOutboxID string             `json:"history_complete_outbox_id,omitempty"`
	LinkOutboxID            string             `json:"link_outbox_id,omitempty"`
	LastError               string             `json:"last_error,omitempty"`
	LastErrorAt             time.Time          `json:"last_error_at,omitempty"`
	RetryCount              int                `json:"retry_count,omitempty"`
	CreatedAt               time.Time          `json:"created_at,omitempty"`
	UpdatedAt               time.Time          `json:"updated_at,omitempty"`
}

type ForkHistoryItem struct {
	ID                string                    `json:"id"`
	OperationID       string                    `json:"operation_id"`
	Ordinal           int                       `json:"ordinal"`
	SourceRecordID    string                    `json:"source_record_id,omitempty"`
	SourceEndRecordID string                    `json:"source_end_record_id,omitempty"`
	SourceLine        int                       `json:"source_line,omitempty"`
	SourceStartOffset int64                     `json:"source_start_offset,omitempty"`
	SourceOffset      int64                     `json:"source_offset,omitempty"`
	SourceTurnID      string                    `json:"source_turn_id,omitempty"`
	Kind              string                    `json:"kind,omitempty"`
	RenderedBody      string                    `json:"rendered_body,omitempty"`
	PartIndex         int                       `json:"part_index,omitempty"`
	PartCount         int                       `json:"part_count,omitempty"`
	RenderedBytes     int                       `json:"rendered_bytes,omitempty"`
	BodyHash          string                    `json:"body_hash,omitempty"`
	OutboxID          string                    `json:"outbox_id,omitempty"`
	TeamsMessageID    string                    `json:"teams_message_id,omitempty"`
	DeliveryStatus    ForkHistoryDeliveryStatus `json:"delivery_status"`
	CreatedAt         time.Time                 `json:"created_at,omitempty"`
	UpdatedAt         time.Time                 `json:"updated_at,omitempty"`
}

type ForkBeginRequest struct {
	OperationID          string
	CommandInboundID     string
	ParentSessionID      string
	ParentChatID         string
	ParentThreadID       string
	ChildSession         SessionContext
	CutoffTurnID         string
	CutoffCodexTurnID    string
	OwnerMachineID       string
	OwnerLeaseGeneration int64
	HistoryPlanVersion   int
	ForkWindowStart      time.Time
	ForkWindowEnd        time.Time
	Now                  time.Time
}

// ForkOwnerLease identifies the control-lease generation that is allowed to
// advance a durable fork operation. It is deliberately passed to every
// mutation that follows an external side effect; checking only at claim time
// would let an old process write milestones after a takeover.
type ForkOwnerLease struct {
	MachineID  string
	Generation int64
}

type ForkManifestMetadata struct {
	SourcePath              string
	SourceFingerprint       string
	HistoryPlanVersion      int
	CutoffSourceRecordID    string
	CutoffSourceLine        int
	CutoffSourceStartOffset int64
	CutoffSourceOffset      int64
	SourcePrefixHash        string
}

var (
	ErrForkAlreadyInProgress = errors.New("a fork is already in progress for this parent session")
	ErrForkParentBusy        = errors.New("fork parent session has a running or queued turn")
	ErrForkParentFenced      = errors.New("fork parent session is fenced")
	ErrForkOwnerLease        = errors.New("fork owner lease is not current")
	ErrForkNotFound          = errors.New("fork operation not found")
	ErrForkHistoryIncomplete = errors.New("fork history is not durably verified")
)

func ForkPhaseTerminal(phase ForkOperationPhase) bool {
	switch phase {
	case ForkPhaseLinkSent, ForkPhaseFailed, ForkPhaseAbandoned:
		return true
	default:
		return false
	}
}

func ForkPhaseBlocksParent(phase ForkOperationPhase) bool {
	return !ForkPhaseTerminal(phase)
}

// forkHistoryOutboxMaySend is the narrow exception to the normal active-session
// outbox gate. A child must remain non-active until its imported history and
// completion marker have been proven sent, but those exact history rows still
// need to traverse the normal Graph outbox path while the child is staged.
func forkHistoryOutboxMaySend(state *State, msg OutboxMessage) bool {
	if state == nil || strings.TrimSpace(msg.ForkOperationID) == "" {
		return false
	}
	role := strings.TrimSpace(msg.ForkRole)
	if role == "" || role == "link" {
		return false
	}
	if role != "complete-marker" && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(msg.Kind)), "fork-history-") {
		return false
	}
	op, ok := state.ForkOperations[strings.TrimSpace(msg.ForkOperationID)]
	if !ok || strings.TrimSpace(op.ChildSessionID) != strings.TrimSpace(msg.SessionID) || strings.TrimSpace(op.ChildChatID) != strings.TrimSpace(msg.TeamsChatID) {
		return false
	}
	if strings.TrimSpace(op.HistoryNamespace) == "" || strings.TrimSpace(msg.ForkHistoryNamespace) != strings.TrimSpace(op.HistoryNamespace) {
		return false
	}
	switch op.Phase {
	case ForkPhaseChildChatStaged, ForkPhaseHistoryPublishing, ForkPhaseHistoryVerified:
		return true
	default:
		return false
	}
}

func activeForkForSessionLocked(state *State, sessionID string) (ForkOperation, bool) {
	if state == nil {
		return ForkOperation{}, false
	}
	sessionID = strings.TrimSpace(sessionID)
	for _, op := range state.ForkOperations {
		if op.ParentSessionID == sessionID && ForkPhaseBlocksParent(op.Phase) {
			return op, true
		}
	}
	return ForkOperation{}, false
}

func (s *Store) ParentFork(ctx context.Context, sessionID string) (ForkOperation, bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ForkOperation{}, false, nil
	}
	state, err := s.Load(ctx)
	if err != nil {
		return ForkOperation{}, false, err
	}
	op, ok := activeForkForSessionLocked(&state, sessionID)
	return op, ok, nil
}

func ForkHistoryItemID(operationID string, ordinal int) string {
	return fmt.Sprintf("fork-history:%s:%06d", strings.TrimSpace(operationID), ordinal)
}

func ForkHistoryManifestHash(items []ForkHistoryItem) string {
	return forkHistoryManifestHash(items, false)
}

// ForkHistoryManifestHashForPlanVersion preserves the legacy v1 manifest hash
// while binding the v2 source range metadata into the immutable manifest. The
// plan version is part of the durable operation, so changing this format for
// v2 must not invalidate an in-flight v1 operation after an upgrade.
func ForkHistoryManifestHashForPlanVersion(items []ForkHistoryItem, planVersion int) string {
	return forkHistoryManifestHash(items, planVersion >= 2)
}

func forkHistoryManifestHash(items []ForkHistoryItem, includeSourceEnd bool) string {
	ordered := append([]ForkHistoryItem(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Ordinal < ordered[j].Ordinal })
	h := sha256.New()
	for _, item := range ordered {
		if includeSourceEnd {
			fmt.Fprintf(h, "%d\x00%s\x00%s\x00%d\x00%d\x00%d\x00%s\x00%d\x00%d\x00%d\x00%s\x00%s\n",
				item.Ordinal,
				item.SourceRecordID,
				item.SourceEndRecordID,
				item.SourceLine,
				item.SourceStartOffset,
				item.SourceOffset,
				item.SourceTurnID,
				item.PartIndex,
				item.PartCount,
				item.RenderedBytes,
				item.BodyHash,
				item.RenderedBody,
			)
			continue
		}
		fmt.Fprintf(h, "%d\x00%s\x00%d\x00%d\x00%d\x00%s\x00%d\x00%d\x00%d\x00%s\x00%s\n",
			item.Ordinal,
			item.SourceRecordID,
			item.SourceLine,
			item.SourceStartOffset,
			item.SourceOffset,
			item.SourceTurnID,
			item.PartIndex,
			item.PartCount,
			item.RenderedBytes,
			item.BodyHash,
			item.RenderedBody,
		)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func normalizeForkHistoryItem(item ForkHistoryItem, now time.Time) ForkHistoryItem {
	item.ID = strings.TrimSpace(item.ID)
	item.OperationID = strings.TrimSpace(item.OperationID)
	item.SourceRecordID = strings.TrimSpace(item.SourceRecordID)
	item.SourceEndRecordID = strings.TrimSpace(item.SourceEndRecordID)
	item.SourceTurnID = strings.TrimSpace(item.SourceTurnID)
	item.Kind = strings.TrimSpace(item.Kind)
	item.RenderedBody = strings.TrimSpace(item.RenderedBody)
	item.BodyHash = strings.TrimSpace(item.BodyHash)
	item.OutboxID = strings.TrimSpace(item.OutboxID)
	item.TeamsMessageID = strings.TrimSpace(item.TeamsMessageID)
	if item.BodyHash == "" && item.RenderedBody != "" {
		item.BodyHash = bodyHash(item.RenderedBody)
	}
	if item.PartIndex <= 0 {
		item.PartIndex = 1
	}
	if item.PartCount <= 0 {
		item.PartCount = 1
	}
	if item.RenderedBytes <= 0 && item.RenderedBody != "" {
		item.RenderedBytes = len(item.RenderedBody)
	}
	if item.ID == "" && item.OperationID != "" {
		item.ID = ForkHistoryItemID(item.OperationID, item.Ordinal)
	}
	if now.IsZero() {
		now = time.Now()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if item.DeliveryStatus == "" {
		item.DeliveryStatus = ForkHistoryDeliveryQueued
	}
	return item
}

func (s *Store) BeginFork(ctx context.Context, req ForkBeginRequest) (ForkOperation, bool, error) {
	if s == nil {
		return ForkOperation{}, false, fmt.Errorf("store is required")
	}
	req.OperationID = strings.TrimSpace(req.OperationID)
	req.ParentSessionID = strings.TrimSpace(req.ParentSessionID)
	req.ChildSession.ID = strings.TrimSpace(req.ChildSession.ID)
	if req.OperationID == "" {
		return ForkOperation{}, false, fmt.Errorf("fork operation id is required")
	}
	if req.ParentSessionID == "" {
		return ForkOperation{}, false, fmt.Errorf("fork parent session id is required")
	}
	if req.ChildSession.ID == "" {
		return ForkOperation{}, false, fmt.Errorf("fork child session id is required")
	}
	var out ForkOperation
	created := false
	err := s.Update(ctx, func(state *State) error {
		now := req.Now
		if now.IsZero() {
			now = time.Now()
		}
		state.ensure(now)
		if existing, ok := state.ForkOperations[req.OperationID]; ok {
			if existing.ParentSessionID != req.ParentSessionID {
				return fmt.Errorf("fork operation %q is bound to another parent", req.OperationID)
			}
			out = existing
			return nil
		}
		if req.OwnerMachineID != "" {
			lease := state.ControlLease
			if lease.HolderMachineID != req.OwnerMachineID || (req.OwnerLeaseGeneration > 0 && lease.Generation != req.OwnerLeaseGeneration) {
				return ErrForkOwnerLease
			}
		}
		forkWindowStart := req.ForkWindowStart
		if forkWindowStart.IsZero() {
			forkWindowStart = now
		}
		forkWindowEnd := req.ForkWindowEnd
		if forkWindowEnd.IsZero() {
			forkWindowEnd = forkWindowStart.Add(15 * time.Minute)
		}
		if !forkWindowEnd.After(forkWindowStart) {
			return fmt.Errorf("fork reconciliation window must end after it starts")
		}
		for _, existing := range state.ForkOperations {
			if existing.ParentSessionID == req.ParentSessionID && ForkPhaseBlocksParent(existing.Phase) {
				return ErrForkAlreadyInProgress
			}
		}
		parent, ok := state.Sessions[req.ParentSessionID]
		if !ok {
			return fmt.Errorf("fork parent session %q not found", req.ParentSessionID)
		}
		if !sessionStatusIsActive(parent.Status) {
			return fmt.Errorf("fork parent session %q is not active", req.ParentSessionID)
		}
		for _, turn := range state.Turns {
			if turn.SessionID == req.ParentSessionID && (turn.Status == TurnStatusRunning || turn.Status == TurnStatusQueued) {
				return ErrForkParentBusy
			}
		}
		cutoff, err := latestForkCutoffLocked(state, req.ParentSessionID, req.CutoffTurnID, req.CutoffCodexTurnID)
		if err != nil {
			return err
		}
		child := req.ChildSession
		if child.Status == "" || child.Status == SessionStatusActive {
			child.Status = SessionStatusStaging
		}
		if child.CreatedAt.IsZero() {
			child.CreatedAt = now
		}
		if child.UpdatedAt.IsZero() {
			child.UpdatedAt = child.CreatedAt
		}
		op := ForkOperation{
			ID:                    req.OperationID,
			CommandInboundID:      strings.TrimSpace(req.CommandInboundID),
			ParentSessionID:       req.ParentSessionID,
			ParentChatID:          firstStoreNonEmptyString(req.ParentChatID, parent.TeamsChatID),
			ParentThreadID:        firstStoreNonEmptyString(req.ParentThreadID, parent.CodexThreadID),
			ChildSessionID:        child.ID,
			Phase:                 ForkPhaseParentFenced,
			OwnerMachineID:        strings.TrimSpace(req.OwnerMachineID),
			OwnerLeaseGeneration:  req.OwnerLeaseGeneration,
			ParentFence:           "fork-parent:" + req.OperationID,
			CutoffTurnID:          cutoff.ID,
			CutoffCodexTurnID:     cutoff.CodexTurnID,
			NativeForkWindowStart: forkWindowStart,
			NativeForkWindowEnd:   forkWindowEnd,
			HistoryNamespace:      "fork-history:" + req.OperationID,
			HistoryPlanVersion:    req.HistoryPlanVersion,
			CreatedAt:             now,
			UpdatedAt:             now,
		}
		if op.HistoryPlanVersion <= 0 {
			op.HistoryPlanVersion = 1
		}
		state.Sessions[child.ID] = child
		state.ForkOperations[op.ID] = op
		if inboundID := strings.TrimSpace(req.CommandInboundID); inboundID != "" {
			if inbound, ok := state.InboundEvents[inboundID]; ok {
				inbound.Status = InboundStatusIgnored
				inbound.UpdatedAt = now
				state.InboundEvents[inboundID] = inbound
			}
		}
		out = op
		created = true
		return nil
	})
	return out, created, err
}

// ForkCutoff returns the cutoff selected by BeginFork. The operation, rather
// than a registry snapshot, is authoritative after the parent fence is set.
func (s *Store) ForkCutoff(ctx context.Context, operationID string) (Turn, bool, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return Turn{}, false, nil
	}
	state, err := s.loadStateFieldsOrFull(ctx, forkCutoffStateFields)
	if err != nil {
		return Turn{}, false, err
	}
	op, ok := state.ForkOperations[operationID]
	if !ok {
		return Turn{}, false, nil
	}
	turn, ok := state.Turns[op.CutoffTurnID]
	if !ok {
		for _, candidate := range state.Turns {
			if candidate.SessionID == op.ParentSessionID && candidate.CodexTurnID == op.CutoffCodexTurnID {
				turn = candidate
				ok = true
				break
			}
		}
	}
	if !ok || turn.SessionID != op.ParentSessionID || turn.Status != TurnStatusCompleted || strings.TrimSpace(turn.CodexTurnID) != strings.TrimSpace(op.CutoffCodexTurnID) {
		return Turn{}, false, nil
	}
	return turn, true, nil
}

func latestForkCutoffLocked(state *State, sessionID string, requestedTurnID string, requestedCodexTurnID string) (Turn, error) {
	if state == nil {
		return Turn{}, fmt.Errorf("fork cutoff state is required")
	}
	sessionID = strings.TrimSpace(sessionID)
	requestedTurnID = strings.TrimSpace(requestedTurnID)
	requestedCodexTurnID = strings.TrimSpace(requestedCodexTurnID)
	var completed []Turn
	for _, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != sessionID || turn.Status != TurnStatusCompleted || strings.TrimSpace(turn.CodexTurnID) == "" {
			continue
		}
		completed = append(completed, turn)
	}
	sort.Slice(completed, func(i, j int) bool {
		if !completed[i].CompletedAt.Equal(completed[j].CompletedAt) {
			return completed[i].CompletedAt.Before(completed[j].CompletedAt)
		}
		return completed[i].ID < completed[j].ID
	})
	if len(completed) == 0 {
		return Turn{}, fmt.Errorf("fork parent session %q has no completed Codex turn", sessionID)
	}
	cutoff := completed[len(completed)-1]
	if requestedTurnID == "" && requestedCodexTurnID == "" {
		return cutoff, nil
	}
	if requestedTurnID != "" && cutoff.ID != requestedTurnID {
		return Turn{}, fmt.Errorf("fork cutoff turn %q is not the latest completed turn", requestedTurnID)
	}
	if requestedCodexTurnID != "" && cutoff.CodexTurnID != requestedCodexTurnID {
		return Turn{}, fmt.Errorf("fork cutoff Codex turn %q is not the latest completed turn", requestedCodexTurnID)
	}
	return cutoff, nil
}

func (s *Store) ForkOperation(ctx context.Context, operationID string) (ForkOperation, bool, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return ForkOperation{}, false, nil
	}
	state, err := s.Load(ctx)
	if err != nil {
		return ForkOperation{}, false, err
	}
	op, ok := state.ForkOperations[operationID]
	return op, ok, nil
}

func (s *Store) ForkOperations(ctx context.Context) ([]ForkOperation, error) {
	state, err := s.loadStateFieldsOrFull(ctx, forkOperationStateFields)
	if err != nil {
		return nil, err
	}
	out := make([]ForkOperation, 0, len(state.ForkOperations))
	for _, op := range state.ForkOperations {
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (s *Store) UpdateForkOperation(ctx context.Context, operationID string, fn func(*ForkOperation) error) (ForkOperation, error) {
	return s.updateForkOperationState(ctx, operationID, nil, fnStateForkOperation(fn))
}

func fnStateForkOperation(fn func(*ForkOperation) error) func(*State, *ForkOperation) error {
	return func(_ *State, op *ForkOperation) error {
		if fn == nil {
			return fmt.Errorf("fork operation id and update function are required")
		}
		return fn(op)
	}
}

// UpdateForkOperationOwned is the owner-checked form of UpdateForkOperation.
// The check and the mutation happen under the same store transaction.
func (s *Store) UpdateForkOperationOwned(ctx context.Context, operationID string, owner ForkOwnerLease, fn func(*ForkOperation) error) (ForkOperation, error) {
	return s.updateForkOperationState(ctx, operationID, &owner, fnStateForkOperation(fn))
}

// ValidateForkOperationOwner performs the same lease and operation-owner
// check without changing durable state. It is used immediately before an
// external Codex or Graph call; the corresponding owned mutation is still
// required after that call to close the race with a takeover.
func (s *Store) ValidateForkOperationOwner(ctx context.Context, operationID string, owner ForkOwnerLease) error {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" || strings.TrimSpace(owner.MachineID) == "" || owner.Generation <= 0 {
		return ErrForkOwnerLease
	}
	now := time.Now()
	return s.Update(ctx, func(state *State) error {
		op, ok := state.ForkOperations[operationID]
		if !ok {
			return ErrForkNotFound
		}
		if err := validateForkOperationOwnerLocked(state, op, owner, now); err != nil {
			return err
		}
		return errStoreNoChange
	})
}

func validateForkOperationOwnerLocked(state *State, op ForkOperation, owner ForkOwnerLease, now time.Time) error {
	if state == nil || strings.TrimSpace(owner.MachineID) == "" || owner.Generation <= 0 {
		return ErrForkOwnerLease
	}
	lease := state.ControlLease
	if lease.HolderMachineID != owner.MachineID || lease.Generation != owner.Generation || !lease.LeaseUntil.After(now) {
		return ErrForkOwnerLease
	}
	if op.OwnerMachineID != owner.MachineID || op.OwnerLeaseGeneration != owner.Generation {
		return ErrForkOwnerLease
	}
	return nil
}

func (s *Store) updateForkOperationState(ctx context.Context, operationID string, owner *ForkOwnerLease, fn func(*State, *ForkOperation) error) (ForkOperation, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" || fn == nil {
		return ForkOperation{}, fmt.Errorf("fork operation id and update function are required")
	}
	var out ForkOperation
	err := s.Update(ctx, func(state *State) error {
		op, ok := state.ForkOperations[operationID]
		if !ok {
			return ErrForkNotFound
		}
		now := time.Now()
		if owner != nil {
			if err := validateForkOperationOwnerLocked(state, op, *owner, now); err != nil {
				return err
			}
		}
		if err := fn(state, &op); err != nil {
			return err
		}
		op.ID = operationID
		op.UpdatedAt = now
		if op.Phase == "" {
			op.Phase = ForkPhaseRequested
		}
		state.ForkOperations[operationID] = op
		out = op
		return nil
	})
	return out, err
}

// ClaimForkOperation changes the durable operation owner only while the
// caller still holds the current control lease. Recovery may therefore be
// performed by a replacement service, but a stale service cannot silently
// keep advancing an operation after another service has taken over.
func (s *Store) ClaimForkOperation(ctx context.Context, operationID string, machineID string, generation int64, now time.Time) (ForkOperation, error) {
	operationID = strings.TrimSpace(operationID)
	machineID = strings.TrimSpace(machineID)
	if operationID == "" || machineID == "" || generation <= 0 {
		return ForkOperation{}, ErrForkOwnerLease
	}
	if now.IsZero() {
		now = time.Now()
	}
	var out ForkOperation
	err := s.Update(ctx, func(state *State) error {
		lease := state.ControlLease
		if lease.HolderMachineID != machineID || lease.Generation != generation || !lease.LeaseUntil.After(now) {
			return ErrForkOwnerLease
		}
		op, ok := state.ForkOperations[operationID]
		if !ok {
			return ErrForkNotFound
		}
		if ForkPhaseTerminal(op.Phase) {
			out = op
			return nil
		}
		op.OwnerMachineID = machineID
		op.OwnerLeaseGeneration = generation
		op.UpdatedAt = now
		state.ForkOperations[operationID] = op
		out = op
		return nil
	})
	return out, err
}

func (s *Store) SaveForkManifest(ctx context.Context, operationID string, items []ForkHistoryItem, sourcePath string, sourceFingerprint string) (ForkOperation, error) {
	return s.SaveForkManifestWithMetadata(ctx, operationID, items, ForkManifestMetadata{
		SourcePath:        sourcePath,
		SourceFingerprint: sourceFingerprint,
	})
}

func (s *Store) SaveForkManifestWithMetadata(ctx context.Context, operationID string, items []ForkHistoryItem, metadata ForkManifestMetadata) (ForkOperation, error) {
	return s.saveForkManifestWithMetadata(ctx, operationID, items, metadata, nil)
}

func (s *Store) SaveForkManifestWithMetadataOwned(ctx context.Context, operationID string, items []ForkHistoryItem, metadata ForkManifestMetadata, owner ForkOwnerLease) (ForkOperation, error) {
	return s.saveForkManifestWithMetadata(ctx, operationID, items, metadata, &owner)
}

func (s *Store) saveForkManifestWithMetadata(ctx context.Context, operationID string, items []ForkHistoryItem, metadata ForkManifestMetadata, owner *ForkOwnerLease) (ForkOperation, error) {
	operationID = strings.TrimSpace(operationID)
	requestedPlanVersion := metadata.HistoryPlanVersion
	var out ForkOperation
	_, err := s.updateForkOperationState(ctx, operationID, owner, func(state *State, op *ForkOperation) error {
		operationPlanVersion := op.HistoryPlanVersion
		if operationPlanVersion <= 0 {
			operationPlanVersion = 1
		}
		planVersion := requestedPlanVersion
		if planVersion <= 0 {
			// The operation is authoritative once BeginFork has durably selected
			// a plan. This keeps legacy store callers from silently downgrading a
			// new v2 operation merely because they use the old metadata shape.
			planVersion = operationPlanVersion
		}
		if strings.TrimSpace(op.ManifestID) != "" {
			if operationPlanVersion != planVersion {
				return fmt.Errorf("fork history plan version is immutable")
			}
			normalized := make([]ForkHistoryItem, 0, len(items))
			for ordinal, item := range items {
				item.OperationID = operationID
				if item.Ordinal == 0 && ordinal != 0 {
					item.Ordinal = ordinal
				}
				item = normalizeForkHistoryItem(item, time.Now())
				if item.RenderedBody != "" {
					normalized = append(normalized, item)
				}
			}
			if len(normalized) != op.ManifestCount || ForkHistoryManifestHashForPlanVersion(normalized, operationPlanVersion) != op.ManifestHash {
				return fmt.Errorf("fork history manifest is immutable")
			}
			out = *op
			return nil
		}
		now := time.Now()
		normalized := make([]ForkHistoryItem, 0, len(items))
		for ordinal, item := range items {
			item.OperationID = operationID
			if item.Ordinal == 0 && ordinal != 0 {
				item.Ordinal = ordinal
			}
			item = normalizeForkHistoryItem(item, now)
			if item.Ordinal < 0 {
				return fmt.Errorf("fork history ordinal must not be negative")
			}
			if item.RenderedBody == "" {
				continue
			}
			normalized = append(normalized, item)
			state.ForkHistoryItems[item.ID] = item
		}
		op.SourceTranscriptPath = strings.TrimSpace(metadata.SourcePath)
		op.SourceFingerprint = strings.TrimSpace(metadata.SourceFingerprint)
		op.HistoryPlanVersion = planVersion
		op.CutoffSourceRecordID = strings.TrimSpace(metadata.CutoffSourceRecordID)
		op.CutoffSourceLine = metadata.CutoffSourceLine
		op.CutoffSourceStartOffset = metadata.CutoffSourceStartOffset
		op.CutoffSourceOffset = metadata.CutoffSourceOffset
		op.SourcePrefixHash = strings.TrimSpace(metadata.SourcePrefixHash)
		op.ManifestID = "fork-manifest:" + operationID
		op.ManifestCount = len(normalized)
		op.ManifestHash = ForkHistoryManifestHashForPlanVersion(normalized, planVersion)
		op.Phase = ForkPhaseSnapshotMaterialized
		op.UpdatedAt = now
		out = *op
		return nil
	})
	return out, err
}

func (s *Store) RecordForkCodexChild(ctx context.Context, operationID string, childThreadID string) (ForkOperation, error) {
	return s.recordForkCodexChild(ctx, operationID, childThreadID, nil)
}

func (s *Store) RecordForkCodexChildOwned(ctx context.Context, operationID string, childThreadID string, owner ForkOwnerLease) (ForkOperation, error) {
	return s.recordForkCodexChild(ctx, operationID, childThreadID, &owner)
}

func (s *Store) recordForkCodexChild(ctx context.Context, operationID string, childThreadID string, owner *ForkOwnerLease) (ForkOperation, error) {
	operationID = strings.TrimSpace(operationID)
	childThreadID = strings.TrimSpace(childThreadID)
	if childThreadID == "" {
		return ForkOperation{}, fmt.Errorf("fork child Codex thread id is required")
	}
	var out ForkOperation
	_, err := s.updateForkOperationState(ctx, operationID, owner, func(state *State, op *ForkOperation) error {
		child, ok := state.Sessions[op.ChildSessionID]
		if !ok {
			return fmt.Errorf("fork child session %q not found", op.ChildSessionID)
		}
		now := time.Now()
		op.ChildThreadID = childThreadID
		op.Phase = ForkPhaseCodexForked
		op.UpdatedAt = now
		child.CodexThreadID = childThreadID
		child.UpdatedAt = now
		state.Sessions[child.ID] = child
		out = *op
		return nil
	})
	return out, err
}

func (s *Store) StageForkChat(ctx context.Context, operationID string, chatID string, chatURL string, topic string, externalID string, start time.Time, end time.Time) (ForkOperation, error) {
	return s.stageForkChat(ctx, operationID, chatID, chatURL, topic, externalID, start, end, nil)
}

func (s *Store) StageForkChatOwned(ctx context.Context, operationID string, chatID string, chatURL string, topic string, externalID string, start time.Time, end time.Time, owner ForkOwnerLease) (ForkOperation, error) {
	return s.stageForkChat(ctx, operationID, chatID, chatURL, topic, externalID, start, end, &owner)
}

func (s *Store) stageForkChat(ctx context.Context, operationID string, chatID string, chatURL string, topic string, externalID string, start time.Time, end time.Time, owner *ForkOwnerLease) (ForkOperation, error) {
	operationID = strings.TrimSpace(operationID)
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ForkOperation{}, fmt.Errorf("fork child chat id is required")
	}
	var out ForkOperation
	_, err := s.updateForkOperationState(ctx, operationID, owner, func(state *State, op *ForkOperation) error {
		child, ok := state.Sessions[op.ChildSessionID]
		if !ok {
			return fmt.Errorf("fork child session %q not found", op.ChildSessionID)
		}
		now := time.Now()
		op.ChildChatID = chatID
		op.ChildChatURL = strings.TrimSpace(chatURL)
		op.GraphExternalID = firstStoreNonEmptyString(externalID, op.GraphExternalID, operationID)
		if !start.IsZero() {
			op.ChatStart = start
		}
		if !end.IsZero() {
			op.ChatEnd = end
		}
		op.Phase = ForkPhaseChildChatStaged
		op.UpdatedAt = now
		child.TeamsChatID = chatID
		child.TeamsChatURL = strings.TrimSpace(chatURL)
		child.TeamsTopic = strings.TrimSpace(topic)
		child.Status = SessionStatusAwaitingHistory
		child.UpdatedAt = now
		state.Sessions[child.ID] = child
		poll := state.ChatPolls[chatID]
		poll.ChatID = chatID
		poll.Seeded = true
		poll.PollState = "warm"
		if !start.IsZero() {
			poll.LastModifiedCursor = start
		}
		poll.UpdatedAt = now
		state.ChatPolls[chatID] = poll
		out = *op
		return nil
	})
	return out, err
}

func (s *Store) QueueForkHistory(ctx context.Context, operationID string, markerBody string) ([]OutboxMessage, error) {
	return s.queueForkHistory(ctx, operationID, markerBody, nil)
}

func (s *Store) QueueForkHistoryOwned(ctx context.Context, operationID string, markerBody string, owner ForkOwnerLease) ([]OutboxMessage, error) {
	return s.queueForkHistory(ctx, operationID, markerBody, &owner)
}

func (s *Store) queueForkHistory(ctx context.Context, operationID string, markerBody string, owner *ForkOwnerLease) ([]OutboxMessage, error) {
	operationID = strings.TrimSpace(operationID)
	var out []OutboxMessage
	_, err := s.updateForkOperationState(ctx, operationID, owner, func(state *State, op *ForkOperation) error {
		if op.ChildChatID == "" {
			return fmt.Errorf("fork child chat is not staged")
		}
		items := make([]ForkHistoryItem, 0)
		for _, item := range state.ForkHistoryItems {
			if item.OperationID == operationID {
				items = append(items, item)
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Ordinal < items[j].Ordinal })
		now := time.Now()
		for _, item := range items {
			if item.OutboxID == "" {
				item.OutboxID = fmt.Sprintf("fork-outbox:%s:%06d:%s", operationID, item.Ordinal, item.BodyHash[:minForkHashPrefix(len(item.BodyHash))])
			}
			msg := OutboxMessage{
				ID:                   item.OutboxID,
				SessionID:            op.ChildSessionID,
				CodexThreadID:        op.ChildThreadID,
				TeamsChatID:          op.ChildChatID,
				Kind:                 "fork-history-" + firstStoreNonEmptyString(item.Kind, "message"),
				Body:                 item.RenderedBody,
				ForkOperationID:      operationID,
				ForkHistoryNamespace: op.HistoryNamespace,
				ForkOrdinal:          item.Ordinal,
				ForkBodyHash:         item.BodyHash,
				ForkRole:             firstStoreNonEmptyString(item.Kind, "message"),
				PartIndex:            item.PartIndex,
				PartCount:            item.PartCount,
				RenderedBytes:        item.RenderedBytes,
				Status:               OutboxStatusQueued,
			}
			queued, _, err := queueOutboxLocked(state, msg, now)
			if err != nil {
				return err
			}
			item.OutboxID = queued.ID
			item.DeliveryStatus = forkHistoryDeliveryFromOutbox(queued)
			item.UpdatedAt = now
			state.ForkHistoryItems[item.ID] = item
			out = append(out, queued)
		}
		markerBody = strings.TrimSpace(markerBody)
		if markerBody == "" {
			markerBody = "History import complete."
		}
		markerID := fmt.Sprintf("fork-marker:%s", operationID)
		marker, _, err := queueOutboxLocked(state, OutboxMessage{
			ID:                   markerID,
			SessionID:            op.ChildSessionID,
			CodexThreadID:        op.ChildThreadID,
			TeamsChatID:          op.ChildChatID,
			Kind:                 "fork-history-complete",
			Body:                 markerBody,
			ForkOperationID:      operationID,
			ForkHistoryNamespace: op.HistoryNamespace,
			ForkRole:             "complete-marker",
			Status:               OutboxStatusQueued,
		}, now)
		if err != nil {
			return err
		}
		op.HistoryCompleteOutboxID = marker.ID
		op.Phase = ForkPhaseHistoryPublishing
		op.UpdatedAt = now
		out = append(out, marker)
		return nil
	})
	return out, err
}

func (s *Store) RefreshForkHistory(ctx context.Context, operationID string) (ForkOperation, bool, error) {
	return s.refreshForkHistory(ctx, operationID, nil)
}

func (s *Store) RefreshForkHistoryOwned(ctx context.Context, operationID string, owner ForkOwnerLease) (ForkOperation, bool, error) {
	return s.refreshForkHistory(ctx, operationID, &owner)
}

// MarkForkHistoryDuplicateSettled records that a Graph read found the
// provenance-marked message after the original send outcome was lost. This is
// stronger than ordinary sent: it documents that the local outbox was settled
// against an already-existing Teams message rather than a fresh POST result.
func (s *Store) MarkForkHistoryDuplicateSettled(ctx context.Context, operationID string, outboxID string, teamsMessageID string) error {
	return s.markForkHistoryDuplicateSettled(ctx, operationID, outboxID, teamsMessageID, nil)
}

func (s *Store) MarkForkHistoryDuplicateSettledOwned(ctx context.Context, operationID string, outboxID string, teamsMessageID string, owner ForkOwnerLease) error {
	return s.markForkHistoryDuplicateSettled(ctx, operationID, outboxID, teamsMessageID, &owner)
}

func (s *Store) markForkHistoryDuplicateSettled(ctx context.Context, operationID string, outboxID string, teamsMessageID string, owner *ForkOwnerLease) error {
	operationID = strings.TrimSpace(operationID)
	outboxID = strings.TrimSpace(outboxID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if operationID == "" || outboxID == "" || teamsMessageID == "" {
		return fmt.Errorf("fork duplicate settlement requires operation, outbox, and Teams message ids")
	}
	_, err := s.updateForkOperationState(ctx, operationID, owner, func(state *State, _ *ForkOperation) error {
		for id, item := range state.ForkHistoryItems {
			if item.OperationID != operationID || item.OutboxID != outboxID {
				continue
			}
			item.TeamsMessageID = teamsMessageID
			item.DeliveryStatus = ForkHistoryDeliveryDuplicateSettled
			item.UpdatedAt = time.Now()
			state.ForkHistoryItems[id] = item
			return nil
		}
		return fmt.Errorf("fork history item for outbox %q not found", outboxID)
	})
	return err
}

func (s *Store) refreshForkHistory(ctx context.Context, operationID string, owner *ForkOwnerLease) (ForkOperation, bool, error) {
	operationID = strings.TrimSpace(operationID)
	var out ForkOperation
	verified := false
	_, err := s.updateForkOperationState(ctx, operationID, owner, func(state *State, op *ForkOperation) error {
		changed := false
		now := time.Now()
		finish := func() error {
			out = *op
			if !changed {
				return errStoreNoChange
			}
			return nil
		}
		items := make([]ForkHistoryItem, 0)
		for id, item := range state.ForkHistoryItems {
			if item.OperationID != operationID {
				continue
			}
			if item.OutboxID != "" {
				if msg, ok := state.OutboxMessages[item.OutboxID]; ok {
					teamsMessageID := strings.TrimSpace(msg.TeamsMessageID)
					deliveryStatus := forkHistoryDeliveryFromOutbox(msg)
					itemChanged := item.TeamsMessageID != teamsMessageID
					if item.DeliveryStatus != ForkHistoryDeliveryDuplicateSettled {
						itemChanged = itemChanged || item.DeliveryStatus != deliveryStatus
					}
					if itemChanged {
						item.TeamsMessageID = teamsMessageID
						if item.DeliveryStatus != ForkHistoryDeliveryDuplicateSettled {
							item.DeliveryStatus = deliveryStatus
						}
						item.UpdatedAt = now
						state.ForkHistoryItems[id] = item
						changed = true
					}
				}
			}
			items = append(items, item)
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Ordinal < items[j].Ordinal })
		if strings.TrimSpace(op.HistoryNamespace) == "" || strings.TrimSpace(op.ManifestID) == "" || strings.TrimSpace(op.SourceTranscriptPath) == "" || strings.TrimSpace(op.SourceFingerprint) == "" || strings.TrimSpace(op.CutoffSourceRecordID) == "" || op.CutoffSourceLine <= 0 || op.CutoffSourceOffset <= 0 || strings.TrimSpace(op.SourcePrefixHash) == "" {
			return finish()
		}
		planVersion := op.HistoryPlanVersion
		if planVersion <= 0 {
			planVersion = 1
		}
		if len(items) != op.ManifestCount || ForkHistoryManifestHashForPlanVersion(items, planVersion) != op.ManifestHash {
			return finish()
		}
		for _, item := range items {
			if item.DeliveryStatus != ForkHistoryDeliverySent && item.DeliveryStatus != ForkHistoryDeliveryDuplicateSettled {
				return finish()
			}
		}
		marker, ok := state.OutboxMessages[op.HistoryCompleteOutboxID]
		if !ok || marker.ForkOperationID != operationID || marker.ForkHistoryNamespace != op.HistoryNamespace || marker.ForkRole != "complete-marker" || marker.Kind != "fork-history-complete" || marker.Status != OutboxStatusSent || strings.TrimSpace(marker.TeamsMessageID) == "" {
			return finish()
		}
		for _, item := range items {
			if item.OutboxID == "" {
				return finish()
			}
			history, ok := state.OutboxMessages[item.OutboxID]
			if !ok || history.ForkOperationID != operationID || history.ForkHistoryNamespace != op.HistoryNamespace || history.ForkRole == "link" {
				return finish()
			}
			if marker.Sequence <= 0 || history.Sequence <= 0 || marker.Sequence <= history.Sequence {
				return finish()
			}
			if item.DeliveryStatus == ForkHistoryDeliveryDuplicateSettled && item.TeamsMessageID != "" {
				continue
			}
			if history.Status != OutboxStatusSent || strings.TrimSpace(history.TeamsMessageID) == "" {
				return finish()
			}
		}
		duplicateSettled := make(map[string]bool)
		for _, item := range items {
			if item.DeliveryStatus == ForkHistoryDeliveryDuplicateSettled && strings.TrimSpace(item.TeamsMessageID) != "" {
				duplicateSettled[item.OutboxID] = true
			}
		}
		for _, message := range state.OutboxMessages {
			if message.ForkOperationID != operationID || message.ForkRole == "link" {
				continue
			}
			if message.ForkHistoryNamespace != op.HistoryNamespace {
				return finish()
			}
			if message.ID != marker.ID && (marker.Sequence <= 0 || message.Sequence <= 0 || marker.Sequence <= message.Sequence) {
				return finish()
			}
			if duplicateSettled[message.ID] {
				continue
			}
			if message.Status != OutboxStatusSent || strings.TrimSpace(message.TeamsMessageID) == "" {
				return finish()
			}
		}
		if op.Phase != ForkPhaseHistoryVerified {
			op.Phase = ForkPhaseHistoryVerified
			changed = true
		}
		out = *op
		verified = true
		if !changed {
			return errStoreNoChange
		}
		return nil
	})
	return out, verified, err
}

func (s *Store) ActivateFork(ctx context.Context, operationID string, link OutboxMessage) (ForkOperation, error) {
	return s.activateFork(ctx, operationID, link, nil)
}

func (s *Store) ActivateForkOwned(ctx context.Context, operationID string, link OutboxMessage, owner ForkOwnerLease) (ForkOperation, error) {
	return s.activateFork(ctx, operationID, link, &owner)
}

func (s *Store) activateFork(ctx context.Context, operationID string, link OutboxMessage, owner *ForkOwnerLease) (ForkOperation, error) {
	operationID = strings.TrimSpace(operationID)
	var out ForkOperation
	_, err := s.updateForkOperationState(ctx, operationID, owner, func(state *State, op *ForkOperation) error {
		if op.Phase != ForkPhaseHistoryVerified && op.Phase != ForkPhaseActivated && op.Phase != ForkPhaseLinkSent {
			return ErrForkHistoryIncomplete
		}
		child, ok := state.Sessions[op.ChildSessionID]
		if !ok {
			return fmt.Errorf("fork child session %q not found", op.ChildSessionID)
		}
		child.Status = SessionStatusActive
		child.CodexThreadID = firstStoreNonEmptyString(child.CodexThreadID, op.ChildThreadID)
		child.UpdatedAt = time.Now()
		state.Sessions[child.ID] = child
		op.Phase = ForkPhaseActivated
		if strings.TrimSpace(link.ID) != "" {
			if link.ForkHistoryNamespace != "" && link.ForkHistoryNamespace != op.HistoryNamespace {
				return fmt.Errorf("fork link history namespace does not match operation")
			}
			link.ForkHistoryNamespace = op.HistoryNamespace
			queued, _, err := queueOutboxLocked(state, link, time.Now())
			if err != nil {
				return err
			}
			op.LinkOutboxID = queued.ID
		}
		op.UpdatedAt = time.Now()
		out = *op
		return nil
	})
	return out, err
}

func (s *Store) MarkForkLinkSent(ctx context.Context, operationID string) (ForkOperation, error) {
	return s.markForkLinkSent(ctx, operationID, nil)
}

func (s *Store) MarkForkLinkSentOwned(ctx context.Context, operationID string, owner ForkOwnerLease) (ForkOperation, error) {
	return s.markForkLinkSent(ctx, operationID, &owner)
}

func (s *Store) markForkLinkSent(ctx context.Context, operationID string, owner *ForkOwnerLease) (ForkOperation, error) {
	return s.updateForkOperationState(ctx, operationID, owner, func(_ *State, op *ForkOperation) error {
		if op.LinkOutboxID == "" {
			return fmt.Errorf("fork link outbox is not recorded")
		}
		op.Phase = ForkPhaseLinkSent
		return nil
	})
}

func forkHistoryDeliveryFromOutbox(msg OutboxMessage) ForkHistoryDeliveryStatus {
	switch msg.Status {
	case OutboxStatusSent:
		if strings.TrimSpace(msg.TeamsMessageID) != "" {
			return ForkHistoryDeliverySent
		}
	case OutboxStatusAccepted:
		if strings.TrimSpace(msg.TeamsMessageID) != "" {
			return ForkHistoryDeliveryAccepted
		}
	case OutboxStatusSending:
		if OutboxSendIsAmbiguous(msg) {
			return ForkHistoryDeliveryAmbiguous
		}
		return ForkHistoryDeliverySending
	case OutboxStatusQueued:
		return ForkHistoryDeliveryQueued
	}
	return ForkHistoryDeliveryQueued
}

func minForkHashPrefix(length int) int {
	if length < 12 {
		return length
	}
	return 12
}
