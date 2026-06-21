package delegation

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	RequestKind   = "cxp.delegation.request.v1"
	ClaimKind     = "cxp.delegation.claim.v1"
	StatusKind    = "cxp.delegation.status.v1"
	ResultKind    = "cxp.delegation.result.v1"
	TombstoneKind = "cxp.delegation.tombstone.v1"

	StateOpen          = "open"
	StateClaimed       = "claimed"
	StateRunning       = "running"
	StateComplete      = "complete"
	StateBlocked       = "blocked"
	StateCanceled      = "canceled"
	StateExpired       = "expired"
	StateConflict      = "conflict"
	StateQuestion      = "question"
	StateReuseRejected = "reuse_rejected"

	ActionStart         = "start"
	ActionAskUser       = "ask_user"
	ActionDoNotDelegate = "do_not_delegate"

	OutboxPending = "pending"
	OutboxSent    = "sent"
	OutboxVisible = "visible"
	OutboxFailed  = "failed"

	ThreadPolicyNew   = "new"
	ThreadPolicyReuse = "reuse"

	RemoteThreadStateIdle   = "idle"
	RemoteThreadStateActive = "active"
	RemoteThreadStateClosed = "closed"
	RemoteThreadStateStale  = "stale"

	MaxTaskTitleRunes           = 160
	MaxTaskObjectiveRunes       = 8000
	MaxTaskListItems            = 16
	MaxTaskListItemRunes        = 512
	MaxArtifactRefs             = 16
	MaxArtifactPathRunes        = 512
	MaxRemoteThreadSummaryRunes = 1200
	MaxRemoteThreadTitleRunes   = 160
	MaxDelegationHops           = 2
	DefaultDelegationTTL        = 2 * time.Hour
	DefaultCandidateTokenTTL    = 15 * time.Minute
	DefaultThreadTokenTTL       = 15 * time.Minute
	DefaultStoreRetention       = 24 * time.Hour
)

type TaskSpec struct {
	Title          string        `json:"title,omitempty"`
	Objective      string        `json:"objective"`
	Constraints    []string      `json:"constraints,omitempty"`
	AllowedActions []string      `json:"allowed_actions,omitempty"`
	Context        []string      `json:"context,omitempty"`
	ArtifactRefs   []ArtifactRef `json:"artifact_refs,omitempty"`
	TimeoutSeconds int           `json:"timeout_seconds,omitempty"`
}

type ArtifactRef struct {
	Kind string `json:"kind,omitempty"`
	Name string `json:"name,omitempty"`
	Path string `json:"path,omitempty"`
	Hash string `json:"hash,omitempty"`
}

type CandidateTokenPayload struct {
	MachineID             string   `json:"machine_id"`
	RegistryGeneration    string   `json:"registry_generation,omitempty"`
	CardRevision          int      `json:"card_revision,omitempty"`
	CapabilityFingerprint string   `json:"capability_fingerprint,omitempty"`
	ProtocolVersions      []string `json:"protocol_versions,omitempty"`
	InboxRef              string   `json:"inbox_ref,omitempty"`
	InboxGeneration       string   `json:"inbox_generation,omitempty"`
	ObservedAt            string   `json:"observed_at"`
	ValidUntil            string   `json:"valid_until"`
}

type ThreadTokenPayload struct {
	Policy               string `json:"policy"`
	ThreadID             string `json:"thread_id,omitempty"`
	MachineID            string `json:"machine_id"`
	SourceSessionID      string `json:"source_session_id,omitempty"`
	WorkspaceFingerprint string `json:"workspace_fingerprint,omitempty"`
	InboxRef             string `json:"inbox_ref,omitempty"`
	InboxGeneration      string `json:"inbox_generation,omitempty"`
	ThreadGeneration     string `json:"thread_generation,omitempty"`
	SummaryHash          string `json:"summary_hash,omitempty"`
	ObservedAt           string `json:"observed_at"`
	ValidUntil           string `json:"valid_until"`
}

type Candidate struct {
	MachineID             string                  `json:"machine_id"`
	InstanceID            string                  `json:"instance_id,omitempty"`
	Label                 string                  `json:"label,omitempty"`
	HostLabel             string                  `json:"host_label,omitempty"`
	Aliases               []string                `json:"aliases,omitempty"`
	State                 string                  `json:"state"`
	Accepting             bool                    `json:"accepting"`
	InboxRef              string                  `json:"inbox_ref,omitempty"`
	InboxGeneration       string                  `json:"inbox_generation,omitempty"`
	RegistryGeneration    string                  `json:"registry_generation,omitempty"`
	CardRevision          int                     `json:"card_revision,omitempty"`
	CapabilityFingerprint string                  `json:"capability_fingerprint,omitempty"`
	Capabilities          []string                `json:"capabilities,omitempty"`
	Skills                []string                `json:"skills,omitempty"`
	ProtocolVersions      []string                `json:"protocol_versions,omitempty"`
	Confidence            float64                 `json:"confidence"`
	MatchedReasons        []string                `json:"matched_reasons,omitempty"`
	NotStartableReasons   []string                `json:"not_startable_reasons,omitempty"`
	CandidateToken        string                  `json:"candidate_token,omitempty"`
	NewThreadToken        string                  `json:"new_thread_token,omitempty"`
	ThreadCandidates      []RemoteThreadCandidate `json:"thread_candidates,omitempty"`
	ValidUntil            string                  `json:"valid_until,omitempty"`
}

type RemoteThreadCandidate struct {
	ThreadID             string   `json:"thread_id"`
	MachineID            string   `json:"machine_id"`
	Title                string   `json:"title,omitempty"`
	Summary              string   `json:"summary,omitempty"`
	LastResultSummary    string   `json:"last_result_summary,omitempty"`
	WorkspaceFingerprint string   `json:"workspace_fingerprint,omitempty"`
	SourceSessionID      string   `json:"source_session_id,omitempty"`
	State                string   `json:"state"`
	LastUsedAt           string   `json:"last_used_at,omitempty"`
	ReuseConfidence      float64  `json:"reuse_confidence,omitempty"`
	ReuseReasons         []string `json:"reuse_reasons,omitempty"`
	ThreadToken          string   `json:"thread_token,omitempty"`
	ValidUntil           string   `json:"valid_until,omitempty"`
}

type ResolveResult struct {
	Action              string                  `json:"action"`
	Reason              string                  `json:"reason"`
	Query               string                  `json:"query,omitempty"`
	CandidateToken      string                  `json:"candidate_token,omitempty"`
	NewThreadToken      string                  `json:"new_thread_token,omitempty"`
	ThreadCandidates    []RemoteThreadCandidate `json:"thread_candidates,omitempty"`
	ValidUntil          string                  `json:"valid_until,omitempty"`
	Candidates          []Candidate             `json:"candidates,omitempty"`
	NotStartableReasons []string                `json:"not_startable_reasons,omitempty"`
}

type Record struct {
	Kind              string        `json:"kind"`
	RecordID          string        `json:"record_id"`
	DelegationID      string        `json:"delegation_id"`
	SourceKey         string        `json:"source_key,omitempty"`
	SourceSessionID   string        `json:"source_session_id,omitempty"`
	SourceTurnID      string        `json:"source_turn_id,omitempty"`
	ParentID          string        `json:"parent_id,omitempty"`
	Path              []string      `json:"path,omitempty"`
	HopBudget         int           `json:"hop_budget,omitempty"`
	MachineID         string        `json:"machine_id,omitempty"`
	InboxRef          string        `json:"inbox_ref,omitempty"`
	InboxGeneration   string        `json:"inbox_generation,omitempty"`
	RemoteThreadID    string        `json:"remote_thread_id,omitempty"`
	ThreadPolicy      string        `json:"thread_policy,omitempty"`
	ThreadGeneration  string        `json:"thread_generation,omitempty"`
	ThreadSummaryHash string        `json:"thread_summary_hash,omitempty"`
	ClaimID           string        `json:"claim_id,omitempty"`
	ClaimEpoch        int           `json:"claim_epoch,omitempty"`
	WorkerInstanceID  string        `json:"worker_instance_id,omitempty"`
	PayloadHash       string        `json:"payload_hash,omitempty"`
	ResultSequence    int           `json:"result_sequence,omitempty"`
	Status            string        `json:"status,omitempty"`
	Reason            string        `json:"reason,omitempty"`
	Body              string        `json:"body,omitempty"`
	Spec              TaskSpec      `json:"spec,omitempty"`
	SpecHash          string        `json:"spec_hash,omitempty"`
	ThreadUpdate      *ThreadUpdate `json:"thread_update,omitempty"`
	CreatedAt         string        `json:"created_at"`
	ExpiresAt         string        `json:"expires_at,omitempty"`
}

type ThreadUpdate struct {
	Title             string `json:"title,omitempty"`
	Summary           string `json:"summary,omitempty"`
	SummaryDelta      string `json:"summary_delta,omitempty"`
	LastResultSummary string `json:"last_result_summary,omitempty"`
	NextStepHint      string `json:"next_step_hint,omitempty"`
}

type State struct {
	DelegationID      string   `json:"delegation_id"`
	Status            string   `json:"status"`
	Request           *Record  `json:"request,omitempty"`
	WinningClaim      *Record  `json:"winning_claim,omitempty"`
	StatusRecords     []Record `json:"status_records,omitempty"`
	Terminal          *Record  `json:"terminal,omitempty"`
	IgnoredRecordIDs  []string `json:"ignored_record_ids,omitempty"`
	ConflictRecordIDs []string `json:"conflict_record_ids,omitempty"`
	Expired           bool     `json:"expired,omitempty"`
}

type Store struct {
	SchemaVersion int                       `json:"schema_version"`
	Records       []Record                  `json:"records"`
	Routes        map[string]Route          `json:"routes,omitempty"`
	RemoteThreads map[string]RemoteThread   `json:"remote_threads,omitempty"`
	Executions    map[string]ExecutionFence `json:"executions,omitempty"`
	Outbox        map[string]OutboxRecord   `json:"outbox,omitempty"`
	InboxCursors  map[string]InboxCursor    `json:"inbox_cursors,omitempty"`
	InboxBackoffs map[string]InboxBackoff   `json:"inbox_backoffs,omitempty"`
}

type Route struct {
	DelegationID     string `json:"delegation_id"`
	SourceKey        string `json:"source_key,omitempty"`
	MachineID        string `json:"machine_id"`
	InboxRef         string `json:"inbox_ref"`
	InboxGeneration  string `json:"inbox_generation,omitempty"`
	RemoteThreadID   string `json:"remote_thread_id,omitempty"`
	ThreadPolicy     string `json:"thread_policy,omitempty"`
	ThreadGeneration string `json:"thread_generation,omitempty"`
	CreatedAt        string `json:"created_at,omitempty"`
	UpdatedAt        string `json:"updated_at,omitempty"`
}

type RemoteThread struct {
	ThreadID             string `json:"thread_id"`
	MachineID            string `json:"machine_id"`
	SourceSessionID      string `json:"source_session_id,omitempty"`
	WorkspaceFingerprint string `json:"workspace_fingerprint,omitempty"`
	ModelProfile         string `json:"model_profile,omitempty"`
	ProtocolVersion      string `json:"protocol_version,omitempty"`
	Title                string `json:"title,omitempty"`
	Summary              string `json:"summary,omitempty"`
	LastResultSummary    string `json:"last_result_summary,omitempty"`
	State                string `json:"state"`
	ActiveDelegationID   string `json:"active_delegation_id,omitempty"`
	Generation           string `json:"generation,omitempty"`
	SummaryHash          string `json:"summary_hash,omitempty"`
	LastTerminalRecordID string `json:"last_terminal_record_id,omitempty"`
	CreatedAt            string `json:"created_at,omitempty"`
	UpdatedAt            string `json:"updated_at,omitempty"`
	LastUsedAt           string `json:"last_used_at,omitempty"`
	ExpiresAt            string `json:"expires_at,omitempty"`
}

type ExecutionFence struct {
	DelegationID     string `json:"delegation_id"`
	ClaimID          string `json:"claim_id"`
	ClaimEpoch       int    `json:"claim_epoch"`
	WorkerInstanceID string `json:"worker_instance_id"`
	MachineID        string `json:"machine_id"`
	Status           string `json:"status"`
	StartedAt        string `json:"started_at,omitempty"`
	UpdatedAt        string `json:"updated_at,omitempty"`
}

type OutboxRecord struct {
	RecordID     string `json:"record_id"`
	DelegationID string `json:"delegation_id,omitempty"`
	ChatID       string `json:"chat_id,omitempty"`
	InboxRef     string `json:"inbox_ref,omitempty"`
	Status       string `json:"status"`
	MessageID    string `json:"message_id,omitempty"`
	Attempts     int    `json:"attempts,omitempty"`
	Error        string `json:"error,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

type InboxCursor struct {
	ChatID            string `json:"chat_id"`
	LastHeadMessageID string `json:"last_head_message_id,omitempty"`
	UpdatedAt         string `json:"updated_at,omitempty"`
}

type InboxBackoff struct {
	ChatID       string `json:"chat_id"`
	BlockedUntil string `json:"blocked_until,omitempty"`
	Reason       string `json:"reason,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

func (s TaskSpec) Validate() error {
	if strings.TrimSpace(s.Objective) == "" {
		return fmt.Errorf("task objective is required")
	}
	if runeLen(s.Title) > MaxTaskTitleRunes {
		return fmt.Errorf("task title is too long")
	}
	if runeLen(s.Objective) > MaxTaskObjectiveRunes {
		return fmt.Errorf("task objective is too long")
	}
	for label, values := range map[string][]string{
		"constraints":     s.Constraints,
		"allowed_actions": s.AllowedActions,
		"context":         s.Context,
	} {
		if len(values) > MaxTaskListItems {
			return fmt.Errorf("%s has too many items", label)
		}
		for _, value := range values {
			if runeLen(value) > MaxTaskListItemRunes {
				return fmt.Errorf("%s item is too long", label)
			}
		}
	}
	if len(s.ArtifactRefs) > MaxArtifactRefs {
		return fmt.Errorf("artifact_refs has too many items")
	}
	for _, ref := range s.ArtifactRefs {
		if runeLen(ref.Path) > MaxArtifactPathRunes || runeLen(ref.Name) > MaxTaskListItemRunes || runeLen(ref.Hash) > MaxTaskListItemRunes {
			return fmt.Errorf("artifact_refs item is too long")
		}
	}
	if s.TimeoutSeconds < 0 || s.TimeoutSeconds > int((24*time.Hour).Seconds()) {
		return fmt.Errorf("timeout_seconds is out of range")
	}
	return nil
}

func (s TaskSpec) Hash() (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return shortHashBytes(raw, 24), nil
}

func EncodeCandidateToken(payload CandidateTokenPayload) (string, error) {
	payload.MachineID = strings.TrimSpace(payload.MachineID)
	if payload.MachineID == "" {
		return "", fmt.Errorf("candidate machine_id is required")
	}
	if strings.TrimSpace(payload.ObservedAt) == "" || strings.TrimSpace(payload.ValidUntil) == "" {
		return "", fmt.Errorf("candidate token times are required")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeCandidateToken(token string, now time.Time) (CandidateTokenPayload, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return CandidateTokenPayload{}, fmt.Errorf("decode candidate token: %w", err)
	}
	var payload CandidateTokenPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return CandidateTokenPayload{}, fmt.Errorf("parse candidate token: %w", err)
	}
	if strings.TrimSpace(payload.MachineID) == "" {
		return CandidateTokenPayload{}, fmt.Errorf("candidate token missing machine_id")
	}
	validUntil, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(payload.ValidUntil))
	if err != nil {
		return CandidateTokenPayload{}, fmt.Errorf("candidate token valid_until is invalid: %w", err)
	}
	if !now.IsZero() && now.After(validUntil) {
		return CandidateTokenPayload{}, fmt.Errorf("candidate token expired")
	}
	return payload, nil
}

func NewCandidateToken(machineID string, now time.Time, ttl time.Duration) (string, string, error) {
	return NewCandidateTokenForCandidate(Candidate{MachineID: machineID}, now, ttl)
}

func NewCandidateTokenForCandidate(candidate Candidate, now time.Time, ttl time.Duration) (string, string, error) {
	if ttl <= 0 {
		ttl = DefaultCandidateTokenTTL
	}
	payload := CandidateTokenPayload{
		MachineID:             strings.TrimSpace(candidate.MachineID),
		RegistryGeneration:    strings.TrimSpace(candidate.RegistryGeneration),
		CardRevision:          candidate.CardRevision,
		CapabilityFingerprint: strings.TrimSpace(candidate.CapabilityFingerprint),
		ProtocolVersions:      append([]string(nil), candidate.ProtocolVersions...),
		InboxRef:              strings.TrimSpace(candidate.InboxRef),
		InboxGeneration:       strings.TrimSpace(candidate.InboxGeneration),
		ObservedAt:            now.UTC().Format(time.RFC3339Nano),
		ValidUntil:            now.Add(ttl).UTC().Format(time.RFC3339Nano),
	}
	token, err := EncodeCandidateToken(payload)
	return token, payload.ValidUntil, err
}

func EncodeThreadToken(payload ThreadTokenPayload) (string, error) {
	payload.Policy = strings.TrimSpace(payload.Policy)
	payload.MachineID = strings.TrimSpace(payload.MachineID)
	payload.ThreadID = strings.TrimSpace(payload.ThreadID)
	if payload.Policy != ThreadPolicyNew && payload.Policy != ThreadPolicyReuse {
		return "", fmt.Errorf("thread token policy must be new or reuse")
	}
	if payload.MachineID == "" {
		return "", fmt.Errorf("thread token machine_id is required")
	}
	if payload.Policy == ThreadPolicyReuse && payload.ThreadID == "" {
		return "", fmt.Errorf("reuse thread token requires thread_id")
	}
	if strings.TrimSpace(payload.ObservedAt) == "" || strings.TrimSpace(payload.ValidUntil) == "" {
		return "", fmt.Errorf("thread token times are required")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeThreadToken(token string, now time.Time) (ThreadTokenPayload, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return ThreadTokenPayload{}, fmt.Errorf("decode thread token: %w", err)
	}
	var payload ThreadTokenPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ThreadTokenPayload{}, fmt.Errorf("parse thread token: %w", err)
	}
	if strings.TrimSpace(payload.MachineID) == "" {
		return ThreadTokenPayload{}, fmt.Errorf("thread token missing machine_id")
	}
	if payload.Policy != ThreadPolicyNew && payload.Policy != ThreadPolicyReuse {
		return ThreadTokenPayload{}, fmt.Errorf("thread token policy must be new or reuse")
	}
	if payload.Policy == ThreadPolicyReuse && strings.TrimSpace(payload.ThreadID) == "" {
		return ThreadTokenPayload{}, fmt.Errorf("reuse thread token missing thread_id")
	}
	validUntil, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(payload.ValidUntil))
	if err != nil {
		return ThreadTokenPayload{}, fmt.Errorf("thread token valid_until is invalid: %w", err)
	}
	if !now.IsZero() && now.After(validUntil) {
		return ThreadTokenPayload{}, fmt.Errorf("thread token expired")
	}
	return payload, nil
}

func NewThreadTokenForCandidate(candidate Candidate, sourceSessionID string, workspaceFingerprint string, now time.Time, ttl time.Duration) (string, ThreadTokenPayload, error) {
	if ttl <= 0 {
		ttl = DefaultThreadTokenTTL
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	threadID := NewRemoteThreadID(sourceSessionID, candidate.MachineID, workspaceFingerprint, now)
	payload := ThreadTokenPayload{
		Policy:               ThreadPolicyNew,
		ThreadID:             threadID,
		MachineID:            strings.TrimSpace(candidate.MachineID),
		SourceSessionID:      strings.TrimSpace(sourceSessionID),
		WorkspaceFingerprint: strings.TrimSpace(workspaceFingerprint),
		InboxRef:             strings.TrimSpace(candidate.InboxRef),
		InboxGeneration:      strings.TrimSpace(candidate.InboxGeneration),
		ThreadGeneration:     NewThreadGeneration(threadID, now),
		ObservedAt:           now.UTC().Format(time.RFC3339Nano),
		ValidUntil:           now.Add(ttl).UTC().Format(time.RFC3339Nano),
	}
	token, err := EncodeThreadToken(payload)
	return token, payload, err
}

func NewThreadTokenForThread(thread RemoteThread, now time.Time, ttl time.Duration) (string, ThreadTokenPayload, error) {
	if ttl <= 0 {
		ttl = DefaultThreadTokenTTL
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	payload := ThreadTokenPayload{
		Policy:               ThreadPolicyReuse,
		ThreadID:             strings.TrimSpace(thread.ThreadID),
		MachineID:            strings.TrimSpace(thread.MachineID),
		SourceSessionID:      strings.TrimSpace(thread.SourceSessionID),
		WorkspaceFingerprint: strings.TrimSpace(thread.WorkspaceFingerprint),
		ThreadGeneration:     strings.TrimSpace(thread.Generation),
		SummaryHash:          strings.TrimSpace(thread.SummaryHash),
		ObservedAt:           now.UTC().Format(time.RFC3339Nano),
		ValidUntil:           now.Add(ttl).UTC().Format(time.RFC3339Nano),
	}
	token, err := EncodeThreadToken(payload)
	return token, payload, err
}

func NewRemoteThreadID(sourceSessionID string, machineID string, workspaceFingerprint string, now time.Time) string {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	parts := []string{
		strings.TrimSpace(sourceSessionID),
		strings.TrimSpace(machineID),
		strings.TrimSpace(workspaceFingerprint),
		now.UTC().Format(time.RFC3339Nano),
	}
	return "rth_" + shortHashString(strings.Join(parts, "\x00"), 32)
}

func NewThreadGeneration(threadID string, now time.Time) string {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return "gen_" + shortHashString(strings.TrimSpace(threadID)+"\x00"+now.UTC().Format(time.RFC3339Nano), 16)
}

func ThreadSummaryHash(title string, summary string, lastResult string) string {
	return shortHashString(strings.Join([]string{strings.TrimSpace(title), strings.TrimSpace(summary), strings.TrimSpace(lastResult)}, "\x00"), 24)
}

func BindRequestToCandidate(record Record, payload CandidateTokenPayload) Record {
	record.InboxRef = strings.TrimSpace(payload.InboxRef)
	record.InboxGeneration = strings.TrimSpace(payload.InboxGeneration)
	return record
}

func BindRequestToThread(record Record, payload ThreadTokenPayload) Record {
	record.RemoteThreadID = strings.TrimSpace(payload.ThreadID)
	record.ThreadPolicy = strings.TrimSpace(payload.Policy)
	record.ThreadGeneration = strings.TrimSpace(payload.ThreadGeneration)
	record.ThreadSummaryHash = strings.TrimSpace(payload.SummaryHash)
	return record
}

func (s *Store) EnsureRoutes() {
	if s.Routes == nil {
		s.Routes = map[string]Route{}
	}
}

func (s *Store) EnsureRemoteThreads() {
	if s.RemoteThreads == nil {
		s.RemoteThreads = map[string]RemoteThread{}
	}
}

func (s *Store) EnsureExecutions() {
	if s.Executions == nil {
		s.Executions = map[string]ExecutionFence{}
	}
}

func (s *Store) EnsureOutbox() {
	if s.Outbox == nil {
		s.Outbox = map[string]OutboxRecord{}
	}
}

func (s *Store) EnsureInboxCursors() {
	if s.InboxCursors == nil {
		s.InboxCursors = map[string]InboxCursor{}
	}
}

func (s *Store) EnsureInboxBackoffs() {
	if s.InboxBackoffs == nil {
		s.InboxBackoffs = map[string]InboxBackoff{}
	}
}

func (s *Store) UpsertRoute(route Route) {
	if s == nil {
		return
	}
	s.EnsureRoutes()
	route.DelegationID = strings.TrimSpace(route.DelegationID)
	if route.DelegationID == "" {
		return
	}
	if strings.TrimSpace(route.CreatedAt) == "" {
		if existing, ok := s.Routes[route.DelegationID]; ok {
			route.CreatedAt = existing.CreatedAt
		}
		if strings.TrimSpace(route.CreatedAt) == "" {
			route.CreatedAt = route.UpdatedAt
		}
	}
	s.Routes[route.DelegationID] = route
}

func (s *Store) UpsertRemoteThread(thread RemoteThread) {
	if s == nil {
		return
	}
	s.EnsureRemoteThreads()
	thread.ThreadID = strings.TrimSpace(thread.ThreadID)
	if thread.ThreadID == "" {
		return
	}
	if strings.TrimSpace(thread.State) == "" {
		thread.State = RemoteThreadStateIdle
	}
	thread.Title = truncateRunes(strings.TrimSpace(thread.Title), MaxRemoteThreadTitleRunes)
	thread.Summary = truncateRunes(strings.TrimSpace(thread.Summary), MaxRemoteThreadSummaryRunes)
	thread.LastResultSummary = truncateRunes(strings.TrimSpace(thread.LastResultSummary), MaxRemoteThreadSummaryRunes)
	if strings.TrimSpace(thread.CreatedAt) == "" {
		if existing, ok := s.RemoteThreads[thread.ThreadID]; ok {
			thread.CreatedAt = existing.CreatedAt
		}
	}
	if strings.TrimSpace(thread.Generation) == "" {
		if existing, ok := s.RemoteThreads[thread.ThreadID]; ok {
			thread.Generation = existing.Generation
		}
	}
	if strings.TrimSpace(thread.SummaryHash) == "" {
		thread.SummaryHash = ThreadSummaryHash(thread.Title, thread.Summary, thread.LastResultSummary)
	}
	s.RemoteThreads[thread.ThreadID] = thread
}

func (s *Store) UpsertExecution(fence ExecutionFence) {
	if s == nil {
		return
	}
	s.EnsureExecutions()
	fence.DelegationID = strings.TrimSpace(fence.DelegationID)
	if fence.DelegationID == "" {
		return
	}
	if strings.TrimSpace(fence.StartedAt) == "" {
		if existing, ok := s.Executions[fence.DelegationID]; ok {
			fence.StartedAt = existing.StartedAt
		}
	}
	s.Executions[fence.DelegationID] = fence
}

func (s *Store) UpsertOutbox(record OutboxRecord) {
	if s == nil {
		return
	}
	s.EnsureOutbox()
	record.RecordID = strings.TrimSpace(record.RecordID)
	if record.RecordID == "" {
		return
	}
	if strings.TrimSpace(record.Status) == "" {
		record.Status = OutboxPending
	}
	if strings.TrimSpace(record.CreatedAt) == "" {
		if existing, ok := s.Outbox[record.RecordID]; ok {
			record.CreatedAt = existing.CreatedAt
		}
	}
	s.Outbox[record.RecordID] = record
}

func (s *Store) UpsertInboxCursor(cursor InboxCursor) {
	if s == nil {
		return
	}
	s.EnsureInboxCursors()
	cursor.ChatID = strings.TrimSpace(cursor.ChatID)
	if cursor.ChatID == "" {
		return
	}
	s.InboxCursors[cursor.ChatID] = cursor
}

func (s *Store) UpsertInboxBackoff(backoff InboxBackoff) {
	if s == nil {
		return
	}
	s.EnsureInboxBackoffs()
	backoff.ChatID = strings.TrimSpace(backoff.ChatID)
	if backoff.ChatID == "" {
		return
	}
	s.InboxBackoffs[backoff.ChatID] = backoff
}

func (s Store) ExecutionForID(delegationID string) (ExecutionFence, bool) {
	delegationID = strings.TrimSpace(delegationID)
	if delegationID == "" || s.Executions == nil {
		return ExecutionFence{}, false
	}
	fence, ok := s.Executions[delegationID]
	return fence, ok
}

func (s Store) OutboxForRecordID(recordID string) (OutboxRecord, bool) {
	recordID = strings.TrimSpace(recordID)
	if recordID == "" || s.Outbox == nil {
		return OutboxRecord{}, false
	}
	record, ok := s.Outbox[recordID]
	return record, ok
}

func (s Store) InboxCursorForChat(chatID string) (InboxCursor, bool) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" || s.InboxCursors == nil {
		return InboxCursor{}, false
	}
	cursor, ok := s.InboxCursors[chatID]
	return cursor, ok
}

func (s Store) InboxBackoffForChat(chatID string) (InboxBackoff, bool) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" || s.InboxBackoffs == nil {
		return InboxBackoff{}, false
	}
	backoff, ok := s.InboxBackoffs[chatID]
	return backoff, ok
}

func (s Store) RouteForID(delegationID string) (Route, bool) {
	delegationID = strings.TrimSpace(delegationID)
	if delegationID == "" || s.Routes == nil {
		return Route{}, false
	}
	route, ok := s.Routes[delegationID]
	return route, ok
}

func (s Store) RemoteThreadForID(threadID string) (RemoteThread, bool) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" || s.RemoteThreads == nil {
		return RemoteThread{}, false
	}
	thread, ok := s.RemoteThreads[threadID]
	return thread, ok
}

func (s Store) RemoteThreadsForMachine(machineID string) []RemoteThread {
	machineID = strings.TrimSpace(machineID)
	if machineID == "" || s.RemoteThreads == nil {
		return nil
	}
	out := make([]RemoteThread, 0)
	for _, thread := range s.RemoteThreads {
		if strings.TrimSpace(thread.MachineID) == machineID {
			out = append(out, thread)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return parseRecordTime(firstNonEmptyString(out[i].LastUsedAt, out[i].UpdatedAt, out[i].CreatedAt)).After(parseRecordTime(firstNonEmptyString(out[j].LastUsedAt, out[j].UpdatedAt, out[j].CreatedAt)))
	})
	return out
}

func SourceKey(sourceSessionID string, sourceTurnID string, parentID string, targetMachineID string, spec TaskSpec) (string, error) {
	specHash, err := spec.Hash()
	if err != nil {
		return "", err
	}
	parts := []string{
		strings.TrimSpace(sourceSessionID),
		strings.TrimSpace(sourceTurnID),
		strings.TrimSpace(parentID),
		strings.TrimSpace(targetMachineID),
		specHash,
	}
	return "src_" + shortHashString(strings.Join(parts, "\x00"), 32), nil
}

func DelegationID(sourceKey string) string {
	return "del_" + shortHashString(strings.TrimSpace(sourceKey), 32)
}

func NewRequestRecord(sourceSessionID string, sourceTurnID string, parentID string, path []string, targetMachineID string, spec TaskSpec, now time.Time) (Record, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	targetMachineID = strings.TrimSpace(targetMachineID)
	if targetMachineID == "" {
		return Record{}, fmt.Errorf("target machine id is required")
	}
	if err := ValidatePath(path, targetMachineID); err != nil {
		return Record{}, err
	}
	specHash, err := spec.Hash()
	if err != nil {
		return Record{}, err
	}
	sourceKey, err := SourceKey(sourceSessionID, sourceTurnID, parentID, targetMachineID, spec)
	if err != nil {
		return Record{}, err
	}
	id := DelegationID(sourceKey)
	hopBudget := MaxDelegationHops - len(appendClean(path, targetMachineID))
	if hopBudget < 0 {
		hopBudget = 0
	}
	return Record{
		Kind:            RequestKind,
		RecordID:        "rec_" + shortHashString(RequestKind+"\x00"+sourceKey, 32),
		DelegationID:    id,
		SourceKey:       sourceKey,
		SourceSessionID: strings.TrimSpace(sourceSessionID),
		SourceTurnID:    strings.TrimSpace(sourceTurnID),
		ParentID:        strings.TrimSpace(parentID),
		Path:            appendClean(path, targetMachineID),
		HopBudget:       hopBudget,
		MachineID:       targetMachineID,
		Spec:            spec,
		SpecHash:        specHash,
		CreatedAt:       now.UTC().Format(time.RFC3339Nano),
		ExpiresAt:       now.Add(DefaultDelegationTTL).UTC().Format(time.RFC3339Nano),
	}, nil
}

func NewClaimRecord(delegationID string, machineID string, workerInstanceID string, epoch int, now time.Time) (Record, error) {
	delegationID = strings.TrimSpace(delegationID)
	machineID = strings.TrimSpace(machineID)
	workerInstanceID = strings.TrimSpace(workerInstanceID)
	if delegationID == "" || machineID == "" || workerInstanceID == "" {
		return Record{}, fmt.Errorf("delegation id, machine id, and worker instance id are required")
	}
	if epoch <= 0 {
		epoch = 1
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	claimID := "claim_" + shortHashString(strings.Join([]string{delegationID, machineID, workerInstanceID, fmt.Sprint(epoch)}, "\x00"), 24)
	return Record{
		Kind:             ClaimKind,
		RecordID:         "rec_" + shortHashString(ClaimKind+"\x00"+claimID, 32),
		DelegationID:     delegationID,
		MachineID:        machineID,
		ClaimID:          claimID,
		ClaimEpoch:       epoch,
		WorkerInstanceID: workerInstanceID,
		CreatedAt:        now.UTC().Format(time.RFC3339Nano),
	}, nil
}

func NewStatusRecord(delegationID string, claim Record, status string, body string, now time.Time) (Record, error) {
	if claim.Kind != ClaimKind {
		return Record{}, fmt.Errorf("status record requires a claim")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = StateRunning
	}
	if status != StateRunning && status != StateQuestion {
		return Record{}, fmt.Errorf("intermediate status must be running or question")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	payloadHash := shortHashString(status+"\x00"+body, 24)
	return Record{
		Kind:             StatusKind,
		RecordID:         "rec_" + shortHashString(strings.Join([]string{StatusKind, delegationID, claim.ClaimID, fmt.Sprint(claim.ClaimEpoch), payloadHash, now.UTC().Format(time.RFC3339Nano)}, "\x00"), 32),
		DelegationID:     strings.TrimSpace(delegationID),
		MachineID:        claim.MachineID,
		ClaimID:          claim.ClaimID,
		ClaimEpoch:       claim.ClaimEpoch,
		WorkerInstanceID: claim.WorkerInstanceID,
		PayloadHash:      payloadHash,
		Status:           status,
		Body:             body,
		CreatedAt:        now.UTC().Format(time.RFC3339Nano),
	}, nil
}

func NewQuestionRecord(delegationID string, claim Record, body string, now time.Time) (Record, error) {
	return NewStatusRecord(delegationID, claim, StateQuestion, body, now)
}

func NewResultRecord(delegationID string, claim Record, status string, body string, sequence int, now time.Time) (Record, error) {
	if claim.Kind != ClaimKind {
		return Record{}, fmt.Errorf("result record requires a claim")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = StateComplete
	}
	if status != StateComplete && status != StateBlocked && status != StateReuseRejected {
		return Record{}, fmt.Errorf("terminal result status must be complete, blocked, or reuse_rejected")
	}
	if sequence <= 0 {
		sequence = 1
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	payloadHash := shortHashString(status+"\x00"+body, 24)
	return Record{
		Kind:             ResultKind,
		RecordID:         "rec_" + shortHashString(strings.Join([]string{ResultKind, delegationID, claim.ClaimID, fmt.Sprint(claim.ClaimEpoch), fmt.Sprint(sequence), payloadHash}, "\x00"), 32),
		DelegationID:     strings.TrimSpace(delegationID),
		MachineID:        claim.MachineID,
		ClaimID:          claim.ClaimID,
		ClaimEpoch:       claim.ClaimEpoch,
		WorkerInstanceID: claim.WorkerInstanceID,
		PayloadHash:      payloadHash,
		ResultSequence:   sequence,
		Status:           status,
		Body:             body,
		CreatedAt:        now.UTC().Format(time.RFC3339Nano),
	}, nil
}

func NewTombstoneRecord(delegationID string, reason string, now time.Time) Record {
	delegationID = strings.TrimSpace(delegationID)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return Record{
		Kind:         TombstoneKind,
		RecordID:     "rec_" + shortHashString(TombstoneKind+"\x00"+delegationID+"\x00"+now.UTC().Format(time.RFC3339Nano), 32),
		DelegationID: delegationID,
		Reason:       strings.TrimSpace(reason),
		Status:       StateCanceled,
		CreatedAt:    now.UTC().Format(time.RFC3339Nano),
	}
}

func Reduce(records []Record, now time.Time) State {
	filtered := append([]Record(nil), records...)
	sort.SliceStable(filtered, func(i, j int) bool {
		return recordBefore(filtered[i], filtered[j])
	})
	state := State{Status: StateOpen}
	var winner *Record
	var terminal *Record
	var tombstone *Record
	var request *Record
	var statuses []Record
	for _, rec := range filtered {
		if state.DelegationID == "" && strings.TrimSpace(rec.DelegationID) != "" {
			state.DelegationID = rec.DelegationID
		}
		switch rec.Kind {
		case RequestKind:
			if request == nil {
				cp := rec
				request = &cp
				continue
			}
			if rec.SourceKey == request.SourceKey && rec.SpecHash == request.SpecHash && rec.MachineID == request.MachineID {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
			} else {
				state.ConflictRecordIDs = append(state.ConflictRecordIDs, rec.RecordID)
			}
		case ClaimKind:
			if tombstone != nil || terminal != nil {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
				continue
			}
			if winner == nil || claimBefore(rec, *winner) {
				if winner != nil {
					state.ConflictRecordIDs = append(state.ConflictRecordIDs, winner.RecordID)
				}
				cp := rec
				winner = &cp
			} else if sameClaim(rec, *winner) {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
			} else {
				state.ConflictRecordIDs = append(state.ConflictRecordIDs, rec.RecordID)
			}
		case StatusKind:
			if winner == nil || !matchesClaim(rec, *winner) || terminal != nil || tombstone != nil {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
			} else {
				statuses = append(statuses, rec)
			}
		case ResultKind:
			if tombstone != nil {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
				continue
			}
			if winner == nil || !matchesClaim(rec, *winner) {
				state.ConflictRecordIDs = append(state.ConflictRecordIDs, rec.RecordID)
				continue
			}
			if terminal == nil || rec.ResultSequence > terminal.ResultSequence || (rec.ResultSequence == terminal.ResultSequence && recordBefore(rec, *terminal)) {
				if terminal != nil && rec.PayloadHash != terminal.PayloadHash {
					state.ConflictRecordIDs = append(state.ConflictRecordIDs, terminal.RecordID)
				}
				cp := rec
				terminal = &cp
			} else if rec.PayloadHash == terminal.PayloadHash {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
			} else {
				state.ConflictRecordIDs = append(state.ConflictRecordIDs, rec.RecordID)
			}
		case TombstoneKind:
			if terminal != nil {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
				continue
			}
			if tombstone == nil {
				cp := rec
				tombstone = &cp
			} else {
				state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
			}
		default:
			state.IgnoredRecordIDs = append(state.IgnoredRecordIDs, rec.RecordID)
		}
	}
	state.Request = request
	state.WinningClaim = winner
	state.StatusRecords = statuses
	state.Terminal = terminal
	switch {
	case tombstone != nil:
		state.Terminal = tombstone
		state.Status = StateCanceled
	case terminal != nil:
		state.Status = terminal.Status
	case request != nil && requestExpired(*request, now):
		state.Status = StateExpired
		state.Expired = true
	case winner != nil && len(statuses) > 0:
		state.Status = strings.TrimSpace(statuses[len(statuses)-1].Status)
		if state.Status == "" {
			state.Status = StateClaimed
		}
	case winner != nil:
		state.Status = StateClaimed
	case request != nil:
		state.Status = StateOpen
	default:
		state.Status = StateConflict
	}
	if len(state.ConflictRecordIDs) > 0 && winner == nil && terminal == nil && tombstone == nil && state.Status != StateComplete && state.Status != StateBlocked && state.Status != StateCanceled && state.Status != StateReuseRejected {
		state.Status = StateConflict
	}
	sort.Strings(state.IgnoredRecordIDs)
	sort.Strings(state.ConflictRecordIDs)
	return state
}

func ValidatePath(path []string, nextMachineID string) error {
	nextMachineID = strings.TrimSpace(nextMachineID)
	seen := map[string]bool{}
	cleaned := 0
	for _, item := range path {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		cleaned++
		if seen[item] {
			return fmt.Errorf("delegation path already contains duplicate machine %q", item)
		}
		seen[item] = true
	}
	if nextMachineID != "" && seen[nextMachineID] {
		return fmt.Errorf("delegation loop rejected for machine %q", nextMachineID)
	}
	if cleaned >= MaxDelegationHops {
		return fmt.Errorf("delegation hop limit exceeded")
	}
	return nil
}

func LoadStore(path string) (Store, error) {
	if strings.TrimSpace(path) == "" {
		return newStore(), nil
	}
	if storePathUsesSQLite(path) {
		return loadSQLiteStore(path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newStore(), nil
		}
		return Store{}, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return newStore(), nil
	}
	var store Store
	if err := json.Unmarshal(raw, &store); err != nil {
		return Store{}, err
	}
	if store.SchemaVersion == 0 {
		store.SchemaVersion = 1
	}
	store.EnsureRoutes()
	store.EnsureRemoteThreads()
	store.EnsureExecutions()
	store.EnsureOutbox()
	store.EnsureInboxCursors()
	store.EnsureInboxBackoffs()
	return store, nil
}

func SaveStore(path string, store Store) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, nil
	}
	if storePathUsesSQLite(path) {
		return saveSQLiteStore(path, store)
	}
	store.SchemaVersion = 1
	store.EnsureRoutes()
	store.EnsureRemoteThreads()
	store.EnsureExecutions()
	store.EnsureOutbox()
	store.EnsureInboxCursors()
	store.EnsureInboxBackoffs()
	raw, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return false, err
	}
	raw = append(raw, '\n')
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, raw) {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".delegation-*.tmp")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Prune(now time.Time, retention time.Duration) {
	if s == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if retention <= 0 {
		retention = DefaultStoreRetention
	}
	cutoff := now.Add(-retention)
	prunedDelegations := map[string]bool{}
	if len(s.Records) > 0 {
		byID := map[string][]Record{}
		for _, record := range s.Records {
			if id := strings.TrimSpace(record.DelegationID); id != "" {
				byID[id] = append(byID[id], record)
			}
		}
		for id, records := range byID {
			state := Reduce(records, now)
			if !terminalOrExpiredState(state) {
				continue
			}
			if t := stateTime(state); !t.IsZero() && t.Before(cutoff) {
				prunedDelegations[id] = true
			}
		}
		if len(prunedDelegations) > 0 {
			next := s.Records[:0]
			for _, record := range s.Records {
				if !prunedDelegations[record.DelegationID] {
					next = append(next, record)
				}
			}
			s.Records = next
		}
	}
	for id, outbox := range s.Outbox {
		if prunedDelegations[outbox.DelegationID] || pruneOutboxRecord(outbox, cutoff) {
			delete(s.Outbox, id)
		}
	}
	for id := range s.Routes {
		if prunedDelegations[id] {
			delete(s.Routes, id)
		}
	}
	for id, thread := range s.RemoteThreads {
		expiresAt := parseRecordTime(thread.ExpiresAt)
		updatedAt := parseRecordTime(firstNonEmptyString(thread.LastUsedAt, thread.UpdatedAt, thread.CreatedAt))
		if thread.State != RemoteThreadStateActive && ((!expiresAt.IsZero() && expiresAt.Before(cutoff)) || (!updatedAt.IsZero() && updatedAt.Before(cutoff))) {
			delete(s.RemoteThreads, id)
		}
	}
	for id, execution := range s.Executions {
		if prunedDelegations[id] || (terminalExecutionStatus(execution.Status) && parseRecordTime(execution.UpdatedAt).Before(cutoff)) {
			delete(s.Executions, id)
		}
	}
	for chatID, cursor := range s.InboxCursors {
		updatedAt := parseRecordTime(cursor.UpdatedAt)
		if !updatedAt.IsZero() && updatedAt.Before(cutoff) {
			delete(s.InboxCursors, chatID)
		}
	}
	for chatID, backoff := range s.InboxBackoffs {
		blockedUntil := parseRecordTime(backoff.BlockedUntil)
		updatedAt := parseRecordTime(backoff.UpdatedAt)
		if (!blockedUntil.IsZero() && blockedUntil.Before(cutoff)) || (!updatedAt.IsZero() && updatedAt.Before(cutoff)) {
			delete(s.InboxBackoffs, chatID)
		}
	}
}

func newStore() Store {
	return Store{
		SchemaVersion: 1,
		Routes:        map[string]Route{},
		RemoteThreads: map[string]RemoteThread{},
		Executions:    map[string]ExecutionFence{},
		Outbox:        map[string]OutboxRecord{},
		InboxCursors:  map[string]InboxCursor{},
		InboxBackoffs: map[string]InboxBackoff{},
	}
}

func terminalOrExpiredState(state State) bool {
	switch state.Status {
	case StateComplete, StateBlocked, StateCanceled, StateExpired, StateConflict, StateReuseRejected:
		return true
	default:
		return false
	}
}

func terminalExecutionStatus(status string) bool {
	switch status {
	case StateComplete, StateBlocked, StateCanceled, StateExpired, StateConflict, StateReuseRejected:
		return true
	default:
		return false
	}
}

func stateTime(state State) time.Time {
	if state.Terminal != nil {
		if t := parseRecordTime(state.Terminal.CreatedAt); !t.IsZero() {
			return t
		}
	}
	if state.Request != nil {
		if state.Status == StateExpired {
			if t := parseRecordTime(state.Request.ExpiresAt); !t.IsZero() {
				return t
			}
		}
		if t := parseRecordTime(state.Request.CreatedAt); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func pruneOutboxRecord(record OutboxRecord, cutoff time.Time) bool {
	if record.Status != OutboxVisible && record.Status != OutboxFailed {
		return false
	}
	updated := parseRecordTime(record.UpdatedAt)
	return !updated.IsZero() && updated.Before(cutoff)
}

func appendClean(path []string, next string) []string {
	out := make([]string, 0, len(path)+1)
	for _, item := range path {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	next = strings.TrimSpace(next)
	if next != "" {
		out = append(out, next)
	}
	return out
}

func requestExpired(rec Record, now time.Time) bool {
	if now.IsZero() || strings.TrimSpace(rec.ExpiresAt) == "" {
		return false
	}
	expires, err := time.Parse(time.RFC3339Nano, rec.ExpiresAt)
	if err != nil {
		return false
	}
	return now.After(expires)
}

func matchesClaim(rec Record, claim Record) bool {
	return rec.ClaimID == claim.ClaimID &&
		rec.ClaimEpoch == claim.ClaimEpoch &&
		rec.WorkerInstanceID == claim.WorkerInstanceID &&
		rec.MachineID == claim.MachineID
}

func sameClaim(a Record, b Record) bool {
	return a.ClaimID == b.ClaimID &&
		a.ClaimEpoch == b.ClaimEpoch &&
		a.WorkerInstanceID == b.WorkerInstanceID &&
		a.MachineID == b.MachineID
}

func claimBefore(a Record, b Record) bool {
	if a.ClaimEpoch != b.ClaimEpoch {
		return a.ClaimEpoch < b.ClaimEpoch
	}
	return recordBefore(a, b)
}

func recordBefore(a Record, b Record) bool {
	at := parseRecordTime(a.CreatedAt)
	bt := parseRecordTime(b.CreatedAt)
	if !at.Equal(bt) {
		return at.Before(bt)
	}
	if ak, bk := recordKindRank(a.Kind), recordKindRank(b.Kind); ak != bk {
		return ak < bk
	}
	return a.RecordID < b.RecordID
}

func recordKindRank(kind string) int {
	switch kind {
	case RequestKind:
		return 10
	case ClaimKind:
		return 20
	case StatusKind:
		return 30
	case ResultKind:
		return 40
	case TombstoneKind:
		return 50
	default:
		return 100
	}
}

func parseRecordTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	return t
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func truncateRunes(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func shortHashString(value string, n int) string {
	return shortHashBytes([]byte(value), n)
}

func shortHashBytes(value []byte, n int) string {
	sum := sha256.Sum256(value)
	encoded := hex.EncodeToString(sum[:])
	if n > 0 && n < len(encoded) {
		return encoded[:n]
	}
	return encoded
}

func runeLen(value string) int {
	return len([]rune(value))
}
