package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const (
	SchemaVersion = 3

	dirMode  os.FileMode = 0o700
	fileMode os.FileMode = 0o600

	maxRetainedSentOutboxMessages      = 512
	maxRetainedTranscriptLedgerRecords = 1024
)

type SessionStatus string

const (
	SessionStatusActive   SessionStatus = "active"
	SessionStatusArchived SessionStatus = "archived"
	SessionStatusClosed   SessionStatus = "closed"
)

type TurnStatus string

const (
	TurnStatusQueued      TurnStatus = "queued"
	TurnStatusRunning     TurnStatus = "running"
	TurnStatusCompleted   TurnStatus = "completed"
	TurnStatusFailed      TurnStatus = "failed"
	TurnStatusInterrupted TurnStatus = "interrupted"
)

type InboundStatus string

const (
	InboundStatusPersisted InboundStatus = "persisted"
	InboundStatusQueued    InboundStatus = "queued"
	InboundStatusIgnored   InboundStatus = "ignored"
	InboundStatusDeferred  InboundStatus = "deferred"
)

type OutboxStatus string

const (
	OutboxStatusQueued   OutboxStatus = "queued"
	OutboxStatusSending  OutboxStatus = "sending"
	OutboxStatusAccepted OutboxStatus = "accepted"
	OutboxStatusSent     OutboxStatus = "sent"
)

var ErrOutboxSendNotClaimed = errors.New("outbox send not claimed")
var ErrUpgradeInProgress = errors.New("Teams upgrade already in progress")

const outboxSendLease = 2 * time.Minute

const (
	HelperUpgradeReason = "codex-proxy upgrade"
	HelperReloadReason  = "codex-proxy reload"
)

type State struct {
	SchemaVersion     int                               `json:"schema_version"`
	CreatedAt         time.Time                         `json:"created_at,omitempty"`
	UpdatedAt         time.Time                         `json:"updated_at,omitempty"`
	Scope             ScopeIdentity                     `json:"scope,omitempty"`
	MachineIdentity   MachineIdentity                   `json:"machine_identity,omitempty"`
	Machines          map[string]MachineRecord          `json:"machines,omitempty"`
	ControlLease      ControlLease                      `json:"control_lease,omitempty"`
	ControlChat       ControlChatBinding                `json:"control_chat,omitempty"`
	ServiceOwner      *OwnerMetadata                    `json:"service_owner,omitempty"`
	LockOwner         *OwnerMetadata                    `json:"lock_owner,omitempty"`
	ServiceControl    ServiceControl                    `json:"service_control,omitempty"`
	Upgrade           *UpgradeRequest                   `json:"upgrade,omitempty"`
	Sessions          map[string]SessionContext         `json:"sessions,omitempty"`
	Turns             map[string]Turn                   `json:"turns,omitempty"`
	InboundEvents     map[string]InboundEvent           `json:"inbound_events,omitempty"`
	OutboxMessages    map[string]OutboxMessage          `json:"outbox_messages,omitempty"`
	ChatPolls         map[string]ChatPollState          `json:"chat_polls,omitempty"`
	Workspaces        map[string]WorkspaceRecord        `json:"workspaces,omitempty"`
	DashboardViews    map[string]DashboardViewRecord    `json:"dashboard_views,omitempty"`
	DashboardNumbers  map[string]DashboardNumberRecord  `json:"dashboard_numbers,omitempty"`
	TranscriptLedger  map[string]TranscriptLedgerRecord `json:"transcript_ledger,omitempty"`
	ImportCheckpoints map[string]ImportCheckpoint       `json:"import_checkpoints,omitempty"`
	ChatSequences     map[string]ChatSequenceState      `json:"chat_sequences,omitempty"`
	ChatRateLimits    map[string]ChatRateLimitState     `json:"chat_rate_limits,omitempty"`
	ArtifactRecords   map[string]ArtifactRecord         `json:"artifact_records,omitempty"`
	Notifications     map[string]NotificationRecord     `json:"notifications,omitempty"`
}

type ScopeIdentity struct {
	ID            string    `json:"id,omitempty"`
	AccountID     string    `json:"account_id,omitempty"`
	UserPrincipal string    `json:"user_principal,omitempty"`
	OSUser        string    `json:"os_user,omitempty"`
	Profile       string    `json:"profile,omitempty"`
	ConfigPath    string    `json:"config_path,omitempty"`
	CodexHome     string    `json:"codex_home,omitempty"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

type MachineKind string

const (
	MachineKindAuto      MachineKind = "auto"
	MachineKindPrimary   MachineKind = "primary"
	MachineKindEphemeral MachineKind = "ephemeral"
)

type MachineStatus string

const (
	MachineStatusActive   MachineStatus = "active"
	MachineStatusStandby  MachineStatus = "standby"
	MachineStatusYielding MachineStatus = "yielding"
)

type MachineIdentity struct {
	ID            string      `json:"id,omitempty"`
	Label         string      `json:"label,omitempty"`
	Hostname      string      `json:"hostname,omitempty"`
	AccountID     string      `json:"account_id,omitempty"`
	UserPrincipal string      `json:"user_principal,omitempty"`
	Profile       string      `json:"profile,omitempty"`
	ScopeID       string      `json:"scope_id,omitempty"`
	Kind          MachineKind `json:"kind,omitempty"`
	Priority      int         `json:"priority,omitempty"`
	CreatedAt     time.Time   `json:"created_at,omitempty"`
	UpdatedAt     time.Time   `json:"updated_at,omitempty"`
}

type MachineRecord struct {
	ID            string        `json:"id"`
	ScopeID       string        `json:"scope_id,omitempty"`
	Label         string        `json:"label,omitempty"`
	Hostname      string        `json:"hostname,omitempty"`
	OSUser        string        `json:"os_user,omitempty"`
	AccountID     string        `json:"account_id,omitempty"`
	UserPrincipal string        `json:"user_principal,omitempty"`
	Profile       string        `json:"profile,omitempty"`
	Kind          MachineKind   `json:"kind,omitempty"`
	Priority      int           `json:"priority,omitempty"`
	Status        MachineStatus `json:"status,omitempty"`
	LastSeen      time.Time     `json:"last_seen,omitempty"`
	CreatedAt     time.Time     `json:"created_at,omitempty"`
	UpdatedAt     time.Time     `json:"updated_at,omitempty"`
}

type ControlLeaseStatus string

const (
	ControlLeaseStatusActive ControlLeaseStatus = "active"
)

type ControlLease struct {
	ScopeID         string             `json:"scope_id,omitempty"`
	HolderMachineID string             `json:"holder_machine_id,omitempty"`
	HolderKind      MachineKind        `json:"holder_kind,omitempty"`
	Priority        int                `json:"priority,omitempty"`
	Generation      int64              `json:"generation,omitempty"`
	Status          ControlLeaseStatus `json:"status,omitempty"`
	LeaseUntil      time.Time          `json:"lease_until,omitempty"`
	LastHeartbeat   time.Time          `json:"last_heartbeat,omitempty"`
	UpdatedAt       time.Time          `json:"updated_at,omitempty"`
}

type LeaseMode string

const (
	LeaseModeActive  LeaseMode = "active"
	LeaseModeStandby LeaseMode = "standby"
)

type ControlLeaseClaim struct {
	Scope    ScopeIdentity
	Machine  MachineRecord
	Duration time.Duration
	Now      time.Time
}

type ControlLeaseDecision struct {
	Mode   LeaseMode
	Lease  ControlLease
	Holder MachineRecord
	Reason string
}

type ControlChatBinding struct {
	MachineID      string    `json:"machine_id,omitempty"`
	ScopeID        string    `json:"scope_id,omitempty"`
	AccountID      string    `json:"account_id,omitempty"`
	Profile        string    `json:"profile,omitempty"`
	TeamsChatID    string    `json:"teams_chat_id,omitempty"`
	TeamsChatURL   string    `json:"teams_chat_url,omitempty"`
	TeamsChatTopic string    `json:"teams_chat_topic,omitempty"`
	BoundAt        time.Time `json:"bound_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type WorkspaceRecord struct {
	ID          string    `json:"id"`
	Path        string    `json:"path"`
	Label       string    `json:"label,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	Number      int       `json:"number,omitempty"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

type DashboardViewRecord struct {
	ID          string              `json:"id"`
	ChatID      string              `json:"chat_id"`
	Kind        string              `json:"kind,omitempty"`
	WorkspaceID string              `json:"workspace_id,omitempty"`
	Items       []DashboardViewItem `json:"items,omitempty"`
	ExpiresAt   time.Time           `json:"expires_at,omitempty"`
	CreatedAt   time.Time           `json:"created_at,omitempty"`
	UpdatedAt   time.Time           `json:"updated_at,omitempty"`
}

type DashboardNumberRecord struct {
	ID          string    `json:"id"`
	ChatID      string    `json:"chat_id,omitempty"`
	Kind        string    `json:"kind,omitempty"`
	Number      int       `json:"number,omitempty"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	SessionID   string    `json:"session_id,omitempty"`
	Label       string    `json:"label,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

type DashboardViewItem struct {
	Number      int    `json:"number"`
	Kind        string `json:"kind,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	Label       string `json:"label,omitempty"`
}

type TranscriptLedgerRecord struct {
	ID             string    `json:"id"`
	SessionID      string    `json:"session_id"`
	CodexThreadID  string    `json:"codex_thread_id,omitempty"`
	SourcePath     string    `json:"source_path,omitempty"`
	SourceLine     int       `json:"source_line,omitempty"`
	SourceRecordID string    `json:"source_record_id,omitempty"`
	Kind           string    `json:"kind,omitempty"`
	TeamsOriginID  string    `json:"teams_origin_id,omitempty"`
	OutboxID       string    `json:"outbox_id,omitempty"`
	ImportedAt     time.Time `json:"imported_at,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type ImportCheckpoint struct {
	ID             string    `json:"id"`
	SessionID      string    `json:"session_id"`
	SourcePath     string    `json:"source_path,omitempty"`
	LastRecordID   string    `json:"last_record_id,omitempty"`
	LastSourceLine int       `json:"last_source_line,omitempty"`
	Status         string    `json:"status,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type ChatSequenceState struct {
	ChatID    string    `json:"chat_id"`
	Next      int64     `json:"next"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type ChatRateLimitState struct {
	ChatID         string    `json:"chat_id"`
	BlockedUntil   time.Time `json:"blocked_until,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	PoisonOutboxID string    `json:"poison_outbox_id,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type ArtifactRecord struct {
	ID          string    `json:"id"`
	SessionID   string    `json:"session_id,omitempty"`
	TurnID      string    `json:"turn_id,omitempty"`
	Path        string    `json:"path,omitempty"`
	UploadName  string    `json:"upload_name,omitempty"`
	DriveItemID string    `json:"drive_item_id,omitempty"`
	OutboxID    string    `json:"outbox_id,omitempty"`
	Status      string    `json:"status,omitempty"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

type NotificationRecord struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id,omitempty"`
	TurnID    string    `json:"turn_id,omitempty"`
	Kind      string    `json:"kind,omitempty"`
	OutboxID  string    `json:"outbox_id,omitempty"`
	SentAt    time.Time `json:"sent_at,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type ServiceControl struct {
	Paused    bool      `json:"paused,omitempty"`
	Draining  bool      `json:"draining,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type UpgradePhase string

const (
	UpgradePhaseDraining  UpgradePhase = "draining"
	UpgradePhaseReady     UpgradePhase = "ready"
	UpgradePhaseCompleted UpgradePhase = "completed"
	UpgradePhaseAborted   UpgradePhase = "aborted"
)

type UpgradeRequest struct {
	ID              string         `json:"id"`
	Phase           UpgradePhase   `json:"phase"`
	Reason          string         `json:"reason,omitempty"`
	PreviousControl ServiceControl `json:"previous_control,omitempty"`
	DeadlineAt      time.Time      `json:"deadline_at,omitempty"`
	StartedAt       time.Time      `json:"started_at,omitempty"`
	ReadyAt         time.Time      `json:"ready_at,omitempty"`
	CompletedAt     time.Time      `json:"completed_at,omitempty"`
	AbortedAt       time.Time      `json:"aborted_at,omitempty"`
	AbortReason     string         `json:"abort_reason,omitempty"`
	UpdatedAt       time.Time      `json:"updated_at,omitempty"`
}

type ChatPollState struct {
	ChatID                string    `json:"chat_id"`
	Seeded                bool      `json:"seeded,omitempty"`
	PollState             string    `json:"state,omitempty"`
	PreviousPollState     string    `json:"previous_state,omitempty"`
	NextPollAt            time.Time `json:"next_poll_at,omitempty"`
	LastActivityAt        time.Time `json:"last_activity_at,omitempty"`
	BlockedUntil          time.Time `json:"blocked_until,omitempty"`
	FailureCount          int       `json:"failure_count,omitempty"`
	ParkedAt              time.Time `json:"parked_at,omitempty"`
	ParkNoticeSentAt      time.Time `json:"park_notice_sent_at,omitempty"`
	LastModifiedCursor    time.Time `json:"last_modified_cursor,omitempty"`
	ContinuationPath      string    `json:"continuation_path,omitempty"`
	LastSuccessfulPollAt  time.Time `json:"last_successful_poll_at,omitempty"`
	LastError             string    `json:"last_error,omitempty"`
	LastErrorAt           time.Time `json:"last_error_at,omitempty"`
	LastWindowFullAt      time.Time `json:"last_window_full_at,omitempty"`
	LastWindowFullMessage string    `json:"last_window_full_message,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
}

type ChatPollScheduleUpdate struct {
	ChatID            string
	PollState         string
	PreviousPollState string
	NextPollAt        time.Time
	LastActivityAt    time.Time
	BlockedUntil      time.Time
	ClearBlockedUntil bool
	ResetFailures     bool
}

type OwnerMetadata struct {
	PID             int       `json:"pid,omitempty"`
	Hostname        string    `json:"hostname,omitempty"`
	ExecutablePath  string    `json:"executable_path,omitempty"`
	HelperVersion   string    `json:"helper_version,omitempty"`
	ScopeID         string    `json:"scope_id,omitempty"`
	MachineID       string    `json:"machine_id,omitempty"`
	LeaseGeneration int64     `json:"lease_generation,omitempty"`
	StartedAt       time.Time `json:"started_at,omitempty"`
	LastHeartbeat   time.Time `json:"last_heartbeat,omitempty"`
	ActiveSessionID string    `json:"active_session_id,omitempty"`
	ActiveTurnID    string    `json:"active_turn_id,omitempty"`
}

type SessionContext struct {
	ID                string        `json:"id"`
	Status            SessionStatus `json:"status"`
	TeamsChatID       string        `json:"teams_chat_id"`
	TeamsChatURL      string        `json:"teams_chat_url,omitempty"`
	TeamsTopic        string        `json:"teams_topic,omitempty"`
	CodexThreadID     string        `json:"codex_thread_id,omitempty"`
	LatestCodexTurnID string        `json:"latest_codex_turn_id,omitempty"`
	LatestTurnID      string        `json:"latest_turn_id,omitempty"`
	RunnerKind        string        `json:"runner_kind,omitempty"`
	CodexVersion      string        `json:"codex_version,omitempty"`
	Cwd               string        `json:"cwd,omitempty"`
	CodexHome         string        `json:"codex_home,omitempty"`
	Profile           string        `json:"profile,omitempty"`
	Model             string        `json:"model,omitempty"`
	Sandbox           string        `json:"sandbox,omitempty"`
	ProxyMode         string        `json:"proxy_mode,omitempty"`
	YoloMode          string        `json:"yolo_mode,omitempty"`
	CreatedAt         time.Time     `json:"created_at,omitempty"`
	UpdatedAt         time.Time     `json:"updated_at,omitempty"`
}

type InboundEvent struct {
	ID              string        `json:"id"`
	SessionID       string        `json:"session_id,omitempty"`
	TeamsChatID     string        `json:"teams_chat_id"`
	TeamsMessageID  string        `json:"teams_message_id"`
	ScopeID         string        `json:"scope_id,omitempty"`
	MachineID       string        `json:"machine_id,omitempty"`
	LeaseGeneration int64         `json:"lease_generation,omitempty"`
	Text            string        `json:"text,omitempty"`
	TextHash        string        `json:"text_hash,omitempty"`
	Source          string        `json:"source,omitempty"`
	Status          InboundStatus `json:"status"`
	TurnID          string        `json:"turn_id,omitempty"`
	ReceivedAt      time.Time     `json:"received_at,omitempty"`
	CreatedAt       time.Time     `json:"created_at,omitempty"`
	UpdatedAt       time.Time     `json:"updated_at,omitempty"`
}

type Turn struct {
	ID              string     `json:"id"`
	SessionID       string     `json:"session_id"`
	InboundEventID  string     `json:"inbound_event_id,omitempty"`
	ScopeID         string     `json:"scope_id,omitempty"`
	MachineID       string     `json:"machine_id,omitempty"`
	LeaseGeneration int64      `json:"lease_generation,omitempty"`
	Status          TurnStatus `json:"status"`
	CodexThreadID   string     `json:"codex_thread_id,omitempty"`
	CodexTurnID     string     `json:"codex_turn_id,omitempty"`
	FailureMessage  string     `json:"failure_message,omitempty"`
	RecoveryReason  string     `json:"recovery_reason,omitempty"`
	QueuedAt        time.Time  `json:"queued_at,omitempty"`
	StartedAt       time.Time  `json:"started_at,omitempty"`
	CompletedAt     time.Time  `json:"completed_at,omitempty"`
	FailedAt        time.Time  `json:"failed_at,omitempty"`
	InterruptedAt   time.Time  `json:"interrupted_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at,omitempty"`
}

type OutboxMessage struct {
	ID                     string       `json:"id"`
	SessionID              string       `json:"session_id,omitempty"`
	TurnID                 string       `json:"turn_id,omitempty"`
	TeamsChatID            string       `json:"teams_chat_id"`
	ScopeID                string       `json:"scope_id,omitempty"`
	MachineID              string       `json:"machine_id,omitempty"`
	LeaseGeneration        int64        `json:"lease_generation,omitempty"`
	Kind                   string       `json:"kind,omitempty"`
	Body                   string       `json:"body,omitempty"`
	Sequence               int64        `json:"sequence,omitempty"`
	PartIndex              int          `json:"part_index,omitempty"`
	PartCount              int          `json:"part_count,omitempty"`
	SourceTextHash         string       `json:"source_text_hash,omitempty"`
	RenderedHash           string       `json:"rendered_hash,omitempty"`
	RenderedBytes          int          `json:"rendered_bytes,omitempty"`
	AttachmentPath         string       `json:"attachment_path,omitempty"`
	AttachmentName         string       `json:"attachment_name,omitempty"`
	AttachmentUploadName   string       `json:"attachment_upload_name,omitempty"`
	AttachmentContentType  string       `json:"attachment_content_type,omitempty"`
	AttachmentUploadFolder string       `json:"attachment_upload_folder,omitempty"`
	AttachmentSize         int64        `json:"attachment_size,omitempty"`
	AttachmentHash         string       `json:"attachment_hash,omitempty"`
	DriveItemID            string       `json:"drive_item_id,omitempty"`
	DriveItemName          string       `json:"drive_item_name,omitempty"`
	DriveItemWebURL        string       `json:"drive_item_web_url,omitempty"`
	DriveItemWebDav        string       `json:"drive_item_web_dav,omitempty"`
	AckKind                string       `json:"ack_kind,omitempty"`
	NotificationKind       string       `json:"notification_kind,omitempty"`
	MentionOwner           bool         `json:"mention_owner,omitempty"`
	UpgradeNonBlocking     bool         `json:"upgrade_non_blocking,omitempty"`
	ArtifactIDs            []string     `json:"artifact_ids,omitempty"`
	Status                 OutboxStatus `json:"status"`
	TeamsMessageID         string       `json:"teams_message_id,omitempty"`
	CreatedAt              time.Time    `json:"created_at,omitempty"`
	UpdatedAt              time.Time    `json:"updated_at,omitempty"`
	SentAt                 time.Time    `json:"sent_at,omitempty"`
	LastSendAttempt        time.Time    `json:"last_send_attempt,omitempty"`
	LastSendError          string       `json:"last_send_error,omitempty"`
}

type RecoveryReport struct {
	InterruptedTurnIDs []string
}

var ErrOwnerLive = errors.New("Teams owner is active")
var ErrUnsupportedSchemaVersion = errors.New("unsupported Teams state schema version")
var ErrControlLeaseNotHeld = errors.New("Teams control lease is not held by this machine")

type UnsupportedSchemaVersionError struct {
	Version int
}

func (e *UnsupportedSchemaVersionError) Error() string {
	return fmt.Sprintf("%v %d", ErrUnsupportedSchemaVersion, e.Version)
}

func (e *UnsupportedSchemaVersionError) Is(target error) bool {
	return target == ErrUnsupportedSchemaVersion
}

type OwnerConflictError struct {
	Existing   OwnerMetadata
	Now        time.Time
	StaleAfter time.Duration
}

func (e *OwnerConflictError) Error() string {
	age := time.Duration(0)
	if !e.Existing.LastHeartbeat.IsZero() && !e.Now.IsZero() {
		age = e.Now.Sub(e.Existing.LastHeartbeat)
	}
	return fmt.Sprintf(
		"%v: pid=%d host=%q executable=%q helper_version=%q started_at=%s last_heartbeat=%s heartbeat_age=%s stale_after=%s active_session_id=%q active_turn_id=%q",
		ErrOwnerLive,
		e.Existing.PID,
		e.Existing.Hostname,
		e.Existing.ExecutablePath,
		e.Existing.HelperVersion,
		e.Existing.StartedAt.Format(time.RFC3339Nano),
		e.Existing.LastHeartbeat.Format(time.RFC3339Nano),
		age,
		e.StaleAfter,
		e.Existing.ActiveSessionID,
		e.Existing.ActiveTurnID,
	)
}

func (e *OwnerConflictError) Is(target error) bool {
	return target == ErrOwnerLive
}

type Store struct {
	path string
	mu   sync.Mutex
	lock *flock.Flock
}

func DefaultPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams", "state.json"), nil
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return nil, err
		}
	}
	return &Store{
		path: path,
		lock: flock.New(path + ".lock"),
	}, nil
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Load(ctx context.Context) (State, error) {
	var state State
	err := s.withStateLock(ctx, func() error {
		var err error
		state, err = s.loadUnlocked()
		return err
	})
	return state, err
}

func (s *Store) Update(ctx context.Context, fn func(*State) error) error {
	return s.withStateLock(ctx, func() error {
		state, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		if err := fn(&state); err != nil {
			return err
		}
		state.ensure(time.Now())
		return s.saveUnlocked(state)
	})
}

func (s *Store) SetPaused(ctx context.Context, paused bool, reason string) (ServiceControl, error) {
	var out ServiceControl
	err := s.Update(ctx, func(state *State) error {
		next := state.ServiceControl
		reason = strings.TrimSpace(reason)
		desiredReason := next.Reason
		switch {
		case paused:
			desiredReason = reason
		case !next.Draining:
			desiredReason = ""
		case reason != "":
			desiredReason = reason
		}
		if next.Paused == paused && next.Reason == desiredReason {
			out = next
			return nil
		}
		next.Paused = paused
		next.Reason = desiredReason
		next.UpdatedAt = time.Now()
		state.ServiceControl = next
		out = next
		return nil
	})
	return out, err
}

func (s *Store) SetDraining(ctx context.Context, reason string) (ServiceControl, error) {
	var out ServiceControl
	err := s.Update(ctx, func(state *State) error {
		next := state.ServiceControl
		reason = strings.TrimSpace(reason)
		if next.Draining && next.Reason == reason {
			out = next
			return nil
		}
		next.Draining = true
		next.Reason = reason
		next.UpdatedAt = time.Now()
		state.ServiceControl = next
		out = next
		return nil
	})
	return out, err
}

func (s *Store) ClearDrain(ctx context.Context) (ServiceControl, error) {
	var out ServiceControl
	err := s.Update(ctx, func(state *State) error {
		next := state.ServiceControl
		if !next.Draining {
			out = next
			return nil
		}
		next.Draining = false
		if !next.Paused {
			next.Reason = ""
		}
		next.UpdatedAt = time.Now()
		state.ServiceControl = next
		out = next
		return nil
	})
	return out, err
}

func (s *Store) ReadControl(ctx context.Context) (ServiceControl, error) {
	state, err := s.Load(ctx)
	if err != nil {
		return ServiceControl{}, err
	}
	return state.ServiceControl, nil
}

func (s *Store) RecordScope(ctx context.Context, scope ScopeIdentity) (ScopeIdentity, error) {
	scope = normalizeScope(scope)
	if scope.ID == "" {
		return ScopeIdentity{}, fmt.Errorf("scope id is required")
	}
	var out ScopeIdentity
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		current := state.Scope
		if current.ID != "" && current.ID != scope.ID {
			return fmt.Errorf("Teams state belongs to scope %q, not %q", current.ID, scope.ID)
		}
		if scope.CreatedAt.IsZero() {
			scope.CreatedAt = current.CreatedAt
		}
		if scope.CreatedAt.IsZero() {
			scope.CreatedAt = now
		}
		scope.UpdatedAt = now
		state.Scope = scope
		out = scope
		return nil
	})
	return out, err
}

func (s *Store) ClaimControlLease(ctx context.Context, claim ControlLeaseClaim) (ControlLeaseDecision, error) {
	claim.Scope = normalizeScope(claim.Scope)
	claim.Machine = normalizeMachine(claim.Machine)
	if claim.Scope.ID == "" {
		return ControlLeaseDecision{}, fmt.Errorf("scope id is required")
	}
	if claim.Machine.ID == "" {
		return ControlLeaseDecision{}, fmt.Errorf("machine id is required")
	}
	if claim.Machine.ScopeID == "" {
		claim.Machine.ScopeID = claim.Scope.ID
	}
	if claim.Machine.ScopeID != claim.Scope.ID {
		return ControlLeaseDecision{}, fmt.Errorf("machine scope %q does not match state scope %q", claim.Machine.ScopeID, claim.Scope.ID)
	}
	if claim.Duration <= 0 {
		claim.Duration = 30 * time.Second
	}
	now := claim.Now
	if now.IsZero() {
		now = time.Now()
	}
	var out ControlLeaseDecision
	err := s.Update(ctx, func(state *State) error {
		if state.Scope.ID != "" && state.Scope.ID != claim.Scope.ID {
			return fmt.Errorf("Teams state belongs to scope %q, not %q", state.Scope.ID, claim.Scope.ID)
		}
		if state.Scope.ID == "" {
			claim.Scope.CreatedAt = now
		} else if !state.Scope.CreatedAt.IsZero() {
			claim.Scope.CreatedAt = state.Scope.CreatedAt
		}
		claim.Scope.UpdatedAt = now
		state.Scope = claim.Scope

		machine := claim.Machine
		existingMachine := state.Machines[machine.ID]
		if !existingMachine.CreatedAt.IsZero() {
			machine.CreatedAt = existingMachine.CreatedAt
		}
		if machine.CreatedAt.IsZero() {
			machine.CreatedAt = now
		}
		machine.LastSeen = now
		machine.UpdatedAt = now

		existing := state.ControlLease
		existingLive := existing.HolderMachineID != "" && existing.ScopeID == claim.Scope.ID && existing.LeaseUntil.After(now)
		sameHolder := existingLive && existing.HolderMachineID == machine.ID
		protectedActiveTurn := false
		if owner, ok := state.readOwner(); ok {
			protectedActiveTurn = existingLive &&
				machine.Priority > existing.Priority &&
				owner.MachineID == existing.HolderMachineID &&
				owner.LeaseGeneration == existing.Generation &&
				owner.ActiveTurnID != ""
		}
		canClaim := !existingLive || sameHolder || machine.Priority > existing.Priority && !protectedActiveTurn
		if canClaim {
			if sameHolder {
				if existing.Generation <= 0 {
					existing.Generation = 1
				}
			} else {
				if previous := state.Machines[existing.HolderMachineID]; previous.ID != "" {
					previous.Status = MachineStatusStandby
					previous.UpdatedAt = now
					state.Machines[previous.ID] = previous
				}
				existing.Generation++
				if existing.Generation <= 0 {
					existing.Generation = 1
				}
			}
			existing.ScopeID = claim.Scope.ID
			existing.HolderMachineID = machine.ID
			existing.HolderKind = machine.Kind
			existing.Priority = machine.Priority
			existing.Status = ControlLeaseStatusActive
			existing.LeaseUntil = now.Add(claim.Duration)
			existing.LastHeartbeat = now
			existing.UpdatedAt = now
			state.ControlLease = existing
			machine.Status = MachineStatusActive
			state.Machines[machine.ID] = machine
			state.MachineIdentity = machine.toMachineIdentity()
			out = ControlLeaseDecision{Mode: LeaseModeActive, Lease: existing, Holder: machine}
			return nil
		}

		machine.Status = MachineStatusStandby
		state.Machines[machine.ID] = machine
		holder := state.Machines[existing.HolderMachineID]
		if holder.ID == "" {
			holder.ID = existing.HolderMachineID
			holder.Kind = existing.HolderKind
			holder.Priority = existing.Priority
			holder.Status = MachineStatusActive
		}
		out = ControlLeaseDecision{
			Mode:   LeaseModeStandby,
			Lease:  existing,
			Holder: holder,
			Reason: fmt.Sprintf("control lease is held by %s (%s)", existing.HolderMachineID, existing.HolderKind),
		}
		return nil
	})
	return out, err
}

func (s *Store) ValidateControlLease(ctx context.Context, machineID string, generation int64, now time.Time) (ControlLease, error) {
	machineID = strings.TrimSpace(machineID)
	if machineID == "" || generation <= 0 {
		return ControlLease{}, ErrControlLeaseNotHeld
	}
	if now.IsZero() {
		now = time.Now()
	}
	state, err := s.Load(ctx)
	if err != nil {
		return ControlLease{}, err
	}
	lease := state.ControlLease
	if lease.HolderMachineID != machineID || lease.Generation != generation || !lease.LeaseUntil.After(now) {
		return lease, ErrControlLeaseNotHeld
	}
	return lease, nil
}

func (s *Store) ReleaseControlLeaseIfHolder(ctx context.Context, machineID string, generation int64) (bool, error) {
	machineID = strings.TrimSpace(machineID)
	if machineID == "" || generation <= 0 {
		return false, nil
	}
	released := false
	err := s.Update(ctx, func(state *State) error {
		lease := state.ControlLease
		if lease.HolderMachineID != machineID || lease.Generation != generation {
			return nil
		}
		state.ControlLease = ControlLease{}
		if machine := state.Machines[machineID]; machine.ID != "" {
			machine.Status = MachineStatusStandby
			machine.UpdatedAt = time.Now()
			state.Machines[machineID] = machine
		}
		released = true
		return nil
	})
	return released, err
}

func (s *Store) BeginUpgrade(ctx context.Context, reason string, timeout time.Duration) (UpgradeRequest, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = HelperUpgradeReason
	}
	var out UpgradeRequest
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		if activeUpgrade(state.Upgrade) {
			out = *state.Upgrade
			if out.Reason == reason {
				return nil
			}
			return ErrUpgradeInProgress
		}
		previous := state.ServiceControl
		req := UpgradeRequest{
			ID:              upgradeID(reason, now),
			Phase:           UpgradePhaseDraining,
			Reason:          reason,
			PreviousControl: previous,
			StartedAt:       now,
			UpdatedAt:       now,
		}
		if timeout > 0 {
			req.DeadlineAt = now.Add(timeout)
		}
		control := previous
		control.Draining = true
		control.Reason = reason
		control.UpdatedAt = now
		state.ServiceControl = control
		state.Upgrade = &req
		out = req
		return nil
	})
	return out, err
}

func (s *Store) ReadUpgrade(ctx context.Context) (UpgradeRequest, bool, error) {
	state, err := s.Load(ctx)
	if err != nil {
		return UpgradeRequest{}, false, err
	}
	if state.Upgrade == nil || state.Upgrade.ID == "" {
		return UpgradeRequest{}, false, nil
	}
	return *state.Upgrade, true, nil
}

func (s *Store) MarkUpgradeReady(ctx context.Context, upgradeID string) (UpgradeRequest, error) {
	return s.updateUpgrade(ctx, upgradeID, func(req UpgradeRequest, now time.Time) (UpgradeRequest, error) {
		if req.Phase == UpgradePhaseCompleted || req.Phase == UpgradePhaseAborted {
			return req, nil
		}
		req.Phase = UpgradePhaseReady
		if req.ReadyAt.IsZero() {
			req.ReadyAt = now
		}
		return req, nil
	})
}

func (s *Store) CompleteUpgrade(ctx context.Context, upgradeID string) (UpgradeRequest, error) {
	return s.updateUpgrade(ctx, upgradeID, func(req UpgradeRequest, now time.Time) (UpgradeRequest, error) {
		req.Phase = UpgradePhaseCompleted
		if req.CompletedAt.IsZero() {
			req.CompletedAt = now
		}
		return req, nil
	})
}

func (s *Store) AbortUpgrade(ctx context.Context, upgradeID string, reason string) (UpgradeRequest, error) {
	return s.updateUpgrade(ctx, upgradeID, func(req UpgradeRequest, now time.Time) (UpgradeRequest, error) {
		req.Phase = UpgradePhaseAborted
		req.AbortReason = trimDiagnostic(reason, 240)
		if req.AbortedAt.IsZero() {
			req.AbortedAt = now
		}
		return req, nil
	})
}

func CurrentOwner(helperVersion string, activeSessionID string, activeTurnID string, now time.Time) (OwnerMetadata, error) {
	if now.IsZero() {
		now = time.Now()
	}
	hostname, err := os.Hostname()
	if err != nil {
		return OwnerMetadata{}, err
	}
	executable, err := os.Executable()
	if err != nil {
		return OwnerMetadata{}, err
	}
	return OwnerMetadata{
		PID:             os.Getpid(),
		Hostname:        hostname,
		ExecutablePath:  executable,
		HelperVersion:   helperVersion,
		StartedAt:       now,
		LastHeartbeat:   now,
		ActiveSessionID: activeSessionID,
		ActiveTurnID:    activeTurnID,
	}, nil
}

func (s *Store) RecordOwnerHeartbeat(ctx context.Context, owner OwnerMetadata, staleAfter time.Duration, now time.Time) (OwnerMetadata, error) {
	var out OwnerMetadata
	err := s.Update(ctx, func(state *State) error {
		if now.IsZero() {
			now = time.Now()
		}
		next, err := owner.withHeartbeat(now)
		if err != nil {
			return err
		}
		if existing, ok := state.readOwner(); ok {
			if sameOwner(existing, next) {
				if !existing.StartedAt.IsZero() {
					next.StartedAt = existing.StartedAt
				}
			} else if !IsStale(existing, staleAfter, now) {
				return &OwnerConflictError{
					Existing:   existing,
					Now:        now,
					StaleAfter: staleAfter,
				}
			}
		}
		state.writeOwner(next)
		out = next
		return nil
	})
	return out, err
}

func (s *Store) ReadOwner(ctx context.Context) (OwnerMetadata, bool, error) {
	state, err := s.Load(ctx)
	if err != nil {
		return OwnerMetadata{}, false, err
	}
	owner, ok := state.readOwner()
	return owner, ok, nil
}

func (s *Store) ClearOwner(ctx context.Context) error {
	return s.Update(ctx, func(state *State) error {
		state.ServiceOwner = nil
		state.LockOwner = nil
		return nil
	})
}

func (s *Store) ClearOwnerIfSame(ctx context.Context, owner OwnerMetadata) (bool, error) {
	cleared := false
	err := s.Update(ctx, func(state *State) error {
		existing, ok := state.readOwner()
		if !ok || !sameOwner(existing, owner) {
			return nil
		}
		state.ServiceOwner = nil
		state.LockOwner = nil
		cleared = true
		return nil
	})
	return cleared, err
}

func (s *Store) RecoverStaleOwner(ctx context.Context, owner OwnerMetadata, staleAfter time.Duration, now time.Time) (OwnerMetadata, bool, error) {
	var out OwnerMetadata
	recovered := false
	err := s.Update(ctx, func(state *State) error {
		if now.IsZero() {
			now = time.Now()
		}
		next, err := owner.withHeartbeat(now)
		if err != nil {
			return err
		}
		existing, ok := state.readOwner()
		switch {
		case !ok:
			recovered = true
		case sameOwner(existing, next):
		case IsStale(existing, staleAfter, now):
			recovered = true
		default:
			return &OwnerConflictError{
				Existing:   existing,
				Now:        now,
				StaleAfter: staleAfter,
			}
		}
		state.writeOwner(next)
		out = next
		return nil
	})
	return out, recovered, err
}

func IsStale(owner OwnerMetadata, staleAfter time.Duration, now time.Time) bool {
	if staleAfter <= 0 || owner.LastHeartbeat.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return !owner.LastHeartbeat.After(now) && now.Sub(owner.LastHeartbeat) > staleAfter
}

func (s *Store) UpdateSession(ctx context.Context, sessionID string, fn func(*State) error) error {
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("session id is required")
	}
	return s.withSessionLock(ctx, sessionID, func() error {
		return s.Update(ctx, fn)
	})
}

func (s *Store) CreateSession(ctx context.Context, session SessionContext) (SessionContext, bool, error) {
	if strings.TrimSpace(session.ID) == "" {
		return SessionContext{}, false, fmt.Errorf("session id is required")
	}
	var out SessionContext
	created := false
	err := s.Update(ctx, func(state *State) error {
		if existing, ok := state.Sessions[session.ID]; ok {
			out = existing
			return nil
		}
		now := time.Now()
		if session.Status == "" {
			session.Status = SessionStatusActive
		}
		if session.CreatedAt.IsZero() {
			session.CreatedAt = now
		}
		if session.UpdatedAt.IsZero() {
			session.UpdatedAt = session.CreatedAt
		}
		state.Sessions[session.ID] = session
		out = session
		created = true
		return nil
	})
	return out, created, err
}

func (s *Store) PersistInbound(ctx context.Context, event InboundEvent) (InboundEvent, bool, error) {
	if strings.TrimSpace(event.ID) == "" {
		event.ID = inboundID(event.TeamsChatID, event.TeamsMessageID)
	}
	if strings.TrimSpace(event.ID) == "" {
		return InboundEvent{}, false, fmt.Errorf("inbound id or Teams chat/message id is required")
	}
	update := s.Update
	if event.SessionID != "" {
		update = func(ctx context.Context, fn func(*State) error) error {
			return s.UpdateSession(ctx, event.SessionID, fn)
		}
	}
	var out InboundEvent
	created := false
	err := update(ctx, func(state *State) error {
		if existing, ok := state.InboundEvents[event.ID]; ok {
			out = existing
			return nil
		}
		for _, existing := range state.InboundEvents {
			if existing.TeamsChatID == event.TeamsChatID && existing.TeamsMessageID == event.TeamsMessageID && event.TeamsMessageID != "" {
				out = existing
				return nil
			}
		}
		now := time.Now()
		if event.Status == "" {
			event.Status = InboundStatusPersisted
		}
		if event.ReceivedAt.IsZero() {
			event.ReceivedAt = now
		}
		if event.CreatedAt.IsZero() {
			event.CreatedAt = now
		}
		if event.UpdatedAt.IsZero() {
			event.UpdatedAt = event.CreatedAt
		}
		state.InboundEvents[event.ID] = event
		out = event
		created = true
		return nil
	})
	return out, created, err
}

func (s *Store) DeferredInbound(ctx context.Context) ([]InboundEvent, error) {
	state, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	var out []InboundEvent
	for _, event := range state.InboundEvents {
		if event.Status == InboundStatusDeferred {
			out = append(out, event)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TeamsChatID != out[j].TeamsChatID {
			return out[i].TeamsChatID < out[j].TeamsChatID
		}
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].TeamsMessageID < out[j].TeamsMessageID
	})
	return out, nil
}

func HasUpgradeBlockingWork(state State, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	for _, turn := range state.Turns {
		if turn.Status == TurnStatusQueued || turn.Status == TurnStatusRunning {
			return true
		}
	}
	for _, msg := range state.OutboxMessages {
		if OutboxBlocksUpgrade(state, msg, now) {
			return true
		}
	}
	return false
}

func OutboxBlocksUpgrade(state State, msg OutboxMessage, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	if msg.UpgradeNonBlocking {
		return false
	}
	if blocked := state.ChatRateLimits[msg.TeamsChatID]; blocked.BlockedUntil.After(now) {
		return false
	}
	switch msg.Status {
	case OutboxStatusQueued:
		return true
	case OutboxStatusSending:
		return msg.LastSendAttempt.IsZero() || now.Sub(msg.LastSendAttempt) <= outboxSendLease
	default:
		return false
	}
}

func (s *Store) QueueTurn(ctx context.Context, turn Turn) (Turn, bool, error) {
	if strings.TrimSpace(turn.SessionID) == "" {
		return Turn{}, false, fmt.Errorf("session id is required")
	}
	var out Turn
	created := false
	err := s.UpdateSession(ctx, turn.SessionID, func(state *State) error {
		if strings.TrimSpace(turn.ID) == "" {
			turn.ID = turnID(turn.InboundEventID)
		}
		if strings.TrimSpace(turn.ID) == "" {
			return fmt.Errorf("turn id or inbound event id is required")
		}
		if existing, ok := state.Turns[turn.ID]; ok {
			out = existing
			return nil
		}
		if turn.InboundEventID != "" {
			if inbound, ok := state.InboundEvents[turn.InboundEventID]; ok {
				if inbound.TurnID != "" {
					if existing, ok := state.Turns[inbound.TurnID]; ok {
						out = existing
						return nil
					}
				}
			}
		}
		session, ok := state.Sessions[turn.SessionID]
		if !ok {
			return fmt.Errorf("session %q not found", turn.SessionID)
		}
		now := time.Now()
		if turn.Status == "" {
			turn.Status = TurnStatusQueued
		}
		if turn.QueuedAt.IsZero() {
			turn.QueuedAt = now
		}
		if turn.CreatedAt.IsZero() {
			turn.CreatedAt = now
		}
		if turn.UpdatedAt.IsZero() {
			turn.UpdatedAt = turn.CreatedAt
		}
		state.Turns[turn.ID] = turn
		session.LatestTurnID = turn.ID
		session.UpdatedAt = now
		state.Sessions[session.ID] = session
		if turn.InboundEventID != "" {
			if inbound, ok := state.InboundEvents[turn.InboundEventID]; ok {
				inbound.TurnID = turn.ID
				inbound.Status = InboundStatusQueued
				inbound.UpdatedAt = now
				state.InboundEvents[inbound.ID] = inbound
			}
		}
		out = turn
		created = true
		return nil
	})
	return out, created, err
}

func (s *Store) MarkTurnRunning(ctx context.Context, turnID string, codexThreadID string, codexTurnID string) (Turn, error) {
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		turn.Status = TurnStatusRunning
		if turn.StartedAt.IsZero() {
			turn.StartedAt = now
		}
		if codexThreadID != "" {
			turn.CodexThreadID = codexThreadID
		}
		if codexTurnID != "" {
			turn.CodexTurnID = codexTurnID
		}
		updateSessionFromTurn(state, turn, now)
		return turn, nil
	})
}

func (s *Store) ClaimNextQueuedTurn(ctx context.Context, sessionID string) (Turn, bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return Turn{}, false, fmt.Errorf("session id is required")
	}
	var out Turn
	claimed := false
	err := s.UpdateSession(ctx, sessionID, func(state *State) error {
		for _, turn := range state.Turns {
			if turn.SessionID == sessionID && turn.Status == TurnStatusRunning {
				return nil
			}
		}
		var queued []Turn
		for _, turn := range state.Turns {
			if turn.SessionID == sessionID && turn.Status == TurnStatusQueued {
				queued = append(queued, turn)
			}
		}
		if len(queued) == 0 {
			return nil
		}
		sort.Slice(queued, func(i, j int) bool {
			left := queuedTurnSortTime(queued[i])
			right := queuedTurnSortTime(queued[j])
			if !left.Equal(right) {
				return left.Before(right)
			}
			return queued[i].ID < queued[j].ID
		})
		now := time.Now()
		turn := queued[0]
		turn.Status = TurnStatusRunning
		if turn.StartedAt.IsZero() {
			turn.StartedAt = now
		}
		turn.UpdatedAt = now
		state.Turns[turn.ID] = turn
		updateSessionFromTurn(state, turn, now)
		out = turn
		claimed = true
		return nil
	})
	return out, claimed, err
}

func (s *Store) MarkTurnCompleted(ctx context.Context, turnID string, codexThreadID string, codexTurnID string) (Turn, error) {
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		if turn.Status == TurnStatusInterrupted {
			return Turn{}, fmt.Errorf("turn %q is interrupted and cannot be completed", turn.ID)
		}
		turn.Status = TurnStatusCompleted
		turn.CompletedAt = now
		if codexThreadID != "" {
			turn.CodexThreadID = codexThreadID
		}
		if codexTurnID != "" {
			turn.CodexTurnID = codexTurnID
		}
		updateSessionFromTurn(state, turn, now)
		return turn, nil
	})
}

func (s *Store) MarkTurnFailed(ctx context.Context, turnID string, message string) (Turn, error) {
	return s.updateTurn(ctx, turnID, func(_ *State, turn Turn, now time.Time) (Turn, error) {
		if turn.Status == TurnStatusInterrupted {
			return Turn{}, fmt.Errorf("turn %q is interrupted and cannot be failed", turn.ID)
		}
		turn.Status = TurnStatusFailed
		turn.FailedAt = now
		turn.FailureMessage = message
		return turn, nil
	})
}

func (s *Store) MarkTurnInterrupted(ctx context.Context, turnID string, reason string) (Turn, error) {
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		turn.Status = TurnStatusInterrupted
		turn.InterruptedAt = now
		turn.RecoveryReason = reason
		markInboundIgnoredForInterruptedTurn(state, turn, now)
		return turn, nil
	})
}

func (s *Store) QueueOutbox(ctx context.Context, msg OutboxMessage) (OutboxMessage, bool, error) {
	if strings.TrimSpace(msg.ID) == "" {
		msg.ID = outboxID(msg)
	}
	if strings.TrimSpace(msg.ID) == "" {
		return OutboxMessage{}, false, fmt.Errorf("outbox id is required")
	}
	update := s.Update
	if msg.SessionID != "" {
		update = func(ctx context.Context, fn func(*State) error) error {
			return s.UpdateSession(ctx, msg.SessionID, fn)
		}
	}
	var out OutboxMessage
	created := false
	err := update(ctx, func(state *State) error {
		if existing, ok := state.OutboxMessages[msg.ID]; ok {
			out = existing
			return nil
		}
		now := time.Now()
		msg.TeamsChatID = strings.TrimSpace(msg.TeamsChatID)
		if msg.TeamsChatID == "" {
			return fmt.Errorf("Teams chat id is required")
		}
		if msg.Status == "" {
			msg.Status = OutboxStatusQueued
		}
		if msg.Sequence <= 0 {
			msg.Sequence = allocateChatSequence(state, msg.TeamsChatID, now)
		}
		if msg.PartCount <= 0 {
			msg.PartCount = 1
		}
		if msg.PartIndex <= 0 && msg.PartCount == 1 {
			msg.PartIndex = 1
		}
		if msg.RenderedHash == "" {
			msg.RenderedHash = bodyHash(msg.Body)
		}
		if msg.CreatedAt.IsZero() {
			msg.CreatedAt = now
		}
		if msg.UpdatedAt.IsZero() {
			msg.UpdatedAt = msg.CreatedAt
		}
		state.OutboxMessages[msg.ID] = msg
		out = msg
		created = true
		return nil
	})
	return out, created, err
}

func (s *Store) MarkOutboxSendAttempt(ctx context.Context, outboxID string) (OutboxMessage, error) {
	return s.updateOutbox(ctx, outboxID, func(msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		switch msg.Status {
		case OutboxStatusSent:
			return msg, ErrOutboxSendNotClaimed
		case OutboxStatusSending:
			if !msg.LastSendAttempt.IsZero() && now.Sub(msg.LastSendAttempt) <= outboxSendLease {
				return msg, ErrOutboxSendNotClaimed
			}
		}
		msg.Status = OutboxStatusSending
		msg.LastSendAttempt = now
		msg.LastSendError = ""
		return msg, nil
	})
}

func (s *Store) EarlierUnsentOutbox(ctx context.Context, msg OutboxMessage) (OutboxMessage, bool, error) {
	chatID := strings.TrimSpace(msg.TeamsChatID)
	if chatID == "" || msg.Sequence <= 0 {
		return OutboxMessage{}, false, nil
	}
	state, err := s.Load(ctx)
	if err != nil {
		return OutboxMessage{}, false, err
	}
	var earlier OutboxMessage
	found := false
	for _, candidate := range state.OutboxMessages {
		if candidate.ID == msg.ID || candidate.TeamsChatID != chatID || candidate.Sequence <= 0 || candidate.Sequence >= msg.Sequence {
			continue
		}
		if candidate.Status == OutboxStatusSent {
			continue
		}
		if !found || candidate.Sequence < earlier.Sequence || candidate.Sequence == earlier.Sequence && candidate.CreatedAt.Before(earlier.CreatedAt) {
			earlier = candidate
			found = true
		}
	}
	return earlier, found, nil
}

func (s *Store) MarkOutboxSendError(ctx context.Context, outboxID string, message string) (OutboxMessage, error) {
	return s.updateOutbox(ctx, outboxID, func(msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		msg.Status = OutboxStatusQueued
		msg.LastSendError = trimDiagnostic(message, 240)
		msg.LastSendAttempt = now
		return msg, nil
	})
}

func (s *Store) MarkOutboxDriveItem(ctx context.Context, outboxID string, itemID string, name string, webURL string, webDavURL string) (OutboxMessage, error) {
	return s.updateOutbox(ctx, outboxID, func(msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		msg.DriveItemID = strings.TrimSpace(itemID)
		msg.DriveItemName = strings.TrimSpace(name)
		msg.DriveItemWebURL = strings.TrimSpace(webURL)
		msg.DriveItemWebDav = strings.TrimSpace(webDavURL)
		msg.LastSendError = ""
		return msg, nil
	})
}

func (s *Store) MarkOutboxAccepted(ctx context.Context, outboxID string, teamsMessageID string) (OutboxMessage, error) {
	return s.updateOutbox(ctx, outboxID, func(msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		if msg.Status == OutboxStatusSent {
			return msg, nil
		}
		msg.Status = OutboxStatusAccepted
		if teamsMessageID != "" {
			msg.TeamsMessageID = teamsMessageID
		}
		msg.LastSendError = ""
		return msg, nil
	})
}

func (s *Store) MarkOutboxSent(ctx context.Context, outboxID string, teamsMessageID string) (OutboxMessage, error) {
	return s.updateOutbox(ctx, outboxID, func(msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		msg.Status = OutboxStatusSent
		if msg.SentAt.IsZero() {
			msg.SentAt = now
		}
		if teamsMessageID != "" {
			msg.TeamsMessageID = teamsMessageID
		}
		return msg, nil
	})
}

func (s *Store) PendingOutbox(ctx context.Context) ([]OutboxMessage, error) {
	return s.PendingOutboxAt(ctx, time.Now())
}

func (s *Store) HasDeliveredOutboxMessage(ctx context.Context, chatID string, teamsMessageID string) (bool, error) {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return false, nil
	}
	state, err := s.Load(ctx)
	if err != nil {
		return false, err
	}
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID != chatID || msg.TeamsMessageID != teamsMessageID {
			continue
		}
		switch msg.Status {
		case OutboxStatusAccepted, OutboxStatusSent:
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) PendingOutboxAt(ctx context.Context, now time.Time) ([]OutboxMessage, error) {
	if now.IsZero() {
		now = time.Now()
	}
	state, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	var pending []OutboxMessage
	for _, msg := range state.OutboxMessages {
		if blocked := state.ChatRateLimits[msg.TeamsChatID]; blocked.BlockedUntil.After(now) {
			continue
		}
		if msg.Status == OutboxStatusAccepted && msg.TeamsMessageID != "" ||
			msg.Status == OutboxStatusQueued ||
			msg.Status == OutboxStatusSending && (msg.LastSendAttempt.IsZero() || now.Sub(msg.LastSendAttempt) > outboxSendLease) {
			pending = append(pending, msg)
		}
	}
	return pending, nil
}

func (s *Store) ChatRateLimit(ctx context.Context, chatID string) (ChatRateLimitState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatRateLimitState{}, false, fmt.Errorf("chat id is required")
	}
	state, err := s.Load(ctx)
	if err != nil {
		return ChatRateLimitState{}, false, err
	}
	limit, ok := state.ChatRateLimits[chatID]
	return limit, ok, nil
}

func (s *Store) SetChatRateLimit(ctx context.Context, chatID string, blockedUntil time.Time, reason string) (ChatRateLimitState, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatRateLimitState{}, fmt.Errorf("chat id is required")
	}
	var out ChatRateLimitState
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		next := state.ChatRateLimits[chatID]
		next.ChatID = chatID
		next.BlockedUntil = blockedUntil
		next.Reason = trimDiagnostic(reason, 240)
		next.UpdatedAt = now
		state.ChatRateLimits[chatID] = next
		out = next
		return nil
	})
	return out, err
}

func (s *Store) ClearChatRateLimit(ctx context.Context, chatID string) error {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return fmt.Errorf("chat id is required")
	}
	return s.Update(ctx, func(state *State) error {
		delete(state.ChatRateLimits, chatID)
		return nil
	})
}

func (s *Store) ChatPoll(ctx context.Context, chatID string) (ChatPollState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, false, fmt.Errorf("chat id is required")
	}
	state, err := s.Load(ctx)
	if err != nil {
		return ChatPollState{}, false, err
	}
	poll, ok := state.ChatPolls[chatID]
	return poll, ok, nil
}

func (s *Store) RecordChatPollSuccess(ctx context.Context, chatID string, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int) (ChatPollState, error) {
	return s.RecordChatPollSuccessWithContinuation(ctx, chatID, lastModifiedCursor, seeded, windowFull, fetched, "")
}

func (s *Store) RecordChatPollSuccessWithContinuation(ctx context.Context, chatID string, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int, continuationPath string) (ChatPollState, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, fmt.Errorf("chat id is required")
	}
	continuationPath = strings.TrimSpace(continuationPath)
	var out ChatPollState
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		poll := state.ChatPolls[chatID]
		poll.ChatID = chatID
		poll.Seeded = poll.Seeded || seeded
		if lastModifiedCursor.After(poll.LastModifiedCursor) {
			poll.LastModifiedCursor = lastModifiedCursor
		}
		poll.LastSuccessfulPollAt = now
		poll.LastError = ""
		poll.LastErrorAt = time.Time{}
		poll.BlockedUntil = time.Time{}
		poll.FailureCount = 0
		poll.ContinuationPath = continuationPath
		if windowFull {
			poll.LastWindowFullAt = now
			poll.LastWindowFullMessage = fmt.Sprintf("Graph returned a full message window (%d messages); older unprocessed messages may require a larger recovery pass", fetched)
		} else {
			poll.LastWindowFullMessage = ""
		}
		poll.UpdatedAt = now
		state.ChatPolls[chatID] = poll
		out = poll
		return nil
	})
	return out, err
}

func (s *Store) RecordChatPollError(ctx context.Context, chatID string, message string) error {
	return s.RecordChatPollErrorWithBlock(ctx, chatID, message, time.Time{})
}

func (s *Store) RecordChatPollErrorWithBlock(ctx context.Context, chatID string, message string, blockedUntil time.Time) error {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return fmt.Errorf("chat id is required")
	}
	message = trimDiagnostic(message, 240)
	return s.Update(ctx, func(state *State) error {
		now := time.Now()
		poll := state.ChatPolls[chatID]
		poll.ChatID = chatID
		poll.LastError = message
		poll.LastErrorAt = now
		poll.FailureCount++
		if blockedUntil.After(now) {
			if poll.PollState != "" && poll.PollState != "blocked" {
				poll.PreviousPollState = poll.PollState
			}
			poll.PollState = "blocked"
			poll.BlockedUntil = blockedUntil
			poll.NextPollAt = blockedUntil
		}
		poll.UpdatedAt = now
		state.ChatPolls[chatID] = poll
		return nil
	})
}

func (s *Store) UpdateChatPollSchedule(ctx context.Context, update ChatPollScheduleUpdate) (ChatPollState, error) {
	chatID := strings.TrimSpace(update.ChatID)
	if chatID == "" {
		return ChatPollState{}, fmt.Errorf("chat id is required")
	}
	var out ChatPollState
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		poll := state.ChatPolls[chatID]
		poll.ChatID = chatID
		if update.PollState != "" {
			if update.PollState == "blocked" {
				previous := strings.TrimSpace(update.PreviousPollState)
				if previous == "" && poll.PollState != "" && poll.PollState != "blocked" {
					previous = poll.PollState
				}
				poll.PreviousPollState = previous
			} else {
				poll.PreviousPollState = strings.TrimSpace(update.PreviousPollState)
			}
			poll.PollState = strings.TrimSpace(update.PollState)
			if poll.PollState == "parked" && poll.ParkedAt.IsZero() {
				poll.ParkedAt = now
			}
			if poll.PollState != "parked" {
				poll.ParkedAt = time.Time{}
				poll.ParkNoticeSentAt = time.Time{}
			}
		}
		poll.NextPollAt = update.NextPollAt
		if update.LastActivityAt.After(poll.LastActivityAt) {
			poll.LastActivityAt = update.LastActivityAt
		}
		if update.ClearBlockedUntil {
			poll.BlockedUntil = time.Time{}
		} else if !update.BlockedUntil.IsZero() {
			poll.BlockedUntil = update.BlockedUntil
		}
		if update.ResetFailures {
			poll.FailureCount = 0
		}
		poll.UpdatedAt = now
		state.ChatPolls[chatID] = poll
		out = poll
		return nil
	})
	return out, err
}

func (s *Store) MarkChatPollParkNoticeSent(ctx context.Context, chatID string, at time.Time) (ChatPollState, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, fmt.Errorf("chat id is required")
	}
	if at.IsZero() {
		at = time.Now()
	}
	var out ChatPollState
	err := s.Update(ctx, func(state *State) error {
		poll := state.ChatPolls[chatID]
		poll.ChatID = chatID
		poll.ParkNoticeSentAt = at
		poll.UpdatedAt = time.Now()
		state.ChatPolls[chatID] = poll
		out = poll
		return nil
	})
	return out, err
}

func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		for id, turn := range state.Turns {
			if turn.Status != TurnStatusQueued && turn.Status != TurnStatusRunning {
				continue
			}
			turn.Status = TurnStatusInterrupted
			turn.InterruptedAt = now
			turn.RecoveryReason = "ambiguous after restart"
			turn.UpdatedAt = now
			state.Turns[id] = turn
			markInboundIgnoredForInterruptedTurn(state, turn, now)
			report.InterruptedTurnIDs = append(report.InterruptedTurnIDs, id)
		}
		return nil
	})
	return report, err
}

func markInboundIgnoredForInterruptedTurn(state *State, turn Turn, now time.Time) {
	if turn.InboundEventID == "" {
		return
	}
	inbound, ok := state.InboundEvents[turn.InboundEventID]
	if !ok {
		return
	}
	if inbound.Status == InboundStatusQueued || inbound.Status == InboundStatusDeferred {
		inbound.Status = InboundStatusIgnored
		inbound.UpdatedAt = now
		state.InboundEvents[inbound.ID] = inbound
	}
}

func (s *Store) updateTurn(ctx context.Context, turnID string, fn func(*State, Turn, time.Time) (Turn, error)) (Turn, error) {
	if strings.TrimSpace(turnID) == "" {
		return Turn{}, fmt.Errorf("turn id is required")
	}
	state, err := s.Load(ctx)
	if err != nil {
		return Turn{}, err
	}
	turn, ok := state.Turns[turnID]
	if !ok {
		return Turn{}, fmt.Errorf("turn %q not found", turnID)
	}
	var out Turn
	err = s.UpdateSession(ctx, turn.SessionID, func(state *State) error {
		current, ok := state.Turns[turnID]
		if !ok {
			return fmt.Errorf("turn %q not found", turnID)
		}
		now := time.Now()
		next, err := fn(state, current, now)
		if err != nil {
			return err
		}
		next.UpdatedAt = now
		state.Turns[turnID] = next
		out = next
		return nil
	})
	return out, err
}

func (s *Store) updateOutbox(ctx context.Context, outboxID string, fn func(OutboxMessage, time.Time) (OutboxMessage, error)) (OutboxMessage, error) {
	if strings.TrimSpace(outboxID) == "" {
		return OutboxMessage{}, fmt.Errorf("outbox id is required")
	}
	state, err := s.Load(ctx)
	if err != nil {
		return OutboxMessage{}, err
	}
	msg, ok := state.OutboxMessages[outboxID]
	if !ok {
		return OutboxMessage{}, fmt.Errorf("outbox message %q not found", outboxID)
	}
	update := s.Update
	if msg.SessionID != "" {
		update = func(ctx context.Context, fn func(*State) error) error {
			return s.UpdateSession(ctx, msg.SessionID, fn)
		}
	}
	var out OutboxMessage
	err = update(ctx, func(state *State) error {
		current, ok := state.OutboxMessages[outboxID]
		if !ok {
			return fmt.Errorf("outbox message %q not found", outboxID)
		}
		now := time.Now()
		next, err := fn(current, now)
		if err != nil {
			return err
		}
		next.UpdatedAt = now
		state.OutboxMessages[outboxID] = next
		out = next
		return nil
	})
	return out, err
}

func (s *Store) updateUpgrade(ctx context.Context, upgradeID string, fn func(UpgradeRequest, time.Time) (UpgradeRequest, error)) (UpgradeRequest, error) {
	upgradeID = strings.TrimSpace(upgradeID)
	if upgradeID == "" {
		return UpgradeRequest{}, fmt.Errorf("upgrade id is required")
	}
	var out UpgradeRequest
	err := s.Update(ctx, func(state *State) error {
		if state.Upgrade == nil || state.Upgrade.ID != upgradeID {
			return fmt.Errorf("upgrade request %q not found", upgradeID)
		}
		now := time.Now()
		next, err := fn(*state.Upgrade, now)
		if err != nil {
			return err
		}
		next.UpdatedAt = now
		state.Upgrade = &next
		if next.Phase == UpgradePhaseCompleted || next.Phase == UpgradePhaseAborted {
			restoreUpgradeControl(state, next, now)
		}
		out = next
		return nil
	})
	return out, err
}

func (s *Store) loadUnlocked() (State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		state := newState()
		return state, nil
	}
	if err != nil {
		return State{}, err
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, err
	}
	if state.SchemaVersion >= 0 && state.SchemaVersion < SchemaVersion {
		state = migrateStateToCurrent(state)
		return state, nil
	}
	if state.SchemaVersion != SchemaVersion {
		return State{}, &UnsupportedSchemaVersionError{Version: state.SchemaVersion}
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) saveUnlocked(state State) error {
	state.ensure(time.Now())
	pruneSentOutboxMessages(&state)
	pruneTranscriptLedgerRecords(&state)
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(s.path, data, fileMode)
}

func pruneSentOutboxMessages(state *State) {
	if state == nil || len(state.OutboxMessages) <= maxRetainedSentOutboxMessages {
		return
	}
	type candidate struct {
		id  string
		msg OutboxMessage
	}
	var sent []candidate
	for id, msg := range state.OutboxMessages {
		if msg.Status != OutboxStatusSent {
			continue
		}
		sent = append(sent, candidate{id: id, msg: msg})
	}
	if len(state.OutboxMessages)-len(sent) >= maxRetainedSentOutboxMessages || len(sent) == 0 {
		return
	}
	sort.SliceStable(sent, func(i, j int) bool {
		left := outboxRetentionTime(sent[i].msg)
		right := outboxRetentionTime(sent[j].msg)
		if !left.Equal(right) {
			return left.After(right)
		}
		return sent[i].id > sent[j].id
	})
	keepSent := maxRetainedSentOutboxMessages - (len(state.OutboxMessages) - len(sent))
	if keepSent < 0 {
		keepSent = 0
	}
	for _, item := range sent[keepSent:] {
		delete(state.OutboxMessages, item.id)
	}
}

func outboxRetentionTime(msg OutboxMessage) time.Time {
	for _, value := range []time.Time{msg.SentAt, msg.UpdatedAt, msg.CreatedAt, msg.LastSendAttempt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func pruneTranscriptLedgerRecords(state *State) {
	if state == nil || len(state.TranscriptLedger) <= maxRetainedTranscriptLedgerRecords {
		return
	}
	type candidate struct {
		id     string
		record TranscriptLedgerRecord
	}
	records := make([]candidate, 0, len(state.TranscriptLedger))
	for id, record := range state.TranscriptLedger {
		records = append(records, candidate{id: id, record: record})
	}
	sort.SliceStable(records, func(i, j int) bool {
		left := transcriptLedgerRetentionTime(records[i].record)
		right := transcriptLedgerRetentionTime(records[j].record)
		if !left.Equal(right) {
			return left.After(right)
		}
		return records[i].id > records[j].id
	})
	for _, item := range records[maxRetainedTranscriptLedgerRecords:] {
		delete(state.TranscriptLedger, item.id)
	}
}

func transcriptLedgerRetentionTime(record TranscriptLedgerRecord) time.Time {
	for _, value := range []time.Time{record.UpdatedAt, record.ImportedAt, record.CreatedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func (s *Store) withStateLock(ctx context.Context, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ensurePrivateDir(filepath.Dir(s.path)); err != nil {
		return err
	}
	ok, err := s.lock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return err
	}
	if !ok {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("Teams state lock was not acquired")
	}
	defer func() {
		_ = s.lock.Unlock()
	}()
	_ = os.Chmod(s.path+".lock", fileMode)
	return fn()
}

func (s *Store) withSessionLock(ctx context.Context, sessionID string, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lockDir := filepath.Join(filepath.Dir(s.path), "session-locks")
	if err := ensurePrivateDir(lockDir); err != nil {
		return err
	}
	lock := flock.New(filepath.Join(lockDir, safeLockName(sessionID)+".lock"))
	ok, err := lock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return err
	}
	if !ok {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("Teams session lock %q was not acquired", sessionID)
	}
	defer func() {
		_ = lock.Unlock()
	}()
	_ = os.Chmod(lock.Path(), fileMode)
	return fn()
}

func newState() State {
	now := time.Now()
	state := State{
		SchemaVersion:     SchemaVersion,
		CreatedAt:         now,
		UpdatedAt:         now,
		Machines:          make(map[string]MachineRecord),
		Sessions:          make(map[string]SessionContext),
		Turns:             make(map[string]Turn),
		InboundEvents:     make(map[string]InboundEvent),
		OutboxMessages:    make(map[string]OutboxMessage),
		ChatPolls:         make(map[string]ChatPollState),
		Workspaces:        make(map[string]WorkspaceRecord),
		DashboardViews:    make(map[string]DashboardViewRecord),
		DashboardNumbers:  make(map[string]DashboardNumberRecord),
		TranscriptLedger:  make(map[string]TranscriptLedgerRecord),
		ImportCheckpoints: make(map[string]ImportCheckpoint),
		ChatSequences:     make(map[string]ChatSequenceState),
		ChatRateLimits:    make(map[string]ChatRateLimitState),
		ArtifactRecords:   make(map[string]ArtifactRecord),
		Notifications:     make(map[string]NotificationRecord),
	}
	return state
}

func (s *State) ensure(now time.Time) {
	if s.SchemaVersion == 0 {
		s.SchemaVersion = SchemaVersion
	}
	if s.CreatedAt.IsZero() {
		if now.IsZero() {
			now = time.Now()
		}
		s.CreatedAt = now
	}
	if !now.IsZero() {
		s.UpdatedAt = now
	}
	if s.Sessions == nil {
		s.Sessions = make(map[string]SessionContext)
	}
	if s.Machines == nil {
		s.Machines = make(map[string]MachineRecord)
	}
	if s.Turns == nil {
		s.Turns = make(map[string]Turn)
	}
	if s.InboundEvents == nil {
		s.InboundEvents = make(map[string]InboundEvent)
	}
	if s.OutboxMessages == nil {
		s.OutboxMessages = make(map[string]OutboxMessage)
	}
	if s.ChatPolls == nil {
		s.ChatPolls = make(map[string]ChatPollState)
	}
	if s.Workspaces == nil {
		s.Workspaces = make(map[string]WorkspaceRecord)
	}
	if s.DashboardViews == nil {
		s.DashboardViews = make(map[string]DashboardViewRecord)
	}
	if s.DashboardNumbers == nil {
		s.DashboardNumbers = make(map[string]DashboardNumberRecord)
	}
	if s.TranscriptLedger == nil {
		s.TranscriptLedger = make(map[string]TranscriptLedgerRecord)
	}
	if s.ImportCheckpoints == nil {
		s.ImportCheckpoints = make(map[string]ImportCheckpoint)
	}
	if s.ChatSequences == nil {
		s.ChatSequences = make(map[string]ChatSequenceState)
	}
	if s.ChatRateLimits == nil {
		s.ChatRateLimits = make(map[string]ChatRateLimitState)
	}
	if s.ArtifactRecords == nil {
		s.ArtifactRecords = make(map[string]ArtifactRecord)
	}
	if s.Notifications == nil {
		s.Notifications = make(map[string]NotificationRecord)
	}
}

func migrateStateToCurrent(state State) State {
	state.SchemaVersion = SchemaVersion
	state.ensure(time.Time{})
	if state.MachineIdentity.ID != "" && state.Machines[state.MachineIdentity.ID].ID == "" {
		machine := MachineRecord{
			ID:            state.MachineIdentity.ID,
			ScopeID:       state.MachineIdentity.ScopeID,
			Label:         state.MachineIdentity.Label,
			Hostname:      state.MachineIdentity.Hostname,
			AccountID:     state.MachineIdentity.AccountID,
			UserPrincipal: state.MachineIdentity.UserPrincipal,
			Profile:       state.MachineIdentity.Profile,
			Kind:          state.MachineIdentity.Kind,
			Priority:      state.MachineIdentity.Priority,
			Status:        MachineStatusStandby,
			CreatedAt:     state.MachineIdentity.CreatedAt,
			UpdatedAt:     state.MachineIdentity.UpdatedAt,
		}
		state.Machines[machine.ID] = normalizeMachine(machine)
	}
	for id, msg := range state.OutboxMessages {
		if msg.TeamsChatID == "" {
			state.OutboxMessages[id] = msg
			continue
		}
		if msg.Sequence <= 0 {
			msg.Sequence = allocateChatSequence(&state, msg.TeamsChatID, time.Time{})
		}
		if msg.PartCount <= 0 {
			msg.PartCount = 1
		}
		if msg.PartIndex <= 0 && msg.PartCount == 1 {
			msg.PartIndex = 1
		}
		if msg.RenderedHash == "" {
			msg.RenderedHash = bodyHash(msg.Body)
		}
		state.OutboxMessages[id] = msg
	}
	return state
}

func activeUpgrade(req *UpgradeRequest) bool {
	if req == nil || req.ID == "" {
		return false
	}
	return req.Phase != UpgradePhaseCompleted && req.Phase != UpgradePhaseAborted
}

func restoreUpgradeControl(state *State, req UpgradeRequest, now time.Time) {
	current := state.ServiceControl
	if current.Draining && current.Reason == req.Reason {
		restored := req.PreviousControl
		restored.UpdatedAt = now
		state.ServiceControl = restored
	}
}

func upgradeID(reason string, now time.Time) string {
	sum := sha256.Sum256([]byte(reason + "\x00" + now.UTC().Format(time.RFC3339Nano)))
	return "upgrade:" + hex.EncodeToString(sum[:])[:16]
}

func (s *State) readOwner() (OwnerMetadata, bool) {
	if s.ServiceOwner != nil {
		return *s.ServiceOwner, true
	}
	if s.LockOwner != nil {
		return *s.LockOwner, true
	}
	return OwnerMetadata{}, false
}

func (s *State) writeOwner(owner OwnerMetadata) {
	serviceOwner := owner
	lockOwner := owner
	s.ServiceOwner = &serviceOwner
	s.LockOwner = &lockOwner
}

func normalizeScope(scope ScopeIdentity) ScopeIdentity {
	scope.ID = strings.TrimSpace(scope.ID)
	scope.AccountID = strings.TrimSpace(scope.AccountID)
	scope.UserPrincipal = strings.TrimSpace(scope.UserPrincipal)
	scope.OSUser = strings.TrimSpace(scope.OSUser)
	scope.Profile = strings.TrimSpace(scope.Profile)
	scope.ConfigPath = strings.TrimSpace(scope.ConfigPath)
	scope.CodexHome = strings.TrimSpace(scope.CodexHome)
	return scope
}

func normalizeMachine(machine MachineRecord) MachineRecord {
	machine.ID = strings.TrimSpace(machine.ID)
	machine.ScopeID = strings.TrimSpace(machine.ScopeID)
	machine.Label = strings.TrimSpace(machine.Label)
	machine.Hostname = strings.TrimSpace(machine.Hostname)
	machine.OSUser = strings.TrimSpace(machine.OSUser)
	machine.AccountID = strings.TrimSpace(machine.AccountID)
	machine.UserPrincipal = strings.TrimSpace(machine.UserPrincipal)
	machine.Profile = strings.TrimSpace(machine.Profile)
	switch machine.Kind {
	case MachineKindPrimary, MachineKindEphemeral:
	default:
		machine.Kind = MachineKindAuto
	}
	if machine.Priority <= 0 {
		machine.Priority = DefaultMachinePriority(machine.Kind)
	}
	return machine
}

func DefaultMachinePriority(kind MachineKind) int {
	switch kind {
	case MachineKindPrimary:
		return 100
	case MachineKindEphemeral:
		return 10
	default:
		return 50
	}
}

func (m MachineRecord) toMachineIdentity() MachineIdentity {
	return MachineIdentity{
		ID:            m.ID,
		Label:         m.Label,
		Hostname:      m.Hostname,
		AccountID:     m.AccountID,
		UserPrincipal: m.UserPrincipal,
		Profile:       m.Profile,
		ScopeID:       m.ScopeID,
		Kind:          m.Kind,
		Priority:      m.Priority,
		CreatedAt:     m.CreatedAt,
		UpdatedAt:     m.UpdatedAt,
	}
}

func (owner OwnerMetadata) withHeartbeat(now time.Time) (OwnerMetadata, error) {
	if owner.PID <= 0 {
		return OwnerMetadata{}, fmt.Errorf("owner pid is required")
	}
	if strings.TrimSpace(owner.Hostname) == "" {
		return OwnerMetadata{}, fmt.Errorf("owner hostname is required")
	}
	if strings.TrimSpace(owner.ExecutablePath) == "" {
		return OwnerMetadata{}, fmt.Errorf("owner executable path is required")
	}
	if owner.StartedAt.IsZero() {
		owner.StartedAt = now
	}
	owner.LastHeartbeat = now
	return owner, nil
}

func sameOwner(a OwnerMetadata, b OwnerMetadata) bool {
	if a.PID != b.PID || a.Hostname != b.Hostname || a.ExecutablePath != b.ExecutablePath {
		return false
	}
	if !a.StartedAt.IsZero() && !b.StartedAt.IsZero() {
		return a.StartedAt.Equal(b.StartedAt)
	}
	return true
}

func updateSessionFromTurn(state *State, turn Turn, now time.Time) {
	session, ok := state.Sessions[turn.SessionID]
	if !ok {
		return
	}
	if turn.CodexThreadID != "" {
		session.CodexThreadID = turn.CodexThreadID
	}
	if turn.CodexTurnID != "" {
		session.LatestCodexTurnID = turn.CodexTurnID
	}
	session.LatestTurnID = turn.ID
	session.UpdatedAt = now
	state.Sessions[session.ID] = session
}

func queuedTurnSortTime(turn Turn) time.Time {
	for _, value := range []time.Time{turn.QueuedAt, turn.CreatedAt, turn.UpdatedAt, turn.StartedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func allocateChatSequence(state *State, chatID string, now time.Time) int64 {
	state.ensure(time.Time{})
	seq := state.ChatSequences[chatID]
	if seq.ChatID == "" {
		seq.ChatID = chatID
	}
	if seq.Next <= 0 {
		seq.Next = 1
	}
	value := seq.Next
	seq.Next++
	if !now.IsZero() {
		seq.UpdatedAt = now
	}
	state.ChatSequences[chatID] = seq
	return value
}

func bodyHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func trimDiagnostic(message string, limit int) string {
	message = strings.TrimSpace(message)
	if limit > 0 && len(message) > limit {
		return message[:limit]
	}
	return message
}

func inboundID(chatID string, messageID string) string {
	if chatID == "" || messageID == "" {
		return ""
	}
	return "inbound:" + chatID + ":" + messageID
}

func turnID(inboundEventID string) string {
	if inboundEventID == "" {
		return ""
	}
	return "turn:" + inboundEventID
}

func outboxID(msg OutboxMessage) string {
	if msg.TurnID == "" || msg.Kind == "" {
		return ""
	}
	return "outbox:" + msg.TurnID + ":" + msg.Kind
}

func safeLockName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "empty"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, dirMode); err != nil {
		return err
	}
	return os.Chmod(path, dirMode)
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := ensurePrivateDir(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return os.Chmod(path, perm)
}
