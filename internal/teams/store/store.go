package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

const (
	// SchemaVersion 7 is an intentional upgrade-only boundary.  The
	// unresolved-execution anchor and terminal outbox fences are safety data;
	// helpers built against schema 6 do not know how to preserve them.  A
	// schema-7 store must therefore never be written by an older helper.
	SchemaVersion = 7

	dirMode  os.FileMode = 0o700
	fileMode os.FileMode = 0o600

	maxRetainedSentOutboxMessages            = 512
	maxRetainedTranscriptLedgerRecords       = 1024
	maxRetainedTranscriptDeliveries          = 65536
	maxRetainedMessageProvenance             = 8192
	maxRetainedHelperDeliveries              = 32768
	maxStatePointerSize                      = 4096
	sourceCheckpointFingerprintBytes   int64 = 8 * 1024
)

type SessionStatus string

const (
	SessionStatusActive          SessionStatus = "active"
	SessionStatusArchived        SessionStatus = "archived"
	SessionStatusClosed          SessionStatus = "closed"
	SessionStatusQuarantined     SessionStatus = "quarantined"
	SessionStatusStaging         SessionStatus = "staging"
	SessionStatusAwaitingHistory SessionStatus = "awaiting_history"
)

type TurnStatus string

const (
	TurnStatusQueued      TurnStatus = "queued"
	TurnStatusRunning     TurnStatus = "running"
	TurnStatusCompleted   TurnStatus = "completed"
	TurnStatusFailed      TurnStatus = "failed"
	TurnStatusInterrupted TurnStatus = "interrupted"
)

// ErrUnresolvedExecution is returned when a durable turn transition would
// start or re-start work in a session whose previous Codex execution has not
// been proven to have ended.  Callers should leave the turn queued and wait
// for the ownership fence to be resolved; this is deliberately distinct from
// ordinary turn/storage failures so it cannot be surfaced as a user answer.
var ErrUnresolvedExecution = errors.New("Codex execution ownership is unresolved")

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
	OutboxStatusSkipped  OutboxStatus = "skipped"
)

type TranscriptDeliveryStatus string

const (
	TranscriptDeliveryStatusQueued   TranscriptDeliveryStatus = "queued"
	TranscriptDeliveryStatusAccepted TranscriptDeliveryStatus = "accepted"
	TranscriptDeliveryStatusSent     TranscriptDeliveryStatus = "sent"
	TranscriptDeliveryStatusSkipped  TranscriptDeliveryStatus = "skipped"
)

type HelperDeliveryStatus string

const (
	HelperDeliveryStatusQueued   HelperDeliveryStatus = "queued"
	HelperDeliveryStatusSending  HelperDeliveryStatus = "sending"
	HelperDeliveryStatusAccepted HelperDeliveryStatus = "accepted"
	HelperDeliveryStatusSent     HelperDeliveryStatus = "sent"
	HelperDeliveryStatusFailed   HelperDeliveryStatus = "failed"
	HelperDeliveryStatusSkipped  HelperDeliveryStatus = "skipped"
)

const (
	MessageOriginHelperOutbox = "helper_outbox"
	MessageOriginUserInbound  = "user_inbound"
)

var ErrOutboxSendNotClaimed = errors.New("outbox send not claimed")

// ErrOutboxNotFound lets migration/replay callers distinguish a pruned
// durable outbox row from a store read failure. Missing rows are safe to skip
// during idempotent legacy-ledger reconciliation; all other errors must still
// abort the migration.
var ErrOutboxNotFound = errors.New("outbox message not found")
var ErrUpgradeInProgress = errors.New("Teams upgrade already in progress")

// ErrStaleExecutionCallback is returned when a recovery callback carries an
// execution identity that no longer belongs to the durable Turn.  The
// callback must not mutate the newer owner or create an anchor for it.
var ErrStaleExecutionCallback = errors.New("stale Codex execution callback")

// ErrCompletionOwnerLost is returned when a completion callback loses the
// durable terminal-owner CAS to another callback.  It is intentionally
// distinct from ErrUnresolvedExecution: the former is an idempotent loser
// path and must not be retried or turned into another user-visible failure.
var ErrCompletionOwnerLost = errors.New("completion lost terminal owner")

// ErrTerminalOutboxConflict means that a deterministic final outbox ID is
// already occupied by a different rendered message.  The transaction must
// roll back instead of replacing a message that may already have been sent.
var ErrTerminalOutboxConflict = errors.New("terminal outbox identity conflict")

// ErrSessionStateProvenanceMismatch is returned when a scoped state lookup
// finds a checkpoint owned by a different durable Teams session. Such state
// must fail closed instead of being treated as a missing checkpoint.
var ErrSessionStateProvenanceMismatch = errors.New("session state provenance mismatch")

// ErrHistoryWatchCheckpointConflict means that a history watcher tried to
// publish a checkpoint based on a stale read.  A watcher must leave the newer
// cursor intact and retry from a fresh snapshot on the next poll; treating
// this as a normal update would let a slower scan move the cursor backwards.
var ErrHistoryWatchCheckpointConflict = errors.New("history-watch checkpoint changed")

var errStoreNoChange = errors.New("teams store no change")
var loadUnlockedTestHook func()

var (
	currentOwnerExecutable = helperpath.RawExecutable
	currentOwnerArgv0      = func() string {
		if len(os.Args) == 0 {
			return ""
		}
		return os.Args[0]
	}
)

const outboxSendLease = 2 * time.Minute
const chatPollSuccessHeartbeatWriteInterval = 30 * time.Second

const (
	HelperUpgradeReason  = "codex-proxy upgrade"
	HelperReloadReason   = "codex-proxy reload"
	CodexUpgradeReason   = "codex cli upgrade"
	chatPollStateHot     = "hot"
	chatPollStateRunning = "running"
	chatPollStateWarm    = "warm"
	chatPollStateCool    = "cool"
	chatPollStateCold    = "cold"
	chatPollStateBlocked = "blocked"
	chatPollStateCatchup = "catchup"
	chatPollStateParked  = "parked"

	importCheckpointStatusImporting = "importing"
	importCheckpointStatusComplete  = "complete"
	importCheckpointStatusBlocked   = "blocked"
)

type State struct {
	SchemaVersion          int                                 `json:"schema_version"`
	CreatedAt              time.Time                           `json:"created_at,omitempty"`
	UpdatedAt              time.Time                           `json:"updated_at,omitempty"`
	Scope                  ScopeIdentity                       `json:"scope,omitempty"`
	MachineIdentity        MachineIdentity                     `json:"machine_identity,omitempty"`
	Machines               map[string]MachineRecord            `json:"machines,omitempty"`
	ControlLease           ControlLease                        `json:"control_lease,omitempty"`
	ControlChat            ControlChatBinding                  `json:"control_chat,omitempty"`
	ServiceOwner           *OwnerMetadata                      `json:"service_owner,omitempty"`
	LockOwner              *OwnerMetadata                      `json:"lock_owner,omitempty"`
	ServiceControl         ServiceControl                      `json:"service_control,omitempty"`
	Upgrade                *UpgradeRequest                     `json:"upgrade,omitempty"`
	Sessions               map[string]SessionContext           `json:"sessions,omitempty"`
	Turns                  map[string]Turn                     `json:"turns,omitempty"`
	InboundEvents          map[string]InboundEvent             `json:"inbound_events,omitempty"`
	OutboxMessages         map[string]OutboxMessage            `json:"outbox_messages,omitempty"`
	MessageProvenance      map[string]MessageProvenanceRecord  `json:"message_provenance,omitempty"`
	ChatPolls              map[string]ChatPollState            `json:"chat_polls,omitempty"`
	Workspaces             map[string]WorkspaceRecord          `json:"workspaces,omitempty"`
	DashboardViews         map[string]DashboardViewRecord      `json:"dashboard_views,omitempty"`
	DashboardNumbers       map[string]DashboardNumberRecord    `json:"dashboard_numbers,omitempty"`
	TranscriptLedger       map[string]TranscriptLedgerRecord   `json:"transcript_ledger,omitempty"`
	TranscriptDeliveries   map[string]TranscriptDeliveryRecord `json:"transcript_deliveries,omitempty"`
	HelperDeliveries       map[string]HelperDeliveryRecord     `json:"helper_deliveries,omitempty"`
	ImportCheckpoints      map[string]ImportCheckpoint         `json:"import_checkpoints,omitempty"`
	HistoryWatch           map[string]HistoryWatchCheckpoint   `json:"history_watch,omitempty"`
	HistoryWatchReady      time.Time                           `json:"history_watch_ready,omitempty"`
	ChatSequences          map[string]ChatSequenceState        `json:"chat_sequences,omitempty"`
	ChatRateLimits         map[string]ChatRateLimitState       `json:"chat_rate_limits,omitempty"`
	ArtifactRecords        map[string]ArtifactRecord           `json:"artifact_records,omitempty"`
	Notifications          map[string]NotificationRecord       `json:"notifications,omitempty"`
	ForkOperations         map[string]ForkOperation            `json:"fork_operations,omitempty"`
	ForkHistoryItems       map[string]ForkHistoryItem          `json:"fork_history_items,omitempty"`
	ModelProfileKeyIntakes map[string]ModelProfileKeyIntake    `json:"model_profile_key_intakes,omitempty"`
	SkillPushReviews       map[string]SkillPushReview          `json:"skill_push_reviews,omitempty"`
	Workflow               WorkflowNotificationConfig          `json:"workflow,omitempty"`
	AutoUpdate             AutoUpdateState                     `json:"auto_update,omitempty"`
	// legacyUnresolvedSessions is populated only while a SQLite transaction is
	// assembling the minimal callback state. It is intentionally not persisted.
	legacyUnresolvedSessions map[string]bool
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
	Owner    OwnerMetadata
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
	UserTitle      string    `json:"user_title,omitempty"`
	TitleSource    string    `json:"title_source,omitempty"`
	BoundAt        time.Time `json:"bound_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type ControlChatBindingUpdate struct {
	ScopeID              string
	AccountID            string
	UserPrincipal        string
	Profile              string
	MachineID            string
	MachineLabel         string
	MachineHostname      string
	MachineKind          MachineKind
	MachinePriority      int
	TeamsChatID          string
	TeamsChatURL         string
	TeamsChatTopic       string
	UserTitle            string
	TitleSource          string
	UpdateTitleIfPresent bool
}

type SkillPushReview struct {
	ID          string                  `json:"id"`
	TeamsChatID string                  `json:"teams_chat_id"`
	Name        string                  `json:"name,omitempty"`
	Direct      bool                    `json:"direct,omitempty"`
	CreatedAt   time.Time               `json:"created_at,omitempty"`
	ExpiresAt   time.Time               `json:"expires_at,omitempty"`
	Sources     []SkillPushReviewSource `json:"sources,omitempty"`
}

type SkillPushReviewSource struct {
	SourceID      string                  `json:"source_id"`
	SourceName    string                  `json:"source_name"`
	RemoteURL     string                  `json:"remote_url"`
	Ref           string                  `json:"ref,omitempty"`
	BaseCommit    string                  `json:"base_commit"`
	ReviewRefSpec string                  `json:"review_refspec"`
	Changes       []SkillPushReviewChange `json:"changes,omitempty"`
}

type SkillPushReviewChange struct {
	Kind       string `json:"kind"`
	RelPath    string `json:"rel_path"`
	SourcePath string `json:"source_path"`
	Commit     string `json:"commit"`
	OldSHA256  string `json:"old_sha256,omitempty"`
	NewSHA256  string `json:"new_sha256,omitempty"`
	OldMode    uint32 `json:"old_mode,omitempty"`
	NewMode    uint32 `json:"new_mode,omitempty"`
	Size       int64  `json:"size,omitempty"`
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

type TranscriptDeliveryRecord struct {
	ID            string `json:"id"`
	SessionID     string `json:"session_id"`
	CodexThreadID string `json:"codex_thread_id,omitempty"`
	// CodexTurnID is populated only from explicit transcript provenance.  It is
	// intentionally separate from the durable Teams TurnID carried by an
	// outbox row; a missing value must not be treated as the outer turn proof.
	CodexTurnID    string                   `json:"codex_turn_id,omitempty"`
	SourcePath     string                   `json:"source_path,omitempty"`
	SourceLine     int                      `json:"source_line,omitempty"`
	SourceOffset   int64                    `json:"source_offset,omitempty"`
	SourceRecordID string                   `json:"source_record_id,omitempty"`
	Kind           string                   `json:"kind,omitempty"`
	TextHash       string                   `json:"text_hash,omitempty"`
	OutboxID       string                   `json:"outbox_id,omitempty"`
	TeamsMessageID string                   `json:"teams_message_id,omitempty"`
	Status         TranscriptDeliveryStatus `json:"status,omitempty"`
	CreatedAt      time.Time                `json:"created_at,omitempty"`
	UpdatedAt      time.Time                `json:"updated_at,omitempty"`
	SentAt         time.Time                `json:"sent_at,omitempty"`
}

type HelperDeliveryRecord struct {
	ID             string               `json:"id"`
	SessionID      string               `json:"session_id,omitempty"`
	TeamsChatID    string               `json:"teams_chat_id,omitempty"`
	CodexThreadID  string               `json:"codex_thread_id,omitempty"`
	TurnID         string               `json:"turn_id,omitempty"`
	Kind           string               `json:"kind,omitempty"`
	KindFamily     string               `json:"kind_family,omitempty"`
	SourceTextHash string               `json:"source_text_hash,omitempty"`
	RenderedHash   string               `json:"rendered_hash,omitempty"`
	VisibleHash    string               `json:"visible_hash,omitempty"`
	OutboxID       string               `json:"outbox_id,omitempty"`
	TeamsMessageID string               `json:"teams_message_id,omitempty"`
	PartIndex      int                  `json:"part_index,omitempty"`
	PartCount      int                  `json:"part_count,omitempty"`
	Status         HelperDeliveryStatus `json:"status,omitempty"`
	CreatedAt      time.Time            `json:"created_at,omitempty"`
	UpdatedAt      time.Time            `json:"updated_at,omitempty"`
	SentAt         time.Time            `json:"sent_at,omitempty"`
}

type ImportCheckpoint struct {
	ID         string `json:"id"`
	SessionID  string `json:"session_id"`
	SourcePath string `json:"source_path,omitempty"`
	// SourceRewriteBlocked fences source-less transcript rows written by an
	// older helper after automatic scanning proves that the linked source was
	// replaced or truncated. Ordinary backlog blocking must not strand them.
	SourceRewriteBlocked bool `json:"source_rewrite_blocked,omitempty"`
	// SourceFingerprint is a bounded content fingerprint for the last trusted
	// cursor.  It is deliberately optional so old checkpoints remain readable;
	// callers must fail closed rather than use the unchanged-file fast path when
	// it is absent.
	SourceFingerprint string `json:"source_fingerprint,omitempty"`
	LastRecordID      string `json:"last_record_id,omitempty"`
	LastSourceLine    int    `json:"last_source_line,omitempty"`
	LastOffset        int64  `json:"last_offset,omitempty"`
	// LastOffsetKnown distinguishes a trusted zero-byte cursor from legacy
	// checkpoints that never persisted a byte position. A false value for a
	// zero offset must never be treated as an EOF proof.
	LastOffsetKnown bool      `json:"last_offset_known,omitempty"`
	SourceSize      int64     `json:"source_size,omitempty"`
	SourceModTime   time.Time `json:"source_mod_time,omitempty"`
	ImportTurnID    string    `json:"import_turn_id,omitempty"`
	KindPrefix      string    `json:"kind_prefix,omitempty"`
	Status          string    `json:"status,omitempty"`
	// LegacyProbeRevision caches the negative SQLite compatibility probe for
	// the interrupted-turn set. It is invalidated by a session revision change
	// and avoids decoding every interrupted row on each hot-path transaction.
	LegacyProbeRevision string `json:"legacy_probe_revision,omitempty"`
	// UnresolvedExecution is deliberately stored inside the existing JSON
	// checkpoint row.  It adds no SQL table/schema migration while preserving
	// execution ownership ambiguity across helper restarts.
	UnresolvedExecution *ExecutionAnchor `json:"unresolved_execution,omitempty"`
	// ExecutionAnchorGeneration is retained after an anchor is cleared so a
	// late callback cannot accidentally clear a subsequently recreated anchor
	// with the same outer turn ID.
	ExecutionAnchorGeneration int64     `json:"execution_anchor_generation,omitempty"`
	UpdatedAt                 time.Time `json:"updated_at,omitempty"`
}

// LinkedTranscriptSessionSnapshot is the single read used by the linked
// transcript bridge to decide which registered sessions are safe to inspect.
// Keeping running-turn state, canonical checkpoints, and the legacy ownership
// probe together avoids three independent JSON reads (or three SQLite lock
// acquisitions) for one poll. The final completion/failure CAS remains
// authoritative; this snapshot is only a read-side guard.
type LinkedTranscriptSessionSnapshot struct {
	Running     map[string]bool
	Checkpoints map[string]ImportCheckpoint
	Ownership   map[string]bool
}

// LinkedTranscriptExecutionSnapshot is the execution-only half of the
// linked-transcript read-side guard.  Callers use this after a cheap
// checkpoint/file no-growth check for the sessions that may actually scan or
// publish.  A trusted idle session does not need to decode its turns merely to
// prove that a no-op has no side effects; the store-side final CAS remains the
// authority before any cursor or outbox mutation.
type LinkedTranscriptExecutionSnapshot struct {
	Running   map[string]bool
	Ownership map[string]bool
}

// TranscriptCheckpointProgress describes the complete record that may be
// committed together with a terminal turn transition.  Keeping this request
// in the store package lets JSON and SQLite apply the same cursor/ownership
// CAS instead of having the bridge update the checkpoint in a separate
// transaction from MarkTurnCompleted.
type TranscriptCheckpointProgress struct {
	ID                string
	SessionID         string
	SourcePath        string
	SourceFingerprint string
	// AnchorSourceFingerprint is the bounded source proof captured at the
	// unresolved anchor cutoff.  SourceFingerprint below describes the cursor
	// being committed and therefore legitimately changes as the transcript
	// grows; it is not sufficient to prove that the bytes before the anchor
	// remained unchanged.  The bridge copies this value from the durable anchor
	// and the store compares it inside the final CAS.
	AnchorSourceFingerprint string
	LastRecordID            string
	LastSourceLine          int
	LastOffset              int64
	LastOffsetKnown         bool
	SourceSize              int64
	SourceModTime           time.Time
}

// CompleteTurnWithFinalRequest is the narrow durable commit used by a real
// Codex completion.  Rendering and bounded source fingerprinting happen
// before entering the store transaction; the transaction itself only
// validates ownership, inserts the deterministic final outbox chunks, and
// commits the terminal Turn/checkpoint/anchor transition.
type CompleteTurnWithFinalRequest struct {
	SessionID        string
	TurnID           string
	CodexThreadID    string
	CodexTurnID      string
	AnchorGeneration int64
	// ResolveInterrupted is set only by ResolveInterruptedTurnWithCompletionProof.
	// Normal completion must never promote an interrupted turn based on a stale
	// in-memory result.  It is intentionally not serialized; the store still
	// revalidates the complete execution anchor inside its transaction.
	ResolveInterrupted bool `json:"-"`
	Progress           TranscriptCheckpointProgress
	FinalOutbox        []OutboxMessage
}

// ExecutionAnchorClearRequest is the durable capability used to clear one
// exact anchor and, when needed, confirm its interrupted outer turn. The
// generation and cutoff fields make a late callback harmless after an anchor
// has been recreated for the same turn.
type ExecutionAnchorClearRequest struct {
	CheckpointID       string
	SessionID          string
	ThreadID           string
	SourcePath         string
	SourceFingerprint  string
	OuterTurnID        string
	CodexTurnID        string
	Generation         int64
	CutoffRecordID     string
	CutoffLine         int
	CutoffOffset       int64
	RecoveryReasonFrom string
	RecoveryReasonTo   string
}

// ExecutionFailureIdentity is the callback identity supplied by app-server.
// The store resolves the current anchor from this identity inside the same
// transaction that changes the Turn, rather than accepting a bridge snapshot
// of the anchor as authority.
type ExecutionFailureIdentity struct {
	SessionID   string
	TurnID      string
	ThreadID    string
	CodexTurnID string
	// AnchorGeneration is required whenever an unresolved anchor is active.
	// The app-server IDs identify the execution, while the generation prevents
	// a late callback from consuming a recreated anchor whose IDs happen to be
	// reused. Zero is retained only for the no-anchor legacy path.
	AnchorGeneration int64
}

// PersistInterruptedTurnWithAnchorRequest is the single durable transition
// used when Codex ownership becomes ambiguous.  The Turn status and its
// transcript ownership anchor are written together so a terminal callback
// cannot commit between the two writes and leave a Completed turn fenced by a
// newly-created unresolved anchor.
type PersistInterruptedTurnWithAnchorRequest struct {
	SessionID          string
	TurnID             string
	CheckpointID       string
	CodexThreadID      string
	CodexTurnID        string
	RecoveryReason     string
	Anchor             ExecutionAnchor
	ConservativeCutoff bool
}

// PersistInterruptedTurnWithAnchorResult describes the result of the atomic
// transition.  Terminal is true when a completion/failure callback already
// won the race; callers must not emit a second interruption notice then.
type PersistInterruptedTurnWithAnchorResult struct {
	Turn     Turn
	Changed  bool
	Terminal bool
}

// ExecutionAnchor records the last durable boundary before Codex execution
// ownership became ambiguous.  Internal Codex task IDs are not required to
// match the outer Teams turn ID: native goal continuations can create new
// task IDs without creating a new durable Teams Turn.
type ExecutionAnchor struct {
	SessionID string `json:"session_id,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
	// OuterTurnID identifies the durable Teams turn record. CodexTurnID is the
	// execution identity written by app-server into the transcript; they are
	// intentionally separate because a durable turn ID is an internal store
	// key and is not required to match the Codex protocol turn ID.
	OuterTurnID           string    `json:"outer_turn_id,omitempty"`
	CodexTurnID           string    `json:"codex_turn_id,omitempty"`
	SourcePath            string    `json:"transcript_source,omitempty"`
	SourceFingerprint     string    `json:"source_fingerprint,omitempty"`
	CutoffRecordID        string    `json:"cutoff_record_id,omitempty"`
	CutoffLine            int       `json:"cutoff_line,omitempty"`
	CutoffOffset          int64     `json:"cutoff_offset,omitempty"`
	Reason                string    `json:"reason,omitempty"`
	ObservedTaskIDs       []string  `json:"observed_internal_task_ids,omitempty"`
	ObservedSourceSize    int64     `json:"observed_source_size,omitempty"`
	ObservedSourceModTime time.Time `json:"observed_source_mod_time,omitempty"`
	TerminalProbeSize     int64     `json:"terminal_probe_size,omitempty"`
	TerminalProbeModTime  time.Time `json:"terminal_probe_mod_time,omitempty"`
	LastFenceCheckAt      time.Time `json:"last_fence_check_at,omitempty"`
	State                 string    `json:"state,omitempty"`
	Generation            int64     `json:"generation,omitempty"`
	CreatedAt             time.Time `json:"created_at,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
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

type HistoryWatchCheckpoint struct {
	ID      string    `json:"id,omitempty"`
	Path    string    `json:"path,omitempty"`
	Size    int64     `json:"size,omitempty"`
	ModTime time.Time `json:"mod_time,omitempty"`
	// SourceFingerprint is a bounded fingerprint of the trusted prefix at
	// Offset.  It is optional for legacy checkpoints; a missing value forces a
	// conservative migration/reconcile rather than proving an unchanged file.
	SourceFingerprint      string `json:"source_fingerprint,omitempty"`
	SourceRewriteBlocked   bool   `json:"source_rewrite_blocked,omitempty"`
	Offset                 int64  `json:"offset,omitempty"`
	Line                   int    `json:"line,omitempty"`
	SessionID              string `json:"session_id,omitempty"`
	ThreadID               string `json:"thread_id,omitempty"`
	TeamsOriginThreadID    string `json:"teams_origin_thread_id,omitempty"`
	TurnID                 string `json:"turn_id,omitempty"`
	TeamsOriginTurnID      string `json:"teams_origin_turn_id,omitempty"`
	ExternalUserPromptSeen bool   `json:"external_user_prompt_seen,omitempty"`
	LastFinalID            string `json:"last_final_id,omitempty"`
	LastFinalLine          int    `json:"last_final_line,omitempty"`
	LastFinalStartOffset   int64  `json:"last_final_start_offset,omitempty"`
	// LastFinalStartOffsetKnown distinguishes a valid zero-byte final start
	// from an old checkpoint that never persisted the boundary position.
	LastFinalStartOffsetKnown bool   `json:"last_final_start_offset_known,omitempty"`
	LastFinalThreadID         string `json:"last_final_thread_id,omitempty"`
	LastFinalTurnID           string `json:"last_final_turn_id,omitempty"`
	LastFinalTextHash         string `json:"last_final_text_hash,omitempty"`
	TerminalBoundarySeen      bool   `json:"terminal_boundary_seen,omitempty"`
	TerminalBoundaryLine      int    `json:"terminal_boundary_line,omitempty"`
	// UnresolvedContinuation is a durable fail-closed marker. It survives a
	// helper restart so the watcher cannot reinterpret a later child task as
	// an ordinary next turn after losing its in-memory scan state.
	UnresolvedContinuation       bool      `json:"unresolved_continuation,omitempty"`
	UnresolvedContinuationLine   int       `json:"unresolved_continuation_line,omitempty"`
	UnresolvedContinuationOffset int64     `json:"unresolved_continuation_offset,omitempty"`
	PendingRootTaskStarted       bool      `json:"pending_root_task_started,omitempty"`
	PendingRootTaskStartedLine   int       `json:"pending_root_task_started_line,omitempty"`
	PendingRootTaskStartedOffset int64     `json:"pending_root_task_started_offset,omitempty"`
	PendingAssistantSourceID     string    `json:"pending_assistant_source_id,omitempty"`
	PendingAssistantThreadID     string    `json:"pending_assistant_thread_id,omitempty"`
	PendingAssistantTurnID       string    `json:"pending_assistant_turn_id,omitempty"`
	PendingAssistantText         string    `json:"pending_assistant_text,omitempty"`
	PendingAssistantCreatedAt    time.Time `json:"pending_assistant_created_at,omitempty"`
	PendingAssistantSourceLine   int       `json:"pending_assistant_source_line,omitempty"`
	PendingAssistantStartOffset  int64     `json:"pending_assistant_start_offset,omitempty"`
	PendingAssistantOffset       int64     `json:"pending_assistant_offset,omitempty"`
	PendingAssistantSourceType   string    `json:"pending_assistant_source_type,omitempty"`
	UpdatedAt                    time.Time `json:"updated_at,omitempty"`
}

type ArtifactRecord struct {
	ID             string    `json:"id"`
	SessionID      string    `json:"session_id,omitempty"`
	TurnID         string    `json:"turn_id,omitempty"`
	Path           string    `json:"path,omitempty"`
	UploadName     string    `json:"upload_name,omitempty"`
	DriveItemID    string    `json:"drive_item_id,omitempty"`
	OutboxID       string    `json:"outbox_id,omitempty"`
	TeamsMessageID string    `json:"teams_message_id,omitempty"`
	Status         string    `json:"status,omitempty"`
	StatusReason   string    `json:"status_reason,omitempty"`
	Error          string    `json:"error,omitempty"`
	UploadedAt     time.Time `json:"uploaded_at,omitempty"`
	SentAt         time.Time `json:"sent_at,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type NotificationRecord struct {
	ID                 string    `json:"id"`
	SessionID          string    `json:"session_id,omitempty"`
	TurnID             string    `json:"turn_id,omitempty"`
	Kind               string    `json:"kind,omitempty"`
	OutboxID           string    `json:"outbox_id,omitempty"`
	Status             string    `json:"status,omitempty"`
	Title              string    `json:"title,omitempty"`
	ChatTitle          string    `json:"chat_title,omitempty"`
	RequestSummary     string    `json:"request_summary,omitempty"`
	Hint               string    `json:"hint,omitempty"`
	ButtonTitle        string    `json:"button_title,omitempty"`
	ButtonURL          string    `json:"button_url,omitempty"`
	Attempts           int       `json:"attempts,omitempty"`
	LastError          string    `json:"last_error,omitempty"`
	LastErrorRetryable bool      `json:"last_error_retryable,omitempty"`
	DeliveryUncertain  bool      `json:"delivery_uncertain,omitempty"`
	LastAttemptAt      time.Time `json:"last_attempt_at,omitempty"`
	SentAt             time.Time `json:"sent_at,omitempty"`
	CreatedAt          time.Time `json:"created_at,omitempty"`
	UpdatedAt          time.Time `json:"updated_at,omitempty"`
}

type WorkflowNotificationConfig struct {
	Enabled               bool      `json:"enabled,omitempty"`
	ControlWebhookURLFile string    `json:"control_webhook_url_file,omitempty"`
	ControlChatID         string    `json:"control_chat_id,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
}

const (
	NotificationStatusQueued  = "queued"
	NotificationStatusSending = "sending"
	NotificationStatusSent    = "sent"
	NotificationStatusFailed  = "failed"
	NotificationStatusUnknown = "unknown"
)

type ServiceControl struct {
	Paused               bool      `json:"paused,omitempty"`
	Draining             bool      `json:"draining,omitempty"`
	Reason               string    `json:"reason,omitempty"`
	DrainOperationID     string    `json:"drain_operation_id,omitempty"`
	LastDrainOperationID string    `json:"last_drain_operation_id,omitempty"`
	LastDrainOperationAt time.Time `json:"last_drain_operation_at,omitempty"`
	UpdatedAt            time.Time `json:"updated_at,omitempty"`
}

var ErrDrainOperationConflict = errors.New("teams drain is owned by another operation")

type UpgradePhase string

const (
	UpgradePhaseRescuing  UpgradePhase = "rescuing"
	UpgradePhaseDraining  UpgradePhase = "draining"
	UpgradePhaseReady     UpgradePhase = "ready"
	UpgradePhaseCompleted UpgradePhase = "completed"
	UpgradePhaseAborted   UpgradePhase = "aborted"
)

type UpgradeRequest struct {
	ID                  string                      `json:"id"`
	Phase               UpgradePhase                `json:"phase"`
	Reason              string                      `json:"reason,omitempty"`
	PreviousControl     ServiceControl              `json:"previous_control,omitempty"`
	NotificationTargets []UpgradeNotificationTarget `json:"notification_targets,omitempty"`
	InstalledTag        string                      `json:"installed_tag,omitempty"`
	CompletionNoticeID  string                      `json:"completion_notice_id,omitempty"`
	CompletionNoticeAt  time.Time                   `json:"completion_notice_at,omitempty"`
	DeadlineAt          time.Time                   `json:"deadline_at,omitempty"`
	StartedAt           time.Time                   `json:"started_at,omitempty"`
	RescueStartedAt     time.Time                   `json:"rescue_started_at,omitempty"`
	RescueCompletedAt   time.Time                   `json:"rescue_completed_at,omitempty"`
	RescueActions       []UpgradeRescueAction       `json:"rescue_actions,omitempty"`
	ReadyAt             time.Time                   `json:"ready_at,omitempty"`
	CompletedAt         time.Time                   `json:"completed_at,omitempty"`
	AbortedAt           time.Time                   `json:"aborted_at,omitempty"`
	AbortReason         string                      `json:"abort_reason,omitempty"`
	UpdatedAt           time.Time                   `json:"updated_at,omitempty"`
}

type UpgradeRescueAction struct {
	Kind      string    `json:"kind,omitempty"`
	ID        string    `json:"id,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	Status    string    `json:"status,omitempty"`
	Detail    string    `json:"detail,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

type UpgradeNotificationTarget struct {
	SessionID   string    `json:"session_id,omitempty"`
	TurnID      string    `json:"turn_id,omitempty"`
	TeamsChatID string    `json:"teams_chat_id,omitempty"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
}

type AutoUpdateState struct {
	LastCheckAt          time.Time `json:"last_check_at,omitempty"`
	NextCheckAt          time.Time `json:"next_check_at,omitempty"`
	BackoffUntil         time.Time `json:"backoff_until,omitempty"`
	LastSuccessAt        time.Time `json:"last_success_at,omitempty"`
	LastError            string    `json:"last_error,omitempty"`
	LastErrorAt          time.Time `json:"last_error_at,omitempty"`
	CandidateTag         string    `json:"candidate_tag,omitempty"`
	CandidateVersion     string    `json:"candidate_version,omitempty"`
	CandidatePriority    string    `json:"candidate_priority,omitempty"`
	CandidateAsset       string    `json:"candidate_asset,omitempty"`
	CandidatePublishedAt time.Time `json:"candidate_published_at,omitempty"`
	CandidateEligibleAt  time.Time `json:"candidate_eligible_at,omitempty"`
	LastAttemptTag       string    `json:"last_attempt_tag,omitempty"`
	LastAttemptAt        time.Time `json:"last_attempt_at,omitempty"`
	LastInstalledTag     string    `json:"last_installed_tag,omitempty"`
	LastInstalledAt      time.Time `json:"last_installed_at,omitempty"`
}

type AutoUpdateRecord struct {
	Now                  time.Time
	NextCheckAt          time.Time
	BackoffUntil         time.Time
	LastError            string
	CandidateTag         string
	CandidateVersion     string
	CandidatePriority    string
	CandidateAsset       string
	CandidatePublishedAt time.Time
	CandidateEligibleAt  time.Time
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

type IdleWorkChatParkCandidate struct {
	Session SessionContext
	Poll    ChatPollState
}

func chatPollParkedSkipEligible(poll ChatPollState) bool {
	return strings.TrimSpace(poll.PollState) == chatPollStateParked &&
		!poll.ParkNoticeSentAt.IsZero() &&
		!poll.ParkedAt.IsZero() &&
		poll.BlockedUntil.IsZero() &&
		strings.TrimSpace(poll.ContinuationPath) == "" &&
		poll.FailureCount == 0 &&
		strings.TrimSpace(poll.LastError) == "" &&
		poll.LastErrorAt.IsZero()
}

type ChatPollScheduleUpdate struct {
	ChatID                string
	PollState             string
	PreviousPollState     string
	NextPollAt            time.Time
	LastActivityAt        time.Time
	BlockedUntil          time.Time
	ClearBlockedUntil     bool
	ClearContinuationPath bool
	ResetFailures         bool
}

type FinalAnswerPollBoostRequest struct {
	SessionID      string
	TeamsChatID    string
	NextPollAt     time.Time
	LastActivityAt time.Time
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
	ID                          string                `json:"id"`
	Status                      SessionStatus         `json:"status"`
	TeamsChatID                 string                `json:"teams_chat_id"`
	TeamsChatURL                string                `json:"teams_chat_url,omitempty"`
	TeamsTopic                  string                `json:"teams_topic,omitempty"`
	UserTitle                   string                `json:"user_title,omitempty"`
	TitleSource                 string                `json:"title_source,omitempty"`
	CodexThreadID               string                `json:"codex_thread_id,omitempty"`
	ModelGeneration             int                   `json:"model_generation,omitempty"`
	LatestCodexTurnID           string                `json:"latest_codex_turn_id,omitempty"`
	LatestTurnID                string                `json:"latest_turn_id,omitempty"`
	RunnerKind                  string                `json:"runner_kind,omitempty"`
	CodexVersion                string                `json:"codex_version,omitempty"`
	Cwd                         string                `json:"cwd,omitempty"`
	CodexHome                   string                `json:"codex_home,omitempty"`
	Profile                     string                `json:"profile,omitempty"`
	Model                       string                `json:"model,omitempty"`
	ModelProfile                modelprofile.Snapshot `json:"model_profile,omitempty"`
	ModelSelectionSource        string                `json:"model_selection_source,omitempty"`
	PendingModelProfile         modelprofile.Snapshot `json:"pending_model_profile,omitempty"`
	PendingModelSelectionSource string                `json:"pending_model_selection_source,omitempty"`
	PendingModelRequestedAt     time.Time             `json:"pending_model_requested_at,omitempty"`
	PendingReasoningEffort      string                `json:"pending_reasoning_effort,omitempty"`
	PendingReasoningSource      string                `json:"pending_reasoning_source,omitempty"`
	ReasoningEffort             string                `json:"reasoning_effort,omitempty"`
	ReasoningEffortSource       string                `json:"reasoning_effort_source,omitempty"`
	Sandbox                     string                `json:"sandbox,omitempty"`
	ProxyMode                   string                `json:"proxy_mode,omitempty"`
	QuarantinedAt               time.Time             `json:"quarantined_at,omitempty"`
	QuarantineReason            string                `json:"quarantine_reason,omitempty"`
	QuarantineSource            string                `json:"quarantine_source,omitempty"`
	QuarantineMessageIDs        []string              `json:"quarantine_message_ids,omitempty"`
	CreatedAt                   time.Time             `json:"created_at,omitempty"`
	UpdatedAt                   time.Time             `json:"updated_at,omitempty"`
}

type InboundEvent struct {
	ID               string                     `json:"id"`
	SessionID        string                     `json:"session_id,omitempty"`
	TeamsChatID      string                     `json:"teams_chat_id"`
	TeamsMessageID   string                     `json:"teams_message_id"`
	AuthorUserID     string                     `json:"author_user_id,omitempty"`
	AuthorName       string                     `json:"author_name,omitempty"`
	ScopeID          string                     `json:"scope_id,omitempty"`
	MachineID        string                     `json:"machine_id,omitempty"`
	LeaseGeneration  int64                      `json:"lease_generation,omitempty"`
	Text             string                     `json:"text,omitempty"`
	TextHash         string                     `json:"text_hash,omitempty"`
	Source           string                     `json:"source,omitempty"`
	Status           InboundStatus              `json:"status"`
	TurnID           string                     `json:"turn_id,omitempty"`
	TeamsBodyType    string                     `json:"teams_body_type,omitempty"`
	TeamsBodyHTML    string                     `json:"teams_body_html,omitempty"`
	TeamsAttachments []InboundAttachmentContext `json:"teams_attachments,omitempty"`
	ReceivedAt       time.Time                  `json:"received_at,omitempty"`
	CreatedAt        time.Time                  `json:"created_at,omitempty"`
	UpdatedAt        time.Time                  `json:"updated_at,omitempty"`
}

type InboundAttachmentContext struct {
	ID          string `json:"id,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	ContentURL  string `json:"content_url,omitempty"`
	Content     string `json:"content,omitempty"`
	Name        string `json:"name,omitempty"`
}

type Turn struct {
	ID              string                `json:"id"`
	SessionID       string                `json:"session_id"`
	InboundEventID  string                `json:"inbound_event_id,omitempty"`
	ScopeID         string                `json:"scope_id,omitempty"`
	MachineID       string                `json:"machine_id,omitempty"`
	LeaseGeneration int64                 `json:"lease_generation,omitempty"`
	Status          TurnStatus            `json:"status"`
	CodexThreadID   string                `json:"codex_thread_id,omitempty"`
	ModelGeneration int                   `json:"model_generation,omitempty"`
	CodexTurnID     string                `json:"codex_turn_id,omitempty"`
	ModelProfile    modelprofile.Snapshot `json:"model_profile,omitempty"`
	ReasoningEffort string                `json:"reasoning_effort,omitempty"`
	// ReasoningEffortSource makes an intentionally empty runtime fallback a
	// captured turn value rather than indistinguishable legacy missing data.
	ReasoningEffortSource string    `json:"reasoning_effort_source,omitempty"`
	FailureMessage        string    `json:"failure_message,omitempty"`
	RecoveryReason        string    `json:"recovery_reason,omitempty"`
	QueuedAt              time.Time `json:"queued_at,omitempty"`
	StartedAt             time.Time `json:"started_at,omitempty"`
	CompletedAt           time.Time `json:"completed_at,omitempty"`
	FailedAt              time.Time `json:"failed_at,omitempty"`
	InterruptedAt         time.Time `json:"interrupted_at,omitempty"`
	CreatedAt             time.Time `json:"created_at,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
}

type OutboxMessage struct {
	ID                   string `json:"id"`
	SessionID            string `json:"session_id,omitempty"`
	ParentFenceSessionID string `json:"parent_fence_session_id,omitempty"`
	TurnID               string `json:"turn_id,omitempty"`
	CodexThreadID        string `json:"codex_thread_id,omitempty"`
	TeamsChatID          string `json:"teams_chat_id"`
	ScopeID              string `json:"scope_id,omitempty"`
	MachineID            string `json:"machine_id,omitempty"`
	LeaseGeneration      int64  `json:"lease_generation,omitempty"`
	Kind                 string `json:"kind,omitempty"`
	Body                 string `json:"body,omitempty"`
	Sequence             int64  `json:"sequence,omitempty"`
	PartIndex            int    `json:"part_index,omitempty"`
	PartCount            int    `json:"part_count,omitempty"`
	// TerminalGroupID binds every chunk of one terminal final.  It is optional
	// for legacy rows; when absent, final/final-N rows for the same TurnID are
	// treated as one terminal group by the fence helpers below.
	TerminalGroupID string `json:"terminal_group_id,omitempty"`
	// Transcript provenance lets the unresolved-execution fence distinguish a
	// queued record from the trusted prefix before an anchor cutoff.  Legacy
	// rows without these fields remain conservatively blocked.
	TranscriptCheckpointID      string `json:"transcript_checkpoint_id,omitempty"`
	TranscriptSourcePath        string `json:"transcript_source_path,omitempty"`
	TranscriptSourceOffset      int64  `json:"transcript_source_offset,omitempty"`
	TranscriptSourceOffsetKnown bool   `json:"transcript_source_offset_known,omitempty"`
	// TranscriptSourceProofFingerprint/Offset authenticate the source prefix
	// that was checked before a linked transcript record was queued.  The
	// record offset above identifies the delivered item; it is not the proof
	// boundary and must not be substituted for it.
	TranscriptSourceProofFingerprint string           `json:"transcript_source_proof_fingerprint,omitempty"`
	TranscriptSourceProofOffset      int64            `json:"transcript_source_proof_offset,omitempty"`
	TranscriptSourceProofOffsetKnown bool             `json:"transcript_source_proof_offset_known,omitempty"`
	SourceTextHash                   string           `json:"source_text_hash,omitempty"`
	RenderedHash                     string           `json:"rendered_hash,omitempty"`
	RenderedBytes                    int              `json:"rendered_bytes,omitempty"`
	AttachmentPath                   string           `json:"attachment_path,omitempty"`
	AttachmentName                   string           `json:"attachment_name,omitempty"`
	AttachmentUploadName             string           `json:"attachment_upload_name,omitempty"`
	AttachmentContentType            string           `json:"attachment_content_type,omitempty"`
	AttachmentUploadFolder           string           `json:"attachment_upload_folder,omitempty"`
	AttachmentSize                   int64            `json:"attachment_size,omitempty"`
	AttachmentHash                   string           `json:"attachment_hash,omitempty"`
	AttachmentUploadURL              string           `json:"attachment_upload_url,omitempty"`
	AttachmentUploadExpiry           time.Time        `json:"attachment_upload_expiry,omitempty"`
	AttachmentUploadOffset           int64            `json:"attachment_upload_offset,omitempty"`
	DriveItemID                      string           `json:"drive_item_id,omitempty"`
	DriveItemName                    string           `json:"drive_item_name,omitempty"`
	DriveItemETag                    string           `json:"drive_item_etag,omitempty"`
	DriveItemWebURL                  string           `json:"drive_item_web_url,omitempty"`
	DriveItemWebDav                  string           `json:"drive_item_web_dav,omitempty"`
	AckKind                          string           `json:"ack_kind,omitempty"`
	QuoteReplyToMessageID            string           `json:"quote_reply_to_message_id,omitempty"`
	NotificationKind                 string           `json:"notification_kind,omitempty"`
	ForkOperationID                  string           `json:"fork_operation_id,omitempty"`
	ForkHistoryNamespace             string           `json:"fork_history_namespace,omitempty"`
	ForkOrdinal                      int              `json:"fork_ordinal,omitempty"`
	ForkBodyHash                     string           `json:"fork_body_hash,omitempty"`
	ForkRole                         string           `json:"fork_role,omitempty"`
	MentionOwner                     bool             `json:"mention_owner,omitempty"`
	MentionUserID                    string           `json:"mention_user_id,omitempty"`
	MentionUserName                  string           `json:"mention_user_name,omitempty"`
	TrustedMath                      bool             `json:"trusted_math,omitempty"`
	MathPlanVersion                  int              `json:"math_plan_version,omitempty"`
	MathSpans                        []OutboxMathSpan `json:"math_spans,omitempty"`
	MathMediaFallback                bool             `json:"math_media_fallback,omitempty"`
	UpgradeNonBlocking               bool             `json:"upgrade_non_blocking,omitempty"`
	ArtifactIDs                      []string         `json:"artifact_ids,omitempty"`
	Status                           OutboxStatus     `json:"status"`
	TeamsMessageID                   string           `json:"teams_message_id,omitempty"`
	// BlockedByUnresolvedExecution records that Graph accepted this message
	// concurrently with a newly persisted execution anchor.  The delivery is
	// reconciled by its stable TeamsMessageID, but transcript checkpoint and
	// turn-completion side effects must not be advanced from this callback.
	BlockedByUnresolvedExecution bool `json:"blocked_by_unresolved_execution,omitempty"`
	// BlockedByTerminalFailure is a durable send fence for a terminal final
	// whose owning Turn has already failed.  It is intentionally separate from
	// an unresolved anchor: the anchor may be cleared by an exact failure proof
	// while an in-flight Graph request still needs to settle without promoting
	// the stale final or being retried after a restart.
	BlockedByTerminalFailure bool `json:"blocked_by_terminal_failure,omitempty"`
	// BlockedBySourceRewrite records that Graph accepted a transcript row but
	// the source proof changed before the final durable delivery CAS. The
	// message ID is retained for reconciliation; this fence never permits a
	// retry or transcript side effect.
	BlockedBySourceRewrite bool      `json:"blocked_by_source_rewrite,omitempty"`
	CreatedAt              time.Time `json:"created_at,omitempty"`
	UpdatedAt              time.Time `json:"updated_at,omitempty"`
	SentAt                 time.Time `json:"sent_at,omitempty"`
	LastSendAttempt        time.Time `json:"last_send_attempt,omitempty"`
	SendAttemptToken       string    `json:"send_attempt_token,omitempty"`
	LastSendError          string    `json:"last_send_error,omitempty"`
}

// OutboxReplayFence is a cold-path proof that an outbox message was already
// accepted by Teams before a legacy store was quarantined. Every populated
// identity field must match the canonical outbox row before it can be promoted
// to sent.
type OutboxReplayFence struct {
	OutboxID       string
	TeamsChatID    string
	TeamsMessageID string
	SessionID      string
	TurnID         string
	Kind           string
	// Source proof fields make replay promotion subject to the same final
	// source fence as normal delivery.  They are optional for non-transcript
	// rows and for explicit history imports.
	SourcePath        string
	SourceFingerprint string
	SourceOffset      int64
	SourceOffsetKnown bool
}

type SessionQuarantineRequest struct {
	SessionID         string
	Reason            string
	Source            string
	TriggerMessageIDs []string
	InFlightOutboxIDs []string
	Now               time.Time
}

type SessionQuarantineReport struct {
	Session            SessionContext
	Changed            bool
	InterruptedTurnIDs []string
	IgnoredInboundIDs  []string
	SkippedOutboxIDs   []string
	PreservedOutboxIDs []string
}

type SessionUnquarantineRequest struct {
	SessionID string
	Now       time.Time
}

type SessionUnquarantineReport struct {
	Session SessionContext
	Changed bool
}

type OutboxMathSpan struct {
	Start  int    `json:"start"`
	End    int    `json:"end"`
	Index  int    `json:"index"`
	Source string `json:"source"`
}

type PendingOutboxCursor struct {
	CreatedAt time.Time
	ID        string
}

func (c PendingOutboxCursor) IsZero() bool {
	return c.CreatedAt.IsZero() && strings.TrimSpace(c.ID) == ""
}

type PendingOutboxQuery struct {
	Now         time.Time
	SessionID   string
	TurnID      string
	TeamsChatID string
	Limit       int
	After       PendingOutboxCursor
}

type PendingOutboxPage struct {
	Messages   []OutboxMessage
	NextCursor PendingOutboxCursor
	More       bool
}

// OutboxEchoCandidateQuery bounds the exceptional rendered-content lookup used
// to recover a helper message that reached Teams before its Graph message ID
// was recorded locally. The normal inbound path must continue to use message
// ID/provenance lookups instead.
type OutboxEchoCandidateQuery struct {
	TeamsChatID    string
	LimitPerStatus int
}

type ModelProfileKeyIntakeStatus string

const (
	ModelProfileKeyIntakePending   ModelProfileKeyIntakeStatus = "pending"
	ModelProfileKeyIntakeConfirmed ModelProfileKeyIntakeStatus = "confirmed"
	ModelProfileKeyIntakeSaving    ModelProfileKeyIntakeStatus = "saving"
	ModelProfileKeyIntakeCompleted ModelProfileKeyIntakeStatus = "completed"
	ModelProfileKeyIntakeCanceled  ModelProfileKeyIntakeStatus = "canceled"
	ModelProfileKeyIntakeExpired   ModelProfileKeyIntakeStatus = "expired"
)

type ModelProfileKeyIntake struct {
	ID               string                      `json:"id"`
	ScopeID          string                      `json:"scope_id,omitempty"`
	TeamsChatID      string                      `json:"teams_chat_id,omitempty"`
	RequestMessageID string                      `json:"request_message_id,omitempty"`
	AuthorUserID     string                      `json:"author_user_id,omitempty"`
	AuthorName       string                      `json:"author_name,omitempty"`
	ProfileName      string                      `json:"profile_name,omitempty"`
	Provider         string                      `json:"provider,omitempty"`
	Model            string                      `json:"model,omitempty"`
	CredentialScope  string                      `json:"credential_scope,omitempty"`
	SSHProxy         string                      `json:"ssh_proxy,omitempty"`
	SetDefault       bool                        `json:"set_default,omitempty"`
	CodeHash         string                      `json:"code_hash,omitempty"`
	Status           ModelProfileKeyIntakeStatus `json:"status,omitempty"`
	LastError        string                      `json:"last_error,omitempty"`
	CreatedAt        time.Time                   `json:"created_at,omitempty"`
	UpdatedAt        time.Time                   `json:"updated_at,omitempty"`
	ExpiresAt        time.Time                   `json:"expires_at,omitempty"`
	ConfirmedAt      time.Time                   `json:"confirmed_at,omitempty"`
	CompletedAt      time.Time                   `json:"completed_at,omitempty"`
	CanceledAt       time.Time                   `json:"canceled_at,omitempty"`
}

type MessageProvenanceRecord struct {
	ID             string    `json:"id"`
	TeamsChatID    string    `json:"teams_chat_id"`
	TeamsMessageID string    `json:"teams_message_id"`
	Origin         string    `json:"origin"`
	SessionID      string    `json:"session_id,omitempty"`
	TurnID         string    `json:"turn_id,omitempty"`
	OutboxID       string    `json:"outbox_id,omitempty"`
	InboundID      string    `json:"inbound_id,omitempty"`
	Kind           string    `json:"kind,omitempty"`
	RenderedHash   string    `json:"rendered_hash,omitempty"`
	Diagnostic     string    `json:"diagnostic,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type MessageLookup struct {
	Provenance         MessageProvenanceRecord
	HasProvenance      bool
	HasInbound         bool
	HasDeliveredOutbox bool
}

type stateFileStamp struct {
	Exists   bool
	Size     int64
	ModTime  time.Time
	Revision stateFileRevision
	Info     os.FileInfo
}

type stateFileRevision struct {
	Valid             bool   `json:"valid,omitempty"`
	VolumeSerial      uint32 `json:"volume_serial,omitempty"`
	FileIndexHigh     uint32 `json:"file_index_high,omitempty"`
	FileIndexLow      uint32 `json:"file_index_low,omitempty"`
	CreationTimeNanos int64  `json:"creation_time_nanos,omitempty"`
	ChangeTimeNanos   int64  `json:"change_time_nanos,omitempty"`
}

type messageLookupCache struct {
	Valid               bool
	Stamp               stateFileStamp
	Provenance          map[string]MessageProvenanceRecord
	ProvenanceCanonical map[string]bool
	Inbound             map[string]bool
	DeliveredOutbox     map[string]bool
}

type RecoveryReport struct {
	InterruptedTurnIDs        []string
	SupersededOutboxIDs       []string
	PreservedOutboxBlockerIDs []string
}

type UpgradeBlocker struct {
	Kind      string
	ID        string
	SessionID string
	Status    string
	Detail    string
}

type UpgradeRescueOptions struct {
	Reason     string
	StaleAfter time.Duration
	ForceOwner bool
}

type UpgradeRescueReport struct {
	Upgrade                     UpgradeRequest
	ClearedOwner                *OwnerMetadata
	PreservedQueuedTurnIDs      []string
	InterruptedTurnIDs          []string
	SupersededOutboxIDs         []string
	PreservedOutboxBlockerIDs   []string
	RemainingUpgradeBlockers    []UpgradeBlocker
	SkippedBecauseOwnerIsActive bool
}

var ErrOwnerLive = errors.New("Teams owner is active")

var (
	ownerHostname     = os.Hostname
	ownerProcessAlive = proc.IsAlive
)
var ErrUnsupportedSchemaVersion = errors.New("unsupported Teams state schema version")
var ErrControlLeaseNotHeld = errors.New("Teams control lease is not held by this machine")
var ErrInboundMessageFromHelperOutbox = errors.New("Teams inbound message already recorded as helper outbox")

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

type CodexThreadBindingConflictError struct {
	SessionID string
	Existing  string
	Observed  string
}

func (e CodexThreadBindingConflictError) Error() string {
	return fmt.Sprintf("Codex thread binding conflict for session %s: existing=%s observed=%s", e.SessionID, e.Existing, e.Observed)
}

type Store struct {
	path                     string
	mu                       sync.Mutex
	lock                     *flock.Flock
	messageLookup            messageLookupCache
	sqliteDB                 *sql.DB
	sqliteDBPath             string
	sqlitePointerCached      bool
	sqlitePointerTrusted     bool
	sqlitePointer            storeSQLitePointer
	sqlitePointerSize        int64
	sqlitePointerMod         time.Time
	sqlitePointerChange      int64
	sqlitePointerFingerprint string
	ownershipProbeStamp      string
	ownershipProbeCache      map[string]bool
}

func DefaultPath() (string, error) {
	path, err := appdirs.StatePath("teams", "state.json")
	if err != nil {
		return "", err
	}
	legacyPath, legacyErr := appdirs.LegacyConfigPath("teams", "state.json")
	if legacyErr != nil {
		return path, nil
	}
	return resolveMigratedDefaultPath(path, legacyPath), nil
}

// DefaultPathReadOnly returns the best existing default store path without
// migrating, copying, locking, chmodding, or otherwise mutating either path.
// It is intended for status and diagnostic commands.
func DefaultPathReadOnly() (string, error) {
	path, err := appdirs.StatePath("teams", "state.json")
	if err != nil {
		return "", err
	}
	if ok, statErr := storePathExists(path); statErr != nil {
		return "", statErr
	} else if ok {
		return path, nil
	}
	legacyPath, legacyErr := appdirs.LegacyConfigPath("teams", "state.json")
	if legacyErr != nil {
		return path, nil
	}
	if ok, statErr := storePathExists(legacyPath); statErr != nil {
		return "", statErr
	} else if ok {
		return legacyPath, nil
	}
	return path, nil
}

func resolveMigratedDefaultPath(path string, legacyPath string) string {
	if defaultPathMigrationComplete(path, legacyPath) {
		return path
	}
	if ok, err := storePathExists(legacyPath); err != nil || !ok {
		return path
	}
	lock := flock.New(legacyPath + ".lock")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil || !locked {
		return legacyPath
	}
	defer func() { _ = lock.Unlock() }()

	sqlitePath := filepath.Join(filepath.Dir(path), storeSQLiteFileName)
	legacySQLitePath := filepath.Join(filepath.Dir(legacyPath), storeSQLiteFileName)
	if resolved, err := appdirs.ResolveMigratedRelatedFiles(sqlitePath, legacySQLitePath, "-wal", "-shm"); err != nil || resolved != sqlitePath {
		return legacyPath
	}
	for _, name := range []string{
		"helper-restart-pending.json",
		"workflow-notifications.json",
		"workflow-webhook-url",
	} {
		legacySidecar := filepath.Join(filepath.Dir(legacyPath), name)
		if ok, err := storePathExists(legacySidecar); err != nil || !ok {
			continue
		}
		if err := appdirs.CopyFileIfMissing(filepath.Join(filepath.Dir(path), name), legacySidecar); err != nil {
			return legacyPath
		}
	}
	if err := appdirs.CopyFileReplacing(path, legacyPath); err != nil {
		return legacyPath
	}
	if !defaultPathMigrationComplete(path, legacyPath) {
		return legacyPath
	}
	return path
}

func defaultPathMigrationComplete(path string, legacyPath string) bool {
	if ok, err := storePathExists(path); err != nil || !ok {
		return false
	}
	if !defaultStorePathLoadable(path) {
		return false
	}
	if defaultPathStateNeedsRefresh(path, legacyPath) && defaultStorePathLoadable(legacyPath) {
		return false
	}
	sqlitePath := filepath.Join(filepath.Dir(path), storeSQLiteFileName)
	legacySQLitePath := filepath.Join(filepath.Dir(legacyPath), storeSQLiteFileName)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		legacyExists, legacyErr := storePathExists(legacySQLitePath + suffix)
		if legacyErr != nil {
			return false
		}
		if !legacyExists {
			continue
		}
		if ok, err := storePathExists(sqlitePath + suffix); err != nil || !ok {
			return false
		}
	}
	for _, name := range []string{
		"helper-restart-pending.json",
		"workflow-notifications.json",
		"workflow-webhook-url",
	} {
		legacySidecar := filepath.Join(filepath.Dir(legacyPath), name)
		legacyExists, legacyErr := storePathExists(legacySidecar)
		if legacyErr != nil {
			return false
		}
		if !legacyExists {
			continue
		}
		if ok, err := storePathExists(filepath.Join(filepath.Dir(path), name)); err != nil || !ok {
			return false
		}
	}
	return true
}

func defaultPathStateNeedsRefresh(path string, legacyPath string) bool {
	legacyInfo, legacyOK := storeRegularFileInfo(legacyPath)
	if !legacyOK {
		return false
	}
	newInfo, newOK := storeRegularFileInfo(path)
	if !newOK {
		return true
	}
	if os.SameFile(legacyInfo, newInfo) {
		return false
	}
	if legacyInfo.ModTime().After(newInfo.ModTime()) {
		return true
	}
	if legacyInfo.ModTime().Equal(newInfo.ModTime()) {
		sameContent, err := storeFileContentsEqual(legacyPath, path, legacyInfo, newInfo)
		return err != nil || !sameContent
	}
	return false
}

func storeRegularFileInfo(path string) (os.FileInfo, bool) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, false
	}
	return info, true
}

func storeFileContentsEqual(a string, b string, aInfo os.FileInfo, bInfo os.FileInfo) (bool, error) {
	if aInfo.Size() != bInfo.Size() {
		return false, nil
	}
	aBytes, err := os.ReadFile(a)
	if err != nil {
		return false, err
	}
	bBytes, err := os.ReadFile(b)
	if err != nil {
		return false, err
	}
	return string(aBytes) == string(bBytes), nil
}

func defaultStorePathLoadable(path string) bool {
	st, err := Open(path)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	_, loadErr := st.Load(ctx)
	cancel()
	closeErr := st.Close()
	return loadErr == nil && closeErr == nil
}

func storePathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
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

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sqliteDB == nil {
		return nil
	}
	err := s.sqliteDB.Close()
	s.sqliteDB = nil
	s.sqliteDBPath = ""
	return err
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Load(ctx context.Context) (State, error) {
	var state State
	err := s.withStateLock(ctx, func() error {
		var err error
		state, err = s.loadUnlocked(ctx)
		return err
	})
	return state, err
}

// LoadPathReadOnly loads a JSON or SQLite-backed store without taking the
// writable flock, creating lock files, running migrations, configuring WAL,
// or changing file permissions. Callers get a point-in-time diagnostic
// snapshot; normal runtime code should continue to use Store.Load.
func LoadPathReadOnly(ctx context.Context, path string) (State, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPathReadOnly()
		if err != nil {
			return State{}, err
		}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return State{}, err
	}
	if pointer, ok, err := storeSQLitePointerFromData(data); err != nil {
		return State{}, err
	} else if ok {
		store := &Store{path: path}
		dbPath, err := store.storeSQLitePath(pointer)
		if err != nil {
			return State{}, err
		}
		return loadSQLiteStateFileReadOnly(ctx, dbPath)
	}
	if backend, ok, err := unsupportedStateStorageBackendFromData(data); err != nil {
		return State{}, err
	} else if ok {
		return State{}, fmt.Errorf("unsupported teams store backend %q", backend)
	}
	return loadStateData(data)
}

// LoadPathOfflineRecoveryReadOnly loads a store after the caller has fenced
// every writer and acquired the store-family locks. Unlike LoadPathReadOnly,
// its SQLite connection uses mode=rw so SQLite may rebuild a missing SHM index
// for an existing WAL. Query-only mode is enabled before any state query; this
// function does not create schemas, checkpoint WAL, or take the Store flock.
//
// Runtime resolver, status, and doctor code must use LoadPathReadOnly instead.
func LoadPathOfflineRecoveryReadOnly(ctx context.Context, path string) (State, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPathReadOnly()
		if err != nil {
			return State{}, err
		}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return State{}, err
	}
	if pointer, ok, err := storeSQLitePointerFromData(data); err != nil {
		return State{}, err
	} else if ok {
		store := &Store{path: path}
		dbPath, err := store.storeSQLitePath(pointer)
		if err != nil {
			return State{}, err
		}
		return loadSQLiteStateFileOfflineRecoveryReadOnly(ctx, dbPath)
	}
	if backend, ok, err := unsupportedStateStorageBackendFromData(data); err != nil {
		return State{}, err
	} else if ok {
		return State{}, fmt.Errorf("unsupported teams store backend %q", backend)
	}
	return loadStateData(data)
}

// RuntimeMetadata is the bounded subset of a Teams store needed to identify a
// scope and determine whether a runtime writer may still be authoritative.
// It deliberately excludes sessions, turns, inbound events, outbox messages,
// and every other unbounded business-data collection.
type RuntimeMetadata struct {
	Scope          ScopeIdentity      `json:"scope"`
	ControlChat    ControlChatBinding `json:"control_chat"`
	ServiceOwner   *OwnerMetadata     `json:"service_owner,omitempty"`
	LockOwner      *OwnerMetadata     `json:"lock_owner,omitempty"`
	ControlLease   ControlLease       `json:"control_lease"`
	ServiceControl ServiceControl     `json:"service_control"`
}

// LoadPathRuntimeMetadataReadOnly reads only the bounded runtime metadata from
// a JSON or SQLite-backed store. It does not take the writable store flock,
// create lock files, migrate data, configure WAL, checkpoint, chmod, or load
// session/outbox tables.
func LoadPathRuntimeMetadataReadOnly(ctx context.Context, path string) (RuntimeMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPathReadOnly()
		if err != nil {
			return RuntimeMetadata{}, err
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return RuntimeMetadata{}, fmt.Errorf("teams runtime metadata path is not a regular file: %s", path)
	}
	if info.Size() <= maxStatePointerSize {
		data, err := os.ReadFile(path)
		if err != nil {
			return RuntimeMetadata{}, err
		}
		if pointer, ok, err := storeSQLitePointerFromData(data); err != nil {
			return RuntimeMetadata{}, err
		} else if ok {
			store := &Store{path: path}
			dbPath, err := store.storeSQLitePath(pointer)
			if err != nil {
				return RuntimeMetadata{}, err
			}
			return loadSQLiteRuntimeMetadataFileReadOnly(ctx, dbPath)
		}
		if backend, ok, err := unsupportedStateStorageBackendFromData(data); err != nil {
			return RuntimeMetadata{}, err
		} else if ok {
			return RuntimeMetadata{}, fmt.Errorf("unsupported teams store backend %q", backend)
		}
	}
	return loadJSONRuntimeMetadataReadOnly(ctx, path)
}

// LoadPathRuntimeMetadataOfflineRecoveryReadOnly is the bounded metadata
// counterpart of LoadPathOfflineRecoveryReadOnly. The caller must have fenced
// writers and acquired the store-family locks. SQLite may recreate SHM for an
// existing WAL, but query-only mode prevents application data changes.
func LoadPathRuntimeMetadataOfflineRecoveryReadOnly(ctx context.Context, path string) (RuntimeMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPathReadOnly()
		if err != nil {
			return RuntimeMetadata{}, err
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return RuntimeMetadata{}, fmt.Errorf("teams runtime metadata path is not a regular file: %s", path)
	}
	if info.Size() <= maxStatePointerSize {
		data, err := os.ReadFile(path)
		if err != nil {
			return RuntimeMetadata{}, err
		}
		if pointer, ok, err := storeSQLitePointerFromData(data); err != nil {
			return RuntimeMetadata{}, err
		} else if ok {
			store := &Store{path: path}
			dbPath, err := store.storeSQLitePath(pointer)
			if err != nil {
				return RuntimeMetadata{}, err
			}
			return loadSQLiteRuntimeMetadataFileOfflineRecoveryReadOnly(ctx, dbPath)
		}
		if backend, ok, err := unsupportedStateStorageBackendFromData(data); err != nil {
			return RuntimeMetadata{}, err
		} else if ok {
			return RuntimeMetadata{}, fmt.Errorf("unsupported teams store backend %q", backend)
		}
	}
	return loadJSONRuntimeMetadataReadOnly(ctx, path)
}

// LoadPathWatchdogStateReadOnly reads only the bounded state needed by the
// service watchdog. The returned State is a partial projection containing the
// control chat, owners, service control, upgrade request, and relevant chat
// poll state. It never loads sessions, turns, inbound events, outbox messages,
// or delivery records, and it performs no store writes.
func LoadPathWatchdogStateReadOnly(ctx context.Context, path string) (State, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPathReadOnly()
		if err != nil {
			return State{}, err
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		return State{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return State{}, fmt.Errorf("teams watchdog state path is not a regular file: %s", path)
	}
	if info.Size() <= maxStatePointerSize {
		data, err := os.ReadFile(path)
		if err != nil {
			return State{}, err
		}
		if pointer, ok, err := storeSQLitePointerFromData(data); err != nil {
			return State{}, err
		} else if ok {
			store := &Store{path: path}
			dbPath, err := store.storeSQLitePath(pointer)
			if err != nil {
				return State{}, err
			}
			return loadSQLiteWatchdogStateFileReadOnly(ctx, dbPath)
		}
		if backend, ok, err := unsupportedStateStorageBackendFromData(data); err != nil {
			return State{}, err
		} else if ok {
			return State{}, fmt.Errorf("unsupported teams store backend %q", backend)
		}
	}
	return loadJSONWatchdogStateReadOnly(ctx, path)
}

type runtimeMetadataContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r runtimeMetadataContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func loadJSONRuntimeMetadataReadOnly(ctx context.Context, path string) (RuntimeMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	defer f.Close()
	decoder := json.NewDecoder(runtimeMetadataContextReader{ctx: ctx, r: f})
	token, err := decoder.Token()
	if err != nil {
		return RuntimeMetadata{}, err
	}
	if token != json.Delim('{') {
		return RuntimeMetadata{}, fmt.Errorf("teams store root must be a JSON object")
	}
	var metadata RuntimeMetadata
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return RuntimeMetadata{}, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return RuntimeMetadata{}, fmt.Errorf("teams store metadata key is not a string")
		}
		switch key {
		case "scope":
			err = decoder.Decode(&metadata.Scope)
		case "control_chat":
			err = decoder.Decode(&metadata.ControlChat)
		case "service_owner":
			err = decoder.Decode(&metadata.ServiceOwner)
		case "lock_owner":
			err = decoder.Decode(&metadata.LockOwner)
		case "control_lease":
			err = decoder.Decode(&metadata.ControlLease)
		case "service_control":
			err = decoder.Decode(&metadata.ServiceControl)
		default:
			err = skipRuntimeMetadataJSONValue(decoder)
		}
		if err != nil {
			return RuntimeMetadata{}, err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return RuntimeMetadata{}, err
	}
	return metadata, nil
}

func loadJSONWatchdogStateReadOnly(ctx context.Context, path string) (State, error) {
	f, err := os.Open(path)
	if err != nil {
		return State{}, err
	}
	defer f.Close()
	decoder := json.NewDecoder(runtimeMetadataContextReader{ctx: ctx, r: f})
	token, err := decoder.Token()
	if err != nil {
		return State{}, err
	}
	if token != json.Delim('{') {
		return State{}, fmt.Errorf("teams store root must be a JSON object")
	}
	state := State{ChatPolls: make(map[string]ChatPollState)}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return State{}, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return State{}, fmt.Errorf("teams watchdog state key is not a string")
		}
		switch key {
		case "scope":
			err = decoder.Decode(&state.Scope)
		case "control_chat":
			err = decoder.Decode(&state.ControlChat)
		case "service_owner":
			err = decoder.Decode(&state.ServiceOwner)
		case "lock_owner":
			err = decoder.Decode(&state.LockOwner)
		case "service_control":
			err = decoder.Decode(&state.ServiceControl)
		case "upgrade":
			err = decoder.Decode(&state.Upgrade)
		case "chat_polls":
			err = decoder.Decode(&state.ChatPolls)
		default:
			err = skipRuntimeMetadataJSONValue(decoder)
		}
		if err != nil {
			return State{}, err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return State{}, err
	}
	return state, nil
}

func skipRuntimeMetadataJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		for decoder.More() {
			if _, err := decoder.Token(); err != nil {
				return err
			}
			if err := skipRuntimeMetadataJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := skipRuntimeMetadataJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	_, err = decoder.Token()
	return err
}

func (s *Store) LoadLegacyJSONState(ctx context.Context) (State, bool, error) {
	var state State
	legacy := false
	err := s.withStateLock(ctx, func() error {
		data, err := os.ReadFile(s.path)
		if errors.Is(err, os.ErrNotExist) {
			state = newState()
			legacy = true
			return nil
		}
		if err != nil {
			return err
		}
		if _, ok, err := storeSQLitePointerFromData(data); err != nil {
			return err
		} else if ok {
			return nil
		}
		if backend, ok, err := unsupportedStateStorageBackendFromData(data); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("unsupported teams store backend %q", backend)
		}
		state, err = loadStateData(data)
		if err != nil {
			return err
		}
		legacy = true
		return nil
	})
	return state, legacy, err
}

var (
	controlStateFields = stateFieldSet(
		"service_control",
	)
	upgradeStateFields = stateFieldSet(
		"upgrade",
	)
	upgradeBlockingStateFields   = stateFieldSet("turns", "outbox_messages", "chat_rate_limits")
	autoUpdateControlStateFields = stateFieldSet()
	pollStateSnapshotFields      = stateFieldSet(
		"sessions",
		"turns",
		"inbound_events",
		"chat_polls",
		"import_checkpoints",
		"service_owner",
	)
	pollScheduleSnapshotFields = stateFieldSet(
		"control_chat",
		"sessions",
		"turns",
		"chat_polls",
		"import_checkpoints",
		"service_owner",
	)
	hotPollScheduleSnapshotFields = stateFieldSet(
		"control_chat",
		"turns",
		"chat_polls",
		"import_checkpoints",
		"service_owner",
	)
	hotPollScheduleBaseFields = stateFieldSet(
		"control_chat",
		"chat_polls",
		"service_owner",
	)
	queuedTurnStateSnapshotFields = stateFieldSet(
		"sessions",
		"turns",
		"import_checkpoints",
		"service_owner",
	)
	transcriptImportStateSnapshotFields = stateFieldSet(
		"import_checkpoints",
		"service_owner",
	)
	sessionExecutionStateSnapshotFields = stateFieldSet(
		"turns",
		"import_checkpoints",
	)
	workflowNotificationStateSnapshotFields = stateFieldSet(
		"control_chat",
		"workflow",
	)
	workflowNotificationPendingStateFields = stateFieldSet(
		"notifications",
	)
	workflowEventStateSnapshotFields = stateFieldSet(
		"sessions",
		"turns",
		"inbound_events",
	)
	turnQueueStateSnapshotFields = stateFieldSet(
		"turns",
		"inbound_events",
	)
	transcriptDedupeSnapshotFields = stateFieldSet(
		"turns",
		"inbound_events",
		"outbox_messages",
		"transcript_deliveries",
		"helper_deliveries",
		"import_checkpoints",
		"service_owner",
	)
	deferredInboundStateFields = stateFieldSet(
		"inbound_events",
	)
	pendingOutboxStateFields = stateFieldSet(
		"outbox_messages",
		"chat_rate_limits",
	)
	outboxStateSnapshotFields = stateFieldSet(
		"outbox_messages",
	)
	chatPollStateFields = stateFieldSet(
		"chat_polls",
	)
	chatRateLimitStateFields = stateFieldSet(
		"chat_rate_limits",
	)
	forkOperationStateFields = stateFieldSet(
		"fork_operations",
	)
	forkCutoffStateFields = stateFieldSet(
		"fork_operations",
		"turns",
	)
	forkPollingStateFields = stateFieldSet(
		"fork_operations",
		"sessions",
		"chat_polls",
	)
)

func stateFieldSet(fields ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out[field] = struct{}{}
		}
	}
	return out
}

func filterTurnQueueSnapshotForSession(state State, sessionID string) State {
	sessionID = strings.TrimSpace(sessionID)
	out := State{SchemaVersion: SchemaVersion, Turns: map[string]Turn{}, InboundEvents: map[string]InboundEvent{}}
	for id, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != sessionID {
			continue
		}
		out.Turns[id] = turn
		if inboundID := strings.TrimSpace(turn.InboundEventID); inboundID != "" {
			if inbound, ok := state.InboundEvents[inboundID]; ok {
				out.InboundEvents[inboundID] = inbound
			}
		}
	}
	for id, inbound := range state.InboundEvents {
		if strings.TrimSpace(inbound.SessionID) == sessionID {
			out.InboundEvents[id] = inbound
		}
	}
	out.ensure(time.Time{})
	return out
}

func filterRecentSessionInboundTurnSnapshot(state State, sessionID string, since time.Time) State {
	sessionID = strings.TrimSpace(sessionID)
	out := State{SchemaVersion: SchemaVersion, Turns: map[string]Turn{}, InboundEvents: map[string]InboundEvent{}}
	for id, inbound := range state.InboundEvents {
		if strings.TrimSpace(inbound.SessionID) != sessionID {
			continue
		}
		if !since.IsZero() {
			activity := inboundStoreActivityTime(inbound)
			if activity.IsZero() || activity.Before(since) {
				continue
			}
		}
		out.InboundEvents[id] = inbound
		if turnID := strings.TrimSpace(inbound.TurnID); turnID != "" {
			if turn, ok := state.Turns[turnID]; ok {
				out.Turns[turnID] = turn
			}
		}
	}
	out.ensure(time.Time{})
	return out
}

func inboundStoreActivityTime(inbound InboundEvent) time.Time {
	if !inbound.ReceivedAt.IsZero() {
		return inbound.ReceivedAt
	}
	if !inbound.CreatedAt.IsZero() {
		return inbound.CreatedAt
	}
	return inbound.UpdatedAt
}

func filterActiveTurnQueueSnapshotForSession(state State, sessionID string) State {
	sessionID = strings.TrimSpace(sessionID)
	out := State{SchemaVersion: SchemaVersion, Turns: map[string]Turn{}, InboundEvents: map[string]InboundEvent{}}
	for id, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != sessionID {
			continue
		}
		switch turn.Status {
		case TurnStatusQueued, TurnStatusRunning:
		default:
			continue
		}
		out.Turns[id] = turn
		if inboundID := strings.TrimSpace(turn.InboundEventID); inboundID != "" {
			if inbound, ok := state.InboundEvents[inboundID]; ok {
				out.InboundEvents[inboundID] = inbound
			}
		}
	}
	out.ensure(time.Time{})
	return out
}

func filterWorkflowEventSnapshotForSession(state State, sessionID string) State {
	out := filterTurnQueueSnapshotForSession(state, sessionID)
	out.Sessions = map[string]SessionContext{}
	if session, ok := state.Sessions[sessionID]; ok {
		out.Sessions[sessionID] = session
	}
	out.ensure(time.Time{})
	return out
}

func filterTranscriptDedupeSnapshotForSession(state State, sessionID string, checkpointID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	checkpointID = strings.TrimSpace(checkpointID)
	out := State{
		SchemaVersion:        SchemaVersion,
		ServiceOwner:         state.ServiceOwner,
		Turns:                map[string]Turn{},
		InboundEvents:        map[string]InboundEvent{},
		OutboxMessages:       map[string]OutboxMessage{},
		TranscriptDeliveries: map[string]TranscriptDeliveryRecord{},
		HelperDeliveries:     map[string]HelperDeliveryRecord{},
		ImportCheckpoints:    map[string]ImportCheckpoint{},
	}
	for id, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) == sessionID {
			out.Turns[id] = turn
		}
	}
	for id, inbound := range state.InboundEvents {
		if strings.TrimSpace(inbound.SessionID) == sessionID {
			out.InboundEvents[id] = inbound
		}
	}
	for id, outbox := range state.OutboxMessages {
		if strings.TrimSpace(outbox.SessionID) == sessionID {
			out.OutboxMessages[id] = outbox
		}
	}
	for id, delivery := range state.TranscriptDeliveries {
		if strings.TrimSpace(delivery.SessionID) == sessionID {
			out.TranscriptDeliveries[id] = delivery
		}
	}
	for id, delivery := range state.HelperDeliveries {
		if strings.TrimSpace(delivery.SessionID) == sessionID {
			out.HelperDeliveries[id] = delivery
		}
	}
	for id, checkpoint := range state.ImportCheckpoints {
		checkpointKey := strings.TrimSpace(id)
		checkpointRowID := strings.TrimSpace(checkpoint.ID)
		if checkpointRowID != "" && checkpointRowID != checkpointKey {
			// A checkpoint whose map key and embedded ID disagree is corrupt.
			// Do not let an extra row with the requested session participate in
			// dedupe decisions merely because its SessionID happens to match.
			if checkpointKey == checkpointID {
				return State{}, fmt.Errorf("%w: checkpoint row id %q is keyed as %q", ErrSessionStateProvenanceMismatch, checkpointRowID, checkpointKey)
			}
			continue
		}
		if checkpointKey == checkpointID {
			if err := validateImportCheckpointProvenance(checkpoint, sessionID, checkpointID); err != nil {
				return State{}, err
			}
			out.ImportCheckpoints[checkpointKey] = checkpoint
			continue
		}
		if strings.TrimSpace(checkpoint.SessionID) == sessionID {
			out.ImportCheckpoints[checkpointKey] = checkpoint
		}
	}
	out.ensure(time.Time{})
	return out, nil
}

func filterWorkflowEventSnapshotForTurn(state State, sessionID string, turnID string) State {
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	out := State{
		SchemaVersion: SchemaVersion,
		Sessions:      map[string]SessionContext{},
		Turns:         map[string]Turn{},
		InboundEvents: map[string]InboundEvent{},
	}
	if sessionID != "" {
		if session, ok := state.Sessions[sessionID]; ok {
			out.Sessions[sessionID] = session
		}
	}
	if turnID != "" {
		if turn, ok := state.Turns[turnID]; ok && (sessionID == "" || strings.TrimSpace(turn.SessionID) == sessionID) {
			out.Turns[turn.ID] = turn
			if sessionID == "" {
				sessionID = strings.TrimSpace(turn.SessionID)
				if sessionID != "" {
					if session, ok := state.Sessions[sessionID]; ok {
						out.Sessions[sessionID] = session
					}
				}
			}
			if inboundID := strings.TrimSpace(turn.InboundEventID); inboundID != "" {
				if inbound, ok := state.InboundEvents[inboundID]; ok {
					out.InboundEvents[inboundID] = inbound
				}
			}
		}
	}
	out.ensure(time.Time{})
	return out
}

func filterThreadResolutionSnapshotForSession(state State, sessionID string) State {
	sessionID = strings.TrimSpace(sessionID)
	out := State{
		SchemaVersion: SchemaVersion,
		Sessions:      map[string]SessionContext{},
		Turns:         map[string]Turn{},
	}
	if sessionID != "" {
		if session, ok := state.Sessions[sessionID]; ok {
			out.Sessions[sessionID] = session
		}
	}
	for id, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) == sessionID {
			out.Turns[id] = turn
		}
	}
	out.ensure(time.Time{})
	return out
}

func (s *Store) PollStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, pollStateSnapshotFields)
}

func (s *Store) PollScheduleSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, pollScheduleSnapshotFields)
}

func (s *Store) HotPollScheduleState(ctx context.Context) (State, error) {
	if state, handled, err := s.hotPollScheduleStateSQLite(ctx); handled || err != nil {
		return state, err
	}
	return s.PollScheduleSnapshot(ctx)
}

func (s *Store) HotPollScheduleSnapshot(ctx context.Context) (State, map[string]bool, error) {
	if state, parkedSkip, handled, err := s.hotPollScheduleSnapshotSQLite(ctx); handled || err != nil {
		return state, parkedSkip, err
	}
	state, err := s.PollScheduleSnapshot(ctx)
	if err != nil {
		return State{}, nil, err
	}
	parkedSkip := make(map[string]bool)
	for chatID, poll := range state.ChatPolls {
		if chatPollParkedSkipEligible(poll) {
			parkedSkip[chatID] = true
		}
	}
	return state, parkedSkip, nil
}

func (s *Store) HotPollWorkCandidates(ctx context.Context, controlChatID string) ([]SessionContext, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return s.HotPollWorkCandidatesExcludingIdle(ctx, controlChatID, time.Time{})
}

func (s *Store) HotPollWorkCandidatesExcludingIdle(ctx context.Context, controlChatID string, idleBefore time.Time) ([]SessionContext, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return s.hotPollWorkCandidatesSQLite(ctx, controlChatID, idleBefore)
}

func (s *Store) IdleWorkChatParkCandidates(ctx context.Context, controlChatID string, idleBefore time.Time, limit int) ([]IdleWorkChatParkCandidate, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return s.idleWorkChatParkCandidatesSQLite(ctx, controlChatID, idleBefore, limit)
}

func (s *Store) SessionsByID(ctx context.Context, ids []string) (map[string]SessionContext, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cleaned := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		cleaned = append(cleaned, id)
	}
	out := make(map[string]SessionContext, len(cleaned))
	if len(cleaned) == 0 {
		return out, nil
	}
	if sessions, handled, err := s.sessionsByIDSQLite(ctx, cleaned); handled || err != nil {
		return sessions, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, stateFieldSet("sessions"))
	if err != nil {
		return nil, err
	}
	for _, id := range cleaned {
		if session, ok := state.Sessions[id]; ok {
			out[id] = session
		}
	}
	return out, nil
}

func (s *Store) HasSessions(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if hasSessions, handled, err := s.hasSessionsSQLite(ctx); handled || err != nil {
		return hasSessions, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, stateFieldSet("sessions"))
	if err != nil {
		return false, err
	}
	return len(state.Sessions) > 0, nil
}

func (s *Store) QueuedTurnStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, queuedTurnStateSnapshotFields)
}

func (s *Store) HasQueuedTurns(ctx context.Context) (bool, error) {
	if hasQueued, handled, err := s.hasQueuedTurnsSQLite(ctx); handled || err != nil {
		return hasQueued, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, stateFieldSet("turns"))
	if err != nil {
		return false, err
	}
	for _, turn := range state.Turns {
		if turn.Status == TurnStatusQueued {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) RunningTurnSessionIDs(ctx context.Context) (map[string]bool, error) {
	if running, handled, err := s.runningTurnSessionIDsSQLite(ctx); handled || err != nil {
		return running, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, stateFieldSet("turns"))
	if err != nil {
		return nil, err
	}
	running := make(map[string]bool)
	for _, turn := range state.Turns {
		sessionID := strings.TrimSpace(turn.SessionID)
		if turn.Status == TurnStatusRunning && sessionID != "" {
			running[sessionID] = true
		}
	}
	return running, nil
}

func (s *Store) HasPendingWorkflowNotifications(ctx context.Context) (bool, error) {
	if hasPending, handled, err := s.hasPendingWorkflowNotificationsSQLite(ctx); handled || err != nil {
		return hasPending, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, workflowNotificationPendingStateFields)
	if err != nil {
		return false, err
	}
	for _, rec := range state.Notifications {
		switch rec.Status {
		case NotificationStatusSent, NotificationStatusUnknown:
			continue
		default:
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) PendingWorkflowNotifications(ctx context.Context) ([]NotificationRecord, error) {
	if out, handled, err := s.pendingWorkflowNotificationsSQLite(ctx); handled || err != nil {
		return out, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, workflowNotificationPendingStateFields)
	if err != nil {
		return nil, err
	}
	out := pendingWorkflowNotificationsFromState(state)
	return out, nil
}

func pendingWorkflowNotificationsFromState(state State) []NotificationRecord {
	var out []NotificationRecord
	for _, rec := range state.Notifications {
		if !isPendingWorkflowNotification(rec) {
			continue
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func isPendingWorkflowNotification(rec NotificationRecord) bool {
	switch rec.Status {
	case NotificationStatusSent, NotificationStatusUnknown:
		return false
	case "", NotificationStatusQueued, NotificationStatusFailed, NotificationStatusSending:
		return true
	default:
		return false
	}
}

func (s *Store) UpdateNotification(ctx context.Context, id string, fn func(NotificationRecord, bool, time.Time) (NotificationRecord, bool, error)) (NotificationRecord, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return NotificationRecord{}, false, fmt.Errorf("notification id is required")
	}
	if fn == nil {
		return NotificationRecord{}, false, fmt.Errorf("notification update function is required")
	}
	if out, changed, handled, err := s.updateNotificationSQLite(ctx, id, fn); handled || err != nil {
		return out, changed, err
	}
	var out NotificationRecord
	changed := false
	err := s.UpdateIfChanged(ctx, func(state *State) (bool, error) {
		now := time.Now()
		if state.Notifications == nil {
			state.Notifications = make(map[string]NotificationRecord)
		}
		current, found := state.Notifications[id]
		next, updateChanged, err := fn(current, found, now)
		if err != nil {
			return false, err
		}
		out = next
		if !updateChanged {
			return false, nil
		}
		next.ID = id
		state.Notifications[next.ID] = next
		out = next
		changed = true
		return true, nil
	})
	return out, changed, err
}

func (s *Store) TranscriptImportStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, transcriptImportStateSnapshotFields)
}

// ImportCheckpointsForSessions returns only the canonical transcript
// checkpoint for each requested session.  Unlike TranscriptImportStateSnapshot
// it does not materialize every checkpoint in the store, which keeps an idle
// linked-transcript poll proportional to the sessions being inspected.
func (s *Store) ImportCheckpointsForSessions(ctx context.Context, sessionIDs []string) (map[string]ImportCheckpoint, error) {
	requested := make(map[string]string, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		sessionID = strings.TrimSpace(sessionID)
		if sessionID != "" {
			requested[transcriptCheckpointIDForSession(sessionID)] = sessionID
		}
	}
	out := make(map[string]ImportCheckpoint, len(requested))
	if len(requested) == 0 {
		return out, nil
	}
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			out, loadErr = s.loadSQLiteImportCheckpointsByIDsUnlocked(ctx, pointer, requested)
			return loadErr
		}
		data, err := os.ReadFile(s.path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		selected, ok, parseErr := loadImportCheckpointsByIDsData(data, requested)
		if parseErr != nil {
			return parseErr
		}
		if ok {
			out = selected
			return nil
		}
		state, loadErr := s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		for id := range requested {
			if checkpoint, found := state.ImportCheckpoints[id]; found {
				if err := validateImportCheckpointProvenance(checkpoint, requested[id], id); err != nil {
					return err
				}
				out[id] = checkpoint
			}
		}
		return nil
	})
	return out, err
}

// LinkedTranscriptSessionSnapshot reads the three pieces of durable state
// needed by the linked-transcript idle loop in one backend pass. It is
// intentionally scoped to the supplied sessions and never replaces the
// store-side ownership/CAS checks used when a result is committed.
func (s *Store) LinkedTranscriptSessionSnapshot(ctx context.Context, sessionIDs []string) (LinkedTranscriptSessionSnapshot, error) {
	requested := make(map[string]struct{}, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
			requested[sessionID] = struct{}{}
		}
	}
	out := LinkedTranscriptSessionSnapshot{
		Running:     make(map[string]bool, len(requested)),
		Checkpoints: make(map[string]ImportCheckpoint, len(requested)),
		Ownership:   make(map[string]bool, len(requested)),
	}
	for sessionID := range requested {
		out.Running[sessionID] = false
		out.Ownership[sessionID] = false
	}
	if len(requested) == 0 {
		return out, nil
	}
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			out, loadErr = s.loadSQLiteLinkedTranscriptSessionSnapshotUnlocked(ctx, pointer, requested)
			return loadErr
		}
		data, err := os.ReadFile(s.path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		selected, ok, parseErr := loadSelectedStateFieldsData(data, stateFieldSet("turns", "import_checkpoints"))
		if parseErr != nil {
			return parseErr
		}
		if !ok {
			selected, err = s.loadUnlocked(ctx)
			if err != nil {
				return err
			}
		}
		for sessionID := range requested {
			out.Running[sessionID] = false
			out.Ownership[sessionID] = false
			checkpointID := transcriptCheckpointIDForSession(sessionID)
			if checkpoint, found := selected.ImportCheckpoints[checkpointID]; found {
				if err := validateImportCheckpointProvenance(checkpoint, sessionID, checkpointID); err != nil {
					return err
				}
				out.Checkpoints[checkpointID] = checkpoint
				out.Ownership[sessionID] = importCheckpointHasUnresolvedExecution(checkpoint)
			}
		}
		for _, turn := range selected.Turns {
			sessionID := strings.TrimSpace(turn.SessionID)
			if _, wanted := requested[sessionID]; !wanted {
				continue
			}
			switch turn.Status {
			case TurnStatusRunning:
				out.Running[sessionID] = true
			case TurnStatusInterrupted:
				if isLegacyUnresolvedTurn(turn) {
					out.Ownership[sessionID] = true
				}
			}
		}
		return nil
	})
	return out, err
}

// LinkedTranscriptExecutionSnapshot reads only the running/interrupted turn
// rows needed by linked-transcript sessions that are not already proven idle.
// Keeping this separate from ImportCheckpointsForSessions avoids a full
// session-wide ownership query for an unchanged transcript poll.
func (s *Store) LinkedTranscriptExecutionSnapshot(ctx context.Context, sessionIDs []string) (LinkedTranscriptExecutionSnapshot, error) {
	requested := make(map[string]struct{}, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
			requested[sessionID] = struct{}{}
		}
	}
	out := LinkedTranscriptExecutionSnapshot{
		Running:   make(map[string]bool, len(requested)),
		Ownership: make(map[string]bool, len(requested)),
	}
	for sessionID := range requested {
		out.Running[sessionID] = false
		out.Ownership[sessionID] = false
	}
	if len(requested) == 0 {
		return out, nil
	}
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			out, loadErr = s.loadSQLiteLinkedTranscriptExecutionSnapshotUnlocked(ctx, pointer, requested)
			return loadErr
		}
		data, err := os.ReadFile(s.path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		selected, ok, parseErr := loadSelectedStateFieldsData(data, stateFieldSet("turns"))
		if parseErr != nil {
			return parseErr
		}
		if !ok {
			selected, err = s.loadUnlocked(ctx)
			if err != nil {
				return err
			}
		}
		for _, turn := range selected.Turns {
			sessionID := strings.TrimSpace(turn.SessionID)
			if _, wanted := requested[sessionID]; !wanted {
				continue
			}
			switch turn.Status {
			case TurnStatusRunning:
				out.Running[sessionID] = true
			case TurnStatusInterrupted:
				if isLegacyUnresolvedTurn(turn) {
					out.Ownership[sessionID] = true
				}
			}
		}
		return nil
	})
	return out, err
}

// SessionExecutionStateSnapshot returns only the durable Turn and transcript
// checkpoint rows needed to decide whether automatic transcript sync is safe
// for one session. SQLite uses session predicates so this hot path does not
// materialize turns or checkpoints belonging to unrelated sessions.
func (s *Store) SessionExecutionStateSnapshot(ctx context.Context, sessionID string, checkpointID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	checkpointID = strings.TrimSpace(checkpointID)
	if sessionID == "" {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteSessionExecutionStateUnlocked(ctx, pointer, sessionID, checkpointID)
			return loadErr
		}
		selected, ok, loadErr := s.loadSelectedStateFieldsUnlocked(ctx, sessionExecutionStateSnapshotFields)
		if loadErr != nil {
			return loadErr
		}
		if ok {
			var filterErr error
			state, filterErr = filterSessionExecutionState(selected, sessionID, checkpointID)
			if filterErr != nil {
				return filterErr
			}
			return nil
		}
		selected, loadErr = s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		var filterErr error
		state, filterErr = filterSessionExecutionState(selected, sessionID, checkpointID)
		if filterErr != nil {
			return filterErr
		}
		return nil
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

// SessionExecutionOwnershipProbe is the cheap idle-poll ownership guard. It
// returns only whether a canonical unresolved anchor or a pre-anchor legacy
// interrupted turn exists for this session; callers that need the full turn
// state use SessionExecutionStateSnapshot. Keeping this probe separate avoids
// decoding every matching Turn on every unchanged transcript poll.
func (s *Store) SessionExecutionOwnershipProbe(ctx context.Context, sessionID string, checkpointID string) (bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	checkpointID = strings.TrimSpace(checkpointID)
	if sessionID == "" {
		return false, nil
	}
	var unresolved bool
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var probeErr error
			unresolved, probeErr = s.loadSQLiteSessionExecutionOwnershipProbeUnlocked(ctx, pointer, sessionID, checkpointID)
			return probeErr
		}
		data, err := os.ReadFile(s.path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		parsed, ok, parseErr := loadSessionExecutionOwnershipProbeData(data, sessionID, checkpointID)
		if parseErr != nil {
			return parseErr
		}
		if ok {
			unresolved = parsed
			return nil
		}
		state, loadErr := s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		unresolved = stateHasUnresolvedExecution(&state, sessionID)
		return nil
	})
	return unresolved, err
}

// SessionExecutionOwnershipProbes performs the same cheap guard for a set of
// sessions in one store read. This is used by the linked-transcript idle loop:
// reading one JSON state file per session would still serialize a full-file
// read for every unchanged chat. SQLite keeps the query session-filtered.
func (s *Store) SessionExecutionOwnershipProbes(ctx context.Context, sessionIDs []string) (map[string]bool, error) {
	requested := make(map[string]struct{}, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
			requested[sessionID] = struct{}{}
		}
	}
	out := make(map[string]bool, len(requested))
	for sessionID := range requested {
		out[sessionID] = false
	}
	if len(requested) == 0 {
		return out, nil
	}
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			stamp := s.sessionExecutionOwnershipCacheStampUnlocked(pointer, true)
			if stamp != "" && stamp == s.ownershipProbeStamp && ownershipProbeCacheCovers(s.ownershipProbeCache, requested) {
				for sessionID := range requested {
					out[sessionID] = s.ownershipProbeCache[sessionID]
				}
				return nil
			}
			var probeErr error
			out, probeErr = s.loadSQLiteSessionExecutionOwnershipProbesUnlocked(ctx, pointer, requested)
			if probeErr == nil && stamp != "" {
				s.ownershipProbeStamp = stamp
				s.ownershipProbeCache = cloneOwnershipProbeResults(out)
			}
			return probeErr
		}
		stamp := s.sessionExecutionOwnershipCacheStampUnlocked(storeSQLitePointer{}, false)
		if stamp != "" && stamp == s.ownershipProbeStamp && ownershipProbeCacheCovers(s.ownershipProbeCache, requested) {
			for sessionID := range requested {
				out[sessionID] = s.ownershipProbeCache[sessionID]
			}
			return nil
		}
		data, err := os.ReadFile(s.path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		probes, ok, parseErr := loadSessionExecutionOwnershipProbesData(data, requested)
		if parseErr != nil {
			return parseErr
		}
		if ok {
			for sessionID := range requested {
				out[sessionID] = probes[sessionID]
			}
			if stamp != "" {
				s.ownershipProbeStamp = stamp
				s.ownershipProbeCache = cloneOwnershipProbeResults(out)
			}
			return nil
		}
		state, loadErr := s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		for sessionID := range requested {
			out[sessionID] = stateHasUnresolvedExecution(&state, sessionID)
		}
		if stamp != "" {
			s.ownershipProbeStamp = stamp
			s.ownershipProbeCache = cloneOwnershipProbeResults(out)
		}
		return nil
	})
	return out, err
}

func ownershipProbeCacheCovers(cache map[string]bool, requested map[string]struct{}) bool {
	if len(cache) == 0 && len(requested) > 0 {
		return false
	}
	for sessionID := range requested {
		if _, ok := cache[sessionID]; !ok {
			return false
		}
	}
	return true
}

func cloneOwnershipProbeResults(values map[string]bool) map[string]bool {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]bool, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func (s *Store) sessionExecutionOwnershipCacheStampUnlocked(pointer storeSQLitePointer, sqlite bool) string {
	if s == nil || strings.TrimSpace(s.path) == "" {
		return ""
	}
	paths := []string{s.path}
	if sqlite {
		// The pointer file is the authority for which SQLite database owns the
		// state. Include its content fingerprint and migration identity in the
		// cache key; size/mtime of the database alone cannot distinguish a
		// pointer switch to another database.
		if strings.TrimSpace(pointer.MigrationID) == "" || strings.TrimSpace(pointer.Path) == "" {
			return ""
		}
		pointerFingerprint := strings.TrimSpace(s.sqlitePointerFingerprint)
		if pointerFingerprint == "" {
			return ""
		}
		dbPath, err := s.storeSQLitePath(pointer)
		if err != nil {
			return ""
		}
		return fmt.Sprintf("sqlite:%s:%d:%s:%s:%s:%s:%s:%s", paths[0], pointer.StorageVersion, pointer.MigrationID, pointer.SourceSHA256, pointerFingerprint, dbPath, fileInfoStampForPath(dbPath), fileInfoStampForPath(dbPath+"-wal"))
	}
	parts := make([]string, 0, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			parts = append(parts, path+":missing")
			continue
		}
		if err != nil {
			return ""
		}
		parts = append(parts, fmt.Sprintf("%s:%d:%d:%d", path, info.Size(), info.ModTime().UnixNano(), fileInfoChangeTimeUnixNano(info)))
	}
	return strings.Join(parts, "|")
}

func fileInfoStampForPath(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "missing"
		}
		return "error"
	}
	return fmt.Sprintf("%d:%d:%d", info.Size(), info.ModTime().UnixNano(), fileInfoChangeTimeUnixNano(info))
}

func filterSessionExecutionState(state State, sessionID string, checkpointID string) (State, error) {
	out := State{
		SchemaVersion:     SchemaVersion,
		Turns:             map[string]Turn{},
		ImportCheckpoints: map[string]ImportCheckpoint{},
	}
	for id, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) == sessionID {
			out.Turns[id] = turn
		}
	}
	for id, checkpoint := range state.ImportCheckpoints {
		checkpointKey := strings.TrimSpace(id)
		checkpointRowID := strings.TrimSpace(checkpoint.ID)
		if checkpointRowID != "" && checkpointRowID != checkpointKey {
			if checkpointKey == checkpointID {
				return State{}, fmt.Errorf("%w: checkpoint row id %q is keyed as %q", ErrSessionStateProvenanceMismatch, checkpointRowID, checkpointKey)
			}
			continue
		}
		if checkpointKey == checkpointID {
			if err := validateImportCheckpointProvenance(checkpoint, sessionID, checkpointKey); err != nil {
				return State{}, err
			}
			out.ImportCheckpoints[checkpointKey] = checkpoint
			continue
		}
		if strings.TrimSpace(checkpoint.SessionID) == sessionID {
			out.ImportCheckpoints[checkpointKey] = checkpoint
		}
	}
	out.ensure(time.Time{})
	return out, nil
}

func validateImportCheckpointSession(checkpoint ImportCheckpoint, sessionID string, checkpointID string) error {
	want := strings.TrimSpace(sessionID)
	got := strings.TrimSpace(checkpoint.SessionID)
	if want == "" || got != want {
		return fmt.Errorf("%w: checkpoint %q belongs to session %q, requested %q", ErrSessionStateProvenanceMismatch, checkpointID, got, want)
	}
	return nil
}

// validateImportCheckpointProvenance is the store-level boundary for a
// checkpoint loaded by ID.  A row with a different embedded ID is as unsafe
// as a row belonging to a different session: accepting it would let a stale
// or corrupt JSON/SQLite row be written back under the requested checkpoint.
func validateImportCheckpointProvenance(checkpoint ImportCheckpoint, sessionID string, checkpointID string) error {
	checkpointID = strings.TrimSpace(checkpointID)
	if checkpointID == "" || strings.TrimSpace(checkpoint.ID) != checkpointID {
		return fmt.Errorf("%w: checkpoint row id %q does not match requested %q", ErrSessionStateProvenanceMismatch, checkpoint.ID, checkpointID)
	}
	return validateImportCheckpointSession(checkpoint, sessionID, checkpointID)
}

// validateImportCheckpointUpdateProvenance prevents a callback that looked up
// one checkpoint ID from silently rebinding a corrupt/foreign row to a new
// session.  UpdateImportCheckpoint is intentionally a generic narrow CAS API,
// so enforce the row identity at both JSON and SQLite implementations rather
// than relying on each bridge caller to pre-read the row.
func validateImportCheckpointUpdateProvenance(id string, current ImportCheckpoint, found bool, next ImportCheckpoint) error {
	id = strings.TrimSpace(id)
	// Validate the deterministic namespace even when this is a create.  A
	// missing row is not a reason to allow UpdateImportCheckpoint to create a
	// canonical transcript:<session> row owned by another session.
	if id == "" {
		return fmt.Errorf("%w: checkpoint id is empty", ErrSessionStateProvenanceMismatch)
	}
	if currentID := strings.TrimSpace(current.ID); currentID != "" && currentID != id {
		return fmt.Errorf("%w: checkpoint row id %q does not match requested %q", ErrSessionStateProvenanceMismatch, current.ID, id)
	}
	if nextID := strings.TrimSpace(next.ID); nextID != "" && nextID != id {
		return fmt.Errorf("%w: checkpoint update id %q does not match requested %q", ErrSessionStateProvenanceMismatch, next.ID, id)
	}
	currentSession := strings.TrimSpace(current.SessionID)
	nextSession := strings.TrimSpace(next.SessionID)
	if currentSession != "" && nextSession != currentSession {
		return fmt.Errorf("%w: checkpoint %q belongs to session %q, update requested %q", ErrSessionStateProvenanceMismatch, id, currentSession, nextSession)
	}
	// The primary transcript checkpoint has a stable ID namespace.  Do not let
	// a corrupt row keyed as transcript:s2 carry session s1 through a generic
	// update merely because the callback preserved the foreign embedded value.
	if strings.HasPrefix(id, "transcript:") {
		expectedSession := strings.TrimSpace(strings.TrimPrefix(id, "transcript:"))
		// Only the primary `transcript:<session>` namespace has a
		// deterministic session suffix.  Subagent checkpoints append
		// `:subagent:<id>` and publish-target checkpoints use a different
		// prefix; their embedded SessionID is intentionally the parent session.
		if expectedSession != "" && !strings.Contains(expectedSession, ":subagent:") {
			if currentSession != "" && currentSession != expectedSession {
				return fmt.Errorf("%w: checkpoint %q embeds session %q, key requires %q", ErrSessionStateProvenanceMismatch, id, currentSession, expectedSession)
			}
			if nextSession != "" && nextSession != expectedSession {
				return fmt.Errorf("%w: checkpoint %q update embeds session %q, key requires %q", ErrSessionStateProvenanceMismatch, id, nextSession, expectedSession)
			}
		}
	}
	return nil
}

// validateTranscriptCheckpointRecordProvenance is shared by the JSON and
// SQLite transcript bookkeeping paths.  These methods accept a caller-owned
// checkpoint rather than an already validated row, so both the incoming row
// and any row currently stored under its key must be checked before the
// cursor or ledger is advanced.  A missing SessionID remains compatible with
// old bookkeeping fixtures; once either side carries a session binding, the
// binding is immutable.
func validateTranscriptCheckpointRecordProvenance(state *State, checkpoint ImportCheckpoint) error {
	if state == nil {
		return nil
	}
	id := strings.TrimSpace(checkpoint.ID)
	if id == "" {
		// A skipped transcript delivery may intentionally carry no checkpoint;
		// it records dedupe/attention state without advancing a cursor.
		return nil
	}
	sessionID := strings.TrimSpace(checkpoint.SessionID)
	if sessionID != "" {
		if err := validateImportCheckpointProvenance(checkpoint, sessionID, id); err != nil {
			return err
		}
	}
	previous, found := state.ImportCheckpoints[id]
	if !found {
		return nil
	}
	previousSessionID := strings.TrimSpace(previous.SessionID)
	if previousSessionID != "" {
		if err := validateImportCheckpointProvenance(previous, previousSessionID, id); err != nil {
			return err
		}
	}
	if sessionID != "" && previousSessionID != "" && sessionID != previousSessionID {
		return fmt.Errorf("%w: checkpoint %q belongs to session %q, requested %q", ErrSessionStateProvenanceMismatch, id, previousSessionID, sessionID)
	}
	return nil
}

func validateLoadedTranscriptCheckpointRow(checkpoint ImportCheckpoint, checkpointID string, sessionID string) error {
	checkpointID = strings.TrimSpace(checkpointID)
	if checkpointID == "" {
		return nil
	}
	expectedSessionID := strings.TrimSpace(sessionID)
	if expectedSessionID == "" {
		expectedSessionID = strings.TrimSpace(checkpoint.SessionID)
	}
	if expectedSessionID == "" {
		if strings.TrimSpace(checkpoint.ID) != checkpointID {
			return fmt.Errorf("%w: checkpoint row id %q does not match requested %q", ErrSessionStateProvenanceMismatch, checkpoint.ID, checkpointID)
		}
		return nil
	}
	return validateImportCheckpointProvenance(checkpoint, expectedSessionID, checkpointID)
}

func validateTranscriptDeliveryCheckpointProvenance(delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint) error {
	deliverySessionID := strings.TrimSpace(delivery.SessionID)
	checkpointSessionID := strings.TrimSpace(checkpoint.SessionID)
	if deliverySessionID != "" && checkpointSessionID != "" && deliverySessionID != checkpointSessionID {
		return fmt.Errorf("%w: delivery session %q does not match checkpoint session %q", ErrSessionStateProvenanceMismatch, deliverySessionID, checkpointSessionID)
	}
	return nil
}

// validateQueuedTranscriptCheckpointProvenance checks a checkpoint snapshot
// supplied by a scanner against the canonical row in the same store update
// that queues the outbox.  The bridge-side read is only a preflight: another
// writer may replace the source or rebind the checkpoint before this call.
// Optional fields in the request are compared only when supplied, because
// explicit history/import callers intentionally pass an ID/session-only
// checkpoint while the canonical row contains the complete source metadata.
func validateQueuedTranscriptCheckpointProvenance(state *State, msg OutboxMessage, checkpoint ImportCheckpoint) error {
	if state == nil {
		return fmt.Errorf("state is required")
	}
	sessionID := strings.TrimSpace(msg.SessionID)
	checkpointID := strings.TrimSpace(msg.TranscriptCheckpointID)
	requestID := strings.TrimSpace(checkpoint.ID)
	if checkpointID == "" {
		checkpointID = requestID
	}
	if checkpointID == "" || sessionID == "" {
		return nil
	}
	if requestID != "" && requestID != checkpointID {
		return fmt.Errorf("%w: outbox checkpoint %q does not match request %q", ErrSessionStateProvenanceMismatch, requestID, checkpointID)
	}
	if requestSession := strings.TrimSpace(checkpoint.SessionID); requestSession != "" && requestSession != sessionID {
		return fmt.Errorf("%w: outbox checkpoint %q belongs to session %q, requested %q", ErrSessionStateProvenanceMismatch, checkpointID, requestSession, sessionID)
	}
	canonical, found := state.ImportCheckpoints[checkpointID]
	if !found {
		// A source-bound automatic row must have an authoritative checkpoint in
		// the same transaction.  If a concurrent writer deleted/replaced it,
		// continuing with the scanner's stale proof would reintroduce the exact
		// queue-after-scan race this validator is meant to close.  Keep the
		// source-less legacy compatibility path permissive until its durable
		// SourceRewriteBlocked marker is observed.
		if !outboxTurnIsExplicitHistory(msg.TurnID) && strings.TrimSpace(checkpoint.LastRecordID) != "" &&
			(strings.TrimSpace(checkpoint.SourcePath) != "" || strings.TrimSpace(checkpoint.SourceFingerprint) != "" || strings.TrimSpace(msg.TranscriptSourcePath) != "") {
			return fmt.Errorf("%w: checkpoint %q is missing for source-bound outbox", ErrSessionStateProvenanceMismatch, checkpointID)
		}
		return nil
	}
	if err := validateImportCheckpointProvenance(canonical, sessionID, checkpointID); err != nil {
		return err
	}
	if requestSession := strings.TrimSpace(checkpoint.SessionID); requestSession != "" && requestSession != strings.TrimSpace(canonical.SessionID) {
		return fmt.Errorf("%w: outbox checkpoint %q changed owner from %q to %q", ErrSessionStateProvenanceMismatch, checkpointID, canonical.SessionID, requestSession)
	}
	if sourcePath := strings.TrimSpace(checkpoint.SourcePath); sourcePath != "" && strings.TrimSpace(canonical.SourcePath) != "" && sourcePath != strings.TrimSpace(canonical.SourcePath) {
		return fmt.Errorf("%w: outbox checkpoint %q source changed from %q to %q", ErrSessionStateProvenanceMismatch, checkpointID, canonical.SourcePath, sourcePath)
	}
	sameCursor := checkpoint.LastOffsetKnown == canonical.LastOffsetKnown &&
		(!checkpoint.LastOffsetKnown || checkpoint.LastOffset == canonical.LastOffset)
	if proof := strings.TrimSpace(checkpoint.SourceFingerprint); proof != "" && strings.TrimSpace(canonical.SourceFingerprint) != "" && sameCursor && proof != strings.TrimSpace(canonical.SourceFingerprint) {
		return fmt.Errorf("%w: outbox checkpoint %q fingerprint changed", ErrSessionStateProvenanceMismatch, checkpointID)
	}
	return nil
}

func (s *Store) SessionTranscriptDedupeSnapshot(ctx context.Context, sessionID string, checkpointID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	checkpointID = strings.TrimSpace(checkpointID)
	if sessionID == "" {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteSessionTranscriptDedupeStateUnlocked(pointer, sessionID, checkpointID)
			return loadErr
		}
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, transcriptDedupeSnapshotFields)
		if err != nil {
			return err
		}
		if ok {
			var filterErr error
			state, filterErr = filterTranscriptDedupeSnapshotForSession(selected, sessionID, checkpointID)
			return filterErr
		}
		var loadErr error
		selected, loadErr = s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		state, err = filterTranscriptDedupeSnapshotForSession(selected, sessionID, checkpointID)
		return err
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) ImportCheckpoint(ctx context.Context, id string) (ImportCheckpoint, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ImportCheckpoint{}, false, nil
	}
	if checkpoint, found, handled, err := s.importCheckpointSQLite(ctx, id); handled || err != nil {
		return checkpoint, found, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, stateFieldSet("import_checkpoints"))
	if err != nil {
		return ImportCheckpoint{}, false, err
	}
	checkpoint, found := state.ImportCheckpoints[id]
	return checkpoint, found, nil
}

func (s *Store) UpdateImportCheckpoint(ctx context.Context, id string, fn func(ImportCheckpoint, bool, time.Time) (ImportCheckpoint, bool, error)) (ImportCheckpoint, bool, error) {
	return s.updateImportCheckpoint(ctx, "", id, fn)
}

// UpdateImportCheckpointIfParentUnfenced applies a checkpoint update only if
// the parent fork fence is still open in the same store transaction. It is
// intended for the background linked-transcript synchronizer; ordinary
// checkpoint callers retain the legacy behavior through UpdateImportCheckpoint.
func (s *Store) UpdateImportCheckpointIfParentUnfenced(ctx context.Context, parentSessionID string, id string, fn func(ImportCheckpoint, bool, time.Time) (ImportCheckpoint, bool, error)) (ImportCheckpoint, bool, error) {
	return s.updateImportCheckpoint(ctx, parentSessionID, id, fn)
}

func (s *Store) updateImportCheckpoint(ctx context.Context, parentSessionID string, id string, fn func(ImportCheckpoint, bool, time.Time) (ImportCheckpoint, bool, error)) (ImportCheckpoint, bool, error) {
	parentSessionID = strings.TrimSpace(parentSessionID)
	id = strings.TrimSpace(id)
	if id == "" {
		return ImportCheckpoint{}, false, fmt.Errorf("import checkpoint id is required")
	}
	if fn == nil {
		return ImportCheckpoint{}, false, fmt.Errorf("import checkpoint update function is required")
	}
	if out, changed, handled, err := s.updateImportCheckpointSQLite(ctx, parentSessionID, id, fn); handled || err != nil {
		return out, changed, err
	}
	var out ImportCheckpoint
	changed := false
	err := s.UpdateIfChanged(ctx, func(state *State) (bool, error) {
		if err := ensureParentUnfencedLocked(state, parentSessionID); err != nil {
			return false, err
		}
		now := time.Now()
		if state.ImportCheckpoints == nil {
			state.ImportCheckpoints = make(map[string]ImportCheckpoint)
		}
		current, found := state.ImportCheckpoints[id]
		next, updateChanged, err := fn(current, found, now)
		if err != nil {
			return false, err
		}
		if err := validateImportCheckpointUpdateProvenance(id, current, found, next); err != nil {
			return false, err
		}
		out = next
		if !updateChanged {
			return false, nil
		}
		next.ID = id
		state.ImportCheckpoints[next.ID] = next
		out = next
		changed = true
		return true, nil
	})
	return out, changed, err
}

func (s *Store) RecordTranscriptCheckpoint(ctx context.Context, checkpoint ImportCheckpoint, ledger TranscriptLedgerRecord) error {
	return s.recordTranscriptCheckpoint(ctx, "", checkpoint, ledger)
}

// RecordTranscriptCheckpointIfParentUnfenced atomically records the
// checkpoint and ledger only while the parent session is not fenced by an
// active fork. The fork fence is checked inside the same SQLite/JSON write
// critical section as the publication state.
func (s *Store) RecordTranscriptCheckpointIfParentUnfenced(ctx context.Context, parentSessionID string, checkpoint ImportCheckpoint, ledger TranscriptLedgerRecord) error {
	return s.recordTranscriptCheckpoint(ctx, parentSessionID, checkpoint, ledger)
}

func (s *Store) recordTranscriptCheckpoint(ctx context.Context, parentSessionID string, checkpoint ImportCheckpoint, ledger TranscriptLedgerRecord) error {
	parentSessionID = strings.TrimSpace(parentSessionID)
	checkpoint.ID = strings.TrimSpace(checkpoint.ID)
	checkpoint.SessionID = strings.TrimSpace(checkpoint.SessionID)
	checkpoint.LastRecordID = strings.TrimSpace(checkpoint.LastRecordID)
	ledger.ID = strings.TrimSpace(ledger.ID)
	if checkpoint.ID == "" {
		return fmt.Errorf("import checkpoint id is required")
	}
	if checkpoint.LastRecordID == "" {
		return nil
	}
	if ledger.ID == "" {
		return fmt.Errorf("transcript ledger id is required")
	}
	if handled, err := s.recordTranscriptCheckpointSQLite(ctx, parentSessionID, checkpoint, ledger); handled || err != nil {
		return err
	}
	update := s.Update
	if checkpoint.SessionID != "" {
		update = func(ctx context.Context, fn func(*State) error) error {
			return s.UpdateSession(ctx, checkpoint.SessionID, fn)
		}
	}
	return update(ctx, func(state *State) error {
		if err := ensureParentUnfencedLocked(state, parentSessionID); err != nil {
			return err
		}
		if err := validateTranscriptCheckpointRecordProvenance(state, checkpoint); err != nil {
			return err
		}
		previous, _ := state.ImportCheckpoints[checkpoint.ID]
		if stateHasUnresolvedExecution(state, checkpoint.SessionID) &&
			!importCheckpointIsExplicitHistoryRun(previous) {
			// Queueing a transcript delivery and advancing its cursor are separate
			// operations.  Once execution ownership becomes ambiguous, refuse the
			// cursor advance here as the final store-level barrier; otherwise a
			// background/import caller could consume quarantined records.
			return ErrUnresolvedExecution
		}
		applyRecordTranscriptCheckpointLocked(state, checkpoint, ledger, time.Now())
		return nil
	})
}

func applyRecordTranscriptCheckpointLocked(state *State, checkpoint ImportCheckpoint, ledger TranscriptLedgerRecord, now time.Time) (ImportCheckpoint, TranscriptLedgerRecord) {
	if state.ImportCheckpoints == nil {
		state.ImportCheckpoints = make(map[string]ImportCheckpoint)
	}
	if state.TranscriptLedger == nil {
		state.TranscriptLedger = make(map[string]TranscriptLedgerRecord)
	}
	id := strings.TrimSpace(checkpoint.ID)
	previous := state.ImportCheckpoints[id]
	status := previous.Status
	if status == "" {
		status = strings.TrimSpace(checkpoint.Status)
	}
	if status == "" || status == importCheckpointStatusBlocked {
		status = importCheckpointStatusComplete
	}
	if now.IsZero() {
		now = time.Now()
	}
	outCheckpoint := ImportCheckpoint{
		ID:                  id,
		SessionID:           firstStoreNonEmptyString(checkpoint.SessionID, previous.SessionID),
		SourcePath:          strings.TrimSpace(checkpoint.SourcePath),
		SourceFingerprint:   firstStoreNonEmptyString(checkpoint.SourceFingerprint, previous.SourceFingerprint),
		LastRecordID:        strings.TrimSpace(checkpoint.LastRecordID),
		LastSourceLine:      checkpoint.LastSourceLine,
		LastOffset:          firstStoreNonZeroInt64(checkpoint.LastOffset, previous.LastOffset),
		LastOffsetKnown:     checkpoint.LastOffsetKnown || previous.LastOffsetKnown || checkpoint.LastOffset != 0 || previous.LastOffset != 0,
		SourceSize:          checkpoint.SourceSize,
		SourceModTime:       checkpoint.SourceModTime,
		ImportTurnID:        previous.ImportTurnID,
		KindPrefix:          previous.KindPrefix,
		Status:              status,
		UnresolvedExecution: previous.UnresolvedExecution,
		// Preserve the monotonic generation even when a normal transcript
		// checkpoint update does not carry an anchor.  A late callback must not
		// be able to clear a recreated anchor after this reconstruction.
		ExecutionAnchorGeneration: maxStoreInt64(previous.ExecutionAnchorGeneration, checkpoint.ExecutionAnchorGeneration),
		UpdatedAt:                 now,
	}
	if checkpoint.UnresolvedExecution != nil {
		outCheckpoint.UnresolvedExecution = checkpoint.UnresolvedExecution
	}
	state.ImportCheckpoints[id] = outCheckpoint

	outLedger := ledger
	outLedger.ID = strings.TrimSpace(outLedger.ID)
	outLedger.SessionID = strings.TrimSpace(outLedger.SessionID)
	outLedger.CodexThreadID = strings.TrimSpace(outLedger.CodexThreadID)
	outLedger.SourcePath = strings.TrimSpace(outLedger.SourcePath)
	outLedger.SourceRecordID = strings.TrimSpace(outLedger.SourceRecordID)
	if outLedger.ImportedAt.IsZero() {
		outLedger.ImportedAt = now
	}
	if outLedger.CreatedAt.IsZero() {
		outLedger.CreatedAt = now
	}
	outLedger.UpdatedAt = now
	state.TranscriptLedger[outLedger.ID] = outLedger
	return outCheckpoint, outLedger
}

func firstStoreNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func maxStoreInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *Store) WorkflowNotificationStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, workflowNotificationStateSnapshotFields)
}

func (s *Store) WorkflowEventStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, workflowEventStateSnapshotFields)
}

func (s *Store) SessionWorkflowEventSnapshot(ctx context.Context, sessionID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return s.WorkflowEventStateSnapshot(ctx)
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteSessionTurnQueueStateUnlocked(pointer, sessionID, true)
			return loadErr
		}
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, workflowEventStateSnapshotFields)
		if err != nil {
			return err
		}
		if ok {
			state = filterWorkflowEventSnapshotForSession(selected, sessionID)
			return nil
		}
		var loadErr error
		selected, loadErr = s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		state = filterWorkflowEventSnapshotForSession(selected, sessionID)
		return nil
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) SessionWorkflowEventSnapshotForTurn(ctx context.Context, sessionID string, turnID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	if sessionID == "" {
		return s.WorkflowEventStateSnapshot(ctx)
	}
	if turnID == "" {
		return s.SessionWorkflowEventSnapshot(ctx, sessionID)
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteSessionWorkflowEventForTurnUnlocked(pointer, sessionID, turnID)
			return loadErr
		}
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, workflowEventStateSnapshotFields)
		if err != nil {
			return err
		}
		if ok {
			state = filterWorkflowEventSnapshotForTurn(selected, sessionID, turnID)
			return nil
		}
		var loadErr error
		selected, loadErr = s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		state = filterWorkflowEventSnapshotForTurn(selected, sessionID, turnID)
		return nil
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) SessionThreadResolutionSnapshot(ctx context.Context, sessionID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return s.WorkflowEventStateSnapshot(ctx)
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteSessionThreadResolutionStateUnlocked(pointer, sessionID)
			return loadErr
		}
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, workflowEventStateSnapshotFields)
		if err != nil {
			return err
		}
		if ok {
			state = filterThreadResolutionSnapshotForSession(selected, sessionID)
			return nil
		}
		var loadErr error
		selected, loadErr = s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		state = filterThreadResolutionSnapshotForSession(selected, sessionID)
		return nil
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) TurnQueueStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, turnQueueStateSnapshotFields)
}

func (s *Store) SessionTurnQueueSnapshot(ctx context.Context, sessionID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteSessionTurnQueueStateUnlocked(pointer, sessionID, false)
			return loadErr
		}
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, turnQueueStateSnapshotFields)
		if err != nil {
			return err
		}
		if ok {
			state = filterTurnQueueSnapshotForSession(selected, sessionID)
			return nil
		}
		var loadErr error
		selected, loadErr = s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		state = filterTurnQueueSnapshotForSession(selected, sessionID)
		return nil
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) RecentSessionInboundTurnSnapshot(ctx context.Context, sessionID string, since time.Time) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteRecentSessionInboundTurnStateUnlocked(pointer, sessionID, since)
			return loadErr
		}
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, turnQueueStateSnapshotFields)
		if err != nil {
			return err
		}
		if !ok {
			var loadErr error
			selected, loadErr = s.loadUnlocked(ctx)
			if loadErr != nil {
				return loadErr
			}
		}
		state = filterRecentSessionInboundTurnSnapshot(selected, sessionID, since)
		return nil
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) SessionActiveTurnQueueSnapshot(ctx context.Context, sessionID string) (State, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	var state State
	err := s.withStateLock(ctx, func() error {
		if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
			if err != nil {
				return err
			}
			var loadErr error
			state, loadErr = s.loadSQLiteSessionActiveTurnQueueStateUnlocked(ctx, pointer, sessionID)
			return loadErr
		}
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, turnQueueStateSnapshotFields)
		if err != nil {
			return err
		}
		if ok {
			state = filterActiveTurnQueueSnapshotForSession(selected, sessionID)
			return nil
		}
		var loadErr error
		selected, loadErr = s.loadUnlocked(ctx)
		if loadErr != nil {
			return loadErr
		}
		state = filterActiveTurnQueueSnapshotForSession(selected, sessionID)
		return nil
	})
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) TurnByID(ctx context.Context, turnID string) (Turn, bool, error) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return Turn{}, false, nil
	}
	if turn, ok, handled, err := s.turnByIDSQLite(ctx, turnID); handled || err != nil {
		return turn, ok, err
	}
	state, err := s.TurnQueueStateSnapshot(ctx)
	if err != nil {
		return Turn{}, false, err
	}
	turn, ok := state.Turns[turnID]
	return turn, ok, nil
}

func (s *Store) OutboxStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, outboxStateSnapshotFields)
}

// RecentOutboxEchoCandidates returns a strictly bounded set of outbox rows for
// rendered-content recovery. SQLite uses the existing
// (status, teams_chat_id, created_at, id) index and never materializes the full
// outbox. The JSON backend necessarily loads its single state file, but still
// caps the returned and decoded comparison set.
func (s *Store) RecentOutboxEchoCandidates(ctx context.Context, query OutboxEchoCandidateQuery) ([]OutboxMessage, error) {
	query.TeamsChatID = strings.TrimSpace(query.TeamsChatID)
	if query.TeamsChatID == "" {
		return nil, nil
	}
	if query.LimitPerStatus <= 0 {
		query.LimitPerStatus = 8
	}
	if query.LimitPerStatus > 32 {
		query.LimitPerStatus = 32
	}
	if messages, handled, err := s.recentOutboxEchoCandidatesSQLite(ctx, query); handled || err != nil {
		return messages, err
	}
	state, err := s.OutboxStateSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	byStatus := map[OutboxStatus][]OutboxMessage{
		OutboxStatusSending:  nil,
		OutboxStatusAccepted: nil,
		OutboxStatusSent:     nil,
	}
	for _, msg := range state.OutboxMessages {
		if strings.TrimSpace(msg.TeamsChatID) != query.TeamsChatID {
			continue
		}
		if _, ok := byStatus[msg.Status]; ok {
			byStatus[msg.Status] = append(byStatus[msg.Status], msg)
		}
	}
	var out []OutboxMessage
	for _, status := range []OutboxStatus{OutboxStatusSending, OutboxStatusAccepted, OutboxStatusSent} {
		messages := byStatus[status]
		sort.Slice(messages, func(i, j int) bool {
			if messages[i].CreatedAt.Equal(messages[j].CreatedAt) {
				if status == OutboxStatusSending {
					return messages[i].ID < messages[j].ID
				}
				return messages[i].ID > messages[j].ID
			}
			if status == OutboxStatusSending {
				return messages[i].CreatedAt.Before(messages[j].CreatedAt)
			}
			return messages[i].CreatedAt.After(messages[j].CreatedAt)
		})
		if len(messages) > query.LimitPerStatus {
			messages = messages[:query.LimitPerStatus]
		}
		out = append(out, messages...)
	}
	return out, nil
}

func (s *Store) SentOutboxMessagesForChat(ctx context.Context, chatID string) ([]OutboxMessage, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil, nil
	}
	if messages, handled, err := s.sentOutboxMessagesForChatSQLite(ctx, chatID); handled || err != nil {
		return messages, err
	}
	state, err := s.OutboxStateSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	messages := make([]OutboxMessage, 0)
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID == chatID && msg.Status == OutboxStatusSent {
			messages = append(messages, msg)
		}
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].CreatedAt.Equal(messages[j].CreatedAt) {
			return messages[i].ID < messages[j].ID
		}
		return messages[i].CreatedAt.Before(messages[j].CreatedAt)
	})
	return messages, nil
}

func (s *Store) OutboxMessageByID(ctx context.Context, outboxID string) (OutboxMessage, error) {
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" {
		return OutboxMessage{}, fmt.Errorf("outbox id is required")
	}
	state, err := s.OutboxStateSnapshot(ctx)
	if err != nil {
		return OutboxMessage{}, err
	}
	msg, ok := state.OutboxMessages[outboxID]
	if !ok {
		return OutboxMessage{}, fmt.Errorf("%w: %q", ErrOutboxNotFound, outboxID)
	}
	return msg, nil
}

// ForkPollingSnapshot returns only the durable fields needed to recover fork
// operations and poll staged child chats. SQLite callers avoid materializing
// unrelated outbox and cold history tables; legacy JSON stores fall back to
// the normal loader.
func (s *Store) ForkPollingSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, forkPollingStateFields)
}

func (s *Store) loadStateFieldsOrFull(ctx context.Context, wantedFields map[string]struct{}) (State, error) {
	var state State
	err := s.withStateLock(ctx, func() error {
		selected, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, wantedFields)
		if err != nil {
			return err
		}
		if ok {
			state = selected
			return nil
		}
		var loadErr error
		state, loadErr = s.loadUnlocked(ctx)
		return loadErr
	})
	return state, err
}

func (s *Store) loadSelectedStateFieldsUnlocked(ctx context.Context, wantedFields map[string]struct{}) (State, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil || ok {
		if err != nil {
			return State{}, false, err
		}
		state, err := s.loadSQLiteSelectedStateFieldsUnlocked(ctx, pointer, wantedFields)
		return state, true, err
	}
	if len(wantedFields) == 0 || !sessionExecutionStateFieldSet(wantedFields) {
		return State{}, false, nil
	}
	// Legacy JSON stores do not have a row-per-field backend, but ownership
	// snapshots only need a small set of top-level maps.  Decode those maps from
	// the current schema without unmarshalling unrelated outbox/history data.
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return newState(), true, nil
	}
	if err != nil {
		return State{}, false, err
	}
	selected, ok, err := loadSelectedStateFieldsData(data, wantedFields)
	return selected, ok, err
}

func sessionExecutionStateFieldSet(fields map[string]struct{}) bool {
	if len(fields) != 2 {
		return false
	}
	_, turns := fields["turns"]
	_, checkpoints := fields["import_checkpoints"]
	return turns && checkpoints
}

func loadImportCheckpointsByIDsData(data []byte, requested map[string]string) (map[string]ImportCheckpoint, bool, error) {
	selected := make(map[string]ImportCheckpoint, len(requested))
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return nil, false, nil
	}
	var schemaVersion int
	var backend string
	foundCheckpoints := false
	ok, err := scanJSONObjectEntries(data, func(key string, raw []byte) error {
		switch key {
		case "schema_version":
			return json.Unmarshal(raw, &schemaVersion)
		case "storage_backend":
			return json.Unmarshal(raw, &backend)
		case "import_checkpoints":
			foundCheckpoints = true
			nestedOK, nestedErr := scanJSONObjectEntries(raw, func(id string, row []byte) error {
				expectedSession, wanted := requested[id]
				if !wanted {
					return nil
				}
				var checkpoint ImportCheckpoint
				if err := json.Unmarshal(row, &checkpoint); err != nil {
					return err
				}
				if strings.TrimSpace(checkpoint.ID) == "" {
					checkpoint.ID = id
				}
				if err := validateImportCheckpointProvenance(checkpoint, expectedSession, id); err != nil {
					return err
				}
				selected[id] = checkpoint
				return nil
			})
			if nestedErr != nil {
				return nestedErr
			}
			if !nestedOK {
				return fmt.Errorf("invalid import_checkpoints JSON object")
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !ok || schemaVersion != SchemaVersion || (strings.TrimSpace(backend) != "" && strings.TrimSpace(backend) != storeSQLiteBackend) {
		return nil, false, nil
	}
	if !foundCheckpoints {
		return selected, true, nil
	}
	return selected, true, nil
}

func loadSelectedStateFieldsData(data []byte, wantedFields map[string]struct{}) (State, bool, error) {
	state := State{}
	value := reflect.ValueOf(&state).Elem()
	typeOfState := value.Type()
	fieldTargets := make(map[string]reflect.Value, len(wantedFields))
	for i := 0; i < typeOfState.NumField(); i++ {
		field := typeOfState.Field(i)
		jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonName == "" || jsonName == "-" {
			continue
		}
		if _, wanted := wantedFields[jsonName]; wanted {
			fieldValue := value.Field(i)
			if fieldValue.CanAddr() {
				fieldTargets[jsonName] = fieldValue
			}
		}
	}
	var schemaVersion int
	backend := ""
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return State{}, false, nil
	}
	i++
	for {
		i = skipJSONSpace(data, i)
		if i >= len(data) {
			return State{}, false, nil
		}
		if data[i] == '}' {
			i++
			break
		}
		keyEnd, ok := scanJSONStringEnd(data, i)
		if !ok {
			return State{}, false, nil
		}
		var key string
		if err := json.Unmarshal(data[i:keyEnd], &key); err != nil {
			return State{}, false, nil
		}
		i = skipJSONSpace(data, keyEnd)
		if i >= len(data) || data[i] != ':' {
			return State{}, false, nil
		}
		valueStart := skipJSONSpace(data, i+1)
		valueEnd, ok := scanJSONValueEnd(data, valueStart)
		if !ok {
			return State{}, false, nil
		}
		switch key {
		case "schema_version":
			if err := json.Unmarshal(data[valueStart:valueEnd], &schemaVersion); err != nil {
				return State{}, false, nil
			}
		case "storage_backend":
			if err := json.Unmarshal(data[valueStart:valueEnd], &backend); err != nil {
				return State{}, false, nil
			}
		default:
			if fieldValue, wanted := fieldTargets[key]; wanted {
				if err := json.Unmarshal(data[valueStart:valueEnd], fieldValue.Addr().Interface()); err != nil {
					return State{}, false, err
				}
			}
		}
		i = skipJSONSpace(data, valueEnd)
		if i >= len(data) {
			return State{}, false, nil
		}
		if data[i] == ',' {
			i++
			continue
		}
		if data[i] == '}' {
			i++
			break
		}
		return State{}, false, nil
	}
	if skipJSONSpace(data, i) != len(data) || schemaVersion != SchemaVersion {
		// Older schemas need the complete migration path before fields can be
		// selected safely.
		return State{}, false, nil
	}
	if strings.TrimSpace(backend) != "" && strings.TrimSpace(backend) != storeSQLiteBackend {
		return State{}, false, nil
	}
	state.SchemaVersion = schemaVersion
	normalizeLoadedState(&state)
	return state, true, nil
}

func loadSessionExecutionOwnershipProbeData(data []byte, sessionID string, checkpointID string) (bool, bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	checkpointID = strings.TrimSpace(checkpointID)
	if sessionID == "" {
		return false, false, nil
	}
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return false, false, nil
	}
	var schemaVersion int
	var backend string
	unresolved := false
	ok, err := scanJSONObjectEntries(data, func(key string, raw []byte) error {
		switch key {
		case "schema_version":
			return json.Unmarshal(raw, &schemaVersion)
		case "storage_backend":
			return json.Unmarshal(raw, &backend)
		case "import_checkpoints":
			nestedOK, nestedErr := scanJSONObjectEntries(raw, func(id string, row []byte) error {
				if unresolved || strings.TrimSpace(id) != checkpointID {
					return nil
				}
				var checkpoint ImportCheckpoint
				if err := json.Unmarshal(row, &checkpoint); err != nil {
					return err
				}
				if err := validateImportCheckpointSession(checkpoint, sessionID, id); err != nil {
					return err
				}
				unresolved = importCheckpointHasUnresolvedExecution(checkpoint)
				return nil
			})
			if nestedErr != nil {
				return nestedErr
			}
			if !nestedOK {
				return fmt.Errorf("invalid import_checkpoints JSON object")
			}
		case "turns":
			if unresolved {
				return nil
			}
			nestedOK, nestedErr := scanJSONObjectEntries(raw, func(_ string, row []byte) error {
				var meta struct {
					SessionID      string     `json:"session_id"`
					Status         TurnStatus `json:"status"`
					RecoveryReason string     `json:"recovery_reason"`
				}
				if err := json.Unmarshal(row, &meta); err != nil {
					return err
				}
				if strings.TrimSpace(meta.SessionID) == sessionID && meta.Status == TurnStatusInterrupted && isLegacyUnresolvedTurn(Turn{Status: meta.Status, RecoveryReason: meta.RecoveryReason}) {
					unresolved = true
				}
				return nil
			})
			if nestedErr != nil {
				return nestedErr
			}
			if !nestedOK {
				return fmt.Errorf("invalid turns JSON object")
			}
		}
		return nil
	})
	if err != nil {
		return false, false, err
	}
	if !ok || schemaVersion != SchemaVersion || (strings.TrimSpace(backend) != "" && strings.TrimSpace(backend) != storeSQLiteBackend) {
		return false, false, nil
	}
	return unresolved, true, nil
}

func loadSessionExecutionOwnershipProbesData(data []byte, requested map[string]struct{}) (map[string]bool, bool, error) {
	if len(requested) == 0 {
		return map[string]bool{}, true, nil
	}
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return nil, false, nil
	}
	var schemaVersion int
	var backend string
	out := make(map[string]bool, len(requested))
	checkpointOwners := make(map[string]string, len(requested))
	for sessionID := range requested {
		out[sessionID] = false
		checkpointOwners[sessionTranscriptCheckpointID(sessionID)] = sessionID
	}
	ok, err := scanJSONObjectEntries(data, func(key string, raw []byte) error {
		switch key {
		case "schema_version":
			return json.Unmarshal(raw, &schemaVersion)
		case "storage_backend":
			return json.Unmarshal(raw, &backend)
		case "import_checkpoints":
			nestedOK, nestedErr := scanJSONObjectEntries(raw, func(id string, row []byte) error {
				var checkpoint ImportCheckpoint
				if err := json.Unmarshal(row, &checkpoint); err != nil {
					return err
				}
				if strings.TrimSpace(checkpoint.ID) == "" {
					checkpoint.ID = id
				}
				sessionID := strings.TrimSpace(checkpoint.SessionID)
				if expectedSession, canonical := checkpointOwners[id]; canonical {
					if err := validateImportCheckpointProvenance(checkpoint, expectedSession, id); err != nil {
						return err
					}
				}
				if _, wanted := requested[sessionID]; !wanted {
					return nil
				}
				if err := validateImportCheckpointProvenance(checkpoint, sessionID, id); err != nil {
					return err
				}
				if importCheckpointHasUnresolvedExecution(checkpoint) {
					out[sessionID] = true
				}
				return nil
			})
			if nestedErr != nil {
				return nestedErr
			}
			if !nestedOK {
				return fmt.Errorf("invalid import_checkpoints JSON object")
			}
		case "turns":
			nestedOK, nestedErr := scanJSONObjectEntries(raw, func(_ string, row []byte) error {
				var meta struct {
					SessionID      string     `json:"session_id"`
					Status         TurnStatus `json:"status"`
					RecoveryReason string     `json:"recovery_reason"`
				}
				if err := json.Unmarshal(row, &meta); err != nil {
					return err
				}
				sessionID := strings.TrimSpace(meta.SessionID)
				if _, wanted := requested[sessionID]; wanted && isLegacyUnresolvedTurn(Turn{Status: meta.Status, RecoveryReason: meta.RecoveryReason}) {
					out[sessionID] = true
				}
				return nil
			})
			if nestedErr != nil {
				return nestedErr
			}
			if !nestedOK {
				return fmt.Errorf("invalid turns JSON object")
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !ok || schemaVersion != SchemaVersion || (strings.TrimSpace(backend) != "" && strings.TrimSpace(backend) != storeSQLiteBackend) {
		return nil, false, nil
	}
	return out, true, nil
}

// scanJSONObjectEntries calls fn for each raw value in a JSON object. It is
// intentionally bounded to one object level; nested values are skipped using
// scanJSONValueEnd without decoding them.
func scanJSONObjectEntries(data []byte, fn func(string, []byte) error) (bool, error) {
	i := skipJSONSpace(data, 0)
	if i < len(data) && data[i] == 'n' && strings.HasPrefix(string(data[i:]), "null") {
		return true, nil
	}
	if i >= len(data) || data[i] != '{' {
		return false, nil
	}
	i++
	for {
		i = skipJSONSpace(data, i)
		if i >= len(data) {
			return false, nil
		}
		if data[i] == '}' {
			return true, nil
		}
		keyEnd, ok := scanJSONStringEnd(data, i)
		if !ok {
			return false, nil
		}
		var key string
		if err := json.Unmarshal(data[i:keyEnd], &key); err != nil {
			return false, err
		}
		i = skipJSONSpace(data, keyEnd)
		if i >= len(data) || data[i] != ':' {
			return false, nil
		}
		valueStart := skipJSONSpace(data, i+1)
		valueEnd, ok := scanJSONValueEnd(data, valueStart)
		if !ok {
			return false, nil
		}
		if fn != nil {
			if err := fn(key, data[valueStart:valueEnd]); err != nil {
				return false, err
			}
		}
		i = skipJSONSpace(data, valueEnd)
		if i >= len(data) {
			return false, nil
		}
		if data[i] == ',' {
			i++
			continue
		}
		if data[i] == '}' {
			return true, nil
		}
		return false, nil
	}
}

func skipJSONSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

func scanJSONStringEnd(data []byte, start int) (int, bool) {
	if start >= len(data) || data[start] != '"' {
		return 0, false
	}
	escaped := false
	for i := start + 1; i < len(data); i++ {
		if escaped {
			escaped = false
			continue
		}
		switch data[i] {
		case '\\':
			escaped = true
		case '"':
			return i + 1, true
		}
	}
	return 0, false
}

func scanJSONValueEnd(data []byte, start int) (int, bool) {
	start = skipJSONSpace(data, start)
	if start >= len(data) {
		return 0, false
	}
	if data[start] == '"' {
		return scanJSONStringEnd(data, start)
	}
	if data[start] != '{' && data[start] != '[' {
		i := start
		for i < len(data) && data[i] != ',' && data[i] != '}' && data[i] != ']' {
			i++
		}
		end := i
		for end > start {
			switch data[end-1] {
			case ' ', '\t', '\r', '\n':
				end--
			default:
				return end, end > start
			}
		}
		return end, end > start
	}
	stack := []byte{data[start]}
	inString := false
	escaped := false
	for i := start + 1; i < len(data); i++ {
		c := data[i]
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			stack = append(stack, c)
		case '}', ']':
			if len(stack) == 0 || (c == '}' && stack[len(stack)-1] != '{') || (c == ']' && stack[len(stack)-1] != '[') {
				return 0, false
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return i + 1, true
			}
		}
	}
	return 0, false
}

func (s *Store) Update(ctx context.Context, fn func(*State) error) error {
	return s.withStateLock(ctx, func() error {
		state, err := s.loadUnlocked(ctx)
		if err != nil {
			return err
		}
		if err := fn(&state); err != nil {
			if errors.Is(err, errStoreNoChange) {
				return nil
			}
			return err
		}
		state.ensure(time.Now())
		if err := s.saveUnlocked(state); err != nil {
			s.invalidateMessageLookupCacheLocked()
			return err
		}
		if s.messageLookup.Valid {
			s.replaceMessageLookupCacheFromStateLocked(state)
		}
		return nil
	})
}

func (s *Store) UpdateIfChanged(ctx context.Context, fn func(*State) (bool, error)) error {
	return s.Update(ctx, func(state *State) error {
		changed, err := fn(state)
		if err != nil {
			return err
		}
		if !changed {
			return errStoreNoChange
		}
		return nil
	})
}

// UpdateHistoryWatch updates only cold history-watch metadata when the store is
// backed by SQLite. Legacy JSON stores still use the normal full-state update.
// An unchanged callback is a real no-op: history-watch polls are allowed to
// retry an incomplete tail, but they must not rewrite the durable store on
// every poll while the source file is unchanged.
func (s *Store) UpdateHistoryWatch(ctx context.Context, fn func(map[string]HistoryWatchCheckpoint, *time.Time) error) error {
	update := func(state *State) (bool, error) {
		before := cloneHistoryWatchCheckpoints(state.HistoryWatch)
		beforeReady := state.HistoryWatchReady
		if state.HistoryWatch == nil {
			state.HistoryWatch = make(map[string]HistoryWatchCheckpoint)
		}
		if err := fn(state.HistoryWatch, &state.HistoryWatchReady); err != nil {
			return false, err
		}
		return !historyWatchCheckpointsEqual(before, state.HistoryWatch) || !beforeReady.Equal(state.HistoryWatchReady), nil
	}
	if handled, err := s.updateSQLiteHistoryWatchIfChanged(ctx, fn); handled || err != nil {
		return err
	}
	return s.UpdateIfChanged(ctx, update)
}

// UpdateHistoryWatchCheckpointIfCurrent atomically replaces one checkpoint
// only when it still equals expected.  expected == nil means that the entry
// must not exist.  The comparison deliberately ignores UpdatedAt because it
// is an audit timestamp, not a source cursor.  This narrow CAS is used by the
// history watcher after scanning outside the store lock, so a slower watcher
// cannot overwrite a newer source cursor.
func (s *Store) UpdateHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *HistoryWatchCheckpoint, next HistoryWatchCheckpoint) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrHistoryWatchCheckpointConflict
	}
	next.ID = id
	update := func(history map[string]HistoryWatchCheckpoint, _ *time.Time) error {
		current, found := history[id]
		if expected == nil {
			if found {
				return ErrHistoryWatchCheckpointConflict
			}
		} else if !found || !historyWatchCheckpointEqual(current, *expected) {
			return ErrHistoryWatchCheckpointConflict
		}
		history[id] = next
		return nil
	}
	if handled, err := s.updateSQLiteHistoryWatchIfChanged(ctx, update); handled || err != nil {
		return err
	}
	return s.UpdateHistoryWatch(ctx, update)
}

// DeleteHistoryWatchCheckpointIfCurrent removes one checkpoint only when the
// caller's scan still owns the expected cursor. A stale source-missing scan
// must not delete a newer checkpoint written by another watcher.
func (s *Store) DeleteHistoryWatchCheckpointIfCurrent(ctx context.Context, id string, expected *HistoryWatchCheckpoint) error {
	id = strings.TrimSpace(id)
	if id == "" || expected == nil {
		return ErrHistoryWatchCheckpointConflict
	}
	update := func(history map[string]HistoryWatchCheckpoint, _ *time.Time) error {
		current, found := history[id]
		if !found || !historyWatchCheckpointEqual(current, *expected) {
			return ErrHistoryWatchCheckpointConflict
		}
		delete(history, id)
		return nil
	}
	if handled, err := s.updateSQLiteHistoryWatchIfChanged(ctx, update); handled || err != nil {
		return err
	}
	return s.UpdateHistoryWatch(ctx, update)
}

func cloneHistoryWatchCheckpoints(values map[string]HistoryWatchCheckpoint) map[string]HistoryWatchCheckpoint {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]HistoryWatchCheckpoint, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func historyWatchCheckpointsEqual(a, b map[string]HistoryWatchCheckpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for key, left := range a {
		right, ok := b[key]
		if !ok {
			return false
		}
		if !historyWatchCheckpointEqual(left, right) {
			return false
		}
	}
	return true
}

func historyWatchCheckpointEqual(a, b HistoryWatchCheckpoint) bool {
	// UpdatedAt is assigned by the watcher for every observed record. It is an
	// audit field, not source progress, and must not participate in the CAS.
	a.UpdatedAt = time.Time{}
	b.UpdatedAt = time.Time{}
	return reflect.DeepEqual(a, b)
}

// HistoryWatchState returns the state fields needed by the Codex history watch.
// SQLite stores load only cold metadata instead of materializing hot tables.
func (s *Store) HistoryWatchState(ctx context.Context) (State, error) {
	if state, handled, err := s.historyWatchStateSQLite(ctx); handled || err != nil {
		return state, err
	}
	state, err := s.Load(ctx)
	if err != nil {
		return State{}, err
	}
	return State{
		SchemaVersion:     SchemaVersion,
		HistoryWatch:      state.HistoryWatch,
		HistoryWatchReady: state.HistoryWatchReady,
	}, nil
}

// HistoryWatchOriginState returns only the session, turn, and inbound records
// needed to decide whether a Codex history prompt originated in Teams. SQLite
// stores avoid materializing outbox and delivery tables, and load inbound rows
// only for sessions associated with threadID. Legacy JSON stores still require
// one full JSON decode, but return the same narrow projection to callers.
func (s *Store) HistoryWatchOriginState(ctx context.Context, threadID string) (State, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return State{
			SchemaVersion: SchemaVersion,
			Sessions:      map[string]SessionContext{},
			Turns:         map[string]Turn{},
			InboundEvents: map[string]InboundEvent{},
		}, nil
	}
	if state, handled, err := s.historyWatchOriginStateSQLite(ctx, threadID); handled || err != nil {
		return state, err
	}
	state, err := s.Load(ctx)
	if err != nil {
		return State{}, err
	}
	return State{
		SchemaVersion: SchemaVersion,
		Sessions:      state.Sessions,
		Turns:         state.Turns,
		InboundEvents: state.InboundEvents,
	}, nil
}

func (s *Store) SetPaused(ctx context.Context, paused bool, reason string) (ServiceControl, error) {
	var out ServiceControl
	update := func(state *State) error {
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
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) SetDraining(ctx context.Context, reason string) (ServiceControl, error) {
	return s.setDrainingOperation(ctx, reason, "")
}

// SetDrainingOperation acquires a persisted maintenance fence. A different
// operation cannot replace the fence until its owner releases it explicitly.
func (s *Store) SetDrainingOperation(ctx context.Context, reason string, operationID string) (ServiceControl, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return ServiceControl{}, fmt.Errorf("drain operation id is required")
	}
	return s.setDrainingOperation(ctx, reason, operationID)
}

func (s *Store) setDrainingOperation(ctx context.Context, reason string, operationID string) (ServiceControl, error) {
	var out ServiceControl
	update := func(state *State) error {
		next := state.ServiceControl
		reason = strings.TrimSpace(reason)
		if next.Draining && next.DrainOperationID != operationID && (next.DrainOperationID != "" || operationID != "") {
			out = next
			return fmt.Errorf("%w: current=%q requested=%q", ErrDrainOperationConflict, next.DrainOperationID, operationID)
		}
		if next.Draining && next.Reason == reason && next.DrainOperationID == operationID {
			out = next
			return errStoreNoChange
		}
		now := time.Now()
		next.Draining = true
		next.Reason = reason
		next.DrainOperationID = operationID
		if operationID != "" {
			// Retain the last fenced maintenance operation after drain release so
			// a subsequent helper start does not select a richer stale mirror.
			next.LastDrainOperationID = operationID
			next.LastDrainOperationAt = now
		}
		next.UpdatedAt = now
		state.ServiceControl = next
		out = next
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) ClearDrain(ctx context.Context) (ServiceControl, error) {
	return s.clearDrainOperation(ctx, "", false)
}

// ClearDrainOperation releases a maintenance fence only when the operation ID
// still matches, preventing stale cleanup from clearing a newer drain.
func (s *Store) ClearDrainOperation(ctx context.Context, operationID string) (ServiceControl, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return ServiceControl{}, fmt.Errorf("drain operation id is required")
	}
	return s.clearDrainOperation(ctx, operationID, true)
}

func (s *Store) clearDrainOperation(ctx context.Context, operationID string, fenced bool) (ServiceControl, error) {
	var out ServiceControl
	update := func(state *State) error {
		next := state.ServiceControl
		if !next.Draining {
			out = next
			return errStoreNoChange
		}
		if fenced && next.DrainOperationID != operationID {
			out = next
			return fmt.Errorf("%w: current=%q requested=%q", ErrDrainOperationConflict, next.DrainOperationID, operationID)
		}
		next.Draining = false
		next.DrainOperationID = ""
		if !next.Paused {
			next.Reason = ""
		}
		next.UpdatedAt = time.Now()
		state.ServiceControl = next
		out = next
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) ReadControl(ctx context.Context) (ServiceControl, error) {
	state, err := s.loadStateFieldsOrFull(ctx, controlStateFields)
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
		if current.ID != "" && scopeClaimMatchesStored(current, scope) && !current.CreatedAt.IsZero() && !current.UpdatedAt.IsZero() {
			out = current
			return errStoreNoChange
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

// RebindScopeForMigration changes only persisted scope identity fields after an
// old scope-ID store has been copied into an unpublished migration target.
// Callers must hold the migration writer fence. Cross-account/profile changes
// are rejected.
func (s *Store) RebindScopeForMigration(ctx context.Context, scope ScopeIdentity) error {
	scope = normalizeScope(scope)
	if scope.ID == "" {
		return fmt.Errorf("scope id is required")
	}
	update := func(state *State) error {
		current := normalizeScope(state.Scope)
		compatible := func(accountID string, principal string, profile string) bool {
			profile = strings.TrimSpace(profile)
			if profile != "" && profile != scope.Profile {
				return false
			}
			sameAccount := strings.TrimSpace(accountID) != "" && scope.AccountID != "" &&
				strings.TrimSpace(accountID) == scope.AccountID
			samePrincipal := strings.TrimSpace(principal) != "" && scope.UserPrincipal != "" &&
				strings.EqualFold(strings.TrimSpace(principal), scope.UserPrincipal)
			return sameAccount || samePrincipal
		}
		if current.ID != "" && current.ID != scope.ID {
			if !compatible(current.AccountID, current.UserPrincipal, current.Profile) {
				return fmt.Errorf("Teams state belongs to scope %q, not migration target %q", current.ID, scope.ID)
			}
		}
		oldScopeIDs := map[string]bool{scope.ID: true}
		if current.ID != "" {
			oldScopeIDs[current.ID] = true
		}
		if scope.CreatedAt.IsZero() {
			scope.CreatedAt = current.CreatedAt
		}
		if scope.CreatedAt.IsZero() {
			scope.CreatedAt = time.Now()
		}
		scope.UpdatedAt = time.Now()
		state.Scope = scope
		if state.ControlChat.ScopeID != "" && !oldScopeIDs[state.ControlChat.ScopeID] {
			if !compatible(state.ControlChat.AccountID, "", state.ControlChat.Profile) {
				return fmt.Errorf("Teams control binding belongs to scope %q, not migration target %q", state.ControlChat.ScopeID, scope.ID)
			}
			oldScopeIDs[state.ControlChat.ScopeID] = true
		}
		if state.ControlChat.ScopeID == "" || oldScopeIDs[state.ControlChat.ScopeID] {
			state.ControlChat.ScopeID = scope.ID
		}
		if state.MachineIdentity.ScopeID != "" && !oldScopeIDs[state.MachineIdentity.ScopeID] {
			if !compatible(state.MachineIdentity.AccountID, state.MachineIdentity.UserPrincipal, state.MachineIdentity.Profile) {
				return fmt.Errorf("Teams machine identity belongs to scope %q, not migration target %q", state.MachineIdentity.ScopeID, scope.ID)
			}
			oldScopeIDs[state.MachineIdentity.ScopeID] = true
		}
		if state.MachineIdentity.ScopeID == "" || oldScopeIDs[state.MachineIdentity.ScopeID] {
			state.MachineIdentity.ScopeID = scope.ID
		}
		for id, machine := range state.Machines {
			if machine.ScopeID != "" && !oldScopeIDs[machine.ScopeID] {
				if !compatible(machine.AccountID, machine.UserPrincipal, machine.Profile) {
					return fmt.Errorf("Teams machine %q belongs to scope %q, not migration target %q", id, machine.ScopeID, scope.ID)
				}
				oldScopeIDs[machine.ScopeID] = true
			}
			if machine.ScopeID == "" || oldScopeIDs[machine.ScopeID] {
				machine.ScopeID = scope.ID
				state.Machines[id] = machine
			}
		}
		if state.ControlLease.ScopeID == "" || oldScopeIDs[state.ControlLease.ScopeID] {
			state.ControlLease.ScopeID = scope.ID
		} else if state.ControlLease.ScopeID != scope.ID {
			return fmt.Errorf("Teams control lease belongs to scope %q, not migration target %q", state.ControlLease.ScopeID, scope.ID)
		}
		return nil
	}
	if handled, err := s.rebindSQLiteScopeForMigration(ctx, update); handled || err != nil {
		return err
	}
	return s.Update(ctx, update)
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
	claim.Now = now
	if out, handled, err := s.claimControlLeaseSQLite(ctx, claim); handled || err != nil {
		return out, err
	}
	var out ControlLeaseDecision
	err := s.Update(ctx, func(state *State) error {
		decision, err := claimControlLeaseInState(state, claim)
		out = decision
		return err
	})
	return out, err
}

func claimControlLeaseInState(state *State, claim ControlLeaseClaim) (ControlLeaseDecision, error) {
	now := claim.Now
	if now.IsZero() {
		now = time.Now()
	}
	storedScope := state.Scope
	if state.Scope.ID != "" && state.Scope.ID != claim.Scope.ID {
		return ControlLeaseDecision{}, fmt.Errorf("Teams state belongs to scope %q, not %q", state.Scope.ID, claim.Scope.ID)
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
	liveLeaseOwner := false
	sameOwner := false
	protectedActiveTurn := false
	if owner, ok := state.readOwner(); ok {
		sameOwner = sameOwnerProcess(owner, claim.Owner)
		liveLeaseOwner = existingLive &&
			owner.MachineID == existing.HolderMachineID &&
			owner.LeaseGeneration == existing.Generation &&
			!IsStale(owner, claim.Duration, now) &&
			!OwnerAppearsLocallyDead(owner)
		protectedActiveTurn = liveLeaseOwner && owner.ActiveTurnID != ""
	}
	canClaim := !existingLive || sameHolder && (!liveLeaseOwner || sameOwner) || machine.Priority > existing.Priority && !liveLeaseOwner && !protectedActiveTurn
	if canClaim {
		if sameHolder && sameOwner && existing.Generation > 0 && existing.LeaseUntil.After(now.Add(claim.Duration/2)) &&
			scopeClaimMatchesStored(storedScope, claim.Scope) &&
			existingMachine.Status == MachineStatusActive &&
			machineClaimMatchesStored(existingMachine, machine) {
			holder := existingMachine
			if holder.ID == "" {
				holder = machine
			}
			holder.Status = MachineStatusActive
			return ControlLeaseDecision{Mode: LeaseModeActive, Lease: existing, Holder: holder}, errStoreNoChange
		}
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
		return ControlLeaseDecision{Mode: LeaseModeActive, Lease: existing, Holder: machine}, nil
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
	return ControlLeaseDecision{
		Mode:   LeaseModeStandby,
		Lease:  existing,
		Holder: holder,
		Reason: fmt.Sprintf("control lease is held by %s (%s)", existing.HolderMachineID, existing.HolderKind),
	}, nil
}

func scopeClaimMatchesStored(existing ScopeIdentity, claim ScopeIdentity) bool {
	return strings.TrimSpace(existing.ID) == strings.TrimSpace(claim.ID) &&
		strings.TrimSpace(existing.AccountID) == strings.TrimSpace(claim.AccountID) &&
		strings.TrimSpace(existing.UserPrincipal) == strings.TrimSpace(claim.UserPrincipal) &&
		strings.TrimSpace(existing.OSUser) == strings.TrimSpace(claim.OSUser) &&
		strings.TrimSpace(existing.Profile) == strings.TrimSpace(claim.Profile) &&
		strings.TrimSpace(existing.ConfigPath) == strings.TrimSpace(claim.ConfigPath) &&
		strings.TrimSpace(existing.CodexHome) == strings.TrimSpace(claim.CodexHome)
}

func machineClaimMatchesStored(existing MachineRecord, claim MachineRecord) bool {
	return strings.TrimSpace(existing.ID) == strings.TrimSpace(claim.ID) &&
		strings.TrimSpace(existing.ScopeID) == strings.TrimSpace(claim.ScopeID) &&
		strings.TrimSpace(existing.Label) == strings.TrimSpace(claim.Label) &&
		strings.TrimSpace(existing.Hostname) == strings.TrimSpace(claim.Hostname) &&
		strings.TrimSpace(existing.OSUser) == strings.TrimSpace(claim.OSUser) &&
		strings.TrimSpace(existing.AccountID) == strings.TrimSpace(claim.AccountID) &&
		strings.TrimSpace(existing.UserPrincipal) == strings.TrimSpace(claim.UserPrincipal) &&
		strings.TrimSpace(existing.Profile) == strings.TrimSpace(claim.Profile) &&
		existing.Kind == claim.Kind &&
		existing.Priority == claim.Priority
}

func (s *Store) ValidateControlLease(ctx context.Context, machineID string, generation int64, now time.Time) (ControlLease, error) {
	machineID = strings.TrimSpace(machineID)
	if machineID == "" || generation <= 0 {
		return ControlLease{}, ErrControlLeaseNotHeld
	}
	if now.IsZero() {
		now = time.Now()
	}
	if lease, handled, err := s.validateControlLeaseSQLite(ctx, machineID, generation, now); handled || err != nil {
		return lease, err
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
	update := func(state *State) error {
		now := time.Now()
		if activeUpgrade(state.Upgrade) {
			out = *state.Upgrade
			if out.Reason == reason {
				return nil
			}
			return ErrUpgradeInProgress
		}
		previous := state.ServiceControl
		if previous.Draining && previous.DrainOperationID != "" {
			return fmt.Errorf("%w: current=%q requested=upgrade", ErrDrainOperationConflict, previous.DrainOperationID)
		}
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
		control.DrainOperationID = ""
		control.UpdatedAt = now
		state.ServiceControl = control
		state.Upgrade = &req
		out = req
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) RescueForUpgrade(ctx context.Context, opts UpgradeRescueOptions) (UpgradeRescueReport, error) {
	reason := strings.TrimSpace(opts.Reason)
	if reason == "" {
		reason = HelperUpgradeReason
	}
	staleAfter := opts.StaleAfter
	if staleAfter <= 0 {
		staleAfter = 2 * time.Minute
	}
	var report UpgradeRescueReport
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		req := UpgradeRequest{}
		if activeUpgrade(state.Upgrade) {
			if state.Upgrade.Reason != reason {
				return ErrUpgradeInProgress
			}
			req = *state.Upgrade
		} else {
			previous := state.ServiceControl
			if previous.Draining && previous.DrainOperationID != "" {
				return fmt.Errorf("%w: current=%q requested=upgrade-rescue", ErrDrainOperationConflict, previous.DrainOperationID)
			}
			req = UpgradeRequest{
				ID:              upgradeID(reason, now),
				Phase:           UpgradePhaseRescuing,
				Reason:          reason,
				PreviousControl: previous,
				StartedAt:       now,
				UpdatedAt:       now,
			}
			control := previous
			control.Draining = true
			control.Reason = reason
			control.DrainOperationID = ""
			control.UpdatedAt = now
			state.ServiceControl = control
		}
		if req.Phase != UpgradePhaseCompleted && req.Phase != UpgradePhaseAborted {
			req.Phase = UpgradePhaseRescuing
		}
		if req.RescueStartedAt.IsZero() {
			req.RescueStartedAt = now
		}

		if owner, ok := state.readOwner(); ok {
			canFence := opts.ForceOwner || IsStale(owner, staleAfter, now) || OwnerAppearsLocallyDead(owner)
			if !canFence {
				report.SkippedBecauseOwnerIsActive = true
				return &OwnerConflictError{Existing: owner, Now: now, StaleAfter: staleAfter}
			}
			cleared := owner
			report.ClearedOwner = &cleared
			state.ServiceOwner = nil
			state.LockOwner = nil
			req.RescueActions = append(req.RescueActions, UpgradeRescueAction{
				Kind:      "clear-owner",
				ID:        owner.MachineID,
				SessionID: owner.ActiveSessionID,
				Detail:    owner.ActiveTurnID,
				CreatedAt: now,
			})
		}

		for id, turn := range state.Turns {
			switch turn.Status {
			case TurnStatusQueued:
				report.PreservedQueuedTurnIDs = append(report.PreservedQueuedTurnIDs, id)
				req.RescueActions = append(req.RescueActions, UpgradeRescueAction{
					Kind:      "preserve-queued-turn",
					ID:        id,
					SessionID: turn.SessionID,
					Status:    string(turn.Status),
					CreatedAt: now,
				})
			case TurnStatusRunning:
				turn.Status = TurnStatusInterrupted
				turn.InterruptedAt = now
				turn.RecoveryReason = "interrupted by helper upgrade rescue"
				turn.UpdatedAt = now
				state.Turns[id] = turn
				markInboundIgnoredForInterruptedTurn(state, turn, now)
				report.InterruptedTurnIDs = append(report.InterruptedTurnIDs, id)
				req.RescueActions = append(req.RescueActions, UpgradeRescueAction{
					Kind:      "interrupt-running-turn",
					ID:        id,
					SessionID: turn.SessionID,
					Status:    string(TurnStatusInterrupted),
					CreatedAt: now,
				})
			}
		}
		for id, msg := range state.OutboxMessages {
			if outboxDeliveryProtected(msg) {
				if OutboxBlocksUpgrade(*state, msg, now) {
					report.PreservedOutboxBlockerIDs = append(report.PreservedOutboxBlockerIDs, id)
					req.RescueActions = append(req.RescueActions, UpgradeRescueAction{
						Kind:      "preserve-protected-outbox",
						ID:        id,
						SessionID: msg.SessionID,
						Status:    string(msg.Status),
						Detail:    msg.Kind,
						CreatedAt: now,
					})
				}
				continue
			}
			if !outboxDeliveryTransient(msg) {
				continue
			}
			switch msg.Status {
			case OutboxStatusQueued, OutboxStatusSending:
				msg.Status = OutboxStatusSkipped
				msg.LastSendError = "superseded by helper upgrade rescue"
				msg.UpdatedAt = now
				state.OutboxMessages[id] = msg
				report.SupersededOutboxIDs = append(report.SupersededOutboxIDs, id)
				req.RescueActions = append(req.RescueActions, UpgradeRescueAction{
					Kind:      "skip-transient-outbox",
					ID:        id,
					SessionID: msg.SessionID,
					Status:    string(OutboxStatusSkipped),
					Detail:    msg.Kind,
					CreatedAt: now,
				})
			}
		}
		req.Phase = UpgradePhaseDraining
		req.RescueCompletedAt = now
		req.UpdatedAt = now
		state.Upgrade = &req
		report.Upgrade = req
		report.RemainingUpgradeBlockers = UpgradeBlockers(*state, now)
		sort.Strings(report.PreservedQueuedTurnIDs)
		sort.Strings(report.InterruptedTurnIDs)
		sort.Strings(report.SupersededOutboxIDs)
		sort.Strings(report.PreservedOutboxBlockerIDs)
		return nil
	})
	return report, err
}

func (s *Store) ReadUpgrade(ctx context.Context) (UpgradeRequest, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var out UpgradeRequest
	found := false
	err := s.withStateLock(ctx, func() error {
		if upgrade, foundRuntime, handled, err := s.readUpgradeSQLiteUnlocked(ctx); handled || err != nil {
			out = upgrade
			found = foundRuntime
			return err
		}
		state, ok, err := s.loadSelectedStateFieldsUnlocked(ctx, upgradeStateFields)
		if err != nil {
			return err
		}
		if !ok {
			state, err = s.loadUnlocked(ctx)
			if err != nil {
				return err
			}
		}
		if state.Upgrade == nil || state.Upgrade.ID == "" {
			return nil
		}
		out = *state.Upgrade
		found = true
		return nil
	})
	return out, found, err
}

func (s *Store) UpgradeBlockingStateSnapshot(ctx context.Context) (State, error) {
	return s.loadStateFieldsOrFull(ctx, upgradeBlockingStateFields)
}

func (s *Store) ReadAutoUpdate(ctx context.Context) (AutoUpdateState, error) {
	state, err := s.loadStateFieldsOrFull(ctx, autoUpdateControlStateFields)
	if err != nil {
		return AutoUpdateState{}, err
	}
	return state.AutoUpdate, nil
}

func (s *Store) ReadAutoUpdateControl(ctx context.Context) (AutoUpdateState, ServiceControl, error) {
	state, err := s.loadStateFieldsOrFull(ctx, autoUpdateControlStateFields)
	if err != nil {
		return AutoUpdateState{}, ServiceControl{}, err
	}
	return state.AutoUpdate, state.ServiceControl, nil
}

func (s *Store) RecordAutoUpdateCheck(ctx context.Context, record AutoUpdateRecord) (AutoUpdateState, error) {
	now := record.Now
	if now.IsZero() {
		now = time.Now()
	}
	var out AutoUpdateState
	update := func(state *State) error {
		next := state.AutoUpdate
		next.LastCheckAt = now
		next.NextCheckAt = record.NextCheckAt
		next.BackoffUntil = record.BackoffUntil
		next.LastError = trimDiagnostic(record.LastError, 240)
		if next.LastError != "" {
			next.LastErrorAt = now
		} else {
			next.LastErrorAt = time.Time{}
			next.LastSuccessAt = now
		}
		next.CandidateTag = strings.TrimSpace(record.CandidateTag)
		next.CandidateVersion = strings.TrimSpace(record.CandidateVersion)
		next.CandidatePriority = strings.TrimSpace(record.CandidatePriority)
		next.CandidateAsset = strings.TrimSpace(record.CandidateAsset)
		next.CandidatePublishedAt = record.CandidatePublishedAt
		next.CandidateEligibleAt = record.CandidateEligibleAt
		state.AutoUpdate = next
		out = next
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) RecordAutoUpdateAttempt(ctx context.Context, tag string, now time.Time) (AutoUpdateState, error) {
	if now.IsZero() {
		now = time.Now()
	}
	var out AutoUpdateState
	update := func(state *State) error {
		next := state.AutoUpdate
		next.LastAttemptTag = strings.TrimSpace(tag)
		next.LastAttemptAt = now
		state.AutoUpdate = next
		out = next
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) RecordAutoUpdateInstalled(ctx context.Context, tag string, now time.Time) (AutoUpdateState, error) {
	if now.IsZero() {
		now = time.Now()
	}
	var out AutoUpdateState
	update := func(state *State) error {
		next := state.AutoUpdate
		next.LastInstalledTag = strings.TrimSpace(tag)
		next.LastInstalledAt = now
		next.CandidateTag = ""
		next.CandidateVersion = ""
		next.CandidatePriority = ""
		next.CandidateAsset = ""
		next.CandidatePublishedAt = time.Time{}
		next.CandidateEligibleAt = time.Time{}
		state.AutoUpdate = next
		out = next
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
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

func (s *Store) AddUpgradeNotificationTarget(ctx context.Context, upgradeID string, target UpgradeNotificationTarget) (UpgradeRequest, error) {
	target.SessionID = strings.TrimSpace(target.SessionID)
	target.TurnID = strings.TrimSpace(target.TurnID)
	target.TeamsChatID = strings.TrimSpace(target.TeamsChatID)
	if target.TeamsChatID == "" {
		return UpgradeRequest{}, fmt.Errorf("upgrade notification target chat id is required")
	}
	return s.updateUpgrade(ctx, upgradeID, func(req UpgradeRequest, now time.Time) (UpgradeRequest, error) {
		if req.Phase == UpgradePhaseCompleted || req.Phase == UpgradePhaseAborted {
			return req, nil
		}
		if target.CreatedAt.IsZero() {
			target.CreatedAt = now
		}
		targetKey := upgradeNotificationTargetKey(target)
		for _, existing := range req.NotificationTargets {
			if upgradeNotificationTargetKey(existing) == targetKey {
				return req, nil
			}
		}
		req.NotificationTargets = append(req.NotificationTargets, target)
		return req, nil
	})
}

func (s *Store) CompleteUpgrade(ctx context.Context, upgradeID string, installedTag ...string) (UpgradeRequest, error) {
	tag := ""
	if len(installedTag) > 0 {
		tag = strings.TrimSpace(installedTag[0])
	}
	return s.updateUpgrade(ctx, upgradeID, func(req UpgradeRequest, now time.Time) (UpgradeRequest, error) {
		req.Phase = UpgradePhaseCompleted
		if req.CompletedAt.IsZero() {
			req.CompletedAt = now
		}
		if tag != "" {
			req.InstalledTag = tag
		}
		return req, nil
	})
}

func (s *Store) MarkUpgradeCompletionNoticeQueued(ctx context.Context, upgradeID string, outboxID string) (UpgradeRequest, error) {
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" {
		return UpgradeRequest{}, fmt.Errorf("completion notice outbox id is required")
	}
	return s.updateUpgrade(ctx, upgradeID, func(req UpgradeRequest, now time.Time) (UpgradeRequest, error) {
		if req.Phase != UpgradePhaseCompleted {
			return req, nil
		}
		if strings.TrimSpace(req.CompletionNoticeID) == "" {
			req.CompletionNoticeID = outboxID
		}
		if req.CompletionNoticeAt.IsZero() {
			req.CompletionNoticeAt = now
		}
		return req, nil
	})
}

func (s *Store) AbortUpgrade(ctx context.Context, upgradeID string, reason string) (UpgradeRequest, error) {
	return s.updateUpgrade(ctx, upgradeID, func(req UpgradeRequest, now time.Time) (UpgradeRequest, error) {
		req.Phase = UpgradePhaseAborted
		req.CompletedAt = time.Time{}
		req.AbortReason = trimDiagnostic(reason, 240)
		if req.AbortedAt.IsZero() {
			req.AbortedAt = now
		}
		return req, nil
	})
}

func (s *Store) AbortExpiredHelperUpgradeDrain(ctx context.Context, upgradeID string, now time.Time, ownerStaleAfter time.Duration, reason string) (UpgradeRequest, bool, error) {
	upgradeID = strings.TrimSpace(upgradeID)
	if upgradeID == "" {
		return UpgradeRequest{}, false, fmt.Errorf("upgrade id is required")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "helper upgrade drain expired; reconciled by watchdog"
	}
	if now.IsZero() {
		now = time.Now()
	}
	if ownerStaleAfter <= 0 {
		ownerStaleAfter = 5 * time.Minute
	}
	var out UpgradeRequest
	changed := false
	update := func(state *State) error {
		if !state.ServiceControl.Draining || state.ServiceControl.Reason != HelperUpgradeReason {
			return nil
		}
		if state.Upgrade == nil || state.Upgrade.ID != upgradeID || state.Upgrade.Reason != HelperUpgradeReason {
			return nil
		}
		if !activeUpgrade(state.Upgrade) || state.Upgrade.DeadlineAt.IsZero() || state.Upgrade.DeadlineAt.After(now) {
			return nil
		}
		if owner, ok := state.readOwner(); ok {
			fresh := !IsStale(owner, ownerStaleAfter, now) && !OwnerAppearsLocallyDead(owner)
			if fresh && (!OwnerAppearsLocal(owner) || strings.TrimSpace(owner.ActiveTurnID) != "") {
				return nil
			}
		}
		next := *state.Upgrade
		next.Phase = UpgradePhaseAborted
		next.AbortReason = trimDiagnostic(reason, 240)
		if next.AbortedAt.IsZero() {
			next.AbortedAt = now
		}
		next.UpdatedAt = now
		state.Upgrade = &next
		restoreUpgradeControl(state, next, now)
		out = next
		changed = true
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, changed, err
	}
	err := s.Update(ctx, update)
	return out, changed, err
}

func (s *Store) ClearStaleHelperReloadDrain(ctx context.Context, now time.Time, staleAfter time.Duration, ownerStaleAfter time.Duration) (ServiceControl, bool, error) {
	if now.IsZero() {
		now = time.Now()
	}
	if ownerStaleAfter <= 0 {
		ownerStaleAfter = 5 * time.Minute
	}
	var out ServiceControl
	changed := false
	update := func(state *State) error {
		if !HelperReloadDrainStale(*state, now, staleAfter) {
			out = state.ServiceControl
			return nil
		}
		if owner, ok := state.readOwner(); ok {
			fresh := !IsStale(owner, ownerStaleAfter, now) && !OwnerAppearsLocallyDead(owner)
			if fresh && (!OwnerAppearsLocal(owner) || strings.TrimSpace(owner.ActiveTurnID) != "") {
				out = state.ServiceControl
				return nil
			}
		}
		next := state.ServiceControl
		next.Draining = false
		next.DrainOperationID = ""
		if !next.Paused {
			next.Reason = ""
		}
		next.UpdatedAt = now
		state.ServiceControl = next
		out = next
		changed = true
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, changed, err
	}
	err := s.Update(ctx, update)
	return out, changed, err
}

func (s *Store) RestoreHelperReloadDrain(ctx context.Context, previous ServiceControl) error {
	update := func(state *State) error {
		current := state.ServiceControl
		if !current.Draining || current.Reason != HelperReloadReason {
			return errStoreNoChange
		}
		restored := previous
		restored.UpdatedAt = time.Now()
		state.ServiceControl = restored
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return err
	}
	return s.Update(ctx, update)
}

func upgradeNotificationTargetKey(target UpgradeNotificationTarget) string {
	chatID := strings.TrimSpace(target.TeamsChatID)
	turnID := strings.TrimSpace(target.TurnID)
	if turnID != "" {
		return chatID + "\x00turn\x00" + turnID
	}
	sessionID := strings.TrimSpace(target.SessionID)
	if sessionID != "" {
		return chatID + "\x00session\x00" + sessionID
	}
	return chatID
}

func CurrentOwner(helperVersion string, activeSessionID string, activeTurnID string, now time.Time) (OwnerMetadata, error) {
	if now.IsZero() {
		now = time.Now()
	}
	hostname, err := os.Hostname()
	if err != nil {
		return OwnerMetadata{}, err
	}
	rawExecutable, err := currentOwnerExecutable()
	if err != nil {
		return OwnerMetadata{}, err
	}
	executable := rawExecutable
	if resolved, resolveErr := helperpath.StableRunnablePathFromSources(rawExecutable, currentOwnerArgv0(), helperpath.Options{}); resolveErr == nil && strings.TrimSpace(resolved.Path) != "" {
		executable = resolved.Path
	} else if class := helperpath.ClassifyPath(rawExecutable); class.Transient {
		if !allowUnresolvedGoTestOwnerExecutable(class) {
			msg := fmt.Sprintf("cannot resolve stable owner executable from %q", rawExecutable)
			if resolveErr != nil {
				return OwnerMetadata{}, fmt.Errorf("%s: %w", msg, resolveErr)
			}
			return OwnerMetadata{}, errors.New(msg)
		}
	} else if strings.TrimSpace(class.Clean) != "" {
		executable = class.Clean
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

func allowUnresolvedGoTestOwnerExecutable(class helperpath.Classification) bool {
	base := strings.ToLower(strings.TrimSpace(class.Base))
	return class.Kind == helperpath.KindGoBuildTemp && (strings.HasSuffix(base, ".test") || strings.HasSuffix(base, ".test.exe"))
}

func (s *Store) RecordOwnerHeartbeat(ctx context.Context, owner OwnerMetadata, staleAfter time.Duration, now time.Time) (OwnerMetadata, error) {
	if out, handled, err := s.recordOwnerHeartbeatSQLite(ctx, owner, staleAfter, now); handled || err != nil {
		return out, err
	}
	var out OwnerMetadata
	err := s.Update(ctx, func(state *State) error {
		next, err := recordOwnerHeartbeatInState(state, owner, staleAfter, now)
		out = next
		return err
	})
	return out, err
}

func recordOwnerHeartbeatInState(state *State, owner OwnerMetadata, staleAfter time.Duration, now time.Time) (OwnerMetadata, error) {
	if now.IsZero() {
		now = time.Now()
	}
	next, err := owner.withHeartbeat(now)
	if err != nil {
		return OwnerMetadata{}, err
	}
	if existing, ok := state.readOwner(); ok {
		if sameOwnerProcess(existing, next) {
			if !existing.StartedAt.IsZero() {
				next.StartedAt = existing.StartedAt
			}
		} else if !IsStale(existing, staleAfter, now) && !OwnerAppearsLocallyDead(existing) {
			return OwnerMetadata{}, &OwnerConflictError{
				Existing:   existing,
				Now:        now,
				StaleAfter: staleAfter,
			}
		}
	}
	state.writeOwner(next)
	return next, nil
}

func (s *Store) ReadOwner(ctx context.Context) (OwnerMetadata, bool, error) {
	if owner, ok, handled, err := s.readOwnerSQLite(ctx); handled || err != nil {
		return owner, ok, err
	}
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
	update := func(state *State) error {
		existing, ok := state.readOwner()
		if !ok || !sameOwnerInstance(existing, owner) {
			return errStoreNoChange
		}
		state.ServiceOwner = nil
		state.LockOwner = nil
		cleared = true
		return nil
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return cleared, err
	}
	err := s.Update(ctx, update)
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
		case sameOwnerProcess(existing, next):
			if !existing.StartedAt.IsZero() {
				next.StartedAt = existing.StartedAt
			}
		case IsStale(existing, staleAfter, now):
			recovered = true
		case OwnerAppearsLocallyDead(existing):
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

func HelperUpgradeDrainExpired(state State, now time.Time) bool {
	if !state.ServiceControl.Draining || state.ServiceControl.Reason != HelperUpgradeReason {
		return false
	}
	if !activeUpgrade(state.Upgrade) || state.Upgrade.Reason != HelperUpgradeReason || state.Upgrade.DeadlineAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return !state.Upgrade.DeadlineAt.After(now)
}

func HelperReloadDrainStale(state State, now time.Time, staleAfter time.Duration) bool {
	if !state.ServiceControl.Draining || state.ServiceControl.Reason != HelperReloadReason {
		return false
	}
	if staleAfter <= 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	updatedAt := state.ServiceControl.UpdatedAt
	if updatedAt.IsZero() {
		return true
	}
	return !updatedAt.After(now) && now.Sub(updatedAt) > staleAfter
}

func OwnerAppearsLocal(owner OwnerMetadata) bool {
	if strings.TrimSpace(owner.Hostname) == "" {
		return false
	}
	hostname, err := ownerHostname()
	hostname = strings.TrimSpace(hostname)
	if err != nil || hostname == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(owner.Hostname), hostname)
}

func OwnerAppearsLocallyDead(owner OwnerMetadata) bool {
	if owner.PID <= 0 || strings.TrimSpace(owner.Hostname) == "" {
		return false
	}
	if !OwnerAppearsLocal(owner) {
		return false
	}
	return !ownerProcessAlive(owner.PID)
}

func (s *Store) UpdateSession(ctx context.Context, sessionID string, fn func(*State) error) error {
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("session id is required")
	}
	return s.withSessionLock(ctx, sessionID, func() error {
		return s.Update(ctx, fn)
	})
}

func (s *Store) UpdateSessionContext(ctx context.Context, sessionID string, fn func(SessionContext, bool, time.Time) (SessionContext, bool, error)) (SessionContext, bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return SessionContext{}, false, fmt.Errorf("session id is required")
	}
	if fn == nil {
		return SessionContext{}, false, fmt.Errorf("session update function is required")
	}
	var out SessionContext
	changed := false
	err := s.withSessionLock(ctx, sessionID, func() error {
		if next, updateChanged, handled, err := s.updateSessionContextSQLite(ctx, sessionID, fn); handled || err != nil {
			out = next
			changed = updateChanged
			return err
		}
		return s.Update(ctx, func(state *State) error {
			current, found := state.Sessions[sessionID]
			now := time.Now()
			next, updateChanged, err := fn(current, found, now)
			if err != nil {
				return err
			}
			out = next
			if !updateChanged {
				return errStoreNoChange
			}
			if strings.TrimSpace(next.ID) == "" {
				next.ID = sessionID
			}
			if next.UpdatedAt.IsZero() {
				next.UpdatedAt = now
			}
			state.Sessions[sessionID] = next
			out = next
			changed = true
			return nil
		})
	})
	return out, changed, err
}

func (s *Store) UpdateInboundEvent(ctx context.Context, inboundID string, fn func(InboundEvent, bool, time.Time) (InboundEvent, bool, error)) (InboundEvent, bool, error) {
	inboundID = strings.TrimSpace(inboundID)
	if inboundID == "" {
		return InboundEvent{}, false, fmt.Errorf("inbound id is required")
	}
	if fn == nil {
		return InboundEvent{}, false, fmt.Errorf("inbound update function is required")
	}
	if out, changed, handled, err := s.updateInboundEventSQLite(ctx, inboundID, fn); handled || err != nil {
		return out, changed, err
	}
	var out InboundEvent
	changed := false
	err := s.Update(ctx, func(state *State) error {
		current, found := state.InboundEvents[inboundID]
		now := time.Now()
		next, updateChanged, err := fn(current, found, now)
		if err != nil {
			return err
		}
		out = next
		if !updateChanged {
			return errStoreNoChange
		}
		if strings.TrimSpace(next.ID) == "" {
			next.ID = inboundID
		}
		if next.UpdatedAt.IsZero() {
			next.UpdatedAt = now
		}
		state.InboundEvents[inboundID] = next
		out = next
		changed = true
		return nil
	})
	return out, changed, err
}

type DashboardStoreRecords struct {
	Views      map[string]DashboardViewRecord
	Numbers    map[string]DashboardNumberRecord
	Workspaces map[string]WorkspaceRecord
}

func (s *Store) UpdateDashboardRecords(ctx context.Context, fn func(*DashboardStoreRecords, time.Time) (bool, error)) error {
	if fn == nil {
		return fmt.Errorf("dashboard update function is required")
	}
	update := func(state *State) error {
		state.ensure(time.Time{})
		records := DashboardStoreRecords{
			Views:      state.DashboardViews,
			Numbers:    state.DashboardNumbers,
			Workspaces: state.Workspaces,
		}
		changed, err := fn(&records, time.Now())
		if err != nil {
			return err
		}
		if !changed {
			return errStoreNoChange
		}
		state.DashboardViews = records.Views
		state.DashboardNumbers = records.Numbers
		state.Workspaces = records.Workspaces
		return nil
	}
	if handled, err := s.updateSQLiteColdState(ctx, update); handled || err != nil {
		return err
	}
	return s.Update(ctx, update)
}

func (s *Store) UpdateWorkflowConfig(ctx context.Context, fn func(WorkflowNotificationConfig, ControlChatBinding, time.Time) (WorkflowNotificationConfig, bool, error)) (WorkflowNotificationConfig, error) {
	if fn == nil {
		return WorkflowNotificationConfig{}, fmt.Errorf("workflow update function is required")
	}
	var out WorkflowNotificationConfig
	update := func(state *State) error {
		next, changed, err := fn(state.Workflow, state.ControlChat, time.Now())
		if err != nil {
			return err
		}
		out = next
		if !changed {
			return errStoreNoChange
		}
		state.Workflow = next
		return nil
	}
	if handled, err := s.updateSQLiteColdState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) UpsertModelProfileKeyIntake(ctx context.Context, intake ModelProfileKeyIntake) error {
	if strings.TrimSpace(intake.ID) == "" {
		return fmt.Errorf("model profile key intake id is required")
	}
	return s.UpdateModelProfileKeyIntakes(ctx, func(intakes map[string]ModelProfileKeyIntake, _ time.Time) (bool, error) {
		current, ok := intakes[intake.ID]
		if ok && reflect.DeepEqual(current, intake) {
			return false, nil
		}
		intakes[intake.ID] = intake
		return true, nil
	})
}

func (s *Store) UpdateModelProfileKeyIntakes(ctx context.Context, fn func(map[string]ModelProfileKeyIntake, time.Time) (bool, error)) error {
	if fn == nil {
		return fmt.Errorf("model profile key intake update function is required")
	}
	update := func(state *State) error {
		state.ensure(time.Time{})
		changed, err := fn(state.ModelProfileKeyIntakes, time.Now())
		if err != nil {
			return err
		}
		if !changed {
			return errStoreNoChange
		}
		return nil
	}
	if handled, err := s.updateSQLiteColdState(ctx, update); handled || err != nil {
		return err
	}
	return s.Update(ctx, update)
}

func (s *Store) RecordControlChatBinding(ctx context.Context, update ControlChatBindingUpdate) (bool, error) {
	update.TeamsChatID = strings.TrimSpace(update.TeamsChatID)
	if update.TeamsChatID == "" {
		return false, nil
	}
	if changed, handled, err := s.recordControlChatBindingSQLite(ctx, update); handled || err != nil {
		return changed, err
	}
	changed := false
	err := s.UpdateIfChanged(ctx, func(state *State) (bool, error) {
		changed = applyControlChatBindingUpdate(state, update, time.Now())
		return changed, nil
	})
	return changed, err
}

func applyControlChatBindingUpdate(state *State, update ControlChatBindingUpdate, now time.Time) bool {
	if state == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	changed := false
	machineChanged := false
	machineID := strings.TrimSpace(update.MachineID)
	if state.MachineIdentity.ID == "" && machineID != "" {
		state.MachineIdentity.ID = machineID
		state.MachineIdentity.CreatedAt = now
		changed = true
	}
	setMachineString := func(target *string, value string) {
		if *target != value {
			*target = value
			machineChanged = true
		}
	}
	setMachineString(&state.MachineIdentity.Label, update.MachineLabel)
	setMachineString(&state.MachineIdentity.Hostname, update.MachineHostname)
	setMachineString(&state.MachineIdentity.AccountID, update.AccountID)
	setMachineString(&state.MachineIdentity.UserPrincipal, update.UserPrincipal)
	setMachineString(&state.MachineIdentity.Profile, update.Profile)
	setMachineString(&state.MachineIdentity.ScopeID, update.ScopeID)
	if update.MachineKind != "" && state.MachineIdentity.Kind != update.MachineKind {
		state.MachineIdentity.Kind = update.MachineKind
		machineChanged = true
	}
	if state.MachineIdentity.Priority != update.MachinePriority {
		state.MachineIdentity.Priority = update.MachinePriority
		machineChanged = true
	}
	if state.MachineIdentity.UpdatedAt.IsZero() {
		machineChanged = true
	}
	if machineChanged {
		state.MachineIdentity.UpdatedAt = now
		changed = true
	}

	controlChanged := false
	if state.ControlChat.BoundAt.IsZero() {
		state.ControlChat.BoundAt = now
		controlChanged = true
	}
	if state.ControlChat.UpdatedAt.IsZero() {
		controlChanged = true
	}
	setControlString := func(target *string, value string) {
		if *target != value {
			*target = value
			controlChanged = true
		}
	}
	setControlString(&state.ControlChat.MachineID, machineID)
	setControlString(&state.ControlChat.ScopeID, update.ScopeID)
	setControlString(&state.ControlChat.AccountID, update.AccountID)
	setControlString(&state.ControlChat.TeamsChatID, update.TeamsChatID)
	setControlString(&state.ControlChat.TeamsChatURL, update.TeamsChatURL)
	setControlString(&state.ControlChat.TeamsChatTopic, update.TeamsChatTopic)
	if update.UpdateTitleIfPresent && (update.UserTitle != "" || update.TitleSource != "") {
		setControlString(&state.ControlChat.UserTitle, update.UserTitle)
		setControlString(&state.ControlChat.TitleSource, update.TitleSource)
	}
	if controlChanged {
		state.ControlChat.UpdatedAt = now
		changed = true
	}
	return changed
}

func (s *Store) BindSessionCodexThread(ctx context.Context, sessionID string, turnID string, threadID string) (SessionContext, bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	threadID = strings.TrimSpace(threadID)
	if sessionID == "" {
		return SessionContext{}, false, fmt.Errorf("session id is required")
	}
	if threadID == "" {
		return SessionContext{}, false, fmt.Errorf("codex thread id is required")
	}
	if out, changed, handled, err := s.bindSessionCodexThreadSQLite(ctx, sessionID, turnID, threadID); handled || err != nil {
		return out, changed, err
	}
	var out SessionContext
	changed := false
	err := s.UpdateSession(ctx, sessionID, func(state *State) error {
		durable := state.Sessions[sessionID]
		if durable.ID == "" {
			return fmt.Errorf("session %q not found", sessionID)
		}
		if existing := strings.TrimSpace(durable.CodexThreadID); existing != "" && existing != threadID {
			return CodexThreadBindingConflictError{SessionID: sessionID, Existing: existing, Observed: threadID}
		}
		var turn Turn
		turnOK := false
		if turnID != "" {
			loaded := state.Turns[turnID]
			if loaded.ID != "" && strings.TrimSpace(loaded.SessionID) == sessionID {
				if existing := strings.TrimSpace(loaded.CodexThreadID); existing != "" && existing != threadID {
					return CodexThreadBindingConflictError{SessionID: sessionID, Existing: existing, Observed: threadID}
				}
				turn = loaded
				turnOK = true
			}
		}
		sessionNeedsUpdate := strings.TrimSpace(durable.CodexThreadID) != threadID
		turnNeedsUpdate := turnOK && strings.TrimSpace(turn.CodexThreadID) != threadID
		out = durable
		if !sessionNeedsUpdate && !turnNeedsUpdate {
			return errStoreNoChange
		}
		now := time.Now()
		durable.CodexThreadID = threadID
		durable.UpdatedAt = now
		state.Sessions[sessionID] = durable
		if turnNeedsUpdate {
			turn.CodexThreadID = threadID
			turn.UpdatedAt = now
			state.Turns[turn.ID] = turn
		}
		out = durable
		changed = true
		return nil
	})
	return out, changed, err
}

func (s *Store) CreateSession(ctx context.Context, session SessionContext) (SessionContext, bool, error) {
	if strings.TrimSpace(session.ID) == "" {
		return SessionContext{}, false, fmt.Errorf("session id is required")
	}
	if out, created, handled, err := s.createSessionSQLite(ctx, session); handled || err != nil {
		return out, created, err
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
	if out, created, handled, err := s.persistInboundSQLite(ctx, event); handled || err != nil {
		return out, created, err
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
			return errStoreNoChange
		}
		if existing, ok := inboundEventByTeamsMessageLocked(state, event.TeamsChatID, event.TeamsMessageID); ok {
			out = existing
			return errStoreNoChange
		}
		if helperOutboxMessageLocked(state, event.TeamsChatID, event.TeamsMessageID) {
			return ErrInboundMessageFromHelperOutbox
		}
		if _, fenced := activeForkForSessionLocked(state, event.SessionID); fenced && event.Status != InboundStatusDeferred {
			return ErrForkParentFenced
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
		recordMessageProvenanceLocked(state, MessageProvenanceRecord{
			TeamsChatID:    event.TeamsChatID,
			TeamsMessageID: event.TeamsMessageID,
			Origin:         MessageOriginUserInbound,
			SessionID:      event.SessionID,
			TurnID:         event.TurnID,
			InboundID:      event.ID,
			Kind:           string(event.Status),
			RenderedHash:   event.TextHash,
			CreatedAt:      event.CreatedAt,
			UpdatedAt:      event.UpdatedAt,
		}, now)
		out = event
		created = true
		return nil
	})
	return out, created, err
}

func (s *Store) RecordMessageProvenance(ctx context.Context, record MessageProvenanceRecord) (MessageProvenanceRecord, error) {
	if out, handled, err := s.recordMessageProvenanceSQLite(ctx, record); handled || err != nil {
		return out, err
	}
	var out MessageProvenanceRecord
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		record.TeamsChatID = strings.TrimSpace(record.TeamsChatID)
		record.TeamsMessageID = strings.TrimSpace(record.TeamsMessageID)
		id := messageProvenanceID(record.TeamsChatID, record.TeamsMessageID)
		before, hadBefore := state.MessageProvenance[id]
		out = recordMessageProvenanceLocked(state, record, now)
		if out.ID == "" {
			return errStoreNoChange
		}
		if hadBefore {
			if after, ok := state.MessageProvenance[out.ID]; ok && messageProvenanceRecordEqual(after, before) {
				return errStoreNoChange
			}
		}
		return nil
	})
	return out, err
}

func messageProvenanceRecordEqual(left MessageProvenanceRecord, right MessageProvenanceRecord) bool {
	return left.ID == right.ID &&
		left.TeamsChatID == right.TeamsChatID &&
		left.TeamsMessageID == right.TeamsMessageID &&
		left.Origin == right.Origin &&
		left.SessionID == right.SessionID &&
		left.TurnID == right.TurnID &&
		left.OutboxID == right.OutboxID &&
		left.InboundID == right.InboundID &&
		left.Kind == right.Kind &&
		left.RenderedHash == right.RenderedHash &&
		left.Diagnostic == right.Diagnostic &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}

func recordOutboxProvenanceLocked(state *State, msg OutboxMessage, now time.Time) MessageProvenanceRecord {
	if strings.TrimSpace(msg.TeamsChatID) == "" || strings.TrimSpace(msg.TeamsMessageID) == "" {
		return MessageProvenanceRecord{}
	}
	return recordMessageProvenanceLocked(state, MessageProvenanceRecord{
		TeamsChatID:    msg.TeamsChatID,
		TeamsMessageID: msg.TeamsMessageID,
		Origin:         MessageOriginHelperOutbox,
		SessionID:      msg.SessionID,
		TurnID:         msg.TurnID,
		OutboxID:       msg.ID,
		Kind:           msg.Kind,
		RenderedHash:   msg.RenderedHash,
		CreatedAt:      msg.CreatedAt,
		UpdatedAt:      firstStoreNonZeroTime(msg.SentAt, msg.UpdatedAt, msg.CreatedAt),
	}, now)
}

func (s *Store) MessageProvenance(ctx context.Context, chatID string, teamsMessageID string) (MessageProvenanceRecord, bool, error) {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return MessageProvenanceRecord{}, false, nil
	}
	lookup, err := s.MessageLookup(ctx, chatID, teamsMessageID)
	if err != nil {
		return MessageProvenanceRecord{}, false, err
	}
	return lookup.Provenance, lookup.HasProvenance, nil
}

func (s *Store) MessageLookup(ctx context.Context, chatID string, teamsMessageID string) (MessageLookup, error) {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return MessageLookup{}, nil
	}
	stamp, err := stateFileStampForPath(s.path)
	if err != nil {
		return MessageLookup{}, err
	}
	if stamp.Exists && stamp.Size <= maxStatePointerSize {
		if out, handled, err := s.messageLookupSQLite(ctx, chatID, teamsMessageID); handled || err != nil {
			return out, err
		}
	}
	s.mu.Lock()
	if lookup, ok := s.messageLookup.lookup(stamp, chatID, teamsMessageID); ok {
		s.mu.Unlock()
		return lookup, nil
	}
	s.mu.Unlock()
	var out MessageLookup
	err = s.withStateLock(ctx, func() error {
		stamp, err := stateFileStampForPath(s.path)
		if err != nil {
			return err
		}
		if lookup, ok := s.messageLookup.lookup(stamp, chatID, teamsMessageID); ok {
			out = lookup
			return nil
		}
		state, err := s.loadUnlocked(ctx)
		if err != nil {
			s.invalidateMessageLookupCacheLocked()
			return err
		}
		s.replaceMessageLookupCacheFromStateLocked(state)
		if lookup, ok := s.messageLookup.lookup(s.messageLookup.Stamp, chatID, teamsMessageID); ok {
			out = lookup
			return nil
		}
		out = messageLookupLocked(&state, chatID, teamsMessageID)
		return nil
	})
	if err != nil {
		return MessageLookup{}, err
	}
	return out, nil
}

func (s *Store) HasInboundMessage(ctx context.Context, chatID string, teamsMessageID string) (bool, error) {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return false, nil
	}
	lookup, err := s.MessageLookup(ctx, chatID, teamsMessageID)
	if err != nil {
		return false, err
	}
	return lookup.HasInbound, nil
}

func (s *Store) DeferredInbound(ctx context.Context) ([]InboundEvent, error) {
	if out, handled, err := s.deferredInboundSQLite(ctx); handled || err != nil {
		return out, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, deferredInboundStateFields)
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
	return len(UpgradeBlockers(state, now)) > 0
}

func UpgradeBlockers(state State, now time.Time) []UpgradeBlocker {
	if now.IsZero() {
		now = time.Now()
	}
	var blockers []UpgradeBlocker
	for _, turn := range state.Turns {
		if turn.Status == TurnStatusQueued || turn.Status == TurnStatusRunning {
			blockers = append(blockers, UpgradeBlocker{
				Kind:      "turn",
				ID:        turn.ID,
				SessionID: turn.SessionID,
				Status:    string(turn.Status),
			})
		}
	}
	for _, msg := range state.OutboxMessages {
		if OutboxBlocksUpgrade(state, msg, now) {
			blockers = append(blockers, UpgradeBlocker{
				Kind:      "outbox",
				ID:        msg.ID,
				SessionID: msg.SessionID,
				Status:    string(msg.Status),
				Detail:    msg.Kind,
			})
		}
	}
	sort.Slice(blockers, func(i, j int) bool {
		if blockers[i].Kind != blockers[j].Kind {
			return blockers[i].Kind < blockers[j].Kind
		}
		if blockers[i].SessionID != blockers[j].SessionID {
			return blockers[i].SessionID < blockers[j].SessionID
		}
		return blockers[i].ID < blockers[j].ID
	})
	return blockers
}

func OutboxBlocksUpgrade(state State, msg OutboxMessage, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	if outboxDeliveryProtected(msg) {
		switch msg.Status {
		case OutboxStatusQueued:
			return true
		case OutboxStatusSending:
			return msg.LastSendAttempt.IsZero() || now.Sub(msg.LastSendAttempt) <= outboxSendLease
		default:
			return false
		}
	}
	if msg.UpgradeNonBlocking || outboxDeliveryTransient(msg) {
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

func outboxDeliveryProtected(msg OutboxMessage) bool {
	kind := strings.ToLower(strings.TrimSpace(msg.Kind))
	notificationKind := strings.ToLower(strings.TrimSpace(msg.NotificationKind))
	if len(msg.ArtifactIDs) > 0 ||
		strings.TrimSpace(msg.AttachmentPath) != "" ||
		strings.TrimSpace(msg.DriveItemID) != "" {
		return true
	}
	switch kind {
	case "final", "answer", "artifact", "attachment":
		return true
	}
	if strings.HasPrefix(kind, "final-") ||
		strings.HasPrefix(kind, "answer-") ||
		strings.Contains(kind, "final") ||
		strings.Contains(kind, "answer") ||
		strings.Contains(kind, "artifact") ||
		strings.Contains(kind, "attachment") {
		return true
	}
	return notificationKind == "turn_completed"
}

func OutboxDeliveryProtected(msg OutboxMessage) bool {
	return outboxDeliveryProtected(msg)
}

func outboxDeliveryTransient(msg OutboxMessage) bool {
	kind := strings.ToLower(strings.TrimSpace(msg.Kind))
	switch kind {
	case "asr-progress", "canceled", "interrupted", "queued-status":
		return true
	}
	return strings.HasPrefix(kind, "codex-status-") ||
		strings.HasPrefix(kind, "codex-progress-") ||
		strings.HasPrefix(kind, "codex-compact-") ||
		strings.HasPrefix(kind, "status-") ||
		strings.HasPrefix(kind, "compact-") ||
		strings.HasPrefix(kind, "progress-") ||
		strings.HasPrefix(kind, "interrupted-after-restart")
}

func OutboxDeliveryTransient(msg OutboxMessage) bool {
	return outboxDeliveryTransient(msg)
}

func OutboxSendIsAmbiguous(msg OutboxMessage) bool {
	return msg.Status == OutboxStatusSending && strings.HasPrefix(strings.TrimSpace(msg.LastSendError), "ambiguous Graph send;")
}

func (s *Store) QueueTurn(ctx context.Context, turn Turn) (Turn, bool, error) {
	if strings.TrimSpace(turn.SessionID) == "" {
		return Turn{}, false, fmt.Errorf("session id is required")
	}
	if out, created, handled, err := s.queueTurnSQLite(ctx, turn); handled || err != nil {
		return out, created, err
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
		if session.Status == SessionStatusQuarantined {
			return fmt.Errorf("session %q is quarantined", turn.SessionID)
		}
		if !sessionStatusIsActive(session.Status) {
			return fmt.Errorf("session %q is not active", turn.SessionID)
		}
		if _, fenced := activeForkForSessionLocked(state, turn.SessionID); fenced {
			return ErrForkParentFenced
		}
		now := time.Now()
		if turn.Status == "" {
			turn.Status = TurnStatusQueued
		}
		if turn.ModelProfile.IsZero() {
			turn.ModelProfile = session.ModelProfile
		}
		if turn.ModelGeneration == 0 {
			turn.ModelGeneration = session.ModelGeneration
		}
		if strings.TrimSpace(turn.ReasoningEffort) == "" && strings.TrimSpace(turn.ReasoningEffortSource) == "" {
			turn.ReasoningEffort = strings.TrimSpace(session.ReasoningEffort)
			turn.ReasoningEffortSource = strings.TrimSpace(session.ReasoningEffortSource)
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
	// Starting a turn only needs the turn/session/checkpoint ownership fence.
	// Loading all outbox rows linked to the turn here needlessly expands the
	// SQLite transaction and was a measurable queue-path regression.
	if out, handled, err := s.updateTurnSQLite(ctx, strings.TrimSpace(turnID), false, func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnRunningLocked(state, turn, codexThreadID, codexTurnID, now)
	}); handled || err != nil {
		return out, err
	}
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnRunningLocked(state, turn, codexThreadID, codexTurnID, now)
	})
}

func markTurnRunningLocked(state *State, turn Turn, codexThreadID string, codexTurnID string, now time.Time) (Turn, error) {
	if turn.Status != TurnStatusQueued && turn.Status != TurnStatusRunning {
		return turn, fmt.Errorf("turn %q with status %q cannot start", turn.ID, turn.Status)
	}
	if session, ok := state.Sessions[strings.TrimSpace(turn.SessionID)]; ok && session.Status == SessionStatusQuarantined {
		return turn, fmt.Errorf("session %q is quarantined", turn.SessionID)
	}
	if stateHasUnresolvedExecution(state, turn.SessionID) {
		// Keep the ownership fence in the same durable mutation as the
		// queued->running transition.  This closes the snapshot/check/mark
		// race in synchronous and control-fallback paths.
		return turn, ErrUnresolvedExecution
	}
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
}

func (s *Store) ClaimNextQueuedTurn(ctx context.Context, sessionID string) (Turn, bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return Turn{}, false, fmt.Errorf("session id is required")
	}
	if out, claimed, handled, err := s.claimNextQueuedTurnSQLite(ctx, sessionID); handled || err != nil {
		return out, claimed, err
	}
	var out Turn
	claimed := false
	err := s.UpdateSession(ctx, sessionID, func(state *State) error {
		if session, ok := state.Sessions[sessionID]; !ok || !sessionStatusIsActive(session.Status) {
			return nil
		}
		if _, fenced := activeForkForSessionLocked(state, sessionID); fenced {
			return nil
		}
		if stateHasUnresolvedExecution(state, sessionID) {
			// The ownership check is repeated inside the same durable-state
			// mutation as the claim.  A stale bridge snapshot must not turn a
			// queued request into a running turn after an anchor was persisted.
			return nil
		}
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

// RequeueTurn returns a claimed turn to the durable queue without creating a
// new inbound event. It is used when a second execution-ownership check wins a
// race after ClaimNextQueuedTurn but before dispatch.
func (s *Store) RequeueTurn(ctx context.Context, turnID string) (Turn, error) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return Turn{}, fmt.Errorf("turn id is required")
	}
	if out, handled, err := s.updateTurnSQLite(ctx, turnID, false, func(state *State, turn Turn, now time.Time) (Turn, error) {
		if turn.Status != TurnStatusRunning {
			return turn, nil
		}
		turn.Status = TurnStatusQueued
		turn.StartedAt = time.Time{}
		turn.UpdatedAt = now
		if inbound, ok := state.InboundEvents[turn.InboundEventID]; ok {
			inbound.Status = InboundStatusQueued
			inbound.UpdatedAt = now
			state.InboundEvents[inbound.ID] = inbound
		}
		updateSessionFromTurn(state, turn, now)
		return turn, nil
	}); handled || err != nil {
		return out, err
	}
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		if turn.Status != TurnStatusRunning {
			return turn, nil
		}
		turn.Status = TurnStatusQueued
		turn.StartedAt = time.Time{}
		turn.UpdatedAt = now
		if inbound, ok := state.InboundEvents[turn.InboundEventID]; ok {
			inbound.Status = InboundStatusQueued
			inbound.UpdatedAt = now
			state.InboundEvents[inbound.ID] = inbound
		}
		updateSessionFromTurn(state, turn, now)
		return turn, nil
	})
}

func sessionTranscriptCheckpointID(sessionID string) string {
	return "transcript:" + strings.TrimSpace(sessionID)
}

func importCheckpointHasUnresolvedExecution(checkpoint ImportCheckpoint) bool {
	return checkpoint.UnresolvedExecution != nil && strings.TrimSpace(checkpoint.UnresolvedExecution.State) != "resolved"
}

func importCheckpointIsExplicitHistoryRun(checkpoint ImportCheckpoint) bool {
	turnID := strings.TrimSpace(checkpoint.ImportTurnID)
	return strings.HasPrefix(turnID, "publish-history:") || strings.HasPrefix(turnID, "publish-full:")
}

func outboxTurnIsExplicitHistory(turnID string) bool {
	turnID = strings.TrimSpace(turnID)
	return strings.HasPrefix(turnID, "import:") || strings.HasPrefix(turnID, "import-bg:") ||
		strings.HasPrefix(turnID, "publish-history:") || strings.HasPrefix(turnID, "publish-full:")
}

// outboxTurnIsUserExplicitHistory distinguishes a user-directed import from a
// budgeted background resume.  Both namespaces are allowed to bypass an
// unresolved execution while the import is explicitly in progress, but a
// background resume must still honor a durable source-rewrite fence.
func outboxTurnIsUserExplicitHistory(turnID string) bool {
	turnID = strings.TrimSpace(turnID)
	return strings.HasPrefix(turnID, "import:") ||
		strings.HasPrefix(turnID, "publish-history:") || strings.HasPrefix(turnID, "publish-full:")
}

func outboxKindIsTranscriptLike(kind string, notificationKind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if strings.EqualFold(strings.TrimSpace(notificationKind), "needs_attention") ||
		strings.HasSuffix(kind, "-needs-attention") || kind == "needs-attention" ||
		strings.HasPrefix(kind, "sync-status-") || strings.HasPrefix(kind, "sync-complete") ||
		strings.HasPrefix(kind, "import-title") || strings.HasPrefix(kind, "import-complete") {
		return false
	}
	return strings.HasPrefix(kind, "import-") || strings.HasPrefix(kind, "sync-") ||
		strings.HasPrefix(kind, "codex-progress-") || strings.HasPrefix(kind, "codex-compact-") ||
		strings.HasPrefix(kind, "codex-assistant") || strings.HasPrefix(kind, "codex-final") ||
		strings.HasPrefix(kind, "answer") || strings.HasPrefix(kind, "final-answer") ||
		kind == "final" || strings.HasPrefix(kind, "final-") ||
		strings.HasPrefix(kind, "import-batch-") || strings.HasPrefix(kind, "import-bg-batch-") ||
		strings.HasPrefix(kind, "sync-batch-") || strings.HasPrefix(kind, "publish-full-batch-") ||
		strings.EqualFold(strings.TrimSpace(notificationKind), "turn_completed")
}

func outboxSendBlockedByUnresolvedExecution(state *State, msg OutboxMessage) bool {
	// A terminal-failure fence remains effective after its execution anchor is
	// cleared.  Without this durable per-outbox marker, an in-flight final can
	// be promoted after the failure winner commits and become visible later.
	if outboxTerminalFailureFenceActive(state, msg) {
		return true
	}
	if state == nil || strings.TrimSpace(msg.SessionID) == "" {
		return false
	}
	if !stateHasUnresolvedExecution(state, msg.SessionID) {
		return false
	}
	if outboxTurnIsExplicitHistory(msg.TurnID) {
		return false
	}
	if checkpoint, ok := state.ImportCheckpoints[sessionTranscriptCheckpointID(msg.SessionID)]; ok &&
		importCheckpointHasUnresolvedExecution(checkpoint) &&
		transcriptDeliveryTrustedBeforeAnchor(state, msg, TranscriptDeliveryRecord{
			SourcePath:   msg.TranscriptSourcePath,
			SourceOffset: msg.TranscriptSourceOffset,
		}, checkpoint) {
		return false
	}
	return outboxKindIsTranscriptLike(msg.Kind, msg.NotificationKind)
}

func outboxTerminalFailureFenceActive(state *State, msg OutboxMessage) bool {
	if msg.Status == OutboxStatusSent {
		return false
	}
	if msg.BlockedByTerminalFailure {
		return true
	}
	if state == nil || strings.TrimSpace(msg.TurnID) == "" || !isTerminalFinalOutboxMessage(msg) {
		return false
	}
	turn, ok := state.Turns[strings.TrimSpace(msg.TurnID)]
	return ok && turn.Status == TurnStatusFailed
}

func isTerminalFinalOutboxKind(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	return kind == "final" || strings.HasPrefix(kind, "final-") || kind == "answer" || kind == "codex-final" || kind == "final-answer"
}

func isTerminalFinalOutboxMessage(msg OutboxMessage) bool {
	if strings.EqualFold(strings.TrimSpace(msg.NotificationKind), "turn_completed") {
		return true
	}
	if strings.TrimSpace(msg.TerminalGroupID) != "" {
		return true
	}
	return isTerminalFinalOutboxKind(msg.Kind)
}

func terminalOutboxGroupID(turnID string) string {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return ""
	}
	return "terminal-final:" + turnID
}

// terminalFinalOutboxForTurn deliberately recognizes legacy final rows that
// predate TerminalGroupID.  The failure transaction is already scoped to a
// failed Turn, so fencing all final/final-N chunks for that Turn is safer than
// fencing only the first chunk identified by NotificationKind.
func terminalFinalOutboxForTurn(msg OutboxMessage, turnID string) bool {
	return strings.TrimSpace(msg.TurnID) == strings.TrimSpace(turnID) && isTerminalFinalOutboxMessage(msg)
}

// markOutboxDeliveryBlockedIfUnresolvedExecution is the final delivery CAS
// fence.  The send path checks ownership before Graph POST, but an anchor can
// be persisted after that check and before the accepted/sent callback.  Once
// Graph has supplied a stable message ID, retain the delivery identity and
// mark it for reconciliation instead of returning a retryable error (which
// could duplicate the Teams message).  Without an ID there is no safe way to
// distinguish an accepted send, so fail closed and leave the attempt queued.
func markOutboxDeliveryBlockedIfUnresolvedExecution(state *State, msg *OutboxMessage, teamsMessageID string) error {
	if state == nil || msg == nil {
		return nil
	}
	if msg.BlockedBySourceRewrite {
		if strings.TrimSpace(teamsMessageID) == "" && strings.TrimSpace(msg.TeamsMessageID) == "" {
			return ErrOutboxSendNotClaimed
		}
		return nil
	}
	if outboxTerminalFailureFenceActive(state, *msg) {
		msg.BlockedByTerminalFailure = true
		// A terminal-failure fence is intentionally independent of the anchor.
		// If Graph has not supplied an ID yet, do not claim that delivery was
		// accepted; accepting without an identity would make a later retry
		// indistinguishable from a duplicate POST.
		if strings.TrimSpace(teamsMessageID) == "" && strings.TrimSpace(msg.TeamsMessageID) == "" {
			return ErrOutboxSendNotClaimed
		}
		return nil
	}
	// A row that is already durably sent is no longer a pending transcript
	// delivery.  An anchor persisted later must not retroactively demote it or
	// suppress side effects which may already have run.
	if msg.Status == OutboxStatusSent {
		msg.BlockedByUnresolvedExecution = false
		return nil
	}
	if !outboxSendBlockedByUnresolvedExecution(state, *msg) {
		// A previously accepted Graph delivery is replayed through this same
		// method after the anchor is resolved.  Clear the durable marker before
		// promoting it so the normal Sent projection can run.
		msg.BlockedByUnresolvedExecution = false
		return nil
	}
	if strings.TrimSpace(teamsMessageID) == "" && strings.TrimSpace(msg.TeamsMessageID) == "" {
		return ErrOutboxSendNotClaimed
	}
	msg.BlockedByUnresolvedExecution = true
	return nil
}

// stateHasUnresolvedExecution also recognizes pre-anchor interrupted turns.
// Claim and MarkTurnRunning are durable fences, so a legacy state must not
// have a window where the bridge has not yet materialized its JSON anchor.
//
// A later durable Completed/Failed turn is deliberately not considered here.
// Durable status is not app-server ownership proof and can coexist with an
// orphan continuation still writing the same transcript.
func stateHasUnresolvedExecution(state *State, sessionID string) bool {
	if state == nil {
		return false
	}
	sessionID = strings.TrimSpace(sessionID)
	if state.legacyUnresolvedSessions != nil && state.legacyUnresolvedSessions[sessionID] {
		return true
	}
	if checkpoint, ok := state.ImportCheckpoints[sessionTranscriptCheckpointID(sessionID)]; ok && importCheckpointHasUnresolvedExecution(checkpoint) {
		return true
	}
	for _, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != sessionID {
			continue
		}
		if isLegacyUnresolvedTurn(turn) {
			return true
		}
	}
	return false
}

// turnCompletionAllowedByUnresolvedExecutionLocked is the terminal-owner
// fence.  A completion callback may commit a running turn while an anchor is
// active only when the callback carries the exact outer turn, thread, and
// Codex turn identity captured by that anchor.  Durable status or a later
// ordinary turn is not ownership proof.
func turnCompletionAllowedByUnresolvedExecutionLocked(state *State, turn Turn, codexThreadID string, codexTurnID string) bool {
	if state == nil || !stateHasUnresolvedExecution(state, turn.SessionID) {
		return true
	}
	// An absent execution identity is not ownership proof.  In particular, an
	// event_msg/agent_message final without a turn ID must not complete the
	// active durable turn while an unresolved anchor is present.  Administrative
	// bookkeeping may still finalize a turn that never started Codex (the
	// queued state has no execution owner to protect).
	if turn.Status == TurnStatusQueued && strings.TrimSpace(codexThreadID) == "" && strings.TrimSpace(codexTurnID) == "" {
		return true
	}
	checkpoint, ok := state.ImportCheckpoints[sessionTranscriptCheckpointID(turn.SessionID)]
	if !ok || !importCheckpointHasUnresolvedExecution(checkpoint) || checkpoint.UnresolvedExecution == nil {
		return false
	}
	anchor := checkpoint.UnresolvedExecution
	expectedThread := strings.TrimSpace(codexThreadID)
	expectedCodexTurn := strings.TrimSpace(codexTurnID)
	if expectedThread == "" || expectedCodexTurn == "" {
		return false
	}
	return strings.TrimSpace(anchor.OuterTurnID) == strings.TrimSpace(turn.ID) &&
		strings.TrimSpace(anchor.ThreadID) != "" && strings.TrimSpace(anchor.ThreadID) == expectedThread &&
		strings.TrimSpace(anchor.CodexTurnID) != "" && strings.TrimSpace(anchor.CodexTurnID) == expectedCodexTurn
}

func isLegacyUnresolvedTurn(turn Turn) bool {
	if turn.Status != TurnStatusInterrupted {
		return false
	}
	reason := strings.TrimSpace(turn.RecoveryReason)
	return strings.HasPrefix(reason, "ambiguous Codex execution:") ||
		reason == "ambiguous after helper restart" ||
		reason == "ambiguous after helper restart; notice sent" ||
		reason == "helper context canceled before Codex result could be verified"
}

func completionIdentityMatches(turn Turn, codexThreadID string, codexTurnID string) bool {
	codexThreadID = strings.TrimSpace(codexThreadID)
	codexTurnID = strings.TrimSpace(codexTurnID)
	if codexThreadID != "" && strings.TrimSpace(turn.CodexThreadID) != codexThreadID {
		return false
	}
	if codexTurnID != "" && strings.TrimSpace(turn.CodexTurnID) != codexTurnID {
		return false
	}
	return true
}

func markTurnCompletedLocked(state *State, turn Turn, codexThreadID string, codexTurnID string, now time.Time) (Turn, error) {
	if turn.Status == TurnStatusInterrupted {
		return Turn{}, fmt.Errorf("turn %q is interrupted and cannot be completed", turn.ID)
	}
	if turn.Status == TurnStatusCompleted || turn.Status == TurnStatusFailed {
		// Terminal callbacks are a CAS: a stale success callback must not
		// overwrite a failed owner (and a duplicate success must not refresh
		// timestamps or IDs after the durable result was committed).
		return turn, errStoreNoChange
	}
	if !turnCompletionAllowedByUnresolvedExecutionLocked(state, turn, codexThreadID, codexTurnID) {
		return turn, ErrUnresolvedExecution
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
}

func (s *Store) MarkTurnCompleted(ctx context.Context, turnID string, codexThreadID string, codexTurnID string) (Turn, error) {
	if out, handled, err := s.updateTurnSQLite(ctx, strings.TrimSpace(turnID), false, func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnCompletedLocked(state, turn, codexThreadID, codexTurnID, now)
	}); handled || err != nil {
		return out, err
	}
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnCompletedLocked(state, turn, codexThreadID, codexTurnID, now)
	})
}

func sameCheckpointSourcePath(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return left == right
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func transcriptCheckpointProgressAlreadyAhead(current ImportCheckpoint, progress TranscriptCheckpointProgress) bool {
	if !sameCheckpointSourcePath(current.SourcePath, progress.SourcePath) || strings.TrimSpace(current.SourcePath) == "" {
		return false
	}
	currentOffsetKnown := current.LastOffsetKnown || current.LastOffset > 0
	progressOffsetKnown := progress.LastOffsetKnown || progress.LastOffset > 0
	if currentOffsetKnown && progressOffsetKnown {
		return current.LastOffset > progress.LastOffset
	}
	return !currentOffsetKnown && !progressOffsetKnown && current.LastOffset == 0 && progress.LastOffset == 0 && current.LastSourceLine > progress.LastSourceLine
}

func importCheckpointEqualExceptUpdatedAt(left ImportCheckpoint, right ImportCheckpoint) bool {
	left.UpdatedAt = time.Time{}
	right.UpdatedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func applyTranscriptCheckpointProgress(current ImportCheckpoint, found bool, progress TranscriptCheckpointProgress, now time.Time) (ImportCheckpoint, bool, error) {
	progress.ID = strings.TrimSpace(progress.ID)
	progress.SessionID = strings.TrimSpace(progress.SessionID)
	progress.SourcePath = strings.TrimSpace(progress.SourcePath)
	progress.LastRecordID = strings.TrimSpace(progress.LastRecordID)
	if progress.ID == "" || progress.LastRecordID == "" {
		return current, false, nil
	}
	if progress.SessionID != "" && current.SessionID != "" && strings.TrimSpace(current.SessionID) != progress.SessionID {
		return current, false, fmt.Errorf("%w: transcript checkpoint %q belongs to session %q, not %q", ErrSessionStateProvenanceMismatch, progress.ID, current.SessionID, progress.SessionID)
	}
	if found && transcriptCheckpointProgressAlreadyAhead(current, progress) {
		return current, false, nil
	}
	next := current
	next.ID = progress.ID
	if next.SessionID == "" {
		next.SessionID = progress.SessionID
	}
	if progress.SourcePath != "" {
		next.SourcePath = progress.SourcePath
	}
	if progress.SourceFingerprint != "" {
		next.SourceFingerprint = progress.SourceFingerprint
	}
	next.LastRecordID = progress.LastRecordID
	next.LastSourceLine = progress.LastSourceLine
	if progress.LastOffset != 0 || current.LastOffset == 0 {
		next.LastOffset = progress.LastOffset
	}
	next.LastOffsetKnown = progress.LastOffsetKnown || current.LastOffsetKnown || progress.LastOffset != 0 || current.LastOffset != 0
	if !progress.SourceModTime.IsZero() || current.SourceModTime.IsZero() {
		next.SourceModTime = progress.SourceModTime
	}
	if progress.SourceSize != 0 || current.SourceSize == 0 {
		next.SourceSize = progress.SourceSize
	}
	if strings.TrimSpace(next.Status) == "" || next.Status == "blocked" {
		next.Status = "complete"
	}
	next.UpdatedAt = now
	if found && importCheckpointEqualExceptUpdatedAt(current, next) {
		return current, false, nil
	}
	return next, true, nil
}

// MarkTurnCompletedWithTranscriptCheckpoint commits the terminal owner and
// its final transcript cursor in one backend transaction. If an unresolved
// anchor appears before the transaction, only the exact outer callback may
// complete it; a mismatched callback leaves both the turn and cursor intact.
func (s *Store) MarkTurnCompletedWithTranscriptCheckpoint(ctx context.Context, turnID string, codexThreadID string, codexTurnID string, progress TranscriptCheckpointProgress) (Turn, error) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return Turn{}, fmt.Errorf("turn id is required")
	}
	if out, handled, err := s.markTurnCompletedWithTranscriptCheckpointSQLite(ctx, turnID, codexThreadID, codexTurnID, progress); handled || err != nil {
		return out, err
	}
	var out Turn
	turn, found, err := s.TurnByID(ctx, turnID)
	if err != nil {
		return Turn{}, err
	}
	if !found {
		return Turn{}, fmt.Errorf("turn %q not found", turnID)
	}
	turnSessionID := strings.TrimSpace(turn.SessionID)
	if strings.TrimSpace(progress.SessionID) != "" && strings.TrimSpace(progress.SessionID) != turnSessionID {
		return Turn{}, fmt.Errorf("%w: turn %q belongs to session %q, progress requested %q", ErrSessionStateProvenanceMismatch, turnID, turnSessionID, progress.SessionID)
	}
	if strings.TrimSpace(progress.SessionID) == "" {
		progress.SessionID = strings.TrimSpace(turn.SessionID)
	}
	err = s.UpdateSession(ctx, turn.SessionID, func(state *State) error {
		current, ok := state.Turns[turnID]
		if !ok {
			return fmt.Errorf("turn %q not found", turnID)
		}
		if strings.TrimSpace(current.SessionID) != turnSessionID {
			return fmt.Errorf("%w: turn %q changed session from %q to %q", ErrSessionStateProvenanceMismatch, turnID, turnSessionID, current.SessionID)
		}
		checkpoint, checkpointFound := state.ImportCheckpoints[progress.ID]
		if checkpointFound {
			if err := validateImportCheckpointProvenance(checkpoint, turnSessionID, progress.ID); err != nil {
				return err
			}
		}
		if checkpointFound && importCheckpointHasUnresolvedExecution(checkpoint) && checkpoint.UnresolvedExecution != nil && checkpoint.UnresolvedExecution.Generation <= 0 {
			if _, ok := migrateLegacyExecutionAnchorGenerationLocked(state, progress.ID, turnSessionID, codexThreadID, turnID, codexTurnID, 0); !ok {
				return ErrUnresolvedExecution
			}
			checkpoint = state.ImportCheckpoints[progress.ID]
		}
		now := time.Now()
		completed, completeErr := markTurnCompletedLocked(state, current, codexThreadID, codexTurnID, now)
		if errors.Is(completeErr, errStoreNoChange) {
			if current.Status != TurnStatusCompleted || !completionIdentityMatches(current, codexThreadID, codexTurnID) {
				out = current
				return nil
			}
			completed = current
		} else if completeErr != nil {
			return completeErr
		}
		anchorCleared := false
		if checkpointFound && importCheckpointHasUnresolvedExecution(checkpoint) && checkpoint.UnresolvedExecution != nil {
			anchor := checkpoint.UnresolvedExecution
			if strings.TrimSpace(anchor.OuterTurnID) != turnID || strings.TrimSpace(anchor.ThreadID) != strings.TrimSpace(codexThreadID) || strings.TrimSpace(anchor.CodexTurnID) != strings.TrimSpace(codexTurnID) || strings.TrimSpace(codexTurnID) == "" {
				return ErrUnresolvedExecution
			}
			if !completionSourceProofVerified(*anchor, progress) {
				return ErrUnresolvedExecution
			}
			checkpoint.UnresolvedExecution = nil
			anchorCleared = true
			if checkpoint.ExecutionAnchorGeneration < anchor.Generation {
				checkpoint.ExecutionAnchorGeneration = anchor.Generation
			}
		}
		next, changed, err := applyTranscriptCheckpointProgress(checkpoint, checkpointFound, progress, now)
		if err != nil {
			return err
		}
		state.Turns[turnID] = completed
		out = completed
		if changed || anchorCleared {
			state.ImportCheckpoints[progress.ID] = next
		}
		return nil
	})
	return out, err
}

// CompleteTurnWithFinal commits a real Codex completion and its terminal
// answer outbox in one backend transaction.  Keeping the final outbox inside
// the same CAS closes the failure-before-queue race: a callback that observes
// a terminal owner never gets a chance to create a new final row afterward.
func (s *Store) CompleteTurnWithFinal(ctx context.Context, req CompleteTurnWithFinalRequest) (Turn, error) {
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.CodexThreadID = strings.TrimSpace(req.CodexThreadID)
	req.CodexTurnID = strings.TrimSpace(req.CodexTurnID)
	// A normal runner completion may legitimately lack app-server identities
	// (older adapters and the control fallback executor only return text). This
	// compatibility path is safe only while there is no unresolved execution
	// anchor; the anchor branch below requires the complete outer/Codex identity.
	if req.SessionID == "" || req.TurnID == "" {
		return Turn{}, ErrStaleExecutionCallback
	}
	if strings.TrimSpace(req.Progress.SessionID) == "" {
		req.Progress.SessionID = req.SessionID
	}
	if strings.TrimSpace(req.Progress.ID) == "" {
		req.Progress.ID = sessionTranscriptCheckpointID(req.SessionID)
	}
	if strings.TrimSpace(req.Progress.SessionID) != req.SessionID || strings.TrimSpace(req.Progress.ID) != sessionTranscriptCheckpointID(req.SessionID) {
		return Turn{}, fmt.Errorf("%w: completion checkpoint does not belong to session %q", ErrSessionStateProvenanceMismatch, req.SessionID)
	}
	for i := range req.FinalOutbox {
		msg := req.FinalOutbox[i]
		if strings.TrimSpace(msg.SessionID) == "" {
			msg.SessionID = req.SessionID
		}
		if strings.TrimSpace(msg.TurnID) == "" {
			msg.TurnID = req.TurnID
		}
		req.FinalOutbox[i] = msg
	}
	if out, handled, err := s.completeTurnWithFinalSQLite(ctx, req); handled || err != nil {
		return out, err
	}
	var out Turn
	err := s.UpdateSession(ctx, req.SessionID, func(state *State) error {
		var err error
		out, err = completeTurnWithFinalLocked(state, req, time.Now())
		return err
	})
	return out, err
}

// ResolveInterruptedTurnWithCompletionProof is the only completion API that
// may promote an Interrupted turn.  The request must carry the exact outer
// and Codex execution IDs (and, when present, source proof); the backend
// rechecks those values together with the anchor generation in one CAS.
func (s *Store) ResolveInterruptedTurnWithCompletionProof(ctx context.Context, req CompleteTurnWithFinalRequest) (Turn, error) {
	req.ResolveInterrupted = true
	return s.CompleteTurnWithFinal(ctx, req)
}

// migrateLegacyExecutionAnchorGenerationLocked assigns a generation exactly
// once to an old anchor that predates the generation field.  Only a callback
// with all three execution identities can perform this migration; an empty or
// mismatched callback is rejected without clearing the anchor.
func migrateLegacyExecutionAnchorGenerationLocked(state *State, checkpointID, sessionID, threadID, outerTurnID, codexTurnID string, requestedGeneration int64) (int64, bool) {
	if state == nil {
		return 0, false
	}
	checkpoint, ok := state.ImportCheckpoints[strings.TrimSpace(checkpointID)]
	if !ok || checkpoint.UnresolvedExecution == nil {
		return 0, false
	}
	anchor := *checkpoint.UnresolvedExecution
	if anchor.Generation > 0 {
		if anchor.Generation != requestedGeneration {
			return 0, false
		}
		return anchor.Generation, true
	}
	if requestedGeneration != 0 || strings.TrimSpace(sessionID) == "" || strings.TrimSpace(threadID) == "" || strings.TrimSpace(outerTurnID) == "" || strings.TrimSpace(codexTurnID) == "" {
		return 0, false
	}
	if strings.TrimSpace(anchor.SessionID) != "" && strings.TrimSpace(anchor.SessionID) != strings.TrimSpace(sessionID) {
		return 0, false
	}
	if strings.TrimSpace(anchor.ThreadID) != strings.TrimSpace(threadID) || strings.TrimSpace(anchor.OuterTurnID) != strings.TrimSpace(outerTurnID) || strings.TrimSpace(anchor.CodexTurnID) != strings.TrimSpace(codexTurnID) {
		return 0, false
	}
	generation := checkpoint.ExecutionAnchorGeneration
	if generation < 1 {
		generation = 1
	} else {
		generation++
	}
	anchor.Generation = generation
	checkpoint.ExecutionAnchorGeneration = generation
	checkpoint.UnresolvedExecution = &anchor
	state.ImportCheckpoints[checkpoint.ID] = checkpoint
	return generation, true
}

func completionSourceProofPresent(anchor ExecutionAnchor, progress TranscriptCheckpointProgress) bool {
	anchorPath := strings.TrimSpace(anchor.SourcePath)
	anchorFingerprint := strings.TrimSpace(anchor.SourceFingerprint)
	if anchorPath == "" && anchorFingerprint == "" && strings.TrimSpace(anchor.CutoffRecordID) == "" && anchor.CutoffLine == 0 && anchor.CutoffOffset == 0 {
		return true
	}
	if anchorPath != "" && (strings.TrimSpace(progress.SourcePath) == "" || !sameCheckpointSourcePath(anchorPath, progress.SourcePath)) {
		return false
	}
	if anchorFingerprint != "" {
		// The progress fingerprint is for the new cursor and normally differs
		// from the anchor's prefix hash after an append.  Require a separate
		// proof of the exact prefix captured at the anchor instead of accepting
		// any non-empty hash supplied by a callback.
		if strings.TrimSpace(progress.SourceFingerprint) == "" ||
			strings.TrimSpace(progress.AnchorSourceFingerprint) != anchorFingerprint {
			return false
		}
	}
	if strings.TrimSpace(anchor.CutoffRecordID) != "" && strings.TrimSpace(progress.LastRecordID) == "" {
		return false
	}
	if (anchor.CutoffLine != 0 || anchor.CutoffOffset != 0) && !progress.LastOffsetKnown && progress.LastOffset == 0 {
		return false
	}
	return true
}

// completionSourceProofVerified turns the source fields in a completion
// request into an actual proof rather than trusting a callback to echo the
// durable anchor string. It is intentionally evaluated inside the final store
// transaction, after the bridge's preflight, so a replacement or in-place
// rewrite between scanning and commit fails closed.
func completionSourceProofVerified(anchor ExecutionAnchor, progress TranscriptCheckpointProgress) bool {
	if !completionSourceProofPresent(anchor, progress) {
		return false
	}
	if strings.TrimSpace(anchor.SourceFingerprint) != "" {
		actual, err := sourceCheckpointFingerprintAtOffset(anchor.SourcePath, anchor.CutoffOffset)
		if err != nil || actual != strings.TrimSpace(anchor.SourceFingerprint) {
			return false
		}
	}
	if strings.TrimSpace(anchor.SourceFingerprint) != "" && strings.TrimSpace(progress.SourceFingerprint) != "" {
		actual, err := sourceCheckpointFingerprintAtOffset(progress.SourcePath, progress.LastOffset)
		if err != nil || actual != strings.TrimSpace(progress.SourceFingerprint) {
			return false
		}
	}
	return true
}

// sourceCheckpointFingerprintAtOffset mirrors the transcript package's
// bounded prefix proof without introducing an import cycle. The opened file
// is checked against the pathname both before and after the bounded read, so
// an atomic replacement during the proof cannot be mistaken for the old
// source. Appends are allowed: they do not change bytes before the cursor or
// the stable file identity.
func sourceCheckpointFingerprintAtOffset(path string, offset int64) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || offset < 0 {
		return "", fmt.Errorf("invalid source checkpoint proof path or offset")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		if err == nil {
			err = fmt.Errorf("source checkpoint proof is a directory")
		}
		return "", err
	}
	if offset > info.Size() {
		return "", fmt.Errorf("source checkpoint offset %d exceeds size %d", offset, info.Size())
	}
	identity, err := SourceFileIdentityFromFileInfo(path, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		if err == nil {
			err = fmt.Errorf("source file identity unavailable")
		}
		return "", err
	}
	start := offset - sourceCheckpointFingerprintBytes
	if start < 0 {
		start = 0
	}
	window := make([]byte, offset-start)
	if len(window) > 0 {
		if n, readErr := f.ReadAt(window, start); readErr != nil || n != len(window) {
			if readErr == nil {
				readErr = io.ErrUnexpectedEOF
			}
			return "", readErr
		}
	}
	currentInfo, statErr := os.Stat(path)
	if statErr != nil {
		return "", statErr
	}
	currentIdentity, identityErr := SourceFileIdentityFromFileInfo(path, currentInfo)
	if identityErr != nil || currentIdentity != identity || currentInfo.IsDir() || currentInfo.Size() < offset {
		if identityErr == nil {
			if currentInfo.Size() < offset {
				identityErr = fmt.Errorf("source file shrank below checkpoint offset during proof")
			} else {
				identityErr = fmt.Errorf("source file identity changed during proof")
			}
		}
		return "", identityErr
	}
	h := sha256.New()
	_, _ = h.Write([]byte(identity))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(filepath.Clean(path)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(start, 10)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(window)
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func completeTurnWithFinalLocked(state *State, req CompleteTurnWithFinalRequest, now time.Time) (Turn, error) {
	if state == nil {
		return Turn{}, fmt.Errorf("state is required")
	}
	state.ensure(time.Time{})
	current, found := state.Turns[req.TurnID]
	if !found {
		return Turn{}, fmt.Errorf("turn %q not found", req.TurnID)
	}
	if strings.TrimSpace(current.SessionID) != req.SessionID {
		return current, fmt.Errorf("%w: turn %q belongs to session %q, not %q", ErrSessionStateProvenanceMismatch, req.TurnID, current.SessionID, req.SessionID)
	}
	if current.Status == TurnStatusCompleted || current.Status == TurnStatusFailed {
		if current.Status == TurnStatusCompleted && completionIdentityMatches(current, req.CodexThreadID, req.CodexTurnID) && terminalFinalOutboxPlanMatches(state, req.FinalOutbox) {
			return current, errStoreNoChange
		}
		return current, ErrCompletionOwnerLost
	}
	if current.Status == TurnStatusInterrupted && !req.ResolveInterrupted {
		return current, ErrUnresolvedExecution
	}
	if strings.TrimSpace(current.CodexThreadID) != "" && strings.TrimSpace(current.CodexThreadID) != req.CodexThreadID {
		return current, ErrCompletionOwnerLost
	}
	if strings.TrimSpace(current.CodexTurnID) != "" && strings.TrimSpace(current.CodexTurnID) != req.CodexTurnID {
		return current, ErrCompletionOwnerLost
	}

	checkpointID := strings.TrimSpace(req.Progress.ID)
	checkpoint, checkpointFound := state.ImportCheckpoints[checkpointID]
	if checkpointFound {
		if err := validateImportCheckpointProvenance(checkpoint, req.SessionID, checkpointID); err != nil {
			return current, err
		}
	}
	if checkpointFound && importCheckpointHasUnresolvedExecution(checkpoint) && checkpoint.UnresolvedExecution != nil && checkpoint.UnresolvedExecution.Generation <= 0 {
		generation, ok := migrateLegacyExecutionAnchorGenerationLocked(state, checkpointID, req.SessionID, req.CodexThreadID, req.TurnID, req.CodexTurnID, req.AnchorGeneration)
		if !ok {
			return current, ErrUnresolvedExecution
		}
		req.AnchorGeneration = generation
		checkpoint = state.ImportCheckpoints[checkpointID]
	}
	if current.Status == TurnStatusInterrupted && req.ResolveInterrupted && (strings.TrimSpace(req.CodexThreadID) == "" || strings.TrimSpace(req.CodexTurnID) == "") {
		return current, ErrUnresolvedExecution
	}
	if !turnCompletionAllowedByUnresolvedExecutionLocked(state, current, req.CodexThreadID, req.CodexTurnID) {
		return current, ErrUnresolvedExecution
	}
	// A transcript-backed completion carries the source proof computed by the
	// bridge. Revalidate it inside the same ownership transaction so a source
	// replacement between the scan and final CAS cannot commit a stale final or
	// cursor. Runner completions without transcript provenance retain the
	// existing compatibility path.
	if strings.TrimSpace(req.Progress.SourcePath) != "" &&
		strings.TrimSpace(req.Progress.SourceFingerprint) != "" &&
		req.Progress.LastOffsetKnown {
		actual, proofErr := sourceCheckpointFingerprintAtOffset(req.Progress.SourcePath, req.Progress.LastOffset)
		if proofErr != nil || actual != strings.TrimSpace(req.Progress.SourceFingerprint) {
			return current, ErrUnresolvedExecution
		}
	}
	anchorCleared := false
	if checkpointFound && importCheckpointHasUnresolvedExecution(checkpoint) && checkpoint.UnresolvedExecution != nil {
		anchor := checkpoint.UnresolvedExecution
		if strings.TrimSpace(req.CodexTurnID) == "" || strings.TrimSpace(anchor.CodexTurnID) == "" || req.AnchorGeneration <= 0 || anchor.Generation != req.AnchorGeneration ||
			strings.TrimSpace(anchor.SessionID) != "" && strings.TrimSpace(anchor.SessionID) != req.SessionID ||
			strings.TrimSpace(anchor.ThreadID) != req.CodexThreadID ||
			strings.TrimSpace(anchor.OuterTurnID) != req.TurnID ||
			strings.TrimSpace(anchor.CodexTurnID) != req.CodexTurnID {
			return current, ErrUnresolvedExecution
		}
		if !completionSourceProofVerified(*anchor, req.Progress) {
			return current, ErrUnresolvedExecution
		}
		checkpoint.UnresolvedExecution = nil
		anchorCleared = true
		if checkpoint.ExecutionAnchorGeneration < anchor.Generation {
			checkpoint.ExecutionAnchorGeneration = anchor.Generation
		}
	}
	if err := validateTerminalFinalOutboxPlan(state, req.FinalOutbox, req.SessionID, req.TurnID); err != nil {
		return current, err
	}
	nextCheckpoint, checkpointChanged, err := applyTranscriptCheckpointProgress(checkpoint, checkpointFound, req.Progress, now)
	if err != nil {
		return current, err
	}
	for _, msg := range req.FinalOutbox {
		if _, _, err := queueOutboxLocked(state, msg, now); err != nil {
			return current, err
		}
	}
	completed := current
	completed.Status = TurnStatusCompleted
	completed.CompletedAt = now
	completed.CodexThreadID = req.CodexThreadID
	completed.CodexTurnID = req.CodexTurnID
	completed.UpdatedAt = now
	state.Turns[req.TurnID] = completed
	updateSessionFromTurn(state, completed, now)
	if checkpointChanged || anchorCleared {
		if strings.TrimSpace(nextCheckpoint.Status) == "" || nextCheckpoint.Status == importCheckpointStatusBlocked {
			nextCheckpoint.Status = importCheckpointStatusComplete
		}
		nextCheckpoint.UpdatedAt = now
		state.ImportCheckpoints[checkpointID] = nextCheckpoint
	}
	return completed, nil
}

func validateTerminalFinalOutboxPlan(state *State, planned []OutboxMessage, sessionID string, turnID string) error {
	seenNotification := false
	groupID := ""
	for _, msg := range planned {
		kind := strings.ToLower(strings.TrimSpace(msg.Kind))
		notificationKind := strings.ToLower(strings.TrimSpace(msg.NotificationKind))
		// A long final is represented by several final/final-N chunks.  Only
		// the owner-mention chunk carries turn_completed; the continuation
		// chunks intentionally have an empty notification kind.  Validate the
		// whole plan as terminal output without requiring every chunk to be the
		// notification row.
		if strings.TrimSpace(msg.ID) == "" || strings.TrimSpace(msg.SessionID) != sessionID || strings.TrimSpace(msg.TurnID) != turnID || (kind != "final" && !strings.HasPrefix(kind, "final-")) || notificationKind != "" && notificationKind != "turn_completed" {
			return fmt.Errorf("%w: final outbox %q has invalid ownership", ErrSessionStateProvenanceMismatch, msg.ID)
		}
		if notificationKind == "turn_completed" {
			seenNotification = true
		}
		if currentGroup := strings.TrimSpace(msg.TerminalGroupID); currentGroup != "" {
			if groupID != "" && groupID != currentGroup {
				return fmt.Errorf("%w: terminal final plan mixes groups", ErrTerminalOutboxConflict)
			}
			groupID = currentGroup
		}
		if existing, ok := state.OutboxMessages[msg.ID]; ok && !terminalFinalOutboxMessageMatches(existing, msg) {
			return fmt.Errorf("%w: outbox %q already contains a different final", ErrTerminalOutboxConflict, msg.ID)
		}
	}
	if len(planned) > 0 && !seenNotification {
		return fmt.Errorf("%w: terminal final plan has no turn_completed notification", ErrSessionStateProvenanceMismatch)
	}
	return nil
}

func terminalFinalOutboxPlanMatches(state *State, planned []OutboxMessage) bool {
	for _, msg := range planned {
		existing, ok := state.OutboxMessages[msg.ID]
		if !ok || !terminalFinalOutboxMessageMatches(existing, msg) {
			return false
		}
	}
	return true
}

func terminalFinalOutboxMessageMatches(existing OutboxMessage, planned OutboxMessage) bool {
	return strings.TrimSpace(existing.ID) == strings.TrimSpace(planned.ID) &&
		strings.TrimSpace(existing.SessionID) == strings.TrimSpace(planned.SessionID) &&
		strings.TrimSpace(existing.TurnID) == strings.TrimSpace(planned.TurnID) &&
		strings.TrimSpace(existing.TeamsChatID) == strings.TrimSpace(planned.TeamsChatID) &&
		strings.TrimSpace(existing.Kind) == strings.TrimSpace(planned.Kind) &&
		existing.Body == planned.Body &&
		strings.TrimSpace(existing.SourceTextHash) == strings.TrimSpace(planned.SourceTextHash) &&
		existing.PartIndex == planned.PartIndex &&
		existing.PartCount == planned.PartCount &&
		(strings.TrimSpace(existing.TerminalGroupID) == "" || strings.TrimSpace(planned.TerminalGroupID) == "" || strings.TrimSpace(existing.TerminalGroupID) == strings.TrimSpace(planned.TerminalGroupID))
}

func executionAnchorMatchesClearRequest(anchor ExecutionAnchor, req ExecutionAnchorClearRequest) bool {
	if strings.TrimSpace(anchor.State) == "resolved" {
		return false
	}
	return (strings.TrimSpace(anchor.SessionID) == "" || strings.TrimSpace(anchor.SessionID) == strings.TrimSpace(req.SessionID)) &&
		strings.TrimSpace(anchor.ThreadID) == strings.TrimSpace(req.ThreadID) &&
		sameCheckpointSourcePath(anchor.SourcePath, req.SourcePath) &&
		strings.TrimSpace(anchor.SourceFingerprint) == strings.TrimSpace(req.SourceFingerprint) &&
		strings.TrimSpace(anchor.OuterTurnID) == strings.TrimSpace(req.OuterTurnID) &&
		strings.TrimSpace(anchor.CodexTurnID) == strings.TrimSpace(req.CodexTurnID) &&
		anchor.Generation == req.Generation &&
		strings.TrimSpace(anchor.CutoffRecordID) == strings.TrimSpace(req.CutoffRecordID) &&
		anchor.CutoffLine == req.CutoffLine &&
		anchor.CutoffOffset == req.CutoffOffset
}

func clearExecutionAnchorLocked(state *State, req ExecutionAnchorClearRequest) bool {
	if state == nil {
		return false
	}
	checkpoint, found := state.ImportCheckpoints[strings.TrimSpace(req.CheckpointID)]
	if !found || checkpoint.UnresolvedExecution == nil || !executionAnchorMatchesClearRequest(*checkpoint.UnresolvedExecution, req) {
		return false
	}
	turn, found := state.Turns[strings.TrimSpace(req.OuterTurnID)]
	if !found || strings.TrimSpace(turn.SessionID) != strings.TrimSpace(req.SessionID) {
		return false
	}
	if turn.Status != TurnStatusInterrupted && turn.Status != TurnStatusCompleted && turn.Status != TurnStatusFailed {
		return false
	}
	if strings.TrimSpace(req.ThreadID) != "" && strings.TrimSpace(turn.CodexThreadID) != strings.TrimSpace(req.ThreadID) {
		return false
	}
	if strings.TrimSpace(req.CodexTurnID) != "" && strings.TrimSpace(turn.CodexTurnID) != strings.TrimSpace(req.CodexTurnID) {
		return false
	}
	if turn.Status == TurnStatusInterrupted {
		currentReason := strings.TrimSpace(turn.RecoveryReason)
		if currentReason != strings.TrimSpace(req.RecoveryReasonTo) && currentReason != strings.TrimSpace(req.RecoveryReasonFrom) && !isLegacyUnresolvedTurn(turn) {
			return false
		}
		if currentReason != strings.TrimSpace(req.RecoveryReasonTo) {
			turn.RecoveryReason = strings.TrimSpace(req.RecoveryReasonTo)
			state.Turns[turn.ID] = turn
		}
	}
	if turn.Status == TurnStatusFailed {
		// A legacy/corrupt state can contain Failed plus an active anchor even
		// though the normal failure CAS installs this fence first.  Clearing the
		// anchor must not reopen a stale terminal final in that recovery path.
		markTerminalFailureOutboxFenceForTurnLocked(state, turn.ID, "superseded by failed turn", time.Now())
	}
	if checkpoint.ExecutionAnchorGeneration < req.Generation {
		checkpoint.ExecutionAnchorGeneration = req.Generation
	}
	checkpoint.UnresolvedExecution = nil
	state.ImportCheckpoints[checkpoint.ID] = checkpoint
	return true
}

// ClearExecutionAnchorAndConfirmTurn clears an exact anchor and updates the
// interrupted turn reason in one backend transaction. A mismatched or stale
// request is a no-op; it must never modify a newer anchor generation.
func (s *Store) ClearExecutionAnchorAndConfirmTurn(ctx context.Context, req ExecutionAnchorClearRequest) error {
	req.CheckpointID = strings.TrimSpace(req.CheckpointID)
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.SourcePath = strings.TrimSpace(req.SourcePath)
	req.SourceFingerprint = strings.TrimSpace(req.SourceFingerprint)
	req.OuterTurnID = strings.TrimSpace(req.OuterTurnID)
	req.CodexTurnID = strings.TrimSpace(req.CodexTurnID)
	req.CutoffRecordID = strings.TrimSpace(req.CutoffRecordID)
	if req.CheckpointID == "" || req.SessionID == "" || req.OuterTurnID == "" {
		return nil
	}
	if handled, err := s.clearExecutionAnchorAndConfirmTurnSQLite(ctx, req); handled || err != nil {
		return err
	}
	return s.UpdateSession(ctx, req.SessionID, func(state *State) error {
		if !clearExecutionAnchorLocked(state, req) {
			return errStoreNoChange
		}
		return nil
	})
}

func (s *Store) MarkTurnFailed(ctx context.Context, turnID string, message string) (Turn, error) {
	return s.MarkTurnFailedWithCodexIDs(ctx, turnID, message, "", "")
}

func (s *Store) MarkTurnFailedWithCodexIDs(ctx context.Context, turnID string, message string, codexThreadID string, codexTurnID string) (Turn, error) {
	if out, handled, err := s.updateTurnSQLite(ctx, strings.TrimSpace(turnID), true, func(state *State, turn Turn, now time.Time) (Turn, error) {
		if turn.Status == TurnStatusInterrupted {
			// An exact app-server failure callback may arrive after the helper
			// recorded an ambiguous interruption.  Route that narrow case through
			// the same atomic failure+anchor-clear transition; an unqualified
			// administrative failure must retain the historical rejection.
			// This legacy API has no anchor generation/provenance field. It must
			// not consume the current anchor based only on matching IDs; callers
			// handling an unresolved app-server execution must use
			// MarkTurnFailedForExecution instead.
			if stateHasUnresolvedExecution(state, turn.SessionID) {
				return turn, ErrUnresolvedExecution
			}
			return Turn{}, fmt.Errorf("turn %q is interrupted and cannot be failed", turn.ID)
		}
		if turn.Status == TurnStatusCompleted || turn.Status == TurnStatusFailed {
			// A stale failure callback cannot replace a committed terminal result.
			if turn.Status == TurnStatusFailed {
				if markTerminalFailureOutboxFenceForTurnLocked(state, turn.ID, "superseded by failed turn", now) > 0 {
					return turn, nil
				}
			}
			return turn, errStoreNoChange
		}
		if !turnCompletionAllowedByUnresolvedExecutionLocked(state, turn, codexThreadID, codexTurnID) {
			// Failure callbacks carry the same execution identity as success
			// callbacks.  A stale callback must not fail a newer owner merely
			// because failure is less visible than an answer.
			return turn, ErrUnresolvedExecution
		}
		if stateHasUnresolvedExecution(state, turn.SessionID) {
			return turn, ErrUnresolvedExecution
		}
		turn.Status = TurnStatusFailed
		turn.FailedAt = now
		turn.FailureMessage = message
		if codexThreadID != "" {
			turn.CodexThreadID = codexThreadID
		}
		if codexTurnID != "" {
			turn.CodexTurnID = codexTurnID
		}
		markTerminalFailureOutboxFenceForTurnLocked(state, turn.ID, "superseded by failed turn", now)
		updateSessionFromTurn(state, turn, now)
		return turn, nil
	}); handled || err != nil {
		return out, err
	}
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		if turn.Status == TurnStatusInterrupted {
			if stateHasUnresolvedExecution(state, turn.SessionID) {
				return turn, ErrUnresolvedExecution
			}
			return Turn{}, fmt.Errorf("turn %q is interrupted and cannot be failed", turn.ID)
		}
		if turn.Status == TurnStatusCompleted || turn.Status == TurnStatusFailed {
			if turn.Status == TurnStatusFailed {
				if markTerminalFailureOutboxFenceForTurnLocked(state, turn.ID, "superseded by failed turn", now) > 0 {
					return turn, nil
				}
			}
			return turn, errStoreNoChange
		}
		if !turnCompletionAllowedByUnresolvedExecutionLocked(state, turn, codexThreadID, codexTurnID) {
			return turn, ErrUnresolvedExecution
		}
		if stateHasUnresolvedExecution(state, turn.SessionID) {
			return turn, ErrUnresolvedExecution
		}
		turn.Status = TurnStatusFailed
		turn.FailedAt = now
		turn.FailureMessage = message
		if codexThreadID != "" {
			turn.CodexThreadID = codexThreadID
		}
		if codexTurnID != "" {
			turn.CodexTurnID = codexTurnID
		}
		markTerminalFailureOutboxFenceForTurnLocked(state, turn.ID, "superseded by failed turn", now)
		updateSessionFromTurn(state, turn, now)
		return turn, nil
	})
}

// markTurnFailedWithExecutionProofLocked consumes an exact app-server failure
// proof while the Turn and its unresolved execution anchor are in one state
// mutation.  A separate MarkTurnFailed followed by ClearExecutionAnchor would
// leave a window in which a retry or transcript sync can observe Failed plus an
// active anchor (or clear a newer anchor generation).
func markTurnFailedWithExecutionProofLocked(state *State, turn Turn, req ExecutionAnchorClearRequest, message string, now time.Time) (Turn, error) {
	if state == nil {
		return turn, ErrUnresolvedExecution
	}
	if turn.Status == TurnStatusCompleted || turn.Status == TurnStatusFailed {
		// A duplicate terminal callback is idempotent.  If an older state left
		// an exact anchor behind, consume only that exact proof; never mutate the
		// already committed terminal result or clear a different generation.
		if strings.TrimSpace(req.SessionID) != "" && strings.TrimSpace(req.OuterTurnID) != "" &&
			strings.TrimSpace(req.ThreadID) != "" && strings.TrimSpace(req.CodexTurnID) != "" &&
			strings.TrimSpace(turn.SessionID) == strings.TrimSpace(req.SessionID) &&
			strings.TrimSpace(turn.ID) == strings.TrimSpace(req.OuterTurnID) &&
			strings.TrimSpace(turn.CodexThreadID) == strings.TrimSpace(req.ThreadID) &&
			strings.TrimSpace(turn.CodexTurnID) == strings.TrimSpace(req.CodexTurnID) {
			if checkpoint, found := state.ImportCheckpoints[strings.TrimSpace(req.CheckpointID)]; found && checkpoint.UnresolvedExecution != nil && executionAnchorMatchesClearRequest(*checkpoint.UnresolvedExecution, req) {
				// Keep the terminal failure fence even when the callback is a
				// duplicate; the first callback may have raced an in-flight
				// delivery before the fence was materialized.
				markTerminalFailureOutboxFenceForTurnLocked(state, turn.ID, "superseded by failed turn", now)
				if clearExecutionAnchorLocked(state, req) {
					return turn, nil
				}
			}
		}
		return turn, errStoreNoChange
	}
	if turn.Status != TurnStatusRunning && turn.Status != TurnStatusInterrupted {
		return turn, ErrUnresolvedExecution
	}
	if strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OuterTurnID) == "" ||
		strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.CodexTurnID) == "" {
		return turn, ErrUnresolvedExecution
	}
	if checkpoint, found := state.ImportCheckpoints[strings.TrimSpace(req.CheckpointID)]; found && checkpoint.UnresolvedExecution != nil && checkpoint.UnresolvedExecution.Generation <= 0 {
		legacyProof := req
		legacyProof.Generation = checkpoint.UnresolvedExecution.Generation
		if !executionAnchorMatchesClearRequest(*checkpoint.UnresolvedExecution, legacyProof) {
			return turn, ErrUnresolvedExecution
		}
		generation, ok := migrateLegacyExecutionAnchorGenerationLocked(state, req.CheckpointID, req.SessionID, req.ThreadID, req.OuterTurnID, req.CodexTurnID, req.Generation)
		if !ok {
			return turn, ErrUnresolvedExecution
		}
		req.Generation = generation
	}
	if strings.TrimSpace(turn.SessionID) != strings.TrimSpace(req.SessionID) ||
		strings.TrimSpace(turn.ID) != strings.TrimSpace(req.OuterTurnID) ||
		strings.TrimSpace(turn.CodexThreadID) != strings.TrimSpace(req.ThreadID) ||
		strings.TrimSpace(turn.CodexTurnID) != strings.TrimSpace(req.CodexTurnID) {
		return turn, ErrUnresolvedExecution
	}
	if turn.Status == TurnStatusInterrupted && !isLegacyUnresolvedTurn(turn) {
		return turn, ErrUnresolvedExecution
	}
	checkpoint, found := state.ImportCheckpoints[strings.TrimSpace(req.CheckpointID)]
	if !found || checkpoint.UnresolvedExecution == nil || !executionAnchorMatchesClearRequest(*checkpoint.UnresolvedExecution, req) {
		return turn, ErrUnresolvedExecution
	}
	next := turn
	next.Status = TurnStatusFailed
	next.FailedAt = now
	next.FailureMessage = message
	next.CodexThreadID = strings.TrimSpace(req.ThreadID)
	next.CodexTurnID = strings.TrimSpace(req.CodexTurnID)
	// A failure callback must never invalidate an in-flight Graph request.  It
	// fences the attempt so a late Graph callback can record its stable ID but
	// cannot publish the stale final.
	markTerminalFailureOutboxFenceForTurnLocked(state, next.ID, "superseded by failed turn", now)
	updateSessionFromTurn(state, next, now)
	state.Turns[next.ID] = next
	if !clearExecutionAnchorLocked(state, req) {
		return turn, ErrUnresolvedExecution
	}
	return next, nil
}

// MarkTurnFailedWithExecutionProof atomically records a terminal app-server
// failure and clears the exact unresolved anchor that proves ownership.  It is
// intentionally narrower than MarkTurnFailedWithCodexIDs: callers must supply
// the complete anchor generation/cutoff/source proof, and stale or incomplete
// callbacks fail closed without changing the durable owner.
func (s *Store) MarkTurnFailedWithExecutionProof(ctx context.Context, req ExecutionAnchorClearRequest, message string) (Turn, error) {
	req.CheckpointID = strings.TrimSpace(req.CheckpointID)
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.SourcePath = strings.TrimSpace(req.SourcePath)
	req.SourceFingerprint = strings.TrimSpace(req.SourceFingerprint)
	req.OuterTurnID = strings.TrimSpace(req.OuterTurnID)
	req.CodexTurnID = strings.TrimSpace(req.CodexTurnID)
	req.CutoffRecordID = strings.TrimSpace(req.CutoffRecordID)
	if req.CheckpointID == "" {
		req.CheckpointID = sessionTranscriptCheckpointID(req.SessionID)
	}
	if req.SessionID == "" || req.OuterTurnID == "" || req.ThreadID == "" || req.CodexTurnID == "" {
		return Turn{}, ErrUnresolvedExecution
	}
	apply := func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnFailedWithExecutionProofLocked(state, turn, req, message, now)
	}
	if out, handled, err := s.updateTurnSQLite(ctx, req.OuterTurnID, true, apply); handled || err != nil {
		return out, err
	}
	return s.updateTurn(ctx, req.OuterTurnID, apply)
}

func currentExecutionAnchorGeneration(state *State, sessionID string) int64 {
	if state == nil {
		return 0
	}
	checkpoint, found := state.ImportCheckpoints[sessionTranscriptCheckpointID(strings.TrimSpace(sessionID))]
	if !found || !importCheckpointHasUnresolvedExecution(checkpoint) || checkpoint.UnresolvedExecution == nil {
		return 0
	}
	return checkpoint.UnresolvedExecution.Generation
}

func executionFailureAnchorRequest(state *State, identity ExecutionFailureIdentity) (ExecutionAnchorClearRequest, bool) {
	if state == nil {
		return ExecutionAnchorClearRequest{}, false
	}
	checkpointID := sessionTranscriptCheckpointID(identity.SessionID)
	checkpoint, found := state.ImportCheckpoints[checkpointID]
	if !found || checkpoint.UnresolvedExecution == nil || strings.TrimSpace(checkpoint.UnresolvedExecution.State) == "resolved" {
		return ExecutionAnchorClearRequest{}, false
	}
	anchor := *checkpoint.UnresolvedExecution
	if strings.TrimSpace(anchor.SessionID) != "" && strings.TrimSpace(anchor.SessionID) != identity.SessionID {
		return ExecutionAnchorClearRequest{}, false
	}
	if strings.TrimSpace(anchor.OuterTurnID) != identity.TurnID ||
		strings.TrimSpace(anchor.ThreadID) != identity.ThreadID ||
		strings.TrimSpace(anchor.CodexTurnID) != identity.CodexTurnID {
		return ExecutionAnchorClearRequest{}, false
	}
	if identity.AnchorGeneration <= 0 || anchor.Generation != identity.AnchorGeneration {
		return ExecutionAnchorClearRequest{}, false
	}
	return ExecutionAnchorClearRequest{
		CheckpointID:      checkpointID,
		SessionID:         identity.SessionID,
		ThreadID:          identity.ThreadID,
		SourcePath:        anchor.SourcePath,
		SourceFingerprint: anchor.SourceFingerprint,
		OuterTurnID:       identity.TurnID,
		CodexTurnID:       identity.CodexTurnID,
		Generation:        anchor.Generation,
		CutoffRecordID:    anchor.CutoffRecordID,
		CutoffLine:        anchor.CutoffLine,
		CutoffOffset:      anchor.CutoffOffset,
	}, true
}

// markTurnFailedForExecutionLocked resolves the callback's current ownership
// inside the same state mutation that marks the Turn failed.  It deliberately
// does not accept an anchor snapshot from the bridge: a concurrent anchor is
// either consumed when it exactly matches the callback or causes a stale /
// unresolved error without changing the current owner.
func markTurnFailedForExecutionLocked(state *State, turn Turn, identity ExecutionFailureIdentity, message string, now time.Time) (Turn, error) {
	if state == nil {
		return turn, ErrUnresolvedExecution
	}
	identity.SessionID = strings.TrimSpace(identity.SessionID)
	identity.TurnID = strings.TrimSpace(identity.TurnID)
	identity.ThreadID = strings.TrimSpace(identity.ThreadID)
	identity.CodexTurnID = strings.TrimSpace(identity.CodexTurnID)
	if identity.SessionID == "" || identity.TurnID == "" {
		return turn, ErrStaleExecutionCallback
	}
	if strings.TrimSpace(turn.SessionID) != identity.SessionID || strings.TrimSpace(turn.ID) != identity.TurnID {
		return turn, ErrStaleExecutionCallback
	}
	if turn.Status == TurnStatusCompleted || turn.Status == TurnStatusFailed {
		if (strings.TrimSpace(turn.CodexThreadID) != "" && identity.ThreadID == "") ||
			(strings.TrimSpace(turn.CodexThreadID) != "" && strings.TrimSpace(turn.CodexThreadID) != identity.ThreadID) ||
			(strings.TrimSpace(turn.CodexThreadID) == "" && identity.ThreadID != "") ||
			(strings.TrimSpace(turn.CodexTurnID) != "" && strings.TrimSpace(turn.CodexTurnID) != identity.CodexTurnID) ||
			(strings.TrimSpace(turn.CodexTurnID) == "" && identity.CodexTurnID != "") {
			return turn, ErrStaleExecutionCallback
		}
		changed := false
		if turn.Status == TurnStatusFailed {
			changed = markTerminalFailureOutboxFenceForTurnLocked(state, turn.ID, "superseded by failed turn", now) > 0
		}
		if req, ok := executionFailureAnchorRequest(state, identity); ok {
			if clearExecutionAnchorLocked(state, req) {
				changed = true
			}
		}
		if changed {
			return turn, nil
		}
		return turn, errStoreNoChange
	}
	if turn.Status != TurnStatusRunning && turn.Status != TurnStatusInterrupted {
		return turn, ErrUnresolvedExecution
	}
	// An entirely anonymous failure callback cannot identify an execution owner.
	// Keep the narrow legacy compatibility path for a callback that still carries
	// the durable thread owner (some older runners did not emit a Codex turn ID),
	// but reject a callback with neither identity even when no anchor exists.
	if identity.ThreadID == "" && identity.CodexTurnID == "" && strings.TrimSpace(turn.CodexThreadID) == "" && strings.TrimSpace(turn.CodexTurnID) == "" {
		return turn, ErrStaleExecutionCallback
	}
	if existing := strings.TrimSpace(turn.CodexThreadID); existing != "" && identity.ThreadID != "" && existing != identity.ThreadID {
		return turn, ErrStaleExecutionCallback
	}
	if existing := strings.TrimSpace(turn.CodexThreadID); existing != "" && identity.ThreadID == "" {
		// Once a durable Turn has an app-server thread owner, a callback without
		// that identity cannot be allowed to fail it (or erase the owner).  An
		// unresolved anchor reports the stronger ambiguity error; otherwise this
		// is simply a stale/incomplete callback.
		if stateHasUnresolvedExecution(state, identity.SessionID) {
			return turn, ErrUnresolvedExecution
		}
		return turn, ErrStaleExecutionCallback
	}
	if strings.TrimSpace(turn.CodexTurnID) != "" && identity.CodexTurnID == "" {
		return turn, ErrStaleExecutionCallback
	}
	if existing := strings.TrimSpace(turn.CodexTurnID); existing != "" && existing != identity.CodexTurnID {
		return turn, ErrStaleExecutionCallback
	}
	if turn.Status == TurnStatusInterrupted && !isLegacyUnresolvedTurn(turn) {
		return turn, ErrUnresolvedExecution
	}
	var clearReq ExecutionAnchorClearRequest
	if stateHasUnresolvedExecution(state, identity.SessionID) {
		if identity.CodexTurnID == "" {
			return turn, ErrUnresolvedExecution
		}
		checkpointID := sessionTranscriptCheckpointID(identity.SessionID)
		if checkpoint, found := state.ImportCheckpoints[checkpointID]; found && checkpoint.UnresolvedExecution != nil && checkpoint.UnresolvedExecution.Generation <= 0 {
			generation, ok := migrateLegacyExecutionAnchorGenerationLocked(state, checkpointID, identity.SessionID, identity.ThreadID, identity.TurnID, identity.CodexTurnID, identity.AnchorGeneration)
			if !ok {
				return turn, ErrUnresolvedExecution
			}
			identity.AnchorGeneration = generation
		}
		if identity.AnchorGeneration <= 0 {
			return turn, ErrUnresolvedExecution
		}
		var ok bool
		clearReq, ok = executionFailureAnchorRequest(state, identity)
		if !ok {
			if checkpoint, found := state.ImportCheckpoints[sessionTranscriptCheckpointID(identity.SessionID)]; found && checkpoint.UnresolvedExecution != nil {
				return turn, ErrStaleExecutionCallback
			}
			return turn, ErrUnresolvedExecution
		}
	}
	next := turn
	next.Status = TurnStatusFailed
	next.FailedAt = now
	next.FailureMessage = message
	next.CodexThreadID = identity.ThreadID
	next.CodexTurnID = identity.CodexTurnID
	markTerminalFailureOutboxFenceForTurnLocked(state, next.ID, "superseded by failed turn", now)
	updateSessionFromTurn(state, next, now)
	state.Turns[next.ID] = next
	if clearReq.CheckpointID != "" && !clearExecutionAnchorLocked(state, clearReq) {
		return turn, ErrUnresolvedExecution
	}
	return next, nil
}

// MarkTurnFailedForExecution is the app-server failure transition.  Unlike
// MarkTurnFailedWithExecutionProof, callers provide only callback identity;
// the store resolves and validates the current anchor in the same JSON or
// SQLite transaction and installs the terminal-failure outbox fence before
// clearing that anchor.
func (s *Store) MarkTurnFailedForExecution(ctx context.Context, identity ExecutionFailureIdentity, message string) (Turn, error) {
	identity.SessionID = strings.TrimSpace(identity.SessionID)
	identity.TurnID = strings.TrimSpace(identity.TurnID)
	identity.ThreadID = strings.TrimSpace(identity.ThreadID)
	identity.CodexTurnID = strings.TrimSpace(identity.CodexTurnID)
	if identity.SessionID == "" || identity.TurnID == "" {
		return Turn{}, ErrStaleExecutionCallback
	}
	apply := func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnFailedForExecutionLocked(state, turn, identity, message, now)
	}
	if out, handled, err := s.updateTurnSQLite(ctx, identity.TurnID, true, apply); handled || err != nil {
		return out, err
	}
	return s.updateTurn(ctx, identity.TurnID, apply)
}

// ValidateTurnCompletionOwnership performs the non-mutating preflight used by
// the bridge before it creates a protected final outbox row.  MarkTurnCompleted
// remains the authoritative CAS: an anchor can appear after this check and
// before the final is committed.
func (s *Store) ValidateTurnCompletionOwnership(ctx context.Context, turnID string, codexThreadID string, codexTurnID string) error {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return fmt.Errorf("turn id is required")
	}
	turn, found, err := s.TurnByID(ctx, turnID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("turn %q not found", turnID)
	}
	if turn.Status == TurnStatusInterrupted {
		return ErrUnresolvedExecution
	}
	if turn.Status == TurnStatusCompleted || turn.Status == TurnStatusFailed {
		if !completionIdentityMatches(turn, codexThreadID, codexTurnID) {
			return ErrUnresolvedExecution
		}
		return nil
	}
	state, err := s.SessionExecutionStateSnapshot(ctx, turn.SessionID, sessionTranscriptCheckpointID(turn.SessionID))
	if err != nil {
		return err
	}
	if current, ok := state.Turns[turn.ID]; ok {
		turn = current
	} else {
		state.Turns[turn.ID] = turn
	}
	if !turnCompletionAllowedByUnresolvedExecutionLocked(&state, turn, codexThreadID, codexTurnID) {
		return ErrUnresolvedExecution
	}
	return nil
}

// QuarantineQueuedTerminalAnswerOutbox removes only unsent protected final
// rows after a completion ownership CAS fails.  Graph-accepted rows are left
// intact so their stable Teams message ID can be reconciled without another
// POST; the normal delivery CAS will keep them Accepted+Blocked.
func (s *Store) QuarantineQueuedTerminalAnswerOutbox(ctx context.Context, turnID string, reason string) (int, error) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return 0, fmt.Errorf("turn id is required")
	}
	changed := 0
	apply := func(state *State, turn Turn, now time.Time) (Turn, error) {
		changed = skipQueuedTerminalAnswerOutboxForTurnLocked(state, turn.ID, firstStoreNonEmptyString(reason, "superseded by unresolved execution"), now)
		if changed == 0 {
			return turn, errStoreNoChange
		}
		return turn, nil
	}
	if _, handled, err := s.updateTurnSQLite(ctx, turnID, true, apply); handled || err != nil {
		return changed, err
	}
	_, err := s.updateTurn(ctx, turnID, apply)
	return changed, err
}

func persistInterruptedTurnWithAnchorLocked(state *State, current Turn, req PersistInterruptedTurnWithAnchorRequest, now time.Time) (Turn, error) {
	if state == nil {
		return Turn{}, fmt.Errorf("state is required")
	}
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.CheckpointID = strings.TrimSpace(req.CheckpointID)
	if req.CheckpointID == "" {
		req.CheckpointID = sessionTranscriptCheckpointID(req.SessionID)
	}
	if req.SessionID == "" || req.TurnID == "" {
		return Turn{}, fmt.Errorf("session and turn ids are required")
	}
	if strings.TrimSpace(current.SessionID) != req.SessionID {
		return current, ErrStaleExecutionCallback
	}
	// Terminal ownership wins.  In particular, do not create an anchor after a
	// completion committed between a caller's snapshot and this transaction.
	if current.Status == TurnStatusCompleted || current.Status == TurnStatusFailed {
		return current, errStoreNoChange
	}
	if strings.TrimSpace(req.CodexThreadID) == "" && strings.TrimSpace(req.CodexTurnID) == "" && (strings.TrimSpace(current.CodexThreadID) != "" || strings.TrimSpace(current.CodexTurnID) != "") {
		return current, ErrStaleExecutionCallback
	}
	if expected := strings.TrimSpace(req.CodexThreadID); expected != "" && strings.TrimSpace(current.CodexThreadID) != "" && strings.TrimSpace(current.CodexThreadID) != expected {
		return current, ErrStaleExecutionCallback
	}
	if expected := strings.TrimSpace(req.CodexTurnID); expected != "" && strings.TrimSpace(current.CodexTurnID) != "" && strings.TrimSpace(current.CodexTurnID) != expected {
		return current, ErrStaleExecutionCallback
	}

	checkpoint := state.ImportCheckpoints[req.CheckpointID]
	if checkpoint.ID == "" {
		checkpoint.ID = req.CheckpointID
	}
	if checkpoint.SessionID == "" {
		checkpoint.SessionID = req.SessionID
	} else if strings.TrimSpace(checkpoint.SessionID) != req.SessionID {
		return current, ErrStaleExecutionCallback
	}
	if active := importCheckpointHasUnresolvedExecution(checkpoint); active && checkpoint.UnresolvedExecution != nil {
		if outer := strings.TrimSpace(checkpoint.UnresolvedExecution.OuterTurnID); outer != "" && outer != req.TurnID {
			// A different unresolved owner is already the session fence. Do not
			// attach this callback to it or overwrite its provenance.
			return current, ErrUnresolvedExecution
		}
		// Never merge a callback identity into an existing anchor when the
		// current Turn has not yet recorded that identity. The anchor is the
		// durable owner; accepting a different thread/Codex turn here would
		// leave Turn and checkpoint provenance permanently inconsistent.
		anchor := checkpoint.UnresolvedExecution
		if expected := strings.TrimSpace(anchor.ThreadID); expected != "" {
			if observed := strings.TrimSpace(req.CodexThreadID); observed != "" && observed != expected {
				return current, ErrStaleExecutionCallback
			}
			if observed := strings.TrimSpace(current.CodexThreadID); observed != "" && observed != expected {
				return current, ErrStaleExecutionCallback
			}
		}
		if expected := strings.TrimSpace(anchor.CodexTurnID); expected != "" {
			if observed := strings.TrimSpace(req.CodexTurnID); observed != "" && observed != expected {
				return current, ErrStaleExecutionCallback
			}
			if observed := strings.TrimSpace(current.CodexTurnID); observed != "" && observed != expected {
				return current, ErrStaleExecutionCallback
			}
		}
	}

	threadID := firstStoreNonEmptyString(req.CodexThreadID, current.CodexThreadID, req.Anchor.ThreadID)
	codexTurnID := firstStoreNonEmptyString(req.CodexTurnID, current.CodexTurnID, req.Anchor.CodexTurnID)
	anchorChanged := false
	var anchor ExecutionAnchor
	if checkpoint.UnresolvedExecution != nil && importCheckpointHasUnresolvedExecution(checkpoint) {
		anchor = *checkpoint.UnresolvedExecution
		if strings.TrimSpace(anchor.State) == "" {
			anchor.State = "unresolved"
			anchorChanged = true
		}
		if anchor.Generation <= 0 {
			anchor.Generation = maxStoreInt64(checkpoint.ExecutionAnchorGeneration, 1)
			anchorChanged = true
		}
		if checkpoint.ExecutionAnchorGeneration < anchor.Generation {
			checkpoint.ExecutionAnchorGeneration = anchor.Generation
			anchorChanged = true
		}
		fields := []struct {
			dst *string
			src string
		}{
			{&anchor.SessionID, req.SessionID},
			{&anchor.ThreadID, threadID},
			{&anchor.OuterTurnID, req.TurnID},
			{&anchor.CodexTurnID, codexTurnID},
			{&anchor.SourcePath, req.Anchor.SourcePath},
			{&anchor.SourceFingerprint, req.Anchor.SourceFingerprint},
			{&anchor.Reason, firstStoreNonEmptyString(req.RecoveryReason, req.Anchor.Reason)},
		}
		for _, field := range fields {
			if strings.TrimSpace(*field.dst) == "" && strings.TrimSpace(field.src) != "" {
				*field.dst = strings.TrimSpace(field.src)
				anchorChanged = true
			}
		}
		if strings.TrimSpace(anchor.Reason) == "" && strings.TrimSpace(req.RecoveryReason) != "" {
			anchor.Reason = strings.TrimSpace(req.RecoveryReason)
			anchorChanged = true
		}
	} else {
		anchor = req.Anchor
		anchor.SessionID = req.SessionID
		anchor.ThreadID = threadID
		anchor.OuterTurnID = req.TurnID
		anchor.CodexTurnID = codexTurnID
		anchor.Reason = firstStoreNonEmptyString(req.RecoveryReason, anchor.Reason)
		anchor.State = "unresolved"
		anchor.Generation = checkpoint.ExecutionAnchorGeneration + 1
		if anchor.Generation <= 0 {
			anchor.Generation = 1
		}
		if strings.TrimSpace(anchor.SourcePath) == "" {
			anchor.SourcePath = strings.TrimSpace(checkpoint.SourcePath)
		}
		if strings.TrimSpace(anchor.SourceFingerprint) == "" {
			anchor.SourceFingerprint = strings.TrimSpace(checkpoint.SourceFingerprint)
		}
		// A checkpoint written after interruption may already include records
		// from the ambiguous execution. It is not a safe cutoff; fail closed by
		// starting the bounded scan at the beginning of the source.
		if !req.ConservativeCutoff && (current.InterruptedAt.IsZero() || checkpoint.UpdatedAt.IsZero() || !checkpoint.UpdatedAt.After(current.InterruptedAt)) {
			anchor.CutoffRecordID = firstStoreNonEmptyString(anchor.CutoffRecordID, checkpoint.LastRecordID)
			if anchor.CutoffLine == 0 {
				anchor.CutoffLine = checkpoint.LastSourceLine
			}
			if anchor.CutoffOffset == 0 {
				anchor.CutoffOffset = checkpoint.LastOffset
			}
		} else {
			anchor.CutoffRecordID = ""
			anchor.CutoffLine = 0
			anchor.CutoffOffset = 0
		}
		anchor.CreatedAt = firstStoreNonZeroTime(anchor.CreatedAt, current.InterruptedAt, current.UpdatedAt, current.CreatedAt, now)
		anchorChanged = true
	}
	if anchor.UpdatedAt.IsZero() || anchorChanged {
		anchor.UpdatedAt = now
	}
	if anchor.CreatedAt.IsZero() {
		anchor.CreatedAt = now
	}
	if checkpoint.ExecutionAnchorGeneration < anchor.Generation {
		checkpoint.ExecutionAnchorGeneration = anchor.Generation
		anchorChanged = true
	}

	next := current
	if strings.TrimSpace(req.CodexThreadID) != "" && strings.TrimSpace(next.CodexThreadID) == "" {
		next.CodexThreadID = strings.TrimSpace(req.CodexThreadID)
	}
	if strings.TrimSpace(req.CodexTurnID) != "" && strings.TrimSpace(next.CodexTurnID) == "" {
		next.CodexTurnID = strings.TrimSpace(req.CodexTurnID)
	}
	reason := firstStoreNonEmptyString(req.RecoveryReason, next.RecoveryReason, anchor.Reason)
	turnChanged := next.Status != TurnStatusInterrupted || strings.TrimSpace(next.RecoveryReason) != reason || next.CodexThreadID != current.CodexThreadID || next.CodexTurnID != current.CodexTurnID
	if !turnChanged && !anchorChanged {
		return current, errStoreNoChange
	}
	if turnChanged {
		next.RecoveryReason = reason
		if next.Status != TurnStatusInterrupted {
			var err error
			next, err = markTurnInterruptedLocked(state, next, reason, now)
			if err != nil {
				return current, err
			}
		} else {
			next.InterruptedAt = now
			next.UpdatedAt = now
			markInboundIgnoredForInterruptedTurn(state, next, now)
			skipTransientOutboxForTurnLocked(state, next.ID, "superseded by interrupted turn", now)
		}
	}
	state.Turns[next.ID] = next
	checkpoint.UnresolvedExecution = &anchor
	checkpoint.Status = importCheckpointStatusBlocked
	checkpoint.UpdatedAt = now
	state.ImportCheckpoints[checkpoint.ID] = checkpoint
	return next, nil
}

// PersistInterruptedTurnWithAnchor atomically records an unresolved execution
// anchor and interrupts its durable outer Turn.  The backend implementations
// re-read the current Turn inside the mutation transaction; a terminal owner
// therefore wins any race with a stale recovery callback.
func (s *Store) PersistInterruptedTurnWithAnchor(ctx context.Context, req PersistInterruptedTurnWithAnchorRequest) (PersistInterruptedTurnWithAnchorResult, error) {
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	if req.CheckpointID == "" {
		req.CheckpointID = sessionTranscriptCheckpointID(req.SessionID)
	}
	if req.SessionID == "" || req.TurnID == "" {
		return PersistInterruptedTurnWithAnchorResult{}, fmt.Errorf("session and turn ids are required")
	}
	var result PersistInterruptedTurnWithAnchorResult
	apply := func(state *State, current Turn, now time.Time) (Turn, error) {
		next, err := persistInterruptedTurnWithAnchorLocked(state, current, req, now)
		result.Turn = next
		if next.Status == TurnStatusCompleted || next.Status == TurnStatusFailed {
			result.Terminal = true
		}
		if err == nil {
			result.Changed = true
		}
		return next, err
	}
	if out, handled, err := s.updateTurnSQLite(ctx, req.TurnID, true, apply); handled || err != nil {
		if out.ID != "" {
			result.Turn = out
		}
		if result.Turn.Status == TurnStatusCompleted || result.Turn.Status == TurnStatusFailed {
			result.Terminal = true
		}
		return result, err
	}
	out, err := s.updateTurn(ctx, req.TurnID, apply)
	if out.ID != "" {
		result.Turn = out
	}
	if result.Turn.Status == TurnStatusCompleted || result.Turn.Status == TurnStatusFailed {
		result.Terminal = true
	}
	return result, err
}

func (s *Store) MarkTurnInterrupted(ctx context.Context, turnID string, reason string) (Turn, error) {
	if out, handled, err := s.updateTurnSQLite(ctx, strings.TrimSpace(turnID), true, func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnInterruptedLocked(state, turn, reason, now)
	}); handled || err != nil {
		return out, err
	}
	return s.updateTurn(ctx, turnID, func(state *State, turn Turn, now time.Time) (Turn, error) {
		return markTurnInterruptedLocked(state, turn, reason, now)
	})
}

// markTurnInterruptedLocked is deliberately terminal-state preserving.  An
// executor callback can arrive after the successful owner has committed a
// completed/failed turn; allowing that stale callback to overwrite the
// terminal state would re-open the turn and can produce a second answer on a
// later retry.  Returning errStoreNoChange keeps this check in the same
// durable mutation for both JSON and SQLite backends.
func markTurnInterruptedLocked(state *State, turn Turn, reason string, now time.Time) (Turn, error) {
	if turn.Status == TurnStatusCompleted || turn.Status == TurnStatusFailed {
		return turn, errStoreNoChange
	}
	turn.Status = TurnStatusInterrupted
	turn.InterruptedAt = now
	turn.RecoveryReason = reason
	markInboundIgnoredForInterruptedTurn(state, turn, now)
	skipTransientOutboxForTurnLocked(state, turn.ID, "superseded by interrupted turn", now)
	return turn, nil
}

func (s *Store) UpdateTurnRecoveryReasonIfMatches(ctx context.Context, turnID string, status TurnStatus, from string, to string) (Turn, bool, error) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return Turn{}, false, fmt.Errorf("turn id is required")
	}
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	changed := false
	apply := func(_ *State, turn Turn, _ time.Time) (Turn, error) {
		if status != "" && turn.Status != status {
			return turn, errStoreNoChange
		}
		if strings.TrimSpace(turn.RecoveryReason) != from {
			return turn, errStoreNoChange
		}
		if from == to {
			return turn, errStoreNoChange
		}
		turn.RecoveryReason = to
		changed = true
		return turn, nil
	}
	if out, handled, err := s.updateTurnSQLite(ctx, turnID, false, apply); handled || err != nil {
		return out, changed, err
	}
	out, err := s.updateTurn(ctx, turnID, apply)
	return out, changed, err
}

func skipTransientOutboxForTurnLocked(state *State, turnID string, reason string, now time.Time) {
	if state == nil || strings.TrimSpace(turnID) == "" {
		return
	}
	if strings.TrimSpace(reason) == "" {
		reason = "superseded"
	}
	for id, msg := range state.OutboxMessages {
		if strings.TrimSpace(msg.TurnID) != strings.TrimSpace(turnID) {
			continue
		}
		if !outboxDeliveryTransient(msg) {
			continue
		}
		switch msg.Status {
		case OutboxStatusQueued:
			msg.Status = OutboxStatusSkipped
			msg.LastSendError = reason
			msg.UpdatedAt = now
			state.OutboxMessages[id] = msg
			updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSkipped, now)
		case OutboxStatusSending:
			// The Graph request may already have been accepted.  Keep the
			// attempt alive so its callback can durably record the message ID;
			// marking it skipped here would make a restart eligible to POST it
			// again.
		}
	}
}

// skipQueuedTerminalAnswerOutboxForTurnLocked prevents a stale success
// callback from being delivered after a failure callback has won the durable
// terminal race. Protected final messages are normally retained for retry, but
// a failed turn is an explicit ownership decision: a queued turn_completed
// answer from the losing callback must not become visible later.
func skipQueuedTerminalAnswerOutboxForTurnLocked(state *State, turnID string, reason string, now time.Time) int {
	if state == nil || strings.TrimSpace(turnID) == "" {
		return 0
	}
	if strings.TrimSpace(reason) == "" {
		reason = "superseded by failed turn"
	}
	changed := 0
	for id, msg := range state.OutboxMessages {
		if !terminalFinalOutboxForTurn(msg, turnID) {
			continue
		}
		// A Sending row has an in-flight Graph request.  Its outcome is still
		// unknown, so quarantining it as Skipped would make the delivery CAS
		// reject a later accepted message ID and could cause a duplicate POST
		// after restart.  Only an unsent queued row can be safely skipped here.
		if msg.Status != OutboxStatusQueued {
			continue
		}
		msg.Status = OutboxStatusSkipped
		msg.LastSendError = reason
		msg.UpdatedAt = now
		state.OutboxMessages[id] = msg
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSkipped, now)
		changed++
	}
	return changed
}

// markTerminalFailureOutboxFenceForTurnLocked suppresses every unsent
// turn_completed final after a failure callback wins the durable terminal
// race.  Queued rows can be skipped immediately.  Sending/Accepted rows must
// retain their delivery identity so a late Graph callback can persist a stable
// message ID without allowing the stale final to become Sent.
func markTerminalFailureOutboxFenceForTurnLocked(state *State, turnID string, reason string, now time.Time) int {
	if state == nil || strings.TrimSpace(turnID) == "" {
		return 0
	}
	if strings.TrimSpace(reason) == "" {
		reason = "superseded by failed turn"
	}
	changed := skipQueuedTerminalAnswerOutboxForTurnLocked(state, turnID, reason, now)
	for id, msg := range state.OutboxMessages {
		if !terminalFinalOutboxForTurn(msg, turnID) {
			continue
		}
		if msg.Status != OutboxStatusSending && msg.Status != OutboxStatusAccepted {
			continue
		}
		if msg.BlockedByTerminalFailure {
			continue
		}
		msg.BlockedByTerminalFailure = true
		msg.LastSendError = trimDiagnostic(reason, 240)
		msg.UpdatedAt = now
		state.OutboxMessages[id] = msg
		changed++
	}
	return changed
}

func (s *Store) QueueOutbox(ctx context.Context, msg OutboxMessage) (OutboxMessage, bool, error) {
	if strings.TrimSpace(msg.ID) == "" {
		msg.ID = outboxID(msg)
	}
	if strings.TrimSpace(msg.ID) == "" {
		return OutboxMessage{}, false, fmt.Errorf("outbox id is required")
	}
	if out, created, handled, err := s.queueOutboxSQLite(ctx, msg); handled || err != nil {
		return out, created, err
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
		now := time.Now()
		var err error
		out, created, err = queueOutboxLocked(state, msg, now)
		return err
	})
	return out, created, err
}

func queueOutboxLocked(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, bool, error) {
	if state == nil {
		return OutboxMessage{}, false, fmt.Errorf("state is required")
	}
	if strings.TrimSpace(msg.ID) == "" {
		msg.ID = outboxID(msg)
	}
	if strings.TrimSpace(msg.ID) == "" {
		return OutboxMessage{}, false, fmt.Errorf("outbox id is required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	if existing, ok := state.OutboxMessages[msg.ID]; ok {
		if outboxTerminalFailureFenceActive(state, existing) {
			existing.BlockedByTerminalFailure = true
			if existing.Status == OutboxStatusQueued {
				existing.Status = OutboxStatusSkipped
				existing.LastSendError = "terminal failure fence: superseded by failed turn"
				updateHelperDeliveryForOutboxLocked(state, existing, HelperDeliveryStatusSkipped, now)
			}
			state.OutboxMessages[existing.ID] = existing
		}
		return existing, false, nil
	}
	msg.TeamsChatID = strings.TrimSpace(msg.TeamsChatID)
	if msg.TeamsChatID == "" {
		return OutboxMessage{}, false, fmt.Errorf("Teams chat id is required")
	}
	if msg.Status == "" {
		msg.Status = OutboxStatusQueued
	}
	if strings.TrimSpace(msg.TerminalGroupID) == "" && isTerminalFinalOutboxMessage(msg) && strings.TrimSpace(msg.TurnID) != "" {
		msg.TerminalGroupID = terminalOutboxGroupID(msg.TurnID)
	}
	if turnID := strings.TrimSpace(msg.TurnID); turnID != "" {
		if turn, ok := state.Turns[turnID]; ok && turn.Status == TurnStatusFailed && isTerminalFinalOutboxMessage(msg) {
			// A stale completion callback can queue its final after the failure
			// winner committed.  Reject that new row at the creation boundary so
			// it cannot race the delivery CAS without inheriting the fence.
			msg.Status = OutboxStatusSkipped
			msg.BlockedByTerminalFailure = true
			msg.LastSendError = "terminal failure fence: superseded by failed turn"
		}
	}
	if sessionID := strings.TrimSpace(msg.SessionID); sessionID != "" {
		if session, ok := state.Sessions[sessionID]; ok && session.Status == SessionStatusQuarantined {
			msg.Status = OutboxStatusSkipped
			msg.LastSendError = firstStoreNonEmptyString(session.QuarantineReason, "session quarantined")
		}
	}
	if outboxDeliveryTransient(msg) && !outboxDeliveryProtected(msg) {
		msg.UpgradeNonBlocking = true
	}
	if msg.Sequence <= 0 {
		msg.Sequence = allocateChatSequence(state, msg.TeamsChatID, now)
	}
	if msg.CodexThreadID == "" {
		msg.CodexThreadID = helperDeliveryCodexThreadIDLocked(state, msg)
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
	updateHelperDeliveryForOutboxLocked(state, msg, helperDeliveryStatusFromOutboxStatus(msg.Status), now)
	return msg, true, nil
}

type TranscriptDeliveryQueueRequest struct {
	Message              OutboxMessage
	Delivery             TranscriptDeliveryRecord
	Checkpoint           ImportCheckpoint
	ParentFenceSessionID string
}

func (s *Store) QueueTranscriptDeliveryOutbox(ctx context.Context, req TranscriptDeliveryQueueRequest) (OutboxMessage, bool, bool, error) {
	req.ParentFenceSessionID = strings.TrimSpace(req.ParentFenceSessionID)
	msg := req.Message
	if strings.TrimSpace(msg.ID) == "" {
		msg.ID = outboxID(msg)
	}
	if strings.TrimSpace(msg.ID) == "" {
		return OutboxMessage{}, false, false, fmt.Errorf("outbox id is required")
	}
	if strings.TrimSpace(req.Delivery.ID) == "" {
		return OutboxMessage{}, false, false, fmt.Errorf("transcript delivery id is required")
	}
	if strings.TrimSpace(req.Delivery.SessionID) == "" {
		req.Delivery.SessionID = msg.SessionID
	}
	if strings.TrimSpace(req.Delivery.SessionID) == "" {
		return OutboxMessage{}, false, false, fmt.Errorf("transcript delivery session id is required")
	}
	if strings.TrimSpace(req.Delivery.OutboxID) == "" {
		req.Delivery.OutboxID = msg.ID
	}
	if req.Delivery.Status == "" {
		req.Delivery.Status = TranscriptDeliveryStatusQueued
	}
	if req.ParentFenceSessionID != "" {
		msg.ParentFenceSessionID = req.ParentFenceSessionID
	}
	// Persist source provenance on the outbox row itself.  The flusher does not
	// load the full transcript-delivery ledger, so it needs a cheap durable way
	// to tell a trusted record before an unresolved anchor cutoff from an
	// ambiguous record after it.  Older rows remain fail-closed because these
	// optional fields are absent.
	if strings.TrimSpace(msg.TranscriptCheckpointID) == "" {
		if checkpointID := strings.TrimSpace(req.Checkpoint.ID); checkpointID != "" {
			msg.TranscriptCheckpointID = checkpointID
		} else {
			msg.TranscriptCheckpointID = sessionTranscriptCheckpointID(msg.SessionID)
		}
	}
	if strings.TrimSpace(msg.TranscriptSourcePath) == "" {
		msg.TranscriptSourcePath = strings.TrimSpace(req.Delivery.SourcePath)
	}
	if !msg.TranscriptSourceOffsetKnown && strings.TrimSpace(msg.TranscriptSourcePath) != "" {
		msg.TranscriptSourceOffset = req.Delivery.SourceOffset
		msg.TranscriptSourceOffsetKnown = true
	}
	// Automatic linked-transcript delivery may carry a bounded proof of the
	// source prefix checked before scanning. Keep it on the outbox row so the
	// sender can reject a source replacement that happens after queueing but
	// before Graph POST. Empty proofs are left empty for legacy and explicit
	// history rows; those paths retain their existing conservative fences.
	if strings.TrimSpace(msg.TranscriptSourceProofFingerprint) == "" &&
		strings.TrimSpace(req.Checkpoint.SourceFingerprint) != "" &&
		strings.TrimSpace(msg.TranscriptSourcePath) != "" &&
		(req.Checkpoint.LastOffsetKnown || req.Checkpoint.LastOffset > 0) {
		msg.TranscriptSourceProofFingerprint = strings.TrimSpace(req.Checkpoint.SourceFingerprint)
		msg.TranscriptSourceProofOffset = req.Checkpoint.LastOffset
		msg.TranscriptSourceProofOffsetKnown = true
	}
	req.Message = msg
	if out, created, alreadyDelivered, handled, err := s.queueTranscriptDeliveryOutboxSQLite(ctx, req); handled || err != nil {
		return out, created, alreadyDelivered, err
	}
	update := s.Update
	if msg.SessionID != "" {
		update = func(ctx context.Context, fn func(*State) error) error {
			return s.UpdateSession(ctx, msg.SessionID, fn)
		}
	}
	var out OutboxMessage
	created := false
	alreadyDelivered := false
	err := update(ctx, func(state *State) error {
		if err := ensureParentUnfencedLocked(state, req.ParentFenceSessionID); err != nil {
			return err
		}
		now := time.Now()
		var err error
		out, created, alreadyDelivered, err = applyQueueTranscriptDeliveryOutboxLocked(state, req.Message, req.Delivery, req.Checkpoint, now)
		return err
	})
	return out, created, alreadyDelivered, err
}

func applyQueueTranscriptDeliveryOutboxLocked(state *State, msg OutboxMessage, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint, now time.Time) (OutboxMessage, bool, bool, error) {
	if state == nil {
		return OutboxMessage{}, false, false, fmt.Errorf("state is required")
	}
	if err := validateQueuedTranscriptCheckpointProvenance(state, msg, checkpoint); err != nil {
		return OutboxMessage{}, false, false, err
	}
	if stateHasUnresolvedExecution(state, msg.SessionID) && !outboxTurnIsExplicitHistory(msg.TurnID) &&
		!transcriptDeliveryUsesExactOuterExecutionProof(state, msg, delivery) &&
		!transcriptDeliveryTrustedBeforeAnchor(state, msg, delivery, checkpoint) {
		// Transcript delivery and checkpoint advancement are intentionally kept
		// behind the same ownership fence.  A caller may not have supplied the
		// checkpoint object (the session checkpoint is still authoritative), and
		// an already-delivered record must not make the cursor appear safe.
		return OutboxMessage{}, false, false, ErrUnresolvedExecution
	}
	if state.OutboxMessages == nil {
		state.OutboxMessages = map[string]OutboxMessage{}
	}
	if state.TranscriptDeliveries == nil {
		state.TranscriptDeliveries = map[string]TranscriptDeliveryRecord{}
	}
	if state.ImportCheckpoints == nil {
		state.ImportCheckpoints = map[string]ImportCheckpoint{}
	}
	if existingDelivery, ok := state.TranscriptDeliveries[delivery.ID]; ok {
		var out OutboxMessage
		if strings.TrimSpace(existingDelivery.OutboxID) != "" {
			if existing, ok := state.OutboxMessages[existingDelivery.OutboxID]; ok {
				out = existing
			}
		}
		if transcriptDeliverySuppressesQueue(existingDelivery) {
			applyTranscriptCheckpointLocked(state, checkpoint, now)
			return OutboxMessage{}, false, true, nil
		}
		if out.ID != "" {
			return out, false, false, nil
		}
		if existingDelivery.OutboxID != "" {
			msg.ID = existingDelivery.OutboxID
		}
		out, created, err := queueOutboxLocked(state, msg, now)
		if err != nil {
			return OutboxMessage{}, false, false, err
		}
		existingDelivery.OutboxID = out.ID
		if out.Status == OutboxStatusSkipped {
			existingDelivery.Status = TranscriptDeliveryStatusSkipped
		} else if existingDelivery.Status == "" {
			existingDelivery.Status = TranscriptDeliveryStatusQueued
		}
		existingDelivery.UpdatedAt = now
		state.TranscriptDeliveries[delivery.ID] = existingDelivery
		return out, created, false, nil
	}
	out, created, err := queueOutboxLocked(state, msg, now)
	if err != nil {
		return OutboxMessage{}, false, false, err
	}
	normalized := normalizeTranscriptDeliveryRecord(delivery, now)
	normalized.OutboxID = out.ID
	if out.Status == OutboxStatusSkipped {
		normalized.Status = TranscriptDeliveryStatusSkipped
	} else if normalized.Status == "" {
		normalized.Status = TranscriptDeliveryStatusQueued
	}
	state.TranscriptDeliveries[normalized.ID] = normalized
	return out, created, false, nil
}

// transcriptDeliveryUsesExactOuterExecutionProof permits the final produced by
// the currently running outer callback to be recorded while its anchor is
// still unresolved.  The later MarkTurnCompleted CAS remains mandatory; this
// helper only avoids rejecting the one result that can provide the typed proof
// needed to clear the anchor.
func transcriptDeliveryUsesExactOuterExecutionProof(state *State, msg OutboxMessage, delivery TranscriptDeliveryRecord) bool {
	if state == nil || !strings.EqualFold(strings.TrimSpace(msg.Kind), "final") || !strings.EqualFold(strings.TrimSpace(msg.NotificationKind), "turn_completed") {
		return false
	}
	turnID := strings.TrimSpace(msg.TurnID)
	if turnID == "" {
		return false
	}
	turn, ok := state.Turns[turnID]
	if !ok || turn.Status != TurnStatusRunning {
		return false
	}
	checkpoint, ok := state.ImportCheckpoints[sessionTranscriptCheckpointID(msg.SessionID)]
	if !ok || !importCheckpointHasUnresolvedExecution(checkpoint) || checkpoint.UnresolvedExecution == nil {
		return false
	}
	anchor := checkpoint.UnresolvedExecution
	threadID := strings.TrimSpace(msg.CodexThreadID)
	if threadID == "" || strings.TrimSpace(delivery.CodexTurnID) == "" {
		return false
	}
	return strings.TrimSpace(anchor.SessionID) == strings.TrimSpace(msg.SessionID) &&
		strings.TrimSpace(anchor.OuterTurnID) == turnID &&
		strings.TrimSpace(anchor.ThreadID) != "" && strings.TrimSpace(anchor.ThreadID) == threadID &&
		strings.TrimSpace(anchor.CodexTurnID) == strings.TrimSpace(delivery.CodexTurnID)
}

// transcriptDeliveryTrustedBeforeAnchor permits a durable record whose source
// position is entirely before the unresolved execution cutoff.  It is the
// narrow liveness exception to the session-wide fail-closed fence: missing or
// mismatched provenance remains blocked, because a timestamp or matching text
// is not ownership proof.
func transcriptDeliveryTrustedBeforeAnchor(state *State, msg OutboxMessage, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint) bool {
	if state == nil || strings.TrimSpace(msg.SessionID) == "" {
		return false
	}
	anchorCheckpoint, ok := state.ImportCheckpoints[sessionTranscriptCheckpointID(msg.SessionID)]
	if !ok || !importCheckpointHasUnresolvedExecution(anchorCheckpoint) || anchorCheckpoint.UnresolvedExecution == nil {
		return false
	}
	anchor := anchorCheckpoint.UnresolvedExecution
	if strings.TrimSpace(anchor.SourcePath) == "" || anchor.CutoffOffset <= 0 {
		return false
	}
	checkpointID := strings.TrimSpace(msg.TranscriptCheckpointID)
	if checkpointID == "" {
		checkpointID = strings.TrimSpace(checkpoint.ID)
	}
	if checkpointID == "" {
		checkpointID = sessionTranscriptCheckpointID(msg.SessionID)
	}
	if checkpointID != sessionTranscriptCheckpointID(msg.SessionID) {
		return false
	}
	sourcePath := strings.TrimSpace(msg.TranscriptSourcePath)
	if sourcePath == "" {
		sourcePath = strings.TrimSpace(delivery.SourcePath)
	}
	if sourcePath == "" || !sameTranscriptSourcePath(sourcePath, anchor.SourcePath) {
		return false
	}
	// The outbox send/CAS path must rely on provenance persisted on the row
	// itself.  In-memory delivery metadata is not available after a restart and
	// must never turn a legacy row into a trusted prefix by accident.
	if !msg.TranscriptSourceOffsetKnown {
		return false
	}
	offset := msg.TranscriptSourceOffset
	return offset >= 0 && offset <= anchor.CutoffOffset
}

func sameTranscriptSourcePath(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func transcriptDeliverySuppressesQueue(record TranscriptDeliveryRecord) bool {
	switch record.Status {
	case TranscriptDeliveryStatusSent, TranscriptDeliveryStatusSkipped:
		return true
	case TranscriptDeliveryStatusAccepted:
		return strings.TrimSpace(record.TeamsMessageID) != ""
	default:
		return false
	}
}

func (s *Store) RecordTranscriptDelivery(ctx context.Context, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint) (TranscriptDeliveryRecord, bool, error) {
	return s.recordTranscriptDelivery(ctx, "", delivery, checkpoint)
}

// RecordTranscriptDeliveryIfParentUnfenced atomically records a transcript
// delivery and its checkpoint only while the parent fork fence is open.
func (s *Store) RecordTranscriptDeliveryIfParentUnfenced(ctx context.Context, parentSessionID string, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint) (TranscriptDeliveryRecord, bool, error) {
	return s.recordTranscriptDelivery(ctx, parentSessionID, delivery, checkpoint)
}

func (s *Store) recordTranscriptDelivery(ctx context.Context, parentSessionID string, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint) (TranscriptDeliveryRecord, bool, error) {
	parentSessionID = strings.TrimSpace(parentSessionID)
	if strings.TrimSpace(delivery.ID) == "" {
		return TranscriptDeliveryRecord{}, false, fmt.Errorf("transcript delivery id is required")
	}
	if delivery.Status == "" {
		delivery.Status = TranscriptDeliveryStatusSkipped
	}
	if out, created, handled, err := s.recordTranscriptDeliverySQLite(ctx, parentSessionID, delivery, checkpoint); handled || err != nil {
		return out, created, err
	}
	update := s.Update
	if delivery.SessionID != "" {
		update = func(ctx context.Context, fn func(*State) error) error {
			return s.UpdateSession(ctx, delivery.SessionID, fn)
		}
	}
	var out TranscriptDeliveryRecord
	created := false
	err := update(ctx, func(state *State) error {
		if err := ensureParentUnfencedLocked(state, parentSessionID); err != nil {
			return err
		}
		now := time.Now()
		if err := validateTranscriptDeliveryCheckpointProvenance(delivery, checkpoint); err != nil {
			return err
		}
		if err := validateTranscriptCheckpointRecordProvenance(state, checkpoint); err != nil {
			return err
		}
		previous, _ := state.ImportCheckpoints[strings.TrimSpace(checkpoint.ID)]
		if stateHasUnresolvedExecution(state, delivery.SessionID) &&
			!importCheckpointIsExplicitHistoryRun(previous) {
			return ErrUnresolvedExecution
		}
		out, created = applyRecordTranscriptDeliveryLocked(state, delivery, checkpoint, now)
		return nil
	})
	return out, created, err
}

func applyRecordTranscriptDeliveryLocked(state *State, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint, now time.Time) (TranscriptDeliveryRecord, bool) {
	if state.TranscriptDeliveries == nil {
		state.TranscriptDeliveries = make(map[string]TranscriptDeliveryRecord)
	}
	if state.ImportCheckpoints == nil {
		state.ImportCheckpoints = make(map[string]ImportCheckpoint)
	}
	if existing, ok := state.TranscriptDeliveries[delivery.ID]; ok {
		applyTranscriptCheckpointLocked(state, checkpoint, now)
		return existing, false
	}
	out := normalizeTranscriptDeliveryRecord(delivery, now)
	state.TranscriptDeliveries[out.ID] = out
	applyTranscriptCheckpointLocked(state, checkpoint, now)
	return out, true
}

func normalizeTranscriptDeliveryRecord(record TranscriptDeliveryRecord, now time.Time) TranscriptDeliveryRecord {
	record.ID = strings.TrimSpace(record.ID)
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.CodexThreadID = strings.TrimSpace(record.CodexThreadID)
	record.CodexTurnID = strings.TrimSpace(record.CodexTurnID)
	record.SourcePath = strings.TrimSpace(record.SourcePath)
	record.SourceRecordID = strings.TrimSpace(record.SourceRecordID)
	record.Kind = strings.TrimSpace(record.Kind)
	record.TextHash = strings.TrimSpace(record.TextHash)
	record.OutboxID = strings.TrimSpace(record.OutboxID)
	record.TeamsMessageID = strings.TrimSpace(record.TeamsMessageID)
	if now.IsZero() {
		now = time.Now()
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	if record.Status == "" {
		record.Status = TranscriptDeliveryStatusSkipped
	}
	return record
}

func (s *Store) UpsertArtifactRecord(ctx context.Context, record ArtifactRecord) (ArtifactRecord, error) {
	if strings.TrimSpace(record.ID) == "" {
		return ArtifactRecord{}, fmt.Errorf("artifact id is required")
	}
	if out, handled, err := s.upsertArtifactRecordSQLite(ctx, record); handled || err != nil {
		return out, err
	}
	update := s.Update
	if strings.TrimSpace(record.SessionID) != "" {
		update = func(ctx context.Context, fn func(*State) error) error {
			return s.UpdateSession(ctx, record.SessionID, fn)
		}
	}
	var out ArtifactRecord
	err := update(ctx, func(state *State) error {
		now := time.Now()
		out = applyUpsertArtifactRecordLocked(state, record, now)
		return nil
	})
	return out, err
}

func applyUpsertArtifactRecordLocked(state *State, record ArtifactRecord, now time.Time) ArtifactRecord {
	if state.ArtifactRecords == nil {
		state.ArtifactRecords = make(map[string]ArtifactRecord)
	}
	record = normalizeArtifactRecord(record, now)
	if existing, ok := state.ArtifactRecords[record.ID]; ok {
		if !existing.CreatedAt.IsZero() {
			record.CreatedAt = existing.CreatedAt
		}
		if record.OutboxID == "" {
			record.OutboxID = existing.OutboxID
		}
		if record.DriveItemID == "" {
			record.DriveItemID = existing.DriveItemID
		}
		if record.TeamsMessageID == "" {
			record.TeamsMessageID = existing.TeamsMessageID
		}
		if record.UploadedAt.IsZero() {
			record.UploadedAt = existing.UploadedAt
		}
		if record.SentAt.IsZero() {
			record.SentAt = existing.SentAt
		}
	}
	state.ArtifactRecords[record.ID] = record
	return record
}

func normalizeArtifactRecord(record ArtifactRecord, now time.Time) ArtifactRecord {
	record.ID = strings.TrimSpace(record.ID)
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.TurnID = strings.TrimSpace(record.TurnID)
	record.Path = strings.TrimSpace(record.Path)
	record.UploadName = strings.TrimSpace(record.UploadName)
	record.DriveItemID = strings.TrimSpace(record.DriveItemID)
	record.OutboxID = strings.TrimSpace(record.OutboxID)
	record.TeamsMessageID = strings.TrimSpace(record.TeamsMessageID)
	record.Status = strings.TrimSpace(record.Status)
	record.StatusReason = trimDiagnostic(record.StatusReason, 240)
	record.Error = trimDiagnostic(record.Error, 240)
	if now.IsZero() {
		now = time.Now()
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	return record
}

func applyTranscriptCheckpointLocked(state *State, checkpoint ImportCheckpoint, now time.Time) {
	if state == nil || strings.TrimSpace(checkpoint.ID) == "" || strings.TrimSpace(checkpoint.LastRecordID) == "" {
		return
	}
	previous := state.ImportCheckpoints[checkpoint.ID]
	status := strings.TrimSpace(checkpoint.Status)
	if status == "" {
		status = previous.Status
	}
	if status == "" || status == "blocked" {
		status = "complete"
	}
	if checkpoint.SessionID == "" {
		checkpoint.SessionID = previous.SessionID
	}
	if checkpoint.SourcePath == "" {
		checkpoint.SourcePath = previous.SourcePath
	}
	if checkpoint.SourceFingerprint == "" {
		checkpoint.SourceFingerprint = previous.SourceFingerprint
	}
	if checkpoint.LastSourceLine == 0 {
		checkpoint.LastSourceLine = previous.LastSourceLine
	}
	if checkpoint.ImportTurnID == "" {
		checkpoint.ImportTurnID = previous.ImportTurnID
	}
	if checkpoint.KindPrefix == "" {
		checkpoint.KindPrefix = previous.KindPrefix
	}
	if checkpoint.LastOffset == 0 {
		checkpoint.LastOffset = previous.LastOffset
	}
	checkpoint.LastOffsetKnown = checkpoint.LastOffsetKnown || previous.LastOffsetKnown || checkpoint.LastOffset != 0 || previous.LastOffset != 0
	if checkpoint.SourceSize == 0 {
		checkpoint.SourceSize = previous.SourceSize
	}
	if checkpoint.SourceModTime.IsZero() {
		checkpoint.SourceModTime = previous.SourceModTime
	}
	if checkpoint.UnresolvedExecution == nil {
		checkpoint.UnresolvedExecution = previous.UnresolvedExecution
	}
	if now.IsZero() {
		now = time.Now()
	}
	checkpoint.Status = status
	checkpoint.UpdatedAt = now
	state.ImportCheckpoints[checkpoint.ID] = checkpoint
}

func (s *Store) MarkOutboxSendAttempt(ctx context.Context, outboxID string) (OutboxMessage, error) {
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, true, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return claimOutboxSendAttemptLocked(state, msg, now)
	}); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return claimOutboxSendAttemptLocked(state, msg, now)
	})
}

func claimOutboxSendAttemptLocked(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
	if msg.BlockedBySourceRewrite || outboxLegacyTranscriptSourceRewriteBlocked(state, msg) {
		return msg, ErrOutboxSendNotClaimed
	}
	if outboxSendBlockedByUnresolvedExecution(state, msg) {
		// The checkpoint/claim decision is made under the same store transaction
		// as the send lease. Returning the normal not-claimed sentinel leaves the
		// message queued for a later pass after ownership is confirmed.
		return msg, ErrOutboxSendNotClaimed
	}
	if sessionID := strings.TrimSpace(msg.SessionID); sessionID != "" {
		session, ok := state.Sessions[sessionID]
		if ok && !sessionStatusIsActive(session.Status) && !forkHistoryOutboxMaySend(state, msg) {
			return msg, ErrOutboxSendNotClaimed
		}
	}
	if parentSessionID := transcriptDeliveryParentFenceSessionID(msg); parentSessionID != "" {
		if _, fenced := activeForkForSessionLocked(state, parentSessionID); fenced {
			return msg, ErrOutboxSendNotClaimed
		}
	}
	switch msg.Status {
	case OutboxStatusQueued:
	case OutboxStatusSending:
		if !msg.LastSendAttempt.IsZero() && now.Sub(msg.LastSendAttempt) <= outboxSendLease {
			return msg, ErrOutboxSendNotClaimed
		}
	default:
		return msg, ErrOutboxSendNotClaimed
	}
	msg.Status = OutboxStatusSending
	msg.LastSendAttempt = now
	msg.SendAttemptToken = outboxSendAttemptToken(msg.ID, now, msg.SendAttemptToken)
	msg.LastSendError = ""
	updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSending, now)
	return msg, nil
}

func transcriptDeliveryParentFenceSessionID(msg OutboxMessage) string {
	if parentSessionID := strings.TrimSpace(msg.ParentFenceSessionID); parentSessionID != "" {
		return parentSessionID
	}
	if strings.HasPrefix(strings.TrimSpace(msg.ID), "outbox:transcript-delivery:") {
		return strings.TrimSpace(msg.SessionID)
	}
	return ""
}

func outboxSendAttemptToken(outboxID string, now time.Time, previousToken string) string {
	previousToken = strings.TrimSpace(previousToken)
	payload := strings.TrimSpace(outboxID) + "\x00" + now.UTC().Format(time.RFC3339Nano) + "\x00" + previousToken
	sum := sha256.Sum256([]byte(payload))
	token := hex.EncodeToString(sum[:16])
	if token == previousToken {
		// Keep the invariant explicit even in the astronomically unlikely event
		// of a truncated digest collision.
		sum = sha256.Sum256([]byte(payload + "\x00next"))
		token = hex.EncodeToString(sum[:16])
	}
	return token
}

func (s *Store) SuppressOutboxOwnerMention(ctx context.Context, outboxID string) (OutboxMessage, error) {
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, false, func(_ *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		msg.MentionOwner = false
		return msg, nil
	}); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(_ *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		msg.MentionOwner = false
		return msg, nil
	})
}

func (s *Store) MarkOutboxMathMediaFallback(ctx context.Context, outboxID string) (OutboxMessage, error) {
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, false, func(_ *State, msg OutboxMessage, _ time.Time) (OutboxMessage, error) {
		msg.MathMediaFallback = true
		msg.LastSendError = ""
		return msg, nil
	}); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(_ *State, msg OutboxMessage, _ time.Time) (OutboxMessage, error) {
		msg.MathMediaFallback = true
		msg.LastSendError = ""
		return msg, nil
	})
}

func (s *Store) EarlierUnsentOutbox(ctx context.Context, msg OutboxMessage) (OutboxMessage, bool, error) {
	chatID := strings.TrimSpace(msg.TeamsChatID)
	if chatID == "" || msg.Sequence <= 0 {
		return OutboxMessage{}, false, nil
	}
	if earlier, found, handled, err := s.earlierUnsentOutboxSQLite(ctx, msg); handled || err != nil {
		return earlier, found, err
	}
	state, err := s.OutboxStateSnapshot(ctx)
	if err != nil {
		return OutboxMessage{}, false, err
	}
	var earlier OutboxMessage
	found := false
	for _, candidate := range state.OutboxMessages {
		if candidate.ID == msg.ID || candidate.TeamsChatID != chatID || candidate.Sequence <= 0 || candidate.Sequence >= msg.Sequence {
			continue
		}
		switch candidate.Status {
		case OutboxStatusSent, OutboxStatusSkipped:
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
	return s.markOutboxSendError(ctx, outboxID, "", message, false)
}

func (s *Store) MarkOutboxSendErrorForAttempt(ctx context.Context, outboxID string, attemptToken string, message string) (OutboxMessage, error) {
	return s.markOutboxSendError(ctx, outboxID, attemptToken, message, true)
}

// MarkOutboxAmbiguousSendErrorForAttempt keeps an attempted message under its
// send lease after Graph may have accepted the POST but failed to return a
// usable response. Leaving the row in sending prevents an immediate duplicate
// POST while inbound echo reconciliation or the normal Graph recovery read has
// time to discover the accepted message.
func (s *Store) MarkOutboxAmbiguousSendErrorForAttempt(ctx context.Context, outboxID string, attemptToken string, message string) (OutboxMessage, error) {
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, true, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return markOutboxAmbiguousSendErrorLocked(state, msg, attemptToken, message, now)
	}); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return markOutboxAmbiguousSendErrorLocked(state, msg, attemptToken, message, now)
	})
}

func (s *Store) MarkOutboxSkippedForAttempt(ctx context.Context, outboxID string, attemptToken string, reason string) (OutboxMessage, error) {
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, true, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return markOutboxSkippedForAttemptLocked(state, msg, attemptToken, reason, now)
	}); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return markOutboxSkippedForAttemptLocked(state, msg, attemptToken, reason, now)
	})
}

func markOutboxSkippedForAttemptLocked(state *State, msg OutboxMessage, attemptToken string, reason string, now time.Time) (OutboxMessage, error) {
	attemptToken = strings.TrimSpace(attemptToken)
	if msg.Status != OutboxStatusSending || attemptToken == "" || strings.TrimSpace(msg.SendAttemptToken) != attemptToken {
		return msg, ErrOutboxSendNotClaimed
	}
	msg.Status = OutboxStatusSkipped
	msg.LastSendError = trimDiagnostic(firstStoreNonEmptyString(reason, "ambiguous transient output superseded"), 240)
	msg.UpdatedAt = now
	updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSkipped, now)
	markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusSkipped, now)
	updateArtifactRecordsForOutboxLocked(state, msg, now, "skipped", msg.LastSendError, "")
	return msg, nil
}

func markOutboxAmbiguousSendErrorLocked(state *State, msg OutboxMessage, attemptToken string, message string, now time.Time) (OutboxMessage, error) {
	attemptToken = strings.TrimSpace(attemptToken)
	if msg.Status != OutboxStatusSending || attemptToken == "" || strings.TrimSpace(msg.SendAttemptToken) != attemptToken {
		return msg, ErrOutboxSendNotClaimed
	}
	if msg.BlockedByTerminalFailure {
		// The request may already have reached Graph, so keep the in-flight
		// attempt fenced instead of re-queueing it for a duplicate POST.  A late
		// accepted callback can still persist the stable Teams message ID.
		msg.LastSendError = trimDiagnostic("ambiguous Graph send after terminal failure: "+message, 240)
		msg.LastSendAttempt = now
		msg.UpdatedAt = now
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusFailed, now)
		updateArtifactRecordsForOutboxLocked(state, msg, now, "message_ambiguous", "terminal failure fence", msg.LastSendError)
		return msg, nil
	}
	if sessionID := strings.TrimSpace(msg.SessionID); sessionID != "" && state.Sessions[sessionID].Status == SessionStatusQuarantined {
		msg.Status = OutboxStatusSkipped
		msg.LastSendError = trimDiagnostic("session quarantined after ambiguous send: "+message, 240)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSkipped, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusSkipped, now)
		updateArtifactRecordsForOutboxLocked(state, msg, now, "skipped", msg.LastSendError, "")
		msg.UpdatedAt = now
		return msg, nil
	}
	msg.LastSendError = trimDiagnostic("ambiguous Graph send; retry held by send lease: "+message, 240)
	msg.LastSendAttempt = now
	msg.UpdatedAt = now
	updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusFailed, now)
	updateArtifactRecordsForOutboxLocked(state, msg, now, "message_ambiguous", "", msg.LastSendError)
	return msg, nil
}

func (s *Store) markOutboxSendError(ctx context.Context, outboxID string, attemptToken string, message string, requireClaim bool) (OutboxMessage, error) {
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, true, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return markOutboxSendErrorLocked(state, msg, attemptToken, message, now, requireClaim)
	}); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		return markOutboxSendErrorLocked(state, msg, attemptToken, message, now, requireClaim)
	})
}

func markOutboxSendErrorLocked(state *State, msg OutboxMessage, attemptToken string, message string, now time.Time, requireClaim bool) (OutboxMessage, error) {
	if requireClaim && (msg.Status != OutboxStatusSending || strings.TrimSpace(attemptToken) != "" && strings.TrimSpace(msg.SendAttemptToken) != strings.TrimSpace(attemptToken)) {
		return msg, ErrOutboxSendNotClaimed
	}
	if !requireClaim && msg.Status != OutboxStatusQueued && msg.Status != OutboxStatusSending {
		return msg, ErrOutboxSendNotClaimed
	}
	if msg.BlockedByTerminalFailure {
		msg.Status = OutboxStatusSkipped
		msg.LastSendError = trimDiagnostic("terminal failure fence: "+message, 240)
		msg.SendAttemptToken = ""
		msg.UpdatedAt = now
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSkipped, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusSkipped, now)
		updateArtifactRecordsForOutboxLocked(state, msg, now, "skipped", msg.LastSendError, "")
		return msg, nil
	}
	quarantined := false
	if sessionID := strings.TrimSpace(msg.SessionID); sessionID != "" {
		quarantined = state.Sessions[sessionID].Status == SessionStatusQuarantined
	}
	if quarantined {
		msg.Status = OutboxStatusSkipped
		msg.LastSendError = trimDiagnostic("session quarantined after send failure: "+message, 240)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSkipped, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusSkipped, now)
	} else {
		msg.Status = OutboxStatusQueued
		msg.LastSendError = trimDiagnostic(message, 240)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusFailed, now)
	}
	msg.LastSendAttempt = now
	msg.UpdatedAt = now
	updateArtifactRecordsForOutboxLocked(state, msg, now, artifactStatusForSendError(msg), "", msg.LastSendError)
	return msg, nil
}

func (s *Store) MarkOutboxDriveItem(ctx context.Context, outboxID string, itemID string, name string, eTag string, webURL string, webDavURL string) (OutboxMessage, error) {
	return s.markOutboxDriveItem(ctx, outboxID, "", itemID, name, eTag, webURL, webDavURL, false)
}

// MarkOutboxDriveItemForAttempt records an uploaded DriveItem only while the
// caller still owns the send lease. A large upload can outlive the lease if it
// stops making progress; without this fence an old worker could attach its
// DriveItem to a newer attempt after another worker reclaimed the outbox row.
func (s *Store) MarkOutboxDriveItemForAttempt(ctx context.Context, outboxID string, attemptToken string, itemID string, name string, eTag string, webURL string, webDavURL string) (OutboxMessage, error) {
	attemptToken = strings.TrimSpace(attemptToken)
	if strings.TrimSpace(outboxID) == "" || attemptToken == "" {
		return OutboxMessage{}, ErrOutboxSendNotClaimed
	}
	return s.markOutboxDriveItem(ctx, outboxID, attemptToken, itemID, name, eTag, webURL, webDavURL, true)
}

func (s *Store) markOutboxDriveItem(ctx context.Context, outboxID string, attemptToken string, itemID string, name string, eTag string, webURL string, webDavURL string, requireAttempt bool) (OutboxMessage, error) {
	update := func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		if requireAttempt && (msg.Status != OutboxStatusSending || strings.TrimSpace(msg.SendAttemptToken) != attemptToken) {
			return msg, ErrOutboxSendNotClaimed
		}
		msg.DriveItemID = strings.TrimSpace(itemID)
		msg.DriveItemName = strings.TrimSpace(name)
		msg.DriveItemETag = strings.TrimSpace(eTag)
		msg.DriveItemWebURL = strings.TrimSpace(webURL)
		msg.DriveItemWebDav = strings.TrimSpace(webDavURL)
		msg.LastSendError = ""
		updateArtifactRecordsForOutboxLocked(state, msg, now, "drive_uploaded", "", "")
		return msg, nil
	}
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, true, update); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, update)
}

// MarkOutboxUploadSessionForAttempt durably records the resumable upload
// session owned by the current send lease. The URL is pre-authenticated and
// must never be reused by a different outbox attempt, so updates are fenced by
// the send-attempt token. Recording progress also renews the send lease while
// a large file is actively making progress.
func (s *Store) MarkOutboxUploadSessionForAttempt(ctx context.Context, outboxID string, attemptToken string, uploadURL string, expiresAt time.Time, offset int64) (OutboxMessage, error) {
	outboxID = strings.TrimSpace(outboxID)
	attemptToken = strings.TrimSpace(attemptToken)
	if outboxID == "" || attemptToken == "" {
		return OutboxMessage{}, ErrOutboxSendNotClaimed
	}
	if offset < 0 {
		return OutboxMessage{}, fmt.Errorf("upload session offset must not be negative")
	}
	update := func(_ *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		if msg.Status != OutboxStatusSending || strings.TrimSpace(msg.SendAttemptToken) != attemptToken {
			return msg, ErrOutboxSendNotClaimed
		}
		if strings.TrimSpace(uploadURL) == "" {
			msg.AttachmentUploadURL = ""
			msg.AttachmentUploadExpiry = time.Time{}
			msg.AttachmentUploadOffset = 0
		} else {
			msg.AttachmentUploadURL = strings.TrimSpace(uploadURL)
			msg.AttachmentUploadExpiry = expiresAt
			msg.AttachmentUploadOffset = offset
		}
		msg.LastSendAttempt = now
		msg.LastSendError = ""
		return msg, nil
	}
	if out, handled, err := s.updateOutboxSQLite(ctx, outboxID, false, false, update); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, update)
}

func (s *Store) MarkOutboxAccepted(ctx context.Context, outboxID string, teamsMessageID string) (OutboxMessage, error) {
	return s.markOutboxAccepted(ctx, outboxID, "", teamsMessageID, false)
}

func (s *Store) MarkOutboxAcceptedForAttempt(ctx context.Context, outboxID string, attemptToken string, teamsMessageID string) (OutboxMessage, error) {
	return s.markOutboxAccepted(ctx, outboxID, attemptToken, teamsMessageID, true)
}

// MarkOutboxAcceptedSourceRewriteForAttempt records a Graph-accepted
// transcript row whose source proof changed before the normal Sent CAS. It
// retains the stable Teams ID while making the no-retry/no-side-effect fence
// durable across helper restarts.
func (s *Store) MarkOutboxAcceptedSourceRewriteForAttempt(ctx context.Context, outboxID string, attemptToken string, teamsMessageID string) (OutboxMessage, error) {
	update := func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		if err := validateOutboxDeliveryAttempt(msg, attemptToken, teamsMessageID, false, true); err != nil {
			return msg, err
		}
		msg.Status = OutboxStatusAccepted
		if strings.TrimSpace(teamsMessageID) != "" {
			msg.TeamsMessageID = strings.TrimSpace(teamsMessageID)
		}
		msg.BlockedBySourceRewrite = true
		msg.LastSendError = ""
		msg.SendAttemptToken = ""
		recordOutboxProvenanceLocked(state, msg, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusAccepted, now)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusAccepted, now)
		return msg, nil
	}
	if out, handled, err := s.updateOutboxSQLite(ctx, strings.TrimSpace(outboxID), false, true, update); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, update)
}

// MarkOutboxSourceRewriteFence reconciles a Graph-accepted transcript row
// discovered by a replay ledger or a post-acceptance source check.  Unlike the
// attempt-scoped variant above, replay may have already cleared the send lease,
// so the durable source fence is validated by outbox identity and stable Graph
// message ID rather than an in-memory attempt token.  The row can never be
// promoted to Sent or retried after this transition.
func (s *Store) MarkOutboxSourceRewriteFence(ctx context.Context, outboxID string, teamsMessageID string) (OutboxMessage, error) {
	outboxID = strings.TrimSpace(outboxID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if outboxID == "" || teamsMessageID == "" {
		return OutboxMessage{}, ErrOutboxSendNotClaimed
	}
	update := func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		if msg.Status == OutboxStatusSkipped {
			return msg, ErrOutboxSendNotClaimed
		}
		if existing := strings.TrimSpace(msg.TeamsMessageID); existing != "" && existing != teamsMessageID {
			return msg, ErrOutboxSendNotClaimed
		}
		// A source check can race with the final Sent CAS.  Demote that row back
		// to Accepted and persist the fence instead of treating Sent as immutable;
		// otherwise a post-CAS rewrite would still run transcript side effects or
		// be replay-promoted after restart.
		msg.Status = OutboxStatusAccepted
		msg.TeamsMessageID = teamsMessageID
		msg.BlockedBySourceRewrite = true
		msg.LastSendError = ""
		msg.SendAttemptToken = ""
		recordOutboxProvenanceLocked(state, msg, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusAccepted, now)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusAccepted, now)
		return msg, nil
	}
	if out, handled, err := s.updateOutboxSQLite(ctx, outboxID, false, true, update); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, update)
}

func (s *Store) markOutboxAccepted(ctx context.Context, outboxID string, attemptToken string, teamsMessageID string, requireClaim bool) (OutboxMessage, error) {
	if out, handled, err := s.markOutboxDeliveredSQLite(ctx, strings.TrimSpace(outboxID), attemptToken, teamsMessageID, false, requireClaim); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		if err := validateOutboxDeliveryAttempt(msg, attemptToken, teamsMessageID, false, requireClaim); err != nil {
			return msg, err
		}
		if err := markOutboxDeliveryBlockedIfUnresolvedExecution(state, &msg, teamsMessageID); err != nil {
			return msg, err
		}
		if msg.Status == OutboxStatusSent {
			recordOutboxProvenanceLocked(state, msg, now)
			updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSent, now)
			updateArtifactRecordsForOutboxLocked(state, msg, now, "uploaded", "", "")
			return msg, nil
		}
		msg.Status = OutboxStatusAccepted
		if teamsMessageID != "" {
			msg.TeamsMessageID = teamsMessageID
		}
		msg.LastSendError = ""
		recordOutboxProvenanceLocked(state, msg, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusAccepted, now)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusAccepted, now)
		return msg, nil
	})
}

func (s *Store) MarkOutboxSent(ctx context.Context, outboxID string, teamsMessageID string) (OutboxMessage, error) {
	return s.markOutboxSent(ctx, outboxID, "", teamsMessageID, false)
}

func (s *Store) MarkOutboxSentForAttempt(ctx context.Context, outboxID string, attemptToken string, teamsMessageID string) (OutboxMessage, error) {
	return s.markOutboxSent(ctx, outboxID, attemptToken, teamsMessageID, true)
}

// ApplyOutboxReplayFences promotes already-delivered canonical outbox rows in
// one transaction. It is intentionally separate from the normal send path so
// regular delivery does not gain a global-ledger lookup.
func (s *Store) ApplyOutboxReplayFences(ctx context.Context, fences []OutboxReplayFence) (int, error) {
	fences, err := normalizeOutboxReplayFences(fences)
	if err != nil || len(fences) == 0 {
		return 0, err
	}
	if changed, handled, err := s.applyOutboxReplayFencesSQLite(ctx, fences); handled || err != nil {
		return changed, err
	}
	changed := 0
	err = s.UpdateIfChanged(ctx, func(state *State) (bool, error) {
		for _, fence := range fences {
			current, ok := state.OutboxMessages[fence.OutboxID]
			if !ok {
				continue
			}
			if err := validateOutboxReplayFence(current, fence); err != nil {
				return false, err
			}
			if err := validateOutboxReplayCheckpointProvenance(state, current); err != nil {
				return false, err
			}
		}
		now := time.Now()
		for _, fence := range fences {
			current, ok := state.OutboxMessages[fence.OutboxID]
			if !ok || current.Status == OutboxStatusSent {
				continue
			}
			current = applyOutboxSentProjectionLocked(state, current, fence.TeamsMessageID, now)
			state.OutboxMessages[current.ID] = current
			changed++
		}
		return changed > 0, nil
	})
	return changed, err
}

// validateOutboxReplayCheckpointProvenance keeps the JSON replay path aligned
// with SQLite.  Automatic transcript/status rows that predate source proofs
// must have a canonical, session-owned checkpoint before a legacy Graph
// delivery can be promoted.  A missing or malformed checkpoint is ambiguity,
// not evidence that the row is safe to send.  Explicit history rows and
// messages with their own source proof are validated by their respective
// paths and are intentionally not subject to this legacy probe.
func validateOutboxReplayCheckpointProvenance(state *State, msg OutboxMessage) error {
	if state == nil || strings.TrimSpace(msg.SessionID) == "" ||
		outboxTurnIsUserExplicitHistory(msg.TurnID) ||
		strings.TrimSpace(msg.TranscriptSourceProofFingerprint) != "" {
		return nil
	}
	kind := strings.ToLower(strings.TrimSpace(msg.Kind))
	legacySyncOrImport := strings.HasPrefix(kind, "sync-") || strings.HasPrefix(kind, "import-")
	legacyCodexStatus := strings.HasPrefix(kind, "codex-status-") &&
		(strings.HasPrefix(strings.TrimSpace(msg.TurnID), "sync:") ||
			strings.HasPrefix(strings.TrimSpace(msg.TurnID), "import:") ||
			strings.TrimSpace(msg.TranscriptCheckpointID) != "" ||
			strings.TrimSpace(msg.TranscriptSourcePath) != "")
	if !legacySyncOrImport && !legacyCodexStatus {
		return nil
	}
	checkpointID := transcriptCheckpointIDForSession(msg.SessionID)
	checkpoint, found := state.ImportCheckpoints[checkpointID]
	if !found {
		return fmt.Errorf("%w: canonical checkpoint %q is missing for legacy outbox %q", ErrSessionStateProvenanceMismatch, checkpointID, msg.ID)
	}
	return validateLoadedTranscriptCheckpointRow(checkpoint, checkpointID, msg.SessionID)
}

func normalizeOutboxReplayFences(fences []OutboxReplayFence) ([]OutboxReplayFence, error) {
	normalized := make([]OutboxReplayFence, 0, len(fences))
	byID := make(map[string]OutboxReplayFence, len(fences))
	for _, fence := range fences {
		fence.OutboxID = strings.TrimSpace(fence.OutboxID)
		fence.TeamsChatID = strings.TrimSpace(fence.TeamsChatID)
		fence.TeamsMessageID = strings.TrimSpace(fence.TeamsMessageID)
		fence.SessionID = strings.TrimSpace(fence.SessionID)
		fence.TurnID = strings.TrimSpace(fence.TurnID)
		fence.Kind = strings.TrimSpace(fence.Kind)
		fence.SourcePath = strings.TrimSpace(fence.SourcePath)
		fence.SourceFingerprint = strings.TrimSpace(fence.SourceFingerprint)
		if fence.OutboxID == "" || fence.TeamsMessageID == "" {
			continue
		}
		if existing, ok := byID[fence.OutboxID]; ok {
			if existing != fence {
				return nil, fmt.Errorf("conflicting replay fences for outbox %q", fence.OutboxID)
			}
			continue
		}
		byID[fence.OutboxID] = fence
		normalized = append(normalized, fence)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].OutboxID < normalized[j].OutboxID
	})
	return normalized, nil
}

func validateOutboxReplayFence(current OutboxMessage, fence OutboxReplayFence) error {
	if current.ID != fence.OutboxID ||
		(fence.TeamsChatID != "" && strings.TrimSpace(current.TeamsChatID) != fence.TeamsChatID) ||
		(fence.SessionID != "" && strings.TrimSpace(current.SessionID) != fence.SessionID) ||
		(fence.TurnID != "" && strings.TrimSpace(current.TurnID) != fence.TurnID) ||
		(fence.Kind != "" && strings.TrimSpace(current.Kind) != fence.Kind) {
		return fmt.Errorf("legacy delivery identity conflicts with canonical outbox %q", fence.OutboxID)
	}
	if existing := strings.TrimSpace(current.TeamsMessageID); existing != "" && existing != fence.TeamsMessageID {
		return fmt.Errorf("legacy Teams message id conflicts with canonical outbox %q", fence.OutboxID)
	}
	if fence.SourcePath != "" && strings.TrimSpace(current.TranscriptSourcePath) != fence.SourcePath {
		return fmt.Errorf("legacy source path conflicts with canonical outbox %q", fence.OutboxID)
	}
	if fence.SourceFingerprint != "" && strings.TrimSpace(current.TranscriptSourceProofFingerprint) != fence.SourceFingerprint {
		return fmt.Errorf("legacy source proof conflicts with canonical outbox %q", fence.OutboxID)
	}
	if fence.SourceOffsetKnown && (!current.TranscriptSourceProofOffsetKnown || current.TranscriptSourceProofOffset != fence.SourceOffset) {
		return fmt.Errorf("legacy source offset conflicts with canonical outbox %q", fence.OutboxID)
	}
	return nil
}

func (s *Store) markOutboxSent(ctx context.Context, outboxID string, attemptToken string, teamsMessageID string, requireClaim bool) (OutboxMessage, error) {
	if out, handled, err := s.markOutboxDeliveredSQLite(ctx, strings.TrimSpace(outboxID), attemptToken, teamsMessageID, true, requireClaim); handled || err != nil {
		return out, err
	}
	return s.updateOutbox(ctx, outboxID, func(state *State, msg OutboxMessage, now time.Time) (OutboxMessage, error) {
		if err := validateOutboxDeliveryAttempt(msg, attemptToken, teamsMessageID, true, requireClaim); err != nil {
			return msg, err
		}
		if err := markOutboxDeliveryBlockedIfUnresolvedExecution(state, &msg, teamsMessageID); err != nil {
			return msg, err
		}
		return applyOutboxSentProjectionLocked(state, msg, teamsMessageID, now), nil
	})
}

func applyOutboxSentProjectionLocked(state *State, msg OutboxMessage, teamsMessageID string, now time.Time) OutboxMessage {
	// Replay and the final delivery CAS both pass through this projection.  A
	// transcript row may be checked by the bridge immediately before the store
	// transaction, so revalidate its persisted source proof here as the last
	// durable fence.  If the source changed (or a legacy automatic row has no
	// proof), retain the stable Teams identity but never promote the row to Sent.
	if outboxLegacyTranscriptSourceRewriteBlocked(state, msg) ||
		(outboxSourceProofRequired(msg) && !outboxSourceProofValid(msg)) {
		return applyOutboxSourceRewriteProjectionLocked(state, msg, teamsMessageID, now)
	}
	if msg.BlockedBySourceRewrite {
		msg.Status = OutboxStatusAccepted
		if teamsMessageID = strings.TrimSpace(teamsMessageID); teamsMessageID != "" {
			msg.TeamsMessageID = teamsMessageID
		}
		msg.UpdatedAt = now
		msg.LastSendError = ""
		msg.SendAttemptToken = ""
		recordOutboxProvenanceLocked(state, msg, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusAccepted, now)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusAccepted, now)
		return msg
	}
	// Replay recovery is a delivery identity reconciliation, not an ownership
	// proof.  Reuse the same durable unresolved/terminal fence as the normal
	// send CAS so a stale global-ledger entry cannot promote a final after a
	// failure or orphan continuation has already won.
	if outboxSendBlockedByUnresolvedExecution(state, msg) {
		msg.BlockedByUnresolvedExecution = true
	}
	if outboxTerminalFailureFenceActive(state, msg) {
		msg.BlockedByTerminalFailure = true
	}
	if msg.BlockedByUnresolvedExecution || msg.BlockedByTerminalFailure {
		// Graph already accepted the message, but ownership became ambiguous
		// before the final durable callback.  Keep the stable identity in the
		// Accepted state.  Neither an unresolved anchor nor a terminal-failure
		// fence may promote the row to Sent, because that would advance the
		// transcript checkpoint and turn-completion side effects.
		msg.Status = OutboxStatusAccepted
		if teamsMessageID = strings.TrimSpace(teamsMessageID); teamsMessageID != "" {
			msg.TeamsMessageID = teamsMessageID
		}
		msg.UpdatedAt = now
		msg.LastSendError = ""
		msg.SendAttemptToken = ""
		recordOutboxProvenanceLocked(state, msg, now)
		markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusAccepted, now)
		updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusAccepted, now)
		return msg
	}
	msg.Status = OutboxStatusSent
	if msg.SentAt.IsZero() {
		msg.SentAt = now
	}
	if teamsMessageID = strings.TrimSpace(teamsMessageID); teamsMessageID != "" {
		msg.TeamsMessageID = teamsMessageID
	}
	msg.UpdatedAt = now
	msg.LastSendError = ""
	msg.SendAttemptToken = ""
	msg.BlockedByUnresolvedExecution = false
	msg.BlockedByTerminalFailure = false
	recordOutboxProvenanceLocked(state, msg, now)
	markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusSent, now)
	updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSent, now)
	updateArtifactRecordsForOutboxLocked(state, msg, now, "uploaded", "", "")
	return msg
}

func applyOutboxSourceRewriteProjectionLocked(state *State, msg OutboxMessage, teamsMessageID string, now time.Time) OutboxMessage {
	msg.Status = OutboxStatusAccepted
	if teamsMessageID = strings.TrimSpace(teamsMessageID); teamsMessageID != "" {
		msg.TeamsMessageID = teamsMessageID
	}
	msg.BlockedBySourceRewrite = true
	msg.UpdatedAt = now
	msg.LastSendError = ""
	msg.SendAttemptToken = ""
	recordOutboxProvenanceLocked(state, msg, now)
	markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusAccepted, now)
	updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusAccepted, now)
	return msg
}

func outboxSourceProofRequired(msg OutboxMessage) bool {
	if outboxTurnIsExplicitHistory(msg.TurnID) {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(msg.Kind))
	if strings.EqualFold(strings.TrimSpace(msg.NotificationKind), "needs_attention") ||
		strings.HasSuffix(kind, "-needs-attention") || kind == "needs-attention" {
		return false
	}
	if strings.TrimSpace(msg.TranscriptCheckpointID) != "" || strings.TrimSpace(msg.TranscriptSourcePath) != "" ||
		strings.TrimSpace(msg.TranscriptSourceProofFingerprint) != "" || msg.TranscriptSourceProofOffsetKnown {
		return true
	}
	// Source-less legacy sync rows are handled by the state-aware
	// outboxLegacyTranscriptSourceRewriteBlocked fence before this predicate is
	// used. Returning false here means only that the row has no own proof to
	// validate; it must not be treated as evidence that the row is safe to send.
	return false
}

func outboxLegacyTranscriptSourceRewriteBlocked(state *State, msg OutboxMessage) bool {
	if state == nil || outboxTurnIsUserExplicitHistory(msg.TurnID) {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(msg.Kind))
	if strings.EqualFold(strings.TrimSpace(msg.NotificationKind), "needs_attention") ||
		strings.HasSuffix(kind, "-needs-attention") || kind == "needs-attention" ||
		strings.HasPrefix(kind, "sync-status-") || strings.HasPrefix(kind, "import-title") || strings.HasPrefix(kind, "import-complete") {
		return false
	}
	legacySyncOrImport := strings.HasPrefix(kind, "sync-") || strings.HasPrefix(kind, "import-")
	legacyCodexStatus := strings.HasPrefix(kind, "codex-status-") &&
		(strings.HasPrefix(strings.TrimSpace(msg.TurnID), "sync:") ||
			strings.HasPrefix(strings.TrimSpace(msg.TurnID), "import:") ||
			strings.TrimSpace(msg.TranscriptCheckpointID) != "" ||
			strings.TrimSpace(msg.TranscriptSourcePath) != "")
	if !legacySyncOrImport && !legacyCodexStatus {
		return false
	}
	if strings.TrimSpace(msg.SessionID) == "" {
		// An automatic transcript row without a session cannot be associated
		// with any canonical checkpoint, so it is never safe to promote.
		return true
	}
	if strings.TrimSpace(msg.TranscriptSourceProofFingerprint) != "" {
		// A complete explicit proof is checked by outboxSourceProofValid at the
		// final CAS. A partial proof is malformed state and must not bypass the
		// legacy fence.
		return strings.TrimSpace(msg.TranscriptSourcePath) == "" || !msg.TranscriptSourceProofOffsetKnown || msg.TranscriptSourceProofOffset < 0
	}
	checkpointID := transcriptCheckpointIDForSession(msg.SessionID)
	checkpoint, ok := state.ImportCheckpoints[checkpointID]
	if !ok {
		// A malformed SQLite projection may key the row by its embedded ID.  A
		// missing canonical row is not proof that the legacy message is safe;
		// fail closed rather than allowing replay to promote it.  This is also
		// the source-less pre-upgrade case: without a canonical row there is no
		// durable boundary against a rewritten transcript.
		for key, candidate := range state.ImportCheckpoints {
			if strings.TrimSpace(key) == checkpointID {
				return true
			}
			if strings.TrimSpace(candidate.ID) == checkpointID || strings.TrimSpace(candidate.SessionID) == strings.TrimSpace(msg.SessionID) {
				return true
			}
		}
		return true
	}
	if err := validateImportCheckpointProvenance(checkpoint, msg.SessionID, checkpointID); err != nil {
		return true
	}
	if checkpoint.SourceRewriteBlocked {
		return true
	}
	if strings.TrimSpace(checkpoint.SourcePath) != "" && strings.TrimSpace(checkpoint.SourceFingerprint) != "" &&
		(checkpoint.LastOffsetKnown || checkpoint.LastOffset > 0) {
		actual, err := sourceCheckpointFingerprintAtOffset(checkpoint.SourcePath, checkpoint.LastOffset)
		return err != nil || strings.TrimSpace(actual) != strings.TrimSpace(checkpoint.SourceFingerprint)
	}
	// A source-less automatic row, or a legacy checkpoint without a durable
	// source proof, cannot be distinguished from a stale tail after a rewrite.
	// Keep it queued until an explicit history operation establishes a boundary.
	return true
}

func outboxSourceProofValid(msg OutboxMessage) bool {
	if !outboxSourceProofRequired(msg) {
		return true
	}
	path := strings.TrimSpace(msg.TranscriptSourcePath)
	expected := strings.TrimSpace(msg.TranscriptSourceProofFingerprint)
	if path == "" || expected == "" || !msg.TranscriptSourceProofOffsetKnown || msg.TranscriptSourceProofOffset < 0 {
		return false
	}
	actual, err := sourceCheckpointFingerprintAtOffset(path, msg.TranscriptSourceProofOffset)
	return err == nil && strings.TrimSpace(actual) == expected
}

func validateOutboxDeliveryAttempt(msg OutboxMessage, attemptToken string, teamsMessageID string, sent bool, requireClaim bool) error {
	attemptToken = strings.TrimSpace(attemptToken)
	if requireClaim && (attemptToken == "" || strings.TrimSpace(msg.SendAttemptToken) != attemptToken) {
		return ErrOutboxSendNotClaimed
	}
	if msg.Status == OutboxStatusSkipped {
		return ErrOutboxSendNotClaimed
	}
	if requireClaim {
		allowed := msg.Status == OutboxStatusSending
		if sent {
			allowed = allowed || msg.Status == OutboxStatusAccepted
		}
		if !allowed {
			return ErrOutboxSendNotClaimed
		}
	}
	if existing := strings.TrimSpace(msg.TeamsMessageID); existing != "" && strings.TrimSpace(teamsMessageID) != "" && existing != strings.TrimSpace(teamsMessageID) {
		return ErrOutboxSendNotClaimed
	}
	return nil
}

func markTranscriptDeliveryForOutboxLocked(state *State, msg OutboxMessage, status TranscriptDeliveryStatus, now time.Time) {
	if state == nil || strings.TrimSpace(msg.ID) == "" {
		return
	}
	for id, delivery := range state.TranscriptDeliveries {
		if strings.TrimSpace(delivery.OutboxID) != strings.TrimSpace(msg.ID) {
			continue
		}
		delivery.Status = status
		delivery.TeamsMessageID = firstStoreNonEmptyString(msg.TeamsMessageID, delivery.TeamsMessageID)
		if status == TranscriptDeliveryStatusSent && delivery.SentAt.IsZero() {
			delivery.SentAt = firstStoreNonZeroTime(msg.SentAt, now)
		}
		delivery.UpdatedAt = now
		state.TranscriptDeliveries[id] = delivery
	}
}

func updateHelperDeliveryForOutboxLocked(state *State, msg OutboxMessage, status HelperDeliveryStatus, now time.Time) {
	if state == nil {
		return
	}
	deliveryIDs := make([]string, 0)
	for id, existing := range state.HelperDeliveries {
		if strings.TrimSpace(existing.OutboxID) == strings.TrimSpace(msg.ID) {
			deliveryIDs = append(deliveryIDs, id)
		}
	}
	updateHelperDeliveryForOutboxIDsLocked(state, msg, status, now, deliveryIDs)
}

func updateHelperDeliveryForOutboxIDsLocked(state *State, msg OutboxMessage, status HelperDeliveryStatus, now time.Time, deliveryIDs []string) string {
	if state == nil {
		return ""
	}
	updatedExisting := false
	for _, id := range deliveryIDs {
		existing, ok := state.HelperDeliveries[id]
		if !ok || strings.TrimSpace(existing.OutboxID) != strings.TrimSpace(msg.ID) {
			continue
		}
		existing.Status = status
		existing.TeamsMessageID = firstStoreNonEmptyString(msg.TeamsMessageID, existing.TeamsMessageID)
		if status == HelperDeliveryStatusSent && existing.SentAt.IsZero() {
			existing.SentAt = firstStoreNonZeroTime(msg.SentAt, now)
		}
		existing.UpdatedAt = now
		state.HelperDeliveries[id] = existing
		updatedExisting = true
	}
	if updatedExisting {
		return ""
	}
	record, ok := helperDeliveryRecordFromOutboxLocked(state, msg, status, now)
	if !ok {
		return ""
	}
	if existing, ok := state.HelperDeliveries[record.ID]; ok {
		if !existing.CreatedAt.IsZero() {
			record.CreatedAt = existing.CreatedAt
		}
		if record.CodexThreadID == "" {
			record.CodexThreadID = existing.CodexThreadID
		}
		if record.TeamsMessageID == "" {
			record.TeamsMessageID = existing.TeamsMessageID
		}
		if record.SentAt.IsZero() {
			record.SentAt = existing.SentAt
		}
	}
	state.HelperDeliveries[record.ID] = record
	return record.ID
}

func helperDeliveryRecordFromOutboxLocked(state *State, msg OutboxMessage, status HelperDeliveryStatus, now time.Time) (HelperDeliveryRecord, bool) {
	kindFamily := helperDeliveryKindFamily(msg.Kind)
	if kindFamily == "" || strings.TrimSpace(msg.TeamsChatID) == "" {
		return HelperDeliveryRecord{}, false
	}
	if status == "" {
		status = helperDeliveryStatusFromOutboxStatus(msg.Status)
	}
	sourceHash := strings.TrimSpace(msg.SourceTextHash)
	renderedHash := strings.TrimSpace(msg.RenderedHash)
	visibleHash := firstStoreNonEmptyString(sourceHash, renderedHash)
	if visibleHash == "" && strings.TrimSpace(msg.Body) != "" {
		visibleHash = bodyHash(msg.Body)
	}
	if sourceHash == "" && visibleHash != "" {
		sourceHash = visibleHash
	}
	if renderedHash == "" && strings.TrimSpace(msg.Body) != "" {
		renderedHash = bodyHash(msg.Body)
	}
	record := HelperDeliveryRecord{
		SessionID:      strings.TrimSpace(msg.SessionID),
		TeamsChatID:    strings.TrimSpace(msg.TeamsChatID),
		CodexThreadID:  helperDeliveryCodexThreadIDLocked(state, msg),
		TurnID:         strings.TrimSpace(msg.TurnID),
		Kind:           strings.TrimSpace(msg.Kind),
		KindFamily:     kindFamily,
		SourceTextHash: sourceHash,
		RenderedHash:   renderedHash,
		VisibleHash:    visibleHash,
		OutboxID:       strings.TrimSpace(msg.ID),
		TeamsMessageID: strings.TrimSpace(msg.TeamsMessageID),
		PartIndex:      msg.PartIndex,
		PartCount:      msg.PartCount,
		Status:         status,
		CreatedAt:      msg.CreatedAt,
		UpdatedAt:      now,
		SentAt:         msg.SentAt,
	}
	if record.PartCount <= 0 {
		record.PartCount = 1
	}
	if record.PartIndex <= 0 && record.PartCount == 1 {
		record.PartIndex = 1
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.Status == HelperDeliveryStatusSent && record.SentAt.IsZero() {
		record.SentAt = firstStoreNonZeroTime(msg.SentAt, now)
	}
	record.ID = helperDeliveryID(record)
	return record, record.ID != ""
}

func helperDeliveryCodexThreadIDLocked(state *State, msg OutboxMessage) string {
	if threadID := strings.TrimSpace(msg.CodexThreadID); threadID != "" {
		return threadID
	}
	if state == nil {
		return ""
	}
	if turnID := strings.TrimSpace(msg.TurnID); turnID != "" {
		if turn, ok := state.Turns[turnID]; ok && strings.TrimSpace(turn.CodexThreadID) != "" {
			return strings.TrimSpace(turn.CodexThreadID)
		}
	}
	if sessionID := strings.TrimSpace(msg.SessionID); sessionID != "" {
		if session, ok := state.Sessions[sessionID]; ok {
			return strings.TrimSpace(session.CodexThreadID)
		}
	}
	return ""
}

func helperDeliveryID(record HelperDeliveryRecord) string {
	keyHash := firstStoreNonEmptyString(record.SourceTextHash, record.VisibleHash, record.RenderedHash, record.OutboxID)
	if strings.TrimSpace(record.TeamsChatID) == "" || strings.TrimSpace(record.KindFamily) == "" || keyHash == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(record.TeamsChatID),
		strings.TrimSpace(record.SessionID),
		strings.TrimSpace(record.CodexThreadID),
		strings.TrimSpace(record.KindFamily),
		keyHash,
		fmt.Sprintf("%d/%d", record.PartIndex, record.PartCount),
	}, "\x00")))
	return "helper-delivery:" + hex.EncodeToString(sum[:])
}

func helperDeliveryKindFamily(kind string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch {
	case strings.Contains(kind, "compact"):
		return "compact"
	case strings.Contains(kind, "progress") || strings.Contains(kind, "status"):
		return "status"
	default:
		return ""
	}
}

func helperDeliveryStatusFromOutboxStatus(status OutboxStatus) HelperDeliveryStatus {
	switch status {
	case OutboxStatusSending:
		return HelperDeliveryStatusSending
	case OutboxStatusAccepted:
		return HelperDeliveryStatusAccepted
	case OutboxStatusSent:
		return HelperDeliveryStatusSent
	case OutboxStatusSkipped:
		return HelperDeliveryStatusSkipped
	default:
		return HelperDeliveryStatusQueued
	}
}

func updateArtifactRecordsForOutboxLocked(state *State, msg OutboxMessage, now time.Time, status string, reason string, errMessage string) {
	if state == nil || strings.TrimSpace(msg.ID) == "" || len(msg.ArtifactIDs) == 0 {
		return
	}
	for _, id := range msg.ArtifactIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		record := state.ArtifactRecords[id]
		if record.ID == "" {
			record.ID = id
			record.SessionID = msg.SessionID
			record.TurnID = msg.TurnID
			record.Path = msg.AttachmentName
			record.UploadName = msg.AttachmentUploadName
			record.CreatedAt = firstStoreNonZeroTime(msg.CreatedAt, now)
		}
		record.OutboxID = firstStoreNonEmptyString(record.OutboxID, msg.ID)
		record.DriveItemID = firstStoreNonEmptyString(msg.DriveItemID, record.DriveItemID)
		record.TeamsMessageID = firstStoreNonEmptyString(msg.TeamsMessageID, record.TeamsMessageID)
		if status != "" {
			record.Status = status
		}
		if reason != "" {
			record.StatusReason = trimDiagnostic(reason, 240)
		}
		if errMessage != "" {
			record.Error = trimDiagnostic(errMessage, 240)
		} else if status == "uploaded" {
			record.Error = ""
			record.StatusReason = ""
		}
		if status == "drive_uploaded" && record.UploadedAt.IsZero() {
			record.UploadedAt = now
		}
		if status == "uploaded" {
			if record.UploadedAt.IsZero() {
				record.UploadedAt = firstStoreNonZeroTime(msg.SentAt, now)
			}
			if record.SentAt.IsZero() {
				record.SentAt = firstStoreNonZeroTime(msg.SentAt, now)
			}
		}
		record.UpdatedAt = now
		state.ArtifactRecords[id] = record
	}
}

func artifactStatusForSendError(msg OutboxMessage) string {
	if strings.TrimSpace(msg.DriveItemID) != "" {
		return "message_failed"
	}
	return "failed"
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
	lookup, err := s.MessageLookup(ctx, chatID, teamsMessageID)
	if err != nil {
		return false, err
	}
	return lookup.HasDeliveredOutbox, nil
}

func (s *Store) PendingOutboxAt(ctx context.Context, now time.Time) ([]OutboxMessage, error) {
	if now.IsZero() {
		now = time.Now()
	}
	page, err := s.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now})
	if err != nil {
		return nil, err
	}
	pending := append([]OutboxMessage(nil), page.Messages...)
	query := PendingOutboxQuery{Now: now, After: page.NextCursor}
	for page.More {
		page, err = s.PendingOutboxPageAt(ctx, query)
		if err != nil {
			return nil, err
		}
		pending = append(pending, page.Messages...)
		query.After = page.NextCursor
	}
	return pending, nil
}

func (s *Store) PendingOutboxPageAt(ctx context.Context, query PendingOutboxQuery) (PendingOutboxPage, error) {
	if query.Now.IsZero() {
		query.Now = time.Now()
	}
	query.SessionID = strings.TrimSpace(query.SessionID)
	query.TurnID = strings.TrimSpace(query.TurnID)
	query.TeamsChatID = strings.TrimSpace(query.TeamsChatID)
	query.After.ID = strings.TrimSpace(query.After.ID)
	if page, handled, err := s.pendingOutboxPageAtSQLite(ctx, query); handled || err != nil {
		return page, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, pendingOutboxStateFields)
	if err != nil {
		return PendingOutboxPage{}, err
	}
	var candidates []OutboxMessage
	for _, msg := range state.OutboxMessages {
		if !pendingOutboxMatchesQuery(msg, state, query) {
			continue
		}
		if !query.After.IsZero() && !pendingOutboxAfterCursor(msg, query.After) {
			continue
		}
		candidates = append(candidates, msg)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return pendingOutboxPageLess(candidates[i], candidates[j])
	})
	limit := query.Limit
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}
	page := PendingOutboxPage{Messages: append([]OutboxMessage(nil), candidates[:limit]...)}
	if len(candidates) > limit {
		page.More = true
	}
	if limit > 0 {
		last := candidates[limit-1]
		page.NextCursor = PendingOutboxCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

func pendingOutboxMatchesQuery(msg OutboxMessage, state State, query PendingOutboxQuery) bool {
	if query.SessionID != "" && msg.SessionID != query.SessionID {
		return false
	}
	if query.TurnID != "" && msg.TurnID != query.TurnID {
		return false
	}
	if query.TeamsChatID != "" && msg.TeamsChatID != query.TeamsChatID {
		return false
	}
	acceptedWithTeamsID := msg.Status == OutboxStatusAccepted && strings.TrimSpace(msg.TeamsMessageID) != ""
	if !acceptedWithTeamsID {
		if blocked := state.ChatRateLimits[msg.TeamsChatID]; blocked.BlockedUntil.After(query.Now) {
			return false
		}
	}
	return acceptedWithTeamsID ||
		msg.Status == OutboxStatusQueued ||
		msg.Status == OutboxStatusSending && (msg.LastSendAttempt.IsZero() || query.Now.Sub(msg.LastSendAttempt) > outboxSendLease)
}

func pendingOutboxAfterCursor(msg OutboxMessage, cursor PendingOutboxCursor) bool {
	if msg.CreatedAt.After(cursor.CreatedAt) {
		return true
	}
	return msg.CreatedAt.Equal(cursor.CreatedAt) && msg.ID > cursor.ID
}

func pendingOutboxPageLess(left OutboxMessage, right OutboxMessage) bool {
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.Before(right.CreatedAt)
	}
	return left.ID < right.ID
}

func (s *Store) ChatRateLimit(ctx context.Context, chatID string) (ChatRateLimitState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatRateLimitState{}, false, fmt.Errorf("chat id is required")
	}
	if limit, ok, handled, err := s.chatRateLimitSQLite(ctx, chatID); handled || err != nil {
		return limit, ok, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, chatRateLimitStateFields)
	if err != nil {
		return ChatRateLimitState{}, false, err
	}
	limit, ok := state.ChatRateLimits[chatID]
	return limit, ok, nil
}

func (s *Store) SetChatRateLimit(ctx context.Context, chatID string, blockedUntil time.Time, reason string) (ChatRateLimitState, error) {
	return s.SetChatRateLimitForOutbox(ctx, chatID, blockedUntil, reason, "")
}

func (s *Store) SetChatRateLimitForOutbox(ctx context.Context, chatID string, blockedUntil time.Time, reason string, outboxID string) (ChatRateLimitState, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatRateLimitState{}, fmt.Errorf("chat id is required")
	}
	outboxID = strings.TrimSpace(outboxID)
	if out, handled, err := s.setChatRateLimitSQLite(ctx, chatID, blockedUntil, reason, outboxID); handled || err != nil {
		return out, err
	}
	var out ChatRateLimitState
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		next := state.ChatRateLimits[chatID]
		next.ChatID = chatID
		next.BlockedUntil = blockedUntil
		next.Reason = trimDiagnostic(reason, 240)
		if outboxID != "" {
			next.PoisonOutboxID = outboxID
		}
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
	if handled, err := s.clearChatRateLimitSQLite(ctx, chatID); handled || err != nil {
		return err
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
	if poll, ok, handled, err := s.chatPollSQLite(ctx, chatID); handled || err != nil {
		return poll, ok, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, chatPollStateFields)
	if err != nil {
		return ChatPollState{}, false, err
	}
	poll, ok := state.ChatPolls[chatID]
	return poll, ok, nil
}

func (s *Store) ChatSessionActivity(ctx context.Context, chatID string) (bool, bool, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return false, false, nil
	}
	if matched, active, handled, err := s.chatSessionActivitySQLite(ctx, chatID); handled || err != nil {
		return matched, active, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, stateFieldSet("sessions"))
	if err != nil {
		return false, false, err
	}
	matched := false
	active := false
	for _, session := range state.Sessions {
		if strings.TrimSpace(session.TeamsChatID) != chatID {
			continue
		}
		matched = true
		if sessionStatusIsActive(session.Status) {
			active = true
			break
		}
	}
	return matched, active, nil
}

func sessionStatusIsActive(status SessionStatus) bool {
	trimmed := strings.TrimSpace(string(status))
	return trimmed == "" || trimmed == string(SessionStatusActive)
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
	if out, handled, err := s.recordChatPollSuccessWithContinuationAndScheduleSQLite(ctx, chatID, lastModifiedCursor, seeded, windowFull, fetched, continuationPath, nil); handled || err != nil {
		return out, err
	}
	var out ChatPollState
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		poll, changed := applyChatPollSuccessLocked(state, chatID, lastModifiedCursor, seeded, windowFull, fetched, continuationPath, now)
		out = poll
		if !changed {
			return errStoreNoChange
		}
		return nil
	})
	return out, err
}

// RecordChatPollSuccessWithContinuationAndSchedule applies a poll success and a
// derived schedule update in one state transaction. The schedule callback runs
// while the store update is locked, so it must be pure and must not call back
// into Store methods.
func (s *Store) RecordChatPollSuccessWithContinuationAndSchedule(ctx context.Context, chatID string, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int, continuationPath string, schedule func(ChatPollState) (ChatPollScheduleUpdate, error)) (ChatPollState, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, fmt.Errorf("chat id is required")
	}
	continuationPath = strings.TrimSpace(continuationPath)
	if out, handled, err := s.recordChatPollSuccessWithContinuationAndScheduleSQLite(ctx, chatID, lastModifiedCursor, seeded, windowFull, fetched, continuationPath, schedule); handled || err != nil {
		return out, err
	}
	var out ChatPollState
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		poll, changed := applyChatPollSuccessLocked(state, chatID, lastModifiedCursor, seeded, windowFull, fetched, continuationPath, now)
		if schedule != nil {
			update, err := schedule(poll)
			if err != nil {
				return err
			}
			update.ChatID = strings.TrimSpace(update.ChatID)
			switch {
			case update.ChatID == "":
				update.ChatID = chatID
			case update.ChatID != chatID:
				return fmt.Errorf("chat poll schedule chat id %q does not match success chat id %q", update.ChatID, chatID)
			}
			var scheduleChanged bool
			poll, scheduleChanged, err = applyChatPollScheduleUpdateLocked(state, update, time.Now())
			if err != nil {
				return err
			}
			changed = changed || scheduleChanged
		}
		out = poll
		if !changed {
			return errStoreNoChange
		}
		return nil
	})
	return out, err
}

func applyChatPollSuccessLocked(state *State, chatID string, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int, continuationPath string, now time.Time) (ChatPollState, bool) {
	poll := state.ChatPolls[chatID]
	if chatPollSuccessIsTimestampOnlyNoop(poll, chatID, lastModifiedCursor, seeded, windowFull, fetched, continuationPath, now) {
		return poll, false
	}
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
	return poll, true
}

func chatPollSuccessIsTimestampOnlyNoop(poll ChatPollState, chatID string, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int, continuationPath string, now time.Time) bool {
	if poll.ChatID != chatID || fetched != 0 || windowFull {
		return false
	}
	if !poll.Seeded && seeded {
		return false
	}
	if lastModifiedCursor.After(poll.LastModifiedCursor) {
		return false
	}
	if poll.LastError != "" || !poll.LastErrorAt.IsZero() || !poll.BlockedUntil.IsZero() || poll.FailureCount != 0 {
		return false
	}
	if poll.ContinuationPath != continuationPath || poll.LastWindowFullMessage != "" {
		return false
	}
	if poll.LastSuccessfulPollAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return now.Sub(poll.LastSuccessfulPollAt) < chatPollSuccessHeartbeatWriteInterval
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
	if handled, err := s.recordChatPollErrorWithBlockSQLite(ctx, chatID, message, blockedUntil); handled || err != nil {
		return err
	}
	return s.Update(ctx, func(state *State) error {
		now := time.Now()
		applyChatPollErrorWithBlockLocked(state, chatID, message, blockedUntil, now)
		return nil
	})
}

func applyChatPollErrorWithBlockLocked(state *State, chatID string, message string, blockedUntil time.Time, now time.Time) ChatPollState {
	if state.ChatPolls == nil {
		state.ChatPolls = make(map[string]ChatPollState)
	}
	poll := state.ChatPolls[chatID]
	poll.ChatID = chatID
	poll.LastError = message
	poll.LastErrorAt = now
	poll.FailureCount++
	if blockedUntil.After(now) {
		if poll.PollState != "" && poll.PollState != chatPollStateBlocked {
			poll.PreviousPollState = poll.PollState
		}
		poll.PollState = chatPollStateBlocked
		poll.BlockedUntil = blockedUntil
		poll.NextPollAt = blockedUntil
	}
	poll.UpdatedAt = now
	state.ChatPolls[chatID] = poll
	return poll
}

func (s *Store) UpdateChatPollSchedule(ctx context.Context, update ChatPollScheduleUpdate) (ChatPollState, error) {
	if out, handled, err := s.updateChatPollSchedulesSQLite(ctx, []ChatPollScheduleUpdate{update}); handled || err != nil {
		return out[strings.TrimSpace(update.ChatID)], err
	}
	var out ChatPollState
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		poll, changed, err := applyChatPollScheduleUpdateLocked(state, update, now)
		if err != nil {
			return err
		}
		if !changed {
			out = poll
			return errStoreNoChange
		}
		out = poll
		return nil
	})
	return out, err
}

func (s *Store) UpdateChatPollSchedules(ctx context.Context, updates []ChatPollScheduleUpdate) (map[string]ChatPollState, error) {
	if len(updates) == 0 {
		return map[string]ChatPollState{}, nil
	}
	if out, handled, err := s.updateChatPollSchedulesSQLite(ctx, updates); handled || err != nil {
		return out, err
	}
	out := make(map[string]ChatPollState, len(updates))
	err := s.Update(ctx, func(state *State) error {
		now := time.Now()
		nextOut := make(map[string]ChatPollState, len(updates))
		changed := false
		for _, update := range updates {
			poll, updateChanged, err := applyChatPollScheduleUpdateLocked(state, update, now)
			if err != nil {
				return err
			}
			nextOut[poll.ChatID] = poll
			changed = changed || updateChanged
		}
		out = nextOut
		if !changed {
			return errStoreNoChange
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) BoostChatPollAfterFinalAnswer(ctx context.Context, req FinalAnswerPollBoostRequest) (ChatPollState, bool, error) {
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.TeamsChatID = strings.TrimSpace(req.TeamsChatID)
	if req.SessionID == "" || req.TeamsChatID == "" {
		return ChatPollState{}, false, nil
	}
	if req.NextPollAt.IsZero() {
		req.NextPollAt = time.Now()
	}
	if out, changed, handled, err := s.boostChatPollAfterFinalAnswerSQLite(ctx, req); handled || err != nil {
		return out, changed, err
	}
	var out ChatPollState
	changed := false
	err := s.Update(ctx, func(state *State) error {
		poll, ok := state.ChatPolls[req.TeamsChatID]
		if !ok || !finalAnswerPollBoostGuardAllows(state, req, poll, req.NextPollAt) {
			out = poll
			return errStoreNoChange
		}
		next, updateChanged, err := applyChatPollScheduleUpdateLocked(state, finalAnswerPollBoostScheduleUpdate(req), req.NextPollAt)
		if err != nil {
			return err
		}
		out = next
		changed = updateChanged
		if !updateChanged {
			return errStoreNoChange
		}
		return nil
	})
	return out, changed, err
}

func (s *Store) ClearChatPollContinuationBackoffAndError(ctx context.Context, chatID string) (ChatPollState, error) {
	return s.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID:                chatID,
		ClearBlockedUntil:     true,
		ClearContinuationPath: true,
		ResetFailures:         true,
	})
}

func finalAnswerPollBoostScheduleUpdate(req FinalAnswerPollBoostRequest) ChatPollScheduleUpdate {
	return ChatPollScheduleUpdate{
		ChatID:         strings.TrimSpace(req.TeamsChatID),
		PollState:      chatPollStateHot,
		NextPollAt:     req.NextPollAt,
		LastActivityAt: req.LastActivityAt,
	}
}

func finalAnswerPollBoostGuardAllows(state *State, req FinalAnswerPollBoostRequest, poll ChatPollState, now time.Time) bool {
	session, ok := state.Sessions[strings.TrimSpace(req.SessionID)]
	if !ok || !sessionStatusIsActive(session.Status) || strings.TrimSpace(session.TeamsChatID) != strings.TrimSpace(req.TeamsChatID) {
		return false
	}
	if strings.TrimSpace(poll.ChatID) != strings.TrimSpace(req.TeamsChatID) || !poll.Seeded {
		return false
	}
	switch strings.TrimSpace(poll.PollState) {
	case chatPollStateParked, chatPollStateBlocked, chatPollStateCatchup:
		return false
	}
	if poll.BlockedUntil.After(now) {
		return false
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointIDForSession(req.SessionID)]
	return !transcriptImportCheckpointIsActive(state, checkpoint)
}

func transcriptCheckpointIDForSession(sessionID string) string {
	return "transcript:" + strings.TrimSpace(sessionID)
}

func transcriptImportCheckpointIsActive(state *State, checkpoint ImportCheckpoint) bool {
	return checkpoint.Status == importCheckpointStatusImporting && !transcriptImportCheckpointIsOrphaned(state, checkpoint)
}

func transcriptImportCheckpointIsOrphaned(state *State, checkpoint ImportCheckpoint) bool {
	if checkpoint.Status != importCheckpointStatusImporting {
		return false
	}
	if state.ServiceOwner == nil || state.ServiceOwner.StartedAt.IsZero() {
		return false
	}
	if checkpoint.UpdatedAt.IsZero() {
		return true
	}
	return state.ServiceOwner.StartedAt.After(checkpoint.UpdatedAt)
}

func applyChatPollScheduleUpdateLocked(state *State, update ChatPollScheduleUpdate, now time.Time) (ChatPollState, bool, error) {
	chatID := strings.TrimSpace(update.ChatID)
	if chatID == "" {
		return ChatPollState{}, false, fmt.Errorf("chat id is required")
	}
	poll := state.ChatPolls[chatID]
	changed := false
	if poll.ChatID != chatID {
		poll.ChatID = chatID
		changed = true
	}
	if update.PollState != "" {
		nextState := strings.TrimSpace(update.PollState)
		nextPrevious := strings.TrimSpace(update.PreviousPollState)
		if nextState == "blocked" {
			if nextPrevious == "" && poll.PollState == "blocked" && poll.PreviousPollState != "" {
				nextPrevious = poll.PreviousPollState
			}
			if nextPrevious == "" && poll.PollState != "" && poll.PollState != "blocked" {
				nextPrevious = poll.PollState
			}
		}
		if poll.PreviousPollState != nextPrevious {
			poll.PreviousPollState = nextPrevious
			changed = true
		}
		if poll.PollState != nextState {
			poll.PollState = nextState
			changed = true
		}
		if poll.PollState == "parked" && poll.ParkedAt.IsZero() {
			poll.ParkedAt = now
			changed = true
		}
		if poll.PollState != "parked" {
			if !poll.ParkedAt.IsZero() {
				poll.ParkedAt = time.Time{}
				changed = true
			}
			if !poll.ParkNoticeSentAt.IsZero() {
				poll.ParkNoticeSentAt = time.Time{}
				changed = true
			}
		}
	}
	if !poll.NextPollAt.Equal(update.NextPollAt) {
		poll.NextPollAt = update.NextPollAt
		changed = true
	}
	if update.LastActivityAt.After(poll.LastActivityAt) {
		poll.LastActivityAt = update.LastActivityAt
		changed = true
	}
	if update.ClearBlockedUntil {
		if !poll.BlockedUntil.IsZero() {
			poll.BlockedUntil = time.Time{}
			changed = true
		}
	} else if !update.BlockedUntil.IsZero() {
		if !poll.BlockedUntil.Equal(update.BlockedUntil) {
			poll.BlockedUntil = update.BlockedUntil
			changed = true
		}
	}
	if update.ClearContinuationPath && poll.ContinuationPath != "" {
		poll.ContinuationPath = ""
		changed = true
	}
	if update.ResetFailures {
		if poll.FailureCount != 0 {
			poll.FailureCount = 0
			changed = true
		}
		if poll.LastError != "" {
			poll.LastError = ""
			changed = true
		}
		if !poll.LastErrorAt.IsZero() {
			poll.LastErrorAt = time.Time{}
			changed = true
		}
	}
	if !changed {
		return poll, false, nil
	}
	poll.UpdatedAt = now
	state.ChatPolls[chatID] = poll
	return poll, true, nil
}

func (s *Store) MarkChatPollParkNoticeSent(ctx context.Context, chatID string, at time.Time) (ChatPollState, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, fmt.Errorf("chat id is required")
	}
	if at.IsZero() {
		at = time.Now()
	}
	if out, handled, err := s.markChatPollParkNoticeSentSQLite(ctx, chatID, at); handled || err != nil {
		return out, err
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
	err := s.UpdateIfChanged(ctx, func(state *State) (bool, error) {
		now := time.Now()
		changed := false
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
			changed = true
		}
		for id, msg := range state.OutboxMessages {
			if outboxDeliveryProtected(msg) {
				if OutboxBlocksUpgrade(*state, msg, now) {
					report.PreservedOutboxBlockerIDs = append(report.PreservedOutboxBlockerIDs, id)
				}
				continue
			}
			if !outboxDeliveryTransient(msg) {
				continue
			}
			switch msg.Status {
			case OutboxStatusQueued, OutboxStatusSending:
				msg.Status = OutboxStatusSkipped
				msg.LastSendError = "superseded by teams recover"
				msg.UpdatedAt = now
				state.OutboxMessages[id] = msg
				report.SupersededOutboxIDs = append(report.SupersededOutboxIDs, id)
				changed = true
			}
		}
		sort.Strings(report.InterruptedTurnIDs)
		sort.Strings(report.SupersededOutboxIDs)
		sort.Strings(report.PreservedOutboxBlockerIDs)
		return changed, nil
	})
	return report, err
}

func (s *Store) QuarantineSession(ctx context.Context, req SessionQuarantineRequest) (SessionQuarantineReport, error) {
	req.SessionID = strings.TrimSpace(req.SessionID)
	if req.SessionID == "" {
		return SessionQuarantineReport{}, fmt.Errorf("session id is required")
	}
	req.Reason = trimDiagnostic(firstStoreNonEmptyString(req.Reason, "helper self-echo circuit breaker"), 240)
	req.Source = trimDiagnostic(firstStoreNonEmptyString(req.Source, "teams_helper"), 80)
	if req.Now.IsZero() {
		req.Now = time.Now()
	}
	var report SessionQuarantineReport
	err := s.withSessionLock(ctx, req.SessionID, func() error {
		if out, handled, err := s.quarantineSessionSQLite(ctx, req); handled || err != nil {
			report = out
			return err
		}
		return s.UpdateIfChanged(ctx, func(state *State) (bool, error) {
			var err error
			report, err = applySessionQuarantine(state, req)
			return report.Changed, err
		})
	})
	return report, err
}

func applySessionQuarantine(state *State, req SessionQuarantineRequest) (SessionQuarantineReport, error) {
	var report SessionQuarantineReport
	session, ok := state.Sessions[req.SessionID]
	if !ok {
		return report, fmt.Errorf("session %q not found", req.SessionID)
	}
	if session.Status == SessionStatusQuarantined {
		report.Session = session
		return report, nil
	}
	if !sessionStatusIsActive(session.Status) {
		return report, fmt.Errorf("session %q has status %q and cannot be quarantined", req.SessionID, session.Status)
	}
	now := req.Now
	session.Status = SessionStatusQuarantined
	session.QuarantinedAt = now
	session.QuarantineReason = req.Reason
	session.QuarantineSource = req.Source
	session.QuarantineMessageIDs = boundedUniqueStrings(req.TriggerMessageIDs, 8)
	session.UpdatedAt = now
	state.Sessions[session.ID] = session
	report.Session = session
	report.Changed = true

	for id, turn := range state.Turns {
		if strings.TrimSpace(turn.SessionID) != req.SessionID || turn.Status != TurnStatusQueued && turn.Status != TurnStatusRunning {
			continue
		}
		turn.Status = TurnStatusInterrupted
		turn.InterruptedAt = now
		turn.RecoveryReason = req.Reason
		turn.UpdatedAt = now
		state.Turns[id] = turn
		report.InterruptedTurnIDs = append(report.InterruptedTurnIDs, id)
	}
	for id, inbound := range state.InboundEvents {
		if strings.TrimSpace(inbound.SessionID) != req.SessionID || inbound.Status != InboundStatusQueued && inbound.Status != InboundStatusDeferred && inbound.Status != InboundStatusPersisted {
			continue
		}
		inbound.Status = InboundStatusIgnored
		inbound.UpdatedAt = now
		state.InboundEvents[id] = inbound
		report.IgnoredInboundIDs = append(report.IgnoredInboundIDs, id)
	}
	for id, msg := range state.OutboxMessages {
		if strings.TrimSpace(msg.SessionID) != req.SessionID {
			continue
		}
		// Once a Graph request is in flight, quarantine cannot safely turn it
		// into Skipped: a late accepted callback would then lose the durable
		// Teams message ID and a restart could issue a duplicate POST.  Keep the
		// attempt fenced until its callback settles, regardless of whether the
		// caller happened to include the row in InFlightOutboxIDs.
		if msg.Status == OutboxStatusSending {
			report.PreservedOutboxIDs = append(report.PreservedOutboxIDs, id)
			continue
		}
		switch msg.Status {
		case OutboxStatusQueued:
			msg.Status = OutboxStatusSkipped
			msg.LastSendError = req.Reason
			msg.UpdatedAt = now
			state.OutboxMessages[id] = msg
			markTranscriptDeliveryForOutboxLocked(state, msg, TranscriptDeliveryStatusSkipped, now)
			updateHelperDeliveryForOutboxLocked(state, msg, HelperDeliveryStatusSkipped, now)
			updateArtifactRecordsForOutboxLocked(state, msg, now, "skipped", req.Reason, "")
			report.SkippedOutboxIDs = append(report.SkippedOutboxIDs, id)
		}
	}
	if chatID := strings.TrimSpace(session.TeamsChatID); chatID != "" {
		poll := state.ChatPolls[chatID]
		poll.ChatID = chatID
		poll.PreviousPollState = poll.PollState
		poll.PollState = chatPollStateParked
		poll.NextPollAt = time.Time{}
		poll.BlockedUntil = time.Time{}
		poll.ContinuationPath = ""
		poll.FailureCount = 0
		poll.LastError = ""
		poll.LastErrorAt = time.Time{}
		poll.ParkedAt = now
		poll.ParkNoticeSentAt = now
		poll.UpdatedAt = now
		state.ChatPolls[chatID] = poll
	}
	sort.Strings(report.InterruptedTurnIDs)
	sort.Strings(report.IgnoredInboundIDs)
	sort.Strings(report.SkippedOutboxIDs)
	sort.Strings(report.PreservedOutboxIDs)
	return report, nil
}

func (s *Store) UnquarantineSession(ctx context.Context, req SessionUnquarantineRequest) (SessionUnquarantineReport, error) {
	req.SessionID = strings.TrimSpace(req.SessionID)
	if req.SessionID == "" {
		return SessionUnquarantineReport{}, fmt.Errorf("session id is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now()
	}
	var report SessionUnquarantineReport
	err := s.withSessionLock(ctx, req.SessionID, func() error {
		if out, handled, err := s.unquarantineSessionSQLite(ctx, req); handled || err != nil {
			report = out
			return err
		}
		return s.UpdateIfChanged(ctx, func(state *State) (bool, error) {
			var err error
			report, err = applySessionUnquarantine(state, req)
			return report.Changed, err
		})
	})
	return report, err
}

func applySessionUnquarantine(state *State, req SessionUnquarantineRequest) (SessionUnquarantineReport, error) {
	var report SessionUnquarantineReport
	session, ok := state.Sessions[req.SessionID]
	if !ok {
		return report, fmt.Errorf("session %q not found", req.SessionID)
	}
	if session.Status != SessionStatusQuarantined {
		return report, fmt.Errorf("session %q has status %q; only quarantined sessions can be unquarantined", req.SessionID, session.Status)
	}
	session.Status = SessionStatusActive
	session.QuarantinedAt = time.Time{}
	session.QuarantineReason = ""
	session.QuarantineSource = ""
	session.QuarantineMessageIDs = nil
	session.UpdatedAt = req.Now
	state.Sessions[session.ID] = session
	if chatID := strings.TrimSpace(session.TeamsChatID); chatID != "" {
		poll := state.ChatPolls[chatID]
		poll.ChatID = chatID
		poll.PreviousPollState = poll.PollState
		poll.PollState = chatPollStateCold
		poll.NextPollAt = req.Now
		poll.BlockedUntil = time.Time{}
		poll.ContinuationPath = ""
		poll.FailureCount = 0
		poll.ParkedAt = time.Time{}
		poll.ParkNoticeSentAt = time.Time{}
		poll.LastError = ""
		poll.LastErrorAt = time.Time{}
		poll.UpdatedAt = req.Now
		state.ChatPolls[chatID] = poll
	}
	report.Session = session
	report.Changed = true
	return report, nil
}

func boundedUniqueStrings(values []string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	seen := make(map[string]bool)
	out := make([]string, 0, min(limit, len(values)))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
		if len(out) == limit {
			break
		}
	}
	return out
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
			if errors.Is(err, errStoreNoChange) {
				out = current
				return nil
			}
			return err
		}
		next.UpdatedAt = now
		state.Turns[turnID] = next
		out = next
		return nil
	})
	return out, err
}

func (s *Store) updateOutbox(ctx context.Context, outboxID string, fn func(*State, OutboxMessage, time.Time) (OutboxMessage, error)) (OutboxMessage, error) {
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
		next, err := fn(state, current, now)
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
	update := func(state *State) error {
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
	}
	if handled, err := s.updateSQLiteRuntimeState(ctx, update); handled || err != nil {
		return out, err
	}
	err := s.Update(ctx, update)
	return out, err
}

func (s *Store) loadUnlocked(ctx context.Context) (State, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	if loadUnlockedTestHook != nil {
		loadUnlockedTestHook()
	}
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.clearSQLitePointerCacheUnlocked()
		state := newState()
		return state, nil
	}
	if err != nil {
		return State{}, err
	}
	if pointer, ok, err := storeSQLitePointerFromData(data); err != nil {
		s.clearSQLitePointerCacheUnlocked()
		return State{}, err
	} else if ok {
		if info, statErr := os.Stat(s.path); statErr == nil {
			s.cacheSQLitePointerUnlocked(pointer, info, false)
			s.sqlitePointerFingerprint = sha256Bytes(data)
		} else {
			s.clearSQLitePointerCacheUnlocked()
		}
		return s.loadSQLiteStateUnlocked(ctx, pointer)
	}
	s.clearSQLitePointerCacheUnlocked()
	if backend, ok, err := unsupportedStateStorageBackendFromData(data); err != nil {
		return State{}, err
	} else if ok {
		return State{}, fmt.Errorf("unsupported teams store backend %q", backend)
	}
	return loadStateData(data)
}

func loadStateData(data []byte) (State, error) {
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
	normalizeLoadedState(&state)
	return state, nil
}

func (s *Store) saveUnlocked(state State) error {
	state.ensure(time.Now())
	pruneSentOutboxMessages(&state)
	pruneTranscriptLedgerRecords(&state)
	pruneTranscriptDeliveryRecords(&state)
	pruneMessageProvenanceRecords(&state)
	pruneHelperDeliveryRecords(&state)
	if pointer, ok, err := s.currentSQLitePointerUnlocked(); err != nil {
		return err
	} else if ok {
		return s.saveSQLiteStateUnlocked(pointer, state)
	}
	if backend, ok, err := s.currentUnsupportedStateStorageBackendUnlocked(); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("unsupported teams store backend %q", backend)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(s.path, data, fileMode)
}

func (s *Store) currentUnsupportedStateStorageBackendUnlocked() (string, bool, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return unsupportedStateStorageBackendFromData(data)
}

func unsupportedStateStorageBackendFromData(data []byte) (string, bool, error) {
	if len(data) > maxStatePointerSize {
		return "", false, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return "", false, nil
	}
	backendRaw, ok := raw["storage_backend"]
	if !ok {
		return "", false, nil
	}
	var backend string
	if err := json.Unmarshal(backendRaw, &backend); err != nil {
		return "", true, err
	}
	backend = strings.TrimSpace(backend)
	if backend == "" || backend == storeSQLiteBackend {
		return "", false, nil
	}
	return backend, true, nil
}

func normalizeLoadedState(state *State) {
	if state == nil {
		return
	}
	state.ensure(time.Time{})
	backfillMessageProvenance(state)
	backfillHelperDeliveries(state)
	normalizeArtifactRecords(state)
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

func pruneTranscriptDeliveryRecords(state *State) {
	if state == nil || len(state.TranscriptDeliveries) <= maxRetainedTranscriptDeliveries {
		return
	}
	type candidate struct {
		id     string
		record TranscriptDeliveryRecord
	}
	records := make([]candidate, 0, len(state.TranscriptDeliveries))
	for id, record := range state.TranscriptDeliveries {
		records = append(records, candidate{id: id, record: record})
	}
	sort.SliceStable(records, func(i, j int) bool {
		left := transcriptDeliveryRetentionTime(records[i].record)
		right := transcriptDeliveryRetentionTime(records[j].record)
		if !left.Equal(right) {
			return left.After(right)
		}
		return records[i].id > records[j].id
	})
	for _, item := range records[maxRetainedTranscriptDeliveries:] {
		delete(state.TranscriptDeliveries, item.id)
	}
}

func pruneMessageProvenanceRecords(state *State) {
	if state == nil || len(state.MessageProvenance) <= maxRetainedMessageProvenance {
		return
	}
	type candidate struct {
		id     string
		record MessageProvenanceRecord
	}
	records := make([]candidate, 0, len(state.MessageProvenance))
	for id, record := range state.MessageProvenance {
		records = append(records, candidate{id: id, record: record})
	}
	sort.SliceStable(records, func(i, j int) bool {
		left := messageProvenanceRetentionTime(records[i].record)
		right := messageProvenanceRetentionTime(records[j].record)
		if !left.Equal(right) {
			return left.After(right)
		}
		return records[i].id > records[j].id
	})
	for _, item := range records[maxRetainedMessageProvenance:] {
		delete(state.MessageProvenance, item.id)
	}
}

func pruneHelperDeliveryRecords(state *State) {
	if state == nil || len(state.HelperDeliveries) <= maxRetainedHelperDeliveries {
		return
	}
	type candidate struct {
		id     string
		record HelperDeliveryRecord
	}
	records := make([]candidate, 0, len(state.HelperDeliveries))
	for id, record := range state.HelperDeliveries {
		records = append(records, candidate{id: id, record: record})
	}
	sort.SliceStable(records, func(i, j int) bool {
		left := helperDeliveryRetentionTime(records[i].record)
		right := helperDeliveryRetentionTime(records[j].record)
		if !left.Equal(right) {
			return left.After(right)
		}
		return records[i].id > records[j].id
	})
	for _, item := range records[maxRetainedHelperDeliveries:] {
		delete(state.HelperDeliveries, item.id)
	}
}

func helperDeliveryRetentionTime(record HelperDeliveryRecord) time.Time {
	for _, value := range []time.Time{record.SentAt, record.UpdatedAt, record.CreatedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func messageProvenanceRetentionTime(record MessageProvenanceRecord) time.Time {
	for _, value := range []time.Time{record.UpdatedAt, record.CreatedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func firstStoreNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func firstStoreNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func transcriptLedgerRetentionTime(record TranscriptLedgerRecord) time.Time {
	for _, value := range []time.Time{record.UpdatedAt, record.ImportedAt, record.CreatedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func transcriptDeliveryRetentionTime(record TranscriptDeliveryRecord) time.Time {
	for _, value := range []time.Time{record.SentAt, record.UpdatedAt, record.CreatedAt} {
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
		SchemaVersion:          SchemaVersion,
		CreatedAt:              now,
		UpdatedAt:              now,
		Machines:               make(map[string]MachineRecord),
		Sessions:               make(map[string]SessionContext),
		Turns:                  make(map[string]Turn),
		InboundEvents:          make(map[string]InboundEvent),
		OutboxMessages:         make(map[string]OutboxMessage),
		MessageProvenance:      make(map[string]MessageProvenanceRecord),
		ChatPolls:              make(map[string]ChatPollState),
		Workspaces:             make(map[string]WorkspaceRecord),
		DashboardViews:         make(map[string]DashboardViewRecord),
		DashboardNumbers:       make(map[string]DashboardNumberRecord),
		TranscriptLedger:       make(map[string]TranscriptLedgerRecord),
		TranscriptDeliveries:   make(map[string]TranscriptDeliveryRecord),
		HelperDeliveries:       make(map[string]HelperDeliveryRecord),
		ImportCheckpoints:      make(map[string]ImportCheckpoint),
		HistoryWatch:           make(map[string]HistoryWatchCheckpoint),
		ChatSequences:          make(map[string]ChatSequenceState),
		ChatRateLimits:         make(map[string]ChatRateLimitState),
		ArtifactRecords:        make(map[string]ArtifactRecord),
		Notifications:          make(map[string]NotificationRecord),
		ForkOperations:         make(map[string]ForkOperation),
		ForkHistoryItems:       make(map[string]ForkHistoryItem),
		ModelProfileKeyIntakes: make(map[string]ModelProfileKeyIntake),
		SkillPushReviews:       make(map[string]SkillPushReview),
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
	if s.MessageProvenance == nil {
		s.MessageProvenance = make(map[string]MessageProvenanceRecord)
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
	if s.TranscriptDeliveries == nil {
		s.TranscriptDeliveries = make(map[string]TranscriptDeliveryRecord)
	}
	if s.HelperDeliveries == nil {
		s.HelperDeliveries = make(map[string]HelperDeliveryRecord)
	}
	if s.ImportCheckpoints == nil {
		s.ImportCheckpoints = make(map[string]ImportCheckpoint)
	}
	if s.HistoryWatch == nil {
		s.HistoryWatch = make(map[string]HistoryWatchCheckpoint)
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
	if s.ForkOperations == nil {
		s.ForkOperations = make(map[string]ForkOperation)
	}
	if s.ForkHistoryItems == nil {
		s.ForkHistoryItems = make(map[string]ForkHistoryItem)
	}
	if s.ModelProfileKeyIntakes == nil {
		s.ModelProfileKeyIntakes = make(map[string]ModelProfileKeyIntake)
	}
	if s.SkillPushReviews == nil {
		s.SkillPushReviews = make(map[string]SkillPushReview)
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
	backfillMessageProvenance(&state)
	backfillHelperDeliveries(&state)
	normalizeArtifactRecords(&state)
	return state
}

func backfillMessageProvenance(state *State) {
	if state == nil {
		return
	}
	state.ensure(time.Time{})
	checkCurrent := len(state.MessageProvenance) > 0
	inboundIDs := sortedMapKeys(state.InboundEvents)
	for _, inboundID := range inboundIDs {
		inbound := state.InboundEvents[inboundID]
		record := MessageProvenanceRecord{
			TeamsChatID:    inbound.TeamsChatID,
			TeamsMessageID: inbound.TeamsMessageID,
			Origin:         MessageOriginUserInbound,
			SessionID:      inbound.SessionID,
			TurnID:         inbound.TurnID,
			InboundID:      inbound.ID,
			Kind:           string(inbound.Status),
			RenderedHash:   inbound.TextHash,
			CreatedAt:      inbound.CreatedAt,
			UpdatedAt:      inbound.UpdatedAt,
		}
		if checkCurrent && messageProvenanceBackfillRecordCurrent(state, record) {
			continue
		}
		recordMessageProvenanceLocked(state, record, time.Time{})
	}
	outboxIDs := sortedMapKeys(state.OutboxMessages)
	for _, outboxID := range outboxIDs {
		msg := state.OutboxMessages[outboxID]
		if msg.TeamsMessageID == "" {
			continue
		}
		switch msg.Status {
		case OutboxStatusAccepted, OutboxStatusSent:
		default:
			continue
		}
		record := MessageProvenanceRecord{
			TeamsChatID:    msg.TeamsChatID,
			TeamsMessageID: msg.TeamsMessageID,
			Origin:         MessageOriginHelperOutbox,
			SessionID:      msg.SessionID,
			TurnID:         msg.TurnID,
			OutboxID:       msg.ID,
			Kind:           msg.Kind,
			RenderedHash:   msg.RenderedHash,
			CreatedAt:      msg.CreatedAt,
			UpdatedAt:      firstStoreNonZeroTime(msg.SentAt, msg.UpdatedAt, msg.CreatedAt),
		}
		if checkCurrent && messageProvenanceBackfillRecordCurrent(state, record) {
			continue
		}
		recordMessageProvenanceLocked(state, record, time.Time{})
	}
}

func messageProvenanceBackfillRecordCurrent(state *State, record MessageProvenanceRecord) bool {
	if state == nil {
		return false
	}
	chatID := strings.TrimSpace(record.TeamsChatID)
	teamsMessageID := strings.TrimSpace(record.TeamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return true
	}
	id := messageProvenanceID(chatID, teamsMessageID)
	if id == "" {
		return true
	}
	existing, ok := state.MessageProvenance[id]
	if !ok {
		return false
	}
	record.Origin = strings.TrimSpace(record.Origin)
	existing.Origin = strings.TrimSpace(existing.Origin)
	if existing.Origin == MessageOriginHelperOutbox && record.Origin == MessageOriginUserInbound {
		return strings.Contains(existing.Diagnostic, "ignored user_inbound")
	}
	if existing.Origin != record.Origin {
		return false
	}
	return messageProvenanceRecordCoversBackfill(existing, record)
}

func messageProvenanceRecordCoversBackfill(existing MessageProvenanceRecord, record MessageProvenanceRecord) bool {
	if strings.TrimSpace(existing.TeamsChatID) != strings.TrimSpace(record.TeamsChatID) ||
		strings.TrimSpace(existing.TeamsMessageID) != strings.TrimSpace(record.TeamsMessageID) {
		return false
	}
	for _, pair := range []struct {
		existing string
		record   string
	}{
		{existing.SessionID, record.SessionID},
		{existing.TurnID, record.TurnID},
		{existing.OutboxID, record.OutboxID},
		{existing.InboundID, record.InboundID},
		{existing.Kind, record.Kind},
		{existing.RenderedHash, record.RenderedHash},
	} {
		want := strings.TrimSpace(pair.record)
		if want != "" && strings.TrimSpace(pair.existing) != want {
			return false
		}
	}
	if !record.CreatedAt.IsZero() && !existing.CreatedAt.Equal(record.CreatedAt) {
		return false
	}
	if !record.UpdatedAt.IsZero() && !existing.UpdatedAt.Equal(record.UpdatedAt) {
		return false
	}
	return true
}

func backfillHelperDeliveries(state *State) {
	if state == nil {
		return
	}
	state.ensure(time.Time{})
	deliveryIDsByOutboxID := make(map[string][]string)
	for deliveryID, delivery := range state.HelperDeliveries {
		outboxID := strings.TrimSpace(delivery.OutboxID)
		deliveryIDsByOutboxID[outboxID] = append(deliveryIDsByOutboxID[outboxID], deliveryID)
	}
	outboxIDs := sortedMapKeys(state.OutboxMessages)
	for _, outboxID := range outboxIDs {
		msg := state.OutboxMessages[outboxID]
		messageOutboxID := strings.TrimSpace(msg.ID)
		createdID := updateHelperDeliveryForOutboxIDsLocked(
			state,
			msg,
			helperDeliveryStatusFromOutboxStatus(msg.Status),
			firstStoreNonZeroTime(msg.UpdatedAt, msg.CreatedAt),
			deliveryIDsByOutboxID[messageOutboxID],
		)
		if createdID != "" {
			deliveryIDsByOutboxID[messageOutboxID] = append(deliveryIDsByOutboxID[messageOutboxID], createdID)
		}
	}
	transcriptDeliveryIDs := sortedMapKeys(state.TranscriptDeliveries)
	for _, deliveryID := range transcriptDeliveryIDs {
		delivery := state.TranscriptDeliveries[deliveryID]
		if helperDeliveryKindFamily(delivery.Kind) == "" || strings.TrimSpace(delivery.TextHash) == "" {
			continue
		}
		status := helperDeliveryStatusFromTranscriptDeliveryStatus(delivery.Status)
		if status == "" {
			continue
		}
		session := state.Sessions[strings.TrimSpace(delivery.SessionID)]
		record := HelperDeliveryRecord{
			SessionID:      strings.TrimSpace(delivery.SessionID),
			TeamsChatID:    strings.TrimSpace(session.TeamsChatID),
			CodexThreadID:  firstStoreNonEmptyString(delivery.CodexThreadID, session.CodexThreadID),
			Kind:           strings.TrimSpace(delivery.Kind),
			KindFamily:     helperDeliveryKindFamily(delivery.Kind),
			SourceTextHash: strings.TrimSpace(delivery.TextHash),
			VisibleHash:    strings.TrimSpace(delivery.TextHash),
			OutboxID:       strings.TrimSpace(delivery.OutboxID),
			TeamsMessageID: strings.TrimSpace(delivery.TeamsMessageID),
			PartIndex:      1,
			PartCount:      1,
			Status:         status,
			CreatedAt:      delivery.CreatedAt,
			UpdatedAt:      firstStoreNonZeroTime(delivery.UpdatedAt, delivery.CreatedAt),
			SentAt:         delivery.SentAt,
		}
		record.ID = helperDeliveryID(record)
		if record.ID != "" {
			if existing, ok := state.HelperDeliveries[record.ID]; ok && !existing.CreatedAt.IsZero() {
				record.CreatedAt = existing.CreatedAt
			}
			state.HelperDeliveries[record.ID] = record
		}
	}
	provenanceIDs := sortedMapKeys(state.MessageProvenance)
	for _, provenanceID := range provenanceIDs {
		provenance := state.MessageProvenance[provenanceID]
		if strings.TrimSpace(provenance.Origin) != MessageOriginHelperOutbox || helperDeliveryKindFamily(provenance.Kind) == "" || strings.TrimSpace(provenance.RenderedHash) == "" {
			continue
		}
		session := state.Sessions[strings.TrimSpace(provenance.SessionID)]
		record := HelperDeliveryRecord{
			SessionID:      strings.TrimSpace(provenance.SessionID),
			TeamsChatID:    strings.TrimSpace(provenance.TeamsChatID),
			CodexThreadID:  strings.TrimSpace(session.CodexThreadID),
			TurnID:         strings.TrimSpace(provenance.TurnID),
			Kind:           strings.TrimSpace(provenance.Kind),
			KindFamily:     helperDeliveryKindFamily(provenance.Kind),
			RenderedHash:   strings.TrimSpace(provenance.RenderedHash),
			VisibleHash:    strings.TrimSpace(provenance.RenderedHash),
			OutboxID:       strings.TrimSpace(provenance.OutboxID),
			TeamsMessageID: strings.TrimSpace(provenance.TeamsMessageID),
			PartIndex:      1,
			PartCount:      1,
			Status:         HelperDeliveryStatusSent,
			CreatedAt:      provenance.CreatedAt,
			UpdatedAt:      firstStoreNonZeroTime(provenance.UpdatedAt, provenance.CreatedAt),
			SentAt:         provenance.UpdatedAt,
		}
		if record.TeamsChatID == "" {
			record.TeamsChatID = strings.TrimSpace(session.TeamsChatID)
		}
		record.ID = helperDeliveryID(record)
		if record.ID != "" {
			if existing, ok := state.HelperDeliveries[record.ID]; ok && !existing.CreatedAt.IsZero() {
				record.CreatedAt = existing.CreatedAt
			}
			state.HelperDeliveries[record.ID] = record
		}
	}
}

func sortedMapKeys[T any](values map[string]T) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func helperDeliveryStatusFromTranscriptDeliveryStatus(status TranscriptDeliveryStatus) HelperDeliveryStatus {
	switch status {
	case TranscriptDeliveryStatusAccepted:
		return HelperDeliveryStatusAccepted
	case TranscriptDeliveryStatusSent:
		return HelperDeliveryStatusSent
	case TranscriptDeliveryStatusSkipped:
		return HelperDeliveryStatusSkipped
	case TranscriptDeliveryStatusQueued:
		return HelperDeliveryStatusQueued
	default:
		return ""
	}
}

func normalizeArtifactRecords(state *State) {
	if state == nil {
		return
	}
	state.ensure(time.Time{})
	for id, record := range state.ArtifactRecords {
		record.ID = firstStoreNonEmptyString(record.ID, id)
		record.SessionID = strings.TrimSpace(record.SessionID)
		record.TurnID = strings.TrimSpace(record.TurnID)
		record.Path = strings.TrimSpace(record.Path)
		record.UploadName = strings.TrimSpace(record.UploadName)
		record.DriveItemID = strings.TrimSpace(record.DriveItemID)
		record.OutboxID = strings.TrimSpace(record.OutboxID)
		record.TeamsMessageID = strings.TrimSpace(record.TeamsMessageID)
		if strings.TrimSpace(record.Status) == "uploaded" && (record.DriveItemID == "" || record.OutboxID == "") {
			record.Status = "legacy_unknown"
			record.StatusReason = "legacy uploaded artifact record did not include outbox or drive item metadata"
		}
		state.ArtifactRecords[id] = record
	}
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

func sameOwnerProcess(a OwnerMetadata, b OwnerMetadata) bool {
	if a.PID != b.PID || a.Hostname != b.Hostname {
		return false
	}
	if a.ExecutablePath == b.ExecutablePath {
		return true
	}
	return canonicalOwnerExecutablePath(a.ExecutablePath) == canonicalOwnerExecutablePath(b.ExecutablePath)
}

func canonicalOwnerExecutablePath(path string) string {
	return helperpath.CanonicalOwnerExecutable(path, helperpath.Options{})
}

func sameOwnerInstance(a OwnerMetadata, b OwnerMetadata) bool {
	if !sameOwnerProcess(a, b) {
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

func recordMessageProvenanceLocked(state *State, record MessageProvenanceRecord, now time.Time) MessageProvenanceRecord {
	state.ensure(time.Time{})
	record.TeamsChatID = strings.TrimSpace(record.TeamsChatID)
	record.TeamsMessageID = strings.TrimSpace(record.TeamsMessageID)
	if record.TeamsChatID == "" || record.TeamsMessageID == "" {
		return MessageProvenanceRecord{}
	}
	if strings.TrimSpace(record.Origin) == "" {
		record.Origin = "unknown"
	}
	record.ID = messageProvenanceID(record.TeamsChatID, record.TeamsMessageID)
	if existing, ok := state.MessageProvenance[record.ID]; ok {
		if !existing.CreatedAt.IsZero() && record.CreatedAt.IsZero() {
			record.CreatedAt = existing.CreatedAt
		}
		switch {
		case strings.TrimSpace(existing.Origin) == MessageOriginHelperOutbox && strings.TrimSpace(record.Origin) == MessageOriginUserInbound:
			if record.UpdatedAt.IsZero() {
				if !now.IsZero() {
					record.UpdatedAt = now
				} else {
					record.UpdatedAt = existing.UpdatedAt
				}
			}
			existing.UpdatedAt = record.UpdatedAt
			existing.Diagnostic = "ignored user_inbound provenance for helper_outbox Teams message"
			state.MessageProvenance[existing.ID] = existing
			return existing
		case strings.TrimSpace(existing.Origin) == MessageOriginUserInbound && strings.TrimSpace(record.Origin) == MessageOriginHelperOutbox:
			record.Diagnostic = "replaced user_inbound provenance with helper_outbox Teams message"
			suppressInboundExecutionForHelperOutboxLocked(state, existing, now)
		}
	}
	if record.CreatedAt.IsZero() {
		if !now.IsZero() {
			record.CreatedAt = now
		} else if !record.UpdatedAt.IsZero() {
			record.CreatedAt = record.UpdatedAt
		}
	}
	if record.UpdatedAt.IsZero() {
		if !now.IsZero() {
			record.UpdatedAt = now
		} else {
			record.UpdatedAt = record.CreatedAt
		}
	}
	state.MessageProvenance[record.ID] = record
	return record
}

func helperOutboxMessageLocked(state *State, chatID string, teamsMessageID string) bool {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if state == nil || chatID == "" || teamsMessageID == "" {
		return false
	}
	return messageLookupLocked(state, chatID, teamsMessageID).HasDeliveredOutbox
}

func (s *Store) invalidateMessageLookupCacheLocked() {
	s.messageLookup = messageLookupCache{}
}

func (s *Store) replaceMessageLookupCacheFromStateLocked(state State) {
	stamp, err := stateFileStampForPath(s.path)
	if err != nil {
		s.invalidateMessageLookupCacheLocked()
		return
	}
	state.ensure(time.Time{})
	backfillMessageProvenance(&state)
	s.messageLookup = buildMessageLookupCache(state, stamp)
}

func stateFileStampForPath(path string) (stateFileStamp, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return stateFileStamp{}, nil
	}
	if err != nil {
		return stateFileStamp{}, err
	}
	revision, err := stateFileStampRevision(path, info)
	if err != nil {
		return stateFileStamp{}, err
	}
	return stateFileStamp{
		Exists:   true,
		Size:     info.Size(),
		ModTime:  info.ModTime(),
		Revision: revision,
		Info:     info,
	}, nil
}

// SourceFileIdentity returns a stable identity for the current file at path.
// It deliberately excludes size, mtime, and change time: those values are
// expected to change while an append-only transcript is being written.  The
// identity is used by transcript checkpoints to reject an atomic replacement
// that happens to retain the same bounded prefix.  Platforms without a
// stable file identity return an empty string so callers can fail closed.
func SourceFileIdentity(path string) (string, error) {
	stamp, err := stateFileStampForPath(path)
	if err != nil {
		return "", err
	}
	if !stamp.Exists || !stamp.Revision.Valid {
		return "", nil
	}
	return sourceFileIdentityFromRevision(stamp.Revision), nil
}

// SourceFileIdentityFromFileInfo is the descriptor-friendly form used by
// transcript scanners. On platforms where FileInfo carries the native inode,
// it avoids a second path lookup and therefore also avoids observing a
// different pathname after an atomic replacement. Platforms without that
// information conservatively fall back to their path-based identity probe.
func SourceFileIdentityFromFileInfo(path string, info os.FileInfo) (string, error) {
	if info == nil || info.IsDir() {
		return "", nil
	}
	revision, err := stateFileStampRevision(path, info)
	if err != nil {
		return "", err
	}
	if !revision.Valid {
		return "", nil
	}
	return sourceFileIdentityFromRevision(revision), nil
}

func sourceFileIdentityFromRevision(revision stateFileRevision) string {
	return fmt.Sprintf("file:%08x:%08x:%08x:%016x",
		revision.VolumeSerial,
		revision.FileIndexHigh,
		revision.FileIndexLow,
		uint64(revision.CreationTimeNanos))
}

func buildMessageLookupCache(state State, stamp stateFileStamp) messageLookupCache {
	cache := messageLookupCache{
		Valid:               true,
		Stamp:               stamp,
		Provenance:          make(map[string]MessageProvenanceRecord, len(state.MessageProvenance)),
		ProvenanceCanonical: make(map[string]bool, len(state.MessageProvenance)),
		Inbound:             make(map[string]bool, len(state.InboundEvents)+len(state.MessageProvenance)),
		DeliveredOutbox:     make(map[string]bool, len(state.OutboxMessages)+len(state.MessageProvenance)),
	}
	for id, record := range state.MessageProvenance {
		key := messageLookupKey(record.TeamsChatID, record.TeamsMessageID)
		if key == "" {
			continue
		}
		canonical := id == messageProvenanceID(record.TeamsChatID, record.TeamsMessageID)
		if _, ok := cache.Provenance[key]; !ok || (canonical && !cache.ProvenanceCanonical[key]) {
			cache.Provenance[key] = record
			cache.ProvenanceCanonical[key] = canonical
		}
	}
	for key, record := range cache.Provenance {
		switch strings.TrimSpace(record.Origin) {
		case MessageOriginUserInbound:
			cache.Inbound[key] = true
		case MessageOriginHelperOutbox:
			cache.DeliveredOutbox[key] = true
		}
	}
	for _, event := range state.InboundEvents {
		key := messageLookupKey(event.TeamsChatID, event.TeamsMessageID)
		if key != "" {
			cache.Inbound[key] = true
		}
	}
	for _, msg := range state.OutboxMessages {
		key := messageLookupKey(msg.TeamsChatID, msg.TeamsMessageID)
		if key == "" {
			continue
		}
		switch msg.Status {
		case OutboxStatusAccepted, OutboxStatusSent:
			cache.DeliveredOutbox[key] = true
		}
	}
	return cache
}

func (c messageLookupCache) lookup(stamp stateFileStamp, chatID string, teamsMessageID string) (MessageLookup, bool) {
	if !c.Valid || !c.Stamp.equal(stamp) {
		return MessageLookup{}, false
	}
	key := messageLookupKey(chatID, teamsMessageID)
	if key == "" {
		return MessageLookup{}, true
	}
	var out MessageLookup
	if record, ok := c.Provenance[key]; ok {
		out.Provenance = record
		out.HasProvenance = true
	}
	out.HasInbound = c.Inbound[key]
	out.HasDeliveredOutbox = c.DeliveredOutbox[key]
	return out, true
}

func (stamp stateFileStamp) equal(other stateFileStamp) bool {
	if stamp.Exists != other.Exists {
		return false
	}
	if !stamp.Exists {
		return true
	}
	if stamp.Size != other.Size || !stamp.ModTime.Equal(other.ModTime) {
		return false
	}
	if stamp.Revision.Valid && other.Revision.Valid {
		return stamp.Revision == other.Revision
	}
	if stamp.Info != nil && other.Info != nil && !os.SameFile(stamp.Info, other.Info) {
		return false
	}
	return true
}

func messageLookupLocked(state *State, chatID string, teamsMessageID string) MessageLookup {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if state == nil || chatID == "" || teamsMessageID == "" {
		return MessageLookup{}
	}
	var out MessageLookup
	if record, ok := messageProvenanceLocked(state, chatID, teamsMessageID); ok {
		out.Provenance = record
		out.HasProvenance = true
		switch strings.TrimSpace(record.Origin) {
		case MessageOriginUserInbound:
			out.HasInbound = true
		case MessageOriginHelperOutbox:
			out.HasDeliveredOutbox = true
		}
	}
	if _, ok := inboundEventByTeamsMessageLocked(state, chatID, teamsMessageID); ok {
		out.HasInbound = true
	}
	if deliveredOutboxMessageLocked(state, chatID, teamsMessageID) {
		out.HasDeliveredOutbox = true
	}
	return out
}

func messageProvenanceLocked(state *State, chatID string, teamsMessageID string) (MessageProvenanceRecord, bool) {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if state == nil || chatID == "" || teamsMessageID == "" {
		return MessageProvenanceRecord{}, false
	}
	id := messageProvenanceID(chatID, teamsMessageID)
	if record, ok := state.MessageProvenance[id]; ok {
		return record, true
	}
	for _, record := range state.MessageProvenance {
		if strings.TrimSpace(record.TeamsChatID) == chatID && strings.TrimSpace(record.TeamsMessageID) == teamsMessageID {
			return record, true
		}
	}
	return MessageProvenanceRecord{}, false
}

func inboundEventByTeamsMessageLocked(state *State, chatID string, teamsMessageID string) (InboundEvent, bool) {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if state == nil || chatID == "" || teamsMessageID == "" {
		return InboundEvent{}, false
	}
	id := inboundID(chatID, teamsMessageID)
	if event, ok := state.InboundEvents[id]; ok {
		return event, true
	}
	for _, event := range state.InboundEvents {
		if strings.TrimSpace(event.TeamsChatID) == chatID && strings.TrimSpace(event.TeamsMessageID) == teamsMessageID {
			return event, true
		}
	}
	return InboundEvent{}, false
}

func deliveredOutboxMessageLocked(state *State, chatID string, teamsMessageID string) bool {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if state == nil || chatID == "" || teamsMessageID == "" {
		return false
	}
	for _, msg := range state.OutboxMessages {
		if strings.TrimSpace(msg.TeamsChatID) != chatID || strings.TrimSpace(msg.TeamsMessageID) != teamsMessageID {
			continue
		}
		switch msg.Status {
		case OutboxStatusAccepted, OutboxStatusSent:
			return true
		}
	}
	return false
}

func suppressInboundExecutionForHelperOutboxLocked(state *State, record MessageProvenanceRecord, now time.Time) {
	if state == nil {
		return
	}
	var inboundIDs []string
	matchedInbound := false
	if id := strings.TrimSpace(record.InboundID); id != "" {
		inboundIDs = append(inboundIDs, id)
		if _, ok := state.InboundEvents[id]; ok {
			matchedInbound = true
		}
	}
	chatID := strings.TrimSpace(record.TeamsChatID)
	teamsMessageID := strings.TrimSpace(record.TeamsMessageID)
	if chatID != "" && teamsMessageID != "" {
		if id := inboundID(chatID, teamsMessageID); id != "" {
			if inbound, ok := state.InboundEvents[id]; ok &&
				strings.TrimSpace(inbound.TeamsChatID) == chatID &&
				strings.TrimSpace(inbound.TeamsMessageID) == teamsMessageID {
				inboundIDs = appendUniqueStoreString(inboundIDs, id)
				matchedInbound = true
			}
		}
		if !matchedInbound {
			for id, inbound := range state.InboundEvents {
				if strings.TrimSpace(inbound.TeamsChatID) == chatID &&
					strings.TrimSpace(inbound.TeamsMessageID) == teamsMessageID &&
					strings.TrimSpace(inbound.TeamsMessageID) != "" {
					inboundIDs = appendUniqueStoreString(inboundIDs, id)
					matchedInbound = true
				}
			}
		}
	}
	for _, id := range inboundIDs {
		inbound := state.InboundEvents[id]
		if inbound.ID == "" {
			continue
		}
		inbound.Status = InboundStatusIgnored
		if !now.IsZero() {
			inbound.UpdatedAt = now
		}
		state.InboundEvents[id] = inbound
	}
	turnUpdated := false
	if turnID := strings.TrimSpace(record.TurnID); turnID != "" {
		if turn, ok := state.Turns[turnID]; ok && turn.Status == TurnStatusQueued {
			turn.Status = TurnStatusInterrupted
			turn.RecoveryReason = "helper_outbox provenance replaced user_inbound for the same Teams message"
			if !now.IsZero() {
				turn.InterruptedAt = now
				turn.UpdatedAt = now
			}
			state.Turns[turnID] = turn
			turnUpdated = true
		}
	}
	if turnUpdated {
		return
	}
	for id, turn := range state.Turns {
		if turn.Status != TurnStatusQueued {
			continue
		}
		matchesInbound := false
		for _, inboundID := range inboundIDs {
			if strings.TrimSpace(turn.InboundEventID) == strings.TrimSpace(inboundID) {
				matchesInbound = true
				break
			}
		}
		if !matchesInbound && strings.TrimSpace(record.TurnID) != "" && strings.TrimSpace(turn.ID) == strings.TrimSpace(record.TurnID) {
			matchesInbound = true
		}
		if !matchesInbound {
			continue
		}
		turn.Status = TurnStatusInterrupted
		turn.RecoveryReason = "helper_outbox provenance replaced user_inbound for the same Teams message"
		if !now.IsZero() {
			turn.InterruptedAt = now
			turn.UpdatedAt = now
		}
		state.Turns[id] = turn
	}
}

func appendUniqueStoreString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if strings.TrimSpace(existing) == value {
			return values
		}
	}
	return append(values, value)
}

func messageProvenanceID(chatID string, teamsMessageID string) string {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(chatID + "\x00" + teamsMessageID))
	return "teams-message:" + hex.EncodeToString(sum[:16])
}

func messageLookupKey(chatID string, teamsMessageID string) string {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return ""
	}
	return chatID + "\x00" + teamsMessageID
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
	if err := durableReplaceFile(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return os.Chmod(path, perm)
}
