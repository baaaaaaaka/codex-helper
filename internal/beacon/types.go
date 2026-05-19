package beacon

import "time"

const StateVersion = 1

type Provider string

const (
	ProviderLocal Provider = "local"
	ProviderSlurm Provider = "slurm"
	ProviderLSF   Provider = "lsf"
)

type ProxyMode string

const (
	ProxyNone       ProxyMode = "none"
	ProxySSHProfile ProxyMode = "ssh_profile"
)

type Isolation string

const (
	IsolationShared    Isolation = "shared"
	IsolationExclusive Isolation = "exclusive"
)

type Profile struct {
	Name              string                `json:"name"`
	Revision          int                   `json:"revision,omitempty"`
	Provider          Provider              `json:"provider"`
	ProxyMode         ProxyMode             `json:"proxy_mode"`
	ProxyProfile      string                `json:"proxy_profile,omitempty"`
	IsolationDefault  Isolation             `json:"isolation_default,omitempty"`
	Slurm             SlurmProfile          `json:"slurm,omitempty"`
	LSF               LSFProfile            `json:"lsf,omitempty"`
	Adapter           ProviderCommandConfig `json:"adapter,omitempty"`
	Confirmed         bool                  `json:"confirmed"`
	ProviderPreviewOK bool                  `json:"provider_preview_ok"`
	DoctorOK          bool                  `json:"doctor_ok"`
	CreatedAt         time.Time             `json:"created_at,omitempty"`
	UpdatedAt         time.Time             `json:"updated_at,omitempty"`
}

type SlurmProfile struct {
	Nodes     int    `json:"nodes,omitempty"`
	GPUCount  int    `json:"gpu_count,omitempty"`
	Partition string `json:"partition,omitempty"`
	Image     string `json:"image,omitempty"`
	Duration  int    `json:"duration,omitempty"`
}

type LSFProfile struct {
	QueueName                  string `json:"queue_name,omitempty"`
	SitePolicyDerivesResources bool   `json:"site_policy_derives_resources,omitempty"`
	AdvancedApproved           bool   `json:"advanced_approved,omitempty"`
}

type Conversation struct {
	ID        string          `json:"id"`
	Current   TargetSnapshot  `json:"current"`
	Pending   *TargetSnapshot `json:"pending,omitempty"`
	Queued    []QueuedTurn    `json:"queued,omitempty"`
	UpdatedAt time.Time       `json:"updated_at,omitempty"`
}

type ExecutionSignature struct {
	Hash                string `json:"hash,omitempty"`
	CodexPath           string `json:"codex_path,omitempty"`
	InstallOrigin       string `json:"install_origin,omitempty"`
	InstallTargetID     string `json:"install_target_id,omitempty"`
	ImageDigest         string `json:"image_digest,omitempty"`
	ImageMutable        bool   `json:"image_mutable,omitempty"`
	HelperProtocolRead  int    `json:"helper_protocol_read,omitempty"`
	HelperProtocolWrite int    `json:"helper_protocol_write,omitempty"`
	WorkerProtocolRead  int    `json:"worker_protocol_read,omitempty"`
	WorkerProtocolWrite int    `json:"worker_protocol_write,omitempty"`
}

type TargetSnapshot struct {
	Target           string              `json:"target"`
	Profile          string              `json:"profile,omitempty"`
	ProfileRevision  int                 `json:"profile_revision,omitempty"`
	Signature        string              `json:"signature,omitempty"`
	SignatureDetails *ExecutionSignature `json:"signature_details,omitempty"`
	ProxyRoute       string              `json:"proxy_route,omitempty"`
	Isolation        Isolation           `json:"isolation,omitempty"`
	ProviderJobID    string              `json:"provider_job_id,omitempty"`
	LeaseID          string              `json:"lease_id,omitempty"`
	MachineID        string              `json:"machine_id,omitempty"`
}

type QueuedTurn struct {
	ID       string         `json:"id"`
	Snapshot TargetSnapshot `json:"snapshot"`
}

type AllocationState string

const (
	AllocationRequestPersisted AllocationState = "request_persisted"
	AllocationSubmitted        AllocationState = "submitted"
	AllocationPending          AllocationState = "pending"
	AllocationRunning          AllocationState = "running"
	AllocationFailed           AllocationState = "failed"
	AllocationCanceled         AllocationState = "canceled"
	AllocationExpired          AllocationState = "expired"
	AllocationNeedsAttention   AllocationState = "needs_attention"
)

type ProviderIdentity struct {
	ProviderJobID   string `json:"provider_job_id,omitempty"`
	AllocationID    string `json:"allocation_id,omitempty"`
	StepID          string `json:"step_id,omitempty"`
	RunIncarnation  string `json:"run_incarnation,omitempty"`
	Host            string `json:"host,omitempty"`
	MembershipProof string `json:"membership_proof,omitempty"`
}

type AllocationRequest struct {
	ID                string             `json:"id"`
	ConversationID    string             `json:"conversation_id"`
	TurnID            string             `json:"turn_id"`
	Profile           string             `json:"profile"`
	ProfileSnapshot   Profile            `json:"profile_snapshot,omitempty"`
	Provider          Provider           `json:"provider"`
	Isolation         Isolation          `json:"isolation,omitempty"`
	Target            TargetSnapshot     `json:"target"`
	Execution         ExecutionSignature `json:"execution,omitempty"`
	DeterministicName string             `json:"deterministic_name"`
	ProviderIdentity  ProviderIdentity   `json:"provider_identity,omitempty"`
	State             AllocationState    `json:"state"`
	RawProviderState  string             `json:"raw_provider_state,omitempty"`
	ProviderReason    string             `json:"provider_reason,omitempty"`
	ProviderDeadline  time.Time          `json:"provider_deadline,omitempty"`
	RenewEpoch        int                `json:"renew_epoch,omitempty"`
	RenewStartedAt    time.Time          `json:"renew_started_at,omitempty"`
	RenewCompletedAt  time.Time          `json:"renew_completed_at,omitempty"`
	RenewError        string             `json:"renew_error,omitempty"`
	ReplacementID     string             `json:"replacement_id,omitempty"`
	ReplacementEpoch  int                `json:"replacement_epoch,omitempty"`
	CancelRequestedAt time.Time          `json:"cancel_requested_at,omitempty"`
	CancelReason      string             `json:"cancel_reason,omitempty"`
	SubmitAttempts    int                `json:"submit_attempts,omitempty"`
	CreatedAt         time.Time          `json:"created_at,omitempty"`
	UpdatedAt         time.Time          `json:"updated_at,omitempty"`
}

type LeaseState string

const (
	LeaseStarting       LeaseState = "starting"
	LeaseAccepting      LeaseState = "accepting"
	LeaseDraining       LeaseState = "draining"
	LeaseDrained        LeaseState = "drained"
	LeaseExpired        LeaseState = "expired"
	LeaseLost           LeaseState = "lost"
	LeaseIncompatible   LeaseState = "incompatible"
	LeaseAmbiguous      LeaseState = "ambiguous"
	LeaseFinalizing     LeaseState = "finalizing"
	LeaseNeedsAttention LeaseState = "needs_attention"
)

type Machine struct {
	ID              string             `json:"id"`
	LeaseID         string             `json:"lease_id"`
	ProviderJobID   string             `json:"provider_job_id,omitempty"`
	WorkerID        string             `json:"worker_id,omitempty"`
	Profile         string             `json:"profile,omitempty"`
	Host            string             `json:"host,omitempty"`
	Isolation       Isolation          `json:"isolation,omitempty"`
	State           string             `json:"state,omitempty"`
	ExternalOwned   bool               `json:"external_owned,omitempty"`
	Doctor          WorkerDoctor       `json:"doctor,omitempty"`
	DoctorBlockers  []string           `json:"doctor_blockers,omitempty"`
	MembershipProof string             `json:"membership_proof,omitempty"`
	ProviderState   ProviderJobState   `json:"provider_state,omitempty"`
	Execution       ExecutionSignature `json:"execution,omitempty"`
	LeaseExpiresAt  time.Time          `json:"lease_expires_at,omitempty"`
	LastHeartbeat   time.Time          `json:"last_heartbeat,omitempty"`
	StartedAt       time.Time          `json:"started_at,omitempty"`
	UpdatedAt       time.Time          `json:"updated_at,omitempty"`
	Chats           []string           `json:"chats,omitempty"`
	Jobs            []string           `json:"jobs,omitempty"`
}

type JobPhase string

const (
	JobQueued      JobPhase = "queued"
	JobClaimed     JobPhase = "claimed"
	JobStartIntent JobPhase = "start_intent"
	JobStarted     JobPhase = "started"
	JobTerminal    JobPhase = "terminal"
	JobAmbiguous   JobPhase = "ambiguous"
	JobQuarantined JobPhase = "quarantined"
	JobTombstoned  JobPhase = "tombstoned"
)

type JobAttempt struct {
	ID               string             `json:"id"`
	RequestID        string             `json:"request_id"`
	TurnID           string             `json:"turn_id"`
	Attempt          int                `json:"attempt"`
	Payload          JobPayload         `json:"payload,omitempty"`
	WorkerID         string             `json:"worker_id,omitempty"`
	LeaseID          string             `json:"lease_id,omitempty"`
	LeaseEpoch       int                `json:"lease_epoch,omitempty"`
	ClaimEpoch       int                `json:"claim_epoch,omitempty"`
	Phase            JobPhase           `json:"phase"`
	Target           TargetSnapshot     `json:"target"`
	ProviderIdentity ProviderIdentity   `json:"provider_identity,omitempty"`
	Execution        ExecutionSignature `json:"execution,omitempty"`
	Reason           string             `json:"reason,omitempty"`
	StartedAt        time.Time          `json:"started_at,omitempty"`
	UpdatedAt        time.Time          `json:"updated_at,omitempty"`
}

type JobPayload struct {
	Prompt        string   `json:"prompt,omitempty"`
	ImagePaths    []string `json:"image_paths,omitempty"`
	WorkingDir    string   `json:"working_dir,omitempty"`
	CodexThreadID string   `json:"codex_thread_id,omitempty"`
}

type JobTerminalPayload struct {
	Text             string `json:"text,omitempty"`
	CodexThreadID    string `json:"codex_thread_id,omitempty"`
	CodexThreadTitle string `json:"codex_thread_title,omitempty"`
	CodexTurnID      string `json:"codex_turn_id,omitempty"`
	Error            string `json:"error,omitempty"`
}

type IdempotencyRecord struct {
	Key       string    `json:"key"`
	Result    string    `json:"result"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

type TerminalRecord struct {
	JobID        string    `json:"job_id"`
	EnvelopeHash string    `json:"envelope_hash"`
	Payload      string    `json:"payload,omitempty"`
	OutboxQueued bool      `json:"outbox_queued"`
	AcceptedAt   time.Time `json:"accepted_at,omitempty"`
}

type AuditRecord struct {
	Seq      int       `json:"seq"`
	PrevHash string    `json:"prev_hash,omitempty"`
	Action   string    `json:"action"`
	Target   string    `json:"target"`
	Secret   string    `json:"secret,omitempty"`
	Hash     string    `json:"hash"`
	At       time.Time `json:"at,omitempty"`
}

type AuditHead struct {
	Seq  int    `json:"seq"`
	Hash string `json:"hash,omitempty"`
}

type State struct {
	Version        int                          `json:"version"`
	Profiles       map[string]Profile           `json:"profiles,omitempty"`
	ProfileHistory map[string]Profile           `json:"profile_history,omitempty"`
	Conversations  map[string]Conversation      `json:"conversations,omitempty"`
	TurnTargets    map[string]TargetSnapshot    `json:"turn_targets,omitempty"`
	Allocations    map[string]AllocationRequest `json:"allocations,omitempty"`
	Machines       map[string]Machine           `json:"machines,omitempty"`
	JobAttempts    map[string]JobAttempt        `json:"job_attempts,omitempty"`
	Idempotency    map[string]IdempotencyRecord `json:"idempotency,omitempty"`
	Terminals      map[string]TerminalRecord    `json:"terminals,omitempty"`
	Audit          []AuditRecord                `json:"audit,omitempty"`
	AuditHead      AuditHead                    `json:"audit_head,omitempty"`
}

func (s *State) normalize() {
	if s.Version == 0 {
		s.Version = StateVersion
	}
	if s.Profiles == nil {
		s.Profiles = map[string]Profile{}
	}
	if s.ProfileHistory == nil {
		s.ProfileHistory = map[string]Profile{}
	}
	if s.Conversations == nil {
		s.Conversations = map[string]Conversation{}
	}
	if s.TurnTargets == nil {
		s.TurnTargets = map[string]TargetSnapshot{}
	}
	if s.Allocations == nil {
		s.Allocations = map[string]AllocationRequest{}
	}
	if s.Machines == nil {
		s.Machines = map[string]Machine{}
	}
	if s.JobAttempts == nil {
		s.JobAttempts = map[string]JobAttempt{}
	}
	if s.Idempotency == nil {
		s.Idempotency = map[string]IdempotencyRecord{}
	}
	if s.Terminals == nil {
		s.Terminals = map[string]TerminalRecord{}
	}
}
