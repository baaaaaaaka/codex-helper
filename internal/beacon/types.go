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
	Name              string       `json:"name"`
	Provider          Provider     `json:"provider"`
	ProxyMode         ProxyMode    `json:"proxy_mode"`
	ProxyProfile      string       `json:"proxy_profile,omitempty"`
	IsolationDefault  Isolation    `json:"isolation_default,omitempty"`
	Slurm             SlurmProfile `json:"slurm,omitempty"`
	LSF               LSFProfile   `json:"lsf,omitempty"`
	Confirmed         bool         `json:"confirmed"`
	ProviderPreviewOK bool         `json:"provider_preview_ok"`
	DoctorOK          bool         `json:"doctor_ok"`
	CreatedAt         time.Time    `json:"created_at,omitempty"`
	UpdatedAt         time.Time    `json:"updated_at,omitempty"`
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

type TargetSnapshot struct {
	Target        string    `json:"target"`
	Profile       string    `json:"profile,omitempty"`
	Signature     string    `json:"signature,omitempty"`
	ProxyRoute    string    `json:"proxy_route,omitempty"`
	Isolation     Isolation `json:"isolation,omitempty"`
	ProviderJobID string    `json:"provider_job_id,omitempty"`
	LeaseID       string    `json:"lease_id,omitempty"`
	MachineID     string    `json:"machine_id,omitempty"`
}

type QueuedTurn struct {
	ID       string         `json:"id"`
	Snapshot TargetSnapshot `json:"snapshot"`
}

type Machine struct {
	ID            string    `json:"id"`
	LeaseID       string    `json:"lease_id"`
	ProviderJobID string    `json:"provider_job_id,omitempty"`
	Profile       string    `json:"profile,omitempty"`
	Host          string    `json:"host,omitempty"`
	Isolation     Isolation `json:"isolation,omitempty"`
	State         string    `json:"state,omitempty"`
	ExternalOwned bool      `json:"external_owned,omitempty"`
	Chats         []string  `json:"chats,omitempty"`
	Jobs          []string  `json:"jobs,omitempty"`
}

type IdempotencyRecord struct {
	Key       string    `json:"key"`
	Result    string    `json:"result"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

type TerminalRecord struct {
	JobID        string    `json:"job_id"`
	EnvelopeHash string    `json:"envelope_hash"`
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
	Version       int                          `json:"version"`
	Profiles      map[string]Profile           `json:"profiles,omitempty"`
	Conversations map[string]Conversation      `json:"conversations,omitempty"`
	Machines      map[string]Machine           `json:"machines,omitempty"`
	Idempotency   map[string]IdempotencyRecord `json:"idempotency,omitempty"`
	Terminals     map[string]TerminalRecord    `json:"terminals,omitempty"`
	Audit         []AuditRecord                `json:"audit,omitempty"`
	AuditHead     AuditHead                    `json:"audit_head,omitempty"`
}

func (s *State) normalize() {
	if s.Version == 0 {
		s.Version = StateVersion
	}
	if s.Profiles == nil {
		s.Profiles = map[string]Profile{}
	}
	if s.Conversations == nil {
		s.Conversations = map[string]Conversation{}
	}
	if s.Machines == nil {
		s.Machines = map[string]Machine{}
	}
	if s.Idempotency == nil {
		s.Idempotency = map[string]IdempotencyRecord{}
	}
	if s.Terminals == nil {
		s.Terminals = map[string]TerminalRecord{}
	}
}
