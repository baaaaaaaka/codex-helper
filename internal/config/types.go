package config

import (
	"encoding/json"
	"time"
)

// CurrentVersion is the schema generation this binary stamps into configs it
// writes. Generation 6 adds the extensible global defaults container. The
// field is additive and keeps the reader floor unchanged, while the newer
// write generation prevents an older helper from silently dropping defaults it
// does not understand.
const CurrentVersion = 6

// MinReaderVersion is the minimum reader generation required to SAFELY read a
// config written by this binary. Raise it ONLY for breaking schema changes
// (removed/renamed/semantically-changed fields). Additive changes MUST leave it
// unchanged so older binaries can still read newer configs — encoding/json
// ignores unknown fields, so an additive change does not require a newer reader.
// See (*Store).loadUnlocked for the gate.
const MinReaderVersion = 1

// SupportedReaderVersion is the newest reader floor this binary can satisfy. A
// config whose minReader exceeds it is rejected with ErrStaleReader (this build
// is too old to read it). It moves together with MinReaderVersion when a
// breaking change lands.
const SupportedReaderVersion = MinReaderVersion

type Config struct {
	Version   int `json:"version"`
	MinReader int `json:"minReader,omitempty"`
	// RuntimeGeneration records that this installation has successfully
	// initialized the generation-1 broker runtime. RuntimeCleanupPending keeps
	// post-commit compatibility cleanup retryable without making an activated
	// installation fall back to the retired runner.
	RuntimeGeneration       int        `json:"runtimeGeneration,omitempty"`
	RuntimeMigrationID      string     `json:"runtimeMigrationId,omitempty"`
	RuntimeMigratedAt       time.Time  `json:"runtimeMigratedAt,omitempty"`
	RuntimeCleanupPending   bool       `json:"runtimeCleanupPending,omitempty"`
	ProxyEnabled            *bool      `json:"proxyEnabled,omitempty"`
	AgentAutoApproveEnabled *bool      `json:"agentAutoApproveEnabled,omitempty"`
	Profiles                []Profile  `json:"profiles"`
	Instances               []Instance `json:"instances,omitempty"`
	// The model settings of earlier releases (global defaults, model
	// profiles, catalogs and third-party providers) are no longer used. They
	// are kept verbatim so a rewrite does not drop them for an older binary
	// that shares this config.
	Defaults              json.RawMessage      `json:"defaults,omitempty"`
	DefaultModelProfile   json.RawMessage      `json:"defaultModelProfile,omitempty"`
	ModelProfiles         json.RawMessage      `json:"modelProfiles,omitempty"`
	ModelConfigVersion    json.RawMessage      `json:"modelConfigVersion,omitempty"`
	ModelCredentials      json.RawMessage      `json:"modelCredentials,omitempty"`
	ModelProviders        json.RawMessage      `json:"modelProviders,omitempty"`
	Models                json.RawMessage      `json:"models,omitempty"`
	ModelSources          json.RawMessage      `json:"modelSources,omitempty"`
	ModelCatalogs         json.RawMessage      `json:"modelCatalogs,omitempty"`
	ModelProviderBindings json.RawMessage      `json:"modelProviderBindings,omitempty"`
	TeamsCodexPath        TeamsCodexPathPolicy `json:"teamsCodexPath,omitempty"`
}

// TeamsCodexPathPolicy controls which executable search path is exposed to
// Codex processes launched by the Teams helper. Empty Mode means
// account-default.
type TeamsCodexPathPolicy struct {
	Mode          string `json:"mode,omitempty"`
	ExplicitPath  string `json:"explicitPath,omitempty"`
	ShellOverride string `json:"shellOverride,omitempty"`
}

type Profile struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	Host            string    `json:"host"`
	Port            int       `json:"port"`
	User            string    `json:"user"`
	SSHArgs         []string  `json:"sshArgs,omitempty"`
	RouteTargetHost string    `json:"routeTargetHost,omitempty"`
	RouteTargetPort int       `json:"routeTargetPort,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
}

const (
	InstanceKindDaemon       = "daemon"
	InstanceKindModelAdapter = "model-adapter"
)

type Instance struct {
	ID        string `json:"id"`
	ProfileID string `json:"profileId"`
	Kind      string `json:"kind,omitempty"`
	// BrokerID is the stable logical identity of a proxy broker. It is kept
	// separate from the process PID because a supervisor may replace the
	// process while the instance record remains the same.
	BrokerID string `json:"brokerId,omitempty"`
	// BrokerEpoch changes whenever a new daemon takes ownership of the
	// instance. It fences health/heartbeat updates from an older process.
	BrokerEpoch string `json:"brokerEpoch,omitempty"`
	// OwnerToken is an opaque lease token passed only to the daemon that won
	// the startup election. A stale daemon must not remove or heartbeat a new
	// owner record.
	OwnerToken string `json:"ownerToken,omitempty"`
	// OwnerAcquiredAt and OwnerLastSeenAt make the lease boundary explicit.
	// LastSeenAt remains the compatibility timestamp used by older callers;
	// new lifecycle code updates both values together.
	OwnerAcquiredAt     time.Time `json:"ownerAcquiredAt,omitempty"`
	OwnerLastSeenAt     time.Time `json:"ownerLastSeenAt,omitempty"`
	OwnerLeaseExpiresAt time.Time `json:"ownerLeaseExpiresAt,omitempty"`
	// RecoveryBudget is persisted across daemon and supervisor replacement.
	// In-memory retry counters alone are unsafe because a native supervisor can
	// restart the process and accidentally reset the budget.
	RecoveryBudget       ProxyRecoveryBudget `json:"recoveryBudget,omitempty"`
	HTTPPort             int                 `json:"httpPort"`
	SocksPort            int                 `json:"socksPort"`
	DaemonPID            int                 `json:"daemonPid"`
	StartedAt            time.Time           `json:"startedAt"`
	LastSeenAt           time.Time           `json:"lastSeenAt"`
	ModelProfileName     string              `json:"modelProfileName,omitempty"`
	ModelUnified         bool                `json:"modelUnified,omitempty"`
	ModelProvider        string              `json:"modelProvider,omitempty"`
	ModelPublicModel     string              `json:"modelPublicModel,omitempty"`
	ModelBaseURL         string              `json:"modelBaseUrl,omitempty"`
	ModelAPIKeyRef       string              `json:"modelApiKeyRef,omitempty"`
	ModelSSHProxy        string              `json:"modelSshProxy,omitempty"`
	ModelUpstreamProxyID string              `json:"modelUpstreamProxyId,omitempty"`
	ModelRevision        int                 `json:"modelRevision,omitempty"`
	ModelProxyKey        string              `json:"modelProxyKey,omitempty"`
	ModelProfileCaptured time.Time           `json:"modelProfileCapturedAt,omitempty"`
}

// ProxyRecoveryBudget is the durable circuit-breaker state for one proxy
// broker. The two counters distinguish tunnel-exit recovery from request
// failure recovery, while Blocked is the authoritative cross-process fence.
// All fields are additive so older readers can ignore the state safely.
type ProxyRecoveryBudget struct {
	RestartWindowStartedAt time.Time `json:"restartWindowStartedAt,omitempty"`
	RestartAttempts        int       `json:"restartAttempts,omitempty"`
	RequestWindowStartedAt time.Time `json:"requestWindowStartedAt,omitempty"`
	RequestAttempts        int       `json:"requestAttempts,omitempty"`
	Blocked                bool      `json:"blocked,omitempty"`
	BlockedAt              time.Time `json:"blockedAt,omitempty"`
	LastReason             string    `json:"lastReason,omitempty"`
}
