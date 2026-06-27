package config

import "time"

// CurrentVersion is the schema generation this binary stamps into configs it
// writes. Generation 3 stops writing the legacy execution-mode toggle and adds
// broker migration metadata. Both changes are reader-compatible: older readers
// ignore the metadata and already treat an absent toggle as disabled.
const CurrentVersion = 3

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
	RuntimeGeneration     int                     `json:"runtimeGeneration,omitempty"`
	RuntimeMigrationID    string                  `json:"runtimeMigrationId,omitempty"`
	RuntimeMigratedAt     time.Time               `json:"runtimeMigratedAt,omitempty"`
	RuntimeCleanupPending bool                    `json:"runtimeCleanupPending,omitempty"`
	ProxyEnabled          *bool                   `json:"proxyEnabled,omitempty"`
	Profiles              []Profile               `json:"profiles"`
	Instances             []Instance              `json:"instances,omitempty"`
	DefaultModelProfile   string                  `json:"defaultModelProfile,omitempty"`
	ModelProfiles         map[string]ModelProfile `json:"modelProfiles,omitempty"`
}

type Profile struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	User      string    `json:"user"`
	SSHArgs   []string  `json:"sshArgs,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type ModelProfile struct {
	Provider  string    `json:"provider"`
	Model     string    `json:"model,omitempty"`
	APIKeyRef string    `json:"apiKeyRef,omitempty"`
	SSHProxy  string    `json:"sshProxy,omitempty"`
	Revision  int       `json:"revision"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

const (
	InstanceKindDaemon       = "daemon"
	InstanceKindModelAdapter = "model-adapter"
)

type Instance struct {
	ID                   string    `json:"id"`
	ProfileID            string    `json:"profileId"`
	Kind                 string    `json:"kind,omitempty"`
	HTTPPort             int       `json:"httpPort"`
	SocksPort            int       `json:"socksPort"`
	DaemonPID            int       `json:"daemonPid"`
	StartedAt            time.Time `json:"startedAt"`
	LastSeenAt           time.Time `json:"lastSeenAt"`
	ModelProfileName     string    `json:"modelProfileName,omitempty"`
	ModelProvider        string    `json:"modelProvider,omitempty"`
	ModelPublicModel     string    `json:"modelPublicModel,omitempty"`
	ModelAPIKeyRef       string    `json:"modelApiKeyRef,omitempty"`
	ModelSSHProxy        string    `json:"modelSshProxy,omitempty"`
	ModelUpstreamProxyID string    `json:"modelUpstreamProxyId,omitempty"`
	ModelRevision        int       `json:"modelRevision,omitempty"`
	ModelProxyKey        string    `json:"modelProxyKey,omitempty"`
	ModelProfileCaptured time.Time `json:"modelProfileCapturedAt,omitempty"`
}
