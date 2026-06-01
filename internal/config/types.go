package config

import "time"

// CurrentVersion is the schema generation this binary stamps into configs it
// writes. It stays at 2 for the first minReader-aware release so v0.1.6 can
// still read config files after rollback; future additive schema changes can
// bump this while keeping MinReaderVersion stable.
const CurrentVersion = 2

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
const SupportedReaderVersion = 1

type Config struct {
	Version             int                     `json:"version"`
	MinReader           int                     `json:"minReader,omitempty"`
	ProxyEnabled        *bool                   `json:"proxyEnabled,omitempty"`
	YoloEnabled         *bool                   `json:"yoloEnabled,omitempty"`
	Profiles            []Profile               `json:"profiles"`
	Instances           []Instance              `json:"instances,omitempty"`
	DefaultModelProfile string                  `json:"defaultModelProfile,omitempty"`
	ModelProfiles       map[string]ModelProfile `json:"modelProfiles,omitempty"`
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
	ModelAPIKeyRef       string    `json:"modelApiKeyRef,omitempty"`
	ModelSSHProxy        string    `json:"modelSshProxy,omitempty"`
	ModelRevision        int       `json:"modelRevision,omitempty"`
	ModelProxyKey        string    `json:"modelProxyKey,omitempty"`
	ModelProfileCaptured time.Time `json:"modelProfileCapturedAt,omitempty"`
}
