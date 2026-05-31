package config

import "time"

const CurrentVersion = 2

type Config struct {
	Version             int                     `json:"version"`
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
