package config

import "time"

// CurrentVersion is the schema generation this binary stamps into configs it
// writes. Generation 5 adds the Teams Codex user-PATH policy. The field is
// additive and keeps the reader floor unchanged, while the newer write
// generation prevents an older helper from silently dropping the policy.
const CurrentVersion = 5

const teamsCodexPathIntroducedVersion = 5

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
	RuntimeGeneration       int                        `json:"runtimeGeneration,omitempty"`
	RuntimeMigrationID      string                     `json:"runtimeMigrationId,omitempty"`
	RuntimeMigratedAt       time.Time                  `json:"runtimeMigratedAt,omitempty"`
	RuntimeCleanupPending   bool                       `json:"runtimeCleanupPending,omitempty"`
	ProxyEnabled            *bool                      `json:"proxyEnabled,omitempty"`
	AgentAutoApproveEnabled *bool                      `json:"agentAutoApproveEnabled,omitempty"`
	Profiles                []Profile                  `json:"profiles"`
	Instances               []Instance                 `json:"instances,omitempty"`
	DefaultModelProfile     string                     `json:"defaultModelProfile,omitempty"`
	ModelProfiles           map[string]ModelProfile    `json:"modelProfiles,omitempty"`
	ModelConfigVersion      int                        `json:"modelConfigVersion,omitempty"`
	ModelCredentials        map[string]ModelCredential `json:"modelCredentials,omitempty"`
	ModelProviders          map[string]ModelProvider   `json:"modelProviders,omitempty"`
	Models                  map[string]ModelDefinition `json:"models,omitempty"`
	ModelSources            map[string]ModelSource     `json:"modelSources,omitempty"`
	TeamsCodexPath          TeamsCodexPathPolicy       `json:"teamsCodexPath,omitempty"`
}

const CurrentModelConfigVersion = 1

type ModelSource struct {
	URL                     string    `json:"url"`
	Ref                     string    `json:"ref,omitempty"`
	File                    string    `json:"file,omitempty"`
	Revision                string    `json:"revision,omitempty"`
	SyncedAt                time.Time `json:"syncedAt,omitempty"`
	BackupActive            bool      `json:"backupActive,omitempty"`
	BackupSince             time.Time `json:"backupSince,omitempty"`
	BackupFailedAt          time.Time `json:"backupFailedAt,omitempty"`
	BackupAttemptedRevision string    `json:"backupAttemptedRevision,omitempty"`
	BackupReason            string    `json:"backupReason,omitempty"`
	Credentials             []string  `json:"credentials,omitempty"`
	Providers               []string  `json:"providers,omitempty"`
	Models                  []string  `json:"models,omitempty"`
	Profiles                []string  `json:"profiles,omitempty"`
}

// ModelCredential owns reusable authentication independently from providers,
// models and launch profiles. Values are always references; raw secrets never
// belong in the main config file.
type ModelCredential struct {
	APIKeyRef string            `json:"apiKeyRef"`
	Pending   bool              `json:"pending,omitempty"`
	AuthType  string            `json:"authType,omitempty"`
	Header    string            `json:"header,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

type ModelHTTPPolicy struct {
	// TimeoutSeconds is the legacy whole-request timeout. New configurations
	// should prefer the phase-specific header and stream idle timeouts below.
	TimeoutSeconds               int   `json:"timeoutSeconds,omitempty"`
	ResponseHeaderTimeoutSeconds int   `json:"responseHeaderTimeoutSeconds,omitempty"`
	MaxRetries                   *int  `json:"maxRetries,omitempty"`
	RetryStatuses                []int `json:"retryStatuses,omitempty"`
	HonorRetryAfter              *bool `json:"honorRetryAfter,omitempty"`
	RetryTransportErrors         *bool `json:"retryTransportErrors,omitempty"`
	// MaxConcurrentRequests limits in-flight requests for deployments that
	// degrade or deadlock under client-side concurrency. Zero is unlimited.
	MaxConcurrentRequests int `json:"maxConcurrentRequests,omitempty"`
}

type ModelStreamPolicy struct {
	UpstreamMode           string `json:"upstreamMode,omitempty"`
	Format                 string `json:"format,omitempty"`
	IdleTimeoutSeconds     int    `json:"idleTimeoutSeconds,omitempty"`
	SynthesizeResponsesSSE *bool  `json:"synthesizeResponsesSSE,omitempty"`
	ReasoningDeltaPath     string `json:"reasoningDeltaPath,omitempty"`
	CachedTokensPath       string `json:"cachedTokensPath,omitempty"`
}

type ModelProvider struct {
	Protocol   string            `json:"protocol"`
	BaseURL    string            `json:"baseUrl"`
	Credential string            `json:"credential,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Endpoints  map[string]string `json:"endpoints,omitempty"`
	HTTP       ModelHTTPPolicy   `json:"http,omitempty"`
	Stream     ModelStreamPolicy `json:"stream,omitempty"`
	SSHProxy   string            `json:"sshProxy,omitempty"`
}

// OptionalBool is a tri-state value: nil inherits, true enables and false
// disables. JSON null and an omitted field both inherit in schema version 1.
type ModelCapabilities struct {
	Tools            *bool `json:"tools,omitempty"`
	ParallelTools    *bool `json:"parallelTools,omitempty"`
	Vision           *bool `json:"vision,omitempty"`
	Reasoning        *bool `json:"reasoning,omitempty"`
	ReasoningSummary *bool `json:"reasoningSummary,omitempty"`
	NativeWebSearch  *bool `json:"nativeWebSearch,omitempty"`
}

type ModelLimits struct {
	ContextWindow           int    `json:"contextWindow,omitempty"`
	MaxContextWindow        int    `json:"maxContextWindow,omitempty"`
	MaxOutputTokens         int    `json:"maxOutputTokens,omitempty"`
	EffectiveContextPercent int    `json:"effectiveContextPercent,omitempty"`
	Source                  string `json:"source,omitempty"`
	Enforcement             string `json:"enforcement,omitempty"`
}

type ModelReasoningPolicy struct {
	SupportedEfforts         []string          `json:"supportedEfforts,omitempty"`
	DefaultEffort            string            `json:"defaultEffort,omitempty"`
	EffortMap                map[string]string `json:"effortMap,omitempty"`
	ThinkingMode             string            `json:"thinkingMode,omitempty"`
	EnabledRequest           map[string]any    `json:"enabledRequest,omitempty"`
	DisabledRequest          map[string]any    `json:"disabledRequest,omitempty"`
	StripSamplingWhenEnabled *bool             `json:"stripSamplingWhenEnabled,omitempty"`
	HistoryPolicy            string            `json:"historyPolicy,omitempty"`
	ResponseField            string            `json:"responseField,omitempty"`
}

type ModelToolPolicy struct {
	ToolChoice            string `json:"toolChoice,omitempty"`
	Parallel              string `json:"parallel,omitempty"`
	StrictSchema          string `json:"strictSchema,omitempty"`
	EmptyAssistantContent string `json:"emptyAssistantContent,omitempty"`
	InvalidArguments      string `json:"invalidArguments,omitempty"`
	PlainTextToolCall     string `json:"plainTextToolCall,omitempty"`
	ToolCallIDMaxLength   int    `json:"toolCallIdMaxLength,omitempty"`
	ToolNameMaxLength     int    `json:"toolNameMaxLength,omitempty"`
	ValidateArguments     *bool  `json:"validateArguments,omitempty"`
	CustomToolMode        string `json:"customToolMode,omitempty"`
}

type ModelMessagePolicy struct {
	SystemRole          string `json:"systemRole,omitempty"`
	DeveloperRole       string `json:"developerRole,omitempty"`
	MergeSystemMessages *bool  `json:"mergeSystemMessages,omitempty"`
	Images              string `json:"images,omitempty"`
}

type ModelSamplingPolicy struct {
	Temperature string `json:"temperature,omitempty"`
	TopP        string `json:"topP,omitempty"`
}

type ModelCachePolicy struct {
	PromptCacheKey     string `json:"promptCacheKey,omitempty"`
	PreviousResponseID string `json:"previousResponseId,omitempty"`
	CacheControl       string `json:"cacheControl,omitempty"`
	UsageField         string `json:"usageField,omitempty"`
}

type ModelSearchPolicy struct {
	Native   *bool               `json:"native,omitempty"`
	Fallback ModelSearchFallback `json:"fallback,omitempty"`
}

type ModelSearchFallback struct {
	Enabled *bool  `json:"enabled,omitempty"`
	Model   string `json:"model,omitempty"`
	Effort  string `json:"effort,omitempty"`
}

type ModelDefinition struct {
	Provider      string               `json:"provider"`
	UpstreamModel string               `json:"upstreamModel"`
	DisplayName   string               `json:"displayName,omitempty"`
	Aliases       []string             `json:"aliases,omitempty"`
	Description   string               `json:"description,omitempty"`
	Priority      int                  `json:"priority,omitempty"`
	Capabilities  ModelCapabilities    `json:"capabilities,omitempty"`
	Limits        ModelLimits          `json:"limits,omitempty"`
	Reasoning     ModelReasoningPolicy `json:"reasoning,omitempty"`
	Tools         ModelToolPolicy      `json:"tools,omitempty"`
	Messages      ModelMessagePolicy   `json:"messages,omitempty"`
	Sampling      ModelSamplingPolicy  `json:"sampling,omitempty"`
	Stream        ModelStreamPolicy    `json:"stream,omitempty"`
	HTTP          ModelHTTPPolicy      `json:"http,omitempty"`
	Cache         ModelCachePolicy     `json:"cache,omitempty"`
	Search        ModelSearchPolicy    `json:"search,omitempty"`
}

// TeamsCodexPathPolicy controls which executable search path is exposed to
// Codex processes launched by the Teams helper. Empty Mode means
// account-default for generation-5 configs; older configs are migrated to an
// explicit service mode so upgrades preserve their previous behavior.
type TeamsCodexPathPolicy struct {
	Mode          string `json:"mode,omitempty"`
	ExplicitPath  string `json:"explicitPath,omitempty"`
	ShellOverride string `json:"shellOverride,omitempty"`
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
	Provider                  string            `json:"provider"`
	Model                     string            `json:"model,omitempty"`
	Credential                string            `json:"credential,omitempty"`
	BaseURL                   string            `json:"baseUrl,omitempty"`
	APIKeyRef                 string            `json:"apiKeyRef,omitempty"`
	SSHProxy                  string            `json:"sshProxy,omitempty"`
	DefaultReasoningEffort    string            `json:"defaultReasoningEffort,omitempty"`
	SupportedReasoningEfforts []string          `json:"supportedReasoningEfforts,omitempty"`
	ReasoningEffortMap        map[string]string `json:"reasoningEffortMap,omitempty"`
	Revision                  int               `json:"revision"`
	CreatedAt                 time.Time         `json:"createdAt"`
	UpdatedAt                 time.Time         `json:"updatedAt"`
	Source                    string            `json:"source,omitempty"`
	VerifiedAt                time.Time         `json:"verifiedAt,omitempty"`
	VerificationFingerprint   string            `json:"verificationFingerprint,omitempty"`
	VerificationError         string            `json:"verificationError,omitempty"`
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
	ModelUnified         bool      `json:"modelUnified,omitempty"`
	ModelProvider        string    `json:"modelProvider,omitempty"`
	ModelPublicModel     string    `json:"modelPublicModel,omitempty"`
	ModelBaseURL         string    `json:"modelBaseUrl,omitempty"`
	ModelAPIKeyRef       string    `json:"modelApiKeyRef,omitempty"`
	ModelSSHProxy        string    `json:"modelSshProxy,omitempty"`
	ModelUpstreamProxyID string    `json:"modelUpstreamProxyId,omitempty"`
	ModelRevision        int       `json:"modelRevision,omitempty"`
	ModelProxyKey        string    `json:"modelProxyKey,omitempty"`
	ModelProfileCaptured time.Time `json:"modelProfileCapturedAt,omitempty"`
}
