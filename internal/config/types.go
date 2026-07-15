package config

import "time"

// CurrentVersion is the schema generation this binary stamps into configs it
// writes. Generation 6 adds the extensible global defaults container. The
// field is additive and keeps the reader floor unchanged, while the newer
// write generation prevents an older helper from silently dropping defaults it
// does not understand.
const CurrentVersion = 6

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
	Defaults                *GlobalDefaults            `json:"defaults,omitempty"`
	DefaultModelProfile     string                     `json:"defaultModelProfile,omitempty"`
	ModelProfiles           map[string]ModelProfile    `json:"modelProfiles,omitempty"`
	ModelConfigVersion      int                        `json:"modelConfigVersion,omitempty"`
	ModelCredentials        map[string]ModelCredential `json:"modelCredentials,omitempty"`
	ModelProviders          map[string]ModelProvider   `json:"modelProviders,omitempty"`
	Models                  map[string]ModelDefinition `json:"models,omitempty"`
	ModelSources            map[string]ModelSource     `json:"modelSources,omitempty"`
	TeamsCodexPath          TeamsCodexPathPolicy       `json:"teamsCodexPath,omitempty"`
}

// GlobalDefaults contains explicit defaults shared by future launches and
// sessions that use the same CXP config root. Nil/empty fields mean that the
// existing consumer-specific fallback remains authoritative. Keep this struct
// typed as new defaults are added; command extensibility must not turn the
// persisted configuration into an unvalidated string map.
type GlobalDefaults struct {
	Model           string `json:"model,omitempty"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
}

const CurrentModelConfigVersion = 1

type ModelSource struct {
	Kind                    string    `json:"kind,omitempty"` // git, file, or directory
	URL                     string    `json:"url"`
	Path                    string    `json:"path,omitempty"`
	Ref                     string    `json:"ref,omitempty"`
	File                    string    `json:"file,omitempty"`
	Manifest                string    `json:"manifest,omitempty"`
	Revision                string    `json:"revision,omitempty"`
	Digest                  string    `json:"digest,omitempty"`
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
	UpstreamMode       string `json:"upstreamMode,omitempty"`
	Format             string `json:"format,omitempty"`
	IdleTimeoutSeconds int    `json:"idleTimeoutSeconds,omitempty"`
	// FirstEventTimeoutSeconds bounds the time to the first semantic provider
	// event. Transport heartbeats/comments do not satisfy this bound.
	FirstEventTimeoutSeconds int `json:"firstEventTimeoutSeconds,omitempty"`
	// SemanticProgressTimeoutSeconds distinguishes a live transport from a
	// provider that is no longer producing reasoning, text, tools, or usage.
	SemanticProgressTimeoutSeconds int `json:"semanticProgressTimeoutSeconds,omitempty"`
	// MaxDurationSeconds is a hard wall-clock deadline for a streamed request.
	MaxDurationSeconds int `json:"maxDurationSeconds,omitempty"`
	// HeartbeatMode is ignore, transport-only, or semantic. The latter lets
	// provider-declared SSE heartbeats reset semantic-progress watchdogs.
	HeartbeatMode          string `json:"heartbeatMode,omitempty"`
	SynthesizeResponsesSSE *bool  `json:"synthesizeResponsesSSE,omitempty"`
	ReasoningDeltaPath     string `json:"reasoningDeltaPath,omitempty"`
	ReasoningTokensPath    string `json:"reasoningTokensPath,omitempty"`
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

// ModelCapabilityModes preserves the provider-facing implementation mode that
// a schema-v2 catalog declared. The legacy boolean fields above answer whether
// a capability is usable; these fields retain whether it is native, translated
// by CXP, supplied by a plugin, advisory only, unsupported, or explicitly
// unknown. Keeping both lets runtime code fail closed without losing the
// declaration needed by catalog/status tooling.
type ModelCapabilityModes struct {
	Tools            string `json:"tools,omitempty"`
	ParallelTools    string `json:"parallelTools,omitempty"`
	Vision           string `json:"vision,omitempty"`
	Reasoning        string `json:"reasoning,omitempty"`
	ReasoningSummary string `json:"reasoningSummary,omitempty"`
	WebSearch        string `json:"webSearch,omitempty"`
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
	ParallelEnforcement   string `json:"parallelEnforcement,omitempty"`
	StrictSchema          string `json:"strictSchema,omitempty"`
	EmptyAssistantContent string `json:"emptyAssistantContent,omitempty"`
	InvalidArguments      string `json:"invalidArguments,omitempty"`
	PlainTextToolCall     string `json:"plainTextToolCall,omitempty"`
	ToolCallIDMaxLength   int    `json:"toolCallIdMaxLength,omitempty"`
	ToolNameMaxLength     int    `json:"toolNameMaxLength,omitempty"`
	ValidateArguments     *bool  `json:"validateArguments,omitempty"`
	CustomToolMode        string `json:"customToolMode,omitempty"`
}

// ModelStructuredOutputPolicy records each Responses structured-output
// format independently. Providers often support json_schema while returning
// unusable output for json_object.
type ModelStructuredOutputPolicy struct {
	JSONObject string `json:"jsonObject,omitempty"`
	JSONSchema string `json:"jsonSchema,omitempty"`
}

type ModelResponsesPolicy struct {
	StructuredOutput ModelStructuredOutputPolicy `json:"structuredOutput,omitempty"`
}

// ModelRoute identifies the wire interface used for one operation. Routes
// are kept separate from provider transport defaults so a future operation-
// aware dispatcher can select an adapter without guessing from model names.
type ModelRoute struct {
	Interface  string `json:"interface"`
	Adapter    string `json:"adapter"`
	Protocol   string `json:"protocol"`
	Conversion string `json:"conversion,omitempty"`
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
	Provider        string                `json:"provider"`
	UpstreamModel   string                `json:"upstreamModel"`
	DisplayName     string                `json:"displayName,omitempty"`
	Aliases         []string              `json:"aliases,omitempty"`
	Description     string                `json:"description,omitempty"`
	Priority        int                   `json:"priority,omitempty"`
	Capabilities    ModelCapabilities     `json:"capabilities,omitempty"`
	CapabilityModes ModelCapabilityModes  `json:"capabilityModes,omitempty"`
	Limits          ModelLimits           `json:"limits,omitempty"`
	Reasoning       ModelReasoningPolicy  `json:"reasoning,omitempty"`
	Tools           ModelToolPolicy       `json:"tools,omitempty"`
	Messages        ModelMessagePolicy    `json:"messages,omitempty"`
	Sampling        ModelSamplingPolicy   `json:"sampling,omitempty"`
	Stream          ModelStreamPolicy     `json:"stream,omitempty"`
	HTTP            ModelHTTPPolicy       `json:"http,omitempty"`
	Cache           ModelCachePolicy      `json:"cache,omitempty"`
	Responses       ModelResponsesPolicy  `json:"responses,omitempty"`
	Routes          map[string]ModelRoute `json:"routes,omitempty"`
	Search          ModelSearchPolicy     `json:"search,omitempty"`
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
