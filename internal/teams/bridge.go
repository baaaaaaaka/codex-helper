package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	xhtml "golang.org/x/net/html"
)

const (
	pollCursorOverlap                       = 2 * time.Minute
	fastPollInterval                        = time.Second
	fastPollDuration                        = 90 * time.Second
	ownerPollMessageTop                     = 20
	outboxRecoveryMessageTop                = 20
	transcriptSyncMinInterval               = 10 * time.Second
	historyWatchSyncMinInterval             = 10 * time.Second
	historyWatchReconcileInterval           = 5 * time.Minute
	historyWatchRecentDays                  = 3
	historyTieredMaxTailBytes               = 512 * 1024
	helperAutoUpdateStateRefreshInterval    = time.Minute
	pendingCodexUpgradeStateRefreshInterval = time.Minute
	pendingUpgradeBlockedRetryInterval      = 5 * time.Second
	registryProjectionSaveMinInterval       = time.Minute
	dashboardProjectsCacheTTL               = 30 * time.Second
	persistentPollFailureRestartAfter       = 10 * time.Minute
	persistentPollFailureRestartMinCount    = 3
	recentDuplicateSessionPromptWindow      = 3 * time.Minute
	teamsASRWarmUpTimeout                   = 20 * time.Minute

	// Automatic transcript sync is for small local Codex CLI catch-up. Large
	// history import must stay explicit, otherwise helper startup can flood a
	// Teams chat and delay inbound polling.
	transcriptSyncMaxRecordsPerSessionPerCycle = 8
	transcriptSyncMaxAutoBacklogRecords        = 80
	transcriptImportMaxBatchesPerCycle         = 1
	transcriptImportBatchSeparatorHTML         = "<p>&nbsp;</p>"
	mainLoopOutboxFlushMaxMessages             = 2
	mainLoopWorkflowFlushMaxNotifications      = 1
	maxQueuedTurnStartsPerCycle                = 16
	parkNoticeGraphFallbackLookupTTL           = 5 * time.Minute
	graphOutboxSendMinInterval                 = 1200 * time.Millisecond

	// Live Graph chat sends in this tenant failed at 102,290 bytes of HTML
	// body content. Split far below that to reduce Teams client rendering
	// stalls while still leaving room for part labels and encoding surprises.
	safeTeamsHTMLContentBytes  = 32 * 1024
	teamsChunkHTMLContentBytes = 24 * 1024
)

var ownerMentionLongTurnThreshold = time.Minute
var discoverCodexProjectsForTeams = codexhistory.DiscoverProjectsContext
var helperRestartDelay = 3 * time.Second
var helperReloadDrainStaleAfter = 6 * time.Minute
var codexIdleStatusInitialDelay = 2 * time.Minute
var codexIdleStatusRepeatDelay = 5 * time.Minute
var codexIdleStatusCancelHintAfter = 7 * time.Minute
var codexIdleStatusMessage = "Still working. No new Codex update yet."
var codexIdleStatusCancelHint = "This is taking longer than usual. To stop the current request, send `helper cancel last` in this chat."
var codexSuspectedStuckAfter = 15 * time.Minute
var codexSuspectedStuckMessage = "Codex has not produced any update for %s. It may be stuck.\n\nI will not retry automatically. To stop the current request, send `helper cancel last` in this chat."
var codexStreamRetryStatusRepeatDelay = 5 * time.Minute
var queuedTurnAttentionDelay = 10 * time.Minute
var queuedTurnAttentionRepeatDelay = 10 * time.Minute

var parkNoticeGraphLookupTops = []int{20, 10, 5, 1}

var errTranscriptCheckpointNotFound = errors.New("transcript checkpoint was not found")
var errTranscriptImportBudgetExhausted = errors.New("transcript import batch budget exhausted")

func isTranscriptCheckpointNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errTranscriptCheckpointNotFound) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "transcript checkpoint was not found") &&
		strings.Contains(msg, "refusing to guess")
}

func transcriptCheckpointNeedsAttentionMessage() string {
	return "Local Codex history sync needs attention. I could not find the saved transcript checkpoint, so I did not guess an import position or import local history from the wrong place."
}

const controlFallbackSessionID = "__control_fallback__"

const (
	importCheckpointStatusImporting = "importing"
	importCheckpointStatusComplete  = "complete"
	importCheckpointStatusFailed    = "failed"
	importCheckpointStatusBlocked   = "blocked"
)

const (
	envTeamsStartupFallbackStopFile      = "CODEX_HELPER_TEAMS_STARTUP_FALLBACK_STOP_FILE"
	envTeamsStartupFallbackExitOnStandby = "CODEX_HELPER_TEAMS_EXIT_ON_STANDBY"
)

type helperRestartNotice struct {
	Version            int                                     `json:"version"`
	Action             string                                  `json:"action,omitempty"`
	Tag                string                                  `json:"tag,omitempty"`
	Manual             bool                                    `json:"manual,omitempty"`
	ControlChatID      string                                  `json:"control_chat_id,omitempty"`
	CommandMessageID   string                                  `json:"command_message_id,omitempty"`
	PendingReplacePath string                                  `json:"pending_replace_path,omitempty"`
	InstallPath        string                                  `json:"install_path,omitempty"`
	RequestedAt        time.Time                               `json:"requested_at,omitempty"`
	ActivationNotices  map[string]helperActivationNoticeRecord `json:"activation_notices,omitempty"`
}

type helperActivationNoticeRecord struct {
	Status   string    `json:"status,omitempty"`
	OutboxID string    `json:"outbox_id,omitempty"`
	QueuedAt time.Time `json:"queued_at,omitempty"`
}

type helperActivationStatus struct {
	Version   int       `json:"version"`
	Status    string    `json:"status,omitempty"`
	Message   string    `json:"message,omitempty"`
	Source    string    `json:"source,omitempty"`
	Dest      string    `json:"dest,omitempty"`
	Want      string    `json:"want,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

const (
	helperRestartNoticeActionRestart = "restart"
	helperRestartNoticeActionReload  = "reload"
	helperRestartNoticeActionUpgrade = "upgrade"

	helperUpgradeActivationFailedNotificationKind         = "helper_upgrade_activation_failed"
	helperUpgradeActivationActionRequiredNotificationKind = "helper_upgrade_activation_action_required"
)

const (
	sessionTitleSourceAuto = "auto"
	sessionTitleSourceUser = "user"
)

const (
	recoveryReasonAmbiguousAfterHelperRestart           = "ambiguous after helper restart"
	recoveryReasonAmbiguousAfterHelperRestartNoticeSent = "ambiguous after helper restart; notice sent"
)

type outboxQueueOptions struct {
	MentionOwner     bool
	NotificationKind string
}

type BridgeOptions struct {
	RegistryPath               string
	StorePath                  string
	Store                      *teamstore.Store
	HelperVersion              string
	OwnerStaleAfter            time.Duration
	Interval                   time.Duration
	Once                       bool
	Top                        int
	MaxWorkChatPollsPerCycle   int
	Executor                   Executor
	ControlFallbackExecutor    Executor
	ControlFallbackModel       string
	ControlFallbackHelpContext string
	ModelProfileResolver       ModelProfileResolver
	ModelProfileManager        ModelProfileManager
	ASRTranscriber             ASRTranscriber
	Runner                     codexrunner.Runner
	HelperRestarter            HelperRestarter
	HelperPendingRestarter     HelperPendingRestarter
	HelperReloader             HelperReloader
	HelperAutoUpdater          HelperAutoUpdater
	HelperAutoUpdatePrerelease bool
	CodexUpgrader              CodexUpgrader
}

type ModelProfileResolver func(context.Context, string) (modelprofile.Snapshot, error)

type ModelProfileManager interface {
	ListModelProfiles(context.Context) (string, error)
	ModelProfileProviders(context.Context) (string, error)
	ModelProfileSetupGuide(context.Context, string) (string, error)
	SetupModelProfile(context.Context, ModelProfileSetupRequest) (ModelProfileSetupResult, error)
	ModelProfileDoctor(context.Context, string) (string, error)
	SetDefaultModelProfile(context.Context, string) (string, error)
	DeleteModelProfile(context.Context, string, bool) (string, error)
	SaveModelProfileAPIKey(context.Context, ModelProfileAPIKeySaveRequest) (ModelProfileAPIKeySaveResult, error)
}

type ModelProfileSetupRequest struct {
	Model      string
	SSHProxy   string
	SetDefault bool
}

type ModelProfileSetupResult struct {
	ProfileName     string
	Provider        string
	Model           string
	DisplayName     string
	APIKeyRef       string
	NeedsAPIKey     bool
	ReusedAPIKey    bool
	CredentialScope string
	SetDefault      bool
}

type ModelProfileAPIKeySaveRequest struct {
	ProfileName     string
	Provider        string
	Model           string
	APIKey          string
	SSHProxy        string
	SetDefault      bool
	CredentialScope string
}

type ModelProfileAPIKeySaveResult struct {
	ProfileName string
	Provider    string
	Model       string
	APIKeyRef   string
	Fingerprint string
	Revision    int
	SetDefault  bool
}

type helperAutoUpdateApplyOptions struct {
	Manual           bool
	ControlChatID    string
	CommandMessageID string
}

type HelperRestarter func(context.Context) error

type HelperPendingRestarter func(context.Context, string, string) error

type HelperReloader func(context.Context, HelperReloadOptions) error

type CodexUpgrader func(context.Context) (CodexUpgradeResult, error)

type CodexUpgradeResult struct {
	Path string
}

type HelperAutoUpdater interface {
	Check(context.Context, HelperAutoUpdateCheck) (HelperAutoUpdateDecision, error)
	Apply(context.Context, HelperAutoUpdateCandidate) (HelperAutoUpdateApplyResult, error)
}

type HelperAutoUpdaterWithApplyOptions interface {
	Check(context.Context, HelperAutoUpdateCheck) (HelperAutoUpdateDecision, error)
	ApplyWithOptions(context.Context, HelperAutoUpdateCandidate, HelperAutoUpdateApplyOptions) (HelperAutoUpdateApplyResult, error)
}

type HelperAutoUpdateApplyOptions struct {
	OwnsPendingReplacement bool
}

type HelperReloadOptions struct {
	Force         bool
	BeforeRestart func(context.Context) error
}

type HelperAutoUpdateCheck struct {
	InstalledVersion  string
	Now               time.Time
	IncludePrerelease bool
	Manual            bool
}

type HelperAutoUpdateDecision struct {
	Candidate    *HelperAutoUpdateCandidate
	NextCheckAt  time.Time
	BackoffUntil time.Time
	LastError    string
}

type HelperAutoUpdateCandidate struct {
	TagName     string
	Version     string
	Priority    string
	PublishedAt time.Time
	EligibleAt  time.Time
	Asset       string
}

type HelperAutoUpdateApplyResult struct {
	Version            string
	InstallPath        string
	RestartRequired    bool
	PendingReplacePath string
	ActivationPending  bool
	ActivationReason   string
}

type PersistentPollFailureError struct {
	Since time.Time
	Count int
	Err   error
}

func (e *PersistentPollFailureError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("persistent Teams poll failure since %s (%d consecutive network/proxy failures); requesting Teams listener restart: %v", e.Since.Format(time.RFC3339), e.Count, e.Err)
}

func (e *PersistentPollFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type Bridge struct {
	graph                             *GraphClient
	readGraph                         *GraphClient
	fileGraph                         *GraphClient
	httpClient                        *http.Client
	registryPath                      string
	reg                               Registry
	regMu                             sync.Mutex
	registryProjectionLastFingerprint string
	registryProjectionLastSavedAt     time.Time
	user                              User
	scope                             teamstore.ScopeIdentity
	machine                           teamstore.MachineRecord
	lease                             teamstore.ControlLease
	leaseDuration                     time.Duration
	out                               io.Writer
	executor                          Executor
	asrTranscriber                    ASRTranscriber
	controlFallbackExecutor           Executor
	controlFallbackModel              string
	helperRestarter                   HelperRestarter
	modelProfileResolver              ModelProfileResolver
	modelProfileManager               ModelProfileManager
	helperPendingRestarter            HelperPendingRestarter
	helperReloader                    HelperReloader
	helperAutoUpdater                 HelperAutoUpdater
	helperVersion                     string
	helperAutoUpdatePrerelease        bool
	helperAutoUpdateMu                sync.Mutex
	helperAutoUpdateNextProbeAt       time.Time
	pendingCodexUpgradeMu             sync.Mutex
	pendingCodexUpgradeNextProbeAt    time.Time
	codexUpgrader                     CodexUpgrader
	controlFallbackHelpContext        string
	store                             *teamstore.Store
	asyncTurns                        bool
	ownerMu                           sync.Mutex
	owner                             teamstore.OwnerMetadata
	ownerStaleAfter                   time.Duration
	ownerHeartbeatInterval            time.Duration
	outboxFlushMu                     sync.Mutex
	outboxSendPaceMu                  sync.Mutex
	outboxSendPaceLast                map[string]time.Time
	pollMu                            sync.Mutex
	fastPollUntil                     time.Time
	lastPollErrorLog                  string
	lastPollErrorLogAt                time.Time
	persistentPollFailureFirstAt      time.Time
	persistentPollFailureCount        int
	parkNoticeLookupMu                sync.Mutex
	parkNoticeLookupPreferences       map[string]parkNoticeLookupPreference
	lastTranscriptSync                time.Time
	lastHistoryWatchSync              time.Time
	lastHistoryWatchReconcile         time.Time
	lastBeaconReconcile               time.Time
	lastBeaconLeaseMaintenance        time.Time
	maxWorkChatPollsPerCycle          int
	maxQueuedTurnStartsPerCycle       int
	dashboardProjectsMu               sync.Mutex
	dashboardProjectsCache            []codexhistory.Project
	dashboardProjectsCachedAt         time.Time
	annotateUserMessages              bool
	annotationDisabled                bool
	annotationWarned                  bool
	markAnswerChatsUnread             bool
	markAnswerUnreadWarned            bool
	asyncTurnWG                       sync.WaitGroup
	helperRestartWG                   sync.WaitGroup
	runningTurnMu                     sync.Mutex
	runningTurnCancels                map[string]*runningTurnCancel
	acceptedOutboxMu                  sync.Mutex
	acceptedOutboxes                  map[string]acceptedOutboxRecovery
	globalOutboundMu                  sync.Mutex
	globalOutboundBackfilled          bool
	deferredNoticeMu                  sync.Mutex
	deferredInterruptedPending        bool
}

type runningTurnCancel struct {
	sessionID string
	cancel    context.CancelFunc
	requested bool
	reason    string
	silent    bool
}

type acceptedOutboxRecovery struct {
	TeamsMessageID string
	AcceptedAt     time.Time
}

type parkNoticeLookupPreference struct {
	Top   int
	Until time.Time
}

func NewBridge(ctx context.Context, auth *AuthManager, registryPath string, out io.Writer) (*Bridge, error) {
	graph := NewGraphClient(auth, out)
	readGraph, err := NewReadGraphClient(out)
	if err != nil {
		return nil, err
	}
	return newBridgeWithGraphClients(ctx, graph, readGraph, registryPath, out)
}

func NewBridgeWithHTTPClient(ctx context.Context, auth *AuthManager, registryPath string, out io.Writer, client *http.Client) (*Bridge, error) {
	graph := NewGraphClientWithHTTPClient(auth, out, client)
	readGraph, err := NewReadGraphClientWithHTTPClient(out, client)
	if err != nil {
		return nil, err
	}
	return newBridgeWithGraphClients(ctx, graph, readGraph, registryPath, out)
}

func newBridgeWithGraphClients(ctx context.Context, graph *GraphClient, readGraph *GraphClient, registryPath string, out io.Writer) (*Bridge, error) {
	user, err := graph.Me(ctx)
	if err != nil {
		return nil, err
	}
	scope := ScopeIdentityForUser(user)
	if strings.TrimSpace(registryPath) == "" {
		var resolvedScope teamstore.ScopeIdentity
		resolvedScope, registryPath, err = ResolveRegistryPathForScope(scope)
		if err != nil {
			return nil, err
		}
		scope = resolvedScope
	}
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		return nil, err
	}
	reg.UserID = user.ID
	reg.UserPrincipal = user.UserPrincipalName
	httpClient := graph.httpClient()
	machine := MachineRecordForUser(user, scope)
	applyMachineHostnameOverrideToRecord(&machine, reg.MachineHostnameOverride)
	return &Bridge{graph: graph, readGraph: readGraph, httpClient: httpClient, registryPath: registryPath, reg: reg, user: user, scope: scope, machine: machine, out: out, markAnswerChatsUnread: true}, nil
}

func (b *Bridge) readClient() *GraphClient {
	if b != nil && b.readGraph != nil {
		return b.readGraph
	}
	return b.graph
}

func (b *Bridge) EnsureControlChat(ctx context.Context) (Chat, error) {
	return b.ensureControlChat(ctx, true)
}

func (b *Bridge) ensureControlChat(ctx context.Context, syncRegistry bool) (Chat, error) {
	if syncRegistry {
		if err := b.migrateRegistryProjectionToStore(ctx); err != nil {
			return Chat{}, err
		}
		if err := b.restoreRegistryFromStore(ctx); err != nil {
			return Chat{}, err
		}
	}
	if b.reg.ControlChatID != "" {
		desiredTopic := b.desiredControlChatTopic()
		if desiredTopic != "" && b.reg.ControlChatTopic != desiredTopic {
			if err := b.graph.UpdateChatTopic(ctx, b.reg.ControlChatID, desiredTopic); err == nil {
				b.reg.ControlChatTopic = SanitizeTopic(desiredTopic)
				_ = b.recordControlChatBinding(ctx, Chat{ID: b.reg.ControlChatID, Topic: b.reg.ControlChatTopic, WebURL: b.reg.ControlChatURL})
				_ = b.Save()
			}
		}
		_ = b.recordControlChatBinding(ctx, Chat{ID: b.reg.ControlChatID, Topic: b.reg.ControlChatTopic, WebURL: b.reg.ControlChatURL})
		return Chat{ID: b.reg.ControlChatID, Topic: b.reg.ControlChatTopic, WebURL: b.reg.ControlChatURL, ChatType: "meeting"}, nil
	}
	topic := ControlChatTitle(ChatTitleOptions{MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()), Profile: b.scope.Profile})
	if chat, ok := b.findExistingControlChat(ctx, topic); ok {
		b.reg.ControlChatID = chat.ID
		b.reg.ControlChatTopic = chat.Topic
		b.reg.ControlChatURL = chat.WebURL
		if err := b.recordControlChatBinding(ctx, chat); err != nil {
			return chat, err
		}
		if err := b.Save(); err != nil {
			return chat, err
		}
		return chat, nil
	}
	chat, err := b.createMeetingChat(ctx, topic)
	if err != nil {
		return Chat{}, err
	}
	b.reg.ControlChatID = chat.ID
	b.reg.ControlChatTopic = chat.Topic
	b.reg.ControlChatURL = chat.WebURL
	if err := b.recordControlChatBinding(ctx, chat); err != nil {
		return chat, err
	}
	if err := b.Save(); err != nil {
		return chat, err
	}
	if err := b.sendChatCreatedMention(ctx, "", chat.ID, "Control chat created."); err != nil {
		return chat, err
	}
	err = b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          directOutboxID(chat.ID, "control-ready", "control chat is ready"),
		TeamsChatID: chat.ID,
		Kind:        "control",
		Body:        "control chat is ready.\n\n" + controlHelpText(),
	})
	return chat, err
}

func (b *Bridge) desiredControlChatTopic() string {
	return ControlChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		UserTitle:    b.reg.ControlChatUserTitle,
	})
}

func (b *Bridge) createMeetingChat(ctx context.Context, topic string) (Chat, error) {
	if b == nil || b.graph == nil {
		return Chat{}, fmt.Errorf("Teams Graph client is not configured")
	}
	return b.graph.CreateMeetingChat(ctx, topic)
}

func (b *Bridge) sendChatCreatedMention(ctx context.Context, sessionID string, chatID string, text string) error {
	return b.sendChatCreatedNotice(ctx, sessionID, chatID, text, true)
}

func (b *Bridge) sendChatCreatedNotice(ctx context.Context, sessionID string, chatID string, text string, notify bool) error {
	chatID = strings.TrimSpace(chatID)
	queued, err := b.queueChatCreatedNotice(ctx, sessionID, chatID, text, notify)
	if err != nil || !queued {
		return err
	}
	return b.flushPendingOutboxForChat(ctx, chatID)
}

func (b *Bridge) queueChatCreatedNotice(ctx context.Context, sessionID string, chatID string, text string, notify bool) (bool, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return false, nil
	}
	text = strings.TrimSpace(text)
	if text == "" {
		text = "Teams chat created."
	}
	kind := "chat-created"
	notificationKind := "chat_created"
	if !notify {
		kind = "chat-created-silent"
		notificationKind = ""
	}
	if _, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               directOutboxID(chatID, "chat-created", text),
		SessionID:        sessionID,
		TeamsChatID:      chatID,
		Kind:             kind,
		Body:             text,
		MentionOwner:     notify,
		NotificationKind: notificationKind,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Bridge) sendLocalSessionStartedNotice(ctx context.Context, sessionID string, chatID string) error {
	chatID = strings.TrimSpace(chatID)
	queued, err := b.queueLocalSessionStartedNotice(ctx, sessionID, chatID)
	if err != nil || !queued {
		return err
	}
	return b.flushPendingOutboxForChat(ctx, chatID)
}

func (b *Bridge) queueLocalSessionStartedNotice(ctx context.Context, sessionID string, chatID string) (bool, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return false, nil
	}
	sessionID = strings.TrimSpace(sessionID)
	text := "Local Codex chat detected."
	if sessionID != "" {
		text = "Local Codex chat detected: " + sessionID + "."
	}
	if _, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:               directOutboxID(chatID, "local-session-started", text),
		SessionID:        sessionID,
		TeamsChatID:      chatID,
		Kind:             "local-session-started",
		Body:             text,
		MentionOwner:     true,
		NotificationKind: "local_session_started",
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Bridge) findExistingControlChat(ctx context.Context, topic string) (Chat, bool) {
	topic = strings.TrimSpace(SanitizeTopic(topic))
	if topic == "" || b.graph == nil {
		return Chat{}, false
	}
	chats, err := b.graph.ListChats(ctx, 50)
	if err != nil {
		return Chat{}, false
	}
	for _, chat := range chats {
		if strings.TrimSpace(chat.Topic) != topic {
			continue
		}
		if chat.ChatType != "" && chat.ChatType != "meeting" {
			continue
		}
		members, err := b.graph.ListChatMembers(ctx, chat.ID)
		if err != nil {
			return Chat{}, false
		}
		if len(members) == 1 && (strings.TrimSpace(members[0].UserID) == b.user.ID || strings.EqualFold(strings.TrimSpace(members[0].Email), strings.TrimSpace(b.user.UserPrincipalName))) {
			return chat, true
		}
	}
	return Chat{}, false
}

func (b *Bridge) Save() error {
	b.regMu.Lock()
	defer b.regMu.Unlock()
	data, err := json.Marshal(b.reg)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	fingerprint := hex.EncodeToString(sum[:])
	now := time.Now()
	if fingerprint == b.registryProjectionLastFingerprint &&
		!b.registryProjectionLastSavedAt.IsZero() &&
		now.Sub(b.registryProjectionLastSavedAt) < registryProjectionSaveMinInterval {
		path := strings.TrimSpace(b.registryPath)
		if path == "" {
			var pathErr error
			path, pathErr = DefaultRegistryPath()
			if pathErr != nil {
				return pathErr
			}
		}
		if info, statErr := os.Stat(path); statErr == nil && !info.ModTime().After(b.registryProjectionLastSavedAt) {
			return nil
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}
	if err := SaveRegistry(b.registryPath, b.reg); err != nil {
		return err
	}
	b.registryProjectionLastFingerprint = fingerprint
	b.registryProjectionLastSavedAt = time.Now()
	return nil
}

func (b *Bridge) markRegistrySent(chatID string, messageID string) {
	if b == nil {
		return
	}
	b.regMu.Lock()
	defer b.regMu.Unlock()
	b.reg.MarkSent(chatID, messageID)
}

func (b *Bridge) Listen(ctx context.Context, opts BridgeOptions) error {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Top <= 0 {
		opts.Top = 50
	}
	if opts.OwnerStaleAfter <= 0 {
		opts.OwnerStaleAfter = 2 * time.Minute
	}
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	if b.machine.ID == "" {
		b.machine = MachineRecordForUser(b.user, b.scope)
		b.applyRegistryMachineHostnameOverride()
	}
	b.maxWorkChatPollsPerCycle = opts.MaxWorkChatPollsPerCycle
	b.leaseDuration = opts.Interval * 3
	if b.leaseDuration < 30*time.Second {
		b.leaseDuration = 30 * time.Second
	}
	if b.leaseDuration < opts.OwnerStaleAfter {
		b.leaseDuration = opts.OwnerStaleAfter
	}
	b.store = opts.Store
	if b.store == nil {
		storePath := opts.StorePath
		if strings.TrimSpace(storePath) == "" {
			var err error
			var resolvedScope teamstore.ScopeIdentity
			resolvedScope, storePath, err = ResolveStorePathForScope(b.scope)
			if err != nil {
				return err
			}
			if resolvedScope.ID != b.scope.ID {
				b.scope = resolvedScope
				b.machine = MachineRecordForUser(b.user, b.scope)
				b.applyRegistryMachineHostnameOverride()
			}
		}
		store, err := teamstore.Open(storePath)
		if err != nil {
			return err
		}
		b.store = store
	}
	if _, err := b.store.RecordScope(ctx, b.scope); err != nil {
		return err
	}
	if err := b.migrateRegistryProjectionToStore(ctx); err != nil {
		return err
	}
	if err := b.restoreRegistryFromStore(ctx); err != nil {
		return err
	}
	if opts.Runner != nil {
		b.executor = RunnerExecutor{Runner: opts.Runner}
	} else {
		b.executor = opts.Executor
	}
	b.asrTranscriber = opts.ASRTranscriber
	b.controlFallbackExecutor = opts.ControlFallbackExecutor
	b.controlFallbackModel = strings.TrimSpace(opts.ControlFallbackModel)
	b.controlFallbackHelpContext = strings.TrimSpace(opts.ControlFallbackHelpContext)
	b.modelProfileResolver = opts.ModelProfileResolver
	b.modelProfileManager = opts.ModelProfileManager
	b.helperRestarter = opts.HelperRestarter
	b.helperPendingRestarter = opts.HelperPendingRestarter
	b.helperReloader = opts.HelperReloader
	b.helperAutoUpdater = opts.HelperAutoUpdater
	b.helperVersion = strings.TrimSpace(opts.HelperVersion)
	b.helperAutoUpdatePrerelease = opts.HelperAutoUpdatePrerelease
	b.codexUpgrader = opts.CodexUpgrader
	b.asyncTurns = !opts.Once
	b.annotateUserMessages = true
	if b.executor == nil {
		b.executor = CodexExecutor{}
	}
	if active, err := b.claimControlLease(ctx); err != nil {
		return err
	} else if !active {
		return b.runStandbyLoop(ctx, opts)
	}
	owner, err := teamstore.CurrentOwner(opts.HelperVersion, "", "", time.Now())
	if err != nil {
		return err
	}
	owner.ScopeID = b.scope.ID
	owner.MachineID = b.machine.ID
	owner.LeaseGeneration = b.currentLeaseGeneration()
	b.setOwner(owner, opts.OwnerStaleAfter)
	if err := b.recordOwnerHeartbeat(ctx, "", ""); err != nil {
		if errors.Is(err, teamstore.ErrOwnerLive) && !opts.Once {
			return b.runStandbyLoop(ctx, opts)
		}
		return err
	}
	defer func() {
		b.clearOwnerIfSame(context.Background())
		_, _ = b.store.ReleaseControlLeaseIfHolder(context.Background(), b.machine.ID, b.currentLeaseGeneration())
	}()
	if err := b.clearStaleHelperReloadDrainOnStart(ctx); err != nil {
		return err
	}
	if err := b.completeExpiredHelperUpgradeDrainOnStart(ctx); err != nil {
		return err
	}
	chat, err := b.initializeControlChatAndRecoveryAfterRegistrySync(ctx)
	if err != nil {
		return err
	}
	if deferMigration, err := b.shouldDeferTeamsStoreSQLiteMigration(ctx); err != nil {
		return err
	} else if !deferMigration {
		if err := b.migrateTeamsStoreToSQLiteOrFallback(ctx); err != nil {
			return err
		}
	}
	ownerHeartbeatCtx, cancelOwnerHeartbeat := context.WithCancel(ctx)
	ownerHeartbeatDone := b.startOwnerHeartbeat(ownerHeartbeatCtx)
	defer func() {
		cancelOwnerHeartbeat()
		if ownerHeartbeatDone != nil {
			<-ownerHeartbeatDone
		}
	}()
	if b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams control chat: %s\n", chat.WebURL)
		_, _ = fmt.Fprintln(b.out, "Listening. Send `help`, `p`, or `n <directory>` in the control chat.")
	}
	b.startASRWarmUp(ctx)
	for {
		if teamsStartupFallbackStopRequested() {
			if b.out != nil {
				_, _ = fmt.Fprintln(b.out, "Teams Startup fallback retire signal detected; exiting.")
			}
			return nil
		}
		if ownerHeartbeatDone != nil {
			select {
			case err := <-ownerHeartbeatDone:
				ownerHeartbeatDone = nil
				if err != nil {
					return err
				}
			default:
			}
		}
		if active, err := b.refreshControlLease(ctx); err != nil {
			return err
		} else if !active {
			b.clearOwnerIfSame(context.Background())
			return b.runStandbyLoop(ctx, opts)
		}
		if err := b.flushPendingOutboxMainLoop(ctx); err != nil && b.out != nil && !isOutboxDeliveryDeferred(err) {
			_, _ = fmt.Fprintf(b.out, "Teams outbox flush error: %v\n", err)
		}
		if err := b.flushPendingWorkflowNotificationsWithLimit(ctx, mainLoopWorkflowFlushMaxNotifications); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow notification flush error: %v\n", err)
		}
		if err := b.pollOnce(ctx, opts.Top); err != nil {
			if b.out != nil {
				if b.shouldLogPollError(err, time.Now()) {
					_, _ = fmt.Fprintf(b.out, "Teams poll error: %v\n", err)
				}
			}
			if restartErr := b.notePollFailure(err, time.Now()); restartErr != nil {
				return restartErr
			}
		}
		if err := b.syncLinkedTranscriptsIfDue(ctx, time.Now()); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams transcript sync error: %v\n", err)
		}
		if err := b.syncCodexHistoryFinalsIfDue(ctx, time.Now()); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams history watch error: %v\n", err)
		}
		if err := b.maybeRunHelperAutoUpdate(ctx, opts); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper auto-update error: %v\n", err)
		}
		if _, err := b.queueCompletedHelperUpgradeNoticeIfNeeded(ctx); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper upgrade completion notice error: %v\n", err)
		}
		if err := b.maybeRunPendingCodexUpgrade(ctx); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams Codex upgrade error: %v\n", err)
		}
		if err := b.maybeRunBeaconReconcile(ctx, time.Now()); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams beacon reconcile error: %v\n", err)
		}
		if err := b.maybeRunBeaconLeaseMaintenance(ctx, time.Now()); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams beacon lease maintenance error: %v\n", err)
		}
		if drained, err := b.drainComplete(ctx); err != nil {
			return err
		} else if drained {
			if b.out != nil {
				_, _ = fmt.Fprintln(b.out, "Teams bridge drained; exiting.")
			}
			return nil
		}
		if err := b.processDeferredInbound(ctx); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams deferred input processing error: %v\n", err)
		}
		if err := b.processQueuedTurns(ctx); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams queued turn processing error: %v\n", err)
		}
		if err := b.sendDeferredInterruptedTurnNotices(ctx); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams interrupted turn notice error: %v\n", err)
		}
		if err := b.Save(); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams registry projection save skipped: %v\n", err)
		}
		if opts.Once {
			return nil
		}
		sleepInterval := b.nextPollInterval(opts.Interval, time.Now())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepInterval):
		}
	}
}

func (b *Bridge) initializeControlChatAndRecovery(ctx context.Context) (Chat, error) {
	return b.initializeControlChatAndRecoveryWithRegistrySync(ctx, true)
}

func (b *Bridge) initializeControlChatAndRecoveryAfterRegistrySync(ctx context.Context) (Chat, error) {
	return b.initializeControlChatAndRecoveryWithRegistrySync(ctx, false)
}

func (b *Bridge) initializeControlChatAndRecoveryWithRegistrySync(ctx context.Context, syncRegistry bool) (Chat, error) {
	chat, err := b.ensureControlChat(ctx, syncRegistry)
	if err != nil {
		return Chat{}, err
	}
	if _, err := b.queueCompletedHelperUpgradeNoticeIfNeeded(ctx); err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper upgrade completion notice error: %v\n", err)
		}
	}
	if err := b.queuePendingHelperRestartNotice(ctx); err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper restart notice error: %v\n", err)
		}
	} else if err := b.flushPendingOutboxForChat(ctx, chat.ID); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams control outbox flush error: %v\n", err)
	}
	if err := b.recoverUnfinishedTurns(ctx); err != nil {
		return Chat{}, err
	}
	return chat, nil
}

func (b *Bridge) startASRWarmUp(ctx context.Context) {
	if b == nil || !teamsASRTranscriberConfigured(b.asrTranscriber) {
		return
	}
	warmable, ok := b.asrTranscriber.(ASRWarmUpTranscriber)
	if !ok || warmable == nil {
		return
	}
	go func() {
		warmCtx, cancel := context.WithTimeout(ctx, teamsASRWarmUpTimeout)
		defer cancel()
		if err := warmable.WarmUpTeamsASR(warmCtx); err != nil && warmCtx.Err() == nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams ASR warm-up failed: %v\n", err)
		}
	}()
}

func (b *Bridge) boostPolling(now time.Time) {
	if b == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	until := now.Add(fastPollDuration)
	b.pollMu.Lock()
	if until.After(b.fastPollUntil) {
		b.fastPollUntil = until
	}
	b.pollMu.Unlock()
}

func (b *Bridge) nextPollInterval(base time.Duration, now time.Time) time.Duration {
	if base <= 0 {
		base = 5 * time.Second
	}
	if b == nil {
		return base
	}
	if now.IsZero() {
		now = time.Now()
	}
	b.pollMu.Lock()
	until := b.fastPollUntil
	b.pollMu.Unlock()
	if now.Before(until) && base > fastPollInterval {
		return fastPollInterval
	}
	return base
}

func (b *Bridge) helperAutoUpdateProbeDue(now time.Time) bool {
	if b == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	b.helperAutoUpdateMu.Lock()
	defer b.helperAutoUpdateMu.Unlock()
	return b.helperAutoUpdateNextProbeAt.IsZero() || !now.Before(b.helperAutoUpdateNextProbeAt)
}

func (b *Bridge) clearHelperAutoUpdateProbeGate() {
	if b == nil {
		return
	}
	b.helperAutoUpdateMu.Lock()
	b.helperAutoUpdateNextProbeAt = time.Time{}
	b.helperAutoUpdateMu.Unlock()
}

func (b *Bridge) scheduleHelperAutoUpdateProbe(now time.Time, candidates ...time.Time) {
	if b == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	next := now.Add(helperAutoUpdateStateRefreshInterval)
	for _, candidate := range candidates {
		if candidate.IsZero() {
			continue
		}
		if !candidate.After(now) {
			next = now
			break
		}
		if candidate.Before(next) {
			next = candidate
		}
	}
	b.helperAutoUpdateMu.Lock()
	b.helperAutoUpdateNextProbeAt = next
	b.helperAutoUpdateMu.Unlock()
}

func (b *Bridge) pendingCodexUpgradeProbeDue(now time.Time) bool {
	if b == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	b.pendingCodexUpgradeMu.Lock()
	defer b.pendingCodexUpgradeMu.Unlock()
	return b.pendingCodexUpgradeNextProbeAt.IsZero() || !now.Before(b.pendingCodexUpgradeNextProbeAt)
}

func (b *Bridge) clearPendingCodexUpgradeProbeGate() {
	if b == nil {
		return
	}
	b.pendingCodexUpgradeMu.Lock()
	b.pendingCodexUpgradeNextProbeAt = time.Time{}
	b.pendingCodexUpgradeMu.Unlock()
}

func (b *Bridge) schedulePendingCodexUpgradeProbe(now time.Time, delay time.Duration) {
	if b == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	if delay <= 0 {
		delay = pendingCodexUpgradeStateRefreshInterval
	}
	b.pendingCodexUpgradeMu.Lock()
	b.pendingCodexUpgradeNextProbeAt = now.Add(delay)
	b.pendingCodexUpgradeMu.Unlock()
}

func (b *Bridge) pollOnce(ctx context.Context, top int) error {
	if b.reg.ControlChatID == "" {
		if _, err := b.EnsureControlChat(ctx); err != nil {
			return err
		}
	}
	state, err := b.store.PollScheduleSnapshot(ctx)
	if err != nil {
		return err
	}
	controlPoll, hasControlPoll := state.ChatPolls[b.reg.ControlChatID]
	controlDecision := decideInboundPoll(inboundPollInput{
		ChatID:  b.reg.ControlChatID,
		Role:    inboundPollRoleControl,
		Poll:    controlPoll,
		HasPoll: hasControlPoll,
		Now:     time.Now(),
	})
	if !controlDecision.Due {
		if !inboundPollDecisionAlreadyPersisted(controlPoll, hasControlPoll, controlDecision) {
			if err := b.persistInboundPollDecision(ctx, controlDecision); err != nil {
				return err
			}
		}
	} else {
		controlHandled, err := b.pollChatWithRoleState(ctx, b.reg.ControlChatID, effectiveOwnerPollTop(top), inboundPollRoleControl, false, controlPoll, hasControlPoll, b.handleControlMessage)
		if err != nil {
			return err
		}
		if controlHandled {
			return nil
		}
	}

	state, err = b.store.PollScheduleSnapshot(ctx)
	if err != nil {
		return err
	}
	runningBySession := runningPollSessions(state)
	queueStateBySession := pollSessionTurnQueueStates(state)
	var decisions []inboundPollDecision
	var pendingScheduleUpdates []teamstore.ChatPollScheduleUpdate
	flushPendingScheduleUpdates := func() error {
		if err := b.persistInboundPollScheduleUpdates(ctx, pendingScheduleUpdates); err != nil {
			return err
		}
		pendingScheduleUpdates = pendingScheduleUpdates[:0]
		return nil
	}
	pollsByChat := make(map[string]teamstore.ChatPollState)
	hasPollByChat := make(map[string]bool)
	pollableByChat := make(map[string]Session)
	for _, session := range b.reg.ActiveSessions() {
		pollable, ok := pollableWorkSessionFromRegistry(state, session, b.reg.ControlChatID)
		if !ok {
			continue
		}
		session = pollable
		if transcriptImportIsActive(state, session.ID) {
			continue
		}
		poll, hasPoll := state.ChatPolls[session.ChatID]
		pollsByChat[session.ChatID] = poll
		hasPollByChat[session.ChatID] = hasPoll
		pollableByChat[session.ChatID] = session
		decision := decideInboundPoll(inboundPollInput{
			ChatID:           session.ChatID,
			Role:             inboundPollRoleWork,
			Poll:             poll,
			HasPoll:          hasPoll,
			Running:          runningBySession[session.ID],
			SessionUpdatedAt: session.UpdatedAt,
			Now:              time.Now(),
		})
		if decision.ShouldPark {
			if !decision.ShouldNotifyPark && !poll.ParkedAt.IsZero() && inboundPollDecisionAlreadyPersisted(poll, hasPoll, decision) {
				continue
			}
			if err := flushPendingScheduleUpdates(); err != nil {
				return err
			}
			if err := b.parkIdleWorkChat(ctx, session, decision); err != nil {
				return err
			}
			continue
		}
		if !decision.Due {
			if !inboundPollDecisionAlreadyPersisted(poll, hasPoll, decision) {
				if update, ok := inboundPollDecisionScheduleUpdate(decision); ok {
					pendingScheduleUpdates = append(pendingScheduleUpdates, update)
				}
			}
			continue
		}
		decisions = append(decisions, decision)
	}
	if err := flushPendingScheduleUpdates(); err != nil {
		return err
	}
	sortInboundPollDecisions(decisions)
	if limit := b.effectiveMaxWorkChatPollsPerCycle(); len(decisions) > limit {
		decisions = decisions[:limit]
	}
	var firstErr error
	for _, decision := range decisions {
		session, ok := pollableByChat[decision.ChatID]
		if !ok {
			continue
		}
		s := session
		turns := queueStateBySession[s.ID]
		if _, err := b.pollChatWithRoleState(ctx, s.ChatID, effectiveOwnerPollTop(top), inboundPollRoleWork, runningBySession[s.ID], pollsByChat[s.ChatID], hasPollByChat[s.ChatID], func(ctx context.Context, msg ChatMessage, text string) error {
			return b.handleSessionMessageWithQueueState(ctx, s.ChatID, msg, text, &turns, nil)
		}); err != nil {
			queueStateBySession[s.ID] = turns
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		queueStateBySession[s.ID] = turns
	}
	return firstErr
}

func effectiveOwnerPollTop(top int) int {
	if top <= 0 || top > ownerPollMessageTop {
		return ownerPollMessageTop
	}
	return normalizedMessageTop(top)
}

func pollableWorkSessionFromRegistry(state teamstore.State, session Session, controlChatID string) (Session, bool) {
	if !isActiveSessionStatus(session.Status) || isControlFallbackSessionID(session.ID) {
		return Session{}, false
	}
	if strings.TrimSpace(session.ChatID) == "" {
		return Session{}, false
	}
	if strings.TrimSpace(controlChatID) != "" && session.ChatID == controlChatID {
		return Session{}, false
	}
	if len(state.Sessions) == 0 {
		return session, true
	}
	durable, ok := state.Sessions[session.ID]
	if !ok {
		return Session{}, false
	}
	if isDurableControlFallbackSession(durable) {
		return Session{}, false
	}
	if !isActiveSessionStatus(string(durable.Status)) {
		return Session{}, false
	}
	if strings.TrimSpace(durable.TeamsChatID) == "" || durable.TeamsChatID != session.ChatID {
		return Session{}, false
	}
	return registrySessionFromDurable(durable), true
}

func isControlFallbackSessionID(sessionID string) bool {
	return strings.TrimSpace(sessionID) == controlFallbackSessionID
}

func isDurableControlFallbackSession(session teamstore.SessionContext) bool {
	return isControlFallbackSessionID(session.ID) || strings.EqualFold(strings.TrimSpace(session.RunnerKind), "control_fallback")
}

func registrySessionFromDurable(durable teamstore.SessionContext) Session {
	status := string(durable.Status)
	if status == "" {
		status = string(teamstore.SessionStatusActive)
	}
	return Session{
		ID:            durable.ID,
		ChatID:        durable.TeamsChatID,
		ChatURL:       durable.TeamsChatURL,
		Topic:         durable.TeamsTopic,
		UserTitle:     durable.UserTitle,
		TitleSource:   durable.TitleSource,
		Status:        status,
		CodexThreadID: durable.CodexThreadID,
		Cwd:           durable.Cwd,
		ModelProfile:  durable.ModelProfile,
		CreatedAt:     durable.CreatedAt,
		UpdatedAt:     durable.UpdatedAt,
	}
}

func sanitizeControlFallbackSession(session *teamstore.SessionContext, model string, snapshot modelprofile.Snapshot, now time.Time) bool {
	if session == nil {
		return false
	}
	changed := false
	setString := func(target *string, value string) {
		if *target != value {
			*target = value
			changed = true
		}
	}
	if session.ID != controlFallbackSessionID {
		session.ID = controlFallbackSessionID
		changed = true
	}
	if session.Status != teamstore.SessionStatusActive {
		session.Status = teamstore.SessionStatusActive
		changed = true
	}
	setString(&session.RunnerKind, "control_fallback")
	if strings.TrimSpace(model) != "" {
		setString(&session.Model, model)
	}
	if session.ModelProfile.IsZero() && !snapshot.IsZero() {
		session.ModelProfile = snapshot
		changed = true
	}
	setString(&session.TeamsChatID, "")
	setString(&session.TeamsChatURL, "")
	setString(&session.TeamsTopic, "")
	setString(&session.Cwd, "")
	if changed {
		session.UpdatedAt = now
	}
	return changed
}

func (b *Bridge) pollChat(ctx context.Context, chatID string, top int, handle func(context.Context, ChatMessage, string) error) (bool, error) {
	return b.pollChatWithRole(ctx, chatID, top, inboundPollRoleWork, false, handle)
}

func (b *Bridge) pollChatWithRole(ctx context.Context, chatID string, top int, role inboundPollRole, running bool, handle func(context.Context, ChatMessage, string) error) (bool, error) {
	if err := b.ensureStore(); err != nil {
		return false, err
	}
	poll, hasPoll, err := b.store.ChatPoll(ctx, chatID)
	if err != nil {
		return false, err
	}
	return b.pollChatWithRoleState(ctx, chatID, top, role, running, poll, hasPoll, handle)
}

func (b *Bridge) pollChatWithRoleState(ctx context.Context, chatID string, top int, role inboundPollRole, running bool, poll teamstore.ChatPollState, hasPoll bool, handle func(context.Context, ChatMessage, string) error) (bool, error) {
	if err := b.ensureStore(); err != nil {
		return false, err
	}
	seeded := hasPoll && poll.Seeded
	var modifiedAfter time.Time
	if seeded && !poll.LastModifiedCursor.IsZero() {
		modifiedAfter = poll.LastModifiedCursor.Add(-pollCursorOverlap)
	}
	var (
		window MessageWindow
		err    error
	)
	useContinuation := seeded && role != inboundPollRoleControl && strings.TrimSpace(poll.ContinuationPath) != ""
	if useContinuation {
		window, err = b.readClient().ListMessagesWindowFromPathWithoutRateLimitRetry(ctx, poll.ContinuationPath)
	} else {
		window, err = b.readClient().ListMessagesWindowWithoutRateLimitRetry(ctx, chatID, top, modifiedAfter)
	}
	if err != nil {
		_ = b.store.RecordChatPollErrorWithBlock(ctx, chatID, err.Error(), inboundPollBlockedUntil(poll, err, time.Now()))
		return false, err
	}
	msgs := window.Messages
	sort.Slice(msgs, func(i, j int) bool {
		return messageSortTime(msgs[i]).Before(messageSortTime(msgs[j]))
	})
	maxModified := maxMessageModifiedTime(msgs)
	windowFull := window.Truncated || len(msgs) >= normalizedMessageTop(top)
	if !seeded && len(b.reg.Chats[chatID].SeenMessageIDs) == 0 {
		for _, msg := range msgs {
			b.reg.MarkSeen(chatID, msg.ID)
		}
		_, err = b.recordChatPollSuccessAndSchedule(ctx, chatID, role, running, maxModified, true, windowFull, len(msgs), "", time.Time{})
		if err != nil {
			return false, err
		}
		if role == inboundPollRoleControl {
			b.notePollSuccess(time.Now())
		}
		return false, nil
	}
	handled := false
	var activityAt time.Time
	for _, msg := range msgs {
		legacyFallback := legacyGeneratedMessageFallbackAllowed(msg, poll, hasPoll)
		ignore, err := b.shouldIgnoreMessage(ctx, chatID, msg, role, legacyFallback)
		if err != nil {
			b.recordChatPollHandlerError(ctx, chatID, poll, err)
			return handled, err
		}
		if ignore {
			b.reg.MarkSeen(chatID, msg.ID)
			continue
		}
		if isPromptlessTeamsAttachmentPlaceholderMessage(msg) {
			b.reg.MarkSeen(chatID, msg.ID)
			continue
		}
		text := promptTextFromTeamsMessageHTML(msg.Body.Content)
		if strings.TrimSpace(text) == "" && len(msg.Attachments) == 0 && len(HostedContentIDsFromHTML(msg.Body.Content)) == 0 {
			b.reg.MarkSeen(chatID, msg.ID)
			continue
		}
		globalClaim, claimed, err := b.tryClaimGlobalInbound(ctx, chatID, msg.ID)
		if err != nil {
			b.recordChatPollHandlerError(ctx, chatID, poll, err)
			return handled, err
		}
		if !claimed {
			b.reg.MarkSeen(chatID, msg.ID)
			continue
		}
		if b.currentLeaseGeneration() > 0 {
			if err := b.ensureActiveControlLease(ctx); err != nil {
				releaseGlobalInbound(ctx, globalClaim)
				b.recordChatPollHandlerError(ctx, chatID, poll, err)
				return handled, err
			}
		}
		if err := handle(ctx, msg, text); err != nil {
			if errors.Is(err, teamstore.ErrInboundMessageFromHelperOutbox) {
				b.markRegistrySent(chatID, msg.ID)
				b.reg.MarkSeen(chatID, msg.ID)
				if completeErr := completeGlobalInbound(ctx, globalClaim); completeErr != nil {
					_ = b.store.RecordChatPollError(ctx, chatID, completeErr.Error())
					return handled, completeErr
				}
				continue
			}
			releaseGlobalInbound(ctx, globalClaim)
			b.recordChatPollHandlerError(ctx, chatID, poll, err)
			return handled, err
		}
		b.reg.MarkSeen(chatID, msg.ID)
		if err := completeGlobalInbound(ctx, globalClaim); err != nil {
			b.recordChatPollHandlerError(ctx, chatID, poll, err)
			return handled, err
		}
		b.annotateIncomingUserMessage(ctx, chatID, msg)
		handled = true
		activityAt = latestTime(activityAt, time.Now(), messageSortTime(msg))
		b.boostPolling(time.Now())
	}
	continuationPath := ""
	if seeded && role != inboundPollRoleControl && window.Truncated {
		continuationPath = window.NextPath
	}
	if role == inboundPollRoleControl {
		b.notePollSuccess(time.Now())
	}
	if handled && activityAt.IsZero() {
		activityAt = time.Now()
	}
	_, err = b.recordChatPollSuccessAndSchedule(ctx, chatID, role, running, maxModified, true, windowFull, len(msgs), continuationPath, activityAt)
	return handled, err
}

func (b *Bridge) recordChatPollHandlerError(ctx context.Context, chatID string, poll teamstore.ChatPollState, err error) {
	if b == nil || b.store == nil || err == nil {
		return
	}
	if isGraphRateLimitError(err) {
		b.recordChatPollBackoffError(ctx, chatID, poll, err)
		return
	}
	_ = b.store.RecordChatPollError(ctx, chatID, err.Error())
}

func (b *Bridge) recordChatPollBackoffError(ctx context.Context, chatID string, poll teamstore.ChatPollState, err error) {
	if b == nil || b.store == nil || err == nil {
		return
	}
	_ = b.store.RecordChatPollErrorWithBlock(ctx, chatID, err.Error(), inboundPollBlockedUntil(poll, err, time.Now()))
}

func legacyGeneratedMessageFallbackAllowed(msg ChatMessage, poll teamstore.ChatPollState, hasPoll bool) bool {
	if !hasPoll || !poll.Seeded || poll.LastModifiedCursor.IsZero() {
		return false
	}
	activity := chatMessageActivityTime(msg)
	if activity.IsZero() {
		return false
	}
	return !activity.After(poll.LastModifiedCursor)
}

func normalizedMessageTop(top int) int {
	return normalizedGraphMessagesTop(top)
}

func messageSortTime(msg ChatMessage) time.Time {
	if t := parseGraphTime(msg.CreatedDateTime); !t.IsZero() {
		return t
	}
	return parseGraphTime(msg.LastModifiedDateTime)
}

func maxMessageModifiedTime(messages []ChatMessage) time.Time {
	var max time.Time
	for _, msg := range messages {
		t := parseGraphTime(msg.LastModifiedDateTime)
		if t.IsZero() {
			t = parseGraphTime(msg.CreatedDateTime)
		}
		if t.After(max) {
			max = t
		}
	}
	return max
}

func (b *Bridge) scheduleChatAfterPoll(ctx context.Context, chatID string, role inboundPollRole, running bool, poll teamstore.ChatPollState, activityAt time.Time) error {
	now := time.Now()
	update := chatPollScheduleUpdateAfterPoll(chatID, role, running, poll, activityAt, now)
	_, err := b.store.UpdateChatPollSchedule(ctx, update)
	return err
}

func (b *Bridge) recordChatPollSuccessAndSchedule(ctx context.Context, chatID string, role inboundPollRole, running bool, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int, continuationPath string, activityAt time.Time) (teamstore.ChatPollState, error) {
	return b.store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, chatID, lastModifiedCursor, seeded, windowFull, fetched, continuationPath, func(poll teamstore.ChatPollState) (teamstore.ChatPollScheduleUpdate, error) {
		return chatPollScheduleUpdateAfterPoll(chatID, role, running, poll, activityAt, time.Now()), nil
	})
}

func chatPollScheduleUpdateAfterPoll(chatID string, role inboundPollRole, running bool, poll teamstore.ChatPollState, activityAt time.Time, now time.Time) teamstore.ChatPollScheduleUpdate {
	poll.NextPollAt = time.Time{}
	decision := decideInboundPoll(inboundPollInput{
		ChatID:          chatID,
		Role:            role,
		Poll:            poll,
		HasPoll:         true,
		Running:         running,
		ForceActivityAt: activityAt,
		Now:             now,
	})
	next := decision.NextPollAt
	if decision.Interval > 0 {
		next = nextInboundPollAt(now, decision.Interval)
	}
	return teamstore.ChatPollScheduleUpdate{
		ChatID:            chatID,
		PollState:         decision.State,
		PreviousPollState: decision.PreviousState,
		NextPollAt:        next,
		LastActivityAt:    activityAt,
		ClearBlockedUntil: true,
		ResetFailures:     true,
	}
}

func (b *Bridge) effectiveMaxWorkChatPollsPerCycle() int {
	if b != nil && b.maxWorkChatPollsPerCycle > 0 {
		return b.maxWorkChatPollsPerCycle
	}
	return maxWorkChatPollsPerCycle
}

func (b *Bridge) effectiveMaxQueuedTurnStartsPerCycle() int {
	if b != nil {
		if b.maxQueuedTurnStartsPerCycle < 0 {
			return 0
		}
		if b.maxQueuedTurnStartsPerCycle > 0 {
			return b.maxQueuedTurnStartsPerCycle
		}
	}
	return maxQueuedTurnStartsPerCycle
}

func (b *Bridge) persistInboundPollDecision(ctx context.Context, decision inboundPollDecision) error {
	update, ok := inboundPollDecisionScheduleUpdate(decision)
	if !ok {
		return nil
	}
	_, err := b.store.UpdateChatPollSchedule(ctx, update)
	return err
}

func (b *Bridge) persistInboundPollScheduleUpdates(ctx context.Context, updates []teamstore.ChatPollScheduleUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	_, err := b.store.UpdateChatPollSchedules(ctx, updates)
	return err
}

func inboundPollDecisionScheduleUpdate(decision inboundPollDecision) (teamstore.ChatPollScheduleUpdate, bool) {
	if strings.TrimSpace(decision.ChatID) == "" || strings.TrimSpace(decision.State) == "" {
		return teamstore.ChatPollScheduleUpdate{}, false
	}
	return teamstore.ChatPollScheduleUpdate{
		ChatID:            decision.ChatID,
		PollState:         decision.State,
		PreviousPollState: decision.PreviousState,
		NextPollAt:        decision.NextPollAt,
		LastActivityAt:    decision.LastActivityAt,
		BlockedUntil:      decision.BlockedUntil,
		ClearBlockedUntil: decision.State != inboundPollStateBlocked,
	}, true
}

func inboundPollDecisionAlreadyPersisted(poll teamstore.ChatPollState, hasPoll bool, decision inboundPollDecision) bool {
	if !hasPoll {
		return false
	}
	if strings.TrimSpace(poll.ChatID) != strings.TrimSpace(decision.ChatID) {
		return false
	}
	if strings.TrimSpace(poll.PollState) != strings.TrimSpace(decision.State) {
		return false
	}
	if strings.TrimSpace(poll.PreviousPollState) != strings.TrimSpace(decision.PreviousState) {
		return false
	}
	if !poll.NextPollAt.Equal(decision.NextPollAt) {
		return false
	}
	if decision.LastActivityAt.After(poll.LastActivityAt) {
		return false
	}
	if strings.TrimSpace(decision.State) == inboundPollStateBlocked {
		return poll.BlockedUntil.Equal(decision.BlockedUntil)
	}
	return poll.BlockedUntil.IsZero()
}

func inboundPollBlockedUntil(poll teamstore.ChatPollState, err error, now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now()
	}
	var graphErr *GraphStatusError
	if errors.As(err, &graphErr) && graphErr.StatusCode == 429 {
		delay := graphErr.RetryAfter
		if delay <= 0 {
			delay = 30 * time.Second
		}
		return now.Add(delay)
	}
	failures := poll.FailureCount + 1
	if failures < 1 {
		failures = 1
	}
	if failures > 5 {
		failures = 5
	}
	delay := time.Duration(1<<uint(failures-1)) * 5 * time.Second
	if IsTemporaryAuthError(err) {
		delay = time.Duration(1<<uint(failures-1)) * 15 * time.Second
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		return now.Add(delay)
	}
	if delay > 2*time.Minute {
		delay = 2 * time.Minute
	}
	return now.Add(delay)
}

func (b *Bridge) shouldLogPollError(err error, now time.Time) bool {
	if err == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	text := strings.TrimSpace(err.Error())
	if text == "" {
		return false
	}
	minInterval := 5 * time.Minute
	if IsTemporaryAuthError(err) {
		minInterval = 30 * time.Minute
	}
	b.pollMu.Lock()
	defer b.pollMu.Unlock()
	if text == b.lastPollErrorLog && now.Sub(b.lastPollErrorLogAt) < minInterval {
		return false
	}
	b.lastPollErrorLog = text
	b.lastPollErrorLogAt = now
	return true
}

func (b *Bridge) notePollSuccess(now time.Time) {
	if b == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	b.pollMu.Lock()
	b.persistentPollFailureFirstAt = time.Time{}
	b.persistentPollFailureCount = 0
	b.pollMu.Unlock()
}

func (b *Bridge) notePollFailure(err error, now time.Time) error {
	if b == nil || err == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !isPersistentPollFailureCandidate(err) {
		b.notePollSuccess(now)
		return nil
	}
	b.pollMu.Lock()
	defer b.pollMu.Unlock()
	if b.persistentPollFailureFirstAt.IsZero() {
		b.persistentPollFailureFirstAt = now
	}
	b.persistentPollFailureCount++
	if b.persistentPollFailureCount < persistentPollFailureRestartMinCount {
		return nil
	}
	if now.Sub(b.persistentPollFailureFirstAt) < persistentPollFailureRestartAfter {
		return nil
	}
	return &PersistentPollFailureError{
		Since: b.persistentPollFailureFirstAt,
		Count: b.persistentPollFailureCount,
		Err:   err,
	}
}

func IsRecoverablePollFailure(err error) bool {
	return isPersistentPollFailureCandidate(err)
}

func isPersistentPollFailureCandidate(err error) bool {
	if err == nil {
		return false
	}
	if IsTemporaryAuthError(err) {
		return true
	}
	var graphErr *GraphStatusError
	if errors.As(err, &graphErr) {
		switch graphErr.StatusCode {
		case http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	lower := strings.ToLower(strings.TrimSpace(err.Error()))
	if lower == "" {
		return false
	}
	for _, token := range []string{
		"bad gateway",
		"client.timeout exceeded",
		"connection refused",
		"connection reset",
		"connection reset by peer",
		"gateway timeout",
		"i/o timeout",
		"network is unreachable",
		"no such host",
		"proxyconnect",
		"socks connect",
		"temporary failure",
		"tls handshake timeout",
		"unexpected eof",
	} {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func runningPollSessions(state teamstore.State) map[string]bool {
	running := make(map[string]bool)
	for _, turn := range state.Turns {
		switch turn.Status {
		case teamstore.TurnStatusQueued, teamstore.TurnStatusRunning:
			if strings.TrimSpace(turn.SessionID) != "" {
				running[turn.SessionID] = true
			}
		}
	}
	return running
}

func pollSessionTurnQueueStates(state teamstore.State) map[string]sessionTurnQueueState {
	out := make(map[string]sessionTurnQueueState)
	for _, turn := range state.Turns {
		sessionID := strings.TrimSpace(turn.SessionID)
		if sessionID == "" {
			continue
		}
		current := out[sessionID]
		switch turn.Status {
		case teamstore.TurnStatusRunning:
			current.Running = true
		case teamstore.TurnStatusQueued:
			current.Queued++
		}
		out[sessionID] = current
	}
	return out
}

func runningTurnSessions(state teamstore.State) map[string]bool {
	running := make(map[string]bool)
	for _, turn := range state.Turns {
		if turn.Status == teamstore.TurnStatusRunning && strings.TrimSpace(turn.SessionID) != "" {
			running[turn.SessionID] = true
		}
	}
	return running
}

func (b *Bridge) parkIdleWorkChat(ctx context.Context, session Session, decision inboundPollDecision) error {
	if err := b.persistInboundPollDecision(ctx, decision); err != nil {
		return err
	}
	if !decision.ShouldNotifyPark {
		return nil
	}
	resumeCommand := "r " + resumeKeyForSession(session)
	poll, _, err := b.store.ChatPoll(ctx, session.ChatID)
	if err != nil {
		return err
	}
	noticeID := parkNoticeOutboxID(session, poll.ParkedAt)
	alreadySent, err := b.parkNoticeAlreadySent(ctx, session, noticeID, resumeCommand)
	if err != nil {
		return err
	}
	if alreadySent {
		_, err = b.store.MarkChatPollParkNoticeSent(ctx, session.ChatID, time.Now())
		return err
	}
	body := renderTeamsFreezeNoticeHTML(
		b.reg.ControlChatURL,
		resumeCommand,
		"Your Codex work is safe. Paused after 48h idle.",
	)
	result, err := b.appendFreezeNoticeToLatestMessage(ctx, session, resumeCommand, decision.LastActivityAt, body)
	if err != nil {
		b.recordChatPollBackoffError(ctx, session.ChatID, poll, err)
		return err
	}
	if result.DeferForNewActivity {
		_, err = b.store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
			ChatID:            session.ChatID,
			PollState:         inboundPollStateCatchup,
			PreviousPollState: inboundPollStateParked,
			NextPollAt:        time.Now(),
			LastActivityAt:    result.ActivityAt,
			ClearBlockedUntil: true,
		})
		return err
	}
	if result.Appended {
		_, err = b.store.MarkChatPollParkNoticeSent(ctx, session.ChatID, time.Now())
		return err
	}
	err = b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          noticeID,
		SessionID:   session.ID,
		TeamsChatID: session.ChatID,
		Kind:        "freeze-notice",
		Body:        body,
	})
	if err != nil {
		return err
	}
	_, err = b.store.MarkChatPollParkNoticeSent(ctx, session.ChatID, time.Now())
	return err
}

type freezeNoticeAppendResult struct {
	Appended            bool
	DeferForNewActivity bool
	ActivityAt          time.Time
}

func (b *Bridge) appendFreezeNoticeToLatestMessage(ctx context.Context, session Session, resumeCommand string, lastKnownActivity time.Time, noticeHTML string) (freezeNoticeAppendResult, error) {
	if b == nil || b.graph == nil || b.readGraph == nil || strings.TrimSpace(session.ChatID) == "" || strings.TrimSpace(noticeHTML) == "" {
		return freezeNoticeAppendResult{}, nil
	}
	msg, ok, err := b.latestMessageForFreezeNotice(ctx, session)
	if err != nil {
		return freezeNoticeAppendResult{}, err
	}
	if !ok {
		return freezeNoticeAppendResult{}, nil
	}
	if graphMessageContainsFreezeNotice(msg, resumeCommand) {
		return freezeNoticeAppendResult{Appended: true}, nil
	}
	activity := chatMessageActivityTime(msg)
	if !activity.IsZero() && activity.After(lastKnownActivity) && !messageAuthoredByCurrentUser(msg, b.user) {
		return freezeNoticeAppendResult{DeferForNewActivity: true, ActivityAt: activity}, nil
	}
	if !editableFreezeNoticeTarget(msg, b.user) {
		return freezeNoticeAppendResult{}, nil
	}
	updated, ok := appendFreezeNoticeToMessageHTML(msg, resumeCommand, noticeHTML)
	if !ok {
		return freezeNoticeAppendResult{}, nil
	}
	if err := b.graph.UpdateChatMessageHTML(ctx, session.ChatID, msg.ID, updated); err != nil {
		if nonRetryableFreezeNoticePatchTargetError(err) {
			return freezeNoticeAppendResult{}, nil
		}
		return freezeNoticeAppendResult{}, err
	}
	return freezeNoticeAppendResult{Appended: true}, nil
}

func (b *Bridge) latestMessageForFreezeNotice(ctx context.Context, session Session) (ChatMessage, bool, error) {
	if b == nil || b.readGraph == nil || strings.TrimSpace(session.ChatID) == "" {
		return ChatMessage{}, false, nil
	}
	messages, err := b.listParkNoticeMessages(ctx, session.ChatID)
	if err != nil {
		return ChatMessage{}, false, err
	}
	sort.SliceStable(messages, func(i, j int) bool {
		left := messageSortTime(messages[i])
		right := messageSortTime(messages[j])
		if left.Equal(right) {
			return messages[i].ID > messages[j].ID
		}
		return left.After(right)
	})
	for _, msg := range messages {
		if !freezeNoticeVisibleMessage(msg) {
			continue
		}
		return msg, true, nil
	}
	return ChatMessage{}, false, nil
}

func (b *Bridge) listParkNoticeMessages(ctx context.Context, chatID string) ([]ChatMessage, error) {
	if b == nil || b.readGraph == nil || strings.TrimSpace(chatID) == "" {
		return nil, nil
	}
	now := time.Now()
	tops := b.parkNoticeLookupTopsForChat(chatID, now)
	var lastErr error
	for idx, top := range tops {
		messages, err := b.readGraph.ListMessagesExactTopWithoutRateLimitRetry(ctx, chatID, top)
		if err == nil {
			return messages, nil
		}
		lastErr = err
		if idx == len(tops)-1 || !retryParkNoticeLookupWithSmallerTop(err) {
			break
		}
		b.rememberParkNoticeLookupTop(chatID, tops[idx+1], err, now)
	}
	return nil, lastErr
}

func (b *Bridge) parkNoticeLookupTopsForChat(chatID string, now time.Time) []int {
	if now.IsZero() {
		now = time.Now()
	}
	chatID = strings.TrimSpace(chatID)
	if b == nil || chatID == "" {
		return parkNoticeGraphLookupTops
	}
	b.parkNoticeLookupMu.Lock()
	defer b.parkNoticeLookupMu.Unlock()
	pref, ok := b.parkNoticeLookupPreferences[chatID]
	if !ok || pref.Top <= 0 || !pref.Until.After(now) {
		return parkNoticeGraphLookupTops
	}
	for idx, top := range parkNoticeGraphLookupTops {
		if top == pref.Top {
			return parkNoticeGraphLookupTops[idx:]
		}
	}
	return parkNoticeGraphLookupTops
}

func (b *Bridge) rememberParkNoticeLookupTop(chatID string, top int, err error, now time.Time) {
	if b == nil || strings.TrimSpace(chatID) == "" || top <= 0 {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	ttl := parkNoticeGraphFallbackLookupTTL
	var graphErr *GraphStatusError
	if errors.As(err, &graphErr) && graphErr.RetryAfter > 0 {
		ttl = graphErr.RetryAfter
	}
	b.parkNoticeLookupMu.Lock()
	defer b.parkNoticeLookupMu.Unlock()
	if b.parkNoticeLookupPreferences == nil {
		b.parkNoticeLookupPreferences = make(map[string]parkNoticeLookupPreference)
	}
	b.parkNoticeLookupPreferences[strings.TrimSpace(chatID)] = parkNoticeLookupPreference{
		Top:   top,
		Until: now.Add(ttl),
	}
}

func retryParkNoticeLookupWithSmallerTop(err error) bool {
	var graphErr *GraphStatusError
	if !errors.As(err, &graphErr) {
		return false
	}
	switch graphErr.StatusCode {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func appendFreezeNoticeToMessageHTML(msg ChatMessage, resumeCommand string, noticeHTML string) (string, bool) {
	if msg.ID == "" || strings.TrimSpace(msg.DeletedDateTime) != "" {
		return "", false
	}
	if msg.MessageType != "" && msg.MessageType != "message" {
		return "", false
	}
	content := strings.TrimSpace(msg.Body.Content)
	if content == "" {
		return "", false
	}
	if graphMessageContainsFreezeNotice(msg, resumeCommand) {
		return "", false
	}
	if strings.TrimSpace(msg.Body.ContentType) != "" && !strings.EqualFold(strings.TrimSpace(msg.Body.ContentType), "html") {
		content = "<p>" + html.EscapeString(PlainTextFromTeamsHTML(content)) + "</p>"
	}
	return content + `<p>&nbsp;</p>` + noticeHTML, true
}

func freezeNoticeVisibleMessage(msg ChatMessage) bool {
	if msg.ID == "" || strings.TrimSpace(msg.DeletedDateTime) != "" {
		return false
	}
	if msg.MessageType != "" && msg.MessageType != "message" {
		return false
	}
	return strings.TrimSpace(msg.Body.Content) != ""
}

func editableFreezeNoticeTarget(msg ChatMessage, user User) bool {
	return freezeNoticeVisibleMessage(msg) && messageAuthoredByCurrentUser(msg, user)
}

func nonRetryableFreezeNoticePatchTargetError(err error) bool {
	var graphErr *GraphStatusError
	if !errors.As(err, &graphErr) {
		return false
	}
	switch graphErr.StatusCode {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed:
		return true
	default:
		return false
	}
}

func parkNoticeOutboxID(session Session, parkedAt time.Time) string {
	id := "outbox:park-notice:" + session.ID + ":" + resumeKeyForSession(session)
	if !parkedAt.IsZero() {
		id += ":" + parkedAt.UTC().Format("20060102T150405.000000000Z")
	}
	return id
}

func (b *Bridge) parkNoticeAlreadySent(ctx context.Context, session Session, noticeID string, resumeCommand string) (bool, error) {
	poll, ok, err := b.store.ChatPoll(ctx, session.ChatID)
	if err != nil {
		return false, err
	}
	if ok && !poll.ParkNoticeSentAt.IsZero() {
		return true, nil
	}
	parkedAt := poll.ParkedAt
	state, err := b.store.OutboxStateSnapshot(ctx)
	if err != nil {
		return false, err
	}
	if sentFreezeNoticeExists(state, session.ChatID, noticeID, resumeCommand, parkedAt) {
		return true, nil
	}
	if b.recentGraphFreezeNoticeExists(ctx, session.ChatID, resumeCommand, parkedAt) {
		return true, nil
	}
	return false, nil
}

func sentFreezeNoticeExists(state teamstore.State, chatID string, noticeID string, resumeCommand string, since time.Time) bool {
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID != chatID || msg.Status != teamstore.OutboxStatusSent {
			continue
		}
		if !since.IsZero() {
			sentAt := outboxMessageActivityTime(msg)
			if !sentAt.IsZero() && sentAt.Before(since) {
				continue
			}
		}
		if msg.ID == noticeID {
			return true
		}
		if msg.Kind != "freeze-notice" {
			continue
		}
		bodyText := PlainTextFromTeamsHTML(msg.Body)
		if strings.Contains(bodyText, "This chat is paused") && strings.Contains(bodyText, resumeCommand) {
			return true
		}
	}
	return false
}

func outboxMessageActivityTime(msg teamstore.OutboxMessage) time.Time {
	for _, value := range []time.Time{msg.SentAt, msg.UpdatedAt, msg.CreatedAt, msg.LastSendAttempt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func (b *Bridge) recentGraphFreezeNoticeExists(ctx context.Context, chatID string, resumeCommand string, since time.Time) bool {
	if b == nil || b.readGraph == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(resumeCommand) == "" {
		return false
	}
	messages, err := b.listParkNoticeMessages(ctx, chatID)
	if err != nil {
		return false
	}
	for _, msg := range messages {
		if !since.IsZero() {
			sentAt := chatMessageActivityTime(msg)
			if !sentAt.IsZero() && sentAt.Before(since) {
				continue
			}
		}
		if graphMessageContainsFreezeNotice(msg, resumeCommand) {
			return true
		}
	}
	return false
}

func graphMessageContainsFreezeNotice(msg ChatMessage, resumeCommand string) bool {
	text := PlainTextFromTeamsHTML(msg.Body.Content)
	return strings.Contains(text, "This chat is paused") && (strings.TrimSpace(resumeCommand) == "" || strings.Contains(text, resumeCommand))
}

func chatMessageActivityTime(msg ChatMessage) time.Time {
	var out time.Time
	for _, raw := range []string{msg.CreatedDateTime, msg.LastModifiedDateTime} {
		t := parseGraphTime(raw)
		if t.After(out) {
			out = t
		}
	}
	return out
}

func resumeKeyForSession(session Session) string {
	seed := strings.Join([]string{strings.TrimSpace(session.ID), strings.TrimSpace(session.ChatID)}, "\x00")
	if strings.Trim(seed, "\x00") == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])[:8]
}

func parseGraphTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return t
}

func (b *Bridge) shouldIgnoreMessage(ctx context.Context, chatID string, msg ChatMessage, role inboundPollRole, legacyGeneratedOutputFallback bool) (bool, error) {
	if msg.ID == "" || b.reg.HasSeen(chatID, msg.ID) || b.reg.HasSent(chatID, msg.ID) {
		return true, nil
	}
	if msg.MessageType != "" && msg.MessageType != "message" {
		return true, nil
	}
	if msg.From.User == nil {
		return true, nil
	}
	if strings.TrimSpace(msg.From.User.ID) == "" {
		return true, nil
	}
	if role != inboundPollRoleWork && !messageAuthoredByCurrentUser(msg, b.user) {
		return true, nil
	}
	if b.store != nil {
		lookup, err := b.store.MessageLookup(ctx, chatID, msg.ID)
		if err != nil {
			return false, err
		}
		if lookup.HasProvenance {
			switch lookup.Provenance.Origin {
			case teamstore.MessageOriginHelperOutbox:
				b.markRegistrySent(chatID, msg.ID)
				return true, nil
			case teamstore.MessageOriginUserInbound:
				return true, nil
			}
		}
		if lookup.HasInbound {
			return true, nil
		}
		if lookup.HasDeliveredOutbox {
			b.markRegistrySent(chatID, msg.ID)
			return true, nil
		}
		if messageAuthoredByCurrentUser(msg, b.user) {
			delivered, err := b.hasGlobalOutboundMessage(ctx, chatID, msg.ID)
			if err != nil {
				return false, err
			}
			if delivered {
				b.markRegistrySent(chatID, msg.ID)
				b.recordGlobalOutboundSuppressionProvenance(ctx, chatID, msg.ID)
				return true, nil
			}
		}
		if legacyGeneratedOutputFallback {
			delivered, err := b.hasDeliveredOutboxMessageByRenderedContent(ctx, chatID, msg)
			if err != nil {
				return false, err
			}
			if delivered {
				b.markRegistrySent(chatID, msg.ID)
				return true, nil
			}
		}
	}
	if legacyGeneratedOutputFallback && isHelperAttachmentEchoMessage(msg) {
		b.markRegistrySent(chatID, msg.ID)
		return true, nil
	}
	plainText := PlainTextFromTeamsHTML(msg.Body.Content)
	if messageAuthoredByCurrentUser(msg, b.user) && graphMessageContainsFreezeNotice(msg, "") {
		b.markRegistrySent(chatID, msg.ID)
		return true, nil
	}
	if messageAuthoredByCurrentUser(msg, b.user) && looksLikeRenderedOutboxOutputMessage(msg, plainText) {
		b.markRegistrySent(chatID, msg.ID)
		return true, nil
	}
	if role == inboundPollRoleControl && messageAuthoredByCurrentUser(msg, b.user) && looksLikeRenderedHelperLifecycleOutputMessage(msg, plainText) {
		b.markRegistrySent(chatID, msg.ID)
		return true, nil
	}
	if role == inboundPollRoleControl && messageAuthoredByCurrentUser(msg, b.user) && looksLikeRenderedHelperOutputMessage(msg, plainText) {
		b.markRegistrySent(chatID, msg.ID)
		return true, nil
	}
	if legacyGeneratedOutputFallback && role == inboundPollRoleControl && looksLikeRenderedHelperOutputPlainText(plainText) {
		b.markRegistrySent(chatID, msg.ID)
		return true, nil
	}
	if legacyGeneratedOutputFallback && (looksLikeRenderedHelperOutputMessage(msg, plainText) || looksLikeRenderedHelperGeneratedOutputPlainText(plainText) || looksLikeRenderedHelperOrCodexOutputPlainText(plainText) || looksLikeRenderedUserTranscriptEchoMessage(msg, plainText)) {
		b.markRegistrySent(chatID, msg.ID)
		return true, nil
	}
	return false, nil
}

func (b *Bridge) hasDeliveredOutboxMessageByRenderedContent(ctx context.Context, chatID string, msg ChatMessage) (bool, error) {
	incomingPlain := PlainTextFromTeamsHTML(msg.Body.Content)
	if !looksLikeRenderedOutboxPlainText(incomingPlain) {
		return false, nil
	}
	incomingKey := comparableTeamsPlainText(incomingPlain)
	if incomingKey == "" {
		return false, nil
	}
	state, err := b.store.OutboxStateSnapshot(ctx)
	if err != nil {
		return false, err
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.TeamsChatID != chatID || !outboxMayHaveReachedTeams(outbox) {
			continue
		}
		if !outboxRenderedPlainTextMatches(outbox, b.user, incomingKey) {
			continue
		}
		if outbox.TeamsMessageID == "" && msg.ID != "" {
			if _, err := b.store.MarkOutboxSent(ctx, outbox.ID, msg.ID); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, nil
}

func outboxRenderedPlainTextMatches(outbox teamstore.OutboxMessage, owner User, incomingKey string) bool {
	for _, rendered := range renderedOutboxHTMLVariants(outbox, owner) {
		if comparableTeamsPlainText(PlainTextFromTeamsHTML(rendered)) == incomingKey {
			return true
		}
	}
	return false
}

func renderedOutboxHTMLVariants(outbox teamstore.OutboxMessage, owner User) []string {
	var variants []string
	add := func(rendered string) {
		if strings.TrimSpace(rendered) == "" {
			return
		}
		for _, existing := range variants {
			if existing == rendered {
				return
			}
		}
		variants = append(variants, rendered)
	}
	add(renderOutboxHTML(outbox))
	if outbox.MentionOwner {
		rendered, _ := renderOutboxMentionHTML(outbox, owner)
		add(rendered)
	} else if user, ok := outboxMentionUser(outbox); ok {
		rendered, _ := renderOutboxUserMentionHTML(outbox, user)
		add(rendered)
	}
	return variants
}

func outboxMayHaveReachedTeams(outbox teamstore.OutboxMessage) bool {
	switch outbox.Status {
	case teamstore.OutboxStatusAccepted, teamstore.OutboxStatusSent:
		return true
	case teamstore.OutboxStatusSending:
		return !outbox.LastSendAttempt.IsZero()
	default:
		return false
	}
}

func looksLikeRenderedOutboxPlainText(text string) bool {
	text = strings.TrimSpace(text)
	for _, prefix := range []string{
		"🔧 Helper:",
		"Helper:",
		"🤖 ⏳ Codex status:",
		"🤖 ✅ Codex answer:",
		"🤖 🛠️ Codex command:",
		"🤖 Codex progress:",
		"💻 Code:",
		"🧑‍💻 User:",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func looksLikeRenderedOutboxOutputMessage(msg ChatMessage, text string) bool {
	if !looksLikeRenderedOutboxPlainText(text) {
		return false
	}
	for _, label := range []string{
		"🤖 ⏳ Codex status",
		"🤖 ✅ Codex answer",
		"🤖 🛠️ Codex command",
		"🤖 Codex progress",
		"💻 Code",
		"🧑‍💻 User",
	} {
		if teamsHTMLFirstTextIsStrongLabel(msg.Body.Content, label) {
			return true
		}
	}
	return false
}

func looksLikeRenderedHelperLifecycleOutputMessage(msg ChatMessage, text string) bool {
	if !looksLikeRenderedHelperOutputMessage(msg, text) {
		return false
	}
	body, ok := renderedHelperOutputBodyPlainText(text)
	return ok && looksLikeRenderedHelperLifecycleOutputPlainText(body)
}

func looksLikeRenderedHelperOutputPlainText(text string) bool {
	text = strings.TrimSpace(text)
	return strings.HasPrefix(text, "🔧 Helper:") || strings.HasPrefix(text, "Helper:")
}

func looksLikeRenderedHelperOutputMessage(msg ChatMessage, text string) bool {
	if !looksLikeRenderedHelperOutputPlainText(text) {
		return false
	}
	return teamsHTMLFirstTextIsStrongLabel(msg.Body.Content, "🔧 Helper") ||
		teamsHTMLFirstTextIsStrongLabel(msg.Body.Content, "Helper")
}

func looksLikeRenderedHelperGeneratedOutputPlainText(text string) bool {
	body, ok := renderedHelperOutputBodyPlainText(text)
	if !ok {
		return false
	}
	return looksLikeRenderedHelperOrCodexOutputPlainText(body) || looksLikeRenderedHelperLifecycleOutputPlainText(body)
}

func renderedHelperOutputBodyPlainText(text string) (string, bool) {
	text = strings.TrimSpace(text)
	for _, label := range []string{"🔧 Helper:", "Helper:"} {
		if text == label {
			return "", true
		}
		if strings.HasPrefix(text, label) {
			return strings.TrimSpace(strings.TrimPrefix(text, label)), true
		}
	}
	return "", false
}

func looksLikeRenderedHelperOrCodexOutputPlainText(text string) bool {
	text = strings.TrimSpace(text)
	for _, prefix := range []string{
		"🤖 ⏳ Codex status:",
		"🤖 ✅ Codex answer:",
		"🤖 🛠️ Codex command:",
		"🤖 Codex progress:",
		"💻 Code:",
		"cancel requested for running turn:",
		"Prompt being canceled:",
		"No other prompts are queued.",
		"Codex request canceled.",
		"Teams helper safety:",
		"Teams beacon reconcile error:",
		"Teams beacon lease maintenance error:",
		"Teams beacon lease renewed:",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func looksLikeRenderedHelperLifecycleOutputPlainText(text string) bool {
	text = strings.TrimSpace(text)
	for _, prefix := range []string{
		"⏳ Codex is working. Request accepted.",
		"⏳ Codex received your question. Request accepted.",
		"❓ Codex received your control-chat question.",
		"⚠️ Your request is queued.",
		"▶️ Codex is starting this queued request.",
		"💬 Work chat is ready.",
		"✅ Codex finished responding.",
		"🔁 Helper reload started",
		"🔄 Helper restart scheduled",
		"⬇️ Helper update scheduled",
		"✅ Helper reload completed",
		"✅ Helper restart completed",
		"✅ Helper update completed",
		"⚠️ Helper reload failed",
		"⚠️ Helper restart failed",
		"⚠️ Helper update activation failed",
		"⚠️ Helper update activation needs attention",
		"⚠️ Helper restart is not available",
		"⚠️ Helper reload is not available",
		"⚠️ Helper restart was not started",
		"⏳ Codex work is still active.",
		"⏳ Helper upgrade recovery is waiting",
		"⏳ Helper reload recovery is waiting",
		"⏳ Helper reload is already in progress.",
		"usage: `helper cancel last`",
		"no running or queued turn is available to cancel in this session.",
		"no running or queued turn is available to cancel in this control chat.",
		"turn canceled:",
		"Codex request canceled.",
		"cancel all requested for this Work chat.",
		"cancel all requested for this Control chat.",
		"cancel all could not cancel every running request",
		"This Codex request is running, but this helper process does not own",
		"turn not found in this session:",
		"turn not found in this control chat:",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func looksLikeRenderedUserTranscriptEchoMessage(msg ChatMessage, text string) bool {
	if !looksLikeRenderedUserTranscriptPlainText(text) {
		return false
	}
	return teamsHTMLFirstTextIsStrongLabel(msg.Body.Content, "🧑‍💻 User")
}

func looksLikeRenderedUserTranscriptPlainText(text string) bool {
	text = strings.TrimSpace(text)
	if text == "🧑‍💻 User:" || strings.HasPrefix(text, "🧑‍💻 User:\n") || strings.HasPrefix(text, "🧑‍💻 User: ") {
		return true
	}
	firstLine := text
	if idx := strings.IndexByte(firstLine, '\n'); idx >= 0 {
		firstLine = firstLine[:idx]
	}
	return strings.HasPrefix(firstLine, "🧑‍💻 User [part ") && strings.HasSuffix(strings.TrimSpace(firstLine), ":")
}

func teamsHTMLFirstTextIsStrongLabel(content string, label string) bool {
	label = strings.TrimSpace(label)
	if label == "" || strings.TrimSpace(content) == "" {
		return false
	}
	root, err := xhtml.Parse(strings.NewReader("<html><body>" + content + "</body></html>"))
	if err != nil {
		return false
	}
	first := firstNonEmptyHTMLTextNode(root)
	if first == nil || !htmlTextNodeHasStrongAncestor(first) {
		return false
	}
	return renderedTeamsLabelMatches(strings.TrimSpace(first.Data), label)
}

func firstNonEmptyHTMLTextNode(root *xhtml.Node) *xhtml.Node {
	if root == nil {
		return nil
	}
	if root.Type == xhtml.TextNode && strings.TrimSpace(root.Data) != "" {
		return root
	}
	for child := root.FirstChild; child != nil; child = child.NextSibling {
		if found := firstNonEmptyHTMLTextNode(child); found != nil {
			return found
		}
	}
	return nil
}

func htmlTextNodeHasStrongAncestor(node *xhtml.Node) bool {
	for parent := node.Parent; parent != nil; parent = parent.Parent {
		if parent.Type == xhtml.ElementNode && (strings.EqualFold(parent.Data, "strong") || strings.EqualFold(parent.Data, "b")) {
			return true
		}
	}
	return false
}

func renderedTeamsLabelMatches(text string, label string) bool {
	text = strings.TrimSpace(text)
	if text == label+":" {
		return true
	}
	return strings.HasPrefix(text, label+" [part ") && strings.HasSuffix(text, ":")
}

func comparableTeamsPlainText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func (b *Bridge) annotateIncomingUserMessage(ctx context.Context, chatID string, msg ChatMessage) {
	if !b.annotateUserMessages {
		return
	}
	if hasSupportedTeamsMediaCardAttachment(msg.Attachments) {
		return
	}
	if msg.ID == "" || hasUserAnnotationPrefix(msg.Body.Content) || isPromptlessTeamsAttachmentPlaceholderMessage(msg) || isHelperAttachmentEchoMessage(msg) {
		return
	}
	if strings.TrimSpace(msg.Body.Content) == "" && !messageHasTeamsAttachmentContext(msg) {
		return
	}
	if messageHasTeamsAttachmentContext(msg) {
		b.annotateIncomingUserMessageWithAttachmentContext(ctx, chatID, msg)
		return
	}
	if messageAuthoredByCurrentUser(msg, b.user) && !b.annotationDisabled && b.graph != nil {
		annotated, ok := userAnnotatedMessageHTML(msg, b.user)
		if !ok {
			return
		}
		if err := b.graph.UpdateChatMessageHTML(ctx, chatID, msg.ID, annotated); err == nil {
			return
		} else {
			if shouldDisableUserMessageAnnotation(err) {
				b.annotationDisabled = true
			}
			if !b.annotationWarned && b.out != nil {
				_, _ = fmt.Fprintf(b.out, "Teams user message annotation disabled or unavailable: %v\n", err)
				b.annotationWarned = true
			}
		}
	}
	if err := b.queueIncomingUserMarkerMirror(ctx, chatID, msg); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams user message marker fallback failed: %v\n", err)
	}
}

func (b *Bridge) annotateIncomingUserMessageWithASRTranscripts(ctx context.Context, chatID string, msg ChatMessage, transcripts []ASRTranscript) {
	if b == nil || !b.annotateUserMessages || b.graph == nil || b.annotationDisabled || len(transcripts) == 0 {
		return
	}
	if strings.TrimSpace(chatID) == "" || strings.TrimSpace(msg.ID) == "" || !messageAuthoredByCurrentUser(msg, b.user) {
		return
	}
	annotated, ok := userAnnotatedASRTranscriptMessageHTML(msg, transcripts)
	if !ok {
		return
	}
	if err := b.graph.UpdateChatMessageHTMLPreservingAttachments(ctx, chatID, msg, annotated); err != nil {
		if shouldDisableAttachmentPreservingUserMessageAnnotation(err) {
			b.annotationDisabled = true
		}
		if !b.annotationWarned && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams user ASR transcript annotation disabled or unavailable: %v\n", err)
			b.annotationWarned = true
		}
	}
}

func (b *Bridge) annotateIncomingUserMessageWithAttachmentContext(ctx context.Context, chatID string, msg ChatMessage) {
	if b == nil || b.graph == nil || b.annotationDisabled || !messageAuthoredByCurrentUser(msg, b.user) {
		return
	}
	annotated, ok := userAnnotatedAttachmentMessageHTML(msg, b.user)
	if !ok {
		return
	}
	if err := b.graph.UpdateChatMessageHTMLPreservingAttachments(ctx, chatID, msg, annotated); err != nil {
		if shouldDisableAttachmentPreservingUserMessageAnnotation(err) {
			b.annotationDisabled = true
		}
		if !b.annotationWarned && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams user message attachment-preserving annotation disabled or unavailable: %v\n", err)
			b.annotationWarned = true
		}
	}
}

func (b *Bridge) queueIncomingUserMarkerMirror(ctx context.Context, chatID string, msg ChatMessage) error {
	if b == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(msg.ID) == "" {
		return nil
	}
	body := strings.TrimSpace(promptTextFromTeamsMessageHTML(msg.Body.Content))
	if body == "" || hasUserAnnotationPrefix(msg.Body.Content) {
		return nil
	}
	sessionID := ""
	if session := b.reg.SessionByChatID(chatID); session != nil {
		sessionID = session.ID
	}
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:             "outbox:user-marker:" + shortStableID(chatID+":"+msg.ID),
		SessionID:      sessionID,
		TeamsChatID:    chatID,
		Kind:           "user",
		Body:           body,
		SourceTextHash: normalizedTextHash(body),
	})
}

func messageHasTeamsAttachmentContext(msg ChatMessage) bool {
	return len(msg.Attachments) > 0 || len(HostedContentIDsFromHTML(msg.Body.Content)) > 0 || hasTeamsAttachmentPlaceholder(msg.Body.Content)
}

func messageAttachmentContextLosslessPatchable(msg ChatMessage) bool {
	if len(msg.Attachments) == 0 || len(HostedContentIDsFromHTML(msg.Body.Content)) > 0 || hasAdaptiveCardAttachment(msg.Attachments) {
		return false
	}
	ids := teamsAttachmentPlaceholderIDSet(msg.Body.Content)
	if len(ids) == 0 {
		return false
	}
	if len(ids) != len(msg.Attachments) {
		return false
	}
	for _, attachment := range msg.Attachments {
		id := strings.TrimSpace(attachment.ID)
		if id == "" || !ids[id] || strings.TrimSpace(attachment.ContentType) == "" {
			return false
		}
	}
	return true
}

func teamsAttachmentPlaceholderIDSet(content string) map[string]bool {
	ids := make(map[string]bool)
	root, err := xhtml.Parse(strings.NewReader("<div>" + content + "</div>"))
	if err != nil {
		return ids
	}
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n == nil {
			return
		}
		if n.Type == xhtml.ElementNode && strings.EqualFold(n.Data, "attachment") {
			for _, attr := range n.Attr {
				if strings.EqualFold(attr.Key, "id") && strings.TrimSpace(attr.Val) != "" {
					ids[strings.TrimSpace(attr.Val)] = true
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return ids
}

func shouldDisableAttachmentPreservingUserMessageAnnotation(err error) bool {
	var graphErr *GraphStatusError
	if !errors.As(err, &graphErr) {
		return false
	}
	switch graphErr.StatusCode {
	case 401, 403, 405:
		return true
	default:
		return false
	}
}

func shouldDisableUserMessageAnnotation(err error) bool {
	var graphErr *GraphStatusError
	if !errors.As(err, &graphErr) {
		return false
	}
	switch graphErr.StatusCode {
	case 400, 401, 403, 404, 405:
		return true
	default:
		return false
	}
}

func userAnnotatedMessageHTML(msg ChatMessage, _ User) (string, bool) {
	content := strings.TrimSpace(msg.Body.Content)
	if content == "" || hasUserAnnotationPrefix(content) || isPromptlessTeamsAttachmentPlaceholderMessage(msg) || isHelperAttachmentEchoMessage(msg) || messageHasTeamsAttachmentContext(msg) {
		return "", false
	}
	label := incomingUserLabel()
	if strings.TrimSpace(msg.Body.ContentType) != "" && !strings.EqualFold(strings.TrimSpace(msg.Body.ContentType), "html") {
		content = "<p>" + html.EscapeString(PlainTextFromTeamsHTML(content)) + "</p>"
	}
	return `<p><strong>` + html.EscapeString(label) + `:</strong></p>` + content, true
}

func userAnnotatedAttachmentMessageHTML(msg ChatMessage, _ User) (string, bool) {
	content := strings.TrimSpace(msg.Body.Content)
	if content == "" || hasUserAnnotationPrefix(content) || isPromptlessTeamsAttachmentPlaceholderMessage(msg) || isHelperAttachmentEchoMessage(msg) || !messageAttachmentContextLosslessPatchable(msg) {
		return "", false
	}
	if strings.TrimSpace(msg.Body.ContentType) != "" && !strings.EqualFold(strings.TrimSpace(msg.Body.ContentType), "html") {
		return "", false
	}
	label := incomingUserLabel()
	return `<p><strong>` + html.EscapeString(label) + `:</strong></p>` + content, true
}

func userAnnotatedASRTranscriptMessageHTML(msg ChatMessage, transcripts []ASRTranscript) (string, bool) {
	if len(transcripts) == 0 || strings.TrimSpace(msg.ID) == "" || isHelperAttachmentEchoMessage(msg) {
		return "", false
	}
	content := strings.TrimSpace(msg.Body.Content)
	if content != "" && strings.TrimSpace(msg.Body.ContentType) != "" && !strings.EqualFold(strings.TrimSpace(msg.Body.ContentType), "html") {
		content = "<p>" + html.EscapeString(PlainTextFromTeamsHTML(content)) + "</p>"
	}
	if strings.Contains(PlainTextFromTeamsHTML(content), teamsASRTranscriptAnnotationLabel) {
		return "", false
	}
	var b strings.Builder
	if hasUserAnnotationPrefix(content) {
		b.WriteString(content)
	} else {
		b.WriteString(`<p><strong>`)
		b.WriteString(html.EscapeString(incomingUserLabel()))
		b.WriteString(`:</strong></p>`)
		if content != "" {
			b.WriteString(content)
		}
	}
	for _, attachment := range msg.Attachments {
		if !isSupportedTeamsMediaCardAttachment(attachment) {
			continue
		}
		id := strings.TrimSpace(attachment.ID)
		if id == "" || teamsAttachmentPlaceholderIDSet(b.String())[id] {
			continue
		}
		b.WriteString(`<attachment id="`)
		b.WriteString(html.EscapeString(id))
		b.WriteString(`"></attachment>`)
	}
	b.WriteString(asrTranscriptAnnotationHTML(transcripts))
	return b.String(), true
}

func messageAuthoredByCurrentUser(msg ChatMessage, current User) bool {
	senderID := strings.TrimSpace(chatMessageAuthorUserID(msg))
	currentID := strings.TrimSpace(current.ID)
	return senderID != "" && currentID != "" && strings.EqualFold(senderID, currentID)
}

func chatMessageExternalAuthor(msg ChatMessage, current User) (User, bool) {
	senderID := strings.TrimSpace(chatMessageAuthorUserID(msg))
	currentID := strings.TrimSpace(current.ID)
	if senderID == "" || currentID == "" || strings.EqualFold(senderID, currentID) {
		return User{}, false
	}
	return User{
		ID:          senderID,
		DisplayName: chatMessageAuthorDisplayName(msg),
	}, true
}

func inboundExternalAuthor(inbound teamstore.InboundEvent, current User) (User, bool) {
	senderID := strings.TrimSpace(inbound.AuthorUserID)
	currentID := strings.TrimSpace(current.ID)
	if senderID == "" || currentID == "" || strings.EqualFold(senderID, currentID) {
		return User{}, false
	}
	return User{
		ID:          senderID,
		DisplayName: strings.TrimSpace(inbound.AuthorName),
	}, true
}

func chatMessageAuthorUserID(msg ChatMessage) string {
	if msg.From.User == nil {
		return ""
	}
	return strings.TrimSpace(msg.From.User.ID)
}

func chatMessageAuthorDisplayName(msg ChatMessage) string {
	if msg.From.User == nil {
		return ""
	}
	return strings.TrimSpace(msg.From.User.DisplayName)
}

func applyOutboxMentionUser(msg *teamstore.OutboxMessage, user User) {
	if msg == nil || strings.TrimSpace(user.ID) == "" {
		return
	}
	msg.MentionUserID = strings.TrimSpace(user.ID)
	msg.MentionUserName = strings.TrimSpace(firstNonEmptyString(user.DisplayName, user.UserPrincipalName))
}

func hasUserAnnotationPrefix(content string) bool {
	_, ok := splitLeadingUserAnnotationPrefix(PlainTextFromTeamsHTML(content))
	return ok
}

func promptTextFromTeamsMessageHTML(content string) string {
	return stripASRTranscriptAnnotation(stripUserAnnotationPrefix(PlainTextFromTeamsHTML(content)))
}

func commandRouteTextFromTeamsMessage(msg ChatMessage, fallbackText string) string {
	if strings.TrimSpace(msg.Body.Content) != "" {
		plainText := CommandRoutePlainTextFromTeamsHTML(msg.Body.Content)
		if looksLikeRenderedOutboxOutputMessage(msg, plainText) {
			return ""
		}
		return commandRouteTextFromPlainText(plainText)
	}
	return commandRouteTextFromPlainText(fallbackText)
}

func commandRouteTextFromPlainText(text string) string {
	text = stripASRTranscriptAnnotation(stripUserAnnotationPrefix(strings.TrimSpace(text)))
	if text == "" {
		return ""
	}
	if looksLikeRenderedOutboxPlainText(text) {
		return ""
	}
	return text
}

func promptTextFromTeamsMessageOrFallback(msg ChatMessage, fallbackText string) (string, bool) {
	if prompt := strings.TrimSpace(promptTextFromTeamsMessageHTML(msg.Body.Content)); prompt != "" && !IsHelperText(prompt) {
		return prompt, true
	}
	fallback := strings.TrimSpace(fallbackText)
	if fallback == "" || IsHelperText(fallback) {
		return "", false
	}
	return fallback, true
}

func stripUserAnnotationPrefix(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	if rest, ok := splitLeadingUserAnnotationPrefix(text); ok {
		return strings.TrimSpace(rest)
	}
	return text
}

func splitLeadingUserAnnotationPrefix(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", false
	}
	label := incomingUserLabel()
	if text == label+":" {
		return "", true
	}
	if strings.HasPrefix(text, label+":") {
		return strings.TrimSpace(strings.TrimPrefix(text, label+":")), true
	}
	firstLine, rest, hasRest := strings.Cut(text, "\n")
	firstLine = strings.TrimSpace(firstLine)
	if !strings.HasPrefix(firstLine, label+" [part ") {
		return "", false
	}
	idx := strings.Index(firstLine, "]:")
	if idx < 0 {
		return "", false
	}
	inline := strings.TrimSpace(firstLine[idx+2:])
	if inline != "" && hasRest {
		return inline + "\n" + rest, true
	}
	if inline != "" {
		return inline, true
	}
	if hasRest {
		return rest, true
	}
	return "", true
}

func incomingUserLabel() string {
	return "🧑‍💻 User"
}

func isPromptlessTeamsAttachmentPlaceholderMessage(msg ChatMessage) bool {
	if strings.TrimSpace(promptTextFromTeamsMessageHTML(msg.Body.Content)) != "" {
		return false
	}
	if len(HostedContentIDsFromHTML(msg.Body.Content)) > 0 {
		return false
	}
	if hasAdaptiveCardAttachment(msg.Attachments) {
		return true
	}
	return len(msg.Attachments) == 0 && hasTeamsAttachmentPlaceholder(msg.Body.Content)
}

func hasAdaptiveCardAttachment(attachments []MessageAttachment) bool {
	for _, attachment := range attachments {
		if strings.EqualFold(strings.TrimSpace(attachment.ContentType), "application/vnd.microsoft.card.adaptive") {
			return true
		}
	}
	return false
}

func hasTeamsAttachmentPlaceholder(content string) bool {
	return strings.Contains(strings.ToLower(content), "<attachment")
}

func isHelperAttachmentEchoMessage(msg ChatMessage) bool {
	if len(msg.Attachments) == 0 && len(HostedContentIDsFromHTML(msg.Body.Content)) == 0 && !hasTeamsAttachmentPlaceholder(msg.Body.Content) {
		return false
	}
	text := strings.TrimSpace(promptTextFromTeamsMessageHTML(msg.Body.Content))
	if text == "" {
		return false
	}
	prefix, rest, ok := strings.Cut(text, ":")
	if !ok || !strings.EqualFold(strings.TrimSpace(prefix), "codex") {
		return false
	}
	rest = strings.ToLower(strings.TrimSpace(rest))
	for _, marker := range []string{"file attached", "artifact attached"} {
		if rest == marker || strings.HasPrefix(rest, marker+":") {
			return true
		}
	}
	return false
}

func (b *Bridge) handleControlMessage(ctx context.Context, msg ChatMessage, text string) error {
	if msg.Body.Content == "" && strings.TrimSpace(text) != "" {
		msg.Body.Content = text
	}
	routeText := commandRouteTextFromTeamsMessage(msg, text)
	if isModelProfileKeyIntakeControlRoute(routeText) {
		return b.handleModelProfileKeyIntakeControlMessage(ctx, msg, routeText)
	}
	if message := modelAPIKeyPreflightMessage(routeText); message != "" {
		return b.sendControl(ctx, message)
	}
	b.recordControlChatUserMessage(ctx, msg, text)
	parsed := ParseDashboardCommand(ChatScopeControl, routeText)
	allowMessageReferences := controlInputAllowsMessageReferences(parsed, routeText)
	if unsupported := unsupportedControlAttachments(msg, allowMessageReferences); len(unsupported) > 0 {
		if !allowMessageReferences && hasMessageReferenceAttachment(unsupported) {
			return b.sendControl(ctx, UnsupportedExplicitControlAttachmentMessage(unsupported))
		}
		return b.sendControl(ctx, UnsupportedControlAttachmentMessage(unsupported))
	}
	if controlCommandConsumesDashboardView(parsed) {
		defer func() { _ = b.clearControlDashboardView(context.Background()) }()
	}
	if parsed.HelperCommand {
		switch parsed.Name {
		case DashboardCommandNew:
			return b.createSession(ctx, msg, parsed.Argument)
		case DashboardCommandAsk:
			arg := strings.TrimSpace(parsed.Argument)
			if arg == "" {
				return b.sendControl(ctx, "usage: `ask <question>`")
			}
			askMsg := msg
			askMsg.Body.ContentType = "html"
			askMsg.Body.Content = html.EscapeString(arg)
			return b.runControlFallback(ctx, askMsg, arg)
		case DashboardCommandWorkspaces:
			message, err := b.formatWorkspaceDashboard(ctx)
			if err != nil {
				_ = b.clearControlDashboardView(context.Background())
				return b.sendControl(ctx, "workspace discovery failed: "+err.Error())
			}
			return b.sendControl(ctx, message)
		case DashboardCommandWorkspace:
			message, err := b.formatWorkspaceSessionsDashboard(ctx, parsed.Target)
			if err != nil {
				_ = b.clearControlDashboardView(context.Background())
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandSessions:
			message, err := b.formatWorkspaceSessionsDashboard(ctx, DashboardCommandTarget{})
			if err != nil {
				_ = b.clearControlDashboardView(context.Background())
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandOpen:
			message, err := b.formatOpenControlTarget(ctx, parsed.Target)
			if err != nil {
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandResume:
			message, err := b.resumeParkedWorkChat(ctx, parsed.Argument)
			if err != nil {
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandStatus:
			return b.sendControl(ctx, b.formatSessionList())
		case DashboardCommandCancel:
			return b.cancelControlFallbackCommand(ctx, strings.TrimSpace(parsed.Argument))
		case DashboardCommandSkills:
			return b.handleSkillsCommandFromMessage(ctx, b.reg.ControlChatID, msg, parsed.Argument)
		case DashboardCommandBeacon:
			return b.handleBeaconControlCommand(ctx, msg, parsed.Argument)
		case DashboardCommandModel:
			message, err := b.handleModelControlCommand(ctx, msg, parsed.Argument)
			if err != nil {
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandRestart:
			return b.restartHelperFromControl(ctx, msg, parsed.Argument)
		case DashboardCommandReload:
			return b.reloadHelperFromControl(ctx, msg, parsed.Argument)
		case DashboardCommandUpdate:
			return b.updateHelperFromControl(ctx, msg, parsed.Argument)
		case DashboardCommandWebhook:
			return b.workflowWebhookFromControl(ctx, msg, parsed.Argument)
		case DashboardCommandASR:
			return b.asrFromControl(ctx, msg, parsed.Argument)
		case DashboardCommandSelect:
			message, err := b.resolveControlSelection(ctx, parsed.Target)
			if err != nil {
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandPublish:
			if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
				return err
			} else if blocked {
				if serviceControlDefersInput(control) {
					deferredMsg := msg
					if resolved, err := b.resolvePublishTargetSessionID(ctx, parsed.Target); err == nil && resolved != "" {
						deferredMsg.Body.ContentType = "html"
						deferredMsg.Body.Content = html.EscapeString("continue " + resolved)
					}
					inbound, _, err := b.persistControlInboundWithStatus(ctx, deferredMsg, teamstore.InboundStatusDeferred, "teams_control_publish")
					if err != nil {
						return err
					}
					return b.sendDeferredServiceControlNotice(ctx, b.reg.ControlChatID, inbound, control)
				}
				return b.sendControl(ctx, serviceControlBlockedMessage(control, "publishing existing sessions"))
			}
			message, err := b.publishCodexSessionWithProgress(ctx, parsed.Target, b.sendControl)
			if err != nil {
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandMkdir:
			return b.createWorkspaceDirectory(ctx, parsed.Argument)
		case DashboardCommandRename:
			if hostname, ok := parseRenameHostnameArgument(parsed.Argument); ok {
				return b.renameMachineHostname(ctx, hostname, b.reg.ControlChatID)
			}
			return b.renameControlChat(ctx, parsed.Argument)
		case DashboardCommandDetails:
			message, err := b.formatDetailsControlTarget(ctx, parsed.Target)
			if err != nil {
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandHelp:
			if isAdvancedHelpArg(parsed.Argument) {
				return b.sendControl(ctx, controlAdvancedHelpText())
			}
			return b.sendControl(ctx, controlHelpText())
		default:
			return b.sendControl(ctx, unknownControlCommandMessage(routeText))
		}
	}
	if looksLikeControlPath(routeText) {
		return b.sendControl(ctx, controlPathHintMessage(routeText))
	}
	return b.runControlFallback(ctx, msg, text)
}

func controlInputAllowsMessageReferences(parsed ParsedDashboardCommand, routeText string) bool {
	if parsed.HelperCommand {
		return parsed.Name == DashboardCommandAsk
	}
	return !looksLikeControlPath(routeText)
}

func modelAPIKeyPreflightMessage(text string) string {
	if !containsRawModelAPIKey(text) {
		return ""
	}
	return "I cannot accept raw API keys in normal Teams messages. Use `model setup <model>` to start the explicit one-time Teams key intake flow, or configure a model from a local terminal with `cxp model setup <model> --api-key-stdin`."
}

func containsRawModelAPIKey(text string) bool {
	fields := strings.Fields(strings.TrimSpace(text))
	for i, field := range fields {
		normalized := strings.ToLower(strings.Trim(field, "`'\""))
		normalized = strings.ReplaceAll(normalized, "_", "-")
		if strings.HasPrefix(normalized, "--api-key-env") || strings.HasPrefix(normalized, "api-key-env") ||
			strings.HasPrefix(normalized, "--api-key-stdin") || strings.HasPrefix(normalized, "api-key-stdin") {
			continue
		}
		switch {
		case strings.HasPrefix(normalized, "--api-key="), strings.HasPrefix(normalized, "api-key="):
			return true
		case normalized == "--api-key", normalized == "api-key":
			if i+1 < len(fields) && strings.TrimSpace(fields[i+1]) != "" {
				return true
			}
		case strings.EqualFold(strings.TrimSuffix(normalized, ":"), "authorization") && i+1 < len(fields) && strings.EqualFold(strings.Trim(fields[i+1], "`'\""), "bearer"):
			return true
		case looksLikeRawModelAPIKeyToken(strings.Trim(field, "`'\"")):
			return true
		}
	}
	return false
}

func looksLikeRawModelAPIKeyToken(token string) bool {
	token = strings.TrimSpace(strings.Trim(token, "`'\""))
	if len(token) < 16 {
		return false
	}
	lower := strings.ToLower(token)
	if !strings.HasPrefix(lower, "sk-") && !strings.HasPrefix(lower, "sk_") {
		return false
	}
	for _, r := range token {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

func unsupportedControlAttachments(msg ChatMessage, allowMessageReferences bool) []MessageAttachment {
	var unsupported []MessageAttachment
	for _, attachment := range msg.Attachments {
		if allowMessageReferences && isMessageReferenceAttachment(attachment) {
			continue
		}
		if allowMessageReferences && isSupportedTeamsMediaCardAttachment(attachment) {
			continue
		}
		unsupported = append(unsupported, attachment)
	}
	for _, id := range HostedContentIDsFromHTML(msg.Body.Content) {
		unsupported = append(unsupported, MessageAttachment{
			ID:          id,
			ContentType: "Teams-hosted inline content",
		})
	}
	if len(msg.Attachments) == 0 && len(unsupported) == 0 && hasTeamsAttachmentPlaceholder(msg.Body.Content) {
		unsupported = append(unsupported, MessageAttachment{ContentType: "Teams attachment placeholder"})
	}
	return unsupported
}

func (b *Bridge) controlFallbackPromptWithMessageReferences(ctx context.Context, msg ChatMessage, text string) (string, string, error) {
	chatID := strings.TrimSpace(msg.ChatID)
	if chatID == "" {
		chatID = strings.TrimSpace(b.reg.ControlChatID)
	}
	referencedMessages, warning, err := b.readMessageReferenceAttachments(ctx, chatID, msg)
	if err != nil {
		return "", "", err
	}
	if warning != "" {
		return "", warning, nil
	}
	return PromptWithReferencedMessages(text, referencedMessages), "", nil
}

func controlFallbackMessageWithPromptBody(msg ChatMessage, prompt string, originalText string) ChatMessage {
	if strings.TrimSpace(prompt) == "" || strings.TrimSpace(prompt) == strings.TrimSpace(originalText) {
		return msg
	}
	msg.Body.ContentType = "html"
	msg.Body.Content = html.EscapeString(prompt)
	return msg
}

func controlCommandConsumesDashboardView(parsed ParsedDashboardCommand) bool {
	if !parsed.HelperCommand {
		return true
	}
	switch parsed.Name {
	case DashboardCommandWorkspaces, DashboardCommandWorkspace, DashboardCommandSessions, DashboardCommandSelect:
		return false
	default:
		return true
	}
}

func (b *Bridge) runControlFallback(ctx context.Context, msg ChatMessage, text string) error {
	hasMedia := hasSupportedTeamsMediaCardAttachment(msg.Attachments)
	if strings.TrimSpace(text) == "" && !hasMedia {
		return b.sendControl(ctx, controlHelpText())
	}
	if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
		return err
	} else if blocked {
		if serviceControlDefersInput(control) {
			session, err := b.ensureControlFallbackSession(ctx)
			if err != nil {
				return err
			}
			inbound, _, err := b.persistInboundWithStatusAndSource(ctx, session, msg, teamstore.InboundStatusDeferred, "teams_control_fallback")
			if err != nil {
				return err
			}
			return b.sendDeferredServiceControlNotice(ctx, session.ChatID, inbound, control)
		}
		return b.sendControl(ctx, serviceControlBlockedMessage(control, "control fallback requests"))
	}
	if hasMedia && !teamsASRTranscriberConfigured(b.asrTranscriber) {
		return b.sendControl(ctx, teamsASRFailureUserMessage(errASRCommandNotConfigured))
	}
	session, err := b.ensureControlFallbackSession(ctx)
	if err != nil {
		return err
	}
	promptText, warning, err := b.controlFallbackPromptWithMessageReferences(ctx, msg, text)
	if err != nil {
		return err
	}
	if warning != "" {
		return b.sendControl(ctx, warning)
	}
	persistMsg := controlFallbackMessageWithPromptBody(msg, promptText, text)
	inbound, created, err := b.persistInbound(ctx, session, persistMsg)
	if err != nil {
		return err
	}
	turn, turnCreated, err := b.queueTurn(ctx, session, inbound)
	if err != nil {
		return err
	}
	if !created || !turnCreated {
		return b.flushPendingOutbox(ctx, session.ID, turn.ID)
	}
	session.UpdatedAt = time.Now()
	if err := b.queueControlFallbackAck(ctx, session, turn); err != nil {
		return err
	}
	if b.asyncTurns {
		if _, err := b.startQueuedTurn(ctx, session, turn.ID, func(runCtx context.Context, runSession *Session, claimed teamstore.Turn) error {
			return b.runControlFallbackQueuedTurnFromMessage(runCtx, runSession, claimed, msg, promptText)
		}); err != nil {
			return err
		}
		b.boostPolling(time.Now())
		return nil
	}
	return b.runControlFallbackQueuedTurnFromMessage(ctx, session, turn, msg, promptText)
}

func (b *Bridge) runControlFallbackQueuedTurnFromMessage(ctx context.Context, session *Session, turn teamstore.Turn, msg ChatMessage, promptText string) error {
	if session == nil {
		return fmt.Errorf("control fallback session is required")
	}
	sessionID := session.ID
	prepCtx, cancelPrep := context.WithCancel(ctx)
	unregisterPrepCancel := b.registerRunningTurnCancel(sessionID, turn.ID, cancelPrep)
	input, cleanupPrompt, preparationMessage, err := b.prepareControlFallbackInputFromTeamsMessage(prepCtx, session, turn.ID, msg, promptText)
	cancelRequested, cancelReason, cancelSilent := b.runningTurnCancelState(turn.ID)
	unregisterPrepCancel()
	cancelPrep()
	if cleanupPrompt == nil {
		cleanupPrompt = func() {}
	}
	defer cleanupPrompt()
	if cancelRequested {
		if _, markErr := b.store.MarkTurnInterrupted(ctx, turn.ID, firstNonEmptyString(cancelReason, "canceled by user")); markErr != nil {
			return markErr
		}
		if cancelSilent {
			return nil
		}
		return b.queueAndSendOutboxChunks(ctx, session.ID, turn.ID, session.ChatID, "canceled", "Codex request canceled.")
	}
	if err != nil {
		return err
	}
	if preparationMessage != "" {
		return b.interruptTurnForAttachmentMessage(ctx, session, turn, preparationMessage)
	}
	return b.runQueuedTurnInputWithExecutor(ctx, b.effectiveControlFallbackExecutor(), session, turn, session.ChatID, input)
}

func (b *Bridge) prepareControlFallbackInputFromTeamsMessage(ctx context.Context, session *Session, turnID string, msg ChatMessage, promptText string) (ExecutionInput, func(), string, error) {
	cleanupHosted := func() {}
	localFiles, cleanupHostedFiles, hostedAttachmentMessage, err := b.downloadHostedContentAttachments(ctx, session, session.ChatID, msg)
	cleanupHosted = cleanupHostedFiles
	if err != nil {
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return ExecutionInput{}, cleanupHosted, message, nil
		}
		return ExecutionInput{}, cleanupHosted, "", err
	}
	if hostedAttachmentMessage != "" {
		return ExecutionInput{}, cleanupHosted, hostedAttachmentMessage, nil
	}
	transcripts, err := b.transcribeTeamsMediaAttachments(ctx, session, turnID, localFiles)
	if err != nil {
		return ExecutionInput{}, cleanupHosted, teamsASRFailureUserMessage(err), nil
	}
	b.annotateIncomingUserMessageWithASRTranscripts(ctx, session.ChatID, msg, transcripts)
	preparedPrompt := promptWithASRTranscripts(promptText, transcripts)
	codexFiles := nonTeamsMediaAttachments(localFiles)
	if strings.TrimSpace(preparedPrompt) == "" && len(codexFiles) > 0 {
		preparedPrompt = defaultLocalAttachmentPrompt
	}
	preparedPrompt = b.controlFallbackCodexPromptForMessage(ctx, preparedPrompt, msg.ID)
	return ExecutionInputWithLocalAttachments(preparedPrompt, codexFiles), cleanupHosted, "", nil
}

func (b *Bridge) ensureControlFallbackSession(ctx context.Context) (*Session, error) {
	if err := b.ensureStore(); err != nil {
		return nil, err
	}
	now := time.Now()
	model := b.effectiveControlFallbackModel()
	snapshot, err := b.resolveNewSessionModelProfile(ctx, "")
	if err != nil {
		return nil, err
	}
	created, _, err := b.store.CreateSession(ctx, teamstore.SessionContext{
		ID:           controlFallbackSessionID,
		Status:       teamstore.SessionStatusActive,
		RunnerKind:   "control_fallback",
		Model:        model,
		ModelProfile: snapshot,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err != nil {
		return nil, err
	}
	if !isDurableControlFallbackSession(created) || created.Status != teamstore.SessionStatusActive || created.Model != model || (created.ModelProfile.IsZero() && !snapshot.IsZero()) || created.TeamsChatID != "" || created.TeamsChatURL != "" || created.TeamsTopic != "" || created.Cwd != "" {
		if err := b.store.UpdateSession(ctx, controlFallbackSessionID, func(state *teamstore.State) error {
			current := state.Sessions[controlFallbackSessionID]
			sanitizeControlFallbackSession(&current, model, snapshot, now)
			state.Sessions[controlFallbackSessionID] = current
			return nil
		}); err != nil {
			return nil, err
		}
		created, _, err = b.store.CreateSession(ctx, teamstore.SessionContext{ID: controlFallbackSessionID})
		if err != nil {
			return nil, err
		}
	}
	return b.controlFallbackSessionFromState(created), nil
}

func (b *Bridge) controlFallbackSessionFromState(durable teamstore.SessionContext) *Session {
	status := string(durable.Status)
	if status == "" {
		status = "active"
	}
	return &Session{
		ID:            controlFallbackSessionID,
		ChatID:        b.reg.ControlChatID,
		ChatURL:       b.reg.ControlChatURL,
		Topic:         firstNonEmptyString(b.reg.ControlChatTopic, durable.TeamsTopic),
		UserTitle:     durable.UserTitle,
		TitleSource:   durable.TitleSource,
		Status:        status,
		CodexThreadID: durable.CodexThreadID,
		Cwd:           durable.Cwd,
		ModelProfile:  durable.ModelProfile,
		CreatedAt:     durable.CreatedAt,
		UpdatedAt:     durable.UpdatedAt,
	}
}

func controlHelpText() string {
	return strings.Join([]string{
		"## 🏠 Control chat",
		"Use this chat to choose a workspace and create or continue 💬 Work chats.",
		"",
		"Start here:",
		"- `p` / `projects` - choose a workspace",
		"- `n <directory>` / `new <directory>` - create a new Work chat for a directory",
		"- `new <directory> --model <model>` - create a Work chat pinned to a model",
		"- `model list` / `model use <model>` - list models or set the default for future chats",
		"- `s` / `sessions` - show sessions in the selected workspace",
		"- `c <number>` / `continue <number>` - continue an old local Codex session in Teams",
		"- `st` / `status` - show active Work chats",
		"- `helper rename <title>` - rename this Control chat",
		"- `helper rename hostname <name>` - rename this machine in all related chat titles",
		"- `skills` - list installed skill subscriptions",
		"",
		"After `p`, reply with a number such as `1` to open that workspace. On a workspace page, send `new` to create a Work chat in that workspace.",
		"",
		"For quick helper questions, type the question here. For project work, use a 💬 Work chat.",
		"",
		"Send `help advanced` for all commands.",
	}, "\n")
}

func controlAdvancedHelpText() string {
	return strings.Join([]string{
		"## 🏠 Control chat advanced help",
		"",
		"Workspace flow:",
		"- `p` / `projects` - list directories with local Codex history",
		"- `p 1` / `project 1` / `1` - open workspace 1 from the `projects` list",
		"- `new` / `n` - create a Work chat in the currently opened workspace",
		"- `n <directory>` / `new <directory>` - create the directory if missing, then create a Work chat there",
		"",
		"History flow:",
		"- `s` / `sessions` / `history` - list local sessions in the selected workspace",
		"- `c 1` / `continue 1` / `1` on a sessions page - create/open a Work chat and import that session history",
		"",
		"Other control commands:",
		"- `st` / `status` - list active Teams work chats",
		"- `d <number>` / `details <number>` - show technical IDs and details",
		"- `m <directory>` / `mkdir <directory>` - create a directory only",
		"- `ask <question>` - ask a quick helper question in this control chat",
		"- `helper cancel last` / `helper cancel all` - stop queued/running Codex question(s) in this Control chat",
		"- `model list`, `model setup <model>`, `model doctor <model>`, `model use <model>` - manage models",
		"- `helper rename <title>` - rename this Control chat",
		"- `helper rename hostname <name>` - rename this machine in the Control chat and all linked Work chats",
		"- `h` / `help` / `menu` - show short help",
		"- `helper restart` - show the safe restart confirmation",
		"- `helper restart now` - restart the local Teams helper after sending a confirmation",
		"- `helper update now` - update to the latest stable helper release",
		"- `helper update prerelease` - update to the newest helper release or pre-release",
		"- `helper asr status` / `helper asr warmup` - inspect or pre-warm local Teams speech recognition",
		"- `helper webhook setup` - show a guided Workflow webhook setup flow",
		"- `helper webhook <url>` - enable Workflow notification cards with a Teams Workflow webhook URL",
		"- `helper webhook off` - disable Workflow notification cards",
		"- `helper skills list` / `helper skills add <url>` / `helper skills sync [name]` / `helper skills push [name]` / `helper skills push confirm` - inspect, sync, or push skill subscriptions",
		"",
		"Beacon commands:",
		"- `beacon list` - list beacon profiles and machines",
		"- `beacon profile create <name> ...` - create a draft beacon profile",
		"- add `--query-command <script> --submit-command <script> --cancel-command <script> --renew-command <script>` to store provider adapters on the profile",
		"- Slurm/LSF adapters use your normal shell setup by default; add `--adapter-shell direct` when user-shell capture is incompatible or a clean service environment is required",
		"- `beacon profile update <name> ...` - create a new profile revision",
		"- `beacon profile history <name>` / `beacon profile rollback <name> <revision>` / `beacon profile gc <name>` - inspect, restore, and prune revisions",
		"- `beacon profile doctor <name>` - validate profile fields and provider adapters",
		"- `beacon profile confirm <name>` - confirm a reviewed profile",
		"- `beacon release <profile|allocation|provider-job|machine> [--force] [--confirm <token>]` - preview and release a beacon resource",
		"- advanced: `beacon allocation ...` and `beacon machine ...` inspect internal state",
		"- `new <directory> --beacon <profile>` - create a Work chat on a ready beacon profile",
		"- Beacon execution profiles are separate from SSH proxy profiles managed by `cxp proxy`.",
		"",
		"Local CLI:",
		"- `cxp skills install-builtin` - install or repair bundled local skills",
		"",
		"work chat commands:",
		"Inside a 💬 Work chat, send your task as a regular Teams message. Use `helper help`, `helper status`, `helper stats`, `helper retry last`, `helper file <relative-path>`, or `helper close` for helper actions.",
		"Status words: `queued`/`running` means wait, `completed` means done, `failed` or `interrupted` means check recent messages and changed files before `helper retry last`.",
		"If this chat stops replying for about a minute, send `helper status`. From the control chat, `helper reload now` loads the latest helper code and `helper restart now` restarts the helper.",
		"",
		"copy-ready examples:",
		"`p`",
		"`n /home/baka/project/codex-helper`",
		"`m ~/tmp/mobile-fix`",
	}, "\n")
}

func (b *Bridge) asrFromControl(ctx context.Context, msg ChatMessage, arg string) error {
	name, _ := splitDashboardCommandBody(strings.TrimSpace(arg))
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "help", "?":
		return b.sendControl(ctx, teamsASRControlHelpText(b.asrTranscriber))
	case "status", "st":
		return b.sendControl(ctx, teamsASRControlStatusText(b.asrTranscriber))
	case "warm", "warmup", "warm-up", "prewarm", "pre-warm", "setup", "preflight":
		return b.startASRWarmUpFromControl(ctx, msg)
	default:
		return b.sendControl(ctx, teamsASRControlHelpText(b.asrTranscriber))
	}
}

func teamsASRControlHelpText(transcriber ASRTranscriber) string {
	return strings.Join([]string{
		"## Speech recognition",
		"",
		teamsASRStatusLine(transcriber),
		"",
		"Commands:",
		"- `helper asr status` - show local ASR readiness mode",
		"- `helper asr warmup` - pre-download and validate managed local ASR runtime assets",
	}, "\n")
}

func teamsASRControlStatusText(transcriber ASRTranscriber) string {
	return strings.Join([]string{
		"## Speech recognition status",
		"",
		teamsASRStatusLine(transcriber),
		"",
		"Managed ASR warm-up runs automatically after the helper starts. Send `helper asr warmup` to retry it now.",
	}, "\n")
}

func (b *Bridge) startASRWarmUpFromControl(ctx context.Context, msg ChatMessage) error {
	if !teamsASRTranscriberConfigured(b.asrTranscriber) {
		return b.sendControl(ctx, teamsASRFailureUserMessage(errASRCommandNotConfigured))
	}
	warmable, ok := b.asrTranscriber.(ASRWarmUpTranscriber)
	if !ok || warmable == nil {
		return b.sendControl(ctx, "Speech recognition is configured, but this ASR backend does not support managed warm-up.")
	}
	if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_asr_warmup"); err != nil {
		return err
	} else if duplicate {
		return nil
	}
	if err := b.sendControl(ctx, "Speech recognition warm-up started. I will send a follow-up when it finishes."); err != nil {
		return err
	}
	go func() {
		warmCtx, cancel := context.WithTimeout(context.Background(), teamsASRWarmUpTimeout)
		defer cancel()
		if err := warmable.WarmUpTeamsASR(warmCtx); err != nil {
			if warmCtx.Err() != nil {
				_ = b.sendControl(context.Background(), "Speech recognition warm-up timed out. It will retry automatically when Teams media is received.")
				return
			}
			_ = b.sendControl(context.Background(), "Speech recognition warm-up failed: "+teamsASRUserFailureDetail(err))
			return
		}
		_ = b.sendControl(context.Background(), "Speech recognition warm-up completed. Teams voice/video transcription is ready to use.")
	}()
	return nil
}

func sessionHelpText() string {
	return strings.Join([]string{
		"💬 Work chat",
		"Send your task as a regular Teams message. Messages starting with `helper` or `!` are helper commands.",
		"",
		"Common commands:",
		"`helper status` or `!status` - check progress",
		"`helper stats` or `!stats` - show Codex token usage from the linked local history",
		"`helper file <relative-path>` or `!file <relative-path>` - upload a file prepared in the helper's Teams upload folder",
		"`helper restore-thread <thread-id>` - restore a missing Codex thread binding before retrying an interrupted turn",
		"`helper close` or `!close` - close this Codex session in Teams",
		"`helper details` or `!details` - show debug IDs and links",
		"`beacon status` - show this Work chat execution target",
		"`beacon switch <profile>` or `beacon switch local` - switch future turns",
		"`model status`, `model switch <model>`, or `model fork <model>` - inspect or change this Work chat model",
		"`helper publish-history` - import a paused local Codex history backlog",
		"`helper skills list` / `helper skills add <url>` / `helper skills push` / `helper skills push confirm` - inspect and push skill subscriptions",
		"",
		"Send `helper help advanced` for retry, cancel, and rename commands.",
	}, "\n")
}

func sessionAdvancedHelpText() string {
	return strings.Join([]string{
		"💬 Work chat advanced help",
		"`helper status` or `!status` - check progress",
		"`helper stats` or `!stats` - show Codex token usage, cache/context analysis, and rate-limit metadata when Codex recorded it",
		"`helper details` or `!details` - show IDs and debug details",
		"`beacon status`, `beacon list`, `beacon switch <profile>`, or `beacon switch local` - inspect or switch this Work chat execution target",
		"`helper rename <title>` or `!rename <title>` - rename this Teams chat",
		"`helper rename hostname <name>` - rename this machine in all related chat titles",
		"`helper file <relative-path>` or `!file <relative-path>` - upload a file prepared in the helper's Teams upload folder",
		"`helper close` or `!close` - close this Codex session in Teams",
		"`helper publish-history` or `!ph` - import a paused local Codex history backlog",
		"`helper restore-thread <thread-id>` - restore a missing Codex thread binding; it never overwrites a different existing binding",
		"advanced commands: `helper retry last`, `helper retry <turn-id>` / `!retry <turn-id>`, or `helper cancel last`, `helper cancel all`, `helper cancel <turn-id>` / `!cancel <turn-id>`",
		"Status words: `queued`/`running` means wait, `completed` means done, `failed` or `interrupted` means check recent messages and changed files before `helper retry last`.",
		"Other text, including unknown slash-prefixed text, is sent to Codex.",
	}, "\n")
}

func isAdvancedHelpArg(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "advanced", "all", "more", "full":
		return true
	default:
		return false
	}
}

func unknownControlCommandMessage(text string) string {
	name, _, _, _ := splitDashboardCommand(text)
	name = strings.TrimSpace(name)
	if name == "" {
		return controlHelpText()
	}
	if isWorkOnlyHelperCommand(text) {
		return "⚠️ Wrong chat\n\nThis is the 🏠 control chat. `helper ...` commands like `helper file`, `helper retry`, and `helper close` work inside a 💬 Work chat.\n\nTo start project work, send `new <directory>` here, then open the new Work chat and send the task there."
	}
	if strings.Contains(name, "/") || strings.HasPrefix(name, ".") {
		return controlPathHintMessage(text)
	}
	return fmt.Sprintf("unknown control command: `%s`\n\n%s", name, controlHelpText())
}

func (b *Bridge) restartHelperFromControl(ctx context.Context, msg ChatMessage, arg string) error {
	arg = strings.TrimSpace(arg)
	if !helperRestartConfirmed(arg) {
		return b.sendControl(ctx, strings.Join([]string{
			"## 🔄 Helper restart",
			"",
			"This restarts the local Teams helper on this machine.",
			"It will only restart when there is no active Codex work.",
			"",
			"To restart, send:",
			"`helper restart now`",
			"",
			"If you are debugging a stuck helper and accept interrupting work, send:",
			"`helper restart force`",
		}, "\n"))
	}
	if b.helperRestarter == nil {
		return b.sendControl(ctx, "⚠️ Helper restart is not available in this helper process. Start it with the normal Teams service command, then try again.")
	}
	if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_restart"); err != nil {
		return err
	} else if duplicate {
		return nil
	}
	if message, blocked, err := b.helperRestartBlockedMessage(ctx, helperRestartForce(arg)); err != nil {
		return err
	} else if blocked {
		return b.sendControl(ctx, message)
	}
	if err := b.writePendingHelperRestartNotice(msg); err != nil {
		return b.sendControl(ctx, "⚠️ Helper restart was not started because I could not record the post-restart confirmation.\n\n"+err.Error())
	}
	if err := b.sendControl(ctx, strings.Join([]string{
		"🔄 Helper restart scheduled",
		"",
		"I may be silent for a few seconds.",
		"After it comes back, send `st` if you want to check status.",
	}, "\n")); err != nil {
		_ = b.clearPendingHelperRestartNotice()
		return err
	}
	restarter := b.helperRestarter
	b.helperRestartWG.Add(1)
	go func() {
		defer b.helperRestartWG.Done()
		b.runDelayedHelperRestart(restarter)
	}()
	return nil
}

func helperRestartConfirmed(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "now", "--now", "restart now", "restart --now", "force", "--force", "restart force", "restart --force":
		return true
	default:
		return false
	}
}

func helperRestartForce(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "force", "--force", "restart force", "restart --force":
		return true
	default:
		return false
	}
}

func (b *Bridge) reloadHelperFromControl(ctx context.Context, msg ChatMessage, arg string) error {
	arg = strings.TrimSpace(arg)
	if !helperReloadConfirmed(arg) {
		return b.sendControl(ctx, strings.Join([]string{
			"## 🔁 Helper reload",
			"",
			"This rebuilds the local Teams helper from the current source checkout, replaces the running helper binary, then restarts it.",
			"It will only reload when there is no active Codex work.",
			"",
			"To reload, send:",
			"`helper reload now`",
			"",
			"Debug only, if you accept interrupting active work:",
			"`helper reload force`",
		}, "\n"))
	}
	if b.helperReloader == nil {
		return b.sendControl(ctx, "⚠️ Helper reload is not available in this helper process. Start it with the normal Teams service command, then try again.")
	}
	if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_reload"); err != nil {
		return err
	} else if duplicate {
		return nil
	}
	previous, message, blocked, err := b.beginHelperReloadDrain(ctx, helperReloadForce(arg))
	if err != nil {
		return err
	}
	if blocked {
		return b.sendControl(ctx, message)
	}
	if err := b.sendControl(ctx, strings.Join([]string{
		"🔁 Helper reload started",
		"",
		"I am testing and rebuilding the helper from the current source checkout.",
		"I may be silent for a short time.",
	}, "\n")); err != nil {
		_ = b.restoreHelperReloadDrain(context.Background(), previous)
		return err
	}
	reloader := b.helperReloader
	b.runDelayedHelperReload(reloader, HelperReloadOptions{Force: helperReloadForce(arg)}, previous, msg)
	return nil
}

func (b *Bridge) updateHelperFromControl(ctx context.Context, msg ChatMessage, arg string) error {
	req, ok := parseHelperUpdateRequest(arg)
	if !ok {
		return b.sendControl(ctx, helperUpdateHelpText(b.helperAutoUpdatePrerelease))
	}
	if b.helperAutoUpdater == nil {
		return b.sendControl(ctx, "⚠️ Helper update is not available in this helper process. Start it with the normal Teams service command, then try again.")
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_update"); err != nil {
		return err
	} else if duplicate {
		return nil
	}
	now := time.Now()
	decision, err := b.helperAutoUpdater.Check(ctx, HelperAutoUpdateCheck{
		InstalledVersion:  b.helperVersion,
		Now:               now,
		IncludePrerelease: req.IncludePrerelease,
		Manual:            true,
	})
	if err != nil {
		_, _ = b.store.RecordAutoUpdateCheck(context.Background(), teamstore.AutoUpdateRecord{
			Now:          now,
			NextCheckAt:  now.Add(30 * time.Minute),
			BackoffUntil: now.Add(30 * time.Minute),
			LastError:    err.Error(),
		})
		return b.sendControl(ctx, "⚠️ Helper update check failed\n\n"+err.Error())
	}
	record := teamstore.AutoUpdateRecord{
		Now:          now,
		NextCheckAt:  decision.NextCheckAt,
		BackoffUntil: decision.BackoffUntil,
		LastError:    decision.LastError,
	}
	if record.NextCheckAt.IsZero() {
		record.NextCheckAt = now.Add(30 * time.Minute)
	}
	if decision.Candidate != nil {
		record.CandidateTag = decision.Candidate.TagName
		record.CandidateVersion = decision.Candidate.Version
		record.CandidatePriority = decision.Candidate.Priority
		record.CandidateAsset = decision.Candidate.Asset
		record.CandidatePublishedAt = decision.Candidate.PublishedAt
		record.CandidateEligibleAt = decision.Candidate.EligibleAt
	}
	if _, err := b.store.RecordAutoUpdateCheck(ctx, record); err != nil {
		return err
	}
	if decision.Candidate == nil {
		if req.IncludePrerelease {
			return b.sendControl(ctx, "✅ Helper is already on the latest available release or pre-release for this machine.")
		}
		return b.sendControl(ctx, "✅ Helper is already on the latest stable release for this machine.")
	}
	if err := b.sendControl(ctx, helperUpdateScheduledMessage(*decision.Candidate, req.IncludePrerelease)); err != nil {
		return err
	}
	if err := b.applyHelperAutoUpdateWhenDrainedWithOptions(ctx, BridgeOptions{
		HelperAutoUpdater:          b.helperAutoUpdater,
		HelperRestarter:            b.helperRestarter,
		HelperPendingRestarter:     b.helperPendingRestarter,
		HelperAutoUpdatePrerelease: b.helperAutoUpdatePrerelease,
	}, *decision.Candidate, helperAutoUpdateApplyOptions{
		Manual:           true,
		ControlChatID:    firstNonEmptyString(b.reg.ControlChatID, msg.ChatID),
		CommandMessageID: msg.ID,
	}); err != nil {
		return b.sendControl(ctx, "⚠️ Helper update failed\n\n"+err.Error())
	}
	return nil
}

func (b *Bridge) workflowWebhookFromControl(ctx context.Context, msg ChatMessage, arg string) error {
	arg = strings.TrimSpace(arg)
	name, rest := splitDashboardCommandBody(arg)
	lowerName := strings.ToLower(strings.TrimSpace(name))
	switch lowerName {
	case "", "help", "?":
		return b.sendControl(ctx, workflowWebhookControlHelpText())
	case "setup", "guide", "start":
		return b.sendControl(ctx, b.workflowWebhookControlSetupText())
	case "status", "st":
		return b.sendControl(ctx, b.workflowWebhookControlStatus(ctx))
	case "off", "disable", "disabled":
		if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_webhook_disable"); err != nil {
			return err
		} else if duplicate {
			return nil
		}
		if _, err := b.ConfigureWorkflowNotifications(ctx, "", false); err != nil {
			return b.sendControl(ctx, "⚠️ Workflow webhook was not disabled.\n\n"+err.Error())
		}
		return b.sendControl(ctx, "🔕 Workflow webhook disabled.\n\nThe helper will stop sending Workflow notification cards.")
	case "test":
		if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_webhook_test"); err != nil {
			return err
		} else if duplicate {
			return nil
		}
		if err := b.SendWorkflowNotificationTest(ctx); err != nil {
			return b.sendControl(ctx, "⚠️ Workflow webhook test failed.\n\n"+err.Error())
		}
		return b.sendControl(ctx, "✅ Workflow webhook test card queued.")
	case "set", "url", "enable":
		arg = strings.TrimSpace(rest)
	}
	if arg == "" {
		return b.sendControl(ctx, workflowWebhookControlHelpText())
	}
	arg = workflowWebhookURLFromControlMessage(msg, arg)
	if !safeWorkflowWebhookURL(arg) {
		return b.sendControl(ctx, "⚠️ Workflow webhook URL was not saved.\n\nSend an absolute `https://...` Teams Workflow webhook URL, for example:\n`helper webhook https://...`")
	}
	if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_webhook_configure"); err != nil {
		return err
	} else if duplicate {
		return nil
	}
	cfg, err := b.ConfigureWorkflowNotificationsFromWebhookURL(ctx, arg)
	if err != nil {
		return b.sendControl(ctx, "⚠️ Workflow webhook was not saved.\n\n"+err.Error())
	}
	lines := []string{
		"✅ Workflow webhook enabled.",
		"",
		"Notification cards will be sent for important helper events.",
		"Webhook URL: saved locally as a private secret file; I will not echo it back.",
	}
	if cfg.ControlChatID != "" {
		lines = append(lines, "Bound control chat: `"+cfg.ControlChatID+"`")
	}
	lines = append(lines, "", "Send `helper webhook test` to send a test card.")
	return b.sendControl(ctx, strings.Join(lines, "\n"))
}

func workflowWebhookControlHelpText() string {
	return strings.Join([]string{
		"## 🔔 Workflow webhook",
		"",
		"Use this to send Teams Workflow notification cards for important helper events.",
		"",
		"Commands:",
		"- `helper webhook setup` - show the setup steps",
		"- `helper webhook <https-url>` - save and enable the webhook",
		"- `helper webhook test` - send one test card",
		"- `helper webhook status` - show whether it is enabled",
		"- `helper webhook off` - disable cards",
		"",
		"The webhook URL is stored locally as a private secret file and is not echoed back.",
	}, "\n")
}

func (b *Bridge) workflowWebhookControlSetupText() string {
	controlTitle := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatTopic, workflowControlChatTitle(b, teamstore.State{}), b.reg.ControlChatID))
	if controlTitle == "" {
		controlTitle = "this Control chat"
	}
	controlHint := controlTitle
	if b.reg.ControlChatID != "" {
		controlHint += "\nChat ID: `" + b.reg.ControlChatID + "`"
	}
	guideURL := "https://support.microsoft.com/en-us/office/create-incoming-webhooks-with-workflows-for-microsoft-teams-8ae491c7-0394-4861-ba59-055e33f75498"
	return strings.Join([]string{
		"## 🔔 Workflow webhook setup",
		"",
		"Use this to send notification cards when Codex finishes, reloads, restarts, or needs your attention.",
		"",
		"Step 1: Open Workflows here",
		"In this Control chat, click the `+` button at the lower right of the message box, then choose `Workflow`.",
		"",
		"Step 2: Pick exactly this Workflow template",
		"Search `webhook`, then choose exactly: **Send webhook alerts to a chat**",
		"",
		"Step 3: Confirm the target chat",
		"Keep the target set to this Control chat:",
		controlHint,
		"",
		"Step 4: Create the Workflow and copy the URL",
		"Click `Next` / `Add workflow`. Teams will show a webhook URL; copy it.",
		"",
		"Step 5: Send this here",
		"`helper webhook <paste-url>`",
		"If Teams turns the pasted URL into a shortened hyperlink, the helper will read the actual hyperlink target.",
		"",
		"Need the Microsoft walkthrough? Open [Teams webhook setup guide](" + guideURL + ").",
		"",
		"Note: Microsoft does not provide a reliable one-click link that pre-fills every workflow field and returns the generated webhook URL.",
	}, "\n")
}

func workflowWebhookURLFromControlMessage(msg ChatMessage, arg string) string {
	arg = strings.TrimSpace(arg)
	if !strings.Contains(strings.ToLower(arg), "http") {
		return arg
	}
	for _, href := range safeWorkflowWebhookHrefsFromTeamsHTML(msg.Body.Content) {
		return href
	}
	return arg
}

func safeWorkflowWebhookHrefsFromTeamsHTML(content string) []string {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}
	root, err := xhtml.Parse(strings.NewReader("<html><body>" + content + "</body></html>"))
	if err != nil {
		return nil
	}
	var out []string
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n == nil {
			return
		}
		if n.Type == xhtml.ElementNode && strings.EqualFold(n.Data, "a") {
			for _, attr := range n.Attr {
				if strings.EqualFold(attr.Key, "href") {
					href := strings.TrimSpace(attr.Val)
					if safeWorkflowWebhookURL(href) {
						out = append(out, href)
					}
					break
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return out
}

func (b *Bridge) workflowWebhookControlStatus(ctx context.Context) string {
	if err := b.ensureStore(); err != nil {
		return "⚠️ Workflow webhook status failed.\n\n" + err.Error()
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return "⚠️ Workflow webhook status failed.\n\n" + err.Error()
	}
	cfg, err := b.effectiveWorkflowNotificationConfig(state)
	if err != nil {
		return "⚠️ Workflow webhook status failed.\n\n" + err.Error()
	}
	if !cfg.Enabled {
		return "🔕 Workflow webhook disabled.\n\nSend `helper webhook <https-url>` to enable Workflow notification cards."
	}
	lines := []string{
		"🔔 Workflow webhook enabled.",
		"",
		"Webhook URL: configured as a local private secret file.",
	}
	if cfg.ControlChatID != "" {
		lines = append(lines, "Bound control chat: `"+cfg.ControlChatID+"`")
	}
	if !cfg.UpdatedAt.IsZero() {
		lines = append(lines, "Updated: "+cfg.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
	}
	return strings.Join(lines, "\n")
}

type helperUpdateRequest struct {
	IncludePrerelease bool
}

func parseHelperUpdateRequest(arg string) (helperUpdateRequest, bool) {
	tokens := strings.Fields(strings.ToLower(strings.TrimSpace(arg)))
	if len(tokens) == 0 {
		return helperUpdateRequest{}, false
	}
	filtered := tokens[:0]
	for _, token := range tokens {
		switch token {
		case "update", "upgrade":
			continue
		default:
			filtered = append(filtered, token)
		}
	}
	if len(filtered) == 0 {
		return helperUpdateRequest{}, false
	}
	req := helperUpdateRequest{}
	recognized := false
	for _, token := range filtered {
		switch token {
		case "now", "--now", "latest", "release", "stable":
			recognized = true
		case "prerelease", "pre-release", "pre", "rc", "--pre", "--prerelease", "--include-prerelease":
			req.IncludePrerelease = true
			recognized = true
		default:
			return helperUpdateRequest{}, false
		}
	}
	return req, recognized
}

func helperUpdateHelpText(autoPrerelease bool) string {
	mode := "stable releases only"
	if autoPrerelease {
		mode = "stable releases plus eligible pre-releases"
	}
	return strings.Join([]string{
		"## ⬇️ Helper update",
		"",
		"This updates the local Teams helper from GitHub releases.",
		"It waits until active Codex work is idle before replacing the helper.",
		"",
		"Commands:",
		"- `helper update now` - update to the latest stable release",
		"- `helper update prerelease` - update to the newest release or pre-release",
		"",
		"Automatic updates currently check: " + mode + ".",
	}, "\n")
}

func helperUpdateScheduledMessage(candidate HelperAutoUpdateCandidate, includePrerelease bool) string {
	channel := "stable release"
	if includePrerelease {
		channel = "release/pre-release"
	}
	return strings.Join([]string{
		"⬇️ Helper update scheduled",
		"",
		"Target: `" + firstNonEmptyString(candidate.TagName, "v"+strings.TrimPrefix(candidate.Version, "v")) + "`",
		"Channel: " + channel,
		"",
		"I will install it when active Codex work is idle, then restart the helper.",
	}, "\n")
}

func helperReloadConfirmed(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "now", "--now", "reload now", "reload --now", "force", "--force", "reload force", "reload --force":
		return true
	default:
		return false
	}
}

func helperReloadForce(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "force", "--force", "reload force", "reload --force":
		return true
	default:
		return false
	}
}

func (b *Bridge) beginHelperReloadDrain(ctx context.Context, force bool) (teamstore.ServiceControl, string, bool, error) {
	if err := b.ensureStore(); err != nil {
		return teamstore.ServiceControl{}, "", false, err
	}
	now := time.Now()
	var previous teamstore.ServiceControl
	var blockedMessage string
	var blocked bool
	err := b.store.Update(ctx, func(state *teamstore.State) error {
		previous = state.ServiceControl
		recoveringStaleReloadDrain := false
		if state.ServiceControl.Draining {
			if teamstore.HelperReloadDrainStale(*state, now, helperReloadDrainStaleAfter) {
				if message, isBlocked := b.staleHelperReloadLifecycleBlockMessage(*state, force, "reload", now); isBlocked {
					blocked = true
					blockedMessage = message
					return nil
				}
				previous = clearStaleHelperReloadControl(state.ServiceControl, now)
				recoveringStaleReloadDrain = true
			} else {
				blocked = true
				blockedMessage = helperDrainBlockedMessage(state.ServiceControl, "reload")
				return nil
			}
		}
		if !recoveringStaleReloadDrain && !force && teamstore.HasUpgradeBlockingWork(*state, now) {
			blocked = true
			blockedMessage = strings.Join([]string{
				"⏳ Codex work is still active.",
				"",
				"I will not reload while work or Teams messages are still in progress.",
				"Wait for it to finish, then send `helper reload now`.",
				"",
				"Debug only: `helper reload force` may interrupt active work.",
			}, "\n")
			return nil
		}
		next := previous
		next.Draining = true
		next.Reason = teamstore.HelperReloadReason
		next.UpdatedAt = now
		state.ServiceControl = next
		return nil
	})
	if err != nil {
		return teamstore.ServiceControl{}, "", false, err
	}
	return previous, blockedMessage, blocked, nil
}

func (b *Bridge) restoreHelperReloadDrain(ctx context.Context, previous teamstore.ServiceControl) error {
	if b == nil || b.store == nil {
		return nil
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		current := state.ServiceControl
		if !current.Draining || current.Reason != teamstore.HelperReloadReason {
			return nil
		}
		restored := previous
		restored.UpdatedAt = time.Now()
		state.ServiceControl = restored
		return nil
	})
}

func (b *Bridge) clearStaleHelperReloadDrainOnStart(ctx context.Context) error {
	if b == nil || b.store == nil {
		return nil
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		current := state.ServiceControl
		if !current.Draining || current.Reason != teamstore.HelperReloadReason {
			return nil
		}
		state.ServiceControl = clearStaleHelperReloadControl(current, time.Now())
		return nil
	})
}

func clearStaleHelperReloadControl(control teamstore.ServiceControl, now time.Time) teamstore.ServiceControl {
	control.Draining = false
	if !control.Paused {
		control.Reason = ""
	}
	control.UpdatedAt = now
	return control
}

func (b *Bridge) completeExpiredHelperUpgradeDrainOnStart(ctx context.Context) error {
	if b == nil || b.store == nil {
		return nil
	}
	state, err := b.store.UpgradeBlockingStateSnapshot(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	if !teamstore.HelperUpgradeDrainExpired(state, now) {
		return nil
	}
	owner, hasOwner := helperRestartStateOwner(state)
	if hasOwner {
		staleAfter := b.ownerStaleAfter
		if staleAfter <= 0 {
			staleAfter = 5 * time.Minute
		}
		ownerFresh := !teamstore.IsStale(owner, staleAfter, now) && !teamstore.OwnerAppearsLocallyDead(owner)
		if ownerFresh && !teamstore.OwnerAppearsLocal(owner) {
			return nil
		}
		if ownerFresh && strings.TrimSpace(owner.ActiveTurnID) != "" {
			return nil
		}
	}
	if state.Upgrade != nil && strings.TrimSpace(state.Upgrade.ID) != "" {
		targetTag := helperUpgradeTargetTagForExpiredDrain(state)
		if targetTag != "" {
			if !helperVersionMatchesTag(b.helperVersion, targetTag) {
				reason := "helper upgrade drain expired before helper reached target " + targetTag
				if _, err := b.store.AbortUpgrade(ctx, state.Upgrade.ID, reason); err != nil {
					return err
				}
				_, err := b.store.ClearDrain(ctx)
				return err
			}
			if _, err := b.store.RecordAutoUpdateInstalled(ctx, targetTag, now); err != nil {
				return err
			}
			_, err := b.store.CompleteUpgrade(ctx, state.Upgrade.ID, targetTag)
			return err
		}
		_, err := b.store.CompleteUpgrade(ctx, state.Upgrade.ID, b.helperVersion)
		return err
	}
	_, err = b.store.ClearDrain(ctx)
	return err
}

func helperUpgradeTargetTagForExpiredDrain(state teamstore.State) string {
	if state.Upgrade == nil {
		return ""
	}
	if tag := strings.TrimSpace(state.Upgrade.InstalledTag); tag != "" {
		return tag
	}
	tag := strings.TrimSpace(state.AutoUpdate.LastAttemptTag)
	if tag == "" || state.AutoUpdate.LastAttemptAt.IsZero() || state.Upgrade.StartedAt.IsZero() {
		return ""
	}
	if state.AutoUpdate.LastAttemptAt.Before(state.Upgrade.StartedAt) {
		return ""
	}
	return tag
}

func (b *Bridge) pendingHelperRestartNoticePath() (string, error) {
	if err := b.ensureStore(); err != nil {
		return "", err
	}
	storePath := strings.TrimSpace(b.store.Path())
	if storePath == "" {
		return "", fmt.Errorf("Teams store path is empty")
	}
	return filepath.Join(filepath.Dir(storePath), "helper-restart-pending.json"), nil
}

func (b *Bridge) writePendingHelperRestartNotice(msg ChatMessage) error {
	return b.writePendingHelperLifecycleNotice(msg, helperRestartNoticeActionRestart)
}

func (b *Bridge) writePendingHelperReloadNotice(msg ChatMessage) error {
	return b.writePendingHelperLifecycleNotice(msg, helperRestartNoticeActionReload)
}

func (b *Bridge) writePendingHelperUpgradeNotice(chatID string, commandMessageID string, tag string, manual bool) error {
	return b.writePendingHelperUpgradeNoticeWithReplacement(chatID, commandMessageID, tag, manual, "", "")
}

func (b *Bridge) writePendingHelperUpgradeNoticeWithReplacement(chatID string, commandMessageID string, tag string, manual bool, pendingReplacePath string, installPath string) error {
	return b.writePendingHelperLifecycleNoticeWithOptions(helperRestartNotice{
		Version:            1,
		Action:             helperRestartNoticeActionUpgrade,
		Tag:                strings.TrimSpace(tag),
		Manual:             manual,
		ControlChatID:      strings.TrimSpace(chatID),
		CommandMessageID:   strings.TrimSpace(commandMessageID),
		PendingReplacePath: strings.TrimSpace(pendingReplacePath),
		InstallPath:        strings.TrimSpace(installPath),
		RequestedAt:        time.Now(),
	})
}

func (b *Bridge) writePendingHelperLifecycleNotice(msg ChatMessage, action string) error {
	return b.writePendingHelperLifecycleNoticeWithOptions(helperRestartNotice{
		Version:          1,
		Action:           normalizedHelperRestartNoticeAction(action),
		ControlChatID:    strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, msg.ChatID)),
		CommandMessageID: strings.TrimSpace(msg.ID),
		RequestedAt:      time.Now(),
	})
}

func (b *Bridge) writePendingHelperLifecycleNoticeWithOptions(notice helperRestartNotice) error {
	path, err := b.pendingHelperRestartNoticePath()
	if err != nil {
		return err
	}
	notice.Action = normalizedHelperRestartNoticeAction(notice.Action)
	notice.ControlChatID = strings.TrimSpace(firstNonEmptyString(notice.ControlChatID, b.reg.ControlChatID))
	notice.CommandMessageID = strings.TrimSpace(notice.CommandMessageID)
	notice.Tag = strings.TrimSpace(notice.Tag)
	notice.PendingReplacePath = strings.TrimSpace(notice.PendingReplacePath)
	notice.InstallPath = strings.TrimSpace(notice.InstallPath)
	if notice.Version == 0 {
		notice.Version = 1
	}
	if notice.RequestedAt.IsZero() {
		notice.RequestedAt = time.Now()
	}
	if notice.ControlChatID == "" {
		return fmt.Errorf("control chat is not bound")
	}
	data, err := json.MarshalIndent(notice, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func normalizedHelperRestartNoticeAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case helperRestartNoticeActionReload:
		return helperRestartNoticeActionReload
	case helperRestartNoticeActionUpgrade:
		return helperRestartNoticeActionUpgrade
	default:
		return helperRestartNoticeActionRestart
	}
}

func (b *Bridge) clearPendingHelperRestartNotice() error {
	path, err := b.pendingHelperRestartNoticePath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (b *Bridge) queuePendingHelperRestartNotice(ctx context.Context) error {
	path, err := b.pendingHelperRestartNoticePath()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var notice helperRestartNotice
	if err := json.Unmarshal(data, &notice); err != nil {
		return err
	}
	chatID := strings.TrimSpace(firstNonEmptyString(notice.ControlChatID, b.reg.ControlChatID))
	if chatID == "" {
		return fmt.Errorf("pending helper restart notice has no control chat")
	}
	action := normalizedHelperRestartNoticeAction(notice.Action)
	if action == helperRestartNoticeActionUpgrade {
		tag := strings.TrimSpace(notice.Tag)
		if handled, err := b.queueCompletedHelperUpgradeNoticeIfNeeded(ctx); err != nil {
			return err
		} else if handled {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			return nil
		}
		if tag == "" || !helperVersionMatchesTag(b.helperVersion, tag) {
			if _, err := b.queuePendingHelperActivationAttentionNotice(ctx, notice); err != nil {
				return err
			}
			return nil
		}
		if _, err := b.store.RecordAutoUpdateInstalled(ctx, tag, time.Now()); err != nil {
			return err
		}
		state, err := b.store.Load(ctx)
		if err != nil {
			return err
		}
		if state.Upgrade != nil && strings.TrimSpace(state.Upgrade.Reason) == teamstore.HelperUpgradeReason {
			if _, err := b.store.CompleteUpgrade(ctx, state.Upgrade.ID, tag); err != nil {
				return err
			}
			if handled, err := b.queueCompletedHelperUpgradeNoticeIfNeeded(ctx); err != nil {
				return err
			} else if handled {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				return nil
			}
		}
	}
	seed := firstNonEmptyString(notice.CommandMessageID, notice.RequestedAt.Format(time.RFC3339Nano), chatID)
	kind := "control-" + action + "-complete"
	body := helperLifecycleCompletedNoticeBody(action, notice.Tag)
	msg := teamstore.OutboxMessage{
		ID:          "outbox:control:helper-" + action + "-complete:" + shortStableID(seed),
		TeamsChatID: chatID,
		Kind:        kind,
		Body:        body,
	}
	if action == helperRestartNoticeActionUpgrade && notice.Manual {
		msg.MentionOwner = true
		msg.NotificationKind = "helper_upgrade_completed"
	}
	if _, err := b.queueOutbox(ctx, msg); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (b *Bridge) queuePendingHelperActivationAttentionNotice(ctx context.Context, notice helperRestartNotice) (bool, error) {
	status, ok, err := readHelperActivationStatus(notice.PendingReplacePath)
	if err != nil {
		if b != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper activation status read error: %v\n", err)
		}
		return false, nil
	}
	if !ok {
		return false, nil
	}
	statusName := strings.ToLower(strings.TrimSpace(status.Status))
	notificationKind := ""
	kind := ""
	switch statusName {
	case "failed":
		kind = "failed-helper-upgrade-activation"
		notificationKind = helperUpgradeActivationFailedNotificationKind
	case "success":
		kind = "mismatched-helper-upgrade-activation"
		notificationKind = helperUpgradeActivationActionRequiredNotificationKind
	default:
		return false, nil
	}
	chatID := strings.TrimSpace(firstNonEmptyString(notice.ControlChatID, b.reg.ControlChatID))
	if chatID == "" {
		return false, fmt.Errorf("pending helper activation notice has no control chat")
	}
	seed := strings.Join([]string{
		strings.TrimSpace(notice.CommandMessageID),
		strings.TrimSpace(notice.Tag),
		strings.TrimSpace(notice.PendingReplacePath),
		statusName,
	}, "\x00")
	msg := teamstore.OutboxMessage{
		ID:               "outbox:control:helper-upgrade-activation-" + statusName + ":" + shortStableID(seed),
		TeamsChatID:      chatID,
		Kind:             kind,
		Body:             b.helperActivationAttentionNoticeBody(notice, status, notificationKind),
		MentionOwner:     true,
		NotificationKind: notificationKind,
	}
	if helperActivationNoticeAlreadyQueued(notice, statusName, msg.ID) {
		if err := b.flushExistingOutboxIfPending(ctx, msg.ID, chatID); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams best-effort outbox send error: %v\n", err)
		}
		return true, nil
	}
	queued, err := b.queueOutbox(ctx, msg)
	if err != nil {
		return false, err
	}
	if err := b.markPendingHelperActivationNoticeQueued(notice, statusName, queued.ID); err != nil {
		return false, err
	}
	if queued.Status != teamstore.OutboxStatusSent {
		if err := b.flushPendingOutboxForChat(ctx, queued.TeamsChatID); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams best-effort outbox send error: %v\n", err)
		}
	}
	return true, nil
}

func helperActivationNoticeAlreadyQueued(notice helperRestartNotice, statusName string, outboxID string) bool {
	key := helperActivationNoticeKey(statusName)
	if key == "" || strings.TrimSpace(outboxID) == "" || notice.ActivationNotices == nil {
		return false
	}
	record, ok := notice.ActivationNotices[key]
	if !ok {
		return false
	}
	return strings.TrimSpace(record.OutboxID) == strings.TrimSpace(outboxID) && !record.QueuedAt.IsZero()
}

func (b *Bridge) markPendingHelperActivationNoticeQueued(notice helperRestartNotice, statusName string, outboxID string) error {
	key := helperActivationNoticeKey(statusName)
	outboxID = strings.TrimSpace(outboxID)
	if key == "" || outboxID == "" {
		return nil
	}
	if notice.ActivationNotices == nil {
		notice.ActivationNotices = make(map[string]helperActivationNoticeRecord)
	}
	record := notice.ActivationNotices[key]
	record.Status = key
	record.OutboxID = outboxID
	if record.QueuedAt.IsZero() {
		record.QueuedAt = time.Now()
	}
	notice.ActivationNotices[key] = record
	return b.writePendingHelperLifecycleNoticeWithOptions(notice)
}

func helperActivationNoticeKey(statusName string) string {
	switch strings.ToLower(strings.TrimSpace(statusName)) {
	case "failed":
		return "failed"
	case "success":
		return "success"
	default:
		return ""
	}
}

func readHelperActivationStatus(pendingPath string) (helperActivationStatus, bool, error) {
	path := helperActivationStatusPath(pendingPath)
	if path == "" {
		return helperActivationStatus{}, false, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return helperActivationStatus{}, false, nil
	}
	if err != nil {
		return helperActivationStatus{}, false, err
	}
	data = trimUTF8BOM(data)
	var status helperActivationStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return helperActivationStatus{}, false, err
	}
	return status, true, nil
}

func helperActivationStatusPath(pendingPath string) string {
	pendingPath = strings.TrimSpace(pendingPath)
	if pendingPath == "" {
		return ""
	}
	return pendingPath + ".activation.json"
}

func trimUTF8BOM(data []byte) []byte {
	if len(data) >= 3 && data[0] == 0xef && data[1] == 0xbb && data[2] == 0xbf {
		return data[3:]
	}
	return data
}

func helperVersionMatchesTag(helperVersion string, tag string) bool {
	expected := strings.TrimPrefix(strings.TrimSpace(tag), "v")
	actual := strings.TrimSpace(helperVersion)
	if fields := strings.Fields(actual); len(fields) > 0 {
		actual = fields[0]
	}
	actual = strings.TrimPrefix(actual, "v")
	if expected == "" || actual == "" {
		return false
	}
	return strings.EqualFold(actual, expected)
}

func (b *Bridge) queueCompletedHelperUpgradeNoticeIfNeeded(ctx context.Context) (bool, error) {
	if b == nil || b.store == nil {
		return false, nil
	}
	req, ok, err := b.store.ReadUpgrade(ctx)
	if err != nil {
		return false, err
	}
	if !ok || req.Phase != teamstore.UpgradePhaseCompleted || strings.TrimSpace(req.Reason) != teamstore.HelperUpgradeReason {
		return false, nil
	}
	state, err := b.store.UpgradeBlockingStateSnapshot(ctx)
	if err != nil {
		return false, err
	}
	if state.Upgrade == nil || state.Upgrade.ID != req.ID {
		return false, nil
	}
	req = *state.Upgrade
	msg, ok := b.completedHelperUpgradeNoticeMessage(state, req)
	if !ok {
		return false, nil
	}
	if strings.TrimSpace(req.CompletionNoticeID) == "" {
		if existing, ok := existingHelperUpgradeCompletionOutbox(state, req, msg); ok {
			msg.ID = existing.ID
		}
	}
	if existing, exists := state.OutboxMessages[msg.ID]; exists {
		if strings.TrimSpace(req.CompletionNoticeID) == "" || req.CompletionNoticeAt.IsZero() {
			if _, err := b.store.MarkUpgradeCompletionNoticeQueued(ctx, req.ID, msg.ID); err != nil {
				return false, err
			}
		}
		if existing.Status != teamstore.OutboxStatusSent {
			if err := b.flushPendingOutboxForChat(ctx, existing.TeamsChatID); err != nil && b.out != nil {
				_, _ = fmt.Fprintf(b.out, "Teams best-effort outbox send error: %v\n", err)
			}
		}
		return true, nil
	}
	if helperUpgradeCompletionNoticeDurablyQueued(req) {
		return true, nil
	}
	queued, err := b.queueOutbox(ctx, msg)
	if err != nil {
		return false, err
	}
	if _, err := b.store.MarkUpgradeCompletionNoticeQueued(ctx, req.ID, queued.ID); err != nil {
		return false, err
	}
	if queued.Status != teamstore.OutboxStatusSent {
		if err := b.flushPendingOutboxForChat(ctx, queued.TeamsChatID); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams best-effort outbox send error: %v\n", err)
		}
	}
	return true, nil
}

func helperUpgradeCompletionNoticeDurablyQueued(req teamstore.UpgradeRequest) bool {
	return strings.TrimSpace(req.CompletionNoticeID) != "" && !req.CompletionNoticeAt.IsZero()
}

func (b *Bridge) completedHelperUpgradeNoticeMessage(state teamstore.State, req teamstore.UpgradeRequest) (teamstore.OutboxMessage, bool) {
	chatID, commandMessageID, manual := helperUpgradeCompletionTarget(req, firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return teamstore.OutboxMessage{}, false
	}
	tag := firstNonEmptyString(req.InstalledTag, state.AutoUpdate.LastInstalledTag)
	if strings.TrimSpace(tag) == "" {
		return teamstore.OutboxMessage{}, false
	}
	if strings.TrimSpace(b.helperVersion) != "" && !helperVersionMatchesTag(b.helperVersion, tag) {
		return teamstore.OutboxMessage{}, false
	}
	outboxID := strings.TrimSpace(req.CompletionNoticeID)
	if outboxID == "" {
		outboxID = helperUpgradeCompletionOutboxID(req, tag, chatID, commandMessageID)
	}
	msg := teamstore.OutboxMessage{
		ID:          outboxID,
		TeamsChatID: chatID,
		Kind:        "control-" + helperRestartNoticeActionUpgrade + "-complete",
		Body:        helperLifecycleCompletedNoticeBody(helperRestartNoticeActionUpgrade, tag),
	}
	if manual {
		msg.MentionOwner = true
		msg.NotificationKind = "helper_upgrade_completed"
	}
	return msg, true
}

func helperUpgradeCompletionOutboxID(req teamstore.UpgradeRequest, tag string, chatID string, commandMessageID string) string {
	seed := strings.Join([]string{
		strings.TrimSpace(req.ID),
		strings.TrimSpace(tag),
		strings.TrimSpace(chatID),
		strings.TrimSpace(commandMessageID),
	}, "\x00")
	return "outbox:control:helper-upgrade-complete:" + shortStableID(seed)
}

func existingHelperUpgradeCompletionOutbox(state teamstore.State, req teamstore.UpgradeRequest, expected teamstore.OutboxMessage) (teamstore.OutboxMessage, bool) {
	chatID := strings.TrimSpace(expected.TeamsChatID)
	if chatID == "" {
		return teamstore.OutboxMessage{}, false
	}
	expectedBody := strings.TrimSpace(expected.Body)
	var best teamstore.OutboxMessage
	found := false
	for _, outbox := range state.OutboxMessages {
		if strings.TrimSpace(outbox.TeamsChatID) != chatID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(outbox.Kind), "control-"+helperRestartNoticeActionUpgrade+"-complete") {
			continue
		}
		if expectedBody != "" && strings.TrimSpace(outbox.Body) != expectedBody {
			continue
		}
		if !req.CompletedAt.IsZero() && !outbox.CreatedAt.IsZero() && outbox.CreatedAt.Before(req.CompletedAt.Add(-10*time.Minute)) {
			continue
		}
		if !found || outbox.CreatedAt.After(best.CreatedAt) {
			best = outbox
			found = true
		}
	}
	return best, found
}

func helperLifecycleCompletedNoticeBody(action string, tag string) string {
	switch normalizedHelperRestartNoticeAction(action) {
	case helperRestartNoticeActionReload:
		return "✅ Helper reload completed\n\nThe Teams helper is back online and running the reloaded code. Send `st` to check status."
	case helperRestartNoticeActionUpgrade:
		lines := []string{"✅ Helper update completed"}
		if strings.TrimSpace(tag) != "" {
			lines = append(lines, "", "Version: `"+strings.TrimSpace(tag)+"`")
		}
		lines = append(lines, "", "The Teams helper is back online and running the updated code.")
		return strings.Join(lines, "\n")
	default:
		return "✅ Helper restart completed\n\nThe Teams helper is back online. Send `st` to check status."
	}
}

func (b *Bridge) helperActivationAttentionNoticeBody(notice helperRestartNotice, status helperActivationStatus, notificationKind string) string {
	title := "⚠️ Helper update activation needs attention"
	if notificationKind == helperUpgradeActivationFailedNotificationKind {
		title = "⚠️ Helper update activation failed"
	}
	lines := []string{title}
	if tag := strings.TrimSpace(notice.Tag); tag != "" {
		lines = append(lines, "", "Target: `"+tag+"`")
	}
	if running := strings.TrimSpace(b.helperVersion); running != "" {
		lines = append(lines, "", "Running helper: `"+running+"`")
	}
	if statusName := strings.TrimSpace(status.Status); statusName != "" {
		lines = append(lines, "", "Activation status: `"+statusName+"`")
	}
	if msg := sanitizeHelperActivationStatusMessage(notice, status); msg != "" {
		lines = append(lines, "", "Reason: "+msg)
	}
	lines = append(lines, "", "The helper may still be running the previous version. Run `cxp teams status` on that machine to check the owner, formal entry, and pending replacement versions.")
	return strings.Join(lines, "\n")
}

func sanitizeHelperActivationStatusMessage(notice helperRestartNotice, status helperActivationStatus) string {
	msg := strings.TrimSpace(status.Message)
	if msg == "" {
		return ""
	}
	for _, replacement := range []struct {
		raw   string
		label string
	}{
		{status.Source, "`<pending helper>`"},
		{notice.PendingReplacePath, "`<pending helper>`"},
		{status.Dest, "`<formal helper>`"},
		{notice.InstallPath, "`<formal helper>`"},
	} {
		raw := strings.TrimSpace(replacement.raw)
		if raw != "" {
			msg = strings.ReplaceAll(msg, raw, replacement.label)
		}
	}
	msg = strings.ReplaceAll(msg, "\r", " ")
	msg = strings.ReplaceAll(msg, "\n", " ")
	return strings.Join(strings.Fields(msg), " ")
}

func (b *Bridge) helperRestartBlockedMessage(ctx context.Context, force bool) (string, bool, error) {
	if err := b.ensureStore(); err != nil {
		return "", false, err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return "", false, err
	}
	if state.ServiceControl.Draining {
		now := time.Now()
		if teamstore.HelperUpgradeDrainExpired(state, now) {
			if message, blocked := b.expiredHelperUpgradeRestartBlockMessage(state, force, now); blocked {
				return message, true, nil
			}
			return "", false, nil
		}
		if teamstore.HelperReloadDrainStale(state, now, helperReloadDrainStaleAfter) {
			if message, blocked := b.staleHelperReloadLifecycleBlockMessage(state, force, "restart", now); blocked {
				return message, true, nil
			}
			return "", false, nil
		}
		return helperDrainBlockedMessage(state.ServiceControl, "restart"), true, nil
	}
	if !force && teamstore.HasUpgradeBlockingWork(state, time.Now()) {
		return strings.Join([]string{
			"⏳ Codex work is still active.",
			"",
			"I will not restart while work or Teams messages are still in progress.",
			"Wait for it to finish, then send `helper restart now`.",
			"",
			"Debug only: `helper restart force` may interrupt active work.",
		}, "\n"), true, nil
	}
	return "", false, nil
}

func (b *Bridge) expiredHelperUpgradeRestartBlockMessage(state teamstore.State, force bool, now time.Time) (string, bool) {
	owner, hasOwner := helperRestartStateOwner(state)
	if !hasOwner {
		return "", false
	}
	staleAfter := b.ownerStaleAfter
	if staleAfter <= 0 {
		staleAfter = 5 * time.Minute
	}
	ownerFresh := !teamstore.IsStale(owner, staleAfter, now) && !teamstore.OwnerAppearsLocallyDead(owner)
	if !ownerFresh {
		return "", false
	}
	if !teamstore.OwnerAppearsLocal(owner) {
		return strings.Join([]string{
			"⏳ Helper upgrade recovery is waiting for another machine.",
			"",
			"The helper upgrade drain has expired, but the active owner is still heartbeating on another machine.",
			"I will not restart this machine because that could create two Teams helpers using the same shared home.",
			"",
			"Owner: host=" + firstNonEmptyString(owner.Hostname, "unknown") + " version=" + firstNonEmptyString(owner.HelperVersion, "unknown"),
		}, "\n"), true
	}
	if strings.TrimSpace(owner.ActiveTurnID) != "" && !force {
		return strings.Join([]string{
			"⏳ Helper upgrade recovery is waiting for active Codex work.",
			"",
			"The helper upgrade drain has expired, but the current owner still reports an active turn.",
			"Wait for it to finish, or send `helper restart force` if you accept interrupting it.",
		}, "\n"), true
	}
	return "", false
}

func (b *Bridge) staleHelperReloadLifecycleBlockMessage(state teamstore.State, force bool, action string, now time.Time) (string, bool) {
	owner, hasOwner := helperRestartStateOwner(state)
	if !hasOwner {
		return "", false
	}
	staleAfter := b.ownerStaleAfter
	if staleAfter <= 0 {
		staleAfter = 5 * time.Minute
	}
	ownerFresh := !teamstore.IsStale(owner, staleAfter, now) && !teamstore.OwnerAppearsLocallyDead(owner)
	if !ownerFresh {
		return "", false
	}
	action = strings.TrimSpace(action)
	if action == "" {
		action = "restart"
	}
	if !teamstore.OwnerAppearsLocal(owner) {
		return strings.Join([]string{
			"⏳ Helper reload recovery is waiting for another machine.",
			"",
			"The previous helper reload drain is stale, but the active owner is still heartbeating on another machine.",
			"I will not " + action + " this machine because that could create two Teams helpers using the same shared home.",
			"",
			"Owner: host=" + firstNonEmptyString(owner.Hostname, "unknown") + " version=" + firstNonEmptyString(owner.HelperVersion, "unknown"),
		}, "\n"), true
	}
	if strings.TrimSpace(owner.ActiveTurnID) != "" && !force {
		return strings.Join([]string{
			"⏳ Helper reload recovery is waiting for active Codex work.",
			"",
			"The previous helper reload drain is stale, but the current owner still reports an active turn.",
			"Wait for it to finish, or send `helper " + action + " force` if you accept interrupting it.",
		}, "\n"), true
	}
	return "", false
}

func helperRestartStateOwner(state teamstore.State) (teamstore.OwnerMetadata, bool) {
	if state.ServiceOwner != nil {
		return *state.ServiceOwner, true
	}
	if state.LockOwner != nil {
		return *state.LockOwner, true
	}
	return teamstore.OwnerMetadata{}, false
}

func helperDrainBlockedMessage(control teamstore.ServiceControl, action string) string {
	switch control.Reason {
	case teamstore.HelperUpgradeReason:
		return strings.Join([]string{
			"⏳ Helper upgrade is already in progress.",
			"",
			"I will not start another " + action + " during upgrade.",
			"Wait for the upgrade to finish, then send `st`.",
		}, "\n")
	case teamstore.HelperReloadReason:
		return strings.Join([]string{
			"⏳ Helper reload is already in progress.",
			"",
			"I will not start another " + action + " during reload.",
			"Wait for the reload to finish, then send `st`.",
		}, "\n")
	case teamstore.CodexUpgradeReason:
		return strings.Join([]string{
			"⏳ Codex CLI upgrade is already in progress.",
			"",
			"I will not start another " + action + " during the Codex upgrade.",
			"Wait for the upgrade to finish, then send `st`.",
		}, "\n")
	default:
		if control.Reason != "" {
			return "⏳ Helper is busy: " + control.Reason + ".\n\nWait for it to finish, then send `st`."
		}
		return "⏳ Helper is busy.\n\nWait for it to finish, then send `st`."
	}
}

func (b *Bridge) runDelayedHelperRestart(restarter HelperRestarter) {
	delay := helperRestartDelay
	if delay > 0 {
		time.Sleep(delay)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := restarter(ctx); err != nil {
		_ = b.clearPendingHelperRestartNotice()
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper restart failed: %v\n", err)
		}
		_ = b.sendControl(context.Background(), "⚠️ Helper restart failed\n\n"+err.Error())
	}
}

func (b *Bridge) runDelayedHelperReload(reloader HelperReloader, opts HelperReloadOptions, previous teamstore.ServiceControl, msg ChatMessage) {
	delay := helperRestartDelay
	if delay > 0 {
		time.Sleep(delay)
	}
	var beforeRestartOnce sync.Once
	var markerWritten bool
	beforeRestart := func(ctx context.Context) error {
		var err error
		beforeRestartOnce.Do(func() {
			if writeErr := b.writePendingHelperReloadNotice(msg); writeErr != nil {
				err = writeErr
				return
			}
			markerWritten = true
			err = b.restoreHelperReloadDrain(ctx, previous)
		})
		return err
	}
	restoreDrainWithoutNotice := func(ctx context.Context) {
		_ = b.restoreHelperReloadDrain(ctx, previous)
	}
	opts.BeforeRestart = beforeRestart
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := reloader(ctx, opts); err != nil {
		if markerWritten {
			_ = b.clearPendingHelperRestartNotice()
		}
		restoreDrainWithoutNotice(context.Background())
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper reload failed: %v\n", err)
		}
		_ = b.sendControl(context.Background(), "⚠️ Helper reload failed\n\n"+err.Error())
		return
	}
	if !markerWritten {
		restoreDrainWithoutNotice(context.Background())
	}
}

func (b *Bridge) maybeRunHelperAutoUpdate(ctx context.Context, opts BridgeOptions) error {
	if opts.Once || opts.HelperAutoUpdater == nil || b.store == nil {
		return nil
	}
	now := time.Now()
	if !b.helperAutoUpdateProbeDue(now) {
		return nil
	}
	auto, control, err := b.store.ReadAutoUpdateControl(ctx)
	if err != nil {
		return err
	}
	if control.Draining && control.Reason == teamstore.HelperUpgradeReason && strings.TrimSpace(auto.CandidateTag) != "" {
		b.clearHelperAutoUpdateProbeGate()
		return b.applyHelperAutoUpdateWhenDrained(ctx, opts, HelperAutoUpdateCandidate{
			TagName:     auto.CandidateTag,
			Version:     auto.CandidateVersion,
			Priority:    auto.CandidatePriority,
			PublishedAt: auto.CandidatePublishedAt,
			EligibleAt:  auto.CandidateEligibleAt,
			Asset:       auto.CandidateAsset,
		})
	}
	if auto.BackoffUntil.After(now) {
		b.scheduleHelperAutoUpdateProbe(now, auto.BackoffUntil, auto.NextCheckAt)
		return nil
	}
	if auto.NextCheckAt.After(now) {
		b.scheduleHelperAutoUpdateProbe(now, auto.NextCheckAt)
		return nil
	}
	decision, err := opts.HelperAutoUpdater.Check(ctx, HelperAutoUpdateCheck{
		InstalledVersion:  opts.HelperVersion,
		Now:               now,
		IncludePrerelease: opts.HelperAutoUpdatePrerelease,
	})
	if err != nil {
		_, recordErr := b.store.RecordAutoUpdateCheck(ctx, teamstore.AutoUpdateRecord{
			Now:          now,
			NextCheckAt:  now.Add(30 * time.Minute),
			BackoffUntil: now.Add(30 * time.Minute),
			LastError:    err.Error(),
		})
		if recordErr != nil {
			return recordErr
		}
		b.scheduleHelperAutoUpdateProbe(now, now.Add(30*time.Minute))
		return err
	}
	record := teamstore.AutoUpdateRecord{
		Now:          now,
		NextCheckAt:  decision.NextCheckAt,
		BackoffUntil: decision.BackoffUntil,
		LastError:    decision.LastError,
	}
	if decision.Candidate != nil {
		record.CandidateTag = decision.Candidate.TagName
		record.CandidateVersion = decision.Candidate.Version
		record.CandidatePriority = decision.Candidate.Priority
		record.CandidateAsset = decision.Candidate.Asset
		record.CandidatePublishedAt = decision.Candidate.PublishedAt
		record.CandidateEligibleAt = decision.Candidate.EligibleAt
	}
	if record.NextCheckAt.IsZero() {
		record.NextCheckAt = now.Add(30 * time.Minute)
	}
	if _, err := b.store.RecordAutoUpdateCheck(ctx, record); err != nil {
		return err
	}
	if decision.Candidate == nil {
		b.scheduleHelperAutoUpdateProbe(now, record.BackoffUntil, record.NextCheckAt)
		return nil
	}
	b.clearHelperAutoUpdateProbeGate()
	return b.applyHelperAutoUpdateWhenDrained(ctx, opts, *decision.Candidate)
}

func (b *Bridge) applyHelperAutoUpdateWhenDrained(ctx context.Context, opts BridgeOptions, candidate HelperAutoUpdateCandidate) error {
	return b.applyHelperAutoUpdateWhenDrainedWithOptions(ctx, opts, candidate, helperAutoUpdateApplyOptions{})
}

func (b *Bridge) applyHelperAutoUpdateWhenDrainedWithOptions(ctx context.Context, opts BridgeOptions, candidate HelperAutoUpdateCandidate, applyOpts helperAutoUpdateApplyOptions) error {
	if opts.HelperAutoUpdater == nil {
		return nil
	}
	req, err := b.store.BeginUpgrade(ctx, teamstore.HelperUpgradeReason, 10*time.Minute)
	if err != nil {
		if errors.Is(err, teamstore.ErrUpgradeInProgress) {
			b.clearHelperAutoUpdateProbeGate()
			return nil
		}
		return err
	}
	b.clearHelperAutoUpdateProbeGate()
	if applyOpts.Manual {
		chatID := strings.TrimSpace(firstNonEmptyString(applyOpts.ControlChatID, b.reg.ControlChatID))
		if chatID != "" {
			target := teamstore.UpgradeNotificationTarget{
				TeamsChatID: chatID,
				TurnID:      strings.TrimSpace(applyOpts.CommandMessageID),
			}
			if target.TurnID == "" {
				target.TurnID = "manual-helper-upgrade"
			}
			updated, err := b.store.AddUpgradeNotificationTarget(ctx, req.ID, target)
			if err != nil {
				return err
			}
			req = updated
		}
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	if teamstore.HasUpgradeBlockingWork(state, time.Now()) {
		return nil
	}
	if _, err := b.store.MarkUpgradeReady(ctx, req.ID); err != nil {
		return err
	}
	if _, err := b.store.RecordAutoUpdateAttempt(ctx, candidate.TagName, time.Now()); err != nil {
		_, _ = b.store.AbortUpgrade(context.Background(), req.ID, err.Error())
		return err
	}
	applyCapabilities := HelperAutoUpdateApplyOptions{
		OwnsPendingReplacement: opts.HelperPendingRestarter != nil,
	}
	var res HelperAutoUpdateApplyResult
	if updater, ok := opts.HelperAutoUpdater.(HelperAutoUpdaterWithApplyOptions); ok {
		res, err = updater.ApplyWithOptions(ctx, candidate, applyCapabilities)
	} else {
		res, err = opts.HelperAutoUpdater.Apply(ctx, candidate)
	}
	if err != nil {
		_, _ = b.store.RecordAutoUpdateCheck(context.Background(), teamstore.AutoUpdateRecord{
			Now:                  time.Now(),
			NextCheckAt:          time.Now().Add(30 * time.Minute),
			BackoffUntil:         time.Now().Add(30 * time.Minute),
			LastError:            err.Error(),
			CandidateTag:         candidate.TagName,
			CandidateVersion:     candidate.Version,
			CandidatePriority:    candidate.Priority,
			CandidateAsset:       candidate.Asset,
			CandidatePublishedAt: candidate.PublishedAt,
			CandidateEligibleAt:  candidate.EligibleAt,
		})
		_, _ = b.store.AbortUpgrade(context.Background(), req.ID, err.Error())
		return err
	}
	tag := candidate.TagName
	if strings.TrimSpace(tag) == "" && strings.TrimSpace(res.Version) != "" {
		tag = "v" + strings.TrimPrefix(strings.TrimSpace(res.Version), "v")
	}
	if res.RestartRequired || strings.TrimSpace(res.PendingReplacePath) != "" {
		pendingReason := "helper update replacement is pending; waiting for the restarted helper to verify " + strings.TrimSpace(tag)
		_, _ = b.store.AbortUpgrade(context.Background(), req.ID, pendingReason)
		completionChatID, completionCommandID, manualNotice := helperUpgradeCompletionTarget(req, b.reg.ControlChatID)
		if completionChatID != "" {
			if err := b.writePendingHelperUpgradeNoticeWithReplacement(completionChatID, completionCommandID, tag, manualNotice, res.PendingReplacePath, res.InstallPath); err != nil {
				return err
			}
		}
		if opts.HelperPendingRestarter != nil && strings.TrimSpace(res.PendingReplacePath) != "" {
			if err := opts.HelperPendingRestarter(ctx, res.PendingReplacePath, res.InstallPath); err != nil {
				if completionChatID != "" {
					_ = b.clearPendingHelperRestartNotice()
				}
				return err
			}
			return nil
		}
		if opts.HelperRestarter == nil {
			return fmt.Errorf("%s", pendingReason)
		}
		if err := opts.HelperRestarter(ctx); err != nil {
			if completionChatID != "" {
				_ = b.clearPendingHelperRestartNotice()
			}
			return err
		}
		return nil
	}
	if res.ActivationPending {
		pendingReason := strings.TrimSpace(res.ActivationReason)
		if pendingReason == "" {
			pendingReason = "helper update activation is pending; waiting for the restarted helper to verify " + strings.TrimSpace(tag)
		}
		_, _ = b.store.AbortUpgrade(context.Background(), req.ID, pendingReason)
		completionChatID, completionCommandID, manualNotice := helperUpgradeCompletionTarget(req, b.reg.ControlChatID)
		if completionChatID != "" {
			if err := b.writePendingHelperUpgradeNotice(completionChatID, completionCommandID, tag, manualNotice); err != nil {
				return err
			}
		}
		return nil
	}
	if _, err := b.store.RecordAutoUpdateInstalled(ctx, tag, time.Now()); err != nil {
		_, _ = b.store.AbortUpgrade(context.Background(), req.ID, err.Error())
		return err
	}
	completed, err := b.store.CompleteUpgrade(ctx, req.ID, tag)
	if err != nil {
		return err
	}
	completionChatID, completionCommandID, manualNotice := helperUpgradeCompletionTarget(completed, b.reg.ControlChatID)
	if opts.HelperRestarter == nil {
		return nil
	}
	if completionChatID != "" {
		if err := b.writePendingHelperUpgradeNotice(completionChatID, completionCommandID, tag, manualNotice); err != nil {
			return err
		}
	}
	if err := opts.HelperRestarter(ctx); err != nil {
		if completionChatID != "" {
			_ = b.clearPendingHelperRestartNotice()
		}
		return err
	}
	return nil
}

func helperUpgradeCompletionTarget(req teamstore.UpgradeRequest, fallbackControlChatID string) (chatID string, commandMessageID string, manual bool) {
	for _, target := range req.NotificationTargets {
		if strings.TrimSpace(target.TeamsChatID) == "" {
			continue
		}
		return strings.TrimSpace(target.TeamsChatID), strings.TrimSpace(target.TurnID), true
	}
	return strings.TrimSpace(fallbackControlChatID), "", false
}

func (b *Bridge) requestCodexUpgradeAfterFailure(ctx context.Context, session *Session, turn teamstore.Turn, chatID string, cause error) error {
	if b == nil || b.store == nil {
		return nil
	}
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	notice := strings.Join([]string{
		"⚠️ Codex CLI needs an update",
		"",
		"Codex rejected this request because the installed CLI is too old for the selected model.",
		"",
		"I will upgrade Codex after current Codex requests finish. I will not retry this request automatically.",
		"After the upgrade finishes, send `helper retry last` here.",
	}, "\n")
	if b.codexUpgrader == nil {
		notice = strings.Join([]string{
			"⚠️ Codex CLI needs an update",
			"",
			"Codex rejected this request because the installed CLI is too old for the selected model.",
			"",
			"Automatic Codex upgrade isn't enabled here, so an administrator needs to update Codex — no action needed from you. Once it's updated, send `helper retry last` here.",
		}, "\n")
	}
	noticeMsg := teamstore.OutboxMessage{
		ID:                 "outbox:" + turn.ID + ":codex-upgrade-required",
		SessionID:          sessionID,
		TurnID:             turn.ID,
		TeamsChatID:        chatID,
		Kind:               "error",
		Body:               notice + "\n\nOriginal error:\n" + trimTeamsCommandOutput(cause.Error(), 600),
		UpgradeNonBlocking: true,
	}
	if err := b.queueAndBestEffortSendOutbox(ctx, noticeMsg); err != nil {
		return err
	}
	if b.codexUpgrader == nil {
		return nil
	}
	req, err := b.store.BeginUpgrade(ctx, teamstore.CodexUpgradeReason, 30*time.Minute)
	if err != nil && !errors.Is(err, teamstore.ErrUpgradeInProgress) {
		return err
	}
	b.clearPendingCodexUpgradeProbeGate()
	if err == nil {
		if _, targetErr := b.store.AddUpgradeNotificationTarget(ctx, req.ID, teamstore.UpgradeNotificationTarget{
			SessionID:   sessionID,
			TurnID:      turn.ID,
			TeamsChatID: chatID,
		}); targetErr != nil {
			return targetErr
		}
	}
	b.boostPolling(time.Now())
	return nil
}

func (b *Bridge) maybeRunPendingCodexUpgrade(ctx context.Context) error {
	if b == nil || b.store == nil || b.codexUpgrader == nil {
		return nil
	}
	now := time.Now()
	if !b.pendingCodexUpgradeProbeDue(now) {
		return nil
	}
	req, ok, err := b.store.ReadUpgrade(ctx)
	if err != nil {
		return err
	}
	if !ok || req.ID == "" || req.Reason != teamstore.CodexUpgradeReason {
		b.schedulePendingCodexUpgradeProbe(now, pendingCodexUpgradeStateRefreshInterval)
		return nil
	}
	switch req.Phase {
	case teamstore.UpgradePhaseCompleted, teamstore.UpgradePhaseAborted:
		b.schedulePendingCodexUpgradeProbe(now, pendingCodexUpgradeStateRefreshInterval)
		return nil
	}
	blockingState, err := b.store.UpgradeBlockingStateSnapshot(ctx)
	if err != nil {
		return err
	}
	if teamstore.HasUpgradeBlockingWork(blockingState, now) {
		b.schedulePendingCodexUpgradeProbe(now, pendingUpgradeBlockedRetryInterval)
		return nil
	}
	if req.Phase == teamstore.UpgradePhaseDraining {
		updated, err := b.store.MarkUpgradeReady(ctx, req.ID)
		if err != nil {
			return err
		}
		req = updated
	}
	result, err := b.codexUpgrader(ctx)
	if err != nil {
		_, _ = b.store.AbortUpgrade(context.Background(), req.ID, err.Error())
		b.schedulePendingCodexUpgradeProbe(now, pendingCodexUpgradeStateRefreshInterval)
		_ = b.sendControl(context.Background(), "⚠️ Codex CLI upgrade failed\n\n"+err.Error())
		if targetErr := b.notifyCodexUpgradeTargets(context.Background(), req.NotificationTargets, false, "", err); targetErr != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams Codex upgrade target notification error: %v\n", targetErr)
		}
		return err
	}
	completed, err := b.store.CompleteUpgrade(ctx, req.ID)
	if err != nil {
		return err
	}
	b.schedulePendingCodexUpgradeProbe(now, pendingCodexUpgradeStateRefreshInterval)
	body := "✅ Codex CLI upgraded."
	if strings.TrimSpace(result.Path) != "" {
		body += "\n\nPath: `" + strings.TrimSpace(result.Path) + "`"
	}
	body += "\n\nRetry the failed Work chat request with `helper retry last`."
	controlErr := b.sendControl(ctx, body)
	targetErr := b.notifyCodexUpgradeTargets(ctx, completed.NotificationTargets, true, result.Path, nil)
	if controlErr != nil {
		return controlErr
	}
	return targetErr
}

func (b *Bridge) notifyCodexUpgradeTargets(ctx context.Context, targets []teamstore.UpgradeNotificationTarget, succeeded bool, path string, cause error) error {
	seen := map[string]bool{}
	var firstErr error
	for _, target := range targets {
		chatID := strings.TrimSpace(target.TeamsChatID)
		if chatID == "" || chatID == strings.TrimSpace(b.reg.ControlChatID) {
			continue
		}
		key := chatID + "\x00" + strings.TrimSpace(target.TurnID)
		if seen[key] {
			continue
		}
		seen[key] = true
		body := codexUpgradeTargetNoticeBody(succeeded, path, cause)
		idSeed := firstNonEmptyString(strings.TrimSpace(target.TurnID), strings.TrimSpace(target.SessionID), chatID)
		msg := teamstore.OutboxMessage{
			ID:                 "outbox:codex-upgrade-target:" + shortStableID(idSeed) + ":" + fmt.Sprintf("%t", succeeded),
			SessionID:          strings.TrimSpace(target.SessionID),
			TurnID:             strings.TrimSpace(target.TurnID),
			TeamsChatID:        chatID,
			Kind:               "helper",
			Body:               body,
			UpgradeNonBlocking: true,
		}
		if err := b.queueAndBestEffortSendOutbox(ctx, msg); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func codexUpgradeTargetNoticeBody(succeeded bool, path string, cause error) string {
	if succeeded {
		body := "✅ Codex CLI upgraded.\n\nRetry this request with `helper retry last`."
		if strings.TrimSpace(path) != "" {
			body += "\n\nPath: `" + strings.TrimSpace(path) + "`"
		}
		return body
	}
	body := "⚠️ Codex CLI upgrade failed.\n\nI could not upgrade Codex automatically; an administrator needs to update it — no action needed from you. Once it's updated, send `helper retry last` here."
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		body += "\n\nError:\n" + trimTeamsCommandOutput(cause.Error(), 600)
	}
	return body
}

// infraLaunchFailureNotice returns a user-facing, non-blaming, Teams-actionable
// message for an infrastructure/setup launch failure (config or version
// mismatch, missing native binary, patch unavailable, transport not
// configured). It is classified structurally via the codexrunner ErrorLaunch
// kind, so it does not depend on fragile error-text matching. The second return
// is false for ordinary codex/content failures, which keep their own text.
func infraLaunchFailureNotice(err error) (string, bool) {
	if !codexrunner.IsKind(err, codexrunner.ErrorLaunch) {
		return "", false
	}
	return strings.Join([]string{
		"⚠️ I couldn't start Codex due to a setup issue on my side — this is not a problem with your request.",
		"",
		"This usually means the Codex CLI or my own configuration needs attention (for example a version mismatch after an update). I did not retry automatically, to avoid duplicating work.",
		"Once it's sorted out, send `helper retry last` here.",
		"",
		"Diagnostic for the admin:",
		trimTeamsCommandOutput(err.Error(), 600),
	}, "\n"), true
}

func codexErrorRequiresUpgrade(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	if !strings.Contains(text, "codex") {
		return false
	}
	if strings.Contains(text, "requires a newer version") ||
		strings.Contains(text, "newer version of codex") ||
		strings.Contains(text, "upgrade to the latest") ||
		strings.Contains(text, "latest app or cli") ||
		(strings.Contains(text, "model") && strings.Contains(text, "upgrade") && strings.Contains(text, "cli")) {
		return true
	}
	return false
}

func isWorkOnlyHelperCommand(text string) bool {
	name, _, syntax, ok := splitDashboardCommand(text)
	if !ok || syntax != dashboardCommandSyntaxHelp {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "file", "image", "send-file", "send-image", "retry", "restore-thread", "restore", "cancel", "close", "rename", "publish-history", "sync-history", "import-history", "stats", "usage", "tokens":
		return true
	default:
		return false
	}
}

func looksLikeControlPath(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" || strings.ContainsAny(text, "\r\n") {
		return false
	}
	switch {
	case strings.HasPrefix(text, "./"), strings.HasPrefix(text, "../"), strings.HasPrefix(text, "~/"):
		return true
	case len(text) >= 3 && ((text[0] >= 'A' && text[0] <= 'Z') || (text[0] >= 'a' && text[0] <= 'z')) && text[1] == ':' && (text[2] == '\\' || text[2] == '/'):
		return true
	default:
		return false
	}
}

func controlPathHintMessage(text string) string {
	path := strings.TrimSpace(text)
	quoted := quoteTeamsCommandPath(path)
	return fmt.Sprintf("📁 Detected path: `%s`\n\nChoose one:\n\n- `new %s` - create a 💬 Work chat there\n- `mkdir %s` - only create the directory", path, quoted, quoted)
}

func quoteTeamsCommandPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return path
	}
	if strings.ContainsAny(path, " \t\"'") {
		return strconv.Quote(path)
	}
	return path
}

func unquoteTeamsCommandPath(path string) (string, bool) {
	path = strings.TrimSpace(path)
	if len(path) < 2 || path[0] != '"' {
		return "", false
	}
	unquoted, err := strconv.Unquote(path)
	if err != nil {
		return "", false
	}
	return unquoted, true
}

func unknownWorkCommandMessage(text string) string {
	name, _, _, _ := splitDashboardCommand(text)
	name = strings.TrimSpace(name)
	if name == "" {
		return sessionHelpText()
	}
	return fmt.Sprintf("unknown work chat command: `%s`\n\n%s", name, sessionHelpText())
}

func (b *Bridge) effectiveControlFallbackExecutor() Executor {
	if b.controlFallbackExecutor != nil {
		return b.controlFallbackExecutor
	}
	args := []string{"-c", CodexReasoningEffortConfigArg(DefaultControlFallbackReasoningEffort)}
	if model := b.effectiveControlFallbackModel(); model != "" {
		args = append(args, "--model", model)
	}
	return CodexExecutor{ExtraArgs: args}
}

func (b *Bridge) effectiveControlFallbackModel() string {
	if model := strings.TrimSpace(b.controlFallbackModel); model != "" {
		return model
	}
	return DefaultControlFallbackModel
}

func (b *Bridge) controlFallbackCodexPrompt(ctx context.Context, prompt string) string {
	return b.controlFallbackCodexPromptForMessage(ctx, prompt, "")
}

func (b *Bridge) controlFallbackCodexPromptForMessage(ctx context.Context, prompt string, excludeMessageID string) string {
	return ControlFallbackCodexPromptWithContext(prompt, b.controlFallbackPromptContext(ctx, excludeMessageID))
}

func (b *Bridge) controlFallbackPromptContext(ctx context.Context, excludeMessageID string) ControlFallbackPromptContext {
	if b == nil {
		return ControlFallbackPromptContext{}
	}
	active := make([]string, 0, len(b.reg.Sessions))
	for _, session := range b.reg.ActiveSessions() {
		if session.ID == controlFallbackSessionID {
			continue
		}
		active = append(active, controlFallbackSessionLine(session))
	}
	historyPath, recentHistory := b.controlChatHistoryPromptContext(excludeMessageID)
	return ControlFallbackPromptContext{
		HelperVersion:        strings.TrimSpace(b.helperVersion),
		ControlChatTitle:     strings.TrimSpace(b.reg.ControlChatTopic),
		ControlChatID:        strings.TrimSpace(b.reg.ControlChatID),
		ActiveWorkChats:      active,
		CurrentDashboard:     b.controlFallbackDashboardSummary(ctx),
		HelperHelpContext:    b.controlFallbackHelpContext,
		ControlHistoryPath:   historyPath,
		RecentControlHistory: recentHistory,
	}
}

func controlFallbackSessionLine(session Session) string {
	parts := []string{"`" + strings.TrimSpace(session.ID) + "`"}
	if title := strings.TrimSpace(firstNonEmptyString(session.UserTitle, session.Topic)); title != "" {
		parts = append(parts, title)
	}
	if cwd := strings.TrimSpace(session.Cwd); cwd != "" {
		parts = append(parts, "cwd=`"+cwd+"`")
	}
	if chatID := strings.TrimSpace(session.ChatID); chatID != "" {
		parts = append(parts, "chat=`"+chatID+"`")
	}
	return strings.Join(parts, " - ")
}

func (b *Bridge) controlFallbackDashboardSummary(ctx context.Context) string {
	if b == nil || b.store == nil {
		return ""
	}
	dashboard := b.previousControlDashboard(ctx)
	if dashboard.CurrentView.Kind == DashboardViewNone || len(dashboard.CurrentView.Items) == 0 {
		return ""
	}
	var lines []string
	lines = append(lines, "- view: `"+string(dashboard.CurrentView.Kind)+"`")
	if dashboard.SelectedWorkspaceID != "" {
		lines = append(lines, "- selected_workspace_id: `"+dashboard.SelectedWorkspaceID+"`")
	}
	lines = append(lines, "- visible_items:")
	for _, item := range dashboard.CurrentView.Items {
		if len(lines) >= 12 {
			lines = append(lines, "  - ... more items omitted")
			break
		}
		label := strings.TrimSpace(item.DisplayTitle)
		if label == "" {
			label = firstNonEmptyString(item.SessionID, item.WorkspaceID)
		}
		lines = append(lines, fmt.Sprintf("  - `%d` %s `%s`", item.Number, item.Kind, label))
	}
	return strings.Join(lines, "\n")
}

func (b *Bridge) queueControlFallbackAck(ctx context.Context, session *Session, turn teamstore.Turn) error {
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + turn.ID + ":control-ack",
		SessionID:   session.ID,
		TurnID:      turn.ID,
		TeamsChatID: session.ChatID,
		Kind:        "ack",
		AckKind:     "control_prompt",
		Body:        "❓ **Codex received your control-chat question.**\n\nThis message is not a helper command, so **Codex will answer it here after helper-local preparation succeeds**. The request has been accepted; attachments or Teams voice/video may still be processed before Codex starts.\n\nTo stop this control-chat request, send `helper cancel last` here. For project work, send `new <directory>`, then send the task inside the new 💬 Work chat.",
	})
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	if err := b.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true}); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams control ACK send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) createSession(ctx context.Context, msg ChatMessage, request string) error {
	if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
		return err
	} else if blocked {
		if serviceControlDefersInput(control) {
			inbound, _, err := b.persistControlInboundWithStatus(ctx, msg, teamstore.InboundStatusDeferred, "teams_control_new")
			if err != nil {
				return err
			}
			return b.sendDeferredServiceControlNotice(ctx, b.reg.ControlChatID, inbound, control)
		}
		return b.sendControl(ctx, serviceControlBlockedMessage(control, "new sessions"))
	}
	if duplicate, err := b.controlCommandAlreadyHandled(ctx, msg, "teams_control_new"); err != nil {
		return err
	} else if duplicate {
		return b.sendControl(ctx, "I already handled this `new` request. Send `st` to see current Work chats, or send a fresh `new <directory>` message to create another one.")
	}
	parsed, err := b.parseNewSessionRequest(ctx, request)
	if err != nil {
		return b.sendControl(ctx, err.Error())
	}
	if err := b.validateBeaconNewSession(parsed); err != nil {
		return b.sendControl(ctx, err.Error())
	}
	if parsed.WorkDir != "" {
		if err := os.MkdirAll(parsed.WorkDir, 0o700); err != nil {
			return b.sendControl(ctx, "create workspace failed: "+err.Error())
		}
	}
	now := time.Now()
	sessionID := b.reg.NextSessionID()
	topic := WorkChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		SessionID:    sessionID,
		Topic:        NewWorkChatPlaceholderTitle(parsed.WorkDir),
		Cwd:          parsed.WorkDir,
	})
	chat, err := b.createMeetingChat(ctx, topic)
	if err != nil {
		return err
	}
	session := Session{
		ID:           sessionID,
		ChatID:       chat.ID,
		ChatURL:      chat.WebURL,
		Topic:        chat.Topic,
		TitleSource:  sessionTitleSourceAuto,
		Status:       "active",
		Cwd:          parsed.WorkDir,
		ModelProfile: parsed.ModelProfileSnapshot,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	b.reg.Sessions = append(b.reg.Sessions, session)
	if err := b.ensureDurableSession(ctx, &session); err != nil {
		return err
	}
	if err := b.activateBeaconNewSession(session.ID, parsed); err != nil {
		return err
	}
	if err := b.sendChatCreatedMention(ctx, session.ID, chat.ID, workChatCreatedNotice(session)); err != nil {
		return err
	}
	if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + session.ID + ":anchor",
		SessionID:   session.ID,
		TeamsChatID: chat.ID,
		Kind:        "anchor",
		Body:        sessionReadyMessage(session, parsed.Title, parsed.modelProfileTargetLabel(), parsed.beaconTargetLabel()),
	}); err != nil {
		return err
	}
	return b.sendControl(ctx, fmt.Sprintf("✅ Work chat created: %s\n\nOpen this Teams link and send your task there:\n%s\n\nIf Teams does not show it right away, search for: %s", session.ID, session.ChatURL, session.ID))
}

func (b *Bridge) controlCommandAlreadyHandled(ctx context.Context, msg ChatMessage, source string) (bool, error) {
	if b == nil || b.store == nil || strings.TrimSpace(msg.ID) == "" || strings.TrimSpace(b.reg.ControlChatID) == "" {
		return false, nil
	}
	inbound, created, err := b.persistControlInboundWithStatus(ctx, msg, teamstore.InboundStatusPersisted, source)
	if err != nil {
		return false, err
	}
	if created {
		return false, nil
	}
	if inbound.Status == teamstore.InboundStatusDeferred {
		return false, nil
	}
	return true, nil
}

type newSessionRequest struct {
	WorkDir              string
	Title                string
	BeaconProfile        string
	BeaconIsolation      string
	ModelProfile         string
	ModelProfileSnapshot modelprofile.Snapshot
}

func (r newSessionRequest) beaconTargetLabel() string {
	if strings.TrimSpace(r.BeaconProfile) == "" {
		return ""
	}
	label := "beacon:" + strings.TrimSpace(r.BeaconProfile)
	if strings.TrimSpace(r.BeaconIsolation) != "" {
		label += " isolation=" + strings.TrimSpace(r.BeaconIsolation)
	}
	return label
}

func (r newSessionRequest) modelProfileTargetLabel() string {
	if r.ModelProfileSnapshot.IsZero() || r.ModelProfileSnapshot.IsDefault() {
		return ""
	}
	return "model:" + modelProfileDisplayName(r.ModelProfileSnapshot)
}

func (b *Bridge) parseNewSessionRequest(ctx context.Context, raw string) (newSessionRequest, error) {
	parsed, err := parseNewSessionRequest(raw)
	if err != nil {
		return newSessionRequest{}, err
	}
	if parsed.WorkDir == "" {
		if workspace, ok := b.currentControlWorkspace(ctx); ok {
			parsed.WorkDir = workspace.Path
		}
	}
	if parsed.WorkDir == "" {
		return newSessionRequest{}, fmt.Errorf("usage: `new <directory>`; after opening a workspace from `projects`, you can also send `new`")
	}
	snapshot, err := b.resolveNewSessionModelProfile(ctx, parsed.ModelProfile)
	if err != nil {
		return newSessionRequest{}, err
	}
	parsed.ModelProfileSnapshot = snapshot
	return parsed, nil
}

func parseNewSessionRequest(raw string) (newSessionRequest, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return newSessionRequest{}, nil
	}
	before, after, hasSep := strings.Cut(raw, " -- ")
	if !hasSep {
		cleaned, profile, isolation, err := parseBeaconOptionsFromNewSession(raw)
		if err != nil {
			return newSessionRequest{}, err
		}
		cleaned, modelProfile, err := parseModelProfileOptionsFromNewSession(cleaned)
		if err != nil {
			return newSessionRequest{}, err
		}
		if strings.TrimSpace(cleaned) == "" {
			return newSessionRequest{BeaconProfile: profile, BeaconIsolation: isolation, ModelProfile: modelProfile}, nil
		}
		resolved, err := resolveUserWorkspacePath(cleaned)
		if err != nil {
			return newSessionRequest{}, err
		}
		return newSessionRequest{WorkDir: resolved, BeaconProfile: profile, BeaconIsolation: isolation, ModelProfile: modelProfile}, nil
	}
	dir, profile, isolation, err := parseBeaconOptionsFromNewSession(before)
	if err != nil {
		return newSessionRequest{}, err
	}
	dir, modelProfile, err := parseModelProfileOptionsFromNewSession(dir)
	if err != nil {
		return newSessionRequest{}, err
	}
	dir = strings.TrimSpace(dir)
	title := strings.TrimSpace(after)
	if dir == "" {
		return newSessionRequest{Title: title, BeaconProfile: profile, BeaconIsolation: isolation, ModelProfile: modelProfile}, nil
	}
	resolved, err := resolveUserWorkspacePath(dir)
	if err != nil {
		return newSessionRequest{}, err
	}
	return newSessionRequest{WorkDir: resolved, Title: title, BeaconProfile: profile, BeaconIsolation: isolation, ModelProfile: modelProfile}, nil
}

func parseModelProfileOptionsFromNewSession(raw string) (string, string, error) {
	if !strings.Contains(raw, "--model-profile") && !strings.Contains(raw, "--modelprofile") && !strings.Contains(raw, "--mp") && !strings.Contains(raw, "--model") {
		return raw, "", nil
	}
	words := strings.Fields(raw)
	var keep []string
	var profile string
	for i := 0; i < len(words); i++ {
		switch strings.ToLower(strings.TrimSpace(words[i])) {
		case "--model-profile", "--modelprofile", "--mp", "--model":
			if i+1 >= len(words) {
				return "", "", fmt.Errorf("%s requires a model profile name", words[i])
			}
			i++
			profile = strings.TrimSpace(words[i])
		default:
			keep = append(keep, words[i])
		}
	}
	return strings.Join(keep, " "), profile, nil
}

func (b *Bridge) resolveNewSessionModelProfile(ctx context.Context, ref string) (modelprofile.Snapshot, error) {
	ref = strings.TrimSpace(ref)
	if choice, ok := modelprofile.LookupModelChoice(ref); ok {
		ref = choice.RecommendedProfile
	}
	if b == nil || b.modelProfileResolver == nil {
		if ref == "" {
			return modelprofile.Snapshot{}, nil
		}
		return modelprofile.Snapshot{}, fmt.Errorf("cannot create model-profile work chat: model profile resolver is not configured")
	}
	snapshot, err := b.modelProfileResolver(ctx, ref)
	if err != nil {
		if ref == "" {
			return modelprofile.Snapshot{}, fmt.Errorf("cannot resolve default model profile: %w", err)
		}
		return modelprofile.Snapshot{}, fmt.Errorf("cannot create model-profile work chat for %q: %w", ref, err)
	}
	return snapshot, nil
}

func modelProfileDisplayName(snapshot modelprofile.Snapshot) string {
	if snapshot.IsZero() || snapshot.IsDefault() {
		return "default"
	}
	name := strings.TrimSpace(snapshot.Name)
	if name == "" {
		name = strings.TrimSpace(snapshot.Provider)
	}
	provider := strings.TrimSpace(snapshot.Provider)
	if provider == "" || strings.EqualFold(provider, name) {
		provider = ""
	}
	label := name
	if provider != "" {
		label += " (" + provider + ")"
	}
	if model := strings.TrimSpace(snapshot.Model); model != "" {
		label += " model " + model
	}
	if snapshot.Revision > 0 {
		label += fmt.Sprintf(" rev %d", snapshot.Revision)
	}
	return label
}

func (b *Bridge) currentControlWorkspace(ctx context.Context) (teamstore.WorkspaceRecord, bool) {
	if b == nil || b.store == nil {
		return teamstore.WorkspaceRecord{}, false
	}
	view, ok, err := b.loadDashboardView(ctx)
	if err != nil || !ok || strings.TrimSpace(view.WorkspaceID) == "" {
		return teamstore.WorkspaceRecord{}, false
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return teamstore.WorkspaceRecord{}, false
	}
	record, ok := state.Workspaces[view.WorkspaceID]
	if !ok || strings.TrimSpace(record.Path) == "" {
		return teamstore.WorkspaceRecord{}, false
	}
	return record, true
}

func resolveUserWorkspacePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if unquoted, ok := unquoteTeamsCommandPath(raw); ok {
		raw = unquoted
	}
	if raw == "" {
		return "", fmt.Errorf("workspace path is required")
	}
	if strings.ContainsRune(raw, 0) {
		return "", fmt.Errorf("workspace path contains NUL byte")
	}
	if strings.HasPrefix(raw, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if raw == "~" {
			raw = home
		} else if strings.HasPrefix(raw, "~/") {
			raw = filepath.Join(home, strings.TrimPrefix(raw, "~/"))
		}
	}
	raw = os.ExpandEnv(raw)
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func (b *Bridge) createWorkspaceDirectory(ctx context.Context, raw string) error {
	dir, err := resolveUserWorkspacePath(raw)
	if err != nil {
		return b.sendControl(ctx, "usage: `mkdir <directory>`")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return b.sendControl(ctx, "create workspace failed: "+err.Error())
	}
	return b.sendControl(ctx, "Directory is ready: "+dir+"\n\nNext: send `new "+quoteTeamsCommandPath(dir)+"` to create a work chat for this directory.")
}

func sessionReadyMessage(session Session, prompt string, target ...string) string {
	var lines []string
	lines = append(lines, "💬 Work chat is ready.")
	lines = append(lines, "Send a task in this chat. Codex will start automatically and continue this session.")
	lines = append(lines, "Status: waiting for your first task.")
	if strings.TrimSpace(session.ID) != "" {
		lines = append(lines, "Session: "+session.ID)
	}
	lines = append(lines, "Project: "+sessionReadyProjectLabel(session))
	if cwd := strings.TrimSpace(session.Cwd); cwd != "" {
		lines = append(lines, "Working directory: "+cwd)
	}
	for _, item := range target {
		if strings.TrimSpace(item) != "" {
			lines = append(lines, "Execution target: "+strings.TrimSpace(item))
		}
	}
	lines = append(lines, "Commands: `helper status` or `!status`, `helper stats`, `helper help`, `helper close` to close this Codex session in Teams.")
	lines = append(lines, "Need more details? Send `helper status`.")
	return strings.Join(lines, "\n")
}

func workChatCreatedNotice(session Session) string {
	return workChatLifecycleNotice("Work chat created", session)
}

func workChatLifecycleNotice(status string, session Session) string {
	status = strings.TrimSpace(status)
	if status == "" {
		status = "Work chat created"
	}
	sessionID := strings.TrimSpace(session.ID)
	lines := []string{status + "."}
	if sessionID != "" {
		lines[0] = status + ": " + sessionID + "."
	}
	if cwd := strings.TrimSpace(session.Cwd); cwd != "" {
		lines = append(lines, "Working directory: "+cwd)
	}
	return strings.Join(lines, "\n")
}

func sessionReadyProjectLabel(session Session) string {
	label := DashboardDisplayTitle("", "", session.Cwd)
	if label == "" || label == "untitled" {
		label = DashboardDisplayTitle("", session.Topic, "")
	}
	if label == "" || label == "untitled" {
		label = "helper default directory"
	}
	return shortenTeamsLine(label, 72)
}

func teamsPromptPreview(prompt string) string {
	prompt = strings.TrimSpace(StripHelperPromptEchoes(StripArtifactManifestBlocks(prompt)))
	if prompt == "" {
		return "your task"
	}
	fields := strings.Fields(prompt)
	if len(fields) == 0 {
		return "your task"
	}
	preview := strings.ReplaceAll(strings.Join(fields, " "), "`", "'")
	return `"` + shortenTeamsLine(preview, 80) + `"`
}

func machineLabel() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "local"
	}
	return host
}

func sanitizeMachineHostnameOverride(raw string) string {
	return SanitizeDashboardTitle(raw)
}

func applyMachineHostnameOverrideToRecord(machine *teamstore.MachineRecord, raw string) bool {
	if machine == nil {
		return false
	}
	label := sanitizeMachineHostnameOverride(raw)
	if label == "" {
		return false
	}
	changed := machine.Label != label || machine.Hostname != label
	machine.Label = label
	machine.Hostname = label
	return changed
}

func (b *Bridge) applyRegistryMachineHostnameOverride() bool {
	if b == nil {
		return false
	}
	return applyMachineHostnameOverrideToRecord(&b.machine, b.reg.MachineHostnameOverride)
}

func (b *Bridge) restoreMachineHostnameOverrideFromStore(ctx context.Context) error {
	if b == nil || b.store == nil {
		return nil
	}
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	if b.machine.ID == "" {
		b.machine = MachineRecordForUser(b.user, b.scope)
	}
	if sanitizeMachineHostnameOverride(b.reg.MachineHostnameOverride) != "" {
		b.applyRegistryMachineHostnameOverride()
		return nil
	}
	if sanitizeMachineHostnameOverride(os.Getenv(envTeamsMachineLabel)) != "" {
		return nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(state.MachineIdentity.ID) == "" || strings.TrimSpace(state.MachineIdentity.ID) != strings.TrimSpace(b.machine.ID) {
		return nil
	}
	storedLabel := sanitizeMachineHostnameOverride(state.MachineIdentity.Label)
	currentLabel := sanitizeMachineHostnameOverride(b.machine.Label)
	if storedLabel == "" || storedLabel == currentLabel {
		return nil
	}
	applyMachineHostnameOverrideToRecord(&b.machine, storedLabel)
	return nil
}

func (b *Bridge) handleSessionMessage(ctx context.Context, chatID string, msg ChatMessage, text string) error {
	return b.handleSessionMessageWithQueueState(ctx, chatID, msg, text, nil, nil)
}

func (b *Bridge) handleSessionMessageWithQueueState(ctx context.Context, chatID string, msg ChatMessage, text string, knownTurns *sessionTurnQueueState, knownQueueSnapshot *teamstore.State) error {
	session := b.reg.SessionByChatID(chatID)
	if session == nil {
		return nil
	}
	if isPromptlessTeamsAttachmentPlaceholderMessage(msg) {
		b.reg.MarkSeen(chatID, msg.ID)
		return nil
	}
	routeText := commandRouteTextFromTeamsMessage(msg, text)
	if message := modelAPIKeyPreflightMessage(routeText); message != "" {
		return b.sendToChat(ctx, chatID, message)
	}
	if parsed := ParseDashboardCommand(ChatScopeWork, routeText); parsed.HelperCommand {
		if !messageAuthoredByCurrentUser(msg, b.user) {
			return b.rejectExternalWorkCommand(ctx, session, msg)
		}
		switch parsed.Name {
		case DashboardCommandClose:
			session.Status = "closed"
			session.UpdatedAt = time.Now()
			if err := b.closeDurableSession(ctx, session); err != nil {
				return err
			}
			return b.sendToChat(ctx, chatID, "Session closed. The helper will no longer read or respond in this Teams chat.\n\nThis chat remains visible in Teams. Use the 🏠 control chat to open or create another work chat.")
		case DashboardCommandStatus:
			return b.sendToChat(ctx, chatID, b.formatWorkSessionStatus(session))
		case DashboardCommandStats:
			return b.sendStatsToChat(ctx, chatID, b.formatWorkSessionStats(ctx, session))
		case DashboardCommandRetry:
			if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
				return err
			} else if blocked {
				return b.rejectSessionWork(ctx, session, msg, control)
			}
			return b.retryTurnCommand(ctx, session, strings.TrimSpace(parsed.Argument))
		case DashboardCommandRestoreThread:
			return b.restoreThreadCommand(ctx, session, strings.TrimSpace(parsed.Argument))
		case DashboardCommandCancel:
			return b.cancelTurnCommand(ctx, session, strings.TrimSpace(parsed.Argument))
		case DashboardCommandSendFile:
			if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
				return err
			} else if blocked {
				return b.rejectSessionWork(ctx, session, msg, control)
			}
			return b.sendFileCommand(ctx, session, strings.TrimSpace(parsed.Argument))
		case DashboardCommandRename:
			if hostname, ok := parseRenameHostnameArgument(parsed.Argument); ok {
				return b.renameMachineHostname(ctx, hostname, chatID)
			}
			return b.renameSessionChat(ctx, session, strings.TrimSpace(parsed.Argument))
		case DashboardCommandDetails:
			return b.sendToChat(ctx, chatID, b.formatSessionDetails(session))
		case DashboardCommandPublishHistory:
			return b.publishWorkSessionHistory(ctx, session)
		case DashboardCommandSkills:
			return b.handleSkillsCommandFromMessage(ctx, chatID, msg, parsed.Argument)
		case DashboardCommandBeacon:
			return b.handleBeaconWorkCommand(ctx, session, msg, parsed.Argument)
		case DashboardCommandModel:
			message, err := b.handleModelWorkCommand(ctx, session, parsed.Argument)
			if err != nil {
				return b.sendToChat(ctx, chatID, controlCommandErrorMessage(err))
			}
			return b.sendToChat(ctx, chatID, message)
		case DashboardCommandHelp:
			if isAdvancedHelpArg(parsed.Argument) {
				return b.sendToChat(ctx, chatID, sessionAdvancedHelpText())
			}
			return b.sendToChat(ctx, chatID, sessionHelpText())
		default:
			return b.sendToChat(ctx, chatID, unknownWorkCommandMessage(routeText))
		}
	}

	if msg.Body.Content == "" && strings.TrimSpace(text) != "" {
		msg.Body.Content = text
	}
	if importing, err := b.sessionTranscriptImportInProgress(ctx, session.ID); err != nil {
		return err
	} else if importing {
		return b.deferSessionMessageDuringTranscriptImport(ctx, session, msg)
	}
	if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
		return err
	} else if blocked {
		return b.rejectSessionWork(ctx, session, msg, control)
	}
	if duplicate, err := b.ignoreRecentDuplicateSessionPromptWithSnapshot(ctx, session, msg, text, knownQueueSnapshot); err != nil {
		return err
	} else if duplicate {
		return nil
	}
	if message := attachmentPreflightMessage(msg); message != "" {
		return b.rejectSessionAttachmentWithMessage(ctx, session, msg, message)
	}
	if b.asyncTurns {
		var turns sessionTurnQueueState
		if knownTurns != nil {
			turns = *knownTurns
		} else {
			var err error
			turns, err = b.sessionTurnQueueState(ctx, session.ID)
			if err != nil {
				return err
			}
		}
		if turns.Running {
			if err := b.ensureDurableSession(ctx, session); err != nil {
				return err
			}
			inbound, created, err := b.persistInbound(ctx, session, msg)
			if err != nil {
				return err
			}
			turn, turnCreated, err := b.queueTurn(ctx, session, inbound)
			if err != nil {
				return err
			}
			if !created || !turnCreated {
				return b.flushPendingOutbox(ctx, session.ID, turn.ID)
			}
			session.UpdatedAt = time.Now()
			ackBody := b.formatBlockedTeamsPromptAckFromSnapshot(ctx, session, localCodexBeforeTeamsGate{
				Block:   true,
				AckBody: "A Codex request is already running in this Work chat.",
			}, knownQueueSnapshot)
			if err := b.queueTeamsPromptAckWithBodyForMessage(ctx, session, turn, ackBody, msg); err != nil {
				return err
			}
			b.boostPolling(time.Now())
			return nil
		}
		gate, err := b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
		if err != nil {
			return err
		}
		if gate.Block {
			if err := b.ensureDurableSession(ctx, session); err != nil {
				return err
			}
			inbound, created, err := b.persistInbound(ctx, session, msg)
			if err != nil {
				return err
			}
			turn, turnCreated, err := b.queueTurn(ctx, session, inbound)
			if err != nil {
				return err
			}
			if !created || !turnCreated {
				return b.flushPendingOutbox(ctx, session.ID, turn.ID)
			}
			ackBody := b.formatBlockedTeamsPromptAckFromSnapshot(ctx, session, gate, knownQueueSnapshot)
			if err := b.queueTeamsPromptAckWithBodyForMessage(ctx, session, turn, ackBody, msg); err != nil {
				return err
			}
			b.boostPolling(time.Now())
			return nil
		}
	}

	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	inbound, created, err := b.persistInbound(ctx, session, msg)
	if err != nil {
		return err
	}
	turn, turnCreated, err := b.queueTurn(ctx, session, inbound)
	if err != nil {
		return err
	}
	if !created || !turnCreated {
		return b.flushPendingOutbox(ctx, session.ID, turn.ID)
	}
	session.UpdatedAt = time.Now()
	if err := b.queueTeamsPromptAckForMessage(ctx, session, turn, msg, false); err != nil {
		return err
	}
	if b.asyncTurns {
		started, err := b.startQueuedTurn(ctx, session, turn.ID, func(runCtx context.Context, runSession *Session, claimed teamstore.Turn) error {
			return b.runPreparedQueuedTurnFromMessage(runCtx, runSession, claimed, runSession.ChatID, msg, text, b.executor)
		})
		if err != nil {
			return err
		}
		if started && knownTurns != nil {
			knownTurns.Running = true
		}
		b.boostPolling(time.Now())
		return nil
	}
	return b.runPreparedQueuedTurnFromMessage(ctx, session, turn, chatID, msg, text, b.executor)
}

func (b *Bridge) rejectExternalWorkCommand(ctx context.Context, session *Session, msg ChatMessage) error {
	if session == nil {
		return nil
	}
	body := "Only the helper owner can run helper commands in this Work chat. Send a normal question or task here and I will pass it to Codex."
	outbox := teamstore.OutboxMessage{
		ID:          "outbox:external-command:" + shortStableID(session.ID+":"+msg.ID),
		SessionID:   session.ID,
		TeamsChatID: session.ChatID,
		Kind:        "control",
		Body:        body,
	}
	if author, ok := chatMessageExternalAuthor(msg, b.user); ok {
		applyOutboxMentionUser(&outbox, author)
	}
	return b.queueAndSendOutbox(ctx, outbox)
}

func (b *Bridge) retryTurnCommand(ctx context.Context, session *Session, turnID string) error {
	if turnID == "" {
		return b.sendToChat(ctx, session.ChatID, "usage: `helper retry last`, `helper retry <turn-id>`, or `!retry <turn-id>`")
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(turnID), "last") {
		resolved, ok := latestRetryableTurnID(state, session.ID)
		if !ok {
			return b.sendToChat(ctx, session.ChatID, "no failed or interrupted turn is available to retry in this session.")
		}
		turnID = resolved
	}
	turn, ok := state.Turns[turnID]
	if !ok || turn.SessionID != session.ID {
		return b.sendToChat(ctx, session.ChatID, "turn not found in this session: "+turnID)
	}
	switch turn.Status {
	case teamstore.TurnStatusFailed, teamstore.TurnStatusInterrupted:
	default:
		return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("turn %s is %s; only failed or interrupted turns can be retried.", turn.ID, turn.Status))
	}
	inbound, ok := state.InboundEvents[turn.InboundEventID]
	if !ok || inbound.TeamsMessageID == "" {
		return b.sendToChat(ctx, session.ChatID, "retry cannot find the original Teams message for "+turn.ID)
	}
	msg, err := b.readClient().GetMessageWithoutRateLimitRetry(ctx, inbound.TeamsChatID, inbound.TeamsMessageID)
	if err != nil {
		return b.sendToChat(ctx, session.ChatID, "retry failed while reading the original Teams message: "+err.Error())
	}
	retryTurn, _, err := b.store.QueueTurn(ctx, teamstore.Turn{
		ID:             retryTurnID(turn.ID),
		SessionID:      session.ID,
		CodexThreadID:  session.CodexThreadID,
		ModelProfile:   retryTurnModelProfile(turn, session.ModelProfile),
		RecoveryReason: "retry of " + turn.ID,
	})
	if err != nil {
		return err
	}
	return b.runPreparedQueuedTurnFromMessage(ctx, session, retryTurn, session.ChatID, msg, inbound.Text, b.executor)
}

func latestRetryableTurnID(state teamstore.State, sessionID string) (string, bool) {
	var latest teamstore.Turn
	for _, turn := range state.Turns {
		if turn.SessionID != sessionID {
			continue
		}
		switch turn.Status {
		case teamstore.TurnStatusFailed, teamstore.TurnStatusInterrupted:
		default:
			continue
		}
		if latest.ID == "" || turnSortTime(turn).After(turnSortTime(latest)) {
			latest = turn
		}
	}
	if latest.ID == "" {
		return "", false
	}
	return latest.ID, true
}

func turnSortTime(turn teamstore.Turn) time.Time {
	for _, value := range []time.Time{turn.UpdatedAt, turn.InterruptedAt, turn.FailedAt, turn.CompletedAt, turn.StartedAt, turn.QueuedAt, turn.CreatedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func (b *Bridge) queueTeamsPromptAck(ctx context.Context, session *Session, turn teamstore.Turn, queuedBehindActive bool) error {
	return b.queueTeamsPromptAckForMessage(ctx, session, turn, ChatMessage{}, queuedBehindActive)
}

func (b *Bridge) queueTeamsPromptAckForMessage(ctx context.Context, session *Session, turn teamstore.Turn, msg ChatMessage, queuedBehindActive bool) error {
	body := "⏳ Codex is working. Request accepted."
	if queuedBehindActive {
		body = b.formatBlockedTeamsPromptAck(ctx, session, localCodexBeforeTeamsGate{
			Block:   true,
			AckBody: "A Codex request is already running in this Work chat.",
		})
	} else if _, ok := chatMessageExternalAuthor(msg, b.user); ok {
		body = "⏳ Codex received your question. Request accepted."
	}
	return b.queueTeamsPromptAckWithBodyForMessage(ctx, session, turn, body, msg)
}

func (b *Bridge) queueTeamsPromptBlockedAck(ctx context.Context, session *Session, turn teamstore.Turn, gate localCodexBeforeTeamsGate) error {
	return b.queueTeamsPromptBlockedAckForMessage(ctx, session, turn, ChatMessage{}, gate)
}

func (b *Bridge) queueTeamsPromptBlockedAckForMessage(ctx context.Context, session *Session, turn teamstore.Turn, msg ChatMessage, gate localCodexBeforeTeamsGate) error {
	return b.queueTeamsPromptAckWithBodyForMessage(ctx, session, turn, b.formatBlockedTeamsPromptAck(ctx, session, gate), msg)
}

func (b *Bridge) formatBlockedTeamsPromptAck(ctx context.Context, session *Session, gate localCodexBeforeTeamsGate) string {
	return b.formatBlockedTeamsPromptAckFromSnapshot(ctx, session, gate, nil)
}

func (b *Bridge) formatBlockedTeamsPromptAckFromSnapshot(ctx context.Context, session *Session, gate localCodexBeforeTeamsGate, snapshot *teamstore.State) string {
	reason := blockedAckReason(gate)
	lines := []string{
		"⚠️ **Your request is queued.**",
		reason,
		"",
		"---",
		"",
	}
	if snapshot != nil && session != nil {
		if queueLines := formatSessionTurnQueueSnapshot(*snapshot, session.ID, requestAheadFallbackSummary(gate)); len(queueLines) > 0 {
			lines = append(lines, queueLines...)
		} else {
			lines = append(lines, b.sessionTurnQueueSnapshotLines(ctx, session, gate)...)
		}
	} else {
		lines = append(lines, b.sessionTurnQueueSnapshotLines(ctx, session, gate)...)
	}
	lines = append(lines,
		"",
		"---",
		"",
		"**Options:**",
		"- Run this request next: do nothing. It is already submitted.",
		"- Need it immediately: send it in a different 💬 Work chat.",
		"- Drop this queued request: `helper cancel last`.",
		"- Interrupt the request ahead only if it looks stuck: open the 🏠 Control chat and send `helper restart force`.",
	)
	return strings.Join(lines, "\n")
}

func blockedAckReason(gate localCodexBeforeTeamsGate) string {
	text := strings.TrimSpace(gate.AckBody)
	switch {
	case strings.Contains(text, "already running"):
		return "Another Codex request is already running in this Work chat, so I will run yours after it finishes."
	case strings.Contains(text, "active in the CLI"):
		return "A local Codex CLI request is active for this chat, so I will run yours after that finishes."
	case strings.Contains(text, "preparing this chat history"):
		return "I am preparing this chat history first, so I will run your request after the import finishes."
	case strings.Contains(text, "paused backlog"):
		return "Local Codex history has a paused backlog, so I need that resolved before running your request."
	case strings.Contains(text, "syncing recent Codex updates"):
		return "I am syncing recent local Codex updates first, so I will run your request after the sync finishes."
	case strings.Contains(text, "history sync needs attention"):
		return "Local Codex history sync needs attention before I can run your request."
	default:
		return "A required Codex/helper step is ahead of your request, so I will run yours after that clears."
	}
}

func requestAheadFallbackSummary(gate localCodexBeforeTeamsGate) string {
	text := strings.TrimSpace(gate.AckBody)
	switch {
	case strings.Contains(text, "active in the CLI"):
		return "Local Codex CLI request. The prompt is not available from Teams."
	case strings.Contains(text, "preparing this chat history"):
		return "Local Codex history import."
	case strings.Contains(text, "paused backlog"):
		return "Paused local Codex history backlog. Send `helper publish-history` when you want to import it."
	case strings.Contains(text, "syncing recent Codex updates"):
		return "Recent local Codex updates are being synced into this Teams chat."
	case strings.Contains(text, "history sync needs attention"):
		return "Local Codex history sync recovery."
	default:
		return "Codex/helper work already in progress."
	}
}

func (b *Bridge) sessionTurnQueueSnapshotLines(ctx context.Context, session *Session, gate localCodexBeforeTeamsGate) []string {
	fallback := requestAheadFallbackSummary(gate)
	if b != nil && b.store != nil && session != nil {
		if state, err := b.store.SessionActiveTurnQueueSnapshot(ctx, session.ID); err == nil {
			if lines := formatSessionTurnQueueSnapshot(state, session.ID, fallback); len(lines) > 0 {
				return lines
			}
		}
	}
	return []string{"**Currently blocking:**", fallback}
}

func formatSessionTurnQueueSnapshot(state teamstore.State, sessionID string, blockingFallback string) []string {
	var lines []string
	running := runningTurnsForSessionState(state, sessionID)
	queued := queuedTurnsForSessionState(state, sessionID)
	if len(running) > 0 {
		lines = append(lines, "**▶️ Running now:**")
		lines = append(lines, formatTurnPromptList(state, running)...)
	} else if strings.TrimSpace(blockingFallback) != "" {
		lines = append(lines, "**Currently blocking:**", strings.TrimSpace(blockingFallback))
	}
	if len(queued) > 0 {
		if len(lines) > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, "**⏳ Queued requests:**")
		lines = append(lines, formatTurnPromptList(state, queued)...)
	}
	return lines
}

func runningTurnsForSessionState(state teamstore.State, sessionID string) []teamstore.Turn {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	var turns []teamstore.Turn
	for _, turn := range state.Turns {
		if turn.SessionID == sessionID && turn.Status == teamstore.TurnStatusRunning {
			turns = append(turns, turn)
		}
	}
	sort.SliceStable(turns, func(i, j int) bool {
		if !turnSortTime(turns[i]).Equal(turnSortTime(turns[j])) {
			return turnSortTime(turns[i]).Before(turnSortTime(turns[j]))
		}
		return turns[i].ID < turns[j].ID
	})
	return turns
}

func queuedTurnsForSessionState(state teamstore.State, sessionID string) []teamstore.Turn {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	var turns []teamstore.Turn
	for _, turn := range state.Turns {
		if turn.SessionID == sessionID && turn.Status == teamstore.TurnStatusQueued {
			turns = append(turns, turn)
		}
	}
	sort.SliceStable(turns, func(i, j int) bool {
		left := queuedTurnAgeBaseTime(turns[i])
		right := queuedTurnAgeBaseTime(turns[j])
		if !left.Equal(right) {
			return left.Before(right)
		}
		return turns[i].ID < turns[j].ID
	})
	return turns
}

func sessionHasQueuedOrRunningTurnState(state teamstore.State, sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	for _, turn := range state.Turns {
		if turn.SessionID != sessionID {
			continue
		}
		if turn.Status == teamstore.TurnStatusQueued || turn.Status == teamstore.TurnStatusRunning {
			return true
		}
	}
	return false
}

func turnPromptSummary(state teamstore.State, turn teamstore.Turn) string {
	inbound := state.InboundEvents[turn.InboundEventID]
	text := strings.TrimSpace(StripHelperPromptEchoes(StripArtifactManifestBlocks(inbound.Text)))
	if text == "" {
		return ""
	}
	text = strings.Join(strings.Fields(text), " ")
	text = strings.ReplaceAll(text, "`", "'")
	return shortenTeamsLine(text, 180)
}

func (b *Bridge) queueTeamsPromptAckWithBody(ctx context.Context, session *Session, turn teamstore.Turn, body string) error {
	return b.queueTeamsPromptAckWithBodyForMessage(ctx, session, turn, body, ChatMessage{})
}

func (b *Bridge) queueTeamsPromptAckWithBodyForMessage(ctx context.Context, session *Session, turn teamstore.Turn, body string, msg ChatMessage) error {
	body = strings.TrimSpace(body)
	if body == "" {
		body = "⏳ Codex is working. Request accepted."
	}
	outbox := teamstore.OutboxMessage{
		ID:          "outbox:" + turn.ID + ":ack",
		SessionID:   session.ID,
		TurnID:      turn.ID,
		TeamsChatID: session.ChatID,
		Kind:        "ack",
		AckKind:     "teams_prompt",
		Body:        body,
	}
	if author, ok := chatMessageExternalAuthor(msg, b.user); ok {
		applyOutboxMentionUser(&outbox, author)
	}
	queued, err := b.queueOutbox(ctx, outbox)
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	if err := b.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true}); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams ACK send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) recoverUnfinishedTurns(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	state, err := b.store.TurnQueueStateSnapshot(ctx)
	if err != nil {
		return err
	}
	var turns []teamstore.Turn
	for _, turn := range state.Turns {
		if turn.Status == teamstore.TurnStatusQueued || turn.Status == teamstore.TurnStatusRunning {
			turns = append(turns, turn)
		}
	}
	sort.Slice(turns, func(i, j int) bool {
		return turns[i].CreatedAt.Before(turns[j].CreatedAt)
	})
	for _, turn := range turns {
		session := b.sessionForTurnState(state, turn)
		if session == nil {
			continue
		}
		switch turn.Status {
		case teamstore.TurnStatusQueued:
			if b.asyncTurns {
				continue
			}
			if err := b.recoverQueuedTurn(ctx, session, turn, state); err != nil {
				return err
			}
		case teamstore.TurnStatusRunning:
			if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, recoveryReasonAmbiguousAfterHelperRestart); err != nil {
				return err
			}
		}
	}
	return b.sendDeferredInterruptedTurnNoticesNow(ctx)
}

func interruptedAfterRestartOutboxID(turnID string) string {
	return "outbox:" + strings.TrimSpace(turnID) + ":interrupted-after-restart"
}

func (b *Bridge) sendDeferredInterruptedTurnNotices(ctx context.Context) error {
	if b == nil || b.store == nil {
		return nil
	}
	if !b.deferredInterruptedNoticePending() {
		return nil
	}
	return b.sendDeferredInterruptedTurnNoticesNow(ctx)
}

func (b *Bridge) sendDeferredInterruptedTurnNoticesNow(ctx context.Context) error {
	if b == nil || b.store == nil {
		return nil
	}
	state, err := b.store.QueuedTurnStateSnapshot(ctx)
	if err != nil {
		return err
	}
	var turns []teamstore.Turn
	pendingAfterScan := false
	for _, turn := range state.Turns {
		if turn.Status != teamstore.TurnStatusInterrupted || strings.TrimSpace(turn.RecoveryReason) != recoveryReasonAmbiguousAfterHelperRestart {
			continue
		}
		if sessionHasQueuedOrRunningTurnState(state, turn.SessionID) {
			pendingAfterScan = true
			continue
		}
		turns = append(turns, turn)
	}
	sort.Slice(turns, func(i, j int) bool {
		left := turnSortTime(turns[i])
		right := turnSortTime(turns[j])
		if !left.Equal(right) {
			return left.Before(right)
		}
		return turns[i].ID < turns[j].ID
	})
	if len(turns) == 0 {
		b.setDeferredInterruptedNoticePending(pendingAfterScan)
		return nil
	}
	b.setDeferredInterruptedNoticePending(true)
	for _, turn := range turns {
		session := b.sessionForTurnState(state, turn)
		if session == nil || strings.TrimSpace(session.ChatID) == "" {
			pendingAfterScan = true
			continue
		}
		if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
			ID:               interruptedAfterRestartOutboxID(turn.ID),
			SessionID:        session.ID,
			TurnID:           turn.ID,
			TeamsChatID:      session.ChatID,
			Kind:             "interrupted-after-restart",
			Body:             "turn was interrupted after helper restart: " + turn.ID + "\n\nUse `helper retry " + turn.ID + "` if you want to run it again.",
			MentionOwner:     true,
			NotificationKind: "needs_attention",
		}); err != nil {
			return err
		}
		if err := b.markInterruptedAfterRestartNoticeSent(ctx, turn); err != nil {
			return err
		}
	}
	b.setDeferredInterruptedNoticePending(pendingAfterScan)
	return nil
}

func (b *Bridge) deferredInterruptedNoticePending() bool {
	if b == nil {
		return false
	}
	b.deferredNoticeMu.Lock()
	defer b.deferredNoticeMu.Unlock()
	return b.deferredInterruptedPending
}

func (b *Bridge) setDeferredInterruptedNoticePending(pending bool) {
	if b == nil {
		return
	}
	b.deferredNoticeMu.Lock()
	b.deferredInterruptedPending = pending
	b.deferredNoticeMu.Unlock()
}

func (b *Bridge) markInterruptedAfterRestartNoticeSent(ctx context.Context, turn teamstore.Turn) error {
	if b == nil || b.store == nil || strings.TrimSpace(turn.ID) == "" || strings.TrimSpace(turn.SessionID) == "" {
		return nil
	}
	return b.store.UpdateSession(ctx, turn.SessionID, func(state *teamstore.State) error {
		current, ok := state.Turns[turn.ID]
		if !ok || current.Status != teamstore.TurnStatusInterrupted || strings.TrimSpace(current.RecoveryReason) != recoveryReasonAmbiguousAfterHelperRestart {
			return nil
		}
		current.RecoveryReason = recoveryReasonAmbiguousAfterHelperRestartNoticeSent
		current.UpdatedAt = time.Now()
		state.Turns[current.ID] = current
		return nil
	})
}

func (b *Bridge) processDeferredInbound(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	control, err := b.store.ReadControl(ctx)
	if err != nil {
		return err
	}
	if control.Paused || control.Draining {
		return nil
	}
	deferred, err := b.store.DeferredInbound(ctx)
	if err != nil {
		return err
	}
	for _, inbound := range deferred {
		switch inbound.Source {
		case "teams_control_new", "teams_control_fallback", "teams_control_publish":
			if err := b.processDeferredControlInbound(ctx, inbound); err != nil {
				return err
			}
			continue
		case "teams_session_attachment_deferred", "teams_session_command_deferred":
			if err := b.rejectDeferredSessionInboundAfterUpgrade(ctx, inbound); err != nil {
				return err
			}
			continue
		}
		session := b.reg.SessionByID(inbound.SessionID)
		if session == nil && inbound.TeamsChatID != "" {
			session = b.reg.SessionByChatID(inbound.TeamsChatID)
		}
		if session == nil {
			if err := b.markDeferredInboundIgnored(ctx, inbound.ID, "deferred input session is no longer available"); err != nil {
				return err
			}
			continue
		}
		if importing, err := b.sessionTranscriptImportInProgress(ctx, session.ID); err != nil {
			return err
		} else if importing {
			continue
		}
		text := strings.TrimSpace(inbound.Text)
		runInput := ExecutionInput{}
		cleanupPrompt := func() {}
		if inbound.Source == "teams_session_import_deferred_attachment" && strings.TrimSpace(inbound.TeamsChatID) != "" && strings.TrimSpace(inbound.TeamsMessageID) != "" {
			if msg, err := b.readClient().GetMessageWithoutRateLimitRetry(ctx, inbound.TeamsChatID, inbound.TeamsMessageID); err == nil {
				prepared, cleanup, warning, err := b.prepareSessionPromptFromTeamsMessage(ctx, session, "", inbound.TeamsChatID, msg, text)
				if err != nil {
					cleanup()
					return err
				}
				if warning != "" {
					cleanup()
					if err := b.markDeferredInboundIgnored(ctx, inbound.ID, warning); err != nil {
						return err
					}
					if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
						ID:          "outbox:" + inbound.ID + ":deferred-rejected",
						SessionID:   session.ID,
						TeamsChatID: session.ChatID,
						Kind:        "error",
						Body:        warning,
					}); err != nil {
						return err
					}
					continue
				}
				runInput = prepared
				cleanupPrompt = cleanup
			}
		}
		if runInput.Prompt == "" && text == "" {
			if err := b.markDeferredInboundIgnored(ctx, inbound.ID, "deferred input text is unavailable"); err != nil {
				return err
			}
			if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
				ID:          "outbox:" + inbound.ID + ":deferred-missing-text",
				SessionID:   session.ID,
				TeamsChatID: session.ChatID,
				Kind:        "error",
				Body:        "deferred Teams input could not be resumed because the original message text was unavailable. Please resend it.",
			}); err != nil {
				return err
			}
			continue
		}
		if runInput.Prompt == "" {
			runInput = ExecutionInput{Prompt: TeamsCodexPrompt(text)}
		}
		turn, turnCreated, err := b.queueTurn(ctx, session, inbound)
		if err != nil {
			cleanupPrompt()
			return err
		}
		if !turnCreated {
			cleanupPrompt()
			if err := b.flushPendingOutbox(ctx, session.ID, turn.ID); err != nil {
				return err
			}
			continue
		}
		if err := b.queueTeamsPromptAck(ctx, session, turn, false); err != nil {
			cleanupPrompt()
			return err
		}
		if b.asyncTurns {
			started, err := b.startQueuedTurn(ctx, session, turn.ID, func(runCtx context.Context, runSession *Session, claimed teamstore.Turn) error {
				defer cleanupPrompt()
				return b.runQueuedTurnInput(runCtx, runSession, claimed, runSession.ChatID, runInput)
			})
			if err != nil {
				cleanupPrompt()
				return err
			}
			if !started {
				cleanupPrompt()
			}
			b.boostPolling(time.Now())
			continue
		}
		if err := b.runQueuedTurnInput(ctx, session, turn, session.ChatID, runInput); err != nil {
			cleanupPrompt()
			return err
		}
		cleanupPrompt()
	}
	return nil
}

func (b *Bridge) processQueuedTurns(ctx context.Context) error {
	if !b.asyncTurns {
		return nil
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	hasQueued, err := b.store.HasQueuedTurns(ctx)
	if err != nil {
		return err
	}
	if !hasQueued {
		return nil
	}
	state, err := b.store.QueuedTurnStateSnapshot(ctx)
	if err != nil {
		return err
	}
	running := make(map[string]bool)
	queued := make(map[string]bool)
	for _, turn := range state.Turns {
		switch turn.Status {
		case teamstore.TurnStatusRunning:
			running[turn.SessionID] = true
		case teamstore.TurnStatusQueued:
			queued[turn.SessionID] = true
		}
	}
	var sessionIDs []string
	for sessionID := range queued {
		if transcriptImportIsActive(state, sessionID) {
			continue
		}
		if !running[sessionID] {
			sessionIDs = append(sessionIDs, sessionID)
		}
	}
	sort.Strings(sessionIDs)
	startLimit := b.effectiveMaxQueuedTurnStartsPerCycle()
	started := 0
	var firstErr error
	recordSessionErr := func(session *Session, stage string, err error) {
		if err == nil {
			return
		}
		if firstErr == nil {
			firstErr = err
		}
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams queued turn %s error for session %s: %v\n", stage, session.ID, err)
		}
	}
	for _, sessionID := range sessionIDs {
		if startLimit > 0 && started >= startLimit {
			break
		}
		session := b.sessionForIDState(state, sessionID)
		if session == nil {
			continue
		}
		gate, err := b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
		if err != nil {
			recordSessionErr(session, "gate", err)
			continue
		}
		if gate.Block {
			if turn, ok := oldestQueuedTurnForSessionState(state, sessionID); ok {
				if err := b.sendQueuedTurnAttentionIfDue(ctx, session, turn, gate, time.Now()); err != nil {
					recordSessionErr(session, "wait notice", err)
				}
			}
			continue
		}
		if startedNow, err := b.startQueuedTurn(ctx, session, "", nil); err != nil {
			recordSessionErr(session, "start", err)
			continue
		} else if startedNow {
			started++
		}
	}
	return firstErr
}

func (b *Bridge) processQueuedTurnsForSession(ctx context.Context, session *Session) error {
	if b == nil || !b.asyncTurns || session == nil || strings.TrimSpace(session.ID) == "" {
		return nil
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	if importing, err := b.sessionTranscriptImportInProgress(ctx, session.ID); err != nil {
		return err
	} else if importing {
		return nil
	}
	state, err := b.store.SessionActiveTurnQueueSnapshot(ctx, session.ID)
	if err != nil {
		return err
	}
	for _, turn := range state.Turns {
		if turn.SessionID == session.ID && turn.Status == teamstore.TurnStatusRunning {
			return nil
		}
	}
	queued, ok := oldestQueuedTurnForSessionState(state, session.ID)
	if !ok {
		return nil
	}
	gate, err := b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
	if err != nil {
		return err
	}
	if gate.Block {
		return b.sendQueuedTurnAttentionIfDue(ctx, session, queued, gate, time.Now())
	}
	_, err = b.startQueuedTurn(ctx, session, "", nil)
	return err
}

func oldestQueuedTurnForSessionState(state teamstore.State, sessionID string) (teamstore.Turn, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return teamstore.Turn{}, false
	}
	var out teamstore.Turn
	for _, turn := range state.Turns {
		if turn.SessionID != sessionID || turn.Status != teamstore.TurnStatusQueued {
			continue
		}
		if out.ID == "" || queuedTurnAgeBaseTime(turn).Before(queuedTurnAgeBaseTime(out)) ||
			(queuedTurnAgeBaseTime(turn).Equal(queuedTurnAgeBaseTime(out)) && turn.ID < out.ID) {
			out = turn
		}
	}
	return out, out.ID != ""
}

func queuedTurnAgeBaseTime(turn teamstore.Turn) time.Time {
	for _, value := range []time.Time{turn.QueuedAt, turn.CreatedAt, turn.UpdatedAt, turn.StartedAt} {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func queuedTurnAttentionBucket(turn teamstore.Turn, now time.Time) (int64, bool) {
	if now.IsZero() {
		now = time.Now()
	}
	base := queuedTurnAgeBaseTime(turn)
	if base.IsZero() || queuedTurnAttentionDelay <= 0 || now.Sub(base) < queuedTurnAttentionDelay {
		return 0, false
	}
	repeat := queuedTurnAttentionRepeatDelay
	if repeat <= 0 {
		repeat = queuedTurnAttentionDelay
	}
	seconds := int64(repeat / time.Second)
	if seconds <= 0 {
		seconds = 1
	}
	return now.Unix() / seconds, true
}

func (b *Bridge) sendQueuedTurnAttentionIfDue(ctx context.Context, session *Session, turn teamstore.Turn, gate localCodexBeforeTeamsGate, now time.Time) error {
	if b == nil || session == nil || strings.TrimSpace(session.ChatID) == "" {
		return nil
	}
	bucket, ok := queuedTurnAttentionBucket(turn, now)
	if !ok {
		return nil
	}
	body := strings.Join([]string{
		"⏳ Still waiting.",
		queuedTurnBlockedReason(gate),
		"",
		"Your Teams message is queued and will run after this clears.",
		"If this looks stale, send `helper status` here. To drop the queued message, send `helper cancel last`.",
	}, "\n")
	if queueLines := b.sessionTurnQueueSnapshotLines(ctx, session, gate); len(queueLines) > 0 {
		body += "\n\n---\n\n" + strings.Join(queueLines, "\n")
	}
	return b.queueAndBestEffortSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          fmt.Sprintf("outbox:%s:queued-wait:%d", turn.ID, bucket),
		SessionID:   session.ID,
		TurnID:      turn.ID,
		TeamsChatID: session.ChatID,
		Kind:        "queued-wait",
		Body:        body,
	})
}

func queuedTurnBlockedReason(gate localCodexBeforeTeamsGate) string {
	text := strings.TrimSpace(gate.AckBody)
	switch {
	case strings.Contains(text, "active in the CLI"):
		return "Local Codex is still active for this chat."
	case strings.Contains(text, "preparing this chat history"):
		return "I am still preparing local history for this chat."
	case strings.Contains(text, "paused backlog"):
		return "Local history import is paused until you send `helper publish-history`."
	case strings.Contains(text, "syncing recent Codex updates"):
		return "I am still syncing recent local Codex updates before running your Teams request."
	case strings.Contains(text, "history sync needs attention"):
		return "Local history sync needs attention before I can continue."
	default:
		return "The helper is waiting for a required local sync step."
	}
}

func (b *Bridge) rejectDeferredSessionInboundAfterUpgrade(ctx context.Context, inbound teamstore.InboundEvent) error {
	session := b.reg.SessionByID(inbound.SessionID)
	if session == nil && inbound.TeamsChatID != "" {
		session = b.reg.SessionByChatID(inbound.TeamsChatID)
	}
	if session == nil {
		return b.markDeferredInboundIgnored(ctx, inbound.ID, "deferred input session is no longer available")
	}
	reason := "deferred Teams input could not be replayed safely. Please resend it."
	if inbound.Source == "teams_session_attachment_deferred" {
		reason = "Teams input received during helper upgrade included files or images. I did not replay it automatically because attachments are not preserved across the upgrade drain. Please resend the message."
	} else if inbound.Source == "teams_session_command_deferred" {
		reason = "Teams command received during helper upgrade was not replayed automatically. Please run the command again."
	}
	if err := b.markDeferredInboundIgnored(ctx, inbound.ID, reason); err != nil {
		return err
	}
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + inbound.ID + ":deferred-rejected",
		SessionID:   session.ID,
		TeamsChatID: session.ChatID,
		Kind:        "error",
		Body:        reason,
	})
}

func (b *Bridge) processDeferredControlInbound(ctx context.Context, inbound teamstore.InboundEvent) error {
	text := strings.TrimSpace(inbound.Text)
	msg, hasContext := chatMessageFromInboundContext(inbound)
	if !hasContext {
		msg = ChatMessage{ID: inbound.TeamsMessageID}
		msg.Body.ContentType = "html"
		msg.Body.Content = html.EscapeString(text)
	}
	if text == "" && !(inbound.Source == "teams_control_fallback" && hasSupportedTeamsMediaCardAttachment(msg.Attachments)) {
		return b.markDeferredInboundIgnored(ctx, inbound.ID, "deferred control input text is unavailable")
	}
	switch inbound.Source {
	case "teams_control_new":
		arg, err := controlNewSessionArgument(text)
		if err != nil {
			if markErr := b.markDeferredInboundIgnored(ctx, inbound.ID, err.Error()); markErr != nil {
				return markErr
			}
			return b.sendControl(ctx, err.Error())
		}
		if err := b.createSession(ctx, msg, arg); err != nil {
			return err
		}
		return b.markDeferredInboundIgnored(ctx, inbound.ID, "replayed control new command")
	case "teams_control_publish":
		target, err := controlPublishTarget(text)
		if err != nil {
			if markErr := b.markDeferredInboundIgnored(ctx, inbound.ID, err.Error()); markErr != nil {
				return markErr
			}
			return b.sendControl(ctx, err.Error())
		}
		message, err := b.publishCodexSession(ctx, target)
		if err != nil {
			return err
		}
		if err := b.sendControl(ctx, message); err != nil {
			return err
		}
		return b.markDeferredInboundIgnored(ctx, inbound.ID, "replayed control publish command")
	case "teams_control_fallback":
		if hasSupportedTeamsMediaCardAttachment(msg.Attachments) && !teamsASRTranscriberConfigured(b.asrTranscriber) {
			message := teamsASRFailureUserMessage(errASRCommandNotConfigured)
			if markErr := b.markDeferredInboundIgnored(ctx, inbound.ID, message); markErr != nil {
				return markErr
			}
			return b.sendControl(ctx, message)
		}
		session, err := b.ensureControlFallbackSession(ctx)
		if err != nil {
			return err
		}
		promptText, warning, err := b.controlFallbackPromptForDeferredInbound(ctx, inbound, text)
		if err != nil {
			return err
		}
		if warning != "" {
			if markErr := b.markDeferredInboundIgnored(ctx, inbound.ID, warning); markErr != nil {
				return markErr
			}
			return b.sendControl(ctx, warning)
		}
		turn, turnCreated, err := b.queueTurn(ctx, session, inbound)
		if err != nil {
			return err
		}
		if !turnCreated {
			return b.flushPendingOutbox(ctx, session.ID, turn.ID)
		}
		if err := b.queueControlFallbackAck(ctx, session, turn); err != nil {
			return err
		}
		if b.asyncTurns {
			if _, err := b.startQueuedTurn(ctx, session, turn.ID, func(runCtx context.Context, runSession *Session, claimed teamstore.Turn) error {
				return b.runControlFallbackQueuedTurnFromMessage(runCtx, runSession, claimed, msg, promptText)
			}); err != nil {
				return err
			}
			b.boostPolling(time.Now())
			return nil
		}
		return b.runControlFallbackQueuedTurnFromMessage(ctx, session, turn, msg, promptText)
	default:
		return b.markDeferredInboundIgnored(ctx, inbound.ID, "unsupported deferred control input")
	}
}

func (b *Bridge) controlFallbackPromptForDeferredInbound(ctx context.Context, inbound teamstore.InboundEvent, text string) (string, string, error) {
	msg, ok := chatMessageFromInboundContext(inbound)
	if !ok || !hasMessageReferenceAttachment(msg.Attachments) {
		return text, "", nil
	}
	if msg.ChatID == "" {
		msg.ChatID = strings.TrimSpace(b.reg.ControlChatID)
	}
	return b.controlFallbackPromptWithMessageReferences(ctx, msg, text)
}

func controlNewSessionArgument(text string) (string, error) {
	if parsed := ParseDashboardCommand(ChatScopeControl, text); parsed.HelperCommand && parsed.Name == DashboardCommandNew {
		return parsed.Argument, nil
	}
	return "", fmt.Errorf("deferred control input is no longer a new-session command")
}

func controlPublishTarget(text string) (DashboardCommandTarget, error) {
	if parsed := ParseDashboardCommand(ChatScopeControl, text); parsed.HelperCommand && parsed.Name == DashboardCommandPublish {
		return parsed.Target, nil
	}
	return DashboardCommandTarget{}, fmt.Errorf("deferred control input is no longer a publish command")
}

func (b *Bridge) markDeferredInboundIgnored(ctx context.Context, inboundID string, reason string) error {
	return b.store.Update(ctx, func(state *teamstore.State) error {
		inbound, ok := state.InboundEvents[inboundID]
		if !ok || inbound.Status != teamstore.InboundStatusDeferred {
			return nil
		}
		inbound.Status = teamstore.InboundStatusIgnored
		inbound.Source = strings.TrimSpace(inbound.Source + " " + reason)
		inbound.UpdatedAt = time.Now()
		state.InboundEvents[inbound.ID] = inbound
		return nil
	})
}

func (b *Bridge) sessionForTurnState(state teamstore.State, turn teamstore.Turn) *Session {
	return b.sessionForIDState(state, turn.SessionID)
}

func (b *Bridge) sessionForIDState(state teamstore.State, sessionID string) *Session {
	if session := b.reg.SessionByID(sessionID); session != nil {
		return session
	}
	durable, ok := state.Sessions[sessionID]
	if !ok || durable.TeamsChatID == "" {
		if ok && sessionID == controlFallbackSessionID && b.reg.ControlChatID != "" {
			return b.controlFallbackSessionFromState(durable)
		}
		return nil
	}
	session := Session{
		ID:            durable.ID,
		ChatID:        durable.TeamsChatID,
		ChatURL:       durable.TeamsChatURL,
		Topic:         durable.TeamsTopic,
		UserTitle:     durable.UserTitle,
		TitleSource:   durable.TitleSource,
		Status:        string(durable.Status),
		CodexThreadID: durable.CodexThreadID,
		Cwd:           durable.Cwd,
		ModelProfile:  durable.ModelProfile,
		CreatedAt:     durable.CreatedAt,
		UpdatedAt:     durable.UpdatedAt,
	}
	if session.Status == "" {
		session.Status = "active"
	}
	b.reg.Sessions = append(b.reg.Sessions, session)
	return b.reg.SessionByID(session.ID)
}

func (b *Bridge) recoverQueuedTurn(ctx context.Context, session *Session, turn teamstore.Turn, state teamstore.State) error {
	inbound, ok := state.InboundEvents[turn.InboundEventID]
	if !ok || inbound.TeamsMessageID == "" {
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "queued turn missing original Teams message"); err != nil {
			return err
		}
		return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
			ID:          "outbox:" + turn.ID + ":recovery-missing-message",
			SessionID:   session.ID,
			TurnID:      turn.ID,
			TeamsChatID: session.ChatID,
			Kind:        "recovery-missing-message",
			Body:        "queued turn could not be recovered because the original Teams message is missing: " + turn.ID,
		})
	}
	msg, err := b.readClient().GetMessageWithoutRateLimitRetry(ctx, inbound.TeamsChatID, inbound.TeamsMessageID)
	if err != nil {
		if session.ID != controlFallbackSessionID {
			return err
		}
		var ok bool
		msg, ok = chatMessageFromInboundContext(inbound)
		if !ok {
			return err
		}
	}
	if session.ID == controlFallbackSessionID {
		return b.runRecoveredControlFallbackQueuedTurn(ctx, session, turn, msg, inbound.Text)
	}
	return b.runPreparedQueuedTurnFromMessage(ctx, session, turn, inbound.TeamsChatID, msg, inbound.Text, b.executor)
}

func (b *Bridge) runRecoveredControlFallbackQueuedTurn(ctx context.Context, session *Session, turn teamstore.Turn, msg ChatMessage, fallbackText string) error {
	prompt, warning, err := b.controlFallbackPromptWithMessageReferences(ctx, msg, fallbackText)
	if err != nil {
		return err
	}
	if warning != "" {
		return b.interruptTurnForAttachmentMessage(ctx, session, turn, warning)
	}
	if strings.TrimSpace(prompt) == "" && !hasSupportedTeamsMediaCardAttachment(msg.Attachments) {
		return b.interruptTurnForAttachmentMessage(ctx, session, turn, "queued turn could not be recovered because the original prompt is empty: "+turn.ID)
	}
	return b.runControlFallbackQueuedTurnFromMessage(ctx, session, turn, msg, prompt)
}

func (b *Bridge) runPreparedQueuedTurnFromMessage(ctx context.Context, session *Session, turn teamstore.Turn, chatID string, msg ChatMessage, fallbackText string, executor Executor) error {
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	prepCtx, cancelPrep := context.WithCancel(ctx)
	unregisterPrepCancel := b.registerRunningTurnCancel(sessionID, turn.ID, cancelPrep)
	input, cleanupPrompt, preparationMessage, err := b.prepareSessionPromptFromTeamsMessage(prepCtx, session, turn.ID, chatID, msg, fallbackText)
	cancelRequested, cancelReason, cancelSilent := b.runningTurnCancelState(turn.ID)
	unregisterPrepCancel()
	cancelPrep()
	if cleanupPrompt == nil {
		cleanupPrompt = func() {}
	}
	defer cleanupPrompt()
	if cancelRequested {
		if _, markErr := b.store.MarkTurnInterrupted(ctx, turn.ID, firstNonEmptyString(cancelReason, "canceled by user")); markErr != nil {
			return markErr
		}
		if cancelSilent {
			return nil
		}
		return b.queueAndSendOutboxChunks(ctx, session.ID, turn.ID, chatID, "canceled", "Codex request canceled.")
	}
	if err != nil {
		return err
	}
	if preparationMessage != "" {
		return b.interruptTurnForAttachmentMessage(ctx, session, turn, preparationMessage)
	}
	if interrupted, err := b.turnInterrupted(ctx, turn.ID); err != nil {
		return err
	} else if interrupted {
		return nil
	}
	return b.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, chatID, input)
}

func (b *Bridge) prepareSessionPromptFromTeamsMessage(ctx context.Context, session *Session, turnID string, chatID string, msg ChatMessage, fallbackText string) (ExecutionInput, func(), string, error) {
	cleanupHosted := func() {}
	cleanupReference := func() {}
	cleanupAll := func() {
		cleanupHosted()
		cleanupReference()
	}
	localFiles, cleanupHostedFiles, hostedAttachmentMessage, err := b.downloadHostedContentAttachments(ctx, session, chatID, msg)
	cleanupHosted = cleanupHostedFiles
	if err != nil {
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return ExecutionInput{}, cleanupAll, message, nil
		}
		return ExecutionInput{}, cleanupAll, "", err
	}
	if hostedAttachmentMessage != "" {
		return ExecutionInput{}, cleanupAll, hostedAttachmentMessage, nil
	}
	referenceFiles, cleanupReferenceFiles, unsupportedAttachmentMessage, err := b.downloadReferenceFileAttachments(ctx, session, msg)
	cleanupReference = cleanupReferenceFiles
	if err != nil {
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return ExecutionInput{}, cleanupAll, message, nil
		}
		return ExecutionInput{}, cleanupAll, "", err
	}
	if unsupportedAttachmentMessage != "" {
		return ExecutionInput{}, cleanupAll, unsupportedAttachmentMessage, nil
	}
	localFiles = append(localFiles, referenceFiles...)
	referencedMessages, referencedMessageWarning, err := b.readMessageReferenceAttachments(ctx, chatID, msg)
	if err != nil {
		return ExecutionInput{}, cleanupAll, "", err
	}
	if referencedMessageWarning != "" {
		return ExecutionInput{}, cleanupAll, referencedMessageWarning, nil
	}
	prompt, promptOK := promptTextFromTeamsMessageOrFallback(msg, fallbackText)
	if !promptOK && len(localFiles) == 0 && len(referencedMessages) == 0 {
		return ExecutionInput{}, cleanupAll, "deferred Teams input could not be resumed because the original message text was unavailable. Please resend it.", nil
	}
	// ASR is helper-local preprocessing. It intentionally runs before beacon
	// execution planning so Teams media credentials and temp files stay local.
	transcripts, err := b.transcribeTeamsMediaAttachments(ctx, session, turnID, localFiles)
	if err != nil {
		return ExecutionInput{}, cleanupAll, teamsASRFailureUserMessage(err), nil
	}
	b.annotateIncomingUserMessageWithASRTranscripts(ctx, chatID, msg, transcripts)
	return executionInputWithPreparedTeamsContext(prompt, referencedMessages, localFiles, transcripts), cleanupAll, "", nil
}

func (b *Bridge) cancelTurnCommand(ctx context.Context, session *Session, turnID string) error {
	return b.cancelTurnCommandForScope(ctx, session, turnID, "Work chat")
}

func (b *Bridge) cancelControlFallbackCommand(ctx context.Context, turnID string) error {
	if strings.TrimSpace(turnID) == "" {
		return b.sendControl(ctx, "usage: `helper cancel last`, `helper cancel all`, or `helper cancel <turn-id>`\n\nThese commands apply to the current Control chat Codex question.")
	}
	session, err := b.ensureControlFallbackSession(ctx)
	if err != nil {
		return err
	}
	return b.cancelTurnCommandForScope(ctx, session, turnID, "Control chat")
}

func (b *Bridge) cancelTurnCommandForScope(ctx context.Context, session *Session, turnID string, scopeLabel string) error {
	if session == nil {
		return fmt.Errorf("cancel %s session is required", strings.ToLower(strings.TrimSpace(firstNonEmptyString(scopeLabel, "request"))))
	}
	scopeLabel = strings.TrimSpace(scopeLabel)
	if scopeLabel == "" {
		scopeLabel = "session"
	}
	scopeRef := cancelScopeReference(scopeLabel)
	if turnID == "" {
		return b.sendToChat(ctx, session.ChatID, "usage: `helper cancel last`, `helper cancel all`, `helper cancel <turn-id>`, or `!cancel <turn-id>`\n\nThese commands apply to this "+scopeLabel+".")
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(turnID)) {
	case "all":
		return b.cancelAllTurnsCommand(ctx, session, state, scopeLabel)
	case "last":
		resolved, ok := latestCancelableTurnID(state, session.ID)
		if !ok {
			return b.sendToChat(ctx, session.ChatID, "no running or queued turn is available to cancel in this "+scopeRef+".")
		}
		turnID = resolved
	}
	turn, ok := state.Turns[turnID]
	if !ok || turn.SessionID != session.ID {
		return b.sendToChat(ctx, session.ChatID, "turn not found in this "+scopeRef+": "+turnID)
	}
	switch turn.Status {
	case teamstore.TurnStatusQueued:
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "canceled by user"); err != nil {
			return err
		}
		if err := b.cancelBeaconTurn(ctx, session, turn, "canceled by user"); err != nil {
			return err
		}
		return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
			ID:          "outbox:" + turn.ID + ":canceled",
			SessionID:   session.ID,
			TurnID:      turn.ID,
			TeamsChatID: session.ChatID,
			Kind:        "canceled",
			Body:        formatCancelOneResponse(state, session.ID, turn, "turn canceled", true),
		})
	case teamstore.TurnStatusRunning:
		if !b.requestRunningTurnCancel(turn.ID) {
			return b.sendToChat(ctx, session.ChatID, "This Codex request is running, but this helper process does not own its live cancel handle. It may have started before the latest helper loaded or before a restart. Wait for it to finish, or use `helper status` to decide whether recovery is needed.")
		}
		if err := b.cancelBeaconTurn(ctx, session, turn, "canceled by user"); err != nil {
			return err
		}
		return b.sendToChat(ctx, session.ChatID, formatCancelOneResponse(state, session.ID, turn, "cancel requested for running turn", true))
	default:
		return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("turn %s is %s and cannot be canceled.", turn.ID, turn.Status))
	}
}

func (b *Bridge) cancelAllTurnsCommand(ctx context.Context, session *Session, state teamstore.State, scopeLabel string) error {
	scopeLabel = strings.TrimSpace(scopeLabel)
	if scopeLabel == "" {
		scopeLabel = "session"
	}
	scopeRef := cancelScopeReference(scopeLabel)
	targets := cancelableTurnsForSession(state, session.ID)
	if len(targets) == 0 {
		return b.sendToChat(ctx, session.ChatID, "no running or queued turn is available to cancel in this "+scopeRef+".")
	}
	var canceledQueued []teamstore.Turn
	var requestedRunning []teamstore.Turn
	var unavailableRunning []teamstore.Turn
	for _, turn := range targets {
		if turn.Status != teamstore.TurnStatusQueued {
			continue
		}
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "canceled by user"); err != nil {
			return err
		}
		if err := b.cancelBeaconTurn(ctx, session, turn, "canceled by user"); err != nil {
			return err
		}
		canceledQueued = append(canceledQueued, turn)
	}
	for _, turn := range targets {
		if turn.Status != teamstore.TurnStatusRunning {
			continue
		}
		if b.requestRunningTurnCancel(turn.ID) {
			if err := b.cancelBeaconTurn(ctx, session, turn, "canceled by user"); err != nil {
				return err
			}
			requestedRunning = append(requestedRunning, turn)
		} else {
			unavailableRunning = append(unavailableRunning, turn)
		}
	}
	return b.sendToChat(ctx, session.ChatID, formatCancelAllResponse(state, canceledQueued, requestedRunning, unavailableRunning, scopeLabel))
}

func latestCancelableTurnID(state teamstore.State, sessionID string) (string, bool) {
	var latest teamstore.Turn
	latestRank := 0
	for _, turn := range cancelableTurnsForSession(state, sessionID) {
		rank := cancelableTurnRank(turn)
		if latest.ID == "" || rank > latestRank || (rank == latestRank && turnSortTime(turn).After(turnSortTime(latest))) {
			latest = turn
			latestRank = rank
		}
	}
	if latest.ID == "" {
		return "", false
	}
	return latest.ID, true
}

func cancelableTurnsForSession(state teamstore.State, sessionID string) []teamstore.Turn {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	var turns []teamstore.Turn
	for _, turn := range state.Turns {
		if turn.SessionID != sessionID || cancelableTurnRank(turn) == 0 {
			continue
		}
		turns = append(turns, turn)
	}
	sort.SliceStable(turns, func(i, j int) bool {
		if cancelableTurnRank(turns[i]) != cancelableTurnRank(turns[j]) {
			return cancelableTurnRank(turns[i]) > cancelableTurnRank(turns[j])
		}
		if !turnSortTime(turns[i]).Equal(turnSortTime(turns[j])) {
			return turnSortTime(turns[i]).Before(turnSortTime(turns[j]))
		}
		return turns[i].ID < turns[j].ID
	})
	return turns
}

func cancelableTurnRank(turn teamstore.Turn) int {
	switch turn.Status {
	case teamstore.TurnStatusRunning:
		return 2
	case teamstore.TurnStatusQueued:
		return 1
	default:
		return 0
	}
}

func cancelScopeReference(scopeLabel string) string {
	if strings.EqualFold(strings.TrimSpace(scopeLabel), "Control chat") {
		return "control chat"
	}
	return "session"
}

func formatCancelOneResponse(state teamstore.State, sessionID string, turn teamstore.Turn, action string, includeRemaining bool) string {
	lines := []string{strings.TrimSpace(action) + ": `" + turn.ID + "`"}
	if summary := turnPromptSummary(state, turn); summary != "" {
		label := "**Canceled prompt:**"
		if turn.Status == teamstore.TurnStatusRunning {
			label = "**Prompt being canceled:**"
		}
		lines = append(lines, "", label, summary)
	}
	if includeRemaining {
		lines = append(lines, "")
		lines = append(lines, formatRemainingQueuedPrompts(state, sessionID, map[string]bool{turn.ID: true})...)
	}
	return strings.Join(lines, "\n")
}

func formatCancelAllResponse(state teamstore.State, canceledQueued []teamstore.Turn, requestedRunning []teamstore.Turn, unavailableRunning []teamstore.Turn, scopeLabel string) string {
	scopeLabel = strings.TrimSpace(scopeLabel)
	if scopeLabel == "" {
		scopeLabel = "Work chat"
	}
	lines := []string{"cancel all requested for this " + scopeLabel + "."}
	if len(canceledQueued)+len(requestedRunning) == 0 && len(unavailableRunning) > 0 {
		lines[0] = "cancel all could not cancel every running request in this " + scopeLabel + "."
	}
	if len(requestedRunning) > 0 {
		lines = append(lines, "", "**Running requests cancel requested:**")
		lines = append(lines, formatTurnPromptList(state, requestedRunning)...)
	}
	if len(canceledQueued) > 0 {
		lines = append(lines, "", "**Queued requests canceled:**")
		lines = append(lines, formatTurnPromptList(state, canceledQueued)...)
	}
	if len(unavailableRunning) > 0 {
		lines = append(lines, "", "**Could not cancel these running requests:**")
		lines = append(lines, formatTurnPromptList(state, unavailableRunning)...)
		lines = append(lines, "", "This helper process does not own their live cancel handles. They may have started before the latest helper loaded or before a restart.")
	}
	if len(canceledQueued)+len(requestedRunning)+len(unavailableRunning) == 0 {
		lines = append(lines, "", "No running or queued requests were canceled.")
	}
	lines = append(lines, "", "No queued prompts remain.")
	return strings.Join(lines, "\n")
}

func formatRemainingQueuedPrompts(state teamstore.State, sessionID string, exclude map[string]bool) []string {
	queued := queuedTurnsForSessionState(state, sessionID)
	if len(exclude) > 0 {
		filtered := queued[:0]
		for _, turn := range queued {
			if exclude[turn.ID] {
				continue
			}
			filtered = append(filtered, turn)
		}
		queued = filtered
	}
	if len(queued) == 0 {
		return []string{"No other prompts are queued."}
	}
	lines := []string{"**Still queued:**"}
	lines = append(lines, formatTurnPromptList(state, queued)...)
	return lines
}

func formatTurnPromptList(state teamstore.State, turns []teamstore.Turn) []string {
	lines := make([]string, 0, len(turns))
	for i, turn := range turns {
		summary := turnPromptSummary(state, turn)
		if summary == "" {
			summary = "Prompt unavailable for `" + turn.ID + "`"
		}
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, summary))
	}
	return lines
}

func (b *Bridge) sendFileCommand(ctx context.Context, session *Session, relPath string) error {
	if relPath == "" {
		root, _ := DefaultOutboundRoot()
		return b.sendToChat(ctx, session.ChatID, "usage: `helper file <relative-path>` or `!file <relative-path>`\nOutbound root: "+root)
	}
	root, ok, err := FileWriteAuthCacheAvailable()
	if err != nil {
		return b.sendToChat(ctx, session.ChatID, "Teams file upload auth check failed: "+err.Error())
	}
	if !ok {
		outboundRoot, _ := DefaultOutboundRoot()
		return b.sendToChat(ctx, session.ChatID, "Teams file upload is not authenticated. Run `codex-proxy teams auth file-write` locally, put files under "+outboundRoot+", then retry `helper file <relative-path>`. Token cache: "+root)
	}
	file, err := PrepareOutboundAttachment(relPath, OutboundAttachmentOptions{})
	if err != nil {
		outboundRoot, _ := DefaultOutboundRoot()
		return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("Teams file upload rejected: %v. `helper file` only reads relative files under the Teams outbound root: %s", err, outboundRoot))
	}
	_, err = b.queueAndSendAttachmentUploadOutbox(ctx, session.ID, "", session.ChatID, "attachment", "file attached: "+file.Name, file, OutboundAttachmentOptions{})
	return err
}

func (b *Bridge) uploadArtifactsFromResult(ctx context.Context, session *Session, turn teamstore.Turn, text string) error {
	blocks := ExtractArtifactManifestBlocks(text)
	if len(blocks) > 0 {
		filtered := blocks[:0]
		for _, block := range blocks {
			if IsPlaceholderArtifactManifestBlock(block) {
				continue
			}
			filtered = append(filtered, block)
		}
		blocks = filtered
	}
	if len(blocks) == 0 {
		return nil
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		return b.sendToChat(ctx, session.ChatID, "artifact upload skipped: cannot resolve Teams outbound root: "+err.Error())
	}
	plans := make([]ArtifactManifestPlan, 0, len(blocks))
	for blockIndex, block := range blocks {
		plan, err := ParseArtifactManifest(block, ArtifactManifestOptions{
			OutboundRoot: root,
			SessionID:    session.ID,
			TurnID:       turn.ID,
		})
		if err != nil {
			return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("artifact manifest %d rejected: %v", blockIndex+1, err))
		}
		plans = append(plans, plan)
	}
	_, ok, err := FileWriteAuthCacheAvailable()
	if err != nil {
		return b.sendToChat(ctx, session.ChatID, "artifact upload auth check failed: "+err.Error())
	}
	if !ok {
		for _, plan := range plans {
			for _, planned := range plan.Files {
				artifactID := artifactRecordID(session.ID, turn.ID, planned.CleanPath, planned.UploadNameSeed)
				if err := b.recordArtifactPlanned(ctx, session, turn, planned, planned.UploadNameSeed, artifactID, "", "auth_unavailable", "Teams file upload is not authenticated"); err != nil {
					return err
				}
			}
		}
		return b.sendToChat(ctx, session.ChatID, "artifact manifest detected, but Teams file upload is not authenticated. Run `codex-proxy teams auth file-write` locally, then retry with `helper file <relative-path>` if needed. Outbound root: "+root)
	}
	for _, plan := range plans {
		for _, planned := range plan.Files {
			file, err := PrepareOutboundAttachment(planned.CleanPath, OutboundAttachmentOptions{Root: root})
			if err != nil {
				return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("artifact upload rejected: %v", err))
			}
			file.UploadName = ArtifactUploadName(session.ID, turn.ID, file.Name, file.Bytes)
			artifactID := artifactRecordID(session.ID, turn.ID, planned.CleanPath, file.UploadName)
			if err := b.recordArtifactPlanned(ctx, session, turn, planned, file.UploadName, artifactID, "", "queued", ""); err != nil {
				return err
			}
			outbox, err := b.queueAndSendAttachmentUploadOutbox(ctx, session.ID, turn.ID, session.ChatID, "artifact", "artifact attached: "+file.Name, file, OutboundAttachmentOptions{ArtifactIDs: []string{artifactID}})
			if err != nil {
				_ = b.recordArtifactPlanned(ctx, session, turn, planned, file.UploadName, artifactID, "", "failed", err.Error())
				return err
			}
			if err := b.recordArtifactOutboxState(ctx, session, turn, planned, file.UploadName, artifactID, outbox); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Bridge) queueAndSendAttachmentUploadOutbox(ctx context.Context, sessionID string, turnID string, chatID string, kind string, message string, file OutboundAttachmentFile, opts OutboundAttachmentOptions) (teamstore.OutboxMessage, error) {
	uploadFolder := strings.TrimSpace(opts.UploadFolder)
	if uploadFolder == "" {
		uploadFolder = defaultOutboundUploadFolder
	}
	staged, err := stageOutboundAttachmentForOutbox(file)
	if err != nil {
		return teamstore.OutboxMessage{}, err
	}
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:                     attachmentUploadOutboxID(chatID, kind, file),
		SessionID:              sessionID,
		TurnID:                 turnID,
		TeamsChatID:            chatID,
		Kind:                   kind,
		Body:                   strings.TrimSpace(message),
		AttachmentPath:         strings.TrimSpace(staged.Path),
		AttachmentName:         strings.TrimSpace(staged.Name),
		AttachmentUploadName:   strings.TrimSpace(staged.UploadName),
		AttachmentContentType:  strings.TrimSpace(staged.ContentType),
		AttachmentUploadFolder: uploadFolder,
		AttachmentSize:         staged.Size,
		AttachmentHash:         attachmentContentHash(staged.Bytes),
		ArtifactIDs:            append([]string(nil), opts.ArtifactIDs...),
	})
	if err != nil {
		return teamstore.OutboxMessage{}, err
	}
	if queued.Status != teamstore.OutboxStatusSent {
		if err := b.flushPendingOutboxForChat(ctx, chatID); err != nil && !isOutboxDeliveryDeferred(err) {
			return teamstore.OutboxMessage{}, err
		}
		state, err := b.store.Load(ctx)
		if err != nil {
			return teamstore.OutboxMessage{}, err
		}
		if current, ok := state.OutboxMessages[queued.ID]; ok {
			queued = current
		}
	}
	return queued, nil
}

func stageOutboundAttachmentForOutbox(file OutboundAttachmentFile) (OutboundAttachmentFile, error) {
	root, err := DefaultOutboundRoot()
	if err != nil {
		return OutboundAttachmentFile{}, err
	}
	if err := ensureOutboundRoot(root); err != nil {
		return OutboundAttachmentFile{}, err
	}
	hash := attachmentContentHash(file.Bytes)
	hashPrefix := hash
	if len(hashPrefix) > 16 {
		hashPrefix = hashPrefix[:16]
	}
	stageRoot := filepath.Join(root, ".outbox")
	if err := ensurePrivateDirectory(stageRoot); err != nil {
		return OutboundAttachmentFile{}, err
	}
	stageDir := filepath.Join(stageRoot, hashPrefix)
	if err := ensurePrivateDirectory(stageDir); err != nil {
		return OutboundAttachmentFile{}, err
	}
	uploadName := strings.TrimSpace(file.UploadName)
	if uploadName == "" {
		uploadName = safeAttachmentName(file.Name)
	}
	if uploadName == "" || strings.HasPrefix(uploadName, ".") || !safeDrivePathSegment(uploadName) {
		return OutboundAttachmentFile{}, fmt.Errorf("unsafe staged upload file name")
	}
	stagePath := filepath.Join(stageDir, uploadName)
	tmp, err := os.CreateTemp(stageDir, ".stage-*.tmp")
	if err != nil {
		return OutboundAttachmentFile{}, outboundPathError("create staged Teams upload file", err)
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return OutboundAttachmentFile{}, err
	}
	if _, err := tmp.Write(file.Bytes); err != nil {
		_ = tmp.Close()
		return OutboundAttachmentFile{}, outboundPathError("write staged Teams upload file", err)
	}
	if err := tmp.Close(); err != nil {
		return OutboundAttachmentFile{}, outboundPathError("close staged Teams upload file", err)
	}
	if err := os.Rename(tmpPath, stagePath); err != nil {
		return OutboundAttachmentFile{}, outboundPathError("publish staged Teams upload file", err)
	}
	keep = true
	if err := os.Chmod(stagePath, 0o600); err != nil {
		return OutboundAttachmentFile{}, err
	}
	staged := file
	staged.Path = stagePath
	staged.UploadName = uploadName
	return staged, nil
}

func ensurePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
		return os.Chmod(path, 0o700)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("Teams staging directory must not be a symlink")
	}
	if !info.IsDir() {
		return fmt.Errorf("Teams staging path is not a directory")
	}
	return os.Chmod(path, 0o700)
}

func (b *Bridge) queueAndSendAttachmentOutbox(ctx context.Context, sessionID string, turnID string, chatID string, kind string, message string, item DriveItem) (teamstore.OutboxMessage, error) {
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:              attachmentOutboxID(chatID, kind, item),
		SessionID:       sessionID,
		TurnID:          turnID,
		TeamsChatID:     chatID,
		Kind:            kind,
		Body:            strings.TrimSpace(message),
		DriveItemID:     strings.TrimSpace(item.ID),
		DriveItemName:   strings.TrimSpace(item.Name),
		DriveItemETag:   strings.TrimSpace(item.ETag),
		DriveItemWebURL: strings.TrimSpace(item.WebURL),
		DriveItemWebDav: strings.TrimSpace(item.WebDavURL),
	})
	if err != nil {
		return teamstore.OutboxMessage{}, err
	}
	if queued.Status != teamstore.OutboxStatusSent {
		if err := b.flushPendingOutboxForChat(ctx, chatID); err != nil && !isOutboxDeliveryDeferred(err) {
			return teamstore.OutboxMessage{}, err
		}
		state, err := b.store.Load(ctx)
		if err != nil {
			return teamstore.OutboxMessage{}, err
		}
		if current, ok := state.OutboxMessages[queued.ID]; ok {
			queued = current
		}
	}
	return queued, nil
}

func attachmentUploadOutboxID(chatID string, kind string, file OutboundAttachmentFile) string {
	id := strings.TrimSpace(firstNonEmptyString(file.UploadName, file.Path, file.Name))
	sum := sha256.Sum256([]byte(chatID + "\x00" + kind + "\x00" + id))
	return "outbox:attachment-upload:" + hex.EncodeToString(sum[:])
}

func attachmentOutboxID(chatID string, kind string, item DriveItem) string {
	id := strings.TrimSpace(firstNonEmptyString(item.ID, item.WebURL, item.WebDavURL, item.Name))
	sum := sha256.Sum256([]byte(chatID + "\x00" + kind + "\x00" + id))
	return "outbox:attachment:" + hex.EncodeToString(sum[:])
}

func attachmentContentHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func driveItemFromOutbox(outbox teamstore.OutboxMessage) DriveItem {
	return DriveItem{
		ID:        strings.TrimSpace(outbox.DriveItemID),
		Name:      strings.TrimSpace(outbox.DriveItemName),
		ETag:      strings.TrimSpace(outbox.DriveItemETag),
		WebURL:    strings.TrimSpace(outbox.DriveItemWebURL),
		WebDavURL: strings.TrimSpace(outbox.DriveItemWebDav),
	}
}

func validateArtifactManifestFile(req ArtifactManifestValidationRequest) (ArtifactManifestFileInfo, error) {
	info, err := os.Lstat(req.LocalPath)
	if err != nil {
		return ArtifactManifestFileInfo{}, outboundPathError("inspect artifact file", err)
	}
	return ArtifactManifestFileInfo{
		Size:      info.Size(),
		IsDir:     info.IsDir(),
		IsSymlink: info.Mode()&os.ModeSymlink != 0,
	}, nil
}

func artifactRecordID(sessionID string, turnID string, cleanPath string, uploadName string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(sessionID) + "\x00" + strings.TrimSpace(turnID) + "\x00" + strings.TrimSpace(cleanPath) + "\x00" + strings.TrimSpace(uploadName)))
	return "artifact:" + hex.EncodeToString(sum[:])
}

func (b *Bridge) recordArtifactPlanned(ctx context.Context, session *Session, turn teamstore.Turn, planned ArtifactManifestFile, uploadName string, artifactID string, outboxID string, status string, reason string) error {
	if b.store == nil {
		return nil
	}
	if strings.TrimSpace(artifactID) == "" {
		artifactID = artifactRecordID(session.ID, turn.ID, planned.CleanPath, uploadName)
	}
	_, err := b.store.UpsertArtifactRecord(ctx, teamstore.ArtifactRecord{
		ID:           artifactID,
		SessionID:    session.ID,
		TurnID:       turn.ID,
		Path:         planned.CleanPath,
		UploadName:   uploadName,
		OutboxID:     outboxID,
		Status:       status,
		StatusReason: reason,
	})
	return err
}

func (b *Bridge) recordArtifactOutboxState(ctx context.Context, session *Session, turn teamstore.Turn, planned ArtifactManifestFile, uploadName string, artifactID string, outbox teamstore.OutboxMessage) error {
	status := artifactStatusFromOutbox(outbox)
	reason := strings.TrimSpace(outbox.LastSendError)
	record := teamstore.ArtifactRecord{
		ID:             artifactID,
		SessionID:      session.ID,
		TurnID:         turn.ID,
		Path:           planned.CleanPath,
		UploadName:     uploadName,
		DriveItemID:    outbox.DriveItemID,
		OutboxID:       outbox.ID,
		TeamsMessageID: outbox.TeamsMessageID,
		Status:         status,
		StatusReason:   reason,
		Error:          reason,
	}
	if status == "uploaded" {
		record.Error = ""
		record.SentAt = outbox.SentAt
		record.UploadedAt = outbox.SentAt
	} else if status == "drive_uploaded" {
		record.UploadedAt = time.Now()
	}
	_, err := b.store.UpsertArtifactRecord(ctx, record)
	return err
}

func artifactStatusFromOutbox(outbox teamstore.OutboxMessage) string {
	switch outbox.Status {
	case teamstore.OutboxStatusSent:
		if strings.TrimSpace(outbox.DriveItemID) != "" && strings.TrimSpace(outbox.TeamsMessageID) != "" {
			return "uploaded"
		}
	case teamstore.OutboxStatusAccepted:
		if strings.TrimSpace(outbox.DriveItemID) != "" {
			return "message_accepted"
		}
	case teamstore.OutboxStatusSending:
		if strings.TrimSpace(outbox.DriveItemID) != "" {
			return "drive_uploaded"
		}
		return "sending"
	case teamstore.OutboxStatusSkipped:
		return "skipped"
	}
	if strings.TrimSpace(outbox.LastSendError) != "" {
		if strings.TrimSpace(outbox.DriveItemID) != "" {
			return "message_failed"
		}
		return "failed"
	}
	if strings.TrimSpace(outbox.DriveItemID) != "" {
		return "drive_uploaded"
	}
	return "queued"
}

func (b *Bridge) renameSessionChat(ctx context.Context, session *Session, title string) error {
	title = SanitizeDashboardTitle(title)
	if title == "" {
		return b.sendToChat(ctx, session.ChatID, "usage: `helper rename <title>` or `!rename <title>`")
	}
	topic := WorkChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		SessionID:    session.ID,
		UserTitle:    title,
		Cwd:          session.Cwd,
	})
	if err := b.graph.UpdateChatTopic(ctx, session.ChatID, topic); err != nil {
		return b.sendToChat(ctx, session.ChatID, "rename failed: "+err.Error())
	}
	topic = SanitizeTopic(topic)
	session.Topic = topic
	session.UserTitle = title
	session.TitleSource = sessionTitleSourceUser
	session.UpdatedAt = time.Now()
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	if err := b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.Sessions[session.ID]
		current.TeamsTopic = topic
		current.UserTitle = title
		current.TitleSource = sessionTitleSourceUser
		current.UpdatedAt = session.UpdatedAt
		state.Sessions[session.ID] = current
		return nil
	}); err != nil {
		return err
	}
	return b.sendToChat(ctx, session.ChatID, "Work chat renamed.\n\nNew title:\n"+shortenTeamsLine(topic, 96)+"\n\nUse `helper details` to see the full Teams title and link.")
}

func parseRenameHostnameArgument(argument string) (string, bool) {
	name, rest := splitDashboardCommandBody(argument)
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "hostname":
		return strings.TrimSpace(rest), true
	default:
		return "", false
	}
}

type hostnameRenameSessionTarget struct {
	ID          string
	ChatID      string
	Topic       string
	UserTitle   string
	TitleSource string
	Cwd         string
	UpdatedAt   time.Time
}

func hostnameRenameSessionTargets(reg Registry, state teamstore.State) []hostnameRenameSessionTarget {
	targets := make(map[string]hostnameRenameSessionTarget)
	for id, session := range state.Sessions {
		id = strings.TrimSpace(id)
		if id == "" || isControlFallbackSessionID(id) || strings.TrimSpace(session.TeamsChatID) == "" {
			continue
		}
		targets[id] = hostnameRenameSessionTarget{
			ID:          id,
			ChatID:      strings.TrimSpace(session.TeamsChatID),
			Topic:       session.TeamsTopic,
			UserTitle:   session.UserTitle,
			TitleSource: session.TitleSource,
			Cwd:         session.Cwd,
			UpdatedAt:   session.UpdatedAt,
		}
	}
	for _, session := range reg.Sessions {
		id := strings.TrimSpace(session.ID)
		if id == "" || isControlFallbackSessionID(id) || strings.TrimSpace(session.ChatID) == "" {
			continue
		}
		targets[id] = hostnameRenameSessionTarget{
			ID:          id,
			ChatID:      strings.TrimSpace(session.ChatID),
			Topic:       session.Topic,
			UserTitle:   session.UserTitle,
			TitleSource: session.TitleSource,
			Cwd:         session.Cwd,
			UpdatedAt:   session.UpdatedAt,
		}
	}
	ids := make([]string, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]hostnameRenameSessionTarget, 0, len(ids))
	for _, id := range ids {
		out = append(out, targets[id])
	}
	return out
}

func workChatTopicForHostnameRename(target hostnameRenameSessionTarget, newLabel string, profile string, oldLabels []string) string {
	displayTopic := strings.TrimSpace(target.Topic)
	if strings.TrimSpace(target.UserTitle) == "" {
		displayTopic = stripWorkChatMachineTitlePrefix(displayTopic, oldLabels)
	}
	return SanitizeTopic(WorkChatTitle(ChatTitleOptions{
		MachineLabel: newLabel,
		Profile:      profile,
		UserTitle:    target.UserTitle,
		Topic:        displayTopic,
		Cwd:          target.Cwd,
	}))
}

func stripWorkChatMachineTitlePrefix(topic string, oldLabels []string) string {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return ""
	}
	for _, label := range oldLabels {
		prefix := strings.TrimSpace(DefaultWorkChatMarker + " " + machineTitlePart(label))
		if prefix == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(topic, prefix+" - "); ok {
			return strings.TrimSpace(rest)
		}
		if strings.EqualFold(topic, prefix) {
			return ""
		}
	}
	return topic
}

func machineHostnameRenameOldLabels(machine teamstore.MachineRecord, state teamstore.State) []string {
	seen := map[string]bool{}
	var labels []string
	add := func(value string) {
		value = sanitizeMachineHostnameOverride(value)
		if value == "" {
			return
		}
		key := strings.ToLower(value)
		if seen[key] {
			return
		}
		seen[key] = true
		labels = append(labels, value)
	}
	add(machine.Label)
	add(machine.Hostname)
	add(state.MachineIdentity.Label)
	add(state.MachineIdentity.Hostname)
	add(machineLabel())
	add("machine")
	return labels
}

func (b *Bridge) renameMachineHostname(ctx context.Context, raw string, replyChatID string) error {
	label := sanitizeMachineHostnameOverride(raw)
	if label == "" {
		return b.sendToChat(ctx, replyChatID, "usage: `helper rename hostname <name>`")
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	if b.machine.ID == "" {
		b.machine = MachineRecordForUser(b.user, b.scope)
		b.applyRegistryMachineHostnameOverride()
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	oldLabels := machineHostnameRenameOldLabels(b.machine, state)
	controlChatID := firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID)
	controlUserTitle := firstNonEmptyString(b.reg.ControlChatUserTitle, state.ControlChat.UserTitle)
	controlTopic := ""
	controlUpdated := false
	var failures []string
	if strings.TrimSpace(controlChatID) != "" {
		controlTopic = SanitizeTopic(ControlChatTitle(ChatTitleOptions{
			MachineLabel: label,
			Profile:      b.scope.Profile,
			UserTitle:    controlUserTitle,
		}))
		if err := b.graph.UpdateChatTopic(ctx, controlChatID, controlTopic); err != nil {
			failures = append(failures, "control chat: "+err.Error())
		} else {
			controlUpdated = true
		}
	}

	sessionTopics := map[string]string{}
	for _, target := range hostnameRenameSessionTargets(b.reg, state) {
		topic := workChatTopicForHostnameRename(target, label, b.scope.Profile, oldLabels)
		if err := b.graph.UpdateChatTopic(ctx, target.ChatID, topic); err != nil {
			failures = append(failures, target.ID+": "+err.Error())
			continue
		}
		sessionTopics[target.ID] = topic
	}

	now := time.Now()
	applyMachineHostnameOverrideToRecord(&b.machine, label)
	b.reg.MachineHostnameOverride = label
	if controlUpdated {
		b.reg.ControlChatID = controlChatID
		b.reg.ControlChatTopic = controlTopic
	}
	for sessionID, topic := range sessionTopics {
		if session := b.reg.SessionByID(sessionID); session != nil {
			session.Topic = topic
			session.UpdatedAt = now
		}
	}
	if err := b.store.UpdateIfChanged(ctx, func(state *teamstore.State) (bool, error) {
		changed := false
		setMachineString := func(target *string, value string) {
			if *target != value {
				*target = value
				changed = true
			}
		}
		if state.MachineIdentity.ID == "" {
			state.MachineIdentity.ID = b.machine.ID
			state.MachineIdentity.CreatedAt = now
			changed = true
		}
		setMachineString(&state.MachineIdentity.Label, label)
		setMachineString(&state.MachineIdentity.Hostname, label)
		setMachineString(&state.MachineIdentity.ScopeID, b.scope.ID)
		setMachineString(&state.MachineIdentity.AccountID, b.user.ID)
		setMachineString(&state.MachineIdentity.UserPrincipal, b.user.UserPrincipalName)
		setMachineString(&state.MachineIdentity.Profile, b.scope.Profile)
		if state.MachineIdentity.Kind != b.machine.Kind {
			state.MachineIdentity.Kind = b.machine.Kind
			changed = true
		}
		if state.MachineIdentity.Priority != b.machine.Priority {
			state.MachineIdentity.Priority = b.machine.Priority
			changed = true
		}
		if changed || state.MachineIdentity.UpdatedAt.IsZero() {
			state.MachineIdentity.UpdatedAt = now
			changed = true
		}
		if machine := state.Machines[b.machine.ID]; machine.ID != "" {
			if machine.Label != label || machine.Hostname != label {
				machine.Label = label
				machine.Hostname = label
				machine.UpdatedAt = now
				state.Machines[machine.ID] = machine
				changed = true
			}
		}
		if controlUpdated {
			if state.ControlChat.MachineID != b.machine.ID {
				state.ControlChat.MachineID = b.machine.ID
				changed = true
			}
			if state.ControlChat.ScopeID != b.scope.ID {
				state.ControlChat.ScopeID = b.scope.ID
				changed = true
			}
			if state.ControlChat.AccountID != b.user.ID {
				state.ControlChat.AccountID = b.user.ID
				changed = true
			}
			if state.ControlChat.Profile != b.scope.Profile {
				state.ControlChat.Profile = b.scope.Profile
				changed = true
			}
			if state.ControlChat.TeamsChatID == "" {
				state.ControlChat.TeamsChatID = controlChatID
				changed = true
			}
			if state.ControlChat.BoundAt.IsZero() {
				state.ControlChat.BoundAt = now
				changed = true
			}
			if state.ControlChat.TeamsChatTopic != controlTopic {
				state.ControlChat.TeamsChatTopic = controlTopic
				state.ControlChat.UpdatedAt = now
				changed = true
			}
		}
		for sessionID, topic := range sessionTopics {
			current := state.Sessions[sessionID]
			if current.ID == "" {
				continue
			}
			if current.TeamsTopic != topic {
				current.TeamsTopic = topic
				current.UpdatedAt = now
				state.Sessions[sessionID] = current
				changed = true
			}
		}
		return changed, nil
	}); err != nil {
		return err
	}
	if err := b.Save(); err != nil {
		return err
	}

	var lines []string
	lines = append(lines, "Hostname renamed.\n\nNew hostname: "+label)
	if controlUpdated {
		lines = append(lines, "Control chat: updated")
	} else if strings.TrimSpace(controlChatID) == "" {
		lines = append(lines, "Control chat: not bound")
	}
	lines = append(lines, fmt.Sprintf("Work chats: %d updated", len(sessionTopics)))
	if len(failures) > 0 {
		lines = append(lines, "")
		lines = append(lines, "Some chat titles were not updated:")
		const maxReportedFailures = 4
		for i, failure := range failures {
			if i >= maxReportedFailures {
				lines = append(lines, fmt.Sprintf("- and %d more", len(failures)-i))
				break
			}
			lines = append(lines, "- "+shortenTeamsLine(failure, 140))
		}
	}
	return b.sendToChat(ctx, replyChatID, strings.Join(lines, "\n"))
}

func (b *Bridge) renameControlChat(ctx context.Context, title string) error {
	title = SanitizeDashboardTitle(title)
	if title == "" {
		return b.sendControl(ctx, "usage: `helper rename <title>` or `rename <title>`")
	}
	if strings.TrimSpace(b.reg.ControlChatID) == "" {
		return b.sendControl(ctx, "rename failed: control chat is not bound yet")
	}
	topic := ControlChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		UserTitle:    title,
	})
	if err := b.graph.UpdateChatTopic(ctx, b.reg.ControlChatID, topic); err != nil {
		return b.sendControl(ctx, "rename failed: "+err.Error())
	}
	topic = SanitizeTopic(topic)
	b.reg.ControlChatTopic = topic
	b.reg.ControlChatUserTitle = title
	b.reg.ControlChatTitleSource = sessionTitleSourceUser
	chat := Chat{ID: b.reg.ControlChatID, Topic: topic, WebURL: b.reg.ControlChatURL, ChatType: "meeting"}
	if err := b.recordControlChatBindingWithTitle(ctx, chat, title, sessionTitleSourceUser); err != nil {
		return err
	}
	if err := b.Save(); err != nil {
		return err
	}
	return b.sendControl(ctx, "Control chat renamed.\n\nNew title:\n"+shortenTeamsLine(topic, 96))
}

func (b *Bridge) refreshWorkChatTitleFromCodexHistory(ctx context.Context, session *Session) error {
	if b == nil || session == nil || !sessionAllowsAutoTitleUpdate(*session) || strings.TrimSpace(session.CodexThreadID) == "" {
		return nil
	}
	projects, err := discoverCodexProjectsForTeams(ctx, "")
	if err != nil {
		return nil
	}
	for _, project := range projects {
		for _, local := range project.Sessions {
			if local.SessionID != session.CodexThreadID {
				continue
			}
			if local.ProjectPath == "" {
				local.ProjectPath = project.Path
			}
			return b.maybeUpdateWorkChatTitleFromLocalSession(ctx, session, local)
		}
	}
	return nil
}

func (b *Bridge) refreshWorkChatTitleFromExecutionResult(ctx context.Context, session *Session, result ExecutionResult) (bool, error) {
	title := strings.TrimSpace(result.CodexThreadTitle)
	if b == nil || session == nil || title == "" {
		return false, nil
	}
	local := codexhistory.Session{
		SessionID:   firstNonEmptyString(result.CodexThreadID, session.CodexThreadID),
		Summary:     title,
		ProjectPath: session.Cwd,
	}
	return true, b.maybeUpdateWorkChatTitleFromLocalSession(ctx, session, local)
}

func (b *Bridge) maybeUpdateWorkChatTitleFromLocalSession(ctx context.Context, session *Session, local codexhistory.Session) error {
	if b == nil || b.graph == nil || session == nil || strings.TrimSpace(session.ChatID) == "" || !sessionAllowsAutoTitleUpdate(*session) {
		return nil
	}
	localTitle := localCodexTitleForWorkChat(local)
	if localTitle == "" {
		return nil
	}
	desiredTopic := SanitizeTopic(WorkChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		SessionID:    session.ID,
		Topic:        localTitle,
		Cwd:          firstNonEmptyString(local.ProjectPath, session.Cwd),
	}))
	if strings.TrimSpace(session.Topic) == desiredTopic {
		return nil
	}
	if err := b.graph.UpdateChatTopic(ctx, session.ChatID, desiredTopic); err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams work chat title update skipped for %s: %v\n", session.ID, err)
		}
		return nil
	}
	now := time.Now()
	session.Topic = desiredTopic
	session.TitleSource = sessionTitleSourceAuto
	session.UpdatedAt = now
	if current := b.reg.SessionByID(session.ID); current != nil {
		current.Topic = desiredTopic
		current.TitleSource = sessionTitleSourceAuto
		current.UpdatedAt = now
	}
	if b.store != nil {
		if err := b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
			current := state.Sessions[session.ID]
			if current.ID == "" {
				return nil
			}
			current.TeamsTopic = desiredTopic
			current.TitleSource = sessionTitleSourceAuto
			current.UpdatedAt = now
			state.Sessions[session.ID] = current
			return nil
		}); err != nil {
			return err
		}
	}
	if strings.TrimSpace(b.registryPath) != "" {
		if err := b.Save(); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams registry title save skipped for %s: %v\n", session.ID, err)
		}
	}
	return nil
}

func sessionAllowsAutoTitleUpdate(session Session) bool {
	source := strings.ToLower(strings.TrimSpace(session.TitleSource))
	if source == sessionTitleSourceUser || strings.TrimSpace(session.UserTitle) != "" {
		return false
	}
	if source == sessionTitleSourceAuto {
		return true
	}
	if source != "" {
		return false
	}
	return legacySessionAllowsAutoTitleUpdate(session)
}

func SessionAllowsAutoTitleUpdate(session Session) bool {
	return sessionAllowsAutoTitleUpdate(session)
}

func legacySessionAllowsAutoTitleUpdate(session Session) bool {
	topic := strings.TrimSpace(session.Topic)
	if topic == "" {
		return true
	}
	if strings.Contains(topic, "New message in ") {
		return true
	}
	return strings.Contains(strings.ToLower(topic), "codex work")
}

func localCodexTitleForWorkChat(local codexhistory.Session) string {
	if strings.TrimSpace(local.Summary) == "" && strings.TrimSpace(local.FirstPrompt) == "" {
		return ""
	}
	return strings.TrimSpace(local.DisplayTitle())
}

func shortenTeamsLine(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 || len([]rune(text)) <= limit {
		return text
	}
	runes := []rune(text)
	if limit <= 1 {
		return string(runes[:limit])
	}
	return strings.TrimSpace(string(runes[:limit-1])) + "…"
}

func (b *Bridge) fileWriteGraph() (*GraphClient, error) {
	if b.fileGraph != nil {
		return b.fileGraph, nil
	}
	graph, err := NewFileWriteGraphClientWithHTTPClient(b.out, b.httpClient)
	if err != nil {
		return nil, err
	}
	b.fileGraph = graph
	return graph, nil
}

func (b *Bridge) registerRunningTurnCancel(sessionID string, turnID string, cancel context.CancelFunc) func() {
	if b == nil || strings.TrimSpace(turnID) == "" || cancel == nil {
		return func() {}
	}
	b.runningTurnMu.Lock()
	if b.runningTurnCancels == nil {
		b.runningTurnCancels = make(map[string]*runningTurnCancel)
	}
	b.runningTurnCancels[turnID] = &runningTurnCancel{sessionID: strings.TrimSpace(sessionID), cancel: cancel}
	b.runningTurnMu.Unlock()
	return func() {
		b.runningTurnMu.Lock()
		delete(b.runningTurnCancels, turnID)
		b.runningTurnMu.Unlock()
	}
}

func (b *Bridge) requestRunningTurnCancel(turnID string) bool {
	return b.requestRunningTurnCancelWithOptions(turnID, "canceled by user", false)
}

func (b *Bridge) requestRunningTurnCancelWithOptions(turnID string, reason string, silent bool) bool {
	if b == nil || strings.TrimSpace(turnID) == "" {
		return false
	}
	b.runningTurnMu.Lock()
	running := b.runningTurnCancels[turnID]
	if running == nil {
		b.runningTurnMu.Unlock()
		return false
	}
	running.requested = true
	running.reason = strings.TrimSpace(reason)
	running.silent = silent
	cancel := running.cancel
	b.runningTurnMu.Unlock()
	cancel()
	return true
}

func (b *Bridge) runningTurnCancelState(turnID string) (bool, string, bool) {
	if b == nil || strings.TrimSpace(turnID) == "" {
		return false, "", false
	}
	b.runningTurnMu.Lock()
	defer b.runningTurnMu.Unlock()
	running := b.runningTurnCancels[turnID]
	if running == nil || !running.requested {
		return false, "", false
	}
	return true, firstNonEmptyString(running.reason, "canceled by user"), running.silent
}

func (b *Bridge) cancelSupersededRunningTurnsForSession(sessionID string, activeTurnID string) []string {
	if b == nil {
		return nil
	}
	sessionID = strings.TrimSpace(sessionID)
	activeTurnID = strings.TrimSpace(activeTurnID)
	if sessionID == "" || activeTurnID == "" {
		return nil
	}
	type cancelTarget struct {
		turnID string
		cancel context.CancelFunc
	}
	var targets []cancelTarget
	b.runningTurnMu.Lock()
	for turnID, running := range b.runningTurnCancels {
		if running == nil || turnID == activeTurnID || strings.TrimSpace(running.sessionID) != sessionID {
			continue
		}
		running.requested = true
		running.reason = "superseded by a newer Teams request after recovery"
		running.silent = true
		if running.cancel != nil {
			targets = append(targets, cancelTarget{turnID: turnID, cancel: running.cancel})
		}
	}
	b.runningTurnMu.Unlock()
	for _, target := range targets {
		target.cancel()
	}
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		out = append(out, target.turnID)
	}
	return out
}

func (b *Bridge) runQueuedTurn(ctx context.Context, session *Session, turn teamstore.Turn, chatID string, text string) error {
	return b.runQueuedTurnInput(ctx, session, turn, chatID, ExecutionInput{Prompt: text})
}

type recentDuplicatePrompt struct {
	Inbound teamstore.InboundEvent
	Turn    teamstore.Turn
}

func (b *Bridge) ignoreRecentDuplicateSessionPrompt(ctx context.Context, session *Session, msg ChatMessage, text string) (bool, error) {
	return b.ignoreRecentDuplicateSessionPromptWithSnapshot(ctx, session, msg, text, nil)
}

func (b *Bridge) ignoreRecentDuplicateSessionPromptWithSnapshot(ctx context.Context, session *Session, msg ChatMessage, text string, snapshot *teamstore.State) (bool, error) {
	if b == nil || session == nil || b.store == nil {
		return false, nil
	}
	duplicate, ok, err := b.recentDuplicateSessionPromptWithSnapshot(ctx, session, msg, text, time.Now(), snapshot)
	if err != nil || !ok {
		return ok, err
	}
	inbound, created, err := b.persistInboundWithStatusAndSource(ctx, session, msg, teamstore.InboundStatusIgnored, "teams_duplicate_prompt")
	if err != nil {
		return true, err
	}
	if !created {
		return true, nil
	}
	return true, b.sendToChat(ctx, session.ChatID, recentDuplicatePromptNotice(inbound, duplicate))
}

func (b *Bridge) recentDuplicateSessionPrompt(ctx context.Context, session *Session, msg ChatMessage, text string, now time.Time) (recentDuplicatePrompt, bool, error) {
	return b.recentDuplicateSessionPromptWithSnapshot(ctx, session, msg, text, now, nil)
}

func (b *Bridge) recentDuplicateSessionPromptWithSnapshot(ctx context.Context, session *Session, msg ChatMessage, text string, now time.Time, snapshot *teamstore.State) (recentDuplicatePrompt, bool, error) {
	if b == nil || session == nil || b.store == nil || messageHasPromptDedupUnsafeAttachments(msg) {
		return recentDuplicatePrompt{}, false, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	text = strings.TrimSpace(firstNonEmptyString(text, promptTextFromTeamsMessageHTML(msg.Body.Content)))
	hash := normalizedTextHash(text)
	if hash == "" {
		return recentDuplicatePrompt{}, false, nil
	}
	var state teamstore.State
	if snapshot != nil {
		state = *snapshot
	} else {
		var err error
		state, err = b.store.SessionTurnQueueSnapshot(ctx, session.ID)
		if err != nil {
			return recentDuplicatePrompt{}, false, err
		}
	}
	var best recentDuplicatePrompt
	var bestAt time.Time
	for _, inbound := range state.InboundEvents {
		if inbound.SessionID != session.ID ||
			inbound.TeamsChatID != session.ChatID ||
			inbound.TeamsMessageID == msg.ID ||
			!strings.EqualFold(strings.TrimSpace(inbound.Source), "teams") ||
			inbound.TextHash == "" ||
			inbound.TextHash != hash ||
			inbound.TurnID == "" {
			continue
		}
		turn, ok := state.Turns[inbound.TurnID]
		if !ok || turn.SessionID != session.ID || !turnStatusSuppressesDuplicatePrompt(turn.Status) {
			continue
		}
		seenAt := inboundEventActivityTime(inbound)
		if seenAt.IsZero() || now.Sub(seenAt) < 0 || now.Sub(seenAt) > recentDuplicateSessionPromptWindow {
			continue
		}
		if bestAt.IsZero() || seenAt.After(bestAt) {
			best = recentDuplicatePrompt{Inbound: inbound, Turn: turn}
			bestAt = seenAt
		}
	}
	if best.Inbound.ID == "" {
		return recentDuplicatePrompt{}, false, nil
	}
	return best, true, nil
}

func messageHasPromptDedupUnsafeAttachments(msg ChatMessage) bool {
	if len(msg.Attachments) > 0 {
		return true
	}
	return len(HostedContentIDsFromHTML(msg.Body.Content)) > 0
}

func turnStatusSuppressesDuplicatePrompt(status teamstore.TurnStatus) bool {
	switch status {
	case teamstore.TurnStatusQueued, teamstore.TurnStatusRunning, teamstore.TurnStatusCompleted:
		return true
	default:
		return false
	}
}

func inboundEventActivityTime(inbound teamstore.InboundEvent) time.Time {
	if !inbound.ReceivedAt.IsZero() {
		return inbound.ReceivedAt
	}
	if !inbound.CreatedAt.IsZero() {
		return inbound.CreatedAt
	}
	return inbound.UpdatedAt
}

func recentDuplicatePromptNotice(inbound teamstore.InboundEvent, duplicate recentDuplicatePrompt) string {
	turnID := firstNonEmptyString(duplicate.Turn.ID, duplicate.Inbound.TurnID)
	var b strings.Builder
	b.WriteString("Duplicate request ignored.\n\n")
	b.WriteString("I already accepted the same Teams prompt in this chat recently, so I will not run it again.\n")
	if turnID != "" {
		b.WriteString("\nPrevious turn: `")
		b.WriteString(turnID)
		b.WriteString("`")
	}
	if !duplicate.Inbound.ReceivedAt.IsZero() {
		b.WriteString("\nAccepted at: `")
		b.WriteString(duplicate.Inbound.ReceivedAt.Format(time.RFC3339))
		b.WriteString("`")
	}
	if strings.TrimSpace(inbound.TeamsMessageID) != "" {
		b.WriteString("\nIgnored duplicate Teams message: `")
		b.WriteString(strings.TrimSpace(inbound.TeamsMessageID))
		b.WriteString("`")
	}
	b.WriteString("\n\nTo intentionally rerun it, change the wording slightly or use `helper retry <turn-id>` if the earlier turn failed.")
	return b.String()
}

func (b *Bridge) runQueuedTurnWithExecutor(ctx context.Context, executor Executor, session *Session, turn teamstore.Turn, chatID string, text string) error {
	return b.runQueuedTurnInputWithExecutor(ctx, executor, session, turn, chatID, ExecutionInput{Prompt: text})
}

func (b *Bridge) runQueuedTurnInput(ctx context.Context, session *Session, turn teamstore.Turn, chatID string, input ExecutionInput) error {
	return b.runQueuedTurnInputWithExecutor(ctx, b.executor, session, turn, chatID, input)
}

func (b *Bridge) runQueuedTurnInputWithExecutor(ctx context.Context, executor Executor, session *Session, turn teamstore.Turn, chatID string, input ExecutionInput) error {
	session = sessionWithTurnModelProfile(session, turn)
	if b.currentLeaseGeneration() > 0 {
		if err := b.ensureActiveControlLease(ctx); err != nil {
			return err
		}
	}
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	b.cancelSupersededRunningTurnsForSession(sessionID, turn.ID)
	plan, handled, err := b.prepareBeaconTurnExecution(ctx, session, turn)
	if handled {
		message := "beacon execution could not start"
		if err != nil {
			message = err.Error()
		}
		if cleanupErr := b.recordBeaconTurnStartFailure(ctx, session, turn, plan, beaconTurnStartFailureProviderReason(plan, message)); cleanupErr != nil {
			return cleanupErr
		}
		if _, markErr := b.store.MarkTurnFailed(ctx, turn.ID, message); markErr != nil {
			return markErr
		}
		return b.queueAndSendOutboxChunksWithOptions(ctx, session.ID, turn.ID, chatID, "helper", message, outboxQueueOptions{
			MentionOwner:     true,
			NotificationKind: "needs_attention",
		})
	}
	if plan.Action == beacon.TurnRunBeacon || plan.Action == beacon.TurnWaitAllocation {
		if plan.Action == beacon.TurnWaitAllocation {
			_ = b.queueAndSendOutboxChunksWithOptions(ctx, session.ID, turn.ID, chatID, "helper", formatBeaconTurnAllocationProgress(plan), outboxQueueOptions{})
		}
		executor = BeaconJobExecutor{Plan: plan}
	}
	if blocked, err := b.resolveCodexThreadBeforeRun(ctx, session, turn); err != nil {
		return err
	} else if blocked {
		return nil
	}
	if _, err := b.store.MarkTurnRunning(ctx, turn.ID, session.CodexThreadID, ""); err != nil {
		return err
	}
	if executor == nil {
		executor = CodexExecutor{}
	}
	execCtx, cancelExec := context.WithCancel(ctx)
	unregisterCancel := b.registerRunningTurnCancel(sessionID, turn.ID, cancelExec)
	result, err := b.runExecutorWithHeartbeat(execCtx, executor, session, turn, chatID, input)
	cancelRequested, cancelReason, cancelSilent := b.runningTurnCancelState(turn.ID)
	unregisterCancel()
	cancelExec()
	if err != nil {
		if cancelRequested && isCanceledExecutionError(err) {
			if plan.Action == beacon.TurnRunBeacon || plan.Action == beacon.TurnWaitAllocation {
				if beaconErr := b.cancelBeaconTurn(ctx, session, turn, firstNonEmptyString(cancelReason, "canceled by user")); beaconErr != nil && b.out != nil {
					_, _ = fmt.Fprintf(b.out, "beacon turn cancel cleanup error: %v\n", beaconErr)
				}
			}
			if _, markErr := b.store.MarkTurnInterrupted(ctx, turn.ID, firstNonEmptyString(cancelReason, "canceled by user")); markErr != nil {
				return markErr
			}
			if cancelSilent {
				return nil
			}
			return b.queueAndSendOutboxChunks(ctx, session.ID, turn.ID, chatID, "canceled", "Codex request canceled.")
		}
		if isCanceledExecutionError(err) {
			notifyCtx := ctx
			if notifyCtx == nil || notifyCtx.Err() != nil {
				notifyCtx = context.Background()
			}
			if recovered, ok := b.completedTurnResultFromCodexHistory(notifyCtx, session, turn, result); ok {
				return b.completeQueuedTurnWithResult(notifyCtx, session, turn, chatID, plan, recovered)
			}
			reason := "helper context canceled before Codex result could be verified"
			if _, markErr := b.store.MarkTurnInterrupted(notifyCtx, turn.ID, reason); markErr != nil {
				return markErr
			}
			return b.queueAndSendOutboxChunksWithOptions(notifyCtx, session.ID, turn.ID, chatID, "interrupted", "Codex request interrupted because the Teams helper stopped, restarted, or lost its execution context before it could verify a final Codex result.\n\nCheck recent messages and changed files first. If no final answer appears, resend the message or use `helper retry last`.", outboxQueueOptions{
				MentionOwner:     true,
				NotificationKind: "needs_attention",
			})
		}
		var threadConflict codexThreadConflictError
		if errors.As(err, &threadConflict) {
			return b.interruptTurnForThreadRecovery(ctx, session, turn, codexThreadConflictKind, threadConflict.Error())
		}
		if IsAmbiguousExecutionError(err) {
			if blocked, bindErr := b.bindObservedCodexThreadOrInterrupt(ctx, session, turn, result.CodexThreadID, "runner_ambiguous"); bindErr != nil {
				return bindErr
			} else if blocked {
				return nil
			}
			if _, runningErr := b.store.MarkTurnRunning(ctx, turn.ID, session.CodexThreadID, result.CodexTurnID); runningErr != nil {
				return runningErr
			}
			if _, markErr := b.store.MarkTurnInterrupted(ctx, turn.ID, "ambiguous Codex execution: "+err.Error()); markErr != nil {
				return markErr
			}
			return b.queueAndSendOutboxChunksWithOptions(ctx, session.ID, turn.ID, chatID, "interrupted", "Codex accepted the request, but the helper could not confirm whether it finished. I did not retry automatically because that could duplicate work.\n\nCheck recent messages and changed files first. To run the same Teams request again in this same session, send `helper retry last` here. Advanced: `helper retry "+turn.ID+"`.", outboxQueueOptions{
				MentionOwner:     true,
				NotificationKind: "needs_attention",
			})
		}
		if blocked, bindErr := b.bindObservedCodexThreadOrInterrupt(ctx, session, turn, result.CodexThreadID, "runner_failed"); bindErr != nil {
			return bindErr
		} else if blocked {
			return nil
		}
		if _, markErr := b.store.MarkTurnFailedWithCodexIDs(ctx, turn.ID, err.Error(), firstNonEmptyString(result.CodexThreadID, session.CodexThreadID), result.CodexTurnID); markErr != nil {
			return markErr
		}
		if plan.Action == beacon.TurnRunBeacon || plan.Action == beacon.TurnWaitAllocation {
			if beaconErr := b.recordBeaconTurnFinish(ctx, session, turn, plan, err.Error()); beaconErr != nil {
				if b.out != nil {
					_, _ = fmt.Fprintf(b.out, "beacon turn failure cleanup error: %v\n", beaconErr)
				}
			}
		}
		if codexErrorRequiresUpgrade(err) {
			if upgradeErr := b.requestCodexUpgradeAfterFailure(ctx, session, turn, chatID, err); upgradeErr != nil {
				return upgradeErr
			}
			return nil
		}
		if infraBody, ok := infraLaunchFailureNotice(err); ok {
			// Setup/infrastructure failure: don't surface the raw internal error
			// as if Codex produced it. Audit loudly for the operator and show the
			// user a reassuring, Teams-actionable message.
			if b.out != nil {
				_, _ = fmt.Fprintf(b.out, "codex launch failure (infrastructure, not user request): %v\n", err)
			}
			return b.queueAndSendOutboxChunksWithOptions(ctx, session.ID, turn.ID, chatID, "error", infraBody, outboxQueueOptions{
				MentionOwner:     true,
				NotificationKind: "needs_attention",
			})
		}
		errorBody := "error: " + err.Error()
		if (plan.Action == beacon.TurnRunBeacon || plan.Action == beacon.TurnWaitAllocation) && strings.HasPrefix(strings.TrimSpace(err.Error()), "Beacon ") {
			errorBody = err.Error()
		}
		return b.queueAndSendOutboxChunksWithOptions(ctx, session.ID, turn.ID, chatID, "error", errorBody, outboxQueueOptions{
			MentionOwner:     true,
			NotificationKind: "needs_attention",
		})
	}
	if cancelRequested {
		if plan.Action == beacon.TurnRunBeacon || plan.Action == beacon.TurnWaitAllocation {
			if beaconErr := b.cancelBeaconTurn(ctx, session, turn, firstNonEmptyString(cancelReason, "canceled by user")); beaconErr != nil && b.out != nil {
				_, _ = fmt.Fprintf(b.out, "beacon turn cancel cleanup error: %v\n", beaconErr)
			}
		}
		if _, markErr := b.store.MarkTurnInterrupted(ctx, turn.ID, firstNonEmptyString(cancelReason, "canceled by user")); markErr != nil {
			return markErr
		}
		if cancelSilent {
			return nil
		}
		return b.queueAndSendOutboxChunks(ctx, session.ID, turn.ID, chatID, "canceled", "Codex request canceled.")
	}
	if interrupted, interruptErr := b.turnInterrupted(ctx, turn.ID); interruptErr != nil {
		return interruptErr
	} else if interrupted {
		return nil
	}
	return b.completeQueuedTurnWithResult(ctx, session, turn, chatID, plan, result)
}

func (b *Bridge) completeQueuedTurnWithResult(ctx context.Context, session *Session, turn teamstore.Turn, chatID string, plan beacon.TurnExecutionPlan, result ExecutionResult) error {
	if blocked, bindErr := b.bindObservedCodexThreadOrInterrupt(ctx, session, turn, result.CodexThreadID, "runner_completed"); bindErr != nil {
		return bindErr
	} else if blocked {
		return nil
	}
	if !result.canonicalTranscriptFinal {
		if transcriptResult, ok := b.completedTurnResultFromLinkedTranscript(ctx, session, turn, result); ok {
			result = executionResultWithTranscriptFinal(result, transcriptResult)
		}
	}
	preFinalQueued, err := b.queueActiveTurnTranscriptStatusBeforeFinal(ctx, session, turn)
	if err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams pre-final transcript status skipped: %v\n", err)
		}
		preFinalQueued = 0
	}
	mentionOwner := true
	visibleText := StripOAIMemoryCitationBlocks(StripHelperPromptEchoes(StripArtifactManifestBlocks(result.Text)))
	if visibleText == "" && len(ExtractArtifactManifestBlocks(result.Text)) > 0 {
		visibleText = "artifact manifest received; uploading listed files."
	}
	queued, err := b.queueOutboxChunksWithOptions(ctx, session.ID, turn.ID, chatID, "final", visibleText, outboxQueueOptions{
		MentionOwner:     mentionOwner,
		NotificationKind: "turn_completed",
	})
	if err != nil {
		return err
	}
	session.UpdatedAt = time.Now()
	if _, err := b.store.MarkTurnCompleted(ctx, turn.ID, firstNonEmptyString(result.CodexThreadID, session.CodexThreadID), result.CodexTurnID); err != nil {
		return err
	}
	if plan.Action == beacon.TurnRunBeacon || plan.Action == beacon.TurnWaitAllocation {
		if beaconErr := b.recordBeaconTurnFinish(ctx, session, turn, plan, ""); beaconErr != nil {
			if b.out != nil {
				_, _ = fmt.Fprintf(b.out, "beacon turn cleanup error: %v\n", beaconErr)
			}
		}
	}
	if preFinalQueued > 0 || len(queued) > 0 {
		if err := b.flushPendingOutboxForChat(ctx, chatID); err != nil {
			if isOutboxDeliveryDeferred(err) || isGraphRateLimitError(err) {
				if b.out != nil {
					_, _ = fmt.Fprintf(b.out, "Teams final outbox delivery deferred: %v\n", err)
				}
			} else {
				return err
			}
		}
		b.boostPolling(time.Now())
	}
	updatedTitle, err := b.refreshWorkChatTitleFromExecutionResult(ctx, session, result)
	if err != nil {
		return err
	}
	if !updatedTitle {
		queueState, err := b.sessionTurnQueueState(ctx, session.ID)
		if err != nil {
			return err
		}
		if queueState.Queued == 0 {
			if err := b.refreshWorkChatTitleFromCodexHistory(ctx, session); err != nil {
				return err
			}
		}
	}
	return b.uploadArtifactsFromResult(ctx, session, turn, result.Text)
}

func (b *Bridge) completedTurnResultFromLinkedTranscript(ctx context.Context, session *Session, turn teamstore.Turn, observed ExecutionResult) (ExecutionResult, bool) {
	if b == nil || session == nil || b.store == nil {
		return ExecutionResult{}, false
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return ExecutionResult{}, false
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	local, ok := linkedTranscriptLocalFromCheckpoint(*session, checkpoint)
	if !ok {
		return ExecutionResult{}, false
	}
	previous := historyTieredFileState{
		Path:      local.FilePath,
		Size:      checkpoint.SourceSize,
		ModTime:   checkpoint.SourceModTime,
		Offset:    checkpoint.LastOffset,
		Line:      checkpoint.LastSourceLine,
		SessionID: firstNonEmptyString(local.SessionID, session.CodexThreadID),
		ThreadID:  firstNonEmptyString(observed.CodexThreadID, turn.CodexThreadID, local.SessionID, session.CodexThreadID),
	}
	return b.completedTurnResultFromLocalCodexHistorySince(ctx, session, turn, observed, local, previous)
}

func executionResultWithTranscriptFinal(observed ExecutionResult, transcriptResult ExecutionResult) ExecutionResult {
	text := mergeObservedArtifactManifestsIntoTranscriptFinal(transcriptResult.Text, observed.Text)
	return ExecutionResult{
		Text:                     text,
		CodexThreadID:            firstNonEmptyString(transcriptResult.CodexThreadID, observed.CodexThreadID),
		CodexThreadTitle:         firstNonEmptyString(observed.CodexThreadTitle, transcriptResult.CodexThreadTitle),
		CodexTurnID:              firstNonEmptyString(transcriptResult.CodexTurnID, observed.CodexTurnID),
		canonicalTranscriptFinal: true,
	}
}

func mergeObservedArtifactManifestsIntoTranscriptFinal(transcriptText string, observedText string) string {
	transcriptBlocks := ExtractArtifactManifestBlocks(transcriptText)
	if hasUsableArtifactManifestBlock(transcriptBlocks) {
		return transcriptText
	}
	if len(transcriptBlocks) > 0 {
		transcriptText = StripArtifactManifestBlocks(transcriptText)
	}
	blocks := ExtractArtifactManifestBlocks(observedText)
	if len(blocks) == 0 {
		return transcriptText
	}
	text := strings.TrimSpace(transcriptText)
	for _, block := range blocks {
		if IsPlaceholderArtifactManifestBlock(block) {
			continue
		}
		if text != "" {
			text += "\n\n"
		}
		text += "```" + ArtifactManifestFenceInfo + "\n" + strings.TrimSpace(string(block)) + "\n```"
	}
	return text
}

func hasUsableArtifactManifestBlock(blocks [][]byte) bool {
	for _, block := range blocks {
		if IsPlaceholderArtifactManifestBlock(block) {
			continue
		}
		plan, err := ParseArtifactManifest(block, ArtifactManifestOptions{})
		if err == nil && len(plan.Files) > 0 {
			return true
		}
	}
	return false
}

func (b *Bridge) completedTurnResultFromCodexHistory(ctx context.Context, session *Session, turn teamstore.Turn, observed ExecutionResult) (ExecutionResult, bool) {
	if b == nil || session == nil {
		return ExecutionResult{}, false
	}
	threadID := firstNonEmptyString(observed.CodexThreadID, turn.CodexThreadID, session.CodexThreadID)
	if strings.TrimSpace(threadID) == "" {
		return ExecutionResult{}, false
	}
	projects, err := discoverCodexProjectsForTeams(ctx, b.scope.CodexHome)
	if err != nil {
		return ExecutionResult{}, false
	}
	local, _, ok := findCodexSession(projects, threadID)
	if !ok || strings.TrimSpace(local.FilePath) == "" {
		return ExecutionResult{}, false
	}
	return b.completedTurnResultFromLocalCodexHistory(ctx, session, turn, observed, local)
}

func (b *Bridge) completedTurnResultFromLocalCodexHistory(ctx context.Context, session *Session, turn teamstore.Turn, observed ExecutionResult, local codexhistory.Session) (ExecutionResult, bool) {
	return b.completedTurnResultFromLocalCodexHistorySince(ctx, session, turn, observed, local, historyTieredFileState{})
}

func (b *Bridge) completedTurnResultFromLocalCodexHistorySince(ctx context.Context, session *Session, turn teamstore.Turn, observed ExecutionResult, local codexhistory.Session, previous historyTieredFileState) (ExecutionResult, bool) {
	if b == nil || session == nil || strings.TrimSpace(local.FilePath) == "" {
		return ExecutionResult{}, false
	}
	threadID := firstNonEmptyString(observed.CodexThreadID, turn.CodexThreadID, local.SessionID, session.CodexThreadID)
	if strings.TrimSpace(threadID) == "" {
		return ExecutionResult{}, false
	}
	observedTurnID := strings.TrimSpace(firstNonEmptyString(observed.CodexTurnID, turn.CodexTurnID))
	scan, err := historyTieredScanTail(local.FilePath, previous, historyTieredMaxTailBytes)
	if err != nil {
		return ExecutionResult{}, false
	}
	if scan.TooLarge {
		if observedTurnID != "" {
			return ExecutionResult{}, false
		}
		scan, err = historyTieredScanTail(local.FilePath, previous, 0)
		if err != nil {
			return ExecutionResult{}, false
		}
	}
	var threshold time.Time
	if !turn.StartedAt.IsZero() {
		threshold = turn.StartedAt.Add(-2 * time.Second)
	} else if !turn.QueuedAt.IsZero() {
		threshold = turn.QueuedAt.Add(-2 * time.Second)
	}
	var selected historyTieredFinal
	for _, final := range scan.Finals {
		if strings.TrimSpace(final.Record.Text) == "" {
			continue
		}
		if strings.TrimSpace(final.Record.ThreadID) != "" && strings.TrimSpace(final.Record.ThreadID) != strings.TrimSpace(threadID) {
			continue
		}
		if observedTurnID != "" {
			if strings.TrimSpace(final.Record.TurnID) != observedTurnID {
				continue
			}
		}
		if !threshold.IsZero() {
			if final.Record.CreatedAt.IsZero() || final.Record.CreatedAt.Before(threshold) {
				continue
			}
		}
		selected = final
	}
	if strings.TrimSpace(selected.Record.Text) == "" {
		return ExecutionResult{}, false
	}
	return ExecutionResult{
		Text:                     strings.TrimSpace(selected.Record.Text),
		CodexThreadID:            firstNonEmptyString(selected.Record.ThreadID, observed.CodexThreadID, turn.CodexThreadID, session.CodexThreadID),
		CodexTurnID:              firstNonEmptyString(selected.Record.TurnID, observed.CodexTurnID, turn.CodexTurnID),
		canonicalTranscriptFinal: true,
	}, true
}

func (b *Bridge) turnInterrupted(ctx context.Context, turnID string) (bool, error) {
	if b == nil || b.store == nil || strings.TrimSpace(turnID) == "" {
		return false, nil
	}
	turn, ok, err := b.store.TurnByID(ctx, turnID)
	if err != nil {
		return false, err
	}
	return ok && turn.Status == teamstore.TurnStatusInterrupted, nil
}

func retryTurnID(turnID string) string {
	return strings.TrimSpace(turnID) + ":retry:" + fmt.Sprintf("%d", time.Now().UnixNano())
}

func (b *Bridge) rejectSessionAttachment(ctx context.Context, session *Session, msg ChatMessage) error {
	return b.rejectSessionAttachmentWithMessage(ctx, session, msg, UnsupportedAttachmentMessage(msg.Attachments))
}

func attachmentDownloadUserMessage(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	text := err.Error()
	if !strings.Contains(text, "Graph response exceeds") {
		return "", false
	}
	return "Teams attachment is too large for Codex helper to download safely (" + text + "). Please send a smaller file or split the content into smaller messages.", true
}

func (b *Bridge) interruptTurnForAttachmentMessage(ctx context.Context, session *Session, turn teamstore.Turn, message string) error {
	if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, message); err != nil {
		return err
	}
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:" + turn.ID + ":attachment-download",
		SessionID:        session.ID,
		TurnID:           turn.ID,
		TeamsChatID:      session.ChatID,
		Kind:             "interrupted",
		Body:             message,
		MentionOwner:     true,
		NotificationKind: "needs_attention",
	})
}

func (b *Bridge) rejectSessionAttachmentWithMessage(ctx context.Context, session *Session, msg ChatMessage, message string) error {
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	inbound, _, err := b.persistInboundWithStatus(ctx, session, msg, teamstore.InboundStatusIgnored)
	if err != nil {
		return err
	}
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + inbound.ID + ":attachment",
		SessionID:   session.ID,
		TeamsChatID: session.ChatID,
		Kind:        "attachment",
		Body:        message,
	})
}

type queuedTurnRunner func(context.Context, *Session, teamstore.Turn) error

type sessionTurnQueueState struct {
	Running bool
	Queued  int
}

func (b *Bridge) sessionTurnQueueState(ctx context.Context, sessionID string) (sessionTurnQueueState, error) {
	if strings.TrimSpace(sessionID) == "" {
		return sessionTurnQueueState{}, nil
	}
	state, err := b.store.SessionActiveTurnQueueSnapshot(ctx, sessionID)
	if err != nil {
		return sessionTurnQueueState{}, err
	}
	var out sessionTurnQueueState
	for _, turn := range state.Turns {
		if turn.SessionID != sessionID {
			continue
		}
		switch turn.Status {
		case teamstore.TurnStatusRunning:
			out.Running = true
		case teamstore.TurnStatusQueued:
			out.Queued++
		}
	}
	return out, nil
}

func (b *Bridge) startQueuedTurn(ctx context.Context, session *Session, preferredTurnID string, preferred queuedTurnRunner) (bool, error) {
	if session == nil {
		return false, nil
	}
	if importing, err := b.sessionTranscriptImportInProgress(ctx, session.ID); err != nil {
		return false, err
	} else if importing {
		return false, nil
	}
	claimed, ok, err := b.store.ClaimNextQueuedTurn(ctx, session.ID)
	if err != nil || !ok {
		return ok, err
	}
	if strings.TrimSpace(preferredTurnID) == "" || claimed.ID != preferredTurnID {
		if err := b.queueAndBestEffortQueuedTurnStartNotice(ctx, session, claimed); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams queued turn start notice error: %v\n", err)
		}
	}
	sessionSnapshot := *session
	runCtx := ctx
	b.asyncTurnWG.Add(1)
	go func() {
		defer b.asyncTurnWG.Done()
		runSession := &sessionSnapshot
		err := b.runClaimedQueuedTurn(runCtx, runSession, claimed, preferredTurnID, preferred)
		if err != nil {
			b.handleClaimedQueuedTurnError(context.Background(), runSession, claimed, err)
		}
		if err := b.processQueuedTurnsForSession(context.Background(), runSession); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams queued turn session follow-up error: %v\n", err)
		}
		if err := b.sendDeferredInterruptedTurnNotices(context.Background()); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams interrupted turn notice error: %v\n", err)
		}
		b.boostPolling(time.Now())
	}()
	return true, nil
}

func queuedTurnStartOutboxID(turnID string) string {
	return "outbox:" + strings.TrimSpace(turnID) + ":queued-start"
}

func (b *Bridge) queueAndBestEffortQueuedTurnStartNotice(ctx context.Context, session *Session, claimed teamstore.Turn) error {
	if b == nil || b.store == nil || session == nil || strings.TrimSpace(session.ChatID) == "" {
		return nil
	}
	body, ok, err := b.formatQueuedTurnStartNotice(ctx, session.ID, claimed)
	if err != nil || !ok {
		return err
	}
	return b.queueAndBestEffortSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          queuedTurnStartOutboxID(claimed.ID),
		SessionID:   session.ID,
		TurnID:      claimed.ID,
		TeamsChatID: session.ChatID,
		Kind:        "queued-status",
		Body:        body,
	})
}

func (b *Bridge) formatQueuedTurnStartNotice(ctx context.Context, sessionID string, claimed teamstore.Turn) (string, bool, error) {
	state, err := b.store.SessionActiveTurnQueueSnapshot(ctx, sessionID)
	if err != nil {
		return "", false, err
	}
	current := turnPromptSummary(state, claimed)
	if current == "" {
		current = "Queued request `" + shortenTeamsLine(claimed.ID, 80) + "`"
	}
	remaining := queuedTurnsForSessionState(state, sessionID)
	lines := []string{
		"▶️ **Codex is starting this queued request.**",
		"",
		"**▶️ Now running:**",
		current,
		"",
		"---",
		"",
		"**⏳ Still queued:**",
	}
	if len(remaining) == 0 {
		lines = append(lines, "No other queued requests.")
	} else {
		lines = append(lines, formatTurnPromptList(state, remaining)...)
	}
	return strings.Join(lines, "\n"), true, nil
}

func (b *Bridge) runClaimedQueuedTurn(ctx context.Context, session *Session, claimed teamstore.Turn, preferredTurnID string, preferred queuedTurnRunner) error {
	if strings.TrimSpace(preferredTurnID) != "" && claimed.ID == preferredTurnID && preferred != nil {
		return preferred(ctx, session, claimed)
	}
	inbound, ok, err := b.store.InboundEventByID(ctx, claimed.InboundEventID)
	if err != nil {
		return err
	}
	state := teamstore.State{SchemaVersion: teamstore.SchemaVersion, InboundEvents: map[string]teamstore.InboundEvent{}}
	if ok {
		state.InboundEvents[inbound.ID] = inbound
	}
	return b.recoverQueuedTurn(ctx, session, claimed, state)
}

func (b *Bridge) handleClaimedQueuedTurnError(ctx context.Context, session *Session, turn teamstore.Turn, err error) {
	if err == nil || b == nil || b.store == nil {
		return
	}
	state, loadErr := b.store.Load(ctx)
	if loadErr == nil {
		if current, ok := state.Turns[turn.ID]; ok {
			switch current.Status {
			case teamstore.TurnStatusCompleted, teamstore.TurnStatusFailed, teamstore.TurnStatusInterrupted:
				return
			}
		}
	}
	if _, markErr := b.store.MarkTurnInterrupted(ctx, turn.ID, "queued turn failed before Codex completed: "+err.Error()); markErr != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams queued turn interrupt error: %v\n", markErr)
		}
		return
	}
	chatID := ""
	if session != nil {
		chatID = session.ChatID
	}
	if strings.TrimSpace(chatID) == "" {
		return
	}
	if queueErr := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:               "outbox:" + turn.ID + ":queued-turn-error",
		SessionID:        turn.SessionID,
		TurnID:           turn.ID,
		TeamsChatID:      chatID,
		Kind:             "error",
		Body:             "Codex could not continue this queued request: " + err.Error() + "\n\nPlease resend the message if you still want to run it.",
		MentionOwner:     true,
		NotificationKind: "needs_attention",
	}); queueErr != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams queued turn error notification failed: %v\n", queueErr)
	}
}

func beaconTurnStartFailureProviderReason(plan beacon.TurnExecutionPlan, message string) string {
	if reason := strings.TrimSpace(plan.ProviderReason); reason != "" {
		return reason
	}
	for _, line := range strings.Split(strings.TrimSpace(message), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return strings.TrimSpace(message)
}

func (b *Bridge) runExecutorWithHeartbeat(ctx context.Context, executor Executor, session *Session, turn teamstore.Turn, chatID string, input ExecutionInput) (ExecutionResult, error) {
	if err := b.recordOwnerHeartbeat(ctx, session.ID, turn.ID); err != nil {
		return ExecutionResult{}, err
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := b.startActiveOwnerHeartbeat(heartbeatCtx, session.ID, turn.ID)
	var result ExecutionResult
	var runErr error
	if streaming, ok := executor.(StreamingInputExecutor); ok {
		forwarder := b.startCodexEventForwarder(ctx, session, turn, chatID)
		result, runErr = streaming.RunInputWithEventHandler(ctx, session, input, forwarder.Handle)
		if runErr == nil {
			if transcriptResult, ok := b.completedTurnResultFromLinkedTranscript(ctx, session, turn, result); ok {
				result = executionResultWithTranscriptFinal(result, transcriptResult)
			}
		}
		if closeErr := forwarder.Close(result.Text); runErr == nil && closeErr != nil {
			runErr = closeErr
		}
	} else if streaming, ok := executor.(StreamingExecutor); ok {
		forwarder := b.startCodexEventForwarder(ctx, session, turn, chatID)
		result, runErr = streaming.RunWithEventHandler(ctx, session, input.Prompt, forwarder.Handle)
		if runErr == nil {
			if transcriptResult, ok := b.completedTurnResultFromLinkedTranscript(ctx, session, turn, result); ok {
				result = executionResultWithTranscriptFinal(result, transcriptResult)
			}
		}
		if closeErr := forwarder.Close(result.Text); runErr == nil && closeErr != nil {
			runErr = closeErr
		}
	} else if inputExecutor, ok := executor.(InputExecutor); ok {
		result, runErr = inputExecutor.RunInput(ctx, session, input)
	} else {
		result, runErr = executor.Run(ctx, session, input.Prompt)
	}
	cancelHeartbeat()
	heartbeatErr := <-heartbeatDone
	clearErr := b.recordOwnerHeartbeat(context.Background(), "", "")
	switch {
	case runErr != nil:
		return result, runErr
	case heartbeatErr != nil:
		return result, heartbeatErr
	case clearErr != nil:
		return result, clearErr
	default:
		return result, nil
	}
}

const maxTeamsCommandOutputRunes = 6000

type codexEventForwarder struct {
	ctx                     context.Context
	bridge                  *Bridge
	sessionID               string
	expectedThreadID        string
	turnID                  string
	chatID                  string
	events                  chan codexrunner.StreamEvent
	done                    chan struct{}
	pendingAgent            string
	lastStreamRetryStatusAt time.Time
	seq                     int
	err                     error
}

func (b *Bridge) startCodexEventForwarder(ctx context.Context, session *Session, turn teamstore.Turn, chatID string) *codexEventForwarder {
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	expectedThreadID := ""
	if session != nil {
		expectedThreadID = strings.TrimSpace(session.CodexThreadID)
	}
	f := &codexEventForwarder{
		ctx:              ctx,
		bridge:           b,
		sessionID:        sessionID,
		expectedThreadID: expectedThreadID,
		turnID:           turn.ID,
		chatID:           chatID,
		events:           make(chan codexrunner.StreamEvent, 128),
		done:             make(chan struct{}),
	}
	go f.run()
	return f
}

func (f *codexEventForwarder) Handle(event codexrunner.StreamEvent) {
	if f == nil {
		return
	}
	select {
	case f.events <- event:
	case <-f.ctx.Done():
	}
}

func (f *codexEventForwarder) Close(finalText string) error {
	if f == nil {
		return nil
	}
	close(f.events)
	<-f.done
	if strings.TrimSpace(f.pendingAgent) != "" && !sameCodexVisibleText(f.pendingAgent, finalText) {
		_ = f.send("progress", f.pendingAgent)
	}
	return f.err
}

func (f *codexEventForwarder) run() {
	defer close(f.done)
	timer := newCodexIdleStatusTimer(codexIdleStatusInitialDelay)
	defer stopCodexIdleStatusTimer(timer)
	quietSince := time.Now()
	var idleC <-chan time.Time
	if timer != nil {
		idleC = timer.C
	}
	for {
		select {
		case event, ok := <-f.events:
			if !ok {
				return
			}
			f.handle(event)
			quietSince = time.Now()
			resetCodexIdleStatusTimer(timer, codexIdleStatusInitialDelay)
		case <-idleC:
			f.sendIdleStatus(time.Since(quietSince))
			resetCodexIdleStatusTimer(timer, codexIdleStatusRepeatDelay)
		case <-f.ctx.Done():
			return
		}
	}
}

func (f *codexEventForwarder) handle(event codexrunner.StreamEvent) {
	if f.observeEventThread(event) {
		return
	}
	switch event.Kind {
	case codexrunner.StreamEventAgentMessage:
		if strings.TrimSpace(f.pendingAgent) != "" {
			_ = f.send("progress", f.pendingAgent)
		}
		f.pendingAgent = event.Text
	case codexrunner.StreamEventCommandStarted:
		f.flushPendingAgent()
	case codexrunner.StreamEventCommandCompleted:
		f.flushPendingAgent()
	case codexrunner.StreamEventStreamRetry:
		f.flushPendingAgent()
		f.sendStreamRetryStatus(event)
	case codexrunner.StreamEventContextCompacted:
		f.flushPendingAgent()
		_ = f.send("compact", transcriptContextCompactMessage)
	case codexrunner.StreamEventTurnFailed:
		f.flushPendingAgent()
		if event.Failure != nil && strings.TrimSpace(event.Failure.Message) != "" {
			_ = f.send("status", "Codex turn failed: "+event.Failure.Message)
		}
	}
}

func (f *codexEventForwarder) observeEventThread(event codexrunner.StreamEvent) bool {
	threadID := strings.TrimSpace(event.ThreadID)
	if threadID == "" {
		return false
	}
	if strings.TrimSpace(f.expectedThreadID) == "" {
		f.expectedThreadID = threadID
		return false
	}
	if f.expectedThreadID == threadID {
		return false
	}
	if f.err == nil {
		f.err = codexThreadConflictError{
			SessionID: f.sessionID,
			Existing:  f.expectedThreadID,
			Observed:  threadID,
			Source:    "stream_" + string(event.Kind),
		}
	}
	f.pendingAgent = ""
	return true
}

func (f *codexEventForwarder) flushPendingAgent() {
	if strings.TrimSpace(f.pendingAgent) == "" {
		return
	}
	_ = f.send("progress", f.pendingAgent)
	f.pendingAgent = ""
}

func (f *codexEventForwarder) sendIdleStatus(quietFor time.Duration) {
	if strings.TrimSpace(f.pendingAgent) != "" {
		f.flushPendingAgent()
		return
	}
	message := codexIdleStatusMessage
	if codexSuspectedStuckAfter > 0 && quietFor >= codexSuspectedStuckAfter && strings.TrimSpace(codexSuspectedStuckMessage) != "" {
		message = formatCodexSuspectedStuckMessage(quietFor)
	} else if quietFor >= codexIdleStatusCancelHintAfter && strings.TrimSpace(codexIdleStatusCancelHint) != "" {
		message += "\n\n" + codexIdleStatusCancelHint
	}
	_ = f.send("status", message)
}

func formatCodexSuspectedStuckMessage(quietFor time.Duration) string {
	message := strings.TrimSpace(codexSuspectedStuckMessage)
	if strings.Contains(message, "%s") {
		return fmt.Sprintf(message, formatCodexQuietDuration(quietFor))
	}
	return message
}

func formatCodexQuietDuration(quietFor time.Duration) string {
	rounded := quietFor.Round(time.Minute)
	if rounded < time.Minute {
		return "less than 1m"
	}
	return strings.TrimSuffix(rounded.String(), "0s")
}

func (f *codexEventForwarder) sendStreamRetryStatus(event codexrunner.StreamEvent) {
	if f == nil {
		return
	}
	now := time.Now()
	if !f.lastStreamRetryStatusAt.IsZero() && now.Sub(f.lastStreamRetryStatusAt) < codexStreamRetryStatusRepeatDelay {
		return
	}
	f.lastStreamRetryStatusAt = now
	_ = f.send("status", formatCodexStreamRetryStatus(event))
}

func formatCodexStreamRetryStatus(event codexrunner.StreamEvent) string {
	message := ""
	code := ""
	if event.Failure != nil {
		message = strings.TrimSpace(event.Failure.Message)
		code = strings.TrimSpace(event.Failure.Code)
	}
	if message == "" {
		message = "Reconnecting..."
	}
	lines := []string{"Connection dropped. Codex is reconnecting."}
	if !strings.EqualFold(strings.TrimSpace(message), strings.TrimSpace(lines[0])) {
		lines = append(lines, "", message)
	}
	if code != "" {
		lines = append(lines, "Reason: "+code)
	}
	return strings.Join(lines, "\n")
}

func (f *codexEventForwarder) send(kind string, text string) error {
	text = strings.TrimSpace(text)
	if text == "" || f.bridge == nil || strings.TrimSpace(f.chatID) == "" {
		return nil
	}
	if !f.bridge.canQueueLiveTurnOutbox(f.ctx, f.sessionID, f.turnID) {
		return nil
	}
	f.seq++
	msgKind := fmt.Sprintf("codex-%s-%03d", kind, f.seq)
	err := f.bridge.queueAndSendOutboxChunks(f.ctx, f.sessionID, f.turnID, f.chatID, msgKind, text)
	if err != nil {
		if isOutboxDeliveryDeferred(err) {
			if f.bridge != nil && f.bridge.out != nil {
				_, _ = fmt.Fprintf(f.bridge.out, "Teams Codex stream outbox delivery deferred: %v\n", err)
			}
			return nil
		}
		if f.err == nil {
			f.err = err
		}
	}
	return err
}

func (b *Bridge) canQueueLiveTurnOutbox(ctx context.Context, sessionID string, turnID string) bool {
	if b == nil || b.store == nil || strings.TrimSpace(turnID) == "" {
		return true
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return true
	}
	turn, ok := state.Turns[turnID]
	if !ok {
		return false
	}
	if strings.TrimSpace(sessionID) != "" && strings.TrimSpace(turn.SessionID) != strings.TrimSpace(sessionID) {
		return false
	}
	return turn.Status == teamstore.TurnStatusRunning
}

func newCodexIdleStatusTimer(delay time.Duration) *time.Timer {
	if delay <= 0 {
		return nil
	}
	return time.NewTimer(delay)
}

func resetCodexIdleStatusTimer(timer *time.Timer, delay time.Duration) {
	if timer == nil || delay <= 0 {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func stopCodexIdleStatusTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func sameCodexVisibleText(left string, right string) bool {
	return strings.TrimSpace(StripOAIMemoryCitationBlocks(StripHelperPromptEchoes(StripArtifactManifestBlocks(left)))) == strings.TrimSpace(StripOAIMemoryCitationBlocks(StripHelperPromptEchoes(StripArtifactManifestBlocks(right))))
}

func formatCodexCommandStarted(event codexrunner.StreamEvent) string {
	command := strings.TrimSpace(event.Command)
	if command == "" {
		command = "(command unavailable)"
	}
	return "Running command:\n" + command
}

func formatCodexCommandCompleted(event codexrunner.StreamEvent) string {
	command := strings.TrimSpace(event.Command)
	if command == "" {
		command = "(command unavailable)"
	}
	status := strings.TrimSpace(event.Status)
	if status == "" {
		status = "completed"
	}
	exit := "unknown"
	if event.ExitCode != nil {
		exit = strconvItoa(*event.ExitCode)
	}
	output := trimTeamsCommandOutput(event.AggregatedOutput, maxTeamsCommandOutputRunes)
	if strings.TrimSpace(output) == "" {
		output = "(no output)"
	}
	return "Command:\n" + command + "\n\nStatus: " + status + "\nExit code: " + exit + "\n\nOutput:\n" + output
}

func trimTeamsCommandOutput(text string, limit int) string {
	text = normalizeTeamsRenderText(text)
	if limit <= 0 || len([]rune(text)) <= limit {
		return text
	}
	runes := []rune(text)
	head := limit * 2 / 3
	tail := limit - head
	if head < 1 || tail < 1 {
		return string(runes[:limit])
	}
	return string(runes[:head]) + "\n\n[output truncated]\n\n" + string(runes[len(runes)-tail:])
}

func (b *Bridge) startActiveOwnerHeartbeat(ctx context.Context, sessionID string, turnID string) <-chan error {
	done := make(chan error, 1)
	interval := b.activeOwnerHeartbeatInterval()
	go func() {
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-timer.C:
				if err := b.recordOwnerHeartbeat(ctx, sessionID, turnID); err != nil {
					done <- err
					return
				}
				timer.Reset(interval)
			}
		}
	}()
	return done
}

func (b *Bridge) startOwnerHeartbeat(ctx context.Context) <-chan error {
	done := make(chan error, 1)
	interval := b.activeOwnerHeartbeatInterval()
	go func() {
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-timer.C:
				if err := b.recordCurrentOwnerHeartbeat(ctx); err != nil {
					done <- err
					return
				}
				timer.Reset(interval)
			}
		}
	}()
	return done
}

func (b *Bridge) activeOwnerHeartbeatInterval() time.Duration {
	b.ownerMu.Lock()
	defer b.ownerMu.Unlock()
	if b.ownerHeartbeatInterval > 0 {
		return b.ownerHeartbeatInterval
	}
	staleAfter := b.ownerStaleAfter
	if staleAfter <= 0 {
		staleAfter = 2 * time.Minute
	}
	interval := staleAfter / 3
	if interval <= 0 || interval > 30*time.Second {
		return 30 * time.Second
	}
	if interval < time.Second {
		return time.Second
	}
	return interval
}

func (b *Bridge) currentLease() teamstore.ControlLease {
	b.ownerMu.Lock()
	defer b.ownerMu.Unlock()
	return b.lease
}

func (b *Bridge) currentLeaseGeneration() int64 {
	return b.currentLease().Generation
}

func (b *Bridge) setControlLease(lease teamstore.ControlLease) {
	b.ownerMu.Lock()
	defer b.ownerMu.Unlock()
	b.lease = lease
}

func (b *Bridge) migrateTeamsStoreToSQLiteOrFallback(ctx context.Context) error {
	if _, err := b.store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("migrate Teams store to sqlite: %w", err)
		}
		if _, legacy, loadErr := b.store.LoadLegacyJSONState(ctx); loadErr != nil {
			return fmt.Errorf("migrate Teams store to sqlite: %w; legacy fallback unavailable: %v", err, loadErr)
		} else if !legacy {
			return fmt.Errorf("migrate Teams store to sqlite: %w; legacy fallback unavailable: current state is not legacy JSON", err)
		}
		b.sendTeamsStoreSQLiteMigrationFallbackNotice(ctx, err)
	}
	return nil
}

func (b *Bridge) shouldDeferTeamsStoreSQLiteMigration(ctx context.Context) (bool, error) {
	if b == nil || b.store == nil {
		return false, nil
	}
	path, err := b.pendingHelperRestartNoticePath()
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	if state.ServiceControl.Draining {
		switch strings.TrimSpace(state.ServiceControl.Reason) {
		case teamstore.HelperUpgradeReason, teamstore.HelperReloadReason:
			return true, nil
		}
	}
	if state.Upgrade != nil && strings.TrimSpace(state.Upgrade.ID) != "" {
		switch state.Upgrade.Phase {
		case teamstore.UpgradePhaseCompleted, teamstore.UpgradePhaseAborted:
		default:
			if strings.TrimSpace(state.Upgrade.Reason) == teamstore.HelperUpgradeReason {
				return true, nil
			}
		}
	}
	return false, nil
}

func (b *Bridge) sendTeamsStoreSQLiteMigrationFallbackNotice(ctx context.Context, migrationErr error) {
	if b == nil || b.store == nil || b.graph == nil || strings.TrimSpace(b.reg.ControlChatID) == "" {
		return
	}
	chatID := strings.TrimSpace(b.reg.ControlChatID)
	msg := teamstore.OutboxMessage{
		ID:          teamsStoreSQLiteMigrationFallbackOutboxID(b.store.Path(), chatID),
		TeamsChatID: chatID,
		Kind:        "teams-store-sqlite-migration-fallback",
		Body:        teamsStoreSQLiteMigrationFallbackNotice(migrationErr),
	}
	if err := b.queueAndBestEffortSendOutbox(ctx, msg); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams store SQLite migration fallback notice failed: %v\n", err)
	}
}

func teamsStoreSQLiteMigrationFallbackOutboxID(storePath string, chatID string) string {
	seed := strings.TrimSpace(chatID) + "\x00" + strings.TrimSpace(storePath)
	return "outbox:control:teams-store-sqlite-migration-fallback:" + shortStableID(seed)
}

func teamsStoreSQLiteMigrationFallbackNotice(migrationErr error) string {
	detail := "unknown error"
	if migrationErr != nil && strings.TrimSpace(migrationErr.Error()) != "" {
		detail = strings.TrimSpace(migrationErr.Error())
	}
	return "Teams state migration to SQLite failed. The helper will keep using the legacy JSON state store for this run, so functionality should continue, but the Teams state performance optimization is disabled until a later retry succeeds.\n\nError: " + detail
}

func (b *Bridge) runStandbyLoop(ctx context.Context, opts BridgeOptions) error {
	if b.out != nil {
		holder := b.currentLease().HolderMachineID
		if holder == "" {
			holder = "unknown"
		}
		_, _ = fmt.Fprintf(b.out, "Teams bridge standby; control lease is held by %s. This process will keep running and retry.\n", holder)
	}
	for {
		if teamsStartupFallbackStopRequested() {
			if b.out != nil {
				_, _ = fmt.Fprintln(b.out, "Teams Startup fallback retire signal detected while standby; exiting.")
			}
			return nil
		}
		if active, err := b.claimControlLease(ctx); err != nil {
			return err
		} else if active {
			if b.out != nil {
				_, _ = fmt.Fprintln(b.out, "Teams bridge acquired control lease; becoming active.")
			}
			return b.Listen(ctx, opts)
		}
		if teamsStartupFallbackExitOnStandby() {
			if b.out != nil {
				_, _ = fmt.Fprintln(b.out, "Teams Startup fallback is not the active owner; exiting for low-cost retry.")
			}
			return nil
		}
		if opts.Once {
			return nil
		}
		interval := opts.Interval
		if interval <= 0 {
			interval = 5 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func teamsStartupFallbackStopRequested() bool {
	path := strings.TrimSpace(os.Getenv(envTeamsStartupFallbackStopFile))
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

func teamsStartupFallbackExitOnStandby() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envTeamsStartupFallbackExitOnStandby))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (b *Bridge) claimControlLease(ctx context.Context) (bool, error) {
	if err := b.ensureStore(); err != nil {
		return false, err
	}
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	if b.machine.ID == "" {
		b.machine = MachineRecordForUser(b.user, b.scope)
		b.applyRegistryMachineHostnameOverride()
	}
	duration := b.leaseDuration
	if duration <= 0 {
		duration = 30 * time.Second
	}
	owner, err := teamstore.CurrentOwner("", "", "", time.Now())
	if err != nil {
		return false, err
	}
	decision, err := b.store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
		Scope:    b.scope,
		Machine:  b.machine,
		Owner:    owner,
		Duration: duration,
	})
	if err != nil {
		return false, err
	}
	b.setControlLease(decision.Lease)
	return decision.Mode == teamstore.LeaseModeActive, nil
}

func (b *Bridge) refreshControlLease(ctx context.Context) (bool, error) {
	return b.claimControlLease(ctx)
}

func (b *Bridge) ensureActiveControlLease(ctx context.Context) error {
	lease := b.currentLease()
	if b.store == nil || b.machine.ID == "" || lease.Generation <= 0 {
		return teamstore.ErrControlLeaseNotHeld
	}
	lease, err := b.store.ValidateControlLease(ctx, b.machine.ID, lease.Generation, time.Now())
	if err != nil {
		return err
	}
	b.setControlLease(lease)
	return nil
}

func (b *Bridge) setOwner(owner teamstore.OwnerMetadata, staleAfter time.Duration) {
	b.ownerMu.Lock()
	defer b.ownerMu.Unlock()
	b.owner = owner
	b.ownerStaleAfter = staleAfter
	if b.ownerHeartbeatInterval <= 0 {
		interval := staleAfter / 3
		if interval <= 0 || interval > 30*time.Second {
			interval = 30 * time.Second
		}
		if interval < time.Second {
			interval = time.Second
		}
		b.ownerHeartbeatInterval = interval
	}
}

func (b *Bridge) recordOwnerHeartbeat(ctx context.Context, activeSessionID string, activeTurnID string) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	if b.currentLeaseGeneration() > 0 {
		active, err := b.refreshControlLease(ctx)
		if err != nil {
			return err
		}
		if !active {
			return teamstore.ErrControlLeaseNotHeld
		}
	}
	b.ownerMu.Lock()
	defer b.ownerMu.Unlock()
	if b.owner.PID <= 0 {
		return nil
	}
	owner := b.owner
	owner.ActiveSessionID = activeSessionID
	owner.ActiveTurnID = activeTurnID
	owner.ScopeID = b.scope.ID
	owner.MachineID = b.machine.ID
	owner.LeaseGeneration = b.lease.Generation
	updated, err := b.store.RecordOwnerHeartbeat(ctx, owner, b.ownerStaleAfter, time.Now())
	if err != nil {
		return err
	}
	b.owner = updated
	return nil
}

func (b *Bridge) recordCurrentOwnerHeartbeat(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	if b.currentLeaseGeneration() > 0 {
		active, err := b.refreshControlLease(ctx)
		if err != nil {
			return err
		}
		if !active {
			return teamstore.ErrControlLeaseNotHeld
		}
	}
	b.ownerMu.Lock()
	defer b.ownerMu.Unlock()
	if b.owner.PID <= 0 {
		return nil
	}
	owner := b.owner
	owner.ScopeID = b.scope.ID
	owner.MachineID = b.machine.ID
	owner.LeaseGeneration = b.lease.Generation
	updated, err := b.store.RecordOwnerHeartbeat(ctx, owner, b.ownerStaleAfter, time.Now())
	if err != nil {
		return err
	}
	b.owner = updated
	return nil
}

func (b *Bridge) clearOwnerIfSame(ctx context.Context) {
	b.ownerMu.Lock()
	owner := b.owner
	b.ownerMu.Unlock()
	if owner.PID <= 0 || b.store == nil {
		return
	}
	_, _ = b.store.ClearOwnerIfSame(ctx, owner)
}

func (b *Bridge) sendControl(ctx context.Context, text string) error {
	return b.sendToChat(ctx, b.reg.ControlChatID, text)
}

func (b *Bridge) sendDeferredUpgradeNotice(ctx context.Context, chatID string, inbound teamstore.InboundEvent) error {
	return b.sendDeferredServiceControlNotice(ctx, chatID, inbound, teamstore.ServiceControl{Draining: true, Reason: teamstore.HelperUpgradeReason})
}

func (b *Bridge) sendDeferredServiceControlNotice(ctx context.Context, chatID string, inbound teamstore.InboundEvent, control teamstore.ServiceControl) error {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil
	}
	ackKind := "upgrade_deferred"
	suffix := "deferred-upgrade-notice"
	body := "upgrade in progress. I saved this message and will resume it after the upgrade finishes."
	if control.Reason == teamstore.HelperReloadReason {
		ackKind = "reload_deferred"
		suffix = "deferred-reload-notice"
		body = "helper reload in progress. I saved this message and will resume it after the helper is back online."
	}
	outbox := teamstore.OutboxMessage{
		ID:                 "outbox:" + inbound.ID + ":" + suffix,
		SessionID:          inbound.SessionID,
		TeamsChatID:        chatID,
		Kind:               "ack",
		AckKind:            ackKind,
		Body:               body,
		UpgradeNonBlocking: true,
	}
	if author, ok := inboundExternalAuthor(inbound, b.user); ok {
		applyOutboxMentionUser(&outbox, author)
	}
	queued, err := b.queueOutbox(ctx, outbox)
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	if err := b.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true}); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams deferred ACK send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) sendExternalDeferredReceipt(ctx context.Context, chatID string, inbound teamstore.InboundEvent, body string) error {
	author, ok := inboundExternalAuthor(inbound, b.user)
	if !ok {
		return nil
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil
	}
	body = strings.TrimSpace(body)
	if body == "" {
		body = "⏳ Codex received your question."
	}
	outbox := teamstore.OutboxMessage{
		ID:          "outbox:" + inbound.ID + ":external-receipt",
		SessionID:   inbound.SessionID,
		TeamsChatID: chatID,
		Kind:        "ack",
		AckKind:     "external_prompt",
		Body:        body,
	}
	applyOutboxMentionUser(&outbox, author)
	queued, err := b.queueOutbox(ctx, outbox)
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	if err := b.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true}); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams external receipt send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) serviceControlBlocksNewWork(ctx context.Context) (teamstore.ServiceControl, bool, error) {
	if err := b.ensureStore(); err != nil {
		return teamstore.ServiceControl{}, false, err
	}
	control, err := b.store.ReadControl(ctx)
	if err != nil {
		return teamstore.ServiceControl{}, false, err
	}
	return control, control.Paused || control.Draining, nil
}

func serviceControlDefersInput(control teamstore.ServiceControl) bool {
	if !control.Draining {
		return false
	}
	switch control.Reason {
	case teamstore.HelperUpgradeReason, teamstore.CodexUpgradeReason, teamstore.HelperReloadReason:
		return true
	default:
		return false
	}
}

func (b *Bridge) rejectSessionWork(ctx context.Context, session *Session, msg ChatMessage, control teamstore.ServiceControl) error {
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	status := teamstore.InboundStatusIgnored
	source := "teams"
	if serviceControlDefersInput(control) {
		status = teamstore.InboundStatusDeferred
		text := strings.TrimSpace(promptTextFromTeamsMessageHTML(msg.Body.Content))
		if len(msg.Attachments) > 0 || len(HostedContentIDsFromHTML(msg.Body.Content)) > 0 {
			source = "teams_session_attachment_deferred"
		} else if ParseDashboardCommand(ChatScopeWork, commandRouteTextFromTeamsMessage(msg, text)).HelperCommand {
			source = "teams_session_command_deferred"
		}
	}
	inbound, _, err := b.persistInboundWithStatusAndSource(ctx, session, msg, status, source)
	if err != nil {
		return err
	}
	if status == teamstore.InboundStatusDeferred {
		return b.sendDeferredServiceControlNotice(ctx, session.ChatID, inbound, control)
	}
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + inbound.ID + ":control",
		SessionID:   session.ID,
		TeamsChatID: session.ChatID,
		Kind:        "control",
		Body:        serviceControlBlockedMessage(control, "new turns"),
	})
}

func (b *Bridge) drainComplete(ctx context.Context) (bool, error) {
	if err := b.ensureStore(); err != nil {
		return false, err
	}
	control, err := b.store.ReadControl(ctx)
	if err != nil {
		return false, err
	}
	if !control.Draining {
		return false, nil
	}
	state, err := b.store.UpgradeBlockingStateSnapshot(ctx)
	if err != nil {
		return false, err
	}
	if !state.ServiceControl.Draining {
		return false, nil
	}
	return !teamstore.HasUpgradeBlockingWork(state, time.Now()), nil
}

func (b *Bridge) ensureStore() error {
	if b.store != nil {
		return nil
	}
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	storePath, err := DefaultStorePathForScope(b.scope.ID)
	if err != nil {
		return err
	}
	store, err := teamstore.Open(storePath)
	if err != nil {
		return err
	}
	b.store = store
	return nil
}

func (b *Bridge) restoreRegistryFromStore(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	changed := false
	if b.reg.UserID == "" && b.user.ID != "" {
		b.reg.UserID = b.user.ID
		changed = true
	}
	if b.reg.UserPrincipal == "" && b.user.UserPrincipalName != "" {
		b.reg.UserPrincipal = b.user.UserPrincipalName
		changed = true
	}
	if state.ControlChat.TeamsChatID != "" {
		setRegistryControlString := func(target *string, value string) {
			if *target != value {
				*target = value
				changed = true
			}
		}
		setRegistryControlStringIfPresent := func(target *string, value string) {
			if strings.TrimSpace(value) == "" && strings.TrimSpace(*target) != "" {
				return
			}
			setRegistryControlString(target, value)
		}
		setRegistryControlString(&b.reg.ControlChatID, state.ControlChat.TeamsChatID)
		setRegistryControlStringIfPresent(&b.reg.ControlChatURL, state.ControlChat.TeamsChatURL)
		setRegistryControlStringIfPresent(&b.reg.ControlChatTopic, state.ControlChat.TeamsChatTopic)
		setRegistryControlStringIfPresent(&b.reg.ControlChatUserTitle, state.ControlChat.UserTitle)
		setRegistryControlStringIfPresent(&b.reg.ControlChatTitleSource, state.ControlChat.TitleSource)
	}
	if reconcileRegistrySessionsFromStore(&b.reg, state) {
		changed = true
	}
	for _, inbound := range state.InboundEvents {
		if inbound.TeamsChatID != "" && inbound.TeamsMessageID != "" && !b.reg.HasSeen(inbound.TeamsChatID, inbound.TeamsMessageID) {
			b.reg.MarkSeen(inbound.TeamsChatID, inbound.TeamsMessageID)
			changed = true
		}
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.TeamsChatID != "" && outbox.TeamsMessageID != "" && outbox.Status == teamstore.OutboxStatusSent && !b.reg.HasSent(outbox.TeamsChatID, outbox.TeamsMessageID) {
			b.markRegistrySent(outbox.TeamsChatID, outbox.TeamsMessageID)
			changed = true
		}
	}
	if changed {
		return b.Save()
	}
	return nil
}

func reconcileRegistrySessionsFromStore(reg *Registry, state teamstore.State) bool {
	if reg == nil {
		return false
	}
	var desired []Session
	for _, durable := range state.Sessions {
		if durable.ID == "" || durable.TeamsChatID == "" || isDurableControlFallbackSession(durable) {
			continue
		}
		desired = append(desired, registrySessionFromDurable(durable))
	}
	sort.Slice(desired, func(i, j int) bool {
		if !desired[i].UpdatedAt.Equal(desired[j].UpdatedAt) {
			return desired[i].UpdatedAt.After(desired[j].UpdatedAt)
		}
		return desired[i].ID < desired[j].ID
	})
	desiredByID := make(map[string]Session, len(desired))
	desiredByChat := make(map[string]Session, len(desired))
	for _, session := range desired {
		desiredByID[session.ID] = session
		desiredByChat[session.ChatID] = session
	}

	changed := false
	var next []Session
	seenIDs := make(map[string]bool, len(reg.Sessions))
	seenChats := make(map[string]bool, len(reg.Sessions))
	for _, existing := range reg.Sessions {
		existingID := strings.TrimSpace(existing.ID)
		existingChatID := strings.TrimSpace(existing.ChatID)
		if existingID == "" || existingChatID == "" || isControlFallbackSessionID(existingID) || (reg.ControlChatID != "" && existingChatID == reg.ControlChatID) {
			changed = true
			continue
		}
		replacement, ok := desiredByID[existingID]
		if !ok {
			replacement, ok = desiredByChat[existingChatID]
		}
		if ok {
			if seenIDs[replacement.ID] || seenChats[replacement.ChatID] {
				changed = true
				continue
			}
			if !registrySessionsEqual(existing, replacement) {
				changed = true
			}
			next = append(next, replacement)
			seenIDs[replacement.ID] = true
			seenChats[replacement.ChatID] = true
			continue
		}
		if len(state.Sessions) > 0 {
			changed = true
			continue
		}
		if seenIDs[existingID] || seenChats[existingChatID] {
			changed = true
			continue
		}
		next = append(next, existing)
		seenIDs[existingID] = true
		seenChats[existingChatID] = true
	}
	for _, session := range desired {
		if seenIDs[session.ID] || seenChats[session.ChatID] {
			continue
		}
		next = append(next, session)
		seenIDs[session.ID] = true
		seenChats[session.ChatID] = true
		changed = true
	}
	if changed {
		reg.Sessions = next
	}
	return changed
}

func registrySessionsEqual(left Session, right Session) bool {
	return left.ID == right.ID &&
		left.ChatID == right.ChatID &&
		left.ChatURL == right.ChatURL &&
		left.Topic == right.Topic &&
		left.UserTitle == right.UserTitle &&
		left.TitleSource == right.TitleSource &&
		left.Status == right.Status &&
		left.CodexThreadID == right.CodexThreadID &&
		left.Cwd == right.Cwd &&
		modelProfileSnapshotsEqual(left.ModelProfile, right.ModelProfile) &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}

func modelProfileSnapshotsEqual(left modelprofile.Snapshot, right modelprofile.Snapshot) bool {
	return left.Name == right.Name &&
		left.Provider == right.Provider &&
		left.APIKeyRef == right.APIKeyRef &&
		left.Model == right.Model &&
		left.SSHProxy == right.SSHProxy &&
		left.Revision == right.Revision &&
		left.KeyFingerprint == right.KeyFingerprint &&
		left.BaseURLHash == right.BaseURLHash &&
		left.AdapterProfile == right.AdapterProfile &&
		left.DefaultModel == right.DefaultModel &&
		left.ModelFingerprint == right.ModelFingerprint &&
		left.CatalogFingerprint == right.CatalogFingerprint &&
		left.SSHProxyFingerprint == right.SSHProxyFingerprint &&
		left.CapturedAt.Equal(right.CapturedAt)
}

func (b *Bridge) migrateRegistryProjectionToStore(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	if err := b.restoreMachineHostnameOverrideFromStore(ctx); err != nil {
		return err
	}
	if b.reg.ControlChatID == "" && len(b.reg.Sessions) == 0 && len(b.reg.Chats) == 0 {
		return nil
	}
	return b.store.UpdateIfChanged(ctx, func(state *teamstore.State) (bool, error) {
		now := time.Now()
		changed := false
		if b.user.ID != "" || b.user.UserPrincipalName != "" {
			if b.scope.ID == "" {
				b.scope = ScopeIdentityForUser(b.user)
			}
			if b.machine.ID == "" {
				b.machine = MachineRecordForUser(b.user, b.scope)
				b.applyRegistryMachineHostnameOverride()
			}
			machineID := b.machine.ID
			if state.MachineIdentity.ID == "" {
				state.MachineIdentity.ID = machineID
				state.MachineIdentity.CreatedAt = now
				changed = true
			}
			machineChanged := false
			setMachineString := func(target *string, value string) {
				if *target != value {
					*target = value
					machineChanged = true
				}
			}
			setMachineString(&state.MachineIdentity.Label, b.machine.Label)
			setMachineString(&state.MachineIdentity.Hostname, b.machine.Hostname)
			setMachineString(&state.MachineIdentity.AccountID, b.user.ID)
			setMachineString(&state.MachineIdentity.UserPrincipal, b.user.UserPrincipalName)
			setMachineString(&state.MachineIdentity.Profile, b.scope.Profile)
			setMachineString(&state.MachineIdentity.ScopeID, b.scope.ID)
			if state.MachineIdentity.Kind != b.machine.Kind {
				state.MachineIdentity.Kind = b.machine.Kind
				machineChanged = true
			}
			if state.MachineIdentity.Priority != b.machine.Priority {
				state.MachineIdentity.Priority = b.machine.Priority
				machineChanged = true
			}
			if state.MachineIdentity.UpdatedAt.IsZero() {
				machineChanged = true
			}
			if machineChanged {
				state.MachineIdentity.UpdatedAt = now
				changed = true
			}
		}
		if state.ControlChat.TeamsChatID == "" && b.reg.ControlChatID != "" {
			state.ControlChat.MachineID = state.MachineIdentity.ID
			state.ControlChat.ScopeID = b.scope.ID
			state.ControlChat.AccountID = b.user.ID
			state.ControlChat.Profile = b.scope.Profile
			state.ControlChat.TeamsChatID = b.reg.ControlChatID
			state.ControlChat.TeamsChatURL = b.reg.ControlChatURL
			state.ControlChat.TeamsChatTopic = b.reg.ControlChatTopic
			state.ControlChat.UserTitle = b.reg.ControlChatUserTitle
			state.ControlChat.TitleSource = b.reg.ControlChatTitleSource
			state.ControlChat.BoundAt = now
			state.ControlChat.UpdatedAt = now
			changed = true
		} else if state.ControlChat.TeamsChatID == b.reg.ControlChatID && b.reg.ControlChatID != "" {
			controlChanged := false
			setControlString := func(target *string, value string) {
				if strings.TrimSpace(value) != "" && *target != value {
					*target = value
					controlChanged = true
				}
			}
			setControlString(&state.ControlChat.TeamsChatURL, b.reg.ControlChatURL)
			setControlString(&state.ControlChat.TeamsChatTopic, b.reg.ControlChatTopic)
			setControlString(&state.ControlChat.UserTitle, b.reg.ControlChatUserTitle)
			setControlString(&state.ControlChat.TitleSource, b.reg.ControlChatTitleSource)
			if controlChanged {
				state.ControlChat.UpdatedAt = now
				changed = true
			}
		}
		for _, session := range b.reg.Sessions {
			if isControlFallbackSessionID(session.ID) {
				continue
			}
			if strings.TrimSpace(session.ID) == "" || strings.TrimSpace(session.ChatID) == "" {
				continue
			}
			if _, ok := state.Sessions[session.ID]; ok {
				continue
			}
			status := teamstore.SessionStatus(session.Status)
			if status == "" {
				status = teamstore.SessionStatusActive
			}
			created := session.CreatedAt
			if created.IsZero() {
				created = now
			}
			updated := session.UpdatedAt
			if updated.IsZero() {
				updated = created
			}
			state.Sessions[session.ID] = teamstore.SessionContext{
				ID:            session.ID,
				Status:        status,
				TeamsChatID:   session.ChatID,
				TeamsChatURL:  session.ChatURL,
				TeamsTopic:    session.Topic,
				UserTitle:     session.UserTitle,
				TitleSource:   session.TitleSource,
				CodexThreadID: session.CodexThreadID,
				Cwd:           session.Cwd,
				ModelProfile:  session.ModelProfile,
				CreatedAt:     created,
				UpdatedAt:     updated,
			}
			changed = true
		}
		if fallback, ok := state.Sessions[controlFallbackSessionID]; ok {
			if sanitizeControlFallbackSession(&fallback, "", modelprofile.Snapshot{}, now) {
				state.Sessions[controlFallbackSessionID] = fallback
				changed = true
			}
		}
		for chatID, chatState := range b.reg.Chats {
			chatID = strings.TrimSpace(chatID)
			if chatID == "" {
				continue
			}
			for _, messageID := range chatState.SeenMessageIDs {
				messageID = strings.TrimSpace(messageID)
				if messageID == "" {
					continue
				}
				id := "inbound:" + chatID + ":" + messageID
				if _, ok := state.InboundEvents[id]; ok {
					continue
				}
				state.InboundEvents[id] = teamstore.InboundEvent{
					ID:             id,
					TeamsChatID:    chatID,
					TeamsMessageID: messageID,
					Source:         "registry_migration",
					Status:         teamstore.InboundStatusPersisted,
					ReceivedAt:     now,
					CreatedAt:      now,
					UpdatedAt:      now,
				}
				changed = true
			}
			for _, messageID := range chatState.SentMessageIDs {
				messageID = strings.TrimSpace(messageID)
				if messageID == "" {
					continue
				}
				id := migratedSentOutboxID(chatID, messageID)
				if _, ok := state.OutboxMessages[id]; ok {
					continue
				}
				state.OutboxMessages[id] = teamstore.OutboxMessage{
					ID:             id,
					TeamsChatID:    chatID,
					TeamsMessageID: messageID,
					Kind:           "registry-sent",
					Status:         teamstore.OutboxStatusSent,
					CreatedAt:      now,
					UpdatedAt:      now,
					SentAt:         now,
				}
				changed = true
			}
		}
		return changed, nil
	})
}

func migratedSentOutboxID(chatID string, messageID string) string {
	sum := sha256.Sum256([]byte(chatID + "\x00" + messageID))
	return "outbox:registry-sent:" + hex.EncodeToString(sum[:])
}

func (b *Bridge) recordControlChatBinding(ctx context.Context, chat Chat) error {
	return b.recordControlChatBindingWithTitle(ctx, chat, "", "")
}

func (b *Bridge) recordControlChatBindingWithTitle(ctx context.Context, chat Chat, userTitle string, titleSource string) error {
	if chat.ID == "" {
		return nil
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	if b.machine.ID == "" {
		b.machine = MachineRecordForUser(b.user, b.scope)
		b.applyRegistryMachineHostnameOverride()
	}
	machineID := b.machine.ID
	label := b.machine.Label
	userTitle = SanitizeDashboardTitle(userTitle)
	titleSource = strings.TrimSpace(titleSource)
	return b.store.UpdateIfChanged(ctx, func(state *teamstore.State) (bool, error) {
		now := time.Now()
		changed := false
		machineChanged := false
		if state.MachineIdentity.ID == "" {
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
		setMachineString(&state.MachineIdentity.Label, label)
		setMachineString(&state.MachineIdentity.Hostname, label)
		setMachineString(&state.MachineIdentity.AccountID, b.user.ID)
		setMachineString(&state.MachineIdentity.UserPrincipal, b.user.UserPrincipalName)
		setMachineString(&state.MachineIdentity.Profile, b.scope.Profile)
		setMachineString(&state.MachineIdentity.ScopeID, b.scope.ID)
		if state.MachineIdentity.Kind != b.machine.Kind {
			state.MachineIdentity.Kind = b.machine.Kind
			machineChanged = true
		}
		if state.MachineIdentity.Priority != b.machine.Priority {
			state.MachineIdentity.Priority = b.machine.Priority
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
		setControlString(&state.ControlChat.ScopeID, b.scope.ID)
		setControlString(&state.ControlChat.AccountID, b.user.ID)
		setControlString(&state.ControlChat.TeamsChatID, chat.ID)
		setControlString(&state.ControlChat.TeamsChatURL, chat.WebURL)
		setControlString(&state.ControlChat.TeamsChatTopic, chat.Topic)
		if userTitle != "" || titleSource != "" {
			setControlString(&state.ControlChat.UserTitle, userTitle)
			setControlString(&state.ControlChat.TitleSource, titleSource)
		}
		if controlChanged {
			state.ControlChat.UpdatedAt = now
			changed = true
		}
		return changed, nil
	})
}

func (b *Bridge) ensureDurableSession(ctx context.Context, session *Session) error {
	if session == nil {
		return nil
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	status := teamstore.SessionStatusActive
	switch session.Status {
	case "closed":
		status = teamstore.SessionStatusClosed
	case "archived":
		status = teamstore.SessionStatusArchived
	}
	_, _, err := b.store.CreateSession(ctx, teamstore.SessionContext{
		ID:            session.ID,
		Status:        status,
		TeamsChatID:   session.ChatID,
		TeamsChatURL:  session.ChatURL,
		TeamsTopic:    session.Topic,
		UserTitle:     session.UserTitle,
		TitleSource:   session.TitleSource,
		CodexThreadID: session.CodexThreadID,
		RunnerKind:    "executor",
		Cwd:           session.Cwd,
		ModelProfile:  session.ModelProfile,
		CreatedAt:     session.CreatedAt,
		UpdatedAt:     session.UpdatedAt,
	})
	return err
}

func (b *Bridge) closeDurableSession(ctx context.Context, session *Session) error {
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.Sessions[session.ID]
		current.Status = teamstore.SessionStatusClosed
		current.UpdatedAt = session.UpdatedAt
		state.Sessions[session.ID] = current
		return nil
	})
}

func (b *Bridge) persistInbound(ctx context.Context, session *Session, msg ChatMessage) (teamstore.InboundEvent, bool, error) {
	return b.persistInboundWithStatus(ctx, session, msg, teamstore.InboundStatusPersisted)
}

func (b *Bridge) persistInboundWithStatus(ctx context.Context, session *Session, msg ChatMessage, status teamstore.InboundStatus) (teamstore.InboundEvent, bool, error) {
	return b.persistInboundWithStatusAndSource(ctx, session, msg, status, "teams")
}

func (b *Bridge) persistControlInboundWithStatus(ctx context.Context, msg ChatMessage, status teamstore.InboundStatus, source string) (teamstore.InboundEvent, bool, error) {
	if err := b.ensureStore(); err != nil {
		return teamstore.InboundEvent{}, false, err
	}
	session := &Session{
		ID:     controlFallbackSessionID,
		ChatID: b.reg.ControlChatID,
	}
	return b.persistInboundWithStatusAndSource(ctx, session, msg, status, source)
}

func (b *Bridge) persistInboundWithStatusAndSource(ctx context.Context, session *Session, msg ChatMessage, status teamstore.InboundStatus, source string) (teamstore.InboundEvent, bool, error) {
	text := promptTextFromTeamsMessageHTML(msg.Body.Content)
	if strings.TrimSpace(source) == "" {
		source = "teams"
	}
	leaseGeneration := b.currentLeaseGeneration()
	event := teamstore.InboundEvent{
		SessionID:       session.ID,
		TeamsChatID:     session.ChatID,
		TeamsMessageID:  msg.ID,
		AuthorUserID:    chatMessageAuthorUserID(msg),
		AuthorName:      chatMessageAuthorDisplayName(msg),
		ScopeID:         b.scope.ID,
		MachineID:       b.machine.ID,
		LeaseGeneration: leaseGeneration,
		Text:            text,
		TextHash:        inboundTextHashForTeamsMessage(text, msg),
		Source:          source,
		Status:          status,
	}
	if shouldPersistInboundAttachmentContext(session, msg) {
		event.TeamsBodyType = strings.TrimSpace(msg.Body.ContentType)
		event.TeamsBodyHTML = msg.Body.Content
		event.TeamsAttachments = inboundAttachmentContextsFromMessage(msg)
	}
	return b.store.PersistInbound(ctx, event)
}

func shouldPersistInboundAttachmentContext(session *Session, msg ChatMessage) bool {
	return session != nil && session.ID == controlFallbackSessionID && (hasMessageReferenceAttachment(msg.Attachments) || hasSupportedTeamsMediaCardAttachment(msg.Attachments))
}

func inboundAttachmentContextsFromMessage(msg ChatMessage) []teamstore.InboundAttachmentContext {
	if len(msg.Attachments) == 0 {
		return nil
	}
	out := make([]teamstore.InboundAttachmentContext, 0, len(msg.Attachments))
	for _, attachment := range msg.Attachments {
		if !isMessageReferenceAttachment(attachment) && !isSupportedTeamsMediaCardAttachment(attachment) {
			continue
		}
		out = append(out, teamstore.InboundAttachmentContext{
			ID:          strings.TrimSpace(attachment.ID),
			ContentType: strings.TrimSpace(attachment.ContentType),
			ContentURL:  strings.TrimSpace(attachment.ContentURL),
			Content:     attachment.Content,
			Name:        strings.TrimSpace(attachment.Name),
		})
	}
	return out
}

func chatMessageFromInboundContext(inbound teamstore.InboundEvent) (ChatMessage, bool) {
	msg := ChatMessage{
		ID:              strings.TrimSpace(inbound.TeamsMessageID),
		ChatID:          strings.TrimSpace(inbound.TeamsChatID),
		CreatedDateTime: inbound.ReceivedAt.Format(time.RFC3339),
	}
	msg.Body.ContentType = strings.TrimSpace(inbound.TeamsBodyType)
	msg.Body.Content = inbound.TeamsBodyHTML
	if msg.Body.Content == "" && strings.TrimSpace(inbound.Text) != "" {
		msg.Body.ContentType = "html"
		msg.Body.Content = html.EscapeString(inbound.Text)
	}
	if len(inbound.TeamsAttachments) > 0 {
		msg.Attachments = make([]MessageAttachment, 0, len(inbound.TeamsAttachments))
		for _, attachment := range inbound.TeamsAttachments {
			msg.Attachments = append(msg.Attachments, MessageAttachment{
				ID:          strings.TrimSpace(attachment.ID),
				ContentType: strings.TrimSpace(attachment.ContentType),
				ContentURL:  strings.TrimSpace(attachment.ContentURL),
				Content:     attachment.Content,
				Name:        strings.TrimSpace(attachment.Name),
			})
		}
	}
	return msg, strings.TrimSpace(msg.Body.Content) != "" || len(msg.Attachments) > 0
}

func inboundTextHashForTeamsMessage(text string, msg ChatMessage) string {
	text = strings.TrimSpace(text)
	if text == "" {
		switch {
		case hasMessageReferenceAttachment(msg.Attachments):
			text = defaultReferencedTeamsMessagePrompt
		case len(msg.Attachments) > 0 || len(HostedContentIDsFromHTML(msg.Body.Content)) > 0:
			text = defaultLocalAttachmentPrompt
		}
	}
	return normalizedTextHash(text)
}

func (b *Bridge) deferSessionMessageDuringTranscriptImport(ctx context.Context, session *Session, msg ChatMessage) error {
	if session == nil {
		return nil
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	source := "teams_session_import_deferred"
	if len(msg.Attachments) > 0 || len(HostedContentIDsFromHTML(msg.Body.Content)) > 0 {
		source = "teams_session_import_deferred_attachment"
	}
	inbound, created, err := b.persistInboundWithStatusAndSource(ctx, session, msg, teamstore.InboundStatusDeferred, source)
	if err != nil {
		return err
	}
	if created {
		return b.sendExternalDeferredReceipt(ctx, session.ChatID, inbound, "⏳ Codex received your question. I’m preparing this chat history first, then I’ll respond.")
	}
	return nil
}

func (b *Bridge) queueTurn(ctx context.Context, session *Session, inbound teamstore.InboundEvent) (teamstore.Turn, bool, error) {
	leaseGeneration := b.currentLeaseGeneration()
	return b.store.QueueTurn(ctx, teamstore.Turn{
		SessionID:       session.ID,
		InboundEventID:  inbound.ID,
		ScopeID:         b.scope.ID,
		MachineID:       b.machine.ID,
		LeaseGeneration: leaseGeneration,
		CodexThreadID:   session.CodexThreadID,
		ModelProfile:    session.ModelProfile,
	})
}

func sessionWithTurnModelProfile(session *Session, turn teamstore.Turn) *Session {
	if session == nil || turn.ModelProfile.IsZero() {
		return session
	}
	clone := *session
	clone.ModelProfile = turn.ModelProfile
	return &clone
}

func retryTurnModelProfile(turn teamstore.Turn, fallback modelprofile.Snapshot) modelprofile.Snapshot {
	if !turn.ModelProfile.IsZero() {
		return turn.ModelProfile
	}
	return fallback
}

func (b *Bridge) queueAndSendOutboxChunks(ctx context.Context, sessionID string, turnID string, chatID string, kind string, text string) error {
	return b.queueAndSendOutboxChunksWithOptions(ctx, sessionID, turnID, chatID, kind, text, outboxQueueOptions{})
}

func (b *Bridge) queueAndSendOutboxChunksWithOptions(ctx context.Context, sessionID string, turnID string, chatID string, kind string, text string, opts outboxQueueOptions) error {
	return b.queueOrSendOutboxChunks(ctx, sessionID, turnID, chatID, kind, text, opts, false)
}

func (b *Bridge) queueOrSendOutboxChunks(ctx context.Context, sessionID string, turnID string, chatID string, kind string, text string, opts outboxQueueOptions, queueOnly bool) error {
	queued, err := b.queueOutboxChunksWithOptions(ctx, sessionID, turnID, chatID, kind, text, opts)
	if err != nil {
		return err
	}
	if len(queued) == 0 || queueOnly {
		return nil
	}
	if err := b.flushPendingOutboxForChat(ctx, chatID); err != nil {
		return err
	}
	b.boostPolling(time.Now())
	return nil
}

func (b *Bridge) queueOutboxChunksWithOptions(ctx context.Context, sessionID string, turnID string, chatID string, kind string, text string, opts outboxQueueOptions) ([]teamstore.OutboxMessage, error) {
	if shouldSuppressCodexCommandOutbox(kind) {
		return nil, nil
	}
	renderKind := renderKindForOutbox(kind)
	if renderKind == TeamsRenderAssistant {
		text = StripOAIMemoryCitationBlocks(text)
	}
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    renderKind,
		Text:    text,
	}, TeamsRenderOptions{
		HardLimitBytes:   safeTeamsHTMLContentBytes,
		TargetLimitBytes: teamsChunkHTMLContentBytes,
	})
	queued := make([]teamstore.OutboxMessage, 0, len(chunks))
	leaseGeneration := b.currentLeaseGeneration()
	for i, chunk := range chunks {
		msgKind := kind
		body := chunk.Text
		if len(chunks) > 1 {
			msgKind = fmt.Sprintf("%s-%03d", kind, i+1)
		}
		msg := teamstore.OutboxMessage{
			SessionID:       sessionID,
			TurnID:          turnID,
			TeamsChatID:     chatID,
			ScopeID:         b.scope.ID,
			MachineID:       b.machine.ID,
			LeaseGeneration: leaseGeneration,
			Kind:            msgKind,
			Body:            body,
			SourceTextHash:  normalizedTextHash(text),
			PartIndex:       chunk.PartIndex,
			PartCount:       chunk.PartCount,
			RenderedBytes:   chunk.ByteLength,
		}
		mentionThisPart := opts.MentionOwner && i == 0
		if opts.MentionOwner && shouldMentionOwnerOnLastOutboxPart(kind, opts.NotificationKind) {
			mentionThisPart = i == len(chunks)-1
		}
		if mentionThisPart {
			msg.MentionOwner = true
			msg.NotificationKind = opts.NotificationKind
		}
		if msg.NotificationKind == "" && msg.MentionOwner {
			msg.NotificationKind = "owner_notification"
		}
		queuedMsg, err := b.queueOutbox(ctx, msg)
		if err != nil {
			return nil, err
		}
		queued = append(queued, queuedMsg)
	}
	return queued, nil
}

func (b *Bridge) queueAndSendTranscriptDeliveryChunksWithOptions(ctx context.Context, session Session, local codexhistory.Session, record TranscriptRecord, checkpointLine int, checkpointOffset int64, kind string, text string, opts outboxQueueOptions) error {
	return b.queueOrSendTranscriptDeliveryChunksWithOptions(ctx, session, local, record, checkpointLine, checkpointOffset, kind, text, opts, "sync:"+session.ID, transcriptCheckpointID(session.ID), false)
}

func (b *Bridge) queueOrSendTranscriptDeliveryChunksWithOptions(ctx context.Context, session Session, local codexhistory.Session, record TranscriptRecord, checkpointLine int, checkpointOffset int64, kind string, text string, opts outboxQueueOptions, turnID string, checkpointID string, queueOnly bool) error {
	queued, err := b.queueTranscriptDeliveryChunksWithOptions(ctx, session, local, record, checkpointLine, checkpointOffset, kind, text, opts, turnID)
	if err != nil {
		return err
	}
	if len(queued) > 0 && !queueOnly {
		if err := b.flushPendingOutboxForChat(ctx, session.ChatID); err != nil {
			return err
		}
		b.boostPolling(time.Now())
	}
	if strings.TrimSpace(checkpointID) == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	return b.recordTranscriptCheckpointDetailedWithID(ctx, session, local.FilePath, transcriptRecordCheckpointKey(record), checkpointLine, checkpointOffset, checkpointID)
}

func (b *Bridge) queueTranscriptDeliveryChunksWithOptions(ctx context.Context, session Session, local codexhistory.Session, record TranscriptRecord, checkpointLine int, checkpointOffset int64, kind string, text string, opts outboxQueueOptions, turnID string) ([]teamstore.OutboxMessage, error) {
	if shouldSuppressCodexCommandOutbox(kind) {
		return nil, nil
	}
	renderKind := renderKindForOutbox(kind)
	if renderKind == TeamsRenderAssistant {
		text = StripOAIMemoryCitationBlocks(text)
	}
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    renderKind,
		Text:    text,
	}, TeamsRenderOptions{
		HardLimitBytes:   safeTeamsHTMLContentBytes,
		TargetLimitBytes: teamsChunkHTMLContentBytes,
	})
	if len(chunks) == 0 {
		return nil, nil
	}
	record.SourceLine = checkpointLine
	record.SourceOffset = checkpointOffset
	baseDelivery := transcriptDeliveryRecord(session, local, record, kind, text)
	queued := make([]teamstore.OutboxMessage, 0, len(chunks))
	leaseGeneration := b.currentLeaseGeneration()
	if strings.TrimSpace(turnID) == "" {
		turnID = "sync:" + session.ID
	}
	for i, chunk := range chunks {
		msgKind := kind
		body := chunk.Text
		if len(chunks) > 1 {
			msgKind = fmt.Sprintf("%s-%03d", kind, i+1)
		}
		delivery := baseDelivery
		delivery.ID = transcriptDeliveryPartID(baseDelivery.ID, chunk.PartIndex, chunk.PartCount)
		delivery.Kind = msgKind
		msg := teamstore.OutboxMessage{
			ID:              transcriptDeliveryOutboxID(delivery.ID),
			SessionID:       session.ID,
			TurnID:          turnID,
			TeamsChatID:     session.ChatID,
			ScopeID:         b.scope.ID,
			MachineID:       b.machine.ID,
			LeaseGeneration: leaseGeneration,
			Kind:            msgKind,
			Body:            body,
			SourceTextHash:  normalizedTextHash(text),
			PartIndex:       chunk.PartIndex,
			PartCount:       chunk.PartCount,
			RenderedBytes:   chunk.ByteLength,
		}
		mentionThisPart := opts.MentionOwner && i == 0
		if opts.MentionOwner && shouldMentionOwnerOnLastOutboxPart(kind, opts.NotificationKind) {
			mentionThisPart = i == len(chunks)-1
		}
		if mentionThisPart {
			msg.MentionOwner = true
			msg.NotificationKind = opts.NotificationKind
		}
		if msg.NotificationKind == "" && msg.MentionOwner {
			msg.NotificationKind = "owner_notification"
		}
		msg = b.prepareOutboxForQueue(ctx, msg)
		queuedMsg, _, _, err := b.store.QueueTranscriptDeliveryOutbox(ctx, teamstore.TranscriptDeliveryQueueRequest{
			Message:  msg,
			Delivery: delivery,
		})
		if err != nil {
			return nil, err
		}
		switch queuedMsg.Status {
		case teamstore.OutboxStatusQueued, teamstore.OutboxStatusSending, teamstore.OutboxStatusAccepted:
			queued = append(queued, queuedMsg)
		}
	}
	return queued, nil
}

func (b *Bridge) queueAndSendOutbox(ctx context.Context, msg teamstore.OutboxMessage) error {
	queued, err := b.queueOutbox(ctx, msg)
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	return b.flushPendingOutboxForChat(ctx, queued.TeamsChatID)
}

func (b *Bridge) queueAndBestEffortSendOutbox(ctx context.Context, msg teamstore.OutboxMessage) error {
	queued, err := b.queueOutbox(ctx, msg)
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	if err := b.flushPendingOutboxForChat(ctx, queued.TeamsChatID); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams best-effort outbox send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) flushExistingOutboxIfPending(ctx context.Context, outboxID string, chatID string) error {
	if b == nil || b.store == nil {
		return nil
	}
	outboxID = strings.TrimSpace(outboxID)
	chatID = strings.TrimSpace(chatID)
	if outboxID == "" || chatID == "" {
		return nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	existing, ok := state.OutboxMessages[outboxID]
	if !ok || existing.Status == teamstore.OutboxStatusSent {
		return nil
	}
	return b.flushPendingOutboxForChat(ctx, chatID)
}

func (b *Bridge) queueOutbox(ctx context.Context, msg teamstore.OutboxMessage) (teamstore.OutboxMessage, error) {
	if err := b.ensureStore(); err != nil {
		return teamstore.OutboxMessage{}, err
	}
	msg = b.prepareOutboxForQueue(ctx, msg)
	queued, created, err := b.store.QueueOutbox(ctx, msg)
	if err != nil {
		return teamstore.OutboxMessage{}, err
	}
	if created {
		b.recordControlChatHelperMessage(ctx, queued)
	}
	return queued, nil
}

func (b *Bridge) prepareOutboxForQueue(ctx context.Context, msg teamstore.OutboxMessage) teamstore.OutboxMessage {
	if msg.ScopeID == "" {
		msg.ScopeID = b.scope.ID
	}
	if msg.MachineID == "" {
		msg.MachineID = b.machine.ID
	}
	if msg.LeaseGeneration == 0 {
		msg.LeaseGeneration = b.currentLeaseGeneration()
	}
	msg, _ = b.outboxWithWorkflowOwnerMentionSuppressed(ctx, msg)
	return msg
}

func (b *Bridge) outboxWithWorkflowOwnerMentionSuppressed(ctx context.Context, msg teamstore.OutboxMessage) (teamstore.OutboxMessage, bool) {
	if b.shouldSuppressOwnerMentionForWorkflow(ctx, msg) {
		msg.MentionOwner = false
		return msg, true
	}
	return msg, false
}

func (b *Bridge) suppressQueuedOutboxOwnerMentionForWorkflow(ctx context.Context, msg teamstore.OutboxMessage) teamstore.OutboxMessage {
	next, suppressed := b.outboxWithWorkflowOwnerMentionSuppressed(ctx, msg)
	if !suppressed || b == nil || b.store == nil || strings.TrimSpace(msg.ID) == "" {
		return next
	}
	updated, err := b.store.SuppressOutboxOwnerMention(ctx, msg.ID)
	if err != nil {
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams workflow mention suppression warning: %v\n", err)
		}
		return next
	}
	return updated
}

func (b *Bridge) shouldSuppressOwnerMentionForWorkflow(ctx context.Context, msg teamstore.OutboxMessage) bool {
	if b == nil || b.store == nil || !msg.MentionOwner || !outboxHasWorkflowNotificationCandidate(msg) {
		return false
	}
	state, err := b.store.WorkflowNotificationStateSnapshot(ctx)
	if err != nil {
		return false
	}
	controlChatID := workflowFallbackControlChatID(state)
	if controlChatID != "" && strings.TrimSpace(msg.TeamsChatID) != controlChatID {
		return true
	}
	if !workflowNotificationConfigPresent(state.Workflow) && strings.TrimSpace(b.scope.ID) == "" {
		return false
	}
	cfg, err := b.effectiveWorkflowNotificationConfig(state)
	if err != nil {
		return false
	}
	if cfg.Enabled && b.workflowCardAvailable(ctx) {
		return true
	}
	return false
}

func workflowNotificationConfigPresent(cfg teamstore.WorkflowNotificationConfig) bool {
	return cfg.Enabled || strings.TrimSpace(cfg.ControlWebhookURLFile) != "" || strings.TrimSpace(cfg.ControlChatID) != ""
}

func outboxHasWorkflowNotificationCandidate(outbox teamstore.OutboxMessage) bool {
	kind := strings.ToLower(strings.TrimSpace(outbox.Kind))
	notificationKind := strings.ToLower(strings.TrimSpace(outbox.NotificationKind))
	switch {
	case notificationKind == "chat_created" || kind == "chat-created":
		return true
	case notificationKind == "local_session_started" || kind == "local-session-started":
		return true
	case notificationKind == "chat_recreated" || kind == "chat-moved":
		return true
	case notificationKind == "turn_completed":
		return true
	case notificationKind == "helper_upgrade_completed":
		return true
	case notificationKind == helperUpgradeActivationFailedNotificationKind || notificationKind == helperUpgradeActivationActionRequiredNotificationKind:
		return true
	case notificationKind == "needs_attention":
		return true
	case strings.Contains(kind, "reload-complete"):
		return true
	case strings.Contains(kind, "restart-complete"):
		return true
	case strings.HasPrefix(outbox.ID, "outbox:codex-upgrade-target:"):
		return true
	case workflowOutboxNeedsAttention(kind):
		return true
	default:
		return false
	}
}

func (b *Bridge) flushPendingOutbox(ctx context.Context, sessionID string, turnID string) error {
	return b.flushPendingOutboxFiltered(ctx, sessionID, turnID, "")
}

func (b *Bridge) flushPendingOutboxMainLoop(ctx context.Context) error {
	return b.flushPendingOutboxFilteredWithOptions(ctx, "", "", "", outboxFlushOptions{MaxMessages: mainLoopOutboxFlushMaxMessages})
}

func (b *Bridge) flushPendingOutboxForChat(ctx context.Context, chatID string) error {
	return b.flushPendingOutboxFiltered(ctx, "", "", chatID)
}

func (b *Bridge) flushPendingOutboxFiltered(ctx context.Context, sessionID string, turnID string, chatID string) error {
	return b.flushPendingOutboxFilteredWithOptions(ctx, sessionID, turnID, chatID, outboxFlushOptions{})
}

type outboxFlushOptions struct {
	MaxMessages int
}

func (b *Bridge) flushPendingOutboxFilteredWithOptions(ctx context.Context, sessionID string, turnID string, chatID string, opts outboxFlushOptions) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	var sentSideEffects []sentOutboxSideEffect
	b.outboxFlushMu.Lock()
	err := func() error {
		defer b.outboxFlushMu.Unlock()
		pending, err := b.store.PendingOutbox(ctx)
		if err != nil {
			return err
		}
		sort.Slice(pending, func(i, j int) bool {
			if pending[i].TeamsChatID != pending[j].TeamsChatID {
				return pending[i].CreatedAt.Before(pending[j].CreatedAt)
			}
			if pending[i].Sequence != pending[j].Sequence {
				return pending[i].Sequence < pending[j].Sequence
			}
			return pending[i].CreatedAt.Before(pending[j].CreatedAt)
		})
		var firstErr error
		var firstBlockedErr error
		sent := 0
		for _, msg := range pending {
			if chatID != "" && msg.TeamsChatID != chatID {
				continue
			}
			if sessionID != "" && msg.SessionID != sessionID {
				continue
			}
			if turnID != "" && msg.TurnID != turnID {
				continue
			}
			if err := b.sendQueuedOutboxWithOptions(ctx, msg, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true, SentSideEffects: &sentSideEffects}); err != nil {
				if isOutboxDeliveryDeferred(err) {
					if firstBlockedErr == nil {
						firstBlockedErr = err
					}
					continue
				}
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			sent++
			if opts.MaxMessages > 0 && sent >= opts.MaxMessages {
				break
			}
		}
		if firstErr != nil {
			return firstErr
		}
		if opts.MaxMessages > 0 && sent >= opts.MaxMessages {
			return nil
		}
		return firstBlockedErr
	}()
	for _, effect := range sentSideEffects {
		b.handleSentOutboxSideEffects(ctx, effect.Outbox, effect.TeamsMessage)
	}
	return err
}

func (b *Bridge) sendQueuedOutbox(ctx context.Context, outbox teamstore.OutboxMessage) error {
	return b.sendQueuedOutboxWithOptions(ctx, outbox, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true})
}

type outboxSendOptions struct {
	RespectRateLimitBlock bool
	RecordRateLimit       bool
	SentSideEffects       *[]sentOutboxSideEffect
}

type sentOutboxSideEffect struct {
	Outbox       teamstore.OutboxMessage
	TeamsMessage ChatMessage
}

func (b *Bridge) rememberAcceptedOutbox(outboxID string, teamsMessageID string) {
	if b == nil || strings.TrimSpace(outboxID) == "" || strings.TrimSpace(teamsMessageID) == "" {
		return
	}
	b.acceptedOutboxMu.Lock()
	defer b.acceptedOutboxMu.Unlock()
	if b.acceptedOutboxes == nil {
		b.acceptedOutboxes = make(map[string]acceptedOutboxRecovery)
	}
	b.acceptedOutboxes[strings.TrimSpace(outboxID)] = acceptedOutboxRecovery{
		TeamsMessageID: strings.TrimSpace(teamsMessageID),
		AcceptedAt:     time.Now(),
	}
}

func (b *Bridge) recoveredAcceptedOutbox(outboxID string) (acceptedOutboxRecovery, bool) {
	if b == nil || strings.TrimSpace(outboxID) == "" {
		return acceptedOutboxRecovery{}, false
	}
	b.acceptedOutboxMu.Lock()
	defer b.acceptedOutboxMu.Unlock()
	record, ok := b.acceptedOutboxes[strings.TrimSpace(outboxID)]
	return record, ok
}

func (b *Bridge) forgetAcceptedOutbox(outboxID string) {
	if b == nil || strings.TrimSpace(outboxID) == "" {
		return
	}
	b.acceptedOutboxMu.Lock()
	defer b.acceptedOutboxMu.Unlock()
	delete(b.acceptedOutboxes, strings.TrimSpace(outboxID))
}

func (b *Bridge) waitForOutboxSendPace(ctx context.Context, chatID string) error {
	if b == nil || !shouldPaceGraphOutboxSends(b.graph) || graphOutboxSendMinInterval <= 0 {
		return nil
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil
	}
	now := time.Now()
	b.outboxSendPaceMu.Lock()
	if b.outboxSendPaceLast == nil {
		b.outboxSendPaceLast = make(map[string]time.Time)
	}
	last := b.outboxSendPaceLast[chatID]
	delay := graphOutboxSendMinInterval - now.Sub(last)
	if last.IsZero() || delay <= 0 {
		b.outboxSendPaceLast[chatID] = now
		b.outboxSendPaceMu.Unlock()
		return nil
	}
	b.outboxSendPaceMu.Unlock()
	if err := b.graph.sleepFor(ctx, delay); err != nil {
		return err
	}
	b.outboxSendPaceMu.Lock()
	if b.outboxSendPaceLast == nil {
		b.outboxSendPaceLast = make(map[string]time.Time)
	}
	b.outboxSendPaceLast[chatID] = time.Now()
	b.outboxSendPaceMu.Unlock()
	return nil
}

func shouldPaceGraphOutboxSends(graph *GraphClient) bool {
	if graph == nil {
		return false
	}
	base := strings.TrimRight(strings.TrimSpace(graph.baseURL), "/")
	if base == "" {
		return true
	}
	graphBase := strings.TrimRight(graphBaseURL, "/")
	return base == graphBase || strings.HasPrefix(base, graphBase+"/")
}

func (b *Bridge) sendQueuedOutboxWithOptions(ctx context.Context, outbox teamstore.OutboxMessage, opts outboxSendOptions) error {
	if b.currentLeaseGeneration() > 0 {
		if err := b.ensureActiveControlLease(ctx); err != nil {
			return err
		}
	}
	if shouldSuppressCodexCommandOutbox(outbox.Kind) {
		_, err := b.store.MarkOutboxSent(ctx, outbox.ID, "")
		return err
	}
	if outbox.Status == teamstore.OutboxStatusAccepted && outbox.TeamsMessageID != "" {
		sent, err := b.store.MarkOutboxSent(ctx, outbox.ID, outbox.TeamsMessageID)
		if err == nil {
			b.forgetAcceptedOutbox(outbox.ID)
			b.recordSentOutboxSideEffect(ctx, sent, ChatMessage{}, opts)
		}
		return err
	}
	if recovered, ok := b.recoveredAcceptedOutbox(outbox.ID); ok && strings.TrimSpace(recovered.TeamsMessageID) != "" {
		if _, err := b.store.MarkOutboxAccepted(ctx, outbox.ID, recovered.TeamsMessageID); err != nil {
			return err
		}
		sent, err := b.store.MarkOutboxSent(ctx, outbox.ID, recovered.TeamsMessageID)
		if err == nil {
			b.forgetAcceptedOutbox(outbox.ID)
			b.recordSentOutboxSideEffect(ctx, sent, ChatMessage{ID: recovered.TeamsMessageID}, opts)
		}
		return err
	}
	if recovered, err := b.recoverAcceptedOutboxFromGraph(ctx, outbox, opts); recovered || err != nil {
		if err != nil && opts.RecordRateLimit {
			b.recordGraphReadRateLimit(context.Background(), outbox.TeamsChatID, err)
		}
		return err
	}
	if opts.RespectRateLimitBlock {
		if blockedUntil, ok := b.chatBlockedUntil(ctx, outbox.TeamsChatID); ok {
			return outboxDeliveryDeferredError{ChatID: outbox.TeamsChatID, Until: blockedUntil}
		}
	}
	if earlier, ok, err := b.store.EarlierUnsentOutbox(ctx, outbox); err != nil {
		return err
	} else if ok {
		return outboxDeliveryDeferredError{ChatID: outbox.TeamsChatID, Until: firstNonZeroTime(earlier.LastSendAttempt, earlier.CreatedAt)}
	}
	if err := b.waitForOutboxSendPace(ctx, outbox.TeamsChatID); err != nil {
		return err
	}
	claimed, err := b.store.MarkOutboxSendAttempt(ctx, outbox.ID)
	if errors.Is(err, teamstore.ErrOutboxSendNotClaimed) {
		return nil
	} else if err != nil {
		return err
	}
	outbox = b.suppressQueuedOutboxOwnerMentionForWorkflow(ctx, claimed)
	if outbox.DriveItemID == "" && outbox.AttachmentPath != "" {
		item, err := b.uploadQueuedOutboxAttachment(ctx, outbox)
		if err != nil {
			_, _ = b.store.MarkOutboxSendError(context.Background(), outbox.ID, err.Error())
			if opts.RecordRateLimit {
				b.recordGraphRateLimit(context.Background(), outbox.TeamsChatID, outbox.ID, err)
			}
			return err
		}
		outbox, err = b.store.MarkOutboxDriveItem(ctx, outbox.ID, item.ID, item.Name, item.ETag, item.WebURL, item.WebDavURL)
		if err != nil {
			return err
		}
	}
	if outbox.DriveItemID != "" && driveItemAttachmentID(driveItemFromOutbox(outbox)) == "" {
		item, err := b.refreshOutboxDriveItemMetadata(ctx, outbox)
		if err != nil {
			_, _ = b.store.MarkOutboxSendError(context.Background(), outbox.ID, err.Error())
			if opts.RecordRateLimit {
				b.recordGraphRateLimit(context.Background(), outbox.TeamsChatID, outbox.ID, err)
			}
			return err
		}
		outbox, err = b.store.MarkOutboxDriveItem(ctx, outbox.ID, item.ID, item.Name, item.ETag, item.WebURL, item.WebDavURL)
		if err != nil {
			return err
		}
	}
	var msg ChatMessage
	if outbox.DriveItemID != "" {
		msg, err = b.graph.SendDriveItemAttachmentWithoutRateLimitRetry(ctx, outbox.TeamsChatID, driveItemFromOutbox(outbox), outbox.Body)
	} else if outbox.MentionOwner {
		body, mentions := renderOutboxMentionHTML(outbox, b.user)
		msg, err = b.graph.SendHTMLWithMentionsWithoutRateLimitRetry(ctx, outbox.TeamsChatID, body, mentions)
	} else if user, ok := outboxMentionUser(outbox); ok {
		body, mentions := renderOutboxUserMentionHTML(outbox, user)
		msg, err = b.graph.SendHTMLWithMentionsWithoutRateLimitRetry(ctx, outbox.TeamsChatID, body, mentions)
	} else {
		msg, err = b.graph.SendHTMLWithoutRateLimitRetry(ctx, outbox.TeamsChatID, renderOutboxHTML(outbox))
	}
	if err != nil {
		_, _ = b.store.MarkOutboxSendError(context.Background(), outbox.ID, err.Error())
		if opts.RecordRateLimit {
			b.recordGraphRateLimit(context.Background(), outbox.TeamsChatID, outbox.ID, err)
		}
		return err
	}
	b.markRegistrySent(outbox.TeamsChatID, msg.ID)
	b.rememberAcceptedOutbox(outbox.ID, msg.ID)
	if err := b.recordGlobalOutboundMessage(ctx, outbox, msg); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams global outbound ledger record error: %v\n", err)
	}
	if _, err := b.store.MarkOutboxAccepted(ctx, outbox.ID, msg.ID); err != nil {
		return err
	}
	sent, err := b.store.MarkOutboxSent(ctx, outbox.ID, msg.ID)
	if err == nil {
		b.forgetAcceptedOutbox(outbox.ID)
		b.recordSentOutboxSideEffect(ctx, sent, msg, opts)
	}
	return err
}

func (b *Bridge) recoverAcceptedOutboxFromGraph(ctx context.Context, outbox teamstore.OutboxMessage, opts outboxSendOptions) (bool, error) {
	if b == nil || b.store == nil || b.readClient() == nil || strings.TrimSpace(outbox.ID) == "" || strings.TrimSpace(outbox.TeamsChatID) == "" {
		return false, nil
	}
	if outbox.Status != teamstore.OutboxStatusSending || strings.TrimSpace(outbox.LastSendError) != "" {
		return false, nil
	}
	if strings.TrimSpace(outbox.TeamsMessageID) != "" || outbox.LastSendAttempt.IsZero() {
		return false, nil
	}
	if blockedUntil, ok := b.chatReadBlockedUntil(ctx, outbox.TeamsChatID); ok {
		return true, outboxDeliveryDeferredError{ChatID: outbox.TeamsChatID, Until: blockedUntil}
	}
	messages, err := b.readClient().ListMessagesWithoutRateLimitRetry(ctx, outbox.TeamsChatID, outboxRecoveryMessageTop)
	if err != nil {
		return true, err
	}
	minActivity := outbox.LastSendAttempt.Add(-2 * time.Minute)
	for _, msg := range messages {
		if strings.TrimSpace(msg.ID) == "" || !messageAuthoredByCurrentUser(msg, b.user) {
			continue
		}
		activity := chatMessageActivityTime(msg)
		if !activity.IsZero() && activity.Before(minActivity) {
			continue
		}
		incomingKey := comparableTeamsPlainText(PlainTextFromTeamsHTML(msg.Body.Content))
		if !outboxRenderedPlainTextMatches(outbox, b.user, incomingKey) {
			continue
		}
		b.markRegistrySent(outbox.TeamsChatID, msg.ID)
		if _, err := b.store.MarkOutboxAccepted(ctx, outbox.ID, msg.ID); err != nil {
			return true, err
		}
		sent, err := b.store.MarkOutboxSent(ctx, outbox.ID, msg.ID)
		if err == nil {
			b.forgetAcceptedOutbox(outbox.ID)
			b.recordSentOutboxSideEffect(ctx, sent, msg, opts)
		}
		return true, err
	}
	return false, nil
}

func (b *Bridge) recordSentOutboxSideEffect(ctx context.Context, outbox teamstore.OutboxMessage, msg ChatMessage, opts outboxSendOptions) {
	if err := b.recordGlobalOutboundMessage(ctx, outbox, msg); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams global outbound ledger record error: %v\n", err)
	}
	if err := b.recordOutboxMessageProvenance(ctx, outbox, msg); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams message provenance record error: %v\n", err)
	}
	if opts.SentSideEffects != nil {
		*opts.SentSideEffects = append(*opts.SentSideEffects, sentOutboxSideEffect{Outbox: outbox, TeamsMessage: msg})
		return
	}
	b.handleSentOutboxSideEffects(ctx, outbox, msg)
}

func (b *Bridge) recordOutboxMessageProvenance(ctx context.Context, outbox teamstore.OutboxMessage, msg ChatMessage) error {
	if b == nil || b.store == nil {
		return nil
	}
	messageID := strings.TrimSpace(firstNonEmptyString(msg.ID, outbox.TeamsMessageID))
	if strings.TrimSpace(outbox.TeamsChatID) == "" || messageID == "" {
		return nil
	}
	_, err := b.store.RecordMessageProvenance(ctx, teamstore.MessageProvenanceRecord{
		TeamsChatID:    outbox.TeamsChatID,
		TeamsMessageID: messageID,
		Origin:         teamstore.MessageOriginHelperOutbox,
		SessionID:      outbox.SessionID,
		TurnID:         outbox.TurnID,
		OutboxID:       outbox.ID,
		Kind:           outbox.Kind,
		RenderedHash:   outbox.RenderedHash,
		CreatedAt:      outbox.CreatedAt,
		UpdatedAt:      firstNonZeroTime(outbox.SentAt, outbox.UpdatedAt, outbox.CreatedAt),
	})
	return err
}

func (b *Bridge) handleSentOutboxSideEffects(ctx context.Context, outbox teamstore.OutboxMessage, msg ChatMessage) {
	b.queueWorkflowNotificationForSentOutbox(ctx, outbox)
	b.markChatUnreadForSentAnswer(ctx, outbox, msg)
}

func (b *Bridge) markChatUnreadForSentAnswer(ctx context.Context, outbox teamstore.OutboxMessage, msg ChatMessage) {
	if b == nil || !b.markAnswerChatsUnread || b.graph == nil || !isCompletionNotificationPart(outbox) {
		return
	}
	readAt := parseGraphTime(msg.CreatedDateTime)
	if !readAt.IsZero() {
		readAt = readAt.Add(-time.Millisecond)
	}
	if err := b.graph.MarkChatUnreadForUserWithoutRateLimitRetry(ctx, outbox.TeamsChatID, b.user, readAt); err != nil {
		if b.out != nil && !b.markAnswerUnreadWarned {
			_, _ = fmt.Fprintf(b.out, "Teams mark-unread after Codex answer failed: %v\n", err)
			b.markAnswerUnreadWarned = true
		}
	}
}

func (b *Bridge) uploadQueuedOutboxAttachment(ctx context.Context, outbox teamstore.OutboxMessage) (DriveItem, error) {
	graph, err := b.fileWriteGraph()
	if err != nil {
		return DriveItem{}, fmt.Errorf("Teams file upload setup failed: %w", err)
	}
	file, opts, err := outboundAttachmentFileFromOutbox(outbox)
	if err != nil {
		return DriveItem{}, err
	}
	uploadFolder := strings.TrimSpace(opts.UploadFolder)
	if uploadFolder == "" {
		uploadFolder = defaultOutboundUploadFolder
	}
	item, err := graph.UploadSmallDriveItemWithoutRateLimitRetry(ctx, uploadFolder, file.UploadName, file.Bytes, file.ContentType)
	if err != nil {
		return DriveItem{}, err
	}
	meta, err := graph.GetDriveItemMetadataWithoutRateLimitRetry(ctx, item.ID)
	if err != nil {
		return DriveItem{}, err
	}
	return meta, nil
}

func (b *Bridge) refreshOutboxDriveItemMetadata(ctx context.Context, outbox teamstore.OutboxMessage) (DriveItem, error) {
	graph, err := b.fileWriteGraph()
	if err != nil {
		return DriveItem{}, fmt.Errorf("Teams file attachment metadata refresh failed: %w", err)
	}
	item, err := graph.GetDriveItemMetadataWithoutRateLimitRetry(ctx, strings.TrimSpace(outbox.DriveItemID))
	if err != nil {
		return DriveItem{}, fmt.Errorf("Teams file attachment metadata refresh failed: %w", err)
	}
	if driveItemAttachmentID(item) == "" {
		return DriveItem{}, fmt.Errorf("Teams file attachment metadata refresh did not return an eTag GUID for drive item %q", strings.TrimSpace(outbox.DriveItemID))
	}
	return item, nil
}

func outboundAttachmentFileFromOutbox(outbox teamstore.OutboxMessage) (OutboundAttachmentFile, OutboundAttachmentOptions, error) {
	path := strings.TrimSpace(outbox.AttachmentPath)
	if path == "" {
		return OutboundAttachmentFile{}, OutboundAttachmentOptions{}, fmt.Errorf("queued attachment is missing a local file path")
	}
	root, err := DefaultOutboundRoot()
	if err != nil {
		return OutboundAttachmentFile{}, OutboundAttachmentOptions{}, err
	}
	data, size, err := readOutboundAttachmentFile(path, root, false, maxOutboundAttachmentBytes)
	if err != nil {
		return OutboundAttachmentFile{}, OutboundAttachmentOptions{}, err
	}
	if outbox.AttachmentSize > 0 && outbox.AttachmentSize != size {
		return OutboundAttachmentFile{}, OutboundAttachmentOptions{}, fmt.Errorf("queued attachment size changed from %d to %d bytes", outbox.AttachmentSize, size)
	}
	hash := attachmentContentHash(data)
	if outbox.AttachmentHash != "" && outbox.AttachmentHash != hash {
		return OutboundAttachmentFile{}, OutboundAttachmentOptions{}, fmt.Errorf("queued attachment content changed since it was accepted")
	}
	name := safeAttachmentName(firstNonEmptyString(outbox.AttachmentName, filepath.Base(path)))
	if name == "" || strings.HasPrefix(name, ".") {
		return OutboundAttachmentFile{}, OutboundAttachmentOptions{}, fmt.Errorf("queued attachment has unsafe file name")
	}
	uploadName := strings.TrimSpace(outbox.AttachmentUploadName)
	if uploadName == "" {
		uploadName = outboundUploadName(name, time.Now())
	}
	if !safeDrivePathSegment(uploadName) {
		return OutboundAttachmentFile{}, OutboundAttachmentOptions{}, fmt.Errorf("queued attachment has unsafe upload name")
	}
	contentType := strings.TrimSpace(outbox.AttachmentContentType)
	if contentType == "" {
		contentType = outboundContentType(filepath.Ext(name))
	}
	uploadFolder := strings.TrimSpace(outbox.AttachmentUploadFolder)
	if uploadFolder == "" {
		uploadFolder = defaultOutboundUploadFolder
	}
	return OutboundAttachmentFile{
		Path:        path,
		Name:        name,
		UploadName:  uploadName,
		ContentType: contentType,
		Bytes:       data,
		Size:        size,
	}, OutboundAttachmentOptions{UploadFolder: uploadFolder}, nil
}

func (b *Bridge) sendToChat(ctx context.Context, chatID string, text string) error {
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderHelper,
		Text:    text,
	}, TeamsRenderOptions{
		HardLimitBytes:   safeTeamsHTMLContentBytes,
		TargetLimitBytes: teamsChunkHTMLContentBytes,
	})
	queued := make([]teamstore.OutboxMessage, 0, len(chunks))
	for i, chunk := range chunks {
		body := chunk.Text
		kind := "helper"
		if len(chunks) > 1 {
			kind = fmt.Sprintf("helper-%03d", i+1)
		}
		msg := teamstore.OutboxMessage{
			ID:            directOutboxID(chatID, kind, body),
			TeamsChatID:   chatID,
			Kind:          kind,
			Body:          body,
			PartIndex:     chunk.PartIndex,
			PartCount:     chunk.PartCount,
			RenderedBytes: chunk.ByteLength,
		}
		queuedMsg, err := b.queueOutbox(ctx, msg)
		if err != nil {
			return err
		}
		queued = append(queued, queuedMsg)
	}
	if len(queued) == 0 {
		return nil
	}
	return b.flushPendingOutboxForChat(ctx, chatID)
}

func (b *Bridge) sendStatsToChat(ctx context.Context, chatID string, text string) error {
	body := renderCodexTokenStatsHTML(text)
	msg := teamstore.OutboxMessage{
		ID:             directOutboxID(chatID, "helper-stats", body),
		TeamsChatID:    chatID,
		Kind:           "helper-stats",
		Body:           body,
		PartIndex:      1,
		PartCount:      1,
		SourceTextHash: normalizedTextHash(text),
		RenderedBytes:  len(body),
	}
	queued, err := b.queueOutbox(ctx, msg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(queued.ID) == "" {
		return nil
	}
	return b.flushPendingOutboxForChat(ctx, chatID)
}

func (b *Bridge) sendSingleToChat(ctx context.Context, chatID string, text string) error {
	return b.sendToChat(ctx, chatID, text)
}

func (b *Bridge) sendLongToChat(ctx context.Context, chatID string, text string) error {
	return b.sendToChat(ctx, chatID, text)
}

func (b *Bridge) discoverDashboardProjects(ctx context.Context) ([]codexhistory.Project, error) {
	if b == nil {
		projects, err := discoverCodexProjectsForTeams(ctx, "")
		if err != nil {
			return nil, err
		}
		return codexhistory.FilterUserVisibleProjects(projects), nil
	}
	now := time.Now()
	b.dashboardProjectsMu.Lock()
	if !b.dashboardProjectsCachedAt.IsZero() && now.Sub(b.dashboardProjectsCachedAt) < dashboardProjectsCacheTTL {
		projects := cloneCodexProjects(b.dashboardProjectsCache)
		b.dashboardProjectsMu.Unlock()
		return projects, nil
	}
	b.dashboardProjectsMu.Unlock()

	projects, err := discoverCodexProjectsForTeams(ctx, "")
	if err != nil {
		return nil, err
	}
	projects = codexhistory.FilterUserVisibleProjects(projects)
	cached := cloneCodexProjects(projects)
	b.dashboardProjectsMu.Lock()
	b.dashboardProjectsCache = cached
	b.dashboardProjectsCachedAt = now
	b.dashboardProjectsMu.Unlock()
	return cloneCodexProjects(cached), nil
}

func cloneCodexProjects(projects []codexhistory.Project) []codexhistory.Project {
	if len(projects) == 0 {
		return nil
	}
	out := make([]codexhistory.Project, len(projects))
	for i, project := range projects {
		out[i] = project
		if len(project.Sessions) > 0 {
			out[i].Sessions = append([]codexhistory.Session(nil), project.Sessions...)
			for j := range out[i].Sessions {
				if len(out[i].Sessions[j].Subagents) > 0 {
					out[i].Sessions[j].Subagents = append([]codexhistory.SubagentSession(nil), out[i].Sessions[j].Subagents...)
				}
			}
		}
	}
	return out
}

func renderOutboxHTML(outbox teamstore.OutboxMessage) string {
	if isChatMovedOutboxKind(outbox.Kind) {
		rendered, _ := renderChatMovedOutboxHTML(outbox, User{}, false)
		return rendered
	}
	if isWorkflowFallbackOutboxKind(outbox.Kind) {
		return renderWorkflowFallbackOutboxHTML(outbox, "")
	}
	if isHelperStatsOutboxKind(outbox.Kind) {
		return outbox.Body
	}
	if strings.EqualFold(strings.TrimSpace(outbox.Kind), "freeze-notice") {
		return outbox.Body
	}
	if isTranscriptImportBatchOutboxKind(outbox.Kind) {
		return outbox.Body
	}
	if isCompletionNotificationOutbox(outbox) && renderKindForOutbox(outbox.Kind) == TeamsRenderAssistant {
		rendered := renderFinalOutboxBodyHTML(outbox)
		if isCompletionNotificationPart(outbox) {
			rendered += `<p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>`
		}
		return rendered
	}
	rendered := renderTeamsHTMLPart(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    renderKindForOutbox(outbox.Kind),
		Text:    outbox.Body,
	}, normalizedPartIndex(outbox), normalizedPartCount(outbox))
	return rendered
}

func renderOutboxMentionHTML(outbox teamstore.OutboxMessage, owner User) (string, []ChatMention) {
	return renderOutboxMentionHTMLWithFallback(outbox, owner, "owner")
}

func renderOutboxUserMentionHTML(outbox teamstore.OutboxMessage, user User) (string, []ChatMention) {
	return renderOutboxMentionHTMLWithFallback(outbox, user, "user")
}

func renderOutboxMentionHTMLWithFallback(outbox teamstore.OutboxMessage, owner User, fallback string) (string, []ChatMention) {
	if isChatMovedOutboxKind(outbox.Kind) {
		return renderChatMovedOutboxHTML(outbox, owner, true)
	}
	mentionText := strings.TrimSpace(firstNonEmptyString(owner.DisplayName, owner.UserPrincipalName, fallback))
	mention := `<at id="0">` + html.EscapeString(mentionText) + `</at>`
	if isWorkflowFallbackOutboxKind(outbox.Kind) {
		return renderWorkflowFallbackOutboxHTML(outbox, mention), []ChatMention{{
			ID:   0,
			Text: mentionText,
			User: owner,
		}}
	}
	label := teamsRenderLabel(renderKindForOutbox(outbox.Kind), normalizedPartIndex(outbox), normalizedPartCount(outbox))
	body := normalizeTeamsRenderTextForKind(renderKindForOutbox(outbox.Kind), outbox.Body)
	rendered := renderTeamsHTMLParagraphs(label, body, mention)
	if isCompletionNotificationOutbox(outbox) && renderKindForOutbox(outbox.Kind) == TeamsRenderAssistant {
		rendered = renderFinalOutboxBodyHTML(outbox)
		if isCompletionNotificationPart(outbox) {
			rendered += `<p><strong>🔧 Helper:</strong> ✅ Codex finished responding. ` + mention + `</p>`
		}
	}
	return rendered, []ChatMention{{
		ID:   0,
		Text: mentionText,
		User: owner,
	}}
}

func isWorkflowFallbackOutboxKind(kind string) bool {
	return strings.EqualFold(strings.TrimSpace(kind), "workflow-fallback")
}

func isHelperStatsOutboxKind(kind string) bool {
	return strings.EqualFold(strings.TrimSpace(kind), "helper-stats")
}

func renderWorkflowFallbackOutboxHTML(outbox teamstore.OutboxMessage, firstPrefixHTML string) string {
	label := teamsRenderLabel(TeamsRenderHelper, normalizedPartIndex(outbox), normalizedPartCount(outbox))
	var out strings.Builder
	out.WriteString("<p><strong>")
	out.WriteString(html.EscapeString(label))
	out.WriteString(":</strong>")
	if strings.TrimSpace(firstPrefixHTML) != "" {
		out.WriteString(" ")
		out.WriteString(firstPrefixHTML)
	}
	out.WriteString("</p>")
	body := strings.TrimSpace(outbox.Body)
	if body == "" {
		return out.String()
	}
	if workflowFallbackBodyLooksHTML(body) {
		out.WriteString(body)
		return out.String()
	}
	for _, block := range parseTeamsMarkdownBlocks(body) {
		out.WriteString(renderTeamsMarkdownBlockHTML(block))
	}
	return out.String()
}

func workflowFallbackBodyLooksHTML(body string) bool {
	body = strings.TrimSpace(body)
	return strings.HasPrefix(body, "<p><strong>")
}

func outboxMentionUser(outbox teamstore.OutboxMessage) (User, bool) {
	id := strings.TrimSpace(outbox.MentionUserID)
	if id == "" {
		return User{}, false
	}
	return User{
		ID:          id,
		DisplayName: strings.TrimSpace(outbox.MentionUserName),
	}, true
}

func isChatMovedOutboxKind(kind string) bool {
	return strings.EqualFold(strings.TrimSpace(kind), "chat-moved")
}

func renderChatMovedOutboxHTML(outbox teamstore.OutboxMessage, owner User, includeMention bool) (string, []ChatMention) {
	target, href := parseChatMovedNoticeBody(outbox.Body)
	if target == "" {
		target = "the new chat"
	}
	label := teamsRenderLabel(TeamsRenderHelper, normalizedPartIndex(outbox), normalizedPartCount(outbox))
	mentionHTML := ""
	var mentions []ChatMention
	if includeMention && strings.TrimSpace(owner.ID) != "" {
		mentionText := strings.TrimSpace(firstNonEmptyString(owner.DisplayName, owner.UserPrincipalName, "owner"))
		if mentionText == "" {
			mentionText = "owner"
		}
		mentionHTML = ` <at id="0">` + html.EscapeString(mentionText) + `</at>`
		mentions = []ChatMention{{
			ID:   0,
			Text: mentionText,
			User: owner,
		}}
	}
	linkText := "Open " + target
	linkHTML := html.EscapeString(linkText)
	if safeHref, ok := safeTeamsMarkdownURL(href); ok && teamsMarkdownURLIsHTTP(safeHref) {
		linkHTML = `<a href="` + html.EscapeString(safeHref) + `">` + html.EscapeString(linkText) + `</a>`
	} else if strings.TrimSpace(href) != "" {
		linkHTML = html.EscapeString(strings.TrimSpace(href))
	}
	var out strings.Builder
	out.WriteString("<p><strong>")
	out.WriteString(html.EscapeString(label))
	out.WriteString(":</strong>")
	out.WriteString(mentionHTML)
	out.WriteString(" 🔁 <strong>This chat moved</strong></p>")
	out.WriteString("<p><strong>Open ")
	out.WriteString(html.EscapeString(target))
	out.WriteString(":</strong><br>")
	out.WriteString(linkHTML)
	out.WriteString("</p>")
	out.WriteString("<p>Messages here may not be handled after the switch.</p>")
	return out.String(), mentions
}

func parseChatMovedNoticeBody(body string) (target string, href string) {
	lines := strings.Split(normalizeTeamsRenderText(body), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)
		if strings.HasPrefix(lower, "open ") && strings.HasSuffix(trimmed, ":") {
			target = strings.TrimSpace(strings.TrimSuffix(trimmed[len("Open "):], ":"))
			continue
		}
		if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
			href = trimmed
		}
	}
	return target, href
}

func renderFinalOutboxBodyHTML(outbox teamstore.OutboxMessage) string {
	label := teamsRenderLabel(TeamsRenderAssistant, normalizedPartIndex(outbox), normalizedPartCount(outbox))
	body := normalizeTeamsRenderTextForKind(TeamsRenderAssistant, StripOAIMemoryCitationBlocks(outbox.Body))
	return renderTeamsHTMLCodexMarkdownAfterLabelBreak(label, body)
}

func isFinalOutboxKind(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	return kind == "final" || strings.HasPrefix(kind, "final-")
}

func isFinalOutboxCompletionPart(outbox teamstore.OutboxMessage) bool {
	if !isFinalOutboxKind(outbox.Kind) {
		return false
	}
	return normalizedPartIndex(outbox) >= normalizedPartCount(outbox)
}

func isCompletionNotificationOutbox(outbox teamstore.OutboxMessage) bool {
	return isCompletionNotificationKind(outbox.Kind, outbox.NotificationKind)
}

func isCompletionNotificationKind(kind string, notificationKind string) bool {
	return isFinalOutboxKind(kind) || strings.EqualFold(strings.TrimSpace(notificationKind), "turn_completed")
}

func shouldMentionOwnerOnLastOutboxPart(kind string, notificationKind string) bool {
	if isCompletionNotificationKind(kind, notificationKind) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(notificationKind), "needs_attention") {
		return true
	}
	return workflowOutboxNeedsAttention(strings.ToLower(strings.TrimSpace(kind)))
}

func isCompletionNotificationPart(outbox teamstore.OutboxMessage) bool {
	if !isCompletionNotificationOutbox(outbox) {
		return false
	}
	return normalizedPartIndex(outbox) >= normalizedPartCount(outbox)
}

func normalizedPartIndex(outbox teamstore.OutboxMessage) int {
	if outbox.PartIndex > 0 {
		return outbox.PartIndex
	}
	return 1
}

func normalizedPartCount(outbox teamstore.OutboxMessage) int {
	if outbox.PartCount > 0 {
		return outbox.PartCount
	}
	return 1
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Now()
}

func renderKindForOutbox(kind string) TeamsRenderKind {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch {
	case kind == "queued-status":
		return TeamsRenderHelper
	case kind == "final" || strings.HasPrefix(kind, "final-") || strings.Contains(kind, "assistant"):
		return TeamsRenderAssistant
	case strings.Contains(kind, "compact"):
		return TeamsRenderStatus
	case strings.Contains(kind, "progress") || strings.Contains(kind, "status"):
		return TeamsRenderProgress
	case strings.Contains(kind, "command"):
		return TeamsRenderCommand
	case strings.Contains(kind, "user"):
		return TeamsRenderUser
	case kind == "error" || strings.Contains(kind, "interrupted") || strings.Contains(kind, "failed") || strings.Contains(kind, "tool") || strings.Contains(kind, "artifact"):
		return TeamsRenderStatus
	case kind == "control" || strings.Contains(kind, "ack") || strings.Contains(kind, "helper"):
		return TeamsRenderHelper
	default:
		return TeamsRenderHelper
	}
}

func isTranscriptImportBatchOutboxKind(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	return strings.HasPrefix(kind, "import-batch-") || strings.HasPrefix(kind, "sync-batch-")
}

func shouldSuppressCodexCommandOutbox(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	return strings.HasPrefix(kind, "codex-command-")
}

func directOutboxID(chatID string, kind string, body string) string {
	sum := sha256.Sum256([]byte(chatID + "\x00" + kind + "\x00" + body + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	return "outbox:direct:" + hex.EncodeToString(sum[:])
}

func normalizedTextHash(text string) string {
	text = strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
	if text == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

type outboxDeliveryDeferredError struct {
	ChatID string
	Until  time.Time
}

func (e outboxDeliveryDeferredError) Error() string {
	return fmt.Sprintf("Teams chat is rate-limited until %s", e.Until.Format(time.RFC3339Nano))
}

func isOutboxDeliveryDeferred(err error) bool {
	var deferred outboxDeliveryDeferredError
	return errors.As(err, &deferred) || isGraphRateLimitError(err)
}

func isGraphRateLimitError(err error) bool {
	var graphErr *GraphStatusError
	return errors.As(err, &graphErr) && graphErr.StatusCode == 429
}

func (b *Bridge) chatBlockedUntil(ctx context.Context, chatID string) (time.Time, bool) {
	if b.store == nil || strings.TrimSpace(chatID) == "" {
		return time.Time{}, false
	}
	limit, ok, err := b.store.ChatRateLimit(ctx, chatID)
	if err != nil || !ok || limit.BlockedUntil.IsZero() {
		return time.Time{}, false
	}
	if time.Now().Before(limit.BlockedUntil) {
		return limit.BlockedUntil, true
	}
	_ = b.store.ClearChatRateLimit(context.Background(), chatID)
	return time.Time{}, false
}

func (b *Bridge) chatReadBlockedUntil(ctx context.Context, chatID string) (time.Time, bool) {
	if b.store == nil || strings.TrimSpace(chatID) == "" {
		return time.Time{}, false
	}
	poll, ok, err := b.store.ChatPoll(ctx, chatID)
	if err != nil || !ok || poll.BlockedUntil.IsZero() {
		return time.Time{}, false
	}
	if time.Now().Before(poll.BlockedUntil) {
		return poll.BlockedUntil, true
	}
	return time.Time{}, false
}

func (b *Bridge) recordGraphReadRateLimit(ctx context.Context, chatID string, err error) {
	if b.store == nil || strings.TrimSpace(chatID) == "" || !isGraphRateLimitError(err) {
		return
	}
	poll, _, pollErr := b.store.ChatPoll(ctx, chatID)
	if pollErr != nil {
		poll = teamstore.ChatPollState{ChatID: chatID}
	}
	_ = b.store.RecordChatPollErrorWithBlock(ctx, chatID, err.Error(), inboundPollBlockedUntil(poll, err, time.Now()))
}

func (b *Bridge) recordGraphRateLimit(ctx context.Context, chatID string, outboxID string, err error) {
	if b.store == nil || strings.TrimSpace(chatID) == "" {
		return
	}
	var graphErr *GraphStatusError
	if !errors.As(err, &graphErr) || graphErr.StatusCode != 429 {
		return
	}
	blockedUntil := time.Now().Add(graphErr.RetryAfter)
	if graphErr.RetryAfter <= 0 {
		blockedUntil = time.Now().Add(30 * time.Second)
	}
	_, _ = b.store.SetChatRateLimit(ctx, chatID, blockedUntil, graphErr.Error())
	if outboxID != "" {
		_ = b.store.Update(ctx, func(state *teamstore.State) error {
			limit := state.ChatRateLimits[chatID]
			limit.ChatID = chatID
			limit.BlockedUntil = blockedUntil
			limit.Reason = graphErr.Error()
			limit.PoisonOutboxID = outboxID
			limit.UpdatedAt = time.Now()
			state.ChatRateLimits[chatID] = limit
			return nil
		})
	}
}

func (b *Bridge) formatSessionList() string {
	active := b.reg.ActiveSessions()
	closedCount := 0
	for _, session := range b.reg.Sessions {
		if !isActiveSessionStatus(session.Status) {
			closedCount++
		}
	}
	if len(active) == 0 {
		if closedCount > 0 {
			return fmt.Sprintf("Control status: no active linked work chats. %d closed work chat(s) are hidden because the helper no longer polls them.\n\n%s\n\nSend `projects` to choose a workspace, `new <directory>` to create a Work chat, or `sessions` then `continue <number>` to import an existing local Codex session.", closedCount, teamsASRStatusLine(b.asrTranscriber))
		}
		return "Control status: no linked work chats yet.\n\n" + teamsASRStatusLine(b.asrTranscriber) + "\n\nNext: send `projects` to choose a workspace, or `new <directory>` to create a Work chat."
	}
	lines := []string{"## Active Work chats"}
	for _, session := range active {
		meta := []string{session.ID, session.ChatURL}
		if label := modelProfileDisplayName(session.ModelProfile); label != "default" {
			meta = append(meta, "Model profile: "+label)
		}
		lines = append(lines, fmt.Sprintf("- **%s** [%s]\n  %s", session.Topic, session.Status, strings.Join(meta, "\n  ")))
	}
	if closedCount > 0 {
		lines = append(lines, fmt.Sprintf("%d closed work chat(s) hidden. The helper no longer reads or responds in closed chats.", closedCount))
	}
	lines = append(lines, teamsASRStatusLine(b.asrTranscriber))
	lines = append(lines, "Next: open one of these Teams chats to continue work, or send `new <directory>` to create another Work chat.")
	return strings.Join(lines, "\n")
}

func (b *Bridge) formatWorkSessionStatus(session *Session) string {
	if session == nil {
		return "Work chat status: session not found."
	}
	lines := []string{
		"STATUS: Work chat",
		"Session: " + session.ID,
		"Chat: " + userFacingChatState(session),
	}
	if strings.TrimSpace(session.Cwd) != "" {
		lines = append(lines, "Folder: "+session.Cwd)
	}
	if label := modelProfileDisplayName(session.ModelProfile); label != "default" {
		lines = append(lines, "Model profile: "+label)
	}
	lines = append(lines, teamsASRStatusLine(b.asrTranscriber))
	if target := b.beaconTargetSummary(session.ID); target != "" {
		lines = append(lines, target)
	}
	var latest teamstore.Turn
	var hasLatest bool
	pendingOutbox := 0
	lastOutboxError := ""
	if b.store != nil {
		if state, err := b.store.Load(context.Background()); err == nil {
			if durable, ok := state.Sessions[session.ID]; ok {
				if durable.LatestTurnID != "" {
					if turn, ok := state.Turns[durable.LatestTurnID]; ok {
						latest = turn
						hasLatest = true
					}
				}
			}
			if !hasLatest {
				for _, turn := range state.Turns {
					if turn.SessionID != session.ID {
						continue
					}
					if !hasLatest || turn.CreatedAt.After(latest.CreatedAt) || turn.UpdatedAt.After(latest.UpdatedAt) {
						latest = turn
						hasLatest = true
					}
				}
			}
			for _, msg := range state.OutboxMessages {
				if msg.SessionID != session.ID && msg.TeamsChatID != session.ChatID {
					continue
				}
				if msg.Status != teamstore.OutboxStatusSent {
					pendingOutbox++
					if lastOutboxError == "" && strings.TrimSpace(msg.LastSendError) != "" {
						lastOutboxError = msg.LastSendError
					}
				}
			}
		}
	}
	if hasLatest {
		lines = append(lines, "Codex status: "+userFacingCodexActivity(latest.Status))
		lines = append(lines, "Last request: "+userFacingTurnStatus(latest.Status))
		if strings.TrimSpace(latest.FailureMessage) != "" {
			lines = append(lines, "Latest error: "+userFacingFailureMessage(latest.FailureMessage))
		}
		switch latest.Status {
		case teamstore.TurnStatusQueued:
			lines = append(lines, "Queued request: waiting for local Codex history or the current Codex run to clear. Send `helper cancel last` if you want to drop the queued request.")
		case teamstore.TurnStatusRunning:
			lines = append(lines, "Running request: Codex is currently working. If no status appears for a while, the helper will send a waiting update.")
		case teamstore.TurnStatusFailed, teamstore.TurnStatusInterrupted:
			lines = append(lines, "Retry: check recent messages and changed files first. `helper retry last` asks Codex to run the same Teams request again in this same session, so it may repeat file edits or terminal commands.")
		}
	} else {
		lines = append(lines, "Codex status: will start when you send a task")
		lines = append(lines, "Last request: none")
	}
	if pendingOutbox > 0 {
		lines = append(lines, fmt.Sprintf("Messages waiting to send: %d", pendingOutbox))
		if lastOutboxError != "" {
			lines = append(lines, "Last send error: "+lastOutboxError)
		}
	}
	if firstNonEmptyString(session.Status, "active") == string(teamstore.SessionStatusClosed) {
		lines = append(lines, "Next: this chat is closed. Use the 🏠 control chat to open or create another work chat.")
	} else if hasLatest && latest.Status == teamstore.TurnStatusQueued {
		lines = append(lines, "Next: wait for the queued request, or send `helper cancel last` to drop it.")
	} else if hasLatest && latest.Status == teamstore.TurnStatusRunning {
		lines = append(lines, "Next: wait for Codex to finish. Send a new task only after the final answer if you want strict ordering.")
	} else if hasLatest && (latest.Status == teamstore.TurnStatusFailed || latest.Status == teamstore.TurnStatusInterrupted) {
		lines = append(lines, "Next: send a new task to start fresh, or `helper retry last` to retry the interrupted request in this session.")
	} else {
		lines = append(lines, "Next: send a task message here to start a Codex run, or `helper help` for commands.")
	}
	return strings.Join(lines, "\n")
}

func userFacingChatState(session *Session) string {
	if session == nil {
		return "unknown"
	}
	switch firstNonEmptyString(session.Status, "active") {
	case string(teamstore.SessionStatusClosed):
		return "closed"
	case "active":
		return "listening"
	default:
		return firstNonEmptyString(session.Status, "unknown")
	}
}

func userFacingCodexActivity(status teamstore.TurnStatus) string {
	switch status {
	case teamstore.TurnStatusQueued, teamstore.TurnStatusRunning:
		return "running"
	case teamstore.TurnStatusCompleted, teamstore.TurnStatusFailed, teamstore.TurnStatusInterrupted:
		return "idle"
	default:
		return firstNonEmptyString(string(status), "unknown")
	}
}

func userFacingTurnStatus(status teamstore.TurnStatus) string {
	switch status {
	case teamstore.TurnStatusQueued:
		return "queued"
	case teamstore.TurnStatusRunning:
		return "running"
	case teamstore.TurnStatusCompleted:
		return "completed"
	case teamstore.TurnStatusFailed:
		return "failed"
	case teamstore.TurnStatusInterrupted:
		return "interrupted"
	default:
		return firstNonEmptyString(string(status), "unknown")
	}
}

func userFacingFailureMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	if message == "context canceled" {
		return "helper lost its execution context before it could verify the Codex result"
	}
	return message
}

func (b *Bridge) formatOpenSession(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "usage: `open <number>`"
	}
	session := b.reg.SessionByID(sessionID)
	if session == nil {
		session = b.reg.SessionByCodexThreadID(sessionID)
	}
	if session == nil {
		return "session not found: " + sessionID
	}
	return b.formatOpenSessionMessage(session, false)
}

func (b *Bridge) formatOpenSessionMessage(session *Session, resumed bool) string {
	if session == nil {
		return "session not found"
	}
	var lines []string
	lines = append(lines, fmt.Sprintf("%s [%s] %s", session.ID, session.Status, session.Topic))
	if session.ChatURL != "" {
		lines = append(lines, session.ChatURL)
	}
	if isActiveSessionStatus(session.Status) {
		if resumed {
			lines = append(lines, "This parked work chat was resumed. Next: open this Teams work chat and send a message there to continue.")
		} else {
			lines = append(lines, "Next: open this Teams work chat and send a message there to continue. `open` only shows the linked chat; it does not import local history.")
		}
	} else {
		lines = append(lines, "This work chat is closed, so the helper no longer reads or responds there. Use `sessions` then `continue <number>` to continue the local Codex session in a new work chat.")
	}
	return strings.Join(lines, "\n")
}

func (b *Bridge) formatDetails(arg string) string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return b.formatSessionList()
	}
	session := b.reg.SessionByID(arg)
	if session == nil {
		session = b.reg.SessionByCodexThreadID(arg)
	}
	if session == nil {
		return "session not found: " + arg
	}
	return b.formatSessionDetails(session)
}

func (b *Bridge) formatDetailsControlTarget(ctx context.Context, target DashboardCommandTarget) (string, error) {
	if target.IsNumber {
		selection, err := b.resolveDashboardTarget(ctx, target.Number)
		if err != nil {
			return "", err
		}
		if selection.Kind != DashboardSelectionSession {
			return "", fmt.Errorf("number %d is a directory in the current list; send `project %d` to list its sessions", target.Number, target.Number)
		}
		session := b.linkedSessionForLocalSessionID(selection.SessionID)
		if session == nil {
			return b.formatLocalSessionDetails(ctx, selection), nil
		}
		return b.formatSessionDetails(session), nil
	}
	return b.formatDetails(target.Raw), nil
}

func (b *Bridge) formatLocalSessionDetails(ctx context.Context, selection DashboardSelection) string {
	if session, ok := b.dashboardSessionForSelection(ctx, selection); ok {
		return formatDashboardSessionDetails(session, selection.Number)
	}
	lines := []string{"Local Codex session"}
	if selection.Number > 0 {
		lines = append(lines, fmt.Sprintf("List number: %d", selection.Number))
	}
	lines = append(lines, "Codex session ID: "+selection.SessionID)
	lines = append(lines, "Teams chat: not published")
	if selection.Number > 0 {
		lines = append(lines, fmt.Sprintf("Next: send `continue %d` to create a work chat and import its history.", selection.Number))
	}
	return strings.Join(lines, "\n")
}

func (b *Bridge) dashboardSessionForSelection(ctx context.Context, selection DashboardSelection) (DashboardSession, bool) {
	projects, err := b.discoverDashboardProjects(ctx)
	if err != nil {
		return DashboardSession{}, false
	}
	dashboard := BuildControlDashboard(b.previousControlDashboard(ctx), ControlDashboardInput{
		Workspaces:          dashboardWorkspacesFromProjects(projects),
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: selection.WorkspaceID,
	}, time.Now())
	for _, session := range dashboard.Sessions {
		if session.WorkspaceID == selection.WorkspaceID && session.ID == selection.SessionID {
			return session, true
		}
	}
	return DashboardSession{}, false
}

func formatDashboardSessionDetails(session DashboardSession, number int) string {
	lines := []string{"Local Codex session"}
	if number > 0 {
		lines = append(lines, fmt.Sprintf("List number: %d", number))
	}
	lines = append(lines, "Title: "+session.DisplayTitle)
	if session.Cwd != "" {
		lines = append(lines, "Working directory: "+session.Cwd)
	}
	if session.UpdatedAt.IsZero() {
		lines = append(lines, "Updated: unknown")
	} else {
		lines = append(lines, "Updated: "+session.UpdatedAt.Local().Format(time.RFC3339))
	}
	if session.ID != "" {
		lines = append(lines, "Codex session ID: "+session.ID)
	}
	if session.CodexThreadID != "" && session.CodexThreadID != session.ID {
		lines = append(lines, "Codex thread: "+session.CodexThreadID)
	}
	lines = append(lines, "Teams chat: not published")
	if number > 0 {
		lines = append(lines, fmt.Sprintf("Next: send `continue %d` to create a work chat and import its history.", number))
	}
	return strings.Join(lines, "\n")
}

func (b *Bridge) formatSessionDetails(session *Session) string {
	if session == nil {
		return "session not found."
	}
	lines := []string{
		fmt.Sprintf("Session: %s", session.ID),
		fmt.Sprintf("Chat polling: %s", userFacingChatState(session)),
		fmt.Sprintf("Title: %s", session.Topic),
	}
	if session.Cwd != "" {
		lines = append(lines, "Working directory: "+session.Cwd)
	}
	if label := modelProfileDisplayName(session.ModelProfile); label != "default" {
		lines = append(lines, "Model profile: "+label)
	}
	if session.CodexThreadID != "" {
		lines = append(lines, "Codex thread: "+session.CodexThreadID)
	}
	if target := b.beaconTargetSummary(session.ID); target != "" {
		lines = append(lines, target)
	}
	if session.ChatURL != "" {
		lines = append(lines, "Teams chat: "+session.ChatURL)
	}
	if b.store != nil {
		if state, err := b.store.Load(context.Background()); err == nil {
			if durable, ok := state.Sessions[session.ID]; ok {
				if durable.LatestTurnID != "" {
					lines = append(lines, "Latest turn: "+durable.LatestTurnID)
					if turn, ok := state.Turns[durable.LatestTurnID]; ok {
						lines = append(lines, "Codex status: "+userFacingCodexActivity(turn.Status))
					}
				}
				if durable.LatestCodexTurnID != "" {
					lines = append(lines, "Latest Codex turn: "+durable.LatestCodexTurnID)
				}
			}
		}
	}
	if !containsLinePrefix(lines, "Codex status:") {
		lines = append(lines, "Codex status: will start when you send a task")
	}
	return strings.Join(lines, "\n")
}

func containsLinePrefix(lines []string, prefix string) bool {
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func (b *Bridge) publishCodexSession(ctx context.Context, target DashboardCommandTarget) (string, error) {
	return b.publishCodexSessionWithProgress(ctx, target, nil)
}

func (b *Bridge) publishCodexSessionWithProgress(ctx context.Context, target DashboardCommandTarget, notify func(context.Context, string) error) (string, error) {
	projects, err := b.discoverDashboardProjects(ctx)
	if err != nil {
		return "", fmt.Errorf("workspace discovery failed: %w", err)
	}
	sessionID, err := b.resolvePublishTargetSessionID(ctx, target)
	if err != nil {
		return "", err
	}
	local, project, ok := findCodexSession(projects, sessionID)
	if !ok {
		return "", fmt.Errorf("Codex session not found: %s", sessionID)
	}
	return b.publishCodexSessionLocal(ctx, local, project, notify)
}

func (b *Bridge) publishCodexSessionLocal(ctx context.Context, local codexhistory.Session, project codexhistory.Project, notify func(context.Context, string) error) (string, error) {
	return b.publishCodexSessionLocalWithOptions(ctx, local, project, publishCodexSessionOptions{
		Notify:                  notify,
		ChatCreatedNotification: true,
	})
}

type publishCodexSessionOptions struct {
	Notify                          func(context.Context, string) error
	ChatCreatedNotification         bool
	ChatCreatedNoticeAfterImport    bool
	LocalSessionStartedNotification bool
	BackgroundImport                bool
}

func (b *Bridge) publishCodexSessionLocalWithOptions(ctx context.Context, local codexhistory.Session, project codexhistory.Project, opts publishCodexSessionOptions) (string, error) {
	if existing := b.reg.SessionByCodexThreadID(local.SessionID); existing != nil && isActiveSessionStatus(existing.Status) {
		if err := b.ensureDurableSession(ctx, existing); err != nil {
			return "", err
		}
		if importing, err := b.sessionTranscriptImportInProgress(ctx, existing.ID); err != nil {
			return "", err
		} else if importing {
			return fmt.Sprintf("Already published as %s: %s\n\nHistory import is still running. Wait for \"Import complete\" in the Work chat before sending a new task there.", existing.ID, existing.ChatURL), nil
		}
		hasNew, err := b.transcriptHasNewRecords(ctx, *existing, local)
		historySyncNeedsAttention := false
		if isTranscriptCheckpointNotFoundError(err) {
			historySyncNeedsAttention = true
		} else if err != nil {
			return "", fmt.Errorf("check history import for %s: %w", existing.ID, err)
		}
		importStatus := "No new local history was imported."
		if historySyncNeedsAttention {
			importStatus = transcriptCheckpointNeedsAttentionMessage()
		} else if hasNew {
			if opts.Notify != nil {
				if err := opts.Notify(ctx, publishHistoryPreparingMessage(existing.ID, existing.ChatURL, true)); err != nil {
					return "", err
				}
			}
			importOpts := transcriptImportRunOptions{}
			if opts.BackgroundImport {
				importOpts.QueueOnly = true
				importOpts.MaxBatches = transcriptImportMaxBatchesPerCycle
			}
			if err := b.importCodexTranscriptToTeamsWithOptions(ctx, *existing, local, importOpts); err != nil {
				if isTranscriptCheckpointNotFoundError(err) {
					importStatus = transcriptCheckpointNeedsAttentionMessage()
					resumed, err := b.resumeWorkChatIfParked(ctx, existing)
					if err != nil {
						return "", err
					}
					return fmt.Sprintf("Already published as %s: %s\n\n%s%s Open this Teams work chat and send a message there to continue.", existing.ID, existing.ChatURL, importStatus, publishedSessionResumeStatus(resumed)), nil
				}
				return "", fmt.Errorf("resume history import for %s: %w", existing.ID, err)
			}
			if opts.BackgroundImport {
				importStatus = "New local history import was queued."
			} else {
				importStatus = "New local history was imported."
			}
		}
		resumed, err := b.resumeWorkChatIfParked(ctx, existing)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Already published as %s: %s\n\n%s%s Open this Teams work chat and send a message there to continue.", existing.ID, existing.ChatURL, importStatus, publishedSessionResumeStatus(resumed)), nil
	}
	newSessionID := b.reg.NextSessionID()
	title := WorkChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		SessionID:    newSessionID,
		Topic:        local.DisplayTitle(),
		Cwd:          firstNonEmptyString(local.ProjectPath, project.Path),
	})
	modelSnapshot, err := b.resolveNewSessionModelProfile(ctx, "")
	if err != nil {
		return "", err
	}
	chat, err := b.createMeetingChat(ctx, title)
	if err != nil {
		return "", err
	}
	now := time.Now()
	session := Session{
		ID:            newSessionID,
		ChatID:        chat.ID,
		ChatURL:       chat.WebURL,
		Topic:         chat.Topic,
		TitleSource:   sessionTitleSourceAuto,
		Status:        "active",
		CodexThreadID: local.SessionID,
		Cwd:           firstNonEmptyString(local.ProjectPath, project.Path),
		ModelProfile:  modelSnapshot,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.reg.Sessions = append(b.reg.Sessions, session)
	if err := b.ensureDurableSession(ctx, &session); err != nil {
		return "", err
	}
	if err := b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.Sessions[session.ID]
		current.Cwd = firstNonEmptyString(local.ProjectPath, project.Path)
		current.CodexThreadID = local.SessionID
		current.UpdatedAt = now
		state.Sessions[session.ID] = current
		return nil
	}); err != nil {
		return "", err
	}
	if opts.Notify != nil {
		if err := opts.Notify(ctx, publishHistoryPreparingMessage(session.ID, session.ChatURL, false)); err != nil {
			return "", err
		}
	}
	createdNotice := workChatCreatedNotice(session)
	if !opts.ChatCreatedNoticeAfterImport {
		if opts.LocalSessionStartedNotification {
			var err error
			if opts.BackgroundImport {
				_, err = b.queueLocalSessionStartedNotice(ctx, session.ID, chat.ID)
			} else {
				err = b.sendLocalSessionStartedNotice(ctx, session.ID, chat.ID)
			}
			if err != nil {
				return "", err
			}
		} else {
			var err error
			if opts.BackgroundImport {
				_, err = b.queueChatCreatedNotice(ctx, session.ID, chat.ID, createdNotice, opts.ChatCreatedNotification)
			} else {
				err = b.sendChatCreatedNotice(ctx, session.ID, chat.ID, createdNotice, opts.ChatCreatedNotification)
			}
			if err != nil {
				return "", err
			}
		}
	}
	importOpts := transcriptImportRunOptions{}
	if opts.BackgroundImport {
		importOpts.QueueOnly = true
		importOpts.MaxBatches = transcriptImportMaxBatchesPerCycle
	}
	if err := b.importCodexTranscriptToTeamsWithOptions(ctx, session, local, importOpts); err != nil {
		if isTranscriptCheckpointNotFoundError(err) {
			return fmt.Sprintf("Published local Codex session as %s: %s\n\n%s Open this Teams work chat and send a message there to continue.", session.ID, session.ChatURL, transcriptCheckpointNeedsAttentionMessage()), nil
		}
		return "", err
	}
	if opts.ChatCreatedNoticeAfterImport {
		if opts.LocalSessionStartedNotification {
			var err error
			if opts.BackgroundImport {
				_, err = b.queueLocalSessionStartedNotice(ctx, session.ID, chat.ID)
			} else {
				err = b.sendLocalSessionStartedNotice(ctx, session.ID, chat.ID)
			}
			if err != nil {
				return "", err
			}
		} else {
			var err error
			if opts.BackgroundImport {
				_, err = b.queueChatCreatedNotice(ctx, session.ID, chat.ID, createdNotice, opts.ChatCreatedNotification)
			} else {
				err = b.sendChatCreatedNotice(ctx, session.ID, chat.ID, createdNotice, opts.ChatCreatedNotification)
			}
			if err != nil {
				return "", err
			}
		}
	}
	return fmt.Sprintf("Published local Codex session as %s: %s\n\nOpen this Teams work chat and send a message there to continue.", session.ID, session.ChatURL), nil
}

func publishedSessionResumeStatus(resumed bool) string {
	if !resumed {
		return ""
	}
	return " The parked Work chat was resumed."
}

func publishHistoryPreparingMessage(sessionID string, chatURL string, existing bool) string {
	status := "Work chat created"
	if existing {
		status = "Work chat reopened"
	}
	lines := []string{
		fmt.Sprintf("✅ %s: %s", status, strings.TrimSpace(sessionID)),
		"",
		"Open Work chat:",
		strings.TrimSpace(chatURL),
		"",
		"Preparing local Codex history now. Long sessions can take a few minutes.",
		`Wait for "Import complete" in the Work chat before sending a new task there.`,
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func (b *Bridge) resolvePublishTargetSessionID(ctx context.Context, target DashboardCommandTarget) (string, error) {
	sessionID := strings.TrimSpace(target.Raw)
	if !target.IsNumber {
		if sessionID == "" {
			return "", fmt.Errorf("usage: `continue <number-or-session-id>`")
		}
		return sessionID, nil
	}
	selection, err := b.resolveDashboardTarget(ctx, target.Number)
	if err != nil {
		return "", err
	}
	if selection.Kind != DashboardSelectionSession {
		return "", fmt.Errorf("number %d is a directory in the current list; send `project %d` first to list its sessions, then `continue <session-number>`", target.Number, target.Number)
	}
	return selection.SessionID, nil
}

type transcriptImportRunOptions struct {
	QueueOnly  bool
	MaxBatches int
}

type transcriptImportResult struct {
	LastRecordID string
	LastLine     int
	LastOffset   int64
	Stats        transcriptImportStats
	Complete     bool
}

func (b *Bridge) importCodexTranscriptToTeams(ctx context.Context, session Session, local codexhistory.Session) error {
	return b.importCodexTranscriptToTeamsWithOptions(ctx, session, local, transcriptImportRunOptions{})
}

func (b *Bridge) importCodexTranscriptToTeamsWithOptions(ctx context.Context, session Session, local codexhistory.Session, opts transcriptImportRunOptions) error {
	importTurnID := "import:" + session.ID
	if opts.QueueOnly && opts.MaxBatches > 0 {
		importTurnID = "import-bg:" + session.ID
	}
	if err := b.markTranscriptImportStartedForRun(ctx, session, local.FilePath, transcriptCheckpointID(session.ID), importTurnID, "import"); err != nil {
		return err
	}
	title := "Imported Codex session history\n\nThe messages below came from your local Codex session. Reply in this chat to continue from here.\n\nSession: " + local.DisplayTitle()
	if err := b.queueOrSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "import-title", title, outboxQueueOptions{}, opts.QueueOnly); err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		return err
	}
	result, err := b.importTranscriptRecordsToTeams(ctx, session, local.FilePath, importTurnID, "import", transcriptCheckpointID(session.ID), opts)
	if err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		if isTranscriptCheckpointNotFoundError(err) {
			_ = b.queueOrSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "import-needs-attention", transcriptCheckpointNeedsAttentionMessage(), outboxQueueOptions{}, opts.QueueOnly)
		}
		return err
	}
	if !result.Complete {
		return b.markTranscriptImportPausedAt(ctx, session, local.FilePath, result.LastRecordID, result.LastLine, result.LastOffset, transcriptCheckpointID(session.ID), importTurnID, "import")
	}
	if err := b.importSubagentMarkersToTeamsWithOptions(ctx, session, local, importTurnID, opts); err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		return err
	}
	complete := formatTranscriptImportCompleteMessage(result.Stats)
	if err := b.queueOrSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "import-complete", complete, outboxQueueOptions{}, opts.QueueOnly); err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		return err
	}
	return b.markTranscriptImportCompleteAtEOF(ctx, session, local.FilePath, result.LastRecordID, result.LastLine)
}

func (b *Bridge) publishWorkSessionHistory(ctx context.Context, session *Session) error {
	if session == nil {
		return nil
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	local, ok, err := b.localCodexSessionForTeamsSession(ctx, *session)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(local.FilePath) == "" {
		return b.sendToChat(ctx, session.ChatID, "No local Codex history file is linked to this Work chat.")
	}
	if importing, err := b.sessionTranscriptImportInProgress(ctx, session.ID); err != nil {
		return err
	} else if importing {
		return b.sendToChat(ctx, session.ChatID, "History import is already running. Wait for `Import complete` before sending another task.")
	}
	importTurnID := "publish-history:" + session.ID
	if err := b.markTranscriptImportStartedForRun(ctx, *session, local.FilePath, transcriptCheckpointID(session.ID), importTurnID, "sync"); err != nil {
		return err
	}
	result, err := b.importTranscriptRecordsToTeams(ctx, *session, local.FilePath, importTurnID, "sync", transcriptCheckpointID(session.ID), transcriptImportRunOptions{})
	if err != nil {
		_ = b.markTranscriptImportFailed(ctx, *session, local.FilePath)
		if isTranscriptCheckpointNotFoundError(err) {
			return b.sendToChat(ctx, session.ChatID, transcriptCheckpointNeedsAttentionMessage())
		}
		return b.sendToChat(ctx, session.ChatID, "History import failed: "+err.Error())
	}
	if err := b.markTranscriptImportCompleteAtEOF(ctx, *session, local.FilePath, result.LastRecordID, result.LastLine); err != nil {
		return err
	}
	body := formatTranscriptImportCompleteMessage(result.Stats)
	if result.Stats.Imported == 0 && result.Stats.SkippedBackground == 0 {
		body = "No new visible local Codex history needed to be imported. This chat is ready."
	}
	if err := b.queueAndSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "sync-complete", body); err != nil {
		return err
	}
	return b.processQueuedTurns(ctx)
}

type transcriptImportStats struct {
	Total             int
	Imported          int
	SkippedBackground int
}

func formatTranscriptImportCompleteMessage(stats transcriptImportStats) string {
	if stats.SkippedBackground <= 0 {
		return "Import complete. This chat is ready; reply here to continue the Codex session."
	}
	return fmt.Sprintf(
		"Import complete. Imported %d visible history item(s) and skipped %d background tool event(s) from history to keep this Teams chat readable. New live Codex status updates will still appear here.\n\nThis chat is ready; reply here to continue the Codex session.",
		stats.Imported,
		stats.SkippedBackground,
	)
}

func (b *Bridge) importTranscriptRecordsToTeams(ctx context.Context, session Session, filePath string, importTurnID string, kindPrefix string, checkpointID string, opts transcriptImportRunOptions) (transcriptImportResult, error) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return transcriptImportResult{Complete: true}, nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return transcriptImportResult{}, err
	}
	if checkpointID == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	var transcript Transcript
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.LastRecordID == "" {
		transcript, err = ReadSessionTranscript(filePath)
		if err != nil {
			return transcriptImportResult{}, fmt.Errorf("read local transcript: %w", err)
		}
	} else {
		for attempt := 0; attempt < 2; attempt++ {
			transcript, err = b.readLinkedTranscriptDelta(filePath, checkpoint, session.CodexThreadID, session.CodexThreadID)
			if err != nil {
				_ = b.markTranscriptImportFailedWithID(ctx, session, filePath, checkpointID)
				return transcriptImportResult{}, err
			}
			if !transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
				break
			}
			_ = b.markTranscriptImportFailedWithID(ctx, session, filePath, checkpointID)
			if checkpointID != transcriptCheckpointID(session.ID) {
				return transcriptImportResult{}, errTranscriptCheckpointNotFound
			}
			recovered, recoverErr := b.recoverFailedTranscriptCheckpoint(ctx, session, codexhistory.Session{
				SessionID:   session.CodexThreadID,
				ProjectPath: session.Cwd,
				FilePath:    filePath,
			})
			if recoverErr != nil {
				return transcriptImportResult{}, recoverErr
			}
			if !recovered {
				return transcriptImportResult{}, errTranscriptCheckpointNotFound
			}
			state, err = b.store.Load(ctx)
			if err != nil {
				return transcriptImportResult{}, err
			}
			checkpoint = state.ImportCheckpoints[checkpointID]
			if strings.TrimSpace(checkpoint.LastRecordID) == "" {
				transcript, err = ReadSessionTranscript(filePath)
				if err != nil {
					return transcriptImportResult{}, fmt.Errorf("read local transcript: %w", err)
				}
				break
			}
			if attempt == 1 {
				return transcriptImportResult{}, errTranscriptCheckpointNotFound
			}
		}
	}
	stats := transcriptImportStats{Total: len(transcript.Records)}
	dedupe := newTranscriptDedupeState()
	batcher := newTranscriptImportBatcher(b, session, filePath, importTurnID, kindPrefix, checkpointID, opts)
	var lastRecordID string
	var lastLine int
	var lastOffset int64
	for i, record := range transcript.Records {
		checkpointLine, checkpointOffset := transcriptCheckpointPositionForRecord(transcript.Records, i)
		checkpointKey := transcriptRecordCheckpointKey(record)
		if strings.TrimSpace(record.Text) == "" {
			continue
		}
		if shouldSkipImportedTranscriptRecord(record) {
			stats.SkippedBackground++
			if err := batcher.recordCheckpoint(ctx, checkpointKey, checkpointLine, checkpointOffset); err != nil {
				return transcriptImportResult{}, err
			}
			lastRecordID, lastLine, lastOffset = checkpointKey, checkpointLine, checkpointOffset
			continue
		}
		body := formatTranscriptRecordForTeams(record)
		if strings.TrimSpace(body) == "" {
			if err := batcher.recordCheckpoint(ctx, checkpointKey, checkpointLine, checkpointOffset); err != nil {
				return transcriptImportResult{}, err
			}
			lastRecordID, lastLine, lastOffset = checkpointKey, checkpointLine, checkpointOffset
			continue
		}
		if dedupe.shouldSkip(record, body) {
			if err := batcher.recordCheckpoint(ctx, checkpointKey, checkpointLine, checkpointOffset); err != nil {
				return transcriptImportResult{}, err
			}
			lastRecordID, lastLine, lastOffset = checkpointKey, checkpointLine, checkpointOffset
			continue
		}
		record.SourceLine = checkpointLine
		record.SourceOffset = checkpointOffset
		kind := transcriptRecordOutboxKind(kindPrefix, record, i+1)
		if err := batcher.add(ctx, transcriptImportBatchRecord{
			Record:        record,
			Kind:          kind,
			Body:          body,
			CheckpointKey: checkpointKey,
		}); errors.Is(err, errTranscriptImportBudgetExhausted) {
			return transcriptImportResult{
				LastRecordID: lastRecordID,
				LastLine:     lastLine,
				LastOffset:   lastOffset,
				Stats:        stats,
				Complete:     false,
			}, nil
		} else if err != nil {
			return transcriptImportResult{}, err
		}
		stats.Imported++
		lastRecordID, lastLine, lastOffset = checkpointKey, checkpointLine, checkpointOffset
	}
	if err := batcher.flush(ctx); err != nil {
		if errors.Is(err, errTranscriptImportBudgetExhausted) {
			return transcriptImportResult{
				LastRecordID: lastRecordID,
				LastLine:     lastLine,
				LastOffset:   lastOffset,
				Stats:        stats,
				Complete:     false,
			}, nil
		}
		return transcriptImportResult{}, err
	}
	if len(transcript.Records) == 0 {
		return transcriptImportResult{Stats: stats, Complete: true}, nil
	}
	last := transcript.Records[len(transcript.Records)-1]
	return transcriptImportResult{
		LastRecordID: transcriptRecordCheckpointKey(last),
		LastLine:     last.SourceLine,
		LastOffset:   last.SourceOffset,
		Stats:        stats,
		Complete:     true,
	}, nil
}

type transcriptImportBatchRecord struct {
	Record        TranscriptRecord
	Kind          string
	Body          string
	CheckpointKey string
}

type transcriptImportCheckpointRecord struct {
	Key          string
	SourceLine   int
	SourceOffset int64
}

type transcriptImportBatcher struct {
	bridge        *Bridge
	session       Session
	filePath      string
	importTurnID  string
	kindPrefix    string
	checkpointID  string
	records       []transcriptImportBatchRecord
	checkpoints   []transcriptImportCheckpointRecord
	htmlParts     []string
	htmlBytes     int
	batchIndex    int
	queueOnly     bool
	maxBatches    int
	queuedBatches int
}

func newTranscriptImportBatcher(b *Bridge, session Session, filePath string, importTurnID string, kindPrefix string, checkpointID string, opts transcriptImportRunOptions) *transcriptImportBatcher {
	return &transcriptImportBatcher{
		bridge:       b,
		session:      session,
		filePath:     filePath,
		importTurnID: importTurnID,
		kindPrefix:   strings.TrimSpace(kindPrefix),
		checkpointID: checkpointID,
		queueOnly:    opts.QueueOnly,
		maxBatches:   opts.MaxBatches,
	}
}

func (b *transcriptImportBatcher) add(ctx context.Context, record transcriptImportBatchRecord) error {
	if b.budgetExhausted() {
		return errTranscriptImportBudgetExhausted
	}
	html := renderTeamsHTMLPart(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    renderKindForOutbox(record.Kind),
		Text:    record.Body,
	}, 1, 1)
	if len(html) > teamsChunkHTMLContentBytes {
		if err := b.flush(ctx); err != nil {
			return err
		}
		if b.budgetExhausted() {
			return errTranscriptImportBudgetExhausted
		}
		local := codexhistory.Session{
			SessionID:   b.session.CodexThreadID,
			ProjectPath: b.session.Cwd,
			FilePath:    b.filePath,
		}
		if err := b.bridge.queueOrSendTranscriptDeliveryChunksWithOptions(ctx, b.session, local, record.Record, record.Record.SourceLine, record.Record.SourceOffset, record.Kind, record.Body, outboxQueueOptions{}, b.importTurnID, b.checkpointID, b.queueOnly); err != nil {
			return err
		}
		b.queuedBatches++
		return nil
	}
	addedBytes := len(html)
	if len(b.htmlParts) > 0 {
		addedBytes += len(transcriptImportBatchSeparatorHTML)
	}
	if len(b.htmlParts) > 0 && b.htmlBytes+addedBytes > teamsChunkHTMLContentBytes {
		if err := b.flush(ctx); err != nil {
			return err
		}
		if b.budgetExhausted() {
			return errTranscriptImportBudgetExhausted
		}
	}
	if len(b.htmlParts) > 0 {
		b.htmlBytes += len(transcriptImportBatchSeparatorHTML)
	}
	b.records = append(b.records, record)
	b.checkpoints = append(b.checkpoints, transcriptImportCheckpointRecord{Key: record.CheckpointKey, SourceLine: record.Record.SourceLine, SourceOffset: record.Record.SourceOffset})
	b.htmlParts = append(b.htmlParts, html)
	b.htmlBytes += len(html)
	return nil
}

func (b *transcriptImportBatcher) recordCheckpoint(ctx context.Context, checkpointKey string, sourceLine int, sourceOffset int64) error {
	if len(b.records) == 0 {
		return b.bridge.recordTranscriptCheckpointDetailedWithID(ctx, b.session, b.filePath, checkpointKey, sourceLine, sourceOffset, b.checkpointID)
	}
	b.checkpoints = append(b.checkpoints, transcriptImportCheckpointRecord{Key: checkpointKey, SourceLine: sourceLine, SourceOffset: sourceOffset})
	return nil
}

func (b *transcriptImportBatcher) flush(ctx context.Context) error {
	if len(b.records) == 0 {
		return nil
	}
	if b.budgetExhausted() {
		return errTranscriptImportBudgetExhausted
	}
	b.batchIndex++
	html := strings.Join(b.htmlParts, transcriptImportBatchSeparatorHTML)
	first := b.records[0]
	last := b.records[len(b.records)-1]
	kind := transcriptImportBatchOutboxKind(b.kindPrefix, first.Record, last.Record, b.batchIndex)
	if err := b.bridge.queueOrSendTranscriptImportBatch(ctx, b.session, b.filePath, b.checkpointID, b.importTurnID, kind, html, first.Record, last.Record, b.queueOnly); err != nil {
		return err
	}
	b.queuedBatches++
	for _, checkpoint := range b.checkpoints {
		if err := b.bridge.recordTranscriptCheckpointDetailedWithID(ctx, b.session, b.filePath, checkpoint.Key, checkpoint.SourceLine, checkpoint.SourceOffset, b.checkpointID); err != nil {
			return err
		}
	}
	b.records = nil
	b.checkpoints = nil
	b.htmlParts = nil
	b.htmlBytes = 0
	return nil
}

func (b *transcriptImportBatcher) budgetExhausted() bool {
	return b != nil && b.maxBatches > 0 && b.queuedBatches >= b.maxBatches
}

func transcriptImportBatchOutboxKind(prefix string, first TranscriptRecord, last TranscriptRecord, index int) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "import"
	}
	firstKey := transcriptRecordKey(first, index)
	lastKey := transcriptRecordKey(last, index)
	return fmt.Sprintf("%s-batch-%04d-%s-%s", prefix, index, firstKey, lastKey)
}

func (b *Bridge) queueAndSendTranscriptImportBatch(ctx context.Context, session Session, sourcePath string, checkpointID string, turnID string, kind string, html string, first TranscriptRecord, last TranscriptRecord) error {
	return b.queueOrSendTranscriptImportBatch(ctx, session, sourcePath, checkpointID, turnID, kind, html, first, last, false)
}

func (b *Bridge) queueOrSendTranscriptImportBatch(ctx context.Context, session Session, sourcePath string, checkpointID string, turnID string, kind string, html string, first TranscriptRecord, last TranscriptRecord, queueOnly bool) error {
	html = strings.TrimSpace(html)
	if html == "" {
		return nil
	}
	delivery := transcriptImportBatchDeliveryRecord(session, sourcePath, checkpointID, turnID, kind, html, first, last)
	msg := b.prepareOutboxForQueue(ctx, teamstore.OutboxMessage{
		ID:              transcriptDeliveryOutboxID(delivery.ID),
		SessionID:       session.ID,
		TurnID:          turnID,
		TeamsChatID:     session.ChatID,
		ScopeID:         b.scope.ID,
		MachineID:       b.machine.ID,
		LeaseGeneration: b.currentLeaseGeneration(),
		Kind:            kind,
		Body:            html,
		PartIndex:       1,
		PartCount:       1,
		RenderedBytes:   len(html),
		SourceTextHash:  normalizedTextHash(html),
	})
	queued, _, _, err := b.store.QueueTranscriptDeliveryOutbox(ctx, teamstore.TranscriptDeliveryQueueRequest{
		Message:  msg,
		Delivery: delivery,
	})
	if err != nil || queueOnly || queued.ID == "" || queued.Status == teamstore.OutboxStatusSent {
		return err
	}
	return b.flushPendingOutboxForChat(ctx, queued.TeamsChatID)
}

func (b *Bridge) importSubagentMarkersToTeams(ctx context.Context, session Session, local codexhistory.Session, importTurnID string) error {
	return b.importSubagentMarkersToTeamsWithOptions(ctx, session, local, importTurnID, transcriptImportRunOptions{})
}

func (b *Bridge) importSubagentMarkersToTeamsWithOptions(ctx context.Context, session Session, local codexhistory.Session, importTurnID string, opts transcriptImportRunOptions) error {
	if len(local.Subagents) == 0 {
		return nil
	}
	subagents := append([]codexhistory.SubagentSession(nil), local.Subagents...)
	sort.SliceStable(subagents, func(i, j int) bool {
		return subagentImportSortTime(subagents[i]).Before(subagentImportSortTime(subagents[j]))
	})
	for i, subagent := range subagents {
		key := subagentImportKey(subagent, i+1)
		checkpointID := transcriptSubagentCheckpointID(session.ID, subagent.SessionID, key)
		if complete, err := b.transcriptCheckpointComplete(ctx, checkpointID); err != nil {
			return err
		} else if complete {
			continue
		}
		sourcePath := strings.TrimSpace(subagent.FilePath)
		if err := b.markTranscriptImportStartedWithID(ctx, session, sourcePath, checkpointID); err != nil {
			return err
		}
		marker := formatSubagentImportMarker(local, subagent)
		if err := b.queueOrSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "import-subagent-marker-"+key, marker, outboxQueueOptions{}, opts.QueueOnly); err != nil {
			return err
		}
		if err := b.markTranscriptImportCompleteWithID(ctx, session, sourcePath, "subagent:"+key, 0, checkpointID); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bridge) transcriptCheckpointComplete(ctx context.Context, checkpointID string) (bool, error) {
	if b == nil || b.store == nil || strings.TrimSpace(checkpointID) == "" {
		return false, nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	return checkpoint.Status == importCheckpointStatusComplete && strings.TrimSpace(checkpoint.LastRecordID) != "", nil
}

func subagentImportSortTime(subagent codexhistory.SubagentSession) time.Time {
	if !subagent.CreatedAt.IsZero() {
		return subagent.CreatedAt
	}
	if !subagent.ModifiedAt.IsZero() {
		return subagent.ModifiedAt
	}
	return time.Time{}
}

func subagentImportKey(subagent codexhistory.SubagentSession, fallback int) string {
	key := firstNonEmptyString(subagent.SessionID, subagent.FilePath, fmt.Sprintf("%d", fallback))
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:16]
}

func transcriptSubagentCheckpointID(sessionID string, subagentSessionID string, fallbackKey string) string {
	key := firstNonEmptyString(subagentSessionID, fallbackKey)
	sum := sha256.Sum256([]byte(key))
	return transcriptCheckpointID(sessionID) + ":subagent:" + hex.EncodeToString(sum[:])[:16]
}

func formatSubagentImportMarker(parent codexhistory.Session, subagent codexhistory.SubagentSession) string {
	lines := []string{
		"Subagent spawned",
		"",
		"The child subagent transcript is not expanded here to keep this Teams chat readable.",
		"Subagent: " + subagent.DisplayTitle(),
	}
	if strings.TrimSpace(subagent.AgentID) != "" {
		lines = append(lines, "Agent: "+strings.TrimSpace(subagent.AgentID))
	}
	if strings.TrimSpace(subagent.SessionID) != "" {
		lines = append(lines, "Subagent session: "+strings.TrimSpace(subagent.SessionID))
	}
	if strings.TrimSpace(subagent.ParentSessionID) != "" {
		lines = append(lines, "Parent session: "+strings.TrimSpace(subagent.ParentSessionID))
	} else if strings.TrimSpace(parent.SessionID) != "" {
		lines = append(lines, "Parent session: "+strings.TrimSpace(parent.SessionID))
	}
	return strings.Join(lines, "\n")
}

func (b *Bridge) transcriptHasNewRecords(ctx context.Context, session Session, local codexhistory.Session) (bool, error) {
	filePath := strings.TrimSpace(local.FilePath)
	if filePath == "" {
		return false, nil
	}
	if err := b.ensureStore(); err != nil {
		return false, err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.Status == importCheckpointStatusFailed {
		if strings.TrimSpace(checkpoint.LastRecordID) == "" && checkpoint.LastSourceLine <= 0 {
			return true, nil
		}
		recovered, err := b.recoverFailedTranscriptCheckpoint(ctx, session, local)
		if err != nil {
			return false, err
		}
		if recovered {
			return b.transcriptHasNewRecords(ctx, session, local)
		}
		return false, errTranscriptCheckpointNotFound
	}
	if checkpoint.Status != "" && checkpoint.Status != importCheckpointStatusComplete {
		return true, nil
	}
	if checkpoint.LastRecordID == "" {
		return true, nil
	}
	transcript, err := b.readLinkedTranscriptDelta(filePath, checkpoint, firstNonEmptyString(local.SessionID, session.CodexThreadID), session.CodexThreadID)
	if err != nil {
		return false, err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
		if err := b.markTranscriptImportFailed(ctx, session, filePath); err != nil {
			return false, err
		}
		recovered, err := b.recoverFailedTranscriptCheckpoint(ctx, session, local)
		if err != nil {
			return false, err
		}
		if recovered {
			return b.transcriptHasNewRecords(ctx, session, local)
		}
		return false, errTranscriptCheckpointNotFound
	}
	for _, record := range transcript.Records {
		if strings.TrimSpace(record.Text) != "" {
			return true, nil
		}
	}
	return false, nil
}

type localCodexBeforeTeamsGate struct {
	Block   bool
	AckBody string
}

type localTranscriptDeltaState struct {
	Active                  bool
	NeedsSync               bool
	CheckpointBeforeActive  string
	CheckpointBeforeLine    int
	CheckpointStatus        string
	CheckpointOrphaned      bool
	CheckpointHadRecord     bool
	HasActionableTranscript bool
}

const localTranscriptCompletedTurnSettleWindow = 5 * time.Minute

func (b *Bridge) prepareLocalCodexBeforeTeamsTurn(ctx context.Context, session *Session) (localCodexBeforeTeamsGate, error) {
	if b == nil || session == nil || strings.TrimSpace(session.CodexThreadID) == "" {
		return localCodexBeforeTeamsGate{}, nil
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return localCodexBeforeTeamsGate{}, err
	}
	local, ok, err := b.localCodexSessionForTeamsSession(ctx, *session)
	if err != nil {
		return localCodexBeforeTeamsGate{}, err
	}
	if !ok || strings.TrimSpace(local.FilePath) == "" {
		return localCodexBeforeTeamsGate{}, nil
	}
	delta, err := b.classifyLocalTranscriptDelta(ctx, *session, local)
	if err != nil {
		if os.IsNotExist(err) {
			return localCodexBeforeTeamsGate{}, nil
		}
		return localCodexBeforeTeamsGate{}, err
	}
	switch delta.CheckpointStatus {
	case importCheckpointStatusImporting:
		if delta.CheckpointOrphaned {
			if err := b.syncSessionTranscript(ctx, *session, local); err != nil {
				return localCodexBeforeTeamsGate{}, err
			}
			return b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
		}
		return localCodexBeforeTeamsGate{
			Block:   true,
			AckBody: "⏳ Queued. I’m preparing this chat history first, then I’ll respond.",
		}, nil
	case importCheckpointStatusBlocked:
		if err := b.syncSessionTranscript(ctx, *session, local); err != nil {
			return localCodexBeforeTeamsGate{}, err
		}
		refreshed, err := b.classifyLocalTranscriptDelta(ctx, *session, local)
		if err != nil {
			if os.IsNotExist(err) {
				return localCodexBeforeTeamsGate{}, nil
			}
			return localCodexBeforeTeamsGate{}, err
		}
		if refreshed.CheckpointStatus != importCheckpointStatusBlocked {
			return b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
		}
		return localCodexBeforeTeamsGate{
			Block:   true,
			AckBody: "⚠️ Queued. Local Codex history has a paused backlog. Send `helper publish-history` here; I’ll continue after the import finishes.",
		}, nil
	case importCheckpointStatusFailed:
		recovered, err := b.recoverFailedTranscriptCheckpoint(ctx, *session, local)
		if err != nil {
			return localCodexBeforeTeamsGate{}, err
		}
		if recovered {
			return b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
		}
		return localCodexBeforeTeamsGate{
			Block:   true,
			AckBody: "⚠️ Queued. Local Codex history sync needs attention before I continue this chat.",
		}, nil
	}
	if delta.Active {
		advanced, err := b.advanceRecentCompletedTeamsTranscriptTail(ctx, *session, local)
		if err != nil {
			return localCodexBeforeTeamsGate{}, err
		}
		if advanced {
			return b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
		}
		if !delta.CheckpointHadRecord && strings.TrimSpace(delta.CheckpointBeforeActive) != "" {
			if err := b.recordTranscriptCheckpoint(ctx, *session, local.FilePath, delta.CheckpointBeforeActive, delta.CheckpointBeforeLine); err != nil {
				return localCodexBeforeTeamsGate{}, err
			}
		}
		return localCodexBeforeTeamsGate{
			Block:   true,
			AckBody: "⏳ Queued. Codex is active in the CLI for this chat; I’ll respond here after that finishes.",
		}, nil
	}
	if delta.NeedsSync {
		if err := b.syncSessionTranscript(ctx, *session, local); err != nil {
			return localCodexBeforeTeamsGate{}, err
		}
		remainingDelta, err := b.classifyLocalTranscriptDelta(ctx, *session, local)
		if err != nil {
			if os.IsNotExist(err) {
				return localCodexBeforeTeamsGate{}, nil
			}
			return localCodexBeforeTeamsGate{}, err
		}
		switch remainingDelta.CheckpointStatus {
		case importCheckpointStatusImporting:
			return localCodexBeforeTeamsGate{
				Block:   true,
				AckBody: "⏳ Queued. I’m preparing this chat history first, then I’ll respond.",
			}, nil
		case importCheckpointStatusBlocked:
			return localCodexBeforeTeamsGate{
				Block:   true,
				AckBody: "⚠️ Queued. Local Codex history has a paused backlog. Send `helper publish-history` here; I’ll continue after the import finishes.",
			}, nil
		case importCheckpointStatusFailed:
			recovered, err := b.recoverFailedTranscriptCheckpoint(ctx, *session, local)
			if err != nil {
				return localCodexBeforeTeamsGate{}, err
			}
			if recovered {
				return b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
			}
			return localCodexBeforeTeamsGate{
				Block:   true,
				AckBody: "⚠️ Queued. Local Codex history sync needs attention before I continue this chat.",
			}, nil
		}
		if remainingDelta.Active || remainingDelta.NeedsSync || remainingDelta.HasActionableTranscript {
			return localCodexBeforeTeamsGate{
				Block:   true,
				AckBody: "⏳ Queued. I’m syncing recent Codex updates first, then I’ll respond.",
			}, nil
		}
	}
	return localCodexBeforeTeamsGate{}, nil
}

func (b *Bridge) localCodexSessionForTeamsSession(ctx context.Context, session Session) (codexhistory.Session, bool, error) {
	if b == nil || strings.TrimSpace(session.CodexThreadID) == "" {
		return codexhistory.Session{}, false, nil
	}
	if err := b.ensureStore(); err != nil {
		return codexhistory.Session{}, false, err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return codexhistory.Session{}, false, err
	}
	if checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]; strings.TrimSpace(checkpoint.SourcePath) != "" {
		return codexhistory.Session{
			SessionID:   session.CodexThreadID,
			ProjectPath: session.Cwd,
			FilePath:    checkpoint.SourcePath,
		}, true, nil
	}
	projects, err := discoverCodexProjectsForTeams(ctx, "")
	if err != nil {
		return codexhistory.Session{}, false, nil
	}
	for _, project := range projects {
		for _, local := range project.Sessions {
			if local.SessionID != session.CodexThreadID {
				continue
			}
			if local.ProjectPath == "" {
				local.ProjectPath = project.Path
			}
			if strings.TrimSpace(local.FilePath) == "" {
				return codexhistory.Session{}, false, nil
			}
			return local, true, nil
		}
	}
	return codexhistory.Session{}, false, nil
}

func (b *Bridge) classifyLocalTranscriptDelta(ctx context.Context, session Session, local codexhistory.Session) (localTranscriptDeltaState, error) {
	var out localTranscriptDeltaState
	if strings.TrimSpace(local.FilePath) == "" {
		return out, nil
	}
	if err := b.ensureStore(); err != nil {
		return out, err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return out, err
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	out.CheckpointStatus = checkpoint.Status
	out.CheckpointOrphaned = transcriptImportCheckpointIsOrphaned(state, checkpoint)
	out.CheckpointHadRecord = checkpoint.LastRecordID != ""
	switch checkpoint.Status {
	case importCheckpointStatusImporting, importCheckpointStatusFailed, importCheckpointStatusBlocked:
		return out, nil
	}
	var transcript Transcript
	if checkpoint.LastRecordID == "" {
		transcript, err = ReadSessionTranscript(local.FilePath)
	} else {
		transcript, err = b.readLinkedTranscriptDelta(local.FilePath, checkpoint, firstNonEmptyString(local.SessionID, session.CodexThreadID), session.CodexThreadID)
	}
	if err != nil {
		return out, err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
		if err := b.markTranscriptImportFailed(ctx, session, local.FilePath); err != nil {
			return out, err
		}
		out.CheckpointStatus = importCheckpointStatusFailed
		return out, nil
	}
	if len(transcript.Records) == 0 {
		return out, nil
	}
	teamsOriginHashes := teamsOriginTextHashes(state, session.ID)
	known := newKnownTranscriptOutboxDedupeState(state, session.ID, checkpoint.UpdatedAt)
	dedupe := newTranscriptDedupeState()
	active := false
	for i, record := range transcript.Records {
		body := formatTranscriptRecordForTeams(record)
		if strings.TrimSpace(body) == "" {
			continue
		}
		if shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginHashes) ||
			known.shouldSkip(record, body) ||
			dedupe.shouldSkip(record, body) {
			continue
		}
		out.HasActionableTranscript = true
		if shouldSkipBackgroundTranscriptRecord(record) {
			if !active {
				out.setCheckpointBeforeActive(transcript.Records, i)
			}
			active = true
			continue
		}
		if transcriptRecordIsTerminal(record) {
			active = false
			out.NeedsSync = true
			continue
		}
		switch record.Kind {
		case TranscriptKindCompact:
			out.NeedsSync = true
		case TranscriptKindUser, TranscriptKindStatus, TranscriptKindArtifact, TranscriptKindUnknown:
			if !active {
				out.setCheckpointBeforeActive(transcript.Records, i)
			}
			active = true
			out.NeedsSync = true
		case TranscriptKindAssistant:
			active = false
			out.NeedsSync = true
		}
	}
	out.Active = active
	if out.Active {
		out.NeedsSync = false
	}
	return out, nil
}

func (b *Bridge) advanceRecentCompletedTeamsTranscriptTail(ctx context.Context, session Session, local codexhistory.Session) (bool, error) {
	if b == nil || strings.TrimSpace(local.FilePath) == "" {
		return false, nil
	}
	if err := b.ensureStore(); err != nil {
		return false, err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if strings.TrimSpace(checkpoint.LastRecordID) == "" {
		return false, nil
	}
	switch checkpoint.Status {
	case importCheckpointStatusImporting, importCheckpointStatusFailed, importCheckpointStatusBlocked:
		return false, nil
	}
	recentTurn, ok := latestRecentCompletedTeamsTurn(state, session.ID, time.Now(), localTranscriptCompletedTurnSettleWindow)
	if !ok {
		return false, nil
	}
	if !checkpoint.UpdatedAt.IsZero() && !recentTurn.CompletedAt.IsZero() && checkpoint.UpdatedAt.After(recentTurn.CompletedAt) {
		return false, nil
	}
	transcript, err := b.readLinkedTranscriptDelta(local.FilePath, checkpoint, firstNonEmptyString(local.SessionID, session.CodexThreadID), session.CodexThreadID)
	if err != nil {
		return false, err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") || len(transcript.Records) == 0 {
		return false, nil
	}
	teamsOriginHashes := teamsOriginTextHashes(state, session.ID)
	known := newKnownTranscriptOutboxDedupeState(state, session.ID, checkpoint.UpdatedAt)
	dedupe := newTranscriptDedupeState()
	sawRecentTeamsTurnRecord := false
	var lastKey string
	var lastLine int
	var lastOffset int64
	for i, record := range transcript.Records {
		body := formatTranscriptRecordForTeams(record)
		skip := strings.TrimSpace(body) == ""
		if !skip && (shouldSkipBackgroundTranscriptRecord(record) || transcriptRecordIsTerminal(record)) {
			skip = true
		}
		if !skip && shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginHashes) {
			sawRecentTeamsTurnRecord = true
			skip = true
		}
		if !skip && known.shouldSkip(record, body) {
			sawRecentTeamsTurnRecord = true
			skip = true
		}
		if !skip && dedupe.shouldSkip(record, body) {
			skip = true
		}
		if !skip && record.Kind == TranscriptKindStatus && sawRecentTeamsTurnRecord {
			skip = true
		}
		if !skip {
			return false, nil
		}
		key := transcriptRecordCheckpointKey(record)
		if strings.TrimSpace(key) == "" {
			continue
		}
		lastKey = key
		lastLine, lastOffset = transcriptCheckpointPositionForRecord(transcript.Records, i)
	}
	if strings.TrimSpace(lastKey) == "" {
		return false, nil
	}
	if !sawRecentTeamsTurnRecord {
		return false, nil
	}
	if err := b.recordTranscriptCheckpointDetailed(ctx, session, local.FilePath, lastKey, lastLine, lastOffset); err != nil {
		return false, err
	}
	return true, nil
}

func latestRecentCompletedTeamsTurn(state teamstore.State, sessionID string, now time.Time, window time.Duration) (teamstore.Turn, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || window <= 0 {
		return teamstore.Turn{}, false
	}
	var latest teamstore.Turn
	for _, turn := range state.Turns {
		if turn.SessionID != sessionID || turn.Status != teamstore.TurnStatusCompleted || turn.CompletedAt.IsZero() {
			continue
		}
		if now.Sub(turn.CompletedAt) > window {
			continue
		}
		inbound, ok := state.InboundEvents[turn.InboundEventID]
		if !ok {
			continue
		}
		if !inboundSourceIsTeamsOrigin(inbound.Source) {
			continue
		}
		if latest.CompletedAt.IsZero() || turn.CompletedAt.After(latest.CompletedAt) {
			latest = turn
		}
	}
	return latest, !latest.CompletedAt.IsZero()
}

type recentCompletedTeamsTranscriptMirrorSkipper struct {
	enabled bool
	seen    bool
}

func newRecentCompletedTeamsTranscriptMirrorSkipper(state teamstore.State, sessionID string, now time.Time) recentCompletedTeamsTranscriptMirrorSkipper {
	_, ok := latestRecentCompletedTeamsTurn(state, sessionID, now, localTranscriptCompletedTurnSettleWindow)
	return recentCompletedTeamsTranscriptMirrorSkipper{enabled: ok}
}

func (s *recentCompletedTeamsTranscriptMirrorSkipper) shouldSkip(record TranscriptRecord, body string, teamsOriginHashes map[string]bool, known *knownTranscriptOutboxDedupeState) bool {
	if s == nil || !s.enabled {
		return false
	}
	if shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginHashes) ||
		known.shouldSkip(record, body) {
		s.seen = true
		return true
	}
	if record.Kind == TranscriptKindStatus && s.seen {
		return true
	}
	if s.seen {
		s.enabled = false
		s.seen = false
	}
	return false
}

func (s *localTranscriptDeltaState) setCheckpointBeforeActive(records []TranscriptRecord, index int) {
	if s == nil || s.CheckpointBeforeActive != "" || index <= 0 || index > len(records)-1 {
		return
	}
	previous := records[index-1]
	s.CheckpointBeforeActive = transcriptRecordCheckpointKey(previous)
	s.CheckpointBeforeLine = previous.SourceLine
}

func (b *Bridge) localTranscriptHasActionableDelta(ctx context.Context, session Session, local codexhistory.Session) (bool, error) {
	delta, err := b.classifyLocalTranscriptDelta(ctx, session, local)
	if err != nil {
		return false, err
	}
	return delta.Active || delta.NeedsSync || delta.HasActionableTranscript, nil
}

func transcriptRecordIsTerminal(record TranscriptRecord) bool {
	source := strings.ToLower(strings.TrimSpace(record.SourceType))
	switch source {
	case "turn.failed", "turn/completed", "turn.completed":
		return true
	default:
		return false
	}
}

func transcriptHasDiagnostic(transcript Transcript, kind string) bool {
	kind = strings.TrimSpace(kind)
	for _, diagnostic := range transcript.Diagnostics {
		if diagnostic.Kind == kind {
			return true
		}
	}
	return false
}

func formatTranscriptRecordForTeams(record TranscriptRecord) string {
	text := strings.TrimSpace(StripHelperPromptEchoes(StripArtifactManifestBlocks(record.Text)))
	if record.Kind == TranscriptKindAssistant {
		text = StripOAIMemoryCitationBlocks(text)
	}
	switch record.Kind {
	case TranscriptKindTool:
		if text != "" && !strings.HasPrefix(strings.ToLower(text), "imported tool/status event:") {
			return "Imported status update: " + text
		}
	}
	return text
}

func shouldSkipImportedTranscriptRecord(record TranscriptRecord) bool {
	return record.Kind == TranscriptKindTool
}

func transcriptRecordOutboxKind(prefix string, record TranscriptRecord, fallback int) string {
	role := "helper"
	switch record.Kind {
	case TranscriptKindUser:
		role = "user"
	case TranscriptKindAssistant:
		role = "assistant"
	case TranscriptKindTool:
		role = "tool"
	case TranscriptKindStatus:
		role = "status"
	case TranscriptKindCompact:
		role = "compact"
	case TranscriptKindArtifact:
		role = "artifact"
	}
	return strings.TrimSpace(prefix) + "-" + role + "-" + transcriptRecordKey(record, fallback)
}

func transcriptRecordKey(record TranscriptRecord, fallback int) string {
	key := firstNonEmptyString(record.ItemID, record.DedupeKey, record.SourceItemID, fmt.Sprintf("%04d", fallback))
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:16]
}

func transcriptRecordCheckpointKey(record TranscriptRecord) string {
	return firstNonEmptyString(record.ItemID, record.DedupeKey)
}

func transcriptDeliveryRecord(session Session, local codexhistory.Session, record TranscriptRecord, kind string, body string) teamstore.TranscriptDeliveryRecord {
	recordID := transcriptRecordCheckpointKey(record)
	sourcePath := strings.TrimSpace(local.FilePath)
	threadID := firstNonEmptyString(record.ThreadID, local.SessionID, session.CodexThreadID)
	textHash := normalizedTextHash(body)
	parts := []string{
		"v1",
		strings.TrimSpace(session.ID),
		strings.TrimSpace(threadID),
		cleanComparablePath(sourcePath),
		strings.TrimSpace(recordID),
		strconv.Itoa(record.SourceLine),
		strconv.FormatInt(record.SourceOffset, 10),
		string(record.Kind),
		strings.TrimSpace(textHash),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return teamstore.TranscriptDeliveryRecord{
		ID:             "transcript-delivery:" + strings.TrimSpace(session.ID) + ":" + hex.EncodeToString(sum[:])[:24],
		SessionID:      strings.TrimSpace(session.ID),
		CodexThreadID:  strings.TrimSpace(threadID),
		SourcePath:     sourcePath,
		SourceLine:     record.SourceLine,
		SourceOffset:   record.SourceOffset,
		SourceRecordID: strings.TrimSpace(recordID),
		Kind:           strings.TrimSpace(kind),
		TextHash:       textHash,
		Status:         teamstore.TranscriptDeliveryStatusQueued,
	}
}

func transcriptImportBatchDeliveryRecord(session Session, sourcePath string, checkpointID string, importTurnID string, kind string, html string, first TranscriptRecord, last TranscriptRecord) teamstore.TranscriptDeliveryRecord {
	firstID := transcriptRecordCheckpointKey(first)
	lastID := transcriptRecordCheckpointKey(last)
	threadID := firstNonEmptyString(first.ThreadID, last.ThreadID, session.CodexThreadID)
	textHash := normalizedTextHash(html)
	parts := []string{
		"v1-import-batch",
		strings.TrimSpace(session.ID),
		strings.TrimSpace(threadID),
		cleanComparablePath(sourcePath),
		strings.TrimSpace(checkpointID),
		strings.TrimSpace(importTurnID),
		strings.TrimSpace(kind),
		strings.TrimSpace(firstID),
		strconv.Itoa(first.SourceLine),
		strconv.FormatInt(first.SourceOffset, 10),
		strings.TrimSpace(lastID),
		strconv.Itoa(last.SourceLine),
		strconv.FormatInt(last.SourceOffset, 10),
		strings.TrimSpace(textHash),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	sourceRecordID := strings.TrimSpace(firstID)
	if sourceRecordID == "" {
		sourceRecordID = strings.TrimSpace(lastID)
	} else if lastID != "" && lastID != firstID {
		sourceRecordID += ".." + strings.TrimSpace(lastID)
	}
	return teamstore.TranscriptDeliveryRecord{
		ID:             "transcript-delivery:" + strings.TrimSpace(session.ID) + ":" + hex.EncodeToString(sum[:])[:24],
		SessionID:      strings.TrimSpace(session.ID),
		CodexThreadID:  strings.TrimSpace(threadID),
		SourcePath:     strings.TrimSpace(sourcePath),
		SourceLine:     last.SourceLine,
		SourceOffset:   last.SourceOffset,
		SourceRecordID: sourceRecordID,
		Kind:           strings.TrimSpace(kind),
		TextHash:       textHash,
		Status:         teamstore.TranscriptDeliveryStatusQueued,
	}
}

func transcriptDeliveryPartID(baseID string, partIndex int, partCount int) string {
	baseID = strings.TrimSpace(baseID)
	if partCount <= 1 {
		return baseID
	}
	if partIndex <= 0 {
		partIndex = 1
	}
	return fmt.Sprintf("%s:part:%03d-of-%03d", baseID, partIndex, partCount)
}

func transcriptDeliveryOutboxID(deliveryID string) string {
	return "outbox:" + strings.TrimSpace(deliveryID)
}

func transcriptDeliveryCheckpoint(session Session, sourcePath string, lastRecordID string, lastLine int, lastOffset int64) teamstore.ImportCheckpoint {
	sourceSize, sourceModTime := transcriptSourceFileState(sourcePath)
	return teamstore.ImportCheckpoint{
		ID:             transcriptCheckpointID(session.ID),
		SessionID:      session.ID,
		SourcePath:     sourcePath,
		LastRecordID:   lastRecordID,
		LastSourceLine: lastLine,
		LastOffset:     lastOffset,
		SourceSize:     sourceSize,
		SourceModTime:  sourceModTime,
		Status:         importCheckpointStatusComplete,
	}
}

func (b *Bridge) recordSkippedTranscriptDelivery(ctx context.Context, session Session, local codexhistory.Session, record TranscriptRecord, checkpointLine int, checkpointOffset int64, kind string, body string) error {
	if b == nil || b.store == nil {
		return nil
	}
	record.SourceLine = checkpointLine
	record.SourceOffset = checkpointOffset
	delivery := transcriptDeliveryRecord(session, local, record, kind, body)
	delivery.Status = teamstore.TranscriptDeliveryStatusSkipped
	_, _, err := b.store.RecordTranscriptDelivery(ctx, delivery, transcriptDeliveryCheckpoint(session, local.FilePath, transcriptRecordCheckpointKey(record), checkpointLine, checkpointOffset))
	return err
}

func (b *Bridge) syncLinkedTranscriptsIfDue(ctx context.Context, now time.Time) error {
	if now.IsZero() {
		now = time.Now()
	}
	if !b.lastTranscriptSync.IsZero() && now.Sub(b.lastTranscriptSync) < transcriptSyncMinInterval {
		return nil
	}
	b.lastTranscriptSync = now
	return b.syncLinkedTranscripts(ctx)
}

func (b *Bridge) syncLinkedTranscripts(ctx context.Context) error {
	if len(b.reg.Sessions) == 0 {
		return nil
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	state, err := b.store.TranscriptImportStateSnapshot(ctx)
	if err != nil {
		return err
	}
	activeTeamsTurns, err := b.store.RunningTurnSessionIDs(ctx)
	if err != nil {
		return err
	}
	var needsDiscovery []Session
	for _, session := range b.reg.ActiveSessions() {
		if session.CodexThreadID == "" {
			continue
		}
		if activeTeamsTurns[session.ID] {
			continue
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		if local, ok := linkedTranscriptLocalFromCheckpoint(session, checkpoint); ok {
			if linkedTranscriptCheckpointIdleUnchanged(local.FilePath, checkpoint) {
				continue
			}
			if err := b.syncSessionTranscriptFromSnapshot(ctx, session, local, state, checkpoint, true); err != nil {
				return err
			}
			continue
		}
		needsDiscovery = append(needsDiscovery, session)
	}
	if len(needsDiscovery) == 0 {
		return nil
	}
	projects, err := discoverCodexProjectsForTeams(ctx, "")
	if err != nil {
		return nil
	}
	byID := make(map[string]codexhistory.Session)
	for _, project := range projects {
		for _, session := range project.Sessions {
			if session.SessionID == "" {
				continue
			}
			if session.ProjectPath == "" {
				session.ProjectPath = project.Path
			}
			byID[session.SessionID] = session
		}
	}
	for _, session := range needsDiscovery {
		local, ok := byID[session.CodexThreadID]
		if !ok || strings.TrimSpace(local.FilePath) == "" {
			continue
		}
		if info, err := os.Stat(local.FilePath); err != nil || info.IsDir() {
			continue
		}
		if err := b.syncSessionTranscriptFromSnapshot(ctx, session, local, state, teamstore.ImportCheckpoint{}, false); err != nil {
			return err
		}
	}
	return nil
}

func linkedTranscriptLocalFromCheckpoint(session Session, checkpoint teamstore.ImportCheckpoint) (codexhistory.Session, bool) {
	sourcePath := strings.TrimSpace(checkpoint.SourcePath)
	if sourcePath == "" {
		return codexhistory.Session{}, false
	}
	info, err := os.Stat(sourcePath)
	if err != nil || info.IsDir() {
		return codexhistory.Session{}, false
	}
	return codexhistory.Session{
		SessionID:   strings.TrimSpace(session.CodexThreadID),
		ProjectPath: strings.TrimSpace(session.Cwd),
		FilePath:    sourcePath,
		ModifiedAt:  info.ModTime(),
	}, true
}

func (b *Bridge) queueActiveTurnTranscriptStatusBeforeFinal(ctx context.Context, session *Session, turn teamstore.Turn) (int, error) {
	if b == nil || session == nil || b.store == nil || strings.TrimSpace(session.ID) == "" || strings.TrimSpace(session.ChatID) == "" || strings.TrimSpace(turn.ID) == "" {
		return 0, nil
	}
	if strings.TrimSpace(session.CodexThreadID) == "" {
		return 0, nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return 0, err
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if strings.TrimSpace(checkpoint.LastRecordID) == "" {
		return 0, nil
	}
	local, ok := linkedTranscriptLocalFromCheckpoint(*session, checkpoint)
	if !ok {
		return 0, nil
	}
	switch checkpoint.Status {
	case importCheckpointStatusImporting, importCheckpointStatusFailed, importCheckpointStatusBlocked:
		return 0, nil
	}
	transcript, err := b.readLinkedTranscriptDelta(local.FilePath, checkpoint, firstNonEmptyString(local.SessionID, session.CodexThreadID), session.CodexThreadID)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") || len(transcript.Records) == 0 {
		return 0, nil
	}
	teamsOriginHashes := teamsOriginTextHashes(state, session.ID)
	teamsOriginDisplays := teamsOriginDisplayTexts(state, session.ID)
	known := newKnownTranscriptOutboxDedupeState(state, session.ID, checkpoint.UpdatedAt)
	dedupe := newTranscriptDedupeState()
	queued := 0
	for i, record := range transcript.Records {
		checkpointLine, checkpointOffset := transcriptCheckpointPositionForRecord(transcript.Records, i)
		record.SourceLine = checkpointLine
		record.SourceOffset = checkpointOffset
		if record.Kind == TranscriptKindAssistant || transcriptRecordIsTerminal(record) {
			break
		}
		body := formatTranscriptRecordForTeams(record)
		body = teamsOriginTranscriptUserDisplayBody(record, body, teamsOriginDisplays)
		if strings.TrimSpace(body) == "" || shouldSkipBackgroundTranscriptRecord(record) {
			continue
		}
		if shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginHashes) || dedupe.shouldSkip(record, body) {
			continue
		}
		kind := transcriptRecordOutboxKind("codex", record, i+1)
		if known.shouldSkip(record, body) {
			if record.Kind == TranscriptKindStatus || record.Kind == TranscriptKindCompact {
				if err := b.recordSkippedTranscriptDelivery(ctx, *session, local, record, checkpointLine, checkpointOffset, kind, body); err != nil {
					return queued, err
				}
			}
			continue
		}
		switch record.Kind {
		case TranscriptKindStatus, TranscriptKindCompact:
			if err := b.queueOrSendTranscriptDeliveryChunksWithOptions(ctx, *session, local, record, checkpointLine, checkpointOffset, kind, body, outboxQueueOptions{}, turn.ID, transcriptCheckpointID(session.ID), true); err != nil {
				return queued, err
			}
			queued++
			if queued >= transcriptSyncMaxRecordsPerSessionPerCycle {
				return queued, nil
			}
		}
	}
	return queued, nil
}

func (b *Bridge) syncSessionTranscript(ctx context.Context, session Session, local codexhistory.Session) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	state, err := b.store.TranscriptImportStateSnapshot(ctx)
	if err != nil {
		return err
	}
	activeTeamsTurns, err := b.store.RunningTurnSessionIDs(ctx)
	if err != nil {
		return err
	}
	if activeTeamsTurns[session.ID] {
		return nil
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint, hasCheckpoint := state.ImportCheckpoints[checkpointID]
	return b.syncSessionTranscriptFromSnapshot(ctx, session, local, state, checkpoint, hasCheckpoint)
}

func (b *Bridge) syncSessionTranscriptFromSnapshot(ctx context.Context, session Session, local codexhistory.Session, state teamstore.State, checkpoint teamstore.ImportCheckpoint, hasCheckpoint bool) error {
	checkpointID := transcriptCheckpointID(session.ID)
	if err := b.maybeUpdateWorkChatTitleFromLocalSession(ctx, &session, local); err != nil {
		return err
	}
	if hasCheckpoint {
		switch checkpoint.Status {
		case importCheckpointStatusImporting:
			if transcriptImportCheckpointIsOrphaned(state, checkpoint) {
				return b.resumeInterruptedTranscriptImport(ctx, session, local, checkpoint)
			}
			return nil
		case importCheckpointStatusFailed:
			recovered, err := b.recoverFailedTranscriptCheckpoint(ctx, session, local)
			if err != nil {
				return err
			}
			if !recovered {
				return nil
			}
			state, err = b.store.Load(ctx)
			if err != nil {
				return err
			}
			checkpoint = state.ImportCheckpoints[checkpointID]
			hasCheckpoint = true
		}
	}
	if hasCheckpoint && transcriptImportCheckpointNeedsBudgetedResume(checkpoint, local.FilePath) {
		return b.resumeBudgetedTranscriptImport(ctx, session, local, checkpoint)
	}
	if !hasCheckpoint || checkpoint.LastRecordID == "" {
		transcript, err := ReadSessionTranscript(local.FilePath)
		if err != nil {
			return err
		}
		if len(transcript.Records) == 0 {
			return nil
		}
		last := transcript.Records[len(transcript.Records)-1]
		return b.recordTranscriptCheckpointDetailed(ctx, session, local.FilePath, firstNonEmptyString(last.DedupeKey, last.ItemID), last.SourceLine, last.SourceOffset)
	}
	if hasCheckpoint && linkedTranscriptCheckpointNeedsPositionBackfill(checkpoint, local.FilePath) {
		updated, ok, err := b.backfillLinkedTranscriptCheckpointPosition(ctx, session, local, checkpoint)
		if err != nil {
			return err
		}
		if ok {
			checkpoint = updated
			if linkedTranscriptCheckpointIdleUnchanged(local.FilePath, checkpoint) {
				return nil
			}
		}
	}
	transcript, err := b.readLinkedTranscriptDelta(local.FilePath, checkpoint, local.SessionID, session.CodexThreadID)
	if err != nil {
		return err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
		return b.markTranscriptImportFailed(ctx, session, local.FilePath)
	}
	if len(transcript.Records) == 0 {
		return nil
	}
	if advanced, err := b.advanceRecentCompletedTeamsTranscriptTail(ctx, session, local); err != nil {
		return err
	} else if advanced {
		return nil
	}
	state, err = b.store.Load(ctx)
	if err != nil {
		return err
	}
	if runningTurnSessions(state)[session.ID] {
		return nil
	}
	if current := state.ImportCheckpoints[checkpointID]; strings.TrimSpace(current.LastRecordID) != "" {
		switch current.Status {
		case importCheckpointStatusImporting, importCheckpointStatusFailed:
			return nil
		}
		if strings.TrimSpace(current.LastRecordID) != strings.TrimSpace(checkpoint.LastRecordID) {
			return nil
		}
	}
	teamsOriginHashes := teamsOriginTextHashes(state, session.ID)
	teamsOriginDisplays := teamsOriginDisplayTexts(state, session.ID)
	teamsOriginTerminalHashes := teamsOriginTerminalTextHashes(state, session.ID)
	knownForCount := newKnownTranscriptOutboxDedupeState(state, session.ID, checkpoint.UpdatedAt)
	now := time.Now()
	visibleBacklog := countVisibleTranscriptSyncRecords(state, session, local, transcript.Records, teamsOriginHashes, teamsOriginDisplays, teamsOriginTerminalHashes, knownForCount, newRecentCompletedTeamsTranscriptMirrorSkipper(state, session.ID, now))
	if visibleBacklog > transcriptSyncMaxAutoBacklogRecords {
		return b.blockAutomaticTranscriptSync(ctx, session, local.FilePath, checkpoint, visibleBacklog)
	}
	dedupe := newTranscriptDedupeState()
	known := newKnownTranscriptOutboxDedupeState(state, session.ID, checkpoint.UpdatedAt)
	recentTeamsMirror := newRecentCompletedTeamsTranscriptMirrorSkipper(state, session.ID, now)
	sent := 0
	for i, record := range transcript.Records {
		checkpointLine, checkpointOffset := transcriptCheckpointPositionForRecord(transcript.Records, i)
		record.SourceLine = checkpointLine
		record.SourceOffset = checkpointOffset
		if strings.TrimSpace(record.Text) == "" {
			continue
		}
		if shouldSkipBackgroundTranscriptRecord(record) {
			if err := b.recordTranscriptCheckpointDetailed(ctx, session, local.FilePath, transcriptRecordCheckpointKey(record), checkpointLine, checkpointOffset); err != nil {
				return err
			}
			continue
		}
		body := formatTranscriptRecordForTeams(record)
		body = teamsOriginTranscriptUserDisplayBody(record, body, teamsOriginDisplays)
		if strings.TrimSpace(body) == "" {
			if err := b.recordTranscriptCheckpointDetailed(ctx, session, local.FilePath, transcriptRecordCheckpointKey(record), checkpointLine, checkpointOffset); err != nil {
				return err
			}
			continue
		}
		kind := transcriptRecordOutboxKind("sync", record, i+1)
		if recentTeamsMirror.shouldSkip(record, body, teamsOriginHashes, known) ||
			shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginTerminalHashes) ||
			known.shouldSkip(record, body) ||
			dedupe.shouldSkip(record, body) {
			if err := b.recordSkippedTranscriptDelivery(ctx, session, local, record, checkpointLine, checkpointOffset, kind, body); err != nil {
				return err
			}
			continue
		}
		opts := transcriptSyncOutboxOptions(record)
		if err := b.queueAndSendTranscriptDeliveryChunksWithOptions(ctx, session, local, record, checkpointLine, checkpointOffset, kind, body, opts); err != nil {
			return err
		}
		sent++
		if sent >= transcriptSyncMaxRecordsPerSessionPerCycle {
			return nil
		}
	}
	return nil
}

func (b *Bridge) readLinkedTranscriptDelta(filePath string, checkpoint teamstore.ImportCheckpoint, sessionID string, threadID string) (Transcript, error) {
	if strings.TrimSpace(filePath) == "" {
		return Transcript{}, nil
	}
	if linkedCheckpointFileUnchanged(filePath, checkpoint) {
		return Transcript{SourceName: filePath}, nil
	}
	if checkpoint.LastOffset > 0 && strings.TrimSpace(checkpoint.SourcePath) == strings.TrimSpace(filePath) {
		previous := historyTieredFileState{
			Path:        filePath,
			Size:        checkpoint.SourceSize,
			ModTime:     checkpoint.SourceModTime,
			Offset:      checkpoint.LastOffset,
			Line:        checkpoint.LastSourceLine,
			SessionID:   strings.TrimSpace(sessionID),
			ThreadID:    strings.TrimSpace(threadID),
			LastFinalID: strings.TrimSpace(checkpoint.LastRecordID),
		}
		result, err := historyTieredScanTail(filePath, previous, historyTieredMaxTailBytes)
		if err != nil {
			return Transcript{}, err
		}
		if result.TooLarge {
			result, err = historyTieredScanTail(filePath, previous, 0)
			if err != nil {
				return Transcript{}, err
			}
		}
		if !result.TooLarge && !result.Truncated {
			return Transcript{SourceName: filePath, Records: filterTranscriptRecordsAfterCheckpoint(result.Records, checkpoint.LastRecordID)}, nil
		}
	}
	return ReadSessionTranscriptSince(filePath, checkpoint.LastRecordID)
}

func filterTranscriptRecordsAfterCheckpoint(records []TranscriptRecord, afterKey string) []TranscriptRecord {
	afterKey = strings.TrimSpace(afterKey)
	if afterKey == "" || len(records) == 0 {
		return records
	}
	for i, record := range records {
		if record.DedupeKey == afterKey || record.ItemID == afterKey {
			return append([]TranscriptRecord(nil), records[i+1:]...)
		}
	}
	return records
}

func transcriptCheckpointPositionForRecord(records []TranscriptRecord, index int) (int, int64) {
	if index < 0 || index >= len(records) {
		return 0, 0
	}
	record := records[index]
	for i := index + 1; i < len(records); i++ {
		if records[i].SourceLine != record.SourceLine {
			break
		}
		if strings.TrimSpace(transcriptRecordCheckpointKey(records[i])) != "" {
			line := record.SourceLine
			if line > 0 {
				line--
			}
			return line, record.SourceStartOffset
		}
	}
	return record.SourceLine, record.SourceOffset
}

func linkedCheckpointFileUnchanged(filePath string, checkpoint teamstore.ImportCheckpoint) bool {
	if checkpoint.SourceSize <= 0 || checkpoint.SourceModTime.IsZero() || checkpoint.LastOffset <= 0 {
		return false
	}
	if checkpoint.LastOffset != checkpoint.SourceSize {
		return false
	}
	info, err := os.Stat(filePath)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Size() == checkpoint.SourceSize && info.ModTime().Equal(checkpoint.SourceModTime)
}

func linkedTranscriptCheckpointNeedsPositionBackfill(checkpoint teamstore.ImportCheckpoint, sourcePath string) bool {
	if checkpoint.Status != importCheckpointStatusComplete {
		return false
	}
	sourcePath = strings.TrimSpace(firstNonEmptyString(sourcePath, checkpoint.SourcePath))
	if strings.TrimSpace(checkpoint.LastRecordID) == "" || sourcePath == "" {
		return false
	}
	if transcriptImportCheckpointNeedsBudgetedResume(checkpoint, sourcePath) {
		return false
	}
	return checkpoint.LastOffset <= 0 || checkpoint.SourceSize <= 0 || checkpoint.SourceModTime.IsZero()
}

func (b *Bridge) backfillLinkedTranscriptCheckpointPosition(ctx context.Context, session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint) (teamstore.ImportCheckpoint, bool, error) {
	sourcePath := strings.TrimSpace(firstNonEmptyString(local.FilePath, checkpoint.SourcePath))
	if sourcePath == "" {
		return checkpoint, false, nil
	}
	position, ok, err := findTranscriptCheckpointPosition(sourcePath, checkpoint.LastRecordID)
	if err != nil || !ok {
		return checkpoint, ok, err
	}
	updated := checkpoint
	if strings.TrimSpace(updated.ID) == "" {
		updated.ID = transcriptCheckpointID(session.ID)
	}
	if strings.TrimSpace(updated.SessionID) == "" {
		updated.SessionID = session.ID
	}
	updated.SourcePath = sourcePath
	updated.LastSourceLine = position.Line
	updated.LastOffset = position.Offset
	updated.SourceSize = position.SourceSize
	updated.SourceModTime = position.SourceModTime
	if strings.TrimSpace(updated.Status) == "" {
		updated.Status = importCheckpointStatusComplete
	}
	applied := false
	if err := b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.ImportCheckpoints[updated.ID]
		if strings.TrimSpace(current.LastRecordID) != "" && strings.TrimSpace(current.LastRecordID) != strings.TrimSpace(checkpoint.LastRecordID) {
			updated = current
			return nil
		}
		if current.LastOffset == updated.LastOffset &&
			current.SourceSize == updated.SourceSize &&
			current.SourceModTime.Equal(updated.SourceModTime) &&
			current.LastSourceLine == updated.LastSourceLine {
			updated = current
			applied = true
			return nil
		}
		if strings.TrimSpace(current.ID) != "" {
			updated.UpdatedAt = current.UpdatedAt
			if strings.TrimSpace(updated.ImportTurnID) == "" {
				updated.ImportTurnID = current.ImportTurnID
			}
			if strings.TrimSpace(updated.KindPrefix) == "" {
				updated.KindPrefix = current.KindPrefix
			}
		} else {
			updated.UpdatedAt = checkpoint.UpdatedAt
		}
		state.ImportCheckpoints[updated.ID] = updated
		applied = true
		return nil
	}); err != nil {
		return checkpoint, false, err
	}
	return updated, applied, nil
}

func linkedTranscriptCheckpointIdleUnchanged(filePath string, checkpoint teamstore.ImportCheckpoint) bool {
	if checkpoint.Status != importCheckpointStatusComplete {
		return false
	}
	if strings.TrimSpace(checkpoint.LastRecordID) == "" {
		return false
	}
	if transcriptImportCheckpointNeedsBudgetedResume(checkpoint, filePath) {
		return false
	}
	return linkedCheckpointFileUnchanged(filePath, checkpoint)
}

func transcriptImportCheckpointNeedsBudgetedResume(checkpoint teamstore.ImportCheckpoint, sourcePath string) bool {
	if checkpoint.Status != importCheckpointStatusComplete {
		return false
	}
	if !strings.HasPrefix(strings.TrimSpace(checkpoint.ImportTurnID), "import-bg:") || strings.TrimSpace(checkpoint.KindPrefix) == "" {
		return false
	}
	if strings.TrimSpace(checkpoint.LastRecordID) == "" {
		return false
	}
	sourceSize, _ := transcriptSourceFileState(firstNonEmptyString(sourcePath, checkpoint.SourcePath))
	return sourceSize > 0
}

func (b *Bridge) resumeBudgetedTranscriptImport(ctx context.Context, session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint) error {
	sourcePath := strings.TrimSpace(firstNonEmptyString(local.FilePath, checkpoint.SourcePath))
	if sourcePath == "" {
		return nil
	}
	importTurnID := strings.TrimSpace(firstNonEmptyString(checkpoint.ImportTurnID, "import:"+session.ID))
	kindPrefix := strings.TrimSpace(firstNonEmptyString(checkpoint.KindPrefix, "import"))
	checkpointID := strings.TrimSpace(firstNonEmptyString(checkpoint.ID, transcriptCheckpointID(session.ID)))
	opts := transcriptImportRunOptions{QueueOnly: true, MaxBatches: transcriptImportMaxBatchesPerCycle}
	if err := b.markTranscriptImportStartedForRun(ctx, session, sourcePath, checkpointID, importTurnID, kindPrefix); err != nil {
		return err
	}
	result, err := b.importTranscriptRecordsToTeams(ctx, session, sourcePath, importTurnID, kindPrefix, checkpointID, opts)
	if err != nil {
		_ = b.markTranscriptImportFailedWithID(ctx, session, sourcePath, checkpointID)
		if isTranscriptCheckpointNotFoundError(err) {
			if sendErr := b.queueOrSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, kindPrefix+"-needs-attention", transcriptCheckpointNeedsAttentionMessage(), outboxQueueOptions{}, true); sendErr != nil {
				return sendErr
			}
			return nil
		}
		return err
	}
	if !result.Complete {
		return b.markTranscriptImportPausedAt(ctx, session, sourcePath, result.LastRecordID, result.LastLine, result.LastOffset, checkpointID, importTurnID, kindPrefix)
	}
	if checkpointID == transcriptCheckpointID(session.ID) {
		resumedLocal := local
		resumedLocal.FilePath = sourcePath
		if err := b.importSubagentMarkersToTeamsWithOptions(ctx, session, resumedLocal, importTurnID, opts); err != nil {
			_ = b.markTranscriptImportFailedWithID(ctx, session, sourcePath, checkpointID)
			return err
		}
	}
	complete := formatTranscriptImportCompleteMessage(result.Stats)
	if err := b.queueOrSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, kindPrefix+"-complete", complete, outboxQueueOptions{}, true); err != nil {
		_ = b.markTranscriptImportFailedWithID(ctx, session, sourcePath, checkpointID)
		return err
	}
	return b.markTranscriptImportCompleteAtEOFWithID(ctx, session, sourcePath, result.LastRecordID, result.LastLine, checkpointID)
}

func (b *Bridge) resumeInterruptedTranscriptImport(ctx context.Context, session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint) error {
	sourcePath := strings.TrimSpace(firstNonEmptyString(checkpoint.SourcePath, local.FilePath))
	if sourcePath == "" {
		return b.markTranscriptImportFailed(ctx, session, local.FilePath)
	}
	importTurnID := strings.TrimSpace(firstNonEmptyString(checkpoint.ImportTurnID, "import:"+session.ID))
	kindPrefix := strings.TrimSpace(firstNonEmptyString(checkpoint.KindPrefix, "import"))
	checkpointID := strings.TrimSpace(firstNonEmptyString(checkpoint.ID, transcriptCheckpointID(session.ID)))
	if err := b.markTranscriptImportStartedForRun(ctx, session, sourcePath, checkpointID, importTurnID, kindPrefix); err != nil {
		return err
	}
	result, err := b.importTranscriptRecordsToTeams(ctx, session, sourcePath, importTurnID, kindPrefix, checkpointID, transcriptImportRunOptions{})
	if err != nil {
		_ = b.markTranscriptImportFailedWithID(ctx, session, sourcePath, checkpointID)
		if isTranscriptCheckpointNotFoundError(err) {
			if sendErr := b.queueAndSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, kindPrefix+"-needs-attention", transcriptCheckpointNeedsAttentionMessage()); sendErr != nil {
				return sendErr
			}
			return nil
		}
		return err
	}
	if !result.Complete {
		return b.markTranscriptImportPausedAt(ctx, session, sourcePath, result.LastRecordID, result.LastLine, result.LastOffset, checkpointID, importTurnID, kindPrefix)
	}
	if checkpointID == transcriptCheckpointID(session.ID) {
		resumedLocal := local
		resumedLocal.FilePath = sourcePath
		if err := b.importSubagentMarkersToTeams(ctx, session, resumedLocal, importTurnID); err != nil {
			_ = b.markTranscriptImportFailedWithID(ctx, session, sourcePath, checkpointID)
			return err
		}
	}
	complete := formatTranscriptImportCompleteMessage(result.Stats)
	if err := b.queueAndSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, kindPrefix+"-complete", complete); err != nil {
		_ = b.markTranscriptImportFailedWithID(ctx, session, sourcePath, checkpointID)
		return err
	}
	if err := b.markTranscriptImportCompleteAtEOFWithID(ctx, session, sourcePath, result.LastRecordID, result.LastLine, checkpointID); err != nil {
		return err
	}
	return b.processQueuedTurns(ctx)
}

func (b *Bridge) recoverFailedTranscriptCheckpoint(ctx context.Context, session Session, local codexhistory.Session) (bool, error) {
	if b == nil || b.store == nil {
		return false, nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	checkpointID := transcriptCheckpointID(session.ID)
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.Status != importCheckpointStatusFailed {
		return false, nil
	}
	sourcePath := strings.TrimSpace(firstNonEmptyString(checkpoint.SourcePath, local.FilePath))
	if sourcePath == "" {
		return false, nil
	}
	if strings.TrimSpace(checkpoint.LastRecordID) != "" {
		transcript, err := ReadSessionTranscriptSince(sourcePath, checkpoint.LastRecordID)
		if err != nil {
			return false, err
		}
		if !transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
			return true, b.markTranscriptImportComplete(ctx, session, sourcePath, checkpoint.LastRecordID, checkpoint.LastSourceLine)
		}
	}
	if checkpoint.LastSourceLine <= 0 {
		return false, nil
	}
	transcript, err := ReadSessionTranscript(sourcePath)
	if err != nil {
		return false, err
	}
	var recovered TranscriptRecord
	for _, record := range transcript.Records {
		if record.SourceLine <= checkpoint.LastSourceLine {
			recovered = record
		}
		if strings.TrimSpace(checkpoint.LastRecordID) != "" &&
			(record.ItemID == checkpoint.LastRecordID || record.DedupeKey == checkpoint.LastRecordID) {
			recovered = record
			break
		}
	}
	if strings.TrimSpace(transcriptRecordCheckpointKey(recovered)) == "" {
		return false, nil
	}
	return true, b.markTranscriptImportComplete(ctx, session, sourcePath, transcriptRecordCheckpointKey(recovered), recovered.SourceLine)
}

func countVisibleTranscriptSyncRecords(state teamstore.State, session Session, local codexhistory.Session, records []TranscriptRecord, teamsOriginHashes map[string]bool, teamsOriginDisplays map[string]string, teamsOriginTerminalHashes map[string]bool, known *knownTranscriptOutboxDedupeState, recentTeamsMirror recentCompletedTeamsTranscriptMirrorSkipper) int {
	dedupe := newTranscriptDedupeState()
	visible := 0
	for i, record := range records {
		checkpointLine, checkpointOffset := transcriptCheckpointPositionForRecord(records, i)
		record.SourceLine = checkpointLine
		record.SourceOffset = checkpointOffset
		if strings.TrimSpace(record.Text) == "" {
			continue
		}
		if shouldSkipBackgroundTranscriptRecord(record) {
			continue
		}
		body := formatTranscriptRecordForTeams(record)
		body = teamsOriginTranscriptUserDisplayBody(record, body, teamsOriginDisplays)
		if strings.TrimSpace(body) == "" {
			continue
		}
		kind := transcriptRecordOutboxKind("sync", record, i+1)
		if transcriptDeliveryKnown(state, session, local, record, kind, body) {
			continue
		}
		if recentTeamsMirror.shouldSkip(record, body, teamsOriginHashes, known) ||
			shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginTerminalHashes) ||
			known.shouldSkip(record, body) ||
			dedupe.shouldSkip(record, body) {
			continue
		}
		visible++
	}
	return visible
}

func transcriptDeliveryKnown(state teamstore.State, session Session, local codexhistory.Session, record TranscriptRecord, kind string, body string) bool {
	base := transcriptDeliveryRecord(session, local, record, kind, body)
	if strings.TrimSpace(base.ID) == "" {
		return false
	}
	if _, ok := state.TranscriptDeliveries[base.ID]; ok {
		return true
	}
	prefix := base.ID + ":part:"
	for id := range state.TranscriptDeliveries {
		if strings.HasPrefix(id, prefix) {
			return true
		}
	}
	return false
}

func (b *Bridge) blockAutomaticTranscriptSync(ctx context.Context, session Session, sourcePath string, checkpoint teamstore.ImportCheckpoint, backlogRecords int) error {
	if err := b.markTranscriptImportBlocked(ctx, session, sourcePath, checkpoint); err != nil {
		return err
	}
	body := fmt.Sprintf("Local Codex history has %d visible new item(s), so I paused automatic history sync to avoid flooding this Teams chat.\n\nNo history was skipped. To import the backlog, send `helper publish-history` here.", backlogRecords)
	idSeed := firstNonEmptyString(checkpoint.LastRecordID, fmt.Sprintf("line-%d", checkpoint.LastSourceLine), "start")
	outboxID := "outbox:sync:" + session.ID + ":backlog-blocked:" + shortStableID(idSeed)
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          outboxID,
		SessionID:   session.ID,
		TurnID:      "sync:" + session.ID,
		TeamsChatID: session.ChatID,
		Kind:        "sync-status-backlog-blocked",
		Body:        body,
	})
}

func shortStableID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])[:12]
}

func transcriptImportIsActive(state teamstore.State, sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(sessionID)]
	return transcriptImportCheckpointIsActive(state, checkpoint)
}

func transcriptImportCheckpointIsActive(state teamstore.State, checkpoint teamstore.ImportCheckpoint) bool {
	return checkpoint.Status == importCheckpointStatusImporting && !transcriptImportCheckpointIsOrphaned(state, checkpoint)
}

func transcriptImportCheckpointIsOrphaned(state teamstore.State, checkpoint teamstore.ImportCheckpoint) bool {
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

func (b *Bridge) sessionTranscriptImportInProgress(ctx context.Context, sessionID string) (bool, error) {
	if b == nil || b.store == nil || strings.TrimSpace(sessionID) == "" {
		return false, nil
	}
	state, err := b.store.TranscriptImportStateSnapshot(ctx)
	if err != nil {
		return false, err
	}
	return transcriptImportIsActive(state, sessionID), nil
}

func teamsOriginTextHashes(state teamstore.State, sessionID string) map[string]bool {
	hashes := make(map[string]bool)
	for _, inbound := range state.InboundEvents {
		if inbound.SessionID != sessionID || inbound.TurnID == "" {
			continue
		}
		if !inboundSourceIsTeamsOrigin(inbound.Source) {
			continue
		}
		addTeamsOriginInboundTextHashes(hashes, inbound)
	}
	return hashes
}

func teamsOriginDisplayTexts(state teamstore.State, sessionID string) map[string]string {
	displays := make(map[string]string)
	for _, inbound := range state.InboundEvents {
		if inbound.SessionID != sessionID || inbound.TurnID == "" {
			continue
		}
		if !inboundSourceIsTeamsOrigin(inbound.Source) {
			continue
		}
		addTeamsOriginInboundDisplayTexts(displays, inbound)
	}
	return displays
}

func teamsOriginTerminalTextHashes(state teamstore.State, sessionID string) map[string]bool {
	hashes := make(map[string]bool)
	for _, inbound := range state.InboundEvents {
		if inbound.SessionID != sessionID || inbound.TurnID == "" {
			continue
		}
		if !inboundSourceIsTeamsOrigin(inbound.Source) {
			continue
		}
		turn, ok := state.Turns[inbound.TurnID]
		if ok && !teamsTurnStatusTerminal(turn.Status) {
			continue
		}
		addTeamsOriginInboundTextHashes(hashes, inbound)
	}
	return hashes
}

func teamsTurnStatusTerminal(status teamstore.TurnStatus) bool {
	switch status {
	case teamstore.TurnStatusCompleted, teamstore.TurnStatusFailed, teamstore.TurnStatusInterrupted:
		return true
	default:
		return false
	}
}

func inboundSourceIsTeamsOrigin(source string) bool {
	source = strings.ToLower(strings.TrimSpace(source))
	return source == "" || source == "teams" || strings.HasPrefix(source, "teams_")
}

func addTeamsOriginInboundTextHashes(hashes map[string]bool, inbound teamstore.InboundEvent) {
	if hashes == nil {
		return
	}
	if inbound.TextHash != "" {
		hashes[inbound.TextHash] = true
		return
	}
	if text := strings.TrimSpace(inbound.Text); text != "" {
		if hash := normalizedTextHash(text); hash != "" {
			hashes[hash] = true
		}
		return
	}
	for _, fallback := range []string{defaultReferencedTeamsMessagePrompt, defaultLocalAttachmentPrompt} {
		if hash := normalizedTextHash(fallback); hash != "" {
			hashes[hash] = true
		}
	}
}

func addTeamsOriginInboundDisplayTexts(displays map[string]string, inbound teamstore.InboundEvent) {
	if displays == nil {
		return
	}
	text := strings.TrimSpace(inbound.Text)
	if text != "" {
		addTeamsOriginDisplayText(displays, inbound.TextHash, text)
		addTeamsOriginDisplayText(displays, normalizedTextHash(text), text)
		return
	}
	for _, fallback := range []string{defaultReferencedTeamsMessagePrompt, defaultLocalAttachmentPrompt} {
		addTeamsOriginDisplayText(displays, normalizedTextHash(fallback), fallback)
	}
}

func addTeamsOriginDisplayText(displays map[string]string, hash string, text string) {
	hash = strings.TrimSpace(hash)
	text = strings.TrimSpace(text)
	if displays == nil || hash == "" || text == "" {
		return
	}
	if _, ok := displays[hash]; !ok {
		displays[hash] = text
	}
}

func shouldSkipTeamsOriginTranscriptRecord(record TranscriptRecord, body string, hashes map[string]bool) bool {
	if record.Kind != TranscriptKindUser {
		return false
	}
	for _, candidate := range teamsOriginTranscriptUserHashCandidates(body) {
		hash := normalizedTextHash(candidate)
		if hash != "" && hashes[hash] {
			return true
		}
	}
	return false
}

func teamsOriginTranscriptUserDisplayBody(record TranscriptRecord, body string, displays map[string]string) string {
	if record.Kind != TranscriptKindUser {
		return body
	}
	for _, candidate := range teamsOriginTranscriptUserCandidates(body) {
		hash := normalizedTextHash(candidate.Text)
		if display := strings.TrimSpace(displays[hash]); display != "" {
			if candidate.ASROnly && strings.EqualFold(strings.TrimSpace(candidate.Text), defaultLocalAttachmentPrompt) && strings.EqualFold(display, defaultLocalAttachmentPrompt) {
				return ""
			}
			return display
		}
	}
	return body
}

func teamsOriginTranscriptUserHashCandidates(body string) []string {
	normalized := teamsOriginTranscriptUserCandidates(body)
	var candidates []string
	seen := make(map[string]bool)
	for _, candidate := range normalized {
		text := strings.TrimSpace(candidate.Text)
		if text == "" || seen[text] {
			continue
		}
		seen[text] = true
		candidates = append(candidates, text)
	}
	return candidates
}

type teamsOriginTranscriptUserCandidate struct {
	Text    string
	ASROnly bool
}

func teamsOriginTranscriptUserCandidates(body string) []teamsOriginTranscriptUserCandidate {
	var candidates []teamsOriginTranscriptUserCandidate
	candidateSeen := make(map[string]bool)
	addCandidate := func(text string, asrOnly bool) {
		text = strings.TrimSpace(text)
		if text == "" {
			return
		}
		key := fmt.Sprintf("%t\x00%s", asrOnly, text)
		if candidateSeen[key] {
			return
		}
		candidateSeen[key] = true
		candidates = append(candidates, teamsOriginTranscriptUserCandidate{Text: text, ASROnly: asrOnly})
	}
	add := func(text string, asrOnly bool) {
		addCandidate(text, asrOnly)
		unwrapped := stripTeamsUserMessageEnvelope(text)
		addCandidate(unwrapped, asrOnly)
		cleaned := strings.TrimSpace(StripHelperPromptEchoes(StripArtifactManifestBlocks(text)))
		addCandidate(cleaned, asrOnly)
		addCandidate(stripTeamsUserMessageEnvelope(cleaned), asrOnly)
		addCandidate(StripHelperPromptEchoes(StripArtifactManifestBlocks(unwrapped)), asrOnly)
	}

	type variant struct {
		text    string
		asrOnly bool
	}
	var variants []variant
	variantSeen := make(map[string]bool)
	enqueue := func(text string, asrOnly bool) {
		text = strings.TrimSpace(text)
		if text == "" {
			return
		}
		key := fmt.Sprintf("%t\x00%s", asrOnly, text)
		if variantSeen[key] {
			return
		}
		variantSeen[key] = true
		variants = append(variants, variant{text: text, asrOnly: asrOnly})
	}

	enqueue(body, false)
	for i := 0; i < len(variants); i++ {
		current := variants[i]
		add(current.text, current.asrOnly)
		unwrapped := stripTeamsUserMessageEnvelope(current.text)
		enqueue(unwrapped, current.asrOnly)
		cleaned := strings.TrimSpace(StripHelperPromptEchoes(StripArtifactManifestBlocks(current.text)))
		enqueue(cleaned, current.asrOnly)
		enqueue(stripTeamsUserMessageEnvelope(cleaned), current.asrOnly)
		enqueue(StripHelperPromptEchoes(StripArtifactManifestBlocks(unwrapped)), current.asrOnly)
		enqueue(stripCodexImagePlaceholders(current.text), current.asrOnly)
		enqueue(stripReferencedTeamsMessagePromptSection(current.text), current.asrOnly)
		enqueue(stripLocalAttachmentPromptSection(current.text), current.asrOnly)
		if stripped, ok := stripASRTranscriptPromptSection(current.text); ok {
			if stripped == "" {
				enqueue(defaultLocalAttachmentPrompt, true)
			} else {
				enqueue(stripped, current.asrOnly)
			}
		}
	}
	return candidates
}

func stripASRTranscriptPromptSection(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", false
	}
	if strings.HasPrefix(text, teamsASRTranscriptPromptLead) {
		return "", true
	}
	for _, prefix := range []string{"\r\n\r\n", "\n\n", "\r\n", "\n"} {
		if idx := strings.Index(text, prefix+teamsASRTranscriptPromptLead); idx >= 0 {
			return strings.TrimSpace(text[:idx]), true
		}
	}
	return text, false
}

func stripCodexImagePlaceholders(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	inImageBlock := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if inImageBlock {
			if strings.EqualFold(trimmed, "</image>") {
				inImageBlock = false
			}
			continue
		}
		if isCodexImagePlaceholderStart(trimmed) {
			if !strings.Contains(trimmed, "</image>") {
				inImageBlock = true
			}
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func isCodexImagePlaceholderStart(line string) bool {
	if !strings.HasPrefix(line, "<image ") || !strings.HasSuffix(line, ">") {
		return false
	}
	line = strings.ToLower(line)
	return strings.Contains(line, "name=[image #") ||
		strings.Contains(line, `name="image #`) ||
		strings.Contains(line, `name='image #`)
}

func stripLocalAttachmentPromptSection(text string) string {
	const marker = "Attached files saved locally for this turn:"
	if idx := strings.Index(text, "\n\n"+marker); idx >= 0 {
		return strings.TrimSpace(text[:idx])
	}
	if strings.HasPrefix(strings.TrimSpace(text), marker) {
		return ""
	}
	for _, lineBreak := range []string{"\n", "\r\n"} {
		if idx := strings.Index(text, lineBreak+marker); idx >= 0 {
			return strings.TrimSpace(text[:idx])
		}
	}
	return strings.TrimSpace(text)
}

func stripReferencedTeamsMessagePromptSection(text string) string {
	const marker = "Referenced Teams message"
	if idx := strings.Index(text, "\n\n"+marker); idx >= 0 {
		return strings.TrimSpace(text[:idx])
	}
	if strings.HasPrefix(strings.TrimSpace(text), marker) {
		return ""
	}
	for _, lineBreak := range []string{"\n", "\r\n"} {
		if idx := strings.Index(text, lineBreak+marker); idx >= 0 {
			return strings.TrimSpace(text[:idx])
		}
	}
	return strings.TrimSpace(text)
}

func knownTranscriptOutboxHashes(state teamstore.State, sessionID string) map[TranscriptKind]map[string]bool {
	return knownTranscriptOutboxHashesSince(state, sessionID, time.Time{})
}

func knownTranscriptOutboxHashesSince(state teamstore.State, sessionID string, since time.Time) map[TranscriptKind]map[string]bool {
	hashes := make(map[TranscriptKind]map[string]bool)
	addHash := func(kind TranscriptKind, hash string) {
		hash = strings.TrimSpace(hash)
		if kind == "" || hash == "" {
			return
		}
		if hashes[kind] == nil {
			hashes[kind] = make(map[string]bool)
		}
		hashes[kind][hash] = true
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.SessionID != sessionID || strings.TrimSpace(outbox.Body) == "" {
			continue
		}
		if !outboxCanDedupeTranscript(outbox) {
			continue
		}
		kind, ok := deliveredOutboxTranscriptKind(outbox.Kind)
		if !ok {
			continue
		}
		if !transcriptKnownDeliveryInDedupeWindow(state, sessionID, outbox.TurnID, outbox.CreatedAt, since, kind) {
			continue
		}
		hash := normalizedTextHash(formatKnownOutboxBodyForTranscriptDedupe(kind, outbox.Body))
		if hash != "" {
			addHash(kind, hash)
		}
		if outbox.SourceTextHash != "" {
			addHash(kind, outbox.SourceTextHash)
		}
	}
	for _, delivery := range state.HelperDeliveries {
		if delivery.SessionID != sessionID {
			continue
		}
		if delivery.OutboxID != "" {
			if _, ok := state.OutboxMessages[delivery.OutboxID]; ok {
				continue
			}
		}
		if !helperDeliveryCanDedupeTranscript(delivery) {
			continue
		}
		kind, ok := helperDeliveryTranscriptKind(delivery)
		if !ok {
			continue
		}
		if !transcriptKnownDeliveryInDedupeWindow(state, sessionID, delivery.TurnID, delivery.CreatedAt, since, kind) {
			continue
		}
		addHash(kind, delivery.SourceTextHash)
		addHash(kind, delivery.VisibleHash)
		addHash(kind, delivery.RenderedHash)
	}
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID != sessionID {
			continue
		}
		if delivery.OutboxID != "" {
			if _, ok := state.OutboxMessages[delivery.OutboxID]; ok {
				continue
			}
		}
		if !transcriptDeliveryCanDedupeByText(delivery) {
			continue
		}
		kind, ok := deliveredOutboxTranscriptKind(delivery.Kind)
		if !ok || kind == TranscriptKindUser || kind == TranscriptKindCompact {
			continue
		}
		if !transcriptDeliveryTextDedupeInWindow(delivery, since, kind) {
			continue
		}
		addHash(kind, delivery.TextHash)
	}
	return hashes
}

type knownTranscriptOutboxDedupeState struct {
	hashes        map[TranscriptKind]map[string]bool
	compactCounts map[string]int
}

func newKnownTranscriptOutboxDedupeState(state teamstore.State, sessionID string, since time.Time) *knownTranscriptOutboxDedupeState {
	hashes := knownTranscriptOutboxHashesSince(state, sessionID, since)
	compactCounts := make(map[string]int)
	for hash := range hashes[TranscriptKindCompact] {
		compactCounts[hash] = 0
	}
	addCompactCount := func(hash string) {
		hash = strings.TrimSpace(hash)
		if hash != "" {
			compactCounts[hash]++
		}
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.SessionID != sessionID || strings.TrimSpace(outbox.Body) == "" {
			continue
		}
		if !outboxCanDedupeTranscript(outbox) {
			continue
		}
		kind, ok := deliveredOutboxTranscriptKind(outbox.Kind)
		if !ok || kind != TranscriptKindCompact {
			continue
		}
		if !transcriptKnownDeliveryInDedupeWindow(state, sessionID, outbox.TurnID, outbox.CreatedAt, since, kind) {
			continue
		}
		hash := normalizedTextHash(formatKnownOutboxBodyForTranscriptDedupe(kind, outbox.Body))
		if hash == "" {
			hash = outbox.SourceTextHash
		}
		addCompactCount(hash)
	}
	for _, delivery := range state.HelperDeliveries {
		if delivery.SessionID != sessionID {
			continue
		}
		if delivery.OutboxID != "" {
			if _, ok := state.OutboxMessages[delivery.OutboxID]; ok {
				continue
			}
		}
		if !helperDeliveryCanDedupeTranscript(delivery) {
			continue
		}
		kind, ok := helperDeliveryTranscriptKind(delivery)
		if !ok || kind != TranscriptKindCompact {
			continue
		}
		if !transcriptKnownDeliveryInDedupeWindow(state, sessionID, delivery.TurnID, delivery.CreatedAt, since, kind) {
			continue
		}
		addCompactCount(firstNonEmptyString(delivery.SourceTextHash, delivery.VisibleHash, delivery.RenderedHash))
	}
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID != sessionID {
			continue
		}
		if delivery.OutboxID != "" {
			if _, ok := state.OutboxMessages[delivery.OutboxID]; ok {
				continue
			}
		}
		if !transcriptDeliveryCanDedupeByText(delivery) || delivery.Status == teamstore.TranscriptDeliveryStatusSkipped {
			continue
		}
		kind, ok := deliveredOutboxTranscriptKind(delivery.Kind)
		if !ok || kind != TranscriptKindCompact {
			continue
		}
		if !transcriptDeliveryTextDedupeInWindow(delivery, since, kind) {
			continue
		}
		addCompactCount(delivery.TextHash)
	}
	return &knownTranscriptOutboxDedupeState{hashes: hashes, compactCounts: compactCounts}
}

func transcriptKnownDeliveryInDedupeWindow(state teamstore.State, sessionID string, turnID string, createdAt time.Time, since time.Time, kind TranscriptKind) bool {
	if since.IsZero() || createdAt.IsZero() || !createdAt.Before(since) {
		return true
	}
	switch kind {
	case TranscriptKindAssistant, TranscriptKindStatus, TranscriptKindCompact:
		return transcriptKnownTurnOverlapsSince(state, sessionID, turnID, since) ||
			since.Sub(createdAt) <= localTranscriptCompletedTurnSettleWindow
	default:
		return false
	}
}

func transcriptKnownTurnOverlapsSince(state teamstore.State, sessionID string, turnID string, since time.Time) bool {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" || since.IsZero() {
		return false
	}
	turn, ok := state.Turns[turnID]
	if !ok || strings.TrimSpace(turn.SessionID) != strings.TrimSpace(sessionID) {
		return false
	}
	threshold := since.Add(-localTranscriptCompletedTurnSettleWindow)
	for _, at := range []time.Time{turn.CompletedAt, turn.UpdatedAt, turn.StartedAt, turn.CreatedAt} {
		if !at.IsZero() && !at.Before(threshold) {
			return true
		}
	}
	return false
}

func transcriptDeliveryCanDedupeByText(delivery teamstore.TranscriptDeliveryRecord) bool {
	if strings.TrimSpace(delivery.TextHash) == "" {
		return false
	}
	switch delivery.Status {
	case teamstore.TranscriptDeliveryStatusSent, teamstore.TranscriptDeliveryStatusSkipped:
		return true
	case teamstore.TranscriptDeliveryStatusAccepted:
		return strings.TrimSpace(delivery.TeamsMessageID) != ""
	default:
		return false
	}
}

func transcriptDeliveryTextDedupeInWindow(delivery teamstore.TranscriptDeliveryRecord, since time.Time, kind TranscriptKind) bool {
	if since.IsZero() || delivery.CreatedAt.IsZero() || !delivery.CreatedAt.Before(since) {
		return true
	}
	if kind == TranscriptKindStatus {
		return true
	}
	return since.Sub(delivery.CreatedAt) <= localTranscriptCompletedTurnSettleWindow
}

func (s *knownTranscriptOutboxDedupeState) shouldSkip(record TranscriptRecord, body string) bool {
	if s == nil {
		return false
	}
	if record.Kind != TranscriptKindCompact {
		return shouldSkipKnownTranscriptOutboxRecord(record, body, s.hashes)
	}
	hash := normalizedTextHash(body)
	if hash == "" {
		return false
	}
	count := s.compactCounts[hash]
	if count <= 0 {
		return false
	}
	s.compactCounts[hash] = count - 1
	return true
}

func outboxCanDedupeTranscript(outbox teamstore.OutboxMessage) bool {
	switch outbox.Status {
	case teamstore.OutboxStatusQueued, teamstore.OutboxStatusSending, teamstore.OutboxStatusAccepted, teamstore.OutboxStatusSent:
		return true
	default:
		return false
	}
}

func helperDeliveryCanDedupeTranscript(delivery teamstore.HelperDeliveryRecord) bool {
	switch delivery.Status {
	case teamstore.HelperDeliveryStatusQueued, teamstore.HelperDeliveryStatusSending, teamstore.HelperDeliveryStatusAccepted, teamstore.HelperDeliveryStatusSent:
		return true
	default:
		return false
	}
}

func helperDeliveryTranscriptKind(delivery teamstore.HelperDeliveryRecord) (TranscriptKind, bool) {
	switch strings.TrimSpace(delivery.KindFamily) {
	case "compact":
		return TranscriptKindCompact, true
	case "status":
		return TranscriptKindStatus, true
	default:
		return deliveredOutboxTranscriptKind(delivery.Kind)
	}
}

func deliveredOutboxTranscriptKind(kind string) (TranscriptKind, bool) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch {
	case kind == "user" || strings.HasPrefix(kind, "user-") || strings.Contains(kind, "-user-"):
		return TranscriptKindUser, true
	case isFinalOutboxKind(kind) || strings.Contains(kind, "assistant"):
		return TranscriptKindAssistant, true
	case strings.Contains(kind, "compact"):
		return TranscriptKindCompact, true
	case strings.Contains(kind, "progress") || strings.Contains(kind, "status"):
		return TranscriptKindStatus, true
	default:
		return "", false
	}
}

func formatKnownOutboxBodyForTranscriptDedupe(kind TranscriptKind, body string) string {
	body = StripHelperPromptEchoes(StripArtifactManifestBlocks(body))
	if kind == TranscriptKindAssistant {
		body = StripOAIMemoryCitationBlocks(body)
	}
	return body
}

func shouldSkipKnownTranscriptOutboxRecord(record TranscriptRecord, body string, hashes map[TranscriptKind]map[string]bool) bool {
	if record.Kind != TranscriptKindUser && record.Kind != TranscriptKindAssistant && record.Kind != TranscriptKindStatus && record.Kind != TranscriptKindCompact {
		return false
	}
	if record.Kind == TranscriptKindUser {
		for _, candidate := range teamsOriginTranscriptUserHashCandidates(body) {
			hash := normalizedTextHash(candidate)
			if hash != "" && hashes[record.Kind][hash] {
				return true
			}
		}
		return false
	}
	hash := normalizedTextHash(body)
	return hash != "" && hashes[record.Kind][hash]
}

func shouldSkipBackgroundTranscriptRecord(record TranscriptRecord) bool {
	return record.Kind == TranscriptKindTool
}

func transcriptSyncOutboxOptions(record TranscriptRecord) outboxQueueOptions {
	if record.Kind != TranscriptKindAssistant {
		return outboxQueueOptions{}
	}
	return outboxQueueOptions{
		MentionOwner:     true,
		NotificationKind: "turn_completed",
	}
}

type transcriptDedupeState struct {
	seenSourceText        map[string]bool
	seenNonUserTextByKind map[string]bool
	seenModelOutputText   map[string]bool
	previousKind          TranscriptKind
	previousTextHash      string
	previousSourceLine    int
}

func newTranscriptDedupeState() *transcriptDedupeState {
	return &transcriptDedupeState{
		seenSourceText:        make(map[string]bool),
		seenNonUserTextByKind: make(map[string]bool),
		seenModelOutputText:   make(map[string]bool),
	}
}

func (s *transcriptDedupeState) shouldSkip(record TranscriptRecord, body string) bool {
	if s == nil {
		return false
	}
	hash := normalizedTextHash(body)
	if hash == "" {
		return false
	}
	previousKind := s.previousKind
	previousTextHash := s.previousTextHash
	previousSourceLine := s.previousSourceLine
	defer s.rememberPrevious(record, hash)
	sourceKey := transcriptRecordSourceDedupeKey(record)
	if sourceKey != "" {
		key := sourceKey + "\x00" + string(record.Kind) + "\x00" + hash
		if s.seenSourceText[key] {
			return true
		}
	}
	if record.Kind == TranscriptKindUser {
		if previousKind == TranscriptKindUser &&
			previousTextHash == hash &&
			transcriptUserDuplicateIsAdjacent(record.SourceLine, previousSourceLine) {
			return true
		}
		if sourceKey != "" {
			s.seenSourceText[sourceKey+"\x00"+string(record.Kind)+"\x00"+hash] = true
		}
		return false
	}
	if record.Kind == TranscriptKindCompact {
		if sourceKey != "" {
			s.seenSourceText[sourceKey+"\x00"+string(record.Kind)+"\x00"+hash] = true
		}
		return false
	}
	if transcriptKindIsModelOutput(record.Kind) {
		if s.seenModelOutputText[hash] {
			return true
		}
	}
	key := string(record.Kind) + "\x00" + hash
	if s.seenNonUserTextByKind[key] {
		return true
	}
	if sourceKey != "" {
		s.seenSourceText[sourceKey+"\x00"+string(record.Kind)+"\x00"+hash] = true
	}
	if transcriptKindIsModelOutput(record.Kind) {
		s.seenModelOutputText[hash] = true
	}
	s.seenNonUserTextByKind[string(record.Kind)+"\x00"+hash] = true
	return false
}

func (s *transcriptDedupeState) rememberPrevious(record TranscriptRecord, hash string) {
	if s == nil || hash == "" {
		return
	}
	s.previousKind = record.Kind
	s.previousTextHash = hash
	s.previousSourceLine = record.SourceLine
}

func transcriptUserDuplicateIsAdjacent(currentLine int, previousLine int) bool {
	if currentLine <= 0 || previousLine <= 0 {
		return false
	}
	return currentLine >= previousLine && currentLine-previousLine <= 3
}

func transcriptRecordSourceDedupeKey(record TranscriptRecord) string {
	return firstNonEmptyString(record.DedupeKey, record.SourceItemID, record.ItemID)
}

func transcriptKindIsModelOutput(kind TranscriptKind) bool {
	return kind == TranscriptKindAssistant || kind == TranscriptKindStatus
}

func transcriptCheckpointID(sessionID string) string {
	return "transcript:" + strings.TrimSpace(sessionID)
}

func (b *Bridge) recordTranscriptCheckpoint(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int) error {
	return b.recordTranscriptCheckpointWithID(ctx, session, sourcePath, lastRecordID, lastLine, transcriptCheckpointID(session.ID))
}

func (b *Bridge) recordTranscriptCheckpointWithID(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, checkpointID string) error {
	return b.recordTranscriptCheckpointDetailedWithID(ctx, session, sourcePath, lastRecordID, lastLine, 0, checkpointID)
}

func (b *Bridge) recordTranscriptCheckpointDetailed(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, lastOffset int64) error {
	return b.recordTranscriptCheckpointDetailedWithID(ctx, session, sourcePath, lastRecordID, lastLine, lastOffset, transcriptCheckpointID(session.ID))
}

func (b *Bridge) recordTranscriptCheckpointDetailedWithID(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, lastOffset int64, checkpointID string) error {
	if strings.TrimSpace(lastRecordID) == "" {
		return nil
	}
	if strings.TrimSpace(checkpointID) == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	sourceSize, sourceModTime := transcriptSourceFileState(sourcePath)
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		id := checkpointID
		previous := state.ImportCheckpoints[id]
		status := previous.Status
		if status == "" || status == importCheckpointStatusBlocked {
			status = importCheckpointStatusComplete
		}
		state.ImportCheckpoints[id] = teamstore.ImportCheckpoint{
			ID:             id,
			SessionID:      session.ID,
			SourcePath:     sourcePath,
			LastRecordID:   lastRecordID,
			LastSourceLine: lastLine,
			LastOffset:     firstNonZeroInt64(lastOffset, previous.LastOffset),
			SourceSize:     sourceSize,
			SourceModTime:  sourceModTime,
			ImportTurnID:   previous.ImportTurnID,
			KindPrefix:     previous.KindPrefix,
			Status:         status,
			UpdatedAt:      now,
		}
		ledgerID := transcriptLedgerID(session.ID, id, lastRecordID)
		state.TranscriptLedger[ledgerID] = teamstore.TranscriptLedgerRecord{
			ID:             ledgerID,
			SessionID:      session.ID,
			CodexThreadID:  session.CodexThreadID,
			SourcePath:     sourcePath,
			SourceLine:     lastLine,
			SourceRecordID: lastRecordID,
			ImportedAt:     now,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		return nil
	})
}

func transcriptSourceFileState(sourcePath string) (int64, time.Time) {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return 0, time.Time{}
	}
	info, err := os.Stat(sourcePath)
	if err != nil || info.IsDir() {
		return 0, time.Time{}
	}
	return info.Size(), info.ModTime()
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func transcriptLedgerID(sessionID string, checkpointID string, recordID string) string {
	parentCheckpointID := transcriptCheckpointID(sessionID)
	if checkpointID == "" || checkpointID == parentCheckpointID {
		return "ledger:" + sessionID + ":" + recordID
	}
	sum := sha256.Sum256([]byte(checkpointID + "\x00" + recordID))
	return "ledger:" + sessionID + ":" + hex.EncodeToString(sum[:])[:24]
}

func (b *Bridge) markTranscriptImportStarted(ctx context.Context, session Session, sourcePath string) error {
	return b.markTranscriptImportStartedWithID(ctx, session, sourcePath, transcriptCheckpointID(session.ID))
}

func (b *Bridge) markTranscriptImportStartedWithID(ctx context.Context, session Session, sourcePath string, checkpointID string) error {
	return b.markTranscriptImportStartedForRun(ctx, session, sourcePath, checkpointID, "", "")
}

func (b *Bridge) markTranscriptImportStartedForRun(ctx context.Context, session Session, sourcePath string, checkpointID string, importTurnID string, kindPrefix string) error {
	if strings.TrimSpace(checkpointID) == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		id := checkpointID
		checkpoint := state.ImportCheckpoints[id]
		checkpoint.ID = id
		checkpoint.SessionID = session.ID
		checkpoint.SourcePath = sourcePath
		if strings.TrimSpace(importTurnID) != "" {
			checkpoint.ImportTurnID = strings.TrimSpace(importTurnID)
		}
		if strings.TrimSpace(kindPrefix) != "" {
			checkpoint.KindPrefix = strings.TrimSpace(kindPrefix)
		}
		checkpoint.Status = importCheckpointStatusImporting
		checkpoint.UpdatedAt = now
		state.ImportCheckpoints[id] = checkpoint
		return nil
	})
}

func (b *Bridge) markTranscriptImportComplete(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int) error {
	return b.markTranscriptImportCompleteWithID(ctx, session, sourcePath, lastRecordID, lastLine, transcriptCheckpointID(session.ID))
}

func (b *Bridge) markTranscriptImportCompleteAtEOF(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int) error {
	return b.markTranscriptImportCompleteAtEOFWithID(ctx, session, sourcePath, lastRecordID, lastLine, transcriptCheckpointID(session.ID))
}

func (b *Bridge) markTranscriptImportCompleteWithID(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, checkpointID string) error {
	return b.markTranscriptImportCompleteDetailedWithID(ctx, session, sourcePath, lastRecordID, lastLine, checkpointID, false)
}

func (b *Bridge) markTranscriptImportCompleteAtEOFWithID(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, checkpointID string) error {
	return b.markTranscriptImportCompleteDetailedWithID(ctx, session, sourcePath, lastRecordID, lastLine, checkpointID, true)
}

func (b *Bridge) markTranscriptImportPausedAt(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, lastOffset int64, checkpointID string, importTurnID string, kindPrefix string) error {
	if strings.TrimSpace(lastRecordID) == "" {
		return nil
	}
	if strings.TrimSpace(checkpointID) == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	sourceSize, sourceModTime := transcriptSourceFileState(sourcePath)
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		id := checkpointID
		checkpoint := state.ImportCheckpoints[id]
		checkpoint.ID = id
		checkpoint.SessionID = session.ID
		checkpoint.SourcePath = sourcePath
		checkpoint.LastRecordID = lastRecordID
		checkpoint.LastSourceLine = lastLine
		checkpoint.LastOffset = lastOffset
		checkpoint.SourceSize = sourceSize
		checkpoint.SourceModTime = sourceModTime
		if strings.TrimSpace(importTurnID) != "" {
			checkpoint.ImportTurnID = strings.TrimSpace(importTurnID)
		}
		if strings.TrimSpace(kindPrefix) != "" {
			checkpoint.KindPrefix = strings.TrimSpace(kindPrefix)
		}
		checkpoint.Status = importCheckpointStatusComplete
		checkpoint.UpdatedAt = now
		state.ImportCheckpoints[id] = checkpoint
		return nil
	})
}

func (b *Bridge) markTranscriptImportCompleteDetailedWithID(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, checkpointID string, completeEOF bool) error {
	if strings.TrimSpace(checkpointID) == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	sourceSize, sourceModTime := transcriptSourceFileState(sourcePath)
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		id := checkpointID
		checkpoint := state.ImportCheckpoints[id]
		checkpoint.ID = id
		checkpoint.SessionID = session.ID
		checkpoint.SourcePath = sourcePath
		if strings.TrimSpace(lastRecordID) != "" {
			checkpoint.LastRecordID = lastRecordID
			checkpoint.LastSourceLine = lastLine
		}
		checkpoint.SourceSize = sourceSize
		checkpoint.SourceModTime = sourceModTime
		if completeEOF && sourceSize > 0 {
			checkpoint.LastOffset = sourceSize
			checkpoint.ImportTurnID = ""
			checkpoint.KindPrefix = ""
		}
		checkpoint.Status = importCheckpointStatusComplete
		checkpoint.UpdatedAt = now
		state.ImportCheckpoints[id] = checkpoint
		return nil
	})
}

func (b *Bridge) markTranscriptImportFailed(ctx context.Context, session Session, sourcePath string) error {
	return b.markTranscriptImportFailedWithID(ctx, session, sourcePath, transcriptCheckpointID(session.ID))
}

func (b *Bridge) markTranscriptImportFailedWithID(ctx context.Context, session Session, sourcePath string, checkpointID string) error {
	if strings.TrimSpace(checkpointID) == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		id := checkpointID
		checkpoint := state.ImportCheckpoints[id]
		checkpoint.ID = id
		checkpoint.SessionID = session.ID
		checkpoint.SourcePath = sourcePath
		checkpoint.Status = importCheckpointStatusFailed
		checkpoint.UpdatedAt = now
		state.ImportCheckpoints[id] = checkpoint
		return nil
	})
}

func (b *Bridge) markTranscriptImportBlocked(ctx context.Context, session Session, sourcePath string, previous teamstore.ImportCheckpoint) error {
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		id := transcriptCheckpointID(session.ID)
		checkpoint := state.ImportCheckpoints[id]
		if strings.TrimSpace(checkpoint.LastRecordID) == "" {
			checkpoint.LastRecordID = previous.LastRecordID
			checkpoint.LastSourceLine = previous.LastSourceLine
		}
		checkpoint.ID = id
		checkpoint.SessionID = session.ID
		checkpoint.SourcePath = sourcePath
		checkpoint.Status = importCheckpointStatusBlocked
		checkpoint.UpdatedAt = now
		state.ImportCheckpoints[id] = checkpoint
		return nil
	})
}

func (b *Bridge) formatWorkspaceDashboard(ctx context.Context) (string, error) {
	projects, err := b.discoverDashboardProjects(ctx)
	if err != nil {
		return "", err
	}
	previous := b.previousControlDashboard(ctx)
	if !dashboardWorkspaceVisibleInProjects(projects, previous.SelectedWorkspaceID) {
		previous.SelectedWorkspaceID = ""
	}
	dashboard := BuildControlDashboard(previous, ControlDashboardInput{
		Workspaces: dashboardWorkspacesFromProjects(projects),
		ViewKind:   DashboardViewWorkspaces,
	}, time.Now())
	if err := b.persistControlDashboard(ctx, dashboard); err != nil {
		return "", err
	}
	if len(dashboard.Workspaces) == 0 {
		return "## 📁 Workspaces\n\nNo local Codex workspaces found on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.\n\n**Next:** send `new <directory>` to create a Work chat for a directory.", nil
	}
	lines := []string{
		"## 📁 Workspaces",
		"",
		"Reply with a number to open a workspace.",
		"",
		"---",
	}
	workspaces := make(map[string]DashboardWorkspace, len(dashboard.Workspaces))
	for _, workspace := range dashboard.Workspaces {
		workspaces[workspace.ID] = workspace
	}
	for _, item := range dashboard.CurrentView.Items {
		workspace, ok := workspaces[item.WorkspaceID]
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("**%d. %s**", workspace.Number, dashboardWorkspaceListTitle(workspace)))
		body := []string{
			dashboardWorkspaceListPathLine(workspace),
			dashboardWorkspaceListMeta(workspace),
		}
		if dashboardAbsolutePath(workspace.Path) == "" {
			body = append(body, "Older Codex records without a working directory are grouped here.")
		}
		body = append(body,
			"",
			fmt.Sprintf("**Next:** send `%d` or `p %d` to open this workspace", workspace.Number, workspace.Number),
		)
		lines = append(lines, dashboardIndentedTextLines(body...)...)
		lines = append(lines, "---")
	}
	lines = append(lines, "", "**Other options:**", "- Send a workspace number, for example `1`, to open it.", "- `n <directory>` or `new <directory>` - create a new Work chat directly.")
	lines = append(lines, "", "Numbers apply to your next reply and expire after 10 minutes. If a number looks wrong, send `projects` again.")
	return strings.Join(lines, "\n"), nil
}

func dashboardWorkspaceListTitle(workspace DashboardWorkspace) string {
	if dashboardAbsolutePath(workspace.Path) == "" {
		return "Unknown workspace"
	}
	if title := strings.TrimSpace(workspace.DisplayTitle); title != "" {
		return title
	}
	return WorkspaceDisplayTitle("", workspace.Path)
}

func dashboardWorkspaceListPathLine(workspace DashboardWorkspace) string {
	if abs := dashboardAbsolutePath(workspace.Path); abs != "" {
		return dashboardInlineCode(abs)
	}
	return "Path not recorded by Codex"
}

func dashboardWorkspaceListMeta(workspace DashboardWorkspace) string {
	if workspace.SessionCount <= 0 {
		return "Sessions: none"
	}
	meta := fmt.Sprintf("%d active, %d idle", workspace.ActiveSessionCount, workspace.IdleSessionCount)
	if !workspace.UpdatedAt.IsZero() {
		meta += ", last updated " + workspace.UpdatedAt.Local().Format("2006-01-02 15:04")
	}
	return "Sessions: " + meta
}

func dashboardIndentedTextLines(lines ...string) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			out = append(out, "")
			continue
		}
		out = append(out, "  "+line)
	}
	return out
}

func dashboardWorkspacePathDisplay(path string) string {
	if abs := dashboardAbsolutePath(path); abs != "" {
		return dashboardInlineCode(abs)
	}
	return "**Unknown workspace**\n   Path: not recorded by Codex"
}

func dashboardAbsolutePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) {
		return clean
	}
	if abs, err := filepath.Abs(clean); err == nil {
		return filepath.Clean(abs)
	}
	return clean
}

func dashboardInlineCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "`") {
		value = strings.ReplaceAll(value, "`", "'")
	}
	return "`" + value + "`"
}

func dashboardWorkspaceByID(workspaces []DashboardWorkspace, id string) DashboardWorkspace {
	id = strings.TrimSpace(id)
	for _, workspace := range workspaces {
		if workspace.ID == id {
			return workspace
		}
	}
	return DashboardWorkspace{}
}

func (b *Bridge) formatWorkspaceSessionsDashboard(ctx context.Context, target DashboardCommandTarget) (string, error) {
	projects, err := b.discoverDashboardProjects(ctx)
	if err != nil {
		return "", fmt.Errorf("workspace discovery failed: %w", err)
	}
	selectedWorkspaceID := ""
	if target.IsNumber {
		selection, err := b.resolveDashboardTarget(ctx, target.Number)
		if err != nil {
			return "", err
		}
		if selection.Kind != DashboardSelectionWorkspace {
			return "", fmt.Errorf("number %d is not a workspace in the current dashboard view", target.Number)
		}
		selectedWorkspaceID = selection.WorkspaceID
	} else if strings.TrimSpace(target.Raw) != "" {
		selectedWorkspaceID = workspaceIDForPath(target.Raw)
	} else if view, ok, err := b.loadDashboardView(ctx); err != nil {
		return "", err
	} else if ok && view.WorkspaceID != "" {
		if dashboardWorkspaceVisibleInProjects(projects, view.WorkspaceID) {
			selectedWorkspaceID = view.WorkspaceID
		}
	}

	previous := b.previousControlDashboard(ctx)
	if !dashboardWorkspaceVisibleInProjects(projects, previous.SelectedWorkspaceID) {
		previous.SelectedWorkspaceID = ""
	}
	dashboard := BuildControlDashboard(previous, ControlDashboardInput{
		Workspaces:          dashboardWorkspacesFromProjects(projects),
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: selectedWorkspaceID,
	}, time.Now())
	if err := b.persistControlDashboard(ctx, dashboard); err != nil {
		return "", err
	}
	if len(dashboard.CurrentView.Items) == 0 {
		selectedWorkspace := dashboardWorkspaceByID(dashboard.Workspaces, dashboard.SelectedWorkspaceID)
		if selectedWorkspace.ID != "" {
			return fmt.Sprintf("## Sessions\n\nWorkspace: %s\n\nNo local Codex sessions were found in this workspace on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.\n\n**Next:** send `new` to create a new Work chat in this workspace.", dashboardWorkspacePathDisplay(selectedWorkspace.Path)), nil
		}
		return "## Sessions\n\nNo local Codex sessions found on this machine.\n\n**Next:** send `projects` to choose a workspace.\n\nTip: send `new <directory>` to create a new Work chat.", nil
	}
	selectedWorkspace := dashboardWorkspaceByID(dashboard.Workspaces, dashboard.SelectedWorkspaceID)
	sessions := make(map[string]DashboardSession, len(dashboard.Sessions))
	for _, session := range dashboard.Sessions {
		sessions[sessionKey(session.WorkspaceID, session.ID)] = session
	}
	heading := "Sessions"
	workspaceLine := ""
	if selectedWorkspace.ID != "" {
		heading = "Sessions"
		workspaceLine = "Workspace: " + dashboardWorkspacePathDisplay(selectedWorkspace.Path)
	}
	lines := []string{
		"## " + heading,
		"",
		workspaceLine,
		"",
		"Reply with a number to continue a session in Teams.",
		"",
		"---",
	}
	if strings.TrimSpace(workspaceLine) == "" {
		lines = []string{
			"## " + heading,
			"",
			"Reply with a number to continue a session in Teams.",
			"",
			"---",
		}
	}
	workspaceByID := make(map[string]DashboardWorkspace, len(dashboard.Workspaces))
	for _, workspace := range dashboard.Workspaces {
		workspaceByID[workspace.ID] = workspace
	}
	displayed := 0
	for _, item := range dashboard.CurrentView.Items {
		session, ok := sessions[sessionKey(item.WorkspaceID, item.SessionID)]
		if !ok {
			continue
		}
		action := fmt.Sprintf("send `%d` or `c %d` to continue this session in Teams", item.Number, item.Number)
		if linked := b.linkedSessionForLocalSessionID(session.ID); linked != nil {
			if isActiveSessionStatus(linked.Status) {
				action = fmt.Sprintf("send `%d` to open this Teams chat or import updates", item.Number)
			} else {
				action = fmt.Sprintf("send `%d` to create a new Work chat from this closed Teams chat", item.Number)
			}
		}
		lines = append(lines, fmt.Sprintf("**%d. %s**", item.Number, session.DisplayTitle))
		pathLine := "Path not recorded by Codex"
		if workspace, ok := workspaceByID[item.WorkspaceID]; ok {
			pathLine = dashboardWorkspaceListPathLine(workspace)
		} else if selectedWorkspace.ID != "" {
			pathLine = dashboardWorkspaceListPathLine(selectedWorkspace)
		}
		body := []string{pathLine}
		if meta := dashboardSessionListMeta(session); meta != "" {
			body = append(body, meta)
		}
		body = append(body, "", "**Next:** "+action)
		lines = append(lines, dashboardIndentedTextLines(body...)...)
		lines = append(lines, "---")
		displayed++
	}
	lines = append(lines, "", "Need debug IDs? Send `details <number>`. Numbers expire after one reply.", "", "**Next:** send `new` to create a new Work chat in this workspace.")
	return strings.Join(lines, "\n"), nil
}

func dashboardSessionListMeta(session DashboardSession) string {
	status := "idle"
	if isActiveSessionStatus(session.Status) {
		status = "active"
	}
	if session.UpdatedAt.IsZero() {
		return "Session: " + status + ", update time not recorded"
	}
	return "Session: " + status + ", last updated " + session.UpdatedAt.Local().Format("2006-01-02 15:04")
}

func (b *Bridge) previousControlDashboard(ctx context.Context) ControlDashboard {
	if err := b.ensureStore(); err != nil {
		return ControlDashboard{}
	}
	chatID := strings.TrimSpace(b.reg.ControlChatID)
	if chatID == "" {
		return ControlDashboard{}
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return ControlDashboard{}
	}
	view, _ := dashboardViewFromRecord(state.DashboardViews[chatID])
	previous := ControlDashboard{
		SelectedWorkspaceID: view.WorkspaceID,
		CurrentView:         view,
	}
	for _, record := range state.DashboardNumbers {
		if record.ChatID != chatID || record.Number <= 0 {
			continue
		}
		switch DashboardSelectionKind(record.Kind) {
		case DashboardSelectionWorkspace:
			previous.Workspaces = append(previous.Workspaces, DashboardWorkspace{
				Number:       record.Number,
				ID:           record.WorkspaceID,
				DisplayTitle: record.Label,
			})
		case DashboardSelectionSession:
			previous.Sessions = append(previous.Sessions, DashboardSession{
				Number:       record.Number,
				ID:           record.SessionID,
				WorkspaceID:  record.WorkspaceID,
				DisplayTitle: record.Label,
			})
		}
	}
	if len(previous.Workspaces) == 0 && len(previous.Sessions) == 0 {
		for _, item := range view.Items {
			switch item.Kind {
			case DashboardSelectionWorkspace:
				previous.Workspaces = append(previous.Workspaces, DashboardWorkspace{
					Number:       item.Number,
					ID:           item.WorkspaceID,
					DisplayTitle: item.DisplayTitle,
				})
			case DashboardSelectionSession:
				previous.Sessions = append(previous.Sessions, DashboardSession{
					Number:       item.Number,
					ID:           item.SessionID,
					WorkspaceID:  item.WorkspaceID,
					DisplayTitle: item.DisplayTitle,
				})
			}
		}
	}
	return previous
}

func (b *Bridge) formatOpenControlTarget(ctx context.Context, target DashboardCommandTarget) (string, error) {
	if target.IsNumber {
		selection, err := b.resolveDashboardTarget(ctx, target.Number)
		if err != nil {
			return "", err
		}
		if selection.Kind != DashboardSelectionSession {
			return "", fmt.Errorf("number %d is a workspace in the current dashboard view; send `project %d` to list its sessions", target.Number, target.Number)
		}
		return b.formatSessionSelection(ctx, selection)
	}
	if session := b.linkedSessionForLocalSessionID(target.Raw); session != nil {
		resumed, err := b.resumeWorkChatIfParked(ctx, session)
		if err != nil {
			return "", err
		}
		return b.formatOpenSessionMessage(session, resumed), nil
	}
	if b.localCodexSessionExists(ctx, target.Raw) {
		return b.localSessionNotInTeamsMessage(0, strings.TrimSpace(target.Raw)), nil
	}
	return b.formatOpenSession(target.Raw), nil
}

func (b *Bridge) resumeWorkChatIfParked(ctx context.Context, session *Session) (bool, error) {
	if b == nil || b.store == nil || session == nil || !isActiveSessionStatus(session.Status) || strings.TrimSpace(session.ChatID) == "" {
		return false, nil
	}
	poll, ok, err := b.store.ChatPoll(ctx, session.ChatID)
	if err != nil {
		return false, err
	}
	if !ok || poll.PollState != inboundPollStateParked {
		return false, nil
	}
	if err := b.resumeWorkChat(ctx, session, time.Now()); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Bridge) resumeWorkChat(ctx context.Context, session *Session, now time.Time) error {
	if session == nil {
		return fmt.Errorf("session is required")
	}
	if err := b.ensureStore(); err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now()
	}
	if _, err := b.store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:                session.ChatID,
		PollState:             inboundPollStateHot,
		PreviousPollState:     "",
		NextPollAt:            now,
		LastActivityAt:        now,
		ClearBlockedUntil:     true,
		ClearContinuationPath: true,
		ResetFailures:         true,
	}); err != nil {
		return err
	}
	session.UpdatedAt = now
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	if err := b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.Sessions[session.ID]
		current.UpdatedAt = now
		state.Sessions[session.ID] = current
		return nil
	}); err != nil {
		return err
	}
	if err := b.sendWorkChatResumeNotice(ctx, *session, now); err != nil {
		return err
	}
	b.boostPolling(now)
	return nil
}

func (b *Bridge) resumeParkedWorkChat(ctx context.Context, arg string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(arg))
	if key == "" {
		return "", fmt.Errorf("usage: `r <resume-key>`")
	}
	var match *Session
	for i := range b.reg.Sessions {
		session := &b.reg.Sessions[i]
		if !isActiveSessionStatus(session.Status) {
			continue
		}
		if strings.EqualFold(session.ID, key) || strings.EqualFold(resumeKeyForSession(*session), key) {
			match = session
			break
		}
	}
	if match == nil {
		return "", fmt.Errorf("resume key not found: %s", key)
	}
	now := time.Now()
	if err := b.resumeWorkChat(ctx, match, now); err != nil {
		return "", err
	}
	if match.ChatURL != "" {
		return fmt.Sprintf("Resumed %s.\n\nOpen Work chat: %s\n\nMessages in that chat will be read again.", match.ID, match.ChatURL), nil
	}
	return fmt.Sprintf("Resumed %s.\n\nMessages in that Work chat will be read again.", match.ID), nil
}

func (b *Bridge) sendWorkChatResumeNotice(ctx context.Context, session Session, resumedAt time.Time) error {
	chatID := strings.TrimSpace(session.ChatID)
	if chatID == "" {
		return nil
	}
	if resumedAt.IsZero() {
		resumedAt = time.Now()
	}
	return b.queueAndBestEffortSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          resumeNoticeOutboxID(session, resumedAt),
		SessionID:   session.ID,
		TeamsChatID: chatID,
		Kind:        "helper",
		Body:        "This chat has been resumed. Messages sent here will be read again.",
	})
}

func resumeNoticeOutboxID(session Session, resumedAt time.Time) string {
	stamp := resumedAt.UTC().Format("20060102T150405.000000000Z")
	return "outbox:resume-notice:" + strings.TrimSpace(session.ID) + ":" + stamp
}

func (b *Bridge) resolveControlSelection(ctx context.Context, target DashboardCommandTarget) (string, error) {
	if !target.IsNumber {
		return "", ErrDashboardNotBareNumber
	}
	selection, err := b.resolveDashboardTarget(ctx, target.Number)
	if err != nil {
		_ = b.clearControlDashboardView(context.Background())
		return "", err
	}
	switch selection.Kind {
	case DashboardSelectionWorkspace:
		return b.formatWorkspaceSessionsDashboard(ctx, DashboardCommandTarget{Number: selection.Number, IsNumber: true})
	case DashboardSelectionSession:
		message, err := b.publishCodexSessionWithProgress(ctx, DashboardCommandTarget{Raw: strconvItoa(selection.Number), Number: selection.Number, IsNumber: true}, b.sendControl)
		_ = b.clearControlDashboardView(context.Background())
		return message, err
	default:
		_ = b.clearControlDashboardView(context.Background())
		return "", ErrDashboardNumberMissing
	}
}

func (b *Bridge) resolveDashboardTarget(ctx context.Context, number int) (DashboardSelection, error) {
	view, ok, err := b.loadDashboardView(ctx)
	if err != nil {
		return DashboardSelection{}, err
	}
	if !ok {
		return DashboardSelection{}, ErrDashboardViewMissing
	}
	selection, err := ResolveDashboardNumber(ChatScopeControl, view, number, time.Now())
	if err != nil {
		return DashboardSelection{}, err
	}
	if !b.dashboardSelectionStillVisible(ctx, selection) {
		return DashboardSelection{}, ErrDashboardNumberMissing
	}
	return selection, nil
}

func (b *Bridge) dashboardSelectionStillVisible(ctx context.Context, selection DashboardSelection) bool {
	projects, err := b.discoverDashboardProjects(ctx)
	if err != nil {
		return false
	}
	switch selection.Kind {
	case DashboardSelectionWorkspace:
		return dashboardWorkspaceVisibleInProjects(projects, selection.WorkspaceID)
	case DashboardSelectionSession:
		for _, project := range projects {
			for _, session := range project.Sessions {
				if workspaceIDForPath(dashboardSessionWorkspacePath(project, session)) == selection.WorkspaceID && session.SessionID == selection.SessionID {
					return true
				}
			}
		}
	default:
		return false
	}
	return false
}

func dashboardWorkspaceVisibleInProjects(projects []codexhistory.Project, workspaceID string) bool {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return false
	}
	for _, workspace := range dashboardWorkspacesFromProjects(projects) {
		if dashboardWorkspaceID(workspace) == workspaceID {
			return true
		}
	}
	return false
}

func (b *Bridge) formatSessionSelection(ctx context.Context, selection DashboardSelection) (string, error) {
	session := b.linkedSessionForLocalSessionID(selection.SessionID)
	if session == nil {
		return b.localSessionNotInTeamsMessage(selection.Number, selection.SessionID), nil
	}
	if !isActiveSessionStatus(session.Status) {
		return fmt.Sprintf("This Codex session has a closed Teams work chat. The helper no longer polls that chat.\nNext: send `continue %d` to create a new work chat and continue from local history.\nUse `details %d` to show technical IDs.", selection.Number, selection.Number), nil
	}
	resumed, err := b.resumeWorkChatIfParked(ctx, session)
	if err != nil {
		return "", err
	}
	return b.formatOpenSessionMessage(session, resumed), nil
}

func (b *Bridge) linkedSessionForLocalSessionID(sessionID string) *Session {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if session := b.reg.SessionByCodexThreadID(sessionID); session != nil {
		return session
	}
	return b.reg.SessionByID(sessionID)
}

func (b *Bridge) localCodexSessionExists(ctx context.Context, sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	projects, err := b.discoverDashboardProjects(ctx)
	if err != nil {
		return false
	}
	_, _, ok := findCodexSession(projects, sessionID)
	return ok
}

func (b *Bridge) localSessionNotInTeamsMessage(number int, _ string) string {
	if number > 0 {
		return fmt.Sprintf("This local Codex session is not in Teams yet.\nNext: send `%d` or `continue %d` to create a Work chat and import its history.\nUse `details %d` to show technical IDs.", number, number, number)
	}
	return "This local Codex session is not in Teams yet.\nNext: send `sessions`, then choose its number to create a Work chat and import its history.\nUse `details <number>` from the sessions list to show technical IDs."
}

func isActiveSessionStatus(status string) bool {
	status = strings.TrimSpace(status)
	return status == "" || status == string(teamstore.SessionStatusActive)
}

func (b *Bridge) persistControlDashboard(ctx context.Context, dashboard ControlDashboard) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	chatID := strings.TrimSpace(b.reg.ControlChatID)
	if chatID == "" {
		return nil
	}
	view := dashboard.CurrentView
	items := make([]teamstore.DashboardViewItem, 0, len(view.Items))
	for _, item := range view.Items {
		items = append(items, teamstore.DashboardViewItem{
			Number:      item.Number,
			Kind:        string(item.Kind),
			WorkspaceID: item.WorkspaceID,
			SessionID:   item.SessionID,
			Label:       item.DisplayTitle,
		})
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		if state.DashboardNumbers == nil {
			state.DashboardNumbers = make(map[string]teamstore.DashboardNumberRecord)
		}
		if state.Workspaces == nil {
			state.Workspaces = make(map[string]teamstore.WorkspaceRecord)
		}
		now := time.Now()
		state.DashboardViews[chatID] = teamstore.DashboardViewRecord{
			ID:          "dashboard:" + chatID,
			ChatID:      chatID,
			Kind:        string(view.Kind),
			WorkspaceID: view.WorkspaceID,
			Items:       items,
			ExpiresAt:   view.ExpiresAt,
			CreatedAt:   view.CreatedAt,
			UpdatedAt:   now,
		}
		for _, workspace := range dashboard.Workspaces {
			id := dashboardNumberRecordID(chatID, DashboardSelectionWorkspace, workspace.ID, "")
			state.DashboardNumbers[id] = teamstore.DashboardNumberRecord{
				ID:          id,
				ChatID:      chatID,
				Kind:        string(DashboardSelectionWorkspace),
				Number:      workspace.Number,
				WorkspaceID: workspace.ID,
				Label:       workspace.DisplayTitle,
				UpdatedAt:   now,
			}
			record := state.Workspaces[workspace.ID]
			record.ID = workspace.ID
			record.Path = workspace.Path
			record.Label = workspace.DisplayTitle
			record.Number = workspace.Number
			if record.CreatedAt.IsZero() {
				record.CreatedAt = now
			}
			record.UpdatedAt = now
			state.Workspaces[workspace.ID] = record
		}
		for _, session := range dashboard.Sessions {
			id := dashboardNumberRecordID(chatID, DashboardSelectionSession, session.WorkspaceID, session.ID)
			state.DashboardNumbers[id] = teamstore.DashboardNumberRecord{
				ID:          id,
				ChatID:      chatID,
				Kind:        string(DashboardSelectionSession),
				Number:      session.Number,
				WorkspaceID: session.WorkspaceID,
				SessionID:   session.ID,
				Label:       session.DisplayTitle,
				UpdatedAt:   now,
			}
		}
		return nil
	})
}

func (b *Bridge) clearControlDashboardView(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	chatID := strings.TrimSpace(b.reg.ControlChatID)
	if chatID == "" {
		return nil
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		delete(state.DashboardViews, chatID)
		return nil
	})
}

func dashboardNumberRecordID(chatID string, kind DashboardSelectionKind, workspaceID string, sessionID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(chatID) + "\x00" + string(kind) + "\x00" + strings.TrimSpace(workspaceID) + "\x00" + strings.TrimSpace(sessionID)))
	return "dashboard-number:" + hex.EncodeToString(sum[:])[:24]
}

func (b *Bridge) loadDashboardView(ctx context.Context) (DashboardView, bool, error) {
	if err := b.ensureStore(); err != nil {
		return DashboardView{}, false, err
	}
	chatID := strings.TrimSpace(b.reg.ControlChatID)
	if chatID == "" {
		return DashboardView{}, false, nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return DashboardView{}, false, err
	}
	view, ok := dashboardViewFromRecord(state.DashboardViews[chatID])
	if !ok {
		return DashboardView{}, false, nil
	}
	return view, true, nil
}

func dashboardViewFromRecord(record teamstore.DashboardViewRecord) (DashboardView, bool) {
	if record.ChatID == "" && record.ID == "" {
		return DashboardView{}, false
	}
	items := make([]DashboardViewItem, 0, len(record.Items))
	workspaceID := strings.TrimSpace(record.WorkspaceID)
	for _, item := range record.Items {
		kind := DashboardSelectionKind(item.Kind)
		if workspaceID == "" && item.WorkspaceID != "" {
			workspaceID = item.WorkspaceID
		}
		items = append(items, DashboardViewItem{
			Number:       item.Number,
			Kind:         kind,
			WorkspaceID:  item.WorkspaceID,
			SessionID:    item.SessionID,
			DisplayTitle: item.Label,
		})
	}
	return DashboardView{
		Kind:        DashboardViewKind(record.Kind),
		WorkspaceID: workspaceID,
		Items:       items,
		CreatedAt:   record.CreatedAt,
		ExpiresAt:   record.ExpiresAt,
	}, true
}

type dashboardWorkspaceProjectAccumulator struct {
	input       DashboardWorkspaceInput
	seenSession map[string]bool
}

func dashboardWorkspacesFromProjects(projects []codexhistory.Project) []DashboardWorkspaceInput {
	accumulators := map[string]*dashboardWorkspaceProjectAccumulator{}
	order := make([]string, 0, len(projects))
	workspaceForPath := func(path string) *dashboardWorkspaceProjectAccumulator {
		path = strings.TrimSpace(path)
		id := workspaceIDForPath(path)
		acc := accumulators[id]
		if acc != nil {
			if acc.input.Path == "" && path != "" {
				acc.input.Path = path
			}
			return acc
		}
		acc = &dashboardWorkspaceProjectAccumulator{
			input: DashboardWorkspaceInput{
				ID:   id,
				Path: path,
			},
			seenSession: map[string]bool{},
		}
		accumulators[id] = acc
		order = append(order, id)
		return acc
	}

	for _, project := range projects {
		if len(project.Sessions) == 0 {
			_ = workspaceForPath(project.Path)
			continue
		}
		for _, session := range project.Sessions {
			workspacePath := dashboardSessionWorkspacePath(project, session)
			acc := workspaceForPath(workspacePath)
			if acc.seenSession[session.SessionID] {
				continue
			}
			acc.seenSession[session.SessionID] = true
			acc.input.Sessions = append(acc.input.Sessions, DashboardSessionInput{
				ID:            session.SessionID,
				WorkspaceID:   acc.input.ID,
				Cwd:           workspacePath,
				Topic:         session.DisplayTitle(),
				Status:        "local",
				CodexThreadID: session.SessionID,
				CreatedAt:     session.CreatedAt,
				UpdatedAt:     session.ModifiedAt,
			})
			acc.input.UpdatedAt = laterDashboardTime(acc.input.UpdatedAt, session.ModifiedAt)
		}
	}
	workspaces := make([]DashboardWorkspaceInput, 0, len(accumulators))
	for _, id := range order {
		workspaces = append(workspaces, accumulators[id].input)
	}
	return workspaces
}

func dashboardSessionWorkspacePath(project codexhistory.Project, session codexhistory.Session) string {
	return firstNonEmptyString(session.ProjectPath, project.Path)
}

func findCodexSession(projects []codexhistory.Project, sessionID string) (codexhistory.Session, codexhistory.Project, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return codexhistory.Session{}, codexhistory.Project{}, false
	}
	for _, project := range projects {
		for _, session := range project.Sessions {
			if session.SessionID == sessionID {
				return session, project, true
			}
		}
	}
	return codexhistory.Session{}, codexhistory.Project{}, false
}

func latestProjectModified(project codexhistory.Project) time.Time {
	var latest time.Time
	for _, session := range project.Sessions {
		latest = laterDashboardTime(latest, session.ModifiedAt)
	}
	return latest
}

func laterDashboardTime(current time.Time, candidate time.Time) time.Time {
	if candidate.After(current) {
		return candidate
	}
	return current
}

func workspaceIDForPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "workspace:unknown"
	}
	sum := CacheSourceFingerprint(path)
	if len(sum) > 16 {
		sum = sum[:16]
	}
	return "workspace:" + sum
}

func splitTextChunks(text string, limit int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return []string{""}
	}
	if limit <= 0 {
		limit = 6000
	}
	var chunks []string
	for len([]rune(text)) > limit {
		runes := []rune(text)
		cut := limit
		for i := limit; i > limit/2; i-- {
			if runes[i] == '\n' || runes[i] == ' ' {
				cut = i
				break
			}
		}
		chunks = append(chunks, strings.TrimSpace(string(runes[:cut])))
		text = strings.TrimSpace(string(runes[cut:]))
	}
	if text != "" {
		chunks = append(chunks, text)
	}
	return chunks
}

func splitTextChunksForHTMLMessage(prefix string, text string, limitBytes int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return []string{""}
	}
	if limitBytes <= len(HTMLMessage(prefix, "")) {
		limitBytes = teamsChunkHTMLContentBytes
	}
	runes := []rune(text)
	var chunks []string
	for len(runes) > 0 {
		remaining := strings.TrimSpace(string(runes))
		if len(HTMLMessage(prefix, remaining)) <= limitBytes {
			chunks = append(chunks, remaining)
			break
		}
		best := 0
		low, high := 1, len(runes)
		for low <= high {
			mid := low + (high-low)/2
			candidate := strings.TrimSpace(string(runes[:mid]))
			if candidate == "" {
				low = mid + 1
				continue
			}
			if len(HTMLMessage(prefix, candidate)) <= limitBytes {
				best = mid
				low = mid + 1
			} else {
				high = mid - 1
			}
		}
		if best <= 0 {
			best = 1
		}
		cut := best
		for i := best; i > best/2; i-- {
			if runes[i-1] != '\n' && runes[i-1] != ' ' {
				continue
			}
			candidate := strings.TrimSpace(string(runes[:i]))
			if candidate != "" && len(HTMLMessage(prefix, candidate)) <= limitBytes {
				cut = i
				break
			}
		}
		chunk := strings.TrimSpace(string(runes[:cut]))
		if chunk == "" {
			chunk = string(runes[:best])
			cut = best
		}
		chunks = append(chunks, chunk)
		runes = []rune(strings.TrimSpace(string(runes[cut:])))
	}
	return chunks
}

func controlCommandErrorMessage(err error) string {
	var providerNotConfigured beacon.ProviderCommandNotConfiguredError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrDashboardViewExpired):
		return "This numbered list expired. Send `projects` or `sessions` again, then choose one of the newly shown numbers."
	case errors.Is(err, ErrDashboardViewMissing):
		return "I do not have a current list yet. Send `projects` or `sessions` first, then choose a number."
	case errors.Is(err, ErrDashboardNumberMissing):
		return "That number is not in the current list. Send `projects` or `sessions` again, then choose one of the newly shown numbers."
	case errors.As(err, &providerNotConfigured):
		return beacon.ProviderAdapterConfigurationNotice(providerNotConfigured).Render()
	default:
		return err.Error()
	}
}

func serviceControlBlockedMessage(control teamstore.ServiceControl, target string) string {
	state := "paused"
	if control.Draining {
		state = "draining"
	}
	message := "Teams bridge is " + state + "; " + target + " are not being accepted."
	if control.Reason != "" {
		message += " Reason: " + control.Reason + "."
	}
	return message
}
