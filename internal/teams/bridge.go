package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	pollCursorOverlap         = 2 * time.Minute
	fastPollInterval          = time.Second
	fastPollDuration          = 90 * time.Second
	transcriptSyncMinInterval = 30 * time.Second
	dashboardProjectsCacheTTL = 30 * time.Second

	// Automatic transcript sync is for small local Codex CLI catch-up. Large
	// history import must stay explicit, otherwise helper startup can flood a
	// Teams chat and delay inbound polling.
	transcriptSyncMaxRecordsPerSessionPerCycle = 8
	transcriptSyncMaxAutoBacklogRecords        = 80
	transcriptImportBatchSeparatorHTML         = "<p>&nbsp;</p>"

	// Live Graph chat sends in this tenant failed at 102,290 bytes of HTML
	// body content. Split well below that to leave room for Teams-side changes,
	// part labels, and encoding surprises.
	safeTeamsHTMLContentBytes  = 80 * 1024
	teamsChunkHTMLContentBytes = 72 * 1024
)

var ownerMentionLongTurnThreshold = time.Minute
var discoverCodexProjectsForTeams = codexhistory.DiscoverProjectsContext
var helperRestartDelay = 3 * time.Second
var codexIdleStatusInitialDelay = 2 * time.Minute
var codexIdleStatusRepeatDelay = 5 * time.Minute
var codexIdleStatusMessage = "Still working. No new Codex update yet."

const controlFallbackSessionID = "__control_fallback__"

const (
	importCheckpointStatusImporting = "importing"
	importCheckpointStatusComplete  = "complete"
	importCheckpointStatusFailed    = "failed"
	importCheckpointStatusBlocked   = "blocked"
)

type outboxQueueOptions struct {
	MentionOwner     bool
	NotificationKind string
}

type BridgeOptions struct {
	RegistryPath             string
	StorePath                string
	Store                    *teamstore.Store
	HelperVersion            string
	OwnerStaleAfter          time.Duration
	Interval                 time.Duration
	Once                     bool
	Top                      int
	MaxWorkChatPollsPerCycle int
	Executor                 Executor
	ControlFallbackExecutor  Executor
	ControlFallbackModel     string
	Runner                   codexrunner.Runner
	HelperRestarter          HelperRestarter
	HelperReloader           HelperReloader
}

type HelperRestarter func(context.Context) error

type HelperReloader func(context.Context, HelperReloadOptions) error

type HelperReloadOptions struct {
	Force         bool
	BeforeRestart func(context.Context) error
}

type Bridge struct {
	graph                     *GraphClient
	readGraph                 *GraphClient
	fileGraph                 *GraphClient
	httpClient                *http.Client
	registryPath              string
	reg                       Registry
	user                      User
	scope                     teamstore.ScopeIdentity
	machine                   teamstore.MachineRecord
	lease                     teamstore.ControlLease
	leaseDuration             time.Duration
	out                       io.Writer
	executor                  Executor
	controlFallbackExecutor   Executor
	controlFallbackModel      string
	helperRestarter           HelperRestarter
	helperReloader            HelperReloader
	store                     *teamstore.Store
	asyncTurns                bool
	ownerMu                   sync.Mutex
	owner                     teamstore.OwnerMetadata
	ownerStaleAfter           time.Duration
	ownerHeartbeatInterval    time.Duration
	pollMu                    sync.Mutex
	fastPollUntil             time.Time
	lastTranscriptSync        time.Time
	maxWorkChatPollsPerCycle  int
	dashboardProjectsMu       sync.Mutex
	dashboardProjectsCache    []codexhistory.Project
	dashboardProjectsCachedAt time.Time
	annotateUserMessages      bool
	annotationDisabled        bool
	annotationWarned          bool
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
		registryPath, err = DefaultRegistryPathForScope(scope.ID)
		if err != nil {
			return nil, err
		}
	}
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		return nil, err
	}
	reg.UserID = user.ID
	reg.UserPrincipal = user.UserPrincipalName
	httpClient := graph.httpClient()
	return &Bridge{graph: graph, readGraph: readGraph, httpClient: httpClient, registryPath: registryPath, reg: reg, user: user, scope: scope, machine: MachineRecordForUser(user, scope), out: out}, nil
}

func (b *Bridge) readClient() *GraphClient {
	if b != nil && b.readGraph != nil {
		return b.readGraph
	}
	return b.graph
}

func (b *Bridge) EnsureControlChat(ctx context.Context) (Chat, error) {
	if err := b.migrateRegistryProjectionToStore(ctx); err != nil {
		return Chat{}, err
	}
	if err := b.restoreRegistryFromStore(ctx); err != nil {
		return Chat{}, err
	}
	if b.reg.ControlChatID != "" {
		desiredTopic := ControlChatTitle(ChatTitleOptions{MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()), Profile: b.scope.Profile})
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

func (b *Bridge) createMeetingChat(ctx context.Context, topic string) (Chat, error) {
	if b == nil || b.graph == nil {
		return Chat{}, fmt.Errorf("Teams Graph client is not configured")
	}
	return b.graph.CreateMeetingChat(ctx, topic)
}

func (b *Bridge) sendChatCreatedMention(ctx context.Context, sessionID string, chatID string, text string) error {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil
	}
	text = strings.TrimSpace(text)
	if text == "" {
		text = "Teams chat created."
	}
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:               directOutboxID(chatID, "chat-created", text),
		SessionID:        sessionID,
		TeamsChatID:      chatID,
		Kind:             "chat-created",
		Body:             text,
		MentionOwner:     true,
		NotificationKind: "chat_created",
	})
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
	return SaveRegistry(b.registryPath, b.reg)
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
			storePath, err = DefaultStorePathForScope(b.scope.ID)
			if err != nil {
				return err
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
	b.controlFallbackExecutor = opts.ControlFallbackExecutor
	b.controlFallbackModel = strings.TrimSpace(opts.ControlFallbackModel)
	b.helperRestarter = opts.HelperRestarter
	b.helperReloader = opts.HelperReloader
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
		return err
	}
	ownerHeartbeatCtx, cancelOwnerHeartbeat := context.WithCancel(ctx)
	ownerHeartbeatDone := b.startOwnerHeartbeat(ownerHeartbeatCtx)
	stopOwnerHeartbeat := func() {
		cancelOwnerHeartbeat()
		if ownerHeartbeatDone != nil {
			<-ownerHeartbeatDone
			ownerHeartbeatDone = nil
		}
	}
	defer func() {
		stopOwnerHeartbeat()
		b.clearOwnerIfSame(context.Background())
		_, _ = b.store.ReleaseControlLeaseIfHolder(context.Background(), b.machine.ID, b.currentLeaseGeneration())
	}()
	if err := b.recoverUnfinishedTurns(ctx); err != nil {
		return err
	}
	chat, err := b.EnsureControlChat(ctx)
	if err != nil {
		return err
	}
	if b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams control chat: %s\n", chat.WebURL)
		_, _ = fmt.Fprintln(b.out, "Listening. Send `help`, `p`, or `n <directory>` in the control chat.")
	}
	for {
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
		if err := b.recordOwnerHeartbeat(ctx, "", ""); err != nil {
			return err
		}
		if err := b.flushPendingOutbox(ctx, "", ""); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams outbox flush error: %v\n", err)
		}
		if err := b.syncLinkedTranscriptsIfDue(ctx, time.Now()); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams transcript sync error: %v\n", err)
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
		if err := b.pollOnce(ctx, opts.Top); err != nil {
			if b.out != nil {
				_, _ = fmt.Fprintf(b.out, "Teams poll error: %v\n", err)
			}
		}
		if err := b.Save(); err != nil {
			return err
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

func (b *Bridge) pollOnce(ctx context.Context, top int) error {
	if b.reg.ControlChatID == "" {
		if _, err := b.EnsureControlChat(ctx); err != nil {
			return err
		}
	}
	state, err := b.store.Load(ctx)
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
		if err := b.persistInboundPollDecision(ctx, controlDecision); err != nil {
			return err
		}
	} else {
		controlHandled, err := b.pollChatWithRoleState(ctx, b.reg.ControlChatID, top, inboundPollRoleControl, false, controlPoll, hasControlPoll, b.handleControlMessage)
		if err != nil {
			return err
		}
		if controlHandled {
			return nil
		}
	}

	state, err = b.store.Load(ctx)
	if err != nil {
		return err
	}
	runningBySession := runningPollSessions(state)
	var decisions []inboundPollDecision
	pollsByChat := make(map[string]teamstore.ChatPollState)
	hasPollByChat := make(map[string]bool)
	for _, session := range b.reg.ActiveSessions() {
		if transcriptImportIsActive(state, session.ID) {
			continue
		}
		poll, hasPoll := state.ChatPolls[session.ChatID]
		pollsByChat[session.ChatID] = poll
		hasPollByChat[session.ChatID] = hasPoll
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
			if err := b.parkIdleWorkChat(ctx, session, decision); err != nil {
				return err
			}
			continue
		}
		if !decision.Due {
			if err := b.persistInboundPollDecision(ctx, decision); err != nil {
				return err
			}
			continue
		}
		decisions = append(decisions, decision)
	}
	sortInboundPollDecisions(decisions)
	if limit := b.effectiveMaxWorkChatPollsPerCycle(); len(decisions) > limit {
		decisions = decisions[:limit]
	}
	var firstErr error
	for _, decision := range decisions {
		session := b.reg.SessionByChatID(decision.ChatID)
		if session == nil {
			continue
		}
		s := session
		if _, err := b.pollChatWithRoleState(ctx, s.ChatID, top, inboundPollRoleWork, runningBySession[s.ID], pollsByChat[s.ChatID], hasPollByChat[s.ChatID], func(ctx context.Context, msg ChatMessage, text string) error {
			return b.handleSessionMessage(ctx, s.ChatID, msg, text)
		}); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	return firstErr
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
	if seeded && strings.TrimSpace(poll.ContinuationPath) != "" {
		window, err = b.readClient().ListMessagesWindowFromPath(ctx, poll.ContinuationPath)
	} else {
		window, err = b.readClient().ListMessagesWindow(ctx, chatID, top, modifiedAfter)
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
		updated, err := b.store.RecordChatPollSuccessWithContinuation(ctx, chatID, maxModified, true, windowFull, len(msgs), "")
		if err != nil {
			return false, err
		}
		err = b.scheduleChatAfterPoll(ctx, chatID, role, running, updated, time.Time{})
		return false, err
	}
	handled := false
	var activityAt time.Time
	for _, msg := range msgs {
		ignore, err := b.shouldIgnoreMessage(ctx, chatID, msg)
		if err != nil {
			_ = b.store.RecordChatPollError(ctx, chatID, err.Error())
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
		b.annotateIncomingUserMessage(ctx, chatID, msg)
		if b.currentLeaseGeneration() > 0 {
			if err := b.ensureActiveControlLease(ctx); err != nil {
				_ = b.store.RecordChatPollError(ctx, chatID, err.Error())
				return handled, err
			}
		}
		if err := handle(ctx, msg, text); err != nil {
			_ = b.store.RecordChatPollError(ctx, chatID, err.Error())
			return handled, err
		}
		handled = true
		activityAt = latestTime(activityAt, time.Now(), messageSortTime(msg))
		b.boostPolling(time.Now())
		b.reg.MarkSeen(chatID, msg.ID)
	}
	continuationPath := ""
	if seeded && window.Truncated {
		continuationPath = window.NextPath
	}
	updated, err := b.store.RecordChatPollSuccessWithContinuation(ctx, chatID, maxModified, true, windowFull, len(msgs), continuationPath)
	if err != nil {
		return handled, err
	}
	if handled && activityAt.IsZero() {
		activityAt = time.Now()
	}
	return handled, b.scheduleChatAfterPoll(ctx, chatID, role, running, updated, activityAt)
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
	_, err := b.store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:            chatID,
		PollState:         decision.State,
		PreviousPollState: decision.PreviousState,
		NextPollAt:        next,
		LastActivityAt:    activityAt,
		ClearBlockedUntil: true,
		ResetFailures:     true,
	})
	return err
}

func (b *Bridge) effectiveMaxWorkChatPollsPerCycle() int {
	if b != nil && b.maxWorkChatPollsPerCycle > 0 {
		return b.maxWorkChatPollsPerCycle
	}
	return maxWorkChatPollsPerCycle
}

func (b *Bridge) persistInboundPollDecision(ctx context.Context, decision inboundPollDecision) error {
	if strings.TrimSpace(decision.ChatID) == "" || strings.TrimSpace(decision.State) == "" {
		return nil
	}
	_, err := b.store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:            decision.ChatID,
		PollState:         decision.State,
		PreviousPollState: decision.PreviousState,
		NextPollAt:        decision.NextPollAt,
		LastActivityAt:    decision.LastActivityAt,
		BlockedUntil:      decision.BlockedUntil,
		ClearBlockedUntil: decision.State != inboundPollStateBlocked,
	})
	return err
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
	if delay > 2*time.Minute {
		delay = 2 * time.Minute
	}
	return now.Add(delay)
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
	body := renderTeamsFreezeNoticeHTML(
		b.reg.ControlChatURL,
		resumeCommand,
		"Your Codex work is safe. Paused after 48h idle.",
	)
	err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:park-notice:" + session.ID + ":" + resumeKeyForSession(session),
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

func (b *Bridge) shouldIgnoreMessage(ctx context.Context, chatID string, msg ChatMessage) (bool, error) {
	if msg.ID == "" || b.reg.HasSeen(chatID, msg.ID) || b.reg.HasSent(chatID, msg.ID) {
		return true, nil
	}
	if msg.MessageType != "" && msg.MessageType != "message" {
		return true, nil
	}
	if msg.From.User == nil {
		return true, nil
	}
	if b.user.ID != "" && msg.From.User.ID != b.user.ID {
		return true, nil
	}
	if b.store != nil {
		delivered, err := b.store.HasDeliveredOutboxMessage(ctx, chatID, msg.ID)
		if err != nil {
			return false, err
		}
		if delivered {
			b.reg.MarkSent(chatID, msg.ID)
			return true, nil
		}
		delivered, err = b.hasDeliveredOutboxMessageByRenderedContent(ctx, chatID, msg)
		if err != nil {
			return false, err
		}
		if delivered {
			b.reg.MarkSent(chatID, msg.ID)
			return true, nil
		}
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
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.TeamsChatID != chatID || !outboxMayHaveReachedTeams(outbox) {
			continue
		}
		if comparableTeamsPlainText(PlainTextFromTeamsHTML(renderOutboxHTML(outbox))) != incomingKey {
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
		"🧑‍💻 User:",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func comparableTeamsPlainText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func (b *Bridge) annotateIncomingUserMessage(ctx context.Context, chatID string, msg ChatMessage) {
	if !b.annotateUserMessages || b.annotationDisabled || b.graph == nil {
		return
	}
	if msg.ID == "" || strings.TrimSpace(msg.Body.Content) == "" || isPromptlessTeamsAttachmentPlaceholderMessage(msg) {
		return
	}
	annotated, ok := userAnnotatedMessageHTML(msg, b.user)
	if !ok {
		return
	}
	if err := b.graph.UpdateChatMessageHTML(ctx, chatID, msg.ID, annotated); err != nil {
		if shouldDisableUserMessageAnnotation(err) {
			b.annotationDisabled = true
		}
		if !b.annotationWarned && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams user message annotation disabled or unavailable: %v\n", err)
			b.annotationWarned = true
		}
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
	if content == "" || hasUserAnnotationPrefix(content) || isPromptlessTeamsAttachmentPlaceholderMessage(msg) {
		return "", false
	}
	label := incomingUserLabel()
	if strings.TrimSpace(msg.Body.ContentType) != "" && !strings.EqualFold(strings.TrimSpace(msg.Body.ContentType), "html") {
		content = "<p>" + html.EscapeString(PlainTextFromTeamsHTML(content)) + "</p>"
	}
	return `<p><strong>` + html.EscapeString(label) + `:</strong></p>` + content, true
}

func hasUserAnnotationPrefix(content string) bool {
	firstLine := strings.TrimSpace(PlainTextFromTeamsHTML(content))
	if firstLine == "" {
		return false
	}
	if before, _, ok := strings.Cut(firstLine, "\n"); ok {
		firstLine = strings.TrimSpace(before)
	}
	return isUserAnnotationLabel(firstLine)
}

func promptTextFromTeamsMessageHTML(content string) string {
	return stripUserAnnotationPrefix(PlainTextFromTeamsHTML(content))
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
	firstLine, rest, ok := strings.Cut(text, "\n")
	firstLine = strings.TrimSpace(firstLine)
	if !isUserAnnotationLabel(firstLine) {
		return text
	}
	if !ok {
		return ""
	}
	return strings.TrimSpace(rest)
}

func isUserAnnotationLabel(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasSuffix(line, ":") && (strings.HasPrefix(line, "🧑‍💻 ") || strings.HasPrefix(line, "👤 "))
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

func (b *Bridge) handleControlMessage(ctx context.Context, msg ChatMessage, text string) error {
	if msg.Body.Content == "" && strings.TrimSpace(text) != "" {
		msg.Body.Content = text
	}
	if len(msg.Attachments) > 0 {
		return b.sendControl(ctx, UnsupportedControlAttachmentMessage(msg.Attachments))
	}
	if parsed := ParseDashboardCommand(ChatScopeControl, text); parsed.HelperCommand {
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
				return b.sendControl(ctx, "workspace discovery failed: "+err.Error())
			}
			return b.sendControl(ctx, message)
		case DashboardCommandWorkspace:
			message, err := b.formatWorkspaceSessionsDashboard(ctx, parsed.Target)
			if err != nil {
				return b.sendControl(ctx, controlCommandErrorMessage(err))
			}
			return b.sendControl(ctx, message)
		case DashboardCommandSessions:
			message, err := b.formatWorkspaceSessionsDashboard(ctx, DashboardCommandTarget{})
			if err != nil {
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
		case DashboardCommandRestart:
			return b.restartHelperFromControl(ctx, parsed.Argument)
		case DashboardCommandReload:
			return b.reloadHelperFromControl(ctx, parsed.Argument)
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
				if control.Draining && control.Reason == teamstore.HelperUpgradeReason {
					deferredMsg := msg
					if resolved, err := b.resolvePublishTargetSessionID(ctx, parsed.Target); err == nil && resolved != "" {
						deferredMsg.Body.ContentType = "html"
						deferredMsg.Body.Content = html.EscapeString("continue " + resolved)
					}
					inbound, _, err := b.persistControlInboundWithStatus(ctx, deferredMsg, teamstore.InboundStatusDeferred, "teams_control_publish")
					if err != nil {
						return err
					}
					return b.sendDeferredUpgradeNotice(ctx, b.reg.ControlChatID, inbound)
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
			return b.sendControl(ctx, "use `helper rename <title>` or `!rename <title>` inside a work chat to update that Teams chat title.")
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
			return b.sendControl(ctx, unknownControlCommandMessage(text))
		}
	}
	if looksLikeControlPath(text) {
		return b.sendControl(ctx, controlPathHintMessage(text))
	}
	return b.runControlFallback(ctx, msg, text)
}

func (b *Bridge) runControlFallback(ctx context.Context, msg ChatMessage, text string) error {
	if strings.TrimSpace(text) == "" {
		return b.sendControl(ctx, controlHelpText())
	}
	if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
		return err
	} else if blocked {
		if control.Draining && control.Reason == teamstore.HelperUpgradeReason {
			session, err := b.ensureControlFallbackSession(ctx)
			if err != nil {
				return err
			}
			inbound, _, err := b.persistInboundWithStatusAndSource(ctx, session, msg, teamstore.InboundStatusDeferred, "teams_control_fallback")
			if err != nil {
				return err
			}
			return b.sendDeferredUpgradeNotice(ctx, session.ChatID, inbound)
		}
		return b.sendControl(ctx, serviceControlBlockedMessage(control, "control fallback requests"))
	}
	session, err := b.ensureControlFallbackSession(ctx)
	if err != nil {
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
	if err := b.queueControlFallbackAck(ctx, session, turn); err != nil {
		return err
	}
	return b.runQueuedTurnWithExecutor(ctx, b.effectiveControlFallbackExecutor(), session, turn, session.ChatID, ControlFallbackCodexPrompt(text))
}

func (b *Bridge) ensureControlFallbackSession(ctx context.Context) (*Session, error) {
	if err := b.ensureStore(); err != nil {
		return nil, err
	}
	now := time.Now()
	model := b.effectiveControlFallbackModel()
	created, _, err := b.store.CreateSession(ctx, teamstore.SessionContext{
		ID:           controlFallbackSessionID,
		Status:       teamstore.SessionStatusActive,
		TeamsChatURL: b.reg.ControlChatURL,
		TeamsTopic:   b.reg.ControlChatTopic,
		RunnerKind:   "control_fallback",
		Model:        model,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err != nil {
		return nil, err
	}
	if created.RunnerKind == "" || created.Model != model || created.TeamsChatURL != b.reg.ControlChatURL || created.TeamsTopic != b.reg.ControlChatTopic {
		if err := b.store.UpdateSession(ctx, controlFallbackSessionID, func(state *teamstore.State) error {
			current := state.Sessions[controlFallbackSessionID]
			current.RunnerKind = "control_fallback"
			current.Model = model
			current.TeamsChatURL = b.reg.ControlChatURL
			current.TeamsTopic = b.reg.ControlChatTopic
			current.UpdatedAt = now
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
		Status:        status,
		CodexThreadID: durable.CodexThreadID,
		Cwd:           durable.Cwd,
		CreatedAt:     durable.CreatedAt,
		UpdatedAt:     durable.UpdatedAt,
	}
}

func controlHelpText() string {
	return strings.Join([]string{
		"## 🏠 Control chat",
		"Use this chat to choose a workspace and create or continue 💬 Codex Work chats.",
		"",
		"Start here:",
		"- `p` / `projects` - choose a workspace",
		"- `n <directory>` / `new <directory>` - create a new Work chat for a directory",
		"- `s` / `sessions` - show sessions in the selected workspace",
		"- `c <number>` / `continue <number>` - continue an old local Codex session in Teams",
		"- `st` / `status` - show active Work chats",
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
		"- `h` / `help` / `menu` - show short help",
		"- `helper restart` - show the safe restart confirmation",
		"- `helper restart now` - restart the local Teams helper after sending a confirmation",
		"",
		"work chat commands:",
		"Inside a 💬 Work chat, send your task as a regular Teams message. Use `helper help`, `helper status`, `helper retry last`, `helper file <relative-path>`, or `helper close` for helper actions.",
		"Status words: `queued`/`running` means wait, `completed` means done, `failed` or `interrupted` means check recent messages and changed files before `helper retry last`.",
		"If this chat stops replying for about a minute, start the helper again on the host, then send `status`.",
		"",
		"copy-ready examples:",
		"`p`",
		"`n /home/baka/project/codex-helper`",
		"`m ~/tmp/mobile-fix`",
	}, "\n")
}

func sessionHelpText() string {
	return strings.Join([]string{
		"💬 Work chat",
		"Send your task as a regular Teams message. Messages starting with `helper` or `!` are helper commands.",
		"",
		"Common commands:",
		"`helper status` or `!status` - check progress",
		"`helper file <relative-path>` or `!file <relative-path>` - upload a file prepared in the helper's Teams upload folder",
		"`helper close` or `!close` - close this Codex session in Teams",
		"`helper details` or `!details` - show debug IDs and links",
		"",
		"Send `helper help advanced` for retry, cancel, and rename commands.",
	}, "\n")
}

func sessionAdvancedHelpText() string {
	return strings.Join([]string{
		"💬 Work chat advanced help",
		"`helper status` or `!status` - check progress",
		"`helper details` or `!details` - show IDs and debug details",
		"`helper rename <title>` or `!rename <title>` - rename this Teams chat",
		"`helper file <relative-path>` or `!file <relative-path>` - upload a file prepared in the helper's Teams upload folder",
		"`helper close` or `!close` - close this Codex session in Teams",
		"advanced commands: `helper retry last`, `helper retry <turn-id>` / `!retry <turn-id>`, or `helper cancel <turn-id>` / `!cancel <turn-id>`",
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
		return "⚠️ Wrong chat\n\nThis is the 🏠 control chat. `helper ...` commands like `helper file`, `helper retry`, `helper close`, and `helper rename` work inside a 💬 Work chat.\n\nTo start project work, send `new <directory>` here, then open the new Work chat and send the task there."
	}
	if strings.Contains(name, "/") || strings.HasPrefix(name, ".") {
		return controlPathHintMessage(text)
	}
	return fmt.Sprintf("unknown control command: `%s`\n\n%s", name, controlHelpText())
}

func (b *Bridge) restartHelperFromControl(ctx context.Context, arg string) error {
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
	if message, blocked, err := b.helperRestartBlockedMessage(ctx, helperRestartForce(arg)); err != nil {
		return err
	} else if blocked {
		return b.sendControl(ctx, message)
	}
	if err := b.sendControl(ctx, strings.Join([]string{
		"🔄 Helper restart scheduled",
		"",
		"I may be silent for a few seconds.",
		"After it comes back, send `st` if you want to check status.",
	}, "\n")); err != nil {
		return err
	}
	restarter := b.helperRestarter
	go b.runDelayedHelperRestart(restarter)
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

func (b *Bridge) reloadHelperFromControl(ctx context.Context, arg string) error {
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
	go b.runDelayedHelperReload(reloader, HelperReloadOptions{Force: helperReloadForce(arg)}, previous)
	return nil
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
		if state.ServiceControl.Draining {
			blocked = true
			blockedMessage = helperDrainBlockedMessage(state.ServiceControl, "reload")
			return nil
		}
		if !force && teamstore.HasUpgradeBlockingWork(*state, now) {
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
		next := state.ServiceControl
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

func (b *Bridge) helperRestartBlockedMessage(ctx context.Context, force bool) (string, bool, error) {
	if err := b.ensureStore(); err != nil {
		return "", false, err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return "", false, err
	}
	if state.ServiceControl.Draining {
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
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper restart failed: %v\n", err)
		}
		_ = b.sendControl(context.Background(), "⚠️ Helper restart failed\n\n"+err.Error())
	}
}

func (b *Bridge) runDelayedHelperReload(reloader HelperReloader, opts HelperReloadOptions, previous teamstore.ServiceControl) {
	delay := helperRestartDelay
	if delay > 0 {
		time.Sleep(delay)
	}
	var clearOnce sync.Once
	clearDrain := func(ctx context.Context) error {
		var err error
		clearOnce.Do(func() {
			err = b.restoreHelperReloadDrain(ctx, previous)
		})
		return err
	}
	opts.BeforeRestart = clearDrain
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := reloader(ctx, opts); err != nil {
		_ = clearDrain(context.Background())
		if b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams helper reload failed: %v\n", err)
		}
		_ = b.sendControl(context.Background(), "⚠️ Helper reload failed\n\n"+err.Error())
		return
	}
	_ = clearDrain(context.Background())
}

func isWorkOnlyHelperCommand(text string) bool {
	name, _, syntax, ok := splitDashboardCommand(text)
	if !ok || syntax != dashboardCommandSyntaxHelp {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "file", "image", "send-file", "send-image", "retry", "cancel", "close", "rename":
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
	return CodexExecutor{ExtraArgs: []string{"--model", b.effectiveControlFallbackModel()}}
}

func (b *Bridge) effectiveControlFallbackModel() string {
	if model := strings.TrimSpace(b.controlFallbackModel); model != "" {
		return model
	}
	return DefaultControlFallbackModel
}

func (b *Bridge) queueControlFallbackAck(ctx context.Context, session *Session, turn teamstore.Turn) error {
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + turn.ID + ":control-ack",
		SessionID:   session.ID,
		TurnID:      turn.ID,
		TeamsChatID: session.ChatID,
		Kind:        "ack",
		AckKind:     "control_prompt",
		Body:        "❓ Quick helper question\n\nI will answer here. For project work, send `new <directory>`, then send the task inside the new 💬 Work chat.",
	})
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	if err := b.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: false}); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams control ACK send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) createSession(ctx context.Context, msg ChatMessage, request string) error {
	if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
		return err
	} else if blocked {
		if control.Draining && control.Reason == teamstore.HelperUpgradeReason {
			inbound, _, err := b.persistControlInboundWithStatus(ctx, msg, teamstore.InboundStatusDeferred, "teams_control_new")
			if err != nil {
				return err
			}
			return b.sendDeferredUpgradeNotice(ctx, b.reg.ControlChatID, inbound)
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
	if parsed.WorkDir != "" {
		if err := os.MkdirAll(parsed.WorkDir, 0o700); err != nil {
			return b.sendControl(ctx, "create workspace failed: "+err.Error())
		}
	}
	now := time.Now()
	sessionID := b.reg.NextSessionID()
	titleTopic := ""
	if parsed.Title != "" {
		titleTopic = SessionTopic(now, parsed.Title)
	}
	topic := WorkChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		SessionID:    sessionID,
		Topic:        titleTopic,
		Cwd:          parsed.WorkDir,
	})
	chat, err := b.createMeetingChat(ctx, topic)
	if err != nil {
		return err
	}
	session := Session{
		ID:        sessionID,
		ChatID:    chat.ID,
		ChatURL:   chat.WebURL,
		Topic:     chat.Topic,
		Status:    "active",
		Cwd:       parsed.WorkDir,
		CreatedAt: now,
		UpdatedAt: now,
	}
	b.reg.Sessions = append(b.reg.Sessions, session)
	if err := b.ensureDurableSession(ctx, &session); err != nil {
		return err
	}
	if err := b.sendChatCreatedMention(ctx, session.ID, chat.ID, "Work chat created: "+session.ID+"."); err != nil {
		return err
	}
	if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + session.ID + ":anchor",
		SessionID:   session.ID,
		TeamsChatID: chat.ID,
		Kind:        "anchor",
		Body:        sessionReadyMessage(session, parsed.Title),
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
	WorkDir string
	Title   string
}

func (b *Bridge) parseNewSessionRequest(ctx context.Context, raw string) (newSessionRequest, error) {
	parsed, err := parseNewSessionRequest(raw)
	if err != nil {
		return newSessionRequest{}, err
	}
	if parsed.WorkDir == "" && strings.TrimSpace(raw) == "" {
		if workspace, ok := b.currentControlWorkspace(ctx); ok {
			parsed.WorkDir = workspace.Path
		}
	}
	if parsed.WorkDir == "" {
		return newSessionRequest{}, fmt.Errorf("usage: `new <directory>`; after opening a workspace from `projects`, you can also send `new`")
	}
	return parsed, nil
}

func parseNewSessionRequest(raw string) (newSessionRequest, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return newSessionRequest{}, nil
	}
	before, after, hasSep := strings.Cut(raw, " -- ")
	if !hasSep {
		resolved, err := resolveUserWorkspacePath(raw)
		if err != nil {
			return newSessionRequest{}, err
		}
		return newSessionRequest{WorkDir: resolved}, nil
	}
	dir := strings.TrimSpace(before)
	title := strings.TrimSpace(after)
	if dir == "" {
		return newSessionRequest{}, fmt.Errorf("usage: `new <directory>`")
	}
	resolved, err := resolveUserWorkspacePath(dir)
	if err != nil {
		return newSessionRequest{}, err
	}
	return newSessionRequest{WorkDir: resolved, Title: title}, nil
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

func sessionReadyMessage(session Session, prompt string) string {
	var lines []string
	lines = append(lines, "💬 Work chat is ready.")
	lines = append(lines, "Send a task in this chat. Codex will start automatically and continue this session.")
	lines = append(lines, "Status: waiting for your first task.")
	if strings.TrimSpace(session.ID) != "" {
		lines = append(lines, "Session: "+session.ID)
	}
	lines = append(lines, "Project: "+sessionReadyProjectLabel(session))
	lines = append(lines, "Commands: `helper status` or `!status`, `helper help`, `helper close` to close this Codex session in Teams.")
	lines = append(lines, "Need the full path? Send `helper status`.")
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

func (b *Bridge) handleSessionMessage(ctx context.Context, chatID string, msg ChatMessage, text string) error {
	session := b.reg.SessionByChatID(chatID)
	if session == nil {
		return nil
	}
	if isPromptlessTeamsAttachmentPlaceholderMessage(msg) {
		b.reg.MarkSeen(chatID, msg.ID)
		return nil
	}
	if parsed := ParseDashboardCommand(ChatScopeWork, text); parsed.HelperCommand {
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
		case DashboardCommandRetry:
			if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
				return err
			} else if blocked {
				return b.rejectSessionWork(ctx, session, msg, control)
			}
			return b.retryTurnCommand(ctx, session, strings.TrimSpace(parsed.Argument))
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
			return b.renameSessionChat(ctx, session, strings.TrimSpace(parsed.Argument))
		case DashboardCommandDetails:
			return b.sendToChat(ctx, chatID, b.formatSessionDetails(session))
		case DashboardCommandHelp:
			if isAdvancedHelpArg(parsed.Argument) {
				return b.sendToChat(ctx, chatID, sessionAdvancedHelpText())
			}
			return b.sendToChat(ctx, chatID, sessionHelpText())
		default:
			return b.sendToChat(ctx, chatID, unknownWorkCommandMessage(text))
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
	if b.asyncTurns {
		turns, err := b.sessionTurnQueueState(ctx, session.ID)
		if err != nil {
			return err
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
			if turns.Queued == 0 {
				if err := b.queueTeamsPromptAck(ctx, session, turn, true); err != nil {
					return err
				}
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
			if turns.Queued == 0 {
				if err := b.queueTeamsPromptAckWithBody(ctx, session, turn, gate.AckBody); err != nil {
					return err
				}
			}
			b.boostPolling(time.Now())
			return nil
		}
	}

	localFiles, cleanupFiles, hostedAttachmentMessage, err := b.downloadHostedContentAttachments(ctx, session, chatID, msg)
	if err != nil {
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return b.rejectSessionAttachmentWithMessage(ctx, session, msg, message)
		}
		return err
	}
	if hostedAttachmentMessage != "" {
		cleanupFiles()
		return b.rejectSessionAttachmentWithMessage(ctx, session, msg, hostedAttachmentMessage)
	}
	referenceFiles, cleanupReferenceFiles, unsupportedAttachmentMessage, err := b.downloadReferenceFileAttachments(ctx, session, msg)
	if err != nil {
		cleanupFiles()
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return b.rejectSessionAttachmentWithMessage(ctx, session, msg, message)
		}
		return err
	}
	cleanupLocalFiles := func() {
		cleanupFiles()
		cleanupReferenceFiles()
	}
	if unsupportedAttachmentMessage != "" {
		cleanupLocalFiles()
		return b.rejectSessionAttachmentWithMessage(ctx, session, msg, unsupportedAttachmentMessage)
	}
	localFiles = append(localFiles, referenceFiles...)

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
	if err := b.queueTeamsPromptAck(ctx, session, turn, false); err != nil {
		cleanupLocalFiles()
		return err
	}
	if b.asyncTurns {
		prompt := PromptWithLocalAttachments(TeamsCodexPrompt(text), localFiles)
		started, err := b.startQueuedTurn(ctx, session, turn.ID, func(runCtx context.Context, claimed teamstore.Turn) error {
			defer cleanupLocalFiles()
			return b.runQueuedTurn(runCtx, session, claimed, chatID, prompt)
		})
		if err != nil {
			cleanupLocalFiles()
			return err
		}
		if !started {
			cleanupLocalFiles()
		}
		b.boostPolling(time.Now())
		return nil
	}
	defer cleanupLocalFiles()

	return b.runQueuedTurn(ctx, session, turn, chatID, PromptWithLocalAttachments(TeamsCodexPrompt(text), localFiles))
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
	msg, err := b.readClient().GetMessage(ctx, inbound.TeamsChatID, inbound.TeamsMessageID)
	if err != nil {
		return b.sendToChat(ctx, session.ChatID, "retry failed while reading the original Teams message: "+err.Error())
	}
	var localFiles []LocalAttachment
	if len(msg.Attachments) > 0 {
		referenceFiles, cleanupReferenceFiles, unsupportedAttachmentMessage, err := b.downloadReferenceFileAttachments(ctx, session, msg)
		if err != nil {
			if message, ok := attachmentDownloadUserMessage(err); ok {
				return b.sendToChat(ctx, session.ChatID, message)
			}
			return b.sendToChat(ctx, session.ChatID, "retry failed while downloading Teams file attachment: "+err.Error())
		}
		defer cleanupReferenceFiles()
		if unsupportedAttachmentMessage != "" {
			return b.sendToChat(ctx, session.ChatID, unsupportedAttachmentMessage)
		}
		localFiles = append(localFiles, referenceFiles...)
	}
	hostedFiles, cleanupFiles, hostedAttachmentMessage, err := b.downloadHostedContentAttachments(ctx, session, inbound.TeamsChatID, msg)
	if err != nil {
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return b.sendToChat(ctx, session.ChatID, message)
		}
		return b.sendToChat(ctx, session.ChatID, "retry failed while downloading Teams hosted content: "+err.Error())
	}
	defer cleanupFiles()
	if hostedAttachmentMessage != "" {
		return b.sendToChat(ctx, session.ChatID, hostedAttachmentMessage)
	}
	localFiles = append(localFiles, hostedFiles...)
	prompt, promptOK := promptTextFromTeamsMessageOrFallback(msg, inbound.Text)
	if !promptOK && len(localFiles) == 0 {
		return b.sendToChat(ctx, session.ChatID, "retry cannot use an empty or helper-generated original message.")
	}
	retryTurn, _, err := b.store.QueueTurn(ctx, teamstore.Turn{
		ID:             retryTurnID(turn.ID),
		SessionID:      session.ID,
		CodexThreadID:  session.CodexThreadID,
		RecoveryReason: "retry of " + turn.ID,
	})
	if err != nil {
		return err
	}
	return b.runQueuedTurn(ctx, session, retryTurn, session.ChatID, PromptWithLocalAttachments(TeamsCodexPrompt(prompt), localFiles))
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
	body := "⏳ Codex is working. Request accepted."
	if queuedBehindActive {
		body = "⏳ Queued. Codex will respond after the current request."
	}
	return b.queueTeamsPromptAckWithBody(ctx, session, turn, body)
}

func (b *Bridge) queueTeamsPromptAckWithBody(ctx context.Context, session *Session, turn teamstore.Turn, body string) error {
	body = strings.TrimSpace(body)
	if body == "" {
		body = "⏳ Codex is working. Request accepted."
	}
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:" + turn.ID + ":ack",
		SessionID:   session.ID,
		TurnID:      turn.ID,
		TeamsChatID: session.ChatID,
		Kind:        "ack",
		AckKind:     "teams_prompt",
		Body:        body,
	})
	if err != nil {
		return err
	}
	if queued.Status == teamstore.OutboxStatusSent {
		return nil
	}
	if err := b.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: false}); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams ACK send error: %v\n", err)
	}
	return nil
}

func (b *Bridge) recoverUnfinishedTurns(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	state, err := b.store.Load(ctx)
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
			if err := b.recoverQueuedTurn(ctx, session, turn, state); err != nil {
				return err
			}
		case teamstore.TurnStatusRunning:
			if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "ambiguous after helper restart"); err != nil {
				return err
			}
			if err := b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
				ID:          "outbox:" + turn.ID + ":interrupted-after-restart",
				SessionID:   session.ID,
				TurnID:      turn.ID,
				TeamsChatID: session.ChatID,
				Kind:        "interrupted-after-restart",
				Body:        "turn was interrupted after helper restart: " + turn.ID + "\nUse `helper retry " + turn.ID + "` if you want to run it again.",
			}); err != nil {
				return err
			}
		}
	}
	return nil
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
		if text == "" {
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
		turn, turnCreated, err := b.queueTurn(ctx, session, inbound)
		if err != nil {
			return err
		}
		if !turnCreated {
			if err := b.flushPendingOutbox(ctx, session.ID, turn.ID); err != nil {
				return err
			}
			continue
		}
		if err := b.queueTeamsPromptAck(ctx, session, turn, false); err != nil {
			return err
		}
		if err := b.runQueuedTurn(ctx, session, turn, session.ChatID, TeamsCodexPrompt(text)); err != nil {
			return err
		}
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
	state, err := b.store.Load(ctx)
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
	for _, sessionID := range sessionIDs {
		session := b.sessionForIDState(state, sessionID)
		if session == nil {
			continue
		}
		gate, err := b.prepareLocalCodexBeforeTeamsTurn(ctx, session)
		if err != nil {
			return err
		}
		if gate.Block {
			continue
		}
		if _, err := b.startQueuedTurn(ctx, session, "", nil); err != nil {
			return err
		}
	}
	return nil
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
	if text == "" {
		return b.markDeferredInboundIgnored(ctx, inbound.ID, "deferred control input text is unavailable")
	}
	msg := ChatMessage{ID: inbound.TeamsMessageID}
	msg.Body.ContentType = "html"
	msg.Body.Content = html.EscapeString(text)
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
		session, err := b.ensureControlFallbackSession(ctx)
		if err != nil {
			return err
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
		return b.runQueuedTurnWithExecutor(ctx, b.effectiveControlFallbackExecutor(), session, turn, session.ChatID, ControlFallbackCodexPrompt(text))
	default:
		return b.markDeferredInboundIgnored(ctx, inbound.ID, "unsupported deferred control input")
	}
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
		Status:        string(durable.Status),
		CodexThreadID: durable.CodexThreadID,
		Cwd:           durable.Cwd,
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
	msg, err := b.readClient().GetMessage(ctx, inbound.TeamsChatID, inbound.TeamsMessageID)
	if err != nil {
		return err
	}
	localFiles, cleanup, hostedAttachmentMessage, err := b.downloadHostedContentAttachments(ctx, session, inbound.TeamsChatID, msg)
	if err != nil {
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return b.interruptTurnForAttachmentMessage(ctx, session, turn, message)
		}
		return err
	}
	defer cleanup()
	if hostedAttachmentMessage != "" {
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "queued turn has too many hosted attachments"); err != nil {
			return err
		}
		return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
			ID:          "outbox:" + turn.ID + ":hosted-attachment-limit",
			SessionID:   session.ID,
			TurnID:      turn.ID,
			TeamsChatID: session.ChatID,
			Kind:        "interrupted",
			Body:        hostedAttachmentMessage,
		})
	}
	referenceFiles, cleanupReferenceFiles, unsupportedAttachmentMessage, err := b.downloadReferenceFileAttachments(ctx, session, msg)
	if err != nil {
		if message, ok := attachmentDownloadUserMessage(err); ok {
			return b.interruptTurnForAttachmentMessage(ctx, session, turn, message)
		}
		return err
	}
	defer cleanupReferenceFiles()
	if unsupportedAttachmentMessage != "" {
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "queued turn has unsupported attachment"); err != nil {
			return err
		}
		return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
			ID:          "outbox:" + turn.ID + ":recovery-unsupported-attachment",
			SessionID:   session.ID,
			TurnID:      turn.ID,
			TeamsChatID: session.ChatID,
			Kind:        "recovery-unsupported-attachment",
			Body:        unsupportedAttachmentMessage,
		})
	}
	localFiles = append(localFiles, referenceFiles...)
	prompt, promptOK := promptTextFromTeamsMessageOrFallback(msg, inbound.Text)
	if !promptOK && len(localFiles) == 0 {
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "queued turn original prompt is empty or helper-generated"); err != nil {
			return err
		}
		return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
			ID:          "outbox:" + turn.ID + ":recovery-empty",
			SessionID:   session.ID,
			TurnID:      turn.ID,
			TeamsChatID: session.ChatID,
			Kind:        "recovery-empty",
			Body:        "queued turn could not be recovered because the original prompt is empty: " + turn.ID,
		})
	}
	basePrompt := TeamsCodexPrompt(prompt)
	executor := b.executor
	if session.ID == controlFallbackSessionID {
		basePrompt = ControlFallbackCodexPrompt(prompt)
		executor = b.effectiveControlFallbackExecutor()
	}
	return b.runQueuedTurnWithExecutor(ctx, executor, session, turn, session.ChatID, PromptWithLocalAttachments(basePrompt, localFiles))
}

func (b *Bridge) cancelTurnCommand(ctx context.Context, session *Session, turnID string) error {
	if turnID == "" {
		return b.sendToChat(ctx, session.ChatID, "usage: `helper cancel <turn-id>` or `!cancel <turn-id>`")
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	turn, ok := state.Turns[turnID]
	if !ok || turn.SessionID != session.ID {
		return b.sendToChat(ctx, session.ChatID, "turn not found in this session: "+turnID)
	}
	switch turn.Status {
	case teamstore.TurnStatusQueued:
		if _, err := b.store.MarkTurnInterrupted(ctx, turn.ID, "canceled by user"); err != nil {
			return err
		}
		return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
			ID:          "outbox:" + turn.ID + ":canceled",
			SessionID:   session.ID,
			TurnID:      turn.ID,
			TeamsChatID: session.ChatID,
			Kind:        "canceled",
			Body:        "turn canceled: " + turn.ID,
		})
	case teamstore.TurnStatusRunning:
		return b.sendToChat(ctx, session.ChatID, "running turn cancellation is not available in this foreground runner yet. Stop the service and use `codex-proxy teams recover` if the turn is stuck.")
	default:
		return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("turn %s is %s and cannot be canceled.", turn.ID, turn.Status))
	}
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
	_, ok, err := FileWriteAuthCacheAvailable()
	if err != nil {
		return b.sendToChat(ctx, session.ChatID, "artifact upload auth check failed: "+err.Error())
	}
	if !ok {
		return b.sendToChat(ctx, session.ChatID, "artifact manifest detected, but Teams file upload is not authenticated. Run `codex-proxy teams auth file-write` locally, then retry with `helper file <relative-path>` if needed. Outbound root: "+root)
	}
	for blockIndex, block := range blocks {
		plan, err := ParseArtifactManifest(block, ArtifactManifestOptions{
			OutboundRoot: root,
			SessionID:    session.ID,
			TurnID:       turn.ID,
			ValidateFile: validateArtifactManifestFile,
		})
		if err != nil {
			return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("artifact manifest %d rejected: %v", blockIndex+1, err))
		}
		for _, planned := range plan.Files {
			file, err := PrepareOutboundAttachment(planned.CleanPath, OutboundAttachmentOptions{Root: root})
			if err != nil {
				return b.sendToChat(ctx, session.ChatID, fmt.Sprintf("artifact upload rejected: %v", err))
			}
			file.UploadName = ArtifactUploadName(session.ID, turn.ID, file.Name, file.Bytes)
			outbox, err := b.queueAndSendAttachmentUploadOutbox(ctx, session.ID, turn.ID, session.ChatID, "artifact", "artifact attached: "+file.Name, file, OutboundAttachmentOptions{})
			if err != nil {
				return err
			}
			result := OutboundAttachmentResult{File: file, Item: driveItemFromOutbox(outbox), Message: ChatMessage{ID: outbox.TeamsMessageID}}
			if err := b.recordArtifactUpload(ctx, session, turn, planned, result); err != nil {
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

func (b *Bridge) recordArtifactUpload(ctx context.Context, session *Session, turn teamstore.Turn, planned ArtifactManifestFile, result OutboundAttachmentResult) error {
	if b.store == nil {
		return nil
	}
	artifactID := "artifact:" + session.ID + ":" + turn.ID + ":" + transcriptRecordKey(TranscriptRecord{DedupeKey: planned.CleanPath + ":" + result.Item.ID}, 0)
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		state.ArtifactRecords[artifactID] = teamstore.ArtifactRecord{
			ID:          artifactID,
			SessionID:   session.ID,
			TurnID:      turn.ID,
			Path:        planned.CleanPath,
			UploadName:  result.File.UploadName,
			DriveItemID: result.Item.ID,
			Status:      "uploaded",
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		return nil
	})
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
	session.UpdatedAt = time.Now()
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	if err := b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		current := state.Sessions[session.ID]
		current.TeamsTopic = topic
		current.UpdatedAt = session.UpdatedAt
		state.Sessions[session.ID] = current
		return nil
	}); err != nil {
		return err
	}
	return b.sendToChat(ctx, session.ChatID, "Work chat renamed.\n\nNew title:\n"+shortenTeamsLine(topic, 96)+"\n\nUse `helper details` to see the full Teams title and link.")
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

func (b *Bridge) runQueuedTurn(ctx context.Context, session *Session, turn teamstore.Turn, chatID string, text string) error {
	return b.runQueuedTurnWithExecutor(ctx, b.executor, session, turn, chatID, text)
}

func (b *Bridge) runQueuedTurnWithExecutor(ctx context.Context, executor Executor, session *Session, turn teamstore.Turn, chatID string, text string) error {
	if b.currentLeaseGeneration() > 0 {
		if err := b.ensureActiveControlLease(ctx); err != nil {
			return err
		}
	}
	if _, err := b.store.MarkTurnRunning(ctx, turn.ID, session.CodexThreadID, ""); err != nil {
		return err
	}
	if executor == nil {
		executor = CodexExecutor{}
	}
	result, err := b.runExecutorWithHeartbeat(ctx, executor, session, turn, chatID, text)
	if err != nil {
		if IsAmbiguousExecutionError(err) || result.CodexThreadID != "" || result.CodexTurnID != "" {
			if _, runningErr := b.store.MarkTurnRunning(ctx, turn.ID, firstNonEmptyString(result.CodexThreadID, session.CodexThreadID), result.CodexTurnID); runningErr != nil {
				return runningErr
			}
			if result.CodexThreadID != "" {
				session.CodexThreadID = result.CodexThreadID
			}
			if _, markErr := b.store.MarkTurnInterrupted(ctx, turn.ID, "ambiguous Codex execution: "+err.Error()); markErr != nil {
				return markErr
			}
			return b.queueAndSendOutboxChunks(ctx, session.ID, turn.ID, chatID, "interrupted", "Codex accepted the request, but the helper could not confirm whether it finished. I did not retry automatically because that could duplicate work.\n\nCheck recent messages and changed files first. To run the same Teams request again in this same session, send `helper retry last` here. Advanced: `helper retry "+turn.ID+"`.")
		}
		if _, markErr := b.store.MarkTurnFailed(ctx, turn.ID, err.Error()); markErr != nil {
			return markErr
		}
		return b.queueAndSendOutboxChunks(ctx, session.ID, turn.ID, chatID, "error", "error: "+err.Error())
	}
	if result.CodexThreadID != "" {
		session.CodexThreadID = result.CodexThreadID
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
	if _, err := b.store.MarkTurnCompleted(ctx, turn.ID, result.CodexThreadID, result.CodexTurnID); err != nil {
		return err
	}
	if len(queued) > 0 {
		if err := b.flushPendingOutboxForChat(ctx, chatID); err != nil {
			return err
		}
		b.boostPolling(time.Now())
	}
	return b.uploadArtifactsFromResult(ctx, session, turn, result.Text)
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
		ID:          "outbox:" + turn.ID + ":attachment-download",
		SessionID:   session.ID,
		TurnID:      turn.ID,
		TeamsChatID: session.ChatID,
		Kind:        "interrupted",
		Body:        message,
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

type queuedTurnRunner func(context.Context, teamstore.Turn) error

type sessionTurnQueueState struct {
	Running bool
	Queued  int
}

func (b *Bridge) sessionTurnQueueState(ctx context.Context, sessionID string) (sessionTurnQueueState, error) {
	if strings.TrimSpace(sessionID) == "" {
		return sessionTurnQueueState{}, nil
	}
	state, err := b.store.Load(ctx)
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
	runCtx := ctx
	go func() {
		err := b.runClaimedQueuedTurn(runCtx, session, claimed, preferredTurnID, preferred)
		if err != nil {
			b.handleClaimedQueuedTurnError(context.Background(), session, claimed, err)
		}
		if err := b.processQueuedTurns(context.Background()); err != nil && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams queued turn follow-up error: %v\n", err)
		}
		b.boostPolling(time.Now())
	}()
	return true, nil
}

func (b *Bridge) runClaimedQueuedTurn(ctx context.Context, session *Session, claimed teamstore.Turn, preferredTurnID string, preferred queuedTurnRunner) error {
	if strings.TrimSpace(preferredTurnID) != "" && claimed.ID == preferredTurnID && preferred != nil {
		return preferred(ctx, claimed)
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
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
		ID:          "outbox:" + turn.ID + ":queued-turn-error",
		SessionID:   turn.SessionID,
		TurnID:      turn.ID,
		TeamsChatID: chatID,
		Kind:        "error",
		Body:        "Codex could not continue this queued request: " + err.Error() + "\n\nPlease resend the message if you still want to run it.",
	}); queueErr != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams queued turn error notification failed: %v\n", queueErr)
	}
}

func (b *Bridge) runExecutorWithHeartbeat(ctx context.Context, executor Executor, session *Session, turn teamstore.Turn, chatID string, text string) (ExecutionResult, error) {
	if err := b.recordOwnerHeartbeat(ctx, session.ID, turn.ID); err != nil {
		return ExecutionResult{}, err
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := b.startActiveOwnerHeartbeat(heartbeatCtx, session.ID, turn.ID)
	var result ExecutionResult
	var runErr error
	if streaming, ok := executor.(StreamingExecutor); ok {
		forwarder := b.startCodexEventForwarder(ctx, session, turn, chatID)
		result, runErr = streaming.RunWithEventHandler(ctx, session, text, forwarder.Handle)
		forwarder.Close(result.Text)
	} else {
		result, runErr = executor.Run(ctx, session, text)
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
	ctx          context.Context
	bridge       *Bridge
	sessionID    string
	turnID       string
	chatID       string
	events       chan codexrunner.StreamEvent
	done         chan struct{}
	pendingAgent string
	seq          int
	err          error
}

func (b *Bridge) startCodexEventForwarder(ctx context.Context, session *Session, turn teamstore.Turn, chatID string) *codexEventForwarder {
	sessionID := ""
	if session != nil {
		sessionID = session.ID
	}
	f := &codexEventForwarder{
		ctx:       ctx,
		bridge:    b,
		sessionID: sessionID,
		turnID:    turn.ID,
		chatID:    chatID,
		events:    make(chan codexrunner.StreamEvent, 128),
		done:      make(chan struct{}),
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

func (f *codexEventForwarder) Close(finalText string) {
	if f == nil {
		return
	}
	close(f.events)
	<-f.done
	if strings.TrimSpace(f.pendingAgent) != "" && !sameCodexVisibleText(f.pendingAgent, finalText) {
		_ = f.send("progress", f.pendingAgent)
	}
}

func (f *codexEventForwarder) run() {
	defer close(f.done)
	timer := newCodexIdleStatusTimer(codexIdleStatusInitialDelay)
	defer stopCodexIdleStatusTimer(timer)
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
			resetCodexIdleStatusTimer(timer, codexIdleStatusInitialDelay)
		case <-idleC:
			f.sendIdleStatus()
			resetCodexIdleStatusTimer(timer, codexIdleStatusRepeatDelay)
		case <-f.ctx.Done():
			return
		}
	}
}

func (f *codexEventForwarder) handle(event codexrunner.StreamEvent) {
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
	case codexrunner.StreamEventTurnFailed:
		f.flushPendingAgent()
		if event.Failure != nil && strings.TrimSpace(event.Failure.Message) != "" {
			_ = f.send("status", "Codex turn failed: "+event.Failure.Message)
		}
	}
}

func (f *codexEventForwarder) flushPendingAgent() {
	if strings.TrimSpace(f.pendingAgent) == "" {
		return
	}
	_ = f.send("progress", f.pendingAgent)
	f.pendingAgent = ""
}

func (f *codexEventForwarder) sendIdleStatus() {
	if strings.TrimSpace(f.pendingAgent) != "" {
		f.flushPendingAgent()
		return
	}
	_ = f.send("status", codexIdleStatusMessage)
}

func (f *codexEventForwarder) send(kind string, text string) error {
	text = strings.TrimSpace(text)
	if text == "" || f.bridge == nil || strings.TrimSpace(f.chatID) == "" {
		return nil
	}
	f.seq++
	msgKind := fmt.Sprintf("codex-%s-%03d", kind, f.seq)
	err := f.bridge.queueAndSendOutboxChunks(f.ctx, f.sessionID, f.turnID, f.chatID, msgKind, text)
	if err != nil && f.err == nil {
		f.err = err
	}
	return err
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
	return strings.TrimSpace(StripHelperPromptEchoes(StripArtifactManifestBlocks(left))) == strings.TrimSpace(StripHelperPromptEchoes(StripArtifactManifestBlocks(right)))
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

func (b *Bridge) runStandbyLoop(ctx context.Context, opts BridgeOptions) error {
	if b.out != nil {
		holder := b.currentLease().HolderMachineID
		if holder == "" {
			holder = "unknown"
		}
		_, _ = fmt.Fprintf(b.out, "Teams bridge standby; control lease is held by %s. This process will keep running and retry.\n", holder)
	}
	for {
		if active, err := b.claimControlLease(ctx); err != nil {
			return err
		} else if active {
			if b.out != nil {
				_, _ = fmt.Fprintln(b.out, "Teams bridge acquired control lease; becoming active.")
			}
			return b.Listen(ctx, opts)
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

func (b *Bridge) claimControlLease(ctx context.Context) (bool, error) {
	if err := b.ensureStore(); err != nil {
		return false, err
	}
	if b.scope.ID == "" {
		b.scope = ScopeIdentityForUser(b.user)
	}
	if b.machine.ID == "" {
		b.machine = MachineRecordForUser(b.user, b.scope)
	}
	duration := b.leaseDuration
	if duration <= 0 {
		duration = 30 * time.Second
	}
	decision, err := b.store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
		Scope:    b.scope,
		Machine:  b.machine,
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
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil
	}
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:                 "outbox:" + inbound.ID + ":deferred-upgrade-notice",
		SessionID:          inbound.SessionID,
		TeamsChatID:        chatID,
		Kind:               "ack",
		AckKind:            "upgrade_deferred",
		Body:               "upgrade in progress. I saved this message and will resume it after the helper restarts.",
		UpgradeNonBlocking: true,
	})
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

func (b *Bridge) rejectSessionWork(ctx context.Context, session *Session, msg ChatMessage, control teamstore.ServiceControl) error {
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	status := teamstore.InboundStatusIgnored
	source := "teams"
	if control.Draining && control.Reason == teamstore.HelperUpgradeReason {
		status = teamstore.InboundStatusDeferred
		text := strings.TrimSpace(promptTextFromTeamsMessageHTML(msg.Body.Content))
		if len(msg.Attachments) > 0 || len(HostedContentIDsFromHTML(msg.Body.Content)) > 0 {
			source = "teams_session_attachment_deferred"
		} else if ParseDashboardCommand(ChatScopeWork, text).HelperCommand {
			source = "teams_session_command_deferred"
		}
	}
	inbound, _, err := b.persistInboundWithStatusAndSource(ctx, session, msg, status, source)
	if err != nil {
		return err
	}
	if status == teamstore.InboundStatusDeferred {
		return b.sendDeferredUpgradeNotice(ctx, session.ChatID, inbound)
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
	state, err := b.store.Load(ctx)
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
	if b.reg.ControlChatID == "" && state.ControlChat.TeamsChatID != "" {
		b.reg.ControlChatID = state.ControlChat.TeamsChatID
		b.reg.ControlChatURL = state.ControlChat.TeamsChatURL
		b.reg.ControlChatTopic = state.ControlChat.TeamsChatTopic
		changed = true
	}
	for _, durable := range state.Sessions {
		if durable.ID == "" || durable.TeamsChatID == "" {
			continue
		}
		if b.reg.SessionByID(durable.ID) != nil || b.reg.SessionByChatID(durable.TeamsChatID) != nil {
			continue
		}
		status := string(durable.Status)
		if status == "" {
			status = "active"
		}
		b.reg.Sessions = append(b.reg.Sessions, Session{
			ID:            durable.ID,
			ChatID:        durable.TeamsChatID,
			ChatURL:       durable.TeamsChatURL,
			Topic:         durable.TeamsTopic,
			Status:        status,
			CodexThreadID: durable.CodexThreadID,
			Cwd:           durable.Cwd,
			CreatedAt:     durable.CreatedAt,
			UpdatedAt:     durable.UpdatedAt,
		})
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
			b.reg.MarkSent(outbox.TeamsChatID, outbox.TeamsMessageID)
			changed = true
		}
	}
	if changed {
		return b.Save()
	}
	return nil
}

func (b *Bridge) migrateRegistryProjectionToStore(ctx context.Context) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
	if b.reg.ControlChatID == "" && len(b.reg.Sessions) == 0 && len(b.reg.Chats) == 0 {
		return nil
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		now := time.Now()
		if b.user.ID != "" || b.user.UserPrincipalName != "" {
			if b.scope.ID == "" {
				b.scope = ScopeIdentityForUser(b.user)
			}
			if b.machine.ID == "" {
				b.machine = MachineRecordForUser(b.user, b.scope)
			}
			machineID := b.machine.ID
			if state.MachineIdentity.ID == "" {
				state.MachineIdentity.ID = machineID
				state.MachineIdentity.CreatedAt = now
			}
			state.MachineIdentity.Label = b.machine.Label
			state.MachineIdentity.Hostname = b.machine.Hostname
			state.MachineIdentity.AccountID = b.user.ID
			state.MachineIdentity.UserPrincipal = b.user.UserPrincipalName
			state.MachineIdentity.Profile = b.scope.Profile
			state.MachineIdentity.ScopeID = b.scope.ID
			state.MachineIdentity.Kind = b.machine.Kind
			state.MachineIdentity.Priority = b.machine.Priority
			state.MachineIdentity.UpdatedAt = now
		}
		if state.ControlChat.TeamsChatID == "" && b.reg.ControlChatID != "" {
			state.ControlChat.MachineID = state.MachineIdentity.ID
			state.ControlChat.AccountID = b.user.ID
			state.ControlChat.TeamsChatID = b.reg.ControlChatID
			state.ControlChat.TeamsChatURL = b.reg.ControlChatURL
			state.ControlChat.TeamsChatTopic = b.reg.ControlChatTopic
			state.ControlChat.BoundAt = now
			state.ControlChat.UpdatedAt = now
		}
		for _, session := range b.reg.Sessions {
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
				CodexThreadID: session.CodexThreadID,
				Cwd:           session.Cwd,
				CreatedAt:     created,
				UpdatedAt:     updated,
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
			}
		}
		return nil
	})
}

func migratedSentOutboxID(chatID string, messageID string) string {
	sum := sha256.Sum256([]byte(chatID + "\x00" + messageID))
	return "outbox:registry-sent:" + hex.EncodeToString(sum[:])
}

func (b *Bridge) recordControlChatBinding(ctx context.Context, chat Chat) error {
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
	}
	machineID := b.machine.ID
	label := b.machine.Label
	return b.store.Update(ctx, func(state *teamstore.State) error {
		now := time.Now()
		if state.MachineIdentity.ID == "" {
			state.MachineIdentity.ID = machineID
			state.MachineIdentity.CreatedAt = now
		}
		state.MachineIdentity.Label = label
		state.MachineIdentity.Hostname = label
		state.MachineIdentity.AccountID = b.user.ID
		state.MachineIdentity.UserPrincipal = b.user.UserPrincipalName
		state.MachineIdentity.Profile = b.scope.Profile
		state.MachineIdentity.ScopeID = b.scope.ID
		state.MachineIdentity.Kind = b.machine.Kind
		state.MachineIdentity.Priority = b.machine.Priority
		state.MachineIdentity.UpdatedAt = now
		if state.ControlChat.BoundAt.IsZero() {
			state.ControlChat.BoundAt = now
		}
		state.ControlChat.MachineID = machineID
		state.ControlChat.ScopeID = b.scope.ID
		state.ControlChat.AccountID = b.user.ID
		state.ControlChat.TeamsChatID = chat.ID
		state.ControlChat.TeamsChatURL = chat.WebURL
		state.ControlChat.TeamsChatTopic = chat.Topic
		state.ControlChat.UpdatedAt = now
		return nil
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
		CodexThreadID: session.CodexThreadID,
		RunnerKind:    "executor",
		Cwd:           session.Cwd,
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
	return b.store.PersistInbound(ctx, teamstore.InboundEvent{
		SessionID:       session.ID,
		TeamsChatID:     session.ChatID,
		TeamsMessageID:  msg.ID,
		ScopeID:         b.scope.ID,
		MachineID:       b.machine.ID,
		LeaseGeneration: leaseGeneration,
		Text:            text,
		TextHash:        normalizedTextHash(text),
		Source:          source,
		Status:          status,
	})
}

func (b *Bridge) deferSessionMessageDuringTranscriptImport(ctx context.Context, session *Session, msg ChatMessage) error {
	if session == nil {
		return nil
	}
	if err := b.ensureDurableSession(ctx, session); err != nil {
		return err
	}
	_, _, err := b.persistInboundWithStatusAndSource(ctx, session, msg, teamstore.InboundStatusDeferred, "teams_session_import_deferred")
	return err
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
	})
}

func (b *Bridge) queueAndSendOutboxChunks(ctx context.Context, sessionID string, turnID string, chatID string, kind string, text string) error {
	return b.queueAndSendOutboxChunksWithOptions(ctx, sessionID, turnID, chatID, kind, text, outboxQueueOptions{})
}

func (b *Bridge) queueAndSendOutboxChunksWithOptions(ctx context.Context, sessionID string, turnID string, chatID string, kind string, text string, opts outboxQueueOptions) error {
	queued, err := b.queueOutboxChunksWithOptions(ctx, sessionID, turnID, chatID, kind, text, opts)
	if err != nil {
		return err
	}
	if len(queued) == 0 {
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
		if opts.MentionOwner && isCompletionNotificationKind(kind, opts.NotificationKind) {
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

func (b *Bridge) queueOutbox(ctx context.Context, msg teamstore.OutboxMessage) (teamstore.OutboxMessage, error) {
	if err := b.ensureStore(); err != nil {
		return teamstore.OutboxMessage{}, err
	}
	if msg.ScopeID == "" {
		msg.ScopeID = b.scope.ID
	}
	if msg.MachineID == "" {
		msg.MachineID = b.machine.ID
	}
	if msg.LeaseGeneration == 0 {
		msg.LeaseGeneration = b.currentLeaseGeneration()
	}
	queued, _, err := b.store.QueueOutbox(ctx, msg)
	if err != nil {
		return teamstore.OutboxMessage{}, err
	}
	return queued, nil
}

func (b *Bridge) flushPendingOutbox(ctx context.Context, sessionID string, turnID string) error {
	return b.flushPendingOutboxFiltered(ctx, sessionID, turnID, "")
}

func (b *Bridge) flushPendingOutboxForChat(ctx context.Context, chatID string) error {
	return b.flushPendingOutboxFiltered(ctx, "", "", chatID)
}

func (b *Bridge) flushPendingOutboxFiltered(ctx context.Context, sessionID string, turnID string, chatID string) error {
	if err := b.ensureStore(); err != nil {
		return err
	}
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
	var firstBlockedErr error
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
		if err := b.sendQueuedOutboxWithOptions(ctx, msg, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true}); err != nil {
			if isOutboxDeliveryDeferred(err) {
				if firstBlockedErr == nil {
					firstBlockedErr = err
				}
				continue
			}
			return err
		}
	}
	return firstBlockedErr
}

func (b *Bridge) sendQueuedOutbox(ctx context.Context, outbox teamstore.OutboxMessage) error {
	return b.sendQueuedOutboxWithOptions(ctx, outbox, outboxSendOptions{RespectRateLimitBlock: true, RecordRateLimit: true})
}

type outboxSendOptions struct {
	RespectRateLimitBlock bool
	RecordRateLimit       bool
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
		_, err := b.store.MarkOutboxSent(ctx, outbox.ID, outbox.TeamsMessageID)
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
	if _, err := b.store.MarkOutboxSendAttempt(ctx, outbox.ID); errors.Is(err, teamstore.ErrOutboxSendNotClaimed) {
		return nil
	} else if err != nil {
		return err
	}
	if outbox.DriveItemID == "" && outbox.AttachmentPath != "" {
		item, err := b.uploadQueuedOutboxAttachment(ctx, outbox)
		if err != nil {
			_, _ = b.store.MarkOutboxSendError(context.Background(), outbox.ID, err.Error())
			return err
		}
		outbox, err = b.store.MarkOutboxDriveItem(ctx, outbox.ID, item.ID, item.Name, item.WebURL, item.WebDavURL)
		if err != nil {
			return err
		}
	}
	var msg ChatMessage
	var err error
	if outbox.DriveItemID != "" {
		msg, err = b.graph.SendDriveItemAttachment(ctx, outbox.TeamsChatID, driveItemFromOutbox(outbox), outbox.Body)
	} else if outbox.MentionOwner {
		body, mentions := renderOutboxMentionHTML(outbox, b.user)
		msg, err = b.graph.SendHTMLWithMentions(ctx, outbox.TeamsChatID, body, mentions)
	} else {
		msg, err = b.graph.SendHTML(ctx, outbox.TeamsChatID, renderOutboxHTML(outbox))
	}
	if err != nil {
		_, _ = b.store.MarkOutboxSendError(context.Background(), outbox.ID, err.Error())
		if opts.RecordRateLimit {
			b.recordGraphRateLimit(context.Background(), outbox.TeamsChatID, outbox.ID, err)
		}
		return err
	}
	b.reg.MarkSent(outbox.TeamsChatID, msg.ID)
	if _, err := b.store.MarkOutboxAccepted(ctx, outbox.ID, msg.ID); err != nil {
		return err
	}
	_, err = b.store.MarkOutboxSent(ctx, outbox.ID, msg.ID)
	return err
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
	return UploadOutboundAttachment(ctx, graph, file, opts)
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

func (b *Bridge) sendSingleToChat(ctx context.Context, chatID string, text string) error {
	return b.sendToChat(ctx, chatID, text)
}

func (b *Bridge) sendLongToChat(ctx context.Context, chatID string, text string) error {
	return b.sendToChat(ctx, chatID, text)
}

func (b *Bridge) discoverDashboardProjects(ctx context.Context) ([]codexhistory.Project, error) {
	if b == nil {
		return discoverCodexProjectsForTeams(ctx, "")
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
	if isChatMovedOutboxKind(outbox.Kind) {
		return renderChatMovedOutboxHTML(outbox, owner, true)
	}
	mentionText := strings.TrimSpace(firstNonEmptyString(owner.DisplayName, owner.UserPrincipalName, "owner"))
	mention := `<at id="0">` + html.EscapeString(mentionText) + `</at>`
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
	case kind == "final" || strings.HasPrefix(kind, "final-") || strings.Contains(kind, "assistant"):
		return TeamsRenderAssistant
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
			return fmt.Sprintf("Control status: no active linked work chats. %d closed work chat(s) are hidden because the helper no longer polls them.\n\nSend `projects` to choose a workspace, `new <directory>` to create a Work chat, or `sessions` then `continue <number>` to import an existing local Codex session.", closedCount)
		}
		return "Control status: no linked work chats yet.\n\nNext: send `projects` to choose a workspace, or `new <directory>` to create a Work chat."
	}
	lines := []string{"## Active Work chats"}
	for _, session := range active {
		lines = append(lines, fmt.Sprintf("- **%s** [%s]\n  %s\n  %s", session.Topic, session.Status, session.ID, session.ChatURL))
	}
	if closedCount > 0 {
		lines = append(lines, fmt.Sprintf("%d closed work chat(s) hidden. The helper no longer reads or responds in closed chats.", closedCount))
	}
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
			lines = append(lines, "Latest error: "+latest.FailureMessage)
		}
		switch latest.Status {
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
	var lines []string
	lines = append(lines, fmt.Sprintf("%s [%s] %s", session.ID, session.Status, session.Topic))
	if session.ChatURL != "" {
		lines = append(lines, session.ChatURL)
	}
	if isActiveSessionStatus(session.Status) {
		lines = append(lines, "Next: open this Teams work chat and send a message there to continue. `open` only shows the linked chat; it does not import local history.")
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
	if session.CodexThreadID != "" {
		lines = append(lines, "Codex thread: "+session.CodexThreadID)
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
	if existing := b.reg.SessionByCodexThreadID(local.SessionID); existing != nil && isActiveSessionStatus(existing.Status) {
		if err := b.ensureDurableSession(ctx, existing); err != nil {
			return "", err
		}
		if importing, err := b.sessionTranscriptImportInProgress(ctx, existing.ID); err != nil {
			return "", err
		} else if importing {
			return fmt.Sprintf("Already published as %s: %s\n\nHistory import is still running. Wait for \"Import complete\" in the Work chat before sending a new task there.", existing.ID, existing.ChatURL), nil
		}
		hasNew, err := b.transcriptHasNewRecords(ctx, existing.ID, local.FilePath)
		if err != nil {
			return "", fmt.Errorf("check history import for %s: %w", existing.ID, err)
		}
		importStatus := "No new local history was imported."
		if hasNew {
			if notify != nil {
				if err := notify(ctx, publishHistoryPreparingMessage(existing.ID, existing.ChatURL, true)); err != nil {
					return "", err
				}
			}
			if err := b.importCodexTranscriptToTeams(ctx, *existing, local); err != nil {
				return "", fmt.Errorf("resume history import for %s: %w", existing.ID, err)
			}
			importStatus = "New local history was imported."
		}
		return fmt.Sprintf("Already published as %s: %s\n\n%s Open this Teams work chat and send a message there to continue.", existing.ID, existing.ChatURL, importStatus), nil
	}
	newSessionID := b.reg.NextSessionID()
	title := WorkChatTitle(ChatTitleOptions{
		MachineLabel: firstNonEmptyString(b.machine.Label, machineLabel()),
		Profile:      b.scope.Profile,
		SessionID:    newSessionID,
		Topic:        local.DisplayTitle(),
		Cwd:          firstNonEmptyString(local.ProjectPath, project.Path),
	})
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
		Status:        "active",
		CodexThreadID: local.SessionID,
		Cwd:           firstNonEmptyString(local.ProjectPath, project.Path),
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
	if notify != nil {
		if err := notify(ctx, publishHistoryPreparingMessage(session.ID, session.ChatURL, false)); err != nil {
			return "", err
		}
	}
	if err := b.sendChatCreatedMention(ctx, session.ID, chat.ID, "Work chat created: "+session.ID+"."); err != nil {
		return "", err
	}
	if err := b.importCodexTranscriptToTeams(ctx, session, local); err != nil {
		return "", err
	}
	return fmt.Sprintf("Published local Codex session as %s: %s\n\nOpen this Teams work chat and send a message there to continue.", session.ID, session.ChatURL), nil
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
	view, ok, err := b.loadDashboardView(ctx)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("run `sessions` first so the helper can resolve session number %d", target.Number)
	}
	selection, err := ResolveDashboardNumber(ChatScopeControl, view, target.Number, time.Now())
	if err != nil {
		return "", err
	}
	if selection.Kind != DashboardSelectionSession {
		return "", fmt.Errorf("number %d is a directory in the current list; send `project %d` first to list its sessions, then `continue <session-number>`", target.Number, target.Number)
	}
	return selection.SessionID, nil
}

func (b *Bridge) importCodexTranscriptToTeams(ctx context.Context, session Session, local codexhistory.Session) error {
	importTurnID := "import:" + session.ID
	if err := b.markTranscriptImportStarted(ctx, session, local.FilePath); err != nil {
		return err
	}
	title := "Imported Codex session history\n\nThe messages below came from your local Codex session. Reply in this chat to continue from here.\n\nSession: " + local.DisplayTitle()
	if err := b.queueAndSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "import-title", title); err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		return err
	}
	lastRecordID, lastLine, stats, err := b.importTranscriptRecordsToTeams(ctx, session, local.FilePath, importTurnID, "import", transcriptCheckpointID(session.ID))
	if err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		return err
	}
	if err := b.importSubagentMarkersToTeams(ctx, session, local, importTurnID); err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		return err
	}
	complete := formatTranscriptImportCompleteMessage(stats)
	if err := b.queueAndSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "import-complete", complete); err != nil {
		_ = b.markTranscriptImportFailed(ctx, session, local.FilePath)
		return err
	}
	return b.markTranscriptImportComplete(ctx, session, local.FilePath, lastRecordID, lastLine)
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

func (b *Bridge) importTranscriptRecordsToTeams(ctx context.Context, session Session, filePath string, importTurnID string, kindPrefix string, checkpointID string) (string, int, transcriptImportStats, error) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return "", 0, transcriptImportStats{}, nil
	}
	transcript, err := ReadSessionTranscript(filePath)
	if err != nil {
		return "", 0, transcriptImportStats{}, fmt.Errorf("read local transcript: %w", err)
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return "", 0, transcriptImportStats{}, err
	}
	if checkpointID == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	if checkpoint := state.ImportCheckpoints[checkpointID]; checkpoint.LastRecordID != "" {
		transcript, err = ReadSessionTranscriptSince(filePath, checkpoint.LastRecordID)
		if err != nil {
			_ = b.markTranscriptImportFailedWithID(ctx, session, filePath, checkpointID)
			return "", 0, transcriptImportStats{}, err
		}
		if transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
			_ = b.markTranscriptImportFailedWithID(ctx, session, filePath, checkpointID)
			return "", 0, transcriptImportStats{}, fmt.Errorf("transcript checkpoint was not found; run `continue` again only after local history is intact")
		}
	}
	stats := transcriptImportStats{Total: len(transcript.Records)}
	dedupe := newTranscriptDedupeState()
	batcher := newTranscriptImportBatcher(b, session, filePath, importTurnID, kindPrefix, checkpointID)
	for i, record := range transcript.Records {
		if strings.TrimSpace(record.Text) == "" {
			continue
		}
		checkpointKey := transcriptRecordCheckpointKey(record)
		if shouldSkipImportedTranscriptRecord(record) {
			stats.SkippedBackground++
			if err := batcher.recordCheckpoint(ctx, checkpointKey, record.SourceLine); err != nil {
				return "", 0, stats, err
			}
			continue
		}
		body := formatTranscriptRecordForTeams(record)
		if strings.TrimSpace(body) == "" {
			if err := batcher.recordCheckpoint(ctx, checkpointKey, record.SourceLine); err != nil {
				return "", 0, stats, err
			}
			continue
		}
		if dedupe.shouldSkip(record, body) {
			if err := batcher.recordCheckpoint(ctx, checkpointKey, record.SourceLine); err != nil {
				return "", 0, stats, err
			}
			continue
		}
		kind := transcriptRecordOutboxKind(kindPrefix, record, i+1)
		if err := batcher.add(ctx, transcriptImportBatchRecord{
			Record:        record,
			Kind:          kind,
			Body:          body,
			CheckpointKey: checkpointKey,
		}); err != nil {
			return "", 0, stats, err
		}
		stats.Imported++
	}
	if err := batcher.flush(ctx); err != nil {
		return "", 0, stats, err
	}
	if len(transcript.Records) == 0 {
		return "", 0, stats, nil
	}
	last := transcript.Records[len(transcript.Records)-1]
	return transcriptRecordCheckpointKey(last), last.SourceLine, stats, nil
}

type transcriptImportBatchRecord struct {
	Record        TranscriptRecord
	Kind          string
	Body          string
	CheckpointKey string
}

type transcriptImportCheckpointRecord struct {
	Key        string
	SourceLine int
}

type transcriptImportBatcher struct {
	bridge       *Bridge
	session      Session
	filePath     string
	importTurnID string
	kindPrefix   string
	checkpointID string
	records      []transcriptImportBatchRecord
	checkpoints  []transcriptImportCheckpointRecord
	htmlParts    []string
	htmlBytes    int
	batchIndex   int
}

func newTranscriptImportBatcher(b *Bridge, session Session, filePath string, importTurnID string, kindPrefix string, checkpointID string) *transcriptImportBatcher {
	return &transcriptImportBatcher{
		bridge:       b,
		session:      session,
		filePath:     filePath,
		importTurnID: importTurnID,
		kindPrefix:   strings.TrimSpace(kindPrefix),
		checkpointID: checkpointID,
	}
}

func (b *transcriptImportBatcher) add(ctx context.Context, record transcriptImportBatchRecord) error {
	html := renderTeamsHTMLPart(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    renderKindForOutbox(record.Kind),
		Text:    record.Body,
	}, 1, 1)
	if len(html) > teamsChunkHTMLContentBytes {
		if err := b.flush(ctx); err != nil {
			return err
		}
		if err := b.bridge.queueAndSendOutboxChunks(ctx, b.session.ID, b.importTurnID, b.session.ChatID, record.Kind, record.Body); err != nil {
			return err
		}
		return b.bridge.recordTranscriptCheckpointWithID(ctx, b.session, b.filePath, record.CheckpointKey, record.Record.SourceLine, b.checkpointID)
	}
	addedBytes := len(html)
	if len(b.htmlParts) > 0 {
		addedBytes += len(transcriptImportBatchSeparatorHTML)
	}
	if len(b.htmlParts) > 0 && b.htmlBytes+addedBytes > teamsChunkHTMLContentBytes {
		if err := b.flush(ctx); err != nil {
			return err
		}
	}
	if len(b.htmlParts) > 0 {
		b.htmlBytes += len(transcriptImportBatchSeparatorHTML)
	}
	b.records = append(b.records, record)
	b.checkpoints = append(b.checkpoints, transcriptImportCheckpointRecord{Key: record.CheckpointKey, SourceLine: record.Record.SourceLine})
	b.htmlParts = append(b.htmlParts, html)
	b.htmlBytes += len(html)
	return nil
}

func (b *transcriptImportBatcher) recordCheckpoint(ctx context.Context, checkpointKey string, sourceLine int) error {
	if len(b.records) == 0 {
		return b.bridge.recordTranscriptCheckpointWithID(ctx, b.session, b.filePath, checkpointKey, sourceLine, b.checkpointID)
	}
	b.checkpoints = append(b.checkpoints, transcriptImportCheckpointRecord{Key: checkpointKey, SourceLine: sourceLine})
	return nil
}

func (b *transcriptImportBatcher) flush(ctx context.Context) error {
	if len(b.records) == 0 {
		return nil
	}
	b.batchIndex++
	html := strings.Join(b.htmlParts, transcriptImportBatchSeparatorHTML)
	first := b.records[0]
	last := b.records[len(b.records)-1]
	kind := transcriptImportBatchOutboxKind(b.kindPrefix, first.Record, last.Record, b.batchIndex)
	if err := b.bridge.queueAndSendTranscriptImportBatch(ctx, b.session.ID, b.importTurnID, b.session.ChatID, kind, html); err != nil {
		return err
	}
	for _, checkpoint := range b.checkpoints {
		if err := b.bridge.recordTranscriptCheckpointWithID(ctx, b.session, b.filePath, checkpoint.Key, checkpoint.SourceLine, b.checkpointID); err != nil {
			return err
		}
	}
	b.records = nil
	b.checkpoints = nil
	b.htmlParts = nil
	b.htmlBytes = 0
	return nil
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

func (b *Bridge) queueAndSendTranscriptImportBatch(ctx context.Context, sessionID string, turnID string, chatID string, kind string, html string) error {
	html = strings.TrimSpace(html)
	if html == "" {
		return nil
	}
	return b.queueAndSendOutbox(ctx, teamstore.OutboxMessage{
		SessionID:     sessionID,
		TurnID:        turnID,
		TeamsChatID:   chatID,
		Kind:          kind,
		Body:          html,
		PartIndex:     1,
		PartCount:     1,
		RenderedBytes: len(html),
	})
}

func (b *Bridge) importSubagentMarkersToTeams(ctx context.Context, session Session, local codexhistory.Session, importTurnID string) error {
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
		sourcePath := strings.TrimSpace(subagent.FilePath)
		if err := b.markTranscriptImportStartedWithID(ctx, session, sourcePath, checkpointID); err != nil {
			return err
		}
		marker := formatSubagentImportMarker(local, subagent)
		if err := b.queueAndSendOutboxChunks(ctx, session.ID, importTurnID, session.ChatID, "import-subagent-marker-"+key, marker); err != nil {
			return err
		}
		if err := b.markTranscriptImportCompleteWithID(ctx, session, sourcePath, "subagent:"+key, 0, checkpointID); err != nil {
			return err
		}
	}
	return nil
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

func (b *Bridge) transcriptHasNewRecords(ctx context.Context, sessionID string, filePath string) (bool, error) {
	filePath = strings.TrimSpace(filePath)
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
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(sessionID)]
	if checkpoint.Status != "" && checkpoint.Status != importCheckpointStatusComplete {
		return true, nil
	}
	if checkpoint.LastRecordID == "" {
		return true, nil
	}
	transcript, err := ReadSessionTranscriptSince(filePath, checkpoint.LastRecordID)
	if err != nil {
		return false, err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
		return false, fmt.Errorf("transcript checkpoint was not found; refusing to guess an import position")
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
	CheckpointHadRecord     bool
	HasActionableTranscript bool
}

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
		return localCodexBeforeTeamsGate{
			Block:   true,
			AckBody: "⏳ Queued. I’m preparing this chat history first, then I’ll respond.",
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
		remaining, err := b.localTranscriptHasActionableDelta(ctx, *session, local)
		if err != nil {
			if os.IsNotExist(err) {
				return localCodexBeforeTeamsGate{}, nil
			}
			return localCodexBeforeTeamsGate{}, err
		}
		if remaining {
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
	out.CheckpointHadRecord = checkpoint.LastRecordID != ""
	switch checkpoint.Status {
	case importCheckpointStatusImporting, importCheckpointStatusFailed:
		return out, nil
	}
	var transcript Transcript
	if checkpoint.LastRecordID == "" {
		transcript, err = ReadSessionTranscript(local.FilePath)
	} else {
		transcript, err = ReadSessionTranscriptSince(local.FilePath, checkpoint.LastRecordID)
	}
	if err != nil {
		return out, err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
		return out, fmt.Errorf("transcript checkpoint was not found; refusing to guess a sync position")
	}
	if len(transcript.Records) == 0 {
		return out, nil
	}
	teamsOriginHashes := teamsOriginTextHashes(state, session.ID)
	knownHashes := knownTranscriptOutboxHashes(state, session.ID)
	dedupe := newTranscriptDedupeState()
	active := false
	for i, record := range transcript.Records {
		body := formatTranscriptRecordForTeams(record)
		if strings.TrimSpace(body) == "" {
			continue
		}
		if shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginHashes) ||
			shouldSkipKnownTranscriptOutboxRecord(record, body, knownHashes) ||
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
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	activeTeamsTurns := runningTurnSessions(state)
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
	for _, session := range b.reg.ActiveSessions() {
		if session.CodexThreadID == "" {
			continue
		}
		if activeTeamsTurns[session.ID] {
			continue
		}
		local, ok := byID[session.CodexThreadID]
		if !ok || strings.TrimSpace(local.FilePath) == "" {
			continue
		}
		if err := b.syncSessionTranscript(ctx, session, local); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bridge) syncSessionTranscript(ctx context.Context, session Session, local codexhistory.Session) error {
	checkpointID := transcriptCheckpointID(session.ID)
	state, err := b.store.Load(ctx)
	if err != nil {
		return err
	}
	if runningTurnSessions(state)[session.ID] {
		return nil
	}
	checkpoint, hasCheckpoint := state.ImportCheckpoints[checkpointID]
	if hasCheckpoint {
		switch checkpoint.Status {
		case importCheckpointStatusImporting:
			return nil
		case importCheckpointStatusBlocked:
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
	if !hasCheckpoint || checkpoint.LastRecordID == "" {
		transcript, err := ReadSessionTranscript(local.FilePath)
		if err != nil {
			return err
		}
		if len(transcript.Records) == 0 {
			return nil
		}
		last := transcript.Records[len(transcript.Records)-1]
		return b.recordTranscriptCheckpoint(ctx, session, local.FilePath, firstNonEmptyString(last.DedupeKey, last.ItemID), last.SourceLine)
	}
	transcript, err := ReadSessionTranscriptSince(local.FilePath, checkpoint.LastRecordID)
	if err != nil {
		return err
	}
	if transcriptHasDiagnostic(transcript, "checkpoint_not_found") {
		return fmt.Errorf("transcript checkpoint was not found; refusing to guess a sync position")
	}
	if len(transcript.Records) == 0 {
		return nil
	}
	if len(transcript.Records) > transcriptSyncMaxAutoBacklogRecords {
		return b.blockAutomaticTranscriptSync(ctx, session, local.FilePath, checkpoint, len(transcript.Records))
	}
	teamsOriginHashes := teamsOriginTextHashes(state, session.ID)
	knownHashes := knownTranscriptOutboxHashes(state, session.ID)
	dedupe := newTranscriptDedupeState()
	syncTurnID := "sync:" + session.ID
	sent := 0
	for i, record := range transcript.Records {
		if strings.TrimSpace(record.Text) == "" {
			continue
		}
		body := formatTranscriptRecordForTeams(record)
		if strings.TrimSpace(body) == "" {
			if err := b.recordTranscriptCheckpoint(ctx, session, local.FilePath, transcriptRecordCheckpointKey(record), record.SourceLine); err != nil {
				return err
			}
			continue
		}
		if shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginHashes) ||
			shouldSkipKnownTranscriptOutboxRecord(record, body, knownHashes) ||
			shouldSkipBackgroundTranscriptRecord(record) ||
			dedupe.shouldSkip(record, body) {
			if err := b.recordTranscriptCheckpoint(ctx, session, local.FilePath, transcriptRecordCheckpointKey(record), record.SourceLine); err != nil {
				return err
			}
			continue
		}
		kind := transcriptRecordOutboxKind("sync", record, i+1)
		opts := transcriptSyncOutboxOptions(record)
		if err := b.queueAndSendOutboxChunksWithOptions(ctx, session.ID, syncTurnID, session.ChatID, kind, body, opts); err != nil {
			return err
		}
		if err := b.recordTranscriptCheckpoint(ctx, session, local.FilePath, transcriptRecordCheckpointKey(record), record.SourceLine); err != nil {
			return err
		}
		sent++
		if sent >= transcriptSyncMaxRecordsPerSessionPerCycle {
			return nil
		}
	}
	return nil
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

func (b *Bridge) blockAutomaticTranscriptSync(ctx context.Context, session Session, sourcePath string, checkpoint teamstore.ImportCheckpoint, backlogRecords int) error {
	if err := b.markTranscriptImportBlocked(ctx, session, sourcePath, checkpoint); err != nil {
		return err
	}
	body := fmt.Sprintf("Local Codex history has %d new items, so I paused automatic history sync to avoid flooding this Teams chat.\n\nNo history was skipped. To import the backlog, send `helper publish-history` here.", backlogRecords)
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
	return checkpoint.Status == importCheckpointStatusImporting
}

func (b *Bridge) sessionTranscriptImportInProgress(ctx context.Context, sessionID string) (bool, error) {
	if b == nil || b.store == nil || strings.TrimSpace(sessionID) == "" {
		return false, nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	return transcriptImportIsActive(state, sessionID), nil
}

func teamsOriginTextHashes(state teamstore.State, sessionID string) map[string]bool {
	hashes := make(map[string]bool)
	for _, inbound := range state.InboundEvents {
		if inbound.SessionID != sessionID || inbound.TextHash == "" || inbound.TurnID == "" {
			continue
		}
		if inbound.Source != "" && inbound.Source != "teams" {
			continue
		}
		hashes[inbound.TextHash] = true
	}
	return hashes
}

func shouldSkipTeamsOriginTranscriptRecord(record TranscriptRecord, body string, hashes map[string]bool) bool {
	if record.Kind != TranscriptKindUser {
		return false
	}
	hash := normalizedTextHash(body)
	return hash != "" && hashes[hash]
}

func knownTranscriptOutboxHashes(state teamstore.State, sessionID string) map[TranscriptKind]map[string]bool {
	hashes := make(map[TranscriptKind]map[string]bool)
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
		hash := normalizedTextHash(formatKnownOutboxBodyForTranscriptDedupe(kind, outbox.Body))
		if hash != "" {
			if hashes[kind] == nil {
				hashes[kind] = make(map[string]bool)
			}
			hashes[kind][hash] = true
		}
		if outbox.SourceTextHash != "" {
			if hashes[kind] == nil {
				hashes[kind] = make(map[string]bool)
			}
			hashes[kind][outbox.SourceTextHash] = true
		}
	}
	return hashes
}

func outboxCanDedupeTranscript(outbox teamstore.OutboxMessage) bool {
	switch outbox.Status {
	case teamstore.OutboxStatusQueued, teamstore.OutboxStatusSending, teamstore.OutboxStatusAccepted, teamstore.OutboxStatusSent:
		return true
	default:
		return false
	}
}

func deliveredOutboxTranscriptKind(kind string) (TranscriptKind, bool) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch {
	case isFinalOutboxKind(kind) || strings.Contains(kind, "assistant"):
		return TranscriptKindAssistant, true
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
	if record.Kind != TranscriptKindAssistant && record.Kind != TranscriptKindStatus {
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
	if strings.TrimSpace(lastRecordID) == "" {
		return nil
	}
	if strings.TrimSpace(checkpointID) == "" {
		checkpointID = transcriptCheckpointID(session.ID)
	}
	return b.store.UpdateSession(ctx, session.ID, func(state *teamstore.State) error {
		now := time.Now()
		id := checkpointID
		status := state.ImportCheckpoints[id].Status
		if status == "" {
			status = importCheckpointStatusComplete
		}
		state.ImportCheckpoints[id] = teamstore.ImportCheckpoint{
			ID:             id,
			SessionID:      session.ID,
			SourcePath:     sourcePath,
			LastRecordID:   lastRecordID,
			LastSourceLine: lastLine,
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
		checkpoint.Status = importCheckpointStatusImporting
		checkpoint.UpdatedAt = now
		state.ImportCheckpoints[id] = checkpoint
		return nil
	})
}

func (b *Bridge) markTranscriptImportComplete(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int) error {
	return b.markTranscriptImportCompleteWithID(ctx, session, sourcePath, lastRecordID, lastLine, transcriptCheckpointID(session.ID))
}

func (b *Bridge) markTranscriptImportCompleteWithID(ctx context.Context, session Session, sourcePath string, lastRecordID string, lastLine int, checkpointID string) error {
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
		if strings.TrimSpace(lastRecordID) != "" {
			checkpoint.LastRecordID = lastRecordID
			checkpoint.LastSourceLine = lastLine
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
	dashboard := BuildControlDashboard(previous, ControlDashboardInput{
		Workspaces: dashboardWorkspacesFromProjects(projects),
		ViewKind:   DashboardViewWorkspaces,
	}, time.Now())
	if err := b.persistControlDashboard(ctx, dashboard); err != nil {
		return "", err
	}
	if len(dashboard.Workspaces) == 0 {
		return "## 📁 Workspaces\n\nNo local Codex workspaces found on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.\n\nNext: send `new <directory>` to create a Work chat for a directory.", nil
	}
	lines := []string{
		"## 📁 Workspaces",
		"",
		"Reply with a number to open a workspace.",
		"",
	}
	for _, workspace := range dashboard.Workspaces {
		meta := fmt.Sprintf("%d sessions", workspace.SessionCount)
		if workspace.SessionCount == 1 {
			meta = "1 session"
		}
		if !workspace.UpdatedAt.IsZero() {
			meta += " - updated " + workspace.UpdatedAt.Local().Format("2006-01-02 15:04")
		}
		pathLine := dashboardWorkspacePathDisplay(workspace.Path)
		lines = append(lines, fmt.Sprintf("`%d` - %s\n   %s", workspace.Number, pathLine, meta))
	}
	lines = append(lines, "", "Next:", "- `1` - open workspace 1", "- `n <directory>` or `new <directory>` - create a new Work chat directly")
	lines = append(lines, "", "Numbers expire after 10 minutes. If a number looks wrong, send `projects` again.")
	return strings.Join(lines, "\n"), nil
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
		view, ok, err := b.loadDashboardView(ctx)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", fmt.Errorf("run `projects` first so the helper can resolve workspace number %d", target.Number)
		}
		selection, err := ResolveDashboardNumber(ChatScopeControl, view, target.Number, time.Now())
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
		selectedWorkspaceID = view.WorkspaceID
	}

	previous := b.previousControlDashboard(ctx)
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
			return fmt.Sprintf("`new` - create a new Work chat in this workspace.\n\n## Sessions\n\nWorkspace: %s\n\nNo local Codex sessions were found in this workspace on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.", dashboardWorkspacePathDisplay(selectedWorkspace.Path)), nil
		}
		return "## Sessions\n\nNo local Codex sessions found on this machine.\n\nNext: send `projects` to choose a workspace, or `new <directory>` to create a Work chat.", nil
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
		"`new` - create a new Work chat in this workspace.",
		"",
		"## " + heading,
		"",
		workspaceLine,
		"",
		"Reply with a number to continue a session in Teams.",
		"",
	}
	if strings.TrimSpace(workspaceLine) == "" {
		lines = []string{
			"`new` - create a new Work chat in this workspace.",
			"",
			"## " + heading,
			"",
			"Reply with a number to continue a session in Teams.",
			"",
		}
	}
	for _, item := range dashboard.CurrentView.Items {
		session, ok := sessions[sessionKey(item.WorkspaceID, item.SessionID)]
		if !ok {
			continue
		}
		action := fmt.Sprintf("send `%d` or `c %d`", item.Number, item.Number)
		if linked := b.linkedSessionForLocalSessionID(session.ID); linked != nil {
			if isActiveSessionStatus(linked.Status) {
				action = fmt.Sprintf("already in Teams -> send `%d` to open/import updates", item.Number)
			} else {
				action = fmt.Sprintf("closed Teams chat -> send `%d` for a new Work chat", item.Number)
			}
		}
		parts := []string{"**" + session.DisplayTitle + "**"}
		if meta := dashboardSessionListMeta(session); meta != "" {
			parts = append(parts, meta)
		}
		parts = append(parts, action)
		lines = append(lines, fmt.Sprintf("`%d` - %s", item.Number, strings.Join(parts, "\n   ")))
	}
	lines = append(lines, "", "Need debug IDs? Send `details <number>`. Numbers expire after 10 minutes.")
	return strings.Join(lines, "\n"), nil
}

func dashboardSessionListMeta(session DashboardSession) string {
	if session.UpdatedAt.IsZero() {
		return ""
	}
	return "updated " + session.UpdatedAt.Local().Format("2006-01-02 15:04")
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
		return b.formatSessionSelection(selection), nil
	}
	if session := b.linkedSessionForLocalSessionID(target.Raw); session != nil {
		return b.formatOpenSession(session.ID), nil
	}
	if b.localCodexSessionExists(ctx, target.Raw) {
		return b.localSessionNotInTeamsMessage(0, strings.TrimSpace(target.Raw)), nil
	}
	return b.formatOpenSession(target.Raw), nil
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
	if _, err := b.store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:            match.ChatID,
		PollState:         inboundPollStateHot,
		PreviousPollState: "",
		NextPollAt:        now,
		LastActivityAt:    now,
		ClearBlockedUntil: true,
		ResetFailures:     true,
	}); err != nil {
		return "", err
	}
	match.UpdatedAt = now
	_ = b.ensureDurableSession(ctx, match)
	b.boostPolling(now)
	if match.ChatURL != "" {
		return fmt.Sprintf("Resumed %s.\n\nOpen Work chat: %s\n\nMessages in that chat will be read again.", match.ID, match.ChatURL), nil
	}
	return fmt.Sprintf("Resumed %s.\n\nMessages in that Work chat will be read again.", match.ID), nil
}

func (b *Bridge) resolveControlSelection(ctx context.Context, target DashboardCommandTarget) (string, error) {
	if !target.IsNumber {
		return "", ErrDashboardNotBareNumber
	}
	selection, err := b.resolveDashboardTarget(ctx, target.Number)
	if err != nil {
		return "", err
	}
	switch selection.Kind {
	case DashboardSelectionWorkspace:
		return b.formatWorkspaceSessionsDashboard(ctx, DashboardCommandTarget{Number: selection.Number, IsNumber: true})
	case DashboardSelectionSession:
		return b.publishCodexSessionWithProgress(ctx, DashboardCommandTarget{Raw: strconvItoa(selection.Number), Number: selection.Number, IsNumber: true}, b.sendControl)
	default:
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
	return ResolveDashboardNumber(ChatScopeControl, view, number, time.Now())
}

func (b *Bridge) formatSessionSelection(selection DashboardSelection) string {
	session := b.linkedSessionForLocalSessionID(selection.SessionID)
	if session == nil {
		return b.localSessionNotInTeamsMessage(selection.Number, selection.SessionID)
	}
	if !isActiveSessionStatus(session.Status) {
		return fmt.Sprintf("This Codex session has a closed Teams work chat. The helper no longer polls that chat.\nNext: send `continue %d` to create a new work chat and continue from local history.\nUse `details %d` to show technical IDs.", selection.Number, selection.Number)
	}
	return b.formatOpenSession(session.ID)
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

func dashboardWorkspacesFromProjects(projects []codexhistory.Project) []DashboardWorkspaceInput {
	workspaces := make([]DashboardWorkspaceInput, 0, len(projects))
	for _, project := range projects {
		sessions := make([]DashboardSessionInput, 0, len(project.Sessions))
		for _, session := range project.Sessions {
			sessions = append(sessions, DashboardSessionInput{
				ID:            session.SessionID,
				WorkspaceID:   workspaceIDForPath(project.Path),
				Cwd:           firstNonEmptyString(session.ProjectPath, project.Path),
				Topic:         session.DisplayTitle(),
				Status:        "local",
				CodexThreadID: session.SessionID,
				CreatedAt:     session.CreatedAt,
				UpdatedAt:     session.ModifiedAt,
			})
		}
		workspaces = append(workspaces, DashboardWorkspaceInput{
			ID:        workspaceIDForPath(project.Path),
			Path:      project.Path,
			UpdatedAt: latestProjectModified(project),
			Sessions:  sessions,
		})
	}
	return workspaces
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
		if session.ModifiedAt.After(latest) {
			latest = session.ModifiedAt
		}
	}
	return latest
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
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrDashboardViewExpired):
		return "This numbered list expired. Send `projects` or `sessions` again, then choose one of the newly shown numbers."
	case errors.Is(err, ErrDashboardViewMissing):
		return "I do not have a current list yet. Send `projects` or `sessions` first, then choose a number."
	case errors.Is(err, ErrDashboardNumberMissing):
		return "That number is not in the current list. Send `projects` or `sessions` again, then choose one of the newly shown numbers."
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
