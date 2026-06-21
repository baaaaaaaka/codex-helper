package teams

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
)

const (
	delegationInboxDrainTop                = 50
	delegationWorkerTerminalExecutionLimit = 512
	delegationWorkerTerminalOutboxLimit    = 512

	// DefaultMachineDelegationClaimRecheckDelay gives competing helpers enough
	// time for their claims to become visible before a worker starts expensive work.
	DefaultMachineDelegationClaimRecheckDelay = 5 * time.Second

	delegationReuseRejectedPrefix = "CXP_REUSE_REJECTED:"
)

type machineRegistryExactTopLister interface {
	ListMessagesExactTopWithoutRateLimitRetry(ctx context.Context, chatID string, top int) ([]machineregistry.ChatMessage, error)
}

type machineRegistryWindowLister interface {
	ListMessagesWindow(ctx context.Context, chatID string, top int) (machineregistry.MessageWindow, error)
	ListMessagesWindowFromPath(ctx context.Context, path string) (machineregistry.MessageWindow, error)
}

func (p *bridgeMachineRegistryPublisher) pollDelegationInboxDelay(ctx context.Context, logError func(error)) time.Duration {
	if delay := p.delegationInboxBackoffDelay(); delay > 0 {
		return delay
	}
	if err := p.pollDelegationInbox(ctx); err != nil {
		if logError != nil {
			logError(err)
		}
		if retryAfter := delegationInboxRetryAfter(err); retryAfter > 0 {
			_ = p.recordDelegationInboxBackoff(retryAfter, err.Error())
			return retryAfter
		}
		return machineRegistryNextBackoff(0, p.inboxPollInterval)
	}
	return p.inboxPollInterval
}

func (p *bridgeMachineRegistryPublisher) pollDelegationInbox(ctx context.Context) error {
	if p == nil || p.executor == nil {
		return nil
	}
	chatID := strings.TrimSpace(p.cache.InboxChatID)
	if chatID == "" {
		return nil
	}
	head, err := p.listDelegationInboxHead(ctx, chatID)
	if err != nil {
		return err
	}
	if len(head) == 0 {
		return nil
	}
	headID := strings.TrimSpace(head[0].ID)
	if headID != "" && headID == p.lastInboxHeadID {
		return nil
	}
	messages, err := p.drainDelegationInboxMessages(ctx, chatID)
	if err != nil {
		return err
	}
	p.lastInboxHeadID = headID
	records := delegation.ObserveRecords(machineDelegationMessages(messages))
	states := reduceOpenDelegations(records, p.now().UTC())
	for _, state := range states {
		if state.Status == delegation.StateCanceled {
			p.cancelDelegationActive(state.DelegationID)
			continue
		}
		if state.Request == nil || strings.TrimSpace(state.Request.MachineID) != strings.TrimSpace(p.machineID) {
			continue
		}
		if state.Status != delegation.StateOpen {
			continue
		}
		if !p.tryMarkDelegationActive(*state.Request) {
			continue
		}
		if err := p.claimAndRunDelegation(ctx, chatID, *state.Request); err != nil {
			p.clearDelegationActive(state.DelegationID)
			return err
		}
	}
	return nil
}

func (p *bridgeMachineRegistryPublisher) listDelegationInboxHead(ctx context.Context, chatID string) ([]machineregistry.ChatMessage, error) {
	if exact, ok := p.store.Graph.(machineRegistryExactTopLister); ok {
		return exact.ListMessagesExactTopWithoutRateLimitRetry(ctx, chatID, 1)
	}
	return p.store.Graph.ListMessages(ctx, chatID, 1)
}

func (p *bridgeMachineRegistryPublisher) claimAndRunDelegation(ctx context.Context, chatID string, request delegation.Record) error {
	now := p.now().UTC()
	claim, err := delegation.NewClaimRecord(request.DelegationID, p.machineID, p.workerInstanceID(), 1, now)
	if err != nil {
		return err
	}
	claim.InboxRef = strings.TrimSpace(p.cache.InboxExternalID)
	claim.InboxGeneration = strings.TrimSpace(p.cache.InboxGeneration)
	if err := p.sendDelegationInboxRecord(ctx, chatID, claim); err != nil {
		return err
	}
	if err := p.waitBeforeDelegationClaimRecheck(ctx); err != nil {
		return err
	}
	messages, err := p.drainDelegationInboxMessages(ctx, chatID)
	if err != nil {
		return err
	}
	records := delegation.RecordsForID(delegation.ObserveRecords(machineDelegationMessages(messages)), request.DelegationID)
	state := delegation.Reduce(records, now)
	if state.WinningClaim == nil || state.WinningClaim.RecordID != claim.RecordID {
		p.clearDelegationActive(request.DelegationID)
		return nil
	}
	started, err := p.tryStartDelegationExecution(request, claim)
	if err != nil {
		p.clearDelegationActive(request.DelegationID)
		return err
	}
	if !started {
		p.clearDelegationActive(request.DelegationID)
		return nil
	}
	running, err := delegation.NewStatusRecord(request.DelegationID, claim, delegation.StateRunning, "Delegated worker accepted the task.", p.now().UTC())
	if err == nil {
		running.InboxRef = strings.TrimSpace(p.cache.InboxExternalID)
		running.InboxGeneration = strings.TrimSpace(p.cache.InboxGeneration)
		_ = p.sendDelegationInboxRecord(ctx, chatID, running)
	}
	execCtx, cancel := context.WithCancel(context.Background())
	p.setDelegationActiveCancel(request.DelegationID, cancel)
	go p.executeDelegation(execCtx, chatID, request, claim)
	return nil
}

func (p *bridgeMachineRegistryPublisher) waitBeforeDelegationClaimRecheck(ctx context.Context) error {
	delay := p.claimRecheckDelay
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *bridgeMachineRegistryPublisher) executeDelegation(ctx context.Context, chatID string, request delegation.Record, claim delegation.Record) {
	defer p.clearDelegationActive(request.DelegationID)
	status := delegation.StateComplete
	body := ""
	result, err := p.executor.Run(ctx, &Session{
		ID:        delegationExecutionSessionID(request),
		Topic:     firstNonEmptyString(request.Spec.Title, "Delegated CXP task"),
		Status:    "active",
		CreatedAt: p.now().UTC(),
		UpdatedAt: p.now().UTC(),
	}, delegationPrompt(request))
	if ctx.Err() != nil {
		_ = p.finishDelegationExecution(request, claim, delegation.StateCanceled)
		return
	}
	if err != nil {
		status = delegation.StateBlocked
		body = "Delegated worker failed: " + err.Error()
	} else {
		body = strings.TrimSpace(result.Text)
		if request.ThreadPolicy == delegation.ThreadPolicyReuse {
			if reason, ok := parseDelegationReuseRejected(body); ok {
				status = delegation.StateReuseRejected
				body = reason
			}
		}
		if body == "" {
			body = "Delegated worker completed without a final message."
		}
	}
	record, err := delegation.NewResultRecord(request.DelegationID, claim, status, body, 1, p.now().UTC())
	if err != nil {
		return
	}
	record.InboxRef = strings.TrimSpace(p.cache.InboxExternalID)
	record.InboxGeneration = strings.TrimSpace(p.cache.InboxGeneration)
	record.ThreadUpdate = delegationThreadUpdate(request, status, body)
	_ = p.sendDelegationInboxRecord(ctx, chatID, record)
	_ = p.finishDelegationExecution(request, claim, status)
}

func (p *bridgeMachineRegistryPublisher) drainDelegationInboxMessages(ctx context.Context, chatID string) ([]machineregistry.ChatMessage, error) {
	windowLister, ok := p.store.Graph.(machineRegistryWindowLister)
	if !ok {
		messages, err := p.store.Graph.ListMessages(ctx, chatID, delegationInboxDrainTop)
		if err == nil {
			p.updateDelegationInboxCursor(chatID, firstMachineRegistryMessageID(messages))
		}
		return messages, err
	}
	cursor := p.delegationInboxCursor(chatID)
	var (
		out      []machineregistry.ChatMessage
		nextPath string
		headID   string
	)
	for page := 0; page < 20; page++ {
		var (
			window machineregistry.MessageWindow
			err    error
		)
		if page == 0 {
			window, err = windowLister.ListMessagesWindow(ctx, chatID, delegationInboxDrainTop)
		} else {
			window, err = windowLister.ListMessagesWindowFromPath(ctx, nextPath)
		}
		if err != nil {
			return nil, err
		}
		if page == 0 {
			headID = firstMachineRegistryMessageID(window.Messages)
		}
		for _, msg := range window.Messages {
			if strings.TrimSpace(cursor.LastHeadMessageID) != "" && strings.TrimSpace(msg.ID) == strings.TrimSpace(cursor.LastHeadMessageID) {
				p.updateDelegationInboxCursor(chatID, headID)
				return out, nil
			}
			out = append(out, msg)
		}
		if !window.Truncated || strings.TrimSpace(window.NextPath) == "" {
			p.updateDelegationInboxCursor(chatID, headID)
			return out, nil
		}
		nextPath = window.NextPath
	}
	p.updateDelegationInboxCursor(chatID, headID)
	return out, nil
}

func (p *bridgeMachineRegistryPublisher) sendDelegationInboxRecord(ctx context.Context, chatID string, record delegation.Record) error {
	if err := p.updateDelegationOutbox(record, delegation.OutboxPending, chatID, "", ""); err != nil {
		return err
	}
	msg, sendErr := p.store.Graph.SendHTML(ctx, chatID, delegation.RenderRecordHTML(record))
	if sendErr != nil {
		messages, readErr := p.store.Graph.ListMessages(ctx, chatID, delegationInboxDrainTop)
		if readErr == nil && containsMachineDelegationRecordID(messages, record.RecordID) {
			return p.updateDelegationOutbox(record, delegation.OutboxVisible, chatID, "", "")
		}
		_ = p.updateDelegationOutbox(record, delegation.OutboxFailed, chatID, "", sendErr.Error())
		if readErr != nil {
			return fmt.Errorf("send delegation record %s: %w; visibility check failed: %v", record.RecordID, sendErr, readErr)
		}
		return sendErr
	}
	messages, err := p.store.Graph.ListMessages(ctx, chatID, delegationInboxDrainTop)
	if err != nil {
		_ = p.updateDelegationOutbox(record, delegation.OutboxSent, chatID, msg.ID, "")
		return err
	}
	if !containsMachineDelegationRecordID(messages, record.RecordID) {
		_ = p.updateDelegationOutbox(record, delegation.OutboxFailed, chatID, msg.ID, "sent record was not visible in inbox reread")
		return fmt.Errorf("delegation record %s sent as message %s but was not visible in inbox reread", record.RecordID, msg.ID)
	}
	return p.updateDelegationOutbox(record, delegation.OutboxVisible, chatID, msg.ID, "")
}

func (p *bridgeMachineRegistryPublisher) tryStartDelegationExecution(request delegation.Record, claim delegation.Record) (bool, error) {
	path := strings.TrimSpace(p.delegationStatePath)
	if path == "" {
		return true, nil
	}
	if delegation.StorePathUsesSQLite(path) {
		return delegation.TryStartWorkerSQLite(path, request, claim, p.now().UTC(), delegationWorkerSQLitePruneLimits())
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return false, err
	}
	p.pruneDelegationWorkerState(&store)
	if existing, ok := store.ExecutionForID(request.DelegationID); ok {
		if existing.ClaimID == claim.ClaimID && existing.ClaimEpoch == claim.ClaimEpoch && existing.WorkerInstanceID == claim.WorkerInstanceID {
			return false, nil
		}
		if existing.Status == delegation.StateRunning || existing.Status == delegation.StateComplete || existing.Status == delegation.StateBlocked || existing.Status == delegation.StateCanceled || existing.Status == delegation.StateReuseRejected {
			return false, nil
		}
	}
	if threadID := strings.TrimSpace(request.RemoteThreadID); threadID != "" {
		if thread, ok := store.RemoteThreadForID(threadID); ok {
			activeID := strings.TrimSpace(thread.ActiveDelegationID)
			if thread.State == delegation.RemoteThreadStateActive && activeID != "" && activeID != strings.TrimSpace(request.DelegationID) {
				return false, nil
			}
		}
	}
	now := p.now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	store.UpsertExecution(delegation.ExecutionFence{
		DelegationID:     request.DelegationID,
		ClaimID:          claim.ClaimID,
		ClaimEpoch:       claim.ClaimEpoch,
		WorkerInstanceID: claim.WorkerInstanceID,
		MachineID:        claim.MachineID,
		Status:           delegation.StateRunning,
		StartedAt:        nowText,
		UpdatedAt:        nowText,
	})
	p.upsertDelegationRemoteThread(&store, request, delegation.RemoteThreadStateActive, request.DelegationID, now)
	store.Prune(now, delegation.DefaultStoreRetention)
	p.pruneDelegationWorkerState(&store)
	_, err = delegation.SaveStore(path, store)
	return err == nil, err
}

func (p *bridgeMachineRegistryPublisher) finishDelegationExecution(request delegation.Record, claim delegation.Record, status string) error {
	path := strings.TrimSpace(p.delegationStatePath)
	if path == "" {
		return nil
	}
	if delegation.StorePathUsesSQLite(path) {
		return delegation.FinishWorkerSQLite(path, request, claim, status, p.now().UTC(), delegationWorkerSQLitePruneLimits())
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return err
	}
	now := p.now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	store.UpsertExecution(delegation.ExecutionFence{
		DelegationID:     request.DelegationID,
		ClaimID:          claim.ClaimID,
		ClaimEpoch:       claim.ClaimEpoch,
		WorkerInstanceID: claim.WorkerInstanceID,
		MachineID:        claim.MachineID,
		Status:           status,
		UpdatedAt:        nowText,
	})
	p.clearDelegationWorkerRemoteThread(&store, request)
	store.Prune(now, delegation.DefaultStoreRetention)
	p.pruneDelegationWorkerState(&store)
	_, err = delegation.SaveStore(path, store)
	return err
}

func (p *bridgeMachineRegistryPublisher) upsertDelegationRemoteThread(store *delegation.Store, request delegation.Record, state string, activeDelegationID string, now time.Time) {
	if store == nil || strings.TrimSpace(request.RemoteThreadID) == "" {
		return
	}
	thread, _ := store.RemoteThreadForID(request.RemoteThreadID)
	thread.ThreadID = strings.TrimSpace(request.RemoteThreadID)
	thread.MachineID = strings.TrimSpace(request.MachineID)
	thread.SourceSessionID = strings.TrimSpace(firstNonEmptyString(thread.SourceSessionID, request.SourceSessionID))
	thread.State = strings.TrimSpace(state)
	thread.ActiveDelegationID = strings.TrimSpace(activeDelegationID)
	thread.Generation = strings.TrimSpace(firstNonEmptyString(thread.Generation, request.ThreadGeneration, delegation.NewThreadGeneration(request.RemoteThreadID, now)))
	thread.Title = firstNonEmptyString(thread.Title, request.Spec.Title, request.Spec.Objective)
	if state == delegation.RemoteThreadStateActive {
		thread.LastTerminalRecordID = ""
	}
	thread.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
	thread.LastUsedAt = thread.UpdatedAt
	if strings.TrimSpace(thread.CreatedAt) == "" {
		thread.CreatedAt = request.CreatedAt
	}
	if strings.TrimSpace(thread.ExpiresAt) == "" {
		thread.ExpiresAt = now.Add(delegation.DefaultStoreRetention).UTC().Format(time.RFC3339Nano)
	}
	store.UpsertRemoteThread(thread)
}

func (p *bridgeMachineRegistryPublisher) clearDelegationWorkerRemoteThread(store *delegation.Store, request delegation.Record) {
	if store == nil {
		return
	}
	threadID := strings.TrimSpace(request.RemoteThreadID)
	if threadID == "" {
		return
	}
	// Worker state only needs active remote-thread fences. Source-side route
	// stores keep idle reusable thread summaries; preserving those avoids
	// accidental loss if a test or diagnostic command points both paths at the
	// same file.
	if len(store.Routes) > 0 {
		p.upsertDelegationRemoteThread(store, request, delegation.RemoteThreadStateIdle, "", p.now().UTC())
		return
	}
	delete(store.RemoteThreads, threadID)
}

func (p *bridgeMachineRegistryPublisher) pruneDelegationWorkerState(store *delegation.Store) {
	if store == nil || len(store.Routes) > 0 {
		return
	}
	pruneDelegationWorkerIdleRemoteThreads(store)
	pruneDelegationWorkerExecutions(store, delegationWorkerTerminalExecutionLimit)
	pruneDelegationWorkerOutbox(store, delegationWorkerTerminalOutboxLimit)
}

func pruneDelegationWorkerIdleRemoteThreads(store *delegation.Store) {
	if store == nil || len(store.RemoteThreads) == 0 {
		return
	}
	for id, thread := range store.RemoteThreads {
		if strings.TrimSpace(thread.State) != delegation.RemoteThreadStateActive {
			delete(store.RemoteThreads, id)
		}
	}
}

func pruneDelegationWorkerExecutions(store *delegation.Store, limit int) {
	if store == nil || limit <= 0 || len(store.Executions) <= limit {
		return
	}
	entries := make([]delegationWorkerPruneEntry, 0, len(store.Executions))
	for id, execution := range store.Executions {
		if !delegationWorkerTerminalStatus(execution.Status) {
			continue
		}
		entries = append(entries, delegationWorkerPruneEntry{id: id, at: parseDelegationWorkerTime(execution.UpdatedAt)})
	}
	pruneDelegationWorkerEntries(entries, limit, func(id string) {
		delete(store.Executions, id)
	})
}

func pruneDelegationWorkerOutbox(store *delegation.Store, limit int) {
	if store == nil || limit <= 0 || len(store.Outbox) <= limit {
		return
	}
	entries := make([]delegationWorkerPruneEntry, 0, len(store.Outbox))
	for id, record := range store.Outbox {
		if record.Status != delegation.OutboxVisible && record.Status != delegation.OutboxFailed {
			continue
		}
		entries = append(entries, delegationWorkerPruneEntry{id: id, at: parseDelegationWorkerTime(record.UpdatedAt)})
	}
	pruneDelegationWorkerEntries(entries, limit, func(id string) {
		delete(store.Outbox, id)
	})
}

type delegationWorkerPruneEntry struct {
	id string
	at time.Time
}

func pruneDelegationWorkerEntries(entries []delegationWorkerPruneEntry, limit int, remove func(string)) {
	if len(entries) <= limit || remove == nil {
		return
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].at.After(entries[j].at)
	})
	for _, entry := range entries[limit:] {
		remove(entry.id)
	}
}

func delegationWorkerTerminalStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case delegation.StateComplete, delegation.StateBlocked, delegation.StateCanceled, delegation.StateReuseRejected:
		return true
	default:
		return false
	}
}

func parseDelegationWorkerTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	return t
}

func (p *bridgeMachineRegistryPublisher) updateDelegationOutbox(record delegation.Record, status string, chatID string, messageID string, errText string) error {
	path := strings.TrimSpace(p.delegationStatePath)
	if path == "" {
		return nil
	}
	if delegation.StorePathUsesSQLite(path) {
		return delegation.UpsertWorkerOutboxSQLite(path, record, status, chatID, messageID, errText, p.now().UTC(), delegationWorkerSQLitePruneLimits())
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return err
	}
	now := p.now().UTC().Format(time.RFC3339Nano)
	existing, _ := store.OutboxForRecordID(record.RecordID)
	attempts := existing.Attempts
	if status == delegation.OutboxPending {
		attempts++
	}
	createdAt := existing.CreatedAt
	if strings.TrimSpace(createdAt) == "" {
		createdAt = now
	}
	if strings.TrimSpace(messageID) == "" {
		messageID = existing.MessageID
	}
	store.UpsertOutbox(delegation.OutboxRecord{
		RecordID:     record.RecordID,
		DelegationID: record.DelegationID,
		ChatID:       strings.TrimSpace(chatID),
		InboxRef:     strings.TrimSpace(record.InboxRef),
		Status:       strings.TrimSpace(status),
		MessageID:    strings.TrimSpace(messageID),
		Attempts:     attempts,
		Error:        strings.TrimSpace(errText),
		CreatedAt:    createdAt,
		UpdatedAt:    now,
	})
	store.Prune(p.now().UTC(), delegation.DefaultStoreRetention)
	_, err = delegation.SaveStore(path, store)
	return err
}

func (p *bridgeMachineRegistryPublisher) delegationInboxCursor(chatID string) delegation.InboxCursor {
	path := strings.TrimSpace(p.delegationStatePath)
	if path == "" {
		return delegation.InboxCursor{ChatID: strings.TrimSpace(chatID)}
	}
	if delegation.StorePathUsesSQLite(path) {
		cursor, ok, err := delegation.InboxCursorSQLite(path, chatID)
		if err == nil && ok {
			return cursor
		}
		return delegation.InboxCursor{ChatID: strings.TrimSpace(chatID)}
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return delegation.InboxCursor{ChatID: strings.TrimSpace(chatID)}
	}
	cursor, ok := store.InboxCursorForChat(chatID)
	if !ok {
		cursor.ChatID = strings.TrimSpace(chatID)
	}
	return cursor
}

func (p *bridgeMachineRegistryPublisher) updateDelegationInboxCursor(chatID string, headID string) {
	chatID = strings.TrimSpace(chatID)
	headID = strings.TrimSpace(headID)
	if chatID == "" || headID == "" {
		return
	}
	path := strings.TrimSpace(p.delegationStatePath)
	if path == "" {
		return
	}
	if delegation.StorePathUsesSQLite(path) {
		_ = delegation.UpsertInboxCursorSQLite(path, delegation.InboxCursor{
			ChatID:            chatID,
			LastHeadMessageID: headID,
			UpdatedAt:         p.now().UTC().Format(time.RFC3339Nano),
		})
		return
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return
	}
	store.UpsertInboxCursor(delegation.InboxCursor{
		ChatID:            chatID,
		LastHeadMessageID: headID,
		UpdatedAt:         p.now().UTC().Format(time.RFC3339Nano),
	})
	store.Prune(p.now().UTC(), delegation.DefaultStoreRetention)
	_, _ = delegation.SaveStore(path, store)
}

func (p *bridgeMachineRegistryPublisher) delegationInboxBackoffDelay() time.Duration {
	chatID := strings.TrimSpace(p.cache.InboxChatID)
	if chatID == "" || strings.TrimSpace(p.delegationStatePath) == "" {
		return 0
	}
	if delegation.StorePathUsesSQLite(p.delegationStatePath) {
		backoff, ok, err := delegation.InboxBackoffSQLite(p.delegationStatePath, chatID)
		if err != nil || !ok || strings.TrimSpace(backoff.BlockedUntil) == "" {
			return 0
		}
		blockedUntil, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(backoff.BlockedUntil))
		if err != nil {
			return 0
		}
		now := p.now().UTC()
		if now.Before(blockedUntil) {
			return blockedUntil.Sub(now)
		}
		return 0
	}
	store, err := delegation.LoadStore(p.delegationStatePath)
	if err != nil {
		return 0
	}
	backoff, ok := store.InboxBackoffForChat(chatID)
	if !ok || strings.TrimSpace(backoff.BlockedUntil) == "" {
		return 0
	}
	blockedUntil, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(backoff.BlockedUntil))
	if err != nil {
		return 0
	}
	now := p.now().UTC()
	if now.Before(blockedUntil) {
		return blockedUntil.Sub(now)
	}
	return 0
}

func (p *bridgeMachineRegistryPublisher) recordDelegationInboxBackoff(delay time.Duration, reason string) error {
	if delay <= 0 {
		return nil
	}
	chatID := strings.TrimSpace(p.cache.InboxChatID)
	path := strings.TrimSpace(p.delegationStatePath)
	if chatID == "" || path == "" {
		return nil
	}
	if delegation.StorePathUsesSQLite(path) {
		now := p.now().UTC()
		return delegation.UpsertInboxBackoffSQLite(path, delegation.InboxBackoff{
			ChatID:       chatID,
			BlockedUntil: now.Add(delay).Format(time.RFC3339Nano),
			Reason:       strings.TrimSpace(reason),
			UpdatedAt:    now.Format(time.RFC3339Nano),
		})
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return err
	}
	now := p.now().UTC()
	store.UpsertInboxBackoff(delegation.InboxBackoff{
		ChatID:       chatID,
		BlockedUntil: now.Add(delay).Format(time.RFC3339Nano),
		Reason:       strings.TrimSpace(reason),
		UpdatedAt:    now.Format(time.RFC3339Nano),
	})
	store.Prune(now, delegation.DefaultStoreRetention)
	_, err = delegation.SaveStore(path, store)
	return err
}

func delegationWorkerSQLitePruneLimits() delegation.WorkerStorePruneLimits {
	return delegation.WorkerStorePruneLimits{
		TerminalExecutionLimit: delegationWorkerTerminalExecutionLimit,
		TerminalOutboxLimit:    delegationWorkerTerminalOutboxLimit,
	}
}

func delegationInboxRetryAfter(err error) time.Duration {
	var graphErr *machineregistry.StatusError
	if errors.As(err, &graphErr) && graphErr.RetryAfter > 0 {
		return graphErr.RetryAfter
	}
	return 0
}

func firstMachineRegistryMessageID(messages []machineregistry.ChatMessage) string {
	if len(messages) == 0 {
		return ""
	}
	return strings.TrimSpace(messages[0].ID)
}

func containsMachineDelegationRecordID(messages []machineregistry.ChatMessage, recordID string) bool {
	recordID = strings.TrimSpace(recordID)
	if recordID == "" {
		return false
	}
	for _, record := range delegation.ObserveRecords(machineDelegationMessages(messages)) {
		if strings.TrimSpace(record.RecordID) == recordID {
			return true
		}
	}
	return false
}

func delegationPrompt(request delegation.Record) string {
	var b strings.Builder
	b.WriteString("You are Agent B completing a CXP cross-machine delegated task.\n\n")
	if strings.TrimSpace(request.RemoteThreadID) != "" {
		b.WriteString("Remote thread: " + strings.TrimSpace(request.RemoteThreadID) + "\n")
		b.WriteString("Thread policy: " + firstNonEmptyString(request.ThreadPolicy, delegation.ThreadPolicyNew) + "\n")
		if request.ThreadPolicy == delegation.ThreadPolicyReuse {
			b.WriteString("If this reused thread has the wrong context for the task, reply with `" + delegationReuseRejectedPrefix + " <short reason>` and stop. Otherwise continue using the existing context.\n")
		}
		b.WriteString("\n")
	}
	if title := strings.TrimSpace(request.Spec.Title); title != "" {
		b.WriteString("Title: " + title + "\n\n")
	}
	b.WriteString("Objective:\n")
	b.WriteString(strings.TrimSpace(request.Spec.Objective))
	b.WriteString("\n")
	if len(request.Spec.Context) > 0 {
		b.WriteString("\nContext:\n")
		for _, item := range request.Spec.Context {
			if strings.TrimSpace(item) != "" {
				b.WriteString("- " + strings.TrimSpace(item) + "\n")
			}
		}
	}
	if len(request.Spec.Constraints) > 0 {
		b.WriteString("\nConstraints:\n")
		for _, item := range request.Spec.Constraints {
			if strings.TrimSpace(item) != "" {
				b.WriteString("- " + strings.TrimSpace(item) + "\n")
			}
		}
	}
	if len(request.Spec.AllowedActions) > 0 {
		b.WriteString("\nAllowed actions:\n")
		for _, item := range request.Spec.AllowedActions {
			if strings.TrimSpace(item) != "" {
				b.WriteString("- " + strings.TrimSpace(item) + "\n")
			}
		}
	}
	b.WriteString("\nReturn a concise final result for Agent A. Do not delegate this same task back to Agent A.\n")
	return b.String()
}

func delegationExecutionSessionID(request delegation.Record) string {
	return firstNonEmptyString(request.RemoteThreadID, request.DelegationID)
}

func parseDelegationReuseRejected(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, delegationReuseRejectedPrefix) {
		return "", false
	}
	reason := strings.TrimSpace(strings.TrimPrefix(text, delegationReuseRejectedPrefix))
	if reason == "" {
		reason = "remote worker rejected reuse for this task"
	}
	return reason, true
}

func delegationThreadUpdate(request delegation.Record, status string, body string) *delegation.ThreadUpdate {
	if strings.TrimSpace(request.RemoteThreadID) == "" {
		return nil
	}
	body = strings.TrimSpace(body)
	if status == delegation.StateReuseRejected {
		return &delegation.ThreadUpdate{LastResultSummary: body}
	}
	if status != delegation.StateComplete && status != delegation.StateBlocked {
		return nil
	}
	update := &delegation.ThreadUpdate{
		Title:             strings.TrimSpace(request.Spec.Title),
		SummaryDelta:      body,
		LastResultSummary: body,
	}
	if update.Title == "" {
		update.Title = strings.TrimSpace(request.Spec.Objective)
	}
	return update
}

func reduceOpenDelegations(records []delegation.Record, now time.Time) []delegation.State {
	byID := map[string][]delegation.Record{}
	for _, record := range records {
		id := strings.TrimSpace(record.DelegationID)
		if id == "" {
			continue
		}
		byID[id] = append(byID[id], record)
	}
	out := make([]delegation.State, 0, len(byID))
	for _, group := range byID {
		out = append(out, delegation.Reduce(group, now))
	}
	return out
}

func (p *bridgeMachineRegistryPublisher) workerInstanceID() string {
	if id := strings.TrimSpace(p.workerInstance); id != "" {
		return id
	}
	return "worker_" + strings.TrimSpace(p.machineID) + "_" + fmt.Sprint(p.now().UTC().UnixNano())
}

func (p *bridgeMachineRegistryPublisher) tryMarkDelegationActive(request delegation.Record) bool {
	id := strings.TrimSpace(request.DelegationID)
	if id == "" {
		return false
	}
	threadID := strings.TrimSpace(request.RemoteThreadID)
	p.workerMu.Lock()
	defer p.workerMu.Unlock()
	if p.activeDelegations == nil {
		p.activeDelegations = map[string]bool{}
	}
	if p.activeRemoteThreads == nil {
		p.activeRemoteThreads = map[string]string{}
	}
	if p.activeDelegations[id] {
		return false
	}
	if threadID != "" {
		if activeID := strings.TrimSpace(p.activeRemoteThreads[threadID]); activeID != "" && activeID != id {
			return false
		}
	}
	p.activeDelegations[id] = true
	if threadID != "" {
		p.activeRemoteThreads[threadID] = id
	}
	return true
}

func (p *bridgeMachineRegistryPublisher) setDelegationActiveCancel(id string, cancel context.CancelFunc) {
	id = strings.TrimSpace(id)
	if id == "" || cancel == nil {
		return
	}
	p.workerMu.Lock()
	defer p.workerMu.Unlock()
	if p.activeCancels == nil {
		p.activeCancels = map[string]context.CancelFunc{}
	}
	p.activeCancels[id] = cancel
}

func (p *bridgeMachineRegistryPublisher) cancelDelegationActive(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	p.workerMu.Lock()
	cancel := p.activeCancels[id]
	p.workerMu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (p *bridgeMachineRegistryPublisher) clearDelegationActive(id string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	p.workerMu.Lock()
	defer p.workerMu.Unlock()
	delete(p.activeDelegations, id)
	for threadID, delegationID := range p.activeRemoteThreads {
		if delegationID == id {
			delete(p.activeRemoteThreads, threadID)
		}
	}
	delete(p.activeCancels, id)
}

func machineDelegationMessages(messages []machineregistry.ChatMessage) []delegation.ChatMessage {
	out := make([]delegation.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, delegation.ChatMessage{
			ID: msg.ID,
			Body: delegation.ChatMessageBody{
				Content: msg.Body.Content,
			},
		})
	}
	return out
}
