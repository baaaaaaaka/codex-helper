package teams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	maxOutboxEchoAttempts        = 1024
	outboxEchoAttemptTTL         = 5 * time.Minute
	outboxEchoActivitySkew       = 30 * time.Second
	helperEchoBreakerWindowSize  = 60 * time.Second
	helperEchoBreakerThreshold   = 3
	maxHelperEchoBreakerSessions = 1024
)

type outboxEchoAttempt struct {
	OutboxID       string
	ChatID         string
	Fingerprint    string
	AttemptedAt    time.Time
	ExpiresAt      time.Time
	TeamsMessageID string
	InFlight       bool
	Generation     uint64
}

type outboxEchoAttemptSlot struct {
	OutboxID   string
	Generation uint64
}

type outboxEchoAttemptCache struct {
	NextGeneration uint64
	Ring           []outboxEchoAttemptSlot
	Entries        map[string]outboxEchoAttempt
	ByKey          map[string]map[string]uint64
}

type helperEchoBreakerObservation struct {
	MessageID string
	At        time.Time
}

type helperEchoBreakerWindow struct {
	Observations []helperEchoBreakerObservation
	Tripped      bool
}

func outboxEchoFingerprintFromHTML(renderedHTML string) string {
	return normalizedTextHash(comparableTeamsPlainText(PlainTextFromTeamsHTML(renderedHTML)))
}

func outboxEchoFingerprintFromPlainText(text string) string {
	return normalizedTextHash(comparableTeamsPlainText(text))
}

func outboxEchoAttemptKey(chatID string, fingerprint string) string {
	return strings.TrimSpace(chatID) + "\x00" + strings.TrimSpace(fingerprint)
}

func (b *Bridge) rememberOutboxEchoAttempt(outbox teamstore.OutboxMessage, renderedHTML string) {
	if b == nil {
		return
	}
	outboxID := strings.TrimSpace(outbox.ID)
	chatID := strings.TrimSpace(outbox.TeamsChatID)
	fingerprint := outboxEchoFingerprintFromHTML(renderedHTML)
	if outboxID == "" || chatID == "" || fingerprint == "" {
		return
	}
	now := time.Now()
	attemptedAt := outbox.LastSendAttempt
	if attemptedAt.IsZero() {
		attemptedAt = now
	}
	b.outboxEchoAttemptMu.Lock()
	defer b.outboxEchoAttemptMu.Unlock()
	cache := &b.outboxEchoAttempts
	if cache.Entries == nil {
		cache.Entries = make(map[string]outboxEchoAttempt)
		cache.ByKey = make(map[string]map[string]uint64)
		cache.Ring = make([]outboxEchoAttemptSlot, maxOutboxEchoAttempts)
	}
	cache.remove(outboxID, 0)
	cache.NextGeneration++
	generation := cache.NextGeneration
	slotIndex := int((generation - 1) % uint64(len(cache.Ring)))
	old := cache.Ring[slotIndex]
	if old.OutboxID != "" {
		cache.remove(old.OutboxID, old.Generation)
	}
	entry := outboxEchoAttempt{
		OutboxID:    outboxID,
		ChatID:      chatID,
		Fingerprint: fingerprint,
		AttemptedAt: attemptedAt,
		ExpiresAt:   now.Add(outboxEchoAttemptTTL),
		InFlight:    true,
		Generation:  generation,
	}
	cache.Entries[outboxID] = entry
	key := outboxEchoAttemptKey(chatID, fingerprint)
	if cache.ByKey[key] == nil {
		cache.ByKey[key] = make(map[string]uint64)
	}
	cache.ByKey[key][outboxID] = generation
	cache.Ring[slotIndex] = outboxEchoAttemptSlot{OutboxID: outboxID, Generation: generation}
}

func (cache *outboxEchoAttemptCache) remove(outboxID string, generation uint64) {
	entry, ok := cache.Entries[outboxID]
	if !ok || generation != 0 && entry.Generation != generation {
		return
	}
	delete(cache.Entries, outboxID)
	key := outboxEchoAttemptKey(entry.ChatID, entry.Fingerprint)
	delete(cache.ByKey[key], outboxID)
	if len(cache.ByKey[key]) == 0 {
		delete(cache.ByKey, key)
	}
}

func (b *Bridge) forgetOutboxEchoAttempt(outboxID string) {
	if b == nil || strings.TrimSpace(outboxID) == "" {
		return
	}
	b.outboxEchoAttemptMu.Lock()
	b.outboxEchoAttempts.remove(strings.TrimSpace(outboxID), 0)
	b.outboxEchoAttemptMu.Unlock()
}

func (b *Bridge) retainAmbiguousOutboxEchoAttempt(outboxID string) {
	if b == nil || strings.TrimSpace(outboxID) == "" {
		return
	}
	b.outboxEchoAttemptMu.Lock()
	defer b.outboxEchoAttemptMu.Unlock()
	entry, ok := b.outboxEchoAttempts.Entries[strings.TrimSpace(outboxID)]
	if !ok {
		return
	}
	entry.ExpiresAt = time.Now().Add(outboxEchoAttemptTTL)
	entry.InFlight = false
	b.outboxEchoAttempts.Entries[entry.OutboxID] = entry
}

// claimKnownAmbiguousOutboxRetry authorizes a same-process explicit retry only
// for the attempt that this Bridge previously sent and then released as
// ambiguous. A restart has no such cache entry, and a concurrent sender marks
// the entry InFlight, so neither case may steal an active send lease and risk a
// duplicate POST.
func (b *Bridge) claimKnownAmbiguousOutboxRetry(outboxID string) (uint64, bool) {
	if b == nil || strings.TrimSpace(outboxID) == "" {
		return 0, false
	}
	now := time.Now()
	b.outboxEchoAttemptMu.Lock()
	defer b.outboxEchoAttemptMu.Unlock()
	entry, ok := b.outboxEchoAttempts.Entries[strings.TrimSpace(outboxID)]
	if !ok || entry.InFlight || !entry.ExpiresAt.After(now) {
		return 0, false
	}
	entry.InFlight = true
	b.outboxEchoAttempts.Entries[entry.OutboxID] = entry
	return entry.Generation, true
}

func (b *Bridge) releaseKnownAmbiguousOutboxRetry(outboxID string, generation uint64) {
	if b == nil || strings.TrimSpace(outboxID) == "" || generation == 0 {
		return
	}
	b.outboxEchoAttemptMu.Lock()
	defer b.outboxEchoAttemptMu.Unlock()
	entry, ok := b.outboxEchoAttempts.Entries[strings.TrimSpace(outboxID)]
	if !ok || entry.Generation != generation {
		return
	}
	entry.InFlight = false
	entry.ExpiresAt = time.Now().Add(outboxEchoAttemptTTL)
	b.outboxEchoAttempts.Entries[entry.OutboxID] = entry
}

func (b *Bridge) matchOutboxEchoAttempt(chatID string, plainText string, messageID string, activityAt time.Time) (outboxEchoAttempt, bool) {
	if b == nil {
		return outboxEchoAttempt{}, false
	}
	fingerprint := outboxEchoFingerprintFromPlainText(plainText)
	key := outboxEchoAttemptKey(chatID, fingerprint)
	if strings.TrimSpace(chatID) == "" || fingerprint == "" {
		return outboxEchoAttempt{}, false
	}
	now := time.Now()
	b.outboxEchoAttemptMu.Lock()
	defer b.outboxEchoAttemptMu.Unlock()
	ids := b.outboxEchoAttempts.ByKey[key]
	var matches []outboxEchoAttempt
	for outboxID, generation := range ids {
		entry, ok := b.outboxEchoAttempts.Entries[outboxID]
		if !ok || entry.Generation != generation {
			delete(ids, outboxID)
			continue
		}
		if !entry.ExpiresAt.After(now) {
			b.outboxEchoAttempts.remove(outboxID, generation)
			continue
		}
		if !activityAt.IsZero() && activityAt.Before(entry.AttemptedAt.Add(-outboxEchoActivitySkew)) {
			continue
		}
		matches = append(matches, entry)
	}
	if len(matches) != 1 {
		return outboxEchoAttempt{}, false
	}
	entry := matches[0]
	entry.TeamsMessageID = strings.TrimSpace(messageID)
	b.outboxEchoAttempts.Entries[entry.OutboxID] = entry
	return entry, true
}

func definitiveGraphSendFailure(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *GraphStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	return statusErr.StatusCode >= http.StatusBadRequest && statusErr.StatusCode < http.StatusInternalServerError
}

func (b *Bridge) shouldSuppressUnprovenancedHelperEcho(ctx context.Context, chatID string, msg ChatMessage, role inboundPollRole, legacy bool) (bool, error) {
	if legacy || role == inboundPollRoleControl {
		return true, nil
	}
	if role != inboundPollRoleWork {
		return false, nil
	}
	return b.noteUnprovenancedHelperEcho(ctx, chatID, msg)
}

func (b *Bridge) noteUnprovenancedHelperEcho(ctx context.Context, chatID string, msg ChatMessage) (bool, error) {
	if b == nil || b.store == nil || strings.TrimSpace(msg.ID) == "" {
		return false, nil
	}
	session := b.reg.SessionByChatID(strings.TrimSpace(chatID))
	if session == nil || strings.TrimSpace(session.ID) == "" {
		return false, nil
	}
	durable, err := b.store.SessionsByID(ctx, []string{session.ID})
	if err != nil {
		return false, err
	}
	durableSession, ok := durable[session.ID]
	if !ok || !isActiveSessionStatus(string(durableSession.Status)) || strings.TrimSpace(durableSession.TeamsChatID) != strings.TrimSpace(chatID) {
		return false, nil
	}
	now := chatMessageActivityTime(msg)
	if now.IsZero() {
		now = time.Now()
	}
	key := strings.TrimSpace(session.ID)
	b.helperEchoBreakerMu.Lock()
	if b.helperEchoBreakers == nil {
		b.helperEchoBreakers = make(map[string]helperEchoBreakerWindow)
	}
	if _, exists := b.helperEchoBreakers[key]; !exists && len(b.helperEchoBreakers) >= maxHelperEchoBreakerSessions {
		oldestKey := ""
		var oldestAt time.Time
		for candidateKey, candidate := range b.helperEchoBreakers {
			candidateAt := time.Time{}
			if n := len(candidate.Observations); n > 0 {
				candidateAt = candidate.Observations[n-1].At
			}
			if oldestKey == "" || candidateAt.Before(oldestAt) {
				oldestKey = candidateKey
				oldestAt = candidateAt
			}
		}
		delete(b.helperEchoBreakers, oldestKey)
	}
	window := b.helperEchoBreakers[key]
	cutoff := now.Add(-helperEchoBreakerWindowSize)
	kept := window.Observations[:0]
	duplicate := false
	for _, observation := range window.Observations {
		if observation.At.Before(cutoff) {
			continue
		}
		if observation.MessageID == strings.TrimSpace(msg.ID) {
			duplicate = true
		}
		kept = append(kept, observation)
	}
	window.Observations = kept
	if !duplicate {
		window.Observations = append(window.Observations, helperEchoBreakerObservation{MessageID: strings.TrimSpace(msg.ID), At: now})
	}
	trip := !window.Tripped && len(window.Observations) >= helperEchoBreakerThreshold
	if trip {
		window.Tripped = true
	}
	b.helperEchoBreakers[key] = window
	b.helperEchoBreakerMu.Unlock()
	if !trip {
		return true, nil
	}

	b.setSessionQuarantineFence(key, true)
	triggerIDs := make([]string, 0, len(window.Observations))
	for _, observation := range window.Observations {
		triggerIDs = append(triggerIDs, observation.MessageID)
	}
	report, err := b.store.QuarantineSession(ctx, teamstore.SessionQuarantineRequest{
		SessionID:         key,
		Reason:            "automatic containment after repeated unprovenanced helper self-echo",
		Source:            "teams_self_echo_breaker",
		TriggerMessageIDs: triggerIDs,
		InFlightOutboxIDs: b.inFlightOutboxIDsForChat(chatID),
		Now:               time.Now(),
	})
	if err != nil {
		b.setSessionQuarantineFence(key, false)
		b.helperEchoBreakerMu.Lock()
		if current, ok := b.helperEchoBreakers[key]; ok {
			current.Tripped = false
			b.helperEchoBreakers[key] = current
		}
		b.helperEchoBreakerMu.Unlock()
		return false, err
	}
	b.cancelRunningTurnsForQuarantine(key, report.Session.QuarantineReason)
	b.syncRegistrySessionProjection(registrySessionFromDurable(report.Session))
	b.queueQuarantineControlNotice(ctx, report)
	return true, nil
}

func (b *Bridge) inFlightOutboxIDsForChat(chatID string) []string {
	if b == nil {
		return nil
	}
	chatID = strings.TrimSpace(chatID)
	now := time.Now()
	b.outboxEchoAttemptMu.Lock()
	defer b.outboxEchoAttemptMu.Unlock()
	var out []string
	for id, entry := range b.outboxEchoAttempts.Entries {
		if strings.TrimSpace(entry.ChatID) == chatID && entry.InFlight && entry.ExpiresAt.After(now) {
			out = append(out, id)
		}
	}
	return out
}

func (b *Bridge) setSessionQuarantineFence(sessionID string, quarantined bool) {
	if b == nil || strings.TrimSpace(sessionID) == "" {
		return
	}
	b.quarantinedSessionMu.Lock()
	if b.quarantinedSessions == nil {
		b.quarantinedSessions = make(map[string]bool)
	}
	if quarantined {
		b.quarantinedSessions[strings.TrimSpace(sessionID)] = true
	} else {
		delete(b.quarantinedSessions, strings.TrimSpace(sessionID))
	}
	b.quarantinedSessionMu.Unlock()
}

func (b *Bridge) sessionQuarantineFenced(sessionID string) bool {
	if b == nil || strings.TrimSpace(sessionID) == "" {
		return false
	}
	b.quarantinedSessionMu.RLock()
	defer b.quarantinedSessionMu.RUnlock()
	return b.quarantinedSessions[strings.TrimSpace(sessionID)]
}

func (b *Bridge) cancelRunningTurnsForQuarantine(sessionID string, reason string) {
	if b == nil {
		return
	}
	type cancelTarget struct {
		cancel context.CancelFunc
	}
	var targets []cancelTarget
	b.runningTurnMu.Lock()
	for _, running := range b.runningTurnCancels {
		if running == nil || strings.TrimSpace(running.sessionID) != strings.TrimSpace(sessionID) {
			continue
		}
		running.requested = true
		running.reason = firstNonEmptyString(strings.TrimSpace(reason), "session quarantined")
		running.silent = true
		if running.cancel != nil {
			targets = append(targets, cancelTarget{cancel: running.cancel})
		}
	}
	b.runningTurnMu.Unlock()
	for _, target := range targets {
		target.cancel()
	}
}

func (b *Bridge) queueQuarantineControlNotice(ctx context.Context, report teamstore.SessionQuarantineReport) {
	if b == nil || !report.Changed || strings.TrimSpace(b.reg.ControlChatID) == "" {
		return
	}
	body := fmt.Sprintf("⚠️ Teams Work chat quarantined automatically.\n\nSession: `%s`\nChat: `%s`\nReason: %s\nInterrupted turns: %d\nSkipped pending messages: %d\n\nInspect the session locally, then use `cxp teams chat unquarantine %s --yes` when it is safe.", report.Session.ID, report.Session.TeamsChatID, report.Session.QuarantineReason, len(report.InterruptedTurnIDs), len(report.SkippedOutboxIDs), report.Session.ID)
	id := "outbox:quarantine:" + normalizedTextHash(report.Session.ID+"\x00"+report.Session.QuarantinedAt.UTC().Format(time.RFC3339Nano))
	queued, err := b.queueOutbox(ctx, teamstore.OutboxMessage{
		ID:          id,
		TeamsChatID: b.reg.ControlChatID,
		Kind:        "helper-quarantine",
		Body:        body,
	})
	if err == nil && queued.Status != teamstore.OutboxStatusSent {
		err = b.flushPendingOutboxForChat(ctx, queued.TeamsChatID)
	}
	if err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams quarantine control notice error: %v\n", err)
	}
}
