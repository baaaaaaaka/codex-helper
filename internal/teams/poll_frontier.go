package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	pollFrontierHead                  = "head"
	pollFrontierContinuation          = "continuation"
	pollFrontierGap                   = "gap-recovery"
	continuationFailureBudget         = 3
	continuationFailureMaxAge         = 10 * time.Minute
	continuationHistoryLimit          = 8
	maxOversizedRecordRefetchAttempts = 3
	maxPendingPageBytes               = 16 << 20
	maxPendingRecordBytes             = 12 << 20
)

var (
	errPendingPageInvalid     = errors.New("pending Graph page is invalid")
	errPendingPageIdentity    = errors.New("pending Graph page identity is ambiguous")
	errPendingPageTooLarge    = errors.New("pending Graph page exceeds bounded storage")
	errContinuationNoProgress = errors.New("Graph continuation made no progress")
)

type pollFailureScope uint8

const (
	pollFailureChat pollFailureScope = iota
	pollFailureControl
	pollFailureStore
	pollFailureLease
)

// pollScopedError keeps chat-level Graph/handler failures separate from
// failures that mean this process cannot safely persist or own any poll. The
// wrapper preserves errors.As/errors.Is for existing callers and diagnostics.
type pollScopedError struct {
	scope pollFailureScope
	err   error
}

func (e *pollScopedError) Error() string {
	if e == nil || e.err == nil {
		return "Teams poll failed"
	}
	return e.err.Error()
}

func (e *pollScopedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func pollStoreFailure(err error) error {
	if err == nil {
		return nil
	}
	return &pollScopedError{scope: pollFailureStore, err: err}
}

func pollControlFailure(err error) error {
	if err == nil {
		return nil
	}
	return &pollScopedError{scope: pollFailureControl, err: err}
}

func pollLeaseFailure(err error) error {
	if err == nil {
		return nil
	}
	return &pollScopedError{scope: pollFailureLease, err: err}
}

func isProcessWidePollFailure(err error) bool {
	var scoped *pollScopedError
	return errors.As(err, &scoped) && scoped != nil && scoped.scope != pollFailureChat
}

func (b *Bridge) pollProcessIncarnation() string {
	if b == nil {
		return ""
	}
	b.pollMu.Lock()
	defer b.pollMu.Unlock()
	if strings.TrimSpace(b.pollProcessInstanceID) == "" {
		// This value is deliberately process-local and changes on every Bridge
		// construction, even when a PID is reused after a restart.
		b.pollProcessInstanceID = fmt.Sprintf("%s:%d:%d", strings.TrimSpace(b.machine.ID), os.Getpid(), time.Now().UnixNano())
	}
	return b.pollProcessInstanceID
}

func pollFrontierIdentity(kind, path string) string {
	return strings.TrimSpace(kind) + ":" + strings.TrimSpace(path)
}

func pollPathFingerprint(path string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(path)))
	return hex.EncodeToString(sum[:])
}

// pollRequestPathBelongsToChat validates only the stable collection path. The
// query string remains opaque and is intentionally not parsed or rebuilt: a
// persisted nextLink must be replayed byte-for-byte. This check prevents a
// corrupt or cross-chat continuation from being sent to Graph while keeping
// the normal continuation hot path to two string operations.
func pollRequestPathBelongsToChat(chatID, requestPath string) bool {
	chatID = strings.TrimSpace(chatID)
	requestPath = strings.TrimSpace(requestPath)
	if chatID == "" || requestPath == "" {
		return false
	}
	want := pathWithoutQuery(chatMessagesPath(chatID, 1, time.Time{}))
	return pathWithoutQuery(requestPath) == want
}

func pendingPageFromWindow(chatID, requestPath, frontier string, epoch uint64, window MessageWindow, baselineOnly bool) (*teamstore.ChatPollPendingPage, error) {
	chatID = strings.TrimSpace(chatID)
	requestPath = strings.TrimSpace(requestPath)
	if chatID == "" || requestPath == "" {
		return nil, fmt.Errorf("%w: chat and request path are required", errPendingPageInvalid)
	}
	page := &teamstore.ChatPollPendingPage{
		ChatID:             chatID,
		RequestPath:        requestPath,
		RequestFingerprint: pollPathFingerprint(requestPath),
		Frontier:           strings.TrimSpace(frontier),
		FrontierEpoch:      epoch,
		BaselineOnly:       baselineOnly,
		NextPath:           strings.TrimSpace(window.NextPath),
		ReceivedAt:         time.Now().UTC(),
	}
	seen := make(map[string]string, len(window.Messages))
	var total int64
	for _, msg := range window.Messages {
		id := strings.TrimSpace(msg.ID)
		if id == "" {
			return nil, fmt.Errorf("%w: Graph message has no stable id", errPendingPageIdentity)
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal %s: %v", errPendingPageInvalid, id, err)
		}
		if len(raw) > maxPendingRecordBytes {
			return nil, fmt.Errorf("%w: record %s is %d bytes", errPendingPageTooLarge, id, len(raw))
		}
		hash := sha256.Sum256(raw)
		hashText := hex.EncodeToString(hash[:])
		if previous, ok := seen[id]; ok && previous != hashText {
			return nil, fmt.Errorf("%w: message %s changed payload within one page", errPendingPageIdentity, id)
		}
		seen[id] = hashText
		page.Records = append(page.Records, json.RawMessage(raw))
		page.RecordIDs = append(page.RecordIDs, id)
		page.RecordHashes = append(page.RecordHashes, hashText)
		disposition := "received"
		if msg.oversizedForPoll {
			disposition = "oversized_record"
		}
		page.Dispositions = append(page.Dispositions, disposition)
		total += int64(len(raw))
	}
	if total > maxPendingPageBytes {
		return nil, fmt.Errorf("%w: page is %d bytes", errPendingPageTooLarge, total)
	}
	page.ReceiptID = pendingPageReceiptID(page)
	return page, nil
}

func pendingPageReceiptID(page *teamstore.ChatPollPendingPage) string {
	if page == nil {
		return ""
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d\x00%t\x00%s", page.ChatID, page.RequestPath, page.Frontier, page.FrontierEpoch, page.BaselineOnly, page.NextPath)
	for i := range page.RecordIDs {
		if i < len(page.RecordHashes) {
			disposition := "received"
			if i < len(page.Dispositions) && strings.TrimSpace(page.Dispositions[i]) != "" {
				disposition = strings.TrimSpace(page.Dispositions[i])
			}
			_, _ = fmt.Fprintf(h, "\x00%s=%s#%s", page.RecordIDs[i], page.RecordHashes[i], disposition)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// pendingPageContentFingerprint deliberately excludes RequestPath and
// NextPath. Graph continuation links are opaque and may be regenerated while
// the underlying page remains unchanged; the page content is the durable
// evidence that a continuation made progress (or did not).
func pendingPageContentFingerprint(page *teamstore.ChatPollPendingPage) string {
	if page == nil {
		return ""
	}
	h := sha256.New()
	for i, id := range page.RecordIDs {
		hash := ""
		if i < len(page.RecordHashes) {
			hash = page.RecordHashes[i]
		}
		disposition := "received"
		if i < len(page.Dispositions) && strings.TrimSpace(page.Dispositions[i]) != "" {
			disposition = strings.TrimSpace(page.Dispositions[i])
		}
		_, _ = fmt.Fprintf(h, "%d\x00%s=%s#%s\x00", i, id, hash, disposition)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func continuationPageHasNoProgress(poll teamstore.ChatPollState, source, requestPath string, window MessageWindow) bool {
	if (source != pollFrontierContinuation && source != pollFrontierGap) || !window.Truncated {
		return false
	}
	requestPath = strings.TrimSpace(requestPath)
	nextPath := strings.TrimSpace(window.NextPath)
	if requestPath == "" || nextPath == "" {
		return false
	}
	if requestPath == nextPath {
		return true
	}
	for _, previousPath := range poll.ContinuationPathHistory {
		if strings.TrimSpace(previousPath) == requestPath {
			return true
		}
	}
	fingerprint := pendingPageContentFingerprint(poll.PendingPage)
	if fingerprint == "" {
		return false
	}
	for _, previousFingerprint := range poll.ContinuationPageFingerprintHistory {
		if strings.TrimSpace(previousFingerprint) == fingerprint {
			return true
		}
	}
	return false
}

func appendContinuationHistory(poll *teamstore.ChatPollState, requestPath string, page *teamstore.ChatPollPendingPage) {
	if poll == nil {
		return
	}
	requestPath = strings.TrimSpace(requestPath)
	fingerprint := pendingPageContentFingerprint(page)
	if requestPath == "" || fingerprint == "" {
		return
	}
	poll.ContinuationPathHistory = appendBoundedString(poll.ContinuationPathHistory, requestPath, continuationHistoryLimit)
	poll.ContinuationPageFingerprintHistory = appendBoundedString(poll.ContinuationPageFingerprintHistory, fingerprint, continuationHistoryLimit)
}

func appendBoundedString(values []string, value string, limit int) []string {
	value = strings.TrimSpace(value)
	if value == "" || limit <= 0 {
		return values
	}
	values = append(values, value)
	if len(values) > limit {
		values = append([]string(nil), values[len(values)-limit:]...)
	}
	return values
}

func clearContinuationHistory(poll *teamstore.ChatPollState) {
	if poll == nil {
		return
	}
	poll.ContinuationPathHistory = nil
	poll.ContinuationPageFingerprintHistory = nil
}

func legacyPendingPageReceiptID(page *teamstore.ChatPollPendingPage) string {
	if page == nil {
		return ""
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d\x00%s", page.ChatID, page.RequestPath, page.Frontier, page.FrontierEpoch, page.NextPath)
	for i := range page.RecordIDs {
		if i < len(page.RecordHashes) {
			_, _ = fmt.Fprintf(h, "\x00%s=%s", page.RecordIDs[i], page.RecordHashes[i])
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func pendingPageToWindow(page *teamstore.ChatPollPendingPage) (MessageWindow, error) {
	if page == nil || strings.TrimSpace(page.ChatID) == "" || strings.TrimSpace(page.RequestPath) == "" || strings.TrimSpace(page.ReceiptID) == "" {
		return MessageWindow{}, errPendingPageInvalid
	}
	if len(page.Records) != len(page.RecordIDs) || len(page.Records) != len(page.RecordHashes) {
		return MessageWindow{}, fmt.Errorf("%w: record metadata length mismatch", errPendingPageInvalid)
	}
	if len(page.Dispositions) != 0 && len(page.Dispositions) != len(page.Records) {
		return MessageWindow{}, fmt.Errorf("%w: disposition length mismatch", errPendingPageInvalid)
	}
	if len(page.RefetchFailures) != 0 && len(page.RefetchFailures) != len(page.Records) {
		return MessageWindow{}, fmt.Errorf("%w: refetch failure metadata length mismatch", errPendingPageInvalid)
	}
	if fingerprint := strings.TrimSpace(page.RequestFingerprint); fingerprint != "" && fingerprint != pollPathFingerprint(page.RequestPath) {
		return MessageWindow{}, fmt.Errorf("%w: request path fingerprint changed", errPendingPageIdentity)
	}
	if pendingPageReceiptID(page) != page.ReceiptID &&
		(len(page.Dispositions) != 0 || legacyPendingPageReceiptID(page) != page.ReceiptID) {
		return MessageWindow{}, fmt.Errorf("%w: receipt changed", errPendingPageIdentity)
	}
	window := MessageWindow{Truncated: strings.TrimSpace(page.NextPath) != "", NextPath: strings.TrimSpace(page.NextPath), baselineOnly: page.BaselineOnly}
	for i, raw := range page.Records {
		if len(raw) > maxPendingRecordBytes {
			return MessageWindow{}, fmt.Errorf("%w: record %d is too large", errPendingPageTooLarge, i)
		}
		h := sha256.Sum256(raw)
		if hex.EncodeToString(h[:]) != page.RecordHashes[i] {
			return MessageWindow{}, fmt.Errorf("%w: record %d payload changed", errPendingPageIdentity, i)
		}
		var msg ChatMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			return MessageWindow{}, fmt.Errorf("%w: record %d: %v", errPendingPageInvalid, i, err)
		}
		if strings.TrimSpace(msg.ID) == "" || msg.ID != page.RecordIDs[i] {
			return MessageWindow{}, fmt.Errorf("%w: record %d id changed", errPendingPageIdentity, i)
		}
		if len(page.Dispositions) > 0 {
			switch strings.TrimSpace(page.Dispositions[i]) {
			case "received":
			case "oversized_record":
				msg.oversizedForPoll = true
			case "oversized_record_quarantined":
				msg.quarantinedForPoll = true
			default:
				return MessageWindow{}, fmt.Errorf("%w: record %d has unknown disposition %q", errPendingPageInvalid, i, page.Dispositions[i])
			}
		}
		window.Messages = append(window.Messages, msg)
	}
	return window, nil
}

// notePendingPageRefetchFailure records a failure for one already-identified
// oversized record. It never changes a normal record into a skipped record.
// Once the bounded recovery budget is exhausted, only that record is marked
// quarantined; the rest of the immutable page remains replayable.
func notePendingPageRefetchFailure(page *teamstore.ChatPollPendingPage, messageID string) (bool, error) {
	if page == nil || strings.TrimSpace(messageID) == "" {
		return false, fmt.Errorf("%w: oversized record identity is missing", errPendingPageInvalid)
	}
	index := -1
	for i, id := range page.RecordIDs {
		if strings.TrimSpace(id) == strings.TrimSpace(messageID) {
			index = i
			break
		}
	}
	if index < 0 || index >= len(page.Records) {
		return false, fmt.Errorf("%w: oversized record %q is not in the pending page", errPendingPageIdentity, messageID)
	}
	if len(page.Dispositions) != len(page.Records) || strings.TrimSpace(page.Dispositions[index]) != "oversized_record" {
		return false, fmt.Errorf("%w: record %q is not a retryable oversized record", errPendingPageIdentity, messageID)
	}
	if len(page.RefetchFailures) == 0 {
		page.RefetchFailures = make([]int, len(page.Records))
	} else if len(page.RefetchFailures) != len(page.Records) {
		return false, fmt.Errorf("%w: refetch failure metadata length mismatch", errPendingPageInvalid)
	}
	page.RefetchFailures[index]++
	if page.RefetchFailures[index] >= maxOversizedRecordRefetchAttempts {
		page.Dispositions[index] = "oversized_record_quarantined"
		page.ReceiptID = pendingPageReceiptID(page)
	}
	return true, nil
}

func appendPollQuarantinedRecordIDs(poll *teamstore.ChatPollState, ids ...string) {
	if poll == nil {
		return
	}
	seen := make(map[string]struct{}, len(poll.QuarantinedRecordIDs)+len(ids))
	for _, id := range poll.QuarantinedRecordIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			seen[id] = struct{}{}
		}
	}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		poll.QuarantinedRecordIDs = append(poll.QuarantinedRecordIDs, id)
	}
	const maxQuarantinedRecordIDs = 32
	if len(poll.QuarantinedRecordIDs) > maxQuarantinedRecordIDs {
		poll.QuarantinedRecordIDs = append([]string(nil), poll.QuarantinedRecordIDs[len(poll.QuarantinedRecordIDs)-maxQuarantinedRecordIDs:]...)
	}
}

func pollPageRequestForState(chatID string, top int, role inboundPollRole, poll teamstore.ChatPollState) (string, string, time.Time) {
	if role == inboundPollRoleWork {
		if poll.Gap != nil {
			if strings.TrimSpace(poll.Gap.RecoveryPath) != "" {
				return pollFrontierGap, strings.TrimSpace(poll.Gap.RecoveryPath), time.Time{}
			}
			// A completed recovery page may have promoted a deferred
			// continuation. Drain that durable P lane before taking another
			// recovery-head sample; otherwise an open gap would permanently
			// shadow the very frontier it just made reachable.
			if strings.TrimSpace(poll.ContinuationPath) != "" {
				return pollFrontierContinuation, strings.TrimSpace(poll.ContinuationPath), time.Time{}
			}
			// An open gap with no opaque recovery path still owns the
			// next head request. It must use RecoveryCursor and remain
			// labelled as gap recovery so SafeCursor is never advanced.
		}
		if strings.TrimSpace(poll.ContinuationPath) != "" {
			return pollFrontierContinuation, strings.TrimSpace(poll.ContinuationPath), time.Time{}
		}
	}
	var modifiedAfter time.Time
	if poll.Gap != nil {
		modifiedAfter = poll.Gap.RecoveryCursor
	} else {
		modifiedAfter = poll.LastModifiedCursor
	}
	if !modifiedAfter.IsZero() {
		modifiedAfter = modifiedAfter.Add(-pollCursorOverlap)
	}
	if role == inboundPollRoleWork && poll.Gap != nil {
		// Gap recovery is deliberately low-volume. A smaller head makes a
		// single huge image/tool payload isolatable and lets later messages
		// become visible without increasing the normal polling limit.
		return pollFrontierGap, chatMessagesExactTopPath(chatID, 1, modifiedAfter), modifiedAfter
	}
	return pollFrontierHead, chatMessagesPath(chatID, top, modifiedAfter), modifiedAfter
}

// reducePollFrontier is the pure compatibility reducer for the old P/D
// representation. It never performs I/O and is safe to table-test in
// isolation. The runtime only persists the result before issuing Graph I/O.
func reducePollFrontier(poll teamstore.ChatPollState) (teamstore.ChatPollState, bool) {
	changed := false
	if normalized := trimContinuationHistory(poll.ContinuationPathHistory); len(normalized) != len(poll.ContinuationPathHistory) {
		poll.ContinuationPathHistory = normalized
		changed = true
	}
	if normalized := trimContinuationHistory(poll.ContinuationPageFingerprintHistory); len(normalized) != len(poll.ContinuationPageFingerprintHistory) {
		poll.ContinuationPageFingerprintHistory = normalized
		changed = true
	}
	if poll.PendingPage != nil {
		return poll, changed
	}
	if strings.TrimSpace(poll.ContinuationPath) != "" && strings.TrimSpace(poll.ContinuationPath) == strings.TrimSpace(poll.DeferredContinuationPath) {
		poll.DeferredContinuationPath = ""
		return poll, true
	}
	if strings.TrimSpace(poll.ContinuationPath) == "" && strings.TrimSpace(poll.DeferredContinuationPath) != "" {
		poll.ContinuationPath = strings.TrimSpace(poll.DeferredContinuationPath)
		poll.DeferredContinuationPath = ""
		poll.FrontierEpoch = normalizeFrontierEpochForPoll(poll) + 1
		return poll, true
	}
	return poll, changed
}

func trimContinuationHistory(values []string) []string {
	if len(values) <= continuationHistoryLimit {
		return values
	}
	return append([]string(nil), values[len(values)-continuationHistoryLimit:]...)
}

// normalizePollFrontier persists reducePollFrontier before any Graph request.
// No network call occurs while the durable state has two operational lanes.
func normalizePollFrontier(ctx context.Context, store *teamstore.Store, chatID string) (teamstore.ChatPollState, bool, error) {
	if store == nil {
		return teamstore.ChatPollState{}, false, fmt.Errorf("teams store is required")
	}
	poll, ok, err := store.ChatPoll(ctx, chatID)
	if err != nil {
		return poll, ok, err
	}
	if !ok {
		return poll, ok, nil
	}
	if _, changed := reducePollFrontier(poll); !changed {
		return poll, ok, nil
	}
	updated, changed, err := store.UpdateChatPoll(ctx, chatID, func(current *teamstore.ChatPollState) error {
		normalized, changed := reducePollFrontier(*current)
		if changed {
			*current = normalized
			// Normalization is a poll-state mutation performed outside the
			// in-flight attempt. Retire the old capability immediately so its
			// owner cannot commit against the normalized representation and the
			// next owner does not wait for the attempt TTL.
			current.Attempt = nil
		}
		return nil
	})
	if err != nil {
		return teamstore.ChatPollState{}, false, err
	}
	if changed {
		return updated, true, nil
	}
	return updated, strings.TrimSpace(updated.ChatID) != "", nil
}

func continuationErrorIsPermanent(path string, err error) bool {
	if err == nil {
		return false
	}
	return stalePollContinuationErrorForPath(path, err)
}

func continuationFailureBudgetExceeded(poll teamstore.ChatPollState, now time.Time) bool {
	if poll.ContinuationFailureCount >= continuationFailureBudget {
		return true
	}
	return !poll.ContinuationFirstFailureAt.IsZero() && now.Sub(poll.ContinuationFirstFailureAt) >= continuationFailureMaxAge
}

func pollPageHasOperationalFrontier(poll teamstore.ChatPollState) bool {
	return poll.PendingPage != nil ||
		strings.TrimSpace(poll.ContinuationPath) != "" ||
		strings.TrimSpace(poll.DeferredContinuationPath) != "" ||
		poll.Gap != nil
}

// pollFrontierNeedsImmediateRetry distinguishes a durable frontier that has a
// concrete local action from an open gap whose opaque cursor has already been
// exhausted. The latter still needs periodic recovery-head sampling, but
// scheduling it at now would turn an empty recovery page into a tight loop.
func pollFrontierNeedsImmediateRetry(poll teamstore.ChatPollState) bool {
	return poll.PendingPage != nil ||
		strings.TrimSpace(poll.ContinuationPath) != "" ||
		strings.TrimSpace(poll.DeferredContinuationPath) != "" ||
		poll.Gap != nil && strings.TrimSpace(poll.Gap.RecoveryPath) != ""
}

func pollPageBackoff(poll teamstore.ChatPollState, err error, now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now()
	}
	// Reuse the existing bounded exponential policy, but do not persist its
	// result as BlockedUntil. A retry schedule is not a semantic chat block.
	return inboundPollBlockedUntil(poll, err, now)
}

func (b *Bridge) commitPollAttemptFailure(ctx context.Context, chatID, attemptID string, expectedRevision uint64, source, path string, pollErr error, forceGap bool, noProgress bool) (bool, error) {
	return b.commitPollAttemptFailureInternal(ctx, chatID, attemptID, nil, expectedRevision, source, path, pollErr, forceGap, noProgress)
}

func (b *Bridge) commitPollAttemptFailureWithCapability(ctx context.Context, chatID string, capability teamstore.ChatPollAttemptCapability, expectedRevision uint64, source, path string, pollErr error, forceGap bool, noProgress bool) (bool, error) {
	return b.commitPollAttemptFailureInternal(ctx, chatID, capability.ID, &capability, expectedRevision, source, path, pollErr, forceGap, noProgress)
}

func (b *Bridge) commitPollAttemptFailureInternal(ctx context.Context, chatID, attemptID string, capability *teamstore.ChatPollAttemptCapability, expectedRevision uint64, source, path string, pollErr error, forceGap bool, noProgress bool) (bool, error) {
	if pollErr == nil {
		pollErr = errors.New("Teams poll failed")
	}
	message := trimPollDiagnostic(pollErr.Error())
	permanentFrontier := source == pollFrontierContinuation || source == pollFrontierGap
	now := time.Now()
	commit := func(fn func(*teamstore.ChatPollState) error) (teamstore.ChatPollState, bool, error) {
		if capability != nil {
			return b.store.CommitChatPollAttemptWithCapability(ctx, chatID, *capability, expectedRevision, fn)
		}
		return b.store.CommitChatPollAttempt(ctx, chatID, attemptID, expectedRevision, fn)
	}
	_, committed, err := commit(func(poll *teamstore.ChatPollState) error {
		poll.LastError = message
		poll.LastErrorAt = now
		poll.FailureCount++
		poll.BlockedUntil = time.Time{}
		if poll.PollState == inboundPollStateBlocked {
			poll.PollState = poll.PreviousPollState
			if strings.TrimSpace(poll.PollState) == "" || poll.PollState == inboundPollStateBlocked {
				poll.PollState = inboundPollStateWarm
			}
		}
		if strings.TrimSpace(poll.PollState) == "" {
			poll.PollState = inboundPollStateWarm
		}
		if permanentFrontier {
			if strings.TrimSpace(poll.ContinuationLastPath) != strings.TrimSpace(path) {
				poll.ContinuationFailureCount = 0
				poll.ContinuationFirstFailureAt = now
			}
			poll.ContinuationFailureCount++
			poll.ContinuationLastFailureAt = now
			poll.ContinuationLastPath = strings.TrimSpace(path)
			if noProgress {
				poll.ContinuationNoProgressCount++
			}
			// Every failed attempt against the same opaque frontier consumes
			// the finite liveness budget, including generic transport/429/5xx
			// failures. A permanent network outage must not keep one chat in
			// an endless retry lane. Strong token failures and malformed pages
			// pass forceGap and skip the budget delay; ordinary failures are
			// initially retained with their Retry-After/backoff and enter the
			// explicit gap only after the bounded budget or age is reached.
			if forceGap || continuationFailureBudgetExceeded(*poll, now) {
				if source == pollFrontierContinuation {
					openPollGap(poll, "unverified-continuation", message, path, now)
				} else {
					// A recovery path can itself be stale or self-looping. Keep the
					// gap and its SafeCursor, but discard only this bad recovery
					// path so the next quantum can try the directional recovery
					// head. Never turn this into a new normal cursor. If a page
					// was already staged for this recovery request, retain its
					// bounded receipt as manual-recovery evidence before removing
					// it from the executable lane.
					movePendingPageToGapEvidence(poll)
					if poll.Gap != nil {
						poll.Gap.RecoveryPath = ""
						poll.Gap.LastProgressAt = now
					}
				}
			}
		} else if forceGap {
			// The page receipt or decoded page was malformed before a normal
			// continuation existed. Retain the chat's SafeCursor and move to
			// an explicit recovery lane; otherwise the same corrupt receipt
			// would be retried forever.
			if source == pollFrontierGap {
				movePendingPageToGapEvidence(poll)
				if poll.Gap != nil {
					poll.Gap.RecoveryPath = ""
					poll.Gap.LastProgressAt = now
				}
			} else {
				openPollGap(poll, "invalid-page", message, path, now)
			}
		}
		poll.NextPollAt = pollPageBackoff(*poll, pollErr, now)
		if poll.Gap != nil && source == pollFrontierContinuation && strings.TrimSpace(poll.Gap.RecoveryPath) == "" {
			// Once the continuation is quarantined, allow the directional
			// recovery head to run promptly. Its cursor is separate from the
			// normal SafeCursor and therefore cannot silently skip the gap.
			poll.NextPollAt = now.Add(time.Second)
		}
		poll.UpdatedAt = now
		return nil
	})
	if err != nil {
		return committed, err
	}
	return committed, nil
}

func (b *Bridge) commitPollAttemptPartial(ctx context.Context, chatID, attemptID string, expectedRevision uint64, result pollMessageWindowResult, quarantine bool) (bool, error) {
	return b.commitPollAttemptPartialInternal(ctx, chatID, attemptID, nil, expectedRevision, result, quarantine)
}

func (b *Bridge) commitPollAttemptPartialWithCapability(ctx context.Context, chatID string, capability teamstore.ChatPollAttemptCapability, expectedRevision uint64, result pollMessageWindowResult, quarantine bool) (bool, error) {
	return b.commitPollAttemptPartialInternal(ctx, chatID, capability.ID, &capability, expectedRevision, result, quarantine)
}

func (b *Bridge) commitPollAttemptPartialInternal(ctx context.Context, chatID, attemptID string, capability *teamstore.ChatPollAttemptCapability, expectedRevision uint64, result pollMessageWindowResult, quarantine bool) (bool, error) {
	now := time.Now()
	commit := func(fn func(*teamstore.ChatPollState) error) (teamstore.ChatPollState, bool, error) {
		if capability != nil {
			return b.store.CommitChatPollAttemptWithCapability(ctx, chatID, *capability, expectedRevision, fn)
		}
		return b.store.CommitChatPollAttempt(ctx, chatID, attemptID, expectedRevision, fn)
	}
	_, committed, err := commit(func(poll *teamstore.ChatPollState) error {
		if result.ActivityAt.After(poll.LastActivityAt) {
			poll.LastActivityAt = result.ActivityAt
		}
		appendPollQuarantinedRecordIDs(poll, result.QuarantinedRecordIDs...)
		if quarantine {
			// Keep the page durable but stop scheduling it as live input until an
			// explicit unquarantine. Unknown records after the breaker boundary
			// must not be discarded or executed under the containment fence.
			poll.PollState = inboundPollStateParked
			if poll.ParkedAt.IsZero() {
				poll.ParkedAt = now
			}
			poll.NextPollAt = time.Time{}
		} else if poll.PollState == inboundPollStateBlocked || strings.TrimSpace(poll.PollState) == "" {
			poll.PollState = inboundPollStateWarm
		} else if result.ActiveClaimHeld {
			poll.NextPollAt = now.Add(2 * time.Second)
		} else {
			poll.NextPollAt = now
		}
		poll.BlockedUntil = time.Time{}
		poll.UpdatedAt = now
		return nil
	})
	if err != nil {
		return committed, err
	}
	return committed, nil
}

func (b *Bridge) commitPollAttemptSuccess(ctx context.Context, chatID, attemptID string, expectedRevision uint64, role inboundPollRole, running bool, source, path string, window MessageWindow, result pollMessageWindowResult, quarantine bool) (bool, error) {
	return b.commitPollAttemptSuccessInternal(ctx, chatID, attemptID, nil, expectedRevision, role, running, source, path, window, result, quarantine)
}

func (b *Bridge) commitPollAttemptSuccessWithCapability(ctx context.Context, chatID string, capability teamstore.ChatPollAttemptCapability, expectedRevision uint64, role inboundPollRole, running bool, source, path string, window MessageWindow, result pollMessageWindowResult, quarantine bool) (bool, error) {
	return b.commitPollAttemptSuccessInternal(ctx, chatID, capability.ID, &capability, expectedRevision, role, running, source, path, window, result, quarantine)
}

func (b *Bridge) commitPollAttemptSuccessInternal(ctx context.Context, chatID, attemptID string, capability *teamstore.ChatPollAttemptCapability, expectedRevision uint64, role inboundPollRole, running bool, source, path string, window MessageWindow, result pollMessageWindowResult, quarantine bool) (bool, error) {
	now := time.Now()
	apply := func(poll *teamstore.ChatPollState) error {
		hadDeferredContinuation := strings.TrimSpace(poll.DeferredContinuationPath) != ""
		pendingPage := poll.PendingPage
		pageFingerprint := pendingPageContentFingerprint(pendingPage)
		poll.PendingPage = nil
		poll.Seeded = true
		poll.LastSuccessfulPollAt = now
		poll.LastError = ""
		poll.LastErrorAt = time.Time{}
		poll.BlockedUntil = time.Time{}
		poll.FailureCount = 0
		poll.ContinuationFailureCount = 0
		poll.ContinuationFirstFailureAt = time.Time{}
		poll.ContinuationLastFailureAt = time.Time{}
		poll.ContinuationLastPath = ""
		poll.ContinuationNoProgressCount = 0
		if result.ActivityAt.After(poll.LastActivityAt) {
			poll.LastActivityAt = result.ActivityAt
		}
		appendPollQuarantinedRecordIDs(poll, result.QuarantinedRecordIDs...)
		// A compatibility D frontier may contain newer work whose ownership is
		// unresolved. Completing the older P page must not move SafeCursor past
		// that frontier, or a subsequent head read could skip its claim.
		if result.MaxModified.After(poll.LastModifiedCursor) && source != pollFrontierGap && !hadDeferredContinuation {
			poll.LastModifiedCursor = result.MaxModified
		}
		if quarantine && role == inboundPollRoleWork {
			// The self-echo breaker is an explicit discard boundary. It may
			// consume the fetched page, but it must not leave a continuation
			// that replays the same helper output after unquarantine.
			poll.PollState = inboundPollStateParked
			if poll.ParkedAt.IsZero() {
				poll.ParkedAt = now
			}
			poll.NextPollAt = time.Time{}
			poll.ContinuationPath = ""
			poll.DeferredContinuationPath = ""
			poll.Gap = nil
		} else if source == pollFrontierGap {
			if poll.Gap == nil {
				poll.Gap = &teamstore.ChatPollGap{Epoch: 1, SafeCursor: poll.LastModifiedCursor, RecoveryCursor: poll.LastModifiedCursor, OpenedAt: now}
			}
			if result.MaxModified.After(poll.Gap.RecoveryCursor) {
				poll.Gap.RecoveryCursor = result.MaxModified
			}
			if window.Truncated {
				poll.Gap.RecoveryPath = strings.TrimSpace(window.NextPath)
			} else {
				poll.Gap.RecoveryPath = ""
				if strings.TrimSpace(poll.DeferredContinuationPath) != "" {
					poll.ContinuationPath = strings.TrimSpace(poll.DeferredContinuationPath)
					poll.DeferredContinuationPath = ""
					poll.FrontierEpoch = normalizeFrontierEpochForPoll(*poll) + 1
				}
			}
		} else if window.baselineOnly {
			// Initial discovery establishes the boundary but must not walk older
			// Graph pages. This matches the pre-receipt seed semantics and keeps a
			// crash replay from converting historical chat contents into backlog.
			poll.ContinuationPath = ""
			poll.DeferredContinuationPath = ""
		} else {
			if window.Truncated {
				poll.ContinuationPath = strings.TrimSpace(window.NextPath)
			} else {
				poll.ContinuationPath = ""
				if strings.TrimSpace(poll.DeferredContinuationPath) != "" {
					poll.ContinuationPath = strings.TrimSpace(poll.DeferredContinuationPath)
					poll.DeferredContinuationPath = ""
					poll.FrontierEpoch = normalizeFrontierEpochForPoll(*poll) + 1
				}
			}
		}
		if role == inboundPollRoleControl {
			// Control history is diagnostic input, not a work-chat backlog. Do not
			// retain its nextLink as an operational continuation.
			poll.ContinuationPath = ""
			poll.DeferredContinuationPath = ""
		}
		// A successful page starts or continues one directional lane. Keep only
		// bounded path/content evidence while that lane has another page; once it
		// reaches a terminal page (or changes lane) the old evidence is no longer
		// relevant. Head starts a fresh lane, while continuation and gap recovery
		// extend the existing one. This evidence is what catches Graph returning
		// the same page under a newly generated nextLink.
		if role == inboundPollRoleControl || quarantine || !window.Truncated || strings.TrimSpace(window.NextPath) == "" {
			clearContinuationHistory(poll)
		} else {
			if source == pollFrontierHead {
				clearContinuationHistory(poll)
			}
			if pageFingerprint != "" {
				appendContinuationHistory(poll, path, pendingPage)
			}
		}
		if result.WindowFull {
			poll.LastWindowFullAt = now
			poll.LastWindowFullMessage = fmt.Sprintf("Graph returned a full message window (%d messages); older unprocessed messages may require a larger recovery pass", result.Fetched)
		} else {
			poll.LastWindowFullMessage = ""
		}
		if poll.FrontierEpoch == 0 {
			poll.FrontierEpoch = 1
		}
		if role == inboundPollRoleWork && result.Handled {
			// A message observed during a parked probe is explicit wake-up
			// evidence. Clear stale park metadata even when the scheduler sees
			// equal activity timestamps on this same commit.
			poll.ParkedAt = time.Time{}
			poll.ParkNoticeSentAt = time.Time{}
			poll.PollState = inboundPollStateHot
			poll.NextPollAt = now
		}
		if !quarantine {
			decision := decideInboundPoll(inboundPollInput{
				ChatID:          chatID,
				Role:            role,
				Poll:            *poll,
				HasPoll:         true,
				Running:         running,
				ForceActivityAt: result.ActivityAt,
				Now:             now,
			})
			if pollPageHasOperationalFrontier(*poll) {
				if poll.Gap != nil && !pollFrontierNeedsImmediateRetry(*poll) {
					// The recovery cursor remains deliberately conservative after an
					// empty/deduplicated page. Keep sampling for newly visible records,
					// but do not spin on the same empty head forever.
					interval := inboundPollColdInterval
					if result.Handled {
						interval = inboundPollWarmInterval
					}
					poll.NextPollAt = now.Add(interval)
				} else {
					poll.NextPollAt = now
				}
				if decision.State != "" && decision.State != inboundPollStateParked {
					poll.PollState = decision.State
				}
			} else {
				poll.PollState = decision.State
				poll.NextPollAt = decision.NextPollAt
				if decision.Interval > 0 {
					poll.NextPollAt = now.Add(decision.Interval)
				}
			}
		}
		if strings.TrimSpace(poll.PollState) != inboundPollStateParked {
			poll.ParkedAt = time.Time{}
			poll.ParkNoticeSentAt = time.Time{}
		}
		poll.UpdatedAt = now
		return nil
	}
	var committed bool
	var err error
	if capability != nil {
		_, committed, err = b.store.CommitChatPollAttemptWithCapability(ctx, chatID, *capability, expectedRevision, apply)
	} else {
		_, committed, err = b.store.CommitChatPollAttempt(ctx, chatID, attemptID, expectedRevision, apply)
	}
	if err != nil {
		return committed, err
	}
	return committed, nil
}

func openPollGap(poll *teamstore.ChatPollState, kind, reason, evidence string, now time.Time) {
	if poll == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	epoch := uint64(1)
	if poll.Gap != nil && poll.Gap.Epoch >= epoch {
		epoch = poll.Gap.Epoch + 1
	}
	quarantinedPage := poll.PendingPage
	if quarantinedPage == nil && poll.Gap != nil {
		quarantinedPage = poll.Gap.QuarantinedPage
	}
	frontierPath := strings.TrimSpace(poll.ContinuationPath)
	if frontierPath == "" {
		frontierPath = strings.TrimSpace(evidence)
	}
	poll.Gap = &teamstore.ChatPollGap{
		Epoch:           epoch,
		Kind:            strings.TrimSpace(kind),
		Reason:          trimPollDiagnostic(reason),
		Evidence:        trimPollDiagnostic(evidence),
		FrontierPath:    frontierPath,
		SafeCursor:      poll.LastModifiedCursor,
		RecoveryCursor:  poll.LastModifiedCursor,
		OpenedAt:        now,
		NoticeEpoch:     epoch,
		QuarantinedPage: quarantinedPage,
	}
	// The page that led to a repeated/ambiguous frontier is removed from the
	// executable lane but retained as bounded evidence under the gap. This gives
	// recovery/manual tooling the original receipt without letting a malformed
	// page starve the directional recovery head.
	poll.PendingPage = nil
	poll.ContinuationPath = ""
	poll.ContinuationFailureCount = 0
	poll.ContinuationFirstFailureAt = time.Time{}
	poll.ContinuationLastFailureAt = time.Time{}
	poll.ContinuationLastPath = ""
	poll.ContinuationNoProgressCount = 0
	clearContinuationHistory(poll)
}

// movePendingPageToGapEvidence retires a page that cannot safely be replayed
// or completed in the executable lane. The original receipt remains available
// for diagnostics/manual reconciliation, while the directional recovery head
// is free to continue looking for newer messages. Only one bounded receipt is
// retained by a gap; the normal scheduler never treats it as executable.
func movePendingPageToGapEvidence(poll *teamstore.ChatPollState) {
	if poll == nil || poll.Gap == nil || poll.PendingPage == nil {
		return
	}
	poll.Gap.QuarantinedPage = poll.PendingPage
	poll.PendingPage = nil
}

func trimPollDiagnostic(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 240 {
		return value[:240]
	}
	return value
}
