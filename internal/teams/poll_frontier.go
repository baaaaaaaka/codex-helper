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
	continuationPageBudget            = 64
	maxOversizedRecordRefetchAttempts = 3
	// Graph message responses are bounded by maxGraphMessagesResponseBytes. Keep the durable
	// page receipt at the same bound so a valid response containing several
	// large (but individually recoverable) records is not turned into a
	// permanent gap retry merely because the receipt used a smaller limit.
	maxPendingPageBytes   = 64 << 20
	maxPendingRecordBytes = 12 << 20
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
		BoundaryReason:     trimPollDiagnostic(window.boundaryReason),
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
		} else if msg.invalidForPoll {
			disposition = "invalid_record"
		} else if msg.quarantinedForPoll {
			disposition = "invalid_record_quarantined"
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
	if boundary := strings.TrimSpace(page.BoundaryReason); boundary != "" {
		_, _ = fmt.Fprintf(h, "\x00boundary=%s", boundary)
	}
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
	window := MessageWindow{
		Truncated:      strings.TrimSpace(page.NextPath) != "",
		NextPath:       strings.TrimSpace(page.NextPath),
		baselineOnly:   page.BaselineOnly,
		boundaryReason: strings.TrimSpace(page.BoundaryReason),
	}
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
			case "invalid_record":
				msg.invalidForPoll = true
			case "oversized_record_quarantined", "invalid_record_quarantined":
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
// exceptional record. It never changes a normal record into a skipped record.
// Once the bounded recovery budget is exhausted, only that record is marked
// quarantined; the rest of the immutable page remains replayable.
func notePendingPageRefetchFailure(page *teamstore.ChatPollPendingPage, messageID string) (bool, error) {
	if page == nil || strings.TrimSpace(messageID) == "" {
		return false, fmt.Errorf("%w: exceptional record identity is missing", errPendingPageInvalid)
	}
	index := -1
	for i, id := range page.RecordIDs {
		if strings.TrimSpace(id) == strings.TrimSpace(messageID) {
			index = i
			break
		}
	}
	if index < 0 || index >= len(page.Records) {
		return false, fmt.Errorf("%w: exceptional record %q is not in the pending page", errPendingPageIdentity, messageID)
	}
	if len(page.Dispositions) != len(page.Records) {
		return false, fmt.Errorf("%w: refetch failure metadata length mismatch", errPendingPageInvalid)
	}
	disposition := strings.TrimSpace(page.Dispositions[index])
	if disposition != "oversized_record" && disposition != "invalid_record" {
		return false, fmt.Errorf("%w: record %q is not a retryable exceptional record", errPendingPageIdentity, messageID)
	}
	if len(page.RefetchFailures) == 0 {
		page.RefetchFailures = make([]int, len(page.Records))
	} else if len(page.RefetchFailures) != len(page.Records) {
		return false, fmt.Errorf("%w: refetch failure metadata length mismatch", errPendingPageInvalid)
	}
	page.RefetchFailures[index]++
	if page.RefetchFailures[index] >= maxOversizedRecordRefetchAttempts {
		if disposition == "invalid_record" {
			page.Dispositions[index] = "invalid_record_quarantined"
		} else {
			page.Dispositions[index] = "oversized_record_quarantined"
		}
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
			if poll.Gap.HeadProbePending {
				// A terminal empty recovery page does not prove that a newly
				// appended message is absent. Take a bounded normal-head sample
				// while retaining the old gap for later recovery.
				modifiedAfter := poll.LastModifiedCursor
				if !modifiedAfter.IsZero() {
					modifiedAfter = modifiedAfter.Add(-pollCursorOverlap)
				}
				return pollFrontierHead, chatMessagesPath(chatID, top, modifiedAfter), modifiedAfter
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
		modifiedAfter = poll.Gap.SafeCursor
	} else {
		modifiedAfter = poll.LastModifiedCursor
	}
	if !modifiedAfter.IsZero() && poll.Gap == nil {
		modifiedAfter = modifiedAfter.Add(-pollCursorOverlap)
	}
	if role == inboundPollRoleWork && poll.Gap != nil {
		// Graph v1.0 supports descending lastModifiedDateTime ordering only. The
		// durable RecoveryCursor is an inclusive upper bound that moves toward
		// SafeCursor after a complete page, so an expired opaque continuation can
		// still be recovered without an unsupported ascending request, repeated
		// newest pages, or a skipped older suffix.
		return pollFrontierGap, chatMessagesGapPath(chatID, ownerPollMessageTop, poll.Gap.SafeCursor, poll.Gap.RecoveryCursor), modifiedAfter
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
	if strings.TrimSpace(poll.ContinuationPath) == "" && strings.TrimSpace(poll.DeferredContinuationPath) == "" && !poll.ContinuationSafeCursor.IsZero() {
		poll.ContinuationSafeCursor = time.Time{}
		poll.ContinuationSafeCursorKnown = false
		return poll, true
	}
	if strings.TrimSpace(poll.ContinuationPath) == "" && strings.TrimSpace(poll.DeferredContinuationPath) == "" && poll.ContinuationSafeCursorKnown {
		poll.ContinuationSafeCursorKnown = false
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
	return normalizePollFrontierForOwner(ctx, store, chatID, "", 0)
}

func normalizePollFrontierForOwner(ctx context.Context, store *teamstore.Store, chatID, machineID string, leaseGeneration int64) (teamstore.ChatPollState, bool, error) {
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
	// Do not even start a compatibility rewrite while a poll attempt is live.
	// The callback below repeats this check inside the store transaction for the
	// read/modify race, but this fast path avoids an unnecessary durable-state
	// round trip on the normal in-flight path.
	if poll.Attempt != nil {
		return poll, ok, nil
	}
	if _, changed := reducePollFrontier(poll); !changed {
		return poll, ok, nil
	}
	mutate := func(current *teamstore.ChatPollState) error {
		// If another poller has already acquired the row, never retire its live
		// attempt while reducing the legacy P/D representation. The next cycle
		// will normalize after that attempt commits or is abandoned. Live callers
		// additionally fence this mutation to their control lease.
		if current.Attempt != nil {
			return nil
		}
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
	}
	var updated teamstore.ChatPollState
	var changed bool
	if strings.TrimSpace(machineID) != "" && leaseGeneration > 0 {
		updated, changed, err = store.UpdateChatPollForOwner(ctx, chatID, machineID, leaseGeneration, mutate)
	} else {
		updated, changed, err = store.UpdateChatPoll(ctx, chatID, mutate)
	}
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
					// The first failure of an opaque recovery link should fall back to
					// the durable time-bounded gap query. That is a different,
					// provider-supported frontier and makes records behind an expired
					// nextLink reachable. Only a failure of that fallback, or an
					// explicitly detected no-progress condition, should schedule a
					// normal head probe; probing the head here could bypass the old
					// suffix and leave the gap unrecovered.
					hadOpaqueRecoveryPath := poll.Gap != nil && strings.TrimSpace(poll.Gap.RecoveryPath) != ""
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
						// The opaque recovery cursor has been exhausted. Mark the
						// next action explicitly as a bounded head probe; otherwise
						// pollPageRequestForState reconstructs the same gap query on
						// every retry and a provider that repeats that page can make
						// the directional frontier permanently livelock.
						poll.Gap.HeadProbePending = noProgress || !hadOpaqueRecoveryPath
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
		if err := persistPollRefetchedMessages(poll.PendingPage, result.RefetchedMessages); err != nil {
			return err
		}
		// A partial commit is still a successfully serviced poll quantum. The
		// page remains pending when the action budget is exhausted, but the
		// durable service-age must advance so a continuously due chat cannot keep
		// winning the same cycle cap and starve another chat behind it.
		clearChatPollRecoveryMarker(poll)
		poll.LastSuccessfulPollAt = now
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

// persistPollRefetchedMessages upgrades exceptional records in the durable
// pending receipt after their individual item fetch succeeds. The list-page
// marker remains the only fallback when a recovered item is too large for the
// bounded receipt; in that exceptional case the next quantum will safely
// re-fetch it rather than storing an incomplete user prompt.
func persistPollRefetchedMessages(page *teamstore.ChatPollPendingPage, messages []ChatMessage) error {
	if page == nil || len(messages) == 0 {
		return nil
	}
	if len(page.Records) != len(page.RecordIDs) || len(page.Records) != len(page.RecordHashes) || len(page.Dispositions) != len(page.Records) {
		return fmt.Errorf("%w: refetched receipt metadata length mismatch", errPendingPageInvalid)
	}
	changed := false
	for _, message := range messages {
		messageID := strings.TrimSpace(message.ID)
		if messageID == "" {
			return fmt.Errorf("%w: refetched message has no stable id", errPendingPageIdentity)
		}
		index := -1
		for i, id := range page.RecordIDs {
			if strings.TrimSpace(id) == messageID {
				index = i
				break
			}
		}
		if index < 0 {
			return fmt.Errorf("%w: refetched message %q is not in the pending page", errPendingPageIdentity, messageID)
		}
		currentDisposition := strings.TrimSpace(page.Dispositions[index])
		if currentDisposition != "oversized_record" && currentDisposition != "invalid_record" {
			continue
		}
		raw, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("%w: marshal refetched message %q: %v", errPendingPageInvalid, messageID, err)
		}
		if len(raw) > maxPendingRecordBytes {
			continue
		}
		hash := sha256.Sum256(raw)
		page.Records[index] = json.RawMessage(raw)
		page.RecordHashes[index] = hex.EncodeToString(hash[:])
		page.Dispositions[index] = "received"
		if len(page.RefetchFailures) == len(page.Records) {
			page.RefetchFailures[index] = 0
		}
		changed = true
	}
	if changed {
		page.ReceiptID = pendingPageReceiptID(page)
	}
	return nil
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
		clearChatPollRecoveryMarker(poll)
		hadDeferredContinuation := strings.TrimSpace(poll.DeferredContinuationPath) != ""
		previousSafeCursor := poll.LastModifiedCursor
		pendingPage := poll.PendingPage
		pageFingerprint := pendingPageContentFingerprint(pendingPage)
		boundaryReason := strings.TrimSpace(window.boundaryReason)
		continuationPage := (source == pollFrontierContinuation || source == pollFrontierGap) &&
			window.Truncated && strings.TrimSpace(window.NextPath) != ""
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
		if continuationPage {
			poll.ContinuationPageCount++
		} else {
			// A head request starts a fresh lane, and a terminal page has drained
			// the current lane. Do not carry an old budget into unrelated work.
			poll.ContinuationPageCount = 0
		}
		if result.ActivityAt.After(poll.LastActivityAt) {
			poll.LastActivityAt = result.ActivityAt
		}
		appendPollQuarantinedRecordIDs(poll, result.QuarantinedRecordIDs...)
		// A compatibility D frontier may contain newer work whose ownership is
		// unresolved. Completing the older P page must not move SafeCursor past
		// that frontier, or a subsequent head read could skip its claim.
		// A head probe taken while a directional gap is open is only a bounded
		// sample.  It must not advance the normal cursor: doing so can skip the
		// older continuation page when the probe itself is truncated.
		headProbeWithGap := source == pollFrontierHead && poll.Gap != nil
		if result.MaxModified.After(poll.LastModifiedCursor) && source != pollFrontierGap && !hadDeferredContinuation && !headProbeWithGap && boundaryReason == "" {
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
		} else if boundaryReason != "" && !window.baselineOnly {
			// The records in this page were usable, but its continuation was not.
			// Keep the normal cursor at the predecessor boundary and put the page's
			// observed upper bound in the directional gap. This may re-read the
			// handled page during recovery, but the inbound ledger makes that safe;
			// advancing past it would make the unknown older suffix unreachable.
			if poll.Gap == nil {
				poll.LastModifiedCursor = previousSafeCursor
				openPollGap(poll, "invalid-next-link", boundaryReason, path, now)
				if poll.Gap != nil && result.MaxModified.After(poll.Gap.RecoveryCursor) {
					poll.Gap.RecoveryCursor = result.MaxModified
				}
			} else {
				// A bounded head probe can encounter a new invalid continuation while
				// an older gap is already open. Retain that gap and schedule another
				// probe; do not replace its safe cursor with the new page.
				poll.Gap.RecoveryPath = ""
				poll.Gap.LastProgressAt = now
				poll.Gap.HeadProbePending = true
			}
		} else if source == pollFrontierGap {
			if poll.Gap == nil {
				poll.Gap = &teamstore.ChatPollGap{Epoch: 1, SafeCursor: poll.LastModifiedCursor, RecoveryCursor: poll.LastModifiedCursor, OpenedAt: now}
			}
			// Recovery pages are fetched newest-first because Graph does not
			// support ascending order. Move the durable upper bound to the oldest
			// record in a fully handled page; the next fallback query can then reach
			// the older suffix instead of asking for the same newest page again.
			if !result.MinModified.IsZero() &&
				(poll.Gap.RecoveryCursor.IsZero() || result.MinModified.Before(poll.Gap.RecoveryCursor)) {
				poll.Gap.RecoveryCursor = result.MinModified
			}
			if window.Truncated {
				poll.Gap.RecoveryPath = strings.TrimSpace(window.NextPath)
			} else if result.Progressed && !result.MinModified.IsZero() {
				poll.Gap.HeadProbePending = false
				poll.Gap.RecoveryPath = ""
				if strings.TrimSpace(poll.DeferredContinuationPath) != "" {
					poll.ContinuationPath = strings.TrimSpace(poll.DeferredContinuationPath)
					poll.DeferredContinuationPath = ""
					poll.FrontierEpoch = normalizeFrontierEpochForPoll(*poll) + 1
					poll.Gap = nil
				} else {
					// A terminal bounded recovery page is proof that the current
					// unresolved time range has been enumerated. Release only this
					// directional gap; the normal cursor remains unchanged and the
					// next head poll will still discover newer records.
					poll.Gap = nil
				}
			} else {
				// An empty/deduplicated page has no durable lower bound. Do not
				// treat it as proof that the unresolved interval was enumerated:
				// provider filtering, clock precision, or a transient empty page
				// could otherwise make later records unreachable. Retain the gap,
				// clear only the opaque path, and let the scheduler back off before
				// taking another bounded recovery-head sample.
				poll.Gap.RecoveryPath = ""
				poll.Gap.LastProgressAt = now
				poll.Gap.HeadProbePending = true
			}
		} else if source == pollFrontierHead && poll.Gap != nil {
			// This is the bounded head sample scheduled after an empty gap
			// response. Keep the older gap, but consume this one-shot probe so
			// the next quantum returns to the directional recovery lane.
			if window.Truncated && strings.TrimSpace(window.NextPath) != "" {
				// The head response has an older page that belongs to the open
				// gap. Preserve that opaque path as the next recovery request;
				// dropping it here would make a deduplicated head sample look
				// complete and permanently hide an actionable older message.
				poll.Gap.RecoveryPath = strings.TrimSpace(window.NextPath)
				poll.Gap.LastProgressAt = now
			}
			poll.Gap.HeadProbePending = false
		} else if window.baselineOnly {
			// Initial discovery establishes the boundary but must not walk older
			// Graph pages. This matches the pre-receipt seed semantics and keeps a
			// crash replay from converting historical chat contents into backlog.
			poll.ContinuationPath = ""
			poll.DeferredContinuationPath = ""
		} else {
			if window.Truncated {
				poll.ContinuationPath = strings.TrimSpace(window.NextPath)
				if source == pollFrontierHead && strings.TrimSpace(window.NextPath) != "" {
					// The head response may have advanced LastModifiedCursor to its
					// newest item while its older continuation is still pending. If
					// that opaque path later expires, recovery must overlap from the
					// cursor that preceded this page rather than skipping the failed
					// page's older records.
					poll.ContinuationSafeCursor = previousSafeCursor
				}
			} else {
				poll.ContinuationPath = ""
				if strings.TrimSpace(poll.DeferredContinuationPath) != "" {
					poll.ContinuationPath = strings.TrimSpace(poll.DeferredContinuationPath)
					poll.DeferredContinuationPath = ""
					poll.FrontierEpoch = normalizeFrontierEpochForPoll(*poll) + 1
				}
			}
		}
		if continuationPage && poll.ContinuationPageCount >= continuationPageBudget {
			if source == pollFrontierContinuation {
				// A long/rotating nextLink cycle can evade the bounded in-memory
				// history. Open the directional gap at the last safe cursor instead
				// of retrying the same opaque lane forever. The current page has
				// already been handled; only the unreachable continuation is held.
				openPollGap(poll, "continuation-page-budget", "continuation page budget exhausted", path, now)
			} else if poll.Gap != nil {
				// Gap recovery has its own safe cursor. Drop only the opaque
				// recovery path and let the next scheduled recovery-head sample
				// continue independently.
				poll.Gap.RecoveryPath = ""
				poll.Gap.LastProgressAt = now
				// Page-budget exhaustion has no opaque continuation that can be
				// trusted further. The next recovery action must be a bounded
				// head probe rather than another reconstruction of this gap.
				poll.Gap.HeadProbePending = true
				poll.ContinuationPageCount = 0
			}
		}
		if role == inboundPollRoleControl {
			// Control history is diagnostic input, not a work-chat backlog. Do not
			// retain its nextLink as an operational continuation.
			poll.ContinuationPath = ""
			poll.DeferredContinuationPath = ""
		}
		if strings.TrimSpace(poll.ContinuationPath) == "" && strings.TrimSpace(poll.DeferredContinuationPath) == "" {
			// Once the continuation lane is drained, the normal cursor is the
			// authoritative frontier again. A gap has already copied the safe
			// predecessor into Gap.SafeCursor before clearing this hint.
			poll.ContinuationSafeCursor = time.Time{}
			poll.ContinuationSafeCursorKnown = false
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

// clearChatPollRecoveryMarker retires only the admission marker created for a
// malformed persisted poll projection.  It is safe to clear it after a page
// has been fetched and durably committed: the canonical poll row then owns the
// frontier, while any separate pending-page/gap evidence remains intact.
func clearChatPollRecoveryMarker(poll *teamstore.ChatPollState) {
	if poll == nil {
		return
	}
	poll.RecoveryRequired = false
	poll.RecoveryReason = ""
	poll.RecoverySourceHash = ""
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
	safeCursor := poll.LastModifiedCursor
	if poll.Gap != nil {
		// An open gap owns its safe boundary, including an explicit zero value.
		// Falling back to the normal cursor here would turn a conservative gap
		// into a silent skip after a restart.
		safeCursor = poll.Gap.SafeCursor
	} else if strings.TrimSpace(poll.ContinuationPath) != "" || strings.TrimSpace(poll.DeferredContinuationPath) != "" {
		if poll.ContinuationSafeCursorKnown || !poll.ContinuationSafeCursor.IsZero() {
			safeCursor = poll.ContinuationSafeCursor
		} else {
			// Legacy rows can carry an opaque continuation without the predecessor
			// proof introduced later. The only safe lower bound is the beginning
			// of the chat; using LastModifiedCursor could skip the entire page that
			// the opaque continuation was supposed to enumerate.
			safeCursor = time.Time{}
		}
	}
	recoveryCursor := poll.LastModifiedCursor
	if recoveryCursor.IsZero() || !safeCursor.IsZero() && !recoveryCursor.After(safeCursor) {
		recoveryCursor = time.Time{}
	}
	poll.Gap = &teamstore.ChatPollGap{
		Epoch:           epoch,
		Kind:            strings.TrimSpace(kind),
		Reason:          trimPollDiagnostic(reason),
		Evidence:        trimPollDiagnostic(evidence),
		FrontierPath:    frontierPath,
		SafeCursor:      safeCursor,
		RecoveryCursor:  recoveryCursor,
		OpenedAt:        now,
		NoticeEpoch:     epoch,
		QuarantinedPage: quarantinedPage,
	}
	// The page that led to a repeated/ambiguous frontier is removed from the
	// executable lane but retained as bounded evidence under the gap. This gives
	// recovery/manual tooling the original receipt without letting a malformed
	// page starve the directional recovery head.
	poll.PendingPage = nil
	if quarantinedPage != nil {
		// A semantically malformed receipt has now been converted into bounded
		// gap evidence. It is safe for the canonical gap/frontier to replace the
		// raw row; syntax-only placeholders without a page retain their recovery
		// marker until an explicit repair supplies canonical evidence.
		clearChatPollRecoveryMarker(poll)
	}
	poll.ContinuationPath = ""
	poll.ContinuationFailureCount = 0
	poll.ContinuationFirstFailureAt = time.Time{}
	poll.ContinuationLastFailureAt = time.Time{}
	poll.ContinuationLastPath = ""
	poll.ContinuationNoProgressCount = 0
	poll.ContinuationPageCount = 0
	poll.ContinuationSafeCursor = time.Time{}
	poll.ContinuationSafeCursorKnown = false
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
	// The malformed receipt is now retained as bounded gap evidence.  The
	// canonical gap may replace the original raw row; keeping the admission
	// marker here would make the SQLite raw-preservation guard reject that
	// durable transition and would leave the same receipt at the frontier.
	clearChatPollRecoveryMarker(poll)
}

func trimPollDiagnostic(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 240 {
		return value[:240]
	}
	return value
}
