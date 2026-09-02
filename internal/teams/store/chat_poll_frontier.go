package store

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"
)

const defaultChatPollAttemptTTL = 2 * time.Minute

// UpdateChatPoll applies one small, chat-scoped mutation. SQLite stores use a
// targeted transaction instead of Store.Update's cold full-state rewrite;
// legacy JSON stores retain the existing state-file lock semantics. The
// callback must not perform I/O.
func (s *Store) UpdateChatPoll(ctx context.Context, chatID string, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	return s.updateChatPollWithCapability(ctx, chatID, nil, fn)
}

// UpdateChatPollForOwner applies a small poll-row mutation only for the
// current control-lease holder. It is used by compatibility/normalization
// paths that run before a per-chat poll attempt exists; an attempt capability
// cannot fence those writes because there is no attempt ID yet.
func (s *Store) UpdateChatPollForOwner(ctx context.Context, chatID, machineID string, leaseGeneration int64, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	if strings.TrimSpace(machineID) == "" || leaseGeneration <= 0 {
		return ChatPollState{}, false, ErrControlLeaseNotHeld
	}
	capability := &ChatPollAttemptCapability{Owner: strings.TrimSpace(machineID), LeaseGeneration: leaseGeneration}
	return s.updateChatPollWithCapability(ctx, chatID, capability, fn)
}

// UpdateChatPollScheduleForOwner applies the same schedule reducer as
// UpdateChatPollSchedule, but fences the mutation against the control lease
// captured by the caller. Schedule updates are often performed after a
// network request, so an old listener must not be able to block or park a
// replacement owner's chat.
func (s *Store) UpdateChatPollScheduleForOwner(ctx context.Context, update ChatPollScheduleUpdate, machineID string, leaseGeneration int64) (ChatPollState, error) {
	chatID := strings.TrimSpace(update.ChatID)
	if chatID == "" {
		return ChatPollState{}, fmt.Errorf("chat id is required")
	}
	var out ChatPollState
	_, _, err := s.UpdateChatPollForOwner(ctx, chatID, machineID, leaseGeneration, func(poll *ChatPollState) error {
		state := State{ChatPolls: map[string]ChatPollState{chatID: *poll}}
		next, changed, err := applyChatPollScheduleUpdateLocked(&state, update, time.Now())
		if err != nil {
			return err
		}
		out = next
		if !changed {
			return errStoreNoChange
		}
		invalidateChatPollAttempt(&next)
		*poll = next
		return nil
	})
	if out.ChatID == "" {
		out, _, _ = s.ChatPoll(ctx, chatID)
	}
	return out, err
}

// MarkChatPollParkNoticeSentForOwner records the park-notice watermark under
// the caller's lease. A delayed notice callback must not mutate a chat after
// a replacement listener has taken ownership.
func (s *Store) MarkChatPollParkNoticeSentForOwner(ctx context.Context, chatID string, at time.Time, machineID string, leaseGeneration int64) (ChatPollState, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, fmt.Errorf("chat id is required")
	}
	if at.IsZero() {
		at = time.Now()
	}
	var out ChatPollState
	_, _, err := s.UpdateChatPollForOwner(ctx, chatID, machineID, leaseGeneration, func(poll *ChatPollState) error {
		poll.ChatID = chatID
		poll.ParkNoticeSentAt = at
		poll.UpdatedAt = time.Now()
		poll.PollRevision++
		poll.ScheduleRevision++
		invalidateChatPollAttempt(poll)
		out = *poll
		return nil
	})
	if out.ChatID == "" {
		out, _, _ = s.ChatPoll(ctx, chatID)
	}
	return out, err
}

// RecordChatPollSuccessWithContinuationAndScheduleForOwner combines the
// non-attempt poll success projection and its derived schedule under the
// current control lease. It is used only by compatibility/parked paths that
// do not have a ChatPollAttempt capability; a delayed old listener must still
// be unable to advance a replacement owner's cursor.
func (s *Store) RecordChatPollSuccessWithContinuationAndScheduleForOwner(ctx context.Context, chatID string, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int, continuationPath string, schedule func(ChatPollState) (ChatPollScheduleUpdate, error), machineID string, leaseGeneration int64) (ChatPollState, error) {
	var out ChatPollState
	_, _, err := s.UpdateChatPollForOwner(ctx, chatID, machineID, leaseGeneration, func(poll *ChatPollState) error {
		state := State{ChatPolls: map[string]ChatPollState{chatID: *poll}}
		now := time.Now()
		next, changed := applyChatPollSuccessLocked(&state, chatID, lastModifiedCursor, seeded, windowFull, fetched, strings.TrimSpace(continuationPath), now)
		if schedule != nil {
			update, err := schedule(next)
			if err != nil {
				return err
			}
			update.ChatID = strings.TrimSpace(update.ChatID)
			switch {
			case update.ChatID == "":
				update.ChatID = chatID
			case update.ChatID != chatID:
				return fmt.Errorf("chat poll schedule chat id %q does not match success chat id %q", update.ChatID, chatID)
			}
			scheduled, scheduleChanged, err := applyChatPollScheduleUpdateLocked(&state, update, time.Now())
			if err != nil {
				return err
			}
			next = scheduled
			changed = changed || scheduleChanged
		}
		out = next
		if !changed {
			return errStoreNoChange
		}
		invalidateChatPollAttempt(&next)
		*poll = next
		return nil
	})
	if out.ChatID == "" {
		out, _, _ = s.ChatPoll(ctx, chatID)
	}
	return out, err
}

// RecordChatPollErrorWithBlockForOwner records a chat-scoped retry/error
// projection only for the current control-lease holder. A Graph error must
// never let a stale listener block a new owner's poll lane.
func (s *Store) RecordChatPollErrorWithBlockForOwner(ctx context.Context, chatID string, message string, blockedUntil time.Time, machineID string, leaseGeneration int64) error {
	_, _, err := s.UpdateChatPollForOwner(ctx, chatID, machineID, leaseGeneration, func(poll *ChatPollState) error {
		state := State{ChatPolls: map[string]ChatPollState{chatID: *poll}}
		next := applyChatPollErrorWithBlockLocked(&state, strings.TrimSpace(chatID), trimDiagnostic(message, 240), blockedUntil, time.Now())
		invalidateChatPollAttempt(&next)
		*poll = next
		return nil
	})
	return err
}

// updateChatPollWithCapability is the common poll-row mutation path. A live
// listener passes the control-lease capability it captured before the Graph
// request; the capability is checked inside the same JSON/SQLite mutation as
// the frontier write. Stores without a materialized control lease retain the
// legacy unbound behavior so an older state file can still be migrated.
func (s *Store) updateChatPollWithCapability(ctx context.Context, chatID string, capability *ChatPollAttemptCapability, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, false, fmt.Errorf("chat id is required")
	}
	if fn == nil {
		return ChatPollState{}, false, fmt.Errorf("chat poll mutation is required")
	}
	mutate := func(poll *ChatPollState) error {
		// ChatPollState contains pointers and slices. A shallow copy makes a
		// callback that updates Attempt/Gap/PendingPage mutate the comparison
		// snapshot as well, so the targeted writer can incorrectly report a
		// real mutation as a no-op. Keep the receipt payload immutable and copy
		// its slice headers only; this avoids copying potentially multi-megabyte
		// raw Graph records on every poll-state write while still isolating all
		// fields that the bridge is allowed to mutate.
		before := cloneChatPollStateForComparison(*poll)
		beforeRevision := poll.PollRevision
		beforeUpdatedAt := poll.UpdatedAt
		if err := fn(poll); err != nil {
			return err
		}
		if reflect.DeepEqual(before, *poll) {
			return errStoreNoChange
		}
		if poll.PollRevision == beforeRevision {
			poll.PollRevision++
		}
		if poll.UpdatedAt.Equal(beforeUpdatedAt) {
			poll.UpdatedAt = time.Now()
		}
		poll.ChatID = chatID
		return nil
	}
	if out, changed, handled, err := s.updateChatPollSQLiteWithCapability(ctx, chatID, capability, mutate); handled || err != nil {
		return out, changed, err
	}
	var out ChatPollState
	changed := false
	err := s.Update(ctx, func(state *State) error {
		if !storeOwnerCapabilityMatchesActiveLease(state, capabilityOwner(capability), capabilityLeaseGeneration(capability)) {
			out = state.ChatPolls[chatID]
			return errStoreNoChange
		}
		if state.ChatPolls == nil {
			state.ChatPolls = make(map[string]ChatPollState)
		}
		poll := state.ChatPolls[chatID]
		if chatPollAttemptNeedsActiveLeaseForReclaim(&poll, capability) &&
			!storeOwnerCapabilityMatchesMaterializedActiveLease(state, capabilityOwner(capability), capabilityLeaseGeneration(capability)) {
			out = poll
			return errStoreNoChange
		}
		if err := mutate(&poll); err != nil {
			if err == errStoreNoChange {
				out = poll
				return errStoreNoChange
			}
			return err
		}
		out = poll
		changed = true
		state.ChatPolls[chatID] = poll
		return nil
	})
	return out, changed, err
}

func cloneChatPollStateForComparison(poll ChatPollState) ChatPollState {
	out := poll
	out.PendingPage = cloneChatPollPendingPageForComparison(poll.PendingPage)
	out.Gap = cloneChatPollGapForComparison(poll.Gap)
	if poll.Attempt != nil {
		attempt := *poll.Attempt
		out.Attempt = &attempt
	}
	out.ContinuationPathHistory = append([]string(nil), poll.ContinuationPathHistory...)
	out.ContinuationPageFingerprintHistory = append([]string(nil), poll.ContinuationPageFingerprintHistory...)
	out.QuarantinedRecordIDs = append([]string(nil), poll.QuarantinedRecordIDs...)
	return out
}

func cloneChatPollGapForComparison(gap *ChatPollGap) *ChatPollGap {
	if gap == nil {
		return nil
	}
	out := *gap
	out.QuarantinedPage = cloneChatPollPendingPageForComparison(gap.QuarantinedPage)
	return &out
}

func cloneChatPollPendingPageForComparison(page *ChatPollPendingPage) *ChatPollPendingPage {
	if page == nil {
		return nil
	}
	out := *page
	// ChatPollPendingPage is an immutable transport receipt after it is
	// staged. Keep the bounded raw-record slice shared: reflect.DeepEqual can
	// compare a shared slice by identity, avoiding a multi-megabyte byte walk on
	// every targeted poll-state write. All production callbacks replace the
	// receipt or mutate only the small metadata slices below; they must not edit
	// a raw record in place.
	out.RecordIDs = append([]string(nil), page.RecordIDs...)
	out.RecordHashes = append([]string(nil), page.RecordHashes...)
	out.Dispositions = append([]string(nil), page.Dispositions...)
	out.RefetchFailures = append([]int(nil), page.RefetchFailures...)
	return &out
}

// BeginChatPollAttempt acquires the per-chat poll capability. An unexpired
// capability is never replaced by another caller in the same lease
// generation. A newer control-lease generation may reclaim an attempt left by
// an owner that disappeared during a hand-off; the generation check is the
// cross-process proof, while the exact attempt capability fences the old
// callback. Expired capabilities are safely replaced as before.
func (s *Store) BeginChatPollAttempt(ctx context.Context, req ChatPollAttemptRequest) (ChatPollState, bool, error) {
	req.ChatID = strings.TrimSpace(req.ChatID)
	if req.ChatID == "" {
		return ChatPollState{}, false, fmt.Errorf("chat id is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now()
	}
	if req.TTL <= 0 {
		req.TTL = defaultChatPollAttemptTTL
	}
	if strings.TrimSpace(req.Owner) == "" {
		req.Owner = fmt.Sprintf("pid:%d", os.Getpid())
	}
	if strings.TrimSpace(req.ProcessIncarnation) == "" {
		req.ProcessIncarnation = fmt.Sprintf("pid:%d", os.Getpid())
	}
	attemptID := fmt.Sprintf("poll-%d-%d", req.Now.UnixNano(), chatPollAttemptSequence.Add(1))
	ownerCapability := &ChatPollAttemptCapability{
		Owner:                        strings.TrimSpace(req.Owner),
		ProcessIncarnation:           strings.TrimSpace(req.ProcessIncarnation),
		LeaseGeneration:              req.LeaseGeneration,
		RequireActiveLeaseForReclaim: true,
	}
	var acquired bool
	poll, changed, err := s.updateChatPollWithCapability(ctx, req.ChatID, ownerCapability, func(poll *ChatPollState) error {
		if req.HasExpectedPollRevision && poll.PollRevision != req.ExpectedPollRevision {
			// The caller observed an older frontier. Do not acquire a capability
			// against the newer row merely because the chat still has the same
			// opaque path; the caller must re-read durable state first.
			return errStoreNoChange
		}
		if poll.Attempt != nil && poll.Attempt.ExpiresAt.After(req.Now) {
			// A listener can lose its control lease after the Graph request has
			// started. The old attempt is intentionally left durable so its late
			// callback cannot commit, but the replacement owner must not wait for
			// the full attempt TTL before making progress. A strictly newer lease
			// generation is the only authority that can reclaim it; same-generation
			// callers still observe the normal busy result.
			if req.LeaseGeneration <= 0 || poll.Attempt.LeaseGeneration >= req.LeaseGeneration {
				return errStoreNoChange
			}
		}
		poll.FrontierEpoch = normalizeFrontierEpoch(poll.FrontierEpoch)
		poll.Attempt = &ChatPollAttempt{
			ID:                       attemptID,
			Owner:                    strings.TrimSpace(req.Owner),
			ProcessIncarnation:       strings.TrimSpace(req.ProcessIncarnation),
			LeaseGeneration:          req.LeaseGeneration,
			ExpectedPollRevision:     poll.PollRevision + 1,
			ExpectedScheduleRevision: poll.ScheduleRevision,
			ExpectedFrontier:         strings.TrimSpace(req.ExpectedFrontier),
			ExpectedReceiptID:        strings.TrimSpace(req.ExpectedReceiptID),
			StartedAt:                req.Now,
			ExpiresAt:                req.Now.Add(req.TTL),
		}
		acquired = true
		return nil
	})
	if !changed {
		acquired = false
	}
	return poll, acquired, err
}

// MutateChatPollAttempt applies a poll-owned mutation while retaining the
// capability. It is used to stage the immutable page receipt before invoking
// any handler. A stale/expired caller becomes a no-op.
func (s *Store) MutateChatPollAttempt(ctx context.Context, chatID, attemptID string, expectedRevision uint64, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	return s.mutateChatPollAttempt(ctx, chatID, attemptID, expectedRevision, nil, fn)
}

// MutateChatPollAttemptWithCapability is the owner-fenced form used by the
// live bridge. A stale callback is intentionally reported as applied=false;
// it must not write a new error or retry schedule after a takeover.
func (s *Store) MutateChatPollAttemptWithCapability(ctx context.Context, chatID string, capability ChatPollAttemptCapability, expectedRevision uint64, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	return s.mutateChatPollAttempt(ctx, chatID, capability.ID, expectedRevision, &capability, fn)
}

func (s *Store) mutateChatPollAttempt(ctx context.Context, chatID, attemptID string, expectedRevision uint64, capability *ChatPollAttemptCapability, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	attemptID = strings.TrimSpace(attemptID)
	if chatID == "" || attemptID == "" || fn == nil {
		return ChatPollState{}, false, nil
	}
	var applied bool
	poll, _, err := s.updateChatPollWithCapability(ctx, chatID, capability, func(poll *ChatPollState) error {
		if !chatPollAttemptMatchesCapability(poll, attemptID, expectedRevision, capability, time.Now()) {
			return errStoreNoChange
		}
		if err := fn(poll); err != nil {
			return err
		}
		poll.PollRevision++
		poll.Attempt.ExpectedPollRevision = poll.PollRevision
		applied = true
		return nil
	})
	return poll, applied, err
}

// CommitChatPollAttempt executes the final poll mutation only if the exact
// capability and revision are still current. It returns committed=false for a
// stale result; callers must not write an error, retry schedule, cursor, or
// notice after that point.
func (s *Store) CommitChatPollAttempt(ctx context.Context, chatID, attemptID string, expectedRevision uint64, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	return s.commitChatPollAttempt(ctx, chatID, attemptID, expectedRevision, nil, fn)
}

// CommitChatPollAttemptWithCapability is the owner-fenced final CAS used by
// the live bridge. It prevents a result produced by an old process incarnation
// from committing after the durable capability has been replaced.
func (s *Store) CommitChatPollAttemptWithCapability(ctx context.Context, chatID string, capability ChatPollAttemptCapability, expectedRevision uint64, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	return s.commitChatPollAttempt(ctx, chatID, capability.ID, expectedRevision, &capability, fn)
}

func (s *Store) commitChatPollAttempt(ctx context.Context, chatID, attemptID string, expectedRevision uint64, capability *ChatPollAttemptCapability, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	attemptID = strings.TrimSpace(attemptID)
	if chatID == "" || attemptID == "" || fn == nil {
		return ChatPollState{}, false, nil
	}
	var committed bool
	poll, _, err := s.updateChatPollWithCapability(ctx, chatID, capability, func(poll *ChatPollState) error {
		if !chatPollAttemptMatchesCapability(poll, attemptID, expectedRevision, capability, time.Now()) {
			return errStoreNoChange
		}
		if err := fn(poll); err != nil {
			return err
		}
		poll.Attempt = nil
		committed = true
		return nil
	})
	return poll, committed, err
}

// AbandonChatPollAttempt releases only the in-flight capability after the
// process loses its control lease. It deliberately leaves PendingPage and all
// data/frontier fields untouched so the next owner can replay the staged page
// without issuing another Graph request. A stale or expired caller is a
// no-op, just like CommitChatPollAttempt.
func (s *Store) AbandonChatPollAttempt(ctx context.Context, chatID, attemptID string, expectedRevision uint64) (ChatPollState, bool, error) {
	return s.abandonChatPollAttempt(ctx, chatID, attemptID, expectedRevision, nil)
}

// AbandonChatPollAttemptWithCapability releases a poll attempt only for the
// owner that acquired it. This keeps late lease-loss cleanup from clearing a
// replacement owner's capability.
func (s *Store) AbandonChatPollAttemptWithCapability(ctx context.Context, chatID string, capability ChatPollAttemptCapability, expectedRevision uint64) (ChatPollState, bool, error) {
	return s.abandonChatPollAttempt(ctx, chatID, capability.ID, expectedRevision, &capability)
}

func (s *Store) abandonChatPollAttempt(ctx context.Context, chatID, attemptID string, expectedRevision uint64, capability *ChatPollAttemptCapability) (ChatPollState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	attemptID = strings.TrimSpace(attemptID)
	if chatID == "" || attemptID == "" {
		return ChatPollState{}, false, nil
	}
	var abandoned bool
	poll, _, err := s.updateChatPollWithCapability(ctx, chatID, capability, func(poll *ChatPollState) error {
		if !chatPollAttemptMatchesCapability(poll, attemptID, expectedRevision, capability, time.Now()) {
			return errStoreNoChange
		}
		poll.Attempt = nil
		abandoned = true
		return nil
	})
	return poll, abandoned, err
}

func chatPollAttemptMatches(poll *ChatPollState, attemptID string, expectedRevision uint64, now time.Time) bool {
	return chatPollAttemptMatchesCapability(poll, attemptID, expectedRevision, nil, now)
}

func chatPollAttemptMatchesCapability(poll *ChatPollState, attemptID string, expectedRevision uint64, capability *ChatPollAttemptCapability, now time.Time) bool {
	if poll == nil || poll.Attempt == nil || strings.TrimSpace(poll.Attempt.ID) != attemptID {
		return false
	}
	// A zero revision is never a valid capability. Treating it as a wildcard
	// would let a caller that lost its CAS token mutate a newly acquired
	// attempt merely because it still knows the attempt ID.
	if expectedRevision == 0 {
		return false
	}
	if !poll.Attempt.ExpiresAt.After(now) {
		return false
	}
	if poll.PollRevision != expectedRevision {
		return false
	}
	if poll.Attempt.ExpectedPollRevision != expectedRevision {
		// Lifecycle/scheduler mutations may advance the row revision while
		// deliberately leaving the old capability object in place until its
		// expiry. The old owner must remain stale even if it later learns the
		// newer row revision.
		return false
	}
	if capability != nil {
		if strings.TrimSpace(capability.ID) == "" || strings.TrimSpace(capability.ID) != strings.TrimSpace(poll.Attempt.ID) ||
			strings.TrimSpace(capability.Owner) != strings.TrimSpace(poll.Attempt.Owner) ||
			strings.TrimSpace(capability.ProcessIncarnation) != strings.TrimSpace(poll.Attempt.ProcessIncarnation) ||
			capability.LeaseGeneration != poll.Attempt.LeaseGeneration {
			return false
		}
	}
	return true
}

func capabilityOwner(capability *ChatPollAttemptCapability) string {
	if capability == nil {
		return ""
	}
	return capability.Owner
}

func capabilityLeaseGeneration(capability *ChatPollAttemptCapability) int64 {
	if capability == nil {
		return 0
	}
	return capability.LeaseGeneration
}

func chatPollAttemptNeedsActiveLeaseForReclaim(poll *ChatPollState, capability *ChatPollAttemptCapability) bool {
	if poll == nil || poll.Attempt == nil || capability == nil || !capability.RequireActiveLeaseForReclaim ||
		capability.LeaseGeneration <= 0 || poll.Attempt.LeaseGeneration >= capability.LeaseGeneration {
		return false
	}
	return poll.Attempt.ExpiresAt.After(time.Now())
}

// storeOwnerCapabilityMatchesMaterializedActiveLease is deliberately stricter
// than storeOwnerCapabilityMatchesActiveLease. The latter preserves legacy
// no-lease compatibility for ordinary operations; reclaiming an unexpired
// attempt is a takeover operation and must have a real current lease as proof.
func storeOwnerCapabilityMatchesMaterializedActiveLease(state *State, machineID string, leaseGeneration int64) bool {
	if state == nil || leaseGeneration <= 0 || strings.TrimSpace(machineID) == "" {
		return false
	}
	lease := state.ControlLease
	if lease.Generation <= 0 || strings.TrimSpace(lease.HolderMachineID) == "" || lease.LeaseUntil.IsZero() {
		return false
	}
	return controlLeaseMatchesCapabilityAt(state, machineID, leaseGeneration, time.Now())
}

// invalidateChatPollAttempt retires a capability owned by a non-poll writer.
// The writer already advanced the row revision, so leaving the old capability
// around would make the next poll wait for its TTL even though its page is
// still safely replayable.
func invalidateChatPollAttempt(poll *ChatPollState) {
	if poll != nil {
		poll.Attempt = nil
	}
}

func normalizeFrontierEpoch(epoch uint64) uint64 {
	if epoch == 0 {
		return 1
	}
	return epoch
}
