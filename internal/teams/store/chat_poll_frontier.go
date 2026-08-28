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
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatPollState{}, false, fmt.Errorf("chat id is required")
	}
	if fn == nil {
		return ChatPollState{}, false, fmt.Errorf("chat poll mutation is required")
	}
	mutate := func(poll *ChatPollState) error {
		before := *poll
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
	if out, changed, handled, err := s.updateChatPollSQLite(ctx, chatID, mutate); handled || err != nil {
		return out, changed, err
	}
	var out ChatPollState
	changed := false
	err := s.Update(ctx, func(state *State) error {
		if state.ChatPolls == nil {
			state.ChatPolls = make(map[string]ChatPollState)
		}
		poll := state.ChatPolls[chatID]
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

// BeginChatPollAttempt acquires the per-chat poll capability. An unexpired
// capability is never replaced, including when both callers are goroutines in
// the same process. Expired capabilities are safely replaced with a new
// process-incarnation/attempt identity.
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
	var acquired bool
	poll, _, err := s.UpdateChatPoll(ctx, req.ChatID, func(poll *ChatPollState) error {
		if req.HasExpectedPollRevision && poll.PollRevision != req.ExpectedPollRevision {
			// The caller observed an older frontier. Do not acquire a capability
			// against the newer row merely because the chat still has the same
			// opaque path; the caller must re-read durable state first.
			return errStoreNoChange
		}
		if poll.Attempt != nil && poll.Attempt.ExpiresAt.After(req.Now) {
			return errStoreNoChange
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
	return poll, acquired, err
}

// MutateChatPollAttempt applies a poll-owned mutation while retaining the
// capability. It is used to stage the immutable page receipt before invoking
// any handler. A stale/expired caller becomes a no-op.
func (s *Store) MutateChatPollAttempt(ctx context.Context, chatID, attemptID string, expectedRevision uint64, fn func(*ChatPollState) error) (ChatPollState, bool, error) {
	chatID = strings.TrimSpace(chatID)
	attemptID = strings.TrimSpace(attemptID)
	if chatID == "" || attemptID == "" || fn == nil {
		return ChatPollState{}, false, nil
	}
	var applied bool
	poll, _, err := s.UpdateChatPoll(ctx, chatID, func(poll *ChatPollState) error {
		if !chatPollAttemptMatches(poll, attemptID, expectedRevision, time.Now()) {
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
	chatID = strings.TrimSpace(chatID)
	attemptID = strings.TrimSpace(attemptID)
	if chatID == "" || attemptID == "" || fn == nil {
		return ChatPollState{}, false, nil
	}
	var committed bool
	poll, _, err := s.UpdateChatPoll(ctx, chatID, func(poll *ChatPollState) error {
		if !chatPollAttemptMatches(poll, attemptID, expectedRevision, time.Now()) {
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
	chatID = strings.TrimSpace(chatID)
	attemptID = strings.TrimSpace(attemptID)
	if chatID == "" || attemptID == "" {
		return ChatPollState{}, false, nil
	}
	var abandoned bool
	poll, _, err := s.UpdateChatPoll(ctx, chatID, func(poll *ChatPollState) error {
		if !chatPollAttemptMatches(poll, attemptID, expectedRevision, time.Now()) {
			return errStoreNoChange
		}
		poll.Attempt = nil
		abandoned = true
		return nil
	})
	return poll, abandoned, err
}

func chatPollAttemptMatches(poll *ChatPollState, attemptID string, expectedRevision uint64, now time.Time) bool {
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
	return true
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
