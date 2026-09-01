package teams

import (
	"sort"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	inboundPollStateHot     = "hot"
	inboundPollStateRunning = "running"
	inboundPollStateWarm    = "warm"
	inboundPollStateCool    = "cool"
	inboundPollStateCold    = "cold"
	inboundPollStateParked  = "parked"
	inboundPollStateCatchup = "catchup"
	inboundPollStateBlocked = "blocked"

	inboundPollHotInterval       = time.Second
	inboundPollRunningInterval   = 3 * time.Second
	inboundPollWarmInterval      = 5 * time.Second
	inboundPollCoolInterval      = 10 * time.Second
	inboundPollColdInterval      = 30 * time.Second
	inboundPollControlInterval   = 5 * time.Second
	inboundPollCatchupInterval   = 10 * time.Second
	inboundPollParkProbeInterval = 10 * time.Minute

	inboundPollHotWindow  = 2 * time.Minute
	inboundPollWarmWindow = 15 * time.Minute
	inboundPollCoolWindow = 4 * time.Hour
	inboundPollParkAfter  = 48 * time.Hour

	maxWorkChatPollsPerCycle = 8
	// A single slow Graph chat must not hold every other due chat behind it,
	// while the bound keeps request pressure and store contention predictable.
	maxConcurrentWorkChatPolls = 4
)

// Keep each Graph read bounded independently from the listener context. The
// listener must be able to continue with other chats when a Graph connection
// stops making progress, while the caller still controls the overall poll
// cycle lifetime.
var inboundPollGraphTimeout = 10 * time.Second

const DefaultMaxWorkChatPollsPerCycle = maxWorkChatPollsPerCycle

type inboundPollRole string

const (
	inboundPollRoleControl inboundPollRole = "control"
	inboundPollRoleWork    inboundPollRole = "work"
)

type inboundPollInput struct {
	ChatID           string
	Role             inboundPollRole
	Poll             teamstore.ChatPollState
	HasPoll          bool
	Running          bool
	SessionUpdatedAt time.Time
	ForceActivityAt  time.Time
	ForceCatchup     bool
	Now              time.Time
}

type inboundPollDecision struct {
	ChatID        string
	State         string
	PreviousState string
	Due           bool
	NextPollAt    time.Time
	// LastSuccessfulPollAt is a durable tie-breaker for due chats. A chat
	// with a continuously operational frontier may deliberately remain due
	// immediately for catch-up, so NextPollAt alone would let the first
	// max-work-chat-polls-per-cycle entries win forever. Older successful
	// polls go first and therefore form a restart-safe aging queue.
	LastSuccessfulPollAt time.Time
	LastActivityAt       time.Time
	BlockedUntil         time.Time
	Interval             time.Duration
	// OperationalFrontier marks a due chat whose next action is already a
	// durable continuation, pending page, or recovery gap. Keep it on the
	// decision so the cycle cap can reserve a slot for an ordinary chat after
	// sorting; the SQLite admission lane alone is insufficient once the bridge
	// applies its smaller per-cycle limit.
	OperationalFrontier bool
	ShouldPark          bool
	ShouldNotifyPark    bool
	ParkedProbe         bool
}

func decideInboundPoll(input inboundPollInput) inboundPollDecision {
	now := input.Now
	if now.IsZero() {
		now = time.Now()
	}
	poll := input.Poll
	// Once a poll row has a durable activity timestamp, it is authoritative.
	// Session UpdatedAt also changes for metadata (title/model/binding) and must
	// not accidentally wake an old parked/backlogged chat. Use it only to seed
	// an uninitialized poll row, then apply explicit forced activity separately.
	lastActivity := poll.LastActivityAt
	if lastActivity.IsZero() && poll.LastSuccessfulPollAt.IsZero() {
		lastActivity = input.SessionUpdatedAt
	}
	lastActivity = latestTime(lastActivity, input.ForceActivityAt)
	decision := inboundPollDecision{
		ChatID:               strings.TrimSpace(input.ChatID),
		LastSuccessfulPollAt: poll.LastSuccessfulPollAt,
		LastActivityAt:       lastActivity,
		OperationalFrontier:  input.Role == inboundPollRoleWork && pollPageHasOperationalFrontier(poll),
	}
	if poll.BlockedUntil.After(now) && !pollPageHasOperationalFrontier(poll) {
		previous := strings.TrimSpace(poll.PreviousPollState)
		if previous == "" && poll.PollState != "" && poll.PollState != inboundPollStateBlocked {
			previous = poll.PollState
		}
		decision.State = inboundPollStateBlocked
		decision.PreviousState = previous
		decision.BlockedUntil = poll.BlockedUntil
		decision.NextPollAt = poll.BlockedUntil
		return decision
	}
	if input.ForceCatchup {
		decision.State = inboundPollStateCatchup
		decision.Due = true
		decision.Interval = inboundPollCatchupInterval
		decision.NextPollAt = now
		return decision
	}
	if !input.HasPoll || !poll.Seeded {
		// A first read can fail before the chat has ever been seeded. Keep the
		// durable retry deadline authoritative in that state too; otherwise the
		// catch-up branch would immediately retry a 429/network failure and turn
		// an isolated chat error into a tight Graph loop.
		if poll.FailureCount > 0 && poll.NextPollAt.After(now) {
			state := strings.TrimSpace(poll.PollState)
			if state == "" || state == inboundPollStateBlocked {
				state = inboundPollStateWarm
			}
			decision.State = state
			decision.Due = false
			decision.NextPollAt = poll.NextPollAt
			return decision
		}
		decision.State = inboundPollStateCatchup
		decision.Due = true
		decision.Interval = inboundPollCatchupInterval
		decision.NextPollAt = now
		return decision
	}
	state, interval, parked := classifyInboundPollState(input.Role, input.Running, lastActivity, now)
	if poll.BlockedUntil.After(now) && pollPageHasOperationalFrontier(poll) {
		// An operational frontier must remain visible to the scheduler, but a
		// transient retry deadline still applies. Keep this as ordinary due
		// scheduling state rather than exposing a semantic chat block or issuing
		// a request before Retry-After/backoff expires.
		if parked {
			state = inboundPollStateCold
			interval = inboundPollColdInterval
		}
		decision.State = state
		decision.Interval = interval
		decision.BlockedUntil = poll.BlockedUntil
		decision.NextPollAt = poll.BlockedUntil
		decision.Due = false
		decision.ShouldPark = false
		return decision
	}
	if parked && input.Role == inboundPollRoleWork && (chatPollHasUnrecoveredRetryableError(poll) || pollPageHasOperationalFrontier(poll)) {
		state = inboundPollStateCold
		interval = inboundPollColdInterval
		parked = false
	}
	if input.Role == inboundPollRoleWork && strings.TrimSpace(poll.PollState) == inboundPollStateParked && !poll.ParkNoticeSentAt.IsZero() && !chatPollHasUnrecoveredRetryableError(poll) && !input.ForceActivityAt.After(poll.LastActivityAt) {
		if pollPageHasOperationalFrontier(poll) {
			decision.State = inboundPollStateCold
			decision.Interval = inboundPollColdInterval
			decision.Due = true
			decision.NextPollAt = now
			return decision
		}
		decision.State = inboundPollStateParked
		decision.Interval = inboundPollParkProbeInterval
		decision.ParkedProbe = true
		decision.NextPollAt = poll.NextPollAt
		if decision.NextPollAt.IsZero() {
			decision.Due = true
			decision.NextPollAt = now
		} else {
			decision.Due = !now.Before(decision.NextPollAt)
		}
		return decision
	}
	decision.State = state
	decision.Interval = interval
	decision.ShouldPark = parked
	if parked {
		decision.ShouldNotifyPark = poll.ParkNoticeSentAt.IsZero()
		decision.Interval = inboundPollParkProbeInterval
		decision.NextPollAt = now.Add(decision.Interval)
		return decision
	}
	next := poll.NextPollAt
	if next.IsZero() {
		decision.Due = true
		decision.NextPollAt = now
		return decision
	}
	decision.NextPollAt = next
	decision.Due = !now.Before(next)
	return decision
}

func classifyInboundPollState(role inboundPollRole, running bool, lastActivity time.Time, now time.Time) (string, time.Duration, bool) {
	if role == inboundPollRoleControl {
		if !lastActivity.IsZero() && now.Sub(lastActivity) < inboundPollHotWindow {
			return inboundPollStateHot, inboundPollHotInterval, false
		}
		return inboundPollStateWarm, inboundPollControlInterval, false
	}
	if running {
		return inboundPollStateRunning, inboundPollRunningInterval, false
	}
	if lastActivity.IsZero() {
		return inboundPollStateWarm, inboundPollWarmInterval, false
	}
	idle := now.Sub(lastActivity)
	switch {
	case idle < inboundPollHotWindow:
		return inboundPollStateHot, inboundPollHotInterval, false
	case idle < inboundPollWarmWindow:
		return inboundPollStateWarm, inboundPollWarmInterval, false
	case idle < inboundPollCoolWindow:
		return inboundPollStateCool, inboundPollCoolInterval, false
	case idle < inboundPollParkAfter:
		return inboundPollStateCold, inboundPollColdInterval, false
	default:
		return inboundPollStateParked, 0, true
	}
}

func chatPollHasUnrecoveredRetryableError(poll teamstore.ChatPollState) bool {
	if poll.FailureCount <= 0 || strings.TrimSpace(poll.LastError) == "" || poll.LastErrorAt.IsZero() {
		return false
	}
	if !isRetryableChatPollErrorMessage(poll.LastError) {
		return false
	}
	return poll.LastSuccessfulPollAt.IsZero() || poll.LastErrorAt.After(poll.LastSuccessfulPollAt)
}

func isRetryableChatPollErrorMessage(message string) bool {
	lower := strings.ToLower(strings.TrimSpace(message))
	if lower == "" {
		return false
	}
	for _, token := range []string{
		"temporarily failed",
		"bad gateway",
		"gateway timeout",
		"service unavailable",
		"too many requests",
		"internal server error",
		"http 429",
		"http 500",
		"http 502",
		"http 503",
		"http 504",
		"timeout",
		"connection refused",
		"connection reset",
		"network is unreachable",
		"no such host",
		"proxyconnect",
		"tls handshake timeout",
		"unexpected eof",
	} {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func nextInboundPollAt(now time.Time, interval time.Duration) time.Time {
	if now.IsZero() {
		now = time.Now()
	}
	if interval <= 0 {
		return time.Time{}
	}
	return now.Add(interval)
}

func sortInboundPollDecisions(decisions []inboundPollDecision) {
	sort.SliceStable(decisions, func(i, j int) bool {
		if decisions[i].Due != decisions[j].Due {
			return decisions[i].Due
		}
		if decisions[i].NextPollAt.IsZero() != decisions[j].NextPollAt.IsZero() {
			return decisions[i].NextPollAt.IsZero()
		}
		if inboundPollSortPriority(decisions[i].State) != inboundPollSortPriority(decisions[j].State) {
			return inboundPollSortPriority(decisions[i].State) < inboundPollSortPriority(decisions[j].State)
		}
		if decisions[i].Due && decisions[j].Due && !decisions[i].LastSuccessfulPollAt.Equal(decisions[j].LastSuccessfulPollAt) {
			// A continuously due catch-up frontier rewrites NextPollAt to the
			// current instant after every page. Comparing that timestamp first
			// would make the chat completed earliest win forever due to tiny clock
			// differences. For the same priority lane, durable service age is the
			// fair ordering key; NextPollAt remains the tie-breaker for ordinary
			// scheduled work.
			if decisions[i].LastSuccessfulPollAt.IsZero() != decisions[j].LastSuccessfulPollAt.IsZero() {
				return decisions[i].LastSuccessfulPollAt.IsZero()
			}
			return decisions[i].LastSuccessfulPollAt.Before(decisions[j].LastSuccessfulPollAt)
		}
		if !decisions[i].NextPollAt.Equal(decisions[j].NextPollAt) {
			return decisions[i].NextPollAt.Before(decisions[j].NextPollAt)
		}
		if !decisions[i].LastSuccessfulPollAt.Equal(decisions[j].LastSuccessfulPollAt) {
			// Zero means never successfully served, so it is the oldest possible
			// dispatch position. This gives newly discovered chats a fair first
			// opportunity without requiring a separate durable round-robin cursor.
			if decisions[i].LastSuccessfulPollAt.IsZero() != decisions[j].LastSuccessfulPollAt.IsZero() {
				return decisions[i].LastSuccessfulPollAt.IsZero()
			}
			return decisions[i].LastSuccessfulPollAt.Before(decisions[j].LastSuccessfulPollAt)
		}
		return decisions[i].ChatID < decisions[j].ChatID
	})
}

func inboundPollSortPriority(state string) int {
	switch state {
	case inboundPollStateRunning:
		return 0
	case inboundPollStateHot:
		return 1
	case inboundPollStateCatchup:
		return 2
	case inboundPollStateWarm:
		return 3
	case inboundPollStateCool:
		return 4
	case inboundPollStateCold:
		return 5
	default:
		return 6
	}
}

func latestTime(values ...time.Time) time.Time {
	var latest time.Time
	for _, value := range values {
		if value.After(latest) {
			latest = value
		}
	}
	return latest
}
