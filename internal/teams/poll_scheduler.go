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

	inboundPollHotInterval     = time.Second
	inboundPollRunningInterval = 3 * time.Second
	inboundPollWarmInterval    = 5 * time.Second
	inboundPollCoolInterval    = 10 * time.Second
	inboundPollColdInterval    = 30 * time.Second
	inboundPollControlInterval = 5 * time.Second
	inboundPollCatchupInterval = 10 * time.Second

	inboundPollHotWindow  = 2 * time.Minute
	inboundPollWarmWindow = 15 * time.Minute
	inboundPollCoolWindow = 4 * time.Hour
	inboundPollParkAfter  = 48 * time.Hour

	maxWorkChatPollsPerCycle = 8
)

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
	ChatID           string
	State            string
	PreviousState    string
	Due              bool
	NextPollAt       time.Time
	LastActivityAt   time.Time
	BlockedUntil     time.Time
	Interval         time.Duration
	ShouldPark       bool
	ShouldNotifyPark bool
}

func decideInboundPoll(input inboundPollInput) inboundPollDecision {
	now := input.Now
	if now.IsZero() {
		now = time.Now()
	}
	poll := input.Poll
	lastActivity := latestTime(poll.LastActivityAt, input.SessionUpdatedAt, input.ForceActivityAt)
	decision := inboundPollDecision{
		ChatID:         strings.TrimSpace(input.ChatID),
		LastActivityAt: lastActivity,
	}
	if poll.BlockedUntil.After(now) {
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
	if strings.TrimSpace(poll.ContinuationPath) != "" {
		decision.State = inboundPollStateCatchup
		decision.Interval = inboundPollCatchupInterval
		if poll.NextPollAt.IsZero() {
			decision.Due = true
			decision.NextPollAt = now
			return decision
		}
		decision.NextPollAt = poll.NextPollAt
		decision.Due = !now.Before(poll.NextPollAt)
		return decision
	}
	if input.ForceCatchup || !input.HasPoll || !poll.Seeded {
		decision.State = inboundPollStateCatchup
		decision.Due = true
		decision.Interval = inboundPollCatchupInterval
		decision.NextPollAt = now
		return decision
	}
	state, interval, parked := classifyInboundPollState(input.Role, input.Running, lastActivity, now)
	if parked && input.Role == inboundPollRoleWork && chatPollHasUnrecoveredRetryableError(poll) {
		state = inboundPollStateCold
		interval = inboundPollColdInterval
		parked = false
	}
	decision.State = state
	decision.Interval = interval
	decision.ShouldPark = parked
	if parked {
		decision.ShouldNotifyPark = poll.ParkNoticeSentAt.IsZero()
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
		if !decisions[i].NextPollAt.Equal(decisions[j].NextPollAt) {
			return decisions[i].NextPollAt.Before(decisions[j].NextPollAt)
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
