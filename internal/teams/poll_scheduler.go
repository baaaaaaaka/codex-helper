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
	// RetryFailure marks a due chat whose durable poll frontier recorded a
	// failed provider/local attempt. It is a scheduling priority only: the
	// retry deadline, owner/attempt fence, and normal poll error policy remain
	// authoritative. Keeping this bit separate lets the cycle quantum rescue a
	// retry from a large ordinary backlog without making every failed chat an
	// unbounded priority lane.
	RetryFailure bool
	NextPollAt   time.Time
	// LastSuccessfulPollAt is a durable tie-breaker for due chats. A chat
	// with a continuously operational frontier may deliberately remain due
	// immediately for catch-up, so NextPollAt alone would let the first
	// max-work-chat-polls-per-cycle entries win forever. Older successful
	// polls go first and therefore form a restart-safe aging queue.
	LastSuccessfulPollAt time.Time
	// LastErrorAt is a durable aging key for due retries. The bridge may reserve
	// one cycle slot for a healthy ordinary chat by dropping the newest retry;
	// an unchanged retry must then become the oldest retry after its siblings
	// make another attempt, otherwise a cold/failing chat can be dropped on
	// every cycle by the state-class ordering below.
	LastErrorAt    time.Time
	LastActivityAt time.Time
	BlockedUntil   time.Time
	Interval       time.Duration
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
		RetryFailure:         input.Role == inboundPollRoleWork && poll.FailureCount > 0,
		LastErrorAt:          poll.LastErrorAt,
	}
	// A normal pending page is already a durable, immutable Graph receipt.
	// Replaying it performs no Graph request, so a provider 429 recorded after
	// staging the page must not hide it behind either the provider deadline or
	// the ordinary poll schedule. Exceptional records are different: the list
	// page stores only identity/order metadata and handlePollMessageWindow must
	// refetch them from Graph before they can be classified. Keep those pages
	// behind the durable retry deadline while allowing ordinary receipts to
	// drain locally.
	localReplay := poll.PendingPage != nil && !pendingPageRequiresGraphReplay(poll.PendingPage)
	// BlockedUntil is the normal durable retry fence. A few older writers only
	// persisted the provider deadline in NextPollAt, however, so a seeded chat
	// with a 429 and a zero BlockedUntil could previously fall through to
	// ForceCatchup and issue an immediate Graph retry. Treat the stronger of the
	// two durable deadlines as the gate for every Graph-dependent path. Local
	// receipt replay remains above this check because it performs no Graph I/O.
	if retryUntil := chatPollDurableRetryDeadline(poll); !localReplay && retryUntil.After(now) && !pollPageHasOperationalFrontier(poll) {
		previous := strings.TrimSpace(poll.PreviousPollState)
		if previous == "" && poll.PollState != "" && poll.PollState != inboundPollStateBlocked {
			previous = poll.PollState
		}
		decision.State = inboundPollStateBlocked
		decision.PreviousState = previous
		decision.BlockedUntil = retryUntil
		decision.NextPollAt = retryUntil
		return decision
	}
	if !input.HasPoll || !poll.Seeded {
		// A first read can fail before the chat has ever been seeded. Keep the
		// durable retry deadline authoritative in that state too; otherwise the
		// catch-up branch would immediately retry a 429/network failure and turn
		// an isolated chat error into a tight Graph loop.
		if !localReplay && poll.FailureCount > 0 && poll.NextPollAt.After(now) {
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
	if localReplay {
		if parked {
			state = inboundPollStateCold
			interval = inboundPollColdInterval
		}
		decision.State = state
		decision.Interval = interval
		decision.NextPollAt = now
		decision.BlockedUntil = time.Time{}
		decision.Due = true
		decision.ShouldPark = false
		return decision
	}
	if retryUntil := chatPollDurableRetryDeadline(poll); !localReplay && retryUntil.After(now) && pollPageHasOperationalFrontier(poll) {
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
		decision.BlockedUntil = retryUntil
		decision.NextPollAt = retryUntil
		decision.Due = false
		decision.ShouldPark = false
		return decision
	}
	// ForceCatchup is a scheduling hint, not permission to bypass a durable
	// provider retry deadline.  Keep it below both blocked checks above so a
	// caller cannot turn an account/chat 429 into a tight retry loop.  A local
	// replay still bypasses the deadline earlier because it needs no Graph
	// request; it returned before reaching this point.
	if input.ForceCatchup {
		decision.State = inboundPollStateCatchup
		decision.Due = true
		decision.Interval = inboundPollCatchupInterval
		decision.NextPollAt = now
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

// pendingPageRequiresGraphReplay is deliberately metadata-only. The page
// builder records the exceptional dispositions before the receipt is persisted;
// inspecting those bounded strings avoids decoding large message envelopes on
// every scheduler pass. A quarantined record is terminal and therefore local;
// only records that still need an individual refetch keep the page Graph-bound.
func pendingPageRequiresGraphReplay(page *teamstore.ChatPollPendingPage) bool {
	if page == nil {
		return false
	}
	if len(page.Dispositions) != 0 && len(page.Dispositions) != len(page.Records) {
		return true
	}
	if len(page.RefetchFailures) != 0 && len(page.RefetchFailures) != len(page.Records) {
		return true
	}
	// Dispositions were added after the first pending-page writer. A legacy
	// receipt with no disposition array is still Graph-bound when its parallel
	// refetch counter proves that an exceptional record was not completed.
	if len(page.Dispositions) == 0 {
		for _, failures := range page.RefetchFailures {
			if failures > 0 {
				return true
			}
		}
		return false
	}
	for i, rawDisposition := range page.Dispositions {
		disposition := strings.TrimSpace(rawDisposition)
		switch disposition {
		case "oversized_record", "invalid_record":
			return true
		case "oversized_record_quarantined", "invalid_record_quarantined", "", "received":
			// These records are either locally replayable or already terminal.
		default:
			// An unknown disposition is not evidence that the record is local. Keep
			// the page Graph-bound until pendingPageToWindow can repair it under the
			// durable retry gate; otherwise a mixed-version receipt could bypass a
			// 429 deadline and repeatedly enter the refetch path.
			return true
		}
		if i < len(page.RefetchFailures) && page.RefetchFailures[i] > 0 && disposition != "oversized_record_quarantined" && disposition != "invalid_record_quarantined" {
			return true
		}
	}
	return false
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
	// An opaque SQLite row cannot safely rewrite its raw JSON error fields.
	// Its targeted writer still persists a blocked scalar schedule projection;
	// treating that explicit recovery gate as retryable keeps a restart from
	// issuing the same Graph read in a tight loop.
	if poll.RecoveryRequired && poll.PollState == inboundPollStateBlocked && !poll.BlockedUntil.IsZero() {
		return true
	}
	if poll.FailureCount <= 0 || strings.TrimSpace(poll.LastError) == "" {
		return false
	}
	if !isRetryableChatPollErrorMessage(poll.LastError) {
		return false
	}
	if poll.LastErrorAt.IsZero() {
		return true
	}
	return poll.LastSuccessfulPollAt.IsZero() || poll.LastErrorAt.After(poll.LastSuccessfulPollAt)
}

// chatPollDurableRetryDeadline returns the latest persisted deadline that can
// authorize a Graph request for this chat. BlockedUntil is explicit for current
// writers. Older writers could persist a provider deadline only in NextPollAt,
// so that field is used as a fallback only when the durable error is known to
// be retryable. Ordinary scheduling still uses NextPollAt unchanged.
func chatPollDurableRetryDeadline(poll teamstore.ChatPollState) time.Time {
	deadline := poll.BlockedUntil
	if poll.FailureCount > 0 && isRetryableChatPollErrorMessage(poll.LastError) && poll.NextPollAt.After(deadline) {
		deadline = poll.NextPollAt
	}
	return deadline
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
		if decisions[i].Due && decisions[i].RetryFailure != decisions[j].RetryFailure {
			// A durable retry deadline is already due. Give it a bounded priority
			// over the ordinary state class so a large fresh backlog cannot hide a
			// chat whose previous provider attempt failed. The cycle limiter below
			// still reserves an ordinary slot when both lanes are populated.
			return decisions[i].RetryFailure
		}
		if decisions[i].Due && decisions[i].RetryFailure && decisions[j].RetryFailure && !decisions[i].LastErrorAt.Equal(decisions[j].LastErrorAt) {
			// The cycle limiter may evict the newest retry to preserve one
			// ordinary slot. Once the other retries are attempted, their durable
			// error timestamps advance and the evicted row ages to the front.
			// Compare this key before state class so a cold failed chat cannot be
			// perpetually hidden behind hot retries.
			if decisions[i].LastErrorAt.IsZero() != decisions[j].LastErrorAt.IsZero() {
				return decisions[i].LastErrorAt.IsZero()
			}
			return decisions[i].LastErrorAt.Before(decisions[j].LastErrorAt)
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
