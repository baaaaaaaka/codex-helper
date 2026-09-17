package teams

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestInboundPollDecisionThresholds(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		idle     time.Duration
		running  bool
		want     string
		interval time.Duration
		parked   bool
	}{
		{name: "hot", idle: time.Minute, want: inboundPollStateHot, interval: inboundPollHotInterval},
		{name: "hot just before boundary", idle: 2*time.Minute - time.Nanosecond, want: inboundPollStateHot, interval: inboundPollHotInterval},
		{name: "warm at hot boundary", idle: 2 * time.Minute, want: inboundPollStateWarm, interval: inboundPollWarmInterval},
		{name: "warm just after hot boundary", idle: 2*time.Minute + time.Nanosecond, want: inboundPollStateWarm, interval: inboundPollWarmInterval},
		{name: "warm", idle: 10 * time.Minute, want: inboundPollStateWarm, interval: inboundPollWarmInterval},
		{name: "warm just before cool boundary", idle: 15*time.Minute - time.Nanosecond, want: inboundPollStateWarm, interval: inboundPollWarmInterval},
		{name: "cool at warm boundary", idle: 15 * time.Minute, want: inboundPollStateCool, interval: inboundPollCoolInterval},
		{name: "cool", idle: time.Hour, want: inboundPollStateCool, interval: inboundPollCoolInterval},
		{name: "cool below extended threshold", idle: 3 * time.Hour, want: inboundPollStateCool, interval: inboundPollCoolInterval},
		{name: "cool just before cold boundary", idle: 4*time.Hour - time.Nanosecond, want: inboundPollStateCool, interval: inboundPollCoolInterval},
		{name: "cold at cool boundary", idle: 4 * time.Hour, want: inboundPollStateCold, interval: inboundPollColdInterval},
		{name: "cold", idle: 5 * time.Hour, want: inboundPollStateCold, interval: inboundPollColdInterval},
		{name: "cold just before park boundary", idle: 48*time.Hour - time.Nanosecond, want: inboundPollStateCold, interval: inboundPollColdInterval},
		{name: "parked at boundary", idle: 48 * time.Hour, want: inboundPollStateParked, interval: inboundPollParkProbeInterval, parked: true},
		{name: "parked", idle: 49 * time.Hour, want: inboundPollStateParked, interval: inboundPollParkProbeInterval, parked: true},
		{name: "running overrides idle", idle: 49 * time.Hour, running: true, want: inboundPollStateRunning, interval: inboundPollRunningInterval},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision := decideInboundPoll(inboundPollInput{
				ChatID:  "chat-1",
				Role:    inboundPollRoleWork,
				HasPoll: true,
				Poll: teamstore.ChatPollState{
					ChatID:         "chat-1",
					Seeded:         true,
					LastActivityAt: now.Add(-tc.idle),
					NextPollAt:     now,
				},
				Running: tc.running,
				Now:     now,
			})
			if decision.State != tc.want || decision.Interval != tc.interval || decision.ShouldPark != tc.parked {
				t.Fatalf("decision = %#v, want state=%s interval=%v parked=%v", decision, tc.want, tc.interval, tc.parked)
			}
		})
	}
}

func TestInboundPollDecisionFutureActivityStaysHot(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:         "chat-1",
			Seeded:         true,
			LastActivityAt: now.Add(5 * time.Minute),
			NextPollAt:     now,
		},
		Now: now,
	})
	if decision.State != inboundPollStateHot || decision.Interval != inboundPollHotInterval || decision.ShouldPark {
		t.Fatalf("future activity decision = %#v, want hot non-parked", decision)
	}
}

func TestInboundPollUnrecoveredErrorPreventsWorkChatParking(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:               "chat-1",
			Seeded:               true,
			PollState:            inboundPollStateParked,
			LastActivityAt:       now.Add(-72 * time.Hour),
			NextPollAt:           now,
			LastSuccessfulPollAt: now.Add(-71 * time.Hour),
			LastError:            "Teams message read token refresh failed: Bad Gateway",
			LastErrorAt:          now.Add(-time.Hour),
			FailureCount:         1731,
			ParkedAt:             now.Add(-30 * time.Minute),
			ParkNoticeSentAt:     now.Add(-30 * time.Minute),
			LastModifiedCursor:   now.Add(-71 * time.Hour),
		},
		Now: now,
	})
	if decision.ShouldPark || decision.State != inboundPollStateCold || decision.Interval != inboundPollColdInterval {
		t.Fatalf("unrecovered poll error should keep work chat cold instead of parked: %#v", decision)
	}
	if !decision.Due || !decision.NextPollAt.Equal(now) {
		t.Fatalf("unrecovered poll error should be due at existing cold schedule: %#v", decision)
	}
}

func TestInboundPollRecoveredErrorCanParkWorkChat(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:               "chat-1",
			Seeded:               true,
			LastActivityAt:       now.Add(-72 * time.Hour),
			LastError:            "old temporary auth error",
			LastErrorAt:          now.Add(-2 * time.Hour),
			LastSuccessfulPollAt: now.Add(-time.Hour),
			FailureCount:         1,
			NextPollAt:           now,
		},
		Now: now,
	})
	if !decision.ShouldPark || decision.State != inboundPollStateParked {
		t.Fatalf("recovered old poll error should not keep an idle work chat polling: %#v", decision)
	}
}

func TestInboundPollUnrecoveredPermanentErrorCanParkWorkChat(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:               "chat-1",
			Seeded:               true,
			LastActivityAt:       now.Add(-72 * time.Hour),
			LastSuccessfulPollAt: now.Add(-71 * time.Hour),
			LastError:            "Teams message read token refresh failed: invalid_grant",
			LastErrorAt:          now.Add(-time.Hour),
			FailureCount:         10,
			NextPollAt:           now,
		},
		Now: now,
	})
	if !decision.ShouldPark || decision.State != inboundPollStateParked {
		t.Fatalf("unrecovered permanent poll error should still allow idle parking: %#v", decision)
	}
}

func TestSortInboundPollDecisionsPrioritizesRunningUnderCycleCap(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decisions := []inboundPollDecision{
		{ChatID: "chat-01", State: inboundPollStateWarm, Due: true, NextPollAt: now},
		{ChatID: "chat-99", State: inboundPollStateRunning, Due: true, NextPollAt: now},
		{ChatID: "chat-02", State: inboundPollStateHot, Due: true, NextPollAt: now},
	}
	sortInboundPollDecisions(decisions)
	got := []string{decisions[0].ChatID, decisions[1].ChatID, decisions[2].ChatID}
	want := []string{"chat-99", "chat-02", "chat-01"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sorted decisions = %#v, want %#v", got, want)
		}
	}
}

func TestInboundPollContinuationDoesNotForceCatchup(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:           "chat-1",
			Seeded:           true,
			ContinuationPath: "/chats/chat-1/messages?$skiptoken=next",
			LastActivityAt:   now.Add(-5 * time.Hour),
			NextPollAt:       now.Add(time.Hour),
		},
		Now: now,
	})
	if decision.Due || decision.State != inboundPollStateCold || decision.Interval != inboundPollColdInterval || !decision.NextPollAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("cold continuation decision = %#v, want cold at existing schedule", decision)
	}
}

func TestInboundPollContinuationDoesNotRegressActiveIntervals(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		idle     time.Duration
		running  bool
		want     string
		interval time.Duration
	}{
		{name: "hot", idle: time.Minute, want: inboundPollStateHot, interval: inboundPollHotInterval},
		{name: "warm", idle: 10 * time.Minute, want: inboundPollStateWarm, interval: inboundPollWarmInterval},
		{name: "cool", idle: time.Hour, want: inboundPollStateCool, interval: inboundPollCoolInterval},
		{name: "running", idle: 49 * time.Hour, running: true, want: inboundPollStateRunning, interval: inboundPollRunningInterval},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision := decideInboundPoll(inboundPollInput{
				ChatID:  "chat-1",
				Role:    inboundPollRoleWork,
				HasPoll: true,
				Poll: teamstore.ChatPollState{
					ChatID:           "chat-1",
					Seeded:           true,
					ContinuationPath: "/chats/chat-1/messages?$skiptoken=next",
					LastActivityAt:   now.Add(-tc.idle),
					NextPollAt:       now.Add(time.Hour),
				},
				Running: tc.running,
				Now:     now,
			})
			if decision.State != tc.want || decision.Interval != tc.interval || decision.Due || !decision.NextPollAt.Equal(now.Add(time.Hour)) {
				t.Fatalf("decision = %#v, want state=%s interval=%v at existing schedule", decision, tc.want, tc.interval)
			}
		})
	}
}

func TestInboundPollDecisionCatchupAndBlocked(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	forceCatchup := decideInboundPoll(inboundPollInput{
		ChatID:       "chat-1",
		Role:         inboundPollRoleWork,
		HasPoll:      true,
		ForceCatchup: true,
		Poll: teamstore.ChatPollState{
			ChatID:     "chat-1",
			Seeded:     true,
			NextPollAt: now.Add(time.Hour),
		},
		Now: now,
	})
	if !forceCatchup.Due || forceCatchup.State != inboundPollStateCatchup || forceCatchup.Interval != inboundPollCatchupInterval || !forceCatchup.NextPollAt.Equal(now) {
		t.Fatalf("force catchup decision = %#v, want due catchup", forceCatchup)
	}
	blockedForceCatchup := decideInboundPoll(inboundPollInput{
		ChatID:       "chat-429",
		Role:         inboundPollRoleWork,
		HasPoll:      true,
		ForceCatchup: true,
		Poll: teamstore.ChatPollState{
			ChatID:       "chat-429",
			Seeded:       true,
			PollState:    inboundPollStateBlocked,
			BlockedUntil: now.Add(time.Minute),
			NextPollAt:   now,
		},
		Now: now,
	})
	if blockedForceCatchup.Due || blockedForceCatchup.State != inboundPollStateBlocked || !blockedForceCatchup.NextPollAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("force catchup bypassed durable retry gate: %#v", blockedForceCatchup)
	}
	legacy429ForceCatchup := decideInboundPoll(inboundPollInput{
		ChatID:       "chat-legacy-429",
		Role:         inboundPollRoleWork,
		HasPoll:      true,
		ForceCatchup: true,
		Poll: teamstore.ChatPollState{
			ChatID:       "chat-legacy-429",
			Seeded:       true,
			FailureCount: 1,
			LastError:    "Graph request failed: HTTP 429",
			NextPollAt:   now.Add(2 * time.Minute),
		},
		Now: now,
	})
	if legacy429ForceCatchup.Due || legacy429ForceCatchup.State != inboundPollStateBlocked || !legacy429ForceCatchup.NextPollAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("force catchup bypassed legacy NextPollAt-only 429 gate: %#v", legacy429ForceCatchup)
	}
	unseededCatchup := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:     "chat-1",
			NextPollAt: now.Add(time.Hour),
		},
		Now: now,
	})
	if !unseededCatchup.Due || unseededCatchup.State != inboundPollStateCatchup || unseededCatchup.Interval != inboundPollCatchupInterval || !unseededCatchup.NextPollAt.Equal(now) {
		t.Fatalf("unseeded catchup decision = %#v, want due catchup", unseededCatchup)
	}
	blockedUntil := now.Add(45 * time.Second)
	blocked := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:            "chat-1",
			Seeded:            true,
			PollState:         inboundPollStateBlocked,
			PreviousPollState: inboundPollStateWarm,
			BlockedUntil:      blockedUntil,
			NextPollAt:        now,
		},
		Now: now,
	})
	if blocked.Due || blocked.State != inboundPollStateBlocked || !blocked.NextPollAt.Equal(blockedUntil) || blocked.PreviousState != inboundPollStateWarm {
		t.Fatalf("blocked decision = %#v", blocked)
	}
}

func TestInboundPollDecisionIgnoresControlContinuation(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "control-chat",
		Role:    inboundPollRoleControl,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:             "control-chat",
			Seeded:             true,
			ContinuationPath:   "/chats/control-chat/messages?$skiptoken=old-history",
			LastActivityAt:     now.Add(-time.Hour),
			LastModifiedCursor: now.Add(-time.Minute),
			NextPollAt:         now.Add(time.Hour),
		},
		Now: now,
	})
	if decision.State == inboundPollStateCatchup || decision.Due {
		t.Fatalf("control continuation should not force catchup polling: %#v", decision)
	}
}

func TestInboundPollSuccessTimeDoesNotCountAsActivity(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:                "chat-1",
			Seeded:                true,
			LastSuccessfulPollAt:  now.Add(-time.Second),
			LastActivityAt:        time.Time{},
			LastModifiedCursor:    now.Add(-time.Second),
			LastWindowFullMessage: "",
			NextPollAt:            now,
		},
		Now: now,
	})
	if decision.State != inboundPollStateWarm {
		t.Fatalf("poll success without user/helper activity should not stay hot: %#v", decision)
	}
}

func TestInboundPollControlNeverParks(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "control",
		Role:    inboundPollRoleControl,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:         "control",
			Seeded:         true,
			LastActivityAt: now.Add(-30 * 24 * time.Hour),
			NextPollAt:     now,
		},
		Now: now,
	})
	if decision.ShouldPark || decision.State != inboundPollStateWarm || decision.Interval != inboundPollControlInterval {
		t.Fatalf("control decision = %#v, want warm non-parked control interval", decision)
	}
}

func TestInboundPollParkNoticeRetriesUntilRecorded(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	poll := teamstore.ChatPollState{
		ChatID:         "chat-1",
		Seeded:         true,
		PollState:      inboundPollStateParked,
		LastActivityAt: now.Add(-49 * time.Hour),
	}
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll:    poll,
		Now:     now,
	})
	if !decision.ShouldPark || !decision.ShouldNotifyPark {
		t.Fatalf("parked without notice should still notify: %#v", decision)
	}
	poll.ParkNoticeSentAt = now.Add(-time.Minute)
	decision = decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll:    poll,
		Now:     now,
	})
	if decision.ShouldPark || decision.ShouldNotifyPark || !decision.ParkedProbe || !decision.Due {
		t.Fatalf("parked with notice should become a due probe: %#v", decision)
	}
}

func TestInboundPollPendingPageBypassesGraph429Deadline(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	blockedUntil := now.Add(10 * time.Minute)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-pending-429",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:         "chat-pending-429",
			Seeded:         true,
			PollState:      inboundPollStateBlocked,
			LastActivityAt: now.Add(-time.Minute),
			NextPollAt:     blockedUntil,
			BlockedUntil:   blockedUntil,
			FailureCount:   4,
			LastError:      "Graph messages failed: HTTP 429 Too Many Requests",
			PendingPage: &teamstore.ChatPollPendingPage{
				ChatID:      "chat-pending-429",
				RequestPath: "/chats/chat-pending-429/messages?$top=20",
				ReceiptID:   "receipt-pending-429",
				Frontier:    "head",
				PollRole:    "work",
			},
		},
		Now: now,
	})
	if !decision.Due || !decision.NextPollAt.Equal(now) || !decision.BlockedUntil.IsZero() || decision.ShouldPark || decision.State == inboundPollStateBlocked {
		t.Fatalf("pending page behind 429 deadline must be immediately locally replayable: %#v", decision)
	}
}

func TestInboundPollPendingExceptionalRecordRespectsGraph429Deadline(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	blockedUntil := now.Add(10 * time.Minute)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-pending-refetch-429",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:         "chat-pending-refetch-429",
			Seeded:         true,
			PollState:      inboundPollStateBlocked,
			LastActivityAt: now.Add(-time.Minute),
			NextPollAt:     blockedUntil,
			BlockedUntil:   blockedUntil,
			FailureCount:   4,
			LastError:      "Graph message refetch failed: HTTP 429 Too Many Requests",
			PendingPage: &teamstore.ChatPollPendingPage{
				ChatID:          "chat-pending-refetch-429",
				RequestPath:     "/chats/chat-pending-refetch-429/messages?$top=20",
				ReceiptID:       "receipt-pending-refetch-429",
				Frontier:        "head",
				PollRole:        "work",
				Dispositions:    []string{"invalid_record"},
				RefetchFailures: []int{1},
			},
		},
		Now: now,
	})
	if decision.Due || !decision.NextPollAt.Equal(blockedUntil) || !decision.BlockedUntil.Equal(blockedUntil) || decision.State == inboundPollStateBlocked {
		t.Fatalf("Graph-dependent pending page must respect 429 deadline: %#v", decision)
	}
}

func TestPendingPageRequiresGraphReplayTreatsLegacyRefetchFailuresAsGraphBound(t *testing.T) {
	page := &teamstore.ChatPollPendingPage{
		Records:         []json.RawMessage{{}},
		RefetchFailures: []int{1},
		// Older writers did not persist Dispositions. A positive refetch count
		// is nevertheless proof that the record still needs Graph, not a local
		// receipt replay that may bypass its retry gate.
	}
	if !pendingPageRequiresGraphReplay(page) {
		t.Fatal("legacy pending page with a refetch failure was treated as local replay")
	}
	page.RefetchFailures = nil
	if pendingPageRequiresGraphReplay(page) {
		t.Fatal("legacy pending page without refetch failures was unexpectedly Graph-bound")
	}
}

func TestPendingPageRequiresGraphReplayFailsClosedOnDispositionLengthMismatch(t *testing.T) {
	page := &teamstore.ChatPollPendingPage{
		Records:      []json.RawMessage{{}, {}},
		Dispositions: []string{"received"},
	}
	if !pendingPageRequiresGraphReplay(page) {
		t.Fatal("pending page with mismatched disposition length was treated as local replay")
	}
}

func TestInboundPollPendingUnknownDispositionRespectsGraph429Deadline(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	blockedUntil := now.Add(10 * time.Minute)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-pending-unknown-429",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:         "chat-pending-unknown-429",
			Seeded:         true,
			PollState:      inboundPollStateBlocked,
			LastActivityAt: now.Add(-time.Minute),
			NextPollAt:     blockedUntil,
			BlockedUntil:   blockedUntil,
			FailureCount:   4,
			LastError:      "Graph message refetch failed: HTTP 429 Too Many Requests",
			PendingPage: &teamstore.ChatPollPendingPage{
				ChatID:          "chat-pending-unknown-429",
				RequestPath:     "/chats/chat-pending-unknown-429/messages?$top=20",
				ReceiptID:       "receipt-pending-unknown-429",
				Frontier:        "head",
				PollRole:        "work",
				Dispositions:    []string{"future-disposition-from-new-writer"},
				RefetchFailures: []int{0},
			},
		},
		Now: now,
	})
	if decision.Due || !decision.NextPollAt.Equal(blockedUntil) || !decision.BlockedUntil.Equal(blockedUntil) || decision.State == inboundPollStateBlocked {
		t.Fatalf("unknown pending disposition must respect 429 deadline: %#v", decision)
	}
}

func TestPollGraphReadBlockedSnapshotPreservesLocalReplayAndHonorsReadGates(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	accountUntil := now.Add(10 * time.Minute)
	chatUntil := now.Add(5 * time.Minute)
	bridge := &Bridge{
		groupChatGuardEnabled: true,
		chatAudiences:         make(map[string]chatAudienceSnapshot),
	}

	if _, blocked := bridge.pollGraphReadBlockedUntilSnapshot(
		"fresh-account-gated", inboundPollRoleWork,
		teamstore.ChatPollState{ChatID: "fresh-account-gated"}, accountUntil, now,
	); !blocked {
		t.Fatal("fresh Graph read ignored the durable account read gate")
	}

	exceptional := teamstore.ChatPollState{
		ChatID:       "exceptional-chat-gated",
		BlockedUntil: chatUntil,
		PendingPage: &teamstore.ChatPollPendingPage{
			ChatID:          "exceptional-chat-gated",
			ReceiptID:       "receipt-exceptional",
			Frontier:        "head",
			PollRole:        "work",
			Records:         []json.RawMessage{{}},
			Dispositions:    []string{"invalid_record"},
			RefetchFailures: []int{1},
		},
	}
	if got, blocked := bridge.pollGraphReadBlockedUntilSnapshot(
		"exceptional-chat-gated", inboundPollRoleWork, exceptional, time.Time{}, now,
	); !blocked || !got.Equal(chatUntil) {
		t.Fatalf("exceptional pending page gate = %v, %v; want %v, true", got, blocked, chatUntil)
	}

	local := teamstore.ChatPollState{
		ChatID:       "local-replay",
		BlockedUntil: accountUntil,
		PendingPage: &teamstore.ChatPollPendingPage{
			ChatID:       "local-replay",
			ReceiptID:    "receipt-local",
			Frontier:     "head",
			PollRole:     "work",
			Records:      []json.RawMessage{{}},
			Dispositions: []string{"received"},
		},
	}
	bridge.groupChatGuardEnabled = false
	if _, blocked := bridge.pollGraphReadBlockedUntilSnapshot(
		"local-replay", inboundPollRoleWork, local, accountUntil, now,
	); blocked {
		t.Fatal("local pending receipt was blocked even though it needs no Graph read")
	}

	bridge.groupChatGuardEnabled = true
	uncachedAudience := local
	uncachedAudience.ChatID = "uncached-audience"
	uncachedAudience.BlockedUntil = chatUntil
	if got, blocked := bridge.pollGraphReadBlockedUntilSnapshot(
		"uncached-audience", inboundPollRoleWork, uncachedAudience, time.Time{}, now,
	); !blocked || !got.Equal(chatUntil) {
		t.Fatalf("uncached audience gate = %v, %v; want %v, true", got, blocked, chatUntil)
	}
	bridge.cacheChatAudience("uncached-audience", chatAudienceSnapshot{Mode: chatAudienceMultiMember, CheckedAt: time.Now()})
	if _, blocked := bridge.pollGraphReadBlockedUntilSnapshot(
		"uncached-audience", inboundPollRoleWork, uncachedAudience, accountUntil, now,
	); blocked {
		t.Fatal("cached audience local replay was blocked by account read gate")
	}
}

func TestInboundPollParkProbeRespectsNextPollAt(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Minute)
	decision := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:           "chat-1",
			Seeded:           true,
			PollState:        inboundPollStateParked,
			ParkedAt:         now.Add(-72 * time.Hour),
			ParkNoticeSentAt: now.Add(-71 * time.Hour),
			NextPollAt:       future,
			LastActivityAt:   now.Add(-72 * time.Hour),
		},
		Now: now,
	})
	if decision.ShouldPark || !decision.ParkedProbe || decision.Due || !decision.NextPollAt.Equal(future) || decision.Interval != inboundPollParkProbeInterval {
		t.Fatalf("future parked probe decision = %#v, want deferred low-frequency probe", decision)
	}
}

func TestLimitInboundPollDecisionsReservesParkedProbeUnderHotLoad(t *testing.T) {
	decisions := make([]inboundPollDecision, 0, 10)
	for i := 0; i < 8; i++ {
		decisions = append(decisions, inboundPollDecision{
			ChatID: "hot-" + strconv.Itoa(i),
			State:  inboundPollStateHot,
			Due:    true,
		})
	}
	decisions = append(decisions, inboundPollDecision{
		ChatID: "parked-probe",
		State:  inboundPollStateParked,
		Due:    true,
	})
	decisions = append(decisions, inboundPollDecision{
		ChatID: "warm-backlog",
		State:  inboundPollStateWarm,
		Due:    true,
	})

	limited := limitInboundPollDecisions(decisions, 8)
	if len(limited) != 8 {
		t.Fatalf("limited decisions = %d, want 8", len(limited))
	}
	seenParked := false
	seenWarm := false
	for _, decision := range limited {
		seenParked = seenParked || decision.State == inboundPollStateParked
		seenWarm = seenWarm || decision.ChatID == "warm-backlog"
	}
	if !seenParked {
		t.Fatalf("per-cycle limit dropped every parked probe: %#v", limited)
	}
	if !seenWarm {
		t.Fatalf("per-cycle limit dropped every due non-hot chat: %#v", limited)
	}
}

func TestLimitInboundPollDecisionsReservesHotTailBehindRunningTurns(t *testing.T) {
	decisions := make([]inboundPollDecision, 0, 9)
	for i := 0; i < 8; i++ {
		decisions = append(decisions, inboundPollDecision{
			ChatID: "running-" + strconv.Itoa(i),
			State:  inboundPollStateRunning,
			Due:    true,
		})
	}
	decisions = append(decisions, inboundPollDecision{
		ChatID:     "hot-tail",
		State:      inboundPollStateHot,
		Due:        true,
		NextPollAt: time.Unix(1, 0),
	})

	for cycle := 0; cycle < 3; cycle++ {
		limited := limitInboundPollDecisions(decisions, 8)
		found := false
		for _, decision := range limited {
			found = found || decision.ChatID == "hot-tail"
		}
		if !found {
			t.Fatalf("cycle %d dropped due hot tail behind running turns: %#v", cycle, limited)
		}
	}
}

func TestLimitInboundPollDecisionsReservesOldestDueNonHotBacklog(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	decisions := make([]inboundPollDecision, 0, 10)
	for i := 0; i < 8; i++ {
		decisions = append(decisions, inboundPollDecision{
			ChatID: "running-" + strconv.Itoa(i),
			State:  inboundPollStateRunning,
			Due:    true,
		})
	}
	decisions = append(decisions,
		inboundPollDecision{
			ChatID:     "warm-newer",
			State:      inboundPollStateWarm,
			Due:        true,
			NextPollAt: now.Add(-time.Minute),
		},
		inboundPollDecision{
			ChatID:     "cold-oldest",
			State:      inboundPollStateCold,
			Due:        true,
			NextPollAt: now.Add(-time.Hour),
		},
	)

	limited := limitInboundPollDecisions(decisions, 8)
	for _, decision := range limited {
		if decision.ChatID == "cold-oldest" {
			return
		}
	}
	t.Fatalf("oldest due non-hot backlog was dropped: %#v", limited)
}

func TestLimitInboundPollDecisionsReservesOrdinaryBehindOperationalFrontiers(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	decisions := make([]inboundPollDecision, 0, 9)
	for i := 0; i < 8; i++ {
		decisions = append(decisions, inboundPollDecision{
			ChatID:               fmt.Sprintf("operational-%02d", i),
			State:                inboundPollStateHot,
			Due:                  true,
			NextPollAt:           now,
			LastSuccessfulPollAt: now,
			OperationalFrontier:  true,
		})
	}
	decisions = append(decisions, inboundPollDecision{
		ChatID:               "ordinary-tail",
		State:                inboundPollStateHot,
		Due:                  true,
		NextPollAt:           now,
		LastSuccessfulPollAt: now,
		OperationalFrontier:  false,
	})

	limited := limitInboundPollDecisions(decisions, 8)
	for _, decision := range limited {
		if decision.ChatID == "ordinary-tail" {
			return
		}
	}
	t.Fatalf("ordinary chat was starved behind operational frontiers: %#v", limited)
}

func TestLimitInboundPollDecisionsReservesHealthyChatAlongsideDueRetries(t *testing.T) {
	now := time.Date(2026, 9, 9, 13, 0, 0, 0, time.UTC)
	decisions := make([]inboundPollDecision, 0, 9)
	for i := 0; i < 8; i++ {
		decision := decideInboundPoll(inboundPollInput{
			ChatID: "retry-" + strconv.Itoa(i), Role: inboundPollRoleWork, HasPoll: true,
			Poll: teamstore.ChatPollState{
				ChatID: "retry-" + strconv.Itoa(i), Seeded: true, PollState: inboundPollStateCold,
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now.Add(-time.Minute),
				LastSuccessfulPollAt: now.Add(-time.Hour), FailureCount: 1,
			},
			Now: now,
		})
		if !decision.RetryFailure || !decision.Due {
			t.Fatalf("retry decision = %#v, want due retry", decision)
		}
		decisions = append(decisions, decision)
	}
	ordinary := decideInboundPoll(inboundPollInput{
		ChatID: "healthy-ordinary", Role: inboundPollRoleWork, HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID: "healthy-ordinary", Seeded: true, PollState: inboundPollStateCold,
			NextPollAt: now.Add(-2 * time.Minute), LastActivityAt: now.Add(-time.Minute),
			LastSuccessfulPollAt: now.Add(-2 * time.Hour),
		},
		Now: now,
	})
	if ordinary.RetryFailure || !ordinary.Due {
		t.Fatalf("ordinary decision = %#v, want due non-retry", ordinary)
	}
	decisions = append(decisions, ordinary)

	sortInboundPollDecisions(decisions)
	limited := limitInboundPollDecisions(decisions, 8)
	foundOrdinary := false
	retryCount := 0
	for _, decision := range limited {
		foundOrdinary = foundOrdinary || decision.ChatID == "healthy-ordinary"
		if decision.RetryFailure {
			retryCount++
		}
	}
	if !foundOrdinary || retryCount == 0 {
		t.Fatalf("cycle dropped one lane: ordinary=%v retry_count=%d decisions=%#v", foundOrdinary, retryCount, limited)
	}
}

func TestLimitInboundPollDecisionsAgesEvictedRetryBackIntoSelection(t *testing.T) {
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	decisions := make([]inboundPollDecision, 0, 9)
	for i := 0; i < 8; i++ {
		decisions = append(decisions, inboundPollDecision{
			ChatID:               fmt.Sprintf("retry-age-%02d", i),
			State:                inboundPollStateCold,
			Due:                  true,
			RetryFailure:         true,
			NextPollAt:           now.Add(-time.Minute),
			LastSuccessfulPollAt: now.Add(-2 * time.Hour),
			LastErrorAt:          now.Add(-time.Duration(8-i) * time.Minute),
		})
	}
	decisions = append(decisions, inboundPollDecision{
		ChatID:               "retry-age-ordinary",
		State:                inboundPollStateCold,
		Due:                  true,
		NextPollAt:           now.Add(-2 * time.Minute),
		LastSuccessfulPollAt: now.Add(-3 * time.Hour),
	})

	sortInboundPollDecisions(decisions)
	first := limitInboundPollDecisions(decisions, 8)
	foundOrdinary := false
	foundNewestRetry := false
	for _, decision := range first {
		foundOrdinary = foundOrdinary || decision.ChatID == "retry-age-ordinary"
		foundNewestRetry = foundNewestRetry || decision.ChatID == "retry-age-07"
	}
	if !foundOrdinary || foundNewestRetry {
		t.Fatalf("first retry/ordinary cycle = %#v, want ordinary and newest retry temporarily evicted", first)
	}

	// The seven admitted retries make another failed attempt. Their durable
	// LastErrorAt values advance; the retry evicted for the ordinary slot must
	// therefore age to the front and survive the next ordinary reservation.
	for index := range decisions {
		if decisions[index].ChatID == "retry-age-07" || !decisions[index].RetryFailure {
			continue
		}
		decisions[index].LastErrorAt = now.Add(time.Duration(index+1) * time.Second)
	}
	sortInboundPollDecisions(decisions)
	second := limitInboundPollDecisions(decisions, 8)
	foundNewestRetry = false
	for _, decision := range second {
		foundNewestRetry = foundNewestRetry || decision.ChatID == "retry-age-07"
	}
	if !foundNewestRetry {
		t.Fatalf("second retry/ordinary cycle still starved the previously evicted retry: %#v", second)
	}
}

func TestLimitInboundPollDecisionsPreservesSoleRetryAgainstOrdinaryReservation(t *testing.T) {
	now := time.Date(2026, 9, 10, 13, 0, 0, 0, time.UTC)
	decisions := []inboundPollDecision{
		{
			ChatID:               "failed-chat",
			State:                inboundPollStateHot,
			Due:                  true,
			RetryFailure:         true,
			NextPollAt:           now.Add(-time.Minute),
			LastErrorAt:          now.Add(-time.Minute),
			LastSuccessfulPollAt: now.Add(-time.Hour),
			LastActivityAt:       now.Add(-10 * time.Minute),
		},
	}
	for i := 0; i < 7; i++ {
		decisions = append(decisions, inboundPollDecision{
			ChatID:               fmt.Sprintf("healthy-hot-%02d", i),
			State:                inboundPollStateHot,
			Due:                  true,
			NextPollAt:           now.Add(-30 * time.Second),
			LastSuccessfulPollAt: now,
			LastActivityAt:       now,
		})
	}
	decisions = append(decisions, inboundPollDecision{
		ChatID:               "ordinary-tail",
		State:                inboundPollStateHot,
		Due:                  true,
		NextPollAt:           now.Add(-2 * time.Minute),
		LastSuccessfulPollAt: now.Add(-2 * time.Hour),
		LastActivityAt:       now.Add(-2 * time.Hour),
	})

	sortInboundPollDecisions(decisions)
	limited := limitInboundPollDecisions(decisions, 8)
	seenFailed := false
	seenOrdinary := false
	for _, decision := range limited {
		seenFailed = seenFailed || decision.ChatID == "failed-chat"
		seenOrdinary = seenOrdinary || decision.ChatID == "ordinary-tail"
	}
	if !seenFailed || !seenOrdinary {
		t.Fatalf("sole retry was lost while reserving ordinary work: failed=%v ordinary=%v decisions=%#v", seenFailed, seenOrdinary, limited)
	}
}

func TestLimitInboundPollDecisionsPreservesSoleOperationalFrontierAgainstOrdinaryReservation(t *testing.T) {
	now := time.Date(2026, 9, 10, 14, 0, 0, 0, time.UTC)
	decisions := make([]inboundPollDecision, 0, 10)
	for i := 0; i < 8; i++ {
		decisions = append(decisions, inboundPollDecision{
			ChatID:               fmt.Sprintf("hot-backlog-%02d", i),
			State:                inboundPollStateHot,
			Due:                  true,
			NextPollAt:           now.Add(-time.Minute),
			LastSuccessfulPollAt: now,
			LastActivityAt:       now,
		})
	}
	decisions = append(decisions,
		inboundPollDecision{
			ChatID:               "expired-continuation",
			State:                inboundPollStateCold,
			Due:                  true,
			NextPollAt:           now.Add(-time.Minute),
			LastSuccessfulPollAt: now.Add(-time.Hour),
			LastActivityAt:       now.Add(-time.Hour),
			OperationalFrontier:  true,
		},
		inboundPollDecision{
			ChatID:               "ordinary-tail",
			State:                inboundPollStateCold,
			Due:                  true,
			NextPollAt:           now.Add(-2 * time.Minute),
			LastSuccessfulPollAt: now.Add(-2 * time.Hour),
			LastActivityAt:       now.Add(-2 * time.Hour),
		},
	)

	sortInboundPollDecisions(decisions)
	limited := limitInboundPollDecisions(decisions, 8)
	seenOperational := false
	seenOrdinary := false
	for _, decision := range limited {
		seenOperational = seenOperational || decision.ChatID == "expired-continuation"
		seenOrdinary = seenOrdinary || decision.ChatID == "ordinary-tail"
	}
	if !seenOperational || !seenOrdinary {
		t.Fatalf("sole operational frontier was lost while reserving ordinary work: operational=%v ordinary=%v decisions=%#v", seenOperational, seenOrdinary, limited)
	}
}
