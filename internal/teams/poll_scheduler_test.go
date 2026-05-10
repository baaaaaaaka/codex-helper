package teams

import (
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
		{name: "parked at boundary", idle: 48 * time.Hour, want: inboundPollStateParked, parked: true},
		{name: "parked", idle: 49 * time.Hour, want: inboundPollStateParked, parked: true},
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

func TestInboundPollDecisionCatchupAndBlocked(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	catchup := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:           "chat-1",
			Seeded:           true,
			ContinuationPath: "/chats/chat-1/messages?$skiptoken=next",
			NextPollAt:       now.Add(time.Hour),
		},
		Now: now,
	})
	if catchup.Due || catchup.State != inboundPollStateCatchup || catchup.Interval != inboundPollCatchupInterval || !catchup.NextPollAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("continuation decision = %#v, want throttled catchup", catchup)
	}
	dueCatchup := decideInboundPoll(inboundPollInput{
		ChatID:  "chat-1",
		Role:    inboundPollRoleWork,
		HasPoll: true,
		Poll: teamstore.ChatPollState{
			ChatID:           "chat-1",
			Seeded:           true,
			ContinuationPath: "/chats/chat-1/messages?$skiptoken=next",
			NextPollAt:       now.Add(-time.Second),
		},
		Now: now,
	})
	if !dueCatchup.Due || dueCatchup.State != inboundPollStateCatchup || dueCatchup.Interval != inboundPollCatchupInterval {
		t.Fatalf("due continuation decision = %#v, want due catchup", dueCatchup)
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
	if !decision.ShouldPark || decision.ShouldNotifyPark {
		t.Fatalf("parked with notice should not notify again: %#v", decision)
	}
}
