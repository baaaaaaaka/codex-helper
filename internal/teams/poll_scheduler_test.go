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
		{name: "warm", idle: 10 * time.Minute, want: inboundPollStateWarm, interval: inboundPollWarmInterval},
		{name: "cool", idle: time.Hour, want: inboundPollStateCool, interval: inboundPollCoolInterval},
		{name: "cool below extended threshold", idle: 3 * time.Hour, want: inboundPollStateCool, interval: inboundPollCoolInterval},
		{name: "cold", idle: 5 * time.Hour, want: inboundPollStateCold, interval: inboundPollColdInterval},
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
