package teams

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestMeetingPrimitives(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	fg, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.Now = func() time.Time { return now } })
	info, mt, err := a.CreateMeetingChat(ctx, "Machine registry", "registry-key", now.Add(-5*time.Minute), now.Add(30*24*time.Hour))
	if err != nil || mt.ID == "" || mt.ChatID != info.ID || !mt.End.Equal(now.Add(30*24*time.Hour)) {
		t.Fatalf("create: %+v %+v %v", info, mt, err)
	}
	again, err := a.CreateChat(ctx, "ignored", "registry-key")
	if err != nil || again.ID != info.ID {
		t.Fatalf("CreateChat with the same key: %+v %v", again, err)
	}
	got, err := a.GetMeeting(ctx, mt.ID)
	if err != nil || got.ChatID != info.ID {
		t.Fatalf("get: %+v %v", got, err)
	}
	renewed, err := a.RenewMeeting(ctx, mt.ID, now, now.Add(60*24*time.Hour))
	if err != nil || !renewed.End.Equal(now.Add(60*24*time.Hour)) {
		t.Fatalf("renew: %+v %v", renewed, err)
	}
	if got, _ := a.GetMeeting(ctx, mt.ID); !got.End.Equal(renewed.End) {
		t.Fatalf("renewal not stored: %+v", got)
	}
	if _, err := a.RenewMeeting(ctx, mt.ID, now, now); !isPermanent(err) {
		t.Fatalf("empty window: %v", err)
	}
	if _, err := a.GetMeeting(ctx, "MSo-missing"); !errors.Is(err, channel.ErrNotFound) {
		t.Fatalf("missing meeting: %v", err)
	}
	if fg.Chats()[0].ExternalID != "registry-key" || len(fg.Chats()) != 1 {
		t.Fatalf("chats %+v", fg.Chats())
	}
	noViolations(t, fg)
}
