package channeltest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

func TestFakeReceiveCursorAndEcho(t *testing.T) {
	ctx := context.Background()
	f := New()
	info, err := f.CreateChat(ctx, "t", "k1")
	if err != nil {
		t.Fatal(err)
	}
	again, _ := f.CreateChat(ctx, "other", "k1")
	if again.ID != info.ID {
		t.Fatalf("CreateChat not idempotent: %s vs %s", again.ID, info.ID)
	}
	f.PostUser(info.ID, "one")
	w, err := f.Receive(ctx, info.ID, "")
	if err != nil || len(w.Msgs) != 1 || w.Msgs[0].Text != "one" {
		t.Fatalf("first window = %+v, %v", w, err)
	}
	parts, _ := f.Render(ctx, channel.RichDoc{Label: "A", Markdown: "answer"})
	if _, err := f.Send(ctx, info.ID, parts[0], "tok1"); err != nil {
		t.Fatal(err)
	}
	f.PostSystem(info.ID, "joined")
	w2, _ := f.Receive(ctx, info.ID, w.Cursor)
	if len(w2.Msgs) != 2 || w2.Msgs[0].Marker != "tok1" || w2.Msgs[0].Hash != parts[0].Hash || w2.Msgs[1].Kind != channel.KindSystem {
		t.Fatalf("second window = %+v", w2.Msgs)
	}
	f.SetCaps(channel.Caps{MaxPartBytes: 4096})
	w3, _ := f.Receive(ctx, info.ID, w.Cursor)
	if len(w3.Msgs) != 1 || w3.Msgs[0].Kind != channel.KindSystem {
		t.Fatalf("no-self-echo window = %+v", w3.Msgs)
	}
}

func TestFakeAmbiguousDeliveredAndProbe(t *testing.T) {
	ctx := context.Background()
	f := New()
	info, _ := f.CreateChat(ctx, "t", "k")
	f.SendFault = func(string, channel.Part) (bool, error) {
		return true, &channel.Ambiguous{Err: errors.New("eof")}
	}
	parts, _ := f.Render(ctx, channel.RichDoc{Label: "A", Markdown: "x"})
	_, err := f.Send(ctx, info.ID, parts[0], "tok")
	if !channel.IsAmbiguous(err) {
		t.Fatalf("err = %v", err)
	}
	back, _ := f.ReadBack(ctx, info.ID, f.Messages(info.ID)[0].Created.Add(-1))
	if len(back) != 1 || back[0].Marker != "tok" {
		t.Fatalf("readback = %+v", back)
	}
	f.SendFault = nil
	st, _, err := channel.ProbeMarker(ctx, f, info.ID, channel.RichDoc{Label: "probe", Markdown: "p"}, "probe-1")
	if err != nil || st != channel.MarkerPreserved {
		t.Fatalf("probe = %v %v", st, err)
	}
	f.StripMarkers = true
	st, _, err = channel.ProbeMarker(ctx, f, info.ID, channel.RichDoc{Label: "probe", Markdown: "p"}, "probe-2")
	if err != nil || st != channel.MarkerStripped {
		t.Fatalf("probe stripped = %v %v", st, err)
	}
}

func TestFakeRenderSplits(t *testing.T) {
	f := New()
	f.SetCaps(channel.Caps{MaxPartBytes: 128, Markers: true})
	md := strings.Repeat("line of text\n", 40)
	parts, err := f.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: md, Mention: true})
	if err != nil || len(parts) < 2 {
		t.Fatalf("parts=%d err=%v", len(parts), err)
	}
	for i, p := range parts {
		if p.Size > 128 || p.Index != i+1 || p.Count != len(parts) || p.Mention != (i == 0) {
			t.Fatalf("part %d = %+v", i, p)
		}
	}
}

func TestTextHashIgnoresWhitespace(t *testing.T) {
	if channel.TextHash("a  b\n c") != channel.TextHash("a b c") {
		t.Fatal("whitespace changed hash")
	}
	if channel.TextHash("a b") == channel.TextHash("ab") {
		t.Fatal("hash ignores word boundaries")
	}
}

func TestFakeEditMarkUnreadAndWindowLimit(t *testing.T) {
	ctx := context.Background()
	f := New()
	info, _ := f.CreateChat(ctx, "t", "k")
	parts, _ := f.Render(ctx, channel.RichDoc{Label: "A", Markdown: "draft"})
	sent, err := f.Send(ctx, info.ID, parts[0], "tok")
	if err != nil {
		t.Fatal(err)
	}
	w, _ := f.Receive(ctx, info.ID, "")
	edited, _ := f.Render(ctx, channel.RichDoc{Label: "A", Markdown: "final"})
	if err := f.Edit(ctx, info.ID, sent.ID, edited[0], "tok"); err != nil {
		t.Fatal(err)
	}
	w2, _ := f.Receive(ctx, info.ID, w.Cursor)
	if len(w2.Msgs) != 1 || !w2.Msgs[0].Edited || w2.Msgs[0].Hash != edited[0].Hash || w2.Msgs[0].Marker != "tok" {
		t.Fatalf("edit echo = %+v", w2.Msgs)
	}
	u := f.PostCoworker(info.ID, "prompt")
	if err := f.Edit(ctx, info.ID, u.ID, edited[0], ""); err == nil {
		t.Fatal("edited a message of someone else")
	}
	if err := f.MarkUnread(ctx, info.ID, sent.Time.Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if at, n := f.Unread(info.ID); n != 1 || !at.Equal(sent.Time.Add(-time.Millisecond)) {
		t.Fatalf("unread = %v %d", at, n)
	}
	f.MaxWindow = 1
	f.PostUser(info.ID, "another")
	var tl *channel.WindowTooLarge
	if _, err := f.Receive(ctx, info.ID, w2.Cursor); !errors.As(err, &tl) || tl.SkipCursor == "" {
		t.Fatalf("window limit: %v", err)
	}
}
