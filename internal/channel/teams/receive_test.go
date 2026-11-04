package teams

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestInboundEmojiKept(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("emoji").ID
	html := `<p>fix this <emoji id="1f600_grinningface" alt="😀" title="Grinning face"></emoji> please ` +
		`<img itemtype="http://schema.skype.com/Emoji" itemscope="" src="https://statics.teams.cdn.office.net/x.png" alt="👍" width="20"> ` +
		`<customemoji id="abc" alt=":party:" source="x"></customemoji></p><p>family <emoji alt="👨‍👩‍👧"></emoji> and <emoji id="heart" title="❤️"></emoji></p>`
	if _, err := fg.PostHTMLAsUser(chat, html); err != nil {
		t.Fatal(err)
	}
	w, err := a.Receive(ctx, chat, "")
	if err != nil || len(w.Msgs) != 1 {
		t.Fatalf("%+v %v", w, err)
	}
	want := "fix this 😀 please 👍 :party:\nfamily 👨‍👩‍👧 and ❤️"
	if got := w.Msgs[0].Text; got != want {
		t.Fatalf("text %q, want %q", got, want)
	}
	// Hashing stays emoji-insensitive (outbound echo matching).
	if w.Msgs[0].Hash != channel.TextHash("fix this please :party:\nfamily and") {
		t.Fatalf("hash changed: %s", w.Msgs[0].Hash)
	}
	cases := map[string]string{
		`a<emoji alt="&lt;3"/>b`:                                      "a<3b",
		`<emoji alt='😀'>😀</emoji>`:                                    "😀",
		`x <emoji id="no-alt"></emoji>y`:                              "x y",
		`<IMG ITEMTYPE='http://schema.skype.com/Emoticon' ALT='(y)'>`: "(y)",
		`<img src="photo.png" alt="not an emoji">`:                    "",
	}
	for in, want := range cases {
		if got := plainText(in); got != want {
			t.Errorf("plainText(%q) = %q, want %q", in, got, want)
		}
	}
	noViolations(t, fg)
}

func TestReceiveWindowTooLarge(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.MaxPages = 2; c.Overlap = time.Millisecond })
	chat := fg.CreateMeetingChatAsUser("backlog").ID
	fg.PostAsUser(chat, "seen")
	w, err := a.Receive(ctx, chat, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2*pageSize+5; i++ {
		fg.PostAsUser(chat, fmt.Sprintf("backlog %d", i))
	}
	_, err = a.Receive(ctx, chat, w.Cursor)
	var tl *channel.WindowTooLarge
	if !errors.As(err, &tl) || tl.Pages != 2 || tl.OldestSeen.IsZero() || tl.SkipCursor <= w.Cursor {
		t.Fatalf("err = %v (%+v)", err, tl)
	}
	var r *channel.Rejected
	if errors.As(err, &r) {
		t.Fatal("WindowTooLarge must not look like a plain Rejected")
	}
	msgs := fg.Messages(chat)
	if !tl.OldestSeen.After(msgs[1].Modified) || tl.OldestSeen.After(msgs[len(msgs)-1].Modified) {
		t.Fatalf("oldest seen %v outside the backlog", tl.OldestSeen)
	}
	// The core may skip after telling the user: the next window is small.
	w2, err := a.Receive(ctx, chat, tl.SkipCursor)
	if err != nil || len(w2.Msgs) > 1 {
		t.Fatalf("after skip: %d msgs, %v", len(w2.Msgs), err)
	}
}

// Recent reads a bounded tail, newest first, across page boundaries, and
// stops fetching once it has enough (a long chat is not walked whole).
func TestRecentReadsBoundedTailNewestFirst(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("tail").ID
	var ids []string
	for i := 0; i < 130; i++ {
		m, err := fg.PostAsUser(chat, fmt.Sprintf("m%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, m.ID)
	}
	before := len(fg.Requests(fakegraph.RequestFilter{Route: routeList}))
	got, err := a.Recent(ctx, chat, 60)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 60 || got[0].ID != ids[129] || got[59].ID != ids[70] || got[0].Text != "m129" {
		t.Fatalf("got %d messages, first %+v", len(got), got[0])
	}
	if n := len(fg.Requests(fakegraph.RequestFilter{Route: routeList})) - before; n != 2 {
		t.Fatalf("%d list requests for 60 of 130 messages, want 2", n)
	}
	if none, err := a.Recent(ctx, chat, 0); err != nil || none != nil {
		t.Fatalf("limit 0: %v %v", none, err)
	}
	noViolations(t, fg)
}
