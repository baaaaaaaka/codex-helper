package teams

import (
	"context"
	"testing"

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
