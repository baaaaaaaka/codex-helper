package teams

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

const (
	routeList = "GET /chats/{chat}/messages"
	routePost = "POST /chats/{chat}/messages"
)

// F2 regression: the newest page used to be admitted before nextLink was
// walked, so a newer prompt ran before an older one behind the page boundary.
func TestReceiveWalksWholeWindowOldestFirst(t *testing.T) {
	ctx := context.Background()
	// Overlap of 1ms: the fake stamps every write a millisecond apart.
	fg, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.Overlap = time.Millisecond })
	chat := fg.CreateMeetingChatAsUser("order").ID
	p1, _ := fg.PostAsUser(chat, "first prompt")
	for i := 0; i < 60; i++ {
		if _, err := fg.PostSystemEvent(chat); err != nil {
			t.Fatal(err)
		}
	}
	p2, _ := fg.PostAsUser(chat, "second prompt")

	w, err := a.Receive(ctx, chat, "")
	if err != nil {
		t.Fatal(err)
	}
	if w.Requests < 2 {
		t.Fatalf("expected several pages, got %d request(s)", w.Requests)
	}
	if len(w.Msgs) != 62 {
		t.Fatalf("got %d messages, want 62", len(w.Msgs))
	}
	if w.Msgs[0].ID != p1.ID || w.Msgs[61].ID != p2.ID {
		t.Fatalf("order: first=%s last=%s, want %s ... %s", w.Msgs[0].ID, w.Msgs[61].ID, p1.ID, p2.ID)
	}
	for i := 1; i < len(w.Msgs); i++ {
		if w.Msgs[i].Created.Before(w.Msgs[i-1].Created) {
			t.Fatalf("message %d is older than %d", i, i-1)
		}
		if i < 61 && w.Msgs[i].Kind != channel.KindSystem {
			t.Fatalf("message %d kind %v, want system", i, w.Msgs[i].Kind)
		}
	}
	if !w.Msgs[0].ByOwner || w.Msgs[0].Text != "first prompt" || w.Msgs[0].Hash != channel.TextHash("first prompt") {
		t.Fatalf("first message = %+v", w.Msgs[0])
	}
	want := p2.Modified.UTC().Format(cursorLayout)
	if w.Cursor != want {
		t.Fatalf("cursor %s, want %s", w.Cursor, want)
	}

	// Next window: overlap re-reads are allowed, older messages are not, and
	// the cursor never moves backwards.
	p3, _ := fg.PostAsCoworker(chat, "third")
	w2, err := a.Receive(ctx, chat, w.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, m := range w2.Msgs {
		ids[m.ID] = true
	}
	if !ids[p3.ID] || !ids[p2.ID] || ids[p1.ID] || len(ids) > 3 {
		t.Fatalf("second window ids %v", ids)
	}
	if w2.Cursor < w.Cursor {
		t.Fatalf("cursor moved backwards: %s < %s", w2.Cursor, w.Cursor)
	}
	last := w2.Msgs[len(w2.Msgs)-1]
	if last.ByOwner || last.From != fg.Coworker().ID {
		t.Fatalf("coworker message = %+v", last)
	}
	noViolations(t, fg)
}

func TestReceiveCursorIsNotRebaselinedWhenEmpty(t *testing.T) {
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("empty").ID
	w, err := a.Receive(context.Background(), chat, "2026-01-02T03:04:05.006Z")
	if err != nil {
		t.Fatal(err)
	}
	if w.Cursor != "2026-01-02T03:04:05.006Z" || len(w.Msgs) != 0 {
		t.Fatalf("window = %+v", w)
	}
	if _, err := a.Receive(context.Background(), chat, "yesterday"); !isPermanent(err) {
		t.Fatalf("bad cursor err = %v", err)
	}
	noViolations(t, fg)
}

func isPermanent(err error) bool {
	var r *channel.Rejected
	return errors.As(err, &r) && r.Permanent
}

func TestRead429IsThrottledAndDoesNotBlockWrites(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("throttle").ID
	fg.AddFault(fakegraph.Fault{Name: "read429", Kind: fakegraph.FaultStatus, Route: routeList, Status: 429, RetryAfter: 7 * time.Second, Times: 1})

	_, err := a.Receive(ctx, chat, "")
	th, ok := channel.AsThrottled(err)
	if !ok || th.RetryAfter != 7*time.Second || th.Scope != channel.ScopeUnknown {
		t.Fatalf("err = %v", err)
	}
	if n := countRequests(fg, routeList); n != 1 {
		t.Fatalf("read was retried internally: %d requests", n)
	}
	p := renderOne(t, a, channel.RichDoc{Label: "Codex answer", Markdown: "hello"})
	if _, err := a.Send(ctx, chat, p, "tok-1"); err != nil {
		t.Fatalf("write after read throttle: %v", err)
	}
	noViolations(t, fg)
}

func TestWrite429IsThrottledAndNotRetried(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("w429").ID
	fg.AddFault(fakegraph.Fault{Name: "write429", Kind: fakegraph.FaultStatus, Route: routePost, Status: 429, RetryAfter: 3 * time.Second, Times: 1})
	p := renderOne(t, a, channel.RichDoc{Label: "Codex answer", Markdown: "x"})
	_, err := a.Send(ctx, chat, p, "tok-1")
	th, ok := channel.AsThrottled(err)
	if !ok || th.RetryAfter != 3*time.Second {
		t.Fatalf("err = %v", err)
	}
	if channel.IsAmbiguous(err) {
		t.Fatal("429 must not be ambiguous")
	}
	if n := countRequests(fg, routePost); n != 1 || len(fg.Messages(chat)) != 0 {
		t.Fatalf("posts=%d stored=%d", n, len(fg.Messages(chat)))
	}
}

func TestAcceptedButResponseLostIsAmbiguousAndReadBackFindsMarker(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("lost").ID
	fg.AddFault(fakegraph.Fault{Name: "lost", Kind: fakegraph.FaultAcceptedLost, Route: routePost, Times: 1})
	p := renderOne(t, a, channel.RichDoc{Label: "Codex answer", Markdown: "the answer"})
	start := time.Now()
	_, err := a.Send(ctx, chat, p, "attempt-42")
	if !channel.IsAmbiguous(err) {
		t.Fatalf("err = %v, want Ambiguous", err)
	}
	if n := countRequests(fg, routePost); n != 1 {
		t.Fatalf("POST sent %d times", n)
	}
	back, err := a.ReadBack(ctx, chat, start.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 || back[0].Marker != "attempt-42" || back[0].Hash != p.Hash || !back[0].ByOwner {
		t.Fatalf("read-back = %+v (part hash %s)", back, p.Hash)
	}
	noViolations(t, fg)
}

func TestDroppedBeforeAcceptIsAmbiguousAndAbsent(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("drop").ID
	fg.AddFault(fakegraph.Fault{Name: "drop", Kind: fakegraph.FaultDropBeforeAccept, Route: routePost, Times: 1})
	p := renderOne(t, a, channel.RichDoc{Label: "Codex answer", Markdown: "x"})
	start := time.Now()
	if _, err := a.Send(ctx, chat, p, "attempt-1"); !channel.IsAmbiguous(err) {
		t.Fatalf("err = %v, want Ambiguous (request body was written)", err)
	}
	back, err := a.ReadBack(ctx, chat, start.Add(-time.Minute))
	if err != nil || len(back) != 0 {
		t.Fatalf("read-back = %+v, %v", back, err)
	}
}

func TestConnectionRefusedBeforeWriteIsRejected(t *testing.T) {
	a, err := New(Config{GraphURL: closedURL(t), Tokens: &staticTokens{tok: "x"}, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"body":{"contentType":"html","content":"<p>x</p>"}}`
	_, err = a.Send(context.Background(), "19:chat@thread.v2", channel.Part{Body: body}, "t1")
	var r *channel.Rejected
	if !errors.As(err, &r) || r.Permanent || channel.IsAmbiguous(err) {
		t.Fatalf("err = %v, want transient Rejected", err)
	}
}

func TestWriteStatusClassification(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("status").ID
	p := renderOne(t, a, channel.RichDoc{Label: "L", Markdown: "x"})

	fg.AddFault(fakegraph.Fault{Name: "500", Kind: fakegraph.FaultStatus, Route: routePost, Status: 500, Times: 1})
	if _, err := a.Send(ctx, chat, p, "a"); !channel.IsAmbiguous(err) {
		t.Fatalf("500 on write: %v, want Ambiguous", err)
	}
	fg.AddFault(fakegraph.Fault{Name: "503", Kind: fakegraph.FaultStatus, Route: routePost, Status: 503, RetryAfter: time.Second, Times: 1})
	if _, err := a.Send(ctx, chat, p, "b"); !channel.IsAmbiguous(err) {
		t.Fatalf("503 on write: %v, want Ambiguous", err)
	}
	_, err := a.Send(ctx, "19:missing@thread.v2", p, "c")
	if !isPermanent(err) || !errors.Is(err, channel.ErrNotFound) {
		t.Fatalf("unknown chat: %v", err)
	}
	if _, err := a.Send(ctx, chat, p, "bad token with spaces"); !isPermanent(err) {
		t.Fatalf("bad marker token: %v", err)
	}
	if n := countRequests(fg, routePost); n != 3 {
		t.Fatalf("POST count %d, want 3 (no internal retries)", n)
	}
}

func TestRead5xxIsRetriedInternally(t *testing.T) {
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("r5xx").ID
	fg.AddFault(fakegraph.Fault{Name: "502", Kind: fakegraph.FaultStatus, Route: routeList, Status: 502, Times: 1})
	if _, err := a.Receive(context.Background(), chat, ""); err != nil {
		t.Fatalf("502 then OK: %v", err)
	}
	if n := countRequests(fg, routeList); n != 2 {
		t.Fatalf("list requests %d, want 2", n)
	}
	fg.AddFault(fakegraph.Fault{Name: "503", Kind: fakegraph.FaultStatus, Route: routeList, Status: 503, RetryAfter: 2 * time.Second, Times: 1})
	// 503 with Retry-After on a read is throttling, not a retryable 5xx.
	_, err := a.Receive(context.Background(), chat, "")
	if th, ok := channel.AsThrottled(err); !ok || th.RetryAfter != 2*time.Second {
		t.Fatalf("503+Retry-After on read: %v", err)
	}
}

func TestSendReplyMentionAndGet(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("reply").ID
	q, _ := fg.PostAsUser(chat, "question")
	p := renderOne(t, a, channel.RichDoc{Label: "🤖 ✅ Codex answer", Markdown: "**done**", Mention: true, ReplyTo: q.ID})
	if !p.Mention || p.ReplyTo != q.ID {
		t.Fatalf("part = %+v", p)
	}
	sent, err := a.Send(ctx, chat, p, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	stored := fg.Messages(chat)
	got := stored[len(stored)-1]
	if got.ID != sent.ID || len(got.QuotedIDs) != 1 || got.QuotedIDs[0] != q.ID || len(got.Mentions) != 1 {
		t.Fatalf("stored = %+v", got)
	}
	m, err := a.Get(ctx, chat, sent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !m.MentionsOwner || m.Marker != "m-1" || m.Hash != p.Hash || sent.Time.IsZero() {
		t.Fatalf("get = %+v (part hash %s)", m, p.Hash)
	}
	if _, err := a.Get(ctx, chat, "999"); !errors.Is(err, channel.ErrNotFound) {
		t.Fatalf("missing message: %v", err)
	}
	noViolations(t, fg)
}

func TestMarkerProbe(t *testing.T) {
	for _, strip := range []bool{false, true} {
		fg, a := newFake(t, fakegraph.Options{StripHTMLComments: strip})
		chat := fg.CreateMeetingChatAsUser("probe").ID
		st, sent, err := channel.ProbeMarker(context.Background(), a, chat, channel.RichDoc{Label: "🔧 Helper", Markdown: "marker self-test"}, "probe-1")
		want := channel.MarkerPreserved
		if strip {
			want = channel.MarkerStripped
		}
		if err != nil || st != want || sent.ID == "" {
			t.Fatalf("strip=%v: status=%v err=%v", strip, st, err)
		}
		// Another helper's marker is surfaced on receive.
		if _, err := fg.PostAsOtherHelper(chat, `<p>peer</p><!-- codex-helper-outbox:peer-9 -->`); err != nil {
			t.Fatal(err)
		}
		w, _ := a.Receive(context.Background(), chat, "")
		last := w.Msgs[len(w.Msgs)-1]
		if (last.Marker == "peer-9") == strip || last.Text != "peer" {
			t.Fatalf("strip=%v: peer message %+v", strip, last)
		}
		noViolations(t, fg)
	}
}

func TestCreateChatIdempotentAndRename(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	c1, err := a.CreateChat(ctx, `codex: fix "bug" / now`, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := a.CreateChat(ctx, "other title", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if c1.ID == "" || c1.ID != c2.ID || c1.Title != `codex- fix -bug- - now` {
		t.Fatalf("c1=%+v c2=%+v", c1, c2)
	}
	if c1.URL != ChatURL(c1.ID, fakegraph.TenantID) {
		t.Fatalf("url %s", c1.URL)
	}
	if err := a.RenameChat(ctx, c1.ID, "renamed\x00 title"); err != nil {
		t.Fatal(err)
	}
	info, _ := fg.Chat(c1.ID)
	if info.Topic != "renamed title" {
		t.Fatalf("topic %q", info.Topic)
	}
	if _, err := a.CreateChat(ctx, "x", ""); !isPermanent(err) {
		t.Fatalf("empty key: %v", err)
	}
	noViolations(t, fg)
}

func TestOwnerUnauthorizedRefreshesOnce(t *testing.T) {
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	tokens := &staticTokens{tok: "not-issued"}
	a, _ := New(Config{GraphURL: fg.URL() + "/v1.0", Tokens: tokens, Sleep: noSleep})
	_, _, err := a.Owner(context.Background())
	var ar *channel.AuthRequired
	if !errors.As(err, &ar) || tokens.refreshes != 1 {
		t.Fatalf("err=%v refreshes=%d", err, tokens.refreshes)
	}
	if n := countRequests(fg, "GET /me"); n != 2 {
		t.Fatalf("GET /me sent %d times, want 2", n)
	}
}

// Q1 (owner ruling 2026-11-05): the history check reads a chat one page per
// call, newest first, following nextLink, and compares an answer's
// rendered text (PartText) with the text a read-back message carries.
func TestHistoryPageOnePagePerCall(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("history").ID
	since := time.Now().Add(-time.Minute)
	p := renderOne(t, a, channel.RichDoc{Markdown: "The **answer**: `x := 1` and [a link](https://example.com).", Math: true})
	sent, err := a.Send(ctx, chat, p, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if _, err := fg.PostSystemEvent(chat); err != nil {
			t.Fatal(err)
		}
	}
	var all []channel.InboundMsg
	pos, pages := "", 0
	for {
		before := countRequests(fg, routeList)
		msgs, next, err := a.HistoryPage(ctx, chat, since, pos)
		if err != nil {
			t.Fatal(err)
		}
		if n := countRequests(fg, routeList) - before; n != 1 {
			t.Fatalf("page %d made %d list requests, want 1", pages, n)
		}
		pages++
		all = append(all, msgs...)
		if next == "" {
			break
		}
		pos = next
	}
	if pages < 2 || len(all) != 61 {
		t.Fatalf("%d pages, %d messages; want several pages and 61", pages, len(all))
	}
	last := all[len(all)-1]
	if last.ID != sent.ID || !last.ByOwner || last.Kind != channel.KindMessage {
		t.Fatalf("oldest message = %+v, want the answer %s last (newest first)", last, sent.ID)
	}
	alnum := func(s string) string {
		return strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, s)
	}
	if got, want := alnum(last.Text), alnum(a.PartText(p.Body)); want == "" || got != want {
		t.Fatalf("read-back text %q, rendered text %q", got, want)
	}
	noViolations(t, fg)
}
