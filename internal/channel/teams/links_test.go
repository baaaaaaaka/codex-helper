package teams

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestInboundNamedLinks(t *testing.T) {
	for _, tc := range []struct {
		name, body, text string
		links            []channel.Link
	}{
		{"named", `<p>See <a href="https://example.com/task?a=1&amp;b=2"><strong>task</strong></a></p>`, "See task", []channel.Link{{Text: "task", URL: "https://example.com/task?a=1&b=2"}}},
		{"teams", `<a href='https://teams.microsoft.com/l/message/19%3Ax/123'>移交对话</a>`, "移交对话", []channel.Link{{Text: "移交对话", URL: "https://teams.microsoft.com/l/message/19%3Ax/123"}}},
		{"autolink", `<a href="https://example.com">https://example.com</a>`, "https://example.com", nil},
		{"bare", `<p>https://example.com</p>`, "https://example.com", nil},
		{"unsafe", `<a href="javascript:alert(1)">task</a>`, "task", nil},
		{"relative", `<a href="/task">task</a>`, "task", nil},
		{"newline", "<a href='https://example.com/\ntask'>task</a>", "task", nil},
		{"empty", `<a href="https://example.com"></a>`, "", []channel.Link{{URL: "https://example.com"}}},
		{"emoji", `<A HREF="https://example.com"><emoji alt="😀"></emoji> task &amp; notes</A>`, "😀 task & notes", []channel.Link{{Text: "😀 task & notes", URL: "https://example.com"}}},
		{"duplicate", `<a href="https://a.test">A</a> <a href="https://a.test">A</a> <a href="https://b.test">A</a>`, "A A A", []channel.Link{{Text: "A", URL: "https://a.test"}, {Text: "A", URL: "https://b.test"}}},
		{"literal", `&lt;a href="https://example.com"&gt;task&lt;/a&gt;`, `<a href="https://example.com">task</a>`, nil},
		{"comment", `<!-- <a href="https://example.com">hidden</a> --><p>task</p>`, "hidden -->task", nil},
		{"unclosed", `<a href="https://example.com">task`, "task", []channel.Link{{Text: "task", URL: "https://example.com"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gm graphMessage
			gm.Body.ContentType, gm.Body.Content = "html", tc.body
			m := (&Adapter{}).convert("chat", gm, "owner")
			if m.Text != tc.text || !reflect.DeepEqual(m.Links, tc.links) {
				t.Fatalf("text=%q links=%+v", m.Text, m.Links)
			}
			if m.Hash != channel.TextHash(tc.text) {
				t.Fatal("visible-text hash changed")
			}
		})
	}
}

func TestInboundNamedLinksReceiveAndGet(t *testing.T) {
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("links").ID
	posted, err := fg.PostHTMLAsUser(chat, `<p>read <a href="https://example.com/task">task</a></p>`)
	if err != nil {
		t.Fatal(err)
	}
	w, err := a.Receive(context.Background(), chat, "")
	if err != nil || len(w.Msgs) != 1 {
		t.Fatalf("%+v %v", w, err)
	}
	m, err := a.Get(context.Background(), chat, posted.ID)
	if err != nil || !reflect.DeepEqual(m.Links, w.Msgs[0].Links) || len(m.Links) != 1 {
		t.Fatalf("%+v %v", m, err)
	}
	if !strings.Contains(channel.WithLinks(m.Text, m.Links), "task: https://example.com/task") {
		t.Fatal(m)
	}
	noViolations(t, fg)
}

func TestQuoteNamedLinks(t *testing.T) {
	for _, preview := range []string{"", "short preview"} {
		at := graphAttachment{Content: `{"originalMessageContent":"<p>Read <a href=\"https://example.com/task\">task</a></p>","messagePreview":"` + preview + `"}`}
		q := quoteFrom(at)
		if !strings.Contains(q.Text, "task: https://example.com/task") {
			t.Fatal(q)
		}
		if preview != "" && !strings.HasPrefix(q.Text, preview) {
			t.Fatal(q)
		}
	}
}

func BenchmarkInboundLinkConversion(b *testing.B) {
	for name, body := range map[string]string{
		"ordinary": `<p>please check the task</p>`,
		"rich":     strings.Repeat(`<p>See <a href="https://example.com/task?a=1&amp;b=2"><b>task</b></a> and notes</p>`, 1000),
	} {
		b.Run(name, func(b *testing.B) {
			b.Run("before", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					plainText(body)
				}
			})
			b.Run("after", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					plainText(body)
					namedLinks(body)
				}
			})
		})
	}
}

func BenchmarkInboundNamedLinks(b *testing.B) {
	for name, body := range map[string]string{
		"ordinary": `<p>please check the task</p>`,
		"long":     strings.Repeat(`<p>See <a href="https://example.com/task?a=1&amp;b=2"><b>task</b></a> and notes</p>`, 1000),
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				namedLinks(body)
			}
		})
	}
}
