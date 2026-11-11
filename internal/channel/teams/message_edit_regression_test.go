package teams

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestMessageEditHTMLVariants(t *testing.T) {
	for _, body := range []string{
		`<p>before</p><attachment id='a&amp;b'></attachment><p>after</p>`,
		`<p>before</p><attachment id=a&amp;b></attachment><p>after</p>`,
		`<p>before</p><attachment id="a&amp;b" data-id="wrong"></attachment><p>after</p>`,
	} {
		t.Run(body, func(t *testing.T) {
			next, err := labelEdit(body, true, []graphAttachment{{ID: "a&b", ContentType: "reference"}}, testLabel)
			if err != nil || next != labelHTML(testLabel)+body {
				t.Fatalf("legal HTML changed or refused: %q, %v", next, err)
			}
		})
	}
	for _, body := range []string{
		`<pre><code>https://example/hostedContents/image/$value &lt;attachment id='example'&gt;</code></pre>`,
		`<p><img src="../hostedContents/image/$value" alt='screenshot'></p>`,
		`<p>🧑‍💻 User [part this is an example</p>`,
		`<p><code>🧑‍💻 User:</code></p>`,
	} {
		next, err := labelEdit(body, true, nil, testLabel)
		if err != nil || next != labelHTML(testLabel)+body {
			t.Errorf("body changed or refused: %q, %v", next, err)
		}
	}
}

func TestMessageEditLabelSurvivesHTMLWrappers(t *testing.T) {
	for _, body := range []string{
		`<div><p><strong>🧑‍💻 User:</strong></p><p>request</p></div>`,
		`<!-- normalized --><div><p><span>🧑‍💻 User:</span></p><p>request</p></div>`,
	} {
		next, err := labelEdit(body, true, nil, testLabel)
		if err != nil || next != "" {
			t.Fatalf("normalization duplicated the user label: %s %v", next, err)
		}
	}
}

func TestMessageEditSelfLinkIsNotReceipt(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("self-link").ID
	m, _ := fg.PostAsUser(chat, "request")
	link := "https://teams.microsoft.com/l/message/" + chat + "/" + m.ID + "?context=%7B%22contextType%22%3A%22chat%22%7D"
	body := `<p><a href="` + link + `">Original message</a></p>`
	if err := fg.EditHTMLAsUser(chat, m.ID, body); err != nil {
		t.Fatal(err)
	}
	p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: "Request accepted."})
	if err := a.Annotate(ctx, chat, m.ID, channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:test"}); err != nil {
		t.Fatal(err)
	}
	if got := fg.Messages(chat)[0].Content; !strings.Contains(got, "Request accepted.") || !strings.HasPrefix(got, body) {
		t.Fatal(got)
	}
}

func TestMessageEditPreservesScreenshot(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("screenshot").ID
	m, _ := fg.PostImageAsUser(chat, "look", []byte{0x89, 'P', 'N', 'G'})
	before := fg.Messages(chat)[0]
	if err := a.Annotate(ctx, chat, m.ID, channel.Annotation{Label: testLabel}); err != nil {
		t.Fatal(err)
	}
	after := fg.Messages(chat)[0]
	if after.Content != labelHTML(testLabel)+before.Content || !reflect.DeepEqual(before.Attachments, after.Attachments) || !reflect.DeepEqual(before.Mentions, after.Mentions) {
		t.Fatal("message content lost")
	}
}

func TestMessageEditMergedReceipt(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("merged").ID
	m, _ := fg.PostHTMLAsUser(chat, `<p>original</p><p><img src="../hostedContents/a/$value"></p>`)
	p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: "Request accepted.\n\nReceived at 2026-11-11 15:06:35 UTC+05:30"})
	an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:merged", Label: testLabel}
	if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
		t.Fatal(err)
	}
	got := fg.Messages(chat)[0]
	if got.PatchCount != 1 || !strings.HasPrefix(got.Content, labelHTML(testLabel)+m.Content) {
		t.Fatal(got.Content)
	}
	reads := fg.Requests(fakegraph.RequestFilter{Route: "GET /chats/{chat}/messages/{message}", ChatID: chat})
	if len(reads) != 1 {
		t.Fatalf("combined edit read the message %d times", len(reads))
	}
	if strings.Contains(got.Content, `>🔧 Helper:</a>`) || !strings.Contains(got.Content, `>2026-11-11 15:06:35 UTC+05:30</a>`) {
		t.Fatal(got.Content)
	}
	an.At = time.Now().Add(time.Hour)
	if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
		t.Fatal(err)
	}
	if fg.Messages(chat)[0].PatchCount != 1 {
		t.Fatal("retry rewrote unchanged message")
	}
}

func TestMessageEditEventIdentity(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("pause").ID
	m, _ := fg.PostHTMLAsUser(chat, `<p>Old notice: 🧊 This chat is paused</p>`)
	notice := channel.RichDoc{Label: "🔧 Helper", Markdown: "🧊 This chat is paused"}
	for _, key := range []string{"freeze:1", "freeze:1", "freeze:2", "freeze:2"} {
		if err := a.Annotate(ctx, chat, m.ID, channel.Annotation{Notice: &notice, Key: key, At: time.Unix(1, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := fg.Messages(chat)[0]; got.PatchCount != 2 || strings.Count(got.Content, "Paused at") != 2 {
		t.Fatal(got.Content)
	}
	clips := []channel.Clip{{Name: "voice.ogg", Text: "actual transcript"}}
	v, _ := fg.PostHTMLAsUser(chat, `<blockquote><p>🎙️ old transcript title</p></blockquote>`)
	for range 2 {
		if err := a.Annotate(ctx, chat, v.ID, channel.Annotation{Clips: clips, ClipsLabel: "🎙️ old transcript title", Label: testLabel}); err != nil {
			t.Fatal(err)
		}
	}
	if got := fg.Messages(chat)[1]; got.PatchCount != 1 || strings.Count(got.Content, "actual transcript") != 1 {
		t.Fatal(got.Content)
	}
	for _, text := range []string{"🙂", "😢"} {
		if err := a.Annotate(ctx, chat, v.ID, channel.Annotation{Clips: []channel.Clip{{Name: "voice.ogg", Text: text}}, ClipsLabel: "Transcript"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := fg.Messages(chat)[1]; got.PatchCount != 3 || !strings.Contains(got.Content, "😢") {
		t.Fatal("transcript identity discarded meaningful symbols", got.Content)
	}
}

func TestMessageEditStampScope(t *testing.T) {
	link := annotationLink("chat", "message", "")
	stamp := timeStamp("Received at", link, time.Unix(1, 0))
	for _, body := range []string{"<blockquote>" + stamp + "</blockquote>", "<pre><code>" + stamp + "</code></pre>", `<p><a href="` + link + `">original</a></p>`} {
		if hasAnnotationStamp(body, link, "Received at", "Request accepted.") {
			t.Fatal("ordinary content acknowledged message", body)
		}
	}
	if !hasAnnotationStamp(stamp, link, "Received at", "Request accepted.") {
		t.Fatal("current stamp missing")
	}
	legacy := `<hr/><p><strong><a href="` + link + `">🔧 Helper:</a></strong></p><p>Request accepted.</p>`
	if !hasAnnotationStamp(legacy, link, "Received at", "Request accepted.") {
		t.Fatal("rc.119 receipt missing")
	}
}

func BenchmarkConfirmationRetryLarge(b *testing.B) {
	link := "https://teams.microsoft.com/l/message/chat/id?context=%7B%22contextType%22%3A%22chat%22%7D"
	content := strings.Repeat("<p>long text &amp; a screenshot URL example</p>", 1000) + `<hr/><p><a href="` + link + `"><strong>🔧 Helper:</strong></a></p><p>Request accepted.</p>`
	p := channel.Part{Count: 1, Body: `{"body":{"contentType":"html","content":"<p><strong>🔧 Helper:</strong></p><p>Request accepted.</p>"}}`}
	an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:test"}
	b.ReportAllocs()
	b.SetBytes(int64(len(content)))
	for b.Loop() {
		next, err := confirmationEdit("chat", "id", content, true, nil, an)
		if err != nil || next != "" {
			b.Fatalf("changed: %s %v", next, err)
		}
	}
}
