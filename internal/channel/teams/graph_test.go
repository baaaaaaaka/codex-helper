package teams

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

func TestRetryAfterParsing(t *testing.T) {
	h := func(v string) http.Header { return http.Header{"Retry-After": {v}} }
	cases := []struct {
		v    string
		want time.Duration
	}{
		{"", defaultRetryAfter},
		{"0", 0},
		{"12", 12 * time.Second},
		{" 5 ", 5 * time.Second},
		{"-3", defaultRetryAfter},
		{"999999999999", maxRetryAfter},
		{"soon", defaultRetryAfter},
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0}, // in the past
	}
	for _, c := range cases {
		if got := retryAfter(h(c.v), defaultRetryAfter); got != c.want {
			t.Errorf("Retry-After %q = %v, want %v", c.v, got, c.want)
		}
	}
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if got := retryAfter(h(future), 0); got < 80*time.Second || got > 91*time.Second {
		t.Errorf("HTTP-date Retry-After = %v", got)
	}
	far := time.Now().Add(48 * time.Hour).UTC().Format(http.TimeFormat)
	if got := retryAfter(h(far), 0); got != maxRetryAfter {
		t.Errorf("far HTTP-date = %v", got)
	}
}

func TestThrottleScopeHeaders(t *testing.T) {
	cases := map[string]channel.ThrottleScope{
		"":                    channel.ScopeUnknown,
		"chat":                channel.ScopeChat,
		"Thread":              channel.ScopeChat,
		"account":             channel.ScopeAccount,
		"global":              channel.ScopeAccount,
		"Tenant_Application/": channel.ScopeAccount,
		"something-else":      channel.ScopeUnknown,
	}
	for v, want := range cases {
		for _, key := range []string{"X-MS-Throttle-Scope", "X-CXP-RateLimit-Scope"} {
			h := http.Header{}
			h.Set(key, v)
			if got := throttleScope(h); got != want {
				t.Errorf("%s=%q: %v, want %v", key, v, got, want)
			}
		}
	}
}

func TestThrottledScopeFromServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Retry-After", "4")
		w.Header().Set("X-MS-Throttle-Scope", "account")
		w.WriteHeader(429)
	}))
	defer srv.Close()
	a, _ := New(Config{GraphURL: srv.URL + "/v1.0", Tokens: &staticTokens{tok: "x"}, Sleep: noSleep})
	_, err := a.Send(context.Background(), "c", channel.Part{Body: `{"body":{"contentType":"html","content":"x"}}`}, "")
	th, ok := channel.AsThrottled(err)
	if !ok || th.Scope != channel.ScopeAccount || th.RetryAfter != 4*time.Second {
		t.Fatalf("err = %v", err)
	}
}

func TestUnparseableAcceptedResponseIsAmbiguous(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(201)
		io.WriteString(w, "{not json")
	}))
	defer srv.Close()
	a, _ := New(Config{GraphURL: srv.URL + "/v1.0", Tokens: &staticTokens{tok: "x"}, Sleep: noSleep})
	_, err := a.Send(context.Background(), "c", channel.Part{Body: `{"body":{"contentType":"html","content":"x"}}`}, "t")
	if !channel.IsAmbiguous(err) {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveNextLink(t *testing.T) {
	base, _ := url.Parse("http://127.0.0.1:9/v1.0")
	g := &graph{base: base}
	got, err := g.resolve("https://graph.microsoft.com/v1.0/chats/19%3Aa%40thread.v2/messages?$top=50&$skiptoken=abc")
	if err != nil || got != "http://127.0.0.1:9/v1.0/chats/19%3Aa%40thread.v2/messages?$top=50&$skiptoken=abc" {
		t.Fatalf("got %q %v", got, err)
	}
	for _, bad := range []string{"https://evil.example/v1.0/me", "https://graph.microsoft.com/beta/me", "not a url"} {
		if _, err := g.resolve(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if got, _ := g.resolve("/me"); got != "http://127.0.0.1:9/v1.0/me" {
		t.Fatalf("relative = %q", got)
	}
	if _, err := g.resolveCall(call{url: "http://uploads.example/x", noAuth: true}); err == nil {
		t.Fatal("plain http pre-authenticated url on a foreign host accepted")
	}
	if got := redactPath("https://t.sharepoint.com/_api/upload?guid=1&tempauth=SECRET"); strings.Contains(got, "SECRET") {
		t.Fatalf("redacted %q", got)
	}
}

func TestWithMarker(t *testing.T) {
	raw, err := withMarker(`{"body":{"contentType":"html","content":"<p>x</p>"},"mentions":[]}`, "a.b:c@d")
	if err != nil {
		t.Fatal(err)
	}
	var b partBody
	if err := json.Unmarshal(raw, &b); err != nil || b.Body.Content != "<!-- codex-helper-outbox:a.b:c@d --><p>x</p>" || b.Mentions == nil {
		t.Fatalf("got %s", raw)
	}
	if m := markerRE.FindStringSubmatch("<!-- codex-helper-outbox:a.b:c@d --><p>x</p>"); m == nil || m[1] != "a.b:c@d" {
		t.Fatal("marker regex")
	}
}
