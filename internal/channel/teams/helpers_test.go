package teams

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

type staticTokens struct {
	mu        sync.Mutex
	tok       string
	refreshes int
}

func (s *staticTokens) Token(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tok, nil
}

func (s *staticTokens) Refresh(context.Context, string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshes++
	return s.tok, nil
}

func noSleep(context.Context, time.Duration) error { return nil }

// loginHostTransport routes login.microsoftonline.com requests to the fake
// the way the E2E harness transport does.
type loginHostTransport struct{ base http.RoundTripper }

func (t loginHostTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if strings.Contains(r.URL.Path, "/oauth2/") {
		r.Header.Set(fakegraph.HeaderOriginalHost, "login.microsoftonline.com")
	}
	return t.base.RoundTrip(r)
}

func newFake(t *testing.T, opts fakegraph.Options, tweak ...func(*Config)) (*fakegraph.Server, *Adapter) {
	t.Helper()
	fg := fakegraph.New(opts)
	t.Cleanup(fg.Close)
	access, _ := fg.IssueTokens("test")
	cfg := Config{
		GraphURL: fg.URL() + "/v1.0",
		Tokens:   &staticTokens{tok: access},
		TenantID: fakegraph.TenantID,
		Sleep:    noSleep,
	}
	for _, f := range tweak {
		f(&cfg)
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return fg, a
}

func noViolations(t *testing.T, fg *fakegraph.Server) {
	t.Helper()
	for _, v := range fg.Violations() {
		t.Errorf("fakegraph violation: %+v", v)
	}
}

func renderOne(t *testing.T, a *Adapter, doc channel.RichDoc) channel.Part {
	t.Helper()
	parts, err := a.Render(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 {
		t.Fatalf("got %d parts, want 1", len(parts))
	}
	return parts[0]
}

func countRequests(fg *fakegraph.Server, route string) int {
	n := 0
	for _, r := range fg.Requests(fakegraph.RequestFilter{}) {
		if r.Route == route {
			n++
		}
	}
	return n
}

func closedURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return "http://" + addr + "/v1.0"
}

type partBody struct {
	Body struct {
		Content string `json:"content"`
	} `json:"body"`
	Mentions       []json.RawMessage `json:"mentions"`
	HostedContents []struct {
		ID    string `json:"@microsoft.graph.temporaryId"`
		Bytes string `json:"contentBytes"`
	} `json:"hostedContents"`
	Attachments []json.RawMessage `json:"attachments"`
}

func decodePart(t *testing.T, p channel.Part) partBody {
	t.Helper()
	var b partBody
	if err := json.Unmarshal([]byte(p.Body), &b); err != nil {
		t.Fatal(err)
	}
	return b
}
