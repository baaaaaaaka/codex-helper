package harness

import (
	"errors"
	"net/http"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestTransportRewritesGraphAndBlocksForeignHosts(t *testing.T) {
	g := fakegraph.New(fakegraph.Options{})
	defer g.Close()
	access, _ := g.IssueTokens("t")
	tr := NewTransport(g, "inst-1")
	client := tr.Client()

	req, _ := http.NewRequest("GET", "https://graph.microsoft.com/v1.0/me", nil)
	req.Header.Set("Authorization", "Bearer "+access)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	reqs := g.Requests(fakegraph.RequestFilter{})
	if len(reqs) != 1 || reqs[0].Instance != "inst-1" || reqs[0].Route != "GET /me" {
		t.Fatalf("requests = %+v", reqs)
	}

	_, err = client.Get("https://api.github.com/repos")
	if !errors.Is(err, ErrForeignHost) {
		t.Fatalf("foreign host err = %v", err)
	}
	if len(g.Blocked()) != 1 {
		t.Fatalf("blocked = %v", g.Blocked())
	}

	tr.Kill()
	if _, err := client.Do(req); !errors.Is(err, ErrInstanceKilled) {
		t.Fatalf("after kill err = %v", err)
	}
}
