package teams

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Review M2: a stalled Graph connection fails the call after
// RequestTimeout instead of pinning the chat until restart.
func TestStalledRequestTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	a, err := New(Config{GraphURL: srv.URL + "/v1.0", Tokens: &staticTokens{tok: "t"}, Sleep: noSleep, RequestTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = a.Receive(context.Background(), "19:chat@thread.v2", "")
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Receive took %s with a 100ms request timeout", d)
	}
	var rej *channel.Rejected
	if !errors.As(err, &rej) || rej.Permanent {
		t.Fatalf("stalled read: %v, want a transient Rejected", err)
	}
}
