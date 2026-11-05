package cli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type closeTracker struct {
	io.Reader
	closed atomic.Bool
}

func (c *closeTracker) Close() error { c.closed.Store(true); return nil }

// Graph writes pass the owner fence; reads do not. A fenced, released or
// disabled owner writes nothing; a write waits out a busy store.
func TestTeamsFencedHTTPClientGatesWritesOnly(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	old := teamsFenceRetry
	teamsFenceRetry = 10 * time.Millisecond
	defer func() { teamsFenceRetry = old }()

	type fv struct{ err error }
	var verdict atomic.Pointer[fv]
	verdict.Store(&fv{corestore.ErrFenced})
	calls := atomic.Int32{}
	fence := func() error {
		calls.Add(1)
		return verdict.Load().err
	}
	c := teamsFencedHTTPClient(srv.Client(), fence)

	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 0 || hits.Load() != 1 {
		t.Fatalf("GET: %d fence checks, %d requests; want 0 and 1", calls.Load(), hits.Load())
	}

	for _, refusal := range []error{corestore.ErrFenced, corestore.ErrNotAcquired, corestore.ErrDisabled} {
		verdict.Store(&fv{refusal})
		body := &closeTracker{Reader: strings.NewReader("{}")}
		req, _ := http.NewRequest(http.MethodPost, srv.URL, body)
		_, err := c.Do(req)
		if !errors.Is(err, refusal) {
			t.Fatalf("POST with fence %v: error %v", refusal, err)
		}
		if !body.closed.Load() {
			t.Fatalf("POST with fence %v: request body not closed", refusal)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("a refused write reached the server (%d requests)", hits.Load())
	}

	// A busy store: the write waits and goes once the renewal succeeds.
	verdict.Store(&fv{errors.New("database is locked (5) (SQLITE_BUSY)")})
	go func() {
		time.Sleep(50 * time.Millisecond)
		verdict.Store(&fv{})
	}()
	before := calls.Load()
	req, _ := http.NewRequest(http.MethodPatch, srv.URL, strings.NewReader("{}"))
	resp, err = c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if hits.Load() != 2 || calls.Load()-before < 2 {
		t.Fatalf("busy then free: %d requests, %d fence checks", hits.Load(), calls.Load()-before)
	}

	// A busy store and a request that gives up: nothing is sent.
	verdict.Store(&fv{errors.New("busy")})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	req, _ = http.NewRequestWithContext(ctx, http.MethodDelete, srv.URL, nil)
	if _, err := c.Do(req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("busy store, request deadline: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("a write was sent while the fence could not be checked (%d requests)", hits.Load())
	}
}
