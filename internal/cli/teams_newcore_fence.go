package cli

import (
	"context"
	"errors"
	"net/http"
	"time"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Every Graph write (and Workflow card post) of the new core passes the
// store's owner fence first (corestore.Store.Fence): a process that was
// stopped past its lease (frozen, suspended) re-proves its ownership before
// it posts anything, and one that a standby took over from posts nothing.
// Reads (GET, HEAD) are not gated: they change nothing a user can see.
//
// The check runs in the transport, as late as possible before the request
// is written, and a refusal is returned before anything was written, so the
// Graph adapter classifies it as not sent (never as an unknown outcome).

// teamsFenceRetry is how long a write waits before checking the fence
// again when the renewal failed for a reason other than the fence (the
// store busy beyond busy_timeout).
var teamsFenceRetry = time.Second

type teamsFenceTransport struct {
	base  http.RoundTripper
	fence func() error
}

// teamsFencedHTTPClient returns a copy of c whose writes pass fence.
func teamsFencedHTTPClient(c *http.Client, fence func() error) *http.Client {
	out := *c
	base := c.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	out.Transport = teamsFenceTransport{base: base, fence: fence}
	return &out
}

func (t teamsFenceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		if err := teamsWaitFence(req.Context(), t.fence); err != nil {
			if req.Body != nil {
				_ = req.Body.Close()
			}
			return nil, err
		}
	}
	return t.base.RoundTrip(req)
}

// teamsWaitFence returns nil once fence allows a write, or the fence's
// verdict (fenced, disabled, released), or ctx's error.
func teamsWaitFence(ctx context.Context, fence func() error) error {
	for {
		err := fence()
		if err == nil || errors.Is(err, corestore.ErrFenced) || errors.Is(err, corestore.ErrNotAcquired) ||
			errors.Is(err, corestore.ErrDisabled) {
			return err
		}
		t := time.NewTimer(teamsFenceRetry)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}
