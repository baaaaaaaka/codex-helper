package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
)

const (
	teamsGraphFailureWindow = time.Minute
	teamsGraphRetryMax      = 5 * time.Minute
	teamsGraphProbeTimeout  = 45 * time.Second
)

type teamsGraphConnectError struct{ status int }

func (e teamsGraphConnectError) Error() string { return http.StatusText(e.status) }

func teamsGraphGatewayFailure(status int) bool {
	return status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

type teamsGraphRequestKind int

const (
	teamsGraphRead teamsGraphRequestKind = iota
	teamsGraphFullAuth
	teamsGraphReadAuth
	teamsGraphRequestKinds
)

type teamsGraphRoute struct {
	lease    teamsGraphHTTPClientLease
	inFlight int
	drained  chan struct{} // set when this route stops admitting requests
}

type teamsGraphRecovery struct {
	mu                         sync.Mutex
	route                      *teamsGraphRoute
	failing                    [teamsGraphRequestKinds]time.Time
	confirmed                  [teamsGraphRequestKinds]bool
	awaitPollSince, switchedAt time.Time
	next                       time.Time
	backoff                    time.Duration
	wake                       chan struct{}
	done                       chan struct{}
	cancel                     context.CancelFunc
	now                        func() time.Time
	window                     time.Duration
	start                      func(context.Context) (teamsGraphHTTPClientLease, error)
	probe                      func(context.Context, *http.Client, teamsGraphRequestKind) error
	log                        io.Writer
}

func newTeamsGraphRecovery(ctx context.Context, initial teamsGraphHTTPClientLease,
	start func(context.Context) (teamsGraphHTTPClientLease, error),
	probe func(context.Context, *http.Client, teamsGraphRequestKind) error,
	now func() time.Time, window time.Duration, log io.Writer) *teamsGraphRecovery {
	ctx, cancel := context.WithCancel(ctx)
	r := &teamsGraphRecovery{
		route: &teamsGraphRoute{lease: initial}, wake: make(chan struct{}, 1), done: make(chan struct{}),
		cancel: cancel, now: now, window: window, start: start, probe: probe, log: log,
	}
	go r.run(ctx)
	return r
}

// degradedSince reports a confirmed outage. Following a switch, wait for
// the bridge itself to complete a poll before clearing the last outage.
func (r *teamsGraphRecovery) degradedSince(lastRead time.Time) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lastRead.After(r.switchedAt) {
		r.awaitPollSince = time.Time{}
	}
	oldest := r.awaitPollSince
	for kind, since := range r.failing {
		if r.confirmed[kind] && (oldest.IsZero() || since.Before(oldest)) {
			oldest = since
		}
	}
	return oldest
}

func (r *teamsGraphRecovery) close() {
	r.cancel()
	<-r.done
	_ = r.route.lease.Close(context.Background())
}

type teamsGraphRecoveryTransport struct {
	recovery *teamsGraphRecovery
	kind     teamsGraphRequestKind
}

func (r *teamsGraphRecovery) client(kind teamsGraphRequestKind, timeout time.Duration) *http.Client {
	return &http.Client{Transport: teamsGraphRecoveryTransport{r, kind}, Timeout: timeout}
}

func (t teamsGraphRecoveryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := t.recovery
	r.mu.Lock()
	route := r.route
	route.inFlight++
	r.mu.Unlock()

	resp, err := route.lease.Client.Transport.RoundTrip(req)
	if t.observes(req) {
		r.observe(route, t.kind, resp, err)
	}
	if err != nil || resp.Body == nil || resp.Body == http.NoBody {
		r.release(route)
	} else {
		resp.Body = &teamsGraphResponseBody{ReadCloser: resp.Body, release: func() { r.release(route) }}
	}
	return resp, err
}

func (t teamsGraphRecoveryTransport) observes(req *http.Request) bool {
	if t.kind == teamsGraphRead {
		return req.Method == http.MethodGet && strings.EqualFold(req.URL.Hostname(), "graph.microsoft.com")
	}
	return req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/oauth2/v2.0/token")
}

type teamsGraphResponseBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *teamsGraphResponseBody) Close() error {
	defer b.once.Do(b.release)
	return b.ReadCloser.Close()
}

func (r *teamsGraphRecovery) release(route *teamsGraphRoute) {
	r.mu.Lock()
	defer r.mu.Unlock()
	route.inFlight--
	if route.inFlight == 0 && route.drained != nil {
		close(route.drained)
	}
}

func (r *teamsGraphRecovery) observe(route *teamsGraphRoute, kind teamsGraphRequestKind, resp *http.Response, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	var connect teamsGraphConnectError
	var network net.Error
	failed := (resp != nil && teamsGraphGatewayFailure(resp.StatusCode)) ||
		errors.As(err, &connect) || errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)

	r.mu.Lock()
	defer r.mu.Unlock()
	if route != r.route {
		return
	}
	if !failed {
		r.failing[kind], r.confirmed[kind] = time.Time{}, false
		if r.failing == [teamsGraphRequestKinds]time.Time{} {
			r.next, r.backoff = time.Time{}, 0
		}
		return
	}
	now := r.now()
	if r.failing[kind].IsZero() {
		r.failing[kind] = now
		return
	}
	if now.Sub(r.failing[kind]) >= r.window {
		r.confirmed[kind] = true
	}
	if r.confirmed[kind] && !now.Before(r.next) {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}
}

// A single worker owns replacement. Requests never wait for it or replay on
// a second route; in particular, the outcome of a Graph write stays with the
// original request and the durable outbox.
func (r *teamsGraphRecovery) run(ctx context.Context) {
	defer close(r.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
			r.recover(ctx)
		}
	}
}

func (r *teamsGraphRecovery) recover(ctx context.Context) {
	r.mu.Lock()
	now := r.now()
	kind := teamsGraphRequestKinds
	// Probe the failing credentials first. A successful Graph read with
	// another credential does not demonstrate that OAuth has recovered.
	for k := teamsGraphReadAuth; k >= teamsGraphRead; k-- {
		if r.confirmed[k] {
			kind = k
			break
		}
	}
	if ctx.Err() != nil || kind == teamsGraphRequestKinds || now.Before(r.next) {
		r.mu.Unlock()
		return
	}
	failure := r.failing[kind]
	r.mu.Unlock()

	startCtx, cancelStart := context.WithTimeout(ctx, teamsGraphProbeTimeout)
	candidate, err := r.start(startCtx)
	cancelStart()
	if err == nil {
		probeCtx, cancelProbe := context.WithTimeout(ctx, teamsGraphProbeTimeout)
		err = r.probe(probeCtx, candidate.Client, kind)
		cancelProbe()
	}

	r.mu.Lock()
	if err != nil || ctx.Err() != nil || r.failing[kind] != failure {
		retry := err != nil && ctx.Err() == nil && r.failing[kind] == failure
		if retry {
			r.backoff = min(teamsGraphRetryMax, max(r.window, 2*r.backoff))
			r.next = r.now().Add(r.backoff)
		}
		delay := r.backoff
		r.mu.Unlock()
		_ = candidate.Close(context.Background())
		if retry && r.log != nil {
			// Errors from authentication can include the tenant URL. Keep
			// recovery logs to the phase and retry timing.
			_, _ = fmt.Fprintf(r.log, "Teams Graph proxy: replacement unavailable; retry in %s if failures continue\n", delay)
		}
		return
	}

	old := r.route
	old.drained = make(chan struct{})
	if old.inFlight == 0 {
		close(old.drained)
	}
	r.route = &teamsGraphRoute{lease: candidate}
	r.awaitPollSince, r.switchedAt = failure, r.now()
	r.failing = [teamsGraphRequestKinds]time.Time{}
	r.confirmed = [teamsGraphRequestKinds]bool{}
	r.next, r.backoff = time.Time{}, 0
	r.mu.Unlock()
	if r.log != nil {
		_, _ = fmt.Fprintf(r.log, "Teams Graph proxy: switched to validated route %s\n", candidate.ProxyURL)
	}
	select {
	case <-old.drained:
	case <-ctx.Done():
	}
	_ = old.lease.Close(context.Background())
}

func teamsGraphRecoveryProbe(ctx context.Context, client *http.Client, auth chteams.AuthConfig) error {
	// A new adapter cannot satisfy /me from its in-memory cache. Its
	// delegated auth still uses the shared token lock and refreshes only
	// when needed, so it cannot consume a rotated token concurrently.
	probe, err := chteams.New(chteams.Config{HTTP: client, Tokens: chteams.NewDelegatedAuth(auth, client), MaxPages: 1})
	if err != nil {
		return err
	}
	defer probe.Close()
	_, err = probe.ScanActivity(ctx, time.Now())
	return err
}
