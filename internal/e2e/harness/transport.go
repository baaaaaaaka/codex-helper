package harness

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// ErrForeignHost is returned for any request to a host other than Graph or
// the Microsoft login endpoint.
var ErrForeignHost = errors.New("e2e transport: request to a foreign host blocked")

// ErrInstanceKilled is returned for every request of a crashed SUT instance.
var ErrInstanceKilled = errors.New("e2e transport: SUT instance was killed")

// Transport rewrites graph.microsoft.com and login.microsoftonline.com to the
// fake server and fails everything else. One Transport belongs to one SUT
// instance so a crash can cut exactly that instance off the network.
type Transport struct {
	graph    *fakegraph.Server
	target   *url.URL
	instance string
	inner    *http.Transport
	killed   atomic.Bool
}

// NewTransport builds a sandbox transport for one SUT instance.
func NewTransport(g *fakegraph.Server, instance string) *Transport {
	target, err := url.Parse(g.URL())
	if err != nil {
		panic(err)
	}
	return &Transport{
		graph:    g,
		target:   target,
		instance: instance,
		inner: &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     30 * time.Second,
		},
	}
}

// Client returns an http.Client using this transport. The timeout mirrors
// the production Graph client's whole-request timeout.
func (tr *Transport) Client() *http.Client {
	return &http.Client{Transport: tr, Timeout: 30 * time.Second}
}

// Kill makes every future request of this instance fail immediately and
// closes idle connections.
func (tr *Transport) Kill() {
	tr.killed.Store(true)
	tr.inner.CloseIdleConnections()
}

// RoundTrip implements http.RoundTripper.
func (tr *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if tr.killed.Load() {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, ErrInstanceKilled
	}
	host := strings.ToLower(req.URL.Hostname())
	switch host {
	case "graph.microsoft.com", "login.microsoftonline.com":
	default:
		tr.graph.RecordBlocked(fakegraph.BlockedRequest{At: time.Now(), Instance: tr.instance, URL: req.URL.Redacted()})
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("%w: %s", ErrForeignHost, req.URL.Host)
	}
	if req.URL.Scheme != "https" {
		tr.graph.RecordBlocked(fakegraph.BlockedRequest{At: time.Now(), Instance: tr.instance, URL: req.URL.Redacted()})
		return nil, fmt.Errorf("%w: non-https scheme %s", ErrForeignHost, req.URL.Scheme)
	}
	out := req.Clone(req.Context())
	u := *req.URL
	u.Scheme = tr.target.Scheme
	u.Host = tr.target.Host
	out.URL = &u
	out.Host = tr.target.Host
	out.Header.Set(fakegraph.HeaderOriginalHost, host)
	out.Header.Set(fakegraph.HeaderInstance, tr.instance)
	return tr.inner.RoundTrip(out)
}
