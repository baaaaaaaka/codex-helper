package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
)

type teamsGraphRoundTripFunc func(*http.Request) (*http.Response, error)

func (f teamsGraphRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestTeamsGraphRecoveryRefreshesBeforeSwitchAndDrainsInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var refreshes, writes atomic.Int32
	var oldOAuthBroken atomic.Bool
	newBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/token"):
			refreshes.Add(1)
			fmt.Fprint(w, `{"access_token":"new-access","expires_in":3600}`)
		case r.URL.Path == "/v1.0/me/chats" && r.Header.Get("Authorization") == "Bearer new-access":
			fmt.Fprint(w, `{"value":[]}`)
		case r.Method == http.MethodGet && r.Header.Get("Authorization") == "Bearer new-access":
			fmt.Fprint(w, `{"id":"owner","displayName":"Owner"}`)
		case r.Method == http.MethodPost:
			writes.Add(1)
			fmt.Fprint(w, `{}`)
		default:
			http.Error(w, "bad auth", http.StatusUnauthorized)
		}
	}))
	defer newBackend.Close()
	address, err := url.Parse(newBackend.URL)
	if err != nil {
		t.Fatal(err)
	}
	candidate := &http.Client{Transport: teamsGraphRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		req = req.Clone(req.Context())
		req.URL.Scheme, req.URL.Host = address.Scheme, address.Host
		return http.DefaultTransport.RoundTrip(req)
	})}

	// The old route can still answer a Graph read while the OAuth
	// connection fails. Leave a response open to hold the old lease.
	oldClosed := make(chan struct{})
	old := &http.Client{Transport: teamsGraphRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/token") && oldOAuthBroken.Load() {
			return nil, teamsGraphConnectError{status: http.StatusBadGateway}
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}
	cfg := chteams.AuthConfig{TenantID: "tenant", ClientID: "client",
		LoginURL: "https://oauth.invalid", Cache: chteams.TokenLocation{Database: filepath.Join(t.TempDir(), "auth.sqlite"), Slot: "full"}}
	if err := chteams.WriteTokenCache(cfg.Cache, chteams.TokenCache{RefreshToken: "old-refresh"}); err != nil {
		t.Fatal(err)
	}

	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	var starts atomic.Int32
	recovery := newTeamsGraphRecovery(ctx, teamsGraphHTTPClientLease{Client: old, close: func(context.Context) error {
		close(oldClosed)
		return nil
	}}, func(context.Context) (teamsGraphHTTPClientLease, error) {
		starts.Add(1)
		return teamsGraphHTTPClientLease{Client: candidate}, nil
	}, func(ctx context.Context, client *http.Client, kind teamsGraphRequestKind) error {
		if kind != teamsGraphFullAuth {
			return fmt.Errorf("unexpected failure kind %d", kind)
		}
		return teamsGraphRecoveryProbe(ctx, client, cfg)
	}, func() time.Time { return time.Unix(0, now.Load()) }, time.Minute, io.Discard)
	defer recovery.close()

	graph := recovery.client(teamsGraphRead, time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := graph.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	oldOAuthBroken.Store(true)
	auth := chteams.NewDelegatedAuth(cfg, recovery.client(teamsGraphFullAuth, time.Second))
	defer auth.Close()
	for i := 0; i < 2; i++ {
		_, err := auth.Token(ctx)
		var gateway teamsGraphConnectError
		if !errors.As(err, &gateway) || gateway.status != http.StatusBadGateway {
			t.Fatalf("refresh %d: %v", i, err)
		}
		if i == 0 {
			probe, err := graph.Do(req.Clone(ctx))
			if err != nil {
				t.Fatal(err)
			}
			probe.Body.Close()
			now.Add(int64(time.Minute))
		}
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		recovery.mu.Lock()
		switched := recovery.route.lease.Client == candidate
		recovery.mu.Unlock()
		if switched {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("no switch: candidates=%d OAuth POSTs=%d", starts.Load(), refreshes.Load())
		}
	}
	if starts.Load() != 1 || refreshes.Load() != 1 {
		t.Fatalf("starts=%d refreshes=%d, want one each", starts.Load(), refreshes.Load())
	}
	if since := recovery.degradedSince(time.Time{}); since.IsZero() {
		t.Fatal("route switch cleared degradation before the bridge polled")
	}
	if since := recovery.degradedSince(time.Unix(0, now.Load()).Add(time.Second)); !since.IsZero() {
		t.Fatalf("successful poll did not clear degradation: %s", since)
	}
	select {
	case <-oldClosed:
		t.Fatal("closed the original route with a response still in flight")
	default:
	}
	if token, err := auth.Token(ctx); err != nil || token != "new-access" {
		t.Fatalf("running auth did not recover: %q, %v", token, err)
	}
	send, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://graph.microsoft.com/v1.0/chats/id/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	sent, err := graph.Do(send)
	if err != nil {
		t.Fatal(err)
	}
	sent.Body.Close()
	if writes.Load() != 1 {
		t.Fatalf("writes = %d, want one", writes.Load())
	}
	res.Body.Close()
	select {
	case <-oldClosed:
	case <-ctx.Done():
		t.Fatal("old lease was not closed after the response drained")
	}
}

func TestTeamsGraphRecoveryFailureAndThrottle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var now, status atomic.Int64
	now.Store(time.Now().UnixNano())
	status.Store(http.StatusTooManyRequests)
	client := &http.Client{Transport: teamsGraphRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: int(status.Load()), Body: http.NoBody}, nil
	})}
	var starts atomic.Int32
	finishedCandidate := make(chan struct{}, 2)
	r := newTeamsGraphRecovery(ctx, teamsGraphHTTPClientLease{Client: client}, func(context.Context) (teamsGraphHTTPClientLease, error) {
		starts.Add(1)
		return teamsGraphHTTPClientLease{Client: client, close: func(context.Context) error {
			finishedCandidate <- struct{}{}
			return nil
		}}, nil
	}, func(context.Context, *http.Client, teamsGraphRequestKind) error {
		return errors.New("candidate could not authenticate")
	}, func() time.Time { return time.Unix(0, now.Load()) }, time.Minute, io.Discard)
	defer r.close()
	get := func() {
		t.Helper()
		resp, err := r.client(teamsGraphRead, time.Second).Get("https://graph.microsoft.com/v1.0/me")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	get()
	now.Add(int64(time.Minute))
	get()
	if !r.degradedSince(time.Time{}).IsZero() {
		t.Fatal("429 triggered degradation")
	}
	status.Store(http.StatusBadGateway)
	get()
	now.Add(int64(time.Minute))
	get()
	select {
	case <-finishedCandidate:
	case <-ctx.Done():
		t.Fatal("failed candidate was not cleaned up")
	}
	get()
	r.mu.Lock()
	inBackoff := time.Unix(0, now.Load()).Before(r.next)
	r.mu.Unlock()
	if starts.Load() != 1 || !inBackoff {
		t.Fatalf("starts=%d backoff=%t", starts.Load(), inBackoff)
	}
	status.Store(http.StatusOK)
	get()
	if !r.degradedSince(time.Time{}).IsZero() {
		t.Fatal("original route recovered but degradation remained")
	}
}

func TestTeamsGraphRecoveryDetectsProxyConnect502(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodConnect {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		http.Error(w, "expected CONNECT", http.StatusBadRequest)
	}))
	defer proxy.Close()
	client, err := newTeamsProxyHTTPClient(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = client.Post("https://login.microsoftonline.com/tenant/oauth2/v2.0/token", "application/x-www-form-urlencoded", strings.NewReader("grant_type=refresh_token"))
	var gateway teamsGraphConnectError
	if !errors.As(err, &gateway) || gateway.status != http.StatusBadGateway || !strings.HasSuffix(err.Error(), "Bad Gateway") {
		t.Fatalf("CONNECT 502 classification changed: %v", err)
	}
}
