package teams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

type routeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *routeClock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *routeClock) add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }
func (c *routeClock) sleep(ctx context.Context, d time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.add(d)
	return nil
}

type routeTransport func(*http.Request) *http.Response

func (f routeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r), nil }

type routeErrorTransport func(*http.Request) (*http.Response, error)

func (f routeErrorTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func routeResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"62"}}, Body: io.NopCloser(strings.NewReader(body))}
}
func routeJWT(user, tenant, app string) string {
	b, _ := json.Marshal(map[string]string{"oid": user, "tid": tenant, "appid": app})
	return "e30." + base64.RawURLEncoding.EncodeToString(b) + ".test"
}
func routeApp(r *http.Request) string {
	id, _ := identityOf(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	return id.App
}
func newRouteAdapter(t *testing.T, fn routeTransport) (*Adapter, *routeClock) {
	t.Helper()
	c := &routeClock{t: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)}
	a, err := New(Config{ReadTokens: &staticTokens{tok: routeJWT("owner", "tenant", "read")}, Tokens: &staticTokens{tok: routeJWT("owner", "tenant", "full")}, ReadFallback: true, HTTP: &http.Client{Transport: fn}, Now: c.now, Sleep: c.sleep})
	if err != nil {
		t.Fatal(err)
	}
	return a, c
}
func monitorCall(g *graph, ctx context.Context) error {
	_, err := g.do(ctx, call{method: http.MethodGet, url: "/chats/c/messages", monitor: true})
	return err
}

func TestReadFallbackCooldownRecoveryAndNoStorm(t *testing.T) {
	var reads, full atomic.Int32
	var fullTimes []time.Time
	var clock *routeClock
	a, c := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "read" {
			if reads.Add(1) == 1 {
				return routeResponse(429, "")
			}
			return routeResponse(200, `{}`)
		}
		full.Add(1)
		fullTimes = append(fullTimes, clock.now())
		return routeResponse(200, `{}`)
	})
	clock = c
	for i := 0; i < 10; i++ {
		if err := monitorCall(a.g, context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if reads.Load() != 1 || full.Load() != 10 {
		t.Fatalf("read=%d full=%d", reads.Load(), full.Load())
	}
	for i := 1; i < len(fullTimes); i++ {
		if fullTimes[i].Sub(fullTimes[i-1]) < time.Second {
			t.Fatal("borrowed HTTP requests burst")
		}
	}
	c.add(62 * time.Second)
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 3 || full.Load() != 10 {
		t.Fatalf("recovery read=%d full=%d", reads.Load(), full.Load())
	}
}

func TestReadFallbackBothThrottledWaitsForEarliestAndDoesNotPoll(t *testing.T) {
	var reads, full atomic.Int32
	a, c := newRouteAdapter(t, func(r *http.Request) *http.Response {
		out := routeResponse(429, "")
		if routeApp(r) == "read" {
			reads.Add(1)
		} else {
			full.Add(1)
			out.Header.Set("Retry-After", "10")
		}
		return out
	})
	for i := 0; i < 100; i++ {
		err := monitorCall(a.g, context.Background())
		th, ok := channel.AsThrottled(err)
		if !ok || th.RetryAfter != 10*time.Second {
			t.Fatalf("%d: %v", i, err)
		}
	}
	if reads.Load() != 1 || full.Load() != 1 {
		t.Fatalf("storm: read=%d full=%d", reads.Load(), full.Load())
	}
	c.add(11 * time.Second)
	_ = monitorCall(a.g, context.Background())
	if reads.Load() != 1 || full.Load() != 2 {
		t.Fatalf("early recovery read=%d full=%d", reads.Load(), full.Load())
	}
}

func TestReadFallbackOnlyUnknown429AndMonitorReads(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		scope   string
		monitor bool
		write   bool
		noAuth  bool
	}{
		{"unknown429", 429, "", true, false, false}, {"chat", 429, "chat", true, false, false},
		{"account", 429, "account", true, false, false}, {"service503", 503, "", true, false, false},
		{"permission", 403, "", true, false, false}, {"generic", 429, "", false, false, false},
		{"write", 429, "", true, true, false}, {"preauthenticated", 429, "", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var borrowed atomic.Int32
			a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
				if routeApp(r) == "full" && r.Method == http.MethodGet {
					borrowed.Add(1)
					return routeResponse(200, `{}`)
				}
				out := routeResponse(tc.status, "")
				out.Header.Set("X-MS-Throttle-Scope", tc.scope)
				return out
			})
			method := http.MethodGet
			if tc.write {
				method = http.MethodPost
			}
			_, err := a.g.do(context.Background(), call{method: method, url: "/chats/c/messages", monitor: tc.monitor, write: tc.write, noAuth: tc.noAuth})
			if tc.name == "unknown429" {
				if err != nil || borrowed.Load() != 1 {
					t.Fatalf("%v borrowed=%d", err, borrowed.Load())
				}
			} else if err == nil || borrowed.Load() != 0 {
				t.Fatalf("unexpected switch: %v borrowed=%d", err, borrowed.Load())
			}
		})
	}
}

func TestReadFallbackRejectsDifferentIdentityAndOpaqueTokens(t *testing.T) {
	for _, tok := range []string{routeJWT("other", "tenant", "full"), routeJWT("owner", "other", "full"), routeJWT("owner", "tenant", "read"), "opaque"} {
		var full atomic.Int32
		a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
			if routeApp(r) != "read" {
				full.Add(1)
			}
			return routeResponse(429, "")
		})
		a.g.tokens = &staticTokens{tok: tok}
		th, ok := channel.AsThrottled(monitorCall(a.g, context.Background()))
		if !ok || th.Scope != channel.ScopeUnknown || th.RetryAfter != 62*time.Second {
			t.Fatalf("original throttle changed: %v", th)
		}
		if full.Load() != 0 {
			t.Fatal("sent request with unverified full identity")
		}
	}
}

func TestReadFallbackConcurrentProbeCancellationAndWrites(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var full atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if r.Method == http.MethodPost {
			return routeResponse(201, `{}`)
		}
		if routeApp(r) == "read" {
			return routeResponse(429, "")
		}
		if full.Add(1) == 1 {
			close(entered)
			<-release
		}
		return routeResponse(200, `{}`)
	})
	done := make(chan error, 1)
	go func() { done <- monitorCall(a.g, context.Background()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no fallback probe")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := monitorCall(a.g, ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	// A blocked borrowed read must not block a write.
	writeDone := make(chan error, 1)
	go func() {
		_, err := a.g.do(context.Background(), call{method: http.MethodPost, url: "/chats/c/messages", write: true})
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write waited behind fallback")
	}
	if full.Load() != 1 {
		t.Fatal("duplicate concurrent probe")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReadFallbackReceivePaginationAndAtomicFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			var readPage2, fullPage2 atomic.Int32
			a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
				if r.URL.Path == "/v1.0/me" {
					return routeResponse(200, `{"id":"owner"}`)
				}
				if r.URL.Query().Get("page") == "2" {
					if routeApp(r) == "read" {
						readPage2.Add(1)
						return routeResponse(429, "")
					}
					fullPage2.Add(1)
					if fail {
						return routeResponse(429, "")
					}
					return routeResponse(200, `{"value":[{"id":"old","createdDateTime":"2026-11-14T00:00:01Z","lastModifiedDateTime":"2026-11-14T00:00:01Z","from":{"user":{"id":"owner"}},"body":{"contentType":"text","content":"old prompt"}}]}`)
				}
				return routeResponse(200, `{"value":[{"id":"new","createdDateTime":"2026-11-14T00:00:02Z","lastModifiedDateTime":"2026-11-14T00:00:02Z","from":{"user":{"id":"owner"}},"body":{"contentType":"text","content":"new prompt"}}],"@odata.nextLink":"https://graph.microsoft.com/v1.0/chats/c/messages?page=2"}`)
			})
			w, err := a.Receive(context.Background(), "c", "2026-11-14T00:00:00Z")
			if readPage2.Load() != 1 || fullPage2.Load() != 1 {
				t.Fatal("did not retry exact failed page")
			}
			if fail {
				if err == nil || w.Cursor != "" || len(w.Msgs) != 0 {
					t.Fatalf("partial window admitted: %+v %v", w, err)
				}
				return
			}
			if err != nil || len(w.Msgs) != 2 || w.Msgs[0].ID != "old" || w.Msgs[1].ID != "new" || w.Cursor != "2026-11-14T00:00:02.000000000Z" {
				t.Fatalf("window %+v err %v", w, err)
			}
		})
	}
}

func TestReadFallbackFailureAfterCooldownNeverReturnsSuccess(t *testing.T) {
	var c *routeClock
	a, clock := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "full" {
			c.add(70 * time.Second)
			return routeResponse(403, "")
		}
		return routeResponse(429, "")
	})
	c = clock
	if _, ok := channel.AsThrottled(monitorCall(a.g, context.Background())); !ok {
		t.Fatal("failed fallback returned success")
	}
}

func TestReadFallbackLatePrimarySuccessCannotClearCooldown(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "read" {
			if reads.Add(1) == 1 {
				close(entered)
				<-release
				return routeResponse(200, `{}`)
			}
			return routeResponse(429, "")
		}
		return routeResponse(200, `{}`)
	})
	done := make(chan error, 1)
	go func() { done <- monitorCall(a.g, context.Background()) }()
	<-entered
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 2 {
		t.Fatal("late success cleared newer cooldown")
	}
}

func TestReadFallbackDisabled(t *testing.T) {
	var full atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "full" {
			full.Add(1)
		}
		return routeResponse(429, "")
	})
	a.g.readRoute = nil
	if monitorCall(a.g, context.Background()) == nil || full.Load() != 0 {
		t.Fatal("fallback enabled without opt-in")
	}
}

type routeRefreshTokens struct {
	token       string
	refresh     func(string) (string, error)
	beforeToken func()
}

func (s *routeRefreshTokens) Token(context.Context) (string, error) {
	if s.beforeToken != nil {
		s.beforeToken()
	}
	return s.token, nil
}
func (s *routeRefreshTokens) Refresh(_ context.Context, stale string) (string, error) {
	return s.refresh(stale)
}

func TestReadFallbackRefreshPinsSourceAndRechecksIdentity(t *testing.T) {
	for _, changeUser := range []bool{false, true} {
		t.Run(fmt.Sprint(changeUser), func(t *testing.T) {
			var sends atomic.Int32
			var c *routeClock
			var times []time.Time
			a, clock := newRouteAdapter(t, func(r *http.Request) *http.Response {
				if routeApp(r) == "read" {
					return routeResponse(429, "")
				}
				times = append(times, c.now())
				if sends.Add(1) == 1 {
					return routeResponse(401, "")
				}
				return routeResponse(200, `{}`)
			})
			c = clock
			source := &routeRefreshTokens{token: routeJWT("owner", "tenant", "full")}
			source.refresh = func(stale string) (string, error) {
				if stale != source.token {
					t.Fatal("refreshed the wrong credential")
				}
				// Changing the graph's default must not change this attempt's
				// token source between Refresh and the second physical send.
				a.g.tokens = &staticTokens{tok: "wrong-global-source"}
				user := "owner"
				if changeUser {
					user = "other"
				}
				source.token = routeJWT(user, "tenant", "full")
				return source.token, nil
			}
			a.g.tokens = source
			err := monitorCall(a.g, context.Background())
			if changeUser {
				if _, ok := channel.AsThrottled(err); !ok || sends.Load() != 1 {
					t.Fatalf("changed identity was sent: sends=%d err=%v", sends.Load(), err)
				}
			} else {
				if err != nil || sends.Load() != 2 {
					t.Fatalf("pinned refresh: sends=%d err=%v", sends.Load(), err)
				}
				if times[1].Sub(times[0]) < time.Second {
					t.Fatal("401 retry bypassed fallback pacing")
				}
			}
		})
	}
}

func TestReadFallbackRecoveryProbeIsSingleFlight(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	a, c := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "read" {
			n := reads.Add(1)
			if n == 1 {
				return routeResponse(429, "")
			}
			if n == 2 {
				close(entered)
				<-release
			}
		}
		return routeResponse(200, `{}`)
	})
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	c.add(63 * time.Second)
	done := make(chan error, 21)
	go func() { done <- monitorCall(a.g, context.Background()) }()
	<-entered
	var launched sync.WaitGroup
	launched.Add(20)
	for i := 0; i < 20; i++ {
		go func() { launched.Done(); done <- monitorCall(a.g, context.Background()) }()
	}
	launched.Wait()
	if reads.Load() != 2 {
		t.Fatal("multiple recovery probes while primary is unproven")
	}
	close(release)
	for i := 0; i < 21; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if a.ReadFallbackStats().Recoveries != 1 {
		t.Fatalf("stats: %+v", a.ReadFallbackStats())
	}
}

func TestReadFallbackWriteThrottleStopsBorrowing(t *testing.T) {
	var reads, full atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if r.Method == http.MethodPost {
			return routeResponse(429, "")
		}
		if routeApp(r) == "read" {
			reads.Add(1)
			return routeResponse(429, "")
		}
		full.Add(1)
		return routeResponse(200, `{}`)
	})
	_, _ = a.g.do(context.Background(), call{method: http.MethodPost, url: "/chats/c/messages", write: true})
	if _, ok := channel.AsThrottled(monitorCall(a.g, context.Background())); !ok {
		t.Fatal("missing write-aware cooldown")
	}
	if reads.Load() != 1 || full.Load() != 0 {
		t.Fatal("borrowed full credential while writes are throttled")
	}
}

func TestReadFallbackZeroRetryAfterAndConcurrentFailuresAreBounded(t *testing.T) {
	var calls atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
		calls.Add(1)
		out := routeResponse(429, "")
		out.Header.Set("Retry-After", "0")
		return out
	})
	// Establish cooling before releasing a burst of concurrent callers.
	_ = monitorCall(a.g, context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := channel.AsThrottled(monitorCall(a.g, context.Background())); !ok {
				t.Error("missing throttle")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 2 {
		t.Fatalf("zero-delay polling storm: %d calls", calls.Load())
	}
}

func TestReadFallbackActivityScanUsesAlternate(t *testing.T) {
	var full atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if r.URL.Path == "/v1.0/me" {
			return routeResponse(200, `{"id":"owner"}`)
		}
		if routeApp(r) == "read" {
			return routeResponse(429, "")
		}
		full.Add(1)
		return routeResponse(200, `{"value":[{"id":"chat","lastMessagePreview":{"id":"m","createdDateTime":"2026-11-14T01:00:00Z","from":{"user":{"id":"owner"}}}}]}`)
	})
	acts, err := a.ScanActivity(context.Background(), time.Time{})
	if err != nil || len(acts) != 1 || acts[0].Chat != "chat" || !acts[0].ByMe || full.Load() != 1 {
		t.Fatalf("activity %+v err %v", acts, err)
	}
}

func TestReadFallbackLargeWindowRecoversBetweenPages(t *testing.T) {
	const pages, perPage = 80, 50
	var primary atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if r.URL.Path == "/v1.0/me" {
			return routeResponse(200, `{"id":"owner"}`)
		}
		if routeApp(r) == "read" && primary.Add(1) == 1 {
			return routeResponse(429, "")
		}
		page := 0
		if _, err := fmt.Sscanf(r.URL.Query().Get("page"), "%d", &page); err != nil && r.URL.Query().Get("page") != "" {
			t.Error(err)
		}
		values := make([]map[string]any, 0, perPage)
		for i := 0; i < perPage; i++ {
			n := (pages-page)*perPage - i
			stamp := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC).Add(time.Duration(n) * time.Second).Format(time.RFC3339)
			values = append(values, map[string]any{"id": fmt.Sprint(n), "createdDateTime": stamp, "lastModifiedDateTime": stamp, "from": map[string]any{"user": map[string]string{"id": "owner"}}, "body": map[string]string{"contentType": "text", "content": "prompt"}})
		}
		body := map[string]any{"value": values}
		if page+1 < pages {
			body["@odata.nextLink"] = fmt.Sprintf("https://graph.microsoft.com/v1.0/chats/c/messages?page=%d", page+1)
		}
		raw, _ := json.Marshal(body)
		return routeResponse(200, string(raw))
	})
	w, err := a.Receive(context.Background(), "c", "")
	if err != nil || len(w.Msgs) != pages*perPage || w.Requests != pages {
		t.Fatalf("window: messages=%d pages=%d err=%v", len(w.Msgs), w.Requests, err)
	}
	for i, m := range w.Msgs {
		if m.ID != fmt.Sprint(i+1) {
			t.Fatalf("missing/duplicate/reordered message at %d: %s", i, m.ID)
		}
	}
	stats := a.ReadFallbackStats()
	if stats.PrimaryThrottles != 1 || stats.Recoveries != 1 || stats.FullRequests > 64 {
		t.Fatalf("unexpected recovery/load: %+v", stats)
	}
}

func TestReadFallbackIdentityCheckDoesNotExtendServerCooldown(t *testing.T) {
	var reads, full atomic.Int32
	a, c := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "read" {
			if reads.Add(1) == 1 {
				return routeResponse(429, "")
			}
		} else {
			full.Add(1)
		}
		return routeResponse(200, `{}`)
	})
	var delayed atomic.Bool
	a.g.tokens = &routeRefreshTokens{token: routeJWT("owner", "tenant", "full"), beforeToken: func() {
		if !delayed.Swap(true) {
			c.add(70 * time.Second)
		}
	}}
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 2 || full.Load() != 0 {
		t.Fatalf("cooldown extended by identity check: read=%d full=%d", reads.Load(), full.Load())
	}
}

func TestReadFallbackOpaquePrimaryKeepsOriginalThrottle(t *testing.T) {
	var requests atomic.Int32
	a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response { requests.Add(1); return routeResponse(429, "") })
	a.g.readTokens = &staticTokens{tok: "opaque-read-token"}
	th, ok := channel.AsThrottled(monitorCall(a.g, context.Background()))
	if !ok || th.Scope != channel.ScopeUnknown || th.RetryAfter != 62*time.Second || requests.Load() != 1 {
		t.Fatalf("original behavior changed: throttle=%v requests=%d", th, requests.Load())
	}
}

func TestReadFallbackTransientRecoveryFailureKeepsFullWorking(t *testing.T) {
	var primary, full atomic.Int32
	a, c := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "read" {
			if primary.Add(1) == 1 {
				return routeResponse(429, "")
			}
			return routeResponse(500, "")
		}
		full.Add(1)
		return routeResponse(200, `{}`)
	})
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	c.add(63 * time.Second)
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatalf("working full lost after transient primary probe: %v", err)
	}
	if primary.Load() != 2 || full.Load() != 2 {
		t.Fatalf("unexpected probes: primary=%d full=%d", primary.Load(), full.Load())
	}
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	if primary.Load() != 2 || full.Load() != 3 {
		t.Fatal("transient recovery failure caused a primary probe storm")
	}
}

func TestReadFallbackRecoveryTimeoutLeavesTimeForFull(t *testing.T) {
	var primary, full atomic.Int32
	a, c := newRouteAdapter(t, func(*http.Request) *http.Response { return routeResponse(200, `{}`) })
	a.g.timeout = 2 * time.Second
	a.g.http.Transport = routeErrorTransport(func(r *http.Request) (*http.Response, error) {
		if routeApp(r) == "read" {
			if primary.Add(1) == 1 {
				return routeResponse(429, ""), nil
			}
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		full.Add(1)
		return routeResponse(200, `{}`), nil
	})
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	c.add(63 * time.Second)
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatalf("slow primary recovery blocked full: %v", err)
	}
	if primary.Load() != 2 || full.Load() != 2 {
		t.Fatalf("primary=%d full=%d", primary.Load(), full.Load())
	}
}

func TestReadFallbackLateShortThrottleCannotShortenCooldown(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	a, c := newRouteAdapter(t, func(r *http.Request) *http.Response {
		if routeApp(r) == "read" {
			if reads.Add(1) == 1 {
				close(entered)
				<-release
				out := routeResponse(429, "")
				out.Header.Set("Retry-After", "10")
				return out
			}
			return routeResponse(429, "")
		}
		return routeResponse(200, `{}`)
	})
	done := make(chan error, 1)
	go func() { done <- monitorCall(a.g, context.Background()) }()
	<-entered
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c.add(15 * time.Second)
	if err := monitorCall(a.g, context.Background()); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 2 {
		t.Fatal("late shorter throttle reopened primary before the longer cooldown")
	}
}

func TestReadFallbackPacingWaitHonorsCancellationAndWriteCooldown(t *testing.T) {
	for _, writeThrottle := range []bool{false, true} {
		t.Run(fmt.Sprint(writeThrottle), func(t *testing.T) {
			var full atomic.Int32
			a, _ := newRouteAdapter(t, func(r *http.Request) *http.Response {
				if r.Method == http.MethodPost || routeApp(r) == "read" {
					return routeResponse(429, "")
				}
				full.Add(1)
				return routeResponse(200, `{}`)
			})
			if err := monitorCall(a.g, context.Background()); err != nil {
				t.Fatal(err)
			}
			waiting, wake := make(chan struct{}), make(chan struct{})
			a.g.readRoute.sleep = func(ctx context.Context, _ time.Duration) error {
				close(waiting)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-wake:
					return nil
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- monitorCall(a.g, ctx) }()
			select {
			case <-waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("did not enter pacing wait")
			}
			if writeThrottle {
				_, _ = a.g.do(context.Background(), call{method: http.MethodPost, url: "/chats/c/messages", write: true})
				close(wake)
				if _, ok := channel.AsThrottled(<-done); !ok {
					t.Fatal("write cooldown did not stop queued alternate")
				}
			} else {
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("pacing cancellation: %v", err)
				}
			}
			if full.Load() != 1 {
				t.Fatal("sent an alternate request after cancellation/write cooldown")
			}
		})
	}
}

func BenchmarkReadFallbackNormal(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprint(enabled), func(b *testing.B) {
			a, err := New(Config{ReadTokens: &staticTokens{tok: "read"}, Tokens: &staticTokens{tok: "full"}, ReadFallback: enabled, HTTP: &http.Client{Transport: routeTransport(func(*http.Request) *http.Response { return routeResponse(200, `{}`) })}})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := monitorCall(a.g, context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
