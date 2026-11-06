package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Microsoft Graph HTTP core.
//
// Every request goes through graph.do, which maps the outcome onto the
// channel error classes. Reads may be retried here (they are idempotent);
// writes never are.

const (
	defaultGraphURL   = "https://graph.microsoft.com/v1.0"
	maxRetryAfter     = time.Hour
	defaultRetryAfter = 30 * time.Second // 429 without a usable Retry-After
	readAttempts      = 3                // transport errors and 5xx on reads
	smallBodyLimit    = 8 << 20
	pageBodyLimit     = 64 << 20
)

type graph struct {
	base   *url.URL
	http   *http.Client
	tokens TokenSource
	sleep  func(context.Context, time.Duration) error
	// timeout bounds one request; transfer one download or large upload
	// body. Zero: no bound beyond ctx.
	timeout, transfer time.Duration
}

// transferBody is the request body size from which transfer applies.
const transferBody = 1 << 20

type call struct {
	method string
	url    string // absolute, or relative to the base ("/me/chats")
	body   []byte
	ctype  string
	write  bool              // not idempotent: never retried, transport errors after the write are Ambiguous
	noAuth bool              // pre-authenticated URLs (upload sessions)
	header map[string]string // extra request headers
	limit  int64             // response body limit
	// sink, when set, consumes a 2xx response body instead of buffering it
	// (downloads). Its error is a transient Rejected for reads.
	sink func(*http.Response) error
}

type response struct {
	status int
	header http.Header
	body   []byte
}

// statusError is the detail wrapped inside a classified error.
type statusError struct {
	Method string
	Path   string
	Status int
	Code   string // Graph error code, if it looks safe to log
}

func (e *statusError) Error() string {
	s := fmt.Sprintf("graph %s %s: HTTP %d", e.Method, e.Path, e.Status)
	if e.Code != "" {
		s += " (" + e.Code + ")"
	}
	return s
}

func (g *graph) do(ctx context.Context, c call) (response, error) {
	if c.write {
		return g.once(ctx, c)
	}
	var last error
	for attempt := 0; attempt < readAttempts; attempt++ {
		if attempt > 0 {
			if err := g.sleep(ctx, time.Duration(attempt*attempt)*300*time.Millisecond); err != nil {
				return response{}, &channel.Rejected{Err: err}
			}
		}
		resp, err := g.once(ctx, c)
		if err == nil || !retryableRead(err) {
			return resp, err
		}
		last = err
	}
	return response{}, last
}

// retryableRead: transport failures and plain 5xx (not throttling).
func retryableRead(err error) bool {
	if _, ok := channel.AsThrottled(err); ok {
		return false
	}
	var r *channel.Rejected
	if !errors.As(err, &r) || r.Permanent {
		return false
	}
	return true
}

// once performs one request, refreshing the token once on 401 for reads.
func (g *graph) once(ctx context.Context, c call) (response, error) {
	resp, stale, err := g.send(ctx, c)
	if err != nil || resp.status != http.StatusUnauthorized || c.noAuth {
		return resp, err
	}
	// 401: the request was not applied. Refresh so the next attempt works;
	// retry here only for reads (writes are never retried internally).
	tok, rerr := g.tokens.Refresh(ctx, stale)
	if rerr != nil {
		return response{}, authErr(rerr)
	}
	if c.write || tok == "" {
		return response{}, &channel.Rejected{Err: g.statusErr(c, resp)}
	}
	resp, _, err = g.send(ctx, c)
	if err == nil && resp.status == http.StatusUnauthorized {
		return response{}, &channel.AuthRequired{Err: g.statusErr(c, resp)}
	}
	return resp, err
}

func authErr(err error) error {
	var a *channel.AuthRequired
	if errors.As(err, &a) {
		return err
	}
	return &channel.Rejected{Err: fmt.Errorf("token refresh: %w", err)}
}

// send returns the raw response for 2xx and 401, and a classified error for
// everything else.
func (g *graph) send(ctx context.Context, c call) (response, string, error) {
	target, err := g.resolveCall(c)
	if err != nil {
		return response{}, "", &channel.Rejected{Permanent: true, Err: err}
	}
	var token string
	if !c.noAuth {
		token, err = g.tokens.Token(ctx)
		if err != nil {
			return response{}, "", authErr(err)
		}
	}
	bound := g.timeout
	if c.sink != nil || len(c.body) >= transferBody {
		bound = g.transfer
	}
	if bound > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, bound)
		defer cancel()
	}
	var body io.Reader
	if c.body != nil {
		body = bytes.NewReader(c.body)
	}
	req, err := http.NewRequestWithContext(ctx, c.method, target, body)
	if err != nil {
		return response{}, token, &channel.Rejected{Permanent: true, Err: err}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", "application/json")
	if c.body != nil {
		ct := c.ctype
		if ct == "" {
			ct = "application/json"
		}
		req.Header.Set("Content-Type", ct)
	}
	for k, v := range c.header {
		req.Header.Set(k, v)
	}
	// wrote is set once the whole request (headers and body) was handed to
	// the connection. Before that point a failure cannot have been applied.
	var wrote atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				wrote.Store(true)
			}
		},
	}))
	hr, err := g.http.Do(req)
	if err != nil {
		err = fmt.Errorf("graph %s %s: %w", c.method, redactPath(target), err)
		if c.write && wrote.Load() {
			return response{}, token, &channel.Ambiguous{Err: err}
		}
		return response{}, token, &channel.Rejected{Err: err}
	}
	defer hr.Body.Close()
	if c.sink != nil && hr.StatusCode/100 == 2 {
		if err := c.sink(hr); err != nil {
			err = fmt.Errorf("graph %s %s: %w", c.method, redactPath(target), err)
			if c.write {
				return response{}, token, &channel.Ambiguous{Err: err}
			}
			return response{}, token, &channel.Rejected{Err: err}
		}
		return response{status: hr.StatusCode, header: hr.Header}, token, nil
	}
	limit := c.limit
	if limit <= 0 {
		limit = smallBodyLimit
	}
	raw, rerr := io.ReadAll(io.LimitReader(hr.Body, limit+1))
	resp := response{status: hr.StatusCode, header: hr.Header, body: raw}
	if rerr == nil && int64(len(raw)) > limit {
		rerr = fmt.Errorf("response body over %d bytes", limit)
	}
	if hr.StatusCode/100 == 2 {
		if rerr != nil {
			err := fmt.Errorf("graph %s %s: reading response: %w", c.method, redactPath(target), rerr)
			if c.write {
				return response{}, token, &channel.Ambiguous{Err: err}
			}
			return response{}, token, &channel.Rejected{Err: err}
		}
		return resp, token, nil
	}
	if hr.StatusCode == http.StatusUnauthorized {
		return resp, token, nil
	}
	return response{}, token, g.classify(c, resp)
}

// classify maps a non-2xx, non-401 status onto the channel error classes.
func (g *graph) classify(c call, resp response) error {
	se := g.statusErr(c, resp)
	switch s := resp.status; {
	case s == http.StatusTooManyRequests:
		return &channel.Throttled{RetryAfter: retryAfter(resp.header, defaultRetryAfter), Scope: throttleScope(resp.header), Err: se}
	case s == http.StatusServiceUnavailable && resp.header.Get("Retry-After") != "" && !c.write:
		// Graph also throttles with 503 + Retry-After. For writes a 503 is
		// still Ambiguous below: we cannot prove nothing was stored.
		return &channel.Throttled{RetryAfter: retryAfter(resp.header, defaultRetryAfter), Scope: throttleScope(resp.header), Err: se}
	case s >= 500 || s == http.StatusRequestTimeout:
		if c.write {
			return &channel.Ambiguous{Err: se}
		}
		return &channel.Rejected{Err: se}
	case s == http.StatusNotFound:
		return &channel.Rejected{Permanent: true, Err: fmt.Errorf("%w: %w", channel.ErrNotFound, se)}
	default:
		return &channel.Rejected{Permanent: true, Err: se}
	}
}

func (g *graph) statusErr(c call, resp response) *statusError {
	target, _ := g.resolveCall(c)
	return &statusError{Method: c.method, Path: redactPath(target), Status: resp.status, Code: graphErrorCode(resp.body)}
}

// resolveCall allows pre-authenticated URLs (upload sessions, download
// redirects) on any https host; everything else must be on the Graph base.
func (g *graph) resolveCall(c call) (string, error) {
	if c.noAuth {
		u, err := url.Parse(c.url)
		if err != nil || u.Host == "" || !(u.Scheme == "https" || u.Host == g.base.Host) {
			return "", fmt.Errorf("graph: bad pre-authenticated url")
		}
		return u.String(), nil
	}
	return g.resolve(c.url)
}

// resolve turns a call URL into an absolute URL on the configured base.
// Absolute links returned by Graph (nextLink) are re-rooted on the base so a
// test or proxy base URL keeps working; other hosts are refused unless the
// call is to a pre-authenticated URL handled by the caller.
func (g *graph) resolve(raw string) (string, error) {
	if strings.HasPrefix(raw, "/") {
		return strings.TrimRight(g.base.String(), "/") + raw, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("graph: bad url %q", redactPath(raw))
	}
	if u.Host == g.base.Host && u.Scheme == g.base.Scheme {
		return u.String(), nil
	}
	if strings.EqualFold(u.Host, "graph.microsoft.com") && strings.HasPrefix(u.Path, g.base.Path+"/") {
		v := *g.base
		v.Path = u.Path
		v.RawPath = u.RawPath
		v.RawQuery = u.RawQuery
		return v.String(), nil
	}
	return "", fmt.Errorf("graph: refusing link to foreign host %q", u.Host)
}

// retryAfter parses Retry-After as seconds or an HTTP date, capped at an
// hour. Missing or unusable values give def.
func retryAfter(h http.Header, def time.Duration) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return def
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n < 0 {
			return def
		}
		if n > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		if d > maxRetryAfter {
			return maxRetryAfter
		}
		return d
	}
	return def
}

// throttleScope reads the scope of a throttling response. Graph does not
// document a reliable header for Teams chat throttling, so only explicit
// values are trusted (X-MS-Throttle-Scope on some workloads; the
// X-CXP-RateLimit-Scope spelling is used by proxies and the fake).
func throttleScope(h http.Header) channel.ThrottleScope {
	for _, key := range []string{"X-CXP-RateLimit-Scope", "X-MS-Throttle-Scope"} {
		v := strings.ToLower(strings.TrimSpace(h.Get(key)))
		switch {
		case v == "":
			continue
		case v == "chat" || v == "thread" || strings.HasPrefix(v, "chat/") || strings.HasPrefix(v, "thread/"):
			return channel.ScopeChat
		case v == "account" || v == "global" || v == "user" || v == "tenant" ||
			strings.HasPrefix(v, "user") || strings.HasPrefix(v, "tenant") || strings.HasPrefix(v, "application"):
			return channel.ScopeAccount
		}
	}
	return channel.ScopeUnknown
}

func graphErrorCode(raw []byte) string {
	var p struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &p) != nil || len(p.Error.Code) > 80 {
		return ""
	}
	for _, r := range p.Error.Code {
		if !(r == '_' || r == '-' || r == '.' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z') {
			return ""
		}
	}
	return p.Error.Code
}

// redactPath drops the query (it can carry filters with message times or
// pre-authenticated tokens) and keeps only the path for error messages.
func redactPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<url>"
	}
	if u.RawQuery != "" && strings.Contains(strings.ToLower(u.RawQuery), "tempauth") {
		return u.Host + "/<upload>"
	}
	return u.Path
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (g *graph) getJSON(ctx context.Context, path string, limit int64, out any) error {
	body, err := g.get(ctx, path, limit)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return decodeError(path, err)
	}
	return nil
}

// get performs an idempotent GET and returns the response body undecoded.
func (g *graph) get(ctx context.Context, path string, limit int64) ([]byte, error) {
	resp, err := g.do(ctx, call{method: http.MethodGet, url: path, limit: limit})
	if err != nil {
		return nil, err
	}
	return resp.body, nil
}

// decodeError is the error of a GET response that does not decode.
func decodeError(path string, err error) error {
	return &channel.Rejected{Err: fmt.Errorf("graph GET %s: decoding: %w", redactPath(path), err)}
}

// writeJSON performs a non-idempotent write and decodes the response. A 2xx
// whose body cannot be decoded is Ambiguous: the write happened but we do not
// know its result.
func (g *graph) writeJSON(ctx context.Context, method, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return &channel.Rejected{Permanent: true, Err: err}
	}
	resp, err := g.do(ctx, call{method: method, url: path, body: body, write: true})
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(resp.body, out); err != nil {
		return &channel.Ambiguous{Err: fmt.Errorf("graph %s %s: decoding response: %w", method, redactPath(path), err)}
	}
	return nil
}
