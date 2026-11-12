package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

type ReadBudget struct {
	MaxRequests int           `json:"max_requests"`
	MaxBytes    int64         `json:"max_bytes"`
	MaxChars    int           `json:"max_chars"`
	MaxMessages int           `json:"max_messages"`
	Timeout     time.Duration `json:"-"`
}

func DefaultReadBudget() ReadBudget { return ReadBudget{20, 8 << 20, 60000, 200, 60 * time.Second} }
func (b ReadBudget) validate() error {
	if b.MaxRequests < 1 || b.MaxRequests > 200 || b.MaxBytes < 1024 || b.MaxBytes > 64<<20 || b.MaxChars < 1 || b.MaxChars > 200000 || b.MaxMessages < 1 || b.MaxMessages > 200 || b.Timeout <= 0 || b.Timeout > 10*time.Minute {
		return errors.New("invalid read budget (requests 1..200, bytes 1KiB..64MiB, chars 1..200000, messages 1..200, timeout <=10m)")
	}
	return nil
}

type ReadOptions struct {
	Before, Latest, Limit int
	Replies               bool
	Cursor                string
	Budget                ReadBudget
}
type ReadAttachment struct {
	Kind        string `json:"kind"`
	Name        string `json:"name,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Ref         string `json:"ref,omitempty"`
	Content     string `json:"content,omitempty"`
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
}
type ReadMessage struct {
	Ref         string           `json:"ref"`
	ID          string           `json:"id"`
	Target      ReadTarget       `json:"target"`
	URL         string           `json:"url"`
	Author      *graphIdentity   `json:"author,omitempty"`
	Created     string           `json:"created_at"`
	Modified    string           `json:"modified_at"`
	Deleted     bool             `json:"deleted"`
	Type        string           `json:"type"`
	Text        string           `json:"text"`
	Attachments []ReadAttachment `json:"attachments,omitempty"`
	Truncated   bool             `json:"truncated,omitempty"`
	NextCursor  string           `json:"next_cursor,omitempty"`
}
type ReadResource struct {
	Target ReadTarget `json:"target"`
	Ref    string     `json:"ref"`
	URL    string     `json:"url"`
	Name   string     `json:"name,omitempty"`
}
type ReadRange struct {
	Kind       string     `json:"kind"`
	Target     ReadTarget `json:"target"`
	Requested  int        `json:"requested"`
	Refs       []string   `json:"refs"`
	State      string     `json:"state"`
	NextCursor string     `json:"next_cursor,omitempty"`
	Error      string     `json:"error,omitempty"`
	Oldest     string     `json:"oldest,omitempty"`
	Newest     string     `json:"newest,omitempty"`
	Order      string     `json:"order"`
}
type ReadUsage struct {
	Requests int   `json:"requests"`
	Bytes    int64 `json:"bytes"`
	Chars    int   `json:"chars"`
}
type ReadResult struct {
	Version   int            `json:"version"`
	FetchedAt time.Time      `json:"fetched_at"`
	Resources []ReadResource `json:"resources"`
	Messages  []ReadMessage  `json:"messages"`
	Ranges    []ReadRange    `json:"ranges"`
	Usage     ReadUsage      `json:"usage"`
}
type SearchOptions struct {
	Limit  int
	Cursor string
	Budget ReadBudget
}
type SearchHit struct {
	Ref              string `json:"ref,omitempty"`
	Summary          string `json:"summary"`
	URL              string `json:"url,omitempty"`
	State            string `json:"state"`
	Reason           string `json:"reason,omitempty"`
	SummaryTruncated bool   `json:"summary_truncated,omitempty"`
}
type SearchResult struct {
	Version    int         `json:"version"`
	FetchedAt  time.Time   `json:"fetched_at"`
	Query      string      `json:"query"`
	Scope      string      `json:"scope"`
	Hits       []SearchHit `json:"hits"`
	State      string      `json:"state"`
	Error      string      `json:"error,omitempty"`
	NextCursor string      `json:"next_cursor,omitempty"`
	Usage      ReadUsage   `json:"usage"`
}

var errReadBudget = errors.New("Teams read budget exhausted")

// Each operation has its own transport budget, including retries and redirects.
// OAuth uses the caller's original client, so refresh is not confused with Graph IO.
type readMeter struct {
	mu        sync.Mutex
	remaining int
	bytes     int64
	requests  int
	used      int64
	base      http.RoundTripper
}

func (m *readMeter) RoundTrip(req *http.Request) (*http.Response, error) {
	weight := 1
	if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/$batch") && req.GetBody != nil {
		body, e := req.GetBody()
		if e != nil {
			return nil, e
		}
		var batch struct {
			Requests []json.RawMessage `json:"requests"`
		}
		e = json.NewDecoder(io.LimitReader(body, 1<<20)).Decode(&batch)
		body.Close()
		if e != nil || len(batch.Requests) < 1 || len(batch.Requests) > 20 {
			return nil, errors.New("invalid read batch")
		}
		weight = len(batch.Requests)
	}
	m.mu.Lock()
	if m.remaining < weight || m.bytes <= 0 {
		m.mu.Unlock()
		return nil, errReadBudget
	}
	m.remaining -= weight
	m.requests += weight
	m.mu.Unlock()
	resp, e := m.base.RoundTrip(req)
	if e != nil {
		return nil, e
	}
	resp.Body = &meterBody{ReadCloser: resp.Body, m: m, remaining: resp.ContentLength}
	return resp, nil
}

type meterBody struct {
	io.ReadCloser
	m         *readMeter
	remaining int64
}

func (b *meterBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	b.m.mu.Lock()
	left := b.m.bytes
	if left <= 0 {
		b.m.mu.Unlock()
		return 0, errReadBudget
	}
	if int64(len(p)) > left {
		p = p[:left]
	}
	b.m.bytes -= int64(len(p))
	b.m.mu.Unlock()
	n, e := b.ReadCloser.Read(p)
	if b.remaining >= 0 {
		b.remaining -= int64(n)
	}
	b.m.mu.Lock()
	b.m.bytes += int64(len(p) - n)
	b.m.used += int64(n)
	b.m.mu.Unlock()
	return n, e
}

type Reader struct{ a *Adapter }

func NewReader(cfg Config) (*Reader, error) {
	a, e := New(cfg)
	if e != nil {
		return nil, e
	}
	return &Reader{a}, nil
}
func (r *Reader) Close() error { return r.a.Close() }
func (r *Reader) session(ctx context.Context, b ReadBudget) (context.Context, context.CancelFunc, *graph, *readMeter, error) {
	if e := b.validate(); e != nil {
		return ctx, func() {}, nil, nil, e
	}
	ctx, cancel := context.WithTimeout(ctx, b.Timeout)
	hc := *r.a.g.http
	base := hc.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	m := &readMeter{remaining: b.MaxRequests, bytes: b.MaxBytes, base: base}
	hc.Transport = m
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	g := *r.a.g
	g.http = &hc
	return ctx, cancel, &g, m, nil
}
func (m *readMeter) usage(chars int) ReadUsage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ReadUsage{m.requests, m.used, chars}
}
func readFailure(err error) string {
	if errors.Is(err, errReadBudget) {
		return errReadBudget.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "Teams read deadline exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "Teams read canceled"
	}
	var auth *channel.AuthRequired
	if errors.As(err, &auth) {
		return "Graph authentication required: sign in with the configured read profile"
	}
	// Never echo transport URLs: they can contain temporary access credentials.
	var se *statusError
	if errors.As(err, &se) {
		switch se.Status {
		case 401:
			return "Graph authentication required (401)"
		case 403:
			return "Graph access denied (403): check message/file permissions"
		case 404:
			return "Graph resource not found or not visible (404)"
		case 429:
			return "Graph throttled (429): retry later"
		}
		return fmt.Sprintf("Graph read failed (HTTP %d)", se.Status)
	}
	return "Teams read failed: network, response or resource validation error"
}
