package fakegraph

import (
	"fmt"
	"strings"
	"time"
)

// Request is one recorded HTTP request as the fake saw it.
type Request struct {
	Seq      int64
	At       time.Time
	Instance string // SUT instance tag added by the harness transport
	Host     string // original host (graph.microsoft.com / login.microsoftonline.com)
	Method   string
	RawPath  string // escaped path + "?" + raw query, as sent
	Path     string // unescaped path without the /v1.0 prefix
	Query    string // raw query string
	Route    string // e.g. "GET /chats/{chat}/messages"
	ChatID   string
	Body     string // request body, truncated
	Status   int
	Fault    string // name of the injected fault, if any
	Outcome  string // "ok", "fault", "violation", "dropped", "error"
	Duration time.Duration
}

// String renders a compact one-line form for logs and reports.
func (r Request) String() string {
	fault := ""
	if r.Fault != "" {
		fault = " fault=" + r.Fault
	}
	inst := ""
	if r.Instance != "" {
		inst = " inst=" + r.Instance
	}
	return fmt.Sprintf("#%d %s %s %s -> %d%s%s", r.Seq, r.At.Format("15:04:05.000"), r.Method, shortenPath(r.RawPath), r.Status, fault, inst)
}

func shortenPath(p string) string {
	if len(p) > 220 {
		return p[:220] + "…"
	}
	return p
}

// Violation is a request that real Graph would reject, or that the fake does
// not model. Both are answered with HTTP 400.
type Violation struct {
	Seq    int64
	At     time.Time
	Method string
	Path   string
	Route  string
	Reason string
	// Kind is "graph-contract" when real Graph documents the request as
	// invalid, and "unmodeled" when the fake simply does not implement it.
	Kind string
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s %s: %s", v.Kind, v.Method, shortenPath(v.Path), v.Reason)
}

// Key groups violations that differ only by ids and timestamps.
func (v Violation) Key() string {
	return v.Kind + " " + v.Method + " " + v.Route + ": " + v.Reason
}

// BlockedRequest is an attempt to reach a host other than Graph or login.
type BlockedRequest struct {
	At       time.Time
	Instance string
	URL      string
}

// RequestFilter selects recorded requests.
type RequestFilter struct {
	Method   string
	Route    string // exact route, e.g. "POST /chats/{chat}/messages"
	ChatID   string
	Instance string
	Since    int64 // only Seq > Since
	OnlyOK   bool
}

func (f RequestFilter) match(r Request) bool {
	if f.Method != "" && !strings.EqualFold(f.Method, r.Method) {
		return false
	}
	if f.Route != "" && f.Route != r.Route {
		return false
	}
	if f.ChatID != "" && f.ChatID != r.ChatID {
		return false
	}
	if f.Instance != "" && f.Instance != r.Instance {
		return false
	}
	if f.Since > 0 && r.Seq <= f.Since {
		return false
	}
	if f.OnlyOK && (r.Status < 200 || r.Status >= 300) {
		return false
	}
	return true
}
