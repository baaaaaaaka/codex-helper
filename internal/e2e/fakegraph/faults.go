package fakegraph

import (
	"sync"
	"time"
)

// FaultKind selects what an injected fault does.
type FaultKind int

const (
	// FaultStatus answers with Status (and Retry-After when set) without
	// touching the model.
	FaultStatus FaultKind = iota
	// FaultAcceptedLost applies the request to the model (the message is
	// stored) and then drops the connection, so the client sees a transport
	// error and cannot know the outcome.
	FaultAcceptedLost
	// FaultDropBeforeAccept drops the connection without applying the
	// request ("timeout before accept").
	FaultDropBeforeAccept
	// FaultLatency only delays the request, then serves it normally.
	FaultLatency
)

// Fault is one injection rule. A request matches when every non-empty
// selector matches. The first matching, still-armed rule wins.
type Fault struct {
	Name       string
	Kind       FaultKind
	Method     string // "" matches any
	Route      string // e.g. "GET /chats/{chat}/messages"; "" matches any
	ChatID     string // "" matches any
	Host       string // "" matches any; "login.microsoftonline.com" for token faults
	Match      func(Request) bool
	Status     int           // for FaultStatus
	RetryAfter time.Duration // Retry-After header for FaultStatus (rounded up to seconds)
	Latency    time.Duration // applied before the fault (or alone for FaultLatency)
	Skip       int           // let the first Skip matching requests through
	Times      int           // fire at most Times times; 0 = unlimited
}

// FaultHandle controls an installed fault.
type FaultHandle struct {
	mu       sync.Mutex
	fault    Fault
	seen     int
	fired    int
	disarmed bool
}

// Disarm removes the fault from future matching.
func (h *FaultHandle) Disarm() {
	h.mu.Lock()
	h.disarmed = true
	h.mu.Unlock()
}

// Fired reports how many times the fault fired.
func (h *FaultHandle) Fired() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fired
}

func (h *FaultHandle) consider(r Request) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := h.fault
	if h.disarmed {
		return false
	}
	if f.Method != "" && f.Method != r.Method {
		return false
	}
	if f.Route != "" && f.Route != r.Route {
		return false
	}
	if f.ChatID != "" && f.ChatID != r.ChatID {
		return false
	}
	if f.Host != "" && f.Host != r.Host {
		return false
	}
	if f.Match != nil && !f.Match(r) {
		return false
	}
	h.seen++
	if h.seen <= f.Skip {
		return false
	}
	if f.Times > 0 && h.fired >= f.Times {
		return false
	}
	h.fired++
	return true
}
