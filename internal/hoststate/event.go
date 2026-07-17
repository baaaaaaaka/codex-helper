package hoststate

import (
	"sync"
	"time"
)

// EventKind identifies a host-level lifecycle hint. These events are hints,
// not proof that a remote backend is usable; consumers must still run their
// own target-specific probe before replacing a backend.
type EventKind string

const (
	EventPowerWillSleep EventKind = "power-will-sleep"
	EventPowerDidWake   EventKind = "power-did-wake"
	EventNetworkChanged EventKind = "network-changed"
	EventObserverError  EventKind = "observer-error"
)

type PowerState string

const (
	PowerUnknown   PowerState = "unknown"
	PowerAwake     PowerState = "awake"
	PowerSuspended PowerState = "suspended"
	PowerWaking    PowerState = "waking"
)

type NetworkState string

const (
	NetworkUnknown NetworkState = "unknown"
	NetworkDown    NetworkState = "down"
	NetworkReady   NetworkState = "ready"
)

type Event struct {
	Kind   EventKind
	At     time.Time
	Source string
}

// Observer is deliberately small so the recovery coordinator can be tested
// without sleeping the test runner or changing its network interfaces.
type Observer interface {
	Events() <-chan Event
	Start() error
	Close() error
}

type Options struct {
	Interval time.Duration
}

// NewDefaultObserver returns the best user-mode observer available on the
// current platform. Platform implementations are intentionally allowed to
// degrade to the polling observer when a native notification API is absent.
func NewDefaultObserver(opts Options) Observer {
	return newPlatformObserver(opts)
}

// ChannelObserver is a deterministic observer for unit tests and fault labs.
// Emit coalesces a full channel rather than blocking a simulated OS callback.
type ChannelObserver struct {
	mu     sync.Mutex
	closed bool
	events chan Event
}

func NewChannelObserver(buffer int) *ChannelObserver {
	if buffer <= 0 {
		buffer = 8
	}
	return &ChannelObserver{events: make(chan Event, buffer)}
}

func (o *ChannelObserver) Events() <-chan Event {
	if o == nil {
		return nil
	}
	return o.events
}

func (o *ChannelObserver) Start() error { return nil }

func (o *ChannelObserver) Emit(event Event) {
	if o == nil {
		return
	}
	if event.At.IsZero() {
		event.At = time.Now()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	select {
	case o.events <- event:
	default:
	}
}

func (o *ChannelObserver) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	if !o.closed {
		o.closed = true
		close(o.events)
	}
	o.mu.Unlock()
	return nil
}
