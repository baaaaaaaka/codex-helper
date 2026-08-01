package appgateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/localproxy"
)

var errBackendUnavailable = errors.New("app gateway backend is unavailable")

type unavailableDialer struct {
	reason atomic.Value
}

func (d *unavailableDialer) Dial(network, addr string) (net.Conn, error) {
	reason := "backend is waiting for network"
	if value := d.reason.Load(); value != nil {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			reason = text
		}
	}
	return nil, fmt.Errorf("%w: %s", errBackendUnavailable, reason)
}

func (d *unavailableDialer) setReason(reason string) {
	d.reason.Store(strings.TrimSpace(reason))
}

// Frontend owns the stable HTTP listener. SwapBackend changes only the
// generation behind that listener; callers may therefore restart SSH without
// changing ChatGPT's --proxy-server URL.
type Frontend struct {
	id      string
	port    int
	addr    string
	proxy   *localproxy.HTTPProxy
	router  *localproxy.GenerationRouter
	dormant *unavailableDialer

	mu      sync.RWMutex
	state   string
	lastErr error
	backend string
	closed  bool
}

func NewFrontend(id string, port int) (*Frontend, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("app gateway frontend id is required")
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("app gateway frontend port %d is invalid", port)
	}
	dormant := &unavailableDialer{}
	dormant.setReason("backend is waiting for network")
	router, err := localproxy.NewGenerationRouter(dormant)
	if err != nil {
		return nil, err
	}
	f := &Frontend{
		id:      id,
		port:    port,
		router:  router,
		dormant: dormant,
		state:   StatePending,
	}
	f.proxy = localproxy.NewHTTPProxy(router, localproxy.Options{
		InstanceID: id,
		HealthProbe: func(context.Context) error {
			f.mu.RLock()
			defer f.mu.RUnlock()
			if f.closed {
				return errors.New("frontend is closed")
			}
			if f.state != StateReady {
				if f.lastErr != nil {
					return f.lastErr
				}
				return errBackendUnavailable
			}
			return nil
		},
		HealthDetails: func(context.Context) localproxy.HealthStatus {
			f.mu.RLock()
			defer f.mu.RUnlock()
			return localproxy.HealthStatus{
				Recovery:         f.state,
				BrokerID:         id,
				HostReady:        f.state == StateReady,
				ActiveGeneration: router.CurrentGeneration(),
			}
		},
	})
	addr, err := f.proxy.Start(fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		_ = router.Close(context.Background())
		if isAddressInUse(err) {
			return nil, fmt.Errorf("%w: %s: %v", ErrPortInUse, addrForPort(port), err)
		}
		return nil, fmt.Errorf("start app gateway frontend: %w", err)
	}
	f.addr = addr
	return f, nil
}

func addrForPort(port int) string { return fmt.Sprintf("127.0.0.1:%d", port) }

func isAddressInUse(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "address already in use") || strings.Contains(text, "only one usage") || strings.Contains(text, "bind:") && strings.Contains(text, "use")
}

func (f *Frontend) ID() string {
	if f == nil {
		return ""
	}
	return f.id
}

func (f *Frontend) Port() int {
	if f == nil {
		return 0
	}
	return f.port
}

func (f *Frontend) Addr() string {
	if f == nil {
		return ""
	}
	return f.addr
}

func (f *Frontend) URL() string {
	if f == nil {
		return ""
	}
	return "http://" + f.addr
}

func (f *Frontend) State() (string, error) {
	if f == nil {
		return "", errors.New("nil app gateway frontend")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state, f.lastErr
}

// SetUnavailable keeps the listener alive while making new requests fail
// locally. Existing connections are allowed to drain; the caller can bound
// the drain with ctx when a backend has definitely been reset.
func (f *Frontend) SetUnavailable(ctx context.Context, reason error) error {
	if f == nil {
		return errors.New("nil app gateway frontend")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if reason == nil {
		reason = errBackendUnavailable
	}
	f.dormant.setReason(reason.Error())
	old, err := f.router.Swap(f.dormant)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.state = StatePending
	f.lastErr = reason
	f.backend = ""
	f.mu.Unlock()
	if old != 0 {
		return f.router.Drain(ctx, old)
	}
	return nil
}

// SwapBackend installs a validated SOCKS backend. The frontend remains
// listening throughout the swap. A bounded drain prevents stale sockets from
// surviving a confirmed backend replacement.
func (f *Frontend) SwapBackend(ctx context.Context, socksAddr, generation string) error {
	if f == nil {
		return errors.New("nil app gateway frontend")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	socksAddr = strings.TrimSpace(socksAddr)
	if socksAddr == "" {
		return errors.New("app gateway SOCKS address is required")
	}
	dialer, err := localproxy.NewSOCKS5Dialer(socksAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("create app gateway SOCKS dialer: %w", err)
	}
	old, err := f.router.Swap(dialer)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.state = StateReady
	f.lastErr = nil
	f.backend = strings.TrimSpace(generation)
	f.mu.Unlock()
	if old != 0 {
		return f.router.Drain(ctx, old)
	}
	return nil
}

func (f *Frontend) BackendGeneration() string {
	if f == nil {
		return ""
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.backend
}

func (f *Frontend) Close(ctx context.Context) error {
	if f == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	f.mu.Unlock()
	if f.proxy != nil {
		_ = f.proxy.Close(ctx)
	}
	if f.router != nil {
		return f.router.Close(ctx)
	}
	return nil
}
