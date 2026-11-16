package stack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/hoststate"
)

type startupTestTunnel struct {
	port        int
	socks       bool
	entered     chan struct{}
	done        chan struct{}
	once        sync.Once
	enteredOnce sync.Once
	ln          net.Listener
}

func (t *startupTestTunnel) Start() error {
	if !t.socks {
		close(t.entered)
		return nil
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", t.port))
	if err != nil {
		return err
	}
	t.ln = ln
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// The TCP probe sends nothing; the SOCKS probe sends a greeting.
			var b [1]byte
			_, err = io.ReadFull(c, b[:])
			if err == nil {
				t.enteredOnce.Do(func() { close(t.entered) })
			}
			c.Close()
		}
	}()
	return nil
}
func (t *startupTestTunnel) Stop(time.Duration) error {
	t.once.Do(func() {
		if t.ln != nil {
			t.ln.Close()
		}
		close(t.done)
	})
	return nil
}
func (t *startupTestTunnel) Done() <-chan struct{} { return t.done }
func (t *startupTestTunnel) Wait() error           { <-t.done; return nil }

func TestStartContextCancellationCleansStartup(t *testing.T) {
	for _, socks := range []bool{false, true} {
		t.Run(fmt.Sprintf("SOCKS=%v", socks), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			previous := newStackTunnel
			t.Cleanup(func() { newStackTunnel = previous })
			entered := make(chan struct{})
			var tunnel *startupTestTunnel
			var starts atomic.Int32
			newStackTunnel = func(_ config.Profile, port int) (tunnelProcess, error) {
				starts.Add(1)
				tunnel = &startupTestTunnel{port: port, socks: socks, entered: entered, done: make(chan struct{})}
				return tunnel, nil
			}
			// An explicit HTTP address lets us verify listener release on failure.
			addr, ln, err := reservePort()
			if err != nil {
				t.Fatal(err)
			}
			ln.Close()
			result := make(chan error, 1)
			go func() {
				st, err := StartContext(ctx, config.Profile{Host: "example.com", Port: 22, User: "alice"}, "cancel-start", Options{HTTPListenAddr: fmt.Sprintf("127.0.0.1:%d", addr)})
				if st != nil {
					st.Close(context.Background())
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("startup did not enter readiness probe")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("startup ignored cancellation")
			}
			if starts.Load() != 1 {
				t.Fatalf("cancelled startup retried %d times", starts.Load())
			}
			select {
			case <-tunnel.done:
			default:
				t.Fatal("tunnel not stopped")
			}
			retry, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", addr))
			if err != nil {
				t.Fatalf("HTTP listener leaked: %v", err)
			}
			retry.Close()
		})
	}
}

func TestStartContextAlreadyCancelledDoesNotCreateTunnel(t *testing.T) {
	previous := newStackTunnel
	t.Cleanup(func() { newStackTunnel = previous })
	newStackTunnel = func(config.Profile, int) (tunnelProcess, error) {
		t.Fatal("created tunnel after cancellation")
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := StartContext(ctx, config.Profile{}, "id", Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestStartContextCancelledDuringPreparationDoesNotLaunch(t *testing.T) {
	previous := newStackTunnel
	t.Cleanup(func() { newStackTunnel = previous })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tunnel := &startupTestTunnel{entered: make(chan struct{}), done: make(chan struct{})}
	newStackTunnel = func(config.Profile, int) (tunnelProcess, error) { cancel(); return tunnel, nil }
	_, err := StartContext(ctx, config.Profile{Host: "example.com", Port: 22, User: "alice"}, "cancel-preparation", Options{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-tunnel.entered:
		t.Fatal("launched tunnel after cancelled preparation")
	default:
	}
}

func TestStartContextSuccessKeepsExplicitOwnership(t *testing.T) {
	previous := newStackTunnel
	t.Cleanup(func() { newStackTunnel = previous })
	var tunnel *probeTestTunnel
	newStackTunnel = func(_ config.Profile, port int) (tunnelProcess, error) {
		tunnel = newProbeTestTunnel(port, nil)
		return tunnel, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := StartContext(ctx, config.Profile{Host: "example.com", Port: 22, User: "alice"}, "owned-start", Options{HostObserver: hoststate.NewChannelObserver(1), ProbeInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(context.Background())
	cancel()
	select {
	case <-tunnel.Done():
		t.Fatal("startup context stopped a successfully acquired stack")
	default:
	}
	resp, err := http.Get(st.HTTPProxyURL() + "/_codex_proxy/health")
	if err != nil {
		t.Fatalf("owned stack stopped accepting requests: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stack health: %s", resp.Status)
	}
}
