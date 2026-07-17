package stack

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/hoststate"
)

func TestSuspendDoesNotConsumeRecoveryBudgetAfterTunnelExit(t *testing.T) {
	initial := newProbeTestTunnel(0, nil)
	observer := hoststate.NewChannelObserver(8)
	fatal := make(chan error, 1)
	states := make(chan string, 8)
	var persisted atomic.Int32
	s := &Stack{
		Profile:      config.Profile{Host: "example.com", Port: 22, User: "alice"},
		tunnel:       initial,
		fatalCh:      fatal,
		stopCh:       make(chan struct{}),
		probeCh:      make(chan struct{}, 1),
		failureCh:    make(chan error, 1),
		hostObserver: observer,
		hostEvents:   observer.Events(),
		hostProbe:    func(context.Context) error { return errors.New("offline") },
		powerState:   hoststate.PowerAwake,
		networkState: hoststate.NetworkReady,
		hostReady:    true,
		persistRecoveryBudget: func(config.ProxyRecoveryBudget) error {
			persisted.Add(1)
			return nil
		},
		setRecoveryState: func(state string, _ error) {
			select {
			case states <- state:
			default:
			}
		},
	}
	if err := initial.Start(); err != nil {
		t.Fatalf("initial Start: %v", err)
	}
	if err := observer.Start(); err != nil {
		t.Fatalf("observer Start: %v", err)
	}
	go s.monitor(Options{
		MaxRestarts:       1,
		RestartBackoff:    time.Millisecond,
		RestartWindow:     time.Minute,
		TunnelStopGrace:   10 * time.Millisecond,
		SocksReadyTimeout: 50 * time.Millisecond,
		ProbeInterval:     time.Hour,
	})

	observer.Emit(hoststate.Event{Kind: hoststate.EventPowerWillSleep})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case state := <-states:
			if state == "suspended" {
				close(initial.done)
				goto exited
			}
		default:
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("monitor did not enter suspended state")

exited:
	select {
	case err := <-fatal:
		t.Fatalf("sleep-related tunnel exit became fatal: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if got := persisted.Load(); got != 0 {
		t.Fatalf("sleep-related tunnel exit persisted %d budget updates", got)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWakeWaitsForHostBeforeCreatingCandidate(t *testing.T) {
	initial := newProbeTestTunnel(0, nil)
	observer := hoststate.NewChannelObserver(32)
	created := make(chan struct{}, 1)
	previousFactory := newStackTunnel
	newStackTunnel = func(config.Profile, int) (tunnelProcess, error) {
		created <- struct{}{}
		return nil, fmt.Errorf("candidate should not start while offline")
	}
	t.Cleanup(func() { newStackTunnel = previousFactory })
	var hostProbes atomic.Int32
	s := &Stack{
		Profile:      config.Profile{Host: "example.com", Port: 22, User: "alice"},
		tunnel:       initial,
		fatalCh:      make(chan error, 1),
		stopCh:       make(chan struct{}),
		probeCh:      make(chan struct{}, 1),
		failureCh:    make(chan error, 1),
		hostObserver: observer,
		hostEvents:   observer.Events(),
		hostProbe: func(context.Context) error {
			hostProbes.Add(1)
			return errors.New("offline")
		},
		powerState:   hoststate.PowerAwake,
		networkState: hoststate.NetworkReady,
		hostReady:    true,
	}
	if err := initial.Start(); err != nil {
		t.Fatalf("initial Start: %v", err)
	}
	if err := observer.Start(); err != nil {
		t.Fatalf("observer Start: %v", err)
	}
	go s.monitor(Options{
		MaxRestarts:       1,
		RestartBackoff:    time.Millisecond,
		RestartWindow:     time.Minute,
		TunnelStopGrace:   10 * time.Millisecond,
		SocksReadyTimeout: 50 * time.Millisecond,
		HostProbeTimeout:  10 * time.Millisecond,
		ProbeInterval:     time.Hour,
	})
	observer.Emit(hoststate.Event{Kind: hoststate.EventPowerDidWake})
	time.Sleep(80 * time.Millisecond)
	select {
	case <-created:
		t.Fatal("candidate started while host probe was failing")
	default:
	}
	if got := hostProbes.Load(); got == 0 {
		t.Fatal("wake did not run host probes")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCloseCancelsAndCleansActiveCandidate(t *testing.T) {
	initial := newProbeTestTunnel(0, nil)
	close(initial.done)
	previousFactory := newStackTunnel
	created := make(chan *probeTestTunnel, 1)
	newStackTunnel = func(_ config.Profile, port int) (tunnelProcess, error) {
		candidate := newProbeTestTunnel(port, nil)
		created <- candidate
		return candidate, nil
	}
	t.Cleanup(func() { newStackTunnel = previousFactory })
	probeStarted := make(chan struct{})
	var probeOnce atomic.Bool
	s := &Stack{
		Profile:   config.Profile{Host: "example.com", Port: 22, User: "alice"},
		tunnel:    initial,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
		probeCh:   make(chan struct{}, 1),
		failureCh: make(chan error, 1),
	}
	go s.monitor(Options{
		MaxRestarts:       1,
		RestartBackoff:    time.Millisecond,
		RestartWindow:     time.Minute,
		TunnelStopGrace:   time.Second,
		SocksReadyTimeout: time.Second,
		ProbeInterval:     time.Hour,
		RouteProbe: func(ctx context.Context, _ string, _ string, _ int, _ time.Duration) error {
			if probeOnce.CompareAndSwap(false, true) {
				close(probeStarted)
			}
			<-ctx.Done()
			return ctx.Err()
		},
	})

	var candidate *probeTestTunnel
	select {
	case candidate = <-created:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for candidate")
	}
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("candidate route probe did not start")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-candidate.Done():
	default:
		t.Fatal("candidate remained alive after Stack.Close")
	}
}
