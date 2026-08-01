package appgateway

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestFrontendKeepsStableListenerAcrossBackendSwaps(t *testing.T) {
	port := freePort(t)
	f, err := NewFrontend("gateway-test", port)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close(context.Background())
	if got := f.Port(); got != port {
		t.Fatalf("frontend port = %d, want %d", got, port)
	}
	if state, _ := f.State(); state != StatePending {
		t.Fatalf("initial state = %q", state)
	}
	health, err := Probe(context.Background(), port, time.Second)
	if err != nil || health.InstanceID != "gateway-test" || health.OK {
		t.Fatalf("pending frontend health = %#v/%v", health, err)
	}
	if err := f.SwapBackend(context.Background(), "127.0.0.1:1", "generation-1"); err != nil {
		t.Fatal(err)
	}
	if got := f.BackendGeneration(); got != "generation-1" {
		t.Fatalf("backend generation = %q", got)
	}
	if state, gotErr := f.State(); state != StateReady || gotErr != nil {
		t.Fatalf("ready state = %q/%v", state, gotErr)
	}
	deadline := time.Now().Add(time.Second)
	for {
		health, err = Probe(context.Background(), port, time.Second)
		if err == nil && health.InstanceID == "gateway-test" && health.OK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ready frontend health = %#v/%v", health, err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := f.SetUnavailable(context.Background(), errors.New("DNS is still recovering")); err != nil {
		t.Fatal(err)
	}
	if state, gotErr := f.State(); state != StatePending || gotErr == nil {
		t.Fatalf("pending state = %q/%v", state, gotErr)
	}
	if got := f.BackendGeneration(); got != "" {
		t.Fatalf("unavailable backend generation = %q, want empty", got)
	}
	if f.Port() != port || f.Addr() != net.JoinHostPort("127.0.0.1", formatPort(port)) {
		t.Fatalf("frontend endpoint changed: %s", f.Addr())
	}
	if err := f.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFrontendRejectsForeignPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	if _, err := NewFrontend("gateway-conflict", port); !errors.Is(err, ErrPortInUse) {
		t.Fatalf("conflict error = %v", err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func formatPort(port int) string {
	return strconv.Itoa(port)
}
