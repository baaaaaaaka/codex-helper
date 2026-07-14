package localproxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type pipeDialer struct {
	mu    sync.Mutex
	peers []net.Conn
}

func (d *pipeDialer) Dial(network, addr string) (net.Conn, error) {
	client, peer := net.Pipe()
	d.mu.Lock()
	d.peers = append(d.peers, peer)
	d.mu.Unlock()
	return client, nil
}

func (d *pipeDialer) ClosePeers() {
	d.mu.Lock()
	peers := append([]net.Conn(nil), d.peers...)
	d.peers = nil
	d.mu.Unlock()
	for _, peer := range peers {
		_ = peer.Close()
	}
}

func TestGenerationRouterSwapKeepsListenerAndFencesOldGeneration(t *testing.T) {
	first := &pipeDialer{}
	second := &pipeDialer{}
	router, err := NewGenerationRouter(first)
	if err != nil {
		t.Fatalf("NewGenerationRouter: %v", err)
	}

	oldConn, err := router.Dial("tcp", "old.example:443")
	if err != nil {
		t.Fatalf("old Dial: %v", err)
	}
	oldGeneration := router.CurrentGeneration()
	if oldGeneration == 0 {
		t.Fatal("expected initial generation")
	}

	newGeneration, err := router.Swap(second)
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
	if newGeneration != oldGeneration {
		t.Fatalf("Swap returned old generation %d, want %d", newGeneration, oldGeneration)
	}
	if router.CurrentGeneration() == oldGeneration {
		t.Fatal("router did not advance generation")
	}

	newConn, err := router.Dial("tcp", "new.example:443")
	if err != nil {
		t.Fatalf("new Dial: %v", err)
	}
	_ = newConn.Close()
	if len(second.peers) != 1 {
		t.Fatalf("new dialer calls = %d, want 1", len(second.peers))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := router.Drain(ctx, oldGeneration); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain error = %v, want deadline after forcing old connection close", err)
	}
	if err := oldConn.Close(); err == nil {
		// The router already closed the underlying connection during bounded
		// drain. Close is allowed to be idempotent, but the connection must no
		// longer be usable.
		if _, writeErr := oldConn.Write([]byte("stale")); writeErr == nil {
			t.Fatal("old generation connection remained writable after drain")
		}
	}

	first.ClosePeers()
	second.ClosePeers()
	if err := router.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type blockingContextDialer struct{}

func (blockingContextDialer) Dial(network, addr string) (net.Conn, error) {
	return nil, errors.New("legacy dial should not be used")
}

func (blockingContextDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGenerationRouterDialContextCancelsBackend(t *testing.T) {
	router, err := NewGenerationRouter(blockingContextDialer{})
	if err != nil {
		t.Fatalf("NewGenerationRouter: %v", err)
	}
	defer router.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = router.DialContext(ctx, "tcp", "blocked.example:443")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DialContext error = %v, want deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("DialContext took too long: %s", elapsed)
	}
}
