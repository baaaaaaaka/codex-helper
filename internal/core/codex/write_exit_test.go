package codex

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// A write that fails because the server closed its input, seen before the
// reader saw the exit, still reports the exit (SETUP-ERR: "app-server
// exited", not "write initialize: broken pipe", whichever came first).
func TestAFailedWriteBeforeTheExitIsSeenReportsTheExit(t *testing.T) {
	r, w := io.Pipe()
	r.Close() // the server closed its input: every write fails
	s := &Server{in: w, pending: map[int64]chan rpcResult{}, done: make(chan struct{})}
	go func() {
		time.Sleep(200 * time.Millisecond) // the reader sees the exit later
		close(s.done)
	}()
	err := s.Call(tctx(t), "initialize", nil, nil)
	if !errors.Is(err, ErrExited) {
		t.Fatalf("want ErrExited, got %v", err)
	}
}

// A failed write of a server that does not exit is reported as such, after
// writeExitWait at most.
func TestAFailedWriteOfARunningServerIsAWriteError(t *testing.T) {
	r, w := io.Pipe()
	r.Close()
	s := &Server{in: w, pending: map[int64]chan rpcResult{}, done: make(chan struct{})}
	start := time.Now()
	err := s.Call(tctx(t), "initialize", nil, nil)
	if err == nil || errors.Is(err, ErrExited) || !strings.Contains(err.Error(), "write initialize") {
		t.Fatalf("want a write error, got %v", err)
	}
	if d := time.Since(start); d < writeExitWait || d > writeExitWait+5*time.Second {
		t.Fatalf("waited %s, want about %s", d, writeExitWait)
	}
}
