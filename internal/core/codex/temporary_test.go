package codex

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestTemporaryServerCleanup(t *testing.T) {
	for _, outcome := range []string{"success", "error", "cancel", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			_, url := newBrain(t)
			home := t.TempDir()
			p := NewPool(PoolConfig{PIDFile: filepath.Join(home, "pids.sqlite")})
			defer p.Close(time.Second)
			ctx, cancel := context.WithCancel(tctx(t))
			defer cancel()
			cfg := fakeConfig(t, url, home)
			parent, err := p.Acquire(ctx, "conv-1", cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Release()
			var temporary *Server
			want := errors.New("operation failed")
			var panicValue any
			func() {
				defer func() { panicValue = recover() }()
				err = p.WithTemporary(ctx, cfg, func(s *Server) error {
					temporary = s
					if s == parent.Server() || len(testPIDRecords(t, p.cfg.PIDFile)) != 2 {
						t.Error("temporary server did not get independent process ownership")
					}
					switch outcome {
					case "error":
						return want
					case "cancel":
						cancel()
						return ctx.Err()
					case "panic":
						panic(want)
					}
					return nil
				})
			}()
			if temporary == nil || !temporary.Exited() || parent.Server().Exited() || p.Size() != 1 {
				t.Fatalf("cleanup: temporary=%v parent exited=%v size=%d", temporary, parent.Server().Exited(), p.Size())
			}
			recs := testPIDRecords(t, p.cfg.PIDFile)
			if len(recs) != 1 || recs[0].PID != parent.Server().PID() {
				t.Fatalf("cleanup touched parent process ownership: %+v", recs)
			}
			switch outcome {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "error":
				if !errors.Is(err, want) {
					t.Fatal(err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "panic":
				if panicValue != want {
					t.Fatalf("panic: %v", panicValue)
				}
			}
		})
	}
}

func TestTemporaryServersAreIndependent(t *testing.T) {
	_, url := newBrain(t)
	home := t.TempDir()
	p := NewPool(PoolConfig{PIDFile: filepath.Join(home, "pids.sqlite")})
	defer p.Close(time.Second)
	ctx := tctx(t)
	started := make(chan *Server, 2)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	done := make(chan error, 2)
	cfg := fakeConfig(t, url, home)
	for range 2 {
		go func() {
			done <- p.WithTemporary(ctx, cfg, func(s *Server) error {
				started <- s
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
	}
	var servers []*Server
	for range 2 {
		select {
		case s := <-started:
			servers = append(servers, s)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if servers[0] == servers[1] || len(testPIDRecords(t, p.cfg.PIDFile)) != 2 {
		t.Fatal("concurrent operations reused a temporary server")
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if p.Size() != 0 || len(testPIDRecords(t, p.cfg.PIDFile)) != 0 || !servers[0].Exited() || !servers[1].Exited() {
		t.Fatal("temporary servers retained after completion")
	}
}

func TestTemporaryServerStartupFailure(t *testing.T) {
	p := NewPool(PoolConfig{})
	defer p.Close(0)
	want := errors.New("launch denied")
	cfg := Config{Bin: "unused", Configure: func(*exec.Cmd) error { return want }}
	called := false
	if err := p.WithTemporary(tctx(t), cfg, func(*Server) error { called = true; return nil }); !errors.Is(err, want) || called || p.Size() != 0 {
		t.Fatalf("startup failure: err=%v called=%v size=%d", err, called, p.Size())
	}
}

type temporaryInput struct{ io.Writer }

func (temporaryInput) Close() error { return nil }

func TestTemporaryServerUnconfirmedExitRetainsOwnership(t *testing.T) {
	// An unresponsive process: PID 0 is refused by the process helpers on
	// every OS. Seed an independent registry row to detect premature removal.
	s := &Server{in: temporaryInput{io.Discard}, done: make(chan struct{}), endpoint: Endpoint{URL: "fixture"}}
	ready := make(chan struct{})
	close(ready)
	p := NewPool(PoolConfig{PIDFile: filepath.Join(t.TempDir(), "pids.sqlite")})
	p.servers["temporary-1"] = &entry{key: "temporary-1", srv: s, ready: ready}
	p.writePIDs("temporary-1")
	if _, err := p.pidStore.Exec(pidPut, 1, "fixture", "temporary-1", "fixture", "fixture"); err != nil {
		t.Fatal(err)
	}
	defer p.Close(0)
	defer close(s.done)
	want := errors.New("operation result uncertain")
	err := p.WithTemporary(tctx(t), Config{}, func(*Server) error { return want })
	if !errors.Is(err, want) || err.Error() == want.Error() {
		t.Fatalf("unconfirmed exit lost failure evidence: %v", err)
	}
	if p.Size() != 1 || len(testPIDRecords(t, p.cfg.PIDFile)) != 1 {
		t.Fatal("unconfirmed process ownership was discarded")
	}
}
