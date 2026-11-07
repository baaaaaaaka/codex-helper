package tuilease

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

type fakeAttach struct {
	acquired chan string
	released chan struct{}
	done     chan struct{}
	err      error
}

func (f *fakeAttach) Attach(_ context.Context, thread string) (engine.TUIEndpoint, func(), <-chan struct{}, error) {
	if f.err != nil {
		return engine.TUIEndpoint{}, nil, nil, f.err
	}
	f.acquired <- thread
	return engine.TUIEndpoint{URL: "ws://127.0.0.1:45678", Token: "private-token"}, func() { f.released <- struct{}{} }, f.done, nil
}
func TestTUILeaseLifetimeAndAuthentication(t *testing.T) {
	for _, backendStops := range []bool{false, true} {
		t.Run(map[bool]string{false: "client-close", true: "backend-exit"}[backendStops], func(t *testing.T) {
			f := &fakeAttach{acquired: make(chan string, 1), released: make(chan struct{}, 1), done: make(chan struct{})}
			path := filepath.Join(t.TempDir(), "endpoint.json")
			s, err := Listen(path, "/private/codex", f, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			stat, _ := os.Stat(path)
			if runtime.GOOS != "windows" && stat.Mode().Perm() != 0600 {
				t.Fatal(stat.Mode())
			}
			raw, _ := os.ReadFile(path)
			var d descriptor
			_ = json.Unmarshal(raw, &d)
			resp, err := http.Post(d.URL+"/attach", "application/json", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 401 {
				t.Fatal(resp.StatusCode)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			lease, err := Acquire(ctx, path, "/private/codex", "thread-id")
			if err != nil || lease == nil {
				t.Fatalf("%v %v", lease, err)
			}
			if got := <-f.acquired; got != "thread-id" {
				t.Fatal(got)
			}
			select {
			case <-f.released:
				t.Fatal("lease released before TUI closed")
			default:
			}
			if backendStops {
				close(f.done)
			} else {
				_ = lease.Close()
			}
			select {
			case <-f.released:
			case <-ctx.Done():
				t.Fatal("lease was not released")
			}
			select {
			case <-lease.Done:
			case <-ctx.Done():
				t.Fatal("client did not observe disconnect")
			}
		})
	}
}
func TestTUILeaseFallbackOnlyForUnmanagedThread(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unmanaged", true: "recovering"}[rejected], func(t *testing.T) {
			f := &fakeAttach{err: engine.ErrUnmanagedThread}
			if rejected {
				f.err = errors.New("still recovering")
			}
			path := filepath.Join(t.TempDir(), "endpoint.json")
			s, err := Listen(path, "/codex", f, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			l, err := Acquire(context.Background(), path, "/codex", "thread")
			if l != nil || (err != nil) != rejected {
				t.Fatalf("%v %v", l, err)
			}
			l, err = Acquire(context.Background(), path, "/other-home", "thread")
			if l != nil || err != nil {
				t.Fatalf("other CODEX_HOME: %v %v", l, err)
			}
		})
	}
}

func TestDescriptorRejectsRemoteAndCredentialURLs(t *testing.T) {
	for _, endpoint := range []string{"http://127.0.0.1:80@remote.invalid", "http://remote.invalid:80", "https://127.0.0.1:80", "http://127.0.0.1:80/path"} {
		t.Run(endpoint, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "endpoint.json")
			data, _ := json.Marshal(descriptor{URL: endpoint, Token: "private", CodexHome: "/codex"})
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			lease, err := Acquire(context.Background(), path, "/codex", "thread")
			if lease != nil || err == nil {
				t.Fatalf("accepted nonlocal descriptor: %v %v", lease, err)
			}
		})
	}
}
