//go:build !windows

package codexagent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/gorilla/websocket"
)

// Native notifications must bind the thread and give the TUI approval ownership
// before the helper reader can process a subsequent server request.
func TestRealCodexSharedTUIThreadBinding(t *testing.T) {
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("offline native contracts are not enabled")
	}
	for _, mode := range []string{"created", "canonical-cwd", "empty-exit", "bind-failure"} {
		t.Run(mode, func(t *testing.T) {
			responses := fakecodex.NewResponses()
			defer responses.Close()
			home := t.TempDir()
			if err := fakecodex.WriteCodexHome(home, responses.URL()); err != nil {
				t.Fatal(err)
			}
			cwd := home
			if mode == "canonical-cwd" {
				cwd = filepath.Join(t.TempDir(), "project")
				if err := os.Symlink(home, cwd); err != nil {
					t.Fatal(err)
				}
			}
			a := New(Config{Server: codex.Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: cwd, SharedDir: home}})
			defer a.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			bound := make(chan string, 1)
			ep, release, done, err := a.Attach(ctx, 7, "", cwd, func(id string) error {
				raw, _ := json.Marshal(map[string]string{"threadId": id})
				if !a.cfg.Server.IgnoreServerRequest(raw) {
					t.Error("approval ownership not established before binding")
				}
				bound <- id
				if mode == "bind-failure" {
					return errors.New("injected binding failure")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if ep.ThreadID != "" || len(responses.Requests()) != 0 {
				t.Fatal("helper pre-created a thread or user input")
			}
			if mode != "empty-exit" {
				c, _, err := websocket.DefaultDialer.Dial(ep.URL, http.Header{"Authorization": []string{"Bearer " + ep.Token}})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
				call := func(id int, method string, params any) json.RawMessage {
					t.Helper()
					if err := c.WriteJSON(map[string]any{"id": id, "method": method, "params": params}); err != nil {
						t.Fatal(err)
					}
					for {
						var r struct {
							ID     int
							Result json.RawMessage
							Error  any
						}
						if err := c.ReadJSON(&r); err != nil {
							t.Fatal(err)
						}
						if r.ID == id {
							if r.Error != nil {
								t.Fatalf("%s: %v", method, r.Error)
							}
							return r.Result
						}
					}
				}
				call(1, "initialize", map[string]any{"clientInfo": map[string]string{"name": "tui-contract", "version": "0"}, "capabilities": map[string]bool{"experimentalApi": true}})
				var started struct{ Thread struct{ ID string } }
				if err := json.Unmarshal(call(2, "thread/start", map[string]string{"cwd": home}), &started); err != nil {
					t.Fatal(err)
				}
				select {
				case id := <-bound:
					if id != started.Thread.ID {
						t.Fatalf("bound different thread: %s %s", id, started.Thread.ID)
					}
				case <-ctx.Done():
					t.Fatal("thread not bound")
				}
				if mode == "bind-failure" {
					select {
					case <-done:
					case <-ctx.Done():
						t.Fatal("binding failure did not close the TUI lease")
					}
				}
			}
			release()
			release()
			a.mu.Lock()
			defer a.mu.Unlock()
			if len(a.attached) != 0 || len(a.tuiThreads) != 0 {
				t.Fatalf("attachment leaked: %v %v", a.attached, a.tuiThreads)
			}
		})
	}
}
