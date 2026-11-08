package codex

import (
	"context"
	"encoding/json"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This runs in the offline native-Codex CI container with synthetic Responses.
func TestRealCodexTitle(t *testing.T) {
	srv, responses, thread := realTitleServer(t)
	if !srv.SupportsTitle() {
		if _, err := srv.GenerateTitle(context.Background(), t.TempDir(), "probe-model", "low", "must skip"); err != ErrTitleUnsupported || len(responses.Requests()) != 0 {
			t.Fatalf("unsupported title made an inference: %v %+v", err, responses.Requests())
		}
		c, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		out, err := srv.RunTurn(c, TurnStartParams{ThreadID: thread, Input: []Input{{Text: "normal task with older Codex"}}}, time.Second, TurnHooks{})
		if err != nil || out.Status != StatusCompleted {
			t.Fatalf("older Codex main task: %+v %v", out, err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	name, err := srv.GenerateTitle(ctx, t.TempDir(), "probe-model", "low", "Summarize login repair")
	if err != nil || name != "Native title contract" {
		t.Fatalf("title %q: %v", name, err)
	}
	if len(responses.Requests()) != 1 {
		t.Fatalf("%d inference requests", len(responses.Requests()))
	}
	req := responses.Requests()[0]
	if len(req.ToolNames) != 0 || len(req.Schema) == 0 || req.Model != "probe-model" || req.Effort != "low" {
		t.Fatalf("native isolation: %+v", req)
	}
	if got, err := srv.NameIfEmpty(ctx, thread, name); err != nil || got != name {
		t.Fatalf("persist: %q %v", got, err)
	}
	if got, err := srv.NameIfEmpty(ctx, thread, "Must not overwrite"); err != nil || got != name {
		t.Fatalf("existing name: %q %v", got, err)
	}
	th, err := srv.ThreadRead(ctx, thread, true)
	if err != nil || len(th.Turns) != 0 {
		t.Fatalf("hidden request entered original history: %d %v", len(th.Turns), err)
	}
	// The temporary request must not leave a second saved rollout.
	home := ""
	for _, env := range srv.cfg.Env {
		if len(env) > 11 && env[:11] == "CODEX_HOME=" {
			home = env[11:]
		}
	}
	files, err := filepath.Glob(filepath.Join(home, "sessions", "*", "*", "*", "*.jsonl"))
	if err != nil || len(files) > 1 {
		t.Fatalf("temporary rollouts: %v %v", files, err)
	}
}

func realTitleServer(t *testing.T) (*Server, *fakecodex.Responses, string) {
	t.Helper()
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("E2E_REAL_CODEX is not set")
	}
	responses := fakecodex.NewResponses()
	t.Cleanup(responses.Close)
	home := t.TempDir()
	if err := fakecodex.WriteCodexHome(home, responses.URL()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv, err := Start(ctx, Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: home})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Kill)
	th, err := srv.ThreadStart(ctx, ThreadStartParams{Cwd: home, Model: "probe-model"})
	if err != nil {
		t.Fatal(err)
	}
	return srv, responses, th.ID
}

func TestRealCodexTitleCancellation(t *testing.T) {
	srv, responses, thread := realTitleServer(t)
	if !srv.SupportsTitle() {
		t.Skip("older Codex skips title requests; fallback covered above")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	temporary := make(chan string, 1)
	unsub := srv.Subscribe("", func(n Notification) {
		if n.Method == "thread/started" {
			var p struct {
				Thread struct {
					ID        string `json:"id"`
					Ephemeral bool   `json:"ephemeral"`
				} `json:"thread"`
			}
			if json.Unmarshal(n.Params, &p) == nil && p.Thread.Ephemeral {
				select {
				case temporary <- p.Thread.ID:
				default:
				}
			}
		}
	})
	defer unsub()
	done := make(chan error, 1)
	go func() {
		_, err := srv.GenerateTitle(ctx, t.TempDir(), "probe-model", "low", "probe:hold:title-cancel")
		done <- err
	}()
	select {
	case <-responses.Started("title-cancel"):
	case <-time.After(10 * time.Second):
		t.Fatal("title did not reach native model")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled title succeeded")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("native title cleanup exceeded bound")
	}
	id := ""
	select {
	case id = <-temporary:
	default:
		t.Fatal("temporary thread was not ephemeral")
	}
	// Codex unsubscribe detaches this client; it does not promise immediate
	// thread-manager eviction. Check the actual protocol contract, not unload.
	var detached struct {
		Status string `json:"status"`
	}
	c, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := srv.Call(c, "thread/unsubscribe", map[string]any{"threadId": id}, &detached); err != nil {
		t.Fatal(err)
	}
	if detached.Status != "notSubscribed" && detached.Status != "notLoaded" {
		t.Fatalf("temporary thread was not detached: %+v", detached)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		reqs := responses.Requests()
		if len(reqs) == 1 && reqs[0].Cancelled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("native model request survived cancellation: %+v", reqs)
		}
		time.Sleep(time.Millisecond)
	}
	// The original conversation is still usable after interrupt and unsubscribe.
	result, err := srv.RunTurn(c, TurnStartParams{ThreadID: thread, Input: []Input{{Text: "Normal task after hidden cancellation"}}}, time.Second, TurnHooks{})
	if err != nil || result.Status != StatusCompleted {
		t.Fatalf("original task damaged: %+v %v", result, err)
	}
}
