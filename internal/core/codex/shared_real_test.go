package codex

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/gorilla/websocket"
)

func realShared(t *testing.T) (*Server, *fakecodex.Responses, string) {
	t.Helper()
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("offline native contract: E2E_REAL_CODEX is not set")
	}
	responses := fakecodex.NewResponses()
	t.Cleanup(responses.Close)
	home := t.TempDir()
	if err := fakecodex.WriteCodexHome(home, responses.URL()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv, err := Start(ctx, Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: home, SharedDir: home})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Kill)
	leftovers, err := filepath.Glob(filepath.Join(home, "app-server-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("startup capability files retained: %v %v", leftovers, err)
	}

	th, err := srv.ThreadStart(ctx, ThreadStartParams{Cwd: home})
	if err != nil {
		t.Fatal(err)
	}
	return srv, responses, th.ID
}
func realCall(t *testing.T, s *Server, method string, params, out any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Call(ctx, method, params, out); err != nil {
		t.Fatal(err)
	}
}

// This exercises the production transport and queue observer against native
// Codex, not a second implementation of the client in the fidelity probe.
func TestRealCodexSharedClientQueue(t *testing.T) {
	srv, responses, thread := realShared(t)
	var first struct {
		Turn Turn `json:"turn"`
	}
	realCall(t, srv, "turn/start", map[string]any{"threadId": thread, "input": inputs([]Input{{Text: "initial probe:hold:production"}}), "clientUserMessageId": "initial"}, &first)
	select {
	case <-responses.Started("production"):
	case <-time.After(10 * time.Second):
		t.Fatal("initial request did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queued := make(chan string, 1)
	started := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		_, err := srv.RunTurn(ctx, TurnStartParams{ThreadID: thread, ClientID: "production-queued", Input: []Input{{Text: "queued production input"}}, Queue: true, Model: "probe-model", Effort: "low", Queued: func(id string) { queued <- id }}, time.Second, TurnHooks{Started: func(id string) { started <- id }})
		done <- err
	}()
	select {
	case <-queued:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case id := <-started:
		t.Fatalf("queued input steered into %s", id)
	default:
	}
	responses.Release("production")
	select {
	case id := <-started:
		if id == first.Turn.ID {
			t.Fatal("queue reused active turn")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range responses.Requests() {
		found = found || strings.Contains(r.UserText, "queued production input")
	}
	if !found {
		t.Fatal("queued input never reached model")
	}
}

func TestRealCodexSharedClientCancellation(t *testing.T) {
	srv, responses, thread := realShared(t)
	realCall(t, srv, "turn/start", map[string]any{"threadId": thread, "input": inputs([]Input{{Text: "initial probe:hold:cancel"}}), "clientUserMessageId": "initial"}, nil)
	select {
	case <-responses.Started("cancel"):
	case <-time.After(10 * time.Second):
		t.Fatal("initial request did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queued := make(chan string, 1)
	cancelled := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := srv.RunTurn(ctx, TurnStartParams{ThreadID: thread, ClientID: "cancel-before-start", Input: []Input{{Text: "must not run"}}, Queue: true, QueueCancelled: cancelled, Queued: func(id string) { queued <- id }}, time.Second, TurnHooks{})
		done <- err
	}()
	id := <-queued
	deleted, err := srv.QueueDelete(ctx, thread, id)
	if err != nil || !deleted {
		t.Fatalf("delete: %v %v", deleted, err)
	}
	close(cancelled)
	if err := <-done; err != ErrQueueCancelled {
		t.Fatalf("cancelled queue: %v", err)
	}
	responses.Release("cancel")
}

func TestRealCodexSharedClientSecondConnection(t *testing.T) {
	srv, responses, thread := realShared(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	started := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		_, err := srv.RunTurn(ctx, TurnStartParams{ThreadID: thread, ClientID: "helper-start", Input: []Input{{Text: "initial probe:hold:two-clients"}}}, time.Second, TurnHooks{Started: func(id string) { started <- id }})
		done <- err
	}()
	turn := <-started
	select {
	case <-responses.Started("two-clients"):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	ep := srv.Endpoint()
	c, _, err := websocket.DefaultDialer.Dial(ep.URL, http.Header{"Authorization": []string{"Bearer " + ep.Token}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	call := func(id int, method string, p any) {
		t.Helper()
		if err := c.WriteJSON(map[string]any{"id": id, "method": method, "params": p}); err != nil {
			t.Fatal(err)
		}
		for {
			_, raw, err := c.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			var r struct {
				ID    int
				Error *RPCError
			}
			if json.Unmarshal(raw, &r) == nil && r.ID == id {
				if r.Error != nil {
					t.Fatal(r.Error)
				}
				return
			}
		}
	}
	call(1, "initialize", map[string]any{"clientInfo": map[string]any{"name": "tui-test", "version": "0"}, "capabilities": map[string]any{"experimentalApi": true}})
	call(2, "thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	call(3, "turn/steer", map[string]any{"threadId": thread, "expectedTurnId": turn, "input": inputs([]Input{{Text: "input from second connection"}}), "clientUserMessageId": "tui-steer"})
	c.Close() // a UI disconnect never stops the shared task
	responses.Release("two-clients")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	recorded, ok, err := srv.FindTurn(ctx, thread, "tui-steer")
	if err != nil || !ok || recorded.ID != turn {
		t.Fatalf("find steered input: %v %v %s", ok, err, recorded.ID)
	}
}

func TestRealCodexSharedClientQueueRecovery(t *testing.T) {
	srv, responses, thread := realShared(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	realCall(t, srv, "turn/start", map[string]any{"threadId": thread, "input": inputs([]Input{{Text: "initial probe:hold:recovery"}}), "clientUserMessageId": "initial"}, nil)
	select {
	case <-responses.Started("recovery"):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err := srv.QueueAdd(ctx, TurnStartParams{ThreadID: thread, ClientID: "recover-pending", Input: []Input{{Text: "pending recovery"}}})
	if err != nil {
		t.Fatal(err)
	}
	srv.Kill()
	recovered, err := Start(ctx, srv.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Kill()
	removed, err := recovered.RemoveQueuedInput(ctx, thread, "recover-pending")
	if err != nil || !removed {
		t.Fatalf("remove before resume: %v %v", removed, err)
	}
	if _, err = recovered.ThreadResume(ctx, thread); err != nil {
		t.Fatal(err)
	}
	if _, found, err := recovered.FindTurn(ctx, thread, "recover-pending"); err != nil || found {
		t.Fatalf("removed input unexpectedly ran: %v %v", found, err)
	}
}

// A configuration reload must not pay the stdio EOF timeout: WebSocket
// disconnect deliberately keeps the server alive for other native clients.
func TestRealCodexSharedClientCloseDoesNotWaitForEOF(t *testing.T) {
	srv, _, _ := realShared(t)
	started := time.Now()
	srv.Close(10 * time.Second)
	if elapsed := time.Since(started); elapsed >= 5*time.Second {
		t.Fatalf("shared close waited for stdin EOF: %s", elapsed)
	}
	select {
	case <-srv.Done():
	default:
		t.Fatal("shared server did not exit")
	}
}
