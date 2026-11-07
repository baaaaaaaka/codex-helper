//go:build !windows

package realcodex

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// sharedClient exercises the native multi-client transport. No CXP broker
// or production internals participate in these protocol contract tests.
type sharedClient struct {
	t       *testing.T
	c       *websocket.Conn
	mu      sync.Mutex
	next    int
	pending map[int]chan RPCMessage
	notes   []RPCMessage
	wake    chan struct{}
	done    chan struct{}
}

func (p *probe) sharedServer() func() *sharedClient {
	p.t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		p.t.Fatal(err)
	}
	url := "ws://" + l.Addr().String()
	l.Close()
	token := "isolated-probe-capability-token"
	file := filepath.Join(p.dir, "ws-token")
	if err := os.WriteFile(file, []byte(token), 0600); err != nil {
		p.t.Fatal(err)
	}
	logPath := filepath.Join(p.dir, "shared.log")
	p.logs = append(p.logs, logPath)
	log, err := os.Create(logPath)
	if err != nil {
		p.t.Fatal(err)
	}
	cmd := exec.Command(p.bin, "app-server", "--listen", url, "--ws-auth", "capability-token", "--ws-token-file", file)
	cmd.Env, cmd.Dir, cmd.Stderr = CodexEnv(p.home), p.dir, log
	if err := cmd.Start(); err != nil {
		log.Close()
		p.t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() { _ = cmd.Wait(); log.Close(); close(stopped) }()
	p.t.Cleanup(func() { _ = cmd.Process.Kill(); <-stopped })
	return func() *sharedClient {
		deadline := time.Now().Add(10 * time.Second)
		var c *websocket.Conn
		for {
			c, _, err = websocket.DefaultDialer.Dial(url, http.Header{"Authorization": {"Bearer " + token}})
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				p.t.Fatal(err)
			}
			select {
			case <-stopped:
				p.t.Fatal("shared server exited")
			case <-time.After(10 * time.Millisecond):
			}
		}
		a := &sharedClient{t: p.t, c: c, pending: map[int]chan RPCMessage{}, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.read()
		p.t.Cleanup(func() { c.Close(); <-a.done })
		a.call("initialize", map[string]any{"clientInfo": map[string]any{"name": "shared-probe", "version": "0"}, "capabilities": map[string]any{"experimentalApi": true}})
		if err := c.WriteJSON(map[string]any{"method": "initialized"}); err != nil {
			p.t.Fatal(err)
		}
		return a
	}
}

func (a *sharedClient) read() {
	defer close(a.done)
	for {
		var m RPCMessage
		if err := a.c.ReadJSON(&m); err != nil {
			return
		}
		a.mu.Lock()
		if len(m.ID) > 0 && m.Method == "" {
			var id int
			_ = json.Unmarshal(m.ID, &id)
			if ch := a.pending[id]; ch != nil {
				delete(a.pending, id)
				ch <- m
			}
		} else {
			a.notes = append(a.notes, m)
		}
		a.mu.Unlock()
		select {
		case a.wake <- struct{}{}:
		default:
		}
	}
}

func (a *sharedClient) request(method string, params any) (json.RawMessage, *RPCError) {
	a.t.Helper()
	a.mu.Lock()
	a.next++
	id := a.next
	ch := make(chan RPCMessage, 1)
	a.pending[id] = ch
	a.mu.Unlock()
	if err := a.c.WriteJSON(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		a.t.Fatal(err)
	}
	select {
	case m := <-ch:
		return m.Result, m.Error
	case <-a.done:
		a.t.Fatal("shared connection closed")
	case <-time.After(15 * time.Second):
		a.t.Fatalf("%s timed out", method)
	}
	return nil, nil
}

func (a *sharedClient) call(method string, params any) json.RawMessage {
	a.t.Helper()
	result, err := a.request(method, params)
	if err != nil {
		a.t.Fatalf("%s: %v", method, err)
	}
	return result
}

func (a *sharedClient) wait(method, contains string) RPCMessage {
	a.t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		a.mu.Lock()
		for _, m := range a.notes {
			if m.Method == method && strings.Contains(string(m.Params), contains) {
				a.mu.Unlock()
				return m
			}
		}
		a.mu.Unlock()
		select {
		case <-a.wake:
		case <-deadline:
			a.t.Fatalf("missing %s %s", method, contains)
		case <-a.done:
			a.t.Fatal("connection closed")
		}
	}
}

func sharedThread(a *sharedClient, dir string) string {
	a.t.Helper()
	var r struct{ Thread struct{ ID string } }
	_ = json.Unmarshal(a.call("thread/start", map[string]any{"cwd": dir}), &r)
	if r.Thread.ID == "" {
		a.t.Fatal("missing thread id")
	}
	return r.Thread.ID
}

func sharedStart(a *sharedClient, thread, text, cid string) string {
	a.t.Helper()
	var r struct{ Turn struct{ ID string } }
	_ = json.Unmarshal(a.call("turn/start", turnStartParams(thread, text, cid)), &r)
	if r.Turn.ID == "" {
		a.t.Fatal("missing turn id")
	}
	return r.Turn.ID
}

func TestRealCodexSharedStartOrSteer(t *testing.T) {
	for _, stage := range []string{"hold", "finalhold"} {
		t.Run(stage, func(t *testing.T) {
			p := newProbe(t)
			connect := p.sharedServer()
			a, b := connect(), connect()
			thread := sharedThread(a, p.dir)
			turn := sharedStart(a, thread, "initial probe:"+stage+":boundary", "cid-initial")
			select {
			case <-p.resp.Started("boundary"):
			case <-time.After(10 * time.Second):
				t.Fatal("model did not reach barrier")
			}
			b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
			if stage == "finalhold" {
				m := a.wait("item/completed", "agentMessage")
				if !strings.Contains(string(m.Params), `"phase":"final_answer"`) {
					t.Fatalf("not a final answer: %s", m.Params)
				}
			}
			steered := sharedStart(b, thread, "follow-up-marker", "cid-follow-up")
			if steered != turn {
				t.Fatalf("steer created new turn: %s != %s", steered, turn)
			}
			p.resp.Release("boundary")
			a.wait("turn/completed", turn)
			b.wait("turn/completed", turn)
			seen := false
			for _, req := range p.resp.Requests() {
				if strings.Contains(req.UserText, "follow-up-marker") {
					seen = true
				}
			}
			if !seen {
				t.Fatal("steer accepted but absent from subsequent model requests")
			}
			p.evidence("shared."+stage, map[string]any{"turn": turn, "requests": p.resp.Requests()})
		})
	}
}

func TestRealCodexSharedDisconnectAndAuthentication(t *testing.T) {
	p := newProbe(t)
	connect := p.sharedServer()
	a, b := connect(), connect()
	unauth, response, err := websocket.DefaultDialer.Dial("ws://"+a.c.RemoteAddr().String(), nil)
	if unauth != nil {
		unauth.Close()
	}
	if response != nil {
		defer response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated connection: response=%v error=%v", response, err)
	}
	thread := sharedThread(a, p.dir)
	turn := sharedStart(a, thread, "initial probe:hold:disconnect", "cid-initial")
	select {
	case <-p.resp.Started("disconnect"):
	case <-time.After(10 * time.Second):
		t.Fatal("model did not reach barrier")
	}
	b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	a.c.Close()
	<-a.done
	c := connect()
	c.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	input := turnStartParams(thread, "after-disconnect-marker", "cid-after-disconnect")
	input["expectedTurnId"] = turn
	b.call("turn/steer", input)
	p.resp.Release("disconnect")
	b.wait("turn/completed", turn)
	c.wait("turn/completed", turn)
}

func TestRealCodexNativeQueueAdmission(t *testing.T) {
	p := newProbe(t)
	connect := p.sharedServer()
	a, b := connect(), connect()
	thread := sharedThread(a, p.dir)
	turn := sharedStart(a, thread, "initial probe:hold:queue", "cid-initial")
	select {
	case <-p.resp.Started("queue"):
	case <-time.After(10 * time.Second):
		t.Fatal("model did not reach barrier")
	}
	b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	q := b.call("thread/queue/add", turnStartParams(thread, "queued-marker", "cid-queued"))
	var added struct{ QueuedSubmission struct{ ID string } }
	_ = json.Unmarshal(q, &added)
	if added.QueuedSubmission.ID == "" {
		t.Fatalf("missing queued submission id: %s", q)
	}
	p.evidence("queue.add", json.RawMessage(q))
	_, rerr := b.request("thread/queue/start", map[string]any{"threadId": thread, "queuedSubmissionId": added.QueuedSubmission.ID})
	if rerr == nil || rerr.Code != -32600 || !strings.Contains(rerr.Message, "active or pending turn") {
		t.Fatalf("queue/start must report busy, got %v", rerr)
	}
	p.evidence("queue.busy", fmt.Sprint(rerr))
	p.resp.Release("queue")
	a.wait("turn/completed", turn)
	// Native queue ownership matters: does it run without a helper-issued
	// start, bypassing helper pause, outbox and capacity admission?
	queuedMessage := b.wait("item/completed", "cid-queued")
	var queued struct {
		TurnID string `json:"turnId"`
	}
	_ = json.Unmarshal(queuedMessage.Params, &queued)
	if queued.TurnID == "" || queued.TurnID == turn {
		t.Fatalf("queued input did not get an independent task: %s", queuedMessage.Params)
	}
	b.wait("turn/completed", queued.TurnID)
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, r := range p.resp.Requests() {
			if strings.Contains(r.UserText, "queued-marker") {
				p.evidence("queue.autostart", true)
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("queued input not presented to model")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A mutex in helper cannot serialize TUI input. Verify the native arbiter
// itself when both clients believe the thread is idle.
func TestRealCodexSharedConcurrentStart(t *testing.T) {
	for iteration := 0; iteration < 10; iteration++ {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			p := newProbe(t)
			connect := p.sharedServer()
			a, b := connect(), connect()
			thread := sharedThread(a, p.dir)
			seed := sharedStart(a, thread, "seed", "cid-seed")
			a.wait("turn/completed", seed)
			b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
			barrier := make(chan struct{})
			var wg sync.WaitGroup
			ids := make([]string, 2)
			for i, client := range []*sharedClient{a, b} {
				wg.Add(1)
				go func(i int, client *sharedClient) {
					defer wg.Done()
					<-barrier
					ids[i] = sharedStart(client, thread, "probe:hold:concurrent", fmt.Sprintf("cid-%d", i))
				}(i, client)
			}
			close(barrier)
			wg.Wait()
			if ids[0] == "" || ids[0] != ids[1] {
				t.Fatalf("simultaneous ordinary inputs created different tasks: %v", ids)
			}
			p.resp.Release("concurrent")
			a.wait("turn/completed", ids[0])
			b.wait("turn/completed", ids[0])
		})
	}
}

func TestRealCodexSharedSteerGuards(t *testing.T) {
	p := newProbe(t)
	connect := p.sharedServer()
	a, b := connect(), connect()
	thread := sharedThread(a, p.dir)
	turn := sharedStart(a, thread, "initial probe:hold:guards", "cid-initial")
	select {
	case <-p.resp.Started("guards"):
	case <-time.After(10 * time.Second):
		t.Fatal("model did not reach barrier")
	}
	b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	input := turnStartParams(thread, "guard-marker", "cid-guard")
	input["expectedTurnId"] = "wrong-turn"
	if _, err := b.request("turn/steer", input); err == nil {
		t.Fatal("mismatched target accepted")
	}
	input["expectedTurnId"] = turn
	b.call("turn/steer", input)
	p.resp.Release("guards")
	a.wait("turn/completed", turn)
	if _, err := b.request("turn/steer", input); err == nil {
		t.Fatal("steer on finished task accepted")
	}
}

// Extra turn/start fields on queue/add are silently ignored. The native
// queue cannot replace start-if-idle(input, model, effort, ...) without
// changing the helper's request settings contract.
func TestRealCodexNativeQueueUsesDispatchSettings(t *testing.T) {
	p := newProbe(t)
	connect := p.sharedServer()
	a, b := connect(), connect()
	thread := sharedThread(a, p.dir)
	turn := sharedStart(a, thread, "initial probe:hold:settings", "cid-initial")
	select {
	case <-p.resp.Started("settings"):
	case <-time.After(10 * time.Second):
		t.Fatal("model did not reach barrier")
	}
	b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	input := turnStartParams(thread, "settings-queue-marker", "cid-settings")
	input["model"] = "queued-request-model"
	b.call("thread/queue/add", input)
	b.call("thread/settings/update", map[string]any{"threadId": thread, "model": "changed-after-enqueue"})
	p.resp.Release("settings")
	a.wait("turn/completed", turn)
	b.wait("item/completed", "cid-settings")
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, req := range p.resp.Requests() {
			if strings.Contains(req.UserText, "settings-queue-marker") {
				if req.Model != "changed-after-enqueue" {
					t.Fatalf("native queue model = %q", req.Model)
				}
				p.evidence("queue.settings", req)
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("queued model request absent")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
