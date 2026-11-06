//go:build !windows

package realcodex

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The probe needs a real codex binary and must run offline with a throwaway
// HOME. Example (from the repository root, inside the isolated container):
//
//	E2E_REAL_CODEX=/codexbin/codex go test -v -run RealCodex ./internal/e2e/realcodex/
//
// Every observation is logged as a line starting with "EVIDENCE".

func codexBin(t *testing.T) string {
	t.Helper()
	p := os.Getenv("E2E_REAL_CODEX")
	if p == "" {
		t.Skip("E2E_REAL_CODEX not set: real-Codex fidelity probe is opt-in")
	}
	if os.Getenv("E2E_REAL_CODEX_CLIENT") != "" {
		t.Skip("client helper process")
	}
	return p
}

type probe struct {
	t    *testing.T
	bin  string
	dir  string
	home string
	resp *Responses
	logs []string
}

func newProbe(t *testing.T) *probe {
	bin := codexBin(t)
	dir := t.TempDir()
	p := &probe{t: t, bin: bin, dir: dir, home: filepath.Join(dir, "codex-home"), resp: NewResponses()}
	t.Cleanup(p.resp.Close)
	if err := WriteCodexHome(p.home, p.resp.URL()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			for _, l := range p.logs {
				data, _ := os.ReadFile(l)
				t.Logf("---- %s (tail) ----\n%s", filepath.Base(l), tail(string(data), 4000))
			}
		}
	})
	return p
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func (p *probe) evidence(name string, v any) {
	data, _ := json.Marshal(v)
	p.t.Logf("EVIDENCE %s: %s", name, data)
}

func (p *probe) start(name string) *AppServer {
	p.t.Helper()
	log := filepath.Join(p.dir, name+".log")
	p.logs = append(p.logs, log)
	a, err := StartAppServer(p.bin, p.home, log, nil)
	if err != nil {
		p.t.Fatal(err)
	}
	p.t.Cleanup(a.Kill)
	if err := a.Initialize(); err != nil {
		p.t.Fatalf("initialize %s: %v", name, err)
	}
	return a
}

func (p *probe) startThread(a *AppServer) string {
	p.t.Helper()
	res, rerr, err := a.Call("thread/start", map[string]any{"cwd": p.dir}, 60*time.Second)
	if err != nil || rerr != nil {
		p.t.Fatalf("thread/start: %v %v", err, rerr)
	}
	var out struct {
		Thread struct {
			ID   string `json:"id"`
			Path string `json:"path"`
		} `json:"thread"`
	}
	_ = json.Unmarshal(res, &out)
	if out.Thread.ID == "" {
		p.t.Fatalf("thread/start result without id: %s", res)
	}
	return out.Thread.ID
}

func turnStartParams(threadID, text, clientID string) map[string]any {
	params := map[string]any{
		"threadId": threadID,
		"input":    []any{map[string]any{"type": "text", "text": text, "text_elements": []any{}}},
	}
	if clientID != "" {
		params["clientUserMessageId"] = clientID
	}
	return params
}

func startTurn(a *AppServer, threadID, text, clientID string) (string, json.RawMessage, *RPCError, error) {
	res, rerr, err := a.Call("turn/start", turnStartParams(threadID, text, clientID), 60*time.Second)
	var out struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	_ = json.Unmarshal(res, &out)
	return out.Turn.ID, res, rerr, err
}

func (p *probe) mustTurn(a *AppServer, threadID, text, clientID string) string {
	p.t.Helper()
	id, res, rerr, err := startTurn(a, threadID, text, clientID)
	if err != nil || rerr != nil || id == "" {
		p.t.Fatalf("turn/start: %v %v %s", err, rerr, res)
	}
	return id
}

func waitCompleted(a *AppServer, turnID string, timeout time.Duration) (RPCMessage, bool) {
	return a.WaitNote(IsMethod("turn/completed", `"`+turnID+`"`), timeout)
}

func (p *probe) readThread(a *AppServer, threadID string) (json.RawMessage, *RPCError) {
	p.t.Helper()
	res, rerr, err := a.Call("thread/read", map[string]any{"threadId": threadID, "includeTurns": true}, 60*time.Second)
	if err != nil {
		p.t.Fatalf("thread/read: %v", err)
	}
	return res, rerr
}

// turnSummary condenses a thread/read result.
type turnSummary struct {
	ID       string   `json:"id"`
	Status   string   `json:"status"`
	Error    any      `json:"error,omitempty"`
	Items    []string `json:"items"`
	ClientID []any    `json:"userMessageClientIds"`
	Answer   string   `json:"answer,omitempty"`
}

func summarizeThread(raw json.RawMessage) []turnSummary {
	var out struct {
		Thread struct {
			Turns []struct {
				ID     string           `json:"id"`
				Status string           `json:"status"`
				Error  any              `json:"error"`
				Items  []map[string]any `json:"items"`
			} `json:"turns"`
		} `json:"thread"`
	}
	_ = json.Unmarshal(raw, &out)
	var sums []turnSummary
	for _, t := range out.Thread.Turns {
		s := turnSummary{ID: t.ID, Status: t.Status, Error: t.Error}
		for _, it := range t.Items {
			typ, _ := it["type"].(string)
			s.Items = append(s.Items, typ)
			if typ == "userMessage" {
				cid, present := it["clientId"]
				if !present {
					cid = "<absent>"
				}
				s.ClientID = append(s.ClientID, cid)
			}
			if typ == "agentMessage" {
				s.Answer, _ = it["text"].(string)
			}
		}
		sums = append(sums, s)
	}
	return sums
}

// rolloutFacts greps a rollout for the facts the probe cares about.
func rolloutFacts(path string, needles ...string) map[string]any {
	facts := map[string]any{"path": filepath.Base(path)}
	data, err := os.ReadFile(path)
	if err != nil {
		facts["error"] = err.Error()
		return facts
	}
	text := string(data)
	var kinds []string
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		var rec struct {
			Type    string         `json:"type"`
			Payload map[string]any `json:"payload"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		k := rec.Type
		if pt, _ := rec.Payload["type"].(string); pt != "" {
			k += "/" + pt
		}
		if item, ok := rec.Payload["item"].(map[string]any); ok {
			if it, _ := item["type"].(string); it != "" {
				k += "/" + it
			}
			if cid, ok := item["client_id"]; ok {
				k += fmt.Sprintf("(client_id=%v)", cid)
			}
		}
		kinds = append(kinds, k)
	}
	facts["records"] = kinds
	for _, n := range needles {
		facts["contains:"+n] = strings.Contains(text, n)
	}
	return facts
}

// TestRealCodexBaselineAndClientID answers probe question (b): is
// clientUserMessageId echoed as clientId on thread/read and as client_id in
// the rollout?
func TestRealCodexBaselineAndClientID(t *testing.T) {
	p := newProbe(t)
	a := p.start("baseline")
	threadID := p.startThread(a)
	const cid = "teams-probe-cid-0001"
	turnID := p.mustTurn(a, threadID, "hello probe baseline", cid)
	done, ok := waitCompleted(a, turnID, 90*time.Second)
	if !ok {
		p.evidence("b.notes", noteMethods(a.Notes()))
		t.Fatalf("turn did not complete")
	}
	p.evidence("b.turn_completed", json.RawMessage(done.Params))
	p.evidence("b.notification_methods", noteMethods(a.Notes()))
	for _, n := range a.Notes() {
		if n.Method == "item/completed" || n.Method == "item/started" {
			if strings.Contains(string(n.Params), "userMessage") {
				p.evidence("b.item_notification_userMessage", json.RawMessage(n.Params))
			}
		}
	}
	raw, rerr := p.readThread(a, threadID)
	p.evidence("b.thread_read_error", rerr)
	p.evidence("b.thread_read_turns", summarizeThread(raw))
	p.evidence("b.thread_read_has_clientId_value", strings.Contains(string(raw), cid))
	path := FindRollout(p.home, threadID)
	p.evidence("b.rollout", rolloutFacts(path, cid, `"client_id":"`+cid+`"`))
	p.evidence("b.model_requests", p.resp.Requests())
}

func noteMethods(notes []RPCMessage) []string {
	var out []string
	for _, n := range notes {
		out = append(out, n.Method)
	}
	return out
}

// procAlive reports whether pid exists and is not a zombie.
func procAlive(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	s := string(data)
	if i := strings.LastIndex(s, ")"); i >= 0 && i+2 < len(s) {
		return s[i+2] != 'Z'
	}
	return true
}

// TestRealCodexInFlightTurnWhenClientDies answers probe question (a) for
// three ways the helper can die while a turn is running.
func TestRealCodexInFlightTurnWhenClientDies(t *testing.T) {
	for _, mode := range []string{"stdin-eof", "kill-client", "kill-group"} {
		t.Run(mode, func(t *testing.T) {
			p := newProbe(t)
			tag := strings.ReplaceAll(mode, "-", "_")
			prompt := "survive test probe:hold:" + tag
			cid := "teams-probe-" + tag
			var threadID string
			var serverPID int
			var client *exec.Cmd
			var inproc *AppServer
			switch mode {
			case "stdin-eof":
				inproc = p.start(mode)
				threadID = p.startThread(inproc)
				p.mustTurn(inproc, threadID, prompt, cid)
				serverPID = inproc.Cmd.Process.Pid
			default:
				client, threadID, serverPID = p.startClientProcess(mode, prompt, cid)
			}
			select {
			case <-p.resp.Started(tag):
			case <-time.After(90 * time.Second):
				t.Fatal("model request never arrived")
			}
			killedAt := time.Now()
			p.evidence("a."+mode+".killed_at", killedAt.Format(time.RFC3339Nano))
			switch mode {
			case "stdin-eof":
				_ = inproc.CloseStdin()
			case "kill-client":
				_ = client.Process.Kill()
				_ = client.Wait()
			case "kill-group":
				_ = syscall.Kill(-client.Process.Pid, syscall.SIGKILL)
				_ = client.Wait()
			}
			time.Sleep(2 * time.Second)
			aliveBeforeRelease := procAlive(serverPID)
			p.resp.Release(tag)
			// Watch the process and the rollout for up to 30s.
			var exitedAfter time.Duration = -1
			deadline := time.Now().Add(30 * time.Second)
			rollout := ""
			for time.Now().Before(deadline) {
				if exitedAfter < 0 && !procAlive(serverPID) {
					exitedAfter = time.Since(killedAt)
				}
				rollout = FindRollout(p.home, threadID)
				if exitedAfter >= 0 && rollout != "" {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			stillAlive := procAlive(serverPID)
			if stillAlive {
				_ = syscall.Kill(serverPID, syscall.SIGKILL)
			}
			var afterEOF []string
			if inproc != nil {
				<-inproc.Done()
				for _, n := range inproc.Notes() {
					if n.At.After(killedAt) {
						afterEOF = append(afterEOF, n.Method)
					}
				}
				p.evidence("a."+mode+".exit", fmt.Sprint(inproc.ExitErr()))
			}
			p.evidence("a."+mode+".process", map[string]any{
				"alive_2s_after_death_before_model_finished": aliveBeforeRelease,
				"exited_after":                  exitedAfter.String(),
				"still_alive_after_30s":         stillAlive,
				"notifications_after_stdin_eof": afterEOF,
			})
			p.evidence("a."+mode+".model_requests", p.resp.Requests())
			p.evidence("a."+mode+".rollout", rolloutFacts(rollout, Answer(tag), cid, "task_complete", "turn_aborted"))
			b := p.start(mode + "-postmortem")
			raw, rerr := p.readThread(b, threadID)
			p.evidence("a."+mode+".postmortem_thread_read_error", rerr)
			p.evidence("a."+mode+".postmortem_thread_read", summarizeThread(raw))
			// Can the next turn run on the thread (stale writer lock?).
			if _, rres, rerr, err := resume(b, threadID); err != nil || rerr != nil {
				p.evidence("a."+mode+".postmortem_resume_error", map[string]any{"err": fmt.Sprint(err), "rpc": rerr, "res": rres})
			} else {
				id, _, terr, err := startTurn(b, threadID, "after death", "")
				p.evidence("a."+mode+".postmortem_next_turn_start", map[string]any{"turn": id, "rpc": terr, "err": fmt.Sprint(err)})
				if id != "" {
					m, ok := waitCompleted(b, id, 60*time.Second)
					p.evidence("a."+mode+".postmortem_next_turn_completed", map[string]any{"ok": ok, "params": json.RawMessage(nonEmpty(m.Params))})
				}
			}
		})
	}
}

func nonEmpty(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	return b
}

func resume(a *AppServer, threadID string) (string, json.RawMessage, *RPCError, error) {
	res, rerr, err := a.Call("thread/resume", map[string]any{"threadId": threadID}, 60*time.Second)
	return threadID, res, rerr, err
}

// startClientProcess re-executes the test binary as a stand-alone helper
// that owns the app-server, starts a turn and then waits to be killed.
func (p *probe) startClientProcess(mode, prompt, cid string) (*exec.Cmd, string, int) {
	p.t.Helper()
	log := filepath.Join(p.dir, mode+".log")
	p.logs = append(p.logs, log)
	cmd := exec.Command(os.Args[0], "-test.run=^TestRealCodexClientProcess$", "-test.v=false")
	cmd.Env = append(os.Environ(),
		"E2E_REAL_CODEX_CLIENT=1",
		"E2E_RC_HOME="+p.home, "E2E_RC_LOG="+log, "E2E_RC_CWD="+p.dir,
		"E2E_RC_PROMPT="+prompt, "E2E_RC_CID="+cid,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.StdoutPipe()
	if err != nil {
		p.t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		p.t.Fatal(err)
	}
	p.t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	ready := make(chan [2]string, 1)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "READY ") {
				f := strings.Fields(line)
				ready <- [2]string{f[1], f[2]}
			}
		}
	}()
	select {
	case r := <-ready:
		var pid int
		fmt.Sscan(r[1], &pid)
		return cmd, r[0], pid
	case <-time.After(120 * time.Second):
		p.t.Fatal("client process never became ready")
	}
	return nil, "", 0
}

// TestRealCodexClientProcess is the body of the re-executed client. It is a
// no-op in a normal run.
func TestRealCodexClientProcess(t *testing.T) {
	if os.Getenv("E2E_REAL_CODEX_CLIENT") == "" {
		t.Skip("only runs as the probe's client helper process")
	}
	a, err := StartAppServer(os.Getenv("E2E_REAL_CODEX"), os.Getenv("E2E_RC_HOME"), os.Getenv("E2E_RC_LOG"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Initialize(); err != nil {
		t.Fatal(err)
	}
	res, rerr, err := a.Call("thread/start", map[string]any{"cwd": os.Getenv("E2E_RC_CWD")}, 60*time.Second)
	if err != nil || rerr != nil {
		t.Fatalf("thread/start %v %v", err, rerr)
	}
	var out struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	_ = json.Unmarshal(res, &out)
	_, _, terr, err := startTurn(a, out.Thread.ID, os.Getenv("E2E_RC_PROMPT"), os.Getenv("E2E_RC_CID"))
	if err != nil || terr != nil {
		t.Fatalf("turn/start %v %v", err, terr)
	}
	fmt.Printf("READY %s %d\n", out.Thread.ID, a.Cmd.Process.Pid)
	<-a.Done()
}

// TestRealCodexActiveWriter answers probe question (c): what does turn/start
// (and thread/resume) return while another turn or another process owns the
// thread?
func TestRealCodexActiveWriter(t *testing.T) {
	p := newProbe(t)
	a := p.start("writer-a")
	threadID := p.startThread(a)
	first := p.mustTurn(a, threadID, "first probe:hold:writer", "cid-first")
	select {
	case <-p.resp.Started("writer"):
	case <-time.After(90 * time.Second):
		t.Fatal("model request never arrived")
	}
	// Same process, same thread, turn already running.
	id, res, rerr, err := startTurn(a, threadID, "second while running", "cid-second")
	p.evidence("c.same_process_turn_start_while_running", map[string]any{"turn": id, "result": json.RawMessage(nonEmpty(res)), "rpc": rerr, "err": fmt.Sprint(err)})
	p.evidence("c.same_process_second_turn_id_equals_running_turn", id == first)
	// Another process, same CODEX_HOME.
	b := p.start("writer-b")
	_, rres, rrerr, rerrx := resume(b, threadID)
	p.evidence("c.other_process_resume_while_running", map[string]any{"result_has_thread": strings.Contains(string(rres), threadID), "rpc": rrerr, "err": fmt.Sprint(rerrx)})
	id, res, rerr, err = startTurn(b, threadID, "from b while a runs", "cid-b1")
	p.evidence("c.other_process_turn_start_while_running", map[string]any{"turn": id, "result": json.RawMessage(nonEmpty(res)), "rpc": rerr, "err": fmt.Sprint(err)})
	if id != "" {
		m, ok := waitCompleted(b, id, 60*time.Second)
		p.evidence("c.other_process_turn_completed_while_running", map[string]any{"ok": ok, "params": json.RawMessage(nonEmpty(m.Params))})
	}
	p.resp.Release("writer")
	m, ok := waitCompleted(a, first, 90*time.Second)
	p.evidence("c.first_turn_completed", map[string]any{"ok": ok, "params": json.RawMessage(nonEmpty(m.Params))})
	// A is idle but still has the thread loaded.
	_, rres, rrerr, rerrx = resume(b, threadID)
	p.evidence("c.other_process_resume_while_owner_idle", map[string]any{"result_has_thread": strings.Contains(string(rres), threadID), "rpc": rrerr, "err": fmt.Sprint(rerrx)})
	id, res, rerr, err = startTurn(b, threadID, "from b while a idle", "cid-b2")
	p.evidence("c.other_process_turn_start_while_owner_idle", map[string]any{"turn": id, "result": json.RawMessage(nonEmpty(res)), "rpc": rerr, "err": fmt.Sprint(err)})
	if id != "" {
		m, ok := waitCompleted(b, id, 60*time.Second)
		p.evidence("c.other_process_turn_completed_while_owner_idle", map[string]any{"ok": ok, "params": json.RawMessage(nonEmpty(m.Params))})
	}
	for _, n := range b.Notes() {
		if n.Method == "error" || strings.Contains(n.Method, "warning") {
			p.evidence("c.b_notification."+n.Method, json.RawMessage(n.Params))
		}
	}
	// After A exits, B again.
	_ = a.CloseStdin()
	select {
	case <-a.Done():
	case <-time.After(30 * time.Second):
	}
	_, rres, rrerr, rerrx = resume(b, threadID)
	p.evidence("c.other_process_resume_after_owner_exit", map[string]any{"result_has_thread": strings.Contains(string(rres), threadID), "rpc": rrerr, "err": fmt.Sprint(rerrx)})
	id, res, rerr, err = startTurn(b, threadID, "from b after a exited", "cid-b3")
	p.evidence("c.other_process_turn_start_after_owner_exit", map[string]any{"turn": id, "result": json.RawMessage(nonEmpty(res)), "rpc": rerr, "err": fmt.Sprint(err)})
	if id != "" {
		m, ok := waitCompleted(b, id, 60*time.Second)
		p.evidence("c.other_process_turn_completed_after_owner_exit", map[string]any{"ok": ok, "params": json.RawMessage(nonEmpty(m.Params))})
	}
	raw, _ := p.readThread(b, threadID)
	p.evidence("c.final_thread_read", summarizeThread(raw))
}

// TestRealCodexCapacityErrors answers probe question (d): what does a
// provider overload / rate-limit error look like on the app-server wire?
func TestRealCodexCapacityErrors(t *testing.T) {
	p := newProbe(t)
	a := p.start("capacity")
	for _, marker := range []string{"http:429", "http:503", "failed-event"} {
		threadID := p.startThread(a)
		turnID := p.mustTurn(a, threadID, "capacity probe:"+marker, "cid-"+marker)
		m, ok := waitCompleted(a, turnID, 120*time.Second)
		p.evidence("d."+marker+".turn_completed", map[string]any{"ok": ok, "params": json.RawMessage(nonEmpty(m.Params))})
		for _, n := range a.Notes() {
			if n.Method == "error" && strings.Contains(string(n.Params), turnID) {
				p.evidence("d."+marker+".error_notification", json.RawMessage(n.Params))
			}
		}
		raw, _ := p.readThread(a, threadID)
		p.evidence("d."+marker+".thread_read", summarizeThread(raw))
	}
	p.evidence("d.model_requests", p.resp.Requests())
	p.evidence("d.all_notification_methods", noteMethods(a.Notes()))
}

// TestRealCodexForkRollout answers the fork question (F-WRK-10): what does
// the child's rollout hold right after thread/fork (the copied history up
// to the cutoff, with the parent's turn and client ids?), and does a fork
// cut at lastTurnId while a later turn runs leave the running turn out?
func TestRealCodexForkRollout(t *testing.T) {
	p := newProbe(t)
	a := p.start("fork")
	threadID := p.startThread(a)
	for _, cid := range []string{"cxp-probe-a", "cxp-probe-b"} {
		id := p.mustTurn(a, threadID, "prompt "+cid, cid)
		if _, ok := waitCompleted(a, id, 90*time.Second); !ok {
			t.Fatalf("turn %s did not complete", cid)
		}
	}
	raw, _ := p.readThread(a, threadID)
	turns := summarizeThread(raw)
	if len(turns) != 2 {
		t.Fatalf("parent turns: %+v", turns)
	}
	running := p.mustTurn(a, threadID, "running probe:hold:fork", "cxp-probe-c")
	select {
	case <-p.resp.Started("fork"):
	case <-time.After(90 * time.Second):
		t.Fatal("model request never arrived")
	}
	res, rerr, err := a.Call("thread/fork", map[string]any{"threadId": threadID, "lastTurnId": turns[1].ID}, 60*time.Second)
	p.evidence("f.fork_result", map[string]any{"result": json.RawMessage(nonEmpty(res)), "rpc": rerr, "err": fmt.Sprint(err)})
	var out struct {
		Thread struct {
			ID   string `json:"id"`
			Path string `json:"path"`
		} `json:"thread"`
	}
	_ = json.Unmarshal(res, &out)
	child := out.Thread.Path
	if child == "" {
		child = FindRollout(p.home, out.Thread.ID)
	}
	st, serr := os.Stat(child)
	p.evidence("f.child_rollout_right_after_fork", map[string]any{"path_in_result": out.Thread.Path != "", "stat_err": fmt.Sprint(serr), "size": func() int64 {
		if st != nil {
			return st.Size()
		}
		return -1
	}()})
	p.evidence("f.child_rollout_facts_after_fork", rolloutFacts(child, turns[0].ID, turns[1].ID, running, "cxp-probe-a", "cxp-probe-c", threadID))
	p.resp.Release("fork")
	if _, ok := waitCompleted(a, running, 90*time.Second); !ok {
		t.Fatal("running turn did not complete")
	}
	before := int64(-1)
	if st, err := os.Stat(child); err == nil {
		before = st.Size()
	}
	if _, rerr, err := a.Call("thread/resume", map[string]any{"threadId": out.Thread.ID}, 60*time.Second); err != nil || rerr != nil {
		p.evidence("f.child_resume_error", map[string]any{"rpc": rerr, "err": fmt.Sprint(err)})
	}
	own := p.mustTurn(a, out.Thread.ID, "child own turn", "cxp-probe-d")
	if _, ok := waitCompleted(a, own, 90*time.Second); !ok {
		t.Fatal("child turn did not complete")
	}
	p.evidence("f.child_rollout_size_before_own_turn", before)
	p.evidence("f.child_rollout_facts_after_own_turn", rolloutFacts(child, own, "cxp-probe-d"))
	craw, _ := p.readThread(a, out.Thread.ID)
	p.evidence("f.child_thread_read", summarizeThread(craw))
}
