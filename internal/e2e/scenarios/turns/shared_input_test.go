package turns

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

type tuiClient struct {
	thread string
	t      *testing.T
	c      *websocket.Conn
	lease  io.ReadCloser
	next   int
}

func (c *tuiClient) close() { _ = c.c.Close(); _ = c.lease.Close() }
func (c *tuiClient) call(method string, p any) json.RawMessage {
	c.t.Helper()
	c.next++
	id := c.next
	_ = c.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := c.c.WriteJSON(map[string]any{"id": id, "method": method, "params": p}); err != nil {
		c.t.Fatal(err)
	}
	for {
		_, raw, err := c.c.ReadMessage()
		if err != nil {
			c.t.Fatal(err)
		}
		var r struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  any             `json:"error"`
		}
		if json.Unmarshal(raw, &r) == nil && r.ID == id {
			if r.Error != nil {
				c.t.Fatalf("%s: %s", method, raw)
			}
			return r.Result
		}
	}
}
func attachTUI(s *harness.Scenario, thread string) *tuiClient { return attachTUIRequest(s, thread, "") }
func attachTUIRequest(s *harness.Scenario, thread, cwd string) *tuiClient {
	s.T.Helper()
	var descriptor string
	_ = filepath.WalkDir(s.Env.StateDir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), "tui-endpoint-") && strings.HasSuffix(d.Name(), ".sqlite") {
			descriptor = path
		}
		return nil
	})
	db, err := sql.Open("sqlite", descriptor+"?mode=ro")
	if err != nil {
		s.T.Fatal(err)
	}
	var endpoint struct{ URL, Token, ThreadID string }
	err = db.QueryRow(`SELECT url,token FROM endpoint WHERE id=1`).Scan(&endpoint.URL, &endpoint.Token)
	closeErr := db.Close()
	if err != nil {
		s.T.Fatal(err)
	}
	if closeErr != nil {
		s.T.Fatal(closeErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", endpoint.URL+"/attach", strings.NewReader(`{"threadId":`+fmt.Sprintf("%q", thread)+`,"cwd":`+fmt.Sprintf("%q", cwd)+`}`))
	req.Header.Set("Authorization", "Bearer "+endpoint.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		s.T.Fatal(err)
	}
	s.T.Cleanup(cancel)
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		s.T.Fatalf("attach: %s %s", resp.Status, body)
	}
	if err = json.NewDecoder(resp.Body).Decode(&endpoint); err != nil {
		s.T.Fatal(err)
	}
	c, _, err := websocket.DefaultDialer.Dial(endpoint.URL, http.Header{"Authorization": []string{"Bearer " + endpoint.Token}})
	if err != nil {
		s.T.Fatal(err)
	}
	if thread == "" {
		thread = endpoint.ThreadID
	}
	tui := &tuiClient{thread: thread, t: s.T, c: c, lease: resp.Body}
	s.T.Cleanup(tui.close)
	tui.call("initialize", map[string]any{"clientInfo": map[string]any{"name": "native-tui-contract", "version": "0"}, "capabilities": map[string]any{"experimentalApi": true}})
	tui.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	return tui
}
func tuiInput(thread, prompt, client string) map[string]any {
	return map[string]any{"threadId": thread, "clientUserMessageId": client, "input": []any{map[string]any{"type": "text", "text": prompt, "text_elements": []any{}}}}
}

func TestSharedTeamsAndTUIInput(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "SHARED-INPUT", harness.Options{Title: "Teams defaults to steer, explicit queue stays separate, and native clients share both execution directions", Budget: 120 * time.Second})
	defer s.Finish()
	first, queued, extra, local, external, foreignExtra := s.Token("first"), s.Token("queued"), s.Token("extra"), s.Token("local"), s.Token("external"), s.Token("foreignextra")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		return harness.CodexBehavior{Block: c.HasToken(first) || c.HasToken(external)}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "shared-input")
	steps.MustPost(s, work.ID, "Initial task "+first)
	s.WaitCodexCalls(first, 1, steps.AnswerTimeout)
	original := s.Codex.CallsWithToken(first)[0]
	tui := attachTUI(s, original.ThreadID)
	steps.MustPost(s, work.ID, "helper queue Later task "+queued)
	steps.MustPost(s, work.ID, "Change direction "+extra)
	s.WaitFor("Teams ordinary input reaches the existing native task", 20*time.Second, func() (bool, string) {
		raw := tui.call("thread/turns/list", map[string]any{"threadId": original.ThreadID, "limit": 5, "sortDirection": "desc"})
		return strings.Contains(string(raw), extra), string(raw)
	})
	s.Check("ordinary input uses no extra execution", len(s.Codex.Calls()) == 1, "1 execution", fmt.Sprint(len(s.Codex.Calls())))
	p := tuiInput(original.ThreadID, "TUI addition "+local, "tui-local")
	p["expectedTurnId"] = original.TurnID
	tui.call("turn/steer", p)
	s.WaitFor("native TUI input is mirrored during a helper execution", 20*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, local)) > 0, "TUI input not mirrored"
	})
	tui.close()
	s.Codex.Release(original.Seq)
	steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	steps.WaitAnswered(s, work.ID, queued, steps.AnswerTimeout)
	s.Check("queue creates its own execution", len(s.Codex.CallsWithToken(queued)) == 1, "1", fmt.Sprint(len(s.Codex.CallsWithToken(queued))))

	// The opposite direction: a native task is already running when Teams
	// sends input. It is steered directly, with no helper-owned duplicate run.
	tui = attachTUI(s, original.ThreadID)
	tui.call("turn/start", tuiInput(original.ThreadID, "Native task "+external, "tui-start"))
	s.WaitCodexCalls(external, 1, steps.AnswerTimeout)
	ext := s.Codex.CallsWithToken(external)[0]
	// The native prompt mirror and a later Teams acknowledgement use
	// independent event paths. Observe the prompt before sending the next
	// input so the transcript has a defined order without delaying production.
	s.Require("native task prompt is mirrored before Teams adds input", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Native task "+external)) == 1, "waiting for native prompt mirror"
	})
	steps.MustPost(s, work.ID, "Teams addition "+foreignExtra)
	s.WaitFor("Teams steers native execution", 20*time.Second, func() (bool, string) {
		raw := tui.call("thread/turns/list", map[string]any{"threadId": original.ThreadID, "limit": 5, "sortDirection": "desc"})
		return strings.Contains(string(raw), foreignExtra), string(raw)
	})
	s.Check("both directions share one backend PID", ext.Pid == original.Pid, fmt.Sprint(original.Pid), fmt.Sprint(ext.Pid))
	tui.close()
	s.Codex.Release(ext.Seq)
	steps.WaitAnswered(s, work.ID, external, steps.AnswerTimeout)
	s.Settle(3*time.Second, 12*time.Second)
	s.Check("shared task answers are posted once", len(steps.AnswerPosts(s, work.ID, first)) == 1 && len(steps.AnswerPosts(s, work.ID, external)) == 1, "one answer per execution", fmt.Sprintf("%d/%d", len(steps.AnswerPosts(s, work.ID, first)), len(steps.AnswerPosts(s, work.ID, external))))
}

func TestSharedTUIBeforeTeamsWorkChat(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "SHARED-LOCAL-FIRST", harness.Options{Title: "a local session attaches before its Teams Work chat exists, then Teams steers that same backend", NoStart: true, Budget: 90 * time.Second})
	defer s.Finish()
	cwd := s.Env.WorkDir(t, "local-first")
	old := harness.NewRolloutAt(t, s.Env, cwd, time.Now().Add(-time.Hour))
	old.AppendTurn(t, "old private prompt", "", "old private answer")
	if err := old.SetModTime(time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	disk := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
	if err := disk.Save(&fakecodex.Thread{ID: old.SessionID, Cwd: cwd, RolloutPath: old.Path, Turns: []*fakecodex.Turn{}}); err != nil {
		t.Fatal(err)
	}
	active, extra := s.Token("active"), s.Token("extra")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		return harness.CodexBehavior{Block: c.HasToken(active)}
	})
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	tui := attachTUI(s, old.SessionID)
	for _, chat := range s.Graph.Chats() {
		s.Check("opening a window does not create a work chat", !strings.HasPrefix(chat.Topic, "💬"), "no Work chat", chat.Topic)
	}
	tui.call("turn/start", tuiInput(old.SessionID, "Local first "+active, "local-first"))
	s.WaitCodexCalls(active, 1, steps.AnswerTimeout)
	var work string
	s.Require("local input creates its Work chat", 20*time.Second, func() (bool, string) {
		for _, chat := range s.Graph.Chats() {
			if chat.ID != control.ID && len(s.SUTMessagesContaining(chat.ID, active)) > 0 {
				work = chat.ID
				return true, work
			}
		}
		return false, "not yet"
	})
	steps.MustPost(s, work, "Teams addition "+extra)
	s.Require("Teams input reaches that native task", 20*time.Second, func() (bool, string) {
		raw := tui.call("thread/turns/list", map[string]any{"threadId": old.SessionID, "limit": 5, "sortDirection": "desc"})
		return strings.Contains(string(raw), extra), string(raw)
	})
	s.Check("adoption does not start a second task", len(s.Codex.Calls()) == 1, "one task", fmt.Sprint(len(s.Codex.Calls())))
	s.Check("old history remains private", len(s.SUTMessagesContaining(work, "old private")) == 0, "no old history", strings.Join(s.Transcript(work, 12), " | "))
	tui.close()
	s.Codex.Release(s.Codex.CallsWithToken(active)[0].Seq)
	steps.WaitAnswered(s, work, active, steps.AnswerTimeout)
}

func TestSharedNewTUIStartsBeforeTeamsChat(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "SHARED-NEW-TUI", harness.Options{Title: "new cxp TUI session joins the helper backend before its first prompt", Budget: 90 * time.Second})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	cwd := s.Env.WorkDir(t, "new-tui")
	local, extra := s.Token("local"), s.Token("extra")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		return harness.CodexBehavior{Block: c.HasToken(local)}
	})
	first := attachTUIRequest(s, "", cwd)
	second := attachTUIRequest(s, "", cwd)
	s.Check("two empty windows get distinct sessions", first.thread != "" && second.thread != "" && first.thread != second.thread, "two distinct thread IDs", first.thread+" "+second.thread)
	second.close()
	first.call("turn/start", tuiInput(first.thread, "New local task "+local, "new-native"))
	s.WaitCodexCalls(local, 1, steps.AnswerTimeout)
	var chat string
	s.WaitFor("first local prompt creates its Work chat", 20*time.Second, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if c.ID != control.ID && len(s.SUTMessagesContaining(c.ID, local)) > 0 {
				chat = c.ID
				return true, chat
			}
		}
		return false, "not yet"
	})
	if chat == "" {
		t.Fatal("no Work chat")
	}
	steps.MustPost(s, chat, "New Teams addition "+extra)
	s.Require("Teams steers the newly created TUI task", 20*time.Second, func() (bool, string) {
		raw := first.call("thread/turns/list", map[string]any{"threadId": first.thread, "limit": 5, "sortDirection": "desc"})
		return strings.Contains(string(raw), extra), string(raw)
	})
	s.Check("new TUI and Teams share one execution", len(s.Codex.Calls()) == 1, "1 execution", fmt.Sprint(len(s.Codex.Calls())))
	first.close()
	s.Codex.Release(s.Codex.CallsWithToken(local)[0].Seq)
	steps.WaitAnswered(s, chat, local, steps.AnswerTimeout)
}
