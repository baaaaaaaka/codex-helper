package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// Check is one assertion outcome.
type Check struct {
	Name     string   `json:"name"`
	Pass     bool     `json:"pass"`
	Expected string   `json:"expected"`
	Observed string   `json:"observed"`
	Evidence []string `json:"evidence,omitempty"`
}

// Verdict is the structured result of one scenario.
type Verdict struct {
	Scenario   string   `json:"scenario"`
	Guarantees []string `json:"guarantees"`
	Title      string   `json:"title"`
	SUT        string   `json:"sut"`
	Pass       bool     `json:"pass"`
	Seconds    float64  `json:"seconds"`
	Checks     []Check  `json:"checks"`
	Notes      []string `json:"notes,omitempty"`
	Violations []string `json:"violations,omitempty"`
	Blocked    []string `json:"blocked,omitempty"`
	Requests   int      `json:"requests"`
	Routes     []string `json:"routes,omitempty"`
	CodexCalls int      `json:"codex_calls"`
	OutputTail string   `json:"output_tail,omitempty"`
}

// Options configures a scenario.
type Options struct {
	Guarantees []string
	Title      string
	Budget     time.Duration // whole-scenario time budget (default 60s)
	Interval   time.Duration // SUT base poll interval (default 250ms)
	Graph      fakegraph.Options
	Factory    SUTFactory
	NoStart    bool
	// SUTEnv is passed to a subprocess SUT (see SUTConfig.ExtraEnv).
	SUTEnv map[string]string
	// WorkflowWebhook gives the owner a Teams Workflow webhook (served by
	// the fake, see fakegraph.NewWorkflowWebhook) that the SUT is
	// configured with before it starts (SUTConfig.WorkflowWebhookURL).
	WorkflowWebhook bool
}

// Scenario owns one isolated run.
type Scenario struct {
	T     *testing.T
	ID    string
	Opts  Options
	Graph *fakegraph.Server
	Env   *Env
	Codex *FakeCodex
	SUT   SUT
	// WorkflowWebhookURL is the owner's webhook when Options.WorkflowWebhook.
	WorkflowWebhookURL string
	started            time.Time
	budget             time.Time
	checks             []Check
	notes              []string
	dumped             bool
	ctx                context.Context
	cancel             context.CancelFunc
}

// DefaultFactory is used when Options.Factory is nil. The scenarios package
// sets it.
var DefaultFactory SUTFactory

// NewScenario builds the fake world and the SUT and (unless NoStart) starts
// the SUT.
func NewScenario(t *testing.T, id string, opts Options) *Scenario {
	t.Helper()
	if opts.Budget <= 0 {
		opts.Budget = 60 * time.Second
	}
	if opts.Interval <= 0 {
		opts.Interval = 250 * time.Millisecond
	}
	if opts.Factory == nil {
		opts.Factory = DefaultFactory
	}
	if opts.Factory == nil {
		t.Fatal("harness: no SUT factory configured")
	}
	g := fakegraph.New(opts.Graph)
	env := NewEnv(t, g)
	codex := NewFakeCodex()
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scenario{
		T: t, ID: id, Opts: opts, Graph: g, Env: env, Codex: codex,
		started: time.Now(), budget: time.Now().Add(opts.Budget), ctx: ctx, cancel: cancel,
	}
	if opts.WorkflowWebhook {
		s.WorkflowWebhookURL = g.NewWorkflowWebhook()
	}
	sut, err := opts.Factory(SUTConfig{Env: env, Graph: g, Codex: codex, Interval: opts.Interval, ExtraEnv: opts.SUTEnv, WorkflowWebhookURL: s.WorkflowWebhookURL, Logf: t.Logf})
	if err != nil {
		t.Fatalf("build SUT: %v", err)
	}
	s.SUT = sut
	t.Cleanup(s.cleanup)
	if !opts.NoStart {
		s.StartSUT()
	}
	return s
}

func (s *Scenario) cleanup() {
	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if s.SUT != nil && s.SUT.Running() {
		if err := s.SUT.Stop(stopCtx); err != nil {
			s.T.Logf("SUT stop during cleanup: %v", err)
			s.dumpGoroutines("cleanup stop failed")
		}
	}
	s.Codex.ReleaseAll()
	s.Codex.Close()
	s.Codex.KillAppServers()
	s.cancel()
	s.Graph.Close()
}

// StartSUT starts the SUT and fails the test if that is impossible.
func (s *Scenario) StartSUT() {
	s.T.Helper()
	if err := s.SUT.Start(s.ctx); err != nil {
		s.T.Fatalf("SUT start: %v\n%s", err, s.OutputTail(4000))
	}
}

// StopSUT stops gracefully.
func (s *Scenario) StopSUT() {
	s.T.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	if err := s.SUT.Stop(ctx); err != nil {
		s.dumpGoroutines("SUT stop failed")
		s.T.Fatalf("SUT stop: %v", err)
	}
}

// CrashSUT stops abruptly.
func (s *Scenario) CrashSUT() {
	s.T.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	if err := s.SUT.Crash(ctx); err != nil {
		s.dumpGoroutines("SUT crash failed")
		s.T.Fatalf("SUT crash: %v", err)
	}
}

// Note adds free-form context to the verdict.
func (s *Scenario) Note(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	s.notes = append(s.notes, msg)
	s.T.Logf("note: %s", msg)
}

// Check records an assertion. A failing check fails the test (without
// stopping it) so the remaining observations are still collected.
func (s *Scenario) Check(name string, pass bool, expected, observed string, evidence ...string) bool {
	s.T.Helper()
	s.checks = append(s.checks, Check{Name: name, Pass: pass, Expected: expected, Observed: observed, Evidence: evidence})
	if pass {
		s.T.Logf("CHECK PASS %s: %s", name, observed)
	} else {
		s.T.Errorf("CHECK FAIL %s: expected %s; observed %s\n%s", name, expected, observed, strings.Join(evidence, "\n"))
	}
	return pass
}

// WaitFor polls cond until it reports true, the timeout passes, or the
// scenario budget is exhausted. On timeout it records a failed check named
// "wait: <desc>", dumps goroutines once and returns false.
func (s *Scenario) WaitFor(desc string, timeout time.Duration, cond func() (bool, string)) bool {
	s.T.Helper()
	deadline := time.Now().Add(timeout)
	if deadline.After(s.budget) {
		deadline = s.budget
	}
	last := ""
	for {
		ok, state := cond()
		last = state
		if ok {
			return true
		}
		if time.Now().After(deadline) {
			break
		}
		if s.SUT != nil && !s.SUT.Running() && s.SUT.ExitErr() != nil {
			last += fmt.Sprintf(" (SUT exited: %v)", s.SUT.ExitErr())
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.dumpGoroutines("timeout waiting for " + desc)
	s.Check("wait: "+desc, false, "condition within "+timeout.String(), "timed out; last state: "+last, s.RequestTail(25)...)
	return false
}

// Poll is WaitFor without recording anything on timeout: it reports whether
// cond became true within timeout (bounded by the scenario budget).
func (s *Scenario) Poll(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	if deadline.After(s.budget) {
		deadline = s.budget
	}
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Require is WaitFor that aborts the scenario (after emitting the verdict)
// when the condition is not met.
func (s *Scenario) Require(desc string, timeout time.Duration, cond func() (bool, string)) {
	s.T.Helper()
	if !s.WaitFor(desc, timeout, cond) {
		s.Finish()
		s.T.FailNow()
	}
}

// Settle waits until the SUT has made no Graph write for quiet, bounded by
// max. It is used before "exactly once" assertions so late duplicates get a
// chance to appear.
func (s *Scenario) Settle(quiet, max time.Duration) {
	deadline := time.Now().Add(max)
	if deadline.After(s.budget) {
		deadline = s.budget
	}
	for time.Now().Before(deadline) {
		writes := s.Graph.Requests(fakegraph.RequestFilter{})
		var last time.Time
		for _, r := range writes {
			if r.Method != "GET" && r.Host == "graph.microsoft.com" {
				last = r.At
			}
		}
		if time.Since(last) >= quiet {
			running := false
			for _, c := range s.Codex.Calls() {
				if c.Outcome == "running" {
					running = true
				}
			}
			if !running {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Finish emits the structured verdict. It is safe to call more than once;
// only the first call emits.
func (s *Scenario) Finish() Verdict {
	v := Verdict{
		Scenario:   s.ID,
		Guarantees: s.Opts.Guarantees,
		Title:      s.Opts.Title,
		SUT:        s.SUT.Name(),
		Seconds:    time.Since(s.started).Seconds(),
		Checks:     s.checks,
		Notes:      s.notes,
		Requests:   len(s.Graph.Requests(fakegraph.RequestFilter{})),
		Routes:     s.RequestsSummary(fakegraph.RequestFilter{}),
		CodexCalls: len(s.Codex.Calls()),
	}
	v.Pass = len(s.checks) > 0
	for _, c := range s.checks {
		if !c.Pass {
			v.Pass = false
		}
	}
	seen := map[string]int{}
	var order []string
	for _, viol := range s.Graph.Violations() {
		k := viol.Key()
		if seen[k] == 0 {
			order = append(order, k)
		}
		seen[k]++
	}
	for _, k := range order {
		v.Violations = append(v.Violations, fmt.Sprintf("%s (x%d)", k, seen[k]))
	}
	for _, b := range s.Graph.Blocked() {
		v.Blocked = append(v.Blocked, b.URL)
	}
	if !v.Pass {
		v.OutputTail = s.OutputTail(3000)
		s.logCodexLogs()
	}
	if s.dumped && s.T != nil {
		v.Notes = append(v.Notes, "goroutine dump emitted in test log")
	}
	data, _ := json.Marshal(v)
	s.T.Logf("E2E-VERDICT %s", data)
	if dir := os.Getenv("E2E_REPORT_DIR"); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
		_ = os.WriteFile(filepath.Join(dir, sanitize(s.ID)+".json"), data, 0o644)
	}
	return v
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		}
		return '_'
	}, s)
}

// logCodexLogs writes the fake app-server protocol logs (if any) to the test
// log for diagnosis.
func (s *Scenario) logCodexLogs() {
	files, _ := filepath.Glob(filepath.Join(s.Env.CodexHome, "e2e-fakecodex", "logs", "*.log"))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		text := string(data)
		if len(text) > 12000 {
			text = "…" + text[len(text)-12000:]
		}
		s.T.Logf("FAKE APP-SERVER LOG %s:\n%s", filepath.Base(f), text)
	}
}

// OutputTail returns the last n bytes of SUT output.
func (s *Scenario) OutputTail(n int) string {
	out := s.SUT.Output()
	if len(out) > n {
		return "…" + out[len(out)-n:]
	}
	return out
}

// RequestTail returns the last n recorded requests as strings.
func (s *Scenario) RequestTail(n int) []string {
	reqs := s.Graph.Requests(fakegraph.RequestFilter{})
	if len(reqs) > n {
		reqs = reqs[len(reqs)-n:]
	}
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.String())
	}
	return out
}

func (s *Scenario) dumpGoroutines(reason string) {
	if s.dumped {
		return
	}
	s.dumped = true
	buf := make([]byte, 8<<20)
	n := runtime.Stack(buf, true)
	stacks := strings.Split(string(buf[:n]), "\n\n")
	var keep []string
	for _, st := range stacks {
		if strings.Contains(st, "internal/teams") || strings.Contains(st, "internal/e2e") {
			keep = append(keep, st)
		}
	}
	dump := strings.Join(keep, "\n\n")
	if len(dump) > 60000 {
		dump = dump[:60000] + "\n…(truncated)"
	}
	s.T.Logf("GOROUTINE DUMP (%s): %d goroutines total, %d relevant\n%s", reason, len(stacks), len(keep), dump)
	s.T.Logf("SUT OUTPUT TAIL:\n%s", s.OutputTail(6000))
	// A subprocess SUT can dump its own goroutines, but that ends it, so
	// only on request (E2E_SUT_STACKS=1) while debugging a hang.
	if d, ok := s.SUT.(interface {
		DumpStacks(context.Context) string
	}); ok && os.Getenv("E2E_SUT_STACKS") == "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out := d.DumpStacks(ctx)
		cancel()
		var keep []string
		for _, st := range strings.Split(out, "\n\n") {
			if strings.Contains(st, "internal/teams") {
				keep = append(keep, st)
			}
		}
		dump := strings.Join(keep, "\n\n")
		if len(dump) > 120000 {
			dump = dump[:120000] + "\n…(truncated)"
		}
		s.T.Logf("SUT GOROUTINE DUMP (SIGQUIT; the SUT is gone now):\n%s", dump)
	}
}

// ---------------------------------------------------------------------------
// Observations (black box only: fake Teams, fake Codex, SUT output)

// ControlChatTopicMarker identifies the control chat by its visible title.
const ControlChatTopicMarker = "Codex Control"

// FindChat returns the first chat whose topic contains sub.
func (s *Scenario) FindChat(sub string) (fakegraph.ChatInfo, bool) {
	for _, c := range s.Graph.Chats() {
		if strings.Contains(c.Topic, sub) {
			return c, true
		}
	}
	return fakegraph.ChatInfo{}, false
}

// WaitControlChat waits for the control chat to exist and returns it.
func (s *Scenario) WaitControlChat(timeout time.Duration) fakegraph.ChatInfo {
	s.T.Helper()
	var chat fakegraph.ChatInfo
	s.Require("control chat created", timeout, func() (bool, string) {
		c, ok := s.FindChat(ControlChatTopicMarker)
		chat = c
		return ok, fmt.Sprintf("%d chats", len(s.Graph.Chats()))
	})
	return chat
}

// SUTMessages returns messages the SUT posted into chatID.
func (s *Scenario) SUTMessages(chatID string) []fakegraph.Message {
	var out []fakegraph.Message
	for _, m := range s.Graph.Messages(chatID) {
		if m.Origin == fakegraph.OriginSUT {
			out = append(out, m)
		}
	}
	return out
}

// SUTMessagesContaining returns SUT messages in chatID whose plain text
// contains sub. chatID "" searches every chat.
func (s *Scenario) SUTMessagesContaining(chatID, sub string) []fakegraph.Message {
	var msgs []fakegraph.Message
	if chatID == "" {
		msgs = s.Graph.AllMessages()
	} else {
		msgs = s.Graph.Messages(chatID)
	}
	var out []fakegraph.Message
	sub = NormalizeSpace(sub)
	for _, m := range msgs {
		if m.Origin == fakegraph.OriginSUT && strings.Contains(NormalizeSpace(m.PlainText()), sub) {
			out = append(out, m)
		}
	}
	return out
}

// NormalizeSpace collapses all whitespace runs to one space so assertions do
// not depend on how the SUT breaks lines in HTML.
func NormalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// WaitPolled waits until the SUT listed chatID's messages after request
// sequence since. It returns the sequence of that poll.
func (s *Scenario) WaitPolled(chatID string, since int64, timeout time.Duration) (int64, bool) {
	s.T.Helper()
	var seq int64
	ok := s.WaitFor("SUT polls chat "+short(chatID), timeout, func() (bool, string) {
		reqs := s.Graph.Requests(fakegraph.RequestFilter{Route: "GET /chats/{chat}/messages", ChatID: chatID, Since: since, OnlyOK: true})
		if len(reqs) > 0 {
			seq = reqs[0].Seq
			return true, ""
		}
		return false, "no successful list of that chat yet"
	})
	return seq, ok
}

// WaitSUTMessage waits for a SUT message in chatID containing sub.
func (s *Scenario) WaitSUTMessage(chatID, sub string, timeout time.Duration) (fakegraph.Message, bool) {
	s.T.Helper()
	var found fakegraph.Message
	ok := s.WaitFor(fmt.Sprintf("SUT message containing %q in %s", sub, short(chatID)), timeout, func() (bool, string) {
		msgs := s.SUTMessagesContaining(chatID, sub)
		if len(msgs) > 0 {
			found = msgs[0]
			return true, ""
		}
		return false, fmt.Sprintf("%d SUT messages in chat", len(s.SUTMessages(chatID)))
	})
	return found, ok
}

// WaitCodexCalls waits until at least n Codex calls contain token.
func (s *Scenario) WaitCodexCalls(token string, n int, timeout time.Duration) bool {
	s.T.Helper()
	return s.WaitFor(fmt.Sprintf("%d codex call(s) for %s", n, token), timeout, func() (bool, string) {
		got := len(s.Codex.CallsWithToken(token))
		return got >= n, fmt.Sprintf("%d calls", got)
	})
}

// Transcript renders a chat as lines for evidence.
func (s *Scenario) Transcript(chatID string, maxLines int) []string {
	msgs := s.Graph.Messages(chatID)
	var out []string
	for _, m := range msgs {
		text := strings.Join(strings.Fields(m.PlainText()), " ")
		if len(text) > 160 {
			text = text[:160] + "…"
		}
		out = append(out, fmt.Sprintf("[%s %s %s] %s", m.ID, m.Created.Format("15:04:05.000"), m.Origin, text))
	}
	if maxLines > 0 && len(out) > maxLines {
		out = append([]string{fmt.Sprintf("…%d earlier", len(out)-maxLines)}, out[len(out)-maxLines:]...)
	}
	return out
}

// RequestsSummary groups requests by route and status.
func (s *Scenario) RequestsSummary(f fakegraph.RequestFilter) []string {
	counts := map[string]int{}
	for _, r := range s.Graph.Requests(f) {
		key := fmt.Sprintf("%s -> %d", r.Route, r.Status)
		if r.Route == "" {
			key = fmt.Sprintf("%s %s -> %d", r.Method, r.Path, r.Status)
		}
		if r.Fault != "" {
			key += " [" + r.Fault + "]"
		}
		counts[key]++
	}
	var out []string
	for k, n := range counts {
		out = append(out, fmt.Sprintf("%5d  %s", n, k))
	}
	sort.Strings(out)
	return out
}

// Token returns a unique token for embedding in a message.
func (s *Scenario) Token(label string) string {
	return fmt.Sprintf("E2E-%s-%s-%d", sanitize(strings.ReplaceAll(s.ID, "-", "")), label, time.Now().UnixNano()%1000000)
}

func short(id string) string {
	if len(id) > 24 {
		return id[:24] + "…"
	}
	return id
}
