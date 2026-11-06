package harness

// Global invariants, checked at every scenario's teardown.
//
// Each invariant has a name, holds for every scenario unless the scenario
// waives it with a written reason (Invariants.Waive), and fails the
// scenario with a message naming the invariant. A fault scenario that
// expects a specific breach declares a narrow exception instead
// (Invariants.Except: the invariant, a substring of the failure message,
// and the reason).
//
// Teardown order: idle check (if the helper is quiescent) → Stop every
// helper → clean exit and leaked processes → CPU budget → transcript golden
// → the request-log invariants.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// Invariant names.
const (
	// InvNoDuplicatePost: no chat holds the same Codex answer twice, and
	// no restarted helper re-posts what an earlier instance posted when
	// nothing new happened in between (checkDuplicates).
	InvNoDuplicatePost = "no-duplicate-post"
	// InvChatOrder: per chat, helper output follows the source order: the
	// answers of one Codex thread in turn order, and mirrored local
	// records in rollout order.
	InvChatOrder = "chat-order"
	// InvNoPostAfterExit: no instance sends anything after it was stopped
	// or crashed (or exited).
	InvNoPostAfterExit = "no-post-after-exit"
	// InvIdleQuiet: an idle helper makes no Graph write (IdleWindow after
	// it settled), apart from the documented heartbeat (idleAllowed).
	InvIdleQuiet = "idle-quiet"
	// InvRateCap: message posts per chat, writes per second and per minute
	// stay under the caps (RateCaps).
	InvRateCap = "rate-cap"
	// InvRetryAfter: after a Retry-After, the same route of the same chat
	// is not requested again before it ends.
	InvRetryAfter = "retry-after"
	// InvNoLoop: no identical write repeated, no failing request retried
	// without bound, no chat read in a tight loop.
	InvNoLoop = "no-loop"
	// InvCatchUpSilent: records written while no helper ran (caught up at
	// the next start) are mirrored without @mention and without a card
	// (owner decision 2026-11-05: catch-up is silent).
	InvCatchUpSilent = "catch-up-silent"
	// InvCleanStop: every helper exits with status 0 within StopWithin of
	// SIGTERM, and no helper exits on its own with an error.
	InvCleanStop = "clean-stop"
	// InvNoLeakedProcess: once every helper stopped, no process of the
	// scenario (HOME = the scenario home) is left.
	InvNoLeakedProcess = "no-leaked-process"
	// InvCPUBudget: the helpers' CPU time (user+system, from /proc and the
	// exit status) stays within the budget.
	InvCPUBudget = "cpu-budget"
	// InvOffline: nothing tried to reach a host the fake does not serve.
	InvOffline = "offline"
	// InvGraphContract: no request real Graph would reject, or that the
	// fake does not model.
	InvGraphContract = "graph-contract"
	// InvLocalHistory: the local Codex history the scenario wrote (the
	// rollouts of a TUI user) is byte for byte what it wrote: the helper
	// only reads it (R-44).
	InvLocalHistory = "local-history-untouched"
)

// AllInvariants lists every invariant in check order.
var AllInvariants = []string{
	InvIdleQuiet, InvCleanStop, InvNoLeakedProcess, InvCPUBudget,
	InvNoDuplicatePost, InvChatOrder, InvNoPostAfterExit, InvRateCap,
	InvRetryAfter, InvNoLoop, InvCatchUpSilent, InvOffline, InvGraphContract,
	InvLocalHistory,
}

// Limits of the invariants.
const (
	IdleSettleQuiet = 3 * time.Second // longer than Timing.ChatRate gaps of a draining queue
	IdleSettleMax   = 15 * time.Second
	IdleWindow      = 3 * time.Second
	StopWithin      = 15 * time.Second
	// DefaultCPUBudget is the helpers' CPU budget of a scenario that sets
	// none (calibrated on the Docker runner: see E2E-BLACKBOX-REPORT.md).
	// The heaviest scenario used 0.6 s in the first calibration run.
	DefaultCPUBudget = 5 * time.Second

	// Rate caps. One helper paces all its Graph writes (message posts,
	// uploads, chat creation, the registry card) with one account token
	// bucket of 3.6/s holding one second of rate: at most 7 in any 1 s
	// and 3.6×61 ≈ 219 in any 60 s, 8% under CapWritesPerMinute; and at
	// most one message per chat per second (Timing.ChatRate). The caps
	// count every helper instance of a scenario together.
	CapChatPostsPerSecond = 2   // message posts to one chat in any 1 s
	CapPostsPerSecond     = 8   // message posts account-wide in any 1 s
	CapWritesPerSecond    = 20  // Graph writes of any kind in any 1 s
	CapWritesPerMinute    = 240 // Graph writes in any 60 s
	CapChatReadsPerSecond = 10  // reads of one chat's messages in any 1 s
	CapIdenticalWrites    = 5   // the same write (method, path, body)
	CapFailingRepeats     = 20  // the same request failing
	retryAfterTolerance   = 150 * time.Millisecond
)

// RegistryChatTopic is the machine registry chat; with the machine
// registry on, the helper edits its card there once per heartbeat (F-XM-2;
// production 5 min).
const RegistryChatTopic = "CXP Machine Registry"

// idleAllowed reports whether an idle helper may make write r: only the
// documented heartbeat, the edit of its machine card in the registry chat.
// The owner heartbeat itself is kept in the store, not in Teams.
func (s *Scenario) idleAllowed(r fakegraph.Request) bool {
	if r.Method != "PATCH" || r.Route != "PATCH /chats/{chat}/messages/{message}" {
		return false
	}
	for _, c := range s.Graph.Chats() {
		if c.ID == r.ChatID {
			return c.Topic == RegistryChatTopic
		}
	}
	return false
}

// Invariants adjusts the teardown invariants of one scenario.
type Invariants struct {
	// Waive turns invariants off: name -> reason (required).
	Waive map[string]string
	// Except accepts the failures of Invariant whose message matches
	// the regular expression Match (keep it narrow: name the instance).
	Except []Exception
	// CPUBudget overrides DefaultCPUBudget.
	CPUBudget time.Duration
}

// Exception is an expected, narrow breach of an invariant.
type Exception struct {
	Invariant string
	Match     string
	Reason    string
}

func (iv Invariants) validate() error {
	known := map[string]bool{}
	for _, n := range AllInvariants {
		known[n] = true
	}
	for n, r := range iv.Waive {
		if !known[n] {
			return fmt.Errorf("invariants: unknown invariant %q", n)
		}
		if strings.TrimSpace(r) == "" {
			return fmt.Errorf("invariants: waiving %s needs a reason", n)
		}
	}
	for _, e := range iv.Except {
		if !known[e.Invariant] || e.Match == "" || strings.TrimSpace(e.Reason) == "" {
			return fmt.Errorf("invariants: exception %+v needs a known invariant, a match and a reason", e)
		}
		if _, err := regexp.Compile(e.Match); err != nil {
			return fmt.Errorf("invariants: exception match %q: %v", e.Match, err)
		}
	}
	return nil
}

type invResults struct {
	fail map[string][]string
	note map[string]string
}

func (r *invResults) add(name, format string, args ...any) {
	r.fail[name] = append(r.fail[name], fmt.Sprintf(format, args...))
}

// teardown runs the invariants and the golden and emits the verdict.
func (s *Scenario) teardown() {
	res := &invResults{fail: map[string][]string{}, note: map[string]string{}}
	s.mu.Lock()
	suts := append([]SUT(nil), s.suts...)
	s.mu.Unlock()

	// 1. Idle: once the helper settled, IdleWindow without a Graph write.
	running := false
	for _, u := range suts {
		Thaw(u)
		if u.Running() {
			running = true
		}
	}
	if running && !s.T.Failed() {
		if s.Settle(IdleSettleQuiet, IdleSettleMax) {
			from := s.Graph.LastSeq()
			time.Sleep(IdleWindow)
			for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: from}) {
				if isGraphWrite(r) && !s.idleAllowed(r) {
					res.add(InvIdleQuiet, "idle helper wrote %s (%s) within %v after it settled: %s", r.Route, r.Instance, IdleWindow, clipBody(r.Body))
				}
			}
		} else {
			res.note[InvIdleQuiet] = "not checked: the helper did not settle (a turn or writes still running)"
		}
	} else if !running {
		res.note[InvIdleQuiet] = "not checked: no helper running at teardown"
	} else {
		res.note[InvIdleQuiet] = "not checked: the scenario already failed"
	}

	// 2. Stop every helper.
	for _, u := range suts {
		if !u.Running() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), StopWithin+15*time.Second)
		t0 := time.Now()
		err := u.Stop(ctx)
		cancel()
		if err != nil {
			res.add(InvCleanStop, "%s did not exit on SIGTERM: %v", u.Instance(), err)
			s.dumpGoroutines("teardown stop failed")
		} else if d := time.Since(t0); d > StopWithin {
			res.add(InvCleanStop, "%s took %v to exit on SIGTERM (limit %v)", u.Instance(), d.Round(time.Millisecond), StopWithin)
		}
	}
	var instances []InstanceRecord
	for _, u := range suts {
		if p, ok := u.(interface{ Instances() []InstanceRecord }); ok {
			instances = append(instances, p.Instances()...)
		}
	}
	for _, in := range instances {
		switch {
		case in.How == "stop" && in.ExitErr != "":
			res.add(InvCleanStop, "%s exited with %q on SIGTERM (want status 0)", in.Tag, in.ExitErr)
		case in.How == "self" && in.ExitErr != "":
			res.add(InvCleanStop, "%s exited on its own with %q", in.Tag, in.ExitErr)
		}
	}

	// 3. Leaked processes.
	if leaks := s.waitScenarioProcesses(5 * time.Second); len(leaks) > 0 {
		for _, l := range leaks {
			res.add(InvNoLeakedProcess, "process left after every helper stopped: %s", l)
		}
		s.killScenarioProcesses()
	}

	// 4. CPU budget.
	var cpu time.Duration
	for _, in := range instances {
		cpu += in.CPU
	}
	budget := s.Opts.Invariants.CPUBudget
	if budget <= 0 {
		budget = DefaultCPUBudget
	}
	if cpu > budget {
		res.add(InvCPUBudget, "helpers used %v CPU (budget %v)", cpu.Round(10*time.Millisecond), budget)
	}

	// 5. Transcript golden.
	golden := s.checkGolden(s.GoldenTranscript())

	// 6. Request log.
	reqs := s.Graph.Requests(fakegraph.RequestFilter{})
	s.checkPostAfterExit(res, reqs, instances)
	s.checkDuplicates(res)
	s.checkOrder(res)
	s.checkRates(res, reqs)
	s.checkRetryAfter(res, reqs)
	s.checkLoops(res, reqs)
	s.checkCatchUp(res, instances)
	for _, b := range s.Graph.Blocked() {
		res.add(InvOffline, "request to a host the fake does not serve: %s (%s)", b.URL, b.Instance)
	}
	for _, v := range s.Graph.Violations() {
		res.add(InvGraphContract, "%s", v.String())
	}
	for _, w := range s.Env.Rollouts() {
		if err := w.Intact(); err != nil {
			res.add(InvLocalHistory, "local Codex history changed: %v", err)
		}
	}

	summary := s.reportInvariants(res)
	s.emitVerdict(summary, golden, cpu)
}

// reportInvariants applies waivers and exceptions, fails the test for the
// rest and returns the per-invariant summary.
func (s *Scenario) reportInvariants(res *invResults) map[string]string {
	out := map[string]string{}
	for _, name := range AllInvariants {
		if reason, ok := s.Opts.Invariants.Waive[name]; ok {
			out[name] = "waived: " + reason
			continue
		}
		var left, excused []string
		for _, f := range res.fail[name] {
			ok := false
			for _, e := range s.Opts.Invariants.Except {
				if e.Invariant == name && regexp.MustCompile(e.Match).MatchString(f) {
					ok = true
					excused = append(excused, f+" [expected: "+e.Reason+"]")
					break
				}
			}
			if !ok {
				left = append(left, f)
			}
		}
		switch {
		case len(left) > 0:
			const maxShown = 12
			shown := left
			if len(shown) > maxShown {
				shown = append(shown[:maxShown:maxShown], fmt.Sprintf("… and %d more", len(left)-maxShown))
			}
			s.T.Errorf("INVARIANT %s violated (%d):\n  %s", name, len(left), strings.Join(shown, "\n  "))
			out[name] = fmt.Sprintf("FAIL (%d): %s", len(left), left[0])
		case len(excused) > 0:
			out[name] = fmt.Sprintf("ok (%d expected)", len(excused))
		case res.note[name] != "":
			out[name] = res.note[name]
		default:
			out[name] = "ok"
		}
	}
	return out
}

func isGraphWrite(r fakegraph.Request) bool {
	return r.Host == "graph.microsoft.com" && r.Method != "GET" && r.Method != "HEAD"
}

func isMessagePost(r fakegraph.Request) bool {
	return r.Method == "POST" && (r.Route == "POST /chats/{chat}/messages" || strings.HasPrefix(r.Route, "POST /chats/{chat}/messages/") || strings.Contains(r.Route, "replyWithQuote"))
}

// fromHelper reports whether a request came from a helper instance (not a
// CLI command the scenario ran).
func fromHelper(r fakegraph.Request) bool {
	return strings.HasPrefix(r.Instance, "cxp") && r.Instance != "cxp-cli"
}

func (s *Scenario) checkPostAfterExit(res *invResults, reqs []fakegraph.Request, instances []InstanceRecord) {
	exited := map[string]InstanceRecord{}
	for _, in := range instances {
		if !in.Exited.IsZero() {
			exited[in.Tag] = in
		}
	}
	for _, r := range reqs {
		in, ok := exited[r.Instance]
		if ok && r.At.After(in.Exited) {
			res.add(InvNoPostAfterExit, "%s %s by %s %v after it exited (%s)", r.Method, r.Route, r.Instance, r.At.Sub(in.Exited).Round(time.Millisecond), in.How)
		}
	}
}

// TokenRE matches the scenario tokens (Scenario.Token) and the fake
// Codex's default answers (E2EANSWER-<n>).
var TokenRE = regexp.MustCompile(`E2E[A-Z0-9]*-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*`)

// outboxMarkerRE matches cxp's per-attempt delivery marker; a re-post of
// the same message carries a new one, so duplicates compare without it.
var outboxMarkerRE = regexp.MustCompile(`<!--\s*codex-helper-outbox:[^>]*-->`)

// checkDuplicates finds re-posted helper messages, comparing HTML without
// the per-attempt delivery marker:
//   - a Codex answer (the fake's answers are unique: E2EANSWER-<n>)
//     appears at most once per chat;
//   - a helper instance never posts what an earlier instance already
//     posted to the chat unless something new happened in between: an
//     owner message in that chat, a Codex turn started, or a local
//     rollout record written (a restart re-sending something that was
//     delivered).
func (s *Scenario) checkDuplicates(res *invResults) {
	var events []time.Time // account-wide causes of new helper output
	if s.Codex != nil {
		for _, c := range s.Codex.Calls() {
			events = append(events, c.Started)
		}
	}
	for _, w := range s.Env.Rollouts() {
		for _, e := range w.Written() {
			events = append(events, e.At)
		}
	}
	between := func(a, b time.Time) bool {
		for _, t := range events {
			if t.After(a) && !t.After(b) {
				return true
			}
		}
		return false
	}
	for _, c := range s.Graph.Chats() {
		answers := map[string]string{}
		type post struct {
			id, instance string
			at           time.Time
			owner        int // owner messages in the chat before it
		}
		last := map[string]post{}
		owner := 0
		for _, m := range s.Graph.Messages(c.ID) {
			if m.Origin == fakegraph.OriginUser {
				owner++
				continue
			}
			if m.Origin != fakegraph.OriginSUT {
				continue
			}
			body := outboxMarkerRE.ReplaceAllString(m.Content, "")
			if answerSeqRE.MatchString(body) {
				if first, ok := answers[body]; ok {
					res.add(InvNoDuplicatePost, "chat %q: message %s repeats answer %s: %.160s", c.Topic, m.ID, first, NormalizeSpace(m.PlainText()))
				}
				answers[body] = m.ID
			}
			if p, ok := last[body]; ok && p.owner == owner && p.instance != "" && m.Instance != "" && p.instance != m.Instance && !between(p.at, m.Created) {
				res.add(InvNoDuplicatePost, "chat %q: %s re-posts message %s of %s with nothing new in between (no owner message here, no Codex turn, no local record): %.160s",
					c.Topic, m.Instance, p.id, p.instance, NormalizeSpace(m.PlainText()))
			}
			last[body] = post{id: m.ID, instance: m.Instance, at: m.Created, owner: owner}
		}
	}
}

var answerSeqRE = regexp.MustCompile(`E2EANSWER-(\d+)\b`)

// messageIndex maps each token to the (chat, position) of the first helper
// message carrying it.
type tokenPos struct {
	chat  string
	topic string
	pos   int
}

func (s *Scenario) tokenPositions() map[string]tokenPos {
	out := map[string]tokenPos{}
	for _, c := range s.Graph.Chats() {
		for i, m := range s.Graph.Messages(c.ID) {
			if m.Origin != fakegraph.OriginSUT {
				continue
			}
			for _, tok := range TokenRE.FindAllString(m.Content, -1) {
				if _, ok := out[tok]; !ok {
					out[tok] = tokenPos{c.ID, c.Topic, i}
				}
			}
		}
	}
	return out
}

func (s *Scenario) checkOrder(res *invResults) {
	pos := s.tokenPositions()
	// Codex answers of one thread, in turn order.
	byThread := map[string][]CodexCall{}
	for _, c := range s.Codex.Calls() {
		if c.ThreadID != "" {
			byThread[c.ThreadID] = append(byThread[c.ThreadID], c)
		}
	}
	for thread, calls := range byThread {
		sort.Slice(calls, func(a, b int) bool { return calls[a].Seq < calls[b].Seq })
		last := map[string]tokenPos{}
		lastTok := map[string]string{}
		for _, c := range calls {
			m := answerSeqRE.FindString(c.Answer)
			if m == "" {
				continue
			}
			p, ok := pos[m]
			if !ok {
				continue
			}
			if prev, ok := last[p.chat]; ok && p.pos < prev.pos {
				res.add(InvChatOrder, "chat %q: answer %s (turn %d of thread %s) is above %s, an earlier turn's answer", p.topic, m, c.Seq, short(thread), lastTok[p.chat])
			}
			last[p.chat], lastTok[p.chat] = p, m
		}
	}
	// Mirrored local records, in rollout order.
	for _, w := range s.Env.Rollouts() {
		last := map[string]tokenPos{}
		lastTok := map[string]string{}
		for _, e := range w.Written() {
			tok := TokenRE.FindString(e.Text)
			if tok == "" {
				continue
			}
			p, ok := pos[tok]
			if !ok {
				continue
			}
			if prev, ok := last[p.chat]; ok && p.pos < prev.pos {
				res.add(InvChatOrder, "chat %q: local %s %s is above %s, written before it in %s", p.topic, e.Kind, tok, lastTok[p.chat], filepath.Base(w.Path))
			}
			last[p.chat], lastTok[p.chat] = p, tok
		}
	}
}

func (s *Scenario) checkRates(res *invResults, reqs []fakegraph.Request) {
	var writes, posts []time.Time
	var writeReqs []fakegraph.Request
	perChat := map[string][]time.Time{}
	reads := map[string][]time.Time{}
	for _, r := range reqs {
		if !fromHelper(r) {
			continue
		}
		if isGraphWrite(r) {
			writes = append(writes, r.At)
			writeReqs = append(writeReqs, r)
		}
		if isMessagePost(r) {
			posts = append(posts, r.At)
			if r.ChatID != "" {
				perChat[r.ChatID] = append(perChat[r.ChatID], r.At)
			}
		}
		if r.Method == "GET" && r.Route == "GET /chats/{chat}/messages" {
			reads[r.ChatID] = append(reads[r.ChatID], r.At)
		}
	}
	if at, n := maxInWindow(posts, time.Second); n > CapPostsPerSecond {
		res.add(InvRateCap, "%d message posts within 1s at %s (cap %d)", n, s.rel(at), CapPostsPerSecond)
	}
	if at, n := maxInWindow(writes, time.Second); n > CapWritesPerSecond {
		res.add(InvRateCap, "%d Graph writes within 1s at %s (cap %d): %s", n, s.rel(at), CapWritesPerSecond, routeCounts(writeReqs, at, time.Second))
	}
	if at, n := maxInWindow(writes, time.Minute); n > CapWritesPerMinute {
		res.add(InvRateCap, "%d Graph writes within 60s from %s (cap %d)", n, s.rel(at), CapWritesPerMinute)
	}
	for chat, ts := range perChat {
		if at, n := maxInWindow(ts, time.Second); n > CapChatPostsPerSecond {
			res.add(InvRateCap, "%d message posts to chat %s within 1s at %s (cap %d)", n, s.topic(chat), s.rel(at), CapChatPostsPerSecond)
		}
	}
	for chat, ts := range reads {
		if at, n := maxInWindow(ts, time.Second); n > CapChatReadsPerSecond {
			res.add(InvNoLoop, "%d reads of chat %s within 1s at %s (cap %d)", n, s.topic(chat), s.rel(at), CapChatReadsPerSecond)
		}
	}
}

func (s *Scenario) rel(t time.Time) string {
	return "+" + t.Sub(s.started).Round(time.Millisecond).String()
}

func (s *Scenario) topic(chatID string) string {
	for _, c := range s.Graph.Chats() {
		if c.ID == chatID {
			return strconv.Quote(c.Topic)
		}
	}
	return short(chatID)
}

// maxInWindow returns the start and size of the fullest window of length w
// (times must be in request order, i.e. sorted).
func maxInWindow(ts []time.Time, w time.Duration) (time.Time, int) {
	best, at := 0, time.Time{}
	j := 0
	for i := range ts {
		for ts[i].Sub(ts[j]) >= w {
			j++
		}
		if n := i - j + 1; n > best {
			best, at = n, ts[j]
		}
	}
	return at, best
}

func (s *Scenario) checkRetryAfter(res *invResults, reqs []fakegraph.Request) {
	for i, r := range reqs {
		if r.RetryAfter <= 0 || !fromHelper(r) {
			continue
		}
		until := r.At.Add(r.Duration).Add(r.RetryAfter).Add(-retryAfterTolerance)
		for _, n := range reqs[i+1:] {
			if n.Instance != r.Instance || n.Route != r.Route || n.ChatID != r.ChatID {
				continue
			}
			if n.At.Before(until) {
				res.add(InvRetryAfter, "%s %s requested %v after a Retry-After of %v", n.Route, s.topic(n.ChatID), n.At.Sub(r.At.Add(r.Duration)).Round(time.Millisecond), r.RetryAfter)
			}
			break
		}
	}
}

func (s *Scenario) checkLoops(res *invResults, reqs []fakegraph.Request) {
	same := map[string]int{}
	failing := map[string]int{}
	for _, r := range reqs {
		if !fromHelper(r) {
			continue
		}
		if isGraphWrite(r) {
			k := r.Method + " " + r.RawPath + " " + r.Body
			same[k]++
			if same[k] == CapIdenticalWrites+1 {
				res.add(InvNoLoop, "the same write was sent %d+ times: %s %s %.120s", CapIdenticalWrites+1, r.Method, r.Route, r.Body)
			}
		}
		if r.Status == 0 || r.Status >= 400 {
			k := r.Method + " " + r.RawPath
			failing[k]++
			if failing[k] == CapFailingRepeats+1 {
				res.add(InvNoLoop, "the same request failed %d+ times: %s %s (last status %d)", CapFailingRepeats+1, r.Method, r.Route, r.Status)
			}
		}
	}
}

// checkCatchUp: records written while no helper ran are caught up
// silently.
func (s *Scenario) checkCatchUp(res *invResults, instances []InstanceRecord) {
	running := func(t time.Time) bool {
		for _, in := range instances {
			if !t.Before(in.Started) && (in.Exited.IsZero() || t.Before(in.Exited)) {
				return true
			}
		}
		return false
	}
	pos := s.tokenPositions()
	catchUpChats := map[string]bool{}
	liveChats := map[string]bool{}
	for _, w := range s.Env.Rollouts() {
		for _, e := range w.Written() {
			tok := TokenRE.FindString(e.Text)
			if tok == "" {
				continue
			}
			p, ok := pos[tok]
			if !ok {
				continue
			}
			if running(e.At) {
				liveChats[p.chat] = true
				continue
			}
			catchUpChats[p.chat] = true
			for _, m := range s.Graph.Messages(p.chat) {
				if m.Origin == fakegraph.OriginSUT && strings.Contains(m.Content, tok) && (len(m.Mentions) > 0 || strings.Contains(m.Content, "<at ")) {
					res.add(InvCatchUpSilent, "chat %q: caught-up %s %s was posted with an @mention", p.topic, e.Kind, tok)
				}
			}
			for _, c := range s.Graph.WorkflowCards() {
				if strings.Contains(c.Raw, tok) {
					res.add(InvCatchUpSilent, "a Workflow card carries the caught-up %s %s", e.Kind, tok)
				}
			}
		}
	}
	// A chat that only caught up (no record written while a helper ran,
	// no prompt sent from Teams) must not be named in any owner
	// notification: no @mention message links to it, no card points at it.
	for chat := range catchUpChats {
		if liveChats[chat] {
			continue
		}
		teams := false
		for _, m := range s.Graph.Messages(chat) {
			if m.Origin == fakegraph.OriginUser {
				teams = true
			}
		}
		if teams {
			continue
		}
		refs := []string{chat, url.QueryEscape(chat)}
		names := func(text string) bool {
			for _, r := range refs {
				if strings.Contains(text, r) {
					return true
				}
			}
			return false
		}
		for _, c := range s.Graph.WorkflowCards() {
			if names(c.Raw) {
				res.add(InvCatchUpSilent, "a Workflow card points at chat %s, which only caught up", s.topic(chat))
			}
		}
		for _, c := range s.Graph.Chats() {
			for _, m := range s.Graph.Messages(c.ID) {
				if m.Origin == fakegraph.OriginSUT && (len(m.Mentions) > 0 || strings.Contains(m.Content, "<at ")) && names(m.Content) {
					res.add(InvCatchUpSilent, "an @mention in chat %s names chat %s, which only caught up: %s", s.topic(c.ID), s.topic(chat), clipBody(m.PlainText()))
				}
			}
		}
	}
}

// scenarioProcesses lists the processes whose HOME is the scenario home
// (cxp, its app-servers and shells), except this test process.
func (s *Scenario) scenarioProcesses() map[int]string {
	out := map[int]string{}
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return out
	}
	want := "HOME=" + s.Env.Home + "\x00"
	self := os.Getpid()
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		env, err := os.ReadFile(filepath.Join("/proc", e.Name(), "environ"))
		if err != nil || !strings.Contains("\x00"+string(env), "\x00"+want) {
			continue
		}
		stat, _ := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if i := strings.LastIndexByte(string(stat), ')'); i >= 0 {
			if f := strings.Fields(string(stat)[i+1:]); len(f) > 0 && f[0] == "Z" {
				continue // a zombie is gone, only not reaped yet
			}
		}
		cmd, _ := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		out[pid] = strings.TrimSpace(strings.ReplaceAll(string(cmd), "\x00", " "))
	}
	return out
}

func (s *Scenario) waitScenarioProcesses(d time.Duration) []string {
	deadline := time.Now().Add(d)
	for {
		procs := s.scenarioProcesses()
		if len(procs) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			var out []string
			for pid, cmd := range procs {
				n := s.newNormalizer()
				out = append(out, fmt.Sprintf("pid %d: %s", pid, n.norm(cmd)))
			}
			sort.Strings(out)
			return out
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *Scenario) killScenarioProcesses() {
	for pid := range s.scenarioProcesses() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// RunAndCheckLeaks runs the tests of a scenario package and then fails the
// package if any process started from the built binaries is still alive.
func RunAndCheckLeaks(m interface{ Run() int }) int {
	code := m.Run()
	if CLIBinary == "" {
		return code
	}
	dir := filepath.Dir(CLIBinary)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var left []string
		ents, _ := os.ReadDir("/proc")
		for _, e := range ents {
			if _, err := strconv.Atoi(e.Name()); err != nil {
				continue
			}
			cmd, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
			if err == nil && strings.HasPrefix(string(cmd), dir+"/") {
				left = append(left, e.Name()+": "+strings.ReplaceAll(string(cmd), "\x00", " "))
			}
		}
		if len(left) == 0 {
			return code
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "INVARIANT %s violated: processes left after the package's scenarios:\n  %s\n", InvNoLeakedProcess, strings.Join(left, "\n  "))
			if code == 0 {
				code = 1
			}
			return code
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// clipBody shortens a request body for a failure message.
func clipBody(b string) string {
	b = strings.Join(strings.Fields(b), " ")
	if len(b) > 240 {
		return b[:240] + "…"
	}
	return b
}

// routeCounts summarizes the routes of the requests in [at, at+w).
func routeCounts(reqs []fakegraph.Request, at time.Time, w time.Duration) string {
	n := map[string]int{}
	for _, r := range reqs {
		if !r.At.Before(at) && r.At.Before(at.Add(w)) {
			n[r.Route]++
		}
	}
	var out []string
	for k, v := range n {
		out = append(out, fmt.Sprintf("%d× %s", v, k))
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}
