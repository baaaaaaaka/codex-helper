package sharedhome

import (
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Lease takeover between hosts that share one home (internal/core/lease,
// TAKEOVER-REPORT.md). Most scenarios shorten the lease's stale bound with
// the seam's Takeover knob (every other lease interval scales with it):
// takeoverStale is that bound, takeoverQuiet the wait a new owner from
// another host leaves the old one before it copies the store, and
// takeoverPoll a standby's check interval.
const (
	takeoverStale = 30 * time.Second
	takeoverQuiet = takeoverStale / 3
	takeoverPoll  = takeoverStale * 2 / 9
)

// takeoverSeam is the seam of a host in a takeover scenario.
func takeoverSeam(host string) harness.Seam {
	return harness.Seam{Hostname: host, Knobs: map[string]string{"Heartbeat": "500ms", "Takeover": takeoverStale.String()}}
}

// serviceChild is the flag a service-run helper gets.
var serviceChild = []string{"--managed-service-child"}

// serving reports whether sut's output says it opened the store and runs.
func serving(sut harness.SUT) bool {
	return strings.Contains(sut.Output(), "Teams helper (new core) running")
}

// waitServing waits up to timeout for sut to serve and returns how long
// that took.
func waitServing(s *harness.Scenario, sut harness.SUT, timeout time.Duration) (time.Duration, bool) {
	t0 := time.Now()
	ok := s.Poll(timeout, func() bool { return serving(sut) })
	return time.Since(t0).Round(time.Second), ok
}

// takeoverNotices are the helper's control-chat posts announcing that
// another host took over.
func takeoverNotices(s *harness.Scenario, control string) []string {
	var out []string
	for _, m := range s.SUTMessagesContaining(control, "The helper now runs on") {
		out = append(out, harness.NormalizeSpace(m.Content))
	}
	return out
}

// writesBy lists the non-GET requests since seq made by the instance tag.
func writesBy(s *harness.Scenario, since int64, instance string) []string {
	var out []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: since}) {
		if r.Instance == instance && r.Method != "GET" {
			out = append(out, r.Route)
		}
	}
	return out
}

// answeredBy waits for token's answer in chat and reports whether it was
// posted by an instance whose tag contains sub. It polls rather than using
// steps.WaitAnswered: the scenario's own SUT may have exited.
func answeredBy(s *harness.Scenario, chatID, token, sub string, timeout time.Duration) bool {
	return s.Poll(timeout, func() bool {
		for _, p := range steps.AnswerPosts(s, chatID, token) {
			if strings.Contains(p.Instance, sub) {
				return true
			}
		}
		return false
	})
}

// exactlyOnce checks that token was run once and answered once.
func exactlyOnce(s *harness.Scenario, chatID, token string) {
	s.Settle(2*time.Second, 8*time.Second)
	n := len(steps.AnswerPosts(s, chatID, token))
	turns := len(s.Codex.CallsWithToken(token))
	s.Check("R-1 the prompt is answered exactly once across the hosts", n == 1 && turns == 1,
		"1 answer, 1 turn", fmt.Sprintf("posts=%d turns=%d", n, turns), s.Transcript(chatID, 12)...)
}

// standbyCPU measures sut's CPU time over d while it stands by.
func standbyCPU(sut harness.SUT, d time.Duration) (used time.Duration, measured bool) {
	cpu, ok := sut.(interface{ CPUTime() time.Duration })
	if !ok {
		time.Sleep(d)
		return 0, false
	}
	c0 := cpu.CPUTime()
	time.Sleep(d)
	return cpu.CPUTime() - c0, true
}

// checkFencedAndSilent thaws a frozen old owner and checks that it exits
// with the fence error and makes no Graph write.
func checkFencedAndSilent(s *harness.Scenario, old harness.SUT) {
	from := s.Graph.LastSeq()
	harness.Thaw(old)
	exited := s.Poll(30*time.Second, func() bool { return !old.Running() })
	s.Settle(2*time.Second, 6*time.Second)
	writes := writesBy(s, from, old.Instance())
	out := old.Output()
	s.Check("the resumed old owner is fenced and exits", exited && strings.Contains(strings.ToLower(out), "fence"),
		"exits with the fence error", fmt.Sprintf("exited=%v; output %s", exited, clipTail(out, 300)))
	s.Check("the fenced old owner makes no Graph write", len(writes) == 0, "0 writes after it resumed",
		fmt.Sprintf("%d: %q", len(writes), writes))
}
