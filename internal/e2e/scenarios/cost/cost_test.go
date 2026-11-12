package cost

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Production timing: the helper runs with its production cadences (the
// seam's timing=production); args are extra `cxp teams run` flags.
func prodOptions(title string, guarantees []string, args ...string) harness.Options {
	return harness.Options{
		Guarantees: guarantees,
		Title:      title,
		Budget:     120 * time.Second,
		Interval:   5 * time.Second,
		Seam:       harness.Seam{ProductionTiming: true},
		Args:       args,
	}
}

const window = 30 * time.Second

// Delegation inbox read pressure (CLEANUP-PLAN §8.2: the inbox is covered by
// the shared activity scan, no dedicated 5 s poll; F-XM-2 heartbeat every
// 5 min). With the machine registry on and nothing to do, the helper must
// not keep reading its own inbox chat.
func TestDelegationInboxReadPressure(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "INBOX", prodOptions(
		"idle helper with machine registry: inbox reads <= 1/min, no idle writes",
		[]string{"F-XM-2", "R-14", "R-12"}, "--machine-registry"))
	defer s.Finish()
	steps.ReadyControlChat(s)
	var inbox fakegraph.ChatInfo
	s.Require("machine inbox chat created", 30*time.Second, func() (bool, string) {
		var ok bool
		inbox, ok = s.FindChat("CXP Machine Inbox")
		return ok, fmt.Sprintf("chats: %d", len(s.Graph.Chats()))
	})
	s.Settle(3*time.Second, 10*time.Second)
	m := measure(s, window)

	inboxReads := m.readsOf(inbox.ID)
	s.Check("delegation inbox is not polled on its own", inboxReads <= 1, fmt.Sprintf("<= 1 read in %.0fs", m.dur.Seconds()),
		fmt.Sprintf("%d reads (%.1f/min): %v", inboxReads, m.rate(inboxReads), firstN(m.routesOf(inbox.ID), 4)))
	s.Check("R-22 an idle helper writes nothing to Teams", m.writes == 0, "0 POST/PATCH", fmt.Sprintf("%d: %v", m.writes, firstN(m.writeRoutes, 4)))
	s.Note("idle %s: %s", m.dur.Round(time.Second), m.summary())
}

// Idle cost (R-14): recently created chats use the 2s tier even without a
// request. Each chat's reads stay within that tier, the helper writes nothing,
// and it stays under 1% of a core. Older tier boundaries are covered by the
// engine interval tests.
func TestIdleCost(t *testing.T) {
	harness.Parallel(t)
	const n = 6
	s := harness.NewScenario(t, "IDLE", prodOptions(
		fmt.Sprintf("%d recently created work chats: bounded reads, no writes, CPU <= 1%% of a core", n),
		[]string{"R-14", "R-22"}))
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	before := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		before[c.ID] = true
	}
	burstAt := time.Now()
	for i := 0; i < n; i++ {
		steps.MustPost(s, control.ID, "new "+s.Env.WorkDir(t, fmt.Sprintf("idle-%d", i)))
	}
	var work []string
	s.Require(fmt.Sprintf("%d work chats ready", n), 60*time.Second, func() (bool, string) {
		work = work[:0]
		for _, c := range s.Graph.Chats() {
			if !before[c.ID] && !strings.Contains(c.Topic, harness.ControlChatTopicMarker) && len(s.SUTMessagesContaining(c.ID, "Work chat is ready")) > 0 {
				work = append(work, c.ID)
			}
		}
		return len(work) == n, fmt.Sprintf("%d/%d", len(work), n)
	})
	s.Require("every `new` answered in the control chat", 60*time.Second, func() (bool, string) {
		got := len(s.SUTMessagesContaining(control.ID, "Work chat created"))
		return got == n, fmt.Sprintf("%d/%d", got, n)
	})
	s.Note("%d `new` commands sent at once were all answered after %.1fs", n, time.Since(burstAt).Seconds())
	s.Settle(7*time.Second, 25*time.Second) // quiet for more than one 5 s interval
	m := measure(s, window)

	maxReads := int(m.dur/(2*time.Second)) + 2 // allow a read at each boundary
	for _, id := range work {
		reads := m.readsOf(id)
		s.Check("recent work chat reads stay within the 2s tier", reads >= 1 && reads <= maxReads,
			fmt.Sprintf("1..%d reads", maxReads), fmt.Sprintf("%d reads in %.0fs", reads, m.dur.Seconds()))
	}
	s.Check("R-22 an idle helper writes nothing to Teams", m.writes == 0, "0 POST/PATCH", fmt.Sprintf("%d: %v", m.writes, firstN(m.writeRoutes, 4)))
	if m.cpu >= 0 {
		pct := 100 * m.cpu.Seconds() / m.dur.Seconds()
		s.Check("R-14 idle CPU <= 1% of a core", pct <= 1, "<= 1.0%", fmt.Sprintf("%.2f%% (%v CPU in %v)", pct, m.cpu, m.dur.Round(time.Second)))
	}
	s.Note("idle %s with %d chats: %s", m.dur.Round(time.Second), n, m.summary())
}

// measurement is what the SUT did during an idle window.
type measurement struct {
	dur         time.Duration
	reqs        []fakegraph.Request
	writes      int
	writeRoutes []string
	cpu         time.Duration // -1 when the SUT cannot report CPU
}

func measure(s *harness.Scenario, d time.Duration) measurement {
	cpuOf := func() time.Duration {
		if c, ok := s.SUT.(interface{ CPUTime() time.Duration }); ok {
			return c.CPUTime()
		}
		return -1
	}
	seq, cpu0, t0 := s.Graph.LastSeq(), cpuOf(), time.Now()
	s.Poll(d, func() bool { return false })
	m := measurement{dur: time.Since(t0), cpu: -1}
	if cpu0 >= 0 {
		m.cpu = cpuOf() - cpu0
	}
	m.reqs = s.Graph.Requests(fakegraph.RequestFilter{Since: seq})
	for _, r := range m.reqs {
		if r.Method != "GET" {
			m.writes++
			m.writeRoutes = append(m.writeRoutes, r.Route+" "+r.ChatID+" "+clip(r.Body, 80))
		}
	}
	return m
}

func (m measurement) readsOf(chatID string) int { return len(m.routesOf(chatID)) }

func (m measurement) routesOf(chatID string) []string {
	var out []string
	for _, r := range m.reqs {
		if r.Method == "GET" && r.ChatID == chatID {
			out = append(out, r.At.Format("15:04:05.0")+" "+r.Route)
		}
	}
	return out
}

func (m measurement) rate(n int) float64 { return float64(n) * 60 / m.dur.Seconds() }

// summary is "<count> <route>" per route, busiest first.
func (m measurement) summary() string {
	count := map[string]int{}
	for _, r := range m.reqs {
		count[r.Route]++
	}
	var keys []string
	for k := range count {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return count[keys[i]] > count[keys[j]] })
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%d %s", count[k], k))
	}
	cpu := "n/a"
	if m.cpu >= 0 {
		cpu = m.cpu.String()
	}
	return fmt.Sprintf("%d requests (%.0f/min), CPU %s: %s", len(m.reqs), m.rate(len(m.reqs)), cpu, strings.Join(parts, ", "))
}

func firstN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
