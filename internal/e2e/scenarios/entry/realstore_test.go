package entry

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// Environment of the real-store acceptance run (never set in CI):
// E2E_REAL_STORE is a read-only directory holding a copy of an old helper's
// scope (store.sqlite, state.json, registry.json, ...); the scenario copies
// it into its sandbox. E2E_REAL_STORE_MEASURE is the measured run time
// after the conversion (default 10m). E2E_REAL_STORE_LIVE=0 leaves out the
// live session (the idle window is then truly idle).
const (
	envRealStore        = "E2E_REAL_STORE"
	envRealStoreMeasure = "E2E_REAL_STORE_MEASURE"
	envRealStoreLive    = "E2E_REAL_STORE_LIVE"
)

// ACCEPT-REALSTORE: the first start of `cxp teams run` on a copy of the
// owner's real rc.59 store (the one whose service could not start: control
// lease hot loop, ownership never resolved). The conversion completes, the
// helper owns the new store at once (no old lease or generation), polls the
// control chat within 5 s, posts the summary once and at most one notice
// per interrupted request, starts the catch-up of a synthetic
// sessions tree, never repeats a log line more than a few times a minute,
// and its CPU is recorded until the catch-up drains and then over the
// measured idle window, which must stay within the idle budget.
func TestAcceptanceRealStoreFirstStart(t *testing.T) {
	src := os.Getenv(envRealStore)
	if src == "" {
		t.Skip(envRealStore + " is not set (real-store acceptance run only)")
	}
	measure := 10 * time.Minute
	if v := os.Getenv(envRealStoreMeasure); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		measure = d
	}
	s := harness.NewScenario(t, "ACCEPT-REALSTORE", harness.Options{
		Guarantees: []string{"auto-migrate", "cpu-budget", "no-hot-loop"},
		Title:      "first start on the owner's real rc.59 store: converted, owned at once, summary once, catch-up drains, CPU recorded",
		Budget:     measure + 55*time.Minute,
		NoStart:    true,
		Seam:       harness.Seam{ProductionTiming: true},
		Args:       []string{"--machine-registry"},
		Golden:     harness.GoldenOptions{Skip: "opt-in run on the owner's private store copy: its transcript is not committed"},
		Invariants: harness.Invariants{CPUBudget: 2 * time.Hour},
	})
	defer s.Finish()

	scope := filepath.Join(teamsDir(s), "scopes", filepath.Base(filepath.Clean(src)))
	copyStart := time.Now()
	if err := copyTree(src, scope); err != nil {
		t.Fatal(err)
	}
	s.Note("copied %s in %s", src, time.Since(copyStart).Round(time.Second))
	control, chats, err := realStoreChats(filepath.Join(scope, "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range chats {
		s.Graph.SeedChat(id, "")
	}
	s.Note("seeded %d chats; control chat present: %v", len(chats), control != "")

	// Synthetic sessions: interactive rollouts over the last 150 days (all
	// caught up: no window) and one live session appending
	// large tool outputs during the whole run.
	tokens := seedSessions(t, s)
	live := harness.NewRollout(t, s.Env, s.Env.WorkRoot)
	stopLive := make(chan struct{})
	liveDone := make(chan int)
	if os.Getenv(envRealStoreLive) == "0" {
		go func() { <-stopLive; liveDone <- 0 }()
	} else {
		go func() { liveDone <- appendLive(live, stopLive) }()
	}
	defer func() {
		select {
		case <-stopLive:
		default:
			close(stopLive)
		}
	}()

	s.StartSUT()
	var converted time.Time
	s.WaitFor("the conversion completes", 20*time.Minute, func() (bool, string) {
		out := s.SUT.Output()
		if strings.Contains(out, "Teams store converted in") {
			converted = time.Now()
			return true, ""
		}
		// A helper that exited stops the wait (checked below).
		return !s.SUT.Running(), s.OutputTail(400)
	})
	if converted.IsZero() {
		s.Check("the conversion completes", false, "Teams store converted", s.OutputTail(1200))
		return
	}
	s.Note("conversion: %s", grepLine(s.SUT.Output(), "Teams store converted in"))
	pid := 0
	s.WaitFor("the helper runs", 60*time.Second, func() (bool, string) {
		if p, ok := s.SUT.(interface{ PID() int }); ok {
			pid = p.PID()
		}
		return pid != 0 && strings.Contains(s.SUT.Output(), runningLine), s.OutputTail(300)
	})

	// The owner's Workflow webhook, enabled the way the owner does it
	// (`cxp teams workflow enable`) once the converted store is in use; a
	// stopped helper's enable would write a fresh store before the
	// conversion.
	s.WorkflowWebhookURL = s.Graph.NewWorkflowWebhook()
	s.EnableWorkflow(s.WorkflowWebhookURL)

	// Owned at once by this process (`cxp teams status`: "Owner: <host> pid
	// <pid>, heartbeat …"). The owner generation is not shown by any
	// command, so "from generation 1" is not observable from outside.
	status, _ := s.RunCLI(60*time.Second, "teams", "status")
	ownerPID := 0
	if m := regexp.MustCompile(`Owner: \S+ pid (\d+), heartbeat`).FindStringSubmatch(status); m != nil {
		ownerPID, _ = strconv.Atoi(m[1])
	}
	s.Check("the helper owns the new store at once", ownerPID == pid, fmt.Sprintf("owner pid %d", pid), harness.NormalizeSpace(status))
	// First control-chat poll within 5 s of the conversion.
	s.WaitFor("the first control-chat poll", 30*time.Second, func() (bool, string) {
		return len(s.Graph.Requests(fakegraph.RequestFilter{Method: "GET", ChatID: control})) > 0, strings.Join(s.RequestTail(5), "\n")
	})
	if reqs := s.Graph.Requests(fakegraph.RequestFilter{Method: "GET", ChatID: control}); len(reqs) > 0 {
		lag := reqs[0].At.Sub(converted)
		s.Check("the control chat is polled within 5 s after the conversion", lag <= 5*time.Second, "<= 5s", lag.Round(10*time.Millisecond).String())
	}

	// CPU per second, in two phases: the start (re-hash, local sessions
	// mirrored, the catch-up posted at the channel's pace) until
	// the helper's writes fall to the live session's rate, then the
	// measured idle window, while the live session keeps appending large
	// tool outputs. The idle budget applies to the second phase.
	drainStart := time.Now()
	active := sampleCPU(pid, 30*time.Minute, func() bool { return catchUpDrained(s) })
	drained := time.Since(drainStart)
	asum := summarizeCPU(active)
	s.Note("CPU from the start until the catch-up drained (%s, %d samples): mean %.3f%%, p99 %.1f%%, max %.1f%% of a core per second (second %d); total %.2fs CPU",
		drained.Round(time.Second), len(active.perSec), asum.mean, asum.p99, asum.max, asum.maxAt, asum.totalSec)
	s.Check("the catch-up drains within 30 min", drained < 30*time.Minute, "< 30m", drained.Round(time.Second).String())

	samples := sampleCPU(pid, measure, nil)
	close(stopLive)
	appended := <-liveDone
	sum := summarizeCPU(samples)
	s.Note("CPU over %s idle after the catch-up (%d samples, live session appended %d large records): mean %.3f%%, p99 %.1f%%, max %.1f%% of a core per second; total %.2fs CPU",
		measure, len(samples.perSec), appended, sum.mean, sum.p99, sum.max, sum.totalSec)
	s.Check("idle CPU mean is negligible", sum.mean <= 0.2 || len(samples.perSec) == 0, "<= 0.2% of a core", fmt.Sprintf("%.3f%%", sum.mean))
	s.Check("no idle 1 s window above 10% of a core", sum.max <= 10, "<= 10%", fmt.Sprintf("max %.1f%% at second %d", sum.max, sum.maxAt))

	out := s.SUT.Output()
	summaryN := 0
	for _, m := range s.Graph.Messages(control) {
		if m.Origin == fakegraph.OriginSUT && strings.Contains(m.PlainText(), "Teams helper data converted") {
			summaryN++
			s.Note("summary: %s", m.PlainText())
		}
	}
	s.Check("exactly one conversion summary in the control chat", summaryN == 1, "1", fmt.Sprint(summaryN), s.Transcript(control, 8)...)

	interrupted := map[string]int{}
	var catchUp, total int
	for _, id := range append(chats, newChats(s, chats)...) {
		for _, m := range s.Graph.Messages(id) {
			if m.Origin != fakegraph.OriginSUT {
				continue
			}
			total++
			text := m.PlainText()
			if strings.Contains(text, "was interrupted when the helper stopped") {
				interrupted[id]++
			}
			for _, tok := range tokens {
				if strings.Contains(text, tok) {
					catchUp++
					break
				}
			}
		}
	}
	worst := 0
	for _, n := range interrupted {
		worst = max(worst, n)
	}
	s.Check("at most one interrupted-request notice per chat", worst <= 1, "<= 1", fmt.Sprintf("%v", interrupted))
	s.Note("helper messages posted: %d (catch-up of synthetic sessions: %d)", total, catchUp)
	s.Check("the catch-up drains", catchUp > 0, "> 0 caught-up prompts/answers", fmt.Sprint(catchUp))

	rep, line := worstRepeat(out)
	s.Check("no log line repeats more than 10 times a minute", rep <= 10, "<= 10/min", fmt.Sprintf("%d× %q", rep, line))
	s.Check("the conversion ran once", strings.Count(out, "Converting the Teams store") == 1, "1", fmt.Sprint(strings.Count(out, "Converting the Teams store")))
	restarts := 0
	if r, ok := s.SUT.(interface{ ServiceRestarts() int }); ok {
		restarts = r.ServiceRestarts()
	}
	s.Check("the helper never restarted", restarts == 0 && s.SUT.Running(), "0 restarts, running", fmt.Sprintf("%d restarts, running %v", restarts, s.SUT.Running()), s.OutputTail(600))
}

// copyTree copies the old scope (files and thread-links/), skipping the old
// helper's .before-* backups.
func copyTree(src, dst string) error {
	return filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.Contains(info.Name(), ".before-") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel, _ := filepath.Rel(src, p)
		to := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(to, 0o700)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
		return os.Chtimes(to, info.ModTime(), info.ModTime())
	})
}

// realStoreChats reads the chat ids an rc.59 store knows (sessions, chat
// polls, the control chat) from the sandbox copy, read-only.
func realStoreChats(db string) (control string, chats []string, err error) {
	h, err := sql.Open("sqlite", "file:"+db+"?mode=ro&immutable=1")
	if err != nil {
		return "", nil, err
	}
	defer h.Close()
	seen := map[string]bool{}
	add := func(q string) error {
		rows, err := h.Query(q)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id sql.NullString
			if err := rows.Scan(&id); err != nil {
				return err
			}
			if id.Valid && chatIDRE.MatchString(id.String) && !seen[id.String] {
				seen[id.String] = true
				chats = append(chats, id.String)
			}
		}
		return rows.Err()
	}
	if err := h.QueryRow(`SELECT json_extract(value, '$.control_chat.teams_chat_id') FROM state_meta WHERE key = 'state_json'`).Scan(&control); err != nil {
		return "", nil, fmt.Errorf("control chat: %w", err)
	}
	for _, q := range []string{
		`SELECT json_extract(value, '$.control_chat.teams_chat_id') FROM state_meta WHERE key = 'state_json'`,
		`SELECT DISTINCT teams_chat_id FROM sessions`,
		`SELECT DISTINCT chat_id FROM chat_polls`,
	} {
		if err := add(q); err != nil {
			return "", nil, err
		}
	}
	sort.Strings(chats)
	return control, chats, nil
}

// seedSessions writes 60 interactive rollouts over the last 150 days and
// returns a token of each prompt and answer.
func seedSessions(t *testing.T, s *harness.Scenario) []string {
	var tokens []string
	for i := 0; i < 60; i++ {
		start := time.Now().Add(-time.Duration(i*60+30) * time.Hour)
		r := harness.NewRolloutAt(t, s.Env, filepath.Join(s.Env.WorkRoot, fmt.Sprintf("proj%02d", i%7)), start)
		for k := 0; k < 3; k++ {
			p, a := fmt.Sprintf("catchup-p-%02d-%d", i, k), fmt.Sprintf("catchup-a-%02d-%d", i, k)
			r.AppendTurn(t, "Synthetic prompt "+p, "", "Synthetic answer "+a+"\n\n"+strings.Repeat("Some answer text. ", 40))
			tokens = append(tokens, p, a)
		}
		if err := r.SetModTime(start.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	return tokens
}

// appendLive plays a live TUI session: every 2 s a large tool output
// (256 KiB function_call_output and exec_command_end), every 30 s a full
// turn. It returns the number of large records appended.
func appendLive(r *harness.Rollout, stop <-chan struct{}) int {
	big := strings.Repeat("0123456789abcdef", 16<<10)
	n, tick := 0, time.NewTicker(2*time.Second)
	defer tick.Stop()
	f, err := os.OpenFile(r.Path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return 0
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for {
		select {
		case <-stop:
			return n
		case <-tick.C:
		}
		ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		call := "call_" + strconv.Itoa(n)
		fmt.Fprintf(w, `{"timestamp":%q,"type":"response_item","payload":{"type":"function_call_output","call_id":%q,"output":%q}}`+"\n", ts, call, big)
		fmt.Fprintf(w, `{"timestamp":%q,"type":"event_msg","payload":{"type":"exec_command_end","call_id":%q,"aggregated_output":%q,"exit_code":0}}`+"\n", ts, call, big)
		_ = w.Flush()
		n += 2
		if n%30 == 0 {
			_, _ = r.Writer.AppendTurn(rollout.Turn{Prompt: "live prompt " + call, Answer: "live answer " + call})
		}
	}
}

type cpuSamples struct {
	perSec   []float64 // % of one core per 1 s window (task schedstat)
	totalSec float64   // utime+stime over the window
}

// sampleCPU samples the process's on-CPU time once a second for up to d,
// or until done (checked each second) reports true.
func sampleCPU(pid int, d time.Duration, done func() bool) cpuSamples {
	var out cpuSamples
	stat0 := procStatSec(pid)
	prev := taskRunNs(pid)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	end := time.Now().Add(d)
	last := time.Now()
	for time.Now().Before(end) {
		<-tick.C
		now := time.Now()
		cur := taskRunNs(pid)
		if cur < 0 || prev < 0 {
			break
		}
		out.perSec = append(out.perSec, float64(cur-prev)/float64(now.Sub(last).Nanoseconds())*100)
		prev, last = cur, now
		if done != nil && done() {
			break
		}
	}
	out.totalSec = procStatSec(pid) - stat0
	return out
}

// catchUpDrained: no chat created for a minute and, over the last 30 s,
// no more posts than the live session's mirrored turns.
func catchUpDrained(s *harness.Scenario) bool {
	now := time.Now()
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", Route: "POST /me/onlineMeetings/createOrGet"}) {
		if now.Sub(r.At) < time.Minute {
			return false
		}
	}
	recent := 0
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", Route: "POST /chats/{chat}/messages"}) {
		if now.Sub(r.At) < 30*time.Second {
			recent++
		}
	}
	return recent <= 4
}

// taskRunNs sums the on-CPU nanoseconds of the process's threads.
func taskRunNs(pid int) int64 {
	tasks, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if err != nil {
		return -1
	}
	var sum int64
	for _, t := range tasks {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/schedstat", pid, t.Name()))
		if err != nil {
			continue
		}
		f := strings.Fields(string(raw))
		if len(f) > 0 {
			v, _ := strconv.ParseInt(f[0], 10, 64)
			sum += v
		}
	}
	return sum
}

// procStatSec is utime+stime of the process (exited threads included).
func procStatSec(pid int) float64 {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(raw)
	f := strings.Fields(s[strings.LastIndexByte(s, ')')+2:])
	if len(f) < 13 {
		return 0
	}
	u, _ := strconv.ParseFloat(f[11], 64)
	k, _ := strconv.ParseFloat(f[12], 64)
	return (u + k) / 100 // USER_HZ
}

type cpuSummary struct {
	mean, p99, max, totalSec float64
	maxAt                    int
}

func summarizeCPU(c cpuSamples) cpuSummary {
	out := cpuSummary{totalSec: c.totalSec}
	if len(c.perSec) == 0 {
		return out
	}
	sorted := append([]float64(nil), c.perSec...)
	sort.Float64s(sorted)
	var sum float64
	for i, v := range c.perSec {
		sum += v
		if v > out.max {
			out.max, out.maxAt = v, i
		}
	}
	out.mean = sum / float64(len(c.perSec))
	out.p99 = sorted[min(len(sorted)-1, len(sorted)*99/100)]
	return out
}

var (
	logLineRE = regexp.MustCompile(`^(\d\d):(\d\d):\d\d\.\d+ (.*)$`)
	digitsRE  = regexp.MustCompile(`\d+`)
)

// worstRepeat is the largest number of times one log line (digits
// normalised) appears within one wall-clock minute.
func worstRepeat(out string) (int, string) {
	counts := map[string]int{}
	worst, line := 0, ""
	for _, l := range strings.Split(out, "\n") {
		m := logLineRE.FindStringSubmatch(strings.TrimSpace(l))
		if m == nil {
			continue
		}
		key := m[1] + m[2] + "|" + digitsRE.ReplaceAllString(m[3], "#")
		counts[key]++
		if counts[key] > worst {
			worst, line = counts[key], m[3]
		}
	}
	return worst, line
}

func grepLine(out, sub string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, sub) {
			return strings.TrimSpace(l)
		}
	}
	return ""
}

// newChats are the chats the helper created during the run.
func newChats(s *harness.Scenario, seeded []string) []string {
	old := map[string]bool{}
	for _, id := range seeded {
		old[id] = true
	}
	var out []string
	for _, c := range s.Graph.Chats() {
		if !old[c.ID] {
			out = append(out, c.ID)
		}
	}
	return out
}
