package delegation

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// The requester is another machine of the same Teams account: `cxp
// delegate` run with its own HOME (its own machine identity and requester
// state) and the same sign-in. The worker is the helper under test, run
// with the machine registry on (`cxp teams run --machine-registry`).

func options(title string, guarantees []string) harness.Options {
	return harness.Options{
		Guarantees: guarantees,
		Title:      title,
		Budget:     150 * time.Second,
		Args:       []string{"--machine-registry"},
	}
}

// requester returns how to run `cxp delegate` as the requesting machine.
func requester(s *harness.Scenario) harness.CLIAs {
	home := filepath.Join(s.Env.Root, "requester")
	state, config := filepath.Join(home, ".local", "state"), filepath.Join(home, ".config")
	for _, d := range []string{home, state, config} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			s.T.Fatal(err)
		}
	}
	return harness.CLIAs{
		Hostname: "requester-host",
		Env: map[string]string{
			"HOME":                             home,
			"XDG_STATE_HOME":                   state,
			"XDG_CONFIG_HOME":                  config,
			"XDG_CACHE_HOME":                   filepath.Join(home, ".cache"),
			"XDG_DATA_HOME":                    filepath.Join(home, ".local", "share"),
			"CODEX_HELPER_STATE_DIR":           filepath.Join(state, "codex-helper"),
			"CODEX_HELPER_CONFIG":              filepath.Join(config, "codex-helper", "config.json"),
			"CODEX_HELPER_TEAMS_AUTH_CONFIG":   filepath.Join(config, "codex-helper", "teams-auth.json"),
			"CODEX_HELPER_TEAMS_MACHINE_LABEL": "requester",
		},
	}
}

// delegateJSON runs `cxp delegate <args> --json` as the requester and
// decodes its output.
func delegateJSON(s *harness.Scenario, as harness.CLIAs, out any, args ...string) bool {
	s.T.Helper()
	res := s.RunCLIAs(90*time.Second, as, append(append([]string{"delegate"}, args...), "--json")...)
	if res.Code != 0 {
		s.Check("cxp delegate "+args[0], false, "exit 0", fmt.Sprintf("exit %d: %s", res.Code, harness.NormalizeSpace(res.Combined)))
		return false
	}
	if err := json.Unmarshal([]byte(res.Stdout), out); err != nil {
		s.Check("cxp delegate "+args[0]+" output", false, "JSON", fmt.Sprintf("%v: %s", err, res.Stdout))
		return false
	}
	return true
}

type candidate struct {
	MachineID      string `json:"machine_id"`
	Label          string `json:"label"`
	State          string `json:"state"`
	Accepting      bool   `json:"accepting"`
	CandidateToken string `json:"candidate_token"`
}

type resolveResult struct {
	Action         string      `json:"action"`
	CandidateToken string      `json:"candidate_token"`
	Candidates     []candidate `json:"candidates"`
}

// resolveWorker waits until the requester sees the worker (machine label
// harness.MachineLabel) online and accepting, and returns its candidate
// token.
func resolveWorker(s *harness.Scenario, as harness.CLIAs) (candidate, bool) {
	var found candidate
	ok := s.WaitFor("the requester sees the worker online and accepting", 60*time.Second, func() (bool, string) {
		var r resolveResult
		res := s.RunCLIAs(60*time.Second, as, "delegate", "resolve", "--query", harness.MachineLabel, "--json")
		if res.Code != 0 || json.Unmarshal([]byte(res.Stdout), &r) != nil {
			return false, harness.NormalizeSpace(res.Combined)
		}
		for _, c := range r.Candidates {
			if c.Label == harness.MachineLabel || strings.Contains(c.Label, harness.MachineLabel) {
				found = c
				return c.State == "online" && c.Accepting && c.CandidateToken != "", fmt.Sprintf("%+v", c)
			}
		}
		return false, fmt.Sprintf("%d candidates", len(r.Candidates))
	})
	return found, ok
}

type state struct {
	DelegationID string  `json:"delegation_id"`
	Status       string  `json:"status"`
	Terminal     *record `json:"terminal"`
}

type startResult struct {
	DelegationID string `json:"delegation_id"`
	Status       string `json:"status"`
	MachineID    string `json:"machine_id"`
}

// start asks the worker to run objective (`cxp delegate start`).
func start(s *harness.Scenario, as harness.CLIAs, c candidate, objective string) (startResult, bool) {
	words := strings.Fields(objective)
	task := filepath.Join(s.Env.Root, "task-"+words[len(words)-1]+".json")
	spec, _ := json.Marshal(map[string]string{"title": "E2E delegated task", "objective": objective})
	if err := os.WriteFile(task, spec, 0o600); err != nil {
		s.T.Fatal(err)
	}
	var r startResult
	ok := delegateJSON(s, as, &r, "start", "--candidate-token", c.CandidateToken, "--task-file", task,
		"--source-session", "src-session-1", "--source-turn", "src-turn-1")
	return r, ok && r.DelegationID != ""
}

// record is the part of a delegation record on the wire (the inbox chat
// carries "CXP_DELEGATION_RECORD_V1 <base64url JSON>") the scenarios read.
type record struct {
	Kind         string `json:"kind"`
	DelegationID string `json:"delegation_id"`
	Status       string `json:"status"`
	Body         string `json:"body"`
}

var recordRE = regexp.MustCompile(`CXP_DELEGATION_RECORD_V1\s+([A-Za-z0-9_\-]+)`)

// inboxRecords returns the records of delegation id in the inbox chat, by
// kind, and the inbox messages that are not records.
func inboxRecords(s *harness.Scenario, id string) (map[string][]record, []string, string) {
	out := map[string][]record{}
	var other []string
	inbox := ""
	for _, c := range s.Graph.Chats() {
		for _, m := range s.Graph.Messages(c.ID) {
			sub := recordRE.FindStringSubmatch(m.PlainText())
			if sub == nil {
				continue
			}
			raw, err := base64.RawURLEncoding.DecodeString(sub[1])
			var r record
			if err != nil || json.Unmarshal(raw, &r) != nil || r.DelegationID != id {
				continue
			}
			inbox = c.ID
			out[r.Kind] = append(out[r.Kind], r)
		}
	}
	if inbox != "" {
		for _, m := range s.Graph.Messages(inbox) {
			if !recordRE.MatchString(m.PlainText()) && (m.MessageType == "" || m.MessageType == "message") {
				other = append(other, harness.NormalizeSpace(m.PlainText()))
			}
		}
	}
	return out, other, inbox
}

func describe(recs map[string][]record) string {
	var b []string
	for _, k := range []string{"request", "claim", "status", "result"} {
		var st []string
		for _, r := range recs[k] {
			st = append(st, r.Status)
		}
		b = append(b, fmt.Sprintf("%s=%d%v", k, len(recs[k]), st))
	}
	return strings.Join(b, " ")
}

// waitTerminal waits for the delegation's terminal record as the requester
// sees it (`cxp delegate status`).
func waitTerminal(s *harness.Scenario, as harness.CLIAs, id string, timeout time.Duration) state {
	var st state
	s.WaitFor("terminal result of "+id, timeout, func() (bool, string) {
		st = state{}
		if !delegateJSON(s, as, &st, "status", "--id", id) {
			return false, "status failed"
		}
		return st.Terminal != nil, st.Status
	})
	return st
}

// DELEG-RT: a request addressed to this machine is claimed, run once as a
// Codex turn and answered with one result record carrying the final; a
// repeated copy of the request does not run it again.
func TestDelegationRoundTrip(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DELEG-RT", options(
		"delegated request runs once and gets one complete result", []string{"F-XM-2", "F-XM-3", "R-1", "R-2"}))
	defer s.Finish()
	as := requester(s)
	tok := s.Token("deleg")
	worker, ok := resolveWorker(s, as)
	if !ok {
		return
	}
	before := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		before[c.ID] = true
	}
	req, ok := start(s, as, worker, "Report the build status "+tok)
	if !ok {
		return
	}
	st := waitTerminal(s, as, req.DelegationID, 60*time.Second)
	s.Settle(2*time.Second, 10*time.Second)
	calls := s.Codex.CallsWithToken(tok)
	s.Check("delegated task runs as one Codex turn", len(calls) == 1, "1 call", fmt.Sprint(len(calls)))
	if len(calls) == 1 && st.Terminal != nil {
		s.Check("result is complete and carries the final", st.Status == "complete" && calls[0].Answer != "" && strings.Contains(st.Terminal.Body, calls[0].Answer),
			"status complete, body = Codex final", fmt.Sprintf("status %q body %q", st.Status, harness.NormalizeSpace(st.Terminal.Body)))
	}

	// A repeated copy of the request (a requester re-posting after a lost
	// response) is not run again.
	_, _, inbox := inboxRecords(s, req.DelegationID)
	for _, m := range s.Graph.Messages(inbox) {
		if sub := recordRE.FindStringSubmatch(m.PlainText()); sub != nil {
			raw, _ := base64.RawURLEncoding.DecodeString(sub[1])
			var r record
			if json.Unmarshal(raw, &r) == nil && r.Kind == "request" && r.DelegationID == req.DelegationID {
				if _, err := s.Graph.PostHTMLAsUser(inbox, m.Content); err != nil {
					t.Fatal(err)
				}
				break
			}
		}
	}
	s.Settle(3*time.Second, 15*time.Second)
	recs, other, _ := inboxRecords(s, req.DelegationID)
	calls = s.Codex.CallsWithToken(tok)
	s.Check("a repeated request is not run again", len(calls) == 1, "1 call", fmt.Sprint(len(calls)))
	s.Check("exactly one result record", len(recs["result"]) == 1, "1", describe(recs))
	s.Check("one claim by this worker", len(recs["claim"]) == 1, "1", describe(recs))
	s.Check("the inbox carries only protocol records", len(other) == 0, "no plain messages", fmt.Sprintf("%d: %q", len(other), other))
	var created []string
	for _, c := range s.Graph.Chats() {
		if !before[c.ID] && c.ID != inbox {
			created = append(created, c.Topic)
		}
	}
	s.Check("the delegated thread gets no chat of its own (not mirrored as a local session)", len(created) == 0, "no new chat", fmt.Sprintf("%q", created))
}

// DELEG-RESTART: the helper stops while a delegated task runs. After the
// restart the task is not run again; the requester gets exactly one
// terminal result (blocked: interrupted by the restart), and a second
// restart adds nothing.
func TestDelegationRestartMidTask(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DELEG-RESTART", options(
		"delegated task interrupted by a restart: not re-run, one terminal result", []string{"F-XM-3", "R-5", "R-4"}))
	defer s.Finish()
	as := requester(s)
	tok := s.Token("deleg-long")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})
	worker, ok := resolveWorker(s, as)
	if !ok {
		return
	}
	req, ok := start(s, as, worker, "Run the long migration "+tok)
	if !ok {
		return
	}
	s.WaitCodexCalls(tok, 1, 60*time.Second)

	s.StopSUT()
	var r resolveResult
	if delegateJSON(s, as, &r, "resolve", "--query", harness.MachineLabel) {
		accepting := false
		for _, c := range r.Candidates {
			if c.MachineID == worker.MachineID {
				accepting = c.Accepting
			}
		}
		s.Check("a stopped helper is not offered as accepting", !accepting, "not accepting", fmt.Sprintf("%+v", r.Candidates))
	}
	s.StartSUT()
	st := waitTerminal(s, as, req.DelegationID, 60*time.Second)
	s.Settle(3*time.Second, 15*time.Second)

	s.StopSUT()
	s.StartSUT()
	_, _, inbox := inboxRecords(s, req.DelegationID)
	if _, ok := s.WaitPolled(inbox, s.Graph.LastSeq(), 30*time.Second); ok {
		s.Settle(3*time.Second, 15*time.Second)
	}
	recs, _, _ := inboxRecords(s, req.DelegationID)
	calls := s.Codex.CallsWithToken(tok)
	s.Check("R-5b the interrupted task is not re-run", len(calls) == 1, "1 call (the original)", fmt.Sprint(len(calls)))
	s.Check("exactly one result record", len(recs["result"]) == 1, "1", describe(recs))
	body := ""
	if st.Terminal != nil {
		body = st.Terminal.Body
	}
	s.Check("the result reports the interruption", st.Status == "blocked" && strings.Contains(strings.ToLower(body), "interrupt"),
		"status blocked, body names the interruption", fmt.Sprintf("status %q body %q", st.Status, harness.NormalizeSpace(body)))
	s.Check("one claim by this worker", len(recs["claim"]) == 1, "1", describe(recs))
}
