package delegation

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
)

// registryTopic is the chat the machine cards are posted in.
const registryTopic = "CXP Machine Registry"

func options(title string, guarantees []string) harness.Options {
	return harness.Options{
		Guarantees: guarantees,
		Title:      title,
		Budget:     120 * time.Second,
		SUTEnv:     map[string]string{"E2E_SUT_MACHINE_REGISTRY": "1"},
	}
}

// card returns this machine's newest card in the registry chat.
func card(s *harness.Scenario) (machineregistry.MachineCard, bool) {
	reg, ok := s.FindChat(registryTopic)
	if !ok {
		return machineregistry.MachineCard{}, false
	}
	var best machineregistry.MachineCard
	found := false
	for _, m := range s.Graph.Messages(reg.ID) {
		if c, ok := machineregistry.ParseCardMessage(m.Content); ok && (!found || c.Sequence >= best.Sequence) {
			best, found = c, true
		}
	}
	return best, found
}

// waitInbox resolves the machine the way `cxp delegate` does: from its
// card in the registry chat (online and accepting) to the inbox chat its
// inbox_ref opens (the meeting externalId). It returns the inbox and the
// machine id requests are addressed to.
func waitInbox(s *harness.Scenario) (fakegraph.ChatInfo, string) {
	var c machineregistry.MachineCard
	s.Require("machine card published, accepting", 30*time.Second, func() (bool, string) {
		var ok bool
		c, ok = card(s)
		return ok && c.Accepting && c.InboxRef != "" && c.MachineID != "", fmt.Sprintf("card %v: accepting %v inbox_ref %q", ok, c.Accepting, c.InboxRef)
	})
	var inbox fakegraph.ChatInfo
	s.Require("the card's inbox_ref opens the inbox chat", 30*time.Second, func() (bool, string) {
		for _, ch := range s.Graph.Chats() {
			if ch.ExternalID == c.InboxRef {
				inbox = ch
				return true, ch.Topic
			}
		}
		return false, fmt.Sprintf("no chat with externalId %q", c.InboxRef)
	})
	status := machineregistry.ObserveMessages([]machineregistry.ChatMessage{{ID: "card", Body: machineregistry.ChatMessageBody{Content: machineregistry.RenderCardHTML(c)}}}, c.RegistryKey, time.Now())
	s.Check("a requester sees the machine online and accepting", len(status) == 1 && status[0].State == "online" && status[0].Accepting,
		"online, accepting", fmt.Sprintf("%+v", status))
	s.Check("the inbox title names no internal id", !strings.Contains(inbox.Topic, c.MachineID), "no machine id", inbox.Topic)
	return inbox, c.MachineID
}

// request posts a delegation request for machine as the owner (another
// machine's helper writes as the same account) and returns it.
func request(s *harness.Scenario, inboxID, machine, objective string) delegation.Record {
	r, err := delegation.NewRequestRecord("src-session-"+s.Token("src"), "src-turn-1", "", nil, machine,
		delegation.TaskSpec{Title: "E2E delegated task", Objective: objective}, time.Now().UTC())
	if err != nil {
		s.Check("request record built", false, "no error", err.Error())
		return r
	}
	repost(s, inboxID, r)
	return r
}

func repost(s *harness.Scenario, inboxID string, r delegation.Record) {
	if _, err := s.Graph.PostHTMLAsUser(inboxID, delegation.RenderRecordHTML(r)); err != nil {
		s.Check("request posted", false, "no error", err.Error())
	}
}

// records returns the decoded records of one delegation in the inbox, by
// kind, and the inbox messages that are not records.
func records(s *harness.Scenario, inboxID, id string) (map[string][]delegation.Record, []string) {
	out := map[string][]delegation.Record{}
	var other []string
	for _, m := range s.Graph.Messages(inboxID) {
		r, ok := delegation.ParseRecordMessage(m.Content)
		if !ok {
			if m.MessageType == "" || m.MessageType == "message" {
				other = append(other, harness.NormalizeSpace(m.PlainText()))
			}
			continue
		}
		if r.DelegationID == id {
			out[r.Kind] = append(out[r.Kind], r)
		}
	}
	return out, other
}

func describe(recs map[string][]delegation.Record) string {
	var b []string
	for _, k := range []string{delegation.RequestKind, delegation.ClaimKind, delegation.StatusKind, delegation.ResultKind} {
		var st []string
		for _, r := range recs[k] {
			st = append(st, r.Status)
		}
		b = append(b, fmt.Sprintf("%s=%d%v", k, len(recs[k]), st))
	}
	return strings.Join(b, " ")
}

// DELEG-RT: a request addressed to this machine is claimed, run once as a
// Codex turn and answered with one result record carrying the final; a
// repeated copy of the request does not run it again.
func TestDelegationRoundTrip(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DELEG-RT", options(
		"delegated request runs once and gets one complete result", []string{"F-XM-2", "F-XM-3", "R-1", "R-2"}))
	defer s.Finish()
	tok := s.Token("deleg")
	inbox, machine := waitInbox(s)
	s.Note("inbox %q, machine %q", inbox.Topic, machine)
	before := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		before[c.ID] = true
	}
	req := request(s, inbox.ID, machine, "Report the build status "+tok)

	var result delegation.Record
	s.WaitFor("result record posted", 60*time.Second, func() (bool, string) {
		recs, _ := records(s, inbox.ID, req.DelegationID)
		if rs := recs[delegation.ResultKind]; len(rs) > 0 {
			result = rs[0]
			return true, describe(recs)
		}
		return false, describe(recs)
	})
	s.Settle(2*time.Second, 10*time.Second)
	calls := s.Codex.CallsWithToken(tok)
	s.Check("delegated task runs as one Codex turn", len(calls) == 1, "1 call", fmt.Sprint(len(calls)))
	if len(calls) == 1 {
		s.Check("result is complete and carries the final", result.Status == delegation.StateComplete && calls[0].Answer != "" && strings.Contains(result.Body, calls[0].Answer),
			"status complete, body = Codex final", fmt.Sprintf("status %q body %q", result.Status, harness.NormalizeSpace(result.Body)))
	}

	// A repeated copy (the requester re-posting after a lost response) is
	// not run again.
	repost(s, inbox.ID, req)
	s.Settle(3*time.Second, 15*time.Second)
	recs, other := records(s, inbox.ID, req.DelegationID)
	calls = s.Codex.CallsWithToken(tok)
	s.Check("a repeated request is not run again", len(calls) == 1, "1 call", fmt.Sprint(len(calls)))
	s.Check("exactly one result record", len(recs[delegation.ResultKind]) == 1, "1", describe(recs))
	s.Check("one claim by this worker", len(recs[delegation.ClaimKind]) == 1, "1", describe(recs))
	s.Check("the inbox carries only protocol records", len(other) == 0, "no plain messages", fmt.Sprintf("%d: %q", len(other), other))
	var created []string
	for _, c := range s.Graph.Chats() {
		if !before[c.ID] {
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
	tok := s.Token("deleg-long")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})
	inbox, machine := waitInbox(s)
	req := request(s, inbox.ID, machine, "Run the long migration "+tok)
	s.WaitCodexCalls(tok, 1, 60*time.Second)

	s.StopSUT()
	c, _ := card(s)
	s.Check("a stopped helper's card says draining, not accepting", c.Draining && !c.Accepting,
		"draining", fmt.Sprintf("accepting %v draining %v", c.Accepting, c.Draining))
	s.StartSUT()
	var result delegation.Record
	s.WaitFor("terminal result after the restart", 60*time.Second, func() (bool, string) {
		recs, _ := records(s, inbox.ID, req.DelegationID)
		if rs := recs[delegation.ResultKind]; len(rs) > 0 {
			result = rs[0]
			return true, describe(recs)
		}
		return false, describe(recs)
	})
	s.Settle(3*time.Second, 15*time.Second)

	s.StopSUT()
	s.StartSUT()
	if _, ok := s.WaitPolled(inbox.ID, s.Graph.LastSeq(), 30*time.Second); ok {
		s.Settle(3*time.Second, 15*time.Second)
	}
	recs, _ := records(s, inbox.ID, req.DelegationID)
	calls := s.Codex.CallsWithToken(tok)
	s.Check("R-5b the interrupted task is not re-run", len(calls) == 1, "1 call (the original)", fmt.Sprint(len(calls)))
	s.Check("exactly one result record", len(recs[delegation.ResultKind]) == 1, "1", describe(recs))
	s.Check("the result reports the interruption", result.Status == delegation.StateBlocked && strings.Contains(strings.ToLower(result.Body), "interrupt"),
		"status blocked, body names the interruption", fmt.Sprintf("status %q body %q", result.Status, harness.NormalizeSpace(result.Body)))
	s.Check("one claim by this worker", len(recs[delegation.ClaimKind]) == 1, "1", describe(recs))
}
