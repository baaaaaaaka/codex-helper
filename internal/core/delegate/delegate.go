// Package delegate implements engine.Delegations over the existing
// cross-machine delegation wire format (internal/teams/delegation), so the
// rebuilt core's worker interoperates with `cxp delegate` requesters.
//
// It is an adapter: the engine never imports internal/teams.
package delegate

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
)

// reuseRejectedPrefix starts a final that refuses a reused thread (the
// worker prompt tells Codex to answer so; same as the old worker).
const reuseRejectedPrefix = "CXP_REUSE_REJECTED:"

// maxBody bounds a result body so the encoded record stays one message.
const maxBody = 12000

// Protocol is engine.Delegations for one machine.
type Protocol struct{ machine string }

// New returns the protocol of the machine with registry id machineID.
func New(machineID string) *Protocol { return &Protocol{machine: strings.TrimSpace(machineID)} }

var _ engine.Delegations = (*Protocol)(nil)

// Decode reads one inbox message.
func (p *Protocol) Decode(text string, now time.Time) (engine.DelegRecord, bool) {
	r, ok := parse(text)
	if !ok {
		return engine.DelegRecord{}, false
	}
	out := engine.DelegRecord{ID: r.DelegationID, Worker: r.WorkerInstanceID,
		Mine: p.machine != "" && strings.TrimSpace(r.MachineID) == p.machine}
	switch r.Kind {
	case delegation.RequestKind:
		out.Kind, out.Thread = engine.DelegRequest, strings.TrimSpace(r.RemoteThreadID)
		if t, err := time.Parse(time.RFC3339Nano, r.ExpiresAt); err == nil && now.After(t) {
			out.Expired = true
		}
	case delegation.ClaimKind:
		out.Kind = engine.DelegClaim
	case delegation.StatusKind:
		out.Kind = engine.DelegStatus
	case delegation.ResultKind:
		out.Kind = engine.DelegResult
	case delegation.TombstoneKind:
		out.Kind = engine.DelegCancel
	default:
		return engine.DelegRecord{}, false
	}
	return out, true
}

// parse reads a record from message text or HTML; the code-span
// backticks of a record the engine posted (read back as plain text) are
// ignored.
func parse(text string) (delegation.Record, bool) {
	return delegation.ParseRecordMessage(strings.ReplaceAll(text, "`", " "))
}

func (p *Protocol) request(text string) (delegation.Record, error) {
	r, ok := parse(text)
	if !ok || r.Kind != delegation.RequestKind {
		return delegation.Record{}, errors.New("not a delegation request")
	}
	return r, nil
}

func (p *Protocol) claim(request, worker string, now time.Time) (delegation.Record, delegation.Record, error) {
	req, err := p.request(request)
	if err != nil {
		return req, delegation.Record{}, err
	}
	// Deterministic for (delegation, machine, worker): a rebuilt claim
	// matches the one posted earlier.
	c, err := delegation.NewClaimRecord(req.DelegationID, p.machine, worker, 1, now)
	if err != nil {
		return req, c, err
	}
	c.InboxRef, c.InboxGeneration = req.InboxRef, req.InboxGeneration
	return req, c, nil
}

func text(r delegation.Record) string {
	html := delegation.RenderRecordHTML(r)
	return strings.TrimSuffix(strings.TrimPrefix(html, "<p>"), "</p>")
}

// Claim is this worker's claim of a request.
func (p *Protocol) Claim(request, worker string, now time.Time) (string, error) {
	_, c, err := p.claim(request, worker, now)
	if err != nil {
		return "", err
	}
	return text(c), nil
}

// Running tells the requester the task started.
func (p *Protocol) Running(request, worker string, now time.Time) (string, error) {
	req, c, err := p.claim(request, worker, now)
	if err != nil {
		return "", err
	}
	s, err := delegation.NewStatusRecord(req.DelegationID, c, delegation.StateRunning, "Delegated worker accepted the task.", now)
	if err != nil {
		return "", err
	}
	s.InboxRef, s.InboxGeneration = c.InboxRef, c.InboxGeneration
	return text(s), nil
}

// Result is the terminal record.
func (p *Protocol) Result(request, worker string, ok bool, body string, now time.Time) (string, error) {
	req, c, err := p.claim(request, worker, now)
	if err != nil {
		return "", err
	}
	status := delegation.StateBlocked
	body = strings.TrimSpace(body)
	if ok {
		status = delegation.StateComplete
		if req.ThreadPolicy == delegation.ThreadPolicyReuse && strings.HasPrefix(body, reuseRejectedPrefix) {
			status = delegation.StateReuseRejected
			body = strings.TrimSpace(strings.TrimPrefix(body, reuseRejectedPrefix))
			if body == "" {
				body = "remote worker rejected reuse for this task"
			}
		}
		if body == "" {
			body = "Delegated worker completed without a final message."
		}
	}
	body = clip(body, maxBody)
	r, err := delegation.NewResultRecord(req.DelegationID, c, status, body, 1, now)
	if err != nil {
		return "", err
	}
	r.InboxRef, r.InboxGeneration = c.InboxRef, c.InboxGeneration
	r.ThreadUpdate = threadUpdate(req, status, body)
	return text(r), nil
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "\n[truncated]"
}

func threadUpdate(req delegation.Record, status, body string) *delegation.ThreadUpdate {
	if strings.TrimSpace(req.RemoteThreadID) == "" {
		return nil
	}
	if status == delegation.StateReuseRejected {
		return &delegation.ThreadUpdate{LastResultSummary: body}
	}
	if status != delegation.StateComplete && status != delegation.StateBlocked {
		return nil
	}
	u := &delegation.ThreadUpdate{Title: strings.TrimSpace(req.Spec.Title), SummaryDelta: body, LastResultSummary: body}
	if u.Title == "" {
		u.Title = strings.TrimSpace(req.Spec.Objective)
	}
	return u
}

// Prompt is the Codex input of a request (the old worker's wording).
func (p *Protocol) Prompt(request string) (string, error) {
	req, err := p.request(request)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("You are Agent B completing a CXP cross-machine delegated task.\n\n")
	if t := strings.TrimSpace(req.RemoteThreadID); t != "" {
		b.WriteString("Remote thread: " + t + "\n")
		policy := req.ThreadPolicy
		if policy == "" {
			policy = delegation.ThreadPolicyNew
		}
		b.WriteString("Thread policy: " + policy + "\n")
		if req.ThreadPolicy == delegation.ThreadPolicyReuse {
			b.WriteString("If this reused thread has the wrong context for the task, reply with `" + reuseRejectedPrefix + " <short reason>` and stop. Otherwise continue using the existing context.\n")
		}
		b.WriteString("\n")
	}
	if t := strings.TrimSpace(req.Spec.Title); t != "" {
		b.WriteString("Title: " + t + "\n\n")
	}
	b.WriteString("Objective:\n" + strings.TrimSpace(req.Spec.Objective) + "\n")
	for _, sec := range []struct {
		name  string
		items []string
	}{{"Context", req.Spec.Context}, {"Constraints", req.Spec.Constraints}, {"Allowed actions", req.Spec.AllowedActions}} {
		if len(sec.items) == 0 {
			continue
		}
		b.WriteString("\n" + sec.name + ":\n")
		for _, it := range sec.items {
			if it = strings.TrimSpace(it); it != "" {
				b.WriteString("- " + it + "\n")
			}
		}
	}
	b.WriteString("\nReturn a concise final result for Agent A. Do not delegate this same task back to Agent A.\n")
	return b.String(), nil
}
