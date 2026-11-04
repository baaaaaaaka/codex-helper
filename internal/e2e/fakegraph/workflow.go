package fakegraph

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Teams Workflows ("Post to a chat when a webhook request is received")
// webhook, as the owner sets it up in Power Automate. The helper POSTs an
// Adaptive Card wrapped in a {"type":"message","attachments":[...]}
// envelope to a pre-signed URL; the service answers 202 Accepted with an
// empty body and posts the card (as the Workflows bot) asynchronously.
//
// The fake validates the envelope, records every accepted card, and does
// not post the card into any chat (where it lands is the flow's business,
// not the helper's). Faults apply by Host: WorkflowHost.

// WorkflowHost is the host of fake Workflow webhook URLs.
const WorkflowHost = "defaulte2e00000000000000000000057.e2.environment.api.powerplatform.com"

const (
	workflowPathPrefix = "/powerautomate/automations/direct/workflows/"
	workflowPathSuffix = "/triggers/manual/paths/invoke"
	maxWorkflowCard    = 28 * 1024
)

type workflow struct {
	id  string
	sig string
}

// WorkflowCard is one card the webhook accepted (2xx) or refused.
type WorkflowCard struct {
	At       time.Time
	Workflow string
	Instance string
	Status   int
	Fault    string
	Raw      string
	// Texts are the TextBlock texts of the card, in order.
	Texts []string
	// Actions are the card actions (type, title, url).
	Actions []map[string]any
}

// Text joins the card's TextBlocks with newlines.
func (c WorkflowCard) Text() string { return strings.Join(c.Texts, "\n") }

// NewWorkflowWebhook registers a webhook and returns its URL.
func (s *Server) NewWorkflowWebhook() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workflows == nil {
		s.workflows = map[string]*workflow{}
	}
	s.idCounter++
	w := &workflow{id: fmt.Sprintf("e2e0f10e%024x", s.idCounter), sig: fmt.Sprintf("e2esig%026x", s.idCounter*7919)}
	s.workflows[w.id] = w
	return "https://" + WorkflowHost + ":443" + workflowPathPrefix + w.id + workflowPathSuffix +
		"?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=" + w.sig
}

// WorkflowCards returns every card the webhook received, accepted or not.
func (s *Server) WorkflowCards() []WorkflowCard {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]WorkflowCard(nil), s.cards...)
}

// AcceptedWorkflowCards returns the cards the webhook accepted (2xx).
func (s *Server) AcceptedWorkflowCards() []WorkflowCard {
	var out []WorkflowCard
	for _, c := range s.WorkflowCards() {
		if c.Status >= 200 && c.Status < 300 {
			out = append(out, c)
		}
	}
	return out
}

// recordWorkflowFault records a card attempt answered by an injected fault.
func (s *Server) recordWorkflowFault(rec Request, body []byte, status int) {
	if rec.Host != WorkflowHost {
		return
	}
	card := parseWorkflowCard(body)
	card.At, card.Instance, card.Status, card.Fault = rec.At, rec.Instance, status, rec.Fault
	s.mu.Lock()
	s.cards = append(s.cards, card)
	s.mu.Unlock()
}

func parseWorkflowCard(body []byte) WorkflowCard {
	card := WorkflowCard{Raw: truncate(string(body), 32*1024)}
	var env struct {
		Attachments []struct {
			Content struct {
				Body    []map[string]any `json:"body"`
				Actions []map[string]any `json:"actions"`
			} `json:"content"`
		} `json:"attachments"`
	}
	if json.Unmarshal(body, &env) == nil {
		for _, a := range env.Attachments {
			var walk func(items []map[string]any)
			walk = func(items []map[string]any) {
				for _, it := range items {
					if t, _ := it["text"].(string); t != "" {
						card.Texts = append(card.Texts, t)
					}
					for _, k := range []string{"items", "columns"} {
						if sub, ok := it[k].([]any); ok {
							var m []map[string]any
							for _, x := range sub {
								if mm, ok := x.(map[string]any); ok {
									m = append(m, mm)
								}
							}
							walk(m)
						}
					}
				}
			}
			walk(a.Content.Body)
			card.Actions = append(card.Actions, a.Content.Actions...)
		}
	}
	return card
}

func (s *Server) handleWorkflow(r *http.Request, body []byte, rec *Request) response {
	p := r.URL.Path
	if !strings.HasPrefix(p, workflowPathPrefix) || !strings.HasSuffix(p, workflowPathSuffix) {
		return violationResponse(rec, "unmodeled", "no such Workflow route in the fake: "+p)
	}
	id := strings.TrimSuffix(strings.TrimPrefix(p, workflowPathPrefix), workflowPathSuffix)
	s.mu.Lock()
	w := s.workflows[id]
	s.mu.Unlock()
	if w == nil {
		return response{status: 404, body: map[string]any{"error": map[string]any{"code": "WorkflowNotFound", "message": "The workflow was not found."}}}
	}
	q := r.URL.Query()
	if q.Get("sig") != w.sig || q.Get("api-version") == "" || q.Get("sp") == "" || q.Get("sv") == "" {
		return response{status: 401, body: map[string]any{"error": map[string]any{"code": "DirectApiAuthorizationRequired", "message": "The request must be authenticated with a valid signature."}}}
	}
	if r.Method != http.MethodPost {
		return violationResponse(rec, "workflow-contract", "Workflow webhooks accept only POST")
	}
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return violationResponse(rec, "workflow-contract", "Workflow webhook body must be application/json, got "+ct)
	}
	if r.Header.Get("Authorization") != "" {
		return violationResponse(rec, "workflow-contract", "Workflow webhook URLs are pre-signed; the helper must not send its Graph token there")
	}
	if len(body) > maxWorkflowCard {
		return violationResponse(rec, "workflow-contract", fmt.Sprintf("card payload is %d bytes, over the 28 KB Teams card limit", len(body)))
	}
	var env struct {
		Type        string `json:"type"`
		Attachments []struct {
			ContentType string          `json:"contentType"`
			Content     json.RawMessage `json:"content"`
		} `json:"attachments"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return violationResponse(rec, "workflow-contract", "Workflow webhook body is not JSON")
	}
	if env.Type != "message" || len(env.Attachments) == 0 {
		return violationResponse(rec, "workflow-contract", `body must be {"type":"message","attachments":[...]}`)
	}
	for _, a := range env.Attachments {
		if a.ContentType != "application/vnd.microsoft.card.adaptive" {
			return violationResponse(rec, "workflow-contract", "attachment contentType must be application/vnd.microsoft.card.adaptive, got "+a.ContentType)
		}
		var card struct {
			Type    string `json:"type"`
			Version string `json:"version"`
		}
		if json.Unmarshal(a.Content, &card) != nil || card.Type != "AdaptiveCard" || card.Version == "" {
			return violationResponse(rec, "workflow-contract", "attachment content must be an AdaptiveCard object with a version")
		}
	}
	card := parseWorkflowCard(body)
	card.At, card.Workflow, card.Instance, card.Status = rec.At, id, rec.Instance, http.StatusAccepted
	s.mu.Lock()
	s.cards = append(s.cards, card)
	s.mu.Unlock()
	return response{status: http.StatusAccepted}
}
