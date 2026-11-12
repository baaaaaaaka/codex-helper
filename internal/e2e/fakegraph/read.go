package fakegraph

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// Only the read subset used by CLI scenarios: GET-only batches and literal
// full-text searches. No KQL, channels, indexing lag or search authorization
// expansion is simulated. See FIDELITY.md and the real Graph experiment.
func (s *Server) readBatch(body []byte, rec *Request) response {
	var in struct {
		Requests []struct {
			ID     string `json:"id"`
			Method string `json:"method"`
			URL    string `json:"url"`
		} `json:"requests"`
	}
	if json.Unmarshal(body, &in) != nil || len(in.Requests) < 1 || len(in.Requests) > 20 {
		return violationResponse(rec, "graph-contract", "read batch requires 1..20 GET requests")
	}
	ids := map[string]bool{}
	out := []any{}
	for _, r := range in.Requests {
		u, e := url.Parse(r.URL)
		if e != nil || r.Method != "GET" || r.ID == "" || ids[r.ID] || u.IsAbs() || !strings.HasPrefix(u.Path, "/") || u.Host != "" {
			return violationResponse(rec, "graph-contract", "invalid read batch item")
		}
		ids[r.ID] = true
		route := matchGraphRoute(http.MethodGet, u.Path)
		sub := *rec
		sub.Method = "GET"
		sub.Route = route.name
		sub.ChatID = route.chatID
		if route.name == "" {
			return violationResponse(rec, "unmodeled", "read batch route not modelled")
		}
		resp := s.handleGraph(route, u.Query(), nil, &sub)
		if resp.violation != nil {
			return resp
		}
		out = append(out, map[string]any{"id": r.ID, "status": resp.status, "headers": resp.headers, "body": resp.body})
	}
	return response{status: 200, body: map[string]any{"responses": out}}
}
func (s *Server) searchMessages(body []byte, rec *Request) response {
	var in struct {
		Requests []struct {
			Types []string `json:"entityTypes"`
			Query struct {
				String string `json:"queryString"`
			} `json:"query"`
			From int `json:"from"`
			Size int `json:"size"`
		} `json:"requests"`
	}
	if json.Unmarshal(body, &in) != nil || len(in.Requests) != 1 {
		return violationResponse(rec, "graph-contract", "expected one search request")
	}
	q := in.Requests[0]
	if len(q.Types) != 1 || q.Types[0] != "chatMessage" || q.From < 0 || q.Size < 1 || q.Size > 20 || q.Query.String == "" {
		return violationResponse(rec, "graph-contract", "invalid message search request")
	}
	if strings.ContainsAny(q.Query.String, ":<>\"") {
		return violationResponse(rec, "unmodeled", "fake search supports literal terms only")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var matched []*Message
	for _, c := range s.chats {
		member := false
		for _, id := range c.Members {
			member = member || id == s.opts.Owner.ID
		}
		if !member {
			continue
		}
		for _, m := range c.Messages {
			if strings.Contains(strings.ToLower(m.PlainText()), strings.ToLower(q.Query.String)) {
				matched = append(matched, m)
			}
		}
	}
	sort.SliceStable(matched, func(i, j int) bool { return matched[i].Created.After(matched[j].Created) })
	hits := []any{}
	end := min(len(matched), q.From+q.Size)
	for i := q.From; i < end; i++ {
		m := matched[i]
		hits = append(hits, map[string]any{"summary": m.PlainText(), "resource": map[string]any{"id": m.ID, "chatId": m.ChatID, "createdDateTime": formatTime(m.Created)}})
	}
	return response{status: 200, body: map[string]any{"value": []any{map[string]any{"hitsContainers": []any{map[string]any{"hits": hits, "total": len(hits), "moreResultsAvailable": end < len(matched)}}}}}}
}
