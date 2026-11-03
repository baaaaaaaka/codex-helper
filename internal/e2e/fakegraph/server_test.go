package fakegraph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type client struct {
	t     *testing.T
	s     *Server
	token string
}

func newClient(t *testing.T) *client {
	s := New(Options{})
	t.Cleanup(s.Close)
	access, _ := s.IssueTokens("test")
	return &client{t: t, s: s, token: access}
}

func (c *client) do(method, path string, body any) (int, map[string]any) {
	c.t.Helper()
	var rd io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		rd = bytes.NewReader(data)
	}
	if strings.HasPrefix(path, "https://graph.microsoft.com") {
		path = strings.TrimPrefix(path, "https://graph.microsoft.com")
	} else {
		path = "/v1.0" + path
	}
	req, err := http.NewRequest(method, c.s.URL()+path, rd)
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (c *client) chat() string {
	return c.s.CreateMeetingChatAsUser("t").ID
}

func TestListMessagesRejectsUnsupportedOperatorsAsContractViolations(t *testing.T) {
	c := newClient(t)
	chat := c.chat()
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	for _, q := range []string{
		"$filter=" + url.QueryEscape("lastModifiedDateTime ge "+ts) + "&$orderby=" + url.QueryEscape("lastModifiedDateTime desc"),
		"$filter=" + url.QueryEscape("lastModifiedDateTime gt "+ts),
		"$orderby=" + url.QueryEscape("lastModifiedDateTime asc"),
		"$top=51",
		"$select=id",
	} {
		status, _ := c.do("GET", "/chats/"+url.PathEscape(chat)+"/messages?"+q, nil)
		if status != http.StatusBadRequest {
			t.Fatalf("query %q: status %d, want 400", q, status)
		}
	}
	if got := len(c.s.Violations()); got != 5 {
		t.Fatalf("violations = %d, want 5: %v", got, c.s.Violations())
	}
	for _, v := range c.s.Violations() {
		if v.Kind != "graph-contract" {
			t.Fatalf("violation kind %q, want graph-contract: %v", v.Kind, v)
		}
	}
}

func TestListMessagesPagesNewestFirstWithNextLink(t *testing.T) {
	c := newClient(t)
	chat := c.chat()
	for i := 0; i < 45; i++ {
		if _, err := c.s.PostAsUser(chat, fmt.Sprintf("m%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	var order []string
	path := "/chats/" + url.PathEscape(chat) + "/messages?$top=20"
	pages := 0
	for path != "" {
		status, body := c.do("GET", path, nil)
		if status != 200 {
			t.Fatalf("status %d: %v", status, body)
		}
		pages++
		for _, v := range body["value"].([]any) {
			id := v.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatalf("duplicate id %s across pages", id)
			}
			seen[id] = true
			order = append(order, id)
		}
		next, _ := body["@odata.nextLink"].(string)
		path = next
	}
	if pages != 3 || len(seen) != 45 {
		t.Fatalf("pages=%d ids=%d, want 3 pages / 45 ids", pages, len(seen))
	}
	for i := 1; i < len(order); i++ {
		if order[i] > order[i-1] {
			t.Fatalf("not newest-first at %d: %v", i, order[i-1:i+1])
		}
	}
}

func TestFilterGtLtWindow(t *testing.T) {
	c := newClient(t)
	chat := c.chat()
	a, _ := c.s.PostAsUser(chat, "a")
	time.Sleep(3 * time.Millisecond)
	b, _ := c.s.PostAsUser(chat, "b")
	time.Sleep(3 * time.Millisecond)
	_, _ = c.s.PostAsUser(chat, "c")
	f := "lastModifiedDateTime gt " + a.Modified.Format(time.RFC3339Nano) + " and lastModifiedDateTime lt " + b.Modified.Add(time.Millisecond).Format(time.RFC3339Nano)
	status, body := c.do("GET", "/chats/"+url.PathEscape(chat)+"/messages?$orderby="+url.QueryEscape("lastModifiedDateTime desc")+"&$filter="+url.QueryEscape(f), nil)
	if status != 200 {
		t.Fatalf("status %d %v", status, body)
	}
	vals := body["value"].([]any)
	if len(vals) != 1 || vals[0].(map[string]any)["id"] != b.ID {
		t.Fatalf("window = %v, want only b", vals)
	}
}

func TestCreateOrGetIsIdempotentByExternalID(t *testing.T) {
	c := newClient(t)
	body := map[string]any{"subject": "s", "externalId": "ext-1", "startDateTime": "2026-01-01T00:00:00Z", "endDateTime": "2026-01-02T00:00:00Z"}
	s1, m1 := c.do("POST", "/me/onlineMeetings/createOrGet", body)
	s2, m2 := c.do("POST", "/me/onlineMeetings/createOrGet", body)
	if s1 != 201 || s2 != 200 {
		t.Fatalf("statuses %d,%d want 201,200", s1, s2)
	}
	t1 := m1["chatInfo"].(map[string]any)["threadId"]
	t2 := m2["chatInfo"].(map[string]any)["threadId"]
	if t1 != t2 || len(c.s.Chats()) != 1 {
		t.Fatalf("threads %v %v chats %d", t1, t2, len(c.s.Chats()))
	}
}

func TestPostValidatesMentionsAndRequiresAuth(t *testing.T) {
	c := newClient(t)
	chat := c.chat()
	status, _ := c.do("POST", "/chats/"+url.PathEscape(chat)+"/messages", map[string]any{
		"body": map[string]any{"contentType": "html", "content": `<at id="0">Owner</at> hi`},
	})
	if status != 400 {
		t.Fatalf("mention without mentions[]: %d, want 400", status)
	}
	c.token = "bogus"
	status, _ = c.do("GET", "/me", nil)
	if status != 401 {
		t.Fatalf("bad token: %d, want 401", status)
	}
}

func TestAcceptedLostStoresMessageButDropsResponse(t *testing.T) {
	c := newClient(t)
	chat := c.chat()
	h := c.s.AddFault(Fault{Name: "lost", Kind: FaultAcceptedLost, Method: "POST", Route: "POST /chats/{chat}/messages", Times: 1})
	data, _ := json.Marshal(map[string]any{"body": map[string]any{"contentType": "html", "content": "<p>x</p>"}})
	req, _ := http.NewRequest("POST", c.s.URL()+"/v1.0/chats/"+url.PathEscape(chat)+"/messages", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("expected a transport error, got status %d", resp.StatusCode)
	}
	if h.Fired() != 1 || len(c.s.Messages(chat)) != 1 {
		t.Fatalf("fired=%d messages=%d, want 1/1", h.Fired(), len(c.s.Messages(chat)))
	}
}

func TestRetryAfterFault(t *testing.T) {
	c := newClient(t)
	c.s.AddFault(Fault{Name: "429", Kind: FaultStatus, Status: 429, RetryAfter: 1500 * time.Millisecond, Route: "GET /me"})
	req, _ := http.NewRequest("GET", c.s.URL()+"/v1.0/me", nil)
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "2" {
		t.Fatalf("status %d retry-after %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}
