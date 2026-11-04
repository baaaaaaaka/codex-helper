package fakegraph

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

const previewQuery = "$expand=lastMessagePreview&$orderby=lastMessagePreview%2FcreatedDateTime+desc"

func chatIDs(body map[string]any) []string {
	var out []string
	for _, v := range body["value"].([]any) {
		out = append(out, v.(map[string]any)["id"].(string))
	}
	return out
}

func TestListChatsExpandsAndOrdersByLastMessagePreview(t *testing.T) {
	c := newClient(t)
	a := c.s.CreateMeetingChatAsUser("a").ID
	b := c.s.CreateMeetingChatAsUser("b").ID
	empty := c.s.CreateMeetingChatAsUser("empty").ID
	if _, err := c.s.PostAsUser(b, "older"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.s.PostAsUser(a, "newest <b>x</b>"); err != nil {
		t.Fatal(err)
	}
	status, body := c.do("GET", "/me/chats?"+previewQuery, nil)
	if status != http.StatusOK {
		t.Fatalf("status %d: %v", status, body)
	}
	if got := chatIDs(body); len(got) != 3 || got[0] != a || got[1] != b || got[2] != empty {
		t.Fatalf("order = %v, want [%s %s %s]", got, a, b, empty)
	}
	first := body["value"].([]any)[0].(map[string]any)
	p, ok := first["lastMessagePreview"].(map[string]any)
	if !ok {
		t.Fatalf("lastMessagePreview missing: %v", first)
	}
	for _, k := range []string{"id", "createdDateTime", "isDeleted", "messageType", "body", "from", "eventDetail"} {
		if _, ok := p[k]; !ok {
			t.Fatalf("chatMessageInfo lacks %q: %v", k, p)
		}
	}
	if p["body"].(map[string]any)["content"] != TextToHTML("newest <b>x</b>") {
		t.Fatalf("preview body = %v", p["body"])
	}
	if _, ok := first["lastMessagePreview@odata.context"]; !ok {
		t.Fatalf("lastMessagePreview@odata.context missing")
	}
	last := body["value"].([]any)[2].(map[string]any)
	if v, ok := last["lastMessagePreview"]; !ok || v != nil {
		t.Fatalf("empty chat preview = %v (present %v), want explicit null", v, ok)
	}
	// Without $expand the property is absent.
	_, plain := c.do("GET", "/me/chats", nil)
	if _, ok := plain["value"].([]any)[0].(map[string]any)["lastMessagePreview"]; ok {
		t.Fatal("lastMessagePreview served without $expand")
	}
}

func TestListChatsPreviewPagingKeepsQuery(t *testing.T) {
	c := newClient(t)
	var ids []string
	for i := 0; i < 5; i++ {
		id := c.s.CreateMeetingChatAsUser("p").ID
		ids = append(ids, id)
	}
	// Post in creation order so preview order is the reverse of ids.
	for _, id := range ids {
		if _, err := c.s.PostAsUser(id, "m"); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	next := "/me/chats?" + previewQuery + "&$top=2"
	for pages := 0; next != ""; pages++ {
		if pages > 5 {
			t.Fatal("paging does not terminate")
		}
		status, body := c.do("GET", next, nil)
		if status != http.StatusOK {
			t.Fatalf("status %d: %v", status, body)
		}
		seen = append(seen, chatIDs(body)...)
		next, _ = body["@odata.nextLink"].(string)
		if next != "" {
			u, _ := url.Parse(next)
			if u.Query().Get("$expand") != "lastMessagePreview" || u.Query().Get("$orderby") != "lastMessagePreview/createdDateTime desc" {
				t.Fatalf("nextLink dropped the query: %s", next)
			}
		}
	}
	if len(seen) != 5 {
		t.Fatalf("seen %v", seen)
	}
	for i := range seen {
		if seen[i] != ids[4-i] {
			t.Fatalf("paged order = %v, want reverse of %v", seen, ids)
		}
	}
}

func TestListChatsPreviewLag(t *testing.T) {
	c := newClient(t)
	c.s.SetPreviewLag(200 * time.Millisecond)
	a := c.s.CreateMeetingChatAsUser("a").ID
	b := c.s.CreateMeetingChatAsUser("b").ID
	if _, err := c.s.PostAsUser(a, "a1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if _, err := c.s.PostAsUser(a, "a2"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.s.PostAsUser(b, "b1"); err != nil {
		t.Fatal(err)
	}
	preview := func(body map[string]any, id string) any {
		for _, v := range body["value"].([]any) {
			ch := v.(map[string]any)
			if ch["id"] == id {
				if p, ok := ch["lastMessagePreview"].(map[string]any); ok {
					return p["body"].(map[string]any)["content"]
				}
				return nil
			}
		}
		t.Fatalf("chat %s not listed", id)
		return nil
	}
	_, body := c.do("GET", "/me/chats?"+previewQuery, nil)
	if got := chatIDs(body); got[0] != a || got[1] != b {
		t.Fatalf("within the lag order = %v, want [a b]", got)
	}
	if p := preview(body, a); p != TextToHTML("a1") {
		t.Fatalf("within the lag a preview = %v, want a1", p)
	}
	if p := preview(body, b); p != nil {
		t.Fatalf("within the lag b preview = %v, want null", p)
	}
	time.Sleep(250 * time.Millisecond)
	_, body = c.do("GET", "/me/chats?"+previewQuery, nil)
	if got := chatIDs(body); got[0] != b || got[1] != a {
		t.Fatalf("after the lag order = %v, want [b a]", got)
	}
	if p := preview(body, a); p != TextToHTML("a2") {
		t.Fatalf("after the lag a preview = %v, want a2", p)
	}
}

func TestListChatsRejectsOtherExpandAndOrderBy(t *testing.T) {
	c := newClient(t)
	for _, q := range []string{"$expand=members", "$orderby=createdDateTime+desc", "$orderby=lastMessagePreview%2FcreatedDateTime+asc"} {
		if status, _ := c.do("GET", "/me/chats?"+q, nil); status != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400", q, status)
		}
	}
	if status, _ := c.do("GET", "/me/chats?$filter="+url.QueryEscape("chatType eq 'group'"), nil); status != http.StatusBadRequest {
		t.Fatal("$filter accepted")
	}
}
