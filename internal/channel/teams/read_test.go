package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func readTestReader(t *testing.T, h http.HandlerFunc) *Reader {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer read" {
			t.Errorf("wrong read identity: %q", r.Header.Get("Authorization"))
		}
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	r, e := NewReader(Config{GraphURL: srv.URL + "/v1.0", Tokens: &staticTokens{tok: "write"}, ReadTokens: &staticTokens{tok: "read"}, TenantID: "tenant", Sleep: noSleep})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
func readFixture(id string, n int, body string) readRecord {
	m := readRecord{ETag: "etag-" + id}
	m.ID = id
	m.Created = fmt.Sprintf("2026-11-01T00:00:%02dZ", n)
	m.Modified = m.Created
	m.MessageType = "message"
	m.Body.ContentType = "html"
	m.Body.Content = body
	return m
}
func writeReadJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func chatTarget(id string) ReadTarget {
	kind := "chat"
	if id != "" {
		kind = "chat_message"
	}
	return ReadTarget{Kind: kind, Tenant: "tenant", Chat: "19:test@thread.v2", Message: id}
}
func readOpts() ReadOptions { return ReadOptions{Limit: 50, Budget: DefaultReadBudget()} }

func TestTeamsReadTargetLinks(t *testing.T) {
	chat := chatTarget("123")
	channel := ReadTarget{Kind: "channel_message", Tenant: "tenant", Team: "group", Channel: "19:c@thread.tacv2", Message: "456", Parent: "123"}
	for _, want := range []ReadTarget{chatTarget(""), chat, channel, channel.root(), {Kind: "channel", Tenant: "tenant", Team: "group", Channel: "19:c@thread.tacv2"}} {
		for _, raw := range []string{want.webURL(), want.Ref()} {
			got, e := ParseReadTarget(raw, "tenant")
			if e != nil || got != want {
				t.Fatalf("parse %s = %+v, %v; want %+v", raw, got, e, want)
			}
		}
	}
	bad := []string{"http://teams.microsoft.com/l/chat/x/0", "https://teams.microsoft.com.evil/l/chat/x/0", "https://teams.microsoft.com:443/l/chat/x/0", "https://teams.microsoft.com/l/chat/0/0?users=a", "https://teams.microsoft.com/l/meetup-join/x/0", "https://teams.microsoft.com/l/message/19:c@thread.tacv2/1", "https://teams.microsoft.com/l/chat/x%2Fy/0", chat.webURL() + "&tenantId=other", chat.webURL() + "&groupId=g", channel.webURL() + "&context=%7B%22contextType%22%3A%22chat%22%7D", strings.Replace(chat.webURL(), "tenantId=tenant", "tenantId=other", 1), "https://teams.microsoft.com/l/chat/x/0?context=%GG"}
	for _, raw := range bad {
		if _, e := ParseReadTarget(raw, "tenant"); e == nil {
			t.Errorf("accepted invalid link: %s", raw)
		}
	}
	if _, e := ParseReadTarget(encodeReadToken(refPrefix, map[string]any{"kind": "chat", "chat": "x", "url": "https://evil"}), ""); e == nil {
		t.Fatal("accepted unknown ref fields")
	}
}
func TestTeamsReadCombinedRangesAndLinks(t *testing.T) {
	var calls atomic.Int32
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		if strings.HasSuffix(req.URL.Path, "/messages/3") {
			writeReadJSON(w, readFixture("3", 3, `<p>继续 <a href="https://example.com/task">任务说明</a></p>`))
			return
		}
		if !strings.HasSuffix(req.URL.Path, "/messages") {
			t.Errorf("unexpected request %s", req.URL.Path)
			w.WriteHeader(404)
			return
		}
		if req.URL.Query().Get("$skiptoken") == "" && req.URL.Query().Get("$orderby") != "createdDateTime desc" {
			t.Error("missing creation sort")
		}
		if req.URL.Query().Get("$filter") != "" || req.URL.Query().Get("$skiptoken") == "old" {
			if req.URL.Query().Get("$skiptoken") == "old" {
				writeReadJSON(w, readPage{Value: []readRecord{readFixture("1", 1, "one")}})
			} else {
				if req.URL.Query().Get("$filter") != "createdDateTime lt 2026-11-01T00:00:03Z" {
					t.Error("wrong filter")
				}
				writeReadJSON(w, readPage{Value: []readRecord{readFixture("2", 2, "two")}, Next: "https://graph.microsoft.com/v1.0/chats/19:test@thread.v2/messages?$skiptoken=old"})
			}
			return
		}
		writeReadJSON(w, readPage{Value: []readRecord{readFixture("4", 4, "four"), readFixture("3", 3, "three"), readFixture("2", 2, "two")}})
	})
	o := readOpts()
	o.Before = 2
	o.Latest = 3
	got, e := r.Read(context.Background(), []string{chatTarget("3").webURL()}, o)
	if e != nil {
		t.Fatal(e)
	}
	if len(got.Messages) != 4 || calls.Load() != 4 || got.Usage.Requests != 4 || len(got.Ranges) != 3 {
		t.Fatalf("wrong combined result: %+v, calls %d", got, calls.Load())
	}
	if !strings.Contains(got.Messages[0].Text, "任务说明: https://example.com/task") {
		t.Fatal("lost named hyperlink")
	}
	for _, rg := range got.Ranges {
		if rg.State != "complete" {
			t.Errorf("incomplete range %+v", rg)
		}
	}
	if got.Ranges[1].Refs[0] != chatTarget("1").Ref() {
		t.Fatal("before window is not chronological")
	}
}
func TestTeamsReadMidPageCursorAndSparsePages(t *testing.T) {
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasSuffix(req.URL.Path, "/messages") {
			writeReadJSON(w, map[string]string{"id": "19:test@thread.v2", "topic": "chat"})
			return
		}
		if req.URL.Query().Get("$skiptoken") == "end" {
			writeReadJSON(w, readPage{Value: []readRecord{readFixture("1", 1, "a")}})
			return
		}
		n := 3
		if req.URL.Query().Get("$top") == "2" {
			n = 2
		}
		msgs := []readRecord{readFixture("3", 3, "c"), readFixture("2", 2, "b"), readFixture("1", 1, "a")}
		pg := readPage{Value: msgs[:n]}
		if n == 2 {
			pg.Next = "https://graph.microsoft.com/v1.0/chats/19:test@thread.v2/messages?$skiptoken=end"
		}
		writeReadJSON(w, pg)
	})
	o := readOpts()
	o.Latest = 3
	o.Budget.MaxMessages = 1
	first, e := r.Read(context.Background(), []string{chatTarget("").Ref()}, o)
	if e != nil {
		t.Fatal(e)
	}
	if len(first.Messages) != 1 || first.Ranges[0].NextCursor == "" || first.Ranges[0].State != "truncated" {
		t.Fatalf("no page continuation: %+v", first)
	}
	o = readOpts()
	o.Cursor = first.Ranges[0].NextCursor
	second, e := r.Read(context.Background(), nil, o)
	if e != nil {
		t.Fatal(e)
	}
	if len(second.Messages) != 2 || second.Messages[0].ID != "2" || second.Messages[1].ID != "1" || second.Ranges[0].State != "complete" {
		t.Fatalf("midpage lost/repeated messages: %+v", second)
	}
}
func TestTeamsReadUnicodeBodyCursorAndEdit(t *testing.T) {
	var edited atomic.Bool
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		gm := readFixture("1", 1, "你好世界")
		if edited.Load() {
			gm.ETag = "changed"
		}
		writeReadJSON(w, gm)
	})
	o := readOpts()
	o.Budget.MaxChars = 3
	got, e := r.Read(context.Background(), []string{chatTarget("1").Ref()}, o)
	if e != nil {
		t.Fatal(e)
	}
	if got.Messages[0].Text != "你好世" || !got.Messages[0].Truncated {
		t.Fatalf("bad Unicode truncation: %+v", got)
	}
	o = readOpts()
	o.Cursor = got.Messages[0].NextCursor
	next, e := r.Read(context.Background(), nil, o)
	if e != nil || len(next.Messages) != 1 || next.Messages[0].Text != "界" || next.Messages[0].Truncated {
		t.Fatalf("bad body continuation %+v %v", next, e)
	}
	edited.Store(true)
	changed, e := r.Read(context.Background(), nil, o)
	if e == nil || len(changed.Messages) != 0 || !strings.Contains(changed.Ranges[0].Error, "changed") {
		t.Fatalf("accepted edited continuation: %+v %v", changed, e)
	}
}
func TestTeamsReadRejectsIgnoredFilterAndForeignPagination(t *testing.T) {
	for _, mode := range []string{"ignored", "foreign", "loop", "empty_next"} {
		t.Run(mode, func(t *testing.T) {
			r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/messages/2") {
					writeReadJSON(w, readFixture("2", 2, "anchor"))
					return
				}
				pg := readPage{Value: []readRecord{readFixture("1", 1, "old")}}
				switch mode {
				case "ignored":
					pg.Value = []readRecord{readFixture("3", 3, "new")}
				case "foreign":
					pg.Next = "https://graph.microsoft.com/v1.0/chats/other/messages?$skiptoken=x"
				case "loop":
					pg.Next = "https://graph.microsoft.com/v1.0/chats/19:test@thread.v2/messages?$skiptoken=x"
				case "empty_next":
					if req.URL.Query().Get("$skiptoken") == "x" {
						pg.Next = ""
					} else {
						pg.Value = nil
						pg.Next = "https://graph.microsoft.com/v1.0/chats/19:test@thread.v2/messages?$skiptoken=x"
					}
				}
				writeReadJSON(w, pg)
			})
			o := readOpts()
			o.Before = 2
			got, e := r.Read(context.Background(), []string{chatTarget("2").Ref()}, o)
			if mode == "empty_next" {
				if e != nil || len(got.Messages) != 2 {
					t.Fatalf("empty next page lost %+v %v", got, e)
				}
			} else if e == nil || got.Ranges[1].State != "error" {
				t.Fatalf("accepted %s: %+v %v", mode, got, e)
			}
		})
	}
}
func TestTeamsReadBatchPartialErrorsAndBudget(t *testing.T) {
	for _, code := range []int{403, 429} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var actual atomic.Int32
			r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
				actual.Add(1)
				if req.Method != "POST" || !strings.HasSuffix(req.URL.Path, "/$batch") {
					t.Errorf("unexpected retry %s", req.URL.Path)
					w.WriteHeader(500)
					return
				}
				writeReadJSON(w, map[string]any{"responses": []any{map[string]any{"id": "1", "status": code, "headers": map[string]string{"Retry-After": "60"}, "body": map[string]any{"error": map[string]string{"code": "Denied"}}}, map[string]any{"id": "0", "status": 200, "body": readFixture("1", 1, "ok")}}})
			})
			o := readOpts()
			o.Budget.MaxRequests = 2
			got, e := r.Read(context.Background(), []string{chatTarget("1").Ref(), chatTarget("2").Ref()}, o)
			if e == nil || len(got.Messages) != 1 || actual.Load() != 1 || got.Usage.Requests != 2 || got.Ranges[1].State != "error" {
				t.Fatalf("bad batch handling %+v %v HTTP=%d", got, e, actual.Load())
			}
		})
	}
	var calls atomic.Int32
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) { calls.Add(1); w.WriteHeader(500) })
	o := readOpts()
	o.Budget.MaxRequests = 2
	got, e := r.Read(context.Background(), []string{chatTarget("1").Ref()}, o)
	if e == nil || calls.Load() != 2 || got.Usage.Requests != 2 {
		t.Fatalf("retry budget bypass %+v %v calls %d", got, e, calls.Load())
	}
}
func TestTeamsReadChannelReplyRoutes(t *testing.T) {
	root := ReadTarget{Kind: "channel_message", Tenant: "tenant", Team: "g", Channel: "19:c@thread.tacv2", Message: "root"}
	reply := root
	reply.Message = "reply"
	reply.Parent = "root"
	var paths []string
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		paths = append(paths, req.URL.Path)
		if strings.HasSuffix(req.URL.Path, "/replies") {
			writeReadJSON(w, readPage{Value: []readRecord{readFixture("reply", 2, "reply")}})
		} else if strings.HasSuffix(req.URL.Path, "/replies/reply") {
			writeReadJSON(w, readFixture("reply", 2, "reply"))
		} else if strings.HasSuffix(req.URL.Path, "/messages/root") {
			writeReadJSON(w, readFixture("root", 1, "root"))
		} else {
			w.WriteHeader(404)
		}
	})
	o := readOpts()
	o.Replies = true
	got, e := r.Read(context.Background(), []string{reply.webURL()}, o)
	if e != nil || len(got.Messages) != 2 || len(paths) != 3 {
		t.Fatalf("bad thread %+v %v paths %v", got, e, paths)
	}
	if got.Messages[0].Target.Parent != "root" {
		t.Fatal("lost reply parent")
	}
	o.Before = 1
	if _, e = r.Read(context.Background(), []string{reply.Ref()}, o); e == nil {
		t.Fatal("accepted chat filter for a channel")
	}
}
func TestTeamsSearchCandidatesAndContinuation(t *testing.T) {
	var bodies []map[string]any
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" || !strings.HasSuffix(req.URL.Path, "/search/query") {
			t.Error("not search")
		}
		b, _ := io.ReadAll(req.Body)
		var body map[string]any
		json.Unmarshal(b, &body)
		bodies = append(bodies, body)
		hits := []any{map[string]any{"summary": "one", "resource": map[string]any{"id": "1", "chatId": "19:test@thread.v2"}}, map[string]any{"summary": "two", "resource": map[string]any{"id": "2", "channelIdentity": map[string]any{"teamId": "g", "channelId": "c"}}}}
		writeReadJSON(w, map[string]any{"value": []any{map[string]any{"hitsContainers": []any{map[string]any{"hits": hits, "total": 6047, "moreResultsAvailable": true}}}}})
	})
	o := SearchOptions{Limit: 2, Budget: DefaultReadBudget()}
	o.Budget.MaxMessages = 1
	first, e := r.Search(context.Background(), "graph", o)
	if e != nil || len(first.Hits) != 1 || first.Hits[0].Ref == "" || first.State != "truncated" {
		t.Fatalf("bad search %+v %v", first, e)
	}
	o = SearchOptions{Limit: 20, Cursor: first.NextCursor, Budget: DefaultReadBudget()}
	next, e := r.Search(context.Background(), "", o)
	if e != nil || len(next.Hits) != 1 || next.Hits[0].State != "unresolved" || next.Hits[0].Ref != "" || next.State != "more" || next.NextCursor == "" {
		t.Fatalf("bad search continuation %+v %v", next, e)
	}
	if len(bodies) != 2 {
		t.Fatal("unexpected search IO")
	}
	if e = ValidateSearch("other", o, "tenant"); e == nil {
		t.Fatal("accepted changed query")
	}
	var c searchCursor
	decodeReadToken(o.Cursor, cursorPrefix, &c)
	c.Tenant = "other"
	o.Cursor = encodeReadToken(cursorPrefix, c)
	if _, e = r.Search(context.Background(), "", o); e == nil {
		t.Fatal("accepted cross-tenant cursor")
	}
}
func TestTeamsReadValidationDoesNoIO(t *testing.T) {
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) { t.Fatal("IO on invalid request") })
	o := readOpts()
	o.Before = 1
	if _, e := r.Read(context.Background(), []string{chatTarget("").Ref()}, o); e == nil {
		t.Fatal("accepted missing anchor")
	}
	o = readOpts()
	o.Cursor = encodeReadToken(cursorPrefix, readCursor{Target: chatTarget("1"), Kind: "latest", Remaining: 2, Next: "https://evil/messages"})
	got, e := r.Read(context.Background(), nil, o)
	if e == nil || len(got.Ranges) != 1 {
		t.Fatalf("invalid cursor not rejected %+v %v", got, e)
	}
	o = readOpts()
	o.Budget.Timeout = 0
	if _, e = r.Read(context.Background(), []string{chatTarget("1").Ref()}, o); e == nil {
		t.Fatal("zero timeout")
	}
}
