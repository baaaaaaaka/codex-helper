package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTeamsReadMidPageKeepsOriginalPageSize(t *testing.T) {
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasSuffix(req.URL.Path, "/messages") {
			writeReadJSON(w, map[string]string{"id": chatTarget("").Chat})
			return
		}
		top, _ := strconv.Atoi(req.URL.Query().Get("$top"))
		skip, _ := strconv.Atoi(req.URL.Query().Get("$skiptoken"))
		end := min(50, skip+top)
		pg := readPage{}
		for i := skip; i < end; i++ {
			pg.Value = append(pg.Value, readFixture(strconv.Itoa(50-i), 50-i, "message"))
		}
		if end < 50 {
			pg.Next = fmt.Sprintf("https://graph.microsoft.com/v1.0/chats/19:test@thread.v2/messages?$top=%d&$skiptoken=%d", top, end)
		}
		writeReadJSON(w, pg)
	})
	o := readOpts()
	o.Latest = 50
	o.Budget.MaxMessages = 45
	first, e := r.Read(context.Background(), []string{chatTarget("").Ref()}, o)
	if e != nil || len(first.Messages) != 45 || first.Ranges[0].NextCursor == "" {
		t.Fatalf("first page %+v %v", first, e)
	}
	o = readOpts()
	o.Cursor = first.Ranges[0].NextCursor
	last, e := r.Read(context.Background(), nil, o)
	if e != nil || len(last.Messages) != 5 || last.Usage.Requests != 1 {
		t.Fatalf("incorrect page replay %+v %v", last, e)
	}
	for i, m := range last.Messages {
		if m.ID != strconv.Itoa(5-i) {
			t.Fatalf("repeated an already consumed message %s", m.ID)
		}
	}
}

func TestTeamsReadChannelReplyPages(t *testing.T) {
	target := ReadTarget{Kind: "channel_message", Tenant: "tenant", Team: "g", Channel: "c", Message: "root"}
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v1.0/teams/g/channels/c/messages/root" {
			writeReadJSON(w, readFixture("root", 1, "root"))
			return
		}
		if req.URL.Path != "/v1.0/teams/g/channels/c/messages/root/replies" || req.URL.Query().Has("$orderby") || req.URL.Query().Has("$filter") {
			t.Errorf("incorrect reply query %s", req.URL)
		}
		if req.URL.Query().Get("$skiptoken") == "next" {
			writeReadJSON(w, readPage{Value: []readRecord{readFixture("r3", 4, "reply")}})
			return
		}
		writeReadJSON(w, readPage{Value: []readRecord{readFixture("r2", 3, "reply"), readFixture("r1", 2, "reply")}, Next: "https://graph.microsoft.com/v1.0/teams/g/channels/c/messages/root/replies?$top=3&$skiptoken=next"})
	})
	o := readOpts()
	o.Replies = true
	o.Limit = 3
	o.Budget.MaxMessages = 2
	a, e := r.Read(context.Background(), []string{target.Ref()}, o)
	if e != nil || len(a.Messages) != 2 || a.Ranges[1].NextCursor == "" {
		t.Fatalf("missing reply continuation %+v %v", a, e)
	}
	o = readOpts()
	o.Cursor = a.Ranges[1].NextCursor
	b, e := r.Read(context.Background(), nil, o)
	if e != nil || len(b.Messages) != 2 || b.Ranges[0].State != "complete" {
		t.Fatalf("lost replies %+v %v", b, e)
	}
	for _, m := range b.Messages {
		if m.Target.Parent != "root" || m.ID == "r2" {
			t.Fatalf("wrong reply locator %+v", m.Target)
		}
	}
}

func TestTeamsReadConcurrentWindows(t *testing.T) {
	arrived, release := make(chan string, 2), make(chan struct{})
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/messages/3") {
			writeReadJSON(w, readFixture("3", 3, "anchor"))
			return
		}
		arrived <- req.URL.Query().Get("$filter")
		select {
		case <-release:
		case <-req.Context().Done():
			return
		}
		if req.URL.Query().Get("$filter") != "" {
			writeReadJSON(w, readPage{Value: []readRecord{readFixture("2", 2, "older")}})
		} else {
			writeReadJSON(w, readPage{Value: []readRecord{readFixture("3", 3, "anchor"), readFixture("2", 2, "older")}})
		}
	})
	o := readOpts()
	o.Before, o.Latest = 1, 2
	type result struct {
		out ReadResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := r.Read(context.Background(), []string{chatTarget("3").Ref()}, o)
		done <- result{out, err}
	}()
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("independent windows did not overlap")
		}
	}
	close(release)
	got := <-done
	if got.err != nil || len(got.out.Messages) != 2 || got.out.Usage.Requests != 3 || len(got.out.Ranges) != 3 || got.out.Ranges[1].Kind != "before" || got.out.Ranges[2].Kind != "latest" {
		t.Fatalf("non-deterministic plan %+v %v", got.out, got.err)
	}
}

func TestTeamsReadBytesDeadlineAndPaginationBounds(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		var calls atomic.Int32
		r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
			calls.Add(1)
			writeReadJSON(w, readFixture("1", 1, strings.Repeat("x", 4096)))
		})
		o := readOpts()
		o.Budget.MaxBytes = 1024
		got, e := r.Read(context.Background(), []string{chatTarget("1").Ref()}, o)
		if e == nil || got.Usage.Bytes > 1024 || got.Usage.Requests != 1 || calls.Load() != 1 || !strings.Contains(got.Ranges[0].Error, "budget") {
			t.Fatalf("unbounded IO %+v %v", got, e)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) { <-req.Context().Done() })
		o := readOpts()
		o.Budget.Timeout = 30 * time.Millisecond
		got, e := r.Read(context.Background(), []string{chatTarget("1").Ref()}, o)
		if e == nil || !strings.Contains(got.Ranges[0].Error, "deadline") {
			t.Fatalf("lost deadline %+v %v", got, e)
		}
	})
	for _, query := range []string{"$orderby=createdDateTime+asc", "$filter=createdDateTime+gt+2026-11-01T00%3A00%3A01Z", "$expand=replies", "$top=51", "$skip=-1", "$top=2&$top=3"} {
		t.Run(query, func(t *testing.T) {
			r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) { t.Fatal("invalid continuation performed IO") })
			o := readOpts()
			o.Cursor = (readCursor{Target: chatTarget("1"), Kind: "latest", Remaining: 3, Next: "https://graph.microsoft.com/v1.0/chats/19:test@thread.v2/messages?" + query}).encode()
			if _, e := r.Read(context.Background(), nil, o); e == nil {
				t.Fatal("accepted altered pagination")
			}
		})
	}
}

func TestTeamsReadBatchChunksAndMissingItem(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			var batches, direct atomic.Int32
			r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
				if req.Method == "GET" {
					direct.Add(1)
					writeReadJSON(w, readFixture("1", 1, "fallback"))
					return
				}
				batches.Add(1)
				var in struct {
					Requests []struct{ ID, URL, Method string }
				}
				if json.NewDecoder(req.Body).Decode(&in) != nil || len(in.Requests) > 20 {
					t.Fatal("invalid batch")
				}
				out := []any{}
				for i := len(in.Requests) - 1; i >= 0; i-- {
					item := in.Requests[i]
					if missing && strings.HasSuffix(item.URL, "/1") {
						continue
					}
					id := item.URL[strings.LastIndex(item.URL, "/")+1:]
					if item.Method != "GET" {
						t.Error("mutating batch")
					}
					out = append(out, map[string]any{"id": item.ID, "status": 200, "body": readFixture(id, 1, "body")})
				}
				writeReadJSON(w, map[string]any{"responses": out})
			})
			ts := []string{}
			for i := 1; i <= 25; i++ {
				ts = append(ts, chatTarget(fmt.Sprint(i)).Ref())
			}
			o := readOpts()
			o.Budget.MaxRequests = 26
			got, e := r.Read(context.Background(), ts, o)
			want := 0
			if missing {
				want = 1
			}
			if e != nil || len(got.Messages) != 25 || batches.Load() != 2 || direct.Load() != int32(want) || got.Usage.Requests != 25+want {
				t.Fatalf("batch results %+v %v batches %d direct %d", got, e, batches.Load(), direct.Load())
			}
		})
	}
}

func TestTeamsSearchSummaryAndPageContinuation(t *testing.T) {
	r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
		var in struct{ Requests []struct{ From, Size int } }
		json.NewDecoder(req.Body).Decode(&in)
		q := in.Requests[0]
		hits := []any{}
		for i := q.From; i < min(5, q.From+q.Size); i++ {
			hits = append(hits, map[string]any{"summary": "一二三四", "resource": map[string]any{"id": fmt.Sprint(i + 1), "chatId": "c"}})
		}
		writeReadJSON(w, map[string]any{"value": []any{map[string]any{"hitsContainers": []any{map[string]any{"hits": hits, "moreResultsAvailable": q.From+len(hits) < 5}}}}})
	})
	o := SearchOptions{Limit: 3, Budget: DefaultReadBudget()}
	o.Budget.MaxChars = 2
	a, e := r.Search(context.Background(), "term", o)
	if e != nil || len(a.Hits) != 1 || !a.Hits[0].SummaryTruncated || a.Hits[0].Summary != "一二" || a.State != "truncated" {
		t.Fatalf("bad summary %+v %v", a, e)
	}
	o = SearchOptions{Limit: 20, Cursor: a.NextCursor, Budget: DefaultReadBudget()}
	b, e := r.Search(context.Background(), "", o)
	if e != nil || len(b.Hits) != 2 || b.State != "more" {
		t.Fatalf("lost partial page %+v %v", b, e)
	}
	o.Cursor = b.NextCursor
	c, e := r.Search(context.Background(), "", o)
	if e != nil || len(c.Hits) != 2 || c.State != "complete" {
		t.Fatalf("bad final page %+v %v", c, e)
	}
	seen := map[string]bool{}
	for _, pg := range []SearchResult{a, b, c} {
		for _, hit := range pg.Hits {
			if seen[hit.Ref] {
				t.Fatal("duplicated search candidate")
			}
			seen[hit.Ref] = true
		}
	}
}

func TestTeamsFetchHostedDiscoveryAndCompletedResume(t *testing.T) {
	for _, mode := range []string{"discovery", "complete", "denied"} {
		t.Run(mode, func(t *testing.T) {
			var transfers atomic.Int32
			r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
				switch {
				case strings.HasSuffix(req.URL.Path, "/hostedContents"):
					if mode == "denied" {
						w.WriteHeader(403)
						return
					}
					writeReadJSON(w, map[string]any{"value": []any{map[string]string{"id": "img"}}})
				case strings.HasSuffix(req.URL.Path, "/$value"):
					transfers.Add(1)
					if req.Header.Get("Range") != "" {
						w.WriteHeader(416)
						return
					}
					fmt.Fprint(w, "image")
				default:
					writeReadJSON(w, readFixture("1", 1, `<img src="opaque">`))
				}
			})
			got, e := r.Read(context.Background(), []string{chatTarget("1").Ref()}, readOpts())
			if e != nil || len(got.Messages) != 1 || len(got.Messages[0].Attachments) != 1 {
				t.Fatalf("lost body %+v %v", got, e)
			}
			at := got.Messages[0].Attachments[0]
			if mode == "denied" {
				if at.State != "unresolved" || !strings.Contains(at.Error, "403") {
					t.Fatal("lost independent file error")
				}
				return
			}
			dst := filepath.Join(t.TempDir(), "image")
			if mode == "complete" {
				stage := dst + ".cxp-download"
				os.Mkdir(stage, 0700)
				state, _ := json.Marshal(fetchState{at.Ref, "etag-1|2026-11-01T00:00:01Z"})
				os.WriteFile(filepath.Join(stage, "state.json"), state, 0600)
				os.WriteFile(filepath.Join(stage, "content.part"), []byte("image"), 0600)
			}
			f, e := r.Fetch(context.Background(), at.Ref, FetchOptions{Output: dst, MaxBytes: 100, Timeout: time.Minute})
			body, _ := os.ReadFile(dst)
			want := int32(1)
			if mode == "complete" {
				want = 2
			}
			if e != nil || f.Bytes != 5 || string(body) != "image" || transfers.Load() != want {
				t.Fatalf("hosted resume %+v %v transfers %d", f, e, transfers.Load())
			}
		})
	}
}
