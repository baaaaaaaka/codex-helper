package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The large fixture exercises the whole in-memory planner and JSON size,
// without an index, message cache, file writes or hidden history scans.
func BenchmarkTeamsReadCombinedLargeFixture(b *testing.B) {
	messages := make([]readRecord, 101)
	for i := 1; i <= 100; i++ {
		m := readFixture(strconv.Itoa(i), 1, strings.Repeat(`<p>context and <a href="https://example.com/spec">任务说明</a></p>`, 8))
		m.Created = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second).Format(time.RFC3339)
		messages[i] = m
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/messages/50") {
			writeReadJSON(w, messages[50])
			return
		}
		page := readPage{}
		if req.URL.Query().Get("$filter") != "" {
			for i := 49; i >= 1; i-- {
				page.Value = append(page.Value, messages[i])
			}
		} else {
			for i := 100; i >= 51; i-- {
				page.Value = append(page.Value, messages[i])
			}
		}
		writeReadJSON(w, page)
	}))
	defer srv.Close()
	r, e := NewReader(Config{GraphURL: srv.URL + "/v1.0", Tokens: &staticTokens{tok: "read"}, TenantID: "tenant"})
	if e != nil {
		b.Fatal(e)
	}
	defer r.Close()
	o := readOpts()
	o.Before, o.Latest = 49, 50
	b.ReportAllocs()
	b.ResetTimer()
	var last ReadResult
	var output []byte
	for i := 0; i < b.N; i++ {
		last, e = r.Read(context.Background(), []string{chatTarget("50").Ref()}, o)
		if e != nil || len(last.Messages) != 100 || last.Usage.Requests != 3 {
			b.Fatal(fmt.Sprintf("unexpected bounded plan: messages=%d requests=%d err=%v", len(last.Messages), last.Usage.Requests, e))
		}
		output, e = json.Marshal(last)
		if e != nil || len(output) > 512<<10 {
			b.Fatal("unbounded output")
		}
	}
	b.ReportMetric(float64(last.Usage.Requests), "requests/op")
	b.ReportMetric(float64(last.Usage.Bytes), "response-B/op")
	b.ReportMetric(float64(len(output)), "output-B/op")
}
