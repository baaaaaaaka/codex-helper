package entry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// These response views describe the public CLI JSON contract independently
// of product types. E2E tests reach the product only through its executable.
type teamsReadCLIResponse struct {
	Version  int `json:"version"`
	Messages []struct {
		Text        string `json:"text"`
		Attachments []struct {
			Ref string `json:"ref"`
		} `json:"attachments"`
	} `json:"messages"`
	Ranges []struct {
		NextCursor string `json:"next_cursor"`
	} `json:"ranges"`
	Usage struct {
		Requests int `json:"requests"`
	} `json:"usage"`
}
type teamsSearchCLIResponse struct {
	Version int `json:"version"`
	Hits    []struct {
		Ref string `json:"ref"`
	} `json:"hits"`
	NextCursor string `json:"next_cursor"`
}

func TestTeamsReadSearchFetchWithoutHelper(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TEAMS-READ", harness.Options{NoStart: true, Title: "read, search and fetch through the production CLI without acquiring helper state", Budget: 90 * time.Second, Golden: harness.GoldenOptions{Skip: "assert exact structured read results, file bytes and read-only network operations; no helper rendering is involved"}})
	defer s.Finish()
	chat := s.Graph.SeedChat("19:read-test@thread.v2", "Existing discussion")
	first, e := s.Graph.PostHTMLAsUser(chat.ID, `<p>needle <a href="https://example.com/definition">任务说明</a></p>`)
	if e != nil {
		t.Fatal(e)
	}
	file, e := s.Graph.PostFileAsUser(chat.ID, "needle file", "a.txt", []byte("exact file bytes"))
	if e != nil {
		t.Fatal(e)
	}
	image, e := s.Graph.PostImageAsUser(chat.ID, "needle image", []byte("exact image bytes"))
	if e != nil {
		t.Fatal(e)
	}
	link := func(id string) string {
		return "https://teams.microsoft.com/l/message/" + chat.ID + "/" + id + "?context=%7B%22contextType%22%3A%22chat%22%7D"
	}
	run := func(args ...string) harness.CLIResult {
		return s.RunCLIAs(20*time.Second, harness.CLIAs{Poll: true}, args...)
	}
	read := func(args ...string) teamsReadCLIResponse {
		t.Helper()
		res := run(append([]string{"teams", "read"}, args...)...)
		if res.Code != 0 {
			t.Fatalf("read failed: %s", res.Combined)
		}
		var out teamsReadCLIResponse
		if e := json.Unmarshal([]byte(res.Stdout), &out); e != nil || out.Version != 1 {
			t.Fatalf("invalid read JSON: %v %s", e, res.Combined)
		}
		return out
	}
	direct := read(link(first.ID), "--json")
	s.Check("named link is visible in the linked message", len(direct.Messages) == 1 && strings.Contains(direct.Messages[0].Text, "任务说明: https://example.com/definition"), "one message with its named URL", fmt.Sprint(len(direct.Messages)))
	windows := read(link(file.ID), "--before", "1", "--latest", "3", "--json")
	s.Check("explicit overlapping windows share messages", len(windows.Messages) == 3 && len(windows.Ranges) == 3, "3 unique messages in 3 explicit ranges", fmt.Sprintf("%d messages, %d ranges", len(windows.Messages), len(windows.Ranges)))
	paged := read("https://teams.microsoft.com/l/chat/"+chat.ID+"/0", "--latest", "3", "--max-messages", "1", "--json")
	if len(paged.Ranges) != 1 || paged.Ranges[0].NextCursor == "" {
		t.Fatalf("no range cursor %+v", paged)
	}
	rest := read("--cursor", paged.Ranges[0].NextCursor, "--json")
	s.Check("page budget resumes without losing messages", len(paged.Messages) == 1 && len(rest.Messages) == 2, "1 then 2 messages", fmt.Sprintf("%d then %d", len(paged.Messages), len(rest.Messages)))
	found := run("teams", "search", "needle", "--limit", "2", "--json")
	var search teamsSearchCLIResponse
	if found.Code != 0 || json.Unmarshal([]byte(found.Stdout), &search) != nil || search.Version != 1 || len(search.Hits) != 2 {
		t.Fatalf("search failed %s", found.Combined)
	}
	precise := read(search.Hits[0].Ref, search.Hits[1].Ref, "--json")
	s.Check("search candidates return to original messages in one batch", len(precise.Messages) == 2 && precise.Usage.Requests == 2, "2 messages; 2 logical requests", fmt.Sprintf("%d messages; %d requests", len(precise.Messages), precise.Usage.Requests))
	more := run("teams", "search", "--cursor", search.NextCursor, "--json")
	var last teamsSearchCLIResponse
	if more.Code != 0 || json.Unmarshal([]byte(more.Stdout), &last) != nil || last.Version != 1 || len(last.Hits) != 1 {
		t.Fatalf("search paging failed %s", more.Combined)
	}
	for _, item := range []struct{ id, body string }{{file.ID, "exact file bytes"}, {image.ID, "exact image bytes"}} {
		original := read(link(item.id), "--json")
		if len(original.Messages) != 1 || len(original.Messages[0].Attachments) != 1 {
			t.Fatalf("missing attachment %+v", original)
		}
		dst := filepath.Join(s.Env.Home, item.id+".download")
		res := run("teams", "fetch", original.Messages[0].Attachments[0].Ref, "--output", dst, "--json")
		got, e := os.ReadFile(dst)
		s.Check("download exact bytes "+item.id, res.Code == 0 && e == nil && bytes.Equal(got, []byte(item.body)), "successful exact bytes", fmt.Sprintf("exit %d; bytes %d; %v", res.Code, len(got), e))
		if res.Code != 0 {
			t.Fatal(res.Combined)
		}
		again := run("teams", "fetch", original.Messages[0].Attachments[0].Ref, "--output", dst, "--json")
		if again.Code == 0 {
			t.Fatal("fetch overwrote an existing target")
		}
	}
	var mutations []string
	for _, req := range s.Graph.Requests(fakegraph.RequestFilter{}) {
		if req.Host == "graph.microsoft.com" && req.Method != "GET" && req.Route != "POST /search/query" && req.Route != "POST /$batch" {
			mutations = append(mutations, req.Route)
		}
	}
	s.Check("CLI reads never send Teams mutations", len(mutations) == 0, "no message/chat/file mutations", fmt.Sprint(mutations))
	s.Check("no helper process needed", !s.SUT.Running(), "helper never started", fmt.Sprint(s.SUT.Running()))
}
