package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
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

// Bypass the Graph test seam deliberately: this regression must exercise the
// production connection lease and a real child-process lifetime. The SSH double
// serves readiness only; absent read credentials prevent any Graph request.
func TestTeamsReadPrivateProxyProcessCleanup(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TEAMS-READ-PROXY", harness.Options{NoStart: true, Budget: 60 * time.Second, Golden: harness.GoldenOptions{Skip: "assert private SSH child cleanup and no helper acquisition"}})
	defer s.Finish()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required for the SSH process fixture")
	}
	bin := filepath.Join(s.Env.Home, "bin")
	if err := os.MkdirAll(bin, 0700); err != nil {
		t.Fatal(err)
	}
	script := "#!" + python + `\nimport json, os, socket, sys, time
if '-G' in sys.argv:
    print('hostname example.invalid\nport 22')
    sys.exit(0)
port = int(sys.argv[sys.argv.index('-D') + 1].split(':')[-1])
with open(os.environ['SSH_TEST_RECORD'], 'w') as f:
    json.dump({'pid': os.getpid(), 'port': port}, f)
if os.environ['SSH_TEST_BLOCK'] == '1':
    time.sleep(60)
s = socket.socket()
s.bind(('127.0.0.1', port))
s.listen()
while True:
    c, _ = s.accept()
    if c.recv(3):
        c.sendall(bytes([5, 0]))
    c.close()
`
	// The shebang needs a real newline, not a literal escape in the raw script.
	script = strings.Replace(script, `\n`, "\n", 1)
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(s.Env.Home, "proxy.json")
	// Public legacy configuration input; no product config types in black-box E2E.
	if err := os.WriteFile(config, []byte(`{"version":1,"minReader":1,"proxyEnabled":true,"profiles":[{"id":"read-proxy","name":"read-proxy","host":"example.invalid","port":22,"user":"alice"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, block := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked=%v", block), func(t *testing.T) {
			record := filepath.Join(s.Env.Home, fmt.Sprintf("ssh-%v.json", block))
			vars := map[string]string{}
			for k, v := range s.Env.Vars {
				vars[k] = v
			}
			vars["PATH"] = bin + string(os.PathListSeparator) + os.Getenv("PATH")
			vars["SSH_TEST_RECORD"] = record
			vars["SSH_TEST_BLOCK"] = "0"
			budget := "5s"
			if block {
				vars["SSH_TEST_BLOCK"] = "1"
				budget = "2s"
			}
			vars["CODEX_HELPER_TEAMS_READ_TOKEN_CACHE"] = filepath.Join(s.Env.Home, "no-read-token.sqlite")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, harness.CLIBinary, "--config", config, "teams", "read", "https://teams.microsoft.com/l/message/chat/1", "--timeout", budget, "--json")
			cmd.Dir = s.Env.Home
			for k, v := range vars {
				cmd.Env = append(cmd.Env, k+"="+v)
			}
			out, runErr := cmd.CombinedOutput()
			if runErr == nil || ctx.Err() != nil {
				t.Fatalf("expected controlled read failure, got %v, context %v: %s", runErr, ctx.Err(), out)
			}
			data, err := os.ReadFile(record)
			if err != nil {
				t.Fatalf("private SSH never started: %v; %s", err, out)
			}
			var child struct {
				PID  int `json:"pid"`
				Port int `json:"port"`
			}
			if err := json.Unmarshal(data, &child); err != nil || child.PID <= 0 {
				t.Fatalf("invalid child record: %s", data)
			}
			proc, err := os.FindProcess(child.PID)
			if err == nil && proc.Signal(syscall.Signal(0)) == nil {
				t.Fatalf("private SSH child %d survived command exit", child.PID)
			}
			// A closed Python socket can leave TIME_WAIT entries. A failed
			// rebind would not prove a live listener; check acceptability instead.
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", child.Port), time.Second)
			if err == nil {
				conn.Close()
				t.Fatal("private SOCKS listener still accepts connections")
			}
			if block && !strings.Contains(string(out), "context deadline exceeded") {
				t.Fatalf("lost deadline cause: %s", out)
			}
		})
	}
	s.Check("helper was never started", !s.SUT.Running(), "no helper process", fmt.Sprint(s.SUT.Running()))
}
