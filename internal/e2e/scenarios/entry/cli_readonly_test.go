package entry

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// The diagnostic commands only look (F-SET-4, F-CLI-2, R-46).
//
// History: status and doctor used to open the store read-write: they ran
// migrations, took the lock the service needed or left WAL files behind
// ("teams status hung while the service ran", U-I14; 6a3c0e1 made the
// status open read-only), and "setup" once logged in and created chats.
//
// With the helper running: `teams setup`, `teams status` and `teams
// doctor` send no Graph request at all, `teams doctor --live` only reads
// (/me and the control chat), and the helper keeps answering meanwhile.
// With the helper stopped: `status`, `doctor` and `doctor --live` change no
// byte under the helper's state directory and create no file there.
func TestDiagnosticsAreReadOnly(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "CLI-READONLY", harness.Options{
		Guarantees: []string{"F-SET-4", "F-CLI-2", "R-46"},
		Title:      "`teams setup`, `status`, `doctor` and `doctor --live` send nothing, write nothing and never block the running helper",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "readonly")
	tok := s.Token("before")
	steps.MustPost(s, work.ID, "Something to store "+tok)
	steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout)

	cliRequests := func(since int64) []fakegraph.Request {
		var out []fakegraph.Request
		for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: since}) {
			if strings.HasPrefix(r.Instance, "cxp-cli") {
				out = append(out, r)
			}
		}
		return out
	}
	describe := func(rs []fakegraph.Request) string {
		var parts []string
		for _, r := range rs {
			parts = append(parts, r.Method+" "+r.Route)
		}
		return fmt.Sprint(parts)
	}
	run := func(args ...string) harness.CLIResult {
		// Their output names paths and times: not part of the golden.
		return s.RunCLIAs(30*time.Second, harness.CLIAs{Poll: true}, args...)
	}

	// While the helper runs.
	for _, args := range [][]string{{"teams", "setup"}, {"teams", "status"}, {"teams", "doctor"}} {
		from := s.Graph.LastSeq()
		res := run(args...)
		rs := cliRequests(from)
		s.Check(fmt.Sprintf("`cxp %s` succeeds and sends no Graph request", strings.Join(args, " ")), res.Code == 0 && len(rs) == 0,
			"exit 0, no request", fmt.Sprintf("exit %d, %s: %.200q", res.Code, describe(rs), res.Combined))
	}
	from := s.Graph.LastSeq()
	res := run("teams", "doctor", "--live")
	rs := cliRequests(from)
	var writes []string
	reads := map[string]bool{}
	for _, r := range rs {
		if r.Host == "graph.microsoft.com" {
			if r.Method != "GET" {
				writes = append(writes, r.Method+" "+r.Route)
			} else {
				reads[r.Route] = true
			}
		}
	}
	s.Check("`cxp teams doctor --live` reads the account and the control chat", res.Code == 0 && reads["GET /me"] && reads["GET /chats/{chat}/messages"],
		"exit 0; GET /me and GET /chats/{chat}/messages", fmt.Sprintf("exit %d, %s: %.200q", res.Code, describe(rs), res.Combined))
	s.Check("`cxp teams doctor --live` writes nothing to Teams", len(writes) == 0, "no Graph write", fmt.Sprint(writes))
	tok2 := s.Token("during")
	steps.MustPost(s, work.ID, "Still answering? "+tok2)
	_, ok := steps.WaitAnswered(s, work.ID, tok2, steps.AnswerTimeout)
	s.Check("the helper keeps answering", ok, "answered", fmt.Sprint(ok))

	// With the helper stopped.
	s.StopSUT()
	before := treeHashes(t, s.Env.StateDir)
	for _, args := range [][]string{{"teams", "status"}, {"teams", "doctor"}, {"teams", "doctor", "--live"}} {
		res := run(args...)
		after := treeHashes(t, s.Env.StateDir)
		diff := hashDiff(before, after)
		s.Check(fmt.Sprintf("`cxp %s` on a stopped helper changes nothing under the state directory", strings.Join(args, " ")),
			res.Code == 0 && len(diff) == 0, "exit 0, no file changed or created", fmt.Sprintf("exit %d, %v", res.Code, diff))
		before = after
	}
}

func treeHashes(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			out[path] = "unreadable: " + err.Error()
			return nil
		}
		out[path] = fmt.Sprintf("%x", sha256.Sum256(data))
		return nil
	})
	return out
}

func hashDiff(a, b map[string]string) []string {
	var diff []string
	for p, h := range b {
		switch old, ok := a[p]; {
		case !ok:
			diff = append(diff, "created "+filepath.Base(p))
		case old != h:
			diff = append(diff, "changed "+filepath.Base(p))
		}
	}
	for p := range a {
		if _, ok := b[p]; !ok {
			diff = append(diff, "removed "+filepath.Base(p))
		}
	}
	sort.Strings(diff)
	return diff
}
