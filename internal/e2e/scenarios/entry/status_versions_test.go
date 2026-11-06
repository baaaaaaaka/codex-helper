package entry

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

// statusLine is the line of `cxp teams status` output starting with
// prefix ("" when there is none).
func statusLine(out, prefix string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, prefix) {
			return strings.TrimSpace(l)
		}
	}
	return ""
}

var lastPollRE = regexp.MustCompile(`^Last poll: \S+ \((\d+)s ago\)$`)

// STATUS-VERSIONS (F-CLI-1, R-17): `cxp teams status` reports the version
// the helper actually runs, a newer installed version as pending until
// the helper restarts, and when the helper last read Teams.
func TestStatusRunningInstalledAndLastPoll(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "STATUS-VERSIONS", harness.Options{
		Guarantees: []string{"F-CLI-1", "R-17"},
		Title:      "`cxp teams status`: running vs installed version (a newer install shown as pending until the restart) and the last poll",
		Budget:     240 * time.Second,
	})
	defer s.Finish()
	const newer = "v9.9.9-e2e"
	newBin, err := sut.VersionedCLI(newer)
	if err != nil {
		t.Fatal(err)
	}
	control := steps.ReadyControlChat(s)
	chat, _ := steps.NewWorkChat(s, control, "status")
	tok := s.Token("status")
	steps.MustPost(s, chat.ID, "Status task "+tok)
	_, ok := steps.WaitAnswered(s, chat.ID, tok, steps.AnswerTimeout)
	s.Check("the helper serves the owner", ok, "answered", fmt.Sprint(ok))

	status := func(bin string) string {
		return s.RunCLIAs(20*time.Second, harness.CLIAs{Poll: true, Binary: bin}, "teams", "status").Stdout
	}
	var out string
	s.Poll(30*time.Second, func() bool {
		out = status("")
		return lastPollRE.MatchString(statusLine(out, "Last poll:"))
	})
	installed := strings.TrimPrefix(statusLine(out, "Installed version: "), "Installed version: ")
	running := strings.TrimPrefix(statusLine(out, "Running version: "), "Running version: ")
	s.Check("status from the running build: installed and running versions agree, nothing pending",
		installed != "" && installed == running && statusLine(out, "Pending version:") == "",
		"Installed version == Running version, no Pending version", fmt.Sprintf("installed=%q running=%q pending=%q", installed, running, statusLine(out, "Pending version:")))
	m := lastPollRE.FindStringSubmatch(statusLine(out, "Last poll:"))
	s.Check("status shows a recent last poll", m != nil && len(m[1]) <= 2, "Last poll: <time> (<100s ago)", statusLine(out, "Last poll:"))

	out = status(newBin)
	wantPending := "Pending version: " + newer + " installed, not running yet (the running helper switches to it when it restarts)"
	s.Check("status from a newer install: the running version stays the old one, the newer one is pending",
		statusLine(out, "Installed version: ") == "Installed version: "+newer && statusLine(out, "Running version: ") == "Running version: "+running &&
			statusLine(out, "Pending version:") == wantPending,
		wantPending, fmt.Sprintf("%q / %q / %q", statusLine(out, "Installed version: "), statusLine(out, "Running version: "), statusLine(out, "Pending version:")))

	s.StopSUT()
	out = status(newBin)
	s.Check("with no helper running nothing is pending, and the last poll names the stopped helper",
		statusLine(out, "Pending version:") == "" && strings.Contains(statusLine(out, "Last poll:"), "which is not running now"),
		"no Pending version; Last poll: … which is not running now", fmt.Sprintf("%q / %q", statusLine(out, "Pending version:"), statusLine(out, "Last poll:")))
}
