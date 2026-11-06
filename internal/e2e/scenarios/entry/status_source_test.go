package entry

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// STATUS-SOURCE (F-CLI-1): while the helper runs, `cxp teams status` and
// `cxp teams control` (the lookup the service bootstrap uses for the
// control chat link) show the helper's current state in full: it follows
// a new work chat within seconds. With the helper stopped they read the
// store itself and show the same state.
func TestStatusWhileTheHelperRunsAndWithout(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "STATUS-SOURCE", harness.Options{
		Guarantees: []string{"F-CLI-1"},
		Title:      "`cxp teams status` and `cxp teams control` while the helper runs (its published state, kept current) and after it stops (the store itself)",
		Budget:     240 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)

	run := func(args ...string) harness.CLIResult {
		return s.RunCLIAs(20*time.Second, harness.CLIAs{Poll: true}, args...)
	}
	details := func(out string) string {
		var lines []string
		for _, prefix := range []string{"Store details:", "Store unreadable", "Running version:", "Helper update:", "Sessions:", "Requests:", "Unsent messages:", "Workflow cards:"} {
			lines = append(lines, statusLine(out, prefix))
		}
		return strings.Join(lines, " | ")
	}

	var out string
	s.Poll(30*time.Second, func() bool {
		out = run("teams", "status").Stdout
		return statusLine(out, "Sessions:") == "Sessions: 0 (0 active, 0 parked)"
	})
	s.Check("status while the helper runs shows its state in full",
		strings.Contains(statusLine(out, "Owner:"), "(live)") && statusLine(out, "Store details:") == "" &&
			statusLine(out, "Running version:") != "" && statusLine(out, "Running version:") != "Running version: unknown" &&
			statusLine(out, "Helper update:") == "Helper update: none pending" &&
			statusLine(out, "Sessions:") == "Sessions: 0 (0 active, 0 parked)" &&
			statusLine(out, "Requests:") == "Requests: 0 queued, 0 running" &&
			statusLine(out, "Unsent messages:") == "Unsent messages: 0 queued, 0 sending, 0 unconfirmed" &&
			statusLine(out, "Workflow cards:") != "",
		"Owner … (live); no Store details note; Running version, Helper update, Sessions 0, Requests, Unsent messages, Workflow cards", details(out), out)

	// The helper publishes its state after commits, a read behind the
	// store; the control chat may have been recorded just now.
	var ctl harness.CLIResult
	var link string
	s.Poll(15*time.Second, func() bool {
		ctl = run("teams", "control")
		link = statusLine(ctl.Stdout, "Link: ")
		return link != ""
	})
	s.Check("`teams control` while the helper runs prints the control chat link",
		ctl.Code == 0 && statusLine(ctl.Stdout, "Title: ") == "Title: "+control.Topic &&
			(strings.Contains(link, control.ID) || strings.Contains(link, url.QueryEscape(control.ID))),
		"Title: "+control.Topic+"; Link: …"+control.ID+"…", fmt.Sprintf("code=%d %q %q", ctl.Code, statusLine(ctl.Stdout, "Title: "), link), ctl.Combined)

	chat, _ := steps.NewWorkChat(s, control, "source")
	s.Check("a new work chat appears", chat.ID != "", "a work chat", chat.ID)
	start := time.Now()
	ok := s.Poll(30*time.Second, func() bool {
		out = run("teams", "status").Stdout
		return statusLine(out, "Sessions:") == "Sessions: 1 (1 active, 0 parked)"
	})
	s.Check("status follows the helper's state within seconds", ok && time.Since(start) < 15*time.Second,
		"Sessions: 1 (1 active, 0 parked) within 15s", fmt.Sprintf("%q after %s", statusLine(out, "Sessions:"), time.Since(start).Round(time.Second)), out)
	// The chat's "Work chat created" may still be queued; the helper sends
	// it before it stops, so compare against the state once it is sent.
	const drained = "Unsent messages: 0 queued, 0 sending, 0 unconfirmed"
	ok = s.Poll(15*time.Second, func() bool {
		out = run("teams", "status").Stdout
		return statusLine(out, "Unsent messages:") == drained
	})
	s.Check("the helper sends what it queued", ok, drained, statusLine(out, "Unsent messages:"), out)
	running := details(out)

	s.StopSUT()
	out = run("teams", "status").Stdout
	s.Check("status with the helper stopped reads the store: the same state, the owner released",
		strings.HasPrefix(statusLine(out, "Owner:"), "Owner: none (released by") && details(out) == running,
		running, details(out), out)
	ctl = run("teams", "control")
	s.Check("`teams control` with the helper stopped prints the same link",
		ctl.Code == 0 && statusLine(ctl.Stdout, "Link: ") == link, link, statusLine(ctl.Stdout, "Link: "), ctl.Combined)
}
