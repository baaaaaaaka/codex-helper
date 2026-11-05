package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Owner ruling 2026-11-05, "stopped everywhere until started": `cxp teams
// stop` records the stop in the kill-switch file and says how to undo it,
// `cxp teams start` removes it; both are idempotent. While stopped, status
// says so first and `service start` / `service restart` refuse. A file made
// by hand is described by its path.
func TestTeamsStopAndStartAreIdempotentAndRecorded(t *testing.T) {
	state := t.TempDir()
	t.Setenv(appdirs.EnvStateDir, state)
	ctx := context.Background()
	anchor, _, err := planTeamsCoreStore(state)
	if err != nil {
		t.Fatal(err)
	}
	disabled := corestore.DisabledPath(filepath.Dir(anchor))
	host, _ := teamsHostname()

	var out bytes.Buffer
	if err := runTeamsStart(ctx, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.HasPrefix(got, "The Teams helper was not stopped; nothing to remove.\nNo Teams service is installed on this machine.") {
		t.Fatalf("start while not stopped:\n%s", got)
	}

	out.Reset()
	if err := runTeamsStop(ctx, &out); err != nil {
		t.Fatal(err)
	}
	want := "Stopping the Teams helper on all machines (created " + disabled + ").\n" +
		"No helper was running.\n" +
		"Helpers standing by and services on every machine stay idle and do not read Teams. Messages sent meanwhile are read when the helper starts again.\n" +
		"To start it again: cxp teams start\n"
	if got := out.String(); got != want {
		t.Fatalf("stop:\n%s\nwant:\n%s", got, want)
	}
	rec, ok, err := corestore.ReadStop(filepath.Dir(anchor))
	if err != nil || !ok || rec.Source != corestore.StopByCLI || rec.Host != host || rec.At.IsZero() {
		t.Fatalf("stop record %+v ok=%v err=%v", rec, ok, err)
	}
	line := teamsStoppedLine()
	if !strings.HasPrefix(line, "Stopped on all machines by `cxp teams stop` ("+host+", ") || !strings.HasSuffix(line, "). Start again with `cxp teams start`.") {
		t.Fatalf("stopped line %q", line)
	}
	out.Reset()
	_ = printTeamsCoreStatus(&out, rec.At) // only its first line matters here
	if first, _, _ := strings.Cut(out.String(), "\n"); first != line {
		t.Fatalf("status first line %q, want %q", first, line)
	}
	for _, action := range []string{"started", "restarted"} {
		err := errTeamsStoppedEverywhere(action)
		if err == nil || err.Error() != line+"\nThe Teams service was not "+action+": it would only stand by until then" {
			t.Fatalf("service %s while stopped: %v", action, err)
		}
	}

	out.Reset()
	if err := runTeamsStop(ctx, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "Already stopped; nothing changed.\n"+line+"\n" {
		t.Fatalf("second stop:\n%s", got)
	}

	out.Reset()
	if err := runTeamsStart(ctx, &out); err != nil {
		t.Fatal(err)
	}
	was := strings.TrimSuffix(line, " Start again with `cxp teams start`.")
	was = strings.TrimSuffix(was, ".")
	if got := out.String(); !strings.HasPrefix(got, "Started the Teams helper again on all machines (it was: "+was+"; removed "+disabled+").\n") {
		t.Fatalf("start:\n%s", got)
	}
	if _, err := os.Stat(disabled); !os.IsNotExist(err) {
		t.Fatalf("the kill-switch file is still there: %v", err)
	}
	if teamsStoppedLine() != "" || errTeamsStoppedEverywhere("started") != nil {
		t.Fatal("still reported stopped after start")
	}

	// Made by hand: still stopped, described by its path.
	if err := os.WriteFile(disabled, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, want := teamsStoppedLine(), "Stopped on all machines: the kill-switch file "+disabled+" exists. Start again with `cxp teams start`."; got != want {
		t.Fatalf("hand-made stopped line %q, want %q", got, want)
	}
}
