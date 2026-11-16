package harness

import (
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestIdleInvariantRequiresCompletedCheck(t *testing.T) {
	for _, tc := range []struct {
		name, waive          string
		running, wantFailure bool
	}{
		{name: "unfinished normal scenario", running: true, wantFailure: true},
		{name: "explicit active-work waiver", running: true, waive: "exercise shutdown while a turn is blocked"},
		{name: "helper deliberately stopped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Scenario{T: t, budget: time.Now().Add(-time.Second)}
			if tc.waive != "" {
				s.Opts.Invariants.Waive = map[string]string{InvIdleQuiet: tc.waive}
			}
			res := &invResults{fail: map[string][]string{}, note: map[string]string{}}
			s.checkIdle(res, tc.running)
			if got := len(res.fail[InvIdleQuiet]) > 0; got != tc.wantFailure {
				t.Fatalf("failure=%v, results=%+v", got, res)
			}
			if tc.wantFailure {
				if res.note[InvIdleQuiet] != "" {
					t.Fatal("unfinished work was downgraded to an informational skip")
				}
			} else {
				summary := s.reportInvariants(res)
				want := "not checked:"
				if tc.waive != "" {
					want = "waived:"
				}
				if !strings.HasPrefix(summary[InvIdleQuiet], want) {
					t.Fatal(summary)
				}
			}
		})
	}
}

func TestIdleInvariantObservesQuiescentHelper(t *testing.T) {
	g := fakegraph.New(fakegraph.Options{})
	defer g.Close()
	s := &Scenario{T: t, Graph: g, Codex: NewFakeCodex(), budget: time.Now().Add(10 * time.Second)}
	res := &invResults{fail: map[string][]string{}, note: map[string]string{}}
	s.checkIdle(res, true)
	if got := s.reportInvariants(res)[InvIdleQuiet]; got != "ok" {
		t.Fatal(got)
	}
}
