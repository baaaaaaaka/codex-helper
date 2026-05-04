package cli

import (
	"context"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func TestTeamsReleaseAutoUpdaterApplyUsesExplicitSelectedTag(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	t.Cleanup(func() { performUpdate = prevPerform })
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.4"}, nil
	}
	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	res, err := updater.Apply(context.Background(), teams.HelperAutoUpdateCandidate{
		TagName:     "v1.2.4",
		Version:     "1.2.4",
		Priority:    "p0",
		PublishedAt: time.Now(),
		EligibleAt:  time.Now(),
	})
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if got.Repo != "owner/name" || got.Version != "v1.2.4" {
		t.Fatalf("PerformUpdate options = %#v, want explicit v1.2.4", got)
	}
	if got.Version == "latest" {
		t.Fatal("auto-updater must never pass latest to PerformUpdate")
	}
	if res.Version != "1.2.4" {
		t.Fatalf("result version = %q, want 1.2.4", res.Version)
	}
}
