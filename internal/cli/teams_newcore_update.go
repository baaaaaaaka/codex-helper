package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

// Updates of the new core (SPEC R-16): the engine decides when (drain,
// restart, one notice); these adapters reuse the old core's release check,
// install and Codex upgrade code unchanged.

// teamsNewCoreUpdater is the helper self-update: the GitHub release
// selection and the validated install of `teams run --auto-update`.
type teamsNewCoreUpdater struct {
	rel     teamsReleaseAutoUpdater
	version string
	log     io.Writer
}

func newTeamsNewCoreUpdater(repo string, prerelease bool, version string, log io.Writer) *teamsNewCoreUpdater {
	return &teamsNewCoreUpdater{rel: teamsReleaseAutoUpdater{repo: repo, includePrerelease: prerelease}, version: version, log: log}
}

func (u *teamsNewCoreUpdater) Check(ctx context.Context, prerelease, manual bool) (engine.UpdateOffer, error) {
	d, err := u.rel.Check(ctx, teams.HelperAutoUpdateCheck{InstalledVersion: u.version, Now: time.Now(),
		IncludePrerelease: prerelease, Manual: manual})
	if err != nil || d.Candidate == nil {
		return engine.UpdateOffer{}, err
	}
	tag := strings.TrimSpace(d.Candidate.TagName)
	if tag == "" && strings.TrimSpace(d.Candidate.Version) != "" {
		tag = "v" + strings.TrimPrefix(strings.TrimSpace(d.Candidate.Version), "v")
	}
	return engine.UpdateOffer{Tag: tag}, nil
}

// Install downloads, validates and installs the release at the stable
// install path. On Windows a locked executable is replaced by the deferred
// move the update package schedules; the engine's restart (exit 75) then
// starts the new binary, and the next process reports the version it runs.
func (u *teamsNewCoreUpdater) Install(ctx context.Context, o engine.UpdateOffer) error {
	res, err := u.rel.ApplyWithOptions(ctx, teams.HelperAutoUpdateCandidate{TagName: o.Tag}, teams.HelperAutoUpdateApplyOptions{})
	if err != nil {
		return err
	}
	if res.ActivationPending && u.log != nil {
		_, _ = fmt.Fprintf(u.log, "helper update %s: %s\n", o.Tag, res.ActivationReason)
	}
	return nil
}

// teamsNewCoreCodexUpgrader upgrades the discovered Codex CLI from its
// install source (`codex update now`, or Codex rejecting a request as too
// old). It is off with an explicit --codex-path, as in the old core.
type teamsNewCoreCodexUpgrader struct {
	root *rootOptions
	log  io.Writer
}

func (c teamsNewCoreCodexUpgrader) Upgrade(ctx context.Context) error {
	if _, err := runTeamsCodexUpgradeFromBridge(ctx, c.root, c.log, ""); err != nil {
		return err
	}
	invalidateTeamsOfficialModelCache()
	return nil
}
