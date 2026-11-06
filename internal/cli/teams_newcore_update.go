package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/selfupdate"
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
	d, err := u.rel.Check(ctx, selfupdate.Check{InstalledVersion: u.version, Now: time.Now(),
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
	res, err := u.rel.ApplyWithOptions(ctx, selfupdate.Candidate{TagName: o.Tag}, selfupdate.ApplyOptions{})
	if err != nil {
		return err
	}
	if res.ActivationPending && u.log != nil {
		_, _ = fmt.Fprintf(u.log, "helper update %s: %s\n", o.Tag, res.ActivationReason)
	}
	return nil
}

// teamsNewCoreCodexUpgrader upgrades the CXP-managed Codex CLI the
// app-servers run (`codex update now`, or Codex rejecting a request as too
// old). It is off with an explicit --codex-path, as in the old core.
//
// An upgrade that leaves the version unchanged cannot help: the install
// source has nothing newer. It is reported as a failure, so the engine
// posts why and does not restart (a restart would only replace the
// app-servers with the same CLI, and the next too-old rejection would
// upgrade and restart again).
type teamsNewCoreCodexUpgrader struct {
	root     *rootOptions
	bin      string // the Codex the app-servers run
	identity *execIdentity
	log      io.Writer
}

var upgradeTeamsCodexForNewCore = upgradeTeamsCodex

func (c teamsNewCoreCodexUpgrader) Upgrade(ctx context.Context) error {
	before := teamsCodexVersionForUpgrade(ctx, c.bin, c.identity)
	path, err := upgradeTeamsCodexForNewCore(ctx, c.root, c.log)
	if err != nil {
		return err
	}
	if after := teamsCodexVersionForUpgrade(ctx, path, c.identity); before != "" && after == before {
		return fmt.Errorf("Codex CLI is already %s, the newest version its install source offers; nothing changed, so the helper was not restarted", before)
	}
	return nil
}

// teamsCodexVersionForUpgrade is `codex --version` of path ("" when it
// cannot be read).
var teamsCodexVersionForUpgrade = func(ctx context.Context, path string, identity *execIdentity) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	pctx, cancel := context.WithTimeout(ctx, codexProbeTimeout)
	defer cancel()
	out, err := runCodexProbeCommand(pctx, path, []string{"--version"}, codexRuntimeEnvironment(os.Environ(), nil, identity), identity)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
