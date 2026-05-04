package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/flock"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

type teamsReleaseAutoUpdater struct {
	repo string
}

func newTeamsReleaseAutoUpdater(repo string) teams.HelperAutoUpdater {
	return teamsReleaseAutoUpdater{repo: repo}
}

func (u teamsReleaseAutoUpdater) Check(ctx context.Context, check teams.HelperAutoUpdateCheck) (teams.HelperAutoUpdateDecision, error) {
	now := check.Now
	if now.IsZero() {
		now = time.Now()
	}
	releases, err := update.ListReleases(ctx, update.ReleaseListOptions{
		Repo:    u.repo,
		Timeout: 8 * time.Second,
	})
	if err != nil {
		return teams.HelperAutoUpdateDecision{
			NextCheckAt:  now.Add(update.DefaultAutoUpdateCheckInterval),
			BackoffUntil: now.Add(update.DefaultAutoUpdateCheckInterval),
			LastError:    err.Error(),
		}, err
	}
	selected := update.SelectAutoUpdateCandidate(releases, update.AutoUpdateSelectionOptions{
		InstalledVersion: check.InstalledVersion,
		Now:              now,
	})
	decision := teams.HelperAutoUpdateDecision{
		NextCheckAt: selected.NextCheckAt,
	}
	if selected.Candidate != nil {
		decision.Candidate = &teams.HelperAutoUpdateCandidate{
			TagName:     selected.Candidate.TagName,
			Version:     selected.Candidate.Version,
			Priority:    string(selected.Candidate.Priority),
			PublishedAt: selected.Candidate.PublishedAt,
			EligibleAt:  selected.Candidate.EligibleAt,
			Asset:       selected.Candidate.Asset,
		}
	}
	return decision, nil
}

func (u teamsReleaseAutoUpdater) Apply(ctx context.Context, candidate teams.HelperAutoUpdateCandidate) (teams.HelperAutoUpdateApplyResult, error) {
	installPath, err := update.ResolveInstallPath("")
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	lock := flock.New(installPath + ".auto-update.lock")
	ok, err := lock.TryLockContext(ctx, 100*time.Millisecond)
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	if !ok {
		return teams.HelperAutoUpdateApplyResult{}, fmt.Errorf("another helper auto-update is already updating %s", installPath)
	}
	defer func() { _ = lock.Unlock() }()
	res, err := performUpdate(ctx, update.UpdateOptions{
		Repo:           u.repo,
		Version:        candidate.TagName,
		InstallPath:    installPath,
		Timeout:        120 * time.Second,
		ValidateBinary: true,
	})
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	return teams.HelperAutoUpdateApplyResult{
		Version:         res.Version,
		RestartRequired: res.RestartRequired,
	}, nil
}
