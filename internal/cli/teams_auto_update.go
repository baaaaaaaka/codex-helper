package cli

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

type teamsReleaseAutoUpdater struct {
	repo              string
	includePrerelease bool
}

var teamsAutoUpdateResolveInstallPath = resolveManagedInstallPathForTeamsAutoUpdate
var teamsAutoUpdateListReleases = update.ListReleases
var teamsAutoUpdateFetchReleaseIndex = update.FetchReleaseIndex
var teamsAutoUpdateExecutable = func() (string, error) { return teamsServiceExecutable() }

func newTeamsReleaseAutoUpdater(repo string, includePrerelease bool) teams.HelperAutoUpdater {
	return teamsReleaseAutoUpdater{repo: repo, includePrerelease: includePrerelease}
}

func (u teamsReleaseAutoUpdater) Check(ctx context.Context, check teams.HelperAutoUpdateCheck) (teams.HelperAutoUpdateDecision, error) {
	now := check.Now
	if now.IsZero() {
		now = time.Now()
	}
	if check.Manual && !check.IncludePrerelease {
		return u.checkManualStable(ctx, check, now)
	}
	releases, err := u.indexedOrListedReleases(ctx)
	if err != nil {
		return teams.HelperAutoUpdateDecision{
			NextCheckAt:  now.Add(update.DefaultAutoUpdateCheckInterval),
			BackoffUntil: now.Add(update.DefaultAutoUpdateCheckInterval),
			LastError:    err.Error(),
		}, err
	}
	installedVersion := check.InstalledVersion
	if check.Manual && (strings.TrimSpace(installedVersion) == "" || strings.EqualFold(strings.TrimSpace(installedVersion), "dev")) {
		installedVersion = "0.0.0"
	}
	selected := update.SelectAutoUpdateCandidate(releases, update.AutoUpdateSelectionOptions{
		InstalledVersion:  installedVersion,
		Now:               now,
		IncludePrerelease: u.includePrerelease || check.IncludePrerelease,
		IgnorePriority:    check.Manual,
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

func (u teamsReleaseAutoUpdater) indexedOrListedReleases(ctx context.Context) ([]update.GitHubRelease, error) {
	indexReleases, indexErr := teamsAutoUpdateFetchReleaseIndex(ctx, update.ReleaseIndexOptions{
		Repo:    u.repo,
		Timeout: 8 * time.Second,
	})
	if indexErr == nil {
		return indexReleases, nil
	}
	releases, listErr := teamsAutoUpdateListReleases(ctx, update.ReleaseListOptions{
		Repo:    u.repo,
		Timeout: 8 * time.Second,
	})
	if listErr != nil {
		return nil, fmt.Errorf("release index failed: %v; release list failed: %w", indexErr, listErr)
	}
	return releases, nil
}

func (u teamsReleaseAutoUpdater) checkManualStable(ctx context.Context, check teams.HelperAutoUpdateCheck, now time.Time) (teams.HelperAutoUpdateDecision, error) {
	installedVersion := check.InstalledVersion
	if strings.TrimSpace(installedVersion) == "" || strings.EqualFold(strings.TrimSpace(installedVersion), "dev") {
		installedVersion = "0.0.0"
	}
	next := now.Add(update.DefaultAutoUpdateCheckInterval)
	indexReleases, indexErr := teamsAutoUpdateFetchReleaseIndex(ctx, update.ReleaseIndexOptions{
		Repo:    u.repo,
		Timeout: 8 * time.Second,
	})
	if indexErr == nil {
		selected := update.SelectAutoUpdateCandidate(indexReleases, update.AutoUpdateSelectionOptions{
			InstalledVersion: installedVersion,
			Now:              now,
			IgnorePriority:   true,
		})
		decision := teams.HelperAutoUpdateDecision{NextCheckAt: selected.NextCheckAt}
		if selected.Candidate != nil {
			decision.Candidate = &teams.HelperAutoUpdateCandidate{
				TagName:     selected.Candidate.TagName,
				Version:     selected.Candidate.Version,
				Priority:    "manual",
				PublishedAt: selected.Candidate.PublishedAt,
				EligibleAt:  selected.Candidate.EligibleAt,
				Asset:       selected.Candidate.Asset,
			}
		}
		return decision, nil
	}
	status := checkForUpdate(ctx, update.CheckOptions{
		Repo:             u.repo,
		InstalledVersion: installedVersion,
		Timeout:          8 * time.Second,
	})
	if !status.Supported {
		err := fmt.Errorf("helper update check failed: %s", strings.TrimSpace(status.Error))
		if strings.TrimSpace(status.Error) == "" {
			err = fmt.Errorf("helper update check failed")
		}
		err = fmt.Errorf("release index failed: %v; %w", indexErr, err)
		return teams.HelperAutoUpdateDecision{
			NextCheckAt:  next,
			BackoffUntil: next,
			LastError:    err.Error(),
		}, err
	}
	decision := teams.HelperAutoUpdateDecision{NextCheckAt: next}
	if !status.UpdateAvailable {
		return decision, nil
	}
	tag := strings.TrimSpace(status.RemoteTag)
	if tag == "" {
		tag = "v" + strings.TrimPrefix(strings.TrimSpace(status.RemoteVersion), "v")
	}
	decision.Candidate = &teams.HelperAutoUpdateCandidate{
		TagName:  tag,
		Version:  strings.TrimPrefix(strings.TrimSpace(status.RemoteVersion), "v"),
		Priority: "manual",
		Asset:    status.Asset,
	}
	return decision, nil
}

func (u teamsReleaseAutoUpdater) Apply(ctx context.Context, candidate teams.HelperAutoUpdateCandidate) (teams.HelperAutoUpdateApplyResult, error) {
	return u.ApplyWithOptions(ctx, candidate, teams.HelperAutoUpdateApplyOptions{})
}

func (u teamsReleaseAutoUpdater) ApplyWithOptions(ctx context.Context, candidate teams.HelperAutoUpdateCandidate, applyOpts teams.HelperAutoUpdateApplyOptions) (teams.HelperAutoUpdateApplyResult, error) {
	installPath, err := teamsAutoUpdateResolveInstallPath("")
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	lock, ok, err := acquireHelperInstallLock(ctx, installPath)
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	if !ok {
		return teams.HelperAutoUpdateApplyResult{}, fmt.Errorf("another helper auto-update is already updating %s", installPath)
	}
	defer func() { _ = lock.Unlock() }()
	activationPending, activationReason := teamsAutoUpdateShouldDeferActivation(installPath)
	res, err := performUpdate(ctx, update.UpdateOptions{
		Repo:               u.repo,
		Version:            candidate.TagName,
		InstallPath:        installPath,
		Timeout:            120 * time.Second,
		ValidateBinary:     true,
		PendingReplacement: teamsPendingReplacementMode(applyOpts),
	})
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	if res.RestartRequired {
		if err := ensureCXPShimForInstallPath(res.InstallPath); err != nil {
			return teams.HelperAutoUpdateApplyResult{}, err
		}
	} else {
		if err := finalizeHelperEntrypointsAfterUpgrade(res.InstallPath, res.Version, io.Discard); err != nil {
			return teams.HelperAutoUpdateApplyResult{}, err
		}
	}
	installBundledSkillsFromHelper(ctx, firstNonEmptyString(res.PendingReplacePath, res.InstallPath), io.Discard)
	return teams.HelperAutoUpdateApplyResult{
		Version:            res.Version,
		InstallPath:        res.InstallPath,
		RestartRequired:    res.RestartRequired,
		PendingReplacePath: res.PendingReplacePath,
		ActivationPending:  activationPending,
		ActivationReason:   activationReason,
	}, nil
}

func teamsPendingReplacementMode(applyOpts teams.HelperAutoUpdateApplyOptions) update.PendingReplacementMode {
	if applyOpts.OwnsPendingReplacement && teamsServiceGOOS() == "windows" {
		return update.PendingReplacementReturnOnly
	}
	return update.PendingReplacementScheduleDeferredMove
}

func teamsAutoUpdateShouldDeferActivation(stableInstallPath string) (bool, string) {
	raw, err := teamsAutoUpdateExecutable()
	if err != nil {
		return true, "helper update installed, but activation is pending because the running helper executable path could not be inspected: " + err.Error()
	}
	resolved, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
	if err != nil {
		class := helperpath.Classify(raw)
		if class.Transient {
			return true, "helper update installed to " + stableInstallPath + ", but activation is pending because the running helper executable is transient: " + class.Reason
		}
		return true, "helper update installed, but activation is pending because the running helper executable path is not stable: " + err.Error()
	}
	class := helperpath.Classify(raw)
	if class.Transient {
		return true, "helper update installed to " + stableInstallPath + ", but activation is pending because the running helper executable is transient: " + class.Reason
	}
	if strings.TrimSpace(stableInstallPath) != "" && !sameHelperInstallLocation(resolved.Path, stableInstallPath, teamsServiceGOOS()) {
		return true, "helper update installed to " + stableInstallPath + ", but activation is pending because the running helper executable is " + resolved.Path
	}
	return false, ""
}

func sameHelperExecutablePath(a string, b string, goos string) bool {
	a = filepath.Clean(strings.TrimSpace(a))
	b = filepath.Clean(strings.TrimSpace(b))
	if a == "" || b == "" {
		return a == b
	}
	if strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return strings.EqualFold(a, b)
	}
	return a == b
}
