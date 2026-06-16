package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func TestTeamsReleaseAutoUpdaterCheckSelectionAndBackoff(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	prevFetchReleaseIndex := teamsAutoUpdateFetchReleaseIndex
	prevCheckForUpdate := checkForUpdate
	t.Cleanup(func() {
		teamsAutoUpdateListReleases = prevListReleases
		teamsAutoUpdateFetchReleaseIndex = prevFetchReleaseIndex
		checkForUpdate = prevCheckForUpdate
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not be used by release-list auto-update checks")
		return update.Status{}
	}
	teamsAutoUpdateFetchReleaseIndex = func(context.Context, update.ReleaseIndexOptions) ([]update.GitHubRelease, error) {
		return nil, errors.New("update index missing")
	}

	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	release := func(tag string, priority update.AutoUpdatePriority, published time.Time) update.GitHubRelease {
		version := strings.TrimPrefix(tag, "v")
		asset := fmt.Sprintf("codex-proxy_%s_%s_%s", version, runtime.GOOS, runtime.GOARCH)
		if runtime.GOOS == "windows" {
			asset += ".exe"
		}
		return update.GitHubRelease{
			TagName:     tag,
			Body:        update.BuildReleasePriorityMarker(priority),
			PublishedAt: published,
			Assets: []struct {
				Name string `json:"name"`
			}{{Name: asset}},
		}
	}

	cases := []struct {
		name        string
		releases    []update.GitHubRelease
		listErr     error
		wantTag     string
		wantVersion string
		wantNext    time.Time
		wantBackoff bool
		includePre  bool
		checkPre    bool
		manual      bool
		installed   string
	}{
		{
			name:        "p0 immediate",
			releases:    []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP0, now.Add(-time.Minute))},
			wantTag:     "v1.2.4",
			wantVersion: "1.2.4",
			wantNext:    now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name:     "p1 waits until eligible",
			releases: []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP1, now.Add(-47*time.Hour-45*time.Minute))},
			wantNext: now.Add(15 * time.Minute),
		},
		{
			name:     "p2 skipped",
			releases: []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP2, now.Add(-time.Hour))},
			wantNext: now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name: "prerelease skipped by default",
			releases: []update.GitHubRelease{func() update.GitHubRelease {
				r := release("v1.2.4-rc.1", update.AutoUpdatePriorityP0, now.Add(-time.Hour))
				r.Prerelease = true
				return r
			}()},
			wantNext: now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name: "prerelease selected when enabled",
			releases: []update.GitHubRelease{func() update.GitHubRelease {
				r := release("v1.2.4-rc.1", update.AutoUpdatePriorityP0, now.Add(-time.Hour))
				r.Prerelease = true
				return r
			}()},
			includePre:  true,
			wantTag:     "v1.2.4-rc.1",
			wantVersion: "1.2.4-rc.1",
			wantNext:    now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name:        "manual prerelease path ignores p2 priority",
			releases:    []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP2, now.Add(-time.Hour))},
			manual:      true,
			checkPre:    true,
			wantTag:     "v1.2.4",
			wantVersion: "1.2.4",
			wantNext:    now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name:        "manual prerelease path can update unknown local version",
			releases:    []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP2, now.Add(-time.Hour))},
			manual:      true,
			checkPre:    true,
			installed:   "dev",
			wantTag:     "v1.2.4",
			wantVersion: "1.2.4",
			wantNext:    now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name:     "missing asset skipped",
			releases: []update.GitHubRelease{{TagName: "v1.2.4", Body: update.BuildReleasePriorityMarker(update.AutoUpdatePriorityP0), PublishedAt: now.Add(-time.Hour)}},
			wantNext: now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name:        "list failure backs off",
			listErr:     errors.New("github unavailable"),
			wantNext:    now.Add(update.DefaultAutoUpdateCheckInterval),
			wantBackoff: true,
		},
		{
			name:        "installed version accepts v prefix and metadata",
			releases:    []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP0, now.Add(-time.Hour))},
			wantTag:     "v1.2.4",
			wantVersion: "1.2.4",
			wantNext:    now.Add(update.DefaultAutoUpdateCheckInterval),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			teamsAutoUpdateListReleases = func(_ context.Context, opts update.ReleaseListOptions) ([]update.GitHubRelease, error) {
				if opts.Repo != "owner/name" {
					t.Fatalf("repo = %q, want owner/name", opts.Repo)
				}
				if opts.Timeout != 8*time.Second {
					t.Fatalf("timeout = %s, want 8s", opts.Timeout)
				}
				if tc.listErr != nil {
					return nil, tc.listErr
				}
				return tc.releases, nil
			}
			updater := teamsReleaseAutoUpdater{repo: "owner/name", includePrerelease: tc.includePre}
			installed := "v1.2.3"
			if strings.Contains(tc.name, "metadata") {
				installed = "v1.2.3+local"
			}
			if tc.installed != "" {
				installed = tc.installed
			}
			got, err := updater.Check(context.Background(), teams.HelperAutoUpdateCheck{
				InstalledVersion:  installed,
				Now:               now,
				IncludePrerelease: tc.checkPre,
				Manual:            tc.manual,
			})
			if tc.listErr != nil {
				if err == nil || !strings.Contains(err.Error(), "github unavailable") {
					t.Fatalf("Check error = %v, want github unavailable", err)
				}
			} else if err != nil {
				t.Fatalf("Check error: %v", err)
			}
			if tc.wantTag == "" {
				if got.Candidate != nil {
					t.Fatalf("candidate = %#v, want nil", got.Candidate)
				}
			} else if got.Candidate == nil || got.Candidate.TagName != tc.wantTag || got.Candidate.Version != tc.wantVersion {
				t.Fatalf("candidate = %#v, want %s/%s", got.Candidate, tc.wantTag, tc.wantVersion)
			}
			if !got.NextCheckAt.Equal(tc.wantNext) {
				t.Fatalf("NextCheckAt = %s, want %s", got.NextCheckAt, tc.wantNext)
			}
			if tc.wantBackoff && !got.BackoffUntil.Equal(tc.wantNext) {
				t.Fatalf("BackoffUntil = %s, want %s", got.BackoffUntil, tc.wantNext)
			}
			if tc.wantBackoff && !strings.Contains(got.LastError, "github unavailable") {
				t.Fatalf("LastError = %q, want github unavailable", got.LastError)
			}
		})
	}
}

func TestTeamsReleaseAutoUpdaterPrefersReleaseIndexOverListAPI(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	prevFetchReleaseIndex := teamsAutoUpdateFetchReleaseIndex
	prevCheckForUpdate := checkForUpdate
	t.Cleanup(func() {
		teamsAutoUpdateListReleases = prevListReleases
		teamsAutoUpdateFetchReleaseIndex = prevFetchReleaseIndex
		checkForUpdate = prevCheckForUpdate
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not be used by release-index auto-update checks")
		return update.Status{}
	}
	teamsAutoUpdateListReleases = func(context.Context, update.ReleaseListOptions) ([]update.GitHubRelease, error) {
		t.Fatal("release index success should not fall back to GitHub release list")
		return nil, nil
	}

	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	var gotIndexOpts update.ReleaseIndexOptions
	teamsAutoUpdateFetchReleaseIndex = func(_ context.Context, opts update.ReleaseIndexOptions) ([]update.GitHubRelease, error) {
		gotIndexOpts = opts
		return []update.GitHubRelease{
			teamsAutoUpdateReleaseForTest("v1.2.4", update.AutoUpdatePriorityP0, now.Add(-time.Minute), false),
		}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	got, err := updater.Check(context.Background(), teams.HelperAutoUpdateCheck{
		InstalledVersion: "1.2.3",
		Now:              now,
	})
	if err != nil {
		t.Fatalf("Check error: %v", err)
	}
	if gotIndexOpts.Repo != "owner/name" || gotIndexOpts.Timeout != 8*time.Second {
		t.Fatalf("release index options = %#v, want repo and timeout", gotIndexOpts)
	}
	if got.Candidate == nil || got.Candidate.TagName != "v1.2.4" || got.Candidate.Priority != "p0" {
		t.Fatalf("candidate = %#v, want indexed p0 v1.2.4", got.Candidate)
	}
}

func TestTeamsReleaseAutoUpdaterReleaseIndexNoCandidateDoesNotFallBackToListAPI(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	prevFetchReleaseIndex := teamsAutoUpdateFetchReleaseIndex
	t.Cleanup(func() {
		teamsAutoUpdateListReleases = prevListReleases
		teamsAutoUpdateFetchReleaseIndex = prevFetchReleaseIndex
	})
	teamsAutoUpdateListReleases = func(context.Context, update.ReleaseListOptions) ([]update.GitHubRelease, error) {
		t.Fatal("release index success with no candidate should not call GitHub release list")
		return nil, nil
	}

	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	teamsAutoUpdateFetchReleaseIndex = func(context.Context, update.ReleaseIndexOptions) ([]update.GitHubRelease, error) {
		return []update.GitHubRelease{
			teamsAutoUpdateReleaseForTest("v1.2.4", update.AutoUpdatePriorityP2, now.Add(-time.Hour), false),
		}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	got, err := updater.Check(context.Background(), teams.HelperAutoUpdateCheck{
		InstalledVersion: "1.2.3",
		Now:              now,
	})
	if err != nil {
		t.Fatalf("Check error: %v", err)
	}
	if got.Candidate != nil {
		t.Fatalf("candidate = %#v, want nil for indexed p2", got.Candidate)
	}
}

func TestTeamsReleaseAutoUpdaterReleaseIndexFailureFallsBackToListAPI(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	prevFetchReleaseIndex := teamsAutoUpdateFetchReleaseIndex
	t.Cleanup(func() {
		teamsAutoUpdateListReleases = prevListReleases
		teamsAutoUpdateFetchReleaseIndex = prevFetchReleaseIndex
	})

	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	teamsAutoUpdateFetchReleaseIndex = func(context.Context, update.ReleaseIndexOptions) ([]update.GitHubRelease, error) {
		return nil, errors.New("index 404")
	}
	teamsAutoUpdateListReleases = func(context.Context, update.ReleaseListOptions) ([]update.GitHubRelease, error) {
		return []update.GitHubRelease{
			teamsAutoUpdateReleaseForTest("v1.2.4", update.AutoUpdatePriorityP0, now.Add(-time.Minute), false),
		}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	got, err := updater.Check(context.Background(), teams.HelperAutoUpdateCheck{
		InstalledVersion: "1.2.3",
		Now:              now,
	})
	if err != nil {
		t.Fatalf("Check error: %v", err)
	}
	if got.Candidate == nil || got.Candidate.TagName != "v1.2.4" {
		t.Fatalf("candidate = %#v, want release-list fallback v1.2.4", got.Candidate)
	}
}

func TestTeamsReleaseAutoUpdaterManualStablePrefersReleaseIndex(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	prevFetchReleaseIndex := teamsAutoUpdateFetchReleaseIndex
	prevCheckForUpdate := checkForUpdate
	t.Cleanup(func() {
		teamsAutoUpdateListReleases = prevListReleases
		teamsAutoUpdateFetchReleaseIndex = prevFetchReleaseIndex
		checkForUpdate = prevCheckForUpdate
	})
	teamsAutoUpdateListReleases = func(context.Context, update.ReleaseListOptions) ([]update.GitHubRelease, error) {
		t.Fatal("manual stable helper update should not list GitHub releases")
		return nil, nil
	}
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("manual stable helper update should not call GitHub latest when release index succeeds")
		return update.Status{}
	}

	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	var gotIndexOpts update.ReleaseIndexOptions
	teamsAutoUpdateFetchReleaseIndex = func(_ context.Context, opts update.ReleaseIndexOptions) ([]update.GitHubRelease, error) {
		gotIndexOpts = opts
		return []update.GitHubRelease{
			teamsAutoUpdateReleaseForTest("v1.2.5-rc.1", update.AutoUpdatePriorityP0, now.Add(-time.Minute), true),
			teamsAutoUpdateReleaseForTest("v1.2.4", update.AutoUpdatePriorityP2, now.Add(-time.Hour), false),
		}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name", includePrerelease: true}
	got, err := updater.Check(context.Background(), teams.HelperAutoUpdateCheck{
		InstalledVersion:  "v1.2.3",
		Now:               now,
		Manual:            true,
		IncludePrerelease: false,
	})
	if err != nil {
		t.Fatalf("Check error: %v", err)
	}
	if gotIndexOpts.Repo != "owner/name" || gotIndexOpts.Timeout != 8*time.Second {
		t.Fatalf("release index options = %#v, want repo and timeout", gotIndexOpts)
	}
	if got.Candidate == nil || got.Candidate.TagName != "v1.2.4" || got.Candidate.Version != "1.2.4" {
		t.Fatalf("candidate = %#v, want v1.2.4", got.Candidate)
	}
	if got.Candidate.Priority != "manual" {
		t.Fatalf("candidate priority = %q, want manual", got.Candidate.Priority)
	}
	if !got.NextCheckAt.Equal(now.Add(update.DefaultAutoUpdateCheckInterval)) {
		t.Fatalf("NextCheckAt = %s, want %s", got.NextCheckAt, now.Add(update.DefaultAutoUpdateCheckInterval))
	}
}

func TestTeamsReleaseAutoUpdaterManualStableFallsBackToLatestCheckWhenIndexFails(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	prevFetchReleaseIndex := teamsAutoUpdateFetchReleaseIndex
	prevCheckForUpdate := checkForUpdate
	t.Cleanup(func() {
		teamsAutoUpdateListReleases = prevListReleases
		teamsAutoUpdateFetchReleaseIndex = prevFetchReleaseIndex
		checkForUpdate = prevCheckForUpdate
	})
	teamsAutoUpdateListReleases = func(context.Context, update.ReleaseListOptions) ([]update.GitHubRelease, error) {
		t.Fatal("manual stable helper update should not list GitHub releases")
		return nil, nil
	}
	teamsAutoUpdateFetchReleaseIndex = func(context.Context, update.ReleaseIndexOptions) ([]update.GitHubRelease, error) {
		return nil, errors.New("index 404")
	}

	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	var gotOpts update.CheckOptions
	checkForUpdate = func(_ context.Context, opts update.CheckOptions) update.Status {
		gotOpts = opts
		if opts.InstalledVersion != "0.0.0" {
			t.Fatalf("InstalledVersion = %q, want dev fallback 0.0.0", opts.InstalledVersion)
		}
		return update.Status{
			Supported:        true,
			Repo:             opts.Repo,
			InstalledVersion: "0.0.0",
			RemoteTag:        "v1.2.4",
			RemoteVersion:    "1.2.4",
			Asset:            "codex-proxy_1.2.4_linux_amd64",
			UpdateAvailable:  true,
		}
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	got, err := updater.Check(context.Background(), teams.HelperAutoUpdateCheck{
		InstalledVersion: "dev",
		Now:              now,
		Manual:           true,
	})
	if err != nil {
		t.Fatalf("Check error: %v", err)
	}
	if gotOpts.Repo != "owner/name" || gotOpts.Timeout != 8*time.Second || gotOpts.IncludePrerelease {
		t.Fatalf("CheckForUpdate options = %#v, want stable latest lookup", gotOpts)
	}
	if got.Candidate == nil || got.Candidate.TagName != "v1.2.4" || got.Candidate.Priority != "manual" {
		t.Fatalf("candidate = %#v, want manual latest fallback v1.2.4", got.Candidate)
	}
}

func TestTeamsReleaseAutoUpdaterManualStableBacksOffWhenIndexAndLatestFail(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	prevFetchReleaseIndex := teamsAutoUpdateFetchReleaseIndex
	prevCheckForUpdate := checkForUpdate
	t.Cleanup(func() {
		teamsAutoUpdateListReleases = prevListReleases
		teamsAutoUpdateFetchReleaseIndex = prevFetchReleaseIndex
		checkForUpdate = prevCheckForUpdate
	})
	teamsAutoUpdateListReleases = func(context.Context, update.ReleaseListOptions) ([]update.GitHubRelease, error) {
		t.Fatal("manual stable helper update should not list GitHub releases")
		return nil, nil
	}
	teamsAutoUpdateFetchReleaseIndex = func(context.Context, update.ReleaseIndexOptions) ([]update.GitHubRelease, error) {
		return nil, errors.New("index 404")
	}

	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	checkForUpdate = func(_ context.Context, opts update.CheckOptions) update.Status {
		if opts.InstalledVersion != "0.0.0" {
			t.Fatalf("InstalledVersion = %q, want dev fallback 0.0.0", opts.InstalledVersion)
		}
		return update.Status{
			Supported: false,
			Repo:      opts.Repo,
			Error:     "release lookup failed: 403 Forbidden",
		}
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	got, err := updater.Check(context.Background(), teams.HelperAutoUpdateCheck{
		InstalledVersion: "dev",
		Now:              now,
		Manual:           true,
	})
	if err == nil || !strings.Contains(err.Error(), "index 404") || !strings.Contains(err.Error(), "403 Forbidden") {
		t.Fatalf("Check error = %v, want index and latest failures", err)
	}
	want := now.Add(update.DefaultAutoUpdateCheckInterval)
	if !got.NextCheckAt.Equal(want) || !got.BackoffUntil.Equal(want) || !strings.Contains(got.LastError, "403 Forbidden") {
		t.Fatalf("decision = %#v, want 30m backoff with error", got)
	}
}

func teamsAutoUpdateReleaseForTest(tag string, priority update.AutoUpdatePriority, published time.Time, prerelease bool) update.GitHubRelease {
	version := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_%s_%s", version, runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		asset += ".exe"
	}
	return update.GitHubRelease{
		TagName:     tag,
		Priority:    string(priority),
		Prerelease:  prerelease,
		PublishedAt: published,
		Assets: []struct {
			Name string `json:"name"`
		}{{Name: asset}},
	}
}

func TestTeamsReleaseAutoUpdaterApplyUsesExplicitSelectedTag(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
	})
	fakeInstallPath := filepath.Join(t.TempDir(), "codex-proxy")
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) {
		return fakeInstallPath, nil
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
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
	if got.InstallPath != fakeInstallPath {
		t.Fatalf("InstallPath = %q, want %q", got.InstallPath, fakeInstallPath)
	}
	if !got.ValidateBinary {
		t.Fatal("auto-updater must validate the downloaded binary before restart")
	}
	if got.PendingReplacement != update.PendingReplacementScheduleDeferredMove {
		t.Fatalf("PendingReplacement = %v, want default scheduled deferred move", got.PendingReplacement)
	}
	if res.Version != "1.2.4" {
		t.Fatalf("result version = %q, want 1.2.4", res.Version)
	}
	if res.InstallPath != fakeInstallPath {
		t.Fatalf("result install path = %q, want %q", res.InstallPath, fakeInstallPath)
	}
}

func TestTeamsReleaseAutoUpdaterApplyUsesManagedDefaultWhenServiceRunsFromGoBin(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
	})
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	managed := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	goBin := filepath.Join(tmp, "go", "bin", "codex-proxy")
	for _, path := range []string{managed, goBin} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte("stable"), 0o755); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  goBin,
		cwd:  tmp,
	})
	teamsAutoUpdateResolveInstallPath = resolveManagedInstallPathForTeams
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	res, err := updater.Apply(context.Background(), teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"})
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if got.InstallPath != managed || res.InstallPath != managed {
		t.Fatalf("install path got options=%q result=%q, want managed %q", got.InstallPath, res.InstallPath, managed)
	}
	if !res.ActivationPending || !strings.Contains(res.ActivationReason, goBin) {
		t.Fatalf("activation = pending %v reason %q, want pending because service runs from go/bin", res.ActivationPending, res.ActivationReason)
	}
}

func TestTeamsReleaseAutoUpdaterApplyCreatesMissingCXPShim(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX cxp shim creation")
	}
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
		teamsAutoUpdateExecutable = prevExecutable
	})
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	managed := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	cxp := filepath.Join(tmp, ".local", "bin", "cxp")
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) {
		return managed, nil
	}
	teamsAutoUpdateExecutable = func() (string, error) {
		return managed, nil
	}
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		writeCLIFile(t, opts.InstallPath, "#!/bin/sh\nexit 0\n", 0o755)
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	if _, err := updater.Apply(context.Background(), teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"}); err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if target, err := os.Readlink(cxp); err != nil || target != managed {
		t.Fatalf("cxp shim = %q err=%v, want symlink to %s", target, err, managed)
	}
}

func TestTeamsReleaseAutoUpdaterApplyWithOptionsReturnsWindowsPendingReplacementToCaller(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	prevGOOS := teamsServiceGOOS
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
		teamsAutoUpdateExecutable = prevExecutable
		teamsServiceGOOS = prevGOOS
	})
	teamsServiceGOOS = func() string { return "windows" }
	fakeInstallPath := filepath.Join(t.TempDir(), "codex-proxy.exe")
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) {
		return fakeInstallPath, nil
	}
	teamsAutoUpdateExecutable = func() (string, error) {
		return fakeInstallPath, nil
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{
			Version:            "1.2.4",
			InstallPath:        opts.InstallPath,
			RestartRequired:    true,
			PendingReplacePath: filepath.Join(filepath.Dir(opts.InstallPath), ".codex-proxy_1.2.4_windows_amd64.exe.123"),
		}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	res, err := updater.ApplyWithOptions(context.Background(), teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"}, teams.HelperAutoUpdateApplyOptions{OwnsPendingReplacement: true})
	if err != nil {
		t.Fatalf("ApplyWithOptions error: %v", err)
	}
	if got.PendingReplacement != update.PendingReplacementReturnOnly {
		t.Fatalf("PendingReplacement = %v, want caller-owned pending replacement", got.PendingReplacement)
	}
	if strings.TrimSpace(res.PendingReplacePath) == "" || !res.RestartRequired {
		t.Fatalf("result = %#v, want pending replacement returned to Teams bridge", res)
	}
}

func TestTeamsReleaseAutoUpdaterApplyDefersActivationFromTransientExecutable(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
		teamsAutoUpdateExecutable = prevExecutable
	})
	dir := t.TempDir()
	stable := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) { return stable, nil }
	teamsAutoUpdateExecutable = func() (string, error) {
		return filepath.Join(dir, ".nfs802014de01c482a800000492"), nil
	}
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}
	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	res, err := updater.Apply(context.Background(), teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"})
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if !res.ActivationPending || !strings.Contains(res.ActivationReason, "transient") {
		t.Fatalf("activation = pending %v reason %q, want transient pending", res.ActivationPending, res.ActivationReason)
	}
}

func TestTeamsReleaseAutoUpdaterApplyKeepsImmediateActivationForStableExecutable(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
		teamsAutoUpdateExecutable = prevExecutable
	})
	dir := t.TempDir()
	stable := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) { return stable, nil }
	teamsAutoUpdateExecutable = func() (string, error) { return stable, nil }
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}
	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	res, err := updater.Apply(context.Background(), teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"})
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if res.ActivationPending || res.ActivationReason != "" {
		t.Fatalf("activation = pending %v reason %q, want immediate activation", res.ActivationPending, res.ActivationReason)
	}
}

func TestTeamsReleaseAutoUpdaterChecksActivationBeforeReplacingStableExecutable(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
		teamsAutoUpdateExecutable = prevExecutable
	})
	dir := t.TempDir()
	stable := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable: %v", err)
	}
	rawExecutable := stable
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) { return stable, nil }
	teamsAutoUpdateExecutable = func() (string, error) { return rawExecutable, nil }
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		rawExecutable = stable + " (deleted)"
		return update.ApplyResult{Version: "1.2.4", InstallPath: opts.InstallPath}, nil
	}

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	res, err := updater.Apply(context.Background(), teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"})
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if res.ActivationPending || res.ActivationReason != "" {
		t.Fatalf("activation = pending %v reason %q, want pre-update stable executable to activate immediately", res.ActivationPending, res.ActivationReason)
	}
}

func TestTeamsAutoUpdateActivationComparesWindowsPathsCaseInsensitively(t *testing.T) {
	lockCLITestHooks(t)
	prevExecutable := teamsAutoUpdateExecutable
	prevGOOS := teamsServiceGOOS
	t.Cleanup(func() {
		teamsAutoUpdateExecutable = prevExecutable
		teamsServiceGOOS = prevGOOS
	})
	teamsServiceGOOS = func() string { return "windows" }
	teamsAutoUpdateExecutable = func() (string, error) {
		return `c:\Users\Alice\AppData\Local\codex-helper\codex-proxy.exe`, nil
	}

	pending, reason := teamsAutoUpdateShouldDeferActivation(`C:\Users\Alice\AppData\Local\codex-helper\codex-proxy.exe`)
	if pending || reason != "" {
		t.Fatalf("activation pending=%v reason=%q, want no pending for same Windows path with different case", pending, reason)
	}
}

func TestTeamsReleaseAutoUpdaterApplyUsesSharedInstallLock(t *testing.T) {
	lockCLITestHooks(t)
	prevPerform := performUpdate
	prevResolveInstallPath := teamsAutoUpdateResolveInstallPath
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolveInstallPath
	})
	fakeInstallPath := filepath.Join(t.TempDir(), "codex-proxy")
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) {
		return fakeInstallPath, nil
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("PerformUpdate should not run while the shared install lock is held")
		return update.ApplyResult{}, nil
	}
	lock := flock.New(fakeInstallPath + ".auto-update.lock")
	locked, err := lock.TryLock()
	if err != nil {
		t.Fatalf("TryLock error: %v", err)
	}
	if !locked {
		t.Fatal("failed to acquire test install lock")
	}
	t.Cleanup(func() { _ = lock.Unlock() })

	updater := teamsReleaseAutoUpdater{repo: "owner/name"}
	_, err = updater.Apply(context.Background(), teams.HelperAutoUpdateCandidate{
		TagName:     "v1.2.4",
		Version:     "1.2.4",
		Priority:    "p0",
		PublishedAt: time.Now(),
		EligibleAt:  time.Now(),
	})
	if err == nil || !strings.Contains(err.Error(), "another helper auto-update is already updating") {
		t.Fatalf("expected shared install lock error, got %v", err)
	}
}
