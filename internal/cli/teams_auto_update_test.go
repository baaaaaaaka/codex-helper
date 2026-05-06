package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func TestTeamsReleaseAutoUpdaterCheckSelectionAndBackoff(t *testing.T) {
	lockCLITestHooks(t)
	prevListReleases := teamsAutoUpdateListReleases
	t.Cleanup(func() { teamsAutoUpdateListReleases = prevListReleases })

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
			name:        "manual ignores p2 priority",
			releases:    []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP2, now.Add(-time.Hour))},
			manual:      true,
			wantTag:     "v1.2.4",
			wantVersion: "1.2.4",
			wantNext:    now.Add(update.DefaultAutoUpdateCheckInterval),
		},
		{
			name:        "manual can update unknown local version",
			releases:    []update.GitHubRelease{release("v1.2.4", update.AutoUpdatePriorityP2, now.Add(-time.Hour))},
			manual:      true,
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
				InstalledVersion: installed,
				Now:              now,
				Manual:           tc.manual,
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
	if got.InstallPath != fakeInstallPath {
		t.Fatalf("InstallPath = %q, want %q", got.InstallPath, fakeInstallPath)
	}
	if !got.ValidateBinary {
		t.Fatal("auto-updater must validate the downloaded binary before restart")
	}
	if res.Version != "1.2.4" {
		t.Fatalf("result version = %q, want 1.2.4", res.Version)
	}
}
