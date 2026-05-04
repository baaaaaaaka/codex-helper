package update

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseAutoUpdatePriority(t *testing.T) {
	tests := []struct {
		name string
		body string
		want AutoUpdatePriority
	}{
		{name: "p0 marker", body: `notes <!-- codex-helper-release: {"auto_update_priority":"p0"} -->`, want: AutoUpdatePriorityP0},
		{name: "p1 marker", body: `<!-- codex-helper-release: {"priority":"p1"} -->`, want: AutoUpdatePriorityP1},
		{name: "p2 marker", body: `<!-- codex-helper-release: {"auto_update_priority":"p2"} -->`, want: AutoUpdatePriorityP2},
		{name: "missing defaults p2", body: `regular release notes`, want: AutoUpdatePriorityP2},
		{name: "bad json defaults p2", body: `<!-- codex-helper-release: {"auto_update_priority": -->`, want: AutoUpdatePriorityP2},
		{name: "unknown defaults p2", body: `<!-- codex-helper-release: {"auto_update_priority":"p9"} -->`, want: AutoUpdatePriorityP2},
		{name: "duplicate defaults p2", body: `<!-- codex-helper-release: {"auto_update_priority":"p0"} --><!-- codex-helper-release: {"auto_update_priority":"p1"} -->`, want: AutoUpdatePriorityP2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseAutoUpdatePriority(tt.body); got != tt.want {
				t.Fatalf("ParseAutoUpdatePriority() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSelectAutoUpdateCandidateMixedReleaseWindow(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	releases := []GitHubRelease{
		releaseForAutoTest("v1.2.6", AutoUpdatePriorityP2, now.Add(-time.Hour), false),
		releaseForAutoTest("v1.2.5", AutoUpdatePriorityP1, now.Add(-49*time.Hour), false),
		releaseForAutoTest("v1.2.4", AutoUpdatePriorityP0, now.Add(-time.Minute), false),
		releaseForAutoTest("v1.2.7-rc.1", AutoUpdatePriorityP0, now.Add(-time.Minute), true),
		releaseForAutoTest("v1.2.3", AutoUpdatePriorityP0, now.Add(-time.Hour), false),
	}
	got := SelectAutoUpdateCandidate(releases, AutoUpdateSelectionOptions{
		InstalledVersion: "1.2.3-rc.1",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
	})
	if got.Candidate == nil {
		t.Fatal("Candidate is nil")
	}
	if got.Candidate.TagName != "v1.2.5" {
		t.Fatalf("candidate tag = %q, want v1.2.5", got.Candidate.TagName)
	}
	if got.Candidate.Priority != AutoUpdatePriorityP1 {
		t.Fatalf("candidate priority = %q, want p1", got.Candidate.Priority)
	}
}

func TestSelectAutoUpdateCandidateP1Waits48Hours(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	rel := releaseForAutoTest("v1.2.4", AutoUpdatePriorityP1, now.Add(-47*time.Hour), false)
	got := SelectAutoUpdateCandidate([]GitHubRelease{rel}, AutoUpdateSelectionOptions{
		InstalledVersion: "1.2.3",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
	})
	if got.Candidate != nil {
		t.Fatalf("candidate = %#v, want nil before 48h", got.Candidate)
	}
	wantNext := now.Add(DefaultAutoUpdateCheckInterval)
	if !got.NextCheckAt.Equal(wantNext) {
		t.Fatalf("NextCheckAt = %s, want %s", got.NextCheckAt, wantNext)
	}
}

func TestSelectAutoUpdateCandidateP1SoonUsesEligibleTime(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	rel := releaseForAutoTest("v1.2.4", AutoUpdatePriorityP1, now.Add(-47*time.Hour-45*time.Minute), false)
	got := SelectAutoUpdateCandidate([]GitHubRelease{rel}, AutoUpdateSelectionOptions{
		InstalledVersion: "1.2.3",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
	})
	if got.Candidate != nil {
		t.Fatalf("candidate = %#v, want nil before 48h", got.Candidate)
	}
	wantNext := now.Add(15 * time.Minute)
	if !got.NextCheckAt.Equal(wantNext) {
		t.Fatalf("NextCheckAt = %s, want %s", got.NextCheckAt, wantNext)
	}
}

func TestSelectAutoUpdateCandidateTakesEligibleP0WhileNewerReleasesWaitOrSkip(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	releases := []GitHubRelease{
		releaseForAutoTest("v1.2.6", AutoUpdatePriorityP2, now.Add(-time.Hour), false),
		releaseForAutoTest("v1.2.5", AutoUpdatePriorityP1, now.Add(-47*time.Hour), false),
		releaseForAutoTest("v1.2.4", AutoUpdatePriorityP0, now.Add(-time.Minute), false),
	}
	got := SelectAutoUpdateCandidate(releases, AutoUpdateSelectionOptions{
		InstalledVersion: "1.2.3",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
	})
	if got.Candidate == nil {
		t.Fatal("Candidate is nil")
	}
	if got.Candidate.TagName != "v1.2.4" {
		t.Fatalf("candidate tag = %q, want v1.2.4", got.Candidate.TagName)
	}
	if got.Candidate.Priority != AutoUpdatePriorityP0 {
		t.Fatalf("candidate priority = %q, want p0", got.Candidate.Priority)
	}
}

func TestSelectAutoUpdateCandidateSkipsMissingAssetAndDowngrade(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	missingAsset := releaseForAutoTest("v1.2.5", AutoUpdatePriorityP0, now.Add(-time.Hour), false)
	missingAsset.Assets = nil
	old := releaseForAutoTest("v1.2.3", AutoUpdatePriorityP0, now.Add(-time.Hour), false)
	got := SelectAutoUpdateCandidate([]GitHubRelease{missingAsset, old}, AutoUpdateSelectionOptions{
		InstalledVersion: "1.2.4",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
	})
	if got.Candidate != nil {
		t.Fatalf("candidate = %#v, want nil", got.Candidate)
	}
}

func TestSelectAutoUpdateCandidateSkipsPrereleaseTagEvenWhenReleaseFlagIsWrong(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	prerelease := releaseForAutoTest("v1.2.4-rc.1", AutoUpdatePriorityP0, now.Add(-time.Hour), false)
	got := SelectAutoUpdateCandidate([]GitHubRelease{prerelease}, AutoUpdateSelectionOptions{
		InstalledVersion: "1.2.3",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
	})
	if got.Candidate != nil {
		t.Fatalf("candidate = %#v, want nil for prerelease tag", got.Candidate)
	}
}

func TestSelectAutoUpdateCandidateUnknownInstalledVersionFailsClosed(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	got := SelectAutoUpdateCandidate([]GitHubRelease{
		releaseForAutoTest("v1.2.4", AutoUpdatePriorityP0, now.Add(-time.Hour), false),
	}, AutoUpdateSelectionOptions{
		InstalledVersion: "dev",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
	})
	if got.Candidate != nil {
		t.Fatalf("candidate = %#v, want nil for unknown installed version", got.Candidate)
	}
}

func TestListReleasesPaginates(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.URL.Query().Get("page") {
		case "1":
			w.Header().Set("Link", `<`+serverURLForTest(r, "2")+`>; rel="next"`)
			writeJSON(t, w, []GitHubRelease{releaseForAutoTest("v1.2.4", AutoUpdatePriorityP2, time.Now(), false)})
		case "2":
			writeJSON(t, w, []GitHubRelease{releaseForAutoTest("v1.2.5", AutoUpdatePriorityP0, time.Now(), false)})
		default:
			t.Fatalf("unexpected page %q", r.URL.Query().Get("page"))
		}
	}))
	defer server.Close()
	prev := githubAPIBase
	githubAPIBase = server.URL
	t.Cleanup(func() { githubAPIBase = prev })

	got, err := ListReleases(context.Background(), ReleaseListOptions{Repo: "owner/name", PerPage: 1, MaxPages: 2})
	if err != nil {
		t.Fatalf("ListReleases error: %v", err)
	}
	if calls != 2 || len(got) != 2 {
		t.Fatalf("calls=%d len=%d, want 2/2", calls, len(got))
	}
}

func releaseForAutoTest(tag string, priority AutoUpdatePriority, published time.Time, prerelease bool) GitHubRelease {
	version := normalizeVersion(tag)
	asset, _ := assetName(version, "linux", "amd64")
	return GitHubRelease{
		TagName:     tag,
		Body:        BuildReleasePriorityMarker(priority),
		Prerelease:  prerelease,
		PublishedAt: published,
		Assets: []struct {
			Name string `json:"name"`
		}{{Name: asset}},
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}

func serverURLForTest(r *http.Request, page string) string {
	q := r.URL.Query()
	q.Set("page", page)
	next := *r.URL
	next.RawQuery = q.Encode()
	return next.String()
}
