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

func TestReleaseAutoUpdatePrioritySourcesAndConflicts(t *testing.T) {
	cases := []struct {
		name string
		rel  GitHubRelease
		want AutoUpdatePriority
	}{
		{
			name: "explicit index priority",
			rel:  GitHubRelease{Priority: "p0"},
			want: AutoUpdatePriorityP0,
		},
		{
			name: "release asset priority marker",
			rel: GitHubRelease{Assets: []struct {
				Name string `json:"name"`
			}{{Name: "codex-helper-auto-update-p1.txt"}}},
			want: AutoUpdatePriorityP1,
		},
		{
			name: "release body priority marker",
			rel:  GitHubRelease{Body: BuildReleasePriorityMarker(AutoUpdatePriorityP0)},
			want: AutoUpdatePriorityP0,
		},
		{
			name: "matching sources accepted",
			rel: GitHubRelease{
				Priority: "p1",
				Body:     BuildReleasePriorityMarker(AutoUpdatePriorityP1),
				Assets: []struct {
					Name string `json:"name"`
				}{{Name: "codex-helper-auto-update-p1"}},
			},
			want: AutoUpdatePriorityP1,
		},
		{
			name: "conflicting sources fail closed",
			rel: GitHubRelease{
				Priority: "p0",
				Body:     BuildReleasePriorityMarker(AutoUpdatePriorityP1),
			},
			want: AutoUpdatePriorityP2,
		},
		{
			name: "duplicate asset markers fail closed",
			rel: GitHubRelease{Assets: []struct {
				Name string `json:"name"`
			}{{Name: "codex-helper-auto-update-p0"}, {Name: "codex-helper-auto-update-p1"}}},
			want: AutoUpdatePriorityP2,
		},
		{
			name: "missing priority defaults p2",
			rel:  GitHubRelease{},
			want: AutoUpdatePriorityP2,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReleaseAutoUpdatePriority(tt.rel); got != tt.want {
				t.Fatalf("ReleaseAutoUpdatePriority() = %q, want %q", got, tt.want)
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

func TestFetchReleaseIndexConvertsStaticIndexToGitHubReleases(t *testing.T) {
	published := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/owner/name/auto-update-index/update-index.json" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		writeJSON(t, w, ReleaseIndex{
			Version: 1,
			Repo:    "owner/name",
			Releases: []ReleaseIndexRelease{{
				TagName:     "v1.2.4",
				Priority:    "p0",
				PublishedAt: published,
				Assets:      []ReleaseIndexAsset{{Name: "codex-proxy_1.2.4_linux_amd64"}},
			}},
		})
	}))
	defer server.Close()
	prev := githubRawBase
	githubRawBase = server.URL
	t.Cleanup(func() { githubRawBase = prev })

	got, err := FetchReleaseIndex(context.Background(), ReleaseIndexOptions{Repo: "owner/name"})
	if err != nil {
		t.Fatalf("FetchReleaseIndex error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("release count = %d, want 1", len(got))
	}
	if got[0].TagName != "v1.2.4" || got[0].Priority != "p0" || !got[0].PublishedAt.Equal(published) {
		t.Fatalf("release = %#v, want indexed v1.2.4 p0", got[0])
	}
	if !releaseHasAsset(got[0], "codex-proxy_1.2.4_linux_amd64") {
		t.Fatalf("indexed release assets = %#v, want linux asset", got[0].Assets)
	}
}

func TestFetchReleaseIndexRejectsWrongRepo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, ReleaseIndex{Version: 1, Repo: "other/repo"})
	}))
	defer server.Close()

	_, err := FetchReleaseIndex(context.Background(), ReleaseIndexOptions{
		Repo: "owner/name",
		URL:  server.URL + "/update-index.json",
	})
	if err == nil {
		t.Fatal("FetchReleaseIndex error = nil, want repo mismatch")
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

func TestSelectAutoUpdateCandidateCanOptIntoLatestPrerelease(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	releases := []GitHubRelease{
		releaseForAutoTest("v1.2.4-rc.2", AutoUpdatePriorityP0, now.Add(-time.Hour), true),
		releaseForAutoTest("v1.2.4-rc.10", AutoUpdatePriorityP0, now.Add(-time.Hour), true),
		releaseForAutoTest("v1.2.3", AutoUpdatePriorityP0, now.Add(-time.Hour), false),
	}
	got := SelectAutoUpdateCandidate(releases, AutoUpdateSelectionOptions{
		InstalledVersion:  "1.2.3",
		Now:               now,
		GOOS:              "linux",
		GOARCH:            "amd64",
		IncludePrerelease: true,
	})
	if got.Candidate == nil {
		t.Fatal("Candidate is nil")
	}
	if got.Candidate.TagName != "v1.2.4-rc.10" {
		t.Fatalf("candidate tag = %q, want v1.2.4-rc.10", got.Candidate.TagName)
	}
}

func TestSelectAutoUpdateCandidateStableBeatsSameVersionPrerelease(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	releases := []GitHubRelease{
		releaseForAutoTest("v1.2.4-rc.10", AutoUpdatePriorityP0, now.Add(-time.Hour), true),
		releaseForAutoTest("v1.2.4", AutoUpdatePriorityP0, now.Add(-30*time.Minute), false),
	}
	got := SelectAutoUpdateCandidate(releases, AutoUpdateSelectionOptions{
		InstalledVersion:  "1.2.3",
		Now:               now,
		GOOS:              "linux",
		GOARCH:            "amd64",
		IncludePrerelease: true,
	})
	if got.Candidate == nil {
		t.Fatal("Candidate is nil")
	}
	if got.Candidate.TagName != "v1.2.4" {
		t.Fatalf("candidate tag = %q, want v1.2.4", got.Candidate.TagName)
	}
}

func TestCompareVersionsReleasePrereleaseBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		left     string
		right    string
		wantSign int
		wantOK   bool
	}{
		{name: "stable beats same base rc", left: "1.2.4", right: "1.2.4-rc.1", wantSign: 1, wantOK: true},
		{name: "same base rc is older than stable", left: "1.2.4-rc.2", right: "1.2.4", wantSign: -1, wantOK: true},
		{name: "newer base rc beats older stable", left: "1.2.5-rc.1", right: "1.2.4", wantSign: 1, wantOK: true},
		{name: "rc numeric identifiers sort numerically", left: "1.2.4-rc.10", right: "1.2.4-rc.2", wantSign: 1, wantOK: true},
		{name: "higher base rc stays newer than lower stable", left: "1.3.0-rc.1", right: "1.2.9", wantSign: 1, wantOK: true},
		{name: "build metadata does not affect ordering", left: "1.2.4+local", right: "1.2.4", wantSign: 0, wantOK: true},
		{name: "invalid versions fail closed", left: "dev", right: "1.2.4", wantSign: versionCompareInvalid, wantOK: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := CompareVersions(tt.left, tt.right)
			if ok != tt.wantOK {
				t.Fatalf("CompareVersions ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				if got != versionCompareInvalid {
					t.Fatalf("CompareVersions(%q, %q) = %d, want invalid sentinel %d", tt.left, tt.right, got, versionCompareInvalid)
				}
				return
			}
			if sign := compareSign(got); sign != tt.wantSign {
				t.Fatalf("CompareVersions(%q, %q) = %d (sign %d), want sign %d", tt.left, tt.right, got, sign, tt.wantSign)
			}
		})
	}
}

func TestSelectAutoUpdateCandidateReleasePrereleaseChannelMatrix(t *testing.T) {
	now := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name              string
		installed         string
		includePrerelease bool
		releases          []GitHubRelease
		wantTag           string
	}{
		{
			name:      "stable to newer stable works by default",
			installed: "1.2.3",
			releases:  []GitHubRelease{releaseForAutoTest("v1.2.4", AutoUpdatePriorityP0, now.Add(-time.Hour), false)},
			wantTag:   "v1.2.4",
		},
		{
			name:      "stable to newer prerelease is skipped by default",
			installed: "1.2.3",
			releases:  []GitHubRelease{releaseForAutoTest("v1.2.4-rc.1", AutoUpdatePriorityP0, now.Add(-time.Hour), true)},
		},
		{
			name:              "stable to newer prerelease works with opt in",
			installed:         "1.2.3",
			includePrerelease: true,
			releases:          []GitHubRelease{releaseForAutoTest("v1.2.4-rc.1", AutoUpdatePriorityP0, now.Add(-time.Hour), true)},
			wantTag:           "v1.2.4-rc.1",
		},
		{
			name:      "prerelease to same base stable works by default",
			installed: "1.2.4-rc.1",
			releases:  []GitHubRelease{releaseForAutoTest("v1.2.4", AutoUpdatePriorityP0, now.Add(-time.Hour), false)},
			wantTag:   "v1.2.4",
		},
		{
			name:              "stable to same base prerelease is not an upgrade even with opt in",
			installed:         "1.2.4",
			includePrerelease: true,
			releases:          []GitHubRelease{releaseForAutoTest("v1.2.4-rc.2", AutoUpdatePriorityP0, now.Add(-time.Hour), true)},
		},
		{
			name:      "high prerelease does not move to lower stable",
			installed: "1.3.0-rc.1",
			releases:  []GitHubRelease{releaseForAutoTest("v1.2.9", AutoUpdatePriorityP0, now.Add(-time.Hour), false)},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectAutoUpdateCandidate(tt.releases, AutoUpdateSelectionOptions{
				InstalledVersion:  tt.installed,
				Now:               now,
				GOOS:              "linux",
				GOARCH:            "amd64",
				IncludePrerelease: tt.includePrerelease,
			})
			if tt.wantTag == "" {
				if got.Candidate != nil {
					t.Fatalf("candidate = %#v, want nil", got.Candidate)
				}
				return
			}
			if got.Candidate == nil || got.Candidate.TagName != tt.wantTag {
				t.Fatalf("candidate = %#v, want %s", got.Candidate, tt.wantTag)
			}
		})
	}
}

func TestSelectAutoUpdateCandidateManualIgnoresPriorityButStillNeedsOptInForPrerelease(t *testing.T) {
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	releases := []GitHubRelease{
		releaseForAutoTest("v1.2.5-rc.1", AutoUpdatePriorityP2, now.Add(-time.Hour), true),
		releaseForAutoTest("v1.2.4", AutoUpdatePriorityP2, now.Add(-time.Hour), false),
	}
	stableOnly := SelectAutoUpdateCandidate(releases, AutoUpdateSelectionOptions{
		InstalledVersion: "1.2.3",
		Now:              now,
		GOOS:             "linux",
		GOARCH:           "amd64",
		IgnorePriority:   true,
	})
	if stableOnly.Candidate == nil || stableOnly.Candidate.TagName != "v1.2.4" {
		t.Fatalf("stable-only manual candidate = %#v, want v1.2.4", stableOnly.Candidate)
	}
	withPrerelease := SelectAutoUpdateCandidate(releases, AutoUpdateSelectionOptions{
		InstalledVersion:  "1.2.3",
		Now:               now,
		GOOS:              "linux",
		GOARCH:            "amd64",
		IncludePrerelease: true,
		IgnorePriority:    true,
	})
	if withPrerelease.Candidate == nil || withPrerelease.Candidate.TagName != "v1.2.5-rc.1" {
		t.Fatalf("prerelease manual candidate = %#v, want v1.2.5-rc.1", withPrerelease.Candidate)
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

func compareSign(value int) int {
	switch {
	case value > 0:
		return 1
	case value < 0:
		return -1
	default:
		return 0
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
