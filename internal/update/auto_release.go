package update

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	AutoUpdatePriorityP0 AutoUpdatePriority = "p0"
	AutoUpdatePriorityP1 AutoUpdatePriority = "p1"
	AutoUpdatePriorityP2 AutoUpdatePriority = "p2"

	DefaultAutoUpdateCheckInterval = 30 * time.Minute
	AutoUpdateP1Delay              = 48 * time.Hour
)

var releaseMetadataRE = regexp.MustCompile(`(?s)<!--\s*codex-helper-release:\s*(\{.*?\})\s*-->`)

type AutoUpdatePriority string

type ReleaseListOptions struct {
	Repo     string
	Timeout  time.Duration
	PerPage  int
	MaxPages int
}

type GitHubRelease struct {
	TagName     string    `json:"tag_name"`
	Name        string    `json:"name"`
	Body        string    `json:"body"`
	Draft       bool      `json:"draft"`
	Prerelease  bool      `json:"prerelease"`
	PublishedAt time.Time `json:"published_at"`
	Assets      []struct {
		Name string `json:"name"`
	} `json:"assets"`
}

type AutoUpdateCandidate struct {
	TagName     string
	Version     string
	Priority    AutoUpdatePriority
	PublishedAt time.Time
	EligibleAt  time.Time
	Asset       string
}

type AutoUpdateSelection struct {
	Candidate     *AutoUpdateCandidate
	NextCheckAt   time.Time
	Scanned       int
	EligibleCount int
}

type AutoUpdateSelectionOptions struct {
	InstalledVersion string
	Now              time.Time
	GOOS             string
	GOARCH           string
}

func ListReleases(ctx context.Context, opts ReleaseListOptions) ([]GitHubRelease, error) {
	repo := ResolveRepo(opts.Repo)
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	perPage := opts.PerPage
	if perPage <= 0 || perPage > 100 {
		perPage = 100
	}
	maxPages := opts.MaxPages
	if maxPages <= 0 {
		maxPages = 2
	}
	client := &http.Client{Timeout: timeout}
	var out []GitHubRelease
	for page := 1; page <= maxPages; page++ {
		endpoint := fmt.Sprintf("%s/repos/%s/releases?per_page=%d&page=%d", githubAPIBase, repo, perPage, page)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "codex-proxy")
		req.Header.Set("Accept", "application/vnd.github+json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, githubReleaseListStatusError(resp, body)
		}
		var pageReleases []GitHubRelease
		if err := json.Unmarshal(body, &pageReleases); err != nil {
			return nil, err
		}
		out = append(out, pageReleases...)
		if len(pageReleases) < perPage || !hasNextLink(resp.Header.Get("Link")) {
			break
		}
	}
	return out, nil
}

func githubReleaseListStatusError(resp *http.Response, body []byte) error {
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	if msg == "" {
		return fmt.Errorf("release list failed: %s", resp.Status)
	}
	return fmt.Errorf("release list failed: %s: %s", resp.Status, msg)
}

func hasNextLink(linkHeader string) bool {
	for _, part := range strings.Split(linkHeader, ",") {
		pieces := strings.Split(part, ";")
		if len(pieces) < 2 {
			continue
		}
		for _, piece := range pieces[1:] {
			if strings.TrimSpace(piece) == `rel="next"` {
				return true
			}
		}
	}
	return false
}

func SelectAutoUpdateCandidate(releases []GitHubRelease, opts AutoUpdateSelectionOptions) AutoUpdateSelection {
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	goos := strings.TrimSpace(opts.GOOS)
	if goos == "" {
		goos = runtime.GOOS
	}
	goarch := strings.TrimSpace(opts.GOARCH)
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	local := normalizeVersion(opts.InstalledVersion)
	nextCheckAt := now.Add(DefaultAutoUpdateCheckInterval)
	if local == "" {
		return AutoUpdateSelection{
			NextCheckAt: nextCheckAt,
			Scanned:     len(releases),
		}
	}
	var candidates []AutoUpdateCandidate
	for _, rel := range releases {
		if rel.Draft || rel.Prerelease {
			continue
		}
		tag := strings.TrimSpace(rel.TagName)
		version := normalizeVersion(tag)
		if tag == "" || version == "" {
			continue
		}
		if isAutoUpdatePrereleaseVersion(version) {
			continue
		}
		newer, ok := compareVersionNewer(version, local)
		if !ok || !newer {
			continue
		}
		asset, err := assetName(version, goos, goarch)
		if err != nil || !releaseHasAsset(rel, asset) {
			continue
		}
		priority := ParseAutoUpdatePriority(rel.Body)
		eligibleAt, ok := autoUpdateEligibleAt(priority, rel.PublishedAt)
		if !ok {
			continue
		}
		if now.Before(eligibleAt) {
			if eligibleAt.Before(nextCheckAt) {
				nextCheckAt = eligibleAt
			}
			continue
		}
		candidates = append(candidates, AutoUpdateCandidate{
			TagName:     tag,
			Version:     version,
			Priority:    priority,
			PublishedAt: rel.PublishedAt,
			EligibleAt:  eligibleAt,
			Asset:       asset,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return compareVersion(candidates[i].Version, candidates[j].Version) > 0
	})
	var selected *AutoUpdateCandidate
	if len(candidates) > 0 {
		c := candidates[0]
		selected = &c
	}
	return AutoUpdateSelection{
		Candidate:     selected,
		NextCheckAt:   nextCheckAt,
		Scanned:       len(releases),
		EligibleCount: len(candidates),
	}
}

func ParseAutoUpdatePriority(body string) AutoUpdatePriority {
	matches := releaseMetadataRE.FindAllStringSubmatch(body, -1)
	if len(matches) != 1 {
		return AutoUpdatePriorityP2
	}
	var payload struct {
		AutoUpdatePriority string `json:"auto_update_priority"`
		Priority           string `json:"priority"`
	}
	if err := json.Unmarshal([]byte(matches[0][1]), &payload); err != nil {
		return AutoUpdatePriorityP2
	}
	value := strings.TrimSpace(payload.AutoUpdatePriority)
	if value == "" {
		value = strings.TrimSpace(payload.Priority)
	}
	switch AutoUpdatePriority(strings.ToLower(value)) {
	case AutoUpdatePriorityP0:
		return AutoUpdatePriorityP0
	case AutoUpdatePriorityP1:
		return AutoUpdatePriorityP1
	case AutoUpdatePriorityP2:
		return AutoUpdatePriorityP2
	default:
		return AutoUpdatePriorityP2
	}
}

func autoUpdateEligibleAt(priority AutoUpdatePriority, publishedAt time.Time) (time.Time, bool) {
	if publishedAt.IsZero() {
		return time.Time{}, false
	}
	switch priority {
	case AutoUpdatePriorityP0:
		return publishedAt, true
	case AutoUpdatePriorityP1:
		return publishedAt.Add(AutoUpdateP1Delay), true
	default:
		return time.Time{}, false
	}
}

func isAutoUpdatePrereleaseVersion(version string) bool {
	_, prerelease, ok := parseComparableVersion(version)
	return ok && prerelease
}

func releaseHasAsset(rel GitHubRelease, asset string) bool {
	if strings.TrimSpace(asset) == "" {
		return false
	}
	for _, candidate := range rel.Assets {
		if candidate.Name == asset {
			return true
		}
	}
	return false
}

func compareVersionNewer(remote, local string) (bool, bool) {
	cmp := compareVersion(remote, local)
	return cmp > 0, cmp != versionCompareInvalid
}

const versionCompareInvalid = -2

func compareVersion(left, right string) int {
	lv, lpre, lok := parseComparableVersion(left)
	rv, rpre, rok := parseComparableVersion(right)
	if !lok || !rok {
		return versionCompareInvalid
	}
	n := len(lv)
	if len(rv) > n {
		n = len(rv)
	}
	for len(lv) < n {
		lv = append(lv, 0)
	}
	for len(rv) < n {
		rv = append(rv, 0)
	}
	for i := 0; i < n; i++ {
		if lv[i] > rv[i] {
			return 1
		}
		if lv[i] < rv[i] {
			return -1
		}
	}
	if lpre != rpre {
		if lpre {
			return -1
		}
		return 1
	}
	return 0
}

func parseComparableVersion(v string) ([]int, bool, bool) {
	s := strings.TrimSpace(v)
	s = strings.TrimPrefix(s, "v")
	if s == "" {
		return nil, false, false
	}
	prerelease := false
	if base, suffix, ok := strings.Cut(s, "-"); ok {
		s = base
		prerelease = strings.TrimSpace(suffix) != ""
	}
	if base, _, ok := strings.Cut(s, "+"); ok {
		s = base
	}
	parts := strings.Split(s, ".")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			return nil, false, false
		}
		n := 0
		for _, ch := range p {
			if ch < '0' || ch > '9' {
				return nil, false, false
			}
			n = n*10 + int(ch-'0')
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, false, false
	}
	return out, prerelease, true
}

func BuildReleasePriorityMarker(priority AutoUpdatePriority) string {
	switch priority {
	case AutoUpdatePriorityP0, AutoUpdatePriorityP1, AutoUpdatePriorityP2:
	default:
		priority = AutoUpdatePriorityP2
	}
	return `<!-- codex-helper-release: {"auto_update_priority":"` + string(priority) + `"} -->`
}
