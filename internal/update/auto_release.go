package update

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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

var (
	releaseMetadataRE      = regexp.MustCompile(`(?s)<!--\s*codex-helper-release:\s*(\{.*?\})\s*-->`)
	releasePriorityAssetRE = regexp.MustCompile(`^codex-helper-auto-update-(p[012])(?:\.(?:txt|json))?$`)
)

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
	Priority    string    `json:"priority,omitempty"`
	Draft       bool      `json:"draft"`
	Prerelease  bool      `json:"prerelease"`
	PublishedAt time.Time `json:"published_at"`
	Assets      []struct {
		Name string `json:"name"`
	} `json:"assets"`
}

type ReleaseIndexOptions struct {
	Repo    string
	URL     string
	Timeout time.Duration
}

type ReleaseIndex struct {
	Version     int                   `json:"version"`
	Repo        string                `json:"repo,omitempty"`
	GeneratedAt time.Time             `json:"generated_at,omitempty"`
	Releases    []ReleaseIndexRelease `json:"releases"`
}

type ReleaseIndexRelease struct {
	TagName     string              `json:"tag_name"`
	Name        string              `json:"name,omitempty"`
	Body        string              `json:"body,omitempty"`
	Priority    string              `json:"priority,omitempty"`
	Draft       bool                `json:"draft,omitempty"`
	Prerelease  bool                `json:"prerelease,omitempty"`
	PublishedAt time.Time           `json:"published_at"`
	Assets      []ReleaseIndexAsset `json:"assets"`
}

type ReleaseIndexAsset struct {
	Name string `json:"name"`
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
	InstalledVersion  string
	Now               time.Time
	GOOS              string
	GOARCH            string
	IncludePrerelease bool
	IgnorePriority    bool
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

func FetchReleaseIndex(ctx context.Context, opts ReleaseIndexOptions) ([]GitHubRelease, error) {
	repo := ResolveRepo(opts.Repo)
	url := ResolveReleaseIndexURL(repo, opts.URL)
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "codex-proxy")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, releaseIndexStatusError(resp, body)
	}
	var idx ReleaseIndex
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("update index parse failed: %w", err)
	}
	if idx.Version != 1 {
		return nil, fmt.Errorf("update index unsupported version %d", idx.Version)
	}
	if strings.TrimSpace(idx.Repo) != "" && !strings.EqualFold(strings.TrimSpace(idx.Repo), repo) {
		return nil, fmt.Errorf("update index repo mismatch: %s", idx.Repo)
	}
	out := make([]GitHubRelease, 0, len(idx.Releases))
	for _, rel := range idx.Releases {
		out = append(out, GitHubRelease{
			TagName:     rel.TagName,
			Name:        rel.Name,
			Body:        rel.Body,
			Priority:    rel.Priority,
			Draft:       rel.Draft,
			Prerelease:  rel.Prerelease,
			PublishedAt: rel.PublishedAt,
			Assets:      releaseAssetsFromNames(releaseIndexAssetNames(rel.Assets)...),
		})
	}
	return out, nil
}

func ResolveReleaseIndexURL(repo string, explicit string) string {
	if v := strings.TrimSpace(explicit); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv(EnvUpdateIndexURL)); v != "" {
		return v
	}
	return fmt.Sprintf("%s/%s/auto-update-index/update-index.json", strings.TrimRight(githubRawBase, "/"), ResolveRepo(repo))
}

func releaseIndexStatusError(resp *http.Response, body []byte) error {
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	if msg == "" {
		return fmt.Errorf("update index lookup failed: %s", resp.Status)
	}
	return fmt.Errorf("update index lookup failed: %s: %s", resp.Status, msg)
}

func releaseIndexAssetNames(assets []ReleaseIndexAsset) []string {
	out := make([]string, 0, len(assets))
	for _, asset := range assets {
		if name := strings.TrimSpace(asset.Name); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func releaseAssetsFromNames(names ...string) []struct {
	Name string `json:"name"`
} {
	out := make([]struct {
		Name string `json:"name"`
	}, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			out = append(out, struct {
				Name string `json:"name"`
			}{Name: name})
		}
	}
	return out
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
		if rel.Draft {
			continue
		}
		tag := strings.TrimSpace(rel.TagName)
		version := normalizeVersion(tag)
		if tag == "" || version == "" {
			continue
		}
		if releaseIsPrerelease(rel, version) && !opts.IncludePrerelease {
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
		priority := ReleaseAutoUpdatePriority(rel)
		eligibleAt, ok := autoUpdateEligibleAt(priority, rel.PublishedAt, opts.IgnorePriority)
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

func ReleaseAutoUpdatePriority(rel GitHubRelease) AutoUpdatePriority {
	var values []AutoUpdatePriority
	if priority, ok := parseAutoUpdatePriorityValue(rel.Priority); ok {
		values = append(values, priority)
	}
	if priority, ok := parseAutoUpdatePriorityAsset(rel.Assets); ok {
		values = append(values, priority)
	}
	if priority, ok := parseAutoUpdatePriorityBody(rel.Body); ok {
		values = append(values, priority)
	}
	if len(values) == 0 {
		return AutoUpdatePriorityP2
	}
	first := values[0]
	for _, value := range values[1:] {
		if value != first {
			return AutoUpdatePriorityP2
		}
	}
	return first
}

func parseAutoUpdatePriorityBody(body string) (AutoUpdatePriority, bool) {
	matches := releaseMetadataRE.FindAllStringSubmatch(body, -1)
	if len(matches) != 1 {
		return "", false
	}
	var payload struct {
		AutoUpdatePriority string `json:"auto_update_priority"`
		Priority           string `json:"priority"`
	}
	if err := json.Unmarshal([]byte(matches[0][1]), &payload); err != nil {
		return "", false
	}
	value := strings.TrimSpace(payload.AutoUpdatePriority)
	if value == "" {
		value = strings.TrimSpace(payload.Priority)
	}
	return parseAutoUpdatePriorityValue(value)
}

func parseAutoUpdatePriorityAsset(assets []struct {
	Name string `json:"name"`
}) (AutoUpdatePriority, bool) {
	var values []AutoUpdatePriority
	for _, asset := range assets {
		matches := releasePriorityAssetRE.FindStringSubmatch(strings.TrimSpace(asset.Name))
		if len(matches) != 2 {
			continue
		}
		priority, ok := parseAutoUpdatePriorityValue(matches[1])
		if ok {
			values = append(values, priority)
		}
	}
	if len(values) != 1 {
		return "", false
	}
	return values[0], true
}

func parseAutoUpdatePriorityValue(value string) (AutoUpdatePriority, bool) {
	switch AutoUpdatePriority(strings.ToLower(strings.TrimSpace(value))) {
	case AutoUpdatePriorityP0:
		return AutoUpdatePriorityP0, true
	case AutoUpdatePriorityP1:
		return AutoUpdatePriorityP1, true
	case AutoUpdatePriorityP2:
		return AutoUpdatePriorityP2, true
	default:
		return "", false
	}
}

func autoUpdateEligibleAt(priority AutoUpdatePriority, publishedAt time.Time, ignorePriority bool) (time.Time, bool) {
	if publishedAt.IsZero() {
		return time.Time{}, false
	}
	if ignorePriority {
		return publishedAt, true
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
	parsed, ok := parseComparableVersion(version)
	return ok && parsed.prerelease != ""
}

func releaseIsPrerelease(rel GitHubRelease, version string) bool {
	return rel.Prerelease || isAutoUpdatePrereleaseVersion(version)
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

func CompareVersions(left, right string) (int, bool) {
	cmp := compareVersion(left, right)
	return cmp, cmp != versionCompareInvalid
}

func compareVersion(left, right string) int {
	lv, lok := parseComparableVersion(left)
	rv, rok := parseComparableVersion(right)
	if !lok || !rok {
		return versionCompareInvalid
	}
	n := len(lv.parts)
	if len(rv.parts) > n {
		n = len(rv.parts)
	}
	for len(lv.parts) < n {
		lv.parts = append(lv.parts, 0)
	}
	for len(rv.parts) < n {
		rv.parts = append(rv.parts, 0)
	}
	for i := 0; i < n; i++ {
		if lv.parts[i] > rv.parts[i] {
			return 1
		}
		if lv.parts[i] < rv.parts[i] {
			return -1
		}
	}
	if lv.prerelease != rv.prerelease {
		if lv.prerelease == "" {
			return 1
		}
		if rv.prerelease == "" {
			return -1
		}
		return comparePrerelease(lv.prerelease, rv.prerelease)
	}
	return 0
}

type comparableVersion struct {
	parts      []int
	prerelease string
}

func parseComparableVersion(v string) (comparableVersion, bool) {
	s := strings.TrimSpace(v)
	s = strings.TrimPrefix(s, "v")
	if s == "" {
		return comparableVersion{}, false
	}
	if base, suffix, ok := strings.Cut(s, "-"); ok {
		s = base
		suffix = strings.TrimSpace(suffix)
		if buildBase, _, ok := strings.Cut(suffix, "+"); ok {
			suffix = strings.TrimSpace(buildBase)
		}
		return parseComparableVersionBase(s, suffix)
	}
	if base, _, ok := strings.Cut(s, "+"); ok {
		s = base
	}
	return parseComparableVersionBase(s, "")
}

func parseComparableVersionBase(base string, prerelease string) (comparableVersion, bool) {
	parts := strings.Split(base, ".")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			return comparableVersion{}, false
		}
		n := 0
		for _, ch := range p {
			if ch < '0' || ch > '9' {
				return comparableVersion{}, false
			}
			n = n*10 + int(ch-'0')
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return comparableVersion{}, false
	}
	return comparableVersion{parts: out, prerelease: prerelease}, true
}

func comparePrerelease(left string, right string) int {
	leftIDs := prereleaseIdentifiers(left)
	rightIDs := prereleaseIdentifiers(right)
	n := len(leftIDs)
	if len(rightIDs) > n {
		n = len(rightIDs)
	}
	for i := 0; i < n; i++ {
		if i >= len(leftIDs) {
			return -1
		}
		if i >= len(rightIDs) {
			return 1
		}
		cmp := comparePrereleaseIdentifier(leftIDs[i], rightIDs[i])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func prereleaseIdentifiers(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool {
		return r == '.' || r == '-'
	})
}

func comparePrereleaseIdentifier(left string, right string) int {
	leftNum, leftNumeric := prereleaseIdentifierNumber(left)
	rightNum, rightNumeric := prereleaseIdentifierNumber(right)
	if leftNumeric && rightNumeric {
		if leftNum > rightNum {
			return 1
		}
		if leftNum < rightNum {
			return -1
		}
		return 0
	}
	if leftNumeric != rightNumeric {
		if leftNumeric {
			return -1
		}
		return 1
	}
	return strings.Compare(left, right)
}

func prereleaseIdentifierNumber(value string) (int, bool) {
	if value == "" {
		return 0, false
	}
	out := 0
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		out = out*10 + int(ch-'0')
	}
	return out, true
}

func BuildReleasePriorityMarker(priority AutoUpdatePriority) string {
	switch priority {
	case AutoUpdatePriorityP0, AutoUpdatePriorityP1, AutoUpdatePriorityP2:
	default:
		priority = AutoUpdatePriorityP2
	}
	return `<!-- codex-helper-release: {"auto_update_priority":"` + string(priority) + `"} -->`
}
