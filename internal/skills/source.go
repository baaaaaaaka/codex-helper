package skills

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

type URLInfo struct {
	Provider  string
	RemoteURL string
	Name      string
	Ref       string
	Path      string
}

type URLParseOptions struct {
	Name      string
	Ref       string
	Path      string
	KnownRefs []string
}

func ParseURL(raw string, opts URLParseOptions) (URLInfo, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return URLInfo{}, fmt.Errorf("empty skill source URL")
	}
	info := URLInfo{RemoteURL: raw}
	if sshInfo, ok := parseScpLikeGitURL(raw); ok {
		info.Provider = sshInfo.Provider
		info.Name = sshInfo.Name
	}
	if parsed, err := url.Parse(raw); err == nil && parsed.Scheme != "" {
		host := strings.ToLower(parsed.Hostname())
		segments := cleanURLPathSegments(parsed.EscapedPath())
		switch {
		case host == "github.com" && len(segments) >= 2:
			info.Provider = "github"
			info.Name = safeName(segments[0] + "-" + stripGitSuffix(segments[1]))
			info.RemoteURL = (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host, Path: "/" + segments[0] + "/" + stripGitSuffix(segments[1]) + ".git"}).String()
			if len(segments) >= 4 && segments[2] == "tree" {
				ref, sub := splitRefAndPath(segments[3:], opts.KnownRefs)
				info.Ref = ref
				info.Path = sub
			}
		case (host == "gitlab.com" || strings.Contains(host, "gitlab")) && len(segments) >= 2:
			info.Provider = "gitlab"
			repoEnd := gitlabRepoSegmentEnd(segments)
			if repoEnd >= 2 {
				repoSegments := segments[:repoEnd]
				repoSegments[len(repoSegments)-1] = stripGitSuffix(repoSegments[len(repoSegments)-1])
				info.Name = safeName(strings.Join(repoSegments, "-"))
				remoteSegments := append([]string(nil), repoSegments...)
				remoteSegments[len(remoteSegments)-1] = remoteSegments[len(remoteSegments)-1] + ".git"
				info.RemoteURL = (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host, Path: "/" + strings.Join(remoteSegments, "/")}).String()
				if repoEnd+2 < len(segments) && segments[repoEnd] == "-" && segments[repoEnd+1] == "tree" {
					ref, sub := splitRefAndPath(segments[repoEnd+2:], opts.KnownRefs)
					info.Ref = ref
					info.Path = sub
				}
			}
		default:
			base := path.Base(strings.TrimSuffix(parsed.Path, "/"))
			info.Name = safeName(stripGitSuffix(base))
		}
	}
	if strings.TrimSpace(opts.Name) != "" {
		info.Name = safeName(opts.Name)
	}
	if strings.TrimSpace(opts.Ref) != "" {
		info.Ref = strings.TrimSpace(opts.Ref)
	}
	if strings.TrimSpace(opts.Path) != "" {
		p, err := cleanGitPath(opts.Path)
		if err != nil {
			return URLInfo{}, err
		}
		info.Path = p
	}
	if info.Name == "" {
		info.Name = safeName(stripGitSuffix(path.Base(raw)))
	}
	if info.Name == "" {
		return URLInfo{}, fmt.Errorf("could not infer source name from %q; pass --name", raw)
	}
	if info.RemoteURL == "" {
		info.RemoteURL = raw
	}
	return info, nil
}

type scpLikeGitURL struct {
	Provider string
	Name     string
}

func parseScpLikeGitURL(raw string) (scpLikeGitURL, bool) {
	if strings.Contains(raw, "://") {
		return scpLikeGitURL{}, false
	}
	at := strings.Index(raw, "@")
	colon := strings.Index(raw, ":")
	if at <= 0 || colon <= at+1 {
		return scpLikeGitURL{}, false
	}
	host := strings.ToLower(raw[at+1 : colon])
	repo := strings.Trim(raw[colon+1:], "/")
	segments := strings.Split(repo, "/")
	if len(segments) < 2 {
		return scpLikeGitURL{}, false
	}
	provider := ""
	if host == "github.com" {
		provider = "github"
	} else if host == "gitlab.com" || strings.Contains(host, "gitlab") {
		provider = "gitlab"
	}
	return scpLikeGitURL{
		Provider: provider,
		Name:     safeName(strings.Join(segments[:len(segments)-1], "-") + "-" + stripGitSuffix(segments[len(segments)-1])),
	}, true
}

func cleanURLPathSegments(escaped string) []string {
	unescaped, err := url.PathUnescape(escaped)
	if err != nil {
		unescaped = escaped
	}
	raw := strings.Split(strings.Trim(unescaped, "/"), "/")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func gitlabRepoSegmentEnd(segments []string) int {
	for i := 0; i < len(segments); i++ {
		if segments[i] == "-" {
			return i
		}
	}
	return len(segments)
}

func splitRefAndPath(segments []string, knownRefs []string) (string, string) {
	if len(segments) == 0 {
		return "", ""
	}
	if len(knownRefs) > 0 {
		refs := append([]string(nil), knownRefs...)
		sort.Slice(refs, func(i, j int) bool {
			return strings.Count(refs[i], "/") > strings.Count(refs[j], "/")
		})
		joined := strings.Join(segments, "/")
		for _, ref := range refs {
			ref = strings.Trim(ref, "/")
			if joined == ref {
				return ref, ""
			}
			if strings.HasPrefix(joined, ref+"/") {
				return ref, strings.TrimPrefix(joined, ref+"/")
			}
		}
	}
	return segments[0], strings.Join(segments[1:], "/")
}

func stripGitSuffix(v string) string {
	return strings.TrimSuffix(strings.TrimSpace(v), ".git")
}

var safeNameRE = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func safeName(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ReplaceAll(v, "\\", "-")
	v = strings.ReplaceAll(v, "/", "-")
	v = safeNameRE.ReplaceAllString(v, "-")
	v = strings.Trim(v, ".-_")
	if v == "" {
		return ""
	}
	return strings.ToLower(v)
}

func stableID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	sum := hex.EncodeToString(h.Sum(nil))
	base := safeName(firstNonEmpty(parts...))
	if base == "" {
		base = "source"
	}
	if len(base) > 40 {
		base = base[:40]
		base = strings.TrimRight(base, ".-_")
	}
	return base + "-" + sum[:10]
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func cleanGitPath(p string) (string, error) {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	p = strings.Trim(p, "/")
	if p == "" {
		return "", nil
	}
	if strings.Contains(p, "\x00") {
		return "", fmt.Errorf("git path contains NUL")
	}
	segments := strings.Split(p, "/")
	for _, s := range segments {
		if err := validatePathSegment(s); err != nil {
			return "", fmt.Errorf("invalid git path %q: %w", p, err)
		}
	}
	return strings.Join(segments, "/"), nil
}

func validateRepoRelPath(p string) error {
	if p == "" {
		return fmt.Errorf("empty path")
	}
	if strings.Contains(p, "\x00") || strings.Contains(p, "\\") {
		return fmt.Errorf("path contains unsupported separator or NUL")
	}
	if strings.HasPrefix(p, "/") || filepath.IsAbs(p) {
		return fmt.Errorf("absolute path is not allowed")
	}
	if strings.HasPrefix(p, "//") || strings.HasPrefix(p, `\\`) {
		return fmt.Errorf("UNC path is not allowed")
	}
	if runtime.GOOS == "windows" && len(p) >= 2 && p[1] == ':' {
		return fmt.Errorf("drive path is not allowed")
	}
	segments := strings.Split(p, "/")
	for _, s := range segments {
		if err := validatePathSegment(s); err != nil {
			return err
		}
	}
	return nil
}

func validatePathSegment(s string) error {
	if s == "" || s == "." || s == ".." {
		return fmt.Errorf("invalid path segment %q", s)
	}
	if strings.ContainsAny(s, "\x00/\\") {
		return fmt.Errorf("invalid path segment %q", s)
	}
	if strings.Contains(s, ":") {
		return fmt.Errorf("Windows alternate stream syntax is not allowed in %q", s)
	}
	trimmed := strings.TrimRight(strings.ToLower(s), ". ")
	switch trimmed {
	case "con", "prn", "aux", "nul",
		"com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
		"lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9":
		return fmt.Errorf("Windows reserved name %q is not allowed", s)
	}
	return nil
}

func upsertSource(sources []Source, source Source) []Source {
	for i := range sources {
		if sources[i].ID == source.ID {
			sources[i] = source
			return sources
		}
	}
	return append(sources, source)
}

func upsertState(states []SourceState, state SourceState) []SourceState {
	for i := range states {
		if states[i].ID == state.ID {
			states[i] = state
			return states
		}
	}
	return append(states, state)
}

func removeSource(sources []Source, idOrName string) ([]Source, *Source) {
	filtered := sources[:0]
	var removed *Source
	for i := range sources {
		if sourceMatches(sources[i], idOrName) {
			copy := sources[i]
			removed = &copy
			continue
		}
		filtered = append(filtered, sources[i])
	}
	return filtered, removed
}

func removeState(states []SourceState, id string) []SourceState {
	filtered := states[:0]
	for _, st := range states {
		if st.ID != id {
			filtered = append(filtered, st)
		}
	}
	return filtered
}

func sourceMatches(source Source, idOrName string) bool {
	q := strings.TrimSpace(idOrName)
	return q != "" && (source.ID == q || strings.EqualFold(source.Name, q))
}

func sourceStateByID(st State, id string) (SourceState, bool) {
	for _, s := range st.Sources {
		if s.ID == id {
			return s, true
		}
	}
	return SourceState{}, false
}

func nowUTC() time.Time {
	return time.Now().UTC().Truncate(time.Second)
}
