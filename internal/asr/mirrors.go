package asr

import (
	"net/url"
	"strings"
)

// A mirror is a directory of the exact published artifacts, not another
// build of the same upstream version. The pinned hash is always unchanged.
func withMirrorBases(m Manifest, bases []string) (Manifest, error) {
	for _, base := range bases {
		u, err := url.Parse(base)
		if err != nil || u.Host == "" || u.Scheme != "https" || u.User != nil || strings.ContainsAny(base, "?#") {
			return Manifest{}, newError(KindNotConfigured, "configure mirrors", nil, "mirror base must be an HTTPS directory without credentials, query or fragment")
		}
	}
	for _, a := range []*Asset{&m.Runtime, &m.FFmpeg, &m.Model, &m.MMProj} {
		if a.URL == "" {
			continue
		}
		mirrors := append([]string(nil), a.Mirrors...)
		a.Mirrors = nil
		seen := map[string]bool{a.URL: true}
		for _, base := range bases {
			raw := strings.TrimRight(base, "/") + "/" + url.PathEscape(a.Name)
			if !seen[raw] {
				a.Mirrors = append(a.Mirrors, raw)
				seen[raw] = true
			}
		}
		for _, raw := range mirrors {
			if !seen[raw] {
				a.Mirrors = append(a.Mirrors, raw)
				seen[raw] = true
			}
		}
	}
	return m, nil
}
