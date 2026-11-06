package lease

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ErrUnsafeMount means the lease directory is on a mount whose options
// break the lease's assumptions; this host must not take part in it.
var ErrUnsafeMount = errors.New("lease: unsafe mount for the shared-home lease")

// mountEntry is one line of /proc/self/mountinfo.
type mountEntry struct {
	Point  string
	FSType string
	Opts   map[string]string // mount and super options; a flag maps to ""
}

// unsafeMount reports why a mount breaks the lease, or "".
//
// The lease (lease.go) relies on close-to-open consistency: a reader that
// opens the file sees what every writer flushed when it closed it. Without
// it a reader may keep serving cached slot bytes for as long as the
// attribute cache lets it: a standby then judges a renewing owner stale,
// and an owner that was taken over may not see the later slot that fences
// it, and keeps posting next to its successor (TAKEOVER-REPORT §9, drill 4).
//
//   - NFS `nocto` turns close-to-open off (nfs(5)).
//   - CIFS/SMB `cache=loose` caches without the oplock/lease coherency
//     the default `cache=strict` keeps (mount.cifs(8)).
func unsafeMount(m mountEntry) string {
	switch m.FSType {
	case "nfs", "nfs4":
		if _, ok := m.Opts["nocto"]; ok {
			return "it is mounted with nocto (no close-to-open consistency)"
		}
	case "cifs", "smb3":
		if m.Opts["cache"] == "loose" {
			return "it is mounted with cache=loose (no cache coherency)"
		}
	}
	return ""
}

// parseMountinfo returns the entry of the mount that holds path (absolute,
// symlinks resolved): the longest mount point that contains it, the last
// one listed among equals (it is mounted over the others).
func parseMountinfo(data, path string) (mountEntry, bool) {
	var best mountEntry
	found := false
	for _, line := range strings.Split(data, "\n") {
		pre, post, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		f, g := strings.Fields(pre), strings.Fields(post)
		if len(f) < 6 || len(g) < 3 {
			continue
		}
		point := unescapeMount(f[4])
		if !within(path, point) || (found && len(point) < len(best.Point)) {
			continue
		}
		e := mountEntry{Point: point, FSType: g[0], Opts: map[string]string{}}
		for _, list := range []string{f[5], g[2]} {
			for _, o := range strings.Split(list, ",") {
				k, v, _ := strings.Cut(o, "=")
				e.Opts[k] = v
			}
		}
		best, found = e, true
	}
	return best, found
}

func within(path, point string) bool {
	if point == "/" {
		return strings.HasPrefix(path, "/")
	}
	return path == point || strings.HasPrefix(path, point+"/")
}

// unescapeMount undoes the octal escapes (\040 for a space, …) of
// /proc/self/mountinfo.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			b.WriteByte((s[i+1]-'0')<<6 | (s[i+2]-'0')<<3 | (s[i+3] - '0'))
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func isOctal(c byte) bool { return '0' <= c && c <= '7' }

// checkMount returns an ErrUnsafeMount error when dir's mount, read from
// mountinfo, breaks the lease; nil when it is fine or cannot be told.
func checkMount(dir string, mountinfo func() (string, error)) error {
	if mountinfo == nil {
		return nil
	}
	data, err := mountinfo()
	if err != nil {
		return nil
	}
	path := existingAncestor(dir)
	m, ok := parseMountinfo(data, path)
	if !ok {
		return nil
	}
	why := unsafeMount(m)
	if why == "" {
		return nil
	}
	return fmt.Errorf("%w: %s is on %s %s, and %s; this host cannot take part in the takeover lease: remount it without that option",
		ErrUnsafeMount, dir, m.FSType, m.Point, why)
}

// existingAncestor resolves dir (or its nearest existing parent) to an
// absolute path without symlinks.
func existingAncestor(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return dir
	}
	for p := abs; ; p = filepath.Dir(p) {
		if r, err := filepath.EvalSymlinks(p); err == nil {
			if rel, err := filepath.Rel(p, abs); err == nil && rel != "." {
				return filepath.Join(r, rel)
			}
			return r
		}
		if p == filepath.Dir(p) {
			return abs
		}
	}
}
