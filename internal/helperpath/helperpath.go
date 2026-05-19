package helperpath

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type Source string

const (
	SourceExplicit   Source = "explicit"
	SourceEnv        Source = "env"
	SourceExecutable Source = "executable"
	SourceArgv0      Source = "argv0"
)

type Kind string

const (
	KindEmpty        Kind = "empty"
	KindStable       Kind = "stable"
	KindNFSSilly     Kind = "nfs_silly_rename"
	KindDeleted      Kind = "deleted"
	KindReloadBackup Kind = "reload_backup"
	KindGoBuildTemp  Kind = "go_build_temp"
)

type Classification struct {
	Raw       string
	Clean     string
	Base      string
	Kind      Kind
	Transient bool
	Reason    string
}

type Probe struct {
	Classification
	Exists               bool
	IsDir                bool
	Executable           bool
	PlausibleHelperEntry bool
	Error                string
}

type Resolved struct {
	Path      string
	Raw       string
	Source    Source
	Reason    string
	Recovered bool
}

type Options struct {
	GOOS       string
	BinaryName string
	Stat       func(string) (os.FileInfo, error)
}

func (opts Options) withDefaults() Options {
	if strings.TrimSpace(opts.GOOS) == "" {
		opts.GOOS = runtime.GOOS
	}
	if strings.TrimSpace(opts.BinaryName) == "" {
		opts.BinaryName = BinaryName(opts.GOOS)
	}
	if opts.Stat == nil {
		opts.Stat = os.Stat
	}
	return opts
}

func BinaryName(goos string) string {
	if strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return "codex-proxy.exe"
	}
	return "codex-proxy"
}

// RawExecutable is the only approved wrapper around os.Executable. Callers must
// resolve the returned path through a purpose-specific helperpath API before
// executing, writing, persisting, or propagating it.
func RawExecutable() (string, error) {
	return os.Executable()
}

func ClassifyPath(path string) Classification {
	return Classify(path)
}

func Classify(path string) Classification {
	raw := strings.TrimSpace(path)
	if raw == "" {
		return Classification{Raw: path, Kind: KindEmpty, Transient: true, Reason: "path is empty"}
	}
	cleanInput := raw
	deleted := false
	if strings.HasSuffix(cleanInput, " (deleted)") {
		deleted = true
		cleanInput = strings.TrimSpace(strings.TrimSuffix(cleanInput, " (deleted)"))
	}
	clean := filepath.Clean(cleanInput)
	base := filepath.Base(clean)
	out := Classification{Raw: path, Clean: clean, Base: base, Kind: KindStable}
	if deleted {
		out.Kind = KindDeleted
		out.Transient = true
		out.Reason = "executable path is marked deleted"
		return out
	}
	if IsNFSSillyRenameBase(base) {
		out.Kind = KindNFSSilly
		out.Transient = true
		out.Reason = "executable path is an NFS silly-renamed file"
		return out
	}
	if reloadBackupBase(base) != base {
		out.Kind = KindReloadBackup
		out.Transient = true
		out.Reason = "executable path is a reload backup"
		return out
	}
	for _, part := range splitPathParts(clean) {
		if strings.HasPrefix(part, "go-build") {
			out.Kind = KindGoBuildTemp
			out.Transient = true
			out.Reason = "executable path is a temporary go-build path"
			return out
		}
	}
	return out
}

func ProbePath(path string, opts Options) Probe {
	opts = opts.withDefaults()
	class := Classify(path)
	probe := Probe{
		Classification:       class,
		PlausibleHelperEntry: isPlausibleHelperEntryBase(class.Base, opts),
	}
	if class.Kind == KindEmpty {
		probe.Error = class.Reason
		return probe
	}
	info, err := opts.Stat(class.Clean)
	if err != nil {
		probe.Error = err.Error()
		return probe
	}
	probe.Exists = true
	probe.IsDir = info.IsDir()
	probe.Executable = executableFileInfoAllowsRun(info, opts)
	return probe
}

func IsNFSSillyRenameBase(base string) bool {
	base = strings.TrimSpace(base)
	if !strings.HasPrefix(base, ".nfs") {
		return false
	}
	rest := strings.TrimPrefix(base, ".nfs")
	if rest == "" {
		return false
	}
	for _, r := range rest {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			continue
		}
		return false
	}
	return true
}

func StableInstallTarget(explicit string, envInstallDir string, rawExecutable string, opts Options) (Resolved, error) {
	return StableInstallTargetFromSources(explicit, envInstallDir, rawExecutable, "", opts)
}

func StableInstallTargetFromSources(explicit string, envInstallDir string, rawExecutable string, argv0 string, opts Options) (Resolved, error) {
	opts = opts.withDefaults()
	if v := strings.TrimSpace(explicit); v != "" {
		return resolveInstallCandidate(v, SourceExplicit, opts, false)
	}
	if v := strings.TrimSpace(envInstallDir); v != "" {
		return resolveInstallCandidate(v, SourceEnv, opts, true)
	}
	if strings.TrimSpace(rawExecutable) == "" {
		return resolveArgv0Fallback(argv0, opts)
	}
	resolved, err := resolveInstallCandidate(rawExecutable, SourceExecutable, opts, true)
	if err == nil {
		return resolved, nil
	}
	if strings.TrimSpace(argv0) != "" {
		if fallback, fallbackErr := resolveArgv0Fallback(argv0, opts); fallbackErr == nil {
			return fallback, nil
		}
	}
	return Resolved{}, err
}

func StableRunnablePath(rawExecutable string, opts Options) (Resolved, error) {
	return StableRunnablePathFromSources(rawExecutable, "", opts)
}

func StableRunnablePathFromSources(rawExecutable string, argv0 string, opts Options) (Resolved, error) {
	opts = opts.withDefaults()
	resolved, err := recoverStablePath(rawExecutable, SourceExecutable, opts, true)
	if err != nil {
		if strings.TrimSpace(argv0) != "" {
			if fallback, fallbackErr := resolveArgv0Fallback(argv0, opts); fallbackErr == nil {
				return fallback, nil
			}
		}
		return Resolved{}, err
	}
	if resolved.Path == "" {
		if strings.TrimSpace(argv0) != "" {
			if fallback, fallbackErr := resolveArgv0Fallback(argv0, opts); fallbackErr == nil {
				return fallback, nil
			}
		}
		return Resolved{}, fmt.Errorf("cannot resolve helper executable path from %q", rawExecutable)
	}
	return resolved, nil
}

func CanonicalOwnerExecutable(path string, opts Options) string {
	opts = opts.withDefaults()
	class := Classify(path)
	if class.Kind == KindDeleted || class.Kind == KindReloadBackup {
		dir, base := filepath.Split(class.Clean)
		return filepath.Join(dir, reloadBackupBase(base))
	}
	resolved, err := recoverStablePath(path, SourceExecutable, opts, false)
	if err == nil && strings.TrimSpace(resolved.Path) != "" {
		return resolved.Path
	}
	return class.Clean
}

func recoverStablePath(path string, source Source, opts Options, failUnresolvedTransient bool) (Resolved, error) {
	opts = opts.withDefaults()
	class := Classify(path)
	if class.Kind == KindEmpty {
		if failUnresolvedTransient {
			return Resolved{}, fmt.Errorf("cannot resolve helper path: %s", class.Reason)
		}
		return Resolved{Path: class.Clean, Raw: path, Source: source, Reason: class.Reason}, nil
	}
	if !class.Transient {
		return Resolved{Path: class.Clean, Raw: path, Source: source, Reason: "stable path"}, nil
	}
	var candidate string
	switch class.Kind {
	case KindNFSSilly:
		candidate = filepath.Join(filepath.Dir(class.Clean), opts.BinaryName)
	case KindDeleted, KindReloadBackup:
		dir, base := filepath.Split(class.Clean)
		candidate = filepath.Join(dir, reloadBackupBase(base))
	case KindGoBuildTemp:
		if failUnresolvedTransient {
			return Resolved{}, fmt.Errorf("cannot use temporary helper executable path %q: %s", class.Clean, class.Reason)
		}
		return Resolved{Path: class.Clean, Raw: path, Source: source, Reason: class.Reason}, nil
	default:
		candidate = class.Clean
	}
	if candidate != "" && executableFileExists(candidate, opts) && (candidate != class.Clean || class.Kind == KindDeleted) {
		return Resolved{Path: filepath.Clean(candidate), Raw: path, Source: source, Reason: class.Reason, Recovered: true}, nil
	}
	if failUnresolvedTransient {
		return Resolved{}, fmt.Errorf("cannot recover stable helper path from %q: %s", class.Clean, class.Reason)
	}
	return Resolved{Path: class.Clean, Raw: path, Source: source, Reason: class.Reason}, nil
}

func resolveInstallCandidate(path string, source Source, opts Options, recoverTransient bool) (Resolved, error) {
	opts = opts.withDefaults()
	candidate := strings.TrimSpace(path)
	if candidate == "" {
		return Resolved{}, fmt.Errorf("cannot resolve helper install path: %s path is empty", source)
	}
	if info, err := opts.Stat(candidate); err == nil && info.IsDir() {
		candidate = filepath.Join(candidate, opts.BinaryName)
	}
	class := Classify(candidate)
	if class.Transient {
		if !recoverTransient || class.Kind == KindGoBuildTemp {
			return Resolved{}, fmt.Errorf("cannot use transient helper install path %q from %s: %s", class.Clean, source, class.Reason)
		}
		return recoverStablePath(candidate, source, opts, true)
	}
	return Resolved{Path: class.Clean, Raw: path, Source: source, Reason: "stable install target"}, nil
}

func resolveArgv0Fallback(argv0 string, opts Options) (Resolved, error) {
	opts = opts.withDefaults()
	candidate := strings.TrimSpace(argv0)
	if candidate == "" {
		return Resolved{}, fmt.Errorf("cannot resolve helper path from argv0: argv0 is empty")
	}
	if !filepath.IsAbs(candidate) {
		return Resolved{}, fmt.Errorf("cannot resolve helper path from argv0 %q: argv0 must be absolute", argv0)
	}
	probe := ProbePath(candidate, opts)
	if probe.Transient {
		return Resolved{}, fmt.Errorf("cannot resolve helper path from argv0 %q: %s", argv0, probe.Reason)
	}
	if !probe.Exists || probe.IsDir || !probe.Executable {
		return Resolved{}, fmt.Errorf("cannot resolve helper path from argv0 %q: path is not an executable file", argv0)
	}
	if !probe.PlausibleHelperEntry {
		return Resolved{}, fmt.Errorf("cannot resolve helper path from argv0 %q: basename is not a known helper entry", argv0)
	}
	return Resolved{Path: probe.Clean, Raw: argv0, Source: SourceArgv0, Reason: "absolute argv0 fallback"}, nil
}

func executableFileExists(path string, opts Options) bool {
	opts = opts.withDefaults()
	info, err := opts.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return executableFileInfoAllowsRun(info, opts)
}

func executableFileInfoAllowsRun(info os.FileInfo, opts Options) bool {
	if strings.EqualFold(opts.GOOS, "windows") {
		return true
	}
	return info.Mode()&0o111 != 0
}

func isPlausibleHelperEntryBase(base string, opts Options) bool {
	base = strings.TrimSpace(base)
	if base == "" {
		return false
	}
	if strings.EqualFold(base, opts.BinaryName) {
		return true
	}
	if strings.EqualFold(opts.GOOS, "windows") {
		return strings.EqualFold(base, "cxp.exe") || strings.EqualFold(base, "codex-proxy.exe")
	}
	return base == "cxp" || base == "codex-proxy"
}

func reloadBackupBase(base string) string {
	if idx := strings.Index(base, ".reload-backup-"); idx >= 0 {
		return base[:idx]
	}
	return base
}

func splitPathParts(path string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\' || r == os.PathSeparator
	}) {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
