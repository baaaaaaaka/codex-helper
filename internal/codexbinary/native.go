package codexbinary

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// ErrNativeBinaryNotFound marks the case where the codex wrapper resolved but
// no native binary exists yet at the expected vendor location — typically a
// transient window while codex's npm package is mid-reinstall. It is retryable;
// other resolution errors (unsupported platform, bad wrapper) are not.
var ErrNativeBinaryNotFound = errors.New("native codex binary not found")

// Native binary resolution retry budget, tunable for tests. The total wait
// rides out the brief window where an npm reinstall has removed the old vendor
// binary but not yet written the new one.
var (
	nativeBinaryResolveAttempts = 8
	nativeBinaryResolveDelay    = 300 * time.Millisecond
)

// targetTriple returns the Rust target triple for the current platform.
func targetTriple() string {
	return targetTripleFor(runtime.GOOS, runtime.GOARCH)
}

func targetTripleFor(goos string, goarch string) string {
	switch goos {
	case "linux":
		switch goarch {
		case "amd64":
			return "x86_64-unknown-linux-musl"
		case "arm64":
			return "aarch64-unknown-linux-musl"
		}
	case "darwin":
		switch goarch {
		case "amd64":
			return "x86_64-apple-darwin"
		case "arm64":
			return "aarch64-apple-darwin"
		}
	case "windows":
		switch goarch {
		case "amd64":
			return "x86_64-pc-windows-msvc"
		case "arm64":
			return "aarch64-pc-windows-msvc"
		}
	}
	return ""
}

// platformPackageName returns the npm optional-dependency package name
// that ships the native binary for the current platform (e.g.
// "@openai/codex-win32-x64"). Returns "" if unknown.
func platformPackageName() string {
	return platformPackageNameForTriple(targetTriple())
}

func platformPackageNameForTriple(triple string) string {
	switch triple {
	case "x86_64-unknown-linux-musl":
		return filepath.Join("@openai", "codex-linux-x64")
	case "aarch64-unknown-linux-musl":
		return filepath.Join("@openai", "codex-linux-arm64")
	case "x86_64-apple-darwin":
		return filepath.Join("@openai", "codex-darwin-x64")
	case "aarch64-apple-darwin":
		return filepath.Join("@openai", "codex-darwin-arm64")
	case "x86_64-pc-windows-msvc":
		return filepath.Join("@openai", "codex-win32-x64")
	case "aarch64-pc-windows-msvc":
		return filepath.Join("@openai", "codex-win32-arm64")
	}
	return ""
}

// nativeBinaryName returns "codex.exe" on Windows, "codex" elsewhere.
func nativeBinaryName() string {
	return nativeBinaryNameForOS(runtime.GOOS)
}

func nativeBinaryNameForOS(goos string) string {
	if goos == "windows" {
		return "codex.exe"
	}
	return "codex"
}

// resolveWrapper resolves the codex wrapper path to the actual codex.js
// entry point. On Unix this follows symlinks; on Windows it parses the
// npm .cmd shim to extract the .js path.
func resolveWrapper(wrapperPath string) (string, error) {
	resolved, err := filepath.EvalSymlinks(wrapperPath)
	if err != nil {
		return "", fmt.Errorf("eval symlinks: %w", err)
	}

	// On Windows npm creates .cmd shims instead of symlinks.
	// Parse the .cmd to find the actual .js entry point.
	if strings.HasSuffix(strings.ToLower(resolved), ".cmd") {
		jsPath, err := parseNpmCmdShim(resolved)
		if err != nil {
			return "", fmt.Errorf("parse cmd shim: %w", err)
		}
		return jsPath, nil
	}

	return resolved, nil
}

// npmCmdShimRe matches the relative .js path in an npm .cmd shim, e.g.:
//
//	"%dp0%\node_modules\@openai\codex\bin\codex.js"
//
// It also accepts Codex's managed-Node shim form:
//
//	"%~dp0node_modules\@openai\codex\bin\codex.js"
var npmCmdShimRe = regexp.MustCompile(`(?:%~?dp0%[\\\/]|%~dp0)([^"]+\.js)`)

// parseNpmCmdShim reads an npm .cmd shim and extracts the path to
// the .js entry point, resolved relative to the .cmd's directory.
func parseNpmCmdShim(cmdPath string) (string, error) {
	data, err := os.ReadFile(cmdPath)
	if err != nil {
		return "", err
	}
	m := npmCmdShimRe.FindSubmatch(data)
	if m == nil {
		return "", fmt.Errorf("could not find .js path in %s", cmdPath)
	}
	cmdDir := filepath.Dir(cmdPath)
	// .cmd files always use backslashes; normalize to the OS separator.
	relPath := strings.ReplaceAll(string(m[1]), `\`, string(filepath.Separator))
	jsPath := filepath.Join(cmdDir, relPath)
	if _, err := os.Stat(jsPath); err != nil {
		return "", fmt.Errorf("resolved .js not found: %s", jsPath)
	}
	return jsPath, nil
}

// findVendorBinary searches for the native binary + path dir under a given
// vendor root. Returns ("", "", nil) if not found.
func findVendorBinary(vendorRoot string) (nativeBin string, pathDir string) {
	triple := targetTriple()
	binName := nativeBinaryName()

	tripleRoot := filepath.Join(vendorRoot, triple)
	candidates := []struct {
		binaryPath string
		pathDir    string
	}{
		{
			binaryPath: filepath.Join(tripleRoot, "bin", binName),
			pathDir:    filepath.Join(tripleRoot, "codex-path"),
		},
		{
			binaryPath: filepath.Join(tripleRoot, "codex", binName),
			pathDir:    filepath.Join(tripleRoot, "path"),
		},
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate.binaryPath); err != nil {
			continue
		}
		pathDir = candidate.pathDir
		if _, err := os.Stat(pathDir); err != nil {
			pathDir = ""
		}
		return candidate.binaryPath, pathDir
	}
	return "", ""
}

func dedupePaths(paths []string) []string {
	deduped := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		deduped = append(deduped, path)
	}
	return deduped
}

func candidatePlatformPackageRoots(moduleDir string, platPkg string) []string {
	if platPkg == "" {
		return nil
	}

	candidates := []string{
		filepath.Join(moduleDir, "node_modules", platPkg),
		filepath.Join(filepath.Dir(moduleDir), "node_modules", platPkg),
	}

	// Mirror the relevant parts of Node's ancestor node_modules lookup so global
	// installs such as <prefix>/lib/node_modules/@openai/codex and sibling alias
	// packages like <prefix>/lib/node_modules/@openai/codex-darwin-arm64 resolve
	// in the same order as require.resolve(.../package.json).
	for dir := filepath.Clean(filepath.Dir(moduleDir)); ; dir = filepath.Dir(dir) {
		if filepath.Base(dir) == "node_modules" {
			candidates = append(candidates, filepath.Join(dir, platPkg))
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}

	return dedupePaths(candidates)
}

func firstResolvedPlatformVendorRoot(moduleDir string, platPkg string) (vendorRoot string, packageRoot string, found bool, err error) {
	for _, pkgRoot := range candidatePlatformPackageRoots(moduleDir, platPkg) {
		packageJSON := filepath.Join(pkgRoot, "package.json")
		if _, statErr := os.Stat(packageJSON); statErr == nil {
			return filepath.Join(pkgRoot, "vendor"), pkgRoot, true, nil
		} else if !os.IsNotExist(statErr) {
			return "", "", false, fmt.Errorf("stat %s: %w", packageJSON, statErr)
		}
	}
	return "", "", false, nil
}

func missingBinaryErrorForVendorRoot(triple string, vendorRoot string) error {
	return fmt.Errorf("%w for %s in resolved platform package vendor %s", ErrNativeBinaryNotFound, triple, filepath.Clean(vendorRoot))
}

// FindNativeBinaryWithRetry wraps FindNativeBinary, retrying only on
// ErrNativeBinaryNotFound (the transient npm-reinstall window). Hard errors
// (unsupported platform, unresolvable wrapper) return immediately.
func FindNativeBinaryWithRetry(codexWrapperPath string) (nativeBin string, pathDir string, err error) {
	attempts := nativeBinaryResolveAttempts
	if attempts < 1 {
		attempts = 1
	}
	for i := 0; i < attempts; i++ {
		nativeBin, pathDir, err = FindNativeBinary(codexWrapperPath)
		if err == nil {
			return nativeBin, pathDir, nil
		}
		if !errors.Is(err, ErrNativeBinaryNotFound) {
			return "", "", err
		}
		if i < attempts-1 {
			time.Sleep(nativeBinaryResolveDelay)
		}
	}
	return "", "", err
}

// FindNativeBinary locates the native Codex binary given the path to the
// codex wrapper (e.g. /home/user/.npm-global/bin/codex on Unix or
// C:\Users\...\npm\codex.cmd on Windows).
//
// It handles:
//   - Unix symlink resolution (wrapper → <pkg>/bin/codex.js)
//   - Windows npm .cmd shim parsing
//   - Current vendor layout (<pkg>/vendor/<triple>/bin/... plus codex-path)
//   - Legacy vendor layout (<pkg>/vendor/<triple>/codex/... plus path)
//   - Platform-specific npm sub-packages (<pkg>/node_modules/@openai/codex-<plat>/vendor/...)
//   - Ancestor node_modules roots used by npm global installs and sibling aliases
func FindNativeBinary(codexWrapperPath string) (nativeBin string, pathDir string, err error) {
	triple := targetTriple()
	if triple == "" {
		return "", "", fmt.Errorf("unsupported platform: %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	resolved, err := resolveWrapper(codexWrapperPath)
	if err != nil {
		return "", "", err
	}

	moduleDir := filepath.Dir(resolved)
	pkgDir := filepath.Dir(moduleDir)
	platPkg := platformPackageName()
	if platPkg != "" {
		vendorRoot, _, found, err := firstResolvedPlatformVendorRoot(moduleDir, platPkg)
		if err != nil {
			return "", "", err
		}
		if found {
			if bin, pd := findVendorBinary(vendorRoot); bin != "" {
				return bin, pd, nil
			}
			return "", "", missingBinaryErrorForVendorRoot(triple, vendorRoot)
		}
	}

	localVendorRoot := filepath.Join(pkgDir, "vendor")
	if bin, pd := findVendorBinary(localVendorRoot); bin != "" {
		return bin, pd, nil
	}

	candidates := []string{localVendorRoot}
	if platPkg != "" {
		candidates = append(candidates, candidatePlatformPackageRoots(moduleDir, platPkg)...)
	}
	return "", "", fmt.Errorf(
		"%w for %s (checked %s)",
		ErrNativeBinaryNotFound,
		triple,
		strings.Join(dedupePaths(candidates), ", "),
	)
}
