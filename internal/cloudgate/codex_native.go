package cloudgate

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

// targetTriple returns the Rust target triple for the current platform.
func targetTriple() string {
	switch runtime.GOOS {
	case "linux":
		switch runtime.GOARCH {
		case "amd64":
			return "x86_64-unknown-linux-musl"
		case "arm64":
			return "aarch64-unknown-linux-musl"
		}
	case "darwin":
		switch runtime.GOARCH {
		case "amd64":
			return "x86_64-apple-darwin"
		case "arm64":
			return "aarch64-apple-darwin"
		}
	case "windows":
		switch runtime.GOARCH {
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
	switch targetTriple() {
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
	if runtime.GOOS == "windows" {
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
var npmCmdShimRe = regexp.MustCompile(`%~?dp0%[\\\/]([^"]+\.js)`)

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
	jsPath := filepath.Join(cmdDir, filepath.FromSlash(string(m[1])))
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
	candidate := filepath.Join(vendorRoot, triple, "codex", binName)
	if _, err := os.Stat(candidate); err != nil {
		return "", ""
	}
	pathDir = filepath.Join(vendorRoot, triple, "path")
	if _, err := os.Stat(pathDir); err != nil {
		pathDir = ""
	}
	return candidate, pathDir
}

// FindNativeBinary locates the native Codex binary given the path to the
// codex wrapper (e.g. /home/user/.npm-global/bin/codex on Unix or
// C:\Users\...\npm\codex.cmd on Windows).
//
// It handles:
//   - Unix symlink resolution (wrapper → <pkg>/bin/codex.js)
//   - Windows npm .cmd shim parsing
//   - Vendor directory in the main package (<pkg>/vendor/<triple>/...)
//   - Platform-specific npm sub-packages (<pkg>/node_modules/@openai/codex-<plat>/vendor/...)
func FindNativeBinary(codexWrapperPath string) (nativeBin string, pathDir string, err error) {
	triple := targetTriple()
	if triple == "" {
		return "", "", fmt.Errorf("unsupported platform: %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	resolved, err := resolveWrapper(codexWrapperPath)
	if err != nil {
		return "", "", err
	}

	// The resolved wrapper should be at <pkg>/bin/codex.js
	pkgDir := filepath.Dir(filepath.Dir(resolved))

	// Strategy 1: vendor/ directly in the package root.
	if bin, pd := findVendorBinary(filepath.Join(pkgDir, "vendor")); bin != "" {
		return bin, pd, nil
	}

	// Strategy 2: platform-specific npm sub-package.
	// e.g. <pkg>/node_modules/@openai/codex-win32-x64/vendor/<triple>/...
	if platPkg := platformPackageName(); platPkg != "" {
		subVendor := filepath.Join(pkgDir, "node_modules", platPkg, "vendor")
		if bin, pd := findVendorBinary(subVendor); bin != "" {
			return bin, pd, nil
		}
	}

	// Strategy 3: one level up (wrapper directly in the package).
	if bin, pd := findVendorBinary(filepath.Join(filepath.Dir(resolved), "..", "vendor")); bin != "" {
		return bin, pd, nil
	}

	return "", "", fmt.Errorf("native binary not found for %s (looked in %s)", triple, pkgDir)
}

// PrepareYoloBinary finds the native Codex binary, patches it for yolo mode
// (permissive system requirements), and returns the patched binary path plus
// any extra environment variables.
func PrepareYoloBinary(codexWrapperPath string, cacheDir string) (*PatchResult, []string, error) {
	nativeBin, pathDir, err := FindNativeBinary(codexWrapperPath)
	if err != nil {
		return nil, nil, err
	}

	result, err := PatchCodexBinary(nativeBin, cacheDir)
	if err != nil {
		return nil, nil, fmt.Errorf("patch binary: %w", err)
	}

	var extraEnv []string
	if pathDir != "" {
		// Prepend the vendor path directory to PATH, mirroring the Node.js wrapper.
		currentPath := os.Getenv("PATH")
		extraEnv = append(extraEnv, "PATH="+pathDir+string(os.PathListSeparator)+currentPath)
	}
	// The Node.js wrapper sets this; some codex features may check for it.
	extraEnv = append(extraEnv, "CODEX_MANAGED_BY_NPM=1")

	return result, extraEnv, nil
}
