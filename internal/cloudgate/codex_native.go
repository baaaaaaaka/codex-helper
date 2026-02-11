package cloudgate

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
	}
	return ""
}

// FindNativeBinary locates the native Codex binary given the path to the
// codex Node.js wrapper (e.g. /home/user/.npm-global/bin/codex).
// It resolves symlinks and finds the vendor/<triple>/codex/codex binary.
func FindNativeBinary(codexWrapperPath string) (nativeBin string, pathDir string, err error) {
	triple := targetTriple()
	if triple == "" {
		return "", "", fmt.Errorf("unsupported platform: %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	// The wrapper is at <prefix>/bin/codex.js or <prefix>/bin/codex.
	// Resolve symlinks to find the real location.
	resolved, err := filepath.EvalSymlinks(codexWrapperPath)
	if err != nil {
		return "", "", fmt.Errorf("eval symlinks: %w", err)
	}

	// The wrapper script is at <pkg>/bin/codex.js
	// The native binary is at <pkg>/vendor/<triple>/codex/codex
	pkgDir := filepath.Dir(filepath.Dir(resolved))

	// Try both with and without .js extension.
	nativeBin = filepath.Join(pkgDir, "vendor", triple, "codex", "codex")
	if _, err := os.Stat(nativeBin); err != nil {
		// Try one level up (in case wrapper is directly in the package).
		nativeBin = filepath.Join(filepath.Dir(resolved), "..", "vendor", triple, "codex", "codex")
		if _, err := os.Stat(nativeBin); err != nil {
			return "", "", fmt.Errorf("native binary not found for %s (looked in %s)", triple, pkgDir)
		}
	}

	pathDir = filepath.Join(pkgDir, "vendor", triple, "path")
	if _, err := os.Stat(pathDir); err != nil {
		pathDir = "" // Optional; doesn't always exist.
	}

	return nativeBin, pathDir, nil
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
		extraEnv = append(extraEnv, "PATH="+pathDir+":"+currentPath)
	}
	// The Node.js wrapper sets this; some codex features may check for it.
	extraEnv = append(extraEnv, "CODEX_MANAGED_BY_NPM=1")

	return result, extraEnv, nil
}
