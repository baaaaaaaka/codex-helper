package appdirs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	AppName     = "codex-helper"
	EnvStateDir = "CODEX_HELPER_STATE_DIR"
)

// StateDir returns the durable state root for codex-helper.
//
// CODEX_HELPER_STATE_DIR is treated as an exact override. Otherwise the path
// honors XDG_STATE_HOME when set and uses a non-cache config area on platforms
// without a standard Go state directory API.
func StateDir() (string, error) {
	if override := strings.TrimSpace(os.Getenv(EnvStateDir)); override != "" {
		return filepath.Clean(expandHome(override)), nil
	}
	if base := strings.TrimSpace(os.Getenv("XDG_STATE_HOME")); base != "" {
		return filepath.Join(expandHome(base), AppName), nil
	}
	if runtime.GOOS != "windows" && runtime.GOOS != "darwin" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("get user home dir: %w", err)
		}
		if strings.TrimSpace(home) == "" {
			return "", fmt.Errorf("get user home dir: empty path")
		}
		return filepath.Join(home, ".local", "state", AppName), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config dir: %w", err)
	}
	return filepath.Join(base, AppName, "state"), nil
}

func StatePath(parts ...string) (string, error) {
	root, err := StateDir()
	if err != nil {
		return "", err
	}
	all := make([]string, 0, len(parts)+1)
	all = append(all, root)
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		all = append(all, part)
	}
	return filepath.Join(all...), nil
}

func LegacyCachePath(parts ...string) (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("get user cache dir: %w", err)
	}
	return legacyPath(base, parts...), nil
}

func LegacyConfigPath(parts ...string) (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config dir: %w", err)
	}
	return legacyPath(base, parts...), nil
}

func legacyPath(base string, parts ...string) string {
	all := make([]string, 0, len(parts)+2)
	all = append(all, base, AppName)
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		all = append(all, part)
	}
	return filepath.Join(all...)
}

func expandHome(path string) string {
	path = strings.TrimSpace(path)
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
			return home
		}
		return path
	}
	if strings.HasPrefix(path, "~/") || strings.HasPrefix(path, `~\`) {
		if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}
