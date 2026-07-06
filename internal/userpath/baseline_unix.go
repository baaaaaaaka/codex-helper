//go:build !windows

package userpath

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

var baselineReadFile = os.ReadFile

func accountProbeEnvironment(service []string, target TargetIdentity, shell string) ([]string, string) {
	pathValue, source := defaultBaselinePath(service, target)
	out := []string{
		"HOME=" + target.Home,
		"USER=" + target.Username,
		"LOGNAME=" + target.Username,
		"SHELL=" + shell,
		"PATH=" + pathValue,
		"TERM=dumb",
	}
	for _, entry := range service {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !stableAccountVariable(key, value, target.Home) {
			continue
		}
		out = setEnvironmentValue(out, key, value)
	}
	return out, source
}

func stableAccountVariable(key, value, home string) bool {
	if key == "LANG" || key == "TZ" || key == "COLORTERM" || strings.HasPrefix(key, "LC_") || strings.HasPrefix(key, "WSL") {
		return true
	}
	switch key {
	case "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME", "ZDOTDIR":
		if value == "" || !filepath.IsAbs(value) {
			return false
		}
		rel, err := filepath.Rel(filepath.Clean(home), filepath.Clean(value))
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
	default:
		return false
	}
}

func defaultBaselinePath(service []string, target TargetIdentity) (string, string) {
	inherited, _ := EnvironmentValue(service, "PATH", false)
	if target.WSLDistro != "" || environmentPresent(service, "WSL_DISTRO_NAME") || environmentPresent(service, "WSL_INTEROP") {
		base := "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
		return appendWSLInteropPath(base, sanitizeInheritedPath(inherited, service)), "wsl-login-default+interop"
	}
	if runtime.GOOS == "darwin" {
		return "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin", "darwin-login-default"
	}
	return "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "unix-login-default"
}

func appendWSLInteropPath(base, inherited string) string {
	automountRoot := wslAutomountRoot()
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, entry := range filepath.SplitList(base) {
		clean := filepath.Clean(entry)
		if clean != "." && !seen[clean] {
			seen[clean] = true
			out = append(out, entry)
		}
	}
	for _, entry := range filepath.SplitList(inherited) {
		clean := filepath.Clean(entry)
		if entry == "" || seen[clean] || !isWSLInteropPathWithRoot(clean, automountRoot) {
			continue
		}
		seen[clean] = true
		out = append(out, entry)
	}
	return strings.Join(out, string(os.PathListSeparator))
}

func isWSLInteropPath(path string) bool {
	return isWSLInteropPathWithRoot(path, wslAutomountRoot())
}

func isWSLInteropPathWithRoot(path string, automountRoot string) bool {
	slashed := filepath.ToSlash(filepath.Clean(path))
	switch slashed {
	case "/usr/games", "/usr/local/games", "/snap/bin":
		return true
	}
	if strings.HasPrefix(slashed, "/usr/lib/wsl/") || slashed == "/mnt/wsl" || strings.HasPrefix(slashed, "/mnt/wsl/") {
		return true
	}
	root := filepath.ToSlash(filepath.Clean(automountRoot))
	if root == "." || !filepath.IsAbs(root) {
		root = "/mnt"
	}
	var rest string
	if root == "/" {
		if !strings.HasPrefix(slashed, "/") {
			return false
		}
		rest = strings.TrimPrefix(slashed, "/")
	} else {
		prefix := strings.TrimSuffix(root, "/") + "/"
		if !strings.HasPrefix(slashed, prefix) {
			return false
		}
		rest = strings.TrimPrefix(slashed, prefix)
	}
	parts := strings.SplitN(rest, "/", 2)
	return len(parts) == 2 && len(parts[0]) == 1 && ((parts[0][0] >= 'a' && parts[0][0] <= 'z') || (parts[0][0] >= 'A' && parts[0][0] <= 'Z'))
}

func wslAutomountRoot() string {
	const fallback = "/mnt"
	data, err := baselineReadFile("/etc/wsl.conf")
	if err != nil {
		return fallback
	}
	inAutomount := false
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inAutomount = strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")), "automount")
			continue
		}
		if !inAutomount {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "root") {
			continue
		}
		value = strings.Trim(strings.TrimSpace(trimINIInlineComment(value)), "\"'")
		if filepath.IsAbs(value) {
			return filepath.Clean(value)
		}
		return fallback
	}
	return fallback
}

func trimINIInlineComment(value string) string {
	quote := rune(0)
	for index, char := range value {
		switch {
		case quote != 0 && char == quote:
			quote = 0
		case quote == 0 && (char == '\'' || char == '"'):
			quote = char
		case quote == 0 && (char == '#' || char == ';') && (index == 0 || value[index-1] == ' ' || value[index-1] == '\t'):
			return strings.TrimSpace(value[:index])
		}
	}
	return strings.TrimSpace(value)
}

func sanitizeInheritedPath(pathValue string, environment []string) string {
	return filterVolatilePath(pathValue, environment, true)
}

func sanitizeResolvedPath(pathValue string, environment []string) string {
	return filterVolatilePath(pathValue, environment, false)
}

func filterVolatilePath(pathValue string, environment []string, deduplicate bool) string {
	blocked := map[string]bool{}
	for _, key := range []string{"CODEX_NODE_INSTALL_ROOT", "CODEX_MANAGED_PACKAGE_ROOT", "CXP_RUNTIME_ROOT"} {
		if value, ok := EnvironmentValue(environment, key, false); ok && strings.TrimSpace(value) != "" {
			blocked[filepath.Clean(value)] = true
			blocked[filepath.Clean(filepath.Join(value, "bin"))] = true
		}
	}
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, entry := range filepath.SplitList(pathValue) {
		if entry == "" {
			if !deduplicate {
				out = append(out, entry)
			}
			continue
		}
		clean := filepath.Clean(entry)
		slashed := filepath.ToSlash(clean)
		if pathWithinBlockedRoot(clean, blocked) ||
			strings.Contains(slashed, "/.cache/codex-proxy/node/") ||
			strings.Contains(slashed, "/.local/bin/.cxp-runtime/") ||
			strings.Contains(slashed, "/.codex/tmp/arg0/") ||
			(strings.Contains(slashed, "/node_modules/@openai/") && strings.HasSuffix(slashed, "/codex-path")) {
			continue
		}
		if deduplicate && seen[clean] {
			continue
		}
		seen[clean] = true
		out = append(out, entry)
	}
	return strings.Join(out, string(os.PathListSeparator))
}

func pathWithinBlockedRoot(path string, blocked map[string]bool) bool {
	for root := range blocked {
		if path == root {
			return true
		}
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return true
		}
	}
	return false
}

func environmentPresent(environment []string, key string) bool {
	value, ok := EnvironmentValue(environment, key, false)
	return ok && strings.TrimSpace(value) != ""
}

func setEnvironmentValue(environment []string, key, value string) []string {
	out := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		entryKey, _, ok := strings.Cut(entry, "=")
		if ok && entryKey == key {
			continue
		}
		out = append(out, entry)
	}
	return append(out, key+"="+value)
}
