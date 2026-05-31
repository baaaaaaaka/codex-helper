package cli

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

var codexAppWSLHostForWindowsFn = defaultCodexAppWSLHostForWindows

func applyCodexDesktopModelProfileLaunch(store *config.Store, opts codexDesktopAppOptions, launch codexModelProfileLaunch) (codexDesktopAppOptions, error) {
	if store == nil || !launch.Enabled {
		return opts, nil
	}
	desktopLaunch := launch
	if opts.Platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() {
		host, err := codexAppWSLHostForWindowsFn()
		if err != nil {
			return opts, fmt.Errorf("resolve WSL host for Windows Codex desktop app: %w", err)
		}
		if strings.TrimSpace(host) != "" {
			desktopLaunch.BaseURL, err = modelProfileBaseURLForHost(launch.BaseURL, host)
			if err != nil {
				return opts, fmt.Errorf("rewrite model profile adapter URL for Windows Codex desktop app: %w", err)
			}
		}
	}
	codexHome, err := writeCodexDesktopModelProfileConfig(store, desktopLaunch, opts.Platform)
	if err != nil {
		return opts, err
	}
	launchCodexHome := codexHome
	if opts.Platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() {
		converted, err := codexAppWSLPathFn(codexHome)
		if err != nil {
			return opts, fmt.Errorf("convert model profile Codex home for Windows launch: %w", err)
		}
		if strings.TrimSpace(converted) != "" {
			launchCodexHome = converted
		}
	}
	opts.ExtraEnv = replaceCodexHomeEnv(opts.ExtraEnv, launchCodexHome)
	opts.ExtraEnv = append(opts.ExtraEnv, envCXPResponsesProxyKey+"="+launch.ProxyKey)
	opts.ModelProfileName = launch.Name
	if opts.Log != nil {
		_, _ = fmt.Fprintf(opts.Log, "using model profile %q for Codex desktop app via %s\n", launch.Name, desktopLaunch.BaseURL)
	}
	return opts, nil
}

func writeCodexDesktopModelProfileConfig(store *config.Store, launch codexModelProfileLaunch, platform codexDesktopPlatform) (string, error) {
	name := safeModelProfilePathPart(launch.Name)
	if name == "" {
		name = "profile"
	}
	dirName := fmt.Sprintf("%s-rev%d", name, launch.Revision)
	if launch.Revision <= 0 {
		dirName = name
	}
	codexHome := filepath.Join(filepath.Dir(store.Path()), "model-profiles", dirName, "codex")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		return "", err
	}
	catalogConfigPath := strings.TrimSpace(launch.CatalogPath)
	if len(launch.CatalogJSON) > 0 {
		catalogPath := filepath.Join(codexHome, "catalog.json")
		if err := os.WriteFile(catalogPath, launch.CatalogJSON, 0o600); err != nil {
			return "", err
		}
		catalogConfigPath = catalogPath
		if platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() {
			converted, err := codexAppWSLPathFn(catalogPath)
			if err != nil {
				return "", fmt.Errorf("convert model profile catalog path for Windows launch: %w", err)
			}
			if strings.TrimSpace(converted) != "" {
				catalogConfigPath = converted
			}
		}
	}
	configPath := filepath.Join(codexHome, "config.toml")
	if err := os.WriteFile(configPath, []byte(codexDesktopModelProfileConfigTOML(launch, catalogConfigPath)), 0o600); err != nil {
		return "", err
	}
	return codexHome, nil
}

func codexDesktopModelProfileConfigTOML(launch codexModelProfileLaunch, catalogPath string) string {
	providerName := "CXP " + launch.ProviderName
	if strings.TrimSpace(launch.ProviderName) == "" {
		providerName = "CXP third-party"
	}
	lines := []string{
		`model_provider = "` + cxpCodexModelProviderID + `"`,
		`model = "` + tomlEscapeString(launch.Model) + `"`,
	}
	if strings.TrimSpace(catalogPath) != "" {
		lines = append(lines, `model_catalog_json = "`+tomlEscapeString(catalogPath)+`"`)
	}
	lines = append(lines,
		"",
		`[model_providers.`+cxpCodexModelProviderID+`]`,
		`name = "`+tomlEscapeString(providerName)+`"`,
		`base_url = "`+tomlEscapeString(launch.BaseURL)+`"`,
		`env_key = "`+envCXPResponsesProxyKey+`"`,
		`wire_api = "responses"`,
		`requires_openai_auth = false`,
		`supports_websockets = false`,
		"",
	)
	return strings.Join(lines, "\n")
}

func replaceCodexHomeEnv(extra []string, codexHome string) []string {
	out := make([]string, 0, len(extra)+2)
	for _, item := range extra {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			out = append(out, item)
			continue
		}
		if key == envCodexHome || key == codexhistory.EnvCodexDir {
			continue
		}
		out = append(out, item)
	}
	out = append(out, codexHomeEnv(codexHome)...)
	return out
}

func safeModelProfilePathPart(raw string) string {
	raw = strings.TrimSpace(raw)
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		case b.Len() > 0:
			b.WriteByte('-')
		}
		if b.Len() >= 80 {
			break
		}
	}
	return strings.Trim(b.String(), "-.")
}

func modelProfileBaseURLForHost(baseURL string, host string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return baseURL, nil
	}
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid base URL %q", baseURL)
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", err
	}
	u.Host = net.JoinHostPort(host, port)
	return u.String(), nil
}

func defaultCodexAppWSLHostForWindows() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet == nil {
				continue
			}
			ip := ipNet.IP.To4()
			if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
				continue
			}
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("no non-loopback IPv4 address found")
}
