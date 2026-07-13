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
	if !launch.Native && opts.Platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() {
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
	codexHome, err := writeCodexDesktopModelProfileConfig(store, desktopLaunch, opts.Platform, opts.CodexHomeSource)
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
	if !launch.Native {
		opts.ExtraEnv = withLoopbackNoProxyEnv(append(opts.ExtraEnv, launch.effectiveEnvKey()+"="+launch.ProxyKey))
	}
	opts.ModelProfileName = launch.Name
	if opts.Log != nil {
		if launch.Native {
			_, _ = fmt.Fprintf(opts.Log, "using global official defaults for Codex desktop app\n")
		} else {
			_, _ = fmt.Fprintf(opts.Log, "using model profile %q for Codex desktop app via %s\n", launch.Name, desktopLaunch.BaseURL)
		}
	}
	return opts, nil
}

func writeCodexDesktopModelProfileConfig(store *config.Store, launch codexModelProfileLaunch, platform codexDesktopPlatform, sourceCodexHome ...string) (string, error) {
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
	source := ""
	if launch.Unified || launch.Native {
		if len(sourceCodexHome) > 0 {
			source = strings.TrimSpace(sourceCodexHome[0])
		}
		if err := copyCodexOfficialAuthToProfileHome(source, codexHome); err != nil {
			return "", err
		}
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
	webSearchFallbackConfigPath := strings.TrimSpace(launch.WebSearchFallbackPath)
	webSearchFallbackTOML := launch.WebSearchFallbackTOML
	if launch.DisableHostedWebSearch && len(webSearchFallbackTOML) == 0 {
		webSearchFallbackTOML = codexWebSearchFallbackRoleConfigTOML()
	}
	if len(webSearchFallbackTOML) > 0 {
		webSearchFallbackPath := filepath.Join(codexHome, codexWebSearchFallbackConfigName)
		if err := os.WriteFile(webSearchFallbackPath, webSearchFallbackTOML, 0o600); err != nil {
			return "", err
		}
		webSearchFallbackConfigPath = webSearchFallbackPath
		if platform == codexDesktopPlatformWindows && codexAppGOOS() == "linux" && codexAppIsWSL() {
			converted, err := codexAppWSLPathFn(webSearchFallbackPath)
			if err != nil {
				return "", fmt.Errorf("convert web search fallback config path for Windows launch: %w", err)
			}
			if strings.TrimSpace(converted) != "" {
				webSearchFallbackConfigPath = converted
			}
		}
	}
	generatedConfig := codexDesktopModelProfileConfigTOML(launch, catalogConfigPath, webSearchFallbackConfigPath)
	if launch.Unified || launch.Native {
		inheritedConfig, err := inheritedCodexDesktopModelProfileConfig(source, generatedConfig, launch)
		if err != nil {
			return "", err
		}
		generatedConfig = inheritedConfig
	}
	configPath := filepath.Join(codexHome, "config.toml")
	if err := os.WriteFile(configPath, []byte(generatedConfig), 0o600); err != nil {
		return "", err
	}
	return codexHome, nil
}

func copyCodexOfficialAuthToProfileHome(sourceCodexHome string, profileCodexHome string) error {
	sourceCodexHome = strings.TrimSpace(sourceCodexHome)
	if sourceCodexHome == "" {
		return fmt.Errorf("official Codex home is unavailable; run `cxp app auth` before launching unified models")
	}
	source := filepath.Join(sourceCodexHome, "auth.json")
	raw, err := os.ReadFile(source)
	if err != nil {
		if os.IsNotExist(err) {
			// Codex may keep credentials in the OS credential store instead of
			// auth.json. The inherited config retains that store selection.
			return nil
		}
		return fmt.Errorf("read official Codex authentication: %w", err)
	}
	destination := filepath.Join(profileCodexHome, "auth.json")
	if err := writeAtomicPrivateFile(destination, raw); err != nil {
		return fmt.Errorf("copy official Codex authentication into unified App profile: %w", err)
	}
	return nil
}

func inheritedCodexDesktopModelProfileConfig(sourceCodexHome, generated string, launches ...codexModelProfileLaunch) (string, error) {
	sourceCodexHome = strings.TrimSpace(sourceCodexHome)
	if sourceCodexHome == "" {
		return "", fmt.Errorf("official Codex home is unavailable; run `cxp app auth` before launching unified models")
	}
	raw, err := os.ReadFile(filepath.Join(sourceCodexHome, "config.toml"))
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read official Codex config: %w", err)
	}
	if len(raw) == 0 {
		return generated, nil
	}
	// Preserve user settings and credential-store selection while removing only
	// fields owned by the generated unified profile. This intentionally avoids
	// copying history/state databases into the isolated App home.
	ownedSections := map[string]bool{
		"model_providers." + cxpUnifiedCodexModelProviderID: true,
		"features.multi_agent_v2":                           true,
		"agents." + codexWebSearchFallbackAgentName:         true,
	}
	ownedTopLevel := map[string]bool{
		"model": true, "model_provider": true, "model_catalog_json": true, "model_reasoning_effort": true, "web_search": true,
	}
	if len(launches) > 0 && launches[0].Native {
		launch := launches[0]
		ownedSections = map[string]bool{
			"model_providers." + cxpUnifiedCodexModelProviderID: true,
		}
		ownedTopLevel = map[string]bool{}
		if strings.TrimSpace(launch.Model) != "" {
			// An explicit official model must not inherit an incompatible custom
			// provider, catalog, or reasoning override.
			ownedTopLevel["model"] = true
			ownedTopLevel["model_provider"] = true
			ownedTopLevel["model_catalog_json"] = true
			ownedTopLevel["model_reasoning_effort"] = true
		}
		if strings.TrimSpace(launch.ReasoningEffort) != "" {
			ownedTopLevel["model_reasoning_effort"] = true
		}
	}
	lines := strings.Split(string(raw), "\n")
	topLevel := make([]string, 0, len(lines))
	sections := make([]string, 0, len(lines))
	inSection := false
	skipSection := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			section := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]"))
			inSection = true
			skipSection = ownedSections[section]
			if skipSection {
				continue
			}
		}
		if skipSection {
			continue
		}
		if !inSection {
			if key, _, ok := strings.Cut(trimmed, "="); ok && ownedTopLevel[strings.TrimSpace(key)] {
				continue
			}
		}
		if inSection {
			sections = append(sections, line)
		} else {
			topLevel = append(topLevel, line)
		}
	}
	parts := make([]string, 0, 3)
	if value := strings.TrimSpace(strings.Join(topLevel, "\n")); value != "" {
		parts = append(parts, value)
	}
	parts = append(parts, strings.TrimSpace(generated))
	if value := strings.TrimSpace(strings.Join(sections, "\n")); value != "" {
		parts = append(parts, value)
	}
	return strings.Join(parts, "\n\n") + "\n", nil
}

func codexDesktopModelProfileConfigTOML(launch codexModelProfileLaunch, catalogPath string, webSearchFallbackPath ...string) string {
	if launch.Native {
		lines := []string{}
		if strings.TrimSpace(launch.Model) != "" {
			lines = append(lines, `model = "`+tomlEscapeString(launch.Model)+`"`)
		}
		if strings.TrimSpace(launch.ReasoningEffort) != "" {
			lines = append(lines, `model_reasoning_effort = "`+tomlEscapeString(launch.ReasoningEffort)+`"`)
		}
		return strings.Join(lines, "\n") + "\n"
	}
	providerName := "CXP " + launch.ProviderName
	if strings.TrimSpace(launch.ProviderName) == "" {
		providerName = "CXP third-party"
	}
	providerID := cxpCodexModelProviderID
	if launch.Unified {
		providerID = cxpUnifiedCodexModelProviderID
		providerName = "CXP Unified models"
	}
	lines := []string{`model_provider = "` + providerID + `"`}
	if strings.TrimSpace(launch.Model) != "" {
		lines = append(lines, `model = "`+tomlEscapeString(launch.Model)+`"`)
	}
	if strings.TrimSpace(launch.ReasoningEffort) != "" {
		lines = append(lines, `model_reasoning_effort = "`+tomlEscapeString(launch.ReasoningEffort)+`"`)
	}
	if launch.DisableHostedWebSearch {
		lines = append(lines, `web_search = "disabled"`)
	}
	if strings.TrimSpace(catalogPath) != "" {
		lines = append(lines, `model_catalog_json = "`+tomlEscapeString(catalogPath)+`"`)
	}
	configuredWebSearchFallbackPath := ""
	if len(webSearchFallbackPath) > 0 {
		configuredWebSearchFallbackPath = strings.TrimSpace(webSearchFallbackPath[0])
	}
	if launch.DisableHostedWebSearch && configuredWebSearchFallbackPath != "" {
		lines = append(lines,
			"",
			`[features.multi_agent_v2]`,
			`enabled = true`,
			`hide_spawn_agent_metadata = false`,
			`root_agent_usage_hint_text = "`+tomlEscapeString(codexWebSearchFallbackRootHint)+`"`,
			"",
			`[agents.`+codexWebSearchFallbackAgentName+`]`,
			`description = "`+tomlEscapeString(codexWebSearchFallbackAgentDescription)+`"`,
			`config_file = "`+tomlEscapeString(configuredWebSearchFallbackPath)+`"`,
		)
	}
	lines = append(lines, "", `[model_providers.`+providerID+`]`, `name = "`+tomlEscapeString(providerName)+`"`, `base_url = "`+tomlEscapeString(launch.BaseURL)+`"`)
	if launch.Unified {
		lines = append(lines,
			`wire_api = "responses"`,
			`requires_openai_auth = true`,
			`supports_websockets = false`,
			`env_http_headers = { "`+cxpUnifiedGatewayHeader+`" = "`+launch.effectiveEnvKey()+`" }`,
		)
	} else {
		lines = append(lines,
			`env_key = "`+launch.effectiveEnvKey()+`"`,
			`wire_api = "responses"`,
			`requires_openai_auth = false`,
			`supports_websockets = false`,
		)
	}
	lines = append(lines, "")
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
