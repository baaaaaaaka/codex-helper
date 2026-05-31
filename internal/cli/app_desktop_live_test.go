package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestLiveCodexDesktopWindowsModelProfileFromWSLOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_MODEL_PROFILE")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_DESKTOP_MODEL_PROFILE=1 to launch the Windows Codex desktop app with a live model profile from WSL")
	}
	if runtime.GOOS != "linux" || !teamsServiceIsWSL() {
		t.Skip("Windows desktop live model-profile test requires WSL")
	}
	cxpPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_CXP_PATH"))
	if cxpPath == "" {
		t.Fatal("CODEX_HELPER_LIVE_DESKTOP_CXP_PATH is required")
	}
	if strings.TrimSpace(os.Getenv("MIMO_API_KEY")) == "" {
		t.Fatal("MIMO_API_KEY is required")
	}
	powershell := liveDesktopPowerShellPath(t)
	appPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_APP_PATH"))
	if appPath == "" {
		appPath = liveDesktopCodexAppPath(t, powershell)
	}

	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "config.json")
	workDir := filepath.Join(tmp, "work")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatalf("mkdir work dir: %v", err)
	}
	env := appendWindowsToolPath(os.Environ(), powershell)
	setup := exec.Command(cxpPath, "--config", configPath, "model-profile", "setup", "mimo25-live", "--provider", "mimo", "--api-key-env", "MIMO_API_KEY", "--no-doctor")
	setup.Env = env
	if out, err := setup.CombinedOutput(); err != nil {
		t.Fatalf("model-profile setup failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	launch := exec.Command(cxpPath, "--config", configPath, "app", "--model-profile", "mimo25-live", "--cwd", workDir, "--app-path", appPath)
	launch.Env = env
	if out, err := launch.CombinedOutput(); err != nil {
		t.Fatalf("desktop app launch failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	cfg := readLiveDesktopConfig(t, configPath)
	inst := liveDesktopModelAdapterInstance(t, cfg)
	t.Cleanup(func() {
		stop := exec.Command(cxpPath, "--config", configPath, "proxy", "stop", inst.ID)
		stop.Env = env
		_ = stop.Run()
	})
	if inst.HTTPPort <= 0 || strings.TrimSpace(inst.ModelProxyKey) == "" {
		t.Fatalf("model adapter instance missing port/key: %#v", inst)
	}
	baseURL := readLiveDesktopModelBaseURL(t, filepath.Join(tmp, "model-profiles", "mimo25-live-rev1", "codex", "config.toml"))
	if strings.Contains(baseURL, "127.0.0.1") || strings.Contains(baseURL, "localhost") {
		t.Fatalf("desktop model profile base URL is not Windows-reachable from WSL: %s", baseURL)
	}
	if !liveDesktopWaitForWindowsModels(t, powershell, baseURL, inst.ModelProxyKey) {
		t.Fatalf("Windows could not reach model adapter at %s", baseURL)
	}
}

func TestLiveCodexDesktopGUIChatModelProfileOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_GUI_CHAT")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_DESKTOP_GUI_CHAT=1 to launch Codex desktop and drive one GUI chat through a live model profile")
	}
	cxpPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_CXP_PATH"))
	if cxpPath == "" {
		t.Fatal("CODEX_HELPER_LIVE_DESKTOP_CXP_PATH is required")
	}
	if strings.TrimSpace(os.Getenv("MIMO_API_KEY")) == "" {
		t.Fatal("MIMO_API_KEY is required")
	}
	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "config.json")
	workDir := filepath.Join(tmp, "work")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatalf("mkdir work dir: %v", err)
	}

	env := os.Environ()
	var powershell string
	var platform string
	appPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_APP_PATH"))
	switch {
	case runtime.GOOS == "linux" && teamsServiceIsWSL():
		platform = "windows"
		powershell = liveDesktopPowerShellPath(t)
		env = appendWindowsToolPath(env, powershell)
		if appPath == "" {
			appPath = liveDesktopCodexAppPath(t, powershell)
		}
	case runtime.GOOS == "darwin":
		platform = "macos"
	default:
		t.Skip("desktop GUI live chat test currently supports WSL->Windows and macOS hosts")
	}

	setupLiveDesktopMimoModelProfile(t, cxpPath, configPath, env)
	args := []string{"--config", configPath, "app", "--model-profile", "mimo25-live", "--cwd", workDir}
	if appPath != "" {
		args = append(args, "--app-path", appPath)
	}
	launch := exec.Command(cxpPath, args...)
	launch.Env = env
	if out, err := launch.CombinedOutput(); err != nil {
		t.Fatalf("desktop app launch failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	cfg := readLiveDesktopConfig(t, configPath)
	inst := liveDesktopModelAdapterInstance(t, cfg)
	t.Cleanup(func() {
		stop := exec.Command(cxpPath, "--config", configPath, "proxy", "stop", inst.ID)
		stop.Env = env
		_ = stop.Run()
	})
	if inst.HTTPPort <= 0 || strings.TrimSpace(inst.ModelProxyKey) == "" {
		t.Fatalf("model adapter instance missing port/key: %#v", inst)
	}
	codexHome := filepath.Join(tmp, "model-profiles", "mimo25-live-rev1", "codex")
	baseURL := readLiveDesktopModelBaseURL(t, filepath.Join(codexHome, "config.toml"))
	switch platform {
	case "windows":
		if strings.Contains(baseURL, "127.0.0.1") || strings.Contains(baseURL, "localhost") {
			t.Fatalf("desktop model profile base URL is not Windows-reachable from WSL: %s", baseURL)
		}
		if !liveDesktopWaitForWindowsModels(t, powershell, baseURL, inst.ModelProxyKey) {
			t.Fatalf("Windows could not reach model adapter at %s", baseURL)
		}
	case "macos":
		if !liveDesktopWaitForHostModels(t, baseURL, inst.ModelProxyKey) {
			t.Fatalf("macOS host could not reach model adapter at %s", baseURL)
		}
	}

	marker := fmt.Sprintf("CXP_DESKTOP_GUI_%d", time.Now().UnixNano())
	prompt := "Reply with exactly this marker on its own line and no extra explanation: " + marker
	switch platform {
	case "windows":
		liveDesktopSendPromptWindows(t, powershell, prompt)
	case "macos":
		liveDesktopSendPromptMac(t, prompt)
	}
	timeout := liveDesktopGUIChatTimeout(t)
	if !liveDesktopWaitForCodexHomeMarker(t, codexHome, marker, timeout) {
		t.Fatalf("desktop GUI chat marker %s was not observed under %s within %s", marker, codexHome, timeout)
	}
}

func TestLiveCodexDesktopMacModelProfileFromHostOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_MAC_MODEL_PROFILE")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_DESKTOP_MAC_MODEL_PROFILE=1 to launch the macOS Codex desktop app with a live model profile")
	}
	if runtime.GOOS != "darwin" {
		t.Skip("macOS desktop live model-profile test requires a macOS host")
	}
	cxpPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_CXP_PATH"))
	if cxpPath == "" {
		t.Fatal("CODEX_HELPER_LIVE_DESKTOP_CXP_PATH is required")
	}
	if strings.TrimSpace(os.Getenv("MIMO_API_KEY")) == "" {
		t.Fatal("MIMO_API_KEY is required")
	}
	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "config.json")
	workDir := filepath.Join(tmp, "work")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatalf("mkdir work dir: %v", err)
	}
	env := os.Environ()
	setupLiveDesktopMimoModelProfile(t, cxpPath, configPath, env)
	args := []string{"--config", configPath, "app", "--model-profile", "mimo25-live", "--cwd", workDir}
	if appPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_APP_PATH")); appPath != "" {
		args = append(args, "--app-path", appPath)
	}
	launch := exec.Command(cxpPath, args...)
	launch.Env = env
	if out, err := launch.CombinedOutput(); err != nil {
		t.Fatalf("desktop app launch failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	cfg := readLiveDesktopConfig(t, configPath)
	inst := liveDesktopModelAdapterInstance(t, cfg)
	t.Cleanup(func() {
		stop := exec.Command(cxpPath, "--config", configPath, "proxy", "stop", inst.ID)
		stop.Env = env
		_ = stop.Run()
	})
	baseURL := readLiveDesktopModelBaseURL(t, filepath.Join(tmp, "model-profiles", "mimo25-live-rev1", "codex", "config.toml"))
	if !strings.Contains(baseURL, "127.0.0.1") && !strings.Contains(baseURL, "localhost") {
		t.Fatalf("macOS model profile base URL should stay host-local, got %s", baseURL)
	}
	if !liveDesktopWaitForHostModels(t, baseURL, inst.ModelProxyKey) {
		t.Fatalf("macOS host could not reach model adapter at %s", baseURL)
	}
}

func setupLiveDesktopMimoModelProfile(t *testing.T, cxpPath string, configPath string, env []string) {
	t.Helper()
	setup := exec.Command(cxpPath, "--config", configPath, "model-profile", "setup", "mimo25-live", "--provider", "mimo", "--api-key-env", "MIMO_API_KEY", "--no-doctor")
	setup.Env = env
	if out, err := setup.CombinedOutput(); err != nil {
		t.Fatalf("model-profile setup failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
}

func liveDesktopPowerShellPath(t *testing.T) string {
	t.Helper()
	candidates := []string{
		strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_POWERSHELL")),
		"/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
	}
	for _, path := range candidates {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	t.Fatal("powershell.exe was not found under /mnt/c/Windows/System32")
	return ""
}

func liveDesktopCodexAppPath(t *testing.T, powershell string) string {
	t.Helper()
	cmd := exec.Command(powershell, "-NoProfile", "-NonInteractive", "-Command", `(Get-AppxPackage -Name OpenAI.Codex | Sort-Object Version -Descending | Select-Object -First 1).InstallLocation`)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("query OpenAI.Codex package failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	winPath := strings.TrimSpace(strings.ReplaceAll(string(out), "\r", ""))
	if winPath == "" {
		t.Fatal("OpenAI.Codex package is not installed")
	}
	converted, err := exec.Command("wslpath", "-u", winPath).CombinedOutput()
	if err != nil {
		t.Fatalf("convert Codex app path failed: %v\n%s", err, strings.TrimSpace(string(converted)))
	}
	return filepath.Join(strings.TrimSpace(string(converted)), "app", "Codex.exe")
}

func appendWindowsToolPath(env []string, powershell string) []string {
	powerShellDir := filepath.Dir(powershell)
	system32 := "/mnt/c/Windows/System32"
	out := make([]string, 0, len(env)+1)
	replaced := false
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key == "PATH" {
			out = append(out, "PATH="+value+":"+powerShellDir+":"+system32)
			replaced = true
			continue
		}
		out = append(out, entry)
	}
	if !replaced {
		out = append(out, "PATH="+powerShellDir+":"+system32)
	}
	return out
}

func readLiveDesktopConfig(t *testing.T, path string) config.Config {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg config.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	return cfg
}

func liveDesktopModelAdapterInstance(t *testing.T, cfg config.Config) config.Instance {
	t.Helper()
	for _, inst := range cfg.Instances {
		if inst.Kind == config.InstanceKindModelAdapter && inst.ModelProfileName == "mimo25-live" {
			return inst
		}
	}
	t.Fatalf("model adapter instance not found: %#v", cfg.Instances)
	return config.Instance{}
}

func readLiveDesktopModelBaseURL(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read desktop model config: %v", err)
	}
	validateLiveDesktopModelConfig(t, path, string(raw))
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "base_url = ") {
			return strings.Trim(strings.TrimPrefix(line, "base_url = "), `"`)
		}
	}
	t.Fatalf("base_url not found in %s:\n%s", path, raw)
	return ""
}

func liveDesktopWaitForWindowsModels(t *testing.T, powershell string, baseURL string, proxyKey string) bool {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	script := `$headers=@{Authorization='Bearer ` + powershellSingleQuoteContent(proxyKey) + `'}; (Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri '` + powershellSingleQuoteContent(strings.TrimRight(baseURL, "/")) + `/models').Content`
	for time.Now().Before(deadline) {
		out, err := exec.Command(powershell, "-NoProfile", "-NonInteractive", "-Command", script).CombinedOutput()
		if err == nil && liveDesktopModelsBodyHasPublicMiMoModels(string(out)) {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func liveDesktopWaitForHostModels(t *testing.T, baseURL string, proxyKey string) bool {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	client := &http.Client{Timeout: 3 * time.Second}
	url := strings.TrimRight(baseURL, "/") + "/models"
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("build model adapter request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+proxyKey)
		resp, err := client.Do(req)
		if err == nil {
			body, readErr := ioReadAllAndClose(resp)
			if readErr == nil && resp.StatusCode == http.StatusOK && liveDesktopModelsBodyHasPublicMiMoModels(string(body)) {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func validateLiveDesktopModelConfig(t *testing.T, configPath string, text string) {
	t.Helper()
	for _, want := range []string{
		`model = "mimo/mimo-v2.5"`,
		`model_catalog_json = "`,
		`wire_api = "responses"`,
		`requires_openai_auth = false`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("desktop model config %s missing %q:\n%s", configPath, want, text)
		}
	}
	catalogPath := filepath.Join(filepath.Dir(configPath), "catalog.json")
	raw, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("read desktop model catalog %s: %v", catalogPath, err)
	}
	if !strings.Contains(string(raw), `"slug": "mimo/mimo-v2.5"`) ||
		!strings.Contains(string(raw), `"slug": "mimo/mimo-v2.5-pro"`) {
		t.Fatalf("desktop model catalog missing public MiMo slugs:\n%s", raw)
	}
}

func liveDesktopModelsBodyHasPublicMiMoModels(body string) bool {
	return strings.Contains(body, `"id":"mimo/mimo-v2.5"`) &&
		strings.Contains(body, `"id":"mimo/mimo-v2.5-pro"`)
}

func liveDesktopGUIChatTimeout(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_DESKTOP_GUI_CHAT_TIMEOUT"))
	if raw == "" {
		return 3 * time.Minute
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil || timeout <= 0 {
		t.Fatalf("CODEX_HELPER_LIVE_DESKTOP_GUI_CHAT_TIMEOUT=%q, want positive duration like 3m", raw)
	}
	return timeout
}

func liveDesktopWaitForCodexHomeMarker(t *testing.T, root string, marker string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		found := false
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			switch strings.ToLower(filepath.Ext(path)) {
			case ".json", ".jsonl", ".log", ".txt", ".md":
			default:
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			if bytes.Contains(raw, []byte(marker)) {
				found = true
				return filepath.SkipAll
			}
			return nil
		})
		if err != nil && !found {
			t.Fatalf("walk desktop Codex home %s: %v", root, err)
		}
		if found {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func liveDesktopSendPromptWindows(t *testing.T, powershell string, prompt string) {
	t.Helper()
	script := strings.Join([]string{
		`Add-Type -AssemblyName System.Windows.Forms`,
		`$shell = New-Object -ComObject WScript.Shell`,
		`$activated = $false`,
		`for ($i = 0; $i -lt 60; $i++) { if ($shell.AppActivate('Codex')) { $activated = $true; break }; Start-Sleep -Milliseconds 500 }`,
		`if (-not $activated) { throw 'Could not activate Codex window' }`,
		`Set-Clipboard -Value '` + powershellSingleQuoteContent(prompt) + `'`,
		`Start-Sleep -Milliseconds 250`,
		`[System.Windows.Forms.SendKeys]::SendWait('^v')`,
		`Start-Sleep -Milliseconds 100`,
		`[System.Windows.Forms.SendKeys]::SendWait('{ENTER}')`,
	}, "; ")
	if out, err := exec.Command(powershell, "-NoProfile", "-STA", "-Command", script).CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(out))
		if strings.Contains(text, "Access is denied") {
			t.Skipf("Windows desktop GUI automation denied SendKeys access; model-profile launch and adapter reachability were already verified: %v\n%s", err, text)
		}
		t.Fatalf("send prompt to Windows Codex desktop failed: %v\n%s", err, text)
	}
}

func liveDesktopSendPromptMac(t *testing.T, prompt string) {
	t.Helper()
	args := []string{
		"-e", `set the clipboard to ` + appleScriptString(prompt),
		"-e", `tell application "Codex" to activate`,
		"-e", `delay 1`,
		"-e", `tell application "System Events"`,
		"-e", `keystroke "v" using command down`,
		"-e", `key code 36`,
		"-e", `end tell`,
	}
	if out, err := exec.Command("osascript", args...).CombinedOutput(); err != nil {
		t.Fatalf("send prompt to macOS Codex desktop failed; Accessibility permission may be required: %v\n%s", err, strings.TrimSpace(string(out)))
	}
}

func appleScriptString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\n", `\n`)
	return `"` + value + `"`
}

func ioReadAllAndClose(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func powershellSingleQuoteContent(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}
