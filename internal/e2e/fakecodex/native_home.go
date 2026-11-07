package fakecodex

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteCodexHome writes a throwaway CODEX_HOME whose only model provider is
// the fake Responses API at baseURL, with retries disabled.
func WriteCodexHome(dir, baseURL string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	cfg := fmt.Sprintf(`model = "probe-model"
model_provider = "probe"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false

[model_providers.probe]
name = "Probe"
base_url = %q
env_key = "PROBE_API_KEY"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
stream_idle_timeout_ms = 600000
`, baseURL)
	return os.WriteFile(filepath.Join(dir, "config.toml"), []byte(cfg), 0o600)
}

// CodexEnv is the environment for a codex process using home as CODEX_HOME.
func CodexEnv(home string) []string {
	env := []string{
		"CODEX_HOME=" + home,
		"HOME=" + filepath.Dir(home),
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"PROBE_API_KEY=probe-key",
		"RUST_LOG=warn",
		"NO_COLOR=1",
	}
	if v := os.Getenv("TMPDIR"); v != "" {
		env = append(env, "TMPDIR="+v)
	}
	return env
}
