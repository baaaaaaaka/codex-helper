package codexrunner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

// TestLiveAuthenticatedCloudConfigPreservesOfficialChatGPTOrigin is an opt-in
// account-backed contract. It deliberately starts from an auth-only CODEX_HOME
// so a cached workspace bundle cannot hide an origin-routing regression.
//
// The direct case proves that the account and official Codex build can fetch
// workspace-managed configuration. When CXP_LIVE_HTTP_PROXY is set, the same
// contract is repeated through that egress proxy while CXP's Responses-only
// policy gateway is active. The two cases therefore distinguish account or
// upstream failures from CXP routing failures without starting a model turn.
func TestLiveAuthenticatedCloudConfigPreservesOfficialChatGPTOrigin(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CXP_LIVE_CLOUD_CONFIG_TEST")) != "1" {
		t.Skip("set CXP_LIVE_CLOUD_CONFIG_TEST=1 to exercise an authenticated workspace bundle fetch")
	}

	command := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX"))
	if command == "" {
		var err error
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}
	sourceHome := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX_HOME"))
	if sourceHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		sourceHome = filepath.Join(home, ".codex")
	}
	auth, err := os.ReadFile(filepath.Join(sourceHome, "auth.json"))
	if err != nil {
		t.Fatalf("read live auth: %v", err)
	}
	originalHash := runtimeContractFileHash(t, command)

	tests := []struct {
		name      string
		httpProxy string
	}{
		{name: "direct"},
	}
	if proxy := strings.TrimSpace(os.Getenv("CXP_LIVE_HTTP_PROXY")); proxy != "" {
		tests = append(tests, struct {
			name      string
			httpProxy string
		}{name: "configured-http-proxy", httpProxy: proxy})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			codexHome := filepath.Join(t.TempDir(), "codex-home")
			if err := os.MkdirAll(codexHome, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), auth, 0o600); err != nil {
				t.Fatal(err)
			}
			workingDir := t.TempDir()
			extraEnv := []string{
				"CODEX_HOME=" + codexHome,
				"CODEX_DIR=" + codexHome,
				"HTTP_PROXY=" + test.httpProxy,
				"HTTPS_PROXY=" + test.httpProxy,
				"ALL_PROXY=",
				"http_proxy=" + test.httpProxy,
				"https_proxy=" + test.httpProxy,
				"all_proxy=",
				"NO_PROXY=127.0.0.1,localhost,::1",
				"no_proxy=127.0.0.1,localhost,::1",
			}
			runner := &AppServerRunner{
				Starter: PolicyAppServerStarter{ServerOptions: responsespolicy.ServerOptions{
					ProxyURL: test.httpProxy,
				}},
				Command:    command,
				ExtraEnv:   extraEnv,
				WorkingDir: workingDir,
				Timeout:    60 * time.Second,
			}
			defer runner.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			if err := runner.ensureReady(ctx); err != nil {
				t.Fatalf("initialize official Codex app-server: %v", err)
			}
			threadID, err := runner.startThread(ctx, TurnInput{WorkingDir: workingDir, Ephemeral: true})
			if err != nil {
				t.Fatalf("thread/start with fresh auth-only CODEX_HOME: %v", err)
			}
			if strings.TrimSpace(threadID) == "" {
				t.Fatal("thread/start returned an empty thread id")
			}
		})
	}

	if got := runtimeContractFileHash(t, command); got != originalHash {
		t.Fatal("original Codex binary changed during live cloud-config contract")
	}
}
