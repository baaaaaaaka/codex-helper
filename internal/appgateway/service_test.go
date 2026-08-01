package appgateway

import (
	"strings"
	"testing"
)

func testServiceSpec(platform ServicePlatform) ServiceSpec {
	return ServiceSpec{
		Platform:   platform,
		Name:       "codex-helper-app-gateway-profile-a",
		Executable: `/home/alice/bin/codex-proxy`,
		ConfigPath: `/home/alice/.config/codex-proxy/config.json`,
		ProfileID:  "profile-a",
		StateDir:   `/home/alice/.local/state/codex-helper/app-gateway/registrations`,
	}
}

func TestServiceRenderSystemdIsRestartingAndUserScoped(t *testing.T) {
	rendered, err := testServiceSpec(ServiceSystemd).Render()
	if err != nil {
		t.Fatal(err)
	}
	text := string(rendered)
	for _, want := range []string{"Restart=on-failure", "RestartSec=10", "proxy app-gateway run", "WantedBy=default.target"} {
		if !strings.Contains(text, want) {
			t.Fatalf("systemd unit missing %q:\n%s", want, text)
		}
	}
}

func TestServiceRenderLaunchdKeepsStableArguments(t *testing.T) {
	rendered, err := testServiceSpec(ServiceLaunchd).Render()
	if err != nil {
		t.Fatal(err)
	}
	text := string(rendered)
	for _, want := range []string{"<key>RunAtLoad</key><true/>", "<key>KeepAlive</key><true/>", "profile-a", "app-gateway"} {
		if !strings.Contains(text, want) {
			t.Fatalf("launchd plist missing %q:\n%s", want, text)
		}
	}
}

func TestServiceRenderWindowsTaskIsLeastPrivilegeAndSingleInstance(t *testing.T) {
	rendered, err := testServiceSpec(ServiceWindows).Render()
	if err != nil {
		t.Fatal(err)
	}
	text := string(rendered)
	for _, want := range []string{"<LogonType>InteractiveToken</LogonType>", "<RunLevel>LeastPrivilege</RunLevel>", "<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>", "profile-a"} {
		if !strings.Contains(text, want) {
			t.Fatalf("Windows task missing %q:\n%s", want, text)
		}
	}
}
