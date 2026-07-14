package proxysupervisor

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

func testSpec(platform Platform) Spec {
	return Spec{
		Platform: platform, Executable: `/tmp/codex proxy`, ConfigPath: `/tmp/config "x".json`,
		InstanceID: "broker/one", OwnerToken: "owner&one", WorkingDir: `/tmp/work`,
		RestartDelay: 5 * time.Second, RestartWindow: time.Minute, RestartBurst: 3,
	}
}

func TestRenderAllPlatformsEscapesIdentityAndBoundsRestart(t *testing.T) {
	for _, platform := range []Platform{PlatformLinux, PlatformDarwin, PlatformWindows} {
		t.Run(string(platform), func(t *testing.T) {
			name, data, err := Render(testSpec(platform))
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if name == "" || len(data) == 0 {
				t.Fatalf("empty rendered artifact: name=%q", name)
			}
			text := string(data)
			if !strings.Contains(text, "--owner-token") || !strings.Contains(text, "--instance-id") {
				t.Fatalf("%s artifact does not preserve owner fencing args: %s", platform, text)
			}
			if platform == PlatformLinux && !strings.Contains(text, `"owner&one"`) {
				t.Fatalf("systemd artifact does not quote owner token: %s", text)
			}
			if platform != PlatformLinux && !strings.Contains(text, "owner&amp;one") {
				t.Fatalf("XML supervisor artifact does not escape owner token: %s", text)
			}
			if platform == PlatformWindows {
				var document struct{}
				if err := xml.Unmarshal(data, &document); err != nil {
					t.Fatalf("Windows task XML is not well formed: %v", err)
				}
			}
		})
	}
}

func TestSpecValidationRejectsMissingOwner(t *testing.T) {
	s := testSpec(PlatformLinux)
	s.OwnerToken = ""
	if _, _, err := Render(s); err == nil {
		t.Fatal("Render accepted empty owner token")
	}
}

func TestChildArgsAreManagedAndStable(t *testing.T) {
	args := testSpec(PlatformLinux).ChildArgs()
	want := []string{"--config", `/tmp/config "x".json`, "proxy", "daemon", "--instance-id", "broker/one", "--owner-token", "owner&one", "--managed"}
	if len(args) != len(want) {
		t.Fatalf("ChildArgs length=%d want=%d: %v", len(args), len(want), args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("ChildArgs[%d]=%q want %q", i, args[i], want[i])
		}
	}
}

func TestWindowsTaskQuotesArgumentsContainingSpaces(t *testing.T) {
	var task struct {
		Actions struct {
			Exec struct {
				Arguments string `xml:"Arguments"`
			} `xml:"Exec"`
		} `xml:"Actions"`
	}
	if err := xml.Unmarshal(mustRenderForTest(t, testSpec(PlatformWindows)), &task); err != nil {
		t.Fatalf("Unmarshal task XML: %v", err)
	}
	if !strings.Contains(task.Actions.Exec.Arguments, `"/tmp/config \"x\".json"`) {
		t.Fatalf("Windows config argument is not quoted: %q", task.Actions.Exec.Arguments)
	}
}

func mustRenderForTest(t *testing.T, spec Spec) []byte {
	t.Helper()
	_, data, err := Render(spec)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	return data
}
