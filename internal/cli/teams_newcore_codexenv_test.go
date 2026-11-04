package cli

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestTeamsNewCoreCodexArgs(t *testing.T) {
	var log bytes.Buffer
	got, err := teamsNewCoreCodexArgs([]string{"--model=gpt-5.5", "-c", `model_reasoning_effort="high"`, "--search",
		"--oss", "--local-provider", "lmstudio", "-C", "/tmp/x", "--add-dir", "/data", "--ephemeral",
		"--sandbox", "workspace-write", "--enable", "foo", "--skip-git-repo-check"}, &log)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-c", `model="gpt-5.5"`, "-c", `model_reasoning_effort="high"`, "-c", `web_search="live"`, "--enable", "foo"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %q\nwant %q", got, want)
	}
	for _, flag := range []string{"--oss", "--local-provider", "-C", "--add-dir", "--ephemeral", "--sandbox"} {
		if !strings.Contains(log.String(), "--codex-arg "+flag+" is ignored") {
			t.Errorf("no warning for %s:\n%s", flag, log.String())
		}
	}
	if strings.Contains(strings.Join(got, " "), "lmstudio") || strings.Contains(strings.Join(got, " "), "/data") {
		t.Fatalf("a skipped value was kept: %q", got)
	}
	if _, err := teamsNewCoreCodexArgs([]string{"--frobnicate"}, &log); err == nil {
		t.Fatal("an unknown argument was accepted")
	}
	if got, err := teamsNewCoreCodexArgs(nil, &log); err != nil || len(got) != 0 {
		t.Fatalf("no args = %q, %v", got, err)
	}
}
