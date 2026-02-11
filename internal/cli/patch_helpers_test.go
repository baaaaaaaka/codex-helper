package cli

import (
	"os"
	"runtime"
	"testing"
)

func TestIsYoloFailure(t *testing.T) {
	cases := []struct {
		output string
		want   bool
	}{
		{"", false},
		{"unknown flag: --yolo", true},
		{"yolo unknown", true},
		{"yolo not supported", true},
		{"yolo invalid", true},
		{"yolo flag provided but not defined", true},
		{"unrelated error", false},
	}
	for _, tc := range cases {
		got := isYoloFailure(os.ErrInvalid, tc.output)
		if got != tc.want {
			t.Fatalf("output %q: expected %v, got %v", tc.output, tc.want, got)
		}
	}
	if isYoloFailure(nil, "yolo unknown") {
		t.Fatalf("expected nil error to return false")
	}
}

func TestStripYoloArgs(t *testing.T) {
	in := []string{"codex", "--yolo", "resume", "abc"}
	out := stripYoloArgs(in)
	want := []string{"codex", "resume", "abc"}
	if len(out) != len(want) {
		t.Fatalf("expected %v, got %v", want, out)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, out)
		}
	}
}

func TestStripYoloArgsEmpty(t *testing.T) {
	out := stripYoloArgs(nil)
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}
}

func TestStripYoloArgsNoMatch(t *testing.T) {
	in := []string{"codex", "resume", "abc"}
	out := stripYoloArgs(in)
	if len(out) != len(in) {
		t.Fatalf("expected %v, got %v", in, out)
	}
}

func TestExtractVersion(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"Codex CLI 1.2.3", "1.2.3"},
		{"v2.0", "2.0"},
		{"version", "version"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := extractVersion(tc.input); got != tc.want {
			t.Fatalf("extractVersion(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestResolveCodexVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	writeStub(t, dir, "codex", "#!/bin/sh\necho \"Codex CLI 1.2.3\"\n", "")
	path := dir + "/codex"
	if got := resolveCodexVersion(path); got != "1.2.3" {
		t.Fatalf("expected version 1.2.3, got %q", got)
	}
}

func TestIsCodexExecutable(t *testing.T) {
	if !isCodexExecutable("codex", "/usr/local/bin/codex") {
		t.Fatalf("expected resolved path to identify codex binary")
	}
	if !isCodexExecutable("codex.exe", "C:\\Users\\user\\codex.exe") {
		t.Fatalf("expected cmd arg to identify codex.exe binary")
	}
	if isCodexExecutable("not-codex", "/tmp/other") {
		t.Fatalf("expected non-codex to be rejected")
	}
}

func TestCurrentProxyVersion(t *testing.T) {
	origVersion := version
	t.Cleanup(func() { version = origVersion })

	version = ""
	if got := currentProxyVersion(); got != "dev" {
		t.Fatalf("expected %q, got %q", "dev", got)
	}

	version = "v1.0.0"
	if got := currentProxyVersion(); got != "v1.0.0" {
		t.Fatalf("expected %q, got %q", "v1.0.0", got)
	}
}
