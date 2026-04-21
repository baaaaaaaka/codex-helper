package cloudgate

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCodexPatchedYoloLaunchIntegration verifies that a real Codex install can
// be patched and launched with yolo flags.
//
// It is skipped unless both env vars are set:
//   - CODEX_PATCH_TEST=1
//   - CODEX_YOLO_PATCH_TEST=1
func TestCodexPatchedYoloLaunchIntegration(t *testing.T) {
	if os.Getenv("CODEX_PATCH_TEST") != "1" {
		t.Skip("CODEX_PATCH_TEST not set")
	}
	if os.Getenv("CODEX_YOLO_PATCH_TEST") != "1" {
		t.Skip("CODEX_YOLO_PATCH_TEST not set")
	}

	wrapper, err := exec.LookPath("codex")
	if err != nil {
		t.Fatalf("codex not found in PATH: %v", err)
	}

	yoloArgs, helpOut, helpErr := detectYoloArgs(wrapper)
	if len(yoloArgs) == 0 {
		if helpErr != nil {
			t.Fatalf(
				"could not detect yolo flags from `codex --help`: %v\noutput=%s",
				helpErr,
				formatProbeOutput(helpOut),
			)
		}
		t.Fatalf("could not detect yolo flags from `codex --help`\noutput=%s", formatProbeOutput(helpOut))
	}

	cacheDir := filepath.Join(t.TempDir(), "patch-cache")
	result, patchEnv, err := PrepareYoloBinary(wrapper, cacheDir)
	if err != nil {
		t.Fatalf("PrepareYoloBinary: %v", err)
	}
	defer result.Cleanup()
	defer cleanupRequirementsDir(result.RequirementsPath)

	if result.PatchedBinary == "" {
		t.Fatal("expected patched binary path")
	}

	args := append(append([]string{}, yoloArgs...), "--help")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, result.PatchedBinary, args...)
	cmd.Env = append(os.Environ(), patchEnv...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("patched codex timed out with args %v", args)
	}
	if err != nil {
		t.Fatalf("patched codex launch failed: %v\nargs=%v\noutput=%s", err, args, strings.TrimSpace(string(out)))
	}
}

func detectYoloArgs(codexPath string) ([]string, string, error) {
	out, err := runProbe(codexPath, "--help")

	if strings.Contains(out, "--yolo") {
		return []string{"--yolo"}, out, err
	}
	if strings.Contains(out, "--dangerously-bypass-approvals-and-sandbox") {
		return []string{"--dangerously-bypass-approvals-and-sandbox"}, out, err
	}
	if strings.Contains(out, "--ask-for-approval") {
		args := []string{"--ask-for-approval", "never"}
		if strings.Contains(out, "--sandbox") {
			args = append(args, "--sandbox", "danger-full-access")
		}
		return args, out, err
	}
	return nil, out, err
}

func formatProbeOutput(out string) string {
	out = strings.TrimSpace(out)
	if out == "" {
		return "<empty>"
	}
	return out
}

func runProbe(path string, arg string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, arg)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("command timed out")
	}
	return string(out), err
}
