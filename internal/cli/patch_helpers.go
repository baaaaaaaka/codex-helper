package cli

import (
	"context"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func currentProxyVersion() string {
	v := strings.TrimSpace(version)
	if v == "" {
		return "dev"
	}
	return v
}

func isCodexExecutable(cmdArg string, resolvedPath string) bool {
	resolvedBase := strings.ToLower(filepath.Base(resolvedPath))
	if resolvedBase == "codex" || resolvedBase == "codex.exe" {
		return true
	}
	cmdBase := strings.ToLower(filepath.Base(cmdArg))
	return cmdBase == "codex" || cmdBase == "codex.exe"
}

func resolveCodexVersion(path string) string {
	out, err := runCodexProbe(path, "--version")
	if err != nil {
		return ""
	}
	return extractVersion(out)
}

func extractVersion(output string) string {
	output = strings.TrimSpace(output)
	if output == "" {
		return ""
	}
	re := regexp.MustCompile(`\d+\.\d+\.\d+`)
	if m := re.FindString(output); m != "" {
		return m
	}
	re = regexp.MustCompile(`\d+\.\d+`)
	if m := re.FindString(output); m != "" {
		return m
	}
	fields := strings.Fields(output)
	if len(fields) > 0 {
		return fields[0]
	}
	return ""
}

// codexYoloArgs returns the CLI arguments to enable yolo mode for the given
// Codex binary. Returns nil if no yolo mechanism is available.
func codexYoloArgs(path string) []string {
	out, _ := runCodexProbe(path, "--help")
	if strings.Contains(out, "--yolo") {
		return []string{"--yolo"}
	}
	if strings.Contains(out, "--ask-for-approval") {
		return []string{"--ask-for-approval", "never"}
	}
	if strings.Contains(out, "--dangerously-bypass-approvals-and-sandbox") {
		return []string{"--dangerously-bypass-approvals-and-sandbox"}
	}
	return nil
}

func runCodexProbe(path string, arg string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, arg)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func isYoloFailure(err error, output string) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(output)
	// Check for --yolo flag not recognized.
	if strings.Contains(lower, "yolo") {
		if strings.Contains(lower, "unknown") || strings.Contains(lower, "unrecognized") {
			return true
		}
		if strings.Contains(lower, "not supported") || strings.Contains(lower, "invalid") {
			return true
		}
		if strings.Contains(lower, "flag provided but not defined") {
			return true
		}
	}
	// Check for approval_policy rejection by cloud requirements.
	if strings.Contains(lower, "approval_policy") && strings.Contains(lower, "not in the allowed set") {
		return true
	}
	// Check for --ask-for-approval flag not recognized.
	if strings.Contains(lower, "ask-for-approval") && (strings.Contains(lower, "unknown") || strings.Contains(lower, "unrecognized")) {
		return true
	}
	return false
}

func stripYoloArgs(cmdArgs []string) []string {
	if len(cmdArgs) == 0 {
		return cmdArgs
	}
	out := make([]string, 0, len(cmdArgs))
	for i := 0; i < len(cmdArgs); i++ {
		switch cmdArgs[i] {
		case "--yolo", "--dangerously-bypass-approvals-and-sandbox":
			continue
		case "--ask-for-approval":
			// Skip the flag and its value.
			if i+1 < len(cmdArgs) {
				i++
			}
			continue
		}
		out = append(out, cmdArgs[i])
	}
	return out
}
