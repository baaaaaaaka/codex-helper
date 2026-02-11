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

func supportsYoloFlag(path string) bool {
	out, err := runCodexProbe(path, "--help")
	if strings.Contains(out, "--yolo") {
		return true
	}
	if err != nil {
		return true
	}
	lower := strings.ToLower(out)
	if strings.Contains(lower, "usage") || strings.Contains(lower, "codex") {
		return false
	}
	return true
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
	if !strings.Contains(lower, "yolo") {
		return false
	}
	if strings.Contains(lower, "unknown") || strings.Contains(lower, "unrecognized") {
		return true
	}
	if strings.Contains(lower, "not supported") || strings.Contains(lower, "invalid") {
		return true
	}
	if strings.Contains(lower, "flag provided but not defined") {
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
		if cmdArgs[i] == "--yolo" {
			continue
		}
		out = append(out, cmdArgs[i])
	}
	return out
}
