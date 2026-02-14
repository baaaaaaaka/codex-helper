package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
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
		args := []string{"--ask-for-approval", "never"}
		if strings.Contains(out, "--sandbox") {
			args = append(args, "--sandbox", "danger-full-access")
		}
		return args
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

// isPatchedBinaryStartupFailure returns true if err + output indicate that a
// patched Codex binary failed to start properly.
func isPatchedBinaryStartupFailure(err error, output string) bool {
	if err == nil {
		return false
	}
	if isPatchedBinaryFailure(err, output) {
		return true
	}
	if exitDueToFatalSignal(err) {
		return true
	}
	// Binary not found or not executable after patching.
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return true
	}
	var execErr *exec.Error
	if errors.As(err, &execErr) {
		return true
	}
	return false
}

// isPatchedBinaryFailure checks output for error patterns specific to a
// patched Codex binary (corrupted binary, missing libraries, etc.).
func isPatchedBinaryFailure(err error, output string) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(output)
	patterns := []string{
		"not a valid executable",
		"exec format error",
		"cannot execute binary",
		"bad cpu type",
		"killed",
		"segmentation fault",
		"bus error",
		"illegal instruction",
		"abort",
	}
	for _, p := range patterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

// hashFileSHA256 computes the SHA-256 hex digest of the file at path.
func hashFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

const maxFailureReasonLen = 256

// formatFailureReason builds a short human-readable failure description.
func formatFailureReason(err error, output string) string {
	parts := make([]string, 0, 2)
	if err != nil {
		parts = append(parts, err.Error())
	}
	out := strings.TrimSpace(output)
	if out != "" {
		parts = append(parts, out)
	}
	reason := strings.Join(parts, ": ")
	if len(reason) > maxFailureReasonLen {
		reason = reason[:maxFailureReasonLen-3] + "..."
	}
	return reason
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
		case "--ask-for-approval", "--sandbox":
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
