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
	"runtime"
	"strings"
	"time"
)

const windowsYoloSandboxConfigArg = `windows.sandbox="unelevated"`

var (
	codexYoloRuntimeGOOS        = runtime.GOOS
	codexHiddenFlagProbeTimeout = 2 * time.Second
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
// Keep this behavior out of README; it is intentionally undocumented there.
func codexYoloArgs(path string) []string {
	out, _ := runCodexProbe(path, "--help")
	var hiddenProbeOK bool
	var hiddenProbeChecked bool
	hiddenProbeAllowed := func() bool {
		if !hiddenProbeChecked {
			hiddenProbeOK = codexProbeRejectsUnknownArgs(path)
			hiddenProbeChecked = true
		}
		return hiddenProbeOK
	}
	if strings.Contains(out, "--yolo") {
		return []string{"--yolo"}
	}
	// Prefer --dangerously-bypass-approvals-and-sandbox over the individual
	// --ask-for-approval / --sandbox flags because the combined flag
	// expresses full yolo intent in a single argument and also skips the
	// git-repo safety check that the individual flags still enforce.
	if strings.Contains(out, "--dangerously-bypass-approvals-and-sandbox") {
		return []string{"--dangerously-bypass-approvals-and-sandbox"}
	}
	if hiddenProbeAllowed() && codexProbeAcceptsArgs(path, out, "--yolo") {
		return []string{"--yolo"}
	}
	if hiddenProbeAllowed() && codexProbeAcceptsArgs(path, out, "--dangerously-bypass-approvals-and-sandbox") {
		return []string{"--dangerously-bypass-approvals-and-sandbox"}
	}
	if strings.Contains(out, "--ask-for-approval") {
		args := []string{"--ask-for-approval", "never"}
		if strings.Contains(out, "--sandbox") {
			args = append(args, "--sandbox", "danger-full-access")
		}
		return args
	}
	if hiddenProbeAllowed() && codexProbeAcceptsArgs(path, out, "--ask-for-approval", "never") {
		args := []string{"--ask-for-approval", "never"}
		if codexProbeAcceptsArgs(path, out, "--sandbox", "danger-full-access") {
			args = append(args, "--sandbox", "danger-full-access")
		}
		return args
	}
	return nil
}

func codexYoloLaunchArgs(path string) []string {
	return codexYoloLaunchArgsWithOptions(path, yoloLaunchOptions{})
}

type yoloLaunchOptions struct {
	ForceFileAuthStore bool
}

func codexYoloLaunchArgsWithOptions(path string, opts yoloLaunchOptions) []string {
	args := codexYoloArgs(path)
	if len(args) == 0 {
		return args
	}
	out := make([]string, 0, len(args)+4)
	if opts.ForceFileAuthStore {
		out = append(out, "-c", `cli_auth_credentials_store="file"`)
	}
	if strings.EqualFold(codexYoloRuntimeGOOS, "windows") {
		out = append(out, "-c", windowsYoloSandboxConfigArg)
	}
	out = append(out, args...)
	return out
}

func runCodexProbe(path string, args ...string) (string, error) {
	return runCodexProbeWithTimeout(10*time.Second, path, args...)
}

func runCodexHiddenFlagProbe(path string, args ...string) (string, error) {
	return runCodexProbeWithTimeout(codexHiddenFlagProbeTimeout, path, args...)
}

func runCodexProbeWithTimeout(timeout time.Duration, path string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	configureCodexProbeCommand(cmd)
	out, err := cmd.CombinedOutput()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return string(out), ctxErr
	}
	return string(out), err
}

func codexProbeAcceptsArgs(path string, helpOutput string, args ...string) bool {
	if strings.TrimSpace(path) == "" || len(args) == 0 {
		return false
	}
	probeArgs := append(append([]string{}, args...), "--help")
	out, err := runCodexHiddenFlagProbe(path, probeArgs...)
	if err != nil {
		return false
	}
	out = strings.TrimSpace(out)
	if out == "" && strings.TrimSpace(helpOutput) == "" {
		return false
	}
	return !codexProbeOutputRejectsArgs(out, args...)
}

func codexProbeRejectsUnknownArgs(path string) bool {
	const unknownArg = "--codex-helper-unsupported-flag-probe"
	out, err := runCodexHiddenFlagProbe(path, unknownArg, "--help")
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return codexProbeOutputRejectsArgs(out, unknownArg)
}

func codexProbeOutputRejectsArgs(output string, args ...string) bool {
	lower := strings.ToLower(output)
	for _, arg := range args {
		trimmed := strings.ToLower(strings.TrimSpace(arg))
		if trimmed == "" || !strings.HasPrefix(trimmed, "-") {
			continue
		}
		if !strings.Contains(lower, trimmed) {
			continue
		}
		for _, marker := range []string{
			"unknown",
			"unrecognized",
			"unexpected",
			"invalid option",
			"invalid argument",
			"flag provided but not defined",
			"not supported",
		} {
			if strings.Contains(lower, marker) {
				return true
			}
		}
	}
	return false
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

// isTransientLaunchFailure returns true if err indicates the process was killed
// for reasons unrelated to a broken binary — OOM (SIGKILL → "signal: killed"),
// a context/timeout kill, or a user interrupt (SIGINT/SIGTERM). These must NOT
// be recorded as patch failures, or a one-off OOM would permanently latch yolo
// off for an otherwise-good codex version.
func isTransientLaunchFailure(err error, output string) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	lower := strings.ToLower(err.Error() + " " + output)
	for _, marker := range []string{
		"signal: killed",     // SIGKILL: OOM-killer or CommandContext timeout
		"signal: interrupt",  // SIGINT: Ctrl-C / cancellation
		"signal: terminated", // SIGTERM
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// isPatchedBinaryStartupFailure returns true if err + output indicate that a
// patched Codex binary failed to start properly. Transient kills (OOM, timeout,
// interrupt) are excluded — see isTransientLaunchFailure.
func isPatchedBinaryStartupFailure(err error, output string) bool {
	if err == nil {
		return false
	}
	if isTransientLaunchFailure(err, output) {
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

// isPatchedBinaryFailure checks output for error patterns that indicate the
// patched binary is structurally broken (corrupted, wrong arch, missing libs).
// Ambiguous/transient markers like "killed" and "abort" are intentionally NOT
// here: genuine crash signals (SIGABRT, SIGSEGV, …) are detected precisely via
// exitDueToFatalSignal, while "killed"/"abort" in output are often transient.
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
		"segmentation fault",
		"bus error",
		"illegal instruction",
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
		arg := strings.TrimSpace(cmdArgs[i])
		switch {
		case arg == "--yolo" || arg == "--dangerously-bypass-approvals-and-sandbox":
			continue
		case (arg == "-c" || arg == "--config") && i+1 < len(cmdArgs) && strings.TrimSpace(cmdArgs[i+1]) == windowsYoloSandboxConfigArg:
			i++
			continue
		case (arg == "-c" || arg == "--config") && i+1 < len(cmdArgs) && strings.TrimSpace(cmdArgs[i+1]) == `cli_auth_credentials_store="file"`:
			i++
			continue
		case arg == "--ask-for-approval" || arg == "--sandbox" || arg == "-s":
			// Skip the flag and its value.
			if i+1 < len(cmdArgs) {
				i++
			}
			continue
		case strings.HasPrefix(arg, "--ask-for-approval=") ||
			strings.HasPrefix(arg, "--sandbox=") ||
			strings.HasPrefix(arg, "-s="):
			continue
		default:
			out = append(out, cmdArgs[i])
		}
	}
	return out
}
