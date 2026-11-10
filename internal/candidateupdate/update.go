package candidateupdate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
)

const (
	ProtocolVersion = 1
	InternalCommand = "__internal-update-apply"
)

type Context = updatestate.Invocation

type Result struct {
	Version         string `json:"version"`
	RuntimePath     string `json:"runtime_path"`
	EntryPath       string `json:"entry_path"`
	RestartRequired bool   `json:"restart_required"`
	Noop            bool   `json:"noop,omitempty"`
}

// Resolves the executing candidate, never a stable launcher inherited from its parent.
var candidateExecutablePath = helperpath.RawExecutable

// Invoke asks a downloaded candidate to own the managed-runtime state
// transition. The caller remains responsible only for download, validation,
// and arranging a restart after a successful result.
func Invoke(ctx context.Context, candidate string, request Context, timeout time.Duration) (Result, error) {
	candidate = filepath.Clean(strings.TrimSpace(candidate))
	if candidate == "" || candidate == "." {
		return Result{}, fmt.Errorf("candidate path is empty")
	}
	// Probe both ABIs before creating invocation or installation state.
	if err := helperruntime.ProbeProtocol(ctx, candidate); err != nil {
		return Result{}, err
	}
	if err := CheckBootstrapEntry(ctx, request.RuntimeRoot, request.EntryPath); err != nil {
		return Result{}, err
	}
	if _, err := helperruntime.ReadBoot(request.RuntimeRoot); err != nil {
		return Result{}, fmt.Errorf("%w: %v", helperruntime.ErrBootstrapState, err)
	}
	request.Schema = ProtocolVersion
	request.CandidatePath = candidate
	if strings.TrimSpace(request.CandidateSHA256) == "" {
		hash, err := fileSHA256(candidate)
		if err != nil {
			return Result{}, fmt.Errorf("hash candidate: %w", err)
		}
		request.CandidateSHA256 = hash
	}
	contextPath, err := writeInvocationContext(candidate, request)
	if err != nil {
		return Result{}, err
	}
	defer deleteInvocationContext(contextPath, request.RequestID)

	cmdCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, candidate, InternalCommand, "--protocol=2", "--state-db="+contextPath, "--request-id="+request.RequestID)
	cmd.Env = append(helperruntime.LauncherEnvironment(os.Environ()), helperruntime.EnvDisable+"=1")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return Result{}, fmt.Errorf("candidate-owned update failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	var result Result
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		return Result{}, fmt.Errorf("decode candidate-owned update result: %w: stdout=%q stderr=%q", err, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
	}
	return result, nil
}

// HandleInternalCommand executes the fixed candidate-owned update ABI before
// immutable-runtime dispatch or Cobra initialization.
func HandleInternalCommand(args []string, buildVersion string, stdout io.Writer, stderr io.Writer) (int, bool) {
	if len(args) < 2 || args[1] != InternalCommand {
		return 0, false
	}
	if code, handled := handleLegacyUpdate(args, buildVersion, stdout, stderr); handled {
		return code, true
	}
	protocol := ""
	contextPath := ""
	requestID := ""
	for _, arg := range args[2:] {
		switch {
		case strings.HasPrefix(arg, "--protocol="):
			protocol = strings.TrimPrefix(arg, "--protocol=")
		case strings.HasPrefix(arg, "--state-db="):
			contextPath = strings.TrimPrefix(arg, "--state-db=")
		case strings.HasPrefix(arg, "--request-id="):
			requestID = strings.TrimPrefix(arg, "--request-id=")
		default:
			_, _ = fmt.Fprintf(stderr, "candidate update: unsupported argument %q\n", arg)
			return 2, true
		}
	}
	if protocol != "2" || strings.TrimSpace(contextPath) == "" || requestID == "" {
		_, _ = fmt.Fprintln(stderr, "candidate update: protocol=2, state-db and request-id are required")
		return 2, true
	}
	request, err := readContext(contextPath, requestID)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "candidate update: %v\n", err)
		return 1, true
	}
	result, err := Apply(context.Background(), request, buildVersion)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "candidate update: %v\n", err)
		return 1, true
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		_, _ = fmt.Fprintf(stderr, "candidate update: encode result: %v\n", err)
		return 1, true
	}
	return 0, true
}

func Apply(ctx context.Context, request Context, buildVersion string) (Result, error) {
	if err := validateContext(request, buildVersion); err != nil {
		return Result{}, err
	}
	executable, err := candidateExecutablePath()
	if err != nil {
		return Result{}, fmt.Errorf("inspect candidate executable: %w", err)
	}
	actualHash, err := fileSHA256(executable)
	if err != nil {
		return Result{}, fmt.Errorf("hash running candidate: %w", err)
	}
	if !strings.EqualFold(actualHash, strings.TrimSpace(request.CandidateSHA256)) {
		return Result{}, fmt.Errorf("candidate sha256 = %s, want %s", actualHash, request.CandidateSHA256)
	}
	if err := CheckBootstrapEntry(ctx, request.RuntimeRoot, request.EntryPath); err != nil {
		return Result{}, err
	}
	if _, err := helperruntime.ReadBoot(request.RuntimeRoot); err != nil {
		return Result{}, fmt.Errorf("%w: %v", helperruntime.ErrBootstrapState, err)
	}
	var result Result
	err = helperruntime.WithRootLock(ctx, request.RuntimeRoot, func() error {
		// Validate the final immutable path before changing any launcher or
		// default. A failed update must not leave the next start pointing at it.
		installed, err := helperruntime.InstallVersion(request.RuntimeRoot, executable, request.TargetVersion, runtime.GOOS, false)
		if err != nil {
			return err
		}
		if err := verifyImmutableRuntime(ctx, installed, request.TargetVersion); err != nil {
			return err
		}
		var reconcileErr error
		result, reconcileErr = reconcileLocked(request, executable)
		return reconcileErr
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

// CheckNativeBootstrap checks admission without replaying old pending intent.
func CheckNativeBootstrap() error {
	current, ok := helperruntime.Current()
	if !ok {
		return nil
	}
	// Native boot is the sole authority. Legacy pending intent cannot replay
	// after an explicit protocol upgrade, including a lost native database.
	_, err := helperruntime.ReadBoot(current.Root)
	if err != nil {
		return fmt.Errorf("%w: %v", helperruntime.ErrBootstrapState, err)
	}
	return nil
}

func reconcileLocked(request Context, source string) (Result, error) {
	boot, err := helperruntime.ReadBoot(request.RuntimeRoot)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", helperruntime.ErrBootstrapState, err)
	}
	targetVersion, _ := helperruntime.NormalizeVersion(request.TargetVersion)
	if sourceVersion := strings.TrimSpace(request.SourceVersion); sourceVersion != "" {
		normalizedSource, ok := helperruntime.NormalizeVersion(sourceVersion)
		if !ok {
			return Result{}, fmt.Errorf("invalid source version %q", sourceVersion)
		}
		if boot.Good != normalizedSource && boot.Good != targetVersion {
			return Result{}, fmt.Errorf("active runtime changed from %s to %s while preparing %s", normalizedSource, boot.Good, targetVersion)
		}
	}
	installed, err := helperruntime.InstallVersion(request.RuntimeRoot, source, targetVersion, runtime.GOOS, false)
	if err != nil {
		return Result{}, fmt.Errorf("publish immutable runtime: %w", err)
	}
	installedHash, err := fileSHA256(installed)
	if err != nil {
		return Result{}, err
	}
	if !strings.EqualFold(installedHash, request.CandidateSHA256) {
		return Result{}, fmt.Errorf("immutable runtime sha256 = %s, want %s", installedHash, request.CandidateSHA256)
	}
	info, err := os.Stat(request.EntryPath)
	if err != nil || !info.Mode().IsRegular() {
		return Result{}, fmt.Errorf("safe boot entry is missing or invalid; restore it with the installer: %v", err)
	}
	if err := helperruntime.Activate(request.RuntimeRoot, targetVersion); err != nil {
		return Result{}, fmt.Errorf("activate target runtime: %w", err)
	}
	if err := updateInstallRecord(request, targetVersion); err != nil {
		return Result{}, err
	}
	return Result{Version: strings.TrimPrefix(targetVersion, "v"), RuntimePath: installed, EntryPath: request.EntryPath, Noop: boot.Good == targetVersion}, nil
}

func validateContext(request Context, buildVersion string) error {
	if err := validateStateContext(request); err != nil {
		return err
	}
	target, _ := helperruntime.NormalizeVersion(request.TargetVersion)
	build, ok := helperruntime.NormalizeVersion(buildVersion)
	if !ok || build != target {
		return fmt.Errorf("candidate build version = %q, want %s", buildVersion, target)
	}
	return nil
}

func validateStateContext(request Context) error {
	if request.Schema != ProtocolVersion {
		return fmt.Errorf("context schema = %d, want %d", request.Schema, ProtocolVersion)
	}
	_, ok := helperruntime.NormalizeVersion(request.TargetVersion)
	if !ok {
		return fmt.Errorf("invalid target version %q", request.TargetVersion)
	}
	sha := strings.TrimSpace(request.CandidateSHA256)
	if len(sha) != sha256.Size*2 {
		return fmt.Errorf("candidate sha256 is invalid")
	}
	if _, err := hex.DecodeString(sha); err != nil {
		return fmt.Errorf("candidate sha256 is invalid: %w", err)
	}
	if !filepath.IsAbs(strings.TrimSpace(request.RuntimeRoot)) {
		return fmt.Errorf("runtime root must be absolute")
	}
	if !filepath.IsAbs(strings.TrimSpace(request.EntryPath)) {
		return fmt.Errorf("entry path must be absolute")
	}
	root, err := cleanAbsolutePath(request.RuntimeRoot)
	if err != nil {
		return fmt.Errorf("runtime root: %w", err)
	}
	entry, err := cleanAbsolutePath(request.EntryPath)
	if err != nil {
		return fmt.Errorf("entry path: %w", err)
	}
	expectedRoot := filepath.Join(filepath.Dir(entry), ".cxp-runtime")
	if !samePath(root, expectedRoot) {
		return fmt.Errorf("runtime root %s is not owned by stable entry %s", root, entry)
	}
	if strings.TrimSpace(request.RequestID) == "" {
		return fmt.Errorf("request id is empty")
	}
	return nil
}

func updateInstallRecord(request Context, targetVersion string) error {
	recordPath := strings.TrimSpace(request.RecordPath)
	if recordPath == "" {
		var err error
		recordPath, err = managedinstall.DefaultRecordPath()
		if err != nil {
			return fmt.Errorf("resolve install record: %w", err)
		}
	}
	record, err := managedinstall.LoadRecord(recordPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load install record: %w", err)
	}
	record.TargetPath = managedinstall.CanonicalTargetPathForEntry(request.EntryPath, runtime.GOOS)
	record.TargetSource = string(managedinstall.SourceCurrentExecutable)
	record.TargetState = string(managedinstall.StateManaged)
	record.Version = strings.TrimPrefix(targetVersion, "v")
	record.GOOS = runtime.GOOS
	record.GOARCH = runtime.GOARCH
	record.UpdatedAt = ""
	if err := managedinstall.SaveRecord(recordPath, record); err != nil {
		return fmt.Errorf("save install record: %w", err)
	}
	return nil
}

func verifyImmutableRuntime(ctx context.Context, runtimePath string, targetVersion string) error {
	runtimePath = filepath.Clean(strings.TrimSpace(runtimePath))
	if runtimePath == "" || runtimePath == "." {
		return fmt.Errorf("verify immutable runtime: runtime path is empty")
	}
	normalizedTarget, ok := helperruntime.NormalizeVersion(targetVersion)
	if !ok {
		return fmt.Errorf("verify immutable runtime: invalid target version %q", targetVersion)
	}
	out, err := helperruntime.ProbeExecutable(ctx, runtimePath, []string{"--version"}, 15*time.Second)
	if err != nil {
		return fmt.Errorf("verify immutable runtime %s: %w: %s", runtimePath, err, strings.TrimSpace(string(out)))
	}
	for _, field := range strings.Fields(string(out)) {
		if version, ok := helperruntime.NormalizeVersion(field); ok && version == normalizedTarget {
			return nil
		}
	}
	return fmt.Errorf("immutable runtime version output %q does not contain %s", strings.TrimSpace(string(out)), normalizedTarget)
}

func readContext(path, id string) (Context, error) {
	if filepath.Base(path) != updatestate.Filename {
		return Context{}, fmt.Errorf("invalid update state database path")
	}
	store, err := updatestate.OpenRead(filepath.Dir(path))
	if err != nil {
		return Context{}, err
	}
	defer store.Close()
	return store.Invocation(id)
}
func writeInvocationContext(candidate string, request Context) (string, error) {
	store, err := updatestate.Open(request.RuntimeRoot)
	if err != nil {
		return "", err
	}
	defer store.Close()
	if err = store.SaveInvocation(request); err != nil {
		return "", err
	}
	return filepath.Join(request.RuntimeRoot, updatestate.Filename), nil
}
func deleteInvocationContext(path, id string) error {
	store, err := updatestate.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer store.Close()
	return store.DeleteInvocation(id)
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func cleanAbsolutePath(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return "", fmt.Errorf("path is empty")
	}
	return filepath.Abs(path)
}

func samePath(left string, right string) bool {
	left = canonicalExistingPath(left)
	right = canonicalExistingPath(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func canonicalExistingPath(path string) string {
	path, _ = filepath.Abs(filepath.Clean(path))
	if resolved, err := filepath.EvalSymlinks(path); err == nil && strings.TrimSpace(resolved) != "" {
		if absolute, absErr := filepath.Abs(filepath.Clean(resolved)); absErr == nil {
			return absolute
		}
	}
	return path
}

func pathWithin(path string, parent string) bool {
	path = canonicalExistingPath(path)
	parent = canonicalExistingPath(parent)
	relative, err := filepath.Rel(parent, path)
	if err != nil {
		return false
	}
	if relative == "." {
		return true
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil && runtime.GOOS != "windows" {
		return err
	}
	return nil
}
