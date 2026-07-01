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
	"golang.org/x/mod/semver"
)

const (
	ProtocolVersion      = 1
	InternalCommand      = "__internal-update-apply"
	PendingFileName      = "pending-update.json"
	firstProtocolVersion = "v0.1.13-rc.36"
)

func SupportsProtocol(version string) bool {
	normalized, ok := helperruntime.NormalizeVersion(version)
	return ok && semver.Compare(normalized, firstProtocolVersion) >= 0
}

type Context struct {
	Schema          int    `json:"schema"`
	CandidatePath   string `json:"candidate_path"`
	CandidateSHA256 string `json:"candidate_sha256"`
	SourceVersion   string `json:"source_version,omitempty"`
	TargetVersion   string `json:"target_version"`
	RuntimeRoot     string `json:"runtime_root"`
	EntryPath       string `json:"entry_path"`
	RecordPath      string `json:"record_path,omitempty"`
	RequestID       string `json:"request_id"`
}

type Result struct {
	Version         string `json:"version"`
	RuntimePath     string `json:"runtime_path"`
	EntryPath       string `json:"entry_path"`
	RestartRequired bool   `json:"restart_required"`
	Noop            bool   `json:"noop,omitempty"`
}

type Pending struct {
	Schema         int    `json:"schema"`
	TargetVersion  string `json:"target_version"`
	TargetSHA256   string `json:"target_sha256"`
	PreviousActive string `json:"previous_active,omitempty"`
	TargetActive   string `json:"target_active"`
	RuntimeRoot    string `json:"runtime_root"`
	EntryPath      string `json:"entry_path"`
	RecordPath     string `json:"record_path,omitempty"`
	RequestID      string `json:"request_id"`
}

// Invoke asks a downloaded candidate to own the managed-runtime state
// transition. The caller remains responsible only for download, validation,
// and arranging a restart after a successful result.
func Invoke(ctx context.Context, candidate string, request Context, timeout time.Duration) (Result, error) {
	candidate = filepath.Clean(strings.TrimSpace(candidate))
	if candidate == "" || candidate == "." {
		return Result{}, fmt.Errorf("candidate path is empty")
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
	defer os.Remove(contextPath)

	cmdCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, candidate, InternalCommand, "--protocol=1", "--context-file="+contextPath)
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
	protocol := ""
	contextPath := ""
	for _, arg := range args[2:] {
		switch {
		case strings.HasPrefix(arg, "--protocol="):
			protocol = strings.TrimPrefix(arg, "--protocol=")
		case strings.HasPrefix(arg, "--context-file="):
			contextPath = strings.TrimPrefix(arg, "--context-file=")
		default:
			_, _ = fmt.Fprintf(stderr, "candidate update: unsupported argument %q\n", arg)
			return 2, true
		}
	}
	if protocol != "1" || strings.TrimSpace(contextPath) == "" {
		_, _ = fmt.Fprintln(stderr, "candidate update: protocol=1 and context-file are required")
		return 2, true
	}
	request, err := readContext(contextPath)
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
	executable, err := helperpath.RawExecutable()
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
	var result Result
	err = helperruntime.WithRootLock(ctx, request.RuntimeRoot, func() error {
		var reconcileErr error
		result, reconcileErr = reconcileLocked(request, executable, false)
		return reconcileErr
	})
	if err != nil {
		return Result{}, err
	}
	if err := verifyFreshEntry(request.EntryPath, targetVersion(request.TargetVersion)); err != nil {
		return Result{}, err
	}
	return result, nil
}

// ResumePending converges an interrupted candidate-owned update from the
// active runtime itself. It is safe to call on every managed-runtime startup.
func ResumePending(ctx context.Context, currentVersion string) error {
	current, ok := helperruntime.Current()
	if !ok {
		return nil
	}
	pendingPath := filepath.Join(current.Root, PendingFileName)
	pending, err := readPending(pendingPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	want, ok := helperruntime.NormalizeVersion(pending.TargetVersion)
	if !ok {
		return fmt.Errorf("pending update has invalid target version %q", pending.TargetVersion)
	}
	got, ok := helperruntime.NormalizeVersion(currentVersion)
	if !ok {
		return nil
	}
	previous, previousOK := helperruntime.NormalizeVersion(pending.PreviousActive)
	if got != want && (!previousOK || got != previous) {
		return nil
	}
	request := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   pending.TargetActive,
		CandidateSHA256: pending.TargetSHA256,
		SourceVersion:   pending.PreviousActive,
		TargetVersion:   pending.TargetVersion,
		RuntimeRoot:     pending.RuntimeRoot,
		EntryPath:       pending.EntryPath,
		RecordPath:      pending.RecordPath,
		RequestID:       pending.RequestID,
	}
	if !samePath(request.RuntimeRoot, current.Root) || !samePath(request.EntryPath, current.EntryPath) {
		return fmt.Errorf("pending update belongs to a different managed runtime")
	}
	if err := validateStateContext(request); err != nil {
		return fmt.Errorf("validate pending update: %w", err)
	}
	expectedTarget := helperruntime.VersionPath(current.Root, want, runtime.GOOS)
	if !samePath(pending.TargetActive, expectedTarget) {
		return fmt.Errorf("pending update target %s does not match managed runtime %s", pending.TargetActive, expectedTarget)
	}
	return helperruntime.WithRootLock(ctx, current.Root, func() error {
		latest, err := readPending(pendingPath)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if latest.RequestID != pending.RequestID || latest.TargetVersion != pending.TargetVersion {
			return nil
		}
		active, err := helperruntime.ReadActive(current.Root)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read active runtime during pending recovery: %w", err)
		}
		if err == nil && active != got && active != want {
			return nil
		}
		targetHash, err := fileSHA256(expectedTarget)
		if err != nil {
			return fmt.Errorf("hash pending update target: %w", err)
		}
		if !strings.EqualFold(targetHash, pending.TargetSHA256) {
			return fmt.Errorf("pending update target sha256 = %s, want %s", targetHash, pending.TargetSHA256)
		}
		if _, err := reconcileLocked(request, pending.TargetActive, true); err != nil {
			return err
		}
		if err := os.Remove(pendingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove completed pending update: %w", err)
		}
		return syncDirectory(current.Root)
	})
}

func reconcileLocked(request Context, source string, resuming bool) (Result, error) {
	targetVersion, _ := helperruntime.NormalizeVersion(request.TargetVersion)
	targetPath := helperruntime.VersionPath(request.RuntimeRoot, targetVersion, runtime.GOOS)
	if targetPath == "" {
		return Result{}, fmt.Errorf("derive target runtime path")
	}
	previous := ""
	if active, err := helperruntime.ReadActive(request.RuntimeRoot); err == nil {
		previous = active
	} else if !errors.Is(err, os.ErrNotExist) {
		return Result{}, fmt.Errorf("read active runtime: %w", err)
	}
	if source := strings.TrimSpace(request.SourceVersion); source != "" {
		normalizedSource, ok := helperruntime.NormalizeVersion(source)
		if !ok {
			return Result{}, fmt.Errorf("invalid source version %q", request.SourceVersion)
		}
		if previous == "" {
			return Result{}, fmt.Errorf("active runtime %s disappeared while preparing %s", normalizedSource, targetVersion)
		}
		if previous != normalizedSource && previous != targetVersion {
			return Result{}, fmt.Errorf("active runtime changed from %s to %s while preparing %s", normalizedSource, previous, targetVersion)
		}
	}
	pending := Pending{
		Schema:         ProtocolVersion,
		TargetVersion:  targetVersion,
		TargetSHA256:   strings.ToLower(strings.TrimSpace(request.CandidateSHA256)),
		PreviousActive: previous,
		TargetActive:   targetPath,
		RuntimeRoot:    request.RuntimeRoot,
		EntryPath:      request.EntryPath,
		RecordPath:     request.RecordPath,
		RequestID:      request.RequestID,
	}
	installed, err := helperruntime.InstallVersion(request.RuntimeRoot, source, targetVersion, runtime.GOOS, false)
	if err != nil {
		return Result{}, fmt.Errorf("publish immutable runtime: %w", err)
	}
	installedHash, err := fileSHA256(installed)
	if err != nil {
		return Result{}, fmt.Errorf("hash immutable runtime: %w", err)
	}
	if !strings.EqualFold(installedHash, request.CandidateSHA256) {
		return Result{}, fmt.Errorf("immutable runtime sha256 = %s, want %s", installedHash, request.CandidateSHA256)
	}
	if err := helperruntime.EnsureStableEntry(installed, request.EntryPath, runtime.GOOS); err != nil {
		return Result{}, fmt.Errorf("ensure stable cxp entry: %w", err)
	}
	// Publish recovery intent only after the immutable target and stable entry
	// are usable. A previous runtime can then safely finish the transition if
	// this candidate exits before switching active.
	if !resuming {
		if err := writePending(filepath.Join(request.RuntimeRoot, PendingFileName), pending); err != nil {
			return Result{}, err
		}
	}
	if previous != "" && previous != targetVersion {
		if err := helperruntime.SetPrevious(request.RuntimeRoot, previous); err != nil {
			return Result{}, fmt.Errorf("record previous runtime: %w", err)
		}
	}
	if err := helperruntime.Activate(request.RuntimeRoot, targetVersion); err != nil {
		return Result{}, fmt.Errorf("activate target runtime: %w", err)
	}
	if err := updateInstallRecord(request, targetVersion); err != nil {
		return Result{}, err
	}
	return Result{
		Version:     strings.TrimPrefix(targetVersion, "v"),
		RuntimePath: installed,
		EntryPath:   request.EntryPath,
		// Managed runtimes are already activated atomically. The caller still
		// performs its normal service restart, but no file replacement is
		// pending and the legacy RestartRequired path must not be selected.
		RestartRequired: false,
		Noop:            previous == targetVersion,
	}, nil
}

func targetVersion(value string) string {
	version, _ := helperruntime.NormalizeVersion(value)
	return version
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

func verifyFreshEntry(entry string, targetVersion string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, entry, "--version")
	cmd.Env = helperruntime.LauncherEnvironment(os.Environ())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("verify fresh stable entry: %w: %s", err, strings.TrimSpace(string(out)))
	}
	for _, field := range strings.Fields(string(out)) {
		if version, ok := helperruntime.NormalizeVersion(field); ok && version == targetVersion {
			return nil
		}
	}
	return fmt.Errorf("fresh stable entry version output %q does not contain %s", strings.TrimSpace(string(out)), targetVersion)
}

func readContext(path string) (Context, error) {
	var request Context
	info, err := os.Lstat(path)
	if err != nil {
		return request, fmt.Errorf("inspect context: %w", err)
	}
	if !info.Mode().IsRegular() {
		return request, fmt.Errorf("context is not a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return request, fmt.Errorf("context permissions %04o expose update state", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return request, fmt.Errorf("read context: %w", err)
	}
	if err := json.Unmarshal(data, &request); err != nil {
		return request, fmt.Errorf("decode context: %w", err)
	}
	return request, nil
}

func writeInvocationContext(candidate string, request Context) (string, error) {
	dir := filepath.Dir(candidate)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, ".cxp-update-context-")
	if err != nil {
		return "", fmt.Errorf("create candidate update context: %w", err)
	}
	path := f.Name()
	cleanup := true
	defer func() {
		_ = f.Close()
		if cleanup {
			_ = os.Remove(path)
		}
	}()
	if err := f.Chmod(0o600); err != nil {
		return "", err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(request); err != nil {
		return "", err
	}
	if err := f.Sync(); err != nil {
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	cleanup = false
	return path, nil
}

func readPending(path string) (Pending, error) {
	var pending Pending
	info, err := os.Lstat(path)
	if err != nil {
		return pending, err
	}
	if !info.Mode().IsRegular() {
		return pending, fmt.Errorf("pending update %s is not a regular file", path)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return pending, fmt.Errorf("pending update %s permissions %04o are unsafe", path, info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return pending, err
	}
	if err := json.Unmarshal(data, &pending); err != nil {
		return pending, fmt.Errorf("decode pending update %s: %w", path, err)
	}
	if pending.Schema != ProtocolVersion {
		return pending, fmt.Errorf("pending update schema = %d, want %d", pending.Schema, ProtocolVersion)
	}
	return pending, nil
}

func writePending(path string, pending Pending) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-update-")
	if err != nil {
		return fmt.Errorf("create pending update: %w", err)
	}
	tmp := f.Name()
	cleanup := true
	defer func() {
		_ = f.Close()
		if cleanup {
			_ = os.Remove(tmp)
		}
	}()
	if err := f.Chmod(0o600); err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(pending); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := replaceStateFile(tmp, path); err != nil {
		return fmt.Errorf("publish pending update: %w", err)
	}
	cleanup = false
	return syncDirectory(filepath.Dir(path))
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
