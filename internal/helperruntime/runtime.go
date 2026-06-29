package helperruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/gofrs/flock"
	"golang.org/x/mod/semver"
)

const (
	EnvRuntime        = "CXP_RUNTIME"
	EnvRuntimeRoot    = "CXP_RUNTIME_ROOT"
	EnvRuntimeVersion = "CXP_RUNTIME_VERSION"
	EnvEntryPath      = "CXP_ENTRY_PATH"
	EnvDisable        = "CXP_RUNTIME_DISABLE"
	EnvForce          = "CXP_RUNTIME_FORCE"
)

type Context struct {
	Root        string
	EntryPath   string
	Version     string
	RuntimePath string
}

var executablePath = helperpath.RawExecutable

func BinaryName(goos string) string {
	if strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return "cxp.exe"
	}
	return "cxp"
}

func Current() (Context, bool) {
	if os.Getenv(EnvRuntime) != "1" {
		return Context{}, false
	}
	root := filepath.Clean(strings.TrimSpace(os.Getenv(EnvRuntimeRoot)))
	entry := filepath.Clean(strings.TrimSpace(os.Getenv(EnvEntryPath)))
	if root == "" || root == "." || entry == "" || entry == "." {
		return Context{}, false
	}
	version, ok := NormalizeVersion(os.Getenv(EnvRuntimeVersion))
	if !ok {
		// Compatibility fallback for a process entered by an older launcher.
		// New launchers always pin the exact running version so a concurrent
		// activation cannot make this process identify itself as a newer one.
		var err error
		version, err = ReadActive(root)
		if err != nil {
			return Context{}, false
		}
	}
	return Context{
		Root:        root,
		EntryPath:   entry,
		Version:     version,
		RuntimePath: VersionPath(root, version, runtime.GOOS),
	}, true
}

// Launch enters the active immutable runtime before normal CLI initialization.
// On Unix, a successful launch replaces the current process and never returns.
// On Windows, it waits for the cxp.exe runtime and returns its exit code.
func Launch(version string, args []string) (exitCode int, handled bool, err error) {
	if os.Getenv(EnvDisable) == "1" || os.Getenv(EnvRuntime) == "1" {
		return 0, false, nil
	}
	normalized, ok := NormalizeVersion(version)
	if !ok {
		return 0, false, nil
	}
	raw, err := executablePath()
	if err != nil {
		return 0, false, err
	}
	if shouldSkipDevelopmentExecutable(raw) && os.Getenv(EnvForce) != "1" {
		return 0, false, nil
	}
	physical := raw
	if resolved, resolveErr := filepath.EvalSymlinks(raw); resolveErr == nil && strings.TrimSpace(resolved) != "" {
		physical = resolved
	}
	physical, err = filepath.Abs(physical)
	if err != nil {
		return 0, false, err
	}
	root := filepath.Join(filepath.Dir(physical), ".cxp-runtime")
	entry := filepath.Join(filepath.Dir(physical), BinaryName(runtime.GOOS))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lock, err := lockRoot(ctx, root)
	if err != nil {
		return 0, false, err
	}
	locked := true
	defer func() {
		if locked {
			_ = lock.Unlock()
		}
	}()

	if err := EnsureStableEntry(physical, entry, runtime.GOOS); err != nil {
		return 0, false, err
	}
	active, activeErr := ReadActive(root)
	if errors.Is(activeErr, os.ErrNotExist) {
		if _, err := InstallVersion(root, physical, normalized, runtime.GOOS, runtime.GOOS == "linux"); err != nil {
			return 0, false, err
		}
		if err := Activate(root, normalized); err != nil {
			return 0, false, err
		}
		active = normalized
	} else if activeErr != nil {
		return 0, false, activeErr
	} else if semver.Compare(normalized, active) > 0 {
		if _, err := InstallVersion(root, physical, normalized, runtime.GOOS, runtime.GOOS == "linux"); err != nil {
			return 0, false, err
		}
		if err := Activate(root, normalized); err != nil {
			return 0, false, err
		}
		active = normalized
	}
	target := VersionPath(root, active, runtime.GOOS)
	if info, statErr := os.Stat(target); statErr != nil || !info.Mode().IsRegular() {
		if statErr != nil {
			return 0, false, fmt.Errorf("active cxp runtime %s is unavailable: %w", target, statErr)
		}
		return 0, false, fmt.Errorf("active cxp runtime %s is not a regular file", target)
	}
	if err := lock.Unlock(); err != nil {
		return 0, false, err
	}
	locked = false
	launchArgs := append([]string{target}, args[1:]...)
	env := setRuntimeEnvironment(os.Environ(), root, entry, active)
	return launchRuntime(target, launchArgs, env)
}

func NormalizeVersion(value string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) == 0 {
		return "", false
	}
	value = fields[0]
	if !strings.HasPrefix(value, "v") {
		value = "v" + value
	}
	if !semver.IsValid(value) {
		return "", false
	}
	return value, true
}

func VersionPath(root string, version string, goos string) string {
	normalized, ok := NormalizeVersion(version)
	if !ok {
		return ""
	}
	return filepath.Join(filepath.Clean(root), "versions", normalized, BinaryName(goos))
}

func ReadActive(root string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(filepath.Clean(root), "active"))
	if err != nil {
		return "", err
	}
	value, ok := NormalizeVersion(string(raw))
	if !ok {
		return "", fmt.Errorf("invalid cxp active runtime value %q", strings.TrimSpace(string(raw)))
	}
	return value, nil
}

func Activate(root string, version string) error {
	normalized, ok := NormalizeVersion(version)
	if !ok {
		return fmt.Errorf("invalid cxp runtime version %q", version)
	}
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(root, ".active-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := io.WriteString(tmp, normalized+"\n"); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceActiveFile(tmpPath, filepath.Join(root, "active")); err != nil {
		return err
	}
	cleanup = false
	return syncDir(root)
}

func InstallVersion(root string, source string, version string, goos string, preferHardlink bool) (string, error) {
	target := VersionPath(root, version, goos)
	if target == "" {
		return "", fmt.Errorf("invalid cxp runtime version %q", version)
	}
	if same, err := sameFileContent(source, target); err == nil && same {
		return target, nil
	} else if err == nil {
		return "", fmt.Errorf("immutable cxp runtime %s already exists with different content", target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".cxp-")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := os.Remove(tmpPath); err != nil {
		return "", err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if preferHardlink {
		err = os.Link(source, tmpPath)
	}
	if !preferHardlink || err != nil {
		if err := copyExecutable(source, tmpPath); err != nil {
			return "", err
		}
	}
	if err := os.Rename(tmpPath, target); err != nil {
		if same, sameErr := sameFileContent(source, target); sameErr == nil && same {
			return target, nil
		}
		return "", err
	}
	cleanup = false
	if err := syncDir(filepath.Dir(target)); err != nil {
		return "", err
	}
	return target, nil
}

// PublishDownloaded atomically publishes an already validated candidate and
// only then switches active. Version directories are immutable.
func PublishDownloaded(root string, candidate string, version string, goos string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lock, err := lockRoot(ctx, root)
	if err != nil {
		return "", err
	}
	defer func() { _ = lock.Unlock() }()
	target := VersionPath(root, version, goos)
	if target == "" {
		return "", fmt.Errorf("invalid cxp runtime version %q", version)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", err
	}
	if same, err := sameFileContent(candidate, target); err == nil && same {
		_ = os.Remove(candidate)
	} else if err == nil {
		return "", fmt.Errorf("immutable cxp runtime %s already exists with different content", target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	} else if err := os.Rename(candidate, target); err != nil {
		return "", err
	}
	if err := Activate(root, version); err != nil {
		return "", err
	}
	return target, nil
}

func LauncherEnvironment(env []string) []string {
	out := make([]string, 0, len(env))
	for _, value := range env {
		name, _, _ := strings.Cut(value, "=")
		blocked := false
		for _, runtimeName := range []string{EnvRuntime, EnvRuntimeRoot, EnvRuntimeVersion, EnvEntryPath} {
			if strings.EqualFold(name, runtimeName) {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}
		out = append(out, value)
	}
	return out
}

func EnsureStableEntry(source string, entry string, goos string) error {
	source = filepath.Clean(source)
	entry = filepath.Clean(entry)
	if source == entry {
		return nil
	}
	if _, err := os.Lstat(entry); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(entry), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(entry), ".cxp-entry-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Remove(tmpPath); err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if !strings.EqualFold(goos, "windows") {
		err = os.Link(source, tmpPath)
	}
	if strings.EqualFold(goos, "windows") || err != nil {
		if err := copyExecutable(source, tmpPath); err != nil {
			return err
		}
	}
	if err := os.Rename(tmpPath, entry); err != nil {
		if _, statErr := os.Lstat(entry); statErr == nil {
			return nil
		}
		return err
	}
	cleanup = false
	return syncDir(filepath.Dir(entry))
}

func lockRoot(ctx context.Context, root string) (*flock.Flock, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	lock := flock.New(filepath.Join(root, "runtime.lock"))
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, fmt.Errorf("timed out waiting for cxp runtime lock at %s", root)
	}
	return lock, nil
}

func copyExecutable(source string, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm()|0o700)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(target)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(target)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(target)
		return err
	}
	return nil
}

func sameFileContent(left string, right string) (bool, error) {
	leftHash, err := fileHash(left)
	if err != nil {
		return false, err
	}
	rightHash, err := fileHash(right)
	if err != nil {
		return false, err
	}
	return leftHash == rightHash, nil
}

func fileHash(path string) ([sha256.Size]byte, error) {
	var out [sha256.Size]byte
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return out, err
	}
	copy(out[:], h.Sum(nil))
	return out, nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil && !strings.EqualFold(runtime.GOOS, "windows") {
		return err
	}
	return nil
}

func setRuntimeEnvironment(env []string, root string, entry string, version string) []string {
	out := LauncherEnvironment(env)
	return append(out, EnvRuntime+"=1", EnvRuntimeRoot+"="+root, EnvRuntimeVersion+"="+version, EnvEntryPath+"="+entry)
}

func shouldSkipDevelopmentExecutable(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	if strings.HasSuffix(base, ".test") || strings.HasSuffix(base, ".test.exe") {
		return true
	}
	for _, part := range strings.FieldsFunc(filepath.Clean(path), func(r rune) bool { return r == '/' || r == '\\' }) {
		if strings.HasPrefix(part, "go-build") {
			return true
		}
	}
	return false
}
