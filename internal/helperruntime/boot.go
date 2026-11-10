package helperruntime

// Automatic updates never replace the stable entry or the legacy active pointer.
// Once present, boot.sqlite is the sole authority for runtime selection. A trial
// consumes its opportunity before execution while Good still names the old
// runtime. Failure recovery therefore requires no successful write.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"github.com/gofrs/flock"
	"golang.org/x/mod/semver"
)

const BootFile = "boot.sqlite"
const maxBootRecordSize = 64 * 1024

// A protocol change also needs a migration bridge for supported historical
// updaters; refusing their download is not an automatic upgrade contract.
const BootProtocol = 2
const EnvBootManaged = "CXP_BOOT_MANAGED"

type BootCandidate struct {
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
	Attempt string `json:"attempt,omitempty"`
}

type BootState struct {
	Schema     int            `json:"schema"`
	Generation uint64         `json:"generation"`
	Good       string         `json:"good"`
	Previous   string         `json:"previous,omitempty"`
	Candidate  *BootCandidate `json:"candidate,omitempty"`
}

func ReadBoot(root string) (BootState, error) {
	return readNativeBoot(root)
}

func validateBoot(s BootState) error {
	valid := func(v string) bool { n, ok := NormalizeVersion(v); return ok && n == v }
	if s.Schema != BootProtocol || s.Generation == 0 || !valid(s.Good) || (s.Previous != "" && !valid(s.Previous)) {
		return fmt.Errorf("unsupported or invalid boot record")
	}
	if c := s.Candidate; c != nil {
		h, err := hex.DecodeString(c.SHA256)
		attempt, attemptErr := hex.DecodeString(c.Attempt)
		if !valid(c.Version) || semver.Compare(c.Version, s.Good) <= 0 || err != nil || len(h) != 32 || attemptErr != nil || (c.Attempt != "" && len(attempt) != 16) {
			return fmt.Errorf("invalid boot candidate")
		}
	}
	return nil
}

// writeBoot is a single-file commit, using the same platform replacement and
// durability helpers as legacy runtime activation. No cache/log write is part
// of this transaction. Tests inject failures at the commit boundary.
var commitBootFile = replaceActiveFile
var syncBootDir = syncDir

type stateFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Name() string
}

var createStateFile = func(root, pattern string) (stateFile, error) { return os.CreateTemp(root, pattern) }

func writeBoot(root string, s BootState) error {
	if err := validateBoot(s); err != nil {
		return err
	}
	b, err := encodeNativeBoot(root, s)
	if err != nil {
		return err
	}
	if err = WriteAtomicState(root, BootFile, b); err != nil {
		return err
	}
	db, err := sqlitestate.Open(filepath.Join(root, BootFile), true)
	if err != nil {
		return err
	}
	defer db.Close()
	return sqlitestate.Seal(db)
}

// WriteAtomicState writes a small, fixed-name record beside the runtime. Callers
// serialize mutations; startup readers require neither a lock nor write access.
func WriteAtomicState(root, base string, b []byte) error {
	if base == "" || base == "." || base == ".." || filepath.Base(base) != base || len(b) > maxBootRecordSize {
		return fmt.Errorf("invalid state record")
	}
	if err := cleanupBootTemps(root, "."+base+"-"); err != nil {
		return err
	}
	f, err := createStateFile(root, "."+base+"-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	n, err := f.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = commitBootFile(name, filepath.Join(root, base)); err != nil {
		return err
	}
	return syncBootDir(root)
}

// StageBootCandidate publishes verified bytes but leaves the default unchanged.
// base is captured before download, fencing concurrent explicit updates/rollbacks.
func StageBootCandidate(ctx context.Context, current Context, base string, generation uint64, source, version, digest string) error {
	version, ok := NormalizeVersion(version)
	if !ok {
		return fmt.Errorf("invalid candidate version")
	}
	h, err := fileHash(source)
	if err != nil {
		return err
	}
	if !strings.EqualFold(hex.EncodeToString(h[:]), digest) {
		return fmt.Errorf("candidate digest mismatch")
	}
	return WithRootLock(ctx, current.Root, func() error {
		s, err := ReadBoot(current.Root)
		if err != nil {
			return fmt.Errorf("%w: initialize the native bootstrap with the installer: %v", ErrBootstrapState, err)
		}
		if s.Good != base || generation != s.Generation {
			return fmt.Errorf("runtime selection changed during download")
		}
		if semver.Compare(version, s.Good) <= 0 {
			return nil
		}
		if s.Candidate != nil && s.Candidate.Version == version {
			return nil
		}
		if s.Candidate != nil && semver.Compare(version, s.Candidate.Version) <= 0 {
			return nil
		}
		if _, err := checkedRuntime(current.Root, s.Good); err != nil {
			return err
		}
		if err := checkAutoDestination(current.Root, version); err != nil {
			return err
		}
		if err := CheckAutoStorage(current.Root, version); err != nil {
			return err
		}
		if err := cleanupBootTemps(filepath.Dir(VersionPath(current.Root, version, runtime.GOOS)), ".cxp-"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := InstallVersion(current.Root, source, version, runtime.GOOS, false); err != nil {
			return err
		}
		s.Generation++
		s.Candidate = &BootCandidate{Version: version, SHA256: strings.ToLower(digest)}
		return writeBoot(current.Root, s)
	})
}

func checkAutoDestination(root, version string) error {
	physicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	for _, path := range []string{filepath.Join(root, "versions"), filepath.Dir(VersionPath(root, version, runtime.GOOS))} {
		physical, err := filepath.EvalSymlinks(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(physicalRoot, physical)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("update directory escapes managed root")
		}
	}
	if info, err := os.Lstat(VersionPath(root, version, runtime.GOOS)); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("symlinked immutable runtime")
	}
	return nil
}

// The caller owns the corresponding mutation lock. These are unpublished
// private temporaries, never an entry or an immutable executable.
func cleanupBootTemps(dir, prefix string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) && !entry.IsDir() {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

// Do not garbage-collect binaries belonging to windows that may stay open for
// weeks. Bound retention instead; a full install quietly defers automatic work.
// Explicit upgrades remain available. The single download cache is also bounded.
func CheckAutoStorage(root, version string) error {
	entries, err := os.ReadDir(filepath.Join(root, "versions"))
	if err != nil {
		return err
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			if entry.Name() == version {
				return nil
			}
			count++
		}
	}
	if count >= 8 {
		return fmt.Errorf("automatic runtime retention limit reached (8 versions); explicit upgrade remains available")
	}
	return nil
}

// tryBootMutation never queues startup behind an update. A busy/read-only root
// just means using the already-known-good version.
func tryBootMutation(root string, fn func(BootState) error) error {
	l := flock.New(filepath.Join(root, "runtime.lock"))
	ok, err := l.TryLock()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("runtime update busy")
	}
	defer l.Unlock()
	s, err := ReadBoot(root)
	if err != nil {
		return err
	}
	return fn(s)
}

func consumeBootCandidate(root string, snapshot BootState) (BootState, bool) {
	var claimed BootState
	err := tryBootMutation(root, func(s BootState) error {
		if s.Generation != snapshot.Generation || s.Candidate == nil || s.Candidate.Attempt != "" {
			return fmt.Errorf("candidate unavailable")
		}
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return err
		}
		s.Candidate.Attempt = hex.EncodeToString(token[:])
		s.Generation++
		if err := writeBoot(root, s); err != nil {
			return err
		}
		claimed = s
		return nil
	})
	if err != nil {
		return BootState{}, false
	}
	// Even a missing/tampered candidate consumes its one chance. Future starts
	// must not repeat a potentially expensive failing validation.
	path, err := checkedRuntime(root, claimed.Candidate.Version)
	if err != nil {
		return claimed, false
	}
	h, err := fileHash(path)
	return claimed, err == nil && strings.EqualFold(hex.EncodeToString(h[:]), claimed.Candidate.SHA256)
}

func confirmBootCandidate(root string, claim BootState) {
	_ = tryBootMutation(root, func(s BootState) error {
		if s.Generation != claim.Generation || s.Good != claim.Good || s.Candidate == nil || claim.Candidate == nil || s.Candidate.Attempt != claim.Candidate.Attempt {
			return fmt.Errorf("stale boot confirmation")
		}
		s.Previous, s.Good = s.Good, s.Candidate.Version
		s.Candidate = nil
		s.Generation++
		return writeBoot(root, s)
	})
}

func rejectBootGood(root string, snapshot BootState) {
	if snapshot.Previous == "" {
		return
	}
	_ = tryBootMutation(root, func(s BootState) error {
		if s.Generation != snapshot.Generation || s.Good != snapshot.Good {
			return fmt.Errorf("stale boot failure")
		}
		s.Good, s.Previous = s.Previous, ""
		s.Candidate = nil
		s.Generation++
		return writeBoot(root, s)
	})
}

func checkedRuntime(root, version string) (string, error) {
	path := VersionPath(root, version, runtime.GOOS)
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 || (runtime.GOOS != "windows" && info.Mode()&0111 == 0) {
		return "", fmt.Errorf("runtime is not executable")
	}
	physicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	physical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(physicalRoot, physical)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("runtime escapes managed root")
	}
	return path, nil
}

// BootSnapshot is also the download comparison baseline. Old windows must not
// compare releases against their own, potentially weeks-old process version.
func BootSnapshot(root string) (string, uint64, error) {
	s, err := ReadBoot(root)
	if err == nil {
		return s.Good, s.Generation, nil
	}
	return "", 0, err
}

// A small constant bound is independent of network request/update timeouts.
const bootReadyTimeout = 10 * time.Second
