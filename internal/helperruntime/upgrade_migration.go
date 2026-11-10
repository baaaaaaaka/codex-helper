package helperruntime

// These helpers belong only to the one-time updater handoff. Ordinary runtime
// selection never decodes JSON or maintains a legacy active-version mirror.
import (
	"context"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// LegacyUpgradeSnapshot may be called only while holding the runtime root lock.
func LegacyUpgradeSnapshot(root string) (BootState, error) {
	for _, path := range []string{filepath.Join(root, BootFile), filepath.Join(root, BootFile) + ".published"} {
		if _, err := os.Lstat(path); err == nil {
			return BootState{}, fmt.Errorf("legacy updater cannot change a published native bootstrap")
		} else if !os.IsNotExist(err) {
			return BootState{}, err
		}
	}
	s, found, err := readLegacyBootForInstall(root)
	if err != nil || found {
		return s, err
	}
	active, err := readVersionPointer(root, "active")
	if err != nil {
		return s, err
	}
	return BootState{Schema: 1, Generation: 1, Good: active}, nil
}

// WriteUpgradeReceipt satisfies the historical caller's read-back contract.
// It is written once during migration, then archived by the native admission.
// The fixed active pointer forwards old dispatchers to the migration binary;
// it never follows subsequent native updates and is not a second authority.
func WriteUpgradeReceipt(root string, old BootState, target string) error {
	if old.Generation == ^uint64(0) {
		return fmt.Errorf("bootstrap generation exhausted")
	}
	if err := WriteAtomicState(root, "active", []byte(target+"\n")); err != nil {
		return err
	}
	if _, err := os.Lstat(filepath.Join(root, "boot.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	receipt := BootState{Schema: 1, Generation: old.Generation + 1, Good: target}
	b, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return WriteAtomicState(root, "boot.json", b)
}

// HasUpgradeForwarder checks only the bounded migration receipt. Runtime
// selection remains in boot.sqlite, even while an old Windows image is mapped.
func HasUpgradeForwarder(root, target string) bool {
	v, err := readVersionPointer(root, "active")
	return err == nil && v == target
}

// RemoveUpgradeForwarder is called only after all physical entries are native.
func RemoveUpgradeForwarder(root, target string) error {
	if _, err := os.Lstat(filepath.Join(root, "active")); os.IsNotExist(err) {
		return nil
	}
	if !HasUpgradeForwarder(root, target) {
		return fmt.Errorf("migration forwarder changed")
	}
	if err := os.Remove(filepath.Join(root, "active")); err != nil {
		return err
	}
	return syncDir(root)
}

// RemoveObsoleteUpgradeForwarder finishes a crash after the native receipt
// was removed. The caller holds the root lock and has verified native entries.
func RemoveObsoleteUpgradeForwarder(root string) error {
	if _, err := ReadBoot(root); err != nil {
		return err
	}
	target, err := readVersionPointer(root, "active")
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return RemoveUpgradeForwarder(root, target)
}

// AdmitUpgradeBootstrap reuses explicit installation's atomic native publication.
// The updater has already verified the immutable source and installation owner.
func AdmitUpgradeBootstrap(ctx context.Context, root, source, version string) error {
	return installBootstrap(ctx, root, source, version, false)
}

// ForwardUpgradeInvocation uses native selection even when an old physical
// dispatcher enters the fixed migration binary after a later native update.
func ForwardUpgradeInvocation(root, entry, version string, args []string) (int, error) {
	boot, err := ReadBoot(root)
	if err != nil {
		return 1, err
	}
	if boot.Good != version {
		return 1, fmt.Errorf("native bootstrap changed during migration forwarding")
	}
	code, _, err := launchBoot(root, entry, args, boot)
	return code, err
}

// ReplaceUpgradeEntry preserves a user symlink and refuses non-CXP files. The
// caller holds the existing root lock. Windows may defer this exact operation
// until the old image mapping is released; no permissions are weakened.
func ReplaceUpgradeEntry(source, entry string) error {
	target, err := filepath.EvalSymlinks(entry)
	if err != nil {
		return err
	}
	source, err = filepath.Abs(source)
	if err != nil {
		return err
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return err
	}
	entryDir, err := filepath.EvalSymlinks(filepath.Dir(entry))
	if err != nil {
		return err
	}
	sameDir := filepath.Dir(target) == entryDir
	if runtime.GOOS == "windows" {
		sameDir = strings.EqualFold(filepath.Dir(target), entryDir)
	}
	if !sameDir {
		return fmt.Errorf("upgrade entry escapes its installation directory")
	}
	want, err := fileHash(source)
	if err != nil {
		return err
	}
	before, err := fileHash(target)
	if err != nil {
		return err
	}
	if before == want {
		return nil
	}
	info, err := buildinfo.ReadFile(target)
	if err != nil || info.Path != "github.com/baaaaaaaka/codex-helper/cmd/codex-proxy" {
		return fmt.Errorf("refuse to replace an unrecognized cxp entry: %s", target)
	}
	f, err := os.CreateTemp(filepath.Dir(target), ".cxp-bootstrap-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if err = f.Close(); err != nil {
		os.Remove(name)
		return err
	}
	defer os.Remove(name)
	if err = os.Remove(name); err != nil {
		return err
	}
	if err = copyExecutable(source, name); err != nil {
		return err
	}
	// Recheck after the copy. An updater that changed the entry invalidates
	// this migration's replacement instead of being silently overwritten.
	latest, err := fileHash(target)
	if err != nil || latest != before {
		return fmt.Errorf("bootstrap changed during migration (expected %s): %v", hex.EncodeToString(before[:]), err)
	}
	if err = replaceActiveFile(name, target); err != nil {
		return err
	}
	return syncDir(filepath.Dir(target))
}
