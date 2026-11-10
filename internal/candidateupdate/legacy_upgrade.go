package candidateupdate

// Legacy transport is accepted only at this migration boundary. Runtime
// business stores and subsequent updater inputs keep their native SQLite APIs.
import (
	"context"
	"encoding/json"
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
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
)

const legacyUpgradePrefix = "sqlite-bootstrap:"

func handleLegacyUpdate(args []string, version string, stdout, stderr io.Writer) (int, bool) {
	legacy := false
	for _, arg := range args[2:] {
		legacy = legacy || arg == "--protocol=1"
	}
	if !legacy {
		return 0, false
	}
	if len(args) != 4 || args[2] != "--protocol=1" || !strings.HasPrefix(args[3], "--context-file=") {
		fmt.Fprintln(stderr, "legacy migration requires protocol=1 and exactly one context-file")
		return 2, true
	}
	r, err := readLegacyInvocation(strings.TrimPrefix(args[3], "--context-file="))
	var result Result
	if err == nil {
		result, err = applyLegacyUpgrade(context.Background(), r, version)
	}
	if err != nil {
		fmt.Fprintf(stderr, "legacy upgrade migration: %v\n", err)
		return 1, true
	}
	if err = json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1, true
	}
	return 0, true
}

func readLegacyInvocation(path string) (Context, error) {
	var r Context
	before, err := os.Lstat(path)
	if err != nil {
		return r, err
	}
	if !before.Mode().IsRegular() || before.Size() > 64<<10 {
		return r, fmt.Errorf("invalid legacy updater request file")
	}
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return r, err
	}
	if !os.SameFile(before, info) || !info.Mode().IsRegular() || info.Size() > 64<<10 || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return r, fmt.Errorf("invalid legacy updater request file")
	}
	d := json.NewDecoder(io.LimitReader(f, (64<<10)+1))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, err
	}
	if _, err = d.Token(); err != io.EOF {
		return r, fmt.Errorf("trailing legacy updater request")
	}
	return r, nil
}

func applyLegacyUpgrade(ctx context.Context, r Context, version string) (Result, error) {
	if err := validateContext(r, version); err != nil {
		return Result{}, err
	}
	source, err := candidateExecutablePath()
	if err != nil {
		return Result{}, err
	}
	digest, err := fileSHA256(source)
	if err != nil || !strings.EqualFold(digest, r.CandidateSHA256) {
		return Result{}, fmt.Errorf("legacy candidate hash mismatch: %v", err)
	}
	if !samePath(source, r.CandidatePath) {
		return Result{}, fmt.Errorf("legacy invocation does not identify its executing candidate")
	}
	var p updatestate.Pending
	err = helperruntime.WithRootLock(ctx, r.RuntimeRoot, func() error {
		old, err := helperruntime.LegacyUpgradeSnapshot(r.RuntimeRoot)
		if err != nil {
			return err
		}
		target, _ := helperruntime.NormalizeVersion(r.TargetVersion)
		from, ok := helperruntime.NormalizeVersion(r.SourceVersion)
		if prior, priorErr := migrationPending(r.RuntimeRoot); priorErr == nil {
			if prior.TargetVersion != target || prior.PreviousActive != from || prior.TargetSHA256 != digest || !samePath(prior.EntryPath, r.EntryPath) {
				return fmt.Errorf("another migration owns this bootstrap")
			}
			p = prior
			if old.Good == target && helperruntime.HasUpgradeForwarder(r.RuntimeRoot, target) {
				return validateMigration(p)
			}
		} else if !os.IsNotExist(priorErr) {
			return priorErr
		}
		if !ok || old.Good != from {
			return fmt.Errorf("legacy source changed from %s to %s", r.SourceVersion, old.Good)
		}
		installed, err := helperruntime.InstallVersion(r.RuntimeRoot, source, target, runtime.GOOS, false)
		if err != nil {
			return err
		}
		if err = verifyImmutableRuntime(ctx, installed, target); err != nil {
			return err
		}
		p = updatestate.Pending{Schema: 1, TargetVersion: target, TargetSHA256: digest,
			PreviousActive: from, TargetActive: installed, RuntimeRoot: r.RuntimeRoot,
			EntryPath: r.EntryPath, RecordPath: r.RecordPath, RequestID: legacyUpgradePrefix + r.RequestID}
		s, err := updatestate.Open(r.RuntimeRoot)
		if err != nil {
			return err
		}
		err = s.SavePending(p)
		s.Close()
		if err != nil {
			return err
		}
		// The historical caller reads its own boot/active format after this
		// process returns, then records its final core update and exits. Do not
		// migrate application state or archive that caller's receipt here.
		return helperruntime.WriteUpgradeReceipt(r.RuntimeRoot, old, target)
	})
	if err != nil {
		return Result{}, err
	}
	if err = repairUpgradeEntries(ctx, p); err != nil {
		if !deferUpgradeEntry(err) {
			return Result{}, err
		}
		if err = scheduleUpgradeRepair(p); err != nil {
			return Result{}, err
		}
	}
	return Result{Version: strings.TrimPrefix(p.TargetVersion, "v"), RuntimePath: p.TargetActive, EntryPath: p.EntryPath}, nil
}

func migrationPending(root string) (updatestate.Pending, error) {
	// Do not invoke the legacy importer from startup/probes. Only a native
	// receipt prepared by the migration entry may authorize a bootstrap repair.
	if _, err := sqlitestate.Inspect(filepath.Join(root, updatestate.Filename)); err != nil {
		return updatestate.Pending{}, err
	}
	s, err := updatestate.OpenRead(root)
	if err != nil {
		return updatestate.Pending{}, err
	}
	defer s.Close()
	p, err := s.Pending()
	if err == nil && !strings.HasPrefix(p.RequestID, legacyUpgradePrefix) {
		err = os.ErrNotExist
	}
	return p, err
}

func validateMigration(p updatestate.Pending) error {
	if p.Schema != 1 || !strings.HasPrefix(p.RequestID, legacyUpgradePrefix) {
		return fmt.Errorf("invalid bootstrap migration receipt")
	}
	r := Context{Schema: 1, TargetVersion: p.TargetVersion, CandidateSHA256: p.TargetSHA256,
		RuntimeRoot: p.RuntimeRoot, EntryPath: p.EntryPath, RequestID: p.RequestID}
	if err := validateStateContext(r); err != nil {
		return err
	}
	if !samePath(p.TargetActive, helperruntime.VersionPath(p.RuntimeRoot, p.TargetVersion, runtime.GOOS)) {
		return fmt.Errorf("migration source is outside its immutable runtime")
	}
	h, err := fileSHA256(p.TargetActive)
	if err != nil || !strings.EqualFold(h, p.TargetSHA256) {
		return fmt.Errorf("migration source changed: %v", err)
	}
	return nil
}

func repairUpgradeEntries(ctx context.Context, p updatestate.Pending) error {
	if err := validateMigration(p); err != nil {
		return err
	}
	return helperruntime.WithRootLock(ctx, p.RuntimeRoot, func() error {
		current, err := migrationPending(p.RuntimeRoot)
		if err != nil || current.RequestID != p.RequestID || current.TargetSHA256 != p.TargetSHA256 {
			return fmt.Errorf("migration repair owner changed: %v", err)
		}
		for i, entry := range upgradeEntries(p) {
			if _, err := os.Lstat(entry); os.IsNotExist(err) {
				if i != 0 {
					continue
				}
				return err
			} else if err != nil {
				return err
			}
			if helperruntime.ProbeProtocol(ctx, entry) == nil {
				continue // never overwrite an already-native/newer bootstrap
			}
			if err := upgradeEntryAvailable(entry); err != nil {
				return err
			}
			if err := helperruntime.ReplaceUpgradeEntry(p.TargetActive, entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func upgradeEntries(p updatestate.Pending) []string {
	entries := []string{p.EntryPath}
	for _, name := range []string{helperruntime.BinaryName(runtime.GOOS), helperpath.BinaryName(runtime.GOOS)} {
		path := filepath.Join(filepath.Dir(p.EntryPath), name)
		found := false
		for _, entry := range entries {
			found = found || samePath(entry, path)
		}
		if !found {
			entries = append(entries, path)
		}
	}
	return entries
}

func ownsUpgradeEntry(p updatestate.Pending, entry string) bool {
	for _, owned := range upgradeEntries(p) {
		if samePath(owned, entry) {
			return true
		}
	}
	return false
}

// CheckBootstrapEntry permits a *verified, fixed* migration forwarder while a
// Windows old launcher still maps its image. Native boot remains authoritative.
func CheckBootstrapEntry(ctx context.Context, root, entry string) error {
	err := helperruntime.ProbeProtocol(ctx, entry)
	if err == nil {
		return nil
	}
	p, readErr := migrationPending(root)
	if readErr != nil || !samePath(p.RuntimeRoot, root) || !ownsUpgradeEntry(p, entry) || validateMigration(p) != nil || !helperruntime.HasUpgradeForwarder(root, p.TargetVersion) {
		return err
	}
	if _, readErr = helperruntime.ReadBoot(root); readErr != nil {
		return err
	}
	out, probeErr := helperruntime.ProbeExecutable(ctx, entry, []string{helperruntime.ProtocolProbeFlag}, 10*time.Second)
	if probeErr != nil || strings.TrimSpace(string(out)) != "1" {
		return err
	}
	return nil
}

// The historical TUI stages a boot trial directly instead of calling the
// candidate updater. Accept only the claimed, hashed immutable trial entered
// by that launcher. SETUP prevents its parent from replaying an old runtime.
func adoptLegacyTrial(ctx context.Context, root, raw, version string) (updatestate.Pending, error) {
	current, ok := helperruntime.Current()
	v, valid := helperruntime.NormalizeVersion(version)
	if !ok || !valid || current.Version != v || os.Getenv(helperruntime.EnvBootManaged) != "1" || !samePath(raw, helperruntime.VersionPath(root, v, runtime.GOOS)) {
		return updatestate.Pending{}, os.ErrNotExist
	}
	old, err := helperruntime.LegacyUpgradeSnapshot(root)
	if err != nil {
		if _, nativeErr := helperruntime.ReadBoot(root); nativeErr == nil {
			return updatestate.Pending{}, os.ErrNotExist
		}
		return updatestate.Pending{}, fmt.Errorf("%w: legacy trial admission: %v", helperruntime.ErrBootstrapState, err)
	}
	if old.Candidate == nil || old.Candidate.Version != v || old.Candidate.Attempt == "" {
		return updatestate.Pending{}, fmt.Errorf("%w: legacy trial is not claimed by this runtime", helperruntime.ErrBootstrapState)
	}
	digest, err := fileSHA256(raw)
	if err != nil || !strings.EqualFold(digest, old.Candidate.SHA256) {
		return updatestate.Pending{}, fmt.Errorf("legacy trial digest changed")
	}
	if err = helperruntime.BeforeInteractiveSetup(); err != nil {
		return updatestate.Pending{}, err
	}
	p := updatestate.Pending{Schema: 1, TargetVersion: v, TargetSHA256: digest, PreviousActive: old.Good, TargetActive: raw, RuntimeRoot: root, EntryPath: current.EntryPath, RequestID: legacyUpgradePrefix + old.Candidate.Attempt}
	err = helperruntime.WithRootLock(ctx, root, func() error {
		latest, err := helperruntime.LegacyUpgradeSnapshot(root)
		if err != nil {
			return err
		}
		if latest.Generation != old.Generation || latest.Candidate == nil || *latest.Candidate != *old.Candidate {
			return fmt.Errorf("legacy trial owner changed")
		}
		s, err := updatestate.Open(root)
		if err != nil {
			return err
		}
		err = s.SavePending(p)
		s.Close()
		if err != nil {
			return err
		}
		return helperruntime.WriteUpgradeReceipt(root, old, v)
	})
	return p, err
}

// A crash may leave the scalar after deleting its native receipt. A native
// launcher needs no legacy state, so verify all known physical entries before
// removing that obsolete pointer. This also finishes explicit old installers.
func cleanupObsoleteForwarder(ctx context.Context, root, entry string) error {
	return helperruntime.WithRootLock(ctx, root, func() error {
		if _, err := migrationPending(root); !os.IsNotExist(err) {
			return err
		}
		for i, path := range upgradeEntries(updatestate.Pending{EntryPath: entry}) {
			if _, err := os.Lstat(path); os.IsNotExist(err) && i != 0 {
				continue
			} else if err != nil {
				return err
			}
			if helperruntime.ProbeProtocol(ctx, path) != nil {
				return nil
			} // still mapped/old: keep forwarding
		}
		return helperruntime.RemoveObsoleteUpgradeForwarder(root)
	})
}

// HandleLegacyMigration runs before normal dispatch. Version probes are pure;
// the first real invocation admits native boot only after historical handoff.
func HandleLegacyMigration(args []string, version string, stdout, stderr io.Writer) (int, bool) {
	if len(args) == 2 && args[1] == "--version" && os.Getenv(helperruntime.EnvDisable) == "1" {
		fmt.Fprintf(stdout, "cxp version %s\n", strings.TrimPrefix(version, "v"))
		return 0, true
	}
	raw, err := candidateExecutablePath()
	if err != nil {
		return 0, false
	}
	root := filepath.Join(filepath.Dir(raw), ".cxp-runtime")
	if c, ok := helperruntime.Current(); ok {
		root = c.Root
	}
	// The native steady path does not open the updater database. This scalar
	// exists only during the bounded migration handoff.
	if _, statErr := os.Lstat(filepath.Join(root, "active")); os.IsNotExist(statErr) {
		if os.Getenv(helperruntime.EnvBootManaged) != "1" {
			return 0, false
		}
		if _, legacyErr := os.Lstat(filepath.Join(root, "boot.json")); os.IsNotExist(legacyErr) {
			return 0, false
		}
	}
	p, err := migrationPending(root)
	if os.IsNotExist(err) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		p, err = adoptLegacyTrial(ctx, root, raw, version)
		cancel()
		if os.IsNotExist(err) {
			// A fixed old dispatcher may outlive its receipt. Native selection
			// still owns the launch; never execute that stale migration build.
			if current, entered := helperruntime.Current(); entered && os.Getenv(helperruntime.EnvBootManaged) != "1" {
				if boot, bootErr := helperruntime.ReadBoot(root); bootErr == nil {
					code, forwardErr := helperruntime.ForwardUpgradeInvocation(root, current.EntryPath, boot.Good, args)
					if forwardErr != nil {
						fmt.Fprintln(stderr, forwardErr)
					}
					return code, true
				}
			}
			if current, entered := helperruntime.Current(); entered && os.Getenv(helperruntime.EnvBootManaged) == "1" && !(len(args) == 2 && args[1] == "--version") {
				if _, bootErr := helperruntime.ReadBoot(root); bootErr == nil {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					cleanupErr := cleanupObsoleteForwarder(ctx, root, current.EntryPath)
					cancel()
					if cleanupErr != nil {
						fmt.Fprintln(stderr, "complete bootstrap cleanup:", cleanupErr)
						return 1, true
					}
				}
			}
			return 0, false
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "migration receipt: %v\n", err)
		return 1, true
	}
	if !samePath(p.RuntimeRoot, root) {
		err = fmt.Errorf("migration receipt belongs to another root")
	} else {
		err = validateMigration(p)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1, true
	}
	// Published native corruption/loss never re-enters legacy admission, even
	// when the requested command is --version or an installer skill refresh.
	if _, bootErr := helperruntime.ReadBoot(root); bootErr != nil {
		for _, name := range []string{helperruntime.BootFile, helperruntime.BootFile + ".published"} {
			if _, nativeErr := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(nativeErr) {
				fmt.Fprintln(stderr, bootErr)
				return 1, true
			}
		}
	}
	// These calls come from the still-running historical installer. Its
	// post-install skill refresh is deferred to the new normal CLI startup.
	if boot, bootErr := helperruntime.ReadBoot(root); bootErr == nil {
		running, _ := helperruntime.NormalizeVersion(version)
		_, enteredRuntime := helperruntime.Current()
		if enteredRuntime && os.Getenv(helperruntime.EnvBootManaged) == "1" && boot.Candidate != nil && boot.Candidate.Version == running && running != boot.Good {
			// The native supervisor owns this trial. Migration must not replay
			// the old Good or issue SETUP in place of its first-frame READY.
			return 0, false
		}
		if running != boot.Good || (enteredRuntime && os.Getenv(helperruntime.EnvBootManaged) != "1") {
			code, err := helperruntime.ForwardUpgradeInvocation(root, p.EntryPath, boot.Good, args)
			if err != nil {
				fmt.Fprintln(stderr, err)
			}
			return code, true
		}
	}
	if len(args) == 2 && args[1] == "--version" {
		fmt.Fprintf(stdout, "cxp version %s\n", strings.TrimPrefix(version, "v"))
		return 0, true
	}
	if _, bootErr := helperruntime.ReadBoot(root); bootErr != nil && len(args) >= 3 && args[1] == "skills" && args[2] == "install-builtin" {
		return 0, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = helperruntime.PrepareBoot(); err == nil {
		// SETUP transfers ownership without confirming a protocol-one trial.
		// The old parent must not replay an old runtime after native writes.
		err = helperruntime.BeforeInteractiveSetup()
	}
	active := p.TargetVersion
	if err == nil {
		boot, bootErr := helperruntime.ReadBoot(root)
		running, _ := helperruntime.NormalizeVersion(version)
		if bootErr == nil && boot.Good != running {
			code, forwardErr := helperruntime.ForwardUpgradeInvocation(root, p.EntryPath, boot.Good, args)
			if forwardErr != nil {
				fmt.Fprintln(stderr, forwardErr)
			}
			return code, true
		}
		if bootErr != nil || boot.Good == p.TargetVersion {
			err = helperruntime.AdmitUpgradeBootstrap(ctx, root, p.TargetActive, p.TargetVersion)
		} else {
			active = boot.Good
		}
	}
	if err == nil {
		err = repairUpgradeEntries(ctx, p)
		if deferUpgradeEntry(err) {
			// A single kernel-owned worker waits for mapped old launchers.
			// Rearming also recovers an interrupted staging caller or TUI.
			if repairErr := scheduleUpgradeRepair(p); repairErr != nil {
				fmt.Fprintln(stderr, "schedule bootstrap repair:", repairErr)
				return 1, true
			}
			return 0, false
		}
	}
	completed := false
	if err == nil {
		err = helperruntime.WithRootLock(ctx, root, func() error {
			current, err := migrationPending(root)
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if current.RequestID != p.RequestID {
				return fmt.Errorf("migration completion owner changed")
			}
			boot, err := helperruntime.ReadBoot(root)
			if err != nil {
				return err
			}
			active = boot.Good
			if err = updateInstallRecord(Context{EntryPath: p.EntryPath, RecordPath: p.RecordPath}, active); err != nil {
				return err
			}
			s, err := updatestate.Open(root)
			if err != nil {
				return err
			}
			defer s.Close()
			if err = s.DeletePending(p.RequestID); err != nil {
				return err
			}
			if err = helperruntime.RemoveUpgradeForwarder(root, p.TargetVersion); err != nil {
				return err
			}
			completed = true
			return nil
		})
	}
	if err != nil {
		fmt.Fprintf(stderr, "complete automatic SQLite upgrade: %v\n", err)
		return 1, true
	}
	// The historical caller's refresh was deferred until its last writes were
	// complete. This is the same best-effort skill install as a normal upgrade,
	// after clearing the receipt so the child cannot recurse into migration.
	if completed && os.Getenv("CODEX_PROXY_SKIP_BUILTIN_SKILLS") != "1" {
		cmd := exec.CommandContext(ctx, helperruntime.VersionPath(root, active, runtime.GOOS), "skills", "install-builtin", "--yes")
		cmd.Env = append(helperruntime.LauncherEnvironment(os.Environ()), helperruntime.EnvDisable+"=1")
		cmd.Stdout = io.Discard
		cmd.Stderr = stderr
		cmd.WaitDelay = 500 * time.Millisecond
		if skillErr := cmd.Run(); skillErr != nil {
			fmt.Fprintln(stderr, "Warning: built-in skill refresh failed; retry `cxp skills install-builtin --yes`:", skillErr)
		}
	}
	return 0, false
}
