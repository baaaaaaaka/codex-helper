package helperruntime

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// HandleBootstrapInstall is an explicit installer entry, before runtime or CLI
// setup. It can publish only the executing verified binary as the new Good.
func HandleBootstrapInstall(args []string, version string, stderr io.Writer) (int, bool) {
	if len(args) < 2 || args[1] != "--cxp-install-bootstrap" {
		return 0, false
	}
	fs := flag.NewFlagSet("--cxp-install-bootstrap", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var target string
	fs.StringVar(&target, "target", "", "Verified installed executable")
	if err := fs.Parse(args[2:]); err != nil {
		return 1, true
	}
	if fs.NArg() != 0 || target == "" {
		fmt.Fprintln(stderr, "bootstrap install requires --target and no positional arguments")
		return 1, true
	}
	raw, err := executablePath()
	if err == nil {
		var a, b os.FileInfo
		a, err = os.Stat(raw)
		if err == nil {
			b, err = os.Stat(target)
		}
		if err == nil && !os.SameFile(a, b) {
			err = fmt.Errorf("bootstrap target must be the executing verified binary")
		}
	}
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		entry := filepath.Join(filepath.Dir(target), BinaryName(runtime.GOOS))
		err = EnsureStableEntry(target, entry, runtime.GOOS)
		if err == nil {
			err = ProbeProtocol(ctx, entry)
		}
		if err == nil {
			err = installNativeBootstrap(ctx, filepath.Join(filepath.Dir(target), ".cxp-runtime"), target, version)
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "install SQLite bootstrap: %v\n", err)
		return 1, true
	}
	return 0, true
}

func installNativeBootstrap(ctx context.Context, root, source, version string) error {
	return installBootstrap(ctx, root, source, version, true)
}

func installBootstrap(ctx context.Context, root, source, version string, allowNativeChange bool) error {
	version, ok := NormalizeVersion(version)
	if !ok {
		return fmt.Errorf("invalid bootstrap version")
	}
	return WithRootLock(ctx, root, func() error {
		s, err := ReadBoot(root)
		native := err == nil
		if native && s.Good != version && !allowNativeChange {
			return fmt.Errorf("native bootstrap changed while admitting legacy upgrade")
		}
		if !native {
			// ReadBoot deliberately refuses legacy state at runtime. Only absence of
			// native publication admits the migration-only legacy decoder here.
			if _, nativeErr := os.Lstat(filepath.Join(root, BootFile)); nativeErr == nil {
				return err
			} else if !os.IsNotExist(nativeErr) {
				return nativeErr
			}
			if _, sealErr := os.Lstat(filepath.Join(root, BootFile) + ".published"); sealErr == nil {
				return err
			} else if !os.IsNotExist(sealErr) {
				return sealErr
			}
			s, _, err = readLegacyBootForInstall(root)
			if err != nil {
				return err
			}
		}
		if s.Generation == math.MaxUint64 {
			return fmt.Errorf("bootstrap generation exhausted")
		}
		if _, err = InstallVersion(root, source, version, runtime.GOOS, false); err != nil {
			return err
		}
		if native && s.Good == version && (s.Candidate == nil || !allowNativeChange) {
			// A prior attempt can have committed before sealing or archiving.
			db, err := sqlitestate.Open(filepath.Join(root, BootFile), true)
			if err != nil {
				return err
			}
			err = sqlitestate.Seal(db)
			db.Close()
			if err != nil {
				return err
			}
			return archiveLegacyBoot(root)
		}
		previous := ""
		if native && s.Good != version {
			if _, err = checkedRuntime(root, s.Good); err == nil {
				previous = s.Good
			}
		}
		next := BootState{Schema: BootProtocol, Generation: s.Generation + 1, Good: version, Previous: previous}
		if err = writeBoot(root, next); err != nil {
			return err
		}
		return archiveLegacyBoot(root)
	})
}

func archiveLegacyBoot(root string) error {
	path := filepath.Join(root, "boot.json")
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if _, err := os.Lstat(path + ".migrated"); err == nil {
		return fmt.Errorf("bootstrap committed; legacy archive already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(path, path+".migrated")
}
