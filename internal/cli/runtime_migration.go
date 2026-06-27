package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexbinary"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

const currentRuntimeGeneration = 1

var runtimeMigrationMu sync.Mutex
var runtimeMigrationTempDir = os.TempDir
var runtimeMigrationRemoteProbe = codexRemoteTUICapable

// prepareRuntimeMigration restores authentication and removes proven legacy
// artifacts before Codex starts. This ordering matters because Codex caches its
// initial auth state for the lifetime of the app-server process.
func prepareRuntimeMigration(store *config.Store, paths effectivePaths, codexPath string, log io.Writer) error {
	runtimeMigrationMu.Lock()
	defer runtimeMigrationMu.Unlock()
	if store == nil {
		return nil
	}
	if _, statErr := os.Stat(store.Path()); os.IsNotExist(statErr) {
		// A fresh installation has no helper-owned legacy runtime to migrate.
		return nil
	}
	cfg, err := store.Load()
	if err != nil || cfg.RuntimeGeneration >= currentRuntimeGeneration {
		return err
	}
	if !runtimeMigrationRemoteProbe(codexPath) {
		return fmt.Errorf("Codex CLI does not support the standard remote runtime (minimum stable version 0.131.0)")
	}
	binaryPath := strings.TrimSpace(codexPath)
	if native, _, nativeErr := codexbinary.FindNativeBinary(binaryPath); nativeErr == nil {
		binaryPath = native
	}
	report, err := migration.CleanupLegacyRuntimeAssets(migration.CleanupOptions{
		ConfigDir:    filepath.Dir(store.Path()),
		CodexHome:    paths.CodexDir,
		TempDir:      runtimeMigrationTempDir(),
		BinaryPath:   binaryPath,
		ProcessAlive: proc.IsAlive,
	})
	if err != nil {
		return fmt.Errorf("prepare standard runtime: %w", err)
	}
	if !report.Complete() {
		return fmt.Errorf("standard runtime migration is waiting for %d active legacy session artifact(s); finish those sessions and retry", len(report.Blockers))
	}
	if log != nil && len(report.Removed)+len(report.Restored)+len(report.Preserved) > 0 {
		_, _ = fmt.Fprintf(log, "runtime migration prepared (%d compatibility artifact(s) removed, %d restored, %d ambiguous file(s) preserved)\n", len(report.Removed), len(report.Restored), len(report.Preserved))
	}
	return nil
}

// runtimeMigrationReadyHook records activation only after the original
// app-server has successfully initialized. Destructive cleanup and auth
// restoration already completed before process launch.
func runtimeMigrationReadyHook(store *config.Store, log io.Writer) func() {
	return func() {
		runtimeMigrationMu.Lock()
		defer runtimeMigrationMu.Unlock()
		if store == nil {
			return
		}
		cfg, err := store.Load()
		if err != nil || cfg.RuntimeGeneration >= currentRuntimeGeneration {
			return
		}
		transactionID, err := ids.New()
		if err != nil {
			if log != nil {
				_, _ = fmt.Fprintf(log, "runtime migration transaction warning: %v\n", err)
			}
			return
		}
		if err := store.Update(func(updated *config.Config) error {
			updated.RuntimeGeneration = currentRuntimeGeneration
			updated.RuntimeMigrationID = transactionID
			updated.RuntimeMigratedAt = time.Now().UTC()
			return nil
		}); err != nil {
			if log != nil {
				_, _ = fmt.Fprintf(log, "runtime migration commit warning: %v\n", err)
			}
			return
		}
		if log != nil {
			_, _ = fmt.Fprintln(log, "runtime migration completed")
		}
	}
}

func codexRemoteTUICapable(codexPath string) bool {
	codexPath = strings.TrimSpace(codexPath)
	if codexPath == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, codexPath, "--help").CombinedOutput()
	return err == nil && strings.Contains(string(output), "--remote")
}

func codexBrokerRuntimeCapable(codexPath string) bool {
	if !codexRemoteTUICapable(codexPath) {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, codexPath, "--version").CombinedOutput()
	if err != nil {
		return false
	}
	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return false
	}
	version := strings.TrimPrefix(fields[len(fields)-1], "v")
	baseVersion, prerelease, _ := strings.Cut(version, "-")
	version = baseVersion
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return false
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil {
		return false
	}
	if major > 0 || minor > 131 {
		return true
	}
	return minor == 131 && prerelease == ""
}
