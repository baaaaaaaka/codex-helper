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

// prepareRuntimeMigration verifies that no legacy session is still active and
// restores authentication before Codex starts. Destructive cleanup of the
// remaining proven assets is deferred until the initialize handshake succeeds.
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
	options := runtimeMigrationCleanupOptions(store, paths, codexPath)
	report, err := migration.InspectLegacyRuntimeAssets(options)
	if err != nil {
		return fmt.Errorf("inspect standard runtime migration: %w", err)
	}
	if !report.Complete() {
		return fmt.Errorf("standard runtime migration is waiting for %d active legacy session artifact(s); finish those sessions and retry", len(report.Blockers))
	}
	authReport, err := migration.PrepareLegacyAuthentication(options)
	if err != nil {
		return fmt.Errorf("prepare standard runtime authentication: %w", err)
	}
	if !authReport.Complete() {
		return fmt.Errorf("standard runtime migration could not safely restore authentication")
	}
	if log != nil && len(authReport.Removed)+len(authReport.Restored)+len(report.Preserved) > 0 {
		_, _ = fmt.Fprintf(log, "runtime migration prepared (%d authentication artifact(s) removed, %d restored, %d ambiguous file(s) preserved until activation)\n", len(authReport.Removed), len(authReport.Restored), len(report.Preserved))
	}
	return nil
}

func runtimeMigrationCleanupOptions(store *config.Store, paths effectivePaths, codexPath string) migration.CleanupOptions {
	binaryPath := strings.TrimSpace(codexPath)
	if native, _, nativeErr := codexbinary.FindNativeBinary(binaryPath); nativeErr == nil {
		binaryPath = native
	}
	return migration.CleanupOptions{
		ConfigDir:    filepath.Dir(store.Path()),
		CodexHome:    paths.CodexDir,
		TempDir:      runtimeMigrationTempDir(),
		BinaryPath:   binaryPath,
		ProcessAlive: proc.IsAlive,
	}
}

// runtimeMigrationReadyHook commits activation after the original app-server
// has initialized successfully, then removes proven legacy assets. Cleanup is
// explicitly retryable, while an activated runtime never falls back.
func runtimeMigrationReadyHook(store *config.Store, paths effectivePaths, codexPath string, log io.Writer) func() {
	return func() {
		runtimeMigrationMu.Lock()
		defer runtimeMigrationMu.Unlock()
		if store == nil {
			return
		}
		cfg, err := store.Load()
		if err != nil {
			return
		}
		if cfg.RuntimeGeneration >= currentRuntimeGeneration && !cfg.RuntimeCleanupPending {
			return
		}
		if cfg.RuntimeGeneration < currentRuntimeGeneration {
			transactionID, transactionErr := ids.New()
			if transactionErr != nil {
				if log != nil {
					_, _ = fmt.Fprintf(log, "runtime migration transaction warning: %v\n", transactionErr)
				}
				return
			}
			if commitErr := store.Update(func(updated *config.Config) error {
				updated.RuntimeGeneration = currentRuntimeGeneration
				updated.RuntimeMigrationID = transactionID
				updated.RuntimeMigratedAt = time.Now().UTC()
				updated.RuntimeCleanupPending = true
				return nil
			}); commitErr != nil {
				if log != nil {
					_, _ = fmt.Fprintf(log, "runtime migration commit warning: %v\n", commitErr)
				}
				return
			}
		}

		report, cleanupErr := migration.CleanupLegacyRuntimeAssets(runtimeMigrationCleanupOptions(store, paths, codexPath))
		if cleanupErr != nil || !report.Complete() {
			if log != nil {
				_, _ = fmt.Fprintf(log, "runtime migration cleanup pending: removed=%d restored=%d preserved=%d blockers=%d err=%v\n", len(report.Removed), len(report.Restored), len(report.Preserved), len(report.Blockers), cleanupErr)
			}
			return
		}
		if cleanupCommitErr := store.Update(func(updated *config.Config) error {
			if updated.RuntimeGeneration >= currentRuntimeGeneration {
				updated.RuntimeCleanupPending = false
			}
			return nil
		}); cleanupCommitErr != nil {
			if log != nil {
				_, _ = fmt.Fprintf(log, "runtime migration cleanup commit warning: %v\n", cleanupCommitErr)
			}
			return
		}
		if log != nil {
			_, _ = fmt.Fprintf(log, "runtime migration completed (%d compatibility artifact(s) removed, %d restored, %d ambiguous file(s) preserved)\n", len(report.Removed), len(report.Restored), len(report.Preserved))
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
