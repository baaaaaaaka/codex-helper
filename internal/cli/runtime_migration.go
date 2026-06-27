package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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

// runtimeMigrationReadyHook commits the runtime transition only after the
// original app-server has answered initialize. Old compatibility artifacts are
// inert before that point and are removed conservatively at commit time.
func runtimeMigrationReadyHook(store *config.Store, paths effectivePaths, codexPath string, log io.Writer) func() {
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
		if !runtimeMigrationRemoteProbe(codexPath) {
			if log != nil {
				_, _ = fmt.Fprintln(log, "runtime migration is waiting for a Codex CLI with remote TUI support (minimum stable version 0.116.0)")
			}
			return
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
			if log != nil {
				_, _ = fmt.Fprintf(log, "runtime migration cleanup warning: %v\n", err)
			}
			return
		}
		if !report.Complete() {
			if log != nil {
				_, _ = fmt.Fprintf(log, "runtime migration is waiting for %d active compatibility artifact(s); the standard runtime remains active\n", len(report.Blockers))
			}
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
		if log != nil && len(report.Removed)+len(report.Restored)+len(report.Preserved) > 0 {
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
