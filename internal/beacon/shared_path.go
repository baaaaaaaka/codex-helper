package beacon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const sharedStoreRelativePath = ".codex-helper/beacon/state.json"

func NormalizeSharedPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return filepath.Clean(path)
}

func SharedStorePath(sharedPath string) string {
	sharedPath = NormalizeSharedPath(sharedPath)
	if sharedPath == "" {
		return ""
	}
	return filepath.Join(sharedPath, sharedStoreRelativePath)
}

func SharedPathIsAbsolute(sharedPath string) bool {
	sharedPath = NormalizeSharedPath(sharedPath)
	return sharedPath != "" && filepath.IsAbs(sharedPath)
}

func ManagedProviderRequiresSharedPath(provider Provider) bool {
	switch provider {
	case ProviderSlurm, ProviderLSF:
		return true
	default:
		return false
	}
}

func ProbeSharedPath(sharedPath string) error {
	sharedPath = NormalizeSharedPath(sharedPath)
	if sharedPath == "" {
		return fmt.Errorf("shared_path is required")
	}
	if !filepath.IsAbs(sharedPath) {
		return fmt.Errorf("shared_path must be absolute")
	}
	if err := os.MkdirAll(filepath.Dir(SharedStorePath(sharedPath)), 0o700); err != nil {
		return fmt.Errorf("create shared_path beacon directory: %w", err)
	}
	probeDir := filepath.Dir(SharedStorePath(sharedPath))
	tmp, err := os.CreateTemp(probeDir, ".cxp-beacon-shared-probe-*")
	if err != nil {
		return fmt.Errorf("create shared_path probe: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write([]byte("ok")); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write shared_path probe: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close shared_path probe: %w", err)
	}
	renamed := tmpName + ".renamed"
	if err := os.Rename(tmpName, renamed); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename shared_path probe: %w", err)
	}
	if err := os.Remove(renamed); err != nil {
		return fmt.Errorf("remove shared_path probe: %w", err)
	}
	return nil
}
