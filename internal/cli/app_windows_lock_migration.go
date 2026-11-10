package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Do not convert another process's live lock. This read-only migration
// inspection lets the existing policy reclaim a stopped legacy owner only.
func readLegacyWindowsManagedLockOwner(dir string) (o codexWindowsManagedLockMetadata, err error) {
	b, err := os.ReadFile(filepath.Join(dir, "owner.json"))
	if err != nil {
		return o, err
	}
	err = json.Unmarshal(b, &o)
	return o, err
}
