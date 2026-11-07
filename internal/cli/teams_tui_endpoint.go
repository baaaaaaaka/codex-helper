package cli

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
)

// Shared homes have one descriptor per helper host, just like PID records.
func teamsTUIEndpointPath(anchor, host string) string {
	sum := sha256.Sum256([]byte(host))
	return filepath.Join(filepath.Dir(anchor), fmt.Sprintf("tui-endpoint-%x.json", sum[:8]))
}
