package candidateupdate

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

// RejectLegacyUpdate refuses historical downloaded-candidate version probes.
// Runtime-disable was already used by old updaters, so only a protocol-two
// probe from the new caller opts in. It is a compatibility signal, not a secret.
func RejectLegacyUpdate(args []string) error {
	if len(args) == 2 && args[1] == "--version" && strings.HasPrefix(strings.ToLower(filepath.Base(args[0])), ".codex-proxy_") && os.Getenv(helperruntime.EnvProbeProtocol) != strconv.Itoa(helperruntime.BootProtocol) {
		return helperruntime.ErrBootstrapState
	}
	return nil
}
