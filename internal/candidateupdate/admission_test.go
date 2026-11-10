package candidateupdate

import (
	"errors"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func TestNativeUpdateRejectsHistoricalVersionProbe(t *testing.T) {
	for _, disable := range []string{"", "1"} {
		for _, protocol := range []string{"", "1", "2", "3"} {
			t.Run("disable="+disable+"/protocol="+protocol, func(t *testing.T) {
				t.Setenv(helperruntime.EnvDisable, disable)
				t.Setenv(helperruntime.EnvProbeProtocol, protocol)
				for _, binary := range []string{".codex-proxy_0.1.99_linux_amd64", ".CODEX-PROXY_0.1.99_windows_amd64.exe"} {
					err := RejectLegacyUpdate([]string{binary, "--version"})
					if (protocol == "2" && err != nil) || (protocol != "2" && !errors.Is(err, helperruntime.ErrBootstrapState)) {
						t.Fatal(binary, err)
					}
				}
				if err := RejectLegacyUpdate([]string{"cxp", "--version"}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
