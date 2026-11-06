// Package sharedhome holds black-box Teams E2E scenarios about several
// helpers on one state (a shared NFS home): the kill-switch file, standby
// and takeover with generation fencing.
package sharedhome

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
