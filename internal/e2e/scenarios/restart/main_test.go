// Package restart holds black-box Teams E2E scenarios. Restart scenarios: down-time and queued work, TUI mirroring catch-up (R-4, R-29, R-5b, R-9, R-10).
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; scenarios inside one package run sequentially because
// the old core reads its configuration from process environment variables.
package restart

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	sut.MustSelect()
	os.Exit(m.Run())
}
