// Package entry holds black-box Teams E2E scenarios for the production entry
// point of the new core, `cxp teams run` (E2E_SUT=cxp): where it
// keeps its store, and that an old store is never converted implicitly.
// Under other SUTs the scenarios are skipped.
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel.
package entry

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
