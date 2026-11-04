// Package cost holds black-box Teams E2E scenarios that measure what an idle
// helper costs: Graph requests, writes and CPU, with production timing.
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; with the default subprocess SUT, scenarios inside a
// package also run in parallel (harness.Parallel).
package cost

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
