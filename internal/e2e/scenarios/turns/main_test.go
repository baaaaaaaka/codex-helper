// Package turns holds black-box Teams E2E scenarios. Turn scenarios: answer-once, ordering, cancel (R-1, R-2, R-3, R-6, R-20, R-30, R-33).
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; with the default subprocess SUT, scenarios inside a
// package also run in parallel (harness.Parallel).
package turns

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
