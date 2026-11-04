// Package faults holds black-box Teams E2E scenarios. Fault scenarios: unknown POST outcome, bad chat isolation, read throttling (R-5, R-11, R-12).
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; with the default subprocess SUT, scenarios inside a
// package also run in parallel (harness.Parallel).
package faults

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
