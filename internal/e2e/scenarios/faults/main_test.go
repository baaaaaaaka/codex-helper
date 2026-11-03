// Package faults holds black-box Teams E2E scenarios. Fault scenarios: unknown POST outcome, bad chat isolation, read throttling (R-5, R-11, R-12).
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; scenarios inside one package run sequentially because
// the old core reads its configuration from process environment variables.
package faults

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	sut.MustSelect()
	os.Exit(m.Run())
}
