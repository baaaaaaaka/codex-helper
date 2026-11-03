// Package turns holds black-box Teams E2E scenarios. Turn scenarios: answer-once, ordering, cancel (R-1, R-2, R-3, R-6, R-20, R-30, R-33).
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; scenarios inside one package run sequentially because
// the old core reads its configuration from process environment variables.
package turns

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	sut.MustSelect()
	os.Exit(m.Run())
}
