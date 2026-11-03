// Package inbound holds black-box Teams E2E scenarios. Inbound classification: self-echo, cross-instance loops, paged backlogs (R-7, R-4).
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; scenarios inside one package run sequentially because
// the old core reads its configuration from process environment variables.
package inbound

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	sut.MustSelect()
	os.Exit(m.Run())
}
