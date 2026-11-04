// Package notify holds black-box Teams E2E scenarios about how the owner is notified of finals: Workflow cards and @mentions (R-49, F-RND-6, owner decision "notify on every final").
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; with the default subprocess SUT, scenarios inside a
// package also run in parallel (harness.Parallel).
package notify

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
