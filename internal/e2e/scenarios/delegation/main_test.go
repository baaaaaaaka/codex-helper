// Package delegation holds black-box Teams E2E scenarios for cross-machine
// delegation, worker side (F-XM-2, F-XM-3): a request posted to this
// machine's inbox runs once as a Codex turn and gets one result record.
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; with the default subprocess SUT, scenarios inside a
// package also run in parallel (harness.Parallel).
package delegation

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
