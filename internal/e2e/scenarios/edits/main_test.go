// Package edits holds black-box Teams E2E scenarios for the helper's chat
// edits besides posts (batch D): the user label on the owner's messages,
// the transcript of a voice message, the freeze notice of an auto-parked
// chat, chat titles and renames, and the unread mark after an answer.
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; scenarios inside a package also run in parallel
// (harness.Parallel), each against its own cxp process.
package edits

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
