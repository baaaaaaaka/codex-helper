// Package chats holds black-box Teams E2E scenarios about what one chat sees: long answers, group chats, attachments, duplicate prompts (R-36, R-43, R-37, R-3).
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; scenarios inside a package also run in parallel
// (harness.Parallel), each against its own cxp process.
package chats

import (
	"os"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}
