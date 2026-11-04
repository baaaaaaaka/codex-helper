package harness

import (
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sandbox"
)

// Transport is the per-instance network sandbox (see package sandbox).
type Transport = sandbox.Transport

// Sandbox errors, re-exported for scenarios.
var (
	ErrForeignHost    = sandbox.ErrForeignHost
	ErrInstanceKilled = sandbox.ErrInstanceKilled
)

// NewTransport builds a sandbox transport for one in-process SUT instance.
func NewTransport(g *fakegraph.Server, instance string) *Transport {
	tr, err := sandbox.New(g.URL(), instance, g.RecordBlocked)
	if err != nil {
		panic(err)
	}
	return tr
}
