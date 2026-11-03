// Package sut selects the system under test for the scenario packages.
package sut

import (
	"fmt"
	"os"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut/oldcore"
)

// EnvSUT names the adapter to use. Default: oldcore.
const EnvSUT = "E2E_SUT"

// MustSelect installs harness.DefaultFactory from E2E_SUT or exits.
func MustSelect() {
	switch name := os.Getenv(EnvSUT); name {
	case "", "oldcore":
		harness.DefaultFactory = oldcore.Factory
	default:
		fmt.Fprintf(os.Stderr, "unknown %s %q (known: oldcore)\n", EnvSUT, name)
		os.Exit(2)
	}
}
