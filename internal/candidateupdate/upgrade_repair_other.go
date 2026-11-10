//go:build !windows

package candidateupdate

import (
	"fmt"
	"io"

	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
)

func deferUpgradeEntry(error) bool       { return false }
func upgradeEntryAvailable(string) error { return nil }
func scheduleUpgradeRepair(updatestate.Pending) error {
	return fmt.Errorf("deferred entry repair is Windows-only")
}
func runUpgradeRepair(string, string, int, io.Writer) error {
	return fmt.Errorf("deferred entry repair is Windows-only")
}
