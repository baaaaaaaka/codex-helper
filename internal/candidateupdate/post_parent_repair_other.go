//go:build !windows

package candidateupdate

import (
	"fmt"
	"io"
)

func ScheduleLegacyDirectSelfUpdateRepair(_ []string, _ string) error {
	return nil
}

func scheduleDeferredStableEntryRepair(_ Result) error {
	return nil
}

func shouldDeferStableEntryRepair(_ error) bool {
	return false
}

func repairPostParentEntry(_ string, _ postParentRepairRequest, _ io.Writer) error {
	return fmt.Errorf("post-parent entry repair is supported only on Windows")
}
