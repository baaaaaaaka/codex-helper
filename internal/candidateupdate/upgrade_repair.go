package candidateupdate

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

const upgradeRepairCommand = "__internal-sqlite-upgrade-repair"

// HandleUpgradeRepair is a migration-only worker, never a business CLI entry.
func HandleUpgradeRepair(args []string, stdout, stderr io.Writer) (int, bool) {
	if len(args) < 2 || args[1] != upgradeRepairCommand {
		return 0, false
	}
	if len(args) != 5 || !strings.HasPrefix(args[2], "--root=") || !strings.HasPrefix(args[3], "--request-id=") || !strings.HasPrefix(args[4], "--parent-pid=") {
		fmt.Fprintln(stderr, "invalid bootstrap repair arguments")
		return 2, true
	}
	pid, err := strconv.Atoi(strings.TrimPrefix(args[4], "--parent-pid="))
	if err != nil || pid <= 0 {
		return 2, true
	}
	err = runUpgradeRepair(strings.TrimPrefix(args[2], "--root="), strings.TrimPrefix(args[3], "--request-id="), pid, stdout)
	if err != nil {
		fmt.Fprintln(stderr, "bootstrap migration repair:", err)
		return 1, true
	}
	return 0, true
}
