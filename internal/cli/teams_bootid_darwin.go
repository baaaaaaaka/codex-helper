//go:build darwin

package cli

import (
	"strings"

	"golang.org/x/sys/unix"
)

// teamsOSBootID is macOS's boot session UUID, new at every boot.
func teamsOSBootID() string {
	id, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(id)
}
