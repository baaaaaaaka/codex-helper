//go:build windows

package cli

import (
	"strconv"

	"golang.org/x/sys/windows/registry"
)

// teamsOSBootID is Windows' boot counter: the kernel increments
// PrefetchParameters\BootId at every boot, and any user may read it.
func teamsOSBootID() string {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE,
		`SYSTEM\CurrentControlSet\Control\Session Manager\Memory Management\PrefetchParameters`, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer k.Close()
	n, _, err := k.GetIntegerValue("BootId")
	if err != nil {
		return ""
	}
	return "win-boot-" + strconv.FormatUint(n, 10)
}
