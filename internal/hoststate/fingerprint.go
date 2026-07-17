package hoststate

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"time"
)

// wallElapsed deliberately strips Go's monotonic component. On Linux and
// some other platforms CLOCK_MONOTONIC pauses while the machine is suspended,
// so comparing the monotonic components would hide the very sleep gap that
// the fallback observer is meant to detect. A wall-clock jump can cause an
// extra harmless wake hint; it must never suppress a real wake hint.
func wallElapsed(now, previous time.Time) time.Duration {
	if now.IsZero() || previous.IsZero() {
		return 0
	}
	return now.Round(0).Sub(previous.Round(0))
}

func interfaceFingerprint() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "error:" + err.Error()
	}
	parts := make([]string, 0, len(interfaces))
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, _ := iface.Addrs()
		addressParts := make([]string, 0, len(addresses))
		for _, address := range addresses {
			addressParts = append(addressParts, address.String())
		}
		sort.Strings(addressParts)
		parts = append(parts, fmt.Sprintf("%d:%s:%s:%s", iface.Index, iface.Name, iface.HardwareAddr.String(), strings.Join(addressParts, ",")))
	}
	sort.Strings(parts)
	return strings.Join(parts, "|")
}
