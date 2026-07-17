package hoststate

import (
	"fmt"
	"net"
	"sort"
	"strings"
)

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
