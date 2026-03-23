//go:build windows

package proc

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func CommandLine(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid %d", pid)
	}

	script := fmt.Sprintf(`$p = Get-CimInstance Win32_Process -Filter "ProcessId=%d"; if ($null -eq $p) { exit 1 }; $p.CommandLine`, pid)
	out, err := exec.Command("powershell", "-NoProfile", "-Command", script).Output()
	if err != nil {
		return "", err
	}
	cmdline := strings.TrimSpace(string(out))
	if cmdline == "" {
		return "", fmt.Errorf("empty command line for pid %s", strconv.Itoa(pid))
	}
	return cmdline, nil
}
