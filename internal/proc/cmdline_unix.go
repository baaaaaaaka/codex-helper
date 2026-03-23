//go:build !windows

package proc

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func CommandLine(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid %d", pid)
	}

	procPath := fmt.Sprintf("/proc/%d/cmdline", pid)
	if b, err := os.ReadFile(procPath); err == nil {
		b = bytes.ReplaceAll(b, []byte{0}, []byte{' '})
		return strings.TrimSpace(string(b)), nil
	}

	out, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return "", err
	}
	cmdline := strings.TrimSpace(string(out))
	if cmdline == "" {
		return "", fmt.Errorf("empty command line for pid %d", pid)
	}
	return cmdline, nil
}
