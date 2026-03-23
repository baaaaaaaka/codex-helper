package proc

import "strings"

func LooksLikeProxyDaemon(pid int) (bool, error) {
	cmdline, err := CommandLine(pid)
	if err != nil {
		return false, err
	}
	return looksLikeProxyDaemonCmdline(cmdline), nil
}

func looksLikeProxyDaemonCmdline(cmdline string) bool {
	cmdline = " " + strings.ToLower(strings.TrimSpace(cmdline)) + " "
	if strings.Contains(cmdline, " proxy daemon ") {
		return true
	}
	if strings.Contains(cmdline, " proxy start ") {
		return true
	}
	return false
}
