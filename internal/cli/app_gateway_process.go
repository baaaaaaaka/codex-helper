package cli

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type desktopProcessRow struct {
	Name        string `json:"Name"`
	CommandLine string `json:"CommandLine"`
}

func parseProxyPorts(commandLine string) []int {
	var ports []int
	for _, raw := range strings.FieldsFunc(commandLine, func(r rune) bool { return r == '\x00' || r == ' ' || r == '\t' || r == '\n' }) {
		arg := strings.Trim(strings.TrimSpace(raw), "\"'")
		if !strings.HasPrefix(strings.ToLower(arg), "--proxy-server=") {
			continue
		}
		rawURL := strings.TrimSpace(arg[len("--proxy-server="):])
		u, err := url.Parse(rawURL)
		if err != nil {
			continue
		}
		portText := u.Port()
		if portText == "" {
			continue
		}
		port, err := strconv.Atoi(portText)
		if err == nil && port > 0 && port <= 65535 {
			ports = append(ports, port)
		}
	}
	return ports
}

func appendUniqueProxyPorts(dst []int, src []int) []int {
	for _, port := range src {
		seen := false
		for _, existing := range dst {
			if existing == port {
				seen = true
				break
			}
		}
		if !seen {
			dst = append(dst, port)
		}
	}
	return dst
}

func desktopProxyPortsFromRows(rows []desktopProcessRow) []int {
	var ports []int
	for _, row := range rows {
		name := desktopProcessName(row.Name)
		if name != "chatgpt" && name != "codex" && name != "chatgpt.exe" && name != "codex.exe" {
			continue
		}
		ports = appendUniqueProxyPorts(ports, parseProxyPorts(row.CommandLine))
	}
	return ports
}

func desktopProcessName(value string) string {
	value = strings.TrimSpace(value)
	if separator := strings.LastIndexAny(value, `/\`); separator >= 0 {
		value = value[separator+1:]
	}
	return strings.ToLower(value)
}

func parseWindowsDesktopProcessRows(output []byte) ([]desktopProcessRow, error) {
	var rows []desktopProcessRow
	if err := json.Unmarshal(output, &rows); err == nil {
		return rows, nil
	}
	var row desktopProcessRow
	if err := json.Unmarshal(output, &row); err != nil {
		return nil, err
	}
	return []desktopProcessRow{row}, nil
}

func parseDarwinDesktopProcessRows(output string) []desktopProcessRow {
	var rows []desktopProcessRow
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		rows = append(rows, desktopProcessRow{
			Name:        fields[1],
			CommandLine: strings.Join(fields[2:], " "),
		})
	}
	return rows
}

func discoverDesktopProxyPortsFromProcRoot(ctx context.Context, root string) ([]int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var rows []desktopProcessRow
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return desktopProxyPortsFromRows(rows), err
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || !entry.IsDir() {
			continue
		}
		executable, err := os.Readlink(filepath.Join(root, entry.Name(), "exe"))
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(root, entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		rows = append(rows, desktopProcessRow{Name: executable, CommandLine: string(cmdline)})
	}
	return desktopProxyPortsFromRows(rows), nil
}
