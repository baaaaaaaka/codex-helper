// Package proxysupervisor contains the platform-neutral contract used by the
// proxy's local and native supervisors. Rendering is kept pure so hosted CI
// can validate all three service definitions without starting OS services.
package proxysupervisor

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Platform string

const (
	PlatformLinux   Platform = "linux"
	PlatformDarwin  Platform = "darwin"
	PlatformWindows Platform = "windows"
)

type Spec struct {
	Platform      Platform
	Executable    string
	ConfigPath    string
	InstanceID    string
	OwnerToken    string
	WorkingDir    string
	RestartDelay  time.Duration
	RestartWindow time.Duration
	RestartBurst  int
}

func (s Spec) Validate() error {
	if s.Platform != PlatformLinux && s.Platform != PlatformDarwin && s.Platform != PlatformWindows {
		return fmt.Errorf("unsupported proxy supervisor platform %q", s.Platform)
	}
	if strings.TrimSpace(s.Executable) == "" {
		return errors.New("proxy supervisor executable is required")
	}
	if strings.TrimSpace(s.ConfigPath) == "" {
		return errors.New("proxy supervisor config path is required")
	}
	if strings.TrimSpace(s.InstanceID) == "" {
		return errors.New("proxy supervisor instance id is required")
	}
	if strings.TrimSpace(s.OwnerToken) == "" {
		return errors.New("proxy supervisor owner token is required")
	}
	return nil
}

func (s Spec) normalized() Spec {
	if s.RestartDelay <= 0 {
		s.RestartDelay = 5 * time.Second
	}
	if s.RestartWindow <= 0 {
		s.RestartWindow = time.Minute
	}
	if s.RestartBurst <= 0 {
		s.RestartBurst = 3
	}
	return s
}

func (s Spec) ChildArgs() []string {
	return []string{
		"--config", s.ConfigPath,
		"proxy", "daemon",
		"--instance-id", s.InstanceID,
		"--owner-token", s.OwnerToken,
		"--managed",
	}
}

func (s Spec) SupervisorArgs() []string {
	return []string{
		"--config", s.ConfigPath,
		"proxy", "supervisor", "run",
		"--instance-id", s.InstanceID,
		"--owner-token", s.OwnerToken,
	}
}

func Render(s Spec) (string, []byte, error) {
	if err := s.Validate(); err != nil {
		return "", nil, err
	}
	s = s.normalized()
	switch s.Platform {
	case PlatformLinux:
		return safeName(s.InstanceID) + ".service", []byte(renderSystemd(s)), nil
	case PlatformDarwin:
		return "com.openai.codex-proxy." + safeName(s.InstanceID) + ".plist", []byte(renderLaunchAgent(s)), nil
	case PlatformWindows:
		return safeName(s.InstanceID) + ".xml", []byte(renderTaskXML(s)), nil
	default:
		return "", nil, fmt.Errorf("unsupported proxy supervisor platform %q", s.Platform)
	}
}

func safeName(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "instance"
	}
	return b.String()
}

func renderSystemd(s Spec) string {
	var b strings.Builder
	b.WriteString("[Unit]\nDescription=Codex proxy broker ")
	b.WriteString(systemdQuote(s.InstanceID))
	b.WriteString("\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nExecStart=")
	b.WriteString(systemdQuote(s.Executable))
	for _, arg := range s.SupervisorArgs() {
		b.WriteByte(' ')
		b.WriteString(systemdQuote(arg))
	}
	b.WriteString("\nRestart=on-failure\nRestartSec=")
	b.WriteString(formatSeconds(s.RestartDelay))
	b.WriteString("\nStartLimitIntervalSec=")
	b.WriteString(strconv.Itoa(maxInt(1, int(s.RestartWindow/time.Second))))
	b.WriteString("\nStartLimitBurst=")
	b.WriteString(strconv.Itoa(s.RestartBurst))
	b.WriteString("\nNoNewPrivileges=true\n\n[Install]\nWantedBy=default.target\n")
	return b.String()
}

func systemdQuote(value string) string { return strconv.Quote(value) }

func formatSeconds(value time.Duration) string {
	if value < time.Second {
		value = time.Second
	}
	if value%time.Second == 0 {
		return strconv.Itoa(int(value/time.Second)) + "s"
	}
	return strconv.FormatFloat(value.Seconds(), 'f', 3, 64) + "s"
}

func plistString(b *strings.Builder, value string) {
	b.WriteString("<string>")
	var escaped bytes.Buffer
	_ = xml.EscapeText(&escaped, []byte(value))
	b.Write(escaped.Bytes())
	b.WriteString("</string>\n")
}

func renderLaunchAgent(s Spec) string {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key>")
	plistString(&b, "com.openai.codex-proxy."+safeName(s.InstanceID))
	b.WriteString("<key>ProgramArguments</key><array>\n")
	plistString(&b, s.Executable)
	for _, arg := range s.SupervisorArgs() {
		plistString(&b, arg)
	}
	b.WriteString("</array>\n<key>RunAtLoad</key><true/>\n<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>\n<key>ThrottleInterval</key><integer>")
	b.WriteString(strconv.Itoa(maxInt(1, int(s.RestartDelay/time.Second))))
	b.WriteString("</integer>\n")
	if s.WorkingDir != "" {
		b.WriteString("<key>WorkingDirectory</key>")
		plistString(&b, s.WorkingDir)
	}
	b.WriteString("</dict></plist>\n")
	return b.String()
}

func renderTaskXML(s Spec) string {
	command, args := s.Executable, joinWindowsArgs(s.SupervisorArgs())
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Task version=\"1.4\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\"><RegistrationInfo><Description>Codex proxy broker ")
	xmlText(&b, s.InstanceID)
	b.WriteString("</Description></RegistrationInfo><Triggers><LogonTrigger><Enabled>true</Enabled></LogonTrigger></Triggers><Principals><Principal id=\"Author\"><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals><Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><StartWhenAvailable>true</StartWhenAvailable><ExecutionTimeLimit>PT0S</ExecutionTimeLimit><RestartOnFailure><Interval>PT1M</Interval><Count>")
	b.WriteString(strconv.Itoa(s.RestartBurst))
	b.WriteString("</Count></RestartOnFailure></Settings><Actions Context=\"Author\"><Exec><Command>")
	xmlText(&b, command)
	b.WriteString("</Command><Arguments>")
	xmlText(&b, args)
	b.WriteString("</Arguments>")
	if s.WorkingDir != "" {
		b.WriteString("<WorkingDirectory>")
		xmlText(&b, s.WorkingDir)
		b.WriteString("</WorkingDirectory>")
	}
	b.WriteString("</Exec></Actions></Task>\n")
	return b.String()
}

// joinWindowsArgs follows the CommandLineToArgvW quoting rules used by the
// Windows process boundary. Task Scheduler stores the executable separately,
// so only the argument vector needs quoting here.
func joinWindowsArgs(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = quoteWindowsArg(arg)
	}
	return strings.Join(quoted, " ")
}

func quoteWindowsArg(value string) string {
	if value != "" && !strings.ContainsAny(value, " \t\"") {
		return value
	}
	var b strings.Builder
	b.WriteByte('"')
	backslashes := 0
	for _, r := range value {
		switch r {
		case '\\':
			backslashes++
		case '"':
			b.WriteString(strings.Repeat("\\", backslashes*2+1))
			b.WriteRune(r)
			backslashes = 0
		default:
			if backslashes > 0 {
				b.WriteString(strings.Repeat("\\", backslashes))
				backslashes = 0
			}
			b.WriteRune(r)
		}
	}
	if backslashes > 0 {
		b.WriteString(strings.Repeat("\\", backslashes*2))
	}
	b.WriteByte('"')
	return b.String()
}

func xmlText(b *strings.Builder, value string) {
	var escaped bytes.Buffer
	_ = xml.EscapeText(&escaped, []byte(value))
	b.Write(escaped.Bytes())
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// RenderPath returns a conventional per-user output path without creating or
// writing anything.
func RenderPath(baseDir string, s Spec) (string, error) {
	name, _, err := Render(s)
	if err != nil {
		return "", err
	}
	return filepath.Join(baseDir, name), nil
}
