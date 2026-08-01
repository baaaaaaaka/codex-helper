package appgateway

import (
	"errors"
	"fmt"
	"html"
	"path/filepath"
	"strings"
)

type ServicePlatform string

const (
	ServiceWindows ServicePlatform = "windows"
	ServiceLaunchd ServicePlatform = "launchd"
	ServiceSystemd ServicePlatform = "systemd"
)

type ServiceSpec struct {
	Platform   ServicePlatform
	Name       string
	Executable string
	ConfigPath string
	ProfileID  string
	StateDir   string
}

func (s ServiceSpec) Args() []string {
	return []string{
		"--config", s.ConfigPath,
		"proxy", "app-gateway", "run",
		"--profile-id", s.ProfileID,
		"--state-dir", s.StateDir,
	}
}

func (s ServiceSpec) Validate() error {
	if s.Platform != ServiceWindows && s.Platform != ServiceLaunchd && s.Platform != ServiceSystemd {
		return fmt.Errorf("unsupported app gateway service platform %q", s.Platform)
	}
	if strings.TrimSpace(s.Name) == "" || strings.TrimSpace(s.Executable) == "" || strings.TrimSpace(s.ConfigPath) == "" || strings.TrimSpace(s.ProfileID) == "" || strings.TrimSpace(s.StateDir) == "" {
		return errors.New("app gateway service name, executable, config, profile and state paths are required")
	}
	return nil
}

func (s ServiceSpec) Render() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	switch s.Platform {
	case ServiceSystemd:
		return []byte(renderSystemd(s)), nil
	case ServiceLaunchd:
		return []byte(renderLaunchd(s)), nil
	case ServiceWindows:
		return []byte(renderWindowsTask(s)), nil
	default:
		return nil, fmt.Errorf("unsupported app gateway service platform %q", s.Platform)
	}
}

func (s ServiceSpec) UnitName() string {
	return strings.TrimSpace(s.Name)
}

func renderSystemd(s ServiceSpec) string {
	return "[Unit]\n" +
		"Description=Codex Helper App Gateway " + s.Name + "\n" +
		"After=network-online.target\n\n" +
		"[Service]\n" +
		"Type=simple\n" +
		"ExecStart=" + systemdQuote(s.Executable) + " " + systemdArgs(s.Args()) + "\n" +
		"Restart=on-failure\n" +
		"RestartSec=10\n" +
		"NoNewPrivileges=true\n\n" +
		"[Install]\n" +
		"WantedBy=default.target\n"
}

func systemdQuote(value string) string {
	return systemdEscape(value)
}

func systemdArgs(args []string) string {
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		parts = append(parts, systemdEscape(arg))
	}
	return strings.Join(parts, " ")
}

func systemdEscape(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "%", "%%")
	value = strings.ReplaceAll(value, " ", "\\x20")
	value = strings.ReplaceAll(value, "\t", "\\x09")
	value = strings.ReplaceAll(value, "\n", "\\x0a")
	return value
}

func renderLaunchd(s ServiceSpec) string {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	b.WriteString("<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n")
	b.WriteString("<plist version=\"1.0\"><dict>\n")
	b.WriteString("<key>Label</key><string>" + xmlEscape(s.Name) + "</string>\n")
	b.WriteString("<key>ProgramArguments</key><array>\n")
	for _, arg := range append([]string{s.Executable}, s.Args()...) {
		b.WriteString("<string>" + xmlEscape(arg) + "</string>\n")
	}
	b.WriteString("</array>\n<key>RunAtLoad</key><true/>\n<key>KeepAlive</key><true/>\n")
	b.WriteString("<key>ProcessType</key><string>Interactive</string>\n")
	b.WriteString("</dict></plist>\n")
	return b.String()
}

func renderWindowsTask(s ServiceSpec) string {
	args := windowsCommandLine(s.Args())
	return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
		"<Task version=\"1.4\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">" +
		"<RegistrationInfo><Description>Codex Helper App Gateway " + xmlEscape(s.Name) + "</Description></RegistrationInfo>" +
		"<Triggers><LogonTrigger><Enabled>true</Enabled></LogonTrigger></Triggers>" +
		"<Principals><Principal id=\"Author\"><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>" +
		"<Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><ExecutionTimeLimit>PT0S</ExecutionTimeLimit><RestartOnFailure><Interval>PT10S</Interval><Count>3</Count></RestartOnFailure></Settings>" +
		"<Actions Context=\"Author\"><Exec><Command>" + xmlEscape(s.Executable) + "</Command><Arguments>" + xmlEscape(strings.TrimPrefix(args, s.Executable+" ")) + "</Arguments><WorkingDirectory>" + xmlEscape(filepath.Dir(s.Executable)) + "</WorkingDirectory></Exec></Actions>" +
		"</Task>\r\n"
}

func xmlEscape(value string) string { return html.EscapeString(value) }

// windowsCommandLine is kept local to avoid coupling the package to the CLI's
// Teams command-line parser. It follows the quoting rules needed by Task
// Scheduler's Arguments element, including empty and whitespace arguments.
func windowsCommandLine(args []string) string {
	var b strings.Builder
	for i, arg := range args {
		if i > 0 {
			b.WriteByte(' ')
		}
		if arg != "" && !strings.ContainsAny(arg, " \t\"") {
			b.WriteString(arg)
			continue
		}
		b.WriteByte('"')
		backslashes := 0
		for _, r := range arg {
			switch r {
			case '\\':
				backslashes++
			case '"':
				b.WriteString(strings.Repeat("\\", backslashes*2+1))
				b.WriteByte('"')
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
	}
	return b.String()
}
