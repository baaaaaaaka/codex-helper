package proc

import "testing"

func TestLooksLikeProxyDaemonCmdline(t *testing.T) {
	tests := []struct {
		name    string
		cmdline string
		want    bool
	}{
		{name: "daemon subcommand", cmdline: "/home/baka/go/bin/codex-proxy proxy daemon --instance-id abc", want: true},
		{name: "foreground start", cmdline: "codex-proxy proxy start --foreground", want: true},
		{name: "run session", cmdline: "codex-proxy run p1 -- codex", want: false},
		{name: "regular codex", cmdline: "/home/baka/.config/codex-proxy/codex-patched-123 resume abc", want: false},
		{name: "mixed case", cmdline: "Codex-Proxy Proxy Daemon --instance-id abc", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := looksLikeProxyDaemonCmdline(tc.cmdline); got != tc.want {
				t.Fatalf("looksLikeProxyDaemonCmdline(%q) = %v, want %v", tc.cmdline, got, tc.want)
			}
		})
	}
}
