//go:build windows

package update

import (
	"strings"
	"testing"
)

func TestWindowsDeferredMoveScriptWaitsRetriesAndEscapesPaths(t *testing.T) {
	script := windowsDeferredMoveScript(`C:\Users\O'Brien\AppData\Local\Temp\codex.tmp`, `C:\Users\O'Brien\.local\bin\codex-proxy.exe`, 12345)

	for _, want := range []string{
		"Wait-Process -Id $parent -Timeout 120",
		"Get-CimInstance Win32_Process",
		"for ($j = 0; $j -lt 240; $j++)",
		"for ($i = 0; $i -lt 240; $i++)",
		"Move-Item -Force -LiteralPath $src -Destination $dest",
		"source missing",
		"failed to move after retries",
		"$parent=12345",
		`O''Brien`,
		"codex-helper\\updates\\codex-proxy-update.log",
		"codex-proxy.exe.update.log",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("deferred move script missing %q:\n%s", want, script)
		}
	}
}
