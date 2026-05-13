//go:build windows

package update

import (
	"strings"
	"testing"
)

func TestWindowsDeferredMoveScriptWaitsRetriesAndEscapesPaths(t *testing.T) {
	script := windowsDeferredMoveScript(`C:\Users\O'Brien\AppData\Local\Temp\codex.tmp`, `C:\Users\O'Brien\.local\bin\codex-proxy.exe`, 12345)

	for _, want := range []string{
		"Wait-Process -Id $parent -Timeout 60",
		"for ($i = 0; $i -lt 120; $i++)",
		"Move-Item -Force -LiteralPath $src -Destination $dest",
		"source missing",
		"failed to move after retries",
		"$parent=12345",
		`O''Brien`,
		"codex-proxy.exe.update.log",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("deferred move script missing %q:\n%s", want, script)
		}
	}
}
