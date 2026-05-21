//go:build windows

package update

import (
	"fmt"
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
		"Move-Item -Force -LiteralPath $src -Destination $dest -ErrorAction Stop",
		"pending helper still exists after Move-Item",
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

func TestWindowsDeferredMoveScriptSafetyInvariantsStress(t *testing.T) {
	for i := 0; i < 128; i++ {
		script := windowsDeferredMoveScript(
			fmt.Sprintf(`C:\Users\Alice\AppData\Local\Temp\codex helper stress %03d O'Brien.tmp`, i),
			fmt.Sprintf(`C:\Users\Alice\.local\bin stress %03d\codex-proxy.exe`, i),
			10000+i,
		)
		commands := windowsDeferredMoveItemCommands(script)
		if len(commands) != 1 {
			t.Fatalf("Move-Item commands = %#v, want one binary move", commands)
		}
		if !strings.Contains(commands[0], "-ErrorAction Stop") {
			t.Fatalf("deferred move command is not terminating:\n%s\nscript:\n%s", commands[0], script)
		}
		for _, want := range []string{
			"pending helper still exists after Move-Item",
			"move attempt ' + ($i + 1) + ' failed",
			"failed to move after retries",
		} {
			if !strings.Contains(script, want) {
				t.Fatalf("deferred move script missing safety invariant %q:\n%s", want, script)
			}
		}
	}
}

func windowsDeferredMoveItemCommands(script string) []string {
	var commands []string
	const marker = "Move-Item "
	rest := script
	for {
		idx := strings.Index(rest, marker)
		if idx < 0 {
			return commands
		}
		command := rest[idx:]
		next := idx + len(marker)
		if end := strings.Index(command, ";"); end >= 0 {
			command = command[:end]
			next = idx + end + 1
		}
		commands = append(commands, strings.TrimSpace(command))
		rest = rest[next:]
	}
}
