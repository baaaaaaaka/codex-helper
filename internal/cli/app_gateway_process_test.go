package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestParseProxyPorts(t *testing.T) {
	got := parseProxyPorts("ChatGPT \"--proxy-server=http://127.0.0.1:3804\" --new-window\x00--PROXY-SERVER=http://127.0.0.1:2743")
	if len(got) != 2 || got[0] != 3804 || got[1] != 2743 {
		t.Fatalf("parseProxyPorts = %#v", got)
	}
}

func TestAppendUniqueProxyPorts(t *testing.T) {
	got := appendUniqueProxyPorts([]int{3804}, []int{3804, 2743, 2743})
	if len(got) != 2 || got[0] != 3804 || got[1] != 2743 {
		t.Fatalf("appendUniqueProxyPorts = %#v", got)
	}
}

func TestDesktopProxyPortsFromRowsFiltersProcessesAndDeduplicates(t *testing.T) {
	rows := []desktopProcessRow{
		{Name: `C:\Program Files\ChatGPT.exe`, CommandLine: `ChatGPT.exe --proxy-server=http://127.0.0.1:3804`},
		{Name: "/opt/codex", CommandLine: "codex --proxy-server=http://127.0.0.1:2743 --proxy-server=http://127.0.0.1:3804"},
		{Name: "other-app", CommandLine: "other-app --proxy-server=http://127.0.0.1:9999"},
	}
	if got := desktopProxyPortsFromRows(rows); !reflect.DeepEqual(got, []int{3804, 2743}) {
		t.Fatalf("desktop proxy ports = %#v", got)
	}
}

func TestParseWindowsDesktopProcessRowsAcceptsArrayAndSingleObject(t *testing.T) {
	arrayRows, err := parseWindowsDesktopProcessRows([]byte(`[{"Name":"ChatGPT.exe","CommandLine":"--proxy-server=http://127.0.0.1:3804"}]`))
	if err != nil || len(arrayRows) != 1 || arrayRows[0].Name != "ChatGPT.exe" {
		t.Fatalf("Windows array rows = %#v/%v", arrayRows, err)
	}
	singleRows, err := parseWindowsDesktopProcessRows([]byte(`{"Name":"Codex.exe","CommandLine":"--proxy-server=http://127.0.0.1:2743"}`))
	if err != nil || len(singleRows) != 1 || singleRows[0].CommandLine == "" {
		t.Fatalf("Windows single row = %#v/%v", singleRows, err)
	}
	if _, err := parseWindowsDesktopProcessRows([]byte(`{"Name":`)); err == nil {
		t.Fatal("malformed PowerShell JSON was accepted")
	}
}

func TestParseDarwinDesktopProcessRowsHandlesCommandArguments(t *testing.T) {
	output := "  10 /Applications/ChatGPT.app/Contents/MacOS/ChatGPT ChatGPT --proxy-server=http://127.0.0.1:3804 --new-window\n" +
		"  11 /usr/local/bin/codex codex --proxy-server=http://127.0.0.1:2743\n" +
		"  12 /usr/bin/other other --proxy-server=http://127.0.0.1:9999\n"
	rows := parseDarwinDesktopProcessRows(output)
	if got := desktopProxyPortsFromRows(rows); !reflect.DeepEqual(got, []int{3804, 2743}) {
		t.Fatalf("darwin proxy ports = %#v (rows=%#v)", got, rows)
	}
}

func TestDiscoverDesktopProxyPortsFromProcRootUsesExecutableAndCmdline(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not provide POSIX /proc symlinks")
	}
	root := t.TempDir()
	for _, pid := range []string{"100", "101", "not-a-pid"} {
		if err := os.MkdirAll(filepath.Join(root, pid), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	chatgptExe := filepath.Join(root, "ChatGPT")
	if err := os.WriteFile(chatgptExe, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(chatgptExe, filepath.Join(root, "100", "exe")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "100", "cmdline"), []byte("ChatGPT\x00--proxy-server=http://127.0.0.1:3804\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	otherExe := filepath.Join(root, "other-bin")
	if err := os.WriteFile(otherExe, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(otherExe, filepath.Join(root, "101", "exe")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "101", "cmdline"), []byte("other --proxy-server=http://127.0.0.1:2743"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := discoverDesktopProxyPortsFromProcRoot(context.Background(), root)
	if err != nil || !reflect.DeepEqual(got, []int{3804}) {
		t.Fatalf("proc-root discovery = %#v/%v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := discoverDesktopProxyPortsFromProcRoot(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled proc-root discovery error = %v", err)
	}
}
