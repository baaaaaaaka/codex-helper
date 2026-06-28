package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCodexRuntimeEnvironmentPrependsManagedNodeAfterStalePATHOverlay(t *testing.T) {
	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported managed-node architecture")
	}
	home := t.TempDir()
	nodeRoot := filepath.Join(home, "managed-node")
	nodeBin := filepath.Join(nodeRoot, "v22-"+runtime.GOOS+"-"+arch, "bin")
	nodeName := "node"
	if runtime.GOOS == "windows" {
		nodeBin = filepath.Join(nodeRoot, "v22-win-"+arch)
		nodeName = "node.exe"
	}
	if err := os.MkdirAll(nodeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nodeBin, nodeName), []byte("managed node fixture"), 0o700); err != nil {
		t.Fatal(err)
	}

	got := codexRuntimeEnvironment(
		[]string{"HOME=" + home, "CODEX_NODE_INSTALL_ROOT=" + nodeRoot, "PATH=/fresh/system"},
		[]string{"PATH=/stale/service", "CXP_TEST=overlay"},
		nil,
	)
	parts := filepath.SplitList(envValue(got, "PATH"))
	if len(parts) < 2 || parts[0] != nodeBin || parts[1] != "/stale/service" {
		t.Fatalf("PATH = %q, want managed Node before preserved overlay", envValue(got, "PATH"))
	}
	if envValue(got, "CXP_TEST") != "overlay" {
		t.Fatalf("overlay missing: %#v", got)
	}
}

func TestCodexRuntimeEnvironmentUsesTargetIdentityManagedNode(t *testing.T) {
	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported managed-node architecture")
	}
	targetHome := t.TempDir()
	nodeBin := filepath.Join(targetHome, ".cache", "codex-proxy", "node", "v22-"+runtime.GOOS+"-"+arch, "bin")
	nodeName := "node"
	if runtime.GOOS == "windows" {
		nodeBin = filepath.Join(targetHome, "AppData", "Local", "codex-proxy", "node", "v22-win-"+arch)
		nodeName = "node.exe"
	}
	if err := os.MkdirAll(nodeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nodeBin, nodeName), []byte("managed node fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	identity := &execIdentity{UID: 1000, GID: 1000, Username: "alice", Home: targetHome, GroupsKnown: true}
	got := codexRuntimeEnvironment([]string{"HOME=/root", "PATH=/usr/bin"}, nil, identity)
	if envValue(got, "HOME") != targetHome || envValue(got, "USER") != "alice" {
		t.Fatalf("identity environment = %#v", got)
	}
	parts := filepath.SplitList(envValue(got, "PATH"))
	if len(parts) == 0 || parts[0] != nodeBin {
		t.Fatalf("PATH = %q, want target managed Node first", envValue(got, "PATH"))
	}
}

func TestCodexRuntimeEnvironmentOverlayCarriesOnlyResolvedPATH(t *testing.T) {
	got := codexRuntimeEnvironmentOverlay(
		[]string{"CODEX_HOME=/explicit", "HTTP_PROXY=http://explicit-proxy", "PATH=/stale"},
		[]string{"HOME=/captured", "HTTP_PROXY=http://captured-proxy", "PATH=/managed-node:/system"},
	)
	if envValue(got, "PATH") != "/managed-node:/system" {
		t.Fatalf("PATH = %q, want resolved runtime path", envValue(got, "PATH"))
	}
	if envValue(got, "HTTP_PROXY") != "http://explicit-proxy" || envValue(got, "CODEX_HOME") != "/explicit" {
		t.Fatalf("explicit overlay values changed: %#v", got)
	}
	if _, ok := explicitEnvironmentValue(got, "HOME"); ok {
		t.Fatalf("captured base environment leaked into overlay: %#v", got)
	}
}

func TestMergeCLIEnvironmentEmitsUniqueKeysAndOverlayWins(t *testing.T) {
	got := mergeCLIEnvironment(
		[]string{"PATH=/base", "HOME=/home/base", "PATH=/base-last"},
		[]string{"PATH=/overlay", "HOME=/home/overlay"},
	)
	counts := map[string]int{}
	for _, entry := range got {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			counts[key]++
		}
	}
	if counts["PATH"] != 1 || counts["HOME"] != 1 {
		t.Fatalf("environment contains duplicate keys: %#v", got)
	}
	if envValue(got, "PATH") != "/overlay" || envValue(got, "HOME") != "/home/overlay" {
		t.Fatalf("overlay did not win: %#v", got)
	}
}

func writeManagedNodeCodexWrapperFixture(t *testing.T, realCodexPath string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX env-node wrapper fixture")
	}
	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported managed-node architecture")
	}
	sedPath, err := exec.LookPath("sed")
	if err != nil {
		t.Skip("sed is required by the app-server fixture")
	}
	root := t.TempDir()
	home := filepath.Join(root, "home")
	nodeBin := filepath.Join(home, ".cache", "codex-proxy", "node", "v22-"+runtime.GOOS+"-"+arch, "bin")
	toolsDir := filepath.Join(root, "tools")
	if err := os.MkdirAll(nodeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(toolsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sedPath, filepath.Join(toolsDir, "sed")); err != nil {
		t.Fatal(err)
	}
	nodeScript := "#!/bin/sh\nshift\nexec \"$CXP_TEST_REAL_CODEX\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(nodeBin, "node"), []byte(nodeScript), 0o700); err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(root, "codex")
	if err := os.WriteFile(wrapper, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("CODEX_NODE_INSTALL_ROOT", filepath.Join(home, ".cache", "codex-proxy", "node"))
	t.Setenv("CXP_TEST_REAL_CODEX", realCodexPath)
	t.Setenv("PATH", toolsDir)
	return wrapper
}
