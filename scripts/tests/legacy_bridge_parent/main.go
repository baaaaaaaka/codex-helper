package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		if os.Getenv("CXP_RUNTIME") == "1" {
			fmt.Println("codex-proxy version 0.1.13-rc.31")
			return
		}
		executable, err := helperpath.RawExecutable()
		if err != nil {
			panic(err)
		}
		root := filepath.Join(filepath.Dir(executable), ".cxp-runtime")
		raw, err := os.ReadFile(filepath.Join(root, "active"))
		if err != nil {
			panic(err)
		}
		version := strings.TrimSpace(string(raw))
		name := "cxp"
		if runtime.GOOS == "windows" {
			name = "cxp.exe"
		}
		target := filepath.Join(root, "versions", version, name)
		cmd := exec.Command(target, "--version")
		for _, value := range os.Environ() {
			name, _, _ := strings.Cut(value, "=")
			if strings.HasPrefix(strings.ToUpper(name), "CXP_RUNTIME") || strings.EqualFold(name, "CXP_ENTRY_PATH") {
				continue
			}
			cmd.Env = append(cmd.Env, value)
		}
		cmd.Env = append(cmd.Env,
			"CXP_RUNTIME=1",
			"CXP_RUNTIME_ROOT="+root,
			"CXP_RUNTIME_VERSION="+version,
			"CXP_ENTRY_PATH="+executable,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			panic(err)
		}
		return
	}
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: legacy_bridge_parent <candidate>")
		os.Exit(2)
	}
	cmd := exec.Command(os.Args[1], "--version")
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
