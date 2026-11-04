// Command fakecodex is the fake Codex CLI used by the Teams E2E harness. Only
// `fakecodex app-server [flags...]` is supported; extra flags are accepted and
// ignored like real Codex ignores unknown config overrides. `--version` prints
// a Codex-like version string. See package fakecodex.
package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && (args[0] == "--version" || args[0] == "-V") {
		fmt.Println("codex-cli 0.58.0")
		return
	}
	if len(args) == 0 || args[0] != "app-server" {
		fmt.Fprintf(os.Stderr, "fakecodex: only `app-server` is supported (got %q)\n", args)
		os.Exit(2)
	}
	brain := os.Getenv(fakecodex.EnvBrainURL)
	if brain == "" {
		fmt.Fprintf(os.Stderr, "fakecodex: %s is not set\n", fakecodex.EnvBrainURL)
		os.Exit(2)
	}
	codexHome := os.Getenv("CODEX_HOME")
	if codexHome == "" {
		home, _ := os.UserHomeDir()
		codexHome = filepath.Join(home, ".codex")
	}
	stateDir := os.Getenv(fakecodex.EnvStateDir)
	if stateDir == "" {
		stateDir = filepath.Join(codexHome, "e2e-fakecodex")
	}
	kind := os.Getenv(fakecodex.EnvKind)
	if kind == "" {
		kind = "work"
	}
	// Like real Codex (a Rust binary, SIGPIPE ignored), writing to a dead
	// helper's pipes must fail with EPIPE instead of killing the process.
	signal.Notify(make(chan os.Signal, 1), syscall.SIGPIPE)

	// The helper keeps only a bounded stderr tail, so the protocol log also
	// goes to a per-process file the harness shows when a scenario fails.
	var log io.Writer = os.Stderr
	if err := os.MkdirAll(filepath.Join(stateDir, "logs"), 0o755); err == nil {
		if f, err := os.Create(filepath.Join(stateDir, "logs", fmt.Sprintf("appserver-%s-%d.log", kind, os.Getpid()))); err == nil {
			defer f.Close()
			log = io.MultiWriter(os.Stderr, f)
		}
	}
	err := fakecodex.Serve(fakecodex.Config{
		In:          os.Stdin,
		Out:         os.Stdout,
		Log:         log,
		Store:       fakecodex.Store{Dir: stateDir},
		SessionsDir: filepath.Join(codexHome, "sessions"),
		Brain:       brain,
		Kind:        kind,
		Instance:    os.Getenv(fakecodex.EnvInstance),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakecodex: %v\n", err)
		os.Exit(1)
	}
}
