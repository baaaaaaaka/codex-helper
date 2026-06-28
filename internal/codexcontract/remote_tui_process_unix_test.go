//go:build !windows

package codexcontract

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

type unixRemoteTUIProcess struct {
	cmd    *exec.Cmd
	output bytes.Buffer
	stop   sync.Once
}

func startRemoteTUIProcess(ctx context.Context, command, remoteURL, remoteAuthTokenEnv, remoteAuthToken, codexHome string) (remoteTUIProcess, error) {
	ptyCommand, err := exec.LookPath("script")
	if err != nil {
		return nil, fmt.Errorf("script is required for the remote TUI contract probe: %w", err)
	}
	commandLine := shellQuote(command) + " -c features.tui_app_server=true --remote " + shellQuote(remoteURL)
	if strings.TrimSpace(remoteAuthTokenEnv) != "" {
		commandLine += " --remote-auth-token-env " + shellQuote(remoteAuthTokenEnv)
	}
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.CommandContext(ctx, ptyCommand, "-q", "/dev/null", "/bin/sh", "-lc", commandLine)
	} else {
		cmd = exec.CommandContext(ctx, ptyCommand, "-qefc", commandLine, "/dev/null")
	}
	// script(1) is only the PTY supervisor. The shell and Codex processes are
	// descendants, so killing only cmd.Process can let Codex keep writing to its
	// temporary home after Wait returns. Give the complete PTY tree a dedicated
	// process group and terminate that group in Stop.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	process := &unixRemoteTUIProcess{cmd: cmd}
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		"CODEX_HOME="+codexHome,
		"CODEX_SQLITE_HOME="+filepath.Join(codexHome, "remote-tui-sqlite"),
		"OPENAI_API_KEY=cxp-contract-key",
	)
	if strings.TrimSpace(remoteAuthTokenEnv) != "" {
		cmd.Env = append(cmd.Env, remoteAuthTokenEnv+"="+remoteAuthToken)
	}
	cmd.Stdout = &process.output
	cmd.Stderr = &process.output
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return process, nil
}

func (p *unixRemoteTUIProcess) Wait() error { return p.cmd.Wait() }

func (p *unixRemoteTUIProcess) Stop() {
	if p != nil && p.cmd != nil && p.cmd.Process != nil {
		p.stop.Do(func() {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		})
	}
}

func (p *unixRemoteTUIProcess) Output() string { return p.output.String() }

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
