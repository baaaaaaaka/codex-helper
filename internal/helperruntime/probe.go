package helperruntime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const ProtocolProbeFlag = "--cxp-boot-protocol"

// EnvProbeProtocol distinguishes the native candidate probe from historical
// updaters, which also set EnvDisable while validating downloads.
const EnvProbeProtocol = "CXP_RUNTIME_PROBE_PROTOCOL"

// HandleProtocolProbe precedes all runtime dispatch and application setup.
func HandleProtocolProbe(args []string, out io.Writer) (int, bool) {
	if len(args) != 2 || args[1] != ProtocolProbeFlag {
		return 0, false
	}
	protocol := BootProtocol
	// Historical update probes already set runtime-disable, but have no native
	// probe marker. The candidate also implements their migration-only ABI.
	if os.Getenv(EnvDisable) == "1" && os.Getenv(EnvProbeProtocol) == "" {
		protocol = 1
	}
	_, err := fmt.Fprintln(out, protocol)
	if err != nil {
		return 1, true
	}
	return 0, true
}

// ProbeProtocol checks native runtime compatibility. Historical callers enter
// through the migration-only ABI instead of a second runtime backend.
func ProbeProtocol(ctx context.Context, path string) error {
	out, err := ProbeExecutable(ctx, path, []string{ProtocolProbeFlag}, 10*time.Second)
	if err != nil {
		return fmt.Errorf("probe boot protocol: %w", err)
	}
	protocol, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil || protocol != BootProtocol {
		return fmt.Errorf("incompatible boot protocol; use the installer to change the bootstrap")
	}
	return nil
}

type probeOutput struct {
	buffer    bytes.Buffer
	truncated bool
}

func (b *probeOutput) Bytes() []byte { return b.buffer.Bytes() }
func (b *probeOutput) Len() int      { return b.buffer.Len() }

func (b *probeOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (16 << 10) - b.Len()
	if len(p) > remaining {
		b.truncated = true
		p = p[:remaining]
	}
	_, _ = b.buffer.Write(p)
	return n, nil
}

// ProbeExecutable bounds time, output, inherited pipes and process lifetime.
// Even a broken --version probe must not exhaust a live TUI's memory or leave
// descendants behind. The executable has already passed download verification.
func ProbeExecutable(ctx context.Context, path string, args []string, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = append(LauncherEnvironment(os.Environ()), EnvDisable+"=1", EnvProbeProtocol+"="+strconv.Itoa(BootProtocol))
	cmd.WaitDelay = 500 * time.Millisecond
	var output probeOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	configureBootProbe(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	closeTree, killTree, _ := attachBootProcess(cmd)
	defer closeTree()
	defer killTree()
	err := cmd.Wait()
	if output.truncated {
		return output.Bytes(), fmt.Errorf("executable probe output exceeds 16 KiB")
	}
	return output.Bytes(), err
}
