package codexrunner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const defaultAppServerProcessStderrLimit = 32 * 1024

var errAppServerProcessStdoutClosed = errors.New("app-server stdout closed")

type AppServerProcessStarter struct {
	StderrLimit int
}

func (s AppServerProcessStarter) StartAppServer(ctx context.Context, req AppServerStartRequest) (AppServerLineTransport, error) {
	startCtx := ctx
	cancelStart := func() {}
	if req.Timeout > 0 {
		startCtx, cancelStart = context.WithTimeout(ctx, req.Timeout)
		defer cancelStart()
	}
	if err := startCtx.Err(); err != nil {
		return nil, err
	}

	command := strings.TrimSpace(req.Command)
	if command == "" {
		command = defaultCodexCommand
	}

	processCtx, cancelProcess := context.WithCancel(context.Background())
	cmd := exec.CommandContext(processCtx, command, req.Args...)
	if req.WorkingDir != "" {
		cmd.Dir = req.WorkingDir
	}

	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		cancelProcess()
		return nil, fmt.Errorf("create app-server stdin pipe: %w", err)
	}
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		cancelProcess()
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		return nil, fmt.Errorf("create app-server stdout pipe: %w", err)
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		cancelProcess()
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		return nil, fmt.Errorf("create app-server stderr pipe: %w", err)
	}

	cmd.Stdin = stdinRead
	cmd.Stdout = stdoutWrite
	cmd.Stderr = stderrWrite

	if err := cmd.Start(); err != nil {
		cancelProcess()
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		_ = stderrRead.Close()
		_ = stderrWrite.Close()
		return nil, fmt.Errorf("start app-server process %q: %w", command, err)
	}

	_ = stdinRead.Close()
	_ = stdoutWrite.Close()
	_ = stderrWrite.Close()

	transport := &appServerProcessTransport{
		cmd:           cmd,
		cancelProcess: cancelProcess,
		stdin:         stdinWrite,
		stdout:        stdoutRead,
		stderr:        stderrRead,
		stderrBuffer:  newLimitedStderrBuffer(s.StderrLimit),
		lines:         make(chan appServerProcessLine, 16),
		done:          make(chan struct{}),
		waitDone:      make(chan struct{}),
	}
	transport.wg.Add(2)
	go transport.readStdout()
	go transport.readStderr()
	go transport.wait()

	if err := startCtx.Err(); err != nil {
		_ = transport.Close()
		return nil, err
	}
	return transport, nil
}

type appServerProcessTransport struct {
	cmd           *exec.Cmd
	cancelProcess context.CancelFunc
	stdin         *os.File
	stdout        *os.File
	stderr        *os.File
	stderrBuffer  *limitedStderrBuffer

	lines    chan appServerProcessLine
	done     chan struct{}
	waitDone chan struct{}

	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
	waitMu    sync.Mutex
	waitErr   error
	wg        sync.WaitGroup
}

type appServerProcessLine struct {
	line []byte
	err  error
}

func (p *appServerProcessTransport) WriteLine(ctx context.Context, line []byte) error {
	select {
	case <-ctx.Done():
		_ = p.Close()
		return p.diagnosticError(ctx.Err(), "write app-server stdin")
	case <-p.done:
		return p.diagnosticError(errors.New("app-server process is closed"), "write app-server stdin")
	default:
	}

	data := append([]byte{}, line...)
	if !bytes.HasSuffix(data, []byte("\n")) {
		data = append(data, '\n')
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	result := make(chan error, 1)
	go func() {
		result <- writeAll(p.stdin, data)
	}()

	select {
	case err := <-result:
		if err != nil {
			return p.diagnosticError(err, "write app-server stdin")
		}
		return nil
	case <-ctx.Done():
		_ = p.Close()
		return p.diagnosticError(ctx.Err(), "write app-server stdin")
	case <-p.done:
		return p.diagnosticError(errors.New("app-server process is closed"), "write app-server stdin")
	}
}

func (p *appServerProcessTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case item, ok := <-p.lines:
		if !ok {
			return nil, p.diagnosticError(errAppServerProcessStdoutClosed, "read app-server stdout")
		}
		if item.err != nil {
			return nil, p.diagnosticError(item.err, "read app-server stdout")
		}
		return item.line, nil
	case <-ctx.Done():
		_ = p.Close()
		return nil, p.diagnosticError(ctx.Err(), "read app-server stdout")
	case <-p.done:
		return nil, p.diagnosticError(errors.New("app-server process is closed"), "read app-server stdout")
	}
}

func (p *appServerProcessTransport) Close() error {
	p.closeOnce.Do(func() {
		close(p.done)
		_ = p.stdin.Close()
		p.cancelProcess()
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		<-p.waitDone
		_ = p.stdout.Close()
		_ = p.stderr.Close()
		p.wg.Wait()

		if err := p.waitError(); err != nil && !isExpectedAppServerCloseError(err) {
			p.closeErr = p.diagnosticError(err, "close app-server process")
		}
	})
	return p.closeErr
}

func (p *appServerProcessTransport) readStdout() {
	defer p.wg.Done()
	defer close(p.lines)

	reader := bufio.NewReader(p.stdout)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			line = bytes.TrimRight(line, "\r\n")
			p.sendLine(appServerProcessLine{line: append([]byte{}, line...)})
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errAppServerProcessStdoutClosed
			}
			p.sendLine(appServerProcessLine{err: err})
			return
		}
	}
}

func (p *appServerProcessTransport) readStderr() {
	defer p.wg.Done()
	_, _ = io.Copy(p.stderrBuffer, p.stderr)
}

func (p *appServerProcessTransport) sendLine(line appServerProcessLine) {
	select {
	case p.lines <- line:
	case <-p.done:
	}
}

func (p *appServerProcessTransport) wait() {
	err := p.cmd.Wait()
	p.waitMu.Lock()
	p.waitErr = err
	p.waitMu.Unlock()
	close(p.waitDone)
}

func (p *appServerProcessTransport) waitError() error {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

func (p *appServerProcessTransport) waitForExit(timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.waitDone:
	case <-timer.C:
	}
}

func (p *appServerProcessTransport) diagnosticError(err error, action string) error {
	if err == nil {
		return nil
	}
	p.waitForExit(50 * time.Millisecond)
	if stderr := p.stderrBuffer.String(); stderr != "" {
		return fmt.Errorf("%s: %w; stderr: %s", action, err, stderr)
	}
	if waitErr := p.waitError(); waitErr != nil {
		return fmt.Errorf("%s: %w; process: %v", action, err, waitErr)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func isExpectedAppServerCloseError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return true
	}
	return false
}

type limitedStderrBuffer struct {
	mu        sync.Mutex
	limit     int
	buf       []byte
	truncated bool
}

func newLimitedStderrBuffer(limit int) *limitedStderrBuffer {
	if limit <= 0 {
		limit = defaultAppServerProcessStderrLimit
	}
	return &limitedStderrBuffer{limit: limit}
}

func (b *limitedStderrBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(p) >= b.limit {
		b.buf = append([]byte{}, p[len(p)-b.limit:]...)
		b.truncated = true
		return len(p), nil
	}
	if overflow := len(b.buf) + len(p) - b.limit; overflow > 0 {
		b.buf = append([]byte{}, b.buf[overflow:]...)
		b.truncated = true
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *limitedStderrBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	message := strings.TrimSpace(string(b.buf))
	if message == "" {
		return ""
	}
	if b.truncated {
		return "[truncated] " + message
	}
	return message
}
