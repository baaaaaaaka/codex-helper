//go:build windows

package codexcontract

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsRemoteTUIProcess struct {
	process windows.Handle
	console windows.Handle
	input   *os.File
	output  *os.File

	bufferMu sync.Mutex
	buffer   bytes.Buffer
	drainWG  sync.WaitGroup
	done     chan struct{}
	waitErr  error
	stopOnce sync.Once
}

func startRemoteTUIProcess(ctx context.Context, command, remoteURL, codexHome string) (remoteTUIProcess, error) {
	var inputRead, inputWrite, outputRead, outputWrite windows.Handle
	security := &windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), InheritHandle: 1}
	if err := windows.CreatePipe(&inputRead, &inputWrite, security, 0); err != nil {
		return nil, fmt.Errorf("create ConPTY input pipe: %w", err)
	}
	defer func() {
		if inputRead != 0 {
			_ = windows.CloseHandle(inputRead)
		}
	}()
	if err := windows.CreatePipe(&outputRead, &outputWrite, security, 0); err != nil {
		_ = windows.CloseHandle(inputWrite)
		return nil, fmt.Errorf("create ConPTY output pipe: %w", err)
	}
	defer func() {
		if outputWrite != 0 {
			_ = windows.CloseHandle(outputWrite)
		}
	}()

	var console windows.Handle
	if err := windows.CreatePseudoConsole(windows.Coord{X: 80, Y: 25}, inputRead, outputWrite, 0, &console); err != nil {
		_ = windows.CloseHandle(inputWrite)
		_ = windows.CloseHandle(outputRead)
		return nil, fmt.Errorf("create ConPTY: %w", err)
	}
	attributes, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		windows.ClosePseudoConsole(console)
		_ = windows.CloseHandle(inputWrite)
		_ = windows.CloseHandle(outputRead)
		return nil, fmt.Errorf("allocate ConPTY process attributes: %w", err)
	}
	defer attributes.Delete()
	if err := attributes.Update(windows.PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, unsafe.Pointer(console), unsafe.Sizeof(console)); err != nil {
		windows.ClosePseudoConsole(console)
		_ = windows.CloseHandle(inputWrite)
		_ = windows.CloseHandle(outputRead)
		return nil, fmt.Errorf("attach ConPTY process attribute: %w", err)
	}

	cmdPath, err := exec.LookPath("cmd.exe")
	if err != nil {
		windows.ClosePseudoConsole(console)
		_ = windows.CloseHandle(inputWrite)
		_ = windows.CloseHandle(outputRead)
		return nil, fmt.Errorf("find cmd.exe: %w", err)
	}
	inner := strings.Join([]string{
		`set "TERM=xterm-256color"`,
		`set "CODEX_HOME=` + strings.ReplaceAll(codexHome, `"`, `""`) + `"`,
		`set "OPENAI_API_KEY=cxp-contract-key"`,
		windowsCmdQuote(command) + ` -c "features.tui_app_server=true" --remote ` + windowsCmdQuote(remoteURL),
	}, "&&")
	application, err := windows.UTF16PtrFromString(cmdPath)
	if err != nil {
		windows.ClosePseudoConsole(console)
		_ = windows.CloseHandle(inputWrite)
		_ = windows.CloseHandle(outputRead)
		return nil, err
	}
	commandLine, err := windows.UTF16PtrFromString(windows.ComposeCommandLine([]string{cmdPath, "/d", "/s", "/c", inner}))
	if err != nil {
		windows.ClosePseudoConsole(console)
		_ = windows.CloseHandle(inputWrite)
		_ = windows.CloseHandle(outputRead)
		return nil, err
	}
	startup := windows.StartupInfoEx{
		StartupInfo:             windows.StartupInfo{Cb: uint32(unsafe.Sizeof(windows.StartupInfoEx{}))},
		ProcThreadAttributeList: attributes.List(),
	}
	var info windows.ProcessInformation
	if err := windows.CreateProcess(application, commandLine, nil, nil, false, windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_UNICODE_ENVIRONMENT, nil, nil, &startup.StartupInfo, &info); err != nil {
		windows.ClosePseudoConsole(console)
		_ = windows.CloseHandle(inputWrite)
		_ = windows.CloseHandle(outputRead)
		return nil, fmt.Errorf("start Codex in ConPTY: %w", err)
	}
	_ = windows.CloseHandle(info.Thread)
	_ = windows.CloseHandle(inputRead)
	inputRead = 0
	_ = windows.CloseHandle(outputWrite)
	outputWrite = 0

	process := &windowsRemoteTUIProcess{
		process: info.Process,
		console: console,
		input:   os.NewFile(uintptr(inputWrite), "conpty-input"),
		output:  os.NewFile(uintptr(outputRead), "conpty-output"),
		done:    make(chan struct{}),
	}
	process.drainWG.Add(1)
	go func() {
		defer process.drainWG.Done()
		_, _ = io.Copy(lockedWriter{process}, process.output)
	}()
	go process.wait()
	go func() {
		select {
		case <-ctx.Done():
			process.Stop()
		case <-process.done:
		}
	}()
	return process, nil
}

type lockedWriter struct{ process *windowsRemoteTUIProcess }

func (w lockedWriter) Write(raw []byte) (int, error) {
	w.process.bufferMu.Lock()
	defer w.process.bufferMu.Unlock()
	return w.process.buffer.Write(raw)
}

func (p *windowsRemoteTUIProcess) wait() {
	defer close(p.done)
	if _, err := windows.WaitForSingleObject(p.process, windows.INFINITE); err != nil {
		p.waitErr = fmt.Errorf("wait for ConPTY process: %w", err)
	} else {
		var exitCode uint32
		if err := windows.GetExitCodeProcess(p.process, &exitCode); err != nil {
			p.waitErr = fmt.Errorf("read ConPTY process exit code: %w", err)
		} else if exitCode != 0 {
			p.waitErr = fmt.Errorf("exit status %d", exitCode)
		}
	}
	windows.ClosePseudoConsole(p.console)
	_ = p.input.Close()
	_ = p.output.Close()
	p.drainWG.Wait()
	_ = windows.CloseHandle(p.process)
}

func (p *windowsRemoteTUIProcess) Wait() error {
	<-p.done
	return p.waitErr
}

func (p *windowsRemoteTUIProcess) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() { _ = windows.TerminateProcess(p.process, 1) })
}

func (p *windowsRemoteTUIProcess) Output() string {
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()
	return p.buffer.String()
}

func windowsCmdQuote(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}
