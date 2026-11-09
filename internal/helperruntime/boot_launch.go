package helperruntime

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/term"
)

const envBootAddress = "CXP_BOOT_ADDRESS"
const envBootToken = "CXP_BOOT_TOKEN"

var bootConnection net.Conn
var bootAck <-chan error
var bootConnectOnce sync.Once
var bootConnectErr error
var bootHandoffMu sync.Mutex
var bootHandoffDone = make(chan struct{})
var bootSignals chan os.Signal

// PrepareBoot connects before CLI initialization. If the parent dies before
// handoff, the child must stop even when initialization never reaches Ready.
func PrepareBoot() error {
	bootConnectOnce.Do(func() { bootConnectErr = connectBoot() })
	return bootConnectErr
}

func connectBoot() error {
	address, token := os.Getenv(envBootAddress), os.Getenv(envBootToken)
	if address == "" && token == "" {
		return nil
	}
	if !strings.HasPrefix(address, "127.0.0.1:") || len(token) != 64 {
		return fmt.Errorf("invalid boot handshake")
	}
	c, err := net.DialTimeout("tcp4", address, bootReadyTimeout)
	if err != nil {
		return err
	}
	if _, err = io.WriteString(c, token+"\n"); err != nil {
		_ = c.Close()
		return err
	}
	reader := bufio.NewReader(io.LimitReader(c, 64))
	_ = c.SetReadDeadline(time.Now().Add(bootReadyTimeout))
	attached, err := reader.ReadString('\n')
	if err != nil {
		_ = c.Close()
		return fmt.Errorf("launcher did not accept startup ownership: %w", err)
	}
	if attached != "ATTACHED\n" {
		_ = c.Close()
		return fmt.Errorf("invalid launcher ownership acknowledgement")
	}
	_ = c.SetReadDeadline(time.Time{})
	ack := make(chan error, 1)
	bootConnection, bootAck = c, ack
	fd := int(os.Stdin.Fd())
	terminal, _ := term.GetState(fd)
	bootSignals = make(chan os.Signal, 2)
	signal.Notify(bootSignals, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-bootSignals:
			phase := "CANCEL"
			if sig == syscall.SIGTERM {
				phase = "TERM"
			}
			if handled, _ := handoffBootOnce(phase); handled {
				code := 130
				if phase == "TERM" {
					code = 143
				}
				os.Exit(code)
			}
		case <-bootHandoffDone:
		}
	}()
	go func() {
		line, err := reader.ReadString('\n')
		if err != nil || line != "GO\n" {
			// No user operation has been accepted yet.
			if terminal != nil {
				_ = term.Restore(fd, terminal)
			}
			abortBootStartup()
		}
		ack <- nil
	}()
	return nil
}

// Ready is called after the first TUI frame, before accepting input or starting
// background work. The parent acknowledges ownership before any user operation.
// A missing parent fails closed; a normal non-supervised launch is a no-op.
func Ready() error {
	return handoffBoot("READY")
}

// A setup question can wait indefinitely for the user. Transfer ownership
// without promoting the candidate; never time out and replay user input.
func BeforeInteractiveSetup() error { return handoffBoot("SETUP") }

// A CLI signal context can turn Ctrl+C into a successful return. Notify the
// parent before returning so that cancellation cannot look like a boot failure.
func CancelStartup() { _ = handoffBoot("CANCEL") }

func handoffBoot(phase string) error {
	_, err := handoffBootOnce(phase)
	return err
}

func handoffBootOnce(phase string) (bool, error) {
	if err := PrepareBoot(); err != nil {
		return false, err
	}
	bootHandoffMu.Lock()
	defer bootHandoffMu.Unlock()
	if bootConnection == nil {
		return false, nil
	}
	c := bootConnection
	_ = c.SetDeadline(time.Now().Add(bootReadyTimeout))
	if _, err := io.WriteString(c, phase+"\n"); err != nil {
		return true, err
	}
	if err := <-bootAck; err != nil {
		return true, err
	}
	_ = c.Close()
	bootConnection = nil
	close(bootHandoffDone)
	signal.Stop(bootSignals)
	// Do not let another TUI in this process, or a child process, reuse the IPC.
	_ = os.Unsetenv(envBootAddress)
	_ = os.Unsetenv(envBootToken)
	return true, nil
}

type bootRunResult struct {
	ready, cancelled bool
	code             int
	err              error
}

func superviseBoot(target string, args, env []string, onReady func(), timeout time.Duration) bootRunResult {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return bootRunResult{err: err}
	}
	defer listener.Close()
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return bootRunResult{err: err}
	}
	secret := hex.EncodeToString(token[:])
	env = append(env, envBootAddress+"="+listener.Addr().String(), envBootToken+"="+secret)
	cmd := exec.Command(target, args[1:]...)
	cmd.Args[0], cmd.Env = target, env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	restore := prepareBootTerminal(cmd)
	defer restore()
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := cmd.Start(); err != nil {
		return bootRunResult{err: err}
	}
	closeTree, killTree, handoffTree := attachBootProcess(cmd)
	defer closeTree()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	type bootHello struct {
		conn  net.Conn
		phase string
	}
	connections := make(chan bootHello)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			_ = c.SetDeadline(time.Now().Add(timeout))
			reader := bufio.NewReader(io.LimitReader(c, 128))
			line, err := reader.ReadString('\n')
			if err != nil || line != secret+"\n" {
				_ = c.Close()
				continue
			}
			// Accept is started only after process-tree ownership is attached.
			// The child must not spawn initialization descendants before its
			// Windows Job Object has been assigned.
			if _, err := io.WriteString(c, "ATTACHED\n"); err != nil {
				_ = c.Close()
				continue
			}
			phase, err := reader.ReadString('\n')
			if err != nil || (phase != "READY\n" && phase != "SETUP\n" && phase != "CANCEL\n" && phase != "TERM\n") {
				_ = c.Close()
				continue
			}
			select {
			case connections <- bootHello{c, phase}:
			case <-stop:
				_ = c.Close()
			}
			return
		}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	result := bootRunResult{}
	for {
		select {
		case hello := <-connections:
			c := hello.conn
			// Confirmation is best effort. An unwritable record retains Good=A,
			// while this healthy B is still allowed to serve the current window.
			if err := handoffTree(); err != nil {
				_ = c.Close()
				killTree()
				<-done
				return bootRunResult{err: err}
			}
			_, err := io.WriteString(c, "GO\n")
			_ = c.Close()
			if err != nil {
				killTree()
				<-done
				return bootRunResult{err: err}
			}
			if hello.phase == "CANCEL\n" || hello.phase == "TERM\n" {
				killTree()
				<-done
				code := 130
				if hello.phase == "TERM\n" {
					code = 143
				}
				return bootRunResult{cancelled: true, code: code}
			}
			result.ready = true
			if hello.phase == "READY\n" {
				onReady()
			}
			_ = listener.Close()
			timer.Stop()
		case err := <-done:
			result.err = err
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				result.code = exit.ExitCode()
				if cancelled, code := bootCancellation(exit); cancelled {
					result.cancelled, result.code = true, code
				}
			} else if err != nil {
				result.code = 1
			}
			if !result.ready {
				killTree()
			}
			return result
		case <-timer.C:
			if result.ready {
				continue
			}
			killTree()
			<-done
			return bootRunResult{err: fmt.Errorf("runtime did not become ready")}
		case sig := <-signals:
			result.cancelled = true
			signalBootProcess(cmd, sig)
			// Cancellation is never a reason to replay this command on A.
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				killTree()
				<-done
			}
			result.code = 130
			if sig == syscall.SIGTERM {
				result.code = 143
			}
			return result
		}
	}
}

// LaunchBoot handles all invocations once a boot record exists. Only an actual
// history TUI can consume/confirm a candidate; --version, services and scripts
// always use the confirmed version and cannot accidentally promote a trial.
func launchBoot(root, entry string, args []string, s BootState) (int, bool, error) {
	interactive := historyTUIArgs(args[1:])
	recovering := false
	if interactive && AutoUpdateAllowed() && s.Candidate != nil && s.Candidate.Attempt == "" {
		if claim, ok := consumeBootCandidate(root, s); ok {
			recovering = true
			path, err := checkedRuntime(root, claim.Candidate.Version)
			if err == nil {
				r := superviseBoot(path, args, bootEnvironment(root, entry, claim.Candidate.Version), func() { confirmBootCandidate(root, claim) }, bootReadyTimeout)
				if r.ready || r.cancelled {
					return r.code, true, nil
				}
			}
			// No rollback write: the consumed record still names the old Good.
		}
	}
	path, err := checkedRuntime(root, s.Good)
	if err == nil {
		if !interactive || recovering {
			return launchRuntimeFn(path, append([]string{path}, args[1:]...), bootEnvironment(root, entry, s.Good))
		}
		r := superviseBoot(path, args, bootEnvironment(root, entry, s.Good), func() {}, bootReadyTimeout)
		if r.ready || r.cancelled {
			return r.code, true, nil
		}
	}
	if s.Previous != "" {
		if previous, err := checkedRuntime(root, s.Previous); err == nil {
			rejectBootGood(root, s)
			return launchRuntimeFn(previous, append([]string{previous}, args[1:]...), bootEnvironment(root, entry, s.Previous))
		}
	}
	// The stable entry contains the original full CLI. Returning unhandled runs
	// that copy directly, without dispatch, network, repair, or another retry.
	return 0, false, nil
}

func bootEnvironment(root, entry, version string) []string {
	return append(setRuntimeEnvironment(os.Environ(), root, entry, version), EnvBootManaged+"=1")
}

func historyTUIArgs(args []string) bool {
	return TUIInvocation(args)
}

// TUIInvocation is shared with the CLI routing regression test.
func TUIInvocation(args []string) bool {
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--help" || a == "-h" || a == "--version" || a == "-v" || strings.HasPrefix(a, "--upgrade-") {
			return false
		}
		if a == "--config" || a == "--profile" || a == "--codex-dir" || a == "--codex-path" || a == "--refresh-interval" {
			i++
			continue
		}
		if strings.HasPrefix(a, "-") {
			known := false
			for _, name := range []string{"--config=", "--profile=", "--codex-dir=", "--codex-path=", "--refresh-interval="} {
				known = known || strings.HasPrefix(a, name)
			}
			if !known {
				return false
			}
			continue
		}
		positional = append(positional, a)
	}
	if len(positional) == 0 {
		return true
	}
	if len(positional) == 2 {
		return positional[0] == "history" && positional[1] == "tui"
	}
	if len(positional) != 1 {
		return false
	}
	// Root accepts a profile name. Reserved commands must never try updates.
	// A CLI test checks this list against the actual Cobra command tree.
	switch positional[0] {
	case "app", "init", "delegate", "run", "teams", "proxy", "skills", "upgrade", "history", "selftest", "help", "completion", "__complete", "__completeNoDesc", "__internal-npm-wrapper", "__user-path-probe":
		return false
	}
	return true
}

func AutoUpdateAllowed() bool {
	setting := strings.ToLower(strings.TrimSpace(os.Getenv("CXP_AUTO_UPDATE")))
	pin := strings.TrimSpace(os.Getenv("CODEX_PROXY_VERSION"))
	return setting != "0" && setting != "off" && setting != "false" && (pin == "" || strings.EqualFold(pin, "latest"))
}
