package asr

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os/exec"
	"strings"
	"syscall"
)

// Kind is a stable failure class. The core maps each kind to one user-visible
// message and keeps the voice file in every case.
type Kind string

const (
	KindNotConfigured Kind = "not_configured"       // no backend, or the configured command does not exist
	KindUnsupported   Kind = "unsupported_platform" // no managed runtime for this OS/CPU
	KindPreparing     Kind = "preparing"            // first-run setup still running (retry later)
	KindDownload      Kind = "download"             // network or HTTP failure while downloading
	KindIntegrity     Kind = "integrity"            // downloaded bytes do not match the pin
	KindDiskSpace     Kind = "disk_space"           // not enough free space for setup
	KindCache         Kind = "cache"                // cache not writable / not executable / cannot publish
	KindNativeLoad    Kind = "native_load"          // binary cannot load here (glibc, missing DLL, wrong arch)
	KindMedia         Kind = "media"                // input cannot be decoded or has no audio
	KindNoSpeech      Kind = "no_speech"            // decoded fine but nothing was recognised
	KindBackend       Kind = "backend"              // the recogniser failed or crashed
	KindTimeout       Kind = "timeout"              // setup or transcription exceeded its bound
	KindCanceled      Kind = "canceled"             // caller canceled (e.g. shutdown)
)

// Error is the only error type the package returns.
type Error struct {
	Kind   Kind
	Op     string // what was being done, e.g. "download model"
	Detail string // short diagnostic, e.g. a stderr tail or HTTP status
	Err    error
}

func (e *Error) Error() string {
	parts := []string{"asr " + string(e.Kind)}
	for _, p := range []string{e.Op, errString(e.Err), e.Detail} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ": ")
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

func newError(kind Kind, op string, err error, detail string) *Error {
	return &Error{Kind: kind, Op: op, Err: err, Detail: shorten(detail, 600)}
}

// KindOf returns the kind of err ("" for nil, KindBackend for foreign errors).
func KindOf(err error) Kind {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindBackend
}

// Retryable reports whether the same voice file should be tried again later
// (keep the row pending) rather than marked failed.
func Retryable(err error) bool {
	switch KindOf(err) {
	case KindPreparing, KindDownload, KindCanceled:
		return true
	}
	return false
}

var userMessages = map[Kind][2]string{
	KindNotConfigured: {"voice transcription is not set up on this helper.", "update or restart the helper; if you configured --asr-command, check that the command exists."},
	KindUnsupported:   {"local voice transcription is not available for this operating system or CPU.", "configure an external transcriber with --asr-command, or use a supported machine."},
	KindPreparing:     {"voice recognition is still being prepared (first-time download).", "nothing to do; the message will be transcribed when setup finishes."},
	KindDownload:      {"downloading the voice recognition files failed.", "check this machine's access to github.com and huggingface.co; the helper will retry."},
	KindIntegrity:     {"a downloaded voice recognition file did not match its pinned checksum and was discarded.", "retry later; if it keeps failing, update the helper."},
	KindDiskSpace:     {"there is not enough free disk space to install voice recognition.", "free space in the cache directory (about 2 GB is needed) and resend the message."},
	KindCache:         {"the voice recognition cache directory is not usable (not writable, not executable, or locked).", "check permissions and noexec mounts of the cache directory, or point CODEX_HELPER_ASR_CACHE_DIR at a local disk."},
	KindNativeLoad:    {"the voice recognition program cannot run on this machine (missing system library or incompatible CPU/OS).", "update the helper; if it persists, report the diagnostic below or configure --asr-command."},
	KindMedia:         {"the voice/video file could not be decoded or contains no audio.", "resend the recording, or type the message."},
	KindNoSpeech:      {"no speech was recognised in the recording.", "resend the recording closer to the microphone, or type the message."},
	KindBackend:       {"voice recognition failed.", "resend the message; if it keeps failing, check the diagnostic below."},
	KindTimeout:       {"voice recognition took too long and was stopped.", "resend a shorter recording or try again later."},
	KindCanceled:      {"voice recognition was interrupted.", "it will be retried automatically."},
}

// UserMessage renders err as one short message: what happened, what to do,
// and a diagnostic line when there is one.
func UserMessage(err error) string {
	if err == nil {
		return ""
	}
	kind := KindOf(err)
	msg, ok := userMessages[kind]
	if !ok {
		msg = userMessages[KindBackend]
	}
	out := "Voice transcription failed: " + msg[0] + " Next: " + msg[1]
	if kind == KindPreparing {
		out = "Voice transcription pending: " + msg[0] + " " + msg[1]
	}
	var e *Error
	if errors.As(err, &e) && kind != KindPreparing {
		if diag := firstNonEmpty(e.Detail, errString(e.Err)); diag != "" {
			out += "\nDiagnostic: " + shorten(diag, 400)
		}
	}
	return out
}

// ctxError converts a context failure into Timeout or Canceled.
func ctxError(ctx context.Context, op string) *Error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return newError(KindTimeout, op, ctx.Err(), "")
	}
	return newError(KindCanceled, op, ctx.Err(), "")
}

// Windows NTSTATUS exit codes that mean "the image could not be loaded".
var windowsLoadFailures = map[uint32]string{
	0xC0000135: "STATUS_DLL_NOT_FOUND (a required DLL such as the VC++ runtime is missing)",
	0xC0000139: "STATUS_ENTRYPOINT_NOT_FOUND",
	0xC000007B: "STATUS_INVALID_IMAGE_FORMAT",
	0xC000001D: "STATUS_ILLEGAL_INSTRUCTION (CPU lacks a required instruction set)",
	0xC0000142: "STATUS_DLL_INIT_FAILED",
}

// Loader messages that mean the binary is incompatible with this host.
var nativeLoadMarkers = []string{
	"error while loading shared libraries",
	"version `glibc_", "version `glibcxx_", "version `cxxabi_",
	"not found (required by",
	"cannot open shared object file",
	"exec format error", "bad cpu type", "not a valid win32 application",
	"dyld: library not loaded", "dyld[", "symbol not found",
	"illegal instruction",
}

// classifyRun turns a failed process start/exit into an *Error. missing is
// the kind to use when the executable does not exist.
func classifyRun(op string, err error, stderr string, missing Kind) *Error {
	tail := tailLines(stderr, 12)
	lower := strings.ToLower(err.Error() + "\n" + stderr)
	var exitErr *exec.ExitError
	switch {
	case errors.Is(err, exec.ErrNotFound) || errors.Is(err, fs.ErrNotExist):
		return newError(missing, op, err, tail)
	case errors.Is(err, syscall.ENOEXEC):
		return newError(KindNativeLoad, op, err, tail)
	case errors.Is(err, fs.ErrPermission):
		return newError(KindCache, op, err, "the cache may be on a noexec mount")
	case errors.As(err, &exitErr):
		if desc, ok := windowsLoadFailures[uint32(exitErr.ExitCode())]; ok {
			return newError(KindNativeLoad, op, err, firstNonEmpty(tail, desc))
		}
		if signaled(exitErr, syscall.SIGILL) {
			return newError(KindNativeLoad, op, err, firstNonEmpty(tail, "illegal instruction: CPU lacks a required instruction set"))
		}
	}
	for _, marker := range nativeLoadMarkers {
		if strings.Contains(lower, marker) {
			return newError(KindNativeLoad, op, err, tail)
		}
	}
	return newError(KindBackend, op, err, tail)
}

func signaled(e *exec.ExitError, sig syscall.Signal) bool {
	ws, ok := e.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == sig
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func shorten(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("… (%d bytes cut)", len(s)-max)
}
