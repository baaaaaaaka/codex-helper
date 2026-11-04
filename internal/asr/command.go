package asr

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultCommandTimeout bounds one external-command transcription.
const DefaultCommandTimeout = 15 * time.Minute

// Command is the external-command backend (--asr-command /
// CODEX_HELPER_TEAMS_ASR_COMMAND). The program receives the media path and
// prints either plain text or JSON {"text","language","model","backend"}.
//
// Args may use the placeholders {input} (= {path}), {prompt_path}, {name},
// {content_type}, {language}, {speed} and {threads}. With no Args the media
// path is the only argument.
type Command struct {
	Path    string
	Args    []string
	Env     []string      // extra KEY=VALUE entries
	Timeout time.Duration // default DefaultCommandTimeout
}

// NewCommand returns nil when path is empty, so callers can fall back to the
// managed backend.
func NewCommand(path string, args []string) *Command {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return &Command{Path: strings.TrimSpace(path), Args: append([]string(nil), args...)}
}

func (c *Command) Close() error { return nil }

func (c *Command) Transcribe(ctx context.Context, req Request) (Result, error) {
	const op = "run ASR command"
	if c == nil || c.Path == "" {
		return Result{}, newError(KindNotConfigured, op, nil, "")
	}
	if _, err := os.Stat(req.Path); err != nil {
		return Result{}, newError(KindMedia, "open voice file", err, "")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultCommandTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	replacer := strings.NewReplacer(
		"{input}", req.Path, "{path}", req.Path, "{prompt_path}", req.displayName(),
		"{name}", req.displayName(), "{content_type}", req.ContentType,
		"{language}", req.language(), "{speed}", "1.25x", "{threads}", strconv.Itoa(MaxThreads),
	)
	args := []string{req.Path}
	if len(c.Args) > 0 {
		args = make([]string, len(c.Args))
		for i, a := range c.Args {
			args[i] = replacer.Replace(a)
		}
	}
	cmd := exec.Command(c.Path, args...)
	cmd.Env = childEnv(append([]string{
		"CODEX_HELPER_TEAMS_ASR_LANGUAGE=" + req.language(),
		"CODEX_HELPER_TEAMS_ASR_SPEED=1.25x",
		"CODEX_HELPER_TEAMS_ASR_THREADS=" + strconv.Itoa(MaxThreads),
	}, c.Env...)...)
	stdout, stderr, err := run(ctx, cmd)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, ctxError(ctx, op)
		}
		return Result{}, classifyRun(op, err, stderr, KindNotConfigured)
	}
	res := Result{Language: req.language(), Backend: "command"}
	out := strings.TrimSpace(stdout)
	var decoded struct {
		Text, Language, Model, Backend string
	}
	if strings.HasPrefix(out, "{") && json.Unmarshal([]byte(out), &decoded) == nil {
		res.Text = strings.TrimSpace(decoded.Text)
		res.Language = firstNonEmpty(decoded.Language, res.Language)
		res.Model = decoded.Model
		res.Backend = firstNonEmpty(decoded.Backend, res.Backend)
	} else {
		res.Text = out
	}
	if res.Text == "" {
		return Result{}, newError(KindNoSpeech, op, nil, tailLines(stderr, 5))
	}
	return res, nil
}

// threadEnvKeys are the thread-pool knobs of common inference libraries.
var threadEnvKeys = []string{
	"OMP_NUM_THREADS", "OMP_THREAD_LIMIT", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
	"BLIS_NUM_THREADS", "GOTO_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS",
	"ONNX_NUM_THREADS", "ORT_NUM_THREADS", "RAYON_NUM_THREADS", "TORCH_NUM_THREADS", "GOMAXPROCS",
}

// childEnv is the helper's environment with every thread knob capped at
// MaxThreads, llama.cpp LLAMA_ARG_* / GGML_BACKEND_PATH overrides removed
// (they would silently change our server), and extra appended (later
// entries win).
func childEnv(extra ...string) []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "LLAMA_ARG_") && !strings.HasPrefix(kv, "GGML_BACKEND_PATH=") {
			env = append(env, kv)
		}
	}
	for _, key := range threadEnvKeys {
		n, err := strconv.Atoi(os.Getenv(key))
		if err != nil || n <= 0 || n > MaxThreads {
			n = MaxThreads
		}
		env = append(env, key+"="+strconv.Itoa(n))
	}
	env = append(env, "TOKENIZERS_PARALLELISM=false")
	return append(env, extra...)
}

// run executes cmd to completion, killing its whole process tree when ctx
// ends. Output is captured with bounds (stdout 4 MiB, stderr last 16 KiB).
func run(ctx context.Context, cmd *exec.Cmd) (string, string, error) {
	stdout := &boundedBuffer{max: 4 << 20}
	stderr := &boundedBuffer{max: 16 << 10, keepTail: true}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	p, err := startProc(cmd)
	if err != nil {
		return "", "", err
	}
	defer p.release()
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err = <-done:
	case <-ctx.Done():
		p.kill()
		err = <-done
	}
	return stdout.String(), stderr.String(), err
}

// boundedBuffer keeps at most max bytes: the head, or the tail if keepTail.
type boundedBuffer struct {
	mu       sync.Mutex
	buf      []byte
	max      int
	keepTail bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		if b.keepTail {
			b.buf = append(b.buf[:0], b.buf[len(b.buf)-b.max:]...)
		} else {
			b.buf = b.buf[:b.max]
		}
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
