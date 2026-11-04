package asr

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const opLoadModel = "load voice model"

// Messages llama-server prints when a model file is unusable (corrupt or
// truncated), as opposed to the binary itself failing to run.
// Projector markers come first: a bad projector must not quarantine the model.
var modelLoadMarkers = []string{
	"failed to load multimodal model", "failed to load mmproj",
	"failed to load model", "error loading model", "invalid magic characters",
}

// llamaServer is one resident llama-server bound to 127.0.0.1 with a
// random API key, so other local users cannot use it.
type llamaServer struct {
	proc    *proc
	url     string
	key     string
	keyFile string
	record  string // orphan record (see orphans.go), removed on stop
	stderr  *boundedBuffer
	exited  chan struct{}
	client  *http.Client
}

// serverArgs sizes the server for one voice message at a time: MaxThreads
// threads, a 2048-token context (60 s of sped-up audio is ~780 audio
// tokens), one slot, two HTTP threads (not one per core), no web UI, no
// warm-up.
func serverArgs(p *paths, device, keyFile string, port int) []string {
	args := []string{
		"-m", p.model, "--mmproj", p.mmproj,
		"--host", "127.0.0.1", "--port", strconv.Itoa(port), "--api-key-file", keyFile,
		"-t", strconv.Itoa(MaxThreads), "-tb", strconv.Itoa(MaxThreads),
		"-c", strconv.Itoa(contextTokens), "-np", "1", "--threads-http", "2", "--no-webui", "--no-warmup",
	}
	if device == "cpu" {
		args = append(args, "--device", "none", "--no-mmproj-offload")
	}
	return args
}

// startLlamaServer starts the server and waits until it is ready. While it
// runs it is recorded under root/run so a later helper can stop it if this
// one dies without doing so.
func startLlamaServer(ctx context.Context, p *paths, device, root, tmp string, loadTimeout time.Duration) (*llamaServer, error) {
	for attempt := 0; ; attempt++ {
		s, err := tryStartServer(ctx, p, device, root, tmp, loadTimeout)
		if err == nil || attempt > 0 || !strings.Contains(strings.ToLower(err.Error()), "bind") {
			return s, err
		}
		// Another process took the port between probe and bind: retry once.
	}
}

func tryStartServer(ctx context.Context, p *paths, device, root, tmp string, loadTimeout time.Duration) (*llamaServer, error) {
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return nil, newError(KindCache, "create temp dir", err, "")
	}
	port, err := freePort()
	if err != nil {
		return nil, newError(KindBackend, "pick local port", err, "")
	}
	key := randomHex(24)
	keyFile := filepath.Join(tmp, "key-"+randomHex(6))
	if err := writeFile(keyFile, strings.NewReader(key), 0o600); err != nil {
		return nil, newError(KindCache, "write server key", err, "")
	}
	cmd := exec.Command(p.server, serverArgs(p, device, keyFile, port)...)
	cmd.Dir = filepath.Dir(p.server)
	cmd.Env = childEnv(gpuPluginEnv(p.server, device)...)
	s := &llamaServer{
		url: "http://127.0.0.1:" + strconv.Itoa(port), key: key, keyFile: keyFile,
		stderr: &boundedBuffer{max: 32 << 10, keepTail: true}, exited: make(chan struct{}),
		client: &http.Client{},
	}
	cmd.Stdout, cmd.Stderr = s.stderr, s.stderr
	if s.proc, err = startProc(cmd); err != nil {
		_ = os.Remove(keyFile)
		return nil, classifyRun("start llama-server", err, "", KindCache)
	}
	s.record = recordServer(root, s.proc.cmd.Process.Pid)
	var waitErr error
	go func() { waitErr = s.proc.cmd.Wait(); close(s.exited) }()

	ctx, cancel := context.WithTimeout(ctx, loadTimeout)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-s.exited:
			s.stop()
			return nil, classifyServerExit(waitErr, s.stderr.String())
		case <-ctx.Done():
			s.stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, newError(KindTimeout, opLoadModel, ctx.Err(),
					"llama-server was not ready after "+loadTimeout.String()+"\n"+tailLines(s.stderr.String(), 6))
			}
			return nil, ctxError(ctx, opLoadModel)
		case <-tick.C:
			if s.healthy(ctx) {
				return s, nil
			}
		}
	}
}

// gpuPluginEnv loads the optional GPU backend plugin, which the runtime
// archives keep in gpu/ so that the default CPU mode never loads (or
// enumerates) a GPU driver. Without a plugin, "gpu" runs on the CPU, and on
// macOS Metal is built into the runtime itself.
func gpuPluginEnv(server, device string) []string {
	if device != "gpu" {
		return nil
	}
	plugins, _ := filepath.Glob(filepath.Join(filepath.Dir(server), "gpu", "*ggml-vulkan*"))
	if len(plugins) == 0 {
		return nil
	}
	return []string{"GGML_BACKEND_PATH=" + plugins[0]}
}

func classifyServerExit(err error, stderr string) error {
	if err == nil {
		err = errors.New("llama-server exited")
	}
	e := classifyRun("start llama-server", err, stderr, KindCache)
	if e.Kind != KindBackend {
		return e
	}
	lower := strings.ToLower(stderr)
	for _, m := range modelLoadMarkers {
		if strings.Contains(lower, m) {
			e.Kind, e.Op = KindIntegrity, opLoadModel
			if strings.Contains(m, "multimodal") || strings.Contains(m, "mmproj") {
				e.Detail = "multimodal projector: " + e.Detail
			}
			return e
		}
	}
	return e
}

func (s *llamaServer) healthy(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url+"/health", nil)
	if err != nil {
		return false
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// exitedWithin reports whether the server process ends within d (a failed
// request may be seen before the crash is).
func (s *llamaServer) exitedWithin(d time.Duration) bool {
	select {
	case <-s.exited:
		return true
	case <-time.After(d):
		return false
	}
}

func (s *llamaServer) alive() bool {
	select {
	case <-s.exited:
		return false
	default:
		return true
	}
}

func (s *llamaServer) stop() {
	if s.alive() {
		s.proc.kill()
		select {
		case <-s.exited:
		case <-time.After(10 * time.Second):
		}
	}
	s.proc.release()
	_ = os.Remove(s.keyFile)
	if s.record != "" {
		_ = os.Remove(s.record)
	}
}

// transcribe sends one WAV segment and returns (text, language).
func (s *llamaServer) transcribe(ctx context.Context, wav []byte) (string, string, error) {
	body, _ := json.Marshal(map[string]any{
		"messages": []any{map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "input_audio", "input_audio": map[string]string{
					"data": base64.StdEncoding.EncodeToString(wav), "format": "wav"}},
				map[string]any{"type": "text", "text": transcribeMsg},
			},
		}},
		"temperature": 0, "max_tokens": maxTokens, "cache_prompt": false,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.key)
	resp, err := s.client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("llama-server HTTP %d: %s", resp.StatusCode, shorten(string(data), 300))
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &out); err != nil || len(out.Choices) == 0 {
		return "", "", fmt.Errorf("llama-server returned an unexpected response: %s", shorten(string(data), 300))
	}
	text, lang := parseASR(out.Choices[0].Message.Content)
	return text, lang, nil
}

// parseASR splits Qwen3-ASR output "language English<asr_text>hello" into
// ("hello", "English"). Output without the marker is taken as plain text.
func parseASR(s string) (string, string) {
	head, text, ok := strings.Cut(s, "<asr_text>")
	if !ok {
		return strings.TrimSpace(s), ""
	}
	text = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text), "</asr_text>"))
	lang := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(head), "language"))
	if strings.EqualFold(lang, "none") {
		lang = ""
	}
	return text, lang
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
