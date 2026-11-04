package asr

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The test binary doubles as the fake native programs: copied (inside the
// fake assets) as llama-server / ffmpeg / fake-asr-cmd, it behaves like
// them according to ASR_FAKE_* variables (inherited through childEnv).
func TestMain(m *testing.M) {
	switch strings.TrimSuffix(filepath.Base(os.Args[0]), ".exe") {
	case "llama-server":
		os.Exit(fakeServer(os.Args[1:]))
	case "ffmpeg":
		os.Exit(fakeFFmpeg(os.Args[1:]))
	case "fake-asr-cmd":
		os.Exit(fakeCommand(os.Args[1:]))
	case "fake-helper":
		os.Exit(fakeHelper())
	}
	os.Exit(m.Run())
}

func fakeLog(format string, args ...any) {
	if p := os.Getenv("ASR_FAKE_LOG"); p != "" {
		f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err == nil {
			fmt.Fprintf(f, format+"\n", args...)
			f.Close()
		}
	}
}

func fakeServer(args []string) int {
	mode := os.Getenv("ASR_FAKE_SERVER")
	if mode == "glibc" {
		fmt.Fprintln(os.Stderr, "./llama-server: /lib64/libc.so.6: version `GLIBC_2.28' not found (required by ./libggml-base.so)")
		return 1
	}
	if code, err := strconv.ParseUint(os.Getenv("ASR_FAKE_EXIT"), 0, 32); err == nil {
		os.Exit(int(code))
	}
	if len(args) == 1 && args[0] == "--version" {
		fakeLog("version")
		fmt.Fprintln(os.Stderr, "version: 1 (fake)")
		return 0
	}
	fakeLog("serve %s", strings.Join(args, " "))
	flags := map[string]string{}
	for i := 0; i+1 < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			flags[args[i]] = args[i+1]
		}
	}
	if mode == "illegal-serve" {
		fmt.Fprintln(os.Stderr, "load_backend: loaded CPU backend\nIllegal instruction (core dumped)")
		return 132
	}
	if b, _ := os.ReadFile(flags["--mmproj"]); !bytes.HasPrefix(b, []byte("GGUF")) {
		fmt.Fprintln(os.Stderr, "clip_init: failed to load model: invalid magic\nsrv load_model: failed to load multimodal model, '"+flags["--mmproj"]+"'")
		return 1
	}
	if b, _ := os.ReadFile(flags["-m"]); !bytes.HasPrefix(b, []byte("GGUF")) {
		fmt.Fprintln(os.Stderr, "gguf_init_from_file_impl: invalid magic characters: 'BAD!'\nllama_model_load: error loading model: llama_model_loader: failed to load model")
		return 1
	}
	key, _ := os.ReadFile(flags["--api-key-file"])
	l, err := net.Listen("tcp", "127.0.0.1:"+flags["--port"])
	if err != nil {
		fmt.Fprintln(os.Stderr, "couldn't bind HTTP server socket:", err)
		return 1
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"status":"ok"}`)) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+string(key) {
			http.Error(w, "invalid api key", http.StatusUnauthorized)
			return
		}
		if mode == "crash-once" {
			marker := filepath.Join(os.Getenv("ASR_FAKE_DIR"), "crashed")
			if _, err := os.Stat(marker); err != nil {
				os.WriteFile(marker, nil, 0o600)
				os.Exit(3)
			}
		}
		var req struct {
			Messages []struct {
				Content []struct {
					Type       string                        `json:"type"`
					Text       string                        `json:"text"`
					InputAudio struct{ Data, Format string } `json:"input_audio"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Messages) != 1 || len(req.Messages[0].Content) != 2 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		wav, _ := base64.StdEncoding.DecodeString(req.Messages[0].Content[0].InputAudio.Data)
		text := ""
		if len(wav) > 44 {
			text = string(bytes.TrimRight(wav[44:], "\x00"))
		}
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{
			"message": map[string]string{"content": "language English<asr_text>" + text}}}})
	})
	http.Serve(l, mux)
	return 0
}

func fakeFFmpeg(args []string) int {
	joined := strings.Join(args, " ")
	switch {
	case joined == "-version":
		fakeLog("ffmpeg-version")
		fmt.Println("ffmpeg version fake")
		return 0
	case strings.HasSuffix(joined, "-filters"):
		fmt.Println(" ... atempo  A->A  Adjust audio tempo.\n ... aresample A->A Resample audio data.")
		return 0
	}
	fakeLog("ffmpeg %s", joined)
	input := ""
	for i, a := range args {
		if a == "-i" && i+1 < len(args) {
			input = args[i+1]
		}
	}
	data, err := os.ReadFile(input)
	if err != nil {
		fmt.Fprintln(os.Stderr, input+": No such file or directory")
		return 1
	}
	if bytes.HasPrefix(data, []byte("NOAUDIO")) {
		fmt.Fprintln(os.Stderr, "Stream map '0:a:0' matches no streams.")
		return 1
	}
	pattern := args[len(args)-1]
	if os.Getenv("ASR_FAKE_FFMPEG") == "copy" { // real-model test: input is already 16 kHz WAV
		os.WriteFile(fmt.Sprintf(pattern, 0), data, 0o600)
		return 0
	}
	n, _ := strconv.Atoi(os.Getenv("ASR_FAKE_SEGMENTS"))
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		seg := make([]byte, 4000)
		copy(seg[44:], fmt.Sprintf("%s part %d", strings.TrimSpace(string(data)), i+1))
		os.WriteFile(fmt.Sprintf(pattern, i), seg, 0o600)
	}
	os.WriteFile(fmt.Sprintf(pattern, n), make([]byte, 100), 0o600) // a sliver, must be skipped
	return 0
}

func fakeCommand(args []string) int {
	mode := os.Getenv("ASR_FAKE_CMD")
	switch {
	case mode == "json":
		json.NewEncoder(os.Stdout).Encode(map[string]string{"text": strings.Join(args, " "), "language": "zh", "model": "m", "backend": "b"})
	case mode == "env":
		var parts []string
		for _, k := range []string{"OMP_NUM_THREADS", "GOMAXPROCS", "CODEX_HELPER_TEAMS_ASR_THREADS", "LLAMA_ARG_THREADS", "EXTRA"} {
			parts = append(parts, k+"="+os.Getenv(k))
		}
		fmt.Println(strings.Join(parts, ";"))
	case mode == "sleep":
		time.Sleep(2 * time.Minute)
	case mode == "empty":
		fmt.Fprintln(os.Stderr, "no speech found")
	case strings.HasPrefix(mode, "exit:"):
		code, _ := strconv.ParseUint(strings.TrimPrefix(mode, "exit:"), 0, 32)
		fmt.Fprintln(os.Stderr, "failing on purpose")
		return int(code)
	default:
		fmt.Println(strings.Join(args, " "))
	}
	return 0
}

// ---- fake asset server ----

type assetServer struct {
	*httptest.Server
	mu      sync.Mutex
	files   map[string][]byte
	hits    map[string]int
	missing map[string]bool          // 404
	down    map[string]bool          // 503
	corrupt map[string]bool          // flip one byte
	hold    map[string]chan struct{} // send half, then wait for the channel (or the client to go away)
	stall   map[string]int           // the first N requests send half, then hang
	trickle map[string]time.Duration // send in 20 chunks with this pause between them
}

func newAssetServer(t *testing.T) *assetServer {
	s := &assetServer{files: map[string][]byte{}, hits: map[string]int{}, missing: map[string]bool{}, down: map[string]bool{},
		corrupt: map[string]bool{}, hold: map[string]chan struct{}{}, stall: map[string]int{},
		trickle: map[string]time.Duration{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(func() {
		s.Server.CloseClientConnections() // ends held/stalled handlers
		s.Server.Close()
	})
	return s
}

func (s *assetServer) serve(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	s.mu.Lock()
	s.hits[name]++
	data, ok := s.files[name]
	missing, corrupt, hold, trickle := s.missing[name], s.corrupt[name], s.hold[name], s.trickle[name]
	stall := s.stall[name] > 0
	if stall {
		s.stall[name]--
	}
	s.mu.Unlock()
	if !ok || missing {
		http.NotFound(w, r)
		return
	}
	if s.isDown(name) {
		http.Error(w, "mirror down", http.StatusServiceUnavailable)
		return
	}
	if corrupt {
		data = append([]byte(nil), data...)
		data[len(data)/2] ^= 0xff
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if trickle > 0 {
		for rest, step := data, len(data)/20+1; len(rest) > 0; {
			n := min(step, len(rest))
			if _, err := w.Write(rest[:n]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			rest = rest[n:]
			select {
			case <-time.After(trickle):
			case <-r.Context().Done():
				return
			}
		}
		return
	}
	if hold != nil || stall {
		w.Write(data[:len(data)/2])
		w.(http.Flusher).Flush()
		select {
		case <-hold: // nil when stalling: wait for the client to give up
		case <-r.Context().Done():
			return
		}
		w.Write(data[len(data)/2:])
		return
	}
	w.Write(data)
}

func (s *assetServer) isDown(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.down[name]
}

func (s *assetServer) hitCount(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[name]
}

func (s *assetServer) set(f func(s *assetServer)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

func (s *assetServer) add(name, format string, data []byte) Asset {
	s.mu.Lock()
	s.files[name] = data
	s.mu.Unlock()
	sum := sha256.Sum256(data)
	a := Asset{Name: name, URL: s.URL + "/" + name, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), Format: format}
	if format == FormatWheel {
		a.Member = "imageio_ffmpeg/binaries/ffmpeg"
	}
	return a
}

// ---- fake assets ----

var (
	selfOnce  sync.Once
	selfBytes []byte
)

func self(t *testing.T) []byte {
	selfOnce.Do(func() {
		exe, err := os.Executable()
		if err == nil {
			selfBytes, err = os.ReadFile(exe)
		}
		if err != nil {
			t.Fatal(err)
		}
	})
	return selfBytes
}

func tarGz(t *testing.T, files map[string][]byte) []byte {
	var buf bytes.Buffer
	gz, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	tw := tar.NewWriter(gz)
	tw.WriteHeader(&tar.Header{Name: "llama-asr-test/", Typeflag: tar.TypeDir, Mode: 0o755})
	for name, data := range files {
		if err := tw.WriteHeader(&tar.Header{Name: "llama-asr-test/" + name, Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		tw.Write(data)
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

func zipFiles(t *testing.T, files map[string][]byte) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range files {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
		if err != nil {
			t.Fatal(err)
		}
		w.Write(data)
	}
	zw.Close()
	return buf.Bytes()
}

type fixture struct {
	srv      *assetServer
	manifest Manifest
	cache    string
	log      string
	dir      string
}

// runtimeFiles is the fake runtime archive content: llama-server plus a library.
func runtimeFiles(server []byte) map[string][]byte {
	return map[string][]byte{exeName("llama-server"): server, "libggml-base.so": []byte("not really a library")}
}

var (
	defaultAssetsOnce             sync.Once
	defaultRuntime, defaultFFmpeg []byte
)

func newFixture(t *testing.T, server []byte) *fixture {
	t.Helper()
	ffName := "imageio_ffmpeg/binaries/ffmpeg-" + runtime.GOOS + "-v7.0.2"
	if runtime.GOOS == "windows" {
		ffName += ".exe"
	}
	defaultAssetsOnce.Do(func() {
		defaultRuntime = tarGz(t, runtimeFiles(self(t)))
		defaultFFmpeg = zipFiles(t, map[string][]byte{
			"imageio_ffmpeg/__init__.py": []byte(""), "imageio_ffmpeg/binaries/README.md": []byte("readme"), ffName: self(t)})
	})
	runtimeArchive := defaultRuntime
	if server != nil {
		runtimeArchive = tarGz(t, runtimeFiles(server))
	}
	f := &fixture{srv: newAssetServer(t), dir: t.TempDir()}
	f.cache = filepath.Join(f.dir, "cache")
	f.log = filepath.Join(f.dir, "fake.log")
	t.Setenv("ASR_FAKE_LOG", f.log)
	t.Setenv("ASR_FAKE_DIR", f.dir)
	t.Setenv("ASR_FAKE_SERVER", "")
	t.Setenv("ASR_FAKE_SEGMENTS", "")
	f.manifest = Manifest{
		ModelID: "test",
		Runtime: f.srv.add("llama-asr-test.tar.gz", FormatTarGz, runtimeArchive),
		Model:   f.srv.add("model.gguf", FormatFile, append([]byte("GGUF"), bytes.Repeat([]byte("m"), 4000)...)),
		MMProj:  f.srv.add("mmproj.gguf", FormatFile, append([]byte("GGUF"), bytes.Repeat([]byte("p"), 3000)...)),
		FFmpeg:  f.srv.add("imageio_ffmpeg-test.whl", FormatWheel, defaultFFmpeg),
	}
	return f
}

func (f *fixture) totalBytes() int64 {
	m := f.manifest
	return m.Runtime.Size + m.Model.Size + m.MMProj.Size + m.FFmpeg.Size
}

func (f *fixture) managed(t *testing.T, mutate ...func(*ManagedConfig)) *Managed {
	t.Helper()
	mf := f.manifest
	cfg := ManagedConfig{CacheDir: f.cache, Manifest: &mf, LoadTimeout: 30 * time.Second, StallTimeout: 10 * time.Second}
	for _, fn := range mutate {
		fn(&cfg)
	}
	m := NewManaged(cfg)
	m.in.backoff = 10 * time.Millisecond
	t.Cleanup(func() { m.Close() })
	return m
}

func (f *fixture) logCount(prefix string) int {
	data, _ := os.ReadFile(f.log)
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}
	return n
}

func (f *fixture) hits() map[string]int {
	f.srv.mu.Lock()
	defer f.srv.mu.Unlock()
	out := map[string]int{}
	for k, v := range f.srv.hits {
		out[k] = v
	}
	return out
}

func (f *fixture) installDir(a Asset) string { return filepath.Join(f.cache, a.dirName()) }

func voiceFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// recorder collects events from Request.Notify.
type recorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *recorder) notify(e Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func transcribe(t *testing.T, tr Transcriber, path string, rec *recorder, timeout time.Duration) (Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req := Request{Path: path}
	if rec != nil {
		req.Notify = rec.notify
	}
	return tr.Transcribe(ctx, req)
}

func mustKind(t *testing.T, err error, want Kind) *Error {
	t.Helper()
	if KindOf(err) != want {
		t.Fatalf("error kind = %q, want %q (err: %v)", KindOf(err), want, err)
	}
	e, _ := err.(*Error)
	return e
}

func garbage(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	copy(b, "\x00\x01garbage") // never a valid ELF/Mach-O/PE header or a #! script
	return b
}

func tmpEntries(t *testing.T, cache string, prefixes ...string) []string {
	t.Helper()
	entries, _ := os.ReadDir(filepath.Join(cache, "tmp"))
	var out []string
	for _, e := range entries {
		for _, p := range prefixes {
			if strings.HasPrefix(e.Name(), p) {
				out = append(out, e.Name())
			}
		}
	}
	return out
}
