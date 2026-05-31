package teams

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestManagedQwenASRTranscriberInvokesIsolatedRuntimeWithDefaults(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("PYTHONHOME", "/bad-python-home")
	t.Setenv("PYTHONPATH", "/bad-python-path")
	t.Setenv("VIRTUAL_ENV", "/bad-venv")
	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{
		CacheRoot:    cacheRoot,
		ModelID:      DefaultManagedASRModelID,
		MinFreeBytes: 1,
		Backend:      managedASRBackendTransformers,
	})
	transcriber.ensureRuntime = func(_ context.Context, cfg ManagedASRConfig) (managedASRRuntime, error) {
		if cfg.CacheRoot != cacheRoot || cfg.ModelID != DefaultManagedASRModelID || cfg.MinFreeBytes != 1 {
			t.Fatalf("managed ASR config = %#v", cfg)
		}
		return managedASRRuntime{
			Python:     "/opt/cxp-asr/python",
			ScriptPath: filepath.Join(cacheRoot, "scripts", managedASRRunnerScriptFileName),
			CacheRoot:  cacheRoot,
			ModelID:    cfg.ModelID,
			Env:        []string{"CUSTOM_ASR_ENV=1"},
		}, nil
	}
	var gotCommand string
	var gotArgs []string
	var gotEnv []string
	transcriber.runCommand = func(_ context.Context, command string, args []string, env []string, stdout *bytes.Buffer, _ *bytes.Buffer) error {
		gotCommand = command
		gotArgs = append([]string(nil), args...)
		gotEnv = append([]string(nil), env...)
		_, _ = stdout.WriteString(`{"text":"测试 mixed English","language":"zh"}`)
		return nil
	}

	transcript, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		SourceIndex: 2,
		File:        LocalAttachment{Path: "/tmp/input.f4a", PromptPath: ".codex-helper/teams-attachments/input.f4a", ContentType: "audio/mp4"},
		Language:    defaultASRLanguage,
		Speed:       defaultASRSpeed,
	})
	if err != nil {
		t.Fatalf("TranscribeTeamsMedia error: %v", err)
	}
	if transcript.Text != "测试 mixed English" || transcript.Language != "zh" || transcript.Model != DefaultManagedASRModelID || transcript.Backend != managedASRBackendTransformers {
		t.Fatalf("transcript = %#v", transcript)
	}
	if gotCommand != "/opt/cxp-asr/python" {
		t.Fatalf("command = %q", gotCommand)
	}
	wantArgs := []string{
		filepath.Join(cacheRoot, "scripts", managedASRRunnerScriptFileName),
		"--input", "/tmp/input.f4a",
		"--language", "auto",
		"--speed", "1.25x",
		"--threads", "4",
		"--model", DefaultManagedASRModelID,
	}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("args = %#v, want %#v", gotArgs, wantArgs)
	}
	env := envSliceToMap(gotEnv)
	for _, key := range []string{"GOMAXPROCS", "OMP_NUM_THREADS", "TORCH_NUM_THREADS", "CODEX_HELPER_TEAMS_ASR_THREADS"} {
		if env[key] != "4" {
			t.Fatalf("%s = %q, want 4 in env %#v", key, env[key], env)
		}
	}
	if env["HF_HOME"] != filepath.Join(cacheRoot, "huggingface") ||
		env["CODEX_HELPER_TEAMS_ASR_TMP"] != filepath.Join(cacheRoot, "tmp") ||
		env["TMPDIR"] != filepath.Join(cacheRoot, "tmp") ||
		env["PYTHONIOENCODING"] != "utf-8" ||
		env["PYTHONNOUSERSITE"] != "1" ||
		env["CUSTOM_ASR_ENV"] != "1" {
		t.Fatalf("managed cache/env not isolated: %#v", env)
	}
	for _, key := range []string{"PYTHONHOME", "PYTHONPATH", "VIRTUAL_ENV"} {
		if _, ok := env[key]; ok {
			t.Fatalf("managed ASR env leaked %s: %#v", key, env)
		}
	}
}

func TestManagedQwenASRTranscriberReportsRuntimeFailureWithStderr(t *testing.T) {
	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{CacheRoot: t.TempDir(), Backend: managedASRBackendTransformers})
	transcriber.ensureRuntime = func(_ context.Context, cfg ManagedASRConfig) (managedASRRuntime, error) {
		return managedASRRuntime{Python: "python", ScriptPath: "runner.py", CacheRoot: cfg.CacheRoot, ModelID: DefaultManagedASRModelID}, nil
	}
	transcriber.runCommand = func(_ context.Context, _ string, _ []string, _ []string, _ *bytes.Buffer, stderr *bytes.Buffer) error {
		_, _ = stderr.WriteString("No space left on device while downloading model")
		return errors.New("exit status 1")
	}

	_, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		File:     LocalAttachment{Path: "/tmp/input.f4a", ContentType: "audio/mp4"},
		Language: defaultASRLanguage,
		Speed:    defaultASRSpeed,
	})
	if err == nil || !strings.Contains(err.Error(), "No space left on device") {
		t.Fatalf("managed ASR error = %v, want stderr detail", err)
	}
}

func TestManagedQwenASRTranscriberLlamaOptInInvokesConfiguredRuntime(t *testing.T) {
	cacheRoot := t.TempDir()
	binary := writeManagedASRTestFile(t, cacheRoot, "bin/llama-mtmd-cli", 0o700)
	model := writeManagedASRTestFile(t, cacheRoot, "models/qwen.gguf", 0o600)
	mmproj := writeManagedASRTestFile(t, cacheRoot, "models/mmproj.gguf", 0o600)
	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{
		CacheRoot:       cacheRoot,
		Backend:         managedASRBackendLlama,
		LlamaBinaryPath: binary,
		LlamaModelPath:  model,
		LlamaMMProjPath: mmproj,
		LlamaDevice:     "cpu",
		MinFreeBytes:    1,
	})
	var gotCommand string
	var gotArgs []string
	var gotEnv []string
	transcriber.runLlamaCommand = func(_ context.Context, command string, args []string, env []string, stdout *bytes.Buffer, _ *bytes.Buffer) error {
		gotCommand = command
		gotArgs = append([]string(nil), args...)
		gotEnv = append([]string(nil), env...)
		_, _ = stdout.WriteString("language Chinese<asr_text>测试 mixed English")
		return nil
	}
	transcriber.ensureRuntime = func(context.Context, ManagedASRConfig) (managedASRRuntime, error) {
		t.Fatal("llama backend should not prepare the transformers runtime")
		return managedASRRuntime{}, nil
	}

	transcript, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		SourceIndex: 4,
		File:        LocalAttachment{Path: "/tmp/input.wav", PromptPath: ".codex-helper/input.wav", ContentType: "audio/wav"},
		Language:    defaultASRLanguage,
		Speed:       "1x",
	})
	if err != nil {
		t.Fatalf("TranscribeTeamsMedia error: %v", err)
	}
	if transcript.Text != "测试 mixed English" || transcript.Language != "Chinese" || transcript.Backend != "qwen-asr-llama/cpu" || transcript.Model != DefaultManagedASRModelID {
		t.Fatalf("llama transcript = %#v", transcript)
	}
	if gotCommand != binary {
		t.Fatalf("llama command = %q, want %q", gotCommand, binary)
	}
	requirePlainTextInOrder(t, strings.Join(gotArgs, "\n"),
		"-m", model,
		"--mmproj", mmproj,
		"--audio", "/tmp/input.wav",
		"-p", managedASRLlamaPrompt,
		"-t", "4",
		"-tb", "4",
		"--device", "none",
	)
	env := envSliceToMap(gotEnv)
	if env["CODEX_HELPER_TEAMS_ASR_THREADS"] != "4" || env["CODEX_HELPER_TEAMS_ASR_SPEED"] != "1x" {
		t.Fatalf("llama env missing ASR controls: %#v", env)
	}
}

func TestManagedASRParseLlamaOutputHandlesLanguageColon(t *testing.T) {
	text, language := managedASRParseLlamaOutput("llama.cpp ready\nLanguage: Chinese\n<asr_text>\n测试 mixed English\n")
	if text != "测试 mixed English" || language != "Chinese" {
		t.Fatalf("parsed llama output = text %q language %q", text, language)
	}
}

func TestManagedQwenASRTranscriberAutoFallsBackToTransformers(t *testing.T) {
	cacheRoot := t.TempDir()
	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{
		CacheRoot:    cacheRoot,
		Backend:      managedASRBackendAuto,
		MinFreeBytes: 1,
	})
	transcriber.ensureLlamaRuntime = func(context.Context, ManagedASRConfig) (managedASRLlamaRuntime, error) {
		return managedASRLlamaRuntime{}, managedASRLlamaFallbackError{Err: errors.New("managed llama runtime unavailable on this platform")}
	}
	transcriber.ensureRuntime = func(_ context.Context, cfg ManagedASRConfig) (managedASRRuntime, error) {
		return managedASRRuntime{Python: "/opt/cxp-asr/python", ScriptPath: "runner.py", CacheRoot: cfg.CacheRoot, ModelID: DefaultManagedASRModelID}, nil
	}
	transcriber.runCommand = func(_ context.Context, _ string, _ []string, _ []string, stdout *bytes.Buffer, _ *bytes.Buffer) error {
		_, _ = stdout.WriteString(`{"text":"fallback text","language":"en"}`)
		return nil
	}

	transcript, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		File:     LocalAttachment{Path: "/tmp/input.wav", ContentType: "audio/wav"},
		Language: defaultASRLanguage,
		Speed:    "1x",
	})
	if err != nil {
		t.Fatalf("TranscribeTeamsMedia error: %v", err)
	}
	if transcript.Text != "fallback text" || transcript.Backend != managedASRBackendTransformers || !strings.Contains(transcript.Warning, "llama ASR backend failed") {
		t.Fatalf("fallback transcript = %#v", transcript)
	}
}

func TestManagedQwenASRTranscriberAutoDoesNotFallbackFromLlamaRuntimeFailure(t *testing.T) {
	cacheRoot := t.TempDir()
	binary := writeManagedASRTestFile(t, cacheRoot, "bin/llama-mtmd-cli", 0o700)
	model := writeManagedASRTestFile(t, cacheRoot, "models/qwen.gguf", 0o600)
	mmproj := writeManagedASRTestFile(t, cacheRoot, "models/mmproj.gguf", 0o600)
	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{
		CacheRoot:       cacheRoot,
		Backend:         managedASRBackendAuto,
		LlamaBinaryPath: binary,
		LlamaModelPath:  model,
		LlamaMMProjPath: mmproj,
		MinFreeBytes:    1,
	})
	transcriber.runLlamaCommand = func(_ context.Context, _ string, _ []string, _ []string, _ *bytes.Buffer, stderr *bytes.Buffer) error {
		_, _ = stderr.WriteString("llama failed to initialize")
		return errors.New("exit status 1")
	}
	transcriber.ensureRuntime = func(context.Context, ManagedASRConfig) (managedASRRuntime, error) {
		t.Fatal("non-fallbackable llama runtime failure should not invoke transformers")
		return managedASRRuntime{}, nil
	}
	_, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		File:     LocalAttachment{Path: "/tmp/input.wav", ContentType: "audio/wav"},
		Language: defaultASRLanguage,
		Speed:    "1x",
	})
	if err == nil || !strings.Contains(err.Error(), "llama failed to initialize") {
		t.Fatalf("auto llama runtime error = %v, want direct llama failure", err)
	}
}

func TestManagedQwenASRTranscriberDefaultDownloadsManagedLlamaRuntime(t *testing.T) {
	cacheRoot := t.TempDir()
	binaryArchive := buildManagedASRLlamaTestArchive(t)
	modelData := []byte("test qwen gguf")
	mmprojData := []byte("test mmproj gguf")
	downloads := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads[r.URL.Path]++
		switch r.URL.Path {
		case "/llama.tar.gz":
			_, _ = w.Write(binaryArchive)
		case "/model.gguf":
			_, _ = w.Write(modelData)
		case "/mmproj.gguf":
			_, _ = w.Write(mmprojData)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevHTTPClient := managedASRLlamaHTTPClient
	prevBinaryAssets := managedASRLlamaBinaryAssetsFn
	prevModelAssets := managedASRLlamaManagedModelAssetsFn
	prevValidate := validateManagedASRLlamaBinaryFn
	t.Cleanup(func() {
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRLlamaBinaryAssetsFn = prevBinaryAssets
		managedASRLlamaManagedModelAssetsFn = prevModelAssets
		validateManagedASRLlamaBinaryFn = prevValidate
	})
	managedASRLlamaHTTPClient = server.Client()
	managedASRLlamaBinaryAssetsFn = func(string, string) ([]managedASRLlamaAsset, error) {
		return []managedASRLlamaAsset{{
			Name:         "llama-test.tar.gz",
			URL:          server.URL + "/llama.tar.gz",
			SHA256:       managedASRTestSHA256(binaryArchive),
			Size:         int64(len(binaryArchive)),
			ArchiveKind:  "tar.gz",
			Acceleration: "cpu",
		}}, nil
	}
	managedASRLlamaManagedModelAssetsFn = func() managedASRLlamaModelAssets {
		return managedASRLlamaModelAssets{
			RootName: "test-qwen3-asr",
			Repo:     "test/qwen3-asr",
			Revision: "test-revision",
			Model: managedASRLlamaFileAsset{
				Name:   "model.gguf",
				URL:    server.URL + "/model.gguf",
				SHA256: managedASRTestSHA256(modelData),
				Size:   int64(len(modelData)),
			},
			MMProj: managedASRLlamaFileAsset{
				Name:   "mmproj.gguf",
				URL:    server.URL + "/mmproj.gguf",
				SHA256: managedASRTestSHA256(mmprojData),
				Size:   int64(len(mmprojData)),
			},
		}
	}
	var validatedCommand string
	var validatedEnv []string
	validateManagedASRLlamaBinaryFn = func(_ context.Context, command string, env []string) error {
		validatedCommand = command
		validatedEnv = append([]string(nil), env...)
		return nil
	}

	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{
		CacheRoot:    cacheRoot,
		MinFreeBytes: 1,
	})
	transcriber.ensureRuntime = func(context.Context, ManagedASRConfig) (managedASRRuntime, error) {
		t.Fatal("default auto backend should use managed llama before transformers")
		return managedASRRuntime{}, nil
	}
	var gotCommand string
	var gotArgs []string
	var gotEnv []string
	transcriber.runLlamaCommand = func(_ context.Context, command string, args []string, env []string, stdout *bytes.Buffer, _ *bytes.Buffer) error {
		gotCommand = command
		gotArgs = append([]string(nil), args...)
		gotEnv = append([]string(nil), env...)
		_, _ = stdout.WriteString("<asr_text>downloaded llama transcript")
		return nil
	}

	for i := 0; i < 2; i++ {
		transcript, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
			File:     LocalAttachment{Path: "/tmp/input.wav", ContentType: "audio/wav"},
			Language: defaultASRLanguage,
			Speed:    "1x",
		})
		if err != nil {
			t.Fatalf("TranscribeTeamsMedia run %d error: %v", i+1, err)
		}
		if transcript.Text != "downloaded llama transcript" || transcript.Backend != "qwen-asr-llama/cpu" {
			t.Fatalf("transcript run %d = %#v", i+1, transcript)
		}
	}
	if downloads["/llama.tar.gz"] != 1 || downloads["/model.gguf"] != 1 || downloads["/mmproj.gguf"] != 1 {
		t.Fatalf("downloads = %#v, want each managed asset downloaded once", downloads)
	}
	if validatedCommand == "" || !strings.Contains(validatedCommand, ".llama-staging-") {
		t.Fatalf("llama command = %q, validated = %q", gotCommand, validatedCommand)
	}
	if !strings.Contains(strings.Join(validatedEnv, string(os.PathListSeparator)), filepath.Join("llama", "bin")) {
		t.Fatalf("validated llama env should include nested archive bin dir, got %#v", validatedEnv)
	}
	if !strings.Contains(gotCommand, filepath.Join(cacheRoot, "llama", "runtime")) {
		t.Fatalf("llama command = %q, want installed cache runtime", gotCommand)
	}
	if !strings.Contains(strings.Join(gotEnv, string(os.PathListSeparator)), filepath.Join(cacheRoot, "llama", "runtime", "llama", "bin")) {
		t.Fatalf("llama run env should include installed archive bin dir, got %#v", gotEnv)
	}
	requirePlainTextInOrder(t, strings.Join(gotArgs, "\n"),
		"-m", filepath.Join(cacheRoot, "llama", "models", "test-qwen3-asr", "model.gguf"),
		"--mmproj", filepath.Join(cacheRoot, "llama", "models", "test-qwen3-asr", "mmproj.gguf"),
		"-n", "-1",
	)
	if _, err := os.Stat(filepath.Join(cacheRoot, "llama", "runtime", "runtime.json")); err != nil {
		t.Fatalf("llama runtime marker missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cacheRoot, "llama", "models", "test-qwen3-asr", "runtime.json")); err != nil {
		t.Fatalf("llama model marker missing: %v", err)
	}
}

func TestManagedASRLlamaModelMarkerRejectsCorruptCachedFile(t *testing.T) {
	cacheRoot := t.TempDir()
	modelData := []byte("fresh qwen gguf")
	mmprojData := []byte("fresh mmproj gguf")
	downloads := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads[r.URL.Path]++
		switch r.URL.Path {
		case "/model.gguf":
			_, _ = w.Write(modelData)
		case "/mmproj.gguf":
			_, _ = w.Write(mmprojData)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevHTTPClient := managedASRLlamaHTTPClient
	prevModelAssets := managedASRLlamaManagedModelAssetsFn
	t.Cleanup(func() {
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRLlamaManagedModelAssetsFn = prevModelAssets
	})
	managedASRLlamaHTTPClient = server.Client()
	managedASRLlamaManagedModelAssetsFn = func() managedASRLlamaModelAssets {
		return managedASRLlamaModelAssets{
			RootName: "test-qwen3-asr",
			Repo:     "test/qwen3-asr",
			Revision: "test-revision",
			Model: managedASRLlamaFileAsset{
				Name:   "model.gguf",
				URL:    server.URL + "/model.gguf",
				SHA256: managedASRTestSHA256(modelData),
				Size:   int64(len(modelData)),
			},
			MMProj: managedASRLlamaFileAsset{
				Name:   "mmproj.gguf",
				URL:    server.URL + "/mmproj.gguf",
				SHA256: managedASRTestSHA256(mmprojData),
				Size:   int64(len(mmprojData)),
			},
		}
	}

	llamaRoot := filepath.Join(cacheRoot, "llama")
	modelPath, _, err := ensureManagedASRLlamaManagedModelFiles(context.Background(), llamaRoot)
	if err != nil {
		t.Fatalf("initial model install: %v", err)
	}
	if err := os.WriteFile(modelPath, []byte("bad"), 0o600); err != nil {
		t.Fatalf("corrupt cached model: %v", err)
	}
	modelPath, _, err = ensureManagedASRLlamaManagedModelFiles(context.Background(), llamaRoot)
	if err != nil {
		t.Fatalf("model reinstall after corruption: %v", err)
	}
	got, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("read repaired model: %v", err)
	}
	if string(got) != string(modelData) {
		t.Fatalf("repaired model = %q, want %q", got, modelData)
	}
	if downloads["/model.gguf"] != 2 || downloads["/mmproj.gguf"] != 2 {
		t.Fatalf("downloads = %#v, want corrupt marker to redownload model set", downloads)
	}
}

func TestManagedASRLlamaRuntimeConcurrentSetupReusesSingleInstall(t *testing.T) {
	cacheRoot := t.TempDir()
	binaryArchive := buildManagedASRLlamaTestArchive(t)
	modelData := []byte("test qwen gguf")
	mmprojData := []byte("test mmproj gguf")
	downloads := map[string]int{}
	var downloadsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloadsMu.Lock()
		downloads[r.URL.Path]++
		downloadsMu.Unlock()
		switch r.URL.Path {
		case "/llama.tar.gz":
			_, _ = w.Write(binaryArchive)
		case "/model.gguf":
			_, _ = w.Write(modelData)
		case "/mmproj.gguf":
			_, _ = w.Write(mmprojData)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevHTTPClient := managedASRLlamaHTTPClient
	prevBinaryAssets := managedASRLlamaBinaryAssetsFn
	prevModelAssets := managedASRLlamaManagedModelAssetsFn
	prevValidate := validateManagedASRLlamaBinaryFn
	t.Cleanup(func() {
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRLlamaBinaryAssetsFn = prevBinaryAssets
		managedASRLlamaManagedModelAssetsFn = prevModelAssets
		validateManagedASRLlamaBinaryFn = prevValidate
	})
	managedASRLlamaHTTPClient = server.Client()
	managedASRLlamaBinaryAssetsFn = func(string, string) ([]managedASRLlamaAsset, error) {
		return []managedASRLlamaAsset{{
			Name:         "llama-test.tar.gz",
			URL:          server.URL + "/llama.tar.gz",
			SHA256:       managedASRTestSHA256(binaryArchive),
			Size:         int64(len(binaryArchive)),
			ArchiveKind:  "tar.gz",
			Acceleration: "cpu",
		}}, nil
	}
	managedASRLlamaManagedModelAssetsFn = func() managedASRLlamaModelAssets {
		return managedASRLlamaModelAssets{
			RootName: "test-qwen3-asr",
			Repo:     "test/qwen3-asr",
			Revision: "test-revision",
			Model: managedASRLlamaFileAsset{
				Name:   "model.gguf",
				URL:    server.URL + "/model.gguf",
				SHA256: managedASRTestSHA256(modelData),
				Size:   int64(len(modelData)),
			},
			MMProj: managedASRLlamaFileAsset{
				Name:   "mmproj.gguf",
				URL:    server.URL + "/mmproj.gguf",
				SHA256: managedASRTestSHA256(mmprojData),
				Size:   int64(len(mmprojData)),
			},
		}
	}
	validateStarted := make(chan struct{})
	releaseValidate := make(chan struct{})
	var validateOnce sync.Once
	validateManagedASRLlamaBinaryFn = func(context.Context, string, []string) error {
		validateOnce.Do(func() {
			close(validateStarted)
			<-releaseValidate
		})
		return nil
	}

	cfg := ManagedASRConfig{CacheRoot: cacheRoot, MinFreeBytes: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		_, err := ensureManagedASRLlamaRuntime(ctx, cfg)
		results <- err
	}()
	<-validateStarted
	go func() {
		_, err := ensureManagedASRLlamaRuntime(ctx, cfg)
		results <- err
	}()
	time.Sleep(100 * time.Millisecond)
	close(releaseValidate)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent setup result %d: %v", i+1, err)
		}
	}
	downloadsMu.Lock()
	defer downloadsMu.Unlock()
	if downloads["/llama.tar.gz"] != 1 || downloads["/model.gguf"] != 1 || downloads["/mmproj.gguf"] != 1 {
		t.Fatalf("downloads = %#v, want one managed install shared by concurrent callers", downloads)
	}
}

func TestManagedASRLlamaBinaryInstallFallsBackToCPUAssetWhenAcceleratedValidationFails(t *testing.T) {
	cacheRoot := t.TempDir()
	vulkanArchive := buildManagedASRLlamaTestArchiveWithRoot(t, "vulkan")
	cpuArchive := buildManagedASRLlamaTestArchiveWithRoot(t, "cpu")
	downloads := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads[r.URL.Path]++
		switch r.URL.Path {
		case "/vulkan.tar.gz":
			_, _ = w.Write(vulkanArchive)
		case "/cpu.tar.gz":
			_, _ = w.Write(cpuArchive)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevHTTPClient := managedASRLlamaHTTPClient
	prevAssets := managedASRLlamaBinaryAssetsFn
	prevValidate := validateManagedASRLlamaBinaryFn
	t.Cleanup(func() {
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRLlamaBinaryAssetsFn = prevAssets
		validateManagedASRLlamaBinaryFn = prevValidate
	})
	managedASRLlamaHTTPClient = server.Client()
	managedASRLlamaBinaryAssetsFn = func(string, string) ([]managedASRLlamaAsset, error) {
		return []managedASRLlamaAsset{
			{
				Name:         "vulkan.tar.gz",
				URL:          server.URL + "/vulkan.tar.gz",
				SHA256:       managedASRTestSHA256(vulkanArchive),
				Size:         int64(len(vulkanArchive)),
				ArchiveKind:  "tar.gz",
				Acceleration: "vulkan",
			},
			{
				Name:         "cpu.tar.gz",
				URL:          server.URL + "/cpu.tar.gz",
				SHA256:       managedASRTestSHA256(cpuArchive),
				Size:         int64(len(cpuArchive)),
				ArchiveKind:  "tar.gz",
				Acceleration: "cpu",
			},
		}, nil
	}
	var validations []string
	validateManagedASRLlamaBinaryFn = func(_ context.Context, command string, _ []string) error {
		validations = append(validations, command)
		if strings.Contains(command, string(os.PathSeparator)+"vulkan"+string(os.PathSeparator)) {
			return errors.New("vulkan runtime cannot start")
		}
		return nil
	}

	command, _, acceleration, err := ensureManagedASRLlamaBinary(context.Background(), filepath.Join(cacheRoot, "llama"), ManagedASRConfig{})
	if err != nil {
		t.Fatalf("cpu fallback install error: %v", err)
	}
	if !strings.Contains(command, string(os.PathSeparator)+"cpu"+string(os.PathSeparator)) {
		t.Fatalf("fallback command = %q, want cpu archive command", command)
	}
	if acceleration != "cpu" {
		t.Fatalf("fallback acceleration = %q, want cpu", acceleration)
	}
	if downloads["/vulkan.tar.gz"] != 1 || downloads["/cpu.tar.gz"] != 1 {
		t.Fatalf("downloads = %#v, want accelerated attempt then cpu fallback", downloads)
	}
	if len(validations) != 2 {
		t.Fatalf("validations = %#v, want both runtime candidates validated", validations)
	}
	data, err := os.ReadFile(filepath.Join(cacheRoot, "llama", "runtime", "runtime.json"))
	if err != nil {
		t.Fatalf("read runtime marker: %v", err)
	}
	if !strings.Contains(string(data), `"acceleration": "cpu"`) || !strings.Contains(string(data), `"asset_name": "cpu.tar.gz"`) {
		t.Fatalf("runtime marker did not record cpu fallback:\n%s", data)
	}
}

func TestManagedASRLlamaBinaryInstallHonorsExplicitCPUDevice(t *testing.T) {
	cacheRoot := t.TempDir()
	vulkanArchive := buildManagedASRLlamaTestArchiveWithRoot(t, "vulkan")
	cpuArchive := buildManagedASRLlamaTestArchiveWithRoot(t, "cpu")
	downloads := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads[r.URL.Path]++
		switch r.URL.Path {
		case "/vulkan.tar.gz":
			_, _ = w.Write(vulkanArchive)
		case "/cpu.tar.gz":
			_, _ = w.Write(cpuArchive)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevHTTPClient := managedASRLlamaHTTPClient
	prevAssets := managedASRLlamaBinaryAssetsFn
	prevValidate := validateManagedASRLlamaBinaryFn
	t.Cleanup(func() {
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRLlamaBinaryAssetsFn = prevAssets
		validateManagedASRLlamaBinaryFn = prevValidate
	})
	managedASRLlamaHTTPClient = server.Client()
	managedASRLlamaBinaryAssetsFn = func(string, string) ([]managedASRLlamaAsset, error) {
		return []managedASRLlamaAsset{
			{
				Name:         "vulkan.tar.gz",
				URL:          server.URL + "/vulkan.tar.gz",
				SHA256:       managedASRTestSHA256(vulkanArchive),
				Size:         int64(len(vulkanArchive)),
				ArchiveKind:  "tar.gz",
				Acceleration: "vulkan",
			},
			{
				Name:         "cpu.tar.gz",
				URL:          server.URL + "/cpu.tar.gz",
				SHA256:       managedASRTestSHA256(cpuArchive),
				Size:         int64(len(cpuArchive)),
				ArchiveKind:  "tar.gz",
				Acceleration: "cpu",
			},
		}, nil
	}
	validateManagedASRLlamaBinaryFn = func(context.Context, string, []string) error { return nil }

	command, _, acceleration, err := ensureManagedASRLlamaBinary(context.Background(), filepath.Join(cacheRoot, "llama"), ManagedASRConfig{LlamaDevice: "cpu"})
	if err != nil {
		t.Fatalf("cpu device install error: %v", err)
	}
	if !strings.Contains(command, string(os.PathSeparator)+"cpu"+string(os.PathSeparator)) || acceleration != "cpu" {
		t.Fatalf("cpu device command = %q acceleration = %q", command, acceleration)
	}
	if downloads["/vulkan.tar.gz"] != 0 || downloads["/cpu.tar.gz"] != 1 {
		t.Fatalf("downloads = %#v, want only cpu asset", downloads)
	}
}

func TestManagedASRLlamaAssetMatrixCoversExpectedAccelerationFallbacks(t *testing.T) {
	cases := []struct {
		name             string
		goos             string
		goarch           string
		wantAcceleration []string
		wantArchive      string
	}{
		{name: "linux amd64", goos: "linux", goarch: "amd64", wantAcceleration: []string{"vulkan", "cpu"}, wantArchive: "tar.gz"},
		{name: "linux arm64", goos: "linux", goarch: "arm64", wantAcceleration: []string{"vulkan", "cpu"}, wantArchive: "tar.gz"},
		{name: "darwin amd64", goos: "darwin", goarch: "amd64", wantAcceleration: []string{"metal"}, wantArchive: "tar.gz"},
		{name: "darwin arm64", goos: "darwin", goarch: "arm64", wantAcceleration: []string{"metal"}, wantArchive: "tar.gz"},
		{name: "windows amd64", goos: "windows", goarch: "amd64", wantAcceleration: []string{"vulkan", "cpu"}, wantArchive: "zip"},
		{name: "windows arm64", goos: "windows", goarch: "arm64", wantAcceleration: []string{"cpu"}, wantArchive: "zip"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assets, err := managedASRLlamaBinaryAssets(tc.goos, tc.goarch)
			if err != nil {
				t.Fatalf("managedASRLlamaBinaryAssets error: %v", err)
			}
			if len(assets) != len(tc.wantAcceleration) {
				t.Fatalf("assets = %#v, want %d entries", assets, len(tc.wantAcceleration))
			}
			for i, want := range tc.wantAcceleration {
				if assets[i].Acceleration != want || assets[i].ArchiveKind != tc.wantArchive || !strings.Contains(assets[i].URL, managedASRLlamaReleaseTag) || assets[i].SHA256 == "" || assets[i].Size <= 0 {
					t.Fatalf("asset[%d] = %#v, want acceleration=%q archive=%q with pinned URL/hash/size", i, assets[i], want, tc.wantArchive)
				}
			}
		})
	}
	if _, err := managedASRLlamaBinaryAssets("linux", "386"); err == nil {
		t.Fatal("unsupported llama platform should return an error")
	}
}

func TestManagedASRFFmpegWheelAssetMatrixCoversExpectedPlatforms(t *testing.T) {
	for _, tc := range []struct {
		name   string
		goos   string
		goarch string
		want   string
	}{
		{name: "linux amd64", goos: "linux", goarch: "amd64", want: "manylinux2014_x86_64"},
		{name: "linux arm64", goos: "linux", goarch: "arm64", want: "manylinux2014_aarch64"},
		{name: "darwin amd64", goos: "darwin", goarch: "amd64", want: "macosx_10_9_x86_64"},
		{name: "darwin arm64", goos: "darwin", goarch: "arm64", want: "macosx_11_0_arm64"},
		{name: "windows amd64", goos: "windows", goarch: "amd64", want: "win_amd64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asset, err := managedASRFFmpegWheelAsset(tc.goos, tc.goarch)
			if err != nil {
				t.Fatalf("managedASRFFmpegWheelAsset error: %v", err)
			}
			if !strings.Contains(asset.Name, tc.want) || !strings.Contains(asset.URL, "files.pythonhosted.org") || asset.SHA256 == "" || asset.Size <= 0 {
				t.Fatalf("ffmpeg asset = %#v, want pinned %q wheel", asset, tc.want)
			}
		})
	}
	if _, err := managedASRFFmpegWheelAsset("windows", "arm64"); !errors.Is(err, errManagedASRFFmpegUnsupported) {
		t.Fatalf("windows/arm64 ffmpeg asset error = %v, want unsupported sentinel", err)
	}
}

func TestManagedASRRealPinnedLlamaAndFFmpegAssetsInstallAndValidate(t *testing.T) {
	if os.Getenv("CODEX_HELPER_ASR_REAL_ASSET_TEST") != "1" {
		t.Skip("set CODEX_HELPER_ASR_REAL_ASSET_TEST=1 to download and validate pinned llama.cpp and ffmpeg assets")
	}
	cacheRoot := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	command, _, acceleration, err := ensureManagedASRLlamaBinary(ctx, filepath.Join(cacheRoot, "llama"), ManagedASRConfig{})
	if err != nil {
		t.Fatalf("install real pinned llama.cpp runtime: %v", err)
	}
	if command == "" || acceleration == "" {
		t.Fatalf("real llama runtime command=%q acceleration=%q", command, acceleration)
	}
	ffmpeg, err := ensureManagedASRFFmpeg(ctx, filepath.Join(cacheRoot, "ffmpeg"))
	if err != nil {
		t.Fatalf("install real pinned ffmpeg runtime: %v", err)
	}
	if ffmpeg == "" {
		t.Fatal("real ffmpeg runtime path is empty")
	}
	t.Logf("validated real managed ASR assets: llama=%s acceleration=%s ffmpeg=%s", command, acceleration, ffmpeg)
}

func TestDownloadManagedASRLlamaFileRejectsSizeAndHashMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("actual data"))
	}))
	defer server.Close()
	prevHTTPClient := managedASRLlamaHTTPClient
	t.Cleanup(func() { managedASRLlamaHTTPClient = prevHTTPClient })
	managedASRLlamaHTTPClient = server.Client()

	tmp := t.TempDir()
	sizeErr := downloadManagedASRLlamaFile(context.Background(), managedASRLlamaFileAsset{
		Name: "asset.bin",
		URL:  server.URL + "/asset.bin",
		Size: 999,
	}, filepath.Join(tmp, "size.bin"), "test asset")
	if sizeErr == nil || !strings.Contains(sizeErr.Error(), "downloaded 11 bytes, want 999") {
		t.Fatalf("size mismatch error = %v", sizeErr)
	}
	hashErr := downloadManagedASRLlamaFile(context.Background(), managedASRLlamaFileAsset{
		Name:   "asset.bin",
		URL:    server.URL + "/asset.bin",
		SHA256: strings.Repeat("0", 64),
		Size:   int64(len("actual data")),
	}, filepath.Join(tmp, "hash.bin"), "test asset")
	if hashErr == nil || !strings.Contains(hashErr.Error(), "sha256") {
		t.Fatalf("hash mismatch error = %v", hashErr)
	}
}

func TestDownloadManagedASRLlamaFileRetriesAndReplacesExistingTarget(t *testing.T) {
	tmp := t.TempDir()
	body := []byte("fresh data")
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests < 3 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()

	prevHTTPClient := managedASRLlamaHTTPClient
	prevBackoff := managedASRLlamaDownloadBackoff
	t.Cleanup(func() {
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRLlamaDownloadBackoff = prevBackoff
	})
	managedASRLlamaHTTPClient = server.Client()
	managedASRLlamaDownloadBackoff = 0
	target := filepath.Join(tmp, "asset.bin")
	if err := os.WriteFile(target, []byte("stale data"), 0o600); err != nil {
		t.Fatalf("write stale target: %v", err)
	}
	if err := downloadManagedASRLlamaFile(context.Background(), managedASRLlamaFileAsset{
		Name:   "asset.bin",
		URL:    server.URL + "/asset.bin",
		SHA256: managedASRTestSHA256(body),
		Size:   int64(len(body)),
	}, target, "test asset"); err != nil {
		t.Fatalf("download with retry: %v", err)
	}
	if requests != 3 {
		t.Fatalf("requests = %d, want 3", requests)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("target body = %q, want %q", got, body)
	}
}

func TestExtractManagedASRZipRejectsUnsafePath(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	writer, err := zw.Create("../evil")
	if err != nil {
		t.Fatalf("create zip entry: %v", err)
	}
	if _, err := writer.Write([]byte("bad")); err != nil {
		t.Fatalf("write zip entry: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
	archivePath := filepath.Join(t.TempDir(), "unsafe.zip")
	if err := os.WriteFile(archivePath, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write zip: %v", err)
	}
	err = extractManagedASRZip(archivePath, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "unsafe managed Python archive path") {
		t.Fatalf("unsafe zip extract error = %v", err)
	}
}

func TestEnsureManagedASRFFmpegInstallsAndReusesManagedWheel(t *testing.T) {
	cacheRoot := t.TempDir()
	ffmpegWheel := buildManagedASRFFmpegTestWheel(t)
	downloads := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads[r.URL.Path]++
		switch r.URL.Path {
		case "/ffmpeg.whl":
			_, _ = w.Write(ffmpegWheel)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevHTTPClient := managedASRLlamaHTTPClient
	prevFFmpegAsset := managedASRFFmpegWheelAssetFn
	prevValidate := validateManagedASRFFmpegBinaryFn
	t.Cleanup(func() {
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRFFmpegWheelAssetFn = prevFFmpegAsset
		validateManagedASRFFmpegBinaryFn = prevValidate
	})
	managedASRLlamaHTTPClient = server.Client()
	managedASRFFmpegWheelAssetFn = func(string, string) (managedASRLlamaFileAsset, error) {
		return managedASRLlamaFileAsset{
			Name:   "ffmpeg.whl",
			URL:    server.URL + "/ffmpeg.whl",
			SHA256: managedASRTestSHA256(ffmpegWheel),
			Size:   int64(len(ffmpegWheel)),
		}, nil
	}
	validateManagedASRFFmpegBinaryFn = func(context.Context, string) error { return nil }

	first, err := ensureManagedASRFFmpeg(context.Background(), filepath.Join(cacheRoot, "ffmpeg"))
	if err != nil {
		t.Fatalf("ensureManagedASRFFmpeg first: %v", err)
	}
	second, err := ensureManagedASRFFmpeg(context.Background(), filepath.Join(cacheRoot, "ffmpeg"))
	if err != nil {
		t.Fatalf("ensureManagedASRFFmpeg second: %v", err)
	}
	if first == "" || first != second {
		t.Fatalf("ffmpeg paths first=%q second=%q", first, second)
	}
	if downloads["/ffmpeg.whl"] != 1 {
		t.Fatalf("downloads = %#v, want managed ffmpeg wheel downloaded once", downloads)
	}
}

func TestPrepareManagedASRAudioForLlamaDownloadsManagedFFmpeg(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake ffmpeg script is POSIX-only")
	}
	cacheRoot := t.TempDir()
	inputPath := writeManagedASRTestFile(t, cacheRoot, "input.f4a", 0o600)
	ffmpegWheel := buildManagedASRFFmpegTestWheel(t)
	downloads := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads[r.URL.Path]++
		switch r.URL.Path {
		case "/ffmpeg.whl":
			_, _ = w.Write(ffmpegWheel)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevLookPath := managedASRLookPath
	prevHTTPClient := managedASRLlamaHTTPClient
	prevFFmpegAsset := managedASRFFmpegWheelAssetFn
	t.Cleanup(func() {
		managedASRLookPath = prevLookPath
		managedASRLlamaHTTPClient = prevHTTPClient
		managedASRFFmpegWheelAssetFn = prevFFmpegAsset
	})
	managedASRLookPath = func(command string) (string, error) {
		return "", errors.New(command + " not found")
	}
	managedASRLlamaHTTPClient = server.Client()
	managedASRFFmpegWheelAssetFn = func(string, string) (managedASRLlamaFileAsset, error) {
		return managedASRLlamaFileAsset{
			Name:   "imageio_ffmpeg-test.whl",
			URL:    server.URL + "/ffmpeg.whl",
			SHA256: managedASRTestSHA256(ffmpegWheel),
			Size:   int64(len(ffmpegWheel)),
		}, nil
	}

	for i := 0; i < 2; i++ {
		out, cleanup, err := prepareManagedASRAudioForLlama(context.Background(), inputPath, defaultASRSpeed, managedASRLlamaRuntime{
			CacheRoot: filepath.Join(cacheRoot, "llama"),
		})
		if cleanup != nil {
			defer cleanup()
		}
		if err != nil {
			t.Fatalf("prepareManagedASRAudioForLlama run %d error: %v", i+1, err)
		}
		if data, err := os.ReadFile(out); err != nil || string(data) != "fake wav\n" {
			t.Fatalf("prepared audio run %d = %q, %v", i+1, string(data), err)
		}
	}
	if downloads["/ffmpeg.whl"] != 1 {
		t.Fatalf("downloads = %#v, want managed ffmpeg downloaded once across marker reuse", downloads)
	}
	if _, err := os.Stat(filepath.Join(cacheRoot, "llama", "ffmpeg", "runtime.json")); err != nil {
		t.Fatalf("ffmpeg marker missing: %v", err)
	}
}

func TestManagedQwenASRTranscriberExplicitLlamaRejectsConfiguredMissingBinary(t *testing.T) {
	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{
		Backend:         managedASRBackendLlama,
		CacheRoot:       t.TempDir(),
		MinFreeBytes:    1,
		LlamaBinaryPath: filepath.Join(t.TempDir(), "missing-llama-mtmd-cli"),
	})
	_, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		File: LocalAttachment{Path: "/tmp/input.wav", ContentType: "audio/wav"},
	})
	if err == nil || !strings.Contains(err.Error(), "llama ASR binary is not usable") {
		t.Fatalf("explicit llama error = %v, want configured missing binary path", err)
	}
}

func managedASRTestSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func writeManagedASRTestFile(t *testing.T, root string, name string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir test file dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("test"), mode); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	return path
}

func TestManagedASRLlamaConfigUsesLightweightDiskPreflight(t *testing.T) {
	cfg, err := resolveManagedASRLlamaConfig(ManagedASRConfig{CacheRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("resolveManagedASRLlamaConfig: %v", err)
	}
	if cfg.MinFreeBytes != managedASRLlamaDefaultMinFreeBytes {
		t.Fatalf("llama min free bytes = %d, want %d", cfg.MinFreeBytes, managedASRLlamaDefaultMinFreeBytes)
	}
	transformers, err := resolveManagedASRConfig(ManagedASRConfig{CacheRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("resolveManagedASRConfig: %v", err)
	}
	if transformers.MinFreeBytes != managedASRDefaultMinFreeBytes {
		t.Fatalf("transformers min free bytes = %d, want %d", transformers.MinFreeBytes, managedASRDefaultMinFreeBytes)
	}
}

func TestManagedASRDiskPreflightReportsActionableSpace(t *testing.T) {
	prev := managedASRDiskFreeBytes
	managedASRDiskFreeBytes = func(string) (uint64, error) {
		return 512 * 1024 * 1024, nil
	}
	t.Cleanup(func() { managedASRDiskFreeBytes = prev })

	err := ensureManagedASRDiskSpace(t.TempDir(), 8*1024*1024*1024)
	var diskErr managedASRDiskSpaceError
	if !errors.As(err, &diskErr) {
		t.Fatalf("disk preflight error = %v, want managedASRDiskSpaceError", err)
	}
	if !strings.Contains(err.Error(), "512.0 MiB available") || !strings.Contains(err.Error(), "8192.0 MiB") {
		t.Fatalf("disk preflight message = %q", err.Error())
	}
}

func TestManagedASRTempCleanupRemovesOnlyStaleRuntimeDirs(t *testing.T) {
	cacheRoot := t.TempDir()
	tmpRoot := filepath.Join(cacheRoot, "tmp")
	now := time.Date(2026, 5, 28, 1, 2, 3, 0, time.UTC)
	old := now.Add(-48 * time.Hour)
	fresh := now.Add(-time.Hour)
	paths := map[string]time.Time{
		"venv-staging-old": old,
		"transcribe-old":   old,
		"ffmpeg-old":       old,
		"transcribe-fresh": fresh,
		"keep-old":         old,
	}
	for name, mod := range paths {
		dir := filepath.Join(tmpRoot, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.Chtimes(dir, mod, mod); err != nil {
			t.Fatalf("chtimes %s: %v", name, err)
		}
	}

	if err := cleanupManagedASRTemp(cacheRoot, now, 24*time.Hour); err != nil {
		t.Fatalf("cleanupManagedASRTemp error: %v", err)
	}
	for _, name := range []string{"venv-staging-old", "transcribe-old", "ffmpeg-old"} {
		if _, err := os.Stat(filepath.Join(tmpRoot, name)); !os.IsNotExist(err) {
			t.Fatalf("%s still exists after cleanup: %v", name, err)
		}
	}
	for _, name := range []string{"transcribe-fresh", "keep-old"} {
		if _, err := os.Stat(filepath.Join(tmpRoot, name)); err != nil {
			t.Fatalf("%s should remain after cleanup: %v", name, err)
		}
	}
}

func TestManagedASRRunnerScriptIsRewrittenAtomically(t *testing.T) {
	cacheRoot := t.TempDir()
	scriptPath, err := ensureManagedASRRunnerScript(cacheRoot)
	if err != nil {
		t.Fatalf("ensureManagedASRRunnerScript error: %v", err)
	}
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read runner script: %v", err)
	}
	for _, want := range []string{
		"from qwen_asr import Qwen3ASRModel",
		"imageio_ffmpeg.get_ffmpeg_exe()",
		"atempo=",
		`tempfile.mkdtemp(prefix="transcribe-", dir=tmp_base)`,
		"torch.cuda.is_available()",
		"Qwen/Qwen3-ASR-0.6B",
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("managed runner script missing %q:\n%s", want, string(data))
		}
	}
	if _, err := os.Stat(scriptPath + ".sha256"); err != nil {
		t.Fatalf("runner script hash missing: %v", err)
	}
}

func TestManagedASRPackageInstallPlanIncludesTorchAndPinnedRuntimeTools(t *testing.T) {
	args := managedASRPackageInstallArgs()
	requirePlainTextInOrder(t, strings.Join(args, "\n"),
		"-m",
		"pip",
		"install",
		"--upgrade",
		"--only-binary=:all:",
		"qwen-asr==0.0.6",
		"imageio-ffmpeg==0.6.0",
		"torch>=2.4,<2.13",
	)
}

func TestManagedASRBootstrapFallsBackToManagedStandalonePython(t *testing.T) {
	cacheRoot := t.TempDir()
	prevLookPath := managedASRLookPath
	prevValidate := validateManagedASRBootstrapPythonFn
	prevEnsureStandalone := ensureManagedASRStandalonePythonFn
	t.Cleanup(func() {
		managedASRLookPath = prevLookPath
		validateManagedASRBootstrapPythonFn = prevValidate
		ensureManagedASRStandalonePythonFn = prevEnsureStandalone
	})

	managedASRLookPath = func(command string) (string, error) {
		return filepath.Join("/usr/bin", command), nil
	}
	validateManagedASRBootstrapPythonFn = func(python managedASRBootstrapPython) error {
		return errors.New(python.Display + " has no venv module")
	}
	ensureManagedASRStandalonePythonFn = func(_ context.Context, gotCacheRoot string) (managedASRBootstrapPython, error) {
		if gotCacheRoot != cacheRoot {
			t.Fatalf("standalone cacheRoot = %q, want %q", gotCacheRoot, cacheRoot)
		}
		return managedASRBootstrapPython{Command: filepath.Join(cacheRoot, "python", "python"), Display: "managed Python"}, nil
	}

	python, err := findManagedASRBootstrapPython(context.Background(), cacheRoot)
	if err != nil {
		t.Fatalf("findManagedASRBootstrapPython error: %v", err)
	}
	if !strings.Contains(python.Command, filepath.Join(cacheRoot, "python")) || python.Display != "managed Python" {
		t.Fatalf("bootstrap python = %#v, want managed fallback", python)
	}
}

func TestManagedASRBootstrapPythonCandidatesArePlatformSpecific(t *testing.T) {
	windowsCandidates := managedASRBootstrapPythonCandidates("windows")
	if len(windowsCandidates) < 2 ||
		windowsCandidates[0].Command != "py" ||
		!reflect.DeepEqual(windowsCandidates[0].Args, []string{"-3"}) ||
		windowsCandidates[0].Display != "py -3" {
		t.Fatalf("windows bootstrap candidates = %#v", windowsCandidates)
	}
	unixCandidates := managedASRBootstrapPythonCandidates("linux")
	if len(unixCandidates) < 2 ||
		unixCandidates[0].Command != "python3" ||
		len(unixCandidates[0].Args) != 0 {
		t.Fatalf("unix bootstrap candidates = %#v", unixCandidates)
	}
}

func TestManagedASRStandalonePythonTargetCommonPlatforms(t *testing.T) {
	cases := map[string]struct {
		goos   string
		goarch string
		want   string
	}{
		"linux amd64":   {goos: "linux", goarch: "amd64", want: "x86_64-unknown-linux-gnu"},
		"linux arm64":   {goos: "linux", goarch: "arm64", want: "aarch64-unknown-linux-gnu"},
		"darwin amd64":  {goos: "darwin", goarch: "amd64", want: "x86_64-apple-darwin"},
		"darwin arm64":  {goos: "darwin", goarch: "arm64", want: "aarch64-apple-darwin"},
		"windows amd64": {goos: "windows", goarch: "amd64", want: "x86_64-pc-windows-msvc"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := managedASRStandalonePythonTarget(tc.goos, tc.goarch)
			if err != nil {
				t.Fatalf("managedASRStandalonePythonTarget error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("target = %q, want %q", got, tc.want)
			}
		})
	}
	if _, err := managedASRStandalonePythonTarget("linux", "386"); err == nil {
		t.Fatal("unsupported platform should return an error")
	}
	if _, err := managedASRStandalonePythonTarget("windows", "arm64"); err == nil {
		t.Fatal("windows/arm64 has no pinned managed Python asset and should return an error")
	}
}

func TestManagedASRStandalonePythonPinnedAssetUsesDirectDownloadURL(t *testing.T) {
	got, err := resolveManagedASRStandalonePythonAsset(context.Background(), "linux", "amd64")
	if err != nil {
		t.Fatalf("resolveManagedASRStandalonePythonAsset error: %v", err)
	}
	for _, want := range []string{
		managedASRStandalonePythonReleaseTag,
		"cpython-" + managedASRStandalonePythonVersion + "+" + managedASRStandalonePythonReleaseTag,
		"x86_64-unknown-linux-gnu-install_only_stripped.tar.gz",
		managedASRStandalonePythonDownloadBase,
	} {
		if !strings.Contains(got.ReleaseTag+" "+got.Name+" "+got.URL, want) {
			t.Fatalf("asset = %#v, missing %q", got, want)
		}
	}
}

func TestManagedASRStandalonePythonDownloadsExtractsAndMarksRuntime(t *testing.T) {
	cacheRoot := t.TempDir()
	target, err := managedASRStandalonePythonTarget(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skipf("platform has no managed standalone Python target: %v", err)
	}
	archive := buildManagedASRStandalonePythonTestArchive(t)
	downloads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/asset.tar.gz":
			downloads++
			_, _ = w.Write(archive)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	prevHTTPClient := managedASRStandalonePythonHTTPClient
	prevValidate := validateManagedASRBootstrapPythonFn
	t.Cleanup(func() {
		managedASRStandalonePythonHTTPClient = prevHTTPClient
		validateManagedASRBootstrapPythonFn = prevValidate
	})
	managedASRStandalonePythonHTTPClient = server.Client()
	var validated []string
	validateManagedASRBootstrapPythonFn = func(python managedASRBootstrapPython) error {
		validated = append(validated, python.Command)
		return nil
	}

	python, err := installManagedASRStandalonePython(context.Background(), filepath.Join(cacheRoot, "python", managedASRStandalonePythonDirName), managedASRStandalonePythonAsset{
		ReleaseTag: managedASRStandalonePythonReleaseTag,
		Name:       "cpython-" + managedASRStandalonePythonVersion + "+" + managedASRStandalonePythonReleaseTag + "-" + target + "-install_only_stripped.tar.gz",
		URL:        server.URL + "/asset.tar.gz",
		Target:     target,
	})
	if err != nil {
		t.Fatalf("installManagedASRStandalonePython error: %v", err)
	}
	if downloads != 1 {
		t.Fatalf("downloads = %d, want 1", downloads)
	}
	if !strings.Contains(python.Command, filepath.Join(cacheRoot, "python", managedASRStandalonePythonDirName)) {
		t.Fatalf("python command = %q, want cache-local standalone Python", python.Command)
	}
	if _, err := os.Stat(filepath.Join(cacheRoot, "python", managedASRStandalonePythonDirName, "runtime.json")); err != nil {
		t.Fatalf("runtime marker missing: %v", err)
	}
	again, err := ensureManagedASRStandalonePython(context.Background(), cacheRoot)
	if err != nil {
		t.Fatalf("second ensureManagedASRStandalonePython error: %v", err)
	}
	if downloads != 1 {
		t.Fatalf("second ensure downloaded again: downloads=%d", downloads)
	}
	if again.Command != python.Command {
		t.Fatalf("second python = %q, want %q", again.Command, python.Command)
	}
	if len(validated) < 2 {
		t.Fatalf("validated calls = %#v, want install and marker validation", validated)
	}
}

func TestManagedASRStandalonePythonDownloadIntegration(t *testing.T) {
	if os.Getenv("CODEX_HELPER_TEST_MANAGED_ASR_PYTHON_DOWNLOAD") != "1" {
		t.Skip("set CODEX_HELPER_TEST_MANAGED_ASR_PYTHON_DOWNLOAD=1 to download and validate the pinned managed Python archive")
	}
	cacheRoot := t.TempDir()
	python, err := ensureManagedASRStandalonePython(context.Background(), cacheRoot)
	if err != nil {
		t.Fatalf("ensureManagedASRStandalonePython error: %v", err)
	}
	out, err := exec.Command(python.Command, "-c", "import sys, venv, ensurepip; print(sys.version)").CombinedOutput()
	if err != nil {
		t.Fatalf("managed Python validation failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "3.10.") {
		t.Fatalf("managed Python version output = %q, want 3.10.x", string(out))
	}
}

func buildManagedASRLlamaTestArchive(t *testing.T) []byte {
	return buildManagedASRLlamaTestArchiveWithRoot(t, "llama")
}

func buildManagedASRLlamaTestArchiveWithRoot(t *testing.T, root string) []byte {
	t.Helper()
	root = strings.Trim(strings.TrimSpace(root), "/")
	if root == "" {
		root = "llama"
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: root + "/", Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
		t.Fatalf("write llama dir header: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: root + "/bin/", Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
		t.Fatalf("write llama bin dir header: %v", err)
	}
	body := []byte("#!/bin/sh\nexit 0\n")
	if runtime.GOOS == "windows" {
		body = []byte("fake exe")
	}
	name := root + "/bin/llama-mtmd-cli"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o700, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatalf("write llama binary header: %v", err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatalf("write llama binary body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close llama tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close llama gzip: %v", err)
	}
	return buf.Bytes()
}

func buildManagedASRFFmpegTestWheel(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	body := []byte("#!/bin/sh\nif [ \"${1:-}\" = \"-version\" ]; then echo 'ffmpeg test'; exit 0; fi\nlast=\"\"\nfor arg in \"$@\"; do last=\"$arg\"; done\nprintf 'fake wav\\n' > \"$last\"\n")
	writer, err := zw.CreateHeader(&zip.FileHeader{
		Name:   "imageio_ffmpeg/binaries/ffmpeg-test",
		Method: zip.Deflate,
	})
	if err != nil {
		t.Fatalf("create ffmpeg zip entry: %v", err)
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatalf("write ffmpeg zip entry: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close ffmpeg zip: %v", err)
	}
	return buf.Bytes()
}

func buildManagedASRStandalonePythonTestArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	exeName := "python/bin/python3"
	if runtime.GOOS == "windows" {
		exeName = "python/python.exe"
	}
	if err := tw.WriteHeader(&tar.Header{Name: "python/", Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
		t.Fatalf("write dir header: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(filepath.Dir(exeName)) + "/", Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
		t.Fatalf("write bin dir header: %v", err)
	}
	body := []byte("#!/bin/sh\nexit 0\n")
	if runtime.GOOS == "windows" {
		body = []byte("fake exe")
	}
	if err := tw.WriteHeader(&tar.Header{Name: exeName, Mode: 0o700, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatalf("write exe header: %v", err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatalf("write exe body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return buf.Bytes()
}
