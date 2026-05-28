package teams

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
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
	transcriber := NewManagedQwenASRTranscriber(ManagedASRConfig{CacheRoot: t.TempDir()})
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
