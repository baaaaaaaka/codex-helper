package teams

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestManagedQwenASRTranscriberInvokesIsolatedRuntimeWithDefaults(t *testing.T) {
	cacheRoot := t.TempDir()
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
		env["CUSTOM_ASR_ENV"] != "1" {
		t.Fatalf("managed cache/env not isolated: %#v", env)
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
		"qwen-asr==0.0.6",
		"imageio-ffmpeg==0.6.0",
		"torch>=2.4,<2.13",
	)
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
