package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

const (
	DefaultManagedASRModelID          = "Qwen/Qwen3-ASR-0.6B"
	managedASRRuntimeVersion          = "qwen3-asr-runtime-v2"
	managedASRDefaultMinFreeBytes     = 8 * 1024 * 1024 * 1024
	managedASRStaleTempAge            = 24 * time.Hour
	managedASRBackendAuto             = "auto"
	managedASRBackendLlama            = "llama"
	managedASRBackendTransformers     = "qwen-asr-transformers"
	managedASRBackendTransformersMode = "transformers"
	managedASRRunnerScriptFileName    = "qwen3_asr_runner.py"
	managedASRLlamaPrompt             = "Transcribe the audio exactly. Do not translate. Preserve the spoken language. Preserve English words, acronyms, code identifiers, file names, and paths as spoken."
	managedASRLlamaContextSize        = "2048"
	managedASRLlamaMaxTokens          = "-1"
)

var managedASRRuntimePackages = []string{
	"qwen-asr==0.0.6",
	"imageio-ffmpeg==0.6.0",
	"torch>=2.4,<2.13",
}

var (
	managedASRLookPath                  = exec.LookPath
	validateManagedASRBootstrapPythonFn = validateManagedASRBootstrapPython
	ensureManagedASRStandalonePythonFn  = ensureManagedASRStandalonePython
)

type ManagedASRConfig struct {
	CacheRoot    string
	ModelID      string
	MinFreeBytes uint64
	Backend      string

	LlamaBinaryPath   string
	LlamaModelPath    string
	LlamaMMProjPath   string
	LlamaDevice       string
	FFmpegPath        string
	NativeLibraryPath string
}

type ManagedASRTranscriber struct {
	Config ManagedASRConfig

	ensureRuntime      managedASRRuntimeEnsurer
	ensureLlamaRuntime managedASRLlamaRuntimeEnsurer
	runCommand         managedASRCommandRunner
	runLlamaCommand    managedASRCommandRunner
}

type managedASRRuntime struct {
	Python     string
	ScriptPath string
	CacheRoot  string
	ModelID    string
	Env        []string
}

type managedASRLlamaRuntime struct {
	Command    string
	CacheRoot  string
	ModelID    string
	ModelPath  string
	MMProjPath string
	Device     string
	FFmpegPath string
	Env        []string
}

type managedASRBootstrapPython struct {
	Command string
	Args    []string
	Display string
}

type managedASRRuntimeEnsurer func(context.Context, ManagedASRConfig) (managedASRRuntime, error)

type managedASRLlamaRuntimeEnsurer func(context.Context, ManagedASRConfig) (managedASRLlamaRuntime, error)

type managedASRCommandRunner func(context.Context, string, []string, []string, *bytes.Buffer, *bytes.Buffer) error

type managedASRDiskSpaceError struct {
	Path      string
	NeedBytes uint64
	FreeBytes uint64
}

func (e managedASRDiskSpaceError) Error() string {
	return fmt.Sprintf(
		"not enough free disk space for Teams speech recognition setup at %s: %s available, need at least %s",
		e.Path,
		formatASRBytes(e.FreeBytes),
		formatASRBytes(e.NeedBytes),
	)
}

func NewManagedQwenASRTranscriber(config ...ManagedASRConfig) *ManagedASRTranscriber {
	var cfg ManagedASRConfig
	if len(config) > 0 {
		cfg = config[0]
	}
	return &ManagedASRTranscriber{
		Config:             cfg,
		ensureRuntime:      ensureManagedASRRuntime,
		ensureLlamaRuntime: ensureManagedASRLlamaRuntime,
		runCommand:         runManagedASRCommand,
		runLlamaCommand:    runManagedASRCommand,
	}
}

func (t *ManagedASRTranscriber) WarmUpTeamsASR(ctx context.Context) error {
	if t == nil {
		return errASRCommandNotConfigured
	}
	switch mode := managedASRBackendMode(t.Config.Backend); mode {
	case managedASRBackendLlama:
		return t.warmUpTeamsMediaLlama(ctx, t.Config)
	case managedASRBackendAuto:
		err := t.warmUpTeamsMediaLlama(ctx, t.Config)
		if err == nil {
			return nil
		}
		if !managedASRCanFallbackToTransformers(err) {
			return err
		}
		if fallbackErr := t.warmUpTeamsMediaTransformers(ctx, t.Config); fallbackErr != nil {
			return fmt.Errorf("llama ASR warm-up failed (%v); transformers warm-up failed: %w", err, fallbackErr)
		}
		return nil
	case managedASRBackendTransformersMode:
		return t.warmUpTeamsMediaTransformers(ctx, t.Config)
	default:
		return fmt.Errorf("unsupported managed Teams ASR backend %q", strings.TrimSpace(t.Config.Backend))
	}
}

func (t *ManagedASRTranscriber) warmUpTeamsMediaLlama(ctx context.Context, cfg ManagedASRConfig) error {
	ensureLlamaRuntime := t.ensureLlamaRuntime
	if ensureLlamaRuntime == nil {
		ensureLlamaRuntime = ensureManagedASRLlamaRuntime
	}
	runtime, err := ensureLlamaRuntime(ctx, cfg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(runtime.FFmpegPath) != "" {
		return nil
	}
	if _, err := resolveManagedASRLlamaFFmpeg(ctx, runtime.CacheRoot, "", runtime.Env); err != nil {
		wrapped := fmt.Errorf("llama ASR backend requires ffmpeg for Teams media preprocessing and speed=%s; install ffmpeg or set CODEX_HELPER_TEAMS_ASR_FFMPEG: %w", defaultASRSpeed, err)
		if errors.Is(wrapped, errManagedASRFFmpegUnsupported) {
			return managedASRLlamaFallbackError{Err: wrapped}
		}
		return wrapped
	}
	return nil
}

func (t *ManagedASRTranscriber) warmUpTeamsMediaTransformers(ctx context.Context, cfg ManagedASRConfig) error {
	ensureRuntime := t.ensureRuntime
	if ensureRuntime == nil {
		ensureRuntime = ensureManagedASRRuntime
	}
	_, err := ensureRuntime(ctx, cfg)
	return err
}

func (t *ManagedASRTranscriber) TranscribeTeamsMedia(ctx context.Context, input ASRTranscribeInput) (ASRTranscript, error) {
	if t == nil {
		return ASRTranscript{}, errASRCommandNotConfigured
	}
	sourcePath := strings.TrimSpace(input.File.Path)
	if sourcePath == "" {
		return ASRTranscript{}, fmt.Errorf("ASR source file path is empty")
	}
	switch mode := managedASRBackendMode(t.Config.Backend); mode {
	case managedASRBackendLlama:
		return t.transcribeTeamsMediaLlama(ctx, input)
	case managedASRBackendAuto:
		transcript, err := t.transcribeTeamsMediaLlama(ctx, input)
		if err == nil {
			return transcript, nil
		}
		if managedASRShouldRetryLlamaRunWithCPU(t.Config, err) {
			cpuConfig := t.Config
			cpuConfig.LlamaDevice = "cpu"
			cpuTranscript, cpuErr := t.transcribeTeamsMediaLlamaWithConfig(ctx, input, cpuConfig)
			if cpuErr == nil {
				if strings.TrimSpace(cpuTranscript.Warning) == "" {
					cpuTranscript.Warning = "accelerated llama ASR failed; retried with CPU"
				}
				return cpuTranscript, nil
			}
			err = fmt.Errorf("accelerated llama ASR backend failed (%v); CPU retry failed: %w", err, cpuErr)
		}
		if !managedASRCanFallbackToTransformers(err) {
			return ASRTranscript{}, err
		}
		fallback, fallbackErr := t.transcribeTeamsMediaTransformers(ctx, input)
		if fallbackErr != nil {
			return ASRTranscript{}, fmt.Errorf("llama ASR backend failed (%v); transformers fallback failed: %w", err, fallbackErr)
		}
		if strings.TrimSpace(fallback.Warning) == "" {
			fallback.Warning = "llama ASR backend failed; used qwen-asr-transformers fallback"
		}
		return fallback, nil
	case managedASRBackendTransformersMode:
		return t.transcribeTeamsMediaTransformers(ctx, input)
	default:
		return ASRTranscript{}, fmt.Errorf("unsupported managed Teams ASR backend %q", strings.TrimSpace(t.Config.Backend))
	}
}

func (t *ManagedASRTranscriber) transcribeTeamsMediaTransformers(ctx context.Context, input ASRTranscribeInput) (ASRTranscript, error) {
	ensureRuntime := t.ensureRuntime
	if ensureRuntime == nil {
		ensureRuntime = ensureManagedASRRuntime
	}
	runCommand := t.runCommand
	if runCommand == nil {
		runCommand = runManagedASRCommand
	}
	runtime, err := ensureRuntime(ctx, t.Config)
	if err != nil {
		return ASRTranscript{}, err
	}
	modelID := firstNonEmptyString(runtime.ModelID, t.Config.ModelID, DefaultManagedASRModelID)
	args := []string{
		runtime.ScriptPath,
		"--input", strings.TrimSpace(input.File.Path),
		"--language", managedASRLanguageArg(input.Language),
		"--speed", firstNonEmptyString(input.Speed, defaultASRSpeed),
		"--threads", strconv.Itoa(teamsASRMaxCPUThreads),
		"--model", modelID,
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	env := managedASREnv(runtime, input)
	if err := runCommand(ctx, runtime.Python, args, env, &stdout, &stderr); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if hint := managedASRNativeRuntimeFailureHint("transformers ASR", err, detail); hint != "" {
			if detail != "" {
				return ASRTranscript{}, fmt.Errorf("%w: %s: %s", err, hint, shortenTeamsLine(detail, 600))
			}
			return ASRTranscript{}, fmt.Errorf("%w: %s", err, hint)
		}
		if detail != "" {
			return ASRTranscript{}, fmt.Errorf("%w: %s", err, shortenTeamsLine(detail, 600))
		}
		return ASRTranscript{}, err
	}
	transcript := ASRTranscript{
		SourceIndex: input.SourceIndex,
		SourceName:  asrTranscriptDisplayName(ASRTranscript{SourceIndex: input.SourceIndex, SourcePath: firstNonEmptyString(input.File.PromptPath, input.File.Path)}),
		SourcePath:  firstNonEmptyString(input.File.PromptPath, input.File.Path),
		ContentType: input.File.ContentType,
		Language:    input.Language,
		Speed:       input.Speed,
		Model:       modelID,
		Backend:     managedASRBackendTransformers,
	}
	out := strings.TrimSpace(stdout.String())
	if out == "" {
		return transcript, nil
	}
	var decoded ASRTranscript
	if err := json.Unmarshal([]byte(out), &decoded); err == nil && asrTranscriptJSONLooksUsable(decoded) {
		return mergeASRTranscriptDefaults(decoded, transcript), nil
	}
	transcript.Text = out
	return transcript, nil
}

func (t *ManagedASRTranscriber) transcribeTeamsMediaLlama(ctx context.Context, input ASRTranscribeInput) (ASRTranscript, error) {
	return t.transcribeTeamsMediaLlamaWithConfig(ctx, input, t.Config)
}

func (t *ManagedASRTranscriber) transcribeTeamsMediaLlamaWithConfig(ctx context.Context, input ASRTranscribeInput, cfg ManagedASRConfig) (ASRTranscript, error) {
	ensureLlamaRuntime := t.ensureLlamaRuntime
	if ensureLlamaRuntime == nil {
		ensureLlamaRuntime = ensureManagedASRLlamaRuntime
	}
	runtime, err := ensureLlamaRuntime(ctx, cfg)
	if err != nil {
		return ASRTranscript{}, err
	}
	if err := os.MkdirAll(runtime.CacheRoot, 0o700); err != nil {
		return ASRTranscript{}, err
	}
	if err := cleanupManagedASRTemp(runtime.CacheRoot, time.Now(), managedASRStaleTempAge); err != nil {
		return ASRTranscript{}, err
	}
	audioPath, cleanup, err := prepareManagedASRAudioForLlama(ctx, strings.TrimSpace(input.File.Path), firstNonEmptyString(input.Speed, defaultASRSpeed), runtime)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return ASRTranscript{}, err
	}

	args := []string{
		"-m", runtime.ModelPath,
		"--mmproj", runtime.MMProjPath,
		"--audio", audioPath,
		"-p", managedASRLlamaPrompt,
		"-t", strconv.Itoa(teamsASRMaxCPUThreads),
		"-tb", strconv.Itoa(teamsASRMaxCPUThreads),
		"-c", managedASRLlamaContextSize,
		"-n", managedASRLlamaMaxTokens,
		"--temp", "0",
		"--no-warmup",
		"--verbosity", "1",
		"--no-log-prefix",
		"--no-log-timestamps",
	}
	if strings.EqualFold(strings.TrimSpace(runtime.Device), "cpu") {
		args = append(args, "--device", "none")
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	runCommand := t.runLlamaCommand
	if runCommand == nil {
		runCommand = runManagedASRCommand
	}
	env := managedASRLlamaEnv(runtime, input)
	if err := runCommand(ctx, runtime.Command, args, env, &stdout, &stderr); err != nil {
		detail := strings.TrimSpace(stderr.String())
		return ASRTranscript{}, managedASRLlamaCommandError(runtime, err, detail)
	}
	text, detectedLanguage := managedASRParseLlamaOutput(stdout.String())
	return ASRTranscript{
		SourceIndex: input.SourceIndex,
		SourceName:  asrTranscriptDisplayName(ASRTranscript{SourceIndex: input.SourceIndex, SourcePath: firstNonEmptyString(input.File.PromptPath, input.File.Path)}),
		SourcePath:  firstNonEmptyString(input.File.PromptPath, input.File.Path),
		ContentType: input.File.ContentType,
		Text:        text,
		Language:    firstNonEmptyString(detectedLanguage, input.Language),
		Speed:       input.Speed,
		Model:       runtime.ModelID,
		Backend:     managedASRLlamaBackendName(runtime.Device),
	}, nil
}

func managedASRBackendMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return managedASRBackendAuto
	case managedASRBackendTransformersMode, managedASRBackendTransformers, "torch", "qwen-asr":
		return managedASRBackendTransformersMode
	case managedASRBackendAuto:
		return managedASRBackendAuto
	case managedASRBackendLlama, "llama.cpp", "llamacpp", "qwen-asr-llama":
		return managedASRBackendLlama
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func managedASRCanFallbackFromLlama(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var fallback managedASRLlamaFallbackError
	return errors.As(err, &fallback)
}

func managedASRCanFallbackToTransformers(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var fallback managedASRLlamaFallbackError
	if !errors.As(err, &fallback) {
		return false
	}
	return managedASRNativeRuntimeFailureHint("llama.cpp", fallback.Err, "") == ""
}

type managedASRLlamaRunError struct {
	Runtime managedASRLlamaRuntime
	Err     error
}

func (e managedASRLlamaRunError) Error() string {
	if e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e managedASRLlamaRunError) Unwrap() error {
	return e.Err
}

func managedASRLlamaCommandError(runtime managedASRLlamaRuntime, err error, detail string) error {
	var wrapped error
	if hint := managedASRNativeRuntimeFailureHint("llama.cpp", err, detail); hint != "" {
		if detail != "" {
			wrapped = fmt.Errorf("%w: %s: %s", err, hint, shortenTeamsLine(detail, 600))
		} else {
			wrapped = fmt.Errorf("%w: %s", err, hint)
		}
	} else if detail != "" {
		wrapped = fmt.Errorf("%w: %s", err, shortenTeamsLine(detail, 600))
	} else {
		wrapped = err
	}
	return managedASRLlamaRunError{Runtime: runtime, Err: wrapped}
}

func managedASRShouldRetryLlamaRunWithCPU(cfg ManagedASRConfig, err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if device := strings.ToLower(strings.TrimSpace(cfg.LlamaDevice)); device != "" && device != managedASRBackendAuto {
		return false
	}
	var runErr managedASRLlamaRunError
	if !errors.As(err, &runErr) {
		return false
	}
	device := strings.ToLower(strings.TrimSpace(runErr.Runtime.Device))
	return device != "" && device != managedASRBackendAuto && device != "cpu" && device != "none"
}

func managedASRLanguageArg(language string) string {
	language = strings.TrimSpace(language)
	if language == "" {
		return defaultASRLanguage
	}
	return language
}

func managedASREnv(runtime managedASRRuntime, input ASRTranscribeInput) []string {
	cacheRoot := strings.TrimSpace(runtime.CacheRoot)
	extra := append([]string(nil), runtime.Env...)
	if cacheRoot != "" {
		tmpRoot := filepath.Join(cacheRoot, "tmp")
		extra = append(extra,
			"HF_HOME="+filepath.Join(cacheRoot, "huggingface"),
			"HUGGINGFACE_HUB_CACHE="+filepath.Join(cacheRoot, "huggingface", "hub"),
			"TRANSFORMERS_CACHE="+filepath.Join(cacheRoot, "huggingface", "transformers"),
			"XDG_CACHE_HOME="+filepath.Join(cacheRoot, "xdg-cache"),
			"CODEX_HELPER_TEAMS_ASR_TMP="+tmpRoot,
			"TMPDIR="+tmpRoot,
			"TEMP="+tmpRoot,
			"TMP="+tmpRoot,
		)
	}
	extra = append(extra, "PYTHONIOENCODING=utf-8", "PYTHONNOUSERSITE=1")
	return asrCommandEnv(managedASRSetupBaseEnv(), extra, input)
}

func prepareManagedASRAudioForLlama(ctx context.Context, path string, speed string, runtime managedASRLlamaRuntime) (string, func(), error) {
	factor := managedASRSpeedFactor(speed)
	if factor == 1.0 && managedASRLlamaCanReadDirectly(path) {
		return path, nil, nil
	}
	ffmpeg := strings.TrimSpace(runtime.FFmpegPath)
	var err error
	if ffmpeg == "" {
		ffmpeg, err = resolveManagedASRLlamaFFmpeg(ctx, runtime.CacheRoot, "", runtime.Env)
		if err != nil || strings.TrimSpace(ffmpeg) == "" {
			baseErr := err
			if baseErr == nil {
				baseErr = errors.New("ffmpeg path is empty")
			}
			wrapped := fmt.Errorf("llama ASR backend requires ffmpeg for Teams media preprocessing and speed=%s; install ffmpeg or set CODEX_HELPER_TEAMS_ASR_FFMPEG: %w", firstNonEmptyString(speed, defaultASRSpeed), baseErr)
			if errors.Is(wrapped, errManagedASRFFmpegUnsupported) {
				return "", nil, managedASRLlamaFallbackError{Err: wrapped}
			}
			return "", nil, wrapped
		}
	}
	tmpRoot := filepath.Join(runtime.CacheRoot, "tmp")
	if err := os.MkdirAll(tmpRoot, 0o700); err != nil {
		return "", nil, err
	}
	tmpDir, err := os.MkdirTemp(tmpRoot, "transcribe-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }
	out := filepath.Join(tmpDir, "input.wav")
	filter := "aresample=16000"
	if factor != 1.0 {
		filter = fmt.Sprintf("atempo=%.6g,%s", factor, filter)
	}
	cmd := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner",
		"-loglevel", "error",
		"-y",
		"-i", path,
		"-filter:a", filter,
		"-ac", "1",
		"-ar", "16000",
		out,
	)
	cmd.Env = append(managedASRSetupBaseEnv(), runtime.Env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := runASRCommand(ctx, cmd); err != nil {
		cleanup()
		detail := strings.TrimSpace(stderr.String())
		if hint := managedASRNativeRuntimeFailureHint("ffmpeg", err, detail); hint != "" {
			if detail != "" {
				return "", nil, fmt.Errorf("prepare llama ASR audio with ffmpeg: %w: %s: %s", err, hint, shortenTeamsLine(detail, 600))
			}
			return "", nil, fmt.Errorf("prepare llama ASR audio with ffmpeg: %w: %s", err, hint)
		}
		if detail != "" {
			return "", nil, fmt.Errorf("prepare llama ASR audio with ffmpeg: %w: %s", err, shortenTeamsLine(detail, 600))
		}
		return "", nil, fmt.Errorf("prepare llama ASR audio with ffmpeg: %w", err)
	}
	return out, cleanup, nil
}

func managedASRSpeedFactor(value string) float64 {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimSuffix(value, "x")
	factor, err := strconv.ParseFloat(value, 64)
	if err != nil || factor <= 0 {
		return 1.0
	}
	return factor
}

func managedASRLlamaCanReadDirectly(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".wav", ".mp3", ".flac":
		return true
	default:
		return false
	}
}

func managedASRLlamaEnv(rt managedASRLlamaRuntime, input ASRTranscribeInput) []string {
	extra := append([]string(nil), rt.Env...)
	if wslLib := "/usr/lib/wsl/lib"; runtime.GOOS == "linux" {
		if _, err := os.Stat(filepath.Join(wslLib, "libcuda.so.1")); err == nil {
			extra = prependASREnvPath(extra, "LD_LIBRARY_PATH", wslLib)
		}
	}
	return asrCommandEnv(managedASRSetupBaseEnv(), extra, input)
}

func prependASREnvPath(extra []string, key string, path string) []string {
	key = strings.TrimSpace(key)
	path = strings.TrimSpace(path)
	if key == "" || path == "" {
		return extra
	}
	for i, item := range extra {
		existingKey, existingValue, ok := strings.Cut(item, "=")
		if !ok || existingKey != key {
			continue
		}
		if existingValue == "" {
			extra[i] = key + "=" + path
		} else {
			extra[i] = key + "=" + path + string(os.PathListSeparator) + existingValue
		}
		return extra
	}
	value := path
	if existing := strings.TrimSpace(os.Getenv(key)); existing != "" {
		value += string(os.PathListSeparator) + existing
	}
	return append(extra, key+"="+value)
}

func managedASRParseLlamaOutput(value string) (string, string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", ""
	}
	const marker = "<asr_text>"
	idx := strings.Index(value, marker)
	if idx < 0 {
		return value, ""
	}
	prefix := strings.TrimSpace(value[:idx])
	text := strings.TrimSpace(value[idx+len(marker):])
	return text, managedASRParseLlamaLanguage(prefix)
}

func managedASRParseLlamaLanguage(prefix string) string {
	language := ""
	for _, line := range strings.Split(prefix, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToLower(line), "language") {
			continue
		}
		language = strings.TrimSpace(line[len("language"):])
		language = strings.TrimSpace(strings.TrimPrefix(language, ":"))
		if language != "" {
			return language
		}
	}
	return language
}

func managedASRLlamaBackendName(device string) string {
	device = strings.ToLower(strings.TrimSpace(device))
	if device == "" {
		device = managedASRBackendAuto
	}
	device = strings.ReplaceAll(device, "/", "-")
	return "qwen-asr-llama/" + device
}

func managedASRNativeRuntimeFailureHint(component string, err error, detail string) string {
	statusText := strings.ToLower(strings.TrimSpace(err.Error() + " " + detail))
	if exitCode, ok := managedASRCommandExitCode(err); ok {
		switch uint32(exitCode) {
		case 0xC0000135:
			return component + " failed to start because a Windows native DLL is missing; provide app-local C/C++ runtime DLLs through the helper ASR native library path, or install the matching Microsoft Visual C++ runtime"
		case 0xC0000139:
			return component + " failed to start because a Windows native DLL entry point is missing; update the C/C++ runtime or use a binary compatible with this Windows version"
		case 0xC000007B:
			return component + " failed to start because the native binary or one of its DLLs has the wrong architecture or is corrupt; verify x64/ARM64 compatibility"
		case 0xC000001D:
			return component + " failed because this CPU does not support an instruction required by the native binary; use a lower-ISA compatible runtime"
		}
	}
	switch {
	case strings.Contains(statusText, "0xc0000135") || strings.Contains(statusText, "status_dll_not_found"):
		return component + " failed to start because a Windows native DLL is missing; provide app-local C/C++ runtime DLLs through the helper ASR native library path, or install the matching Microsoft Visual C++ runtime"
	case strings.Contains(statusText, "0xc0000139") || strings.Contains(statusText, "status_entrypoint_not_found"):
		return component + " failed to start because a Windows native DLL entry point is missing; update the C/C++ runtime or use a binary compatible with this Windows version"
	case strings.Contains(statusText, "0xc000007b") || strings.Contains(statusText, "status_invalid_image_format"):
		return component + " failed to start because the native binary or one of its DLLs has the wrong architecture or is corrupt; verify x64/ARM64 compatibility"
	case strings.Contains(statusText, "0xc000001d") || strings.Contains(statusText, "status_illegal_instruction"):
		return component + " failed because this CPU does not support an instruction required by the native binary; use a lower-ISA compatible runtime"
	case strings.Contains(statusText, "error while loading shared libraries"):
		return component + " could not load a required Linux shared library; provide the library through the helper ASR native library path or use a compatible runtime bundle"
	case strings.Contains(statusText, "glibc_") && strings.Contains(statusText, "not found"):
		return component + " requires a newer glibc than this system provides; use a low-glibc compatible runtime bundle instead of a library path workaround"
	case (strings.Contains(statusText, "glibcxx_") || strings.Contains(statusText, "cxxabi_")) && strings.Contains(statusText, "not found"):
		return component + " requires a newer C++ runtime than this system provides; use a compatible bundled runtime or provide app-local C++ libraries"
	case strings.Contains(statusText, "no such file or directory") && runtime.GOOS == "linux":
		return component + " may be missing the Linux dynamic loader or may not be compatible with this distribution; use a compatible glibc or musl runtime"
	case strings.Contains(statusText, "illegal instruction"):
		return component + " failed because this CPU does not support an instruction required by the native binary; use a lower-ISA compatible runtime"
	case strings.Contains(statusText, "library not loaded") || strings.Contains(statusText, "image not found"):
		return component + " could not load a required macOS dynamic library; provide it through the helper ASR native library path or use a bundled runtime"
	case strings.Contains(statusText, "symbol not found"):
		return component + " requires a macOS runtime symbol that is not available on this system; use a binary compatible with this macOS version"
	case strings.Contains(statusText, "bad cpu type"):
		return component + " is not compatible with this Mac architecture; use the matching arm64 or x64 runtime"
	case strings.Contains(statusText, "dll load failed") || strings.Contains(statusText, "winerror 126"):
		return component + " could not load a required Windows native DLL; provide app-local runtime DLLs through the helper ASR native library path or use a compatible runtime"
	default:
		return ""
	}
}

func managedASRCommandExitCode(err error) (int, bool) {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), true
	}
	return 0, false
}

func runManagedASRCommand(ctx context.Context, command string, args []string, env []string, stdout *bytes.Buffer, stderr *bytes.Buffer) error {
	cmd := exec.Command(strings.TrimSpace(command), args...)
	cmd.Env = env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return runASRCommand(ctx, cmd)
}

func ensureManagedASRRuntime(ctx context.Context, cfg ManagedASRConfig) (managedASRRuntime, error) {
	resolved, err := resolveManagedASRConfig(cfg)
	if err != nil {
		return managedASRRuntime{}, err
	}
	nativeLibraryDirs, err := managedASRConfiguredNativeLibraryDirs(resolved.NativeLibraryPath)
	if err != nil {
		return managedASRRuntime{}, err
	}
	if err := os.MkdirAll(resolved.CacheRoot, 0o700); err != nil {
		return managedASRRuntime{}, err
	}
	if err := os.Chmod(resolved.CacheRoot, 0o700); err != nil {
		return managedASRRuntime{}, err
	}
	if err := ensureManagedASRDiskSpace(resolved.CacheRoot, resolved.MinFreeBytes); err != nil {
		return managedASRRuntime{}, err
	}
	lock := flock.New(filepath.Join(resolved.CacheRoot, "runtime.lock"))
	locked, err := lock.TryLockContext(ctx, time.Second)
	if err != nil {
		return managedASRRuntime{}, err
	}
	if !locked {
		return managedASRRuntime{}, fmt.Errorf("Teams speech recognition setup is already running in another helper process")
	}
	defer func() { _ = lock.Unlock() }()

	if err := cleanupManagedASRTemp(resolved.CacheRoot, time.Now(), managedASRStaleTempAge); err != nil {
		return managedASRRuntime{}, err
	}
	scriptPath, err := ensureManagedASRRunnerScript(resolved.CacheRoot)
	if err != nil {
		return managedASRRuntime{}, err
	}
	venvPython := managedASRVenvPython(resolved.CacheRoot)
	if _, err := os.Stat(venvPython); err != nil {
		if err := createManagedASRVenv(ctx, resolved.CacheRoot); err != nil {
			return managedASRRuntime{}, err
		}
	}
	markerPath := filepath.Join(resolved.CacheRoot, "runtime.json")
	if !managedASRRuntimeMarkerCurrent(markerPath) {
		if err := installManagedASRPackages(ctx, venvPython); err != nil {
			return managedASRRuntime{}, err
		}
		if err := writeManagedASRRuntimeMarker(markerPath); err != nil {
			return managedASRRuntime{}, err
		}
	}
	return managedASRRuntime{
		Python:     venvPython,
		ScriptPath: scriptPath,
		CacheRoot:  resolved.CacheRoot,
		ModelID:    resolved.ModelID,
		Env:        managedASRMergeNativeLibraryEnv(nil, nativeLibraryDirs),
	}, nil
}

func resolveManagedASRConfig(cfg ManagedASRConfig) (ManagedASRConfig, error) {
	cfg.CacheRoot = strings.TrimSpace(cfg.CacheRoot)
	if cfg.CacheRoot == "" {
		root, err := defaultManagedASRCacheRoot()
		if err != nil {
			return ManagedASRConfig{}, err
		}
		cfg.CacheRoot = root
	}
	if !filepath.IsAbs(cfg.CacheRoot) {
		abs, err := filepath.Abs(cfg.CacheRoot)
		if err != nil {
			return ManagedASRConfig{}, err
		}
		cfg.CacheRoot = abs
	}
	cfg.ModelID = firstNonEmptyString(cfg.ModelID, DefaultManagedASRModelID)
	if cfg.MinFreeBytes == 0 {
		cfg.MinFreeBytes = managedASRDefaultMinFreeBytes
	}
	return cfg, nil
}

func defaultManagedASRCacheRoot() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams-asr", safePathPart(strings.ToLower(DefaultManagedASRModelID))), nil
}

func ensureManagedASRDiskSpace(path string, minFreeBytes uint64) error {
	if minFreeBytes == 0 {
		return nil
	}
	freeBytes, err := managedASRDiskFreeBytes(path)
	if err != nil {
		return fmt.Errorf("could not check free disk space for Teams speech recognition setup at %s: %w", path, err)
	}
	if freeBytes < minFreeBytes {
		return managedASRDiskSpaceError{Path: path, NeedBytes: minFreeBytes, FreeBytes: freeBytes}
	}
	return nil
}

func ensureManagedASRRunnerScript(cacheRoot string) (string, error) {
	scriptDir := filepath.Join(cacheRoot, "scripts")
	if err := os.MkdirAll(scriptDir, 0o700); err != nil {
		return "", err
	}
	scriptPath := filepath.Join(scriptDir, managedASRRunnerScriptFileName)
	content := []byte(managedQwenASRRunnerScript)
	hash := sha256.Sum256(content)
	hashPath := scriptPath + ".sha256"
	wantHash := hex.EncodeToString(hash[:])
	if current, err := os.ReadFile(hashPath); err == nil && strings.TrimSpace(string(current)) == wantHash {
		if _, err := os.Stat(scriptPath); err == nil {
			return scriptPath, nil
		}
	}
	if err := writePrivateFileReplacing(scriptPath, content, 0o700); err != nil {
		return "", err
	}
	if err := writePrivateFileReplacing(hashPath, []byte(wantHash+"\n"), 0o600); err != nil {
		return "", err
	}
	return scriptPath, nil
}

func createManagedASRVenv(ctx context.Context, cacheRoot string) error {
	python, err := findManagedASRBootstrapPython(ctx, cacheRoot)
	if err != nil {
		return err
	}
	venvDir := filepath.Join(cacheRoot, "venv")
	staging := filepath.Join(cacheRoot, "tmp", "venv-staging-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	if err := os.MkdirAll(filepath.Dir(staging), 0o700); err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(staging) }()
	args := appendManagedASRBootstrapArgs(python, "-m", "venv", staging)
	if err := runManagedASRSetupCommand(ctx, python.Command, args, managedASRSetupBaseEnv()); err != nil {
		return fmt.Errorf("create isolated Teams speech recognition Python environment: %w", err)
	}
	_ = os.RemoveAll(venvDir)
	return os.Rename(staging, venvDir)
}

func installManagedASRPackages(ctx context.Context, python string) error {
	env := asrCommandEnv(managedASRSetupBaseEnv(), []string{
		"PIP_DISABLE_PIP_VERSION_CHECK=1",
		"PIP_NO_CACHE_DIR=1",
		"PIP_NO_INPUT=1",
		"PYTHONNOUSERSITE=1",
	}, ASRTranscribeInput{Language: defaultASRLanguage, Speed: defaultASRSpeed})
	if err := runManagedASRSetupCommand(ctx, python, []string{"-m", "pip", "install", "--upgrade", "pip"}, env); err != nil {
		return fmt.Errorf("upgrade isolated Teams speech recognition installer: %w", err)
	}
	if err := runManagedASRSetupCommand(ctx, python, managedASRPackageInstallArgs(), env); err != nil {
		return fmt.Errorf("install Teams speech recognition runtime packages: %w", err)
	}
	return nil
}

func managedASRSetupBaseEnv() []string {
	env := envSliceToMap(os.Environ())
	for _, key := range []string{
		"PYTHONHOME",
		"PYTHONPATH",
		"VIRTUAL_ENV",
		"CONDA_PREFIX",
		"CONDA_DEFAULT_ENV",
	} {
		delete(env, key)
	}
	return envMapToSlice(env)
}

func managedASRPackageInstallArgs() []string {
	args := []string{"-m", "pip", "install", "--upgrade", "--only-binary=:all:"}
	args = append(args, managedASRRuntimePackages...)
	return args
}

func runManagedASRSetupCommand(ctx context.Context, command string, args []string, env []string) error {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(strings.TrimSpace(command), args...)
	cmd.Env = env
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := runASRCommand(ctx, cmd); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return fmt.Errorf("%w: %s", err, shortenTeamsLine(detail, 600))
		}
		return err
	}
	return nil
}

func findManagedASRBootstrapPython(ctx context.Context, cacheRoot string) (managedASRBootstrapPython, error) {
	var errors []string
	for _, candidate := range managedASRBootstrapPythonCandidates(runtime.GOOS) {
		path, err := managedASRLookPath(candidate.Command)
		if err != nil {
			continue
		}
		candidate.Command = path
		if candidate.Display == "" {
			candidate.Display = filepath.Base(path)
		}
		if err := validateManagedASRBootstrapPythonFn(candidate); err != nil {
			errors = append(errors, err.Error())
			continue
		}
		return candidate, nil
	}
	managed, managedErr := ensureManagedASRStandalonePythonFn(ctx, cacheRoot)
	if managedErr == nil {
		return managed, nil
	}
	if len(errors) > 0 {
		return managedASRBootstrapPython{}, fmt.Errorf("could not prepare isolated Python 3.10+ for managed Teams speech recognition setup: system Python candidates failed (%s); managed Python setup failed: %w", strings.Join(errors, "; "), managedErr)
	}
	return managedASRBootstrapPython{}, fmt.Errorf("could not prepare isolated Python 3.10+ for managed Teams speech recognition setup: no usable system Python 3.10+ was found on PATH; managed Python setup failed: %w", managedErr)
}

func managedASRBootstrapPythonCandidates(goos string) []managedASRBootstrapPython {
	if goos == "windows" {
		return []managedASRBootstrapPython{
			{Command: "py", Args: []string{"-3"}, Display: "py -3"},
			{Command: "python", Display: "python"},
		}
	}
	return []managedASRBootstrapPython{
		{Command: "python3", Display: "python3"},
		{Command: "python", Display: "python"},
	}
}

func validateManagedASRBootstrapPython(python managedASRBootstrapPython) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := appendManagedASRBootstrapArgs(python, "-c", "import sys, venv, ensurepip; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)")
	cmd := exec.CommandContext(ctx, python.Command, args...)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	detail := strings.TrimSpace(string(output))
	if detail != "" {
		return fmt.Errorf("%s is not a usable Python 3.10+ venv runtime: %s", python.Display, shortenTeamsLine(detail, 240))
	}
	return fmt.Errorf("%s is not a usable Python 3.10+ venv runtime", python.Display)
}

func appendManagedASRBootstrapArgs(python managedASRBootstrapPython, args ...string) []string {
	out := append([]string(nil), python.Args...)
	out = append(out, args...)
	return out
}

func managedASRVenvPython(cacheRoot string) string {
	if runtime.GOOS == "windows" {
		return filepath.Join(cacheRoot, "venv", "Scripts", "python.exe")
	}
	return filepath.Join(cacheRoot, "venv", "bin", "python")
}

func managedASRRuntimeMarkerCurrent(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var marker struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(data, &marker); err != nil {
		return false
	}
	return marker.Version == managedASRRuntimeVersion
}

func writeManagedASRRuntimeMarker(path string) error {
	data, err := json.MarshalIndent(struct {
		Version   string `json:"version"`
		UpdatedAt string `json:"updated_at"`
	}{
		Version:   managedASRRuntimeVersion,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}, "", "  ")
	if err != nil {
		return err
	}
	return writePrivateFileReplacing(path, append(data, '\n'), 0o600)
}

func cleanupManagedASRTemp(cacheRoot string, now time.Time, maxAge time.Duration) error {
	tmpRoot := filepath.Join(cacheRoot, "tmp")
	if err := os.MkdirAll(tmpRoot, 0o700); err != nil {
		return err
	}
	if maxAge <= 0 {
		return nil
	}
	entries, err := os.ReadDir(tmpRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) < maxAge {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "venv-staging-") || strings.HasPrefix(name, "transcribe-") || strings.HasPrefix(name, "ffmpeg-") {
			_ = os.RemoveAll(filepath.Join(tmpRoot, name))
		}
	}
	return nil
}

func writePrivateFileReplacing(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return err
	}
	return replaceFile(tmpPath, path)
}

func replaceFile(oldPath string, newPath string) error {
	if runtime.GOOS == "windows" {
		_ = os.Remove(newPath)
	}
	return os.Rename(oldPath, newPath)
}

func formatASRBytes(bytes uint64) string {
	const mib = 1024 * 1024
	if bytes < mib {
		return fmt.Sprintf("%d bytes", bytes)
	}
	return fmt.Sprintf("%.1f MiB", float64(bytes)/float64(mib))
}

const managedQwenASRRunnerScript = `#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import tempfile


def _speed_factor(value):
    value = (value or "").strip().lower()
    if value.endswith("x"):
        value = value[:-1]
    try:
        factor = float(value)
    except ValueError:
        return 1.0
    if factor <= 0:
        return 1.0
    return factor


def _qwen_language(value):
    value = (value or "").strip()
    if not value or value.lower() == "auto":
        return None
    mapping = {
        "zh": "Chinese",
        "cn": "Chinese",
        "en": "English",
        "yue": "Cantonese",
    }
    return mapping.get(value.lower(), value)


def _device_and_dtype():
    import torch

    if torch.cuda.is_available():
        return "cuda:0", torch.bfloat16
    mps = getattr(getattr(torch, "backends", None), "mps", None)
    if mps is not None and mps.is_available():
        return "mps", torch.float16
    return "cpu", torch.float32


def _prepare_audio(path, speed):
    factor = _speed_factor(speed)
    if abs(factor - 1.0) < 0.001:
        return path, None
    import imageio_ffmpeg

    tmp_base = os.environ.get("CODEX_HELPER_TEAMS_ASR_TMP") or None
    if tmp_base:
        os.makedirs(tmp_base, exist_ok=True)
    tmpdir = tempfile.mkdtemp(prefix="transcribe-", dir=tmp_base)
    out = os.path.join(tmpdir, "input.wav")
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            path,
            "-filter:a",
            "atempo=%.6g" % factor,
            "-ac",
            "1",
            "-ar",
            "16000",
            out,
        ],
        check=True,
    )
    return out, tmpdir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--language", default="auto")
    parser.add_argument("--speed", default="1.25x")
    parser.add_argument("--threads", default="4")
    parser.add_argument("--model", default="Qwen/Qwen3-ASR-0.6B")
    args = parser.parse_args()

    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    for key in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "TORCH_NUM_THREADS",
    ):
        os.environ[key] = str(args.threads)

    import torch
    from qwen_asr import Qwen3ASRModel

    device, dtype = _device_and_dtype()
    audio, tmpdir = _prepare_audio(args.input, args.speed)
    try:
        model = Qwen3ASRModel.from_pretrained(
            args.model,
            dtype=dtype,
            device_map=device,
            max_inference_batch_size=1,
            max_new_tokens=512,
        )
        results = model.transcribe(audio=audio, language=_qwen_language(args.language))
        result = results[0] if results else None
        text = getattr(result, "text", "") if result is not None else ""
        language = getattr(result, "language", "") if result is not None else ""
        print(
            json.dumps(
                {
                    "text": text,
                    "language": language or args.language,
                    "model": args.model,
                    "backend": "qwen-asr/" + device,
                    "speed": args.speed,
                },
                ensure_ascii=False,
            )
        )
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
`
