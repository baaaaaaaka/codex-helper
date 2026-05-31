package teams

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	managedASRLlamaRuntimeVersion = "llama-cpp-qwen3-asr-runtime-v2"
	managedASRLlamaReleaseTag     = "b9437"
	managedASRLlamaDownloadBase   = "https://github.com/ggml-org/llama.cpp/releases/download"
	managedASRLlamaBinaryFileName = "llama-mtmd-cli"

	managedASRLlamaModelRepo     = "ggml-org/Qwen3-ASR-0.6B-GGUF"
	managedASRLlamaModelRevision = "928ab958557df9aa2ef1c93e0e83c7ad0933fae2"
	managedASRLlamaModelFile     = "Qwen3-ASR-0.6B-Q8_0.gguf"
	managedASRLlamaModelSHA256   = "bca259818b50ca7c4c05e9bdb35a5dc04fa039653a6d6f3f0f331f96f6aa1971"
	managedASRLlamaModelSize     = int64(804749248)
	managedASRLlamaMMProjFile    = "mmproj-Qwen3-ASR-0.6B-Q8_0.gguf"
	managedASRLlamaMMProjSHA256  = "41a342b5e4c514e968cb756de6cd1b7be39eff43c44c57a2ef5fc6522e36603d"
	managedASRLlamaMMProjSize    = int64(214392480)

	managedASRFFmpegRuntimeVersion = "imageio-ffmpeg-0.6.0-runtime-v1"
	managedASRFFmpegVersion        = "0.6.0"
)

var (
	managedASRLlamaHTTPClient           = http.DefaultClient
	managedASRLlamaBinaryAssetsFn       = managedASRLlamaBinaryAssets
	managedASRLlamaManagedModelAssetsFn = managedASRLlamaManagedModelAssets
	managedASRFFmpegWheelAssetFn        = managedASRFFmpegWheelAsset
	validateManagedASRFFmpegBinaryFn    = validateManagedASRFFmpegBinary
	validateManagedASRLlamaBinaryFn     = validateManagedASRLlamaBinary
	managedASRLlamaDownloadBackoff      = time.Second
	managedASRLlamaDownloadTimeout      = 15 * time.Minute
)

var errManagedASRFFmpegUnsupported = errors.New("managed ffmpeg is not available on this platform")

const (
	managedASRLlamaDownloadAttempts    = 3
	managedASRLlamaDefaultMinFreeBytes = 2 * 1024 * 1024 * 1024
)

type managedASRLlamaAsset struct {
	Name         string
	URL          string
	SHA256       string
	Size         int64
	ArchiveKind  string
	Acceleration string
}

type managedASRLlamaBinaryMarker struct {
	Version      string `json:"version"`
	ReleaseTag   string `json:"release_tag"`
	AssetName    string `json:"asset_name"`
	AssetURL     string `json:"asset_url"`
	AssetSHA256  string `json:"asset_sha256"`
	BinaryRel    string `json:"binary_rel"`
	BinarySize   int64  `json:"binary_size,omitempty"`
	Acceleration string `json:"acceleration"`
	UpdatedAt    string `json:"updated_at"`
}

type managedASRLlamaModelMarker struct {
	Version       string `json:"version"`
	ModelRepo     string `json:"model_repo"`
	ModelRevision string `json:"model_revision"`
	ModelFile     string `json:"model_file"`
	ModelSHA256   string `json:"model_sha256"`
	ModelSize     int64  `json:"model_size,omitempty"`
	MMProjFile    string `json:"mmproj_file"`
	MMProjSHA256  string `json:"mmproj_sha256"`
	MMProjSize    int64  `json:"mmproj_size,omitempty"`
	UpdatedAt     string `json:"updated_at"`
}

type managedASRLlamaFileAsset struct {
	Name   string
	URL    string
	SHA256 string
	Size   int64
}

type managedASRLlamaModelAssets struct {
	RootName string
	Repo     string
	Revision string
	Model    managedASRLlamaFileAsset
	MMProj   managedASRLlamaFileAsset
}

type managedASRFFmpegMarker struct {
	Version     string `json:"version"`
	FFmpegRel   string `json:"ffmpeg_rel"`
	AssetName   string `json:"asset_name"`
	AssetURL    string `json:"asset_url"`
	AssetSHA256 string `json:"asset_sha256"`
	UpdatedAt   string `json:"updated_at"`
}

type managedASRLlamaFallbackError struct {
	Err error
}

func (e managedASRLlamaFallbackError) Error() string {
	return e.Err.Error()
}

func (e managedASRLlamaFallbackError) Unwrap() error {
	return e.Err
}

type managedASRLlamaValidationError struct {
	Err error
}

func (e managedASRLlamaValidationError) Error() string {
	return e.Err.Error()
}

func (e managedASRLlamaValidationError) Unwrap() error {
	return e.Err
}

func ensureManagedASRLlamaRuntime(ctx context.Context, cfg ManagedASRConfig) (managedASRLlamaRuntime, error) {
	resolved, err := resolveManagedASRLlamaConfig(cfg)
	if err != nil {
		return managedASRLlamaRuntime{}, err
	}
	if err := os.MkdirAll(resolved.CacheRoot, 0o700); err != nil {
		return managedASRLlamaRuntime{}, err
	}
	if err := os.Chmod(resolved.CacheRoot, 0o700); err != nil {
		return managedASRLlamaRuntime{}, err
	}
	if err := ensureManagedASRDiskSpace(resolved.CacheRoot, resolved.MinFreeBytes); err != nil {
		return managedASRLlamaRuntime{}, err
	}

	lock := flock.New(filepath.Join(resolved.CacheRoot, "runtime.lock"))
	locked, err := lock.TryLockContext(ctx, time.Second)
	if err != nil {
		return managedASRLlamaRuntime{}, err
	}
	if !locked {
		return managedASRLlamaRuntime{}, fmt.Errorf("Teams speech recognition setup is already running in another helper process")
	}
	defer func() { _ = lock.Unlock() }()

	if err := cleanupManagedASRTemp(resolved.CacheRoot, time.Now(), managedASRStaleTempAge); err != nil {
		return managedASRLlamaRuntime{}, err
	}

	llamaRoot := filepath.Join(resolved.CacheRoot, "llama")
	if err := os.MkdirAll(llamaRoot, 0o700); err != nil {
		return managedASRLlamaRuntime{}, err
	}
	command, runtimeEnv, acceleration, err := ensureManagedASRLlamaBinary(ctx, llamaRoot, cfg)
	if err != nil {
		return managedASRLlamaRuntime{}, err
	}
	modelPath, mmprojPath, err := ensureManagedASRLlamaModelFiles(ctx, llamaRoot, cfg)
	if err != nil {
		return managedASRLlamaRuntime{}, err
	}
	return managedASRLlamaRuntime{
		Command:    command,
		CacheRoot:  llamaRoot,
		ModelID:    resolved.ModelID,
		ModelPath:  modelPath,
		MMProjPath: mmprojPath,
		Device:     firstNonEmptyString(acceleration, strings.TrimSpace(cfg.LlamaDevice), managedASRBackendAuto),
		FFmpegPath: strings.TrimSpace(cfg.FFmpegPath),
		Env:        runtimeEnv,
	}, nil
}

func resolveManagedASRLlamaConfig(cfg ManagedASRConfig) (ManagedASRConfig, error) {
	defaultMinFreeBytes := cfg.MinFreeBytes == 0
	resolved, err := resolveManagedASRConfig(cfg)
	if err != nil {
		return ManagedASRConfig{}, err
	}
	if defaultMinFreeBytes {
		resolved.MinFreeBytes = managedASRLlamaDefaultMinFreeBytes
	}
	return resolved, nil
}

func managedASRLlamaAssetsForDevice(assets []managedASRLlamaAsset, device string) ([]managedASRLlamaAsset, error) {
	device = strings.ToLower(strings.TrimSpace(device))
	switch device {
	case "", managedASRBackendAuto:
		return assets, nil
	case "none":
		device = "cpu"
	}
	var filtered []managedASRLlamaAsset
	for _, asset := range assets {
		if strings.EqualFold(asset.Acceleration, device) {
			filtered = append(filtered, asset)
		}
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("managed llama.cpp Teams speech recognition runtime with %s acceleration is not available for %s/%s", device, runtime.GOOS, runtime.GOARCH)
	}
	return filtered, nil
}

func managedASRLlamaAccelerationMatchesDevice(acceleration string, device string) bool {
	device = strings.ToLower(strings.TrimSpace(device))
	if device == "" || device == managedASRBackendAuto {
		return true
	}
	if device == "none" {
		device = "cpu"
	}
	return strings.EqualFold(strings.TrimSpace(acceleration), device)
}

func managedASRLlamaInstallErrorFallbackable(err error) bool {
	var validation managedASRLlamaValidationError
	return errors.As(err, &validation)
}

func ensureManagedASRLlamaBinary(ctx context.Context, llamaRoot string, cfg ManagedASRConfig) (string, []string, string, error) {
	command := strings.TrimSpace(cfg.LlamaBinaryPath)
	if command != "" {
		if _, err := os.Stat(command); err != nil {
			return "", nil, "", fmt.Errorf("llama ASR binary is not usable at %s: %w", command, err)
		}
		return command, nil, firstNonEmptyString(strings.TrimSpace(cfg.LlamaDevice), managedASRBackendAuto), nil
	}

	installRoot := filepath.Join(llamaRoot, "runtime")
	if command, env, acceleration, ok := managedASRLlamaBinaryFromMarker(installRoot); ok {
		if managedASRLlamaAccelerationMatchesDevice(acceleration, cfg.LlamaDevice) {
			return command, env, acceleration, nil
		}
	}

	assets, err := managedASRLlamaBinaryAssetsFn(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		if path, lookErr := managedASRLookPath(managedASRLlamaBinaryFileName); lookErr == nil {
			return path, nil, firstNonEmptyString(strings.TrimSpace(cfg.LlamaDevice), managedASRBackendAuto), nil
		}
		return "", nil, "", managedASRLlamaFallbackError{Err: err}
	}
	assets, err = managedASRLlamaAssetsForDevice(assets, cfg.LlamaDevice)
	if err != nil {
		return "", nil, "", err
	}
	var installErrors []string
	fallbackable := len(assets) > 0
	for _, asset := range assets {
		command, env, err := installManagedASRLlamaBinary(ctx, installRoot, asset)
		if err == nil {
			return command, env, asset.Acceleration, nil
		}
		if !managedASRLlamaInstallErrorFallbackable(err) {
			fallbackable = false
		}
		installErrors = append(installErrors, asset.Name+": "+err.Error())
	}
	if path, lookErr := managedASRLookPath(managedASRLlamaBinaryFileName); lookErr == nil {
		return path, nil, firstNonEmptyString(strings.TrimSpace(cfg.LlamaDevice), managedASRBackendAuto), nil
	}
	err = fmt.Errorf("managed llama.cpp download/install failed: %s", strings.Join(installErrors, "; "))
	if fallbackable {
		return "", nil, "", managedASRLlamaFallbackError{Err: err}
	}
	return "", nil, "", err
}

func managedASRLlamaBinaryFromMarker(installRoot string) (string, []string, string, bool) {
	markerPath := filepath.Join(installRoot, "runtime.json")
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return "", nil, "", false
	}
	var marker managedASRLlamaBinaryMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return "", nil, "", false
	}
	if marker.Version != managedASRLlamaRuntimeVersion || marker.ReleaseTag != managedASRLlamaReleaseTag || strings.TrimSpace(marker.BinaryRel) == "" {
		return "", nil, "", false
	}
	command := filepath.Join(installRoot, filepath.FromSlash(marker.BinaryRel))
	info, err := os.Stat(command)
	if err != nil || info.IsDir() || info.Size() == 0 {
		return "", nil, "", false
	}
	if marker.BinarySize > 0 && info.Size() != marker.BinarySize {
		return "", nil, "", false
	}
	return command, managedASRLlamaInstallEnvForCommand(installRoot, command), strings.TrimSpace(marker.Acceleration), true
}

func installManagedASRLlamaBinary(ctx context.Context, installRoot string, asset managedASRLlamaAsset) (string, []string, error) {
	parent := filepath.Dir(installRoot)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", nil, err
	}
	staging := filepath.Join(parent, ".llama-staging-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	archivePath := filepath.Join(parent, ".llama-"+safePathPart(asset.Name))
	defer func() {
		_ = os.RemoveAll(staging)
		_ = os.Remove(archivePath)
	}()
	if err := os.RemoveAll(staging); err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return "", nil, err
	}
	if err := downloadManagedASRLlamaFile(ctx, managedASRLlamaFileAsset{
		Name:   asset.Name,
		URL:    asset.URL,
		SHA256: asset.SHA256,
		Size:   asset.Size,
	}, archivePath, "llama.cpp runtime"); err != nil {
		return "", nil, err
	}
	switch asset.ArchiveKind {
	case "tar.gz":
		if err := extractManagedASRTarGz(archivePath, staging); err != nil {
			return "", nil, fmt.Errorf("extract llama.cpp archive: %w", err)
		}
	case "zip":
		if err := extractManagedASRZip(archivePath, staging); err != nil {
			return "", nil, fmt.Errorf("extract llama.cpp archive: %w", err)
		}
	default:
		return "", nil, fmt.Errorf("unsupported llama.cpp archive type %q", asset.ArchiveKind)
	}
	command, err := findManagedASRLlamaExecutable(staging)
	if err != nil {
		return "", nil, err
	}
	env := managedASRLlamaInstallEnvForCommand(staging, command)
	if validateManagedASRLlamaBinaryFn != nil {
		if err := validateManagedASRLlamaBinaryFn(ctx, command, env); err != nil {
			return "", nil, managedASRLlamaValidationError{Err: fmt.Errorf("downloaded llama.cpp runtime is not usable: %w", err)}
		}
	}
	rel, err := filepath.Rel(staging, command)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(command)
	if err != nil {
		return "", nil, err
	}
	marker := managedASRLlamaBinaryMarker{
		Version:      managedASRLlamaRuntimeVersion,
		ReleaseTag:   managedASRLlamaReleaseTag,
		AssetName:    asset.Name,
		AssetURL:     asset.URL,
		AssetSHA256:  asset.SHA256,
		BinaryRel:    filepath.ToSlash(rel),
		BinarySize:   info.Size(),
		Acceleration: asset.Acceleration,
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	markerData, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return "", nil, err
	}
	if err := writePrivateFileReplacing(filepath.Join(staging, "runtime.json"), append(markerData, '\n'), 0o600); err != nil {
		return "", nil, err
	}
	if err := os.RemoveAll(installRoot); err != nil {
		return "", nil, err
	}
	if err := os.Rename(staging, installRoot); err != nil {
		return "", nil, err
	}
	finalCommand := filepath.Join(installRoot, rel)
	return finalCommand, managedASRLlamaInstallEnvForCommand(installRoot, finalCommand), nil
}

func ensureManagedASRLlamaModelFiles(ctx context.Context, llamaRoot string, cfg ManagedASRConfig) (string, string, error) {
	modelPath := strings.TrimSpace(cfg.LlamaModelPath)
	mmprojPath := strings.TrimSpace(cfg.LlamaMMProjPath)
	if modelPath != "" {
		if _, err := os.Stat(modelPath); err != nil {
			return "", "", fmt.Errorf("llama ASR model is not usable at %s: %w", modelPath, err)
		}
	}
	if mmprojPath != "" {
		if _, err := os.Stat(mmprojPath); err != nil {
			return "", "", fmt.Errorf("llama ASR mmproj is not usable at %s: %w", mmprojPath, err)
		}
	}
	if modelPath != "" && mmprojPath != "" {
		return modelPath, mmprojPath, nil
	}

	managedModelPath, managedMMProjPath, err := ensureManagedASRLlamaManagedModelFiles(ctx, llamaRoot)
	if err != nil {
		return "", "", err
	}
	if modelPath == "" {
		modelPath = managedModelPath
	}
	if mmprojPath == "" {
		mmprojPath = managedMMProjPath
	}
	return modelPath, mmprojPath, nil
}

func ensureManagedASRLlamaManagedModelFiles(ctx context.Context, llamaRoot string) (string, string, error) {
	assets := managedASRLlamaManagedModelAssetsFn()
	modelRoot := filepath.Join(llamaRoot, "models", safePathPart(assets.RootName))
	modelPath := filepath.Join(modelRoot, assets.Model.Name)
	mmprojPath := filepath.Join(modelRoot, assets.MMProj.Name)
	if managedASRLlamaModelMarkerCurrent(modelRoot, modelPath, mmprojPath, assets) {
		return modelPath, mmprojPath, nil
	}
	if err := os.MkdirAll(modelRoot, 0o700); err != nil {
		return "", "", err
	}
	if err := downloadManagedASRLlamaFile(ctx, assets.Model, modelPath, "Qwen3-ASR GGUF model"); err != nil {
		return "", "", err
	}
	if err := downloadManagedASRLlamaFile(ctx, assets.MMProj, mmprojPath, "Qwen3-ASR mmproj GGUF"); err != nil {
		return "", "", err
	}
	marker := managedASRLlamaModelMarker{
		Version:       managedASRLlamaRuntimeVersion,
		ModelRepo:     assets.Repo,
		ModelRevision: assets.Revision,
		ModelFile:     assets.Model.Name,
		ModelSHA256:   assets.Model.SHA256,
		ModelSize:     assets.Model.Size,
		MMProjFile:    assets.MMProj.Name,
		MMProjSHA256:  assets.MMProj.SHA256,
		MMProjSize:    assets.MMProj.Size,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return "", "", err
	}
	if err := writePrivateFileReplacing(filepath.Join(modelRoot, "runtime.json"), append(data, '\n'), 0o600); err != nil {
		return "", "", err
	}
	return modelPath, mmprojPath, nil
}

func managedASRLlamaModelMarkerCurrent(modelRoot string, modelPath string, mmprojPath string, assets managedASRLlamaModelAssets) bool {
	data, err := os.ReadFile(filepath.Join(modelRoot, "runtime.json"))
	if err != nil {
		return false
	}
	var marker managedASRLlamaModelMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return false
	}
	if marker.Version != managedASRLlamaRuntimeVersion ||
		marker.ModelRepo != assets.Repo ||
		marker.ModelRevision != assets.Revision ||
		marker.ModelFile != assets.Model.Name ||
		marker.ModelSHA256 != assets.Model.SHA256 ||
		marker.ModelSize != assets.Model.Size ||
		marker.MMProjFile != assets.MMProj.Name ||
		marker.MMProjSHA256 != assets.MMProj.SHA256 ||
		marker.MMProjSize != assets.MMProj.Size {
		return false
	}
	for _, file := range []struct {
		path string
		size int64
	}{
		{path: modelPath, size: assets.Model.Size},
		{path: mmprojPath, size: assets.MMProj.Size},
	} {
		path := file.path
		info, err := os.Stat(path)
		if err != nil || info.IsDir() || info.Size() == 0 {
			return false
		}
		if file.size > 0 && info.Size() != file.size {
			return false
		}
	}
	return true
}

func managedASRLlamaModelURL(name string) string {
	return "https://huggingface.co/" + managedASRLlamaModelRepo + "/resolve/" + managedASRLlamaModelRevision + "/" + name
}

func managedASRLlamaManagedModelAssets() managedASRLlamaModelAssets {
	return managedASRLlamaModelAssets{
		RootName: managedASRLlamaModelRepo + "-" + managedASRLlamaModelRevision[:12],
		Repo:     managedASRLlamaModelRepo,
		Revision: managedASRLlamaModelRevision,
		Model: managedASRLlamaFileAsset{
			Name:   managedASRLlamaModelFile,
			URL:    managedASRLlamaModelURL(managedASRLlamaModelFile),
			SHA256: managedASRLlamaModelSHA256,
			Size:   managedASRLlamaModelSize,
		},
		MMProj: managedASRLlamaFileAsset{
			Name:   managedASRLlamaMMProjFile,
			URL:    managedASRLlamaModelURL(managedASRLlamaMMProjFile),
			SHA256: managedASRLlamaMMProjSHA256,
			Size:   managedASRLlamaMMProjSize,
		},
	}
}

func resolveManagedASRLlamaFFmpeg(ctx context.Context, cacheRoot string, configuredPath string) (string, error) {
	configuredPath = strings.TrimSpace(configuredPath)
	if configuredPath != "" {
		if _, err := os.Stat(configuredPath); err != nil {
			return "", fmt.Errorf("configured ffmpeg is not usable at %s: %w", configuredPath, err)
		}
		return configuredPath, nil
	}
	if path, err := managedASRLookPath("ffmpeg"); err == nil {
		return path, nil
	}
	cacheRoot = strings.TrimSpace(cacheRoot)
	if cacheRoot == "" {
		return "", fmt.Errorf("managed ffmpeg cache root is empty")
	}
	path, err := ensureManagedASRFFmpeg(ctx, filepath.Join(cacheRoot, "ffmpeg"))
	if errors.Is(err, errManagedASRFFmpegUnsupported) {
		return "", err
	}
	return path, err
}

func ensureManagedASRFFmpeg(ctx context.Context, installRoot string) (string, error) {
	if path, ok := managedASRFFmpegFromMarker(installRoot); ok {
		return path, nil
	}
	if err := os.MkdirAll(filepath.Dir(installRoot), 0o700); err != nil {
		return "", err
	}
	lock := flock.New(filepath.Join(filepath.Dir(installRoot), "ffmpeg.lock"))
	locked, err := lock.TryLockContext(ctx, time.Second)
	if err != nil {
		return "", err
	}
	if !locked {
		return "", fmt.Errorf("Teams speech recognition ffmpeg setup is already running in another helper process")
	}
	defer func() { _ = lock.Unlock() }()
	if path, ok := managedASRFFmpegFromMarker(installRoot); ok {
		return path, nil
	}
	asset, err := managedASRFFmpegWheelAssetFn(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return "", err
	}
	return installManagedASRFFmpeg(ctx, installRoot, asset)
}

func managedASRFFmpegFromMarker(installRoot string) (string, bool) {
	data, err := os.ReadFile(filepath.Join(installRoot, "runtime.json"))
	if err != nil {
		return "", false
	}
	var marker managedASRFFmpegMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return "", false
	}
	if marker.Version != managedASRFFmpegRuntimeVersion || strings.TrimSpace(marker.FFmpegRel) == "" {
		return "", false
	}
	path := filepath.Join(installRoot, filepath.FromSlash(marker.FFmpegRel))
	if info, err := os.Stat(path); err != nil || info.IsDir() {
		return "", false
	}
	return path, true
}

func installManagedASRFFmpeg(ctx context.Context, installRoot string, asset managedASRLlamaFileAsset) (string, error) {
	parent := filepath.Dir(installRoot)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", err
	}
	staging := filepath.Join(parent, ".ffmpeg-staging-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	archivePath := filepath.Join(parent, ".ffmpeg-"+safePathPart(asset.Name))
	defer func() {
		_ = os.RemoveAll(staging)
		_ = os.Remove(archivePath)
	}()
	if err := os.RemoveAll(staging); err != nil {
		return "", err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return "", err
	}
	if err := downloadManagedASRLlamaFile(ctx, asset, archivePath, "ffmpeg runtime"); err != nil {
		return "", err
	}
	if err := extractManagedASRZip(archivePath, staging); err != nil {
		return "", fmt.Errorf("extract ffmpeg runtime: %w", err)
	}
	ffmpegPath, err := findManagedASRFFmpegExecutable(staging)
	if err != nil {
		return "", err
	}
	_ = os.Chmod(ffmpegPath, 0o700)
	if validateManagedASRFFmpegBinaryFn != nil {
		if err := validateManagedASRFFmpegBinaryFn(ctx, ffmpegPath); err != nil {
			return "", fmt.Errorf("downloaded ffmpeg runtime is not usable: %w", err)
		}
	}
	rel, err := filepath.Rel(staging, ffmpegPath)
	if err != nil {
		return "", err
	}
	marker := managedASRFFmpegMarker{
		Version:     managedASRFFmpegRuntimeVersion,
		FFmpegRel:   filepath.ToSlash(rel),
		AssetName:   asset.Name,
		AssetURL:    asset.URL,
		AssetSHA256: asset.SHA256,
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return "", err
	}
	if err := writePrivateFileReplacing(filepath.Join(staging, "runtime.json"), append(data, '\n'), 0o600); err != nil {
		return "", err
	}
	if err := os.RemoveAll(installRoot); err != nil {
		return "", err
	}
	if err := os.Rename(staging, installRoot); err != nil {
		return "", err
	}
	return filepath.Join(installRoot, rel), nil
}

func managedASRFFmpegWheelAsset(goos string, goarch string) (managedASRLlamaFileAsset, error) {
	switch goos + "/" + goarch {
	case "linux/amd64":
		return managedASRFFmpegAsset("imageio_ffmpeg-0.6.0-py3-none-manylinux2014_x86_64.whl", "https://files.pythonhosted.org/packages/a0/2d/43c8522a2038e9d0e7dbdf3a61195ecc31ca576fb1527a528c877e87d973/imageio_ffmpeg-0.6.0-py3-none-manylinux2014_x86_64.whl", "c7e46fcec401dd990405049d2e2f475e2b397779df2519b544b8aab515195282", 29498237), nil
	case "linux/arm64":
		return managedASRFFmpegAsset("imageio_ffmpeg-0.6.0-py3-none-manylinux2014_aarch64.whl", "https://files.pythonhosted.org/packages/33/e7/1925bfbc563c39c1d2e82501d8372734a5c725e53ac3b31b4c2d081e895b/imageio_ffmpeg-0.6.0-py3-none-manylinux2014_aarch64.whl", "1d47bebd83d2c5fc770720d211855f208af8a596c82d17730aa51e815cdee6dc", 25632706), nil
	case "darwin/amd64":
		return managedASRFFmpegAsset("imageio_ffmpeg-0.6.0-py3-none-macosx_10_9_intel.macosx_10_9_x86_64.whl", "https://files.pythonhosted.org/packages/da/58/87ef68ac83f4c7690961bce288fd8e382bc5f1513860fc7f90a9c1c1c6bf/imageio_ffmpeg-0.6.0-py3-none-macosx_10_9_intel.macosx_10_9_x86_64.whl", "9d2baaf867088508d4a3458e61eeb30e945c4ad8016025545f66c4b5aaef0a61", 24932969), nil
	case "darwin/arm64":
		return managedASRFFmpegAsset("imageio_ffmpeg-0.6.0-py3-none-macosx_11_0_arm64.whl", "https://files.pythonhosted.org/packages/40/5c/f3d8a657d362cc93b81aab8feda487317da5b5d31c0e1fdfd5e986e55d17/imageio_ffmpeg-0.6.0-py3-none-macosx_11_0_arm64.whl", "b1ae3173414b5fc5f538a726c4e48ea97edc0d2cdc11f103afee655c463fa742", 21113891), nil
	case "windows/amd64":
		return managedASRFFmpegAsset("imageio_ffmpeg-0.6.0-py3-none-win_amd64.whl", "https://files.pythonhosted.org/packages/2c/c6/fa760e12a2483469e2bf5058c5faff664acf66cadb4df2ad6205b016a73d/imageio_ffmpeg-0.6.0-py3-none-win_amd64.whl", "02fa47c83703c37df6bfe4896aab339013f62bf02c5ebf2dce6da56af04ffc0a", 31246824), nil
	default:
		return managedASRLlamaFileAsset{}, fmt.Errorf("%w: %s/%s", errManagedASRFFmpegUnsupported, goos, goarch)
	}
}

func managedASRFFmpegAsset(name string, rawURL string, sha256sum string, size int64) managedASRLlamaFileAsset {
	return managedASRLlamaFileAsset{Name: name, URL: rawURL, SHA256: sha256sum, Size: size}
}

func findManagedASRFFmpegExecutable(root string) (string, error) {
	var found string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() || found != "" {
			return err
		}
		name := strings.ToLower(entry.Name())
		if strings.HasPrefix(name, "ffmpeg") && !strings.HasSuffix(name, ".py") && !strings.HasSuffix(name, ".md") {
			found = path
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("ffmpeg runtime archive did not contain an ffmpeg executable")
	}
	return found, nil
}

func validateManagedASRFFmpegBinary(ctx context.Context, command string) error {
	validateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(validateCtx, strings.TrimSpace(command), "-version")
	cmd.Env = managedASRSetupBaseEnv()
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := runASRCommand(validateCtx, cmd); err != nil {
		detail := strings.TrimSpace(out.String())
		if detail != "" {
			return fmt.Errorf("%w: %s", err, shortenTeamsLine(detail, 400))
		}
		return err
	}
	return nil
}

func managedASRLlamaBinaryAssets(goos string, goarch string) ([]managedASRLlamaAsset, error) {
	switch goos {
	case "linux":
		switch goarch {
		case "amd64":
			return []managedASRLlamaAsset{
				managedASRLlamaReleaseAsset("llama-b9437-bin-ubuntu-vulkan-x64.tar.gz", "177e7e70d13ac17df524a4126404025eff2b1f4b5a6f393e07b4bf1c25d31c65", 32237070, "vulkan"),
				managedASRLlamaReleaseAsset("llama-b9437-bin-ubuntu-x64.tar.gz", "07b0bf370a696329463d999ecb5c4860717eef6824e55eaf062214d70e78174d", 14514126, "cpu"),
			}, nil
		case "arm64":
			return []managedASRLlamaAsset{
				managedASRLlamaReleaseAsset("llama-b9437-bin-ubuntu-vulkan-arm64.tar.gz", "4e9d6fb2e17ccb23bbf4346f70e09c53f9f992c9eaf0e2a264672a5c189b0a0d", 25462765, "vulkan"),
				managedASRLlamaReleaseAsset("llama-b9437-bin-ubuntu-arm64.tar.gz", "ba0a1cf3c190d16107e78cd3e81c05a4c97dcab5c82291cb63f9bc0093d6987f", 11539448, "cpu"),
			}, nil
		}
	case "darwin":
		switch goarch {
		case "arm64":
			return []managedASRLlamaAsset{
				managedASRLlamaReleaseAsset("llama-b9437-bin-macos-arm64.tar.gz", "be62e359c081e718397e4ac9f8b7b346b77133681aa052bc6a26f5525ad0f723", 9657828, "metal"),
			}, nil
		case "amd64":
			return []managedASRLlamaAsset{
				managedASRLlamaReleaseAsset("llama-b9437-bin-macos-x64.tar.gz", "2a355c6c22fab70a47f25bff49b73083e0d59cb266a5cc2df5544bfd0b86e13d", 9929975, "metal"),
			}, nil
		}
	case "windows":
		switch goarch {
		case "amd64":
			return []managedASRLlamaAsset{
				managedASRLlamaReleaseAsset("llama-b9437-bin-win-vulkan-x64.zip", "02e354109984a4fc9f6dfa9cdfa8e2482b6b3d8ba6462856910efa0e84278855", 33096187, "vulkan"),
				managedASRLlamaReleaseAsset("llama-b9437-bin-win-cpu-x64.zip", "7f19b3da00425946e41a83c15f8ef4bf5cd261f35f941e408e9b2634ce8b6d7f", 16104427, "cpu"),
			}, nil
		case "arm64":
			return []managedASRLlamaAsset{
				managedASRLlamaReleaseAsset("llama-b9437-bin-win-cpu-arm64.zip", "9e0e177c5d6fba1834e6fb33d8da78c9abb2ecc225357274584fa945d405a380", 9707285, "cpu"),
			}, nil
		}
	}
	return nil, fmt.Errorf("managed llama.cpp Teams speech recognition runtime is not available for %s/%s", goos, goarch)
}

func managedASRLlamaReleaseAsset(name string, sha256sum string, size int64, acceleration string) managedASRLlamaAsset {
	kind := "tar.gz"
	if strings.HasSuffix(strings.ToLower(name), ".zip") {
		kind = "zip"
	}
	return managedASRLlamaAsset{
		Name:         name,
		URL:          managedASRLlamaDownloadBase + "/" + managedASRLlamaReleaseTag + "/" + name,
		SHA256:       sha256sum,
		Size:         size,
		ArchiveKind:  kind,
		Acceleration: acceleration,
	}
}

func downloadManagedASRLlamaFile(ctx context.Context, asset managedASRLlamaFileAsset, path string, label string) error {
	var lastErr error
	for attempt := 1; attempt <= managedASRLlamaDownloadAttempts; attempt++ {
		err := downloadManagedASRLlamaFileOnce(ctx, asset, path, label)
		if err == nil {
			return nil
		}
		lastErr = err
		if !managedASRLlamaDownloadRetryable(ctx, err) || attempt == managedASRLlamaDownloadAttempts {
			return err
		}
		if err := sleepContext(ctx, managedASRLlamaDownloadBackoff*time.Duration(attempt)); err != nil {
			return err
		}
	}
	return lastErr
}

type managedASRLlamaDownloadTransientError struct {
	Err error
}

func (e managedASRLlamaDownloadTransientError) Error() string {
	return e.Err.Error()
}

func (e managedASRLlamaDownloadTransientError) Unwrap() error {
	return e.Err
}

type managedASRLlamaDownloadHTTPError struct {
	Label      string
	StatusCode int
}

func (e managedASRLlamaDownloadHTTPError) Error() string {
	return fmt.Sprintf("download %s: HTTP %d %s", e.Label, e.StatusCode, http.StatusText(e.StatusCode))
}

func managedASRLlamaDownloadRetryable(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	var transient managedASRLlamaDownloadTransientError
	if errors.As(err, &transient) {
		return true
	}
	var httpErr managedASRLlamaDownloadHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusRequestTimeout ||
			httpErr.StatusCode == http.StatusTooManyRequests ||
			httpErr.StatusCode >= http.StatusInternalServerError
	}
	return false
}

func downloadManagedASRLlamaFileOnce(ctx context.Context, asset managedASRLlamaFileAsset, path string, label string) error {
	if strings.TrimSpace(asset.URL) == "" {
		return fmt.Errorf("%s download URL is empty", label)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, managedASRLlamaDownloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, asset.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("User-Agent", "codex-helper-managed-asr")
	client := managedASRLlamaHTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return managedASRLlamaDownloadTransientError{Err: fmt.Errorf("download %s: %w", label, err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return managedASRLlamaDownloadHTTPError{Label: label, StatusCode: resp.StatusCode}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".download-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	hash := sha256.New()
	written, err := io.Copy(tmp, io.TeeReader(resp.Body, hash))
	if err != nil {
		_ = tmp.Close()
		return managedASRLlamaDownloadTransientError{Err: fmt.Errorf("download %s: %w", label, err)}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if asset.Size > 0 && written != asset.Size {
		return fmt.Errorf("download %s: downloaded %d bytes, want %d", label, written, asset.Size)
	}
	gotSHA := hex.EncodeToString(hash.Sum(nil))
	if strings.TrimSpace(asset.SHA256) != "" && !strings.EqualFold(gotSHA, asset.SHA256) {
		return fmt.Errorf("download %s: sha256 %s, want %s", label, gotSHA, asset.SHA256)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return err
	}
	return replaceFile(tmpPath, path)
}

func extractManagedASRZip(archivePath string, dest string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()
	for _, file := range reader.File {
		target, err := safeManagedASRExtractPath(dest, file.Name)
		if err != nil {
			return err
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, file.Mode()&0o777); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		src, err := file.Open()
		if err != nil {
			return err
		}
		mode := file.Mode() & 0o777
		if mode == 0 {
			mode = 0o600
		}
		dst, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			_ = src.Close()
			return err
		}
		_, copyErr := io.Copy(dst, src)
		closeErr := dst.Close()
		srcErr := src.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if srcErr != nil {
			return srcErr
		}
	}
	return nil
}

func findManagedASRLlamaExecutable(root string) (string, error) {
	name := managedASRLlamaBinaryFileName
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	preferred := []string{
		filepath.Join(root, "bin", name),
		filepath.Join(root, name),
	}
	for _, path := range preferred {
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path, nil
		}
	}
	var found string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() || found != "" {
			return err
		}
		if strings.EqualFold(entry.Name(), name) {
			found = path
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("llama.cpp archive did not contain %s", name)
	}
	return found, nil
}

func managedASRLlamaInstallEnv(root string) []string {
	return managedASRLlamaInstallEnvForCommand(root, "")
}

func managedASRLlamaInstallEnvForCommand(root string, command string) []string {
	var dirs []string
	addDir := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			return
		}
		for _, existing := range dirs {
			if existing == path {
				return
			}
		}
		dirs = append(dirs, path)
	}
	if command != "" {
		commandDir := filepath.Dir(command)
		addDir(commandDir)
		addDir(filepath.Join(commandDir, "bin"))
		addDir(filepath.Join(commandDir, "lib"))
		parent := filepath.Dir(commandDir)
		addDir(parent)
		addDir(filepath.Join(parent, "bin"))
		addDir(filepath.Join(parent, "lib"))
	}
	for _, rel := range []string{"bin", "lib", "."} {
		addDir(filepath.Join(root, rel))
	}
	if len(dirs) == 0 {
		return nil
	}
	switch runtime.GOOS {
	case "windows":
		value := strings.Join(dirs, string(os.PathListSeparator))
		if existing := strings.TrimSpace(os.Getenv("PATH")); existing != "" {
			value += string(os.PathListSeparator) + existing
		}
		return []string{"PATH=" + value}
	case "darwin":
		value := strings.Join(dirs, string(os.PathListSeparator))
		if existing := strings.TrimSpace(os.Getenv("DYLD_LIBRARY_PATH")); existing != "" {
			value += string(os.PathListSeparator) + existing
		}
		return []string{"DYLD_LIBRARY_PATH=" + value}
	default:
		value := strings.Join(dirs, string(os.PathListSeparator))
		if existing := strings.TrimSpace(os.Getenv("LD_LIBRARY_PATH")); existing != "" {
			value += string(os.PathListSeparator) + existing
		}
		return []string{"LD_LIBRARY_PATH=" + value}
	}
}

func validateManagedASRLlamaBinary(ctx context.Context, command string, env []string) error {
	validateCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(validateCtx, strings.TrimSpace(command), "--help")
	cmd.Env = append(managedASRSetupBaseEnv(), env...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := runASRCommand(validateCtx, cmd); err != nil {
		detail := strings.TrimSpace(out.String())
		if detail != "" {
			return fmt.Errorf("%w: %s", err, shortenTeamsLine(detail, 400))
		}
		return err
	}
	return nil
}
