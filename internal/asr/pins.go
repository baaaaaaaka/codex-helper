package asr

import (
	"fmt"
	"strings"
)

// Archive formats of an Asset.
const (
	FormatTarGz = "tar.gz" // runtime archive (all platforms); one top-level directory, stripped on extract
	FormatFile  = "file"   // a single file, stored under its Name
)

// Asset is one pinned download. An empty SHA256 means "not published yet".
type Asset struct {
	Name    string
	URL     string
	Mirrors []string // tried in order when URL fails; the same pinned bytes
	SHA256  string
	Size    int64
	Format  string
}

// urls is URL followed by the mirrors.
func (a Asset) urls() []string { return append([]string{a.URL}, a.Mirrors...) }

// Manifest is everything the managed backend installs on one platform.
type Manifest struct {
	ModelID string
	Runtime Asset // llama-server and its libraries
	Model   Asset
	MMProj  Asset
	FFmpeg  Asset // zero: use a system ffmpeg found on PATH
}

const (
	// LlamaTag is the llama.cpp release our runtime assets are built from
	// (scripts/ci/build-llama-asr.sh, .github/workflows/llama-asr-assets.yml).
	LlamaTag   = "b9437"
	runtimeRev = "r1"
	// AssetRelease is the GitHub release that holds every voice asset: the
	// runtimes, the audio-only ffmpeg builds and a copy of the model.
	AssetRelease = "llama-asr-" + LlamaTag + "-" + runtimeRev
	runtimeBase  = "https://github.com/baaaaaaaka/codex-helper/releases/download/" + AssetRelease + "/"
	// FFmpegTag is the FFmpeg release of the audio-only LGPL ffmpeg builds
	// (scripts/ci/build-ffmpeg-audio.sh).
	FFmpegTag    = "n7.1.5"
	modelRepo    = "ggml-org/Qwen3-ASR-0.6B-GGUF"
	modelRev     = "928ab958557df9aa2ef1c93e0e83c7ad0933fae2"
	modelBaseURL = "https://huggingface.co/" + modelRepo + "/resolve/" + modelRev + "/"
)

// assetPlatforms names each supported GOOS/GOARCH in asset file names.
var assetPlatforms = map[string]string{
	"linux/amd64":   "linux-x64",
	"linux/arm64":   "linux-arm64",
	"darwin/arm64":  "macos-arm64",
	"darwin/amd64":  "macos-x64",
	"windows/amd64": "win-x64",
	"windows/arm64": "win-arm64",
}

// runtimePins are our own llama.cpp builds (one archive per platform with
// every CPU variant and, where available, the GPU backend). Fill SHA256/Size
// from the <name>.pin.json files the llama-asr-assets workflow uploads.
var runtimePins = map[string]Asset{
	"linux/amd64":   runtimeAsset("linux-x64", FormatTarGz, "", 0),
	"linux/arm64":   runtimeAsset("linux-arm64", FormatTarGz, "", 0),
	"darwin/arm64":  runtimeAsset("macos-arm64", FormatTarGz, "", 0),
	"darwin/amd64":  runtimeAsset("macos-x64", FormatTarGz, "", 0),
	"windows/amd64": runtimeAsset("win-x64", FormatTarGz, "", 0),
	"windows/arm64": runtimeAsset("win-arm64", FormatTarGz, "", 0),
}

// ffmpegPins are our audio-only LGPL ffmpeg builds (one static binary per
// platform), from the same release and workflow as the runtimes.
var ffmpegPins = map[string]Asset{
	"linux/amd64":   ffmpegAsset("linux-x64", "", 0),
	"linux/arm64":   ffmpegAsset("linux-arm64", "", 0),
	"darwin/arm64":  ffmpegAsset("macos-arm64", "", 0),
	"darwin/amd64":  ffmpegAsset("macos-x64", "", 0),
	"windows/amd64": ffmpegAsset("win-x64", "", 0),
	"windows/arm64": ffmpegAsset("win-arm64", "", 0),
}

// DefaultManifest returns the pinned assets for goos/goarch.
func DefaultManifest(goos, goarch string) (Manifest, error) {
	key := goos + "/" + goarch
	rt, ok := runtimePins[key]
	if !ok {
		return Manifest{}, newError(KindUnsupported, "select runtime", fmt.Errorf("no managed voice runtime for %s", key), "")
	}
	if rt.SHA256 == "" {
		return Manifest{}, newError(KindUnsupported, "select runtime", fmt.Errorf("the managed voice runtime for %s is not published yet", key), "")
	}
	return Manifest{
		ModelID: "Qwen/Qwen3-ASR-0.6B",
		Runtime: rt,
		Model:   modelAsset("Qwen3-ASR-0.6B-Q8_0.gguf", "bca259818b50ca7c4c05e9bdb35a5dc04fa039653a6d6f3f0f331f96f6aa1971", 804749248),
		MMProj:  modelAsset("mmproj-Qwen3-ASR-0.6B-Q8_0.gguf", "41a342b5e4c514e968cb756de6cd1b7be39eff43c44c57a2ef5fc6522e36603d", 214392480),
		FFmpeg:  publishedOrZero(ffmpegPins[key]), // zero: a system ffmpeg on PATH
	}, nil
}

// modelAsset is a model file: the copy in our release first, Hugging Face
// (where it was published) as the mirror. Both serve the same pinned bytes.
func modelAsset(name, sha string, size int64) Asset {
	return Asset{Name: name, URL: runtimeBase + name, Mirrors: []string{modelBaseURL + name}, SHA256: sha, Size: size, Format: FormatFile}
}

func ffmpegAsset(platform, sha string, size int64) Asset {
	name := "ffmpeg-audio-" + FFmpegTag + "-" + runtimeRev + "-" + platform + "." + FormatTarGz
	return Asset{Name: name, URL: runtimeBase + name, SHA256: sha, Size: size, Format: FormatTarGz}
}

func publishedOrZero(a Asset) Asset {
	if a.SHA256 == "" {
		return Asset{}
	}
	return a
}

func runtimeAsset(platform, format, sha string, size int64) Asset {
	name := "llama-asr-" + LlamaTag + "-" + runtimeRev + "-" + platform + "." + format
	return Asset{Name: name, URL: runtimeBase + name, SHA256: sha, Size: size, Format: format}
}

// dirName is the content-addressed install directory of an asset.
func (a Asset) dirName() string {
	base := a.Name
	for _, ext := range []string{".tar.gz", ".gguf"} {
		base = strings.TrimSuffix(base, ext)
	}
	sha := a.SHA256
	if len(sha) > 12 {
		sha = sha[:12]
	}
	return base + "-" + sha
}
