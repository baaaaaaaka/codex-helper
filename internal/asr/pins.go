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
// every CPU variant and, where available, the GPU backend), as published in
// AssetRelease by the llama-asr-assets workflow. A rebuild is published as a
// new asset_rev (runtimeRev), never over these bytes.
var runtimePins = map[string]Asset{
	"linux/amd64":   runtimeAsset("linux-x64", FormatTarGz, "ae6948386623a88fa7846c07e57a1cfaa2b7f12b258356bc044c065695d5b504", 9625511),
	"linux/arm64":   runtimeAsset("linux-arm64", FormatTarGz, "1aad8ab749506ff07c214083e87d08863be627eda65cedd9903244c1650a0d30", 4747516),
	"darwin/arm64":  runtimeAsset("macos-arm64", FormatTarGz, "d46d67d51d347cc30d68f3839452b43fc8bf4e87c4148fe5d128ddc3d63bd512", 11405562),
	"darwin/amd64":  runtimeAsset("macos-x64", FormatTarGz, "95f6d2d5a8e859b80daa3cfa2ed1589501f71224732817446b07dc630d490467", 17362478),
	"windows/amd64": runtimeAsset("win-x64", FormatTarGz, "b12f13720a426260e39f44b7b1c5ff8b612678916974c885c3e499caa80cc0cf", 28191082),
	"windows/arm64": runtimeAsset("win-arm64", FormatTarGz, "286d89bf6837bc038684ba54c8c2296894e05d6b7d5c131f3eafb9f8712d6bcc", 5178094),
}

// ffmpegPins are our audio-only LGPL ffmpeg builds (one static binary per
// platform), from the same release and workflow as the runtimes.
var ffmpegPins = map[string]Asset{
	"linux/amd64":   ffmpegAsset("linux-x64", "f0c2a4a5f6b36354333c49905063c363c6ff1fc25e27c243b400a293b64d8fa2", 1225068),
	"linux/arm64":   ffmpegAsset("linux-arm64", "6121275cef77046c09b1062f09291effeab2f92a839cdaf086e7e390bb6398d9", 1245425),
	"darwin/arm64":  ffmpegAsset("macos-arm64", "49331b22855fd60c5884e847eac1ef6dad3fea04b8aa2046828384b215bb2142", 1181986),
	"darwin/amd64":  ffmpegAsset("macos-x64", "e8dcc254fb7d5a53d9722adca8748e6db663cc1852d6cf5631be2bc6954720ea", 1224158),
	"windows/amd64": ffmpegAsset("win-x64", "270e83c8a9a0c278c5edfcd5ee2ff7bc6163e660b62de32d9e889dd03d740dc8", 1299642),
	"windows/arm64": ffmpegAsset("win-arm64", "b7863d34e1c3bae6a3d6aaabe26d1bd30d9ab47a780a032dc0951571969edfb7", 1172314),
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
		FFmpeg:  ffmpegPins[key],
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
