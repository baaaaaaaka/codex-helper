package asr

import (
	"fmt"
	"strings"
)

// Archive formats of an Asset.
const (
	FormatTarGz = "tar.gz" // runtime archive (all platforms); one top-level directory, stripped on extract
	FormatFile  = "file"   // a single file, stored under its Name
	FormatWheel = "wheel"  // imageio-ffmpeg wheel; only the Member binary is kept, as "ffmpeg"
)

// Asset is one pinned download. An empty SHA256 means "not published yet".
type Asset struct {
	Name   string
	URL    string
	SHA256 string
	Size   int64
	Format string
	Member string // FormatWheel: path prefix of the binary inside the wheel
}

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
	LlamaTag     = "b9437"
	runtimeRev   = "r1"
	runtimeBase  = "https://github.com/baaaaaaaka/codex-helper/releases/download/llama-asr-" + LlamaTag + "-" + runtimeRev + "/"
	modelRepo    = "ggml-org/Qwen3-ASR-0.6B-GGUF"
	modelRev     = "928ab958557df9aa2ef1c93e0e83c7ad0933fae2"
	modelBaseURL = "https://huggingface.co/" + modelRepo + "/resolve/" + modelRev + "/"
)

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

// ffmpegPins are the static ffmpeg builds inside the imageio-ffmpeg 0.6.0
// wheels (manylinux2014, so they also run on glibc 2.17).
var ffmpegPins = map[string]Asset{
	"linux/amd64":   wheel("manylinux2014_x86_64", "a0/2d/43c8522a2038e9d0e7dbdf3a61195ecc31ca576fb1527a528c877e87d973", "c7e46fcec401dd990405049d2e2f475e2b397779df2519b544b8aab515195282", 29498237),
	"linux/arm64":   wheel("manylinux2014_aarch64", "33/e7/1925bfbc563c39c1d2e82501d8372734a5c725e53ac3b31b4c2d081e895b", "1d47bebd83d2c5fc770720d211855f208af8a596c82d17730aa51e815cdee6dc", 25632706),
	"darwin/amd64":  wheel("macosx_10_9_intel.macosx_10_9_x86_64", "da/58/87ef68ac83f4c7690961bce288fd8e382bc5f1513860fc7f90a9c1c1c6bf", "9d2baaf867088508d4a3458e61eeb30e945c4ad8016025545f66c4b5aaef0a61", 24932969),
	"darwin/arm64":  wheel("macosx_11_0_arm64", "40/5c/f3d8a657d362cc93b81aab8feda487317da5b5d31c0e1fdfd5e986e55d17", "b1ae3173414b5fc5f538a726c4e48ea97edc0d2cdc11f103afee655c463fa742", 21113891),
	"windows/amd64": wheel("win_amd64", "2c/c6/fa760e12a2483469e2bf5058c5faff664acf66cadb4df2ad6205b016a73d", "02fa47c83703c37df6bfe4896aab339013f62bf02c5ebf2dce6da56af04ffc0a", 31246824),
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
		Model: Asset{Name: "Qwen3-ASR-0.6B-Q8_0.gguf", URL: modelBaseURL + "Qwen3-ASR-0.6B-Q8_0.gguf",
			SHA256: "bca259818b50ca7c4c05e9bdb35a5dc04fa039653a6d6f3f0f331f96f6aa1971", Size: 804749248, Format: FormatFile},
		MMProj: Asset{Name: "mmproj-Qwen3-ASR-0.6B-Q8_0.gguf", URL: modelBaseURL + "mmproj-Qwen3-ASR-0.6B-Q8_0.gguf",
			SHA256: "41a342b5e4c514e968cb756de6cd1b7be39eff43c44c57a2ef5fc6522e36603d", Size: 214392480, Format: FormatFile},
		FFmpeg: ffmpegPins[key], // zero on windows/arm64: system ffmpeg
	}, nil
}

func runtimeAsset(platform, format, sha string, size int64) Asset {
	name := "llama-asr-" + LlamaTag + "-" + runtimeRev + "-" + platform + "." + format
	return Asset{Name: name, URL: runtimeBase + name, SHA256: sha, Size: size, Format: format}
}

func wheel(tag, path, sha string, size int64) Asset {
	name := "imageio_ffmpeg-0.6.0-py3-none-" + tag + ".whl"
	return Asset{Name: name, URL: "https://files.pythonhosted.org/packages/" + path + "/" + name,
		SHA256: sha, Size: size, Format: FormatWheel, Member: "imageio_ffmpeg/binaries/ffmpeg"}
}

// dirName is the content-addressed install directory of an asset.
func (a Asset) dirName() string {
	base := a.Name
	for _, ext := range []string{".tar.gz", ".whl", ".gguf"} {
		base = strings.TrimSuffix(base, ext)
	}
	sha := a.SHA256
	if len(sha) > 12 {
		sha = sha[:12]
	}
	return base + "-" + sha
}
