package asr

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// A pinned asset is fetched from its URL and then from each mirror: the
// model is in our GitHub release and on Hugging Face, and either may be
// blocked or missing on a given network.
func TestDownloadFallsBackToMirrors(t *testing.T) {
	data := []byte(strings.Repeat("model bytes ", 500))
	const name = "model.gguf"
	setup := func(t *testing.T, primary, mirror func(*assetServer)) (*installer, Asset, *assetServer, *assetServer) {
		p, m := newAssetServer(t), newAssetServer(t)
		a := p.add(name, FormatFile, data)
		m.add(name, FormatFile, data)
		a.Mirrors = []string{m.URL + "/" + name}
		p.set(primary)
		m.set(mirror)
		in := newInstaller(t.TempDir(), nil)
		in.backoff = time.Millisecond
		return in, a, p, m
	}
	ok := func(*assetServer) {}
	missing := func(s *assetServer) { s.missing[name] = true }
	down := func(s *assetServer) { s.down[name] = true }
	corrupt := func(s *assetServer) { s.corrupt[name] = true }

	for _, c := range []struct {
		name            string
		primary, mirror func(*assetServer)
		primaryHits     int
		mirrorHits      int
	}{
		{"primary serves", ok, ok, 1, 0},
		{"primary missing (404)", missing, ok, 1, 1},
		{"primary serves wrong bytes", corrupt, ok, 1, 1},
		{"primary down (503)", down, ok, 1, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			in, a, p, m := setup(t, c.primary, c.mirror)
			file, err := in.download(context.Background(), a)
			if err != nil {
				t.Fatalf("download: %v", err)
			}
			got, _ := os.ReadFile(file)
			if string(got) != string(data) {
				t.Fatal("downloaded bytes differ from the pin")
			}
			if p.hitCount(name) != c.primaryHits || m.hitCount(name) != c.mirrorHits {
				t.Fatalf("hits: primary %d (want %d), mirror %d (want %d)", p.hitCount(name), c.primaryHits, m.hitCount(name), c.mirrorHits)
			}
		})
	}

	t.Run("missing everywhere is permanent", func(t *testing.T) {
		in, a, _, _ := setup(t, missing, missing)
		_, err := in.download(context.Background(), a)
		mustKind(t, err, KindDownload)
		if !errors.Is(err, errPermanent) {
			t.Fatalf("want a permanent download error, got %v", err)
		}
	})
	t.Run("a mirror that is only down keeps the error retryable", func(t *testing.T) {
		in, a, _, _ := setup(t, missing, down)
		_, err := in.download(context.Background(), a)
		mustKind(t, err, KindDownload)
		if errors.Is(err, errPermanent) {
			t.Fatalf("a temporarily unavailable mirror made the failure permanent: %v", err)
		}
	})
	t.Run("wrong bytes everywhere is an integrity error", func(t *testing.T) {
		in, a, _, _ := setup(t, corrupt, corrupt)
		_, err := in.download(context.Background(), a)
		mustKind(t, err, KindIntegrity)
	})
}

// Every platform gets its runtime and ffmpeg from our release and the model
// from our release with Hugging Face as the mirror.
func TestPinsCoverEveryPlatform(t *testing.T) {
	platforms := assetPlatforms
	if len(runtimePins) != len(platforms) || len(ffmpegPins) != len(platforms) {
		t.Fatalf("pins for %d runtimes and %d ffmpeg builds, want %d each", len(runtimePins), len(ffmpegPins), len(platforms))
	}
	sha := regexp.MustCompile(`^[0-9a-f]{64}$`)
	release := "https://github.com/baaaaaaaka/codex-helper/releases/download/" + AssetRelease + "/"
	for key, platform := range platforms {
		rt, ff := runtimePins[key], ffmpegPins[key]
		if want := "llama-asr-" + LlamaTag + "-" + runtimeRev + "-" + platform + ".tar.gz"; rt.Name != want || rt.URL != release+want {
			t.Errorf("%s runtime = %s %s", key, rt.Name, rt.URL)
		}
		if want := "ffmpeg-audio-" + FFmpegTag + "-" + runtimeRev + "-" + platform + ".tar.gz"; ff.Name != want || ff.URL != release+want || ff.Format != FormatTarGz {
			t.Errorf("%s ffmpeg = %s %s %s", key, ff.Name, ff.URL, ff.Format)
		}
		// Every platform is published: a user on any of them gets voice
		// transcription with nothing installed.
		for _, a := range []Asset{rt, ff} {
			if !sha.MatchString(a.SHA256) || a.Size <= 0 {
				t.Errorf("%s: bad pin %+v", key, a)
			}
		}
		goos, goarch, _ := strings.Cut(key, "/")
		mf, err := DefaultManifest(goos, goarch)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if mf.FFmpeg.URL != ff.URL || mf.FFmpeg.SHA256 != ff.SHA256 {
			t.Errorf("%s: manifest ffmpeg %+v", key, mf.FFmpeg)
		}
		for _, a := range []Asset{mf.Model, mf.MMProj} {
			if a.URL != release+a.Name || len(a.Mirrors) != 1 || a.Mirrors[0] != modelBaseURL+a.Name || !sha.MatchString(a.SHA256) {
				t.Errorf("%s: model asset %+v", key, a)
			}
		}
	}
}
