package asr

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRealModel runs the managed backend (download, verify, unpack, ffmpeg
// decode, llama-server) against the real runtime, the real ffmpeg and the
// real Qwen3-ASR model. It is skipped unless a directory of real assets is
// given in ASR_REAL_DIR (default testdata/real/, untracked):
//
//	llama-asr-*-<platform>.tar.gz           this platform's runtime archive
//	ffmpeg-audio-*-<platform>.tar.gz        this platform's ffmpeg archive (optional:
//	                                        without it the voice WAV is passed through)
//	Qwen3-ASR-0.6B-Q8_0.gguf, mmproj-Qwen3-ASR-0.6B-Q8_0.gguf
//	voice.wav, voice.<ext> ...              speech saying "... unit tests ..." in any
//	                                        container Teams sends (ogg, m4a, webm, mp3)
//
// With ASR_REAL_PUBLISHED=1 the assets are not read from the directory but
// downloaded exactly as a user's helper downloads them (DefaultManifest:
// our GitHub release, the model's Hugging Face mirror); only the voice
// files come from the directory.
func TestRealModel(t *testing.T) {
	dir := os.Getenv("ASR_REAL_DIR")
	if dir == "" {
		dir = filepath.Join("testdata", "real")
	}
	published := os.Getenv("ASR_REAL_PUBLISHED") == "1"
	platform := assetPlatforms[runtime.GOOS+"/"+runtime.GOARCH]
	one := func(pattern string) string {
		m, _ := filepath.Glob(filepath.Join(dir, pattern))
		if len(m) != 1 {
			return ""
		}
		return m[0]
	}
	voices, _ := filepath.Glob(filepath.Join(dir, "voice.*"))
	if len(voices) == 0 || (!published && one("llama-asr-*-"+platform+".tar.gz") == "") {
		t.Skipf("no real assets for %s in %s", platform, dir)
	}

	if published {
		if _, err := DefaultManifest(runtime.GOOS, runtime.GOARCH); err != nil {
			t.Skipf("voice assets for %s: %v", platform, err) // TODO(pins): fail once the pins are filled
		}
	}
	work := t.TempDir()
	cfg := ManagedConfig{CacheDir: filepath.Join(work, "cache")}
	if !published {
		read := func(p string) []byte {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
		srv := newAssetServer(t)
		add := func(p, format string) Asset { return srv.add(filepath.Base(p), format, read(p)) }
		mf := Manifest{
			Runtime: add(one("llama-asr-*-"+platform+".tar.gz"), FormatTarGz),
			Model:   add(filepath.Join(dir, "Qwen3-ASR-0.6B-Q8_0.gguf"), FormatFile),
			MMProj:  add(filepath.Join(dir, "mmproj-Qwen3-ASR-0.6B-Q8_0.gguf"), FormatFile),
		}
		if ff := one("ffmpeg-audio-*-" + platform + ".tar.gz"); ff != "" {
			mf.FFmpeg = add(ff, FormatTarGz)
		} else {
			cfg.FFmpegPath = filepath.Join(work, exeName("ffmpeg"))
			if err := os.WriteFile(cfg.FFmpegPath, self(t), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("ASR_FAKE_FFMPEG", "copy")
			voices = []string{filepath.Join(dir, "voice.wav")}
		}
		cfg.Manifest = &mf
	}
	m := NewManaged(cfg)
	defer m.Close()

	for i, voice := range voices {
		label := filepath.Base(voice)
		if i == 0 {
			label += " (cold: install + load)"
		}
		start := time.Now()
		rec := &recorder{}
		res, err := transcribe(t, m, voice, rec, 30*time.Minute)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		t.Logf("%s: %v, %d notice(s), result %+v", label, time.Since(start).Round(time.Millisecond), rec.count(), res)
		if !strings.Contains(strings.ToLower(res.Text), "unit tests") || res.Language != "English" {
			t.Fatalf("%s: unexpected transcript %+v", label, res)
		}
		want := 0 // the first message prepares; the rest reuse the install and the server
		if i == 0 {
			want = 1
		}
		if rec.count() != want {
			t.Fatalf("%s: %d notices, want %d", label, rec.count(), want)
		}
	}
	m.runMu.Lock()
	if m.srv != nil {
		if b, err := os.ReadFile("/proc/" + strconv.Itoa(m.srv.proc.cmd.Process.Pid) + "/status"); err == nil {
			for _, l := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(l, "VmHWM") || strings.HasPrefix(l, "Threads") {
					t.Logf("llama-server %s", strings.Join(strings.Fields(l), " "))
				}
			}
		}
	}
	m.runMu.Unlock()
	if published {
		m.mu.Lock()
		mf := m.mf
		m.mu.Unlock()
		if mf.FFmpeg.URL == "" {
			t.Fatalf("no published ffmpeg for %s: a system ffmpeg was used", platform)
		}
	}
}
