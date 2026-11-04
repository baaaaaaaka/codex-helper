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

// TestRealModel runs the managed backend against the real runtime archive
// and the real Qwen3-ASR model, served from a local asset server. It runs
// only when testdata/real/ holds (untracked):
//
//	llama-asr-*-linux-x64.tar.gz           output of scripts/ci/build-llama-asr.sh
//	Qwen3-ASR-0.6B-Q8_0.gguf, mmproj-Qwen3-ASR-0.6B-Q8_0.gguf
//	voice.wav                              16 kHz mono speech saying "... unit tests ..."
//
// ffmpeg is faked (the WAV is passed through) because the test container
// has none; everything else is the production path.
func TestRealModel(t *testing.T) {
	dir := filepath.Join("testdata", "real")
	runtimes, _ := filepath.Glob(filepath.Join(dir, "llama-asr-*-linux-x64.tar.gz"))
	if len(runtimes) != 1 || runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("no real runtime/model in testdata/real")
	}
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Skip(err)
		}
		return b
	}
	srv := newAssetServer(t)
	mf := Manifest{
		Runtime: srv.add(filepath.Base(runtimes[0]), FormatTarGz, read(filepath.Base(runtimes[0]))),
		Model:   srv.add("Qwen3-ASR-0.6B-Q8_0.gguf", FormatFile, read("Qwen3-ASR-0.6B-Q8_0.gguf")),
		MMProj:  srv.add("mmproj-Qwen3-ASR-0.6B-Q8_0.gguf", FormatFile, read("mmproj-Qwen3-ASR-0.6B-Q8_0.gguf")),
	}
	work := t.TempDir()
	ffmpeg := filepath.Join(work, "ffmpeg")
	if err := os.WriteFile(ffmpeg, self(t), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ASR_FAKE_FFMPEG", "copy")
	m := NewManaged(ManagedConfig{CacheDir: filepath.Join(work, "cache"), Manifest: &mf, FFmpegPath: ffmpeg})
	defer m.Close()
	voice, _ := filepath.Abs(filepath.Join(dir, "voice.wav"))

	for i, label := range []string{"cold (install + load)", "warm (resident server)"} {
		start := time.Now()
		rec := &recorder{}
		res, err := transcribe(t, m, voice, rec, 10*time.Minute)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		t.Logf("%s: %v, %d notice(s), result %+v", label, time.Since(start).Round(time.Millisecond), rec.count(), res)
		if !strings.Contains(strings.ToLower(res.Text), "unit tests") || res.Language != "English" {
			t.Fatalf("unexpected transcript %+v", res)
		}
		if want := 1 - i; rec.count() != want {
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
}
