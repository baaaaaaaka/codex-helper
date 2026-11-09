package asr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func downloadAsset(raw string, data []byte) Asset {
	sum := sha256.Sum256(data)
	return Asset{Name: "model.gguf", URL: raw, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), Format: FormatFile}
}

func TestDownloadResumeAndRangeValidation(t *testing.T) {
	data := []byte(strings.Repeat("GGUF bytes", 1000))
	for _, mode := range []string{"resume", "ignore", "bad-range", "wrong-length", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			var hits atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if r.Header.Get("Accept-Encoding") != "identity" {
					t.Error("compressed range requested")
				}
				if mode == "corrupt" && hits.Load() > 1 {
					if r.Header.Get("Range") != "" {
						t.Error("corrupt prefix was not discarded")
					}
					w.Write(data)
					return
				}
				if r.Header.Get("Range") != "bytes=100-" {
					t.Errorf("range=%q", r.Header.Get("Range"))
				}
				body := data[100:]
				if mode == "ignore" {
					w.Write(data)
					return
				}
				if mode == "bad-range" {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))
				} else {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes 100-%d/%d", len(data)-1, len(data)))
				}
				if mode == "wrong-length" {
					body = body[:10]
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(body)))
				w.WriteHeader(206)
				w.Write(body)
			}))
			defer s.Close()
			in := newInstaller(t.TempDir(), nil)
			in.backoff = time.Millisecond
			if err := os.MkdirAll(in.tmp(), 0700); err != nil {
				t.Fatal(err)
			}
			part := filepath.Join(in.tmp(), "saved.part")
			prefix := append([]byte(nil), data[:100]...)
			if mode == "corrupt" {
				prefix[0] ^= 1
			}
			if err := os.WriteFile(part, prefix, 0600); err != nil {
				t.Fatal(err)
			}
			a := downloadAsset(s.URL, data)
			_, err := in.downloadTo(context.Background(), a, part)
			switch mode {
			case "resume", "ignore", "corrupt":
				if err != nil {
					t.Fatal(err)
				}
				got, _ := os.ReadFile(part)
				if string(got) != string(data) {
					t.Fatal("wrong bytes")
				}
			case "bad-range":
				if KindOf(err) != KindDownload {
					t.Fatalf("%v", err)
				}
				st, _ := os.Stat(part)
				if st.Size() != 100 {
					t.Fatal("appended invalid range")
				}
			default:
				if KindOf(err) != KindIntegrity {
					t.Fatalf("%v", err)
				}
			}
			wantHits := int32(1)
			if mode == "corrupt" {
				wantHits = 2
			}
			if hits.Load() != wantHits {
				t.Fatalf("hits=%d", hits.Load())
			}
		})
	}
}

func TestDownloadInterruptedTransferKeepsPrefix(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%v", chunked), func(t *testing.T) {
			data := []byte(strings.Repeat("voice", 10000))
			var hits atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hits.Add(1) == 1 {
					if !chunked {
						w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					}
					w.Write(data[:100])
					if chunked {
						w.(http.Flusher).Flush()
					}
					return
				}
				if r.Header.Get("Range") != "bytes=100-" {
					t.Errorf("not resumed: %q", r.Header.Get("Range"))
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 100-%d/%d", len(data)-1, len(data)))
				w.Header().Set("Content-Length", fmt.Sprint(len(data)-100))
				w.WriteHeader(206)
				w.Write(data[100:])
			}))
			defer s.Close()
			in := newInstaller(t.TempDir(), nil)
			in.backoff = time.Millisecond
			file, err := in.download(context.Background(), downloadAsset(s.URL, data))
			if err != nil {
				t.Fatal(err)
			}
			got, _ := os.ReadFile(file)
			if string(got) != string(data) || hits.Load() != 2 {
				t.Fatalf("bad resume; hits=%d", hits.Load())
			}
		})
	}
}

func TestDownloadCompletePartialIsVerifiedBeforeReuse(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt=%v", corrupt), func(t *testing.T) {
			data := []byte("complete pinned model")
			var hits atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if r.Header.Get("Range") != "" {
					t.Error("complete corrupt partial must restart")
				}
				w.Write(data)
			}))
			defer s.Close()
			in := newInstaller(t.TempDir(), nil)
			in.backoff = time.Millisecond
			part := filepath.Join(in.root, "complete.part")
			prefix := append([]byte(nil), data...)
			if corrupt {
				prefix[0] ^= 1
			}
			if err := os.WriteFile(part, prefix, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := in.downloadTo(context.Background(), downloadAsset(s.URL, data), part); err != nil {
				t.Fatal(err)
			}
			wantHits := int32(0)
			if corrupt {
				wantHits = 1
			}
			if hits.Load() != wantHits {
				t.Fatalf("requests=%d, want %d", hits.Load(), wantHits)
			}
		})
	}
}

func TestDownloadSlowSourceYieldsWithPrefix(t *testing.T) {
	data := []byte(strings.Repeat("voice", 1000))
	var slowHits atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowHits.Add(1)
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.Write(data[:100])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=100-" {
			t.Errorf("prefix lost: %q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 100-%d/%d", len(data)-1, len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(len(data)-100))
		w.WriteHeader(206)
		w.Write(data[100:])
	}))
	defer fast.Close()
	a := downloadAsset(slow.URL, data)
	a.Mirrors = []string{fast.URL}
	in := newInstaller(t.TempDir(), nil)
	in.slowWindow = 50 * time.Millisecond
	in.minRate = 1 << 20
	in.logf = t.Logf
	// Bound a broken failover without treating shared-runner scheduling or
	// Windows file sync time as part of the 50 ms slow-source window.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := in.download(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if slowHits.Load() != 1 {
		t.Fatal("slow source retried before mirror")
	}
}

func TestMirrorBasesCoverEveryAssetWithoutChangingPins(t *testing.T) {
	m, err := DefaultManifest("linux", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	got, err := withMirrorBases(m, []string{"https://mirror.example/voice/", "https://mirror.example/voice"})
	if err != nil {
		t.Fatal(err)
	}
	before := []Asset{m.Runtime, m.FFmpeg, m.Model, m.MMProj}
	after := []Asset{got.Runtime, got.FFmpeg, got.Model, got.MMProj}
	for i, a := range after {
		if a.SHA256 != before[i].SHA256 || a.Size != before[i].Size || len(a.Mirrors) != len(before[i].Mirrors)+1 || a.Mirrors[0] != "https://mirror.example/voice/"+a.Name {
			t.Fatalf("bad mirrors: %+v", a)
		}
	}
	for _, base := range []string{"http://mirror.example/", "https://user:secret@mirror.example/", "https://mirror.example/?token=x", "https://mirror.example/?", "https://mirror.example/#", "/relative"} {
		if _, err := withMirrorBases(m, []string{base}); err == nil {
			t.Fatal("accepted invalid base")
		}
	}
}

func TestDownloadCancellationResumesAcrossInstallersAndWarmCacheIsReadOnly(t *testing.T) {
	data := []byte(strings.Repeat("GGUF voice", 10000))
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.Write(data[:100])
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if r.Header.Get("Range") != "bytes=100-" {
			t.Errorf("restart lost prefix: %q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 100-%d/%d", len(data)-1, len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(len(data)-100))
		w.WriteHeader(206)
		w.Write(data[100:])
	}))
	defer s.Close()
	root := t.TempDir()
	a := downloadAsset(s.URL, data)
	in := newInstaller(root, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in.progress = cancel
	if _, err := in.ensure(ctx, a, nil); KindOf(err) != KindCanceled {
		t.Fatalf("cancel: %v", err)
	}
	part := filepath.Join(in.tmp(), "part-"+a.dirName())
	st, err := os.Stat(part)
	if err != nil || st.Size() != 100 {
		t.Fatalf("partial not kept: %v %v", st, err)
	}
	in = newInstaller(root, nil)
	dir, err := in.ensure(context.Background(), a, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, manifestName)
	before, _ := os.Stat(manifest)
	if _, err := newInstaller(root, nil).ensure(context.Background(), a, nil); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(manifest)
	if hits.Load() != 2 || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("warm cache downloaded or rewrote its manifest")
	}
}

func BenchmarkASRVerifyDownload(b *testing.B) {
	root := b.TempDir()
	part := filepath.Join(root, "model.part")
	f, err := os.Create(part)
	if err != nil {
		b.Fatal(err)
	}
	chunk := []byte(strings.Repeat("v", 64<<10))
	for i := 0; i < 1024; i++ {
		if _, err := f.Write(chunk); err != nil {
			b.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
	sum, err := fileSHA256(part)
	if err != nil {
		b.Fatal(err)
	}
	a := Asset{Name: "model", SHA256: sum, Size: 64 << 20}
	in := newInstaller(root, nil)
	b.SetBytes(a.Size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := in.verifyDownload(a, part); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPartialCleanupDoesNotDeleteActiveResume(t *testing.T) {
	in := newInstaller(t.TempDir(), nil)
	if err := os.MkdirAll(in.tmp(), 0700); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(in.tmp(), "part-asset-hash")
	if err := os.WriteFile(part, []byte("prefix"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * staleTempAge)
	if err := os.Chtimes(part, old, old); err != nil {
		t.Fatal(err)
	}
	unlock, locked := in.lock(context.Background(), "asset-hash")
	if !locked {
		t.Skip("locking unavailable")
	}
	in.cleanTemp(time.Now())
	_, err := os.Stat(part)
	unlock()
	if err != nil {
		t.Fatal("active partial deleted")
	}
	in.cleanTemp(time.Now())
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatal("abandoned partial retained")
	}
}
