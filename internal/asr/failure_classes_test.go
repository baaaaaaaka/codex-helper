package asr

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// One end-to-end test per failure class the managed backend must survive.
// Everything runs against a local fake asset server and fake binaries.

// (a) Clean host: first use installs everything (one notice, one download per
// asset), later uses and later processes reuse the cache; a host that cannot
// be served gets one clear, actionable error.
func TestFailureClassCleanHost(t *testing.T) {
	f := newFixture(t, nil)
	t.Setenv("ASR_FAKE_SEGMENTS", "2")
	voice := voiceFile(t, f.dir, "voice.ogg", "hello world")

	m := f.managed(t)
	rec := &recorder{}
	res, err := transcribe(t, m, voice, rec, time.Minute)
	if err != nil {
		t.Fatalf("first transcription: %v", err)
	}
	if res.Text != "hello world part 1 hello world part 2" || res.Language != "English" || res.Backend != "llama.cpp/cpu" {
		t.Fatalf("result = %+v", res)
	}
	if rec.count() != 1 || rec.events[0].Kind != EventPreparing || rec.events[0].CacheDir != f.cache ||
		rec.events[0].DownloadBytes != f.totalBytes() {
		t.Fatalf("events = %+v, want one preparing event for %d bytes in %s", rec.events, f.totalBytes(), f.cache)
	}
	for name, n := range f.hits() {
		if n != 1 {
			t.Fatalf("%s downloaded %d times", name, n)
		}
	}
	if serve := f.logCount("serve "); serve != 1 {
		t.Fatalf("llama-server started %d times", serve)
	}
	line := ""
	for _, l := range strings.Split(readFile(t, f.log), "\n") {
		if strings.HasPrefix(l, "serve ") {
			line = l
		}
	}
	for _, want := range []string{"-t 4", "-tb 4", "-c 2048", "-np 1", "--device none", "--no-mmproj-offload", "--host 127.0.0.1", "--api-key-file"} {
		if !strings.Contains(line, want) {
			t.Fatalf("server args %q lack %q", line, want)
		}
	}
	if f.logCount("ffmpeg ") != 1 || !strings.Contains(readFile(t, f.log), "atempo=1.25,aresample=16000") {
		t.Fatalf("ffmpeg not run with the speed-up filter:\n%s", readFile(t, f.log))
	}

	// Second message: resident server, no downloads, no notice.
	rec2 := &recorder{}
	if _, err := transcribe(t, m, voice, rec2, time.Minute); err != nil {
		t.Fatal(err)
	}
	// A new helper process on the same cache: verified install, no downloads.
	m.Close()
	m2 := f.managed(t)
	if _, err := transcribe(t, m2, voice, rec2, time.Minute); err != nil {
		t.Fatal(err)
	}
	if rec2.count() != 0 || f.logCount("serve ") != 2 {
		t.Fatalf("events %+v, server starts %d", rec2.events, f.logCount("serve "))
	}
	for name, n := range f.hits() {
		if n != 1 {
			t.Fatalf("%s downloaded %d times after reuse", name, n)
		}
	}
	if _, err := os.Stat(voice); err != nil {
		t.Fatalf("voice file was touched: %v", err)
	}

	t.Run("unreachable asset", func(t *testing.T) {
		g := newFixture(t, nil)
		g.srv.set(func(s *assetServer) { s.missing["model.gguf"] = true })
		_, err := transcribe(t, g.managed(t), voiceFile(t, g.dir, "v.ogg", "x"), nil, time.Minute)
		e := mustKind(t, err, KindDownload)
		// 404 is permanent: no retries, and the row fails instead of staying pending.
		if Retryable(err) || g.hitCount("model.gguf") != 1 || !strings.Contains(e.Error(), "HTTP 404") {
			t.Fatalf("retryable=%v hits=%d err=%v", Retryable(err), g.hitCount("model.gguf"), err)
		}
		if msg := UserMessage(err); !strings.Contains(msg, "github.com and huggingface.co") {
			t.Fatalf("message not actionable: %s", msg)
		}
		if _, err := os.Stat(g.installDir(g.manifest.Model)); !os.IsNotExist(err) {
			t.Fatalf("model dir published after failed download: %v", err)
		}
	})
	t.Run("unsupported platform", func(t *testing.T) {
		_, err := DefaultManifest("plan9", "386")
		mustKind(t, err, KindUnsupported)
		if Retryable(err) || !strings.Contains(UserMessage(err), "--asr-command") {
			t.Fatalf("message: %s", UserMessage(err))
		}
	})
}

// prewarm installs everything but the model, so a test that measures setup
// progress is not timing the (race-instrumented) fake binaries' validation.
func (f *fixture) prewarm(t *testing.T) {
	t.Helper()
	f.srv.set(func(s *assetServer) { s.missing["model.gguf"] = true })
	_, err := transcribe(t, f.managed(t), voiceFile(t, f.dir, "prewarm.ogg", "x"), nil, time.Minute)
	mustKind(t, err, KindDownload)
	f.srv.set(func(s *assetServer) { delete(s.missing, "model.gguf") })
	f.srv.set(func(s *assetServer) { s.hits = map[string]int{} })
}

func (f *fixture) hitCount(name string) int { return f.srv.hitCount(name) }

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, _ := os.ReadFile(p)
	return string(b)
}

// (b) Native incompatibility: the runtime cannot run on this host. The error
// is classified, the voice file is kept, the user gets one actionable
// message, nothing broken is published, and repeated messages fail fast
// without re-downloading or re-running the binary (no crash loop).
func TestFailureClassNativeIncompatibility(t *testing.T) {
	cases := []struct {
		name   string
		server []byte
		env    string
		want   string
	}{
		{"glibc too old", nil, "glibc", "GLIBC_2.28"},
		{"not an executable for this OS", garbage(4096), "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.server)
			t.Setenv("ASR_FAKE_SERVER", tc.env)
			voice := voiceFile(t, f.dir, "voice.ogg", "hello")
			m := f.managed(t)
			_, err := transcribe(t, m, voice, nil, time.Minute)
			e := mustKind(t, err, KindNativeLoad)
			if Retryable(err) || !strings.Contains(e.Error(), tc.want) {
				t.Fatalf("err = %v", err)
			}
			msg := UserMessage(err)
			if !strings.Contains(msg, "cannot run on this machine") || !strings.Contains(msg, "Diagnostic:") {
				t.Fatalf("message: %s", msg)
			}
			if _, err := os.Stat(f.installDir(f.manifest.Runtime)); !os.IsNotExist(err) {
				t.Fatalf("unusable runtime was published: %v", err)
			}
			if f.hitCount("model.gguf") != 0 {
				t.Fatal("model downloaded although the runtime cannot run")
			}
			runs, hits := f.logCount("version"), f.hitCount("llama-asr-test.tar.gz")
			start := time.Now()
			for i := 0; i < 5; i++ {
				_, err = transcribe(t, m, voice, nil, time.Minute)
				mustKind(t, err, KindNativeLoad)
			}
			if time.Since(start) > 2*time.Second || f.logCount("version") != runs || f.hitCount("llama-asr-test.tar.gz") != hits {
				t.Fatalf("retries were not fail-fast: %v, runs %d->%d, downloads %d->%d",
					time.Since(start), runs, f.logCount("version"), hits, f.hitCount("llama-asr-test.tar.gz"))
			}
			if _, err := os.Stat(voice); err != nil {
				t.Fatalf("voice file lost: %v", err)
			}
		})
	}

	t.Run("crash while loading (CPU variant)", func(t *testing.T) {
		f := newFixture(t, nil)
		t.Setenv("ASR_FAKE_SERVER", "illegal-serve")
		voice := voiceFile(t, f.dir, "voice.ogg", "hello")
		m := f.managed(t)
		_, err := transcribe(t, m, voice, nil, time.Minute)
		mustKind(t, err, KindNativeLoad)
		for i := 0; i < 3; i++ {
			_, err = transcribe(t, m, voice, nil, time.Minute)
			mustKind(t, err, KindNativeLoad)
		}
		if n := f.logCount("serve "); n != 1 {
			t.Fatalf("llama-server started %d times; want 1 (no crash loop)", n)
		}
	})

	t.Run("external command", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), exeName("asr-tool"))
		os.WriteFile(bad, garbage(2048), 0o755)
		voice := voiceFile(t, t.TempDir(), "voice.ogg", "hello")
		_, err := transcribe(t, NewCommand(bad, nil), voice, nil, time.Minute)
		mustKind(t, err, KindNativeLoad)
		_, err = transcribe(t, NewCommand(filepath.Join(t.TempDir(), "missing"), nil), voice, nil, time.Minute)
		mustKind(t, err, KindNotConfigured)
	})

	t.Run("windows missing DLL exit status", func(t *testing.T) {
		if runtime.GOOS != "windows" {
			t.Skip("NTSTATUS exit codes exist only on Windows; classification is table-driven in TestClassifyRun")
		}
		f := newFixture(t, nil)
		t.Setenv("ASR_FAKE_EXIT", "0xC0000135")
		_, err := transcribe(t, f.managed(t), voiceFile(t, f.dir, "v.ogg", "x"), nil, time.Minute)
		e := mustKind(t, err, KindNativeLoad)
		if !strings.Contains(e.Detail, "DLL") {
			t.Fatalf("detail: %q", e.Detail)
		}
	})
}

// (c) Slow first-run setup: callers are told once per voice file and return
// promptly with a retryable KindPreparing while the download continues in
// the background; nobody waits on anybody else; setup is bounded; a stalled
// download is cut and retried.
func TestFailureClassSlowSetup(t *testing.T) {
	f := newFixture(t, nil)
	gate := make(chan struct{})
	f.srv.set(func(s *assetServer) { s.hold["model.gguf"] = gate })
	m := f.managed(t)

	// Three chats send voice messages while setup is downloading.
	recs := make([]*recorder, 3)
	voices := make([]string, 3)
	var wg sync.WaitGroup
	for i := range recs {
		recs[i] = &recorder{}
		voices[i] = voiceFile(t, f.dir, "voice"+string(rune('a'+i))+".ogg", "chat "+string(rune('a'+i)))
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := time.Now()
			_, err := transcribe(t, m, voices[i], recs[i], 300*time.Millisecond)
			if KindOf(err) != KindPreparing || !Retryable(err) || time.Since(start) > 3*time.Second {
				t.Errorf("chat %d: err=%v after %v; want prompt retryable preparing", i, err, time.Since(start))
			}
		}(i)
	}
	wg.Wait()
	// The core retries the first chat's message: no second notice.
	_, err := transcribe(t, m, voices[0], recs[0], 200*time.Millisecond)
	mustKind(t, err, KindPreparing)
	if !strings.HasPrefix(UserMessage(err), "Voice transcription pending:") {
		t.Fatalf("message: %s", UserMessage(err))
	}
	for i, r := range recs {
		if r.count() != 1 {
			t.Fatalf("chat %d got %d notices, want 1", i, r.count())
		}
	}
	f.srv.set(func(s *assetServer) { delete(s.hold, "model.gguf") })
	close(gate)
	for i, v := range voices {
		res, err := transcribe(t, m, v, recs[i], time.Minute)
		if err != nil || res.Text != "chat "+string(rune('a'+i))+" part 1" {
			t.Fatalf("chat %d after setup: %+v %v", i, res, err)
		}
		if recs[i].count() != 1 {
			t.Fatalf("chat %d notified again after setup", i)
		}
	}
	if n := f.hitCount("model.gguf"); n != 1 {
		t.Fatalf("model downloaded %d times; setup must be shared", n)
	}

	t.Run("setup timeout is bounded and recoverable", func(t *testing.T) {
		g := newFixture(t, nil)
		g.prewarm(t)
		g.srv.set(func(s *assetServer) { s.hold["model.gguf"] = make(chan struct{}) })
		m := g.managed(t, func(c *ManagedConfig) { c.SetupTimeout = time.Second })
		start := time.Now()
		_, err := transcribe(t, m, voiceFile(t, g.dir, "v.ogg", "late"), nil, time.Minute)
		mustKind(t, err, KindTimeout)
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("setup timeout took %v", d)
		}
		if leftovers := tmpEntries(t, g.cache, "dl-", "stage-"); len(leftovers) != 0 {
			t.Fatalf("timed-out setup left %v", leftovers)
		}
		g.srv.set(func(s *assetServer) { delete(s.hold, "model.gguf") })
		if res, err := transcribe(t, m, filepath.Join(g.dir, "v.ogg"), nil, time.Minute); err != nil || res.Text != "late part 1" {
			t.Fatalf("after timeout: %+v %v", res, err)
		}
	})

	t.Run("slow but progressing setup keeps the message pending", func(t *testing.T) {
		g := newFixture(t, nil)
		g.prewarm(t)
		g.srv.set(func(s *assetServer) { s.trickle["model.gguf"] = 100 * time.Millisecond })
		m := g.managed(t, func(c *ManagedConfig) { c.SetupIdleTimeout = 600 * time.Millisecond })
		voice := voiceFile(t, g.dir, "v.ogg", "slow")
		rec := &recorder{}
		start := time.Now()
		for {
			m.mu.Lock()
			ready := m.ready != nil
			m.mu.Unlock()
			wait := 200 * time.Millisecond // the core's short poll while setup runs
			if ready {
				wait = time.Minute
			}
			res, err := transcribe(t, m, voice, rec, wait)
			if err == nil {
				if res.Text != "slow part 1" {
					t.Fatalf("text %q", res.Text)
				}
				break
			}
			m.mu.Lock()
			ready = m.ready != nil
			m.mu.Unlock()
			if ready && KindOf(err) == KindTimeout && wait < time.Minute {
				continue // setup finished during the short poll; transcription needs longer
			}
			if KindOf(err) != KindPreparing || !Retryable(err) {
				t.Fatalf("after %v: %v; a progressing setup must keep the row pending", time.Since(start), err)
			}
			if time.Since(start) > time.Minute {
				t.Fatal("setup never finished")
			}
		}
		if d := time.Since(start); d < 1500*time.Millisecond {
			t.Fatalf("setup took only %v; the trickle must outlast SetupIdleTimeout for this test to mean anything", d)
		}
		if rec.count() != 1 || g.hitCount("model.gguf") != 1 {
			t.Fatalf("notices %d, model requests %d; want 1 and 1", rec.count(), g.hitCount("model.gguf"))
		}
	})

	t.Run("setup without progress fails the pending message once", func(t *testing.T) {
		g := newFixture(t, nil)
		g.prewarm(t)
		g.srv.set(func(s *assetServer) { s.hold["model.gguf"] = make(chan struct{}) })
		m := g.managed(t, func(c *ManagedConfig) {
			c.StallTimeout = 200 * time.Millisecond
			c.SetupIdleTimeout = 1500 * time.Millisecond
		})
		voice := voiceFile(t, g.dir, "v.ogg", "stuck")
		rec := &recorder{}
		_, err := transcribe(t, m, voice, rec, 300*time.Millisecond)
		mustKind(t, err, KindPreparing)
		// Setup keeps retrying the download, but every attempt stops at the
		// same byte, which is not progress. Wait for it to give up.
		deadline := time.Now().Add(30 * time.Second)
		for {
			m.mu.Lock()
			done := m.setup == nil
			m.mu.Unlock()
			if done {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("setup without progress was not stopped")
			}
			time.Sleep(50 * time.Millisecond)
		}
		if g.hitCount("model.gguf") < 3 {
			t.Fatalf("model requests = %d; transient failures must be retried while setup runs", g.hitCount("model.gguf"))
		}
		// The core retries the pending row: it now gets the final failure.
		_, err = transcribe(t, m, voice, rec, time.Minute)
		e := mustKind(t, err, KindDownload)
		if Retryable(err) || !strings.Contains(e.Detail, "no download progress") {
			t.Fatalf("err = %v", err)
		}
		if msg := UserMessage(err); !strings.HasPrefix(msg, "Voice transcription failed:") || !strings.Contains(msg, "resend") {
			t.Fatalf("message: %s", msg)
		}
		if _, err := os.Stat(voice); err != nil {
			t.Fatalf("voice file was removed: %v", err)
		}
		// Once delivered, a resend starts a fresh setup and succeeds.
		g.srv.set(func(s *assetServer) { delete(s.hold, "model.gguf") })
		if res, err := transcribe(t, m, voice, rec, time.Minute); err != nil || res.Text != "stuck part 1" {
			t.Fatalf("resend: %+v %v", res, err)
		}
		if rec.count() != 2 {
			t.Fatalf("notices = %d; want one per setup", rec.count())
		}
	})

	t.Run("stalled download is retried", func(t *testing.T) {
		g := newFixture(t, nil)
		g.srv.set(func(s *assetServer) { s.stall["model.gguf"] = 1 })
		m := g.managed(t, func(c *ManagedConfig) { c.StallTimeout = 300 * time.Millisecond })
		if _, err := transcribe(t, m, voiceFile(t, g.dir, "v.ogg", "x"), nil, time.Minute); err != nil {
			t.Fatal(err)
		}
		if n := g.hitCount("model.gguf"); n != 2 {
			t.Fatalf("model requests = %d, want 2 (stalled + retry)", n)
		}
	})
}

// (d) Broken or half-written cache, including a cache shared by several
// hosts (NFS home): problems are detected from the install manifest and
// checksums, repaired by re-downloading only the affected asset, and
// publication is atomic so racing hosts never see or produce a partial
// install.
func TestFailureClassBrokenCache(t *testing.T) {
	f := newFixture(t, nil)
	voice := voiceFile(t, f.dir, "voice.ogg", "fix me")
	first := f.managed(t)
	if _, err := transcribe(t, first, voice, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	first.Close()

	mf := f.manifest
	damage := []struct {
		name   string
		asset  Asset
		breakF func(t *testing.T)
	}{
		{"truncated model", mf.Model, func(t *testing.T) {
			os.Truncate(filepath.Join(f.installDir(mf.Model), mf.Model.Name), 100)
		}},
		{"flipped byte in llama-server", mf.Runtime, func(t *testing.T) {
			p := filepath.Join(f.installDir(mf.Runtime), exeName("llama-server"))
			b, _ := os.ReadFile(p)
			b[len(b)/2] ^= 0xff
			os.WriteFile(p, b, 0o755)
		}},
		{"missing install manifest", mf.MMProj, func(t *testing.T) {
			os.Remove(filepath.Join(f.installDir(mf.MMProj), manifestName))
		}},
		{"model with valid size but bad content", mf.Model, func(t *testing.T) {
			p := filepath.Join(f.installDir(mf.Model), mf.Model.Name)
			fh, _ := os.OpenFile(p, os.O_WRONLY, 0)
			fh.WriteAt([]byte("BAD!"), 0)
			fh.Close()
		}},
		{"bad projector", mf.MMProj, func(t *testing.T) {
			p := filepath.Join(f.installDir(mf.MMProj), mf.MMProj.Name)
			fh, _ := os.OpenFile(p, os.O_WRONLY, 0)
			fh.WriteAt([]byte("BAD!"), 0)
			fh.Close()
		}},
	}
	for _, d := range damage {
		t.Run(d.name, func(t *testing.T) {
			before := f.hits()
			d.breakF(t)
			rec := &recorder{}
			res, err := transcribe(t, f.managed(t), voice, rec, time.Minute)
			if err != nil || res.Text != "fix me part 1" {
				t.Fatalf("not repaired in the same call: %+v %v", res, err)
			}
			after := f.hits()
			for name := range after {
				want := before[name]
				if name == d.asset.Name {
					want++
				}
				if after[name] != want {
					t.Fatalf("%s downloaded %d times, want %d", name, after[name]-before[name], want-before[name])
				}
			}
			if rec.count() != 1 || rec.events[0].DownloadBytes != d.asset.Size {
				t.Fatalf("events %+v, want one notice for %d bytes", rec.events, d.asset.Size)
			}
			if err := verifyInstall(f.installDir(d.asset), d.asset); err != nil {
				t.Fatalf("repaired install does not verify: %v", err)
			}
		})
	}

	t.Run("leftovers from an interrupted setup", func(t *testing.T) {
		tmp := filepath.Join(f.cache, "tmp")
		old := time.Now().Add(-48 * time.Hour)
		for _, name := range []string{"stage-old", "dl-old", "broken-x-old"} {
			p := filepath.Join(tmp, name)
			os.MkdirAll(p, 0o700)
			os.Chtimes(p, old, old)
		}
		os.MkdirAll(filepath.Join(tmp, "stage-live"), 0o700) // may belong to another host right now
		if _, err := transcribe(t, f.managed(t), voice, nil, time.Minute); err != nil {
			t.Fatal(err)
		}
		if got := tmpEntries(t, f.cache, "stage-", "dl-", "broken-"); len(got) != 1 || got[0] != "stage-live" {
			t.Fatalf("tmp after cleanup = %v, want only stage-live", got)
		}
	})

	t.Run("corrupt download is never published", func(t *testing.T) {
		g := newFixture(t, nil)
		g.srv.set(func(s *assetServer) { s.corrupt["mmproj.gguf"] = true })
		const base = 400 * time.Millisecond
		m := g.managed(t, func(c *ManagedConfig) { c.IntegrityBackoff = base })
		voice := voiceFile(t, g.dir, "v.ogg", "x")
		failed := time.Now()
		_, err := transcribe(t, m, voice, nil, time.Minute)
		e := mustKind(t, err, KindIntegrity)
		if msg := UserMessage(err); !strings.Contains(msg, "checksum") || !strings.Contains(msg, "will not retry setup before") {
			t.Fatalf("message: %s", msg)
		}
		if w := e.RetryAt.Sub(failed); w < base || w > base+5*time.Second {
			t.Fatalf("first retry in %v, want about %v", w, base)
		}
		if _, err := os.Stat(g.installDir(g.manifest.MMProj)); !os.IsNotExist(err) {
			t.Fatalf("corrupt projector published: %v", err)
		}
		if got := tmpEntries(t, g.cache, "dl-", "stage-"); len(got) != 0 {
			t.Fatalf("leftovers %v", got)
		}
		hits := g.hitCount("mmproj.gguf")
		_, err = transcribe(t, m, voice, nil, time.Minute)
		mustKind(t, err, KindIntegrity)
		if g.hitCount("mmproj.gguf") != hits {
			t.Fatal("bad mirror is hammered: integrity failures must back off")
		}
		// After the backoff it tries again, and the next wait doubles.
		time.Sleep(time.Until(e.RetryAt) + 50*time.Millisecond)
		failed = time.Now()
		_, err = transcribe(t, m, voice, nil, time.Minute)
		e = mustKind(t, err, KindIntegrity)
		if g.hitCount("mmproj.gguf") != hits+1 {
			t.Fatalf("mmproj requests %d, want %d", g.hitCount("mmproj.gguf"), hits+1)
		}
		if w := e.RetryAt.Sub(failed); w < 2*base || w > 2*base+5*time.Second {
			t.Fatalf("second retry in %v, want about %v", w, 2*base)
		}
		g.srv.set(func(s *assetServer) { delete(s.corrupt, "mmproj.gguf") })
		time.Sleep(time.Until(e.RetryAt) + 50*time.Millisecond)
		if _, err := transcribe(t, m, voice, nil, time.Minute); err != nil {
			t.Fatalf("after the mirror is fixed: %v", err)
		}
	})

	for _, locks := range []bool{true, false} {
		name := "hosts sharing one cache with working locks"
		if !locks {
			name = "hosts sharing one cache without locks (NFS)"
		}
		t.Run(name, func(t *testing.T) {
			g := newFixture(t, nil)
			const hosts = 4
			ms := make([]*Managed, hosts)
			for i := range ms {
				ms[i] = g.managed(t)
				ms[i].in.noLock = !locks
			}
			var wg sync.WaitGroup
			for i, m := range ms {
				v := voiceFile(t, g.dir, "host"+string(rune('0'+i))+".ogg", "host "+string(rune('0'+i)))
				wg.Add(1)
				go func(i int, m *Managed) {
					defer wg.Done()
					res, err := transcribe(t, m, v, nil, time.Minute)
					if err != nil || res.Text != "host "+string(rune('0'+i))+" part 1" {
						t.Errorf("host %d: %+v %v", i, res, err)
					}
				}(i, m)
			}
			wg.Wait()
			for _, a := range []Asset{g.manifest.Runtime, g.manifest.Model, g.manifest.MMProj, g.manifest.FFmpeg} {
				if err := verifyInstall(g.installDir(a), a); err != nil {
					t.Fatalf("%s: %v", a.Name, err)
				}
				if n := g.hitCount(a.Name); n < 1 || n > hosts || locks && n != 1 {
					t.Fatalf("%s downloaded %d times (locks=%v)", a.Name, n, locks)
				}
			}
			if got := tmpEntries(t, g.cache, "dl-", "stage-"); len(got) != 0 {
				t.Fatalf("leftovers %v", got)
			}
		})
	}
}
