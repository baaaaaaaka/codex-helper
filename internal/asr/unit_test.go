package asr

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPromptRoundTrip(t *testing.T) {
	prompt := PromptWithTranscripts("please check", []Clip{
		{Name: "a.ogg", Result: Result{Text: "run the tests", Language: "English", Backend: "llama.cpp/cpu"}},
		{Name: "b.mp4", Result: Result{}},
	})
	for _, want := range []string{PromptLead, "Voice/video clip 1: a.ogg (language=English, backend=llama.cpp/cpu)", "run the tests",
		"Voice/video clip 2: b.mp4 (language=auto)", "(empty transcript)", "recognition errors"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt lacks %q:\n%s", want, prompt)
		}
	}
	if got := StripTranscripts(prompt); got != "please check" {
		t.Fatalf("StripTranscripts = %q", got)
	}
	if got := StripTranscripts(PromptWithTranscripts("", []Clip{{Name: "a", Result: Result{Text: "x"}}})); got != "" {
		t.Fatalf("voice-only prompt mirrors as %q, want empty", got)
	}
	if PromptWithTranscripts(" typed ", nil) != "typed" {
		t.Fatal("no clips must keep the text")
	}
}

func TestParseASR(t *testing.T) {
	for in, want := range map[string][2]string{
		"language English<asr_text>Hello world.":  {"Hello world.", "English"},
		"language Chinese<asr_text>你好</asr_text>": {"你好", "Chinese"},
		"language None<asr_text>":                 {"", ""},
		"plain text":                              {"plain text", ""},
	} {
		text, lang := parseASR(in)
		if text != want[0] || lang != want[1] {
			t.Errorf("parseASR(%q) = %q, %q", in, text, lang)
		}
	}
}

func TestServerArgs(t *testing.T) {
	p := &paths{model: "m.gguf", mmproj: "p.gguf"}
	cpu := strings.Join(serverArgs(p, "cpu", "k", 1234), " ")
	for _, want := range []string{"-t 4 -tb 4", "-c 2048", "-np 1 --threads-http 2", "--port 1234", "--host 127.0.0.1", "--device none --no-mmproj-offload", "--no-webui"} {
		if !strings.Contains(cpu, want) {
			t.Fatalf("cpu args %q lack %q", cpu, want)
		}
	}
	if gpu := strings.Join(serverArgs(p, "gpu", "k", 1), " "); strings.Contains(gpu, "--device") {
		t.Fatalf("gpu args select a device: %q", gpu)
	}
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "gpu"), 0o755)
	os.WriteFile(filepath.Join(dir, "gpu", "ggml-vulkan.dll"), nil, 0o644)
	server := filepath.Join(dir, "llama-server")
	if env := gpuPluginEnv(server, "cpu"); env != nil {
		t.Fatalf("cpu mode loads a GPU plugin: %v", env)
	}
	if env := gpuPluginEnv(server, "gpu"); len(env) != 1 || env[0] != "GGML_BACKEND_PATH="+filepath.Join(dir, "gpu", "ggml-vulkan.dll") {
		t.Fatalf("gpu env = %v", env)
	}
	t.Setenv("GGML_BACKEND_PATH", "/elsewhere/ggml-cuda.so")
	for _, kv := range childEnv() {
		if strings.HasPrefix(kv, "GGML_BACKEND_PATH=") {
			t.Fatalf("inherited %s", kv)
		}
	}
	for dev, want := range map[string]string{"": "cpu", "cpu": "cpu", "none": "cpu", "metal": "gpu", "vulkan": "gpu", "auto": "gpu"} {
		if got := (&Managed{cfg: ManagedConfig{Device: dev}}).device(); got != want {
			t.Errorf("device(%q) = %q, want %q", dev, got, want)
		}
	}
}

func TestClassifyRun(t *testing.T) {
	cases := []struct {
		err    error
		stderr string
		want   Kind
	}{
		{exec.ErrNotFound, "", KindNotConfigured},
		{&fs.PathError{Op: "fork/exec", Path: "x", Err: syscall.ENOENT}, "", KindNotConfigured},
		{&fs.PathError{Op: "fork/exec", Path: "x", Err: syscall.ENOEXEC}, "", KindNativeLoad},
		{&fs.PathError{Op: "fork/exec", Path: "x", Err: syscall.EACCES}, "", KindCache},
		{errors.New("exit status 127"), "./llama-server: error while loading shared libraries: libvulkan.so.1: cannot open shared object file", KindNativeLoad},
		{errors.New("exit status 1"), "/lib64/libstdc++.so.6: version `GLIBCXX_3.4.29' not found (required by ./llama-server)", KindNativeLoad},
		{errors.New("exit status 1"), "dyld[123]: Library not loaded: @rpath/libggml.dylib", KindNativeLoad},
		{errors.New("%1 is not a valid Win32 application."), "", KindNativeLoad},
		{errors.New("exit status 1"), "something else went wrong", KindBackend},
	}
	for _, c := range cases {
		if got := classifyRun("op", c.err, c.stderr, KindNotConfigured).Kind; got != c.want {
			t.Errorf("classifyRun(%v, %q) = %s, want %s", c.err, c.stderr, got, c.want)
		}
	}
	for code, desc := range windowsLoadFailures {
		if !strings.Contains(desc, "STATUS_") || code < 0xC0000000 {
			t.Errorf("bad table entry %#x", code)
		}
	}
	// Model files the server cannot parse are integrity problems, not native ones.
	e := classifyServerExit(errors.New("exit status 1"), "llama_model_load: error loading model: invalid magic characters")
	if KindOf(e) != KindIntegrity {
		t.Fatalf("bad model classified as %v", e)
	}
}

func TestUserMessageEveryKind(t *testing.T) {
	for kind := range userMessages {
		msg := UserMessage(newError(kind, "op", nil, "detail"))
		if !strings.HasPrefix(msg, "Voice transcription ") || strings.Count(msg, "\n") > 1 {
			t.Errorf("%s: %q", kind, msg)
		}
	}
	if UserMessage(errors.New("foreign")) == "" || UserMessage(nil) != "" {
		t.Fatal("foreign/nil errors")
	}
}

func fakeCmd(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), exeName("fake-asr-cmd"))
	if err := os.WriteFile(p, self(t), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCommandBackend(t *testing.T) {
	cmd := fakeCmd(t)
	voice := voiceFile(t, t.TempDir(), "v.ogg", "x")
	if NewCommand("  ", nil) != nil {
		t.Fatal("empty command must mean: use the managed backend")
	}
	if _, ok := New("", nil, ManagedConfig{CacheDir: t.TempDir()}).(*Managed); !ok {
		t.Fatal("New without a command must return the managed backend")
	}

	t.Setenv("ASR_FAKE_CMD", "")
	res, err := transcribe(t, NewCommand(cmd, nil), voice, nil, time.Minute)
	if err != nil || res.Text != voice || res.Backend != "command" || res.Language != "auto" {
		t.Fatalf("default args: %+v %v", res, err)
	}
	c := NewCommand(cmd, []string{"--in={input}", "{name}", "{language}", "{speed}", "{threads}"})
	res, err = c.Transcribe(t.Context(), Request{Path: voice, Name: "Voice note.ogg", Language: "zh"})
	if err != nil || res.Text != "--in="+voice+" Voice note.ogg zh 1.25x 4" {
		t.Fatalf("placeholders: %+v %v", res, err)
	}

	t.Setenv("ASR_FAKE_CMD", "json")
	res, err = transcribe(t, NewCommand(cmd, []string{"hi"}), voice, nil, time.Minute)
	if err != nil || res != (Result{Text: "hi", Language: "zh", Model: "m", Backend: "b"}) {
		t.Fatalf("json: %+v %v", res, err)
	}

	t.Setenv("ASR_FAKE_CMD", "env")
	t.Setenv("OMP_NUM_THREADS", "64")
	t.Setenv("GOMAXPROCS", "2")
	t.Setenv("LLAMA_ARG_THREADS", "32")
	c = NewCommand(cmd, nil)
	c.Env = []string{"EXTRA=1"}
	res, err = transcribe(t, c, voice, nil, time.Minute)
	if want := "OMP_NUM_THREADS=4;GOMAXPROCS=2;CODEX_HELPER_TEAMS_ASR_THREADS=4;LLAMA_ARG_THREADS=;EXTRA=1"; err != nil || res.Text != want {
		t.Fatalf("env: %q %v, want %q", res.Text, err, want)
	}

	t.Setenv("ASR_FAKE_CMD", "empty")
	_, err = transcribe(t, NewCommand(cmd, nil), voice, nil, time.Minute)
	mustKind(t, err, KindNoSpeech)

	t.Setenv("ASR_FAKE_CMD", "exit:3")
	_, err = transcribe(t, NewCommand(cmd, nil), voice, nil, time.Minute)
	if e := mustKind(t, err, KindBackend); !strings.Contains(e.Detail, "failing on purpose") {
		t.Fatalf("detail %q", e.Detail)
	}

	_, err = transcribe(t, NewCommand(cmd, nil), filepath.Join(t.TempDir(), "gone.ogg"), nil, time.Minute)
	mustKind(t, err, KindMedia)

	c = NewCommand(cmd, nil)
	c.Timeout = 50 * time.Millisecond
	t.Setenv("ASR_FAKE_CMD", "")
	slow := filepath.Join(t.TempDir(), exeName("fake-asr-cmd"))
	if os.WriteFile(slow, []byte("#!/bin/sh\nsleep 30\n"), 0o755) == nil && !isWindows() {
		c.Path = slow
		start := time.Now()
		_, err = transcribe(t, c, voice, nil, time.Minute)
		mustKind(t, err, KindTimeout)
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timeout did not kill the command: %v", time.Since(start))
		}
	}
}

func isWindows() bool { return filepath.Separator == '\\' }

func TestManagedServerLifecycle(t *testing.T) {
	t.Run("idle unload and restart", func(t *testing.T) {
		f := newFixture(t, nil)
		m := f.managed(t, func(c *ManagedConfig) { c.IdleUnload = 200 * time.Millisecond })
		voice := voiceFile(t, f.dir, "v.ogg", "x")
		if _, err := transcribe(t, m, voice, nil, time.Minute); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			m.runMu.Lock()
			unloaded := m.srv == nil
			m.runMu.Unlock()
			if unloaded {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("server not unloaded after idle period")
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, err := transcribe(t, m, voice, nil, time.Minute); err != nil {
			t.Fatal(err)
		}
		if n := f.logCount("serve "); n != 2 {
			t.Fatalf("server starts = %d, want 2", n)
		}
	})

	t.Run("crash mid-request restarts once", func(t *testing.T) {
		f := newFixture(t, nil)
		t.Setenv("ASR_FAKE_SERVER", "crash-once")
		m := f.managed(t)
		res, err := transcribe(t, m, voiceFile(t, f.dir, "v.ogg", "again"), nil, time.Minute)
		if err != nil || res.Text != "again part 1" || f.logCount("serve ") != 2 {
			t.Fatalf("%+v %v starts=%d", res, err, f.logCount("serve "))
		}
	})

	t.Run("undecodable media keeps the backend healthy", func(t *testing.T) {
		f := newFixture(t, nil)
		m := f.managed(t)
		_, err := transcribe(t, m, voiceFile(t, f.dir, "bad.ogg", "NOAUDIO"), nil, time.Minute)
		mustKind(t, err, KindMedia)
		if _, err := transcribe(t, m, voiceFile(t, f.dir, "ok.ogg", "fine"), nil, time.Minute); err != nil {
			t.Fatalf("next message after a bad one: %v", err)
		}
		if got := tmpEntries(t, f.cache, "req-", "key-"); len(got) != 1 || !strings.HasPrefix(got[0], "key-") {
			t.Fatalf("temp files left behind: %v (want only the live server key)", got)
		}
	})

	t.Run("close stops setup and server", func(t *testing.T) {
		f := newFixture(t, nil)
		m := f.managed(t)
		if _, err := transcribe(t, m, voiceFile(t, f.dir, "v.ogg", "x"), nil, time.Minute); err != nil {
			t.Fatal(err)
		}
		m.Close()
		_, err := transcribe(t, m, filepath.Join(f.dir, "v.ogg"), nil, time.Minute)
		mustKind(t, err, KindCanceled)
		if got := tmpEntries(t, f.cache, "key-"); len(got) != 0 {
			t.Fatalf("server key left after Close: %v", got)
		}
	})
}
