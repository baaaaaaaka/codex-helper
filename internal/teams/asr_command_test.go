package teams

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestCommandASRTranscriberArgsReplacePlaceholders(t *testing.T) {
	transcriber := &CommandASRTranscriber{Args: []string{
		"--input={input}",
		"--lang={language}",
		"--speed={speed}",
		"--threads={threads}",
		"--index={source_index}",
		"--alias={prompt_path}",
		"--type={content_type}",
	}}
	got := transcriber.commandArgs(ASRTranscribeInput{
		SourceIndex: 3,
		File:        LocalAttachment{Path: "/tmp/audio.m4a", PromptPath: ".codex-helper/audio.m4a", ContentType: "audio/mp4"},
		Language:    "auto",
		Speed:       "1.25x",
	})
	want := []string{
		"--input=/tmp/audio.m4a",
		"--lang=auto",
		"--speed=1.25x",
		"--threads=4",
		"--index=3",
		"--alias=.codex-helper/audio.m4a",
		"--type=audio/mp4",
	}
	if len(got) != len(want) {
		t.Fatalf("args len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("arg %d = %q, want %q; all args %#v", i, got[i], want[i], got)
		}
	}
}

func TestCommandASRTranscriberRunsCommandAndParsesJSON(t *testing.T) {
	transcriber := &CommandASRTranscriber{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestCommandASRTranscriberHelperProcess$", "--", "{input}", "{threads}"},
		Env:     []string{"CODEX_HELPER_TEST_ASR_COMMAND=json"},
		Model:   "default-model",
		Backend: "command-test",
	}
	transcript, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		SourceIndex: 5,
		File:        LocalAttachment{Path: "/tmp/audio.m4a", PromptPath: ".codex-helper/audio.m4a", ContentType: "audio/mp4"},
		Language:    "auto",
		Speed:       "1.25x",
	})
	if err != nil {
		t.Fatalf("TranscribeTeamsMedia error: %v", err)
	}
	if transcript.Text != "hello 测试" || transcript.Language != "zh" || transcript.Model != "helper-model" || transcript.Backend != "helper" {
		t.Fatalf("transcript = %#v", transcript)
	}
	if transcript.SourceIndex != 5 || transcript.SourcePath != ".codex-helper/audio.m4a" || transcript.ContentType != "audio/mp4" {
		t.Fatalf("defaulted transcript fields = %#v", transcript)
	}
}

func TestCommandASRTranscriberUsesPlainStdout(t *testing.T) {
	transcriber := &CommandASRTranscriber{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestCommandASRTranscriberHelperProcess$"},
		Env:     []string{"CODEX_HELPER_TEST_ASR_COMMAND=plain"},
	}
	transcript, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		SourceIndex: 1,
		File:        LocalAttachment{Path: "/tmp/audio.m4a", ContentType: "audio/mp4"},
		Language:    "auto",
		Speed:       "1.25x",
	})
	if err != nil {
		t.Fatalf("TranscribeTeamsMedia error: %v", err)
	}
	if transcript.Text != "plain transcript" || transcript.Backend != "command" {
		t.Fatalf("plain transcript = %#v", transcript)
	}
}

func TestCommandASRTranscriberReturnsStderrOnFailure(t *testing.T) {
	transcriber := &CommandASRTranscriber{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestCommandASRTranscriberHelperProcess$"},
		Env:     []string{"CODEX_HELPER_TEST_ASR_COMMAND=fail"},
	}
	_, err := transcriber.TranscribeTeamsMedia(context.Background(), ASRTranscribeInput{
		SourceIndex: 1,
		File:        LocalAttachment{Path: "/tmp/audio.m4a", ContentType: "audio/mp4"},
	})
	if err == nil || !strings.Contains(err.Error(), "bad audio") {
		t.Fatalf("failure error = %v, want stderr detail", err)
	}
}

func TestCommandASRTranscriberHelperProcess(t *testing.T) {
	mode := os.Getenv("CODEX_HELPER_TEST_ASR_COMMAND")
	if mode == "" {
		return
	}
	switch mode {
	case "json":
		args := commandASRTranscriberHelperArgs()
		if len(args) != 2 || args[0] != "/tmp/audio.m4a" || args[1] != "4" {
			_, _ = fmt.Fprintf(os.Stderr, "unexpected args: %#v\n", args)
			os.Exit(2)
		}
		for _, key := range []string{"CODEX_HELPER_TEAMS_ASR_THREADS", "GOMAXPROCS", "OMP_NUM_THREADS"} {
			if os.Getenv(key) != "4" {
				_, _ = fmt.Fprintf(os.Stderr, "%s=%q, want 4\n", key, os.Getenv(key))
				os.Exit(3)
			}
		}
		if os.Getenv("TOKENIZERS_PARALLELISM") != "false" {
			_, _ = fmt.Fprintf(os.Stderr, "TOKENIZERS_PARALLELISM=%q, want false\n", os.Getenv("TOKENIZERS_PARALLELISM"))
			os.Exit(4)
		}
		_, _ = fmt.Fprint(os.Stdout, `{"text":"hello 测试","language":"zh","model":"helper-model","backend":"helper"}`)
	case "plain":
		_, _ = fmt.Fprintln(os.Stdout, "plain transcript")
	case "fail":
		_, _ = fmt.Fprintln(os.Stderr, "bad audio")
		os.Exit(9)
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown mode: %s\n", mode)
		os.Exit(2)
	}
	os.Exit(0)
}

func commandASRTranscriberHelperArgs() []string {
	for i, arg := range os.Args {
		if arg == "--" {
			return os.Args[i+1:]
		}
	}
	return nil
}

func TestASRCommandEnvCapsCPUThreadVariables(t *testing.T) {
	env := asrCommandEnv(
		[]string{
			"GOMAXPROCS=12",
			"OMP_NUM_THREADS=16",
			"MKL_NUM_THREADS=2",
			"CODEX_HELPER_TEAMS_ASR_THREADS=99",
			"TOKENIZERS_PARALLELISM=true",
			"UNRELATED=value",
		},
		[]string{
			"OPENBLAS_NUM_THREADS=32",
			"TOKENIZERS_PARALLELISM=true",
			"CODEX_HELPER_TEAMS_ASR_THREADS=99",
		},
		ASRTranscribeInput{SourceIndex: 2, Language: "auto", Speed: "1.25x"},
	)
	values := envSliceToMap(env)
	if values["OMP_NUM_THREADS"] != "4" {
		t.Fatalf("OMP_NUM_THREADS = %q, want 4", values["OMP_NUM_THREADS"])
	}
	if values["MKL_NUM_THREADS"] != "2" {
		t.Fatalf("MKL_NUM_THREADS = %q, want existing value under cap", values["MKL_NUM_THREADS"])
	}
	if values["TORCH_NUM_THREADS"] != "4" {
		t.Fatalf("TORCH_NUM_THREADS = %q, want default cap", values["TORCH_NUM_THREADS"])
	}
	if values["GOMAXPROCS"] != "4" || values["ONNX_NUM_THREADS"] != "4" || values["OMP_THREAD_LIMIT"] != "4" || values["OPENBLAS_NUM_THREADS"] != "4" {
		t.Fatalf("extended thread env not capped: %#v", values)
	}
	if values["CODEX_HELPER_TEAMS_ASR_THREADS"] != "4" || values["TOKENIZERS_PARALLELISM"] != "false" {
		t.Fatalf("missing ASR runtime controls: %#v", values)
	}
	if values["CODEX_HELPER_TEAMS_ASR_SPEED"] != "1.25x" || values["CODEX_HELPER_TEAMS_ASR_LANGUAGE"] != "auto" {
		t.Fatalf("missing ASR metadata env: %#v", values)
	}
}
