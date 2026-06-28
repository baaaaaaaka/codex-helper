package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestCodexExecJSONEventIncludesRetryDiagnostics(t *testing.T) {
	raw := codexExecJSONEvent(codexrunner.StreamEvent{
		Kind:     codexrunner.StreamEventStreamRetry,
		ThreadID: "thread-1",
		TurnID:   "turn-1",
		Failure: &codexrunner.TurnFailure{
			Code:    "responseStreamDisconnected",
			Message: "Reconnecting... 1/5: upstream returned HTTP 502",
		},
	})
	for _, want := range []string{`"type":"turn.retrying"`, `"thread_id":"thread-1"`, `"Code":"responseStreamDisconnected"`, "upstream returned HTTP 502"} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("retry JSON missing %q: %s", want, raw)
		}
	}
}

func TestRunCodexExecFacadeUsesStandardApprovalAfterFixedDelay(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX app-server fixture")
	}
	dir := t.TempDir()
	codexPath := filepath.Join(dir, "codex")
	script := `#!/bin/sh
set -eu
case "${1:-}" in
  --version) echo 'codex-cli 0.133.0'; exit 0 ;;
  --help) echo 'Options: --remote <ADDR> --remote-auth-token-env <ENV_VAR>'; exit 0 ;;
  app-server)
    while IFS= read -r line; do
      id=$(printf %s "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
      case "$line" in
        *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
        *'"method":"thread/list"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"data":[]}}\n' "$id" ;;
        *'"method":"thread/start"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-runtime"}}}\n' "$id" ;;
        *'"method":"turn/start"'*)
          printf '{"jsonrpc":"2.0","id":%s,"result":{"turn":{"id":"turn-runtime","status":"inProgress","items":[]}}}\n' "$id"
          printf '%s\n' '{"jsonrpc":"2.0","id":99,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-runtime","turnId":"turn-runtime"}}'
          IFS= read -r approval
          case "$approval" in
            *'"id":99'*'"decision":"accept"'*) ;;
            *) exit 65 ;;
          esac
          printf '%s\n' '{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-runtime","turnId":"turn-runtime","item":{"id":"item-final","type":"agentMessage","text":"runtime done"}}}'
          printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-runtime","turn":{"id":"turn-runtime","status":"completed","items":[]}}}'
          ;;
      esac
    done
    ;;
  *) exit 64 ;;
esac
`
	if err := os.WriteFile(codexPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	store := newCodexOpenTestStore(t)
	var output bytes.Buffer
	started := time.Now()
	err := runCodexCLIInvocation(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil,
		[]string{codexPath, "exec", "--json", "run hardware probe"}, false,
		runTargetOptions{Cwd: dir, Stdin: strings.NewReader(""), Stdout: &output, Stderr: io.Discard, Log: io.Discard})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("runCodexCLIInvocation: %v", err)
	}
	if elapsed < 500*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("approval elapsed = %s, want fixed 500ms plus startup overhead", elapsed)
	}
	text := output.String()
	if !strings.Contains(text, `"type":"item.completed"`) || !strings.Contains(text, `"text":"runtime done"`) || !strings.Contains(text, `"type":"turn.completed"`) {
		t.Fatalf("JSONL output = %s", text)
	}
}

func TestLiveCodexExecFacadeUsesOriginalBinaryAndAssignedGPU(t *testing.T) {
	codexPath := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX"))
	if codexPath == "" {
		t.Skip("set CXP_LIVE_CODEX to an original Codex native binary")
	}
	before, err := hashFileSHA256(codexPath)
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	sourceHome := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX_HOME"))
	if sourceHome == "" {
		sourceHome = filepath.Join(home, ".codex")
	}
	isolatedHome := filepath.Join(t.TempDir(), "codex-home")
	if err := os.MkdirAll(isolatedHome, 0o700); err != nil {
		t.Fatal(err)
	}
	auth, err := os.ReadFile(filepath.Join(sourceHome, "auth.json"))
	if err != nil {
		t.Fatalf("read live auth: %v", err)
	}
	if err := os.WriteFile(filepath.Join(isolatedHome, "auth.json"), auth, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envCodexHome, isolatedHome)
	t.Setenv("CODEX_DIR", isolatedHome)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	direct := false
	if err := store.Save(config.Config{ProxyEnabled: &direct, RuntimeGeneration: currentRuntimeGeneration}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := []string{codexPath}
	if model := strings.TrimSpace(os.Getenv("CXP_LIVE_MODEL")); model != "" {
		command = append(command, "--model", model)
	}
	command = append(command, "exec", "Use shell_command to run exactly: nvidia-smi --query-gpu=name --format=csv,noheader. Report the exact command output.")
	err = runCodexCLIInvocation(ctx, &rootOptions{configPath: store.Path()}, store, nil, nil,
		command, false,
		runTargetOptions{Cwd: t.TempDir(), Stdin: strings.NewReader(""), Stdout: &output, Stderr: io.Discard, Log: io.Discard})
	if err != nil {
		t.Fatalf("live exec facade: %v", err)
	}
	if !strings.Contains(output.String(), "NVIDIA") {
		t.Fatalf("live output did not contain the assigned GPU: %s", output.String())
	}
	after, err := hashFileSHA256(codexPath)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("original Codex binary changed during live run")
	}
}

func TestSplitCodexCLIInvocationMigratesLegacyArguments(t *testing.T) {
	invocation, err := splitCodexCLIInvocation([]string{
		"--yolo", "--sandbox", "danger-full-access", "-c", `approval_policy="never"`,
		"-m", "gpt-test", "exec", "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if invocation.Command != "exec" || strings.Join(invocation.GlobalArgs, " ") != "-m gpt-test" || strings.Join(invocation.Args, " ") != "hello" {
		t.Fatalf("invocation = %#v", invocation)
	}
	joined := fmt.Sprint(invocation)
	for _, forbidden := range []string{"--yolo", "danger-full-access", "approval_policy"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("migrated invocation retained %q: %#v", forbidden, invocation)
		}
	}
}

func TestSplitCodexCLIInvocationPreservesInteractivePromptAndInputs(t *testing.T) {
	invocation, err := splitCodexCLIInvocation([]string{"--image", "shot.png", "--add-dir", "/data", "inspect", "this"})
	if err != nil {
		t.Fatal(err)
	}
	if invocation.Command != "" || strings.Join(invocation.Args, " ") != "inspect this" {
		t.Fatalf("invocation = %#v", invocation)
	}
	if len(invocation.ImagePaths) != 1 || len(invocation.AdditionalDirs) != 1 {
		t.Fatalf("inputs were not preserved: %#v", invocation)
	}
}

func TestCodexExecAliasPreservesGlobalImagesAndAdditionalDirsInOrder(t *testing.T) {
	invocation, err := splitCodexCLIInvocation([]string{
		"--image", "first.png", "--image", "second.png",
		"--add-dir", "/data/one", "--add-dir", "/data/two",
		"e", "inspect",
	})
	if err != nil {
		t.Fatal(err)
	}
	if invocation.Command != "e" {
		t.Fatalf("command = %q, want e", invocation.Command)
	}
	want := []string{
		"--image", "first.png", "--image", "second.png",
		"--add-dir", "/data/one", "--add-dir", "/data/two",
		"inspect",
	}
	if got := codexFacadeArgsWithGlobalInputs(invocation, invocation.Args); !reflect.DeepEqual(got, want) {
		t.Fatalf("facade args = %#v, want %#v", got, want)
	}
}

func TestCodexReviewArgsUseAppServerFacadePrompt(t *testing.T) {
	args, err := codexReviewArgsToExecArgs([]string{"--commit", "abc123", "--title", "Fix race", "--json"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"--json", "commit abc123", "Fix race"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("review args missing %q: %v", want, args)
		}
	}
}

func TestParseCodexExecFacadeOutputSchemaAndAdditionalDir(t *testing.T) {
	dir := t.TempDir()
	schema := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(schema, []byte(`{"type":"object"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	options, err := parseCodexExecFacadeArgs([]string{"--add-dir", "/extra", "--output-schema", schema, "prompt"}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.AdditionalDirs) != 1 || options.AdditionalDirs[0] != "/extra" || !json.Valid(options.OutputSchema) {
		t.Fatalf("options = %#v", options)
	}
}

func TestCodexInvocationPassesThroughCurrentAndFutureNativeCommands(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX command fixture")
	}
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	codexPath := filepath.Join(dir, "codex")
	script := fmt.Sprintf(`#!/bin/sh
case "${1:-}" in
  --version) echo 'codex-cli 0.133.0' ;;
  --help) printf 'Commands:\n  plugin  Manage plugins\n  future-command  Future diagnostic that wraps\n                  onto another line\n\nOptions:\n' ;;
  plugin|future-command) printf '%%s\n' "$@" > %s ;;
  *) exit 64 ;;
esac
`, shellSingleQuoteForBeaconCLITest(argsPath))
	if err := os.WriteFile(codexPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	store := newCodexOpenTestStore(t)
	for _, invocation := range [][]string{{codexPath, "plugin", "list"}, {codexPath, "future-command", "inspect"}} {
		if err := runCodexCLIInvocation(context.Background(), &rootOptions{configPath: store.Path()}, store, nil, nil, invocation, false, runTargetOptions{Cwd: dir, Log: io.Discard}); err != nil {
			t.Fatalf("%v: %v", invocation, err)
		}
		got := readArgLines(t, argsPath)
		if strings.Join(got, " ") != strings.Join(invocation[1:], " ") {
			t.Fatalf("native args = %#v, want %#v", got, invocation[1:])
		}
	}
	if commands := discoverCodexTopLevelCommands(context.Background(), codexPath); commands["onto"] {
		t.Fatalf("wrapped help description was parsed as a command: %#v", commands)
	}
}

func TestCodexInvocationRoutesLoaderOptionsAndHelpThroughNativeCLI(t *testing.T) {
	tests := []struct {
		args []string
		want bool
	}{
		{args: []string{"--help"}, want: true},
		{args: []string{"exec", "--ignore-user-config", "prompt"}, want: true},
		{args: []string{"exec", "--profile", "work", "prompt"}, want: true},
		{args: []string{"exec", "prompt"}, want: false},
	}
	for _, test := range tests {
		invocation, err := splitCodexCLIInvocation(test.args)
		if err != nil {
			t.Fatal(err)
		}
		if got := codexInvocationUsesNativeCLI(invocation, test.args); got != test.want {
			t.Fatalf("args=%v native=%v want=%v", test.args, got, test.want)
		}
	}
}
