package modelprofile

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLiveCodexAppServerModelListUsesGeneratedCatalogOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_MODEL_CATALOG")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_CODEX_MODEL_CATALOG=1 to verify generated catalogs against a real Codex app-server model/list")
	}
	codexPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_PATH"))
	if codexPath == "" {
		codexPath = strings.TrimSpace(os.Getenv("CODEX_CLI"))
	}
	if codexPath == "" {
		codexPath = "codex"
	}
	if _, err := exec.LookPath(codexPath); err != nil && !filepath.IsAbs(codexPath) {
		t.Fatalf("find codex executable %q: %v", codexPath, err)
	}

	spec, err := MustLookupProvider("mimo")
	if err != nil {
		t.Fatalf("lookup mimo: %v", err)
	}
	catalog, err := CodexModelCatalogJSON(spec)
	if err != nil {
		t.Fatalf("CodexModelCatalogJSON: %v", err)
	}

	tmp := t.TempDir()
	catalogPath := filepath.Join(tmp, "catalog.json")
	if err := os.WriteFile(catalogPath, catalog, 0o600); err != nil {
		t.Fatalf("write catalog: %v", err)
	}
	codexHome := filepath.Join(tmp, "codex-home")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatalf("mkdir codex home: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, codexPath,
		"-c", `model_catalog_json="`+tomlEscape(catalogPath)+`"`,
		"-c", `model="`+tomlEscape(spec.DefaultPublicModel())+`"`,
		"app-server", "--listen", "stdio://",
	)
	cmd.Env = append(os.Environ(), "CODEX_HOME="+codexHome, "CODEX_DIR="+codexHome)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start Codex app-server: %v", err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	enc := json.NewEncoder(stdin)
	scanner := bufio.NewScanner(stdout)
	mustSendLiveCodexAppServerRequest(t, enc, 1, "initialize", map[string]any{
		"clientInfo":   map[string]string{"name": "codex-helper-catalog-live-test", "version": "0"},
		"capabilities": nil,
	})
	if msg := readLiveCodexAppServerResponse(t, scanner, 1, stderr.String()); msg.Error != nil {
		t.Fatalf("initialize failed: %#v\nstderr:\n%s", msg.Error, stderr.String())
	}
	if err := enc.Encode(map[string]any{"jsonrpc": "2.0", "method": "initialized", "params": map[string]any{}}); err != nil {
		t.Fatalf("send initialized: %v", err)
	}
	mustSendLiveCodexAppServerRequest(t, enc, 2, "model/list", map[string]any{})
	msg := readLiveCodexAppServerResponse(t, scanner, 2, stderr.String())
	if msg.Error != nil {
		t.Fatalf("model/list failed: %#v\nstderr:\n%s", msg.Error, stderr.String())
	}

	var result struct {
		Data []struct {
			Model     string `json:"model"`
			IsDefault bool   `json:"isDefault"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg.Result, &result); err != nil {
		t.Fatalf("decode model/list result: %v\nraw=%s", err, msg.Result)
	}
	if len(result.Data) < 2 {
		t.Fatalf("model/list returned too few models: %#v", result.Data)
	}
	if result.Data[0].Model != "mimo/mimo-v2.5" || !result.Data[0].IsDefault {
		t.Fatalf("first/default model = %#v, want mimo/mimo-v2.5 default", result.Data[0])
	}
	if result.Data[1].Model != "mimo/mimo-v2.5-pro" || result.Data[1].IsDefault {
		t.Fatalf("second model = %#v, want mimo/mimo-v2.5-pro non-default", result.Data[1])
	}
}

type liveCodexAppServerMessage struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  map[string]any  `json:"error"`
}

func mustSendLiveCodexAppServerRequest(t *testing.T, enc *json.Encoder, id int, method string, params map[string]any) {
	t.Helper()
	if err := enc.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		t.Fatalf("send %s: %v", method, err)
	}
}

func readLiveCodexAppServerResponse(t *testing.T, scanner *bufio.Scanner, id int, stderr string) liveCodexAppServerMessage {
	t.Helper()
	for scanner.Scan() {
		var msg liveCodexAppServerMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		if msg.ID == id {
			return msg
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read app-server response %d: %v\nstderr:\n%s", id, err, stderr)
	}
	t.Fatalf("app-server exited before response %d\nstderr:\n%s", id, stderr)
	return liveCodexAppServerMessage{}
}

func tomlEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `"`, `\"`)
}
