package testphase

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestEmitWritesOneStructuredEventWhenEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "phases", "events.jsonl")
	t.Setenv("CODEX_HELPER_CI_PHASE_FILE", path)
	t.Setenv("CODEX_HELPER_CI_TEST_NAME", "TestExample")

	Emit("fixture_ready", map[string]string{"backend": "sqlite"})

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open phase file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		t.Fatalf("phase file is empty: %v", scanner.Err())
	}
	var event Event
	if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
		t.Fatalf("decode phase event: %v", err)
	}
	if event.Schema != schemaVersion || event.Phase != "fixture_ready" || event.Test != "TestExample" || event.PID <= 0 {
		t.Fatalf("phase event = %#v", event)
	}
	if event.Fields["backend"] != "sqlite" || event.UnixNano <= 0 || event.Seq == 0 {
		t.Fatalf("phase event fields = %#v", event)
	}
}

func TestEmitIsInertWithoutPhaseFile(t *testing.T) {
	t.Setenv("CODEX_HELPER_CI_PHASE_FILE", "")
	Emit("ignored", nil)
}
