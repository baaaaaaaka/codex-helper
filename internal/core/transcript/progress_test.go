package transcript

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

func TestHelperProgressSelectiveOwnershipAndTerminal(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	open, err := w.BeginTurn(rollout.Turn{ID: "owned", ClientID: "cxp-owned", Prompt: "User message:\nquestion"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := w.AppendRecord("response_item", map[string]any{"type": "message", "id": fmt.Sprint("msg", i), "role": "assistant", "phase": "commentary", "content": []any{map[string]any{"type": "output_text", "text": "same text"}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.FinishTurn(open, rollout.Turn{Answer: "owned final"}); err != nil {
		t.Fatal(err)
	}
	skip := func(client, turn string) bool { return strings.HasPrefix(client, "cxp-") || turn == "owned" }
	legacy := NewTracker(w.SessionID, 0, 0, skip)
	if got := readAll(t, w.Path, legacy); len(got) != 0 {
		t.Fatalf("upgrade replays legacy helper execution: %+v", got)
	}
	tr := NewTracker(w.SessionID, 0, 0, skip)
	tr.StableFallbackIDs()
	tr.HelperProgress(func(id string) bool { return id == "owned" })
	got := readAll(t, w.Path, tr)
	if len(got) != 3 || got[0].Kind != OutputStatus || got[1].Kind != OutputStatus || got[2].Kind != OutputEnd || got[0].RecordID == got[1].RecordID {
		t.Fatalf("selective outputs: %+v", got)
	}
	// Native rollout writes may precede the Started callback. The durable
	// client key is known before Run, so no native-id lookup is required.
	early := NewTracker(w.SessionID, 0, 0, skip)
	early.StableFallbackIDs()
	early.HelperProgressByClient(func(client, _ string) bool { return client == "cxp-owned" })
	got = readAll(t, w.Path, early)
	if len(got) != 3 || got[0].Kind != OutputStatus || got[2].Kind != OutputEnd {
		t.Fatalf("progress before native callback lost: %+v", got)
	}
}

func TestStableUnphasedProgressLookBehind(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	open, err := w.BeginTurn(rollout.Turn{ID: "t", Prompt: "question"})
	if err != nil {
		t.Fatal(err)
	}
	tr := NewTracker(w.SessionID, 0, 0, nil)
	tr.StableFallbackIDs()
	_ = readAll(t, w.Path, tr)
	for i, text := range []string{"first intermediate", "second intermediate", "the answer"} {
		if err := w.AppendRecord("response_item", map[string]any{"type": "message", "id": fmt.Sprint("m", i), "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": text}}}); err != nil {
			t.Fatal(err)
		}
		got := readAll(t, w.Path, tr)
		if i == 0 && len(got) != 0 {
			t.Fatal("last unphased message published eagerly")
		}
		if i > 0 && (len(got) != 1 || got[0].Kind != OutputStatus) {
			t.Fatalf("look-behind output %+v", got)
		}
	}
	if err := w.FinishTurn(open, rollout.Turn{Answer: "", Unphased: true}); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, w.Path, tr)
	if len(got) != 1 || got[0].Kind != OutputFinal || got[0].Text != "the answer" {
		t.Fatalf("unphased final: %+v", got)
	}
}

func TestAbortFlushesUnphasedProgressBeforeTerminal(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	open, err := w.BeginTurn(rollout.Turn{ID: "aborted", ClientID: "cxp-aborted", Prompt: "question"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRecord("response_item", map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "last progress before cancellation"}}}); err != nil {
		t.Fatal(err)
	}
	if err := w.FinishTurn(open, rollout.Turn{Aborted: true}); err != nil {
		t.Fatal(err)
	}
	tr := NewTracker(w.SessionID, 0, 0, func(client, _ string) bool { return strings.HasPrefix(client, "cxp-") })
	tr.StableFallbackIDs()
	tr.HelperProgress(func(id string) bool { return id == "aborted" })
	got := readAll(t, w.Path, tr)
	if len(got) != 2 || got[0].Kind != OutputStatus || got[1].Kind != OutputEnd {
		t.Fatalf("abort terminal overtook pending progress: %+v", got)
	}
}

func TestStableFallbackIDsSurviveUnrelatedLineShifts(t *testing.T) {
	w := newRollout(t, rollout.Options{})
	mustTurn(t, w, rollout.Turn{Prompt: "question", Answer: "answer"})
	data, err := os.ReadFile(w.Path)
	if err != nil {
		t.Fatal(err)
	}
	// Remove actual message IDs, then insert an unrelated tool record.
	data = stripMessageIDs(t, data)
	if err := os.WriteFile(w.Path, data, 0600); err != nil {
		t.Fatal(err)
	}
	tracker := func() *Tracker { tr := NewTracker(w.SessionID, 0, 0, nil); tr.StableFallbackIDs(); return tr }
	before := readAll(t, w.Path, tracker())
	shifted := append([]byte("{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"output\":\"tool\"}}\n"), data...)
	if err := os.WriteFile(w.Path, shifted, 0600); err != nil {
		t.Fatal(err)
	}
	after := readAll(t, w.Path, tracker())
	if len(before) != len(after) {
		t.Fatal("line shift changed selected records")
	}
	for i := range before {
		if before[i].RecordID != after[i].RecordID || before[i].LegacyRecordID == after[i].LegacyRecordID {
			t.Fatalf("unstable fallback identity: before=%+v after=%+v", before[i], after[i])
		}
	}
}

func stripMessageIDs(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if p, ok := record["payload"].(map[string]any); ok && record["type"] == "response_item" && p["type"] == "message" {
			delete(p, "id")
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(encoded)
		b.WriteByte('\n')
	}
	return b.Bytes()
}
