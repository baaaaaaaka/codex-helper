package transcript

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkTrackerProgressHistory(b *testing.B) {
	var records []string
	records = append(records, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"t"}}`, `{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"question"}]}}`, `{"type":"event_msg","payload":{"type":"item_completed","turn_id":"t","item":{"type":"UserMessage","client_id":"local","content":[]}}}`)
	for i := 0; i < 1000; i++ {
		records = append(records, fmt.Sprintf(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"entry-%d"}]}}`, i))
	}
	records = append(records, `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"t","last_agent_message":"answer"}}`)
	var lines []Line
	var off int64
	for _, r := range records {
		line := Line{Off: off, End: off + int64(len(r)) + 1, Data: []byte(r)}
		lines = append(lines, line)
		off = line.End
	}
	b.SetBytes(int64(len(strings.Join(records, "\n"))))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr := NewTracker("thread", 0, 0, nil)
		if stable, ok := any(tr).(interface{ StableFallbackIDs() }); ok {
			stable.StableFallbackIDs()
		}
		out := tr.Feed(lines)
		if len(out) != 1002 {
			b.Fatalf("outputs %d", len(out))
		}
	}
}
