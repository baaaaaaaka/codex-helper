package transcript

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const testHeartbeat = "<heartbeat>\n  <automation_id>watch-pr_123</automation_id>\n  <current_time_iso>2026-11-14T15:30:00+08:00</current_time_iso>\n  <instructions>\n检查 PR，有新意见就处理。\n  </instructions>\n</heartbeat>\n"

func scheduledRecord(typ string, payload any) string {
	b, _ := json.Marshal(map[string]any{"type": typ, "payload": payload})
	return string(b)
}

func scheduledUser(text string, metadata any) string {
	return scheduledRecord("response_item", map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": text}}, "internal_chat_message_metadata_passthrough": metadata})
}

func scheduledLines(records ...string) []Line {
	var lines []Line
	var off int64
	for _, r := range records {
		end := off + int64(len(r)) + 1
		lines = append(lines, Line{Off: off, End: end, Data: []byte(r)})
		off = end
	}
	return lines
}

func TestScheduledHeartbeatRequiresHostEvidence(t *testing.T) {
	marker := map[string]any{"content_item_kinds": []string{"user.heartbeat"}}
	for _, tc := range []struct {
		name, text string
		metadata   any
		want       bool
	}{
		{"native", testHeartbeat, marker, true},
		{"quoted by user", testHeartbeat, nil, false},
		{"unknown kind", testHeartbeat, map[string]any{"content_item_kinds": []string{"future.kind"}}, false},
		{"mixed kinds", testHeartbeat, map[string]any{"content_item_kinds": []string{"user.heartbeat", "user.text"}}, false},
		{"malformed metadata", testHeartbeat, []int{1}, false},
		{"bad timestamp", strings.Replace(testHeartbeat, "2026-11-14T15:30:00+08:00", "", 1), marker, false},
		{"bad id", strings.Replace(testHeartbeat, "watch-pr_123", "<script>", 1), marker, false},
		{"oversized", strings.Replace(testHeartbeat, "检查 PR", strings.Repeat("x", 4000), 1), marker, false},
		{"unfamiliar envelope", "quoted:\n" + testHeartbeat, marker, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := Parse([]byte(scheduledUser(tc.text, tc.metadata)))
			if ev.Kind != KindUserMessage || (ev.Schedule != nil) != tc.want {
				t.Fatalf("event: %+v", ev)
			}
			if tc.want && (ev.Schedule.ID != "watch-pr_123" || ev.Schedule.Instructions != "检查 PR，有新意见就处理。") {
				t.Fatalf("schedule: %+v", ev.Schedule)
			}
		})
	}
	for _, extra := range []any{map[string]any{"type": "input_text", "text": "extra"}, map[string]any{"type": "input_image", "image_url": "local"}} {
		raw := scheduledRecord("response_item", map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": testHeartbeat}, extra}, "internal_chat_message_metadata_passthrough": marker})
		if ev := Parse([]byte(raw)); ev.Kind != KindUserMessage || ev.Schedule != nil {
			t.Fatalf("extra content accepted as a heartbeat: %+v", ev)
		}
	}
}

func TestScheduledAttributionFailsOpen(t *testing.T) {
	for _, tc := range []struct {
		name        string
		attribution any
		want        bool
	}{
		{"cron", map[string]any{"turn_id": "t", "turn_trigger": "automation_cron_scheduled"}, true},
		{"manual run", map[string]any{"turn_id": "t", "turn_trigger": "automation_cron_run_now"}, true},
		{"ordinary", map[string]any{"turn_id": "t", "turn_trigger": "composer"}, false},
		{"wrong turn", map[string]any{"turn_id": "other", "turn_trigger": "automation_cron_scheduled"}, false},
		{"unknown", map[string]any{"turn_id": "t", "turn_trigger": "future_automation"}, false},
		{"malformed object", map[string]any{"turn_id": 12}, false},
		{"malformed type", []int{1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := Parse([]byte(scheduledRecord("event_msg", map[string]any{"type": "task_started", "turn_id": "t", "turn_attribution": tc.attribution})))
			if ev.Kind != KindTurnStarted || ev.TurnID != "t" || (ev.Schedule != nil) != tc.want {
				t.Fatalf("event %+v", ev)
			}
		})
	}
	if ev := Parse([]byte(`{"type":"event_msg","payload":{"type":"task_started","turn_attribution":{"turn_trigger":"automation_cron_scheduled"}}}`)); ev.Schedule != nil {
		t.Fatal("attribution without a turn ID was accepted")
	}
}

func TestScheduledTrackerScopedMixedAndReplay(t *testing.T) {
	start := `{"type":"event_msg","payload":{"type":"task_started","turn_id":"t"}}`
	ack := `{"type":"event_msg","payload":{"type":"item_completed","turn_id":"t","item":{"type":"UserMessage","client_id":"local"}}}`
	answer := `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","id":"answer","content":[{"type":"output_text","text":"结果"}]}}`
	end := `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"t"}}`
	lines := scheduledLines(start, scheduledUser(testHeartbeat, map[string]any{"content_item_kinds": []string{"user.heartbeat"}}), ack, answer, end,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"ordinary"}}`, scheduledUser("普通问题", nil), answer)
	tr := NewTracker("th", 0, 0, nil)
	tr.StableFallbackIDs()
	var out []Output
	for _, l := range lines {
		out = append(out, tr.Feed([]Line{l})...)
	}
	if len(out) != 4 {
		t.Fatalf("outputs %+v", out)
	}
	if out[0].Schedule == nil || out[1].Schedule == nil || out[1].Schedule.Mixed || !out[0].Schedule.Input || out[0].Text != strings.TrimSpace(testHeartbeat) {
		t.Fatalf("scheduled outputs %+v", out[:2])
	}
	if out[2].Schedule != nil || out[3].Schedule != nil {
		t.Fatal("schedule leaked into next turn")
	}
	// Existing identities must not change when the same text lacks provenance.
	plain := NewTracker("th", 0, 0, nil)
	plain.StableFallbackIDs()
	plainLines := scheduledLines(start, scheduledUser(testHeartbeat, nil), ack, answer, end)
	plainOut := plain.Feed(plainLines)
	if len(plainOut) != 1 || plainOut[0].RecordID != out[1].RecordID {
		t.Fatalf("answer identity changed: %+v", plainOut)
	}
	// Reconstruct an open turn from its existing checkpoint, without a new store.
	tr = NewTracker("th", 0, 0, nil)
	tr.StableFallbackIDs()
	tr.Feed(lines[:3])
	off, line := tr.Resume()
	replay := NewTracker("th", off, line, nil)
	replay.StableFallbackIDs()
	var remaining []Line
	for _, l := range lines[:5] {
		if l.Off >= off {
			remaining = append(remaining, l)
		}
	}
	if got := replay.Feed(remaining); !reflect.DeepEqual(got, out[:2]) {
		t.Fatalf("replay mismatch\n%+v\n%+v", got, out[:2])
	}
	// Human input can join an automated turn, but is never rewritten as instructions.
	mixed := NewTracker("th", 0, 0, nil)
	mixed.StableFallbackIDs()
	ml := scheduledLines(start, scheduledUser(testHeartbeat, map[string]any{"content_item_kinds": []string{"user.heartbeat"}}), ack, scheduledUser("先别改代码", nil), ack, answer, end)
	mo := mixed.Feed(ml)
	if len(mo) != 3 || !mo[2].Schedule.Mixed || mo[1].Schedule.Input || mo[1].Text != "先别改代码" {
		t.Fatalf("mixed outputs %+v", mo)
	}
}

func TestScheduledCronInitialPromptIsNotMixed(t *testing.T) {
	tr := NewTracker("th", 0, 0, nil)
	out := tr.Feed(scheduledLines(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"t","turn_attribution":{"turn_id":"t","turn_trigger":"automation_cron_scheduled"}}}`, `{"type":"turn_context","payload":{"turn_id":"t","model":"model"}}`, scheduledUser("Check the PR", nil), `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"t","last_agent_message":"Done"}}`))
	if len(out) != 2 || out[1].Schedule == nil || out[1].Schedule.Mixed || !out[0].Schedule.Input {
		t.Fatalf("outputs %+v", out)
	}
}

func TestScheduledBufferedPromptsKeepTheirOwnInstructions(t *testing.T) {
	marker := map[string]any{"content_item_kinds": []string{"user.heartbeat"}}
	second := strings.ReplaceAll(strings.ReplaceAll(testHeartbeat, "watch-pr_123", "second-task"), "检查 PR，有新意见就处理。", "只检查 CI。")
	start := `{"type":"event_msg","payload":{"type":"task_started","turn_id":"t"}}`
	end := `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"t","last_agent_message":"结果"}}`
	for _, humanFirst := range []bool{false, true} {
		first := scheduledUser(testHeartbeat, marker)
		if humanFirst {
			first = scheduledUser("原来的普通请求", nil)
		}
		// No UserMessage acknowledgement between the inputs: release them together.
		tr := NewTracker("th", 0, 0, nil)
		out := tr.Feed(scheduledLines(start, first, scheduledUser(second, marker), end))
		if len(out) != 3 {
			t.Fatalf("outputs %+v", out)
		}
		if humanFirst {
			if out[0].Schedule != nil || out[0].Text != "原来的普通请求" {
				t.Fatal("ordinary input was retroactively relabeled")
			}
		} else if out[0].Schedule.ID != "watch-pr_123" || out[0].Schedule.Instructions != "检查 PR，有新意见就处理。" {
			t.Fatalf("first scheduled prompt acquired later instructions: %+v", out[0].Schedule)
		}
		if out[1].Schedule.ID != "second-task" || out[1].Schedule.Instructions != "只检查 CI。" || !out[1].Schedule.Input || !out[2].Schedule.Mixed || out[2].Schedule.Input {
			t.Fatalf("later source lost or result mislabeled: %+v", out)
		}
	}
}
