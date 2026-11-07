package transcript

import (
	"strings"
	"testing"
)

// The goal and model settings a rollout records (2026-11-07): the outputs
// carry what the records before them said; a goal of another thread (a
// subagent's) is ignored.
func TestTrackerInfoFromGoalAndSettingsRecords(t *testing.T) {
	ts := `"timestamp":"2026-11-07T03:04:05.000Z"`
	p := writeLines(t,
		`{`+ts+`,"type":"session_meta","payload":{"id":"thr-1","timestamp":"2026-11-07T03:04:05.000Z","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"thread_settings_applied","thread_id":"thr-1","thread_settings":{"model":"gpt-a","service_tier":"priority","reasoning_effort":"high"}}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"thread_goal_updated","threadId":"thr-1","goal":{"threadId":"thr-1","objective":"Finish it","status":"active","tokensUsed":5}}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"thread_goal_updated","threadId":"thr-sub","goal":{"objective":"Subagent goal","status":"active"}}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"task_started","turn_id":"t1"}}`+"\n",
		`{`+ts+`,"type":"turn_context","payload":{"turn_id":"t1","cwd":"/w","model":"gpt-b","effort":"low"}}`+"\n",
		`{`+ts+`,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"go"}]}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","client_id":"local"}}}`+"\n",
		`{`+ts+`,"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"done"}]}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"thread_goal_updated","threadId":"thr-1","goal":{"objective":"Finish it","status":"complete"}}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"task_complete","turn_id":"t1","last_agent_message":"done"}}`+"\n",
	)
	tr := NewTracker("", 0, 0, nil)
	out := readAll(t, p, tr)
	if got := keys(out); got != "U:go | F:done" {
		t.Fatalf("outputs %q", got)
	}
	f := out[1].Info
	if f.Goal == nil || f.Goal.Objective != "Finish it" || f.Goal.Status != "active" || f.Model != "gpt-b" || f.Effort != "low" || f.Tier != "priority" {
		t.Fatalf("final info %+v goal %+v", f, f.Goal)
	}
	if i := tr.Info(); i.Goal == nil || i.Goal.Status != "complete" {
		t.Fatalf("tracker info after the turn %+v", i)
	}
	// A tracker seeded with that state, as after a restart.
	tr2 := NewTracker("thr-1", 0, 0, nil)
	tr2.SetInfo(Info{Goal: &Goal{Objective: "Seeded", Status: "active"}, Tier: "flex"})
	if i := tr2.Info(); i.Goal.Objective != "Seeded" || i.Tier != "flex" {
		t.Fatalf("seeded %+v", i)
	}
}

// A turn Codex starts by itself (goal continuation: no prompt, its
// instructions an internal context record) posts its commentary as it
// comes, not at the turn's end; a prompt the helper steers into it
// later waits for its user item, which drops it and the rest of the turn.
func TestTrackerPromptlessTurnIsDecidedAtItsFirstStatus(t *testing.T) {
	ts := `"timestamp":"2026-11-07T03:04:05.000Z"`
	head := []string{
		`{` + ts + `,"type":"session_meta","payload":{"id":"thr-1","timestamp":"2026-11-07T03:04:05.000Z","cwd":"/w","originator":"codex_cli_rs","source":"cli"}}` + "\n",
		`{` + ts + `,"type":"event_msg","payload":{"type":"task_started","turn_id":"g1"}}` + "\n",
		`{` + ts + `,"type":"turn_context","payload":{"turn_id":"g1","cwd":"/w","model":"gpt-b"}}` + "\n",
		`{` + ts + `,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<codex_internal_context source=\"goal\">continue</codex_internal_context>"}]}}` + "\n",
		`{` + ts + `,"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"working on the goal"}]}}` + "\n",
	}
	tr := NewTracker("", 0, 0, func(client, _ string) bool { return strings.HasPrefix(client, "cxp-") })
	if got := keys(readAll(t, writeLines(t, head...), tr)); got != "S:working on the goal" {
		t.Fatalf("continuation status held: %q", got)
	}
	steer := append(head,
		`{`+ts+`,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"from teams"}]}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","client_id":"cxp-1"}}}`+"\n",
		`{`+ts+`,"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"answer for teams"}]}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"task_complete","turn_id":"g1","last_agent_message":"answer for teams"}}`+"\n",
	)
	tr = NewTracker("", 0, 0, func(client, _ string) bool { return strings.HasPrefix(client, "cxp-") })
	if got := keys(readAll(t, writeLines(t, steer...), tr)); got != "S:working on the goal" {
		t.Fatalf("steered helper prompt mirrored: %q", got)
	}
	// A local prompt steered in is mirrored once its user item says so.
	local := append(head,
		`{`+ts+`,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"from the tui"}]}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"item_completed","item":{"type":"UserMessage","client_id":"tui"}}}`+"\n",
		`{`+ts+`,"type":"event_msg","payload":{"type":"task_complete","turn_id":"g1","last_agent_message":"local answer"}}`+"\n",
	)
	tr = NewTracker("", 0, 0, func(client, _ string) bool { return strings.HasPrefix(client, "cxp-") })
	if got := keys(readAll(t, writeLines(t, local...), tr)); got != "S:working on the goal | U:from the tui | F:local answer" {
		t.Fatalf("steered local prompt: %q", got)
	}
}
