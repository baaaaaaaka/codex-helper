package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestGoalArtifactsSurviveNativeTurnsAndRestart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "GOAL-ARTIFACTS", harness.Options{
		Guarantees: []string{"F-RND-10", "R-37", "F-WRK-10"},
		Title:      "native goal uploads survive restart, and a scheduled turn after fork uploads only to the child",
		Budget:     100 * time.Second,
	})
	defer s.Finish()

	control := steps.ReadyControlChat(s)
	waitFinished := func(want int) {
		t.Helper()
		s.Require("control receives the expected completion notices", steps.AnswerTimeout, func() (bool, string) {
			got := len(s.SUTMessagesContaining(control.ID, "✅ Codex finished"))
			return got == want, fmt.Sprint(got)
		})
	}
	work, _ := steps.NewWorkChat(s, control, "goal-artifacts")
	seed := s.Token("seed")
	steps.MustPost(s, work.ID, "Prepare a report "+seed)
	if _, ok := steps.WaitAnswered(s, work.ID, seed, steps.AnswerTimeout); !ok {
		t.Fatal("seed request did not answer")
	}
	waitFinished(1)
	first := s.Codex.CallsWithToken(seed)[0]
	const directoryLead = "write them under this local directory:\n"
	_, rest, found := strings.Cut(first.Prompt, directoryLead)
	root, _, _ := strings.Cut(rest, "\n")
	if !found || root == "" || !strings.Contains(first.Prompt, `"version":2`) {
		t.Fatalf("seed prompt did not advertise the conversation directory and v2")
	}
	disk := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
	thread, err := disk.Load(first.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"first report", "second report"}
	writeGoal := func(index int) {
		t.Helper()
		base := fmt.Sprintf("delivery-%d.txt", index)
		if err := os.WriteFile(filepath.Join(root, base), []byte(want[index]), 0o600); err != nil {
			t.Fatal(err)
		}
		answer := fmt.Sprintf("Goal report %d is ready.\n\n```codex-helper-artifacts\n", index) +
			fmt.Sprintf(`{"version":2,"files":[{"path":%q,"name":"report.txt"}]}`, base) + "\n```"
		appendNativeAnswer(t, thread.RolloutPath, fmt.Sprintf("e2e-artifact-goal-%d", index), "goal", "", answer)
	}
	count := func(chat, body string) int {
		n := 0
		for _, file := range s.Graph.OutboundFiles(chat) {
			if string(file.Item.Data) == body && strings.HasPrefix(file.Name, "report") {
				n++
			}
		}
		return n
	}

	writeGoal(0)
	s.Require("the promptless goal delivers the first report", steps.AnswerTimeout, func() (bool, string) {
		return count(work.ID, want[0]) == 1, fmt.Sprint(s.Graph.OutboundFiles(work.ID))
	})
	waitFinished(2)
	s.StopSUT()
	writeGoal(1)
	s.StartSUT()
	s.Require("startup catch-up delivers the second report", steps.AnswerTimeout, func() (bool, string) {
		return count(work.ID, want[1]) == 1, fmt.Sprint(s.Graph.OutboundFiles(work.ID))
	})

	before := chatsNow(s)
	steps.MustPost(s, work.ID, "helper fork")
	fork, created := newForkChat(s, before)
	if !created {
		t.Fatal("fork chat was not created")
	}
	if _, ok := s.WaitSUTMessage(fork.ID, forkMarker, steps.CommandTimeout); !ok {
		t.Fatal("fork history did not finish")
	}
	if _, ok := s.WaitSUTMessage(work.ID, "Fork complete", steps.CommandTimeout); !ok {
		t.Fatal("fork completion was not reported")
	}
	for _, suffix := range []string{"0", "1"} {
		text := "Goal report " + suffix + " is ready."
		s.Check("the fork shows "+text, len(s.SUTMessagesContaining(fork.ID, text)) == 1, "one message", fmt.Sprint(len(s.SUTMessagesContaining(fork.ID, text))))
	}
	s.Check("the original conversation gets each file once", count(work.ID, want[0]) == 1 && count(work.ID, want[1]) == 1, "one of each", fmt.Sprint(s.Graph.OutboundFiles(work.ID)))
	s.Check("fork history uploads no files", len(s.Graph.OutboundFiles(fork.ID)) == 0, "zero files", fmt.Sprint(s.Graph.OutboundFiles(fork.ID)))

	child := s.Token("child")
	steps.MustPost(s, fork.ID, "Prepare a new report "+child)
	if _, ok := steps.WaitAnswered(s, fork.ID, child, steps.AnswerTimeout); !ok {
		t.Fatal("fork request did not answer")
	}
	waitFinished(3)
	childCall := s.Codex.CallsWithToken(child)[0]
	_, childRest, found := strings.Cut(childCall.Prompt, directoryLead)
	childRoot, _, _ := strings.Cut(childRest, "\n")
	if !found || childRoot == "" || childRoot == root {
		t.Fatalf("the child must have its own conversation directory, got %q", childRoot)
	}
	childThread, err := disk.Load(childCall.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	const scheduled = "scheduled report"
	if err := os.WriteFile(filepath.Join(childRoot, "delivery-scheduled.txt"), []byte(scheduled), 0o600); err != nil {
		t.Fatal(err)
	}
	appendNativeAnswer(t, childThread.RolloutPath, "e2e-artifact-scheduled", "automation_cron_scheduled", "Check today's report", "Scheduled report is ready.\n\n```codex-helper-artifacts\n"+
		`{"version":2,"files":[{"path":"delivery-scheduled.txt","name":"report.txt"}]}`+"\n```")
	s.Require("the scheduled turn uploads only to the child", steps.AnswerTimeout, func() (bool, string) {
		return count(fork.ID, scheduled) == 1 && count(work.ID, scheduled) == 0, fmt.Sprint(s.Graph.OutboundFiles(fork.ID))
	})
	waitFinished(4)

	for _, chat := range []fakegraph.ChatInfo{work, fork} {
		n := len(s.SUTMessagesContaining(chat.ID, `"version":2`)) +
			len(s.SUTMessagesContaining(chat.ID, "Some files could not be attached"))
		s.Check("Teams does not show the attachment manifest or failure in "+chat.Topic, n == 0, "no messages", fmt.Sprint(n))
	}
}

func appendNativeAnswer(t *testing.T, file, turn, trigger, prompt, answer string) {
	t.Helper()
	out, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	write := func(kind string, payload map[string]any) {
		if err := enc.Encode(map[string]any{"timestamp": time.Now().UTC(), "type": kind, "payload": payload}); err != nil {
			t.Fatal(err)
		}
	}
	write("event_msg", map[string]any{"type": "task_started", "turn_id": turn,
		"turn_attribution": map[string]string{"turn_id": turn, "turn_trigger": trigger}})
	if prompt != "" {
		write("response_item", map[string]any{"type": "message", "role": "user", "id": turn + "-user",
			"content": []map[string]string{{"type": "input_text", "text": prompt}}})
	}
	write("response_item", map[string]any{"type": "message", "role": "assistant", "phase": "final_answer",
		"content": []map[string]string{{"type": "output_text", "text": answer}}})
	write("event_msg", map[string]any{"type": "task_complete", "turn_id": turn, "last_agent_message": answer})
}
