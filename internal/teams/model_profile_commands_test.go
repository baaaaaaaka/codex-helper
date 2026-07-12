package teams

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

type backupWarningModelProfileManager struct{ *fakeModelProfileManager }

func (m *backupWarningModelProfileManager) ModelProfileRuntimeWarning(context.Context, modelprofile.Snapshot) (string, bool, error) {
	return "⚠️ backup JSON remains active", true, nil
}

func TestModelNaturalLanguageListRequest(t *testing.T) {
	for _, value := range []string{
		"里面应该列出来所有可用的 gpt 模型，另外列出来 default 具体是什么模型",
		"有哪些可用模型",
		"what models are available if I am logged in",
		"please list models and explain default",
	} {
		if !modelNaturalLanguageListRequest(value) {
			t.Errorf("modelNaturalLanguageListRequest(%q) = false, want true", value)
		}
	}
	for _, value := range []string{
		"list",
		"status",
		"switch nemotron",
		"请帮我切换到 nemotron",
		"delete old-profile",
	} {
		if modelNaturalLanguageListRequest(value) {
			t.Errorf("modelNaturalLanguageListRequest(%q) = true, want false", value)
		}
	}
}

func TestPromptAckAlwaysIncludesActiveBackupJSONWarning(t *testing.T) {
	bridge := &Bridge{modelProfileManager: &backupWarningModelProfileManager{fakeModelProfileManager: &fakeModelProfileManager{}}}
	for turn := 1; turn <= 2; turn++ {
		got := bridge.appendModelProfileRuntimeWarning(context.Background(), "⏳ accepted", modelprofile.Snapshot{Name: "work-glm", Provider: "glm"})
		if !strings.Contains(got, "backup JSON remains active") {
			t.Fatalf("turn %d omitted persistent warning: %q", turn, got)
		}
	}
}

func TestModelNaturalLanguageListRequestRoutesInControlAndWorkChats(t *testing.T) {
	bridge := &Bridge{modelProfileManager: &fakeModelProfileManager{}}
	request := "里面应该列出来所有可用的 gpt 模型，另外列出来 default 具体是什么模型"
	control, err := bridge.handleModelControlCommand(context.Background(), ChatMessage{}, request)
	if err != nil || control != "profiles" {
		t.Fatalf("control result = %q, err=%v", control, err)
	}
	work, err := bridge.handleModelWorkCommand(context.Background(), &Session{ID: "s1"}, request)
	if err != nil || work != "profiles" {
		t.Fatalf("work result = %q, err=%v", work, err)
	}
}
