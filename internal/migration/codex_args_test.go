package migration

import (
	"reflect"
	"testing"
)

func TestRemoveLegacyCodexExecutionOverrides(t *testing.T) {
	input := []string{
		"--yolo",
		"--dangerously-bypass-approvals-and-sandbox",
		"--sandbox", "danger-full-access",
		"--ask-for-approval=never",
		"-c", `sandbox_mode="danger-full-access"`,
		"-c", `model="gpt-5"`,
		"--search",
	}
	want := []string{"-c", `model="gpt-5"`, "--search"}
	if got := RemoveLegacyCodexExecutionOverrides(input); !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}
