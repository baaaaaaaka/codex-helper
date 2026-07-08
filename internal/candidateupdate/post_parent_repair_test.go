package candidateupdate

import (
	"bytes"
	"strings"
	"testing"
)

func TestParsePostParentRepairRequest(t *testing.T) {
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	request, err := parsePostParentRepairRequest([]string{
		"--parent-pid=123",
		"--target-path=/tmp/cxp",
		"--ready-path=/tmp/ready",
		"--expected-current-sha256=" + hashA,
		"--source-sha256=" + hashB,
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.ParentPID != 123 || request.TargetPath != "/tmp/cxp" || request.ReadyPath != "/tmp/ready" || request.ExpectedCurrentSHA256 != hashA || request.SourceSHA256 != hashB {
		t.Fatalf("request = %#v", request)
	}
}

func TestParsePostParentRepairRequestRejectsIncompleteOrDuplicateInput(t *testing.T) {
	hash := strings.Repeat("a", 64)
	for _, args := range [][]string{
		{"--parent-pid=0", "--target-path=/tmp/cxp", "--ready-path=/tmp/ready", "--expected-current-sha256=" + hash, "--source-sha256=" + hash},
		{"--parent-pid=1", "--parent-pid=2", "--target-path=/tmp/cxp", "--ready-path=/tmp/ready", "--expected-current-sha256=" + hash, "--source-sha256=" + hash},
		{"--parent-pid=1", "--target-path=/tmp/cxp", "--expected-current-sha256=" + hash, "--source-sha256=" + hash},
		{"--parent-pid=1", "--target-path=/tmp/cxp", "--ready-path=/tmp/ready", "--expected-current-sha256=short", "--source-sha256=" + hash},
		{"--parent-pid=1", "--target-path=/tmp/cxp", "--ready-path=/tmp/ready", "--expected-current-sha256=" + hash, "--source-sha256=" + hash, "--unknown=value"},
	} {
		if _, err := parsePostParentRepairRequest(args); err == nil {
			t.Fatalf("parsePostParentRepairRequest(%q) unexpectedly succeeded", args)
		}
	}
}

func TestHandlePostParentRepairCommandRejectsUnsupportedArguments(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode, handled := HandlePostParentRepairCommand([]string{"cxp", PostParentRepairCommand, "--unsupported"}, &stdout, &stderr)
	if !handled || exitCode != 2 {
		t.Fatalf("handled=%v exit=%d, want true/2", handled, exitCode)
	}
	if !strings.Contains(stderr.String(), "unsupported argument") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
