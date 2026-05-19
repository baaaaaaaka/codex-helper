package skills

import (
	"strings"
	"testing"
)

func TestSafeGitEnvStripsAmbientGitRepositoryVariables(t *testing.T) {
	t.Setenv("GIT_DIR", "/tmp/wrong")
	t.Setenv("GIT_WORK_TREE", "/tmp/work")
	t.Setenv("GIT_INDEX_FILE", "/tmp/index")
	t.Setenv("GIT_OBJECT_DIRECTORY", "/tmp/objects")

	env := safeGitEnv([]string{"EXTRA_GIT_TEST=1"})
	for _, forbidden := range []string{"GIT_DIR=", "GIT_WORK_TREE=", "GIT_INDEX_FILE=", "GIT_OBJECT_DIRECTORY="} {
		for _, kv := range env {
			if strings.HasPrefix(kv, forbidden) {
				t.Fatalf("safeGitEnv kept %s in %v", forbidden, env)
			}
		}
	}
	for _, want := range []string{"GIT_TERMINAL_PROMPT=0", "GCM_INTERACTIVE=never", "GIT_CONFIG_NOSYSTEM=1", "EXTRA_GIT_TEST=1"} {
		if !envContains(env, want) {
			t.Fatalf("safeGitEnv missing %s in %v", want, env)
		}
	}
}

func TestGitErrorScrubsURLSecrets(t *testing.T) {
	err := (&GitError{
		Args:   []string{"fetch", "https://token:secret@github.com/acme/private.git"},
		Output: "fatal: Authentication failed for 'https://token:secret@github.com/acme/private.git/'",
		Err:    errGitExitForTest{},
	}).Error()
	if strings.Contains(err, "token:secret") {
		t.Fatalf("GitError leaked token: %s", err)
	}
	if !strings.Contains(err, "https://<redacted>@github.com/acme/private.git") {
		t.Fatalf("GitError did not include redacted URL: %s", err)
	}
}

func TestAuthHintGitLabSSHIncludesPortAndMissingUserNote(t *testing.T) {
	hint := authHint(Source{
		Provider:  "gitlab",
		RemoteURL: "ssh://gitlab-master.nvidia.com:12051/jawei/fgx_tin_skill.git",
	})
	if !strings.Contains(hint, "`ssh -T -p 12051 git@gitlab-master.nvidia.com`") {
		t.Fatalf("auth hint missing port-aware SSH check:\n%s", hint)
	}
	if !strings.Contains(hint, "SSH URL has no user") {
		t.Fatalf("auth hint missing no-user diagnosis:\n%s", hint)
	}
}

func TestAuthHintSSHKeepsExplicitUser(t *testing.T) {
	hint := authHint(Source{
		Provider:  "gitlab",
		RemoteURL: "ssh://deploy@gitlab.example.com:2222/acme/skills.git",
	})
	if !strings.Contains(hint, "`ssh -T -p 2222 deploy@gitlab.example.com`") {
		t.Fatalf("auth hint missing explicit SSH user:\n%s", hint)
	}
	if strings.Contains(hint, "SSH URL has no user") {
		t.Fatalf("auth hint incorrectly reported missing user:\n%s", hint)
	}
}

type errGitExitForTest struct{}

func (errGitExitForTest) Error() string { return "exit status 128" }

func envContains(env []string, want string) bool {
	for _, kv := range env {
		if kv == want {
			return true
		}
	}
	return false
}
