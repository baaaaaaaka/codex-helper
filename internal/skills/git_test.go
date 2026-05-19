package skills

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSafeGitEnvStripsAmbientGitRepositoryVariables(t *testing.T) {
	t.Setenv("GIT_DIR", "/tmp/wrong")
	t.Setenv("GIT_WORK_TREE", "/tmp/work")
	t.Setenv("GIT_INDEX_FILE", "/tmp/index")
	t.Setenv("GIT_OBJECT_DIRECTORY", "/tmp/objects")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	env := safeGitEnv([]string{"EXTRA_GIT_TEST=1"})
	for _, forbidden := range []string{"GIT_DIR=", "GIT_WORK_TREE=", "GIT_INDEX_FILE=", "GIT_OBJECT_DIRECTORY=", "GIT_CONFIG_NOSYSTEM="} {
		for _, kv := range env {
			if strings.HasPrefix(kv, forbidden) {
				t.Fatalf("safeGitEnv kept %s in %v", forbidden, env)
			}
		}
	}
	for _, want := range []string{"GIT_TERMINAL_PROMPT=0", "GCM_INTERACTIVE=never", "EXTRA_GIT_TEST=1"} {
		if !envContains(env, want) {
			t.Fatalf("safeGitEnv missing %s in %v", want, env)
		}
	}
}

func TestExecGitRunnerHonorsSystemGitConfig(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	systemConfig := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(systemConfig, []byte("[cxp-skills]\n\tmarker = from-system-config\n"), 0o600); err != nil {
		t.Fatalf("write system git config: %v", err)
	}
	t.Setenv("GIT_CONFIG_SYSTEM", systemConfig)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	out, err := (ExecGitRunner{}).Run(context.Background(), "", nil, "config", "--get", "cxp-skills.marker")
	if err != nil {
		t.Fatalf("git config --get: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "from-system-config" {
		t.Fatalf("system git config marker = %q", got)
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

func TestRedactURLSecretsKeepsSSHUserAndRedactsPassword(t *testing.T) {
	got := RedactURLSecrets("ssh://git@gitlab.example.com/acme/repo.git ssh://deploy:secret@gitlab.example.com/acme/repo.git")
	if !strings.Contains(got, "ssh://git@gitlab.example.com/acme/repo.git") {
		t.Fatalf("redaction should keep SSH username:\n%s", got)
	}
	if strings.Contains(got, "deploy:secret") {
		t.Fatalf("redaction leaked SSH password:\n%s", got)
	}
	if !strings.Contains(got, "ssh://deploy:<redacted>@gitlab.example.com/acme/repo.git") {
		t.Fatalf("redaction missing SSH password placeholder:\n%s", got)
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

func TestAuthHintRedactsGenericHTTPRemoteSecrets(t *testing.T) {
	hint := authHint(Source{
		RemoteURL: "https://token:secret@git.example.com/acme/private.git",
	})
	if strings.Contains(hint, "token:secret") {
		t.Fatalf("auth hint leaked credential:\n%s", hint)
	}
	if !strings.Contains(hint, "https://<redacted>@git.example.com/acme/private.git") {
		t.Fatalf("auth hint missing redacted remote:\n%s", hint)
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
