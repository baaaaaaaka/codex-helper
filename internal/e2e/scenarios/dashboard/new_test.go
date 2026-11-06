package dashboard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// NEW-MKDIR: v0.1.20's directory commands in the control chat.
//
//   - `mkdir ~/dir` creates the directory (0700) and suggests `new`;
//   - a path sent as text gets the path hint (`new` or `mkdir`);
//   - `new ~/missing` creates the missing directory (0700) and its chat;
//   - bare `new` without an opened workspace gets the usage, and after
//     `p` and a workspace number it creates a chat in that workspace;
//   - `new <dir> --model <m> -- <title>` pins the model and titles the
//     chat; an unknown model is refused.
func TestNewAndMkdir(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "NEW-MKDIR", harness.Options{
		Guarantees: []string{"F-CTL-4", "F-CTL-7"},
		Title:      "mkdir, the path hint, new with ~ and a missing directory, bare new in the opened workspace, new --model",
		Budget:     3 * time.Minute,
		NoStart:    true,
	})
	defer s.Finish()
	ws := s.Env.WorkDir(t, "opened")
	localSession(s, ws, base, "Old work in the opened workspace", "Old work done")

	s.StartSUT()
	control := steps.ReadyControlChat(s)
	mode := func(dir string) string {
		st, err := os.Stat(dir)
		if err != nil {
			return err.Error()
		}
		return fmt.Sprintf("%v %o", st.IsDir(), st.Mode().Perm())
	}

	made := filepath.Join(s.Env.Home, "made-by-mkdir")
	command(s, control.ID, "mkdir ~/made-by-mkdir", "Directory is ready: "+made)
	s.Check("`mkdir` creates the directory 0700", mode(made) == "true 700", "true 700", mode(made))
	command(s, control.ID, "./some/project", "📁 Detected path: ./some/project Choose one: new ./some/project - create a 💬 Work chat there mkdir ./some/project - only create the directory")
	command(s, control.ID, "new", "usage: new <directory>; after opening a workspace from projects, you can also send new")

	fresh := filepath.Join(s.Env.Home, "fresh-proj")
	before := chatsNow(s)
	command(s, control.ID, "new ~/fresh-proj", "✅ Work chat created: s001")
	chat1 := newChat(s, before, "the chat in the missing directory")
	s.Check("`new` creates the missing directory 0700", mode(fresh) == "true 700", "true 700", mode(fresh))
	_, ok := s.WaitSUTMessage(chat1.ID, "Working directory: "+fresh, steps.CommandTimeout)
	s.Check("its chat works in that directory", ok, fresh, strings.Join(s.Transcript(chat1.ID, 3), " | "))

	command(s, control.ID, "p", "📁 Workspaces Reply with a number to open a workspace.")
	command(s, control.ID, "1", "Reply with a number to continue a session in Teams.")
	before = chatsNow(s)
	command(s, control.ID, "new", "✅ Work chat created: s002")
	chat2 := newChat(s, before, "the chat in the opened workspace")
	_, ok = s.WaitSUTMessage(chat2.ID, "Working directory: "+ws, steps.CommandTimeout)
	s.Check("bare `new` uses the opened workspace", ok, ws, strings.Join(s.Transcript(chat2.ID, 3), " | "))

	pinned := s.Env.WorkDir(t, "pinned")
	command(s, control.ID, "new "+pinned+" --model no-such-model", "unknown model")
	before = chatsNow(s)
	command(s, control.ID, "new "+pinned+" --model gpt-5-codex -- Pinned chat", "✅ Work chat created: s003")
	chat3 := newChat(s, before, "the pinned chat")
	s.Check("`-- <title>` titles the chat", strings.Contains(chat3.Topic, "Pinned chat"), "Pinned chat", chat3.Topic)
	command(s, control.ID, "d s003", "Model profile: gpt-5-codex")
}
