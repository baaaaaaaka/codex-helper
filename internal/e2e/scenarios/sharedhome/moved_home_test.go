package sharedhome

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// The owner's machine move (2026-11-06): stop the helper on the old
// machine, copy every file of the home except ~/.ssh to the new machine,
// start the helper there. The new machine has another host name; its
// helper finds the old machine's released lease record in the copied
// state, takes the store over by a copy (core.g2), announces the move once
// and resumes exactly where the old one stopped: the prompt posted in the
// gap is answered once, a TUI turn written on the new machine is mirrored
// once, and nothing earlier is posted again.
func TestMovedHomeResumesOnTheNewMachine(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "MOVED-HOME", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1", "R-9", "R-10"},
		Title:      "machine move: stop, copy the home without ~/.ssh, start on a new host; it resumes at once, announces the move once, answers the gap prompt once and mirrors the TUI once",
		Budget:     150 * time.Second,
		Seam:       harness.Seam{Hostname: "host-a"},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "moved")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Before the move "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("the old machine answers", answered, "answered", fmt.Sprint(answered))

	tui := harness.NewRollout(t, s.Env, s.Env.WorkDir(t, "moved-tui"))
	p1, a1 := s.Token("tuiprompt1"), s.Token("tuianswer1")
	tui.AppendTurn(t, "Local question "+p1, "", "Local answer "+a1)
	var tuiChat string
	s.WaitFor("the old machine mirrors the TUI session", 30*time.Second, func() (bool, string) {
		if cs := steps.ChatsWithSUTText(s, a1); len(cs) > 0 {
			tuiChat = cs[0].ID
			return true, ""
		}
		return false, "not mirrored yet"
	})
	s.Settle(2*time.Second, 10*time.Second)

	// The move: a clean stop, then the copy. The new machine's home is at
	// the same path (same user name); the old machine keeps its files.
	s.StopSUT()
	if err := os.MkdirAll(filepath.Join(s.Env.Home, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.Env.Home, ".ssh", "id_test"), []byte("not copied\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := s.Env.Home + ".old-machine"
	if err := os.Rename(s.Env.Home, old); err != nil {
		t.Fatal(err)
	}
	if err := copyHome(old, s.Env.Home, ".ssh"); err != nil {
		t.Fatalf("copy the home: %v", err)
	}
	gap := s.Token("gap")
	steps.MustPost(s, work.ID, "During the move "+gap)
	p2, a2 := s.Token("tuiprompt2"), s.Token("tuianswer2")
	tui.AppendTurn(t, "On the new machine "+p2, "", "New machine answer "+a2)

	from := s.Graph.LastSeq()
	b := s.NewPeer("B", harness.PeerOptions{Seam: harness.Seam{Hostname: "host-b"}})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	took, ok := waitServing(s, b, 30*time.Second)
	s.Check("the new machine serves at once, with no manual step", ok && took <= 15*time.Second,
		"serving within 15s", fmt.Sprintf("serving=%v after %s; output %s", ok, took, clipTail(b.Output(), 600)))
	s.Check("the new machine takes the store over by a copy", strings.Contains(b.Output(), "copying the store"),
		"a copy into a new generation", clipTail(b.Output(), 600))

	byB := answeredBy(s, work.ID, gap, "-B-", steps.AnswerTimeout)
	s.Check("the prompt posted during the move is answered by the new machine", byB, "answered by B", fmt.Sprint(byB), s.Transcript(work.ID, 12)...)
	s.WaitFor("the TUI turn written on the new machine is mirrored", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(tuiChat, a2)) > 0, "not mirrored yet"
	})
	s.Settle(3*time.Second, 15*time.Second)

	exactlyOnce(s, work.ID, gap)
	n, turns := len(steps.AnswerPosts(s, work.ID, first)), len(s.Codex.CallsWithToken(first))
	s.Check("R-1 the prompt answered before the move is not answered again", n == 1 && turns == 1,
		"1 answer, 1 turn", fmt.Sprintf("posts=%d turns=%d", n, turns), s.Transcript(work.ID, 12)...)
	for _, c := range []struct{ name, prompt, answer string }{
		{"the TUI turn mirrored before the move", "Local question " + p1, a1},
		{"the TUI turn written on the new machine", "On the new machine " + p2, a2},
	} {
		np, na := steps.UserRecords(s, tuiChat, c.prompt), len(s.SUTMessagesContaining(tuiChat, c.answer))
		s.Check("R-10 "+c.name+" is in the chat exactly once", np == 1 && na == 1,
			"prompt 1, answer 1", fmt.Sprintf("prompt %d, answer %d", np, na), s.Transcript(tuiChat, 20)...)
	}
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming both machines", len(notices) == 1 &&
		strings.Contains(notices[0], "host-b") && strings.Contains(notices[0], "host-a") && strings.Contains(notices[0], "stopped"),
		"1 notice: runs on host-b, took over from host-a, whose helper stopped", fmt.Sprintf("%d: %q", len(notices), notices))
	writes := writesBy(s, from, b.Instance())
	s.Check("no burst: the new machine's writes are this scenario's few posts and edits", len(writes) <= 30,
		"at most 30 writes", fmt.Sprintf("%d: %q", len(writes), writes))
	if _, err := os.Stat(filepath.Join(s.Env.Home, ".ssh")); !os.IsNotExist(err) {
		t.Fatalf("the copy carried ~/.ssh: %v", err)
	}
}

// copyHome copies the tree src to dst (which must not exist) like
// `rsync -a --exclude=<skip>`: directories, regular files and symlinks with
// their modes and modification times. Anything else (sockets, fifos) is
// left behind, as a copy to another machine would.
func copyHome(src, dst, skip string) error {
	type dir struct {
		path string
		mod  time.Time
	}
	var dirs []dir
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == skip {
			return filepath.SkipDir
		}
		target := filepath.Join(dst, rel)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			if err := os.MkdirAll(target, info.Mode().Perm()); err != nil {
				return err
			}
			dirs = append(dirs, dir{target, info.ModTime()})
		case info.Mode()&fs.ModeSymlink != 0:
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(link, target)
		case info.Mode().IsRegular():
			if err := copyFile(path, target, info.Mode().Perm()); err != nil {
				return err
			}
			return os.Chtimes(target, info.ModTime(), info.ModTime())
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chtimes(dirs[i].path, dirs[i].mod, dirs[i].mod); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string, perm fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
