package lease

import (
	"os"
	"os/exec"
	"testing"
)

// HeadGone knows only this machine's processes: an exited pid of this
// host and boot is gone; this process, or another host's, is not.
func TestHeadGone(t *testing.T) {
	self := Local("box", "boot-1")
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skipf("cannot run a child process: %v", err)
	}
	head := Record{Gen: 2, State: Active, Host: "box", Boot: "boot-1", PIDNS: self.PIDNS, PID: cmd.ProcessState.Pid()}
	in := Info{Exists: true, Head: head, HeadGen: 2, HeadOK: true}
	if !in.HeadGone("box", "boot-1") {
		t.Fatal("an exited pid of this machine is not gone")
	}
	in.Head.PID, in.Head.Start = os.Getpid(), self.Start
	if in.HeadGone("box", "boot-1") {
		t.Fatal("this process is gone")
	}
	in.Head.PID = cmd.ProcessState.Pid()
	if in.HeadGone("other", "boot-2") {
		t.Fatal("another machine's pid judged here")
	}
	in.HeadOK = false
	if in.HeadGone("box", "boot-1") {
		t.Fatal("an unreadable head is gone")
	}
}
