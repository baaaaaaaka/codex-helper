package proc

import (
	"os"
	"os/exec"
	"testing"
)

func TestCheckAlive(t *testing.T) {
	for _, pid := range []int{0, -1} {
		if alive, err := CheckAlive(pid); alive || err != nil {
			t.Fatalf("CheckAlive(%d) = %v, %v", pid, alive, err)
		}
	}
	if alive, err := CheckAlive(os.Getpid()); !alive || err != nil {
		t.Fatalf("current process = %v, %v", alive, err)
	}
	// Reuse the test binary for a portable, already-reaped process fixture.
	cmd := exec.Command(os.Args[0], "-test.run=^TestCheckAliveChild$")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	if alive, err := CheckAlive(cmd.Process.Pid); alive || err != nil {
		t.Fatalf("reaped process = %v, %v", alive, err)
	}
}

func TestCheckAliveChild(t *testing.T) {}
