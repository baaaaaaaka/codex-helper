package fakecodex

import (
	"os/exec"
	"testing"
	"time"
)

func TestZombieCannotOwnAnActiveWriter(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()
	deadline := time.Now().Add(3 * time.Second)
	for pidAlive(cmd.Process.Pid) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pidAlive(cmd.Process.Pid) {
		t.Fatal("an exited, unreaped process is still treated as an active writer")
	}
}
