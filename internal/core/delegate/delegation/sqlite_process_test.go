package delegation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

// The child is this inert Go test binary. It opens only its synthetic SQLite
// fixture; it cannot launch or discover a helper, Teams service or proxy.
func TestWorkerSQLiteReservationAcrossProcesses(t *testing.T) {
	const env = "CXP_TEST_SQLITE_RESERVATION_CHILD"
	if path := os.Getenv(env); path != "" {
		s, err := OpenWorkerDB(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err = fmt.Fprintln(os.Stdout, "ready"); err != nil {
			t.Fatal(err)
		}
		var barrier [1]byte
		if _, err = io.ReadFull(os.Stdin, barrier[:]); err != nil {
			t.Fatal(err)
		}
		_, won, err := s.ReserveOutbox(context.Background(), Record{RecordID: "once", DelegationID: "task"}, "chat", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(os.Stdout, "reserved=%t\n", won)
		return
	}
	path := filepath.Join(t.TempDir(), "worker.sqlite")
	s, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	executable, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	type child struct {
		cmd    *exec.Cmd
		in     io.WriteCloser
		out    io.ReadCloser
		stderr bytes.Buffer
	}
	children := make([]*child, 4)
	for i := range children {
		c := &child{cmd: exec.CommandContext(ctx, executable, "-test.run=^TestWorkerSQLiteReservationAcrossProcesses$")}
		c.cmd.Env = append(os.Environ(), env+"="+path)
		c.cmd.Stderr = &c.stderr
		c.in, err = c.cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		c.out, err = c.cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err = c.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		children[i] = c
	}
	// All children have separate open pools before any may reserve.
	for _, c := range children {
		var ready [6]byte
		if _, err = io.ReadFull(c.out, ready[:]); err != nil || string(ready[:]) != "ready\n" {
			t.Fatalf("child ready=%q err=%v stderr=%s", ready, err, c.stderr.String())
		}
	}
	for _, c := range children {
		if _, err = c.in.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		c.in.Close()
	}
	winners := 0
	for _, c := range children {
		out, err := io.ReadAll(c.out)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.cmd.Wait(); err != nil {
			t.Fatalf("child: %v %s", err, c.stderr.String())
		}
		if strings.Contains(string(out), "reserved=true") {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("reservation winners=%d, want 1", winners)
	}
	s, err = OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	out, ok, err := s.Outbox(ctx, "once")
	if err != nil || !ok || out.Attempts != 1 || out.Status != OutboxPending {
		t.Fatalf("witness=%+v ok=%v err=%v", out, ok, err)
	}
}
