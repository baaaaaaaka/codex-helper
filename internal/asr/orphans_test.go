package asr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fakeHelper is a helper process that transcribes one voice file, prints
// the llama-server PID, and then exits without Close (or keeps running with
// ASR_HELPER_HOLD), like a helper that is killed or crashes.
func fakeHelper() int {
	if os.Getenv("ASR_HELPER_NO_PDEATHSIG") != "" {
		deathSignal = false
	}
	var mf Manifest
	if err := json.Unmarshal([]byte(os.Getenv("ASR_HELPER_MANIFEST")), &mf); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	m := NewManaged(ManagedConfig{CacheDir: os.Getenv("ASR_HELPER_CACHE"), Manifest: &mf, LoadTimeout: 30 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := m.Transcribe(ctx, Request{Path: os.Getenv("ASR_HELPER_VOICE")}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(m.srv.proc.cmd.Process.Pid)
	if os.Getenv("ASR_HELPER_HOLD") != "" {
		select {}
	}
	return 0
}

type helperProc struct {
	cmd    *exec.Cmd
	server int
	exited chan struct{}
}

func startFakeHelper(t *testing.T, f *fixture, hold, pdeathsig bool) *helperProc {
	t.Helper()
	bin := filepath.Join(f.dir, exeName("fake-helper"))
	if _, err := os.Stat(bin); err != nil {
		if err := os.WriteFile(bin, self(t), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mf, _ := json.Marshal(f.manifest)
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "ASR_HELPER_CACHE="+f.cache, "ASR_HELPER_MANIFEST="+string(mf),
		"ASR_HELPER_VOICE="+voiceFile(t, f.dir, "orphan.ogg", "orphan"))
	if hold {
		cmd.Env = append(cmd.Env, "ASR_HELPER_HOLD=1")
	}
	if !pdeathsig {
		cmd.Env = append(cmd.Env, "ASR_HELPER_NO_PDEATHSIG=1")
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	h := &helperProc{cmd: cmd, exited: make(chan struct{})}
	line, _ := bufio.NewReader(out).ReadString('\n')
	go func() { cmd.Wait(); close(h.exited) }()
	if h.server, err = strconv.Atoi(strings.TrimSpace(line)); err != nil {
		cmd.Process.Kill()
		<-h.exited
		t.Fatalf("fake helper: %q %v\n%s", line, err, stderr.String())
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		<-h.exited
		if id, ok := processID(h.server); ok {
			killIfMatches(h.server, id)
		}
	})
	return h
}

func (h *helperProc) kill(t *testing.T) {
	t.Helper()
	h.cmd.Process.Kill()
	<-h.exited
}

func alive(pid int) bool {
	_, ok := processID(pid)
	return ok
}

func waitGone(t *testing.T, pid int, what string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); alive(pid); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s (pid %d) is still running", what, pid)
		}
	}
}

func runRecords(t *testing.T, root string) []serverRecord {
	t.Helper()
	db, err := openServerRecords(root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.page(hostName(), 0)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// A helper that dies without Close must not leave llama-server running.
func TestOrphanedServer(t *testing.T) {
	if _, ok := processID(os.Getpid()); !ok {
		t.Skip("process identity is not implemented on " + runtime.GOOS)
	}

	t.Run("server of a dead helper is stopped by the next helper", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("the kill-on-close job object stops the server with the helper")
		}
		f := newFixture(t, nil)
		h := startFakeHelper(t, f, false, false)
		<-h.exited
		if !alive(h.server) {
			t.Fatal("precondition: without Pdeathsig the server must outlive its helper")
		}
		if n := len(runRecords(t, f.cache)); n != 1 {
			t.Fatalf("%d server records, want 1", n)
		}
		f.managed(t)
		waitGone(t, h.server, "orphaned llama-server")
		if n := len(runRecords(t, f.cache)); n != 0 {
			t.Fatalf("%d server records left", n)
		}
	})

	t.Run("server of a running helper is left alone", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("the kill-on-close job object stops the server with the helper")
		}
		f := newFixture(t, nil)
		h := startFakeHelper(t, f, true, false)
		m := f.managed(t)
		if _, err := transcribe(t, m, voiceFile(t, f.dir, "mine.ogg", "mine"), nil, time.Minute); err != nil {
			t.Fatal(err)
		}
		if !alive(h.server) {
			t.Fatal("another running helper's server was killed")
		}
		if n := len(runRecords(t, f.cache)); n != 2 {
			t.Fatalf("%d server records, want 2", n)
		}
		h.kill(t)
		m.Close() // stops its own server and removes its record
		f.managed(t)
		waitGone(t, h.server, "server of the killed helper")
		if n := len(runRecords(t, f.cache)); n != 0 {
			t.Fatalf("%d server records left", n)
		}
	})

	t.Run("recycled PID and other hosts are never killed", func(t *testing.T) {
		f := newFixture(t, nil)
		t.Setenv("ASR_FAKE_CMD", "sleep")
		bystander := exec.Command(fakeCmd(t))
		if err := bystander.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { bystander.Process.Kill(); bystander.Wait() })
		pid := bystander.Process.Pid
		realID, ok := processID(pid)
		if !ok {
			t.Fatal("cannot identify the bystander")
		}
		write := func(name string, rec serverRecord) string {
			p := filepath.Join(f.cache, "run", name)
			os.MkdirAll(filepath.Dir(p), 0o700)
			data, _ := json.Marshal(rec)
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
			return p
		}
		const deadOwner = 1 << 30 // no such PID
		reused := write("reused.json", serverRecord{Host: hostName(), PID: pid, ID: "12345 /old/llama-server", OwnerPID: deadOwner, OwnerID: "x"})
		other := write("other.json", serverRecord{Host: "some-other-host", PID: pid, ID: realID, OwnerPID: deadOwner, OwnerID: "x"})
		f.managed(t)
		time.Sleep(200 * time.Millisecond)
		if !alive(pid) {
			t.Fatal("a process that reused a recorded PID was killed")
		}
		if records := runRecords(t, f.cache); len(records) != 0 {
			t.Fatal("stale native record was not removed")
		}
		if _, err := os.Stat(reused + ".migrated"); err != nil {
			t.Fatal("old record not archived", err)
		}
		db, err := openServerRecords(f.cache)
		if err != nil {
			t.Fatal(err)
		}
		records, err := db.page("some-other-host", 0)
		db.Close()
		if err != nil || len(records) != 1 {
			t.Fatal("another host's native record was removed")
		}
		if _, err := os.Stat(other + ".migrated"); err != nil {
			t.Fatal("other-host migration archive", err)
		}
	})

	t.Run("Linux kills the server together with the helper", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("Pdeathsig is Linux-only")
		}
		f := newFixture(t, nil)
		h := startFakeHelper(t, f, true, true)
		h.kill(t)
		waitGone(t, h.server, "llama-server after its helper was killed")
	})
}
