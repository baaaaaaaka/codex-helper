//go:build linux

package teams

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxHistoryWatchEventsDetectAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write history fixture: %v", err)
	}
	source := newHistoryWatchEventSource()
	defer source.Close()
	if _, uncertain, err := source.Update([]string{path}, true); err != nil {
		t.Fatalf("initial history watcher update: %v", err)
	} else if !uncertain {
		t.Fatal("initial watch registration was reported as certain")
	}
	if err := os.WriteFile(path, []byte("{}\n{}\n"), 0o600); err != nil {
		t.Fatalf("append history fixture: %v", err)
	}
	dirty, uncertain, err := source.Update([]string{path}, false)
	if err != nil {
		t.Fatalf("drain history watcher: %v", err)
	}
	if uncertain {
		t.Fatal("ordinary append made the watcher uncertain")
	}
	if len(dirty) != 1 || dirty[0] != path {
		t.Fatalf("dirty paths = %#v, want [%q]", dirty, path)
	}
}

func TestLinuxHistoryWatchEventsFeedBridgeDirtyPaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write history fixture: %v", err)
	}
	bridge := &Bridge{historyWatchEvents: newHistoryWatchEventSource()}
	t.Cleanup(func() { _ = bridge.closeHistoryWatchEvents() })

	if dirty, uncertain := bridge.historyWatchDirtyPaths([]string{path}, []string{path}, true); !uncertain || len(dirty) != 0 {
		t.Fatalf("initial bridge watcher dirty=%#v uncertain=%t, want uncertain registration without dirty paths", dirty, uncertain)
	}
	if dirty, uncertain := bridge.historyWatchDirtyPaths(nil, nil, false); uncertain || len(dirty) != 0 {
		t.Fatalf("settled bridge watcher dirty=%#v uncertain=%t, want clean cycle", dirty, uncertain)
	}
	if err := os.WriteFile(path, []byte("{}\n{}\n"), 0o600); err != nil {
		t.Fatalf("append history fixture: %v", err)
	}
	dirty, uncertain := bridge.historyWatchDirtyPaths([]string{path}, []string{path}, false)
	if uncertain {
		t.Fatal("ordinary append made bridge watcher uncertain")
	}
	if len(dirty) != 1 || dirty[0] != path {
		t.Fatalf("bridge dirty paths = %#v, want [%q]", dirty, path)
	}
}

func TestLinuxHistoryWatchEventsReleaseObsoleteDirectories(t *testing.T) {
	root := t.TempDir()
	firstDir := filepath.Join(root, "first")
	secondDir := filepath.Join(root, "second")
	if err := os.MkdirAll(firstDir, 0o700); err != nil {
		t.Fatalf("create first history directory: %v", err)
	}
	if err := os.MkdirAll(secondDir, 0o700); err != nil {
		t.Fatalf("create second history directory: %v", err)
	}
	firstPath := filepath.Join(firstDir, "session.jsonl")
	secondPath := filepath.Join(secondDir, "session.jsonl")
	if err := os.WriteFile(firstPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write first history fixture: %v", err)
	}
	if err := os.WriteFile(secondPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write second history fixture: %v", err)
	}
	source := newHistoryWatchEventSource()
	linuxSource, ok := source.(*linuxHistoryWatchEventSource)
	if !ok {
		t.Skip("inotify unavailable")
	}
	defer source.Close()
	if _, _, err := source.Update([]string{firstPath}, true); err != nil {
		t.Fatalf("watch first history fixture: %v", err)
	}
	if len(linuxSource.watchedDirs) != 1 {
		t.Fatalf("watched directories after first update = %d, want 1", len(linuxSource.watchedDirs))
	}
	if _, _, err := source.Update([]string{secondPath}, true); err != nil {
		t.Fatalf("replace watched history directory: %v", err)
	}
	if len(linuxSource.watchedDirs) != 1 {
		t.Fatalf("watched directories after replacement = %d, want 1", len(linuxSource.watchedDirs))
	}
	if _, ok := linuxSource.watchedDirs[firstDir]; ok {
		t.Fatalf("obsolete directory %q is still watched", firstDir)
	}
	if _, ok := linuxSource.watchedDirs[secondDir]; !ok {
		t.Fatalf("current history directory %q is not watched", secondDir)
	}
}

func TestLinuxHistoryWatchEventsIncrementalUpdateRetainsExistingDirectory(t *testing.T) {
	root := t.TempDir()
	firstDir := filepath.Join(root, "first")
	secondDir := filepath.Join(root, "second")
	if err := os.MkdirAll(firstDir, 0o700); err != nil {
		t.Fatalf("create first history directory: %v", err)
	}
	if err := os.MkdirAll(secondDir, 0o700); err != nil {
		t.Fatalf("create second history directory: %v", err)
	}
	firstPath := filepath.Join(firstDir, "session.jsonl")
	secondPath := filepath.Join(secondDir, "session.jsonl")
	if err := os.WriteFile(firstPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write first history fixture: %v", err)
	}
	if err := os.WriteFile(secondPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write second history fixture: %v", err)
	}
	source := newHistoryWatchEventSource()
	linuxSource, ok := source.(*linuxHistoryWatchEventSource)
	if !ok {
		t.Skip("inotify unavailable")
	}
	defer source.Close()
	if _, _, err := source.Update([]string{firstPath}, true); err != nil {
		t.Fatalf("watch first history fixture: %v", err)
	}
	if _, _, err := source.Update(nil, false); err != nil {
		t.Fatalf("settle first history watcher: %v", err)
	}
	if _, _, err := source.Update([]string{secondPath}, false); err != nil {
		t.Fatalf("add second history directory incrementally: %v", err)
	}
	if len(linuxSource.watchedDirs) != 2 {
		t.Fatalf("watched directories after incremental add = %d, want 2", len(linuxSource.watchedDirs))
	}
	if _, ok := linuxSource.watchedDirs[firstDir]; !ok {
		t.Fatalf("existing directory %q was pruned by incremental update", firstDir)
	}
	if _, ok := linuxSource.watchedDirs[secondDir]; !ok {
		t.Fatalf("incremental directory %q was not watched", secondDir)
	}
}

func TestLinuxHistoryWatchEventsOverflowIsUncertain(t *testing.T) {
	pipeFDs := []int{-1, -1}
	if err := unix.Pipe(pipeFDs); err != nil {
		t.Fatalf("create event pipe: %v", err)
	}
	defer unix.Close(pipeFDs[0])
	defer unix.Close(pipeFDs[1])
	if err := unix.SetNonblock(pipeFDs[0], true); err != nil {
		t.Fatalf("set event pipe nonblocking: %v", err)
	}

	watcher := &linuxHistoryWatchEventSource{
		fd:          pipeFDs[0],
		watchedDirs: make(map[string]int32),
		dirsByWatch: make(map[int32]string),
		pendingDirs: make(map[string]struct{}),
		readBuffer:  make([]byte, 64*1024),
		dirtyPaths:  make(map[string]struct{}),
		initialized: true,
	}
	event := make([]byte, unix.SizeofInotifyEvent)
	wd := int32(-1)
	binary.LittleEndian.PutUint32(event[0:4], uint32(wd))
	binary.LittleEndian.PutUint32(event[4:8], unix.IN_Q_OVERFLOW)
	if _, err := unix.Write(pipeFDs[1], event); err != nil {
		t.Fatalf("write overflow event: %v", err)
	}
	dirty, uncertain, err := watcher.drain()
	if err != nil {
		t.Fatalf("drain overflow event: %v", err)
	}
	if !uncertain {
		t.Fatal("inotify overflow was reported as certain")
	}
	if len(dirty) != 0 {
		t.Fatalf("overflow dirty paths = %#v, want none", dirty)
	}
}

func TestLinuxHistoryWatchEventsReaddsRecreatedDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "sessions")
	path := filepath.Join(dir, "session.jsonl")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create history directory: %v", err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write history fixture: %v", err)
	}
	source := newHistoryWatchEventSource()
	linuxSource, ok := source.(*linuxHistoryWatchEventSource)
	if !ok {
		t.Skip("inotify unavailable")
	}
	defer source.Close()
	if _, _, err := source.Update([]string{path}, true); err != nil {
		t.Fatalf("initial history watcher update: %v", err)
	}
	if _, _, err := source.Update(nil, false); err != nil {
		t.Fatalf("settle initial history watcher: %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove watched directory: %v", err)
	}
	if _, _, err := source.Update(nil, false); err != nil {
		t.Fatalf("drain removed directory event: %v", err)
	}
	if _, stillWatched := linuxSource.watchedDirs[dir]; stillWatched {
		t.Fatalf("removed directory %q remains in watcher bookkeeping", dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("recreate history directory: %v", err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write recreated history fixture: %v", err)
	}
	if _, uncertain, err := source.Update([]string{path}, false); err != nil {
		t.Fatalf("re-add recreated history directory: %v", err)
	} else if !uncertain {
		t.Fatal("recreated directory registration was not marked uncertain")
	}
	if _, _, err := source.Update(nil, false); err != nil {
		t.Fatalf("settle recreated history watcher: %v", err)
	}
	if err := os.WriteFile(path, []byte("{}\n{}\n"), 0o600); err != nil {
		t.Fatalf("append recreated history fixture: %v", err)
	}
	dirty, uncertain, err := source.Update(nil, false)
	if err != nil {
		t.Fatalf("drain recreated history event: %v", err)
	}
	if uncertain {
		t.Fatal("ordinary append after directory recreation made watcher uncertain")
	}
	if len(dirty) != 1 || dirty[0] != path {
		t.Fatalf("dirty paths after directory recreation = %#v, want [%q]", dirty, path)
	}
}
