//go:build linux

package teams

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

type linuxHistoryWatchEventSource struct {
	fd          int
	watchedDirs map[string]int32
	dirsByWatch map[int32]string
	pendingDirs map[string]struct{}
	readBuffer  []byte
	dirtyPaths  map[string]struct{}
	initialized bool
}

func newHistoryWatchEventSource() historyWatchEventSource {
	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return fallbackHistoryWatchEventSource{}
	}
	return &linuxHistoryWatchEventSource{
		fd:          fd,
		watchedDirs: make(map[string]int32),
		dirsByWatch: make(map[int32]string),
		pendingDirs: make(map[string]struct{}),
		readBuffer:  make([]byte, 64*1024),
		dirtyPaths:  make(map[string]struct{}),
	}
}

func (w *linuxHistoryWatchEventSource) Update(paths []string, prune bool) ([]string, bool, error) {
	if w == nil || w.fd < 0 {
		return nil, true, errors.New("history watcher is closed")
	}
	// On startup and reconciliation, the indexed path set is the completeness
	// boundary for this watcher. Do not retain watches for directories that are
	// no longer represented in that complete set: long-lived helpers can
	// otherwise accumulate one inotify watch per historical project/session
	// directory. Normal cycles pass only recent paths with prune=false; existing
	// watches are retained and only newly relevant directories are added. A
	// directory that becomes relevant again is added below and deliberately
	// makes the stream uncertain once, so events that happened while it was
	// unwatched cannot be mistaken for a complete change history.
	wantedDirs := make(map[string]struct{})
	for _, path := range uniqueSortedCleanPaths(paths) {
		wantedDirs[filepath.Dir(path)] = struct{}{}
	}
	if prune {
		for dir := range w.pendingDirs {
			if _, wanted := wantedDirs[dir]; !wanted {
				delete(w.pendingDirs, dir)
			}
		}
		for dir, wd := range w.watchedDirs {
			if _, wanted := wantedDirs[dir]; wanted {
				continue
			}
			if _, err := unix.InotifyRmWatch(w.fd, uint32(wd)); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
				return nil, true, err
			}
			delete(w.watchedDirs, dir)
			delete(w.dirsByWatch, wd)
		}
	} else {
		for dir := range w.pendingDirs {
			wantedDirs[dir] = struct{}{}
		}
	}
	uncertain := !w.initialized
	for dir := range wantedDirs {
		if _, ok := w.watchedDirs[dir]; ok {
			continue
		}
		wd, err := unix.InotifyAddWatch(w.fd, dir, unix.IN_MODIFY|unix.IN_CLOSE_WRITE|unix.IN_MOVED_TO|unix.IN_MOVED_FROM|unix.IN_CREATE|unix.IN_DELETE|unix.IN_ATTRIB|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF)
		if err != nil {
			if os.IsNotExist(err) || errors.Is(err, unix.ENOENT) {
				uncertain = true
				w.pendingDirs[dir] = struct{}{}
				continue
			}
			return nil, true, err
		}
		w.watchedDirs[dir] = int32(wd)
		w.dirsByWatch[int32(wd)] = dir
		delete(w.pendingDirs, dir)
		// Events that happened before this directory was watched cannot be
		// reconstructed. Scan it once before trusting the event stream.
		uncertain = true
	}
	w.initialized = true
	dirty, drainUncertain, err := w.drain()
	if err != nil {
		return nil, true, err
	}
	return dirty, uncertain || drainUncertain, nil
}

func (w *linuxHistoryWatchEventSource) drain() ([]string, bool, error) {
	// The event source is owned by one bridge and drained synchronously. Reuse
	// the fixed read buffer and dirty set across cycles so a quiet poll does not
	// pay a fresh 64 KiB allocation or map setup. The returned slice is a new
	// snapshot; callers may retain or sort it independently.
	if len(w.readBuffer) == 0 {
		w.readBuffer = make([]byte, 64*1024)
	}
	buffer := w.readBuffer
	if w.dirtyPaths == nil {
		w.dirtyPaths = make(map[string]struct{})
	}
	clear(w.dirtyPaths)
	dirty := w.dirtyPaths
	uncertain := false
	for {
		n, err := unix.Read(w.fd, buffer)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			break
		}
		if err != nil {
			return nil, true, err
		}
		if n == 0 {
			break
		}
		data := buffer[:n]
		for len(data) >= unix.SizeofInotifyEvent {
			event := *(*unix.InotifyEvent)(unsafe.Pointer(&data[0]))
			recordSize := unix.SizeofInotifyEvent + int(event.Len)
			if recordSize > len(data) {
				return nil, true, errors.New("truncated inotify event")
			}
			if event.Mask&unix.IN_Q_OVERFLOW != 0 || event.Mask&(unix.IN_IGNORED|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF) != 0 {
				uncertain = true
				// The kernel has invalidated this watch. Remove it from both
				// indexes immediately so an incremental update can register a
				// recreated directory at the same path instead of believing the
				// dead watch is still active.
				if dir, ok := w.dirsByWatch[event.Wd]; ok {
					delete(w.dirsByWatch, event.Wd)
					delete(w.watchedDirs, dir)
					w.pendingDirs[dir] = struct{}{}
				}
			}
			dir, ok := w.dirsByWatch[event.Wd]
			if ok && event.Len > 0 {
				nameBytes := data[unix.SizeofInotifyEvent:recordSize]
				nameBytes = bytes.TrimRight(nameBytes, "\x00")
				name := strings.TrimSpace(string(nameBytes))
				if name != "" {
					path := filepath.Join(dir, name)
					if event.Mask&unix.IN_ISDIR != 0 {
						uncertain = true
					} else if strings.HasSuffix(path, ".jsonl") {
						dirty[filepath.Clean(path)] = struct{}{}
					}
				}
			}
			data = data[recordSize:]
		}
		if len(data) != 0 {
			return nil, true, errors.New("misaligned inotify event stream")
		}
	}
	out := make([]string, 0, len(dirty))
	for path := range dirty {
		out = append(out, path)
	}
	return uniqueSortedCleanPaths(out), uncertain, nil
}

func (w *linuxHistoryWatchEventSource) Close() error {
	if w == nil || w.fd < 0 {
		return nil
	}
	err := unix.Close(w.fd)
	w.fd = -1
	return err
}
