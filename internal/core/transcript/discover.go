package transcript

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Window is how far back discovery and catch-up look (owner decision).
const Window = 120 * 24 * time.Hour

// Found is one rollout file seen by Walk.
type Found struct {
	Path string
	Info FileInfo
}

// Walk lists the rollout files (rollout-*.jsonl) under sessionsDir whose
// modification time is at or after since, oldest first. When dirsFrom is
// not zero, only the YYYY/MM/DD directories dated on or after that day are
// visited (cheap periodic scan for new sessions); otherwise every
// directory is visited, because a session started long ago may have been
// resumed recently.
func Walk(sessionsDir string, since, dirsFrom time.Time) ([]Found, error) {
	var out []Found
	err := filepath.WalkDir(sessionsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				if d != nil && d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			return err
		}
		if d.IsDir() {
			if path == sessionsDir || dirsFrom.IsZero() {
				return nil
			}
			if day, ok := dirDay(sessionsDir, path); ok && day.Before(dirsFrom.Truncate(24*time.Hour).AddDate(0, 0, -1)) {
				return fs.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasPrefix(name, "rollout-") || !strings.HasSuffix(name, ".jsonl") {
			return nil
		}
		fi, err := Stat(path)
		if err != nil {
			return nil
		}
		if fi.MTime.Before(since) {
			return nil
		}
		out = append(out, Found{Path: path, Info: fi})
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		err = nil
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Info.MTime.Before(out[j].Info.MTime) })
	return out, err
}

// DayDir is one sessions/YYYY/MM/DD directory.
type DayDir struct {
	Path string
	Day  time.Time
}

// DayDirs lists the day directories under sessionsDir, oldest first, by
// reading only the year and month directories.
func DayDirs(sessionsDir string) ([]DayDir, error) {
	var out []DayDir
	years, err := os.ReadDir(sessionsDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, y := range years {
		if !y.IsDir() {
			continue
		}
		months, err := os.ReadDir(filepath.Join(sessionsDir, y.Name()))
		if err != nil {
			continue
		}
		for _, m := range months {
			if !m.IsDir() {
				continue
			}
			days, err := os.ReadDir(filepath.Join(sessionsDir, y.Name(), m.Name()))
			if err != nil {
				continue
			}
			for _, d := range days {
				p := filepath.Join(sessionsDir, y.Name(), m.Name(), d.Name())
				if day, ok := dirDay(sessionsDir, p); ok && d.IsDir() {
					out = append(out, DayDir{Path: p, Day: day})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Day.Before(out[j].Day) })
	return out, nil
}

// dirDay returns the date of a sessions/YYYY[/MM[/DD]] directory (the last
// day it can contain, for partial paths it is the end of the period).
func dirDay(root, path string) (time.Time, bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return time.Time{}, false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	nums := make([]int, 0, 3)
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return time.Time{}, false
		}
		nums = append(nums, n)
	}
	switch len(nums) {
	case 1:
		return time.Date(nums[0], 12, 31, 0, 0, 0, 0, time.UTC), true
	case 2:
		return time.Date(nums[0], time.Month(nums[1])+1, 0, 0, 0, 0, 0, time.UTC), true
	case 3:
		return time.Date(nums[0], time.Month(nums[1]), nums[2], 0, 0, 0, 0, time.UTC), true
	}
	return time.Time{}, false
}

// ReadMeta reads the session_meta record (the first line) of a rollout.
func ReadMeta(path string) (SessionMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return SessionMeta{}, err
	}
	defer f.Close()
	lines, _, err := Read(f, 0, 1)
	if err != nil {
		return SessionMeta{}, err
	}
	if len(lines) == 0 || lines[0].Oversize {
		return SessionMeta{}, errors.New("transcript: no session_meta record")
	}
	ev := ParseLine(lines[0])
	if ev.Kind != KindSessionMeta {
		return SessionMeta{}, errors.New("transcript: first record is not session_meta")
	}
	return *ev.Meta, nil
}

// FirstPrompt returns the first user prompt of a rollout (for chat titles),
// reading at most limit bytes.
func FirstPrompt(path string, limit int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	lines, _, _ := Read(f, 0, limit)
	for _, l := range lines {
		if l.Oversize {
			continue
		}
		if ev := ParseLine(l); ev.Kind == KindUserMessage && !IsBoilerplate(ev.Text) && strings.TrimSpace(ev.Text) != "" {
			return strings.TrimSpace(ev.Text)
		}
	}
	return ""
}
