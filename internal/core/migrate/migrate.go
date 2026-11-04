// Package migrate converts a legacy Teams store (v0.1.20 or
// v0.1.22-rc.59) into a core store (STORE-DESIGN.md §7). It is offline and
// one-shot: the source is never modified or deleted, and ~/.codex is only
// read (stat and the first line of rollouts).
package migrate

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Options configure one conversion.
type Options struct {
	// Source is the legacy scope directory holding state.json.
	Source string
	// Out is where core.sqlite is written; it defaults to Source.
	Out string
	// DryRun writes the result into Out and never touches Source (no lock
	// file, no marker). Out must be set.
	DryRun bool
	// Immutable reads an existing consistent snapshot directly (read-only,
	// immutable=1) instead of taking an online backup. It is refused when a
	// non-empty -wal file exists.
	Immutable bool
	// CodexSessions is the rollout root (~/.codex/sessions) used for stat
	// and first-line classification. Empty skips the disk scan.
	CodexSessions string
	// CountBacklog scans every tracked rollout to EOF (read-only) to count
	// the prompts and finals the mirror will post on first start. Without
	// it the proof scan stops at the furthest old position.
	CountBacklog bool
	// Now is the clock (defaults to time.Now).
	Now func() time.Time

	registry legacyRegistry
}

const (
	// TargetName is the file name of the converted store.
	TargetName = "core.sqlite"
	// MarkerName is dropped into the source directory after a real migration.
	MarkerName = "MIGRATED-TO-core.sqlite"
	// ReportName is the report written next to the target.
	ReportName = "migration-report.txt"
)

// Run converts opt.Source. It returns the report even on failure when one
// was produced.
func Run(opt Options) (*Report, error) {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Source == "" {
		return nil, errors.New("migrate: no source directory")
	}
	if opt.Out == "" {
		if opt.DryRun {
			return nil, errors.New("migrate: --dry-run needs --out")
		}
		opt.Out = opt.Source
	}
	now := opt.Now()
	rep := newReport()
	rep.SourceDir, rep.DryRun, rep.Started = opt.Source, opt.DryRun, now
	start := time.Now()

	version, dbPath, err := identify(opt.Source)
	if err != nil {
		return nil, err
	}
	rep.Version = version
	if _, err := os.Stat(filepath.Join(opt.Source, MarkerName)); err == nil && !opt.DryRun {
		return nil, fmt.Errorf("migrate: %s already migrated (%s exists)", opt.Source, MarkerName)
	}
	if err := os.MkdirAll(opt.Out, 0o700); err != nil {
		return nil, err
	}
	final := filepath.Join(opt.Out, TargetName)
	rep.OutPath = final
	if _, err := os.Stat(final); err == nil {
		return nil, fmt.Errorf("migrate: %s already exists", final)
	}
	unlock, err := lockSource(opt.Source, !opt.DryRun)
	if err != nil {
		return nil, err
	}
	defer unlock()

	snapDir := filepath.Join(opt.Out, "migrate.tmp")
	l, err := openSource(opt.Source, version, dbPath, opt.Immutable, snapDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		l.close()
		_ = os.RemoveAll(snapDir)
	}()
	if err := l.checkLive(now); err != nil {
		return nil, err
	}
	opt.registry, err = readRegistry(opt.Source)
	if err != nil {
		rep.note("registry.json unreadable: %v", err)
	}
	links, err := readThreadLinks(filepath.Join(opt.Source, "thread-links"))
	if err != nil {
		rep.note("thread-links journal unreadable: %v", err)
	}
	rep.Notes = append(rep.Notes, l.notes...)

	tmp := final + ".tmp"
	for _, p := range []string{tmp, tmp + "-wal", tmp + "-shm"} {
		_ = os.Remove(p)
	}
	st, err := store.OpenWith(tmp, store.Options{Now: opt.Now})
	if err != nil {
		return nil, err
	}
	c := &converter{
		l: l, rep: rep, opt: opt, now: now,
		sessions: map[string]*sessInfo{}, convByChat: map[string]int64{}, labelOf: map[int64]string{},
		forkParent: map[string]string{}, holdChild: map[string]bool{}, polls: map[string]legacyPoll{},
		turns: map[string]*compactTurn{}, extSeen: map[int64]map[string]bool{}, oldestIn: map[int64]int64{},
		inboundOld: map[string]int64{}, needPause: map[int64]string{}, threadLinks: links,
		orphans: map[string]int64{}, lastAct: map[int64]int64{},
		proofs: map[string]*proof{}, outboxMirror: map[string]string{}, loserOf: map[int64]int64{}, mirrorUsed: map[string]bool{},
	}
	err = st.Import(func(im *store.Importer) error {
		c.im = im
		return c.run()
	})
	if err == nil {
		validate(st, c)
	}
	closeErr := st.Close()
	rep.Wall = time.Since(start)
	if err == nil && closeErr != nil {
		err = closeErr
	}
	if err == nil && !rep.OK() {
		err = errors.New("migrate: validation failed (see report); nothing was renamed")
	}
	if werr := writeReport(filepath.Join(opt.Out, ReportName), rep); werr != nil && err == nil {
		err = werr
	}
	if err != nil {
		return rep, err
	}
	if err := syncFile(tmp); err != nil {
		return rep, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return rep, err
	}
	_ = syncDir(opt.Out)
	if !opt.DryRun {
		body := fmt.Sprintf("migrated to %s at %s by cxp teams migrate (%s)\n", final, now.Format(time.RFC3339), version)
		if err := os.WriteFile(filepath.Join(opt.Source, MarkerName), []byte(body), 0o600); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

func writeReport(path string, rep *Report) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	rep.WriteText(w)
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func readRegistry(dir string) (legacyRegistry, error) {
	var r legacyRegistry
	raw, err := os.ReadFile(filepath.Join(dir, "registry.json"))
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	_, err = decode(raw, &r)
	return r, err
}

// readThreadLinks folds the thread-link journal: the latest record per session.
func readThreadLinks(dir string) (map[string]threadLink, error) {
	out := map[string]threadLink{}
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return out, err
	}
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			return out, err
		}
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 64<<10), 16<<20)
		for sc.Scan() {
			var tl threadLink
			if json.Unmarshal(sc.Bytes(), &tl) != nil || tl.SessionID == "" || tl.CodexThreadID == "" {
				continue
			}
			if cur, ok := out[tl.SessionID]; !ok || !tl.ObservedAt.Before(cur.ObservedAt.Time) {
				out[tl.SessionID] = tl
			}
		}
		fh.Close()
	}
	return out, nil
}

// Candidates lists legacy scope directories (holding state.json) under a
// codex-helper state root: <root>/teams and <root>/teams/scopes/*.
func Candidates(roots ...string) []string {
	var out []string
	seen := map[string]bool{}
	for _, root := range roots {
		if root == "" {
			continue
		}
		matches, _ := filepath.Glob(filepath.Join(root, "teams", "scopes", "*", "state.json"))
		matches = append([]string{filepath.Join(root, "teams", "state.json")}, matches...)
		for _, m := range matches {
			if _, err := os.Stat(m); err != nil {
				continue
			}
			d := filepath.Dir(m)
			if !seen[d] {
				seen[d] = true
				out = append(out, d)
			}
		}
	}
	return out
}

var bg = context.Background()

func validate(st *store.Store, c *converter) {
	rep := c.rep
	add := func(name string, ok bool, detail string) {
		rep.Checks = append(rep.Checks, Check{Name: name, OK: ok, Detail: detail})
	}
	_ = st.View(func(q store.Querier) error {
		var integrity string
		_ = q.QueryRowContext(bg, `PRAGMA integrity_check`).Scan(&integrity)
		add("integrity_check", integrity == "ok", integrity)
		var fk int
		_ = q.QueryRowContext(bg, `SELECT count(*) FROM pragma_foreign_key_check`).Scan(&fk)
		add("foreign_key_check", fk == 0, fmt.Sprintf("%d violations", fk))

		// Every active or parked work conversation exists with the same chat and thread.
		var bad []string
		n := 0
		for _, label := range sortedKeys(c.sessions) {
			si := c.sessions[label]
			if si.state == "control" || (si.s.Status != "active" && si.s.Status != "archived") {
				continue
			}
			n++
			var chat, thread string
			err := q.QueryRowContext(bg, `SELECT COALESCE(c.ext_chat_id,''), COALESCE(t.thread_id,'') FROM conversations c
LEFT JOIN threads t ON t.conv_id = c.id WHERE c.label = ? AND c.kind = 'work'`, label).Scan(&chat, &thread)
			if err != nil || chat != si.s.TeamsChatID || thread != si.s.CodexThreadID {
				bad = append(bad, label)
			}
		}
		add("active/parked work conversations keep chat and thread", len(bad) == 0, fmt.Sprintf("%d checked, mismatches: %s", n, strings.Join(bad, ",")))

		var ctl int
		_ = q.QueryRowContext(bg, `SELECT count(*) FROM conversations WHERE kind = 'control' AND ext_chat_id IS NOT NULL`).Scan(&ctl)
		add("control chat binding present", c.controlChat == "" || ctl == 1, fmt.Sprintf("source %q, target rows %d", c.controlChat, ctl))

		var unk int
		_ = q.QueryRowContext(bg, `SELECT count(*) FROM outbox WHERE state = 'unknown'`).Scan(&unk)
		add("unknown outbox rows = source unknown-outcome rows", unk == c.unknownSrc, fmt.Sprintf("%d / %d", unk, c.unknownSrc))

		var zero int
		_ = q.QueryRowContext(bg, `SELECT count(*) FROM transcript_sources WHERE state = 'tracking' AND offset = 0 AND size > 0`).Scan(&zero)
		add("tracking sources at offset 0 are exactly the no-proof ones resumed at 0", zero == c.zeroNoProof, fmt.Sprintf("%d / %d", zero, c.zeroNoProof))

		lost := 0
		for loser := range c.loserOf {
			var n int
			_ = q.QueryRowContext(bg, `SELECT count(*) FROM transcript_sources WHERE conv_id = ?`, loser).Scan(&n)
			lost += n
		}
		add("parked shared-thread conversations own no transcript source", lost == 0, fmt.Sprintf("%d losers, %d sources", len(c.loserOf), lost))

		var maxKey int
		_ = q.QueryRowContext(bg, `SELECT COALESCE(MAX(length(turn_key)),0) FROM turns`).Scan(&maxKey)
		add("turn keys <= 64 chars", maxKey <= 64, fmt.Sprintf("max %d", maxKey))
		return nil
	})
}
