package transcript

// The usage cache behind `helper stats` (F-STA-1). v0.1.20 read the whole
// linked rollout on every request; the owner's sessions directory holds
// thousands of rollouts and tens of GB, so the cache keeps each rollout's
// parse state (usage.go) and its read offset, and a request only reads
// what was appended since the last one. Nothing runs between requests (no
// timer, no watcher): the cache is brought up to date by the request that
// needs it. Nothing is persisted (F-STA-1): after a restart the first
// request reads the rollouts it needs once.
//
// The index of the sessions directory is kept the same way: a day
// directory is listed again only when its modification time changed, and
// a rollout's first record (who spawned it) is read once.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// UsageCache is the incremental token usage of the rollouts under a
// sessions directory. It is safe for concurrent use; requests are
// serialized.
type UsageCache struct {
	mu  sync.Mutex
	dir string
	c   cacheData
	// byThread maps thread id -> rollout paths (from the file names).
	byThread map[string][]string
}

type cacheData struct {
	Days  map[string]dayEntry
	Files map[string]*usageFile
}

type dayEntry struct {
	MTime int64
	Day   int64 // unix seconds of the day
	Names []string
}

// usageFile is one rollout's cached state.
type usageFile struct {
	Thread   string
	MetaDone bool
	MetaInfo FileInfo // when the first record could not be read: retry once it changes
	Subagent bool
	Parent   string // immediate parent thread: subagent spawner or forked_from_id
	Title    string
	Info     FileInfo // at the last read
	Off      int64    // bytes consumed (complete lines)
	St       *usageState
}

// NewUsageCache returns the (empty) cache of sessionsDir.
func NewUsageCache(sessionsDir string) *UsageCache {
	return &UsageCache{dir: sessionsDir, c: cacheData{Days: map[string]dayEntry{}, Files: map[string]*usageFile{}}}
}

// RolloutUsage is the usage of one rollout.
type RolloutUsage struct {
	Path   string
	Thread string
	Title  string // a subagent's nickname, else its thread id
	Stats  TokenStats
	Err    error // the rollout could not be read
	// Problem: a fork child whose inherited prefix could not be separated
	// (Stats then hold the whole rollout), or whose parent is unavailable.
	Problem string
	Fork    bool // a fork of another thread (its prefix was separated)
}

// ChatUsage is the usage of one chat's threads.
type ChatUsage struct {
	Main      []RolloutUsage // the threads' rollouts, oldest thread first
	Missing   []string       // threads without a rollout
	Subagents []RolloutUsage // every subagent spawned under them
}

// Usage brings the cache up to date for the chats' threads (each list
// oldest first) and returns their usage. It reads only what changed since
// the last call. ctx cancels a long first read.
func (c *UsageCache) Usage(ctx context.Context, chats [][]string) ([]ChatUsage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.refreshIndex(); err != nil {
		return nil, err
	}
	out := make([]ChatUsage, len(chats))
	minDay := int64(-1)
	for i, threads := range chats {
		for _, t := range dedupe(threads) {
			paths := c.byThread[t]
			if len(paths) == 0 {
				out[i].Missing = append(out[i].Missing, t)
				continue
			}
			for _, p := range paths {
				if d := c.dayOf(p); minDay < 0 || d < minDay {
					minDay = d
				}
			}
		}
	}
	if minDay < 0 {
		return out, nil
	}
	// Subagents are spawned after their parent started: only rollouts of
	// the day before the oldest root on can be one (a day of margin for
	// time zones).
	children, err := c.children(ctx, minDay-86400)
	if err != nil {
		return nil, err
	}
	r := &refresher{c: c, ctx: ctx, done: map[string]bool{}, busy: map[string]bool{}}
	for i, threads := range chats {
		seen := map[string]bool{}
		var queue []string
		for _, t := range dedupe(threads) {
			for _, p := range c.byThread[t] {
				out[i].Main = append(out[i].Main, r.rollout(p))
				seen[p] = true
				queue = append(queue, c.c.Files[p].Thread)
			}
		}
		for len(queue) > 0 {
			t := queue[0]
			queue = queue[1:]
			for _, p := range children[t] {
				if seen[p] {
					continue
				}
				seen[p] = true
				out[i].Subagents = append(out[i].Subagents, r.rollout(p))
				queue = append(queue, c.c.Files[p].Thread)
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// refreshIndex lists the day directories, again only those whose
// modification time changed, and rebuilds the thread index.
func (c *UsageCache) refreshIndex() error {
	days, err := DayDirs(c.dir)
	if err != nil {
		return err
	}
	live := make(map[string]bool, len(days))
	for _, d := range days {
		live[d.Path] = true
		fi, err := os.Stat(d.Path)
		if err != nil {
			continue
		}
		mt := fi.ModTime().UnixNano()
		if e, ok := c.c.Days[d.Path]; ok && e.MTime == mt {
			continue
		}
		ents, err := os.ReadDir(d.Path)
		if err != nil {
			continue
		}
		var names []string
		for _, e := range ents {
			if n := e.Name(); !e.IsDir() && strings.HasPrefix(n, "rollout-") && strings.HasSuffix(n, ".jsonl") {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		c.c.Days[d.Path] = dayEntry{MTime: mt, Day: d.Day.Unix(), Names: names}
	}
	files := map[string]bool{}
	c.byThread = map[string][]string{}
	for p, e := range c.c.Days {
		if !live[p] {
			delete(c.c.Days, p)
			continue
		}
		for _, n := range e.Names {
			path := filepath.Join(p, n)
			files[path] = true
			if t := threadOfName(n); t != "" {
				c.byThread[t] = append(c.byThread[t], path)
			}
		}
	}
	for p := range c.c.Files {
		if !files[p] {
			delete(c.c.Files, p) // deleted rollout
		}
	}
	for _, ps := range c.byThread {
		sort.Strings(ps)
	}
	return nil
}

// threadOfName returns the thread id at the end of a rollout file name
// (rollout-<time>-<uuid>.jsonl).
func threadOfName(name string) string {
	s := strings.TrimSuffix(name, ".jsonl")
	if len(s) < 36 {
		return ""
	}
	id := s[len(s)-36:]
	if id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return ""
	}
	return id
}

func (c *UsageCache) dayOf(path string) int64 {
	if e, ok := c.c.Days[filepath.Dir(path)]; ok {
		return e.Day
	}
	return 0
}

func (c *UsageCache) entry(path string) *usageFile {
	f := c.c.Files[path]
	if f == nil {
		f = &usageFile{Thread: threadOfName(filepath.Base(path))}
		c.c.Files[path] = f
	}
	return f
}

// meta reads a rollout's first record once.
func (c *UsageCache) meta(path string) *usageFile {
	f := c.entry(path)
	if f.MetaDone {
		return f
	}
	info, err := Stat(path)
	if err != nil || (f.MetaInfo != FileInfo{} && f.MetaInfo == info) {
		return f
	}
	m, err := readUsageMeta(path)
	if err != nil {
		f.MetaInfo = info
		return f
	}
	f.MetaDone, f.MetaInfo = true, FileInfo{}
	if m.id != "" {
		f.Thread = m.id
	}
	f.Subagent, f.Parent, f.Title = m.subagent, m.parent, m.title
	return f
}

// children maps parent thread -> subagent rollouts, among the rollouts of
// the days from `from` (unix seconds) on.
func (c *UsageCache) children(ctx context.Context, from int64) (map[string][]string, error) {
	out := map[string][]string{}
	n := 0
	for dir, e := range c.c.Days {
		if e.Day < from {
			continue
		}
		for _, name := range e.Names {
			if n++; n%256 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			p := filepath.Join(dir, name)
			f := c.meta(p)
			if f.Subagent && f.Parent != "" {
				out[f.Parent] = append(out[f.Parent], p)
			}
		}
	}
	for _, ps := range out {
		sort.Strings(ps)
	}
	return out, nil
}

type usageMeta struct {
	id, parent, title string
	subagent          bool
}

// readUsageMeta decodes a rollout's session_meta record: its id, whether
// it is a subagent, and its immediate parent (the spawning thread, else
// the thread it was forked from), as 0.1.22's session index read them.
func readUsageMeta(path string) (usageMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return usageMeta{}, err
	}
	defer f.Close()
	lines, _, err := Read(f, 0, 1)
	if err != nil {
		return usageMeta{}, err
	}
	if len(lines) == 0 || lines[0].Oversize || lines[0].Cut {
		return usageMeta{}, errors.New("transcript: no session_meta record")
	}
	var env struct {
		Type    string `json:"type"`
		Payload struct {
			ID             string          `json:"id"`
			Source         json.RawMessage `json:"source"`
			ParentThreadID string          `json:"parent_thread_id"`
			ForkedFrom     string          `json:"forked_from_id"`
			Nickname       string          `json:"agent_nickname"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(lines[0].Data, &env); err != nil || env.Type != "session_meta" {
		return usageMeta{}, errors.New("transcript: first record is not session_meta")
	}
	p := env.Payload
	m := usageMeta{id: strings.TrimSpace(p.ID), title: strings.TrimSpace(p.Nickname)}
	var spawn string
	m.subagent, spawn = subagentSource(p.Source)
	m.parent = firstString(spawn, p.ParentThreadID, p.ForkedFrom)
	if m.title == "" {
		m.title = m.id
	}
	return m, nil
}

// subagentSource parses session_meta.source (0.1.22 parseSessionSource).
func subagentSource(raw json.RawMessage) (bool, string) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return false, ""
	}
	var outer struct {
		Subagent json.RawMessage `json:"subagent"`
	}
	if json.Unmarshal(raw, &outer) != nil || len(outer.Subagent) == 0 {
		return false, ""
	}
	sub := bytes.TrimSpace(outer.Subagent)
	if len(sub) > 0 && sub[0] == '{' {
		var o struct {
			ThreadSpawn struct {
				ParentThreadID string `json:"parent_thread_id"`
			} `json:"thread_spawn"`
		}
		if json.Unmarshal(sub, &o) == nil {
			return true, strings.TrimSpace(o.ThreadSpawn.ParentThreadID)
		}
	}
	return true, ""
}

// refresher brings rollouts up to date, each parent before its children.
type refresher struct {
	c    *UsageCache
	ctx  context.Context
	done map[string]bool
	busy map[string]bool // parents being resolved (a fork cycle is not followed)
	errs map[string]error
}

// rollout refreshes one rollout and returns its usage.
func (r *refresher) rollout(path string) RolloutUsage {
	f := r.c.meta(path)
	u := RolloutUsage{Path: path, Thread: f.Thread, Title: f.Title}
	if u.Title == "" {
		u.Title = f.Thread
	}
	ledger, problem := r.parent(f)
	if err := r.refresh(path, f, ledger); err != nil {
		u.Err = err
		return u
	}
	if f.St == nil {
		return u
	}
	st, reason := f.St.clone().stats()
	st.SourcePath = path
	u.Stats = st
	switch {
	case problem != "":
		u.Problem = problem
	case reason != "":
		u.Problem = reason
	case f.Parent != "" && f.St.Attr != nil:
		u.Fork = true
	}
	return u
}

// parent returns the immediate parent's ledger of a fork child (nil when
// the rollout is not one), or why it is unavailable.
func (r *refresher) parent(f *usageFile) (*parentLedger, string) {
	if f.Parent == "" || f.Parent == f.Thread {
		return nil, ""
	}
	paths := r.c.byThread[f.Parent]
	if len(paths) == 0 {
		return nil, "immediate parent transcript is unavailable for token attribution"
	}
	pp := paths[0]
	if r.busy[pp] {
		return nil, "immediate parent transcript is unavailable for token attribution"
	}
	r.busy[pp] = true
	pf := r.c.meta(pp)
	pl, _ := r.parent(pf)
	delete(r.busy, pp)
	if err := r.refresh(pp, pf, pl); err != nil {
		return nil, "immediate parent transcript could not be read: " + err.Error()
	}
	if pf.St == nil {
		return &parentLedger{}, ""
	}
	return &parentLedger{TL: pf.St.TL, Incomplete: pf.St.NonCum}, ""
}

// refresh reads what was appended to a rollout since the last read.
// ledger is the parent's of a fork child; a child read without one (its
// parent was unavailable) is read again from the start once it is there.
func (r *refresher) refresh(path string, f *usageFile, ledger *parentLedger) error {
	if r.done[path] {
		return r.errs[path]
	}
	err := r.c.read(r.ctx, path, f, ledger)
	r.done[path] = true
	if err != nil {
		if r.errs == nil {
			r.errs = map[string]error{}
		}
		r.errs[path] = err
	}
	return err
}

func (c *UsageCache) read(ctx context.Context, path string, f *usageFile, ledger *parentLedger) error {
	info, err := Stat(path)
	if err != nil {
		return err
	}
	child := ledger != nil
	if f.St == nil || info.Inode != f.Info.Inode || info.Size < f.Off || (f.St.Attr != nil) != child {
		f.St, f.Off = newUsageState(child), 0
	}
	f.Info = info
	if info.Size == f.Off {
		return nil
	}
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	off, err := feed(ctx, fh, f.St, f.Off, info.Size, ledger)
	if off != f.Off {
		f.Off = off
	}
	return err
}

// feed consumes the complete lines of f between off and end and returns
// the offset after the last one. A record that cannot carry usage is
// classified from its first bytes and not buffered.
func feed(ctx context.Context, f *os.File, st *usageState, off, end int64, ledger *parentLedger) (int64, error) {
	return feedReader(ctx, io.NewSectionReader(f, off, end-off), st, off, ledger)
}

// feedReader is feed over a reader that starts at offset off.
func feedReader(ctx context.Context, rd io.Reader, st *usageState, off int64, ledger *parentLedger) (int64, error) {
	r := bufio.NewReaderSize(rd, 256<<10)
	pos := off
	var buf []byte
	nextCheck := off + 8<<20
	for {
		if pos >= nextCheck {
			if err := ctx.Err(); err != nil {
				return off, err
			}
			nextCheck = pos + 8<<20
		}
		buf = buf[:0]
		decided, skip, short, over := false, false, false, false
		var last byte
		var n int64
		for {
			chunk, err := r.ReadSlice('\n')
			n += int64(len(chunk))
			complete := err == nil
			data := chunk
			if complete {
				data = chunk[:len(chunk)-1]
			}
			if b, ok := lastNonSpace(data); ok {
				last = b
			}
			switch {
			case !decided:
				if len(buf) == 0 {
					data = bytes.TrimLeft(data, " \t\r")
				}
				buf = append(buf, data...)
				if complete || len(buf) >= cheapRecordAt {
					decided = true
					if len(buf) > 0 && buf[0] == '{' && skippable(head(buf, cutHead)) {
						skip = true
						short = complete && len(buf) <= cheapRecordAt
					}
				}
			case skip || over:
			case len(buf)+len(data) > MaxLine:
				over, buf = true, buf[:0]
			default:
				buf = append(buf, data...)
			}
			if complete {
				break
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			// The rest is an incomplete record (no newline yet): it is read
			// again once complete.
			if errors.Is(err, io.EOF) {
				return off, nil
			}
			return off, err
		}
		pos += n
		off = pos
		switch {
		case skip && short:
			// A short record is checked exactly, as v0.1.20 decoded it.
			if json.Valid(buf) {
				st.blank()
			} else {
				st.skipped(0)
			}
		case skip:
			st.skipped(last)
		case over:
			st.blank() // a record of more than MaxLine bytes cannot be a usage event
		case len(buf) == 0 || buf[0] != '{':
			st.blank()
		default:
			st.consume(buf, ledger)
		}
		if buf != nil && cap(buf) > 1<<20 {
			buf = nil // do not keep a large record's buffer
		}
	}
}

func lastNonSpace(b []byte) (byte, bool) {
	for i := len(b) - 1; i >= 0; i-- {
		switch b[i] {
		case ' ', '\t', '\r', '\n':
			continue
		}
		return b[i], true
	}
	return 0, false
}

// Aggregate combines the usage of several main rollouts of one chat
// (0.1.22 aggregateCodexTokenStats): totals and model details are summed;
// the last call, context window and rate limits are the newest
// rollout's. stats are oldest first.
func Aggregate(stats []TokenStats) TokenStats {
	switch len(stats) {
	case 0:
		return TokenStats{}
	case 1:
		return stats[0]
	}
	newest := stats[len(stats)-1]
	latest := newest
	for i := len(stats) - 1; i >= 0; i-- {
		if stats[i].HasUsage() {
			latest = stats[i]
			break
		}
	}
	out := newest
	out.Source = fmt.Sprintf("aggregated across %d linked parent transcripts", len(stats))
	out.SourceLine = latest.SourceLine
	out.SourcePath = ""
	out.SourcePaths = nil
	out.Diagnostics = nil
	out.FallbackOnly = true
	out.UsageEvents, out.NonAdvancing, out.Resets, out.Recoveries, out.MissingLast, out.Overflow = 0, 0, 0, 0, 0, false
	var total TokenUsage
	var tiers [][]ModelTierUsage
	var models [][]ModelUsage
	for _, s := range stats {
		if s.SourcePath != "" && !contains(out.SourcePaths, s.SourcePath) {
			out.SourcePaths = append(out.SourcePaths, s.SourcePath)
		}
		out.UsageEvents += s.UsageEvents
		out.NonAdvancing += s.NonAdvancing
		out.Resets += s.Resets
		out.Recoveries += s.Recoveries
		out.MissingLast += s.MissingLast
		out.Overflow = out.Overflow || s.Overflow
		out.FallbackOnly = out.FallbackOnly && s.FallbackOnly
		for k, n := range s.Diagnostics {
			if out.Diagnostics == nil {
				out.Diagnostics = map[string]int{}
			}
			out.Diagnostics[k] += n
		}
		total, _ = AddUsage(total, s.Overall())
		tiers = append(tiers, s.ModelTiers)
		models = append(models, s.Models)
	}
	out.NativeLatestTotal = TokenUsage{}
	out.Info.Total = total
	if latest.Info.Last.HasTokens() {
		out.Info.Last = latest.Info.Last
	}
	out.ModelTiers = SumTiers(tiers...)
	out.Models = SumModels(models...)
	return out
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}
