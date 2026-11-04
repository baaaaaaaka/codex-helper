package migrate

import (
	"bufio"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

var rolloutThread = regexp.MustCompile(`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$`)

func threadFromName(path string) string {
	if m := rolloutThread.FindStringSubmatch(filepath.Base(path)); m != nil {
		return m[1]
	}
	return ""
}

type offsetCand struct {
	rank   int // lower wins
	src    string
	offset int64
	line   int64
}

type pathInfo struct {
	path           string
	conv           int64 // chosen conversation (0: unlinked)
	convs          map[int64]bool
	thread         string
	subagent       bool
	cands          []offsetCand
	anchor         *legacyAnchor
	rewriteBlocked bool
	progress       bool
	recSize        int64
	recMTime       int64
	fromDisk       bool
	// positions recorded by the old core (none of them is proof)
	ckpt, ckptLine     int64
	cutoff, cutoffLine int64
	watch, watchLine   int64
	scan               *scanResult
	scanFrom, scanLine int64
}

func (c *converter) pinfo(m map[string]*pathInfo, path string) *pathInfo {
	p := m[path]
	if p == nil {
		p = &pathInfo{path: path, convs: map[int64]bool{}}
		m[path] = p
	}
	return p
}

// convertSources builds transcript_sources from import checkpoints, the
// history watch and (when available) a stat-only scan of the rollouts.
func (c *converter) convertSources() error {
	paths := map[string]*pathInfo{}
	threadConv, err := c.resolveSharedThreads()
	if err != nil {
		return err
	}
	ce := c.rep.E("import_checkpoints")
	rewriteFlags, watchFlags := 0, 0
	err = c.l.each("import_checkpoints", func(r row) error {
		ce.Source++
		var cp legacyCheckpoint
		if warn, err := decode(r.JSON, &cp); err != nil {
			ce.Failed = append(ce.Failed, r.ID)
			ce.Outcomes["unparsable -> skipped"]++
			return nil
		} else if warn != "" {
			ce.Outcomes["wrong-typed field tolerated"]++
			c.rep.note("checkpoint %s: %s", r.ID, warn)
		}
		path := firstNonEmpty(cp.SourcePath)
		if path == "" && cp.UnresolvedExecution != nil {
			path = cp.UnresolvedExecution.SourcePath
		}
		if path == "" {
			ce.Outcomes["no source path -> dropped"]++
			return nil
		}
		ce.Imported++
		p := c.pinfo(paths, path)
		if strings.Contains(r.ID, ":subagent:") {
			p.subagent = true
		} else if conv, ok := c.convOf(cp.SessionID, ""); ok && conv != c.control {
			p.convs[conv] = true
		}
		// rc.59 itself treats "last_offset_known || last_offset > 0" as known;
		// the flag only distinguishes a trusted zero, which is no resume point.
		if cp.LastOffset > 0 {
			p.cands = append(p.cands, offsetCand{rank: 1, src: "checkpoint", offset: cp.LastOffset, line: cp.LastSourceLine})
			if cp.LastOffset > p.ckpt {
				p.ckpt, p.ckptLine = cp.LastOffset, cp.LastSourceLine
			}
		}
		if cp.SourceSize > 0 && cp.Status == "complete" {
			p.cands = append(p.cands, offsetCand{rank: 3, src: "checkpoint source_size", offset: cp.SourceSize})
		}
		if cp.LastRecordID != "" || cp.LastSourceLine > 0 || cp.LastOffset > 0 || cp.SourceSize > 0 {
			p.progress = true
		}
		if cp.SourceSize > p.recSize {
			p.recSize, p.recMTime = cp.SourceSize, ms(cp.SourceModTime)
		}
		if cp.UnresolvedExecution != nil {
			a := *cp.UnresolvedExecution
			p.anchor = &a
			ce.Outcomes["execution anchor"]++
			if a.CutoffOffset > 0 {
				p.cands = append(p.cands, offsetCand{rank: 1, src: "anchor cutoff", offset: a.CutoffOffset, line: a.CutoffLine})
				p.cutoff, p.cutoffLine = a.CutoffOffset, a.CutoffLine
			}
		}
		if cp.SourceRewriteBlocked {
			p.rewriteBlocked = true
			rewriteFlags++
		}
		ce.Outcomes["status "+firstNonEmpty(cp.Status, "(empty)")]++
		return nil
	})
	if err != nil {
		return err
	}
	we := c.rep.E("history_watch")
	err = c.l.each("history_watch", func(r row) error {
		we.Source++
		var w legacyWatch
		if _, err := decode(r.JSON, &w); err != nil {
			we.Failed = append(we.Failed, r.ID)
			return nil
		}
		if w.Path == "" {
			we.Outcomes["no path -> dropped"]++
			return nil
		}
		we.Imported++
		p := c.pinfo(paths, w.Path)
		for _, t := range []string{w.ThreadID, w.SessionID, threadFromName(w.Path)} {
			if conv, ok := threadConv[t]; ok && t != "" {
				p.convs[conv] = true
				p.thread = t
				break
			}
		}
		if w.Offset > 0 {
			p.cands = append(p.cands, offsetCand{rank: 2, src: "history_watch", offset: w.Offset, line: w.Line})
			p.progress = true
			if w.Offset > p.watch {
				p.watch, p.watchLine = w.Offset, w.Line
			}
		}
		if w.Size > p.recSize {
			p.recSize, p.recMTime = w.Size, ms(w.ModTime)
		}
		if w.SourceRewriteBlocked {
			watchFlags++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if c.opt.CodexSessions != "" {
		err := filepath.WalkDir(c.opt.CodexSessions, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // unreadable subtree: skip
			}
			if d.IsDir() || !strings.HasPrefix(d.Name(), "rollout-") || !strings.HasSuffix(d.Name(), ".jsonl") {
				return nil
			}
			p := c.pinfo(paths, path)
			p.fromDisk = true
			return nil
		})
		if err != nil {
			return err
		}
	}
	// Link by the rollout file name when nothing else linked the path.
	for _, p := range paths {
		if len(p.convs) == 0 && !p.subagent {
			if conv, ok := threadConv[threadFromName(p.path)]; ok {
				p.convs[conv] = true
			}
		}
		if p.thread == "" {
			p.thread = threadFromName(p.path)
		}
	}
	c.rep.note("source_rewrite_blocked flags ignored (false positives): %d on checkpoints, %d on history_watch entries", rewriteFlags, watchFlags)

	keys := sortedKeys(paths)
	for _, k := range keys {
		p := paths[k]
		p.conv = c.sourceConv(p)
		if p.conv == 0 || c.opt.CodexSessions == "" {
			continue
		}
		// history_watch links a subagent rollout by its parent's session id;
		// it is never a conversation's own transcript.
		if meta, err := readSessionMeta(p.path); err == nil && meta.subagentOrExec() {
			p.conv, p.subagent = 0, true
			c.rep.Buckets["linked by its parent session, but subagent/exec -> unlinked"]++
		}
	}
	c.scanLinked(paths, keys)
	for _, k := range keys {
		if err := c.importSource(paths[k]); err != nil {
			return err
		}
	}
	return nil
}

// sourceConv picks the conversation a rollout is mirrored into (0: unlinked).
func (c *converter) sourceConv(p *pathInfo) int64 {
	var conv int64
	for cv := range p.convs {
		if w, lost := c.loserOf[cv]; lost {
			cv = w // a rollout belongs to the winner of its shared thread
		}
		if conv == 0 || c.preferSourceConv(cv, conv, p.thread) {
			conv = cv
		}
	}
	return conv
}

// scanLinked runs the bounded proof scan on every linked rollout that exists:
// from the furthest delivery proof (a validated record end) up to the
// furthest old position, or to EOF with Options.CountBacklog.
func (c *converter) scanLinked(paths map[string]*pathInfo, keys []string) {
	if c.opt.CodexSessions == "" {
		return
	}
	teams := map[string]bool{}
	for _, ct := range c.turns {
		if ct.t.CodexTurnID != "" {
			teams[ct.t.CodexTurnID] = true
		}
	}
	reqs := map[string]scanReq{}
	for _, k := range keys {
		p := paths[k]
		if p.conv == 0 {
			continue
		}
		fst := statFile(p.path)
		if !fst.ok {
			continue
		}
		q := scanReq{path: p.path, teamsTurn: teams, to: max(p.ckpt, p.cutoff, p.watch),
			offsets: []int64{p.ckpt, p.cutoff, p.watch}}
		if pr := c.proofs[p.path]; pr != nil {
			if pr.endOff > 0 && pr.endLine > 0 && pr.endOff <= fst.size && recordBoundary(p.path, pr.endOff) {
				q.from, q.fromLine = pr.endOff, pr.endLine
			} else if pr.endOff > 0 {
				c.rep.Buckets["proof: delivery offset is not a record end (or has no line) in the current file -> scanned from 0"]++
			}
			q.lines, q.ids = map[int64]bool{}, pr.ledgerIDs
			for ln := range pr.ledgerLines {
				if ln > q.fromLine {
					q.lines[ln] = true
					q.toLine = max(q.toLine, ln)
				}
			}
		}
		if c.opt.CountBacklog {
			q.to = fst.size
		}
		q.to = min(max(q.to, q.from), fst.size)
		p.scanFrom, p.scanLine = q.from, q.fromLine
		reqs[p.path] = q
	}
	res := c.runScans(reqs)
	for path, r := range res {
		r := r
		paths[path].scan = &r
	}
}

type fileStat struct {
	ok    bool
	size  int64
	mtime int64
	inode int64
}

func statFile(path string) fileStat {
	st, err := os.Stat(path)
	if err != nil || !st.Mode().IsRegular() {
		return fileStat{}
	}
	return fileStat{ok: true, size: st.Size(), mtime: st.ModTime().UnixMilli(), inode: inodeOf(st)}
}

func (c *converter) importSource(p *pathInfo) error {
	b := c.rep.Buckets
	canStat := c.opt.CodexSessions != ""
	fst := fileStat{}
	if canStat {
		fst = statFile(p.path)
	}
	size, mtime := p.recSize, p.recMTime
	if fst.ok {
		size, mtime = fst.size, fst.mtime
	}
	conv := p.conv
	if len(p.convs) > 1 {
		c.rep.note("rollout %s linked to %d conversations; kept %s", p.path, len(p.convs), c.labelOf[conv])
	}
	src := store.TranscriptSource{Path: p.path, ThreadID: p.thread, Size: size, MTime: mtime, Inode: fst.inode}

	if conv == 0 {
		// Unlinked rollout: classify by stat and the first line only.
		if p.anchor != nil {
			c.rep.note("execution anchor of %s is on an unlinked rollout %s (classified like any unlinked rollout)", p.anchor.SessionID, p.path)
		}
		if !canStat {
			if p.subagent {
				src.State, src.Offset = store.SrcIgnored, size
				b["unlinked subagent (checkpoint) -> ignored"]++
			} else {
				src.State, src.Offset = store.SrcBaseline, size
				b["unlinked, not classified (no rollout access) -> baseline at recorded size"]++
			}
			return c.putSource(src)
		}
		if !fst.ok {
			b["unlinked, file missing -> no row"]++
			return nil
		}
		meta, err := readSessionMeta(p.path)
		switch {
		case p.subagent || (err == nil && meta.subagentOrExec()):
			src.State, src.Offset = store.SrcIgnored, size
			b["unlinked subagent/exec -> ignored"]++
		case err == nil && meta.Originator == "codex-helper":
			src.State, src.Offset = store.SrcIgnored, size
			b["unlinked originator=codex-helper -> ignored"]++
		case err != nil:
			src.State, src.Offset = store.SrcBaseline, size
			b["unlinked, first line unreadable -> baseline at EOF"]++
		default:
			// No row, however old: runtime discovery creates its chat and
			// catches it up (no window, owner decision 2026-11-05).
			b["unlinked interactive -> no row (discovery)"]++
			return nil
		}
		if src.Offset < 0 {
			src.Offset = 0
		}
		return c.putSource(src)
	}

	src.ConvID = conv
	if si := c.sessionOfConv(conv); si != nil && si.s.CodexThreadID != "" && p.thread == "" {
		src.ThreadID = si.s.CodexThreadID
	}
	src.State = store.SrcTracking
	if canStat && !fst.ok {
		src.State = store.SrcMissing
	}
	scanned := p.scan != nil && p.scan.Err == nil
	if p.scan != nil && p.scan.Err != nil {
		c.rep.note("rollout %s: proof scan failed (%v); old position rule used", p.path, p.scan.Err)
	}
	// Closed conversations get no catch-up (owner decision 2026-11-04): the
	// source stays at its old position as a baseline, and is not mirrored
	// unless the core reopens the conversation.
	closed := false
	if si := c.sessionOfConv(conv); si != nil && si.state == store.ConvClosed {
		closed = true
	}
	switch {
	case closed:
		if src.State == store.SrcTracking {
			src.State = store.SrcBaseline
		}
		why := "closed conversation"
		if !scanned {
			why += ", not scanned"
		}
		c.legacyResume(p, &src, size, why)
		if scanned {
			c.reportResume(p, c.decideResume(p, size), size, true, src.Offset)
		}
	case scanned:
		rs := c.decideResume(p, size)
		src.Offset, src.Line = rs.off, rs.line
		c.reportResume(p, rs, size, false, 0)
	default:
		why := "file missing"
		if fst.ok {
			why = "no rollout access"
		}
		c.legacyResume(p, &src, size, why+", not scanned")
	}
	if fst.ok && src.Offset > fst.size {
		b["linked: recorded offset beyond EOF (runtime rewrite handling)"]++
	}
	if p.progress && src.Offset == 0 && (!scanned || closed) {
		c.rep.note("source %s had progress but no usable offset", p.path)
	}
	return c.putSource(src)
}

// resolveSharedThreads maps each Codex thread to the one conversation that
// owns its rollout. When several work conversations share a thread, the one
// with the most recent activity wins; the others are parked, keep their
// thread_id as history and get no transcript source.
func (c *converter) resolveSharedThreads() (map[string]int64, error) {
	byThread := map[string][]int64{}
	for _, label := range sortedKeys(c.sessions) {
		si := c.sessions[label]
		if si.s.CodexThreadID == "" || si.conv == 0 || si.state == "control" {
			continue
		}
		byThread[si.s.CodexThreadID] = append(byThread[si.s.CodexThreadID], si.conv)
	}
	out := map[string]int64{}
	e := c.rep.E("shared Codex threads")
	for _, th := range sortedKeys(byThread) {
		convs := byThread[th]
		win := convs[0]
		for _, cv := range convs[1:] {
			if a, b := c.lastAct[cv], c.lastAct[win]; a > b || (a == b && c.preferConv(cv, win)) {
				win = cv
			}
		}
		out[th] = win
		if len(convs) == 1 {
			continue
		}
		e.Source++
		for _, cv := range convs {
			if cv == win {
				continue
			}
			c.loserOf[cv] = win
			si := c.sessionOfConv(cv)
			before := si.state
			if before == store.ConvActive || before == store.ConvCreating {
				if err := c.im.Park(cv); err != nil {
					return nil, err
				}
				si.state = store.ConvParked
			}
			e.Imported++
			e.Outcomes["loser "+before+" -> "+si.state+", no transcript source"]++
			c.rep.Shared = append(c.rep.Shared, SharedThread{Thread: th, Winner: c.labelOf[win], WinnerLast: c.lastAct[win],
				Loser: c.labelOf[cv], LoserLast: c.lastAct[cv], LoserBefore: before, LoserAfter: si.state})
		}
	}
	return out, nil
}

// preferConv orders conversations sharing a Codex thread: active before
// parked before the rest, then the newest.
func (c *converter) preferConv(a, b int64) bool {
	ra, rb := c.convRank(a), c.convRank(b)
	if ra != rb {
		return ra < rb
	}
	return a > b
}

func (c *converter) convRank(conv int64) int {
	si := c.sessionOfConv(conv)
	if si == nil {
		return 9
	}
	switch si.state {
	case store.ConvActive:
		return 0
	case store.ConvParked:
		return 1
	case store.ConvClosed:
		return 3
	}
	return 2
}

// preferSourceConv picks the conversation a rollout is mirrored into: the
// one whose thread is the file's thread, then preferConv.
func (c *converter) preferSourceConv(a, b int64, thread string) bool {
	ma, mb := c.threadOf(a) == thread, c.threadOf(b) == thread
	if ma != mb {
		return ma
	}
	return c.preferConv(a, b)
}

func (c *converter) threadOf(conv int64) string {
	if si := c.sessionOfConv(conv); si != nil {
		return si.s.CodexThreadID
	}
	return ""
}

func (c *converter) putSource(src store.TranscriptSource) error {
	_, err := c.im.Source(src)
	if err == nil {
		c.rep.E("transcript sources").Imported++
	}
	return err
}

// sessionMeta is the payload of a rollout's first record.
type sessionMeta struct {
	ID         string          `json:"id"`
	Originator string          `json:"originator"`
	Source     json.RawMessage `json:"source"`
}

func (m sessionMeta) subagentOrExec() bool {
	src := strings.TrimSpace(string(m.Source))
	if strings.HasPrefix(src, "{") {
		var obj map[string]json.RawMessage
		if json.Unmarshal(m.Source, &obj) == nil {
			if _, ok := obj["subagent"]; ok {
				return true
			}
		}
	}
	var s string
	_ = json.Unmarshal(m.Source, &s)
	return s == "exec" || strings.Contains(m.Originator, "exec")
}

// readSessionMeta decodes only the first record of a rollout (read-only).
func readSessionMeta(path string) (sessionMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return sessionMeta{}, err
	}
	defer f.Close()
	dec := json.NewDecoder(bufio.NewReader(io.LimitReader(f, 64<<20)))
	var rec struct {
		Type    string      `json:"type"`
		Payload sessionMeta `json:"payload"`
	}
	if err := dec.Decode(&rec); err != nil {
		return sessionMeta{}, err
	}
	if rec.Type != "session_meta" {
		return sessionMeta{}, errNotMeta
	}
	return rec.Payload, nil
}

var errNotMeta = &fs.PathError{Op: "read", Path: "session_meta", Err: fs.ErrInvalid}

// legacyResume is the position rule when no proof scan ran (file missing, or
// no rollout access): the furthest checkpoint or anchor cutoff, else the
// watcher position, else EOF.
func (c *converter) legacyResume(p *pathInfo, src *store.TranscriptSource, size int64, why string) {
	b := c.rep.Buckets
	cand := offsetCand{rank: 99}
	sort.SliceStable(p.cands, func(i, j int) bool {
		if p.cands[i].rank != p.cands[j].rank {
			return p.cands[i].rank < p.cands[j].rank
		}
		return p.cands[i].offset > p.cands[j].offset
	})
	if len(p.cands) > 0 {
		cand = p.cands[0]
	}
	switch {
	case cand.rank < 99:
		src.Offset, src.Line = cand.offset, cand.line
	case size > 0:
		cand.src = "EOF (no recorded position)"
		src.Offset = size
	default:
		cand.src = "none"
	}
	b["linked "+src.State+" ("+why+"): offset from "+cand.src]++
}
