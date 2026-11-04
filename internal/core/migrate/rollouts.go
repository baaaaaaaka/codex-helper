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
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// DiscoveryWindow: unlinked interactive rollouts modified within this window
// get no transcript_sources row, so runtime discovery creates their chat.
const DiscoveryWindow = 120 * 24 * time.Hour

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
	conv           int64
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
	threadConv := map[string]int64{}
	for _, label := range sortedKeys(c.sessions) {
		si := c.sessions[label]
		if si.s.CodexThreadID == "" || si.conv == 0 || si.state == "control" {
			continue
		}
		if cur, ok := threadConv[si.s.CodexThreadID]; !ok || c.preferConv(si.conv, cur) {
			threadConv[si.s.CodexThreadID] = si.conv
		}
	}
	ce := c.rep.E("import_checkpoints")
	rewriteFlags, watchFlags := 0, 0
	err := c.l.each("import_checkpoints", func(r row) error {
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
		if bool(cp.LastOffsetKnown) && cp.LastOffset > 0 {
			p.cands = append(p.cands, offsetCand{rank: 1, src: "checkpoint", offset: cp.LastOffset, line: cp.LastSourceLine})
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
		if err := c.importSource(paths[k]); err != nil {
			return err
		}
	}
	return nil
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
	var conv int64
	for cv := range p.convs {
		if conv == 0 || c.preferSourceConv(cv, conv, p.thread) {
			conv = cv
		}
	}
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
		case c.now.Sub(time.UnixMilli(mtime)) <= DiscoveryWindow:
			b["unlinked interactive, mtime within 120 days -> no row (discovery)"]++
			return nil
		default:
			src.State, src.Offset = store.SrcBaseline, size
			b["unlinked interactive, older than 120 days -> baseline at EOF"]++
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
	// Within a rank the furthest position wins: every rank-1 candidate (a
	// checkpoint's known offset, an execution anchor's cutoff) is a
	// delivered position, so resuming before the furthest one re-mirrors.
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
	if p.anchor != nil {
		switch {
		case p.anchor.CutoffOffset > 0 && cand.offset == p.anchor.CutoffOffset:
			b["anchor (0 synthetic turns): at cutoff_offset"]++
		case p.anchor.CutoffOffset > 0:
			b["anchor (0 synthetic turns): past cutoff_offset, at a later delivered "+cand.src+" offset"]++
		default:
			b["anchor (0 synthetic turns): no cutoff_offset, at "+cand.src+" offset"]++
		}
	}
	if p.rewriteBlocked {
		if cand.rank <= 2 {
			b["rewrite-blocked (flag ignored): resumed at stored offset"]++
		} else {
			b["rewrite-blocked (flag ignored): no stored offset -> "+cand.src]++
		}
	}
	if fst.ok && src.Offset > fst.size {
		b["linked: recorded offset beyond EOF (runtime rewrite handling)"]++
	}
	b["linked "+src.State+": offset from "+cand.src]++
	if p.progress && src.Offset == 0 {
		c.rep.note("source %s had progress but no usable offset", p.path)
	}
	return c.putSource(src)
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
