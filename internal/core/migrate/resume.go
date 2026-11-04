package migrate

import "time"

// resume is the decided position of one scanned, tracked rollout.
type resume struct {
	off, line int64
	proof     int64 // end of the last record proven delivered (0: none)
	hasProof  bool
	via       string
	gap       [2]int // prompts, finals in [proof, max old position)
	backlog   [2]int // prompts, finals from off to the scan end
	old       int    // of the backlog: records older than DiscoveryWindow
}

// decideResume applies the proof rule (STORE-DESIGN §7.2) to a scanned
// rollout: P is the end of the last record a delivery ledger proves handled;
// the resume point is the furthest old position (checkpoint, anchor cutoff,
// watcher) that is a record end after P with no prompt or final the mirror
// would post in between, else P itself.
func (c *converter) decideResume(p *pathInfo, size int64) resume {
	sc := p.scan
	rs := resume{proof: p.scanFrom, line: p.scanLine}
	pr := c.proofs[p.path]
	if pr != nil && pr.endOff > 0 && p.scanFrom == pr.endOff {
		rs.hasProof = true
	}
	// transcript_ledger entries carry lines and record ids, not offsets.
	for ln, end := range sc.LineEnd {
		rs.hasProof = true
		if end > rs.proof {
			rs.proof, rs.line = end, ln
		}
	}
	for _, e := range sc.IDEnd {
		rs.hasProof = true
		if e[0] > rs.proof {
			rs.proof, rs.line = e[0], e[1]
		}
	}
	rs.off = rs.proof
	first := int64(-1) // start of the first mirrorable record at or after P
	for _, r := range sc.Recs {
		if r.Start >= rs.proof {
			first = r.Start
			break
		}
	}
	rs.via = "proof"
	if !rs.hasProof {
		rs.via = "no proof: start of file"
	}
	for _, x := range []struct {
		off  int64
		name string
	}{{p.ckpt, "checkpoint"}, {p.cutoff, "anchor cutoff"}, {p.watch, "history_watch"}} {
		ln, boundary := sc.Boundary[x.off]
		if x.off <= rs.off || x.off > size || !boundary || (first >= 0 && first < x.off) {
			continue
		}
		rs.off, rs.line, rs.via = x.off, ln, x.name+" (nothing to mirror between proof and it)"
	}
	top := max(p.ckpt, p.cutoff, p.watch)
	if top == 0 && !rs.hasProof {
		// The old core recorded nothing for this rollout: keep its EOF
		// baseline (records it never mirrored are not caught up).
		rs.off, rs.line, rs.via = size, 0, "EOF (no recorded position, no proof)"
		if sc.EndOff == size {
			rs.line = sc.EndLine
		}
	}
	for _, r := range sc.Recs {
		k := 0
		if r.Kind == "final" {
			k = 1
		}
		if r.Start >= rs.proof && r.Start < top {
			rs.gap[k]++
		}
		if r.Start >= rs.off {
			rs.backlog[k]++
			if r.TS > 0 && c.now.Sub(time.UnixMilli(r.TS)) > DiscoveryWindow {
				rs.old++
			}
		}
	}
	return rs
}

// reportResume records buckets, the per-source rows the owner asked for and
// the catch-up totals.
func (c *converter) reportResume(p *pathInfo, rs resume, size int64) {
	b := c.rep.Buckets
	top := max(p.ckpt, p.cutoff, p.watch)
	b["linked tracking: resume at "+rs.via]++
	switch {
	case top == 0:
	case rs.off == top:
		b["linked tracking: old furthest position confirmed"]++
	case rs.off < top:
		b["linked tracking: resume before the old furthest position (gap is mirrored)"]++
	}
	if p.anchor != nil {
		switch {
		case p.anchor.CutoffOffset > 0 && rs.off == p.anchor.CutoffOffset:
			b["anchor (0 synthetic turns): at cutoff_offset"]++
		case p.anchor.CutoffOffset > 0 && rs.off > p.anchor.CutoffOffset:
			b["anchor (0 synthetic turns): past cutoff_offset"]++
		case p.anchor.CutoffOffset > 0:
			b["anchor (0 synthetic turns): before cutoff_offset (gap is mirrored)"]++
		default:
			b["anchor (0 synthetic turns): no cutoff_offset"]++
		}
	}
	if rs.off == 0 && size > 0 {
		c.zeroNoProof++
	}
	if c.opt.CountBacklog {
		bl := &c.rep.Backlog
		bl.Counted = true
		bl.Sources++
		bl.Bytes += max(p.scan.EndOff-rs.off, 0)
		bl.Prompts += rs.backlog[0]
		bl.Finals += rs.backlog[1]
		bl.Old += rs.old
		bl.Gap += rs.gap[0] + rs.gap[1]
		if rs.off == size && max(p.ckpt, p.cutoff, p.watch) == 0 && !rs.hasProof {
			bl.NoPositionSources++
			bl.NoPositionRecords += len(p.scan.Recs)
		} else if !rs.hasProof {
			bl.NoProofSources++
			bl.NoProofPrompts += rs.backlog[0]
			bl.NoProofFinals += rs.backlog[1]
			bl.NoProofOld += rs.old
		}
	}
	var cases []string
	if max(p.ckpt, p.cutoff) > 0 && p.watch > max(p.ckpt, p.cutoff) {
		cases = append(cases, "watcher-ahead")
	}
	if p.rewriteBlocked && p.ckpt == 0 {
		cases = append(cases, "rewrite-blocked")
	}
	if !rs.hasProof && top > 0 {
		cases = append(cases, "no-proof")
	}
	if len(cases) == 0 && rs.gap[0]+rs.gap[1] > 0 {
		cases = append(cases, "proof-behind")
	}
	for _, cs := range cases {
		row := ResumeRow{Path: p.path, Conv: c.labelOf[p.conv], Case: cs, Size: size,
			Checkpoint: p.ckpt, Cutoff: p.cutoff, Watcher: p.watch, Proof: rs.proof, Resume: rs.off,
			GapPrompts: rs.gap[0], GapFinals: rs.gap[1], BacklogPrompts: -1}
		if c.opt.CountBacklog {
			row.BacklogPrompts, row.BacklogFinals, row.BacklogOld = rs.backlog[0], rs.backlog[1], rs.old
		}
		c.rep.Resume = append(c.rep.Resume, row)
	}
}
