package lease

import (
	"os"
	"strconv"
	"strings"
)

// Plan says how a new generation gets its store.
type Plan struct {
	Gen int64
	// Why the generation could be claimed: cold (no owner yet), released,
	// dead (the owner's process is gone) or stale (it stopped renewing).
	Why string
	// From is the generation that served last, if any.
	From    Record
	FromGen int64
	// Src is the store to start from; DB is the store this generation
	// serves. They are the same file when InPlace.
	Src, DB string
	InPlace bool
	// Quiet: wait Timing.Quiet after the claim before copying the store
	// and serving, because the previous owner, or a claimer between it and
	// this generation, may still act until its lease runs out.
	Quiet bool
	// Gap: a claim between From and this generation never finished.
	Gap bool
}

// CrossHost reports whether this generation takes over from another host.
func (pl Plan) CrossHost(self Ident) bool {
	return pl.FromGen != 0 && !strings.EqualFold(pl.From.Host, self.Host)
}

// plan decides how generation gen gets its store. Each store file is
// opened with SQLite by processes of one host only: the store is used in
// place only by a successor on the same host whose predecessor released it
// or is dead, with no unfinished claim in between. Every other successor
// copies the store into a file of its own.
func plan(cfg *Config, p paths, v view, gen int64, why string) Plan {
	r, rg, gap := v.current(gen)
	pl := Plan{Gen: gen, Why: why, From: r, FromGen: rg, Gap: gap}
	if rg == 0 {
		pl.Src = coldSource(p)
		pl.InPlace = !gap
		pl.Quiet = gap
	} else {
		pl.Src = p.dbPath(r.DB)
		here := cfg.Self.sameMachine(r) || cfg.Self.rebooted(r)
		dead := cfg.dead(r)
		pl.InPlace = !gap && here && (r.State == Released || dead)
		pl.Quiet = gap || (r.State == Active && !dead)
	}
	if pl.InPlace {
		pl.DB = pl.Src
	} else {
		pl.DB = p.genDB(gen)
	}
	return pl
}

// coldSource is the store to start from when no generation has served:
// the newest generation store if any is left (the lease file was lost),
// else the anchor.
func coldSource(p paths) string {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return p.anchor
	}
	best, path := int64(0), p.anchor
	for _, e := range entries {
		if g, ok := genOf(p, e.Name()); ok && g > best {
			best, path = g, p.dbPath(e.Name())
		}
	}
	return path
}

// genOf parses a generation store name <base>.g<N>.sqlite.
func genOf(p paths, name string) (int64, bool) {
	rest, ok := strings.CutPrefix(name, p.base+".g")
	if !ok {
		return 0, false
	}
	num, ok := strings.CutSuffix(rest, ".sqlite")
	if !ok {
		return 0, false
	}
	g, err := strconv.ParseInt(num, 10, 64)
	return g, err == nil && g > 0
}
