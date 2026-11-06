// Package catalog checks the behaviour catalog (testdata/BEHAVIOR-CATALOG.md)
// against the black-box E2E scenarios. It reads only the catalog and the
// scenario sources (go/parser); it imports nothing of the product.
//
// Default run (always on): the catalog parses; ids are unique and complete
// (106 SPEC F-entries, R-1..R-50); statuses and e2e cells are well-formed;
// every scenario a row cites exists; every scenario Guarantees tag is a
// catalog id or a listed alias; and every row a scenario claims cites that
// scenario (so the catalog cannot fall behind the suite). It logs the gap
// counts and the keep rows without a scenario.
//
// Strict run: also fails while any keep row has e2e NONE (neither a
// scenario nor an "exempt: <reason>"). Switch it on with
//
//	CXP_CATALOG_STRICT=1 go test ./internal/e2e/catalog/
//	go test ./internal/e2e/catalog/ -args -strict
package catalog

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var strictFlag = flag.Bool("strict", false, "fail while any keep catalog entry has no E2E scenario and no exemption (or set CXP_CATALOG_STRICT=1)")

const (
	defaultCatalog = "testdata/BEHAVIOR-CATALOG.md"
	scenariosDir   = "../scenarios"
	wantF          = 106 // SPEC §1 F-entries
	wantR          = 50  // R-1..R-49 plus R-50 (owner 决定-4)
)

var (
	idRE     = regexp.MustCompile(`^(F-[A-Z]+-[0-9]+|R-[0-9]+)([a-z])?$`)
	parenRE  = regexp.MustCompile(`\([^()]*\)`)
	statuses = map[string]bool{"keep": true, "removed-by-owner": true, "ask-owner": true}
)

type entry struct {
	id, status, statusText, impl, e2e string
	line                              int
}

func strict() bool { return *strictFlag || os.Getenv("CXP_CATALOG_STRICT") == "1" }

func catalogPath() string {
	if p := os.Getenv("CXP_CATALOG_FILE"); p != "" {
		return p
	}
	return defaultCatalog
}

// section returns the table rows (cells, line number) between the markers.
func section(t *testing.T, lines []string, name string, cols int) ([][]string, []int) {
	t.Helper()
	begin, end := "<!-- "+name+":begin -->", "<!-- "+name+":end -->"
	var rows [][]string
	var at []int
	in, seen := false, false
	for i, l := range lines {
		l = strings.TrimSpace(l)
		switch {
		case l == begin:
			in, seen = true, true
			continue
		case l == end:
			in = false
			continue
		}
		if !in || !strings.HasPrefix(l, "|") {
			continue
		}
		cells := strings.Split(strings.Trim(l, "|"), "|")
		for j := range cells {
			cells[j] = strings.TrimSpace(cells[j])
		}
		if len(cells) != cols {
			t.Errorf("catalog line %d: %d cells, want %d: %s", i+1, len(cells), cols, l)
			continue
		}
		if cells[0] == "id" || cells[0] == "tag" || strings.HasPrefix(cells[0], "---") {
			continue // header and separator
		}
		rows = append(rows, cells)
		at = append(at, i+1)
	}
	if !seen {
		t.Fatalf("catalog has no %q section", begin)
	}
	return rows, at
}

func readCatalog(t *testing.T) ([]entry, map[string]string) {
	t.Helper()
	data, err := os.ReadFile(catalogPath())
	if err != nil {
		t.Fatalf("read catalog: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	rows, at := section(t, lines, "catalog", 5)
	var es []entry
	for i, c := range rows {
		st := c[1]
		if k := strings.IndexAny(st, " ("); k >= 0 {
			st = st[:k]
		}
		es = append(es, entry{id: c[0], status: st, statusText: c[1], impl: c[2], e2e: c[3], line: at[i]})
	}
	arows, _ := section(t, lines, "aliases", 3)
	aliases := map[string]string{}
	for _, c := range arows {
		if _, dup := aliases[c[0]]; dup {
			t.Errorf("alias %q listed twice", c[0])
		}
		aliases[c[0]] = c[1]
	}
	return es, aliases
}

// scenarioSet holds the scenario ids and, per Guarantees tag, the ids of the
// scenarios that claim it.
type scenarioSet struct {
	ids    map[string]string   // scenario id -> file
	claims map[string][]string // tag -> scenario ids
}

func readScenarios(t *testing.T) scenarioSet {
	t.Helper()
	s := scenarioSet{ids: map[string]string{}, claims: map[string][]string{}}
	fset := token.NewFileSet()
	err := filepath.WalkDir(scenariosDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(scenariosDir, path)
		// Table-driven scenarios pass a variable id; resolve it from the
		// `id: "..."` fields of the file's case literals.
		var tableIDs []string
		ast.Inspect(f, func(n ast.Node) bool {
			if kv, ok := n.(*ast.KeyValueExpr); ok {
				if k, ok := kv.Key.(*ast.Ident); ok && (k.Name == "id" || k.Name == "ID") {
					if v, ok := stringLit(kv.Value); ok {
						tableIDs = append(tableIDs, v)
					}
				}
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isNewScenario(call.Fun) || len(call.Args) < 3 {
				return true
			}
			var ids []string
			if v, ok := stringLit(call.Args[1]); ok {
				ids = []string{v}
			} else {
				ids = tableIDs
			}
			if len(ids) == 0 {
				t.Errorf("%s: NewScenario with an id the catalog test cannot resolve", fset.Position(call.Pos()))
				return true
			}
			var tags []string
			for _, a := range call.Args[2:] {
				tags = append(tags, guarantees(a)...)
			}
			for _, id := range ids {
				if prev, dup := s.ids[id]; dup && prev != rel {
					t.Errorf("scenario id %s declared in %s and %s", id, prev, rel)
				}
				s.ids[id] = rel
				for _, tag := range tags {
					s.claims[tag] = appendUnique(s.claims[tag], id)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk scenarios: %v", err)
	}
	if len(s.ids) < 50 {
		t.Fatalf("only %d scenarios found under %s; the walk is broken", len(s.ids), scenariosDir)
	}
	return s
}

// guarantees extracts the Guarantees tags of a NewScenario options argument:
// the `Guarantees: []string{...}` field of an Options literal, or, for an
// options helper call such as prodOptions(title, []string{...}, args...), the
// first []string literal argument of that call. Other []string fields (Args)
// are not tags.
func guarantees(arg ast.Expr) []string {
	var tags []string
	ast.Inspect(arg, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.KeyValueExpr:
			if k, ok := n.Key.(*ast.Ident); ok && k.Name == "Guarantees" {
				tags = append(tags, stringSlice(n.Value)...)
			}
			return false
		case *ast.CallExpr:
			for _, a := range n.Args {
				if s := stringSlice(a); s != nil {
					tags = append(tags, s...)
					break
				}
			}
		}
		return true
	})
	return tags
}

// stringSlice returns the string literals of a []string{...} literal, or nil.
func stringSlice(e ast.Expr) []string {
	cl, ok := e.(*ast.CompositeLit)
	if !ok {
		return nil
	}
	at, ok := cl.Type.(*ast.ArrayType)
	if !ok {
		return nil
	}
	if el, ok := at.Elt.(*ast.Ident); !ok || el.Name != "string" {
		return nil
	}
	out := []string{}
	for _, x := range cl.Elts {
		if v, ok := stringLit(x); ok {
			out = append(out, v)
		}
	}
	return out
}

func isNewScenario(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name == "NewScenario"
	case *ast.Ident:
		return f.Name == "NewScenario"
	}
	return false
}

func stringLit(e ast.Expr) (string, bool) {
	bl, ok := e.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(bl.Value)
	return v, err == nil
}

func appendUnique(xs []string, x string) []string {
	for _, y := range xs {
		if y == x {
			return xs
		}
	}
	return append(xs, x)
}

// e2eKind classifies an e2e cell: "none", "na", "exempt" or "scenarios"
// (with the cited scenario patterns).
func e2eKind(cell string) (string, []string) {
	if strings.HasPrefix(cell, "exempt:") {
		return "exempt", nil
	}
	bare := cell
	for parenRE.MatchString(bare) {
		bare = parenRE.ReplaceAllString(bare, "")
	}
	bare = strings.TrimSpace(bare)
	switch bare {
	case "NONE":
		return "none", nil
	case "n/a":
		return "na", nil
	}
	var pats []string
	for _, p := range strings.Split(bare, ",") {
		if p = strings.TrimSpace(p); p != "" {
			pats = append(pats, p)
		}
	}
	return "scenarios", pats
}

func matches(pat, id string) bool {
	if strings.HasSuffix(pat, "*") {
		return strings.HasPrefix(id, strings.TrimSuffix(pat, "*"))
	}
	return pat == id
}

func implKind(impl string) string {
	s := strings.TrimLeft(impl, "* ")
	switch {
	case strings.HasPrefix(s, "MISSING"):
		return "missing"
	case strings.HasPrefix(s, "PARTIAL"):
		return "partial"
	case strings.HasPrefix(s, "n/a"):
		return "na"
	}
	return "implemented"
}

// target resolves a Guarantees tag to a catalog id ("-" = deliberately none).
func target(tag string, aliases map[string]string) (string, bool) {
	if m := idRE.FindStringSubmatch(tag); m != nil {
		return m[1], true
	}
	v, ok := aliases[tag]
	return v, ok
}

func TestBehaviorCatalog(t *testing.T) {
	es, aliases := readCatalog(t)
	sc := readScenarios(t)

	byID := map[string]*entry{}
	nF, nR := 0, 0
	for i := range es {
		e := &es[i]
		where := fmt.Sprintf("catalog line %d (%s)", e.line, e.id)
		if _, dup := byID[e.id]; dup {
			t.Errorf("%s: duplicate id", where)
		}
		byID[e.id] = e
		m := idRE.FindStringSubmatch(e.id)
		switch {
		case m == nil || m[2] != "":
			t.Errorf("%s: id is not an F-/R- catalog id", where)
		case strings.HasPrefix(e.id, "F-"):
			nF++
		default:
			nR++
		}
		if !statuses[e.status] {
			t.Errorf("%s: status %q is not keep, removed-by-owner or ask-owner", where, e.status)
		}
		if e.status == "removed-by-owner" && !strings.Contains(e.statusText, "(") {
			t.Errorf("%s: removed-by-owner must cite the owner decision in parentheses", where)
		}
		if e.status == "ask-owner" && !strings.Contains(e.statusText, "default") {
			t.Errorf("%s: ask-owner must state the default and whether the owner answered", where)
		}
		ik := implKind(e.impl)
		if e.impl == "" {
			t.Errorf("%s: empty implementation cell", where)
		}
		if (e.status == "removed-by-owner") != (ik == "na") {
			t.Errorf("%s: implementation is n/a exactly for removed-by-owner rows (status %s, impl %q)", where, e.status, e.impl)
		}
		kind, pats := e2eKind(e.e2e)
		if kind == "exempt" && len(strings.TrimSpace(strings.TrimPrefix(e.e2e, "exempt:"))) < 10 {
			t.Errorf("%s: exempt needs a reason", where)
		}
		if kind == "na" && e.status != "removed-by-owner" {
			t.Errorf("%s: e2e n/a is only for removed rows", where)
		}
		for _, p := range pats {
			found := false
			for id := range sc.ids {
				if matches(p, id) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: cites scenario %q, which does not exist under internal/e2e/scenarios", where, p)
			}
		}
	}
	if nF != wantF || nR != wantR {
		t.Errorf("catalog has %d F-entries and %d R-entries, want %d and %d", nF, nR, wantF, wantR)
	}
	for i := 1; i <= wantR; i++ {
		if byID[fmt.Sprintf("R-%d", i)] == nil {
			t.Errorf("catalog has no row R-%d", i)
		}
	}
	for tag, to := range aliases {
		if idRE.MatchString(tag) {
			t.Errorf("alias %q: catalog-id-shaped tags map to themselves; drop the alias", tag)
		}
		if to != "-" && byID[to] == nil {
			t.Errorf("alias %q -> %q: no such catalog row", tag, to)
		}
	}

	// Every Guarantees tag resolves, and every claimed row cites its scenarios.
	tags := make([]string, 0, len(sc.claims))
	for tag := range sc.claims {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		to, ok := target(tag, aliases)
		if !ok {
			t.Errorf("scenario tag %q (in %s) is neither a catalog id nor a listed alias; add it to the aliases table", tag, strings.Join(sc.claims[tag], ", "))
			continue
		}
		if to == "-" {
			continue
		}
		e := byID[to]
		if e == nil {
			t.Errorf("scenario tag %q (in %s) names %s, which is not in the catalog", tag, strings.Join(sc.claims[tag], ", "), to)
			continue
		}
		_, pats := e2eKind(e.e2e)
		for _, id := range sc.claims[tag] {
			cited := false
			for _, p := range pats {
				if matches(p, id) {
					cited = true
					break
				}
			}
			if !cited {
				t.Errorf("catalog line %d (%s): scenario %s claims it (tag %q) but the e2e cell %q does not cite it", e.line, e.id, id, tag, e.e2e)
			}
		}
	}

	// Gap summary.
	var keep, impl, missing, partial, exempt, removed, ask int
	var uncovered []string
	for _, e := range es {
		switch e.status {
		case "removed-by-owner":
			removed++
			continue
		case "ask-owner":
			ask++
			continue
		}
		keep++
		switch implKind(e.impl) {
		case "missing":
			missing++
		case "partial":
			partial++
		default:
			impl++
		}
		switch k, _ := e2eKind(e.e2e); k {
		case "exempt":
			exempt++
		case "none":
			uncovered = append(uncovered, e.id)
		}
	}
	t.Logf("catalog: %d rows; keep %d (implemented %d, partial %d, missing %d; no scenario %d, exempt %d); removed-by-owner %d; ask-owner %d; scenarios %d",
		len(es), keep, impl, partial, missing, len(uncovered), exempt, removed, ask, len(sc.ids))
	if len(uncovered) == 0 {
		return
	}
	msg := fmt.Sprintf("%d keep entries have no E2E scenario and no exemption: %s", len(uncovered), strings.Join(uncovered, ", "))
	if strict() {
		t.Error(msg)
	} else {
		t.Log(msg + " (strict mode off: set CXP_CATALOG_STRICT=1 or pass -args -strict to fail on these)")
	}
}
