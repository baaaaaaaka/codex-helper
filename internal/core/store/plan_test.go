package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// hotFiles hold the runtime API. Every SQL statement in them is checked.
// vacuum.go (maintenance) and import.go (one-shot converter) are not hot
// paths.
var hotFiles = []string{"store.go", "owner.go", "conversations.go", "receive.go", "turns.go", "outbox.go", "transcripts.go"}

// fragmentCompletions are representative full statements for SQL that the
// code assembles from fragments (convUpdate, turnMove, outMove).
var fragmentCompletions = map[string]string{
	"UPDATE conversations SET ": `UPDATE conversations SET state = 'parked', parked_at = ?, updated_at = ? WHERE id = ? AND state IN (?)`,
	"UPDATE turns SET ":         `UPDATE turns SET state = 'starting', thread_id = ? WHERE id = ? AND state IN (?,?,?)`,
	"UPDATE outbox SET ":        `UPDATE outbox SET state = 'sent', ext_msg_id = ? WHERE id = ? AND state IN (?,?)`,
}

// fullLoads are statements that intentionally read a whole hot table: they
// run once at startup to build the in-memory stat index.
var fullLoads = map[string]bool{
	`SELECT ` + sourceCols + ` FROM transcript_sources ORDER BY id`: true,
}

// sqlStatements extracts the SQL string expressions from the hot files,
// resolving the package's string constants.
func sqlStatements(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	consts := map[string]string{}
	var files []*ast.File
	names, _ := filepath.Glob("*.go")
	for _, n := range names {
		if strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		mustT(t, err)
		files = append(files, f)
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || g.Tok != token.CONST {
				continue
			}
			for _, sp := range g.Specs {
				vs := sp.(*ast.ValueSpec)
				for i, id := range vs.Names {
					if i < len(vs.Values) {
						if s, ok := evalStr(vs.Values[i], consts); ok {
							consts[id.Name] = s
						}
					}
				}
			}
		}
	}
	hot := map[string]bool{}
	for _, n := range hotFiles {
		hot[n] = true
	}
	kw := regexp.MustCompile(`^\s*(SELECT|INSERT|UPDATE|DELETE)\b`)
	seen := map[string]bool{}
	var out []string
	for _, f := range files {
		if !hot[filepath.Base(fset.File(f.Pos()).Name())] {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			e, ok := n.(ast.Expr)
			if !ok {
				return true
			}
			if _, isBin := e.(*ast.BinaryExpr); !isBin {
				if _, isLit := e.(*ast.BasicLit); !isLit {
					return true
				}
			}
			s, ok := evalStr(e, consts)
			if !ok || !kw.MatchString(s) {
				return true
			}
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
			return false
		})
	}
	sort.Strings(out)
	return out
}

func evalStr(e ast.Expr, consts map[string]string) (string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(v.Value)
		return s, err == nil
	case *ast.Ident:
		s, ok := consts[v.Name]
		return s, ok
	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			return "", false
		}
		a, ok1 := evalStr(v.X, consts)
		b, ok2 := evalStr(v.Y, consts)
		return a + b, ok1 && ok2
	case *ast.ParenExpr:
		return evalStr(v.X, consts)
	}
	return "", false
}

// hotTables must never be fully scanned by a hot statement. A scan of one of
// the partial open-work indexes is allowed: its size is bounded by open work,
// not by history.
var (
	hotTables    = regexp.MustCompile(`^SCAN (inbound|outbox|turns|transcript_sources)\b`)
	partialIndex = regexp.MustCompile(`USING (COVERING )?INDEX (outbox_pending|turns_open|inbound_open)\b`)
)

// TestHotQueriesUseIndexes runs EXPLAIN QUERY PLAN on every SQL statement of
// the runtime API and fails on a full scan of a history-sized table.
func TestHotQueriesUseIndexes(t *testing.T) {
	s, _ := openTest(t)
	checkHotPlans(t, s)
}

// checkHotPlans EXPLAINs every hot statement against s and fails on a SCAN
// of inbound, outbox, turns or transcript_sources that is not a partial
// open-work index.
func checkHotPlans(t *testing.T, s *Store) {
	t.Helper()
	if _, err := os.Stat("schema.go"); err != nil {
		t.Skip("source not available")
	}
	stmts := sqlStatements(t)
	if len(stmts) < 40 {
		t.Fatalf("only %d statements extracted", len(stmts))
	}
	checked := 0
	for _, q := range stmts {
		if c, ok := fragmentCompletions[q]; ok {
			q = c
		}
		if fullLoads[q] {
			continue
		}
		rows, err := s.w.Query(`EXPLAIN QUERY PLAN `+q, make([]any, numParams(q))...)
		if err != nil {
			t.Errorf("explain %q: %v", firstLine(q), err)
			continue
		}
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			mustT(t, rows.Scan(&id, &parent, &unused, &detail))
			plan = append(plan, detail)
			if hotTables.MatchString(detail) && !partialIndex.MatchString(detail) {
				t.Errorf("full scan in hot query:\n  %s\n  plan: %s", q, detail)
			}
		}
		rows.Close()
		checked++
		if testing.Verbose() {
			t.Logf("%s\n    %s", firstLine(q), strings.Join(plan, " | "))
		}
	}
	t.Logf("checked %d statements", checked)
}

func firstLine(q string) string {
	if i := strings.IndexByte(q, '\n'); i >= 0 {
		return q[:i]
	}
	return q
}

var numbered = regexp.MustCompile(`\?(\d+)`)

// numParams counts the bind parameters of q (all NULL for EXPLAIN).
func numParams(q string) int {
	if m := numbered.FindAllStringSubmatch(q, -1); len(m) > 0 {
		n := 0
		for _, x := range m {
			if v, _ := strconv.Atoi(x[1]); v > n {
				n = v
			}
		}
		return n
	}
	return strings.Count(q, "?")
}
