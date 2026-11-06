package e2e

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/baaaaaaaka/codex-helper/"

// allowedInternal are the module packages E2E code may import: the harness
// and the fakes. Everything else of the module is the system under test,
// reached only through the built cxp binary.
var allowedInternal = []string{
	"internal/e2e/harness",
	"internal/e2e/steps",
	"internal/e2e/sut",
	"internal/e2e/fakegraph",
	"internal/e2e/fakecodex",
	"internal/e2e/realcodex",
	"internal/e2e/rollout",
}

func allowed(path string) bool {
	if !strings.HasPrefix(path, module) {
		return true // standard library or third party
	}
	rel := strings.TrimPrefix(path, module)
	for _, a := range allowedInternal {
		if rel == a || strings.HasPrefix(rel, a+"/") {
			return true
		}
	}
	return false
}

// fixture reports whether a unit test outside internal/e2e may import p.
func fixture(p string) bool {
	switch strings.TrimPrefix(p, module) {
	case "internal/e2e/fakegraph", "internal/e2e/fakecodex", "internal/e2e/rollout":
		return true
	}
	return false
}

// TestBlackBoxBoundary keeps the E2E suite black-box: no file under
// internal/e2e imports a module package outside the harness and the fakes,
// and nothing outside internal/e2e imports the E2E packages (no test hook
// can reach into the product; unit tests may use the fakes and the
// rollout writer as fixtures).
func TestBlackBoxBoundary(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s", root)
	}
	e2eDir := filepath.Join(root, "internal", "e2e")
	fset := token.NewFileSet()
	checked := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		inE2E := strings.HasPrefix(path, e2eDir+string(filepath.Separator))
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			switch {
			case inE2E && !allowed(p):
				t.Errorf("%s imports %s: E2E code may import only the harness, the fakes, rollout, the standard library and third-party packages; observe cxp through its external interfaces instead", rel, p)
			case !inE2E && strings.HasPrefix(p, module+"internal/e2e"):
				// Unit tests may use the fakes and the rollout writer as
				// fixtures; the harness itself is never reachable from the
				// product, and product code imports nothing of internal/e2e.
				if strings.HasSuffix(path, "_test.go") && fixture(p) {
					continue
				}
				t.Errorf("%s imports %s: product code must not depend on the E2E harness", rel, p)
			}
		}
		checked++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 100 {
		t.Fatalf("only %d Go files checked; the walk is broken", checked)
	}
}
