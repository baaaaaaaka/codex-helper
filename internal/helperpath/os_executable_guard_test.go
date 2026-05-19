package helperpath

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestOSExecutableUsageIsCentralized(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	allowedDir := filepath.ToSlash(filepath.Join("internal", "helperpath"))
	var violations []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			switch name {
			case ".git", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, allowedDir+"/") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		osAliases := importedOSAliases(file)
		if len(osAliases) == 0 {
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			sel, ok := node.(*ast.SelectorExpr)
			if !ok || sel.Sel == nil || sel.Sel.Name != "Executable" {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || !osAliases[ident.Name] {
				return true
			}
			pos := fset.Position(sel.Pos())
			violations = append(violations, filepath.ToSlash(rel)+":"+strconv.Itoa(pos.Line))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) > 0 {
		t.Fatalf("os.Executable must be wrapped by helperpath.RawExecutable; direct usages: %s", strings.Join(violations, ", "))
	}
}

func importedOSAliases(file *ast.File) map[string]bool {
	out := make(map[string]bool)
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != "os" {
			continue
		}
		if spec.Name != nil {
			if spec.Name.Name != "_" && spec.Name.Name != "." {
				out[spec.Name.Name] = true
			}
			continue
		}
		out["os"] = true
	}
	return out
}
