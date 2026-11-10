package sqlitestate

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// These packages own persistence, not network/CLI JSON. A new runtime JSON
// import here needs an explicit boundary decision rather than silently putting
// a serialized object back in a SQL TEXT column. Wire encoders in delegation
// stay explicit; other application packages remain free to use network JSON.
func TestSQLiteOnlyPersistenceBoundary(t *testing.T) {
	for _, dir := range []string{"core/store", "core/delegate/delegation", "config", "updatestate"} {
		path := filepath.Join("..", filepath.FromSlash(dir))
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			migration := strings.HasPrefix(name, "migrate") || strings.Contains(name, "migration")
			wire := dir == "core/delegate/delegation" && (name == "protocol.go" || name == "registry.go")
			if migration || wire {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(path, name), nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range f.Imports {
				importPath, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					t.Fatal(err)
				}
				if importPath == "encoding/json" {
					t.Errorf("runtime persistence %s/%s imports JSON; put legacy decoding in the owning migration entry", dir, name)
				}
			}
		}
	}
}
