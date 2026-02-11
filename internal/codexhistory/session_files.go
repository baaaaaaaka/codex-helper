package codexhistory

import (
	"io/fs"
	"path/filepath"
	"strings"
)

// collectSessionFiles walks sessionsDir (e.g. ~/.codex/sessions/) recursively
// and returns all .jsonl file paths.
func collectSessionFiles(sessionsDir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(sessionsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".jsonl") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
