//go:build !windows

package skills

import (
	"fmt"
	"os"
)

func replaceSkillFile(tmp, path string) error {
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}
