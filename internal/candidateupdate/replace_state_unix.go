//go:build !windows

package candidateupdate

import "os"

func replaceStateFile(source string, target string) error {
	return os.Rename(source, target)
}
