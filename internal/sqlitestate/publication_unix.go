//go:build !windows

package sqlitestate

import "os"

func syncPublicationDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
