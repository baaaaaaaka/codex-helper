//go:build !windows

package teams

import "os"

func defaultDurableReplaceFile(src string, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	_ = syncParentDir(dst)
	return nil
}
