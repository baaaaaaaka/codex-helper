//go:build !windows

package codexhistory

import (
	"fmt"
	"os"
)

func defaultPersistentCacheWriterScopeID() (string, error) {
	return fmt.Sprintf("uid-%d", os.Geteuid()), nil
}
