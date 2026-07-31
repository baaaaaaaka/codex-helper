//go:build linux

package teams

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func runtimeStoreOwnerExitConfirmed(owner teamstore.OwnerMetadata) bool {
	if owner.PID <= 0 || !teamstore.OwnerAppearsLocal(owner) {
		return false
	}
	_, err := os.Stat(filepath.Join("/proc", strconv.Itoa(owner.PID)))
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return false
	}
	return !runtimeStoreNestedPIDNamespace()
}

func runtimeStoreNestedPIDNamespace() bool {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return true
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "NSpid:") {
			continue
		}
		return len(strings.Fields(strings.TrimPrefix(line, "NSpid:"))) > 1
	}
	return true
}
