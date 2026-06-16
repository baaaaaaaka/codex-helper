package update

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	for _, key := range []string{
		EnvInstallPath,
		EnvInstallDir,
		EnvRepo,
		EnvVersion,
	} {
		_ = os.Unsetenv(key)
	}
	os.Exit(m.Run())
}
