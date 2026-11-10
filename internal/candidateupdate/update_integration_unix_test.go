//go:build cxp_boot_e2e && !windows

package candidateupdate

import "testing"

func holdNativeCandidateEntry(t *testing.T, path string) func() { return func() {} }
