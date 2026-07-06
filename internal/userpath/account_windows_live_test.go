//go:build windows

package userpath

import (
	"context"
	"os"
	"testing"
)

func TestWindowsAccountDefaultLiveTokenEnvironment(t *testing.T) {
	if os.Getenv("CODEX_HELPER_USER_PATH_WINDOWS_LIVE") != "1" {
		t.Skip("set CODEX_HELPER_USER_PATH_WINDOWS_LIVE=1 on a Windows account")
	}
	result, err := (DefaultResolver{}).Resolve(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Path == "" || result.Target.SID == "" || result.Adapter != "CreateEnvironmentBlock" {
		t.Fatalf("live Windows account environment = %#v", result)
	}
	if result.EntryCount == 0 || result.Fingerprint == "" {
		t.Fatalf("live Windows PATH metadata = %#v", result)
	}
}
