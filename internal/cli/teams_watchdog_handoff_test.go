package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLegacyTeamsWatchdogHandoff(t *testing.T) {
	for _, tc := range []struct {
		name, old              string
		main, wd               int
		change, fail, wrong    bool
		wantRestart, wantError bool
	}{
		{name: "legacy", old: "v0.1.20", main: 42, wd: 43, wantRestart: true},
		{name: "rc59", old: "v0.1.22-rc.59", main: 42, wd: 43, wantRestart: true},
		{name: "modern", old: "v0.1.22-rc.87", main: 42, wd: 43},
		{name: "foreign main", old: "v0.1.20", main: 99, wd: 43},
		{name: "stopped", old: "v0.1.20", main: 42},
		{name: "foreign installation", old: "foreign", main: 42, wd: 43},
		{name: "concurrent activation", old: "v0.1.20", main: 42, wd: 43, change: true, wantError: true},
		{name: "restart denied", old: "v0.1.20", main: 42, wd: 43, fail: true, wantRestart: true, wantError: true},
		{name: "wrong executable", old: "v0.1.20", main: 42, wd: 43, wrong: true, wantRestart: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			restarted, reads := false, 0
			run := func(_ context.Context, args ...string) ([]byte, error) {
				if args[0] == "restart" {
					if len(args) != 2 || args[1] != teamsServiceWatchdogUnitName {
						t.Fatalf("unsafe restart: %v", args)
					}
					restarted = true
					if tc.fail {
						return nil, errors.New("access denied")
					}
					return nil, nil
				}
				if args[0] != "show" {
					t.Fatalf("unexpected command: %v", args)
				}
				n := tc.wd
				if args[len(args)-1] == teamsServiceUnitName {
					n = tc.main
				}
				return []byte(fmt.Sprint(n)), nil
			}
			link := func(string) (string, error) {
				v := tc.old
				if restarted && !tc.wrong {
					v = "v0.1.22-rc.88"
				}
				if v == "foreign" {
					return filepath.Join(root, "other", "cxp"), nil
				}
				return filepath.Join(root, "versions", v, "cxp"), nil
			}
			active := func(string) (string, error) {
				reads++
				if reads > 1 && tc.change {
					return "v0.1.22-rc.89", nil
				}
				return "v0.1.22-rc.88", nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := handoffLegacyTeamsWatchdog(ctx, root, "0.1.22-rc.88 (commit) date", 42, run, link, active)
			if restarted != tc.wantRestart || (err != nil) != tc.wantError {
				t.Fatalf("restart=%v error=%v", restarted, err)
			}
			if err != nil && strings.Contains(err.Error(), "unsafe") {
				t.Fatal(err)
			}
		})
	}
}
