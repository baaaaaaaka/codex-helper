package lease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const mountinfoFixture = `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
40 22 0:35 / /home rw,relatime shared:20 - nfs4 srv:/home rw,vers=4.2,rsize=1048576,hard,proto=tcp,timeo=600,local_lock=none
41 40 0:36 / /home/u/nocto\040dir rw,relatime shared:21 - nfs srv:/x rw,vers=3,nocto,hard,proto=tcp,local_lock=all
42 22 0:37 / /mnt/smb rw,relatime shared:22 - cifs //srv/s rw,vers=3.1.1,cache=loose,uid=0
43 22 0:38 / /mnt/smb2 rw,relatime shared:23 - cifs //srv/t rw,vers=3.1.1,cache=strict,uid=0
44 22 0:39 / /mnt/v3 rw,relatime,nocto shared:24 - nfs srv:/v3 rw,vers=3,hard
`

func TestParseMountinfoAndUnsafeOptions(t *testing.T) {
	for _, tc := range []struct {
		path, point, fstype string
		unsafe              bool
	}{
		{"/home/u/.local/state", "/home", "nfs4", false},
		{"/home/u/nocto dir/state", "/home/u/nocto dir", "nfs", true},
		{"/home/u/nocto dirx", "/home", "nfs4", false},
		{"/var/tmp", "/", "ext4", false},
		{"/mnt/smb/state", "/mnt/smb", "cifs", true},
		{"/mnt/smb2/state", "/mnt/smb2", "cifs", false},
		{"/mnt/v3", "/mnt/v3", "nfs", true}, // nocto among the per-mount options
	} {
		m, ok := parseMountinfo(mountinfoFixture, tc.path)
		if !ok || m.Point != tc.point || m.FSType != tc.fstype {
			t.Fatalf("%s: got %+v ok=%v, want %s on %s", tc.path, m, ok, tc.fstype, tc.point)
		}
		if got := unsafeMount(m) != ""; got != tc.unsafe {
			t.Fatalf("%s: unsafe=%v (%q), want %v", tc.path, got, unsafeMount(m), tc.unsafe)
		}
	}
}

func noctoMountinfo(dir string) func() (string, error) {
	return func() (string, error) {
		return fmt.Sprintf("22 1 8:1 / / rw - ext4 /dev/sda1 rw\n40 22 0:35 / %s rw,relatime - nfs4 srv:/h rw,vers=4.2,nocto,hard\n",
			strings.ReplaceAll(dir, " ", `\040`)), nil
	}
}

// A host whose shared home is mounted with nocto never claims the lease:
// a foreground start fails with ErrUnsafeMount and a service standby says
// why it waits, and claims once the mount is fixed.
func TestNoctoMountRefusesTheLease(t *testing.T) {
	e := newEnv(t)
	dir, err := filepath.EvalSymlinks(e.dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := e.cfg(ident("a", 1), false)
	cfg.MountInfo = noctoMountinfo(dir)
	_, err = e.acquire(cfg)
	if !errors.Is(err, ErrUnsafeMount) || !strings.Contains(err.Error(), "nocto") {
		t.Fatalf("foreground: %v", err)
	}
	if _, err := os.Stat(Path(e.anchor)); !os.IsNotExist(err) {
		t.Fatal("a refused host must not create the lease file")
	}

	cfg = e.cfg(ident("a", 1), true)
	fixed := false
	inner := noctoMountinfo(dir)
	cfg.MountInfo = func() (string, error) {
		if fixed {
			return "22 1 8:1 / / rw - ext4 /dev/sda1 rw\n", nil
		}
		return inner()
	}
	var got []Standby
	cfg.Report = func(s Standby) { got = append(got, s) }
	n := 0
	cfg.Sleep = func(c context.Context, d time.Duration) error {
		if n++; n == 5 {
			if _, err := os.Stat(Path(e.anchor)); !os.IsNotExist(err) {
				t.Error("claimed on a nocto mount")
			}
			fixed = true
		}
		return e.clock.Sleep(c, d)
	}
	h, err := e.acquire(cfg)
	if err != nil || h == nil {
		t.Fatalf("after remount: %v", err)
	}
	if len(got) == 0 || got[0].Reason != ReasonError || !errors.Is(got[0].Err, ErrUnsafeMount) {
		t.Fatalf("reports: %+v", got)
	}
}
