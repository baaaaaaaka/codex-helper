package cli

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func TestTeamsGraphRecoveryPollSnapshotCompatibility(t *testing.T) {
	dir := t.TempDir()
	path := teamsPollStampPath(dir)
	now := time.Now().Truncate(time.Millisecond)
	if err := sqlitestate.PublishSnapshot(path, nil, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE poll(id INTEGER PRIMARY KEY CHECK(id=1),host TEXT NOT NULL,pid INTEGER NOT NULL,version TEXT NOT NULL,at INTEGER NOT NULL)`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO poll VALUES(1,'host',1,'old',?)`, now.UnixMilli())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok := readTeamsPollStamp(dir); !ok || got.At != now.UnixMilli() || got.Generation != 0 || got.FailureSince != 0 {
		t.Fatalf("legacy snapshot: %+v, readable=%t", got, ok)
	}

	owner := lease.Record{Host: "host", Gen: 2, PID: os.Getpid(), State: lease.Active, Wall: now.UnixMilli()}
	view := teamsStoreView{Anchor: filepath.Join(dir, "core.sqlite"), Info: lease.Info{HeadOK: true, HeadGen: owner.Gen, Head: owner}}
	var read time.Time
	since := now.Add(-2 * time.Minute)
	stamper := teamsPollStamper{dir: dir, host: owner.Host, gen: owner.Gen, lastRead: func() time.Time { return read }, failedSince: func(time.Time) time.Time { return since }}
	stamper.tick(now)
	if line := teamsLastPollLine(view, now); !strings.Contains(line, "Last poll: none recorded\nTeams Graph: degraded") {
		t.Fatalf("first-start outage: %q", line)
	}
	view.Info.HeadGen++
	if line := teamsLastPollLine(view, now); strings.Contains(line, "degraded") {
		t.Fatalf("degradation came from an earlier generation: %q", line)
	}
	view.Info.HeadGen--
	written := stamper.wroteAt
	stamper.tick(now.Add(time.Hour))
	if stamper.wroteAt != written {
		t.Fatal("an unchanged outage rewrote the poll snapshot")
	}
	read, since = now.Add(time.Minute), time.Time{}
	stamper.tick(now.Add(time.Minute))
	if line := teamsLastPollLine(view, now); strings.Contains(line, "degraded") || strings.Contains(line, "none recorded") {
		t.Fatalf("recovered poll: %q", line)
	}

	db, err := sqlitestate.OpenSnapshot(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var stamp teamsPollStamp
	if err := db.QueryRow(`SELECT host,pid,version,at FROM poll WHERE id=1`).Scan(&stamp.Host, &stamp.PID, &stamp.Version, &stamp.At); err != nil || stamp.At != read.UnixMilli() {
		t.Fatalf("old reader could not read new snapshot: %+v %v", stamp, err)
	}
}
