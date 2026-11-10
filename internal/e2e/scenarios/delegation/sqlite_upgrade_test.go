package delegation

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	_ "modernc.org/sqlite"
)

func TestDelegationSQLiteMigrationPreservesUnknownPOST(t *testing.T) {
	harness.Parallel(t)
	opts := options("an old worker SQLite blob store upgrades without reopening an unknown Graph POST", []string{"F-XM-2", "R-1", "R-2"})
	opts.Golden = harness.GoldenOptions{Skip: "migration evidence and exact request/execution counts are asserted; existing DELEG-RT covers wire rendering"}
	s := harness.NewScenario(t, "DELEG-SQLITE-UPGRADE", opts)
	defer s.Finish()
	as := requester(s)
	worker, ok := resolveWorker(s, as)
	if !ok {
		return
	}
	// Use the same signed-in requester with a separate, pre-upgrade state
	// root. No requester process holds the store while the fixture is built.
	stateRoot := filepath.Join(s.Env.Root, "old-requester-state")
	as.Env["CODEX_HELPER_STATE_DIR"] = stateRoot
	path := filepath.Join(stateRoot, "delegation", "state.sqlite")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	marker := s.Token("never-repost")
	// Frozen v1 wire/ID formula. Do not import production serializers: the
	// migrated witness must be independent of the implementation under test.
	raw, err := json.Marshal(struct {
		Objective string `json:"objective"`
	}{"Investigate " + marker})
	if err != nil {
		t.Fatal(err)
	}
	hash := func(value []byte, n int) string { sum := sha256.Sum256(value); return hex.EncodeToString(sum[:])[:n] }
	sourceKey := "src_" + hash([]byte(strings.Join([]string{"upgrade-source", "upgrade-turn", "", worker.MachineID, hash(raw, 24)}, "\x00")), 32)
	delegationID := "del_" + hash([]byte(sourceKey), 32)
	recordID := "rec_" + hash([]byte("cxp.delegation.request.v1\x00"+sourceKey), 32)
	schema, err := os.ReadFile(filepath.Join("..", "..", "..", "core", "delegate", "delegation", "testdata", "worker-v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(schema)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	witness := fmt.Sprintf(`{"record_id":%q,"delegation_id":%q,"status":"unknown","attempts":7,"error":"original lost response"}`, recordID, delegationID)
	if _, err = db.Exec(`INSERT INTO outbox(id,json) VALUES(?,?)`, recordID, witness); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	task := filepath.Join(s.Env.Root, "upgrade-task.json")
	if err = os.WriteFile(task, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		res := s.RunCLIAs(40*time.Second, as, "delegate", "start", "--candidate-token", worker.CandidateToken, "--task-file", task, "--source-session", "upgrade-source", "--source-turn", "upgrade-turn", "--json")
		s.Check(fmt.Sprintf("unknown POST stays fenced on CLI attempt %d", attempt+1), res.Code != 0 && strings.Contains(res.Combined, "refusing automatic replay"), "explicit refusal to replay", harness.NormalizeSpace(res.Combined))
	}
	recs, _, _ := inboxRecords(s, delegationID)
	s.Check("upgrade and cold reopen issue no request or execution", len(recs["request"]) == 0 && len(s.Codex.CallsWithToken(marker)) == 0, "zero request posts; zero executions", describe(recs))
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, attempts, blobs int
	var status string
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT status,attempts FROM outbox WHERE id=?`, recordID).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM pragma_table_info('outbox') WHERE name='json'`).Scan(&blobs); err != nil {
		t.Fatal(err)
	}
	s.Check("the migrated reservation retains its original evidence", version == 2 && status == "unknown" && attempts == 7 && blobs == 0, "v2; unknown; seven attempts; no JSON column", fmt.Sprintf("version=%d status=%s attempts=%d blobs=%d", version, status, attempts, blobs))
}
