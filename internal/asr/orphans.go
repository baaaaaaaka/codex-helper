package asr

import (
	"os"
)

// A helper that dies without Close (SIGKILL, crash, power loss of a
// session) must not leave a 1-2 GB llama-server behind. Linux kills it with
// the helper (Pdeathsig), Windows through the kill-on-close job object; on
// every platform the server is also recorded under <cache>/run, and the next
// helper on the same host stops recorded servers whose helper is gone.
// Processes are identified by PID plus start time and executable, so a
// recycled PID is never killed.

// deathSignal can be turned off by tests that need a server to outlive its
// helper.
var deathSignal = true

type serverRecord struct {
	Host     string `json:"host"`
	PID      int    `json:"pid"`
	ID       string `json:"id"`
	OwnerPID int    `json:"owner_pid"`
	OwnerID  string `json:"owner_id"`
}

func hostName() string {
	h, _ := os.Hostname()
	if h == "" {
		h = "localhost"
	}
	return h
}

// recordServer retains one connection for this server's lifetime. Platforms
// without process identities retain their existing parent-death protection.
func recordServer(root string, pid int) (*serverRegistration, error) {
	id, ok := processID(pid)
	owner, ownerOK := processID(os.Getpid())
	if !ok || !ownerOK {
		return nil, nil
	}
	db, err := openServerRecords(root)
	if err != nil {
		return nil, err
	}
	rec := serverRecord{Host: hostName(), PID: pid, ID: id, OwnerPID: os.Getpid(), OwnerID: owner}
	if err = db.put(rec); err != nil {
		db.Close()
		return nil, err
	}
	return &serverRegistration{db: db, rec: rec}, nil
}

// reapOrphans reads only this host's records in bounded pages. No database
// transaction spans process inspection or termination.
func reapOrphans(root string) error {
	db, err := openServerRecords(root)
	if err != nil {
		return err
	}
	defer db.Close()
	var after int
	for {
		records, err := db.page(hostName(), after)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, rec := range records {
			after = rec.PID
			if id, ok := processID(rec.OwnerPID); ok && id == rec.OwnerID {
				continue
			}
			killIfMatches(rec.PID, rec.ID)
			if err = db.remove(rec); err != nil {
				return err
			}
		}
	}
}
