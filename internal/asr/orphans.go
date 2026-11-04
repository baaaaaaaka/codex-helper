package asr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// recordServer notes a started server and returns the record's path ("" if
// the platform cannot identify processes).
func recordServer(root string, pid int) string {
	id, ok := processID(pid)
	owner, ownerOK := processID(os.Getpid())
	if !ok || !ownerOK {
		return ""
	}
	rec := serverRecord{Host: hostName(), PID: pid, ID: id, OwnerPID: os.Getpid(), OwnerID: owner}
	data, _ := json.Marshal(rec)
	dir := filepath.Join(root, "run")
	if os.MkdirAll(dir, 0o700) != nil {
		return ""
	}
	safe := strings.Map(func(r rune) rune {
		if r == '.' || r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			return r
		}
		return '_'
	}, rec.Host)
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.json", safe, pid))
	tmp := path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) != nil || os.Rename(tmp, path) != nil {
		os.Remove(tmp)
		return ""
	}
	return path
}

// reapOrphans stops servers recorded on this host whose helper is no longer
// running. Records of other hosts sharing the cache are left alone.
func reapOrphans(root string) {
	dir := filepath.Join(root, "run")
	entries, _ := os.ReadDir(dir)
	host := hostName()
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var rec serverRecord
		if json.Unmarshal(data, &rec) != nil {
			os.Remove(path)
			continue
		}
		if rec.Host != host {
			continue
		}
		if id, ok := processID(rec.OwnerPID); ok && id == rec.OwnerID {
			continue // its helper is still running
		}
		killIfMatches(rec.PID, rec.ID)
		os.Remove(path)
	}
}
