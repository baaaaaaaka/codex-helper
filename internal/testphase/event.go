// Package testphase emits optional, machine-readable phase events for tests
// that exercise asynchronous durable workflows.  It is deliberately inert
// unless CODEX_HELPER_CI_PHASE_FILE is set, so normal unit tests do not gain a
// timing dependency on the diagnostic path.
package testphase

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const schemaVersion = 1

var writeMu sync.Mutex
var sequence atomic.Uint64

// Event is the stable JSONL shape written for one phase.  UnixNano is used
// alongside the RFC3339 timestamp so a runner can order events precisely
// without relying on wall-clock formatting.
type Event struct {
	Schema   int               `json:"schema"`
	Seq      uint64            `json:"seq"`
	Phase    string            `json:"phase"`
	Test     string            `json:"test,omitempty"`
	PID      int               `json:"pid"`
	At       string            `json:"at"`
	UnixNano int64             `json:"unix_nano"`
	Fields   map[string]string `json:"fields,omitempty"`
}

// Emit appends a phase event when the CI runner requested diagnostics.  A
// diagnostic write must never turn a passing product test into a failure, so
// filesystem errors are intentionally ignored.  The manifest runner records
// missing or unreadable phase files in its report instead of changing the
// test's semantic result.
func Emit(phase string, fields map[string]string) {
	path := strings.TrimSpace(os.Getenv("CODEX_HELPER_CI_PHASE_FILE"))
	if path == "" || strings.TrimSpace(phase) == "" {
		return
	}
	now := time.Now().UTC()
	event := Event{
		Schema:   schemaVersion,
		Seq:      sequence.Add(1),
		Phase:    strings.TrimSpace(phase),
		Test:     strings.TrimSpace(os.Getenv("CODEX_HELPER_CI_TEST_NAME")),
		PID:      os.Getpid(),
		At:       now.Format(time.RFC3339Nano),
		UnixNano: now.UnixNano(),
	}
	if len(fields) != 0 {
		event.Fields = make(map[string]string, len(fields))
		for key, value := range fields {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			event.Fields[key] = value
		}
		if len(event.Fields) == 0 {
			event.Fields = nil
		}
	}
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	data = append(data, '\n')

	writeMu.Lock()
	defer writeMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(data)
}
