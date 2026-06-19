package beacon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

func TestJobStreamPathForStorePathValidatesJobID(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	path, err := JobStreamPathForStorePath(storePath, "job-req_1.2")
	if err != nil {
		t.Fatalf("JobStreamPathForStorePath valid id: %v", err)
	}
	if want := filepath.Join(filepath.Dir(storePath), "streams", "jobs", "job-req_1.2.jsonl"); path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
	for _, id := range []string{"", "../job", "job/one", `job\one`, "."} {
		if _, err := JobStreamPathForStorePath(storePath, id); err == nil {
			t.Fatalf("JobStreamPathForStorePath(%q) succeeded, want validation error", id)
		}
	}
}

func TestJobStreamWriterReaderIgnoresPartialAndOversizedLines(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	job := JobAttempt{
		ID:               "job-req-1",
		RequestID:        "req-1",
		TurnID:           "turn-1",
		WorkerID:         "worker-1",
		LeaseID:          "lease-1",
		ClaimEpoch:       2,
		ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
	}
	writer, err := NewJobStreamWriter(storePath, job)
	if err != nil {
		t.Fatalf("NewJobStreamWriter: %v", err)
	}
	if err := writer.Append(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Phase: "final_answer", Text: "first", ThreadID: "thread-1", TurnID: "codex-turn-1", Raw: []byte("raw must not persist")}); err != nil {
		t.Fatalf("Append first: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	path, err := JobStreamPathForStorePath(storePath, job.ID)
	if err != nil {
		t.Fatalf("JobStreamPathForStorePath: %v", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open stream append: %v", err)
	}
	if _, err := f.WriteString(strings.Repeat("x", JobStreamMaxLineBytes+1) + "\n"); err != nil {
		t.Fatalf("write oversized line: %v", err)
	}
	if _, err := f.WriteString(`{"version":1,"job_id":"job-req-1","event":{"kind":"agent_message","text":"partial"}`); err != nil {
		t.Fatalf("write partial line: %v", err)
	}
	_ = f.Close()

	reader, err := NewJobStreamReader(storePath, job.ID, JobStreamReaderOptions{})
	if err != nil {
		t.Fatalf("NewJobStreamReader: %v", err)
	}
	records, err := reader.ReadAvailable()
	if err != nil {
		t.Fatalf("ReadAvailable: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1: %#v", len(records), records)
	}
	if records[0].Seq != 1 || records[0].JobID != job.ID || records[0].ClaimEpoch != 2 || records[0].ProviderJobID != "slurm-1" {
		t.Fatalf("record metadata = %#v", records[0])
	}
	event := records[0].Event.StreamEvent()
	if event.Kind != codexrunner.StreamEventAgentMessage || event.Phase != "final_answer" || event.Text != "first" || len(event.Raw) != 0 {
		t.Fatalf("event = %#v", event)
	}
	if records, err = reader.ReadAvailable(); err != nil || len(records) != 0 {
		t.Fatalf("second ReadAvailable records=%#v err=%v, want none", records, err)
	}
}

func TestJobStreamReaderStartAtEndSkipsExistingContent(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	job := JobAttempt{ID: "job-req-2", RequestID: "req-2", TurnID: "turn-2"}
	writer, err := NewJobStreamWriter(storePath, job)
	if err != nil {
		t.Fatalf("NewJobStreamWriter: %v", err)
	}
	if err := writer.Append(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: "old"}); err != nil {
		t.Fatalf("Append old: %v", err)
	}
	reader, err := NewJobStreamReader(storePath, job.ID, JobStreamReaderOptions{StartAtEnd: true})
	if err != nil {
		t.Fatalf("NewJobStreamReader: %v", err)
	}
	if err := writer.Append(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: "new"}); err != nil {
		t.Fatalf("Append new: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	records, err := reader.ReadAvailable()
	if err != nil {
		t.Fatalf("ReadAvailable: %v", err)
	}
	if len(records) != 1 || records[0].Event.Text != "new" {
		t.Fatalf("records = %#v, want only new event", records)
	}
	if records[0].At.IsZero() || time.Since(records[0].At) > time.Minute {
		t.Fatalf("record timestamp = %v", records[0].At)
	}
}

func TestJobStreamWriterReaderStressManyEventsIncrementalTail(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	job := JobAttempt{
		ID:               "job-stress",
		RequestID:        "req-stress",
		TurnID:           "turn-stress",
		WorkerID:         "worker-stress",
		LeaseID:          "lease-stress",
		ClaimEpoch:       7,
		ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-stress"},
	}
	writer, err := NewJobStreamWriter(storePath, job)
	if err != nil {
		t.Fatalf("NewJobStreamWriter: %v", err)
	}
	reader, err := NewJobStreamReader(storePath, job.ID, JobStreamReaderOptions{})
	if err != nil {
		t.Fatalf("NewJobStreamReader: %v", err)
	}
	const total = 750
	var records []JobStreamRecord
	for i := 0; i < total; i++ {
		event := codexrunner.StreamEvent{
			Kind:             codexrunner.StreamEventAgentMessage,
			Text:             fmt.Sprintf("progress-%03d", i),
			ThreadID:         "thread-stress",
			TurnID:           "turn-stress",
			AggregatedOutput: strings.Repeat("must-not-persist", 64),
			Raw:              []byte("raw must not persist"),
		}
		if i%17 == 0 {
			exitCode := i % 3
			event.Kind = codexrunner.StreamEventCommandCompleted
			event.Text = ""
			event.Command = fmt.Sprintf("cmd-%03d", i)
			event.ExitCode = &exitCode
		}
		if err := writer.Append(event); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		if i%11 == 0 {
			next, err := reader.ReadAvailable()
			if err != nil {
				t.Fatalf("ReadAvailable(%d): %v", i, err)
			}
			records = append(records, next...)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	for len(records) < total {
		next, err := reader.ReadAvailable()
		if err != nil {
			t.Fatalf("ReadAvailable final: %v", err)
		}
		if len(next) == 0 {
			break
		}
		records = append(records, next...)
	}
	if len(records) != total {
		t.Fatalf("records = %d, want %d", len(records), total)
	}
	for i, record := range records {
		if record.Seq != i+1 || record.JobID != job.ID || record.ClaimEpoch != job.ClaimEpoch || record.ProviderJobID != "slurm-stress" {
			t.Fatalf("record[%d] metadata = %#v", i, record)
		}
		event := record.Event.StreamEvent()
		if len(event.Raw) != 0 || strings.TrimSpace(event.AggregatedOutput) != "" {
			t.Fatalf("record[%d] leaked raw/output: %#v", i, event)
		}
		if i%17 == 0 {
			if event.Kind != codexrunner.StreamEventCommandCompleted || event.Command != fmt.Sprintf("cmd-%03d", i) {
				t.Fatalf("record[%d] command event = %#v", i, event)
			}
			continue
		}
		if event.Kind != codexrunner.StreamEventAgentMessage || event.Text != fmt.Sprintf("progress-%03d", i) {
			t.Fatalf("record[%d] agent event = %#v", i, event)
		}
	}
}

func TestJobStreamReaderStressMalformedRecordMatrix(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	job := JobAttempt{ID: "job-malformed", RequestID: "req-malformed", TurnID: "turn-malformed"}
	writer, err := NewJobStreamWriter(storePath, job)
	if err != nil {
		t.Fatalf("NewJobStreamWriter: %v", err)
	}
	if err := writer.Append(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: "valid-0"}); err != nil {
		t.Fatalf("Append valid-0: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	path, err := JobStreamPathForStorePath(storePath, job.ID)
	if err != nil {
		t.Fatalf("JobStreamPathForStorePath: %v", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open stream append: %v", err)
	}
	lines := []string{
		`not-json`,
		`{"version":2,"job_id":"job-malformed","event":{"kind":"agent_message","text":"future-version"}}`,
		`{"version":1,"job_id":"","event":{"kind":"agent_message","text":"missing-job"}}`,
		`{"version":1,"job_id":"job-malformed","event":{}}`,
		strings.Repeat("x", JobStreamMaxLineBytes+64),
		`{"version":1,"seq":2,"job_id":"job-malformed","event":{"kind":"agent_message","text":"valid-1"}}`,
	}
	for _, line := range lines {
		if _, err := f.WriteString(line + "\n"); err != nil {
			t.Fatalf("write malformed matrix line: %v", err)
		}
	}
	if _, err := f.WriteString(`{"version":1,"seq":3,"job_id":"job-malformed","event":{"kind":"agent_message","text":"partial"}`); err != nil {
		t.Fatalf("write partial line: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close stream append: %v", err)
	}
	reader, err := NewJobStreamReader(storePath, job.ID, JobStreamReaderOptions{})
	if err != nil {
		t.Fatalf("NewJobStreamReader: %v", err)
	}
	records, err := reader.ReadAvailable()
	if err != nil {
		t.Fatalf("ReadAvailable: %v", err)
	}
	var texts []string
	for _, record := range records {
		texts = append(texts, record.Event.Text)
	}
	if got, want := strings.Join(texts, ","), "valid-0,valid-1"; got != want {
		t.Fatalf("valid texts = %q, want %q; records=%#v", got, want, records)
	}
	if records, err = reader.ReadAvailable(); err != nil || len(records) != 0 {
		t.Fatalf("second ReadAvailable records=%#v err=%v, want none", records, err)
	}
}

func TestJobStreamReaderStressContinuesAfterReadWindow(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	job := JobAttempt{ID: "job-read-window", RequestID: "req-read-window", TurnID: "turn-read-window"}
	writer, err := NewJobStreamWriter(storePath, job)
	if err != nil {
		t.Fatalf("NewJobStreamWriter: %v", err)
	}
	const total = 2300
	payload := strings.Repeat("x", 2048)
	for i := 0; i < total; i++ {
		if err := writer.Append(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: fmt.Sprintf("window-%04d-%s", i, payload)}); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	reader, err := NewJobStreamReader(storePath, job.ID, JobStreamReaderOptions{})
	if err != nil {
		t.Fatalf("NewJobStreamReader: %v", err)
	}
	var records []JobStreamRecord
	for len(records) < total {
		next, err := reader.ReadAvailable()
		if err != nil {
			t.Fatalf("ReadAvailable: %v", err)
		}
		if len(next) == 0 {
			break
		}
		records = append(records, next...)
	}
	if len(records) != total {
		t.Fatalf("records = %d, want %d", len(records), total)
	}
	if records[0].Seq != 1 || records[len(records)-1].Seq != total {
		t.Fatalf("seq range = %d..%d, want 1..%d", records[0].Seq, records[len(records)-1].Seq, total)
	}
}

func TestCleanupJobStreamsKeepsActiveAndRemovesExpiredTerminal(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	now := time.Now()
	active := JobAttempt{ID: "job-active", Phase: JobStarted, UpdatedAt: now.Add(-48 * time.Hour)}
	terminal := JobAttempt{ID: "job-terminal", Phase: JobTerminal, UpdatedAt: now.Add(-48 * time.Hour)}
	for _, job := range []JobAttempt{active, terminal, {ID: "job-orphan"}} {
		writer, err := NewJobStreamWriter(storePath, job)
		if err != nil {
			t.Fatalf("NewJobStreamWriter(%s): %v", job.ID, err)
		}
		if err := writer.Append(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: job.ID}); err != nil {
			t.Fatalf("Append(%s): %v", job.ID, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close(%s): %v", job.ID, err)
		}
	}
	orphanPath, err := JobStreamPathForStorePath(storePath, "job-orphan")
	if err != nil {
		t.Fatalf("orphan path: %v", err)
	}
	old := now.Add(-48 * time.Hour)
	if err := os.Chtimes(orphanPath, old, old); err != nil {
		t.Fatalf("chtimes orphan: %v", err)
	}
	st := State{
		JobAttempts: map[string]JobAttempt{
			active.ID:   active,
			terminal.ID: terminal,
		},
		Terminals: map[string]TerminalRecord{
			terminal.ID: {JobID: terminal.ID, AcceptedAt: now.Add(-48 * time.Hour)},
		},
	}
	st.normalize()
	if err := CleanupJobStreams(storePath, st, now, 24*time.Hour); err != nil {
		t.Fatalf("CleanupJobStreams: %v", err)
	}
	activePath, err := JobStreamPathForStorePath(storePath, active.ID)
	if err != nil {
		t.Fatalf("active path: %v", err)
	}
	terminalPath, err := JobStreamPathForStorePath(storePath, terminal.ID)
	if err != nil {
		t.Fatalf("terminal path: %v", err)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active stream should remain: %v", err)
	}
	if _, err := os.Stat(terminalPath); !os.IsNotExist(err) {
		t.Fatalf("terminal stream stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan stream stat err = %v, want not exist", err)
	}
}

func TestCleanupJobStreamsStressPhaseMatrix(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	now := time.Now()
	old := now.Add(-72 * time.Hour)
	recent := now.Add(-time.Hour)
	st := State{JobAttempts: map[string]JobAttempt{}, Terminals: map[string]TerminalRecord{}}
	cases := []struct {
		name       string
		phase      JobPhase
		updatedAt  time.Time
		acceptedAt time.Time
		wantRemove bool
	}{
		{name: "queued", phase: JobQueued, updatedAt: old},
		{name: "claimed", phase: JobClaimed, updatedAt: old},
		{name: "start-intent", phase: JobStartIntent, updatedAt: old},
		{name: "started", phase: JobStarted, updatedAt: old},
		{name: "ambiguous", phase: JobAmbiguous, updatedAt: old},
		{name: "terminal-old", phase: JobTerminal, updatedAt: old, acceptedAt: old, wantRemove: true},
		{name: "terminal-recent", phase: JobTerminal, updatedAt: old, acceptedAt: recent},
		{name: "terminal-no-accepted", phase: JobTerminal, updatedAt: old, wantRemove: true},
		{name: "quarantined-old", phase: JobQuarantined, updatedAt: old, wantRemove: true},
		{name: "tombstoned-old", phase: JobTombstoned, updatedAt: old, wantRemove: true},
		{name: "unknown-phase", phase: JobPhase("future_phase"), updatedAt: old},
	}
	for _, tc := range cases {
		job := JobAttempt{ID: "job-phase-" + tc.name, Phase: tc.phase, UpdatedAt: tc.updatedAt}
		st.JobAttempts[job.ID] = job
		if tc.phase == JobTerminal && !tc.acceptedAt.IsZero() {
			st.Terminals[job.ID] = TerminalRecord{JobID: job.ID, AcceptedAt: tc.acceptedAt}
		}
		writeJobStreamForCleanupStress(t, storePath, job, old)
	}
	st.Terminals["job-terminal-only-old"] = TerminalRecord{JobID: "job-terminal-only-old", AcceptedAt: old}
	writeJobStreamForCleanupStress(t, storePath, JobAttempt{ID: "job-terminal-only-old"}, old)
	st.Terminals["job-terminal-only-recent"] = TerminalRecord{JobID: "job-terminal-only-recent", AcceptedAt: recent}
	writeJobStreamForCleanupStress(t, storePath, JobAttempt{ID: "job-terminal-only-recent"}, old)
	st.normalize()
	if err := CleanupJobStreams(storePath, st, now, 24*time.Hour); err != nil {
		t.Fatalf("CleanupJobStreams: %v", err)
	}
	for _, tc := range cases {
		path, err := JobStreamPathForStorePath(storePath, "job-phase-"+tc.name)
		if err != nil {
			t.Fatalf("%s path: %v", tc.name, err)
		}
		_, err = os.Stat(path)
		if tc.wantRemove {
			if !os.IsNotExist(err) {
				t.Fatalf("%s stream stat err = %v, want removed", tc.name, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s stream should remain: %v", tc.name, err)
		}
	}
	for _, tc := range []struct {
		jobID      string
		wantRemove bool
	}{
		{jobID: "job-terminal-only-old", wantRemove: true},
		{jobID: "job-terminal-only-recent"},
	} {
		path, err := JobStreamPathForStorePath(storePath, tc.jobID)
		if err != nil {
			t.Fatalf("%s path: %v", tc.jobID, err)
		}
		_, err = os.Stat(path)
		if tc.wantRemove {
			if !os.IsNotExist(err) {
				t.Fatalf("%s stream stat err = %v, want removed", tc.jobID, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s stream should remain: %v", tc.jobID, err)
		}
	}
}

func TestCleanupJobStreamsStressManyFiles(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "state.json")
	now := time.Now()
	st := State{
		JobAttempts: map[string]JobAttempt{},
		Terminals:   map[string]TerminalRecord{},
	}
	const activeCount = 40
	const terminalCount = 40
	const orphanCount = 40
	for i := 0; i < activeCount; i++ {
		job := JobAttempt{ID: fmt.Sprintf("job-active-%02d", i), Phase: JobStarted, UpdatedAt: now.Add(-72 * time.Hour)}
		st.JobAttempts[job.ID] = job
		writeJobStreamForCleanupStress(t, storePath, job, now.Add(-72*time.Hour))
	}
	for i := 0; i < terminalCount; i++ {
		job := JobAttempt{ID: fmt.Sprintf("job-terminal-%02d", i), Phase: JobTerminal, UpdatedAt: now.Add(-72 * time.Hour)}
		st.JobAttempts[job.ID] = job
		st.Terminals[job.ID] = TerminalRecord{JobID: job.ID, AcceptedAt: now.Add(-72 * time.Hour)}
		writeJobStreamForCleanupStress(t, storePath, job, now.Add(-72*time.Hour))
	}
	for i := 0; i < orphanCount; i++ {
		job := JobAttempt{ID: fmt.Sprintf("job-orphan-%02d", i)}
		writeJobStreamForCleanupStress(t, storePath, job, now.Add(-72*time.Hour))
	}
	st.normalize()
	if err := CleanupJobStreams(storePath, st, now, 24*time.Hour); err != nil {
		t.Fatalf("CleanupJobStreams: %v", err)
	}
	for i := 0; i < activeCount; i++ {
		path, err := JobStreamPathForStorePath(storePath, fmt.Sprintf("job-active-%02d", i))
		if err != nil {
			t.Fatalf("active path %d: %v", i, err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("active stream %d should remain: %v", i, err)
		}
	}
	for _, prefix := range []string{"job-terminal", "job-orphan"} {
		for i := 0; i < terminalCount; i++ {
			path, err := JobStreamPathForStorePath(storePath, fmt.Sprintf("%s-%02d", prefix, i))
			if err != nil {
				t.Fatalf("%s path %d: %v", prefix, i, err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("%s stream %d stat err = %v, want not exist", prefix, i, err)
			}
		}
	}
}

func writeJobStreamForCleanupStress(t *testing.T, storePath string, job JobAttempt, modTime time.Time) {
	t.Helper()
	writer, err := NewJobStreamWriter(storePath, job)
	if err != nil {
		t.Fatalf("NewJobStreamWriter(%s): %v", job.ID, err)
	}
	if err := writer.Append(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: job.ID}); err != nil {
		t.Fatalf("Append(%s): %v", job.ID, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(%s): %v", job.ID, err)
	}
	path, err := JobStreamPathForStorePath(storePath, job.ID)
	if err != nil {
		t.Fatalf("JobStreamPathForStorePath(%s): %v", job.ID, err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("chtimes(%s): %v", job.ID, err)
	}
}
