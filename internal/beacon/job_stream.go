package beacon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

const (
	JobStreamSchemaVersion = 1
	JobStreamMaxLineBytes  = 256 << 10
	JobStreamMaxReadBytes  = 4 << 20
)

const (
	jobStreamMaxTextBytes    = 64 << 10
	jobStreamMaxCommandBytes = 4 << 10
)

type JobStreamRecord struct {
	Version       int                 `json:"version"`
	Seq           int                 `json:"seq"`
	JobID         string              `json:"job_id"`
	RequestID     string              `json:"request_id,omitempty"`
	TurnID        string              `json:"turn_id,omitempty"`
	WorkerID      string              `json:"worker_id,omitempty"`
	LeaseID       string              `json:"lease_id,omitempty"`
	ClaimEpoch    int                 `json:"claim_epoch,omitempty"`
	ProviderJobID string              `json:"provider_job_id,omitempty"`
	Kind          string              `json:"kind,omitempty"`
	At            time.Time           `json:"at,omitempty"`
	Event         JobStreamCodexEvent `json:"event"`
}

type JobStreamCodexEvent struct {
	Kind      codexrunner.StreamEventKind `json:"kind"`
	ThreadID  string                      `json:"thread_id,omitempty"`
	TurnID    string                      `json:"turn_id,omitempty"`
	ItemID    string                      `json:"item_id,omitempty"`
	ItemType  string                      `json:"item_type,omitempty"`
	Text      string                      `json:"text,omitempty"`
	Command   string                      `json:"command,omitempty"`
	Status    string                      `json:"status,omitempty"`
	ExitCode  *int                        `json:"exit_code,omitempty"`
	Failure   *codexrunner.TurnFailure    `json:"failure,omitempty"`
	WillRetry bool                        `json:"will_retry,omitempty"`
	Usage     codexrunner.Usage           `json:"usage,omitempty"`
}

func JobStreamRootForStorePath(storePath string) string {
	return filepath.Join(filepath.Dir(filepath.Clean(storePath)), "streams", "jobs")
}

func JobStreamPathForStorePath(storePath string, jobID string) (string, error) {
	safeJobID, err := safeJobStreamID(jobID)
	if err != nil {
		return "", err
	}
	root := JobStreamRootForStorePath(storePath)
	path := filepath.Join(root, safeJobID+".jsonl")
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", fmt.Errorf("resolve job stream path: %w", err)
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("job stream path escapes stream root")
	}
	return path, nil
}

func safeJobStreamID(jobID string) (string, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return "", fmt.Errorf("job id is required")
	}
	if jobID == "." || jobID == ".." || len(jobID) > 160 {
		return "", fmt.Errorf("invalid job id %q", jobID)
	}
	for _, r := range jobID {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			continue
		}
		return "", fmt.Errorf("invalid job id %q", jobID)
	}
	return jobID, nil
}

type JobStreamWriter struct {
	mu   sync.Mutex
	file *os.File
	job  JobAttempt
	seq  int
}

func NewJobStreamWriter(storePath string, job JobAttempt) (*JobStreamWriter, error) {
	path, err := JobStreamPathForStorePath(storePath, job.ID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create job stream dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open job stream: %w", err)
	}
	return &JobStreamWriter{file: file, job: job}, nil
}

func (w *JobStreamWriter) Append(event codexrunner.StreamEvent) error {
	if w == nil || strings.TrimSpace(string(event.Kind)) == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	record := NewJobStreamRecord(w.job, w.seq+1, event, time.Now().UTC())
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal job stream event: %w", err)
	}
	if len(line) > JobStreamMaxLineBytes {
		return fmt.Errorf("job stream event too large: %d bytes", len(line))
	}
	line = append(line, '\n')
	n, err := w.file.Write(line)
	if err != nil {
		return fmt.Errorf("write job stream event: %w", err)
	}
	if n != len(line) {
		return io.ErrShortWrite
	}
	w.seq++
	return nil
}

func (w *JobStreamWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func NewJobStreamRecord(job JobAttempt, seq int, event codexrunner.StreamEvent, at time.Time) JobStreamRecord {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	record := JobStreamRecord{
		Version:       JobStreamSchemaVersion,
		Seq:           seq,
		JobID:         strings.TrimSpace(job.ID),
		RequestID:     strings.TrimSpace(job.RequestID),
		TurnID:        strings.TrimSpace(job.TurnID),
		WorkerID:      strings.TrimSpace(job.WorkerID),
		LeaseID:       strings.TrimSpace(job.LeaseID),
		ClaimEpoch:    job.ClaimEpoch,
		ProviderJobID: strings.TrimSpace(job.ProviderIdentity.ProviderJobID),
		Kind:          strings.TrimSpace(string(event.Kind)),
		At:            at,
		Event:         newJobStreamCodexEvent(event),
	}
	return record
}

func newJobStreamCodexEvent(event codexrunner.StreamEvent) JobStreamCodexEvent {
	var failure *codexrunner.TurnFailure
	if event.Failure != nil {
		copyFailure := *event.Failure
		failure = &copyFailure
	}
	return JobStreamCodexEvent{
		Kind:      event.Kind,
		ThreadID:  strings.TrimSpace(event.ThreadID),
		TurnID:    strings.TrimSpace(event.TurnID),
		ItemID:    strings.TrimSpace(event.ItemID),
		ItemType:  strings.TrimSpace(event.ItemType),
		Text:      truncateStringBytes(event.Text, jobStreamMaxTextBytes),
		Command:   truncateStringBytes(event.Command, jobStreamMaxCommandBytes),
		Status:    strings.TrimSpace(event.Status),
		ExitCode:  event.ExitCode,
		Failure:   failure,
		WillRetry: event.WillRetry,
		Usage:     event.Usage,
	}
}

func (event JobStreamCodexEvent) StreamEvent() codexrunner.StreamEvent {
	var failure *codexrunner.TurnFailure
	if event.Failure != nil {
		copyFailure := *event.Failure
		failure = &copyFailure
	}
	return codexrunner.StreamEvent{
		Kind:      event.Kind,
		ThreadID:  event.ThreadID,
		TurnID:    event.TurnID,
		ItemID:    event.ItemID,
		ItemType:  event.ItemType,
		Text:      event.Text,
		Command:   event.Command,
		Status:    event.Status,
		ExitCode:  event.ExitCode,
		Failure:   failure,
		WillRetry: event.WillRetry,
		Usage:     event.Usage,
	}
}

type JobStreamReaderOptions struct {
	StartAtEnd   bool
	MaxLineBytes int
}

type JobStreamReader struct {
	path         string
	offset       int64
	pending      []byte
	maxLineBytes int
}

func NewJobStreamReader(storePath string, jobID string, opts JobStreamReaderOptions) (*JobStreamReader, error) {
	path, err := JobStreamPathForStorePath(storePath, jobID)
	if err != nil {
		return nil, err
	}
	maxLineBytes := opts.MaxLineBytes
	if maxLineBytes <= 0 {
		maxLineBytes = JobStreamMaxLineBytes
	}
	reader := &JobStreamReader{path: path, maxLineBytes: maxLineBytes}
	if opts.StartAtEnd {
		info, err := os.Stat(path)
		switch {
		case err == nil:
			reader.offset = info.Size()
		case errors.Is(err, os.ErrNotExist):
		default:
			return nil, fmt.Errorf("stat job stream: %w", err)
		}
	}
	return reader, nil
}

func CleanupJobStreams(storePath string, st State, now time.Time, retention time.Duration) error {
	if retention <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	st.normalize()
	root := JobStreamRootForStorePath(storePath)
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read job stream dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		jobID := strings.TrimSuffix(entry.Name(), ".jsonl")
		path, err := JobStreamPathForStorePath(storePath, jobID)
		if err != nil {
			continue
		}
		removeAfter, known, removable := jobStreamCleanupDecision(st, jobID)
		if known && !removable {
			continue
		}
		if !known {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			removeAfter = info.ModTime()
		}
		if removeAfter.IsZero() || now.Sub(removeAfter) < retention {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove job stream %q: %w", jobID, err)
		}
	}
	return nil
}

func jobStreamCleanupDecision(st State, jobID string) (time.Time, bool, bool) {
	jobID = strings.TrimSpace(jobID)
	if attempt, ok := st.JobAttempts[jobID]; ok {
		switch attempt.Phase {
		case JobQueued, JobClaimed, JobStartIntent, JobStarted, JobAmbiguous:
			return time.Time{}, true, false
		case JobTerminal:
			if terminal, ok := st.Terminals[jobID]; ok && !terminal.AcceptedAt.IsZero() {
				return terminal.AcceptedAt, true, true
			}
			return attempt.UpdatedAt, true, true
		case JobQuarantined, JobTombstoned:
			return attempt.UpdatedAt, true, true
		default:
			return time.Time{}, true, false
		}
	}
	if terminal, ok := st.Terminals[jobID]; ok {
		return terminal.AcceptedAt, true, true
	}
	return time.Time{}, false, false
}

func (r *JobStreamReader) ReadAvailable() ([]JobStreamRecord, error) {
	if r == nil {
		return nil, nil
	}
	file, err := os.Open(r.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open job stream: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat job stream: %w", err)
	}
	if r.offset > info.Size() {
		r.offset = 0
		r.pending = nil
	}
	if r.offset == info.Size() {
		return nil, nil
	}
	if _, err := file.Seek(r.offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek job stream: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(file, JobStreamMaxReadBytes))
	if err != nil {
		return nil, fmt.Errorf("read job stream: %w", err)
	}
	r.offset += int64(len(data))
	r.pending = append(r.pending, data...)
	return r.consumeCompleteLines(), nil
}

func (r *JobStreamReader) consumeCompleteLines() []JobStreamRecord {
	var records []JobStreamRecord
	for {
		idx := bytes.IndexByte(r.pending, '\n')
		if idx < 0 {
			break
		}
		line := r.pending[:idx]
		r.pending = r.pending[idx+1:]
		line = bytes.TrimSpace(line)
		if len(line) == 0 || len(line) > r.maxLineBytes {
			continue
		}
		var record JobStreamRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}
		if record.Version != JobStreamSchemaVersion || strings.TrimSpace(record.JobID) == "" || record.Event.Kind == "" {
			continue
		}
		records = append(records, record)
	}
	if len(r.pending) > r.maxLineBytes {
		r.pending = nil
	}
	return records
}

func truncateStringBytes(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	cut := 0
	for idx := range value {
		if idx > limit {
			break
		}
		cut = idx
	}
	if cut <= 0 {
		return ""
	}
	return value[:cut]
}
