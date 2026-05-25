package teams

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTeamsReplaceFileWithRetryRetriesRetryableErrors(t *testing.T) {
	retryErr := errors.New("access denied")
	attempts := 0
	err := replaceFileWithRetry("tmp", "registry.json", func(string, string) error {
		attempts++
		if attempts < 4 {
			return retryErr
		}
		return nil
	}, func(err error) bool {
		return errors.Is(err, retryErr)
	})
	if err != nil {
		t.Fatalf("replaceFileWithRetry error: %v", err)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d, want 4", attempts)
	}
}

func TestTeamsReplaceFileWithRetryStopsOnPermanentError(t *testing.T) {
	permanentErr := errors.New("permission denied")
	attempts := 0
	err := replaceFileWithRetry("tmp", "registry.json", func(string, string) error {
		attempts++
		return permanentErr
	}, func(error) bool { return false })
	if !errors.Is(err, permanentErr) {
		t.Fatalf("replaceFileWithRetry error = %v, want permanent error", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 for permanent error", attempts)
	}
}

func TestTeamsReplaceFileWithRetryBoundsRetryableErrors(t *testing.T) {
	prevSleep := durableReplaceSleep
	var sleeps []time.Duration
	durableReplaceSleep = func(delay time.Duration) {
		sleeps = append(sleeps, delay)
	}
	t.Cleanup(func() { durableReplaceSleep = prevSleep })

	retryErr := errors.New("sharing violation")
	attempts := 0
	err := replaceFileWithRetry("tmp", "registry.json", func(string, string) error {
		attempts++
		return retryErr
	}, func(err error) bool {
		return errors.Is(err, retryErr)
	})
	if !errors.Is(err, retryErr) {
		t.Fatalf("replaceFileWithRetry error = %v, want retry error", err)
	}
	if attempts != durableReplaceAttempts {
		t.Fatalf("attempts = %d, want bounded attempts %d", attempts, durableReplaceAttempts)
	}
	wantSleeps := []time.Duration{
		25 * time.Millisecond,
		50 * time.Millisecond,
		75 * time.Millisecond,
		100 * time.Millisecond,
		125 * time.Millisecond,
		150 * time.Millisecond,
		175 * time.Millisecond,
	}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("retry sleeps = %v, want %v", sleeps, wantSleeps)
	}
	for i := range sleeps {
		if sleeps[i] != wantSleeps[i] {
			t.Fatalf("retry sleeps = %v, want %v", sleeps, wantSleeps)
		}
	}
}

func TestTeamsDurableWriteFileUsesTempAndCleansFailedReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	replaceErr := errors.New("replace failed")
	prev := durableReplaceFile
	t.Cleanup(func() { durableReplaceFile = prev })
	var tempPath string
	durableReplaceFile = func(src string, dst string) error {
		tempPath = src
		if dst != path {
			t.Fatalf("replace dst = %q, want %q", dst, path)
		}
		if filepath.Dir(src) != filepath.Dir(path) {
			t.Fatalf("replace src dir = %q, want %q", filepath.Dir(src), filepath.Dir(path))
		}
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("read temp during replace: %v", err)
		}
		if string(data) != "new registry" {
			t.Fatalf("temp data = %q, want new registry", data)
		}
		return replaceErr
	}

	err := durableWriteFile(path, []byte("new registry"), 0o600)
	if !errors.Is(err, replaceErr) {
		t.Fatalf("durableWriteFile error = %v, want replace error", err)
	}
	if tempPath == "" {
		t.Fatal("durableReplaceFile was not called")
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file still exists after replace failure: stat err=%v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target should not exist after failed replace: stat err=%v", err)
	}
}
