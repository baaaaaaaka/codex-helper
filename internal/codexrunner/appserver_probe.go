package codexrunner

import (
	"context"
	"strings"
	"time"
)

type AppServerProbeOptions struct {
	Starter    AppServerTransportStarter
	Command    string
	Args       []string
	WorkingDir string
	Timeout    time.Duration
	Runs       int
	Limit      int
}

type AppServerProbeRun struct {
	Index       int
	ThreadCount int
	Duration    time.Duration
}

type AppServerProbeResult struct {
	Command    string
	WorkingDir string
	Runs       []AppServerProbeRun
	Total      time.Duration
	Min        time.Duration
	Max        time.Duration
}

func ProbeAppServerCompatibility(ctx context.Context, opts AppServerProbeOptions) (AppServerProbeResult, error) {
	runs := opts.Runs
	if runs <= 0 {
		runs = 1
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1
	}
	command := strings.TrimSpace(opts.Command)
	if command == "" {
		command = defaultCodexCommand
	}
	starter := opts.Starter
	if starter == nil {
		starter = AppServerProcessStarter{}
	}
	result := AppServerProbeResult{
		Command:    command,
		WorkingDir: strings.TrimSpace(opts.WorkingDir),
	}
	totalStart := time.Now()
	for i := 0; i < runs; i++ {
		runner := &AppServerRunner{
			Starter:       starter,
			Command:       command,
			AppServerArgs: append([]string{}, opts.Args...),
			WorkingDir:    result.WorkingDir,
			Timeout:       opts.Timeout,
		}
		started := time.Now()
		threads, err := runner.ListThreads(ctx, ListThreadsOptions{
			WorkingDir: result.WorkingDir,
			Limit:      limit,
		})
		duration := time.Since(started)
		_ = runner.Close()
		if err != nil {
			result.Total = time.Since(totalStart)
			return result, err
		}
		run := AppServerProbeRun{
			Index:       i + 1,
			ThreadCount: len(threads),
			Duration:    duration,
		}
		result.Runs = append(result.Runs, run)
		if result.Min == 0 || duration < result.Min {
			result.Min = duration
		}
		if duration > result.Max {
			result.Max = duration
		}
	}
	result.Total = time.Since(totalStart)
	return result, nil
}
