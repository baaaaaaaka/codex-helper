package codexrunner

import "io"

const defaultLaunchStdoutCaptureBytes = 1 << 20

type LaunchOutputRecorder struct {
	stdout boundedOutputBuffer
	stream *EventStreamWriter
}

type LaunchOutputOptions struct {
	IncludeCommandOutput bool
	IncludeRawEvent      bool
	RecoverParseErrors   bool
}

func NewLaunchOutputRecorder(handler EventHandler) *LaunchOutputRecorder {
	return NewLaunchOutputRecorderWithOptions(handler, LaunchOutputOptions{IncludeCommandOutput: true, IncludeRawEvent: true})
}

func NewLaunchOutputRecorderWithOptions(handler EventHandler, options LaunchOutputOptions) *LaunchOutputRecorder {
	recorder := &LaunchOutputRecorder{
		stdout: boundedOutputBuffer{max: defaultLaunchStdoutCaptureBytes},
	}
	recorder.stream = NewEventStreamCollectorWithOptions(&recorder.stdout, handler, EventStreamOptions{
		IncludeCommandOutput: options.IncludeCommandOutput,
		IncludeRawEvent:      options.IncludeRawEvent,
		RecoverParseErrors:   options.RecoverParseErrors,
	})
	return recorder
}

func (r *LaunchOutputRecorder) StdoutWriter() io.Writer {
	if r == nil {
		return io.Discard
	}
	return r.stream
}

func (r *LaunchOutputRecorder) LaunchResult(stderr []byte, exitCode int) LaunchResult {
	if r == nil {
		return LaunchResult{Stderr: stderr, ExitCode: exitCode}
	}
	parsed, parseErr := r.stream.Finish()
	return LaunchResult{
		Stdout:          r.stdout.Bytes(),
		StdoutTruncated: r.stdout.Truncated(),
		Stderr:          stderr,
		ExitCode:        exitCode,
		ParsedResult:    &parsed,
		ParseErr:        parseErr,
	}
}

type boundedOutputBuffer struct {
	max       int
	buf       []byte
	truncated bool
}

func (b *boundedOutputBuffer) Write(p []byte) (int, error) {
	if b.max <= 0 {
		if len(p) > 0 {
			b.truncated = true
		}
		return len(p), nil
	}
	if len(p) >= b.max {
		b.buf = append(b.buf[:0], p[len(p)-b.max:]...)
		b.truncated = true
		return len(p), nil
	}
	if len(b.buf)+len(p) <= b.max {
		b.buf = append(b.buf, p...)
		return len(p), nil
	}
	overflow := len(b.buf) + len(p) - b.max
	copy(b.buf, b.buf[overflow:])
	b.buf = b.buf[:len(b.buf)-overflow]
	b.buf = append(b.buf, p...)
	b.truncated = true
	return len(p), nil
}

func (b *boundedOutputBuffer) Bytes() []byte {
	if len(b.buf) == 0 {
		return nil
	}
	return append([]byte(nil), b.buf...)
}

func (b *boundedOutputBuffer) Truncated() bool {
	return b.truncated
}
