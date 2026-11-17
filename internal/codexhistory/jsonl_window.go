package codexhistory

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
)

// readJSONLWindow shares line alignment, cancellation and bounded-window EOF
// handling. A valid final record need not end in a newline. The caller decides
// whether that record is provisional and which complete offset to checkpoint.
func readJSONLWindow(ctx context.Context, source io.ReaderAt, offset, size int64, visit func([]byte) error) error {
	if offset < 0 || size < 0 {
		return errors.New("invalid JSONL window")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	aligned := offset == 0
	if offset > 0 {
		var previous [1]byte
		n, err := source.ReadAt(previous[:], offset-1)
		if n != 1 {
			if err != nil {
				return err
			}
			return io.ErrUnexpectedEOF
		}
		aligned = previous[0] == '\n'
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(source, offset, size), 64*1024)
	var consumed int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := reader.ReadBytes('\n')
		consumed += int64(len(line))
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF && consumed != size {
			return io.ErrUnexpectedEOF
		}
		if aligned {
			if line = bytes.TrimSpace(line); len(line) > 0 {
				if err := visit(line); err != nil {
					return err
				}
			}
		}
		aligned = true
		if err == io.EOF {
			return ctx.Err()
		}
	}
}
