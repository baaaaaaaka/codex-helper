package transcript

import (
	"bufio"
	"errors"
	"io"
	"os"
	"time"
)

// MaxLine is the longest record kept in memory. Longer lines (huge tool
// outputs) are skipped without being buffered.
const MaxLine = 16 << 20

// Line is one complete record line (without its newline).
type Line struct {
	Off, End int64 // byte range of the line including its newline
	Data     []byte
	Oversize bool // longer than MaxLine; Data is nil
	// Cut: a long record classified from its head (a tool output, say);
	// Data is only its first cutHead bytes, the rest was not buffered
	// (B6). ParseLine handles it.
	Cut bool
}

// Read returns the complete lines of f that start at or after off, reading
// about maxBytes (it always finishes the line it is in). If off is not on a
// line boundary (the byte before it is not a newline), the partial line is
// skipped: reading starts after the next newline. A last line without a
// newline is incomplete and is left for a later call. next is the offset
// after the last returned (or skipped) complete line.
func Read(f *os.File, off, maxBytes int64) (lines []Line, next int64, err error) {
	if off < 0 {
		off = 0
	}
	next = off
	align := false
	if off > 0 {
		var b [1]byte
		if _, err := f.ReadAt(b[:], off-1); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, off, nil
			}
			return nil, off, err
		}
		align = b[0] != '\n'
	}
	r := bufio.NewReaderSize(io.NewSectionReader(f, off, 1<<62), 64<<10)
	pos := off
	var read int64
	for maxBytes <= 0 || read < maxBytes {
		start := pos
		data, n, over, cut, complete, rerr := readLine(r)
		pos += n
		if !complete {
			// Incomplete tail (no newline yet): not consumed.
			return lines, next, ignoreEOF(rerr)
		}
		read += n
		next = pos
		if align {
			align = false
			continue
		}
		l := Line{Off: start, End: pos, Oversize: over, Cut: cut}
		if !over {
			l.Data = data
		}
		lines = append(lines, l)
		if rerr != nil {
			return lines, next, ignoreEOF(rerr)
		}
	}
	return lines, next, nil
}

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// readLine reads up to and including the next newline. It returns the line
// without the newline (nil when longer than MaxLine), the bytes consumed,
// whether a newline was found. A long line whose head classifies it
// (parseHead) is cut: only the head is kept, the rest is read through
// without being buffered (B6: a 2 MB tool output no longer allocates
// about 5 MB).
func readLine(r *bufio.Reader) (data []byte, n int64, over, cut, complete bool, err error) {
	var buf []byte
	checked := false // the head was looked at (a long line's head is final)
	for {
		chunk, e := r.ReadSlice('\n')
		n += int64(len(chunk))
		full := errors.Is(e, bufio.ErrBufferFull)
		switch {
		case over:
		case cut:
			if n > MaxLine {
				over, cut, buf = true, false, nil // as before: an oversize line is not parsed
			}
		case int64(len(buf)+len(chunk)) > MaxLine:
			over, buf = true, nil
		case full && len(buf) == 0 && len(chunk) > cheapRecordAt:
			checked = true
			if _, ok := parseHead(chunk, true); ok {
				buf, cut = append([]byte(nil), chunk[:cutHead]...), true
			} else {
				buf = append(buf, chunk...)
			}
		default:
			buf = append(buf, chunk...)
			if full && !checked && len(buf) > cheapRecordAt {
				checked = true
				if _, ok := parseHead(buf, true); ok {
					buf, cut = append([]byte(nil), buf[:cutHead]...), true
				}
			}
		}
		switch {
		case e == nil:
			if !over && !cut {
				buf = buf[:len(buf)-1]
				if len(buf) > 0 && buf[len(buf)-1] == '\r' {
					buf = buf[:len(buf)-1]
				}
			}
			return buf, n, over, cut, true, nil
		case full:
			continue
		default:
			return nil, n, over, cut, false, e
		}
	}
}

// FileInfo is what the mirror compares between scans.
type FileInfo struct {
	Size  int64
	MTime time.Time
	Inode int64
}

// Stat returns the size, modification time and file identity of path: the
// inode on Unix, the NTFS file index on Windows (0 when unknown).
func Stat(path string) (FileInfo, error) {
	st, err := os.Stat(path)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{Size: st.Size(), MTime: st.ModTime(), Inode: fileID(path, st)}, nil
}

// Rewritten reports whether a file was replaced or truncated since it was
// last read up to offset with inode inode: only a size below the offset or
// an inode change counts (a size or mtime change alone is an append).
func Rewritten(cur FileInfo, offset, inode int64) bool {
	if inode != 0 && cur.Inode != 0 && cur.Inode != inode {
		return true
	}
	return cur.Size < offset
}
