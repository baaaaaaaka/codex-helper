package codexhistory

import (
	"bufio"
	"bytes"
	"context"
	"io"
)

const sessionSummaryMaxBytes int64 = 128 * 1024

type sessionSummaryReader struct {
	catalog *catalogSQLiteStore
}

func readSessionFileSummaryCachedContext(ctx context.Context, path string) (sessionFileMeta, error) {
	return (&sessionSummaryReader{}).read(ctx, path)
}

func (r *sessionSummaryReader) read(ctx context.Context, path string) (sessionFileMeta, error) {
	if err := ctx.Err(); err != nil {
		return sessionFileMeta{}, err
	}
	entry, info, hit, err := getSessionFileCacheEntry(path)
	if err != nil {
		return sessionFileMeta{}, err
	}
	if hit {
		if entry.hasMeta {
			meta := entry.meta
			meta.ModifiedAt = info.ModTime()
			return meta, nil
		}
		if entry.hasSummary {
			return entry.summary, nil
		}
	}
	if r.catalog != nil {
		cached, found, err := r.catalog.loadSessionMeta(ctx, path)
		if isContextError(err) {
			return sessionFileMeta{}, err
		}
		if err != nil {
			// A catalog failure affects this pass, not just one session. Avoid
			// paying the SQLite busy timeout for every remaining file.
			r.catalog = nil
		} else if found && cached.parsedOffset == info.Size() && matchesFileInfo(path, info, cached.fileKey) {
			entry.meta, entry.hasMeta = cached.meta, true
			setSessionFileCacheEntry(path, entry)
			meta := cached.meta
			meta.ModifiedAt = info.ModTime()
			return meta, nil
		}
	}
	f, err := openSessionMetaFile(path)
	if err != nil {
		return sessionFileMeta{}, err
	}
	defer f.Close()
	meta, err := readSessionSummary(ctx, f, info.Size())
	if err != nil {
		return meta, err
	}
	// A directory only needs recency, not the timestamp of every event.
	meta.ModifiedAt = info.ModTime()
	entry.summary, entry.hasSummary = meta, true
	setSessionFileCacheEntry(path, entry)
	return meta, nil
}

func readSessionSummary(ctx context.Context, source io.Reader, size int64) (sessionFileMeta, error) {
	var meta sessionFileMeta
	reader := bufio.NewReaderSize(io.LimitReader(source, min(size, sessionSummaryMaxBytes)), 4096)
	var consumed int64
	for {
		if err := ctx.Err(); err != nil {
			return meta, err
		}
		line, err := reader.ReadBytes('\n')
		consumed += int64(len(line))
		if err != nil && err != io.EOF {
			return meta, err
		}
		// Never decode a JSON record cut by the byte budget. A complete final
		// record without a newline is still supported for small sessions.
		if bytes.HasSuffix(line, []byte{'\n'}) || consumed == size {
			if line = bytes.TrimSpace(line); len(line) > 0 {
				processMetaLine(line, &meta)
			}
		}
		if err == io.EOF || meta.FirstPrompt != "" {
			break
		}
	}
	if consumed < size {
		meta.MessageCount = -1 // unknown, rather than an exact zero or partial count
	}
	return meta, ctx.Err()
}
