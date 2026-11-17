package codexhistory

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
)

const sessionSummaryMaxBytes int64 = 128 * 1024

type sessionSummaryReader struct {
	catalog *catalogSQLiteStore
}

func readSessionFileSummaryCachedContext(ctx context.Context, path string) (sessionFileMeta, error) {
	store, _ := currentCatalogSQLiteStore(path)
	return (&sessionSummaryReader{catalog: store}).read(ctx, path)
}

func (r *sessionSummaryReader) read(ctx context.Context, path string) (sessionFileMeta, error) {
	if err := ctx.Err(); err != nil {
		return sessionFileMeta{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return sessionFileMeta{}, err
	}
	if r.catalog != nil {
		cached, found, err := loadCatalogMetadata(ctx, r.catalog, path)
		if isContextError(err) {
			return sessionFileMeta{}, err
		}
		if err != nil {
			// A catalog failure affects this pass, not just one session. Avoid
			// paying the SQLite busy timeout for every remaining file.
			r.catalog = nil
		} else if found && (cached.parsedOffset == info.Size() || cached.isSummary) && matchesFileInfo(path, info, cached.fileKey) {
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
	meta, covered, err := readSessionSummary(ctx, f, info.Size())
	if err != nil {
		return meta, err
	}
	// A directory only needs recency, not the timestamp of every event.
	meta.ModifiedAt = info.ModTime()
	cached := catalogSessionMetaEntry{path: path, fileKey: newFileCacheKey(path, info), parsedOffset: covered, isSummary: true, meta: meta}
	if !stageCatalogSessionMeta(ctx, cached) {
		if err := writeCatalogSessionMeta(ctx, cached); isContextError(err) {
			return meta, err
		}
	}
	return meta, nil
}

func readSessionSummary(ctx context.Context, source io.Reader, size int64) (sessionFileMeta, int64, error) {
	var meta sessionFileMeta
	reader := bufio.NewReaderSize(io.LimitReader(source, min(size, sessionSummaryMaxBytes)), 4096)
	var consumed int64
	for {
		if err := ctx.Err(); err != nil {
			return meta, consumed, err
		}
		line, err := reader.ReadBytes('\n')
		consumed += int64(len(line))
		if err != nil && err != io.EOF {
			return meta, consumed, err
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
	return meta, consumed, ctx.Err()
}
