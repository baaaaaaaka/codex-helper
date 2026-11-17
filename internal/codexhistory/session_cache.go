package codexhistory

import (
	"context"
	"errors"
	"os"
)

func readSessionFileMetaCached(filePath string) (sessionFileMeta, error) {
	return readSessionFileMetaCachedContext(context.Background(), filePath)
}

func readSessionFileMetaCachedContext(ctx context.Context, filePath string) (sessionFileMeta, error) {
	if err := ctx.Err(); err != nil {
		return sessionFileMeta{}, err
	}
	info, err := os.Stat(filePath)
	if err != nil {
		if !stageCatalogSessionMetaDelete(ctx, filePath) {
			if delErr := deleteCatalogSessionMeta(ctx, filePath); isContextError(delErr) {
				return sessionFileMeta{}, delErr
			}
		}
		return sessionFileMeta{}, err
	}
	store, storeErr := currentCatalogSQLiteStore(filePath)
	if isContextError(storeErr) {
		return sessionFileMeta{}, storeErr
	}
	var cached catalogSessionMetaEntry
	var found bool
	if storeErr == nil {
		cached, found, err = loadCatalogMetadata(ctx, store, filePath)
		if isContextError(err) {
			return sessionFileMeta{}, err
		}
		if err != nil {
			found = false
		}
	}
	if found && !cached.isSummary && cached.parsedOffset == info.Size() && matchesFileInfo(filePath, info, cached.fileKey) {
		return sessionMetaWithFallbackTime(cached.meta, info), nil
	}

	completeOffset, complete := sessionPreviewCompleteOffset(filePath, info)
	if !complete {
		return sessionFileMeta{}, errors.New("cannot locate complete metadata boundary")
	}
	meta := sessionFileMeta{}
	startOffset := int64(0)
	appendable := found && !cached.isSummary && canAppendCacheFile(filePath, info, cached.fileKey, cached.parsedOffset, cached.prefixTailHash, cached.prefixTailSize)
	if appendable {
		meta = cached.meta
		startOffset = cached.parsedOffset
	}
	if startOffset < completeOffset {
		meta, err = readSessionFileMetaWindowRawContext(ctx, filePath, startOffset, completeOffset-startOffset, meta)
		if err != nil {
			return meta, err
		}
	}
	if !unchangedJSONLPath(filePath, info) {
		return sessionFileMeta{}, errors.New("metadata source changed during read")
	}
	// An incomplete final line never advances or repeatedly rewrites the prefix.
	if !appendable || completeOffset > cached.parsedOffset {
		hash, hashSize, ok := sessionPreviewPrefixTailHash(filePath, completeOffset)
		if !ok {
			return meta, errors.New("cannot verify metadata prefix")
		}
		entry := catalogSessionMetaEntry{path: filePath, fileKey: newFileCacheKey(filePath, info), parsedOffset: completeOffset, prefixTailHash: hash, prefixTailSize: hashSize, meta: meta}
		if !stageCatalogSessionMeta(ctx, entry) {
			if err := writeCatalogSessionMeta(ctx, entry); isContextError(err) {
				return meta, err
			}
		}
	}
	if completeOffset < info.Size() {
		meta, err = readSessionFileMetaWindowRawContext(ctx, filePath, completeOffset, info.Size()-completeOffset, meta)
		if err != nil {
			return meta, err
		}
		if !unchangedJSONLPath(filePath, info) {
			return sessionFileMeta{}, errors.New("metadata source changed during tail read")
		}
	}
	return sessionMetaWithFallbackTime(meta, info), nil
}

func SessionFileIsSubagentContext(ctx context.Context, filePath string) (bool, error) {
	meta, err := readSessionFileMetaCachedContext(ctx, filePath)
	if err != nil {
		return false, err
	}
	return meta.IsSubagent, nil
}
