package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type driveUploadSession struct {
	UploadURL          string   `json:"uploadUrl"`
	ExpirationDateTime string   `json:"expirationDateTime"`
	NextExpectedRanges []string `json:"nextExpectedRanges"`
}

type driveUploadChunkResponse struct {
	NextExpectedRanges []string `json:"nextExpectedRanges"`
}

type transferProgress struct {
	lastProgress atomic.Int64
	stalled      atomic.Bool
}

func newTransferProgress() *transferProgress {
	progress := &transferProgress{}
	progress.mark()
	return progress
}

func (p *transferProgress) mark() {
	if p != nil {
		p.lastProgress.Store(time.Now().UnixNano())
	}
}

func (p *transferProgress) last() time.Time {
	if p == nil {
		return time.Now()
	}
	nanos := p.lastProgress.Load()
	if nanos <= 0 {
		return time.Now()
	}
	return time.Unix(0, nanos)
}

type transferProgressReadCloser struct {
	reader   io.ReadCloser
	progress *transferProgress
}

func (r transferProgressReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.progress.mark()
	}
	return n, err
}

func (r transferProgressReadCloser) Close() error {
	return r.reader.Close()
}

type transferProgressBody struct {
	body     io.ReadCloser
	progress *transferProgress
	stop     func()
	idle     time.Duration
}

func (b *transferProgressBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if n > 0 {
		b.progress.mark()
	}
	if err != nil && b.progress.stalled.Load() {
		return n, &transferStallError{Idle: b.idle}
	}
	return n, err
}

func (b *transferProgressBody) Close() error {
	err := b.body.Close()
	if b.stop != nil {
		b.stop()
	}
	return err
}

type transferStallError struct {
	Idle time.Duration
}

func (e *transferStallError) Error() string {
	return fmt.Sprintf("Graph file transfer stalled for %s without progress", e.Idle)
}

func (g *GraphClient) transferIdle() time.Duration {
	if g != nil && g.transferIdleTimeout > 0 {
		return g.transferIdleTimeout
	}
	return defaultTransferIdleTimeout
}

func (g *GraphClient) transferChunk() int64 {
	if g != nil && g.transferChunkSize > 0 {
		return g.transferChunkSize
	}
	return defaultUploadSessionChunkSize
}

func (g *GraphClient) transferRetries() int {
	if g != nil && g.transferMaxRetries >= 0 {
		return g.transferMaxRetries
	}
	return defaultTransferMaxRetries
}

func (g *GraphClient) singlePutLimit() int64 {
	if g != nil && g.singlePutMaxBytes > 0 {
		return g.singlePutMaxBytes
	}
	return maxSingleDriveItemBytes
}

func (g *GraphClient) transferHTTPClient() *http.Client {
	client := *g.httpClient()
	// The normal Graph client has a 30-second whole-request timeout. A file
	// transfer must not inherit it: a healthy upload can take much longer.
	// The request-local idle watchdog below still cancels a genuinely stalled
	// transfer.
	client.Timeout = 0
	return &client
}

func (g *GraphClient) doTransferRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	idle := g.transferIdle()
	attemptCtx, cancel := context.WithCancel(ctx)
	progress := newTransferProgress()
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: progress.mark,
	}
	request := req.WithContext(httptrace.WithClientTrace(attemptCtx, trace))
	if request.Body != nil {
		request.Body = transferProgressReadCloser{reader: request.Body, progress: progress}
	}

	done := make(chan struct{})
	var stopOnce atomic.Bool
	stop := func() {
		if stopOnce.Swap(true) {
			return
		}
		close(done)
		cancel()
	}
	go func() {
		interval := idle / 4
		if interval < 10*time.Millisecond {
			interval = 10 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if time.Since(progress.last()) >= idle {
					progress.stalled.Store(true)
					cancel()
					return
				}
			case <-done:
				return
			}
		}
	}()

	response, err := g.transferHTTPClient().Do(request)
	if err != nil {
		stop()
		if progress.stalled.Load() {
			return nil, &transferStallError{Idle: idle}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	response.Body = &transferProgressBody{body: response.Body, progress: progress, stop: stop, idle: idle}
	return response, nil
}

func (g *GraphClient) UploadDriveItemFromFile(ctx context.Context, folder string, name string, filePath string, size int64, contentType string) (DriveItem, error) {
	return g.uploadDriveItemFromFileWithOptions(ctx, folder, name, filePath, size, contentType, graphRequestOptions{})
}

func (g *GraphClient) UploadDriveItemFromFileWithoutRateLimitRetry(ctx context.Context, folder string, name string, filePath string, size int64, contentType string) (DriveItem, error) {
	return g.uploadDriveItemFromFileWithOptions(ctx, folder, name, filePath, size, contentType, graphRequestOptions{returnRateLimitWithoutRetry: true})
}

func (g *GraphClient) uploadDriveItemFromFileWithOptions(ctx context.Context, folder string, name string, filePath string, size int64, contentType string, opts graphRequestOptions) (DriveItem, error) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return DriveItem{}, fmt.Errorf("upload file path is required")
	}
	f, err := os.Open(filePath)
	if err != nil {
		return DriveItem{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return DriveItem{}, err
	}
	if size < 0 {
		size = info.Size()
	}
	if info.Size() != size {
		return DriveItem{}, fmt.Errorf("upload file size changed from %d to %d bytes", size, info.Size())
	}
	if size > maxTeamsTransferBytes {
		return DriveItem{}, fmt.Errorf("refusing to upload file larger than %d bytes", maxTeamsTransferBytes)
	}
	if size <= g.singlePutLimit() {
		return g.uploadDriveItemSingleFileWithOptions(ctx, folder, name, f, size, contentType, opts)
	}
	session, err := g.createDriveUploadSessionWithOptions(ctx, folder, name, opts)
	if err != nil {
		return DriveItem{}, err
	}
	return g.uploadDriveItemSession(ctx, session, f, size, opts)
}

func (g *GraphClient) uploadDriveItemSingleFileWithOptions(ctx context.Context, folder string, name string, file *os.File, size int64, contentType string, opts graphRequestOptions) (DriveItem, error) {
	path, err := meDriveRootContentPath(folder, name)
	if err != nil {
		return DriveItem{}, err
	}
	token, err := g.auth.AccessToken(ctx, g.out, false)
	if err != nil {
		return DriveItem{}, err
	}
	refreshed := false
	retries := 0
	for {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return DriveItem{}, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, g.graphURL(path), io.NewSectionReader(file, 0, size))
		if err != nil {
			return DriveItem{}, err
		}
		req.ContentLength = size
		req.Header.Set("Authorization", "Bearer "+token)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := g.doTransferRequest(ctx, req)
		if err != nil {
			return DriveItem{}, err
		}
		if resp.StatusCode == http.StatusUnauthorized && !refreshed {
			discardAndClose(resp.Body)
			token, err = g.auth.RefreshAccessToken(ctx)
			if err != nil {
				return DriveItem{}, err
			}
			refreshed = true
			continue
		}
		retryable := shouldRetryTransferStatus(resp.StatusCode)
		if opts.returnRateLimitWithoutRetry && resp.StatusCode == http.StatusTooManyRequests {
			retryable = false
		}
		if retryable && retries < g.transferRetries() {
			delay := g.retryDelay(resp, retries)
			discardAndClose(resp.Body)
			if err := g.sleepFor(ctx, delay); err != nil {
				return DriveItem{}, err
			}
			retries++
			continue
		}
		raw, readErr := readLimited(resp.Body, maxDriveItemJSONBytes)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return DriveItem{}, readErr
		}
		if closeErr != nil {
			return DriveItem{}, closeErr
		}
		if resp.StatusCode >= 400 {
			return DriveItem{}, graphStatusError(http.MethodPut, path, resp, raw)
		}
		var item DriveItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return DriveItem{}, err
		}
		if item.ID == "" {
			return DriveItem{}, fmt.Errorf("drive upload response did not include item id")
		}
		return item, nil
	}
}

func (g *GraphClient) createDriveUploadSessionWithOptions(ctx context.Context, folder string, name string, opts graphRequestOptions) (driveUploadSession, error) {
	path, err := meDriveRootUploadSessionPath(folder, name)
	if err != nil {
		return driveUploadSession{}, err
	}
	var session driveUploadSession
	body := map[string]any{
		"item": map[string]any{
			"@microsoft.graph.conflictBehavior": "replace",
			"name":                              name,
		},
	}
	if err := g.doWithOptions(ctx, http.MethodPost, path, body, &session, opts); err != nil {
		return driveUploadSession{}, err
	}
	if err := g.validateUploadSessionURL(session.UploadURL); err != nil {
		return driveUploadSession{}, err
	}
	return session, nil
}

func (g *GraphClient) validateUploadSessionURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return fmt.Errorf("Graph upload session returned an invalid upload URL")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return fmt.Errorf("Graph upload session returned an unsupported upload URL scheme")
	}
	base, baseErr := url.Parse(g.baseURL)
	if parsed.Scheme == "http" && (baseErr != nil || base == nil || parsed.Host != base.Host) {
		return fmt.Errorf("Graph upload session returned an insecure upload URL")
	}
	return nil
}

func (g *GraphClient) uploadDriveItemSession(ctx context.Context, session driveUploadSession, file *os.File, size int64, opts graphRequestOptions) (DriveItem, error) {
	offset := int64(0)
	chunkSize := g.transferChunk()
	if chunkSize <= 0 || chunkSize >= 60*1024*1024 {
		return DriveItem{}, fmt.Errorf("invalid upload session chunk size %d", chunkSize)
	}
	for offset < size {
		length := chunkSize
		if remaining := size - offset; remaining < length {
			length = remaining
		}
		item, next, err := g.uploadDriveItemSessionChunk(ctx, session.UploadURL, file, offset, length, size, opts)
		if err != nil {
			return DriveItem{}, err
		}
		if item.ID != "" {
			return item, nil
		}
		if next <= offset || next > size {
			next = offset + length
		}
		offset = next
	}
	return DriveItem{}, fmt.Errorf("upload session completed without drive item metadata")
}

func (g *GraphClient) uploadDriveItemSessionChunk(ctx context.Context, uploadURL string, file *os.File, offset int64, length int64, total int64, opts graphRequestOptions) (DriveItem, int64, error) {
	retries := 0
	for {
		body := io.NewSectionReader(file, offset, length)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, body)
		if err != nil {
			return DriveItem{}, 0, err
		}
		req.ContentLength = length
		req.Header.Set("Content-Length", strconv.FormatInt(length, 10))
		req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, total))
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := g.doTransferRequest(ctx, req)
		if err != nil {
			if next, queryErr := g.queryUploadSessionOffset(ctx, uploadURL); queryErr == nil && next > offset && next <= total {
				return DriveItem{}, next, nil
			}
			if retries < g.transferRetries() && ctx.Err() == nil {
				retries++
				if err := g.sleepFor(ctx, g.retryDelay(nil, retries-1)); err != nil {
					return DriveItem{}, 0, err
				}
				continue
			}
			return DriveItem{}, 0, err
		}
		retryable := shouldRetryTransferStatus(resp.StatusCode)
		if opts.returnRateLimitWithoutRetry && resp.StatusCode == http.StatusTooManyRequests {
			retryable = false
		}
		if retryable && retries < g.transferRetries() {
			delay := g.retryDelay(resp, retries)
			discardAndClose(resp.Body)
			if err := g.sleepFor(ctx, delay); err != nil {
				return DriveItem{}, 0, err
			}
			retries++
			continue
		}
		raw, readErr := readLimited(resp.Body, maxDriveItemJSONBytes)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return DriveItem{}, 0, readErr
		}
		if closeErr != nil {
			return DriveItem{}, 0, closeErr
		}
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			if next, queryErr := g.queryUploadSessionOffset(ctx, uploadURL); queryErr == nil && next > offset && next <= total {
				return DriveItem{}, next, nil
			}
		}
		if resp.StatusCode >= 400 {
			return DriveItem{}, 0, graphStatusError(http.MethodPut, "/drive/upload-session", resp, raw)
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			var item DriveItem
			if err := json.Unmarshal(raw, &item); err != nil {
				return DriveItem{}, 0, err
			}
			if item.ID == "" {
				return DriveItem{}, 0, fmt.Errorf("final upload session response did not include item id")
			}
			return item, total, nil
		}
		var payload driveUploadChunkResponse
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &payload); err != nil {
				return DriveItem{}, 0, err
			}
		}
		next := nextUploadOffset(payload.NextExpectedRanges, offset+length)
		return DriveItem{}, next, nil
	}
}

func (g *GraphClient) queryUploadSessionOffset(ctx context.Context, uploadURL string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uploadURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := g.doTransferRequest(ctx, req)
	if err != nil {
		return 0, err
	}
	raw, readErr := readLimited(resp.Body, maxDriveItemJSONBytes)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return 0, readErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if resp.StatusCode >= 400 {
		return 0, graphStatusError(http.MethodGet, "/drive/upload-session", resp, raw)
	}
	var session driveUploadSession
	if err := json.Unmarshal(raw, &session); err != nil {
		return 0, err
	}
	next := nextUploadOffset(session.NextExpectedRanges, -1)
	if next < 0 {
		return 0, fmt.Errorf("upload session status did not include nextExpectedRanges")
	}
	return next, nil
}

func nextUploadOffset(ranges []string, fallback int64) int64 {
	best := int64(-1)
	for _, raw := range ranges {
		start, _, ok := strings.Cut(strings.TrimSpace(raw), "-")
		if !ok {
			continue
		}
		offset, err := strconv.ParseInt(strings.TrimSpace(start), 10, 64)
		if err != nil || offset < 0 {
			continue
		}
		if best < 0 || offset < best {
			best = offset
		}
	}
	if best >= 0 {
		return best
	}
	return fallback
}

func shouldRetryTransferStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func (g *GraphClient) DownloadSharedDriveItemContentToFile(ctx context.Context, rawURL string, destination string) (string, int64, error) {
	return g.downloadSharedDriveItemContentToFileWithOptions(ctx, rawURL, destination, graphRequestOptions{})
}

func (g *GraphClient) DownloadSharedDriveItemContentToFileWithoutRateLimitRetry(ctx context.Context, rawURL string, destination string) (string, int64, error) {
	return g.downloadSharedDriveItemContentToFileWithOptions(ctx, rawURL, destination, graphRequestOptions{returnRateLimitWithoutRetry: true})
}

func (g *GraphClient) downloadSharedDriveItemContentToFileWithOptions(ctx context.Context, rawURL string, destination string, opts graphRequestOptions) (string, int64, error) {
	shareID := graphShareID(rawURL)
	if shareID == "" {
		return "", 0, fmt.Errorf("sharing URL is required")
	}
	destination = strings.TrimSpace(destination)
	if destination == "" {
		return "", 0, fmt.Errorf("download destination is required")
	}
	path := "/shares/" + url.PathEscape(shareID) + "/driveItem/content"
	token, err := g.auth.AccessToken(ctx, g.out, false)
	if err != nil {
		return "", 0, err
	}
	retries := 0
	refreshed := false
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.graphURL(path), nil)
		if err != nil {
			return "", 0, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := g.doTransferRequest(ctx, req)
		if err != nil {
			return "", 0, err
		}
		if resp.StatusCode == http.StatusUnauthorized && !refreshed {
			discardAndClose(resp.Body)
			token, err = g.auth.RefreshAccessToken(ctx)
			if err != nil {
				return "", 0, err
			}
			refreshed = true
			continue
		}
		retryable := shouldRetryTransferStatus(resp.StatusCode)
		if opts.returnRateLimitWithoutRetry && resp.StatusCode == http.StatusTooManyRequests {
			retryable = false
		}
		if retryable && retries < g.transferRetries() {
			delay := g.retryDelay(resp, retries)
			discardAndClose(resp.Body)
			if err := g.sleepFor(ctx, delay); err != nil {
				return "", 0, err
			}
			retries++
			continue
		}
		if resp.StatusCode >= 400 {
			raw, readErr := readLimited(resp.Body, maxDriveItemJSONBytes)
			_ = resp.Body.Close()
			if readErr != nil {
				return "", 0, readErr
			}
			return "", 0, graphStatusError(http.MethodGet, path, resp, raw)
		}
		contentLength := resp.ContentLength
		if contentLength > maxTeamsTransferBytes {
			_ = resp.Body.Close()
			return "", 0, fmt.Errorf("refusing to download file larger than %d bytes", maxTeamsTransferBytes)
		}
		file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			_ = resp.Body.Close()
			return "", 0, err
		}
		written, copyErr := io.Copy(file, io.LimitReader(resp.Body, maxTeamsTransferBytes+1))
		closeFileErr := file.Close()
		closeBodyErr := resp.Body.Close()
		if copyErr != nil {
			_ = os.Remove(destination)
			return "", 0, copyErr
		}
		if closeFileErr != nil {
			_ = os.Remove(destination)
			return "", 0, closeFileErr
		}
		if closeBodyErr != nil {
			_ = os.Remove(destination)
			return "", 0, closeBodyErr
		}
		if written > maxTeamsTransferBytes || contentLength >= 0 && written != contentLength {
			_ = os.Remove(destination)
			return "", 0, fmt.Errorf("downloaded file size mismatch: got %d bytes, response declared %d", written, contentLength)
		}
		return resp.Header.Get("Content-Type"), written, nil
	}
}
