package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
)

type driveUploadSession struct {
	UploadURL          string   `json:"uploadUrl"`
	ExpirationDateTime string   `json:"expirationDateTime"`
	NextExpectedRanges []string `json:"nextExpectedRanges"`
}

type driveUploadSessionCheckpoint struct {
	UploadURL          string
	ExpirationDateTime string
	Offset             int64
}

type driveUploadChunkResponse struct {
	NextExpectedRanges []string `json:"nextExpectedRanges"`
}

type transferPartMetadata struct {
	Version int    `json:"version"`
	ShareID string `json:"share_id"`
}

const (
	transferPartMetadataVersion = 1
	downloadPartLockWait        = 100 * time.Millisecond
	downloadPartialRetention    = 24 * time.Hour
)

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
	if g == nil {
		return http.DefaultClient
	}
	if g.transferClientOnce == nil {
		// Tests and a few low-level callers construct GraphClient literals. The
		// normal constructor initializes this pointer, so copied production
		// clients still share one initialization state.
		g.transferClientOnce = &sync.Once{}
	}
	g.transferClientOnce.Do(func() {
		client := *g.httpClient()
		// The normal Graph client has a 30-second whole-request timeout. A file
		// transfer must not inherit it: a healthy upload can take much longer.
		// The request-local idle watchdog below still cancels a genuinely stalled
		// transfer.
		client.Timeout = 0
		transport := client.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		if transport, ok := transport.(*http.Transport); ok {
			cloned := transport.Clone()
			if cloned.DialContext == nil {
				dialer := &net.Dialer{Timeout: defaultTransferDialTimeout}
				cloned.DialContext = dialer.DialContext
			}
			cloned.TLSHandshakeTimeout = defaultTransferDialTimeout
			cloned.ResponseHeaderTimeout = defaultTransferHeaderTimeout
			// Upload sessions are sequential per file. Keep the shared pool
			// bounded so retries or multiple chats cannot create a connection
			// storm against Graph/OneDrive.
			cloned.MaxConnsPerHost = 4
			cloned.MaxIdleConnsPerHost = 4
			client.Transport = cloned
		}
		g.transferClient = &client
	})
	return g.transferClient
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
	return g.uploadDriveItemFromFileWithCheckpoint(ctx, folder, name, filePath, size, contentType, opts, nil, nil)
}

func (g *GraphClient) uploadDriveItemFromFileWithCheckpoint(ctx context.Context, folder string, name string, filePath string, size int64, contentType string, opts graphRequestOptions, checkpoint *driveUploadSessionCheckpoint, persist func(driveUploadSessionCheckpoint) error) (DriveItem, error) {
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
	// An upload-session URL is deliberately not retried forever. A 401/403/404/410
	// means the server-side session is gone; recreate it a bounded number of
	// times and restart from byte zero. Other failures stay fail-closed and are
	// surfaced to the outbox retry policy.
	var session *driveUploadSession
	var offset int64
	if checkpoint != nil && strings.TrimSpace(checkpoint.UploadURL) != "" && checkpoint.Offset >= 0 && checkpoint.Offset < size && g.uploadSessionCheckpointUsable(*checkpoint) {
		candidate := driveUploadSession{
			UploadURL:          strings.TrimSpace(checkpoint.UploadURL),
			ExpirationDateTime: strings.TrimSpace(checkpoint.ExpirationDateTime),
		}
		if err := g.validateUploadSessionURL(candidate.UploadURL); err == nil {
			if next, queryErr := g.queryUploadSessionOffset(ctx, candidate.UploadURL, size); queryErr == nil {
				if next >= 0 && next <= size {
					session = &candidate
					offset = next
				}
			} else if !isExpiredUploadSessionError(queryErr) {
				return DriveItem{}, queryErr
			}
		}
	}
	for sessionRecreates := 0; ; sessionRecreates++ {
		if session == nil {
			created, err := g.createDriveUploadSessionWithOptions(ctx, folder, name, opts)
			if err != nil {
				return DriveItem{}, err
			}
			session = &created
			offset = 0
		}
		if persist != nil {
			if err := persist(driveUploadSessionCheckpoint{UploadURL: session.UploadURL, ExpirationDateTime: session.ExpirationDateTime, Offset: offset}); err != nil {
				return DriveItem{}, err
			}
		}
		item, err := g.uploadDriveItemSession(ctx, *session, f, size, opts, offset, persist)
		if err == nil {
			return item, nil
		}
		if !isExpiredUploadSessionError(err) || sessionRecreates >= g.transferRetries() || ctx.Err() != nil {
			return DriveItem{}, err
		}
		if err := g.sleepFor(ctx, g.retryDelay(nil, sessionRecreates)); err != nil {
			return DriveItem{}, err
		}
		session = nil
		offset = 0
	}
}

func (g *GraphClient) uploadSessionCheckpointUsable(checkpoint driveUploadSessionCheckpoint) bool {
	if checkpoint.Offset < 0 {
		return false
	}
	expiresAt := strings.TrimSpace(checkpoint.ExpirationDateTime)
	if expiresAt == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339Nano, expiresAt)
	return err == nil && time.Now().Before(t)
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
			if ctx.Err() != nil {
				return DriveItem{}, ctx.Err()
			}
			if retries < g.transferRetries() {
				delay := g.retryDelay(nil, retries)
				if err := g.sleepFor(ctx, delay); err != nil {
					return DriveItem{}, err
				}
				retries++
				continue
			}
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

func (g *GraphClient) uploadDriveItemSession(ctx context.Context, session driveUploadSession, file *os.File, size int64, opts graphRequestOptions, initialOffset int64, persist func(driveUploadSessionCheckpoint) error) (DriveItem, error) {
	if initialOffset < 0 || initialOffset > size {
		return DriveItem{}, fmt.Errorf("upload session initial offset %d is invalid for file size %d", initialOffset, size)
	}
	offset := initialOffset
	chunkSize := g.transferChunk()
	if chunkSize <= 0 || chunkSize >= 60*1024*1024 {
		return DriveItem{}, fmt.Errorf("invalid upload session chunk size %d", chunkSize)
	}
	noProgress := 0
	for offset < size {
		length := chunkSize
		if remaining := size - offset; remaining < length {
			length = remaining
		}
		item, next, err := g.uploadDriveItemSessionChunk(ctx, session.UploadURL, file, offset, length, size, opts)
		if err != nil {
			return DriveItem{}, err
		}
		if next < 0 || next > size {
			return DriveItem{}, fmt.Errorf("upload session returned invalid next offset %d for file size %d", next, size)
		}
		if persist != nil {
			if err := persist(driveUploadSessionCheckpoint{UploadURL: session.UploadURL, ExpirationDateTime: session.ExpirationDateTime, Offset: next}); err != nil {
				return DriveItem{}, err
			}
		}
		if item.ID != "" {
			if item.Size > 0 && item.Size != size {
				return DriveItem{}, fmt.Errorf("upload session returned item size %d, want %d", item.Size, size)
			}
			return item, nil
		}
		if next == offset {
			noProgress++
			if noProgress > g.transferRetries()+1 {
				return DriveItem{}, fmt.Errorf("upload session made no progress at offset %d", offset)
			}
		} else {
			noProgress = 0
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
			if next, queryErr := g.queryUploadSessionOffset(ctx, uploadURL, total); queryErr == nil {
				if rangeErr := validateUploadSessionNextOffset(next, offset, length, total); rangeErr != nil {
					return DriveItem{}, 0, rangeErr
				}
				return DriveItem{}, next, nil
			} else if isExpiredUploadSessionError(queryErr) {
				// The body request may have failed before a response was
				// available, but the status query is authoritative when it says
				// that the pre-authenticated session no longer exists. Let the
				// bounded outer loop recreate it instead of retrying a dead URL.
				return DriveItem{}, 0, queryErr
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
			if next, queryErr := g.queryUploadSessionOffset(ctx, uploadURL, total); queryErr == nil {
				if rangeErr := validateUploadSessionNextOffset(next, offset, length, total); rangeErr != nil {
					return DriveItem{}, 0, rangeErr
				}
				return DriveItem{}, next, nil
			} else if isExpiredUploadSessionError(queryErr) {
				return DriveItem{}, 0, queryErr
			}
		}
		if resp.StatusCode >= 400 {
			return DriveItem{}, 0, graphStatusError(http.MethodPut, "/drive/upload-session", resp, raw)
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			if offset+length != total {
				return DriveItem{}, 0, fmt.Errorf("upload session reported completion at %d of %d bytes", offset+length, total)
			}
			var item DriveItem
			if err := json.Unmarshal(raw, &item); err != nil {
				return DriveItem{}, 0, err
			}
			if item.ID == "" {
				return DriveItem{}, 0, fmt.Errorf("final upload session response did not include item id")
			}
			return item, total, nil
		}
		if resp.StatusCode != http.StatusAccepted {
			return DriveItem{}, 0, fmt.Errorf("unexpected upload session response status %d", resp.StatusCode)
		}
		var payload driveUploadChunkResponse
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &payload); err != nil {
				return DriveItem{}, 0, err
			}
		}
		next, err := parseUploadSessionOffset(payload.NextExpectedRanges, total)
		if err != nil {
			return DriveItem{}, 0, err
		}
		if err := validateUploadSessionNextOffset(next, offset, length, total); err != nil {
			return DriveItem{}, 0, err
		}
		return DriveItem{}, next, nil
	}
}

func validateUploadSessionNextOffset(next int64, offset int64, length int64, total int64) error {
	if next < 0 || next > total {
		return fmt.Errorf("upload session returned invalid next offset %d for file size %d", next, total)
	}
	if next < offset {
		return fmt.Errorf("upload session moved backward from %d to %d", offset, next)
	}
	if next > offset+length {
		return fmt.Errorf("upload session advanced from %d beyond sent range ending at %d", next, offset+length)
	}
	return nil
}

func isExpiredUploadSessionError(err error) bool {
	var statusErr *GraphStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	return statusErr.StatusCode == http.StatusUnauthorized ||
		statusErr.StatusCode == http.StatusForbidden ||
		statusErr.StatusCode == http.StatusNotFound ||
		statusErr.StatusCode == http.StatusGone
}

func (g *GraphClient) queryUploadSessionOffset(ctx context.Context, uploadURL string, total int64) (int64, error) {
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
	if resp.StatusCode != http.StatusOK {
		return 0, graphStatusError(http.MethodGet, "/drive/upload-session", resp, raw)
	}
	var session driveUploadSession
	if err := json.Unmarshal(raw, &session); err != nil {
		return 0, err
	}
	return parseUploadSessionOffset(session.NextExpectedRanges, total)
}

func nextUploadOffset(ranges []string, fallback int64) int64 {
	// Kept as a small compatibility helper for callers/tests that only need to
	// inspect a Graph range list. Upload production paths use the strict parser
	// below and never fall back to a guessed offset.
	best, err := parseUploadSessionOffset(ranges, maxTeamsTransferBytes)
	if err == nil {
		return best
	}
	return fallback
}

func parseUploadSessionOffset(ranges []string, total int64) (int64, error) {
	if total < 0 {
		return 0, fmt.Errorf("upload session file size must not be negative")
	}
	best := int64(-1)
	for _, raw := range ranges {
		raw = strings.TrimSpace(raw)
		startText, endText, ok := strings.Cut(raw, "-")
		if !ok || strings.TrimSpace(startText) == "" {
			return 0, fmt.Errorf("upload session returned malformed nextExpectedRange %q", raw)
		}
		offset, err := strconv.ParseInt(strings.TrimSpace(startText), 10, 64)
		if err != nil || offset < 0 || offset > total {
			return 0, fmt.Errorf("upload session returned out-of-range nextExpectedRange %q", raw)
		}
		if strings.TrimSpace(endText) != "" {
			end, err := strconv.ParseInt(strings.TrimSpace(endText), 10, 64)
			if err != nil || end < offset || end >= total {
				return 0, fmt.Errorf("upload session returned invalid nextExpectedRange %q", raw)
			}
		}
		if best < 0 || offset < best {
			best = offset
		}
	}
	if best < 0 {
		return 0, fmt.Errorf("upload session status did not include nextExpectedRanges")
	}
	return best, nil
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

func (g *GraphClient) downloadSharedDriveItemContentToFileWithOptions(ctx context.Context, rawURL string, destination string, opts graphRequestOptions) (contentType string, size int64, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	shareID := graphShareID(rawURL)
	if shareID == "" {
		return "", 0, fmt.Errorf("sharing URL is required")
	}
	destination = strings.TrimSpace(destination)
	if destination == "" {
		return "", 0, fmt.Errorf("download destination is required")
	}
	path := "/shares/" + url.PathEscape(shareID) + "/driveItem/content"
	partPath := destination + ".part"
	partMetaPath := partPath + ".meta"
	partLockPath := partPath + ".lock"
	partLock := flock.New(partLockPath)
	lockCtx, cancelLock := context.WithTimeout(ctx, downloadPartLockWait)
	locked, err := partLock.TryLockContext(lockCtx, downloadPartLockWait/4)
	cancelLock()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return "", 0, fmt.Errorf("download destination is already being written: %s", destination)
		}
		return "", 0, err
	}
	if !locked {
		return "", 0, fmt.Errorf("download destination is already being written: %s", destination)
	}
	published := false
	defer func() {
		// Unlock before removing the lock file. Windows does not allow an open
		// lock-file handle to be unlinked, while on Unix a waiter that already
		// opened the old inode remains serialized. Once the partial has been
		// renamed to the final destination, every waiter checks that destination
		// before writing, so removing the path after releasing the lock cannot
		// create a second writer for this transfer.
		unlockErr := partLock.Unlock()
		if published && unlockErr == nil {
			_ = os.Remove(partLockPath)
		}
	}()
	if _, err := os.Lstat(destination); err == nil {
		return "", 0, fmt.Errorf("download destination already exists: %s", destination)
	} else if !os.IsNotExist(err) {
		return "", 0, err
	}
	offset, err := prepareDownloadTransferPart(partPath, partMetaPath, shareID)
	if err != nil {
		return "", 0, err
	}
	defer func() {
		if retErr == nil {
			_ = os.Remove(partMetaPath)
			return
		}
		// Preserve non-empty, identified partial data so a process restart or a
		// caller retry can resume it. Empty/corrupt attempts are just cleanup
		// debris and should not survive a failed request.
		info, statErr := os.Stat(partPath)
		if statErr != nil || info.Size() == 0 {
			_ = os.Remove(partPath)
			_ = os.Remove(partMetaPath)
		}
	}()
	token, err := g.auth.AccessToken(ctx, g.out, false)
	if err != nil {
		return "", 0, err
	}

	var expectedTotal int64 = -1
	retries := 0
	restarts := 0
	refreshed := false
	preauthenticatedURLRefreshes := 0
	for {
		downloadURL, graphURLRequest, err := g.resolveSharedDriveItemDownloadURL(ctx, path, token)
		if err != nil {
			var statusErr *GraphStatusError
			if !refreshed && errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusUnauthorized {
				token, err = g.auth.RefreshAccessToken(ctx)
				if err != nil {
					return "", 0, err
				}
				refreshed = true
				continue
			}
			if retryableTransferError(err) && retries < g.transferRetries() && ctx.Err() == nil {
				retries++
				if err := g.sleepFor(ctx, g.retryDelay(nil, retries-1)); err != nil {
					return "", 0, err
				}
				continue
			}
			return "", 0, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
		if err != nil {
			return "", 0, err
		}
		if offset > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		if graphURLRequest {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := g.doTransferRequest(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return "", 0, ctx.Err()
			}
			if retries < g.transferRetries() {
				retries++
				if err := g.sleepFor(ctx, g.retryDelay(nil, retries-1)); err != nil {
					return "", 0, err
				}
				continue
			}
			return "", 0, err
		}
		if resp.StatusCode == http.StatusUnauthorized && graphURLRequest && !refreshed {
			discardAndClose(resp.Body)
			token, err = g.auth.RefreshAccessToken(ctx)
			if err != nil {
				return "", 0, err
			}
			refreshed = true
			continue
		}
		if !graphURLRequest && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) && preauthenticatedURLRefreshes < g.transferRetries() {
			// OneDrive pre-authenticated URLs can expire independently of the
			// Graph bearer token. Resolve a fresh URL instead of treating this as
			// a permanent permission failure. Keep the partial offset intact.
			discardAndClose(resp.Body)
			preauthenticatedURLRefreshes++
			retries++
			if err := g.sleepFor(ctx, g.retryDelay(nil, retries-1)); err != nil {
				return "", 0, err
			}
			continue
		}
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && offset > 0 {
			discardAndClose(resp.Body)
			if restarts >= 1 {
				return "", 0, fmt.Errorf("download range %d is no longer valid", offset)
			}
			if err := truncateTransferPart(partPath); err != nil {
				return "", 0, err
			}
			offset = 0
			expectedTotal = -1
			restarts++
			continue
		}
		retryable := shouldRetryTransferStatus(resp.StatusCode)
		if opts.returnRateLimitWithoutRetry && resp.StatusCode == http.StatusTooManyRequests {
			retryable = false
		}
		if retryable && retries < g.transferRetries() {
			delay := g.retryDelay(resp, retries)
			discardAndClose(resp.Body)
			retries++
			if err := g.sleepFor(ctx, delay); err != nil {
				return "", 0, err
			}
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

		responseOffset, responseEnd, responseTotal, hasRange, err := parseDownloadContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			discardAndClose(resp.Body)
			return "", 0, err
		}
		if offset > 0 {
			if resp.StatusCode != http.StatusPartialContent || !hasRange || responseOffset != offset {
				if resp.StatusCode == http.StatusOK && restarts < 1 {
					discardAndClose(resp.Body)
					if err := truncateTransferPart(partPath); err != nil {
						return "", 0, err
					}
					offset = 0
					expectedTotal = -1
					restarts++
					continue
				}
				discardAndClose(resp.Body)
				return "", 0, fmt.Errorf("download resume response did not honor Range %d", offset)
			}
		} else if resp.StatusCode == http.StatusPartialContent {
			if !hasRange || responseOffset != 0 {
				discardAndClose(resp.Body)
				return "", 0, fmt.Errorf("initial partial download response has invalid Content-Range")
			}
		} else if resp.StatusCode != http.StatusOK {
			discardAndClose(resp.Body)
			return "", 0, fmt.Errorf("unexpected download response status %d", resp.StatusCode)
		}
		if hasRange {
			if responseEnd < responseOffset || responseTotal < responseEnd+1 || responseTotal > maxTeamsTransferBytes {
				discardAndClose(resp.Body)
				return "", 0, fmt.Errorf("download response has invalid Content-Range %q", resp.Header.Get("Content-Range"))
			}
			if expectedTotal >= 0 && expectedTotal != responseTotal {
				discardAndClose(resp.Body)
				return "", 0, fmt.Errorf("download response total changed from %d to %d", expectedTotal, responseTotal)
			}
			expectedTotal = responseTotal
		}
		if contentLength := resp.ContentLength; contentLength > maxTeamsTransferBytes-offset {
			discardAndClose(resp.Body)
			return "", 0, fmt.Errorf("refusing to download file larger than %d bytes", maxTeamsTransferBytes)
		}
		if !hasRange && resp.ContentLength >= 0 {
			if expectedTotal >= 0 && expectedTotal != resp.ContentLength {
				discardAndClose(resp.Body)
				return "", 0, fmt.Errorf("download response total changed from %d to %d", expectedTotal, resp.ContentLength)
			}
			expectedTotal = resp.ContentLength
		}
		if contentType == "" {
			contentType = strings.TrimSpace(resp.Header.Get("Content-Type"))
		}

		expectedBody := maxTeamsTransferBytes - offset
		if hasRange {
			expectedBody = responseEnd - responseOffset + 1
		}
		startOffset := offset
		file, err := os.OpenFile(partPath, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			discardAndClose(resp.Body)
			return "", 0, err
		}
		written, copyErr := io.Copy(file, io.LimitReader(resp.Body, expectedBody+1))
		syncErr := file.Sync()
		closeFileErr := file.Close()
		closeBodyErr := resp.Body.Close()
		if written > expectedBody {
			if truncateErr := truncateTransferPartTo(partPath, startOffset); truncateErr != nil {
				return "", 0, truncateErr
			}
			return "", 0, fmt.Errorf("downloaded response body exceeded declared range: got at least %d, want %d", written, expectedBody)
		}
		if copyErr != nil || syncErr != nil || closeFileErr != nil || closeBodyErr != nil {
			transferErr := firstTransferError(copyErr, syncErr, closeFileErr, closeBodyErr)
			if ctx.Err() != nil {
				return "", 0, ctx.Err()
			}
			if retries < g.transferRetries() {
				offset = startOffset + written
				retries++
				if err := g.sleepFor(ctx, g.retryDelay(nil, retries-1)); err != nil {
					return "", 0, err
				}
				continue
			}
			return "", 0, transferErr
		}
		if hasRange && written != expectedBody {
			offset = startOffset + written
			if retries < g.transferRetries() && ctx.Err() == nil {
				retries++
				if err := g.sleepFor(ctx, g.retryDelay(nil, retries-1)); err != nil {
					return "", 0, err
				}
				continue
			}
			return "", 0, fmt.Errorf("downloaded response body length mismatch: got %d, want %d", written, expectedBody)
		}
		offset = startOffset + written
		retries = 0
		if expectedTotal >= 0 {
			if offset > expectedTotal {
				return "", 0, fmt.Errorf("downloaded file exceeds declared size %d", expectedTotal)
			}
			if offset < expectedTotal {
				continue
			}
		}
		if err := syncAndCloseTransferPart(partPath); err != nil {
			return "", 0, err
		}
		if err := os.Rename(partPath, destination); err != nil {
			return "", 0, err
		}
		published = true
		return contentType, offset, nil
	}
}

func (g *GraphClient) resolveSharedDriveItemDownloadURL(ctx context.Context, path string, token string) (string, bool, error) {
	resolveCtx, cancel := context.WithTimeout(ctx, defaultGraphHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(resolveCtx, http.MethodGet, g.graphURL(path), nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	// Keep the cached transfer transport and only customize this client copy;
	// mutating the shared client's redirect policy would race with downloads.
	client := *g.transferHTTPClient()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
	if err != nil {
		if resolveCtx.Err() != nil && ctx.Err() == nil {
			return "", false, &transferStallError{Idle: defaultGraphHTTPTimeout}
		}
		return "", false, err
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		location, locationErr := resp.Location()
		_ = resp.Body.Close()
		if locationErr != nil {
			return "", false, locationErr
		}
		if err := g.validateDownloadURL(location.String()); err != nil {
			return "", false, err
		}
		return location.String(), false, nil
	}
	if resp.StatusCode == http.StatusOK {
		_ = resp.Body.Close()
		return g.graphURL(path), true, nil
	}
	raw, readErr := readLimited(resp.Body, maxDriveItemJSONBytes)
	_ = resp.Body.Close()
	if readErr != nil {
		return "", false, readErr
	}
	return "", false, graphStatusError(http.MethodGet, path, resp, raw)
}

func (g *GraphClient) validateDownloadURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return fmt.Errorf("Graph download returned an invalid download URL")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return fmt.Errorf("Graph download returned an unsupported download URL scheme")
	}
	base, baseErr := url.Parse(g.baseURL)
	if parsed.Scheme == "http" && (baseErr != nil || base == nil || parsed.Host != base.Host) {
		return fmt.Errorf("Graph download returned an insecure download URL")
	}
	return nil
}

func parseDownloadContentRange(raw string) (int64, int64, int64, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, -1, false, nil
	}
	if !strings.HasPrefix(strings.ToLower(raw), "bytes ") {
		return 0, 0, -1, false, fmt.Errorf("download response has invalid Content-Range %q", raw)
	}
	value := strings.TrimSpace(raw[len("bytes "):])
	rangeText, totalText, ok := strings.Cut(value, "/")
	if !ok {
		return 0, 0, -1, false, fmt.Errorf("download response has invalid Content-Range %q", raw)
	}
	startText, endText, ok := strings.Cut(strings.TrimSpace(rangeText), "-")
	if !ok {
		return 0, 0, -1, false, fmt.Errorf("download response has invalid Content-Range %q", raw)
	}
	start, startErr := strconv.ParseInt(strings.TrimSpace(startText), 10, 64)
	end, endErr := strconv.ParseInt(strings.TrimSpace(endText), 10, 64)
	total, totalErr := strconv.ParseInt(strings.TrimSpace(totalText), 10, 64)
	if startErr != nil || endErr != nil || totalErr != nil || start < 0 || end < start || total <= end {
		return 0, 0, -1, false, fmt.Errorf("download response has invalid Content-Range %q", raw)
	}
	return start, end, total, true, nil
}

func truncateTransferPart(path string) error {
	return truncateTransferPartTo(path, 0)
}

func truncateTransferPartTo(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func syncAndCloseTransferPart(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func prepareDownloadTransferPart(partPath string, metadataPath string, shareID string) (int64, error) {
	partInfo, partErr := os.Lstat(partPath)
	metadataInfo, metadataErr := os.Lstat(metadataPath)
	if partErr != nil && !os.IsNotExist(partErr) {
		return 0, partErr
	}
	if metadataErr != nil && !os.IsNotExist(metadataErr) {
		return 0, metadataErr
	}
	if partErr == nil && partInfo.Mode()&os.ModeSymlink != 0 {
		return 0, fmt.Errorf("refusing to resume download through a symlink: %s", partPath)
	}
	if metadataErr == nil && metadataInfo.Mode()&os.ModeSymlink != 0 {
		return 0, fmt.Errorf("refusing to read download metadata through a symlink: %s", metadataPath)
	}

	if partErr == nil && !partInfo.Mode().IsRegular() {
		return 0, fmt.Errorf("download partial path is not a regular file: %s", partPath)
	}
	if partErr == nil && metadataErr == nil {
		metadata, readErr := readTransferPartMetadata(metadataPath)
		stale := time.Since(partInfo.ModTime()) > downloadPartialRetention
		if readErr == nil && metadata.Version == transferPartMetadataVersion && metadata.ShareID == shareID && !stale {
			if partInfo.Size() > maxTeamsTransferBytes {
				return 0, fmt.Errorf("download partial file exceeds %d bytes", maxTeamsTransferBytes)
			}
			if err := os.Chmod(partPath, 0o600); err != nil {
				return 0, err
			}
			return partInfo.Size(), nil
		}
	}

	// A partial file without matching metadata could belong to a different
	// share or to an older implementation. Never append to it blindly.
	if partErr == nil {
		if err := os.Remove(partPath); err != nil && !os.IsNotExist(err) {
			return 0, err
		}
	}
	if metadataErr == nil {
		if err := os.Remove(metadataPath); err != nil && !os.IsNotExist(err) {
			return 0, err
		}
	}
	part, err := os.OpenFile(partPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, err
	}
	if err := part.Chmod(0o600); err != nil {
		_ = part.Close()
		_ = os.Remove(partPath)
		return 0, err
	}
	if err := part.Sync(); err != nil {
		_ = part.Close()
		_ = os.Remove(partPath)
		return 0, err
	}
	if err := part.Close(); err != nil {
		_ = os.Remove(partPath)
		return 0, err
	}
	if err := writeTransferPartMetadata(metadataPath, transferPartMetadata{
		Version: transferPartMetadataVersion,
		ShareID: shareID,
	}); err != nil {
		_ = os.Remove(partPath)
		return 0, err
	}
	return 0, nil
}

func readTransferPartMetadata(path string) (transferPartMetadata, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return transferPartMetadata{}, err
	}
	var metadata transferPartMetadata
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return transferPartMetadata{}, err
	}
	return metadata, nil
}

func writeTransferPartMetadata(path string, metadata transferPartMetadata) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".download-meta-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(encoded, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	keep = true
	return nil
}

func firstTransferError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func retryableTransferError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *GraphStatusError
	if errors.As(err, &statusErr) {
		return shouldRetryTransferStatus(statusErr.StatusCode)
	}
	return true
}
