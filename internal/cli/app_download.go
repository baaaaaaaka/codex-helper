package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	codexAppDownloadAttempts        = 5
	codexAppDownloadRetryDelay      = 5 * time.Second
	codexAppDownloadConnectTimeout  = 30 * time.Second
	codexAppDownloadParallelParts   = 4
	codexAppDownloadParallelMinSize = 32 * 1024 * 1024
)

var (
	errCodexAppDownloadRangeNotSupported = errors.New("server did not honor range download")
	codexAppDownloadSleep                = codexAppDownloadContextSleep
)

type codexAppDownloadOptions struct {
	URL      string
	Path     string
	ProxyURL string
	Log      io.Writer

	Attempts        int
	ParallelParts   int
	ParallelMinSize int64
}

type codexAppDownloadMetadata struct {
	Size          int64
	SupportsRange bool
}

func downloadCodexAppPackage(ctx context.Context, opts codexAppDownloadOptions) error {
	if strings.TrimSpace(opts.URL) == "" {
		return fmt.Errorf("download URL is empty")
	}
	if strings.TrimSpace(opts.Path) == "" {
		return fmt.Errorf("download destination is empty")
	}
	if parsed, err := url.Parse(opts.URL); err != nil || parsed.Scheme == "" || parsed.Host == "" {
		if err != nil {
			return fmt.Errorf("parse download URL: %w", err)
		}
		return fmt.Errorf("download URL must include scheme and host")
	}
	if proxyURL := strings.TrimSpace(opts.ProxyURL); proxyURL != "" {
		if _, err := parseCodexAppDownloadProxyURL(proxyURL); err != nil {
			return err
		}
	}
	attempts := opts.Attempts
	if attempts <= 0 {
		attempts = codexAppDownloadAttempts
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 && opts.Log != nil {
			_, _ = fmt.Fprintf(opts.Log, "retrying Codex desktop app download (%d/%d): %v\n", attempt, attempts, lastErr)
		}
		if err := downloadCodexAppPackageOnce(ctx, opts); err != nil {
			lastErr = err
			_ = os.Remove(downloadCodexAppTempPath(opts.Path))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if attempt == attempts {
				break
			}
			if err := codexAppDownloadSleep(ctx, codexAppDownloadRetryDelay); err != nil {
				return err
			}
			continue
		}
		return nil
	}
	return lastErr
}

func downloadCodexAppPackageOnce(ctx context.Context, opts codexAppDownloadOptions) error {
	client, err := codexAppDownloadHTTPClient(opts.ProxyURL)
	if err != nil {
		return err
	}
	meta := probeCodexAppDownload(ctx, client, opts.URL)
	parts := opts.ParallelParts
	if parts <= 0 {
		parts = codexAppDownloadParallelParts
	}
	minSize := opts.ParallelMinSize
	if minSize <= 0 {
		minSize = codexAppDownloadParallelMinSize
	}
	if meta.SupportsRange && meta.Size >= minSize && parts > 1 {
		if opts.Log != nil {
			_, _ = fmt.Fprintf(opts.Log, "downloading Codex desktop app package (%s) with %d parallel connections...\n", formatCodexAppDownloadBytes(meta.Size), parts)
		}
		if err := downloadCodexAppPackageParallel(ctx, client, opts.URL, opts.Path, meta.Size, parts, opts.Log); err == nil {
			return nil
		} else if !errors.Is(err, errCodexAppDownloadRangeNotSupported) {
			return err
		} else if opts.Log != nil {
			_, _ = fmt.Fprintln(opts.Log, "parallel download is not supported by the server; falling back to single-connection download...")
		}
	}
	if opts.Log != nil {
		if meta.Size > 0 {
			_, _ = fmt.Fprintf(opts.Log, "downloading Codex desktop app package (%s)...\n", formatCodexAppDownloadBytes(meta.Size))
		} else {
			_, _ = fmt.Fprintln(opts.Log, "downloading Codex desktop app package...")
		}
	}
	return downloadCodexAppPackageSingle(ctx, client, opts.URL, opts.Path, meta.Size, opts.Log)
}

func codexAppDownloadHTTPClient(proxyURL string) (*http.Client, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = http.ProxyFromEnvironment
	tr.DialContext = (&net.Dialer{
		Timeout:   codexAppDownloadConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	tr.ResponseHeaderTimeout = codexAppDownloadConnectTimeout
	tr.DisableCompression = true
	if proxyURL = strings.TrimSpace(proxyURL); proxyURL != "" {
		parsed, err := parseCodexAppDownloadProxyURL(proxyURL)
		if err != nil {
			return nil, err
		}
		tr.Proxy = http.ProxyURL(parsed)
	}
	return &http.Client{Transport: tr}, nil
}

func parseCodexAppDownloadProxyURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse proxy URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("proxy URL must include scheme and host")
	}
	return parsed, nil
}

func probeCodexAppDownload(ctx context.Context, client *http.Client, rawURL string) codexAppDownloadMetadata {
	meta := probeCodexAppDownloadHEAD(ctx, client, rawURL)
	if meta.SupportsRange && meta.Size > 0 {
		return meta
	}
	probe := probeCodexAppDownloadRange(ctx, client, rawURL)
	if probe.Size > 0 {
		meta.Size = probe.Size
	}
	if probe.SupportsRange {
		meta.SupportsRange = true
	}
	return meta
}

func probeCodexAppDownloadHEAD(ctx context.Context, client *http.Client, rawURL string) codexAppDownloadMetadata {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return codexAppDownloadMetadata{}
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return codexAppDownloadMetadata{}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return codexAppDownloadMetadata{}
	}
	return codexAppDownloadMetadata{
		Size:          resp.ContentLength,
		SupportsRange: resp.ContentLength > 0 && strings.Contains(strings.ToLower(resp.Header.Get("Accept-Ranges")), "bytes"),
	}
}

func probeCodexAppDownloadRange(ctx context.Context, client *http.Client, rawURL string) codexAppDownloadMetadata {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return codexAppDownloadMetadata{}
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return codexAppDownloadMetadata{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return codexAppDownloadMetadata{}
	}
	return codexAppDownloadMetadata{
		Size:          parseCodexAppContentRangeSize(resp.Header.Get("Content-Range")),
		SupportsRange: true,
	}
}

func downloadCodexAppPackageSingle(ctx context.Context, client *http.Client, rawURL string, dest string, total int64, log io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: %s", rawURL, resp.Status)
	}
	if total <= 0 {
		total = resp.ContentLength
	}
	progress := newCodexAppDownloadProgress(log, total)
	progress.start()
	tmp := downloadCodexAppTempPath(dest)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	copyErr := copyCodexAppDownload(ctx, file, resp.Body, progress)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if total > 0 && progress.downloadedBytes() != total {
		_ = os.Remove(tmp)
		return fmt.Errorf("downloaded %d bytes, want %d", progress.downloadedBytes(), total)
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	progress.finish()
	return replaceCodexAppDownloadTemp(tmp, dest)
}

func downloadCodexAppPackageParallel(ctx context.Context, client *http.Client, rawURL string, dest string, total int64, parts int, log io.Writer) error {
	if total <= 0 {
		return errCodexAppDownloadRangeNotSupported
	}
	if int64(parts) > total {
		parts = int(total)
	}
	if parts <= 1 {
		return downloadCodexAppPackageSingle(ctx, client, rawURL, dest, total, log)
	}
	tmp := downloadCodexAppTempPath(dest)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Truncate(total); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	progress := newCodexAppDownloadProgress(log, total)
	progress.start()
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, parts)
	var rangeUnsupported atomic.Bool
	var wg sync.WaitGroup
	for _, part := range splitCodexAppDownloadRanges(total, parts) {
		wg.Add(1)
		go func(part codexAppDownloadRange) {
			defer wg.Done()
			if err := downloadCodexAppPackageRange(ctx, client, rawURL, file, part, progress); err != nil {
				if errors.Is(err, errCodexAppDownloadRangeNotSupported) {
					rangeUnsupported.Store(true)
				}
				errCh <- err
				cancel()
			}
		}(part)
	}
	wg.Wait()
	closeErr := file.Close()
	close(errCh)
	if err := parentCtx.Err(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if rangeUnsupported.Load() {
		_ = os.Remove(tmp)
		return errCodexAppDownloadRangeNotSupported
	}
	if err := firstCodexAppDownloadError(errCh); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	progress.finish()
	return replaceCodexAppDownloadTemp(tmp, dest)
}

type codexAppDownloadRange struct {
	Start int64
	End   int64
}

func splitCodexAppDownloadRanges(total int64, parts int) []codexAppDownloadRange {
	ranges := make([]codexAppDownloadRange, 0, parts)
	base := total / int64(parts)
	rem := total % int64(parts)
	start := int64(0)
	for i := 0; i < parts; i++ {
		size := base
		if int64(i) < rem {
			size++
		}
		end := start + size - 1
		ranges = append(ranges, codexAppDownloadRange{Start: start, End: end})
		start = end + 1
	}
	return ranges
}

func downloadCodexAppPackageRange(ctx context.Context, client *http.Client, rawURL string, file *os.File, part codexAppDownloadRange, progress *codexAppDownloadProgress) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", part.Start, part.End))
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return errCodexAppDownloadRangeNotSupported
	}
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("GET %s range %d-%d: %s", rawURL, part.Start, part.End, resp.Status)
	}
	if !codexAppContentRangeMatches(resp.Header.Get("Content-Range"), part.Start, part.End) {
		return errCodexAppDownloadRangeNotSupported
	}
	expected := part.End - part.Start + 1
	var written int64
	buf := make([]byte, 128*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, err := file.WriteAt(buf[:n], part.Start+written); err != nil {
				return err
			}
			written += int64(n)
			progress.add(int64(n))
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	if written != expected {
		return fmt.Errorf("GET %s range %d-%d: downloaded %d bytes, want %d", rawURL, part.Start, part.End, written, expected)
	}
	return nil
}

func copyCodexAppDownload(ctx context.Context, dst io.Writer, src io.Reader, progress *codexAppDownloadProgress) error {
	buf := make([]byte, 128*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
			progress.add(int64(n))
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

type codexAppDownloadProgress struct {
	log        io.Writer
	total      int64
	downloaded atomic.Int64
	mu         sync.Mutex
	lastPct    int
	started    bool
}

func newCodexAppDownloadProgress(log io.Writer, total int64) *codexAppDownloadProgress {
	return &codexAppDownloadProgress{log: log, total: total, lastPct: -1}
}

func (p *codexAppDownloadProgress) start() {
	if p == nil || p.log == nil || p.total <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true
	p.lastPct = 0
	_, _ = fmt.Fprintln(p.log, "Codex desktop app package download: 0%")
}

func (p *codexAppDownloadProgress) add(n int64) {
	if p == nil || p.log == nil || p.total <= 0 || n <= 0 {
		return
	}
	done := p.downloaded.Add(n)
	pct := int(done * 100 / p.total)
	if pct > 100 {
		pct = 100
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if pct <= p.lastPct {
		return
	}
	p.lastPct = pct
	_, _ = fmt.Fprintf(p.log, "Codex desktop app package download: %d%%\n", pct)
}

func (p *codexAppDownloadProgress) finish() {
	if p == nil || p.log == nil || p.total <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastPct < 100 {
		p.lastPct = 100
		_, _ = fmt.Fprintln(p.log, "Codex desktop app package download: 100%")
	}
}

func (p *codexAppDownloadProgress) downloadedBytes() int64 {
	if p == nil {
		return 0
	}
	return p.downloaded.Load()
}

func codexAppContentRangeMatches(raw string, start int64, end int64) bool {
	gotStart, gotEnd, _, ok := parseCodexAppContentRange(raw)
	return ok && gotStart == start && gotEnd == end
}

func parseCodexAppContentRangeSize(raw string) int64 {
	_, _, size, ok := parseCodexAppContentRange(raw)
	if !ok {
		return 0
	}
	return size
}

func parseCodexAppContentRange(raw string) (int64, int64, int64, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(strings.ToLower(raw), "bytes ") {
		return 0, 0, 0, false
	}
	raw = strings.TrimSpace(raw[len("bytes "):])
	slash := strings.LastIndex(raw, "/")
	if slash <= 0 || slash == len(raw)-1 {
		return 0, 0, 0, false
	}
	rangePart := strings.TrimSpace(raw[:slash])
	sizePart := strings.TrimSpace(raw[slash+1:])
	dash := strings.Index(rangePart, "-")
	if dash <= 0 || dash == len(rangePart)-1 {
		return 0, 0, 0, false
	}
	start, err := strconv.ParseInt(strings.TrimSpace(rangePart[:dash]), 10, 64)
	if err != nil || start < 0 {
		return 0, 0, 0, false
	}
	end, err := strconv.ParseInt(strings.TrimSpace(rangePart[dash+1:]), 10, 64)
	if err != nil || end < start {
		return 0, 0, 0, false
	}
	var size int64
	if sizePart != "*" {
		size, err = strconv.ParseInt(sizePart, 10, 64)
		if err != nil || size <= end {
			return 0, 0, 0, false
		}
	}
	return start, end, size, true
}

func firstCodexAppDownloadError(errCh <-chan error) error {
	var canceled error
	for err := range errCh {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				if canceled == nil {
					canceled = err
				}
				continue
			}
			return err
		}
	}
	return canceled
}

func downloadCodexAppTempPath(dest string) string {
	return dest + ".tmp"
}

func replaceCodexAppDownloadTemp(tmp string, dest string) error {
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func codexAppDownloadContextSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func formatCodexAppDownloadBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KiB", "MiB", "GiB"}
	value := float64(n)
	for _, unit := range units {
		value /= 1024
		if value < 1024 || unit == units[len(units)-1] {
			return fmt.Sprintf("%.1f %s", value, unit)
		}
	}
	return fmt.Sprintf("%d B", n)
}
