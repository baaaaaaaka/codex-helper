package asr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var errSlow = errors.New("download below minimum rate")

func sourceHost(raw string) string {
	u, _ := url.Parse(raw)
	if u == nil {
		return "invalid"
	}
	return u.Host
}

func (in *installer) log(format string, args ...any) {
	if in.logf != nil {
		in.logf("asr: "+format, args...)
	}
}

// download is also used by the HTTP fixture tests. ensure holds the asset
// lock before using a persistent partial; callers without a lock use a
// unique partial so best-effort/NFS locking never shares an active writer.
func (in *installer) download(ctx context.Context, a Asset) (string, error) {
	part := filepath.Join(in.tmp(), "private-"+a.dirName()+"-"+randomHex(6))
	file, err := in.downloadTo(ctx, a, part)
	if err != nil {
		_ = os.Remove(part)
	}
	return file, err
}

func (in *installer) downloadTo(ctx context.Context, a Asset, part string) (string, error) {
	if err := os.MkdirAll(in.tmp(), 0700); err != nil {
		return "", newError(KindCache, "create download directory", err, "")
	}
	urls := a.urls()
	in.mu.Lock()
	preferred := in.preferred
	in.mu.Unlock()
	sort.SliceStable(urls, func(i, j int) bool { return sourceHost(urls[i]) == preferred && sourceHost(urls[j]) != preferred })
	disabled := map[string]bool{}
	var last, transient *Error
	// Visit every source before retrying a failed source. A low-speed
	// source gets one chance to yield; subsequent rounds allow slow but
	// progressing transfers, avoiding repeated downloads on slow networks.
	for round := 0; round < downloadTries; round++ {
		for _, source := range urls {
			if disabled[source] {
				continue
			}
			err := in.downloadAttempt(ctx, a, source, part, round == 0 && len(urls) > 1)
			if err == nil {
				in.mu.Lock()
				in.preferred = sourceHost(source)
				in.mu.Unlock()
				return part, nil
			}
			last = err
			in.log("asset=%s source=%s attempt=%d failed kind=%s reason=%s", a.Name, sourceHost(source), round+1, err.Kind, err.Detail)
			if ctx.Err() != nil {
				return "", ctxError(ctx, "download "+a.Name)
			}
			if err.Kind == KindCache {
				return "", err
			}
			if err.Kind == KindIntegrity || errors.Is(err, errPermanent) {
				disabled[source] = true
			} else {
				transient = err
			}
		}
		if len(disabled) == len(urls) {
			break
		}
		if round+1 < downloadTries {
			timer := time.NewTimer(in.backoff * time.Duration(round+1))
			select {
			case <-ctx.Done():
				timer.Stop()
				return "", ctxError(ctx, "download "+a.Name)
			case <-timer.C:
			}
		}
	}
	if last == nil {
		return "", newError(KindDownload, "download "+a.Name, errPermanent, "no download sources")
	}
	if last.Kind == KindDownload && transient != nil {
		return "", transient
	}
	return "", last
}

func (in *installer) downloadAttempt(parent context.Context, a Asset, source, part string, checkSlow bool) *Error {
	op := "download " + a.Name
	start := time.Now()
	var offset int64
	if st, err := os.Lstat(part); err == nil {
		if !st.Mode().IsRegular() {
			return newError(KindCache, op, nil, "partial is not a regular file")
		}
		offset = st.Size()
	} else if !os.IsNotExist(err) {
		return newError(KindCache, op, err, "")
	}
	if offset > a.Size {
		if err := os.Remove(part); err != nil {
			return newError(KindCache, op, err, "")
		}
		offset = 0
	}
	verify := func() *Error {
		err := in.verifyDownload(a, part)
		if err != nil && err.Kind == KindIntegrity && offset > 0 {
			// The saved prefix may be corrupt. It has been discarded;
			// retry fresh before blaming a source we only resumed from.
			return newError(KindDownload, op, nil, "saved partial failed checksum; discarded for fresh retry")
		}
		return err
	}
	if offset == a.Size {
		return verify()
	}
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	// Includes DNS, TCP, TLS and redirect chains, even with a custom client.
	headerWait := min(15*time.Second, in.stall)
	stalled := time.AfterFunc(headerWait, func() { cancel(errors.New("response headers timed out")) })
	defer stalled.Stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return newError(KindDownload, op, errPermanent, "invalid source URL")
	}
	req.Header.Set("User-Agent", "codex-helper-asr")
	req.Header.Set("Accept-Encoding", "identity")
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	in.log("asset=%s source=%s request offset=%d", a.Name, sourceHost(source), offset)
	resp, err := in.client.Do(req)
	if err != nil {
		reason := "network error"
		var ne net.Error
		var dns *net.DNSError
		if errors.As(err, &ne) && ne.Timeout() {
			reason = "network timeout"
		}
		if errors.As(err, &dns) {
			reason = "DNS failure"
		}
		if context.Cause(ctx) != nil && parent.Err() == nil {
			reason = "response-header deadline"
		}
		return newError(KindDownload, op, nil, fmt.Sprintf("%s after %s", reason, time.Since(start).Round(time.Millisecond)))
	}
	defer resp.Body.Close()
	stalled.Reset(in.stall)
	in.log("asset=%s source=%s headers status=%d wait=%s", a.Name, sourceHost(source), resp.StatusCode, time.Since(start).Round(time.Millisecond))
	if resp.StatusCode == http.StatusPartialContent {
		want := fmt.Sprintf("bytes %d-%d/%d", offset, a.Size-1, a.Size)
		if resp.Header.Get("Content-Range") != want {
			return newError(KindDownload, op, errPermanent, "invalid Content-Range")
		}
	} else if resp.StatusCode == http.StatusOK {
		offset = 0 // Range ignored: replace, never append.
	} else {
		var cause error
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 408 && resp.StatusCode != 429 {
			cause = errPermanent
		}
		return newError(KindDownload, op, cause, fmt.Sprintf("HTTP %d", resp.StatusCode))
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return newError(KindDownload, op, errPermanent, "unexpected Content-Encoding")
	}
	if resp.ContentLength >= 0 && resp.ContentLength != a.Size-offset {
		return newError(KindIntegrity, op, nil, "unexpected Content-Length")
	}
	flags := os.O_CREATE | os.O_WRONLY
	if offset == 0 {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(part, flags, 0600)
	if err != nil {
		return newError(KindCache, op, err, "")
	}
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return newError(KindCache, op, err, "")
	}
	var received atomic.Int64
	if checkSlow && in.slowWindow > 0 {
		go func() {
			tick := time.NewTicker(in.slowWindow)
			defer tick.Stop()
			var prev int64
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					n := received.Load()
					if float64(n-prev)/in.slowWindow.Seconds() < float64(in.minRate) {
						cancel(errSlow)
						return
					}
					prev = n
				}
			}
		}()
	}
	// Reader failures retain the prefix; writer failures are local cache
	// errors and must not cause retries against every mirror.
	buf := make([]byte, 64<<10)
	var readErr, writeErr error
	oversize := false
	lastLog := time.Now()
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			stalled.Reset(in.stall)
			if offset+received.Load()+int64(n) > a.Size {
				oversize = true
				readErr = errors.New("response exceeds pinned size")
				break
			}
			written, werr := f.Write(buf[:n])
			received.Add(int64(written))
			in.report()
			if werr != nil {
				writeErr = werr
				break
			}
			if time.Since(lastLog) >= 30*time.Second {
				in.log("asset=%s source=%s bytes=%d/%d elapsed=%s", a.Name, sourceHost(source), offset+received.Load(), a.Size, time.Since(start).Round(time.Second))
				lastLog = time.Now()
			}
		}
		if rerr != nil {
			readErr = rerr
			break
		}
	}
	closeErr := f.Close()
	if writeErr != nil {
		return newError(KindCache, op, writeErr, "")
	}
	if closeErr != nil {
		return newError(KindCache, op, closeErr, "")
	}
	if oversize {
		_ = os.Remove(part)
		return newError(KindIntegrity, op, nil, "response exceeds pinned size; partial discarded")
	}
	if parent.Err() != nil {
		return newError(KindCanceled, op, parent.Err(), "")
	}
	if errors.Is(context.Cause(ctx), errSlow) {
		return newError(KindDownload, op, errSlow, "low speed; trying another source with saved progress")
	}
	if readErr != io.EOF {
		reason := "transfer interrupted"
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			reason = "unexpected EOF"
		}
		if context.Cause(ctx) != nil {
			reason = "no-data timeout"
		}
		return newError(KindDownload, op, nil, reason+"; saved progress")
	}
	if offset+received.Load() != a.Size {
		return newError(KindDownload, op, nil, "response ended before pinned size; saved progress")
	}
	in.log("asset=%s source=%s downloaded bytes=%d resumed=%d elapsed=%s", a.Name, sourceHost(source), received.Load(), offset, time.Since(start).Round(time.Millisecond))
	return verify()
}

func (in *installer) verifyDownload(a Asset, part string) *Error {
	start := time.Now()
	f, err := os.OpenFile(part, os.O_RDWR, 0)
	if err != nil {
		return newError(KindCache, "verify "+a.Name, err, "")
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, a.Size+1))
	if err != nil {
		return newError(KindCache, "verify "+a.Name, err, "")
	}
	if n != a.Size || !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), a.SHA256) {
		_ = f.Close()
		_ = os.Remove(part)
		return newError(KindIntegrity, "verify "+a.Name, nil, "size or SHA-256 mismatch; partial discarded")
	}
	if err := f.Sync(); err != nil {
		return newError(KindCache, "sync "+a.Name, err, "")
	}
	if err := f.Close(); err != nil {
		return newError(KindCache, "close "+a.Name, err, "")
	}
	in.log("asset=%s verified elapsed=%s", a.Name, time.Since(start).Round(time.Millisecond))
	return nil
}
