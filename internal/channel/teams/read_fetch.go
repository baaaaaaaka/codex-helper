package teams

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type FetchOptions struct {
	Output   string
	MaxBytes int64
	Timeout  time.Duration
}
type FetchResult struct {
	Version     int       `json:"version"`
	Path        string    `json:"path"`
	Bytes       int64     `json:"bytes"`
	ContentType string    `json:"content_type"`
	SHA256      string    `json:"sha256"`
	Usage       ReadUsage `json:"usage"`
}
type fetchState struct {
	Ref  string `json:"ref"`
	ETag string `json:"etag"`
}

func ValidateFetch(raw string, o FetchOptions, tenant string) error {
	var a attachmentRef
	if e := decodeReadToken(raw, attachmentPrefix, &a); e != nil {
		return e
	}
	if e := a.Target.validate(tenant); e != nil {
		return e
	}
	if a.Target.Message == "" || !validReadID(a.ID) || a.Kind != "file" && a.Kind != "hosted" {
		return errors.New("invalid Teams attachment reference")
	}
	if strings.TrimSpace(o.Output) == "" || o.MaxBytes < 1 || o.MaxBytes > maxDownload || o.Timeout <= 0 || o.Timeout > 10*time.Minute {
		return errors.New("fetch requires --output, a size limit <=2GiB and timeout <=10m")
	}
	return nil
}

// Fetch downloads only a ref revalidated against its source message. It never
// sends Graph credentials to a file URL and never overwrites a final target.
func (r *Reader) Fetch(ctx context.Context, raw string, o FetchOptions) (FetchResult, error) {
	out := FetchResult{Version: 1}
	if e := ValidateFetch(raw, o, r.a.cfg.TenantID); e != nil {
		return out, e
	}
	var a attachmentRef
	_ = decodeReadToken(raw, attachmentPrefix, &a)
	dst, e := filepath.Abs(o.Output)
	if e != nil {
		return out, e
	}
	out.Path = dst
	if _, e = os.Lstat(dst); e == nil {
		return out, errors.New("output already exists")
	}
	if !errors.Is(e, os.ErrNotExist) {
		return out, e
	}
	b := DefaultReadBudget()
	b.Timeout = o.Timeout
	b.MaxBytes = 64 << 20
	// Transfer bytes have their own bound; metadata and transport retries still
	// share the request budget. Allow the metered stream up to the explicit cap.
	ctx, cancel, g, m, e := r.session(ctx, b)
	if e != nil {
		return out, e
	}
	defer cancel()
	m.bytes = o.MaxBytes + (8 << 20)
	finish := func(e error) (FetchResult, error) {
		out.Usage = m.usage(0)
		if e != nil {
			return out, errors.New(readFailure(e))
		}
		return out, nil
	}
	var gm readRecord
	if e = g.getJSON(ctx, a.Target.path(), smallBodyLimit, &gm); e != nil {
		return finish(e)
	}
	if gm.ID != a.Target.Message {
		return out, errors.New("attachment source message mismatch")
	}
	var path, expectedType string
	if a.Kind == "hosted" {
		found := false
		for _, match := range hostedRE.FindAllStringSubmatch(gm.Body.Content, -1) {
			found = found || match[1] == a.ID
		}
		for _, at := range gm.Attachments {
			for _, match := range hostedRE.FindAllStringSubmatch(at.Content, -1) {
				found = found || match[1] == a.ID
			}
		}
		if !found {
			var pg hostedReadPage
			if e = g.getJSON(ctx, a.Target.path()+"/hostedContents", smallBodyLimit, &pg); e != nil {
				return finish(e)
			}
			for _, item := range pg.Value {
				found = found || item.ID == a.ID
			}
			if !found {
				return out, errors.New("hosted attachment is no longer referenced by the source message")
			}
		}
		path = a.Target.path() + "/hostedContents/" + url.PathEscape(a.ID) + "/$value"
	} else {
		for _, at := range gm.Attachments {
			if at.ID == a.ID && at.ContentType == "reference" {
				u, err := url.Parse(at.ContentURL)
				if err != nil || u.Scheme != "https" || u.User != nil || !r.a.allowedHost(u.Hostname()) {
					return out, errors.New("attachment is not on an allowed SharePoint host")
				}
				path = "/shares/u!" + base64.RawURLEncoding.EncodeToString([]byte(at.ContentURL)) + "/driveItem"
				break
			}
		}
		if path == "" {
			return out, errors.New("attachment no longer exists in source message")
		}
	}
	etag := ""
	metadataPath := path
	expectedSize := int64(-1)
	if a.Kind == "file" {
		var item struct {
			Size int64  `json:"size"`
			ETag string `json:"eTag"`
			File *struct {
				MIME string `json:"mimeType"`
			} `json:"file"`
		}
		if e = g.getJSON(ctx, path, smallBodyLimit, &item); e != nil {
			return finish(e)
		}
		if item.File == nil {
			return out, errors.New("attachment is not a downloadable file")
		}
		if item.Size < 0 || item.Size > o.MaxBytes {
			return out, errors.New("attachment exceeds download size limit")
		}
		etag = item.ETag
		expectedSize = item.Size
		expectedType = item.File.MIME
		path += "/content"
	} else {
		etag = gm.ETag + "|" + gm.Modified
	}
	if etag == "" || etag == "|" {
		return out, errors.New("attachment has no stable version for a safe download")
	}
	// A private side directory atomically claims this destination. Refuse an
	// existing unrelated/symlinked staging path, rather than truncating it.
	stage := dst + ".cxp-download"
	created := false
	if e = os.Mkdir(stage, 0700); e == nil {
		created = true
	} else if !errors.Is(e, os.ErrExist) {
		return out, e
	}
	st, e := os.Lstat(stage)
	if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || runtime.GOOS != "windows" && st.Mode().Perm()&0077 != 0 {
		return out, errors.New("unsafe download staging directory")
	}
	lock, e := os.OpenFile(filepath.Join(stage, "lock"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return out, errors.New("download staging is already in use")
	}
	_ = lock.Close()
	defer os.Remove(filepath.Join(stage, "lock"))
	statePath := filepath.Join(stage, "state.json")
	part := filepath.Join(stage, "content.part")
	want := fetchState{Ref: raw, ETag: etag}
	if !created {
		state, e := readFetchState(statePath)
		var prior fetchState
		if e != nil || len(state) > maxReadToken+4096 || json.Unmarshal(state, &prior) != nil || prior != want {
			return out, errors.New("download staging belongs to another source or version; choose a different output")
		}
	}
	if created {
		b, _ := json.Marshal(want)
		if e = os.WriteFile(statePath, b, 0600); e != nil {
			return out, e
		}
	}
	flags := os.O_CREATE | os.O_RDWR
	if !created {
		st, e := os.Lstat(part)
		if e != nil || !st.Mode().IsRegular() {
			return out, errors.New("unsafe or missing download partial file")
		}
	} else {
		flags |= os.O_EXCL
	}
	f, e := os.OpenFile(part, flags, 0600)
	if e != nil {
		return out, e
	}
	defer f.Close()
	st, e = f.Stat()
	if e != nil {
		return out, e
	}
	have := st.Size()
	if have > o.MaxBytes || expectedSize >= 0 && have > expectedSize {
		return out, errors.New("partial download exceeds size limit")
	}
	// A previous transfer may have completed before its final version check
	// failed. Revalidate and publish those bytes without asking for bytes=N-.
	received := expectedSize >= 0 && have == expectedSize
	out.ContentType = expectedType
	for hop, restarted := 0, false; !received && hop <= 5; hop++ {
		hdr := map[string]string{"Accept": "*/*"}
		if a.Kind == "file" {
			hdr["If-Match"] = etag
		}
		if have > 0 {
			hdr["Range"] = fmt.Sprintf("bytes=%d-", have)
		}
		noAuth := strings.HasPrefix(path, "https://")
		var contentType string
		// Do not retry a stream with headers calculated for an older offset.
		// A later explicit invocation revalidates state and resumes safely.
		resp, err := g.once(ctx, call{method: http.MethodGet, url: path, noAuth: noAuth, header: hdr, readRedirect: true, sink: func(hr *http.Response) error {
			if hr.StatusCode == http.StatusPartialContent {
				start, total, err := parseFetchRange(hr.Header.Get("Content-Range"))
				if err != nil || start != have || total > o.MaxBytes || hr.ContentLength >= 0 && hr.ContentLength != total-start {
					return errors.New("invalid download Content-Range")
				}
			} else if hr.StatusCode == 200 {
				have = 0
				if e := f.Truncate(0); e != nil {
					return e
				}
			} else {
				return errors.New("unexpected download status")
			}
			if have > 0 {
				if _, e = f.Seek(have, io.SeekStart); e != nil {
					return e
				}
			} else {
				if _, e = f.Seek(0, io.SeekStart); e != nil {
					return e
				}
			}
			if hr.ContentLength > o.MaxBytes-have {
				return errors.New("download exceeds size limit")
			}
			contentType = hr.Header.Get("Content-Type")
			n, e := io.Copy(f, io.LimitReader(hr.Body, o.MaxBytes-have+1))
			have += n
			if e != nil {
				return e
			}
			if have > o.MaxBytes {
				return errors.New("download exceeds size limit")
			}
			if hr.StatusCode == 206 {
				_, total, _ := parseFetchRange(hr.Header.Get("Content-Range"))
				if have != total {
					return errors.New("partial download did not reach file end")
				}
			}
			received = true
			return nil
		}})
		if err != nil {
			var status *statusError
			if have > 0 && !restarted && errors.As(err, &status) && status.Status == http.StatusRequestedRangeNotSatisfiable {
				// Hosted contents have no file-size metadata. An interrupted
				// finalization can leave a complete partial file; restart once.
				have = 0
				restarted = true
				hop--
				continue
			}
			return finish(err)
		}
		if resp.status/100 == 2 {
			out.ContentType = first(contentType, expectedType)
			break
		}
		next, e := url.Parse(resp.header.Get("Location"))
		if e != nil || next.Scheme != "https" || next.User != nil || !r.a.allowedHost(next.Hostname()) || next.Fragment != "" || hop == 5 {
			return out, errors.New("unsafe or excessive file redirect")
		}
		path = next.String()
	}
	if !received {
		return out, errors.New("download returned no content")
	}
	if expectedSize >= 0 && have != expectedSize {
		return out, errors.New("download size differs from file metadata")
	}
	// Servers may ignore If-Match, especially across signed redirects. Check
	// the authoritative version again before publishing a complete output.
	if a.Kind == "file" {
		var current struct {
			ETag string `json:"eTag"`
			Size int64  `json:"size"`
		}
		if e = g.getJSON(ctx, metadataPath, smallBodyLimit, &current); e != nil {
			return finish(e)
		}
		if current.ETag != etag || current.Size != expectedSize {
			return out, errors.New("attachment changed during download; choose a new output and retry")
		}
	} else {
		var current readRecord
		if e = g.getJSON(ctx, a.Target.path(), smallBodyLimit, &current); e != nil {
			return finish(e)
		}
		if current.ID != gm.ID || current.ETag+"|"+current.Modified != etag {
			return out, errors.New("message changed during hosted download; choose a new output and retry")
		}
	}
	if e = f.Sync(); e != nil {
		return out, e
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return out, e
	}
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return out, e
	}
	out.Bytes = have
	out.SHA256 = hex.EncodeToString(h.Sum(nil))
	if e = f.Close(); e != nil {
		return out, e
	}
	// Link creates a final name without replacing an existing file (including
	// a concurrent writer or symlink). The stage is on the same filesystem.
	if e = os.Link(part, dst); e != nil {
		return out, fmt.Errorf("finalize download without overwriting: %w", e)
	}
	_ = os.Remove(part)
	_ = os.Remove(statePath)
	_ = os.Remove(filepath.Join(stage, "lock"))
	_ = os.Remove(stage)
	out.Usage = m.usage(0)
	return out, nil
}

func readFetchState(path string) ([]byte, error) {
	st, e := os.Lstat(path)
	if e != nil {
		return nil, e
	}
	if !st.Mode().IsRegular() || st.Size() > maxReadToken+4096 {
		return nil, errors.New("invalid download state file")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, maxReadToken+4097))
}
func parseFetchRange(s string) (start, total int64, err error) {
	if !strings.HasPrefix(s, "bytes ") {
		return 0, 0, errors.New("invalid range")
	}
	parts := strings.Split(strings.TrimPrefix(s, "bytes "), "/")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid range")
	}
	bounds := strings.Split(parts[0], "-")
	if len(bounds) != 2 {
		return 0, 0, errors.New("invalid range")
	}
	start, e1 := strconv.ParseInt(bounds[0], 10, 64)
	end, e2 := strconv.ParseInt(bounds[1], 10, 64)
	total, e3 := strconv.ParseInt(parts[1], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil || start < 0 || end < start || end != total-1 {
		return 0, 0, errors.New("invalid range")
	}
	return start, total, nil
}
