package teams

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Files.
//
// Outbound: files go to the signed-in user's OneDrive under
// <UploadFolder>/<unique upload folder>/<original name>, so the original
// name is kept and no earlier upload is ever overwritten. Files up to
// singleUploadMax use one PUT; larger files use a resumable upload session in
// ChunkSize pieces. The session is handed to the core (save callback) after
// every acknowledged chunk.
//
// Inbound: hosted contents (inline images, voice/video clips) are read from
// the chat message; reference attachments on allowed SharePoint hosts are
// read through /shares. Downloads resume from dst+".part" with a Range
// request.

const (
	singleUploadMax = 10 << 20
	maxDownload     = 2 << 30
)

var guidRE = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

type driveItem struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	ETag      string `json:"eTag"`
	WebURL    string `json:"webUrl"`
	WebDavURL string `json:"webDavUrl"`
}

// driveRef is the opaque FileRef.Ref of an uploaded file.
type driveRef struct {
	ItemID       string `json:"item"`
	AttachmentID string `json:"att"`
	URL          string `json:"url"`
	Name         string `json:"name"`
}

func parseDriveRef(ref string) (driveRef, error) {
	var d driveRef
	raw, ok := strings.CutPrefix(ref, "drive:")
	if !ok || json.Unmarshal([]byte(raw), &d) != nil || d.AttachmentID == "" || d.URL == "" {
		return driveRef{}, fmt.Errorf("teams: %q is not an uploaded file reference", ref)
	}
	return d, nil
}

func (it driveItem) ref() (channel.FileRef, error) {
	att := strings.ToLower(guidRE.FindString(it.ETag))
	u := first(it.WebDavURL, it.WebURL)
	if it.ID == "" || att == "" || u == "" {
		return channel.FileRef{}, errors.New("drive item response lacks id, eTag GUID or URL")
	}
	raw, _ := json.Marshal(driveRef{ItemID: it.ID, AttachmentID: att, URL: u, Name: it.Name})
	return channel.FileRef{Kind: channel.FileDoc, Name: it.Name, Size: it.Size, Ref: "drive:" + string(raw)}, nil
}

// UploadFile uploads src as name. See the package comment above for layout.
func (a *Adapter) UploadFile(ctx context.Context, src, name string, sess *channel.UploadSession, save func(channel.UploadSession) error) (channel.FileRef, error) {
	f, err := os.Open(src)
	if err != nil {
		return channel.FileRef{}, &channel.Rejected{Permanent: true, Err: err}
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return channel.FileRef{}, &channel.Rejected{Permanent: true, Err: fmt.Errorf("upload source %s is not a regular file", src)}
	}
	size := st.Size()
	if sess != nil && sess.URL != "" {
		return a.resumeUpload(ctx, f, size, *sess, save)
	}
	target, err := a.uploadTarget(name)
	if err != nil {
		return channel.FileRef{}, &channel.Rejected{Permanent: true, Err: err}
	}
	if size <= singleUploadMax {
		data, err := io.ReadAll(f)
		if err != nil {
			return channel.FileRef{}, &channel.Rejected{Err: err}
		}
		resp, err := a.g.do(ctx, call{
			method: http.MethodPut, url: target + ":/content?@microsoft.graph.conflictBehavior=fail",
			body: data, ctype: "application/octet-stream", write: true,
		})
		if err != nil {
			return channel.FileRef{}, err
		}
		return itemFromResponse(resp.body)
	}
	var out struct {
		UploadURL  string `json:"uploadUrl"`
		Expiration string `json:"expirationDateTime"`
	}
	in := map[string]any{"item": map[string]any{"@microsoft.graph.conflictBehavior": "fail"}}
	if err := a.g.writeJSON(ctx, http.MethodPost, target+":/createUploadSession", in, &out); err != nil {
		return channel.FileRef{}, err
	}
	if out.UploadURL == "" {
		return channel.FileRef{}, &channel.Ambiguous{Err: errors.New("upload session response has no uploadUrl")}
	}
	s := channel.UploadSession{URL: out.UploadURL, Target: target}
	s.ExpiresAt, _ = time.Parse(time.RFC3339Nano, out.Expiration)
	if save != nil {
		if err := save(s); err != nil {
			return channel.FileRef{}, &channel.Rejected{Err: err}
		}
	}
	return a.sendChunks(ctx, f, size, s, save)
}

// uploadTarget returns the drive path ("/me/drive/root:/a/b/name") for a new upload.
func (a *Adapter) uploadTarget(name string) (string, error) {
	name = safeFileName(name)
	if name == "" {
		return "", errors.New("upload name is empty")
	}
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	sub := "codex-" + a.cfg.Now().UTC().Format("20060102-150405") + "-" + hex.EncodeToString(rnd[:])
	segs := append(strings.Split(strings.Trim(a.cfg.UploadFolder, "/"), "/"), sub, name)
	for i, s := range segs {
		if s == "" || s == "." || s == ".." {
			return "", fmt.Errorf("bad upload folder %q", a.cfg.UploadFolder)
		}
		segs[i] = url.PathEscape(s)
	}
	return "/me/drive/root:/" + strings.Join(segs, "/"), nil
}

// safeFileName keeps the original name but removes what OneDrive refuses.
func safeFileName(name string) string {
	name = path.Base(strings.ReplaceAll(strings.TrimSpace(name), `\`, "/"))
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || strings.ContainsRune(`"*:<>?/\|#%`, r) {
			return '_'
		}
		return r
	}, name)
	name = strings.Trim(name, " .")
	if name == "" || name == "_" {
		return "attachment"
	}
	if len(name) > 200 {
		ext := path.Ext(name)
		if len(ext) > 20 {
			ext = ""
		}
		name = strings.ToValidUTF8(name[:200-len(ext)], "") + ext
	}
	return name
}

func itemFromResponse(body []byte) (channel.FileRef, error) {
	var it driveItem
	if err := json.Unmarshal(body, &it); err != nil {
		return channel.FileRef{}, &channel.Ambiguous{Err: fmt.Errorf("upload response: %w", err)}
	}
	ref, err := it.ref()
	if err != nil {
		return channel.FileRef{}, &channel.Ambiguous{Err: err}
	}
	return ref, nil
}

// resumeUpload asks the session where to continue. A session that is gone
// may have completed: the target path is checked before giving up.
func (a *Adapter) resumeUpload(ctx context.Context, f *os.File, size int64, s channel.UploadSession, save func(channel.UploadSession) error) (channel.FileRef, error) {
	resp, err := a.g.do(ctx, call{method: http.MethodGet, url: s.URL, noAuth: true})
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) && s.Target != "" {
			var it driveItem
			if gerr := a.g.getJSON(ctx, s.Target, smallBodyLimit, &it); gerr == nil && it.Size == size {
				ref, rerr := it.ref()
				if rerr != nil {
					return channel.FileRef{}, &channel.Rejected{Err: rerr}
				}
				return ref, nil
			}
		}
		return channel.FileRef{}, err
	}
	off, err := nextOffset(resp.body)
	if err != nil {
		return channel.FileRef{}, &channel.Rejected{Err: err}
	}
	s.Offset = off
	return a.sendChunks(ctx, f, size, s, save)
}

func nextOffset(body []byte) (int64, error) {
	var st struct {
		Next []string `json:"nextExpectedRanges"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		return 0, fmt.Errorf("upload status: %w", err)
	}
	if len(st.Next) == 0 {
		return 0, errors.New("upload status has no nextExpectedRanges")
	}
	start, _, _ := strings.Cut(st.Next[0], "-")
	return strconv.ParseInt(start, 10, 64)
}

func (a *Adapter) sendChunks(ctx context.Context, f *os.File, size int64, s channel.UploadSession, save func(channel.UploadSession) error) (channel.FileRef, error) {
	buf := make([]byte, a.cfg.ChunkSize)
	for {
		if s.Offset < 0 || s.Offset >= size {
			return channel.FileRef{}, &channel.Rejected{Err: fmt.Errorf("upload session offset %d outside file of %d bytes", s.Offset, size)}
		}
		n := min(int64(len(buf)), size-s.Offset)
		if _, err := f.ReadAt(buf[:n], s.Offset); err != nil && !errors.Is(err, io.EOF) {
			return channel.FileRef{}, &channel.Rejected{Err: err}
		}
		resp, err := a.g.do(ctx, call{
			method: http.MethodPut, url: s.URL, noAuth: true, write: true, body: buf[:n], ctype: "application/octet-stream",
			header: map[string]string{"Content-Range": fmt.Sprintf("bytes %d-%d/%d", s.Offset, s.Offset+n-1, size)},
		})
		if err != nil {
			// A chunk PUT can be repeated; the core resumes with the saved session.
			return channel.FileRef{}, err
		}
		if resp.status == http.StatusOK || resp.status == http.StatusCreated {
			return itemFromResponse(resp.body)
		}
		off, err := nextOffset(resp.body)
		if err != nil {
			return channel.FileRef{}, &channel.Rejected{Err: err}
		}
		if off <= s.Offset {
			return channel.FileRef{}, &channel.Rejected{Err: fmt.Errorf("upload session did not advance past %d", s.Offset)}
		}
		s.Offset = off
		if save != nil {
			if err := save(s); err != nil {
				return channel.FileRef{}, &channel.Rejected{Err: err}
			}
		}
	}
}

// FetchFile downloads ref to dst, resuming dst+".part".
func (a *Adapter) FetchFile(ctx context.Context, ref channel.FileRef, dst string) (channel.FileRef, error) {
	var src string
	switch {
	case strings.HasPrefix(ref.Ref, "hosted:"):
		segs := strings.Split(strings.TrimPrefix(ref.Ref, "hosted:"), "/")
		if len(segs) != 3 {
			return ref, &channel.Rejected{Permanent: true, Err: fmt.Errorf("bad hosted ref")}
		}
		src = "/chats/" + segs[0] + "/messages/" + segs[1] + "/hostedContents/" + segs[2] + "/$value"
	case strings.HasPrefix(ref.Ref, "share:"):
		raw := strings.TrimPrefix(ref.Ref, "share:")
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || !a.allowedHost(u.Hostname()) {
			return ref, &channel.Rejected{Permanent: true, Err: fmt.Errorf("file link host is not allowed")}
		}
		src = "/shares/u!" + base64.RawURLEncoding.EncodeToString([]byte(raw)) + "/driveItem/content"
	case strings.HasPrefix(ref.Ref, "drive:"):
		d, err := parseDriveRef(ref.Ref)
		if err != nil {
			return ref, &channel.Rejected{Permanent: true, Err: err}
		}
		src = "/me/drive/items/" + url.PathEscape(d.ItemID) + "/content"
	default:
		return ref, &channel.Rejected{Permanent: true, Err: fmt.Errorf("file %q cannot be fetched: %s", ref.Name, first(ref.Note, "unsupported reference"))}
	}
	part := dst + ".part"
	var have int64
	if st, err := os.Stat(part); err == nil {
		have = st.Size()
	}
	hdr := map[string]string{"Accept": "*/*"}
	if have > 0 {
		hdr["Range"] = fmt.Sprintf("bytes=%d-", have)
	}
	var ctype string
	_, err := a.g.do(ctx, call{method: http.MethodGet, url: src, header: hdr, sink: func(hr *http.Response) error {
		ctype = hr.Header.Get("Content-Type")
		flags := os.O_CREATE | os.O_WRONLY
		if hr.StatusCode == http.StatusPartialContent && have > 0 && strings.HasPrefix(hr.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-", have)) {
			flags |= os.O_APPEND
		} else {
			flags |= os.O_TRUNC
			have = 0
		}
		out, err := os.OpenFile(part, flags, 0o600)
		if err != nil {
			return err
		}
		n, err := io.Copy(out, io.LimitReader(hr.Body, maxDownload-have+1))
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		if err == nil && have+n > maxDownload {
			err = fmt.Errorf("download larger than %d bytes", int64(maxDownload))
		}
		return err
	}})
	if err != nil {
		return ref, err
	}
	if err := os.Rename(part, dst); err != nil {
		return ref, &channel.Rejected{Err: err}
	}
	st, err := os.Stat(dst)
	if err != nil {
		return ref, &channel.Rejected{Err: err}
	}
	ref.Size = st.Size()
	if ref.ContentType == "" {
		ref.ContentType = strings.TrimSpace(strings.Split(ctype, ";")[0])
	}
	return ref, nil
}
