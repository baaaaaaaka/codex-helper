package teams

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// driveFront models the OneDrive, hosted-content and /shares endpoints the
// fake Graph does not, and forwards everything else to it.
type driveFront struct {
	t     *testing.T
	srv   *httptest.Server
	mu    sync.Mutex
	files map[string][]byte // drive path -> content
	ups   map[string]*upload
	blobs map[string][]byte // hosted / share content by key
	seq   int

	failChunk int  // fail the PUT of this chunk number once (1-based); 0 = never
	loseFinal bool // complete the upload but answer 500
	chunks    int
	ranges    []string
	puts      int
}

type upload struct {
	path string
	data []byte
	done bool
}

func newDriveFront(t *testing.T, fg *fakegraph.Server) *driveFront {
	d := &driveFront{t: t, files: map[string][]byte{}, ups: map[string]*upload{}, blobs: map[string][]byte{}}
	target, _ := url.Parse(fg.URL())
	proxy := httputil.NewSingleHostReverseProxy(target)
	d.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !d.serve(w, r) {
			proxy.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(d.srv.Close)
	return d
}

func (d *driveFront) item(path string, size int) map[string]any {
	d.seq++
	name := path[strings.LastIndex(path, "/")+1:]
	return map[string]any{
		"id": fmt.Sprintf("item-%d", d.seq), "name": name, "size": size,
		"eTag":   fmt.Sprintf(`"{0A1B2C3D-0000-4000-8000-%012d},1"`, d.seq),
		"webUrl": "https://contoso.sharepoint.com/personal/o/Documents/" + url.PathEscape(name),
	}
}

func writeJSONResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (d *driveFront) serve(w http.ResponseWriter, r *http.Request) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := r.URL.Path
	authed := strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ")
	switch {
	case strings.HasPrefix(p, "/v1.0/me/drive/root:/"):
		if !authed {
			w.WriteHeader(401)
			return true
		}
		inner := strings.TrimPrefix(p, "/v1.0/me/drive/root:/")
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(inner, ":/content"):
			path := strings.TrimSuffix(inner, ":/content")
			if r.URL.Query().Get("@microsoft.graph.conflictBehavior") != "fail" {
				d.t.Errorf("simple upload without conflictBehavior=fail: %s", r.URL.RawQuery)
			}
			if _, ok := d.files[path]; ok {
				writeJSONResp(w, 409, map[string]any{"error": map[string]string{"code": "nameAlreadyExists"}})
				return true
			}
			data, _ := io.ReadAll(r.Body)
			d.puts++
			d.files[path] = data
			writeJSONResp(w, 201, d.item(path, len(data)))
		case r.Method == http.MethodPost && strings.HasSuffix(inner, ":/createUploadSession"):
			path := strings.TrimSuffix(inner, ":/createUploadSession")
			d.seq++
			id := strconv.Itoa(d.seq)
			d.ups[id] = &upload{path: path}
			writeJSONResp(w, 200, map[string]any{"uploadUrl": d.srv.URL + "/upload/" + id + "?tempauth=secret", "expirationDateTime": "2030-01-01T00:00:00Z"})
		case r.Method == http.MethodGet:
			data, ok := d.files[inner]
			if !ok {
				writeJSONResp(w, 404, map[string]any{"error": map[string]string{"code": "itemNotFound"}})
				return true
			}
			writeJSONResp(w, 200, d.item(inner, len(data)))
		default:
			w.WriteHeader(400)
		}
		return true
	case strings.HasPrefix(p, "/upload/"):
		if r.Header.Get("Authorization") != "" {
			d.t.Errorf("upload URL received an Authorization header")
		}
		u := d.ups[strings.TrimPrefix(p, "/upload/")]
		if u == nil || u.done {
			writeJSONResp(w, 404, map[string]any{"error": map[string]string{"code": "itemNotFound"}})
			return true
		}
		if r.Method == http.MethodGet {
			writeJSONResp(w, 200, map[string]any{"nextExpectedRanges": []string{fmt.Sprintf("%d-", len(u.data))}})
			return true
		}
		var a, b, total int
		if _, err := fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &a, &b, &total); err != nil || a != len(u.data) {
			writeJSONResp(w, 416, map[string]any{"error": map[string]string{"code": "invalidRange"}})
			return true
		}
		body, _ := io.ReadAll(r.Body)
		d.chunks++
		if d.chunks == d.failChunk {
			d.failChunk = 0
			w.WriteHeader(500)
			return true
		}
		u.data = append(u.data, body...)
		if len(u.data) < total {
			writeJSONResp(w, 202, map[string]any{"nextExpectedRanges": []string{fmt.Sprintf("%d-", len(u.data))}})
			return true
		}
		u.done = true
		d.files[u.path] = u.data
		if d.loseFinal {
			d.loseFinal = false
			w.WriteHeader(500)
			return true
		}
		writeJSONResp(w, 201, d.item(u.path, len(u.data)))
		return true
	case strings.Contains(p, "/hostedContents/") || strings.HasPrefix(p, "/dl/"):
		if strings.Contains(p, "/hostedContents/") && !authed {
			w.WriteHeader(401)
			return true
		}
		data, ok := d.blobs[p]
		if !ok {
			w.WriteHeader(404)
			return true
		}
		d.ranges = append(d.ranges, r.Header.Get("Range"))
		w.Header().Set("Content-Type", "audio/ogg")
		var from int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &from); err == nil && !strings.HasPrefix(p, "/dl/") {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, len(data)-1, len(data)))
			w.WriteHeader(206)
			w.Write(data[from:])
			return true
		}
		w.Write(data)
		return true
	case strings.HasPrefix(p, "/v1.0/shares/"):
		id := strings.TrimSuffix(strings.TrimPrefix(p, "/v1.0/shares/"), "/driveItem/content")
		http.Redirect(w, r, d.srv.URL+"/dl/"+id, http.StatusFound)
		return true
	}
	return false
}

func frontAdapter(t *testing.T, chunk int64) (*fakegraph.Server, *driveFront, *Adapter) {
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	d := newDriveFront(t, fg)
	access, _ := fg.IssueTokens("files")
	a, err := New(Config{GraphURL: d.srv.URL + "/v1.0", Tokens: &staticTokens{tok: access}, Sleep: noSleep,
		ChunkSize: chunk, SharePointHosts: []string{".sharepoint.com", "127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	return fg, d, a
}

func randFile(t *testing.T, name string, n int) (string, []byte) {
	data := make([]byte, n)
	_, _ = rand.Read(data)
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p, data
}

func TestUploadSmallFileAndAttach(t *testing.T) {
	ctx := context.Background()
	fg, d, a := frontAdapter(t, 0)
	src, data := randFile(t, "report.pdf", 4096)
	ref, err := a.UploadFile(ctx, src, "../evil/Q3 report: final?.pdf", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.puts != 1 || ref.Name != "Q3 report_ final_.pdf" || ref.Size != 4096 {
		t.Fatalf("ref=%+v puts=%d", ref, d.puts)
	}
	for path, got := range d.files {
		if !strings.HasPrefix(path, "Microsoft Teams Chat Files/codex-") || !bytes.Equal(got, data) {
			t.Fatalf("stored at %q", path)
		}
	}
	// A second upload with the same name never overwrites the first.
	if _, err := a.UploadFile(ctx, src, "Q3 report: final?.pdf", nil, nil); err != nil || len(d.files) != 2 {
		t.Fatalf("second upload: %v (%d files)", err, len(d.files))
	}
	chat := fg.CreateMeetingChatAsUser("files").ID
	parts, err := a.Render(ctx, channel.RichDoc{Label: "🤖 ✅ Codex answer", Markdown: "attached", Files: []channel.FileRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Send(ctx, chat, parts[0], "f1"); err != nil {
		t.Fatal(err)
	}
	msgs := fg.Messages(chat)
	if len(msgs) != 1 || len(msgs[0].Attachments) != 1 || !strings.Contains(string(msgs[0].Attachments[0]), `"contentType":"reference"`) {
		t.Fatalf("stored message %+v", msgs)
	}
	noViolations(t, fg)
}

func TestUploadLargeFileResumes(t *testing.T) {
	ctx := context.Background()
	_, d, a := frontAdapter(t, 4*320<<10)
	src, data := randFile(t, "big.bin", singleUploadMax+(1<<20)+123)
	d.failChunk = 4
	var saved []channel.UploadSession
	save := func(s channel.UploadSession) error { saved = append(saved, s); return nil }
	_, err := a.UploadFile(ctx, src, "big.bin", nil, save)
	if !channel.IsAmbiguous(err) {
		t.Fatalf("chunk failure err = %v", err)
	}
	last := saved[len(saved)-1]
	if last.URL == "" || last.Target == "" || last.Offset != 3*4*320<<10 || last.ExpiresAt.IsZero() {
		t.Fatalf("saved session %+v", last)
	}
	ref, err := a.UploadFile(ctx, src, "big.bin", &last, save)
	if err != nil {
		t.Fatal(err)
	}
	var stored []byte
	for _, v := range d.files {
		stored = v
	}
	if !bytes.Equal(stored, data) || ref.Size != int64(len(data)) || ref.Name != "big.bin" {
		t.Fatalf("stored %d bytes, ref %+v", len(stored), ref)
	}

	// Final chunk accepted but the answer lost: resume finds the finished file.
	d.loseFinal = true
	saved = nil
	if _, err := a.UploadFile(ctx, src, "big2.bin", nil, save); !channel.IsAmbiguous(err) {
		t.Fatalf("lost final err = %v", err)
	}
	ref, err = a.UploadFile(ctx, src, "big2.bin", &saved[len(saved)-1], save)
	if err != nil || ref.Name != "big2.bin" {
		t.Fatalf("resume after lost final: %+v %v", ref, err)
	}
}

func TestFetchHostedAndSharedFiles(t *testing.T) {
	ctx := context.Background()
	_, d, a := frontAdapter(t, 0)
	voice := bytes.Repeat([]byte("voice-"), 1000)
	d.blobs["/v1.0/chats/19:c@thread.v2/messages/171/hostedContents/aGk=/$value"] = voice
	dir := t.TempDir()
	dst := filepath.Join(dir, "voice.ogg")
	// A previous attempt left the first half.
	if err := os.WriteFile(dst+".part", voice[:2500], 0o600); err != nil {
		t.Fatal(err)
	}
	ref := channel.FileRef{Kind: channel.FileVoice, Ref: hostedRef("19:c@thread.v2", "171", "aGk=")}
	got, err := a.FetchFile(ctx, ref, dst)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(dst)
	if !bytes.Equal(b, voice) || got.Size != int64(len(voice)) || got.ContentType != "audio/ogg" || d.ranges[0] != "bytes=2500-" {
		t.Fatalf("hosted fetch: %d bytes, ref %+v, ranges %v", len(b), got, d.ranges)
	}

	doc := []byte("%PDF-1.7 shared")
	link := "https://contoso.sharepoint.com/personal/o/Documents/a.pdf"
	d.blobs["/dl/u!"+encodeShare(link)] = doc
	dst2 := filepath.Join(dir, "a.pdf")
	if err := os.WriteFile(dst2+".part", []byte("stale partial that the server will not resume"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.FetchFile(ctx, channel.FileRef{Ref: "share:" + link}, dst2); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst2); !bytes.Equal(b, doc) {
		t.Fatalf("share fetch got %q", b)
	}

	if _, err := a.FetchFile(ctx, channel.FileRef{Ref: "share:https://evil.example/x"}, dst2); !isPermanent(err) {
		t.Fatalf("foreign host: %v", err)
	}
	if _, err := a.FetchFile(ctx, channel.FileRef{Kind: channel.FileUnsupported, Note: "card"}, dst2); !isPermanent(err) {
		t.Fatalf("unsupported: %v", err)
	}
	_, err = a.FetchFile(ctx, channel.FileRef{Ref: hostedRef("19:c@thread.v2", "171", "missing")}, filepath.Join(dir, "m"))
	if !errors.Is(err, channel.ErrNotFound) {
		t.Fatalf("missing hosted: %v", err)
	}
}

func TestInboundAttachmentsClassified(t *testing.T) {
	a, _ := New(Config{GraphURL: "http://127.0.0.1:9/v1.0", Tokens: &staticTokens{tok: "x"}})
	raw := `{
	  "id": "1700000000123", "messageType": "message",
	  "createdDateTime": "2026-11-04T10:00:00.123Z", "lastModifiedDateTime": "2026-11-04T10:00:00.123Z",
	  "from": {"user": {"id": "owner", "displayName": "Owner"}},
	  "body": {"contentType": "html", "content": "<p>see <img src=\"https://graph.microsoft.com/v1.0/chats/c/messages/1700000000123/hostedContents/aW1n/$value\"></p>"},
	  "attachments": [
	    {"id": "a1", "contentType": "reference", "contentUrl": "https://contoso.sharepoint.com/personal/o/Documents/spec.docx", "name": "spec.docx"},
	    {"id": "a2", "contentType": "reference", "contentUrl": "https://evil.example/x.docx", "name": "x.docx"},
	    {"id": "a3", "contentType": "application/vnd.microsoft.card.audio", "name": "voice.ogg",
	     "content": "{\"media\":[{\"url\":\"https://graph.microsoft.com/v1.0/chats/c/messages/1700000000123/hostedContents/dm9pY2U=/$value\"}]}"},
	    {"id": "a4", "contentType": "messageReference",
	     "content": "{\"messageId\":\"1699\",\"messagePreview\":\"earlier answer\",\"messageSender\":{\"user\":{\"id\":\"u\",\"displayName\":\"Coworker\"}}}"},
	    {"id": "a5", "contentType": "application/vnd.microsoft.card.adaptive", "name": "card"}
	  ]}`
	var gm graphMessage
	if err := json.Unmarshal([]byte(raw), &gm); err != nil {
		t.Fatal(err)
	}
	m := a.convert("c", gm, "owner")
	want := []struct {
		kind channel.FileKind
		ref  string
	}{
		{channel.FileImage, hostedRef("c", "1700000000123", "aW1n")},
		{channel.FileDoc, "share:https://contoso.sharepoint.com/personal/o/Documents/spec.docx"},
		{channel.FileUnsupported, ""},
		{channel.FileVoice, hostedRef("c", "1700000000123", "dm9pY2U=")},
		{channel.FileUnsupported, ""},
	}
	if len(m.Files) != len(want) || !m.ByOwner {
		t.Fatalf("files %+v", m.Files)
	}
	for i, w := range want {
		if m.Files[i].Kind != w.kind || m.Files[i].Ref != w.ref || (w.kind == channel.FileUnsupported && m.Files[i].Note == "") {
			t.Errorf("file %d = %+v, want %v %q", i, m.Files[i], w.kind, w.ref)
		}
	}
	if len(m.Quotes) != 1 || m.Quotes[0] != (channel.Quote{ID: "1699", FromName: "Coworker", Text: "earlier answer"}) {
		t.Fatalf("quotes %+v", m.Quotes)
	}

	// Deleted and system messages carry no files.
	gm.Deleted = new(string)
	*gm.Deleted = "2026-11-04T10:01:00Z"
	if d := a.convert("c", gm, "owner"); d.Kind != channel.KindDeleted || d.Files != nil {
		t.Fatalf("deleted: %+v", d)
	}
}

func encodeShare(link string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(link))
}
