package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTeamsFetchFilesRedirectResumeAndFailures(t *testing.T) {
	for _, mode := range []string{"normal", "resume", "interrupted", "expired", "complete_partial", "range_ignored", "bad_range", "short_range", "changed", "oversize", "foreign", "deleted_attachment", "final_exists", "stage_symlink"} {
		t.Run(mode, func(t *testing.T) {
			const data = "abcdef"
			var requests, metadata, downloads atomic.Int32
			download := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				n := downloads.Add(1)
				if req.Header.Get("Authorization") != "" {
					t.Error("Graph token leaked to file host")
				}
				if mode == "interrupted" && n == 1 {
					w.Header().Set("Content-Length", "6")
					fmt.Fprint(w, "ab")
					return
				}
				if mode == "expired" && n == 1 {
					w.WriteHeader(403)
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				if mode == "resume" || mode == "interrupted" || mode == "bad_range" || mode == "short_range" {
					if req.Header.Get("Range") != "bytes=2-" {
						t.Errorf("wrong resume range %s", req.Header.Get("Range"))
					}
					value := "bytes 2-5/6"
					if mode == "bad_range" {
						value = "bytes 1-5/6"
					}
					if mode == "short_range" {
						value = "bytes 2-4/6"
					}
					w.Header().Set("Content-Range", value)
					w.WriteHeader(206)
					fmt.Fprint(w, data[2:])
					return
				}
				fmt.Fprint(w, data)
			}))
			defer download.Close()
			source := chatTarget("1")
			gm := readFixture("1", 1, "file")
			gm.Attachments = []graphAttachment{{ID: "a", ContentType: "reference", ContentURL: "https://contoso.sharepoint.com/Documents/a.bin", Name: "a.bin"}}
			graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.Add(1)
				if req.Header.Get("Authorization") != "Bearer read" {
					t.Error("wrong graph identity")
				}
				switch {
				case strings.HasSuffix(req.URL.Path, "/messages/1"):
					copy := gm
					if mode == "deleted_attachment" {
						copy.Attachments = nil
					}
					writeReadJSON(w, copy)
				case strings.HasSuffix(req.URL.Path, "/driveItem/content"):
					location := download.URL + "/download?tempauth=DO_NOT_EXPOSE"
					if mode == "foreign" {
						location = "https://evil.example/file?tempauth=SECRET"
					}
					w.Header().Set("Location", location)
					w.WriteHeader(302)
				case strings.HasSuffix(req.URL.Path, "/driveItem"):
					n := metadata.Add(1)
					etag := "file-v1"
					if mode == "changed" && n > 1 {
						etag = "file-v2"
					}
					size := len(data)
					if mode == "oversize" {
						size = 100
					}
					writeReadJSON(w, map[string]any{"id": "file", "size": size, "eTag": etag, "file": map[string]string{"mimeType": "application/octet-stream"}})
				default:
					t.Errorf("unexpected path %s", req.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer graphSrv.Close()
			r, e := NewReader(Config{GraphURL: graphSrv.URL + "/v1.0", HTTP: download.Client(), Tokens: &staticTokens{tok: "read"}, TenantID: "tenant", SharePointHosts: []string{".sharepoint.com", "127.0.0.1"}, Sleep: noSleep})
			if e != nil {
				t.Fatal(e)
			}
			ref := encodeReadToken(attachmentPrefix, attachmentRef{source, "file", "a"})
			dst := filepath.Join(t.TempDir(), "out.bin")
			if mode == "final_exists" {
				os.WriteFile(dst, []byte("preserve"), 0600)
			}
			if mode == "stage_symlink" {
				elsewhere := t.TempDir()
				if e := os.Symlink(elsewhere, dst+".cxp-download"); e != nil {
					t.Skipf("symlinks unavailable: %v", e)
				}
			}
			if mode == "resume" || mode == "complete_partial" || mode == "range_ignored" || mode == "bad_range" || mode == "short_range" {
				stage := dst + ".cxp-download"
				os.Mkdir(stage, 0700)
				state, _ := json.Marshal(fetchState{ref, "file-v1"})
				os.WriteFile(filepath.Join(stage, "state.json"), state, 0600)
				os.WriteFile(filepath.Join(stage, "content.part"), []byte(data[:2]), 0600)
				if mode == "complete_partial" {
					os.WriteFile(filepath.Join(stage, "content.part"), []byte(data), 0600)
				}
			}
			baseCalls := int32(0)
			if mode == "interrupted" || mode == "expired" {
				_, firstErr := r.Fetch(context.Background(), ref, FetchOptions{Output: dst, MaxBytes: 10, Timeout: time.Minute})
				if firstErr == nil || downloads.Load() != 1 {
					t.Fatalf("failed stream retried or succeeded: %v, downloads %d", firstErr, downloads.Load())
				}
				if _, err := os.Stat(dst); !os.IsNotExist(err) {
					t.Fatal("interrupted file was published")
				}
				if strings.Contains(firstErr.Error(), "DO_NOT_EXPOSE") {
					t.Fatal("expired temporary URL leaked")
				}
				baseCalls = requests.Load() + downloads.Load()
			}
			got, e := r.Fetch(context.Background(), ref, FetchOptions{Output: dst, MaxBytes: 10, Timeout: time.Minute})
			success := mode == "normal" || mode == "resume" || mode == "interrupted" || mode == "expired" || mode == "complete_partial" || mode == "range_ignored"
			if success {
				if e != nil {
					t.Fatal(e)
				}
				b, e := os.ReadFile(dst)
				if e != nil || string(b) != data || got.Bytes != 6 || len(got.SHA256) != 64 || got.Usage.Requests != int(requests.Load()+downloads.Load()-baseCalls) {
					t.Fatalf("bad download %+v %v body %q", got, e, b)
				}
				if mode == "complete_partial" && downloads.Load() != 0 {
					t.Fatal("complete staged bytes downloaded again")
				}
				if _, e = os.Stat(dst + ".cxp-download"); !os.IsNotExist(e) {
					t.Fatal("staging not cleaned")
				}
			} else {
				if e == nil {
					t.Fatal("accepted invalid download")
				}
				if strings.Contains(e.Error(), "SECRET") || strings.Contains(e.Error(), "DO_NOT_EXPOSE") {
					t.Fatal("temporary credentials leaked")
				}
				b, e := os.ReadFile(dst)
				if mode == "final_exists" {
					if string(b) != "preserve" || requests.Load() != 0 {
						t.Fatal("existing output changed or used network")
					}
				} else if !os.IsNotExist(e) {
					t.Fatalf("failed download published %q", b)
				}
			}
		})
	}
}
func TestTeamsFetchHostedChannelAndLimits(t *testing.T) {
	root := ReadTarget{Kind: "channel_message", Tenant: "tenant", Team: "g", Channel: "c", Message: "reply", Parent: "root"}
	for _, mode := range []string{"normal", "too_big", "changed"} {
		t.Run(mode, func(t *testing.T) {
			var metadata atomic.Int32
			r := readTestReader(t, func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/hostedContents/img/$value") {
					if !strings.Contains(req.URL.Path, "/messages/root/replies/reply/") {
						t.Error("lost channel parent")
					}
					w.Header().Set("Content-Type", "image/png")
					fmt.Fprint(w, "123456")
					return
				}
				if req.URL.Path != "/v1.0/teams/g/channels/c/messages/root/replies/reply" {
					t.Errorf("wrong source %s", req.URL.Path)
				}
				gm := readFixture("reply", 1, `<img src="../hostedContents/img/$value">`)
				if mode == "changed" && metadata.Add(1) > 1 {
					gm.ETag = "other"
				}
				writeReadJSON(w, gm)
			})
			dst := filepath.Join(t.TempDir(), "image.png")
			limit := int64(100)
			if mode == "too_big" {
				limit = 3
			}
			got, e := r.Fetch(context.Background(), encodeReadToken(attachmentPrefix, attachmentRef{root, "hosted", "img"}), FetchOptions{Output: dst, MaxBytes: limit, Timeout: time.Minute})
			if mode == "normal" {
				if e != nil || got.Bytes != 6 || got.ContentType != "image/png" {
					t.Fatalf("bad hosted %+v %v", got, e)
				}
			} else if e == nil {
				t.Fatal("invalid hosted published")
			}
		})
	}
}
func TestTeamsFetchRangeValidation(t *testing.T) {
	for _, raw := range []string{"", "bytes */4", "bytes -1-2/4", "bytes 0-5/5", "bytes 3-1/4", "bytes 0-4/*", "bytes 0-2/6"} {
		if _, _, e := parseFetchRange(raw); e == nil {
			t.Errorf("accepted %q", raw)
		}
	}
}
