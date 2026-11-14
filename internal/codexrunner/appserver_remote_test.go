package codexrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func sharedRemoteFixture(t testing.TB) (RemoteAppServerStarter, <-chan *websocket.Conn) {
	t.Helper()
	connections := make(chan *websocket.Conn, 4)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer shared-secret" {
			http.Error(w, "unauthorized", 401)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			connections <- conn
		}
	}))
	t.Cleanup(server.Close)
	return RemoteAppServerStarter{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Token: "shared-secret"}, connections
}

// The same shared backend must obey AAA on/off, preserve non-approval RPC,
// and remain available after the TUI/broker disconnects.
func TestSharedRemoteBrokerApprovals(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(map[bool]string{false: "manual", true: "AAA"}[automatic], func(t *testing.T) {
			starter, connections := sharedRemoteFixture(t)
			mode := ApprovalModeManual
			if automatic {
				mode = ApprovalModeAutomatic
			}
			broker, err := StartRemoteBroker(context.Background(), RemoteBrokerOptions{Starter: starter, ApprovalMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			defer broker.Close(context.Background())
			backend := <-connections
			defer backend.Close()
			client, _, err := websocket.DefaultDialer.Dial(broker.URL(), http.Header{"Authorization": []string{"Bearer " + broker.AuthToken()}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			_ = backend.SetReadDeadline(time.Now().Add(5 * time.Second))
			_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
			initialize := []byte(`{"id":1,"method":"initialize","params":{"clientInfo":{"name":"codex_tui","version":"test"}}}`)
			if err := client.WriteMessage(websocket.TextMessage, initialize); err != nil {
				t.Fatal(err)
			}
			_, got, err := backend.ReadMessage()
			if err != nil || !bytes.Equal(got, initialize) {
				t.Fatalf("initialize = %s, %v", got, err)
			}
			requests := []string{
				`{"id":2,"method":"item/commandExecution/requestApproval","params":{}}`,
				`{"id":3,"method":"item/fileChange/requestApproval","params":{}}`,
				`{"id":4,"method":"item/permissions/requestApproval","params":{"permissions":{"network":{"enabled":true}}}}`,
				`{"id":5,"method":"mcpServer/elicitation/request","params":{"_meta":{"codex_approval_kind":"mcp_tool_call"}}}`,
			}
			for _, request := range requests {
				var req appServerMessage
				if err := json.Unmarshal([]byte(request), &req); err != nil {
					t.Fatal(err)
				}
				started := time.Now()
				if err := backend.WriteMessage(websocket.TextMessage, []byte(request)); err != nil {
					t.Fatal(err)
				}
				if !automatic {
					_, got, err := client.ReadMessage()
					if err != nil || string(got) != request {
						t.Fatalf("manual request = %s, %v", got, err)
					}
					if err := client.WriteMessage(websocket.TextMessage, []byte(`{"id":99,"result":{"decision":"decline"}}`)); err != nil {
						t.Fatal(err)
					}
				}
				_, got, err := backend.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				if automatic {
					var response appServerResultResponse
					if err := json.Unmarshal(got, &response); err != nil || !bytes.Equal(response.ID, req.ID) {
						t.Fatalf("response ID = %s for request %s: %v", got, req.ID, err)
					}
					if time.Since(started) < 100*time.Millisecond {
						t.Fatal("approval skipped delay")
					}
					if !bytes.Contains(got, []byte(`"result"`)) || bytes.Contains(got, []byte(`"error"`)) {
						t.Fatalf("approval = %s", got)
					}
				} else if string(got) != `{"id":99,"result":{"decision":"decline"}}` {
					t.Fatalf("manual reply = %s", got)
				}
			}
			// A normal question must still reach the TUI, even with AAA enabled.
			question := []byte(`{"id":6,"method":"item/tool/requestUserInput","params":{}}`)
			if err := backend.WriteMessage(websocket.TextMessage, question); err != nil {
				t.Fatal(err)
			}
			_, got, err = client.ReadMessage()
			if err != nil || !bytes.Equal(got, question) {
				t.Fatalf("question = %s, %v", got, err)
			}
			if err := broker.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			next, err := starter.StartAppServer(context.Background(), AppServerStartRequest{})
			if err != nil {
				t.Fatalf("backend unavailable after detach: %v", err)
			}
			defer next.Close()
			nextBackend := <-connections
			defer nextBackend.Close()
		})
	}
}

func TestRemoteAppServerStreamsLargeFrames(t *testing.T) {
	starter, connections := sharedRemoteFixture(t)
	transport, err := starter.StartAppServer(context.Background(), AppServerStartRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	backend := <-connections
	defer backend.Close()
	expected := []byte(strings.Repeat("x", defaultAppServerProcessStdoutLineLimit+1))
	sent := make(chan error, 1)
	go func() { sent <- backend.WriteMessage(websocket.TextMessage, expected) }()
	var size, largest int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	read, err := transport.(AppServerRecordTransport).ReadRecord(ctx, func(chunk []byte) error {
		size += len(chunk)
		if len(chunk) > largest {
			largest = len(chunk)
		}
		if bytes.Count(chunk, []byte("x")) != len(chunk) {
			return io.ErrUnexpectedEOF
		}
		return nil
	})
	if err != nil || !read || size != len(expected) || largest > appServerProcessStdoutChunkSize {
		t.Fatalf("stream read=%t size=%d chunk=%d err=%v", read, size, largest, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	// Cancellation must unblock a pending socket read.
	canceled, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := transport.ReadLine(canceled); done <- err }()
	stop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled read blocked")
	}
}

func TestRemoteAppServerRejectsInvalidEndpoints(t *testing.T) {
	for _, endpoint := range []string{"ws://example.com:80", "ws://127.0.0.1:1?token=x", "wss://127.0.0.1:1", "ws://user@127.0.0.1:1", "ws://127.0.0.1:1/path"} {
		_, err := (RemoteAppServerStarter{URL: endpoint, Token: "secret"}).StartAppServer(context.Background(), AppServerStartRequest{})
		if err == nil {
			t.Fatalf("accepted %q", endpoint)
		}
	}
	starter, _ := sharedRemoteFixture(t)
	starter.Token = "incorrect"
	if _, err := starter.StartAppServer(context.Background(), AppServerStartRequest{}); err == nil {
		t.Fatal("accepted incorrect token")
	}
}

// Per-record allocation should stay bounded when history payloads grow.
func BenchmarkRemoteAppServerReadRecord(b *testing.B) {
	for _, size := range []int{64 << 10, 32 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			starter, connections := sharedRemoteFixture(b)
			transport, err := starter.StartAppServer(context.Background(), AppServerStartRequest{})
			if err != nil {
				b.Fatal(err)
			}
			defer transport.Close()
			backend := <-connections
			defer backend.Close()
			payload := bytes.Repeat([]byte("x"), size)
			done := make(chan error, 1)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			go func() {
				for i := 0; i < b.N; i++ {
					if err := backend.WriteMessage(websocket.TextMessage, payload); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			for i := 0; i < b.N; i++ {
				if _, err := transport.(AppServerRecordTransport).ReadRecord(context.Background(), func([]byte) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				b.Fatal(err)
			}
		})
	}
}
