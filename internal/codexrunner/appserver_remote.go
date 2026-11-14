package codexrunner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RemoteAppServerStarter connects to an existing local app-server. Closing
// its transport only detaches this client; it never owns the backend process.
// The TUI's initialize exchange is relayed by RemoteBroker unchanged.
type RemoteAppServerStarter struct {
	URL   string
	Token string
}

func (s RemoteAppServerStarter) StartAppServer(ctx context.Context, req AppServerStartRequest) (AppServerLineTransport, error) {
	u, err := url.Parse(s.URL)
	if err != nil || u.Scheme != "ws" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || strings.TrimSpace(s.Token) == "" {
		return nil, fmt.Errorf("invalid local shared app-server endpoint")
	}
	// Do not send the local capability through a configured HTTP proxy.
	dialer := websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	if req.Timeout > 0 {
		dialer.HandshakeTimeout = req.Timeout
	}
	conn, _, err := dialer.DialContext(ctx, s.URL, http.Header{"Authorization": []string{"Bearer " + s.Token}})
	if err != nil {
		return nil, fmt.Errorf("connect to shared app-server: %w", err)
	}
	return &remoteAppServerTransport{conn: conn, buf: make([]byte, appServerProcessStdoutChunkSize)}, nil
}

type remoteAppServerTransport struct {
	closeOnce sync.Once
	closeErr  error
	conn      *websocket.Conn
	buf       []byte
}

func (t *remoteAppServerTransport) Close() error {
	t.closeOnce.Do(func() { t.closeErr = t.conn.Close() })
	return t.closeErr
}
func (t *remoteAppServerTransport) WriteLine(ctx context.Context, line []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = t.Close() })
	defer stop()
	_ = t.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return t.conn.WriteMessage(websocket.TextMessage, line)
}
func (t *remoteAppServerTransport) ReadRecord(ctx context.Context, writeChunk func([]byte) error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	stop := context.AfterFunc(ctx, func() { _ = t.Close() })
	defer stop()
	for {
		kind, reader, err := t.conn.NextReader()
		if err != nil {
			return false, err
		}
		if kind != websocket.TextMessage {
			continue
		}
		// Preserve frame boundaries and the broker's bounded streaming behavior.
		buf := t.buf
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				if writeErr := writeChunk(buf[:n]); writeErr != nil {
					return false, writeErr
				}
			}
			if err == io.EOF {
				return true, nil
			}
			if err != nil {
				return false, err
			}
		}
	}
}
func (t *remoteAppServerTransport) ReadLine(ctx context.Context) ([]byte, error) {
	var line []byte
	_, err := t.ReadRecord(ctx, func(chunk []byte) error {
		if len(line)+len(chunk) > defaultAppServerProcessStdoutLineLimit {
			return errAppServerProcessStdoutLineTooLong
		}
		line = append(line, chunk...)
		return nil
	})
	return line, err
}
