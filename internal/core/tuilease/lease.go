// Package tuilease holds a TUI lease through one authenticated local HTTP
// connection. RPC and notifications travel directly to native app-server.
package tuilease

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"database/sql"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

type descriptor struct {
	URL       string `json:"url"`
	Token     string `json:"token"`
	CodexHome string `json:"codexHome"`
}
type Server struct {
	http     *http.Server
	path     string
	listener net.Listener
}

type Attacher interface {
	Attach(context.Context, string) (engine.TUIEndpoint, func(), <-chan struct{}, error)
}

func Listen(path, codexHome string, e Attacher, secureFile func(string) error) (*Server, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		l.Close()
		return nil, err
	}
	d := descriptor{URL: "http://" + l.Addr().String(), Token: hex.EncodeToString(secret), CodexHome: codexHome}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+d.Token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != "POST" || r.URL.Path != "/attach" {
			http.NotFound(w, r)
			return
		}
		var p struct {
			Thread string `json:"threadId"`
			Cwd    string `json:"cwd"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&p); err != nil || (p.Thread == "" && p.Cwd == "") {
			http.Error(w, "invalid thread", 400)
			return
		}
		var endpoint engine.TUIEndpoint
		var release func()
		var done <-chan struct{}
		var err error
		if p.Thread != "" {
			endpoint, release, done, err = e.Attach(r.Context(), p.Thread)
		} else if creator, ok := e.(interface {
			AttachNew(context.Context, string) (engine.TUIEndpoint, func(), <-chan struct{}, error)
		}); ok {
			endpoint, release, done, err = creator.AttachNew(r.Context(), p.Cwd)
		} else {
			err = engine.ErrUnmanagedThread
		}
		if err != nil {
			code := http.StatusConflict
			if errors.Is(err, engine.ErrUnmanagedThread) {
				code = 404
			}
			http.Error(w, err.Error(), code)
			return
		}
		defer release()
		w.Header().Set("Content-Type", "application/x-ndjson")
		if err := json.NewEncoder(w).Encode(endpoint); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-done:
		}
	})
	srv := &Server{http: &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second}, listener: l, path: path}
	// The descriptor changes only with the helper's lifetime.
	if err = writeDescriptor(path, d, secureFile); err != nil {
		l.Close()
		return nil, err
	}

	go func() { _ = srv.http.Serve(l) }()
	return srv, nil
}
func (s *Server) Close() { _ = s.http.Close(); _ = os.Remove(s.path) }

type Lease struct {
	Endpoint engine.TUIEndpoint
	body     io.ReadCloser
	Done     <-chan error
}

func (l *Lease) Close() error { return l.body.Close() }

// Acquire returns nil when no compatible local helper manages this thread.
// A live helper rejecting admission is an error, never a fallback writer.
func Acquire(ctx context.Context, path, codexHome, thread string) (*Lease, error) {
	return acquire(ctx, path, codexHome, `{"threadId":`+quoted(thread)+`}`)
}
func AcquireNew(ctx context.Context, path, codexHome, cwd string) (*Lease, error) {
	return acquire(ctx, path, codexHome, `{"cwd":`+quoted(cwd)+`}`)
}
func acquire(ctx context.Context, path, codexHome, payload string) (*Lease, error) {
	d, err := readDescriptor(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if d.CodexHome != codexHome {
		return nil, nil
	}
	// Never send a capability to a remote or proxied descriptor endpoint.
	endpointURL, parseErr := url.Parse(d.URL)
	if parseErr != nil || endpointURL.Scheme != "http" || endpointURL.Hostname() != "127.0.0.1" || endpointURL.Port() == "" || endpointURL.User != nil || endpointURL.Path != "" || endpointURL.RawQuery != "" || endpointURL.Fragment != "" {
		return nil, errors.New("invalid local TUI endpoint")
	}
	tr := &http.Transport{Proxy: nil, ResponseHeaderTimeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "POST", d.URL+"/attach", strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+d.Token)
	resp, err := (&http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
	if err != nil {
		tr.CloseIdleConnections()
		return nil, err
	}
	if resp.StatusCode != 200 {
		defer tr.CloseIdleConnections()
		defer resp.Body.Close()
		if resp.StatusCode == 404 {
			return nil, nil
		}
		return nil, errors.New("the Teams helper cannot attach this TUI yet; retry shortly")
	}
	var ep engine.TUIEndpoint
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 4096))
	if err = decoder.Decode(&ep); err != nil {
		resp.Body.Close()
		tr.CloseIdleConnections()
		return nil, err
	}
	done := make(chan error)
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		tr.CloseIdleConnections()
	}()
	return &Lease{Endpoint: ep, body: resp.Body, Done: done}, nil
}
func quoted(s string) string { b, _ := json.Marshal(s); return string(b) }

func writeDescriptor(path string, d descriptor, secure func(string) error) error {
	return sqlitestate.PublishSnapshot(path, secure, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE endpoint(id INTEGER PRIMARY KEY CHECK(id=1), url TEXT NOT NULL, token TEXT NOT NULL, codex_home TEXT NOT NULL)`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO endpoint VALUES(1,?,?,?)`, d.URL, d.Token, d.CodexHome)
		return err
	})
}
func readDescriptor(path string) (descriptor, error) {
	var d descriptor
	db, err := sqlitestate.OpenSnapshot(path, 64*1024)
	if err != nil {
		return d, err
	}
	defer db.Close()
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return d, err
	}
	if v != 1 {
		return d, errors.New("unsupported TUI endpoint schema")
	}
	err = db.QueryRow(`SELECT url,token,codex_home FROM endpoint WHERE id=1`).Scan(&d.URL, &d.Token, &d.CodexHome)
	return d, err
}
