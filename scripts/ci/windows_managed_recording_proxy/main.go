// Command windows-managed-recording-proxy is a credential-free CI fixture.
// It implements the small part of an HTTP proxy needed by the managed ChatGPT
// smoke: a JSON health endpoint, ordinary absolute-form HTTP requests, and
// CONNECT tunnelling for the public HTTPS MSIX download. Every request is
// recorded so a test can prove that the launched child actually used the
// selected proxy instead of merely inheriting a proxy-looking environment.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type fixture struct {
	instanceID string
	mu         sync.Mutex
	log        *os.File
}

func (f *fixture) record(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.log == nil {
		return
	}
	_, _ = fmt.Fprintf(f.log, format+"\n", args...)
	_ = f.log.Sync()
}

func (f *fixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_codex_proxy/health" && r.Method == http.MethodGet && !r.URL.IsAbs() {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "instanceId": f.instanceID})
		return
	}
	f.record("HTTP %s host=%s uri=%s", r.Method, r.Host, r.RequestURI)
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.WriteString(w, "fixture proxy response\n")
}

func (f *fixture) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		f.serveHTTP(w, r)
		return
	}
	f.record("CONNECT target=%s", r.Host)
	dialer := &net.Dialer{Timeout: 20 * time.Second}
	upstream, err := dialer.DialContext(r.Context(), "tcp", r.Host)
	if err != nil {
		http.Error(w, "fixture CONNECT failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "fixture does not support hijacking", http.StatusInternalServerError)
		return
	}
	client, rw, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}
	_, _ = rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
	_ = rw.Flush()
	go func() {
		_, _ = io.Copy(upstream, client)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(client, upstream)
	_ = client.Close()
	_ = upstream.Close()
}

func main() {
	var portFile, logPath, instanceID string
	flag.StringVar(&portFile, "port-file", "", "file receiving the selected listener port")
	flag.StringVar(&logPath, "log", "", "recording log path")
	flag.StringVar(&instanceID, "instance-id", "ci-recording", "health response instance id")
	flag.Parse()
	if strings.TrimSpace(portFile) == "" || strings.TrimSpace(logPath) == "" {
		log.Fatal("--port-file and --log are required")
	}
	if err := os.MkdirAll(filepath.Dir(portFile), 0o700); err != nil {
		log.Fatal(err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	f := &fixture{instanceID: instanceID, log: logFile}
	server := &http.Server{Handler: http.HandlerFunc(f.serve), ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	serveStarted := make(chan struct{})
	go func() {
		close(serveStarted)
		serveErr <- server.Serve(listener)
	}()
	<-serveStarted
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		_ = server.Shutdown(context.Background())
	}()
	// Publish the port only after the serving goroutine has been scheduled. The
	// CI caller treats the port file as the readiness signal and immediately
	// performs the health check; without this small hand-off it could race the
	// first Accept and accidentally fall back to a different backend.
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(portFile, []byte(fmt.Sprintf("%d\n", port)), 0o600); err != nil {
		log.Fatal(err)
	}
	if err := <-serveErr; err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
