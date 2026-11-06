// Command clusterdrill runs the shared-home takeover drills of
// TAKEOVER-REPORT.md §9 outside the Go test runner: several Docker
// containers ("hosts") mount one NFS export as their home and each runs the
// release cxp binary (`cxp teams run`) against one fake Microsoft Graph, so
// duplicate or missing posts across hosts are visible in one place.
//
//	clusterdrill serve   -listen :8080 -brain :8081 -ctl 127.0.0.1:8090 -tokens <dir>
//	clusterdrill forward -listen 127.0.0.1:18080 -to drill-graph:8080
//	clusterdrill ctl     <path> [key=value ...]
//	clusterdrill probe   -file <path> (how soon each lookup method sees a file another host created)
//	clusterdrill tlsproxy -listen 127.0.0.1:443 -to http://cxpd-graph:8080 -ca <file> -instance <tag>
//
// serve runs the fake Graph and the fake Codex brain (both answer on all
// interfaces, through a TCP relay to their loopback listeners), writes the
// token caches the hosts use, and serves a small control API on -ctl:
//
//	POST /setup?dirs=a,b        wait for the control chat, create a work chat per dir
//	POST /traffic?every=4s&prefix=x&on=1   start (on=1) or stop (on=0) posting prompts
//	POST /mark?name=x           record a named instant (with the last request seq)
//	GET  /dump                  prompts, marks, chats, messages and requests (JSON)
//
// tlsproxy serves the Microsoft host names over TLS with a certificate of
// its own CA (written to -ca) and passes each request to the fake Graph
// with the headers the test seam would add. It lets a release that
// predates the seam (v0.1.22-rc.59) run against the fake: its host maps
// the Microsoft names to 127.0.0.1 and trusts the CA (SSL_CERT_FILE).
//
// forward is the relay a host runs so that cxp reaches the fake Graph on a
// loopback address, which the test seam requires. The SUT is only the cxp
// binary; nothing here reaches into it.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: clusterdrill serve|forward|ctl ...")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "forward":
		err = forward(os.Args[2:])
	case "ctl":
		err = ctl(os.Args[2:])
	case "tlsproxy":
		err = tlsproxy(os.Args[2:])
	case "probe":
		err = probe(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "clusterdrill:", err)
		os.Exit(1)
	}
}

// relay accepts on listen and copies each connection to target.
func relay(listen, target string) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				t, err := net.DialTimeout("tcp", target, 10*time.Second)
				if err != nil {
					return
				}
				defer t.Close()
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(t, c); done <- struct{}{} }()
				go func() { _, _ = io.Copy(c, t); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return nil
}

func forward(args []string) error {
	fs := flag.NewFlagSet("forward", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:18080", "")
	to := fs.String("to", "drill-graph:8080", "")
	_ = fs.Parse(args)
	if err := relay(*listen, *to); err != nil {
		return err
	}
	select {}
}

func hostPort(u string) string {
	p, _ := url.Parse(u)
	return p.Host
}

type prompt struct {
	Token string    `json:"token"`
	Chat  string    `json:"chat"`
	At    time.Time `json:"at"`
}

type mark struct {
	Name string    `json:"name"`
	At   time.Time `json:"at"`
	Seq  int64     `json:"seq"`
}

type drill struct {
	g     *fakegraph.Server
	mu    sync.Mutex
	work  []fakegraph.ChatInfo
	ps    []prompt
	marks []mark
	stop  context.CancelFunc
	n     int
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "fake Graph, all interfaces")
	brain := fs.String("brain", ":8081", "fake Codex brain, all interfaces")
	ctlAddr := fs.String("ctl", "127.0.0.1:8090", "control API")
	tokens := fs.String("tokens", "", "directory to write the token caches into")
	_ = fs.Parse(args)

	g := fakegraph.New(fakegraph.Options{})
	codex := harness.NewFakeCodex()
	if err := relay(*listen, hostPort(g.URL())); err != nil {
		return err
	}
	if err := relay(*brain, hostPort(codex.BrainURL())); err != nil {
		return err
	}
	if *tokens != "" {
		if err := writeTokens(g, *tokens); err != nil {
			return err
		}
	}
	d := &drill{g: g}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /setup", d.setup)
	mux.HandleFunc("POST /traffic", d.traffic)
	mux.HandleFunc("POST /mark", func(w http.ResponseWriter, r *http.Request) {
		m := mark{Name: r.URL.Query().Get("name"), At: time.Now(), Seq: g.LastSeq()}
		d.mu.Lock()
		d.marks = append(d.marks, m)
		d.mu.Unlock()
		_ = json.NewEncoder(w).Encode(m)
	})
	mux.HandleFunc("POST /post", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		m, err := g.PostAsUser(q.Get("chat"), q.Get("text"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(m.ID)
	})
	mux.HandleFunc("GET /dump", d.dump)
	fmt.Printf("clusterdrill: graph %s brain %s ctl %s\n", *listen, *brain, *ctlAddr)
	return http.ListenAndServe(*ctlAddr, mux)
}

func writeTokens(g *fakegraph.Server, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	scopes := map[string]string{
		"chat":       "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite",
		"read":       "openid profile offline_access User.Read Chat.Read Files.Read",
		"file-write": "openid profile offline_access User.Read Chat.ReadWrite Files.ReadWrite",
		"full":       "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite",
	}
	exp := time.Now().Add(240 * time.Hour)
	for kind, scope := range scopes {
		access, refresh := g.IssueTokens(kind)
		data, _ := json.MarshalIndent(map[string]any{
			"access_token": access, "refresh_token": refresh, "expires_at": exp.Unix(),
			"expires_in": int64(time.Until(exp) / time.Second), "scope": scope, "token_type": "Bearer",
		}, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "teams-"+kind+"-token.json"), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (d *drill) waitSUT(chatID, sub string, timeout time.Duration) bool {
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		for _, m := range d.g.Messages(chatID) {
			if m.Origin == fakegraph.OriginSUT && strings.Contains(harness.NormalizeSpace(m.PlainText()), sub) {
				return true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func (d *drill) control(timeout time.Duration) (fakegraph.ChatInfo, bool) {
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		for _, c := range d.g.Chats() {
			if strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
				return c, true
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fakegraph.ChatInfo{}, false
}

// setup waits for the control chat and creates one work chat per dir.
func (d *drill) setup(w http.ResponseWriter, r *http.Request) {
	control, ok := d.control(5 * time.Minute)
	if !ok || !d.waitSUT(control.ID, "control chat is ready", 5*time.Minute) {
		http.Error(w, "no control chat", 500)
		return
	}
	time.Sleep(15 * time.Second) // one control poll after the ready notice
	for _, dir := range strings.Split(r.URL.Query().Get("dirs"), ",") {
		before := map[string]bool{}
		for _, c := range d.g.Chats() {
			before[c.ID] = true
		}
		if _, err := d.g.PostAsUser(control.ID, "new "+dir); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var work fakegraph.ChatInfo
		end := time.Now().Add(5 * time.Minute)
		for work.ID == "" && time.Now().Before(end) {
			for _, c := range d.g.Chats() {
				if !before[c.ID] && !strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
					work = c
				}
			}
			time.Sleep(250 * time.Millisecond)
		}
		if work.ID == "" || !d.waitSUT(work.ID, "Work chat is ready", 5*time.Minute) {
			http.Error(w, "work chat for "+dir+" not ready", 500)
			return
		}
		d.mu.Lock()
		d.work = append(d.work, work)
		d.mu.Unlock()
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"control": control.ID, "work": d.work})
}

// traffic starts or stops posting one prompt every `every`, round robin
// over the work chats.
func (d *drill) traffic(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stop != nil {
		d.stop()
		d.stop = nil
	}
	if q.Get("on") != "1" {
		_, _ = fmt.Fprintf(w, "stopped after %d prompts\n", len(d.ps))
		return
	}
	every, err := time.ParseDuration(q.Get("every"))
	if err != nil || every <= 0 {
		every = 4 * time.Second
	}
	prefix := q.Get("prefix")
	if prefix == "" {
		prefix = "X"
	}
	work := append([]fakegraph.ChatInfo(nil), d.work...)
	if len(work) == 0 {
		http.Error(w, "no work chats", 400)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.stop = cancel
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			d.mu.Lock()
			d.n++
			n := d.n
			d.mu.Unlock()
			c := work[n%len(work)]
			tok := fmt.Sprintf("E2EDRILL-%s-%05d", prefix, n)
			at := time.Now()
			if _, err := d.g.PostAsUser(c.ID, "Drill prompt "+tok); err != nil {
				continue
			}
			d.mu.Lock()
			d.ps = append(d.ps, prompt{Token: tok, Chat: c.ID, At: at})
			d.mu.Unlock()
		}
	}()
	_, _ = fmt.Fprintf(w, "posting every %s into %d chats\n", every, len(work))
}

type dumpMsg struct {
	ID       string    `json:"id"`
	Chat     string    `json:"chat"`
	At       time.Time `json:"at"`
	Origin   string    `json:"origin"`
	Instance string    `json:"instance"`
	Seq      int64     `json:"seq"`
	Text     string    `json:"text"`
}

type dumpReq struct {
	Seq      int64     `json:"seq"`
	At       time.Time `json:"at"`
	Instance string    `json:"instance"`
	Host     string    `json:"host"`
	Method   string    `json:"method"`
	Route    string    `json:"route"`
	Chat     string    `json:"chat"`
	Status   int       `json:"status"`
	Tokens   []string  `json:"tokens,omitempty"`
}

func (d *drill) dump(w http.ResponseWriter, r *http.Request) {
	out := struct {
		Prompts  []prompt                   `json:"prompts"`
		Marks    []mark                     `json:"marks"`
		Work     []fakegraph.ChatInfo       `json:"work"`
		Chats    []fakegraph.ChatInfo       `json:"chats"`
		Messages []dumpMsg                  `json:"messages"`
		Requests []dumpReq                  `json:"requests"`
		Blocked  []fakegraph.BlockedRequest `json:"blocked"`
		Viol     []fakegraph.Violation      `json:"violations"`
	}{}
	d.mu.Lock()
	out.Prompts = append(out.Prompts, d.ps...)
	out.Marks = append(out.Marks, d.marks...)
	out.Work = append(out.Work, d.work...)
	d.mu.Unlock()
	out.Chats = d.g.Chats()
	for _, m := range d.g.AllMessages() {
		text := harness.NormalizeSpace(m.PlainText())
		if len(text) > 300 {
			text = text[:300]
		}
		out.Messages = append(out.Messages, dumpMsg{ID: m.ID, Chat: m.ChatID, At: m.Created, Origin: string(m.Origin),
			Instance: m.Instance, Seq: m.RequestSeq, Text: text})
	}
	for _, q := range d.g.Requests(fakegraph.RequestFilter{}) {
		var toks []string
		if q.Method != "GET" {
			toks = harness.TokenPattern.FindAllString(q.Body, -1)
		}
		out.Requests = append(out.Requests, dumpReq{Seq: q.Seq, At: q.At, Instance: q.Instance, Host: q.Host,
			Method: q.Method, Route: q.Route, Chat: q.ChatID, Status: q.Status, Tokens: toks})
	}
	out.Blocked = d.g.Blocked()
	out.Viol = d.g.Violations()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// ctl calls the control API: ctl <METHOD> <path> [key=value ...].
func ctl(args []string) error {
	if len(args) < 2 {
		return errors.New("usage: ctl GET|POST <path> [key=value ...]")
	}
	base := os.Getenv("DRILL_CTL")
	if base == "" {
		base = "http://127.0.0.1:8090"
	}
	q := url.Values{}
	for _, kv := range args[2:] {
		k, v, _ := strings.Cut(kv, "=")
		q.Set(k, v)
	}
	req, err := http.NewRequest(args[0], base+args[1]+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 30 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(os.Stdout, resp.Body)
	if err == nil && resp.StatusCode >= 300 {
		err = fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return err
}

func tlsproxy(args []string) error {
	fs := flag.NewFlagSet("tlsproxy", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:443", "")
	to := fs.String("to", "http://cxpd-graph:8080", "")
	caPath := fs.String("ca", "/run/drill/ca.pem", "")
	instance := fs.String("instance", "", "")
	_ = fs.Parse(args)
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "clusterdrill CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(240 * time.Hour), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644); err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "graph.microsoft.com"},
		DNSNames:  []string{"graph.microsoft.com", "login.microsoftonline.com", "*.sharepoint.com"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(240 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	caCert, _ := x509.ParseCertificate(caDER)
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, caCert, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	target, err := url.Parse(*to)
	if err != nil {
		return err
	}
	rp := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		host := r.In.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		r.SetURL(target)
		r.Out.Host = target.Host
		r.Out.Header.Set(fakegraph.HeaderOriginalHost, host)
		r.Out.Header.Set(fakegraph.HeaderOriginalScheme, "https")
		if *instance != "" {
			r.Out.Header.Set(fakegraph.HeaderInstance, *instance)
		}
	}}
	srv := &http.Server{Addr: *listen, Handler: rp, TLSConfig: &tls.Config{Certificates: []tls.Certificate{{
		Certificate: [][]byte{leafDER}, PrivateKey: key}}}}
	return srv.ListenAndServeTLS("", "")
}

// probe polls for -file with several lookup methods at once and prints how
// long after the start each first saw it: a plain stat (the kill-switch
// check), an open of the file, a stat after opening the directory, and a
// directory listing.
func probe(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	file := fs.String("file", "", "")
	every := fs.Duration("every", 100*time.Millisecond, "")
	limit := fs.Duration("limit", 5*time.Minute, "")
	only := fs.String("method", "", "one method (methods share the client's caches)")
	_ = fs.Parse(args)
	dir, name := filepath.Dir(*file), filepath.Base(*file)
	methods := []struct {
		name string
		seen func() bool
	}{
		{"stat", func() bool { _, err := os.Stat(*file); return err == nil }},
		{"open", func() bool {
			f, err := os.Open(*file)
			if err == nil {
				f.Close()
			}
			return err == nil
		}},
		{"opendir+stat", func() bool {
			if d, err := os.Open(dir); err == nil {
				d.Close()
			}
			_, err := os.Stat(*file)
			return err == nil
		}},
		{"readdir", func() bool {
			es, _ := os.ReadDir(dir)
			for _, e := range es {
				if e.Name() == name {
					return true
				}
			}
			return false
		}},
	}
	if *only != "" {
		for _, m := range methods {
			if m.name == *only {
				methods = append(methods[:0], m)
				break
			}
		}
	}
	start := time.Now()
	found := make([]time.Duration, len(methods))
	for left := len(methods); left > 0 && time.Since(start) < *limit; time.Sleep(*every) {
		for i, m := range methods {
			if found[i] == 0 && m.seen() {
				found[i] = time.Since(start)
				left--
				lag := "?"
				if fi, err := os.Stat(*file); err == nil {
					lag = fmt.Sprintf("%.2fs", time.Since(fi.ModTime()).Seconds())
				}
				fmt.Printf("%s seen %s after it was created\n", m.name, lag)
			}
		}
	}
	return nil
}
