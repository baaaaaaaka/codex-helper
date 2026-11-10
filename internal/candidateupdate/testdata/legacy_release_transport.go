// This file is injected ONLY into the frozen rc.96 test build. Its main,
// downloader, validator, updater, bootstrap and CLI remain historical source.
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type fixtureReleaseTransport struct {
	base   http.RoundTripper
	origin *url.URL
}

func (f fixtureReleaseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != "github.com" || !strings.Contains(r.URL.Path, "/releases/download/") {
		return nil, fmt.Errorf("unexpected network request in frozen upgrade fixture: %s", r.URL)
	}
	r = r.Clone(r.Context())
	u := *r.URL
	r.URL = &u
	r.URL.Scheme, r.URL.Host = f.origin.Scheme, f.origin.Host
	return f.base.RoundTrip(r)
}

func init() {
	if len(os.Args) == 2 && os.Args[1] == "__fixture-hold-entry" {
		fmt.Fprintln(os.Stdout, "mapped")
		io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}

	if len(os.Args) == 3 && os.Args[1] == "__fixture-last-core-write" {
		s, err := store.Open(os.Args[2])
		if err == nil {
			_, err = s.Acquire(store.Ident{Host: "upgrade-fixture", PID: os.Getpid(), BootID: "fixture"})
			if err == nil {
				err = s.Update(func(tx *store.Tx) error {
					return tx.SetMeta("engine_update_pending", `{"tag":"v9.99.0","from":"v0.1.22-rc.96"}`)
				})
			}
			s.Close()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if value := os.Getenv("CXP_LEGACY_RELEASE_ORIGIN"); value != "" {
		u, err := url.Parse(value)
		if err != nil || u.Scheme != "http" || !strings.HasPrefix(u.Host, "127.0.0.1:") {
			panic("invalid fixture origin")
		}
		http.DefaultTransport = fixtureReleaseTransport{http.DefaultTransport, u}
	}
}
