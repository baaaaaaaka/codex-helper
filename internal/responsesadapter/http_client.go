package responsesadapter

import (
	"net"
	"net/http"
	"net/url"
	"time"
)

var (
	upstreamHTTPDialTimeout           = 30 * time.Second
	upstreamHTTPTLSHandshakeTimeout   = 10 * time.Second
	upstreamHTTPResponseHeaderTimeout = 2 * time.Minute
	upstreamHTTPExpectContinueTimeout = time.Second
	upstreamHTTPIdleConnTimeout       = 90 * time.Second
	upstreamHTTPStreamIdleTimeout     = 5 * time.Minute
)

func NewUpstreamHTTPClient(proxy func(*http.Request) (*url.URL, error)) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = proxy
	tr.DialContext = (&net.Dialer{Timeout: upstreamHTTPDialTimeout, KeepAlive: 30 * time.Second}).DialContext
	tr.TLSHandshakeTimeout = upstreamHTTPTLSHandshakeTimeout
	tr.ResponseHeaderTimeout = upstreamHTTPResponseHeaderTimeout
	tr.ExpectContinueTimeout = upstreamHTTPExpectContinueTimeout
	tr.IdleConnTimeout = upstreamHTTPIdleConnTimeout
	return &http.Client{Transport: tr}
}

func defaultUpstreamHTTPClient() *http.Client {
	return NewUpstreamHTTPClient(http.ProxyFromEnvironment)
}
