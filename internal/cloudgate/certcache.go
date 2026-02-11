package cloudgate

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"sync"
	"time"
)

// CertCache caches per-host TLS certificates signed by the MITM CA.
type CertCache struct {
	ca    *CA
	mu    sync.RWMutex
	cache map[string]*tls.Certificate
}

// NewCertCache creates a CertCache backed by the given CA.
func NewCertCache(ca *CA) *CertCache {
	return &CertCache{
		ca:    ca,
		cache: make(map[string]*tls.Certificate),
	}
}

// GetCert returns a TLS certificate for host, generating one if needed.
func (c *CertCache) GetCert(host string) (*tls.Certificate, error) {
	c.mu.RLock()
	if cert, ok := c.cache[host]; ok {
		c.mu.RUnlock()
		return cert, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock.
	if cert, ok := c.cache[host]; ok {
		return cert, nil
	}

	cert, err := c.generate(host)
	if err != nil {
		return nil, err
	}
	c.cache[host] = cert
	return cert, nil
}

func (c *CertCache) generate(host string) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate host key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: host,
		},
		DNSNames:  []string{host},
		NotBefore: time.Now().Add(-1 * time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, c.ca.Cert, &key.PublicKey, c.ca.Key)
	if err != nil {
		return nil, fmt.Errorf("sign host cert: %w", err)
	}

	leaf, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, err
	}

	tlsCert := &tls.Certificate{
		Certificate: [][]byte{certDER, c.ca.Cert.Raw},
		PrivateKey:  key,
		Leaf:        leaf,
	}
	return tlsCert, nil
}
