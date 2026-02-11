package cloudgate

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// CA holds a self-signed certificate authority used for MITM.
type CA struct {
	Cert    *x509.Certificate
	Key     crypto.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// EnsureCA loads or generates an EC P-256 CA stored in configDir.
func EnsureCA(configDir string) (*CA, error) {
	certPath := filepath.Join(configDir, "mitm-ca.pem")
	keyPath := filepath.Join(configDir, "mitm-ca-key.pem")

	if ca, err := loadCA(certPath, keyPath); err == nil {
		return ca, nil
	}

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return nil, fmt.Errorf("create config dir: %w", err)
	}

	return generateCA(certPath, keyPath)
}

func loadCA(certPath, keyPath string) (*CA, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in cert file")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, fmt.Errorf("no PEM block in key file")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, err
	}

	return &CA{
		Cert:    cert,
		Key:     key,
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
	}, nil
}

func generateCA(certPath, keyPath string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "Codex Proxy MITM CA",
			Organization: []string{"Codex Helper"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(5 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return nil, fmt.Errorf("write cert: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return nil, fmt.Errorf("write key: %w", err)
	}

	return &CA{
		Cert:    cert,
		Key:     key,
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
	}, nil
}
