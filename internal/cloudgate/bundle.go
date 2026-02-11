package cloudgate

import (
	"fmt"
	"os"
)

// knownCABundlePaths is the set of common system CA bundle locations.
var knownCABundlePaths = []string{
	"/etc/ssl/certs/ca-certificates.crt", // Debian/Ubuntu
	"/etc/pki/tls/certs/ca-bundle.crt",   // RHEL/CentOS
	"/etc/ssl/cert.pem",                  // Alpine/macOS
}

// CreateBundle writes a temporary file containing the system CA certificates
// plus our MITM CA PEM. Returns the temp file path.
func CreateBundle(ca *CA) (string, error) {
	sysPath := systemCABundlePath()
	if sysPath == "" {
		return "", fmt.Errorf("system CA bundle not found")
	}

	sysBundle, err := os.ReadFile(sysPath)
	if err != nil {
		return "", fmt.Errorf("read system bundle: %w", err)
	}

	combined := append(sysBundle, '\n')
	combined = append(combined, ca.CertPEM...)

	f, err := os.CreateTemp("", "codex-proxy-bundle-*.pem")
	if err != nil {
		return "", fmt.Errorf("create temp bundle: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(combined); err != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("write bundle: %w", err)
	}

	return f.Name(), nil
}

// systemCABundlePath returns the path to the system CA certificate bundle,
// or empty string if none found.
func systemCABundlePath() string {
	// Check SSL_CERT_FILE env first.
	if p := os.Getenv("SSL_CERT_FILE"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	for _, p := range knownCABundlePaths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}
