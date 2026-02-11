package cloudgate

import "runtime"

// Setup initializes full cloudgate (fingerprinting + MITM on Linux).
// configDir is used to store the CA certificate and key.
func Setup(configDir string) (*Config, error) {
	if runtime.GOOS != "linux" {
		return SetupFingerprintOnly(), nil
	}

	ca, err := EnsureCA(configDir)
	if err != nil {
		return SetupFingerprintOnly(), nil
	}

	bundlePath, err := CreateBundle(ca)
	if err != nil {
		// No system CA bundle — fall back to fingerprinting only.
		return SetupFingerprintOnly(), nil
	}

	cc := NewCertCache(ca)
	cfg := DefaultConfig()
	cfg.MITM = &MITMConfig{
		CA:         ca,
		CertCache:  cc,
		BundlePath: bundlePath,
	}
	return cfg, nil
}

// SetupFingerprintOnly returns a Config with Layer 1 (fingerprinting) only.
func SetupFingerprintOnly() *Config {
	return DefaultConfig()
}
