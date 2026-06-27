package responsespolicy

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func newCapabilityToken() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate loopback capability token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
