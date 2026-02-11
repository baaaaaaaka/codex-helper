package cloudgate

import "strings"

// Config holds cloudgate settings for the current session.
type Config struct {
	Hosts map[string]bool
	MITM  *MITMConfig // nil = fingerprinting only
}

// DefaultConfig returns a Config targeting the known ChatGPT hosts.
func DefaultConfig() *Config {
	return &Config{
		Hosts: map[string]bool{
			"chatgpt.com":     true,
			"chat.openai.com": true,
		},
	}
}

// ShouldIntercept reports whether traffic to host should be intercepted.
// The host may include a port suffix which is stripped before lookup.
func (c *Config) ShouldIntercept(host string) bool {
	if c == nil {
		return false
	}
	h := host
	if idx := strings.LastIndex(h, ":"); idx != -1 {
		h = h[:idx]
	}
	return c.Hosts[h]
}

// Cleanup removes any temporary resources (e.g., the SSL bundle file).
func (c *Config) Cleanup() {
	if c == nil || c.MITM == nil {
		return
	}
	c.MITM.Cleanup()
}
