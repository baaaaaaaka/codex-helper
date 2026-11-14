package teams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

type tokenIdentity struct{ Tenant, User, App string }

// Tokens come only from our configured OAuth token sources. Reading their
// claims is a routing restriction, not token/signature authentication. Opaque
// tokens remain usable normally but cannot enable fallback.
func identityOf(token string) (tokenIdentity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return tokenIdentity{}, errors.New("read fallback: token identity unavailable")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return tokenIdentity{}, errors.New("read fallback: token identity unavailable")
	}
	var claims struct {
		Tenant string `json:"tid"`
		User   string `json:"oid"`
		App    string `json:"appid"`
		AZP    string `json:"azp"`
	}
	if json.Unmarshal(raw, &claims) != nil {
		return tokenIdentity{}, errors.New("read fallback: token identity unavailable")
	}
	if claims.App == "" {
		claims.App = claims.AZP
	}
	if claims.Tenant == "" || claims.User == "" || claims.App == "" {
		return tokenIdentity{}, errors.New("read fallback: token identity unavailable")
	}
	return tokenIdentity{claims.Tenant, claims.User, claims.App}, nil
}

type identitySource struct {
	source  TokenSource
	primary tokenIdentity
}

func sameIdentitySource(ctx context.Context, primary, full TokenSource) (TokenSource, error) {
	token, err := primary.Token(ctx)
	if err != nil {
		return nil, errors.New("read fallback: primary identity unavailable")
	}
	id, err := identityOf(token)
	if err != nil {
		return nil, err
	}
	return identitySource{full, id}, nil
}

func (s identitySource) check(token string, err error) (string, error) {
	if err != nil {
		return "", err
	}
	id, err := identityOf(token)
	if err != nil {
		return "", err
	}
	if id.Tenant != s.primary.Tenant || id.User != s.primary.User || id.App == s.primary.App {
		return "", errors.New("read fallback: credentials must have the same tenant/user and different applications")
	}
	return token, nil
}

func (s identitySource) Token(ctx context.Context) (string, error) {
	return s.check(s.source.Token(ctx))
}
func (s identitySource) Refresh(ctx context.Context, stale string) (string, error) {
	return s.check(s.source.Refresh(ctx, stale))
}
