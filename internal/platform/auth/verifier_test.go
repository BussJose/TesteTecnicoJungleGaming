package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testIssuer   = "http://keycloak.test/realms/wager"
	testAudience = "wager-api"
)

type signer struct {
	key *rsa.PrivateKey
	kid string
}

func newSigner(t *testing.T, kid string) *signer {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return &signer{key: k, kid: kid}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (s *signer) jwk() map[string]string {
	return map[string]string{
		"kty": "RSA", "kid": s.kid, "use": "sig", "alg": "RS256",
		"n": b64(s.key.N.Bytes()), "e": b64(big.NewInt(int64(s.key.E)).Bytes()),
	}
}

func (s *signer) sign(t *testing.T, alg string, claims map[string]any) string {
	t.Helper()
	h, _ := json.Marshal(map[string]string{"alg": alg, "kid": s.kid, "typ": "JWT"})
	c, _ := json.Marshal(claims)
	in := b64(h) + "." + b64(c)
	sum := sha256.Sum256([]byte(in))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return in + "." + b64(sig)
}

// jwksServer serve as chaves atuais; contagem de acessos em hits.
type jwksServer struct {
	*httptest.Server
	mu   sync.Mutex
	keys []*signer
	hits atomic.Int32
}

func newJWKS(t *testing.T, keys ...*signer) *jwksServer {
	s := &jwksServer{keys: keys}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		s.mu.Lock()
		defer s.mu.Unlock()
		var list []map[string]string
		for _, k := range s.keys {
			list = append(list, k.jwk())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": list})
	}))
	t.Cleanup(s.Close)
	return s
}

func baseClaims(now time.Time) map[string]any {
	return map[string]any{
		"iss": testIssuer, "sub": "svc-1", "aud": []string{testAudience, "account"},
		"azp": "provider-a", "provider_id": "provider-a",
		"exp": now.Add(5 * time.Minute).Unix(), "nbf": now.Add(-time.Minute).Unix(),
		"realm_access": map[string]any{"roles": []string{RoleProvider}},
	}
}

func TestVerify(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	good := newSigner(t, "k1")
	other := newSigner(t, "k1") // mesma kid, chave diferente
	srv := newJWKS(t, good)
	v := NewVerifier(Config{Issuer: testIssuer, JWKSURL: srv.URL, Audience: testAudience, Now: func() time.Time { return now }})

	mut := func(f func(map[string]any)) map[string]any {
		c := baseClaims(now)
		f(c)
		return c
	}
	cases := []struct {
		name  string
		token func() string
		ok    bool
	}{
		{"valid", func() string { return good.sign(t, "RS256", baseClaims(now)) }, true},
		{"aud as string", func() string { return good.sign(t, "RS256", mut(func(c map[string]any) { c["aud"] = testAudience })) }, true},
		{"expired", func() string {
			return good.sign(t, "RS256", mut(func(c map[string]any) { c["exp"] = now.Add(-time.Hour).Unix() }))
		}, false},
		{"within leeway", func() string {
			return good.sign(t, "RS256", mut(func(c map[string]any) { c["exp"] = now.Add(-10 * time.Second).Unix() }))
		}, true},
		{"not before in the future", func() string {
			return good.sign(t, "RS256", mut(func(c map[string]any) { c["nbf"] = now.Add(time.Hour).Unix() }))
		}, false},
		{"missing exp", func() string { return good.sign(t, "RS256", mut(func(c map[string]any) { delete(c, "exp") })) }, false},
		{"wrong issuer", func() string {
			return good.sign(t, "RS256", mut(func(c map[string]any) { c["iss"] = "http://evil/realms/wager" }))
		}, false},
		{"wrong audience", func() string {
			return good.sign(t, "RS256", mut(func(c map[string]any) { c["aud"] = []string{"other"} }))
		}, false},
		{"missing audience", func() string { return good.sign(t, "RS256", mut(func(c map[string]any) { delete(c, "aud") })) }, false},
		{"forged signature", func() string { return other.sign(t, "RS256", baseClaims(now)) }, false},
		{"alg none", func() string {
			h := b64([]byte(`{"alg":"none","kid":"k1"}`))
			c, _ := json.Marshal(baseClaims(now))
			return h + "." + b64(c) + "."
		}, false},
		{"alg HS256", func() string { return good.sign(t, "HS256", baseClaims(now)) }, false},
		{"garbage", func() string { return "abc" }, false},
		{"empty", func() string { return "" }, false},
		{"tampered payload", func() string {
			tok := good.sign(t, "RS256", baseClaims(now))
			c := baseClaims(now)
			c["provider_id"] = "provider-b"
			raw, _ := json.Marshal(c)
			parts := splitToken(tok)
			return parts[0] + "." + b64(raw) + "." + parts[2]
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, err := v.Verify(context.Background(), c.token())
			if c.ok {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if id.ProviderID != "provider-a" || !id.HasRole(RoleProvider) || id.HasRole(RoleInternal) {
					t.Fatalf("identity %+v", id)
				}
				return
			}
			if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("want ErrUnauthenticated, got %v", err)
			}
		})
	}
}

func splitToken(t string) [3]string {
	var out [3]string
	i, start := 0, 0
	for p := 0; p < len(t); p++ {
		if t[p] == '.' {
			out[i] = t[start:p]
			i++
			start = p + 1
		}
	}
	out[i] = t[start:]
	return out
}

func TestKeyRotationRefreshesJWKS(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	clock := now
	oldKey, newKey := newSigner(t, "old"), newSigner(t, "new")
	srv := newJWKS(t, oldKey)
	v := NewVerifier(Config{Issuer: testIssuer, JWKSURL: srv.URL, Audience: testAudience, Now: func() time.Time { return clock }})

	if _, err := v.Verify(context.Background(), oldKey.sign(t, "RS256", baseClaims(now))); err != nil {
		t.Fatal(err)
	}
	// o Keycloak passa a assinar com outra chave
	srv.mu.Lock()
	srv.keys = []*signer{oldKey, newKey}
	srv.mu.Unlock()

	tok := newKey.sign(t, "RS256", baseClaims(now))
	if _, err := v.Verify(context.Background(), tok); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("refresh too soon must not happen: %v", err)
	}
	hits := srv.hits.Load()
	clock = clock.Add(11 * time.Second)
	if _, err := v.Verify(context.Background(), tok); err != nil {
		t.Fatalf("after refresh window: %v", err)
	}
	if srv.hits.Load() != hits+1 {
		t.Fatal("JWKS must be fetched exactly once more")
	}
}

func TestUnreachableJWKSIsNotAnAuthenticationError(t *testing.T) {
	s := newSigner(t, "k")
	v := NewVerifier(Config{Issuer: testIssuer, JWKSURL: "http://127.0.0.1:1/none", Audience: testAudience})
	_, err := v.Verify(context.Background(), s.sign(t, "RS256", baseClaims(time.Now())))
	if err == nil || errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("want infrastructure error, got %v", err)
	}
}
