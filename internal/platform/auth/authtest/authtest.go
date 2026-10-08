// Package authtest é um provedor de identidade OIDC mínimo para testes: gera
// um par de chaves RSA, publica o JWKS por HTTP e emite tokens assinados.
// Os tokens passam pelo auth.Verifier real (assinatura e claims verificadas).
// Os testes contra o Keycloak de verdade ficam na suíte de integração.
package authtest

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/platform/auth"
)

const (
	Issuer   = "http://idp.test/realms/wager"
	Audience = "wager-api"
)

// IdP é o provedor de identidade de teste.
type IdP struct {
	Server *httptest.Server
	key    *rsa.PrivateKey
}

// New cria o IdP e o serviço de JWKS.
func New(t testing.TB) *IdP {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &IdP{key: k}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": "test-key", "use": "sig", "alg": "RS256",
			"n": b64(k.N.Bytes()), "e": b64(big.NewInt(int64(k.E)).Bytes()),
		}}})
	}))
	t.Cleanup(p.Server.Close)
	return p
}

// Verifier devolve um auth.Verifier real apontado para este IdP.
func (p *IdP) Verifier() *auth.Verifier {
	return auth.NewVerifier(auth.Config{Issuer: Issuer, JWKSURL: p.Server.URL, Audience: Audience})
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// Token emite um token com as claims padrão mais as extras informadas.
func (p *IdP) Token(roles []string, providerID string, extra map[string]any) string {
	now := time.Now()
	claims := map[string]any{
		"iss": Issuer, "sub": "svc-" + providerID, "aud": []string{Audience}, "azp": "client-" + providerID,
		"exp": now.Add(5 * time.Minute).Unix(), "nbf": now.Add(-time.Minute).Unix(), "iat": now.Unix(),
		"realm_access": map[string]any{"roles": roles},
	}
	if providerID != "" {
		claims["provider_id"] = providerID
	}
	for k, v := range extra {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}
	h, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "test-key", "typ": "JWT"})
	c, _ := json.Marshal(claims)
	in := b64(h) + "." + b64(c)
	sum := sha256.Sum256([]byte(in))
	sig, err := rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA256, sum[:])
	if err != nil {
		panic(err)
	}
	return in + "." + b64(sig)
}

// Internal emite um token do serviço interno.
func (p *IdP) Internal() string { return p.Token([]string{auth.RoleInternal}, "", nil) }

// Provider emite um token de provedor de jogos.
func (p *IdP) Provider(providerID string) string {
	return p.Token([]string{auth.RoleProvider}, providerID, nil)
}
