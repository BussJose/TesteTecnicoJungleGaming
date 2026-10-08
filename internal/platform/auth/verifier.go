// Package auth valida tokens JWT (RS256) emitidos pelo Keycloak, usando só a
// biblioteca padrão do Go. Verifica assinatura, emissor (iss), audiência
// (aud), validade (exp/nbf) e extrai a identidade (provedor e papéis).
package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Papéis usados pela API (definidos como roles de realm no Keycloak).
const (
	RoleInternal = "wallet-internal" // serviço interno: operações de carteira
	RoleProvider = "wager-provider"  // provedor de jogos: operações de aposta
)

// ErrUnauthenticated indica token ausente, malformado, expirado ou inválido.
var ErrUnauthenticated = errors.New("auth: unauthenticated")

func unauth(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnauthenticated, fmt.Sprintf(format, args...))
}

// Identity é a identidade extraída de um token válido.
type Identity struct {
	Subject    string
	ClientID   string
	ProviderID string // claim "provider_id" (só provedores)
	Roles      map[string]bool
}

// HasRole informa se a identidade tem o papel.
func (i *Identity) HasRole(role string) bool { return i != nil && i.Roles[role] }

// TokenVerifier valida um token e devolve a identidade.
type TokenVerifier interface {
	Verify(ctx context.Context, token string) (*Identity, error)
}

// Config configura o Verifier.
type Config struct {
	Issuer     string        // valor esperado da claim iss
	JWKSURL    string        // endereço das chaves públicas (pode diferir do issuer dentro do Docker)
	Audience   string        // se não vazio, a claim aud deve conter este valor
	HTTPClient *http.Client  // opcional
	Leeway     time.Duration // tolerância de relógio (padrão 30s)
	Now        func() time.Time
}

// Verifier valida JWTs RS256 com chaves obtidas do JWKS.
type Verifier struct {
	cfg Config

	mu      sync.Mutex
	keys    map[string]*rsa.PublicKey
	fetched time.Time
}

// NewVerifier cria o Verifier. As chaves são buscadas sob demanda.
func NewVerifier(cfg Config) *Verifier {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if cfg.Leeway == 0 {
		cfg.Leeway = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Verifier{cfg: cfg, keys: map[string]*rsa.PublicKey{}}
}

type header struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

type claims struct {
	Issuer      string          `json:"iss"`
	Subject     string          `json:"sub"`
	Audience    json.RawMessage `json:"aud"`
	Exp         *float64        `json:"exp"`
	Nbf         *float64        `json:"nbf"`
	Azp         string          `json:"azp"`
	ProviderID  string          `json:"provider_id"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

func decodeSegment(s string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Verify valida o token e devolve a identidade.
func (v *Verifier) Verify(ctx context.Context, token string) (*Identity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, unauth("malformed token")
	}
	var h header
	if err := decodeSegment(parts[0], &h); err != nil {
		return nil, unauth("malformed header")
	}
	if h.Alg != "RS256" { // rejeita "none" e algoritmos simétricos
		return nil, unauth("unsupported algorithm")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, unauth("malformed signature")
	}
	key, err := v.key(ctx, h.Kid)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return nil, unauth("invalid signature")
	}

	var c claims
	if err := decodeSegment(parts[1], &c); err != nil {
		return nil, unauth("malformed claims")
	}
	now := v.cfg.Now()
	if c.Exp == nil {
		return nil, unauth("missing exp")
	}
	if !now.Before(time.Unix(int64(*c.Exp), 0).Add(v.cfg.Leeway)) {
		return nil, unauth("token expired")
	}
	if c.Nbf != nil && now.Add(v.cfg.Leeway).Before(time.Unix(int64(*c.Nbf), 0)) {
		return nil, unauth("token not valid yet")
	}
	if c.Issuer != v.cfg.Issuer {
		return nil, unauth("unexpected issuer")
	}
	if v.cfg.Audience != "" && !audienceContains(c.Audience, v.cfg.Audience) {
		return nil, unauth("unexpected audience")
	}
	id := &Identity{
		Subject: c.Subject, ClientID: c.Azp, ProviderID: c.ProviderID,
		Roles: make(map[string]bool, len(c.RealmAccess.Roles)),
	}
	for _, r := range c.RealmAccess.Roles {
		id.Roles[r] = true
	}
	return id, nil
}

func audienceContains(raw json.RawMessage, want string) bool {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return one == want
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, a := range many {
			if a == want {
				return true
			}
		}
	}
	return false
}

const jwksMinRefresh = 10 * time.Second

// key devolve a chave pública do kid; se não a conhece, atualiza o JWKS
// (no máximo 1 vez a cada 10s) para acompanhar a rotação de chaves.
func (v *Verifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	if !v.fetched.IsZero() && v.cfg.Now().Sub(v.fetched) < jwksMinRefresh {
		return nil, unauth("unknown key id")
	}
	keys, err := v.fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("auth: cannot load signing keys: %w", err)
	}
	v.keys, v.fetched = keys, v.cfg.Now()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	return nil, unauth("unknown key id")
}

type jwks struct {
	Keys []struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		Use string `json:"use"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func (v *Verifier) fetch(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks: status %d", resp.StatusCode)
	}
	var doc jwks
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 1<<20)).Decode(&doc); err != nil {
		return nil, err
	}
	out := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		if k.Kty != "RSA" || k.Use == "enc" {
			continue
		}
		nb, err1 := base64.RawURLEncoding.DecodeString(k.N)
		eb, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if err1 != nil || err2 != nil || len(nb) == 0 || len(eb) == 0 || len(eb) > 4 {
			continue
		}
		e := 0
		for _, b := range eb {
			e = e<<8 | int(b)
		}
		out[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: e}
	}
	if len(out) == 0 {
		return nil, errors.New("jwks: no usable RSA keys")
	}
	return out, nil
}
