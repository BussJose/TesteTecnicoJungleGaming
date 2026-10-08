//go:build integration

package httpapi_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	httpapi "github.com/monii/backend-challenge-go/internal/http"
	"github.com/monii/backend-challenge-go/internal/http/health"
	"github.com/monii/backend-challenge-go/internal/platform/auth"
	"github.com/monii/backend-challenge-go/internal/platform/metrics"
	"github.com/monii/backend-challenge-go/internal/platform/postgres"
	"github.com/monii/backend-challenge-go/internal/platform/postgres/pgtest"
)

// Autenticação e autorização com o Keycloak REAL (realm importado de
// keycloak/realm-wager.json) e PostgreSQL real, sem nenhum mock de IdP:
//
//	KEYCLOAK_URL=http://localhost:8081 DATABASE_URL=... \
//	    go test -tags integration -run Keycloak ./internal/http/...

func keycloakToken(t *testing.T, base, client, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {secret}}
	res, err := http.PostForm(base+"/realms/wager/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatalf("token request: %v", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.AccessToken == "" {
		t.Fatalf("token for %s: %d %s", client, res.StatusCode, raw)
	}
	return out.AccessToken
}

func TestKeycloakAuthentication(t *testing.T) {
	base := strings.TrimRight(os.Getenv("KEYCLOAK_URL"), "/")
	if base == "" {
		t.Skip("KEYCLOAK_URL não definida: teste com Keycloak ignorado")
	}
	realm := base + "/realms/wager"
	verifier := auth.NewVerifier(auth.Config{
		Issuer: realm, JWKSURL: realm + "/protocol/openid-connect/certs", Audience: "wager-api",
	})
	store := postgres.NewStore(pgtest.NewDatabase(t))
	svc := wagering.NewService(store, wagering.DefaultConfig())
	api := httpapi.NewAPI(svc, verifier, slog.New(slog.NewJSONHandler(io.Discard, nil)), metrics.New(), health.NewHandler())
	srv := httptest.NewServer(api.Routes())
	t.Cleanup(srv.Close)
	e := &env{t: t, srv: srv}

	internal := keycloakToken(t, base, "wager-internal", "internal-secret")
	provA := keycloakToken(t, base, "provider-a", "provider-a-secret")
	provB := keycloakToken(t, base, "provider-b", "provider-b-secret")

	player := id.New().String()
	open := e.do("POST", "/wallets", internal, "",
		`{"playerId":"`+player+`","initialBalance":{"amount":"100.00","currency":"BRL"}}`)
	if open.Code != 201 {
		t.Fatalf("open wallet: %d %s", open.Code, open.Raw)
	}
	wallet := open.str("id")
	bet := betBody("kc-bet-1", wallet, player, "BET", "30.00", "")

	t.Run("missing, malformed and forged tokens get 401 with no effect", func(t *testing.T) {
		tampered := provA[:len(provA)-4] + "AAAA"
		parts := strings.Split(provA, ".")
		// mesmo cabeçalho e payload, assinatura de outro token
		spliced := parts[0] + "." + parts[1] + "." + strings.Split(provB, ".")[2]
		for name, tok := range map[string]string{
			"none": "", "garbage": "abc.def.ghi", "tampered signature": tampered, "spliced signature": spliced,
		} {
			if r := e.do("POST", "/wagering/transactions", tok, "k-"+name, bet); r.Code != 401 {
				t.Errorf("%s: got %d %s", name, r.Code, r.Raw)
			}
		}
		if r := e.do("GET", "/wallets/"+wallet, internal, "", ""); r.str("balance", "amount") != "100.00" {
			t.Fatalf("unauthorized call moved money: %s", r.Raw)
		}
	})

	t.Run("roles are enforced", func(t *testing.T) {
		if r := e.do("GET", "/wallets/"+wallet, provA, "", ""); r.Code != 403 {
			t.Errorf("provider reading a wallet: %d %s", r.Code, r.Raw)
		}
		if r := e.do("POST", "/wallets/"+wallet+"/reconciliation", provA, "", ""); r.Code != 403 {
			t.Errorf("provider reconciling: %d %s", r.Code, r.Raw)
		}
		if r := e.do("POST", "/wagering/transactions", internal, "k-int", bet); r.Code != 403 {
			t.Errorf("internal client betting: %d %s", r.Code, r.Raw)
		}
		if r := e.do("GET", "/wallets/"+wallet, internal, "", ""); r.str("balance", "amount") != "100.00" {
			t.Fatalf("forbidden call moved money: %s", r.Raw)
		}
	})

	t.Run("providers only see their own transactions", func(t *testing.T) {
		ok := e.do("POST", "/wagering/transactions", provA, "k-a", bet)
		if ok.Code != 200 || ok.str("balance", "amount") != "70.00" {
			t.Fatalf("provider-a bet: %d %s", ok.Code, ok.Raw)
		}
		if r := e.do("GET", "/providers/provider-a/wagering/transactions/kc-bet-1", provA, "", ""); r.Code != 200 {
			t.Errorf("owner read: %d %s", r.Code, r.Raw)
		}
		if r := e.do("GET", "/providers/provider-a/wagering/transactions/kc-bet-1", provB, "", ""); r.Code != 403 {
			t.Errorf("other provider via a's path: %d %s", r.Code, r.Raw)
		}
		if r := e.do("GET", "/providers/provider-b/wagering/transactions/kc-bet-1", provB, "", ""); r.Code != 404 {
			t.Errorf("other provider own path: %d %s", r.Code, r.Raw)
		}
		if r := e.do("GET", "/wagering/transactions/"+ok.str("transactionId"), provB, "", ""); r.Code != 404 {
			t.Errorf("other provider by id: %d %s", r.Code, r.Raw)
		}
		// replay: só o dono recebe o resultado original
		if r := e.do("POST", "/wagering/transactions", provA, "k-a", bet); r.Code != 200 || r.Body["idempotentReplay"] != true {
			t.Errorf("owner replay: %d %s", r.Code, r.Raw)
		}
		// outro provedor com a mesma chave e o mesmo id externo é uma operação independente
		other := e.do("POST", "/wagering/transactions", provB, "k-a", bet)
		if other.Code != 200 || other.Body["idempotentReplay"] != false || other.str("transactionId") == ok.str("transactionId") {
			t.Errorf("provider-b must not see provider-a's result: %d %s", other.Code, other.Raw)
		}
		// providerId forjado no corpo não troca a identidade
		forged := strings.Replace(bet, `{`, `{"providerId":"provider-a",`, 1)
		if r := e.do("POST", "/wagering/transactions", provB, "k-forged", forged); r.Code != 403 {
			t.Errorf("forged providerId: %d %s", r.Code, r.Raw)
		}
	})
}
