//go:build integration

package httpapi_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	httpapi "github.com/monii/backend-challenge-go/internal/http"
	"github.com/monii/backend-challenge-go/internal/http/health"
	"github.com/monii/backend-challenge-go/internal/platform/auth/authtest"
	"github.com/monii/backend-challenge-go/internal/platform/metrics"
	"github.com/monii/backend-challenge-go/internal/platform/postgres"
	"github.com/monii/backend-challenge-go/internal/platform/postgres/pgtest"
)

// Três instâncias independentes da API (cada uma com seu serviço, seu cache
// de chaves e seu registro de métricas) sobre o MESMO PostgreSQL real.
func threeInstances(t *testing.T) (urls []string, idp *authtest.IdP) {
	t.Helper()
	pool := pgtest.NewDatabase(t)
	store := postgres.NewStore(pool)
	idp = authtest.New(t)
	for i := 0; i < 3; i++ {
		svc := wagering.NewService(store, wagering.DefaultConfig(), wagering.WithInstanceID(string(rune('a'+i))))
		api := httpapi.NewAPI(svc, idp.Verifier(), slog.New(slog.NewJSONHandler(io.Discard, nil)), metrics.New(), health.NewHandler())
		srv := httptest.NewServer(api.Routes())
		t.Cleanup(srv.Close)
		urls = append(urls, srv.URL)
	}
	return urls, idp
}

func TestHTTPConcurrencyAcrossInstances(t *testing.T) {
	urls, idp := threeInstances(t)
	e := &env{t: t, srv: &httptest.Server{URL: urls[0]}, idp: idp}
	wallet, player := e.openWallet("100.00")
	provider := idp.Provider("provider-a")
	call := func(base, key, body string) resp {
		e2 := &env{t: t, srv: &httptest.Server{URL: base}, idp: idp}
		return e2.do(http.MethodPost, "/wagering/transactions", provider, key, body)
	}

	t.Run("50 identical bets", func(t *testing.T) {
		body := betBody("same", wallet, player, "BET", "10.00", "")
		var mu sync.Mutex
		fresh, replay := 0, 0
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				r := call(urls[i%3], "key-same", body)
				mu.Lock()
				defer mu.Unlock()
				if r.Code != 200 || r.str("balance", "amount") != "90.00" {
					t.Errorf("%d %s", r.Code, r.Raw)
				}
				if r.Body["idempotentReplay"] == true {
					replay++
				} else {
					fresh++
				}
			}(i)
		}
		wg.Wait()
		if fresh != 1 || replay != 49 {
			t.Fatalf("fresh=%d replay=%d", fresh, replay)
		}
	})

	t.Run("two 80.00 bets over the remaining balance", func(t *testing.T) {
		w2, p2 := e.openWallet("100.00")
		codes := make([]int, 2)
		fail := make([]string, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ext := "eighty-" + string(rune('a'+i))
				r := call(urls[i], "key-"+ext, betBody(ext, w2, p2, "BET", "80.00", ""))
				codes[i], fail[i] = r.Code, r.str("failureCode")
			}(i)
		}
		wg.Wait()
		ok, rejected := 0, 0
		for i, c := range codes {
			switch c {
			case 200:
				ok++
			case 422:
				rejected++
				if fail[i] != "INSUFFICIENT_FUNDS" {
					t.Errorf("failure code %q", fail[i])
				}
			}
		}
		if ok != 1 || rejected != 1 {
			t.Fatalf("codes %v", codes)
		}
		if w := e.do("GET", "/wallets/"+w2, idp.Internal(), "", ""); w.str("balance", "amount") != "20.00" {
			t.Fatalf("balance %s", w.Raw)
		}
		if rc := e.do("POST", "/wallets/"+w2+"/reconciliation", idp.Internal(), "", ""); rc.Body["consistent"] != true {
			t.Fatalf("reconciliation %s", rc.Raw)
		}
	})
	_ = id.New
}
