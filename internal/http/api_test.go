package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/application/wagering/wageringtest"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	httpapi "github.com/monii/backend-challenge-go/internal/http"
	"github.com/monii/backend-challenge-go/internal/http/health"
	"github.com/monii/backend-challenge-go/internal/platform/auth"
	"github.com/monii/backend-challenge-go/internal/platform/auth/authtest"
	"github.com/monii/backend-challenge-go/internal/platform/metrics"
)

type env struct {
	t   *testing.T
	srv *httptest.Server
	idp *authtest.IdP
}

func newEnv(t *testing.T) *env {
	t.Helper()
	idp := authtest.New(t)
	store := wageringtest.NewMemStore()
	svc := wagering.NewService(store, wagering.DefaultConfig())
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	api := httpapi.NewAPI(svc, idp.Verifier(), log, metrics.New(), health.NewHandler())
	srv := httptest.NewServer(api.Routes())
	t.Cleanup(srv.Close)
	return &env{t: t, srv: srv, idp: idp}
}

type resp struct {
	Code   int
	Header http.Header
	Body   map[string]any
	Raw    string
}

func (e *env) do(method, path, token, key, body string) resp {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	out := resp{Code: r.StatusCode, Header: r.Header, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

func (r resp) str(path ...string) string {
	var cur any = r.Body
	for _, p := range path {
		m, _ := cur.(map[string]any)
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func (e *env) openWallet(initial string) (walletID, playerID string) {
	e.t.Helper()
	playerID = id.New().String()
	r := e.do("POST", "/wallets", e.idp.Internal(), "",
		`{"playerId":"`+playerID+`","initialBalance":{"amount":"`+initial+`","currency":"BRL"}}`)
	if r.Code != 201 {
		e.t.Fatalf("open wallet: %d %s", r.Code, r.Raw)
	}
	return r.str("id"), playerID
}

func betBody(ext, wallet, player, kind, amount, ref string) string {
	b := map[string]any{
		"externalTransactionId": ext, "playerId": player, "walletId": wallet, "roundId": "r1", "gameId": "g1",
		"kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		b["referenceExternalTransactionId"] = ref
	}
	raw, _ := json.Marshal(b)
	return string(raw)
}

func TestAuthentication(t *testing.T) {
	e := newEnv(t)
	wallet, _ := e.openWallet("10.00")
	provider := e.idp.Provider("provider-a")

	expired := e.idp.Token([]string{auth.RoleInternal}, "", map[string]any{"exp": time.Now().Add(-time.Hour).Unix()})
	noProviderID := e.idp.Token([]string{auth.RoleProvider}, "", nil)
	cases := []struct {
		name, method, path, token string
		want                      int
	}{
		{"no token", "GET", "/wallets/" + wallet, "", 401},
		{"garbage token", "GET", "/wallets/" + wallet, "abc.def.ghi", 401},
		{"expired token", "GET", "/wallets/" + wallet, expired, 401},
		{"provider cannot read wallets", "GET", "/wallets/" + wallet, provider, 403},
		{"provider cannot open wallets", "POST", "/wallets", provider, 403},
		{"provider cannot reconcile", "POST", "/wallets/" + wallet + "/reconciliation", provider, 403},
		{"internal cannot submit", "POST", "/wagering/transactions", e.idp.Internal(), 403},
		{"provider without provider_id", "POST", "/wagering/transactions", noProviderID, 403},
		{"internal ok", "GET", "/wallets/" + wallet, e.idp.Internal(), 200},
		{"health is public", "GET", "/health/live", "", 200},
	}
	for _, c := range cases {
		if r := e.do(c.method, c.path, c.token, "", ""); r.Code != c.want {
			t.Errorf("%s: got %d want %d (%s)", c.name, r.Code, c.want, r.Raw)
		}
	}
	if r := e.do("GET", "/wallets/"+wallet, "", "", ""); r.Header.Get("WWW-Authenticate") == "" {
		t.Error("401 must carry WWW-Authenticate")
	}
}

func TestWalletEndpoints(t *testing.T) {
	e := newEnv(t)
	tok := e.idp.Internal()
	wallet, player := e.openWallet("100.00")

	r := e.do("GET", "/wallets/"+wallet, tok, "", "")
	if r.Code != 200 || r.str("balance", "amount") != "100.00" || r.Body["version"].(float64) != 1 {
		t.Fatalf("%d %s", r.Code, r.Raw)
	}
	dup := e.do("POST", "/wallets", tok, "", `{"playerId":"`+player+`","initialBalance":{"amount":"1.00","currency":"BRL"}}`)
	if dup.Code != 409 || dup.str("error", "code") != "WALLET_ALREADY_EXISTS" {
		t.Fatalf("duplicate: %d %s", dup.Code, dup.Raw)
	}
	for name, body := range map[string]string{
		"float amount":    `{"playerId":"` + id.New().String() + `","initialBalance":{"amount":10.5,"currency":"BRL"}}`,
		"scientific":      `{"playerId":"` + id.New().String() + `","initialBalance":{"amount":"1e3","currency":"BRL"}}`,
		"negative":        `{"playerId":"` + id.New().String() + `","initialBalance":{"amount":"-1.00","currency":"BRL"}}`,
		"too many digits": `{"playerId":"` + id.New().String() + `","initialBalance":{"amount":"1.234","currency":"BRL"}}`,
		"no currency":     `{"playerId":"` + id.New().String() + `","initialBalance":{"amount":"1.00"}}`,
		"no balance":      `{"playerId":"` + id.New().String() + `"}`,
		"bad player":      `{"playerId":"nope","initialBalance":{"amount":"1.00","currency":"BRL"}}`,
		"unknown field":   `{"playerId":"` + id.New().String() + `","initialBalance":{"amount":"1.00","currency":"BRL"},"x":1}`,
		"two objects":     `{"playerId":"` + id.New().String() + `","initialBalance":{"amount":"1.00","currency":"BRL"}} {}`,
		"not json":        `hello`,
	} {
		if r := e.do("POST", "/wallets", tok, "", body); r.Code != 400 || r.str("error", "code") != "INVALID_REQUEST" {
			t.Errorf("%s: %d %s", name, r.Code, r.Raw)
		}
	}
	if r := e.do("GET", "/wallets/"+id.New().String(), tok, "", ""); r.Code != 404 {
		t.Errorf("unknown wallet: %d", r.Code)
	}
	if r := e.do("GET", "/wallets/not-a-uuid", tok, "", ""); r.Code != 400 {
		t.Errorf("bad id: %d", r.Code)
	}
}

func TestSubmitFlow(t *testing.T) {
	e := newEnv(t)
	provider := e.idp.Provider("provider-a")
	wallet, player := e.openWallet("100.00")

	// sem Idempotency-Key
	if r := e.do("POST", "/wagering/transactions", provider, "", betBody("b1", wallet, player, "BET", "30.00", "")); r.Code != 400 ||
		r.str("error", "code") != "IDEMPOTENCY_KEY_REQUIRED" {
		t.Fatalf("missing key: %d %s", r.Code, r.Raw)
	}
	body := betBody("b1", wallet, player, "BET", "30.00", "")
	r := e.do("POST", "/wagering/transactions", provider, "key-1", body)
	if r.Code != 200 || r.str("status") != "PROCESSED" || r.str("balance", "amount") != "70.00" || r.Body["idempotentReplay"] != false {
		t.Fatalf("bet: %d %s", r.Code, r.Raw)
	}
	again := e.do("POST", "/wagering/transactions", provider, "key-1", body)
	if again.Code != 200 || again.Body["idempotentReplay"] != true || again.str("balance", "amount") != "70.00" ||
		again.str("transactionId") != r.str("transactionId") {
		t.Fatalf("replay: %d %s", again.Code, again.Raw)
	}
	// mesma chave, conteúdo diferente
	if c := e.do("POST", "/wagering/transactions", provider, "key-1", betBody("b1", wallet, player, "BET", "31.00", "")); c.Code != 409 ||
		c.str("error", "code") != "IDEMPOTENCY_KEY_CONFLICT" {
		t.Fatalf("conflict: %d %s", c.Code, c.Raw)
	}
	// saldo insuficiente: 422 com código
	rej := e.do("POST", "/wagering/transactions", provider, "key-2", betBody("b2", wallet, player, "BET", "80.00", ""))
	if rej.Code != 422 || rej.str("status") != "REJECTED" || rej.str("failureCode") != "INSUFFICIENT_FUNDS" || rej.str("balance", "amount") != "70.00" {
		t.Fatalf("rejected: %d %s", rej.Code, rej.Raw)
	}
	// estorno antes da referência: 202
	pend := e.do("POST", "/wagering/transactions", provider, "key-3", betBody("rf1", wallet, player, "REFUND", "5.00", "bet-later"))
	if pend.Code != 202 || pend.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("pending: %d %s", pend.Code, pend.Raw)
	}
	// refund de verdade
	ok := e.do("POST", "/wagering/transactions", provider, "key-4", betBody("rf2", wallet, player, "REFUND", "30.00", "b1"))
	if ok.Code != 200 || ok.str("balance", "amount") != "100.00" {
		t.Fatalf("refund: %d %s", ok.Code, ok.Raw)
	}
	// consulta por id externo (do próprio provedor)
	g := e.do("GET", "/providers/provider-a/wagering/transactions/b1", provider, "", "")
	if g.Code != 200 || g.str("status") != "PROCESSED" {
		t.Fatalf("get: %d %s", g.Code, g.Raw)
	}
	// consulta por id interno
	byID := e.do("GET", "/wagering/transactions/"+g.str("transactionId"), provider, "", "")
	if byID.Code != 200 || byID.str("externalTransactionId") != "b1" {
		t.Fatalf("get by id: %d %s", byID.Code, byID.Raw)
	}
	// a leitura mostra a transação pendente e o código de rejeição
	if r := e.do("GET", "/providers/provider-a/wagering/transactions/rf1", provider, "", ""); r.Code != 200 || r.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("pending read: %d %s", r.Code, r.Raw)
	}
	if r := e.do("GET", "/providers/provider-a/wagering/transactions/b2", provider, "", ""); r.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejected read: %d %s", r.Code, r.Raw)
	}
	// providerId no corpo é opcional, mas precisa ser o do token
	withProv := func(pid string) string {
		var m map[string]any
		_ = json.Unmarshal([]byte(betBody("pv1", wallet, player, "BET", "1.00", "")), &m)
		m["providerId"] = pid
		raw, _ := json.Marshal(m)
		return string(raw)
	}
	if r := e.do("POST", "/wagering/transactions", provider, "key-pv-bad", withProv("provider-b")); r.Code != 403 || r.str("error", "code") != "PROVIDER_MISMATCH" {
		t.Fatalf("provider mismatch: %d %s", r.Code, r.Raw)
	}
	if r := e.do("POST", "/wagering/transactions", provider, "key-pv-ok", withProv("provider-a")); r.Code != 200 {
		t.Fatalf("provider match: %d %s", r.Code, r.Raw)
	}
}

func TestSubmitValidation(t *testing.T) {
	e := newEnv(t)
	provider := e.idp.Provider("provider-a")
	wallet, player := e.openWallet("100.00")
	good := map[string]any{
		"externalTransactionId": "x1", "playerId": player, "walletId": wallet, "roundId": "r", "gameId": "g",
		"kind": "BET", "money": map[string]string{"amount": "1.00", "currency": "BRL"},
	}
	with := func(k string, v any) string {
		m := map[string]any{}
		for a, b := range good {
			m[a] = b
		}
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
		raw, _ := json.Marshal(m)
		return string(raw)
	}
	cases := map[string]string{
		"kind OPENING":        with("kind", "OPENING"),
		"unknown kind":        with("kind", "FOO"),
		"missing kind":        with("kind", nil),
		"missing money":       with("money", nil),
		"amount as number":    with("money", map[string]any{"amount": 1.0, "currency": "BRL"}),
		"NaN":                 with("money", map[string]string{"amount": "NaN", "currency": "BRL"}),
		"Infinity":            with("money", map[string]string{"amount": "Infinity", "currency": "BRL"}),
		"zero bet":            with("money", map[string]string{"amount": "0.00", "currency": "BRL"}),
		"bad wallet id":       with("walletId", "x"),
		"missing external id": with("externalTransactionId", nil),
		"reference on BET":    with("referenceExternalTransactionId", "z"),
		"loss with amount":    with("kind", "LOSS"),
		"empty round":         with("roundId", ""),
		"external id padding": with("externalTransactionId", " x1"),
	}
	for name, body := range cases {
		if r := e.do("POST", "/wagering/transactions", provider, "k-"+id.New().String(), body); r.Code != 400 {
			t.Errorf("%s: got %d %s", name, r.Code, r.Raw)
		}
	}
	// nada disso deve ter movimentado a carteira
	if r := e.do("GET", "/wallets/"+wallet, e.idp.Internal(), "", ""); r.str("balance", "amount") != "100.00" {
		t.Fatalf("balance changed: %s", r.Raw)
	}
	if r := e.do("POST", "/wagering/transactions", provider, "k-unknown-wallet",
		betBody("w1", id.New().String(), player, "BET", "1.00", "")); r.Code != 404 {
		t.Errorf("unknown wallet: %d", r.Code)
	}
}

func TestProviderIsolation(t *testing.T) {
	e := newEnv(t)
	a, b := e.idp.Provider("provider-a"), e.idp.Provider("provider-b")
	wallet, player := e.openWallet("100.00")

	ra := e.do("POST", "/wagering/transactions", a, "ka", betBody("same-id", wallet, player, "BET", "10.00", ""))
	if ra.Code != 200 {
		t.Fatalf("a: %d %s", ra.Code, ra.Raw)
	}
	// provider-b não enxerga a transação de provider-a, por nenhuma das rotas
	if r := e.do("GET", "/providers/provider-b/wagering/transactions/same-id", b, "", ""); r.Code != 404 {
		t.Fatalf("b sees a's transaction by external id: %d %s", r.Code, r.Raw)
	}
	if r := e.do("GET", "/providers/provider-a/wagering/transactions/same-id", b, "", ""); r.Code != 403 {
		t.Fatalf("b reads a's provider path: %d %s", r.Code, r.Raw)
	}
	if r := e.do("GET", "/wagering/transactions/"+ra.str("transactionId"), b, "", ""); r.Code != 404 {
		t.Fatalf("b sees a's transaction by id: %d %s", r.Code, r.Raw)
	}
	// repetir a requisição de a não vaza nada para b e continua sendo replay de a
	if r := e.do("POST", "/wagering/transactions", a, "ka", betBody("same-id", wallet, player, "BET", "10.00", "")); r.Code != 200 || r.Body["idempotentReplay"] != true {
		t.Fatalf("a replay: %d %s", r.Code, r.Raw)
	}
	// o mesmo id externo para outro provedor é outra operação
	if r := e.do("POST", "/wagering/transactions", b, "kb", betBody("same-id", wallet, player, "BET", "10.00", "")); r.Code != 200 ||
		r.Body["idempotentReplay"] != false {
		t.Fatalf("b: %d %s", r.Code, r.Raw)
	}
	// provider-b não consegue referenciar a aposta de provider-a
	r := e.do("POST", "/wagering/transactions", b, "kb2", betBody("rf", wallet, player, "REFUND", "10.00", "same-id"))
	if r.Code != 200 { // referencia a PRÓPRIA aposta "same-id" do provider-b
		t.Fatalf("b refund own bet: %d %s", r.Code, r.Raw)
	}
	if w := e.do("GET", "/wallets/"+wallet, e.idp.Internal(), "", ""); w.str("balance", "amount") != "90.00" {
		t.Fatalf("balance: %s", w.Raw)
	}
}

func TestLedgerAndReconciliation(t *testing.T) {
	e := newEnv(t)
	tok := e.idp.Internal()
	provider := e.idp.Provider("provider-a")
	wallet, player := e.openWallet("100.00")
	for i, a := range []string{"10.00", "20.00"} {
		ext := "b" + string(rune('1'+i))
		if r := e.do("POST", "/wagering/transactions", provider, "k"+ext, betBody(ext, wallet, player, "BET", a, "")); r.Code != 200 {
			t.Fatal(r.Raw)
		}
	}
	p1 := e.do("GET", "/wallets/"+wallet+"/ledger?limit=2", tok, "", "")
	if p1.Code != 200 || len(p1.Body["entries"].([]any)) != 2 || p1.str("nextCursor") == "" {
		t.Fatalf("page1: %d %s", p1.Code, p1.Raw)
	}
	p2 := e.do("GET", "/wallets/"+wallet+"/ledger?limit=2&cursor="+p1.str("nextCursor"), tok, "", "")
	if len(p2.Body["entries"].([]any)) != 1 || p2.str("nextCursor") != "" {
		t.Fatalf("page2: %s", p2.Raw)
	}
	first := p1.Body["entries"].([]any)[0].(map[string]any)
	if first["direction"] != "CREDIT" || first["balanceAfter"].(map[string]any)["amount"] != "100.00" {
		t.Fatalf("opening entry: %v", first)
	}
	for _, q := range []string{"limit=0", "limit=x", "cursor=%25%25"} {
		if r := e.do("GET", "/wallets/"+wallet+"/ledger?"+q, tok, "", ""); r.Code != 400 {
			t.Errorf("%s: %d", q, r.Code)
		}
	}
	rc := e.do("POST", "/wallets/"+wallet+"/reconciliation", tok, "", "")
	if rc.Code != 200 || rc.Body["consistent"] != true || rc.str("storedBalance", "amount") != "70.00" ||
		rc.str("calculatedBalance", "amount") != "70.00" || rc.str("difference", "amount") != "0.00" ||
		rc.Body["checkedEntries"].(float64) != 3 {
		t.Fatalf("reconciliation: %d %s", rc.Code, rc.Raw)
	}
}

func TestObservability(t *testing.T) {
	e := newEnv(t)
	r := e.do("GET", "/health/live", "", "", "")
	if r.Header.Get("X-Request-Id") == "" {
		t.Fatal("missing X-Request-Id")
	}
	req, _ := http.NewRequest("GET", e.srv.URL+"/health/ready", nil)
	req.Header.Set("X-Request-Id", "abc-123")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.Header.Get("X-Request-Id") != "abc-123" {
		t.Fatal("request id must be propagated")
	}
	m := e.do("GET", "/metrics", "", "", "")
	if m.Code != 200 || !strings.Contains(m.Raw, `http_requests_total{method="GET",route="GET /health/live",status="200"} 1`) {
		t.Fatalf("metrics: %s", m.Raw)
	}
	// erro de corpo não vaza detalhes e leva requestId
	bad := e.do("POST", "/wallets", e.idp.Internal(), "", "{")
	if bad.str("error", "requestId") == "" {
		t.Fatal("error body must carry requestId")
	}
	_ = bytes.NewReader
}
