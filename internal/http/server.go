// Package httpapi expõe a API HTTP: rotas, autenticação, erros e métricas.
package httpapi

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/http/health"
	"github.com/monii/backend-challenge-go/internal/platform/auth"
	"github.com/monii/backend-challenge-go/internal/platform/metrics"
)

type apiMetrics struct {
	requests     *metrics.CounterVec
	duration     *metrics.HistogramVec
	transactions *metrics.CounterVec
}

// API reúne as dependências dos handlers.
type API struct {
	svc      *wagering.Service
	verifier auth.TokenVerifier
	log      *slog.Logger
	m        apiMetrics
	health   *health.Handler
	registry *metrics.Registry
}

// NewAPI cria a API.
func NewAPI(svc *wagering.Service, verifier auth.TokenVerifier, log *slog.Logger,
	reg *metrics.Registry, h *health.Handler) *API {
	return &API{
		svc: svc, verifier: verifier, log: log, health: h, registry: reg,
		m: apiMetrics{
			requests: reg.Counter("http_requests_total", "Requisições HTTP.", "method", "route", "status"),
			duration: reg.Histogram("http_request_duration_seconds", "Duração das requisições HTTP.",
				[]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}, "method", "route"),
			transactions: reg.Counter("wager_transactions_total", "Operações de aposta processadas.", "kind", "status"),
		},
	}
}

// Routes monta o roteador.
//
//	Rotas de carteira   → papel wallet-internal (serviço interno)
//	Rotas de aposta     → papel wager-provider (provedor; providerId vem do token)
func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	a.handle(mux, "GET /health/live", "", a.health.Live)
	a.handle(mux, "GET /health/ready", "", a.health.Ready)
	mux.Handle("GET /metrics", a.registry.Handler())

	a.handle(mux, "POST /wallets", auth.RoleInternal, a.openWallet)
	a.handle(mux, "GET /wallets/{id}", auth.RoleInternal, a.getWallet)
	a.handle(mux, "GET /wallets/{id}/ledger", auth.RoleInternal, a.listLedger)
	a.handle(mux, "POST /wallets/{id}/reconciliation", auth.RoleInternal, a.reconcile)

	a.handle(mux, "POST /wagering/transactions", auth.RoleProvider, a.submit)
	a.handle(mux, "GET /wagering/transactions/{externalId}", auth.RoleProvider, a.getTransaction)
	return mux
}

// Server é o servidor HTTP com timeouts seguros.
type Server struct{ srv *http.Server }

// NewServer cria o servidor.
func NewServer(addr string, handler http.Handler) *Server {
	return &Server{srv: &http.Server{
		Addr: addr, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}}
}

// Listen abre a porta (falha cedo se estiver ocupada) e devolve o listener.
func (s *Server) Listen() (net.Listener, error) { return net.Listen("tcp", s.srv.Addr) }

// Serve atende requisições no listener até o Shutdown.
func (s *Server) Serve(l net.Listener) error {
	if err := s.srv.Serve(l); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown encerra o servidor esperando as requisições em andamento.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }
