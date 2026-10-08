// Package app monta a aplicação com Uber Fx: cria as dependências e liga o
// início e o fim de cada componente ao ciclo de vida do processo.
package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.uber.org/fx"

	"github.com/monii/backend-challenge-go/internal/application/outbox"
	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/config"
	httpapi "github.com/monii/backend-challenge-go/internal/http"
	"github.com/monii/backend-challenge-go/internal/http/health"
	"github.com/monii/backend-challenge-go/internal/messaging"
	"github.com/monii/backend-challenge-go/internal/platform/auth"
	"github.com/monii/backend-challenge-go/internal/platform/metrics"
	"github.com/monii/backend-challenge-go/internal/platform/postgres"
	"github.com/monii/backend-challenge-go/internal/platform/sqs"
)

// Module devolve o grafo de dependências da aplicação.
func Module() fx.Option {
	return fx.Module("app",
		fx.Provide(
			config.Load,
			newLogger,
			metrics.New,
			newPostgresPool,
			newStore,
			newSQSClient,
			newWageringService,
			newVerifier,
			newHealth,
			newAPI,
		),
		fx.Invoke(registerHTTP, registerPendingWorker, registerConsumer, registerOutbox),
	)
}

func newLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With(slog.String("instance", cfg.InstanceID))
}

func newPostgresPool(lc fx.Lifecycle, cfg config.Config) (*postgres.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { pool.Close(); return nil }})
	return pool, nil
}

func newStore(pool *postgres.Pool) *postgres.Store { return postgres.NewStore(pool.Pool) }

func newSQSClient(cfg config.Config) (*sqs.Client, error) {
	return sqs.NewClient(context.Background(), sqs.Options{
		Region: cfg.AWSRegion, Endpoint: cfg.AWSEndpointURL,
		WagerQueueURL: cfg.SQSWagerQueueURL, EventsQueueURL: cfg.SQSEventsQueueURL,
	})
}

func newWageringService(store *postgres.Store, cfg config.Config) *wagering.Service {
	wc := wagering.DefaultConfig()
	wc.ReferenceTTL, wc.MaxReferenceAttempts = cfg.ReferenceTTL, cfg.MaxReferenceAttempts
	wc.BackoffBase, wc.BackoffMax = cfg.BackoffBase, cfg.BackoffMax
	return wagering.NewService(store, wc, wagering.WithInstanceID(cfg.InstanceID))
}

func newVerifier(cfg config.Config) *auth.Verifier {
	return auth.NewVerifier(auth.Config{
		Issuer: cfg.KeycloakIssuer, JWKSURL: cfg.KeycloakJWKSURL, Audience: cfg.KeycloakAudience,
	})
}

func newHealth(pool *postgres.Pool, q *sqs.Client) *health.Handler {
	return health.NewHandler(
		health.Check{Name: "postgres", Fn: pool.Ready},
		health.Check{Name: "sqs", Fn: q.Ready},
	)
}

func newAPI(svc *wagering.Service, v *auth.Verifier, log *slog.Logger, reg *metrics.Registry, h *health.Handler) *httpapi.API {
	return httpapi.NewAPI(svc, v, log, reg, h)
}

func registerHTTP(lc fx.Lifecycle, cfg config.Config, api *httpapi.API, log *slog.Logger) {
	srv := httpapi.NewServer(cfg.HTTPAddr, api.Routes())
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			l, err := srv.Listen() // falha cedo se a porta estiver ocupada
			if err != nil {
				return err
			}
			log.Info("http server listening", slog.String("addr", cfg.HTTPAddr))
			go func() {
				if err := srv.Serve(l); err != nil {
					log.Error("http server stopped", slog.String("error", err.Error()))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			return srv.Shutdown(sctx)
		},
	})
}

// registerPendingWorker roda o ciclo que conclui (ou expira) as operações
// que aguardam a transação referenciada.
func registerPendingWorker(lc fx.Lifecycle, cfg config.Config, svc *wagering.Service, log *slog.Logger) {
	var cancel context.CancelFunc
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())
			go func() {
				defer close(done)
				t := time.NewTicker(cfg.PendingPollInterval)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						for {
							n, err := svc.ResolvePendingReferences(ctx)
							if err != nil && ctx.Err() == nil {
								log.Error("pending references cycle failed", slog.String("error", err.Error()))
							}
							if err != nil || n == 0 || ctx.Err() != nil {
								break
							}
						}
					}
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			select {
			case <-done:
			case <-ctx.Done():
			}
			return nil
		},
	})
}

// background liga uma tarefa de segundo plano ao ciclo de vida: run recebe um
// contexto cancelado no desligamento, e o desligamento espera a tarefa
// terminar (até o prazo do Fx).
func background(lc fx.Lifecycle, run func(ctx context.Context)) {
	var cancel context.CancelFunc
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())
			go func() { defer close(done); run(ctx) }()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			select {
			case <-done:
			case <-ctx.Done():
			}
			return nil
		},
	})
}

// registerConsumer consome a fila de transações (inbox + processamento).
func registerConsumer(lc fx.Lifecycle, cfg config.Config, q *sqs.Client, svc *wagering.Service, log *slog.Logger, reg *metrics.Registry) {
	c := messaging.New(q.WagerQueue(), svc,
		messaging.Config{Workers: cfg.ConsumerWorkers, Batch: 10, Wait: 5 * time.Second},
		messaging.WithLogger(log), messaging.WithMetrics(reg))
	background(lc, c.Run)
}

// registerOutbox publica os eventos gravados na outbox.
func registerOutbox(lc fx.Lifecycle, cfg config.Config, store *postgres.Store, q *sqs.Client, log *slog.Logger, reg *metrics.Registry) {
	oc := outbox.DefaultConfig()
	oc.Poll, oc.Batch = cfg.OutboxPollInterval, cfg.OutboxBatch
	d := outbox.NewDispatcher(store, q.EventPublisher(), oc,
		outbox.WithOwner(cfg.InstanceID), outbox.WithLogger(log), outbox.WithMetrics(reg))
	background(lc, d.Run)
}
