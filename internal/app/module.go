package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/monii/backend-challenge-go/internal/config"
	httpapi "github.com/monii/backend-challenge-go/internal/http"
	"github.com/monii/backend-challenge-go/internal/http/health"
	"github.com/monii/backend-challenge-go/internal/platform/postgres"
	"github.com/monii/backend-challenge-go/internal/platform/sqs"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			config.Load,
			newPostgresPool,
			newSQSClient,
			health.NewHandler,
			newHTTPServer,
		),
		fx.Invoke(registerLifecycle),
	)
}

func newPostgresPool(lc fx.Lifecycle, cfg config.Config) (*postgres.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})

	return pool, nil
}

func newSQSClient(cfg config.Config) (*sqs.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return sqs.New(ctx, sqs.Options{
		Region:       cfg.AWSRegion,
		EndpointURL:  cfg.AWSEndpointURL,
		QueueURL:     cfg.SQSWagerQueueURL,
		StaticKey:    envOr("AWS_ACCESS_KEY_ID", "test"),
		StaticSecret: envOr("AWS_SECRET_ACCESS_KEY", "test"),
	})
}

func newHTTPServer(cfg config.Config, healthHandler *health.Handler) *httpapi.Server {
	return httpapi.NewServer(cfg.HTTPAddr, healthHandler)
}

func registerLifecycle(lc fx.Lifecycle, srv *httpapi.Server) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			errCh := make(chan error, 1)
			go func() {
				errCh <- srv.Start()
			}()

			select {
			case err := <-errCh:
				return err
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				return fmt.Errorf("http shutdown: %w", err)
			}
			return nil
		},
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
