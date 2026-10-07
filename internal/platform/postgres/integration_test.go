//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/application/wagering/wageringtest"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/platform/postgres"
)

// Estes testes usam um PostgreSQL REAL (sem mocks). Cada cenário roda num
// banco novo, criado e destruído pelo próprio teste.
//
//	DATABASE_URL=postgres://wager:wager@localhost:5432/wager?sslmode=disable \
//	    go test -race -tags integration ./internal/platform/postgres/...

func adminURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("DATABASE_URL")
	if u == "" {
		t.Skip("DATABASE_URL não definida: teste de integração ignorado")
	}
	return u
}

func newEnv(t *testing.T) (*wageringtest.Env, *testPool) {
	t.Helper()
	pool := newIsolatedPool(t, adminURL(t))
	store := postgres.NewStore(pool.Pool)
	clock := wageringtest.NewFakeClock(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	n := 0
	return &wageringtest.Env{
		Clock:  clock,
		Events: store.Events,
		NewService: func() *wagering.Service {
			n++
			return wagering.NewService(store, wagering.DefaultConfig(),
				wagering.WithClock(clock.Now), wagering.WithInstanceID(fmt.Sprintf("instance-%d", n)))
		},
	}, pool
}

func TestSuiteOnPostgres(t *testing.T) {
	adminURL(t)
	wageringtest.RunSuite(t, func(t *testing.T) *wageringtest.Env {
		env, _ := newEnv(t)
		return env
	})
}

func pgCode(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}

// O banco, e não só o código, protege as invariantes financeiras.
func TestDatabaseGuards(t *testing.T) {
	env, pool := newEnv(t)
	ctx := context.Background()
	svc := env.NewService()
	w, err := svc.OpenWallet(ctx, wagering.OpenWalletCommand{PlayerID: id.New(), Currency: "BRL", InitialBalance: "10.00"})
	if err != nil {
		t.Fatal(err)
	}
	wid := w.ID().String()

	t.Run("negative balance is rejected by CHECK", func(t *testing.T) {
		_, err := pool.Exec(ctx, `UPDATE wallets SET balance_minor = -1 WHERE id = $1::text::uuid`, wid)
		if pgCode(err) != "23514" {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("ledger is append-only", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = $1::text::uuid`, wid); err == nil {
			t.Fatal("UPDATE on ledger must fail")
		}
		if _, err := pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1::text::uuid`, wid); err == nil {
			t.Fatal("DELETE on ledger must fail")
		}
		if _, err := pool.Exec(ctx, `TRUNCATE wallet_ledger_entries`); err == nil {
			t.Fatal("TRUNCATE on ledger must fail")
		}
	})
	t.Run("terminal transactions are immutable", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET status = 'FAILED', failure_code = 'X' WHERE wallet_id = $1::text::uuid`, wid); err == nil {
			t.Fatal("UPDATE of a PROCESSED transaction must fail")
		}
		if _, err := pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1::text::uuid`, wid); err == nil {
			t.Fatal("DELETE of a transaction must fail")
		}
	})
	t.Run("outbox snapshot is immutable", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE outbox_events SET payload = '{}'::jsonb WHERE aggregate_id = $1::text::uuid`, wid); err == nil {
			t.Fatal("changing a stored event must fail")
		}
	})
	t.Run("wallet version check blocks stale writers", func(t *testing.T) {
		tag, err := pool.Exec(ctx, `UPDATE wallets SET balance_minor = 0, version = version + 1 WHERE id = $1::text::uuid AND version = 99`, wid)
		if err != nil || tag.RowsAffected() != 0 {
			t.Fatalf("stale update affected rows: %v %v", tag, err)
		}
	})
}
