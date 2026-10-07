package wagering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

// Config reúne os parâmetros do processamento de referências pendentes.
type Config struct {
	ReferenceTTL         time.Duration // prazo total de espera pela referência
	MaxReferenceAttempts int           // tentativas máximas do worker
	BackoffBase          time.Duration // espera da 1ª tentativa
	BackoffMax           time.Duration // teto do backoff exponencial
	ClaimLease           time.Duration // tempo de reserva de uma transação pelo worker
	ClaimBatch           int           // transações reservadas por ciclo
}

// DefaultConfig devolve valores razoáveis.
func DefaultConfig() Config {
	return Config{
		ReferenceTTL: 5 * time.Minute, MaxReferenceAttempts: 10,
		BackoffBase: time.Second, BackoffMax: 30 * time.Second,
		ClaimLease: 30 * time.Second, ClaimBatch: 20,
	}
}

func (c Config) backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 20 {
		attempt = 20
	}
	d := c.BackoffBase << uint(attempt)
	if d > c.BackoffMax || d <= 0 {
		return c.BackoffMax
	}
	return d
}

// Service implementa os casos de uso.
type Service struct {
	uow        UnitOfWork
	cfg        Config
	now        func() time.Time
	instanceID string
}

// Option personaliza o Service.
type Option func(*Service)

// WithClock troca o relógio (testes).
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

// WithInstanceID identifica a instância nos leases do worker.
func WithInstanceID(v string) Option { return func(s *Service) { s.instanceID = v } }

// NewService cria o serviço.
func NewService(uow UnitOfWork, cfg Config, opts ...Option) *Service {
	s := &Service{uow: uow, cfg: cfg, now: func() time.Time { return time.Now().UTC() }, instanceID: id.New().String()}
	for _, o := range opts {
		o(s)
	}
	return s
}

const maxAttempts = 4

// retry repete a unidade de trabalho em conflitos temporários e de unicidade.
// A cada repetição a função é reexecutada do zero (estado novo).
func (s *Service) retry(ctx context.Context, fn func(ctx context.Context, r Repos) error) error {
	var err error
	for i := 0; i < maxAttempts; i++ {
		err = s.uow.Do(ctx, fn)
		if err == nil || ctx.Err() != nil {
			return err
		}
		if !errors.Is(err, ErrUniqueViolation) && !errors.Is(err, ErrTransient) && !errors.Is(err, ErrConcurrentUpdate) {
			return err
		}
	}
	return err
}

// ---------------------------------------------------------------- carteira

// OpenWalletCommand pede a abertura de uma carteira.
type OpenWalletCommand struct {
	PlayerID       id.ID
	Currency       string
	InitialBalance string // vazio = 0.00
}

// OpenWallet cria a carteira (versão 1). Com saldo inicial positivo grava
// OPENING, ledger e eventos na mesma transação.
func (s *Service) OpenWallet(ctx context.Context, cmd OpenWalletCommand) (*wager.Wallet, error) {
	amount := cmd.InitialBalance
	if amount == "" {
		amount = "0.00"
	}
	initial, err := money.Parse(amount, cmd.Currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	var out *wager.Wallet
	err = s.retry(ctx, func(ctx context.Context, r Repos) error {
		opened, err := wager.OpenWallet(wager.OpenWalletParams{
			WalletID: id.New(), PlayerID: cmd.PlayerID, OpeningTransactionID: id.New(),
			LedgerEntryID: id.New(), Initial: initial, Now: s.now(),
		})
		if err != nil {
			return err
		}
		if err := r.Wallets.Insert(ctx, opened.Wallet); err != nil {
			return err
		}
		if opened.Opening != nil {
			if err := r.Transactions.Insert(ctx, opened.Opening); err != nil {
				return err
			}
			if err := r.Ledger.Insert(ctx, opened.Entry); err != nil {
				return err
			}
			x := &run{s: s, r: r, w: opened.Wallet, now: s.now()}
			if err := x.emit(ctx, opened.Opening, opened.Entry, false, opened.Opening.ID().String()); err != nil {
				return err
			}
		}
		out = opened.Wallet
		return nil
	})
	return out, err
}
