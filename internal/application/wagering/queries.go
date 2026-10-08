package wagering

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

// GetWallet lê uma carteira.
func (s *Service) GetWallet(ctx context.Context, walletID id.ID) (*wager.Wallet, error) {
	var w *wager.Wallet
	err := s.uow.Do(ctx, func(ctx context.Context, r Repos) error {
		var err error
		w, err = r.Wallets.Get(ctx, walletID)
		return err
	})
	return w, err
}

// GetTransaction lê uma transação pelo id interno.
func (s *Service) GetTransaction(ctx context.Context, txID id.ID) (*wager.WagerTransaction, error) {
	var t *wager.WagerTransaction
	err := s.uow.Do(ctx, func(ctx context.Context, r Repos) error {
		var err error
		t, err = r.Transactions.Get(ctx, txID)
		return err
	})
	return t, err
}

// FindTransaction lê uma transação por (provedor, id externo).
func (s *Service) FindTransaction(ctx context.Context, providerID, externalID string) (*wager.WagerTransaction, error) {
	var t *wager.WagerTransaction
	err := s.uow.Do(ctx, func(ctx context.Context, r Repos) error {
		var err error
		t, err = r.Transactions.FindByExternal(ctx, providerID, externalID)
		return err
	})
	return t, err
}

// LedgerPage é uma página do ledger.
type LedgerPage struct {
	Entries    []*wager.LedgerEntry
	NextCursor string // vazio = fim
}

// ListLedger lista o ledger em ordem de inserção, com cursor opaco.
func (s *Service) ListLedger(ctx context.Context, walletID id.ID, cursor string, limit int) (*LedgerPage, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	var after int64
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err == nil {
			after, err = strconv.ParseInt(string(raw), 10, 64)
		}
		if err != nil || after < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", ErrInvalidInput)
		}
	}
	page := &LedgerPage{}
	err := s.uow.Do(ctx, func(ctx context.Context, r Repos) error {
		if _, err := r.Wallets.Get(ctx, walletID); err != nil {
			return err
		}
		rows, err := r.Ledger.List(ctx, walletID, after, limit+1)
		if err != nil {
			return err
		}
		if len(rows) > limit {
			rows = rows[:limit]
			last := rows[len(rows)-1].Seq
			page.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(last, 10)))
		}
		for _, row := range rows {
			page.Entries = append(page.Entries, row.Entry)
		}
		return nil
	})
	return page, err
}

// Reconciliation compara o saldo da carteira com a soma do ledger.
type Reconciliation struct {
	WalletID      id.ID
	WalletBalance money.Money
	LedgerBalance money.Money
	Difference    money.Money // saldo armazenado menos saldo reconstruído
	WalletVersion int64
	Entries       int64
	Consistent    bool
}

// Reconcile verifica se o saldo da carteira é explicado pelo ledger. Apenas
// relata: não corrige nada.
func (s *Service) Reconcile(ctx context.Context, walletID id.ID) (*Reconciliation, error) {
	var out *Reconciliation
	err := s.uow.Do(ctx, func(ctx context.Context, r Repos) error {
		sum, err := r.Ledger.Summarize(ctx, walletID)
		if err != nil {
			return err
		}
		c, err := sum.WalletBalance.Cmp(sum.LedgerBalance)
		diff, _ := sum.WalletBalance.Sub(sum.LedgerBalance)
		out = &Reconciliation{
			WalletID: walletID, WalletBalance: sum.WalletBalance, LedgerBalance: sum.LedgerBalance, Difference: diff,
			WalletVersion: sum.WalletVersion, Entries: sum.Entries, Consistent: err == nil && c == 0,
		}
		return nil
	})
	return out, err
}
