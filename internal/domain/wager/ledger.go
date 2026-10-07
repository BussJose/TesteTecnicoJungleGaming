package wager

import (
	"time"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
)

// LedgerEntry é um lançamento imutável do ledger de uma carteira.
type LedgerEntry struct {
	id            id.ID
	walletID      id.ID
	transactionID id.ID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// NewLedgerEntry cria um lançamento validando balanceAfter = balanceBefore ± amount.
func NewLedgerEntry(entryID, walletID, transactionID id.ID, dir Direction,
	amount, before, after money.Money, now time.Time) (*LedgerEntry, error) {
	e := &LedgerEntry{
		id: entryID, walletID: walletID, transactionID: transactionID,
		direction: dir, amount: amount, balanceBefore: before, balanceAfter: after,
		createdAt: now.UTC(),
	}
	if err := e.validate(); err != nil {
		return nil, err
	}
	return e, nil
}

// RehydrateLedgerEntry reconstrói um lançamento persistido (sem criar eventos
// nem reaplicar movimentações). Revalida as invariantes.
func RehydrateLedgerEntry(entryID, walletID, transactionID id.ID, dir Direction,
	amount, before, after money.Money, createdAt time.Time) (*LedgerEntry, error) {
	return NewLedgerEntry(entryID, walletID, transactionID, dir, amount, before, after, createdAt)
}

func (e *LedgerEntry) validate() error {
	if e.id.IsZero() || e.walletID.IsZero() || e.transactionID.IsZero() {
		return invalid("ledger entry requires id, walletId and transactionId")
	}
	if !e.direction.valid() {
		return invalid("ledger direction %q", e.direction)
	}
	if !e.amount.IsValid() || !e.balanceBefore.IsValid() || !e.balanceAfter.IsValid() {
		return invalid("ledger entry requires valid money values")
	}
	if !e.amount.IsPositive() {
		return invalid("ledger amount must be positive")
	}
	if e.balanceBefore.IsNegative() || e.balanceAfter.IsNegative() {
		return invalid("ledger balances must not be negative")
	}
	var expected money.Money
	var err error
	if e.direction == DirectionCredit {
		expected, err = e.balanceBefore.Add(e.amount)
	} else {
		expected, err = e.balanceBefore.Sub(e.amount)
	}
	if err != nil {
		return invalid("ledger arithmetic: %v", err)
	}
	cmp, err := expected.Cmp(e.balanceAfter)
	if err != nil {
		return invalid("ledger arithmetic: %v", err)
	}
	if cmp != 0 {
		return invalid("balanceAfter must equal balanceBefore %s amount", e.direction)
	}
	return nil
}

func (e *LedgerEntry) ID() id.ID                  { return e.id }
func (e *LedgerEntry) WalletID() id.ID            { return e.walletID }
func (e *LedgerEntry) TransactionID() id.ID       { return e.transactionID }
func (e *LedgerEntry) Direction() Direction       { return e.direction }
func (e *LedgerEntry) Amount() money.Money        { return e.amount }
func (e *LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e *LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e *LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
