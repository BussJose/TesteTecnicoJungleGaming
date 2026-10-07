package wager

import (
	"time"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
)

// Wallet é a raiz do agregado financeiro. O par (playerId, currency)
// identifica uma única carteira. O saldo só muda por Apply, que gera o
// lançamento correspondente no ledger.
type Wallet struct {
	id        id.ID
	playerID  id.ID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// OpenWalletParams reúne os dados da abertura de uma carteira.
type OpenWalletParams struct {
	WalletID             id.ID
	PlayerID             id.ID
	OpeningTransactionID id.ID // usado só se Initial > 0
	LedgerEntryID        id.ID // usado só se Initial > 0
	Initial              money.Money
	Now                  time.Time
}

// OpenedWallet é o resultado da abertura. Opening e Entry são nulos quando o
// saldo inicial é zero: nesse caso não há OPENING, ledger nem eventos.
type OpenedWallet struct {
	Wallet  *Wallet
	Opening *WagerTransaction
	Entry   *LedgerEntry
}

// OpenWallet cria a carteira com versão 1. Com saldo inicial positivo, cria
// também a transação OPENING (PROCESSED) e o lançamento de crédito.
func OpenWallet(p OpenWalletParams) (*OpenedWallet, error) {
	if p.WalletID.IsZero() || p.PlayerID.IsZero() {
		return nil, invalid("wallet requires walletId and playerId")
	}
	if !p.Initial.IsValid() {
		return nil, invalid("initial balance requires a valid currency")
	}
	if p.Initial.IsNegative() {
		return nil, invalid("initial balance must not be negative")
	}
	now := p.Now.UTC()
	w := &Wallet{
		id: p.WalletID, playerID: p.PlayerID, balance: p.Initial,
		version: 1, createdAt: now, updatedAt: now,
	}
	out := &OpenedWallet{Wallet: w}
	if !p.Initial.IsPositive() {
		return out, nil
	}
	if p.OpeningTransactionID.IsZero() || p.LedgerEntryID.IsZero() {
		return nil, invalid("positive initial balance requires opening transaction and ledger entry ids")
	}
	zero, err := money.Zero(p.Initial.Currency())
	if err != nil {
		return nil, invalid("%v", err)
	}
	opening, err := newOpening(p.OpeningTransactionID, w.id, w.playerID, p.Initial, now)
	if err != nil {
		return nil, err
	}
	entry, err := NewLedgerEntry(p.LedgerEntryID, w.id, opening.id, DirectionCredit, p.Initial, zero, p.Initial, now)
	if err != nil {
		return nil, err
	}
	out.Opening = opening
	out.Entry = entry
	return out, nil
}

// RehydrateWallet reconstrói uma carteira persistida, sem reaplicar
// movimentações nem emitir eventos. Rejeita valores não inicializados.
func RehydrateWallet(walletID, playerID id.ID, balance money.Money, version int64,
	createdAt, updatedAt time.Time) (*Wallet, error) {
	if walletID.IsZero() || playerID.IsZero() {
		return nil, invalid("wallet requires walletId and playerId")
	}
	if !balance.IsValid() {
		return nil, invalid("wallet balance requires a valid currency")
	}
	if balance.IsNegative() {
		return nil, invalid("wallet balance must not be negative")
	}
	if version < 1 {
		return nil, invalid("wallet version must be >= 1")
	}
	return &Wallet{
		id: walletID, playerID: playerID, balance: balance, version: version,
		createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC(),
	}, nil
}

// Apply debita ou credita a carteira e devolve o lançamento correspondente.
// É atômico: em caso de erro (saldo insuficiente, moeda diferente, overflow)
// a carteira permanece inalterada. A versão só avança quando o saldo muda.
func (w *Wallet) Apply(entryID, transactionID id.ID, dir Direction, amount money.Money, now time.Time) (*LedgerEntry, error) {
	if !dir.valid() {
		return nil, invalid("direction %q", dir)
	}
	if !amount.IsValid() {
		return nil, invalid("amount requires a valid currency")
	}
	if !amount.IsPositive() {
		return nil, invalid("amount must be positive")
	}
	before := w.balance
	var after money.Money
	var err error
	if dir == DirectionCredit {
		after, err = before.Add(amount)
	} else {
		after, err = before.Sub(amount)
	}
	if err != nil {
		return nil, invalid("%v", err) // moeda diferente ou overflow
	}
	if after.IsNegative() {
		return nil, ErrInsufficientFunds
	}
	entry, err := NewLedgerEntry(entryID, w.id, transactionID, dir, amount, before, after, now)
	if err != nil {
		return nil, err
	}
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
	return entry, nil
}

func (w *Wallet) ID() id.ID            { return w.id }
func (w *Wallet) PlayerID() id.ID      { return w.playerID }
func (w *Wallet) Balance() money.Money { return w.balance }
func (w *Wallet) Currency() string     { return w.balance.Currency() }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }
