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

// SubmitCommand é uma operação externa (HTTP ou SQS). ProviderID vem da
// identidade autenticada, nunca do corpo da requisição.
type SubmitCommand struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	WalletID                       id.ID
	PlayerID                       id.ID
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

// Outcome é o resultado de Submit.
type Outcome struct {
	Transaction      *wager.WagerTransaction
	Balance          money.Money // saldo observado no processamento (ou atual, se ainda pendente)
	IdempotentReplay bool
}

// Submit processa uma operação de forma idempotente e atômica.
//
// Dentro de uma única transação do banco: trava a carteira (SELECT ... FOR
// UPDATE), procura a operação por chave de idempotência e por
// (provedor, id externo), decide o resultado, grava transação + ledger +
// saldo + eventos na outbox. Nada é publicado antes do commit.
func (s *Service) Submit(ctx context.Context, cmd SubmitCommand) (*Outcome, error) {
	kind, err := wager.ParseExternalKind(cmd.Kind)
	if err != nil {
		return nil, err
	}
	amount, err := money.Parse(cmd.Amount, cmd.Currency)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	var out *Outcome
	err = s.retry(ctx, func(ctx context.Context, r Repos) error {
		now := s.now()
		t, err := wager.NewExternal(wager.NewExternalParams{
			ID: id.New(), ProviderID: cmd.ProviderID, ExternalTransactionID: cmd.ExternalTransactionID,
			IdempotencyKey: cmd.IdempotencyKey, WalletID: cmd.WalletID, PlayerID: cmd.PlayerID,
			RoundID: cmd.RoundID, GameID: cmd.GameID, Kind: kind, Money: amount,
			ReferenceExternalTransactionID: cmd.ReferenceExternalTransactionID, Now: now,
		})
		if err != nil {
			return err
		}
		w, err := r.Wallets.GetForUpdate(ctx, t.WalletID())
		if err != nil {
			return err
		}
		if ex, err := s.findExisting(ctx, r, t); err != nil {
			return err
		} else if ex != nil {
			if ex.PayloadHash() != t.PayloadHash() {
				return ErrIdempotencyConflict
			}
			out = &Outcome{Transaction: ex, Balance: balanceOf(ex, w), IdempotentReplay: true}
			return nil
		}
		x := &run{s: s, r: r, w: w, now: now}
		if err := x.settle(ctx, t, true); err != nil {
			return err
		}
		out = &Outcome{Transaction: t, Balance: balanceOf(t, w)}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Service) findExisting(ctx context.Context, r Repos, t *wager.WagerTransaction) (*wager.WagerTransaction, error) {
	ex, err := r.Transactions.FindByIdempotencyKey(ctx, t.ProviderID(), t.IdempotencyKey())
	if err == nil {
		return ex, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	ex, err = r.Transactions.FindByExternal(ctx, t.ProviderID(), t.ExternalTransactionID())
	if err == nil {
		return ex, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	return nil, nil
}

func balanceOf(t *wager.WagerTransaction, w *wager.Wallet) money.Money {
	if b, ok := t.ResultBalance(); ok {
		return b
	}
	return w.Balance()
}

// run reúne o contexto de uma unidade de trabalho com a carteira travada.
type run struct {
	s   *Service
	r   Repos
	w   *wager.Wallet
	now time.Time
}

type verdict int

const (
	vOK verdict = iota
	vMissing
	vPending
	vFail
)

type resolution struct {
	v    verdict
	ref  *wager.WagerTransaction
	code wager.FailureCode
}

// allowedReference diz se a operação pode referenciar uma transação do tipo ref.
func allowedReference(op, ref wager.Kind) bool {
	switch op {
	case wager.KindWin, wager.KindRefund:
		return ref == wager.KindBet
	case wager.KindRollback:
		return ref == wager.KindBet || ref == wager.KindWin || ref == wager.KindRefund
	}
	return false
}

// movement é o sentido do saldo causado por uma transação de tipo k.
func movement(k wager.Kind) wager.Direction {
	if k == wager.KindBet {
		return wager.DirectionDebit
	}
	return wager.DirectionCredit // WIN, REFUND, OPENING
}

func (x *run) resolve(ctx context.Context, t *wager.WagerTransaction) (resolution, error) {
	ref, err := x.r.Transactions.FindByExternal(ctx, t.ProviderID(), t.ReferenceExternalTransactionID())
	if errors.Is(err, ErrNotFound) {
		return resolution{v: vMissing}, nil
	}
	if err != nil {
		return resolution{}, err
	}
	fail := func(c wager.FailureCode) (resolution, error) { return resolution{v: vFail, code: c}, nil }
	if ref.WalletID() != t.WalletID() || ref.PlayerID() != t.PlayerID() ||
		ref.Money().Currency() != t.Money().Currency() || ref.RoundID() != t.RoundID() {
		return fail(wager.FailReferenceMismatch)
	}
	switch ref.Status() {
	case wager.StatusPending, wager.StatusPendingReference:
		return resolution{v: vPending}, nil
	case wager.StatusRejected, wager.StatusFailed:
		return fail(wager.FailReferenceNotProcessed)
	}
	if !allowedReference(t.Kind(), ref.Kind()) {
		return fail(wager.FailReferenceInvalidKind)
	}
	if t.Kind().IsReversal() {
		if c, err := ref.Money().Cmp(t.Money()); err != nil || c != 0 {
			return fail(wager.FailReferenceMismatch)
		}
		_, err := x.r.Transactions.FindProcessedReversal(ctx, ref.ID())
		if err == nil {
			return fail(wager.FailAlreadyReversed)
		}
		if !errors.Is(err, ErrNotFound) {
			return resolution{}, err
		}
	}
	return resolution{v: vOK, ref: ref}, nil
}

// settle decide o destino de t e persiste tudo (transação, ledger, saldo,
// eventos). isNew indica que t ainda não existe no banco.
func (x *run) settle(ctx context.Context, t *wager.WagerTransaction, isNew bool) error {
	var entry *wager.LedgerEntry
	var err error
	switch {
	case x.w.PlayerID() != t.PlayerID() || x.w.Currency() != t.Money().Currency():
		err = t.Reject(wager.FailWalletMismatch, x.w.Balance(), x.now)
	case t.ReferenceExternalTransactionID() == "":
		entry, err = x.execute(t, nil)
	default:
		var res resolution
		if res, err = x.resolve(ctx, t); err != nil {
			return err
		}
		switch res.v {
		case vFail:
			err = t.Reject(res.code, x.w.Balance(), x.now)
		case vMissing, vPending:
			err = x.waitOrExpire(t, res, isNew)
		default:
			entry, err = x.execute(t, res.ref)
		}
	}
	if err != nil {
		return err
	}
	return x.save(ctx, t, entry, isNew)
}

func (x *run) waitOrExpire(t *wager.WagerTransaction, res resolution, isNew bool) error {
	cfg := x.s.cfg
	if isNew {
		return t.WaitForReference(x.now.Add(cfg.ReferenceTTL), x.now.Add(cfg.backoff(0)), x.now)
	}
	exp, _ := t.ExpiresAt()
	if !x.now.Before(exp) || t.ReferenceAttempts()+1 >= cfg.MaxReferenceAttempts {
		code := wager.FailReferenceNotFound
		if res.v == vPending {
			code = wager.FailReferenceNotProcessed
		}
		return t.Reject(code, x.w.Balance(), x.now)
	}
	next := x.now.Add(cfg.backoff(t.ReferenceAttempts() + 1))
	if next.After(exp) {
		next = exp
	}
	return t.RetryReference(next, x.now)
}

// execute aplica a movimentação (se houver) e conclui t. Em saldo
// insuficiente, rejeita sem tocar na carteira.
func (x *run) execute(t *wager.WagerTransaction, ref *wager.WagerTransaction) (*wager.LedgerEntry, error) {
	if t.Kind() == wager.KindLoss {
		return nil, t.MarkProcessed(x.w.Balance(), id.Nil, x.now)
	}
	var dir wager.Direction
	switch t.Kind() {
	case wager.KindBet:
		dir = wager.DirectionDebit
	case wager.KindWin, wager.KindRefund:
		dir = wager.DirectionCredit
	case wager.KindRollback:
		dir = movement(ref.Kind()).Opposite()
	default:
		return nil, fmt.Errorf("%w: unexpected kind %s", ErrInvalidInput, t.Kind())
	}
	entry, err := x.w.Apply(id.New(), t.ID(), dir, t.Money(), x.now)
	if errors.Is(err, wager.ErrInsufficientFunds) {
		code := wager.FailInsufficientFunds
		if t.Kind().IsReversal() {
			code = wager.FailReversalInsufficientFunds
		}
		return nil, t.Reject(code, x.w.Balance(), x.now)
	}
	if err != nil {
		return nil, err
	}
	refID := id.Nil
	if ref != nil {
		refID = ref.ID()
	}
	return entry, t.MarkProcessed(x.w.Balance(), refID, x.now)
}

// save grava transação, ledger, saldo e eventos, nesta ordem.
func (x *run) save(ctx context.Context, t *wager.WagerTransaction, entry *wager.LedgerEntry, isNew bool) error {
	var err error
	if isNew {
		err = x.r.Transactions.Insert(ctx, t)
	} else {
		err = x.r.Transactions.Update(ctx, t)
	}
	if err != nil {
		return err
	}
	if entry != nil {
		if err := x.r.Ledger.Insert(ctx, entry); err != nil {
			return err
		}
		if err := x.r.Wallets.UpdateBalance(ctx, x.w, x.w.Version()-1); err != nil {
			return err
		}
	}
	if err := x.emit(ctx, t, entry, isNew, t.IdempotencyKey()); err != nil {
		return err
	}
	if t.Status() == wager.StatusProcessed && t.Origin() == wager.OriginExternal {
		return x.r.Transactions.WakeDependents(ctx, t.WalletID(), t.ProviderID(), t.ExternalTransactionID(), x.now)
	}
	return nil
}

// emit grava na outbox os eventos do estado atual de t.
func (x *run) emit(ctx context.Context, t *wager.WagerTransaction, entry *wager.LedgerEntry, isNew bool, corr string) error {
	add := func(ev wager.Event, err error) error {
		if err != nil {
			return err
		}
		return x.r.Outbox.Insert(ctx, ev)
	}
	switch t.Status() {
	case wager.StatusProcessed:
		if err := add(wager.NewTransactionProcessed(id.New(), t, corr, x.now)); err != nil {
			return err
		}
		if entry != nil {
			return add(wager.NewWalletBalanceChanged(id.New(), entry, x.w.Version(), corr, x.now))
		}
	case wager.StatusRejected:
		return add(wager.NewTransactionRejected(id.New(), t, corr, x.now))
	case wager.StatusPendingReference:
		if isNew {
			return add(wager.NewPendingReference(id.New(), t, corr, x.now))
		}
	}
	return nil
}

// ResolvePendingReferences é um ciclo do worker: reserva transações
// PENDING_REFERENCE vencidas e tenta concluí-las. Devolve quantas foram
// examinadas. Pode rodar em várias instâncias ao mesmo tempo.
func (s *Service) ResolvePendingReferences(ctx context.Context) (int, error) {
	var ids []id.ID
	err := s.retry(ctx, func(ctx context.Context, r Repos) error {
		var err error
		ids, err = r.Transactions.ClaimPendingReferences(ctx, s.now(), s.cfg.ClaimBatch, s.cfg.ClaimLease, s.instanceID)
		return err
	})
	if err != nil {
		return 0, err
	}
	var first error
	for _, txID := range ids {
		if err := s.resolveOne(ctx, txID); err != nil && first == nil {
			first = fmt.Errorf("resolve %s: %w", txID, err)
		}
	}
	return len(ids), first
}

func (s *Service) resolveOne(ctx context.Context, txID id.ID) error {
	return s.retry(ctx, func(ctx context.Context, r Repos) error {
		probe, err := r.Transactions.Get(ctx, txID)
		if err != nil {
			return err
		}
		w, err := r.Wallets.GetForUpdate(ctx, probe.WalletID())
		if err != nil {
			return err
		}
		t, err := r.Transactions.Get(ctx, txID) // releitura já com a carteira travada
		if err != nil {
			return err
		}
		if t.Status() != wager.StatusPendingReference {
			return nil
		}
		x := &run{s: s, r: r, w: w, now: s.now()}
		return x.settle(ctx, t, false)
	})
}
