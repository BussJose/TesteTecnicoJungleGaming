package wager

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
)

var t0 = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func openWallet(t *testing.T, initial string) *OpenedWallet {
	t.Helper()
	o, err := OpenWallet(OpenWalletParams{
		WalletID: id.New(), PlayerID: id.New(), OpeningTransactionID: id.New(),
		LedgerEntryID: id.New(), Initial: brl(t, initial), Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestOpenWalletWithBalance(t *testing.T) {
	o := openWallet(t, "100.00")
	if o.Wallet.Version() != 1 || o.Wallet.Balance().Amount() != "100.00" {
		t.Fatalf("wallet: v=%d b=%s", o.Wallet.Version(), o.Wallet.Balance())
	}
	if o.Opening == nil || o.Entry == nil {
		t.Fatal("opening and ledger entry expected")
	}
	if o.Opening.Kind() != KindOpening || o.Opening.Status() != StatusProcessed || o.Opening.Origin() != OriginInternal {
		t.Fatalf("bad opening: %v %v", o.Opening.Kind(), o.Opening.Status())
	}
}

func TestOpenWalletZero(t *testing.T) {
	o := openWallet(t, "0.00")
	if o.Opening != nil || o.Entry != nil || o.Wallet.Version() != 1 {
		t.Fatal("zero wallet must have no opening nor ledger")
	}
}

func TestApplyDebitCredit(t *testing.T) {
	w := openWallet(t, "100.00").Wallet
	e, err := w.Apply(id.New(), id.New(), DirectionDebit, brl(t, "30.00"), t0)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "70.00" || w.Version() != 2 || e.BalanceBefore().Amount() != "100.00" {
		t.Fatalf("state %s v%d", w.Balance(), w.Version())
	}
	if _, err := w.Apply(id.New(), id.New(), DirectionCredit, brl(t, "5.50"), t0); err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "75.50" || w.Version() != 3 {
		t.Fatalf("state %s v%d", w.Balance(), w.Version())
	}
}

func TestApplyInsufficientFundsKeepsWallet(t *testing.T) {
	w := openWallet(t, "100.00").Wallet
	_, err := w.Apply(id.New(), id.New(), DirectionDebit, brl(t, "100.01"), t0)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("got %v", err)
	}
	if w.Balance().Amount() != "100.00" || w.Version() != 1 {
		t.Fatal("wallet must be unchanged")
	}
	// exatamente o saldo é permitido
	if _, err := w.Apply(id.New(), id.New(), DirectionDebit, brl(t, "100.00"), t0); err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "0.00" {
		t.Fatal("balance must be zero")
	}
}

func TestApplyRejectsZeroAndOtherCurrency(t *testing.T) {
	w := openWallet(t, "10.00").Wallet
	if _, err := w.Apply(id.New(), id.New(), DirectionCredit, brl(t, "0.00"), t0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero: %v", err)
	}
	usd, _ := money.Parse("1.00", "USD")
	if _, err := w.Apply(id.New(), id.New(), DirectionCredit, usd, t0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("currency: %v", err)
	}
	if w.Version() != 1 {
		t.Fatal("version must not change")
	}
}

func newTx(t *testing.T, kind Kind, amount, ref string) (*WagerTransaction, error) {
	t.Helper()
	return NewExternal(NewExternalParams{
		ID: id.New(), ProviderID: "prov", ExternalTransactionID: "ext-1", IdempotencyKey: "key-1",
		WalletID: id.New(), PlayerID: id.New(), RoundID: "r1", GameID: "g1", Kind: kind,
		Money: brl(t, amount), ReferenceExternalTransactionID: ref, Now: t0,
	})
}

func TestNewExternalRules(t *testing.T) {
	cases := []struct {
		name        string
		kind        Kind
		amount, ref string
		wantInvalid bool
	}{
		{"bet ok", KindBet, "10.00", "", false},
		{"bet zero", KindBet, "0.00", "", true},
		{"loss zero ok", KindLoss, "0.00", "", false},
		{"loss positive", KindLoss, "1.00", "", true},
		{"win without ref ok", KindWin, "5.00", "", false},
		{"win with ref ok", KindWin, "5.00", "bet-1", false},
		{"bet with ref", KindBet, "5.00", "x", true},
		{"loss with ref", KindLoss, "0.00", "x", true},
		{"refund no ref", KindRefund, "5.00", "", true},
		{"refund ref ok", KindRefund, "5.00", "bet-1", false},
		{"rollback no ref", KindRollback, "5.00", "", true},
		{"rollback ref ok", KindRollback, "5.00", "bet-1", false},
		{"self reference", KindRefund, "5.00", "ext-1", true},
		{"opening external", KindOpening, "5.00", "", true},
		{"unknown kind", Kind("FOO"), "5.00", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tx, err := newTx(t, c.kind, c.amount, c.ref)
			if c.wantInvalid {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("want ErrInvalid, got %v", err)
				}
				return
			}
			if err != nil || tx.Status() != StatusPending {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestNewExternalTextValidation(t *testing.T) {
	base := NewExternalParams{
		ID: id.New(), ProviderID: "p", ExternalTransactionID: "e", IdempotencyKey: "k",
		WalletID: id.New(), PlayerID: id.New(), RoundID: "r", GameID: "g", Kind: KindBet,
		Money: brl(t, "1.00"), Now: t0,
	}
	for name, mut := range map[string]func(*NewExternalParams){
		"empty provider": func(p *NewExternalParams) { p.ProviderID = "" },
		"spaces":         func(p *NewExternalParams) { p.GameID = " g" },
		"control":        func(p *NewExternalParams) { p.RoundID = "a\nb" },
		"too long":       func(p *NewExternalParams) { p.IdempotencyKey = string(make([]byte, 201)) },
	} {
		p := base
		mut(&p)
		if _, err := NewExternal(p); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestHashEquivalentAmounts(t *testing.T) {
	w, p := id.New(), id.New()
	mk := func(a string) string {
		tx, err := NewExternal(NewExternalParams{
			ID: id.New(), ProviderID: "p", ExternalTransactionID: "e", IdempotencyKey: "k",
			WalletID: w, PlayerID: p, RoundID: "r", GameID: "g", Kind: KindBet, Money: brl(t, a), Now: t0,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tx.PayloadHash()
	}
	if mk("25") != mk("25.00") || mk("25.5") != mk("25.50") {
		t.Fatal("equivalent amounts must hash the same")
	}
	if mk("25.00") == mk("25.01") {
		t.Fatal("different amounts must hash differently")
	}
}

func TestTransitions(t *testing.T) {
	tx, _ := newTx(t, KindRefund, "5.00", "bet-1")
	if err := tx.WaitForReference(t0.Add(time.Hour), t0.Add(time.Second), t0); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusPendingReference {
		t.Fatal(tx.Status())
	}
	if err := tx.RetryReference(t0.Add(2*time.Second), t0); err != nil || tx.ReferenceAttempts() != 1 {
		t.Fatalf("retry: %v %d", err, tx.ReferenceAttempts())
	}
	if err := tx.MarkProcessed(brl(t, "10.00"), id.New(), t0); err != nil {
		t.Fatal(err)
	}
	if _, ok := tx.ExpiresAt(); ok {
		t.Fatal("expiry must be cleared")
	}
	if err := tx.Reject(FailureCode("X"), brl(t, "1.00"), t0); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("terminal: %v", err)
	}
	if err := tx.RetryReference(t0, t0); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("terminal retry: %v", err)
	}
}

func TestWaitForReferenceRequiresReference(t *testing.T) {
	tx, _ := newTx(t, KindBet, "5.00", "")
	if err := tx.WaitForReference(t0, t0, t0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

func TestRehydrateRoundTrip(t *testing.T) {
	tx, _ := newTx(t, KindBet, "5.00", "")
	_ = tx.MarkProcessed(brl(t, "95.00"), id.Nil, t0)
	back, err := RehydrateTransaction(tx.Record())
	if err != nil {
		t.Fatal(err)
	}
	if back.PayloadHash() != tx.PayloadHash() || back.Status() != StatusProcessed {
		t.Fatal("round trip mismatch")
	}
	r := tx.Record()
	r.Status = StatusRejected
	r.FailureCode = ""
	if _, err := RehydrateTransaction(r); !errors.Is(err, ErrInvalid) {
		t.Fatalf("rejected without code: %v", err)
	}
}

func TestLedgerEntryInvariant(t *testing.T) {
	_, err := NewLedgerEntry(id.New(), id.New(), id.New(), DirectionDebit,
		brl(t, "10.00"), brl(t, "100.00"), brl(t, "80.00"), t0)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

func TestEvents(t *testing.T) {
	o := openWallet(t, "100.00")
	tx, _ := newTx(t, KindBet, "30.00", "")
	e, err := o.Wallet.Apply(id.New(), tx.ID(), DirectionDebit, tx.Money(), t0)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.MarkProcessed(o.Wallet.Balance(), id.Nil, t0)
	ev, err := NewWalletBalanceChanged(id.New(), e, o.Wallet.Version(), "key-1", t0)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Type() != EventWalletBalanceChanged || ev.Version() != 1 || ev.CausationID() != tx.ID().String() {
		t.Fatalf("bad event %v", ev.Type())
	}
	env := RehydrateEnvelope(ev.ID(), ev.Type(), ev.Version(), ev.AggregateID(), ev.CorrelationID(),
		ev.CausationID(), ev.OccurredAt(), ev.Payload())
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := m[k]; !ok {
			t.Errorf("envelope missing %s", k)
		}
	}
	data := m["data"].(map[string]any)
	if data["balanceAfter"].(map[string]any)["amount"] != "70.00" || data["walletVersion"].(float64) != 2 {
		t.Fatalf("data: %v", data)
	}
	if _, err := NewTransactionRejected(id.New(), tx, "k", t0); err == nil {
		t.Fatal("rejected event for processed tx must fail")
	}
	if _, err := NewTransactionProcessed(id.New(), tx, "", t0); err == nil {
		t.Fatal("empty correlation must fail")
	}
}
