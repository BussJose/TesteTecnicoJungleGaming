package wageringtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

// FakeClock é um relógio controlável, compartilhado entre instâncias.
type FakeClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewFakeClock cria um relógio parado em t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{t: t.UTC()} }

// Now devolve o instante atual do relógio.
func (c *FakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// Advance avança o relógio.
func (c *FakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Env é o ambiente de um cenário: serviços independentes (instâncias)
// sobre o mesmo armazenamento e o mesmo relógio.
type Env struct {
	// NewService cria uma instância nova e independente da aplicação.
	NewService func() *wagering.Service
	Clock      *FakeClock
	// Events lista os tipos de evento gravados na outbox de uma carteira, em ordem.
	Events func(ctx context.Context, walletID id.ID) ([]wager.EventType, error)
}

const provider = "provider-a"

type h struct {
	t      *testing.T
	ctx    context.Context
	env    *Env
	svc    *wagering.Service
	player id.ID
	wallet id.ID
}

func newH(t *testing.T, newEnv func(t *testing.T) *Env, initial string) *h {
	t.Helper()
	env := newEnv(t)
	x := &h{t: t, ctx: context.Background(), env: env, svc: env.NewService(), player: id.New()}
	w, err := x.svc.OpenWallet(x.ctx, wagering.OpenWalletCommand{PlayerID: x.player, Currency: "BRL", InitialBalance: initial})
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	x.wallet = w.ID()
	return x
}

func (x *h) cmd(kind, amount, ref string) wagering.SubmitCommand {
	return wagering.SubmitCommand{
		ProviderID: provider, ExternalTransactionID: "ext-" + id.New().String(),
		IdempotencyKey: "key-" + id.New().String(), WalletID: x.wallet, PlayerID: x.player,
		RoundID: "round-1", GameID: "game-1", Kind: kind, Amount: amount, Currency: "BRL",
		ReferenceExternalTransactionID: ref,
	}
}

func (x *h) submit(c wagering.SubmitCommand) *wagering.Outcome {
	x.t.Helper()
	o, err := x.svc.Submit(x.ctx, c)
	if err != nil {
		x.t.Fatalf("submit %s: %v", c.Kind, err)
	}
	return o
}

func (x *h) expect(o *wagering.Outcome, st wager.Status, code wager.FailureCode) {
	x.t.Helper()
	if o.Transaction.Status() != st || o.Transaction.FailureCode() != code {
		x.t.Fatalf("got %s/%s, want %s/%s", o.Transaction.Status(), o.Transaction.FailureCode(), st, code)
	}
}

func (x *h) wal() *wager.Wallet {
	x.t.Helper()
	w, err := x.svc.GetWallet(x.ctx, x.wallet)
	if err != nil {
		x.t.Fatal(err)
	}
	return w
}

func (x *h) balance(want string, version int64) {
	x.t.Helper()
	w := x.wal()
	if w.Balance().Amount() != want || (version > 0 && w.Version() != version) {
		x.t.Fatalf("wallet = %s v%d, want %s v%d", w.Balance().Amount(), w.Version(), want, version)
	}
}

func (x *h) ledgerCount() int {
	x.t.Helper()
	n := 0
	cursor := ""
	for {
		p, err := x.svc.ListLedger(x.ctx, x.wallet, cursor, 3)
		if err != nil {
			x.t.Fatal(err)
		}
		n += len(p.Entries)
		if p.NextCursor == "" {
			return n
		}
		cursor = p.NextCursor
	}
}

func (x *h) reconciled() {
	x.t.Helper()
	r, err := x.svc.Reconcile(x.ctx, x.wallet)
	if err != nil {
		x.t.Fatal(err)
	}
	if !r.Consistent {
		x.t.Fatalf("reconciliation inconsistent: wallet %s ledger %s", r.WalletBalance, r.LedgerBalance)
	}
}

func (x *h) eventsTail(n int) []wager.EventType {
	x.t.Helper()
	if x.env.Events == nil {
		return nil
	}
	all, err := x.env.Events(x.ctx, x.wallet)
	if err != nil {
		x.t.Fatal(err)
	}
	if len(all) < n {
		x.t.Fatalf("only %d events", len(all))
	}
	return all[len(all)-n:]
}

func (x *h) eventCount() int {
	x.t.Helper()
	if x.env.Events == nil {
		return 0
	}
	all, _ := x.env.Events(x.ctx, x.wallet)
	return len(all)
}

func sameEvents(t *testing.T, got []wager.EventType, want ...wager.EventType) {
	t.Helper()
	if got == nil {
		return
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

// runParallel executa n funções ao mesmo tempo e espera todas terminarem.
func runParallel(n int, fn func(i int)) {
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			fn(i)
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
}

// RunSuite executa os cenários de negócio. newEnv cria um ambiente limpo.
func RunSuite(t *testing.T, newEnv func(t *testing.T) *Env) {
	scenarios := []struct {
		name string
		fn   func(*testing.T, func(*testing.T) *Env)
	}{
		{"OpenWallet", testOpenWallet},
		{"BetDebitsAndEmitsEvents", testBet},
		{"InvalidInputIsNotPersisted", testInvalidInput},
		{"IdempotentReplayAndConflicts", testIdempotency},
		{"Parallel50IdenticalBetsAcrossInstances", testParallelIdentical},
		{"TwoBets80Over100", testTwoBets},
		{"ManyParallelBetsNeverGoNegative", testManyBets},
		{"LossDoesNotTouchLedgerOrVersion", testLoss},
		{"Win", testWin},
		{"RefundAndRollback", testReversals},
		{"ReversalInsufficientFunds", testReversalInsufficient},
		{"RefundBeforeBetThenResolve", testRefundBeforeBet},
		{"PendingReferenceExpires", testPendingExpires},
		{"ReferenceMismatches", testMismatches},
		{"WalletMismatchAndNotFound", testWalletChecks},
		{"TwoConcurrentPendingResolvers", testConcurrentResolvers},
	}
	for _, s := range scenarios {
		s := s
		t.Run(s.name, func(t *testing.T) { s.fn(t, newEnv) })
	}
}

func testOpenWallet(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	x.balance("100.00", 1)
	if x.ledgerCount() != 1 {
		t.Fatal("opening must create one ledger entry")
	}
	x.reconciled()
	sameEvents(t, x.eventsTail(2), wager.EventTransactionProcessed, wager.EventWalletBalanceChanged)

	_, err := x.svc.OpenWallet(x.ctx, wagering.OpenWalletCommand{PlayerID: x.player, Currency: "BRL", InitialBalance: "5.00"})
	if !errors.Is(err, wagering.ErrWalletExists) {
		t.Fatalf("duplicate wallet: %v", err)
	}
	// moeda diferente para o mesmo jogador é outra carteira
	if _, err := x.svc.OpenWallet(x.ctx, wagering.OpenWalletCommand{PlayerID: x.player, Currency: "USD"}); err != nil {
		t.Fatalf("second currency: %v", err)
	}
	z := newH(t, newEnv, "0.00")
	z.balance("0.00", 1)
	if z.ledgerCount() != 0 || z.eventCount() != 0 {
		t.Fatal("zero wallet must have no ledger nor events")
	}
	for _, bad := range []wagering.OpenWalletCommand{
		{PlayerID: id.New(), Currency: "XXX"},
		{PlayerID: id.New(), Currency: "BRL", InitialBalance: "-1.00"},
		{PlayerID: id.New(), Currency: "BRL", InitialBalance: "1.234"},
		{Currency: "BRL"},
	} {
		if _, err := x.svc.OpenWallet(x.ctx, bad); !errors.Is(err, wagering.ErrInvalidInput) {
			t.Errorf("%+v: got %v", bad, err)
		}
	}
}

func testBet(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	o := x.submit(x.cmd("BET", "30.00", ""))
	x.expect(o, wager.StatusProcessed, "")
	if o.Balance.Amount() != "70.00" || o.IdempotentReplay {
		t.Fatalf("outcome %s replay=%v", o.Balance, o.IdempotentReplay)
	}
	x.balance("70.00", 2)
	if x.ledgerCount() != 2 {
		t.Fatal("ledger entries")
	}
	sameEvents(t, x.eventsTail(2), wager.EventTransactionProcessed, wager.EventWalletBalanceChanged)
	x.reconciled()
}

func testInvalidInput(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	for _, a := range []string{"", "NaN", "Infinity", "1e3", "-1.00", "1.234", "abc", " 1.00", "0x10"} {
		c := x.cmd("BET", a, "")
		if _, err := x.svc.Submit(x.ctx, c); !errors.Is(err, wagering.ErrInvalidInput) {
			t.Errorf("amount %q: got %v", a, err)
		}
	}
	for _, c := range []wagering.SubmitCommand{
		x.cmd("OPENING", "1.00", ""), x.cmd("FOO", "1.00", ""), x.cmd("BET", "0.00", ""),
		x.cmd("LOSS", "1.00", ""), x.cmd("REFUND", "1.00", ""), x.cmd("BET", "1.00", "x"),
	} {
		if _, err := x.svc.Submit(x.ctx, c); !errors.Is(err, wagering.ErrInvalidInput) {
			t.Errorf("%s %s ref=%q: got %v", c.Kind, c.Amount, c.ReferenceExternalTransactionID, err)
		}
	}
	c := x.cmd("BET", "1.00", "")
	c.Currency = "ZZZ"
	if _, err := x.svc.Submit(x.ctx, c); !errors.Is(err, wagering.ErrInvalidInput) {
		t.Errorf("currency: %v", err)
	}
	x.balance("100.00", 1)
	if x.ledgerCount() != 1 || x.eventCount() > 2 {
		t.Fatal("invalid input must not persist anything")
	}
}

func testIdempotency(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	c := x.cmd("BET", "25", "")
	first := x.submit(c)
	events := x.eventCount()

	again := x.submit(c)
	if !again.IdempotentReplay || again.Transaction.ID() != first.Transaction.ID() || again.Balance.Amount() != "75.00" {
		t.Fatalf("replay: %+v", again)
	}
	equiv := c
	equiv.Amount = "25.00" // equivalente a "25"
	if r := x.submit(equiv); !r.IdempotentReplay {
		t.Fatal("25 and 25.00 must be the same payload")
	}
	// o saldo muda; o replay continua devolvendo o saldo original
	x.submit(x.cmd("BET", "5.00", ""))
	if r := x.submit(c); r.Balance.Amount() != "75.00" || !r.IdempotentReplay {
		t.Fatalf("replay after change: %s", r.Balance)
	}
	if x.ledgerCount() != 3 {
		t.Fatalf("ledger = %d", x.ledgerCount())
	}
	_ = events

	diff := c
	diff.Amount = "26.00"
	if _, err := x.svc.Submit(x.ctx, diff); !errors.Is(err, wagering.ErrIdempotencyConflict) {
		t.Fatalf("different amount, same key: %v", err)
	}
	other := c
	other.IdempotencyKey = "key-" + id.New().String()
	other.Amount = "26.00"
	if _, err := x.svc.Submit(x.ctx, other); !errors.Is(err, wagering.ErrIdempotencyConflict) {
		t.Fatalf("different payload, same external id: %v", err)
	}
	x.balance("70.00", 3)
	x.reconciled()
}

func testParallelIdentical(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	svcs := []*wagering.Service{x.svc, x.env.NewService(), x.env.NewService()}
	c := x.cmd("BET", "10.00", "")
	var mu sync.Mutex
	fresh, replays := 0, 0
	runParallel(50, func(i int) {
		o, err := svcs[i%3].Submit(x.ctx, c)
		if err != nil {
			t.Errorf("submit: %v", err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if o.IdempotentReplay {
			replays++
		} else {
			fresh++
		}
		if o.Balance.Amount() != "90.00" {
			t.Errorf("balance %s", o.Balance)
		}
	})
	if fresh != 1 || replays != 49 {
		t.Fatalf("fresh=%d replays=%d", fresh, replays)
	}
	x.balance("90.00", 2)
	if x.ledgerCount() != 2 {
		t.Fatalf("ledger = %d", x.ledgerCount())
	}
	sameEvents(t, x.eventsTail(2), wager.EventTransactionProcessed, wager.EventWalletBalanceChanged)
	x.reconciled()
}

func testTwoBets(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	svcs := []*wagering.Service{x.svc, x.env.NewService()}
	cmds := []wagering.SubmitCommand{x.cmd("BET", "80.00", ""), x.cmd("BET", "80.00", "")}
	res := make([]*wagering.Outcome, 2)
	runParallel(2, func(i int) {
		o, err := svcs[i].Submit(x.ctx, cmds[i])
		if err != nil {
			t.Errorf("submit: %v", err)
			return
		}
		res[i] = o
	})
	if res[0] == nil || res[1] == nil {
		t.FailNow()
	}
	processed, rejected := 0, 0
	for _, o := range res {
		switch o.Transaction.Status() {
		case wager.StatusProcessed:
			processed++
		case wager.StatusRejected:
			rejected++
			if o.Transaction.FailureCode() != wager.FailInsufficientFunds {
				t.Errorf("code %s", o.Transaction.FailureCode())
			}
		}
		if o.Balance.Amount() != "20.00" && o.Balance.Amount() != "100.00" {
			t.Errorf("unexpected observed balance %s", o.Balance)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	x.balance("20.00", 2)
	x.reconciled()
}

func testManyBets(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	svcs := []*wagering.Service{x.svc, x.env.NewService(), x.env.NewService()}
	var mu sync.Mutex
	processed := 0
	runParallel(30, func(i int) {
		o, err := svcs[i%3].Submit(x.ctx, x.cmd("BET", "10.00", ""))
		if err != nil {
			t.Errorf("submit: %v", err)
			return
		}
		if o.Transaction.Status() == wager.StatusProcessed {
			mu.Lock()
			processed++
			mu.Unlock()
		}
	})
	if processed != 10 {
		t.Fatalf("processed = %d, want 10", processed)
	}
	x.balance("0.00", 11)
	if x.ledgerCount() != 11 {
		t.Fatalf("ledger = %d", x.ledgerCount())
	}
	x.reconciled()
}

func testLoss(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	o := x.submit(x.cmd("LOSS", "0.00", ""))
	x.expect(o, wager.StatusProcessed, "")
	x.balance("100.00", 1)
	if x.ledgerCount() != 1 {
		t.Fatal("LOSS must not create ledger entries")
	}
	sameEvents(t, x.eventsTail(1), wager.EventTransactionProcessed)
	x.reconciled()
}

func testWin(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	bet := x.cmd("BET", "20.00", "")
	x.submit(bet)
	x.expect(x.submit(x.cmd("WIN", "50.00", "")), wager.StatusProcessed, "")
	x.balance("130.00", 3)
	x.expect(x.submit(x.cmd("WIN", "10.00", bet.ExternalTransactionID)), wager.StatusProcessed, "")
	x.balance("140.00", 4)
	x.reconciled()
}

func testReversals(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	bet := x.cmd("BET", "30.00", "")
	x.submit(bet)
	refund := x.submit(x.cmd("REFUND", "30.00", bet.ExternalTransactionID))
	x.expect(refund, wager.StatusProcessed, "")
	x.balance("100.00", 3)
	if refund.Transaction.ReferenceTransactionID().IsZero() {
		t.Fatal("refund must link to the referenced transaction")
	}
	// segunda reversão da mesma aposta
	x.expect(x.submit(x.cmd("REFUND", "30.00", bet.ExternalTransactionID)), wager.StatusRejected, wager.FailAlreadyReversed)
	x.expect(x.submit(x.cmd("ROLLBACK", "30.00", bet.ExternalTransactionID)), wager.StatusRejected, wager.FailAlreadyReversed)
	x.balance("100.00", 3)

	// ROLLBACK de WIN debita
	win := x.cmd("WIN", "40.00", "")
	x.submit(win)
	x.balance("140.00", 4)
	x.expect(x.submit(x.cmd("ROLLBACK", "40.00", win.ExternalTransactionID)), wager.StatusProcessed, "")
	x.balance("100.00", 5)

	// ROLLBACK de BET credita
	bet2 := x.cmd("BET", "10.00", "")
	x.submit(bet2)
	x.expect(x.submit(x.cmd("ROLLBACK", "10.00", bet2.ExternalTransactionID)), wager.StatusProcessed, "")
	x.balance("100.00", 7)

	// ROLLBACK de REFUND debita
	bet3 := x.cmd("BET", "10.00", "")
	x.submit(bet3)
	ref3 := x.cmd("REFUND", "10.00", bet3.ExternalTransactionID)
	x.submit(ref3)
	x.expect(x.submit(x.cmd("ROLLBACK", "10.00", ref3.ExternalTransactionID)), wager.StatusProcessed, "")
	x.balance("90.00", 10)
	x.reconciled()
}

func testReversalInsufficient(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	win := x.cmd("WIN", "100.00", "")
	x.submit(win)
	x.submit(x.cmd("BET", "200.00", "")) // saldo 200 -> 0
	x.balance("0.00", 3)
	o := x.submit(x.cmd("ROLLBACK", "100.00", win.ExternalTransactionID))
	x.expect(o, wager.StatusRejected, wager.FailReversalInsufficientFunds)
	if o.Transaction.FailureCode() == wager.FailInsufficientFunds {
		t.Fatal("reversal must use a code different from BET insufficient funds")
	}
	x.balance("0.00", 3)
	// a rejeição não consome a possibilidade de uma reversão futura bem-sucedida
	x.submit(x.cmd("WIN", "100.00", ""))
	x.expect(x.submit(x.cmd("ROLLBACK", "100.00", win.ExternalTransactionID)), wager.StatusProcessed, "")
	x.balance("0.00", 5)
	x.reconciled()
}

func testRefundBeforeBet(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	bet := x.cmd("BET", "30.00", "")
	refundCmd := x.cmd("REFUND", "30.00", bet.ExternalTransactionID)

	pending := x.submit(refundCmd)
	x.expect(pending, wager.StatusPendingReference, "")
	x.balance("100.00", 1)
	sameEvents(t, x.eventsTail(1), wager.EventPendingReference)
	if r := x.submit(refundCmd); !r.IdempotentReplay || r.Transaction.Status() != wager.StatusPendingReference {
		t.Fatal("replay of a pending transaction must return the pending state")
	}

	// ainda sem a aposta: o worker não encerra nada antes do prazo
	x.env.Clock.Advance(2 * time.Second)
	if _, err := x.svc.ResolvePendingReferences(x.ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := x.svc.GetTransaction(x.ctx, pending.Transaction.ID()); got.Status() != wager.StatusPendingReference || got.ReferenceAttempts() != 1 {
		t.Fatalf("after retry: %s attempts=%d", got.Status(), got.ReferenceAttempts())
	}

	x.submit(bet) // a aposta chega
	x.balance("70.00", 2)
	if _, err := x.svc.ResolvePendingReferences(x.ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := x.svc.GetTransaction(x.ctx, pending.Transaction.ID())
	if got.Status() != wager.StatusProcessed {
		t.Fatalf("refund = %s/%s", got.Status(), got.FailureCode())
	}
	x.balance("100.00", 3)
	if b, _ := got.ResultBalance(); b.Amount() != "100.00" {
		t.Fatalf("result balance %s", b)
	}
	// replay depois de processado devolve o resultado final
	if r := x.submit(refundCmd); r.Transaction.Status() != wager.StatusProcessed || r.Balance.Amount() != "100.00" {
		t.Fatal("replay after resolution")
	}
	x.reconciled()
}

func testPendingExpires(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	o := x.submit(x.cmd("ROLLBACK", "10.00", "never-arrives"))
	x.expect(o, wager.StatusPendingReference, "")
	for i := 0; i < 40; i++ {
		x.env.Clock.Advance(40 * time.Second)
		if _, err := x.svc.ResolvePendingReferences(x.ctx); err != nil {
			t.Fatal(err)
		}
		got, _ := x.svc.GetTransaction(x.ctx, o.Transaction.ID())
		if got.Status() == wager.StatusPendingReference {
			continue
		}
		if got.Status() != wager.StatusRejected || got.FailureCode() != wager.FailReferenceNotFound {
			t.Fatalf("final = %s/%s", got.Status(), got.FailureCode())
		}
		x.balance("100.00", 1)
		sameEvents(t, x.eventsTail(1), wager.EventTransactionRejected)
		// reprocessar não muda nada
		n, _ := x.svc.ResolvePendingReferences(x.ctx)
		if n != 0 {
			t.Fatalf("claimed %d after expiry", n)
		}
		return
	}
	t.Fatal("pending transaction never expired")
}

func testMismatches(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	bet := x.cmd("BET", "30.00", "")
	x.submit(bet)

	x.expect(x.submit(x.cmd("REFUND", "29.00", bet.ExternalTransactionID)), wager.StatusRejected, wager.FailReferenceMismatch)

	other := x.cmd("REFUND", "30.00", bet.ExternalTransactionID)
	other.RoundID = "round-2"
	x.expect(x.submit(other), wager.StatusRejected, wager.FailReferenceMismatch)

	// referência a transação de outra carteira
	otherPlayer := id.New()
	ow, err := x.svc.OpenWallet(x.ctx, wagering.OpenWalletCommand{PlayerID: otherPlayer, Currency: "BRL", InitialBalance: "100.00"})
	if err != nil {
		t.Fatal(err)
	}
	oBet := x.cmd("BET", "10.00", "")
	oBet.WalletID, oBet.PlayerID = ow.ID(), otherPlayer
	x.submit(oBet)
	x.expect(x.submit(x.cmd("REFUND", "10.00", oBet.ExternalTransactionID)), wager.StatusRejected, wager.FailReferenceMismatch)

	// REFUND só aceita BET; WIN/LOSS são tipos inválidos como referência
	loss := x.cmd("LOSS", "0.00", "")
	x.submit(loss)
	x.expect(x.submit(x.cmd("REFUND", "1.00", loss.ExternalTransactionID)), wager.StatusRejected, wager.FailReferenceInvalidKind)
	win := x.cmd("WIN", "5.00", "")
	x.submit(win)
	x.expect(x.submit(x.cmd("REFUND", "5.00", win.ExternalTransactionID)), wager.StatusRejected, wager.FailReferenceInvalidKind)

	// referência rejeitada nunca foi processada
	bad := x.cmd("BET", "1000.00", "")
	x.expect(x.submit(bad), wager.StatusRejected, wager.FailInsufficientFunds)
	x.expect(x.submit(x.cmd("REFUND", "1000.00", bad.ExternalTransactionID)), wager.StatusRejected, wager.FailReferenceNotProcessed)

	x.balance("75.00", 3)
	x.reconciled()
}

func testWalletChecks(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	c := x.cmd("BET", "10.00", "")
	c.PlayerID = id.New()
	x.expect(x.submit(c), wager.StatusRejected, wager.FailWalletMismatch)

	c = x.cmd("BET", "10.00", "")
	c.Currency = "USD"
	x.expect(x.submit(c), wager.StatusRejected, wager.FailWalletMismatch)
	x.balance("100.00", 1)

	c = x.cmd("BET", "10.00", "")
	c.WalletID = id.New()
	if _, err := x.svc.Submit(x.ctx, c); !errors.Is(err, wagering.ErrWalletNotFound) {
		t.Fatalf("unknown wallet: %v", err)
	}
	if _, err := x.svc.GetWallet(x.ctx, id.New()); !errors.Is(err, wagering.ErrWalletNotFound) {
		t.Fatalf("GetWallet: %v", err)
	}
	if _, err := x.svc.Reconcile(x.ctx, id.New()); !errors.Is(err, wagering.ErrWalletNotFound) {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, err := x.svc.ListLedger(x.ctx, x.wallet, "%%%", 10); !errors.Is(err, wagering.ErrInvalidInput) {
		t.Fatalf("bad cursor: %v", err)
	}
}

func testConcurrentResolvers(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	const n = 5
	bets := make([]wagering.SubmitCommand, n)
	refunds := make([]*wagering.Outcome, n)
	for i := range bets {
		bets[i] = x.cmd("BET", "10.00", "")
		refunds[i] = x.submit(x.cmd("REFUND", "10.00", bets[i].ExternalTransactionID))
		x.expect(refunds[i], wager.StatusPendingReference, "")
	}
	for i := range bets {
		x.submit(bets[i])
	}
	x.balance("50.00", 6)
	workers := []*wagering.Service{x.env.NewService(), x.env.NewService(), x.env.NewService()}
	for round := 0; round < 3; round++ {
		runParallel(len(workers), func(i int) {
			if _, err := workers[i].ResolvePendingReferences(x.ctx); err != nil {
				t.Errorf("worker: %v", err)
			}
		})
	}
	for _, r := range refunds {
		got, err := x.svc.GetTransaction(x.ctx, r.Transaction.ID())
		if err != nil {
			t.Fatal(err)
		}
		if got.Status() != wager.StatusProcessed {
			t.Fatalf("refund %s", got.Status())
		}
	}
	x.balance("100.00", 11)
	if x.ledgerCount() != 1+2*n {
		t.Fatalf("ledger = %d", x.ledgerCount())
	}
	x.reconciled()
}
