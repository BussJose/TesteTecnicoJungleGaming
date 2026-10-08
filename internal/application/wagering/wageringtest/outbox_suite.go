package wageringtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/outbox"
	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

// RecordingPublisher guarda o que foi publicado, na ordem. FailWhen, se
// definida, decide quais envios falham (injeção de falhas).
type RecordingPublisher struct {
	mu       sync.Mutex
	envs     []wager.Envelope
	FailWhen func(env wager.Envelope, call int) bool
	calls    int
}

// Publish implementa outbox.Publisher.
func (p *RecordingPublisher) Publish(_ context.Context, env wager.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.FailWhen != nil && p.FailWhen(env, p.calls) {
		return errors.New("injected publish failure")
	}
	p.envs = append(p.envs, env)
	return nil
}

// Published devolve uma cópia do que foi publicado.
func (p *RecordingPublisher) Published() []wager.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]wager.Envelope(nil), p.envs...)
}

func (x *h) dispatcher(pub outbox.Publisher, owner string) *outbox.Dispatcher {
	return outbox.NewDispatcher(x.env.Outbox, pub, outbox.DefaultConfig(),
		outbox.WithClock(x.env.Clock.Now), outbox.WithOwner(owner))
}

func (x *h) pending() int {
	x.t.Helper()
	n, err := x.env.OutboxPending(x.ctx, x.wallet)
	if err != nil {
		x.t.Fatal(err)
	}
	return n
}

func (x *h) message(consumer, msgID, hash string, c wagering.SubmitCommand) (*wagering.Outcome, error) {
	return x.svc.SubmitMessage(x.ctx, wagering.InboxMessage{Consumer: consumer, MessageID: msgID, PayloadHash: hash}, c)
}

func testInbox(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	c := x.cmd("BET", "10.00", "")

	o, err := x.message("q", "msg-1", "hash-a", c)
	if err != nil || o.DuplicateMessage || o.Transaction.Status() != wager.StatusProcessed {
		t.Fatalf("first delivery: %+v %v", o, err)
	}
	// reentrega da mesma mensagem (consumidor caiu antes de confirmar)
	o, err = x.message("q", "msg-1", "hash-a", c)
	if err != nil || !o.DuplicateMessage {
		t.Fatalf("redelivery: %+v %v", o, err)
	}
	x.balance("90.00", 2)

	// mesmo messageId com conteúdo diferente
	if _, err := x.message("q", "msg-1", "hash-b", c); !errors.Is(err, wagering.ErrInboxConflict) {
		t.Fatalf("payload change: %v", err)
	}
	// o produtor reenviou o mesmo negócio como mensagem nova: a idempotência
	// de negócio impede a segunda movimentação
	o, err = x.message("q", "msg-2", "hash-a", c)
	if err != nil || !o.IdempotentReplay {
		t.Fatalf("producer retry: %+v %v", o, err)
	}
	x.balance("90.00", 2)
	// outro consumidor tem a sua própria inbox
	if o, err = x.message("other", "msg-1", "hash-a", c); err != nil || !o.IdempotentReplay {
		t.Fatalf("other consumer: %+v %v", o, err)
	}
	if x.ledgerCount() != 2 {
		t.Fatalf("ledger = %d", x.ledgerCount())
	}
	x.reconciled()

	// 20 entregas simultâneas da mesma mensagem (visibilidade expirou)
	c2 := x.cmd("BET", "5.00", "")
	var mu sync.Mutex
	fresh, dup := 0, 0
	runParallel(20, func(int) {
		o, err := x.env.NewService().SubmitMessage(x.ctx, wagering.InboxMessage{Consumer: "q", MessageID: "msg-par", PayloadHash: "h"}, c2)
		if err != nil {
			t.Errorf("parallel: %v", err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if o.DuplicateMessage {
			dup++
		} else {
			fresh++
		}
	})
	if fresh != 1 || dup != 19 {
		t.Fatalf("fresh=%d dup=%d", fresh, dup)
	}
	x.balance("85.00", 3)
}

func testInboxAtomic(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	bad := x.cmd("BET", "10.00", "")
	bad.Kind = "NOPE"
	if _, err := x.message("q", "msg-1", "h1", bad); !errors.Is(err, wagering.ErrInvalidInput) {
		t.Fatalf("invalid message: %v", err)
	}
	unknown := x.cmd("BET", "10.00", "")
	unknown.WalletID = id.New()
	if _, err := x.message("q", "msg-1", "h2", unknown); !errors.Is(err, wagering.ErrWalletNotFound) {
		t.Fatalf("unknown wallet: %v", err)
	}
	// nada ficou registrado na inbox: a mesma mensagem, corrigida, é aceita
	o, err := x.message("q", "msg-1", "h3", x.cmd("BET", "10.00", ""))
	if err != nil || o.DuplicateMessage || o.Transaction.Status() != wager.StatusProcessed {
		t.Fatalf("after fix: %+v %v", o, err)
	}
	x.balance("90.00", 2)
}

func testOutboxTwoPublishers(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "1000.00")
	for i := 0; i < 15; i++ {
		x.submit(x.cmd("BET", "1.00", ""))
	}
	total := x.eventCount()
	if total != 2+15*2 || x.pending() != total {
		t.Fatalf("events=%d pending=%d", total, x.pending())
	}
	pub := &RecordingPublisher{}
	d1, d2 := x.dispatcher(pub, "pub-1"), x.dispatcher(pub, "pub-2")
	runParallel(2, func(i int) {
		d := []*outbox.Dispatcher{d1, d2}[i]
		for j := 0; j < 100; j++ {
			n, err := d.Cycle(x.ctx)
			if err != nil {
				t.Errorf("cycle: %v", err)
				return
			}
			if n == 0 && x.pending() == 0 {
				return
			}
		}
	})
	got := pub.Published()
	if len(got) != total || x.pending() != 0 {
		t.Fatalf("published %d of %d, pending %d", len(got), total, x.pending())
	}
	seen := map[id.ID]bool{}
	for _, e := range got {
		if seen[e.EventID] {
			t.Fatalf("event %s published twice", e.EventID)
		}
		seen[e.EventID] = true
		if e.AggregateID != x.wallet || e.Version != 1 || e.CorrelationID == "" || len(e.Data) == 0 {
			t.Fatalf("bad envelope %+v", e)
		}
	}
	// a ordem de publicação da carteira é a ordem de gravação
	types, _ := x.env.Events(x.ctx, x.wallet)
	for i, e := range got {
		if e.EventType != types[i] {
			t.Fatalf("event %d: published %s, stored %s", i, e.EventType, types[i])
		}
	}
}

func testOutboxRetry(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	x.submit(x.cmd("BET", "10.00", ""))
	x.submit(x.cmd("BET", "10.00", ""))
	total := x.eventCount()

	failing := true
	pub := &RecordingPublisher{FailWhen: func(wager.Envelope, int) bool { return failing }}
	d := x.dispatcher(pub, "pub")
	n, err := d.Cycle(x.ctx)
	if n != 0 || err == nil {
		t.Fatalf("expected failure, got n=%d err=%v", n, err)
	}
	if x.pending() != total {
		t.Fatalf("failed publish must keep the event: pending %d of %d", x.pending(), total)
	}
	// ainda dentro do backoff: nada a fazer
	if n, _ := d.Cycle(x.ctx); n != 0 {
		t.Fatalf("published during backoff: %d", n)
	}
	failing = false
	for i := 0; i < 60 && x.pending() > 0; i++ {
		x.env.Clock.Advance(40 * time.Second)
		if _, err := d.Cycle(x.ctx); err != nil {
			t.Fatal(err)
		}
	}
	got := pub.Published()
	if len(got) != total || x.pending() != 0 {
		t.Fatalf("published %d of %d (pending %d)", len(got), total, x.pending())
	}
	types, _ := x.env.Events(x.ctx, x.wallet)
	for i, e := range got {
		if e.EventType != types[i] {
			t.Fatalf("order broken at %d: %s vs %s", i, e.EventType, types[i])
		}
	}
}

func testOutboxLease(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	x.submit(x.cmd("BET", "10.00", ""))
	total := x.eventCount()

	// um publicador reserva os eventos e "morre" sem publicar
	recs, err := x.env.Outbox.Claim(x.ctx, x.env.Clock.Now(), 100, 30*time.Second, "dead-publisher")
	if err != nil || len(recs) == 0 {
		t.Fatalf("claim: %d %v", len(recs), err)
	}
	pub := &RecordingPublisher{}
	d := x.dispatcher(pub, "alive")
	if n, _ := d.Cycle(x.ctx); n != 0 {
		t.Fatalf("must not steal a live lease, published %d", n)
	}
	x.env.Clock.Advance(31 * time.Second) // lease vencido
	for i := 0; i < 10 && x.pending() > 0; i++ {
		if _, err := d.Cycle(x.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := pub.Published(); len(got) != total || x.pending() != 0 {
		t.Fatalf("takeover: published %d of %d (pending %d) %s", len(got), total, x.pending(), fmt.Sprint(recs[0].EventID))
	}
}

// A mesma operação chegando pelos dois canais (HTTP e SQS), em qualquer ordem
// e em paralelo, move o dinheiro uma única vez: o hash e a idempotência são
// os mesmos para os dois.
func testCrossChannel(t *testing.T, newEnv func(*testing.T) *Env) {
	x := newH(t, newEnv, "100.00")
	c := x.cmd("BET", "10.00", "")

	if o := x.submit(c); o.IdempotentReplay {
		t.Fatal("first call must not be a replay")
	}
	o, err := x.message("q", "msg-1", "h1", c) // SQS depois do HTTP
	if err != nil || !o.IdempotentReplay || o.Transaction.Status() != wager.StatusProcessed {
		t.Fatalf("sqs after http: %+v %v", o, err)
	}
	x.balance("90.00", 2)

	// SQS primeiro, HTTP depois
	c2 := x.cmd("BET", "5.00", "")
	if o, err := x.message("q", "msg-2", "h2", c2); err != nil || o.IdempotentReplay {
		t.Fatalf("sqs first: %+v %v", o, err)
	}
	if o := x.submit(c2); !o.IdempotentReplay {
		t.Fatal("http after sqs must be a replay")
	}
	x.balance("85.00", 3)

	// os dois canais ao mesmo tempo
	c3 := x.cmd("BET", "7.00", "")
	var fresh atomic.Int32
	runParallel(20, func(i int) {
		var o *wagering.Outcome
		var err error
		if i%2 == 0 {
			o, err = x.env.NewService().Submit(x.ctx, c3)
		} else {
			o, err = x.env.NewService().SubmitMessage(x.ctx,
				wagering.InboxMessage{Consumer: "q", MessageID: fmt.Sprintf("par-%d", i), PayloadHash: "h"}, c3)
		}
		if err != nil {
			t.Errorf("parallel %d: %v", i, err)
			return
		}
		if !o.IdempotentReplay {
			fresh.Add(1)
		}
	})
	if fresh.Load() != 1 {
		t.Fatalf("operation applied %d times", fresh.Load())
	}
	x.balance("78.00", 4)
	x.reconciled()
}
