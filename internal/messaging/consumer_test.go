package messaging_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/application/wagering/wageringtest"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/messaging"
)

// fakeQueue imita o comportamento do SQS que importa aqui: visibilidade,
// contagem de recebimentos e redrive para a DLQ.
type fakeQueue struct {
	mu         sync.Mutex
	msgs       []*fqMsg
	dlq        []string
	maxReceive int
	failDelete int
	seq        int
}

type fqMsg struct {
	id       string
	body     string
	inflight bool
	count    int
	receipt  string
}

func (q *fakeQueue) Send(body string) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	m := &fqMsg{id: "m-" + strconv.Itoa(q.seq), body: body}
	q.msgs = append(q.msgs, m)
	return m.id
}

func (q *fakeQueue) Receive(_ context.Context, max int, _ time.Duration) ([]messaging.Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []messaging.Message
	var keep []*fqMsg
	for _, m := range q.msgs {
		if m.inflight || len(out) >= max {
			keep = append(keep, m)
			continue
		}
		if m.count >= q.maxReceive {
			q.dlq = append(q.dlq, m.body)
			continue
		}
		m.count++
		m.inflight = true
		m.receipt = fmt.Sprintf("%s/%d", m.id, m.count)
		out = append(out, messaging.Message{ID: m.id, ReceiptHandle: m.receipt, Body: m.body, ReceiveCount: m.count})
		keep = append(keep, m)
	}
	q.msgs = keep
	return out, nil
}

func (q *fakeQueue) Delete(_ context.Context, receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.failDelete > 0 {
		q.failDelete--
		return errors.New("injected delete failure")
	}
	for i, m := range q.msgs {
		if m.receipt == receipt {
			q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
			return nil
		}
	}
	return nil
}

func (q *fakeQueue) Release(_ context.Context, receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, m := range q.msgs {
		if m.receipt == receipt {
			m.inflight = false
		}
	}
	return nil
}

// ExpireVisibility faz as mensagens em processamento voltarem à fila.
func (q *fakeQueue) ExpireVisibility() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, m := range q.msgs {
		m.inflight = false
	}
}

func (q *fakeQueue) Len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.msgs) }
func (q *fakeQueue) DLQ() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.dlq) }

type rig struct {
	t      *testing.T
	store  *wageringtest.MemStore
	svc    *wagering.Service
	q      *fakeQueue
	c      *messaging.Consumer
	wallet id.ID
	player id.ID
}

func newRig(t *testing.T) *rig {
	store := wageringtest.NewMemStore()
	clock := wageringtest.NewFakeClock(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	svc := wagering.NewService(store, wagering.DefaultConfig(), wagering.WithClock(clock.Now))
	w, err := svc.OpenWallet(context.Background(), wagering.OpenWalletCommand{PlayerID: id.New(), Currency: "BRL", InitialBalance: "100.00"})
	if err != nil {
		t.Fatal(err)
	}
	q := &fakeQueue{maxReceive: 3}
	return &rig{t: t, store: store, svc: svc, q: q, c: messaging.New(q, svc, messaging.DefaultConfig()),
		wallet: w.ID(), player: w.PlayerID()}
}

func (r *rig) body(ext, kind, amount string) string {
	return fmt.Sprintf(`{"idempotencyKey":"k-%s","providerId":"prov","externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"r1","gameId":"g1","kind":"%s","money":{"amount":"%s","currency":"BRL"}}`,
		ext, ext, r.player, r.wallet, kind, amount)
}

// drain recebe e trata mensagens até a fila esvaziar ou n rodadas.
func (r *rig) drain(rounds int) {
	for i := 0; i < rounds; i++ {
		msgs, _ := r.q.Receive(context.Background(), 10, 0)
		for _, m := range msgs {
			_, _ = r.c.Handle(context.Background(), m)
		}
		r.q.ExpireVisibility()
	}
}

func (r *rig) balance() string {
	w, err := r.svc.GetWallet(context.Background(), r.wallet)
	if err != nil {
		r.t.Fatal(err)
	}
	return w.Balance().Amount()
}

func TestHappyPathAndPoison(t *testing.T) {
	r := newRig(t)
	r.q.Send(r.body("b1", "BET", "30.00"))
	r.q.Send(`{"nope":true}`)               // desconhecido -> veneno
	r.q.Send(r.body("b2", "WRONG", "1.00")) // inválido -> veneno
	r.q.Send(r.body("b3", "BET", "500.00")) // saldo insuficiente -> REJECTED, apagada
	r.drain(6)
	if got := r.balance(); got != "70.00" {
		t.Fatalf("balance = %s", got)
	}
	if r.q.Len() != 0 || r.q.DLQ() != 2 {
		t.Fatalf("queue=%d dlq=%d", r.q.Len(), r.q.DLQ())
	}
	tx, err := r.svc.FindTransaction(context.Background(), "prov", "b3")
	if err != nil || tx.Status() != "REJECTED" {
		t.Fatalf("b3: %v %v", tx, err)
	}
}

// O consumidor grava (COMMIT) e cai antes de apagar a mensagem.
func TestInterruptedAfterCommit(t *testing.T) {
	r := newRig(t)
	r.q.Send(r.body("b1", "BET", "30.00"))
	r.q.failDelete = 1
	msgs, _ := r.q.Receive(context.Background(), 10, 0)
	if _, err := r.c.Handle(context.Background(), msgs[0]); err == nil {
		t.Fatal("expected delete failure")
	}
	if r.balance() != "70.00" || r.q.Len() != 1 {
		t.Fatalf("balance=%s queue=%d", r.balance(), r.q.Len())
	}
	r.q.ExpireVisibility() // a fila entrega de novo
	msgs, _ = r.q.Receive(context.Background(), 10, 0)
	res, err := r.c.Handle(context.Background(), msgs[0])
	if err != nil || res != messaging.ResultDuplicate {
		t.Fatalf("redelivery: %s %v", res, err)
	}
	if r.balance() != "70.00" || r.q.Len() != 0 {
		t.Fatalf("balance=%s queue=%d", r.balance(), r.q.Len())
	}
}

// Wallet inexistente: fica na fila (não é apagada) e vai para a DLQ no fim.
func TestUnknownWalletEndsInDLQ(t *testing.T) {
	r := newRig(t)
	r.wallet = id.New()
	r.q.Send(r.body("b1", "BET", "1.00"))
	r.drain(6)
	if r.q.Len() != 0 || r.q.DLQ() != 1 {
		t.Fatalf("queue=%d dlq=%d", r.q.Len(), r.q.DLQ())
	}
}

// Várias instâncias lendo a mesma fila, com mensagens repetidas.
func TestConcurrentConsumers(t *testing.T) {
	r := newRig(t)
	for i := 0; i < 20; i++ {
		r.q.Send(r.body("b"+strconv.Itoa(i), "BET", "1.00"))
		r.q.Send(r.body("b"+strconv.Itoa(i), "BET", "1.00")) // produtor repetiu o envio
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		c := messaging.New(r.q, r.svc, messaging.Config{Workers: 2, Batch: 10, Wait: time.Millisecond})
		wg.Add(1)
		go func() { defer wg.Done(); c.Run(ctx) }()
	}
	deadline := time.Now().Add(10 * time.Second)
	for r.q.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if got := r.balance(); got != "80.00" {
		t.Fatalf("balance = %s, want 80.00", got)
	}
}
