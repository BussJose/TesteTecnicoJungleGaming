//go:build integration

package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/monii/backend-challenge-go/internal/application/outbox"
	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
	"github.com/monii/backend-challenge-go/internal/messaging"
	"github.com/monii/backend-challenge-go/internal/platform/postgres"
	"github.com/monii/backend-challenge-go/internal/platform/postgres/pgtest"
	"github.com/monii/backend-challenge-go/internal/platform/sqs"
)

// Estes testes usam PostgreSQL e SQS (LocalStack) REAIS, sem mocks:
//
//	DATABASE_URL=postgres://wager:wager@localhost:5432/wager?sslmode=disable \
//	AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//	    go test -race -tags integration ./internal/platform/sqs/...
//
// Cada teste cria as suas próprias filas (com nome único) e as apaga no fim.

type rig struct {
	t      *testing.T
	ctx    context.Context
	client *sqs.Client
	api    *awssqs.Client
	store  *postgres.Store
	svc    *wagering.Service
	wager  string // URL da fila de entrada
	dlq    string
	events string
	wallet id.ID
	player id.ID
}

func newRig(t *testing.T) *rig {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("AWS_ENDPOINT_URL não definida: teste com LocalStack ignorado")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := pgtest.NewDatabase(t)
	store := postgres.NewStore(pool)
	svc := wagering.NewService(store, wagering.DefaultConfig(), wagering.WithInstanceID("it"))

	base, err := sqs.NewClient(ctx, sqs.Options{Region: "us-east-1", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	api := base.API
	suffix := id.New().String()[24:]
	create := func(name string, attrs map[string]string) (string, string) {
		attrs["FifoQueue"] = "true"
		out, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(name + "-" + suffix + ".fifo"), Attributes: attrs})
		if err != nil {
			t.Fatalf("create queue %s: %v", name, err)
		}
		t.Cleanup(func() { _, _ = api.DeleteQueue(context.Background(), &awssqs.DeleteQueueInput{QueueUrl: out.QueueUrl}) })
		a, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
			QueueUrl: out.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
		if err != nil {
			t.Fatal(err)
		}
		return aws.ToString(out.QueueUrl), a.Attributes["QueueArn"]
	}
	dlqURL, dlqARN := create("dlq", map[string]string{})
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": dlqARN, "maxReceiveCount": "3"})
	wagerURL, _ := create("tx", map[string]string{"VisibilityTimeout": "2", "RedrivePolicy": string(redrive)})
	eventsURL, _ := create("events", map[string]string{"VisibilityTimeout": "30"})

	w, err := svc.OpenWallet(ctx, wagering.OpenWalletCommand{PlayerID: id.New(), Currency: "BRL", InitialBalance: "100.00"})
	if err != nil {
		t.Fatal(err)
	}
	client := &sqs.Client{API: api, WagerQueueURL: wagerURL, EventsQueueURL: eventsURL}
	return &rig{t: t, ctx: ctx, client: client, api: api, store: store, svc: svc,
		wager: wagerURL, dlq: dlqURL, events: eventsURL, wallet: w.ID(), player: w.PlayerID()}
}

// body monta o envelope WagerTransactionRequested. messageId = "msg-"+ext,
// então reenviar o mesmo ext é a mesma mensagem lógica.
func (r *rig) body(ext, amount string) string {
	return fmt.Sprintf(`{"messageId":"msg-%s-%s","type":"WagerTransactionRequested","occurredAt":"2026-10-08T12:00:00Z","data":{"idempotencyKey":"k-%s","providerId":"prov","externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"%s","currency":"BRL"}}}`,
		r.wallet.String()[24:], ext, ext, ext, r.player, r.wallet, amount)
}

var sendSeq atomic.Int64

// send envia uma mensagem; cada envio tem um MessageDeduplicationId próprio,
// o que simula um produtor que repete o envio fora da janela de deduplicação.
func (r *rig) send(group, body string) {
	_, err := r.api.SendMessage(r.ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(r.wager), MessageBody: aws.String(body),
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(fmt.Sprintf("d-%d", sendSeq.Add(1))),
	})
	if err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) startConsumer(q messaging.Queue) context.CancelFunc {
	c := messaging.New(q, r.svc, messaging.Config{Workers: 2, Batch: 10, Wait: time.Second})
	ctx, cancel := context.WithCancel(r.ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); c.Run(ctx) }()
	return func() { cancel(); wg.Wait() }
}

func (r *rig) balance() string {
	w, err := r.svc.GetWallet(r.ctx, r.wallet)
	if err != nil {
		r.t.Fatal(err)
	}
	return w.Balance().Amount()
}

func (r *rig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.t.Fatalf("timeout esperando: %s", what)
}

// drain lê (e apaga) mensagens da fila até chegarem n ou o prazo acabar.
func (r *rig) drain(url string, n int, within time.Duration) []string {
	var out []string
	deadline := time.Now().Add(within)
	for len(out) < n && time.Now().Before(deadline) {
		res, err := r.api.ReceiveMessage(r.ctx, &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			r.t.Fatal(err)
		}
		for _, m := range res.Messages {
			out = append(out, aws.ToString(m.Body))
			_, _ = r.api.DeleteMessage(r.ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return out
}

func (r *rig) dispatcher(pub outbox.Publisher, owner string) *outbox.Dispatcher {
	cfg := outbox.DefaultConfig()
	cfg.Poll = 50 * time.Millisecond
	return outbox.NewDispatcher(r.store, pub, cfg, outbox.WithOwner(owner))
}

type countingPublisher struct {
	inner outbox.Publisher
	mu    sync.Mutex
	count map[id.ID]int
}

func (p *countingPublisher) Publish(ctx context.Context, env wager.Envelope) error {
	if err := p.inner.Publish(ctx, env); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count[env.EventID]++
	return nil
}

// Caminho feliz: fila de entrada -> consumidor -> Postgres -> outbox -> fila de eventos.
func TestEndToEndThroughSQS(t *testing.T) {
	r := newRig(t)
	stop := r.startConsumer(r.client.WagerQueue())
	defer stop()

	r.send(r.wallet.String(), r.body("b1", "30.00"))
	r.send(r.wallet.String(), r.body("b2", "20.00"))
	r.waitFor("saldo 50.00", func() bool { return r.balance() == "50.00" })

	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	go r.dispatcher(r.client.EventPublisher(), "pub").Run(ctx)

	// OPENING(2 eventos) + 2 x (Processed + BalanceChanged)
	msgs := r.drain(r.events, 6, 30*time.Second)
	if len(msgs) != 6 {
		t.Fatalf("eventos recebidos = %d, want 6", len(msgs))
	}
	seen := map[id.ID]bool{}
	for _, b := range msgs {
		var env wager.Envelope
		if err := json.Unmarshal([]byte(b), &env); err != nil {
			t.Fatal(err)
		}
		if seen[env.EventID] {
			t.Fatalf("evento repetido %s", env.EventID)
		}
		seen[env.EventID] = true
		if env.AggregateID != r.wallet || env.Version != 1 || env.CorrelationID == "" || len(env.Data) == 0 {
			t.Fatalf("envelope inválido: %s", b)
		}
	}
}

// Duas mensagens com o mesmo negócio (produtor repetiu) e uma entrega duplicada.
func TestDuplicateMessagesMoveMoneyOnce(t *testing.T) {
	r := newRig(t)
	stop := r.startConsumer(r.client.WagerQueue())
	defer stop()
	for i := 0; i < 5; i++ {
		r.send(r.wallet.String(), r.body("same", "10.00"))
	}
	r.send(r.wallet.String(), r.body("last", "1.00"))
	r.waitFor("saldo 89.00", func() bool { return r.balance() == "89.00" })
	time.Sleep(3 * time.Second) // dá tempo a qualquer reentrega
	if got := r.balance(); got != "89.00" {
		t.Fatalf("saldo = %s, want 89.00", got)
	}
}

// Mensagem inválida: nunca é processada e termina na DLQ após o redrive.
func TestPoisonMessageEndsInDLQ(t *testing.T) {
	r := newRig(t)
	stop := r.startConsumer(r.client.WagerQueue())
	defer stop()
	r.send("poison-group", `{"isto":"nao é uma transação"}`)
	got := r.drain(r.dlq, 1, 40*time.Second)
	if len(got) != 1 {
		t.Fatalf("DLQ recebeu %d mensagens, want 1", len(got))
	}
	if r.balance() != "100.00" {
		t.Fatalf("saldo mudou: %s", r.balance())
	}
}

// failingDelete simula o consumidor que cai depois do COMMIT e antes de apagar.
type failingDelete struct {
	messaging.Queue
	left atomic.Int32
}

func (q *failingDelete) Delete(ctx context.Context, h string) error {
	if q.left.Add(-1) >= 0 {
		return errors.New("processo interrompido antes do DeleteMessage")
	}
	return q.Queue.Delete(ctx, h)
}

func TestConsumerInterruptedAfterCommit(t *testing.T) {
	r := newRig(t)
	q := &failingDelete{Queue: r.client.WagerQueue()}
	q.left.Store(1)
	stop := r.startConsumer(q)
	defer stop()
	r.send(r.wallet.String(), r.body("b1", "30.00"))
	r.waitFor("saldo 70.00", func() bool { return r.balance() == "70.00" })
	// a fila reentrega após o timeout de visibilidade (2s); a inbox evita repetir
	time.Sleep(6 * time.Second)
	if got := r.balance(); got != "70.00" {
		t.Fatalf("saldo = %s, want 70.00", got)
	}
	if q.left.Load() >= 0 {
		t.Fatal("a falha de Delete nunca ocorreu: o teste não exercitou a reentrega")
	}
}

// Dois publicadores sobre a mesma outbox: cada evento é enviado uma única vez.
func TestTwoOutboxPublishers(t *testing.T) {
	r := newRig(t)
	for i := 0; i < 10; i++ {
		if _, err := r.svc.Submit(r.ctx, wagering.SubmitCommand{
			ProviderID: "prov", ExternalTransactionID: fmt.Sprintf("b%d", i), IdempotencyKey: fmt.Sprintf("k%d", i),
			WalletID: r.wallet, PlayerID: r.player, RoundID: "r", GameID: "g", Kind: "BET", Amount: "1.00", Currency: "BRL",
		}); err != nil {
			t.Fatal(err)
		}
	}
	pub := &countingPublisher{inner: r.client.EventPublisher(), count: map[id.ID]int{}}
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	go r.dispatcher(pub, "pub-1").Run(ctx)
	go r.dispatcher(pub, "pub-2").Run(ctx)

	const total = 2 + 10*2
	r.waitFor("todos os eventos publicados", func() bool {
		n, err := r.store.PendingOutbox(r.ctx, r.wallet)
		return err == nil && n == 0
	})
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.count) != total {
		t.Fatalf("eventos distintos = %d, want %d", len(pub.count), total)
	}
	for eid, n := range pub.count {
		if n != 1 {
			t.Fatalf("evento %s enviado %d vezes", eid, n)
		}
	}
}
