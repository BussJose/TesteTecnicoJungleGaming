//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/platform/postgres/pgtest"
	"github.com/monii/backend-challenge-go/internal/platform/sqs"
)

// Sobe a aplicação inteira com o Fx (composição, partida e desligamento)
// sobre PostgreSQL, SQS (LocalStack) e Keycloak REAIS, e verifica:
//   - a composição valida e a aplicação fica pronta (/health/ready);
//   - o fluxo completo: HTTP -> fila de entrada -> consumidor -> banco -> outbox -> fila de eventos;
//   - o desligamento libera tudo (porta fechada, consumidor parado, sem perder mensagem);
//   - uma nova partida retoma o trabalho que ficou na fila.

var dedupSeq atomic.Int64

func TestAppLifecycleWithRealInfrastructure(t *testing.T) {
	kc := strings.TrimRight(os.Getenv("KEYCLOAK_URL"), "/")
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if kc == "" || endpoint == "" {
		t.Skip("KEYCLOAK_URL e AWS_ENDPOINT_URL são necessárias: teste ignorado")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, dbURL := pgtest.NewDatabaseURL(t)

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	sq, err := sqs.NewClient(ctx, sqs.Options{Region: "us-east-1", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	suffix := id.New().String()[24:]
	newQueue := func(name string, attrs map[string]string) string {
		attrs["FifoQueue"] = "true"
		out, err := sq.API.CreateQueue(ctx, &awssqs.CreateQueueInput{
			QueueName: aws.String(name + "-" + suffix + ".fifo"), Attributes: attrs})
		if err != nil {
			t.Fatalf("create queue: %v", err)
		}
		t.Cleanup(func() {
			_, _ = sq.API.DeleteQueue(context.Background(), &awssqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
		})
		return aws.ToString(out.QueueUrl)
	}
	txQueue := newQueue("app-tx", map[string]string{"VisibilityTimeout": "5"})
	eventsQueue := newQueue("app-events", map[string]string{"VisibilityTimeout": "30"})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	base := "http://" + addr

	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("SQS_WAGER_QUEUE_URL", txQueue)
	t.Setenv("SQS_EVENTS_QUEUE_URL", eventsQueue)
	t.Setenv("KEYCLOAK_URL", kc)
	t.Setenv("INSTANCE_ID", "app-test")
	t.Setenv("PENDING_POLL_INTERVAL", "100ms")
	t.Setenv("OUTBOX_POLL_INTERVAL", "50ms")

	start := func() *fx.App {
		a := fx.New(Module(), fx.NopLogger)
		if err := a.Err(); err != nil {
			t.Fatalf("composição inválida: %v", err)
		}
		sctx, c := context.WithTimeout(ctx, 40*time.Second)
		defer c()
		if err := a.Start(sctx); err != nil {
			t.Fatalf("partida: %v", err)
		}
		return a
	}
	stop := func(a *fx.App) {
		sctx, c := context.WithTimeout(ctx, 30*time.Second)
		defer c()
		if err := a.Stop(sctx); err != nil {
			t.Fatalf("desligamento: %v", err)
		}
	}

	token := func() string {
		form := url.Values{"grant_type": {"client_credentials"}, "client_id": {"wager-internal"}, "client_secret": {"internal-secret"}}
		res, err := http.PostForm(kc+"/realms/wager/protocol/openid-connect/token", form)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var out struct {
			AccessToken string `json:"access_token"`
		}
		_ = json.NewDecoder(res.Body).Decode(&out)
		if out.AccessToken == "" {
			t.Fatal("sem token do Keycloak")
		}
		return out.AccessToken
	}()
	call := func(method, path, body string) (int, map[string]any) {
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, nil
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		return res.StatusCode, m
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(40 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("timeout esperando: %s", what)
	}
	balance := func(walletID string) string {
		_, m := call("GET", "/wallets/"+walletID, "")
		b, _ := m["balance"].(map[string]any)
		s, _ := b["amount"].(string)
		return s
	}
	dbBalance := func(walletID string) int64 {
		var n int64
		if err := pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id = $1::text::uuid`, walletID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	app1 := start()
	waitFor("/health/ready", func() bool { c, _ := call("GET", "/health/ready", ""); return c == 200 })

	player := id.New().String()
	code, w := call("POST", "/wallets", `{"playerId":"`+player+`","initialBalance":{"amount":"100.00","currency":"BRL"}}`)
	if code != 201 {
		t.Fatalf("abrir carteira: %d %v", code, w)
	}
	wallet, _ := w["id"].(string)

	send := func(ext, amount string) {
		body := fmt.Sprintf(`{"messageId":"msg-%s","type":"WagerTransactionRequested","occurredAt":"2026-10-08T12:00:00Z","data":{"idempotencyKey":"k-%s","providerId":"provider-a","externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"%s","currency":"BRL"}}}`,
			ext, ext, ext, player, wallet, amount)
		_, err := sq.API.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl: aws.String(txQueue), MessageBody: aws.String(body),
			MessageGroupId:         aws.String(wallet),
			MessageDeduplicationId: aws.String(fmt.Sprintf("d-%d", dedupSeq.Add(1))),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// 1) fluxo completo pela aplicação montada
	send("app-bet-1", "10.00")
	waitFor("saldo 90.00", func() bool { return balance(wallet) == "90.00" })
	types := map[string]bool{}
	deadline := time.Now().Add(40 * time.Second)
	for len(types) < 2 && time.Now().Before(deadline) {
		res, err := sq.API.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(eventsQueue), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range res.Messages {
			var env struct {
				EventType   string `json:"eventType"`
				AggregateID string `json:"aggregateId"`
			}
			_ = json.Unmarshal([]byte(aws.ToString(m.Body)), &env)
			if env.AggregateID == wallet {
				types[env.EventType] = true
			}
		}
	}
	if !types["WagerTransactionProcessed"] || !types["WalletBalanceChanged"] {
		t.Fatalf("eventos publicados: %v", types)
	}

	// 2) desligamento: porta fechada e consumidor parado
	stop(app1)
	if c, _ := call("GET", "/health/live", ""); c != 0 {
		t.Fatalf("a porta HTTP continuou aberta depois do desligamento (status %d)", c)
	}
	send("app-bet-2", "10.00")
	time.Sleep(4 * time.Second)
	if got := dbBalance(wallet); got != 9000 {
		t.Fatalf("o consumidor continuou ativo depois do desligamento: saldo %d", got)
	}

	// 3) nova partida: a mensagem que ficou na fila é processada
	app2 := start()
	defer stop(app2)
	waitFor("saldo 80.00 após reinício", func() bool { return balance(wallet) == "80.00" })
	if c, m := call("POST", "/wallets/"+wallet+"/reconciliation", ""); c != 200 || m["consistent"] != true {
		t.Fatalf("conciliação após reinício: %d %v", c, m)
	}
}
