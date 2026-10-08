// Package messaging consome as mensagens de transação da fila SQS FIFO.
//
// Cada mensagem é processada pelo mesmo serviço da API, numa única
// transação que também grava a inbox. Se o processo cair depois do COMMIT e
// antes de apagar a mensagem, a reentrega é reconhecida pela inbox e apenas
// apagada: nenhuma movimentação é duplicada.
package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/platform/metrics"
)

// ConsumerName identifica este consumidor na tabela inbox_messages.
const ConsumerName = "wager-transactions"

// Message é uma mensagem recebida da fila.
type Message struct {
	ID            string // MessageId do SQS (estável entre reentregas)
	ReceiptHandle string
	Body          string
	ReceiveCount  int
}

// Queue é a fila de entrada.
type Queue interface {
	// Receive espera até wait por até max mensagens (long polling).
	Receive(ctx context.Context, max int, wait time.Duration) ([]Message, error)
	// Delete apaga a mensagem (processada com sucesso).
	Delete(ctx context.Context, receiptHandle string) error
	// Release devolve a mensagem à fila imediatamente (visibilidade 0).
	// Depois de maxReceiveCount recebimentos a fila a move para a DLQ.
	Release(ctx context.Context, receiptHandle string) error
}

// Processor é o que o consumidor precisa do serviço de aplicação.
type Processor interface {
	SubmitMessage(ctx context.Context, m wagering.InboxMessage, c wagering.SubmitCommand) (*wagering.Outcome, error)
}

// Payload é o corpo JSON esperado na fila. Campos desconhecidos são rejeitados.
type Payload struct {
	IdempotencyKey        string `json:"idempotencyKey"`
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	PlayerID              string `json:"playerId"`
	WalletID              string `json:"walletId"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind"`
	Money                 struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

// Command converte o corpo da mensagem em comando de aplicação.
func Command(body string) (wagering.SubmitCommand, error) {
	var p Payload
	dec := json.NewDecoder(bytes.NewReader([]byte(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return wagering.SubmitCommand{}, fmt.Errorf("%w: body: %v", wagering.ErrInvalidInput, err)
	}
	if dec.More() {
		return wagering.SubmitCommand{}, fmt.Errorf("%w: body has trailing data", wagering.ErrInvalidInput)
	}
	if p.ProviderID == "" || p.IdempotencyKey == "" {
		return wagering.SubmitCommand{}, fmt.Errorf("%w: providerId and idempotencyKey are required", wagering.ErrInvalidInput)
	}
	wid, err := id.Parse(p.WalletID)
	if err != nil {
		return wagering.SubmitCommand{}, fmt.Errorf("%w: walletId: %v", wagering.ErrInvalidInput, err)
	}
	pid, err := id.Parse(p.PlayerID)
	if err != nil {
		return wagering.SubmitCommand{}, fmt.Errorf("%w: playerId: %v", wagering.ErrInvalidInput, err)
	}
	return wagering.SubmitCommand{
		ProviderID: p.ProviderID, ExternalTransactionID: p.ExternalTransactionID,
		IdempotencyKey: p.IdempotencyKey, WalletID: wid, PlayerID: pid,
		RoundID: p.RoundID, GameID: p.GameID, Kind: p.Kind,
		Amount: p.Money.Amount, Currency: p.Money.Currency,
		ReferenceExternalTransactionID: p.ReferenceExternalTransactionID,
	}, nil
}

// Result é o destino dado a uma mensagem.
type Result string

const (
	ResultProcessed Result = "processed" // processada e apagada
	ResultDuplicate Result = "duplicate" // reentrega reconhecida pela inbox; apagada
	ResultPoison    Result = "poison"    // inválida ou conflitante; segue para a DLQ
	ResultRetry     Result = "retry"     // falha temporária; fica na fila
)

// Config parametriza o consumidor.
type Config struct {
	Workers int
	Batch   int
	Wait    time.Duration
}

// DefaultConfig devolve valores razoáveis.
func DefaultConfig() Config { return Config{Workers: 4, Batch: 10, Wait: 5 * time.Second} }

// Consumer lê a fila e entrega as mensagens ao serviço.
type Consumer struct {
	queue Queue
	proc  Processor
	cfg   Config
	log   *slog.Logger
	msgs  *metrics.CounterVec
}

// Option personaliza o Consumer.
type Option func(*Consumer)

// WithLogger define o logger.
func WithLogger(l *slog.Logger) Option { return func(c *Consumer) { c.log = l } }

// WithMetrics registra o contador de mensagens.
func WithMetrics(r *metrics.Registry) Option {
	return func(c *Consumer) {
		c.msgs = r.Counter("queue_messages_total", "Mensagens consumidas por resultado.", "result")
	}
}

// New cria o consumidor.
func New(q Queue, p Processor, cfg Config, opts ...Option) *Consumer {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Batch < 1 || cfg.Batch > 10 {
		cfg.Batch = 10
	}
	c := &Consumer{queue: q, proc: p, cfg: cfg, log: slog.Default()}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Handle processa uma mensagem e decide o que fazer com ela na fila.
func (c *Consumer) Handle(ctx context.Context, m Message) (Result, error) {
	res, err := c.decide(ctx, m)
	if c.msgs != nil {
		c.msgs.Inc(string(res))
	}
	switch res {
	case ResultProcessed, ResultDuplicate:
		if derr := c.queue.Delete(context.WithoutCancel(ctx), m.ReceiptHandle); derr != nil {
			// A movimentação já está gravada; a reentrega será dedupada pela inbox.
			return res, fmt.Errorf("delete message %s: %w", m.ID, derr)
		}
	case ResultPoison:
		if rerr := c.queue.Release(context.WithoutCancel(ctx), m.ReceiptHandle); rerr != nil {
			return res, errors.Join(err, fmt.Errorf("release message %s: %w", m.ID, rerr))
		}
	}
	return res, err
}

func (c *Consumer) decide(ctx context.Context, m Message) (Result, error) {
	cmd, err := Command(m.Body)
	if err != nil {
		return ResultPoison, err
	}
	sum := sha256.Sum256([]byte(m.Body))
	out, err := c.proc.SubmitMessage(ctx, wagering.InboxMessage{
		Consumer: ConsumerName, MessageID: m.ID, PayloadHash: hex.EncodeToString(sum[:]),
	}, cmd)
	switch {
	case err == nil && out.DuplicateMessage:
		return ResultDuplicate, nil
	case err == nil:
		return ResultProcessed, nil
	case errors.Is(err, wagering.ErrInvalidInput),
		errors.Is(err, wagering.ErrInboxConflict),
		errors.Is(err, wagering.ErrIdempotencyConflict):
		return ResultPoison, err
	default: // carteira inexistente ainda, banco indisponível, etc.
		return ResultRetry, err
	}
}

func (c *Consumer) poll(ctx context.Context) {
	for ctx.Err() == nil {
		msgs, err := c.queue.Receive(ctx, c.cfg.Batch, c.cfg.Wait)
		if err != nil {
			if ctx.Err() == nil {
				c.log.Error("receive", slog.String("error", err.Error()))
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			continue
		}
		for _, m := range msgs {
			res, err := c.Handle(ctx, m)
			if err != nil {
				c.log.Warn("message not completed", slog.String("messageId", m.ID),
					slog.String("result", string(res)), slog.Int("receiveCount", m.ReceiveCount),
					slog.String("error", err.Error()))
			}
		}
	}
}

// Run consome até o contexto ser cancelado.
func (c *Consumer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.poll(ctx) }()
	}
	wg.Wait()
}
