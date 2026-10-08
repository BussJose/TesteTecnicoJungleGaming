// Package outbox publica os eventos gravados na tabela outbox_events.
//
// Os eventos são gravados na mesma transação do negócio (pelo serviço) e só
// depois do COMMIT chegam aqui. A entrega é "pelo menos uma vez": se o
// processo cair depois de enviar e antes de marcar como publicado, o evento é
// reenviado com o mesmo eventId (que é também a chave de deduplicação da
// fila FIFO) e os consumidores devem tratá-lo de forma idempotente.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
	"github.com/monii/backend-challenge-go/internal/platform/metrics"
)

// Record é um evento pendente lido da outbox.
type Record struct {
	EventID       id.ID
	AggregateID   id.ID
	Type          wager.EventType
	Version       int
	CorrelationID string
	CausationID   string
	Payload       []byte
	OccurredAt    time.Time
	Attempts      int
}

// Envelope monta o formato publicado (eventId, eventType, aggregateId,
// correlationId, causationId, occurredAt, version, data).
func (r Record) Envelope() wager.Envelope {
	return wager.RehydrateEnvelope(r.EventID, r.Type, r.Version, r.AggregateID,
		r.CorrelationID, r.CausationID, r.OccurredAt, r.Payload)
}

// Store é o armazenamento da outbox.
type Store interface {
	// Claim reserva (lease) até limit eventos não publicados, em ordem de
	// gravação, no máximo UM por agregado (carteira) e só se todos os eventos
	// anteriores do agregado já foram publicados. Instâncias concorrentes não
	// recebem o mesmo evento (FOR UPDATE SKIP LOCKED).
	Claim(ctx context.Context, now time.Time, limit int, lease time.Duration, owner string) ([]Record, error)
	// MarkPublished marca o evento como publicado.
	MarkPublished(ctx context.Context, eventID id.ID, now time.Time) error
	// Release devolve o evento à fila para nova tentativa (attempts + 1).
	Release(ctx context.Context, eventID id.ID, nextAttempt time.Time) error
}

// Publisher envia um evento ao destino (SQS FIFO).
type Publisher interface {
	Publish(ctx context.Context, env wager.Envelope) error
}

// Config parametriza o Dispatcher.
type Config struct {
	Batch       int
	Lease       time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	Poll        time.Duration
}

// DefaultConfig devolve valores razoáveis.
func DefaultConfig() Config {
	return Config{Batch: 50, Lease: 30 * time.Second, BackoffBase: time.Second, BackoffMax: 30 * time.Second, Poll: 200 * time.Millisecond}
}

// Dispatcher lê a outbox e publica os eventos.
type Dispatcher struct {
	store Store
	pub   Publisher
	cfg   Config
	owner string
	now   func() time.Time
	log   *slog.Logger

	published *metrics.CounterVec
	failures  *metrics.CounterVec
	lag       *metrics.HistogramVec
}

// Option personaliza o Dispatcher.
type Option func(*Dispatcher)

// WithClock troca o relógio (testes).
func WithClock(now func() time.Time) Option { return func(d *Dispatcher) { d.now = now } }

// WithOwner identifica a instância nos leases.
func WithOwner(v string) Option { return func(d *Dispatcher) { d.owner = v } }

// WithLogger define o logger.
func WithLogger(l *slog.Logger) Option { return func(d *Dispatcher) { d.log = l } }

// WithMetrics registra os contadores no registro informado.
func WithMetrics(r *metrics.Registry) Option {
	return func(d *Dispatcher) {
		d.published = r.Counter("outbox_events_published_total", "Eventos publicados.", "event_type")
		d.failures = r.Counter("outbox_publish_failures_total", "Falhas ao publicar eventos.", "event_type")
		d.lag = r.Histogram("outbox_publish_lag_seconds", "Tempo entre a gravação do evento e a publicação.",
			[]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}, "event_type")
	}
}

// NewDispatcher cria o Dispatcher.
func NewDispatcher(store Store, pub Publisher, cfg Config, opts ...Option) *Dispatcher {
	d := &Dispatcher{store: store, pub: pub, cfg: cfg, owner: id.New().String(),
		now: func() time.Time { return time.Now().UTC() }, log: slog.Default()}
	for _, o := range opts {
		o(d)
	}
	return d
}

func (d *Dispatcher) backoff(attempts int) time.Duration {
	if attempts > 20 {
		attempts = 20
	}
	v := d.cfg.BackoffBase << uint(attempts)
	if v > d.cfg.BackoffMax || v <= 0 {
		return d.cfg.BackoffMax
	}
	return v
}

// Cycle executa um ciclo: reserva, publica e marca. Devolve quantos eventos
// foram publicados. Falhas de publicação não interrompem o ciclo: o evento
// volta à fila com backoff e o erro é devolvido ao final.
func (d *Dispatcher) Cycle(ctx context.Context) (int, error) {
	recs, err := d.store.Claim(ctx, d.now(), d.cfg.Batch, d.cfg.Lease, d.owner)
	if err != nil {
		return 0, fmt.Errorf("claim outbox: %w", err)
	}
	var errs []error
	published := 0
	for _, r := range recs {
		if err := d.pub.Publish(ctx, r.Envelope()); err != nil {
			if d.failures != nil {
				d.failures.Inc(string(r.Type))
			}
			errs = append(errs, fmt.Errorf("publish %s: %w", r.EventID, err))
			if rerr := d.store.Release(context.WithoutCancel(ctx), r.EventID, d.now().Add(d.backoff(r.Attempts))); rerr != nil {
				errs = append(errs, fmt.Errorf("release %s: %w", r.EventID, rerr))
			}
			continue
		}
		if err := d.store.MarkPublished(context.WithoutCancel(ctx), r.EventID, d.now()); err != nil {
			// Já foi enviado: o lease expira e o evento será reenviado com o
			// mesmo eventId (deduplicado pela fila e pelos consumidores).
			errs = append(errs, fmt.Errorf("mark published %s: %w", r.EventID, err))
			continue
		}
		published++
		if d.published != nil {
			d.published.Inc(string(r.Type))
			d.lag.Observe(d.now().Sub(r.OccurredAt).Seconds(), string(r.Type))
		}
	}
	return published, errors.Join(errs...)
}

// Run publica continuamente até o contexto ser cancelado.
func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(d.cfg.Poll)
	defer t.Stop()
	for {
		for { // esvazia a fila sem esperar o próximo tick
			n, err := d.Cycle(ctx)
			if err != nil && ctx.Err() == nil {
				d.log.Error("outbox cycle", slog.String("error", err.Error()))
			}
			if err != nil || n == 0 || ctx.Err() != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
