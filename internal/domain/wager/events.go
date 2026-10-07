package wager

import (
	"encoding/json"
	"time"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
)

// EventType identifica um evento de integração.
type EventType string

const (
	EventTransactionProcessed EventType = "WagerTransactionProcessed"
	EventTransactionRejected  EventType = "WagerTransactionRejected"
	EventWalletBalanceChanged EventType = "WalletBalanceChanged"
	EventPendingReference     EventType = "WagerTransactionPendingReference"
)

const eventVersion = 1

// Event é um evento de integração. Tipo e versão são definidos pelo
// construtor de cada evento; o payload é um snapshot imutável (JSON).
// O aggregateId é a carteira, o que permite ordenar por carteira no destino.
type Event struct {
	id            id.ID
	eventType     EventType
	version       int
	aggregateID   id.ID
	correlationID string
	causationID   string
	occurredAt    time.Time
	payload       []byte
}

// Envelope é o formato publicado no destino dos eventos.
type Envelope struct {
	EventID       id.ID           `json:"eventId"`
	EventType     EventType       `json:"eventType"`
	AggregateID   id.ID           `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

// TransactionProcessedData é o payload de WagerTransactionProcessed.
type TransactionProcessedData struct {
	TransactionID         id.ID       `json:"transactionId"`
	WalletID              id.ID       `json:"walletId"`
	PlayerID              id.ID       `json:"playerId"`
	Origin                Origin      `json:"origin"`
	ProviderID            string      `json:"providerId,omitempty"`
	ExternalTransactionID string      `json:"externalTransactionId,omitempty"`
	RoundID               string      `json:"roundId,omitempty"`
	GameID                string      `json:"gameId,omitempty"`
	Kind                  Kind        `json:"kind"`
	Money                 money.Money `json:"money"`
	Status                Status      `json:"status"`
}

// TransactionRejectedData é o payload de WagerTransactionRejected.
type TransactionRejectedData struct {
	TransactionID         id.ID       `json:"transactionId"`
	WalletID              id.ID       `json:"walletId"`
	PlayerID              id.ID       `json:"playerId"`
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	RoundID               string      `json:"roundId"`
	GameID                string      `json:"gameId"`
	Kind                  Kind        `json:"kind"`
	Money                 money.Money `json:"money"`
	FailureCode           FailureCode `json:"failureCode"`
}

// WalletBalanceChangedData é o payload de WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      id.ID       `json:"walletId"`
	TransactionID id.ID       `json:"transactionId"`
	Direction     Direction   `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

// PendingReferenceData é o payload de WagerTransactionPendingReference.
type PendingReferenceData struct {
	TransactionID                  id.ID     `json:"transactionId"`
	WalletID                       id.ID     `json:"walletId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	Kind                           Kind      `json:"kind"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	ExpiresAt                      time.Time `json:"expiresAt"`
}

func newEvent(eventID id.ID, typ EventType, aggregate id.ID, correlationID, causationID string,
	now time.Time, data any) (Event, error) {
	if eventID.IsZero() || aggregate.IsZero() {
		return Event{}, invalid("event requires eventId and aggregateId")
	}
	if correlationID == "" {
		return Event{}, invalid("event requires a correlationId")
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return Event{}, invalid("event payload: %v", err)
	}
	return Event{
		id: eventID, eventType: typ, version: eventVersion, aggregateID: aggregate,
		correlationID: correlationID, causationID: causationID,
		occurredAt: now.UTC(), payload: payload,
	}, nil
}

// NewTransactionProcessed cria WagerTransactionProcessed (inclui LOSS e OPENING).
func NewTransactionProcessed(eventID id.ID, t *WagerTransaction, correlationID string, now time.Time) (Event, error) {
	if t.status != StatusProcessed {
		return Event{}, invalid("transaction must be PROCESSED")
	}
	return newEvent(eventID, EventTransactionProcessed, t.walletID, correlationID, t.id.String(), now,
		TransactionProcessedData{
			TransactionID: t.id, WalletID: t.walletID, PlayerID: t.playerID, Origin: t.origin,
			ProviderID: t.providerID, ExternalTransactionID: t.externalID,
			RoundID: t.roundID, GameID: t.gameID, Kind: t.kind, Money: t.amount, Status: t.status,
		})
}

// NewTransactionRejected cria WagerTransactionRejected (rejeição definitiva).
func NewTransactionRejected(eventID id.ID, t *WagerTransaction, correlationID string, now time.Time) (Event, error) {
	if t.status != StatusRejected {
		return Event{}, invalid("transaction must be REJECTED")
	}
	return newEvent(eventID, EventTransactionRejected, t.walletID, correlationID, t.id.String(), now,
		TransactionRejectedData{
			TransactionID: t.id, WalletID: t.walletID, PlayerID: t.playerID,
			ProviderID: t.providerID, ExternalTransactionID: t.externalID,
			RoundID: t.roundID, GameID: t.gameID, Kind: t.kind, Money: t.amount,
			FailureCode: t.failureCode,
		})
}

// NewWalletBalanceChanged cria WalletBalanceChanged a partir do lançamento.
func NewWalletBalanceChanged(eventID id.ID, e *LedgerEntry, walletVersion int64, correlationID string, now time.Time) (Event, error) {
	return newEvent(eventID, EventWalletBalanceChanged, e.walletID, correlationID, e.transactionID.String(), now,
		WalletBalanceChangedData{
			WalletID: e.walletID, TransactionID: e.transactionID, Direction: e.direction,
			Money: e.amount, BalanceBefore: e.balanceBefore, BalanceAfter: e.balanceAfter,
			WalletVersion: walletVersion,
		})
}

// NewPendingReference cria WagerTransactionPendingReference.
func NewPendingReference(eventID id.ID, t *WagerTransaction, correlationID string, now time.Time) (Event, error) {
	if t.status != StatusPendingReference || t.expiresAt == nil {
		return Event{}, invalid("transaction must be PENDING_REFERENCE")
	}
	return newEvent(eventID, EventPendingReference, t.walletID, correlationID, t.id.String(), now,
		PendingReferenceData{
			TransactionID: t.id, WalletID: t.walletID, ProviderID: t.providerID,
			ExternalTransactionID: t.externalID, Kind: t.kind,
			ReferenceExternalTransactionID: t.referenceExternalID, ExpiresAt: t.expiresAt.UTC(),
		})
}

func (e Event) ID() id.ID             { return e.id }
func (e Event) Type() EventType       { return e.eventType }
func (e Event) Version() int          { return e.version }
func (e Event) AggregateID() id.ID    { return e.aggregateID }
func (e Event) CorrelationID() string { return e.correlationID }
func (e Event) CausationID() string   { return e.causationID }
func (e Event) OccurredAt() time.Time { return e.occurredAt }

// Payload devolve uma cópia do snapshot JSON do campo "data".
func (e Event) Payload() []byte { return append([]byte(nil), e.payload...) }

// RehydrateEnvelope monta o envelope publicável a partir dos campos
// persistidos na outbox (usado pelo publisher).
func RehydrateEnvelope(eventID id.ID, typ EventType, version int, aggregate id.ID,
	correlationID, causationID string, occurredAt time.Time, payload []byte) Envelope {
	return Envelope{
		EventID: eventID, EventType: typ, AggregateID: aggregate,
		CorrelationID: correlationID, CausationID: causationID,
		OccurredAt: occurredAt.UTC(), Version: version, Data: json.RawMessage(payload),
	}
}
