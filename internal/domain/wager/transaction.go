package wager

import (
	"strings"
	"time"
	"unicode"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
)

const maxFieldLen = 200

// WagerTransaction é uma operação financeira (externa ou a abertura interna
// de carteira). Segue a máquina de estados:
//
//	PENDING ──► PROCESSED | REJECTED | FAILED
//	PENDING ──► PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED
//
// Estados terminais não sofrem novas transições.
type WagerTransaction struct {
	id                  id.ID
	origin              Origin
	providerID          string
	externalID          string
	idempotencyKey      string
	payloadHash         string
	walletID            id.ID
	playerID            id.ID
	roundID             string
	gameID              string
	kind                Kind
	amount              money.Money
	referenceExternalID string
	referenceID         id.ID
	status              Status
	failureCode         FailureCode
	resultBalance       *money.Money
	attempts            int
	nextAttemptAt       time.Time
	expiresAt           *time.Time
	createdAt           time.Time
	updatedAt           time.Time
	processedAt         *time.Time
}

// NewExternalParams reúne os dados de uma operação recebida por HTTP ou SQS.
type NewExternalParams struct {
	ID                             id.ID
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	WalletID                       id.ID
	PlayerID                       id.ID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
	Now                            time.Time
}

func checkText(name, v string) error {
	if v == "" {
		return invalid("%s is required", name)
	}
	if len(v) > maxFieldLen {
		return invalid("%s exceeds %d characters", name, maxFieldLen)
	}
	if strings.TrimSpace(v) != v {
		return invalid("%s must not have leading or trailing spaces", name)
	}
	for _, r := range v {
		if unicode.IsControl(r) {
			return invalid("%s must not contain control characters", name)
		}
	}
	return nil
}

// NewExternal cria uma operação externa em PENDING, validando todas as
// regras por tipo. Rejeita OPENING.
func NewExternal(p NewExternalParams) (*WagerTransaction, error) {
	if p.ID.IsZero() || p.WalletID.IsZero() || p.PlayerID.IsZero() {
		return nil, invalid("transaction requires id, walletId and playerId")
	}
	for _, f := range []struct{ n, v string }{
		{"providerId", p.ProviderID}, {"externalTransactionId", p.ExternalTransactionID},
		{"idempotencyKey", p.IdempotencyKey}, {"roundId", p.RoundID}, {"gameId", p.GameID},
	} {
		if err := checkText(f.n, f.v); err != nil {
			return nil, err
		}
	}
	if _, err := ParseExternalKind(string(p.Kind)); err != nil {
		return nil, err
	}
	if !p.Money.IsValid() {
		return nil, invalid("money requires a valid currency")
	}
	if p.Money.IsNegative() {
		return nil, invalid("money must not be negative")
	}
	if p.Kind == KindLoss {
		if !p.Money.IsZero() {
			return nil, invalid("LOSS requires amount 0.00")
		}
	} else if !p.Money.IsPositive() {
		return nil, invalid("%s requires an amount greater than zero", p.Kind)
	}
	ref := p.ReferenceExternalTransactionID
	switch {
	case p.Kind.IsReversal() && ref == "":
		return nil, invalid("%s requires referenceExternalTransactionId", p.Kind)
	case ref != "" && !p.Kind.AllowsReference():
		return nil, invalid("%s does not accept referenceExternalTransactionId", p.Kind)
	}
	if ref != "" {
		if err := checkText("referenceExternalTransactionId", ref); err != nil {
			return nil, err
		}
		if ref == p.ExternalTransactionID {
			return nil, invalid("a transaction cannot reference itself")
		}
	}
	now := p.Now.UTC()
	return &WagerTransaction{
		id: p.ID, origin: OriginExternal,
		providerID: p.ProviderID, externalID: p.ExternalTransactionID, idempotencyKey: p.IdempotencyKey,
		payloadHash: computePayloadHash(p.ProviderID, p.ExternalTransactionID, p.PlayerID, p.WalletID,
			p.RoundID, p.GameID, p.Kind, p.Money, ref),
		walletID: p.WalletID, playerID: p.PlayerID, roundID: p.RoundID, gameID: p.GameID,
		kind: p.Kind, amount: p.Money, referenceExternalID: ref,
		status: StatusPending, nextAttemptAt: now, createdAt: now, updatedAt: now,
	}, nil
}

// newOpening cria a abertura interna já concluída (OPENING / PROCESSED).
func newOpening(txID, walletID, playerID id.ID, amount money.Money, now time.Time) (*WagerTransaction, error) {
	if txID.IsZero() {
		return nil, invalid("opening transaction requires an id")
	}
	if !amount.IsPositive() {
		return nil, invalid("opening requires a positive amount")
	}
	t := &WagerTransaction{
		id: txID, origin: OriginInternal, walletID: walletID, playerID: playerID,
		kind: KindOpening, amount: amount, status: StatusPending,
		nextAttemptAt: now.UTC(), createdAt: now.UTC(), updatedAt: now.UTC(),
	}
	if err := t.MarkProcessed(amount, id.Nil, now); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *WagerTransaction) ensureOpen() error {
	if t.status.IsTerminal() {
		return ErrTerminalState
	}
	return nil
}

func (t *WagerTransaction) touch(now time.Time) { t.updatedAt = now.UTC() }

// MarkProcessed conclui a operação com sucesso. balance é o saldo observado
// após o processamento (devolvido em replays). referenceID é a transação
// resolvida, quando houver.
func (t *WagerTransaction) MarkProcessed(balance money.Money, referenceID id.ID, now time.Time) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	if !balance.IsValid() || balance.IsNegative() {
		return invalid("result balance must be a valid non-negative amount")
	}
	t.status = StatusProcessed
	t.failureCode = ""
	t.resultBalance = &balance
	t.referenceID = referenceID
	t.expiresAt = nil
	p := now.UTC()
	t.processedAt = &p
	t.touch(now)
	return nil
}

// Reject encerra a operação por uma regra de negócio (resultado definitivo).
func (t *WagerTransaction) Reject(code FailureCode, balance money.Money, now time.Time) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	if code == "" {
		return invalid("rejection requires a failure code")
	}
	if !balance.IsValid() || balance.IsNegative() {
		return invalid("result balance must be a valid non-negative amount")
	}
	t.status = StatusRejected
	t.failureCode = code
	t.resultBalance = &balance
	t.expiresAt = nil
	p := now.UTC()
	t.processedAt = &p
	t.touch(now)
	return nil
}

// Fail registra uma falha permanente de infraestrutura, para auditoria.
func (t *WagerTransaction) Fail(code FailureCode, now time.Time) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	if code == "" {
		return invalid("failure requires a failure code")
	}
	t.status = StatusFailed
	t.failureCode = code
	t.expiresAt = nil
	p := now.UTC()
	t.processedAt = &p
	t.touch(now)
	return nil
}

// WaitForReference move PENDING para PENDING_REFERENCE: a referência ainda
// não está disponível. expiresAt é o prazo (TTL) final de espera.
func (t *WagerTransaction) WaitForReference(expiresAt, nextAttemptAt, now time.Time) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	if t.status != StatusPending {
		return ErrInvalidTransition
	}
	if t.referenceExternalID == "" {
		return invalid("transaction has no reference to wait for")
	}
	e := expiresAt.UTC()
	t.status = StatusPendingReference
	t.expiresAt = &e
	t.nextAttemptAt = nextAttemptAt.UTC()
	t.attempts = 0
	t.touch(now)
	return nil
}

// RetryReference registra mais uma tentativa sem sucesso e agenda a próxima.
func (t *WagerTransaction) RetryReference(nextAttemptAt, now time.Time) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	if t.status != StatusPendingReference {
		return ErrInvalidTransition
	}
	t.attempts++
	t.nextAttemptAt = nextAttemptAt.UTC()
	t.touch(now)
	return nil
}

// Getters.
func (t *WagerTransaction) ID() id.ID                              { return t.id }
func (t *WagerTransaction) Origin() Origin                         { return t.origin }
func (t *WagerTransaction) ProviderID() string                     { return t.providerID }
func (t *WagerTransaction) ExternalTransactionID() string          { return t.externalID }
func (t *WagerTransaction) IdempotencyKey() string                 { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string                    { return t.payloadHash }
func (t *WagerTransaction) WalletID() id.ID                        { return t.walletID }
func (t *WagerTransaction) PlayerID() id.ID                        { return t.playerID }
func (t *WagerTransaction) RoundID() string                        { return t.roundID }
func (t *WagerTransaction) GameID() string                         { return t.gameID }
func (t *WagerTransaction) Kind() Kind                             { return t.kind }
func (t *WagerTransaction) Money() money.Money                     { return t.amount }
func (t *WagerTransaction) ReferenceExternalTransactionID() string { return t.referenceExternalID }
func (t *WagerTransaction) ReferenceTransactionID() id.ID          { return t.referenceID }
func (t *WagerTransaction) Status() Status                         { return t.status }
func (t *WagerTransaction) FailureCode() FailureCode               { return t.failureCode }
func (t *WagerTransaction) ReferenceAttempts() int                 { return t.attempts }
func (t *WagerTransaction) NextAttemptAt() time.Time               { return t.nextAttemptAt }
func (t *WagerTransaction) CreatedAt() time.Time                   { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time                   { return t.updatedAt }

// ResultBalance é o saldo observado no processamento (nil enquanto pendente).
func (t *WagerTransaction) ResultBalance() (money.Money, bool) {
	if t.resultBalance == nil {
		return money.Money{}, false
	}
	return *t.resultBalance, true
}

// ExpiresAt é o prazo de espera da referência (zero se não houver).
func (t *WagerTransaction) ExpiresAt() (time.Time, bool) {
	if t.expiresAt == nil {
		return time.Time{}, false
	}
	return *t.expiresAt, true
}

// ProcessedAt é o instante em que a operação chegou a um estado terminal.
func (t *WagerTransaction) ProcessedAt() (time.Time, bool) {
	if t.processedAt == nil {
		return time.Time{}, false
	}
	return *t.processedAt, true
}

// TransactionRecord é o retrato completo de uma transação, usado pela
// persistência para gravar e reidratar sem expor o estado interno.
type TransactionRecord struct {
	ID                             id.ID
	Origin                         Origin
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       id.ID
	PlayerID                       id.ID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         id.ID
	Status                         Status
	FailureCode                    FailureCode
	ResultBalance                  *money.Money
	Attempts                       int
	NextAttemptAt                  time.Time
	ExpiresAt                      *time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	ProcessedAt                    *time.Time
}

// Record devolve um retrato da transação.
func (t *WagerTransaction) Record() TransactionRecord {
	return TransactionRecord{
		ID: t.id, Origin: t.origin, ProviderID: t.providerID, ExternalTransactionID: t.externalID,
		IdempotencyKey: t.idempotencyKey, PayloadHash: t.payloadHash, WalletID: t.walletID,
		PlayerID: t.playerID, RoundID: t.roundID, GameID: t.gameID, Kind: t.kind, Money: t.amount,
		ReferenceExternalTransactionID: t.referenceExternalID, ReferenceTransactionID: t.referenceID,
		Status: t.status, FailureCode: t.failureCode, ResultBalance: t.resultBalance,
		Attempts: t.attempts, NextAttemptAt: t.nextAttemptAt, ExpiresAt: t.expiresAt,
		CreatedAt: t.createdAt, UpdatedAt: t.updatedAt, ProcessedAt: t.processedAt,
	}
}

// RehydrateTransaction reconstrói uma transação persistida. Não reaplica
// transições, movimentações nem eventos; apenas valida a consistência.
func RehydrateTransaction(r TransactionRecord) (*WagerTransaction, error) {
	if r.ID.IsZero() || r.WalletID.IsZero() || r.PlayerID.IsZero() {
		return nil, invalid("transaction requires id, walletId and playerId")
	}
	if !r.Kind.valid() || !r.Status.valid() {
		return nil, invalid("transaction has unknown kind or status")
	}
	if !r.Money.IsValid() {
		return nil, invalid("transaction money requires a valid currency")
	}
	switch r.Origin {
	case OriginInternal:
		if r.Kind != KindOpening {
			return nil, invalid("internal transactions must be OPENING")
		}
	case OriginExternal:
		if r.Kind == KindOpening || r.ProviderID == "" || r.ExternalTransactionID == "" ||
			r.IdempotencyKey == "" || r.PayloadHash == "" {
			return nil, invalid("external transaction is missing mandatory fields")
		}
	default:
		return nil, invalid("transaction has unknown origin %q", r.Origin)
	}
	if (r.Status == StatusRejected || r.Status == StatusFailed) && r.FailureCode == "" {
		return nil, invalid("rejected or failed transaction requires a failure code")
	}
	return &WagerTransaction{
		id: r.ID, origin: r.Origin, providerID: r.ProviderID, externalID: r.ExternalTransactionID,
		idempotencyKey: r.IdempotencyKey, payloadHash: r.PayloadHash, walletID: r.WalletID,
		playerID: r.PlayerID, roundID: r.RoundID, gameID: r.GameID, kind: r.Kind, amount: r.Money,
		referenceExternalID: r.ReferenceExternalTransactionID, referenceID: r.ReferenceTransactionID,
		status: r.Status, failureCode: r.FailureCode, resultBalance: r.ResultBalance,
		attempts: r.Attempts, nextAttemptAt: r.NextAttemptAt.UTC(), expiresAt: r.ExpiresAt,
		createdAt: r.CreatedAt.UTC(), updatedAt: r.UpdatedAt.UTC(), processedAt: r.ProcessedAt,
	}, nil
}
