// Package wager contém o domínio financeiro: carteira, transações de aposta,
// lançamentos do ledger e eventos de integração. É independente de HTTP,
// SQS, Fx e de bibliotecas de persistência.
package wager

// Kind é o tipo de uma transação.
type Kind string

const (
	KindOpening  Kind = "OPENING" // reservado: abertura interna de carteira
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseExternalKind valida o tipo recebido por HTTP ou SQS. OPENING é rejeitado.
func ParseExternalKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	case KindOpening:
		return "", invalid("kind OPENING is reserved for internal wallet opening")
	}
	return "", invalid("unknown kind %q", s)
}

func (k Kind) valid() bool {
	switch k {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	}
	return false
}

// IsReversal informa se o tipo desfaz uma transação anterior.
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// AllowsReference informa se o tipo pode carregar referenceExternalTransactionId.
func (k Kind) AllowsReference() bool { return k == KindWin || k.IsReversal() }

// Status é o estado de uma transação.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func (s Status) valid() bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

// IsTerminal informa se o estado não admite novas transições.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Direction é o sentido de um lançamento no ledger.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

func (d Direction) valid() bool { return d == DirectionDebit || d == DirectionCredit }

// Opposite devolve o sentido contrário.
func (d Direction) Opposite() Direction {
	if d == DirectionDebit {
		return DirectionCredit
	}
	return DirectionDebit
}

// Origin distingue operações externas (provedores) das internas (abertura).
type Origin string

const (
	OriginExternal Origin = "EXTERNAL"
	OriginInternal Origin = "INTERNAL"
)

// FailureCode é um código estável de rejeição ou falha.
type FailureCode string

// Códigos de rejeição (resultados definitivos, persistidos e auditáveis).
// Entradas inválidas (corrigíveis) não geram transação: viram erro 400 e
// não têm FailureCode.
const (
	// FailInsufficientFunds: BET sem saldo suficiente.
	FailInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
	// FailReversalInsufficientFunds: reversão que precisaria debitar mais que o saldo.
	FailReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	// FailReferenceNotFound: a referência não chegou até o fim do prazo/tentativas.
	FailReferenceNotFound FailureCode = "REFERENCE_NOT_FOUND"
	// FailReferenceNotProcessed: a referência existe, mas terminou sem sucesso
	// (REJECTED/FAILED) ou continuou pendente até o fim do prazo.
	FailReferenceNotProcessed FailureCode = "REFERENCE_NOT_PROCESSED"
	// FailReferenceMismatch: provedor, jogador, carteira, moeda, rodada ou valor divergem.
	FailReferenceMismatch FailureCode = "REFERENCE_MISMATCH"
	// FailReferenceInvalidKind: o tipo da referência não é aceito por esta operação.
	FailReferenceInvalidKind FailureCode = "REFERENCE_INVALID_KIND"
	// FailAlreadyReversed: a referência já recebeu uma reversão bem-sucedida.
	FailAlreadyReversed FailureCode = "ALREADY_REVERSED"
	// FailWalletMismatch: a carteira não pertence ao jogador ou a moeda difere.
	FailWalletMismatch FailureCode = "WALLET_MISMATCH"
	// FailInfrastructure: falha permanente de infraestrutura (estado FAILED).
	FailInfrastructure FailureCode = "INFRASTRUCTURE_FAILURE"
)
