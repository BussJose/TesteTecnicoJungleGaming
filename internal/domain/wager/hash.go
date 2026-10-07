package wager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
)

// computePayloadHash calcula o hash de idempotência de uma operação externa.
//
// Algoritmo: SHA-256 (hex minúsculo) do JSON canônico (chaves em ordem
// alfabética, sem espaços) com estes campos de negócio:
//
//	providerId, externalTransactionId, playerId, walletId, roundId, gameId,
//	kind, amount, currency e, só se informada, referenceExternalTransactionId.
//
// Ficam de fora a chave de idempotência e qualquer metadado de transporte
// (headers, messageId, correlationId). Os valores são normalizados antes do
// hash: UUIDs em minúsculas canônicas e dinheiro com 2 casas ("25", "25.5" e
// "25.00" geram o mesmo hash). HTTP e SQS usam esta mesma função.
func computePayloadHash(providerID, externalID string, playerID, walletID id.ID,
	roundID, gameID string, kind Kind, amount money.Money, reference string) string {
	canonical := map[string]string{
		"providerId":            providerID,
		"externalTransactionId": externalID,
		"playerId":              playerID.String(),
		"walletId":              walletID.String(),
		"roundId":               roundID,
		"gameId":                gameID,
		"kind":                  string(kind),
		"amount":                amount.Amount(),
		"currency":              amount.Currency(),
	}
	if reference != "" {
		canonical["referenceExternalTransactionId"] = reference
	}
	b, _ := json.Marshal(canonical) // map[string]string: chaves ordenadas, nunca falha
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
