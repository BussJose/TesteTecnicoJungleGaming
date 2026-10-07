// Package wagering orquestra os casos de uso financeiros: abrir carteira,
// processar operações de aposta (com idempotência e referências pendentes),
// consultar e conciliar. Depende só de portas (interfaces) e do domínio.
package wagering

import (
	"errors"

	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

var (
	// ErrInvalidInput: entrada corrigível (HTTP 400). Não gera transação.
	ErrInvalidInput = wager.ErrInvalid
	// ErrNotFound: registro inexistente (usado pelos repositórios).
	ErrNotFound = errors.New("wagering: not found")
	// ErrWalletNotFound: a carteira informada não existe.
	ErrWalletNotFound = errors.New("wagering: wallet not found")
	// ErrWalletExists: já existe carteira para (playerId, currency).
	ErrWalletExists = errors.New("wagering: wallet already exists")
	// ErrIdempotencyConflict: mesma chave/operação com conteúdo diferente.
	ErrIdempotencyConflict = errors.New("wagering: idempotency key reused with different content")
	// ErrUniqueViolation: violação de unicidade no banco (o serviço tenta de novo).
	ErrUniqueViolation = errors.New("wagering: unique violation")
	// ErrConcurrentUpdate: a versão da carteira mudou entre leitura e escrita.
	ErrConcurrentUpdate = errors.New("wagering: concurrent wallet update")
	// ErrTransient: falha temporária (deadlock, timeout, conexão). Pode repetir.
	ErrTransient = errors.New("wagering: transient failure")
)
