package wagering

import (
	"context"
	"time"

	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

// WalletRepo persiste carteiras.
type WalletRepo interface {
	// Insert grava uma carteira nova. ErrWalletExists se (playerId, currency) já existe.
	Insert(ctx context.Context, w *wager.Wallet) error
	// GetForUpdate lê a carteira travando a linha até o fim da unidade de trabalho.
	// ErrWalletNotFound se não existir.
	GetForUpdate(ctx context.Context, walletID id.ID) (*wager.Wallet, error)
	// Get lê sem travar. ErrWalletNotFound se não existir.
	Get(ctx context.Context, walletID id.ID) (*wager.Wallet, error)
	// UpdateBalance grava saldo/versão só se a versão atual no banco for
	// expectedVersion. Caso contrário, ErrConcurrentUpdate.
	UpdateBalance(ctx context.Context, w *wager.Wallet, expectedVersion int64) error
}

// TransactionRepo persiste transações.
type TransactionRepo interface {
	Insert(ctx context.Context, t *wager.WagerTransaction) error
	// Update grava o novo estado de uma transação existente e libera o lease.
	Update(ctx context.Context, t *wager.WagerTransaction) error
	Get(ctx context.Context, txID id.ID) (*wager.WagerTransaction, error)
	FindByExternal(ctx context.Context, providerID, externalID string) (*wager.WagerTransaction, error)
	FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wager.WagerTransaction, error)
	// FindProcessedReversal devolve a reversão PROCESSED da transação referenciada.
	FindProcessedReversal(ctx context.Context, referencedID id.ID) (*wager.WagerTransaction, error)
	// ClaimPendingReferences reserva (lease) até limit transações PENDING_REFERENCE
	// vencidas, sem bloquear outras instâncias (SKIP LOCKED).
	ClaimPendingReferences(ctx context.Context, now time.Time, limit int, lease time.Duration, owner string) ([]id.ID, error)
	// WakeDependents antecipa a próxima tentativa das transações da mesma
	// carteira que aguardam (providerId, externalId).
	WakeDependents(ctx context.Context, walletID id.ID, providerID, externalID string, now time.Time) error
}

// LedgerRow é um lançamento com sua posição (seq) no ledger.
type LedgerRow struct {
	Seq   int64
	Entry *wager.LedgerEntry
}

// LedgerSummary é um retrato consistente de carteira × ledger.
type LedgerSummary struct {
	WalletBalance money.Money
	WalletVersion int64
	LedgerBalance money.Money // créditos − débitos
	Entries       int64
}

// LedgerRepo persiste o ledger (somente inserção).
type LedgerRepo interface {
	Insert(ctx context.Context, e *wager.LedgerEntry) error
	List(ctx context.Context, walletID id.ID, afterSeq int64, limit int) ([]LedgerRow, error)
	// Summarize lê saldo da carteira e soma do ledger numa única consulta.
	Summarize(ctx context.Context, walletID id.ID) (LedgerSummary, error)
}

// OutboxRepo grava eventos para publicação posterior (mesma transação do negócio).
type OutboxRepo interface {
	Insert(ctx context.Context, ev wager.Event) error
}

// InboxRepo deduplica mensagens consumidas (inbox transacional).
type InboxRepo interface {
	// Begin registra a mensagem. inserted=false (sem erro) significa que ela já
	// foi processada antes: o consumidor só precisa confirmá-la. Se o mesmo
	// messageId voltar com outro conteúdo, devolve ErrInboxConflict.
	Begin(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (inserted bool, err error)
	// Complete marca a mensagem como concluída (na mesma transação do negócio).
	Complete(ctx context.Context, consumer, messageID string, now time.Time) error
}

// Repos agrupa os repositórios de uma unidade de trabalho.
type Repos struct {
	Inbox        InboxRepo
	Wallets      WalletRepo
	Transactions TransactionRepo
	Ledger       LedgerRepo
	Outbox       OutboxRepo
}

// UnitOfWork executa fn numa transação: commit se fn devolver nil, rollback caso contrário.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, r Repos) error) error
}
