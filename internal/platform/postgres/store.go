package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

// txStarter é o pedaço do *pgxpool.Pool de que o Store precisa.
type txStarter interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// Store implementa wagering.UnitOfWork sobre o PostgreSQL.
type Store struct{ db txStarter }

// NewStore cria o Store. Normalmente recebe o *pgxpool.Pool.
func NewStore(db txStarter) *Store { return &Store{db: db} }

var _ wagering.UnitOfWork = (*Store)(nil)

// Do abre uma transação READ COMMITTED, executa fn e faz commit; se fn
// devolver erro, faz rollback. A correção sob concorrência vem do
// SELECT ... FOR UPDATE da carteira (feito pelo serviço), das restrições
// únicas e do UPDATE com checagem de versão, não do nível de isolamento.
func (s *Store) Do(ctx context.Context, fn func(ctx context.Context, r wagering.Repos) error) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return mapErr(err)
	}
	q := querier{tx}
	err = fn(ctx, wagering.Repos{
		Wallets: walletRepo{q}, Transactions: txRepo{q}, Ledger: ledgerRepo{q}, Outbox: outboxRepo{q},
	})
	if err != nil {
		// O rollback não pode ser cancelado junto com a requisição.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return mapErr(err)
	}
	return nil
}

type querier struct{ tx pgx.Tx }

// mapErr traduz erros do PostgreSQL para os erros da aplicação.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "23505": // unique_violation
			if pg.ConstraintName == "wallets_player_currency_uq" {
				return wagering.ErrWalletExists
			}
			return fmt.Errorf("%w: %s", wagering.ErrUniqueViolation, pg.ConstraintName)
		case "40001", "40P01", "55P03", "57014", "57P01": // serialização, deadlock, lock, cancelamento, shutdown
			return fmt.Errorf("%w: %w", wagering.ErrTransient, err)
		}
		if len(pg.Code) >= 2 && pg.Code[:2] == "08" { // falha de conexão
			return fmt.Errorf("%w: %w", wagering.ErrTransient, err)
		}
		return err
	}
	return fmt.Errorf("%w: %w", wagering.ErrTransient, err) // rede, EOF etc.
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullID(v id.ID) *string {
	if v.IsZero() {
		return nil
	}
	s := v.String()
	return &s
}

// ============================================================ carteiras

type walletRepo struct{ q querier }

const walletCols = `id::text, player_id::text, currency::text, balance_minor, version, created_at, updated_at`

func (r walletRepo) Insert(ctx context.Context, w *wager.Wallet) error {
	_, err := r.q.tx.Exec(ctx, `
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ($1::text::uuid, $2::text::uuid, $3::text, $4, $5, $6, $7)`,
		w.ID().String(), w.PlayerID().String(), w.Currency(), w.Balance().Minor(), w.Version(),
		w.CreatedAt(), w.UpdatedAt())
	return mapErr(err)
}

func (r walletRepo) load(ctx context.Context, walletID id.ID, suffix string) (*wager.Wallet, error) {
	var wid, pid, cur string
	var minor, version int64
	var created, updated time.Time
	err := r.q.tx.QueryRow(ctx, `SELECT `+walletCols+` FROM wallets WHERE id = $1::text::uuid`+suffix,
		walletID.String()).Scan(&wid, &pid, &cur, &minor, &version, &created, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, wagering.ErrWalletNotFound
	}
	if err != nil {
		return nil, mapErr(err)
	}
	return scanWallet(wid, pid, cur, minor, version, created, updated)
}

func scanWallet(wid, pid, cur string, minor, version int64, created, updated time.Time) (*wager.Wallet, error) {
	walletID, err := id.Parse(wid)
	if err != nil {
		return nil, err
	}
	playerID, err := id.Parse(pid)
	if err != nil {
		return nil, err
	}
	bal, err := money.New(minor, cur)
	if err != nil {
		return nil, err
	}
	return wager.RehydrateWallet(walletID, playerID, bal, version, created, updated)
}

func (r walletRepo) GetForUpdate(ctx context.Context, walletID id.ID) (*wager.Wallet, error) {
	return r.load(ctx, walletID, " FOR UPDATE")
}

func (r walletRepo) Get(ctx context.Context, walletID id.ID) (*wager.Wallet, error) {
	return r.load(ctx, walletID, "")
}

func (r walletRepo) UpdateBalance(ctx context.Context, w *wager.Wallet, expectedVersion int64) error {
	tag, err := r.q.tx.Exec(ctx, `
UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4
WHERE id = $1::text::uuid AND version = $5`,
		w.ID().String(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() != 1 {
		return wagering.ErrConcurrentUpdate
	}
	return nil
}

// ========================================================== transações

type txRepo struct{ q querier }

// A moeda do saldo-resultado vem da carteira (a da transação pode diferir
// quando a rejeição é WALLET_MISMATCH).
const txSelect = `
SELECT t.id::text, t.origin, t.provider_id, t.external_transaction_id, t.idempotency_key, t.payload_hash,
       t.wallet_id::text, t.player_id::text, t.round_id, t.game_id, t.kind, t.amount_minor, t.currency::text,
       t.reference_external_transaction_id, t.reference_transaction_id::text, t.status, t.failure_code,
       t.result_balance_minor, w.currency::text, t.attempts, t.next_attempt_at, t.expires_at,
       t.created_at, t.updated_at, t.processed_at
FROM wager_transactions t JOIN wallets w ON w.id = t.wallet_id `

type scanner interface{ Scan(dest ...any) error }

func scanTx(row scanner) (*wager.WagerTransaction, error) {
	var (
		idS, origin, walletS, playerS, kind, cur, status, walletCur string
		provider, external, idem, hash, round, game                 *string
		refExt, refID, failure                                      *string
		amount                                                      int64
		resultMinor                                                 *int64
		attempts                                                    int
		next, created, updated                                      time.Time
		expires, processed                                          *time.Time
	)
	err := row.Scan(&idS, &origin, &provider, &external, &idem, &hash, &walletS, &playerS, &round, &game,
		&kind, &amount, &cur, &refExt, &refID, &status, &failure, &resultMinor, &walletCur, &attempts,
		&next, &expires, &created, &updated, &processed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, wagering.ErrNotFound
	}
	if err != nil {
		return nil, mapErr(err)
	}
	val := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	rec := wager.TransactionRecord{
		Origin: wager.Origin(origin), ProviderID: val(provider), ExternalTransactionID: val(external),
		IdempotencyKey: val(idem), PayloadHash: val(hash), RoundID: val(round), GameID: val(game),
		Kind: wager.Kind(kind), ReferenceExternalTransactionID: val(refExt), Status: wager.Status(status),
		FailureCode: wager.FailureCode(val(failure)), Attempts: attempts,
		NextAttemptAt: next, ExpiresAt: expires, CreatedAt: created, UpdatedAt: updated, ProcessedAt: processed,
	}
	if rec.ID, err = id.Parse(idS); err != nil {
		return nil, err
	}
	if rec.WalletID, err = id.Parse(walletS); err != nil {
		return nil, err
	}
	if rec.PlayerID, err = id.Parse(playerS); err != nil {
		return nil, err
	}
	if refID != nil {
		if rec.ReferenceTransactionID, err = id.Parse(*refID); err != nil {
			return nil, err
		}
	}
	if rec.Money, err = money.New(amount, cur); err != nil {
		return nil, err
	}
	if resultMinor != nil {
		m, err := money.New(*resultMinor, walletCur)
		if err != nil {
			return nil, err
		}
		rec.ResultBalance = &m
	}
	return wager.RehydrateTransaction(rec)
}

func (r txRepo) Insert(ctx context.Context, t *wager.WagerTransaction) error {
	rec := t.Record()
	var result *int64
	if rec.ResultBalance != nil {
		v := rec.ResultBalance.Minor()
		result = &v
	}
	_, err := r.q.tx.Exec(ctx, `
INSERT INTO wager_transactions (
    id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
    wallet_id, player_id, round_id, game_id, kind, amount_minor, currency,
    reference_external_transaction_id, reference_transaction_id, status, failure_code,
    result_balance_minor, attempts, next_attempt_at, expires_at, created_at, updated_at, processed_at)
VALUES ($1::text::uuid, $2, $3, $4, $5, $6,
        $7::text::uuid, $8::text::uuid, $9, $10, $11, $12, $13::text,
        $14, $15::text::uuid, $16, $17,
        $18, $19, $20, $21, $22, $23, $24)`,
		rec.ID.String(), string(rec.Origin), nullStr(rec.ProviderID), nullStr(rec.ExternalTransactionID),
		nullStr(rec.IdempotencyKey), nullStr(rec.PayloadHash),
		rec.WalletID.String(), rec.PlayerID.String(), nullStr(rec.RoundID), nullStr(rec.GameID),
		string(rec.Kind), rec.Money.Minor(), rec.Money.Currency(),
		nullStr(rec.ReferenceExternalTransactionID), nullID(rec.ReferenceTransactionID),
		string(rec.Status), nullStr(string(rec.FailureCode)),
		result, rec.Attempts, rec.NextAttemptAt, rec.ExpiresAt, rec.CreatedAt, rec.UpdatedAt, rec.ProcessedAt)
	return mapErr(err)
}

func (r txRepo) Update(ctx context.Context, t *wager.WagerTransaction) error {
	rec := t.Record()
	var result *int64
	if rec.ResultBalance != nil {
		v := rec.ResultBalance.Minor()
		result = &v
	}
	tag, err := r.q.tx.Exec(ctx, `
UPDATE wager_transactions SET
    status = $2, failure_code = $3, reference_transaction_id = $4::text::uuid,
    result_balance_minor = $5, attempts = $6, next_attempt_at = $7, expires_at = $8,
    updated_at = $9, processed_at = $10, locked_by = NULL, locked_until = NULL
WHERE id = $1::text::uuid`,
		rec.ID.String(), string(rec.Status), nullStr(string(rec.FailureCode)), nullID(rec.ReferenceTransactionID),
		result, rec.Attempts, rec.NextAttemptAt, rec.ExpiresAt, rec.UpdatedAt, rec.ProcessedAt)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() != 1 {
		return wagering.ErrNotFound
	}
	return nil
}

func (r txRepo) Get(ctx context.Context, txID id.ID) (*wager.WagerTransaction, error) {
	return scanTx(r.q.tx.QueryRow(ctx, txSelect+`WHERE t.id = $1::text::uuid`, txID.String()))
}

func (r txRepo) FindByExternal(ctx context.Context, providerID, externalID string) (*wager.WagerTransaction, error) {
	return scanTx(r.q.tx.QueryRow(ctx,
		txSelect+`WHERE t.provider_id = $1 AND t.external_transaction_id = $2`, providerID, externalID))
}

func (r txRepo) FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wager.WagerTransaction, error) {
	return scanTx(r.q.tx.QueryRow(ctx,
		txSelect+`WHERE t.provider_id = $1 AND t.idempotency_key = $2`, providerID, key))
}

func (r txRepo) FindProcessedReversal(ctx context.Context, referenced id.ID) (*wager.WagerTransaction, error) {
	return scanTx(r.q.tx.QueryRow(ctx, txSelect+`
WHERE t.reference_transaction_id = $1::text::uuid AND t.kind IN ('REFUND','ROLLBACK') AND t.status = 'PROCESSED'
LIMIT 1`, referenced.String()))
}

// ClaimPendingReferences reserva transações vencidas. FOR UPDATE SKIP LOCKED
// faz instâncias concorrentes pegarem conjuntos diferentes, sem esperar umas
// pelas outras; o lease (locked_until) cobre o caso da instância cair no meio.
func (r txRepo) ClaimPendingReferences(ctx context.Context, now time.Time, limit int, lease time.Duration, owner string) ([]id.ID, error) {
	rows, err := r.q.tx.Query(ctx, `
UPDATE wager_transactions SET locked_by = $1, locked_until = $2
WHERE id IN (
    SELECT id FROM wager_transactions
    WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $3
      AND (locked_until IS NULL OR locked_until <= $3)
    ORDER BY next_attempt_at
    LIMIT $4
    FOR UPDATE SKIP LOCKED)
RETURNING id::text`, owner, now.Add(lease), now, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []id.ID
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, mapErr(err)
		}
		v, err := id.Parse(s)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, mapErr(rows.Err())
}

func (r txRepo) WakeDependents(ctx context.Context, walletID id.ID, providerID, externalID string, now time.Time) error {
	_, err := r.q.tx.Exec(ctx, `
UPDATE wager_transactions SET next_attempt_at = $4
WHERE wallet_id = $1::text::uuid AND provider_id = $2 AND reference_external_transaction_id = $3
  AND status = 'PENDING_REFERENCE' AND next_attempt_at > $4`,
		walletID.String(), providerID, externalID, now)
	return mapErr(err)
}

// ============================================================== ledger

type ledgerRepo struct{ q querier }

func (r ledgerRepo) Insert(ctx context.Context, e *wager.LedgerEntry) error {
	_, err := r.q.tx.Exec(ctx, `
INSERT INTO wallet_ledger_entries
    (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, created_at)
VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4, $5, $6, $7, $8)`,
		e.ID().String(), e.WalletID().String(), e.TransactionID().String(), string(e.Direction()),
		e.Amount().Minor(), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	return mapErr(err)
}

func (r ledgerRepo) List(ctx context.Context, walletID id.ID, afterSeq int64, limit int) ([]wagering.LedgerRow, error) {
	rows, err := r.q.tx.Query(ctx, `
SELECT l.seq, l.id::text, l.transaction_id::text, l.direction, l.amount_minor,
       l.balance_before_minor, l.balance_after_minor, l.created_at, w.currency::text
FROM wallet_ledger_entries l JOIN wallets w ON w.id = l.wallet_id
WHERE l.wallet_id = $1::text::uuid AND l.seq > $2
ORDER BY l.seq
LIMIT $3`, walletID.String(), afterSeq, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []wagering.LedgerRow
	for rows.Next() {
		var (
			seq, amount, before, after int64
			idS, txS, dir, cur         string
			created                    time.Time
		)
		if err := rows.Scan(&seq, &idS, &txS, &dir, &amount, &before, &after, &created, &cur); err != nil {
			return nil, mapErr(err)
		}
		e, err := rehydrateEntry(idS, walletID, txS, dir, amount, before, after, cur, created)
		if err != nil {
			return nil, err
		}
		out = append(out, wagering.LedgerRow{Seq: seq, Entry: e})
	}
	return out, mapErr(rows.Err())
}

func rehydrateEntry(idS string, walletID id.ID, txS, dir string, amount, before, after int64, cur string, created time.Time) (*wager.LedgerEntry, error) {
	eid, err := id.Parse(idS)
	if err != nil {
		return nil, err
	}
	tid, err := id.Parse(txS)
	if err != nil {
		return nil, err
	}
	a, err := money.New(amount, cur)
	if err != nil {
		return nil, err
	}
	b, err := money.New(before, cur)
	if err != nil {
		return nil, err
	}
	c, err := money.New(after, cur)
	if err != nil {
		return nil, err
	}
	return wager.RehydrateLedgerEntry(eid, walletID, tid, wager.Direction(dir), a, b, c, created)
}

// Summarize lê saldo e soma do ledger numa única consulta (um único
// snapshot), para a conciliação não ver um estado "no meio" de outra escrita.
func (r ledgerRepo) Summarize(ctx context.Context, walletID id.ID) (wagering.LedgerSummary, error) {
	var balance, version, ledger, entries int64
	var cur string
	err := r.q.tx.QueryRow(ctx, `
SELECT w.balance_minor, w.version, w.currency::text,
       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint,
       COUNT(l.id)::bigint
FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
WHERE w.id = $1::text::uuid
GROUP BY w.id`, walletID.String()).Scan(&balance, &version, &cur, &ledger, &entries)
	if errors.Is(err, pgx.ErrNoRows) {
		return wagering.LedgerSummary{}, wagering.ErrWalletNotFound
	}
	if err != nil {
		return wagering.LedgerSummary{}, mapErr(err)
	}
	wb, err := money.New(balance, cur)
	if err != nil {
		return wagering.LedgerSummary{}, err
	}
	lb, err := money.New(ledger, cur)
	if err != nil {
		return wagering.LedgerSummary{}, err
	}
	return wagering.LedgerSummary{WalletBalance: wb, WalletVersion: version, LedgerBalance: lb, Entries: entries}, nil
}

// ============================================================== outbox

type outboxRepo struct{ q querier }

func (r outboxRepo) Insert(ctx context.Context, ev wager.Event) error {
	_, err := r.q.tx.Exec(ctx, `
INSERT INTO outbox_events
    (event_id, aggregate_id, event_type, version, correlation_id, causation_id, payload, occurred_at)
VALUES ($1::text::uuid, $2::text::uuid, $3, $4, $5, $6, $7::text::jsonb, $8)`,
		ev.ID().String(), ev.AggregateID().String(), string(ev.Type()), ev.Version(),
		ev.CorrelationID(), nullStr(ev.CausationID()), string(ev.Payload()), ev.OccurredAt())
	return mapErr(err)
}

// Events lista os tipos de evento da outbox de uma carteira, na ordem de
// gravação (usado em testes).
func (s *Store) Events(ctx context.Context, walletID id.ID) ([]wager.EventType, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, mapErr(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `SELECT event_type FROM outbox_events WHERE aggregate_id = $1::text::uuid ORDER BY seq`,
		walletID.String())
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []wager.EventType
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, mapErr(err)
		}
		out = append(out, wager.EventType(t))
	}
	return out, mapErr(rows.Err())
}
