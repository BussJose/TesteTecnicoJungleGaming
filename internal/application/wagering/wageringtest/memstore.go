// Package wageringtest contém um repositório em memória e uma suíte de
// cenários reutilizável. A suíte roda sobre a memória (rápido, a cada
// `go test`) e sobre o PostgreSQL real (teste de integração).
package wageringtest

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/outbox"
	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

type walletRow struct {
	id, player         id.ID
	balance            money.Money
	version            int64
	createdAt, updated time.Time
}

type lease struct {
	owner string
	until time.Time
}

type outboxRow struct {
	ev        wager.Event
	seq       int64
	attempts  int
	next      time.Time
	lockedBy  string
	lockedTil time.Time
	published bool
}

type inboxRow struct {
	hash      string
	completed bool
}

type state struct {
	wallets map[id.ID]walletRow
	txs     map[id.ID]wager.TransactionRecord
	ledger  map[id.ID][]wagering.LedgerRow
	outbox  []outboxRow
	inbox   map[string]inboxRow
	leases  map[id.ID]lease
	seq     int64
}

func (s *state) clone() *state {
	c := &state{
		wallets: make(map[id.ID]walletRow, len(s.wallets)),
		txs:     make(map[id.ID]wager.TransactionRecord, len(s.txs)),
		ledger:  make(map[id.ID][]wagering.LedgerRow, len(s.ledger)),
		outbox:  append([]outboxRow(nil), s.outbox...),
		inbox:   make(map[string]inboxRow, len(s.inbox)),
		leases:  make(map[id.ID]lease, len(s.leases)),
		seq:     s.seq,
	}
	for k, v := range s.wallets {
		c.wallets[k] = v
	}
	for k, v := range s.txs {
		c.txs[k] = v
	}
	for k, v := range s.ledger {
		c.ledger[k] = append([]wagering.LedgerRow(nil), v...)
	}
	for k, v := range s.leases {
		c.leases[k] = v
	}
	for k, v := range s.inbox {
		c.inbox[k] = v
	}
	return c
}

// MemStore é um UnitOfWork em memória. Serializa as unidades de trabalho com
// um mutex global e desfaz tudo (rollback) quando a função devolve erro.
// A concorrência real (linhas travadas, SKIP LOCKED) é validada no PostgreSQL.
type MemStore struct {
	mu sync.Mutex
	st *state
}

// NewMemStore cria um repositório vazio.
func NewMemStore() *MemStore {
	return &MemStore{st: &state{
		wallets: map[id.ID]walletRow{}, txs: map[id.ID]wager.TransactionRecord{},
		ledger: map[id.ID][]wagering.LedgerRow{}, leases: map[id.ID]lease{}, inbox: map[string]inboxRow{},
	}}
}

// Do implementa wagering.UnitOfWork.
func (m *MemStore) Do(ctx context.Context, fn func(ctx context.Context, r wagering.Repos) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := m.st.clone()
	st := m.st
	err := fn(ctx, wagering.Repos{Wallets: walletRepo{st}, Transactions: txRepo{st}, Ledger: ledgerRepo{st}, Outbox: outboxRepo{st}, Inbox: inboxRepo{st}})
	if err != nil {
		m.st = snap
	}
	return err
}

// Events devolve os tipos de evento gravados na outbox para a carteira.
func (m *MemStore) Events(_ context.Context, walletID id.ID) ([]wager.EventType, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []wager.EventType
	for _, e := range m.st.outbox {
		if e.ev.AggregateID() == walletID {
			out = append(out, e.ev.Type())
		}
	}
	return out, nil
}

type walletRepo struct{ st *state }
type txRepo struct{ st *state }
type ledgerRepo struct{ st *state }
type outboxRepo struct{ st *state }
type inboxRepo struct{ st *state }

// ---- WalletRepo

func (r walletRepo) Insert(_ context.Context, w *wager.Wallet) error {
	for _, o := range r.st.wallets {
		if o.player == w.PlayerID() && o.balance.Currency() == w.Currency() {
			return wagering.ErrWalletExists
		}
	}
	r.st.wallets[w.ID()] = walletRow{w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt(), w.UpdatedAt()}
	return nil
}

func (r walletRepo) load(walletID id.ID) (*wager.Wallet, error) {
	row, ok := r.st.wallets[walletID]
	if !ok {
		return nil, wagering.ErrWalletNotFound
	}
	return wager.RehydrateWallet(row.id, row.player, row.balance, row.version, row.createdAt, row.updated)
}

func (r walletRepo) GetForUpdate(_ context.Context, walletID id.ID) (*wager.Wallet, error) {
	return r.load(walletID)
}

func (r walletRepo) Get(_ context.Context, walletID id.ID) (*wager.Wallet, error) {
	return r.load(walletID)
}

func (r walletRepo) UpdateBalance(_ context.Context, w *wager.Wallet, expected int64) error {
	row, ok := r.st.wallets[w.ID()]
	if !ok {
		return wagering.ErrWalletNotFound
	}
	if row.version != expected {
		return wagering.ErrConcurrentUpdate
	}
	row.balance, row.version, row.updated = w.Balance(), w.Version(), w.UpdatedAt()
	r.st.wallets[w.ID()] = row
	return nil
}

// ---- TransactionRepo

func (r txRepo) Insert(_ context.Context, t *wager.WagerTransaction) error {
	rec := t.Record()
	for _, o := range r.st.txs {
		if rec.Origin == wager.OriginExternal && o.Origin == wager.OriginExternal &&
			(o.ProviderID == rec.ProviderID && o.IdempotencyKey == rec.IdempotencyKey ||
				(o.ProviderID == rec.ProviderID && o.ExternalTransactionID == rec.ExternalTransactionID)) {
			return wagering.ErrUniqueViolation
		}
	}
	r.st.txs[rec.ID] = rec
	return nil
}

func (r txRepo) Update(_ context.Context, t *wager.WagerTransaction) error {
	rec := t.Record()
	old, ok := r.st.txs[rec.ID]
	if !ok {
		return wagering.ErrNotFound
	}
	if old.Status == wager.StatusProcessed || old.Status == wager.StatusRejected || old.Status == wager.StatusFailed {
		return wager.ErrTerminalState // espelha o trigger do banco
	}
	r.st.txs[rec.ID] = rec
	delete(r.st.leases, rec.ID)
	return nil
}

func (r txRepo) build(rec wager.TransactionRecord, ok bool) (*wager.WagerTransaction, error) {
	if !ok {
		return nil, wagering.ErrNotFound
	}
	return wager.RehydrateTransaction(rec)
}

func (r txRepo) Get(_ context.Context, txID id.ID) (*wager.WagerTransaction, error) {
	rec, ok := r.st.txs[txID]
	return r.build(rec, ok)
}

func (r txRepo) find(match func(wager.TransactionRecord) bool) (*wager.WagerTransaction, error) {
	for _, rec := range r.st.txs {
		if match(rec) {
			return r.build(rec, true)
		}
	}
	return nil, wagering.ErrNotFound
}

func (r txRepo) FindByExternal(_ context.Context, provider, external string) (*wager.WagerTransaction, error) {
	return r.find(func(x wager.TransactionRecord) bool {
		return x.Origin == wager.OriginExternal && x.ProviderID == provider && x.ExternalTransactionID == external
	})
}

func (r txRepo) FindByIdempotencyKey(_ context.Context, provider, key string) (*wager.WagerTransaction, error) {
	return r.find(func(x wager.TransactionRecord) bool {
		return x.Origin == wager.OriginExternal && x.ProviderID == provider && x.IdempotencyKey == key
	})
}

func (r txRepo) FindProcessedReversal(_ context.Context, referenced id.ID) (*wager.WagerTransaction, error) {
	return r.find(func(x wager.TransactionRecord) bool {
		return x.Status == wager.StatusProcessed && x.ReferenceTransactionID == referenced &&
			(x.Kind == wager.KindRefund || x.Kind == wager.KindRollback)
	})
}

func (r txRepo) ClaimPendingReferences(_ context.Context, now time.Time, limit int, ttl time.Duration, owner string) ([]id.ID, error) {
	var due []wager.TransactionRecord
	for _, rec := range r.st.txs {
		if rec.Status != wager.StatusPendingReference || rec.NextAttemptAt.After(now) {
			continue
		}
		if l, ok := r.st.leases[rec.ID]; ok && l.until.After(now) {
			continue
		}
		due = append(due, rec)
	}
	sort.Slice(due, func(i, j int) bool { return due[i].NextAttemptAt.Before(due[j].NextAttemptAt) })
	if len(due) > limit {
		due = due[:limit]
	}
	ids := make([]id.ID, 0, len(due))
	for _, rec := range due {
		r.st.leases[rec.ID] = lease{owner: owner, until: now.Add(ttl)}
		ids = append(ids, rec.ID)
	}
	return ids, nil
}

func (r txRepo) WakeDependents(_ context.Context, walletID id.ID, provider, external string, now time.Time) error {
	for k, rec := range r.st.txs {
		if rec.Status == wager.StatusPendingReference && rec.WalletID == walletID &&
			rec.ProviderID == provider && rec.ReferenceExternalTransactionID == external &&
			rec.NextAttemptAt.After(now) {
			rec.NextAttemptAt = now.UTC()
			r.st.txs[k] = rec
		}
	}
	return nil
}

// ---- LedgerRepo

func (r ledgerRepo) Insert(_ context.Context, e *wager.LedgerEntry) error {
	r.st.seq++
	r.st.ledger[e.WalletID()] = append(r.st.ledger[e.WalletID()], wagering.LedgerRow{Seq: r.st.seq, Entry: e})
	return nil
}

func (r ledgerRepo) List(_ context.Context, walletID id.ID, after int64, limit int) ([]wagering.LedgerRow, error) {
	var out []wagering.LedgerRow
	for _, row := range r.st.ledger[walletID] {
		if row.Seq > after && len(out) < limit {
			out = append(out, row)
		}
	}
	return out, nil
}

func (r ledgerRepo) Summarize(_ context.Context, walletID id.ID) (wagering.LedgerSummary, error) {
	w, ok := r.st.wallets[walletID]
	if !ok {
		return wagering.LedgerSummary{}, wagering.ErrWalletNotFound
	}
	sum, _ := money.Zero(w.balance.Currency())
	rows := r.st.ledger[walletID]
	for _, row := range rows {
		var err error
		if row.Entry.Direction() == wager.DirectionCredit {
			sum, err = sum.Add(row.Entry.Amount())
		} else {
			sum, err = sum.Sub(row.Entry.Amount())
		}
		if err != nil {
			return wagering.LedgerSummary{}, err
		}
	}
	return wagering.LedgerSummary{WalletBalance: w.balance, WalletVersion: w.version, LedgerBalance: sum, Entries: int64(len(rows))}, nil
}

// ---- OutboxRepo

func (r outboxRepo) Insert(_ context.Context, ev wager.Event) error {
	r.st.seq++
	r.st.outbox = append(r.st.outbox, outboxRow{ev: ev, seq: r.st.seq, next: ev.OccurredAt()})
	return nil
}

// ---- InboxRepo

func (r inboxRepo) Begin(_ context.Context, consumer, messageID, hash string, _ time.Time) (bool, error) {
	k := consumer + "\x00" + messageID
	if row, ok := r.st.inbox[k]; ok {
		if row.hash != hash {
			return false, wagering.ErrInboxConflict
		}
		return false, nil
	}
	r.st.inbox[k] = inboxRow{hash: hash}
	return true, nil
}

func (r inboxRepo) Complete(_ context.Context, consumer, messageID string, _ time.Time) error {
	k := consumer + "\x00" + messageID
	row := r.st.inbox[k]
	row.completed = true
	r.st.inbox[k] = row
	return nil
}

// ---- outbox.Store (a publicação lê a outbox fora de uma unidade de trabalho)

var _ outbox.Store = (*MemStore)(nil)

func (m *MemStore) Claim(_ context.Context, now time.Time, limit int, lease time.Duration, owner string) ([]outbox.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	blocked := map[id.ID]bool{} // agregados com evento anterior não publicado
	var out []outbox.Record
	for i := range m.st.outbox {
		row := &m.st.outbox[i]
		if row.published {
			continue
		}
		agg := row.ev.AggregateID()
		wasBlocked := blocked[agg]
		blocked[agg] = true
		if wasBlocked || len(out) >= limit || row.next.After(now) || row.lockedTil.After(now) {
			continue
		}
		row.lockedBy, row.lockedTil = owner, now.Add(lease)
		out = append(out, outbox.Record{
			EventID: row.ev.ID(), AggregateID: agg, Type: row.ev.Type(), Version: row.ev.Version(),
			CorrelationID: row.ev.CorrelationID(), CausationID: row.ev.CausationID(),
			Payload: row.ev.Payload(), OccurredAt: row.ev.OccurredAt(), Attempts: row.attempts,
		})
	}
	return out, nil
}

func (m *MemStore) MarkPublished(_ context.Context, eventID id.ID, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.st.outbox {
		if m.st.outbox[i].ev.ID() == eventID {
			m.st.outbox[i].published = true
			m.st.outbox[i].lockedBy, m.st.outbox[i].lockedTil = "", time.Time{}
		}
	}
	return nil
}

func (m *MemStore) Release(_ context.Context, eventID id.ID, next time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.st.outbox {
		if row := &m.st.outbox[i]; row.ev.ID() == eventID && !row.published {
			row.attempts++
			row.next = next
			row.lockedBy, row.lockedTil = "", time.Time{}
		}
	}
	return nil
}

// PendingOutbox devolve quantos eventos da carteira ainda não foram publicados.
func (m *MemStore) PendingOutbox(walletID id.ID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, row := range m.st.outbox {
		if row.ev.AggregateID() == walletID && !row.published {
			n++
		}
	}
	return n
}
