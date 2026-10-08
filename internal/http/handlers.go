package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/domain/money"
	"github.com/monii/backend-challenge-go/internal/domain/wager"
)

const maxBody = 64 << 10

// decode lê um único objeto JSON estrito: sem campos desconhecidos (por
// exemplo, um provedor não consegue injetar "providerId"), sem lixo depois.
func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: invalid JSON body: %v", wagering.ErrInvalidInput, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: body must contain a single JSON object", wagering.ErrInvalidInput)
	}
	return nil
}

func pathID(r *http.Request, name string) (id.ID, error) {
	v, err := id.Parse(r.PathValue(name))
	if err != nil {
		return id.Nil, fmt.Errorf("%w: invalid %s", wagering.ErrInvalidInput, name)
	}
	return v, nil
}

// ------------------------------------------------------------ carteiras

type walletDTO struct {
	ID        id.ID       `json:"id"`
	PlayerID  id.ID       `json:"playerId"`
	Balance   money.Money `json:"balance"`
	Version   int64       `json:"version"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

func toWalletDTO(w *wager.Wallet) walletDTO {
	return walletDTO{w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt(), w.UpdatedAt()}
}

type openWalletRequest struct {
	PlayerID       id.ID       `json:"playerId"`
	InitialBalance money.Money `json:"initialBalance"`
}

func (a *API) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if err := decode(w, r, &req); err != nil {
		a.fail(w, r, err)
		return
	}
	wal, err := a.svc.OpenWallet(r.Context(), wagering.OpenWalletCommand{
		PlayerID: req.PlayerID, Currency: req.InitialBalance.Currency(), InitialBalance: req.InitialBalance.Amount(),
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+wal.ID().String())
	writeJSON(w, http.StatusCreated, toWalletDTO(wal))
}

func (a *API) getWallet(w http.ResponseWriter, r *http.Request) {
	walletID, err := pathID(r, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	wal, err := a.svc.GetWallet(r.Context(), walletID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletDTO(wal))
}

type ledgerEntryDTO struct {
	ID            id.ID           `json:"id"`
	TransactionID id.ID           `json:"transactionId"`
	Direction     wager.Direction `json:"direction"`
	Money         money.Money     `json:"money"`
	BalanceBefore money.Money     `json:"balanceBefore"`
	BalanceAfter  money.Money     `json:"balanceAfter"`
	CreatedAt     time.Time       `json:"createdAt"`
}

func (a *API) listLedger(w http.ResponseWriter, r *http.Request) {
	walletID, err := pathID(r, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 {
			a.fail(w, r, fmt.Errorf("%w: invalid limit", wagering.ErrInvalidInput))
			return
		}
	}
	page, err := a.svc.ListLedger(r.Context(), walletID, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	entries := make([]ledgerEntryDTO, 0, len(page.Entries))
	for _, e := range page.Entries {
		entries = append(entries, ledgerEntryDTO{e.ID(), e.TransactionID(), e.Direction(), e.Amount(),
			e.BalanceBefore(), e.BalanceAfter(), e.CreatedAt()})
	}
	out := map[string]any{"entries": entries}
	if page.NextCursor != "" {
		out["nextCursor"] = page.NextCursor
	}
	writeJSON(w, http.StatusOK, out)
}

type reconciliationDTO struct {
	WalletID          id.ID       `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"` // armazenado menos reconstruído
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
	WalletVersion     int64       `json:"walletVersion"`
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	walletID, err := pathID(r, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	rec, err := a.svc.Reconcile(r.Context(), walletID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if !rec.Consistent {
		a.m.divergences.Inc()
		a.log.Error("reconciliation divergence", slog.String("walletId", walletID.String()),
			slog.String("stored", rec.WalletBalance.Amount()), slog.String("calculated", rec.LedgerBalance.Amount()),
			slog.String("difference", rec.Difference.Amount()))
	}
	writeJSON(w, http.StatusOK, reconciliationDTO{
		WalletID: rec.WalletID, StoredBalance: rec.WalletBalance, CalculatedBalance: rec.LedgerBalance,
		Difference: rec.Difference, Consistent: rec.Consistent, CheckedEntries: rec.Entries,
		WalletVersion: rec.WalletVersion})
}

// ----------------------------------------------------- operações de aposta

type submitRequest struct {
	// ProviderID é opcional: a identidade vem do token; se informado, deve ser o mesmo.
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	PlayerID                       id.ID       `json:"playerId"`
	WalletID                       id.ID       `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
}

type transactionDTO struct {
	TransactionID                  id.ID        `json:"transactionId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	WalletID                       id.ID        `json:"walletId"`
	Kind                           wager.Kind   `json:"kind"`
	Status                         wager.Status `json:"status"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	Money                          money.Money  `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	Balance                        *money.Money `json:"balance,omitempty"`
	IdempotentReplay               *bool        `json:"idempotentReplay,omitempty"`
	CreatedAt                      time.Time    `json:"createdAt"`
	ProcessedAt                    *time.Time   `json:"processedAt,omitempty"`
}

func toTransactionDTO(t *wager.WagerTransaction) transactionDTO {
	d := transactionDTO{
		TransactionID: t.ID(), ExternalTransactionID: t.ExternalTransactionID(), WalletID: t.WalletID(),
		Kind: t.Kind(), Status: t.Status(), FailureCode: string(t.FailureCode()), Money: t.Money(),
		ReferenceExternalTransactionID: t.ReferenceExternalTransactionID(), CreatedAt: t.CreatedAt(),
	}
	if b, ok := t.ResultBalance(); ok {
		d.Balance = &b
	}
	if p, ok := t.ProcessedAt(); ok {
		d.ProcessedAt = &p
	}
	return d
}

func statusFor(s wager.Status) int {
	switch s {
	case wager.StatusProcessed:
		return http.StatusOK
	case wager.StatusPendingReference, wager.StatusPending:
		return http.StatusAccepted
	default: // REJECTED, FAILED
		return http.StatusUnprocessableEntity
	}
}

func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeProblem(w, r, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "header Idempotency-Key is required")
		return
	}
	var req submitRequest
	if err := decode(w, r, &req); err != nil {
		a.fail(w, r, err)
		return
	}
	if req.ProviderID != "" && req.ProviderID != ident.ProviderID {
		writeProblem(w, r, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId does not match the authenticated provider")
		return
	}
	out, err := a.svc.Submit(r.Context(), wagering.SubmitCommand{
		ProviderID:            ident.ProviderID, // vem do token, nunca do corpo
		ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: key,
		WalletID: req.WalletID, PlayerID: req.PlayerID, RoundID: req.RoundID, GameID: req.GameID,
		Kind: req.Kind, Amount: req.Money.Amount(), Currency: req.Money.Currency(),
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	t := out.Transaction
	if !out.IdempotentReplay {
		a.m.transactions.Inc(string(t.Kind()), string(t.Status()))
	}
	d := toTransactionDTO(t)
	bal, replay := out.Balance, out.IdempotentReplay
	d.Balance, d.IdempotentReplay = &bal, &replay
	writeJSON(w, statusFor(t.Status()), d)
}

// getTransaction lê por id interno. Transações de outro provedor (ou internas,
// como OPENING) respondem 404, igual a uma transação inexistente.
func (a *API) getTransaction(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	txID, err := pathID(r, "transactionId")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	t, err := a.svc.GetTransaction(r.Context(), txID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if t.ProviderID() != ident.ProviderID {
		a.fail(w, r, wagering.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionDTO(t))
}

// getProviderTransaction lê por (provedor, id externo). O provedor do caminho
// precisa ser o do token; a busca é sempre dentro dele.
func (a *API) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	if r.PathValue("providerId") != ident.ProviderID {
		writeProblem(w, r, http.StatusForbidden, "FORBIDDEN", "the client is not allowed to read this provider")
		return
	}
	t, err := a.svc.FindTransaction(r.Context(), ident.ProviderID, r.PathValue("externalId"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionDTO(t))
}
