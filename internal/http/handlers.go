package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	WalletID      id.ID       `json:"walletId"`
	WalletBalance money.Money `json:"walletBalance"`
	LedgerBalance money.Money `json:"ledgerBalance"`
	WalletVersion int64       `json:"walletVersion"`
	LedgerEntries int64       `json:"ledgerEntries"`
	Consistent    bool        `json:"consistent"`
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
	writeJSON(w, http.StatusOK, reconciliationDTO{rec.WalletID, rec.WalletBalance, rec.LedgerBalance,
		rec.WalletVersion, rec.Entries, rec.Consistent})
}

// ----------------------------------------------------- operações de aposta

type submitRequest struct {
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

func (a *API) getTransaction(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	// A busca é sempre dentro do provedor autenticado: um provedor nunca
	// enxerga transações de outro.
	t, err := a.svc.FindTransaction(r.Context(), ident.ProviderID, r.PathValue("externalId"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionDTO(t))
}
