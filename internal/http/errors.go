package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/platform/auth"
)

type errorBody struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"requestId,omitempty"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message, b.Error.RequestID = code, msg, requestID(r.Context())
	writeJSON(w, status, b)
}

// fail traduz um erro da aplicação para a resposta HTTP. Erros inesperados
// são registrados no log e nunca expõem detalhes internos ao cliente.
func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, wagering.ErrInvalidInput):
		writeProblem(w, r, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, wagering.ErrWalletNotFound):
		writeProblem(w, r, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found")
	case errors.Is(err, wagering.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found")
	case errors.Is(err, wagering.ErrWalletExists):
		writeProblem(w, r, http.StatusConflict, "WALLET_ALREADY_EXISTS", "a wallet for this player and currency already exists")
	case errors.Is(err, wagering.ErrIdempotencyConflict):
		writeProblem(w, r, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT",
			"this idempotency key or externalTransactionId was already used with different content")
	case errors.Is(err, auth.ErrUnauthenticated):
		w.Header().Set("WWW-Authenticate", `Bearer realm="wager"`)
		writeProblem(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid or missing access token")
	case errors.Is(err, wagering.ErrTransient), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		a.log.WarnContext(r.Context(), "temporary failure", slog.String("error", err.Error()))
		w.Header().Set("Retry-After", "1")
		writeProblem(w, r, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "temporary failure, retry later")
	default:
		a.log.ErrorContext(r.Context(), "unexpected error", slog.String("error", err.Error()))
		writeProblem(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
	}
}
