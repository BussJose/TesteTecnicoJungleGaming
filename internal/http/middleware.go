package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
	"github.com/monii/backend-challenge-go/internal/platform/auth"
)

type ctxKey int

const (
	keyRequestID ctxKey = iota
	keyIdentity
)

func requestID(ctx context.Context) string {
	s, _ := ctx.Value(keyRequestID).(string)
	return s
}

func identityFrom(ctx context.Context) *auth.Identity {
	i, _ := ctx.Value(keyIdentity).(*auth.Identity)
	return i
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// handle registra uma rota com observabilidade, autenticação e papel exigido.
// role vazio = rota pública.
func (a *API) handle(mux *http.ServeMux, pattern, role string, h http.HandlerFunc) {
	var next http.Handler = h
	if role != "" {
		next = a.authenticate(role, next)
	}
	mux.Handle(pattern, a.observe(pattern, next))
}

// observe cria o request id, mede a duração, conta e registra em log JSON.
func (a *API) observe(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-Id")
		if rid == "" || len(rid) > 100 || strings.ContainsAny(rid, "\r\n") {
			rid = id.New().String()
		}
		w.Header().Set("X-Request-Id", rid)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		ctx := context.WithValue(r.Context(), keyRequestID, rid)
		next.ServeHTTP(rec, r.WithContext(ctx))

		d := time.Since(start)
		status := strconv.Itoa(rec.status)
		a.m.requests.Inc(r.Method, route, status)
		a.m.duration.Observe(d.Seconds(), r.Method, route)
		attrs := []any{
			slog.String("request_id", rid), slog.String("method", r.Method), slog.String("route", route),
			slog.Int("status", rec.status), slog.Float64("duration_ms", float64(d.Microseconds())/1000),
		}
		if i := identityFrom(ctx); i != nil {
			attrs = append(attrs, slog.String("client_id", i.ClientID))
			if i.ProviderID != "" {
				attrs = append(attrs, slog.String("provider_id", i.ProviderID))
			}
		}
		a.log.InfoContext(ctx, "http_request", attrs...)
	})
}

// authenticate exige um token Bearer válido e o papel indicado.
func (a *API) authenticate(role string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		scheme, token, ok := strings.Cut(h, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			a.fail(w, r, auth.ErrUnauthenticated)
			return
		}
		ident, err := a.verifier.Verify(r.Context(), strings.TrimSpace(token))
		if err != nil {
			if errors.Is(err, auth.ErrUnauthenticated) {
				a.fail(w, r, err)
			} else { // Keycloak inacessível etc.: falha temporária, não é 401
				a.fail(w, r, errors.Join(wagering.ErrTransient, err))
			}
			return
		}
		if !ident.HasRole(role) {
			writeProblem(w, r, http.StatusForbidden, "FORBIDDEN", "the client is not allowed to use this endpoint")
			return
		}
		if role == auth.RoleProvider && ident.ProviderID == "" {
			writeProblem(w, r, http.StatusForbidden, "FORBIDDEN", "token has no provider identity")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), keyIdentity, ident)))
	})
}
